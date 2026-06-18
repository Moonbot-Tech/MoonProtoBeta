//! Candle rows and low-level candle request helpers.
//!
//! Regular applications normally use `MoonClient`: retained 5m candles are
//! loaded after trades storage is enabled and then read through market-history
//! readers; demand-driven CoinCard candles are requested with
//! `MoonClient::candles().request_coin_card_for(...)` and read from the
//! snapshot. The string-keyed request helper is kept for scripts/tools.
//!
//! The raw packed records and chunked `RequestCandlesData` parser remain in
//! this module for protocol tests and custom tools, but they are hidden from
//! the normal rustdoc surface.

use zerocopy::byteorder::little_endian::{F32 as LeF32, F64 as LeF64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use super::engine_api::EngineMethod;
use super::engine_request::build_engine_request_full;
#[cfg(any(test, feature = "diagnostics"))]
use crate::time::DelphiTime;
use crate::time::MoonTime;

mod aggregator;
mod request_parser;

#[doc(hidden)]
#[cfg(feature = "diagnostics")]
pub use self::aggregator::CandlesAggregator;
#[doc(hidden)]
#[cfg(not(feature = "diagnostics"))]
pub(crate) use self::aggregator::CandlesAggregator;
pub(crate) use self::aggregator::CandlesChunkResult;
#[doc(hidden)]
#[cfg(feature = "diagnostics")]
pub use self::request_parser::parse_request_candles_data_response;
#[doc(hidden)]
#[cfg(not(feature = "diagnostics"))]
pub(crate) use self::request_parser::parse_request_candles_data_response;
pub(crate) use self::request_parser::parse_request_candles_data_response_partial;
#[cfg(test)]
pub(crate) use self::request_parser::{
    parse_request_candles_data_response_partial_with_local_shift,
    parse_request_candles_data_response_with_local_shift, read_deep_price_pack,
    read_deep_price_pack_old,
};

/// Candle-count safety cap for one market in demand-driven candle responses.
/// Normal UI requests are much smaller; absurd wire counts are rejected before
/// allocation.
pub(crate) const MAX_REQUEST_CANDLES_PER_MARKET: usize = 25_000;
pub(crate) const MAX_REQUEST_CANDLES_MARKETS: usize = 4_096;
pub(crate) const MAX_REQUEST_CANDLES_TOTAL: usize = 10_000_000;

/// Chunk payload is the zlib stream from `StoreCandlesToZip` after the outer
/// EngineResponse layer is decoded. Live full snapshots are single-digit MiB;
/// this leaves large future headroom while preventing a second GiB-scale copy
/// during chunk aggregation.
pub(crate) const MAX_REQUEST_CANDLES_CHUNKED_PAYLOAD_BYTES: usize = 128 * 1024 * 1024;

/// Domain cap for the decompressed `StoreCandlesToZip` stream. The global
/// inflate cap stays larger for strategy blobs; candle snapshots have known row
/// sizes and should not need unbounded transient memory.
pub(crate) const MAX_REQUEST_CANDLES_DECOMPRESSED_BYTES: usize = 384 * 1024 * 1024;

/// CoinCard is a demand-driven UI mini/history chart. A 2-billion row count is
/// never meaningful here; cap it before allocation and before the zero-tail
/// record loop.
pub(crate) const MAX_COIN_CARD_CANDLES: usize = 10_000;

/// Candle row used by CoinCard history and candle snapshots.
///
/// Use `open()`, `high()`, `low()`, `close()`, `volume()`, and time helpers in
/// application code. The raw fields are hidden to keep callers on the stable
/// helper API and away from legacy wire timestamp details.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DeepPrice {
    #[doc(hidden)]
    pub(crate) open: f32,
    #[doc(hidden)]
    pub(crate) close: f32,
    #[doc(hidden)]
    pub(crate) high: f32,
    #[doc(hidden)]
    pub(crate) low: f32,
    #[doc(hidden)]
    pub(crate) volume: f32,
    /// Legacy wire timestamp: double days since 1899-12-30.
    #[doc(hidden)]
    pub(crate) time: f64,
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
struct WireDeepPrice {
    open: LeF32,
    close: LeF32,
    high: LeF32,
    low: LeF32,
    volume: LeF32,
    time: LeF64,
}

#[doc(hidden)]
pub(crate) const DEEP_PRICE_SIZE: usize = std::mem::size_of::<WireDeepPrice>();
const _: [(); 28] = [(); DEEP_PRICE_SIZE];
const MINS_IN_DAY: f64 = 1440.0;

impl DeepPrice {
    #[inline]
    pub fn open(self) -> f32 {
        self.open
    }

    #[inline]
    pub fn close(self) -> f32 {
        self.close
    }

    #[inline]
    pub fn high(self) -> f32 {
        self.high
    }

    #[inline]
    pub fn low(self) -> f32 {
        self.low
    }

    #[inline]
    pub fn volume(self) -> f32 {
        self.volume
    }

    #[inline]
    pub fn time(self) -> MoonTime {
        MoonTime::from_delphi_days(self.time).unwrap_or(MoonTime::ZERO)
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn time_delphi(self) -> DelphiTime {
        DelphiTime::from_days(self.time)
    }

    #[inline]
    pub fn unix_millis(self) -> i64 {
        self.time().unix_millis()
    }

    fn from_wire(wire: WireDeepPrice) -> Self {
        Self {
            open: wire.open.get(),
            close: wire.close.get(),
            high: wire.high.get(),
            low: wire.low.get(),
            volume: wire.volume.get(),
            time: wire.time.get(),
        }
    }

    fn to_wire(self) -> WireDeepPrice {
        WireDeepPrice {
            open: LeF32::new(self.open),
            close: LeF32::new(self.close),
            high: LeF32::new(self.high),
            low: LeF32::new(self.low),
            volume: LeF32::new(self.volume),
            time: LeF64::new(self.time),
        }
    }

    /// Read one packed candle record from `data`.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn read_from(data: &[u8], pos: &mut usize) -> Option<Self> {
        if *pos + DEEP_PRICE_SIZE > data.len() {
            return None;
        }
        let wire = WireDeepPrice::read_from_bytes(&data[*pos..*pos + DEEP_PRICE_SIZE]).ok()?;
        *pos += DEEP_PRICE_SIZE;
        Some(Self::from_wire(wire))
    }

    /// Read one packed candle record from `data`.
    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) fn read_from(data: &[u8], pos: &mut usize) -> Option<Self> {
        if *pos + DEEP_PRICE_SIZE > data.len() {
            return None;
        }
        let wire = WireDeepPrice::read_from_bytes(&data[*pos..*pos + DEEP_PRICE_SIZE]).ok()?;
        *pos += DEEP_PRICE_SIZE;
        Some(Self::from_wire(wire))
    }

    fn read_from_delphi_stream(data: &[u8], pos: &mut usize) -> Option<Self> {
        let mut bytes = [0u8; DEEP_PRICE_SIZE];
        let available = data.len().saturating_sub(*pos).min(DEEP_PRICE_SIZE);
        if available > 0 {
            bytes[..available].copy_from_slice(&data[*pos..*pos + available]);
            *pos += available;
        }
        let wire = WireDeepPrice::read_from_bytes(&bytes).ok()?;
        Some(Self::from_wire(wire))
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn write_to(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.to_wire().as_bytes());
    }

    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) fn write_to(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.to_wire().as_bytes());
    }
}

/// Packed `TDeepPricePack` inside `RequestCandlesData` stream.
///
/// Compact 20-byte record for each 5m candle; open/close are reconstructed from
/// high/low on receive.
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
struct WireDeepPricePack {
    high: LeF32,
    low: LeF32,
    volume: LeF32,
    time: LeF64,
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
struct WireDeepPricePackOld {
    high: LeF64,
    low: LeF64,
    volume: LeF64,
    time: LeF64,
}

#[doc(hidden)]
pub(crate) const DEEP_PRICE_PACK_SIZE: usize = std::mem::size_of::<WireDeepPricePack>();
const _: [(); 20] = [(); DEEP_PRICE_PACK_SIZE];
#[doc(hidden)]
pub(crate) const DEEP_PRICE_PACK_OLD_SIZE: usize = std::mem::size_of::<WireDeepPricePackOld>();
const _: [(); 32] = [(); DEEP_PRICE_PACK_OLD_SIZE];
const WALL_ITEM_SIZE: usize = 8;
const REQUEST_CANDLES_MARKET_MIN_SIZE: usize = 2 + 4 + WALL_ITEM_SIZE * 8;

/// Packed wall bucket: `volume: f32`, `count: i32`.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct WallItem {
    pub volume: f32,
    pub count: i32,
}

/// One market entry from the compressed candle snapshot stream.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq)]
pub struct RequestCandlesMarket {
    pub market_name: String,
    pub candles_5m: Vec<DeepPrice>,
    pub buy_wall: [WallItem; 4],
    pub sell_wall: [WallItem; 4],
}

/// Demand-driven CoinCard/history candle interval.
///
/// Byte-exact order in the current MoonBot core:
/// `(hk_1m, hk_5m, hk_30m, hk_1h, hk_4h, hk_1d)` — six values.
/// The old backup source had five values without `hk_4h`; using those old
/// ordinals would shift `Day1` to value 4 and the server would interpret the
/// request as `hk_4h`.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeepHistoryKind {
    Min1 = 0,  // hk_1m
    Min5 = 1,  // hk_5m
    Min30 = 2, // hk_30m
    Hour1 = 3, // hk_1h
    Hour4 = 4, // hk_4h
    Day1 = 5,  // hk_1d
}

// =============================================================================
//  Builders
// =============================================================================

/// `emk_GetCoinCardCandles(market, ticks)` — request CoinCard candles.
///
/// Wire: market_name + `WriteByte(Ord(ticks))`.
pub(crate) fn get_coin_card_candles(market_name: &str, ticks: DeepHistoryKind) -> Vec<u8> {
    let params = vec![ticks as u8];
    build_engine_request_full(EngineMethod::GetCoinCardCandles, market_name, &[], &params)
}

// =============================================================================
//  Response parser
// =============================================================================

/// Parse `emk_GetCoinCardCandles` response:
/// `count:i32 + N × TDeepPrice`.
///
/// `data` is already-uncompressed `EngineResponse.data`.
pub(crate) fn parse_coin_card_candles_response(data: &[u8]) -> Option<Vec<DeepPrice>> {
    let mut pos = 0usize;
    let count_raw = i32::from_le_bytes(read_zero_tail::<4>(data, &mut pos));
    if count_raw <= 0 {
        return Some(Vec::new());
    }
    let count = count_raw as usize;
    if count > MAX_COIN_CARD_CANDLES {
        log::warn!(target: "moonproto::candles",
            "CoinCard candle count {count} exceeds domain cap {MAX_COIN_CARD_CANDLES}");
        return None;
    }
    let mut out = Vec::new();
    if out.try_reserve_exact(count).is_err() {
        log::warn!(target: "moonproto::candles", "coin-card candle count {} cannot be allocated", count);
        return None;
    }
    for _ in 0..count {
        out.push(DeepPrice::read_from_delphi_stream(data, &mut pos)?);
    }
    Some(out)
}

#[cfg(unix)]
pub(crate) fn current_local_time_shift_minutes() -> f64 {
    unsafe {
        let now = libc::time(std::ptr::null_mut());
        if now == -1 {
            return 0.0;
        }

        let mut local_tm: libc::tm = std::mem::zeroed();
        let mut utc_tm: libc::tm = std::mem::zeroed();
        if libc::localtime_r(&now, &mut local_tm).is_null()
            || libc::gmtime_r(&now, &mut utc_tm).is_null()
        {
            return 0.0;
        }

        let local_secs = libc::mktime(&mut local_tm);
        utc_tm.tm_isdst = -1;
        let utc_as_local_secs = libc::mktime(&mut utc_tm);
        if local_secs == -1 || utc_as_local_secs == -1 {
            return 0.0;
        }
        ((local_secs - utc_as_local_secs) as f64 / 60.0).round()
    }
}

#[cfg(windows)]
pub(crate) fn current_local_time_shift_minutes() -> f64 {
    #[repr(C)]
    struct SystemTime {
        year: u16,
        month: u16,
        day_of_week: u16,
        day: u16,
        hour: u16,
        minute: u16,
        second: u16,
        milliseconds: u16,
    }

    #[repr(C)]
    struct TimeZoneInformation {
        bias: i32,
        standard_name: [u16; 32],
        standard_date: SystemTime,
        standard_bias: i32,
        daylight_name: [u16; 32],
        daylight_date: SystemTime,
        daylight_bias: i32,
    }

    extern "system" {
        fn GetTimeZoneInformation(info: *mut TimeZoneInformation) -> u32;
    }

    const TIME_ZONE_ID_INVALID: u32 = u32::MAX;
    const TIME_ZONE_ID_STANDARD: u32 = 1;
    const TIME_ZONE_ID_DAYLIGHT: u32 = 2;

    unsafe {
        let mut info: TimeZoneInformation = std::mem::zeroed();
        let zone_id = GetTimeZoneInformation(&mut info);
        if zone_id == TIME_ZONE_ID_INVALID {
            return 0.0;
        }
        let extra_bias = match zone_id {
            TIME_ZONE_ID_STANDARD => info.standard_bias,
            TIME_ZONE_ID_DAYLIGHT => info.daylight_bias,
            _ => 0,
        };
        (-(info.bias + extra_bias) as f64).round()
    }
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn current_local_time_shift_minutes() -> f64 {
    0.0
}

fn read_zero_tail<const N: usize>(data: &[u8], pos: &mut usize) -> [u8; N] {
    let mut out = [0u8; N];
    let available = data.len().saturating_sub(*pos).min(N);
    if available > 0 {
        out[..available].copy_from_slice(&data[*pos..*pos + available]);
        *pos += available;
    }
    out
}

#[cfg(test)]
mod tests;
