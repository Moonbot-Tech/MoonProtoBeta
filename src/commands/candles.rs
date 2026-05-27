//! Candles channel — TDeepPrice records (28-byte packed) for CoinCard and
//! packed market candles stream for `RequestCandlesData`.
//!
//! Delphi sources: `MarketsU.pas:701-705 TDeepPrice`,
//! `MoonProtoEngineServer.pas:382-395` (`emk_GetCoinCardCandles`), and
//! `MoonProtoClient.pas:795-876` (chunked candles aggregation for
//! `emk_RequestCandlesData`).
//!
//! ## Wire format
//!
//! `TDeepPrice` (28 bytes packed):
//! ```text
//! OpenP:  f32 (4)
//! CloseP: f32 (4)
//! MaxP:   f32 (4)
//! MinP:   f32 (4)
//! Vol:    f32 (4)
//! Time:   f64 (8)  // TDateTime
//! ```
//!
//! ## Requests
//!
//! - **`emk_GetCoinCardCandles`** — simple response:
//!   `count:i32 + N × TDeepPrice`.
//! - **`emk_RequestCandlesData`** — chunked: each response starts with
//!   `ChunkIndex:u16 + ChunkTotal:u16` + chunk_data. After all chunks are merged,
//!   the resulting bytes are the zlib stream produced by Delphi
//!   `TMarkets.StoreCandlesToZip`. Parsed `TDeepPricePack.Time` values are adjusted
//!   with the same local-timezone correction as Delphi `TMarkets.ApplyRecvdStream`.
//!
//! Use [`CandlesAggregator`] to merge chunked responses in low-level tools.

use zerocopy::byteorder::little_endian::{F32 as LeF32, F64 as LeF64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use super::engine_api::EngineMethod;
use super::engine_request::build_engine_request_full;
use crate::time::DelphiTime;

mod aggregator;
mod request_parser;

pub use self::aggregator::CandlesAggregator;
pub(crate) use self::aggregator::CandlesChunkResult;
pub use self::request_parser::parse_request_candles_data_response;
pub(crate) use self::request_parser::parse_request_candles_data_response_partial_like_delphi;
#[cfg(test)]
pub(crate) use self::request_parser::{
    parse_request_candles_data_response_partial_with_local_shift,
    parse_request_candles_data_response_with_local_shift, read_deep_price_pack,
    read_deep_price_pack_old,
};

/// Packed `TDeepPrice` (28 bytes), matching Delphi `MarketsU.pas:701-705`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DeepPrice {
    pub open_p: f32,
    pub close_p: f32,
    pub max_p: f32,
    pub min_p: f32,
    pub vol: f32,
    /// `TDateTime` (Delphi double, days since 1899-12-30).
    pub time: f64,
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
struct WireDeepPrice {
    open_p: LeF32,
    close_p: LeF32,
    max_p: LeF32,
    min_p: LeF32,
    vol: LeF32,
    time: LeF64,
}

pub const DEEP_PRICE_SIZE: usize = std::mem::size_of::<WireDeepPrice>();
const _: [(); 28] = [(); DEEP_PRICE_SIZE];
const MINS_IN_DAY: f64 = 1440.0;

impl DeepPrice {
    #[inline]
    pub fn open(self) -> f32 {
        self.open_p
    }

    #[inline]
    pub fn close(self) -> f32 {
        self.close_p
    }

    #[inline]
    pub fn high(self) -> f32 {
        self.max_p
    }

    #[inline]
    pub fn low(self) -> f32 {
        self.min_p
    }

    #[inline]
    pub fn time_delphi(self) -> DelphiTime {
        DelphiTime::from_days(self.time)
    }

    #[inline]
    pub fn unix_millis(self) -> Option<i64> {
        self.time_delphi().unix_millis()
    }

    fn from_wire(wire: WireDeepPrice) -> Self {
        Self {
            open_p: wire.open_p.get(),
            close_p: wire.close_p.get(),
            max_p: wire.max_p.get(),
            min_p: wire.min_p.get(),
            vol: wire.vol.get(),
            time: wire.time.get(),
        }
    }

    fn to_wire(self) -> WireDeepPrice {
        WireDeepPrice {
            open_p: LeF32::new(self.open_p),
            close_p: LeF32::new(self.close_p),
            max_p: LeF32::new(self.max_p),
            min_p: LeF32::new(self.min_p),
            vol: LeF32::new(self.vol),
            time: LeF64::new(self.time),
        }
    }

    /// Read one packed candle record from `data`.
    pub fn read_from(data: &[u8], pos: &mut usize) -> Option<Self> {
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

    pub fn write_to(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.to_wire().as_bytes());
    }
}

/// Packed `TDeepPricePack` inside `RequestCandlesData` stream.
///
/// Delphi writes this compact 20-byte record for each 5m candle and reconstructs
/// `OpenP = MaxP`, `CloseP = MinP` on receive.
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
struct WireDeepPricePack {
    max_p: LeF32,
    min_p: LeF32,
    vol: LeF32,
    time: LeF64,
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
struct WireDeepPricePackOld {
    max_p: LeF64,
    min_p: LeF64,
    vol: LeF64,
    time: LeF64,
}

pub const DEEP_PRICE_PACK_SIZE: usize = std::mem::size_of::<WireDeepPricePack>();
const _: [(); 20] = [(); DEEP_PRICE_PACK_SIZE];
pub const DEEP_PRICE_PACK_OLD_SIZE: usize = std::mem::size_of::<WireDeepPricePackOld>();
const _: [(); 32] = [(); DEEP_PRICE_PACK_OLD_SIZE];
const WALL_ITEM_SIZE: usize = 8;
const REQUEST_CANDLES_MARKET_MIN_SIZE: usize = 2 + 4 + WALL_ITEM_SIZE * 8;

/// Delphi `TWallItem = record vol: Single; count: Integer end`.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct WallItem {
    pub vol: f32,
    pub count: i32,
}

/// One market entry from Delphi `TMarkets.StoreCandlesToZip`.
#[derive(Debug, Clone, PartialEq)]
pub struct RequestCandlesMarket {
    pub market_name: String,
    pub candles_5m: Vec<DeepPrice>,
    pub buy_wall: [WallItem; 4],
    pub sell_wall: [WallItem; 4],
}

/// `TMarketDeepHistoryKind` enum (EngineBase.pas:60).
///
/// Byte-exact order in current Delphi:
/// `(hk_1m, hk_5m, hk_30m, hk_1h, hk_4h, hk_1d)` — six values.
/// The old backup source had five values without `hk_4h`; using those old
/// ordinals would shift `Day1` to value 4 and the server would interpret the
/// request as `hk_4h`.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
pub fn get_coin_card_candles(market_name: &str, ticks: DeepHistoryKind) -> Vec<u8> {
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
pub fn parse_coin_card_candles_response(data: &[u8]) -> Option<Vec<DeepPrice>> {
    let mut pos = 0usize;
    let count_raw = i32::from_le_bytes(read_zero_tail::<4>(data, &mut pos));
    if count_raw <= 0 {
        return Some(Vec::new());
    }
    let count = count_raw as usize;
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
