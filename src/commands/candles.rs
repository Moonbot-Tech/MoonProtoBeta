//! Candles channel — TDeepPrice records (28-byte packed) for CoinCard and
//! packed market candles stream for `RequestCandlesData`.
//!
//! Источник Delphi: `MarketsU.pas:701-705 TDeepPrice` + `MoonProtoEngineServer.pas:382-395` (`emk_GetCoinCardCandles`)
//! + `MoonProtoClient.pas:795-876` (chunked candles aggregation для `emk_RequestCandlesData`).
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
//! ## Запросы
//!
//! - **`emk_GetCoinCardCandles`** — простой response: `count:i32 + N × TDeepPrice`.
//! - **`emk_RequestCandlesData`** — chunked: each response starts with
//!   `ChunkIndex:u16 + ChunkTotal:u16` + chunk_data. After all chunks are merged,
//!   the resulting bytes are the zlib stream produced by Delphi
//!   `TMarkets.StoreCandlesToZip`. Parsed `TDeepPricePack.Time` values are adjusted
//!   with the same local-timezone correction as Delphi `TMarkets.ApplyRecvdStream`.
//!
//! Используй `CandlesAggregator` для сборки chunked responses.

use std::io::Read;

use flate2::read::ZlibDecoder;
use zerocopy::byteorder::little_endian::{F32 as LeF32, F64 as LeF64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use super::engine_api::EngineMethod;
use super::engine_request::build_engine_request_full;

mod aggregator;

pub use self::aggregator::CandlesAggregator;
pub(crate) use self::aggregator::CandlesChunkResult;

/// Packed `TDeepPrice` (28 bytes). Соответствует Delphi `MarketsU.pas:701-705`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DeepPrice {
    pub open_p: f32,
    pub close_p: f32,
    pub max_p: f32,
    pub min_p: f32,
    pub vol: f32,
    /// `TDateTime` (Delphi double, дни с 1899-12-30).
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

    /// Прочитать один record из bytes.
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
/// **Byte-exact с текущим Delphi**: `(hk_1m, hk_5m, hk_30m, hk_1h, hk_4h, hk_1d)` — 6 значений.
/// Старая версия (bak/) имела 5 значений без hk_4h. Использование старых ординалов сместило бы
/// `Day1` на позицию 4 → сервер интерпретировал бы запрос как `hk_4h` (4-часовые свечи).
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

/// `emk_GetCoinCardCandles(market, ticks)` — запрос свечей для CoinCard.
///
/// Wire: market_name + `WriteByte(Ord(ticks))`.
pub fn get_coin_card_candles(market_name: &str, ticks: DeepHistoryKind) -> Vec<u8> {
    let params = vec![ticks as u8];
    build_engine_request_full(EngineMethod::GetCoinCardCandles, market_name, &[], &params)
}

// =============================================================================
//  Response parser
// =============================================================================

/// Распарсить `emk_GetCoinCardCandles` response: `count:i32 + N × TDeepPrice`.
/// `data` — `EngineResponse.data` (уже распакованный DEFLATE).
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

/// Parse merged `emk_RequestCandlesData` bytes.
///
/// Input is the concatenated chunk payload returned by [`CandlesAggregator`].
/// Delphi stores a zlib-compressed stream:
///
/// ```text
/// legacy_count:i32
/// version:u8
/// if version > 1 { count:i32 } else { count = legacy_count }
/// server_timezone_shift_minutes:f64
/// repeated count times:
///   market_name: Delphi UTF-16 String (u16 char count + chars)
///   candle_count:i32
///   candle_count * TDeepPricePack(version >= 2) or TDeepPricePackOLD(version == 1)
///   buy_wall:  4 * TWallItem
///   sell_wall: 4 * TWallItem
/// ```
///
pub fn parse_request_candles_data_response(
    zipped_data: &[u8],
) -> Option<Vec<RequestCandlesMarket>> {
    parse_request_candles_data_response_with_local_shift(
        zipped_data,
        current_local_time_shift_minutes(),
    )
}

pub(crate) fn parse_request_candles_data_response_partial_like_delphi(
    zipped_data: &[u8],
) -> Option<Vec<RequestCandlesMarket>> {
    parse_request_candles_data_response_partial_with_local_shift(
        zipped_data,
        current_local_time_shift_minutes(),
    )
}

fn parse_request_candles_data_response_with_local_shift(
    zipped_data: &[u8],
    local_time_shift_minutes: f64,
) -> Option<Vec<RequestCandlesMarket>> {
    let mut decoder = ZlibDecoder::new(zipped_data);
    let mut data = Vec::new();
    if let Err(e) = decoder.read_to_end(&mut data) {
        log::warn!(target: "moonproto::candles", "RequestCandlesData zlib decode failed: {e}");
        return None;
    }

    let mut pos = 0usize;
    let legacy_count = read_i32(&data, &mut pos)?;
    if legacy_count < 0 {
        log::warn!(target: "moonproto::candles", "RequestCandlesData negative legacy count {legacy_count}");
        return None;
    }

    let ver = read_u8(&data, &mut pos)?;
    if ver > 2 {
        log::warn!(target: "moonproto::candles", "RequestCandlesData unsupported version {ver}");
        return None;
    }

    let count_raw = if ver > 1 {
        read_i32(&data, &mut pos)?
    } else {
        legacy_count
    };
    if count_raw < 0 {
        log::warn!(target: "moonproto::candles", "RequestCandlesData negative market count {count_raw}");
        return None;
    }
    let count = count_raw as usize;

    let server_time_shift_minutes = read_f64(&data, &mut pos)?;
    let time_shift_days =
        (local_time_shift_minutes.round() - server_time_shift_minutes) / MINS_IN_DAY;

    let min_required = count.saturating_mul(REQUEST_CANDLES_MARKET_MIN_SIZE);
    let remaining = data.len().saturating_sub(pos);
    if min_required > remaining {
        log::warn!(target: "moonproto::candles",
            "RequestCandlesData market count {count} requires at least {min_required} bytes, remaining {remaining}");
        return None;
    }

    let mut markets = Vec::with_capacity(count);
    for _ in 0..count {
        let market_name = read_delphi_utf16_string(&data, &mut pos)?;
        let candle_count_raw = read_i32(&data, &mut pos)?;
        if candle_count_raw < 0 {
            log::warn!(target: "moonproto::candles",
                "RequestCandlesData negative candle count for {market_name}: {candle_count_raw}");
            return None;
        }
        let candle_count = candle_count_raw as usize;
        let record_size = if ver >= 2 {
            DEEP_PRICE_PACK_SIZE
        } else {
            DEEP_PRICE_PACK_OLD_SIZE
        };
        let required = candle_count.checked_mul(record_size)?;
        if required > data.len().saturating_sub(pos) {
            log::warn!(target: "moonproto::candles",
                "RequestCandlesData market {market_name} requires {required} candle bytes, remaining {}",
                data.len().saturating_sub(pos));
            return None;
        }

        let mut candles_5m = Vec::with_capacity(candle_count);
        for _ in 0..candle_count {
            let mut candle = if ver >= 2 {
                read_deep_price_pack(&data, &mut pos)?
            } else {
                read_deep_price_pack_old(&data, &mut pos)?
            };
            candle.time += time_shift_days;
            candles_5m.push(candle);
        }
        let buy_wall = read_wall_data(&data, &mut pos)?;
        let sell_wall = read_wall_data(&data, &mut pos)?;
        markets.push(RequestCandlesMarket {
            market_name,
            candles_5m,
            buy_wall,
            sell_wall,
        });
    }

    Some(markets)
}

fn parse_request_candles_data_response_partial_with_local_shift(
    zipped_data: &[u8],
    local_time_shift_minutes: f64,
) -> Option<Vec<RequestCandlesMarket>> {
    let mut decoder = ZlibDecoder::new(zipped_data);
    let mut data = Vec::new();
    if let Err(e) = decoder.read_to_end(&mut data) {
        log::warn!(target: "moonproto::candles", "RequestCandlesData zlib decode failed: {e}");
        return None;
    }

    let mut pos = 0usize;
    let legacy_count = read_i32(&data, &mut pos)?;
    let ver = read_u8(&data, &mut pos)?;
    if ver > 2 {
        log::warn!(target: "moonproto::candles", "RequestCandlesData unsupported version {ver}");
        return None;
    }

    let count_raw = if ver > 1 {
        read_i32(&data, &mut pos)?
    } else {
        legacy_count
    };
    let server_time_shift_minutes = read_f64(&data, &mut pos)?;
    let time_shift_days =
        (local_time_shift_minutes.round() - server_time_shift_minutes) / MINS_IN_DAY;

    if count_raw <= 0 {
        return Some(Vec::new());
    }

    let count = count_raw as usize;
    let mut markets = Vec::new();
    for _ in 0..count {
        let Some(market_name) = read_delphi_utf16_string(&data, &mut pos) else {
            break;
        };
        let Some(candle_count_raw) = read_i32(&data, &mut pos) else {
            break;
        };
        if candle_count_raw < 0 {
            break;
        }
        let candle_count = candle_count_raw as usize;
        let record_size = if ver >= 2 {
            DEEP_PRICE_PACK_SIZE
        } else {
            DEEP_PRICE_PACK_OLD_SIZE
        };
        let Some(required) = candle_count.checked_mul(record_size) else {
            break;
        };
        if required > data.len().saturating_sub(pos) {
            break;
        }

        let mut candles_5m = Vec::new();
        if candles_5m.try_reserve_exact(candle_count).is_err() {
            break;
        }
        let mut ok = true;
        for _ in 0..candle_count {
            let candle = if ver >= 2 {
                read_deep_price_pack(&data, &mut pos)
            } else {
                read_deep_price_pack_old(&data, &mut pos)
            };
            let Some(mut candle) = candle else {
                ok = false;
                break;
            };
            candle.time += time_shift_days;
            candles_5m.push(candle);
        }
        if !ok {
            break;
        }

        let buy_wall = read_wall_data_zero_tail(&data, &mut pos);
        let sell_wall = read_wall_data_zero_tail(&data, &mut pos);
        markets.push(RequestCandlesMarket {
            market_name,
            candles_5m,
            buy_wall,
            sell_wall,
        });
    }

    Some(markets)
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

fn read_u8(data: &[u8], pos: &mut usize) -> Option<u8> {
    if *pos + 1 > data.len() {
        return None;
    }
    let value = data[*pos];
    *pos += 1;
    Some(value)
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

fn read_i32(data: &[u8], pos: &mut usize) -> Option<i32> {
    if *pos + 4 > data.len() {
        return None;
    }
    let value = i32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Some(value)
}

fn read_f32(data: &[u8], pos: &mut usize) -> Option<f32> {
    if *pos + 4 > data.len() {
        return None;
    }
    let value = f32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Some(value)
}

fn read_f64(data: &[u8], pos: &mut usize) -> Option<f64> {
    if *pos + 8 > data.len() {
        return None;
    }
    let value = f64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Some(value)
}

fn read_delphi_utf16_string(data: &[u8], pos: &mut usize) -> Option<String> {
    if *pos + 2 > data.len() {
        return None;
    }
    let chars = u16::from_le_bytes(data[*pos..*pos + 2].try_into().unwrap()) as usize;
    *pos += 2;
    let bytes = chars.checked_mul(2)?;
    if *pos + bytes > data.len() {
        return None;
    }
    let mut utf16 = Vec::with_capacity(chars);
    for chunk in data[*pos..*pos + bytes].chunks_exact(2) {
        utf16.push(u16::from_le_bytes([chunk[0], chunk[1]]));
    }
    *pos += bytes;
    Some(String::from_utf16_lossy(&utf16))
}

fn read_deep_price_pack(data: &[u8], pos: &mut usize) -> Option<DeepPrice> {
    if *pos + DEEP_PRICE_PACK_SIZE > data.len() {
        return None;
    }
    let wire = WireDeepPricePack::read_from_bytes(&data[*pos..*pos + DEEP_PRICE_PACK_SIZE]).ok()?;
    *pos += DEEP_PRICE_PACK_SIZE;
    let max_p = wire.max_p.get();
    let min_p = wire.min_p.get();
    Some(DeepPrice {
        open_p: max_p,
        close_p: min_p,
        max_p,
        min_p,
        vol: wire.vol.get(),
        time: wire.time.get(),
    })
}

fn read_deep_price_pack_old(data: &[u8], pos: &mut usize) -> Option<DeepPrice> {
    if *pos + DEEP_PRICE_PACK_OLD_SIZE > data.len() {
        return None;
    }
    let wire =
        WireDeepPricePackOld::read_from_bytes(&data[*pos..*pos + DEEP_PRICE_PACK_OLD_SIZE]).ok()?;
    *pos += DEEP_PRICE_PACK_OLD_SIZE;
    let max_p = wire.max_p.get() as f32;
    let min_p = wire.min_p.get() as f32;
    Some(DeepPrice {
        open_p: max_p,
        close_p: min_p,
        max_p,
        min_p,
        vol: wire.vol.get() as f32,
        time: wire.time.get(),
    })
}

fn read_wall_data(data: &[u8], pos: &mut usize) -> Option<[WallItem; 4]> {
    let mut out = [WallItem::default(); 4];
    for item in &mut out {
        item.vol = read_f32(data, pos)?;
        item.count = read_i32(data, pos)?;
    }
    Some(out)
}

fn read_wall_data_zero_tail(data: &[u8], pos: &mut usize) -> [WallItem; 4] {
    let mut out = [WallItem::default(); 4];
    for item in &mut out {
        item.vol = f32::from_le_bytes(read_zero_tail::<4>(data, pos));
        item.count = i32::from_le_bytes(read_zero_tail::<4>(data, pos));
    }
    out
}

#[cfg(test)]
mod tests;
