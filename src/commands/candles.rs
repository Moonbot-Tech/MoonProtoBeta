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

        let Some(buy_wall) = read_wall_data(&data, &mut pos) else {
            break;
        };
        let Some(sell_wall) = read_wall_data(&data, &mut pos) else {
            break;
        };
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

// =============================================================================
//  Chunked aggregator (для emk_RequestCandlesData)
// =============================================================================

/// Aggregator для chunked candles response. Каждый chunk имеет header
/// `ChunkIndex:u16 + ChunkTotal:u16`, затем payload данных. После сборки всех
/// чанков — `merged_data()` возвращает склеенный поток для парсинга.
///
/// **Требования к caller'у:**
/// 1. `response_data` — это `EngineResponse.data` **уже после DEFLATE-decompression**
///    (если `is_compressed=true` — `parse_engine_response` распаковал автоматически).
/// 2. Фильтровать chunks по `request_uid`: если запущено несколько параллельных
///    `RequestCandlesData`, нужно вести отдельный `CandlesAggregator` для каждого
///    `request_uid` либо сбрасывать `reset()` при смене запроса. В Delphi эта
///    фильтрация делается через `resp.RequestUID == CandlesRequestUID`.
/// 3. Aggregator не валидирует payload — просто склеивает в порядке `ChunkIndex`.
///
/// Используется так:
/// ```ignore
/// let mut agg = CandlesAggregator::new();
/// // На каждый response с emk_RequestCandlesData:
/// if let Some(merged) = agg.on_chunk(&response.data) {
///     // Все чанки получены — merged содержит zlib stream from StoreCandlesToZip.
///     let markets = parse_request_candles_data_response(&merged)?;
/// }
/// ```
#[derive(Debug, Default)]
pub struct CandlesAggregator {
    chunks: Vec<Option<Vec<u8>>>,
    received: usize,
    total: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CandlesChunkResult {
    Ignored,
    Stored,
    Complete(Vec<u8>),
}

impl CandlesAggregator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Добавить chunk. Если все чанки собраны — вернуть склеенный буфер и сбросить state.
    /// Wire: `ChunkIndex:u16 + ChunkTotal:u16 + chunk_payload`.
    pub fn on_chunk(&mut self, response_data: &[u8]) -> Option<Vec<u8>> {
        match self.on_chunk_result(response_data) {
            CandlesChunkResult::Complete(merged) => Some(merged),
            CandlesChunkResult::Ignored | CandlesChunkResult::Stored => None,
        }
    }

    /// Добавить chunk и вернуть точный статус обработки.
    ///
    /// Delphi обновляет `Markets.LastChunkTime` только после сохранения нового
    /// chunk'а в пустой слот. Caller использует `Stored`/`Complete`, чтобы не
    /// продлевать timeout дубликатами или невалидными chunk headers.
    pub(crate) fn on_chunk_result(&mut self, response_data: &[u8]) -> CandlesChunkResult {
        if response_data.len() < 4 {
            return CandlesChunkResult::Ignored;
        }
        let chunk_index = u16::from_le_bytes([response_data[0], response_data[1]]) as usize;
        let chunk_total = u16::from_le_bytes([response_data[2], response_data[3]]) as usize;
        let payload = &response_data[4..];

        // Delphi stores ChunkTotal as Word and has no additional capacity cap.
        // `chunk_total` is already bounded by u16::MAX by wire format.
        if chunk_total == 0 {
            return CandlesChunkResult::Ignored;
        }

        // Resize если первый раз или total изменился
        if self.total != chunk_total {
            self.chunks.clear();
            self.chunks.resize_with(chunk_total, || None);
            self.received = 0;
            self.total = chunk_total;
        }

        // Сохранить chunk (дедупликация если повтор)
        if chunk_index < chunk_total && self.chunks[chunk_index].is_none() {
            self.chunks[chunk_index] = Some(payload.to_vec());
            self.received += 1;
        } else {
            return CandlesChunkResult::Ignored;
        }

        // Все ли собраны?
        if self.received == self.total && self.total > 0 {
            let mut merged = Vec::with_capacity(
                self.chunks
                    .iter()
                    .filter_map(|c| c.as_ref().map(|v| v.len()))
                    .sum(),
            );
            for chunk in self.chunks.drain(..).flatten() {
                merged.extend_from_slice(&chunk);
            }
            self.received = 0;
            self.total = 0;
            return CandlesChunkResult::Complete(merged);
        }
        CandlesChunkResult::Stored
    }

    /// Сбросить state (при новом запросе свечей).
    pub fn reset(&mut self) {
        self.chunks.clear();
        self.received = 0;
        self.total = 0;
    }

    pub fn progress(&self) -> (usize, usize) {
        (self.received, self.total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{write::ZlibEncoder, Compression};
    use std::io::Write;

    #[test]
    fn deep_price_size_is_28() {
        assert_eq!(std::mem::size_of::<WireDeepPrice>(), 28);
        assert_eq!(DEEP_PRICE_SIZE, 28);
        assert_eq!(std::mem::size_of::<WireDeepPricePack>(), 20);
        assert_eq!(DEEP_PRICE_PACK_SIZE, 20);
        assert_eq!(std::mem::size_of::<WireDeepPricePackOld>(), 32);
        assert_eq!(DEEP_PRICE_PACK_OLD_SIZE, 32);
    }

    #[test]
    fn deep_price_roundtrip() {
        let dp = DeepPrice {
            open_p: 100.0,
            close_p: 101.5,
            max_p: 102.0,
            min_p: 99.5,
            vol: 1234.5,
            time: 45123.5,
        };
        let mut buf = Vec::new();
        dp.write_to(&mut buf);
        assert_eq!(buf.len(), 28);
        let mut pos = 0;
        let dp2 = DeepPrice::read_from(&buf, &mut pos).unwrap();
        assert_eq!(dp, dp2);
        assert_eq!(pos, 28);
    }

    #[test]
    fn deep_price_pack_uses_private_wire_struct() {
        let mut bytes = Vec::new();
        write_deep_price_pack(&mut bytes, 101.0, -0.0, 12.5, 45_000.25);

        let mut expected = Vec::new();
        expected.extend_from_slice(&101.0f32.to_le_bytes());
        expected.extend_from_slice(&(-0.0f32).to_le_bytes());
        expected.extend_from_slice(&12.5f32.to_le_bytes());
        expected.extend_from_slice(&45_000.25f64.to_le_bytes());
        assert_eq!(bytes, expected);

        let mut pos = 0;
        let parsed = read_deep_price_pack(&bytes, &mut pos).expect("valid TDeepPricePack");
        assert_eq!(pos, DEEP_PRICE_PACK_SIZE);
        assert_eq!(parsed.open_p, 101.0);
        assert_eq!(parsed.close_p.to_bits(), (-0.0f32).to_bits());
        assert_eq!(parsed.max_p, 101.0);
        assert_eq!(parsed.min_p.to_bits(), (-0.0f32).to_bits());
        assert_eq!(parsed.vol, 12.5);
        assert_eq!(parsed.time, 45_000.25);
    }

    #[test]
    fn deep_price_pack_old_uses_private_wire_struct() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&101.0f64.to_le_bytes());
        bytes.extend_from_slice(&99.5f64.to_le_bytes());
        bytes.extend_from_slice(&12.5f64.to_le_bytes());
        bytes.extend_from_slice(&45_000.25f64.to_le_bytes());

        let mut pos = 0;
        let parsed = read_deep_price_pack_old(&bytes, &mut pos).expect("valid TDeepPricePackOLD");
        assert_eq!(pos, DEEP_PRICE_PACK_OLD_SIZE);
        assert_eq!(parsed.open_p, 101.0);
        assert_eq!(parsed.close_p, 99.5);
        assert_eq!(parsed.max_p, 101.0);
        assert_eq!(parsed.min_p, 99.5);
        assert_eq!(parsed.vol, 12.5);
        assert_eq!(parsed.time, 45_000.25);
    }

    #[test]
    fn coin_card_candles_response_roundtrip() {
        let candles = vec![
            DeepPrice {
                open_p: 100.0,
                close_p: 105.0,
                max_p: 110.0,
                min_p: 95.0,
                vol: 500.0,
                time: 45000.0,
            },
            DeepPrice {
                open_p: 105.0,
                close_p: 102.0,
                max_p: 107.0,
                min_p: 100.0,
                vol: 750.0,
                time: 45000.04,
            },
            DeepPrice {
                open_p: 102.0,
                close_p: 108.0,
                max_p: 109.0,
                min_p: 101.0,
                vol: 1200.0,
                time: 45000.08,
            },
        ];
        // Build response
        let mut buf = Vec::new();
        buf.extend_from_slice(&(candles.len() as i32).to_le_bytes());
        for c in &candles {
            c.write_to(&mut buf);
        }
        // Parse
        let parsed = parse_coin_card_candles_response(&buf).unwrap();
        assert_eq!(parsed, candles);
    }

    #[test]
    fn coin_card_candles_response_matches_delphi_read_tails() {
        assert_eq!(parse_coin_card_candles_response(&[]), Some(Vec::new()));
        assert_eq!(
            parse_coin_card_candles_response(&(-1i32).to_le_bytes()),
            Some(Vec::new())
        );

        let mut partial = Vec::new();
        partial.extend_from_slice(&(1i32).to_le_bytes());
        partial.extend_from_slice(&101.5f32.to_le_bytes());

        let parsed = parse_coin_card_candles_response(&partial).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].open_p, 101.5);
        assert_eq!(parsed[0].close_p, 0.0);
        assert_eq!(parsed[0].max_p, 0.0);
        assert_eq!(parsed[0].min_p, 0.0);
        assert_eq!(parsed[0].vol, 0.0);
        assert_eq!(parsed[0].time, 0.0);
    }

    #[test]
    fn coin_card_candles_response_zero_fills_missing_records_like_delphi_array_read() {
        let first = DeepPrice {
            open_p: 100.0,
            close_p: 105.0,
            max_p: 110.0,
            min_p: 95.0,
            vol: 500.0,
            time: 45000.0,
        };
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(2i32).to_le_bytes());
        first.write_to(&mut bytes);

        let parsed = parse_coin_card_candles_response(&bytes).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0], first);
        assert_eq!(
            parsed[1],
            DeepPrice {
                open_p: 0.0,
                close_p: 0.0,
                max_p: 0.0,
                min_p: 0.0,
                vol: 0.0,
                time: 0.0,
            }
        );
    }

    #[test]
    fn aggregator_single_chunk() {
        let mut agg = CandlesAggregator::new();
        // ChunkIndex=0, ChunkTotal=1, payload=[1,2,3,4]
        let chunk = vec![0, 0, 1, 0, 1, 2, 3, 4];
        let merged = agg.on_chunk(&chunk).unwrap();
        assert_eq!(merged, vec![1, 2, 3, 4]);
    }

    #[test]
    fn aggregator_multi_chunk() {
        let mut agg = CandlesAggregator::new();
        // Total=3 chunks. Шлём в неправильном порядке.
        let c0 = {
            let mut v = vec![0u8, 0u8, 3u8, 0u8]; // idx=0, total=3
            v.extend_from_slice(&[10, 11]);
            v
        };
        let c2 = {
            let mut v = vec![2u8, 0u8, 3u8, 0u8]; // idx=2, total=3
            v.extend_from_slice(&[30, 31]);
            v
        };
        let c1 = {
            let mut v = vec![1u8, 0u8, 3u8, 0u8]; // idx=1, total=3
            v.extend_from_slice(&[20, 21]);
            v
        };
        assert!(agg.on_chunk(&c0).is_none());
        assert_eq!(agg.progress(), (1, 3));
        assert!(agg.on_chunk(&c2).is_none());
        assert_eq!(agg.progress(), (2, 3));
        let merged = agg.on_chunk(&c1).unwrap();
        // Merge order = idx 0, 1, 2 (по позициям в массиве, не по порядку прихода).
        assert_eq!(merged, vec![10, 11, 20, 21, 30, 31]);
    }

    #[test]
    fn aggregator_duplicate_chunk_ignored() {
        let mut agg = CandlesAggregator::new();
        // Шлём один и тот же chunk дважды.
        let chunk = vec![0u8, 0u8, 2u8, 0u8, 1, 2];
        assert!(agg.on_chunk(&chunk).is_none());
        assert_eq!(agg.progress(), (1, 2));
        assert!(agg.on_chunk(&chunk).is_none()); // дубликат — игнорируется
        assert_eq!(agg.progress(), (1, 2));
        // Прислать второй chunk
        let chunk2 = vec![1u8, 0u8, 2u8, 0u8, 3, 4];
        let merged = agg.on_chunk(&chunk2).unwrap();
        assert_eq!(merged, vec![1, 2, 3, 4]);
    }

    #[test]
    fn aggregator_reports_stored_only_for_new_chunks() {
        let mut agg = CandlesAggregator::new();
        let first = vec![0u8, 0u8, 2u8, 0u8, 1, 2];
        let duplicate = first.clone();
        let bad_index = vec![4u8, 0u8, 2u8, 0u8, 9, 9];
        let second = vec![1u8, 0u8, 2u8, 0u8, 3, 4];

        assert_eq!(agg.on_chunk_result(&first), CandlesChunkResult::Stored);
        assert_eq!(agg.on_chunk_result(&duplicate), CandlesChunkResult::Ignored);
        assert_eq!(agg.on_chunk_result(&bad_index), CandlesChunkResult::Ignored);
        assert_eq!(
            agg.on_chunk_result(&second),
            CandlesChunkResult::Complete(vec![1, 2, 3, 4])
        );
    }

    #[test]
    fn aggregator_accepts_delphi_word_sized_chunk_total() {
        let mut agg = CandlesAggregator::new();
        let mut chunk = Vec::new();
        chunk.extend_from_slice(&0u16.to_le_bytes());
        chunk.extend_from_slice(&65_535u16.to_le_bytes());
        chunk.extend_from_slice(&[1, 2, 3]);
        assert!(agg.on_chunk(&chunk).is_none());
        assert_eq!(agg.progress(), (1, 65_535));
    }

    #[test]
    fn request_candles_data_parser_reads_delphi_zlib_stream() {
        let mut plain = Vec::new();
        plain.extend_from_slice(&0i32.to_le_bytes()); // legacy count for v1 readers
        plain.push(2); // ServerCandlesVersion
        plain.extend_from_slice(&1i32.to_le_bytes()); // market count
        plain.extend_from_slice(&0f64.to_le_bytes()); // TimeShift minutes
        write_delphi_utf16_string(&mut plain, "BTCUSDT");
        plain.extend_from_slice(&2i32.to_le_bytes());
        write_deep_price_pack(&mut plain, 101.0, 99.0, 12.5, 45_000.0);
        write_deep_price_pack(&mut plain, 102.0, 100.0, 13.5, 45_000.5);
        for i in 0i32..4 {
            plain.extend_from_slice(&(10.0 + i as f32).to_le_bytes());
            plain.extend_from_slice(&i.to_le_bytes());
        }
        for i in 0i32..4 {
            plain.extend_from_slice(&(20.0 + i as f32).to_le_bytes());
            plain.extend_from_slice(&(10 + i).to_le_bytes());
        }

        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&plain).unwrap();
        let zipped = encoder.finish().unwrap();

        let markets = parse_request_candles_data_response(&zipped).unwrap();
        assert_eq!(markets.len(), 1);
        assert_eq!(markets[0].market_name, "BTCUSDT");
        assert_eq!(markets[0].candles_5m.len(), 2);
        assert_eq!(markets[0].candles_5m[0].open_p, 101.0);
        assert_eq!(markets[0].candles_5m[0].close_p, 99.0);
        assert_eq!(markets[0].candles_5m[0].vol, 12.5);
        assert_eq!(markets[0].buy_wall[3].vol, 13.0);
        assert_eq!(markets[0].sell_wall[3].count, 13);
    }

    #[test]
    fn request_candles_data_parser_applies_delphi_timezone_shift() {
        let mut plain = Vec::new();
        plain.extend_from_slice(&0i32.to_le_bytes());
        plain.push(2);
        plain.extend_from_slice(&1i32.to_le_bytes());
        plain.extend_from_slice(&60f64.to_le_bytes()); // server TimeShift minutes
        write_delphi_utf16_string(&mut plain, "BTCUSDT");
        plain.extend_from_slice(&1i32.to_le_bytes());
        write_deep_price_pack(&mut plain, 101.0, 99.0, 12.5, 45_000.0);
        for _ in 0..8 {
            plain.extend_from_slice(&0f32.to_le_bytes());
            plain.extend_from_slice(&0i32.to_le_bytes());
        }

        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&plain).unwrap();
        let zipped = encoder.finish().unwrap();

        let markets = parse_request_candles_data_response_with_local_shift(&zipped, 180.0).unwrap();
        let expected = 45_000.0 + (180.0 - 60.0) / MINS_IN_DAY;
        assert!((markets[0].candles_5m[0].time - expected).abs() < f64::EPSILON);
    }

    #[test]
    fn request_candles_data_rejects_impossible_market_count_before_alloc() {
        let mut plain = Vec::new();
        plain.extend_from_slice(&0i32.to_le_bytes());
        plain.push(2);
        plain.extend_from_slice(&i32::MAX.to_le_bytes());
        plain.extend_from_slice(&0f64.to_le_bytes());

        let zipped = zip_plain(&plain);

        assert!(parse_request_candles_data_response(&zipped).is_none());
    }

    #[test]
    fn request_candles_data_partial_parser_keeps_complete_prior_markets() {
        let mut plain = Vec::new();
        plain.extend_from_slice(&0i32.to_le_bytes());
        plain.push(2);
        plain.extend_from_slice(&2i32.to_le_bytes());
        plain.extend_from_slice(&0f64.to_le_bytes());
        write_candles_market(&mut plain, "BTCUSDT", 45_000.0);
        plain.extend_from_slice(&7u16.to_le_bytes());
        plain.extend_from_slice(&('E' as u16).to_le_bytes());

        let zipped = zip_plain(&plain);

        assert!(parse_request_candles_data_response(&zipped).is_none());
        let markets =
            parse_request_candles_data_response_partial_with_local_shift(&zipped, 0.0).unwrap();
        assert_eq!(markets.len(), 1);
        assert_eq!(markets[0].market_name, "BTCUSDT");
        assert_eq!(markets[0].candles_5m.len(), 1);
        assert_eq!(markets[0].candles_5m[0].time, 45_000.0);
        assert_eq!(markets[0].buy_wall[0].vol, 10.0);
        assert_eq!(markets[0].sell_wall[3].count, 13);
    }

    #[test]
    fn request_candles_data_test_writer_wraps_utf16_len_like_delphi() {
        let mut plain = Vec::new();
        plain.extend_from_slice(&0i32.to_le_bytes());
        plain.push(2);
        plain.extend_from_slice(&1i32.to_le_bytes());
        plain.extend_from_slice(&0f64.to_le_bytes());
        write_delphi_utf16_string(&mut plain, &"X".repeat(65_537));
        plain.extend_from_slice(&0i32.to_le_bytes());
        for _ in 0..8 {
            plain.extend_from_slice(&0f32.to_le_bytes());
            plain.extend_from_slice(&0i32.to_le_bytes());
        }

        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&plain).unwrap();
        let zipped = encoder.finish().unwrap();

        let markets = parse_request_candles_data_response(&zipped).unwrap();
        assert_eq!(markets.len(), 1);
        assert_eq!(markets[0].market_name, "X");
        assert!(markets[0].candles_5m.is_empty());
    }

    fn write_delphi_utf16_string(out: &mut Vec<u8>, value: &str) {
        let utf16: Vec<u16> = value.encode_utf16().collect();
        let len = utf16.len() as u16;
        out.extend_from_slice(&len.to_le_bytes());
        for ch in utf16.iter().take(usize::from(len)) {
            out.extend_from_slice(&ch.to_le_bytes());
        }
    }

    fn write_candles_market(out: &mut Vec<u8>, market: &str, time: f64) {
        write_delphi_utf16_string(out, market);
        out.extend_from_slice(&1i32.to_le_bytes());
        write_deep_price_pack(out, 101.0, 99.0, 12.5, time);
        for i in 0i32..4 {
            out.extend_from_slice(&(10.0 + i as f32).to_le_bytes());
            out.extend_from_slice(&i.to_le_bytes());
        }
        for i in 0i32..4 {
            out.extend_from_slice(&(20.0 + i as f32).to_le_bytes());
            out.extend_from_slice(&(10 + i).to_le_bytes());
        }
    }

    fn write_deep_price_pack(out: &mut Vec<u8>, max_p: f32, min_p: f32, vol: f32, time: f64) {
        let wire = WireDeepPricePack {
            max_p: LeF32::new(max_p),
            min_p: LeF32::new(min_p),
            vol: LeF32::new(vol),
            time: LeF64::new(time),
        };
        out.extend_from_slice(wire.as_bytes());
    }

    fn zip_plain(plain: &[u8]) -> Vec<u8> {
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(plain).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn get_coin_card_candles_builder() {
        let raw = get_coin_card_candles("BTCUSDT", DeepHistoryKind::Hour1);
        // Wire: header(11) + Method(1) + MarketName(2+7) + MarketNames count(4) + ParamsSize(4) + Params(1)
        // = 11 + 1 + 9 + 4 + 4 + 1 = 30 bytes
        assert_eq!(raw.len(), 30);
        // Method byte after header (offset 11)
        assert_eq!(raw[11], EngineMethod::GetCoinCardCandles.to_byte());
    }
}
