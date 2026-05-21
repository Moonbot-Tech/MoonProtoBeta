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
//!   `TMarkets.StoreCandlesToZip`.
//!
//! Используй `CandlesAggregator` для сборки chunked responses.

use std::io::Read;

use flate2::read::ZlibDecoder;

use super::engine_api::EngineMethod;
use super::engine_request::build_engine_request_full;

/// Packed `TDeepPrice` (28 bytes). Соответствует Delphi `MarketsU.pas:701-705`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DeepPrice {
    pub open_p:  f32,
    pub close_p: f32,
    pub max_p:   f32,
    pub min_p:   f32,
    pub vol:     f32,
    /// `TDateTime` (Delphi double, дни с 1899-12-30).
    pub time:    f64,
}

pub const DEEP_PRICE_SIZE: usize = 28;

impl DeepPrice {
    /// Прочитать один record из bytes.
    pub fn read_from(data: &[u8], pos: &mut usize) -> Option<Self> {
        if *pos + DEEP_PRICE_SIZE > data.len() { return None; }
        let open_p  = f32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap()); *pos += 4;
        let close_p = f32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap()); *pos += 4;
        let max_p   = f32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap()); *pos += 4;
        let min_p   = f32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap()); *pos += 4;
        let vol     = f32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap()); *pos += 4;
        let time    = f64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap()); *pos += 8;
        Some(Self { open_p, close_p, max_p, min_p, vol, time })
    }

    pub fn write_to(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.open_p.to_le_bytes());
        out.extend_from_slice(&self.close_p.to_le_bytes());
        out.extend_from_slice(&self.max_p.to_le_bytes());
        out.extend_from_slice(&self.min_p.to_le_bytes());
        out.extend_from_slice(&self.vol.to_le_bytes());
        out.extend_from_slice(&self.time.to_le_bytes());
    }
}

/// Packed `TDeepPricePack` inside `RequestCandlesData` stream.
///
/// Delphi writes this compact 20-byte record for each 5m candle and reconstructs
/// `OpenP = MaxP`, `CloseP = MinP` on receive.
pub const DEEP_PRICE_PACK_SIZE: usize = 20;
pub const DEEP_PRICE_PACK_OLD_SIZE: usize = 32;

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
    Min1   = 0, // hk_1m
    Min5   = 1, // hk_5m
    Min30  = 2, // hk_30m
    Hour1  = 3, // hk_1h
    Hour4  = 4, // hk_4h
    Day1   = 5, // hk_1d
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
    if pos + 4 > data.len() { return None; }
    let count_raw = i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
    pos += 4;
    // DoS guard: отрицательный count или count*DEEP_PRICE_SIZE overflow.
    if count_raw < 0 {
        log::warn!(target: "moonproto::candles", "negative count {} rejected", count_raw);
        return None;
    }
    let count = count_raw as usize;
    let required = count.saturating_mul(DEEP_PRICE_SIZE);
    if required > data.len().saturating_sub(pos) {
        log::warn!(target: "moonproto::candles",
            "count={} requires {} bytes but only {} remaining", count, required, data.len() - pos);
        return None;
    }
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        out.push(DeepPrice::read_from(data, &mut pos)?);
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
/// The timezone-shift field is intentionally not applied here. Delphi applies it
/// when mutating local `Markets` state; the public library returns the server
/// stream content and parsed values without depending on process-local timezone.
pub fn parse_request_candles_data_response(zipped_data: &[u8]) -> Option<Vec<RequestCandlesMarket>> {
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

    // TimeShift minutes. Delphi applies a local timezone correction later.
    let _server_time_shift = read_f64(&data, &mut pos)?;

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
        let record_size = if ver >= 2 { DEEP_PRICE_PACK_SIZE } else { DEEP_PRICE_PACK_OLD_SIZE };
        let required = candle_count.checked_mul(record_size)?;
        if required > data.len().saturating_sub(pos) {
            log::warn!(target: "moonproto::candles",
                "RequestCandlesData market {market_name} requires {required} candle bytes, remaining {}",
                data.len().saturating_sub(pos));
            return None;
        }

        let mut candles_5m = Vec::with_capacity(candle_count);
        for _ in 0..candle_count {
            candles_5m.push(if ver >= 2 {
                read_deep_price_pack(&data, &mut pos)?
            } else {
                read_deep_price_pack_old(&data, &mut pos)?
            });
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

fn read_u8(data: &[u8], pos: &mut usize) -> Option<u8> {
    if *pos + 1 > data.len() { return None; }
    let value = data[*pos];
    *pos += 1;
    Some(value)
}

fn read_i32(data: &[u8], pos: &mut usize) -> Option<i32> {
    if *pos + 4 > data.len() { return None; }
    let value = i32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Some(value)
}

fn read_f32(data: &[u8], pos: &mut usize) -> Option<f32> {
    if *pos + 4 > data.len() { return None; }
    let value = f32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Some(value)
}

fn read_f64(data: &[u8], pos: &mut usize) -> Option<f64> {
    if *pos + 8 > data.len() { return None; }
    let value = f64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Some(value)
}

fn read_delphi_utf16_string(data: &[u8], pos: &mut usize) -> Option<String> {
    if *pos + 2 > data.len() { return None; }
    let chars = u16::from_le_bytes(data[*pos..*pos + 2].try_into().unwrap()) as usize;
    *pos += 2;
    let bytes = chars.checked_mul(2)?;
    if *pos + bytes > data.len() { return None; }
    let mut utf16 = Vec::with_capacity(chars);
    for chunk in data[*pos..*pos + bytes].chunks_exact(2) {
        utf16.push(u16::from_le_bytes([chunk[0], chunk[1]]));
    }
    *pos += bytes;
    Some(String::from_utf16_lossy(&utf16))
}

fn read_deep_price_pack(data: &[u8], pos: &mut usize) -> Option<DeepPrice> {
    let max_p = read_f32(data, pos)?;
    let min_p = read_f32(data, pos)?;
    let vol = read_f32(data, pos)?;
    let time = read_f64(data, pos)?;
    Some(DeepPrice {
        open_p: max_p,
        close_p: min_p,
        max_p,
        min_p,
        vol,
        time,
    })
}

fn read_deep_price_pack_old(data: &[u8], pos: &mut usize) -> Option<DeepPrice> {
    let max_p = read_f64(data, pos)? as f32;
    let min_p = read_f64(data, pos)? as f32;
    let vol = read_f64(data, pos)? as f32;
    let time = read_f64(data, pos)?;
    Some(DeepPrice {
        open_p: max_p,
        close_p: min_p,
        max_p,
        min_p,
        vol,
        time,
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
    chunks:   Vec<Option<Vec<u8>>>,
    received: usize,
    total:    usize,
}

impl CandlesAggregator {
    pub fn new() -> Self { Self::default() }

    /// Добавить chunk. Если все чанки собраны — вернуть склеенный буфер и сбросить state.
    /// Wire: `ChunkIndex:u16 + ChunkTotal:u16 + chunk_payload`.
    pub fn on_chunk(&mut self, response_data: &[u8]) -> Option<Vec<u8>> {
        if response_data.len() < 4 { return None; }
        let chunk_index = u16::from_le_bytes([response_data[0], response_data[1]]) as usize;
        let chunk_total = u16::from_le_bytes([response_data[2], response_data[3]]) as usize;
        let payload = &response_data[4..];

        // Delphi stores ChunkTotal as Word and has no additional capacity cap.
        // `chunk_total` is already bounded by u16::MAX by wire format.
        if chunk_total == 0 {
            return None;
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
        }

        // Все ли собраны?
        if self.received == self.total && self.total > 0 {
            let mut merged = Vec::with_capacity(self.chunks.iter()
                .filter_map(|c| c.as_ref().map(|v| v.len())).sum());
            for chunk in self.chunks.drain(..).flatten() {
                merged.extend_from_slice(&chunk);
            }
            self.received = 0;
            self.total = 0;
            return Some(merged);
        }
        None
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
    use flate2::{Compression, write::ZlibEncoder};
    use std::io::Write;

    #[test]
    fn deep_price_size_is_28() {
        assert_eq!(DEEP_PRICE_SIZE, 28);
    }

    #[test]
    fn deep_price_roundtrip() {
        let dp = DeepPrice {
            open_p: 100.0, close_p: 101.5, max_p: 102.0, min_p: 99.5,
            vol: 1234.5, time: 45123.5,
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
    fn coin_card_candles_response_roundtrip() {
        let candles = vec![
            DeepPrice { open_p: 100.0, close_p: 105.0, max_p: 110.0, min_p: 95.0,  vol: 500.0,  time: 45000.0 },
            DeepPrice { open_p: 105.0, close_p: 102.0, max_p: 107.0, min_p: 100.0, vol: 750.0,  time: 45000.04 },
            DeepPrice { open_p: 102.0, close_p: 108.0, max_p: 109.0, min_p: 101.0, vol: 1200.0, time: 45000.08 },
        ];
        // Build response
        let mut buf = Vec::new();
        buf.extend_from_slice(&(candles.len() as i32).to_le_bytes());
        for c in &candles { c.write_to(&mut buf); }
        // Parse
        let parsed = parse_coin_card_candles_response(&buf).unwrap();
        assert_eq!(parsed, candles);
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
        for i in 0..4 {
            plain.extend_from_slice(&(10.0 + i as f32).to_le_bytes());
            plain.extend_from_slice(&(i as i32).to_le_bytes());
        }
        for i in 0..4 {
            plain.extend_from_slice(&(20.0 + i as f32).to_le_bytes());
            plain.extend_from_slice(&((10 + i) as i32).to_le_bytes());
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

    fn write_delphi_utf16_string(out: &mut Vec<u8>, value: &str) {
        let utf16: Vec<u16> = value.encode_utf16().collect();
        out.extend_from_slice(&(utf16.len() as u16).to_le_bytes());
        for ch in utf16 {
            out.extend_from_slice(&ch.to_le_bytes());
        }
    }

    fn write_deep_price_pack(out: &mut Vec<u8>, max_p: f32, min_p: f32, vol: f32, time: f64) {
        out.extend_from_slice(&max_p.to_le_bytes());
        out.extend_from_slice(&min_p.to_le_bytes());
        out.extend_from_slice(&vol.to_le_bytes());
        out.extend_from_slice(&time.to_le_bytes());
    }

    #[test]
    fn get_coin_card_candles_builder() {
        let raw = get_coin_card_candles("BTCUSDT", DeepHistoryKind::Hour1);
        // Wire: header(11) + Method(1) + MarketName(2+7) + MarketNames count(4) + ParamsSize(4) + Params(1)
        // = 11 + 1 + 9 + 4 + 4 + 1 = 30 bytes
        assert_eq!(raw.len(), 30);
        // Method byte after header (offset 11)
        assert_eq!(raw[11], EngineMethod::GetCoinCardCandles as u8);
    }
}
