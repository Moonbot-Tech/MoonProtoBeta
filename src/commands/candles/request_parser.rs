//! Chunked `emk_RequestCandlesData` response parser.

use super::{
    current_local_time_shift_minutes, read_zero_tail, DeepPrice, RequestCandlesMarket, WallItem,
    WireDeepPricePack, WireDeepPricePackOld, DEEP_PRICE_PACK_OLD_SIZE, DEEP_PRICE_PACK_SIZE,
    MINS_IN_DAY, REQUEST_CANDLES_MARKET_MIN_SIZE,
};
use flate2::read::ZlibDecoder;
use std::io::Read;
use zerocopy::FromBytes;
/// Parse merged `emk_RequestCandlesData` bytes.
///
/// Input is the concatenated chunk payload returned by [`super::CandlesAggregator`].
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

pub(crate) fn parse_request_candles_data_response_with_local_shift(
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

pub(crate) fn parse_request_candles_data_response_partial_with_local_shift(
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

fn read_u8(data: &[u8], pos: &mut usize) -> Option<u8> {
    if *pos + 1 > data.len() {
        return None;
    }
    let value = data[*pos];
    *pos += 1;
    Some(value)
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

pub(crate) fn read_deep_price_pack(data: &[u8], pos: &mut usize) -> Option<DeepPrice> {
    if *pos + DEEP_PRICE_PACK_SIZE > data.len() {
        return None;
    }
    let wire = WireDeepPricePack::read_from_bytes(&data[*pos..*pos + DEEP_PRICE_PACK_SIZE]).ok()?;
    *pos += DEEP_PRICE_PACK_SIZE;
    let high = wire.high.get();
    let low = wire.low.get();
    Some(DeepPrice {
        open: high,
        close: low,
        high,
        low,
        volume: wire.volume.get(),
        time: wire.time.get(),
    })
}

pub(crate) fn read_deep_price_pack_old(data: &[u8], pos: &mut usize) -> Option<DeepPrice> {
    if *pos + DEEP_PRICE_PACK_OLD_SIZE > data.len() {
        return None;
    }
    let wire =
        WireDeepPricePackOld::read_from_bytes(&data[*pos..*pos + DEEP_PRICE_PACK_OLD_SIZE]).ok()?;
    *pos += DEEP_PRICE_PACK_OLD_SIZE;
    let high = wire.high.get() as f32;
    let low = wire.low.get() as f32;
    Some(DeepPrice {
        open: high,
        close: low,
        high,
        low,
        volume: wire.volume.get() as f32,
        time: wire.time.get(),
    })
}

fn read_wall_data(data: &[u8], pos: &mut usize) -> Option<[WallItem; 4]> {
    let mut out = [WallItem::default(); 4];
    for item in &mut out {
        item.volume = read_f32(data, pos)?;
        item.count = read_i32(data, pos)?;
    }
    Some(out)
}

fn read_wall_data_zero_tail(data: &[u8], pos: &mut usize) -> [WallItem; 4] {
    let mut out = [WallItem::default(); 4];
    for item in &mut out {
        item.volume = f32::from_le_bytes(read_zero_tail::<4>(data, pos));
        item.count = i32::from_le_bytes(read_zero_tail::<4>(data, pos));
    }
    out
}
