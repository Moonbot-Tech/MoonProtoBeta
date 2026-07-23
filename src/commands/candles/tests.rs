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
        open: 100.0,
        close: 101.5,
        high: 102.0,
        low: 99.5,
        volume: 1234.5,
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
fn candle_timeframe_state_wire_matches_delphi_layout() {
    let mut payload = Vec::new();
    payload.push(4);
    payload.extend_from_slice(&CURRENT_PROTO_CMD_VER.to_le_bytes());
    payload.extend_from_slice(&0x1122_3344_5566_7788u64.to_le_bytes());
    payload.extend_from_slice(&321u16.to_le_bytes());
    payload.push((-1i8) as u8);
    payload.extend_from_slice(&12345i32.to_le_bytes());

    let parsed =
        parse_candle_timeframe_state_command(&payload).expect("valid TCandleTFStateCommand");
    assert_eq!(payload.len(), 18);
    assert_eq!(parsed.uid, 0x1122_3344_5566_7788);
    assert_eq!(parsed.market_index, 321);
    assert_eq!(parsed.timeframe, -1);
    assert_eq!(parsed.revision, 12345);
}

#[test]
fn candle_timeframe_state_rejects_newer_command_version() {
    let mut payload = vec![4];
    payload.extend_from_slice(&(CURRENT_PROTO_CMD_VER + 1).to_le_bytes());
    payload.extend_from_slice(&[0; 15]);
    assert!(parse_candle_timeframe_state_command(&payload).is_none());
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
    assert_eq!(parsed.open, 101.0);
    assert_eq!(parsed.close.to_bits(), (-0.0f32).to_bits());
    assert_eq!(parsed.high, 101.0);
    assert_eq!(parsed.low.to_bits(), (-0.0f32).to_bits());
    assert_eq!(parsed.volume, 12.5);
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
    assert_eq!(parsed.open, 101.0);
    assert_eq!(parsed.close, 99.5);
    assert_eq!(parsed.high, 101.0);
    assert_eq!(parsed.low, 99.5);
    assert_eq!(parsed.volume, 12.5);
    assert_eq!(parsed.time, 45_000.25);
}

#[test]
fn coin_card_candles_response_roundtrip() {
    let candles = vec![
        DeepPrice {
            open: 100.0,
            close: 105.0,
            high: 110.0,
            low: 95.0,
            volume: 500.0,
            time: 45000.0,
        },
        DeepPrice {
            open: 105.0,
            close: 102.0,
            high: 107.0,
            low: 100.0,
            volume: 750.0,
            time: 45000.04,
        },
        DeepPrice {
            open: 102.0,
            close: 108.0,
            high: 109.0,
            low: 101.0,
            volume: 1200.0,
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
    assert_eq!(parsed[0].open, 101.5);
    assert_eq!(parsed[0].close, 0.0);
    assert_eq!(parsed[0].high, 0.0);
    assert_eq!(parsed[0].low, 0.0);
    assert_eq!(parsed[0].volume, 0.0);
    assert_eq!(parsed[0].time, 0.0);
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:TMoonProtoEngine.getDeepHistory
fn coin_card_candles_response_zero_fills_missing_records() {
    let first = DeepPrice {
        open: 100.0,
        close: 105.0,
        high: 110.0,
        low: 95.0,
        volume: 500.0,
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
            open: 0.0,
            close: 0.0,
            high: 0.0,
            low: 0.0,
            volume: 0.0,
            time: 0.0,
        }
    );
}

#[test]
fn coin_card_candles_rejects_absurd_count_before_alloc() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&((MAX_COIN_CARD_CANDLES + 1) as i32).to_le_bytes());

    assert!(parse_coin_card_candles_response(&bytes).is_none());
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
    // Total=3 chunks. Sent out of order.
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
    // Merge order = idx 0, 1, 2 (by position in the array, not by arrival order).
    assert_eq!(merged, vec![10, 11, 20, 21, 30, 31]);
}

#[test]
fn aggregator_duplicate_chunk_ignored() {
    let mut agg = CandlesAggregator::new();
    // Send the same chunk twice.
    let chunk = vec![0u8, 0u8, 2u8, 0u8, 1, 2];
    assert!(agg.on_chunk(&chunk).is_none());
    assert_eq!(agg.progress(), (1, 2));
    assert!(agg.on_chunk(&chunk).is_none()); // duplicate — ignored
    assert_eq!(agg.progress(), (1, 2));
    // Send the second chunk
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
fn aggregator_rejects_payload_above_domain_cap_before_copying_more() {
    let mut agg = CandlesAggregator::with_max_payload_bytes(3);
    let chunk = vec![0u8, 0u8, 1u8, 0u8, 1, 2, 3, 4];

    assert_eq!(agg.on_chunk_result(&chunk), CandlesChunkResult::Ignored);
    assert_eq!(agg.progress(), (0, 0));
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
    assert_eq!(markets[0].candles_5m[0].open, 101.0);
    assert_eq!(markets[0].candles_5m[0].close, 99.0);
    assert_eq!(markets[0].candles_5m[0].volume, 12.5);
    assert_eq!(markets[0].buy_wall[3].volume, 13.0);
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
fn request_candles_data_public_parser_converts_server_local_candles_to_utc() {
    let now = crate::MoonTime::now();
    let server_shift_minutes = 180.0;
    let server_local_days = now.to_delphi_days() + server_shift_minutes / MINS_IN_DAY;

    let mut plain = Vec::new();
    plain.extend_from_slice(&0i32.to_le_bytes());
    plain.push(2);
    plain.extend_from_slice(&1i32.to_le_bytes());
    plain.extend_from_slice(&server_shift_minutes.to_le_bytes());
    write_delphi_utf16_string(&mut plain, "BTCUSDT");
    plain.extend_from_slice(&1i32.to_le_bytes());
    write_deep_price_pack(&mut plain, 101.0, 99.0, 12.5, server_local_days);
    for _ in 0..8 {
        plain.extend_from_slice(&0f32.to_le_bytes());
        plain.extend_from_slice(&0i32.to_le_bytes());
    }

    let markets = parse_request_candles_data_response(&zip_plain(&plain)).unwrap();
    let candle_time = markets[0].candles_5m[0].time();
    let age_ms = crate::MoonTime::now().unix_millis() - candle_time.unix_millis();
    assert!(
        age_ms.abs() < 60_000,
        "public parser must expose UTC MoonTime, not server/client-local Delphi time; age_ms={age_ms}"
    );
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
fn request_candles_data_rejects_market_count_above_domain_cap_before_alloc() {
    let mut plain = Vec::new();
    plain.extend_from_slice(&0i32.to_le_bytes());
    plain.push(2);
    plain.extend_from_slice(&((MAX_REQUEST_CANDLES_MARKETS + 1) as i32).to_le_bytes());
    plain.extend_from_slice(&0f64.to_le_bytes());

    let zipped = zip_plain(&plain);

    assert!(parse_request_candles_data_response(&zipped).is_none());
}

#[test]
fn request_candles_data_rejects_candle_count_above_domain_cap_before_alloc() {
    let mut plain = Vec::new();
    plain.extend_from_slice(&0i32.to_le_bytes());
    plain.push(2);
    plain.extend_from_slice(&1i32.to_le_bytes());
    plain.extend_from_slice(&0f64.to_le_bytes());
    write_delphi_utf16_string(&mut plain, "BTCUSDT");
    plain.extend_from_slice(&((MAX_REQUEST_CANDLES_PER_MARKET + 1) as i32).to_le_bytes());
    plain.extend_from_slice(&[0u8; 64]);

    let zipped = zip_plain(&plain);

    assert!(parse_request_candles_data_response(&zipped).is_none());
}

#[test]
fn request_candles_data_partial_stops_on_candle_count_cap_after_prior_market() {
    let mut plain = Vec::new();
    plain.extend_from_slice(&0i32.to_le_bytes());
    plain.push(2);
    plain.extend_from_slice(&2i32.to_le_bytes());
    plain.extend_from_slice(&0f64.to_le_bytes());
    write_candles_market(&mut plain, "BTCUSDT", 45_000.0);
    write_delphi_utf16_string(&mut plain, "ETHUSDT");
    plain.extend_from_slice(&((MAX_REQUEST_CANDLES_PER_MARKET + 1) as i32).to_le_bytes());
    plain.extend_from_slice(&[0u8; 64]);

    let zipped = zip_plain(&plain);

    assert!(parse_request_candles_data_response(&zipped).is_none());
    let markets =
        parse_request_candles_data_response_partial_with_local_shift(&zipped, 0.0).unwrap();
    assert_eq!(markets.len(), 1);
    assert_eq!(markets[0].market_name, "BTCUSDT");
    assert_eq!(markets[0].candles_5m.len(), 1);
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
    assert_eq!(markets[0].buy_wall[0].volume, 10.0);
    assert_eq!(markets[0].sell_wall[3].count, 13);
}

#[test]
fn request_candles_data_partial_parser_keeps_current_market_when_wall_tail_is_short() {
    let mut plain = Vec::new();
    plain.extend_from_slice(&0i32.to_le_bytes());
    plain.push(2);
    plain.extend_from_slice(&1i32.to_le_bytes());
    plain.extend_from_slice(&0f64.to_le_bytes());
    write_delphi_utf16_string(&mut plain, "BTCUSDT");
    plain.extend_from_slice(&1i32.to_le_bytes());
    write_deep_price_pack(&mut plain, 101.0, 99.0, 12.5, 45_000.0);
    // Delphi has already applied Deep5m at this point. A short wall tail
    // must not cancel the current market's deterministic candle state.
    plain.extend_from_slice(&10.0f32.to_le_bytes()[..2]);

    let zipped = zip_plain(&plain);

    assert!(parse_request_candles_data_response(&zipped).is_none());
    let markets =
        parse_request_candles_data_response_partial_with_local_shift(&zipped, 0.0).unwrap();
    assert_eq!(markets.len(), 1);
    assert_eq!(markets[0].market_name, "BTCUSDT");
    assert_eq!(markets[0].candles_5m.len(), 1);
    assert_eq!(markets[0].candles_5m[0].time, 45_000.0);
    assert_eq!(markets[0].candles_5m[0].volume, 12.5);
    assert_eq!(markets[0].buy_wall[0].count, 0);
    assert_eq!(markets[0].sell_wall[3], WallItem::default());
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessApiCommand (emk_RequestCandlesData)
fn request_candles_data_writer_wraps_utf16_len() {
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

fn write_deep_price_pack(out: &mut Vec<u8>, high: f32, low: f32, volume: f32, time: f64) {
    let wire = WireDeepPricePack {
        high: LeF32::new(high),
        low: LeF32::new(low),
        volume: LeF32::new(volume),
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

#[test]
fn subscribe_candles_builder_matches_engine_request_layout() {
    let raw = subscribe_candles(&["BTCUSDT", "ETHUSDT"], DeepHistoryKind::Hour4);
    assert_eq!(raw[0], 2);
    assert_eq!(raw[11], EngineMethod::SubscribeCandles.to_byte());

    let mut pos = 12usize;
    let market_len = u16::from_le_bytes(raw[pos..pos + 2].try_into().unwrap()) as usize;
    pos += 2 + market_len;
    assert_eq!(market_len, 0, "SubscribeCandles uses empty MarketName");

    let count = i32::from_le_bytes(raw[pos..pos + 4].try_into().unwrap());
    pos += 4;
    assert_eq!(count, 2);
    for expected in ["BTCUSDT", "ETHUSDT"] {
        let len = u16::from_le_bytes(raw[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        assert_eq!(&raw[pos..pos + len], expected.as_bytes());
        pos += len;
    }

    let params_size = i32::from_le_bytes(raw[pos..pos + 4].try_into().unwrap());
    pos += 4;
    assert_eq!(params_size, 1);
    assert_eq!(raw[pos], DeepHistoryKind::Hour4.to_byte());
    assert_eq!(pos + 1, raw.len());
}

#[test]
fn unsubscribe_candles_builder_has_no_params() {
    let raw = unsubscribe_candles(&["BTCUSDT"]);
    assert_eq!(raw[0], 2);
    assert_eq!(raw[11], EngineMethod::UnsubscribeCandles.to_byte());

    let mut pos = 12usize;
    let market_len = u16::from_le_bytes(raw[pos..pos + 2].try_into().unwrap()) as usize;
    pos += 2 + market_len;
    assert_eq!(market_len, 0);
    let count = i32::from_le_bytes(raw[pos..pos + 4].try_into().unwrap());
    pos += 4;
    assert_eq!(count, 1);
    let len = u16::from_le_bytes(raw[pos..pos + 2].try_into().unwrap()) as usize;
    pos += 2 + len;
    let params_size = i32::from_le_bytes(raw[pos..pos + 4].try_into().unwrap());
    assert_eq!(params_size, 0);
    assert_eq!(pos + 4, raw.len());
}

#[test]
fn candle_update_command_parser_matches_delphi_wire() {
    let mut raw = Vec::new();
    raw.push(3);
    raw.extend_from_slice(&crate::commands::registry::CURRENT_PROTO_CMD_VER.to_le_bytes());
    raw.extend_from_slice(&777u64.to_le_bytes());
    raw.extend_from_slice(&42u16.to_le_bytes());
    raw.push(DeepHistoryKind::Min30.to_byte());
    raw.extend_from_slice(&100.0f32.to_le_bytes());
    raw.extend_from_slice(&105.0f32.to_le_bytes());
    raw.extend_from_slice(&110.0f32.to_le_bytes());
    raw.extend_from_slice(&95.0f32.to_le_bytes());
    raw.extend_from_slice(&1234.5f32.to_le_bytes());
    raw.extend_from_slice(&45_123.25f64.to_le_bytes());

    let parsed = parse_candle_update_command(&raw).unwrap();
    assert_eq!(parsed.uid, 777);
    assert_eq!(parsed.market_index, 42);
    assert_eq!(parsed.kind, DeepHistoryKind::Min30);
    assert_eq!(parsed.candle.open(), 100.0);
    assert_eq!(parsed.candle.close(), 105.0);
    assert_eq!(parsed.candle.high(), 110.0);
    assert_eq!(parsed.candle.low(), 95.0);
    assert_eq!(parsed.candle.volume(), 1234.5);
    assert_eq!(parsed.candle.time, 45_123.25);
}
