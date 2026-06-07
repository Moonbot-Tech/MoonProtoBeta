use super::*;

fn watcher_fill_bytes() -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(&(-12i16).to_le_bytes());
    data.extend_from_slice(&123.5f32.to_le_bytes());
    data.extend_from_slice(&(-0.25f32).to_le_bytes());
    data.extend_from_slice(&0.03125f32.to_le_bytes());
    data.extend_from_slice(&4.5f32.to_le_bytes());
    data.push(7);
    data.push(
        (watcher_fill_flags::IS_SHORT | watcher_fill_flags::IS_OPEN | watcher_fill_flags::IS_TAKER)
            .bits(),
    );
    data
}

fn trade_row_bytes(time_delta_ms: i16, a: f32, b: f32) -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(&time_delta_ms.to_le_bytes());
    data.extend_from_slice(&a.to_le_bytes());
    data.extend_from_slice(&b.to_le_bytes());
    data
}

#[test]
fn trades_stream_rows_use_private_wire_structs() {
    assert_eq!(std::mem::size_of::<WireTradesPacketHeader>(), 10);
    assert_eq!(TRADES_PACKET_HEADER_SIZE, 10);
    assert_eq!(std::mem::size_of::<WireTradeRow>(), 10);
    assert_eq!(TRADE_ROW_SIZE, 10);
    assert_eq!(std::mem::size_of::<WireWatcherFill>(), 20);
    assert_eq!(WATCHER_FILL_RECORD_SIZE, 20);

    let mut packet = Vec::new();
    packet.extend_from_slice(&45_000.0f64.to_le_bytes());
    packet.extend_from_slice(&7u16.to_le_bytes());
    let header = read_trades_packet_header(&packet).expect("header");
    assert_eq!(header.base_time.get(), 45_000.0);
    assert_eq!(header.packet_num.get(), 7);

    packet.extend_from_slice(&(-12i16).to_le_bytes());
    packet.extend_from_slice(&123.5f32.to_le_bytes());
    packet.extend_from_slice(&(-0.25f32).to_le_bytes());
    let mut pos = TRADES_PACKET_HEADER_SIZE;
    let row = read_trade_row(&packet, &mut pos).expect("trade row");
    assert_eq!(pos, TRADES_PACKET_HEADER_SIZE + TRADE_ROW_SIZE);
    assert_eq!(row.time_delta_ms.get(), -12);
    assert_eq!(row.a.get(), 123.5);
    assert_eq!(row.b.get(), -0.25);
}

#[test]
fn parse_watcher_fills_decodes_delphi_records() {
    let fills = parse_watcher_fills(&watcher_fill_bytes()).expect("watcher fill");

    assert_eq!(fills.len(), 1);
    let fill = fills[0];
    assert_eq!(fill.time_delta_ms, -12);
    assert_eq!(fill.price, 123.5);
    assert_eq!(fill.qty, -0.25);
    assert_eq!(fill.z_btc, 0.03125);
    assert_eq!(fill.position, 4.5);
    assert_eq!(fill.order_type.to_byte(), 7);
    assert!(!fill.order_type.is_known());
    assert_eq!(
        fill.flags.bits(),
        (watcher_fill_flags::IS_SHORT | watcher_fill_flags::IS_OPEN | watcher_fill_flags::IS_TAKER)
            .bits()
    );
    assert!(fill.is_short());
    assert!(fill.is_open());
    assert!(fill.is_taker());
}

#[test]
fn parse_watcher_fills_rejects_partial_record() {
    let mut data = watcher_fill_bytes();
    data.pop();
    assert!(parse_watcher_fills(&data).is_none());
}

#[test]
fn trades_packet_exposes_typed_watcher_fill_helper() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&45_000.0f64.to_le_bytes());
    payload.extend_from_slice(&42u16.to_le_bytes());
    payload.extend_from_slice(&(0xC000u16 | 5).to_le_bytes());
    payload.push(1); // ExtType WatcherFills
    payload.extend_from_slice(&[0xAB; 20]);
    payload.push(1);
    payload.extend_from_slice(&watcher_fill_bytes());
    payload.push(0); // packet flags

    let packet = parse_trades_packet(&payload).expect("trades packet");
    assert_eq!(packet.packet_num, 42);
    let TradeSection::WatcherFills {
        market_index,
        user,
        data,
    } = &packet.sections[0]
    else {
        panic!("expected watcher fills");
    };
    assert_eq!(*market_index, 5);
    assert_eq!(*user, [0xAB; 20]);
    assert_eq!(data.len(), WATCHER_FILL_RECORD_SIZE);

    let records = packet.sections[0]
        .watcher_fill_records()
        .expect("typed watcher fills");
    assert_eq!(records[0].order_type.to_byte(), 7);
}

#[test]
fn section_iter_decodes_all_section_types_without_collecting_first() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&45_000.0f64.to_le_bytes());
    payload.extend_from_slice(&77u16.to_le_bytes());

    payload.extend_from_slice(&5u16.to_le_bytes()); // futures trades
    payload.push(2);
    payload.extend_from_slice(&trade_row_bytes(1, 100.0, 0.5));
    payload.extend_from_slice(&trade_row_bytes(2, 101.0, -0.25));

    payload.extend_from_slice(&(0x4000u16 | 6).to_le_bytes()); // MMOrders
    payload.push(1);
    payload.extend_from_slice(&trade_row_bytes(3, 12.0, 34.0));
    payload.extend_from_slice(&[0x11; 20]);

    payload.extend_from_slice(&(0x8000u16 | 7).to_le_bytes()); // spot trades
    payload.push(1);
    payload.extend_from_slice(&trade_row_bytes(4, 102.0, 0.75));

    payload.extend_from_slice(&(0xC000u16 | 8).to_le_bytes()); // LiqOrders
    payload.push(0);
    payload.push(1);
    payload.extend_from_slice(&trade_row_bytes(5, 99.0, -1.0));

    payload.extend_from_slice(&(0xC000u16 | 9).to_le_bytes()); // WatcherFills
    payload.push(1);
    payload.extend_from_slice(&[0xAB; 20]);
    payload.push(1);
    payload.extend_from_slice(&watcher_fill_bytes());

    payload.push(TRADES_FLAG_HAS_TAKER);

    let decoded = decode_trades_packet(&payload).expect("decoded packet");
    assert_eq!(decoded.base_time, 45_000.0);
    assert_eq!(decoded.packet_num, 77);

    let mut sections = decoded.sections();
    let TradeSectionRef::Trades(rows) = sections.next().expect("futures section") else {
        panic!("expected futures trades");
    };
    assert_eq!(rows.market_index(), 5);
    assert!(!rows.is_spot());
    assert_eq!(rows.len(), 2);
    let trades: Vec<_> = rows.collect();
    assert_eq!(trades[1].qty, -0.25);

    let TradeSectionRef::MMOrders(rows) = sections.next().expect("mm section") else {
        panic!("expected mm orders");
    };
    assert_eq!(rows.market_index(), 6);
    let orders: Vec<_> = rows.collect();
    assert_eq!(orders[0].taker, Some([0x11; 20]));

    let TradeSectionRef::Trades(rows) = sections.next().expect("spot section") else {
        panic!("expected spot trades");
    };
    assert_eq!(rows.market_index(), 7);
    assert!(rows.is_spot());
    assert_eq!(rows.collect::<Vec<_>>()[0].price, 102.0);

    let TradeSectionRef::LiqOrders(rows) = sections.next().expect("liq section") else {
        panic!("expected liq orders");
    };
    assert_eq!(rows.market_index(), 8);
    assert_eq!(rows.collect::<Vec<_>>()[0].qty, -1.0);

    let TradeSectionRef::WatcherFills {
        market_index,
        user,
        data,
    } = sections.next().expect("watcher section")
    else {
        panic!("expected watcher fills");
    };
    assert_eq!(market_index, 9);
    assert_eq!(user, [0xAB; 20]);
    assert_eq!(data.len(), WATCHER_FILL_RECORD_SIZE);
    assert!(sections.next().is_none());
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:TMoonProtoEngine.ProcessTradesStream
fn section_iter_keeps_only_complete_rows_from_truncated_tail() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&45_000.0f64.to_le_bytes());
    payload.extend_from_slice(&78u16.to_le_bytes());
    payload.extend_from_slice(&5u16.to_le_bytes());
    payload.push(2);
    payload.extend_from_slice(&trade_row_bytes(1, 100.0, 0.5));
    payload.push(0xEE); // one byte of the second row; not enough for a row or next section
    payload.push(0);

    let decoded = decode_trades_packet(&payload).expect("decoded packet");
    let TradeSectionRef::Trades(rows) = decoded.sections().next().expect("trades section") else {
        panic!("expected trades");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows.collect::<Vec<_>>()[0].price, 100.0);

    let owned = parse_trades_packet(&payload).expect("owned packet");
    let TradeSection::Trades(trades) = &owned.sections[0] else {
        panic!("expected owned trades");
    };
    assert_eq!(trades.len(), 1);
}

#[test]
fn section_iter_consumes_truncated_declared_rows_instead_of_reparsing_tail() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&45_000.0f64.to_le_bytes());
    payload.extend_from_slice(&79u16.to_le_bytes());
    payload.extend_from_slice(&5u16.to_le_bytes());
    payload.push(2);
    payload.extend_from_slice(&trade_row_bytes(1, 100.0, 0.5));
    payload.extend_from_slice(&[0, 0, 0]); // malformed tail of declared row #2.
    payload.push(0);

    let decoded = decode_trades_packet(&payload).expect("decoded packet");
    let mut sections = decoded.sections();
    let TradeSectionRef::Trades(rows) = sections.next().expect("trades section") else {
        panic!("expected trades");
    };
    assert_eq!(rows.collect::<Vec<_>>().len(), 1);
    assert!(
            sections.next().is_none(),
            "Delphi reaches stream end while reading the declared rows; the partial tail must not become a fake empty section"
        );

    let owned = parse_trades_packet(&payload).expect("owned packet");
    assert_eq!(owned.sections.len(), 1);
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:TMoonProtoEngine.ProcessTradesStream
fn section_iter_stops_on_unknown_ext_type() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&45_000.0f64.to_le_bytes());
    payload.extend_from_slice(&80u16.to_le_bytes());

    payload.extend_from_slice(&5u16.to_le_bytes());
    payload.push(1);
    payload.extend_from_slice(&trade_row_bytes(1, 100.0, 0.5));

    payload.extend_from_slice(&(0xC000u16 | 6).to_le_bytes());
    payload.push(99); // unknown ExtType: Delphi logs and exits ProcessTradesStream.

    payload.extend_from_slice(&7u16.to_le_bytes());
    payload.push(1);
    payload.extend_from_slice(&trade_row_bytes(2, 101.0, 0.75));
    payload.push(0);

    let decoded = decode_trades_packet(&payload).expect("decoded packet");
    let mut sections = decoded.sections();
    let TradeSectionRef::Trades(rows) = sections.next().expect("first section") else {
        panic!("expected first trades section");
    };
    assert_eq!(rows.collect::<Vec<_>>().len(), 1);
    assert!(
            sections.next().is_none(),
            "Delphi exits on an unknown extended section; bytes after it must not be parsed as another section"
        );

    let owned = parse_trades_packet(&payload).expect("owned packet");
    assert_eq!(owned.sections.len(), 1);
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:TMoonProtoEngine.ProcessTradesStream
fn section_iter_stops_on_truncated_watcher_fills() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&45_000.0f64.to_le_bytes());
    payload.extend_from_slice(&81u16.to_le_bytes());

    payload.extend_from_slice(&5u16.to_le_bytes());
    payload.push(1);
    payload.extend_from_slice(&trade_row_bytes(1, 100.0, 0.5));

    payload.extend_from_slice(&(0xC000u16 | 9).to_le_bytes());
    payload.push(1); // ExtType WatcherFills
    payload.extend_from_slice(&[0xAB; 20]);
    payload.push(2); // declared two 20-byte records
    payload.extend_from_slice(&watcher_fill_bytes());
    payload.extend_from_slice(&[0xEE; 3]); // partial second record
    payload.push(0);

    let decoded = decode_trades_packet(&payload).expect("decoded packet");
    let mut sections = decoded.sections();
    let TradeSectionRef::Trades(rows) = sections.next().expect("first section") else {
        panic!("expected first trades section");
    };
    assert_eq!(rows.collect::<Vec<_>>().len(), 1);
    assert!(
            sections.next().is_none(),
            "Delphi reaches stream end while reading watcher-fill rows; the partial tail must not become another section"
        );

    let owned = parse_trades_packet(&payload).expect("owned packet");
    assert_eq!(owned.sections.len(), 1);
}
