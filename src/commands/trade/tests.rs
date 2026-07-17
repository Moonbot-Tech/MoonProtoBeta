use super::records::{IMMUNE_ITEM_SIZE, PRICE_ZONE_SIZE};
use super::*;

#[test]
fn stop_settings_wire_layout_matches_delphi_record() {
    let stops = StopSettings {
        stop_loss_on: DelphiBool::TRUE,
        sl_fixed: DelphiBool::FALSE,
        sl_level: 1.25,
        sl_spread: 2.5,
        trailing_on: DelphiBool::TRUE,
        trailing_fixed: DelphiBool::TRUE,
        trailing_level: 3.75,
        ts_spread: 4.5,
        use_take_profit: DelphiBool::FALSE,
        take_profit: 5.125,
        take_profit_changed: DelphiBool::TRUE,
    };

    let mut expected = Vec::new();
    expected.push(1);
    expected.push(0);
    expected.extend_from_slice(&1.25f64.to_le_bytes());
    expected.extend_from_slice(&2.5f64.to_le_bytes());
    expected.push(1);
    expected.push(1);
    expected.extend_from_slice(&3.75f64.to_le_bytes());
    expected.extend_from_slice(&4.5f64.to_le_bytes());
    expected.push(0);
    expected.extend_from_slice(&5.125f64.to_le_bytes());
    expected.push(1);

    let mut encoded = Vec::new();
    stops.write_to(&mut encoded);

    assert_eq!(STOP_SETTINGS_SIZE, 46);
    assert_eq!(encoded, expected);

    let parsed = StopSettings::from_bytes(&expected).expect("valid StopSettings");
    let mut roundtrip = Vec::new();
    parsed.write_to(&mut roundtrip);
    assert_eq!(roundtrip, expected);
}

#[test]
// parity: MoonBot MarketsU.pas:TOrderCompact.AdjustTime
fn order_compact_adjust_time_skips_zero_dates() {
    let mut order = OrderCompact {
        open_time: 0.0,
        close_time: 0.5,
        create_time: 2.0,
        ..OrderCompact::default()
    };

    order.adjust_time(0.25);

    let open_time = order.open_time;
    let close_time = order.close_time;
    let create_time = order.create_time;
    assert_eq!(open_time, 0.0);
    assert_eq!(close_time, 0.5);
    assert_eq!(create_time, 1.75);
}

#[test]
fn order_compact_uses_private_wire_struct() {
    assert_eq!(ORDER_COMPACT_SIZE, 117);

    let order = OrderCompact {
        int_id: -101,
        quantity: 1.25,
        quantity_remaining: 2.5,
        total_btc: 3.75,
        spent_btc: 4.125,
        open_time: 45_000.5,
        close_time: 45_001.25,
        actual_price: 5.5,
        mean_price: -0.0,
        quantity_base: 6.75,
        actual_q: 7.875,
        tmp_btc: 8.25,
        create_time: 45_002.5,
        panic_sell_down: 9.5,
        order_type: OrderType::Buy,
        sub_type: OrderSubType::Stop,
        stop_flag: 3,
        partial_done: 4,
        leverage: 5,
        is_opened: DelphiBool::from_byte(6),
        is_closed: DelphiBool::from_byte(7),
        canceled: DelphiBool::from_byte(8),
        is_short: DelphiBool::from_byte(9),
    };

    let mut expected = Vec::new();
    expected.extend_from_slice(&(-101i64).to_le_bytes());
    expected.extend_from_slice(&1.25f64.to_le_bytes());
    expected.extend_from_slice(&2.5f64.to_le_bytes());
    expected.extend_from_slice(&3.75f64.to_le_bytes());
    expected.extend_from_slice(&4.125f64.to_le_bytes());
    expected.extend_from_slice(&45_000.5f64.to_le_bytes());
    expected.extend_from_slice(&45_001.25f64.to_le_bytes());
    expected.extend_from_slice(&5.5f64.to_le_bytes());
    expected.extend_from_slice(&(-0.0f64).to_le_bytes());
    expected.extend_from_slice(&6.75f64.to_le_bytes());
    expected.extend_from_slice(&7.875f64.to_le_bytes());
    expected.extend_from_slice(&8.25f64.to_le_bytes());
    expected.extend_from_slice(&45_002.5f64.to_le_bytes());
    expected.extend_from_slice(&9.5f32.to_le_bytes());
    expected.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9]);

    let mut encoded = Vec::new();
    order.write_to(&mut encoded);
    assert_eq!(encoded, expected);

    let parsed = OrderCompact::from_bytes(&expected).expect("valid TOrderCompact");
    assert_eq!(parsed.int_id, order.int_id);
    assert_eq!(parsed.quantity, order.quantity);
    assert_eq!(parsed.quantity_remaining, order.quantity_remaining);
    assert_eq!(parsed.total_btc, order.total_btc);
    assert_eq!(parsed.spent_btc, order.spent_btc);
    assert_eq!(parsed.open_time, order.open_time);
    assert_eq!(parsed.close_time, order.close_time);
    assert_eq!(parsed.actual_price, order.actual_price);
    assert_eq!(parsed.mean_price.to_bits(), order.mean_price.to_bits());
    assert_eq!(parsed.quantity_base, order.quantity_base);
    assert_eq!(parsed.actual_q, order.actual_q);
    assert_eq!(parsed.tmp_btc, order.tmp_btc);
    assert_eq!(parsed.create_time, order.create_time);
    assert_eq!(parsed.panic_sell_down, order.panic_sell_down);
    assert_eq!(parsed.order_type, order.order_type);
    assert_eq!(parsed.sub_type, order.sub_type);
    assert_eq!(parsed.stop_flag, order.stop_flag);
    assert_eq!(parsed.partial_done, order.partial_done);
    assert_eq!(parsed.leverage, order.leverage);
    assert_eq!(parsed.is_opened, order.is_opened);
    assert_eq!(parsed.is_closed, order.is_closed);
    assert_eq!(parsed.canceled, order.canceled);
    assert_eq!(parsed.is_short, order.is_short);
}

#[test]
// parity: MoonBot MarketsU.pas:TOrderUpdateData.AdjustTime
fn order_update_data_adjust_time_skips_zero_dates() {
    let mut missing_time = OrderUpdateData {
        open_time: 0.0,
        ..OrderUpdateData::default()
    };
    missing_time.adjust_time(0.25);
    let missing_open_time = missing_time.open_time;
    assert_eq!(missing_open_time, 0.0);

    let mut valid_time = OrderUpdateData {
        open_time: 2.0,
        ..OrderUpdateData::default()
    };
    valid_time.adjust_time(0.25);
    let valid_open_time = valid_time.open_time;
    assert_eq!(valid_open_time, 1.75);
}

#[test]
fn order_update_data_uses_private_wire_struct() {
    assert_eq!(ORDER_UPDATE_DATA_SIZE, 66);

    let data = OrderUpdateData {
        int_id: -123456789,
        actual_price: 1.25,
        open_time: 45_000.5,
        quantity: 2.5,
        quantity_remaining: 3.75,
        actual_q: 4.125,
        total_btc: 5.5,
        mean_price: -0.0,
        partial_done: 7,
        stop_flag: 0xA5,
    };

    let mut expected = Vec::new();
    expected.extend_from_slice(&(-123456789i64).to_le_bytes());
    expected.extend_from_slice(&1.25f64.to_le_bytes());
    expected.extend_from_slice(&45_000.5f64.to_le_bytes());
    expected.extend_from_slice(&2.5f64.to_le_bytes());
    expected.extend_from_slice(&3.75f64.to_le_bytes());
    expected.extend_from_slice(&4.125f64.to_le_bytes());
    expected.extend_from_slice(&5.5f64.to_le_bytes());
    expected.extend_from_slice(&(-0.0f64).to_le_bytes());
    expected.push(7);
    expected.push(0xA5);

    let mut encoded = Vec::new();
    data.write_to(&mut encoded);
    assert_eq!(encoded, expected);

    let parsed = OrderUpdateData::from_bytes(&expected).expect("valid TOrderUpdateData");
    assert_eq!(parsed.int_id, data.int_id);
    assert_eq!(parsed.actual_price, data.actual_price);
    assert_eq!(parsed.open_time, data.open_time);
    assert_eq!(parsed.quantity, data.quantity);
    assert_eq!(parsed.quantity_remaining, data.quantity_remaining);
    assert_eq!(parsed.actual_q, data.actual_q);
    assert_eq!(parsed.total_btc, data.total_btc);
    assert_eq!(parsed.mean_price.to_bits(), data.mean_price.to_bits());
    assert_eq!(parsed.partial_done, data.partial_done);
    assert_eq!(parsed.stop_flag, data.stop_flag);
}

fn minimal_order_status_payload(cmd_id: u8, uid: u64) -> Vec<u8> {
    let mut out = Vec::new();
    write_base_command_header(&mut out, cmd_id, uid);
    out.push(1);
    out.push(2);
    write_string(&mut out, "BTCUSDT");
    out.extend_from_slice(&1u16.to_le_bytes());
    out.push(OrderWorkerStatus::None.to_byte());
    OrderCompact::default().write_to(&mut out);
    OrderCompact::default().write_to(&mut out);
    StopSettings::default().write_to(&mut out);
    out.extend_from_slice(&0u64.to_le_bytes());
    out.push(0);
    out.extend_from_slice(&0i32.to_le_bytes());
    out.push(0);
    out.push(0);
    out.push(0);
    // fe600fd TOrderStatus VStop flags byte (disabled, no Level/Vol tail).
    out.push(0);
    out
}

#[test]
// parity: MoonBot MoonProtoTradeStruct.pas:TOrderStatus VStop tail
fn all_statuses_consumes_vstop_tail_before_next_order() {
    let mut first = minimal_order_status_payload(4, 0x1111);
    *first.last_mut().expect("vstop flags") = 1 | 2 | 4;
    first.extend_from_slice(&12.5f64.to_le_bytes());
    first.extend_from_slice(&3.25f64.to_le_bytes());
    let second = minimal_order_status_payload(4, 0x2222);

    let mut raw = Vec::new();
    write_base_command_header(&mut raw, 8, 7);
    raw.extend_from_slice(&2i32.to_le_bytes());
    raw.extend_from_slice(&first);
    raw.extend_from_slice(&second);

    let TradeCommand::AllStatuses(snapshot) = TradeCommand::parse(&raw).expect("snapshot") else {
        panic!("wrong command")
    };
    assert_eq!(snapshot.orders.len(), 2);
    let TradeCommand::OrderStatus(first) = &snapshot.orders[0] else {
        panic!("first nested command is not OrderStatus")
    };
    assert_eq!(first.epoch_header.market.base.uid, 0x1111);
    assert!(first.vstop_on);
    assert!(first.vstop_fixed);
    assert_eq!(first.vstop_level, 12.5);
    assert_eq!(first.vstop_vol, 3.25);
    let TradeCommand::OrderStatus(second) = &snapshot.orders[1] else {
        panic!("second nested command is not OrderStatus")
    };
    assert_eq!(second.epoch_header.market.base.uid, 0x2222);
    assert!(!second.vstop_on);
}

fn minimal_market_payload(cmd_id: u8) -> Vec<u8> {
    let mut raw = Vec::new();
    write_market_header(&mut raw, cmd_id, 0xAA, "BTCUSDT", 1, 2);
    raw
}

#[test]
// parity: MoonBot MoonProtoTradeStruct.pas:TBaseMarketCommand.CreateFromStream
fn market_header_short_declared_string_rejects() {
    let mut raw = Vec::new();
    write_base_command_header(&mut raw, 23, 0xAA);
    raw.push(1);
    raw.push(2);
    raw.extend_from_slice(&3u16.to_le_bytes());
    raw.extend_from_slice(b"BT");

    assert!(
        TradeCommand::parse(&raw).is_none(),
        "ReadStringFromStreamUtf8 is ReadBuffer-like: declared string bytes must be present"
    );
}

#[test]
// parity: MoonBot MoonProtoTradeStruct.pas:TBaseMarketCommand.CreateFromStream
fn trade_market_fixed_tails_accept_empty_after_string() {
    for cmd_id in [3, 11, 12, 13, 14, 15, 16, 17, 25, 26, 27, 28, 30] {
        let raw = minimal_market_payload(cmd_id);
        assert!(
            TradeCommand::parse(&raw).is_some(),
            "CmdId={cmd_id} fixed tail must zero-tail after a valid market string"
        );
    }
}

#[test]
// parity: MoonBot MoonProtoTradeStruct.pas:TTradeEpochCommand.CreateFromStream
fn trade_epoch_fixed_tails_accept_empty_after_string() {
    for cmd_id in [2, 4, 5, 6, 7, 10, 18, 19, 20, 21, 29] {
        let raw = minimal_market_payload(cmd_id);
        assert!(
            TradeCommand::parse(&raw).is_some(),
            "CmdId={cmd_id} epoch/fixed tail must zero-tail after a valid market string"
        );
    }
}

#[test]
// parity: MoonBot MoonProtoTradeStruct.pas:TOrderStatus.CreateFromStream
fn order_status_zero_tails_partial_order_record() {
    let mut raw = minimal_market_payload(4);
    raw.extend_from_slice(&0xBEEFu16.to_le_bytes());
    raw.push(OrderWorkerStatus::BuySet.to_byte());
    raw.push(0x7A);

    match TradeCommand::parse(&raw).unwrap() {
        TradeCommand::OrderStatus(cmd) => {
            assert_eq!(cmd.epoch_header.epoch, 0xBEEF);
            assert_eq!(cmd.epoch_header.status, OrderWorkerStatus::BuySet);
            assert_eq!(cmd.buy_order.int_id, 0x7A);
            assert_eq!(cmd.sell_order.int_id, 0);
            assert_eq!(cmd.stops.stop_loss_on, DelphiBool::FALSE);
            assert_eq!(cmd.strat_id, 0);
            assert!(!cmd.is_short);
            assert_eq!(cmd.db_id, 0);
            assert!(!cmd.from_cache);
        }
        other => panic!("wrong variant: {other:?}"),
    }
}

#[test]
// parity: MoonBot MoonProtoTradeStruct.pas:TAllStatuses.CreateFromStream
fn all_statuses_dispatches_nested_trade_command_by_cmd_id() {
    let mut raw = Vec::new();
    write_base_command_header(&mut raw, 8, 0xAA);
    raw.extend_from_slice(&1i32.to_le_bytes());
    raw.extend_from_slice(&minimal_market_payload(29));

    let TradeCommand::AllStatuses(snapshot) = TradeCommand::parse(&raw).expect("snapshot") else {
        panic!("wrong command")
    };
    assert!(matches!(
        snapshot.orders.as_slice(),
        [TradeCommand::VStopUpdate(_)]
    ));
}

#[test]
// parity: MoonBot MoonProtoTradeStruct.pas:TAllStatuses.CreateFromStream
fn all_statuses_negative_count_is_empty_snapshot() {
    let mut raw = Vec::new();
    write_base_command_header(&mut raw, 8, 0xAA);
    raw.extend_from_slice(&(-1i32).to_le_bytes());

    match TradeCommand::parse(&raw).unwrap() {
        TradeCommand::AllStatuses(snap) => assert!(snap.orders.is_empty()),
        other => panic!("wrong variant: {other:?}"),
    }
}

#[test]
fn all_statuses_rejects_absurd_count_before_loop() {
    let mut raw = Vec::new();
    write_base_command_header(&mut raw, 8, 0xAA);
    raw.extend_from_slice(&((MAX_ALL_STATUSES_ORDERS as i32) + 1).to_le_bytes());

    assert!(TradeCommand::parse(&raw).is_none());
}

#[test]
// parity: MoonBot MoonProtoTradeStruct.pas:TAllStatuses.CreateFromStream
fn all_statuses_keeps_present_items_when_count_overstates_remaining() {
    let mut raw = Vec::new();
    write_base_command_header(&mut raw, 8, 0xAA);
    raw.extend_from_slice(&2i32.to_le_bytes());
    raw.extend_from_slice(&minimal_order_status_payload(4, 0xBB));

    match TradeCommand::parse(&raw).unwrap() {
        TradeCommand::AllStatuses(snap) => {
            assert_eq!(snap.orders.len(), 1);
            let TradeCommand::OrderStatus(status) = &snap.orders[0] else {
                panic!("wrong nested command")
            };
            assert_eq!(status.epoch_header.market.base.uid, 0xBB);
        }
        other => panic!("wrong variant: {other:?}"),
    }
}

#[test]
// parity: MoonBot MoonProtoTradeStruct.pas:TBulkReplaceNotify.CreateFromStream
fn bulk_replace_notify_keeps_declared_count_with_zero_tail() {
    let mut raw = Vec::new();
    write_base_command_header(&mut raw, 28, 0xAA);
    raw.push(1);
    raw.push(2);
    write_string(&mut raw, "BTCUSDT");
    raw.push(OrderType::Buy.to_byte());
    raw.extend_from_slice(&2u16.to_le_bytes());
    raw.extend_from_slice(&0x1122_3344_5566_7788u64.to_le_bytes());

    match TradeCommand::parse(&raw).unwrap() {
        TradeCommand::BulkReplaceNotify(cmd) => {
            assert_eq!(cmd.uids, vec![0x1122_3344_5566_7788, 0]);
        }
        other => panic!("wrong variant: {other:?}"),
    }
}

#[test]
// parity: MoonBot MoonProtoTradeStruct.pas:TSetImmuneCommand.CreateFromStream
fn set_immune_keeps_declared_count_with_zero_tail() {
    let mut raw = Vec::new();
    write_base_command_header(&mut raw, 22, 0xAA);
    raw.push(2);
    raw.extend_from_slice(&0x1122_3344_5566_7788u64.to_le_bytes());
    raw.push(1);

    match TradeCommand::parse(&raw).unwrap() {
        TradeCommand::SetImmune(cmd) => {
            assert_eq!(cmd.items.len(), 2);
            assert_eq!(cmd.items[0].uid, 0x1122_3344_5566_7788);
            assert!(cmd.items[0].value);
            assert_eq!(cmd.items[1].uid, 0);
            assert!(!cmd.items[1].value);
        }
        other => panic!("wrong variant: {other:?}"),
    }
}

#[test]
fn market_header_invalid_utf8_uses_delphi_question_mark_fallback() {
    let mut raw = Vec::new();
    write_base_command_header(&mut raw, 23, 77);
    raw.push(1);
    raw.push(2);
    raw.extend_from_slice(&3u16.to_le_bytes());
    raw.extend_from_slice(&[b'A', 0xFF, b'B']);

    match TradeCommand::parse(&raw).unwrap() {
        TradeCommand::Penalty(header) => {
            assert_eq!(header.base.uid, 77);
            assert_eq!(header.market_name, "A?B");
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
// parity: MoonBot MoonProtoTradeStruct.pas:TTradeEpochCommand.CreateFromStream
fn trade_epoch_header_preserves_unknown_status_ordinal() {
    let mut raw = Vec::new();
    write_base_command_header(&mut raw, 18, 77);
    raw.push(1);
    raw.push(2);
    write_string(&mut raw, "BTCUSDT");
    raw.extend_from_slice(&123u16.to_le_bytes());
    raw.push(250);

    match TradeCommand::parse(&raw).unwrap() {
        TradeCommand::OrderStatusRequest(header) => {
            assert_eq!(header.market.base.uid, 77);
            assert_eq!(header.epoch, 123);
            assert_eq!(header.status.to_byte(), 250);
            assert!(!header.status.is_known());
            assert_eq!(header.status.name(), "Unknown");
        }
        other => panic!("wrong variant: {other:?}"),
    }
}

#[test]
// parity: MoonBot MoonProtoTradeStruct.pas (TBulkReplaceNotify/TOrderTracePoint CreateFromStream)
fn order_type_fields_preserve_unknown_ordinals() {
    let mut bulk = Vec::new();
    write_base_command_header(&mut bulk, 28, 77);
    bulk.push(1);
    bulk.push(2);
    write_string(&mut bulk, "BTCUSDT");
    bulk.push(250);
    bulk.extend_from_slice(&1u16.to_le_bytes());
    bulk.extend_from_slice(&0x1122_3344_5566_7788u64.to_le_bytes());

    match TradeCommand::parse(&bulk).unwrap() {
        TradeCommand::BulkReplaceNotify(cmd) => {
            assert_eq!(cmd.order_type.to_byte(), 250);
            assert!(!cmd.order_type.is_known());
            assert_eq!(cmd.uids, vec![0x1122_3344_5566_7788]);
        }
        other => panic!("wrong variant: {other:?}"),
    }

    let mut trace = Vec::new();
    write_base_command_header(&mut trace, 25, 88);
    trace.push(1);
    trace.push(2);
    write_string(&mut trace, "BTCUSDT");
    trace.extend_from_slice(&45_000.25f64.to_le_bytes());
    trace.extend_from_slice(&1.5f32.to_le_bytes());
    trace.extend_from_slice(&1.25f32.to_le_bytes());
    trace.extend_from_slice(&0.0f32.to_le_bytes());
    trace.push(251);
    trace.push(trace_flags::IS_INITIAL);

    match TradeCommand::parse(&trace).unwrap() {
        TradeCommand::OrderTracePoint(cmd) => {
            assert_eq!(cmd.ord_type.to_byte(), 251);
            assert!(!cmd.ord_type.is_known());
            assert_eq!(cmd.flags, trace_flags::IS_INITIAL);
        }
        other => panic!("wrong variant: {other:?}"),
    }
}

#[test]
// parity: MoonBot MoonProtoTradeStruct.pas (TMoveAllSellsCommand/TMoveAllBuysCommand CreateFromStream)
fn move_all_commands_preserve_unknown_ordinals() {
    let mut sells = Vec::new();
    write_base_command_header(&mut sells, 13, 77);
    sells.push(1);
    sells.push(2);
    write_string(&mut sells, "BTCUSDT");
    sells.push(250);
    sells.push(251);
    sells.extend_from_slice(&123.0f64.to_le_bytes());
    PriceZone {
        min_p: 1.0,
        max_p: 2.0,
    }
    .write_to(&mut sells);
    sells.push(252);

    match TradeCommand::parse(&sells).unwrap() {
        TradeCommand::MoveAllSells(cmd) => {
            assert_eq!(cmd.cmd_type, 250);
            assert_eq!(cmd.move_kind.to_byte(), 251);
            assert!(!cmd.move_kind.is_known());
            assert_eq!(cmd.side.to_byte(), 252);
            assert!(!cmd.side.is_known());
        }
        other => panic!("wrong variant: {other:?}"),
    }

    let mut buys = Vec::new();
    write_base_command_header(&mut buys, 27, 88);
    buys.push(1);
    buys.push(2);
    write_string(&mut buys, "ETHUSDT");
    buys.push(253);
    buys.push(254);
    buys.extend_from_slice(&456.0f64.to_le_bytes());
    buys.push(255);

    match TradeCommand::parse(&buys).unwrap() {
        TradeCommand::MoveAllBuys(cmd) => {
            assert_eq!(cmd.cmd_type, 253);
            assert_eq!(cmd.move_kind.to_byte(), 254);
            assert!(!cmd.move_kind.is_known());
            assert_eq!(cmd.side.to_byte(), 255);
            assert!(!cmd.side.is_known());
        }
        other => panic!("wrong variant: {other:?}"),
    }
}

#[test]
fn set_immune_wire_layout_matches_delphi_record() {
    assert_eq!(IMMUNE_ITEM_SIZE, 9);

    let payload = build_set_immune(
        0x0102_0304_0506_0708,
        &[ImmuneItem {
            uid: 0x1112_1314_1516_1718,
            value: true,
        }],
    );

    let mut expected = Vec::new();
    expected.push(22);
    expected.extend_from_slice(&CURRENT_PROTO_CMD_VER.to_le_bytes());
    expected.extend_from_slice(&0x0102_0304_0506_0708u64.to_le_bytes());
    expected.push(1);
    expected.extend_from_slice(&0x1112_1314_1516_1718u64.to_le_bytes());
    expected.push(1);

    assert_eq!(payload, expected);
    match TradeCommand::parse(&payload).expect("valid SetImmune") {
        TradeCommand::SetImmune(cmd) => {
            assert_eq!(cmd.header.uid, 0x0102_0304_0506_0708);
            assert_eq!(cmd.items.len(), 1);
            assert_eq!(cmd.items[0].uid, 0x1112_1314_1516_1718);
            assert!(cmd.items[0].value);
        }
        other => panic!("unexpected command: {other:?}"),
    }
}

#[test]
fn set_immune_count_is_written_as_byte_without_clamp() {
    let items: Vec<_> = (0..260u64)
        .map(|uid| ImmuneItem {
            uid,
            value: uid % 2 == 0,
        })
        .collect();

    let payload = build_set_immune(0xAA, &items);

    assert_eq!(payload[11], 4);
    assert_eq!(payload.len(), 11 + 1 + 4 * 9);
    match TradeCommand::parse(&payload).expect("valid SetImmune") {
        TradeCommand::SetImmune(cmd) => {
            assert_eq!(cmd.items.len(), 4);
            assert_eq!(cmd.items[0].uid, 0);
            assert!(cmd.items[0].value);
            assert_eq!(cmd.items[3].uid, 3);
            assert!(!cmd.items[3].value);
        }
        other => panic!("unexpected command: {other:?}"),
    }
}

#[test]
fn order_replace_builder_uses_delphi_client_epoch_header() {
    let ctx = TradeCtx::with_route_bytes(0x0102_0304_0506_0708, 1, 4);
    let payload = build_order_replace(ctx, "BTCUSDT", OrderType::Sell, 50100.25);

    match TradeCommand::parse(&payload).expect("valid OrderReplace") {
        TradeCommand::OrderReplace(cmd) => {
            assert_eq!(cmd.epoch_header.market.base.uid, ctx.uid);
            assert_eq!(cmd.epoch_header.epoch, 0);
            assert_eq!(cmd.epoch_header.status, OrderWorkerStatus::None);
            assert_eq!(cmd.order_type, OrderType::Sell);
            assert_eq!(cmd.new_price, 50100.25);
        }
        other => panic!("unexpected command: {other:?}"),
    }
}

#[test]
fn turn_panic_sell_builder_uses_delphi_client_epoch_header() {
    let ctx = TradeCtx::with_route_bytes(0x1112_1314_1516_1718, 1, 4);
    let payload = build_turn_panic_sell(ctx, "ETHUSDT", true);

    match TradeCommand::parse(&payload).expect("valid TurnPanicSell") {
        TradeCommand::TurnPanicSell(cmd) => {
            assert_eq!(cmd.epoch_header.market.base.uid, ctx.uid);
            assert_eq!(cmd.epoch_header.epoch, 0);
            assert_eq!(cmd.epoch_header.status, OrderWorkerStatus::None);
            assert!(cmd.turn_on);
        }
        other => panic!("unexpected command: {other:?}"),
    }
}

#[test]
// parity: MoonBot MoonProtoTradeStruct.pas:TClosedSellOrderReportCommand.
fn closed_sell_order_report_parses_dbid_and_sql() {
    let mut raw = Vec::new();
    write_base_command_header(&mut raw, 31, 0x1112_1314_1516_1718);
    raw.extend_from_slice(&123456789i64.to_le_bytes());
    write_string(&mut raw, "UPDATE Orders SET Status=1 WHERE ID=123456789");

    match TradeCommand::parse(&raw).expect("valid ClosedSellOrderReport") {
        TradeCommand::ClosedSellOrderReport(report) => {
            assert_eq!(report.header.uid, 0x1112_1314_1516_1718);
            assert_eq!(report.db_id, 123456789);
            assert_eq!(report.sql, "UPDATE Orders SET Status=1 WHERE ID=123456789");
        }
        other => panic!("unexpected command: {other:?}"),
    }
}

#[test]
fn price_zone_uses_private_wire_struct_without_public_endian_wrappers() {
    assert_eq!(PRICE_ZONE_SIZE, 16);

    let zone = PriceZone {
        min_p: 12.5,
        max_p: -0.0,
    };
    let mut bytes = Vec::new();
    zone.write_to(&mut bytes);

    let mut expected = Vec::new();
    expected.extend_from_slice(&12.5f64.to_le_bytes());
    expected.extend_from_slice(&(-0.0f64).to_le_bytes());
    assert_eq!(bytes, expected);

    let parsed = PriceZone::from_bytes(&bytes).expect("valid TPriceZone");
    assert_eq!(parsed.min_p, 12.5);
    assert_eq!(parsed.max_p.to_bits(), (-0.0f64).to_bits());
}
