use super::*;
use crate::commands::market::{BaseCurrency, ExchangeCode};

fn make_base(uid: u64, ver: u16) -> BaseCommandHeader {
    BaseCommandHeader {
        cmd_id: 4,
        ver,
        uid,
    }
}

fn make_market(uid: u64, ver: u16, market_name: &str) -> MarketCommandHeader {
    MarketCommandHeader {
        base: make_base(uid, ver),
        currency: 1,
        platform: 4,
        market_name: market_name.to_string(),
    }
}

fn make_epoch(
    uid: u64,
    ver: u16,
    market: &str,
    epoch: u16,
    status: OrderWorkerStatus,
) -> TradeEpochHeader {
    TradeEpochHeader {
        market: make_market(uid, ver, market),
        epoch,
        status,
    }
}

fn make_status(uid: u64, market: &str, status: OrderWorkerStatus, epoch: u16) -> OrderStatus {
    OrderStatus {
        epoch_header: make_epoch(uid, 3, market, epoch, status),
        buy_order: OrderCompact::default(),
        sell_order: OrderCompact::default(),
        stops: StopSettings::default(),
        strat_id: 0,
        is_short: false,
        db_id: 0,
        from_cache: false,
        emulator_mode: false,
        immune_for_clicks: false,
    }
}

fn order_status_cmd(status: OrderStatus) -> TradeCommand {
    TradeCommand::OrderStatus(Box::new(status))
}

fn trace_point(
    uid: u64,
    order_type: OrderType,
    flags: u8,
    time: f64,
    price: f32,
    base: f32,
    stop: f32,
) -> OrderTracePoint {
    OrderTracePoint {
        market: make_market(uid, 3, "BTCUSDT"),
        trace_time: time,
        trace_price: price,
        base_price: base,
        stop_price: stop,
        ord_type: order_type,
        flags,
    }
}

#[test]
fn sell_reason_descriptions_match_delphi_sell_reason_code_to_str() {
    let cases = [
        (SellReason::Unknown, "Unknown"),
        (SellReason::SellPrice, "Sell Price"),
        (SellReason::AutoPriceDown, "Auto Price Down"),
        (SellReason::SellLevel, "Sell Level"),
        (SellReason::SellSpread, "SellSpread"),
        (SellReason::SellShot, "SellShot"),
        (SellReason::PanicSell, "PanicSell"),
        (SellReason::StopLoss, "StopLoss"),
        (SellReason::Trailing, "Trailing"),
        (SellReason::MarketStop, "Market Stop"),
        (SellReason::ManualSell, "Manual Sell"),
        (SellReason::JoinedSell, "JoinedSell"),
        (SellReason::SellFromAssets, "SellFromAssets"),
        (SellReason::BvSvStop, "BV/SV Stop"),
        (SellReason::TakeProfit, "TakeProfit"),
    ];

    for (reason, expected) in cases {
        assert_eq!(reason.description(), expected);
    }
}

fn order_replace_response_cmd(response: OrderReplaceResponse) -> TradeCommand {
    TradeCommand::OrderReplaceResponse(Box::new(response))
}

#[test]
fn terminal_status_marks_done_then_deferred_removal() {
    let mut orders = Orders::new();
    let s1 = make_status(42, "BTCUSDT", OrderWorkerStatus::BuySet, 1);
    let (res, ev) = orders.apply(order_status_cmd(s1));
    assert_eq!(res, ApplyResult::Applied);
    assert!(matches!(ev, OrderEvent::Created(42)));
    assert!(orders.get(42).is_some());

    let s2 = make_status(42, "BTCUSDT", OrderWorkerStatus::SellDone, 1);
    let (_, ev) = orders.apply(order_status_cmd(s2));
    assert!(matches!(ev, OrderEvent::Updated(42)));
    assert!(orders.get(42).unwrap().job_is_done);

    let removed = orders.drain_pending_removals();
    assert_eq!(removed, vec![42]);
    assert!(orders.get(42).is_none());
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder
fn from_cache_status_does_not_create_unknown_order() {
    let mut orders = Orders::new();
    let mut cached = make_status(42, "BTCUSDT", OrderWorkerStatus::BuySet, 1);
    cached.from_cache = true;

    let (res, ev) = orders.apply(order_status_cmd(cached));

    assert_eq!(res, ApplyResult::OrderNotFound);
    assert!(matches!(
        ev,
        OrderEvent::Ignored {
            uid: 42,
            reason: ApplyResult::OrderNotFound
        }
    ));
    assert!(orders.get(42).is_none());
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder
fn existing_full_status_keeps_worker_identity_fields() {
    let mut orders = Orders::new();
    let mut first = make_status(42, "BTCUSDT", OrderWorkerStatus::BuySet, 1);
    first.epoch_header.market.currency = 1;
    first.epoch_header.market.platform = 4;
    first.strat_id = 11;
    first.is_short = false;
    first.db_id = 101;
    first.from_cache = false;
    first.emulator_mode = false;
    first.buy_order.actual_price = 10.0;
    first.stops.sl_level = 1.0;
    first.immune_for_clicks = false;
    orders.apply(order_status_cmd(first));

    let mut second = make_status(42, "ETHUSDT", OrderWorkerStatus::BuySet, 2);
    second.epoch_header.market.currency = 9;
    second.epoch_header.market.platform = 8;
    second.strat_id = 22;
    second.is_short = true;
    second.db_id = 202;
    second.from_cache = true;
    second.emulator_mode = true;
    second.buy_order.actual_price = 25.0;
    second.stops.sl_level = 2.0;
    second.immune_for_clicks = true;

    let (res, ev) = orders.apply(order_status_cmd(second));

    assert_eq!(res, ApplyResult::Applied);
    assert!(matches!(ev, OrderEvent::Updated(42)));
    let order = orders.get(42).unwrap();
    assert_eq!(order.market_name, "BTCUSDT");
    assert_eq!(order.currency, BaseCurrency::USDT);
    assert_eq!(order.platform, ExchangeCode::FBinance);
    assert_eq!(order.strat_id, 11);
    assert!(!order.is_short);
    assert_eq!(order.db_id, 101);
    assert!(!order.from_cache);
    assert!(!order.emulator_mode);
    assert_eq!(order.buy_order.actual_price, 25.0);
    assert_eq!(order.stops.sl_level, 2.0);
    assert!(order.immune_for_clicks);
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder
fn incoming_set_immune_is_not_applied_by_process_command_order() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        42,
        "BTCUSDT",
        OrderWorkerStatus::BuySet,
        1,
    )));

    let set_immune = SetImmuneCommand {
        header: make_base(777, 3),
        items: vec![ImmuneItem {
            uid: 42,
            value: true,
        }],
    };
    let (res, ev) = orders.apply(TradeCommand::SetImmune(set_immune));

    assert_eq!(res, ApplyResult::NotApplicable);
    assert!(matches!(
        ev,
        OrderEvent::Ignored {
            uid: 777,
            reason: ApplyResult::NotApplicable
        }
    ));
    assert!(!orders.get(42).unwrap().immune_for_clicks);
}

#[test]
// parity: MoonBot MoonProtoServer.pas:TMoonProtoNetServer.ProcessCommandOrder (TSetImmuneCommand)
fn outgoing_set_immune_clicks_mutates_only_found_active_orders() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        42,
        "BTCUSDT",
        OrderWorkerStatus::BuySet,
        1,
    )));
    orders.apply(order_status_cmd(make_status(
        43,
        "BTCUSDT",
        OrderWorkerStatus::SellDone,
        1,
    )));

    let applied = orders.set_immune_clicks(&[
        ImmuneItem {
            uid: 42,
            value: true,
        },
        ImmuneItem {
            uid: 43,
            value: true,
        },
        ImmuneItem {
            uid: 44,
            value: true,
        },
    ]);

    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].uid, 42);
    assert!(orders.get(42).unwrap().immune_for_clicks);
    assert!(!orders.get(43).unwrap().immune_for_clicks);
    assert!(orders.get(44).is_none());
}

#[test]
fn outgoing_send_stops_if_changed_matches_delphi_change_gate() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        42,
        "BTCUSDT",
        OrderWorkerStatus::BuySet,
        1,
    )));

    assert!(
        orders
            .send_stops_if_changed(404, &StopSettings::default())
            .is_none(),
        "Delphi exits when vOrder/local worker is absent"
    );
    assert!(
        orders
            .send_stops_if_changed(42, &StopSettings::default())
            .is_none(),
        "Delphi exits when Cur == FPrevStops"
    );

    let stops = StopSettings {
        stop_loss_on: DelphiBool::TRUE,
        sl_level: 12.5,
        trailing_on: DelphiBool::TRUE,
        trailing_level: -0.0,
        ..StopSettings::default()
    };
    assert!(
        orders.send_stops_if_changed(42, &stops).is_none(),
        "Delphi exits when worker.vOrder is nil even if stops changed"
    );
    assert!(orders.mark_local_visual_order(42));
    let (ctx, market, status, sent_stops) = orders
        .send_stops_if_changed(42, &stops)
        .expect("changed stops should be sent");

    assert_eq!(ctx.uid, 42);
    assert_eq!(ctx.currency, BaseCurrency::USDT);
    assert_eq!(ctx.platform, ExchangeCode::FBinance);
    assert_eq!(market, "BTCUSDT");
    assert_eq!(status, OrderWorkerStatus::BuySet);
    assert_eq!(sent_stops, stops);
    assert_eq!(orders.get(42).unwrap().stops, stops);
    assert!(
        orders.send_stops_if_changed(42, &stops).is_none(),
        "FPrevStops was updated before sending"
    );

    let same_by_float_math = StopSettings {
        trailing_level: 0.0,
        ..stops
    };
    assert!(
        orders
            .send_stops_if_changed(42, &same_by_float_math)
            .is_some(),
        "Delphi TStopSettings.Equal is CompareMem, so -0.0 and +0.0 differ"
    );
}

#[test]
// parity: MoonBot Unit1.pas (GUI send-stops; TakeProfit auto-default guard)
fn send_stops_derives_take_profit_changed() {
    // Regression guard: the trader must never set the take_profit_changed wire
    // flag by hand. The runtime derives it so the server never auto-defaults
    // (clobbers) the trader's TP on the SELL transition (Unit1.pas:18760).
    // Enabling/changing the TP latches the flag true; once set it stays latched.
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        7,
        "BTCUSDT",
        OrderWorkerStatus::BuySet,
        1,
    )));
    assert!(orders.mark_local_visual_order(7));

    // Caller forgets the flag (FALSE) but enables a take-profit: runtime sends TRUE.
    let stops = StopSettings {
        use_take_profit: DelphiBool::TRUE,
        take_profit: 123.0,
        take_profit_changed: DelphiBool::FALSE,
        ..StopSettings::default()
    };
    let (_, _, _, sent) = orders
        .send_stops_if_changed(7, &stops)
        .expect("enabling TP must send");
    assert_eq!(
        sent.take_profit_changed,
        DelphiBool::TRUE,
        "enabling/changing TP must latch take_profit_changed regardless of caller input"
    );

    // Re-sending the same stops (still FALSE from the caller) is a no-op: the
    // latched-true stored value makes the derived flag equal, so nothing changes.
    assert!(
        orders.send_stops_if_changed(7, &stops).is_none(),
        "re-sending identical stops is a no-op; the latched flag keeps it equal"
    );

    // Changing only a non-TP field keeps the flag latched true (Delphi keeps it
    // once set), proving the latch is independent of the caller's flag.
    let sl_only = StopSettings {
        stop_loss_on: DelphiBool::TRUE,
        sl_level: 50.0,
        ..stops
    };
    let (_, _, _, sent2) = orders
        .send_stops_if_changed(7, &sl_only)
        .expect("SL change must send");
    assert_eq!(
        sent2.take_profit_changed,
        DelphiBool::TRUE,
        "take_profit_changed stays latched once set, like Delphi"
    );
}

#[test]
fn local_visual_order_marker_can_be_registered_before_first_status() {
    let mut orders = Orders::new();
    assert!(
        !orders.mark_local_visual_order(42),
        "no read-model entry exists yet, marker is stored for the first status"
    );

    let mut cached = make_status(42, "BTCUSDT", OrderWorkerStatus::BuySet, 1);
    cached.from_cache = true;
    let (res, _) = orders.apply(order_status_cmd(cached));
    assert_eq!(res, ApplyResult::OrderNotFound);
    assert!(orders.get(42).is_none());

    orders.apply(order_status_cmd(make_status(
        42,
        "BTCUSDT",
        OrderWorkerStatus::BuySet,
        2,
    )));

    assert!(orders.get(42).unwrap().has_local_visual_order);
}

#[test]
fn outgoing_send_vstop_if_changed_matches_delphi_change_gate() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        42,
        "BTCUSDT",
        OrderWorkerStatus::SellSet,
        1,
    )));

    assert!(
        orders
            .send_vstop_if_changed(404, false, false, 0.0, 0.0)
            .is_none(),
        "Delphi exits when vOrder/local worker is absent"
    );
    assert!(
        orders
            .send_vstop_if_changed(42, false, false, 0.0, 0.0)
            .is_none(),
        "Delphi exits when VStop fields equal FPrevVStop*"
    );

    assert!(
        orders
            .send_vstop_if_changed(42, true, false, 12.5, 100.0)
            .is_none(),
        "Delphi exits when worker.vOrder is nil even if VStop changed"
    );
    assert!(orders.mark_local_visual_order(42));
    let (ctx, market, params) = orders
        .send_vstop_if_changed(42, true, false, 12.5, 100.0)
        .expect("changed VStop should be sent");

    assert_eq!(ctx.uid, 42);
    assert_eq!(ctx.currency, BaseCurrency::USDT);
    assert_eq!(ctx.platform, ExchangeCode::FBinance);
    assert_eq!(market, "BTCUSDT");
    assert_eq!(params.status, OrderWorkerStatus::SellSet);
    assert!(params.vstop_on);
    assert!(!params.vstop_fixed);
    assert_eq!(params.vstop_level, 12.5);
    assert_eq!(params.vstop_vol, 100.0);
    assert!(orders.get(42).unwrap().vstop_on);
    assert_eq!(orders.get(42).unwrap().vstop_level, 12.5);
    assert!(
        orders
            .send_vstop_if_changed(42, true, false, 12.5, 100.0)
            .is_none(),
        "FPrevVStop* was updated before sending"
    );
}

#[test]
fn outgoing_send_replace_if_requested_matches_delphi_gate() {
    let mut orders = Orders::new();
    let mut buy_status = make_status(42, "BTCUSDT", OrderWorkerStatus::BuySet, 1);
    buy_status.buy_order.order_type = OrderType::Buy;
    orders.apply(order_status_cmd(buy_status));

    assert!(
        orders.send_replace_if_requested(404, 10.0, 1000).is_none(),
        "Delphi exits when local worker is absent"
    );

    let (ctx, market, order_type, price) = orders
        .send_replace_if_requested(42, 10.5, 1000)
        .expect("first replace should be sent");
    assert_eq!(ctx.uid, 42);
    assert_eq!(ctx.currency, BaseCurrency::USDT);
    assert_eq!(ctx.platform, ExchangeCode::FBinance);
    assert_eq!(market, "BTCUSDT");
    assert_eq!(order_type, OrderType::Buy);
    assert_eq!(price, 10.5);
    let order = orders.get(42).unwrap();
    assert_eq!(order.buy_price, 10.5);
    assert!(order.bulk_replace_buy);
    assert_eq!(order.replace_sent_time_ms, 1000);

    assert!(
        orders.send_replace_if_requested(42, 10.7, 1001).is_none(),
        "ReplaceSentTime gate suppresses another packet while replace is in flight"
    );
    assert_eq!(orders.get(42).unwrap().buy_price, 10.7);

    let mut pending = make_status(43, "BTCUSDT", OrderWorkerStatus::None, 1);
    pending.buy_order.mean_price = 9.0;
    orders.apply(order_status_cmd(pending));
    assert!(
        orders.send_replace_if_requested(43, 9.0, 1000).is_none(),
        "pending replace sends only when BuyCondPrice changes"
    );
    let (_, _, order_type, price) = orders
        .send_replace_if_requested(43, 9.1, 1000)
        .expect("changed pending price should send O_BUY replace");
    assert_eq!(order_type, OrderType::Buy);
    assert_eq!(price, 9.1);
    assert_eq!(orders.get(43).unwrap().pending_buy_cond_price, Some(9.1));
}

#[test]
fn outgoing_send_cancel_if_requested_matches_delphi_gate() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        42,
        "BTCUSDT",
        OrderWorkerStatus::SellSet,
        1,
    )));
    orders.apply(order_status_cmd(make_status(
        43,
        "BTCUSDT",
        OrderWorkerStatus::SellDone,
        1,
    )));

    assert!(orders.send_cancel_if_requested(404, 1000).is_none());
    assert!(orders.send_cancel_if_requested(43, 1000).is_none());

    let send = orders
        .send_cancel_if_requested(42, 1000)
        .expect("sell-set cancel should be sent");
    match send {
        OrderCancelSend::Cancel {
            ctx,
            market,
            status,
        } => {
            assert_eq!(ctx.uid, 42);
            assert_eq!(market, "BTCUSDT");
            assert_eq!(status, OrderWorkerStatus::SellSet);
        }
        other => panic!("unexpected cancel send: {other:?}"),
    }
    assert!(
        !orders.get(42).unwrap().cancel_request,
        "Delphi clears FOrder.CancelRequest after sending"
    );

    let mut pending = make_status(44, "BTCUSDT", OrderWorkerStatus::None, 1);
    pending.buy_order.mean_price = 9.25;
    orders.apply(order_status_cmd(pending));
    let send = orders
        .send_cancel_if_requested(44, 1000)
        .expect("pending cancel should be sent");
    match send {
        OrderCancelSend::PendingReplaceThenCancel { ctx, market, price } => {
            assert_eq!(ctx.uid, 44);
            assert_eq!(market, "BTCUSDT");
            assert_eq!(price, 9.25);
        }
        other => panic!("unexpected pending cancel send: {other:?}"),
    }
    assert!(
        orders.get(44).unwrap().pending_cancel,
        "Delphi leaves vOrder.PendingCancel set on the pending order"
    );
    assert!(
        orders.tick_pending_cancel_resends(1031).is_empty(),
        "Delphi worker loop sleeps 32 ms between pending cancel sends"
    );
    let sends = orders.tick_pending_cancel_resends(1032);
    assert_eq!(sends.len(), 1);
    match &sends[0] {
        OrderCancelSend::PendingReplaceThenCancel { ctx, market, price } => {
            assert_eq!(ctx.uid, 44);
            assert_eq!(market, "BTCUSDT");
            assert_eq!(*price, 9.25);
        }
        other => panic!("unexpected pending cancel resend: {other:?}"),
    }
}

#[test]
fn outgoing_send_panic_sell_if_changed_matches_delphi_gate() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        42,
        "BTCUSDT",
        OrderWorkerStatus::SellSet,
        1,
    )));
    orders.apply(order_status_cmd(make_status(
        43,
        "BTCUSDT",
        OrderWorkerStatus::BuySet,
        1,
    )));

    assert!(
        orders.send_panic_sell_if_changed(43, true).is_none(),
        "Delphi sends panic-sell only from OS_SellSet workers"
    );
    assert!(
        orders.send_panic_sell_if_changed(42, false).is_none(),
        "initial PrevPanicSell=false suppresses redundant false"
    );

    let send = orders
        .send_panic_sell_if_changed(42, true)
        .expect("false -> true should be sent");
    assert_eq!(send.ctx.uid, 42);
    assert_eq!(send.market, "BTCUSDT");
    assert!(send.turn_on);
    assert!(orders.get(42).unwrap().panic_sell);

    assert!(
        orders.send_panic_sell_if_changed(42, true).is_none(),
        "PrevPanicSell was updated before sending"
    );
    let send = orders
        .send_panic_sell_if_changed(42, false)
        .expect("true -> false should be sent");
    assert!(!send.turn_on);
    assert!(!orders.get(42).unwrap().panic_sell);
}

#[test]
fn outgoing_market_panic_sell_matches_delphi_workers_toggle() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        42,
        "BTCUSDT",
        OrderWorkerStatus::SellSet,
        1,
    )));
    orders.apply(order_status_cmd(make_status(
        43,
        "BTCUSDT",
        OrderWorkerStatus::SellSet,
        1,
    )));
    orders.apply(order_status_cmd(make_status(
        44,
        "ETHUSDT",
        OrderWorkerStatus::SellSet,
        1,
    )));
    orders.apply(order_status_cmd(make_status(
        45,
        "BTCUSDT",
        OrderWorkerStatus::BuySet,
        1,
    )));

    let sends = orders.turn_panic_sell_by_market("BTCUSDT", true);
    assert_eq!(sends.len(), 2);
    assert!(orders.get(42).unwrap().panic_sell);
    assert!(orders.get(43).unwrap().panic_sell);
    assert!(!orders.get(44).unwrap().panic_sell);
    assert!(!orders.get(45).unwrap().panic_sell);

    let (panic_sell_on, sends) = orders.switch_panic_sell_by_market("BTCUSDT", true);
    assert!(!panic_sell_on);
    assert_eq!(sends.len(), 2);
    assert!(sends.iter().all(|send| !send.turn_on));
    assert!(!orders.get(42).unwrap().panic_sell);
    assert!(!orders.get(43).unwrap().panic_sell);

    let (panic_sell_on, sends) = orders.switch_panic_sell_by_market("BTCUSDT", true);
    assert!(panic_sell_on);
    assert_eq!(sends.len(), 2);
    assert!(sends.iter().all(|send| send.turn_on));
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder
fn incoming_turn_panic_sell_is_not_applied_by_process_command_order() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        42,
        "BTCUSDT",
        OrderWorkerStatus::SellSet,
        1,
    )));

    let turn = TurnPanicSellCommand {
        epoch_header: make_epoch(42, 3, "BTCUSDT", 2, OrderWorkerStatus::SellSet),
        turn_on: true,
    };
    let (res, ev) = orders.apply(TradeCommand::TurnPanicSell(turn));

    assert_eq!(res, ApplyResult::NotApplicable);
    assert!(matches!(
        ev,
        OrderEvent::Ignored {
            uid: 42,
            reason: ApplyResult::NotApplicable
        }
    ));
    assert_eq!(orders.get(42).unwrap().status, OrderWorkerStatus::SellSet);
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder
fn incoming_noop_trade_epoch_still_updates_epoch() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        42,
        "BTCUSDT",
        OrderWorkerStatus::SellSet,
        1,
    )));

    let turn = TurnPanicSellCommand {
        epoch_header: make_epoch(42, 3, "BTCUSDT", 2, OrderWorkerStatus::SellSet),
        turn_on: true,
    };
    let (res, _) = orders.apply(TradeCommand::TurnPanicSell(turn));
    assert_eq!(res, ApplyResult::NotApplicable);

    let stale_update = OrderStatusUpdate {
        epoch_header: make_epoch(42, 3, "BTCUSDT", 1, OrderWorkerStatus::SellSet),
        update_data: OrderUpdateData {
            actual_price: 123.0,
            ..OrderUpdateData::default()
        },
        sell_reason_code: 0,
    };
    let (res, _) = orders.apply(TradeCommand::OrderStatusUpdate(stale_update));

    assert_eq!(
            res,
            ApplyResult::OutOfOrder,
            "Delphi AcceptServerCommand updates FServerLatestEpoch even for no-op TTradeEpochCommand receive"
        );
    let sell_actual = orders.get(42).unwrap().sell_order.actual_price;
    assert_eq!(sell_actual, 0.0);
}

#[test]
fn move_all_sells_candidate_gate_matches_delphi_active_client_overloads() {
    let mut orders = Orders::new();
    let mut immune_short = make_status(1, "BTCUSDT", OrderWorkerStatus::SellSet, 1);
    immune_short.is_short = true;
    immune_short.immune_for_clicks = true;
    orders.apply(order_status_cmd(immune_short));

    let move_kind = MoveAllSellsParams {
        cmd_type: MoveAllCmdType::MoveKind,
        move_kind: ReplaceMultiKind::TopVol,
        price: 10.0,
        price_zone: PriceZone::default(),
        side: FixedPosition::Short,
    };
    assert!(
        !orders.has_move_all_sells_candidate("BTCUSDT", move_kind),
        "MoveKind overload checks not ImmuneForClicks before wire send"
    );

    let pers = MoveAllSellsParams {
        cmd_type: MoveAllCmdType::Pers,
        ..move_kind
    };
    assert!(
        orders.has_move_all_sells_candidate("BTCUSDT", pers),
        "percent overload ignores ImmuneForClicks in Delphi"
    );

    let mut long = make_status(2, "BTCUSDT", OrderWorkerStatus::SellSet, 1);
    long.is_short = false;
    orders.apply(order_status_cmd(long));

    let price_zone = MoveAllSellsParams {
        cmd_type: MoveAllCmdType::PriceZone,
        side: FixedPosition::Short,
        ..move_kind
    };
    assert!(
        orders.has_move_all_sells_candidate("BTCUSDT", price_zone),
        "PriceZone active-client send gate ignores ASide and checks only market/status/non-immune"
    );

    let none = MoveAllSellsParams {
        move_kind: ReplaceMultiKind::None,
        ..move_kind
    };
    assert!(
        !orders.has_move_all_sells_candidate("BTCUSDT", none),
        "RM_None exits before sending"
    );
}

#[test]
fn move_all_buys_candidate_gate_matches_delphi_active_client_overloads() {
    let mut orders = Orders::new();
    let mut immune_long = make_status(1, "BTCUSDT", OrderWorkerStatus::BuySet, 1);
    immune_long.is_short = false;
    immune_long.immune_for_clicks = true;
    orders.apply(order_status_cmd(immune_long));

    assert!(
        !orders.has_move_all_buys_candidate(
            "BTCUSDT",
            MoveAllBuysCmdType::MoveKind,
            ReplaceMultiKind::TopVol,
            FixedPosition::Long,
        ),
        "MoveKind overload checks not ImmuneForClicks before wire send"
    );
    assert!(
        orders.has_move_all_buys_candidate(
            "BTCUSDT",
            MoveAllBuysCmdType::Pers,
            ReplaceMultiKind::None,
            FixedPosition::Short,
        ),
        "percent overload checks only active market BuySet"
    );

    let mut short = make_status(2, "BTCUSDT", OrderWorkerStatus::BuySet, 1);
    short.is_short = true;
    orders.apply(order_status_cmd(short));

    assert!(
        orders.has_move_all_buys_candidate(
            "BTCUSDT",
            MoveAllBuysCmdType::MoveKind,
            ReplaceMultiKind::LastSet,
            FixedPosition::Short,
        ),
        "MoveKind gate honors ASide"
    );
    assert!(
        !orders.has_move_all_buys_candidate(
            "BTCUSDT",
            MoveAllBuysCmdType::MoveKind,
            ReplaceMultiKind::None,
            FixedPosition::Both,
        ),
        "RM_None exits before sending"
    );
}

#[test]
fn sell_almost_done_is_terminal() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        42,
        "BTCUSDT",
        OrderWorkerStatus::SellSet,
        1,
    )));

    let s2 = make_status(42, "BTCUSDT", OrderWorkerStatus::SellAlmostDone, 2);
    let (res, ev) = orders.apply(order_status_cmd(s2));
    assert_eq!(res, ApplyResult::Applied);
    assert!(matches!(ev, OrderEvent::Updated(42)));
    assert!(orders.get(42).unwrap().job_is_done);
    assert_eq!(orders.drain_pending_removals(), vec![42]);
    assert!(orders.get(42).is_none());
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder
fn visual_trace_after_terminal_status_is_accepted_before_deferred_removal() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        42,
        "BTCUSDT",
        OrderWorkerStatus::SellSet,
        1,
    )));

    orders.apply(order_status_cmd(make_status(
        42,
        "BTCUSDT",
        OrderWorkerStatus::SellDone,
        2,
    )));

    let trace = OrderTracePoint {
        market: make_market(42, 3, "BTCUSDT"),
        trace_time: 45_000.0,
        trace_price: 101.0,
        base_price: 100.0,
        stop_price: 0.0,
        ord_type: OrderType::Sell,
        flags: trace_flags::IS_FINISH,
    };
    let (res, ev) = orders.apply(TradeCommand::OrderTracePoint(trace));

    assert_eq!(res, ApplyResult::Applied);
    assert!(matches!(ev, OrderEvent::TracePoint { uid: 42 }));
    assert_eq!(orders.get(42).unwrap().trace_points.len(), 1);
    assert_eq!(orders.drain_pending_removals(), vec![42]);
    assert!(orders.get(42).is_none());
}

#[test]
fn trace_points_are_not_capped_like_former_rust_ring_buffer() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        42,
        "BTCUSDT",
        OrderWorkerStatus::SellSet,
        1,
    )));

    for n in 0..300 {
        let trace = OrderTracePoint {
            market: make_market(42, 3, "BTCUSDT"),
            trace_time: 45_000.0 + n as f64,
            trace_price: 100.0 + n as f32,
            base_price: 100.0,
            stop_price: 0.0,
            ord_type: OrderType::Sell,
            flags: 0,
        };
        let (res, ev) = orders.apply(TradeCommand::OrderTracePoint(trace));
        assert_eq!(res, ApplyResult::Applied);
        assert!(matches!(ev, OrderEvent::TracePoint { uid: 42 }));
    }

    assert_eq!(orders.get(42).unwrap().trace_points.len(), 300);
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder
fn trace_line_ignores_non_initial_without_existing_line() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        42,
        "BTCUSDT",
        OrderWorkerStatus::SellSet,
        1,
    )));

    let (res, ev) = orders.apply(TradeCommand::OrderTracePoint(trace_point(
        42,
        OrderType::Sell,
        0,
        45_000.0,
        101.0,
        100.0,
        0.0,
    )));

    assert_eq!(res, ApplyResult::Applied);
    assert!(matches!(ev, OrderEvent::TracePoint { uid: 42 }));
    let order = orders.get(42).unwrap();
    assert_eq!(order.trace_points.len(), 1);
    assert!(order.sell_trace_line.is_none());
}

#[test]
fn trace_line_initial_temp_and_finish_match_delphi_order_line() {
    let mut orders = Orders::new();
    let mut status = make_status(42, "BTCUSDT", OrderWorkerStatus::SellSet, 1);
    status.sell_order.create_time = 44_000.0;
    status.sell_order.int_id = 77;
    orders.apply(order_status_cmd(status));

    orders.apply(TradeCommand::OrderTracePoint(trace_point(
        42,
        OrderType::Sell,
        trace_flags::IS_INITIAL,
        45_000.0,
        101.0,
        100.0,
        99.0,
    )));
    {
        let line = orders.get(42).unwrap().sell_trace_line.as_ref().unwrap();
        assert_eq!(line.order_type, OrderType::Sell);
        assert_eq!(line.order_id, 77);
        assert!(line.prevent_delete);
        assert_eq!(line.stop_price, Some(99.0));
        assert_eq!(
            line.points,
            vec![
                OrderTraceChartPoint {
                    time: 44_000.0,
                    price: 100.0,
                },
                OrderTraceChartPoint {
                    time: 45_000.0,
                    price: 100.0,
                },
                OrderTraceChartPoint::default(),
                OrderTraceChartPoint {
                    time: 45_000.0,
                    price: 101.0,
                },
            ]
        );
        assert!(line.can_finish);
    }

    orders.apply(TradeCommand::OrderTracePoint(trace_point(
        42,
        OrderType::Sell,
        trace_flags::IS_TEMP,
        45_010.0,
        102.0,
        100.0,
        0.0,
    )));
    {
        let line = orders.get(42).unwrap().sell_trace_line.as_ref().unwrap();
        assert_eq!(
            line.tmp_point,
            Some(OrderTraceChartPoint {
                time: 45_010.0,
                price: 102.0,
            })
        );
        assert_eq!(line.points.len(), 4);
    }

    orders.apply(TradeCommand::OrderTracePoint(trace_point(
        42,
        OrderType::Sell,
        0,
        45_020.0,
        103.0,
        100.0,
        0.0,
    )));
    {
        let line = orders.get(42).unwrap().sell_trace_line.as_ref().unwrap();
        assert_eq!(
            &line.points[4..],
            &[
                OrderTraceChartPoint {
                    time: 45_020.0,
                    price: 101.0,
                },
                OrderTraceChartPoint {
                    time: 45_010.0,
                    price: 102.0,
                },
                OrderTraceChartPoint {
                    time: 45_020.0,
                    price: 103.0,
                },
            ]
        );
        assert!(line.can_finish);
    }

    orders.apply(TradeCommand::OrderTracePoint(trace_point(
        42,
        OrderType::Sell,
        trace_flags::IS_FINISH,
        45_030.0,
        104.0,
        100.0,
        0.0,
    )));
    let line = orders.get(42).unwrap().sell_trace_line.as_ref().unwrap();
    assert_eq!(line.points.last().unwrap().price, 104.0);
    assert!(!line.can_finish);
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder
fn order_not_found_marks_server_forced_then_deferred_removal() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        42,
        "BTCUSDT",
        OrderWorkerStatus::BuySet,
        1,
    )));
    {
        let order = std::sync::Arc::make_mut(orders.map.get_mut(&42).unwrap());
        order.buy_order.is_opened = DelphiBool::TRUE;
        order.buy_order.canceled = DelphiBool::FALSE;
        order.buy_order.is_closed = DelphiBool::FALSE;
        order.buy_order.close_time = 11.0;
        order.sell_order.is_opened = DelphiBool::TRUE;
        order.sell_order.canceled = DelphiBool::FALSE;
        order.sell_order.is_closed = DelphiBool::FALSE;
        order.sell_order.close_time = 12.0;
        order.bulk_replace_buy = true;
        order.bulk_replace_sell = true;
        order.replace_sent_time_ms = 1000;
    }

    let not_found = make_epoch(42, 3, "BTCUSDT", 0, OrderWorkerStatus::None);
    let (res, ev) = orders.apply(TradeCommand::OrderNotFound(not_found));

    assert_eq!(res, ApplyResult::Applied);
    assert!(matches!(ev, OrderEvent::Updated(42)));
    let order = orders.get(42).unwrap();
    assert!(order.server_forced_remove);
    assert!(order.cancel_request);
    let buy_is_opened = order.buy_order.is_opened;
    let buy_canceled = order.buy_order.canceled;
    let buy_is_closed = order.buy_order.is_closed;
    let buy_close_time = order.buy_order.close_time;
    let sell_is_opened = order.sell_order.is_opened;
    let sell_canceled = order.sell_order.canceled;
    let sell_is_closed = order.sell_order.is_closed;
    let sell_close_time = order.sell_order.close_time;
    assert_eq!(
        (buy_is_opened, buy_canceled, buy_is_closed),
        (DelphiBool::TRUE, DelphiBool::FALSE, DelphiBool::FALSE)
    );
    assert_eq!(
        (sell_is_opened, sell_canceled, sell_is_closed),
        (DelphiBool::TRUE, DelphiBool::FALSE, DelphiBool::FALSE)
    );
    assert_eq!(buy_close_time, 11.0);
    assert_eq!(sell_close_time, 12.0);
    assert!(order.bulk_replace_buy);
    assert!(order.bulk_replace_sell);
    assert!(
        !order.job_is_done,
        "Delphi TOrderNotFound sets CancellRequest, not JobIsDone, inside ProcessCommandOrder"
    );
    assert_eq!(orders.drain_pending_removals(), vec![42]);
    assert!(orders.get(42).is_none());
}

#[test]
fn phase_rollback_rejected() {
    let mut orders = Orders::new();
    // SellSet (phase 3) → then BuySet (phase 1) → rollback
    orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::SellSet,
        5,
    )));
    let (res, _) = orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::BuySet,
        6,
    )));
    assert_eq!(res, ApplyResult::PhaseRollback);
}

#[test]
fn phase_rollback_not_applied_for_terminal() {
    // BuySet (phase 1) → BuyCancel (phase 0): NOT rollback because the new phase = 0
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::BuySet,
        5,
    )));
    let (res, _) = orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::BuyCancel,
        6,
    )));
    // BuyCancel terminal → marked for deferred removal.
    assert_eq!(res, ApplyResult::Applied);
    assert!(orders.get(1).unwrap().job_is_done);
    assert_eq!(orders.drain_pending_removals(), vec![1]);
}

#[test]
fn epoch_out_of_order_rejected() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::BuySet,
        10,
    )));
    // In Delphi the first full status that creates a worker goes through
    // OnMServerOrder -> HandleServerCommand and does not populate
    // FServerLatestEpoch. The next command for this status already passes
    // AcceptServerCommand and sets latest=10.
    let (res, _) = orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::BuySet,
        10,
    )));
    assert_eq!(res, ApplyResult::Applied);
    // epoch 5 after 10: backDist=10-5=5 <= 100 → stale
    let (res, _) = orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::BuySet,
        5,
    )));
    assert_eq!(res, ApplyResult::OutOfOrder);
}

#[test]
fn epoch_duplicate_rejected() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::BuySet,
        10,
    )));
    let (res, _) = orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::BuySet,
        10,
    )));
    assert_eq!(res, ApplyResult::Applied);
    // Same epoch after AcceptServerCommand latest=10 — a duplicate,
    // rejected (Delphi EpochIsOK: LastEpoch=NewEpoch → false).
    let (res, _) = orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::BuySet,
        10,
    )));
    assert_eq!(res, ApplyResult::OutOfOrder);
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder
fn first_same_epoch_after_new_order_is_accepted() {
    let mut orders = Orders::new();
    let mut status = make_status(1, "X", OrderWorkerStatus::BuySet, 10);
    status.buy_order.actual_price = 10.0;
    orders.apply(order_status_cmd(status));

    let same_epoch_update = OrderStatusUpdate {
        epoch_header: make_epoch(1, 3, "X", 10, OrderWorkerStatus::BuySet),
        update_data: OrderUpdateData {
            actual_price: 11.0,
            ..Default::default()
        },
        sell_reason_code: 0,
    };
    let (res, ev) = orders.apply(TradeCommand::OrderStatusUpdate(same_epoch_update));

    assert_eq!(
        res,
        ApplyResult::Applied,
        "Delphi first TOrderStatus bypasses AcceptServerCommand, so latest epoch is still zero"
    );
    assert!(matches!(ev, OrderEvent::Updated(1)));
    let actual = orders.get(1).unwrap().buy_order.actual_price;
    assert_eq!(actual, 11.0);
}

#[test]
fn epoch_wrap_around_accepted() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::BuySet,
        65500,
    )));
    // wrap: 65500 → 200, backDist = 65500-200 = 65300 > 100 → accept (new message)
    let (res, _) = orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::BuySet,
        200,
    )));
    assert_eq!(res, ApplyResult::Applied);
}

#[test]
fn replace_response_updates_epoch_slot() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::BuySet,
        10,
    )));

    let rr = OrderReplaceResponse {
        epoch_header: make_epoch(1, 3, "X", 11, OrderWorkerStatus::BuySet),
        order_type: OrderType::Buy,
        price: 123.0,
        update_data: OrderUpdateData::default(),
        quantity_base: 0.0,
    };

    let (res, _) = orders.apply(order_replace_response_cmd(rr.clone()));
    assert_eq!(res, ApplyResult::Applied);

    let (res, _) = orders.apply(order_replace_response_cmd(rr));
    assert_eq!(res, ApplyResult::OutOfOrder);
}

#[test]
fn stops_update_uses_epoch_guard() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::BuySet,
        10,
    )));
    let (res, _) = orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::BuySet,
        10,
    )));
    assert_eq!(res, ApplyResult::Applied);

    let stops = StopSettings {
        stop_loss_on: DelphiBool::TRUE,
        ..Default::default()
    };
    let stale = OrderStopsUpdate {
        epoch_header: make_epoch(1, 3, "X", 5, OrderWorkerStatus::BuySet),
        stops,
    };

    let (res, _) = orders.apply(TradeCommand::OrderStopsUpdate(stale));
    assert_eq!(res, ApplyResult::OutOfOrder);
    assert_eq!(orders.get(1).unwrap().stops.stop_loss_on, DelphiBool::FALSE);
}

#[test]
fn vstop_update_uses_phase_guard() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::SellSet,
        10,
    )));

    let rollback = VStopUpdate {
        epoch_header: make_epoch(1, 3, "X", 200, OrderWorkerStatus::BuySet),
        vstop_on: true,
        vstop_fixed: false,
        vstop_level: 42.0,
        vstop_vol: 1.0,
    };

    let (res, _) = orders.apply(TradeCommand::VStopUpdate(rollback));
    assert_eq!(res, ApplyResult::PhaseRollback);
    assert!(!orders.get(1).unwrap().vstop_on);
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder
fn terminal_status_update_does_not_apply_update_data() {
    let mut orders = Orders::new();
    let mut status = make_status(1, "X", OrderWorkerStatus::SellSet, 10);
    status.sell_order.actual_price = 10.0;
    orders.apply(order_status_cmd(status));

    let terminal_update = OrderStatusUpdate {
        epoch_header: make_epoch(1, 3, "X", 11, OrderWorkerStatus::SellDone),
        update_data: OrderUpdateData {
            actual_price: 999.0,
            mean_price: 999.0,
            quantity: 999.0,
            ..Default::default()
        },
        sell_reason_code: 14,
    };
    let (res, ev) = orders.apply(TradeCommand::OrderStatusUpdate(terminal_update));

    assert_eq!(res, ApplyResult::Applied);
    assert!(matches!(ev, OrderEvent::Updated(1)));
    let order = orders.get(1).unwrap();
    let sell_actual = order.sell_order.actual_price;
    let sell_mean = order.sell_order.mean_price;
    assert_eq!(sell_actual, 10.0);
    assert_eq!(sell_mean, 0.0);
    assert_eq!(order.sell_reason, SellReason::TakeProfit);
    assert!(order.job_is_done);
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder
fn sell_done_status_update_applies_set_done_flags() {
    let mut orders = Orders::new();
    let mut status = make_status(1, "X", OrderWorkerStatus::SellSet, 10);
    status.buy_order.is_opened = DelphiBool::TRUE;
    status.buy_order.is_closed = DelphiBool::FALSE;
    status.buy_order.canceled = DelphiBool::FALSE;
    status.sell_order.is_opened = DelphiBool::TRUE;
    status.sell_order.is_closed = DelphiBool::FALSE;
    status.sell_order.canceled = DelphiBool::FALSE;
    orders.apply(order_status_cmd(status));

    {
        let order = std::sync::Arc::make_mut(orders.map.get_mut(&1).unwrap());
        order.bulk_replace_buy = true;
        order.bulk_replace_sell = true;
    }

    let terminal_update = OrderStatusUpdate {
        epoch_header: make_epoch(1, 3, "X", 11, OrderWorkerStatus::SellDone),
        update_data: Default::default(),
        sell_reason_code: 0,
    };
    let (res, _) = orders.apply(TradeCommand::OrderStatusUpdate(terminal_update));

    assert_eq!(res, ApplyResult::Applied);
    let order = orders.get(1).unwrap();
    assert_eq!(order.sell_order.is_opened, DelphiBool::FALSE);
    assert_eq!(order.sell_order.is_closed, DelphiBool::TRUE);
    assert_eq!(
        order.sell_order.canceled,
        DelphiBool::FALSE,
        "SetDoneFlags does not mark sell side canceled"
    );
    assert_eq!(order.buy_order.is_opened, DelphiBool::FALSE);
    assert_eq!(order.buy_order.is_closed, DelphiBool::FALSE);
    assert_eq!(
        order.buy_order.canceled,
        DelphiBool::TRUE,
        "SetDoneFlags cancels buy side only when it was not already closed"
    );
    assert!(!order.bulk_replace_buy);
    assert!(!order.bulk_replace_sell);
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder
fn sell_done_full_status_applies_set_done_flags() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::SellSet,
        10,
    )));

    {
        let order = std::sync::Arc::make_mut(orders.map.get_mut(&1).unwrap());
        order.bulk_replace_buy = true;
        order.bulk_replace_sell = true;
    }

    let mut done = make_status(1, "X", OrderWorkerStatus::SellDone, 11);
    done.buy_order.is_opened = DelphiBool::TRUE;
    done.buy_order.is_closed = DelphiBool::TRUE;
    done.buy_order.canceled = DelphiBool::FALSE;
    done.sell_order.is_opened = DelphiBool::TRUE;
    done.sell_order.is_closed = DelphiBool::FALSE;
    done.sell_order.canceled = DelphiBool::FALSE;
    let (res, _) = orders.apply(order_status_cmd(done));

    assert_eq!(res, ApplyResult::Applied);
    let order = orders.get(1).unwrap();
    assert_eq!(order.sell_order.is_opened, DelphiBool::FALSE);
    assert_eq!(order.sell_order.is_closed, DelphiBool::TRUE);
    assert_eq!(order.sell_order.canceled, DelphiBool::FALSE);
    assert_eq!(order.buy_order.is_opened, DelphiBool::FALSE);
    assert_eq!(
        order.buy_order.canceled,
        DelphiBool::FALSE,
        "already closed buy side is not marked canceled by SetDoneFlags"
    );
    assert!(!order.bulk_replace_buy);
    assert!(!order.bulk_replace_sell);
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder
fn zero_sell_reason_update_keeps_previous_reason() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::SellSet,
        10,
    )));

    let first_reason = OrderStatusUpdate {
        epoch_header: make_epoch(1, 3, "X", 11, OrderWorkerStatus::SellSet),
        update_data: Default::default(),
        sell_reason_code: 14,
    };
    orders.apply(TradeCommand::OrderStatusUpdate(first_reason));
    assert_eq!(orders.get(1).unwrap().sell_reason, SellReason::TakeProfit);

    let zero_reason = OrderStatusUpdate {
        epoch_header: make_epoch(1, 3, "X", 12, OrderWorkerStatus::SellSet),
        update_data: Default::default(),
        sell_reason_code: 0,
    };
    orders.apply(TradeCommand::OrderStatusUpdate(zero_reason));
    assert_eq!(
        orders.get(1).unwrap().sell_reason,
        SellReason::TakeProfit,
        "Delphi ignores SellReasonCode=0 and keeps FPrevSellReasonCode/SellReason"
    );

    let changed_reason = OrderStatusUpdate {
        epoch_header: make_epoch(1, 3, "X", 13, OrderWorkerStatus::SellSet),
        update_data: Default::default(),
        sell_reason_code: 9,
    };
    orders.apply(TradeCommand::OrderStatusUpdate(changed_reason));
    assert_eq!(orders.get(1).unwrap().sell_reason, SellReason::MarketStop);
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder
fn pending_status_update_tracks_vorder_buy_cond_price() {
    let mut orders = Orders::new();
    let mut status = make_status(1, "X", OrderWorkerStatus::None, 10);
    status.buy_order.mean_price = 10.0;
    orders.apply(order_status_cmd(status));

    assert_eq!(orders.get(1).unwrap().pending_buy_cond_price, Some(10.0));
    let initial_buy_mean = orders.get(1).unwrap().buy_order.mean_price;
    assert_eq!(initial_buy_mean, 10.0);

    let pending_update = OrderStatusUpdate {
        epoch_header: make_epoch(1, 3, "X", 11, OrderWorkerStatus::None),
        update_data: OrderUpdateData {
            mean_price: 11.0,
            actual_price: 999.0,
            ..Default::default()
        },
        sell_reason_code: 0,
    };
    let (res, ev) = orders.apply(TradeCommand::OrderStatusUpdate(pending_update));

    assert_eq!(res, ApplyResult::Applied);
    assert!(matches!(ev, OrderEvent::Updated(1)));
    let order = orders.get(1).unwrap();
    let buy_mean = order.buy_order.mean_price;
    let buy_actual = order.buy_order.actual_price;
    assert_eq!(order.pending_buy_cond_price, Some(11.0));
    assert_eq!(
        buy_mean, 10.0,
        "OS_None update changes vOrder.BuyCondPrice, not pBuyOrder"
    );
    assert_eq!(buy_actual, 0.0);

    let buy_set_update = OrderStatusUpdate {
        epoch_header: make_epoch(1, 3, "X", 12, OrderWorkerStatus::BuySet),
        update_data: OrderUpdateData {
            mean_price: 12.0,
            actual_price: 12.0,
            ..Default::default()
        },
        sell_reason_code: 0,
    };
    orders.apply(TradeCommand::OrderStatusUpdate(buy_set_update));
    assert_eq!(orders.get(1).unwrap().pending_buy_cond_price, None);
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder
fn os_none_update_without_pending_vorder_does_not_create_pending_price() {
    let mut orders = Orders::new();
    let mut status = make_status(1, "X", OrderWorkerStatus::BuySet, 10);
    status.buy_order.mean_price = 10.0;
    orders.apply(order_status_cmd(status));

    let non_pending_none = OrderStatusUpdate {
        epoch_header: make_epoch(1, 3, "X", 11, OrderWorkerStatus::None),
        update_data: OrderUpdateData {
            mean_price: 77.0,
            actual_price: 88.0,
            ..Default::default()
        },
        sell_reason_code: 0,
    };
    let (res, ev) = orders.apply(TradeCommand::OrderStatusUpdate(non_pending_none));

    assert_eq!(res, ApplyResult::Applied);
    assert!(matches!(ev, OrderEvent::Updated(1)));
    let order = orders.get(1).unwrap();
    assert_eq!(order.status, OrderWorkerStatus::None);
    assert_eq!(
        order.pending_buy_cond_price, None,
        "Delphi changes vOrder.BuyCondPrice only when IsPending and vOrder exists"
    );
    let buy_mean_price = order.buy_order.mean_price;
    assert_eq!(
        buy_mean_price, 10.0,
        "OS_None update still must not ApplyTo(pBuyOrder)"
    );
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder
fn full_os_none_status_for_existing_pending_keeps_vorder_price() {
    let mut orders = Orders::new();
    let mut pending = make_status(1, "X", OrderWorkerStatus::None, 10);
    pending.buy_order.mean_price = 10.0;
    orders.apply(order_status_cmd(pending));
    assert_eq!(orders.get(1).unwrap().pending_buy_cond_price, Some(10.0));

    let mut full_status = make_status(1, "X", OrderWorkerStatus::None, 11);
    full_status.buy_order.mean_price = 77.0;
    let (res, ev) = orders.apply(order_status_cmd(full_status));

    assert_eq!(res, ApplyResult::Applied);
    assert!(matches!(ev, OrderEvent::Updated(1)));
    let order = orders.get(1).unwrap();
    assert_eq!(
        order.pending_buy_cond_price,
        Some(10.0),
        "Delphi TOrderStatus does not copy BuyOrder.MeanPrice into existing vOrder.BuyCondPrice"
    );
    let buy_mean = order.buy_order.mean_price;
    assert_eq!(
        buy_mean, 77.0,
        "Delphi still applies Cmd.BuyOrder to pBuyOrder"
    );
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder
fn full_os_none_status_for_existing_non_pending_does_not_create_vorder() {
    let mut orders = Orders::new();
    let mut status = make_status(1, "X", OrderWorkerStatus::BuySet, 10);
    status.buy_order.mean_price = 10.0;
    orders.apply(order_status_cmd(status));

    let mut full_none = make_status(1, "X", OrderWorkerStatus::None, 11);
    full_none.buy_order.mean_price = 88.0;
    let (res, ev) = orders.apply(order_status_cmd(full_none));

    assert_eq!(res, ApplyResult::Applied);
    assert!(matches!(ev, OrderEvent::Updated(1)));
    let order = orders.get(1).unwrap();
    assert_eq!(order.status, OrderWorkerStatus::None);
    assert_eq!(
        order.pending_buy_cond_price, None,
        "Delphi creates pending vOrder only on the new OnMServerOrder path"
    );
    let buy_mean = order.buy_order.mean_price;
    assert_eq!(buy_mean, 88.0);
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder
fn corridor_update_marks_order_as_moon_shot() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::BuySet,
        10,
    )));
    assert!(!orders.get(1).unwrap().is_moon_shot);

    let (res, ev) = orders.apply(TradeCommand::CorridorUpdate(CorridorUpdate {
        market: make_market(1, 3, "X"),
        price_down: 10.5,
        price_up: 12.25,
    }));

    assert_eq!(res, ApplyResult::Applied);
    assert!(matches!(ev, OrderEvent::CorridorChanged(1)));
    let order = orders.get(1).unwrap();
    assert!(order.is_moon_shot);
    assert_eq!(order.corridor_price_down, 10.5);
    assert_eq!(order.corridor_price_up, 12.25);
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder
fn replace_response_quantity_base_zero_preserves_existing_value() {
    let mut orders = Orders::new();
    let mut status = make_status(1, "X", OrderWorkerStatus::BuySet, 10);
    status.buy_order.quantity_base = 12.5;
    orders.apply(order_status_cmd(status));

    let rr = OrderReplaceResponse {
        epoch_header: make_epoch(1, 3, "X", 11, OrderWorkerStatus::BuySet),
        order_type: OrderType::Buy,
        price: 123.0,
        update_data: OrderUpdateData {
            actual_price: 123.0,
            ..Default::default()
        },
        quantity_base: 0.0,
    };
    let (res, ev) = orders.apply(order_replace_response_cmd(rr));

    assert_eq!(res, ApplyResult::Applied);
    assert!(matches!(ev, OrderEvent::Updated(1)));
    let quantity_base = orders.get(1).unwrap().buy_order.quantity_base;
    assert_eq!(quantity_base, 12.5);
    assert_eq!(orders.get(1).unwrap().buy_price, 123.0);
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder
fn replace_response_buy_stop_uses_sell_side() {
    let mut orders = Orders::new();
    let mut status = make_status(1, "X", OrderWorkerStatus::BuySet, 10);
    status.buy_order.actual_price = 111.0;
    status.sell_order.actual_price = 222.0;
    orders.apply(order_status_cmd(status));

    let rr = OrderReplaceResponse {
        epoch_header: make_epoch(1, 3, "X", 11, OrderWorkerStatus::SellSet),
        order_type: OrderType::BuyStop,
        price: 456.0,
        update_data: OrderUpdateData {
            actual_price: 456.0,
            ..Default::default()
        },
        quantity_base: 7.5,
    };
    let (res, ev) = orders.apply(order_replace_response_cmd(rr));

    assert_eq!(res, ApplyResult::Applied);
    assert!(matches!(ev, OrderEvent::Updated(1)));
    let order = orders.get(1).unwrap();
    let buy_actual_price = order.buy_order.actual_price;
    let sell_actual_price = order.sell_order.actual_price;
    let sell_quantity_base = order.sell_order.quantity_base;
    assert_eq!(buy_actual_price, 111.0);
    assert_eq!(order.buy_price, 111.0);
    assert_eq!(sell_actual_price, 456.0);
    assert_eq!(sell_quantity_base, 7.5);
    assert_eq!(order.sell_price, 456.0);
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder (ReplaceSentTime lifecycle)
fn bulk_replace_timeout_clears_flag_after_5000ms() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::BuySet,
        10,
    )));

    let notify = BulkReplaceNotify {
        market: make_market(0, 3, "X"),
        order_type: OrderType::Buy,
        uids: vec![1],
    };
    let (res, ev) = orders.apply_at(TradeCommand::BulkReplaceNotify(notify), 1000);
    assert_eq!(res, ApplyResult::Applied);
    assert!(matches!(ev, OrderEvent::BulkReplaced { .. }));
    assert!(orders.get(1).unwrap().bulk_replace_buy);

    assert!(orders.tick_bulk_replace_timeouts(6000).is_empty());
    assert!(orders.get(1).unwrap().bulk_replace_buy);

    let events = orders.tick_bulk_replace_timeouts(6001);
    assert!(matches!(events.as_slice(), [OrderEvent::Updated(1)]));
    assert!(!orders.get(1).unwrap().bulk_replace_buy);
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder (ReplaceSentTime lifecycle)
fn replace_response_clears_flag_then_tick_clears_shared_sent_time() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::BuySet,
        10,
    )));

    assert!(orders.send_replace_if_requested(1, 123.0, 1000).is_some());
    assert!(orders.get(1).unwrap().bulk_replace_buy);
    assert_eq!(orders.get(1).unwrap().replace_sent_time_ms, 1000);

    let rr = OrderReplaceResponse {
        epoch_header: make_epoch(1, 3, "X", 11, OrderWorkerStatus::BuySet),
        order_type: OrderType::Buy,
        price: 123.0,
        update_data: OrderUpdateData::default(),
        quantity_base: 0.0,
    };
    let (res, _) = orders.apply(order_replace_response_cmd(rr));
    assert_eq!(res, ApplyResult::Applied);

    let order = orders.get(1).unwrap();
    assert!(!order.bulk_replace_buy);
    assert_eq!(
        order.replace_sent_time_ms, 1000,
        "Delphi TOrderReplaceResponse clears p*Order.OrderReplace, not ReplaceSentTime"
    );

    assert!(orders.tick_bulk_replace_timeouts(1001).is_empty());
    assert_eq!(
        orders.get(1).unwrap().replace_sent_time_ms,
        0,
        "Delphi CheckReplaceFlag clears ReplaceSentTime when current FOrder flag is false"
    );
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder (ReplaceSentTime lifecycle)
fn bulk_replace_tick_checks_only_current_side() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::BuySet,
        10,
    )));

    let notify = BulkReplaceNotify {
        market: make_market(0, 3, "X"),
        order_type: OrderType::BuyStop,
        uids: vec![1],
    };
    let (res, _) = orders.apply_at(TradeCommand::BulkReplaceNotify(notify), 1000);
    assert_eq!(res, ApplyResult::Applied);
    assert!(orders.get(1).unwrap().bulk_replace_sell);
    assert_eq!(orders.get(1).unwrap().replace_sent_time_ms, 1000);

    assert!(orders.tick_bulk_replace_timeouts(6001).is_empty());
    let order = orders.get(1).unwrap();
    assert!(order.bulk_replace_sell);
    assert_eq!(
        order.replace_sent_time_ms, 0,
        "Delphi current FOrder=buy clears only ReplaceSentTime; opposite side flag is untouched"
    );
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder (TBulkReplaceNotify)
fn bulk_replace_notify_reports_only_found_workers() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::BuySet,
        10,
    )));

    let notify = BulkReplaceNotify {
        market: make_market(0, 3, "X"),
        order_type: OrderType::Buy,
        uids: vec![1, 2],
    };
    let (res, ev) = orders.apply_at(TradeCommand::BulkReplaceNotify(notify), 1000);

    assert_eq!(res, ApplyResult::Applied);
    assert!(orders.get(1).unwrap().bulk_replace_buy);
    assert!(matches!(
        ev,
        OrderEvent::BulkReplaced {
            order_type: OrderType::Buy,
            uids
        } if uids == vec![1]
    ));

    let missing_notify = BulkReplaceNotify {
        market: make_market(0, 3, "X"),
        order_type: OrderType::Sell,
        uids: vec![2],
    };
    let (res, ev) = orders.apply_at(TradeCommand::BulkReplaceNotify(missing_notify), 1000);

    assert_eq!(res, ApplyResult::OrderNotFound);
    assert!(matches!(
        ev,
        OrderEvent::Ignored {
            uid: 0,
            reason: ApplyResult::OrderNotFound
        }
    ));
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder (TBulkReplaceNotify)
fn bulk_replace_notify_buy_stop_uses_sell_side() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::SellSet,
        10,
    )));

    let notify = BulkReplaceNotify {
        market: make_market(0, 3, "X"),
        order_type: OrderType::BuyStop,
        uids: vec![1],
    };
    let (res, ev) = orders.apply_at(TradeCommand::BulkReplaceNotify(notify), 1000);

    assert_eq!(res, ApplyResult::Applied);
    assert!(matches!(ev, OrderEvent::BulkReplaced { .. }));
    let order = orders.get(1).unwrap();
    assert!(!order.bulk_replace_buy);
    assert!(order.bulk_replace_sell);
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder (TBulkReplaceNotify)
fn bulk_replace_notify_unknown_order_type_uses_sell_side() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::SellSet,
        10,
    )));

    let notify = BulkReplaceNotify {
        market: make_market(0, 3, "X"),
        order_type: OrderType::from_byte(250),
        uids: vec![1],
    };
    let (res, ev) = orders.apply_at(TradeCommand::BulkReplaceNotify(notify), 1000);

    assert_eq!(res, ApplyResult::Applied);
    assert!(matches!(ev, OrderEvent::BulkReplaced { .. }));
    let order = orders.get(1).unwrap();
    assert!(!order.bulk_replace_buy);
    assert!(order.bulk_replace_sell);
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder
fn order_status_maintains_local_price_fields() {
    let mut orders = Orders::new();
    let mut status = make_status(1, "X", OrderWorkerStatus::BuySet, 10);
    status.buy_order.actual_price = 10.0;
    status.sell_order.actual_price = 20.0;
    orders.apply(order_status_cmd(status));

    let order = orders.get(1).unwrap();
    assert_eq!(order.buy_price, 10.0);
    assert_eq!(order.sell_price, 20.0);

    let mut repeated = make_status(1, "X", OrderWorkerStatus::BuySet, 11);
    repeated.buy_order.actual_price = 11.0;
    repeated.sell_order.actual_price = 21.0;
    orders.apply(order_status_cmd(repeated));

    let order = orders.get(1).unwrap();
    assert_eq!(order.buy_price, 11.0);
    assert_eq!(order.sell_price, 21.0);
}

#[test]
fn epoch_is_ok_unit() {
    // Delphi: backDist := last - new (Word wrapping); accept = backDist > 100.

    // duplicate
    assert!(!epoch_is_ok(10, 10));
    // stale and close: backDist = 100-50 = 50 <= 100 → reject.
    assert!(!epoch_is_ok(100, 50));
    // accept forward through wrap: backDist = 100-250 = 65386 > 100 → accept.
    assert!(epoch_is_ok(100, 250));
    // wrap-around forward far: last=65500, new=200. backDist = 65300 > 100 → accept.
    assert!(epoch_is_ok(65500, 200));
    // last=200, new=65500. backDist = 200-65500 (wrap) = 236 > 100 → accept.
    assert!(epoch_is_ok(200, 65500));
    // near stale: last=10, new=65500. backDist = 10-65500 (wrap) = 46 <= 100 → reject.
    assert!(!epoch_is_ok(10, 65500));
    // window boundary: backDist = 100 → NOT accept (requires STRICTLY > 100).
    assert!(!epoch_is_ok(500, 400));
    // one past the boundary → accept.
    assert!(epoch_is_ok(500, 399));
}

#[test]
fn missing_after_snapshot_returns_old_orders_after_dispatcher_style_status_loop() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::BuySet,
        1,
    )));
    orders.apply(order_status_cmd(make_status(
        2,
        "Y",
        OrderWorkerStatus::BuySet,
        1,
    )));

    orders.begin_snapshot();
    orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::SellSet,
        2,
    )));

    let missing = orders.missing_after_snapshot();
    assert_eq!(missing, vec![2]);
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder (SnapshotFlag)
fn existing_order_command_refreshes_snapshot_flag_before_epoch_guard() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::BuySet,
        10,
    )));
    let (res, _) = orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::BuySet,
        10,
    )));
    assert_eq!(res, ApplyResult::Applied);

    orders.begin_snapshot();
    let duplicate_update = OrderStatusUpdate {
        epoch_header: make_epoch(1, 3, "X", 10, OrderWorkerStatus::BuySet),
        update_data: OrderUpdateData::default(),
        sell_reason_code: 0,
    };
    let (res, _) = orders.apply(TradeCommand::OrderStatusUpdate(duplicate_update));

    assert_eq!(res, ApplyResult::OutOfOrder);
    assert!(
        orders.missing_after_snapshot().is_empty(),
        "Delphi sets Worker.SnapshotFlag before AcceptServerCommand can reject the command"
    );
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder (SnapshotFlag)
fn unknown_status_ordinal_preserves_snapshot_flag_and_skips_epoch_index() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::BuySet,
        10,
    )));

    orders.begin_snapshot();
    let unknown_status = OrderWorkerStatus::from_byte(250);
    let (res, _) = orders.apply(TradeCommand::OrderStatusRequest(make_epoch(
        1,
        3,
        "X",
        11,
        unknown_status,
    )));

    assert_eq!(res, ApplyResult::NotApplicable);
    assert!(
            orders.missing_after_snapshot().is_empty(),
            "Delphi sets SnapshotFlag before AcceptServerCommand; invalid enum ordinal only skips FServerLatestEpoch indexing"
        );
    assert_eq!(orders.get(1).unwrap().status, OrderWorkerStatus::BuySet);
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder (TBulkReplaceNotify early-exit branch)
fn bulk_replace_notify_does_not_refresh_snapshot_flag() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::BuySet,
        10,
    )));

    orders.begin_snapshot();
    let notify = BulkReplaceNotify {
        market: make_market(0, 3, "X"),
        order_type: OrderType::Buy,
        uids: vec![1],
    };
    let (res, _) = orders.apply_at(TradeCommand::BulkReplaceNotify(notify), 1000);

    assert_eq!(res, ApplyResult::Applied);
    assert_eq!(
        orders.missing_after_snapshot(),
        vec![1],
        "Delphi TBulkReplaceNotify exits before the general WCache SnapshotFlag assignment"
    );
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder (SnapshotFlag)
fn non_base_market_trade_command_does_not_refresh_snapshot_flag() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        1,
        "X",
        OrderWorkerStatus::BuySet,
        10,
    )));

    orders.begin_snapshot();
    let (res, _) = orders.apply(TradeCommand::AllStatusesRequest(make_base(1, 3)));

    assert_eq!(res, ApplyResult::NotApplicable);
    assert_eq!(
        orders.missing_after_snapshot(),
        vec![1],
        "Delphi ClientNewData calls ProcessCommandOrder only for TBaseMarketCommand descendants"
    );
}

#[test]
// parity: MoonBot MoonProtoClient.pas:TMoonProtoNetClient.ProcessCommandOrder (WCache SnapshotFlag sweep)
fn missing_after_snapshot_keeps_terminal_entry_until_deferred_removal() {
    let mut orders = Orders::new();
    orders.apply_at(
        order_status_cmd(make_status(1, "X", OrderWorkerStatus::SellSet, 1)),
        1000,
    );
    orders.apply_at(
        order_status_cmd(make_status(1, "X", OrderWorkerStatus::SellDone, 2)),
        1001,
    );
    assert!(orders.get(1).unwrap().job_is_done);

    orders.begin_snapshot();

    assert_eq!(
        orders.missing_after_snapshot(),
        vec![1],
        "Delphi virtual worker is still in WCache and not JobIsDone until DoTheJobVirtual returns"
    );
    assert_eq!(orders.drain_pending_removals_due(1401), vec![1]);
    assert!(orders.missing_after_snapshot().is_empty());
}

#[test]
fn direct_all_statuses_is_not_hidden_batch_inside_process_command_order() {
    let mut orders = Orders::new();
    let snap = AllStatuses {
        header: make_base(0, 3),
        orders: vec![make_status(1, "X", OrderWorkerStatus::SellSet, 2)],
    };

    let (res, ev) = orders.apply(TradeCommand::AllStatuses(snap));

    assert_eq!(res, ApplyResult::NotApplicable);
    assert!(matches!(
        ev,
        OrderEvent::Ignored {
            uid: 0,
            reason: ApplyResult::NotApplicable
        }
    ));
    assert!(orders.is_empty());
    assert_eq!(orders.current_snapshot_flag(), 0);
}

#[test]
fn accepts_more_than_former_rust_order_cap() {
    const FORMER_MAX_ORDERS: u64 = 50_000;
    let mut orders = Orders::new();
    for uid in 1..=FORMER_MAX_ORDERS + 1 {
        let (res, ev) = orders.apply(order_status_cmd(make_status(
            uid,
            "X",
            OrderWorkerStatus::BuySet,
            1,
        )));
        assert_eq!(res, ApplyResult::Applied);
        assert!(matches!(ev, OrderEvent::Created(id) if id == uid));
    }

    assert_eq!(orders.len(), (FORMER_MAX_ORDERS + 1) as usize);
    assert!(orders.get(FORMER_MAX_ORDERS + 1).is_some());
}

#[test]
fn snapshot_cow_mutating_one_order_does_not_deep_clone_other_orders() {
    let mut orders = Orders::new();
    orders.apply(order_status_cmd(make_status(
        1,
        "BTCUSDT",
        OrderWorkerStatus::BuySet,
        1,
    )));
    orders.apply(order_status_cmd(make_status(
        2,
        "ETHUSDT",
        OrderWorkerStatus::SellSet,
        1,
    )));

    let snapshot = orders.clone();
    assert!(std::sync::Arc::ptr_eq(
        orders.map.get(&1).unwrap(),
        snapshot.map.get(&1).unwrap()
    ));
    assert!(std::sync::Arc::ptr_eq(
        orders.map.get(&2).unwrap(),
        snapshot.map.get(&2).unwrap()
    ));

    assert!(orders.mark_local_visual_order(1));

    assert!(!std::sync::Arc::ptr_eq(
        orders.map.get(&1).unwrap(),
        snapshot.map.get(&1).unwrap()
    ));
    assert!(std::sync::Arc::ptr_eq(
        orders.map.get(&2).unwrap(),
        snapshot.map.get(&2).unwrap()
    ));
}
