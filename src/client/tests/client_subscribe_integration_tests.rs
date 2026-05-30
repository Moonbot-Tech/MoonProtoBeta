use super::*;
use crate::commands::engine_api::EngineMethod;

fn dummy_cfg() -> ClientConfig {
    ClientConfig {
        server_ip: "127.0.0.1".to_string(),
        server_port: 3000,
        master_key: [0; 16],
        mac_key: [0; 16],
        mask_ver: TransportMode::V0,
        client_id: 0,
        ntp_host: None,
        refresh: RefreshConfig {
            update_markets_every: None,
            check_tags_every: None,
        },
    }
}

fn ready_client() -> Client {
    let mut client = Client::new(dummy_cfg());
    client.set_domain_ready(true);
    client
}

#[test]
// parity: MoonBot MoonProtoIntStruct.pas:TMoonProtoDataToSend.Create
fn client_retry_left_clamps_zero() {
    let client = ready_client();

    client.send_cmd_keyed(
        vec![1, 2, 3, 4],
        Command::UI,
        SendPriority::High,
        true,
        0,
        UniqueKey::base_ui_settings_slot(),
    );

    let (_, high, _) = client.take_send_queues_for_test();
    assert_eq!(high.len(), 1);
    assert_eq!(high[0].max_retries, 0);
    assert_eq!(
        high[0].retry_left, 0,
        "Delphi clamps RetryLeft with Max(0, MaxRetryCount - 1)"
    );
}

fn command_uid(payload: &[u8]) -> Option<u64> {
    payload
        .get(3..11)
        .and_then(|s| s.try_into().ok())
        .map(u64::from_le_bytes)
}

fn method_id(payload: &[u8]) -> Option<u8> {
    payload.get(11).copied()
}

fn market_names_count(payload: &[u8]) -> Option<i32> {
    let bytes: [u8; 4] = payload.get(14..18)?.try_into().ok()?;
    Some(i32::from_le_bytes(bytes))
}

fn drain_api_requests(client: &Client) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let (sliced, high, low) = client.take_send_queues_for_test();
    for item in sliced.into_iter().chain(high).chain(low) {
        if item.cmd == Command::API.to_byte() {
            out.push(item.data);
        }
    }
    out
}

fn tracked_orders(
    uid: u64,
    currency: u8,
    platform: u8,
    market_name: &str,
    status: crate::commands::trade::OrderWorkerStatus,
    is_short: bool,
    immune_for_clicks: bool,
) -> crate::state::Orders {
    use crate::commands::trade::{
        BaseCommandHeader, MarketCommandHeader, OrderCompact, OrderStatus, StopSettings,
        TradeCommand, TradeEpochHeader,
    };

    let mut orders = crate::state::Orders::new();
    let status_cmd = OrderStatus {
        epoch_header: TradeEpochHeader {
            market: MarketCommandHeader {
                base: BaseCommandHeader {
                    cmd_id: 4,
                    ver: 3,
                    uid,
                },
                currency,
                platform,
                market_name: market_name.to_string(),
            },
            epoch: 11,
            status,
        },
        buy_order: OrderCompact::default(),
        sell_order: OrderCompact::default(),
        stops: StopSettings::default(),
        strat_id: 0,
        is_short,
        db_id: 0,
        from_cache: false,
        emulator_mode: false,
        immune_for_clicks,
    };
    let _ = orders.apply(TradeCommand::OrderStatus(Box::new(status_cmd)));
    orders
}

fn assert_no_queued_wire(client: &Client) {
    let (sliced, high, low) = client.take_send_queues_for_test();
    assert!(sliced.is_empty() && high.is_empty() && low.is_empty());
}

#[test]
fn pre_init_subscription_intents_update_registry_without_wire() {
    let client = Client::new(dummy_cfg());

    client.subscribe_orderbook("BTCUSDT");
    client.subscribe_all_trades(true);
    client.ui_mm_subscribe(false);

    client.with_subscription_registry(|registry| {
        assert!(registry.orderbook_subs.contains("BTCUSDT"));
        assert_eq!(
            registry.trades_sub,
            Some(TradesSubscription { want_mm: true })
        );
        assert_eq!(registry.mm_orders_sub, Some(false));
    });
    assert_no_queued_wire(&client);
}

#[test]
fn pre_init_stateful_order_actions_do_not_mutate_or_send() {
    use crate::commands::trade::{DelphiBool, OrderWorkerStatus, StopSettings};

    let client = Client::new(dummy_cfg());
    let uid = 0x5151;
    let mut orders = tracked_orders(
        uid,
        17,
        9,
        "DOGEUSDT",
        OrderWorkerStatus::SellSet,
        false,
        false,
    );

    assert!(!client.replace_order(&mut orders, uid, 50100.0));
    assert!(!client.update_order_stops(
        &mut orders,
        uid,
        &StopSettings {
            stop_loss_on: DelphiBool::TRUE,
            sl_level: 12.5,
            ..StopSettings::default()
        }
    ));

    let order = orders.get(uid).expect("local order remains");
    assert_eq!(order.sell_price, 0.0);
    assert_eq!(order.stops, StopSettings::default());
    assert_no_queued_wire(&client);
}

#[test]
fn post_init_flush_sends_pre_init_registry_subscriptions_once() {
    let mut client = Client::new(dummy_cfg());

    client.subscribe_orderbook("BTCUSDT");
    client.subscribe_all_trades(true);
    assert_no_queued_wire(&client);

    client.set_domain_ready(true);
    client.send_registry_subscriptions_after_init();
    let sent = drain_api_requests(&client);
    let methods: Vec<_> = sent
        .iter()
        .filter_map(|payload| method_id(payload))
        .collect();
    assert_eq!(methods.len(), 2);
    assert!(methods.contains(&(EngineMethod::SubscribeAllTrades.to_byte())));
    assert!(methods.contains(&(EngineMethod::SubscribeOrderBook.to_byte())));

    client.subscribe_all_trades(true);
    let sent = drain_api_requests(&client);
    assert!(
        sent.is_empty(),
        "same all-trades intent after init must not duplicate the pre-init flush"
    );
}

#[test]
fn client_subscribe_orderbook_updates_registry_and_wire_queue_through_sender() {
    let client = ready_client();
    client.subscribe_orderbook("BTCUSDT");
    assert!(
        client.with_subscription_registry(|registry| registry.orderbook_subs.contains("BTCUSDT"))
    );
    let sent = drain_api_requests(&client);
    assert_eq!(sent.len(), 1);
    assert_eq!(
        method_id(&sent[0]),
        Some(EngineMethod::SubscribeOrderBook.to_byte())
    );
}

#[test]
fn client_sender_can_be_held_independently_of_client() {
    // Sender holds a clone; even if client is held by a `&` reference, the sender
    // is independent. This is the basis for multi-thread subscribing without an app-event backlog.
    let client = ready_client();
    let sender = client.sender();
    sender.subscribe_all_trades(true);
    assert_eq!(
        client.with_subscription_registry(|registry| registry.trades_sub),
        Some(TradesSubscription { want_mm: true })
    );
    let sent = drain_api_requests(&client);
    assert_eq!(sent.len(), 1);
    assert_eq!(
        method_id(&sent[0]),
        Some(EngineMethod::SubscribeAllTrades.to_byte())
    );
}

#[test]
fn cancel_tracked_order_uses_order_state_context() {
    use crate::commands::trade::{OrderWorkerStatus, TradeCommand};

    let uid = 0x1122_3344_5566_7788;
    let mut orders = tracked_orders(
        uid,
        17,
        9,
        "DOGEUSDT",
        OrderWorkerStatus::SellSet,
        false,
        false,
    );
    let client = ready_client();

    assert!(client.cancel_tracked_order(&mut orders, uid));

    let (_, high, _) = client.take_send_queues_for_test();
    assert_eq!(high.len(), 1);
    let item = &high[0];
    assert_eq!(item.cmd, Command::Order.to_byte());
    assert_eq!(item.priority, SendPriority::High);
    assert_eq!(item.max_retries, 3);
    assert_eq!(item.u_key, UniqueKey::order_move(uid));

    match TradeCommand::parse(&item.data).expect("valid cancel command") {
        TradeCommand::OrderCancel(cmd) => {
            assert_eq!(cmd.epoch_header.market.base.uid, uid);
            assert_eq!(cmd.epoch_header.market.currency, 17);
            assert_eq!(cmd.epoch_header.market.platform, 9);
            assert_eq!(cmd.epoch_header.market.market_name, "DOGEUSDT");
            assert_eq!(cmd.epoch_header.epoch, 0);
            assert_eq!(cmd.epoch_header.status, OrderWorkerStatus::SellSet);
        }
        other => panic!("unexpected trade command: {other:?}"),
    }
}

#[test]
fn client_cancel_pending_order_matches_delphi_replace_then_cancel_effect() {
    use crate::commands::trade::{OrderWorkerStatus, TradeCommand};

    let uid = 0x1188;
    let mut orders = tracked_orders(
        uid,
        17,
        9,
        "DOGEUSDT",
        OrderWorkerStatus::None,
        false,
        false,
    );
    let client = ready_client();

    assert!(client.cancel_order(&mut orders, uid));
    assert!(
        orders.get(uid).unwrap().pending_cancel,
        "Delphi keeps vOrder.PendingCancel set on a pending order"
    );

    let (_, high, _) = client.take_send_queues_for_test();
    assert_eq!(
            high.len(),
            1,
            "Delphi SendCmdInt UKey-dedups the immediate replace before the cancel if the writer has not copied it"
        );
    assert_eq!(high[0].u_key, UniqueKey::order_move(uid));
    match TradeCommand::parse(&high[0].data).expect("valid cancel command") {
        TradeCommand::OrderCancel(cmd) => {
            assert_eq!(cmd.epoch_header.market.base.uid, uid);
            assert_eq!(cmd.epoch_header.market.currency, 17);
            assert_eq!(cmd.epoch_header.market.platform, 9);
            assert_eq!(cmd.epoch_header.market.market_name, "DOGEUSDT");
            assert_eq!(cmd.epoch_header.status, OrderWorkerStatus::None);
        }
        other => panic!("unexpected trade command: {other:?}"),
    }
}

#[test]
fn client_replace_order_uses_delphi_local_gate() {
    use crate::commands::trade::{OrderType, OrderWorkerStatus, TradeCommand};

    let uid = 0x2233;
    let mut orders = tracked_orders(
        uid,
        17,
        9,
        "DOGEUSDT",
        OrderWorkerStatus::SellSet,
        false,
        false,
    );
    let client = ready_client();

    assert!(client.replace_order(&mut orders, uid, 50100.0));
    assert_eq!(orders.get(uid).unwrap().sell_price, 50100.0);
    assert!(orders.get(uid).unwrap().bulk_replace_sell);

    let (_, high, _) = client.take_send_queues_for_test();
    assert_eq!(high.len(), 1);
    assert_eq!(high[0].u_key, UniqueKey::order_move(uid));
    match TradeCommand::parse(&high[0].data).expect("valid replace command") {
        TradeCommand::OrderReplace(cmd) => {
            assert_eq!(cmd.epoch_header.market.base.uid, uid);
            assert_eq!(cmd.epoch_header.market.currency, 17);
            assert_eq!(cmd.epoch_header.market.platform, 9);
            assert_eq!(cmd.epoch_header.market.market_name, "DOGEUSDT");
            assert_eq!(cmd.order_type, OrderType::Sell);
            assert_eq!(cmd.new_price, 50100.0);
        }
        other => panic!("unexpected trade command: {other:?}"),
    }

    assert!(
        !client.replace_order(&mut orders, uid, 50200.0),
        "Delphi ReplaceSentTime gate suppresses a second replace while in flight"
    );
    assert_eq!(orders.get(uid).unwrap().sell_price, 50200.0);
    let (_, high, _) = client.take_send_queues_for_test();
    assert!(high.is_empty());
}

#[test]
fn client_turn_order_panic_sell_uses_delphi_local_gate() {
    use crate::commands::trade::{OrderWorkerStatus, TradeCommand};

    let uid = 0x3344;
    let mut orders = tracked_orders(
        uid,
        17,
        9,
        "DOGEUSDT",
        OrderWorkerStatus::SellSet,
        false,
        false,
    );
    let client = ready_client();

    assert!(
        !client.turn_order_panic_sell(&mut orders, uid, false),
        "initial PrevPanicSell=false suppresses redundant false"
    );
    let (_, high, _) = client.take_send_queues_for_test();
    assert!(high.is_empty());

    assert!(client.turn_order_panic_sell(&mut orders, uid, true));
    assert!(orders.get(uid).unwrap().panic_sell);
    let (_, high, _) = client.take_send_queues_for_test();
    assert_eq!(high.len(), 1);
    assert_eq!(high[0].u_key, UniqueKey::order_move(uid));
    match TradeCommand::parse(&high[0].data).expect("valid panic command") {
        TradeCommand::TurnPanicSell(cmd) => {
            assert_eq!(cmd.epoch_header.market.base.uid, uid);
            assert_eq!(cmd.epoch_header.market.currency, 17);
            assert_eq!(cmd.epoch_header.market.platform, 9);
            assert_eq!(cmd.epoch_header.market.market_name, "DOGEUSDT");
            assert!(cmd.turn_on);
        }
        other => panic!("unexpected trade command: {other:?}"),
    }

    assert!(!client.turn_order_panic_sell(&mut orders, uid, true));
    let (_, high, _) = client.take_send_queues_for_test();
    assert!(high.is_empty());
}

#[test]
fn client_switch_panic_sell_by_market_matches_delphi_button_semantics() {
    use crate::commands::trade::{OrderWorkerStatus, TradeCommand};

    let uid_a = 0x3345;
    let uid_b = 0x3346;
    let mut orders = tracked_orders(
        uid_a,
        17,
        9,
        "DOGEUSDT",
        OrderWorkerStatus::SellSet,
        false,
        false,
    );
    let status_cmd = {
        use crate::commands::trade::{
            BaseCommandHeader, MarketCommandHeader, OrderCompact, OrderStatus, StopSettings,
            TradeCommand, TradeEpochHeader,
        };
        TradeCommand::OrderStatus(Box::new(OrderStatus {
            epoch_header: TradeEpochHeader {
                market: MarketCommandHeader {
                    base: BaseCommandHeader {
                        cmd_id: 4,
                        ver: 3,
                        uid: uid_b,
                    },
                    currency: 17,
                    platform: 9,
                    market_name: "DOGEUSDT".to_string(),
                },
                epoch: 11,
                status: OrderWorkerStatus::SellSet,
            },
            buy_order: OrderCompact::default(),
            sell_order: OrderCompact::default(),
            stops: StopSettings::default(),
            strat_id: 0,
            is_short: false,
            db_id: 0,
            from_cache: false,
            emulator_mode: false,
            immune_for_clicks: false,
        }))
    };
    let _ = orders.apply(status_cmd);
    let client = ready_client();

    assert!(client.switch_panic_sell_by_market(&mut orders, "DOGEUSDT", true));
    let (_, high, _) = client.take_send_queues_for_test();
    assert_eq!(high.len(), 2);
    for item in &high {
        match TradeCommand::parse(&item.data).expect("valid panic command") {
            TradeCommand::TurnPanicSell(cmd) => {
                assert_eq!(cmd.epoch_header.market.market_name, "DOGEUSDT");
                assert!(cmd.turn_on);
            }
            other => panic!("unexpected trade command: {other:?}"),
        }
    }

    assert!(!client.switch_panic_sell_by_market(&mut orders, "DOGEUSDT", true));
    let (_, high, _) = client.take_send_queues_for_test();
    assert_eq!(high.len(), 2);
    for item in &high {
        match TradeCommand::parse(&item.data).expect("valid panic command") {
            TradeCommand::TurnPanicSell(cmd) => assert!(!cmd.turn_on),
            other => panic!("unexpected trade command: {other:?}"),
        }
    }
}

#[test]
fn client_move_all_sells_uses_delphi_pre_send_gate() {
    use crate::commands::trade::{
        FixedPosition, MoveAllCmdType, MoveAllSellsParams, OrderWorkerStatus, PriceZone,
        ReplaceMultiKind, TradeCommand, TradeCtx,
    };

    let params = MoveAllSellsParams {
        cmd_type: MoveAllCmdType::MoveKind,
        move_kind: ReplaceMultiKind::TopVol,
        price: 50100.0,
        price_zone: PriceZone {
            min_p: 49_500.0,
            max_p: 50_500.0,
        },
        side: FixedPosition::Long,
    };
    let ctx = TradeCtx::with_route_bytes(0xCAFE, 17, 9);
    let client = ready_client();
    let empty_orders = crate::state::Orders::new();

    assert!(
        !client.move_all_sells(&empty_orders, ctx, "DOGEUSDT", params),
        "Delphi active-client branch sends nothing without a matching order"
    );
    let (sliced, high, low) = client.take_send_queues_for_test();
    assert!(sliced.is_empty() && high.is_empty() && low.is_empty());

    let orders = tracked_orders(
        7,
        17,
        9,
        "DOGEUSDT",
        OrderWorkerStatus::SellSet,
        false,
        false,
    );
    assert!(client.move_all_sells(&orders, ctx, "DOGEUSDT", params));

    let (_, high, _) = client.take_send_queues_for_test();
    assert_eq!(high.len(), 1);
    match TradeCommand::parse(&high[0].data).expect("valid move all sells") {
        TradeCommand::MoveAllSells(cmd) => {
            assert_eq!(cmd.market.base.uid, ctx.uid);
            assert_eq!(cmd.market.currency, 17);
            assert_eq!(cmd.market.platform, 9);
            assert_eq!(cmd.cmd_type, MoveAllCmdType::MoveKind.to_byte());
            assert_eq!(cmd.move_kind, ReplaceMultiKind::TopVol);
            assert_eq!(cmd.side, FixedPosition::Long);
        }
        other => panic!("unexpected trade command: {other:?}"),
    }
}

#[test]
fn client_move_all_buys_uses_buy_only_cmd_type_and_delphi_gate() {
    use crate::commands::trade::{
        FixedPosition, MoveAllBuysCmdType, MoveAllBuysParams, OrderWorkerStatus, ReplaceMultiKind,
        TradeCommand, TradeCtx,
    };

    let ctx = TradeCtx::with_route_bytes(0xBEEF, 17, 9);
    let client = ready_client();
    let immune_orders =
        tracked_orders(8, 17, 9, "DOGEUSDT", OrderWorkerStatus::BuySet, false, true);

    assert!(
        !client.move_all_buys(
            &immune_orders,
            ctx,
            "DOGEUSDT",
            MoveAllBuysParams {
                cmd_type: MoveAllBuysCmdType::MoveKind,
                move_kind: ReplaceMultiKind::TopVol,
                price: 50100.0,
                side: FixedPosition::Long,
            },
        ),
        "MoveKind buy overload checks not ImmuneForClicks"
    );
    let (_, high, _) = client.take_send_queues_for_test();
    assert!(high.is_empty());

    assert!(client.move_all_buys(
        &immune_orders,
        ctx,
        "DOGEUSDT",
        MoveAllBuysParams {
            cmd_type: MoveAllBuysCmdType::Pers,
            move_kind: ReplaceMultiKind::None,
            price: 1.5,
            side: FixedPosition::Short,
        },
    ));

    let (_, high, _) = client.take_send_queues_for_test();
    assert_eq!(high.len(), 1);
    match TradeCommand::parse(&high[0].data).expect("valid move all buys") {
        TradeCommand::MoveAllBuys(cmd) => {
            assert_eq!(cmd.market.base.uid, ctx.uid);
            assert_eq!(cmd.cmd_type, MoveAllBuysCmdType::Pers.to_byte());
            assert_eq!(cmd.move_kind, ReplaceMultiKind::None);
            assert_eq!(cmd.side, FixedPosition::Short);
        }
        other => panic!("unexpected trade command: {other:?}"),
    }
}

#[test]
fn client_set_immune_applies_local_side_effect_before_wire_send() {
    use crate::commands::trade::{ImmuneItem, OrderWorkerStatus, TradeCommand};

    let mut orders = tracked_orders(
        0x100,
        17,
        9,
        "DOGEUSDT",
        OrderWorkerStatus::SellSet,
        false,
        false,
    );
    let client = ready_client();
    let items = [
        ImmuneItem {
            uid: 0x100,
            value: true,
        },
        ImmuneItem {
            uid: 0x200,
            value: true,
        },
    ];

    assert!(client.set_immune(&mut orders, &items));
    assert!(orders.get(0x100).unwrap().immune_for_clicks);

    let (_, high, _) = client.take_send_queues_for_test();
    assert_eq!(high.len(), 1);
    assert_eq!(high[0].u_key, UniqueKey::immune_clicks(0x100));
    match TradeCommand::parse(&high[0].data).expect("valid set immune") {
        TradeCommand::SetImmune(cmd) => {
            assert_eq!(cmd.items.len(), 1);
            assert_eq!(cmd.items[0].uid, 0x100);
            assert!(cmd.items[0].value);
        }
        other => panic!("unexpected trade command: {other:?}"),
    }

    assert!(
        !client.set_immune(
            &mut orders,
            &[ImmuneItem {
                uid: 0x200,
                value: false,
            }],
        ),
        "Delphi does not send if no local worker was found"
    );
    let (_, high, _) = client.take_send_queues_for_test();
    assert!(high.is_empty());
}

#[test]
fn client_update_order_stops_uses_delphi_send_if_changed_gate() {
    use crate::commands::trade::{DelphiBool, OrderWorkerStatus, StopSettings, TradeCommand};

    let uid = 0x4444;
    let mut orders = tracked_orders(
        uid,
        17,
        9,
        "DOGEUSDT",
        OrderWorkerStatus::BuySet,
        false,
        false,
    );
    let client = ready_client();

    assert!(
        !client.update_order_stops(&mut orders, uid, &StopSettings::default()),
        "Delphi SendStopsIfChanged exits when Cur == FPrevStops"
    );
    let (_, high, _) = client.take_send_queues_for_test();
    assert!(high.is_empty());

    let stops = StopSettings {
        stop_loss_on: DelphiBool::TRUE,
        sl_level: 12.5,
        use_take_profit: DelphiBool::TRUE,
        take_profit: 15.0,
        ..StopSettings::default()
    };
    assert!(
        !client.update_order_stops(&mut orders, uid, &stops),
        "Delphi SendStopsIfChanged exits when worker.vOrder is nil"
    );
    let (_, high, _) = client.take_send_queues_for_test();
    assert!(high.is_empty());

    assert!(orders.mark_local_visual_order(uid));
    assert!(client.update_order_stops(&mut orders, uid, &stops));
    // U2 (sverka #14): the runtime derives take_profit_changed = TRUE here because
    // the take-profit was enabled/changed from the default. The caller-supplied
    // flag (FALSE) is ignored, closing the SELL auto-default money-trap.
    let expected = StopSettings {
        take_profit_changed: DelphiBool::TRUE,
        ..stops
    };
    assert_eq!(orders.get(uid).unwrap().stops, expected);

    let (_, high, _) = client.take_send_queues_for_test();
    assert_eq!(high.len(), 1);
    assert_eq!(high[0].u_key, UniqueKey::order_move(uid));
    match TradeCommand::parse(&high[0].data).expect("valid stops update") {
        TradeCommand::OrderStopsUpdate(cmd) => {
            assert_eq!(cmd.epoch_header.market.base.uid, uid);
            assert_eq!(cmd.epoch_header.market.currency, 17);
            assert_eq!(cmd.epoch_header.market.platform, 9);
            assert_eq!(cmd.epoch_header.market.market_name, "DOGEUSDT");
            assert_eq!(cmd.epoch_header.epoch, 0);
            assert_eq!(cmd.epoch_header.status, OrderWorkerStatus::BuySet);
            assert_eq!(cmd.stops, expected);
        }
        other => panic!("unexpected trade command: {other:?}"),
    }

    assert!(
        !client.update_order_stops(&mut orders, uid, &stops),
        "FPrevStops/current state was updated before queueing"
    );
    let (_, high, _) = client.take_send_queues_for_test();
    assert!(high.is_empty());
}

#[test]
fn client_update_vstop_uses_delphi_send_if_changed_gate() {
    use crate::commands::trade::{OrderWorkerStatus, TradeCommand};

    let uid = 0x5555;
    let mut orders = tracked_orders(
        uid,
        17,
        9,
        "DOGEUSDT",
        OrderWorkerStatus::SellSet,
        false,
        false,
    );
    let client = ready_client();

    assert!(
        !client.update_vstop(&mut orders, uid, false, false, 0.0, 0.0),
        "Delphi SendVStopIfChanged exits when fields equal FPrevVStop*"
    );
    let (_, high, _) = client.take_send_queues_for_test();
    assert!(high.is_empty());

    assert!(
        !client.update_vstop(&mut orders, uid, true, false, 12.5, 100.0),
        "Delphi SendVStopIfChanged exits when worker.vOrder is nil"
    );
    let (_, high, _) = client.take_send_queues_for_test();
    assert!(high.is_empty());

    assert!(orders.mark_local_visual_order(uid));
    assert!(client.update_vstop(&mut orders, uid, true, false, 12.5, 100.0));
    let order = orders.get(uid).unwrap();
    assert!(order.vstop_on);
    assert_eq!(order.vstop_level, 12.5);
    assert_eq!(order.vstop_vol, 100.0);

    let (_, high, _) = client.take_send_queues_for_test();
    assert_eq!(high.len(), 1);
    assert_eq!(high[0].u_key, UniqueKey::order_move(uid));
    match TradeCommand::parse(&high[0].data).expect("valid VStop update") {
        TradeCommand::VStopUpdate(cmd) => {
            assert_eq!(cmd.epoch_header.market.base.uid, uid);
            assert_eq!(cmd.epoch_header.market.currency, 17);
            assert_eq!(cmd.epoch_header.market.platform, 9);
            assert_eq!(cmd.epoch_header.market.market_name, "DOGEUSDT");
            assert_eq!(cmd.epoch_header.epoch, 0);
            assert_eq!(cmd.epoch_header.status, OrderWorkerStatus::SellSet);
            assert!(cmd.vstop_on);
            assert!(!cmd.vstop_fixed);
            assert_eq!(cmd.vstop_level, 12.5);
            assert_eq!(cmd.vstop_vol, 100.0);
        }
        other => panic!("unexpected trade command: {other:?}"),
    }

    assert!(
        !client.update_vstop(&mut orders, uid, true, false, 12.5, 100.0),
        "FPrevVStop* current state was updated before queueing"
    );
    let (_, high, _) = client.take_send_queues_for_test();
    assert!(high.is_empty());
}

#[test]
fn client_ui_mm_subscribe_updates_registry_and_pushes_keyed_send() {
    let client = ready_client();
    client.ui_mm_subscribe(true);

    assert_eq!(
        client.with_subscription_registry(|registry| registry.mm_orders_sub),
        Some(true)
    );
    let (_, high, _) = client.take_send_queues_for_test();
    assert_eq!(high.len(), 1);
    let item = &high[0];
    assert_eq!(
        Client::outgoing_mm_orders_subscribe_intent(item),
        Some(true)
    );
    let uid = command_uid(&item.data).expect("wire command UID");
    assert_eq!(item.u_key, UniqueKey::turn_mm_detection_for(uid));
}

#[test]
fn ui_switches_use_delphi_command_uid_in_u_key() {
    let client = ready_client();

    client.ui_switch_dex("MainDex");
    client.ui_switch_spot(1);

    let (mut sent, mut high, mut low) = client.take_send_queues_for_test();
    sent.append(&mut high);
    sent.append(&mut low);
    assert_eq!(sent.len(), 2);

    let dex_uid = command_uid(&sent[0].data).expect("dex wire UID");
    assert_eq!(sent[0].cmd, Command::UI.to_byte());
    assert_eq!(sent[0].u_key, UniqueKey::dex_switch_for(dex_uid));

    let spot_uid = command_uid(&sent[1].data).expect("spot wire UID");
    assert_eq!(sent[1].cmd, Command::UI.to_byte());
    assert_eq!(sent[1].u_key, UniqueKey::spot_switch_for(spot_uid));
}

#[test]
fn ui_single_slot_commands_use_delphi_fixed_u_key_uid() {
    let client = ready_client();

    let settings = crate::commands::ui::ClientSettingsCommand::default();
    client.ui_send_settings(&settings);

    let lev = crate::commands::ui::LevManage {
        uid: 0,
        cmd_ver: 1,
        auto_max_order: false,
        auto_lev_up: false,
        auto_isolated: false,
        auto_cross: false,
        auto_fix_lev: false,
        fix_lev: 0,
        tlg_report: false,
        lev_control: String::new(),
    };
    client.ui_lev_manage(&lev);

    let (mut sent, mut high, mut low) = client.take_send_queues_for_test();
    sent.append(&mut high);
    sent.append(&mut low);
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0].u_key, UniqueKey::base_ui_settings_slot());
    assert_eq!(sent[1].u_key, UniqueKey::lev_manage_settings_slot());
}

#[test]
fn subscribe_orderbook_inserts_into_registry() {
    let client = ready_client();
    client.subscribe_orderbook("BTC");
    assert!(client.with_subscription_registry(|registry| registry.orderbook_subs.contains("BTC")));
}

#[test]
fn subscribe_orderbooks_inserts_batched_orderbooks_into_registry() {
    let client = ready_client();
    client.subscribe_orderbooks(["BTC", "ETH", "BTC"]);
    client.with_subscription_registry(|registry| {
        assert_eq!(registry.orderbook_subs.len(), 2);
        assert!(registry.orderbook_subs.contains("BTC"));
        assert!(registry.orderbook_subs.contains("ETH"));
    });
}

#[test]
fn unsubscribe_orderbook_removes_from_registry() {
    let client = ready_client();
    client.subscribe_orderbook("BTC");
    client.unsubscribe_orderbook("BTC");
    assert!(!client.with_subscription_registry(|registry| registry.orderbook_subs.contains("BTC")));
}

#[test]
fn batched_unsubscribe_orderbooks_removes_from_registry() {
    let client = Client::new(dummy_cfg());
    client.subscribe_orderbooks(["BTC", "ETH", "XRP"]);
    client.unsubscribe_orderbooks(["ETH", "DOGE"]);
    client.with_subscription_registry(|registry| {
        assert!(registry.orderbook_subs.contains("BTC"));
        assert!(!registry.orderbook_subs.contains("ETH"));
        assert!(registry.orderbook_subs.contains("XRP"));
    });
}

#[test]
fn unsubscribe_all_orderbooks_clears_registry_and_sends_existing_names() {
    let client = ready_client();
    client.subscribe_orderbooks(["BTC", "ETH"]);
    let _ = drain_api_requests(&client);
    client.unsubscribe_all_orderbooks();
    assert!(client.with_subscription_registry(|registry| registry.orderbook_subs.is_empty()));
    let sent = drain_api_requests(&client);
    assert_eq!(sent.len(), 1);
    assert_eq!(
        method_id(&sent[0]),
        Some(EngineMethod::UnsubscribeOrderBook.to_byte())
    );
    assert_eq!(market_names_count(&sent[0]), Some(2));
}

#[test]
fn unsubscribe_all_orderbooks_with_empty_registry_sends_no_wire() {
    let client = ready_client();
    client.unsubscribe_all_orderbooks();
    assert!(client.with_subscription_registry(|registry| registry.orderbook_subs.is_empty()));
    assert!(drain_api_requests(&client).is_empty());
}

#[test]
fn subscribe_orderbook_is_idempotent() {
    // A double subscribe for the same pair must have no side effects
    // on the registry (HashSet dedup) and must not send a second wire request.
    let client = ready_client();
    client.subscribe_orderbook("ETH");
    client.subscribe_orderbook("ETH");
    assert_eq!(
        client.with_subscription_registry(|registry| registry.orderbook_subs.len()),
        1
    );
    let sent = drain_api_requests(&client);
    assert_eq!(sent.len(), 1);
}

#[test]
fn subscribe_all_trades_sets_registry() {
    let client = Client::new(dummy_cfg());
    client.subscribe_all_trades(true);
    assert_eq!(
        client.with_subscription_registry(|registry| registry.trades_sub),
        Some(TradesSubscription { want_mm: true }),
    );
    assert_eq!(
        client.with_subscription_registry(|registry| registry.mm_orders_sub),
        Some(true)
    );
    assert!(client.with_subscription_registry(|registry| registry.trades_storage_scope.is_all()));
    // A repeat with a different want_mm updates the registry.
    client.subscribe_all_trades(false);
    assert_eq!(
        client.with_subscription_registry(|registry| registry.trades_sub),
        Some(TradesSubscription { want_mm: false }),
    );
    assert_eq!(
        client.with_subscription_registry(|registry| registry.mm_orders_sub),
        Some(false)
    );
    assert!(client.with_subscription_registry(|registry| registry.trades_storage_scope.is_all()));
}

#[test]
fn subscribe_trades_for_sets_storage_scope_without_changing_wire_shape() {
    let client = ready_client();
    client.subscribe_trades_for(true, ["ETHUSDT", "BTCUSDT", "ETHUSDT"]);
    client.with_subscription_registry(|registry| {
        assert_eq!(
            registry.trades_sub,
            Some(TradesSubscription { want_mm: true })
        );
        assert_eq!(registry.mm_orders_sub, Some(true));
        assert!(registry.trades_storage_scope.contains("BTCUSDT"));
        assert!(registry.trades_storage_scope.contains("ETHUSDT"));
        assert!(!registry.trades_storage_scope.contains("SOLUSDT"));
    });
    let sent = drain_api_requests(&client);
    assert_eq!(sent.len(), 1);
    assert_eq!(
        method_id(&sent[0]),
        Some(EngineMethod::SubscribeAllTrades.to_byte())
    );
}

#[test]
fn unsubscribe_all_trades_clears_registry() {
    let client = Client::new(dummy_cfg());
    client.subscribe_all_trades(true);
    client.unsubscribe_all_trades();
    assert!(client.with_subscription_registry(|registry| registry.trades_sub.is_none()));
    assert!(client.trades_storage_scope_intent().is_none());
    assert!(crate::events::ActiveDispatchContext::from_client(&client)
        .trades_storage_scope
        .is_none());
    assert_eq!(
        client.with_subscription_registry(|registry| registry.mm_orders_sub),
        Some(true),
        "Delphi UnsubscribeAllTrades clears IsTradesSubscribed but not the MM flag",
    );
}

#[test]
fn apply_mm_orders_subscribe_keeps_all_trades_want_mm() {
    let mut client = Client::new(dummy_cfg());
    client.subscribe_all_trades(false);
    let _ = client.take_send_queues_for_test(); // drain SubscribeAllTrades send command

    client.apply_mm_orders_subscribe_intent(true);

    assert_eq!(
        client.with_subscription_registry(|registry| registry.mm_orders_sub),
        Some(true)
    );
    assert_eq!(
        client.with_subscription_registry(|registry| registry.trades_sub),
        Some(TradesSubscription { want_mm: false }),
    );
}
