//! Runtime-owner loop and command handlers for `MoonClient`.

use super::commands::{
    RuntimeCommand, RuntimeCommandKind, RuntimeCommandRequest, RuntimeReply,
    RuntimeTradeCommandKind, StratRuntimeCommand, UiRuntimeCommand,
};
use super::*;
use std::sync::RwLock;

const ACTIVE_RUNTIME_TICK: Duration = Duration::from_millis(20);

pub(super) fn runtime_loop(
    mut client: Client,
    mut dispatcher: crate::events::EventDispatcher,
    rx: mpsc::Receiver<RuntimeCommand>,
    events_tx: mpsc::Sender<crate::events::Event>,
    snapshot: Arc<RwLock<Option<Arc<crate::events::EventDispatcherSnapshot>>>>,
) {
    let mut auto_candles = Vec::new();
    loop {
        let (stop, changed) = drain_commands(&mut client, &mut dispatcher, &rx, &mut auto_candles);
        if changed {
            publish_snapshot(&dispatcher, &snapshot);
        }
        if stop {
            break;
        }

        client.run_with_dispatcher_worker_queued(ACTIVE_RUNTIME_TICK, &mut dispatcher);

        if poll_auto_candles(&mut auto_candles, &mut dispatcher) {
            publish_snapshot(&dispatcher, &snapshot);
        }

        if publish_queued_events(&mut dispatcher, &events_tx) {
            publish_snapshot(&dispatcher, &snapshot);
        }

        let (stop, changed) = drain_commands(&mut client, &mut dispatcher, &rx, &mut auto_candles);
        if changed {
            publish_snapshot(&dispatcher, &snapshot);
        }
        if stop {
            break;
        }
    }
}

fn drain_commands(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    rx: &mpsc::Receiver<RuntimeCommand>,
    auto_candles: &mut Vec<mpsc::Receiver<crate::client::MergedCandles>>,
) -> (bool, bool) {
    let mut changed = false;
    loop {
        match rx.try_recv() {
            Ok(RuntimeCommand::Stop) | Err(mpsc::TryRecvError::Disconnected) => {
                return (true, changed);
            }
            Ok(cmd) => {
                changed |= handle_command(client, dispatcher, cmd, auto_candles);
            }
            Err(mpsc::TryRecvError::Empty) => return (false, changed),
        }
    }
}

fn handle_command(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    cmd: RuntimeCommand,
    auto_candles: &mut Vec<mpsc::Receiver<crate::client::MergedCandles>>,
) -> bool {
    match cmd {
        RuntimeCommand::Stop => false,
        RuntimeCommand::SubscribeOrderBook(name) => {
            client.subscribe_orderbook(&name);
            false
        }
        RuntimeCommand::SubscribeOrderBooks(names) => {
            client.subscribe_orderbooks(names);
            false
        }
        RuntimeCommand::UnsubscribeOrderBook(name) => {
            client.unsubscribe_orderbook(&name);
            false
        }
        RuntimeCommand::UnsubscribeOrderBooks(names) => {
            client.unsubscribe_orderbooks(names);
            false
        }
        RuntimeCommand::UnsubscribeAllOrderBooks => {
            client.unsubscribe_all_orderbooks();
            false
        }
        RuntimeCommand::SubscribeAllTrades(want_mm) => {
            client.subscribe_all_trades(want_mm);
            sync_runtime_trade_storage_scope(client, dispatcher);
            if client.trades_storage_scope_intent().is_some() {
                schedule_auto_candles_snapshot(client, auto_candles);
            }
            false
        }
        RuntimeCommand::SubscribeTradesFor { want_mm, markets } => {
            client.subscribe_trades_for(want_mm, markets);
            sync_runtime_trade_storage_scope(client, dispatcher);
            if client.trades_storage_scope_intent().is_some() {
                schedule_auto_candles_snapshot(client, auto_candles);
            }
            false
        }
        RuntimeCommand::UnsubscribeAllTrades => {
            client.unsubscribe_all_trades();
            auto_candles.clear();
            sync_runtime_trade_storage_scope(client, dispatcher);
            false
        }
        RuntimeCommand::BalanceRefresh => {
            client.balance_request_refresh();
            false
        }
        RuntimeCommand::Ui(cmd) => {
            handle_ui_command(client, cmd);
            false
        }
        RuntimeCommand::Strat(cmd) => {
            handle_strat_command(client, cmd);
            false
        }
        RuntimeCommand::StrategySetChecked {
            strategy_id,
            checked,
            reply,
        } => {
            let changed = dispatcher.set_strategy_checked(strategy_id, checked);
            let _ = reply.send(changed);
            changed
        }
        RuntimeCommand::StrategySendCheckedDelta => {
            dispatcher.send_strategy_checked_delta(client);
            false
        }
        RuntimeCommand::StrategyStartStop { is_start } => {
            dispatcher.ui_strat_start_stop_v2(client, is_start);
            false
        }
        RuntimeCommand::WithUsizeReply { cmd, reply } => {
            let result = handle_usize_command(client, dispatcher, *cmd);
            let _ = reply.send(result);
            false
        }
        RuntimeCommand::Request { request, reply } => {
            let (response, changed) = handle_request_command(client, dispatcher, request);
            let _ = reply.send(response);
            changed
        }
        RuntimeCommand::OrderAction { kind, reply } => {
            let result = handle_order_action(client, dispatcher, kind);
            let _ = reply.send(result);
            result
        }
        RuntimeCommand::TradeAction { kind, reply } => {
            let result = handle_trade_action(client, dispatcher, kind);
            let _ = reply.send(result);
            false
        }
    }
}

fn handle_request_command(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    request: RuntimeCommandRequest,
) -> (RuntimeReply, bool) {
    match request {
        RuntimeCommandRequest::OrderSnapshot { timeout } => (
            RuntimeReply::OrderSnapshot(client.request_order_snapshot(dispatcher, timeout)),
            true,
        ),
        RuntimeCommandRequest::BalanceSnapshot { timeout } => (
            RuntimeReply::BalanceSnapshot(client.request_balance_snapshot(dispatcher, timeout)),
            true,
        ),
        RuntimeCommandRequest::Balance { asset, timeout } => (
            RuntimeReply::Balance(client.request_balance(dispatcher, &asset, timeout)),
            false,
        ),
        RuntimeCommandRequest::HedgeMode { timeout } => (
            RuntimeReply::HedgeMode(client.request_hedge_mode(dispatcher, timeout)),
            false,
        ),
        RuntimeCommandRequest::ApiExpirationTime { timeout } => (
            RuntimeReply::ApiExpirationTime(
                client.request_api_expiration_time(dispatcher, timeout),
            ),
            false,
        ),
        RuntimeCommandRequest::TransferAssets {
            balance_type,
            timeout,
        } => (
            RuntimeReply::TransferAssets(client.request_transfer_assets(
                dispatcher,
                balance_type,
                timeout,
            )),
            false,
        ),
        RuntimeCommandRequest::CandlesData { timeout } => (
            RuntimeReply::CandlesData(client.request_candles_data(dispatcher, timeout)),
            true,
        ),
        RuntimeCommandRequest::CoinCardCandles {
            market,
            ticks,
            timeout,
        } => (
            RuntimeReply::CoinCardCandles(
                client.request_coin_card_candles(dispatcher, &market, ticks, timeout),
            ),
            false,
        ),
        RuntimeCommandRequest::ClientSettings { timeout } => (
            RuntimeReply::ClientSettings(client.request_client_settings(dispatcher, timeout)),
            true,
        ),
        RuntimeCommandRequest::EngineRaw { payload, timeout } => (
            RuntimeReply::EngineRaw(client.request_engine_response(dispatcher, &payload, timeout)),
            false,
        ),
    }
}

fn handle_usize_command(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    cmd: RuntimeCommand,
) -> usize {
    match cmd {
        RuntimeCommand::StrategySendCheckedDelta => dispatcher.send_strategy_checked_delta(client),
        RuntimeCommand::StrategyStartStop { is_start } => {
            dispatcher.ui_strat_start_stop_v2(client, is_start)
        }
        _ => {
            let mut auto_candles = Vec::new();
            handle_command(client, dispatcher, cmd, &mut auto_candles);
            0
        }
    }
}

fn schedule_auto_candles_snapshot(
    client: &mut Client,
    auto_candles: &mut Vec<mpsc::Receiver<crate::client::MergedCandles>>,
) {
    let (_uid, rx) = client.api_request_candles_data_async_registered();
    auto_candles.push(rx);
}

fn sync_runtime_trade_storage_scope(
    client: &Client,
    dispatcher: &mut crate::events::EventDispatcher,
) {
    let scope = client.trades_storage_scope_intent();
    dispatcher.set_trade_storage_scope(scope.as_deref(), crate::client::delphi_now_raw());
}

fn poll_auto_candles(
    auto_candles: &mut Vec<mpsc::Receiver<crate::client::MergedCandles>>,
    dispatcher: &mut crate::events::EventDispatcher,
) -> bool {
    let mut changed = false;
    let mut i = 0;
    while i < auto_candles.len() {
        match auto_candles[i].try_recv() {
            Ok(merged) => {
                changed |= dispatcher.apply_candles_snapshot(&merged.markets);
                auto_candles.swap_remove(i);
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                auto_candles.swap_remove(i);
            }
            Err(mpsc::TryRecvError::Empty) => {
                i += 1;
            }
        }
    }
    changed
}

fn handle_ui_command(client: &mut Client, cmd: UiRuntimeCommand) {
    match cmd {
        UiRuntimeCommand::SettingsRequest => client.ui_settings_request(),
        UiRuntimeCommand::MmSubscribe(subscribe) => client.ui_mm_subscribe(subscribe),
        UiRuntimeCommand::SendSettings(settings) => client.ui_send_settings(&settings),
        UiRuntimeCommand::UpdateVersion {
            version_name,
            is_release,
        } => client.ui_update_version(&version_name, is_release),
        UiRuntimeCommand::SwitchDex(dex_name) => client.ui_switch_dex(&dex_name),
        UiRuntimeCommand::SwitchSpot(spot_index) => client.ui_switch_spot(spot_index),
    }
}

fn handle_strat_command(client: &mut Client, cmd: StratRuntimeCommand) {
    match cmd {
        StratRuntimeCommand::SellPriceUpdate {
            strategy_id,
            sell_price,
        } => client.strat_sell_price_update(strategy_id, sell_price),
        StratRuntimeCommand::Delete {
            strategy_id,
            folder_path,
        } => client.strat_delete(strategy_id, &folder_path),
    }
}

fn handle_order_action(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    kind: RuntimeCommandKind,
) -> bool {
    match kind {
        RuntimeCommandKind::MoveOrder { uid, new_price } => {
            client.replace_tracked_order(dispatcher.orders_mut(), uid, new_price)
        }
        RuntimeCommandKind::CancelOrder { uid } => {
            client.cancel_tracked_order(dispatcher.orders_mut(), uid)
        }
        RuntimeCommandKind::UpdateStops { uid, stops } => {
            client.update_tracked_order_stops(dispatcher.orders_mut(), uid, &stops)
        }
        RuntimeCommandKind::UpdateVStop {
            uid,
            on,
            fixed,
            level,
            vol,
        } => client.update_tracked_order_vstop(dispatcher.orders_mut(), uid, on, fixed, level, vol),
        RuntimeCommandKind::SetImmune { items } => {
            client.set_immune(dispatcher.orders_mut(), &items)
        }
        RuntimeCommandKind::TurnOrderPanicSell { uid, turn_on } => {
            client.turn_tracked_order_panic_sell(dispatcher.orders_mut(), uid, turn_on)
        }
        RuntimeCommandKind::RequestOrderStatus { uid } => {
            let Some(order) = dispatcher.orders().get(uid).cloned() else {
                return false;
            };
            client.request_tracked_order_status(&order)
        }
        RuntimeCommandKind::SwitchPanicSellByMarket {
            market_name,
            turn_on,
        } => client.switch_panic_sell_by_market(dispatcher.orders_mut(), &market_name, turn_on),
    }
}

fn handle_trade_action(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    kind: RuntimeTradeCommandKind,
) -> Result<bool, TradeContextError> {
    match kind {
        RuntimeTradeCommandKind::NewOrder(params) => {
            let ctx = client.random_trade_ctx()?;
            Ok(client.new_order(
                ctx,
                &params.market,
                params.side.is_short(),
                params.price,
                params.strategy_id.unwrap_or(0),
                params.size,
            ))
        }
        RuntimeTradeCommandKind::JoinOrders { market_name, side } => {
            let ctx = client.random_trade_ctx()?;
            Ok(client.join_orders(ctx, &market_name, side.is_short()))
        }
        RuntimeTradeCommandKind::SplitOrder(params) => {
            let ctx = client.random_trade_ctx()?;
            Ok(client.split_order(
                ctx,
                &params.market,
                params.parts,
                params.split_small,
                params.split_small_sell,
            ))
        }
        RuntimeTradeCommandKind::MoveAllSells {
            market_name,
            params,
        } => {
            let ctx = client.random_trade_ctx()?;
            Ok(client.move_all_sells(dispatcher.orders(), ctx, &market_name, params))
        }
        RuntimeTradeCommandKind::MoveAllBuys {
            market_name,
            params,
        } => {
            let ctx = client.random_trade_ctx()?;
            Ok(client.move_all_buys(dispatcher.orders(), ctx, &market_name, params))
        }
        RuntimeTradeCommandKind::ClosePosition(params) => {
            let ctx = client.random_trade_ctx()?;
            Ok(client.do_close_position(ctx, &params.market, params.market_sell))
        }
        RuntimeTradeCommandKind::LimitClosePosition { market_name, side } => {
            let ctx = client.random_trade_ctx()?;
            Ok(client.do_limit_close_position(ctx, &market_name, side.is_short()))
        }
        RuntimeTradeCommandKind::SplitPosition { market_name, side } => {
            let ctx = client.random_trade_ctx()?;
            Ok(client.do_split_position(ctx, &market_name, side.is_short()))
        }
        RuntimeTradeCommandKind::SellOrder(params) => {
            let ctx = client.random_trade_ctx()?;
            Ok(client.do_sell_order(ctx, &params.market, params.price, params.size))
        }
        RuntimeTradeCommandKind::MarketSplitPosition { market_name, side } => {
            let ctx = client.random_trade_ctx()?;
            Ok(client.do_market_split_position(ctx, &market_name, side.is_short()))
        }
        RuntimeTradeCommandKind::Penalty { market_name } => {
            let ctx = client.random_trade_ctx()?;
            Ok(client.penalty(ctx, &market_name))
        }
    }
}

pub(super) fn publish_queued_events(
    dispatcher: &mut crate::events::EventDispatcher,
    events_tx: &mpsc::Sender<crate::events::Event>,
) -> bool {
    let events = dispatcher.take_queued_events();
    let changed = !events.is_empty();
    for event in events {
        let _ = events_tx.send(event);
    }
    changed
}

pub(super) fn publish_snapshot(
    dispatcher: &crate::events::EventDispatcher,
    snapshot: &RwLock<Option<Arc<crate::events::EventDispatcherSnapshot>>>,
) {
    *snapshot.write().unwrap() = Some(Arc::new(dispatcher.snapshot()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::engine_api::ServerInfo;
    use crate::commands::trade::TradeCommand;

    fn dummy_cfg() -> ClientConfig {
        ClientConfig {
            server_ip: "127.0.0.1".to_string(),
            server_port: 3000,
            master_key: [0; 16],
            mac_key: [0; 16],
            mask_ver: 0,
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
        client.testing_set_domain_ready(true);
        client.set_server_info(ServerInfo {
            exchange_code: Some(9),
            base_currency_code: Some(17),
            ..Default::default()
        });
        client
    }

    #[test]
    fn moon_trade_new_order_derives_route_and_builds_delphi_wire() {
        let mut client = ready_client();
        let mut dispatcher = crate::events::EventDispatcher::new();

        let queued = handle_trade_action(
            &mut client,
            &mut dispatcher,
            RuntimeTradeCommandKind::NewOrder(
                NewOrderParams::new("DOGEUSDT", OrderSide::Short, 12.5, 0.25).with_strategy_id(42),
            ),
        )
        .expect("BaseCheck route is present");

        assert!(queued);
        let (_, high, _) = client.take_send_queues_for_test();
        assert_eq!(high.len(), 1);
        match TradeCommand::parse(&high[0].data).expect("valid new order") {
            TradeCommand::NewOrder(cmd) => {
                assert_eq!(cmd.market.market_name, "DOGEUSDT");
                assert_eq!(cmd.market.currency, 17);
                assert_eq!(cmd.market.platform, 9);
                assert!(cmd.is_short);
                assert_eq!(cmd.price, 12.5);
                assert_eq!(cmd.strat_id, 42);
                assert_eq!(cmd.order_size, 0.25);
            }
            other => panic!("unexpected trade command: {other:?}"),
        }
    }

    #[test]
    fn moon_trade_returns_route_error_before_base_check_fields() {
        let mut client = Client::new(dummy_cfg());
        client.testing_set_domain_ready(true);
        let mut dispatcher = crate::events::EventDispatcher::new();

        let err = handle_trade_action(
            &mut client,
            &mut dispatcher,
            RuntimeTradeCommandKind::Penalty {
                market_name: "DOGEUSDT".to_string(),
            },
        )
        .expect_err("new Client has no BaseCheck route");

        assert!(err.missing_exchange_code);
        assert!(err.missing_base_currency_code);
        let (sliced, high, low) = client.take_send_queues_for_test();
        assert!(sliced.is_empty() && high.is_empty() && low.is_empty());
    }
}
