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
    let mut pending = RuntimePending::default();
    if client.trades_storage_scope_intent().is_some() {
        sync_runtime_trade_storage_scope(&client, &mut dispatcher);
        schedule_auto_candles_snapshot(&mut client, &mut pending);
    }
    loop {
        let (stop, changed) = drain_commands(&mut client, &mut dispatcher, &rx, &mut pending);
        if changed {
            publish_snapshot(&dispatcher, &snapshot);
        }
        if stop {
            break;
        }

        client.run_with_dispatcher_worker_queued(ACTIVE_RUNTIME_TICK, &mut dispatcher);

        let candles_changed = poll_auto_candles(&mut pending, &mut dispatcher);
        let coin_card_changed =
            poll_coin_card_candles(&mut pending.coin_card_candles, &mut dispatcher);
        let transfer_assets_changed = poll_transfer_assets(&mut pending, &mut dispatcher);
        poll_engine_actions(&mut pending.engine_actions, &mut dispatcher);
        if candles_changed || coin_card_changed || transfer_assets_changed {
            publish_snapshot(&dispatcher, &snapshot);
        }

        if publish_queued_events(&mut dispatcher, &events_tx) {
            publish_snapshot(&dispatcher, &snapshot);
        }

        let (stop, changed) = drain_commands(&mut client, &mut dispatcher, &rx, &mut pending);
        if changed {
            publish_snapshot(&dispatcher, &snapshot);
        }
        if stop {
            break;
        }
    }
}

#[derive(Default)]
struct RuntimePending {
    auto_candles_scope: Option<std::sync::Arc<crate::state::TradeStorageScope>>,
    auto_candles_requested: bool,
    auto_candles: Vec<PendingAutoCandles>,
    auto_candles_apply: Vec<PendingAutoCandlesApply>,
    coin_card_candles: Vec<PendingCoinCardCandles>,
    transfer_assets: Vec<PendingTransferAssets>,
    transfer_assets_batches: Vec<PendingTransferAssetsBatch>,
    next_transfer_assets_batch_id: u64,
    engine_actions: Vec<PendingEngineAction>,
}

struct PendingAutoCandles {
    uid: u64,
    rx: mpsc::Receiver<crate::client::MergedCandles>,
}

struct PendingAutoCandlesApply {
    uid: u64,
    summary: crate::state::CandlesSnapshotApplySummary,
    rx: mpsc::Receiver<()>,
}

struct PendingTransferAssets {
    kind: crate::state::ExchangeKind,
    batch_id: Option<u64>,
    rx: mpsc::Receiver<crate::commands::engine_api::EngineResponse>,
}

struct PendingTransferAssetsBatch {
    id: u64,
    remaining: usize,
    updated: usize,
    failed: usize,
}

struct PendingCoinCardCandles {
    ticket: super::CoinCardCandlesTicket,
    rx: mpsc::Receiver<crate::commands::engine_api::EngineResponse>,
}

struct PendingEngineAction {
    kind: crate::events::EngineActionKind,
    ticket: super::EngineActionTicket,
    rx: mpsc::Receiver<crate::commands::engine_api::EngineResponse>,
}

fn drain_commands(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    rx: &mpsc::Receiver<RuntimeCommand>,
    pending: &mut RuntimePending,
) -> (bool, bool) {
    let mut changed = false;
    loop {
        match rx.try_recv() {
            Ok(RuntimeCommand::Stop) | Err(mpsc::TryRecvError::Disconnected) => {
                return (true, changed);
            }
            Ok(cmd) => {
                changed |= handle_command(client, dispatcher, cmd, pending);
            }
            Err(mpsc::TryRecvError::Empty) => return (false, changed),
        }
    }
}

fn handle_command(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    cmd: RuntimeCommand,
    pending: &mut RuntimePending,
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
            schedule_auto_candles_snapshot(client, pending);
            false
        }
        RuntimeCommand::SubscribeTradesFor { want_mm, markets } => {
            client.subscribe_trades_for(want_mm, markets);
            sync_runtime_trade_storage_scope(client, dispatcher);
            schedule_auto_candles_snapshot(client, pending);
            false
        }
        RuntimeCommand::UnsubscribeAllTrades => {
            client.unsubscribe_all_trades();
            pending.auto_candles.clear();
            pending.auto_candles_apply.clear();
            pending.auto_candles_requested = false;
            pending.auto_candles_scope = None;
            sync_runtime_trade_storage_scope(client, dispatcher);
            false
        }
        RuntimeCommand::BalanceRefresh => {
            client.balance_request_refresh();
            false
        }
        RuntimeCommand::OrderSnapshotRefresh => {
            client.request_all_statuses(rand::random());
            false
        }
        RuntimeCommand::TransferAssetsRefresh => {
            schedule_transfer_assets_refresh(client, pending);
            false
        }
        RuntimeCommand::TransferAssetsRefreshKind(kind) => {
            schedule_transfer_assets_refresh_kind(client, &mut pending.transfer_assets, kind, None);
            false
        }
        RuntimeCommand::EngineAction {
            kind,
            ticket,
            payload,
        } => {
            schedule_engine_action(client, &mut pending.engine_actions, kind, ticket, payload);
            false
        }
        RuntimeCommand::CoinCardCandles { ticket, payload } => {
            schedule_coin_card_candles(client, &mut pending.coin_card_candles, ticket, payload);
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
        RuntimeCommandRequest::TransferAssets { kind, timeout } => (
            RuntimeReply::TransferAssets(client.request_transfer_assets(dispatcher, kind, timeout)),
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
            let mut pending = RuntimePending::default();
            handle_command(client, dispatcher, cmd, &mut pending);
            0
        }
    }
}

fn schedule_auto_candles_snapshot(client: &mut Client, pending: &mut RuntimePending) {
    let Some(scope) = client.trades_storage_scope_intent() else {
        return;
    };
    if pending.auto_candles_scope.as_deref() != Some(scope.as_ref()) {
        pending.auto_candles.clear();
        pending.auto_candles_apply.clear();
        pending.auto_candles_requested = false;
        pending.auto_candles_scope = Some(scope);
    }
    if pending.auto_candles_requested {
        return;
    }
    let (uid, rx) = client.api_request_candles_data_async_registered();
    pending.auto_candles_requested = true;
    pending.auto_candles.push(PendingAutoCandles { uid, rx });
}

fn schedule_transfer_assets_refresh(client: &mut Client, pending: &mut RuntimePending) {
    pending.next_transfer_assets_batch_id =
        pending.next_transfer_assets_batch_id.wrapping_add(1).max(1);
    let batch_id = pending.next_transfer_assets_batch_id;
    pending
        .transfer_assets_batches
        .push(PendingTransferAssetsBatch {
            id: batch_id,
            remaining: crate::state::ExchangeKind::ALL.len(),
            updated: 0,
            failed: 0,
        });
    for kind in crate::state::ExchangeKind::ALL {
        schedule_transfer_assets_refresh_kind(
            client,
            &mut pending.transfer_assets,
            kind,
            Some(batch_id),
        );
    }
}

fn schedule_transfer_assets_refresh_kind(
    client: &mut Client,
    pending: &mut Vec<PendingTransferAssets>,
    kind: crate::state::ExchangeKind,
    batch_id: Option<u64>,
) {
    let rx = client.api_update_transfer_assets(kind);
    pending.push(PendingTransferAssets { kind, batch_id, rx });
}

fn schedule_engine_action(
    client: &mut Client,
    pending: &mut Vec<PendingEngineAction>,
    kind: crate::events::EngineActionKind,
    ticket: super::EngineActionTicket,
    payload: Vec<u8>,
) {
    let rx = client.send_api_request_async(&payload);
    pending.push(PendingEngineAction { kind, ticket, rx });
}

fn schedule_coin_card_candles(
    client: &mut Client,
    pending: &mut Vec<PendingCoinCardCandles>,
    ticket: super::CoinCardCandlesTicket,
    payload: Vec<u8>,
) {
    let rx = client.send_api_request_async(&payload);
    pending.push(PendingCoinCardCandles { ticket, rx });
}

fn sync_runtime_trade_storage_scope(
    client: &Client,
    dispatcher: &mut crate::events::EventDispatcher,
) {
    let scope = client.trades_storage_scope_intent();
    dispatcher.set_trade_storage_scope(scope.as_deref(), crate::client::delphi_now_raw());
}

fn poll_auto_candles(
    pending: &mut RuntimePending,
    dispatcher: &mut crate::events::EventDispatcher,
) -> bool {
    let mut changed = false;
    let mut i = 0;
    while i < pending.auto_candles.len() {
        match pending.auto_candles[i].rx.try_recv() {
            Ok(merged) => {
                let request_uid = merged.uid;
                let fallback_uid = pending.auto_candles[i].uid;
                let summary = dispatcher.apply_candles_snapshot(&merged.markets);
                pending.auto_candles.swap_remove(i);
                if let Some(summary) = summary {
                    if let Some(rx) = dispatcher.market_history_barrier_async() {
                        pending.auto_candles_apply.push(PendingAutoCandlesApply {
                            uid: request_uid,
                            summary,
                            rx,
                        });
                    } else {
                        dispatcher.queue_candles_snapshot_event(
                            crate::state::CandlesSnapshotEvent::Failed {
                                request_uid: Some(request_uid),
                                error: "market history worker unavailable after snapshot apply"
                                    .to_string(),
                            },
                        );
                        changed = true;
                    }
                } else {
                    dispatcher.queue_candles_snapshot_event(
                        crate::state::CandlesSnapshotEvent::Failed {
                            request_uid: Some(if request_uid != 0 {
                                request_uid
                            } else {
                                fallback_uid
                            }),
                            error: "candles snapshot was not applied to retained history"
                                .to_string(),
                        },
                    );
                    changed = true;
                }
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                let uid = pending.auto_candles.swap_remove(i).uid;
                dispatcher.queue_candles_snapshot_event(
                    crate::state::CandlesSnapshotEvent::Failed {
                        request_uid: Some(uid),
                        error: "pending full candles receiver closed before response".to_string(),
                    },
                );
                changed = true;
            }
            Err(mpsc::TryRecvError::Empty) => {
                i += 1;
            }
        }
    }

    let mut i = 0;
    while i < pending.auto_candles_apply.len() {
        match pending.auto_candles_apply[i].rx.try_recv() {
            Ok(()) => {
                let applied = pending.auto_candles_apply.swap_remove(i);
                dispatcher.queue_candles_snapshot_event(
                    crate::state::CandlesSnapshotEvent::Ready {
                        request_uid: applied.uid,
                        summary: applied.summary,
                    },
                );
                changed = true;
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                let applied = pending.auto_candles_apply.swap_remove(i);
                dispatcher.queue_candles_snapshot_event(
                    crate::state::CandlesSnapshotEvent::Failed {
                        request_uid: Some(applied.uid),
                        error: "market history worker barrier closed before ack".to_string(),
                    },
                );
                changed = true;
            }
            Err(mpsc::TryRecvError::Empty) => {
                i += 1;
            }
        }
    }
    changed
}

fn poll_coin_card_candles(
    pending: &mut Vec<PendingCoinCardCandles>,
    dispatcher: &mut crate::events::EventDispatcher,
) -> bool {
    let mut changed = false;
    let mut i = 0;
    while i < pending.len() {
        match pending[i].rx.try_recv() {
            Ok(resp) => {
                let ticket = pending.swap_remove(i).ticket;
                changed |=
                    dispatcher.apply_coin_card_candles_response(ticket.market, ticket.kind, resp);
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                let ticket = pending.swap_remove(i).ticket;
                dispatcher.coin_card_candles_request_failed(
                    ticket.market,
                    ticket.kind,
                    ticket.request_uid,
                    "pending CoinCard candles receiver closed before response",
                );
            }
            Err(mpsc::TryRecvError::Empty) => {
                i += 1;
            }
        }
    }
    changed
}

fn poll_transfer_assets(
    pending: &mut RuntimePending,
    dispatcher: &mut crate::events::EventDispatcher,
) -> bool {
    let mut changed = false;
    let mut i = 0;
    while i < pending.transfer_assets.len() {
        match pending.transfer_assets[i].rx.try_recv() {
            Ok(resp) => {
                let item = pending.transfer_assets.swap_remove(i);
                let success = dispatcher.apply_transfer_assets_response(item.kind, resp);
                changed |= success;
                finish_transfer_assets_batch_item(pending, dispatcher, item.batch_id, success);
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                let item = pending.transfer_assets.swap_remove(i);
                dispatcher.transfer_assets_request_failed(
                    item.kind,
                    "pending transfer-assets receiver closed before response",
                );
                changed = true;
                finish_transfer_assets_batch_item(pending, dispatcher, item.batch_id, false);
            }
            Err(mpsc::TryRecvError::Empty) => {
                i += 1;
            }
        }
    }
    changed
}

fn finish_transfer_assets_batch_item(
    pending: &mut RuntimePending,
    dispatcher: &mut crate::events::EventDispatcher,
    batch_id: Option<u64>,
    success: bool,
) {
    let Some(batch_id) = batch_id else {
        return;
    };
    let Some(pos) = pending
        .transfer_assets_batches
        .iter()
        .position(|batch| batch.id == batch_id)
    else {
        return;
    };
    let batch = &mut pending.transfer_assets_batches[pos];
    batch.remaining = batch.remaining.saturating_sub(1);
    if success {
        batch.updated += 1;
    } else {
        batch.failed += 1;
    }
    if batch.remaining != 0 {
        return;
    }
    let batch = pending.transfer_assets_batches.swap_remove(pos);
    dispatcher.queue_events([crate::events::Event::TransferAssets(
        crate::state::TransferAssetsEvent::RefreshCompleted {
            request_id: batch.id,
            requested: batch.updated + batch.failed,
            updated: batch.updated,
            failed: batch.failed,
            revision: dispatcher.transfer_assets().revision(),
        },
    )]);
}

fn poll_engine_actions(
    pending: &mut Vec<PendingEngineAction>,
    dispatcher: &mut crate::events::EventDispatcher,
) {
    let mut i = 0;
    while i < pending.len() {
        match pending[i].rx.try_recv() {
            Ok(resp) => {
                let kind = pending[i].kind.clone();
                dispatcher.queue_engine_action_response(kind, resp);
                pending.swap_remove(i);
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                let action = pending.swap_remove(i);
                dispatcher.queue_engine_action_disconnected(
                    action.kind,
                    action.ticket.request_uid,
                    action.ticket.method,
                    "pending Engine API action receiver closed before response",
                );
            }
            Err(mpsc::TryRecvError::Empty) => {
                i += 1;
            }
        }
    }
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

    #[test]
    fn auto_candles_snapshot_is_one_shot_for_current_trades_scope() {
        let mut client = ready_client();
        let mut dispatcher = crate::events::EventDispatcher::new();
        let mut pending = RuntimePending::default();

        handle_command(
            &mut client,
            &mut dispatcher,
            RuntimeCommand::SubscribeAllTrades(false),
            &mut pending,
        );
        assert!(pending.auto_candles_requested);
        assert_eq!(pending.auto_candles.len(), 1);

        handle_command(
            &mut client,
            &mut dispatcher,
            RuntimeCommand::SubscribeAllTrades(false),
            &mut pending,
        );
        assert_eq!(
            pending.auto_candles.len(),
            1,
            "same trades scope must not schedule duplicate full candles requests"
        );

        handle_command(
            &mut client,
            &mut dispatcher,
            RuntimeCommand::UnsubscribeAllTrades,
            &mut pending,
        );
        assert!(!pending.auto_candles_requested);
        assert!(pending.auto_candles.is_empty());
        assert!(pending.auto_candles_apply.is_empty());
        assert!(pending.auto_candles_scope.is_none());
    }

    #[test]
    fn init_time_trades_scope_schedules_auto_candles_when_runtime_starts() {
        let mut client = ready_client();
        client.subscribe_all_trades(false);
        let mut dispatcher = crate::events::EventDispatcher::new();
        let mut pending = RuntimePending::default();

        sync_runtime_trade_storage_scope(&client, &mut dispatcher);
        schedule_auto_candles_snapshot(&mut client, &mut pending);

        assert!(pending.auto_candles_requested);
        assert_eq!(pending.auto_candles.len(), 1);
    }

    #[test]
    fn transfer_assets_batch_emits_completion_after_all_kinds_finish() {
        let mut pending = RuntimePending::default();
        pending
            .transfer_assets_batches
            .push(PendingTransferAssetsBatch {
                id: 7,
                remaining: 3,
                updated: 0,
                failed: 0,
            });
        let mut dispatcher = crate::events::EventDispatcher::new();

        finish_transfer_assets_batch_item(&mut pending, &mut dispatcher, Some(7), true);
        assert!(dispatcher.take_queued_events().is_empty());
        finish_transfer_assets_batch_item(&mut pending, &mut dispatcher, Some(7), false);
        assert!(dispatcher.take_queued_events().is_empty());
        finish_transfer_assets_batch_item(&mut pending, &mut dispatcher, Some(7), true);

        assert!(matches!(
            dispatcher.take_queued_events().as_slice(),
            [crate::events::Event::TransferAssets(
                crate::state::TransferAssetsEvent::RefreshCompleted {
                    request_id: 7,
                    requested: 3,
                    updated: 2,
                    failed: 1,
                    ..
                }
            )]
        ));
        assert!(pending.transfer_assets_batches.is_empty());
    }
}
