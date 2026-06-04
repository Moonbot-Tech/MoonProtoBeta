//! Runtime command intake: draining the command channel, dispatching each
//! [`RuntimeCommand`], and scheduling the async Engine API requests whose
//! pending state lives in [`super::pending`].

use super::*;

pub(super) fn engine_pending_deadline() -> Instant {
    Instant::now() + Duration::from_millis(crate::api_pending::DEFAULT_PENDING_TIMEOUT_MS as u64)
}

pub(super) fn drain_commands(
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

pub(super) fn drain_commands_during_startup(
    rx: &mpsc::Receiver<RuntimeCommand>,
    deferred: &mut VecDeque<RuntimeCommand>,
) -> (bool, bool) {
    loop {
        match rx.try_recv() {
            Ok(RuntimeCommand::Stop) | Err(mpsc::TryRecvError::Disconnected) => {
                return (true, false);
            }
            Ok(cmd) => deferred.push_back(cmd),
            Err(mpsc::TryRecvError::Empty) => return (false, false),
        }
    }
}

pub(super) fn drain_deferred_and_live_commands(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    rx: &mpsc::Receiver<RuntimeCommand>,
    pending: &mut RuntimePending,
    deferred: &mut VecDeque<RuntimeCommand>,
) -> (bool, bool) {
    let mut changed = false;
    while let Some(cmd) = deferred.pop_front() {
        match cmd {
            RuntimeCommand::Stop => return (true, changed),
            cmd => changed |= handle_command(client, dispatcher, cmd, pending),
        }
    }
    let (stop, live_changed) = drain_commands(client, dispatcher, rx, pending);
    (stop, changed || live_changed)
}

pub(super) fn handle_command(
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
            clear_auto_candles_pending(client, pending);
            pending.auto_candles_scope = None;
            sync_runtime_trade_storage_scope(client, dispatcher);
            false
        }
        RuntimeCommand::BalanceRefresh => {
            client.balance_request_refresh();
            false
        }
        RuntimeCommand::AccountHedgeModeRefresh => {
            schedule_account_refresh(
                client,
                &mut pending.account_refreshes,
                PendingAccountRefreshKind::HedgeMode,
                crate::commands::engine_request::query_hedge_mode(),
            );
            false
        }
        RuntimeCommand::AccountApiExpirationRefresh => {
            schedule_account_refresh(
                client,
                &mut pending.account_refreshes,
                PendingAccountRefreshKind::ApiExpiration,
                crate::commands::engine_request::check_api_expiration_time(),
            );
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
        RuntimeCommand::SetExcludeBlacklistedMarketsFromExchangeDelta(exclude) => dispatcher
            .markets
            .set_exclude_blacklisted_markets_from_exchange_delta(exclude),
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
        RuntimeCommand::StrategySnapshotBatch(strategies) => {
            handle_strategy_snapshot_batch(client, dispatcher, strategies)
        }
        RuntimeCommand::StrategySetChecked {
            strategy_id,
            checked,
        } => dispatcher.set_strategy_checked(strategy_id, checked),
        RuntimeCommand::StrategySendCheckedDelta => {
            dispatcher.send_strategy_checked_delta(client);
            false
        }
        RuntimeCommand::StrategyStartStop { is_start } => {
            dispatcher.ui_strat_start_stop_v2(client, is_start);
            false
        }
        #[cfg(any(test, feature = "diagnostics"))]
        RuntimeCommand::DebugOutgoingBlackhole(enabled) => {
            client.debug_set_outgoing_blackhole(enabled);
            false
        }
        #[cfg(any(test, feature = "diagnostics"))]
        RuntimeCommand::DebugResetErrEmuDiagnostics => {
            client.reset_err_emu_diagnostics();
            false
        }
        RuntimeCommand::OrderAction(kind) => {
            let result = handle_order_action(client, dispatcher, &kind);
            if !result {
                queue_order_action_rejected(dispatcher, &kind);
            }
            result
        }
        RuntimeCommand::TradeAction(kind) => {
            if let Err(err) = handle_trade_action(client, dispatcher, kind) {
                log::warn!(target: "moonproto::active_runtime", "trade intent rejected: {err}");
            }
            false
        }
    }
}

pub(super) fn schedule_auto_candles_snapshot(client: &mut Client, pending: &mut RuntimePending) {
    let Some(scope) = client.trades_storage_scope_intent() else {
        return;
    };
    if pending.auto_candles_scope.as_deref() != Some(scope.as_ref()) {
        clear_auto_candles_pending(client, pending);
        pending.auto_candles_scope = Some(scope);
    }
    if pending.auto_candles_requested {
        return;
    }
    let (uid, rx) = client.api_request_candles_data_async_registered();
    pending.auto_candles_requested = true;
    pending.auto_candles.push(PendingAutoCandles {
        uid,
        deadline: engine_pending_deadline(),
        rx,
    });
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
    let payload = crate::commands::engine_request::update_transfer_assets(kind.to_byte());
    let request_uid = engine_request_uid(&payload);
    let rx = client.send_api_request_async(&payload);
    pending.push(PendingTransferAssets {
        kind,
        batch_id,
        request_uid,
        deadline: engine_pending_deadline(),
        rx,
    });
}

fn schedule_engine_action(
    client: &mut Client,
    pending: &mut Vec<PendingEngineAction>,
    kind: crate::events::EngineActionKind,
    ticket: super::super::EngineActionTicket,
    payload: Vec<u8>,
) {
    let rx = client.send_api_request_async(&payload);
    pending.push(PendingEngineAction {
        kind,
        ticket,
        deadline: engine_pending_deadline(),
        rx,
    });
}

fn schedule_coin_card_candles(
    client: &mut Client,
    pending: &mut Vec<PendingCoinCardCandles>,
    ticket: super::super::CoinCardCandlesTicket,
    payload: Vec<u8>,
) {
    let rx = client.send_api_request_async(&payload);
    pending.push(PendingCoinCardCandles {
        ticket,
        deadline: engine_pending_deadline(),
        rx,
    });
}

fn schedule_account_refresh(
    client: &mut Client,
    pending: &mut Vec<PendingAccountRefresh>,
    kind: PendingAccountRefreshKind,
    payload: Vec<u8>,
) {
    let request_uid = engine_request_uid(&payload);
    let rx = client.send_api_request_async(&payload);
    pending.push(PendingAccountRefresh {
        kind,
        request_uid,
        deadline: engine_pending_deadline(),
        rx,
    });
}

pub(super) fn sync_runtime_trade_storage_scope(
    client: &Client,
    dispatcher: &mut crate::events::EventDispatcher,
) {
    let scope = client.trades_storage_scope_intent();
    dispatcher.set_trade_storage_scope(scope.as_deref(), crate::client::delphi_now_raw());
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
        UiRuntimeCommand::SwitchSpot(spot) => client.ui_switch_spot(spot.to_byte()),
        UiRuntimeCommand::LevManage(cmd) => client.ui_lev_manage(&cmd),
        UiRuntimeCommand::EmuTrades {
            market_index,
            base_time,
            points,
        } => client.ui_emu_trades(market_index, base_time, &points),
        UiRuntimeCommand::TriggerManage {
            action,
            all_markets,
            markets,
            keys,
        } => client.ui_trigger_manage(action, all_markets, &markets, &keys),
        UiRuntimeCommand::ResetProfit(kind) => client.ui_reset_profit(kind),
        UiRuntimeCommand::ArbActivateNotify(valid_days) => {
            client.ui_arb_activate_notify(valid_days)
        }
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

fn handle_strategy_snapshot_batch(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    strategies: Vec<crate::commands::strategy_serializer::StrategySnapshot>,
) -> bool {
    let Some(schema) = dispatcher.strats().strategy_schema().cloned() else {
        log::warn!(
            target: "moonproto::active_runtime",
            "strategy snapshot batch ignored: live strategy schema is not available"
        );
        return false;
    };
    let server_epoch = dispatcher.mark_local_strategies_changed();
    dispatcher.set_local_strategies(&strategies);
    client.strat_send_snapshot_batch(server_epoch, false, &schema, &strategies);
    true
}

fn handle_order_action(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    kind: &RuntimeCommandKind,
) -> bool {
    match kind {
        RuntimeCommandKind::MoveOrder { uid, new_price } => {
            client.replace_tracked_order(dispatcher.orders_mut(), *uid, *new_price)
        }
        RuntimeCommandKind::CancelOrder { uid } => {
            client.cancel_tracked_order(dispatcher.orders_mut(), *uid)
        }
        RuntimeCommandKind::UpdateStops { uid, stops } => {
            client.update_tracked_order_stops(dispatcher.orders_mut(), *uid, stops)
        }
        RuntimeCommandKind::UpdateVStop { uid, params } => client.update_tracked_order_vstop(
            dispatcher.orders_mut(),
            *uid,
            params.enabled,
            params.fixed,
            params.level,
            params.volume,
        ),
        RuntimeCommandKind::SetImmune { items } => {
            client.set_immune(dispatcher.orders_mut(), items)
        }
        RuntimeCommandKind::TurnOrderPanicSell { uid, turn_on } => {
            client.turn_tracked_order_panic_sell(dispatcher.orders_mut(), *uid, *turn_on)
        }
        RuntimeCommandKind::RequestOrderStatus { uid } => {
            let Some(order) = dispatcher.orders().get(*uid).cloned() else {
                return false;
            };
            client.request_tracked_order_status(&order)
        }
        RuntimeCommandKind::SwitchPanicSellByMarket {
            market_name,
            turn_on,
        } => client.switch_panic_sell_by_market(dispatcher.orders_mut(), market_name, *turn_on),
    }
}

fn queue_order_action_rejected(
    dispatcher: &mut crate::events::EventDispatcher,
    kind: &RuntimeCommandKind,
) {
    #[cfg(not(any(test, feature = "diagnostics")))]
    {
        let _ = (dispatcher, kind);
    }
    #[cfg(any(test, feature = "diagnostics"))]
    {
        let uid = match kind {
            RuntimeCommandKind::MoveOrder { uid, .. }
            | RuntimeCommandKind::CancelOrder { uid }
            | RuntimeCommandKind::UpdateStops { uid, .. }
            | RuntimeCommandKind::UpdateVStop { uid, .. }
            | RuntimeCommandKind::TurnOrderPanicSell { uid, .. }
            | RuntimeCommandKind::RequestOrderStatus { uid } => Some(*uid),
            RuntimeCommandKind::SetImmune { .. }
            | RuntimeCommandKind::SwitchPanicSellByMarket { .. } => None,
        };
        if let Some(uid) = uid {
            dispatcher.queue_events([crate::events::Event::Order(
                crate::state::OrderEvent::Ignored {
                    uid,
                    reason: crate::state::ApplyResult::NotApplicable,
                },
            )]);
        }
    }
}

pub(super) fn handle_trade_action(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    kind: RuntimeTradeCommandKind,
) -> Result<bool, TradeContextError> {
    match kind {
        RuntimeTradeCommandKind::NewOrder {
            params,
            request_uid,
        } => {
            let ctx = client.trade_ctx(request_uid)?;
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
                params.is_strategy_piece(),
                params.sells_strategy_piece(),
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
            Ok(client.do_close_position(ctx, &params.market, params.uses_market_order()))
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
