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
                changed |= handle_command_profiled(client, dispatcher, cmd, pending);
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
            cmd => changed |= handle_command_profiled(client, dispatcher, cmd, pending),
        }
    }
    let (stop, live_changed) = drain_commands(client, dispatcher, rx, pending);
    (stop, changed || live_changed)
}

fn handle_command_profiled(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    cmd: RuntimeCommand,
    pending: &mut RuntimePending,
) -> bool {
    #[cfg(any(test, feature = "diagnostics"))]
    let (kind, payload_len) = cmd.profile_source();
    #[cfg(any(test, feature = "diagnostics"))]
    let start = Instant::now();
    let changed = handle_command(client, dispatcher, cmd, pending);
    #[cfg(any(test, feature = "diagnostics"))]
    client
        .metrics
        .protocol_metrics
        .record_profile_phase_labeled(
            ProfilePhase::RuntimeCommandDispatch,
            start.elapsed(),
            crate::client::metrics::RUNTIME_PROFILE_CMD,
            kind,
            payload_len,
        );
    changed
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
        RuntimeCommand::SubscribeCandles { markets, kind } => {
            client.subscribe_candles(markets, kind);
            false
        }
        RuntimeCommand::UnsubscribeCandles(markets) => {
            client.unsubscribe_candles(markets);
            false
        }
        RuntimeCommand::SetDeltasByTrades(enabled) => {
            dispatcher.set_deltas_by_trades(enabled);
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
            client.request_orders_snapshot();
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
        RuntimeCommand::Ui(cmd) => handle_ui_command(client, dispatcher, cmd),
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
        RuntimeCommand::ReportSchemaRefresh => {
            client.request_report_schema_at(client.now_ms());
            false
        }
        RuntimeCommand::ReportSync { ticket, request } => {
            client.set_report_sync_intent(request);
            let can_send = matches!(client.auth_status, crate::client::AuthStatus::AuthDone)
                && client.subscriptions.domain_ready
                && client.server_token != 0;
            if !can_send {
                dispatcher.defer_report_sync_until_schema(ticket, request);
                return false;
            }
            if dispatcher.report_schema().is_some() && client.report_schema_is_current() {
                let request_uid = dispatcher.begin_report_sync(ticket, request);
                client.send_report_sync_at(request_uid, request, client.now_ms());
            } else {
                dispatcher.defer_report_sync_until_schema(ticket, request);
                client.request_report_schema_at(client.now_ms());
            }
            false
        }
        RuntimeCommand::ReportPageApplied(page) => {
            if !client.report_page_is_waiting_apply(page.request_uid) {
                log::warn!(
                    target: "moonproto::reports",
                    "ignored report page acknowledgement outside the active apply barrier: sync={} request={}",
                    page.ticket.sync_id,
                    page.request_uid
                );
                return false;
            }
            match dispatcher.report_page_applied(&page) {
                crate::state::ReportPageApplyAction::SendNext {
                    request_uid,
                    request,
                } => {
                    if client.finish_report_page_apply(page.request_uid, None) {
                        client.set_report_sync_intent(request);
                        if client.report_schema_is_current() {
                            client.send_report_sync_at(request_uid, request, client.now_ms());
                        } else {
                            client.request_report_schema_at(client.now_ms());
                        }
                    }
                }
                crate::state::ReportPageApplyAction::Complete {
                    received_request_uid,
                    durable_request,
                } => {
                    client.finish_report_page_apply(received_request_uid, Some(durable_request));
                }
                crate::state::ReportPageApplyAction::Ignored => {
                    log::warn!(
                        target: "moonproto::reports",
                        "ignored stale or mismatched report page acknowledgement: sync={} request={}",
                        page.ticket.sync_id,
                        page.request_uid
                    );
                }
            }
            false
        }
        RuntimeCommand::ReportCheckOpenRows(rec_ids) => {
            client.set_report_open_rows_intent(Arc::clone(&rec_ids));
            if rec_ids.is_empty() {
                dispatcher.clear_report_open_rows_check();
                return false;
            }
            let can_send = matches!(client.auth_status, crate::client::AuthStatus::AuthDone)
                && client.subscriptions.domain_ready
                && client.server_token != 0;
            if !can_send
                || dispatcher.report_schema().is_none()
                || !client.report_schema_is_current()
            {
                dispatcher.defer_report_open_rows_check_until_schema(rec_ids);
                if can_send {
                    client.request_report_schema_at(client.now_ms());
                }
                return false;
            }
            dispatcher.begin_report_open_rows_check(Arc::clone(&rec_ids));
            client.send_report_open_rows_check_at(&rec_ids, client.now_ms());
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
        #[cfg(any(test, feature = "diagnostics"))]
        RuntimeCommand::DiagFillMarketHistoryToCapacity {
            market_name,
            now_time,
            span_ms,
            reply,
        } => {
            let filled =
                dispatcher.diag_fill_market_history_to_capacity(&market_name, now_time, span_ms);
            let _ = reply.send(filled);
            false
        }
        RuntimeCommand::OrderAction(kind) => {
            let result = handle_order_action(client, dispatcher, &kind);
            if !result {
                queue_order_action_rejected(dispatcher, &kind);
            }
            result
        }
        RuntimeCommand::TradeAction(kind) => match handle_trade_action(client, dispatcher, kind) {
            Ok(changed) => changed,
            Err(err) => {
                log::warn!(target: "moonproto::active_runtime", "trade intent rejected: {err}");
                false
            }
        },
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

fn handle_ui_command(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    cmd: UiRuntimeCommand,
) -> bool {
    match cmd {
        UiRuntimeCommand::SettingsRequest => {
            client.ui_settings_request();
            false
        }
        UiRuntimeCommand::MmSubscribe(subscribe) => {
            client.ui_mm_subscribe(subscribe);
            false
        }
        UiRuntimeCommand::SendSettings(settings) => {
            client.ui_send_settings(&settings);
            false
        }
        UiRuntimeCommand::UpdateVersion {
            version_name,
            is_release,
        } => {
            client.ui_update_version(&version_name, is_release);
            false
        }
        UiRuntimeCommand::SwitchDex(dex_name) => {
            client.ui_switch_dex(&dex_name);
            false
        }
        UiRuntimeCommand::SwitchSpot(spot) => {
            client.ui_switch_spot(spot.to_byte());
            false
        }
        UiRuntimeCommand::LevManage(cmd) => {
            client.ui_lev_manage(&cmd);
            false
        }
        UiRuntimeCommand::EmuTrades {
            market_index,
            base_time,
            points,
        } => {
            client.ui_emu_trades(market_index, base_time, &points);
            false
        }
        UiRuntimeCommand::TriggerManage {
            action,
            all_markets,
            markets,
            keys,
        } => {
            client.ui_trigger_manage(action, all_markets, &markets, &keys);
            false
        }
        UiRuntimeCommand::ResetProfit(kind) => {
            client.ui_reset_profit(kind);
            false
        }
        UiRuntimeCommand::ArbActivateNotify(valid_days) => {
            client.ui_arb_activate_notify(valid_days);
            false
        }
        UiRuntimeCommand::AlertObject(cmd) => {
            client.ui_alert_object(&cmd);
            false
        }
        UiRuntimeCommand::AlertSnapshotRequest => {
            client.ui_alert_snapshot_request();
            false
        }
        UiRuntimeCommand::ChartTextState(cmd) => {
            let changed = dispatcher.chart_text.set_visible_market(&cmd);
            client.ui_chart_text_state(&cmd);
            changed
        }
        UiRuntimeCommand::OrdersHistoryRequest(market_name) => {
            client.ui_orders_history_request(&market_name);
            false
        }
        UiRuntimeCommand::RestartNow => {
            client.ui_restart_now();
            false
        }
        UiRuntimeCommand::KernelLicenseStateRequest => {
            client.ui_kernel_license_state_request(0);
            false
        }
        UiRuntimeCommand::AutoDetect(active) => {
            client.ui_auto_detect(active);
            false
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
    #[cfg(any(test, feature = "diagnostics"))]
    let strategy_count = strategies.len();
    if dispatcher.strats().strategy_schema().is_none() {
        log::warn!(
            target: "moonproto::active_runtime",
            "strategy snapshot batch ignored: live strategy schema is not available"
        );
        return false;
    }
    let server_epoch = dispatcher.mark_local_strategies_changed();
    #[cfg(any(test, feature = "diagnostics"))]
    let state_started = Instant::now();
    dispatcher.set_local_strategies_owned(strategies);
    #[cfg(any(test, feature = "diagnostics"))]
    client
        .metrics
        .protocol_metrics
        .record_profile_phase_labeled(
            ProfilePhase::StrategySnapshotState,
            state_started.elapsed(),
            crate::client::metrics::RUNTIME_PROFILE_CMD,
            50,
            strategy_count,
        );
    #[cfg(any(test, feature = "diagnostics"))]
    let serialize_started = Instant::now();
    let Some(reply) = dispatcher.local_strategy_snapshot_reply() else {
        log::warn!(
            target: "moonproto::active_runtime",
            "strategy snapshot batch ignored: local strategy payload could not be serialized"
        );
        return true;
    };
    #[cfg(any(test, feature = "diagnostics"))]
    client
        .metrics
        .protocol_metrics
        .record_profile_phase_labeled(
            ProfilePhase::StrategySnapshotSerialize,
            serialize_started.elapsed(),
            crate::client::metrics::RUNTIME_PROFILE_CMD,
            50,
            reply.data.len(),
        );
    #[cfg(any(test, feature = "diagnostics"))]
    let send_started = Instant::now();
    client.strat_send_snapshot_payload(
        server_epoch,
        reply.client_max_last_date,
        false,
        &reply.data,
    );
    #[cfg(any(test, feature = "diagnostics"))]
    client
        .metrics
        .protocol_metrics
        .record_profile_phase_labeled(
            ProfilePhase::StrategySnapshotSend,
            send_started.elapsed(),
            crate::client::metrics::RUNTIME_PROFILE_CMD,
            50,
            reply.data.len(),
        );
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
        RuntimeCommandKind::RequestOrderStatus { uid } => client.request_tracked_order_status(*uid),
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
            client.new_order(
                request_uid,
                &params.market,
                params.side.is_short(),
                params.price,
                params.strategy_id.unwrap_or(0),
                params.size,
                params.planned_sell_price,
                params.use_market_stop,
            );
            Ok(false)
        }
        RuntimeTradeCommandKind::JoinOrders { market_name, side } => {
            client.join_orders(random_nonzero_u64(), &market_name, side.is_short());
            Ok(false)
        }
        RuntimeTradeCommandKind::SplitOrder(params) => {
            client.split_order(
                random_nonzero_u64(),
                params.order.uid(),
                params.parts,
                params.is_strategy_piece(),
                params.sells_strategy_piece(),
            );
            Ok(false)
        }
        RuntimeTradeCommandKind::MoveAllSells {
            market_name,
            params,
        } => {
            client.move_all_sells(dispatcher.orders(), &market_name, params);
            Ok(false)
        }
        RuntimeTradeCommandKind::MoveAllBuys {
            market_name,
            params,
        } => {
            client.move_all_buys(dispatcher.orders(), &market_name, params);
            Ok(false)
        }
        RuntimeTradeCommandKind::ClosePosition(params) => {
            client.do_close_position(
                random_nonzero_u64(),
                &params.market,
                params.uses_market_order(),
            );
            Ok(false)
        }
        RuntimeTradeCommandKind::LimitClosePosition { market_name, side } => {
            client.do_limit_close_position(random_nonzero_u64(), &market_name, side.is_short());
            Ok(false)
        }
        RuntimeTradeCommandKind::SplitPosition { market_name, side } => {
            client.do_split_position(random_nonzero_u64(), &market_name, side.is_short());
            Ok(false)
        }
        RuntimeTradeCommandKind::SellOrder(params) => {
            client.do_sell_order(
                random_nonzero_u64(),
                &params.market,
                params.price,
                params.size,
            );
            Ok(false)
        }
        RuntimeTradeCommandKind::MarketSplitPosition { market_name, side } => {
            client.do_market_split_position(random_nonzero_u64(), &market_name, side.is_short());
            Ok(false)
        }
        RuntimeTradeCommandKind::Penalty { market_name } => {
            let ctx = client.random_trade_ctx()?;
            client.penalty(ctx, &market_name);
            Ok(false)
        }
        RuntimeTradeCommandKind::PanicSellAll => {
            if !client.panic_sell_all(random_nonzero_u64()) {
                return Ok(false);
            }
            Ok(dispatcher.orders_mut().mark_panic_sell_all())
        }
    }
}

fn random_nonzero_u64() -> u64 {
    loop {
        let value = rand::random::<u64>();
        if value != 0 {
            return value;
        }
    }
}
