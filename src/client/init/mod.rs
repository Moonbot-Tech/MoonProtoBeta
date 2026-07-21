//! MoonBot-compatible domain init sequence owned by `MoonClient`.

use super::active_runtime::TradesStreamMode;
use super::*;

// =============================================================================
// Internal one-time init spine:
//
// `BaseCheck -> AuthCheck -> GetMarketsList -> UpdateMarketsList
//  -> strategy schema -> post-init resync -> optional subscriptions`.
//
// This mirrors Delphi `TCryptoPumpTool.InitInt`, but it is not a public
// application runtime model. `MoonClient::connect` owns this sequence inside its
// runtime thread and reports completion through lifecycle events.
//
// The init code is split across this directory:
//   - `config`  — the `InitConfig`/`ConnectConfig` inputs, `InitResult`, and the
//                 `InitError`/`ConnectError` types.
//   - `machine` — the runtime-owned, non-blocking `RuntimeInitMachine`.
//   - `steps`   — the per-step Engine API helpers shared by the machine and the
//                 test-only linear `run_init_sequence` glue below.
// =============================================================================

mod config;
mod machine;
mod steps;

// Re-glob `steps` so each sibling's `use super::*;` (super = this module) also
// sees the shared per-step helpers and pending-state types it exposes via
// `pub(super)`. `config`/`machine` items reach the siblings through the
// explicit re-exports below.
use steps::*;

pub(crate) use config::InitResult;
pub use config::{ConnectConfig, ConnectError, InitConfig, InitError, InitialStrategies};
pub(crate) use machine::{RuntimeInitMachine, RuntimeInitPoll};
#[cfg(test)]
pub(crate) use steps::run_base_check_delphi;
pub(crate) use steps::{send_post_init_resync, CriticalInitStatus};

#[cfg(test)]
fn wait_until_authorized(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    timeout: Duration,
) -> Result<(), ConnectError> {
    let started = Instant::now();
    client.with_owned_runtime_stepper(dispatcher, |client, stepper, _dispatcher| {
        while !client.is_authorized() {
            if client.shutdown_requested() {
                return Err(ConnectError::Canceled);
            }
            if timeout_remaining(started, timeout).is_none() {
                return Err(ConnectError::ConnectTimedOut { timeout });
            }
            if !stepper.step(client, _dispatcher) {
                return Err(ConnectError::Canceled);
            }
            stepper.barrier();
        }
        Ok(())
    })
}

#[cfg(test)]
pub(crate) fn connect_and_init(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    cfg: ConnectConfig,
) -> Result<InitResult, ConnectError> {
    if let Some(initial) = cfg.init.initial_strategies.as_ref() {
        dispatcher.set_local_strategy_epoch(initial.epoch);
        dispatcher.set_local_strategies(&initial.strategies);
    }

    if !client.is_authorized() {
        wait_until_authorized(client, dispatcher, cfg.connect_timeout)?;
    }

    match run_init_sequence(client, dispatcher, cfg.init) {
        Ok(result) => Ok(result),
        Err(_) if client.shutdown_requested() => Err(ConnectError::Canceled),
        Err(err) => Err(ConnectError::from(err)),
    }
}

/// Run the MoonBot-compatible one-time domain initialization sequence.
///
/// Internal one-time domain initialization sequence after transport
/// authorization. A successful run opens the
/// dispatcher domain gate and sends the post-init refresh set:
/// strategy schema request, order snapshot, client strategy snapshot, settings
/// request, MM-orders subscription flag, balance refresh, and optional stream
/// subscriptions.
/// Incoming `TStratSnapshotRequest` is still answered from the same
/// library-owned strategy state.
///
/// Do not call this again after a reconnect in the same [`Client`] session.
/// Reconnect restore is owned by the library once init has succeeded.
#[cfg(test)]
pub(crate) fn run_init_sequence(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    cfg: InitConfig,
) -> Result<InitResult, InitError> {
    let init_started_at = Instant::now();
    let waiting_update = client.take_server_update_sent();
    if waiting_update {
        wait_auth_done_after_server_update(client, dispatcher);
    }
    check_init_shutdown(client)?;

    if !client.is_authorized() {
        return Err(InitError::NotAuthenticated);
    }

    let timeout = cfg.step_timeout.unwrap_or(Duration::from_millis(
        crate::api_pending::DEFAULT_PENDING_TIMEOUT_MS as u64,
    ));
    let mut result = InitResult::default();
    let mut strategy_schema: Option<PendingStrategySchemaStep> = None;

    // === 1. BaseCheck/AuthCheck === Delphi InitInt first auth block.
    // On success, parse server identity and store it in Client.server_info
    // (multi-server support: the application distinguishes servers via `client.server_info().bot_id`).
    let auth_block_errors_before = result.errors.len();
    let mut base_status = run_base_check_delphi(
        client,
        dispatcher,
        &mut result,
        timeout,
        waiting_update,
        Duration::from_millis(DELPHI_BASE_CHECK_UPDATE_RETRY_PAUSE_MS),
    )?;
    if base_status.is_ok() {
        fire_init_step(client, "BaseCheck", init_started_at);
        strategy_schema = Some(begin_required_strategy_schema_step(client, dispatcher));
    }

    // === 2. AuthCheck ===
    let mut auth_status = if base_status.is_ok() {
        run_auth_check_once(client, dispatcher, &mut result, timeout)?
    } else {
        CriticalInitStatus::Skipped
    };
    if auth_status.is_ok() {
        fire_init_step(client, "AuthCheck", init_started_at);
    }

    // Delphi `TCryptoPumpTool.InitInt`: if either BaseCheck or AuthCheck failed,
    // sleep 200ms, call BaseCheck once more, then assign Result from AuthCheck.
    // The second BaseCheck still refreshes local ServerInfo if it succeeds, but
    // the retry branch's final gate is the second AuthCheck.
    let used_init_auth_retry = !base_status.is_ok() || !auth_status.is_ok();
    if used_init_auth_retry {
        check_init_shutdown(client)?;
        pump_client_for(
            client,
            dispatcher,
            Duration::from_millis(DELPHI_INIT_AUTH_RETRY_PAUSE_MS),
        );
        check_init_shutdown(client)?;
        base_status = run_base_check_once(client, dispatcher, &mut result, timeout)?;
        if base_status.is_ok() {
            fire_init_step(client, "BaseCheck", init_started_at);
            if strategy_schema.is_none() {
                strategy_schema = Some(begin_required_strategy_schema_step(client, dispatcher));
            }
        }
        auth_status = run_auth_check_once(client, dispatcher, &mut result, timeout)?;
        if auth_status.is_ok() {
            fire_init_step(client, "AuthCheck", init_started_at);
        }
    }

    if !used_init_auth_retry {
        if let Some(err) = base_status.final_error("BaseCheck") {
            return Err(err);
        }
    } else if auth_status.is_ok() {
        result.errors.truncate(auth_block_errors_before);
    }
    if let Some(err) = auth_status.final_error("AuthCheck") {
        return Err(err);
    }

    // Agreed active-library behavior: unlike the Delphi UI client, the Rust
    // library asks the server for the live strategy schema during Init so API
    // consumers get field types, picklists, visibility and chapters without
    // hardcoded Rust copies of TStrategy metadata.
    //
    // The schema does not depend on AuthCheck/markets/indexes/prices, only on a
    // live authorized transport. Start it after the first successful BaseCheck
    // (or the fallback successful auth path) and let the normal Engine API waits
    // pump its Sliced response in parallel with AuthCheck and the critical
    // market init steps. Strategy schema, snapshot request/runtime state,
    // core runtime/license state, and news/history are the startup-safe
    // exceptions to the general pre-domain gate.
    // A pre-init TStratSnapshotRequest is only latched by EventDispatcher; the
    // actual TStratSnapshot reply is sent by post-init resync after schema/state
    // are ready. The rest of MPC_Strat remains gated until domain_ready.
    if strategy_schema.is_none() {
        strategy_schema = Some(begin_required_strategy_schema_step(client, dispatcher));
    }

    // === 3. GetMarketsList === critical Delphi init step.
    // Delphi `ProcessApiCommand` only parks the response in PendingRequests;
    // `TMoonProtoEngine.GetMarketsList` applies markets after SendAndWait.
    let resp = run_required_engine_step(
        client,
        dispatcher,
        &mut result,
        "GetMarketsList",
        crate::commands::engine_request::get_markets_list(),
        timeout,
    )?;
    apply_required_get_markets_list_response(dispatcher, &resp, &mut result)?;
    result.markets_response_bytes = resp.data.len();
    fire_init_step(client, "GetMarketsList", init_started_at);

    // Delphi `GetMarketsList` rebuilds `SrvMarkets` during cold init and stores
    // `FLastServerAppToken := PeerAppToken`. A separate `GetMarketsIndexes`
    // request is not part of `TCryptoPumpTool.InitInt`; it is only the stale
    // token/reconnect repair path inside `TMoonProtoEngine.UpdateMarketsList`.
    client.reconnect.tracked_indexes_peer_app_token = client.peer_app_token;

    // === 4. UpdateMarketsList === critical: Delphi InitInt does exactly
    // `GetMarketsList and UpdateMarketsList`; if the token later becomes stale,
    // reconnect/periodic restore must run `GetMarketsIndexes` before prices.
    let resp = run_required_engine_step(
        client,
        dispatcher,
        &mut result,
        "UpdateMarketsList",
        crate::commands::engine_request::update_markets_list(),
        timeout,
    )?;
    apply_required_update_markets_list_response(dispatcher, &resp, &mut result)?;
    result.update_markets_response_bytes = resp.data.len();
    fire_init_step(client, "UpdateMarketsList", init_started_at);

    client.subscriptions.domain_restore = DomainRestoreIntent {
        fetch_indexes: true,
    };
    client.set_domain_ready(true);

    if let Err(err) = finish_required_strategy_schema_step(
        client,
        dispatcher,
        &mut result,
        strategy_schema
            .as_mut()
            .expect("strategy schema step must be started before finish"),
        timeout,
    ) {
        client.set_domain_ready(false);
        return Err(err);
    }
    fire_init_step(client, "StrategySchema", init_started_at);

    send_post_init_resync(client, dispatcher, &cfg, &mut result);
    client.send_registry_subscriptions_after_init();

    // === 6. SubscribeAllTrades === optional; registry update + direct wire enqueue.
    if let Some(mode) = cfg.subscribe_trades {
        client.subscribe_all_trades(mode.want_market_makers());
        result.trades_subscribed = true;
    }

    // === 7. Subscribe orderbooks === optional; fire-and-forget via registry
    for name in &cfg.subscribe_orderbooks {
        client.subscribe_orderbook(name);
        result.orderbooks_subscribed += 1;
    }

    // === 8. Pump queued post-init wire commands ===
    // post-init resync and optional subscriptions have already appended wire
    // commands to Delphi-style send queues; run a short tick so they are flushed
    // before the helper returns.
    if result.post_init_resync_sent
        || cfg.subscribe_trades.is_some()
        || !cfg.subscribe_orderbooks.is_empty()
    {
        pump_client_for(client, dispatcher, Duration::from_millis(100));
        check_init_shutdown(client)?;
        fire_init_step(client, "PostInitFlush", init_started_at);
    }

    Ok(result)
}
