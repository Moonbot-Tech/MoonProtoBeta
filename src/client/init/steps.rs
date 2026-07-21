//! Per-step Engine API helpers for the one-time Init sequence.
//!
//! These are the building blocks shared by the runtime-owned
//! [`RuntimeInitMachine`](super::machine::RuntimeInitMachine) and the test-only
//! linear `run_init_sequence`: critical-step status tracking, BaseCheck/AuthCheck
//! drivers, GetMarketsList/UpdateMarketsList apply helpers, the strategy-schema
//! step, and the post-init resync.

use super::*;

pub(super) struct PendingEngineInit {
    request_uid: Option<u64>,
    rx: mpsc::Receiver<EngineResponse>,
    deadline: Instant,
}

pub(super) enum PendingEnginePoll {
    Pending,
    Response(EngineResponse),
    Timeout,
    Disconnected,
}

pub(super) enum StrategySchemaPoll {
    Pending,
    Ready,
    Failed(InitError),
}

pub(super) fn begin_engine_init_step(
    client: &Client,
    request_payload: Vec<u8>,
    timeout: Duration,
) -> PendingEngineInit {
    let request_uid = engine_request_uid(&request_payload);
    let rx = client.send_api_request_async(&request_payload);
    PendingEngineInit {
        request_uid,
        rx,
        deadline: Instant::now() + timeout,
    }
}

pub(super) fn poll_engine_init_step(
    client: &mut Client,
    pending: &mut PendingEngineInit,
) -> PendingEnginePoll {
    match pending.rx.try_recv() {
        Ok(resp) => PendingEnginePoll::Response(resp),
        Err(mpsc::TryRecvError::Disconnected) => PendingEnginePoll::Disconnected,
        Err(mpsc::TryRecvError::Empty) => {
            if Instant::now() >= pending.deadline {
                if let Some(uid) = pending.request_uid {
                    client.pending_api.api_pending.remove(uid);
                }
                PendingEnginePoll::Timeout
            } else {
                PendingEnginePoll::Pending
            }
        }
    }
}

/// Run the full init sequence: BaseCheck → AuthCheck → GetMarketsList →
/// UpdateMarketsList →
/// Delphi post-init resync → optional subscriptions.
///
/// Until this function completes successfully,
/// `EventDispatcher::dispatch_into_active` drops domain pushes
/// (`Order`/`Strat`/`Balance`/`Trades*`/`OrderBook`/`UI`), matching Delphi
/// `ClientNewData` under `not InitDone`. After a successful bootstrap, the
/// library sends `TOrderStatusRequest(OrderID=0, ExactRev=0)`, `TSettingsRequest`,
/// `TStratSnapshot.CreateFromStrats(...)`, `TMMOrdersSubscribeCommand`, and
/// `TRequestBalanceRefresh`. The dispatcher also answers later server
/// `TStratSnapshotRequest` commands from the same library-owned strategy list.
///
/// On successful BaseCheck, the helper parses [`ServerInfo`] and stores it in
/// `client.server_info()` for multi-server identification.
///
/// Critical step timing follows the Delphi reference: `TMoonProtoEngine.FTimeout`
/// is 12000 ms for each `SendAndWait` request. Rust keeps pumping the client
/// loop while it waits for each Engine API response. If a UI command marked
/// `ServerUpdateSent`, the Init spine also mirrors Delphi `BaseCheck`:
/// wait up to 34 * 300 ms for `AuthDone`, clear the marker, send BaseCheck once,
/// and if it still fails retry it 10 times with 2000 ms pauses. All init steps
/// above are mandatory: a timeout/error means Init failed and `domain_ready`
/// stays closed.
///
/// [`ServerInfo`]: crate::commands::engine_api::ServerInfo
#[derive(Debug, Clone)]
pub(crate) enum CriticalInitStatus {
    Skipped,
    Ok,
    Failed(String),
    TimedOut,
}

impl CriticalInitStatus {
    pub(super) fn is_ok(&self) -> bool {
        matches!(self, Self::Ok)
    }

    pub(super) fn final_error(&self, step: &'static str) -> Option<InitError> {
        match self {
            Self::Ok | Self::Skipped => None,
            Self::TimedOut => Some(InitError::CriticalStepTimedOut(step)),
            Self::Failed(message) => Some(InitError::CriticalStepFailed {
                step,
                message: message.clone(),
            }),
        }
    }
}

pub(super) fn response_error_message(resp: &EngineResponse) -> String {
    format!("code={} msg={}", resp.error_code, resp.error_msg)
}

pub(super) fn check_init_shutdown(client: &Client) -> Result<(), InitError> {
    if client.shutdown_requested() {
        Err(InitError::SendChannelClosed)
    } else {
        Ok(())
    }
}

#[cfg(test)]
pub(super) fn pump_client_for(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    duration: Duration,
) {
    client.with_owned_runtime_stepper(dispatcher, |client, stepper, _dispatcher| {
        stepper.step_for(client, _dispatcher, duration);
    });
}

pub(super) fn fire_init_step(client: &mut Client, step: &'static str, start: Instant) {
    client.fire_lifecycle(LifecycleEvent::InitStepCompleted {
        step,
        elapsed_ms: start.elapsed().as_millis() as u64,
    });
}

#[cfg(test)]
pub(super) fn run_base_check_once(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    result: &mut InitResult,
    timeout: Duration,
) -> Result<CriticalInitStatus, InitError> {
    let req = crate::commands::engine_request::base_check();
    match client.request_engine_response_for_init(dispatcher, &req, timeout) {
        Ok(resp) if resp.success => {
            result.base_check_ok = true;
            let info = parse_base_check_response(&resp.data);
            client.set_server_info(info);
            Ok(CriticalInitStatus::Ok)
        }
        Ok(resp) => {
            let message = response_error_message(&resp);
            result.errors.push(format!("BaseCheck error: {message}"));
            Ok(CriticalInitStatus::Failed(message))
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            result.errors.push("BaseCheck timeout".to_string());
            Ok(CriticalInitStatus::TimedOut)
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => Err(InitError::SendChannelClosed),
    }
}

#[cfg(test)]
pub(super) fn wait_auth_done_after_server_update(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
) {
    for _ in 0..DELPHI_BASE_CHECK_UPDATE_AUTH_WAITS {
        if client.shutdown_requested() {
            break;
        }
        if client.is_authorized() {
            break;
        }
        pump_client_for(
            client,
            dispatcher,
            Duration::from_millis(DELPHI_BASE_CHECK_UPDATE_AUTH_WAIT_MS),
        );
    }
}

#[cfg(test)]
pub(crate) fn run_base_check_delphi(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    result: &mut InitResult,
    timeout: Duration,
    waiting_update: bool,
    retry_pause: Duration,
) -> Result<CriticalInitStatus, InitError> {
    let errors_before = result.errors.len();
    let mut status = run_base_check_once(client, dispatcher, result, timeout)?;
    if waiting_update && !status.is_ok() {
        for _ in 0..DELPHI_BASE_CHECK_UPDATE_RETRIES {
            check_init_shutdown(client)?;
            pump_client_for(client, dispatcher, retry_pause);
            check_init_shutdown(client)?;
            status = run_base_check_once(client, dispatcher, result, timeout)?;
            if status.is_ok() {
                break;
            }
        }
    }
    if status.is_ok() {
        result.errors.truncate(errors_before);
    }
    Ok(status)
}

#[cfg(test)]
pub(super) fn run_auth_check_once(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    result: &mut InitResult,
    timeout: Duration,
) -> Result<CriticalInitStatus, InitError> {
    let req = crate::commands::engine_request::auth_check();
    match client.request_engine_response_for_init(dispatcher, &req, timeout) {
        Ok(resp) if resp.success => {
            let len = resp.data.len();
            match parse_auth_check_response(&resp.data) {
                Some(auth) => {
                    client.set_auth_info(auth.clone());
                    result.auth_info = Some(auth);
                }
                None => {
                    result
                        .errors
                        .push(format!("AuthCheck parse: malformed payload ({len} bytes)"));
                }
            }
            result.auth_check_ok = true;
            Ok(CriticalInitStatus::Ok)
        }
        Ok(resp) => {
            let message = response_error_message(&resp);
            result.errors.push(format!("AuthCheck error: {message}"));
            Ok(CriticalInitStatus::Failed(message))
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            result.errors.push("AuthCheck timeout".to_string());
            Ok(CriticalInitStatus::TimedOut)
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => Err(InitError::SendChannelClosed),
    }
}

#[cfg(test)]
pub(super) fn run_required_engine_step(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    result: &mut InitResult,
    step: &'static str,
    req: Vec<u8>,
    timeout: Duration,
) -> Result<EngineResponse, InitError> {
    match client.request_engine_response_for_init(dispatcher, &req, timeout) {
        Ok(resp) if resp.success => Ok(resp),
        Ok(resp) => {
            let message = response_error_message(&resp);
            result.errors.push(format!("{step} error: {message}"));
            Err(InitError::CriticalStepFailed { step, message })
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            result.errors.push(format!("{step}: timeout"));
            Err(InitError::CriticalStepTimedOut(step))
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => Err(InitError::SendChannelClosed),
    }
}

fn malformed_required_engine_step(
    result: &mut InitResult,
    step: &'static str,
    len: usize,
) -> InitError {
    let message = format!("malformed payload ({len} bytes)");
    result.errors.push(format!("{step}: {message}"));
    InitError::CriticalStepFailed { step, message }
}

pub(super) fn apply_required_get_markets_list_response(
    dispatcher: &mut crate::events::EventDispatcher,
    resp: &EngineResponse,
    result: &mut InitResult,
) -> Result<(), InitError> {
    let mut events = Vec::new();
    if !dispatcher.apply_get_markets_list_response(resp, &mut events) {
        return Err(malformed_required_engine_step(
            result,
            "GetMarketsList",
            resp.data.len(),
        ));
    }
    dispatcher.queue_events(events);
    Ok(())
}

pub(super) fn apply_required_update_markets_list_response(
    dispatcher: &mut crate::events::EventDispatcher,
    resp: &EngineResponse,
    result: &mut InitResult,
) -> Result<(), InitError> {
    let mut events = Vec::new();
    if !dispatcher.apply_update_markets_list_response(resp, 0, None, &mut events) {
        return Err(malformed_required_engine_step(
            result,
            "UpdateMarketsList",
            resp.data.len(),
        ));
    }
    dispatcher.queue_events(events);
    Ok(())
}

pub(super) struct PendingStrategySchemaStep {
    schema_revision_before: u64,
    schema_failures_before: u64,
    start: Instant,
    next_request_at: Instant,
}

pub(super) fn begin_required_strategy_schema_step(
    client: &mut Client,
    dispatcher: &crate::events::EventDispatcher,
) -> PendingStrategySchemaStep {
    let start = Instant::now();
    let pending = PendingStrategySchemaStep {
        schema_revision_before: dispatcher.strats().strategy_schema_revision(),
        schema_failures_before: dispatcher.strats().strategy_schema_failures(),
        start,
        next_request_at: start + Duration::from_millis(SETTINGS_HELPER_RETRY_PAUSE_MS),
    };
    client.strat_schema_request();
    pending
}

pub(super) fn poll_required_strategy_schema_step(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    result: &mut InitResult,
    pending: &mut PendingStrategySchemaStep,
    timeout: Duration,
) -> StrategySchemaPoll {
    const STEP: &str = "TStratSchemaRequest";

    if let Err(err) = check_init_shutdown(client) {
        return StrategySchemaPoll::Failed(err);
    }
    if dispatcher.strats().strategy_schema_revision() != pending.schema_revision_before {
        let Some(schema) = dispatcher.strats().strategy_schema() else {
            let message = "schema revision advanced but schema state is empty".to_string();
            result.errors.push(format!("{STEP}: {message}"));
            return StrategySchemaPoll::Failed(InitError::CriticalStepFailed {
                step: STEP,
                message,
            });
        };
        result.strategy_schema_raw_bytes = dispatcher
            .strats()
            .strategy_schema_raw()
            .map_or(0, |raw| raw.len());
        result.strategy_schema_kind_count = schema.kinds.len();
        result.strategy_schema_field_count = schema.fields.len();
        return StrategySchemaPoll::Ready;
    }

    if dispatcher.strats().strategy_schema_failures() != pending.schema_failures_before {
        let message = dispatcher
            .strats()
            .strategy_schema_last_error()
            .unwrap_or("strategy schema parse failed")
            .to_string();
        result.errors.push(format!("{STEP}: {message}"));
        return StrategySchemaPoll::Failed(InitError::CriticalStepFailed {
            step: STEP,
            message,
        });
    }

    if timeout_remaining(pending.start, timeout).is_none() {
        result.errors.push(format!("{STEP}: timeout"));
        return StrategySchemaPoll::Failed(InitError::CriticalStepTimedOut(STEP));
    }

    let now = Instant::now();
    if now >= pending.next_request_at {
        client.strat_schema_request();
        pending.next_request_at = now + Duration::from_millis(SETTINGS_HELPER_RETRY_PAUSE_MS);
    }

    StrategySchemaPoll::Pending
}

#[cfg(test)]
pub(super) fn finish_required_strategy_schema_step(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    result: &mut InitResult,
    pending: &mut PendingStrategySchemaStep,
    timeout: Duration,
) -> Result<(), InitError> {
    const STEP: &str = "TStratSchemaRequest";

    client.with_owned_runtime_stepper(dispatcher, |client, stepper, dispatcher| loop {
        check_init_shutdown(client)?;
        if dispatcher.strats().strategy_schema_revision() != pending.schema_revision_before {
            let Some(schema) = dispatcher.strats().strategy_schema() else {
                let message = "schema revision advanced but schema state is empty".to_string();
                result.errors.push(format!("{STEP}: {message}"));
                return Err(InitError::CriticalStepFailed {
                    step: STEP,
                    message,
                });
            };
            result.strategy_schema_raw_bytes = dispatcher
                .strats()
                .strategy_schema_raw()
                .map_or(0, |raw| raw.len());
            result.strategy_schema_kind_count = schema.kinds.len();
            result.strategy_schema_field_count = schema.fields.len();
            return Ok(());
        }

        if dispatcher.strats().strategy_schema_failures() != pending.schema_failures_before {
            let message = dispatcher
                .strats()
                .strategy_schema_last_error()
                .unwrap_or("strategy schema parse failed")
                .to_string();
            result.errors.push(format!("{STEP}: {message}"));
            return Err(InitError::CriticalStepFailed {
                step: STEP,
                message,
            });
        }

        if timeout_remaining(pending.start, timeout).is_none() {
            result.errors.push(format!("{STEP}: timeout"));
            return Err(InitError::CriticalStepTimedOut(STEP));
        }

        let now = Instant::now();
        if now >= pending.next_request_at {
            client.strat_schema_request();
            pending.next_request_at = now + Duration::from_millis(SETTINGS_HELPER_RETRY_PAUSE_MS);
        }

        if !stepper.step(client, dispatcher) {
            return Err(InitError::SendChannelClosed);
        }
        stepper.barrier();
    })
}

pub(crate) fn send_post_init_resync(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    cfg: &InitConfig,
    result: &mut InitResult,
) {
    client.request_orders_snapshot();
    if let Some(snapshot) = dispatcher.pending_or_local_strategy_snapshot_reply() {
        client.strat_send_snapshot_payload(
            snapshot.server_epoch,
            snapshot.client_max_last_date,
            snapshot.full,
            &snapshot.data,
        );
    } else {
        client.strat_schema_request();
    }
    client.ui_settings_request();
    let registry_mm_orders = client
        .subscriptions
        .subscription_registry
        .lock()
        .mm_orders_sub;
    let mm_orders = cfg
        .mm_orders_subscribe
        .or(registry_mm_orders)
        .unwrap_or(false);
    client.apply_mm_orders_subscribe_intent(mm_orders);
    client.send_mm_orders_subscribe_cmd(mm_orders);
    client.balance_request_refresh();
    result.post_init_resync_sent = true;
}
