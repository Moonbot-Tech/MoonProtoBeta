//! MoonBot-compatible domain init sequence helpers.

use super::*;

// =============================================================================
//  Init sequence helper — free function (НЕ метод Client)
//
//  Логически единственный init-проход после `Connected{fresh:true}`:
//  `BaseCheck → AuthCheck → GetMarketsList → GetMarketsIndexes → UpdateMarketsList
//   → Delphi post-init resync → optional subscriptions`.
//  Аналог Delphi `TCryptoPumpTool.InitInt` (`Unit1.pas:4987-5150`).
//
//  Почему free function, а не `Client::run_init_sequence`:
//   - low-level finite pumps занимают `&mut Client` на всё
//     время выполнения (main loop крутится). Метод-helper не мог бы быть вызван
//     ВО ВРЕМЯ работы run().
//   - Free function принимает `&mut Client` явно — компилятор уровнем доказывает
//     что run() не запущен (иначе borrow checker не пустит). Helper вызывается
//     между run-сессиями: после `Connected{fresh:true}` короткий run завершается,
//     app зовёт `run_init_sequence(&mut client, cfg)`, затем входит в main run.
//   - Pattern в trading_flow.rs — Phase 1 (15s short run) → run_init_sequence →
//     Phase 5 (long run). Эта free function — упаковка этого pattern'а в один
//     вызов с retry/timeout/error handling.
//
//  См. audit_responsibility F1, audit_responsibility_hints Q13.
// =============================================================================

/// Application-owned strategy state that must be installed before Init.
///
/// The server can ask for a client strategy snapshot as part of the active-lib
/// handshake. Regular applications pass their current local strategy list here
/// so `MoonClient` can store it in the runtime-owned dispatcher before the
/// one-time Init sequence starts.
#[derive(Debug, Clone, Default)]
pub struct InitialStrategies {
    /// Delphi `cfg.ServerStratEpoch` analogue for local snapshots.
    pub epoch: u64,
    /// Full decoded local strategy list in Delphi list order.
    pub strategies: Vec<crate::commands::strategy_serializer::StrategySnapshot>,
}

impl InitialStrategies {
    /// Build an initial strategy state for [`InitConfig`].
    pub fn new(
        epoch: u64,
        strategies: Vec<crate::commands::strategy_serializer::StrategySnapshot>,
    ) -> Self {
        Self { epoch, strategies }
    }
}

/// Configuration for [`run_init_sequence`].
///
/// Delphi-critical init steps are not configurable: BaseCheck, AuthCheck,
/// GetMarketsList, GetMarketsIndexes, UpdateMarketsList, balance refresh,
/// orders, strategy snapshot sync, and settings sync are the init contract
/// itself. This config only carries optional stream subscriptions and timing.
#[derive(Debug, Clone, Default)]
pub struct InitConfig {
    /// Local strategies to install into the active library before Init starts.
    ///
    /// `None` preserves any strategy state already configured on the
    /// `EventDispatcher`, which is useful for custom low-level runtimes.
    /// `MoonClient` creates the dispatcher internally, so applications that
    /// have local strategies should pass `Some(InitialStrategies::new(...))`.
    /// An explicit empty list is valid and means "client has no local
    /// strategies".
    pub initial_strategies: Option<InitialStrategies>,
    /// Value for the post-init `TMMOrdersSubscribeCommand`.
    ///
    /// Delphi always sends this UI command after `InitDone` with
    /// `cfg.ShowHeatMap`. `None` falls back to a previously queued
    /// `ui_mm_subscribe` intent, then to `false`.
    pub mm_orders_subscribe: Option<bool>,
    /// Subscribe to all-trades with this `want_mm` value. `None` skips the
    /// all-trades subscription during init.
    pub subscribe_trades: Option<bool>,
    /// Subscribe to orderbooks by market name.
    ///
    /// The server resolves names, so callers can request these before
    /// `GetMarketsList` has populated the local market model.
    pub subscribe_orderbooks: Vec<String>,
    /// Per-step Engine API timeout. Default = `DEFAULT_PENDING_TIMEOUT_MS`
    /// (12s), matching Delphi `TMoonProtoEngine.FTimeout = 12000`.
    ///
    /// `BaseCheck`/`AuthCheck` use this timeout for each `SendAndWait`
    /// request. A pending Delphi `ServerUpdateSent` marker enables the exact
    /// Delphi BaseCheck update branch: one normal BaseCheck attempt, then up to
    /// 10 retries with 2000 ms between attempts.
    pub step_timeout: Option<Duration>,
}

/// Result of [`run_init_sequence`].
#[derive(Debug, Default)]
pub struct InitResult {
    /// `BaseCheck` succeeded and `Client::server_info()` was updated.
    pub base_check_ok: bool,
    /// `AuthCheck` succeeded.
    pub auth_check_ok: bool,
    /// Parsed per-account metadata from the successful `AuthCheck`, when its
    /// payload could be parsed. Delphi logs AuthCheck parse failures but keeps
    /// the success result; Rust mirrors that by leaving this as `None` while
    /// keeping `auth_check_ok = true`.
    pub auth_info: Option<AuthCheckResponse>,
    /// Payload size in bytes for the `GetMarketsList` response. The actual
    /// market count is parsed into `EventDispatcher::markets()`.
    pub markets_response_bytes: usize,
    /// Payload size in bytes for the `GetMarketsIndexes` response.
    pub indexes_response_bytes: usize,
    /// Payload size in bytes for the `UpdateMarketsList` response.
    pub update_markets_response_bytes: usize,
    /// Raw `TStratSchema.Data` size from the mandatory Init schema request.
    pub strategy_schema_raw_bytes: usize,
    /// Number of strategy kinds in the decoded Init schema.
    pub strategy_schema_kind_count: usize,
    /// Number of public `TStrategy` fields in the decoded Init schema.
    pub strategy_schema_field_count: usize,
    /// Whether post-init resync commands were enqueued.
    pub post_init_resync_sent: bool,
    /// Whether init requested the all-trades subscription.
    pub trades_subscribed: bool,
    /// Number of orderbook subscriptions requested during init.
    pub orderbooks_subscribed: usize,
    /// Text errors from BaseCheck retry attempts before a final successful
    /// retry, plus future non-fatal init notes. Mandatory init-step errors
    /// return [`InitError`] and leave `domain_ready` closed.
    pub errors: Vec<String>,
}

/// Errors returned by [`run_init_sequence`].
///
/// These are returned only when continuing would be meaningless. Non-fatal
/// notes are accumulated in `InitResult::errors`.
#[derive(Debug, Clone)]
pub enum InitError {
    /// The command channel is closed because the client loop is no longer alive.
    SendChannelClosed,
    /// BaseCheck or AuthCheck timed out after its configured wait.
    CriticalStepTimedOut(&'static str),
    /// A critical init step returned server-side error or malformed payload.
    CriticalStepFailed {
        /// Name of the failed init step.
        step: &'static str,
        /// Server-side error message.
        message: String,
    },
    /// The transport is not authorized yet.
    ///
    /// Run the client until `LifecycleEvent::Connected { fresh: true }` or use
    /// [`connect_and_init`] to combine connection and init.
    NotAuthenticated,
}

impl std::fmt::Display for InitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SendChannelClosed => write!(f, "client send channel closed during init"),
            Self::CriticalStepTimedOut(step) => write!(f, "critical init step '{step}' timed out"),
            Self::CriticalStepFailed { step, message } => {
                write!(f, "critical init step '{step}' failed: {message}")
            }
            Self::NotAuthenticated => write!(f, "client not authenticated (wait for authorization or use MoonClient/connect_and_init)"),
        }
    }
}

impl std::error::Error for InitError {}

/// Configuration for [`connect_and_init`].
///
/// This is the common consumer entry point when an application wants a ready
/// connection before it starts issuing one-shot requests or subscriptions.
#[derive(Debug, Clone)]
pub struct ConnectConfig {
    /// Maximum time to wait for the client to become connected.
    pub connect_timeout: Duration,
    /// Initial requests/subscriptions to run after the transport connection is ready.
    pub init: InitConfig,
}

impl Default for ConnectConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(15),
            init: InitConfig::default(),
        }
    }
}

impl ConnectConfig {
    /// Build a connect-and-init configuration from init settings and the default
    /// 15 second transport connection timeout.
    pub fn new(init: InitConfig) -> Self {
        Self {
            init,
            ..Self::default()
        }
    }

    /// Override the transport connection timeout used before init starts.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }
}

/// Errors returned by [`connect_and_init`].
#[derive(Debug, Clone)]
pub enum ConnectError {
    /// The client did not reach the connected/authenticated state before the
    /// configured timeout expired.
    ConnectTimedOut {
        /// Timeout that expired.
        timeout: Duration,
    },
    /// The transport connection succeeded, but one of the init steps failed.
    Init(InitError),
}

impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConnectTimedOut { timeout } => {
                write!(f, "connection did not become ready within {:?}", timeout)
            }
            Self::Init(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for ConnectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Init(err) => Some(err),
            Self::ConnectTimedOut { .. } => None,
        }
    }
}

impl From<InitError> for ConnectError {
    fn from(err: InitError) -> Self {
        Self::Init(err)
    }
}

/// Connect the client and run the configured init sequence.
///
/// This helper is the low-level ready-session setup used by `MoonClient` and
/// protocol tools. Regular applications should normally call
/// [`MoonClient::connect`](crate::MoonClient::connect), which owns the runtime
/// thread after this setup succeeds.
pub fn connect_and_init(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    cfg: ConnectConfig,
) -> Result<InitResult, ConnectError> {
    if let Some(initial) = cfg.init.initial_strategies.as_ref() {
        dispatcher.set_local_strategy_epoch(initial.epoch);
        dispatcher.set_local_strategies(&initial.strategies);
    }

    if !client.is_authorized() {
        client.run_with_dispatcher_worker_queued(cfg.connect_timeout, dispatcher);
    }

    if !client.is_authorized() {
        return Err(ConnectError::ConnectTimedOut {
            timeout: cfg.connect_timeout,
        });
    }

    run_init_sequence(client, dispatcher, cfg.init).map_err(ConnectError::from)
}

/// Run the full init sequence: BaseCheck → AuthCheck → GetMarketsList →
/// GetMarketsIndexes → UpdateMarketsList →
/// Delphi post-init resync → optional subscriptions.
///
/// Until this function completes successfully,
/// `EventDispatcher::dispatch_into_active` drops domain pushes
/// (`Order`/`Strat`/`Balance`/`Trades*`/`OrderBook`/`UI`), matching Delphi
/// `ClientNewData` under `not InitDone`. After a successful bootstrap, the
/// library sends `TAllStatusesReq`, `TSettingsRequest`,
/// `TStratSnapshot.CreateFromStrats(...)`, `TMMOrdersSubscribeCommand`, and
/// `TRequestBalanceRefresh`. The dispatcher also answers later server
/// `TStratSnapshotRequest` commands from the same library-owned strategy list.
///
/// The mutable `EventDispatcher` is required because the helper keeps pumping
/// the client loop while it waits. Engine API responses are also applied to
/// market state through that dispatcher (`indexes_synchronized`, market list,
/// prices); without it, TradesStream and OrderBook packets remain blocked by
/// active-library gating.
///
/// Call this after the transport has reached `Connected { fresh: true }`, or
/// use [`connect_and_init`] to perform both phases. If the client is not
/// authorized, the function returns `InitError::NotAuthenticated`.
///
/// On successful BaseCheck, the helper parses [`ServerInfo`] and stores it in
/// `client.server_info()` for multi-server identification.
///
/// Critical step timing follows the Delphi reference: `TMoonProtoEngine.FTimeout`
/// is 12000 ms for each `SendAndWait` request. Rust keeps pumping the client
/// loop while it waits for each Engine API response. If a UI command marked
/// `ServerUpdateSent`, `run_init_sequence` also mirrors Delphi `BaseCheck`:
/// wait up to 34 * 300 ms for `AuthDone`, clear the marker, send BaseCheck once,
/// and if it still fails retry it 10 times with 2000 ms pauses. All init steps
/// above are mandatory: a timeout/error means Init failed and `domain_ready`
/// stays closed.
///
/// `MoonClient::connect` is the public happy path; call this function directly
/// only when writing a custom runtime or protocol test.
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
    fn is_ok(&self) -> bool {
        matches!(self, Self::Ok)
    }

    fn final_error(&self, step: &'static str) -> Option<InitError> {
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

fn response_error_message(resp: &EngineResponse) -> String {
    format!("code={} msg={}", resp.error_code, resp.error_msg)
}

fn run_base_check_once(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    result: &mut InitResult,
    timeout: Duration,
) -> Result<CriticalInitStatus, InitError> {
    let req = crate::commands::engine_request::base_check();
    match client.request_engine_response(dispatcher, &req, timeout) {
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

fn wait_auth_done_after_server_update(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
) {
    for _ in 0..DELPHI_BASE_CHECK_UPDATE_AUTH_WAITS {
        if client.is_authorized() {
            break;
        }
        client.run_with_dispatcher_worker_queued(
            Duration::from_millis(DELPHI_BASE_CHECK_UPDATE_AUTH_WAIT_MS),
            dispatcher,
        );
    }
}

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
            client.run_with_dispatcher_worker_queued(retry_pause, dispatcher);
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

fn run_auth_check_once(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    result: &mut InitResult,
    timeout: Duration,
) -> Result<CriticalInitStatus, InitError> {
    let req = crate::commands::engine_request::auth_check();
    match client.request_engine_response(dispatcher, &req, timeout) {
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

fn run_required_engine_step(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    result: &mut InitResult,
    step: &'static str,
    req: Vec<u8>,
    timeout: Duration,
) -> Result<EngineResponse, InitError> {
    match client.request_engine_response(dispatcher, &req, timeout) {
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

fn run_required_strategy_schema_step(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    result: &mut InitResult,
    timeout: Duration,
) -> Result<(), InitError> {
    const STEP: &str = "TStratSchemaRequest";
    const TICK: Duration = Duration::from_millis(50);

    let schema_revision_before = dispatcher.strats().strategy_schema_revision();
    let schema_failures_before = dispatcher.strats().strategy_schema_failures();
    let start = Instant::now();
    let mut next_request_at = start + Duration::from_millis(SETTINGS_HELPER_RETRY_PAUSE_MS);
    client.strat_schema_request();

    loop {
        if dispatcher.strats().strategy_schema_revision() != schema_revision_before {
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

        if dispatcher.strats().strategy_schema_failures() != schema_failures_before {
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

        let Some(remaining) = timeout_remaining(start, timeout) else {
            result.errors.push(format!("{STEP}: timeout"));
            return Err(InitError::CriticalStepTimedOut(STEP));
        };

        let now = Instant::now();
        if now >= next_request_at {
            client.strat_schema_request();
            next_request_at = now + Duration::from_millis(SETTINGS_HELPER_RETRY_PAUSE_MS);
        }

        client.run_with_dispatcher_worker_queued(remaining.min(TICK), dispatcher);
    }
}

/// Run the MoonBot-compatible one-time domain initialization sequence.
///
/// Call this after transport authorization, or use [`connect_and_init`] to wait
/// for authorization and init in one helper. A successful run opens the
/// dispatcher domain gate and sends the Delphi post-init refresh set:
/// strategy schema request, order snapshot, client strategy snapshot, settings
/// request, MM-orders subscription flag, balance refresh, and optional stream
/// subscriptions.
/// Incoming `TStratSnapshotRequest` is still answered from the same
/// library-owned strategy state.
///
/// Do not call this again after a reconnect in the same [`Client`] session.
/// Reconnect restore is owned by the library once init has succeeded.
pub fn run_init_sequence(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    cfg: InitConfig,
) -> Result<InitResult, InitError> {
    let waiting_update = client.take_server_update_sent();
    if waiting_update {
        wait_auth_done_after_server_update(client, dispatcher);
    }

    if !client.is_authorized() {
        return Err(InitError::NotAuthenticated);
    }

    let timeout = cfg.step_timeout.unwrap_or(Duration::from_millis(
        crate::api_pending::DEFAULT_PENDING_TIMEOUT_MS as u64,
    ));
    let mut result = InitResult::default();

    // === 1. BaseCheck/AuthCheck === Delphi InitInt first auth block.
    // При успехе — парсим server identity и сохраняем в Client.server_info
    // (multi-server support: приложение различает серверы через `client.server_info().bot_id`).
    let auth_block_errors_before = result.errors.len();
    let mut base_status = run_base_check_delphi(
        client,
        dispatcher,
        &mut result,
        timeout,
        waiting_update,
        Duration::from_millis(DELPHI_BASE_CHECK_UPDATE_RETRY_PAUSE_MS),
    )?;

    // === 2. AuthCheck ===
    let mut auth_status = if base_status.is_ok() {
        run_auth_check_once(client, dispatcher, &mut result, timeout)?
    } else {
        CriticalInitStatus::Skipped
    };

    // Delphi `TCryptoPumpTool.InitInt`: if either BaseCheck or AuthCheck failed,
    // sleep 200ms, call BaseCheck once more, then assign Result from AuthCheck.
    // The second BaseCheck still refreshes local ServerInfo if it succeeds, but
    // the retry branch's final gate is the second AuthCheck.
    let used_init_auth_retry = !base_status.is_ok() || !auth_status.is_ok();
    if used_init_auth_retry {
        client.run_with_dispatcher_worker_queued(
            Duration::from_millis(DELPHI_INIT_AUTH_RETRY_PAUSE_MS),
            dispatcher,
        );
        base_status = run_base_check_once(client, dispatcher, &mut result, timeout)?;
        auth_status = run_auth_check_once(client, dispatcher, &mut result, timeout)?;
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

    // === 3. GetMarketsList === критический Delphi init step.
    // Markets state в dispatcher обновляется автоматически через
    // `EventDispatcher::dispatch_into` ветка Command::API → GetMarketsList.
    let resp = run_required_engine_step(
        client,
        dispatcher,
        &mut result,
        "GetMarketsList",
        crate::commands::engine_request::get_markets_list(),
        timeout,
    )?;
    if crate::commands::market::parse_markets_list_response(&resp.data, 2).is_none() {
        return Err(malformed_required_engine_step(
            &mut result,
            "GetMarketsList",
            resp.data.len(),
        ));
    }
    result.markets_response_bytes = resp.data.len();

    // === 4. GetMarketsIndexes === критический: indexed streams stay gated
    // until this map is current for the active PeerAppToken.
    let resp = run_required_engine_step(
        client,
        dispatcher,
        &mut result,
        "GetMarketsIndexes",
        crate::commands::engine_request::get_markets_indexes(),
        timeout,
    )?;
    if crate::commands::market::parse_markets_indexes_response(&resp.data).is_none() {
        return Err(malformed_required_engine_step(
            &mut result,
            "GetMarketsIndexes",
            resp.data.len(),
        ));
    }
    result.indexes_response_bytes = resp.data.len();

    // === 5. UpdateMarketsList === критический: Delphi InitInt does
    // `GetMarketsList and UpdateMarketsList`, and UpdateMarketsList also owns the
    // PeerAppToken/index synchronization path in TMoonProtoEngine.
    let resp = run_required_engine_step(
        client,
        dispatcher,
        &mut result,
        "UpdateMarketsList",
        crate::commands::engine_request::update_markets_list(),
        timeout,
    )?;
    if crate::commands::market::parse_markets_prices_response(&resp.data).is_none() {
        return Err(malformed_required_engine_step(
            &mut result,
            "UpdateMarketsList",
            resp.data.len(),
        ));
    }
    result.update_markets_response_bytes = resp.data.len();

    client.domain_restore = DomainRestoreIntent {
        fetch_indexes: true,
    };
    client.set_domain_ready(true);

    // Agreed active-library behavior: unlike the Delphi UI client, the Rust
    // library asks the server for the live strategy schema during Init so API
    // consumers get field types, picklists, visibility and chapters without
    // hardcoded Rust copies of TStrategy metadata.
    if let Err(err) = run_required_strategy_schema_step(client, dispatcher, &mut result, timeout) {
        client.set_domain_ready(false);
        return Err(err);
    }

    send_post_init_resync(client, dispatcher, &cfg, &mut result);
    client.send_registry_subscriptions_after_init();

    // === 6. SubscribeAllTrades === optional; registry update + direct wire enqueue.
    if let Some(want_mm) = cfg.subscribe_trades {
        client.subscribe_all_trades(want_mm);
        result.trades_subscribed = true;
    }

    // === 7. Subscribe orderbooks === optional; fire-and-forget через registry
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
        client.run_with_dispatcher_worker_queued(Duration::from_millis(100), dispatcher);
    }

    Ok(result)
}

pub(crate) fn send_post_init_resync(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    cfg: &InitConfig,
    result: &mut InitResult,
) {
    client.request_all_statuses(rand::random());
    if let Some(snapshot) = dispatcher.local_strategy_snapshot_reply() {
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
    let registry_mm_orders = client.subscription_registry.lock().unwrap().mm_orders_sub;
    let mm_orders = cfg
        .mm_orders_subscribe
        .or(registry_mm_orders)
        .unwrap_or(false);
    client.apply_mm_orders_subscribe_intent(mm_orders);
    client.send_mm_orders_subscribe_cmd(mm_orders);
    client.balance_request_refresh();
    result.post_init_resync_sent = true;
}
