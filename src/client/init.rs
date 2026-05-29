//! MoonBot-compatible domain init sequence owned by `MoonClient`.

use super::active_runtime::TradesStreamMode;
use super::*;

// =============================================================================
// Internal one-time init spine:
//
// `BaseCheck -> AuthCheck -> GetMarketsList -> UpdateMarketsList
//  -> Delphi post-init resync -> optional subscriptions`.
//
// This mirrors Delphi `TCryptoPumpTool.InitInt`, but it is not a public
// application runtime model. `MoonClient::connect` owns this sequence inside its
// runtime thread and reports completion through lifecycle events.
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

/// Configuration for the one-time Init sequence run by [`MoonClient`].
///
/// Delphi-critical init steps are not configurable: BaseCheck, AuthCheck,
/// GetMarketsList, UpdateMarketsList, balance refresh,
/// orders, strategy snapshot sync, and settings sync are the init contract
/// itself. This config only carries optional stream subscriptions and timing.
#[derive(Debug, Clone, Default)]
pub struct InitConfig {
    /// Local strategies to install into the active library before Init starts.
    ///
    /// `None` preserves any strategy state already configured on the internal
    /// dispatcher. Applications that have local strategies should pass
    /// `Some(InitialStrategies::new(...))`.
    /// An explicit empty list is valid and means "client has no local
    /// strategies".
    pub initial_strategies: Option<InitialStrategies>,
    /// Value for the post-init `TMMOrdersSubscribeCommand`.
    ///
    /// Delphi always sends this UI command after `InitDone` with
    /// `cfg.ShowHeatMap`. `None` falls back to a previously queued
    /// `ui_mm_subscribe` intent, then to `false`.
    pub mm_orders_subscribe: Option<bool>,
    /// Subscribe to all-trades during init. `None` skips the all-trades
    /// subscription during init.
    pub subscribe_trades: Option<TradesStreamMode>,
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

/// Result of the internal one-time Init sequence.
#[derive(Debug, Default)]
pub(crate) struct InitResult {
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

/// Errors reported by the one-time Init sequence.
///
/// These are returned only when continuing would be meaningless. Non-fatal
/// notes are accumulated internally while Init runs.
#[derive(Debug, Clone, PartialEq)]
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
    /// [`MoonClient::connect`](crate::MoonClient::connect) to combine connection and init.
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
            Self::NotAuthenticated => write!(
                f,
                "client not authenticated (wait for authorization or use MoonClient::connect)"
            ),
        }
    }
}

impl std::error::Error for InitError {}

/// Configuration for [`MoonClient::connect`](crate::MoonClient::connect).
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

/// Errors reported by the connection/init startup sequence.
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectError {
    /// Non-blocking runtime startup was canceled by `MoonClient::disconnect`.
    Canceled,
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
            Self::Canceled => write!(f, "connection was canceled"),
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
            Self::Canceled => None,
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

pub(crate) enum RuntimeInitPoll {
    Pending { changed: bool },
    Ready(InitResult),
    Failed(ConnectError),
}

pub(crate) struct RuntimeInitMachine {
    cfg: InitConfig,
    started: Instant,
    init_started_at: Instant,
    connect_timeout: Duration,
    step_timeout: Duration,
    phase: RuntimeInitPhase,
    result: InitResult,
    waiting_update: bool,
    base_errors_before: usize,
    auth_block_errors_before: usize,
    base_status: CriticalInitStatus,
    auth_status: CriticalInitStatus,
    strategy_schema: Option<PendingStrategySchemaStep>,
}

enum RuntimeInitPhase {
    WaitAuthorized,
    ServerUpdateAuthWait {
        waits_done: u8,
        next_at: Instant,
    },
    SendBaseCheck {
        attempt: BaseAttempt,
    },
    WaitBaseCheck {
        attempt: BaseAttempt,
        pending: PendingEngineInit,
    },
    BaseUpdateRetryPause {
        next_retry: u8,
        next_at: Instant,
    },
    SendAuthCheck {
        attempt: AuthAttempt,
    },
    WaitAuthCheck {
        attempt: AuthAttempt,
        pending: PendingEngineInit,
    },
    InitAuthRetryPause {
        next_at: Instant,
    },
    SendGetMarketsList,
    WaitGetMarketsList {
        pending: PendingEngineInit,
    },
    SendUpdateMarketsList,
    WaitUpdateMarketsList {
        pending: PendingEngineInit,
    },
    WaitStrategySchema,
    PostInit,
    PostInitFlush {
        until: Instant,
    },
    Done,
}

#[derive(Clone, Copy)]
enum BaseAttempt {
    First,
    UpdateRetry { retry_no: u8 },
    InitRetry,
}

#[derive(Clone, Copy)]
enum AuthAttempt {
    First,
    InitRetry,
}

struct PendingEngineInit {
    request_uid: Option<u64>,
    rx: mpsc::Receiver<EngineResponse>,
    deadline: Instant,
}

enum PendingEnginePoll {
    Pending,
    Response(EngineResponse),
    Timeout,
    Disconnected,
}

enum StrategySchemaPoll {
    Pending,
    Ready,
    Failed(InitError),
}

impl RuntimeInitMachine {
    pub(crate) fn new(cfg: ConnectConfig, dispatcher: &mut crate::events::EventDispatcher) -> Self {
        if let Some(initial) = cfg.init.initial_strategies.as_ref() {
            dispatcher.set_local_strategy_epoch(initial.epoch);
            dispatcher.set_local_strategies(&initial.strategies);
        }
        let now = Instant::now();
        let step_timeout = cfg.init.step_timeout.unwrap_or(Duration::from_millis(
            crate::api_pending::DEFAULT_PENDING_TIMEOUT_MS as u64,
        ));
        Self {
            connect_timeout: cfg.connect_timeout,
            step_timeout,
            cfg: cfg.init,
            started: now,
            init_started_at: now,
            phase: RuntimeInitPhase::WaitAuthorized,
            result: InitResult::default(),
            waiting_update: false,
            base_errors_before: 0,
            auth_block_errors_before: 0,
            base_status: CriticalInitStatus::Skipped,
            auth_status: CriticalInitStatus::Skipped,
            strategy_schema: None,
        }
    }

    pub(crate) fn poll(
        &mut self,
        client: &mut Client,
        dispatcher: &mut crate::events::EventDispatcher,
    ) -> RuntimeInitPoll {
        if client.shutdown_requested() {
            client.disconnect();
            return RuntimeInitPoll::Failed(ConnectError::Canceled);
        }

        let mut changed = false;
        loop {
            let phase = std::mem::replace(&mut self.phase, RuntimeInitPhase::Done);
            match phase {
                RuntimeInitPhase::WaitAuthorized => {
                    if client.is_authorized() {
                        self.init_started_at = Instant::now();
                        self.waiting_update = client.take_server_update_sent();
                        self.auth_block_errors_before = self.result.errors.len();
                        self.base_errors_before = self.result.errors.len();
                        self.phase = if self.waiting_update {
                            RuntimeInitPhase::ServerUpdateAuthWait {
                                waits_done: 0,
                                next_at: Instant::now(),
                            }
                        } else {
                            RuntimeInitPhase::SendBaseCheck {
                                attempt: BaseAttempt::First,
                            }
                        };
                        continue;
                    }
                    if timeout_remaining(self.started, self.connect_timeout).is_none() {
                        self.phase = RuntimeInitPhase::Done;
                        return RuntimeInitPoll::Failed(ConnectError::ConnectTimedOut {
                            timeout: self.connect_timeout,
                        });
                    }
                    self.phase = RuntimeInitPhase::WaitAuthorized;
                    return RuntimeInitPoll::Pending { changed };
                }
                RuntimeInitPhase::ServerUpdateAuthWait {
                    mut waits_done,
                    mut next_at,
                } => {
                    if client.is_authorized()
                        || waits_done >= DELPHI_BASE_CHECK_UPDATE_AUTH_WAITS as u8
                    {
                        self.phase = RuntimeInitPhase::SendBaseCheck {
                            attempt: BaseAttempt::First,
                        };
                        continue;
                    }
                    let now = Instant::now();
                    if now >= next_at {
                        waits_done = waits_done.saturating_add(1);
                        next_at =
                            now + Duration::from_millis(DELPHI_BASE_CHECK_UPDATE_AUTH_WAIT_MS);
                    }
                    self.phase = RuntimeInitPhase::ServerUpdateAuthWait {
                        waits_done,
                        next_at,
                    };
                    return RuntimeInitPoll::Pending { changed };
                }
                RuntimeInitPhase::SendBaseCheck { attempt } => {
                    let pending = begin_engine_init_step(
                        client,
                        crate::commands::engine_request::base_check(),
                        self.step_timeout,
                    );
                    self.phase = RuntimeInitPhase::WaitBaseCheck { attempt, pending };
                    continue;
                }
                RuntimeInitPhase::WaitBaseCheck {
                    attempt,
                    mut pending,
                } => match poll_engine_init_step(client, &mut pending) {
                    PendingEnginePoll::Pending => {
                        self.phase = RuntimeInitPhase::WaitBaseCheck { attempt, pending };
                        return RuntimeInitPoll::Pending { changed };
                    }
                    PendingEnginePoll::Response(resp) => {
                        let status = self.apply_base_check_response(client, resp);
                        if status.is_ok() {
                            fire_init_step(client, "BaseCheck", self.init_started_at);
                            self.ensure_strategy_schema_started(client, dispatcher);
                        }
                        match attempt {
                            BaseAttempt::First => {
                                self.base_status = status;
                                if self.waiting_update && !self.base_status.is_ok() {
                                    self.phase = RuntimeInitPhase::BaseUpdateRetryPause {
                                        next_retry: 1,
                                        next_at: Instant::now()
                                            + Duration::from_millis(
                                                DELPHI_BASE_CHECK_UPDATE_RETRY_PAUSE_MS,
                                            ),
                                    };
                                } else if self.base_status.is_ok() {
                                    self.phase = RuntimeInitPhase::SendAuthCheck {
                                        attempt: AuthAttempt::First,
                                    };
                                } else {
                                    self.auth_status = CriticalInitStatus::Skipped;
                                    self.phase = RuntimeInitPhase::InitAuthRetryPause {
                                        next_at: Instant::now()
                                            + Duration::from_millis(
                                                DELPHI_INIT_AUTH_RETRY_PAUSE_MS,
                                            ),
                                    };
                                }
                                continue;
                            }
                            BaseAttempt::UpdateRetry { retry_no } => {
                                self.base_status = status;
                                if self.base_status.is_ok() {
                                    self.result.errors.truncate(self.base_errors_before);
                                    self.phase = RuntimeInitPhase::SendAuthCheck {
                                        attempt: AuthAttempt::First,
                                    };
                                } else if retry_no < DELPHI_BASE_CHECK_UPDATE_RETRIES as u8 {
                                    self.phase = RuntimeInitPhase::BaseUpdateRetryPause {
                                        next_retry: retry_no + 1,
                                        next_at: Instant::now()
                                            + Duration::from_millis(
                                                DELPHI_BASE_CHECK_UPDATE_RETRY_PAUSE_MS,
                                            ),
                                    };
                                } else {
                                    self.auth_status = CriticalInitStatus::Skipped;
                                    self.phase = RuntimeInitPhase::InitAuthRetryPause {
                                        next_at: Instant::now()
                                            + Duration::from_millis(
                                                DELPHI_INIT_AUTH_RETRY_PAUSE_MS,
                                            ),
                                    };
                                }
                                continue;
                            }
                            BaseAttempt::InitRetry => {
                                self.base_status = status;
                                self.phase = RuntimeInitPhase::SendAuthCheck {
                                    attempt: AuthAttempt::InitRetry,
                                };
                                continue;
                            }
                        }
                    }
                    PendingEnginePoll::Timeout => {
                        self.result.errors.push("BaseCheck timeout".to_string());
                        let status = CriticalInitStatus::TimedOut;
                        match attempt {
                            BaseAttempt::First => {
                                self.base_status = status;
                                if self.waiting_update {
                                    self.phase = RuntimeInitPhase::BaseUpdateRetryPause {
                                        next_retry: 1,
                                        next_at: Instant::now()
                                            + Duration::from_millis(
                                                DELPHI_BASE_CHECK_UPDATE_RETRY_PAUSE_MS,
                                            ),
                                    };
                                } else {
                                    self.auth_status = CriticalInitStatus::Skipped;
                                    self.phase = RuntimeInitPhase::InitAuthRetryPause {
                                        next_at: Instant::now()
                                            + Duration::from_millis(
                                                DELPHI_INIT_AUTH_RETRY_PAUSE_MS,
                                            ),
                                    };
                                }
                                continue;
                            }
                            BaseAttempt::UpdateRetry { retry_no } => {
                                self.base_status = status;
                                if retry_no < DELPHI_BASE_CHECK_UPDATE_RETRIES as u8 {
                                    self.phase = RuntimeInitPhase::BaseUpdateRetryPause {
                                        next_retry: retry_no + 1,
                                        next_at: Instant::now()
                                            + Duration::from_millis(
                                                DELPHI_BASE_CHECK_UPDATE_RETRY_PAUSE_MS,
                                            ),
                                    };
                                } else {
                                    self.auth_status = CriticalInitStatus::Skipped;
                                    self.phase = RuntimeInitPhase::InitAuthRetryPause {
                                        next_at: Instant::now()
                                            + Duration::from_millis(
                                                DELPHI_INIT_AUTH_RETRY_PAUSE_MS,
                                            ),
                                    };
                                }
                                continue;
                            }
                            BaseAttempt::InitRetry => {
                                self.base_status = status;
                                self.phase = RuntimeInitPhase::SendAuthCheck {
                                    attempt: AuthAttempt::InitRetry,
                                };
                                continue;
                            }
                        }
                    }
                    PendingEnginePoll::Disconnected => {
                        self.phase = RuntimeInitPhase::Done;
                        return RuntimeInitPoll::Failed(ConnectError::from(
                            InitError::SendChannelClosed,
                        ));
                    }
                },
                RuntimeInitPhase::BaseUpdateRetryPause {
                    next_retry,
                    next_at,
                } => {
                    if Instant::now() >= next_at {
                        self.phase = RuntimeInitPhase::SendBaseCheck {
                            attempt: BaseAttempt::UpdateRetry {
                                retry_no: next_retry,
                            },
                        };
                        continue;
                    }
                    self.phase = RuntimeInitPhase::BaseUpdateRetryPause {
                        next_retry,
                        next_at,
                    };
                    return RuntimeInitPoll::Pending { changed };
                }
                RuntimeInitPhase::SendAuthCheck { attempt } => {
                    let pending = begin_engine_init_step(
                        client,
                        crate::commands::engine_request::auth_check(),
                        self.step_timeout,
                    );
                    self.phase = RuntimeInitPhase::WaitAuthCheck { attempt, pending };
                    continue;
                }
                RuntimeInitPhase::WaitAuthCheck {
                    attempt,
                    mut pending,
                } => {
                    let status = match poll_engine_init_step(client, &mut pending) {
                        PendingEnginePoll::Pending => {
                            self.phase = RuntimeInitPhase::WaitAuthCheck { attempt, pending };
                            return RuntimeInitPoll::Pending { changed };
                        }
                        PendingEnginePoll::Response(resp) => {
                            self.apply_auth_check_response(client, resp)
                        }
                        PendingEnginePoll::Timeout => {
                            self.result.errors.push("AuthCheck timeout".to_string());
                            CriticalInitStatus::TimedOut
                        }
                        PendingEnginePoll::Disconnected => {
                            self.phase = RuntimeInitPhase::Done;
                            return RuntimeInitPoll::Failed(ConnectError::from(
                                InitError::SendChannelClosed,
                            ));
                        }
                    };
                    if status.is_ok() {
                        fire_init_step(client, "AuthCheck", self.init_started_at);
                    }
                    match attempt {
                        AuthAttempt::First => {
                            self.auth_status = status;
                            if !self.base_status.is_ok() || !self.auth_status.is_ok() {
                                self.phase = RuntimeInitPhase::InitAuthRetryPause {
                                    next_at: Instant::now()
                                        + Duration::from_millis(DELPHI_INIT_AUTH_RETRY_PAUSE_MS),
                                };
                            } else {
                                self.phase = RuntimeInitPhase::SendGetMarketsList;
                            }
                            continue;
                        }
                        AuthAttempt::InitRetry => {
                            self.auth_status = status;
                            if self.auth_status.is_ok() {
                                self.result.errors.truncate(self.auth_block_errors_before);
                                if self.strategy_schema.is_none() {
                                    self.ensure_strategy_schema_started(client, dispatcher);
                                }
                                self.phase = RuntimeInitPhase::SendGetMarketsList;
                                continue;
                            }
                            self.phase = RuntimeInitPhase::Done;
                            return RuntimeInitPoll::Failed(ConnectError::from(
                                self.auth_status
                                    .final_error("AuthCheck")
                                    .unwrap_or(InitError::CriticalStepTimedOut("AuthCheck")),
                            ));
                        }
                    }
                }
                RuntimeInitPhase::InitAuthRetryPause { next_at } => {
                    if Instant::now() >= next_at {
                        self.phase = RuntimeInitPhase::SendBaseCheck {
                            attempt: BaseAttempt::InitRetry,
                        };
                        continue;
                    }
                    self.phase = RuntimeInitPhase::InitAuthRetryPause { next_at };
                    return RuntimeInitPoll::Pending { changed };
                }
                RuntimeInitPhase::SendGetMarketsList => {
                    if self.strategy_schema.is_none() {
                        self.ensure_strategy_schema_started(client, dispatcher);
                    }
                    let pending = begin_engine_init_step(
                        client,
                        crate::commands::engine_request::get_markets_list(),
                        self.step_timeout,
                    );
                    self.phase = RuntimeInitPhase::WaitGetMarketsList { pending };
                    continue;
                }
                RuntimeInitPhase::WaitGetMarketsList { mut pending } => {
                    let resp = match self.poll_required_engine_response(
                        client,
                        &mut pending,
                        "GetMarketsList",
                    ) {
                        Ok(Some(resp)) => resp,
                        Ok(None) => {
                            self.phase = RuntimeInitPhase::WaitGetMarketsList { pending };
                            return RuntimeInitPoll::Pending { changed };
                        }
                        Err(err) => {
                            self.phase = RuntimeInitPhase::Done;
                            return RuntimeInitPoll::Failed(ConnectError::from(err));
                        }
                    };
                    if let Err(err) = apply_required_get_markets_list_response(
                        dispatcher,
                        &resp,
                        &mut self.result,
                    ) {
                        self.phase = RuntimeInitPhase::Done;
                        return RuntimeInitPoll::Failed(ConnectError::from(err));
                    }
                    self.result.markets_response_bytes = resp.data.len();
                    fire_init_step(client, "GetMarketsList", self.init_started_at);
                    client.tracked_indexes_peer_app_token = client.peer_app_token;
                    self.phase = RuntimeInitPhase::SendUpdateMarketsList;
                    changed = true;
                    continue;
                }
                RuntimeInitPhase::SendUpdateMarketsList => {
                    let pending = begin_engine_init_step(
                        client,
                        crate::commands::engine_request::update_markets_list(),
                        self.step_timeout,
                    );
                    self.phase = RuntimeInitPhase::WaitUpdateMarketsList { pending };
                    continue;
                }
                RuntimeInitPhase::WaitUpdateMarketsList { mut pending } => {
                    let resp = match self.poll_required_engine_response(
                        client,
                        &mut pending,
                        "UpdateMarketsList",
                    ) {
                        Ok(Some(resp)) => resp,
                        Ok(None) => {
                            self.phase = RuntimeInitPhase::WaitUpdateMarketsList { pending };
                            return RuntimeInitPoll::Pending { changed };
                        }
                        Err(err) => {
                            self.phase = RuntimeInitPhase::Done;
                            return RuntimeInitPoll::Failed(ConnectError::from(err));
                        }
                    };
                    if let Err(err) = apply_required_update_markets_list_response(
                        dispatcher,
                        &resp,
                        &mut self.result,
                    ) {
                        self.phase = RuntimeInitPhase::Done;
                        return RuntimeInitPoll::Failed(ConnectError::from(err));
                    }
                    self.result.update_markets_response_bytes = resp.data.len();
                    fire_init_step(client, "UpdateMarketsList", self.init_started_at);
                    client.domain_restore = DomainRestoreIntent {
                        fetch_indexes: true,
                    };
                    client.set_domain_ready(true);
                    self.phase = RuntimeInitPhase::WaitStrategySchema;
                    changed = true;
                    continue;
                }
                RuntimeInitPhase::WaitStrategySchema => {
                    let timeout = self.step_timeout;
                    let Some(pending) = self.strategy_schema.as_mut() else {
                        self.ensure_strategy_schema_started(client, dispatcher);
                        self.phase = RuntimeInitPhase::WaitStrategySchema;
                        continue;
                    };
                    match poll_required_strategy_schema_step(
                        client,
                        dispatcher,
                        &mut self.result,
                        pending,
                        timeout,
                    ) {
                        StrategySchemaPoll::Ready => {
                            fire_init_step(client, "StrategySchema", self.init_started_at);
                            self.phase = RuntimeInitPhase::PostInit;
                            changed = true;
                            continue;
                        }
                        StrategySchemaPoll::Pending => {
                            self.phase = RuntimeInitPhase::WaitStrategySchema;
                            return RuntimeInitPoll::Pending { changed };
                        }
                        StrategySchemaPoll::Failed(err) => {
                            client.set_domain_ready(false);
                            self.phase = RuntimeInitPhase::Done;
                            return RuntimeInitPoll::Failed(ConnectError::from(err));
                        }
                    }
                }
                RuntimeInitPhase::PostInit => {
                    send_post_init_resync(client, dispatcher, &self.cfg, &mut self.result);
                    client.send_registry_subscriptions_after_init();
                    if let Some(mode) = self.cfg.subscribe_trades {
                        client.subscribe_all_trades(mode.want_market_makers());
                        self.result.trades_subscribed = true;
                    }
                    for name in &self.cfg.subscribe_orderbooks {
                        client.subscribe_orderbook(name);
                        self.result.orderbooks_subscribed += 1;
                    }
                    self.phase = RuntimeInitPhase::PostInitFlush {
                        until: Instant::now() + Duration::from_millis(100),
                    };
                    changed = true;
                    continue;
                }
                RuntimeInitPhase::PostInitFlush { until } => {
                    if Instant::now() >= until {
                        fire_init_step(client, "PostInitFlush", self.init_started_at);
                        self.phase = RuntimeInitPhase::Done;
                        return RuntimeInitPoll::Ready(std::mem::take(&mut self.result));
                    }
                    self.phase = RuntimeInitPhase::PostInitFlush { until };
                    return RuntimeInitPoll::Pending { changed };
                }
                RuntimeInitPhase::Done => {
                    self.phase = RuntimeInitPhase::Done;
                    return RuntimeInitPoll::Pending { changed };
                }
            }
        }
    }

    fn ensure_strategy_schema_started(
        &mut self,
        client: &mut Client,
        dispatcher: &crate::events::EventDispatcher,
    ) {
        if self.strategy_schema.is_none() {
            self.strategy_schema = Some(begin_required_strategy_schema_step(client, dispatcher));
        }
    }

    fn apply_base_check_response(
        &mut self,
        client: &mut Client,
        resp: EngineResponse,
    ) -> CriticalInitStatus {
        if resp.success {
            self.result.base_check_ok = true;
            let info = parse_base_check_response(&resp.data);
            client.set_server_info(info);
            CriticalInitStatus::Ok
        } else {
            let message = response_error_message(&resp);
            self.result
                .errors
                .push(format!("BaseCheck error: {message}"));
            CriticalInitStatus::Failed(message)
        }
    }

    fn apply_auth_check_response(
        &mut self,
        client: &mut Client,
        resp: EngineResponse,
    ) -> CriticalInitStatus {
        if resp.success {
            let len = resp.data.len();
            match parse_auth_check_response(&resp.data) {
                Some(auth) => {
                    client.set_auth_info(auth.clone());
                    self.result.auth_info = Some(auth);
                }
                None => {
                    self.result
                        .errors
                        .push(format!("AuthCheck parse: malformed payload ({len} bytes)"));
                }
            }
            self.result.auth_check_ok = true;
            CriticalInitStatus::Ok
        } else {
            let message = response_error_message(&resp);
            self.result
                .errors
                .push(format!("AuthCheck error: {message}"));
            CriticalInitStatus::Failed(message)
        }
    }

    fn poll_required_engine_response(
        &mut self,
        client: &mut Client,
        pending: &mut PendingEngineInit,
        step: &'static str,
    ) -> Result<Option<EngineResponse>, InitError> {
        match poll_engine_init_step(client, pending) {
            PendingEnginePoll::Pending => Ok(None),
            PendingEnginePoll::Response(resp) if resp.success => Ok(Some(resp)),
            PendingEnginePoll::Response(resp) => {
                let message = response_error_message(&resp);
                self.result.errors.push(format!("{step} error: {message}"));
                Err(InitError::CriticalStepFailed { step, message })
            }
            PendingEnginePoll::Timeout => {
                self.result.errors.push(format!("{step}: timeout"));
                Err(InitError::CriticalStepTimedOut(step))
            }
            PendingEnginePoll::Disconnected => Err(InitError::SendChannelClosed),
        }
    }
}

fn begin_engine_init_step(
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

fn poll_engine_init_step(
    client: &mut Client,
    pending: &mut PendingEngineInit,
) -> PendingEnginePoll {
    match pending.rx.try_recv() {
        Ok(resp) => PendingEnginePoll::Response(resp),
        Err(mpsc::TryRecvError::Disconnected) => PendingEnginePoll::Disconnected,
        Err(mpsc::TryRecvError::Empty) => {
            if Instant::now() >= pending.deadline {
                if let Some(uid) = pending.request_uid {
                    client.api_pending.remove(uid);
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
/// library sends `TAllStatusesReq`, `TSettingsRequest`,
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

fn check_init_shutdown(client: &Client) -> Result<(), InitError> {
    if client.shutdown_requested() {
        Err(InitError::SendChannelClosed)
    } else {
        Ok(())
    }
}

#[cfg(test)]
fn pump_client_for(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    duration: Duration,
) {
    client.with_owned_runtime_stepper(dispatcher, |client, stepper, _dispatcher| {
        stepper.step_for(client, _dispatcher, duration);
    });
}

fn fire_init_step(client: &mut Client, step: &'static str, start: Instant) {
    client.fire_lifecycle(LifecycleEvent::InitStepCompleted {
        step,
        elapsed_ms: start.elapsed().as_millis() as u64,
    });
}

#[cfg(test)]
fn run_base_check_once(
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
fn wait_auth_done_after_server_update(
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
fn run_auth_check_once(
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
fn run_required_engine_step(
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

fn apply_required_get_markets_list_response(
    dispatcher: &mut crate::events::EventDispatcher,
    resp: &EngineResponse,
    result: &mut InitResult,
) -> Result<(), InitError> {
    let mut events = Vec::new();
    if !dispatcher.apply_get_markets_list_response_like_delphi(resp, &mut events) {
        return Err(malformed_required_engine_step(
            result,
            "GetMarketsList",
            resp.data.len(),
        ));
    }
    dispatcher.queue_events(events);
    Ok(())
}

fn apply_required_update_markets_list_response(
    dispatcher: &mut crate::events::EventDispatcher,
    resp: &EngineResponse,
    result: &mut InitResult,
) -> Result<(), InitError> {
    let mut events = Vec::new();
    if !dispatcher.apply_update_markets_list_response_like_delphi(resp, None, &mut events) {
        return Err(malformed_required_engine_step(
            result,
            "UpdateMarketsList",
            resp.data.len(),
        ));
    }
    dispatcher.queue_events(events);
    Ok(())
}

struct PendingStrategySchemaStep {
    schema_revision_before: u64,
    schema_failures_before: u64,
    start: Instant,
    next_request_at: Instant,
}

fn begin_required_strategy_schema_step(
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

fn poll_required_strategy_schema_step(
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
fn finish_required_strategy_schema_step(
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

/// Run the MoonBot-compatible one-time domain initialization sequence.
///
/// Internal one-time domain initialization sequence after transport
/// authorization. A successful run opens the
/// dispatcher domain gate and sends the Delphi post-init refresh set:
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
    // market init steps. Only TStratSchemaRequest/TStratSchema are allowed
    // through the pre-domain gate.
    // A pre-init TStratSnapshotRequest is only latched by EventDispatcher; the
    // actual TStratSnapshot reply is sent by post-init resync after schema/state
    // are ready. The rest of MPC_Strat remains gated until domain_ready.
    if strategy_schema.is_none() {
        strategy_schema = Some(begin_required_strategy_schema_step(client, dispatcher));
    }

    // === 3. GetMarketsList === критический Delphi init step.
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
    client.tracked_indexes_peer_app_token = client.peer_app_token;

    // === 4. UpdateMarketsList === критический: Delphi InitInt does exactly
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

    client.domain_restore = DomainRestoreIntent {
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
        pump_client_for(client, dispatcher, Duration::from_millis(100));
        check_init_shutdown(client)?;
        fire_init_step(client, "PostInitFlush", init_started_at);
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
