//! Init/connect configuration, result, and error types.

use super::*;

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
