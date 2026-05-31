use super::ConnectError;
use crate::commands::engine_api::ServerInfo;
use crate::state::MarketHistorySizing;
use crate::MoonKey;
use std::time::Duration;
/// Transport authorization state for one [`crate::client::Client`].
///
/// This is a low-level diagnostic value. Most applications should watch
/// [`LifecycleEvent`] and use [`crate::client::Client::is_authorized`] /
/// [`crate::client::Client::is_domain_ready`] for coarse readiness.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AuthStatus {
    /// Initial state before any successful transport exchange.
    Base,
    /// Transport connection is established, but domain auth is not complete yet.
    Connected,
    /// Transport and auth handshake are complete.
    AuthDone,
    /// Client is offline and reconnect logic is active or pending.
    Offline,
}

/// Error returned when a session-derived [`crate::commands::trade::TradeCtx`]
/// cannot be built yet.
///
/// Trade command wire headers carry two Delphi enum ordinals from the active
/// server session: `cfg.BaseCurrency` and `cfg.Header.Current`. They are learned
/// from `emk_BaseCheck`, so applications that skipped BaseCheck must run it
/// before sending market-level trade commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeContextError {
    /// `ServerInfo::exchange_code` is missing.
    pub missing_exchange_code: bool,
    /// `ServerInfo::base_currency_code` is missing.
    pub missing_base_currency_code: bool,
}

impl TradeContextError {
    pub(crate) fn from_server_info(info: &ServerInfo) -> Option<Self> {
        let err = Self {
            missing_exchange_code: info.exchange_code.is_none(),
            missing_base_currency_code: info.base_currency_code.is_none(),
        };
        if err.missing_exchange_code || err.missing_base_currency_code {
            Some(err)
        } else {
            None
        }
    }
}

impl std::fmt::Display for TradeContextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.missing_exchange_code, self.missing_base_currency_code) {
            (true, true) => write!(
                f,
                "trade route is unavailable: run BaseCheck first (missing exchange_code and base_currency_code)"
            ),
            (true, false) => write!(
                f,
                "trade route is unavailable: run BaseCheck first (missing exchange_code)"
            ),
            (false, true) => write!(
                f,
                "trade route is unavailable: run BaseCheck first (missing base_currency_code)"
            ),
            (false, false) => write!(f, "trade route is available"),
        }
    }
}

impl std::error::Error for TradeContextError {}

/// Lifecycle event for the connection to the MoonProto server.
///
/// Register a callback with [`crate::client::Client::on_lifecycle`]. During client run calls,
/// the callback is delivered through the application callback queue, not inside
/// the protocol writer tick.
///
/// Typical sequence:
/// ```text
///   Connecting  → Connected{fresh:true}  → [running] → Disconnected
///                       │
///                       └──[link loss]──► Reconnecting → Connected{fresh:false} → ...
///                                                  │
///                                                  └──[detected restart]──► ServerRestart
/// ```
///
/// `Connected` can be emitted several times during one `Client` lifetime after
/// successful re-handshakes. `fresh = true` is emitted only for the first
/// connection after `Client::new`; reconnects use `fresh = false`.
///
/// Session invariant: init is a one-time operation for a `Client` session.
/// Before init, transport `Fine` does not start Engine API traffic. After init,
/// reconnect in the same session restores fresh indexes only after a changed
/// `PeerAppToken`, refreshes `UpdateMarketsList`, and restores registry
/// subscriptions automatically. The initial post-init resync
/// (orders, settings, balance, client strategy snapshot) is not repeated on
/// reconnect.
///
/// Applications should treat lifecycle events as UI/observability signals; they
/// do not need to run init again to keep requested streams alive.
#[derive(Debug, Clone, PartialEq)]
pub enum LifecycleEvent {
    /// Handshake started (`Hello` sent), but `Fine` has not arrived yet.
    ///
    /// No application recovery action is required: the client retries and
    /// rotates local UDP bind ports by itself.
    Connecting,
    /// `Fine` received: the transport channel is authorized and can send or
    /// receive commands.
    ///
    /// `fresh = true` means this is the first connection since the runtime
    /// started. `MoonClient` runs the one-time init sequence automatically.
    ///
    /// `fresh = false` is a reconnect after link loss or server restart. If init
    /// already succeeded, the library refreshes indexes only when the
    /// `PeerAppToken` changed, restores `UpdateMarketsList`, and requested
    /// subscriptions; the application does not repeat init.
    Connected {
        /// `true` only for the first successful connection after `Client::new`;
        /// reconnects in the same client session use `false`.
        fresh: bool,
    },
    /// The one-time Init sequence completed and Active Lib state is ready.
    Ready,
    /// One mandatory Init step completed inside `MoonClient`.
    ///
    /// This is progress/diagnostic information for UI status bars and FireTest
    /// timing. It is not a recovery hook and does not change the final `Ready`
    /// contract.
    InitStepCompleted {
        /// Stable step name: `BaseCheck`, `AuthCheck`, `GetMarketsList`,
        /// `UpdateMarketsList`, `StrategySchema`, `PostInitFlush`,
        /// `StartupSnapshot`, or `StartupEvents`.
        step: &'static str,
        /// Wall-clock time since the init sequence started.
        elapsed_ms: u64,
    },
    /// Initial connect/init failed in the background runtime.
    ///
    /// Carries the typed [`ConnectError`] so observers can branch on the failure
    /// kind (connect timeout vs. cancellation vs. a specific failed init step)
    /// instead of parsing a message; its `Display` still yields the same
    /// human-readable text.
    ConnectFailed {
        /// Typed startup failure for this connection attempt.
        error: ConnectError,
    },
    /// The application explicitly called `client.disconnect()`.
    ///
    /// This is a final state for the current instance; create a new `Client` to
    /// connect again.
    Disconnected,
    /// Link loss exceeded the reconnect threshold.
    ///
    /// The client tries soft reconnect (`HelloAgain`) first. If the server no
    /// longer remembers this client, the next cycle starts a fresh `Hello` and
    /// emits `Connecting`. No application recovery action is required.
    Reconnecting,
    /// Critical UDP bind status: repeated 200-port bind sweeps failed.
    ///
    /// Typical causes are mobile background networking restrictions, exhausted
    /// ephemeral ports, OS permission errors, or VPN conflicts. The library keeps
    /// retrying forever, matching Delphi, but this event lets the application
    /// show a clear network-permission or bind-failure status instead of an
    /// endless generic "connecting" indicator.
    ///
    /// `consecutive_failures` counts how many complete 200-port sweeps failed in
    /// a row. The first event is emitted after about 15 seconds of continuous
    /// failure, then at most once every 50 seconds.
    BindFailed {
        /// Number of complete 200-port bind sweeps that failed in a row.
        consecutive_failures: u32,
    },
    /// Server restart detected through a changed `PeerAppToken`.
    ///
    /// The library marks market indexes stale and blocks indexed TradesStream
    /// and OrderBook packets until it has synchronized fresh indexes. Before the
    /// first init it does not send `GetMarketsIndexes`, `UpdateMarketsList`, or
    /// subscriptions. After init, restore runs automatically on successful
    /// reconnect.
    ///
    /// The application may show a UI indicator; it does not need to repeat init
    /// to restore requested streams.
    ServerRestart,
}

/// Lifecycle callback type registered with [`crate::client::Client::on_lifecycle`].
pub type LifecycleFn = Box<dyn FnMut(LifecycleEvent) + Send>;

/// Configuration for periodic refresh requests owned by the active library.
///
/// Long-running clients need fresh market prices, funding, and token tags. The
/// Delphi bot does this from background workers, and the Rust active library
/// mirrors that cadence after domain init succeeds.
///
/// Set a field to `None` when the application intentionally owns that Engine API
/// refresh manually.
///
/// Refresh ticks start after domain init completes. This keeps fresh
/// BaseCheck/AuthCheck requests from being queued behind background
/// `UpdateMarketsList` traffic on cold connect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefreshConfig {
    /// Periodically send `emk_UpdateMarketsList` for fresh prices and funding.
    ///
    /// Default: `Some(2s)`, matching the Delphi full-proxy worker after init.
    pub update_markets_every: Option<Duration>,
    /// Periodically send `emk_CheckBinanceTags`.
    ///
    /// Default: `Some(60s)`. The hourly four-request burst with 200 ms spacing
    /// is handled automatically, matching Delphi `BHeavyApiWorker`.
    pub check_tags_every: Option<Duration>,
}

pub(crate) const CHECK_TAGS_BURST_COUNT: u8 = 4;
pub(crate) const CHECK_TAGS_BURST_SPACING_MS: i64 = 200;

impl Default for RefreshConfig {
    fn default() -> Self {
        Self {
            update_markets_every: Some(Duration::from_secs(2)),
            check_tags_every: Some(Duration::from_secs(60)),
        }
    }
}

/// MoonProto transport mode selected on both client and server.
///
/// This is the public form of the Delphi `mask_ver` byte. Wire helpers still
/// take raw bytes internally; application code should use `TransportMode::V0`,
/// `TransportMode::V1`, or `TransportMode::V2`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct TransportMode(u8);

#[allow(non_upper_case_globals)]
impl TransportMode {
    pub const V0: Self = Self(0);
    pub const V1: Self = Self(1);
    pub const V2: Self = Self(2);

    pub const fn from_byte(b: u8) -> Self {
        match b {
            1 => Self::V1,
            2 => Self::V2,
            _ => Self::V0,
        }
    }

    pub const fn to_byte(self) -> u8 {
        self.0
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::V1 => "V1",
            Self::V2 => "V2",
            _ => "V0",
        }
    }
}

impl std::fmt::Debug for TransportMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// Configuration for one MoonProto UDP session.
///
/// Use [`ClientConfig::new`] for normal clients. It selects the open base
/// transport, generates a random client id, enables the Delphi-style
/// process-level NTP syncer, and enables active-library market refresh after
/// init. Direct struct literals remain available for test tools and advanced
/// protocol integrations.
#[derive(Clone)]
pub struct ClientConfig {
    /// Server host or IP address.
    pub server_ip: String,
    /// Server UDP port.
    pub server_port: u16,
    /// AES-GCM master key imported from MoonBot.
    pub master_key: MoonKey,
    /// Transport MAC/obfuscation key imported from MoonBot.
    pub mac_key: MoonKey,
    /// Transport mode (`V0`, `V1`, or `V2`). It must match the server-side
    /// connection setting.
    pub mask_ver: TransportMode,
    /// Client id sent in transport headers. `ClientConfig::new` generates it
    /// randomly; override only for deterministic tools/tests.
    pub client_id: u64,
    /// If `Some(host)`, `Client::new` acquires the process-level NTP syncer that
    /// updates `GlobalMPTimeOffset` about every 500 ms in the background. All
    /// clients in one process share the same worker, matching Delphi
    /// `TMoonProtoTymeSyncer` and its global offset.
    ///
    /// `None` disables managed NTP. This is useful for tests and tools that
    /// manage NTP explicitly through `ntp::spawn_sync_thread`.
    ///
    /// Use the same `ntp_host` for all clients in the process. If another host
    /// is requested while the process-level syncer is already running, the
    /// existing worker remains active because the corrected time offset is
    /// process-global, not per-client.
    pub ntp_host: Option<String>,
    /// Periodic refresh settings. Defaults enable Delphi-worker intervals, but
    /// Engine API refresh traffic starts only after successful init.
    pub refresh: RefreshConfig,
    /// Retained market-history capacity policy used when trades storage becomes
    /// active. Default `Auto` sizes rings from system memory after the market
    /// list and requested trade-storage scope are known.
    pub market_history: MarketHistorySizing,
}

impl ClientConfig {
    /// Create config with production defaults for V0/base transport:
    /// - `transport mode = V0`;
    /// - `client_id = rand::random()`;
    /// - `ntp_host = Some("pool.ntp.org")` (shared process-level syncer);
    /// - `refresh = RefreshConfig::default()` (Delphi-worker refresh after Init).
    ///
    /// Tests and offline tools can call [`Self::without_ntp`].
    /// Applications can select V1/V2 with [`Self::with_transport_mode`] when the
    /// server-side connection setting uses the same mode.
    pub fn new(
        server_ip: impl Into<String>,
        server_port: u16,
        master_key: MoonKey,
        mac_key: MoonKey,
    ) -> Self {
        Self {
            server_ip: server_ip.into(),
            server_port,
            master_key,
            mac_key,
            mask_ver: TransportMode::V0,
            client_id: rand::random(),
            ntp_host: Some("pool.ntp.org".to_string()),
            refresh: RefreshConfig::default(),
            market_history: MarketHistorySizing::default(),
        }
    }

    /// Override transport mode.
    pub fn with_transport_mode(mut self, mode: TransportMode) -> Self {
        self.mask_ver = mode;
        self
    }

    /// Override transport mode from a raw Delphi `mask_ver` byte.
    ///
    /// This is for config importers and protocol tests. Application code should
    /// call [`Self::with_transport_mode`] with a named [`TransportMode`].
    #[doc(hidden)]
    pub fn with_transport_mode_byte(mut self, mask_ver: u8) -> Self {
        self.mask_ver = TransportMode::from_byte(mask_ver);
        self
    }

    /// Override the random client id. Useful for deterministic tests and tools.
    pub fn with_client_id(mut self, client_id: u64) -> Self {
        self.client_id = client_id;
        self
    }

    /// Override the host used by the process-level NTP syncer.
    pub fn with_ntp_host(mut self, host: impl Into<String>) -> Self {
        self.ntp_host = Some(host.into());
        self
    }

    /// Disable managed NTP for this client.
    pub fn without_ntp(mut self) -> Self {
        self.ntp_host = None;
        self
    }

    /// Override periodic refresh behavior.
    pub fn with_refresh(mut self, refresh: RefreshConfig) -> Self {
        self.refresh = refresh;
        self
    }

    /// Override retained-history capacity sizing for trades/candles/price
    /// lines. Use `MarketHistorySizing::Auto` for memory-aware defaults or
    /// `MarketHistorySizing::fixed(config)` for exact per-market capacities.
    pub fn with_market_history(mut self, market_history: impl Into<MarketHistorySizing>) -> Self {
        self.market_history = market_history.into();
        self
    }
}

// Custom Debug keeps imported MoonBot keys out of accidental logs.
impl std::fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientConfig")
            .field("server_ip", &self.server_ip)
            .field("server_port", &self.server_port)
            .field("master_key", &"<REDACTED>")
            .field("mac_key", &"<REDACTED>")
            .field("mask_ver", &self.mask_ver)
            .field("client_id", &format_args!("{:#x}", self.client_id))
            .field("ntp_host", &self.ntp_host)
            .field("refresh", &self.refresh)
            .field("market_history", &self.market_history)
            .finish()
    }
}
