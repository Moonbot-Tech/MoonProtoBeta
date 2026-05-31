//! High-level MoonProto client session API.
//!
//! [`Client`] owns one UDP session: transport handshake, reconnect, retry
//! queues, slicing, pending Engine API responses, lifecycle events, and the
//! active read-model work performed by [`crate::events::EventDispatcher`].
//! Regular applications should use [`MoonClient`], which owns the runtime thread
//! and keeps the session alive until `disconnect()` or drop.

use crate::api_pending::ApiPending;
use crate::commands::candles::{CandlesAggregator, CandlesChunkResult};
use crate::commands::engine_api::{
    parse_auth_check_response, parse_base_check_response, parse_engine_response, AuthCheckResponse,
    EngineMethod, EngineResponse,
};
use crate::compression;
use crate::crypto;
use crate::protocol::{control, crypted, handshake, slicing, slider::Slider, Command};
use crate::MoonKey;
use log::{debug, error, warn};
// MoonProto UDP Client architecture follows Delphi receive machine effects
// inside one ProtocolCore owner: recv drain, immediate service replies,
// domain dispatch enqueue, then send/maintenance.
use polling::{Event as PollEvent, Poller};
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

mod active_runtime;
mod app_dispatch;
mod bps;
mod candles;
mod clock;
mod config;
mod constants;
mod diagnostic_api;
mod diagnostics;
mod domain_balance;
mod domain_send;
mod domain_strat;
mod domain_trade;
mod domain_ui;
mod engine_api;
mod helpers;
mod init;
mod lifecycle;
mod metrics;
mod protocol_api;
mod protocol_connect;
mod protocol_core;
mod protocol_delivery;
mod protocol_direct_send;
mod protocol_helpers;
mod protocol_io;
mod protocol_recv;
mod protocol_recv_handlers;
mod protocol_recv_state;
mod protocol_send;
mod protocol_sliced_send;
mod protocol_tick;
mod refresh_clocks;
mod runtime;
mod runtime_active_actions;
mod runtime_dispatcher;
mod runtime_wait;
mod send_api;
mod send_queue;
mod sender;
mod session_api;
mod socket;
mod socket_lifecycle;
mod subscription_api;
mod subscriptions;
mod transport_state;

pub use active_runtime::{
    ClosePositionParams, CoinCardCandlesTicket, EngineActionTicket, MoonAccount, MoonBalances,
    MoonCandles, MoonClient, MoonClientError, MoonClientEvent, MoonClientSnapshot, MoonEmulator,
    MoonEventQueue, MoonEventSink, MoonOrders, MoonSettings, MoonStrategies, MoonStreams,
    MoonTrade, NewOrderParams, NewOrderTicket, OrderSide, OrderTarget, SellOrderParams,
    SplitOrderParams, TradesStreamMode, VStopParams,
};
pub use bps::BpsCounter;
pub(crate) use candles::MergedCandles;
pub use clock::set_ntp_offset;
pub use config::{
    AuthStatus, ClientConfig, LifecycleEvent, LifecycleFn, RefreshConfig, TradeContextError,
    TransportMode,
};
#[cfg(any(test, feature = "diagnostics"))]
#[doc(hidden)]
pub use diagnostics::ERR_EMU_RATE;
#[cfg(any(test, feature = "diagnostics"))]
#[doc(hidden)]
pub use diagnostics::{
    set_err_emu, ErrEmuCommandDiagnostics, ErrEmuDiagnostics, ErrEmuSlicedBlockDiagnostics,
    ErrEmuSlicedDatagramDiagnostics,
};
#[cfg(test)]
pub(crate) use init::{connect_and_init, run_init_sequence, InitResult};
pub use init::{ConnectConfig, ConnectError, InitConfig, InitError, InitialStrategies};
#[cfg(any(test, feature = "diagnostics"))]
pub use metrics::ProtocolMetricsSnapshot;
pub use send_queue::{
    SendPriority, UniqueKey, UK_ARB_PRICES, UK_BALANCE_FULL, UK_BASE_UI_SETTINGS, UK_DEX_SWITCH,
    UK_IMMUNE_CLICKS, UK_LEV_MANAGE_SETTINGS, UK_NONE, UK_ORDER_MOVE, UK_ORDER_STATUS,
    UK_ORDER_STATUS_SHORT, UK_SPOT_SWITCH, UK_STOP_MOVE, UK_STRAT_SELL_PRICE_UPDATE,
    UK_STRAT_SNAPSHOT, UK_TURN_MM_DETECTION,
};
pub(crate) use sender::{ClientSender, SubscribeError};
pub use subscriptions::{ActiveSubscriptions, TradesSubscription};

#[cfg(test)]
pub(crate) use app_dispatch::OnDataFn;
#[cfg(test)]
use app_dispatch::RawAppEvent;
use app_dispatch::{metric_api_method, DispatchSink, DispatcherEventFn, RunMode};
pub(crate) use candles::{EngineResponseMeta, PartialCandles};
pub(crate) use clock::{
    current_utc_hour_slot, delphi_now, delphi_now_raw, get_server_time_delta_global,
    set_server_time_delta_global,
};
use config::{CHECK_TAGS_BURST_COUNT, CHECK_TAGS_BURST_SPACING_MS};
use constants::*;
use diagnostics::{
    diagnostic_duplicate_sliced_acks, fnv1a64, trace_elapsed_ms, trace_head, trace_io_enabled,
};
#[cfg(any(test, feature = "diagnostics"))]
use diagnostics::{err_emu_drop_decision, ErrEmuDiagnosticsState};
#[cfg(test)]
use diagnostics::{err_emu_drop_rate_for_cmd, is_service_cmd};
use helpers::*;
#[cfg(test)]
pub(crate) use init::{run_base_check_delphi, send_post_init_resync, CriticalInitStatus};
use lifecycle::ClientLifecycle;
use metrics::ClientMetrics;
#[cfg(any(test, feature = "diagnostics"))]
use metrics::ProtocolMetrics;
use protocol_core::ProtocolCore;
use refresh_clocks::{PendingApi, RefreshClocks};
#[cfg(test)]
pub(crate) use send_queue::SendQueues;
pub(crate) use send_queue::{initial_retry_left, SendItem, SendLockState};
pub(crate) use sender::ClientSenderShared;
use socket::{set_dont_fragment_for_socket, set_socket_buffers, ClientTransport};
pub(crate) use subscriptions::{
    refresh_subscription_summary, DomainRestoreIntent, PendingTradesUnsubscribe, ReconnectRestore,
    SubscriptionRegistry, SubscriptionRegistrySummary, Subscriptions,
};
pub(crate) use transport_state::{
    DataReadState, ReaderSlicedStats, RecvState, SentSliced, SlicedAck,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HelloWaitState {
    Idle,
    PrimaryHelloCold,
    PrimaryHelloNewSession,
    PrimaryImFriendSent,
    RebindHelloAgain,
}

impl HelloWaitState {
    #[inline]
    pub(crate) fn is_waiting(self) -> bool {
        !matches!(self, Self::Idle)
    }

    #[inline]
    pub(crate) fn allows_hello_again_retry(self) -> bool {
        matches!(self, Self::RebindHelloAgain)
    }

    #[inline]
    pub(crate) fn allows_who_are_you(self) -> bool {
        matches!(self, Self::PrimaryHelloCold | Self::PrimaryHelloNewSession)
    }

    #[inline]
    pub(crate) fn allows_fine(self) -> bool {
        matches!(self, Self::PrimaryImFriendSent | Self::RebindHelloAgain)
    }
}

/// Server/account identity learned during Init (`emk_BaseCheck` / `emk_AuthCheck`).
///
/// Grouped off the [`Client`] God object: the `BaseCheck` server identity, the
/// per-packet `Arc<str>` cache of its base-currency name, and the `AuthCheck`
/// per-account payload. Field types and meaning are unchanged from when they
/// lived directly on `Client`; see the accessor docs in `session_api.rs`.
struct SessionIdentity {
    /// Server identity obtained from the `emk_BaseCheck` response. Filled during
    /// Init (or by an internal test via [`Client::set_server_info`]). Before the first
    /// successful BaseCheck — `ServerInfo::default()` (all fields `None`,
    /// `has_identity()=false`).
    ///
    /// **Multi-server**: when connecting to several servers the application keeps a
    /// `Vec<Client>` and tells them apart by `client.server_info().bot_id`.
    server_info: crate::commands::engine_api::ServerInfo,

    /// Cache of `server_info.base_currency_name` as `Arc<str>`. Cloned (refcount-bump)
    /// in `ActiveDispatchContext::from_client` on EVERY packet instead of heap-cloning the
    /// string — Delphi reads `cfg.BaseCurrency` inline without a copy (parity, opt #7). The
    /// public `ServerInfo.base_currency_name` stays a `String` for API ergonomics.
    server_base_currency_name_arc: Option<std::sync::Arc<str>>,

    /// Per-account data received from Delphi `TMoonProtoEngine.AuthCheck`.
    ///
    /// Delphi stores `BinanceAccountID`, `BTCAddress`, `AccountID`,
    /// `RecvdMaxPayload`, and Hyperliquid DEX tail in local engine/cfg state
    /// during init. Rust keeps the parsed payload here so active-lib state and
    /// user code can observe the same successful AuthCheck result.
    auth_info: Option<crate::commands::engine_api::AuthCheckResponse>,
}

impl SessionIdentity {
    fn new() -> Self {
        Self {
            server_info: crate::commands::engine_api::ServerInfo::default(),
            server_base_currency_name_arc: None,
            auth_info: None,
        }
    }
}

/// Low-level UDP session object behind [`MoonClient`].
///
/// Regular applications should not own this directly. It remains public under
/// `moonproto::client` for internal diagnostics and custom protocol tools, but
/// the application model is [`MoonClient`] plus EventSink/snapshots/intents.
#[doc(hidden)]
pub struct Client {
    cfg: ClientConfig,

    /// Runtime/lifecycle state: the lifecycle callback plumbing
    /// (`lifecycle_cb`/`lifecycle_app_tx`), the first-Connected marker
    /// (`was_ever_connected`), and the shutdown/queue flags shared with the
    /// runtime thread and `ClientSender` (`app_queue_alive`/`runtime_shutdown`).
    /// See [`ClientLifecycle`].
    lifecycle: ClientLifecycle,
    // Delphi `SendLock`: raw send queues, SlicedACK queue, and TmpSlider are
    // copied as one short writer snapshot before CheckSeningData.
    send_lock: Arc<Mutex<SendLockState>>,

    // Pending H-commands (main thread only, no sharing)
    pending_h: Vec<SendItem>,
    // Sent Sliced datagrams awaiting ACK (matches TMoonProtoClient.Sending)
    sending: Vec<SentSliced>,

    // Transport: UDP socket, recv/send buffers, slicing receiver, bind-port
    // cursor, cached server address, bind-failure tracking, MacContext and the
    // V2 transport-mode state. The hot recv/send path. `last_socket_recreate`
    // stays on `Client` (reconnect-throttle clock). See [`ClientTransport`].
    transport: ClientTransport,
    connected: bool, // FConnected: true after first valid packet received
    authorized: bool,
    last_online: i64,
    auth_status: AuthStatus,
    need_connect: bool,
    force_disconnect: bool,
    soft_reconnect: bool,
    hello_wait_state: HelloWaitState,
    next_primary_hello_new_session: bool,
    waiting_hello: bool,

    client_token: u64,
    server_token: u64,
    app_token: u64,
    encode_key: MoonKey,
    decode_key: MoonKey,
    /// Cached AES-128-GCM cipher for encode direction. Handshake refreshes it
    /// with `encode_key`, so encrypted packets do not rebuild the AES key
    /// schedule on the send hot path.
    encode_cipher: Option<crate::crypto::Aes128Gcm>,

    _start: Instant,
    last_sent_hello: i64,
    waiting_hello_start: i64,
    last_socket_recreate: i64,
    last_need_hello_again: i64,
    prev_cycle_tm: i64, // for ActualSleepTime EMA

    crypt_msg_counter: AtomicU64,
    send_datagram_num: u16,

    round_trip_delay: i64,
    actual_pmtu: u16,
    rs: f64,
    overheat: u8,
    peer_app_token: u64,    // PeerAppToken from WhoAreYou (detect server restart)
    server_time_delta: f64, // ServerTimeDelta = Ping.InitialTime - Now (for order time correction)
    global_timing_orders: u16, // GlobalTimingOrders from Ping
    net_lag_ping: i64,      // NetLagPing (ms abs diff between NTP-corrected time and server time)

    // Adaptive rate control (matches MoonProtoIntStruct.pas:197-245)
    trip_delay_k: f64,        // TripDelayK (1.05-1.25, init 1.1)
    last_set_trip_k: i64,     // LastSetTripK
    last_checked_slices: i64, // LastCheckedSlices: outer CheckSeningData gate
    avg_dup_count: f64,       // AvgDupCount
    avg_over_heat: f64,       // AvgOverHeat (% retransmission overhead, EMA, matches :1210-1212)
    can_send_rate: i32,       // CanSendRate (bytes/sec, init 2MB/s)
    used_sliced_limit: bool,  // UsedSlicedLimit
    actual_sleep_time: f64,   // ActualSleepTime (EMA of actual loop cycle time)

    // BytesPerSec sliding window (10 sec) — observability metric.
    // B-13 fix: a running sum is maintained alongside the window — `bytes_per_sec_*` is O(1)
    // instead of an O(N) walk on every request.
    // #5 audit_delphi_deviation: O(1) EMA counter (port of Delphi MoonProtoUDPClient.pas:113-138
    // AddBytesCount). The VecDeque<(i64,u64)> sliding window was removed — it cost ~8MB heap on
    // a 50K pps burst plus 100K push_back/pop_front ops/sec. Now it is 24 bytes + 4 ops/add.
    //
    // Observability/diagnostics cluster: byte/packet counters (`total_recv`,
    // `total_sent`, `total_recv_shared`), the BytesPerSec EMA counters
    // (`bps_sent`/`bps_recv`), the passive `protocol_metrics` sink, the
    // `log_last` throttle table, the client-side `err_emu_diagnostics`, and the
    // FireTest `debug_outgoing_blackhole` hook. None of these influence
    // send/retry/drop decisions. See [`metrics::ClientMetrics`].
    metrics: ClientMetrics,

    // Grouped send batch (TmpSendList equivalent)
    tmp_send_buf: Vec<u8>, // accumulated Grouped payload
    tmp_send_count: usize, // items in batch
    tmp_send_size: usize, // Delphi TmpSendSize accounting: sum of (payload + header + grouped item header)
    copy_send_sliced: Vec<SendItem>,
    copy_send_high: Vec<SendItem>,
    copy_send_low: Vec<SendItem>,
    copy_sliced_acks: Vec<SlicedAck>,

    // Receive/replay state grouped off the Client God object: the Delphi
    // DataReadInt receive state (`data_read_state` — MPSlider replay/ACK bitmap,
    // SizeAck series, decode cipher) and the `recvd_slider` server-ACK bitmap
    // copied from TmpSlider. Both survive a soft reconnect. See [`RecvState`].
    recv: RecvState,
    ping_count: u32,

    /// In-flight Engine API response collectors: the pending-request registry
    /// (`api_pending`) and the internal chunked full-candles snapshot collectors
    /// (`pending_candles`). `api_pending` is cloned into the runtime loop. See
    /// [`PendingApi`].
    pending_api: PendingApi,

    /// Previous auth_status (for detecting transitions).
    prev_auth_status: AuthStatus,

    /// **Active library — subscription cluster**: the subscription registry +
    /// atomic summary mirror + trades-scope filter, the Delphi `InitDone` domain
    /// gate (`domain_ready` + its `Arc<AtomicBool>` mirror for `ClientSender`),
    /// and the saved single-Init restore intent. See [`Subscriptions`].
    subscriptions: Subscriptions,

    /// **Active library — post-reconnect restore bookkeeping**: markets-indexes
    /// restore state (tracked peer token + in-flight fetch guard + deferred
    /// update-markets/orderbook flags), all-trades reconnect clocks/requests, and
    /// orderbook reconnect clocks/requests. The three `Arc<Atomic*>` request
    /// clocks inside are also cloned into `ClientSender`. See [`ReconnectRestore`].
    reconnect: ReconnectRestore,

    // Bind-failure tracking (`bind_failure_streak`, `first_bind_failure_ms`,
    // `last_bind_failed_event_ms`) — emits `LifecycleEvent::BindFailed`, audit
    // H9 — now lives on [`Self::transport`].
    /// Guard for the shared process-level NTP syncer (if `cfg.ntp_host = Some`).
    /// Dropping the last guard stops the worker. This matches Delphi's single
    /// `TMoonProtoTymeSyncer` for the process instead of a worker per client.
    _ntp_process_guard: Option<crate::ntp::ProcessNtpGuard>,

    /// **Periodic refresh clocks**: the F6/F7 update-markets / check-tags timers
    /// and burst state, plus the Delphi `ServerUpdateSent` marker
    /// (`server_update_sent`, cloned into `ClientSender`). See [`RefreshClocks`].
    refresh_clocks: RefreshClocks,

    /// Server/account identity learned during Init (`emk_BaseCheck` /
    /// `emk_AuthCheck`): `server_info`, the per-packet `Arc<str>` base-currency
    /// cache, and the `auth_info` per-account payload. See
    /// [`SessionIdentity`] and the accessors in `session_api.rs`.
    identity: SessionIdentity,

    /// **Per-Client ServerTimeDelta handle** — shareable via `Arc::clone`.
    ///
    /// Holds the current `ServerTimeDelta` (in days, TDateTime format, packed into u64
    /// via `f64::to_bits`). Updated when processing `MPC_Ping`, in sync with
    /// `self.server_time_delta` and with the global `SERVER_TIME_DELTA_DAYS`,
    /// which is only needed by raw `EventDispatcher::dispatch_into` without a handle.
    ///
    /// **Multi-Client**: `EventDispatcher` must be bound to
    /// this handle via `EventDispatcher::set_server_time_delta_source(handle)`
    /// or automatically through the active runtime. Without
    /// the binding, EventDispatcher falls back to the global, which under multi-Client gives
    /// off-by-50-1000ms timestamps in orders (the last Client overwrites the
    /// delta of all the others).
    server_time_delta_handle: Arc<std::sync::atomic::AtomicU64>,
    // `mac_ctx`, `transport_mode_state`, and `send_buf` now live on
    // [`Self::transport`]; see [`ClientTransport`].
}

impl Client {
    #[inline]
    pub(crate) fn set_hello_wait_state(&mut self, state: HelloWaitState) {
        self.hello_wait_state = state;
        self.waiting_hello = state.is_waiting();
    }

    #[inline]
    pub(crate) fn start_hello_wait(&mut self, state: HelloWaitState, cur_tm: i64) {
        self.waiting_hello_start = cur_tm;
        self.set_hello_wait_state(state);
    }

    #[inline]
    pub(crate) fn clear_hello_wait_state(&mut self) {
        self.set_hello_wait_state(HelloWaitState::Idle);
    }

    #[inline]
    pub(crate) fn mark_next_primary_hello_new_session(&mut self) {
        self.next_primary_hello_new_session = true;
        self.clear_hello_wait_state();
    }

    #[inline]
    pub(crate) fn should_accept_want_new_hello(&self) -> bool {
        self.hello_wait_state.allows_hello_again_retry()
    }

    fn has_trades_subscription_intent(&self) -> bool {
        self.subscriptions.subscription_summary.trades_subscribed()
    }

    pub(crate) fn trades_storage_scope_intent(
        &self,
    ) -> Option<Arc<crate::state::TradeStorageScope>> {
        if !self.subscriptions.subscription_summary.trades_subscribed() {
            return None;
        }
        self.subscriptions.subscription_trades_scope.read().clone()
    }

    /// Create a client session from [`ClientConfig`].
    ///
    /// Construction does not contact the server. The socket, handshake, and
    /// reconnect machinery start when a runtime begins pumping the protocol.
    /// Regular applications should prefer the high-level [`MoonClient`]
    /// runtime, which owns that pump internally.
    ///
    /// The returned client owns unbounded Delphi-style protocol queues. Clone
    /// [`Self::sender`] before entering a long-running loop when other UI or
    /// worker threads need to enqueue commands.
    pub fn new(cfg: ClientConfig) -> Self {
        // Delphi queues are ordinary grow-only TList/TDictionary structures with no
        // fixed capacity cap. Keep Rust queues unbounded too: accepted UDP packets
        // and user commands must not disappear because a local channel filled up.
        // Reader packets and raw send queues are separate so dense incoming
        // streams cannot keep user/API sends behind recv progress.
        let app_queue_alive = Arc::new(AtomicBool::new(true));
        let runtime_shutdown = Arc::new(AtomicBool::new(false));
        let send_lock = Arc::new(Mutex::new(SendLockState::default()));
        let subscription_summary = Arc::new(SubscriptionRegistrySummary::default());
        let subscription_trades_scope = Arc::new(parking_lot::RwLock::new(None));
        let domain_ready_flag = Arc::new(AtomicBool::new(false));
        let last_trades_subscribe_request_ms = Arc::new(AtomicI64::new(NEVER_TIME_MS));
        let last_orderbook_subscribe_request_ms = Arc::new(AtomicI64::new(NEVER_TIME_MS));
        let last_orderbook_subscribe_request_uid =
            Arc::new(AtomicU64::new(NO_PENDING_ENGINE_REQUEST_UID));

        // Active library F8: acquire the Delphi-style process-level NTP syncer
        // when cfg.ntp_host is set. It periodically updates GlobalMPTimeOffset
        // through set_ntp_offset and is shared by all clients in this process.
        let ntp_process_guard = cfg
            .ntp_host
            .as_ref()
            .and_then(|host| crate::ntp::acquire_process_sync(host.clone(), set_ntp_offset));

        // Cached SipHash-1-3 context for cfg.mac_key — fixed for the whole
        // life of the Client. `mac()` reuses the keyed initial state instead of
        // deriving it again for each packet.
        let mac_ctx = crate::transport::MacContext::new(&cfg.mac_key);

        Self {
            cfg,
            lifecycle: ClientLifecycle::new(app_queue_alive, runtime_shutdown),
            send_lock,
            pending_h: Vec::new(),
            sending: Vec::new(),
            transport: ClientTransport::new(
                mac_ctx,
                1024 + (rand::random::<u16>() % (65000 - 1024)),
            ),
            connected: false,
            authorized: false,
            last_online: 0,
            auth_status: AuthStatus::Base,
            need_connect: true,
            force_disconnect: false,
            soft_reconnect: false,
            hello_wait_state: HelloWaitState::Idle,
            next_primary_hello_new_session: false,
            waiting_hello: false,
            client_token: rand::random::<u64>(),
            server_token: 0,
            app_token: rand::random(),
            encode_key: [0; 16],
            decode_key: [0; 16],
            encode_cipher: None,
            _start: Instant::now(),
            // NEVER_SENT sentinel = "long ago". Any `(cur_tm - NEVER_SENT) > interval`
            // is instantly true -> the first Hello / cleanup / etc fire on the first main loop tick
            // (5ms after bind instead of a 2 second delay). Delphi used `GetTickCount64`
            // (milliseconds since boot) ~= 10^7+ when initializing `FLastSentHello := 0`, which gave
            // the same effect; in Rust `now_ms()` = `Instant::elapsed()` starts at 0 -> an explicit
            // sentinel is needed. See delphi_deviation audit #1.
            last_sent_hello: NEVER_SENT_MS,
            waiting_hello_start: 0,
            last_socket_recreate: i64::MIN / 2,
            last_need_hello_again: i64::MIN / 2,
            prev_cycle_tm: 0,
            crypt_msg_counter: AtomicU64::new(0),
            send_datagram_num: 0,
            round_trip_delay: 0,
            actual_pmtu: 508,
            rs: 1.0,
            overheat: 0,
            peer_app_token: 0,
            server_time_delta: 0.0,
            global_timing_orders: 0,
            net_lag_ping: 0,
            trip_delay_k: 1.1,
            last_set_trip_k: i64::MIN / 2,
            last_checked_slices: 0,
            avg_dup_count: 0.0,
            avg_over_heat: 0.0,
            can_send_rate: 2 * 1024 * 1024, // StartCanSendRate = 2 MB/s
            used_sliced_limit: false,
            actual_sleep_time: 5.0,
            metrics: ClientMetrics::new(),
            tmp_send_buf: Vec::new(),
            tmp_send_count: 0,
            tmp_send_size: 0,
            copy_send_sliced: Vec::new(),
            copy_send_high: Vec::new(),
            copy_send_low: Vec::new(),
            copy_sliced_acks: Vec::new(),
            recv: RecvState::new(),
            ping_count: 0,
            pending_api: PendingApi::new(),
            prev_auth_status: AuthStatus::Base,
            subscriptions: Subscriptions::new(
                subscription_summary,
                subscription_trades_scope,
                domain_ready_flag,
            ),
            reconnect: ReconnectRestore::new(
                last_trades_subscribe_request_ms,
                last_orderbook_subscribe_request_ms,
                last_orderbook_subscribe_request_uid,
            ),
            _ntp_process_guard: ntp_process_guard,
            server_time_delta_handle: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            identity: SessionIdentity::new(),
            refresh_clocks: RefreshClocks::new(Arc::new(AtomicBool::new(false))),
        }
    }

    /// Test-only setter for `server_token` — lets a test simulate the state after
    /// a successful handshake without a real network connection. Used in
    /// `events.rs` tests to verify `dispatch_into_active` token tracking.
    #[cfg(test)]
    pub(crate) fn testing_set_server_token(&mut self, token: u64) {
        self.server_token = token;
    }

    #[cfg(test)]
    pub(crate) fn testing_set_subscribed_book_server_token(&mut self, token: u64) {
        self.reconnect.subscribed_book_server_token = token;
    }

    fn set_domain_ready(&mut self, ready: bool) {
        self.subscriptions.domain_ready = ready;
        self.subscriptions
            .domain_ready_flag
            .store(ready, Ordering::Relaxed);
    }

    #[inline]
    fn domain_ready_for_typed_send(&self) -> bool {
        self.subscriptions.domain_ready
    }

    #[cfg(test)]
    pub(crate) fn testing_set_domain_ready(&mut self, ready: bool) {
        self.set_domain_ready(ready);
    }

    #[cfg(test)]
    pub(crate) fn testing_set_peer_app_tokens(&mut self, peer: u64, tracked: u64) {
        self.peer_app_token = peer;
        self.reconnect.tracked_indexes_peer_app_token = tracked;
    }
}

/// Drop: mark app queues closed and unregister the UDP poller even if the
/// consumer did not call `disconnect()`.
/// The process-level NTP guard is released automatically after the `drop` body; if
/// this was the last client, the shared NTP worker stops.
impl Drop for Client {
    fn drop(&mut self) {
        self.lifecycle
            .app_queue_alive
            .store(false, Ordering::Relaxed);
        self.clear_recv_poller();
    }
}

#[cfg(test)]
mod tests;
