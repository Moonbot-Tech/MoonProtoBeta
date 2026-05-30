//! High-level MoonProto client session API.
//!
//! [`Client`] owns one UDP session: transport handshake, reconnect, retry
//! queues, slicing, pending Engine API responses, lifecycle events, and the
//! active read-model work performed by [`crate::events::EventDispatcher`].
//! Regular applications should use [`MoonClient`], which owns the runtime thread
//! and keeps the session alive until `disconnect()` or drop.

use crate::api_pending::ApiPending;
use crate::commands::candles::{
    parse_request_candles_data_response, parse_request_candles_data_response_partial,
    CandlesAggregator, CandlesChunkResult,
};
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
use polling::{Event as PollEvent, Events as PollEvents, Poller};
use std::collections::HashMap;
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
    MoonCandles, MoonClient, MoonClientError, MoonClientEvent, MoonClientSnapshot, MoonEventQueue,
    MoonEventSink, MoonOrders, MoonSettings, MoonStrategies, MoonStreams, MoonTrade,
    NewOrderParams, NewOrderTicket, OrderSide, OrderTarget, SellOrderParams, SplitOrderParams,
    TradesStreamMode, VStopParams,
};
pub use bps::BpsCounter;
pub(crate) use candles::MergedCandles;
pub use clock::set_ntp_offset;
pub use config::{
    AuthStatus, ClientConfig, LifecycleEvent, LifecycleFn, RefreshConfig, TradeContextError,
    TransportMode,
};
#[doc(hidden)]
pub use diagnostics::ERR_EMU_RATE;
pub use diagnostics::{
    set_err_emu, ErrEmuCommandDiagnostics, ErrEmuDiagnostics, ErrEmuSlicedBlockDiagnostics,
    ErrEmuSlicedDatagramDiagnostics,
};
#[cfg(test)]
pub(crate) use init::{connect_and_init, run_init_sequence, InitResult};
pub use init::{ConnectConfig, ConnectError, InitConfig, InitError, InitialStrategies};
pub use metrics::ProtocolMetricsSnapshot;
pub use send_queue::{
    SendPriority, UniqueKey, UK_ARB_PRICES, UK_BALANCE_FULL, UK_BASE_UI_SETTINGS, UK_DEX_SWITCH,
    UK_IMMUNE_CLICKS, UK_LEV_MANAGE_SETTINGS, UK_NONE, UK_ORDER_MOVE, UK_ORDER_STATUS,
    UK_ORDER_STATUS_SHORT, UK_SPOT_SWITCH, UK_STOP_MOVE, UK_STRAT_SELL_PRICE_UPDATE,
    UK_STRAT_SNAPSHOT, UK_TURN_MM_DETECTION,
};
#[doc(hidden)]
pub use sender::{ClientSender, SubscribeError};
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
    diagnostic_duplicate_sliced_acks, err_emu_drop_decision, fnv1a64, trace_elapsed_ms, trace_head,
    trace_io_enabled, ErrEmuDiagnosticsState,
};
#[cfg(test)]
use diagnostics::{err_emu_drop_rate_for_cmd, is_service_cmd};
use helpers::*;
#[cfg(test)]
pub(crate) use init::{run_base_check_delphi, send_post_init_resync, CriticalInitStatus};
use metrics::ProtocolMetrics;
use protocol_core::ProtocolCore;
#[cfg(test)]
pub(crate) use send_queue::SendQueues;
pub(crate) use send_queue::{initial_retry_left, SendItem, SendLockState};
pub(crate) use sender::ClientSenderShared;
use socket::{set_dont_fragment_for_socket, set_socket_buffers};
pub(crate) use subscriptions::{
    refresh_subscription_summary, DomainRestoreIntent, PendingTradesUnsubscribe,
    SubscriptionRegistry, SubscriptionRegistrySummary,
};
pub(crate) use transport_state::{DataReadState, ReaderSlicedStats, SentSliced, SlicedAck};

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
        matches!(
            self,
            Self::PrimaryHelloCold | Self::PrimaryHelloNewSession | Self::PrimaryImFriendSent
        )
    }

    #[inline]
    pub(crate) fn allows_fine(self) -> bool {
        matches!(self, Self::PrimaryImFriendSent | Self::RebindHelloAgain)
    }
}

/// Public handle to the client. Allows sending commands from any thread.
pub struct Client {
    cfg: ClientConfig,

    app_queue_alive: Arc<AtomicBool>,
    runtime_shutdown: Arc<AtomicBool>,
    // Delphi `SendLock`: raw send queues, SlicedACK queue, and TmpSlider are
    // copied as one short writer snapshot before CheckSeningData.
    send_lock: Arc<Mutex<SendLockState>>,

    // Pending H-commands (main thread only, no sharing)
    pending_h: Vec<SendItem>,
    // Sent Sliced datagrams awaiting ACK (matches TMoonProtoClient.Sending)
    sending: Vec<SentSliced>,

    // Main thread state
    socket: Option<UdpSocket>,
    recv_slicer: slicing::SlicingReceiver,
    recv_poller: Option<Poller>,
    recv_events: PollEvents,
    connected: bool, // FConnected: true after first valid packet received
    authorized: bool,
    last_online: i64,
    total_recv: u64,
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
    /// B-V2-03 fix: cached AES-128-GCM cipher for encode (encrypt direction).
    /// Refreshed together with `encode_key` at handshake. `Aes128Gcm::new` is
    /// expensive (key schedule expansion, ~100 bytes of work) — it used to run
    /// for every encrypted packet (thousands of times/sec). Now it runs once per
    /// session.
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
    bps_sent: BpsCounter,
    bps_recv: BpsCounter,

    // Log throttle: key -> last raise timestamp (anti-spam).
    log_last: std::collections::HashMap<&'static str, i64>,

    // Grouped send batch (TmpSendList equivalent)
    tmp_send_buf: Vec<u8>, // accumulated Grouped payload
    tmp_send_count: usize, // items in batch
    tmp_send_size: usize, // Delphi TmpSendSize accounting: sum of (payload + header + grouped item header)
    copy_send_sliced: Vec<SendItem>,
    copy_send_high: Vec<SendItem>,
    copy_send_low: Vec<SendItem>,
    copy_sliced_acks: Vec<SlicedAck>,

    // Delphi DataReadInt receive state that survives soft reconnect: MPSlider
    // replay/ACK bitmap, SizeAck series, and decode cipher. TmpSlider lives in
    // SendLockState so the send phase copies it atomically with ACK queues.
    data_read_state: DataReadState,
    // Delphi RecvdSlider/TmpSlider: server ACK bitmap from incoming MPC_Ping.
    // Reader/DataReadInt writes TmpSlider; writer CheckSeningData copies it to
    // RecvdSlider and only then drops ACKed PendingH.
    recvd_slider: Slider,
    total_sent: AtomicU64,
    total_recv_shared: AtomicU64,
    err_emu_diagnostics: Arc<Mutex<ErrEmuDiagnosticsState>>,
    protocol_metrics: Arc<ProtocolMetrics>,
    next_port: u16,
    ping_count: u32,

    /// Registry of pending Engine API requests.
    /// On receiving a `Command::API` packet, `dispatch` delivers the response
    /// to the registered receiver if the UID is found.
    api_pending: Arc<ApiPending>,

    /// Lifecycle callback — queued on channel status change (Connecting -> Connected{fresh} -> Reconnecting/Disconnected).
    /// Set it via `client.on_lifecycle(cb)`. Optional.
    lifecycle_cb: Option<LifecycleFn>,
    lifecycle_app_tx: Arc<Mutex<Option<mpsc::Sender<LifecycleEvent>>>>,
    /// Delphi `cfg.MoonProtoConfig.ServerUpdateSent`: set by UI commands that
    /// can make the server restart/change routing; consumed by BaseCheck init.
    server_update_sent: Arc<AtomicBool>,
    /// Previous auth_status (for detecting transitions).
    prev_auth_status: AuthStatus,

    /// Cached resolved server address. Closes B-05: previously `server_addr()` formatted
    /// a string and `send_to(&str)` ran a `getaddrinfo` resolve on every send (potentially
    /// DNS-blocking). The cache is cleared on a resolve error (e.g. DNS went down) — it is
    /// re-resolved on the next bind_socket.
    cached_server_addr: Option<SocketAddr>,

    /// **Active library — subscription registry**: what the app asked to subscribe.
    /// The transport handshake does not send this registry before Init. After Init,
    /// reconnect restores the registry itself via the current keys / market mapping.
    pub(crate) subscription_registry: Arc<Mutex<SubscriptionRegistry>>,
    subscription_summary: Arc<SubscriptionRegistrySummary>,
    subscription_trades_scope:
        Arc<parking_lot::RwLock<Option<Arc<crate::state::TradeStorageScope>>>>,

    /// Shared mirror of [`Self::domain_ready`] for [`ClientSender`].
    ///
    /// Typed/high-level domain APIs use this gate to record pre-init intent
    /// without putting domain wire commands into send queues before the single
    /// Init pass opens the Delphi `InitDone` gate.
    domain_ready_flag: Arc<AtomicBool>,

    /// Saved intent of the first and only init pass. Needed for post-reconnect
    /// restore without a second Init.
    domain_restore: DomainRestoreIntent,

    /// Whether a successful Connected ever happened (`Fine` received >=1 time).
    /// Used in `LifecycleEvent::Connected { fresh }` — `fresh = !was_ever_connected`
    /// on the FIRST Connected; for all later ones `fresh = false`.
    was_ever_connected: bool,

    /// Internal full-candles snapshot collectors by `request_uid`. Filled by
    /// the automatic Active Lib snapshot request and cleared when the
    /// aggregator completes or times out.
    ///
    /// Application code does not see this packet-shaped layer; it gets retained
    /// candles through snapshots/events.
    pending_candles: HashMap<u64, PartialCandles>,

    /// The previous PeerAppToken that was registered with `MarketsState.indexes_synchronized = true`.
    /// Used in handshake/Ping processing to detect a server restart:
    /// if incoming `peer_app_token != tracked_peer_app_token` — mark the indexes stale.
    /// 0 = no successful synchronization yet (init state).
    tracked_indexes_peer_app_token: u64,

    /// `true` if the init/API layer already sent a markets indexes request and is waiting for the response.
    /// Guards against a storm of repeated explicit requests before a response arrives.
    indexes_fetch_in_flight: bool,

    /// On reconnect restore: as soon as a fresh `GetMarketsIndexes` arrives
    /// successfully, immediately request `UpdateMarketsList`. This reproduces the
    /// Delphi meaning of `TMoonProtoEngine.UpdateMarketsList`: on a new `PeerAppToken`
    /// it first synchronizes indexes, then refreshes prices/funding.
    update_markets_after_indexes: bool,

    /// On reconnect restore: deferred replay of the orderbook registry until a fresh
    /// `GetMarketsIndexes`. Delphi `CheckBookTopics` returns early while
    /// `FLastServerAppToken <> PeerAppToken`; orderbook subscriptions cannot be replayed
    /// before the new server app session's indexes are synchronized.
    restore_orderbooks_after_indexes: bool,

    /// Delphi `TMoonProtoEngine.LastReconnectCheck` for AllTrades reconnect.
    /// `NeedReconnectAllTrades` spends this throttle before it runs the
    /// unsubscribe/sleep/subscribe sequence again.
    last_trades_reconnect_check_ms: i64,

    /// Last queued `emk_SubscribeAllTrades` request, including requests queued
    /// through `ClientSender`. Delphi `SubscribeAllTrades` blocks inside
    /// `SendAndWait` for `FTimeout=12000`, so `NeedReconnectAllTrades` cannot
    /// run while that request is in flight. Rust queues it asynchronously,
    /// therefore this timestamp is part of the machine-effect gate.
    last_trades_subscribe_request_ms: Arc<AtomicI64>,

    /// Delphi `TMoonProtoEngine.FSubscribedBookServerToken`: current
    /// `ServerToken` confirmed by a successful full `BookSubbed` batch subscribe.
    subscribed_book_server_token: u64,

    /// Delphi `TMoonProtoEngine.LastBookReconnectCheck`: 5s throttle for
    /// `NeedResubscribeOrderBooks`.
    last_book_reconnect_check_ms: i64,

    /// Last queued `emk_SubscribeOrderBook` request. Delphi
    /// `DoSubscribeOrderBooks` blocks in `SendAndWait` for `FTimeout=12000`;
    /// Rust queues orderbook subscribes asynchronously, so reconnect retry must
    /// not issue a second batch until the Delphi-equivalent wait window ends or
    /// a response closes it.
    last_orderbook_subscribe_request_ms: Arc<AtomicI64>,
    last_orderbook_subscribe_request_uid: Arc<AtomicU64>,

    /// UID of the last full-registry `emk_SubscribeOrderBook` replay. A success
    /// for this UID, unlike a normal one-market subscribe, is allowed to advance
    /// `subscribed_book_server_token`.
    pending_orderbook_resubscribe_uid: Option<u64>,

    /// Delayed `DoSubscribeAllTrades(false)` after Delphi `Sleep(100)` in
    /// `BMarketHistoryWorker.Execute` reconnect branch.
    ///
    /// The sleep starts only after `UnSubscribeAllTrades` has completed its
    /// Delphi `SendAndWait` equivalent. Sending Subscribe after a naked 100ms
    /// timer is wrong on UDP: a retried Unsubscribe can arrive after Subscribe
    /// and leave the server-side client unsubscribed.
    pending_trades_unsubscribe: Option<PendingTradesUnsubscribe>,
    pending_trades_resubscribe_after_ms: Option<i64>,

    /// FireTest-only hook: drop every outgoing datagram before socket send.
    /// This lets the live health test force a real server-side disconnect and
    /// then verify the library reconnect path. It is deliberately hidden from
    /// public API docs.
    debug_outgoing_blackhole: Arc<AtomicBool>,

    /// When (`now_ms`) the last `api_get_markets_indexes` was sent. Used for
    /// timeout protection: the UDP response may have been lost — after `INDEXES_FETCH_TIMEOUT_MS`
    /// we reset `indexes_fetch_in_flight = false`. The timeout handler itself does not
    /// resend the request: a new send is allowed only from the init/API layer.
    indexes_fetch_started_ms: i64,

    /// When `trades_state.tick()` was last called from the active main loop.
    /// Throttle ~100ms — matches the periodicity of Delphi
    /// `MoonProtoEngine.pas:1483 CheckMissingTradesPackets`.
    last_trades_tick_ms: i64,

    /// How many times in a row the whole 200-port retry in `bind_socket` failed. It is
    /// incremented on each failure series (= one main loop tick where all 200 ports were
    /// rejected); on the first successful bind it resets to 0. Used to emit
    /// `LifecycleEvent::BindFailed`. The event is throttled by real elapsed time:
    /// the first signal after 15s of continuous failures, then no more than once per 50s.
    /// See audit H9.
    bind_failure_streak: u32,
    first_bind_failure_ms: i64,
    last_bind_failed_event_ms: i64,

    /// Guard for the shared process-level NTP syncer (if `cfg.ntp_host = Some`).
    /// Dropping the last guard stops the worker. This matches Delphi's single
    /// `TMoonProtoTymeSyncer` for the process instead of a worker per client.
    _ntp_process_guard: Option<crate::ntp::ProcessNtpGuard>,

    /// F6/F7: timestamps of the last periodic refresh commands. `i64::MIN/2` =
    /// "never" -> the first tick fires immediately after Connected (if the matching
    /// interval is set in `cfg.refresh`). After that — every
    /// `update_markets_every` / `check_tags_every`.
    last_update_markets_ms: i64,
    last_check_tags_ms: i64,
    /// Delphi `BHeavyApiWorker` issues up to 4 quick `CheckBinanceTags` after
    /// the hour changes. These fields hold the current wall-clock hour slot and burst progress.
    check_tags_hour_slot: i64,
    check_tags_burst_sent: u8,
    last_check_tags_burst_ms: i64,

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

    /// Delphi `InitDone`: transport auth is already complete, but domain pushes
    /// (`Order`/`Strat`/`Balance`/`Trades*`/`OrderBook`/`UI`) can only be applied
    /// after the full init bootstrap. Before that, `dispatch_into_active`
    /// drops these channels, like `TMoonProtoNetClient.ClientNewData`.
    domain_ready: bool,

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

    /// Cached MAC context — the ipad CRC + opad block computed once for `cfg.mac_key`.
    /// Used on the transport pack/unpack hot-path instead of recomputing HMAC ipad/opad
    /// (128 XOR + crc32c) for every packet. See `crate::transport::MacContext`.
    ///
    /// Since `mac_key` is fixed for the whole life of the Client (it comes in
    /// ClientConfig and never changes), this context is also fixed and
    /// reused by the receive/send phases.
    mac_ctx: crate::transport::MacContext,

    /// Delphi `SentCountDNS` equivalent for transport mode V2.
    /// Lives on the client, not in a global/static transport helper.
    transport_mode_state: crate::transport::ClientTransportModeState,

    /// Reusable buffer for client transport pack — saves an alloc/dealloc on every
    /// outgoing packet. Capacity grows up to the peak packet size and is reused.
    /// audit_rust_quality #4: 50K pps × 1500B = 75 MB/s of allocator pressure eliminated.
    send_buf: Vec<u8>,
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
        !self.authorized || self.need_connect || self.hello_wait_state.allows_hello_again_retry()
    }

    fn has_trades_subscription_intent(&self) -> bool {
        self.subscription_summary.trades_subscribed()
    }

    pub(crate) fn trades_storage_scope_intent(
        &self,
    ) -> Option<Arc<crate::state::TradeStorageScope>> {
        if !self.subscription_summary.trades_subscribed() {
            return None;
        }
        self.subscription_trades_scope.read().clone()
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
        let err_emu_diagnostics = Arc::new(Mutex::new(ErrEmuDiagnosticsState::default()));
        let protocol_metrics = Arc::new(ProtocolMetrics::default());
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

        // Cached MacContext for cfg.mac_key — fixed for the whole life of the Client.
        // Construction does 128 XOR + crc32c(ipad_block) once; afterwards `mac()` calls
        // are only crc32c_append(cached, data) + crc32c_append(prev, opad_block).
        let mac_ctx = crate::transport::MacContext::new(&cfg.mac_key);

        Self {
            cfg,
            app_queue_alive,
            runtime_shutdown,
            send_lock,
            pending_h: Vec::new(),
            sending: Vec::new(),
            socket: None,
            recv_slicer: slicing::SlicingReceiver::new(),
            recv_poller: None,
            recv_events: PollEvents::new(),
            connected: false,
            authorized: false,
            last_online: 0,
            total_recv: 0,
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
            bps_sent: BpsCounter::new(),
            bps_recv: BpsCounter::new(),
            log_last: std::collections::HashMap::new(),
            tmp_send_buf: Vec::new(),
            tmp_send_count: 0,
            tmp_send_size: 0,
            copy_send_sliced: Vec::new(),
            copy_send_high: Vec::new(),
            copy_send_low: Vec::new(),
            copy_sliced_acks: Vec::new(),
            data_read_state: DataReadState::new(),
            recvd_slider: Slider::new(),
            total_sent: AtomicU64::new(0),
            total_recv_shared: AtomicU64::new(0),
            err_emu_diagnostics,
            protocol_metrics,
            next_port: 1024 + (rand::random::<u16>() % (65000 - 1024)),
            ping_count: 0,
            api_pending: ApiPending::new_arc(),
            lifecycle_cb: None,
            lifecycle_app_tx: Arc::new(Mutex::new(None)),
            server_update_sent: Arc::new(AtomicBool::new(false)),
            prev_auth_status: AuthStatus::Base,
            cached_server_addr: None,
            subscription_registry: Arc::new(Mutex::new(SubscriptionRegistry::default())),
            subscription_summary,
            subscription_trades_scope,
            domain_ready_flag,
            domain_restore: DomainRestoreIntent::default(),
            was_ever_connected: false,
            pending_candles: HashMap::new(),
            tracked_indexes_peer_app_token: 0,
            indexes_fetch_in_flight: false,
            update_markets_after_indexes: false,
            restore_orderbooks_after_indexes: false,
            last_trades_reconnect_check_ms: NEVER_TIME_MS,
            last_trades_subscribe_request_ms,
            subscribed_book_server_token: 0,
            last_book_reconnect_check_ms: NEVER_TIME_MS,
            last_orderbook_subscribe_request_ms,
            last_orderbook_subscribe_request_uid,
            pending_orderbook_resubscribe_uid: None,
            pending_trades_unsubscribe: None,
            pending_trades_resubscribe_after_ms: None,
            debug_outgoing_blackhole: Arc::new(AtomicBool::new(false)),
            indexes_fetch_started_ms: 0,
            last_trades_tick_ms: i64::MIN / 2,
            bind_failure_streak: 0,
            first_bind_failure_ms: NEVER_TIME_MS,
            last_bind_failed_event_ms: NEVER_TIME_MS,
            _ntp_process_guard: ntp_process_guard,
            server_time_delta_handle: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            server_info: crate::commands::engine_api::ServerInfo::default(),
            server_base_currency_name_arc: None,
            auth_info: None,
            domain_ready: false,
            last_update_markets_ms: i64::MIN / 2,
            last_check_tags_ms: i64::MIN / 2,
            check_tags_hour_slot: i64::MIN,
            check_tags_burst_sent: CHECK_TAGS_BURST_COUNT,
            last_check_tags_burst_ms: i64::MIN / 2,
            mac_ctx,
            transport_mode_state: crate::transport::ClientTransportModeState::new(),
            send_buf: Vec::with_capacity(2048), // typical send packet ~500-1500 bytes
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
        self.subscribed_book_server_token = token;
    }

    fn set_domain_ready(&mut self, ready: bool) {
        self.domain_ready = ready;
        self.domain_ready_flag.store(ready, Ordering::Relaxed);
    }

    #[inline]
    fn domain_ready_for_typed_send(&self) -> bool {
        self.domain_ready
    }

    #[cfg(test)]
    pub(crate) fn testing_set_domain_ready(&mut self, ready: bool) {
        self.set_domain_ready(ready);
    }

    #[cfg(test)]
    pub(crate) fn testing_set_peer_app_tokens(&mut self, peer: u64, tracked: u64) {
        self.peer_app_token = peer;
        self.tracked_indexes_peer_app_token = tracked;
    }
}

/// Drop: mark app queues closed and unregister the UDP poller even if the
/// consumer did not call `disconnect()`.
/// The process-level NTP guard is released automatically after the `drop` body; if
/// this was the last client, the shared NTP worker stops.
impl Drop for Client {
    fn drop(&mut self) {
        self.app_queue_alive.store(false, Ordering::Relaxed);
        self.clear_recv_poller();
    }
}

#[cfg(test)]
mod tests;
