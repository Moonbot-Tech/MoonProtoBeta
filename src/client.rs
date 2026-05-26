//! High-level MoonProto client session API.
//!
//! [`Client`] owns one UDP session: transport handshake, reconnect, retry
//! queues, slicing, pending Engine API responses, lifecycle events, and the
//! active read-model work performed by [`crate::events::EventDispatcher`].
//! Regular applications should create one `Client` plus one dispatcher per
//! server and use [`connect_and_init`] followed by
//! [`Client::run_with_dispatcher`].
//!
//! This module also exposes low-level command queue primitives for protocol
//! tools. Application code should prefer the typed helpers on `Client` and
//! [`ClientSender`] because those helpers encode Delphi priority, retry, UKey,
//! encryption, and reconnect-registry behavior.

use crate::api_pending::ApiPending;
use crate::commands::candles::{
    parse_coin_card_candles_response, parse_request_candles_data_response,
    parse_request_candles_data_response_partial_like_delphi, CandlesAggregator, CandlesChunkResult,
    DeepPrice,
};
use crate::commands::engine_api::{
    parse_api_expiration_time_response, parse_auth_check_response, parse_base_check_response,
    parse_engine_response, parse_get_balance_response, parse_query_hedge_mode_response,
    parse_update_transfer_assets_response, ApiExpirationTime, AuthCheckResponse, EngineMethod,
    EngineResponse, ServerInfo, TransferAsset,
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

pub use app_dispatch::{EventFn, EventWithStateFn, OnDataFn};
pub use bps::BpsCounter;
pub use candles::MergedCandles;
pub use clock::set_ntp_offset;
pub use config::{
    AuthStatus, ClientConfig, EngineRequestError, LifecycleEvent, LifecycleFn, RefreshConfig,
    TradeContextError,
};
#[doc(hidden)]
pub use diagnostics::ERR_EMU_RATE;
pub use diagnostics::{
    set_err_emu, ErrEmuCommandDiagnostics, ErrEmuDiagnostics, ErrEmuSlicedBlockDiagnostics,
    ErrEmuSlicedDatagramDiagnostics,
};
pub use init::{
    connect_and_init, run_init_sequence, ConnectConfig, ConnectError, InitConfig, InitError,
    InitResult,
};
pub use metrics::ProtocolMetricsSnapshot;
pub use send_queue::{
    SendPriority, UniqueKey, UK_ARB_PRICES, UK_BALANCE_FULL, UK_BASE_UI_SETTINGS, UK_DEX_SWITCH,
    UK_IMMUNE_CLICKS, UK_LEV_MANAGE_SETTINGS, UK_NONE, UK_ORDER_MOVE, UK_ORDER_STATUS,
    UK_ORDER_STATUS_SHORT, UK_SPOT_SWITCH, UK_STOP_MOVE, UK_STRAT_SELL_PRICE_UPDATE,
    UK_STRAT_SNAPSHOT, UK_TURN_MM_DETECTION,
};
pub use sender::{ClientSender, SubscribeError};
pub use subscriptions::TradesSubscription;

use app_dispatch::{
    metric_api_method, run_dispatcher_worker, wait_dispatcher_worker_barrier, DispatchSink,
    DispatcherEventFn, DispatcherWorkItem, RawAppEvent, RunMode, StateAppEvent,
};
pub(crate) use candles::{EngineResponseMeta, PartialCandles};
pub(crate) use clock::{
    current_utc_hour_slot, delphi_now, delphi_now_raw, get_server_time_delta_global,
    set_server_time_delta_global,
};
use config::{CHECK_TAGS_BURST_COUNT, CHECK_TAGS_BURST_SPACING_MS};
use constants::*;
use diagnostics::{
    diagnostic_duplicate_sliced_acks, err_emu_drop_decision, trace_io_enabled,
    ErrEmuDiagnosticsState,
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

/// Public handle to the client. Allows sending commands from any thread.
pub struct Client {
    cfg: ClientConfig,

    app_queue_alive: Arc<AtomicBool>,
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
    waiting_hello: bool,

    client_token: u64,
    server_token: u64,
    app_token: u64,
    encode_key: MoonKey,
    decode_key: MoonKey,
    /// B-V2-03 fix: кэшированный AES-128-GCM cipher для encode (encrypt direction).
    /// Обновляется одновременно с `encode_key` при handshake. `Aes128Gcm::new` дорогой
    /// (key schedule expansion ~100 байт операций) — раньше делалось на каждый
    /// зашифрованный пакет (тысячи раз/сек). Теперь один раз за сессию.
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

    // BytesPerSec sliding window (10 sec) — observability метрик.
    // B-13 fix: running sum поддерживается одновременно с window — `bytes_per_sec_*` O(1)
    // вместо O(N) обхода каждого запроса.
    // #5 audit_delphi_deviation: O(1) EMA counter (порт Delphi MoonProtoUDPClient.pas:113-138
    // AddBytesCount). VecDeque<(i64,u64)> sliding window удалён — он давал ~8MB heap на
    // burst 50K pps + 100K push_back/pop_front ops/sec. Сейчас 24 байта + 4 ops/add.
    bps_sent: BpsCounter,
    bps_recv: BpsCounter,

    // Log throttle: ключ → последний raise timestamp (anti-spam).
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
    dispatcher_trades_server_token: Arc<AtomicU64>,
    next_port: u16,
    ping_count: u32,

    /// Реестр pending Engine API запросов.
    /// При получении `Command::API` пакета — `dispatch` доставит response
    /// в зарегистрированный receiver, если UID найден.
    api_pending: Arc<ApiPending>,

    /// Lifecycle callback — queued при изменении статуса канала (Connecting → Connected{fresh} → Reconnecting/Disconnected).
    /// Установить через `client.on_lifecycle(cb)`. Опционально.
    lifecycle_cb: Option<LifecycleFn>,
    lifecycle_app_tx: Arc<Mutex<Option<mpsc::Sender<LifecycleEvent>>>>,
    /// Delphi `cfg.MoonProtoConfig.ServerUpdateSent`: set by UI commands that
    /// can make the server restart/change routing; consumed by BaseCheck init.
    server_update_sent: Arc<AtomicBool>,
    /// Предыдущий auth_status (для детектирования переходов).
    prev_auth_status: AuthStatus,

    /// Кэш разрешённого адреса сервера. Закрывает B-05: до этого `server_addr()` форматировал
    /// строку + `send_to(&str)` делал `getaddrinfo` resolve на каждый send (потенциально DNS-блокирующий).
    /// Кэш сбрасывается при ошибке resolve (например, DNS отвалился) — на следующем bind_socket
    /// повторно резолвится.
    cached_server_addr: Option<SocketAddr>,

    /// **Active library — subscription registry**: что app просил подписать.
    /// До Init transport handshake не отправляет этот реестр. После Init reconnect
    /// сам восстанавливает registry через текущие keys / market mapping.
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

    /// Сохранённый intent первого и единственного init-прохода. Нужен для
    /// post-reconnect restore без повторного `run_init_sequence`.
    domain_restore: DomainRestoreIntent,

    /// Был ли когда-нибудь успешный Connected (`Fine` получен ≥1 раз).
    /// Используется в `LifecycleEvent::Connected { fresh }` — `fresh = !was_ever_connected`
    /// при ПЕРВОМ Connected; для всех последующих `fresh = false`.
    was_ever_connected: bool,

    /// Pending candles aggregators по `request_uid`. Заполняется в
    /// `api_request_candles_data_async`, очищается когда aggregator вернул merged
    /// (отправили в Receiver) или истёк timeout.
    ///
    /// **Внутренняя работа** — потребитель API не знает об этом поле, видит только
    /// `mpsc::Receiver<MergedCandles>`.
    pending_candles: HashMap<u64, PartialCandles>,

    /// Прошлый PeerAppToken который был зарегистрирован в `MarketsState.indexes_synchronized = true`.
    /// Используется в handshake/Ping processing для детекции server restart:
    /// если incoming `peer_app_token != tracked_peer_app_token` — помечаем индексы stale.
    /// 0 = ещё не было успешной синхронизации (init состояние).
    tracked_indexes_peer_app_token: u64,

    /// `true` если init/API слой уже отправил markets indexes request и ждёт ответа.
    /// Защита от шторма повторных явных запросов до получения ответа.
    indexes_fetch_in_flight: bool,

    /// После reconnect restore: как только fresh `GetMarketsIndexes` успешно пришёл,
    /// сразу запросить `UpdateMarketsList`. Это повторяет Delphi-смысл
    /// `TMoonProtoEngine.UpdateMarketsList`: при новом `PeerAppToken` он сначала
    /// синхронизирует индексы, затем обновляет prices/funding.
    update_markets_after_indexes: bool,

    /// После reconnect restore: отложенный replay orderbook registry до fresh
    /// `GetMarketsIndexes`. Delphi `CheckBookTopics` выходит, пока
    /// `FLastServerAppToken <> PeerAppToken`; подписки стаканов нельзя replay'ить
    /// до синхронизации индексов новой server app session.
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

    /// Когда (`now_ms`) был отправлен последний `api_get_markets_indexes`. Используется
    /// для timeout protection: UDP-ответ мог потеряться — после `INDEXES_FETCH_TIMEOUT_MS`
    /// сбрасываем `indexes_fetch_in_flight = false`. Сам timeout handler запрос
    /// не переотправляет: новая отправка разрешена только init/API слою.
    indexes_fetch_started_ms: i64,

    /// Когда последний раз вызвали `trades_state.tick()` из main loop (в режиме
    /// `run_with_dispatcher`). Throttle ~100ms — соответствует Delphi
    /// `MoonProtoEngine.pas:1483 CheckMissingTradesPackets` периодичности.
    last_trades_tick_ms: i64,

    /// Сколько раз подряд весь 200-port retry в `bind_socket` упал. На каждой
    /// серии неудач (= один main loop tick где все 200 портов отвергнуты)
    /// инкрементируется; на первом успешном bind сбрасывается в 0. Используется
    /// для эмиссии `LifecycleEvent::BindFailed`. Событие throttled по реальному
    /// elapsed time: первый сигнал после 15с непрерывных неудач, дальше не чаще
    /// одного раза в 50с. См. audit H9.
    bind_failure_streak: u32,
    first_bind_failure_ms: i64,
    last_bind_failed_event_ms: i64,

    /// Guard for the shared process-level NTP syncer (if `cfg.ntp_host = Some`).
    /// Dropping the last guard stops the worker. This matches Delphi's single
    /// `TMoonProtoTymeSyncer` for the process instead of a worker per client.
    _ntp_process_guard: Option<crate::ntp::ProcessNtpGuard>,

    /// F6/F7: timestamps последних periodic refresh-команд. `i64::MIN/2` =
    /// "никогда" → первый tick срабатывает мгновенно после Connected (если в
    /// `cfg.refresh` задан соответствующий интервал). Дальше — каждый
    /// `update_markets_every` / `check_tags_every`.
    last_update_markets_ms: i64,
    last_check_tags_ms: i64,
    /// Delphi `BHeavyApiWorker` делает до 4 быстрых `CheckBinanceTags` после
    /// смены часа. Эти поля хранят текущий wall-clock hour slot и прогресс burst.
    check_tags_hour_slot: i64,
    check_tags_burst_sent: u8,
    last_check_tags_burst_ms: i64,

    /// Identity сервера полученная из `emk_BaseCheck` response. Заполняется в
    /// [`run_init_sequence`] (или может быть выставлена приложением вручную через
    /// [`Client::set_server_info`] если init делается своим pattern'ом). До первого
    /// успешного BaseCheck — `ServerInfo::default()` (все поля `None`,
    /// `has_identity()=false`).
    ///
    /// **Multi-server**: при подключении к нескольким серверам приложение хранит
    /// `Vec<Client>` и различает их по `client.server_info().bot_id`.
    server_info: crate::commands::engine_api::ServerInfo,

    /// Per-account data received from Delphi `TMoonProtoEngine.AuthCheck`.
    ///
    /// Delphi stores `BinanceAccountID`, `BTCAddress`, `AccountID`,
    /// `RecvdMaxPayload`, and Hyperliquid DEX tail in local engine/cfg state
    /// during init. Rust keeps the parsed payload here so active-lib state and
    /// user code can observe the same successful AuthCheck result.
    auth_info: Option<crate::commands::engine_api::AuthCheckResponse>,

    /// Delphi `InitDone`: transport auth уже завершён, но domain-пуши
    /// (`Order`/`Strat`/`Balance`/`Trades*`/`OrderBook`/`UI`) можно применять
    /// только после полного init bootstrap. До этого `dispatch_into_active`
    /// дропает эти каналы, как `TMoonProtoNetClient.ClientNewData`.
    domain_ready: bool,

    /// **Per-Client ServerTimeDelta handle** — shareable через `Arc::clone`.
    ///
    /// Хранит текущий `ServerTimeDelta` (в днях, TDateTime-формат, упакован в u64
    /// через `f64::to_bits`). Обновляется при обработке `MPC_Ping` синхронно с
    /// `self.server_time_delta` и с глобальным `SERVER_TIME_DELTA_DAYS`,
    /// который нужен только raw `EventDispatcher::dispatch_into` без handle.
    ///
    /// **Multi-Client** (DEVIATION #23): `EventDispatcher` должен быть привязан к
    /// этому handle через `EventDispatcher::set_server_time_delta_source(handle)`
    /// или автоматически через `run_with_dispatcher`. Без
    /// привязки EventDispatcher падает обратно на global, что при multi-Client даёт
    /// off-by-50-1000ms timestamps в ордерах (последний Client перезаписывает
    /// delta всех остальных).
    server_time_delta_handle: Arc<std::sync::atomic::AtomicU64>,

    /// Cached MAC context — один раз вычисленные ipad CRC + opad block для `cfg.mac_key`.
    /// Используется в transport_pack/unpack hot-path вместо пересчёта HMAC ipad/opad
    /// (128 XOR + crc32c) на каждом пакете. См. `crate::transport::MacContext`.
    ///
    /// Поскольку `mac_key` фиксирован на всю life Client'а (приходит в
    /// ClientConfig и не меняется) — этот context тоже фиксирован и
    /// переиспользуется receive/send фазами.
    mac_ctx: crate::transport::MacContext,

    /// Reusable buffer для `transport_pack_into_with_mac` — экономит alloc/dealloc на каждый
    /// исходящий пакет. Capacity растёт до peak packet size и переиспользуется.
    /// audit_rust_quality #4: 50K pps × 1500б = 75 MB/s allocator pressure eximinated.
    send_buf: Vec<u8>,
}

impl Client {
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
    /// reconnect machinery start when the client loop runs through [`Self::run`],
    /// [`Self::run_with_dispatcher`], or [`connect_and_init`].
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
        let send_lock = Arc::new(Mutex::new(SendLockState::default()));
        let subscription_summary = Arc::new(SubscriptionRegistrySummary::default());
        let subscription_trades_scope = Arc::new(parking_lot::RwLock::new(None));
        let err_emu_diagnostics = Arc::new(Mutex::new(ErrEmuDiagnosticsState::default()));
        let protocol_metrics = Arc::new(ProtocolMetrics::default());
        let dispatcher_trades_server_token = Arc::new(AtomicU64::new(0));
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

        // Кэшированный MacContext для cfg.mac_key — фиксирован на всю life Client'а.
        // Создание делает 128 XOR + crc32c(ipad_block) единожды; затем `mac()` вызовы
        // только crc32c_append(cached, data) + crc32c_append(prev, opad_block).
        let mac_ctx = crate::transport::MacContext::new(&cfg.mac_key);

        Self {
            cfg,
            app_queue_alive,
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
            waiting_hello: false,
            client_token: rand::random::<u64>(),
            server_token: 0,
            app_token: rand::random(),
            encode_key: [0; 16],
            decode_key: [0; 16],
            encode_cipher: None,
            _start: Instant::now(),
            // NEVER_SENT sentinel = "очень давно". Любое `(cur_tm - NEVER_SENT) > interval`
            // мгновенно true → первый Hello / cleanup / etc выстреливают на первом тике main loop
            // (5мс после bind вместо 2 секунд задержки). Делфи использовал `GetTickCount64`
            // (миллисекунды с boot) ≈ 10^7+ при инициализации `FLastSentHello := 0`, что давало
            // тот же эффект; в Rust `now_ms()` = `Instant::elapsed()` стартует с 0 → нужен явный
            // sentinel. См. delphi_deviation audit #1.
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
            dispatcher_trades_server_token,
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
            auth_info: None,
            domain_ready: false,
            last_update_markets_ms: i64::MIN / 2,
            last_check_tags_ms: i64::MIN / 2,
            check_tags_hour_slot: i64::MIN,
            check_tags_burst_sent: CHECK_TAGS_BURST_COUNT,
            last_check_tags_burst_ms: i64::MIN / 2,
            mac_ctx,
            send_buf: Vec::with_capacity(2048), // типичный send packet ~500-1500 байт
        }
    }

    /// Test-only setter для `server_token` — позволяет имитировать состояние после
    /// успешного handshake без реального сетевого подключения. Используется в
    /// `events.rs` тестах для проверки `dispatch_into_active` token tracking.
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
/// Process-level NTP guard освобождается автоматически после тела `drop`; если
/// это был последний клиент, общий NTP worker остановится.
impl Drop for Client {
    fn drop(&mut self) {
        self.app_queue_alive.store(false, Ordering::Relaxed);
        self.clear_recv_poller();
    }
}

#[cfg(test)]
mod tests;
