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
mod diagnostic_api;
mod diagnostics;
mod domain_commands;
mod engine_api;
mod init;
mod metrics;
mod protocol_core;
mod runtime;
mod send_api;
mod send_queue;
mod sender;
mod socket;
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
use diagnostics::{
    diagnostic_duplicate_sliced_acks, err_emu_drop_decision, trace_io_enabled,
    ErrEmuDiagnosticsState,
};
#[cfg(test)]
use diagnostics::{err_emu_drop_rate_for_cmd, is_service_cmd};
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

#[inline]
fn is_domain_push_command(cmd: Command) -> bool {
    matches!(
        cmd,
        Command::Order
            | Command::Strat
            | Command::Balance
            | Command::TradesStream
            | Command::TradesResendResponse
            | Command::OrderBook
            | Command::UI
    )
}

#[inline]
fn is_trades_stream_command(cmd: Command) -> bool {
    matches!(cmd, Command::TradesStream | Command::TradesResendResponse)
}

#[inline]
fn is_datagram_too_large_error(e: &std::io::Error) -> bool {
    match e.raw_os_error() {
        Some(90) => true,    // Linux EMSGSIZE
        Some(10040) => true, // Windows WSAEMSGSIZE
        Some(40)
            if cfg!(any(
                target_os = "macos",
                target_os = "ios",
                target_os = "freebsd",
                target_os = "openbsd",
                target_os = "netbsd",
            )) =>
        {
            true
        }
        _ => false,
    }
}

#[inline]
fn engine_request_uid(request_payload: &[u8]) -> Option<u64> {
    request_payload
        .get(3..11)
        .and_then(|s| s.try_into().ok())
        .map(u64::from_le_bytes)
}

#[inline]
fn engine_request_method(request_payload: &[u8]) -> Option<EngineMethod> {
    request_payload
        .get(11)
        .copied()
        .map(EngineMethod::from_byte)
}

#[inline]
fn engine_method_allowed_before_domain_ready(method: EngineMethod) -> bool {
    matches!(
        method,
        EngineMethod::BaseCheck
            | EngineMethod::AuthCheck
            | EngineMethod::GetMarketsList
            | EngineMethod::GetMarketsIndexes
            | EngineMethod::UpdateMarketsList
    )
}

#[inline]
fn outgoing_allowed_before_domain_ready(cmd: u8, data: &[u8]) -> bool {
    matches!(
        Command::from_byte(cmd),
        Command::API
            if engine_request_method(data)
                .is_some_and(engine_method_allowed_before_domain_ready)
    )
}

#[inline]
fn timeout_remaining(start: Instant, timeout: Duration) -> Option<Duration> {
    let elapsed = start.elapsed();
    if elapsed >= timeout {
        None
    } else {
        Some(timeout.saturating_sub(elapsed))
    }
}

#[inline]
fn queued_client_settings_updated_since(
    dispatcher: &crate::events::EventDispatcher,
    first_new_event: usize,
) -> bool {
    dispatcher
        .queued_events()
        .get(first_new_event..)
        .unwrap_or(&[])
        .iter()
        .any(|event| {
            matches!(
                event,
                crate::events::Event::Settings(crate::state::SettingsEvent::ClientSettingsUpdated)
            )
        })
}

// === Constants matching Delphi exactly ===
const DEFAULT_SLEEP_MS: u64 = 5; // MoonProtoFunc.pas:19
const DELPHI_SEND_AND_WAIT_POLL_MS: u64 = 10; // MoonProtoEngine.pas:531
const SETTINGS_HELPER_RETRY_PAUSE_MS: u64 = 5_000;
const DELPHI_BASE_CHECK_UPDATE_AUTH_WAITS: usize = 34; // MoonProtoEngine.pas:574
const DELPHI_BASE_CHECK_UPDATE_AUTH_WAIT_MS: u64 = 300; // MoonProtoEngine.pas:575
const DELPHI_BASE_CHECK_UPDATE_RETRIES: usize = 10; // MoonProtoEngine.pas:586
const DELPHI_BASE_CHECK_UPDATE_RETRY_PAUSE_MS: u64 = 2_000; // MoonProtoEngine.pas:589
const DELPHI_INIT_AUTH_RETRY_PAUSE_MS: u64 = 200; // Unit1.pas:5064-5068
const RECONNECT_WAITING_MS: i64 = 7000; // MoonProtoUDPClient.pas:88
const RECONNECT_THROTTLE_MS: i64 = 15000; // MoonProtoUDPClient.pas:89
const OFFLINE_BASE_MS: i64 = 2300; // MoonProtoUDPClient.pas:772
const DEAD_ZONE_MS: i64 = 5000; // MoonProtoUDPClient.pas:799
const NEED_HELLO_AGAIN_THROTTLE_MS: i64 = 700; // MoonProtoUDPClient.pas:568
const COMPRESSED_FLAG: u8 = 0x80; // MoonProtoDataStruct.pas:27
const MIN_SIZE_TO_COMPRESS: usize = 64; // MoonProtoDataStruct.pas:31
const NEVER_SENT_MS: i64 = i64::MIN / 2; // Эквивалент Delphi LastSentHello=0 при uptime-clock
const NEVER_TIME_MS: i64 = i64::MIN / 2;
const NO_PENDING_ENGINE_REQUEST_UID: u64 = u64::MAX;
const BIND_FAILED_FIRST_EVENT_MS: i64 = 15_000;
const BIND_FAILED_REPEAT_EVENT_MS: i64 = 50_000;
const TRADES_RECONNECT_THROTTLE_MS: i64 = 5_000; // MoonProtoEngine.NeedReconnectAllTrades
const TRADES_RECONNECT_RESUBSCRIBE_DELAY_MS: i64 = 100; // BWorks.pas Sleep(100)
const ORDERBOOK_RECONNECT_THROTTLE_MS: i64 = 5_000; // MoonProtoEngine.NeedResubscribeOrderBooks

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
    /// (128 XOR + crc32c) на каждом пакете. См. `moonproto_transport::MacContext`.
    ///
    /// Поскольку `mac_key` фиксирован на всю life Client'а (приходит в
    /// ClientConfig и не меняется) — этот context тоже фиксирован и
    /// переиспользуется receive/send фазами.
    mac_ctx: moonproto_transport::MacContext,

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
        let mac_ctx = moonproto_transport::MacContext::new(&cfg.mac_key);

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

    /// Identity сервера (`bot_id`, `exchange_name`, `base_currency_name`, версии и т.д.).
    /// Заполняется автоматически в [`run_init_sequence`] после успешного `emk_BaseCheck`.
    ///
    /// До первого успешного BaseCheck возвращает дефолт со всеми `None`. Используется
    /// для UI ("подключён к Binance Futures, USDT") и для multi-server идентификации.
    ///
    /// См. [`crate::commands::engine_api::ServerInfo`].
    pub fn server_info(&self) -> &crate::commands::engine_api::ServerInfo {
        &self.server_info
    }

    /// Per-account metadata from the last successful `emk_AuthCheck`.
    ///
    /// Filled automatically by [`run_init_sequence`] and [`Self::request_auth_check`].
    /// Returns `None` before a successful AuthCheck, or if a successful response
    /// had a malformed mandatory AuthCheck payload.
    pub fn auth_info(&self) -> Option<&crate::commands::engine_api::AuthCheckResponse> {
        self.auth_info.as_ref()
    }

    /// Snapshot client-side [`set_err_emu`] counters for live tests.
    ///
    /// This does not affect protocol behavior. FireTest uses it to distinguish
    /// "server did not send", "ErrEmu dropped all retries", and
    /// "Sliced reassembly/parse failed after packets arrived".
    pub fn err_emu_diagnostics_snapshot(&self) -> ErrEmuDiagnostics {
        let configured_rate = ERR_EMU_RATE.load(std::sync::atomic::Ordering::Relaxed);
        self.err_emu_diagnostics
            .lock()
            .unwrap()
            .snapshot(configured_rate)
    }

    /// Clear client-side [`set_err_emu`] counters without changing the loss rate.
    pub fn reset_err_emu_diagnostics(&self) {
        *self.err_emu_diagnostics.lock().unwrap() = ErrEmuDiagnosticsState::default();
    }

    /// Snapshot passive protocol loop metrics.
    ///
    /// These counters are diagnostics only. They never change retry, ACK,
    /// reconnect, queueing, or drop decisions. Use this to prove that
    /// receive-side protocol work and writer send/maintenance phases stay
    /// bounded while auditing Delphi machine-effect parity.
    pub fn protocol_metrics_snapshot(&self) -> ProtocolMetricsSnapshot {
        self.protocol_metrics.snapshot(0)
    }

    /// Snapshot protocol metrics and include the current dispatcher public
    /// event queue length.
    pub fn protocol_metrics_snapshot_with_dispatcher(
        &self,
        dispatcher: &crate::events::EventDispatcher,
    ) -> ProtocolMetricsSnapshot {
        self.protocol_metrics
            .snapshot(dispatcher.queued_event_count())
    }

    /// Установить `ServerInfo` вручную. Обычно не нужно — `run_init_sequence` делает
    /// это автоматически. Полезно если приложение использует свой init pattern
    /// (минуя `run_init_sequence`) и хочет вручную распарсить ответ `api_base_check`.
    pub fn set_server_info(&mut self, info: crate::commands::engine_api::ServerInfo) {
        self.server_info = info;
    }

    /// Set per-account AuthCheck metadata manually for custom init flows.
    pub fn set_auth_info(&mut self, info: crate::commands::engine_api::AuthCheckResponse) {
        self.auth_info = Some(info);
    }

    /// Build a trade command context from the active server route.
    ///
    /// This is the recommended path for market-level trade commands such as
    /// [`Self::new_order`], [`Self::move_all_sells`], or position close/split
    /// commands. It uses `ServerInfo::base_currency_code` and
    /// `ServerInfo::exchange_code`, which are filled by `connect_and_init` /
    /// `run_init_sequence`, or by [`Self::request_base_check`].
    ///
    /// Existing-order actions should usually use the `*_tracked_order` wrappers
    /// instead, because they derive the route and current status from
    /// `EventDispatcher::orders()` state.
    pub fn trade_ctx(
        &self,
        uid: u64,
    ) -> Result<crate::commands::trade::TradeCtx, TradeContextError> {
        match (
            self.server_info.base_currency_code,
            self.server_info.exchange_code,
        ) {
            (Some(currency), Some(platform)) => Ok(crate::commands::trade::TradeCtx::with_route(
                uid, currency, platform,
            )),
            _ => Err(TradeContextError::from_server_info(&self.server_info)
                .expect("route fields are missing")),
        }
    }

    /// Build a session-derived trade context with a random command UID.
    ///
    /// Use this for client-originated market commands where the UID only needs to
    /// be unique for the outgoing command. For actions on an existing order,
    /// prefer tracked-order wrappers because their UID must be the server order
    /// task id.
    pub fn random_trade_ctx(&self) -> Result<crate::commands::trade::TradeCtx, TradeContextError> {
        self.trade_ctx(rand::random())
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

    /// Shareable handle на `ServerTimeDelta` этого клиента (days, f64 в u64-bits).
    ///
    /// Используется для линковки с `EventDispatcher` в multi-Client архитектуре:
    /// ```ignore
    /// let mut dispatcher = EventDispatcher::new();
    /// dispatcher.set_server_time_delta_source(client.server_time_delta_handle());
    /// ```
    ///
    /// Если использовать `Client::run_with_dispatcher` — линковка делается
    /// автоматически на первом active-dispatch шаге.
    ///
    /// См. `DEVIATION.md #23` (single-Client → multi-Client refactor).
    pub fn server_time_delta_handle(&self) -> Arc<std::sync::atomic::AtomicU64> {
        Arc::clone(&self.server_time_delta_handle)
    }

    /// Установить lifecycle callback.
    ///
    /// During `run*` calls the callback is queued outside the protocol writer
    /// tick, matching Delphi `TThread.Queue` for status notifications.
    pub fn on_lifecycle(&mut self, cb: LifecycleFn) {
        self.lifecycle_cb = Some(cb);
    }

    /// Mark Delphi `ServerUpdateSent`.
    ///
    /// UI wrappers that can trigger a server-side restart/routing update
    /// (`ui_update_version`, `ui_switch_dex`, `ui_switch_spot`) call this
    /// automatically. Use it only when sending the same raw UI commands through
    /// lower-level APIs: the next `run_init_sequence` consumes the flag and runs
    /// the Delphi BaseCheck retry path.
    pub fn mark_server_update_sent(&self) {
        self.server_update_sent.store(true, Ordering::Relaxed);
    }

    /// Whether a Delphi-style server update marker is pending.
    pub fn server_update_sent(&self) -> bool {
        self.server_update_sent.load(Ordering::Relaxed)
    }

    fn take_server_update_sent(&self) -> bool {
        self.server_update_sent.swap(false, Ordering::Relaxed)
    }

    /// Внутренний хук: вызывает callback на переход состояния.
    /// Должен вызываться там где меняется `self.auth_status` или `self.need_connect`.
    fn fire_lifecycle(&mut self, ev: LifecycleEvent) {
        let tx = self.lifecycle_app_tx.lock().unwrap().clone();
        if let Some(tx) = tx {
            let _ = tx.send(ev);
            return;
        }
        if let Some(ref mut cb) = self.lifecycle_cb {
            cb(ev);
        }
    }

    /// Проверить изменение auth_status и эмитировать соответствующий lifecycle event.
    /// Вызывается из main loop после каждого пакета.
    fn check_lifecycle_transition(&mut self) {
        if self.auth_status == self.prev_auth_status {
            return;
        }
        let new_ev = match (self.prev_auth_status, self.auth_status) {
            // Первичное подключение (cold start или после Disconnect)
            (AuthStatus::Base, AuthStatus::Connected) => Some(LifecycleEvent::Connecting),
            // Re-handshake после потери связи (soft reconnect) — Offline → Connected
            (AuthStatus::Offline, AuthStatus::Connected) => Some(LifecycleEvent::Connecting),
            // Успешная авторизация (Fine received) — `fresh = true` только для первого
            // в жизни Connected. После was_ever_connected становится true и все
            // последующие re-handshake'и шлют `fresh = false`.
            (_, AuthStatus::AuthDone) if self.prev_auth_status != AuthStatus::AuthDone => {
                let fresh = !self.was_ever_connected;
                self.was_ever_connected = true;
                Some(LifecycleEvent::Connected { fresh })
            }
            // Потеря связи
            (AuthStatus::AuthDone, AuthStatus::Offline) => Some(LifecycleEvent::Reconnecting),
            // Disconnect от потребителя (явный)
            (_, AuthStatus::Base)
                if self.prev_auth_status != AuthStatus::Base && !self.need_connect =>
            {
                Some(LifecycleEvent::Disconnected)
            }
            _ => None,
        };
        self.prev_auth_status = self.auth_status;
        if let Some(ev) = new_ev {
            self.fire_lifecycle(ev);
        }
    }

    fn parse_sliced_ack_payload(payload: &[u8]) -> Option<SlicedAck> {
        // Delphi OnNewSlicedACK reads Flags(32 bytes) + DatagramNum(2 bytes)
        // from the command payload after the transport header.
        let (flags, datagram_num) = slicing::parse_ack_bytes(payload)?;
        Some(SlicedAck {
            flags,
            datagram_num,
        })
    }

    fn push_sliced_ack_payload(send_lock: &Arc<Mutex<SendLockState>>, payload: &[u8]) {
        if let Some(ack) = Self::parse_sliced_ack_payload(payload) {
            send_lock.lock().unwrap().push_sliced_ack(ack);
        }
    }

    fn decode_handshake_hello(
        master_key: &MoonKey,
        client_id: u64,
        payload: &[u8],
    ) -> Option<handshake::Hello> {
        let aad = client_id.to_le_bytes();
        let decrypted = crypto::decrypt(master_key, payload, &aad)?;
        handshake::Hello::from_bytes(&decrypted)
    }

    fn build_size_ack_payload(
        data_read_state: &mut DataReadState,
        payload: &[u8],
    ) -> Option<Vec<u8>> {
        let size_test = control::SizeTestData::read(payload)?;
        let size = size_test.size;
        if (size as usize) < 6 {
            return None;
        }
        let series = data_read_state.update_data_size_ack_series_num(size_test.series_num);
        Some(control::SizeTestData::ack_bytes(size, series))
    }

    fn build_probe_mtu_ack_payload(payload: &[u8]) -> Option<Vec<u8>> {
        let probe = control::ProbeMtu::read(payload)?;
        if (probe.test_size as usize) < control::PROBE_MTU_ACK_SIZE {
            return None;
        }
        Some(probe.ack_bytes())
    }

    fn apply_ping_and_build_response(
        &mut self,
        payload: &[u8],
        raw_now_dt: f64,
        corrected_now_dt: f64,
        total_sent_before_ping: u64,
        total_recv_after_packet: u64,
    ) -> Option<Vec<u8>> {
        let ping = control::PingFrame::read(payload)?;

        // UDPRead Ping branch: update transport ping fields before DataRead.
        let rs = ping.rs();
        const COMFORTABLE_RS: f64 = 0.92;
        const CRITICAL_RS: f64 = 0.85;
        const MIN_RATE: i32 = 256 * 1024;
        const MAX_RATE: i32 = 8 * 1024 * 1024;
        self.round_trip_delay = ping.trip_delay as i64;
        self.actual_pmtu = ping.pmtu;
        self.overheat = ping.overheat;
        self.rs = rs;
        // A server can start sending Ping after it created its side of the
        // client, even if the final MPC_Fine was lost on the way back. Ping
        // proves the peer is alive, but it does not complete authorization.
        // Keep the connect loop alive until AuthDone, otherwise a single lost
        // Fine can leave the client permanently Connected-but-not-authorized.
        if self.auth_status == AuthStatus::AuthDone {
            self.need_connect = false;
        }
        if self.used_sliced_limit {
            let new_rate = if rs > COMFORTABLE_RS {
                let increase = (self.can_send_rate as f64 * 0.03).round() as i32;
                self.can_send_rate + increase.max(32 * 1024)
            } else if rs < CRITICAL_RS {
                (self.can_send_rate as f64 * 0.85).round() as i32
            } else {
                let drift = (rs - COMFORTABLE_RS) / COMFORTABLE_RS;
                (self.can_send_rate as f64 * (1.0 + drift * 0.05)).round() as i32
            };
            self.can_send_rate = new_rate.clamp(MIN_RATE, MAX_RATE);
            self.used_sliced_limit = false;
        }

        // DataReadInt(MPC_Ping): write server ACK bitmap into TmpSlider.
        self.send_lock
            .lock()
            .unwrap()
            .apply_ping_ack_bitmap(payload);

        // ClientNewData(MPC_Ping): update wall-clock deltas before SendPing.
        self.ping_count = self.ping_count.wrapping_add(1);
        self.global_timing_orders = ping.global_timing_orders;
        let initial_time = ping.initial_time;
        let server_time = ping.time;
        let server_time_delta = initial_time - raw_now_dt;
        self.server_time_delta = server_time_delta;
        self.server_time_delta_handle.store(
            server_time_delta.to_bits(),
            std::sync::atomic::Ordering::Relaxed,
        );
        set_server_time_delta_global(server_time_delta);
        self.net_lag_ping = ((corrected_now_dt - server_time) * 86400000.0).abs() as i64;

        // SendPing(var APing): mutate the same Ping struct, then append our ACK half.
        let (ack_start, ack_words) = self.data_read_state.build_ack_half();
        let mut response = ping.response_bytes(
            corrected_now_dt,
            total_sent_before_ping,
            total_recv_after_packet,
            ack_start,
        );
        for word in &ack_words {
            response.extend_from_slice(&word.to_le_bytes());
        }

        Some(response)
    }

    #[cfg(test)]
    fn on_new_sliced_ack(&self, payload: &[u8]) {
        Self::push_sliced_ack_payload(&self.send_lock, payload);
    }

    fn apply_sliced_ack(&mut self, ack: SlicedAck, _now_ms: i64) {
        // Matches TMoonProtoClient.ApplyACK (MoonProtoIntStruct.pas:1200-1218):
        // find first matching Sending datagram, apply, maybe remove, then stop.
        let mut completed_ratio = None;
        let mut completed_idx = None;
        if let Some(idx) = self
            .sending
            .iter()
            .position(|s| s.datagram_num == ack.datagram_num)
        {
            let s = &mut self.sending[idx];
            // Merge ACK flags (set union, like Delphi Flags := Flags + ACK.Flags).
            // If no new flag appears, Delphi `ApplyACK` exits before touching
            // the piece list; keep the same no-op machine effect.
            let mut changed = false;
            for (dst, src) in s.ack_flags.iter_mut().zip(ack.flags) {
                let before = *dst;
                *dst |= src;
                changed |= before != *dst;
            }
            if changed {
                // Delphi server/client fix: ACK progress proves the peer is
                // alive, so the datagram retry budget starts over.
                s.retry_count = 0;
                let complete = (0..s.blocks_count).all(|block| s.is_block_acked(block));
                if complete {
                    if s.blocks_count > 0 {
                        completed_ratio =
                            Some((s.sent_count as f64 / s.blocks_count as f64 - 1.0) * 100.0);
                    }
                    if trace_io_enabled() {
                        eprintln!(
                            "[mp-sliced-ack] d={} acked={}/{} complete=true sent_count={}",
                            s.datagram_num, s.blocks_count, s.blocks_count, s.sent_count
                        );
                    }
                    completed_idx = Some(idx);
                } else {
                    // Current Delphi keeps the retry clocks of remaining holes:
                    // ACK-progress only removes ACKed pieces and resets FRetryCount.
                    // Rust keeps arrays indexed by block number, so recompute the
                    // datagram clock from unACKed blocks instead of zeroing them.
                    s.refresh_last_checked_from_unacked(_now_ms);
                    if trace_io_enabled() {
                        let acked = (0..s.blocks_count)
                            .filter(|&block| s.is_block_acked(block))
                            .count();
                        eprintln!(
                            "[mp-sliced-ack] d={} acked={}/{} complete=false last_checked={}",
                            s.datagram_num, acked, s.blocks_count, s.last_checked
                        );
                    }
                }
            }
        }

        if let Some(idx) = completed_idx {
            self.sending.remove(idx);
        }

        if let Some(ratio) = completed_ratio {
            self.avg_over_heat = if self.avg_over_heat == 0.0 {
                ratio
            } else {
                (self.avg_over_heat * 9.0 + ratio) * 0.1
            };
        }
    }

    fn decode_data_read_int_payload_shared(
        data_read_state: &mut DataReadState,
        raw_cmd: u8,
        data: &[u8],
    ) -> Option<(u8, Vec<u8>)> {
        // B-V2-01 fix: используем Cow вместо безусловного data.to_vec(). Большинство
        // пакетов не Crypted и не Compressed (Ping, handshake, Sliced-блоки) — для них
        // payload остаётся borrowed (zero alloc). Crypted и Compressed создают Owned
        // только когда реально нужны. На пике TradesStream это устраняет 50K alloc'ов/сек.
        use std::borrow::Cow;
        let mut cmd = raw_cmd;
        let mut payload: Cow<'_, [u8]> = Cow::Borrowed(data);

        if Command::from_byte(cmd & 0x7F) == Command::Crypted {
            // B-V2-03: используем кэшированный cipher вместо ключа. До handshake
            // (cipher = None) Crypted-пакетов и быть не должно — но защищаемся return.
            let DataReadState {
                decode_cipher,
                slider,
                ..
            } = data_read_state;
            let decode_cipher = decode_cipher.as_ref()?;
            let decrypted = crypted::decrypt_command(decode_cipher, &payload, slider);
            if let Some((inner_cmd, decrypted, _want_ack)) = decrypted {
                cmd = inner_cmd;
                payload = Cow::Owned(decrypted);
            } else {
                return None;
            }
        }

        if cmd & COMPRESSED_FLAG != 0 {
            cmd &= 0x7F;
            if let Some(decompressed) = compression::mp_decompress(&payload) {
                payload = Cow::Owned(decompressed);
            }
        }

        // MPC_Ping is handled in the reader Ping path. Its server ACK bitmap follows the
        // Delphi TmpSlider -> RecvdSlider -> ApplyRegularHLAck path, not this
        // generic delivery branch.
        Some((cmd, payload.into_owned()))
    }

    fn decode_data_read_int_payload_owned(
        data_read_state: &mut DataReadState,
        raw_cmd: u8,
        data: Vec<u8>,
    ) -> Option<(u8, Vec<u8>)> {
        let mut cmd = raw_cmd;
        let mut payload = data;

        if Command::from_byte(cmd & 0x7F) == Command::Crypted {
            let DataReadState {
                decode_cipher,
                slider,
                ..
            } = data_read_state;
            let decode_cipher = decode_cipher.as_ref()?;
            let decrypted = crypted::decrypt_command(decode_cipher, &payload, slider);
            if let Some((inner_cmd, decrypted, _want_ack)) = decrypted {
                cmd = inner_cmd;
                payload = decrypted;
            } else {
                return None;
            }
        }

        if cmd & COMPRESSED_FLAG != 0 {
            cmd &= 0x7F;
            if let Some(decompressed) = compression::mp_decompress(&payload) {
                payload = decompressed;
            }
        }

        Some((cmd, payload))
    }

    fn engine_response_request_uid_from_payload(payload: &[u8]) -> Option<u64> {
        // Engine response payload includes 11-byte TBaseCommand header, then
        // RequestUID. This is enough to cheaply check ApiPending without
        // inflating a full response in the receive phase.
        let uid = payload.get(11..19)?;
        Some(u64::from_le_bytes(uid.try_into().unwrap()))
    }

    fn engine_response_meta_from_payload(payload: &[u8]) -> Option<EngineResponseMeta> {
        if payload.len() < 11 {
            return None;
        }
        let mut pos = 11usize;
        let request_uid = u64::from_le_bytes(payload.get(pos..pos + 8)?.try_into().ok()?);
        pos += 8;
        let method = EngineMethod::from_byte(*payload.get(pos)?);
        pos += 1;
        let success = *payload.get(pos)? != 0;
        pos += 1;
        // ErrorCode.
        payload.get(pos..pos + 4)?;
        pos += 4;
        // ErrorMsg string, length-prefixed UTF-8. Skip only; no allocation.
        let len = u16::from_le_bytes(payload.get(pos..pos + 2)?.try_into().ok()?) as usize;
        pos += 2;
        payload.get(pos..pos + len)?;
        Some(EngineResponseMeta {
            request_uid,
            method,
            success,
        })
    }

    fn engine_response_method_from_payload(payload: &[u8]) -> Option<EngineMethod> {
        payload.get(19).copied().map(EngineMethod::from_byte)
    }

    fn apply_engine_response_client_bookkeeping(&mut self, resp: &EngineResponse) {
        // Active library: auto-clear indexes_fetch_in_flight на ответе
        // GetMarketsIndexes (любой — даже неуспешный, чтобы не зависнуть навсегда).
        if resp.method == EngineMethod::GetMarketsIndexes {
            self.indexes_fetch_in_flight = false;
            let indexes_payload_ok = resp.success
                && crate::commands::market::parse_markets_indexes_response(&resp.data).is_some();
            if indexes_payload_ok {
                // Запоминаем что для текущего PeerAppToken индексы получены.
                self.tracked_indexes_peer_app_token = self.peer_app_token;
                if self.update_markets_after_indexes {
                    self.update_markets_after_indexes = false;
                    self.send_api_request(&crate::commands::engine_request::update_markets_list());
                }
                if self.restore_orderbooks_after_indexes {
                    self.restore_orderbooks_after_indexes = false;
                    self.restore_orderbook_subscriptions_from_registry();
                }
            }
        }

        // Delphi `DoSubscribeOrderBooks`: только успешный ответ подтверждает
        // текущий `ServerToken`. Для reconnect batch это полный `BookSubbed`
        // replay; обычная точечная подписка может выставить token только в
        // initial state, как Delphi `FSubscribedBookServerToken = 0`.
        if resp.method == EngineMethod::SubscribeOrderBook {
            let is_reconnect_batch =
                self.pending_orderbook_resubscribe_uid == Some(resp.request_uid);
            if resp.success && (self.subscribed_book_server_token == 0 || is_reconnect_batch) {
                self.subscribed_book_server_token = self.server_token;
            }
            self.close_orderbook_subscribe_wait_if_matches(resp.request_uid);
            if is_reconnect_batch {
                self.pending_orderbook_resubscribe_uid = None;
            }
        }

        // Delphi `TMoonProtoEngine.SubscribeAllTrades`: successful
        // `emk_SubscribeAllTrades` refreshes `LastReconnectCheck`.
        // Until the first TradesStream packet updates `FTradesServerToken`,
        // this 5s gate prevents immediate unsubscribe/resubscribe churn.
        if resp.method == EngineMethod::SubscribeAllTrades && resp.success {
            let now_ms = self.now_ms();
            self.last_trades_reconnect_check_ms = now_ms;
        }
        if resp.method == EngineMethod::SubscribeAllTrades {
            self.last_trades_subscribe_request_ms
                .store(NEVER_TIME_MS, Ordering::Relaxed);
        }
        if resp.method == EngineMethod::UnsubscribeAllTrades {
            self.close_trades_unsubscribe_wait_if_matches(resp.request_uid);
        }
    }

    fn apply_engine_response_meta_bookkeeping(&mut self, meta: EngineResponseMeta) {
        if meta.method == EngineMethod::SubscribeOrderBook {
            let is_reconnect_batch =
                self.pending_orderbook_resubscribe_uid == Some(meta.request_uid);
            if meta.success && (self.subscribed_book_server_token == 0 || is_reconnect_batch) {
                self.subscribed_book_server_token = self.server_token;
            }
            self.close_orderbook_subscribe_wait_if_matches(meta.request_uid);
            if is_reconnect_batch {
                self.pending_orderbook_resubscribe_uid = None;
            }
        }

        if meta.method == EngineMethod::SubscribeAllTrades && meta.success {
            let now_ms = self.now_ms();
            self.last_trades_reconnect_check_ms = now_ms;
        }
        if meta.method == EngineMethod::SubscribeAllTrades {
            self.last_trades_subscribe_request_ms
                .store(NEVER_TIME_MS, Ordering::Relaxed);
        }
        if meta.method == EngineMethod::UnsubscribeAllTrades {
            self.close_trades_unsubscribe_wait_if_matches(meta.request_uid);
        }
    }

    fn process_api_bookkeeping_light(&mut self, payload: &[u8]) {
        let Some(meta) = Self::engine_response_meta_from_payload(payload) else {
            return;
        };
        if meta.method == EngineMethod::GetMarketsIndexes {
            if let Some(resp) = parse_engine_response(payload) {
                self.apply_engine_response_client_bookkeeping(&resp);
            }
        } else {
            self.apply_engine_response_meta_bookkeeping(meta);
        }
    }

    fn dispatch_api_pending_inline(api_pending: &ApiPending, cmd: u8, payload: &[u8]) -> bool {
        if cmd != Command::API.to_byte() {
            return false;
        }
        let Some(uid) = Self::engine_response_request_uid_from_payload(payload) else {
            return false;
        };
        if !api_pending.contains(uid) {
            return false;
        }
        let Some(resp) = parse_engine_response(payload) else {
            return false;
        };
        api_pending.dispatch(resp).is_none()
    }

    fn dispatch_candles_chunk_inline(
        pending_candles: &mut HashMap<u64, PartialCandles>,
        cmd: u8,
        payload: &[u8],
        now_ms: i64,
    ) -> bool {
        if cmd != Command::API.to_byte() {
            return false;
        }
        if Self::engine_response_method_from_payload(payload)
            != Some(EngineMethod::RequestCandlesData)
        {
            return false;
        }
        let Some(uid) = Self::engine_response_request_uid_from_payload(payload) else {
            return false;
        };
        if !pending_candles.contains_key(&uid) {
            return false;
        }
        let Some(resp) = parse_engine_response(payload) else {
            return false;
        };
        Self::handle_candles_chunk_in_map(pending_candles, &resp, now_ms)
    }

    fn client_new_data_decoded(
        &mut self,
        cmd: u8,
        payload: Vec<u8>,
        api_pending_consumed_by_reader: bool,
        candles_chunk_consumed_by_reader: bool,
        sink: &mut DispatchSink<'_>,
    ) {
        if cmd == Command::API.to_byte() {
            match self.process_api_command_decoded(
                payload,
                api_pending_consumed_by_reader,
                candles_chunk_consumed_by_reader,
                sink,
            ) {
                Ok(()) => {
                    return;
                }
                Err(payload) => {
                    sink.deliver_owned(Command::from_byte(cmd), payload);
                    return;
                }
            }
        }

        sink.deliver_owned(Command::from_byte(cmd), payload);
    }

    fn process_api_command_decoded(
        &mut self,
        payload: Vec<u8>,
        api_pending_consumed_by_reader: bool,
        candles_chunk_consumed_by_reader: bool,
        sink: &mut DispatchSink<'_>,
    ) -> Result<(), Vec<u8>> {
        // Engine API responses: попытаться доставить в pending registry / chunked
        // candles aggregator / internal recovery flags. Если UID не зарегистрирован —
        // пробрасываем как обычный data callback.
        if candles_chunk_consumed_by_reader {
            return Ok(());
        }
        if let Some(resp) = parse_engine_response(&payload) {
            // 1. Chunked candles (RequestCandlesData) — aggregator поддерживает
            // несколько response пакетов с одинаковым UID. До завершения сборки
            // не дропаем slot.
            let now_ms = self.now_ms();
            if resp.method == EngineMethod::RequestCandlesData
                && Self::handle_candles_chunk_in_map(&mut self.pending_candles, &resp, now_ms)
            {
                // Чанк потреблён aggregator'ом. Передаём в on_data только
                // если потребитель НЕ использует async API (тогда тут merged
                // ещё не готов — пусть приложение видит сырые chunks).
                // Однако: чтобы не путать — пропускаем on_data callback.
                // Async-потребитель получит результат через Receiver<MergedCandles>.
                return Ok(());
            }
            // Если slot не зарегистрирован — fallback на pending registry /
            // on_data для fire-and-forget API users.

            self.apply_engine_response_client_bookkeeping(&resp);

            // 2. Pending registry (обычный async API).
            let pending_consumed =
                api_pending_consumed_by_reader || self.api_pending.dispatch(resp).is_none();
            if !pending_consumed || sink.is_buffer() {
                // Если response не ждал конкретный receiver — это обычный API event.
                // Если ждал, но мы в Dispatcher mode, всё равно отдаём raw payload
                // dispatcher'у: active state (markets/indexes/tags) должен обновиться
                // независимо от того, ждёт ли user code этот же ответ через Receiver.
                // Callback mode сохраняет семантику: pending response не
                // дублируется в on_data callback.
                sink.deliver_owned(Command::API, payload);
            }
            return Ok(());
        }
        // Не распарсилось — fallback на raw sink.
        Err(payload)
    }

    /// Поглотить candles chunk через pending aggregator. Возвращает `true` если slot
    /// найден и chunk обработан (даже если merged ещё не готов — копить дальше);
    /// `false` если UID не зарегистрирован (потребитель не использует async API).
    ///
    /// Когда aggregator вернул merged — sender'у отправляется готовый `MergedCandles`,
    /// slot удаляется. Если sender уже дропнут (receiver не ждёт) — slot всё равно
    /// удаляется (semantic = "fire-and-forget с финализацией").
    fn handle_candles_chunk_in_map(
        pending_candles: &mut HashMap<u64, PartialCandles>,
        resp: &EngineResponse,
        _now_ms: i64,
    ) -> bool {
        // Проверяем slot отдельным lookup — потом полное удаление через remove() если merged.
        if !resp.success {
            if let Some(partial) = pending_candles.remove(&resp.request_uid) {
                log::warn!(target: "moonproto::client",
                    "candles request uid={} failed code={} msg={}",
                    resp.request_uid, resp.error_code, resp.error_msg);
                drop(partial);
                return true;
            }
            return false;
        }

        let uid = resp.request_uid;
        let chunk_result = {
            let Some(partial) = pending_candles.get_mut(&uid) else {
                return false;
            };
            let chunk_result = partial.aggregator.on_chunk_result(&resp.data);
            if matches!(
                chunk_result,
                CandlesChunkResult::Stored | CandlesChunkResult::Complete(_)
            ) {
                // Delphi updates `Markets.LastChunkTime` for the UI waiting
                // thread, but does not cancel the protocol-side collector on
                // that timeout. Rust keeps the pending slot until explicit
                // complete/error/reset/caller timeout.
            }
            chunk_result
        };
        if let CandlesChunkResult::Complete(zipped_data) = chunk_result {
            let markets = parse_request_candles_data_response(&zipped_data).unwrap_or_else(|| {
                log::warn!(target: "moonproto::client",
                    "candles aggregator merged but strict parse failed for uid={} ({} bytes); trying Delphi partial apply",
                    uid,
                    zipped_data.len()
                );
                parse_request_candles_data_response_partial_like_delphi(&zipped_data)
                    .unwrap_or_default()
            });
            if let Some(partial) = pending_candles.remove(&uid) {
                let _ = partial.sender.send(MergedCandles {
                    uid,
                    zipped_data,
                    markets,
                });
                // Sender дропается → receiver получает Ok(...) / уже получил.
            }
        }
        true
    }

    /// Auto-compress payload если `cmd` ещё не помечен `COMPRESSED_FLAG`, размер > 64 байт
    /// и `mp_compress` дал savings ≥ 5% (`mp_compress` сам возвращает None если меньше).
    /// Соответствует Delphi `TMoonProtoDataToSend.Create` (MoonProtoIntStruct.pas:661-672).
    ///
    /// Аудит #3 (audit_delphi_deviation): возвращает `Cow<'_, [u8]>` вместо `Vec<u8>`.
    /// Раньше делали безусловный `data.to_vec()` даже когда компрессия не применялась —
    /// 1 alloc на каждый отправляемый H/L/Sliced пакет. В Delphi `TMemoryStream` передаётся
    /// по ссылке, ноль копий. Теперь `Cow::Borrowed` когда без сжатия → zero alloc.
    fn maybe_compress<'a>(cmd: u8, data: &'a [u8]) -> (u8, std::borrow::Cow<'a, [u8]>) {
        if cmd & COMPRESSED_FLAG == 0 && data.len() > MIN_SIZE_TO_COMPRESS {
            if let Some(compressed) = compression::mp_compress(data) {
                return (cmd | COMPRESSED_FLAG, std::borrow::Cow::Owned(compressed));
            }
        }
        (cmd, std::borrow::Cow::Borrowed(data))
    }

    fn crypted_wire_cmd(inner_cmd: u8) -> u8 {
        if inner_cmd & COMPRESSED_FLAG != 0 {
            Command::Crypted.to_byte() | COMPRESSED_FLAG
        } else {
            Command::Crypted.to_byte()
        }
    }

    fn send_raw_packet_cmd(&mut self, cmd: u8, payload: &[u8]) {
        let Some(addr) = self.server_socket_addr() else {
            return;
        };
        // Zero-alloc fast path: reuse self.send_buf + cached MacContext.
        let extra = moonproto_transport::transport_pack_into_with_mac(
            &mut self.send_buf,
            &self.mac_ctx,
            &self.cfg.mac_key,
            cmd,
            self.cfg.client_id,
            payload,
            self.cfg.mask_ver,
        );
        // Извлекаем packet чтобы borrow checker не ругался на двойной &mut self
        // (dispatch_send берёт &mut self, ему не нужен send_buf после copy в socket).
        // Из send_buf берём slice — оно живёт в self, socket.send_to не сохранит ссылку.
        // SAFETY pattern: take/restore чтобы &mut self в dispatch_send не пересекался с
        // &self.send_buf — но проще: pass slice через owned vec swap.
        let packet = std::mem::take(&mut self.send_buf);
        self.dispatch_send(cmd, &packet, extra.as_deref(), addr);
        // Возвращаем буфер обратно (capacity сохранился, content сейчас не нужен).
        self.send_buf = packet;
        self.send_buf.clear();
    }

    fn send_raw_packet(&mut self, cmd: Command, payload: &[u8]) {
        let Some(addr) = self.server_socket_addr() else {
            return;
        };
        let extra = moonproto_transport::transport_pack_into_with_mac(
            &mut self.send_buf,
            &self.mac_ctx,
            &self.cfg.mac_key,
            cmd.to_byte(),
            self.cfg.client_id,
            payload,
            self.cfg.mask_ver,
        );
        let packet = std::mem::take(&mut self.send_buf);
        self.dispatch_send(cmd.to_byte(), &packet, extra.as_deref(), addr);
        self.send_buf = packet;
        self.send_buf.clear();
    }

    /// Реально отправляет пакет (плюс optional extra-пакет от moonext) с обработкой ошибок.
    /// Закрывает D-06: send errors больше не игнорируются через `.ok()`.
    /// EWOULDBLOCK логируется как warn (нормальная буферизация ядра). Прочие ошибки логируются,
    /// но не меняют reconnect-state: Delphi `DoSendPacket` возвращает false и не ставит
    /// `ForceDisconnect`.
    fn dispatch_send(&mut self, cmd: u8, packet: &[u8], extra: Option<&[u8]>, addr: SocketAddr) {
        if self.debug_outgoing_blackhole.load(Ordering::Relaxed) {
            self.err_emu_diagnostics
                .lock()
                .unwrap()
                .record_outgoing(cmd, true);
            if trace_io_enabled() {
                eprintln!(
                    "[mp-io-tx-blackhole] cmd={:?} raw={} packet_len={} extra_len={} addr={}",
                    Command::from_byte(cmd),
                    cmd,
                    packet.len(),
                    extra.map(|p| p.len()).unwrap_or(0),
                    addr
                );
            }
            return;
        }

        if trace_io_enabled() {
            eprintln!(
                "[mp-io-tx-attempt] cmd={:?} raw={} packet_len={} extra_len={} addr={}",
                Command::from_byte(cmd),
                cmd,
                packet.len(),
                extra.map(|p| p.len()).unwrap_or(0),
                addr
            );
        }
        // Сначала выполняем сетевые операции, собирая Result'ы в owned-переменные,
        // потом обрабатываем через self.should_log без conflicting borrow.
        let extra_result = match (extra, self.socket.as_ref()) {
            (Some(extra_pkt), Some(sock)) => Some(sock.send_to(extra_pkt, addr)),
            _ => None,
        };
        let main_result = match self.socket.as_ref() {
            Some(sock) => sock.send_to(packet, addr),
            None => return,
        };

        if let Some(Err(e)) = extra_result {
            if self.should_log("send_extra_err", 1000) {
                warn!("send_to(extra, cmd={cmd}) failed: {e}");
            }
        }
        match main_result {
            Ok(_) => {
                self.err_emu_diagnostics
                    .lock()
                    .unwrap()
                    .record_outgoing(cmd, false);
                let total_sent = self
                    .total_sent
                    .fetch_add(packet.len() as u64, Ordering::Relaxed)
                    + packet.len() as u64;
                self.track_sent(packet.len() as u64, self.now_ms());
                if trace_io_enabled() {
                    eprintln!(
                        "[mp-io-tx-ok] cmd={:?} raw={} packet_len={} total_sent={}",
                        Command::from_byte(cmd),
                        cmd,
                        packet.len(),
                        total_sent
                    );
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if trace_io_enabled() {
                    eprintln!(
                        "[mp-io-tx-wouldblock] cmd={:?} raw={} packet_len={} err={}",
                        Command::from_byte(cmd),
                        cmd,
                        packet.len(),
                        e
                    );
                }
                if self.should_log("send_wouldblock", 1000) {
                    warn!("send_to(cmd={cmd}) would block (kernel send buffer full)");
                }
            }
            Err(e) if is_datagram_too_large_error(&e) => {
                if trace_io_enabled() {
                    eprintln!(
                        "[mp-io-tx-too-large] cmd={:?} raw={} packet_len={} err={}",
                        Command::from_byte(cmd),
                        cmd,
                        packet.len(),
                        e
                    );
                }
                if self.should_log("send_too_large", 1000) {
                    warn!("send_to(cmd={cmd}) packet too large for current path MTU: {e}");
                }
            }
            Err(e) => {
                if trace_io_enabled() {
                    eprintln!(
                        "[mp-io-tx-error] cmd={:?} raw={} packet_len={} err={}",
                        Command::from_byte(cmd),
                        cmd,
                        packet.len(),
                        e
                    );
                }
                if self.should_log("send_err", 1000) {
                    error!("send_to(cmd={cmd}) failed: {e}");
                }
            }
        }
    }

    /// Matches TMoonProtoClient.Reset (IntStruct.pas:972-1000)
    /// Does NOT reset: server_token, actual_pmtu, send_datagram_num, pending_h,
    /// sending, api_pending, pending_candles, trip_delay_k, can_send_rate.
    fn full_reset(&mut self) {
        self.crypt_msg_counter.store(0, Ordering::Relaxed);
        self.total_sent.store(0, Ordering::Relaxed);
        self.total_recv = 0;
        self.total_recv_shared.store(0, Ordering::Relaxed);
        self.rs = 1.0;
        self.used_sliced_limit = false;
        self.data_read_state.reset();
        self.send_lock.lock().unwrap().reset_tmp_slider();
        self.recvd_slider = Slider::new();
        self.recv_slicer = slicing::SlicingReceiver::new();
        self.last_online = 0;
        self.last_sent_hello = NEVER_SENT_MS;
    }

    fn bind_socket(&mut self, cur_tm: i64) {
        self.force_disconnect = false;
        if self.next_port < 1024 || self.next_port > 65000 {
            self.next_port = 1024;
        }
        // Bind family выбирается по серверному адресу. Если сервер — IPv6 literal `[2001:db8::1]:3000`
        // или DNS name резолвящийся в AAAA — bindаемся `[::]:port`. Иначе IPv4 `0.0.0.0:port`.
        let bind_family = if self.cfg.server_ip.contains(':') {
            "[::]"
        } else {
            "0.0.0.0"
        };
        let mut last_err: Option<std::io::Error> = None;
        for _ in 0..200 {
            let addr = format!("{}:{}", bind_family, self.next_port);
            match UdpSocket::bind(&addr) {
                Ok(sock) => {
                    if let Err(e) = sock.set_read_timeout(Some(Duration::from_secs(1))) {
                        warn!("set_read_timeout failed: {e}");
                    }
                    set_socket_buffers(&sock);
                    debug!("bound UDP socket on {}:{}", bind_family, self.next_port);
                    self.next_port += 1;
                    self.socket = Some(sock);
                    // Сброс кэша адреса сервера — может измениться при reconnect через DNS.
                    self.cached_server_addr = None;
                    self.start_inline_reader_session();
                    self.reset_bind_failure_tracking();
                    return;
                }
                Err(e) => {
                    last_err = Some(e);
                    self.next_port += 1;
                    if self.next_port > 65000 {
                        self.next_port = 1024;
                    }
                }
            }
        }
        // Все 200 попыток bind упали → не можем создать сокет В ЭТОТ ТИК.
        // НЕ ставим need_connect=false (audit_responsibility H3): на mobile при port
        // exhaustion (CGNAT, iOS background, ulimit) Disconnected заставил бы app
        // пересоздавать Client. Delphi (`MoonProtoUDPClient.pas:680+`) ретраит forever —
        // active library тоже должна.
        //
        // Throttled error-лог чтобы не спамить (раз в 5 сек). Следующий тик main loop
        // снова войдёт в bind_socket — обычно через короткое время порты освободятся.
        if self.should_log("bind_socket_exhausted", 5000) {
            if let Some(ref e) = last_err {
                error!(target: "moonproto::client",
                    "UdpSocket::bind failed after 200 attempts on {}:*, last error: {} (will retry on next tick)",
                    bind_family, e);
            } else {
                error!(target: "moonproto::client",
                    "UdpSocket::bind failed after 200 attempts on {}:* (will retry on next tick)",
                    bind_family);
            }
        }

        self.record_bind_failure(cur_tm);

        // auth_status оставляем Base — main loop попробует bind ещё раз через DEFAULT_SLEEP_MS.
        // Если app явно вызвал disconnect() — он сам выставит need_connect=false.
    }

    fn reset_bind_failure_tracking(&mut self) {
        self.bind_failure_streak = 0;
        self.first_bind_failure_ms = NEVER_TIME_MS;
        self.last_bind_failed_event_ms = NEVER_TIME_MS;
    }

    fn record_bind_failure(&mut self, cur_tm: i64) {
        if self.first_bind_failure_ms == NEVER_TIME_MS {
            self.first_bind_failure_ms = cur_tm;
        }
        self.bind_failure_streak = self.bind_failure_streak.saturating_add(1);

        let first_due =
            cur_tm.saturating_sub(self.first_bind_failure_ms) >= BIND_FAILED_FIRST_EVENT_MS;
        let repeat_due = self.last_bind_failed_event_ms == NEVER_TIME_MS
            || cur_tm.saturating_sub(self.last_bind_failed_event_ms) >= BIND_FAILED_REPEAT_EVENT_MS;

        if first_due && repeat_due {
            self.last_bind_failed_event_ms = cur_tm;
            self.fire_lifecycle(LifecycleEvent::BindFailed {
                consecutive_failures: self.bind_failure_streak,
            });
        }
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
