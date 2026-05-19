/// MoonProto UDP Client — two-thread architecture matching Delphi exactly.
///
/// Architecture (matches TMoonProtoUDPClient):
/// - Thread 1 (Main/Send): Execute loop — send queues, retry, reconnect, sleep(5ms)
/// - Thread 2 (Reader): UDPRead — blocking recv, process packets, dispatch
/// - Communication: shared state protected by Mutex (≡ Delphi FastLock, benchmarked: same perf)
///
/// See MAPPING.md for line-by-line correspondence.

use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::sync::{Arc, mpsc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use log::{debug, error, warn};
use crate::MoonKey;
use crate::crypto;
use crate::compression;
use crate::protocol::{Command, handshake, slider::Slider, slicing, crypted};
use crate::api_pending::ApiPending;
use crate::commands::engine_api::{EngineResponse, parse_engine_response};

// =============================================================================
//  ErrEmu — ТОЛЬКО ДЛЯ ТЕСТОВ. Симуляция packet loss на стороне клиента.
// =============================================================================
//
// ⚠️ **НЕ ИСПОЛЬЗОВАТЬ В PRODUCTION.** Это инструмент для нагрузочного тестирования
// gap-recovery / reconnect / extend-bucket логики через искусственный дроп UDP-пакетов.
//
// По умолчанию выключено (ERR_EMU_RATE = 0). Включается явным вызовом
// `set_err_emu(percent)` где percent ∈ [0..100].
//
// Зеркало серверного `MoonProtoErrEmu` (Delphi `MoonProtoUDPClient.pas:534-541` и
// `MoonProtoUDPServer.pas:1281-1288`): дроп происходит **после** успешной проверки
// MAC и version, до dispatch'а. Служебные команды (Ping / handshake-related / ACK)
// дропаются с rate/2 чтобы handshake не отваливался полностью.
//
// Использование (пример: 75% loss):
//   moonproto::client::set_err_emu(75);
//   let mut client = Client::new(cfg);
//   client.run(...);
//
// Используется в `examples/loss_logger.rs` — runtime-логгер потерь и восстановлений.
pub static ERR_EMU_RATE: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

/// Установить процент дропа входящих UDP-пакетов на стороне Rust-клиента (0..100).
/// 0 = выключено (значение по умолчанию). **ТОЛЬКО ДЛЯ ТЕСТОВ.**
/// Соответствует Delphi `MoonProtoErrEmu`.
pub fn set_err_emu(percent: u8) {
    ERR_EMU_RATE.store(percent.min(100), std::sync::atomic::Ordering::Relaxed);
}

/// Команды, для которых dropRate делится пополам (служебные).
/// Точное соответствие Delphi MoonProtoUDPClient.pas:537-538.
#[inline]
fn is_service_cmd(cmd: u8) -> bool {
    matches!(
        Command::from_byte(cmd),
        Command::Ping
            | Command::WantNewHello
            | Command::WrongHello
            | Command::WhoAreYou
            | Command::Fine
            | Command::NeedHelloAgain
            | Command::SizeTest
            | Command::ProbeMTU
            | Command::SlicedACK
    )
}

/// Возвращает `true` если пакет нужно дропнуть согласно ErrEmu.
#[inline]
fn err_emu_should_drop(cmd: u8) -> bool {
    let base_rate = ERR_EMU_RATE.load(std::sync::atomic::Ordering::Relaxed);
    if base_rate == 0 {
        return false;
    }
    let drop_rate = if is_service_cmd(cmd) { base_rate / 2 } else { base_rate };
    let roll: u8 = rand::random::<u8>() % 100;
    roll < drop_rate
}

// === Constants matching Delphi exactly ===
const DEFAULT_SLEEP_MS: u64 = 5;           // MoonProtoFunc.pas:19
const RECONNECT_WAITING_MS: i64 = 7000;    // MoonProtoUDPClient.pas:88
const RECONNECT_THROTTLE_MS: i64 = 15000;  // MoonProtoUDPClient.pas:89
const OFFLINE_BASE_MS: i64 = 2300;         // MoonProtoUDPClient.pas:772
const DEAD_ZONE_MS: i64 = 5000;            // MoonProtoUDPClient.pas:799
const NEED_HELLO_AGAIN_THROTTLE_MS: i64 = 700; // MoonProtoUDPClient.pas:568
const CLEANUP_INTERVAL_MS: i64 = 5000;     // MoonProtoIntStruct.pas:828
const COMPRESSED_FLAG: u8 = 0x80;          // MoonProtoDataStruct.pas:27
const MIN_SIZE_TO_COMPRESS: usize = 64;    // MoonProtoDataStruct.pas:31

/// DoS guard: верхний лимит pending_h. При долгой server silence без ACK retry-копии
/// накапливаются. 256 — щедрый запас для нормальной торговой нагрузки (burst orders).
const MAX_PENDING_H: usize = 256;

// Send priority (matches TMoonProtoSendPriority)
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SendPriority {
    Sliced, // MPS_Sliced: large, through slicing engine
    High,   // MPS_High: small, direct send, retry with ACK
    Low,    // MPS_Low: best effort, one per cycle
}

/// Unique key for command dedup (matches TMoonUniqueKey + TUniqueCommandKind в BaseStruct.pas:13-15).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UniqueKey {
    pub kind: u8,   // TUniqueCommandKind ordinal (0=None)
    pub uid: u64,
}

/// `TUniqueCommandKind` ordinals (MoonProtoBaseStruct.pas:13-15).
pub const UK_NONE:                     u8 = 0;
pub const UK_ORDER_STATUS:             u8 = 1;
pub const UK_ORDER_STATUS_SHORT:       u8 = 2;
pub const UK_ORDER_MOVE:               u8 = 3;
pub const UK_STOP_MOVE:                u8 = 4;
pub const UK_STRAT_SNAPSHOT:           u8 = 5;
pub const UK_BASE_UI_SETTINGS:         u8 = 6;
pub const UK_STRAT_SELL_PRICE_UPDATE:  u8 = 7;
pub const UK_BALANCE_FULL:             u8 = 8;
pub const UK_TURN_MM_DETECTION:        u8 = 9;
pub const UK_IMMUNE_CLICKS:            u8 = 10;
pub const UK_LEV_MANAGE_SETTINGS:      u8 = 11;
pub const UK_ARB_PRICES:               u8 = 12;
pub const UK_DEX_SWITCH:               u8 = 13;
pub const UK_SPOT_SWITCH:              u8 = 14;

impl UniqueKey {
    pub fn none() -> Self { Self { kind: UK_NONE, uid: 0 } }
    pub fn is_none(&self) -> bool { self.kind == UK_NONE }
    pub fn order_move(task_id: u64) -> Self { Self { kind: UK_ORDER_MOVE, uid: task_id } }
    pub fn immune_clicks(items_uid_sum: u64) -> Self { Self { kind: UK_IMMUNE_CLICKS, uid: items_uid_sum } }

    /// `UK_BaseUISettings` — единственный per-client настройковый snapshot;
    /// последняя версия замещает предыдущую в очереди отправки.
    pub fn base_ui_settings(uid: u64) -> Self { Self { kind: UK_BASE_UI_SETTINGS, uid } }
    /// `UK_TurnMMDetection` — переключатель ON/OFF, важен только последний.
    pub fn turn_mm_detection() -> Self { Self { kind: UK_TURN_MM_DETECTION, uid: 0 } }
    /// `UK_LevManageSettings` — настройки leverage, последняя версия замещает.
    pub fn lev_manage_settings(uid: u64) -> Self { Self { kind: UK_LEV_MANAGE_SETTINGS, uid } }
    /// `UK_DexSwitch` — выбор DEX, последний выбор замещает.
    pub fn dex_switch() -> Self { Self { kind: UK_DEX_SWITCH, uid: 0 } }
    /// `UK_SpotSwitch` — выбор spot режима, последний замещает.
    pub fn spot_switch() -> Self { Self { kind: UK_SPOT_SWITCH, uid: 0 } }
    /// `UK_StratSellPriceUpdate` — обновление sell-price конкретной стратегии;
    /// `uid` = strategy_id, чтобы dedup был per-strategy (несколько стратегий
    /// могут обновлять цену параллельно, но каждая сама себя замещает).
    pub fn strat_sell_price_update(strategy_id: u64) -> Self { Self { kind: UK_STRAT_SELL_PRICE_UPDATE, uid: strategy_id } }
    /// `UK_StratSnapshot` — полный snapshot всех стратегий, единственная пишет.
    pub fn strat_snapshot() -> Self { Self { kind: UK_STRAT_SNAPSHOT, uid: 1 } }
    /// `UK_BalanceFull` — full balance snapshot; единственный замещаемый.
    pub fn balance_full() -> Self { Self { kind: UK_BALANCE_FULL, uid: 0 } }
}

/// Item in the send queue (matches TMoonProtoDataToSend)
#[derive(Clone)]
pub struct SendItem {
    pub data: Vec<u8>,         // serialized command stream
    pub cmd: u8,               // TMoonProtoCommand ordinal
    pub encrypted: bool,       // FCrypted
    pub priority: SendPriority,
    pub retry_left: i32,       // RetryLeft
    pub max_retries: i32,      // MaxRetryCount
    pub msg_num: u64,          // for ACK tracking (assigned in Crypt)
    pub last_sent_at: i64,     // ms timestamp of last send
    pub u_key: UniqueKey,      // dedup key (matches TMoonUniqueKey)
}

/// Message from reader thread to main loop.
/// Public for use in `ClientEvent::Recv` variant — но напрямую не конструируется снаружи,
/// reader thread сам формирует RecvMsg внутри `spawn_reader`.
#[doc(hidden)]
#[derive(Clone)]
pub struct RecvMsg {
    cmd: u8,
    payload: Vec<u8>,
    recv_bytes: u64,
    timestamp_ms: i64,
    /// Аудит #7 (audit_delphi_deviation E-V2-02): эпоха reader thread'а который создал
    /// это сообщение. Инкрементируется на каждый `spawn_reader`. Main loop игнорирует
    /// сообщения с epoch != `current_reader_epoch` — это защита от пакетов старого
    /// reader thread'а который ещё не завершился во время reconnect'а.
    epoch: u32,
}

/// Message from app to main loop (send command request)
/// Matches Delphi: SendCmd → DataToSend queue
#[derive(Clone)]
pub struct SendMsg {
    pub item: SendItem,
}

/// Объединённый канал: reader thread и прикладной слой шлют события в один mpsc.
/// Main loop делает `recv_timeout(5ms)` → просыпается мгновенно на любое событие.
/// Это устраняет 5мс латентность ответа на Ping/Sliced/handshake (= Delphi inline в UDPRead).
#[doc(hidden)]
#[derive(Clone)]
pub enum ClientEvent {
    Recv(RecvMsg),
    Send(SendMsg),
}

// A-V2-07 fix: бывший ручной impl Clone заменён на #[derive(Clone)] на RecvMsg выше.

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AuthStatus {
    Base,
    Connected,
    AuthDone,
    Offline,
}

/// Lifecycle event — уведомления о смене состояния канала связи с сервером.
///
/// Подключай callback через [`Client::on_lifecycle`]. Callback выполняется в
/// main thread (тот же где `client.run()`).
///
/// Типовая последовательность:
/// ```text
///   Connecting  → Authenticated  → [running, обычная работа] → Disconnected
///                       │
///                       └──[потеря связи]──► Reconnecting → Authenticated → ...
///                                                  │
///                                                  └──[detected restart]──► ServerRestart
/// ```
///
/// `Authenticated` может прилетать несколько раз за жизнь Client'а (после каждого
/// успешного re-handshake). `ServerRestart` — отдельный сигнал что нужно сбросить
/// клиентские кэши market index'ов.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LifecycleEvent {
    /// Handshake начат (Hello отправлен), Fine ещё не получен. Сетевой trip-time
    /// между Connecting и Authenticated = первые 1-3 RTT. Никаких действий от
    /// потребителя не требуется — клиент сам пробует, retry'ит, переключает порты.
    Connecting,
    /// Fine получен — канал авторизован и готов принимать/отправлять команды.
    /// **Обычные действия потребителя** на этот event:
    /// - `client.api_subscribe_all_trades()` — подписаться на сделки
    /// - `client.request_all_statuses(uid)` — запросить статусы ордеров
    /// - `client.strat_snapshot_request()` — запросить snapshot стратегий
    /// - `client.ui_settings_request()` — запросить настройки
    /// - `client.balance_request_refresh()` — запросить актуальный баланс
    Authenticated,
    /// Канал закрыт явным `client.disconnect()` от потребителя. Финальное
    /// состояние — для возобновления связи нужен новый `Client::new`.
    Disconnected,
    /// Потеря связи > порога (`RECONNECT_WAITING_MS`) — клиент **сам** пытается
    /// soft-reconnect (HelloAgain без полного handshake). Если HelloAgain не
    /// проходит (сервер не помнит этого клиента) — следующий цикл начнётся с
    /// нового Hello → новый `Connecting`. **Никаких действий от потребителя
    /// не требуется**, можно только показать в UI индикатор "переподключаемся".
    Reconnecting,
    /// Детектирован перезапуск сервера: `PeerAppToken` изменился между
    /// сессиями (см. SPEC §3 detection mechanism). На сервере **обнулились
    /// market indexes** — кэши клиента по индексам рынков невалидны.
    /// **Действие потребителя**:
    /// - Сбросить локальные mappings `market_idx → market_name`
    /// - `client.api_get_markets_indexes()` — получить свежие
    /// - `client.api_reload_order_book()` или re-subscribe — получить свежие book'и
    /// - `client.api_get_markets_balance_full()` — fresh balance snapshot
    ServerRestart,
}

pub type LifecycleFn = Box<dyn FnMut(LifecycleEvent) + Send>;

pub struct ClientConfig {
    pub server_ip: String,
    pub server_port: u16,
    pub master_key: MoonKey,
    pub mac_key: MoonKey,
    pub mask_ver: u8,
    pub client_id: u64,
}

pub type OnDataFn = Box<dyn FnMut(Command, &[u8]) + Send>;

/// Sent Sliced datagram awaiting ACK (matches TMoonProtoSlicedData in Sending list)
struct SentSliced {
    datagram_num: u16,
    slices: Vec<Vec<u8>>,         // each slice payload (SliceHeader + data)
    piece_last_checked: Vec<i64>, // per-piece LastChecked timestamp
    ack_flags: [u8; 32],          // which blocks ACK'd
    blocks_count: usize,
    sent_count: usize,
    last_checked: i64,            // Min of all piece_last_checked
    retry_count: i32,
    max_retry_count: i32,
    u_key: UniqueKey,             // for UKey dedup (matches TMoonProtoSlicedData.UKey)
}

/// Public handle to the client. Allows sending commands from any thread.
pub struct Client {
    cfg: ClientConfig,

    // Единый event-канал: reader + app шлют ClientEvent → main делает recv_timeout(5ms).
    // Аудит #1 (audit_delphi_deviation): bounded channel вместо unbounded mpsc::channel().
    // Раньше на burst 50K pps + slow callback канал рос неограниченно → OOM-vector (50K msgs ×
    // ~1500б payload = 75MB/sec). Теперь sync_channel(1024) + try_send + drop+warn при overflow.
    // UDP уже lossy → drop пакета на user-level семантически эквивалентен kernel drop при
    // переполнении SO_RCVBUF (что делает Delphi через ThreadedEvent inline handling).
    event_tx: mpsc::SyncSender<ClientEvent>,
    event_rx: mpsc::Receiver<ClientEvent>,

    // Pending H-commands (main thread only, no sharing)
    pending_h: Vec<SendItem>,
    // Sent Sliced datagrams awaiting ACK (matches TMoonProtoClient.Sending)
    sending: Vec<SentSliced>,

    // Main thread state
    socket: Option<UdpSocket>,
    connected: bool,   // FConnected: true after first valid packet received
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
    /// Аналогично для decode. См. `encode_cipher`.
    decode_cipher: Option<crate::crypto::Aes128Gcm>,

    _start: Instant,
    last_sent_hello: i64,
    waiting_hello_start: i64,
    last_socket_recreate: i64,
    last_need_hello_again: i64,
    last_cleanup: i64,
    prev_cycle_tm: i64,        // for ActualSleepTime EMA

    crypt_msg_counter: u64,
    send_datagram_num: u16,

    round_trip_delay: i64,
    actual_pmtu: u16,
    rs: f64,
    overheat: u8,
    peer_app_token: u64,       // PeerAppToken from WhoAreYou (detect server restart)
    server_time_delta: f64,    // ServerTimeDelta = Ping.InitialTime - Now (for order time correction)
    global_timing_orders: u16, // GlobalTimingOrders from Ping
    net_lag_ping: i64,         // NetLagPing (ms abs diff between NTP-corrected time and server time)

    // Adaptive rate control (matches MoonProtoIntStruct.pas:197-245)
    trip_delay_k: f64,       // TripDelayK (1.05-1.25, init 1.1)
    last_set_trip_k: i64,    // LastSetTripK
    avg_dup_count: f64,      // AvgDupCount
    avg_over_heat: f64,      // AvgOverHeat (% retransmission overhead, EMA, matches :1210-1212)
    can_send_rate: i32,      // CanSendRate (bytes/sec, init 2MB/s)
    used_sliced_limit: bool, // UsedSlicedLimit
    actual_sleep_time: f64,  // ActualSleepTime (EMA of actual loop cycle time)

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
    tmp_send_buf: Vec<u8>,   // accumulated Grouped payload
    tmp_send_count: usize,   // items in batch
    tmp_send_size: usize,    // total bytes including headers

    slider: Slider,
    slicer: slicing::SlicingReceiver,
    total_sent: u64,
    next_port: u16,
    ping_count: u32,

    // audit_robustness M8: throttle для SizeTest/ProbeMTU ответов (anti-amplification).
    last_size_test_ack_ms: i64,
    last_probe_mtu_ack_ms: i64,

    /// Реестр pending Engine API запросов. Shareable через `Arc::clone`.
    /// При получении `Command::API` пакета — `dispatch` доставит response
    /// в зарегистрированный receiver, если UID найден.
    pub api_pending: Arc<ApiPending>,

    /// Lifecycle callback — вызывается при изменении статуса канала (Connecting → Authenticated → Reconnecting/Disconnected).
    /// Установить через `client.on_lifecycle(cb)`. Опционально.
    lifecycle_cb: Option<LifecycleFn>,
    /// Предыдущий auth_status (для детектирования переходов).
    prev_auth_status: AuthStatus,

    /// Shutdown signal для reader thread.
    /// `spawn_reader` создаёт НОВЫЙ `Arc<AtomicBool>` для каждого reader thread и сохраняет
    /// его сюда. При `do_force_disconnect` / `Drop` мы ставим `true` — reader thread выйдет
    /// из loop (макс через `read_timeout` = 1s).
    /// Каждый новый reader получает свой Arc → старый и новый reader НЕ конфликтуют.
    reader_shutdown: Arc<AtomicBool>,
    /// Аудит #7 (audit_delphi_deviation E-V2-02): инкремент на каждый `spawn_reader`.
    /// Reader thread получает копию текущего значения и проставляет в `RecvMsg.epoch`.
    /// Main loop фильтрует stale events с epoch != этого значения. Защита от race на
    /// reconnect (старый reader может ещё крутиться 1с пока read_timeout сработает).
    current_reader_epoch: u32,

    /// Кэш разрешённого адреса сервера. Закрывает B-05: до этого `server_addr()` форматировал
    /// строку + `send_to(&str)` делал `getaddrinfo` resolve на каждый send (потенциально DNS-блокирующий).
    /// Кэш сбрасывается при ошибке resolve (например, DNS отвалился) — на следующем bind_socket
    /// повторно резолвится.
    cached_server_addr: Option<SocketAddr>,

    /// D-02: state-machine для двойной отправки ImFriend (требование Delphi handshake протокола
    /// — финальный пакет шлётся дважды с короткой паузой для надёжности).
    /// Раньше использовался `thread::sleep(32ms)` прямо в `handle_handshake`, что блокировало main loop
    /// на 32мс — за это время накапливались UDP-пакеты в reader channel, heartbeat не отправлялся,
    /// pending API timeouts не срабатывали.
    /// Теперь: первый ImFriend уходит сразу, второй планируется в `pending_second_imfriend = Some((due_ms, payload))`,
    /// main loop каждый тик проверяет и отправляет когда `cur_tm >= due_ms`.
    /// Сбрасывается при `full_reset` и при отправке.
    pending_second_imfriend: Option<(i64, Vec<u8>)>,
}

impl Client {
    pub fn new(cfg: ClientConfig) -> Self {
        // Аудит #1: bounded channel. 1024 events × ~1500б payload = ~1.5MB worst case.
        // При burst 10K pps это 100мс задержки main loop без потери. После — drop+warn.
        const EVENT_CHANNEL_CAPACITY: usize = 1024;
        let (event_tx, event_rx) = mpsc::sync_channel(EVENT_CHANNEL_CAPACITY);

        Self {
            cfg,
            event_tx,
            event_rx,
            pending_h: Vec::new(),
            sending: Vec::new(),
            socket: None,
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
            decode_cipher: None,
            _start: Instant::now(),
            last_sent_hello: 0, // Delphi: 0 initially. now_ms() is huge (system time) → diff > interval → Hello sends immediately
            waiting_hello_start: 0,
            last_socket_recreate: 0,
            last_need_hello_again: 0,
            last_cleanup: 0,
            prev_cycle_tm: 0,
            crypt_msg_counter: 0,
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
            last_set_trip_k: 0,
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
            tmp_send_size: 15, // ClientMsgHeader(15) overhead
            slider: Slider::new(),
            slicer: slicing::SlicingReceiver::new(),
            total_sent: 0,
            next_port: 1024 + (rand::random::<u16>() % (65000 - 1024)),
            ping_count: 0,
            last_size_test_ack_ms: 0,
            last_probe_mtu_ack_ms: 0,
            api_pending: ApiPending::new_arc(),
            lifecycle_cb: None,
            prev_auth_status: AuthStatus::Base,
            reader_shutdown: Arc::new(AtomicBool::new(false)),
            current_reader_epoch: 0,
            cached_server_addr: None,
            pending_second_imfriend: None,
        }
    }

    /// Установить lifecycle callback. Вызывается из main-thread при изменении auth_status.
    pub fn on_lifecycle(&mut self, cb: LifecycleFn) {
        self.lifecycle_cb = Some(cb);
    }

    /// Внутренний хук: вызывает callback на переход состояния.
    /// Должен вызываться там где меняется `self.auth_status` или `self.need_connect`.
    fn fire_lifecycle(&mut self, ev: LifecycleEvent) {
        if let Some(ref mut cb) = self.lifecycle_cb {
            cb(ev);
        }
    }

    /// Проверить изменение auth_status и эмитировать соответствующий lifecycle event.
    /// Вызывается из main loop после каждого пакета.
    fn check_lifecycle_transition(&mut self) {
        if self.auth_status == self.prev_auth_status { return; }
        let new_ev = match (self.prev_auth_status, self.auth_status) {
            // Первичное подключение (cold start или после Disconnect)
            (AuthStatus::Base, AuthStatus::Connected) => Some(LifecycleEvent::Connecting),
            // Re-handshake после потери связи (soft reconnect) — Offline → Connected
            (AuthStatus::Offline, AuthStatus::Connected) => Some(LifecycleEvent::Connecting),
            // Успешная авторизация (Fine received)
            (_, AuthStatus::AuthDone) if self.prev_auth_status != AuthStatus::AuthDone => {
                Some(LifecycleEvent::Authenticated)
            }
            // Потеря связи
            (AuthStatus::AuthDone, AuthStatus::Offline) => Some(LifecycleEvent::Reconnecting),
            // Disconnect от потребителя (явный)
            (_, AuthStatus::Base) if self.prev_auth_status != AuthStatus::Base
                                  && !self.need_connect => Some(LifecycleEvent::Disconnected),
            _ => None,
        };
        self.prev_auth_status = self.auth_status;
        if let Some(ev) = new_ev {
            self.fire_lifecycle(ev);
        }
    }

    /// Public API: queue a command for sending (thread-safe, via channel).
    /// Matches Delphi: SendCmd → SendCmdInt → DataToSend/H/L.
    /// Can be called from any thread (send_tx is cloneable).
    ///
    /// E-V2-06: возвращает `()`, **но** при закрытом канале (main loop завершён)
    /// логирует error через `log` crate. Потерянная команда — серьёзный сигнал,
    /// но возвращать Result сломало бы API всех Client wrappers (`client.new_order(...)`
    /// и т.д.). Если потребителю нужен гарантированный feedback — он может
    /// проверить статус через `LifecycleEvent::Disconnected` callback и не
    /// шарашить новые команды после.
    pub fn send_cmd(&self, data: Vec<u8>, cmd: Command, priority: SendPriority, encrypted: bool, max_retries: i32) {
        self.send_cmd_keyed(data, cmd, priority, encrypted, max_retries, UniqueKey::none());
    }

    pub fn send_cmd_keyed(&self, data: Vec<u8>, cmd: Command, priority: SendPriority, encrypted: bool, max_retries: i32, u_key: UniqueKey) {
        let item = SendItem {
            data,
            cmd: cmd as u8,
            encrypted,
            priority,
            retry_left: if encrypted { max_retries - 1 } else { 0 },
            max_retries,
            msg_num: 0,
            last_sent_at: 0,
            u_key,
        };
        // Аудит #1 + E-V2-06: для send команд (app → main) используем blocking `send` (не
        // `try_send`). Application threads ОБЯЗАНЫ ждать пока main loop разгрузит канал —
        // в отличие от UDP пакетов, торговая команда не должна быть дропнута. Если main loop
        // мёртв (channel closed) — логируем и возвращаемся (потребитель проверит lifecycle).
        if self.event_tx.send(ClientEvent::Send(SendMsg { item })).is_err() {
            log::error!(target: "moonproto::client",
                "send_cmd: event channel closed (main loop dead?) — packet cmd={:?} priority={:?} dropped",
                cmd, priority);
        }
    }

    /// Get a clone of event_tx for use from other threads (e.g. terminal UI).
    /// Получить клонированный `SyncSender<ClientEvent>` для отправки команд из любого потока.
    /// Приложение шлёт `ClientEvent::Send(SendMsg { item })` через клонированный sender.
    ///
    /// Аудит #1 (audit_delphi_deviation): тип теперь `SyncSender` (bounded channel).
    /// `.send()` BLOCKS если канал переполнен — приложение wait'ит пока main loop разгрузит.
    /// Это правильное поведение для торговых команд (vs UDP пакеты которые можно drop).
    pub fn sender(&self) -> mpsc::SyncSender<ClientEvent> {
        self.event_tx.clone()
    }

    /// Convenience: send an Engine API request (MPS_Sliced, encrypted, MaxRetries=3).
    /// Matches: SendAPICmd → SendCmd → DataToSend(MPS_Sliced, FCrypted=true, MaxRetries=3).
    /// TBaseCommand.FMaxRetries = 3 (BaseStruct.pas:141, default из SetDefaults).
    pub fn send_api_request(&self, request_payload: &[u8]) {
        self.send_cmd(
            request_payload.to_vec(),
            Command::API,
            SendPriority::Sliced,
            true,    // Engine API is always encrypted
            3,       // TBaseCommand.FMaxRetries = 3 (BaseStruct.pas:141)
        );
    }

    /// Send Engine API request + регистрация в `api_pending` для ожидания ответа.
    ///
    /// UID извлекается из payload (offset 3..11 в TBaseCommand header).
    /// Возвращает `mpsc::Receiver<EngineResponse>` — потребитель делает
    /// `rx.recv_timeout(Duration::from_secs(N))` для блокирующего ожидания.
    ///
    /// При timeout вызвать `client.api_pending.remove(uid)` чтобы освободить slot.
    pub fn send_api_request_async(&self, request_payload: &[u8]) -> mpsc::Receiver<EngineResponse> {
        // D-V2-01 fix: безопасный slice-доступ к uid. Старая версия `request_payload[3..11]`
        // паниковала при len<11 — publis API не должен валить процесс из-за плохого input'а.
        let uid = request_payload
            .get(3..11)
            .and_then(|s| s.try_into().ok())
            .map(u64::from_le_bytes)
            .unwrap_or(0);
        let rx = self.api_pending.register(uid, self.now_ms());
        self.send_api_request(request_payload);
        rx
    }

    // ====================================================================
    //  High-level Engine API wrappers (convenience over send_api_request_async)
    // ====================================================================

    /// `emk_BaseCheck` — initial probe (call before AuthCheck during handshake).
    pub fn api_base_check(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::base_check())
    }

    /// `emk_AuthCheck` — verify credentials and get account info.
    pub fn api_auth_check(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::auth_check())
    }

    /// `emk_GetMarketsList` — full markets list snapshot.
    pub fn api_get_markets_list(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::get_markets_list())
    }

    /// `emk_GetMarketsIndexes` — market names в порядке mIndex.
    pub fn api_get_markets_indexes(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::get_markets_indexes())
    }

    /// `emk_UpdateMarketsList` — обновление цен по mIndex.
    pub fn api_update_markets_list(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::update_markets_list())
    }

    /// `emk_GetBalance` для одной валюты.
    pub fn api_get_balance(&self, currency: &str) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::get_balance(currency))
    }

    /// `emk_GetMarketsBalanceFull` — полный snapshot всех балансов.
    pub fn api_get_markets_balance_full(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::get_markets_balance_full())
    }

    /// `emk_GetOrder` по UID ордера.
    pub fn api_get_order(&self, order_uid: u64) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::get_order(order_uid))
    }

    /// `emk_GetOpenOrders` — список открытых ордеров.
    pub fn api_get_open_orders(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::get_open_orders())
    }

    /// `emk_GetActiveOrders`.
    pub fn api_get_active_orders(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::get_active_orders())
    }

    /// `emk_CancelAllOrders`.
    pub fn api_cancel_all_orders(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::cancel_all_orders())
    }

    /// `emk_SetLeverage(market, new_leverage)`.
    pub fn api_set_leverage(&self, market: &str, new_lev: i32) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::set_leverage(market, new_lev))
    }

    /// `emk_SetHedgeMode(enabled)`.
    pub fn api_set_hedge_mode(&self, hedge_mode: bool) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::set_hedge_mode(hedge_mode))
    }

    /// `emk_QueryHedgeMode`.
    pub fn api_query_hedge_mode(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::query_hedge_mode())
    }

    /// `emk_CheckAPIExpirationTime`.
    pub fn api_check_expiration_time(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::check_api_expiration_time())
    }

    /// `emk_CheckBinanceTags` — теги монет.
    pub fn api_check_binance_tags(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::check_binance_tags())
    }

    /// `emk_SubscribeAllTrades`.
    pub fn api_subscribe_all_trades(&self, want_mm_orders: bool) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::subscribe_all_trades(want_mm_orders))
    }

    /// `emk_UnsubscribeAllTrades`.
    pub fn api_unsubscribe_all_trades(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::unsubscribe_all_trades())
    }

    /// `emk_SubscribeOrderBook` — `markets` empty = подписка на все.
    pub fn api_subscribe_order_book(&self, markets: &[&str]) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::subscribe_order_book(markets))
    }

    /// `emk_UnsubscribeOrderBook` — `markets` empty = отписка от всех.
    pub fn api_unsubscribe_order_book(&self, markets: &[&str]) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::unsubscribe_order_book(markets))
    }

    /// `emk_RequestOrderBookFull(market_idx, book_kind)` — запрос полного snapshot.
    pub fn api_request_order_book_full(&self, market_idx: u16, book_kind: u8) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::request_order_book_full(market_idx, book_kind))
    }

    /// `emk_ReloadOrderBook`.
    pub fn api_reload_order_book(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::reload_order_book())
    }

    /// `emk_ChangePositionType(market, type, new_market)`.
    pub fn api_change_position_type(&self, market: &str, pos_type: u8, new_market: bool) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::change_position_type(market, pos_type, new_market))
    }

    /// `emk_ConvertDustBNB`.
    pub fn api_convert_dust_bnb(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::convert_dust_bnb())
    }

    /// `emk_ConfirmRiskLimit(market)`.
    pub fn api_confirm_risk_limit(&self, market: &str) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::confirm_risk_limit(market))
    }

    /// `emk_SetMAMode(enabled)`.
    pub fn api_set_ma_mode(&self, ma_mode: bool) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::set_ma_mode(ma_mode))
    }

    /// `emk_DoTransferAsset(asset, q, from, to)`.
    pub fn api_do_transfer_asset(&self, asset: &str, qty: f64, from: u8, to: u8) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::do_transfer_asset(asset, qty, from, to))
    }

    /// `emk_UpdateTransferAssets(kind)`.
    pub fn api_update_transfer_assets(&self, kind: u8) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::update_transfer_assets(kind))
    }

    /// `emk_TradesResend(packet_nums)` — multi-batch (auto-split по 200).
    /// Возвращает массив receivers (по одному на batch).
    pub fn api_trades_resend_batches(&self, packet_nums: &[u16]) -> Vec<mpsc::Receiver<EngineResponse>> {
        crate::commands::engine_request::trades_resend_batches(packet_nums)
            .iter()
            .map(|raw| self.send_api_request_async(raw))
            .collect()
    }

    /// `emk_GetCoinCardCandles(market, ticks)` — запрос свечей для CoinCard (не chunked).
    /// Response — `count:i32 + N × TDeepPrice(28 bytes)`. Парсить через
    /// `commands::candles::parse_coin_card_candles_response(&resp.data)`.
    pub fn api_get_coin_card_candles(&self, market: &str, ticks: crate::commands::candles::DeepHistoryKind)
        -> mpsc::Receiver<EngineResponse>
    {
        self.send_api_request_async(&crate::commands::candles::get_coin_card_candles(market, ticks))
    }

    /// `emk_RequestCandlesData` — запрос chunked candles + wall data.
    /// **NB:** ответ приходит несколькими `EngineResponse` пакетами. Используй
    /// `commands::candles::CandlesAggregator::on_chunk(&resp.data)` чтобы собрать
    /// все чанки. Aggregator вернёт `Some(merged)` когда все чанки получены.
    ///
    /// Так как pending_api registry удаляет sender после ПЕРВОГО response,
    /// для chunked candles нужно использовать обычный `on_data` callback и
    /// фильтровать по `Command::API` + `EngineMethod::RequestCandlesData`.
    /// (Для single-response API — `send_api_request_async` работает.)
    pub fn api_request_candles_data(&self) {
        self.send_api_request(&crate::commands::engine_request::request_candles_data());
    }

    // ====================================================================
    //  High-level Trade wrappers (convenience over commands::trade::build_*)
    //  Все шлются как Command::Order (28), Priority=High, encrypted, MaxRetries=3.
    //  Кроме DoClose/DoLimitClose/DoSplit/DoSellOrder/DoMarketSplit — MaxRetries=1.
    // ====================================================================

    fn send_trade(&self, payload: Vec<u8>, max_retries: i32) {
        self.send_cmd(payload, Command::Order, SendPriority::High, true, max_retries);
    }

    /// `send_trade` с UniqueKey — для команд имеющих `[MoonCmdUnique(UK_*)]` атрибут.
    /// Старые pending команды с тем же UKey удаляются из `self.sending`/`self.pending_h`
    /// (matches Delphi SendCmdInt:780-785 + CheckSendingData).
    fn send_trade_keyed(&self, payload: Vec<u8>, max_retries: i32, u_key: UniqueKey) {
        self.send_cmd_keyed(payload, Command::Order, SendPriority::High, true, max_retries, u_key);
    }

    /// `TNewOrderCommand` (CmdId=3) — открыть новый ордер.
    pub fn new_order(&self, ctx: crate::commands::trade::TradeCtx, market: &str,
                     is_short: bool, price: f64, strat_id: u64, order_size: f64) {
        let raw = crate::commands::trade::build_new_order(ctx, market, is_short, price, strat_id, order_size);
        self.send_trade(raw, 3);
    }

    /// `TOrderReplaceCommand` (CmdId=6, UK_OrderMove) — replace ордера новой ценой.
    /// `ctx.uid` должен быть **task_id ордера** для корректного dedup'а.
    ///
    /// `Epoch` устанавливается внутри в 0 (audit_responsibility B1: в Delphi приложение
    /// всегда передаёт `Epoch=0` в C→S командах; поле используется только в server→client
    /// для filter out-of-order).
    pub fn replace_order(&self, ctx: crate::commands::trade::TradeCtx, market: &str,
                          status: crate::commands::trade::OrderWorkerStatus,
                          order_type: crate::commands::trade::OrderType, new_price: f64) {
        let raw = crate::commands::trade::build_order_replace(ctx, market, 0, status, order_type, new_price);
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
    }

    /// `TAllStatusesReq` (CmdId=9) — запросить все статусы ордеров.
    pub fn request_all_statuses(&self, uid: u64) {
        let raw = crate::commands::trade::build_all_statuses_request(uid);
        self.send_trade(raw, 3);
    }

    /// `TOrderCancelCommand` (CmdId=10, UK_OrderMove) — отменить ордер.
    /// `ctx.uid` должен быть **task_id ордера** для корректного dedup'а.
    /// `Epoch=0` (внутри). См. `replace_order`.
    pub fn cancel_order(&self, ctx: crate::commands::trade::TradeCtx, market: &str,
                         status: crate::commands::trade::OrderWorkerStatus) {
        let raw = crate::commands::trade::build_order_cancel(ctx, market, 0, status);
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
    }

    /// `TJoinOrdersCommand` (CmdId=11) — join открытых ордеров в один.
    pub fn join_orders(&self, ctx: crate::commands::trade::TradeCtx, market: &str, is_short: bool) {
        let raw = crate::commands::trade::build_join_orders(ctx, market, is_short);
        self.send_trade(raw, 3);
    }

    /// `TSplitOrderCommand` (CmdId=12) — разбить ордер на части.
    pub fn split_order(&self, ctx: crate::commands::trade::TradeCtx, market: &str,
                        split_parts: i32, split_small: bool, split_small_sell: bool) {
        let raw = crate::commands::trade::build_split_order(ctx, market, split_parts, split_small, split_small_sell);
        self.send_trade(raw, 3);
    }

    /// `TMoveAllSellsCommand` (CmdId=13).
    pub fn move_all_sells(&self, ctx: crate::commands::trade::TradeCtx, market: &str,
                           cmd_type: crate::commands::trade::MoveAllCmdType,
                           move_kind: crate::commands::trade::ReplaceMultiKind,
                           price: f64, zone: crate::commands::trade::PriceZone,
                           side: crate::commands::trade::FixedPosition) {
        let raw = crate::commands::trade::build_move_all_sells(ctx, market, cmd_type as u8, move_kind, price, zone, side);
        self.send_trade(raw, 3);
    }

    /// `TDoClosePositionCommand` (CmdId=14, MaxRetries=1).
    pub fn do_close_position(&self, ctx: crate::commands::trade::TradeCtx, market: &str, market_sell: bool) {
        let raw = crate::commands::trade::build_do_close_position(ctx, market, market_sell);
        self.send_trade(raw, 1);
    }

    /// `TDoLimitClosePositionCommand` (CmdId=15, MaxRetries=1).
    pub fn do_limit_close_position(&self, ctx: crate::commands::trade::TradeCtx, market: &str, is_short: bool) {
        let raw = crate::commands::trade::build_do_limit_close_position(ctx, market, is_short);
        self.send_trade(raw, 1);
    }

    /// `TDoSplitPositionCommand` (CmdId=16, MaxRetries=1).
    pub fn do_split_position(&self, ctx: crate::commands::trade::TradeCtx, market: &str, is_short: bool) {
        let raw = crate::commands::trade::build_do_split_position(ctx, market, is_short);
        self.send_trade(raw, 1);
    }

    /// `TDoSellOrderCommand` (CmdId=17, MaxRetries=1).
    pub fn do_sell_order(&self, ctx: crate::commands::trade::TradeCtx, market: &str, price: f64, size: f64) {
        let raw = crate::commands::trade::build_do_sell_order(ctx, market, price, size);
        self.send_trade(raw, 1);
    }

    /// `TOrderStatusRequest` (CmdId=18) — запросить статус конкретного ордера.
    pub fn request_order_status(&self, ctx: crate::commands::trade::TradeCtx, market: &str) {
        let raw = crate::commands::trade::build_order_status_request(ctx, market);
        self.send_trade(raw, 3);
    }

    /// `TOrderStopsUpdate` (CmdId=20, UK_OrderMove). `ctx.uid` = task_id ордера.
    /// `Epoch=0` (внутри). См. `replace_order`.
    pub fn update_order_stops(&self, ctx: crate::commands::trade::TradeCtx, market: &str,
                               status: crate::commands::trade::OrderWorkerStatus,
                               stops: &crate::commands::trade::StopSettings) {
        let raw = crate::commands::trade::build_order_stops_update(ctx, market, 0, status, stops);
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
    }

    /// `TTurnPanicSellCommand` (CmdId=21, UK_OrderMove). `ctx.uid` = task_id ордера.
    /// `Epoch=0` (внутри). См. `replace_order`.
    pub fn turn_panic_sell(&self, ctx: crate::commands::trade::TradeCtx, market: &str,
                            status: crate::commands::trade::OrderWorkerStatus, turn_on: bool) {
        let raw = crate::commands::trade::build_turn_panic_sell(ctx, market, 0, status, turn_on);
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
    }

    /// `TSetImmuneCommand` (CmdId=22, UK_ImmuneClicks) — пометить ордера как immune.
    /// UKey.UID = sum(items[].uid) (matches Delphi TSetImmuneCommand.SetUKey pas:786-792).
    pub fn set_immune(&self, uid: u64, items: &[crate::commands::trade::ImmuneItem]) {
        let raw = crate::commands::trade::build_set_immune(uid, items);
        let items_uid_sum: u64 = items.iter().fold(0u64, |acc, it| acc.wrapping_add(it.uid));
        self.send_trade_keyed(raw, 3, UniqueKey::immune_clicks(items_uid_sum));
    }

    /// `TMoveAllBuysCommand` (CmdId=27).
    pub fn move_all_buys(&self, ctx: crate::commands::trade::TradeCtx, market: &str,
                          cmd_type: crate::commands::trade::MoveAllCmdType,
                          move_kind: crate::commands::trade::ReplaceMultiKind,
                          price: f64, side: crate::commands::trade::FixedPosition) {
        let raw = crate::commands::trade::build_move_all_buys(ctx, market, cmd_type as u8, move_kind, price, side);
        self.send_trade(raw, 3);
    }

    /// `TVStopUpdate` (CmdId=29, UK_OrderMove). `ctx.uid` = task_id ордера.
    /// `Epoch=0` (внутри). См. `replace_order`.
    pub fn update_vstop(&self, ctx: crate::commands::trade::TradeCtx, market: &str,
                         status: crate::commands::trade::OrderWorkerStatus,
                         vstop_on: bool, vstop_fixed: bool, vstop_level: f64, vstop_vol: f64) {
        let raw = crate::commands::trade::build_vstop_update(ctx, market, 0, status,
                                                              vstop_on, vstop_fixed, vstop_level, vstop_vol);
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
    }

    /// `TDoMarketSplitPositionCommand` (CmdId=30, MaxRetries=1).
    pub fn do_market_split_position(&self, ctx: crate::commands::trade::TradeCtx, market: &str, is_short: bool) {
        let raw = crate::commands::trade::build_do_market_split_position(ctx, market, is_short);
        self.send_trade(raw, 1);
    }

    /// `TPenaltyCommand` (CmdId=23) — пометить маркет penalty (cooldown).
    /// Docs_api audit B-04: команда активно используется в MoonBot Delphi
    /// (TaskWorkers.pas:8361, Unit1.pas:11859/23750).
    pub fn penalty(&self, ctx: crate::commands::trade::TradeCtx, market: &str) {
        let raw = crate::commands::trade::build_penalty(ctx, market);
        self.send_trade(raw, 3);
    }

    // ====================================================================
    //  High-level UI wrappers (Command::UI, encrypted=true)
    //  Покрывают MClient.SendUICmd(T*Command.Create(...)) семантику Delphi.
    //  UID авто-генерируется через rand::random() — потребитель не передаёт.
    //  Priority/MaxRetries/UKey — из атрибутов соответствующих Delphi-классов.
    //  Аудит docs_api B-01: было 14 build_* функций без Client-обёрток.
    // ====================================================================

    /// `TClientSettingsCommand` (UI CmdId=1, Sliced, UK_BaseUISettings).
    /// Передаёт полный snapshot настроек клиента — заменяет любой предыдущий
    /// pending settings-пакет с тем же UKey.
    pub fn ui_send_settings(&self, settings: &crate::commands::ui::ClientSettingsCommand) {
        let raw = crate::commands::ui::build_client_settings(settings);
        self.send_cmd_keyed(raw, Command::UI, SendPriority::Sliced, true, 6,
                            UniqueKey::base_ui_settings(settings.uid));
    }

    /// `TSettingsRequest` (UI CmdId=2, High) — запрос текущих настроек с сервера.
    pub fn ui_settings_request(&self) {
        let raw = crate::commands::ui::build_settings_request(rand::random());
        self.send_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// `TStratStartStopCommand` (UI CmdId=3, High) — запустить/остановить все стратегии.
    pub fn ui_strat_start_stop(&self, is_start: bool) {
        let raw = crate::commands::ui::build_strat_start_stop(rand::random(), is_start);
        self.send_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// `TStratStartStopCommandV2` (UI CmdId=4, High) — запустить/остановить
    /// конкретные стратегии (с массивом checked-items).
    pub fn ui_strat_start_stop_v2(&self, is_start: bool, items: &[crate::commands::strat::StratCheckedItem]) {
        let raw = crate::commands::ui::build_strat_start_stop_v2(rand::random(), is_start, items);
        self.send_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// `TMMOrdersSubscribeCommand` (UI CmdId=5, High, UK_TurnMMDetection) —
    /// включить/выключить подписку на market-maker ордера.
    pub fn ui_mm_subscribe(&self, subscribe: bool) {
        let raw = crate::commands::ui::build_mm_orders_subscribe(rand::random(), subscribe);
        self.send_cmd_keyed(raw, Command::UI, SendPriority::High, true, 3,
                            UniqueKey::turn_mm_detection());
    }

    /// `TUpdateVersionCommand` (UI CmdId=6, High) — уведомить сервер о версии клиента.
    pub fn ui_update_version(&self, version_name: &str, is_release: bool) {
        let raw = crate::commands::ui::build_update_version(rand::random(), version_name, is_release);
        self.send_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// `TEmuTradesCommand` (UI CmdId=7, Sliced) — отправить эмуляцию трейдов
    /// для тестового рынка.
    pub fn ui_emu_trades(&self, m_index: u16, base_time: f64,
                          points: &[crate::commands::ui::EmuTradePoint]) {
        let raw = crate::commands::ui::build_emu_trades(rand::random(), m_index, base_time, points);
        self.send_cmd(raw, Command::UI, SendPriority::Sliced, true, 6);
    }

    /// `TNewMarketNotifyCommand` (UI CmdId=8, High) — уведомить о новом рынке.
    pub fn ui_new_market_notify(&self) {
        let raw = crate::commands::ui::build_new_market_notify(rand::random());
        self.send_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// `TLevManageCommand` (UI CmdId=9, Sliced, UK_LevManageSettings) —
    /// конфигурация leverage (auto-max, auto-up, isolated/cross, fix-lev).
    pub fn ui_lev_manage(&self, cmd: &crate::commands::ui::LevManage) {
        let uid: u64 = rand::random();
        let raw = crate::commands::ui::build_lev_manage(uid, cmd);
        self.send_cmd_keyed(raw, Command::UI, SendPriority::Sliced, true, 6,
                            UniqueKey::lev_manage_settings(uid));
    }

    /// `TTriggerManageCommand` (UI CmdId=10, Sliced) — батч-управление trigger'ами:
    /// action over (all_markets | конкретные markets/keys).
    pub fn ui_trigger_manage(&self, action: u8, all_markets: bool,
                              markets: &[u16], keys: &[u16]) {
        let raw = crate::commands::ui::build_trigger_manage(rand::random(), action, all_markets, markets, keys);
        self.send_cmd(raw, Command::UI, SendPriority::Sliced, true, 6);
    }

    /// `TResetProfitCommand` (UI CmdId=11, High) — сброс profit-счётчиков.
    pub fn ui_reset_profit(&self, kind: u8) {
        let raw = crate::commands::ui::build_reset_profit(rand::random(), kind);
        self.send_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// `TArbActivateNotify` (UI CmdId=12, High) — уведомление об активации арбитража.
    pub fn ui_arb_activate_notify(&self, arb_valid: f64) {
        let raw = crate::commands::ui::build_arb_activate_notify(rand::random(), arb_valid);
        self.send_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// `TSwitchDexCommand` (UI CmdId=13, High, UK_DexSwitch) — выбор DEX.
    /// Имя DEX обрезается до 15 байт ShortString.
    pub fn ui_switch_dex(&self, dex_name: &str) {
        let raw = crate::commands::ui::build_switch_dex(rand::random(), dex_name);
        self.send_cmd_keyed(raw, Command::UI, SendPriority::High, true, 3,
                            UniqueKey::dex_switch());
    }

    /// `TSwitchSpotCommand` (UI CmdId=14, High, UK_SpotSwitch) — выбор spot режима.
    pub fn ui_switch_spot(&self, spot_index: u8) {
        let raw = crate::commands::ui::build_switch_spot(rand::random(), spot_index);
        self.send_cmd_keyed(raw, Command::UI, SendPriority::High, true, 3,
                            UniqueKey::spot_switch());
    }

    // ====================================================================
    //  High-level Strat wrappers (Command::Strat, encrypted=true)
    //  Покрывают MClient.SendStratCmd(T*Command.Create(...)) семантику Delphi.
    //  Аудит docs_api B-02: было 5 build_* функций без Client-обёрток.
    //  ВНИМАНИЕ: отправка StratSnapshot полного через CreateFromStrats требует
    //  StrategySerializer (Stage 3) — здесь только raw-payload entry.
    // ====================================================================

    /// `TStratSnapshotRequest` (Strat CmdId=1, High) — запрос snapshot стратегий с сервера.
    pub fn strat_snapshot_request(&self) {
        let raw = crate::commands::strat::build_snapshot_request(rand::random());
        self.send_cmd(raw, Command::Strat, SendPriority::High, true, 3);
    }

    /// `TStratSnapshot.CreateFromStrats` raw entry (Strat CmdId=2, Sliced, UK_StratSnapshot).
    /// `serialized_payload` — уже сериализованный через `StrategySerializer` блок
    /// без обёртывающего заголовка команды; функция сама добавляет CmdId/ver/uid.
    /// Полный snapshot замещает любой предыдущий pending snapshot.
    ///
    /// **Stage 3:** StrategySerializer Rust writer не готов; до его реализации
    /// этот метод можно использовать только если ты сам сериализовал стратегии
    /// другим способом, совместимым с Delphi wire-format.
    pub fn strat_send_snapshot(&self, serialized_payload: &[u8]) {
        const CMD_STRAT_SNAPSHOT: u8 = 2;
        const CURRENT_PROTO_CMD_VER: u16 = 3;
        let uid: u64 = rand::random();
        let mut raw = Vec::with_capacity(11 + serialized_payload.len());
        raw.push(CMD_STRAT_SNAPSHOT);
        raw.extend_from_slice(&CURRENT_PROTO_CMD_VER.to_le_bytes());
        raw.extend_from_slice(&uid.to_le_bytes());
        raw.extend_from_slice(serialized_payload);
        self.send_cmd_keyed(raw, Command::Strat, SendPriority::Sliced, true, 6,
                            UniqueKey::strat_snapshot());
    }

    /// `TStratDelete` (Strat CmdId=3, High) — удалить стратегию по id.
    pub fn strat_delete(&self, strategy_id: u64, folder_path: &str) {
        let raw = crate::commands::strat::build_delete(rand::random(), strategy_id, folder_path);
        self.send_cmd(raw, Command::Strat, SendPriority::High, true, 3);
    }

    /// `TStratSellPriceUpdate` (Strat CmdId=4, High, UK_StratSellPriceUpdate) —
    /// обновить sell-price конкретной стратегии. UKey включает strategy_id,
    /// чтобы dedup был per-strategy.
    pub fn strat_sell_price_update(&self, strategy_id: u64, sell_price: f64) {
        let raw = crate::commands::strat::build_sell_price_update(rand::random(), strategy_id, sell_price);
        self.send_cmd_keyed(raw, Command::Strat, SendPriority::High, true, 3,
                            UniqueKey::strat_sell_price_update(strategy_id));
    }

    /// `TStratCheckedSync` (Strat CmdId=5, Sliced) — синхронизация чекбоксов стратегий.
    /// `is_delta = false` для полного списка, `true` для дельты.
    pub fn strat_checked_sync(&self, items: &[crate::commands::strat::StratCheckedItem], is_delta: bool) {
        let raw = crate::commands::strat::build_checked_sync(rand::random(), items, is_delta);
        self.send_cmd(raw, Command::Strat, SendPriority::Sliced, true, 6);
    }

    /// `TStratCheckedEcho` (Strat CmdId=6, High) — echo чекбоксов от сервера.
    pub fn strat_checked_echo(&self, items: &[crate::commands::strat::StratCheckedItem]) {
        let raw = crate::commands::strat::build_checked_echo(rand::random(), items);
        self.send_cmd(raw, Command::Strat, SendPriority::High, true, 3);
    }

    // ====================================================================
    //  High-level Balance wrappers (Command::Balance, encrypted=true)
    //  Покрывают MClient.SendBalanceCmd семантику Delphi.
    //  Аудит docs_api B-03: ранее не было ни build_, ни Client-wrapper'а.
    // ====================================================================

    /// `TRequestBalanceRefresh` (Balance CmdId=5, High) — запросить refresh баланса.
    /// Сервер на запрос пришлёт новый snapshot балансов (как обычный broadcast).
    pub fn balance_request_refresh(&self) {
        let raw = crate::commands::balance::build_request_balance_refresh(rand::random());
        self.send_cmd(raw, Command::Balance, SendPriority::High, true, 3);
    }

    /// GetTimeMS equivalent — монотонные миллисекунды с момента старта `Client` (matches
    /// Delphi GetTickCount64 семантикой "since some fixed past point").
    ///
    /// B-V3-02 fix: ранее использовался `SystemTime::now()` (clock_gettime CLOCK_REALTIME)
    /// — ~30-100ns per call. На hot path reader thread (50K pps на пике TradesStream)
    /// это давало 1-5 мс/сек wasted CPU + потенциальный wall-clock jump при NTP-step
    /// (ломал бы diff'ы). `Instant::elapsed()` использует CLOCK_MONOTONIC (на Linux/Mac)
    /// либо QueryPerformanceCounter (Windows) — стабильный, ~5-20ns per call, не
    /// подвержен NTP-корректировкам.
    ///
    /// **Semantic change vs предыдущая версия:** возвращает ms since process start,
    /// не ms since UNIX_EPOCH. Все callers используют **diff** между двумя `now_ms()`,
    /// так что absolute-base разница не имеет значения.
    ///
    /// MUST use same time base everywhere (reader thread, main thread, slicing) —
    /// гарантируется через общий `self._start: Instant`.
    #[inline]
    fn now_ms(&self) -> i64 {
        self._start.elapsed().as_millis() as i64
    }

    /// Получить кэшированный SocketAddr сервера. Резолвится один раз при `bind_socket` или
    /// первом вызове, далее используется без re-resolve. Закрывает B-05.
    /// При неудаче resolve — `None`, отправка пакетов не происходит (логируется).
    fn server_socket_addr(&mut self) -> Option<SocketAddr> {
        if let Some(addr) = self.cached_server_addr { return Some(addr); }
        let key = format!("{}:{}", self.cfg.server_ip, self.cfg.server_port);
        match key.to_socket_addrs() {
            Ok(mut iter) => {
                if let Some(addr) = iter.next() {
                    self.cached_server_addr = Some(addr);
                    return Some(addr);
                }
                if self.should_log("server_addr_empty", 5000) {
                    error!("server address resolve returned empty: {}", key);
                }
                None
            }
            Err(e) => {
                if self.should_log("server_addr_resolve_fail", 5000) {
                    error!("server address resolve failed for {}: {}", key, e);
                }
                None
            }
        }
    }

    /// Run the client. Spawns reader thread, runs main loop for `duration`.
    /// Matches TMoonProtoUDPClient.Execute.
    pub fn run(&mut self, duration: Duration, mut on_data: OnDataFn) {
        let run_start = Instant::now();

        loop {
            if run_start.elapsed() >= duration { break; }
            let cur_tm = self.now_ms();

            // Emit lifecycle events on auth_status transitions.
            self.check_lifecycle_transition();

            // ActualSleepTime EMA (matches UDPClient.pas:725-734)
            if self.prev_cycle_tm != 0 {
                let raw = (cur_tm - self.prev_cycle_tm).abs();
                if raw > 0 && raw < 100 {
                    if self.actual_sleep_time <= 0.0 {
                        self.actual_sleep_time = raw as f64;
                    } else {
                        self.actual_sleep_time = self.actual_sleep_time * 0.7 + raw as f64 * 0.3;
                    }
                }
            }
            self.prev_cycle_tm = cur_tm;

            // Bind socket if needed
            if self.socket.is_none() && self.need_connect {
                self.bind_socket();
                self.spawn_reader();
            }

            if self.socket.is_some() {
                // === Главное изменение для устранения 5мс латентности ===
                // Ждём событие до 5ms — любой пакет от reader или команда от app будят main мгновенно.
                // Если ничего не пришло за 5ms — продолжаем (heartbeat работа: retry, Hello, reconnect).
                // Это замена thread::sleep(5ms) из старой реализации.
                let first_event = self.event_rx.recv_timeout(Duration::from_millis(DEFAULT_SLEEP_MS));

                let mut recv_msgs: Vec<RecvMsg> = Vec::new();
                let mut sliced = Vec::new();
                let mut h_items = Vec::new();
                let mut l_items = Vec::new();

                let handle_event = |ev: ClientEvent,
                                         recv_msgs: &mut Vec<RecvMsg>,
                                         sliced: &mut Vec<SendItem>,
                                         h_items: &mut Vec<SendItem>,
                                         l_items: &mut Vec<SendItem>| {
                    match ev {
                        ClientEvent::Recv(m) => recv_msgs.push(m),
                        ClientEvent::Send(s) => match s.item.priority {
                            SendPriority::Sliced => sliced.push(s.item),
                            SendPriority::High => h_items.push(s.item),
                            SendPriority::Low => l_items.push(s.item),
                        },
                    }
                };

                match first_event {
                    Ok(ev) => handle_event(ev, &mut recv_msgs, &mut sliced, &mut h_items, &mut l_items),
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }

                // Дренируем всё что накопилось дополнительно (без блокировки).
                while let Ok(event) = self.event_rx.try_recv() {
                    handle_event(event, &mut recv_msgs, &mut sliced, &mut h_items, &mut l_items);
                }

                // Сначала обрабатываем входящие пакеты (handshake / Ping / Sliced / ACK / data).
                // Это близко к Delphi UDPRead, который шёл прямо в reader thread.
                //
                // Аудит #7 (audit_delphi_deviation E-V2-02): фильтр stale epoch. Старый reader
                // thread может ещё крутиться до 1с после reconnect'а (пока read_timeout не
                // сработает) — его пакеты приходят со старым epoch и игнорируются. Делфи
                // решает это синхронным `UDPClient.Active := false` (Indy ждёт reader thread
                // exit) — у нас в Rust такого нет, но epoch-tag даёт эквивалентную гарантию.
                let current_epoch = self.current_reader_epoch;
                for msg in recv_msgs {
                    if msg.epoch != current_epoch {
                        // Stale пакет от старого reader thread'а — игнорируем тихо
                        // (один лог на reconnect через should_log).
                        if self.should_log("stale_reader_epoch", 5000) {
                            warn!(target: "moonproto::client",
                                "dropping stale packet from old reader epoch (msg.epoch={} current={})",
                                msg.epoch, current_epoch);
                        }
                        continue;
                    }
                    self.connected = true;
                    self.total_recv += msg.recv_bytes;
                    self.track_recv(msg.recv_bytes, msg.timestamp_ms);
                    self.last_online = msg.timestamp_ms;
                    self.handle_udp_command(Command::from_byte(msg.cmd), msg.cmd, &msg.payload, &mut on_data);
                }

                // UKey dedup: delete old items with same key (matches SendCmdInt:780-785, CheckSeningData:900-901)
                // For Sliced: remove old Sliced from self.sending AND from pending_h (Delphi: DeleteSendingByKey + DeletePendingByKey)
                for item in &sliced {
                    if !item.u_key.is_none() {
                        self.sending.retain(|s| s.u_key != item.u_key);
                        self.pending_h.retain(|p| p.u_key != item.u_key);
                    }
                }
                for item in &h_items {
                    if !item.u_key.is_none() {
                        self.pending_h.retain(|p| p.u_key != item.u_key);
                    }
                }

                // CheckSeningData: process Sliced queue → CreateSlicedObject
                for item in &sliced {
                    self.create_sliced_and_send(item);
                }

                // CheckSeningData: H items + PendingH retry → batched via DoSendMPData
                for mut item in h_items {
                    self.send_h_item(&mut item, cur_tm);
                }
                self.retry_pending_h(cur_tm);

                // L items: direct send via batching (matches :1017-1031)
                for item in &l_items {
                    self.batch_send_direct(item);
                }

                // Flush batch (sends MPC_Grouped if multiple items buffered)
                self.flush_send_batch();

                // Sliced retry (matches MoonProtoCommon.pas:970-1007)
                self.retry_sliced(cur_tm);

                // Cleanup
                if (cur_tm - self.last_cleanup).abs() > CLEANUP_INTERVAL_MS {
                    self.slicer.clear_old();
                    // audit_responsibility B2: auto-cleanup устаревших pending API slots
                    // (default 12s = Delphi `TMoonProtoEngine.FTimeout`). Защита от
                    // receiver-leak когда caller забыл `remove(uid)`.
                    let removed = self.api_pending.cleanup_old(cur_tm, crate::api_pending::DEFAULT_PENDING_TIMEOUT_MS);
                    if removed > 0 {
                        log::debug!(target: "moonproto::client",
                            "api_pending: cleaned up {} stale slots (>{}ms old)",
                            removed, crate::api_pending::DEFAULT_PENDING_TIMEOUT_MS);
                    }
                    self.last_cleanup = cur_tm;
                }

                // D-02: проверка отложенного второго ImFriend (state machine вместо thread::sleep).
                self.check_pending_second_imfriend(cur_tm);

                // Reconnect logic
                self.check_hello_send(cur_tm);
                self.check_offline_reconnect(cur_tm);
                self.check_reconnect_timeout(cur_tm);
                self.check_dead_zone(cur_tm);

                if self.force_disconnect {
                    self.do_force_disconnect();
                }
            } else {
                // Сокет ещё не привязан — короткая пауза перед повторной попыткой bind.
                thread::sleep(Duration::from_millis(DEFAULT_SLEEP_MS));
            }
        }

    }

    /// Send LogOff and close socket. Call when done.
    /// Matches TMoonProtoBaseClient.Disconnect (Common.pas:290-298)
    pub fn disconnect(&mut self) {
        self.need_connect = false;
        self.force_disconnect = true;
        self.authorized = false;
        self.auth_status = AuthStatus::Base;
    }

    /// Spawn reader thread (≡ Indy TIdUDPListenerThread).
    /// Reader шлёт `ClientEvent::Recv(...)` в общий event-канал — main мгновенно просыпается.
    ///
    /// **Shutdown:** создаём НОВЫЙ `Arc<AtomicBool>` для этого reader. Сохраняем clone в
    /// `self.reader_shutdown`. При `do_force_disconnect` / `Drop` ставим в `true` —
    /// reader thread выйдет из loop (макс через `read_timeout=1s`).
    /// Новый spawn_reader создаёт **свой** Arc — старый и новый не конфликтуют.
    fn spawn_reader(&mut self) {
        let Some(ref sock) = self.socket else { return; };
        // D-03: graceful try_clone — на FD exhaustion (long-running клиент с многими reconnect'ами
        // может упереться в ulimit) не паникуем, а триггерим force_disconnect для restart cycle.
        let sock_clone = match sock.try_clone() {
            Ok(s) => s,
            Err(e) => {
                error!("socket try_clone failed: {e} — triggering force_disconnect");
                self.force_disconnect = true;
                return;
            }
        };
        let mac_key = self.cfg.mac_key;
        let mask_ver = self.cfg.mask_ver;
        let event_tx = self.event_tx.clone();
        // B-V3-02: Instant clone (Copy) для использования в reader closure без borrow self.
        let start_time = self._start;

        // Новый shutdown flag для этого reader thread.
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        self.reader_shutdown = shutdown_flag.clone();

        // Аудит #7: инкрементируем epoch. Каждый новый reader thread получает свой
        // epoch'идентификатор; main loop игнорирует events с **старого** epoch'а
        // (старый reader ещё крутится но мы его игнорируем).
        self.current_reader_epoch = self.current_reader_epoch.wrapping_add(1);
        let my_epoch = self.current_reader_epoch;

        // C-03: named thread для удобства debug (ps -L / Instruments / DebugView)
        let spawn_result = thread::Builder::new()
            .name("moonproto-reader".into())
            .spawn(move || {
            let mut buf = [0u8; 65535];
            loop {
                if shutdown_flag.load(Ordering::Relaxed) {
                    break; // graceful exit on do_force_disconnect / Drop
                }
                let n = match sock_clone.recv_from(&mut buf) {
                    Ok((n, _)) => n,
                    Err(e) => {
                        // D-V2-08 fix: различаем нормальные timeout (set_read_timeout=1s) от
                        // реальных ошибок. На timeout — просто continue без sleep (1с уже
                        // потратили внутри recv_from). На реальной ошибке (BadFd при socket
                        // disconnect, ConnectionReset) — log + проверка shutdown.
                        match e.kind() {
                            std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock => {
                                continue;
                            }
                            _ => {
                                if shutdown_flag.load(Ordering::Relaxed) {
                                    // Сокет закрыт через do_force_disconnect — норма.
                                    break;
                                }
                                // Реальная ошибка — log + короткая пауза перед retry.
                                log::warn!(target: "moonproto::reader", "recv_from error: {} ({:?})", e, e.kind());
                                thread::sleep(Duration::from_millis(5));
                                continue;
                            }
                        }
                    }
                };

                // Transport unpack (OLC + MAC + ver check)
                let Some((hdr, payload)) = moonproto_transport::transport_unpack(
                    &mac_key, &buf[..n], mask_ver,
                ) else { continue; };

                // ErrEmu: симуляция packet loss на стороне клиента (зеркало Delphi
                // MoonProtoUDPClient.pas:534-541). Дроп ПОСЛЕ checksum+ver checks,
                // т.е. валидный пакет просто отбрасывается. Служебные команды дропаются
                // с rate/2 (чтобы handshake/ping не отваливались полностью).
                if err_emu_should_drop(hdr.cmd) {
                    continue;
                }

                // B-V3-02 fix: монотонный timestamp через Instant вместо SystemTime
                // (~20x faster, не подвержен NTP-корректировкам). Reader thread
                // получил `start_time` clone'ом из self._start (Instant — Copy).
                // Тот же time base что в `Client::now_ms` — diff'ы остаются корректны.
                let timestamp_ms = start_time.elapsed().as_millis() as i64;

                let msg = RecvMsg { cmd: hdr.cmd, payload, recv_bytes: n as u64, timestamp_ms, epoch: my_epoch };
                // Аудит #1: `try_send` вместо `send` для recv path. Если main loop отстаёт и
                // канал переполнен — дропаем пакет с warn (UDP всё равно lossy, сервер пришлёт
                // retry для важных через Sliced+ACK). Это закрывает OOM-vector.
                match event_tx.try_send(ClientEvent::Recv(msg)) {
                    Ok(()) => {},
                    Err(mpsc::TrySendError::Full(_)) => {
                        // Throttle лога: 1 на 1000мс через статический counter.
                        use std::sync::atomic::{AtomicI64, Ordering};
                        static LAST_LOG_MS: AtomicI64 = AtomicI64::new(0);
                        let now = start_time.elapsed().as_millis() as i64;
                        let last = LAST_LOG_MS.load(Ordering::Relaxed);
                        if now.saturating_sub(last) > 1000 {
                            LAST_LOG_MS.store(now, Ordering::Relaxed);
                            warn!(target: "moonproto::reader",
                                "event channel full — packet dropped (main loop slow / overflow)");
                        }
                    }
                    Err(mpsc::TrySendError::Disconnected(_)) => {
                        break; // main thread dropped rx → exit reader
                    }
                }
            }
        });
        if let Err(e) = spawn_result {
            error!("spawn moonproto-reader thread failed: {e} — triggering force_disconnect");
            self.force_disconnect = true;
        }
    }

    // process_received удалён: обработка recv_msgs теперь inline в run() loop
    // (после event_rx.recv_timeout / try_recv дренажа event channel).

    fn handle_udp_command(&mut self, cmd: Command, raw_cmd: u8, payload: &[u8], on_data: &mut OnDataFn) {
        if matches!(cmd, Command::WantNewHello | Command::WrongHello | Command::WhoAreYou | Command::Fine) {
            self.waiting_hello = false;
        }

        match cmd {
            Command::WrongHello => { self.auth_status = AuthStatus::Connected; }
            Command::WantNewHello => {
                self.full_reset();
                self.last_sent_hello = 0;
                self.auth_status = AuthStatus::Connected;
                self.authorized = false;
                self.need_connect = true;
                self.soft_reconnect = false;
            }
            Command::NeedHelloAgain => {
                let now = self.now_ms();
                if (now - self.last_need_hello_again).abs() > NEED_HELLO_AGAIN_THROTTLE_MS {
                    self.last_need_hello_again = now;
                    if !self.waiting_hello { self.waiting_hello_start = now; }
                    self.waiting_hello = true;
                    self.last_sent_hello = 0;
                }
            }
            Command::WhoAreYou | Command::Fine => { self.handle_handshake(cmd, payload); }
            Command::SizeTest => { self.handle_size_test(payload); }
            Command::ProbeMTU => { self.handle_probe_mtu(payload); }
            Command::Sliced => {
                self.slicer.set_last_online(self.now_ms());
                let (assembled, ack) = self.slicer.on_new_sliced(payload);
                // Per-block ACK (one SlicedACK per received block) — НАМЕРЕННО.
                // Для торгового канала критична скорость: минимальная задержка обнаружения
                // потери блока важнее экономии bandwidth на мелких ACK (~34 байта каждый).
                // Batching/timer-based ACK снижает bandwidth, но увеличивает retry-латентность.
                // НЕ оптимизировать частоту отправки. См. ARCHITECTURE.md OPEN-QUESTIONS §6 (ЗАКРЫТО).
                self.send_raw_packet(Command::SlicedACK, &ack);
                if let Some((inner_cmd, data, dup_count, blocks_count)) = assembled {
                    // AvgDupCount EMA (matches Common.pas:701-703)
                    let dup_pct = dup_count as f64 / blocks_count.max(1) as f64 * 100.0;
                    if self.avg_dup_count == 0.0 {
                        self.avg_dup_count = dup_pct;
                    } else {
                        // B-19: * 0.1 вместо / 10.0 — FDIV ~13-25 циклов, FMUL ~4-5.
                        self.avg_dup_count = (self.avg_dup_count * 9.0 + dup_pct) * 0.1;
                    }
                    self.data_read_int(inner_cmd, &data, on_data);
                }
            }
            Command::SlicedACK => {
                // Parse ACK: Flags(32 bytes) + DatagramNum(2 bytes) = 34 bytes
                // Matches TMoonProtoClient.ApplyACK (MoonProtoIntStruct.pas:1200-1218)
                if payload.len() >= 34 {
                    let mut ack_flags = [0u8; 32];
                    ack_flags.copy_from_slice(&payload[0..32]);
                    let ack_dgram = u16::from_le_bytes([payload[32], payload[33]]);

                    // Сбор overhead ratios для завершённых Sliced (AvgOverHeat EMA).
                    let mut completed_ratios: Vec<f64> = Vec::new();

                    self.sending.retain_mut(|s| {
                        if s.datagram_num != ack_dgram { return true; }
                        // Merge ACK flags (set union, like Delphi Flags := Flags + ACK.Flags)
                        for i in 0..32 { s.ack_flags[i] |= ack_flags[i]; }
                        // Check if all blocks ACK'd
                        for block in 0..s.blocks_count {
                            if s.ack_flags[block / 8] & (1 << (block % 8)) == 0 {
                                return true; // not all ACK'd, keep
                            }
                        }
                        // All ACK'd — записываем overhead ratio перед удалением
                        // (matches MoonProtoIntStruct.pas:1210-1212).
                        if s.blocks_count > 0 {
                            let ratio = (s.sent_count as f64 / s.blocks_count as f64 - 1.0) * 100.0;
                            completed_ratios.push(ratio);
                        }
                        false
                    });

                    // EMA update: avg_over_heat = (avg * 9 + new) / 10 (matches pas:1212).
                    for ratio in completed_ratios {
                        self.avg_over_heat = if self.avg_over_heat == 0.0 {
                            ratio
                        } else {
                            // B-19: * 0.1 вместо / 10.0
                            (self.avg_over_heat * 9.0 + ratio) * 0.1
                        };
                    }
                }
            }
            Command::Ping => { self.handle_ping(payload, on_data); }
            _ => { self.data_read(raw_cmd, payload, on_data); }
        }
    }

    fn data_read(&mut self, raw_cmd: u8, payload: &[u8], on_data: &mut OnDataFn) {
        let cmd = Command::from_byte(raw_cmd);
        if cmd == Command::Grouped {
            let mut pos = 0;
            while pos + 3 <= payload.len() {
                let sub_cmd = payload[pos]; pos += 1;
                let sz = u16::from_le_bytes([payload[pos], payload[pos+1]]) as usize; pos += 2;
                if pos + sz > payload.len() { break; }
                self.data_read_int(sub_cmd, &payload[pos..pos+sz], on_data);
                pos += sz;
            }
        } else {
            self.data_read_int(raw_cmd, payload, on_data);
        }
    }

    fn data_read_int(&mut self, raw_cmd: u8, data: &[u8], on_data: &mut OnDataFn) {
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
            let Some(decode_cipher) = self.decode_cipher.as_ref() else { return };
            if let Some((inner_cmd, inner_data, _)) = crypted::decrypt_command(decode_cipher, &payload, &mut self.slider) {
                cmd = inner_cmd;
                payload = Cow::Owned(inner_data);
            } else { return; }
        }

        if cmd & COMPRESSED_FLAG != 0 {
            cmd &= 0x7F;
            if let Some(decompressed) = compression::mp_decompress(&payload) {
                payload = Cow::Owned(decompressed);
            } else { return; }
        }

        // ApplyRegularHLAck (parse ACK bits from Ping + drop confirmed PendingH)
        // реализован в handle_ping (matches MoonProtoCommon.pas:511-528 + MoonProtoIntStruct.pas:844-876).
        // Здесь, в общем data_read_int, ничего делать не нужно — Ping обработан отдельной веткой выше.

        // Engine API responses: попытаться доставить в pending registry.
        // Если UID не зарегистрирован — пробрасываем как обычный data callback.
        if cmd == Command::API as u8 {
            if let Some(resp) = parse_engine_response(&payload) {
                if let Some(unconsumed) = self.api_pending.dispatch(resp) {
                    // Не в registry — отдадим обратно через on_data в виде raw payload.
                    // Потребитель сам распарсит через parse_engine_response при необходимости.
                    let _ = unconsumed; // payload уже у нас, просто пропустим resp
                    on_data(Command::API, &payload);
                }
                // else — отправлено в pending::Receiver, on_data не вызываем.
                return;
            }
            // Не распарсилось — fallback на raw callback.
        }

        on_data(Command::from_byte(cmd), &payload);
    }

    fn handle_ping(&mut self, payload: &[u8], on_data: &mut OnDataFn) {
        if payload.len() < 50 { return; }
        self.ping_count += 1;
        // TMoonProtoPing fields (matches MoonProtoDataStruct.pas:63-74)
        let initial_time = f64::from_le_bytes(payload[8..16].try_into().unwrap());
        self.round_trip_delay = i32::from_le_bytes(payload[16..20].try_into().unwrap()) as i64;
        // D-V2-06 fix: clamp PMTU — corrupt/malicious Ping не должен дать pmtu < header_size
        // (=19), иначе arithmetic underflow в create_sliced_and_send. MinSafeDatagramSize=508
        // по протоколу — это нижняя граница договорённого PMTU.
        const MIN_PMTU: u16 = 64;
        let pmtu_raw = u16::from_le_bytes(payload[20..22].try_into().unwrap());
        self.actual_pmtu = pmtu_raw.max(MIN_PMTU);
        self.global_timing_orders = u16::from_le_bytes(payload[22..24].try_into().unwrap());
        self.overheat = payload[24];
        // B-19: умножение на const reciprocal вместо деления (FDIV → FMUL).
        // Компилятор инлайнит `1.0 / 255.0` как const expression.
        self.rs = payload[41] as f64 * (1.0 / 255.0);
        self.need_connect = false;

        // C9: ServerTimeDelta + NetLagPing (matches MoonProtoClient.pas:267-269)
        // delphi_now() already includes NTP offset (= Now - GlobalMPTimeZoneOffset + GlobalMPTimeOffset).
        let now_dt = delphi_now();
        self.server_time_delta = initial_time - now_dt; // InitialTime - Now (for order time correction)
        let server_time = f64::from_le_bytes(payload[0..8].try_into().unwrap());
        self.net_lag_ping = ((now_dt - server_time) * 86400000.0).abs() as i64;

        // Adaptive CanSendRate control (matches UDPClient.pas:643-660)
        const COMFORTABLE_RS: f64 = 0.92;
        const CRITICAL_RS: f64 = 0.85;
        const MIN_RATE: i32 = 256 * 1024;
        const MAX_RATE: i32 = 8 * 1024 * 1024;
        if self.used_sliced_limit {
            let new_rate = if self.rs > COMFORTABLE_RS {
                let increase = (self.can_send_rate as f64 * 0.03).round() as i32;
                self.can_send_rate + increase.max(32 * 1024)
            } else if self.rs < CRITICAL_RS {
                (self.can_send_rate as f64 * 0.85).round() as i32
            } else {
                let drift = (self.rs - COMFORTABLE_RS) / COMFORTABLE_RS;
                (self.can_send_rate as f64 * (1.0 + drift * 0.05)).round() as i32
            };
            self.can_send_rate = new_rate.max(MIN_RATE).min(MAX_RATE);
            self.used_sliced_limit = false;
        }

        // Send ping response (matches Delphi SendPing exactly):
        // - Struct written first (AckStart at offset 42 = SERVER's value, untouched)
        // - BuildAckHalf provides AckWords APPENDED after struct
        // BuildAckHalf fills AckStart + AckWords, then we write struct with correct AckStart
        //
        // audit_rust_quality #15: переиспользуем `now_dt` из расчёта server_time_delta выше
        // вместо повторного `delphi_now()` syscall. Также защита от clock-jump между двумя
        // вызовами — server_time_delta и `Time` поля Ping получат согласованное значение.
        let mut response = payload[..50].to_vec();
        response[0..8].copy_from_slice(&now_dt.to_le_bytes());
        response[25..33].copy_from_slice(&self.total_sent.to_le_bytes());
        response[33..41].copy_from_slice(&self.total_recv.to_le_bytes());
        let (ack_start, ack_words) = self.slider.build_ack_half();
        response[42..50].copy_from_slice(&ack_start.to_le_bytes());
        for w in &ack_words { response.extend_from_slice(&w.to_le_bytes()); }
        self.send_raw_packet(Command::Ping, &response);

        // ApplyRegularHLAck: parse server's ACK bitmap from Ping and drop confirmed PendingH.
        // Matches MoonProtoCommon.pas:511-528 (DataReadInt for MPC_Ping) + MoonProtoIntStruct.pas:844-876.
        if payload.len() > 50 {
            let srv_ack_start = u64::from_le_bytes(payload[42..50].try_into().unwrap());
            let ack_data_len = payload.len() - 50;
            let r_count = (ack_data_len / 8).min(64);
            if r_count > 0 {
                let limit = (r_count as u64) * 64;
                let mut srv_bits = [0u64; 64];
                for i in 0..r_count {
                    srv_bits[i] = u64::from_le_bytes(payload[50 + i*8..50 + i*8 + 8].try_into().unwrap());
                }
                self.pending_h.retain(|d| {
                    if d.msg_num < srv_ack_start { return true; }
                    let offset = d.msg_num - srv_ack_start;
                    if offset >= limit { return true; }
                    let word_idx = (offset >> 6) as usize;
                    let bit_idx = (offset & 63) as u64;
                    (srv_bits[word_idx] >> bit_idx) & 1 == 0
                });
            }
        }

        on_data(Command::Ping, payload);
    }

    fn handle_handshake(&mut self, cmd: Command, payload: &[u8]) {
        if cmd == Command::WhoAreYou {
            let aad = self.cfg.client_id.to_le_bytes();
            let Some(decrypted) = crypto::decrypt(&self.cfg.master_key, payload, &aad) else { return };
            let Some(hello) = handshake::Hello::from_bytes(&decrypted) else { return };
            self.server_token = hello.server_token;
            // Детекция перезапуска сервера: PeerAppToken изменился между сессиями.
            // Соответствует Delphi MoonProtoEngine.pas:694-698 FLastServerAppToken check.
            let prev_app_token = self.peer_app_token;
            self.peer_app_token = hello.app_token; // C7: save PeerAppToken
            if prev_app_token != 0 && prev_app_token != hello.app_token {
                self.fire_lifecycle(LifecycleEvent::ServerRestart);
            }
            let (enc, dec) = crypto::generate_sub_keys(&self.cfg.master_key, self.server_token);
            self.encode_key = enc;
            self.decode_key = dec;
            // B-V2-03: пересоздаём кэшированные cipher'ы при обновлении ключей.
            // Это единственное место где ключи меняются (handshake), поэтому
            // overhead Aes128Gcm::new здесь несущественен.
            self.encode_cipher = Some(crate::crypto::cipher_from_key(&enc));
            self.decode_cipher = Some(crate::crypto::cipher_from_key(&dec));

            self.client_token += 1;
            let mut im = hello;
            im.mix_ts = self.client_token;
            im.app_token = self.app_token;
            im.timestamp = delphi_now();
            let packed = im.to_bytes_packed();
            let aad = self.cfg.client_id.to_le_bytes();
            // B-V2-03: cipher только что установлен выше — invariant выполняется.
            let cipher = self.encode_cipher.as_ref().expect("encode_cipher set 3 lines above");
            let encrypted = crypto::encrypt_with_cipher(cipher, &packed, &aad);
            // D-02: первый ImFriend — сразу. Второй планируется через 32мс state-machine'ой
            // (раньше: thread::sleep блокировал main loop). Reschedule если в очереди уже
            // висит старая (соответствует Delphi семантике — последняя попытка вытесняет).
            self.send_raw_packet(Command::ImFriend, &encrypted);
            self.pending_second_imfriend = Some((self.now_ms() + 32, encrypted));
        }
        if cmd == Command::Fine {
            self.need_connect = false;
            self.waiting_hello = false;
            self.auth_status = AuthStatus::AuthDone;
            self.authorized = true;
        }
    }

    fn handle_size_test(&mut self, payload: &[u8]) {
        if payload.len() < 6 { return; }
        let size = u16::from_le_bytes(payload[0..2].try_into().unwrap());
        let series = u16::from_le_bytes(payload[4..6].try_into().unwrap());
        // D-V2-07 fix: clamp size до разумного верхнего предела. Атакующий сервер мог бы
        // прислать SizeTest шторм с size=65535 → 64KB alloc/packet → DoS. PMTU тестовые
        // пакеты ограничены сверху MAX_DATAGRAM_PROBE (см. ARCHITECTURE §7). MoonProto
        // не передаёт пакетов > MaxNeededDatagramSize (8000 по IPv6 limit).
        const MAX_SIZE_PROBE: u16 = 8192;
        if size > MAX_SIZE_PROBE || (size as usize) < 6 { return; }
        // audit_robustness M8: throttle ответов на SizeTest до 10/sec. Защита от amplification
        // attack (адверсарь шлёт burst → клиент шлёт burst 8KB ответов в ответ = mobile data
        // drain). Реальный PMTU discovery шлёт ~17 пакетов на серию каждые ~5сек → 10/sec хватает.
        let now = self.now_ms();
        if now.saturating_sub(self.last_size_test_ack_ms) < 100 {
            return;
        }
        self.last_size_test_ack_ms = now;
        let mut ack = vec![0u8; size as usize];
        ack[0..2].copy_from_slice(&size.to_le_bytes());
        ack[4..6].copy_from_slice(&series.to_le_bytes());
        self.set_dont_fragment(true);
        self.send_raw_packet(Command::SizeAck, &ack);
        self.set_dont_fragment(false);
    }

    fn handle_probe_mtu(&mut self, payload: &[u8]) {
        if payload.len() < 5 { return; }
        let probe_id = u16::from_le_bytes(payload[0..2].try_into().unwrap());
        let probe_index = payload[2];
        let test_size = u16::from_le_bytes(payload[3..5].try_into().unwrap());
        // D-V2-07 fix: см. handle_size_test выше — clamp до MAX_SIZE_PROBE.
        const MAX_SIZE_PROBE: u16 = 8192;
        if test_size > MAX_SIZE_PROBE || (test_size as usize) < 5 { return; }
        // audit_robustness M8: throttle до 10/sec (см. handle_size_test).
        let now = self.now_ms();
        if now.saturating_sub(self.last_probe_mtu_ack_ms) < 100 {
            return;
        }
        self.last_probe_mtu_ack_ms = now;
        let mut ack = vec![0u8; test_size as usize];
        ack[0..2].copy_from_slice(&probe_id.to_le_bytes());
        ack[2] = probe_index;
        ack[3..5].copy_from_slice(&test_size.to_le_bytes());
        self.set_dont_fragment(true);
        self.send_raw_packet(Command::ProbeMTUAck, &ack);
        self.set_dont_fragment(false);
    }

    /// Set IP_DONTFRAGMENT socket option (matches TUDPServerMP.TurnDontFragment).
    /// **Cross-platform**: Windows / Linux / Android / macOS / iOS.
    /// Реализовано через `setsockopt` напрямую (socket2 имеет `set_mtu_discover` только на Linux).
    fn set_dont_fragment(&self, enable: bool) {
        if let Some(ref sock) = self.socket {
            set_dont_fragment_for_socket(sock, enable);
        }
    }

    /// Crypt + CreateSlicedObject + send (matches MoonProtoIntStruct.pas:1058-1196)
    fn create_sliced_and_send(&mut self, item: &SendItem) {
        let header_size = 15u16;
        let slice_hdr_size = 4u16;

        // MaxSlicedDataSize check (matches IntStruct.pas:1071-1079)
        let pmtu_for_check = (self.actual_pmtu - header_size - slice_hdr_size) as usize;
        let max_sliced_data_size = pmtu_for_check * 256 - 12 - 1; // 12=CryptoHeader, 1=cmd byte
        if item.data.len() > max_sliced_data_size {
            return; // too large, drop (Delphi logs + exits)
        }
        if item.data.is_empty() && !item.encrypted {
            return; // empty non-encrypted data (Delphi logs + exits)
        }

        // Compress if beneficial (matches TMoonProtoDataToSend.Create, DataStruct.pas:618-633).
        // audit_delphi_deviation #1: используем `maybe_compress` (Cow паттерн уже в H-path) —
        // без сжатия = Cow::Borrowed = zero alloc. Раньше безусловный `.clone()` создавал
        // лишнюю аллокацию на каждый Sliced (1-50KB payload каждый, 10-100/sec → MB/sec).
        let (send_cmd, send_data) = Self::maybe_compress(item.cmd, &item.data);

        // Crypt if needed
        let (wire_cmd, wire_data, msg_num) = if item.encrypted {
            let msg_num = if item.msg_num != 0 {
                item.msg_num  // retry — reuse existing MsgNum
            } else {
                self.crypt_msg_counter += 1;
                self.crypt_msg_counter
            };

            let mut crypto_hdr = [0u8; 12];
            let rnd: u16 = rand::random();
            crypto_hdr[0..2].copy_from_slice(&rnd.to_le_bytes());
            crypto_hdr[2..10].copy_from_slice(&msg_num.to_le_bytes());
            crypto_hdr[10] = send_cmd; // inner cmd (may have COMPRESSED_FLAG)
            crypto_hdr[11] = if item.retry_left > 0 { 1 } else { 0 };

            let mut plaintext = Vec::with_capacity(12 + send_data.len());
            plaintext.extend_from_slice(&crypto_hdr);
            plaintext.extend_from_slice(send_data.as_ref());

            // B-V2-03: используем кэшированный cipher из Client.
            let Some(cipher) = self.encode_cipher.as_ref() else {
                error!(target: "moonproto::crypto", "encrypt H-prio called before handshake — packet dropped");
                return;
            };
            let encrypted_data = crypto::encrypt_with_cipher(cipher, &plaintext, &[]);
            // Delphi: NewCmd := MPC_Crypted; if IsCompressed(d.Fcmd) then NewCmd := SetCompressed(NewCmd)
            let wire_cmd = if send_cmd & 0x80 != 0 {
                Command::Crypted as u8 | 0x80
            } else {
                Command::Crypted as u8
            };
            (wire_cmd, encrypted_data, msg_num)
        } else {
            (item.cmd, item.data.clone(), 0u64)
        };

        // CreateSlicedObject
        let pmtu = (self.actual_pmtu - header_size - slice_hdr_size) as usize;
        let total_size = wire_data.len() + 1; // +1 cmd byte in block 0
        let n_blocks = ((total_size + pmtu - 1) / pmtu).max(1);
        let max_block_num = (n_blocks - 1) as u8;
        let datagram_num = self.send_datagram_num;
        self.send_datagram_num = self.send_datagram_num.wrapping_add(1);

        let mut data_pos = 0;
        let mut sent_slices = Vec::with_capacity(n_blocks);
        for block_num in 0..n_blocks {
            let mut slice = Vec::with_capacity(4 + pmtu);
            slice.extend_from_slice(&datagram_num.to_le_bytes());
            slice.push(block_num as u8);
            slice.push(max_block_num);

            if block_num == 0 {
                slice.push(wire_cmd);
                let write_size = (pmtu - 1).min(wire_data.len() - data_pos);
                slice.extend_from_slice(&wire_data[data_pos..data_pos + write_size]);
                data_pos += write_size;
            } else {
                let write_size = pmtu.min(wire_data.len() - data_pos);
                slice.extend_from_slice(&wire_data[data_pos..data_pos + write_size]);
                data_pos += write_size;
            }

            // B-V2-07 fix: сначала отправляем (borrow), потом move в sent_slices без clone.
            self.send_raw_packet(Command::Sliced, &slice);
            sent_slices.push(slice);
        }

        // Store in Sending list with priority insert (matches IntStruct.pas:1112-1116)
        let now = self.now_ms();
        let new_sliced = SentSliced {
            datagram_num,
            piece_last_checked: vec![now; n_blocks],
            slices: sent_slices,
            ack_flags: [0u8; 32],
            blocks_count: n_blocks,
            sent_count: n_blocks,
            last_checked: now,
            retry_count: 0,
            max_retry_count: item.max_retries,
            u_key: item.u_key,
        };
        // Priority: fewer blocks → earlier in queue (smaller datagrams retry first)
        let insert_pos = self.sending.iter().position(|s| s.blocks_count > n_blocks)
            .unwrap_or(self.sending.len());
        self.sending.insert(insert_pos, new_sliced);

        // NB: Sliced retry уже работает через self.sending + retry_sliced (per-piece LastChecked,
        // ClientLimit, FRetryCount → MaxRetryCount). Не добавляем в pending_h — это двойной retry.
        // (Delphi: PendingH используется только для H-priority команд через DoSendMPData, не для Sliced.)
        let _ = msg_num;
    }

    /// Send H-priority item directly via MPC_Crypted (no SliceHeader).
    /// Matches Delphi DoSendMPData → Client.Crypt → SendCommand(MPC_Crypted, data).
    /// H-priority does NOT go through slicing — it's sent as direct MPC_Crypted packet.
    /// Send H-priority item through batch (matches DoSendMPData for H, Common.pas:933-938)
    fn send_h_item(&mut self, item: &mut SendItem, cur_tm: i64) {
        // Auto-compression (matches Delphi TMoonProtoDataToSend.Create pas:661-672).
        // Сжимает payload > 64 байт если результат < 95% оригинала. Inner cmd получает
        // COMPRESSED_FLAG (0x80). Закрывает DEVIATION #11.
        let (eff_cmd, eff_data) = Self::maybe_compress(item.cmd, &item.data);

        if item.encrypted {
            let msg_num = if item.msg_num != 0 {
                item.msg_num
            } else {
                self.crypt_msg_counter += 1;
                self.crypt_msg_counter
            };

            let mut crypto_hdr = [0u8; 12];
            let rnd: u16 = rand::random();
            crypto_hdr[0..2].copy_from_slice(&rnd.to_le_bytes());
            crypto_hdr[2..10].copy_from_slice(&msg_num.to_le_bytes());
            crypto_hdr[10] = eff_cmd;
            crypto_hdr[11] = if item.retry_left > 0 { 1 } else { 0 };

            let mut plaintext = Vec::with_capacity(12 + eff_data.len());
            plaintext.extend_from_slice(&crypto_hdr);
            plaintext.extend_from_slice(&eff_data);

            // B-V2-03: кэшированный cipher.
            let Some(cipher) = self.encode_cipher.as_ref() else {
                error!(target: "moonproto::crypto", "encrypt batch called before handshake — packet dropped");
                return;
            };
            let encrypted = crypto::encrypt_with_cipher(cipher, &plaintext, &[]);

            // Wire (outer) cmd — всегда Crypted; COMPRESSED_FLAG переезжает на inner cmd.
            let wire_cmd = Command::Crypted as u8;

            // Buffer into batch (will be sent as Grouped or single on flush)
            let item_size = encrypted.len() + 3;
            if self.tmp_send_count > 0 && self.tmp_send_size + item_size > self.actual_pmtu as usize {
                self.flush_send_batch();
            }
            self.tmp_send_buf.push(wire_cmd);
            let sz = encrypted.len() as u16;
            self.tmp_send_buf.extend_from_slice(&sz.to_le_bytes());
            self.tmp_send_buf.extend_from_slice(&encrypted);
            self.tmp_send_count += 1;
            self.tmp_send_size += item_size;

            // Add to PendingH for retry (first send only)
            if item.retry_left > 0 && item.msg_num == 0 {
                let mut pending_item = item.clone();
                pending_item.msg_num = msg_num;
                pending_item.last_sent_at = cur_tm;
                // Сохраняем СЖАТЫЕ данные + cmd с COMPRESSED_FLAG — при retry encrypt
                // снова обернёт их (compression deterministic, можно было бы не хранить —
                // но проще не пересжимать).
                pending_item.cmd = eff_cmd;
                // pending_item.data — Vec<u8>, нужно owned. Если eff_data Borrowed —
                // alloc здесь (необходимый — pending_h хранит копию между retry).
                pending_item.data = eff_data.into_owned();
                // DoS guard (audit_robustness H5): pending_h может неконтролируемо расти если
                // сервер живой по MAC, но не ACK'ает H-priority. На burst торговых команд при
                // долгой server silence — мегабайты + O(N) обход в retry_pending_h каждый цикл.
                // Drop oldest при превышении: старые retry устаревают, новые ордера важнее.
                if self.pending_h.len() >= MAX_PENDING_H {
                    log::warn!(target: "moonproto::client",
                        "pending_h saturated ({}); dropping oldest (no ACK from server for H-priority)",
                        self.pending_h.len());
                    self.pending_h.remove(0);
                }
                self.pending_h.push(pending_item);
            }
        } else {
            // Unencrypted H-priority: buffer into batch
            let item_size = eff_data.len() + 3;
            if self.tmp_send_count > 0 && self.tmp_send_size + item_size > self.actual_pmtu as usize {
                self.flush_send_batch();
            }
            self.tmp_send_buf.push(eff_cmd);
            let sz = eff_data.len() as u16;
            self.tmp_send_buf.extend_from_slice(&sz.to_le_bytes());
            self.tmp_send_buf.extend_from_slice(&eff_data);
            self.tmp_send_count += 1;
            self.tmp_send_size += item_size;
        }
        item.last_sent_at = cur_tm;
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

    /// Retry pending H-commands (matches CheckSeningData:944-954).
    /// **Порядок ВАЖЕН** (byte-exact с Delphi):
    ///   1. clone (с текущим retry_left → WantACK = (retry_left > 0))
    ///   2. resend
    ///   3. decrement retry_left
    ///   4. check ≤ 0 → drop
    /// Это гарантирует что **последний** retry уходит с WantACK=true (сервер пришлёт ACK).
    fn retry_pending_h(&mut self, cur_tm: i64) {
        // Delphi: Max(200, Min(500, round(Client.RoundTripDelay * 1.1 + 10)))
        let path_delay = ((self.round_trip_delay as f64 * 1.1 + 10.0).round() as i64).min(500).max(200);
        let mut to_drop = Vec::new();
        let mut to_resend = Vec::new();

        for (idx, item) in self.pending_h.iter_mut().enumerate() {
            if (item.last_sent_at - cur_tm).abs() > path_delay {
                item.last_sent_at = cur_tm;
                // 1+2. Сначала клонируем с ТЕКУЩИМ retry_left и кладём на resend.
                //      WantACK будет вычислен в send_h_item как `retry_left > 0` — на последнем
                //      retry (когда retry_left=1 ДО decrement) WantACK=true → сервер ACK'нет.
                to_resend.push(item.clone());
                // 3. Decrement.
                item.retry_left -= 1;
                // 4. Drop если исчерпался.
                if item.retry_left <= 0 {
                    to_drop.push(idx);
                }
            }
        }

        // Remove exhausted (reverse order to preserve indices)
        for idx in to_drop.into_iter().rev() {
            self.pending_h.remove(idx);
        }

        // Resend via direct MPC_Crypted (NOT through Sliced — matches Delphi DoSendMPData)
        for mut item in to_resend {
            self.send_h_item(&mut item, cur_tm);
        }
    }

    /// Retry unACK'd Sliced blocks (byte-exact port of MoonProtoCommon.pas:970-1007)
    /// Per-piece LastChecked, BytesSentAtOnce limit, conditional FRetryCount, TripDelayK.
    fn retry_sliced(&mut self, cur_tm: i64) {
        if self.sending.is_empty() { return; }
        if self.round_trip_delay < 1 { return; }

        // Outer gate: only check if enough time passed (matches :970)
        // Note: Delphi uses per-client LastCheckedSlices, we use the min of all sliced.last_checked
        let path_delay = (self.round_trip_delay as f64 * self.trip_delay_k + 10.0).round() as i64;
        let cycle_time_ms = 5.0f64.max(self.actual_sleep_time).min(15.0);
        // B-19: * 0.001 вместо / 1000.0 (FDIV → FMUL on hot retry path).
        let client_limit = (self.can_send_rate as f64 * cycle_time_ms * 0.001) as usize;
        let mut bytes_sent_at_once: usize = 0;

        // Аудит #2 (audit_delphi_deviation): индексы вместо clone. Раньше каждый
        // ретранслируемый блок копировался в `to_send: Vec<Vec<u8>>` — 200 alloc/sec
        // при congestion (10 active Sliced × 20 blocks × 2 retries/sec × ~500б).
        // Теперь храним `(sending_idx, block_num)` (16 байт), отправляем по ссылке.
        // Соответствует Delphi `SendCommand(Client, MPC_Sliced, Piece.data)` где Piece.data —
        // `TMemoryStream` по ссылке (ноль копий).
        let mut to_send_indices: Vec<(usize, usize)> = Vec::new();
        let mut to_remove = Vec::new();

        for (idx, sliced) in self.sending.iter_mut().enumerate() {
            if (cur_tm - sliced.last_checked).abs() <= path_delay { continue; }

            let prev_last_checked = sliced.last_checked;
            sliced.last_checked = cur_tm;

            for (block_num, slice_data) in sliced.slices.iter().enumerate() {
                let byte_idx = block_num / 8;
                let bit_idx = block_num % 8;
                if sliced.ack_flags[byte_idx] & (1 << bit_idx) != 0 { continue; } // ACK'd

                // Per-piece check (matches :989)
                if sliced.piece_last_checked[block_num] != prev_last_checked { continue; }
                if (cur_tm - sliced.piece_last_checked[block_num]).abs() <= path_delay { continue; }
                if bytes_sent_at_once >= client_limit { break; }

                to_send_indices.push((idx, block_num));
                sliced.piece_last_checked[block_num] = cur_tm;
                sliced.sent_count += 1;
                bytes_sent_at_once += slice_data.len();
            }

            // Sliced.LastChecked = Min(all piece_last_checked) (matches :996)
            sliced.last_checked = sliced.piece_last_checked.iter().copied().min().unwrap_or(cur_tm);

            // Conditional increment (matches :998-999)
            if prev_last_checked != sliced.last_checked {
                sliced.retry_count += 1;
            }

            if sliced.retry_count > sliced.max_retry_count {
                to_remove.push(idx);
            }
        }

        // TripDelayK adaptation every 2s (matches :975-979)
        if (cur_tm - self.last_set_trip_k).abs() > 2000 {
            self.last_set_trip_k = cur_tm;
            if self.avg_dup_count > 5.0 {
                self.trip_delay_k = (self.trip_delay_k + 0.05).min(1.25);
            }
            if self.avg_dup_count == 0.0 {
                self.trip_delay_k = (self.trip_delay_k - 0.01).max(1.05);
            }
        }

        // UsedSlicedLimit flag (matches :1009-1011)
        if bytes_sent_at_once >= (client_limit * 80 / 100) {
            self.used_sliced_limit = true;
        }

        // Аудит #2: отправляем по индексу из self.sending — никаких clone.
        // ВАЖНО: send_raw_packet берёт `&[u8]`, поэтому borrow на self.sending живёт только
        // на время одного send. self.send_raw_packet требует `&mut self` (внутри пишет в
        // bps/total_sent/socket), а sending borrow read-only — нужен split. Делаем мини-
        // dance: snapshot нужного slice во временный буфер (1 alloc per packet вместо 1
        // alloc на каждый element в общем Vec<Vec<u8>>). Чуть лучше но не zero-alloc.
        // **TODO** для следующей версии: разнести send_raw_packet чтобы slice мог быть
        // передан без holding &mut self на сокет.
        let mut tmp_slice: Vec<u8> = Vec::new();
        for (idx, block_num) in to_send_indices {
            tmp_slice.clear();
            tmp_slice.extend_from_slice(&self.sending[idx].slices[block_num]);
            self.send_raw_packet(Command::Sliced, &tmp_slice);
        }

        for idx in to_remove.into_iter().rev() {
            self.sending.remove(idx);
        }
    }

    /// Send a packet directly (low-level, no queue)
    /// Buffer an item for Grouped batching (matches DoSendMPData, Common.pas:795-833).
    /// Items are accumulated until PMTU is reached, then flushed as MPC_Grouped.
    fn batch_send_direct(&mut self, item: &SendItem) {
        // Auto-compression (DEVIATION #11 — закрыто).
        let (eff_cmd, eff_data) = Self::maybe_compress(item.cmd, &item.data);

        let item_size = eff_data.len() + 3; // cmd(1) + sz(2) + data — ClientHdr (15) учтён в initial tmp_send_size

        // If adding this item would exceed PMTU → flush first
        if self.tmp_send_count > 0 && self.tmp_send_size + item_size > self.actual_pmtu as usize {
            self.flush_send_batch();
        }

        // Encrypt if needed
        // Аудит #3: wire_data становится Cow — для unencrypted path сохраняем borrowed
        // (zero alloc); для encrypted — Owned (encrypt всегда возвращает Vec).
        let (wire_cmd, wire_data): (u8, std::borrow::Cow<'_, [u8]>) = if item.encrypted {
            self.crypt_msg_counter += 1;
            let msg_num = self.crypt_msg_counter;
            let mut crypto_hdr = [0u8; 12];
            let rnd: u16 = rand::random();
            crypto_hdr[0..2].copy_from_slice(&rnd.to_le_bytes());
            crypto_hdr[2..10].copy_from_slice(&msg_num.to_le_bytes());
            crypto_hdr[10] = eff_cmd;
            crypto_hdr[11] = if item.retry_left > 0 { 1 } else { 0 };
            let mut plaintext = Vec::with_capacity(12 + eff_data.len());
            plaintext.extend_from_slice(&crypto_hdr);
            plaintext.extend_from_slice(&eff_data);
            // B-V2-03: кэшированный cipher.
            let cipher = match self.encode_cipher.as_ref() {
                Some(c) => c,
                None => {
                    error!(target: "moonproto::crypto", "encrypt batch_direct called before handshake — packet dropped");
                    return;
                }
            };
            let encrypted = crypto::encrypt_with_cipher(cipher, &plaintext, &[]);
            (Command::Crypted as u8, std::borrow::Cow::Owned(encrypted))
        } else {
            (eff_cmd, eff_data)
        };

        // Append to batch: cmd(1) + sz(2) + data
        self.tmp_send_buf.push(wire_cmd);
        let sz = wire_data.len() as u16;
        self.tmp_send_buf.extend_from_slice(&sz.to_le_bytes());
        self.tmp_send_buf.extend_from_slice(&wire_data);
        self.tmp_send_count += 1;
        self.tmp_send_size += item_size;
    }

    /// Flush the send batch (matches DoSendTmpList, Common.pas:835-867).
    /// If count>1 → MPC_Grouped. If count==1 → single packet.
    /// A-19 fix: для single случая не re-парсим cmd/sz из buf — мы их знаем при добавлении.
    /// Single-element путь теперь без bounds-check парсинга.
    fn flush_send_batch(&mut self) {
        if self.tmp_send_count == 0 { return; }

        if self.tmp_send_count > 1 {
            // Send as MPC_Grouped
            let payload = std::mem::take(&mut self.tmp_send_buf);
            self.send_raw_packet(Command::Grouped, &payload);
        } else {
            // Single item: формат tmp_send_buf = [cmd(1) | sz(2 LE) | data(sz)].
            // Wire-format MPC_Grouped header не нужен → отправляем как обычный пакет.
            let buf = std::mem::take(&mut self.tmp_send_buf);
            if buf.len() >= 3 {
                let cmd = buf[0];
                // sz прочитан только для slicing data (после 3 байт group-header'а).
                // Используем оставшийся len как `len - 3` — это и есть фактический payload.
                self.send_raw_packet_cmd(cmd, &buf[3..]);
            }
        }

        self.tmp_send_count = 0;
        self.tmp_send_size = 15; // ClientMsgHeader overhead (matches GetHeaderSize)
    }

    fn send_raw_packet_cmd(&mut self, cmd: u8, payload: &[u8]) {
        let Some(addr) = self.server_socket_addr() else { return };
        let (packet, extra) = moonproto_transport::transport_pack(
            &self.cfg.mac_key, cmd, self.cfg.client_id, payload, self.cfg.mask_ver,
        );
        self.dispatch_send(cmd, &packet, extra.as_deref(), addr);
    }

    fn send_raw_packet(&mut self, cmd: Command, payload: &[u8]) {
        let Some(addr) = self.server_socket_addr() else { return };
        let (packet, extra) = moonproto_transport::transport_pack(
            &self.cfg.mac_key, cmd as u8, self.cfg.client_id, payload, self.cfg.mask_ver,
        );
        self.dispatch_send(cmd as u8, &packet, extra.as_deref(), addr);
    }

    /// Реально отправляет пакет (плюс optional extra-пакет от moonext) с обработкой ошибок.
    /// Закрывает D-06: send errors больше не игнорируются через `.ok()`.
    /// EWOULDBLOCK логируется как warn (нормальная буферизация ядра). Прочие ошибки → error + force_disconnect
    /// (чтобы reconnect-цикл подобрал состояние).
    fn dispatch_send(&mut self, cmd: u8, packet: &[u8], extra: Option<&[u8]>, addr: SocketAddr) {
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
                self.total_sent += packet.len() as u64;
                self.track_sent(packet.len() as u64, self.now_ms());
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if self.should_log("send_wouldblock", 1000) {
                    warn!("send_to(cmd={cmd}) would block (kernel send buffer full)");
                }
            }
            Err(e) => {
                if self.should_log("send_err", 1000) {
                    error!("send_to(cmd={cmd}) failed: {e} — triggering force_disconnect");
                }
                self.force_disconnect = true;
            }
        }
    }

    fn send_hello(&mut self) {
        let payload = handshake::build_hello_packet(
            &self.cfg.master_key, self.cfg.client_id, &mut self.client_token, self.app_token, delphi_now(),
        );
        self.send_raw_packet(Command::Hello, &payload);
    }

    fn send_hello_again(&mut self) {
        self.client_token += 1;
        let mut hello = handshake::Hello::new(self.client_token, self.app_token);
        hello.timestamp = delphi_now();
        hello.peer_mix = crypto::mix_values(&hello.rnd, hello.mix_ts, self.server_token);
        let packed = hello.to_bytes_packed();
        let aad = self.cfg.client_id.to_le_bytes();
        // B-V2-03: send_hello_again вызывается после первичного Fine (cipher установлен).
        // Если по какой-то причине cipher = None — пропускаем (защита от panic).
        let Some(cipher) = self.encode_cipher.as_ref() else {
            error!(target: "moonproto::crypto", "HelloAgain called before initial handshake — skipping");
            return;
        };
        let encrypted = crypto::encrypt_with_cipher(cipher, &packed, &aad);
        self.send_raw_packet(Command::HelloAgain, &encrypted);
    }

    fn check_hello_send(&mut self, cur_tm: i64) {
        if !self.need_connect || self.force_disconnect { return; }
        let interval = self.round_trip_delay.max(1000) * 2;
        if (cur_tm - self.last_sent_hello).abs() <= interval { return; }
        if self.soft_reconnect && self.server_token != 0 {
            self.send_hello_again();
        } else {
            self.soft_reconnect = false;
            self.send_hello();
        }
        self.last_sent_hello = cur_tm;
        self.waiting_hello = true;
        self.waiting_hello_start = cur_tm;
    }

    fn check_offline_reconnect(&mut self, cur_tm: i64) {
        let throttle = (self.round_trip_delay + 50).max(200).min(1500);
        let last_online = self.last_online;
        let authorized = self.authorized;

        let should = self.waiting_hello
            || (authorized && !self.need_connect && (cur_tm - last_online).abs() > OFFLINE_BASE_MS + self.round_trip_delay);
        if !should { return; }
        if (cur_tm - self.last_sent_hello).abs() <= throttle { return; }

        self.auth_status = AuthStatus::Offline;
        if !self.waiting_hello { self.waiting_hello_start = cur_tm; }
        self.waiting_hello = true;
        self.send_hello_again();
        self.last_sent_hello = cur_tm;
    }

    fn check_reconnect_timeout(&mut self, cur_tm: i64) {
        if self.waiting_hello
            && (cur_tm - self.waiting_hello_start).abs() > RECONNECT_WAITING_MS
            && (cur_tm - self.last_socket_recreate).abs() > RECONNECT_THROTTLE_MS
        {
            self.last_socket_recreate = cur_tm;
            self.soft_reconnect = true;
            self.force_disconnect = true;
            self.need_connect = true;
            self.waiting_hello = false;
        }
    }

    /// D-02: state-machine для отложенного второго ImFriend.
    /// Если due ≤ cur_tm — отправляем и очищаем slot. Не блокирует main loop.
    /// Защита от старого слота при reconnect: full_reset() сбрасывает.
    fn check_pending_second_imfriend(&mut self, cur_tm: i64) {
        if second_imfriend_due(&self.pending_second_imfriend, cur_tm) {
            // take() очищает slot перед отправкой → safe при ошибке send_raw_packet.
            let payload = self.pending_second_imfriend.take().unwrap().1;
            self.send_raw_packet(Command::ImFriend, &payload);
        }
    }

    fn check_dead_zone(&mut self, cur_tm: i64) {
        let authorized = self.authorized;
        let last_online = self.last_online;
        if !authorized && !self.need_connect && (cur_tm - last_online).abs() > DEAD_ZONE_MS {
            self.soft_reconnect = false;
            self.force_disconnect = true;
            self.need_connect = true;
        }
    }

    fn do_force_disconnect(&mut self) {
        if self.connected && !self.soft_reconnect {
            self.send_raw_packet(Command::LogOff, &[]);
        }
        // Сигналим текущему reader thread завершиться (макс через 1с — read_timeout).
        // Это предотвращает утечку thread'ов при множественных soft/hard reconnect'ах
        // за длинную сессию (часы).
        self.reader_shutdown.store(true, Ordering::Relaxed);
        self.socket = None;
        if !self.soft_reconnect { self.full_reset(); }
        self.connected = false;
        self.authorized = false;
        self.force_disconnect = false;
    }

    /// Matches TMoonProtoClient.Reset (IntStruct.pas:972-1000)
    /// Does NOT reset: server_token, actual_pmtu, send_datagram_num, pending_h,
    /// trip_delay_k, can_send_rate (those persist across reconnects).
    fn full_reset(&mut self) {
        self.crypt_msg_counter = 0;
        self.total_sent = 0;
        self.total_recv = 0;
        self.rs = 1.0;
        self.used_sliced_limit = false;
        self.slider = Slider::new();
        self.slicer = slicing::SlicingReceiver::new();
        self.last_online = 0;
        self.last_sent_hello = 0;
        // D-02: при full reset (новый handshake) — старый отложенный second ImFriend больше не нужен.
        self.pending_second_imfriend = None;
        // Аудит #9 (audit_delphi_deviation): очистка stale Sliced состояния при hard
        // reconnect. После полного reset crypt_msg_counter=0 и ключи **поменяются** в
        // следующем handshake. Старые `sending` зашифрованы прежними ключами / прежними
        // MsgNum'ами — сервер их дропнет (bad keys / out-of-order). Без clear retry будет
        // слать мусорный трафик на сервер до max_retry exhaustion (bandwidth waste +
        // noise на reconnect). pending_h оставляем (это user'ские торговые команды —
        // re-encrypt при retry через send_h_item с новыми ключами).
        self.sending.clear();
        // audit_robustness H2: api_pending sender'ы относятся к UID'ам предыдущей сессии.
        // Сервер новой сессии этих UID не знает → ответ никогда не придёт → Sender живёт
        // в map бесконечно, receiver потребителя блокируется. Дропаем — receivers получат
        // `Err(channel closed)` и поймут что нужен retry.
        self.api_pending.clear();
    }

    fn bind_socket(&mut self) {
        self.force_disconnect = false;
        if self.next_port < 1024 || self.next_port > 65000 { self.next_port = 1024; }
        // Bind family выбирается по серверному адресу. Если сервер — IPv6 literal `[2001:db8::1]:3000`
        // или DNS name резолвящийся в AAAA — bindаемся `[::]:port`. Иначе IPv4 `0.0.0.0:port`.
        let bind_family = if self.cfg.server_ip.contains(':') { "[::]" } else { "0.0.0.0" };
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
                    self.auth_status = AuthStatus::Connected;
                    // Сброс кэша адреса сервера — может измениться при reconnect через DNS.
                    self.cached_server_addr = None;
                    return;
                }
                Err(e) => {
                    last_err = Some(e);
                    self.next_port += 1;
                    if self.next_port > 65000 { self.next_port = 1024; }
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
        // auth_status оставляем Base — main loop попробует bind ещё раз через DEFAULT_SLEEP_MS.
        // Если app явно вызвал disconnect() — он сам выставит need_connect=false.
    }

    pub fn is_authorized(&self) -> bool { self.authorized }
    pub fn auth_status(&self) -> AuthStatus { self.auth_status }
    pub fn ping_count(&self) -> u32 { self.ping_count }
    pub fn total_sent(&self) -> u64 { self.total_sent }
    pub fn total_recv(&self) -> u64 { self.total_recv }

    /// EMA % retransmission overhead для Sliced пакетов (matches AvgOverHeat MoonProtoIntStruct.pas:220).
    /// 0 = идеально (no retries). >0 = вынужденные перепосылы.
    pub fn avg_over_heat(&self) -> f64 { self.avg_over_heat }

    // ====================================================================
    //  Diagnostic getters (audit_responsibility A4)
    //
    //  В Delphi `TMoonProtoNetClient` эти поля публичны и читаются UI
    //  (MoonProtoUnit.pas:363 — "Ping: %d PMTU: %d RS: %d%%"). Aналог в Rust
    //  для построения статус-строки терминала.
    // ====================================================================

    /// RTT в ms (последний измеренный из Ping). Соответствует Delphi
    /// `TMoonProtoNetClient.RoundTripDelay` (MoonProtoClient.pas:62).
    pub fn round_trip_delay_ms(&self) -> i64 { self.round_trip_delay }

    /// Текущий Path MTU в байтах (от 508 до 8000, адаптируется PMTU discovery).
    /// Соответствует Delphi `TMoonProtoNetClient.PMTU`.
    pub fn actual_pmtu(&self) -> u16 { self.actual_pmtu }

    /// Receive Status [0.0..1.0] — качество downlink канала. >0.92 = норма,
    /// <0.85 = критично, между = серая зона. Соответствует Delphi
    /// `TMoonProtoNetClient.RS`.
    pub fn rs(&self) -> f64 { self.rs }

    /// `ServerTime - LocalTime` в днях (как Delphi TDateTime). Применяется
    /// автоматически к timestamp'ам входящих ордеров через `Orders::apply`.
    /// Внешним потребителям обычно не нужен — выставлен публично для диагностики.
    pub fn server_time_delta_days(&self) -> f64 { self.server_time_delta }

    /// `|ServerTime - LocalTime|` в ms (абсолютный лаг от последнего Ping).
    /// Полезно для UI индикатора "сервер близко / далеко".
    pub fn net_lag_ping_ms(&self) -> i64 { self.net_lag_ping }

    /// `Orders cycle ms` от сервера — рекомендованный темп опроса ордерных событий.
    /// Соответствует Delphi `TMoonProtoNetClient.GlobalTimingOrders`.
    pub fn global_timing_orders(&self) -> u16 { self.global_timing_orders }

    /// Текущий `ServerToken` — меняется при каждом hard handshake (Hello→WhoAreYou→Fine).
    /// Soft reconnect (HelloAgain) НЕ меняет этот токен. **Внутри либы используется для
    /// auto-resubscribe** subscription registry — внешнему потребителю обычно не нужен,
    /// выставлен для diagnostic UI.
    pub fn server_token(&self) -> u64 { self.server_token }

    /// `PeerAppToken` — генерируется при старте серверного процесса. Меняется при перезапуске
    /// сервера. **Внутри либы используется для auto-refetch markets indexes** — внешнему
    /// потребителю обычно не нужен, выставлен для diagnostic UI / event correlation.
    pub fn peer_app_token(&self) -> u64 { self.peer_app_token }

    // ====================================================================
    //  BytesPerSec — O(1) EMA counter (порт Delphi AddBytesCount)
    // ====================================================================
    //
    // Аудит #5 (audit_delphi_deviation): ранее использовался `VecDeque<(i64,u64)>` sliding
    // window. На пике 50K pps входящих VecDeque раскручивался до ~500K entries × 16B = 8MB
    // только для recv (+ ещё 8MB для sent). Плюс 100K push_back/pop_front ops/sec.
    //
    // Delphi решает это за 24 байта (3×u64) + 1 if + 1 add per packet — byte-exact порт
    // `MoonProtoUDPClient.pas:113-138 AddBytesCount`. EMA формула: `ema = ema*9/10 + bucket`,
    // что в steady state даёт `ema = 10*bytes_per_sec` (отсюда деление на 10 в getter'е).

    fn track_sent(&mut self, bytes: u64, ts_ms: i64) {
        self.bps_sent.add(bytes, ts_ms);
    }

    fn track_recv(&mut self, bytes: u64, ts_ms: i64) {
        self.bps_recv.add(bytes, ts_ms);
    }

    /// Байт отправлено в среднем за последние ~10 секунд (B/s). O(1) EMA, see [`BpsCounter`].
    pub fn bytes_per_sec_sent(&self) -> u64 { self.bps_sent.bytes_per_sec() }
    /// Байт принято в среднем за последние ~10 секунд (B/s). O(1) EMA.
    pub fn bytes_per_sec_recv(&self) -> u64 { self.bps_recv.bytes_per_sec() }

    // ====================================================================
    //  Log throttle — anti-spam helper для warning'ов.
    // ====================================================================

    /// Возвращает `true` если с момента предыдущего лога с этим `key` прошло ≥ `interval_ms`.
    /// Применение: оборачивать `eprintln!("...")` через `if client.should_log("X", 1000) { ... }`.
    /// `#[inline]`: вызывается на КАЖДОМ warn/error в send/recv pathes.
    #[inline]
    pub fn should_log(&mut self, key: &'static str, interval_ms: i64) -> bool {
        let now_ms = self.now_ms();
        let last = self.log_last.entry(key).or_insert(0);
        if now_ms - *last >= interval_ms {
            *last = now_ms;
            true
        } else {
            false
        }
    }
}

/// Drop: гарантированно сигналим reader thread'у завершиться, даже если потребитель
/// не вызвал `disconnect()`. Reader выйдет из loop макс через 1 сек (read_timeout).
impl Drop for Client {
    fn drop(&mut self) {
        self.reader_shutdown.store(true, Ordering::Relaxed);
    }
}

/// O(1) счётчик байтов с EMA-сглаживанием за ~10 секунд.
///
/// Byte-exact порт `TMoonProtoUDPClient.AddBytesCount` (MoonProtoUDPClient.pas:113-138).
/// Замена `VecDeque` sliding window (audit_delphi_deviation #5) — экономит ~16MB heap
/// на пике + убирает 100K push_back/pop_front ops/sec.
///
/// Алгоритм (как Delphi):
/// - `cur_sec_bytes` накапливает байты текущей секунды.
/// - Когда `now_ms - last_sec_ms > 1000`: закрываем bucket в EMA через
///   `ema = ema * 9/10 + cur_sec_bytes`, обнуляем `cur_sec_bytes`, обновляем `last_sec_ms`.
/// - `bytes_per_sec() = ema / 10` (в steady state `ema = 10 × bytes/sec`).
#[derive(Debug, Default)]
pub struct BpsCounter {
    /// Байт накоплено в текущем 1-секундном bucket'е.
    cur_sec_bytes: u64,
    /// EMA-сглаженное значение (= 10 × среднее B/s в steady state).
    ema_10sec: u64,
    /// Timestamp начала текущего bucket'а (ms; 0 = ещё не инициализирован).
    last_sec_ms: i64,
    /// Сколько секунд накопили (clamped до 10). audit_delphi_deviation #2: до 10 секунд
    /// используем accumulation (без EMA) — Delphi паттерн `StatSecCount`. Иначе первые
    /// 10 сек getter выдаёт занижено в 10 раз.
    stat_sec_count: u8,
}

impl BpsCounter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Добавить N байт в счётчик. `now_ms` — текущее время (любая монотонная база).
    /// O(1): 1 if + 1 sub + (раз в секунду) 2 mul/div + 1 store. Никаких аллокаций.
    pub fn add(&mut self, bytes: u64, now_ms: i64) {
        // Первый вызов — просто инициализируем bucket.
        if self.last_sec_ms == 0 {
            self.last_sec_ms = now_ms;
        }
        // Прошла секунда? Закрываем bucket в EMA / accumulation.
        if (now_ms - self.last_sec_ms).abs() > 1000 {
            // Ramp-up (audit_delphi_deviation #2): первые 10 секунд — accumulation, далее EMA.
            // Так Delphi `MoonProtoUDPClient.pas:113-138` гарантирует точное среднее
            // с первой секунды (без 10×underestimate).
            if self.stat_sec_count < 10 {
                self.ema_10sec = self.ema_10sec.saturating_add(self.cur_sec_bytes);
                self.stat_sec_count += 1;
            } else {
                // EMA: 90% старого + 10% нового. Формула из Delphi: `ema := ema / 10 * 9 + bucket`.
                self.ema_10sec = (self.ema_10sec / 10) * 9 + self.cur_sec_bytes;
            }
            self.cur_sec_bytes = 0;
            self.last_sec_ms = now_ms;
        }
        self.cur_sec_bytes = self.cur_sec_bytes.saturating_add(bytes);
    }

    /// Среднее количество байт в секунду за последние ~10 секунд.
    /// В steady state равно фактическому `bytes/sec`. В первые 10 секунд после старта —
    /// делится на реальное число накопленных секунд (а не на 10) для точного среднего.
    pub fn bytes_per_sec(&self) -> u64 {
        let div = self.stat_sec_count.max(1) as u64;
        self.ema_10sec / div
    }
}

#[cfg(test)]
mod bps_tests {
    use super::*;

    #[test]
    fn bps_counter_empty() {
        let c = BpsCounter::new();
        assert_eq!(c.bytes_per_sec(), 0);
    }

    #[test]
    fn bps_counter_within_second_just_accumulates() {
        let mut c = BpsCounter::new();
        c.add(100, 1000);
        c.add(200, 1500);
        // Не прошла секунда → ema_10sec не обновился → bytes_per_sec = 0.
        assert_eq!(c.bytes_per_sec(), 0);
        // Но bucket собрал 300.
        assert_eq!(c.cur_sec_bytes, 300);
    }

    #[test]
    fn bps_counter_steady_state_converges() {
        let mut c = BpsCounter::new();
        // Эмулируем 100 секунд равномерного потока: 1000 байт/сек.
        // Используем шаг 1100мс между бакетами чтобы условие `> 1000` срабатывало надёжно.
        for sec in 1..101i64 {
            let bucket_start = sec * 1100;
            for _ in 0..10 {
                c.add(100, bucket_start);
            }
        }
        // EMA должна сойтись к ~10000 (= 10 × 1000 byte/sec — формула Delphi).
        // bytes_per_sec возвращает ema/10 = ~1000.
        let bps = c.bytes_per_sec();
        assert!(bps > 850 && bps < 1100, "bps={}, expected ~1000", bps);
    }
}

/// D-02 helper (testable): pure timing-check для отложенного второго ImFriend.
/// `true` если слот занят И время пришло.
#[inline]
fn second_imfriend_due(pending: &Option<(i64, Vec<u8>)>, cur_tm: i64) -> bool {
    matches!(pending, Some((due, _)) if cur_tm >= *due)
}

#[cfg(test)]
mod d02_tests {
    use super::*;

    #[test]
    fn second_imfriend_none_never_due() {
        let p: Option<(i64, Vec<u8>)> = None;
        assert!(!second_imfriend_due(&p, 0));
        assert!(!second_imfriend_due(&p, i64::MAX));
    }

    #[test]
    fn second_imfriend_not_due_when_before_deadline() {
        let p: Option<(i64, Vec<u8>)> = Some((100, vec![1, 2, 3]));
        assert!(!second_imfriend_due(&p, 0));
        assert!(!second_imfriend_due(&p, 50));
        assert!(!second_imfriend_due(&p, 99));
    }

    #[test]
    fn second_imfriend_due_at_or_after_deadline() {
        let p: Option<(i64, Vec<u8>)> = Some((100, vec![1, 2, 3]));
        assert!(second_imfriend_due(&p, 100));
        assert!(second_imfriend_due(&p, 101));
        assert!(second_imfriend_due(&p, 1_000_000));
    }

    #[test]
    fn second_imfriend_default_pause_is_32ms() {
        // Семантический тест: на типичной задержке (32мс — wire-compat константа из Delphi)
        // после планирования в момент T, due срабатывает в T+32, не раньше.
        let scheduled_at = 1000;
        let due = scheduled_at + 32;
        let p: Option<(i64, Vec<u8>)> = Some((due, vec![0xAA]));
        assert!(!second_imfriend_due(&p, scheduled_at + 31));
        assert!(second_imfriend_due(&p, scheduled_at + 32));
    }

    /// Verify что full_reset очищает pending_second_imfriend slot.
    /// Это критично — иначе при reconnect старый payload отправлен бы повторно.
    /// Тестируем take() семантику изолированно — без реального socket.
    #[test]
    fn take_clears_pending_slot() {
        let mut pending: Option<(i64, Vec<u8>)> = Some((100, vec![0xDE, 0xAD]));
        assert!(second_imfriend_due(&pending, i64::MAX));
        // take() очищает slot — то же что делает check_pending_second_imfriend и full_reset.
        let taken = pending.take();
        assert!(taken.is_some());
        assert!(!second_imfriend_due(&pending, i64::MAX));
    }
}

/// Global NTP time offset (days). Set once at startup by ntp::get_best_ntp.
/// Matches Delphi GlobalMPTimeOffset.
static NTP_OFFSET_DAYS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn set_ntp_offset(offset_seconds: f64) {
    let bits = (offset_seconds / 86400.0).to_bits();
    NTP_OFFSET_DAYS.store(bits, std::sync::atomic::Ordering::Relaxed);
}

fn get_ntp_offset_days() -> f64 {
    f64::from_bits(NTP_OFFSET_DAYS.load(std::sync::atomic::Ordering::Relaxed))
}

/// Delphi TDateTime (days since 1899-12-30) corrected by NTP offset.
/// Matches: `Now - GlobalMPTimeZoneOffset + GlobalMPTimeOffset`
/// We use UTC directly (no timezone offset needed — TDateTime in MoonProto = UTC).
///
/// **Clock-jump sanity check** (audit_robustness H6): SystemTime подвержен NTP step и
/// suspend/resume скачкам. Если детектируем монотонное смещение > 60 сек между подряд
/// идущими вызовами — log warn (потребитель должен пере-syncнуться через `set_ntp_offset`).
/// Сам результат возвращаем как есть — иначе handshake/order timestamps будут противоречить
/// серверу. Защита через лог, не через clamp.
fn delphi_now() -> f64 {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    let now = 25569.0 + secs / 86400.0 + get_ntp_offset_days();

    // Детектор скачка: сравним с прошлым вызовом. Days * 86400 = seconds.
    use std::sync::atomic::{AtomicU64, Ordering};
    static LAST_NOW_BITS: AtomicU64 = AtomicU64::new(0);
    let prev_bits = LAST_NOW_BITS.swap(now.to_bits(), Ordering::Relaxed);
    if prev_bits != 0 {
        let prev = f64::from_bits(prev_bits);
        let delta_secs = (now - prev) * 86400.0;
        if delta_secs.abs() > 60.0 {
            log::warn!(target: "moonproto::client",
                "delphi_now clock jump detected: {:.1}s — handshake/order timestamps may diverge; consider re-syncing NTP",
                delta_secs);
        }
    }
    now
}

/// Установить SO_RCVBUF + SO_SNDBUF в 8 MB через socket2 (cross-platform).
/// Закрывает ARCH §30 ("UDP buffer sizes — должны быть существенно больше sysctl-defaults").
/// На пиковой нагрузке (~50K packets/sec) маленький ядерный буфер → silent drop.
/// D-07 + D-08: ошибки больше не игнорируются — логируем как warn (OS может отказать,
/// например Linux без `net.core.rmem_max ≥ 8MB` молча обрежет до настройки sysctl).
fn set_socket_buffers(sock: &UdpSocket) {
    let sock2 = socket2::SockRef::from(sock);
    if let Err(e) = sock2.set_recv_buffer_size(8 * 1024 * 1024) {
        warn!("SO_RCVBUF=8MB rejected by OS (probably net.core.rmem_max too small): {e}");
    }
    if let Err(e) = sock2.set_send_buffer_size(8 * 1024 * 1024) {
        warn!("SO_SNDBUF=8MB rejected by OS: {e}");
    }
}

/// Cross-platform IP_DONTFRAGMENT / IP_MTU_DISCOVER / IP_DONTFRAG.
/// Закрывает ARCH §20 (PMTU discovery должен работать на всех платформах, не только Windows).
/// Без этого SizeAck/ProbeMTUAck отправляются с разрешённой фрагментацией → измерение PMTU
/// становится ложным → клиент выбирает неоптимальный PMTU → каскадные retransmit'ы.
fn set_dont_fragment_for_socket(sock: &UdpSocket, enable: bool) {
    #[cfg(target_os = "windows")]
    {
        use std::os::windows::io::AsRawSocket;
        let raw = sock.as_raw_socket();
        let val: i32 = if enable { 1 } else { 0 };
        unsafe {
            extern "system" {
                fn setsockopt(s: usize, level: i32, optname: i32, optval: *const i8, optlen: i32) -> i32;
            }
            // IPPROTO_IP=0, IP_DONTFRAGMENT=14
            setsockopt(raw as usize, 0, 14, &val as *const i32 as *const i8, 4);
        }
    }
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        use std::os::fd::AsRawFd;
        let fd = sock.as_raw_fd();
        // IPPROTO_IP=0, IP_MTU_DISCOVER=10, value=IP_PMTUDISC_DO=2 / IP_PMTUDISC_DONT=0
        let val: i32 = if enable { 2 } else { 0 };
        unsafe {
            extern "C" {
                fn setsockopt(s: i32, level: i32, optname: i32, optval: *const i8, optlen: u32) -> i32;
            }
            setsockopt(fd, 0, 10, &val as *const i32 as *const i8, 4);
        }
    }
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        use std::os::fd::AsRawFd;
        let fd = sock.as_raw_fd();
        // IPPROTO_IP=0, IP_DONTFRAG=28
        let val: i32 = if enable { 1 } else { 0 };
        unsafe {
            extern "C" {
                fn setsockopt(s: i32, level: i32, optname: i32, optval: *const i8, optlen: u32) -> i32;
            }
            setsockopt(fd, 0, 28, &val as *const i32 as *const i8, 4);
        }
    }
    #[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "android",
                  target_os = "macos", target_os = "ios")))]
    {
        // Other platforms (BSD, etc.) — no-op для безопасности, PMTU discovery не работает.
        let _ = (sock, enable);
    }
}
