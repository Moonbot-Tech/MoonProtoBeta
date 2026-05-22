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
    parse_coin_card_candles_response, parse_request_candles_data_response, CandlesAggregator,
    CandlesChunkResult, DeepPrice, RequestCandlesMarket,
};
use crate::commands::engine_api::{
    parse_api_expiration_time_response, parse_auth_check_response, parse_base_check_response,
    parse_engine_response, parse_get_balance_response, parse_query_hedge_mode_response,
    parse_update_transfer_assets_response, ApiExpirationTime, AuthCheckResponse, EngineMethod,
    EngineResponse, ServerInfo, TransferAsset,
};
use crate::compression;
use crate::crypto;
use crate::protocol::{crypted, handshake, slicing, slider::Slider, Command};
use crate::MoonKey;
use log::{debug, error, warn};
// MoonProto UDP Client architecture follows the Delphi split:
// main/send loop plus UDP reader thread. See MAPPING.md for line-by-line
// correspondence.
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(feature = "diagnostic-trace")]
use std::sync::OnceLock;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

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
// MAC и version, а в Delphi-клиенте ещё и после побочных эффектов `TotalRecvBytes`
// / `LastOnline`. Rust сохраняет тот же порядок: валидный packet, выбранный ErrEmu
// для дропа, всё равно доезжает до main-loop, обновляет transport stats, и только
// потом не dispatch'ится в protocol layer. Служебные команды (Ping /
// handshake-related / ACK) дропаются с rate/2 чтобы handshake не отваливался
// полностью.
//
// Использование (пример: 75% loss):
//   moonproto::client::set_err_emu(75);
//   let mut client = Client::new(cfg);
//   client.run(...);
//
// Используется в `examples/loss_logger.rs` — runtime-логгер потерь и восстановлений.
/// Process-wide incoming packet-loss emulator rate, in percent.
///
/// This is a test hook for stress and FireTest-style scenarios. Prefer
/// [`set_err_emu`] instead of writing the atomic directly.
#[doc(hidden)]
pub static ERR_EMU_RATE: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

/// Set the client-side incoming packet-loss emulator percentage (`0..=100`).
///
/// `0` disables emulation and is the default. This hook is for tests only and
/// mirrors Delphi `MoonProtoErrEmu`.
pub fn set_err_emu(percent: u8) {
    ERR_EMU_RATE.store(percent.min(100), std::sync::atomic::Ordering::Relaxed);
}

#[cfg(feature = "diagnostic-trace")]
fn trace_io_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var_os("MOONPROTO_TRACE_IO")
            .map(|v| {
                let v = v.to_string_lossy();
                !(v.is_empty() || v == "0" || v.eq_ignore_ascii_case("false"))
            })
            .unwrap_or(false)
    })
}

#[cfg(not(feature = "diagnostic-trace"))]
#[inline(always)]
fn trace_io_enabled() -> bool {
    false
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
    let drop_rate = if is_service_cmd(cmd) {
        base_rate / 2
    } else {
        base_rate
    };
    let roll: u8 = rand::random::<u8>() % 100;
    roll < drop_rate
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
fn timeout_remaining(start: Instant, timeout: Duration) -> Option<Duration> {
    let elapsed = start.elapsed();
    if elapsed >= timeout {
        None
    } else {
        Some(timeout.saturating_sub(elapsed))
    }
}

// === Constants matching Delphi exactly ===
const DEFAULT_SLEEP_MS: u64 = 5; // MoonProtoFunc.pas:19
                                 // Temporary Rust-only guard while control intents and active delivery still share
                                 // the main loop with writer work. Accepted UDP packets are no longer budgeted here.
const EVENT_DRAIN_BUDGET: usize = 512;
const DELPHI_SEND_AND_WAIT_POLL_MS: u64 = 10; // MoonProtoEngine.pas:531
const DELPHI_BASE_CHECK_UPDATE_AUTH_WAITS: usize = 34; // MoonProtoEngine.pas:574
const DELPHI_BASE_CHECK_UPDATE_AUTH_WAIT_MS: u64 = 300; // MoonProtoEngine.pas:575
const DELPHI_BASE_CHECK_UPDATE_RETRIES: usize = 10; // MoonProtoEngine.pas:586
const DELPHI_BASE_CHECK_UPDATE_RETRY_PAUSE_MS: u64 = 2_000; // MoonProtoEngine.pas:589
const RECONNECT_WAITING_MS: i64 = 7000; // MoonProtoUDPClient.pas:88
const RECONNECT_THROTTLE_MS: i64 = 15000; // MoonProtoUDPClient.pas:89
const OFFLINE_BASE_MS: i64 = 2300; // MoonProtoUDPClient.pas:772
const DEAD_ZONE_MS: i64 = 5000; // MoonProtoUDPClient.pas:799
const NEED_HELLO_AGAIN_THROTTLE_MS: i64 = 700; // MoonProtoUDPClient.pas:568
const CLEANUP_INTERVAL_MS: i64 = 5000; // MoonProtoIntStruct.pas:828
const COMPRESSED_FLAG: u8 = 0x80; // MoonProtoDataStruct.pas:27
const MIN_SIZE_TO_COMPRESS: usize = 64; // MoonProtoDataStruct.pas:31
const IMFRIEND_DUPLICATE_DELAY_MS: u64 = 32; // MoonProtoUDPClient.pas:433-436
const NEVER_SENT_MS: i64 = i64::MIN / 2; // Эквивалент Delphi LastSentHello=0 при uptime-clock
const NEVER_TIME_MS: i64 = i64::MIN / 2;
const BIND_FAILED_FIRST_EVENT_MS: i64 = 15_000;
const BIND_FAILED_REPEAT_EVENT_MS: i64 = 50_000;

/// Send priority matching Delphi `TMoonProtoSendPriority`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SendPriority {
    /// `MPS_Sliced`: large reliable payload sent through the slicing engine.
    Sliced,
    /// `MPS_High`: small direct payload with ACK/retry handling.
    High,
    /// `MPS_Low`: best-effort low-priority payload, one per send cycle.
    Low,
}

/// Unique key for command deduplication.
///
/// This matches Delphi `TMoonUniqueKey`: commands with the same `(kind, uid)`
/// replace older pending commands in send queues.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UniqueKey {
    /// `TUniqueCommandKind` ordinal (`0` means no dedup).
    pub kind: u8,
    /// Command-specific dedup identity, usually a server order UID or fixed
    /// singleton slot.
    pub uid: u64,
}

/// `UK_None`: no queue deduplication.
pub const UK_NONE: u8 = 0;
/// `UK_OrderStatus`: low-level order-status request key.
pub const UK_ORDER_STATUS: u8 = 1;
/// `UK_OrderStatusShort`: low-level short order-status request key.
pub const UK_ORDER_STATUS_SHORT: u8 = 2;
/// `UK_OrderMove`: replace/cancel/stops/panic/VStop dedup by order task id.
pub const UK_ORDER_MOVE: u8 = 3;
/// `UK_StopMove`: legacy stop-move dedup ordinal from Delphi.
pub const UK_STOP_MOVE: u8 = 4;
/// `UK_StratSnapshot`: singleton strategy snapshot dedup key.
pub const UK_STRAT_SNAPSHOT: u8 = 5;
/// `UK_BaseUISettings`: singleton client-settings snapshot dedup key.
pub const UK_BASE_UI_SETTINGS: u8 = 6;
/// `UK_StratSellPriceUpdate`: per-strategy sell-price dedup key.
pub const UK_STRAT_SELL_PRICE_UPDATE: u8 = 7;
/// `UK_BalanceFull`: singleton full-balance snapshot dedup key.
pub const UK_BALANCE_FULL: u8 = 8;
/// `UK_TurnMMDetection`: MM-orders subscription command key.
pub const UK_TURN_MM_DETECTION: u8 = 9;
/// `UK_ImmuneClicks`: batch order-immunity dedup key.
pub const UK_IMMUNE_CLICKS: u8 = 10;
/// `UK_LevManageSettings`: singleton leverage-management settings key.
pub const UK_LEV_MANAGE_SETTINGS: u8 = 11;
/// `UK_ArbPrices`: arbitrage price command key.
pub const UK_ARB_PRICES: u8 = 12;
/// `UK_DexSwitch`: DEX switch command key.
pub const UK_DEX_SWITCH: u8 = 13;
/// `UK_SpotSwitch`: spot-mode switch command key.
pub const UK_SPOT_SWITCH: u8 = 14;

impl UniqueKey {
    /// No deduplication.
    pub fn none() -> Self {
        Self {
            kind: UK_NONE,
            uid: 0,
        }
    }
    /// Return whether this key disables deduplication.
    pub fn is_none(&self) -> bool {
        self.kind == UK_NONE
    }
    /// UKey for order move/cancel/stops/panic/vstop commands keyed by task id.
    pub fn order_move(task_id: u64) -> Self {
        Self {
            kind: UK_ORDER_MOVE,
            uid: task_id,
        }
    }
    /// UKey for `TSetImmuneCommand`, keyed by the wrapping sum of item UIDs.
    pub fn immune_clicks(items_uid_sum: u64) -> Self {
        Self {
            kind: UK_IMMUNE_CLICKS,
            uid: items_uid_sum,
        }
    }

    /// `UK_BaseUISettings` with the supplied UID.
    ///
    /// Prefer [`Self::base_ui_settings_slot`] for the Delphi settings snapshot
    /// singleton slot.
    pub fn base_ui_settings(uid: u64) -> Self {
        Self {
            kind: UK_BASE_UI_SETTINGS,
            uid,
        }
    }
    /// `UK_BaseUISettings` with Delphi `TClientSettingsCommand.SetUKey`
    /// semantics: every settings snapshot competes for the single UID=1 slot.
    pub fn base_ui_settings_slot() -> Self {
        Self {
            kind: UK_BASE_UI_SETTINGS,
            uid: 1,
        }
    }
    /// Legacy one-slot `UK_TurnMMDetection` helper.
    ///
    /// Delphi `TMMOrdersSubscribeCommand` does not override `SetUKey`, so the
    /// high-level wrapper uses [`Self::turn_mm_detection_for`] with the command
    /// UID. Keep this helper only for tools that intentionally want explicit
    /// single-slot dedup.
    pub fn turn_mm_detection() -> Self {
        Self {
            kind: UK_TURN_MM_DETECTION,
            uid: 0,
        }
    }
    /// Delphi `TMMOrdersSubscribeCommand` UKey: `(UK_TurnMMDetection, UID)`.
    pub fn turn_mm_detection_for(uid: u64) -> Self {
        Self {
            kind: UK_TURN_MM_DETECTION,
            uid,
        }
    }
    /// `UK_LevManageSettings` with the supplied UID.
    ///
    /// Prefer [`Self::lev_manage_settings_slot`] for the Delphi singleton slot.
    pub fn lev_manage_settings(uid: u64) -> Self {
        Self {
            kind: UK_LEV_MANAGE_SETTINGS,
            uid,
        }
    }
    /// `UK_LevManageSettings` with Delphi `TLevManageCommand.SetUKey`
    /// semantics: every leverage-management snapshot competes for UID=1.
    pub fn lev_manage_settings_slot() -> Self {
        Self {
            kind: UK_LEV_MANAGE_SETTINGS,
            uid: 1,
        }
    }
    /// Legacy one-slot `UK_DexSwitch` helper.
    ///
    /// Delphi `TSwitchDexCommand` does not override `SetUKey`, so the
    /// high-level wrapper uses [`Self::dex_switch_for`] with the command UID.
    pub fn dex_switch() -> Self {
        Self {
            kind: UK_DEX_SWITCH,
            uid: 0,
        }
    }
    /// Delphi `TSwitchDexCommand` UKey: `(UK_DexSwitch, UID)`.
    pub fn dex_switch_for(uid: u64) -> Self {
        Self {
            kind: UK_DEX_SWITCH,
            uid,
        }
    }
    /// Legacy one-slot `UK_SpotSwitch` helper.
    ///
    /// Delphi `TSwitchSpotCommand` does not override `SetUKey`, so the
    /// high-level wrapper uses [`Self::spot_switch_for`] with the command UID.
    pub fn spot_switch() -> Self {
        Self {
            kind: UK_SPOT_SWITCH,
            uid: 0,
        }
    }
    /// Delphi `TSwitchSpotCommand` UKey: `(UK_SpotSwitch, UID)`.
    pub fn spot_switch_for(uid: u64) -> Self {
        Self {
            kind: UK_SPOT_SWITCH,
            uid,
        }
    }
    /// `UK_StratSellPriceUpdate` keyed by `strategy_id` so dedup is per
    /// strategy.
    pub fn strat_sell_price_update(strategy_id: u64) -> Self {
        Self {
            kind: UK_STRAT_SELL_PRICE_UPDATE,
            uid: strategy_id,
        }
    }
    /// `UK_StratSnapshot` singleton slot for full strategy snapshots.
    pub fn strat_snapshot() -> Self {
        Self {
            kind: UK_STRAT_SNAPSHOT,
            uid: 1,
        }
    }
    /// `UK_BalanceFull` singleton slot for full balance snapshots.
    pub fn balance_full() -> Self {
        Self {
            kind: UK_BALANCE_FULL,
            uid: 1,
        }
    }
}

/// Item in the send queue (matches TMoonProtoDataToSend)
#[derive(Clone)]
pub(crate) struct SendItem {
    pub data: Vec<u8>,   // serialized command stream
    pub cmd: u8,         // TMoonProtoCommand ordinal
    pub encrypted: bool, // FCrypted
    pub priority: SendPriority,
    pub retry_left: i32,   // RetryLeft
    pub max_retries: i32,  // MaxRetryCount
    pub msg_num: u64,      // for ACK tracking (assigned in Crypt)
    pub last_sent_at: i64, // ms timestamp of last send
    pub u_key: UniqueKey,  // dedup key (matches TMoonUniqueKey)
}

/// Delphi `TMoonProtoBaseNet.DataToSend*` queues.
///
/// `SendCmdInt` appends directly into one of these grow-only lists under
/// `SendLock`; the writer tick later copies and clears them through
/// `GetCopySendList`. Keep the same machine effect: no local capacity cap, and
/// UKey dedup only for Sliced/High queues, removing the first older item with
/// the same key before appending the new item.
#[derive(Default)]
pub(crate) struct SendQueues {
    sliced: Vec<SendItem>,
    high: Vec<SendItem>,
    low: Vec<SendItem>,
}

impl SendQueues {
    fn push_send_cmd_int(&mut self, item: SendItem) {
        let queue = match item.priority {
            SendPriority::Sliced => &mut self.sliced,
            SendPriority::High => &mut self.high,
            SendPriority::Low => &mut self.low,
        };

        if !item.u_key.is_none()
            && matches!(item.priority, SendPriority::Sliced | SendPriority::High)
        {
            if let Some(pos) = queue.iter().position(|queued| queued.u_key == item.u_key) {
                queue.remove(pos);
            }
        }

        queue.push(item);
    }

    fn take_into(
        &mut self,
        sliced: &mut Vec<SendItem>,
        high: &mut Vec<SendItem>,
        low: &mut Vec<SendItem>,
    ) {
        sliced.append(&mut self.sliced);
        high.append(&mut self.high);
        low.append(&mut self.low);
    }
}

/// Test-only legacy reader message.
///
/// Production reader no longer queues accepted UDP packets as `ClientEvent::Recv`:
/// protocol-owned receive work runs in the reader stack, and decoded payloads
/// use `pending_reader_decoded` plus a coalesced `Wake`.
#[cfg(test)]
#[derive(Clone)]
pub(crate) struct RecvMsg {
    cmd: u8,
    payload: Vec<u8>,
    recv_bytes: u64,
    timestamp_ms: i64,
    /// Packet protocol body was already handled in the reader thread.
    ///
    /// Delphi `UDPRead` performs some protocol branches fully in the reader
    /// path. Main/writer still has to observe the accepted-packet side effects
    /// (`TotalRecvBytes`, `LastOnline`), but must not parse that packet again.
    handled_in_reader: bool,
    /// Delphi updates `TotalRecvBytes`/`LastOnline` for a valid packet before
    /// `MoonProtoErrEmu` drops it. Keep the packet in the reader→main FIFO so
    /// main can apply those side effects, then skip protocol parsing.
    err_emu_drop: bool,
    /// Reader already performed the DataReadInt protocol-owned core for this
    /// packet, but main still has to run the remaining command-specific branch.
    reader_dataread_core_done: bool,
    /// Аудит #7 (audit_delphi_deviation E-V2-02): эпоха reader thread'а который создал
    /// это сообщение. Инкрементируется на каждый `spawn_reader`. Main loop игнорирует
    /// сообщения с epoch != `current_reader_epoch` — это защита от пакетов старого
    /// reader thread'а который ещё не завершился во время reconnect'а.
    epoch: u32,
}

#[derive(Clone)]
pub(crate) struct ReaderDecodedMsg {
    cmd: u8,
    payload: Option<Vec<u8>>,
    recv_bytes: u64,
    timestamp_ms: i64,
    epoch: u32,
    apply_recv_effects: bool,
    sliced_stats: Option<ReaderSlicedStats>,
    ping_update: Option<ReaderPingUpdate>,
    handshake_update: Option<ReaderHandshakeUpdate>,
}

#[derive(Clone)]
pub(crate) struct ReaderSlicedStats {
    dup_count: u8,
    blocks_count: usize,
}

#[derive(Clone)]
pub(crate) struct ReaderPingUpdate {
    ping_count: u32,
    round_trip_delay: i64,
    actual_pmtu: u16,
    global_timing_orders: u16,
    overheat: u8,
    rs: f64,
    server_time_delta: f64,
    net_lag_ping: i64,
    can_send_rate: i32,
    used_sliced_limit: bool,
}

#[derive(Clone)]
pub(crate) struct ReaderHandshakeUpdate {
    cmd: Command,
    server_token: u64,
    peer_app_token: u64,
    client_token: u64,
    encode_key: MoonKey,
    decode_key: MoonKey,
}

/// Legacy message from app to main loop (send command request).
///
/// New production send paths append directly to [`SendQueues`] to match Delphi
/// `SendCmdInt`. Keep this variant temporarily for tests and any already queued
/// control-path events while the threading rewrite is in progress.
#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SendMsg {
    pub item: SendItem,
}

/// Reader/main control channel.
///
/// Production reader sends only coalesced `Wake` notifications here after
/// reader-side protocol progress. Accepted UDP packets are not queued as events:
/// data and service command branches run in the reader stack, then decoded
/// payloads use a separate queue for user/active delivery.
///
/// F4: Subscribe events позволяют UI-thread'у попросить либу обновить подписку
/// без `&mut Client` lock. Main loop обрабатывает их идентично прямым
/// `subscribe_*` методам — apply registry change + emit wire request.
#[derive(Clone)]
pub(crate) enum ClientEvent {
    /// Wakes main loop after reader-side protocol progress that is stored
    /// outside the shared Recv queue.
    Wake,
    #[cfg(test)]
    Recv(RecvMsg),
    #[cfg(test)]
    Send(SendMsg),
    /// Registry-aware `TMMOrdersSubscribeCommand` intent.
    MmOrdersSubscribe { subscribe: bool },
    /// Подписаться на orderbook рынка. Main loop обновит registry и отправит
    /// `emk_SubscribeOrderBook` если подписки ещё не было (idempotent).
    SubscribeOrderBook { market_name: String },
    /// Subscribe to several orderbook markets with one registry-aware intent.
    SubscribeOrderBooks { market_names: Vec<String> },
    /// Отписаться от orderbook рынка.
    UnsubscribeOrderBook { market_name: String },
    /// Unsubscribe from several orderbook markets with one registry-aware intent.
    UnsubscribeOrderBooks { market_names: Vec<String> },
    /// Clear all remembered orderbook subscriptions and send the server's
    /// empty-market-list unsubscribe request.
    UnsubscribeAllOrderBooks,
    /// Подписаться на all-trades поток с параметром `want_mm` (нужны ли MM-ордера).
    SubscribeAllTrades { want_mm: bool },
    /// Отписаться от all-trades потока.
    UnsubscribeAllTrades,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecvEnqueue {
    Delivered,
    Disconnected,
}

#[cfg(test)]
fn enqueue_recv_event(
    event_tx: &mpsc::Sender<ClientEvent>,
    msg: RecvMsg,
    _start_time: Instant,
) -> RecvEnqueue {
    // Test helper for the former reader->main packet FIFO. The production
    // reader path must not add a second local loss/backpressure mode on top of
    // UDP/ErrEmu.
    match event_tx.send(ClientEvent::Recv(msg)) {
        Ok(()) => RecvEnqueue::Delivered,
        Err(_) => RecvEnqueue::Disconnected,
    }
}

fn notify_reader_work(
    event_tx: &mpsc::Sender<ClientEvent>,
    wake_pending: &AtomicBool,
) -> RecvEnqueue {
    if wake_pending.swap(true, Ordering::AcqRel) {
        return RecvEnqueue::Delivered;
    }
    match event_tx.send(ClientEvent::Wake) {
        Ok(()) => RecvEnqueue::Delivered,
        Err(_) => {
            wake_pending.store(false, Ordering::Release);
            RecvEnqueue::Disconnected
        }
    }
}

enum QueuedControlEvent {
    Subscribe(ClientEvent),
    MmOrdersSubscribe(bool),
}

/// Error returned by fallible [`ClientSender`] queueing methods.
///
/// Send/control queues are intentionally unbounded to preserve the Delphi
/// no-local-cap behavior of `SendCmdInt`. The only normal failure is that the
/// owning `Client` has been dropped or its run loop has shut down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscribeError {
    /// Legacy/reserved. Internal queues are intentionally unbounded to match
    /// Delphi: accepted packets and user intents are not dropped by a local
    /// capacity check. Current code does not return this variant.
    ChannelFull,
    /// The owning `Client` was dropped or the main loop exited, so this sender
    /// can no longer enqueue work.
    Disconnected,
}

impl std::fmt::Display for SubscribeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ChannelFull => write!(f, "Client queues have no capacity cap"),
            Self::Disconnected => write!(f, "Client queues disconnected"),
        }
    }
}

impl std::error::Error for SubscribeError {}

/// Thread-safe handle for UI and worker threads.
///
/// Obtain it with [`Client::sender`], clone it freely, and send work while the
/// owning `Client` is running on another thread. Subscription helpers update the
/// active-library registry. Raw command helpers append already-serialized
/// command payloads directly into the Delphi-style send queues used by `Client`
/// wrappers. The sender also mirrors fire-and-forget trade, UI, strategy, and
/// balance wrappers so terminal UI code can send typed actions without
/// rebuilding wire priorities, retry counts, or UKey values by hand.
///
/// ```ignore
/// let mut client = Client::new(cfg);
/// let sender = client.sender();
/// // Move the sender into a UI thread:
/// thread::spawn(move || {
///     sender.subscribe_orderbook("DOGEUSDT");
/// });
/// // Main thread:
/// client.run_with_dispatcher(...);
/// ```
///
/// Fire-and-forget methods log if the client is gone. `try_*` methods return
/// [`SubscribeError`] when the caller needs explicit feedback.
#[derive(Clone)]
pub struct ClientSender {
    app_events: Arc<Mutex<VecDeque<ClientEvent>>>,
    app_queue_alive: Arc<AtomicBool>,
    send_queues: Arc<Mutex<SendQueues>>,
    server_update_sent: Arc<AtomicBool>,
}

impl ClientSender {
    /// Subscribe to an orderbook stream and remember the intent for reconnect
    /// restore.
    pub fn subscribe_orderbook(&self, market_name: &str) {
        if let Err(e) = self.try_subscribe_orderbook(market_name) {
            log::warn!(target: "moonproto::client",
                "subscribe_orderbook({market_name}) dropped: {e}");
        }
    }

    /// Unsubscribe from an orderbook stream and update the reconnect registry.
    pub fn unsubscribe_orderbook(&self, market_name: &str) {
        if let Err(e) = self.try_unsubscribe_orderbook(market_name) {
            log::warn!(target: "moonproto::client",
                "unsubscribe_orderbook({market_name}) dropped: {e}");
        }
    }

    /// Subscribe to several orderbook streams and remember all intents for
    /// reconnect restore.
    ///
    /// This sends one control intent to the client loop. The loop deduplicates
    /// already remembered market names and emits one batched
    /// `emk_SubscribeOrderBook` request for newly added markets.
    pub fn subscribe_orderbooks<I, S>(&self, market_names: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if let Err(e) = self.try_subscribe_orderbooks(market_names) {
            log::warn!(target: "moonproto::client",
                "subscribe_orderbooks dropped: {e}");
        }
    }

    /// Unsubscribe from several orderbook streams and update the reconnect
    /// registry.
    pub fn unsubscribe_orderbooks<I, S>(&self, market_names: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if let Err(e) = self.try_unsubscribe_orderbooks(market_names) {
            log::warn!(target: "moonproto::client",
                "unsubscribe_orderbooks dropped: {e}");
        }
    }

    /// Unsubscribe from all orderbook streams remembered by the registry.
    pub fn unsubscribe_all_orderbooks(&self) {
        if let Err(e) = self.try_unsubscribe_all_orderbooks() {
            log::warn!(target: "moonproto::client",
                "unsubscribe_all_orderbooks dropped: {e}");
        }
    }

    /// Subscribe to the all-trades stream and remember the intent for reconnect
    /// restore.
    pub fn subscribe_all_trades(&self, want_mm: bool) {
        if let Err(e) = self.try_subscribe_all_trades(want_mm) {
            log::warn!(target: "moonproto::client",
                "subscribe_all_trades(want_mm={want_mm}) dropped: {e}");
        }
    }

    /// Unsubscribe from the all-trades stream and update the reconnect registry.
    pub fn unsubscribe_all_trades(&self) {
        if let Err(e) = self.try_unsubscribe_all_trades() {
            log::warn!(target: "moonproto::client",
                "unsubscribe_all_trades dropped: {e}");
        }
    }

    /// Fallible orderbook subscription.
    pub fn try_subscribe_orderbook(&self, market_name: &str) -> Result<(), SubscribeError> {
        self.try_send(ClientEvent::SubscribeOrderBook {
            market_name: market_name.to_string(),
        })
    }

    /// Fallible orderbook unsubscribe.
    pub fn try_unsubscribe_orderbook(&self, market_name: &str) -> Result<(), SubscribeError> {
        self.try_send(ClientEvent::UnsubscribeOrderBook {
            market_name: market_name.to_string(),
        })
    }

    /// Fallible batched orderbook subscription.
    pub fn try_subscribe_orderbooks<I, S>(&self, market_names: I) -> Result<(), SubscribeError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let market_names: Vec<String> = market_names
            .into_iter()
            .map(|name| name.as_ref().to_string())
            .collect();
        if market_names.is_empty() {
            return Ok(());
        }
        self.try_send(ClientEvent::SubscribeOrderBooks { market_names })
    }

    /// Fallible batched orderbook unsubscribe.
    pub fn try_unsubscribe_orderbooks<I, S>(&self, market_names: I) -> Result<(), SubscribeError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let market_names: Vec<String> = market_names
            .into_iter()
            .map(|name| name.as_ref().to_string())
            .collect();
        if market_names.is_empty() {
            return Ok(());
        }
        self.try_send(ClientEvent::UnsubscribeOrderBooks { market_names })
    }

    /// Fallible all-orderbooks unsubscribe.
    pub fn try_unsubscribe_all_orderbooks(&self) -> Result<(), SubscribeError> {
        self.try_send(ClientEvent::UnsubscribeAllOrderBooks)
    }

    /// Fallible all-trades subscription.
    pub fn try_subscribe_all_trades(&self, want_mm: bool) -> Result<(), SubscribeError> {
        self.try_send(ClientEvent::SubscribeAllTrades { want_mm })
    }

    /// Fallible all-trades unsubscribe.
    pub fn try_unsubscribe_all_trades(&self) -> Result<(), SubscribeError> {
        self.try_send(ClientEvent::UnsubscribeAllTrades)
    }

    /// Queue an already-serialized command payload for sending.
    ///
    /// This is the thread-safe counterpart of [`Client::send_cmd`]. It does not
    /// build protocol payloads for the caller; use typed builders in
    /// [`crate::commands`] or prefer high-level `Client` wrappers when the caller
    /// already owns the client thread.
    pub fn send_cmd(
        &self,
        data: Vec<u8>,
        cmd: Command,
        priority: SendPriority,
        encrypted: bool,
        max_retries: i32,
    ) {
        if let Err(e) = self.try_send_cmd(data, cmd, priority, encrypted, max_retries) {
            log::warn!(target: "moonproto::client",
                "ClientSender::send_cmd({cmd:?}) dropped: {e}");
        }
    }

    /// Fallible variant of [`Self::send_cmd`].
    pub fn try_send_cmd(
        &self,
        data: Vec<u8>,
        cmd: Command,
        priority: SendPriority,
        encrypted: bool,
        max_retries: i32,
    ) -> Result<(), SubscribeError> {
        self.try_send_cmd_keyed(
            data,
            cmd,
            priority,
            encrypted,
            max_retries,
            UniqueKey::none(),
        )
    }

    /// Queue an already-serialized command payload with a Delphi UKey dedup key.
    ///
    /// This is the thread-safe counterpart of [`Client::send_cmd_keyed`].
    pub fn send_cmd_keyed(
        &self,
        data: Vec<u8>,
        cmd: Command,
        priority: SendPriority,
        encrypted: bool,
        max_retries: i32,
        u_key: UniqueKey,
    ) {
        if let Err(e) = self.try_send_cmd_keyed(data, cmd, priority, encrypted, max_retries, u_key)
        {
            log::warn!(target: "moonproto::client",
                "ClientSender::send_cmd_keyed({cmd:?}) dropped: {e}");
        }
    }

    /// Fallible variant of [`Self::send_cmd_keyed`].
    pub fn try_send_cmd_keyed(
        &self,
        data: Vec<u8>,
        cmd: Command,
        priority: SendPriority,
        encrypted: bool,
        max_retries: i32,
        u_key: UniqueKey,
    ) -> Result<(), SubscribeError> {
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
        self.try_enqueue_send_item(item)
    }

    /// Queue a fire-and-forget Engine API request from another thread.
    ///
    /// The payload must be a complete `TEngineRequest` body, for example from
    /// [`crate::commands::engine_request`]. This method does not register a
    /// pending response receiver; responses will surface as ordinary
    /// `Event::EngineResponse` values in the running dispatcher.
    pub fn send_api_request(&self, request_payload: Vec<u8>) {
        if let Err(e) = self.try_send_api_request(request_payload) {
            log::warn!(target: "moonproto::client",
                "ClientSender::send_api_request dropped: {e}");
        }
    }

    /// Fallible variant of [`Self::send_api_request`].
    pub fn try_send_api_request(&self, request_payload: Vec<u8>) -> Result<(), SubscribeError> {
        self.try_send_cmd(request_payload, Command::API, SendPriority::Sliced, true, 6)
    }

    fn send_trade(&self, payload: Vec<u8>, max_retries: i32) {
        self.send_cmd(
            payload,
            Command::Order,
            SendPriority::High,
            true,
            max_retries,
        );
    }

    fn send_trade_keyed(&self, payload: Vec<u8>, max_retries: i32, u_key: UniqueKey) {
        self.send_cmd_keyed(
            payload,
            Command::Order,
            SendPriority::High,
            true,
            max_retries,
            u_key,
        );
    }

    /// Send `TNewOrderCommand` from a thread-safe sender.
    ///
    /// This mirrors [`Client::new_order`]: `MPC_Order`, high priority,
    /// encrypted, `MaxRetries=3`.
    pub fn new_order(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        is_short: bool,
        price: f64,
        strat_id: u64,
        order_size: f64,
    ) {
        let raw = crate::commands::trade::build_new_order(
            ctx, market, is_short, price, strat_id, order_size,
        );
        self.send_trade(raw, 3);
    }

    /// Send `TOrderReplaceCommand` for an existing order task id.
    pub fn replace_order(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        order_type: crate::commands::trade::OrderType,
        new_price: f64,
    ) {
        let raw = crate::commands::trade::build_order_replace(ctx, market, order_type, new_price);
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
    }

    /// Replace an order already tracked by `EventDispatcher::orders()`.
    pub fn replace_tracked_order(
        &self,
        order: &crate::state::Order,
        order_type: crate::commands::trade::OrderType,
        new_price: f64,
    ) {
        self.replace_order(order.trade_ctx(), &order.market_name, order_type, new_price);
    }

    /// Send low-level `TAllStatusesReq`.
    ///
    /// This is fire-and-forget. Use [`Client::request_order_snapshot`] when the
    /// caller owns the `Client` and wants to wait for the applied snapshot.
    pub fn request_all_statuses(&self, uid: u64) {
        let raw = crate::commands::trade::build_all_statuses_request(uid);
        self.send_trade(raw, 3);
    }

    /// Send `TOrderCancelCommand` for one order.
    pub fn cancel_order(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        status: crate::commands::trade::OrderWorkerStatus,
    ) {
        let raw = crate::commands::trade::build_order_cancel(ctx, market, 0, status);
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
    }

    /// Cancel an order already tracked by `EventDispatcher::orders()`.
    pub fn cancel_tracked_order(&self, order: &crate::state::Order) {
        self.cancel_order(order.trade_ctx(), &order.market_name, order.status);
    }

    /// Send `TJoinOrdersCommand`.
    pub fn join_orders(&self, ctx: crate::commands::trade::TradeCtx, market: &str, is_short: bool) {
        let raw = crate::commands::trade::build_join_orders(ctx, market, is_short);
        self.send_trade(raw, 3);
    }

    /// Send `TSplitOrderCommand`.
    pub fn split_order(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        split_parts: i32,
        split_small: bool,
        split_small_sell: bool,
    ) {
        let raw = crate::commands::trade::build_split_order(
            ctx,
            market,
            split_parts,
            split_small,
            split_small_sell,
        );
        self.send_trade(raw, 3);
    }

    /// Split an order already tracked by `EventDispatcher::orders()`.
    pub fn split_tracked_order(
        &self,
        order: &crate::state::Order,
        split_parts: i32,
        split_small: bool,
        split_small_sell: bool,
    ) {
        self.split_order(
            order.trade_ctx(),
            &order.market_name,
            split_parts,
            split_small,
            split_small_sell,
        );
    }

    /// Send `TMoveAllSellsCommand`.
    pub fn move_all_sells(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        params: crate::commands::trade::MoveAllSellsParams,
    ) {
        let raw = crate::commands::trade::build_move_all_sells(ctx, market, params);
        self.send_trade(raw, 3);
    }

    /// Send `TDoClosePositionCommand` (`MaxRetries=1`).
    pub fn do_close_position(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        market_sell: bool,
    ) {
        let raw = crate::commands::trade::build_do_close_position(ctx, market, market_sell);
        self.send_trade(raw, 1);
    }

    /// Send `TDoLimitClosePositionCommand` (`MaxRetries=1`).
    pub fn do_limit_close_position(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        is_short: bool,
    ) {
        let raw = crate::commands::trade::build_do_limit_close_position(ctx, market, is_short);
        self.send_trade(raw, 1);
    }

    /// Send `TDoSplitPositionCommand` (`MaxRetries=1`).
    pub fn do_split_position(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        is_short: bool,
    ) {
        let raw = crate::commands::trade::build_do_split_position(ctx, market, is_short);
        self.send_trade(raw, 1);
    }

    /// Send `TDoSellOrderCommand` (`MaxRetries=1`).
    pub fn do_sell_order(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        price: f64,
        size: f64,
    ) {
        let raw = crate::commands::trade::build_do_sell_order(ctx, market, price, size);
        self.send_trade(raw, 1);
    }

    /// Send `TOrderStatusRequest`.
    pub fn request_order_status(&self, ctx: crate::commands::trade::TradeCtx, market: &str) {
        let raw = crate::commands::trade::build_order_status_request(ctx, market);
        self.send_trade(raw, 3);
    }

    /// Request a fresh status for an order already tracked by
    /// `EventDispatcher::orders()`.
    pub fn request_tracked_order_status(&self, order: &crate::state::Order) {
        self.request_order_status(order.trade_ctx(), &order.market_name);
    }

    /// Send `TOrderStopsUpdate` for one order.
    pub fn update_order_stops(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        status: crate::commands::trade::OrderWorkerStatus,
        stops: &crate::commands::trade::StopSettings,
    ) {
        let raw = crate::commands::trade::build_order_stops_update(ctx, market, 0, status, stops);
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
    }

    /// Update stops for an order already tracked by `EventDispatcher::orders()`.
    pub fn update_tracked_order_stops(
        &self,
        order: &crate::state::Order,
        stops: &crate::commands::trade::StopSettings,
    ) {
        self.update_order_stops(order.trade_ctx(), &order.market_name, order.status, stops);
    }

    /// Send `TTurnPanicSellCommand`.
    pub fn turn_panic_sell(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        turn_on: bool,
    ) {
        let raw = crate::commands::trade::build_turn_panic_sell(ctx, market, turn_on);
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
    }

    /// Toggle panic sell for an order already tracked by
    /// `EventDispatcher::orders()`.
    pub fn turn_tracked_order_panic_sell(&self, order: &crate::state::Order, turn_on: bool) {
        self.turn_panic_sell(order.trade_ctx(), &order.market_name, turn_on);
    }

    /// Send `TSetImmuneCommand` for a batch of order immunity flags.
    pub fn set_immune(&self, uid: u64, items: &[crate::commands::trade::ImmuneItem]) {
        let raw = crate::commands::trade::build_set_immune(uid, items);
        let items_uid_sum: u64 = items.iter().fold(0u64, |acc, it| acc.wrapping_add(it.uid));
        self.send_trade_keyed(raw, 3, UniqueKey::immune_clicks(items_uid_sum));
    }

    /// Send `TMoveAllBuysCommand`.
    pub fn move_all_buys(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        cmd_type: crate::commands::trade::MoveAllCmdType,
        move_kind: crate::commands::trade::ReplaceMultiKind,
        price: f64,
        side: crate::commands::trade::FixedPosition,
    ) {
        let raw = crate::commands::trade::build_move_all_buys(
            ctx,
            market,
            cmd_type as u8,
            move_kind,
            price,
            side,
        );
        self.send_trade(raw, 3);
    }

    /// Send `TVStopUpdate` for one order.
    pub fn update_vstop(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        params: crate::commands::trade::VStopUpdateParams,
    ) {
        let raw = crate::commands::trade::build_vstop_update(ctx, market, 0, params);
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
    }

    /// Update VStop for an order already tracked by `EventDispatcher::orders()`.
    pub fn update_tracked_order_vstop(
        &self,
        order: &crate::state::Order,
        vstop_on: bool,
        vstop_fixed: bool,
        vstop_level: f64,
        vstop_vol: f64,
    ) {
        self.update_vstop(
            order.trade_ctx(),
            &order.market_name,
            crate::commands::trade::VStopUpdateParams {
                status: order.status,
                vstop_on,
                vstop_fixed,
                vstop_level,
                vstop_vol,
            },
        );
    }

    /// Send `TDoMarketSplitPositionCommand` (`MaxRetries=1`).
    pub fn do_market_split_position(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        is_short: bool,
    ) {
        let raw = crate::commands::trade::build_do_market_split_position(ctx, market, is_short);
        self.send_trade(raw, 1);
    }

    /// Send `TPenaltyCommand`.
    pub fn penalty(&self, ctx: crate::commands::trade::TradeCtx, market: &str) {
        let raw = crate::commands::trade::build_penalty(ctx, market);
        self.send_trade(raw, 3);
    }

    /// Mark Delphi `ServerUpdateSent` from a thread-safe sender.
    ///
    /// Call this when sending raw UI update/switch payloads through
    /// [`Self::send_cmd`] rather than the typed wrappers below.
    pub fn mark_server_update_sent(&self) {
        self.server_update_sent.store(true, Ordering::Relaxed);
    }

    /// Send `TClientSettingsCommand`.
    pub fn ui_send_settings(&self, settings: &crate::commands::ui::ClientSettingsCommand) {
        let mut wire_settings = settings.clone();
        wire_settings.uid = rand::random();
        let raw = crate::commands::ui::build_client_settings(&wire_settings);
        self.send_cmd_keyed(
            raw,
            Command::UI,
            SendPriority::Sliced,
            true,
            6,
            UniqueKey::base_ui_settings_slot(),
        );
    }

    /// Send `TSettingsRequest`.
    pub fn ui_settings_request(&self) {
        let raw = crate::commands::ui::build_settings_request(rand::random());
        self.send_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// Send `TStratStartStopCommand`.
    pub fn ui_strat_start_stop(&self, is_start: bool) {
        let raw = crate::commands::ui::build_strat_start_stop(rand::random(), is_start);
        self.send_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// Send `TStratStartStopCommandV2` with checked strategy items.
    pub fn ui_strat_start_stop_v2(
        &self,
        is_start: bool,
        items: &[crate::commands::strat::StratCheckedItem],
    ) {
        let raw = crate::commands::ui::build_strat_start_stop_v2(rand::random(), is_start, items);
        self.send_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// Send `TMMOrdersSubscribeCommand`.
    pub fn ui_mm_subscribe(&self, subscribe: bool) {
        if let Err(e) = self.try_send(ClientEvent::MmOrdersSubscribe { subscribe }) {
            log::warn!(target: "moonproto::client",
                "ui_mm_subscribe({subscribe}) dropped: {e}");
        }
    }

    /// Send `TUpdateVersionCommand` and mark Delphi `ServerUpdateSent`.
    pub fn ui_update_version(&self, version_name: &str, is_release: bool) {
        let raw =
            crate::commands::ui::build_update_version(rand::random(), version_name, is_release);
        self.send_cmd(raw, Command::UI, SendPriority::High, true, 3);
        self.mark_server_update_sent();
    }

    /// Send `TEmuTradesCommand`.
    pub fn ui_emu_trades(
        &self,
        m_index: u16,
        base_time: f64,
        points: &[crate::commands::ui::EmuTradePoint],
    ) {
        let raw = crate::commands::ui::build_emu_trades(rand::random(), m_index, base_time, points);
        self.send_cmd(raw, Command::UI, SendPriority::Sliced, true, 6);
    }

    /// Send `TNewMarketNotifyCommand`.
    pub fn ui_new_market_notify(&self) {
        let raw = crate::commands::ui::build_new_market_notify(rand::random());
        self.send_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// Send `TLevManageCommand`.
    pub fn ui_lev_manage(&self, cmd: &crate::commands::ui::LevManage) {
        let uid: u64 = rand::random();
        let raw = crate::commands::ui::build_lev_manage(uid, cmd);
        self.send_cmd_keyed(
            raw,
            Command::UI,
            SendPriority::Sliced,
            true,
            6,
            UniqueKey::lev_manage_settings_slot(),
        );
    }

    /// Send `TTriggerManageCommand`.
    pub fn ui_trigger_manage(&self, action: u8, all_markets: bool, markets: &[u16], keys: &[u16]) {
        let raw = crate::commands::ui::build_trigger_manage(
            rand::random(),
            action,
            all_markets,
            markets,
            keys,
        );
        self.send_cmd(raw, Command::UI, SendPriority::Sliced, true, 6);
    }

    /// Send `TResetProfitCommand`.
    pub fn ui_reset_profit(&self, kind: u8) {
        let raw = crate::commands::ui::build_reset_profit(rand::random(), kind);
        self.send_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// Send `TArbActivateNotify`.
    pub fn ui_arb_activate_notify(&self, arb_valid: f64) {
        let raw = crate::commands::ui::build_arb_activate_notify(rand::random(), arb_valid);
        self.send_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// Send `TSwitchDexCommand` and mark Delphi `ServerUpdateSent`.
    pub fn ui_switch_dex(&self, dex_name: &str) {
        let uid = rand::random();
        let raw = crate::commands::ui::build_switch_dex(uid, dex_name);
        self.send_cmd_keyed(
            raw,
            Command::UI,
            SendPriority::High,
            true,
            3,
            UniqueKey::dex_switch_for(uid),
        );
        self.mark_server_update_sent();
    }

    /// Send `TSwitchSpotCommand` and mark Delphi `ServerUpdateSent`.
    pub fn ui_switch_spot(&self, spot_index: u8) {
        let uid = rand::random();
        let raw = crate::commands::ui::build_switch_spot(uid, spot_index);
        self.send_cmd_keyed(
            raw,
            Command::UI,
            SendPriority::High,
            true,
            3,
            UniqueKey::spot_switch_for(uid),
        );
        self.mark_server_update_sent();
    }

    /// Send `TStratSnapshotRequest`.
    pub fn strat_snapshot_request(&self) {
        let raw = crate::commands::strat::build_snapshot_request(rand::random());
        self.send_cmd(raw, Command::Strat, SendPriority::High, true, 3);
    }

    fn send_strat_snapshot_command(&self, raw: Vec<u8>) {
        self.send_cmd_keyed(
            raw,
            Command::Strat,
            SendPriority::Sliced,
            true,
            6,
            UniqueKey::strat_snapshot(),
        );
    }

    /// Send `TStratSnapshot` from an already serialized strategy payload.
    pub fn strat_send_snapshot_payload(
        &self,
        server_epoch: u64,
        client_max_last_date: u64,
        full: bool,
        data: &[u8],
    ) {
        let uid: u64 = rand::random();
        let raw = crate::commands::strat::build_snapshot(
            uid,
            server_epoch,
            client_max_last_date,
            full,
            data,
        );
        self.send_strat_snapshot_command(raw);
    }

    /// Send `TStratSnapshot` from decoded strategy snapshots.
    pub fn strat_send_snapshot_batch(
        &self,
        server_epoch: u64,
        full: bool,
        strategies: &[crate::commands::strategy_serializer::StrategySnapshot],
    ) {
        let uid: u64 = rand::random();
        let raw = crate::commands::strat::build_snapshot_from_strategies(
            uid,
            server_epoch,
            full,
            strategies,
        );
        self.send_strat_snapshot_command(raw);
    }

    /// Send `TStratDelete` for one strategy or folder.
    pub fn strat_delete(&self, strategy_id: u64, folder_path: &str) {
        let raw = crate::commands::strat::build_delete(rand::random(), strategy_id, folder_path);
        self.send_cmd(raw, Command::Strat, SendPriority::High, true, 3);
    }

    /// Send `TStratSellPriceUpdate` for one strategy.
    pub fn strat_sell_price_update(&self, strategy_id: u64, sell_price: f64) {
        let raw = crate::commands::strat::build_sell_price_update(
            rand::random(),
            strategy_id,
            sell_price,
        );
        self.send_cmd_keyed(
            raw,
            Command::Strat,
            SendPriority::High,
            true,
            3,
            UniqueKey::strat_sell_price_update(strategy_id),
        );
    }

    /// Send `TStratCheckedSync`.
    pub fn strat_checked_sync(
        &self,
        items: &[crate::commands::strat::StratCheckedItem],
        is_delta: bool,
    ) {
        let raw = crate::commands::strat::build_checked_sync(rand::random(), items, is_delta);
        self.send_cmd(raw, Command::Strat, SendPriority::Sliced, true, 6);
    }

    /// Send `TStratCheckedEcho`.
    pub fn strat_checked_echo(&self, items: &[crate::commands::strat::StratCheckedItem]) {
        let raw = crate::commands::strat::build_checked_echo(rand::random(), items);
        self.send_cmd(raw, Command::Strat, SendPriority::High, true, 3);
    }

    /// Send `TRequestBalanceRefresh`.
    pub fn balance_request_refresh(&self) {
        let raw = crate::commands::balance::build_request_balance_refresh(rand::random());
        self.send_cmd(raw, Command::Balance, SendPriority::High, true, 3);
    }

    fn try_send(&self, ev: ClientEvent) -> Result<(), SubscribeError> {
        if !self.app_queue_alive.load(Ordering::Relaxed) {
            return Err(SubscribeError::Disconnected);
        }
        self.app_events.lock().unwrap().push_back(ev);
        Ok(())
    }

    fn try_enqueue_send_item(&self, item: SendItem) -> Result<(), SubscribeError> {
        if !self.app_queue_alive.load(Ordering::Relaxed) {
            return Err(SubscribeError::Disconnected);
        }
        self.send_queues.lock().unwrap().push_send_cmd_int(item);
        Ok(())
    }
}

// A-V2-07 fix: бывший ручной impl Clone заменён на #[derive(Clone)] на RecvMsg выше.

/// Transport authorization state for one [`Client`].
///
/// This is a low-level diagnostic value. Most applications should watch
/// [`LifecycleEvent`] and use [`Client::is_authorized`] /
/// [`Client::is_domain_ready`] for coarse readiness.
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

/// Error returned by one-shot Engine API helpers such as
/// [`Client::request_balance`] and [`Client::request_coin_card_candles`].
#[derive(Debug, Clone, PartialEq)]
pub enum EngineRequestError {
    /// No response was delivered before the caller's timeout.
    Timeout,
    /// The pending response channel was closed, usually because the client loop
    /// cleared in-flight requests during reconnect or shutdown.
    Disconnected,
    /// The server returned an Engine API failure response.
    Server {
        /// Engine method that failed.
        method: EngineMethod,
        /// Server error code.
        code: i32,
        /// Server error message.
        message: String,
    },
    /// The server reported success, but the method-specific payload parser
    /// could not decode `EngineResponse::data`.
    MalformedPayload {
        /// Engine method whose successful payload was malformed.
        method: EngineMethod,
        /// Payload length in bytes.
        len: usize,
    },
}

impl From<mpsc::RecvTimeoutError> for EngineRequestError {
    fn from(value: mpsc::RecvTimeoutError) -> Self {
        match value {
            mpsc::RecvTimeoutError::Timeout => Self::Timeout,
            mpsc::RecvTimeoutError::Disconnected => Self::Disconnected,
        }
    }
}

impl std::fmt::Display for EngineRequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout => write!(f, "engine request timed out"),
            Self::Disconnected => write!(f, "engine request channel disconnected"),
            Self::Server {
                method,
                code,
                message,
            } => {
                write!(
                    f,
                    "engine request {method:?} failed with code {code}: {message}"
                )
            }
            Self::MalformedPayload { method, len } => {
                write!(
                    f,
                    "engine request {method:?} returned malformed payload ({len} bytes)"
                )
            }
        }
    }
}

impl std::error::Error for EngineRequestError {}

/// Error returned when a session-derived [`crate::commands::trade::TradeCtx`]
/// cannot be built yet.
///
/// Trade command wire headers carry two Delphi enum ordinals from the active
/// server session: `cfg.BaseCurrency` and `cfg.Header.Current`. They are learned
/// from `emk_BaseCheck`, so applications that skipped BaseCheck must either run
/// it or use the explicit low-level [`crate::commands::trade::TradeCtx::with_route`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeContextError {
    /// `ServerInfo::exchange_code` is missing.
    pub missing_exchange_code: bool,
    /// `ServerInfo::base_currency_code` is missing.
    pub missing_base_currency_code: bool,
}

impl TradeContextError {
    fn from_server_info(info: &ServerInfo) -> Option<Self> {
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
/// Register a callback with [`Client::on_lifecycle`]. The callback runs on the
/// same thread that executes the client loop.
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
/// reconnect in the same session restores fresh indexes, `UpdateMarketsList`,
/// and registry subscriptions automatically. The initial post-init resync
/// (orders, settings, balance, client strategy snapshot) is not repeated on
/// reconnect.
///
/// Applications should treat lifecycle events as UI/observability signals; they
/// do not need to run init again to keep requested streams alive.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LifecycleEvent {
    /// Handshake started (`Hello` sent), but `Fine` has not arrived yet.
    ///
    /// No application recovery action is required: the client retries and
    /// rotates local UDP bind ports by itself.
    Connecting,
    /// `Fine` received: the transport channel is authorized and can send or
    /// receive commands.
    ///
    /// `fresh = true` means this is the first connection since `Client::new`.
    /// The application can run `run_init_sequence` or use `connect_and_init`.
    ///
    /// `fresh = false` is a reconnect after link loss or server restart. If init
    /// already succeeded, the library restores indexes, `UpdateMarketsList`, and
    /// requested subscriptions; the application does not repeat init.
    Connected {
        /// `true` only for the first successful connection after `Client::new`;
        /// reconnects in the same client session use `false`.
        fresh: bool,
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

/// Lifecycle callback type registered with [`Client::on_lifecycle`].
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
/// Refresh ticks start after domain init completes (`connect_and_init` /
/// `run_init_sequence`). This keeps fresh BaseCheck/AuthCheck requests from
/// being queued behind background `UpdateMarketsList` traffic on cold connect.
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

const CHECK_TAGS_BURST_COUNT: u8 = 4;
const CHECK_TAGS_BURST_SPACING_MS: i64 = 200;

impl Default for RefreshConfig {
    fn default() -> Self {
        Self {
            update_markets_every: Some(Duration::from_secs(2)),
            check_tags_every: Some(Duration::from_secs(60)),
        }
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
    /// Transport mode: `0` for base transport, `1`/`2` for extended `moonext`.
    pub mask_ver: u8,
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
}

impl ClientConfig {
    /// Create config with production defaults for V0 (open base transport):
    /// - `mask_ver = 0`;
    /// - `client_id = rand::random()`;
    /// - `ntp_host = Some("pool.ntp.org")` (shared process-level syncer);
    /// - `refresh = RefreshConfig::default()` (Delphi-worker refresh after Init).
    ///
    /// Tests and offline tools can call [`Self::without_ntp`].
    /// Applications with extended transport can use [`Self::with_transport_mode`].
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
            mask_ver: 0,
            client_id: rand::random(),
            ntp_host: Some("pool.ntp.org".to_string()),
            refresh: RefreshConfig::default(),
        }
    }

    /// Override transport mode (`0` = base, `1/2` = extended and requires
    /// `moonext` availability).
    pub fn with_transport_mode(mut self, mask_ver: u8) -> Self {
        self.mask_ver = mask_ver;
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
}

// Custom Debug — secret keys redacted (audit rust_quality #20).
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
            .finish()
    }
}

/// Raw callback used by [`Client::run`].
///
/// This callback receives decoded MoonProto command payloads after transport
/// decrypt/decompress/group handling, but before `EventDispatcher` state
/// application. Regular applications should use [`Client::run_with_dispatcher`]
/// instead.
pub type OnDataFn = Box<dyn FnMut(Command, &[u8]) + Send>;

/// Callback that receives typed events from [`Client::run_with_dispatcher`].
pub type EventFn = Box<dyn FnMut(&crate::events::Event) + Send>;

/// Callback that receives an event plus the updated read-only dispatcher state.
///
/// Use this with [`Client::run_with_dispatcher_state`] when the event only
/// carries an id and the UI immediately needs the applied read model.
pub type EventWithStateFn =
    Box<dyn FnMut(&crate::events::Event, &crate::events::EventDispatcher) + Send>;

/// Куда доставлять `Command + payload` после внутренней обработки (decrypt,
/// decompress, Grouped split, API pending dispatch). Два варианта:
///
/// * `Callback` — старый путь через `OnDataFn` (используется `Client::run`).
/// * `Buffer` — буфер (Command, Vec<u8>) для пост-обработки через
///   `EventDispatcher` (используется `Client::run_with_dispatcher`).
///
/// Этот enum позволяет one-and-the-same internal pipeline (`handle_udp_command`
/// и др.) обслуживать оба сценария без `Arc<Mutex>`-обходов borrow checker.
pub(crate) enum DispatchSink<'a> {
    Callback(&'a mut OnDataFn),
    Buffer(&'a mut Vec<(Command, Vec<u8>)>),
}

impl<'a> DispatchSink<'a> {
    #[inline]
    fn is_buffer(&self) -> bool {
        matches!(self, Self::Buffer(_))
    }

    /// Доставка по ссылке — копия только для Buffer ветки.
    #[cfg(test)]
    #[inline]
    fn deliver(&mut self, cmd: Command, payload: &[u8]) {
        match self {
            Self::Callback(cb) => cb(cmd, payload),
            Self::Buffer(buf) => buf.push((cmd, payload.to_vec())),
        }
    }

    /// Доставка с уже-владеемым Vec (avoid лишний `to_vec`, когда payload
    /// родился из decrypt/decompress и уже Owned).
    #[inline]
    fn deliver_owned(&mut self, cmd: Command, payload: Vec<u8>) {
        match self {
            Self::Callback(cb) => cb(cmd, &payload),
            Self::Buffer(buf) => buf.push((cmd, payload)),
        }
    }
}

/// Режим работы main loop — определяет как доставлять входящие data-пакеты
/// и нужны ли active-library auto-actions (periodic trades tick).
///
/// `Callback` — backwards-compat path для `Client::run`. Потребитель получает
/// сырые `(Command, &[u8])` и сам решает что с ними делать (обычно — свой
/// `dispatcher.dispatch_into(...)`).
///
/// `Dispatcher` — active-library path для `Client::run_with_dispatcher`. Liба
/// сама пропускает data-пакеты через `EventDispatcher::dispatch_into_active_actions`,
/// делает auto-actions (RequestOrderBookFull, periodic trades.tick, indexes
/// sync gate), потребитель получает уже разобранные типизированные `Event`.
pub(crate) enum RunMode<'a> {
    Callback {
        on_data: OnDataFn,
    },
    Dispatcher {
        dispatcher: &'a mut crate::events::EventDispatcher,
        on_event: DispatcherEventFn,
        /// Переиспользуемый буфер событий (избегаем alloc per packet).
        event_buf: Vec<crate::events::Event>,
        /// Переиспользуемый буфер payload'ов из handle_udp_command.
        payload_buf: Vec<(Command, Vec<u8>)>,
        /// Переиспользуемый буфер active-library side effects.
        active_actions_buf: Vec<crate::events::ActiveAction>,
    },
}

/// Два варианта event callback'а: только `&Event` или `(&Event, &EventDispatcher)`.
/// Изоляция позволяет иметь два публичных метода (`run_with_dispatcher` /
/// `run_with_dispatcher_state`) без дубликации main loop кода.
pub(crate) enum DispatcherEventFn {
    EventOnly(EventFn),
    EventWithState(EventWithStateFn),
    Queue,
}

impl DispatcherEventFn {
    fn drain_events(
        &mut self,
        events: &mut Vec<crate::events::Event>,
        dispatcher: &mut crate::events::EventDispatcher,
    ) {
        match self {
            Self::EventOnly(cb) => {
                for event in events.iter() {
                    cb(event);
                }
                events.clear();
            }
            Self::EventWithState(cb) => {
                for event in events.iter() {
                    cb(event, dispatcher);
                }
                events.clear();
            }
            Self::Queue => {
                dispatcher.queue_events(events.drain(..));
            }
        }
    }
}

// =============================================================================
//  Subscription Registry — active library principle
//
//  Хранит ВОЛЮ потребителя: какие streams подписаны и с какими параметрами.
//  До первого Init transport handshake этот реестр не отправляет. После Init
//  (`domain_ready=true`) reconnect внутри той же Client-сессии сам восстанавливает
//  registry, чтобы пользователь НЕ запускал Init второй раз.
//
//  Ключ orderbook — `market_name` (стабилен через reindex), не `market_idx`
//  (последний меняется при ServerRestart). Аналог Delphi
//  `MoonProtoEngine.pas:305-360 BookSubbed: TSet<TMarket>`.
// =============================================================================

/// Stored all-trades subscription intent.
///
/// `want_mm` is retained so init and reconnect restore can replay the exact
/// server-side subscription parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradesSubscription {
    /// Whether market-maker order sections should be included in the trades
    /// stream.
    pub want_mm: bool,
}

/// Реестр подписок — что app просил, что либа обязана поддерживать на протяжении сессии.
///
/// Transport handshake сам подписки не шлет: registry применяется только из init/API
/// слоя, чтобы `Fine` оставался Delphi-тождественным auth-блоком.
#[derive(Default)]
pub(crate) struct SubscriptionRegistry {
    pub orderbook_subs: HashSet<String>,
    pub trades_sub: Option<TradesSubscription>,
    /// Последний серверный флаг `IsMMOrdersSubscribed`.
    ///
    /// Delphi обновляет его двумя путями: `emk_SubscribeAllTrades` с bool-параметром
    /// и прямой `TMMOrdersSubscribeCommand` из UI/strategy state. После reconnect
    /// новый серверный client-state стартует с false, поэтому active library должна
    /// воспроизвести последний известный intent в init/API слое.
    pub mm_orders_sub: Option<bool>,
}

/// Что единственный пользовательский Init заказал у доменного слоя.
///
/// Инвариант: `run_init_sequence` вызывается один раз за жизнь `Client`-сессии.
/// После этого reconnect не требует повторного Init: transport после нового
/// `Fine` восстанавливает только эти сохранённые intent'ы и registry-подписки.
#[derive(Debug, Clone, Copy, Default)]
struct DomainRestoreIntent {
    fetch_indexes: bool,
}

// =============================================================================
//  CandlesAggregator async API
// =============================================================================

/// Merged result returned by `api_request_candles_data_async`.
///
/// The server answers `RequestCandlesData` with several `EngineResponse` chunks
/// sharing one `request_uid`. The library aggregates those chunks through
/// [`CandlesAggregator`] and returns both the merged zlib stream and parsed
/// market entries.
#[derive(Debug, Clone)]
pub struct MergedCandles {
    /// Request UID used to correlate the chunked response.
    pub uid: u64,
    /// Merged zlib stream from Delphi `TMarkets.StoreCandlesToZip`.
    pub zipped_data: Vec<u8>,
    /// Parsed market entries from the zipped stream.
    pub markets: Vec<RequestCandlesMarket>,
}

/// Внутреннее состояние частично собранного набора свечей.
struct PartialCandles {
    aggregator: CandlesAggregator,
    /// Sender который будет уведомлён когда aggregator вернёт merged.
    sender: mpsc::Sender<MergedCandles>,
    /// Timestamp регистрации / последнего сохранённого нового chunk.
    last_activity_ms: i64,
}

/// Timeout for pending candle aggregation, measured from the last accepted chunk.
///
/// Delphi `DataUpdaters.pas -> CheckGlass` waits 15 seconds from
/// `Markets.LastChunkTime`.
pub const DEFAULT_PENDING_CANDLES_TIMEOUT_MS: i64 = 15_000;

/// Sent Sliced datagram awaiting ACK (matches TMoonProtoSlicedData in Sending list)
struct SentSliced {
    datagram_num: u16,
    slices: Vec<Vec<u8>>,         // each slice payload (SliceHeader + data)
    piece_last_checked: Vec<i64>, // per-piece LastChecked timestamp
    ack_flags: [u8; 32],          // which blocks ACK'd
    blocks_count: usize,
    sent_count: usize,
    last_checked: i64, // Min of all piece_last_checked
    retry_count: i32,
    max_retry_count: i32,
    u_key: UniqueKey, // for UKey dedup (matches TMoonProtoSlicedData.UKey)
}

impl SentSliced {
    #[inline]
    fn is_block_acked(&self, block_num: usize) -> bool {
        self.ack_flags[block_num / 8] & (1 << (block_num % 8)) != 0
    }

    fn refresh_last_checked_from_unacked(&mut self, fallback: i64) {
        self.last_checked = (0..self.blocks_count)
            .filter(|&block| !self.is_block_acked(block))
            .map(|block| self.piece_last_checked[block])
            .min()
            .unwrap_or(fallback);
    }
}

#[derive(Clone, Copy)]
struct SlicedAck {
    flags: [u8; 32],
    datagram_num: u16,
}

struct ReaderProtocolState {
    decode_cipher: Option<crate::crypto::Aes128Gcm>,
    slider: Slider,
    tmp_slider: Slider,
    data_size_ack_series_num: u16,
}

impl ReaderProtocolState {
    fn new() -> Self {
        Self {
            decode_cipher: None,
            slider: Slider::new(),
            tmp_slider: Slider::new(),
            data_size_ack_series_num: 0,
        }
    }

    fn reset(&mut self) {
        self.slider = Slider::new();
        self.tmp_slider = Slider::new();
        self.data_size_ack_series_num = 0;
    }

    fn set_decode_cipher(&mut self, cipher: crate::crypto::Aes128Gcm) {
        self.decode_cipher = Some(cipher);
    }

    fn copy_tmp_slider(&mut self) -> Option<Slider> {
        let has_new_data = self.tmp_slider.has_new_data;
        let copied = has_new_data.then(|| self.tmp_slider.clone());
        self.tmp_slider.has_new_data = false;
        copied
    }

    fn apply_ping_ack_bitmap(&mut self, payload: &[u8]) {
        // DataReadInt(MPC_Ping): parse server's ACK bitmap into TmpSlider only.
        // Delphi drops PendingH later in writer CheckSeningData via
        // CopyRecvdData -> ApplyRegularHLAck.
        if payload.len() > 50 {
            let srv_ack_start = u64::from_le_bytes(payload[42..50].try_into().unwrap());
            let ack_data_len = payload.len() - 50;
            let r_count = (ack_data_len / 8).min(64);
            let mut bits = [0u64; 64];
            for i in 0..r_count {
                bits[i] =
                    u64::from_le_bytes(payload[50 + i * 8..50 + i * 8 + 8].try_into().unwrap());
            }
            self.tmp_slider.bit_field = bits;
            self.tmp_slider.start_num = srv_ack_start;
            self.tmp_slider.has_new_data = true;
            self.tmp_slider.r_count = r_count as i32;
        }
    }

    fn build_ack_half(&self) -> (u64, Vec<u64>) {
        self.slider.build_ack_half()
    }

    fn update_data_size_ack_series_num(&mut self, series_num: u16) -> u16 {
        if self.data_size_ack_series_num != series_num {
            self.data_size_ack_series_num = series_num;
        }
        self.data_size_ack_series_num
    }
}

struct ReaderPingState {
    ping_count: u32,
    can_send_rate: i32,
    used_sliced_limit: bool,
}

impl ReaderPingState {
    fn new() -> Self {
        Self {
            ping_count: 0,
            can_send_rate: 2 * 1024 * 1024,
            used_sliced_limit: false,
        }
    }

    fn reset_protocol_session(&mut self) {
        self.used_sliced_limit = false;
    }

    #[cfg(test)]
    fn sync_from_main(&mut self, ping_count: u32, can_send_rate: i32, used_sliced_limit: bool) {
        self.ping_count = ping_count;
        self.can_send_rate = can_send_rate;
        self.used_sliced_limit = used_sliced_limit;
    }
}

/// Public handle to the client. Allows sending commands from any thread.
pub struct Client {
    cfg: ClientConfig,

    // Reader → main control FIFO. Production reader sends only coalesced Wake
    // notifications; accepted UDP packets are not queued here. Test builds keep
    // a legacy Recv variant for parity regression tests.
    event_tx: mpsc::Sender<ClientEvent>,
    pub(crate) event_rx: mpsc::Receiver<ClientEvent>,

    // App/control → main FIFO: registry-changing intents that still require
    // `&mut Client`. Raw SendCmd no longer uses this queue; it appends directly
    // into Delphi-style `send_queues` below.
    app_events: Arc<Mutex<VecDeque<ClientEvent>>>,
    app_queue_alive: Arc<AtomicBool>,
    // Delphi `DataToSend`, `DataToSendH`, `DataToSendL`: raw/user/API sends are
    // appended here directly by `send_cmd` / `ClientSender::send_cmd`.
    send_queues: Arc<Mutex<SendQueues>>,

    // Pending H-commands (main thread only, no sharing)
    pending_h: Vec<SendItem>,
    // Sent Sliced datagrams awaiting ACK (matches TMoonProtoClient.Sending)
    sending: Vec<SentSliced>,
    // Reader -> writer SlicedACK list (matches TMoonProtoBaseNet.ACKs).
    incoming_sliced_acks: Arc<Mutex<Vec<SlicedAck>>>,
    // Reader -> main decoded OnNewData payloads. Completed incoming Sliced
    // datagrams run the DataReadInt decrypt/decompress core in the reader stack
    // and enter this queue only for user/active-library delivery.
    pending_reader_decoded: Arc<Mutex<Vec<ReaderDecodedMsg>>>,
    // Reader wake is a level-triggered notification for pending_reader_decoded:
    // dense reader-side DataReadInt progress must not create an empty Wake
    // backlog in the shared main-loop FIFO.
    reader_wake_pending: Arc<AtomicBool>,

    // Main thread state
    socket: Option<UdpSocket>,
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
    last_cleanup: i64,
    prev_cycle_tm: i64, // for ActualSleepTime EMA

    crypt_msg_counter: u64,
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

    // Reader-shared part of Delphi DataReadInt:
    // MPSlider replay/ACK bitmap, TmpSlider, and decode cipher. Delphi mutates
    // these from UDPRead/reader thread and CheckSeningData copies TmpSlider from
    // writer thread under SendLock.
    reader_protocol: Arc<Mutex<ReaderProtocolState>>,
    // Reader-owned Ping block state used to send `MPC_Ping` replies from
    // UDPRead order before main/writer observes the packet.
    reader_ping_state: Arc<Mutex<ReaderPingState>>,
    // Delphi RecvdSlider/TmpSlider: server ACK bitmap from incoming MPC_Ping.
    // Reader/DataReadInt writes TmpSlider; writer CheckSeningData copies it to
    // RecvdSlider and only then drops ACKed PendingH.
    recvd_slider: Slider,
    slicer: Arc<Mutex<slicing::SlicingReceiver>>,
    total_sent: Arc<AtomicU64>,
    total_recv_shared: Arc<AtomicU64>,
    next_port: u16,
    ping_count: u32,

    /// Реестр pending Engine API запросов.
    /// При получении `Command::API` пакета — `dispatch` доставит response
    /// в зарегистрированный receiver, если UID найден.
    api_pending: Arc<ApiPending>,

    /// Lifecycle callback — вызывается при изменении статуса канала (Connecting → Connected{fresh} → Reconnecting/Disconnected).
    /// Установить через `client.on_lifecycle(cb)`. Опционально.
    lifecycle_cb: Option<LifecycleFn>,
    /// Delphi `cfg.MoonProtoConfig.ServerUpdateSent`: set by UI commands that
    /// can make the server restart/change routing; consumed by BaseCheck init.
    server_update_sent: Arc<AtomicBool>,
    /// Предыдущий auth_status (для детектирования переходов).
    prev_auth_status: AuthStatus,

    /// Shutdown signal для reader thread.
    /// `spawn_reader` создаёт НОВЫЙ `Arc<AtomicBool>` для каждого reader thread и сохраняет
    /// его сюда. При `do_force_disconnect` / `Drop` мы ставим `true` — reader thread выйдет
    /// из loop (макс через `read_timeout` = 1s).
    /// Каждый новый reader получает свой Arc → старый и новый reader НЕ конфликтуют.
    reader_shutdown: Arc<AtomicBool>,
    /// Аудит #7 (audit_delphi_deviation E-V2-02): инкремент на каждый `spawn_reader`.
    /// Reader thread получает копию текущего значения и проставляет её в
    /// reader-owned decoded/control records. Main loop фильтрует stale reader
    /// work с epoch != этого значения. Защита от race на reconnect (старый
    /// reader может ещё крутиться 1с пока read_timeout сработает).
    current_reader_epoch: u32,

    /// Кэш разрешённого адреса сервера. Закрывает B-05: до этого `server_addr()` форматировал
    /// строку + `send_to(&str)` делал `getaddrinfo` resolve на каждый send (потенциально DNS-блокирующий).
    /// Кэш сбрасывается при ошибке resolve (например, DNS отвалился) — на следующем bind_socket
    /// повторно резолвится.
    cached_server_addr: Option<SocketAddr>,

    /// **Active library — subscription registry**: что app просил подписать.
    /// До Init transport handshake не отправляет этот реестр. После Init reconnect
    /// сам восстанавливает registry через текущие keys / market mapping.
    pub(crate) subscription_registry: SubscriptionRegistry,

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
    /// Используется в `handle_handshake` и `handle_ping` для детекции server restart:
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

    /// FireTest-only hook: drop every outgoing datagram before socket send.
    /// This lets the live health test force a real server-side disconnect and
    /// then verify the library reconnect path. It is deliberately hidden from
    /// public API docs.
    debug_outgoing_blackhole: bool,

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

    /// Последнее process-global поколение clock-jump, обработанное этим Client.
    /// В multi-server процессе один скачок часов должен быть виден каждому
    /// соединению, поэтому глобальный сигнал нельзя consume'ить через AtomicBool.
    seen_clock_jump_generation: u64,

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

    /// Delphi `InitDone`: transport auth уже завершён, но domain-пуши
    /// (`Order`/`Strat`/`Balance`/`Trades*`/`OrderBook`/`UI`) можно применять
    /// только после полного init bootstrap. До этого `dispatch_into_active`
    /// дропает эти каналы, как `TMoonProtoNetClient.ClientNewData`.
    domain_ready: bool,

    /// **Per-Client ServerTimeDelta handle** — shareable через `Arc::clone`.
    ///
    /// Хранит текущий `ServerTimeDelta` (в днях, TDateTime-формат, упакован в u64
    /// через `f64::to_bits`). Обновляется в `handle_ping` синхронно с
    /// `self.server_time_delta` и (для back-compat) с глобальным
    /// `SERVER_TIME_DELTA_DAYS`.
    ///
    /// **Multi-Client** (DEVIATION #23): `EventDispatcher` должен быть привязан к
    /// этому handle через `EventDispatcher::set_server_time_delta_source(handle)`
    /// или автоматически через `run_with_dispatcher` / `dispatch_into_active`. Без
    /// привязки EventDispatcher падает обратно на global, что при multi-Client даёт
    /// off-by-50-1000ms timestamps в ордерах (последний Client перезаписывает
    /// delta всех остальных).
    server_time_delta_handle: Arc<std::sync::atomic::AtomicU64>,

    /// Cached MAC context — один раз вычисленные ipad CRC + opad block для `cfg.mac_key`.
    /// Используется в transport_pack/unpack hot-path вместо пересчёта HMAC ipad/opad
    /// (128 XOR + crc32c) на каждом пакете. См. `moonproto_transport::MacContext`.
    ///
    /// Поскольку `mac_key` фиксирован на всю life Client'а (приходит в ClientConfig
    /// и не меняется) — этот context тоже фиксирован. Clone() в `spawn_reader`
    /// для передачи в reader thread.
    mac_ctx: moonproto_transport::MacContext,

    /// Reusable buffer для `transport_pack_into_with_mac` — экономит alloc/dealloc на каждый
    /// исходящий пакет. Capacity растёт до peak packet size и переиспользуется.
    /// audit_rust_quality #4: 50K pps × 1500б = 75 MB/s allocator pressure eximinated.
    send_buf: Vec<u8>,
}

impl Client {
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
        // Reader packets, app/control intents, and raw send queues are separate
        // so dense incoming streams cannot keep user/API sends behind recv backlog.
        let (event_tx, event_rx) = mpsc::channel();
        let app_events = Arc::new(Mutex::new(VecDeque::new()));
        let app_queue_alive = Arc::new(AtomicBool::new(true));
        let send_queues = Arc::new(Mutex::new(SendQueues::default()));
        let incoming_sliced_acks = Arc::new(Mutex::new(Vec::new()));
        let pending_reader_decoded = Arc::new(Mutex::new(Vec::new()));
        let reader_wake_pending = Arc::new(AtomicBool::new(false));
        let reader_protocol = Arc::new(Mutex::new(ReaderProtocolState::new()));
        let reader_ping_state = Arc::new(Mutex::new(ReaderPingState::new()));
        let total_recv_shared = Arc::new(AtomicU64::new(0));

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
            event_tx,
            event_rx,
            app_events,
            app_queue_alive,
            send_queues,
            pending_h: Vec::new(),
            sending: Vec::new(),
            incoming_sliced_acks,
            pending_reader_decoded,
            reader_wake_pending,
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
            last_cleanup: i64::MIN / 2,
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
            reader_protocol,
            reader_ping_state,
            recvd_slider: Slider::new(),
            slicer: Arc::new(Mutex::new(slicing::SlicingReceiver::new())),
            total_sent: Arc::new(AtomicU64::new(0)),
            total_recv_shared,
            next_port: 1024 + (rand::random::<u16>() % (65000 - 1024)),
            ping_count: 0,
            api_pending: ApiPending::new_arc(),
            lifecycle_cb: None,
            server_update_sent: Arc::new(AtomicBool::new(false)),
            prev_auth_status: AuthStatus::Base,
            reader_shutdown: Arc::new(AtomicBool::new(false)),
            current_reader_epoch: 0,
            cached_server_addr: None,
            subscription_registry: SubscriptionRegistry::default(),
            domain_restore: DomainRestoreIntent::default(),
            was_ever_connected: false,
            pending_candles: HashMap::new(),
            tracked_indexes_peer_app_token: 0,
            indexes_fetch_in_flight: false,
            update_markets_after_indexes: false,
            debug_outgoing_blackhole: false,
            indexes_fetch_started_ms: 0,
            last_trades_tick_ms: i64::MIN / 2,
            bind_failure_streak: 0,
            first_bind_failure_ms: NEVER_TIME_MS,
            last_bind_failed_event_ms: NEVER_TIME_MS,
            seen_clock_jump_generation: CLOCK_JUMP_GENERATION.load(Ordering::Relaxed),
            _ntp_process_guard: ntp_process_guard,
            server_time_delta_handle: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            server_info: crate::commands::engine_api::ServerInfo::default(),
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

    /// Установить `ServerInfo` вручную. Обычно не нужно — `run_init_sequence` делает
    /// это автоматически. Полезно если приложение использует свой init pattern
    /// (минуя `run_init_sequence`) и хочет вручную распарсить ответ `api_base_check`.
    pub fn set_server_info(&mut self, info: crate::commands::engine_api::ServerInfo) {
        self.server_info = info;
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
    pub(crate) fn testing_set_domain_ready(&mut self, ready: bool) {
        self.domain_ready = ready;
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
    /// Если использовать `Client::run_with_dispatcher` или
    /// `EventDispatcher::dispatch_into_active(&mut Client)` — линковка делается
    /// автоматически на первом вызове (lazy).
    ///
    /// См. `DEVIATION.md #23` (single-Client → multi-Client refactor).
    pub fn server_time_delta_handle(&self) -> Arc<std::sync::atomic::AtomicU64> {
        Arc::clone(&self.server_time_delta_handle)
    }

    /// Установить lifecycle callback. Вызывается из main-thread при изменении auth_status.
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

    /// Public API: queue a command for sending through the owning client loop.
    ///
    /// The command is appended directly to the unbounded Delphi-style
    /// `DataToSend*` queue for its priority, separate from accepted UDP packets
    /// and app/control events. This API has no local capacity-drop branch.
    ///
    /// E-V2-06: возвращает `()`, **но** при закрытом канале (main loop завершён)
    /// логирует error через `log` crate. Потерянная команда — серьёзный сигнал,
    /// но возвращать Result сломало бы API всех Client wrappers (`client.new_order(...)`
    /// и т.д.). Если потребителю нужен гарантированный feedback — он может
    /// проверить статус через `LifecycleEvent::Disconnected` callback и не
    /// шарашить новые команды после.
    ///
    /// **QUEUE BEHAVIOR:** internal send queues are unbounded. This matches
    /// Delphi `MoonProtoCommon.pas:765 SendCmdInt`: user commands are appended
    /// to protocol queues without a fixed capacity cap. `send_cmd` does not
    /// block on local queue fullness and never silently drops a trading/API
    /// command because the Rust main loop is busy. If the client is gone, the
    /// command is rejected and the error is logged.
    pub fn send_cmd(
        &self,
        data: Vec<u8>,
        cmd: Command,
        priority: SendPriority,
        encrypted: bool,
        max_retries: i32,
    ) {
        self.send_cmd_keyed(
            data,
            cmd,
            priority,
            encrypted,
            max_retries,
            UniqueKey::none(),
        );
    }

    /// Public API: queue a command with an explicit Delphi UKey dedup key.
    ///
    /// Use this only for advanced tools that already know the correct UKey
    /// semantics. Regular applications should use typed `Client` wrappers or
    /// [`ClientSender`], which choose the correct key, priority, encryption, and
    /// retry count.
    pub fn send_cmd_keyed(
        &self,
        data: Vec<u8>,
        cmd: Command,
        priority: SendPriority,
        encrypted: bool,
        max_retries: i32,
        u_key: UniqueKey,
    ) {
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
        // Delphi `SendCmdInt`: append into DataToSend/DataToSendH/DataToSendL
        // under SendLock. The writer tick later copies those lists; raw sends do
        // not wait behind the app/control FIFO.
        if self.enqueue_send_item(item).is_err() {
            log::error!(target: "moonproto::client",
                "send_cmd: send queues closed (client dropped?) — packet cmd={:?} priority={:?} dropped",
                cmd, priority);
        }
    }

    fn enqueue_send_item(&self, item: SendItem) -> Result<(), SubscribeError> {
        if !self.app_queue_alive.load(Ordering::Relaxed) {
            return Err(SubscribeError::Disconnected);
        }
        self.send_queues.lock().unwrap().push_send_cmd_int(item);
        Ok(())
    }

    fn pop_app_event(&self) -> Option<ClientEvent> {
        self.app_events.lock().unwrap().pop_front()
    }

    #[cfg(test)]
    pub(crate) fn drain_app_events_for_test(&self) -> Vec<ClientEvent> {
        self.app_events.lock().unwrap().drain(..).collect()
    }

    #[cfg(test)]
    pub(crate) fn take_send_queues_for_test(
        &self,
    ) -> (Vec<SendItem>, Vec<SendItem>, Vec<SendItem>) {
        let mut sliced = Vec::new();
        let mut high = Vec::new();
        let mut low = Vec::new();
        self.send_queues
            .lock()
            .unwrap()
            .take_into(&mut sliced, &mut high, &mut low);
        (sliced, high, low)
    }

    /// Convenience: send an Engine API request (MPS_Sliced, encrypted, MaxRetries=6).
    /// Matches Delphi: `TEngineRequest` has explicit `MoonCmdPriority(MPS_Sliced)`,
    /// and `TCommandRegistry.InitRegistry` gives Sliced commands `MaxRetries=6`.
    pub fn send_api_request(&self, request_payload: &[u8]) {
        self.send_cmd(
            request_payload.to_vec(),
            Command::API,
            SendPriority::Sliced,
            true, // Engine API is always encrypted
            6,    // TEngineRequest effective MaxRetries for MPS_Sliced
        );
    }

    /// Send an Engine API request and register it in `api_pending`.
    ///
    /// The UID is read from the payload at offset `3..11` in the
    /// `TBaseCommand` header. In single-threaded consumer code, prefer
    /// [`Self::request_engine_response`] or wait for the returned receiver
    /// through [`Self::run_until_response`] so the UDP loop keeps running.
    /// Direct `rx.recv_timeout(...)` is only correct when another thread is
    /// already running the client loop.
    ///
    /// One-shot request helpers remove the pending slot when the caller's
    /// timeout expires. Raw receiver users should keep pumping the client until
    /// the response arrives or use [`Self::request_engine_response`] when they
    /// need timeout-owned cleanup.
    pub fn send_api_request_async(&self, request_payload: &[u8]) -> mpsc::Receiver<EngineResponse> {
        // D-V2-01 fix: безопасный slice-доступ к uid. Старая версия `request_payload[3..11]`
        // паниковала при len<11 — public API не должен валить процесс из-за плохого input'а.
        let Some(uid) = engine_request_uid(request_payload) else {
            log::warn!(target: "moonproto::client",
                "send_api_request_async: malformed Engine API request ({} bytes) — not queued",
                request_payload.len());
            let (_tx, rx) = mpsc::channel();
            return rx;
        };
        let rx = self.api_pending.register(uid);
        self.send_api_request(request_payload);
        rx
    }

    /// Send one Engine API request and wait for the matching `EngineResponse`
    /// while the client loop keeps running.
    ///
    /// This is the one-shot counterpart to [`Self::send_api_request_async`].
    /// It is the preferred single-threaded API when the caller wants a direct
    /// request/response operation: it registers the pending UID, sends the
    /// request, pumps [`Self::run_with_dispatcher`] in short ticks, and removes
    /// the pending slot if the caller's timeout expires.
    pub fn request_engine_response(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        request_payload: &[u8],
        timeout: Duration,
    ) -> Result<EngineResponse, mpsc::RecvTimeoutError> {
        let uid = engine_request_uid(request_payload);
        let rx = self.send_api_request_async(request_payload);
        match self.run_until_response(dispatcher, &rx, timeout) {
            Ok(resp) => Ok(resp),
            Err(err) => {
                if let Some(uid) = uid {
                    self.api_pending.remove(uid);
                }
                Err(err)
            }
        }
    }

    fn request_engine_parsed<T>(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        request_payload: &[u8],
        timeout: Duration,
        parse: impl FnOnce(&[u8]) -> Option<T>,
    ) -> Result<T, EngineRequestError> {
        let resp = self
            .request_engine_response(dispatcher, request_payload, timeout)
            .map_err(EngineRequestError::from)?;

        if !resp.success {
            return Err(EngineRequestError::Server {
                method: resp.method,
                code: resp.error_code,
                message: resp.error_msg,
            });
        }

        let method = resp.method;
        let len = resp.data.len();
        parse(&resp.data).ok_or(EngineRequestError::MalformedPayload { method, len })
    }

    /// Run `emk_BaseCheck`, store the returned server identity in
    /// [`Self::server_info`], and return it.
    pub fn request_base_check(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        timeout: Duration,
    ) -> Result<ServerInfo, EngineRequestError> {
        let resp = self
            .request_engine_response(
                dispatcher,
                &crate::commands::engine_request::base_check(),
                timeout,
            )
            .map_err(EngineRequestError::from)?;

        if !resp.success {
            return Err(EngineRequestError::Server {
                method: resp.method,
                code: resp.error_code,
                message: resp.error_msg,
            });
        }

        let info = parse_base_check_response(&resp.data);
        self.set_server_info(info.clone());
        Ok(info)
    }

    /// Run `emk_AuthCheck` and parse the account metadata payload.
    pub fn request_auth_check(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        timeout: Duration,
    ) -> Result<AuthCheckResponse, EngineRequestError> {
        self.request_engine_parsed(
            dispatcher,
            &crate::commands::engine_request::auth_check(),
            timeout,
            parse_auth_check_response,
        )
    }

    /// Run `emk_GetBalance` and parse the returned quantity.
    pub fn request_balance(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        currency: &str,
        timeout: Duration,
    ) -> Result<f64, EngineRequestError> {
        self.request_engine_parsed(
            dispatcher,
            &crate::commands::engine_request::get_balance(currency),
            timeout,
            parse_get_balance_response,
        )
    }

    /// Run `emk_QueryHedgeMode` and parse the returned hedge-mode flag.
    pub fn request_hedge_mode(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        timeout: Duration,
    ) -> Result<bool, EngineRequestError> {
        self.request_engine_parsed(
            dispatcher,
            &crate::commands::engine_request::query_hedge_mode(),
            timeout,
            parse_query_hedge_mode_response,
        )
    }

    /// Run `emk_CheckAPIExpirationTime` and parse the returned API-key expiration time.
    pub fn request_api_expiration_time(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        timeout: Duration,
    ) -> Result<ApiExpirationTime, EngineRequestError> {
        self.request_engine_parsed(
            dispatcher,
            &crate::commands::engine_request::check_api_expiration_time(),
            timeout,
            parse_api_expiration_time_response,
        )
    }

    /// Run `emk_UpdateTransferAssets` and parse the transferable asset rows.
    ///
    /// `kind` is the server's exchange-wallet kind ordinal. The response rows
    /// contain the asset symbol, transferable amount, and total amount reported
    /// by the server.
    pub fn request_transfer_assets(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        kind: u8,
        timeout: Duration,
    ) -> Result<Vec<TransferAsset>, EngineRequestError> {
        self.request_engine_parsed(
            dispatcher,
            &crate::commands::engine_request::update_transfer_assets(kind),
            timeout,
            parse_update_transfer_assets_response,
        )
    }

    /// Run `emk_GetCoinCardCandles` and parse the returned historical candles.
    pub fn request_coin_card_candles(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        market: &str,
        ticks: crate::commands::candles::DeepHistoryKind,
        timeout: Duration,
    ) -> Result<Vec<DeepPrice>, EngineRequestError> {
        self.request_engine_parsed(
            dispatcher,
            &crate::commands::candles::get_coin_card_candles(market, ticks),
            timeout,
            parse_coin_card_candles_response,
        )
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

    /// `emk_GetMarketsBalanceFull` — trigger server-side full balance refresh.
    ///
    /// The current Delphi server does not serialize a balance snapshot in this
    /// response yet, so a successful response normally has empty `data`.
    pub fn api_get_markets_balance_full(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::get_markets_balance_full())
    }

    /// `emk_GetOrder` by order UID.
    ///
    /// The current Delphi reference server has no request-handler branch for this
    /// method and returns `Unknown method`.
    pub fn api_get_order(&self, order_uid: u64) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::get_order(order_uid))
    }

    /// `emk_GetOpenOrders`.
    ///
    /// The current Delphi reference server has no request-handler branch for this
    /// method and returns `Unknown method`.
    pub fn api_get_open_orders(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::get_open_orders())
    }

    /// `emk_GetActiveOrders`.
    ///
    /// The current Delphi reference server has no request-handler branch for this
    /// method and returns `Unknown method`.
    pub fn api_get_active_orders(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::get_active_orders())
    }

    /// `emk_CancelAllOrders`.
    pub fn api_cancel_all_orders(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::cancel_all_orders())
    }

    /// `emk_SetLeverage(market, new_leverage)`.
    pub fn api_set_leverage(&self, market: &str, new_lev: i32) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::set_leverage(
            market, new_lev,
        ))
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
        self.send_api_request_async(&crate::commands::engine_request::subscribe_all_trades(
            want_mm_orders,
        ))
    }

    /// `emk_UnsubscribeAllTrades`.
    pub fn api_unsubscribe_all_trades(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::unsubscribe_all_trades())
    }

    /// `emk_SubscribeOrderBook` — `markets` empty = подписка на все.
    ///
    /// **Low-level вариант** (не обновляет subscription registry, не resolve'ит market_name).
    /// Для нормальной работы используй [`Client::subscribe_orderbook`].
    pub fn api_subscribe_order_book(&self, markets: &[&str]) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::subscribe_order_book(
            markets,
        ))
    }

    /// `emk_UnsubscribeOrderBook` — `markets` empty = отписка от всех.
    ///
    /// **Low-level вариант** (не обновляет registry). См. [`Client::unsubscribe_orderbook`].
    pub fn api_unsubscribe_order_book(&self, markets: &[&str]) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::unsubscribe_order_book(
            markets,
        ))
    }

    /// `emk_RequestOrderBookFull(market_idx, book_kind)` — запрос полного snapshot.
    pub fn api_request_order_book_full(
        &self,
        market_idx: u16,
        book_kind: u8,
    ) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::request_order_book_full(
            market_idx, book_kind,
        ))
    }

    /// `emk_ReloadOrderBook`.
    pub fn api_reload_order_book(&self) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::reload_order_book())
    }

    /// `emk_ChangePositionType(market, type, new_market)`.
    pub fn api_change_position_type(
        &self,
        market: &str,
        pos_type: u8,
        new_market: bool,
    ) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::change_position_type(
            market, pos_type, new_market,
        ))
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
    pub fn api_do_transfer_asset(
        &self,
        asset: &str,
        qty: f64,
        from: u8,
        to: u8,
    ) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::do_transfer_asset(
            asset, qty, from, to,
        ))
    }

    /// `emk_UpdateTransferAssets(kind)`.
    pub fn api_update_transfer_assets(&self, kind: u8) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::engine_request::update_transfer_assets(
            kind,
        ))
    }

    /// `emk_TradesResend(packet_nums)` — multi-batch (auto-split по 200).
    /// Возвращает массив receivers (по одному на batch).
    pub fn api_trades_resend_batches(
        &self,
        packet_nums: &[u16],
    ) -> Vec<mpsc::Receiver<EngineResponse>> {
        crate::commands::engine_request::trades_resend_batches(packet_nums)
            .iter()
            .map(|raw| self.send_api_request_async(raw))
            .collect()
    }

    /// `emk_GetCoinCardCandles(market, ticks)` — запрос свечей для CoinCard (не chunked).
    /// Response — `count:i32 + N × TDeepPrice(28 bytes)`. Парсить через
    /// `commands::candles::parse_coin_card_candles_response(&resp.data)`.
    pub fn api_get_coin_card_candles(
        &self,
        market: &str,
        ticks: crate::commands::candles::DeepHistoryKind,
    ) -> mpsc::Receiver<EngineResponse> {
        self.send_api_request_async(&crate::commands::candles::get_coin_card_candles(
            market, ticks,
        ))
    }

    /// `emk_RequestCandlesData` — низкоуровневый fire-and-forget. Сервер пришлёт
    /// несколько chunked `EngineResponse`-пакетов с одинаковым `request_uid`.
    /// **Для нормальной работы используй [`Client::api_request_candles_data_async`]**
    /// — он автоматически агрегирует chunks через [`CandlesAggregator`] и возвращает
    /// `Receiver<MergedCandles>` для blocking-ожидания финального результата.
    pub fn api_request_candles_data(&self) {
        self.send_api_request(&crate::commands::engine_request::request_candles_data());
    }

    fn api_request_candles_data_async_registered(
        &mut self,
    ) -> (u64, mpsc::Receiver<MergedCandles>) {
        let raw = crate::commands::engine_request::request_candles_data();
        // UID извлекается из BaseCommand header offset 3..11 (тот же что в send_api_request_async).
        let uid = raw
            .get(3..11)
            .and_then(|s| s.try_into().ok())
            .map(u64::from_le_bytes)
            .unwrap_or(0);
        let (tx, rx) = mpsc::channel();
        let partial = PartialCandles {
            aggregator: CandlesAggregator::new(),
            sender: tx,
            last_activity_ms: self.now_ms(),
        };
        // Замещение существующего slot'а допустимо — старый sender дропнется, его
        // receiver получит Err(Disconnected) (что корректно при двойном вызове).
        self.pending_candles.insert(uid, partial);
        self.send_api_request(&raw);
        (uid, rx)
    }

    /// **Async-вариант `emk_RequestCandlesData`** — отправляет запрос и регистрирует
    /// chunked aggregator. Возвращает `Receiver<MergedCandles>` — потребитель ждёт
    /// его пока main loop продолжает крутиться и получает уже собранный zlib stream
    /// от Delphi `TMarkets.StoreCandlesToZip` плюс parsed market entries.
    ///
    /// Сервер шлёт несколько `EngineResponse` пакетов с одинаковым `request_uid`,
    /// каждый — chunk `ChunkIndex:u16 + ChunkTotal:u16 + payload`. Liба сама агрегирует
    /// через `CandlesAggregator`, парсит через `parse_request_candles_data_response`,
    /// уведомляет sender → потребитель получает `MergedCandles`.
    ///
    /// Auto-cleanup: pending slot удаляется автоматически если финальный chunk не пришёл
    /// в течение `DEFAULT_PENDING_CANDLES_TIMEOUT_MS` от последнего принятого chunk —
    /// sender дропается, receiver получает `Err(Disconnected)`.
    pub fn api_request_candles_data_async(&mut self) -> mpsc::Receiver<MergedCandles> {
        self.api_request_candles_data_async_registered().1
    }

    /// Request the full chunked candles stream and wait for the merged result
    /// while the client loop keeps running.
    ///
    /// This is the one-shot counterpart to
    /// [`Self::api_request_candles_data_async`]. It registers the chunked
    /// aggregator, sends `emk_RequestCandlesData`, pumps
    /// [`Self::run_with_dispatcher`] in short ticks, and removes the pending
    /// candles slot if the caller's timeout expires before the final chunk.
    pub fn request_candles_data(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        timeout: Duration,
    ) -> Result<MergedCandles, mpsc::RecvTimeoutError> {
        let (uid, rx) = self.api_request_candles_data_async_registered();
        match self.run_until_response(dispatcher, &rx, timeout) {
            Ok(merged) => Ok(merged),
            Err(err) => {
                self.pending_candles.remove(&uid);
                Err(err)
            }
        }
    }

    // ====================================================================
    //  Active library: subscription API (по market_name + registry)
    //
    //  F4: thread-safe API через [`ClientSender`]. Эти методы — **главный
    //  публичный API** для подписок. В отличие от `api_subscribe_order_book`
    //  (low-level) они:
    //   1. Запоминают подписку в `subscription_registry`.
    //   2. После единственного Init восстанавливаются самой либой при reconnect.
    //   3. Принимают `market_name` (стабилен через reindex), не market_idx.
    //   4. Работают на `&self` — доступны во время `run_with_dispatcher`
    //      через `client.sender()` clone из любого thread'а.
    //
    //  Аналог Delphi `MoonProtoEngine.pas:305-360 CheckBookTopics` с
    //  `BookSubbed: TSet<TMarket>` и `NeedResubscribeOrderBooks`.
    // ====================================================================

    /// Thread-safe sender handle for subscribing and sending commands from any
    /// thread.
    ///
    /// The returned `ClientSender` is cloneable and can live in a UI thread,
    /// worker thread, or any other owner. `Client::run_with_dispatcher` drains
    /// those intents from the client main loop.
    ///
    /// ```ignore
    /// let mut client = Client::new(cfg);
    /// let sender = client.sender();
    /// thread::spawn(move || {
    ///     sender.subscribe_orderbook("DOGEUSDT");
    /// });
    /// client.run_with_dispatcher(...);
    /// ```
    pub fn sender(&self) -> ClientSender {
        ClientSender {
            app_events: Arc::clone(&self.app_events),
            app_queue_alive: Arc::clone(&self.app_queue_alive),
            send_queues: Arc::clone(&self.send_queues),
            server_update_sent: Arc::clone(&self.server_update_sent),
        }
    }

    /// Hidden FireTest hook: when enabled, no outgoing datagrams are sent.
    ///
    /// Normal applications must not use this. The live FireTest uses it to make
    /// the MoonBot server stop hearing from this client, then verifies that the
    /// library reconnects and restores subscriptions after the flag is cleared.
    #[doc(hidden)]
    pub fn debug_set_outgoing_blackhole(&mut self, enabled: bool) {
        self.debug_outgoing_blackhole = enabled;
    }

    /// Subscribe to the orderbook stream for one market name.
    ///
    /// This is a fire-and-forget convenience wrapper around
    /// `self.sender().subscribe_orderbook(...)`. It enqueues into the unbounded
    /// app-to-client FIFO; a warning is logged only if the client loop is gone.
    /// Use `client.sender().try_subscribe_orderbook(...)` when the caller needs
    /// explicit failure feedback.
    ///
    /// The subscription is stored in the registry. Before init, reconnect does
    /// not send it. After init, reconnect restores it automatically without a
    /// second init. The server resolves `market_name -> market_idx`, so callers
    /// may subscribe before `emk_GetMarketsList` has completed. The call is
    /// idempotent; futures and spot books are distinguished by incoming
    /// `book_kind`, not by the subscribe request.
    pub fn subscribe_orderbook(&self, market_name: &str) {
        self.sender().subscribe_orderbook(market_name);
    }

    /// Subscribe to several orderbook streams in one registry-aware batch.
    ///
    /// Already remembered market names are ignored. Newly added names are sent
    /// through one `emk_SubscribeOrderBook` request, matching the server's
    /// batch-oriented `MarketNames` field.
    pub fn subscribe_orderbooks<I, S>(&self, market_names: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.sender().subscribe_orderbooks(market_names);
    }

    /// Unsubscribe from one market's orderbook stream.
    ///
    /// See [`Client::subscribe_orderbook`] for registry and reconnect behavior.
    pub fn unsubscribe_orderbook(&self, market_name: &str) {
        self.sender().unsubscribe_orderbook(market_name);
    }

    /// Unsubscribe from several orderbook streams in one registry-aware batch.
    pub fn unsubscribe_orderbooks<I, S>(&self, market_names: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.sender().unsubscribe_orderbooks(market_names);
    }

    /// Unsubscribe from all remembered orderbook streams.
    ///
    /// This clears the reconnect registry and sends the server's
    /// `emk_UnsubscribeOrderBook` request with an empty market list. Prefer this
    /// high-level method over raw `api_unsubscribe_order_book(&[])`; the raw call
    /// does not update the registry and reconnect would restore stale
    /// subscriptions.
    pub fn unsubscribe_all_orderbooks(&self) {
        self.sender().unsubscribe_all_orderbooks();
    }

    /// Subscribe to the all-trades stream.
    ///
    /// `want_mm` requests market-maker order sections. The subscription is
    /// stored in the registry and restored automatically after reconnect once
    /// init has completed. Calling it again with a different `want_mm` updates
    /// the remembered intent and sends a fresh subscribe request.
    pub fn subscribe_all_trades(&self, want_mm: bool) {
        self.sender().subscribe_all_trades(want_mm);
    }

    /// Unsubscribe from the all-trades stream and remove the registry intent.
    pub fn unsubscribe_all_trades(&self) {
        self.sender().unsubscribe_all_trades();
    }

    /// F6/F7: проверка пора ли слать periodic refresh-команды.
    /// Вызывается из main loop каждый тик (~5мс), но реальная отправка происходит
    /// только когда прошёл `update_markets_every` / `check_tags_every` от последнего раза.
    ///
    /// Fire-and-forget: используем `send_api_request` без регистрации в pending registry —
    /// EventDispatcher автоматически применяет ответ к MarketsState когда он придёт.
    /// На случай если ответ не дойдёт (UDP loss / server reset) — следующий тик
    /// просто пошлёт заново, никакого retry/timeout-кода не нужно.
    fn tick_periodic_refresh(&mut self, cur_tm: i64) {
        let hour_slot = if self.cfg.refresh.check_tags_every.is_some() {
            current_utc_hour_slot()
        } else {
            self.check_tags_hour_slot
        };
        self.tick_periodic_refresh_at(cur_tm, hour_slot);
    }

    fn tick_periodic_refresh_at(&mut self, cur_tm: i64, hour_slot: i64) {
        if self.domain_ready
            && self.domain_restore_needs_indexes()
            && self.peer_app_token != 0
            && !self.market_indexes_current_for_peer()
            && !self.indexes_fetch_in_flight
        {
            self.send_markets_indexes_restore_request(cur_tm);
        }

        if let Some(interval) = self.cfg.refresh.update_markets_every {
            let interval_ms = interval.as_millis() as i64;
            if (cur_tm - self.last_update_markets_ms) >= interval_ms {
                self.send_api_request(&crate::commands::engine_request::update_markets_list());
                self.last_update_markets_ms = cur_tm;
            }
        }
        if let Some(interval) = self.cfg.refresh.check_tags_every {
            if self.check_tags_hour_slot == i64::MIN {
                self.check_tags_hour_slot = hour_slot;
            } else if hour_slot != self.check_tags_hour_slot {
                self.check_tags_hour_slot = hour_slot;
                self.check_tags_burst_sent = 0;
                self.last_check_tags_burst_ms = i64::MIN / 2;
            }

            let interval_ms = interval.as_millis() as i64;
            let burst_due = self.check_tags_burst_sent < CHECK_TAGS_BURST_COUNT
                && (cur_tm - self.last_check_tags_burst_ms) >= CHECK_TAGS_BURST_SPACING_MS;
            let interval_due = (cur_tm - self.last_check_tags_ms) >= interval_ms;

            if burst_due || interval_due {
                self.send_api_request(&crate::commands::engine_request::check_binance_tags());
                self.last_check_tags_ms = cur_tm;
                if self.check_tags_burst_sent < CHECK_TAGS_BURST_COUNT {
                    self.check_tags_burst_sent += 1;
                    self.last_check_tags_burst_ms = cur_tm;
                }
            }
        }
    }

    /// Внутренний метод: применить одну subscribe-команду (registry update + wire send).
    /// Вызывается main loop при получении `ClientEvent::Subscribe*`/`Unsubscribe*`.
    fn apply_subscribe_event(&mut self, ev: ClientEvent) {
        match ev {
            ClientEvent::SubscribeOrderBook { market_name } => {
                // Wire подписка идёт по `market_name` (resolve делает сервер) — поэтому
                // подписку можно вызвать ДО получения `emk_GetMarketsList`.
                let newly_added = self
                    .subscription_registry
                    .orderbook_subs
                    .insert(market_name.clone());
                if newly_added {
                    self.send_api_request(&crate::commands::engine_request::subscribe_order_book(
                        &[&market_name],
                    ));
                }
            }
            ClientEvent::SubscribeOrderBooks { market_names } => {
                let mut new_names = Vec::new();
                for market_name in market_names {
                    if self
                        .subscription_registry
                        .orderbook_subs
                        .insert(market_name.clone())
                    {
                        new_names.push(market_name);
                    }
                }
                if !new_names.is_empty() {
                    let refs: Vec<&str> = new_names.iter().map(String::as_str).collect();
                    self.send_api_request(&crate::commands::engine_request::subscribe_order_book(
                        &refs,
                    ));
                }
            }
            ClientEvent::UnsubscribeOrderBook { market_name } => {
                if self
                    .subscription_registry
                    .orderbook_subs
                    .remove(&market_name)
                {
                    self.send_api_request(
                        &crate::commands::engine_request::unsubscribe_order_book(&[&market_name]),
                    );
                }
            }
            ClientEvent::UnsubscribeOrderBooks { market_names } => {
                let mut removed_names = Vec::new();
                for market_name in market_names {
                    if self
                        .subscription_registry
                        .orderbook_subs
                        .remove(&market_name)
                    {
                        removed_names.push(market_name);
                    }
                }
                if !removed_names.is_empty() {
                    let refs: Vec<&str> = removed_names.iter().map(String::as_str).collect();
                    self.send_api_request(
                        &crate::commands::engine_request::unsubscribe_order_book(&refs),
                    );
                }
            }
            ClientEvent::UnsubscribeAllOrderBooks => {
                self.subscription_registry.orderbook_subs.clear();
                self.send_api_request(
                    &crate::commands::engine_request::unsubscribe_order_book(&[]),
                );
            }
            ClientEvent::SubscribeAllTrades { want_mm } => {
                self.subscription_registry.trades_sub = Some(TradesSubscription { want_mm });
                self.subscription_registry.mm_orders_sub = Some(want_mm);
                self.send_api_request(&crate::commands::engine_request::subscribe_all_trades(
                    want_mm,
                ));
            }
            ClientEvent::UnsubscribeAllTrades => {
                if self.subscription_registry.trades_sub.take().is_some() {
                    self.send_api_request(
                        &crate::commands::engine_request::unsubscribe_all_trades(),
                    );
                }
            }
            // Не-subscribe события не обрабатываются этим методом
            ClientEvent::Wake | ClientEvent::MmOrdersSubscribe { .. } => {
                debug_assert!(
                    false,
                    "apply_subscribe_event called with non-subscribe event"
                );
            }
            #[cfg(test)]
            ClientEvent::Recv(_) => {
                debug_assert!(
                    false,
                    "apply_subscribe_event called with non-subscribe event"
                );
            }
            #[cfg(test)]
            ClientEvent::Send(_) => {
                debug_assert!(
                    false,
                    "apply_subscribe_event called with non-subscribe event"
                );
            }
        }
    }

    #[cfg(test)]
    fn outgoing_mm_orders_subscribe_intent(item: &SendItem) -> Option<bool> {
        if item.cmd != Command::UI as u8 || item.u_key.kind != UK_TURN_MM_DETECTION {
            return None;
        }
        if item.data.first().copied() != Some(5) {
            return None;
        }
        item.data.last().map(|v| *v != 0)
    }

    fn apply_mm_orders_subscribe_intent(&mut self, subscribe: bool) {
        self.subscription_registry.mm_orders_sub = Some(subscribe);
        if let Some(trades_sub) = self.subscription_registry.trades_sub.as_mut() {
            trades_sub.want_mm = subscribe;
        }
    }

    fn send_mm_orders_subscribe_cmd(&self, subscribe: bool) {
        let uid = rand::random();
        let raw = crate::commands::ui::build_mm_orders_subscribe(uid, subscribe);
        self.send_cmd_keyed(
            raw,
            Command::UI,
            SendPriority::High,
            true,
            3,
            UniqueKey::turn_mm_detection_for(uid),
        );
    }

    fn domain_restore_needs_indexes(&self) -> bool {
        self.domain_restore.fetch_indexes
            || self.subscription_registry.trades_sub.is_some()
            || !self.subscription_registry.orderbook_subs.is_empty()
    }

    fn send_markets_indexes_restore_request(&mut self, now_ms: i64) {
        self.update_markets_after_indexes = true;
        if self.indexes_fetch_in_flight {
            return;
        }
        self.indexes_fetch_in_flight = true;
        self.indexes_fetch_started_ms = now_ms;
        self.send_api_request(&crate::commands::engine_request::get_markets_indexes());
    }

    /// Restore domain intent after reconnect inside an already initialized Client session.
    ///
    /// This is deliberately gated by `domain_ready`: before the single init pass `Fine`
    /// remains transport-only and must not emit Engine API traffic.
    fn restore_domain_after_reconnect(&mut self) {
        if !self.domain_ready {
            return;
        }

        if self.domain_restore_needs_indexes() {
            self.send_markets_indexes_restore_request(self.now_ms());
        }

        self.restore_registry_subscriptions();
    }

    /// Batch restore helper for the subscription registry.
    ///
    /// OrderBook подписки отправляются одним `emk_SubscribeOrderBook` batch'ем:
    /// в Delphi wire request нет `OrderBookKind`, только список имён рынков.
    fn restore_registry_subscriptions(&mut self) {
        if let Some(sub) = self.subscription_registry.trades_sub {
            let want_mm = self
                .subscription_registry
                .mm_orders_sub
                .unwrap_or(sub.want_mm);
            self.send_api_request(&crate::commands::engine_request::subscribe_all_trades(
                want_mm,
            ));
        } else if let Some(subscribe) = self.subscription_registry.mm_orders_sub {
            self.send_mm_orders_subscribe_cmd(subscribe);
        }
        let refs: Vec<&str> = self
            .subscription_registry
            .orderbook_subs
            .iter()
            .map(String::as_str)
            .collect();
        if !refs.is_empty() {
            self.send_api_request(&crate::commands::engine_request::subscribe_order_book(
                &refs,
            ));
        }
    }

    // ====================================================================
    //  Init helper УБРАН: дизайн `run_init_sequence` конфликтовал с
    //  `&mut Client` который держит `run()` — метод не мог быть вызван из
    //  обычного flow. Init шаги выполняются напрямую: вызови `subscribe_*` /
    //  `api_*` ДО `client.run_with_dispatcher` (методы требуют `&mut self` —
    //  это безопасно пока main loop не запущен), либо после `Connected{fresh}`
    //  через тот же `&mut Client` если используется single-thread runner.
    // ====================================================================

    // ====================================================================
    //  High-level Trade wrappers (convenience over commands::trade::build_*)
    //  Все шлются как Command::Order (28), Priority=High, encrypted, MaxRetries=3.
    //  Кроме DoClose/DoLimitClose/DoSplit/DoSellOrder/DoMarketSplit — MaxRetries=1.
    // ====================================================================

    fn send_trade(&self, payload: Vec<u8>, max_retries: i32) {
        self.send_cmd(
            payload,
            Command::Order,
            SendPriority::High,
            true,
            max_retries,
        );
    }

    /// `send_trade` с UniqueKey — для команд имеющих `[MoonCmdUnique(UK_*)]` атрибут.
    /// Старые pending команды с тем же UKey удаляются из `self.sending`/`self.pending_h`
    /// (matches Delphi SendCmdInt:780-785 + CheckSendingData).
    fn send_trade_keyed(&self, payload: Vec<u8>, max_retries: i32, u_key: UniqueKey) {
        self.send_cmd_keyed(
            payload,
            Command::Order,
            SendPriority::High,
            true,
            max_retries,
            u_key,
        );
    }

    /// Send `TNewOrderCommand` (CmdId=3) to open a new order.
    pub fn new_order(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        is_short: bool,
        price: f64,
        strat_id: u64,
        order_size: f64,
    ) {
        let raw = crate::commands::trade::build_new_order(
            ctx, market, is_short, price, strat_id, order_size,
        );
        self.send_trade(raw, 3);
    }

    /// Send `TOrderReplaceCommand` (CmdId=6, `UK_OrderMove`) with a new price.
    ///
    /// `ctx.uid` must be the server order task id so UKey dedup collapses
    /// repeated moves for the same order.
    ///
    /// `Epoch=0` и `Status=OS_None` устанавливаются внутри: Delphi
    /// `TOrderReplaceCommand.Create` не принимает статус для client-side replace.
    pub fn replace_order(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        order_type: crate::commands::trade::OrderType,
        new_price: f64,
    ) {
        let raw = crate::commands::trade::build_order_replace(ctx, market, order_type, new_price);
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
    }

    /// Replace an order already tracked by `EventDispatcher::orders()`.
    ///
    /// This is the regular consumer path for UI actions on an existing order:
    /// UID, market name, currency, and platform are derived from the tracked
    /// order state.
    pub fn replace_tracked_order(
        &self,
        order: &crate::state::Order,
        order_type: crate::commands::trade::OrderType,
        new_price: f64,
    ) {
        self.replace_order(order.trade_ctx(), &order.market_name, order_type, new_price);
    }

    /// Send low-level `TAllStatusesReq` (CmdId=9).
    ///
    /// Regular applications should prefer [`Self::request_order_snapshot`].
    pub fn request_all_statuses(&self, uid: u64) {
        let raw = crate::commands::trade::build_all_statuses_request(uid);
        self.send_trade(raw, 3);
    }

    /// Request the current order snapshot and wait until it is applied to
    /// `EventDispatcher::orders()`.
    ///
    /// This is the high-level consumer helper for `TAllStatusesReq`. It hides the
    /// protocol UID, pumps the UDP loop while waiting, and also waits for the
    /// active dispatcher to finish Delphi `CleanupMissingWorkers` follow-up
    /// requests for orders absent from the snapshot.
    pub fn request_order_snapshot(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        timeout: Duration,
    ) -> Result<Vec<crate::state::Order>, mpsc::RecvTimeoutError> {
        const TICK: Duration = Duration::from_millis(50);

        let previous_snapshot_flag = dispatcher.orders().current_snapshot_flag();
        let start = Instant::now();
        self.request_all_statuses(rand::random());

        loop {
            let snapshot_seen =
                dispatcher.orders().current_snapshot_flag() != previous_snapshot_flag;
            if snapshot_seen && dispatcher.orders().missing_after_snapshot().is_empty() {
                return Ok(dispatcher.orders().iter().cloned().collect());
            }

            let Some(remaining) = timeout_remaining(start, timeout) else {
                return Err(mpsc::RecvTimeoutError::Timeout);
            };

            let tick = remaining.min(TICK);
            self.run_with_dispatcher_queued(tick, dispatcher);
        }
    }

    /// Send `TOrderCancelCommand` (CmdId=10, `UK_OrderMove`) for one order.
    ///
    /// `ctx.uid` must be the server order task id for correct dedup. The wrapper
    /// writes `Epoch=0`, matching the Delphi client-originated command path.
    pub fn cancel_order(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        status: crate::commands::trade::OrderWorkerStatus,
    ) {
        let raw = crate::commands::trade::build_order_cancel(ctx, market, 0, status);
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
    }

    /// Cancel an order already tracked by `EventDispatcher::orders()`.
    ///
    /// The Delphi client creates `TOrderCancelCommand` from the live order
    /// worker, so consumers should not have to remember the protocol context or
    /// current status separately.
    pub fn cancel_tracked_order(&self, order: &crate::state::Order) {
        self.cancel_order(order.trade_ctx(), &order.market_name, order.status);
    }

    /// Send `TJoinOrdersCommand` (CmdId=11) to join open orders.
    pub fn join_orders(&self, ctx: crate::commands::trade::TradeCtx, market: &str, is_short: bool) {
        let raw = crate::commands::trade::build_join_orders(ctx, market, is_short);
        self.send_trade(raw, 3);
    }

    /// Send `TSplitOrderCommand` (CmdId=12) to split an order into parts.
    pub fn split_order(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        split_parts: i32,
        split_small: bool,
        split_small_sell: bool,
    ) {
        let raw = crate::commands::trade::build_split_order(
            ctx,
            market,
            split_parts,
            split_small,
            split_small_sell,
        );
        self.send_trade(raw, 3);
    }

    /// Split an order already tracked by `EventDispatcher::orders()`.
    pub fn split_tracked_order(
        &self,
        order: &crate::state::Order,
        split_parts: i32,
        split_small: bool,
        split_small_sell: bool,
    ) {
        self.split_order(
            order.trade_ctx(),
            &order.market_name,
            split_parts,
            split_small,
            split_small_sell,
        );
    }

    /// `TMoveAllSellsCommand` (CmdId=13).
    ///
    /// The move mode, price, zone and side live in [`crate::commands::trade::MoveAllSellsParams`]
    /// to keep the public API resistant to swapped positional arguments.
    pub fn move_all_sells(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        params: crate::commands::trade::MoveAllSellsParams,
    ) {
        let raw = crate::commands::trade::build_move_all_sells(ctx, market, params);
        self.send_trade(raw, 3);
    }

    /// `TDoClosePositionCommand` (CmdId=14, MaxRetries=1).
    pub fn do_close_position(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        market_sell: bool,
    ) {
        let raw = crate::commands::trade::build_do_close_position(ctx, market, market_sell);
        self.send_trade(raw, 1);
    }

    /// `TDoLimitClosePositionCommand` (CmdId=15, MaxRetries=1).
    pub fn do_limit_close_position(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        is_short: bool,
    ) {
        let raw = crate::commands::trade::build_do_limit_close_position(ctx, market, is_short);
        self.send_trade(raw, 1);
    }

    /// `TDoSplitPositionCommand` (CmdId=16, MaxRetries=1).
    pub fn do_split_position(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        is_short: bool,
    ) {
        let raw = crate::commands::trade::build_do_split_position(ctx, market, is_short);
        self.send_trade(raw, 1);
    }

    /// `TDoSellOrderCommand` (CmdId=17, MaxRetries=1).
    pub fn do_sell_order(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        price: f64,
        size: f64,
    ) {
        let raw = crate::commands::trade::build_do_sell_order(ctx, market, price, size);
        self.send_trade(raw, 1);
    }

    /// `TOrderStatusRequest` (CmdId=18) — запросить статус конкретного ордера.
    pub fn request_order_status(&self, ctx: crate::commands::trade::TradeCtx, market: &str) {
        let raw = crate::commands::trade::build_order_status_request(ctx, market);
        self.send_trade(raw, 3);
    }

    /// Request a fresh status for an order already tracked by
    /// `EventDispatcher::orders()`.
    pub fn request_tracked_order_status(&self, order: &crate::state::Order) {
        self.request_order_status(order.trade_ctx(), &order.market_name);
    }

    /// `TOrderStopsUpdate` (CmdId=20, UK_OrderMove). `ctx.uid` = task_id ордера.
    /// `Epoch=0` (внутри). См. `replace_order`.
    pub fn update_order_stops(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        status: crate::commands::trade::OrderWorkerStatus,
        stops: &crate::commands::trade::StopSettings,
    ) {
        let raw = crate::commands::trade::build_order_stops_update(ctx, market, 0, status, stops);
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
    }

    /// Update stops for an order already tracked by `EventDispatcher::orders()`.
    pub fn update_tracked_order_stops(
        &self,
        order: &crate::state::Order,
        stops: &crate::commands::trade::StopSettings,
    ) {
        self.update_order_stops(order.trade_ctx(), &order.market_name, order.status, stops);
    }

    /// `TTurnPanicSellCommand` (CmdId=21, UK_OrderMove). `ctx.uid` = task_id ордера.
    /// `Epoch=0` и `Status=OS_None` устанавливаются внутри, как в Delphi client path.
    pub fn turn_panic_sell(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        turn_on: bool,
    ) {
        let raw = crate::commands::trade::build_turn_panic_sell(ctx, market, turn_on);
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
    }

    /// Toggle panic sell for an order already tracked by
    /// `EventDispatcher::orders()`.
    pub fn turn_tracked_order_panic_sell(&self, order: &crate::state::Order, turn_on: bool) {
        self.turn_panic_sell(order.trade_ctx(), &order.market_name, turn_on);
    }

    /// Send `TSetImmuneCommand` (CmdId=22, `UK_ImmuneClicks`) for a batch of
    /// order immunity flags.
    ///
    /// The dedup UID is `sum(items[].uid)`, matching Delphi
    /// `TSetImmuneCommand.SetUKey`.
    pub fn set_immune(&self, uid: u64, items: &[crate::commands::trade::ImmuneItem]) {
        let raw = crate::commands::trade::build_set_immune(uid, items);
        let items_uid_sum: u64 = items.iter().fold(0u64, |acc, it| acc.wrapping_add(it.uid));
        self.send_trade_keyed(raw, 3, UniqueKey::immune_clicks(items_uid_sum));
    }

    /// `TMoveAllBuysCommand` (CmdId=27).
    pub fn move_all_buys(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        cmd_type: crate::commands::trade::MoveAllCmdType,
        move_kind: crate::commands::trade::ReplaceMultiKind,
        price: f64,
        side: crate::commands::trade::FixedPosition,
    ) {
        let raw = crate::commands::trade::build_move_all_buys(
            ctx,
            market,
            cmd_type as u8,
            move_kind,
            price,
            side,
        );
        self.send_trade(raw, 3);
    }

    /// Send `TVStopUpdate` (CmdId=29, `UK_OrderMove`).
    ///
    /// This wrapper writes `Epoch=0`, matching Delphi client-originated order
    /// moves. `ctx.uid` must be the order task id. Status and VStop values live
    /// in [`crate::commands::trade::VStopUpdateParams`].
    pub fn update_vstop(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        params: crate::commands::trade::VStopUpdateParams,
    ) {
        let raw = crate::commands::trade::build_vstop_update(ctx, market, 0, params);
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
    }

    /// Update VStop for an order already tracked by `EventDispatcher::orders()`.
    pub fn update_tracked_order_vstop(
        &self,
        order: &crate::state::Order,
        vstop_on: bool,
        vstop_fixed: bool,
        vstop_level: f64,
        vstop_vol: f64,
    ) {
        self.update_vstop(
            order.trade_ctx(),
            &order.market_name,
            crate::commands::trade::VStopUpdateParams {
                status: order.status,
                vstop_on,
                vstop_fixed,
                vstop_level,
                vstop_vol,
            },
        );
    }

    /// `TDoMarketSplitPositionCommand` (CmdId=30, MaxRetries=1).
    pub fn do_market_split_position(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        is_short: bool,
    ) {
        let raw = crate::commands::trade::build_do_market_split_position(ctx, market, is_short);
        self.send_trade(raw, 1);
    }

    /// Send `TPenaltyCommand` (CmdId=23) to mark a market as under strategy
    /// penalty/cooldown.
    ///
    /// Manual and alert strategies are intentionally not blocked by this server
    /// flag; it affects automatic strategy checks.
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

    /// Send `TClientSettingsCommand` (UI CmdId=1, Sliced,
    /// `UK_BaseUISettings`).
    ///
    /// This sends a full client-settings snapshot and replaces any older
    /// pending settings packet with the same UKey slot.
    pub fn ui_send_settings(&self, settings: &crate::commands::ui::ClientSettingsCommand) {
        let mut wire_settings = settings.clone();
        wire_settings.uid = rand::random();
        let raw = crate::commands::ui::build_client_settings(&wire_settings);
        self.send_cmd_keyed(
            raw,
            Command::UI,
            SendPriority::Sliced,
            true,
            6,
            UniqueKey::base_ui_settings_slot(),
        );
    }

    /// Send `TSettingsRequest` (UI CmdId=2, High) to request current settings.
    pub fn ui_settings_request(&self) {
        let raw = crate::commands::ui::build_settings_request(rand::random());
        self.send_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// Request the current UI settings snapshot and wait for the next
    /// `TClientSettingsCommand` while pumping the UDP loop.
    ///
    /// This is the UI-channel counterpart to [`Self::run_until_response`] for
    /// Engine API calls. `TSettingsRequest` does not carry a request/response
    /// UID pair on the wire: Delphi answers by sending a fresh
    /// `TClientSettingsCommand`. The helper therefore waits until
    /// `EventDispatcher` observes a settings snapshot with a new command UID.
    pub fn request_client_settings(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        timeout: Duration,
    ) -> Result<crate::commands::ui::ClientSettingsCommand, mpsc::RecvTimeoutError> {
        const TICK: Duration = Duration::from_millis(50);

        let previous_uid = dispatcher
            .settings()
            .client_settings
            .as_ref()
            .map(|settings| settings.uid);
        let start = Instant::now();
        self.ui_settings_request();

        loop {
            if let Some(settings) = dispatcher.settings().client_settings.as_ref() {
                if previous_uid != Some(settings.uid) {
                    return Ok(settings.clone());
                }
            }

            let Some(remaining) = timeout_remaining(start, timeout) else {
                return Err(mpsc::RecvTimeoutError::Timeout);
            };

            let tick = remaining.min(TICK);
            self.run_with_dispatcher_queued(tick, dispatcher);
        }
    }

    /// Send `TStratStartStopCommand` (UI CmdId=3, High) to start or stop all
    /// strategies.
    pub fn ui_strat_start_stop(&self, is_start: bool) {
        let raw = crate::commands::ui::build_strat_start_stop(rand::random(), is_start);
        self.send_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// Send `TStratStartStopCommandV2` (UI CmdId=4, High) with checked strategy
    /// items.
    pub fn ui_strat_start_stop_v2(
        &self,
        is_start: bool,
        items: &[crate::commands::strat::StratCheckedItem],
    ) {
        let raw = crate::commands::ui::build_strat_start_stop_v2(rand::random(), is_start, items);
        self.send_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// Send `TMMOrdersSubscribeCommand` (UI CmdId=5, High,
    /// `UK_TurnMMDetection`) to set the market-maker orders subscription flag.
    pub fn ui_mm_subscribe(&self, subscribe: bool) {
        self.sender().ui_mm_subscribe(subscribe);
    }

    /// `TUpdateVersionCommand` (UI CmdId=6, High) — request a MoonBot version update.
    ///
    /// Delphi uses this from the update UI:
    /// - release button sends `VersionName=""`, `IsRelease=true`;
    /// - beta/test install command sends the requested version name and release flag.
    ///
    /// The server handles the command and broadcasts the same UI command back to
    /// clients. Delphi clients then run their local updater in
    /// `HandleRemoteUpdateCommand`; this Rust wrapper only sends the protocol
    /// command and marks Delphi `ServerUpdateSent` so the next init uses the
    /// update-aware BaseCheck retry path.
    pub fn ui_update_version(&self, version_name: &str, is_release: bool) {
        let raw =
            crate::commands::ui::build_update_version(rand::random(), version_name, is_release);
        self.send_cmd(raw, Command::UI, SendPriority::High, true, 3);
        self.mark_server_update_sent();
    }

    /// Send `TEmuTradesCommand` (UI CmdId=7, Sliced) with emulated trades for a
    /// test market.
    pub fn ui_emu_trades(
        &self,
        m_index: u16,
        base_time: f64,
        points: &[crate::commands::ui::EmuTradePoint],
    ) {
        let raw = crate::commands::ui::build_emu_trades(rand::random(), m_index, base_time, points);
        self.send_cmd(raw, Command::UI, SendPriority::Sliced, true, 6);
    }

    /// Send `TNewMarketNotifyCommand` (UI CmdId=8, High) to notify about a new
    /// market.
    pub fn ui_new_market_notify(&self) {
        let raw = crate::commands::ui::build_new_market_notify(rand::random());
        self.send_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// Send `TLevManageCommand` (UI CmdId=9, Sliced,
    /// `UK_LevManageSettings`) with leverage-management settings.
    pub fn ui_lev_manage(&self, cmd: &crate::commands::ui::LevManage) {
        let uid: u64 = rand::random();
        let raw = crate::commands::ui::build_lev_manage(uid, cmd);
        self.send_cmd_keyed(
            raw,
            Command::UI,
            SendPriority::Sliced,
            true,
            6,
            UniqueKey::lev_manage_settings_slot(),
        );
    }

    /// Send `TTriggerManageCommand` (UI CmdId=10, Sliced) for batch trigger
    /// management over all markets or selected market/key pairs.
    pub fn ui_trigger_manage(&self, action: u8, all_markets: bool, markets: &[u16], keys: &[u16]) {
        let raw = crate::commands::ui::build_trigger_manage(
            rand::random(),
            action,
            all_markets,
            markets,
            keys,
        );
        self.send_cmd(raw, Command::UI, SendPriority::Sliced, true, 6);
    }

    /// Send `TResetProfitCommand` (UI CmdId=11, High) to reset profit counters.
    pub fn ui_reset_profit(&self, kind: u8) {
        let raw = crate::commands::ui::build_reset_profit(rand::random(), kind);
        self.send_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// Send `TArbActivateNotify` (UI CmdId=12, High) with an arbitration-valid
    /// timestamp.
    pub fn ui_arb_activate_notify(&self, arb_valid: f64) {
        let raw = crate::commands::ui::build_arb_activate_notify(rand::random(), arb_valid);
        self.send_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// Send `TSwitchDexCommand` (UI CmdId=13, High, `UK_DexSwitch`).
    ///
    /// The DEX name is truncated to the Delphi 15-byte short-string payload.
    pub fn ui_switch_dex(&self, dex_name: &str) {
        let uid = rand::random();
        let raw = crate::commands::ui::build_switch_dex(uid, dex_name);
        self.send_cmd_keyed(
            raw,
            Command::UI,
            SendPriority::High,
            true,
            3,
            UniqueKey::dex_switch_for(uid),
        );
        self.mark_server_update_sent();
    }

    /// Send `TSwitchSpotCommand` (UI CmdId=14, High, `UK_SpotSwitch`) to select
    /// the spot mode.
    pub fn ui_switch_spot(&self, spot_index: u8) {
        let uid = rand::random();
        let raw = crate::commands::ui::build_switch_spot(uid, spot_index);
        self.send_cmd_keyed(
            raw,
            Command::UI,
            SendPriority::High,
            true,
            3,
            UniqueKey::spot_switch_for(uid),
        );
        self.mark_server_update_sent();
    }

    // ====================================================================
    //  High-level Strat wrappers (Command::Strat, encrypted=true)
    //  Покрывают MClient.SendStratCmd(T*Command.Create(...)) семантику Delphi.
    //  Аудит docs_api B-02: было 5 build_* функций без Client-обёрток.
    // ====================================================================

    /// Send `TStratSnapshotRequest` (Strat CmdId=1, High) to request a strategy
    /// snapshot from the server.
    pub fn strat_snapshot_request(&self) {
        let raw = crate::commands::strat::build_snapshot_request(rand::random());
        self.send_cmd(raw, Command::Strat, SendPriority::High, true, 3);
    }

    fn send_strat_snapshot_command(&self, raw: Vec<u8>) {
        self.send_cmd_keyed(
            raw,
            Command::Strat,
            SendPriority::Sliced,
            true,
            6,
            UniqueKey::strat_snapshot(),
        );
    }

    /// `TStratSnapshot` (Strat CmdId=2, Sliced, UK_StratSnapshot) from an already
    /// serialized `TStrategySerializer` payload.
    ///
    /// `data` is only the `TStratSnapshot.Data` blob. The method adds the required
    /// Delphi fields: `ServerEpoch`, `ClientMaxLastDate`, `Size`, and `Full`.
    /// Use [`Client::strat_send_snapshot_batch`] when the application has decoded
    /// `StrategySnapshot` values rather than a prebuilt serializer payload.
    pub fn strat_send_snapshot_payload(
        &self,
        server_epoch: u64,
        client_max_last_date: u64,
        full: bool,
        data: &[u8],
    ) {
        let uid: u64 = rand::random();
        let raw = crate::commands::strat::build_snapshot(
            uid,
            server_epoch,
            client_max_last_date,
            full,
            data,
        );
        self.send_strat_snapshot_command(raw);
    }

    /// `TStratSnapshot` (Strat CmdId=2, Sliced, UK_StratSnapshot) from typed
    /// strategy snapshots.
    ///
    /// This is the high-level counterpart to Delphi `CreateFromStrats` /
    /// `CreateFromList`: it serializes the batch, computes `ClientMaxLastDate`,
    /// and sends a valid CmdId=2 packet.
    pub fn strat_send_snapshot_batch(
        &self,
        server_epoch: u64,
        full: bool,
        strategies: &[crate::commands::strategy_serializer::StrategySnapshot],
    ) {
        let uid: u64 = rand::random();
        let raw = crate::commands::strat::build_snapshot_from_strategies(
            uid,
            server_epoch,
            full,
            strategies,
        );
        self.send_strat_snapshot_command(raw);
    }

    /// Send `TStratDelete` (Strat CmdId=3, High) for one strategy or folder.
    pub fn strat_delete(&self, strategy_id: u64, folder_path: &str) {
        let raw = crate::commands::strat::build_delete(rand::random(), strategy_id, folder_path);
        self.send_cmd(raw, Command::Strat, SendPriority::High, true, 3);
    }

    /// Send `TStratSellPriceUpdate` (Strat CmdId=4, High,
    /// `UK_StratSellPriceUpdate`) for one strategy.
    ///
    /// The UKey includes `strategy_id`, so dedup is per strategy.
    pub fn strat_sell_price_update(&self, strategy_id: u64, sell_price: f64) {
        let raw = crate::commands::strat::build_sell_price_update(
            rand::random(),
            strategy_id,
            sell_price,
        );
        self.send_cmd_keyed(
            raw,
            Command::Strat,
            SendPriority::High,
            true,
            3,
            UniqueKey::strat_sell_price_update(strategy_id),
        );
    }

    /// Send `TStratCheckedSync` (Strat CmdId=5, Sliced) for strategy checked
    /// state.
    ///
    /// `is_delta = false` sends a full list; `true` sends a delta.
    pub fn strat_checked_sync(
        &self,
        items: &[crate::commands::strat::StratCheckedItem],
        is_delta: bool,
    ) {
        let raw = crate::commands::strat::build_checked_sync(rand::random(), items, is_delta);
        self.send_cmd(raw, Command::Strat, SendPriority::Sliced, true, 6);
    }

    /// Send `TStratCheckedEcho` (Strat CmdId=6, High).
    pub fn strat_checked_echo(&self, items: &[crate::commands::strat::StratCheckedItem]) {
        let raw = crate::commands::strat::build_checked_echo(rand::random(), items);
        self.send_cmd(raw, Command::Strat, SendPriority::High, true, 3);
    }

    // ====================================================================
    //  High-level Balance wrappers (Command::Balance, encrypted=true)
    //  Покрывают MClient.SendBalanceCmd семантику Delphi.
    //  Аудит docs_api B-03: ранее не было ни build_, ни Client-wrapper'а.
    // ====================================================================

    /// Send `TRequestBalanceRefresh` (Balance CmdId=5, High).
    ///
    /// The server responds by broadcasting a fresh balance snapshot through the
    /// normal balance channel.
    pub fn balance_request_refresh(&self) {
        let raw = crate::commands::balance::build_request_balance_refresh(rand::random());
        self.send_cmd(raw, Command::Balance, SendPriority::High, true, 3);
    }

    /// Request a fresh full balance snapshot and wait until it is applied to
    /// `EventDispatcher::balances()`.
    ///
    /// `TRequestBalanceRefresh` is not an Engine API request and has no response
    /// UID. Delphi handles it by forcing the next balance worker tick to
    /// broadcast `TBalanceSnapshotFull`. This helper hides that fire-and-forget
    /// shape: it sends the request, keeps the UDP loop running, waits for a new
    /// full balance snapshot epoch, then returns a cloned read model.
    pub fn request_balance_snapshot(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        timeout: Duration,
    ) -> Result<crate::state::BalancesState, mpsc::RecvTimeoutError> {
        const TICK: Duration = Duration::from_millis(50);

        let previous_epoch = dispatcher.balances().last_epoch;
        let start = Instant::now();
        self.balance_request_refresh();

        loop {
            let Some(remaining) = timeout_remaining(start, timeout) else {
                return Err(mpsc::RecvTimeoutError::Timeout);
            };

            let first_new_event = dispatcher.queued_event_count();
            let tick = remaining.min(TICK);
            self.run_with_dispatcher_queued(tick, dispatcher);
            if dispatcher.queued_events()[first_new_event..]
                .iter()
                .any(|event| {
                    matches!(
                        event,
                        crate::events::Event::Balance(
                            crate::state::BalanceEvent::SnapshotApplied { epoch, .. }
                        ) if *epoch != previous_epoch
                    )
                })
            {
                return Ok(dispatcher.balances().clone());
            }
        }
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
        if let Some(addr) = self.cached_server_addr {
            return Some(addr);
        }
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
    pub fn run(&mut self, duration: Duration, on_data: OnDataFn) {
        // Тонкий wrapper над унифицированным `run_inner`. Backwards-compat API —
        // существует только для потребителей которым НЕ нужны active-library
        // auto-actions (RequestOrderBookFull, periodic trades.tick, и т.п.).
        // **Для большинства случаев предпочтительнее `run_with_dispatcher`** —
        // см. его doc-comment.
        let mode = RunMode::Callback { on_data };
        self.run_inner(duration, mode);
    }

    /// Send LogOff and close socket. Call when done.
    /// Matches TMoonProtoBaseClient.Disconnect (Common.pas:290-298)
    pub fn disconnect(&mut self) {
        self.need_connect = false;
        self.force_disconnect = true;
        self.authorized = false;
        self.auth_status = AuthStatus::Base;
    }

    /// Active-library entry point: run the client with an integrated
    /// `EventDispatcher`.
    ///
    /// Unlike [`Self::run`], this method routes incoming payloads through
    /// `dispatcher.dispatch_into_active` and performs active-library work:
    ///   - orderbook corrupted-cache recovery sends `RequestOrderBookFull`
    ///     without surfacing a separate callback event;
    ///   - trades gap recovery ticks and sends `TradesResend` batches;
    ///   - `TradesState::tick` runs about every 100 ms from the main loop;
    ///   - market-index gating and per-client server-time delta are applied by
    ///     the dispatcher.
    ///
    /// The callback is informational: the dispatcher has already parsed the
    /// event and updated the read model.
    ///
    /// Basic pattern:
    /// ```ignore
    /// let mut client = Client::new(cfg);
    /// let mut dispatcher = EventDispatcher::new();
    /// client.run_with_dispatcher(
    ///     Duration::from_secs(3600),
    ///     &mut dispatcher,
    ///     Box::new(|ev| match ev {
    ///         Event::Order(o) => /* update UI */,
    ///         Event::EngineResponse(r) if !r.success => /* show error */,
    ///         _ => {}
    ///     })
    /// );
    /// ```
    pub fn run_with_dispatcher(
        &mut self,
        duration: Duration,
        dispatcher: &mut crate::events::EventDispatcher,
        on_event: EventFn,
    ) {
        // Тонкий wrapper над унифицированным `run_inner`. Все active-library
        // auto-actions (RequestOrderBookFull, periodic trades.tick, indexes
        // sync gate, ServerTimeDelta apply, server-token state reset) живут
        // в `dispatch_into_active_actions` + `run_inner`.
        let mode = RunMode::Dispatcher {
            dispatcher,
            on_event: DispatcherEventFn::EventOnly(on_event),
            event_buf: Vec::with_capacity(8),
            payload_buf: Vec::with_capacity(4),
            active_actions_buf: Vec::with_capacity(4),
        };
        self.run_inner(duration, mode);
    }

    /// Same as [`Self::run_with_dispatcher`], but the callback also receives the
    /// updated read-only `EventDispatcher`.
    ///
    /// This is useful for UI events that carry only an id, such as
    /// `OrderEvent::Updated(uid)`: the callback can immediately read the
    /// current order from the dispatcher state.
    pub fn run_with_dispatcher_state(
        &mut self,
        duration: Duration,
        dispatcher: &mut crate::events::EventDispatcher,
        on_event: EventWithStateFn,
    ) {
        let mode = RunMode::Dispatcher {
            dispatcher,
            on_event: DispatcherEventFn::EventWithState(on_event),
            event_buf: Vec::with_capacity(8),
            payload_buf: Vec::with_capacity(4),
            active_actions_buf: Vec::with_capacity(4),
        };
        self.run_inner(duration, mode);
    }

    fn run_with_dispatcher_queued(
        &mut self,
        duration: Duration,
        dispatcher: &mut crate::events::EventDispatcher,
    ) {
        let mode = RunMode::Dispatcher {
            dispatcher,
            on_event: DispatcherEventFn::Queue,
            event_buf: Vec::with_capacity(8),
            payload_buf: Vec::with_capacity(4),
            active_actions_buf: Vec::with_capacity(4),
        };
        self.run_inner(duration, mode);
    }

    /// Wait for a `Receiver<T>` while continuing to pump the UDP client loop.
    ///
    /// `Client::api_*` methods return `mpsc::Receiver<T>`, but the response is
    /// delivered only while the client loop is running. Calling
    /// `rx.recv_timeout(...)` directly on the same thread that owns the `Client`
    /// usually times out because UDP packets are not processed during that
    /// blocking wait.
    ///
    /// This helper runs short dispatcher ticks (10 ms, matching Delphi
    /// `SendAndWait` sleep) until a value arrives, the channel disconnects, or
    /// the overall timeout expires. Events produced while the helper waits are
    /// stored in
    /// [`EventDispatcher::queued_events`](crate::events::EventDispatcher::queued_events)
    /// and can be drained through
    /// [`EventDispatcher::take_queued_events`](crate::events::EventDispatcher::take_queued_events).
    /// It works with any receiver: Engine API responses, the candle aggregator,
    /// or custom registry slots.
    ///
    /// **Pattern**:
    /// ```ignore
    /// let rx = client.api_get_markets_list();
    /// let resp = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(12))?;
    /// ```
    pub fn run_until_response<T>(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        rx: &mpsc::Receiver<T>,
        timeout: Duration,
    ) -> Result<T, mpsc::RecvTimeoutError> {
        let start = Instant::now();
        loop {
            match rx.try_recv() {
                Ok(resp) => return Ok(resp),
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err(mpsc::RecvTimeoutError::Disconnected);
                }
                Err(mpsc::TryRecvError::Empty) => {}
            }
            let Some(remaining) = timeout_remaining(start, timeout) else {
                return Err(mpsc::RecvTimeoutError::Timeout);
            };
            let tick = remaining.min(Duration::from_millis(DELPHI_SEND_AND_WAIT_POLL_MS));
            self.run_with_dispatcher_queued(tick, dispatcher);
        }
    }

    /// Унифицированный main loop. Закрывает дубликацию `run`/`run_with_dispatcher`
    /// которая существовала с момента введения active library (rust_quality #1 +
    /// delphi_dev #2 audits). Любой fix в loop body (новый cleanup, новый periodic
    /// check, новое поведение recv/send) делается ОДИН раз.
    ///
    /// Различия двух режимов локализованы в:
    ///   - `process_reader_decoded(...)` — куда доставлять decoded payload
    ///     (Callback sink для `run`; Buffer sink +
    ///     dispatcher.dispatch_into_active для `run_with_dispatcher`).
    ///   - В конце iter: для Dispatcher mode дополнительно — periodic
    ///     `trades.tick()` каждые 100мс. Для Callback mode tick не нужен (callback
    ///     потребитель сам решает что делать с TradesEvent).
    fn run_inner(&mut self, duration: Duration, mut mode: RunMode<'_>) {
        let run_start = Instant::now();

        loop {
            if run_start.elapsed() >= duration {
                break;
            }
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
                self.bind_socket(cur_tm);
                self.spawn_reader();
            }

            if self.socket.is_some() {
                let mut sliced = Vec::new();
                let mut h_items = Vec::new();
                let mut l_items = Vec::new();
                // F4: subscribe/unsubscribe/control events применяются ПОСЛЕ closure (нужен
                // &mut self для registry mutation + send_api_request — borrow checker
                // не пропустит внутри closure которая уже держит &mut на четыре Vec).
                let mut control_events: Vec<QueuedControlEvent> = Vec::new();

                let mut processed_events = 0usize;
                let mut waited_for_reader = false;

                self.drain_reader_decoded(cur_tm, &mut mode);

                // Delphi separates app SendCmdInt queues from UDPRead. Raw/API
                // sends already append directly to send queues; the remaining
                // budget covers control intents and coalesced reader Wake events,
                // not accepted UDP packets.
                while processed_events < EVENT_DRAIN_BUDGET {
                    self.drain_reader_decoded(cur_tm, &mut mode);
                    let event = if let Some(ev) = self.pop_app_event() {
                        Some(ev)
                    } else if !waited_for_reader && processed_events == 0 {
                        waited_for_reader = true;
                        match self
                            .event_rx
                            .recv_timeout(Duration::from_millis(DEFAULT_SLEEP_MS))
                        {
                            Ok(ev) => Some(ev),
                            Err(mpsc::RecvTimeoutError::Timeout) => None,
                            Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        }
                    } else {
                        match self.event_rx.try_recv() {
                            Ok(ev) => Some(ev),
                            Err(mpsc::TryRecvError::Empty) => break,
                            Err(mpsc::TryRecvError::Disconnected) => return,
                        }
                    };

                    if let Some(ev) = event {
                        processed_events += 1;
                        self.handle_main_event(
                            ev,
                            cur_tm,
                            &mut mode,
                            &mut sliced,
                            &mut h_items,
                            &mut l_items,
                            &mut control_events,
                        );
                    } else {
                        break;
                    }
                }
                self.drain_reader_decoded(cur_tm, &mut mode);

                // F4: применяем subscribe/unsubscribe/control события до GetCopySendList:
                // control handlers call send_cmd/send_api_request, and Delphi would place
                // those commands into DataToSend* before the writer copies queues if the
                // SendLock is acquired in this order.
                for ev in control_events {
                    self.apply_control_event(ev);
                }

                // Delphi `GetCopySendList`: copy DataToSend/DataToSendH/DataToSendL
                // under SendLock and clear the source lists. Raw `send_cmd` paths
                // append directly to these queues instead of going through app FIFO.
                self.get_copy_send_list(&mut sliced, &mut h_items, &mut l_items);

                // Legacy ClientEvent::Send events can still accumulate local items
                // during the rewrite. Keep Delphi UKey "latest wins" semantics for
                // the copied batch too, including duplicates across old/new paths.
                Self::dedup_send_items_by_u_key(&mut sliced);
                Self::dedup_send_items_by_u_key(&mut h_items);

                self.apply_sliced_send_u_key_cleanup(&sliced);

                let copy_acks = self.get_copy_acks();
                self.copy_recvd_data();

                // CheckSeningData: Sliced → CreateSlicedObject; H → batched; PendingH retry; L → batch
                for item in &sliced {
                    self.create_sliced_and_send(item);
                }
                self.apply_copy_acks(copy_acks, cur_tm);
                self.apply_regular_hl_ack();
                self.apply_high_send_u_key_cleanup(&h_items);
                for mut item in h_items {
                    self.send_h_item(&mut item, cur_tm);
                }
                self.retry_pending_h(cur_tm);
                self.send_low_items_around_sliced_retry(&l_items, cur_tm);

                // Cleanup periodic (pending_candles). `Receiving` cleanup belongs
                // to the reader-side UDPRead path (`FClient.DoCleanUp` before
                // command handling in Delphi), so it is not driven by writer ticks.
                if (cur_tm - self.last_cleanup).abs() > CLEANUP_INTERVAL_MS {
                    let candles_before = self.pending_candles.len();
                    self.pending_candles.retain(|_uid, partial| {
                        (cur_tm - partial.last_activity_ms) < DEFAULT_PENDING_CANDLES_TIMEOUT_MS
                    });
                    let candles_removed = candles_before - self.pending_candles.len();
                    if candles_removed > 0 {
                        log::debug!(target: "moonproto::client",
                            "pending_candles: cleaned up {} stale aggregators (>{}ms old)",
                            candles_removed, DEFAULT_PENDING_CANDLES_TIMEOUT_MS);
                    }
                    self.last_cleanup = cur_tm;
                }

                // Timeout protection для init/API markets-index request marker.
                self.check_indexes_fetch_timeout(cur_tm);

                // F6/F7: periodic refresh prices + tags (опционально через ClientConfig.refresh).
                // Шлём только если auth_status == AuthDone (сервер примет запрос только в этой
                // фазе; до неё запрос потеряется впустую).
                if matches!(self.auth_status, AuthStatus::AuthDone) && self.domain_ready {
                    self.tick_periodic_refresh(cur_tm);
                }

                // audit_robustness H5: после clock-jump (NTP step / mobile suspend-resume)
                // handshake timestamp устарел и сервер reject'нёт hello. Force reconnect
                // чтобы full_reset + новый Hello с актуальным временем.
                self.check_clock_jump();

                // Active library: periodic trades.tick — только в Dispatcher mode.
                // В Callback mode TradesEvent попадает к потребителю напрямую,
                // он сам управляет gap recovery (если нужно — через свой EventDispatcher).
                self.periodic_trades_tick(cur_tm, &mut mode);

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

    fn handle_main_event(
        &mut self,
        ev: ClientEvent,
        _cur_tm: i64,
        _mode: &mut RunMode<'_>,
        _sliced: &mut Vec<SendItem>,
        _h_items: &mut Vec<SendItem>,
        _l_items: &mut Vec<SendItem>,
        control_events: &mut Vec<QueuedControlEvent>,
    ) {
        match ev {
            ClientEvent::Wake => {}
            #[cfg(test)]
            ClientEvent::Recv(msg) => self.process_recv_event(msg, _cur_tm, _mode),
            #[cfg(test)]
            ClientEvent::Send(s) => {
                if let Some(subscribe) = Self::outgoing_mm_orders_subscribe_intent(&s.item) {
                    control_events.push(QueuedControlEvent::MmOrdersSubscribe(subscribe));
                }
                match s.item.priority {
                    SendPriority::Sliced => _sliced.push(s.item),
                    SendPriority::High => _h_items.push(s.item),
                    SendPriority::Low => _l_items.push(s.item),
                }
            }
            ClientEvent::MmOrdersSubscribe { subscribe } => {
                control_events.push(QueuedControlEvent::MmOrdersSubscribe(subscribe));
            }
            ev @ ClientEvent::SubscribeOrderBook { .. }
            | ev @ ClientEvent::SubscribeOrderBooks { .. }
            | ev @ ClientEvent::UnsubscribeOrderBook { .. }
            | ev @ ClientEvent::UnsubscribeOrderBooks { .. }
            | ev @ ClientEvent::UnsubscribeAllOrderBooks
            | ev @ ClientEvent::SubscribeAllTrades { .. }
            | ev @ ClientEvent::UnsubscribeAllTrades => {
                control_events.push(QueuedControlEvent::Subscribe(ev));
            }
        }
    }

    fn get_copy_send_list(
        &self,
        sliced: &mut Vec<SendItem>,
        h_items: &mut Vec<SendItem>,
        l_items: &mut Vec<SendItem>,
    ) {
        self.send_queues
            .lock()
            .unwrap()
            .take_into(sliced, h_items, l_items);
    }

    fn apply_sliced_send_u_key_cleanup(&mut self, sliced: &[SendItem]) {
        // Delphi `CheckSeningData` keeps the cleanup scopes separate:
        // CopySendList (Sliced) calls `DeleteSendingByKey` before
        // `CreateSlicedObject`. Delphi removes only the first matching entry.
        for item in sliced {
            if !item.u_key.is_none() {
                if let Some(pos) = self.sending.iter().position(|s| s.u_key == item.u_key) {
                    self.sending.remove(pos);
                }
            }
        }
    }

    fn apply_high_send_u_key_cleanup(&mut self, h_items: &[SendItem]) {
        // Delphi calls `DeletePendingByKey` for copied High items after
        // `ApplyACK` and `ApplyRegularHLAck`, immediately before sending High.
        // It removes only the first matching PendingH entry.
        for item in h_items {
            if !item.u_key.is_none() {
                if let Some(pos) = self.pending_h.iter().position(|p| p.u_key == item.u_key) {
                    self.pending_h.remove(pos);
                }
            }
        }
    }

    fn apply_control_event(&mut self, ev: QueuedControlEvent) {
        match ev {
            QueuedControlEvent::Subscribe(ev) => self.apply_subscribe_event(ev),
            QueuedControlEvent::MmOrdersSubscribe(subscribe) => {
                self.apply_mm_orders_subscribe_intent(subscribe);
                self.send_mm_orders_subscribe_cmd(subscribe);
            }
        }
    }

    #[cfg(test)]
    fn process_recv_event(&mut self, msg: RecvMsg, cur_tm: i64, mode: &mut RunMode<'_>) {
        // Фильтр stale epoch — пакеты от старого reader thread'а после reconnect
        // (epoch-tag дает эквивалент Delphi `UDPClient.Active := false`).
        let current_epoch = self.current_reader_epoch;
        if msg.epoch != current_epoch {
            if self.should_log("stale_reader_epoch", 5000) {
                warn!(target: "moonproto::client",
                    "dropping stale packet from old reader epoch (msg.epoch={} current={})",
                    msg.epoch, current_epoch);
            }
            return;
        }

        self.apply_recv_side_effects(msg.recv_bytes, msg.timestamp_ms);
        if msg.err_emu_drop {
            return;
        }
        if msg.handled_in_reader {
            return;
        }
        self.process_recv_msg(msg, cur_tm, mode);
    }

    fn apply_recv_side_effects(&mut self, recv_bytes: u64, timestamp_ms: i64) {
        self.connected = true;
        if self.auth_status == AuthStatus::Base {
            self.auth_status = AuthStatus::Connected;
        }
        self.total_recv += recv_bytes;
        self.track_recv(recv_bytes, timestamp_ms);
        self.last_online = timestamp_ms;
    }

    fn drain_reader_decoded(&mut self, cur_tm: i64, mode: &mut RunMode<'_>) {
        self.reader_wake_pending.store(false, Ordering::Release);
        let decoded = {
            let mut pending = self.pending_reader_decoded.lock().unwrap();
            if pending.is_empty() {
                return;
            }
            std::mem::take(&mut *pending)
        };

        for msg in decoded {
            self.process_reader_decoded(msg, cur_tm, mode);
        }
    }

    /// Обработать один входящий UDP-пакет: decrypt/decompress/Grouped-split через
    /// handle_udp_command, доставить наружу через mode-specific sink.
    #[cfg(test)]
    fn process_recv_msg(&mut self, msg: RecvMsg, cur_tm: i64, mode: &mut RunMode<'_>) {
        let cmd = Command::from_byte(msg.cmd);
        let raw_cmd = msg.cmd;
        let reader_dataread_core_done = msg.reader_dataread_core_done;
        match mode {
            RunMode::Callback { on_data } => {
                let mut sink = DispatchSink::Callback(on_data);
                self.handle_udp_command(
                    cmd,
                    raw_cmd,
                    &msg.payload,
                    &mut sink,
                    reader_dataread_core_done,
                );
            }
            RunMode::Dispatcher {
                dispatcher,
                on_event,
                event_buf,
                payload_buf,
                ..
            } => {
                payload_buf.clear();
                let authorized_before = self.authorized;
                {
                    let mut sink = DispatchSink::Buffer(payload_buf);
                    self.handle_udp_command(
                        cmd,
                        raw_cmd,
                        &msg.payload,
                        &mut sink,
                        reader_dataread_core_done,
                    );
                }
                // During startup the server may send large app-state snapshots before
                // `Fine`. Do not let active state parsing delay the handshake path:
                // handle transport/handshake commands first, then dispatch app payloads
                // only once the client is authorized.
                if !authorized_before && !self.authorized {
                    payload_buf.clear();
                    return;
                }
                for (c, p) in payload_buf.drain(..) {
                    event_buf.clear();
                    dispatcher.dispatch_into_active(c, &p, cur_tm, event_buf, self);
                    on_event.drain_events(event_buf, dispatcher);
                }
            }
        }
    }

    fn process_reader_decoded(
        &mut self,
        msg: ReaderDecodedMsg,
        cur_tm: i64,
        mode: &mut RunMode<'_>,
    ) {
        if msg.epoch != self.current_reader_epoch {
            if self.should_log("stale_reader_decoded_epoch", 5000) {
                warn!(target: "moonproto::client",
                    "dropping decoded reader data from old reader epoch (msg.epoch={} current={})",
                    msg.epoch, self.current_reader_epoch);
            }
            return;
        }

        if msg.apply_recv_effects {
            self.apply_recv_side_effects(msg.recv_bytes, msg.timestamp_ms);
        }
        if let Some(stats) = msg.sliced_stats {
            let dup_pct = stats.dup_count as f64 / stats.blocks_count.max(1) as f64 * 100.0;
            if self.avg_dup_count == 0.0 {
                self.avg_dup_count = dup_pct;
            } else {
                self.avg_dup_count = (self.avg_dup_count * 9.0 + dup_pct) * 0.1;
            }
        }
        if let Some(update) = msg.ping_update {
            self.apply_reader_ping_update(update);
        }
        if let Some(update) = msg.handshake_update {
            self.apply_reader_handshake_update(update, msg.timestamp_ms);
        }
        if let Some(payload) = msg.payload {
            self.process_decoded_data_read_int(msg.cmd, payload, cur_tm, mode);
        }
    }

    fn apply_reader_ping_update(&mut self, update: ReaderPingUpdate) {
        self.ping_count = update.ping_count;
        self.round_trip_delay = update.round_trip_delay;
        self.actual_pmtu = update.actual_pmtu;
        self.global_timing_orders = update.global_timing_orders;
        self.overheat = update.overheat;
        self.rs = update.rs;
        self.need_connect = false;
        self.server_time_delta = update.server_time_delta;
        self.server_time_delta_handle.store(
            update.server_time_delta.to_bits(),
            std::sync::atomic::Ordering::Relaxed,
        );
        set_server_time_delta_global(update.server_time_delta);
        self.net_lag_ping = update.net_lag_ping;
        self.can_send_rate = update.can_send_rate;
        self.used_sliced_limit = update.used_sliced_limit;
    }

    fn apply_reader_handshake_update(&mut self, update: ReaderHandshakeUpdate, timestamp_ms: i64) {
        match update.cmd {
            Command::WrongHello => {
                self.waiting_hello = false;
                self.auth_status = AuthStatus::Connected;
            }
            Command::WantNewHello => {
                self.waiting_hello = false;
                self.full_reset();
                self.last_sent_hello = NEVER_SENT_MS;
                self.auth_status = AuthStatus::Connected;
                self.authorized = false;
                self.need_connect = true;
                self.soft_reconnect = false;
            }
            Command::NeedHelloAgain => {
                if (timestamp_ms - self.last_need_hello_again).abs() > NEED_HELLO_AGAIN_THROTTLE_MS
                {
                    self.last_need_hello_again = timestamp_ms;
                    if !self.waiting_hello {
                        self.waiting_hello_start = timestamp_ms;
                    }
                    self.waiting_hello = true;
                    self.last_sent_hello = NEVER_SENT_MS;
                }
            }
            Command::WhoAreYou => {
                self.waiting_hello = false;
                self.server_token = update.server_token;
                let prev_app_token = self.peer_app_token;
                self.peer_app_token = update.peer_app_token;
                if prev_app_token != 0 && prev_app_token != update.peer_app_token {
                    self.indexes_fetch_in_flight = false;
                    self.tracked_indexes_peer_app_token = 0;
                    self.fire_lifecycle(LifecycleEvent::ServerRestart);
                }
                self.encode_key = update.encode_key;
                self.decode_key = update.decode_key;
                self.encode_cipher = Some(crate::crypto::cipher_from_key(&self.encode_key));
                self.reader_protocol
                    .lock()
                    .unwrap()
                    .set_decode_cipher(crate::crypto::cipher_from_key(&self.decode_key));
                self.client_token = update.client_token;
            }
            Command::Fine => {
                let restore_after_reconnect = self.domain_ready && self.was_ever_connected;
                self.need_connect = false;
                self.waiting_hello = false;
                self.auth_status = AuthStatus::AuthDone;
                self.authorized = true;
                if restore_after_reconnect {
                    self.restore_domain_after_reconnect();
                }
            }
            _ => {}
        }
    }

    pub(crate) fn apply_active_actions<I>(&self, actions: I)
    where
        I: IntoIterator<Item = crate::events::ActiveAction>,
    {
        for action in actions {
            match action {
                crate::events::ActiveAction::RequestOrderBookFull {
                    market_index,
                    book_kind,
                } => {
                    self.send_api_request(
                        &crate::commands::engine_request::request_order_book_full(
                            market_index,
                            book_kind,
                        ),
                    );
                }
                crate::events::ActiveAction::SendStrategySnapshot {
                    server_epoch,
                    client_max_last_date,
                    full,
                    data,
                } => {
                    self.strat_send_snapshot_payload(
                        server_epoch,
                        client_max_last_date,
                        full,
                        &data,
                    );
                }
                crate::events::ActiveAction::RequestOrderStatus { ctx, market_name } => {
                    self.request_order_status(ctx, &market_name);
                }
            }
        }
    }

    fn process_decoded_data_read_int(
        &mut self,
        cmd: u8,
        payload: Vec<u8>,
        cur_tm: i64,
        mode: &mut RunMode<'_>,
    ) {
        match mode {
            RunMode::Callback { on_data } => {
                let mut sink = DispatchSink::Callback(on_data);
                self.deliver_data_read_int_decoded(cmd, payload, &mut sink);
            }
            RunMode::Dispatcher {
                dispatcher,
                on_event,
                event_buf,
                payload_buf,
                active_actions_buf,
            } => {
                payload_buf.clear();
                let authorized_before = self.authorized;
                {
                    let mut sink = DispatchSink::Buffer(payload_buf);
                    self.deliver_data_read_int_decoded(cmd, payload, &mut sink);
                }
                if !authorized_before && !self.authorized {
                    payload_buf.clear();
                    return;
                }
                for (c, p) in payload_buf.drain(..) {
                    event_buf.clear();
                    active_actions_buf.clear();
                    let ctx = crate::events::ActiveDispatchContext::from_client(self);
                    dispatcher.dispatch_into_active_actions(
                        c,
                        &p,
                        cur_tm,
                        event_buf,
                        &ctx,
                        active_actions_buf,
                    );
                    self.apply_active_actions(active_actions_buf.drain(..));
                    on_event.drain_events(event_buf, dispatcher);
                }
            }
        }
    }

    /// Periodic trades.tick (только в Dispatcher mode). Throttle 100мс — соответствует
    /// Delphi `MoonProtoEngine.pas:1483 CheckMissingTradesPackets`. Сам tick также
    /// имеет internal throttle 100мс, наш guard здесь только чтобы не дёргать его
    /// на каждом packet (он всё равно вернёт пустой Vec).
    fn periodic_trades_tick(&mut self, cur_tm: i64, mode: &mut RunMode<'_>) {
        if let RunMode::Dispatcher {
            dispatcher,
            on_event,
            event_buf,
            ..
        } = mode
        {
            if cur_tm - self.last_trades_tick_ms >= 100 {
                self.last_trades_tick_ms = cur_tm;
                let rtt = self.round_trip_delay;
                let (payloads, tick_events) = dispatcher.trades.tick_with_events(rtt, cur_tm);
                for p in payloads {
                    self.send_api_request(&p);
                }
                event_buf.clear();
                event_buf.extend(tick_events.into_iter().map(crate::events::Event::Trade));
                on_event.drain_events(event_buf, dispatcher);
            }
        }
    }

    /// Spawn reader thread (≡ Indy TIdUDPListenerThread).
    /// Accepted packets run their protocol-owned receive core in this reader
    /// stack; main is woken only to apply queued user/active-library delivery
    /// and writer-visible side effects that are still being split out.
    ///
    /// **Shutdown:** создаём НОВЫЙ `Arc<AtomicBool>` для этого reader. Сохраняем clone в
    /// `self.reader_shutdown`. При `do_force_disconnect` / `Drop` ставим в `true` —
    /// reader thread выйдет из loop (макс через `read_timeout=1s`).
    /// Новый spawn_reader создаёт **свой** Arc — старый и новый не конфликтуют.
    fn spawn_reader(&mut self) {
        let Some(ref sock) = self.socket else {
            return;
        };
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
        let master_key = self.cfg.master_key;
        let mac_ctx = self.mac_ctx.clone(); // shared cached HMAC context (hot path).
        let mask_ver = self.cfg.mask_ver;
        let client_id = self.cfg.client_id;
        let app_token = self.app_token;
        let reader_client_token_start = self.client_token;
        let server_addr = self.server_socket_addr();
        let event_tx = self.event_tx.clone();
        let incoming_sliced_acks = Arc::clone(&self.incoming_sliced_acks);
        let pending_reader_decoded = Arc::clone(&self.pending_reader_decoded);
        let reader_wake_pending = Arc::clone(&self.reader_wake_pending);
        let reader_protocol = Arc::clone(&self.reader_protocol);
        let reader_ping_state = Arc::clone(&self.reader_ping_state);
        let server_time_delta_handle = Arc::clone(&self.server_time_delta_handle);
        let slicer = Arc::clone(&self.slicer);
        let total_sent = Arc::clone(&self.total_sent);
        let total_recv_shared = Arc::clone(&self.total_recv_shared);
        let debug_outgoing_blackhole = self.debug_outgoing_blackhole;
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
                let mut reader_client_token = reader_client_token_start;
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
                if shutdown_flag.load(Ordering::Relaxed) {
                    break;
                }

                // Transport unpack (OLC + MAC + ver check) — кэшированный MacContext.
                let Some((hdr, payload)) = moonproto_transport::transport_unpack_with_mac(
                    &mac_ctx, &mac_key, &buf[..n], mask_ver,
                ) else { continue; };
                if trace_io_enabled() {
                    eprintln!(
                        "[mp-io-rx] cmd={:?} raw={} packet_len={} payload_len={}",
                        Command::from_byte(hdr.cmd),
                        hdr.cmd,
                        n,
                        payload.len()
                    );
                }

                // ErrEmu: симуляция packet loss на стороне клиента (зеркало Delphi
                // MoonProtoUDPClient.pas:534-541). Дроп ПОСЛЕ checksum+ver checks,
                // т.е. валидный пакет просто отбрасывается. Служебные команды дропаются
                // с rate/2 (чтобы handshake/ping не отваливались полностью).
                let total_recv_after =
                    total_recv_shared.fetch_add(n as u64, Ordering::Relaxed) + n as u64;
                let err_emu_drop = err_emu_should_drop(hdr.cmd);
                if err_emu_drop {
                    if trace_io_enabled() {
                        eprintln!(
                            "[mp-io-drop-err-emu] cmd={:?} raw={} payload_len={}",
                            Command::from_byte(hdr.cmd),
                            hdr.cmd,
                            payload.len()
                        );
                    }
                    if slicing::trace_enabled() && Command::from_byte(hdr.cmd) == Command::Sliced {
                        if let Some(sh) = slicing::SliceHeader::from_bytes(&payload) {
                            eprintln!(
                                "[slice-rx-drop-err-emu] d={} b={}/{} len={}",
                                sh.datagram_num,
                                sh.block_num,
                                sh.max_block_num,
                                payload.len()
                            );
                        } else {
                            eprintln!("[slice-rx-drop-err-emu] malformed len={}", payload.len());
                        }
                    }
                }

                // B-V3-02 fix: монотонный timestamp через Instant вместо SystemTime
                // (~20x faster, не подвержен NTP-корректировкам). Reader thread
                // получил `start_time` clone'ом из self._start (Instant — Copy).
                // Тот же time base что в `Client::now_ms` — diff'ы остаются корректны.
                let timestamp_ms = start_time.elapsed().as_millis() as i64;
                let cmd = Command::from_byte(hdr.cmd);
                if err_emu_drop {
                    Client::push_reader_recv_side_effect(
                        &pending_reader_decoded,
                        hdr.cmd,
                        n as u64,
                        timestamp_ms,
                        my_epoch,
                    );
                } else {
                    {
                        let mut slicer = slicer.lock().unwrap();
                        slicer.set_last_online(timestamp_ms);
                        slicer.do_cleanup();
                    }
                    match cmd {
                        Command::Ping => {
                            let raw_now_dt = delphi_now_raw();
                            let corrected_now_dt = delphi_now();
                            if let Some((ping_update, response)) =
                                Client::reader_build_ping_update_and_response(
                                    &reader_protocol,
                                    &reader_ping_state,
                                    &server_time_delta_handle,
                                    &payload,
                                    raw_now_dt,
                                    corrected_now_dt,
                                    total_sent.load(Ordering::Relaxed),
                                    total_recv_after,
                                )
                            {
                                Client::reader_send_raw_packet(
                                    &sock_clone,
                                    server_addr,
                                    &mac_ctx,
                                    &mac_key,
                                    Command::Ping,
                                    client_id,
                                    &response,
                                    mask_ver,
                                    &total_sent,
                                    debug_outgoing_blackhole,
                                );
                                pending_reader_decoded.lock().unwrap().push(ReaderDecodedMsg {
                                    cmd: hdr.cmd,
                                    payload: Some(payload.clone()),
                                    recv_bytes: n as u64,
                                    timestamp_ms,
                                    epoch: my_epoch,
                                    apply_recv_effects: true,
                                    sliced_stats: None,
                                    ping_update: Some(ping_update),
                                    handshake_update: None,
                                });
                            } else {
                                Client::push_reader_recv_side_effect(
                                    &pending_reader_decoded,
                                    hdr.cmd,
                                    n as u64,
                                    timestamp_ms,
                                    my_epoch,
                                );
                            }
                        }
                        Command::WrongHello
                        | Command::WantNewHello
                        | Command::NeedHelloAgain => {
                            pending_reader_decoded.lock().unwrap().push(ReaderDecodedMsg {
                                cmd: hdr.cmd,
                                payload: None,
                                recv_bytes: n as u64,
                                timestamp_ms,
                                epoch: my_epoch,
                                apply_recv_effects: true,
                                sliced_stats: None,
                                ping_update: None,
                                handshake_update: Some(Client::simple_handshake_update(cmd)),
                            });
                        }
                        Command::WhoAreYou => {
                            if let Some(hello) =
                                Client::decode_handshake_hello(&master_key, client_id, &payload)
                            {
                                let (update, encrypted) = Client::build_who_are_you_imfriend(
                                    &master_key,
                                    client_id,
                                    app_token,
                                    &mut reader_client_token,
                                    hello,
                                );
                                reader_protocol
                                    .lock()
                                    .unwrap()
                                    .set_decode_cipher(crate::crypto::cipher_from_key(
                                        &update.decode_key,
                                    ));
                                Client::reader_send_raw_packet(
                                    &sock_clone,
                                    server_addr,
                                    &mac_ctx,
                                    &mac_key,
                                    Command::ImFriend,
                                    client_id,
                                    &encrypted,
                                    mask_ver,
                                    &total_sent,
                                    debug_outgoing_blackhole,
                                );
                                thread::sleep(Duration::from_millis(IMFRIEND_DUPLICATE_DELAY_MS));
                                Client::reader_send_raw_packet(
                                    &sock_clone,
                                    server_addr,
                                    &mac_ctx,
                                    &mac_key,
                                    Command::ImFriend,
                                    client_id,
                                    &encrypted,
                                    mask_ver,
                                    &total_sent,
                                    debug_outgoing_blackhole,
                                );
                                pending_reader_decoded.lock().unwrap().push(ReaderDecodedMsg {
                                    cmd: hdr.cmd,
                                    payload: None,
                                    recv_bytes: n as u64,
                                    timestamp_ms,
                                    epoch: my_epoch,
                                    apply_recv_effects: true,
                                    sliced_stats: None,
                                    ping_update: None,
                                    handshake_update: Some(update),
                                });
                            } else {
                                Client::push_reader_recv_side_effect(
                                    &pending_reader_decoded,
                                    hdr.cmd,
                                    n as u64,
                                    timestamp_ms,
                                    my_epoch,
                                );
                            }
                        }
                        Command::Fine => {
                            if Client::decode_handshake_hello(&master_key, client_id, &payload)
                                .is_some()
                            {
                                pending_reader_decoded.lock().unwrap().push(ReaderDecodedMsg {
                                    cmd: hdr.cmd,
                                    payload: None,
                                    recv_bytes: n as u64,
                                    timestamp_ms,
                                    epoch: my_epoch,
                                    apply_recv_effects: true,
                                    sliced_stats: None,
                                    ping_update: None,
                                    handshake_update: Some(Client::fine_handshake_update()),
                                });
                            } else {
                                Client::push_reader_recv_side_effect(
                                    &pending_reader_decoded,
                                    hdr.cmd,
                                    n as u64,
                                    timestamp_ms,
                                    my_epoch,
                                );
                            }
                        }
                        Command::SizeTest => {
                            if let Some(ack) =
                                Client::build_size_ack_payload(&reader_protocol, &payload)
                            {
                                set_dont_fragment_for_socket(&sock_clone, true);
                                Client::reader_send_raw_packet(
                                    &sock_clone,
                                    server_addr,
                                    &mac_ctx,
                                    &mac_key,
                                    Command::SizeAck,
                                    client_id,
                                    &ack,
                                    mask_ver,
                                    &total_sent,
                                    debug_outgoing_blackhole,
                                );
                                set_dont_fragment_for_socket(&sock_clone, false);
                            }
                            Client::push_reader_recv_side_effect(
                                &pending_reader_decoded,
                                hdr.cmd,
                                n as u64,
                                timestamp_ms,
                                my_epoch,
                            );
                        }
                        Command::ProbeMTU => {
                            if let Some(ack) = Client::build_probe_mtu_ack_payload(&payload) {
                                set_dont_fragment_for_socket(&sock_clone, true);
                                Client::reader_send_raw_packet(
                                    &sock_clone,
                                    server_addr,
                                    &mac_ctx,
                                    &mac_key,
                                    Command::ProbeMTUAck,
                                    client_id,
                                    &ack,
                                    mask_ver,
                                    &total_sent,
                                    debug_outgoing_blackhole,
                                );
                                set_dont_fragment_for_socket(&sock_clone, false);
                            }
                            Client::push_reader_recv_side_effect(
                                &pending_reader_decoded,
                                hdr.cmd,
                                n as u64,
                                timestamp_ms,
                                my_epoch,
                            );
                        }
                        Command::SlicedACK => {
                            Client::push_sliced_ack(&incoming_sliced_acks, &payload);
                            Client::push_reader_recv_side_effect(
                                &pending_reader_decoded,
                                hdr.cmd,
                                n as u64,
                                timestamp_ms,
                                my_epoch,
                            );
                        }
                        Command::Sliced => {
                            if shutdown_flag.load(Ordering::Relaxed) {
                                break;
                            }
                            let (assembled, ack) = {
                                let mut slicer = slicer.lock().unwrap();
                                slicer.on_new_sliced(&payload)
                            };
                            if shutdown_flag.load(Ordering::Relaxed) {
                                break;
                            }
                            Client::reader_send_raw_packet(
                                &sock_clone,
                                server_addr,
                                &mac_ctx,
                                &mac_key,
                                Command::SlicedACK,
                                client_id,
                                &ack,
                                mask_ver,
                                &total_sent,
                                debug_outgoing_blackhole,
                            );
                            if let Some((datagram_num, cmd, payload, dup_count, blocks_count)) =
                                assembled
                            {
                                let decoded =
                                    Client::decode_data_read_int_payload_shared(
                                        &reader_protocol,
                                        cmd,
                                        &payload,
                                    );
                                slicer.lock().unwrap().receiving.remove(&datagram_num);
                                let (cmd, payload) = decoded
                                    .map(|(cmd, payload)| (cmd, Some(payload)))
                                    .unwrap_or((cmd, None));
                                pending_reader_decoded.lock().unwrap().push(ReaderDecodedMsg {
                                    cmd,
                                    payload,
                                    recv_bytes: n as u64,
                                    timestamp_ms,
                                    epoch: my_epoch,
                                    apply_recv_effects: true,
                                    sliced_stats: Some(ReaderSlicedStats {
                                        dup_count,
                                        blocks_count,
                                    }),
                                    ping_update: None,
                                    handshake_update: None,
                                });
                            } else {
                                Client::push_reader_recv_side_effect(
                                    &pending_reader_decoded,
                                    hdr.cmd,
                                    n as u64,
                                    timestamp_ms,
                                    my_epoch,
                                );
                            }
                        }
                        _ => {
                            let decoded = Client::reader_decode_data_packets(
                                &reader_protocol,
                                hdr.cmd,
                                &payload,
                                n as u64,
                                timestamp_ms,
                                my_epoch,
                            );
                            pending_reader_decoded.lock().unwrap().extend(decoded);
                        }
                    }
                }

                if notify_reader_work(&event_tx, &reader_wake_pending)
                    == RecvEnqueue::Disconnected
                {
                    break;
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

    fn parse_sliced_ack_payload(payload: &[u8]) -> Option<SlicedAck> {
        // Delphi OnNewSlicedACK reads Flags(32 bytes) + DatagramNum(2 bytes)
        // from the command payload after the transport header.
        if payload.len() < 34 {
            return None;
        }
        let mut flags = [0u8; 32];
        flags.copy_from_slice(&payload[0..32]);
        Some(SlicedAck {
            flags,
            datagram_num: u16::from_le_bytes([payload[32], payload[33]]),
        })
    }

    fn push_sliced_ack(queue: &Arc<Mutex<Vec<SlicedAck>>>, payload: &[u8]) {
        if let Some(ack) = Self::parse_sliced_ack_payload(payload) {
            queue.lock().unwrap().push(ack);
        }
    }

    fn push_reader_recv_side_effect(
        pending: &Arc<Mutex<Vec<ReaderDecodedMsg>>>,
        cmd: u8,
        recv_bytes: u64,
        timestamp_ms: i64,
        epoch: u32,
    ) {
        pending.lock().unwrap().push(ReaderDecodedMsg {
            cmd,
            payload: None,
            recv_bytes,
            timestamp_ms,
            epoch,
            apply_recv_effects: true,
            sliced_stats: None,
            ping_update: None,
            handshake_update: None,
        });
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

    fn build_who_are_you_imfriend(
        master_key: &MoonKey,
        client_id: u64,
        app_token: u64,
        client_token: &mut u64,
        hello: handshake::Hello,
    ) -> (ReaderHandshakeUpdate, Vec<u8>) {
        let server_token = hello.server_token;
        let peer_app_token = hello.app_token;
        let (encode_key, decode_key) = crypto::generate_sub_keys(master_key, server_token);

        *client_token += 1;
        let mut im = hello;
        im.mix_ts = *client_token;
        im.app_token = app_token;
        im.timestamp = delphi_now();
        let packed = im.to_bytes_packed();
        let aad = client_id.to_le_bytes();
        let cipher = crate::crypto::cipher_from_key(&encode_key);
        let encrypted = crypto::encrypt_with_cipher(&cipher, &packed, &aad);

        (
            ReaderHandshakeUpdate {
                cmd: Command::WhoAreYou,
                server_token,
                peer_app_token,
                client_token: *client_token,
                encode_key,
                decode_key,
            },
            encrypted,
        )
    }

    fn fine_handshake_update() -> ReaderHandshakeUpdate {
        Self::simple_handshake_update(Command::Fine)
    }

    fn simple_handshake_update(cmd: Command) -> ReaderHandshakeUpdate {
        ReaderHandshakeUpdate {
            cmd,
            server_token: 0,
            peer_app_token: 0,
            client_token: 0,
            encode_key: [0; 16],
            decode_key: [0; 16],
        }
    }

    fn build_size_ack_payload(
        reader_protocol: &Arc<Mutex<ReaderProtocolState>>,
        payload: &[u8],
    ) -> Option<Vec<u8>> {
        if payload.len() < 6 {
            return None;
        }
        let size = u16::from_le_bytes(payload[0..2].try_into().unwrap());
        let series = u16::from_le_bytes(payload[4..6].try_into().unwrap());
        if (size as usize) < 6 {
            return None;
        }
        let series = reader_protocol
            .lock()
            .unwrap()
            .update_data_size_ack_series_num(series);
        let mut ack = vec![0u8; size as usize];
        ack[0..2].copy_from_slice(&size.to_le_bytes());
        ack[4..6].copy_from_slice(&series.to_le_bytes());
        Some(ack)
    }

    fn build_probe_mtu_ack_payload(payload: &[u8]) -> Option<Vec<u8>> {
        if payload.len() < 5 {
            return None;
        }
        let probe_id = u16::from_le_bytes(payload[0..2].try_into().unwrap());
        let probe_index = payload[2];
        let test_size = u16::from_le_bytes(payload[3..5].try_into().unwrap());
        if (test_size as usize) < 5 {
            return None;
        }
        let mut ack = vec![0u8; test_size as usize];
        ack[0..2].copy_from_slice(&probe_id.to_le_bytes());
        ack[2] = probe_index;
        ack[3..5].copy_from_slice(&test_size.to_le_bytes());
        Some(ack)
    }

    fn reader_build_ping_update_and_response(
        reader_protocol: &Arc<Mutex<ReaderProtocolState>>,
        reader_ping_state: &Arc<Mutex<ReaderPingState>>,
        server_time_delta_handle: &Arc<std::sync::atomic::AtomicU64>,
        payload: &[u8],
        raw_now_dt: f64,
        corrected_now_dt: f64,
        total_sent_before_ping: u64,
        total_recv_after_packet: u64,
    ) -> Option<(ReaderPingUpdate, Vec<u8>)> {
        if payload.len() < 50 {
            return None;
        }

        // UDPRead Ping branch: update transport ping fields before DataRead.
        let round_trip_delay = i32::from_le_bytes(payload[16..20].try_into().unwrap()) as i64;
        let actual_pmtu = u16::from_le_bytes(payload[20..22].try_into().unwrap());
        let global_timing_orders = u16::from_le_bytes(payload[22..24].try_into().unwrap());
        let overheat = payload[24];
        let rs = payload[41] as f64 * (1.0 / 255.0);

        const COMFORTABLE_RS: f64 = 0.92;
        const CRITICAL_RS: f64 = 0.85;
        const MIN_RATE: i32 = 256 * 1024;
        const MAX_RATE: i32 = 8 * 1024 * 1024;
        let (ping_count, can_send_rate, used_sliced_limit) = {
            let mut state = reader_ping_state.lock().unwrap();
            if state.used_sliced_limit {
                let new_rate = if rs > COMFORTABLE_RS {
                    let increase = (state.can_send_rate as f64 * 0.03).round() as i32;
                    state.can_send_rate + increase.max(32 * 1024)
                } else if rs < CRITICAL_RS {
                    (state.can_send_rate as f64 * 0.85).round() as i32
                } else {
                    let drift = (rs - COMFORTABLE_RS) / COMFORTABLE_RS;
                    (state.can_send_rate as f64 * (1.0 + drift * 0.05)).round() as i32
                };
                state.can_send_rate = new_rate.clamp(MIN_RATE, MAX_RATE);
                state.used_sliced_limit = false;
            }
            state.ping_count = state.ping_count.wrapping_add(1);
            (
                state.ping_count,
                state.can_send_rate,
                state.used_sliced_limit,
            )
        };

        // DataReadInt(MPC_Ping): write server ACK bitmap into TmpSlider.
        reader_protocol
            .lock()
            .unwrap()
            .apply_ping_ack_bitmap(payload);

        // ClientNewData(MPC_Ping): update wall-clock deltas before SendPing.
        let initial_time = f64::from_le_bytes(payload[8..16].try_into().unwrap());
        let server_time = f64::from_le_bytes(payload[0..8].try_into().unwrap());
        let server_time_delta = initial_time - raw_now_dt;
        server_time_delta_handle.store(
            server_time_delta.to_bits(),
            std::sync::atomic::Ordering::Relaxed,
        );
        set_server_time_delta_global(server_time_delta);
        let net_lag_ping = ((corrected_now_dt - server_time) * 86400000.0).abs() as i64;

        // SendPing(var APing): mutate the same Ping struct, then append our ACK half.
        let mut response = payload[..50].to_vec();
        response[0..8].copy_from_slice(&corrected_now_dt.to_le_bytes());
        response[25..33].copy_from_slice(&total_sent_before_ping.to_le_bytes());
        response[33..41].copy_from_slice(&total_recv_after_packet.to_le_bytes());
        let (ack_start, ack_words) = reader_protocol.lock().unwrap().build_ack_half();
        response[42..50].copy_from_slice(&ack_start.to_le_bytes());
        for word in &ack_words {
            response.extend_from_slice(&word.to_le_bytes());
        }

        Some((
            ReaderPingUpdate {
                ping_count,
                round_trip_delay,
                actual_pmtu,
                global_timing_orders,
                overheat,
                rs,
                server_time_delta,
                net_lag_ping,
                can_send_rate,
                used_sliced_limit,
            },
            response,
        ))
    }

    fn reader_send_raw_packet(
        sock: &UdpSocket,
        addr: Option<SocketAddr>,
        mac_ctx: &moonproto_transport::MacContext,
        mac_key: &MoonKey,
        cmd: Command,
        client_id: u64,
        payload: &[u8],
        mask_ver: u8,
        total_sent: &Arc<AtomicU64>,
        debug_outgoing_blackhole: bool,
    ) {
        let Some(addr) = addr else {
            return;
        };
        let mut packet = Vec::new();
        let extra = moonproto_transport::transport_pack_into_with_mac(
            &mut packet,
            mac_ctx,
            mac_key,
            cmd as u8,
            client_id,
            payload,
            mask_ver,
        );

        if debug_outgoing_blackhole {
            if trace_io_enabled() {
                eprintln!(
                    "[mp-io-tx-blackhole] cmd={:?} raw={} packet_len={} extra_len={} addr={}",
                    cmd,
                    cmd as u8,
                    packet.len(),
                    extra.as_ref().map(|p| p.len()).unwrap_or(0),
                    addr
                );
            }
            return;
        }

        if let Some(extra_pkt) = extra.as_deref() {
            if let Err(e) = sock.send_to(extra_pkt, addr) {
                warn!("send_to(extra, cmd={}) failed: {e}", cmd as u8);
            }
        }
        match sock.send_to(&packet, addr) {
            Ok(_) => {
                total_sent.fetch_add(packet.len() as u64, Ordering::Relaxed);
                if trace_io_enabled() {
                    eprintln!(
                        "[mp-io-tx-ok] cmd={:?} raw={} packet_len={} total_sent={}",
                        cmd,
                        cmd as u8,
                        packet.len(),
                        total_sent.load(Ordering::Relaxed)
                    );
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                warn!(
                    "send_to(cmd={}) would block (kernel send buffer full)",
                    cmd as u8
                );
            }
            Err(e) if is_datagram_too_large_error(&e) => {
                warn!(
                    "send_to(cmd={}) packet too large for current path MTU: {e}",
                    cmd as u8
                );
            }
            Err(e) => {
                error!("send_to(cmd={}) failed: {e}", cmd as u8);
            }
        }
    }

    #[cfg(test)]
    fn on_new_sliced_ack(&self, payload: &[u8]) {
        Self::push_sliced_ack(&self.incoming_sliced_acks, payload);
    }

    fn get_copy_acks(&self) -> Vec<SlicedAck> {
        let mut acks = self.incoming_sliced_acks.lock().unwrap();
        std::mem::take(&mut *acks)
    }

    fn copy_recvd_data(&mut self) {
        if let Some(tmp_slider) = self.reader_protocol.lock().unwrap().copy_tmp_slider() {
            self.recvd_slider = tmp_slider;
        }
    }

    fn apply_copy_acks(&mut self, copy_acks: Vec<SlicedAck>, cur_tm: i64) {
        for ack in copy_acks {
            self.apply_sliced_ack(ack, cur_tm);
        }
    }

    fn apply_regular_hl_ack(&mut self) {
        if !self.recvd_slider.has_new_data {
            return;
        }
        self.recvd_slider.has_new_data = false;

        let limit = (self.recvd_slider.r_count.max(0) as u64) * 64;
        self.pending_h.retain(|d| {
            if d.msg_num < self.recvd_slider.start_num {
                return true;
            }
            let offset = d.msg_num - self.recvd_slider.start_num;
            if offset >= limit {
                return true;
            }
            let word_idx = (offset >> 6) as usize;
            let bit_idx = offset & 63;
            (self.recvd_slider.bit_field[word_idx] >> bit_idx) & 1 == 0
        });
    }

    fn apply_sliced_ack(&mut self, ack: SlicedAck, now_ms: i64) {
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
                    // Delphi ApplyACK frees ACKed pieces before retry. Rust keeps
                    // arrays indexed by block number, so recompute over unACKed
                    // pieces only.
                    s.refresh_last_checked_from_unacked(now_ms);
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

    #[cfg(test)]
    fn handle_udp_command(
        &mut self,
        cmd: Command,
        raw_cmd: u8,
        payload: &[u8],
        sink: &mut DispatchSink<'_>,
        reader_dataread_core_done: bool,
    ) {
        if matches!(
            cmd,
            Command::WantNewHello | Command::WrongHello | Command::WhoAreYou | Command::Fine
        ) {
            self.waiting_hello = false;
        }

        match cmd {
            Command::WrongHello => {
                self.auth_status = AuthStatus::Connected;
            }
            Command::WantNewHello => {
                self.full_reset();
                self.last_sent_hello = NEVER_SENT_MS;
                self.auth_status = AuthStatus::Connected;
                self.authorized = false;
                self.need_connect = true;
                self.soft_reconnect = false;
            }
            Command::NeedHelloAgain => {
                let now = self.now_ms();
                if (now - self.last_need_hello_again).abs() > NEED_HELLO_AGAIN_THROTTLE_MS {
                    self.last_need_hello_again = now;
                    if !self.waiting_hello {
                        self.waiting_hello_start = now;
                    }
                    self.waiting_hello = true;
                    self.last_sent_hello = NEVER_SENT_MS;
                }
            }
            Command::WhoAreYou | Command::Fine => {
                self.handle_handshake(cmd, payload);
            }
            Command::SizeTest => {
                self.handle_size_test(payload);
            }
            Command::ProbeMTU => {
                self.handle_probe_mtu(payload);
            }
            Command::Sliced => {
                let (assembled, ack) = {
                    let mut slicer = self.slicer.lock().unwrap();
                    slicer.set_last_online(self.now_ms());
                    slicer.on_new_sliced(payload)
                };
                // Per-block ACK (one SlicedACK per received block) — НАМЕРЕННО.
                // Для торгового канала критична скорость: минимальная задержка обнаружения
                // потери блока важнее экономии bandwidth на мелких ACK (~34 байта каждый).
                // Batching/timer-based ACK снижает bandwidth, но увеличивает retry-латентность.
                // НЕ оптимизировать частоту отправки. См. ARCHITECTURE.md OPEN-QUESTIONS §6 (ЗАКРЫТО).
                self.send_raw_packet(Command::SlicedACK, &ack);
                if let Some((datagram_num, inner_cmd, data, dup_count, blocks_count)) = assembled {
                    self.data_read_int(inner_cmd, &data, sink);
                    self.slicer.lock().unwrap().receiving.remove(&datagram_num);
                    // AvgDupCount EMA (matches Common.pas:701-703)
                    let dup_pct = dup_count as f64 / blocks_count.max(1) as f64 * 100.0;
                    if self.avg_dup_count == 0.0 {
                        self.avg_dup_count = dup_pct;
                    } else {
                        // B-19: * 0.1 вместо / 10.0 — FDIV ~13-25 циклов, FMUL ~4-5.
                        self.avg_dup_count = (self.avg_dup_count * 9.0 + dup_pct) * 0.1;
                    }
                }
            }
            Command::SlicedACK => {
                self.on_new_sliced_ack(payload);
            }
            Command::Ping => {
                self.handle_ping_with_reader_core(payload, sink, reader_dataread_core_done);
            }
            _ => {
                self.data_read(raw_cmd, payload, sink);
            }
        }
    }

    #[cfg(test)]
    fn data_read(&mut self, raw_cmd: u8, payload: &[u8], sink: &mut DispatchSink<'_>) {
        let cmd = Command::from_byte(raw_cmd);
        if cmd == Command::Grouped {
            let mut pos = 0;
            while pos + 3 <= payload.len() {
                let sub_cmd = payload[pos];
                pos += 1;
                let sz = u16::from_le_bytes([payload[pos], payload[pos + 1]]) as usize;
                pos += 2;
                if pos + sz > payload.len() {
                    break;
                }
                self.data_read_int(sub_cmd, &payload[pos..pos + sz], sink);
                pos += sz;
            }
        } else {
            self.data_read_int(raw_cmd, payload, sink);
        }
    }

    #[cfg(test)]
    fn decode_data_read_int_payload(&self, raw_cmd: u8, data: &[u8]) -> Option<(u8, Vec<u8>)> {
        Self::decode_data_read_int_payload_shared(&self.reader_protocol, raw_cmd, data)
    }

    fn decode_data_read_int_payload_shared(
        reader_protocol: &Arc<Mutex<ReaderProtocolState>>,
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
            let decrypted = {
                let mut protocol = reader_protocol.lock().unwrap();
                let ReaderProtocolState {
                    decode_cipher,
                    slider,
                    ..
                } = &mut *protocol;
                let decode_cipher = decode_cipher.as_ref()?;
                crypted::decrypt_command(decode_cipher, &payload, slider)
            };
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

        // MPC_Ping is handled in handle_ping. Its server ACK bitmap follows the
        // Delphi TmpSlider -> RecvdSlider -> ApplyRegularHLAck path, not this
        // generic delivery branch.
        Some((cmd, payload.into_owned()))
    }

    fn reader_decode_data_packets(
        reader_protocol: &Arc<Mutex<ReaderProtocolState>>,
        raw_cmd: u8,
        payload: &[u8],
        recv_bytes: u64,
        timestamp_ms: i64,
        epoch: u32,
    ) -> Vec<ReaderDecodedMsg> {
        let mut out = Vec::new();
        let mut push_decoded = |raw_cmd: u8, data: &[u8]| {
            let apply_recv_effects = out.is_empty();
            let decoded = Self::decode_data_read_int_payload_shared(reader_protocol, raw_cmd, data);
            let (cmd, payload) = decoded
                .map(|(cmd, payload)| (cmd, Some(payload)))
                .unwrap_or((raw_cmd, None));
            out.push(ReaderDecodedMsg {
                cmd,
                payload,
                recv_bytes,
                timestamp_ms,
                epoch,
                apply_recv_effects,
                sliced_stats: None,
                ping_update: None,
                handshake_update: None,
            });
        };

        if Command::from_byte(raw_cmd) == Command::Grouped {
            let mut pos = 0;
            while pos + 3 <= payload.len() {
                let sub_cmd = payload[pos];
                pos += 1;
                let sz = u16::from_le_bytes([payload[pos], payload[pos + 1]]) as usize;
                pos += 2;
                if pos + sz > payload.len() {
                    break;
                }
                push_decoded(sub_cmd, &payload[pos..pos + sz]);
                pos += sz;
            }
            if out.is_empty() {
                out.push(ReaderDecodedMsg {
                    cmd: raw_cmd,
                    payload: None,
                    recv_bytes,
                    timestamp_ms,
                    epoch,
                    apply_recv_effects: true,
                    sliced_stats: None,
                    ping_update: None,
                    handshake_update: None,
                });
            }
        } else {
            push_decoded(raw_cmd, payload);
        }

        out
    }

    #[cfg(test)]
    fn data_read_int(&mut self, raw_cmd: u8, data: &[u8], sink: &mut DispatchSink<'_>) {
        let Some((cmd, payload)) = self.decode_data_read_int_payload(raw_cmd, data) else {
            return;
        };
        self.deliver_data_read_int_decoded(cmd, payload, sink);
    }

    fn deliver_data_read_int_decoded(
        &mut self,
        cmd: u8,
        payload: Vec<u8>,
        sink: &mut DispatchSink<'_>,
    ) {
        // Engine API responses: попытаться доставить в pending registry / chunked
        // candles aggregator / internal recovery flags. Если UID не зарегистрирован —
        // пробрасываем как обычный data callback.
        if cmd == Command::API as u8 {
            if let Some(resp) = parse_engine_response(&payload) {
                // 1. Chunked candles (RequestCandlesData) — aggregator поддерживает
                // несколько response пакетов с одинаковым UID. До завершения сборки
                // не дропаем slot.
                if resp.method == EngineMethod::RequestCandlesData
                    && self.handle_candles_chunk(&resp)
                {
                    // Чанк потреблён aggregator'ом. Передаём в on_data только
                    // если потребитель НЕ использует async API (тогда тут merged
                    // ещё не готов — пусть приложение видит сырые chunks).
                    // Однако: чтобы не путать — пропускаем on_data callback.
                    // Async-потребитель получит результат через Receiver<MergedCandles>.
                    return;
                }
                // Если slot не зарегистрирован — fallback на pending registry /
                // on_data (для пользователей старого fire-and-forget API).

                // 2. Active library: auto-clear indexes_fetch_in_flight на ответе
                // GetMarketsIndexes (любой — даже неуспешный, чтобы не зависнуть навсегда).
                if resp.method == EngineMethod::GetMarketsIndexes {
                    self.indexes_fetch_in_flight = false;
                    if resp.success {
                        // Запоминаем что для текущего PeerAppToken индексы получены.
                        self.tracked_indexes_peer_app_token = self.peer_app_token;
                        if self.update_markets_after_indexes {
                            self.update_markets_after_indexes = false;
                            self.send_api_request(
                                &crate::commands::engine_request::update_markets_list(),
                            );
                        }
                    }
                }

                // 3. Pending registry (обычный async API).
                let pending_consumed = self.api_pending.dispatch(resp).is_none();
                if !pending_consumed || sink.is_buffer() {
                    // Если response не ждал конкретный receiver — это обычный API event.
                    // Если ждал, но мы в Dispatcher mode, всё равно отдаём raw payload
                    // dispatcher'у: active state (markets/indexes/tags) должен обновиться
                    // независимо от того, ждёт ли user code этот же ответ через Receiver.
                    // Callback mode сохраняет старую семантику: pending response не
                    // дублируется в on_data callback.
                    sink.deliver_owned(Command::API, payload);
                }
                return;
            }
            // Не распарсилось — fallback на raw sink.
        }

        sink.deliver_owned(Command::from_byte(cmd), payload);
    }

    /// Поглотить candles chunk через pending aggregator. Возвращает `true` если slot
    /// найден и chunk обработан (даже если merged ещё не готов — копить дальше);
    /// `false` если UID не зарегистрирован (потребитель не использует async API).
    ///
    /// Когда aggregator вернул merged — sender'у отправляется готовый `MergedCandles`,
    /// slot удаляется. Если sender уже дропнут (receiver не ждёт) — slot всё равно
    /// удаляется (semantic = "fire-and-forget с финализацией").
    fn handle_candles_chunk(&mut self, resp: &EngineResponse) -> bool {
        // Проверяем slot отдельным lookup — потом полное удаление через remove() если merged.
        let now_ms = self.now_ms();
        if !resp.success {
            if let Some(partial) = self.pending_candles.remove(&resp.request_uid) {
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
            let Some(partial) = self.pending_candles.get_mut(&uid) else {
                return false;
            };
            let chunk_result = partial.aggregator.on_chunk_result(&resp.data);
            if matches!(
                chunk_result,
                CandlesChunkResult::Stored | CandlesChunkResult::Complete(_)
            ) {
                partial.last_activity_ms = now_ms;
            }
            chunk_result
        };
        if let CandlesChunkResult::Complete(zipped_data) = chunk_result {
            let markets = parse_request_candles_data_response(&zipped_data).unwrap_or_else(|| {
                log::warn!(target: "moonproto::client",
                    "candles aggregator merged but parse failed for uid={} ({} bytes)", uid, zipped_data.len());
                Vec::new()
            });
            if let Some(partial) = self.pending_candles.remove(&uid) {
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

    #[cfg(test)]
    fn handle_ping(&mut self, payload: &[u8], sink: &mut DispatchSink<'_>) {
        self.handle_ping_with_reader_core(payload, sink, false);
    }

    #[cfg(test)]
    fn handle_ping_with_reader_core(
        &mut self,
        payload: &[u8],
        sink: &mut DispatchSink<'_>,
        reader_dataread_core_done: bool,
    ) {
        let raw_now_dt = delphi_now_raw();
        let corrected_now_dt = delphi_now();
        self.handle_ping_at(
            payload,
            sink,
            raw_now_dt,
            corrected_now_dt,
            reader_dataread_core_done,
        );
    }

    #[cfg(test)]
    fn handle_ping_at(
        &mut self,
        payload: &[u8],
        sink: &mut DispatchSink<'_>,
        raw_now_dt: f64,
        corrected_now_dt: f64,
        reader_dataread_core_done: bool,
    ) {
        if payload.len() < 50 {
            return;
        }
        self.ping_count += 1;
        // TMoonProtoPing fields (matches MoonProtoDataStruct.pas:63-74)
        let initial_time = f64::from_le_bytes(payload[8..16].try_into().unwrap());
        self.round_trip_delay = i32::from_le_bytes(payload[16..20].try_into().unwrap()) as i64;
        // Delphi assigns APing.PMTU verbatim (MoonProtoUDPClient.pas:632-635).
        // Runtime ProbeMTU can legitimately grow above MaxNeededDatagramSize=8000
        // by +32 steps, so upper clamping here would break discovery.
        let pmtu_raw = u16::from_le_bytes(payload[20..22].try_into().unwrap());
        self.actual_pmtu = pmtu_raw;
        self.global_timing_orders = u16::from_le_bytes(payload[22..24].try_into().unwrap());
        self.overheat = payload[24];
        // B-19: умножение на const reciprocal вместо деления (FDIV → FMUL).
        // Компилятор инлайнит `1.0 / 255.0` как const expression.
        self.rs = payload[41] as f64 * (1.0 / 255.0);
        self.need_connect = false;

        // C9: ServerTimeDelta + NetLagPing (matches MoonProtoClient.pas:267-269)
        // Delphi uses raw `Now` for order-time correction, but NTP-corrected
        // `Now - GlobalMPTimeZoneOffset + GlobalMPTimeOffset` for NetLagPing
        // and the outbound Ping.Time.
        self.server_time_delta = initial_time - raw_now_dt;
        // audit_responsibility A5 / active library: автоматически пробрасываем delta в
        // per-Client `Arc<AtomicU64>` handle (multi-Client) И в глобальный atomic
        // (back-compat для одиночных EventDispatcher::new() без линковки). См. DEVIATION #23.
        self.server_time_delta_handle.store(
            self.server_time_delta.to_bits(),
            std::sync::atomic::Ordering::Relaxed,
        );
        set_server_time_delta_global(self.server_time_delta);
        let server_time = f64::from_le_bytes(payload[0..8].try_into().unwrap());
        self.net_lag_ping = ((corrected_now_dt - server_time) * 86400000.0).abs() as i64;

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
            self.can_send_rate = new_rate.clamp(MIN_RATE, MAX_RATE);
            self.used_sliced_limit = false;
        }
        self.reader_ping_state.lock().unwrap().sync_from_main(
            self.ping_count,
            self.can_send_rate,
            self.used_sliced_limit,
        );

        // DataReadInt(MPC_Ping): parse server's ACK bitmap into TmpSlider before
        // OnNewData/SendPing. Delphi DataReadInt writes TmpSlider, then
        // TMoonProtoNetClient.ClientNewData sends the Ping response.
        if !reader_dataread_core_done {
            self.apply_ping_ack_bitmap(payload);
        }

        // Send ping response (matches Delphi SendPing exactly):
        // - Struct written first (AckStart at offset 42 = SERVER's value, untouched)
        // - BuildAckHalf provides AckWords APPENDED after struct
        // BuildAckHalf fills AckStart + AckWords, then we write struct with correct AckStart
        //
        let mut response = payload[..50].to_vec();
        response[0..8].copy_from_slice(&corrected_now_dt.to_le_bytes());
        response[25..33].copy_from_slice(&self.total_sent.load(Ordering::Relaxed).to_le_bytes());
        response[33..41].copy_from_slice(&self.total_recv.to_le_bytes());
        let (ack_start, ack_words) = self.reader_protocol.lock().unwrap().build_ack_half();
        response[42..50].copy_from_slice(&ack_start.to_le_bytes());
        for w in &ack_words {
            response.extend_from_slice(&w.to_le_bytes());
        }
        self.send_raw_packet(Command::Ping, &response);

        sink.deliver(Command::Ping, payload);
    }

    #[cfg(test)]
    fn apply_ping_ack_bitmap(&mut self, payload: &[u8]) {
        self.reader_protocol
            .lock()
            .unwrap()
            .apply_ping_ack_bitmap(payload);
    }

    #[cfg(test)]
    fn handle_handshake(&mut self, cmd: Command, payload: &[u8]) {
        let Some(hello) =
            Self::decode_handshake_hello(&self.cfg.master_key, self.cfg.client_id, payload)
        else {
            return;
        };

        if cmd == Command::WhoAreYou {
            let mut client_token = self.client_token;
            let (update, encrypted) = Self::build_who_are_you_imfriend(
                &self.cfg.master_key,
                self.cfg.client_id,
                self.app_token,
                &mut client_token,
                hello,
            );
            self.apply_reader_handshake_update(update, self.now_ms());
            // Delphi sends ImFriend twice with a blocking Sleep(32) between sends
            // before it returns to the UDP read loop. Keep that ordering: post-Fine
            // active work must not overtake the duplicate ImFriend.
            self.send_raw_packet(Command::ImFriend, &encrypted);
            thread::sleep(Duration::from_millis(IMFRIEND_DUPLICATE_DELAY_MS));
            self.send_raw_packet(Command::ImFriend, &encrypted);
        }
        if cmd == Command::Fine {
            self.apply_reader_handshake_update(Self::fine_handshake_update(), self.now_ms());
        }
    }

    /// audit_robustness H5: process-global clock-jump generation → force_disconnect.
    /// Извлечён в метод для testability + чтобы main loop был чище.
    fn check_clock_jump(&mut self) {
        use std::sync::atomic::Ordering;
        let generation = CLOCK_JUMP_GENERATION.load(Ordering::Relaxed);
        if generation != self.seen_clock_jump_generation {
            self.seen_clock_jump_generation = generation;
            log::warn!(target: "moonproto::client",
                "clock jump → force_disconnect; reconnect will refresh handshake timestamp");
            self.force_disconnect = true;
        }
    }

    /// Periodic timeout cleanup/retry for an in-flight markets-index restore marker.
    /// UDP-ответ может потеряться — без этой проверки `indexes_fetch_in_flight = true`
    /// остался бы навсегда. До Init запрос НЕ отправляется; после Init reconnect
    /// restore имеет право повторить `GetMarketsIndexes`, потому что пользовательский
    /// intent уже был задан единственным init-проходом.
    ///
    /// Вызывается из main loop'ов `run` / `run_with_dispatcher` раз за тик.
    fn check_indexes_fetch_timeout(&mut self, now_ms: i64) {
        const INDEXES_FETCH_TIMEOUT_MS: i64 = 12_000;
        if self.indexes_fetch_in_flight
            && now_ms - self.indexes_fetch_started_ms > INDEXES_FETCH_TIMEOUT_MS
        {
            self.indexes_fetch_in_flight = false;
            if self.domain_ready
                && self.domain_restore_needs_indexes()
                && self.peer_app_token != 0
                && !self.market_indexes_current_for_peer()
            {
                self.send_markets_indexes_restore_request(now_ms);
            }
        }
    }

    #[cfg(test)]
    fn handle_size_test(&mut self, payload: &[u8]) {
        // PMTU discovery шлёт серию ~17 SizeTest пакетов каждые ~5с (Delphi
        // MoonProtoUDPClient.pas). Старый throttle 10/sec **ломал** PMTU
        // discovery — серия не помещалась в окно. Delphi не throttle'ит,
        // возвращаем byte-exact behavior.
        let Some(ack) = Self::build_size_ack_payload(&self.reader_protocol, payload) else {
            return;
        };
        self.set_dont_fragment(true);
        self.send_raw_packet(Command::SizeAck, &ack);
        self.set_dont_fragment(false);
    }

    #[cfg(test)]
    fn handle_probe_mtu(&mut self, payload: &[u8]) {
        // ProbeMTU тоже не throttle — см. handle_size_test rationale.
        let Some(ack) = Self::build_probe_mtu_ack_payload(payload) else {
            return;
        };
        self.set_dont_fragment(true);
        self.send_raw_packet(Command::ProbeMTUAck, &ack);
        self.set_dont_fragment(false);
    }

    /// Set IP_DONTFRAGMENT socket option (matches TUDPServerMP.TurnDontFragment).
    /// **Cross-platform**: Windows / Linux / Android / macOS / iOS.
    /// Реализовано через `setsockopt` напрямую (socket2 имеет `set_mtu_discover` только на Linux).
    #[cfg(test)]
    fn set_dont_fragment(&self, enable: bool) {
        if let Some(ref sock) = self.socket {
            set_dont_fragment_for_socket(sock, enable);
        }
    }

    /// Crypt + CreateSlicedObject + send (matches MoonProtoIntStruct.pas:1058-1196)
    fn create_sliced_and_send(&mut self, item: &SendItem) {
        let header_size = 15u16;
        let slice_hdr_size = 4u16;

        // TMoonProtoDataToSend.Create compresses before CreateSlicedObject sees
        // the stream. Therefore size/empty checks below use the effective
        // compressed payload, not the original item data.
        let (send_cmd, send_data) = Self::maybe_compress(item.cmd, &item.data);

        // MaxSlicedDataSize check (matches IntStruct.pas:1071-1079)
        let pmtu_for_check_i32 =
            self.actual_pmtu as i32 - header_size as i32 - slice_hdr_size as i32;
        if pmtu_for_check_i32 <= 0 {
            return;
        }
        let pmtu_for_check = pmtu_for_check_i32 as usize;
        let max_sliced_data_size = pmtu_for_check * 256 - 12 - 1; // 12=CryptoHeader, 1=cmd byte
        if send_data.len() >= max_sliced_data_size {
            return; // too large, drop (Delphi logs + exits)
        }
        if send_data.is_empty() {
            return; // empty data (Delphi logs + exits before Crypt)
        }

        // Crypt if needed
        let (wire_cmd, wire_data, msg_num) = if item.encrypted {
            let msg_num = if item.msg_num != 0 {
                item.msg_num // retry — reuse existing MsgNum
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
            let wire_cmd = Self::crypted_wire_cmd(send_cmd);
            (wire_cmd, encrypted_data, msg_num)
        } else {
            (send_cmd, send_data.into_owned(), 0u64)
        };

        // CreateSlicedObject
        let pmtu = (self.actual_pmtu - header_size - slice_hdr_size) as usize;
        let total_size = wire_data.len() + 1; // +1 cmd byte in block 0
        let n_blocks = total_size.div_ceil(pmtu).max(1);
        let max_block_num = (n_blocks - 1) as u8;
        let datagram_num = self.send_datagram_num;
        self.send_datagram_num = self.send_datagram_num.wrapping_add(1);

        if trace_io_enabled() {
            let api = if item.cmd == Command::API as u8 && item.data.len() >= 12 {
                let uid = u64::from_le_bytes(item.data[3..11].try_into().unwrap());
                let method = item.data[11];
                format!(" api_uid={uid} api_method={method}")
            } else {
                String::new()
            };
            eprintln!(
                "[mp-sliced-queue] d={} inner_cmd={:?} raw={} encrypted={} payload_len={} blocks={} max_retries={}{}",
                datagram_num,
                Command::from_byte(item.cmd),
                item.cmd,
                item.encrypted,
                item.data.len(),
                n_blocks,
                item.max_retries,
                api
            );
        }

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

            sent_slices.push(slice);
        }

        // Store in Sending list with priority insert (matches IntStruct.pas:1112-1116)
        let new_sliced = SentSliced {
            datagram_num,
            // Delphi `TMoonProtoSlice.Create` and `TMoonProtoSlicedData.Create`
            // initialise LastChecked to 0. `CreateSlicedObject` only enqueues the
            // slices; actual sends happen below in `retry_sliced` / CheckSeningData
            // under ClientLimit.
            piece_last_checked: vec![0; n_blocks],
            slices: sent_slices,
            ack_flags: [0u8; 32],
            blocks_count: n_blocks,
            sent_count: 0,
            last_checked: 0,
            retry_count: 0,
            max_retry_count: item.max_retries,
            u_key: item.u_key,
        };
        // Priority: fewer blocks → earlier in queue (smaller datagrams retry first)
        let insert_pos = self
            .sending
            .iter()
            .position(|s| s.blocks_count > n_blocks)
            .unwrap_or(self.sending.len());
        self.sending.insert(insert_pos, new_sliced);
        self.last_checked_slices = 0;

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

            // Delphi `Client.Crypt`: outer MPC_Crypted carries COMPRESSED_FLAG too
            // when the encrypted inner command is compressed.
            let wire_cmd = Self::crypted_wire_cmd(eff_cmd);

            self.do_send_mp_data_wire(wire_cmd, &encrypted);

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
                // Delphi `PendingH` не имеет capacity-cap: H-команды живут до ACK
                // или исчерпания `RetryLeft`. Старые trading-команды не дропаются
                // искусственно при большом burst'е.
                self.pending_h.push(pending_item);
            }
        } else {
            self.do_send_mp_data_wire(eff_cmd, &eff_data);
        }
        item.last_sent_at = cur_tm;
    }

    fn dedup_send_items_by_u_key(items: &mut Vec<SendItem>) {
        let mut idx = 0;
        while idx < items.len() {
            let u_key = items[idx].u_key;
            if !u_key.is_none() && items[idx + 1..].iter().any(|item| item.u_key == u_key) {
                items.remove(idx);
            } else {
                idx += 1;
            }
        }
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
            Command::Crypted as u8 | COMPRESSED_FLAG
        } else {
            Command::Crypted as u8
        }
    }

    /// Retry pending H-commands (matches CheckSeningData:944-954).
    /// **Порядок ВАЖЕН** (byte-exact с Delphi):
    ///   1. clone (с текущим retry_left → WantACK = (retry_left > 0))
    ///   2. resend
    ///   3. decrement retry_left
    ///   4. check ≤ 0 → drop
    ///
    /// Это гарантирует что **последний** retry уходит с WantACK=true (сервер пришлёт ACK).
    fn retry_pending_h(&mut self, cur_tm: i64) {
        // Delphi: Max(200, Min(500, round(Client.RoundTripDelay * 1.1 + 10)))
        let path_delay =
            ((self.round_trip_delay as f64 * 1.1 + 10.0).round() as i64).clamp(200, 500);
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
        if self.sending.is_empty() {
            return;
        }

        // Outer gate: only check if enough time passed (matches Common.pas:970).
        if (cur_tm - self.last_checked_slices).abs() <= self.round_trip_delay {
            return;
        }

        // TripDelayK adaptation every 2s (matches :975-979). Delphi does this
        // before PathDelay is computed, so the same tick uses the new K.
        if (cur_tm - self.last_set_trip_k).abs() > 2000 {
            self.last_set_trip_k = cur_tm;
            if self.avg_dup_count > 5.0 {
                self.trip_delay_k = (self.trip_delay_k + 0.05).min(1.25);
            }
            if self.avg_dup_count == 0.0 {
                self.trip_delay_k = (self.trip_delay_k - 0.01).max(1.05);
            }
        }

        let path_delay = (self.round_trip_delay as f64 * self.trip_delay_k + 10.0).round() as i64;
        let cycle_time_ms = 5.0f64.max(self.actual_sleep_time).min(15.0);
        // B-19: * 0.001 вместо / 1000.0 (FDIV → FMUL on hot retry path).
        // Delphi uses `round(Client.CanSendRate * CycleTimeMS / 1000.0)`,
        // so keep rounding instead of truncating on the hot retry boundary.
        let client_limit = (self.can_send_rate as f64 * cycle_time_ms * 0.001).round() as usize;
        let mut bytes_sent_at_once: usize = 0;
        self.last_checked_slices = cur_tm;

        // Аудит #2 (audit_delphi_deviation): индексы вместо clone. Раньше каждый
        // ретранслируемый блок копировался в `to_send: Vec<Vec<u8>>` — 200 alloc/sec
        // при congestion (10 active Sliced × 20 blocks × 2 retries/sec × ~500б).
        // Теперь храним `(sending_idx, block_num)` (16 байт), отправляем по ссылке.
        // Соответствует Delphi `SendCommand(Client, MPC_Sliced, Piece.data)` где Piece.data —
        // `TMemoryStream` по ссылке (ноль копий).
        let mut to_send_indices: Vec<(usize, usize)> = Vec::new();
        let mut to_remove = Vec::new();

        for (idx, sliced) in self.sending.iter_mut().enumerate() {
            if (cur_tm - sliced.last_checked).abs() <= path_delay {
                continue;
            }

            let prev_last_checked = sliced.last_checked;
            sliced.last_checked = cur_tm;

            for (block_num, slice_data) in sliced.slices.iter().enumerate() {
                if sliced.is_block_acked(block_num) {
                    continue;
                } // ACK'd

                // Per-piece check (matches :989)
                if sliced.piece_last_checked[block_num] != prev_last_checked {
                    continue;
                }
                if (cur_tm - sliced.piece_last_checked[block_num]).abs() <= path_delay {
                    continue;
                }
                if bytes_sent_at_once >= client_limit {
                    break;
                }

                if trace_io_enabled() {
                    eprintln!(
                        "[mp-sliced-tx] d={} block={}/{} retry_count={} sent_count={} bytes_this_tick={} client_limit={}",
                        sliced.datagram_num,
                        block_num,
                        sliced.blocks_count.saturating_sub(1),
                        sliced.retry_count,
                        sliced.sent_count,
                        bytes_sent_at_once,
                        client_limit
                    );
                }
                to_send_indices.push((idx, block_num));
                sliced.piece_last_checked[block_num] = cur_tm;
                sliced.sent_count += 1;
                bytes_sent_at_once += slice_data.len();
            }

            // Sliced.LastChecked = Min(remaining Piece.LastChecked) (matches :996
            // after Delphi `ApplyACK` removed ACKed pieces from the list).
            sliced.refresh_last_checked_from_unacked(cur_tm);

            // Conditional increment (matches :998-999)
            if prev_last_checked != sliced.last_checked {
                sliced.retry_count += 1;
            }
            self.last_checked_slices = self.last_checked_slices.min(sliced.last_checked);

            if sliced.retry_count > sliced.max_retry_count {
                to_remove.push(idx);
            }
        }

        // UsedSlicedLimit flag (matches :1009-1011)
        let used_limit_threshold = (client_limit as f64 * 0.8).round() as usize;
        if bytes_sent_at_once >= used_limit_threshold {
            self.used_sliced_limit = true;
            self.reader_ping_state.lock().unwrap().used_sliced_limit = true;
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
            (
                Self::crypted_wire_cmd(eff_cmd),
                std::borrow::Cow::Owned(encrypted),
            )
        } else {
            (eff_cmd, eff_data)
        };

        self.do_send_mp_data_wire(wire_cmd, &wire_data);
    }

    fn push_tmp_send_item(&mut self, wire_cmd: u8, wire_data: &[u8], accounted_size: usize) {
        self.tmp_send_buf.push(wire_cmd);
        let sz = wire_data.len() as u16;
        self.tmp_send_buf.extend_from_slice(&sz.to_le_bytes());
        self.tmp_send_buf.extend_from_slice(&wire_data);
        self.tmp_send_count += 1;
        self.tmp_send_size += accounted_size;
    }

    fn do_send_mp_data_wire(&mut self, wire_cmd: u8, wire_data: &[u8]) {
        // Delphi DoSendMPData uses `sz = d.ms.Size + GetHeaderSize + 3`.
        // The counter intentionally over-accounts the transport header for every
        // buffered item, and its overflow branch may send the current item
        // directly while keeping the previous buffer for a later NeedFlush.
        let accounted_size = wire_data.len() + 15 + 3;
        if self.tmp_send_size + accounted_size > self.actual_pmtu as usize {
            if self.tmp_send_size > accounted_size {
                self.flush_send_batch();
                self.push_tmp_send_item(wire_cmd, wire_data, accounted_size);
            } else {
                self.send_raw_packet_cmd(wire_cmd, wire_data);
            }
        } else {
            self.push_tmp_send_item(wire_cmd, wire_data, accounted_size);
        }
    }

    fn send_low_items_around_sliced_retry(&mut self, l_items: &[SendItem], cur_tm: i64) {
        // Delphi CheckSeningData has two Low phases:
        // 1. before Sliced retry: send only CopySendListL[0] with NeedFlush=true
        //    (or just flush accumulated H batch when there is no Low item);
        // 2. after Sliced retry: send the remaining Low items and flush.
        if let Some(first) = l_items.first() {
            self.batch_send_direct(first);
        }
        self.flush_send_batch();

        self.retry_sliced(cur_tm);

        for item in l_items.iter().skip(1) {
            self.batch_send_direct(item);
        }
        self.flush_send_batch();
    }

    /// Flush the send batch (matches DoSendTmpList, Common.pas:835-867).
    /// If count>1 → MPC_Grouped. If count==1 → single packet.
    /// A-19 fix: для single случая не re-парсим cmd/sz из buf — мы их знаем при добавлении.
    /// Single-element путь теперь без bounds-check парсинга.
    fn flush_send_batch(&mut self) {
        if self.tmp_send_count == 0 {
            return;
        }

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
        self.tmp_send_size = 0;
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
            cmd as u8,
            self.cfg.client_id,
            payload,
            self.cfg.mask_ver,
        );
        let packet = std::mem::take(&mut self.send_buf);
        self.dispatch_send(cmd as u8, &packet, extra.as_deref(), addr);
        self.send_buf = packet;
        self.send_buf.clear();
    }

    /// Реально отправляет пакет (плюс optional extra-пакет от moonext) с обработкой ошибок.
    /// Закрывает D-06: send errors больше не игнорируются через `.ok()`.
    /// EWOULDBLOCK логируется как warn (нормальная буферизация ядра). Прочие ошибки логируются,
    /// но не меняют reconnect-state: Delphi `DoSendPacket` возвращает false и не ставит
    /// `ForceDisconnect`.
    fn dispatch_send(&mut self, cmd: u8, packet: &[u8], extra: Option<&[u8]>, addr: SocketAddr) {
        if self.debug_outgoing_blackhole {
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

    fn send_hello(&mut self) {
        let payload = handshake::build_hello_packet(
            &self.cfg.master_key,
            self.cfg.client_id,
            &mut self.client_token,
            self.app_token,
            delphi_now(),
        );
        self.send_raw_packet(Command::Hello, &payload);
    }

    fn build_hello_again_packet(&mut self) -> Vec<u8> {
        self.client_token += 1;
        let mut hello = handshake::Hello::new(self.client_token, self.app_token);
        hello.timestamp = delphi_now();
        hello.peer_mix = crypto::mix_values(&hello.rnd, hello.mix_ts, self.server_token);
        let packed = hello.to_bytes_packed();
        let aad = self.cfg.client_id.to_le_bytes();
        if let Some(cipher) = self.encode_cipher.as_ref() {
            crypto::encrypt_with_cipher(cipher, &packed, &aad)
        } else {
            // Delphi initializes TMoonProtoClient.MPKeys[true/false] with MasterKey.
            // Early HelloAgain packets before WhoAreYou are real packets encrypted
            // with MasterKey, not skipped.
            crypto::encrypt(&self.cfg.master_key, &packed, &aad)
        }
    }

    fn send_hello_again(&mut self) {
        let encrypted = self.build_hello_again_packet();
        self.send_raw_packet(Command::HelloAgain, &encrypted);
    }

    fn check_hello_send(&mut self, cur_tm: i64) {
        if !self.need_connect || self.force_disconnect {
            return;
        }
        let interval = self.round_trip_delay.max(1000) * 2;
        if (cur_tm - self.last_sent_hello).abs() <= interval {
            return;
        }
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
        let throttle = (self.round_trip_delay + 50).clamp(200, 1500);
        let last_online = self.last_online;
        let authorized = self.authorized;

        let should = self.waiting_hello
            || (authorized
                && !self.need_connect
                && (cur_tm - last_online).abs() > OFFLINE_BASE_MS + self.round_trip_delay);
        if !should {
            return;
        }
        if (cur_tm - self.last_sent_hello).abs() <= throttle {
            return;
        }

        self.auth_status = AuthStatus::Offline;
        if !self.waiting_hello {
            self.waiting_hello_start = cur_tm;
        }
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
        if !self.soft_reconnect {
            self.full_reset();
        }
        self.connected = false;
        self.authorized = false;
        self.force_disconnect = false;
    }

    /// Matches TMoonProtoClient.Reset (IntStruct.pas:972-1000)
    /// Does NOT reset: server_token, actual_pmtu, send_datagram_num, pending_h,
    /// trip_delay_k, can_send_rate (those persist across reconnects).
    fn full_reset(&mut self) {
        self.crypt_msg_counter = 0;
        self.total_sent.store(0, Ordering::Relaxed);
        self.total_recv = 0;
        self.total_recv_shared.store(0, Ordering::Relaxed);
        self.rs = 1.0;
        self.used_sliced_limit = false;
        self.reader_ping_state
            .lock()
            .unwrap()
            .reset_protocol_session();
        self.reader_protocol.lock().unwrap().reset();
        self.recvd_slider = Slider::new();
        *self.slicer.lock().unwrap() = slicing::SlicingReceiver::new();
        self.pending_reader_decoded.lock().unwrap().clear();
        self.reader_wake_pending.store(false, Ordering::Release);
        self.last_online = 0;
        self.last_sent_hello = NEVER_SENT_MS;
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
        // audit_responsibility F9: pending candles aggregators — те же UID'ы старой сессии.
        // Симметрично с api_pending: senders drop'аются → receivers получают
        // `Err(Disconnected)` → потребитель делает re-request с новым UID. Иначе
        // зависнут до DEFAULT_PENDING_CANDLES_TIMEOUT_MS.
        self.pending_candles.clear();
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

    /// Returns true after the transport handshake has reached `AuthDone`.
    ///
    /// This is transport readiness, not full domain readiness. Use
    /// [`Self::is_domain_ready`] after `connect_and_init` / `run_init_sequence`
    /// when the application needs markets, indexes, settings, balances, and
    /// subscriptions initialized.
    pub fn is_authorized(&self) -> bool {
        self.authorized
    }
    /// Returns true after the MoonBot-compatible domain init has completed.
    pub fn is_domain_ready(&self) -> bool {
        self.domain_ready
    }
    /// Current low-level transport authorization state.
    pub fn auth_status(&self) -> AuthStatus {
        self.auth_status
    }
    /// Number of accepted Ping packets processed by this client.
    pub fn ping_count(&self) -> u32 {
        self.ping_count
    }
    /// Total UDP bytes sent by this client session.
    pub fn total_sent(&self) -> u64 {
        self.total_sent.load(Ordering::Relaxed)
    }
    /// Total accepted UDP bytes received by this client session.
    ///
    /// Valid packets selected by the test packet-loss emulator still contribute
    /// to this counter, matching Delphi side effects before `MoonProtoErrEmu`
    /// drops the packet from protocol dispatch.
    pub fn total_recv(&self) -> u64 {
        self.total_recv
    }

    /// Number of outgoing Sliced datagrams still waiting for `SlicedACK`.
    pub fn sliced_in_flight_count(&self) -> usize {
        self.sending.len()
    }

    /// Total Sliced blocks still waiting for `SlicedACK` across all datagrams.
    pub fn sliced_in_flight_blocks(&self) -> usize {
        self.sending.iter().map(|s| s.blocks_count).sum()
    }

    /// Number of H-priority encrypted commands still waiting for regular ACK.
    pub fn pending_high_count(&self) -> usize {
        self.pending_h.len()
    }

    /// EMA % retransmission overhead для Sliced пакетов (matches AvgOverHeat MoonProtoIntStruct.pas:220).
    /// 0 = идеально (no retries). >0 = вынужденные перепосылы.
    pub fn avg_over_heat(&self) -> f64 {
        self.avg_over_heat
    }

    // ====================================================================
    //  Diagnostic getters (audit_responsibility A4)
    //
    //  В Delphi `TMoonProtoNetClient` эти поля публичны и читаются UI
    //  (MoonProtoUnit.pas:363 — "Ping: %d PMTU: %d RS: %d%%"). Aналог в Rust
    //  для построения статус-строки терминала.
    // ====================================================================

    /// RTT в ms (последний измеренный из Ping). Соответствует Delphi
    /// `TMoonProtoNetClient.RoundTripDelay` (MoonProtoClient.pas:62).
    pub fn round_trip_delay_ms(&self) -> i64 {
        self.round_trip_delay
    }

    /// Текущий Path MTU в байтах. Стартует с 508; runtime ProbeMTU может
    /// увеличивать значение выше 8000 шагами по 32 байта.
    /// Соответствует Delphi `TMoonProtoNetClient.PMTU`.
    pub fn actual_pmtu(&self) -> u16 {
        self.actual_pmtu
    }

    /// Receive Status [0.0..1.0] — качество downlink канала. >0.92 = норма,
    /// <0.85 = критично, между = серая зона. Соответствует Delphi
    /// `TMoonProtoNetClient.RS`.
    pub fn rs(&self) -> f64 {
        self.rs
    }

    /// `ServerTime - LocalTime` в днях (как Delphi TDateTime). Применяется
    /// автоматически к timestamp'ам входящих ордеров через `Orders::apply`.
    /// Внешним потребителям обычно не нужен — выставлен публично для диагностики.
    pub fn server_time_delta_days(&self) -> f64 {
        self.server_time_delta
    }

    /// `|ServerTime - LocalTime|` в ms (абсолютный лаг от последнего Ping).
    /// Полезно для UI индикатора "сервер близко / далеко".
    pub fn net_lag_ping_ms(&self) -> i64 {
        self.net_lag_ping
    }

    /// `Orders cycle ms` от сервера — рекомендованный темп опроса ордерных событий.
    /// Соответствует Delphi `TMoonProtoNetClient.GlobalTimingOrders`.
    pub fn global_timing_orders(&self) -> u16 {
        self.global_timing_orders
    }

    /// Текущий `ServerToken` — меняется при каждом hard handshake (Hello→WhoAreYou→Fine).
    /// Soft reconnect (HelloAgain) НЕ меняет этот токен. **Внутри либы используется для
    /// init/API subscription restore** — внешнему потребителю обычно не нужен,
    /// выставлен для diagnostic UI.
    pub fn server_token(&self) -> u64 {
        self.server_token
    }

    /// `PeerAppToken` — генерируется при старте серверного процесса. Меняется при перезапуске
    /// сервера. **Внутри либы используется для проверки свежести markets indexes** — внешнему
    /// потребителю обычно не нужен, выставлен для diagnostic UI / event correlation.
    pub fn peer_app_token(&self) -> u64 {
        self.peer_app_token
    }

    pub(crate) fn market_indexes_current_for_peer(&self) -> bool {
        self.peer_app_token != 0 && self.peer_app_token == self.tracked_indexes_peer_app_token
    }

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
    pub fn bytes_per_sec_sent(&self) -> u64 {
        self.bps_sent.bytes_per_sec()
    }
    /// Байт принято в среднем за последние ~10 секунд (B/s). O(1) EMA.
    pub fn bytes_per_sec_recv(&self) -> u64 {
        self.bps_recv.bytes_per_sec()
    }

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

// =============================================================================
//  Init sequence helper — free function (НЕ метод Client)
//
//  Логически единственный init-проход после `Connected{fresh:true}`:
//  `BaseCheck → AuthCheck → GetMarketsList → GetMarketsIndexes → UpdateMarketsList
//   → GetMarketsBalanceFull → Delphi post-init resync → optional subscriptions`.
//  Аналог Delphi `TCryptoPumpTool.InitInt` (`Unit1.pas:4987-5150`).
//
//  Почему free function, а не `Client::run_init_sequence`:
//   - `Client::run` / `Client::run_with_dispatcher` занимают `&mut Client` на всё
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

/// Configuration for [`run_init_sequence`].
///
/// Delphi-critical init steps are not configurable: BaseCheck, AuthCheck,
/// GetMarketsList, GetMarketsIndexes, UpdateMarketsList, balance refresh, orders,
/// strategy snapshot, and settings sync are the init contract itself. This config
/// only carries optional stream subscriptions and timing.
#[derive(Debug, Clone, Default)]
pub struct InitConfig {
    /// Value for the post-init `TMMOrdersSubscribeCommand`.
    ///
    /// Delphi always sends this UI command after `InitDone` with
    /// `cfg.ShowHeatMap`. `None` uses `subscribe_trades` as a fallback, then
    /// falls back to `false`.
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
    /// Payload size in bytes for the `GetMarketsList` response. The actual
    /// market count is parsed into `EventDispatcher::markets()`.
    pub markets_response_bytes: usize,
    /// Payload size in bytes for the `GetMarketsIndexes` response.
    pub indexes_response_bytes: usize,
    /// Payload size in bytes for the `UpdateMarketsList` response.
    pub update_markets_response_bytes: usize,
    /// Payload size in bytes for the `GetMarketsBalanceFull` response.
    pub balances_response_bytes: usize,
    /// Whether post-init resync commands were enqueued.
    pub post_init_resync_sent: bool,
    /// Whether a strategy snapshot reply was sent or queued from dispatcher
    /// state.
    pub strategy_snapshot_sent: bool,
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
    /// BaseCheck or AuthCheck returned server-side error.
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
            Self::NotAuthenticated => write!(f, "client not authenticated (call run_with_dispatcher until Connected{{fresh:true}} first)"),
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
/// This helper is the recommended one-shot setup path for regular consumers.
/// It hides the transport-ready wait before [`run_init_sequence`], while still
/// using the same `Client::run_with_dispatcher` pump internally. Applications
/// that need a custom phased UI can keep using `run_with_dispatcher` and
/// `run_init_sequence` directly.
pub fn connect_and_init(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    cfg: ConnectConfig,
) -> Result<InitResult, ConnectError> {
    if !client.is_authorized() {
        client.run_with_dispatcher_queued(cfg.connect_timeout, dispatcher);
    }

    if !client.is_authorized() {
        return Err(ConnectError::ConnectTimedOut {
            timeout: cfg.connect_timeout,
        });
    }

    run_init_sequence(client, dispatcher, cfg.init).map_err(ConnectError::from)
}

/// Run the full init sequence: BaseCheck → AuthCheck → GetMarketsList →
/// GetMarketsIndexes → UpdateMarketsList → GetMarketsBalanceFull →
/// Delphi post-init resync → optional subscriptions.
///
/// Until this function completes successfully,
/// `EventDispatcher::dispatch_into_active` drops domain pushes
/// (`Order`/`Strat`/`Balance`/`Trades*`/`OrderBook`/`UI`), matching Delphi
/// `ClientNewData` under `not InitDone`. After a successful bootstrap, the
/// library sends `TAllStatusesReq`, a fresh `TStratSnapshot` through the
/// registered strategy provider, `TSettingsRequest`, `TMMOrdersSubscribeCommand`,
/// and `TRequestBalanceRefresh`.
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
/// Pattern:
/// ```ignore
/// let mut client = Client::new(cfg);
/// let mut dispatcher = EventDispatcher::new();
/// // Phase 1 — handshake.
/// client.run_with_dispatcher(Duration::from_secs(3), &mut dispatcher, Box::new(|_| {}));
/// // Phase 2 — init while the helper pumps the client loop.
/// let r = run_init_sequence(&mut client, &mut dispatcher, InitConfig::default())?;
/// // Phase 3 — long-running stream.
/// client.run_with_dispatcher(Duration::from_secs(3600), &mut dispatcher, Box::new(|ev| {...}));
/// ```
///
/// [`ServerInfo`]: crate::commands::engine_api::ServerInfo
#[derive(Debug, Clone)]
enum CriticalInitStatus {
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
        client.run_with_dispatcher_queued(
            Duration::from_millis(DELPHI_BASE_CHECK_UPDATE_AUTH_WAIT_MS),
            dispatcher,
        );
    }
}

fn run_base_check_delphi(
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
            client.run_with_dispatcher_queued(retry_pause, dispatcher);
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

/// Run the MoonBot-compatible one-time domain initialization sequence.
///
/// Call this after transport authorization, or use [`connect_and_init`] to wait
/// for authorization and init in one helper. A successful run opens the
/// dispatcher domain gate and sends the Delphi post-init refresh set:
/// order snapshot, strategy snapshot reply, settings request, MM-orders
/// subscription flag, balance refresh, and optional stream subscriptions.
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

    // === 1. BaseCheck === критический шаг.
    // При успехе — парсим server identity и сохраняем в Client.server_info
    // (multi-server support: приложение различает серверы через `client.server_info().bot_id`).
    let base_status = run_base_check_delphi(
        client,
        dispatcher,
        &mut result,
        timeout,
        waiting_update,
        Duration::from_millis(DELPHI_BASE_CHECK_UPDATE_RETRY_PAUSE_MS),
    )?;

    // === 2. AuthCheck === критический шаг
    let auth_status = if base_status.is_ok() {
        run_auth_check_once(client, dispatcher, &mut result, timeout)?
    } else {
        CriticalInitStatus::Skipped
    };

    if let Some(err) = base_status.final_error("BaseCheck") {
        return Err(err);
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
    result.update_markets_response_bytes = resp.data.len();

    // === 6. GetMarketsBalanceFull === критический full-balance refresh.
    // Delphi server currently refreshes balances server-side but does not serialize a
    // balance snapshot in the EngineResponse (`WriteBalancesToStream` is TODO).
    let resp = run_required_engine_step(
        client,
        dispatcher,
        &mut result,
        "GetMarketsBalanceFull",
        crate::commands::engine_request::get_markets_balance_full(),
        timeout,
    )?;
    result.balances_response_bytes = resp.data.len();

    client.domain_restore = DomainRestoreIntent {
        fetch_indexes: true,
    };
    client.domain_ready = true;
    send_post_init_resync(client, dispatcher, &cfg, &mut result);

    // === 7. SubscribeAllTrades === optional; идёт через subscription_registry (fire-and-forget).
    // Subscribe events идут в event channel; main loop их применит на следующем тике
    // (либо здесь же если ниже идёт wait, либо в основном run_with_dispatcher после init).
    if let Some(want_mm) = cfg.subscribe_trades {
        client.subscribe_all_trades(want_mm);
        result.trades_subscribed = true;
    }

    // === 8. Subscribe orderbooks === optional; fire-and-forget через registry
    for name in &cfg.subscribe_orderbooks {
        client.subscribe_orderbook(name);
        result.orderbooks_subscribed += 1;
    }

    // === 9. Drain fire-and-forget subscribe events ===
    // subscribe_* пушит ClientEvent::Subscribe* в channel. Без тика main loop
    // events лежат в channel и wire-команды не уходят. Прогоняем короткий тик
    // чтобы обработка подписки реально стартовала к моменту выхода из init.
    if result.post_init_resync_sent
        || cfg.subscribe_trades.is_some()
        || !cfg.subscribe_orderbooks.is_empty()
    {
        client.run_with_dispatcher_queued(Duration::from_millis(100), dispatcher);
    }

    Ok(result)
}

fn send_post_init_resync(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    cfg: &InitConfig,
    result: &mut InitResult,
) {
    client.request_all_statuses(rand::random());
    result.strategy_snapshot_sent =
        dispatcher.send_or_queue_strategy_snapshot_request(rand::random(), client);
    client.ui_settings_request();
    let mm_orders = cfg
        .mm_orders_subscribe
        .or(client.subscription_registry.mm_orders_sub)
        .or(cfg.subscribe_trades)
        .unwrap_or(false);
    client.apply_mm_orders_subscribe_intent(mm_orders);
    client.send_mm_orders_subscribe_cmd(mm_orders);
    client.balance_request_refresh();
    result.post_init_resync_sent = true;
}

/// Drop: сигналим reader thread'у завершиться даже если потребитель не вызвал
/// `disconnect()`. Reader выйдет из loop макс через 1 сек (read_timeout).
/// Process-level NTP guard освобождается автоматически после тела `drop`; если
/// это был последний клиент, общий NTP worker остановится.
impl Drop for Client {
    fn drop(&mut self) {
        self.app_queue_alive.store(false, Ordering::Relaxed);
        self.reader_shutdown.store(true, Ordering::Relaxed);
    }
}

/// O(1) byte-rate counter with about 10 seconds of EMA smoothing.
///
/// This mirrors Delphi `TMoonProtoUDPClient.AddBytesCount` without a heap-backed
/// sliding window.
///
/// Algorithm:
/// - `cur_sec_bytes` accumulates bytes in the current one-second bucket.
/// - Once a second passes, the bucket is folded into the EMA.
/// - `bytes_per_sec()` returns the smoothed bytes-per-second value.
#[derive(Debug, Default)]
pub struct BpsCounter {
    /// Bytes accumulated in the current one-second bucket.
    cur_sec_bytes: u64,
    /// EMA-smoothed value (`10 * average B/s` in steady state).
    ema_10sec: u64,
    /// Timestamp of the current bucket start in milliseconds (`0` means
    /// uninitialized).
    last_sec_ms: i64,
    /// Number of complete seconds accumulated, clamped to 10.
    stat_sec_count: u8,
}

impl BpsCounter {
    /// Create an empty byte-rate counter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add bytes observed at a monotonic millisecond timestamp.
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

    /// Return the average bytes per second over the recent smoothing window.
    ///
    /// During the first 10 seconds, this divides by the actual number of closed
    /// buckets instead of by 10, matching Delphi's ramp-up behavior.
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

#[cfg(test)]
mod api_pending_dispatch_tests {
    use super::*;
    use crate::commands::engine_api::EngineMethod;
    use crate::commands::market::build_markets_indexes_response;
    use crate::events::EventDispatcher;

    fn dummy_cfg() -> ClientConfig {
        ClientConfig {
            server_ip: "127.0.0.1".to_string(),
            server_port: 3000,
            master_key: [0; 16],
            mac_key: [0; 16],
            mask_ver: 0,
            client_id: 0,
            ntp_host: None,
            refresh: RefreshConfig {
                update_markets_every: None,
                check_tags_every: None,
            },
        }
    }

    fn build_engine_response_payload(
        request_uid: u64,
        method: EngineMethod,
        data: &[u8],
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(1u8); // TEngineResponse CmdId
        buf.extend_from_slice(&3u16.to_le_bytes()); // version
        buf.extend_from_slice(&0xAABB_CCDD_u64.to_le_bytes());
        buf.extend_from_slice(&request_uid.to_le_bytes());
        buf.push(method as u8);
        buf.push(1u8); // success
        buf.extend_from_slice(&0i32.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes()); // empty error_msg
        buf.push(0u8); // not compressed
        buf.extend_from_slice(&(data.len() as i32).to_le_bytes());
        buf.extend_from_slice(data);
        buf
    }

    fn drain_base_check_sends(client: &mut Client) -> usize {
        let mut count = 0;
        let (sliced, high, low) = client.take_send_queues_for_test();
        for item in sliced.into_iter().chain(high).chain(low) {
            if item.cmd == Command::API as u8
                && item.data.get(11) == Some(&(EngineMethod::BaseCheck as u8))
            {
                assert_eq!(item.priority, SendPriority::Sliced);
                assert!(item.encrypted);
                assert_eq!(item.max_retries, 6);
                count += 1;
            }
        }
        count
    }

    #[test]
    fn server_update_ui_commands_mark_delphi_base_check_flag() {
        let client = Client::new(dummy_cfg());

        assert!(!client.server_update_sent());
        client.ui_update_version("MoonBot-1", true);
        assert!(client.server_update_sent());
        assert!(client.take_server_update_sent());
        assert!(!client.server_update_sent());

        client.ui_switch_dex("Main");
        assert!(client.server_update_sent());
        assert!(client.take_server_update_sent());

        client.ui_switch_spot(1);
        assert!(client.server_update_sent());
    }

    #[test]
    fn base_check_without_server_update_uses_one_sendandwait_attempt() {
        let mut client = Client::new(dummy_cfg());
        let mut dispatcher = EventDispatcher::new();
        let mut result = InitResult::default();

        let status = run_base_check_delphi(
            &mut client,
            &mut dispatcher,
            &mut result,
            Duration::ZERO,
            false,
            Duration::ZERO,
        )
        .expect("zero-timeout BaseCheck should return a status, not disconnect");

        assert!(matches!(status, CriticalInitStatus::TimedOut));
        assert_eq!(drain_base_check_sends(&mut client), 1);
    }

    #[test]
    fn base_check_after_server_update_uses_delphi_retry_count() {
        let mut client = Client::new(dummy_cfg());
        let mut dispatcher = EventDispatcher::new();
        let mut result = InitResult::default();

        let status = run_base_check_delphi(
            &mut client,
            &mut dispatcher,
            &mut result,
            Duration::ZERO,
            true,
            Duration::ZERO,
        )
        .expect("zero-timeout BaseCheck should return a status, not disconnect");

        assert!(matches!(status, CriticalInitStatus::TimedOut));
        assert_eq!(
            drain_base_check_sends(&mut client),
            1 + DELPHI_BASE_CHECK_UPDATE_RETRIES
        );
    }

    #[test]
    fn pending_api_response_still_reaches_dispatcher_state() {
        let mut client = Client::new(dummy_cfg());
        let request_uid = 0x1122_3344_5566_7788;
        let rx = client.api_pending.register(request_uid);

        let names = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
        let response_data = build_markets_indexes_response(&names);
        let payload = build_engine_response_payload(
            request_uid,
            EngineMethod::GetMarketsIndexes,
            &response_data,
        );

        let mut payloads = Vec::new();
        {
            let mut sink = DispatchSink::Buffer(&mut payloads);
            client.data_read_int(Command::API as u8, &payload, &mut sink);
        }

        let resp = rx.try_recv().expect("pending receiver must get response");
        assert_eq!(resp.request_uid, request_uid);
        assert_eq!(resp.method, EngineMethod::GetMarketsIndexes);

        assert_eq!(
            payloads.len(),
            1,
            "dispatcher buffer must also receive API payload",
        );
        let (cmd, dispatcher_payload) = payloads.pop().unwrap();
        assert_eq!(cmd, Command::API);

        let mut dispatcher = EventDispatcher::new();
        let mut out = Vec::new();
        dispatcher.dispatch_into_active(
            cmd,
            &dispatcher_payload,
            client.now_ms(),
            &mut out,
            &mut client,
        );

        assert!(dispatcher.markets().indexes_synchronized);
        assert_eq!(dispatcher.markets().market_indexes, names);
    }

    #[test]
    fn pending_api_response_is_not_duplicated_to_callback_sink() {
        let mut client = Client::new(dummy_cfg());
        let request_uid = 7;
        let rx = client.api_pending.register(request_uid);
        let payload = build_engine_response_payload(request_uid, EngineMethod::BaseCheck, &[]);

        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_for_cb = calls.clone();
        let mut cb: OnDataFn = Box::new(move |_cmd, _payload| {
            calls_for_cb.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        });
        {
            let mut sink = DispatchSink::Callback(&mut cb);
            client.data_read_int(Command::API as u8, &payload, &mut sink);
        }

        assert!(rx.try_recv().is_ok(), "pending receiver must get response");
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    #[test]
    fn failed_compressed_payload_is_delivered_with_real_cmd_like_delphi() {
        let mut client = Client::new(dummy_cfg());
        let compressed_garbage = vec![4, 0, 1, 0, 0, 0, 0x0F, 0];
        let mut payloads = Vec::new();

        {
            let mut sink = DispatchSink::Buffer(&mut payloads);
            client.data_read_int(
                Command::UI as u8 | COMPRESSED_FLAG,
                &compressed_garbage,
                &mut sink,
            );
        }

        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].0, Command::UI);
        assert_eq!(payloads[0].1, compressed_garbage);
    }

    #[test]
    fn malformed_api_request_async_returns_closed_receiver_without_pending_slot() {
        let client = Client::new(dummy_cfg());

        let rx = client.send_api_request_async(&[2, 3, 0]);

        assert_eq!(client.api_pending.pending_count(), 0);
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
        assert!(client.drain_app_events_for_test().is_empty());
    }

    #[test]
    fn request_candles_data_timeout_removes_pending_slot() {
        let mut client = Client::new(dummy_cfg());
        let mut dispatcher = EventDispatcher::new();

        let err = client
            .request_candles_data(&mut dispatcher, Duration::from_millis(0))
            .expect_err("zero timeout should expire before any chunk arrives");

        assert!(matches!(err, mpsc::RecvTimeoutError::Timeout));
        assert!(client.pending_candles.is_empty());
    }

    #[test]
    fn run_until_response_does_not_overflow_huge_timeout_when_ready() {
        let mut client = Client::new(dummy_cfg());
        let mut dispatcher = EventDispatcher::new();
        let (tx, rx) = mpsc::channel();
        tx.send(123u32).unwrap();

        let value = client
            .run_until_response(&mut dispatcher, &rx, Duration::MAX)
            .expect("ready response should be returned without touching timeout arithmetic");

        assert_eq!(value, 123);
    }

    #[test]
    fn run_until_response_queues_events_seen_while_waiting() {
        let mut client = Client::new(dummy_cfg());
        client.socket = Some(std::net::UdpSocket::bind("127.0.0.1:0").unwrap());
        client.need_connect = false;
        client.authorized = true;
        client.auth_status = AuthStatus::AuthDone;

        let request_uid = 0x55AA;
        let rx = client.api_pending.register(request_uid);
        let payload = build_engine_response_payload(request_uid, EngineMethod::AuthCheck, &[]);
        client
            .event_tx
            .send(ClientEvent::Recv(RecvMsg {
                cmd: Command::API as u8,
                payload,
                recv_bytes: 64,
                timestamp_ms: client.now_ms(),
                handled_in_reader: false,
                err_emu_drop: false,
                reader_dataread_core_done: false,
                epoch: client.current_reader_epoch,
            }))
            .unwrap();

        let mut dispatcher = EventDispatcher::new();
        let resp = client
            .run_until_response(&mut dispatcher, &rx, Duration::from_millis(200))
            .expect("pending response should be delivered while the loop is pumped");

        assert_eq!(resp.request_uid, request_uid);
        assert_eq!(dispatcher.queued_event_count(), 1);

        let queued = dispatcher.take_queued_events();
        assert_eq!(dispatcher.queued_event_count(), 0);
        match &queued[0] {
            crate::events::Event::EngineResponse(event_resp) => {
                assert_eq!(event_resp.request_uid, request_uid);
                assert_eq!(event_resp.method, EngineMethod::AuthCheck);
            }
            other => panic!("expected queued EngineResponse, got {other:?}"),
        }
    }

    #[test]
    fn post_init_resync_enqueues_delphi_commands() {
        let mut client = Client::new(dummy_cfg());
        let mut dispatcher = EventDispatcher::new();
        dispatcher.set_strategy_snapshot_provider(|_| {
            Some(crate::events::StrategySnapshotReply::from_payload(
                7,
                99,
                true,
                vec![0xAA, 0xBB],
            ))
        });
        let cfg = InitConfig {
            mm_orders_subscribe: Some(true),
            ..Default::default()
        };
        let mut result = InitResult::default();

        send_post_init_resync(&mut client, &mut dispatcher, &cfg, &mut result);

        assert!(result.post_init_resync_sent);
        assert!(result.strategy_snapshot_sent);

        let mut seen_order_req = false;
        let mut seen_strat_snapshot = false;
        let mut seen_settings_req = false;
        let mut seen_mm_orders_true = false;
        let mut seen_balance_refresh = false;

        let (sliced, high, low) = client.take_send_queues_for_test();
        for item in sliced.into_iter().chain(high).chain(low) {
            let data = item.data;
            match Command::from_byte(item.cmd) {
                Command::Order if data.first().copied() == Some(9) => {
                    seen_order_req = true;
                }
                Command::Strat if data.first().copied() == Some(2) => {
                    seen_strat_snapshot = true;
                }
                Command::UI if data.first().copied() == Some(2) => {
                    seen_settings_req = true;
                }
                Command::UI
                    if data.first().copied() == Some(5) && data.last().copied() == Some(1) =>
                {
                    seen_mm_orders_true = true;
                }
                Command::Balance if data.first().copied() == Some(5) => {
                    seen_balance_refresh = true;
                }
                _ => {}
            }
        }

        assert!(seen_order_req, "post-init must request TAllStatuses");
        assert!(
            seen_strat_snapshot,
            "post-init must send fresh TStratSnapshot when provider exists"
        );
        assert!(seen_settings_req, "post-init must request settings");
        assert!(
            seen_mm_orders_true,
            "post-init must send TMMOrdersSubscribeCommand"
        );
        assert!(
            seen_balance_refresh,
            "post-init must request balance refresh"
        );
    }

    #[test]
    fn post_init_resync_without_strategy_provider_sends_empty_strategy_snapshot() {
        let mut client = Client::new(dummy_cfg());
        let mut dispatcher = EventDispatcher::new();
        let cfg = InitConfig::default();
        let mut result = InitResult::default();

        send_post_init_resync(&mut client, &mut dispatcher, &cfg, &mut result);

        assert!(result.post_init_resync_sent);
        assert!(result.strategy_snapshot_sent);

        let mut seen_strat_snapshot = false;
        let (sliced, high, low) = client.take_send_queues_for_test();
        for item in sliced.into_iter().chain(high).chain(low) {
            if Command::from_byte(item.cmd) == Command::Strat
                && item.data.first().copied() == Some(2)
            {
                seen_strat_snapshot = true;
            }
        }
        assert!(
            seen_strat_snapshot,
            "without provider, init must still send an empty strategy snapshot"
        );
        assert!(
            dispatcher.queued_events().iter().any(|event| matches!(
                event,
                crate::events::Event::Strat(crate::state::StratEvent::SnapshotRequested { .. })
            )),
            "init still surfaces SnapshotRequested for app/UI awareness"
        );
    }
}

#[cfg(test)]
mod client_sender_tests {
    use super::*;

    fn make_sender() -> (
        ClientSender,
        Arc<Mutex<VecDeque<ClientEvent>>>,
        Arc<Mutex<SendQueues>>,
        Arc<AtomicBool>,
        Arc<AtomicBool>,
    ) {
        let app_events = Arc::new(Mutex::new(VecDeque::new()));
        let send_queues = Arc::new(Mutex::new(SendQueues::default()));
        let app_queue_alive = Arc::new(AtomicBool::new(true));
        let server_update_sent = Arc::new(AtomicBool::new(false));
        (
            ClientSender {
                app_events: Arc::clone(&app_events),
                app_queue_alive: Arc::clone(&app_queue_alive),
                send_queues: Arc::clone(&send_queues),
                server_update_sent: Arc::clone(&server_update_sent),
            },
            app_events,
            send_queues,
            app_queue_alive,
            server_update_sent,
        )
    }

    fn pop_event(q: &Arc<Mutex<VecDeque<ClientEvent>>>) -> ClientEvent {
        q.lock()
            .unwrap()
            .pop_front()
            .expect("event should be queued")
    }

    fn take_send_items(q: &Arc<Mutex<SendQueues>>) -> Vec<SendItem> {
        let mut sliced = Vec::new();
        let mut high = Vec::new();
        let mut low = Vec::new();
        q.lock()
            .unwrap()
            .take_into(&mut sliced, &mut high, &mut low);
        sliced.extend(high);
        sliced.extend(low);
        sliced
    }

    fn command_uid(payload: &[u8]) -> Option<u64> {
        payload
            .get(3..11)
            .and_then(|s| s.try_into().ok())
            .map(u64::from_le_bytes)
    }

    #[test]
    fn subscribe_orderbook_pushes_event_with_correct_fields() {
        let (sender, q, _, _, _) = make_sender();
        sender.subscribe_orderbook("BTCUSDT");
        match pop_event(&q) {
            ClientEvent::SubscribeOrderBook { market_name } => {
                assert_eq!(market_name, "BTCUSDT");
            }
            other => panic!(
                "expected SubscribeOrderBook, got {:?}",
                std::mem::discriminant(&other)
            ),
        }
    }

    #[test]
    fn unsubscribe_orderbook_pushes_event() {
        let (sender, q, _, _, _) = make_sender();
        sender.unsubscribe_orderbook("ETHUSDT");
        match pop_event(&q) {
            ClientEvent::UnsubscribeOrderBook { market_name } => {
                assert_eq!(market_name, "ETHUSDT");
            }
            _ => panic!("expected UnsubscribeOrderBook"),
        }
    }

    #[test]
    fn subscribe_orderbooks_pushes_one_batched_event() {
        let (sender, q, _, _, _) = make_sender();
        sender.subscribe_orderbooks(["BTCUSDT", "ETHUSDT"]);
        match pop_event(&q) {
            ClientEvent::SubscribeOrderBooks { market_names } => {
                assert_eq!(
                    market_names,
                    vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
                );
            }
            _ => panic!("expected SubscribeOrderBooks"),
        }
    }

    #[test]
    fn unsubscribe_orderbooks_pushes_one_batched_event() {
        let (sender, q, _, _, _) = make_sender();
        sender.unsubscribe_orderbooks(["BTCUSDT", "ETHUSDT"]);
        match pop_event(&q) {
            ClientEvent::UnsubscribeOrderBooks { market_names } => {
                assert_eq!(
                    market_names,
                    vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
                );
            }
            _ => panic!("expected UnsubscribeOrderBooks"),
        }
    }

    #[test]
    fn unsubscribe_all_orderbooks_pushes_event() {
        let (sender, q, _, _, _) = make_sender();
        sender.unsubscribe_all_orderbooks();
        assert!(matches!(
            pop_event(&q),
            ClientEvent::UnsubscribeAllOrderBooks
        ));
    }

    #[test]
    fn subscribe_all_trades_carries_want_mm_flag() {
        let (sender, q, _, _, _) = make_sender();
        sender.subscribe_all_trades(true);
        sender.subscribe_all_trades(false);
        match pop_event(&q) {
            ClientEvent::SubscribeAllTrades { want_mm } => assert!(want_mm),
            _ => panic!("expected SubscribeAllTrades(true)"),
        }
        match pop_event(&q) {
            ClientEvent::SubscribeAllTrades { want_mm } => assert!(!want_mm),
            _ => panic!("expected SubscribeAllTrades(false)"),
        }
    }

    #[test]
    fn unsubscribe_all_trades_pushes_event() {
        let (sender, q, _, _, _) = make_sender();
        sender.unsubscribe_all_trades();
        assert!(matches!(pop_event(&q), ClientEvent::UnsubscribeAllTrades));
    }

    #[test]
    fn try_subscribe_returns_ok() {
        let (sender, _, _, _, _) = make_sender();
        assert!(sender.try_subscribe_orderbook("BTC").is_ok());
        assert!(sender.try_subscribe_orderbooks(["BTC", "ETH"]).is_ok());
        assert!(sender.try_subscribe_all_trades(true).is_ok());
    }

    #[test]
    fn try_subscribe_has_no_capacity_cap() {
        let (sender, _, _, _, _) = make_sender();
        for i in 0..4096 {
            assert!(
                sender.try_subscribe_orderbook(&format!("M{i}")).is_ok(),
                "unbounded event queue must not fail on local capacity"
            );
        }
    }

    #[test]
    fn try_subscribe_returns_disconnected_when_receiver_dropped() {
        let (sender, _, _, alive, _) = make_sender();
        alive.store(false, Ordering::Relaxed);
        let err = sender.try_unsubscribe_all_trades().unwrap_err();
        assert_eq!(err, SubscribeError::Disconnected);
    }

    #[test]
    fn sender_try_send_cmd_keyed_queues_send_item() {
        let (sender, q, send_q, _, _) = make_sender();
        let payload = vec![1, 2, 3, 4];
        let key = UniqueKey::order_move(42);

        sender
            .try_send_cmd_keyed(
                payload.clone(),
                Command::Order,
                SendPriority::High,
                true,
                3,
                key,
            )
            .expect("send command should enqueue");

        assert!(q.lock().unwrap().is_empty());
        let sent = take_send_items(&send_q);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].data, payload);
        assert_eq!(sent[0].cmd, Command::Order as u8);
        assert_eq!(sent[0].priority, SendPriority::High);
        assert!(sent[0].encrypted);
        assert_eq!(sent[0].max_retries, 3);
        assert_eq!(sent[0].retry_left, 2);
        assert_eq!(sent[0].u_key, key);
    }

    #[test]
    fn sender_try_send_api_request_uses_sliced_api_defaults() {
        let (sender, q, send_q, _, _) = make_sender();
        let payload = crate::commands::engine_request::base_check();

        sender
            .try_send_api_request(payload.clone())
            .expect("api request should enqueue");

        assert!(q.lock().unwrap().is_empty());
        let sent = take_send_items(&send_q);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].data, payload);
        assert_eq!(sent[0].cmd, Command::API as u8);
        assert_eq!(sent[0].priority, SendPriority::Sliced);
        assert!(sent[0].encrypted);
        assert_eq!(sent[0].max_retries, 6);
        assert_eq!(sent[0].retry_left, 5);
        assert_eq!(sent[0].u_key, UniqueKey::none());
    }

    #[test]
    fn cloned_sender_pushes_into_same_channel() {
        // Это база для thread-safe API: получили sender, клонировали, оба пушат в
        // один и тот же channel который слушает main loop.
        let (sender_a, q, _, _, _) = make_sender();
        let sender_b = sender_a.clone();
        sender_a.subscribe_orderbook("A");
        sender_b.subscribe_orderbook("B");
        let evs: Vec<_> = q.lock().unwrap().drain(..).collect();
        assert_eq!(evs.len(), 2);
        // FIFO: первое событие — от sender_a.
        match &evs[0] {
            ClientEvent::SubscribeOrderBook { market_name, .. } => assert_eq!(market_name, "A"),
            _ => panic!("expected first SubscribeOrderBook(A)"),
        }
        match &evs[1] {
            ClientEvent::SubscribeOrderBook { market_name, .. } => assert_eq!(market_name, "B"),
            _ => panic!("expected second SubscribeOrderBook(B)"),
        }
    }

    #[test]
    fn sender_replace_order_uses_client_wrapper_wire_defaults() {
        let (sender, q, send_q, _, _) = make_sender();
        let ctx = crate::commands::trade::TradeCtx::with_route(42, 17, 9);

        sender.replace_order(
            ctx,
            "BTCUSDT",
            crate::commands::trade::OrderType::Sell,
            50100.0,
        );

        assert!(q.lock().unwrap().is_empty());
        let sent = take_send_items(&send_q);
        assert_eq!(sent.len(), 1);
        let item = &sent[0];
        assert_eq!(item.cmd, Command::Order as u8);
        assert_eq!(item.priority, SendPriority::High);
        assert!(item.encrypted);
        assert_eq!(item.max_retries, 3);
        assert_eq!(item.retry_left, 2);
        assert_eq!(item.u_key, UniqueKey::order_move(ctx.uid));

        match crate::commands::trade::TradeCommand::parse(&item.data)
            .expect("valid replace command")
        {
            crate::commands::trade::TradeCommand::OrderReplace(cmd) => {
                assert_eq!(cmd.epoch_header.market.base.uid, 42);
                assert_eq!(cmd.epoch_header.market.currency, 17);
                assert_eq!(cmd.epoch_header.market.platform, 9);
                assert_eq!(cmd.epoch_header.market.market_name, "BTCUSDT");
            }
            other => panic!("unexpected trade command: {other:?}"),
        }
    }

    #[test]
    fn sender_ui_switches_mark_server_update_sent_and_keep_delphi_u_key_uid() {
        let (sender, q, send_q, _, server_update_sent) = make_sender();

        sender.ui_switch_dex("MainDex");
        sender.ui_switch_spot(1);

        assert!(server_update_sent.load(Ordering::Relaxed));

        assert!(q.lock().unwrap().is_empty());
        let sent = take_send_items(&send_q);
        assert_eq!(sent.len(), 2);

        let dex_uid = command_uid(&sent[0].data).expect("dex wire UID");
        assert_eq!(sent[0].cmd, Command::UI as u8);
        assert_eq!(sent[0].priority, SendPriority::High);
        assert_eq!(sent[0].u_key, UniqueKey::dex_switch_for(dex_uid));

        let spot_uid = command_uid(&sent[1].data).expect("spot wire UID");
        assert_eq!(sent[1].cmd, Command::UI as u8);
        assert_eq!(sent[1].priority, SendPriority::High);
        assert_eq!(sent[1].u_key, UniqueKey::spot_switch_for(spot_uid));
    }

    #[test]
    fn sender_strat_snapshot_payload_uses_sliced_snapshot_u_key() {
        let (sender, q, send_q, _, _) = make_sender();

        sender.strat_send_snapshot_payload(1, 2, true, &[1, 2, 3]);

        assert!(q.lock().unwrap().is_empty());
        let sent = take_send_items(&send_q);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].cmd, Command::Strat as u8);
        assert_eq!(sent[0].priority, SendPriority::Sliced);
        assert!(sent[0].encrypted);
        assert_eq!(sent[0].max_retries, 6);
        assert_eq!(sent[0].retry_left, 5);
        assert_eq!(sent[0].u_key, UniqueKey::strat_snapshot());
    }

    #[test]
    fn sender_balance_request_refresh_uses_balance_channel_defaults() {
        let (sender, q, send_q, _, _) = make_sender();

        sender.balance_request_refresh();

        assert!(q.lock().unwrap().is_empty());
        let sent = take_send_items(&send_q);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].cmd, Command::Balance as u8);
        assert_eq!(sent[0].priority, SendPriority::High);
        assert!(sent[0].encrypted);
        assert_eq!(sent[0].max_retries, 3);
        assert_eq!(sent[0].retry_left, 2);
        assert_eq!(sent[0].data.first().copied(), Some(5));
    }

    #[test]
    fn subscribe_error_displays_with_message() {
        // Просто проверка что Display impl работает (полезно для логирования).
        assert_eq!(
            format!("{}", SubscribeError::ChannelFull),
            "Client queues have no capacity cap"
        );
        assert_eq!(
            format!("{}", SubscribeError::Disconnected),
            "Client queues disconnected"
        );
    }
}

#[cfg(test)]
mod client_subscribe_integration_tests {
    use super::*;
    use crate::commands::engine_api::EngineMethod;

    fn dummy_cfg() -> ClientConfig {
        ClientConfig {
            server_ip: "127.0.0.1".to_string(),
            server_port: 3000,
            master_key: [0; 16],
            mac_key: [0; 16],
            mask_ver: 0,
            client_id: 0,
            ntp_host: None,
            refresh: RefreshConfig {
                update_markets_every: None,
                check_tags_every: None,
            },
        }
    }

    fn command_uid(payload: &[u8]) -> Option<u64> {
        payload
            .get(3..11)
            .and_then(|s| s.try_into().ok())
            .map(u64::from_le_bytes)
    }

    fn method_id(payload: &[u8]) -> Option<u8> {
        payload.get(11).copied()
    }

    fn empty_market_names_count(payload: &[u8]) -> Option<i32> {
        let bytes: [u8; 4] = payload.get(14..18)?.try_into().ok()?;
        Some(i32::from_le_bytes(bytes))
    }

    fn drain_api_requests(client: &Client) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let (sliced, high, low) = client.take_send_queues_for_test();
        for item in sliced.into_iter().chain(high).chain(low) {
            if item.cmd == Command::API as u8 {
                out.push(item.data);
            }
        }
        out
    }

    fn tracked_order(
        uid: u64,
        currency: u8,
        platform: u8,
        status: crate::commands::trade::OrderWorkerStatus,
    ) -> crate::state::Order {
        use crate::commands::trade::{
            BaseCommandHeader, MarketCommandHeader, OrderCompact, OrderStatus, StopSettings,
            TradeCommand, TradeEpochHeader,
        };

        let mut orders = crate::state::Orders::new();
        let status_cmd = OrderStatus {
            epoch_header: TradeEpochHeader {
                market: MarketCommandHeader {
                    base: BaseCommandHeader {
                        cmd_id: 4,
                        ver: 3,
                        uid,
                    },
                    currency,
                    platform,
                    market_name: "DOGEUSDT".to_string(),
                },
                epoch: 11,
                status,
            },
            buy_order: OrderCompact::default(),
            sell_order: OrderCompact::default(),
            stops: StopSettings::default(),
            strat_id: 0,
            is_short: false,
            db_id: 0,
            from_cache: false,
            emulator_mode: false,
            immune_for_clicks: false,
        };
        let _ = orders.apply(TradeCommand::OrderStatus(Box::new(status_cmd)));
        orders.get(uid).expect("order should be applied").clone()
    }

    #[test]
    fn client_subscribe_orderbook_pushes_event_through_sender() {
        // Convenience-метод `Client::subscribe_orderbook(&self, ...)` должен пушить
        // событие в app→main queue. До запуска run_with_dispatcher event лежит
        // в очереди и будет обработан первым сетевым тиком.
        let client = Client::new(dummy_cfg());
        client.subscribe_orderbook("BTCUSDT");
        let ev = client
            .drain_app_events_for_test()
            .pop()
            .expect("event should be queued");
        match ev {
            ClientEvent::SubscribeOrderBook { market_name } => {
                assert_eq!(market_name, "BTCUSDT");
            }
            _ => panic!("expected SubscribeOrderBook"),
        }
    }

    #[test]
    fn client_sender_can_be_held_independently_of_client() {
        // Sender держит clone; даже если client держится по `&` ссылке — sender
        // независим. Это база для multi-thread субскрайба.
        let client = Client::new(dummy_cfg());
        let sender = client.sender();
        sender.subscribe_all_trades(true);
        let ev = client
            .drain_app_events_for_test()
            .pop()
            .expect("event queued via sender");
        assert!(matches!(
            ev,
            ClientEvent::SubscribeAllTrades { want_mm: true }
        ));
    }

    #[test]
    fn cancel_tracked_order_uses_order_state_context() {
        use crate::commands::trade::{OrderWorkerStatus, TradeCommand};

        let uid = 0x1122_3344_5566_7788;
        let order = tracked_order(uid, 17, 9, OrderWorkerStatus::SellSet);
        let client = Client::new(dummy_cfg());

        client.cancel_tracked_order(&order);

        let (_, high, _) = client.take_send_queues_for_test();
        assert_eq!(high.len(), 1);
        let item = &high[0];
        assert_eq!(item.cmd, Command::Order as u8);
        assert_eq!(item.priority, SendPriority::High);
        assert_eq!(item.max_retries, 3);
        assert_eq!(item.u_key, UniqueKey::order_move(uid));

        match TradeCommand::parse(&item.data).expect("valid cancel command") {
            TradeCommand::OrderCancel(cmd) => {
                assert_eq!(cmd.epoch_header.market.base.uid, uid);
                assert_eq!(cmd.epoch_header.market.currency, 17);
                assert_eq!(cmd.epoch_header.market.platform, 9);
                assert_eq!(cmd.epoch_header.market.market_name, "DOGEUSDT");
                assert_eq!(cmd.epoch_header.epoch, 0);
                assert_eq!(cmd.epoch_header.status, OrderWorkerStatus::SellSet);
            }
            other => panic!("unexpected trade command: {other:?}"),
        }
    }

    #[test]
    fn client_ui_mm_subscribe_updates_registry_and_pushes_keyed_send() {
        let mut client = Client::new(dummy_cfg());
        client.ui_mm_subscribe(true);
        let ev = client
            .drain_app_events_for_test()
            .pop()
            .expect("mm subscribe intent queued");
        let mut mode = RunMode::Callback {
            on_data: Box::new(|_, _| {}),
        };
        let mut sliced = Vec::new();
        let mut h_items = Vec::new();
        let mut l_items = Vec::new();
        let mut control_events = Vec::new();
        client.handle_main_event(
            ev,
            0,
            &mut mode,
            &mut sliced,
            &mut h_items,
            &mut l_items,
            &mut control_events,
        );
        for ev in control_events.drain(..) {
            client.apply_control_event(ev);
        }

        assert_eq!(client.subscription_registry.mm_orders_sub, Some(true));
        let (_, high, _) = client.take_send_queues_for_test();
        assert_eq!(high.len(), 1);
        let item = &high[0];
        assert_eq!(
            Client::outgoing_mm_orders_subscribe_intent(item),
            Some(true)
        );
        let uid = command_uid(&item.data).expect("wire command UID");
        assert_eq!(item.u_key, UniqueKey::turn_mm_detection_for(uid));
    }

    #[test]
    fn ui_switches_use_delphi_command_uid_in_u_key() {
        let client = Client::new(dummy_cfg());

        client.ui_switch_dex("MainDex");
        client.ui_switch_spot(1);

        let (mut sent, mut high, mut low) = client.take_send_queues_for_test();
        sent.append(&mut high);
        sent.append(&mut low);
        assert_eq!(sent.len(), 2);

        let dex_uid = command_uid(&sent[0].data).expect("dex wire UID");
        assert_eq!(sent[0].cmd, Command::UI as u8);
        assert_eq!(sent[0].u_key, UniqueKey::dex_switch_for(dex_uid));

        let spot_uid = command_uid(&sent[1].data).expect("spot wire UID");
        assert_eq!(sent[1].cmd, Command::UI as u8);
        assert_eq!(sent[1].u_key, UniqueKey::spot_switch_for(spot_uid));
    }

    #[test]
    fn ui_single_slot_commands_use_delphi_fixed_u_key_uid() {
        let client = Client::new(dummy_cfg());

        let settings = crate::commands::ui::ClientSettingsCommand::default();
        client.ui_send_settings(&settings);

        let lev = crate::commands::ui::LevManage {
            uid: 0,
            cmd_ver: 1,
            auto_max_order: false,
            auto_lev_up: false,
            auto_isolated: false,
            auto_cross: false,
            auto_fix_lev: false,
            fix_lev: 0,
            tlg_report: false,
            lev_control: String::new(),
        };
        client.ui_lev_manage(&lev);

        let (mut sent, mut high, mut low) = client.take_send_queues_for_test();
        sent.append(&mut high);
        sent.append(&mut low);
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0].u_key, UniqueKey::base_ui_settings_slot());
        assert_eq!(sent[1].u_key, UniqueKey::lev_manage_settings_slot());
    }

    #[test]
    fn apply_subscribe_event_inserts_into_registry() {
        // apply_subscribe_event — точка где main loop принимает решение
        // обновить registry. Без живого сервера wire-команда уходит в socket=None
        // ветку (log warn + skip), но регистрация в registry происходит.
        let mut client = Client::new(dummy_cfg());
        client.apply_subscribe_event(ClientEvent::SubscribeOrderBook {
            market_name: "BTC".to_string(),
        });
        assert!(client.subscription_registry.orderbook_subs.contains("BTC"));
    }

    #[test]
    fn apply_subscribe_event_inserts_batched_orderbooks_into_registry() {
        let mut client = Client::new(dummy_cfg());
        client.apply_subscribe_event(ClientEvent::SubscribeOrderBooks {
            market_names: vec!["BTC".to_string(), "ETH".to_string(), "BTC".to_string()],
        });
        assert_eq!(client.subscription_registry.orderbook_subs.len(), 2);
        assert!(client.subscription_registry.orderbook_subs.contains("BTC"));
        assert!(client.subscription_registry.orderbook_subs.contains("ETH"));
    }

    #[test]
    fn apply_subscribe_event_unsubscribe_removes_from_registry() {
        let mut client = Client::new(dummy_cfg());
        client.apply_subscribe_event(ClientEvent::SubscribeOrderBook {
            market_name: "BTC".to_string(),
        });
        client.apply_subscribe_event(ClientEvent::UnsubscribeOrderBook {
            market_name: "BTC".to_string(),
        });
        assert!(!client.subscription_registry.orderbook_subs.contains("BTC"));
    }

    #[test]
    fn apply_subscribe_event_batched_unsubscribe_removes_from_registry() {
        let mut client = Client::new(dummy_cfg());
        client.apply_subscribe_event(ClientEvent::SubscribeOrderBooks {
            market_names: vec!["BTC".to_string(), "ETH".to_string(), "XRP".to_string()],
        });
        client.apply_subscribe_event(ClientEvent::UnsubscribeOrderBooks {
            market_names: vec!["ETH".to_string(), "DOGE".to_string()],
        });
        assert!(client.subscription_registry.orderbook_subs.contains("BTC"));
        assert!(!client.subscription_registry.orderbook_subs.contains("ETH"));
        assert!(client.subscription_registry.orderbook_subs.contains("XRP"));
    }

    #[test]
    fn apply_subscribe_event_unsubscribe_all_orderbooks_clears_registry() {
        let mut client = Client::new(dummy_cfg());
        client.apply_subscribe_event(ClientEvent::SubscribeOrderBooks {
            market_names: vec!["BTC".to_string(), "ETH".to_string()],
        });
        let _ = drain_api_requests(&client);
        client.apply_subscribe_event(ClientEvent::UnsubscribeAllOrderBooks);
        assert!(client.subscription_registry.orderbook_subs.is_empty());
        let sent = drain_api_requests(&client);
        assert_eq!(sent.len(), 1);
        assert_eq!(
            method_id(&sent[0]),
            Some(EngineMethod::UnsubscribeOrderBook as u8)
        );
        assert_eq!(empty_market_names_count(&sent[0]), Some(0));
    }

    #[test]
    fn apply_subscribe_event_is_idempotent() {
        // Двойной subscribe для одной пары не должен иметь побочных эффектов
        // в registry (HashSet dedup) и не должен слать второй wire-запрос (но это
        // мы не можем проверить здесь — socket=None, проверяем только registry).
        let mut client = Client::new(dummy_cfg());
        let ev = || ClientEvent::SubscribeOrderBook {
            market_name: "ETH".to_string(),
        };
        client.apply_subscribe_event(ev());
        client.apply_subscribe_event(ev());
        assert_eq!(client.subscription_registry.orderbook_subs.len(), 1);
    }

    #[test]
    fn apply_subscribe_all_trades_sets_registry() {
        let mut client = Client::new(dummy_cfg());
        client.apply_subscribe_event(ClientEvent::SubscribeAllTrades { want_mm: true });
        assert_eq!(
            client.subscription_registry.trades_sub,
            Some(TradesSubscription { want_mm: true }),
        );
        assert_eq!(client.subscription_registry.mm_orders_sub, Some(true));
        // Повторный с другим want_mm — обновляет registry.
        client.apply_subscribe_event(ClientEvent::SubscribeAllTrades { want_mm: false });
        assert_eq!(
            client.subscription_registry.trades_sub,
            Some(TradesSubscription { want_mm: false }),
        );
        assert_eq!(client.subscription_registry.mm_orders_sub, Some(false));
    }

    #[test]
    fn apply_unsubscribe_all_trades_clears_registry() {
        let mut client = Client::new(dummy_cfg());
        client.apply_subscribe_event(ClientEvent::SubscribeAllTrades { want_mm: true });
        client.apply_subscribe_event(ClientEvent::UnsubscribeAllTrades);
        assert!(client.subscription_registry.trades_sub.is_none());
        assert_eq!(
            client.subscription_registry.mm_orders_sub,
            Some(true),
            "Delphi UnsubscribeAllTrades clears IsTradesSubscribed but not the MM flag",
        );
    }

    #[test]
    fn apply_mm_orders_subscribe_updates_registry_and_active_trades_flag() {
        let mut client = Client::new(dummy_cfg());
        client.apply_subscribe_event(ClientEvent::SubscribeAllTrades { want_mm: false });
        let _ = client.take_send_queues_for_test(); // drain SubscribeAllTrades send command

        client.apply_mm_orders_subscribe_intent(true);

        assert_eq!(client.subscription_registry.mm_orders_sub, Some(true));
        assert_eq!(
            client.subscription_registry.trades_sub,
            Some(TradesSubscription { want_mm: true }),
        );
    }
}

#[cfg(test)]
mod pmtu_tests {
    use super::*;

    fn dummy_cfg() -> ClientConfig {
        ClientConfig {
            server_ip: "127.0.0.1".to_string(),
            server_port: 3000,
            master_key: [0; 16],
            mac_key: [0; 16],
            mask_ver: 0,
            client_id: 0,
            ntp_host: None,
            refresh: RefreshConfig {
                update_markets_every: None,
                check_tags_every: None,
            },
        }
    }

    fn unpack_client_packet(mac_key: &MoonKey, raw: &[u8]) -> (u8, Vec<u8>) {
        const CLIENT_HDR_SIZE: usize = 15;
        let mut buf = raw.to_vec();
        moonproto_transport::outer_light_crypt(&mut buf, mac_key);
        let hdr = moonproto_transport::ClientMsgHeader::from_bytes(&buf).unwrap();
        let saved = [buf[1], buf[2], buf[3], buf[4]];
        buf[1..5].copy_from_slice(&0u32.to_le_bytes());
        let mac = moonproto_transport::MacContext::new(mac_key).mac(&buf);
        assert_eq!(mac, hdr.checksum);
        buf[1..5].copy_from_slice(&saved);
        (hdr.cmd, buf[CLIENT_HDR_SIZE..].to_vec())
    }

    fn ping_payload_with_pmtu(pmtu: u16) -> Vec<u8> {
        let mut payload = vec![0u8; 50];
        payload[20..22].copy_from_slice(&pmtu.to_le_bytes());
        payload[41] = 255; // RSQ
        payload
    }

    fn ping_payload_with_ack(ack_start: u64, ack_words: &[u64]) -> Vec<u8> {
        let mut payload = ping_payload_with_pmtu(508);
        payload[42..50].copy_from_slice(&ack_start.to_le_bytes());
        for word in ack_words {
            payload.extend_from_slice(&word.to_le_bytes());
        }
        payload
    }

    fn pending_h_item(msg_num: u64) -> SendItem {
        SendItem {
            data: vec![0x11],
            cmd: Command::UI as u8,
            encrypted: true,
            priority: SendPriority::High,
            retry_left: 1,
            max_retries: 3,
            msg_num,
            last_sent_at: 0,
            u_key: UniqueKey::none(),
        }
    }

    fn sent_sliced_with_lengths(lengths: &[usize], last_checked: i64) -> SentSliced {
        SentSliced {
            datagram_num: 1,
            slices: lengths.iter().map(|len| vec![0xA5; *len]).collect(),
            piece_last_checked: vec![last_checked; lengths.len()],
            ack_flags: [0; 32],
            blocks_count: lengths.len(),
            sent_count: lengths.len(),
            last_checked,
            retry_count: 0,
            max_retry_count: 6,
            u_key: UniqueKey::none(),
        }
    }

    #[test]
    fn ping_pmtu_above_8192_is_preserved() {
        let mut client = Client::new(dummy_cfg());
        let mut delivered = Vec::new();
        let mut sink = DispatchSink::Buffer(&mut delivered);

        client.handle_ping(&ping_payload_with_pmtu(8_224), &mut sink);

        assert_eq!(client.actual_pmtu(), 8_224);
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].0, Command::Ping);
    }

    #[test]
    fn ping_server_time_delta_uses_raw_now_not_ntp_corrected_now() {
        let mut client = Client::new(dummy_cfg());
        let mut delivered = Vec::new();
        let mut sink = DispatchSink::Buffer(&mut delivered);
        let raw_now: f64 = 45_000.0;
        let corrected_now: f64 = raw_now + 3600.0 / 86400.0;
        let initial_time: f64 = raw_now + 2.0 / 86400.0;
        let server_time: f64 = corrected_now + 3.0 / 86400.0;
        let mut payload = ping_payload_with_pmtu(508);
        payload[0..8].copy_from_slice(&server_time.to_le_bytes());
        payload[8..16].copy_from_slice(&initial_time.to_le_bytes());

        client.handle_ping_at(&payload, &mut sink, raw_now, corrected_now, false);

        assert!(
            ((client.server_time_delta_days() * 86400.0) - 2.0).abs() < 0.001,
            "Delphi ClientNewData uses raw Now for ServerTimeDelta, not NTP-corrected SendPing time"
        );
        assert_eq!(client.net_lag_ping_ms(), 3000);
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].0, Command::Ping);
    }

    #[test]
    fn tiny_ping_pmtu_does_not_underflow_sliced_send() {
        let mut client = Client::new(dummy_cfg());
        let mut delivered = Vec::new();
        let mut sink = DispatchSink::Buffer(&mut delivered);
        client.handle_ping(&ping_payload_with_pmtu(18), &mut sink);
        assert_eq!(client.actual_pmtu(), 18);

        let item = SendItem {
            data: vec![1],
            cmd: Command::UI as u8,
            encrypted: false,
            priority: SendPriority::Sliced,
            retry_left: 0,
            max_retries: 0,
            msg_num: 0,
            last_sent_at: 0,
            u_key: UniqueKey::none(),
        };

        client.create_sliced_and_send(&item);
        assert!(client.sending.is_empty());
    }

    #[test]
    fn ping_ack_does_not_drop_pending_h_until_writer_copy_apply() {
        let mut client = Client::new(dummy_cfg());
        client.pending_h.push(pending_h_item(42));
        let mut delivered = Vec::new();
        let mut sink = DispatchSink::Buffer(&mut delivered);

        // AckStart=40, bit 2 set => MsgNum 42 is ACKed by the server.
        client.handle_ping(&ping_payload_with_ack(40, &[1 << 2]), &mut sink);

        assert_eq!(
            client.pending_h.len(),
            1,
            "Delphi DataReadInt(MPC_Ping) writes TmpSlider only; PendingH is writer work"
        );
        assert!(
            client
                .reader_protocol
                .lock()
                .unwrap()
                .tmp_slider
                .has_new_data
        );
        assert!(!client.recvd_slider.has_new_data);

        client.copy_recvd_data();
        assert!(
            !client
                .reader_protocol
                .lock()
                .unwrap()
                .tmp_slider
                .has_new_data
        );
        assert!(client.recvd_slider.has_new_data);

        client.apply_regular_hl_ack();
        assert!(
            client.pending_h.is_empty(),
            "CheckSeningData/ApplyRegularHLAck must drop ACKed High packet"
        );
    }

    #[test]
    fn ping_ack_reader_core_is_not_reapplied_by_main_ping_branch() {
        let mut client = Client::new(dummy_cfg());
        let payload = ping_payload_with_ack(40, &[1 << 2]);
        client.apply_ping_ack_bitmap(&payload);
        client.copy_recvd_data();
        assert!(
            !client
                .reader_protocol
                .lock()
                .unwrap()
                .tmp_slider
                .has_new_data
        );
        assert!(client.recvd_slider.has_new_data);

        let mut delivered = Vec::new();
        let mut sink = DispatchSink::Buffer(&mut delivered);
        client.handle_ping_at(&payload, &mut sink, 10.0, 10.0, true);

        assert!(
            !client
                .reader_protocol
                .lock()
                .unwrap()
                .tmp_slider
                .has_new_data,
            "main Ping branch must not write TmpSlider again after reader DataReadInt core"
        );
        assert_eq!(delivered.len(), 1);
    }

    #[test]
    fn sliced_u_key_cleanup_does_not_drop_pending_h_like_delphi() {
        let mut client = Client::new(dummy_cfg());
        let key = UniqueKey::order_move(42);

        let mut old_sliced = sent_sliced_with_lengths(&[8], 0);
        old_sliced.u_key = key;
        client.sending.push(old_sliced);
        let mut second_old_sliced = sent_sliced_with_lengths(&[8], 0);
        second_old_sliced.u_key = key;
        client.sending.push(second_old_sliced);

        let mut pending_h = pending_h_item(10);
        pending_h.u_key = key;
        client.pending_h.push(pending_h);
        let mut second_pending_h = pending_h_item(11);
        second_pending_h.u_key = key;
        client.pending_h.push(second_pending_h);

        let new_sliced = SendItem {
            data: vec![0x22],
            cmd: Command::UI as u8,
            encrypted: false,
            priority: SendPriority::Sliced,
            retry_left: 0,
            max_retries: 6,
            msg_num: 0,
            last_sent_at: 0,
            u_key: key,
        };

        client.apply_sliced_send_u_key_cleanup(&[new_sliced]);

        assert_eq!(
            client.sending.len(),
            1,
            "Delphi DeleteSendingByKey removes only the first matching Sliced entry"
        );
        assert_eq!(
            client.pending_h.len(),
            2,
            "Delphi DeleteSendingByKey must not remove PendingH entries"
        );

        let new_high = SendItem {
            data: vec![0x33],
            cmd: Command::UI as u8,
            encrypted: true,
            priority: SendPriority::High,
            retry_left: 1,
            max_retries: 3,
            msg_num: 0,
            last_sent_at: 0,
            u_key: key,
        };

        client.apply_high_send_u_key_cleanup(&[new_high]);

        assert_eq!(
            client.pending_h.len(),
            1,
            "Delphi DeletePendingByKey removes only the first matching PendingH entry"
        );
    }

    #[test]
    fn high_u_key_cleanup_runs_after_regular_ack_like_delphi() {
        let mut client = Client::new(dummy_cfg());
        let key = UniqueKey::order_move(42);

        let mut acked_same_key = pending_h_item(42);
        acked_same_key.u_key = key;
        client.pending_h.push(acked_same_key);
        let mut not_acked_same_key = pending_h_item(43);
        not_acked_same_key.u_key = key;
        client.pending_h.push(not_acked_same_key);

        client.recvd_slider.start_num = 40;
        client.recvd_slider.bit_field[0] = 1 << 2;
        client.recvd_slider.has_new_data = true;
        client.recvd_slider.r_count = 1;

        let new_high = SendItem {
            data: vec![0x33],
            cmd: Command::UI as u8,
            encrypted: true,
            priority: SendPriority::High,
            retry_left: 1,
            max_retries: 3,
            msg_num: 0,
            last_sent_at: 0,
            u_key: key,
        };

        client.apply_regular_hl_ack();
        assert_eq!(
            client.pending_h.len(),
            1,
            "Delphi ApplyRegularHLAck runs before CopySendListH DeletePendingByKey"
        );
        client.apply_high_send_u_key_cleanup(&[new_high]);
        assert!(
            client.pending_h.is_empty(),
            "then Delphi DeletePendingByKey removes the first remaining same-key High entry"
        );
    }

    #[test]
    fn create_sliced_object_queues_without_immediate_send_like_delphi() {
        let mut client = Client::new(dummy_cfg());
        let item = SendItem {
            data: vec![0x11, 0x22, 0x33],
            cmd: Command::UI as u8,
            encrypted: false,
            priority: SendPriority::Sliced,
            retry_left: 0,
            max_retries: 5,
            msg_num: 0,
            last_sent_at: 0,
            u_key: UniqueKey::none(),
        };

        client.create_sliced_and_send(&item);

        assert_eq!(client.sending.len(), 1);
        assert_eq!(client.sending[0].sent_count, 0);
        assert_eq!(client.sending[0].last_checked, 0);
        assert!(client.sending[0]
            .piece_last_checked
            .iter()
            .all(|&last_checked| last_checked == 0));
    }

    #[test]
    fn sliced_size_check_uses_compressed_size_like_delphi() {
        let mut client = Client::new(dummy_cfg());
        let item = SendItem {
            data: (0..130_000).map(|i| (i % 4) as u8).collect(),
            cmd: Command::UI as u8,
            encrypted: false,
            priority: SendPriority::Sliced,
            retry_left: 0,
            max_retries: 5,
            msg_num: 0,
            last_sent_at: 0,
            u_key: UniqueKey::none(),
        };

        client.create_sliced_and_send(&item);

        assert_eq!(
            client.sending.len(),
            1,
            "Delphi compresses TMoonProtoDataToSend before CreateSlicedObject size check"
        );
        assert_eq!(
            client.sending[0].slices[0][4],
            Command::UI as u8 | COMPRESSED_FLAG
        );
    }

    #[test]
    fn encrypted_empty_sliced_is_dropped_before_crypt_like_delphi() {
        let mut client = Client::new(dummy_cfg());
        client.encode_cipher = Some(crypto::cipher_from_key(&[0; 16]));
        let item = SendItem {
            data: Vec::new(),
            cmd: Command::UI as u8,
            encrypted: true,
            priority: SendPriority::Sliced,
            retry_left: 1,
            max_retries: 5,
            msg_num: 0,
            last_sent_at: 0,
            u_key: UniqueKey::none(),
        };

        client.create_sliced_and_send(&item);

        assert!(
            client.sending.is_empty(),
            "Delphi CreateSlicedObject drops empty data.ms before Crypt(data)"
        );
    }

    #[test]
    fn encrypted_low_batch_size_uses_wire_size_after_crypt() {
        let mut client = Client::new(dummy_cfg());
        client.encode_cipher = Some(crypto::cipher_from_key(&[0; 16]));

        let item = SendItem {
            data: vec![0xA5; 10],
            cmd: Command::UI as u8,
            encrypted: true,
            priority: SendPriority::Low,
            retry_left: 0,
            max_retries: 0,
            msg_num: 0,
            last_sent_at: 0,
            u_key: UniqueKey::none(),
        };

        client.batch_send_direct(&item);

        let wire_len =
            u16::from_le_bytes([client.tmp_send_buf[1], client.tmp_send_buf[2]]) as usize;
        assert_eq!(client.tmp_send_buf[0], Command::Crypted as u8);
        assert_eq!(wire_len, 60);
        assert_eq!(client.tmp_send_buf.len(), 3 + wire_len);
        assert_eq!(client.tmp_send_size, 15 + 3 + wire_len);
    }

    #[test]
    fn do_send_mp_data_sends_current_item_direct_when_buffer_is_smaller_like_delphi() {
        let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        server_sock
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let server_addr = server_sock.local_addr().unwrap();
        let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();

        let mut cfg = dummy_cfg();
        cfg.server_port = server_addr.port();
        let mut client = Client::new(cfg);
        client.socket = Some(client_sock);
        client.actual_pmtu = 100;

        let small = SendItem {
            data: vec![0x11; 10], // Delphi sz = 10 + header(15) + item hdr(3) = 28
            cmd: Command::UI as u8,
            encrypted: false,
            priority: SendPriority::Low,
            retry_left: 0,
            max_retries: 0,
            msg_num: 0,
            last_sent_at: 0,
            u_key: UniqueKey::none(),
        };
        let large = SendItem {
            data: vec![0x22; 80], // sz = 98; 28 + 98 > PMTU and 28 > 98 is false
            cmd: Command::API as u8,
            encrypted: false,
            priority: SendPriority::Low,
            retry_left: 0,
            max_retries: 0,
            msg_num: 0,
            last_sent_at: 0,
            u_key: UniqueKey::none(),
        };

        client.batch_send_direct(&small);
        client.batch_send_direct(&large);

        let mut raw = [0u8; 256];
        let (n, _) = server_sock.recv_from(&mut raw).unwrap();
        let (cmd, payload) = unpack_client_packet(&client.cfg.mac_key, &raw[..n]);
        assert_eq!(
            cmd,
            Command::API as u8,
            "Delphi DoSendMPData sends the current oversized item directly and keeps the older buffer"
        );
        assert_eq!(payload, large.data);
        assert_eq!(client.tmp_send_count, 1);
        assert_eq!(client.tmp_send_buf[0], Command::UI as u8);
        assert_eq!(
            u16::from_le_bytes([client.tmp_send_buf[1], client.tmp_send_buf[2]]) as usize,
            small.data.len()
        );
        assert_eq!(&client.tmp_send_buf[3..], small.data.as_slice());

        client.flush_send_batch();
        let (n, _) = server_sock.recv_from(&mut raw).unwrap();
        let (cmd, payload) = unpack_client_packet(&client.cfg.mac_key, &raw[..n]);
        assert_eq!(cmd, Command::UI as u8);
        assert_eq!(payload, small.data);
    }

    #[test]
    fn low_priority_items_are_split_around_sliced_retry_like_delphi() {
        let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        server_sock
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let server_addr = server_sock.local_addr().unwrap();
        let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();

        let mut cfg = dummy_cfg();
        cfg.server_port = server_addr.port();
        let mut client = Client::new(cfg);
        client.socket = Some(client_sock);
        client.actual_pmtu = 508;
        client.round_trip_delay = 0;
        client.trip_delay_k = 1.1;
        client.can_send_rate = 1_000_000;
        client.sending.push(sent_sliced_with_lengths(&[8], 0));

        let first_low = SendItem {
            data: vec![0x11],
            cmd: Command::UI as u8,
            encrypted: false,
            priority: SendPriority::Low,
            retry_left: 0,
            max_retries: 0,
            msg_num: 0,
            last_sent_at: 0,
            u_key: UniqueKey::none(),
        };
        let second_low = SendItem {
            data: vec![0x22],
            cmd: Command::API as u8,
            encrypted: false,
            priority: SendPriority::Low,
            retry_left: 0,
            max_retries: 0,
            msg_num: 0,
            last_sent_at: 0,
            u_key: UniqueKey::none(),
        };
        let l_items = vec![first_low.clone(), second_low.clone()];

        client.send_low_items_around_sliced_retry(&l_items, 1000);

        let mut raw = [0u8; 256];
        let (n, _) = server_sock.recv_from(&mut raw).unwrap();
        let (cmd, payload) = unpack_client_packet(&client.cfg.mac_key, &raw[..n]);
        assert_eq!(cmd, Command::UI as u8);
        assert_eq!(payload, first_low.data);

        let (n, _) = server_sock.recv_from(&mut raw).unwrap();
        let (cmd, _payload) = unpack_client_packet(&client.cfg.mac_key, &raw[..n]);
        assert_eq!(
            cmd,
            Command::Sliced as u8,
            "Delphi retries Sliced after only the first Low item was flushed"
        );

        let (n, _) = server_sock.recv_from(&mut raw).unwrap();
        let (cmd, payload) = unpack_client_packet(&client.cfg.mac_key, &raw[..n]);
        assert_eq!(cmd, Command::API as u8);
        assert_eq!(payload, second_low.data);
    }

    #[test]
    fn encrypted_low_batch_preserves_outer_compressed_flag() {
        let mut client = Client::new(dummy_cfg());
        client.encode_cipher = Some(crypto::cipher_from_key(&[0; 16]));

        let item = SendItem {
            data: vec![0xA5; 10],
            cmd: Command::UI as u8 | COMPRESSED_FLAG,
            encrypted: true,
            priority: SendPriority::Low,
            retry_left: 0,
            max_retries: 0,
            msg_num: 0,
            last_sent_at: 0,
            u_key: UniqueKey::none(),
        };

        client.batch_send_direct(&item);

        assert_eq!(
            client.tmp_send_buf[0],
            Command::Crypted as u8 | COMPRESSED_FLAG
        );
    }

    #[test]
    fn encrypted_high_send_preserves_outer_compressed_flag() {
        let mut client = Client::new(dummy_cfg());
        client.encode_cipher = Some(crypto::cipher_from_key(&[0; 16]));

        let mut item = SendItem {
            data: vec![0xA5; 10],
            cmd: Command::UI as u8 | COMPRESSED_FLAG,
            encrypted: true,
            priority: SendPriority::High,
            retry_left: 1,
            max_retries: 2,
            msg_num: 0,
            last_sent_at: 0,
            u_key: UniqueKey::none(),
        };

        client.send_h_item(&mut item, 123);

        assert_eq!(
            client.tmp_send_buf[0],
            Command::Crypted as u8 | COMPRESSED_FLAG
        );
        assert_eq!(client.pending_h.len(), 1);
        assert_eq!(client.pending_h[0].cmd, Command::UI as u8 | COMPRESSED_FLAG);
    }

    #[test]
    fn sliced_retry_client_limit_is_rounded_like_delphi() {
        let mut client = Client::new(dummy_cfg());
        client.round_trip_delay = 100;
        client.trip_delay_k = 1.1;
        client.actual_sleep_time = 5.0;
        client.can_send_rate = 262_120; // 262120 * 5ms / 1000 = 1310.6 -> 1311
        client.sending.push(sent_sliced_with_lengths(&[1310, 1], 0));

        client.retry_sliced(1000);

        assert_eq!(client.sending[0].sent_count, 4);
    }

    #[test]
    fn sliced_retry_used_limit_threshold_is_rounded_like_delphi() {
        let mut client = Client::new(dummy_cfg());
        client.round_trip_delay = 100;
        client.trip_delay_k = 1.1;
        client.actual_sleep_time = 5.0;
        client.can_send_rate = 262_120; // ClientLimit = 1311, 80% threshold = round(1048.8) = 1049
        client.sending.push(sent_sliced_with_lengths(&[1048], 0));

        client.retry_sliced(1000);

        assert!(!client.used_sliced_limit);
    }

    #[test]
    fn sliced_retry_uses_delphi_last_checked_slices_outer_gate() {
        let mut client = Client::new(dummy_cfg());
        client.round_trip_delay = 100;
        client.trip_delay_k = 1.1; // PathDelay = round(100 * 1.1 + 10) = 120
        client.actual_sleep_time = 5.0;
        client.can_send_rate = 1_000_000;
        client.last_checked_slices = 1000;
        client.sending.push(sent_sliced_with_lengths(&[10], 1000));

        client.retry_sliced(1105);
        assert_eq!(
            client.sending[0].sent_count, 1,
            "Delphi outer gate may run before PathDelay and sends nothing"
        );
        assert_eq!(
            client.last_checked_slices, 1105,
            "Delphi still writes LastCheckedSlices := CurTm on that empty pass"
        );

        client.retry_sliced(1126);
        assert_eq!(
            client.sending[0].sent_count, 1,
            "after the empty pass Delphi waits another RoundTripDelay before retry"
        );

        client.retry_sliced(1206);
        assert_eq!(client.sending[0].sent_count, 2);
    }

    #[test]
    fn sliced_retry_updates_trip_delay_k_before_path_delay_like_delphi() {
        let mut client = Client::new(dummy_cfg());
        client.round_trip_delay = 1000;
        client.trip_delay_k = 1.1;
        client.avg_dup_count = 10.0;
        client.last_set_trip_k = 0;
        client.last_checked_slices = 0;
        client.actual_sleep_time = 5.0;
        client.can_send_rate = 1_000_000;
        client.sending.push(sent_sliced_with_lengths(&[10], 1360));

        client.retry_sliced(2500);

        assert!((client.trip_delay_k - 1.15).abs() < 1e-12);
        assert_eq!(
            client.sending[0].sent_count, 1,
            "Delphi raises TripDelayK before PathDelay; this tick is not due yet with the new K"
        );
    }

    #[test]
    fn sliced_retry_clock_ignores_acked_blocks_like_delphi_apply_ack_removes_them() {
        let mut client = Client::new(dummy_cfg());
        client.round_trip_delay = 100;
        client.trip_delay_k = 1.1;
        client.actual_sleep_time = 5.0;
        client.can_send_rate = 1_000_000;
        client
            .sending
            .push(sent_sliced_with_lengths(&[10, 10, 10], 100));

        let mut ack = [0u8; 34];
        ack[0] = 0b0000_0011; // blocks 0 and 1 ACKed; block 2 still pending.
        ack[32..34].copy_from_slice(&1u16.to_le_bytes());
        let mut delivered = Vec::new();
        let mut sink = DispatchSink::Buffer(&mut delivered);
        client.handle_udp_command(
            Command::SlicedACK,
            Command::SlicedACK as u8,
            &ack,
            &mut sink,
            false,
        );
        let copy_acks = client.get_copy_acks();
        client.apply_copy_acks(copy_acks, 300);

        client.retry_sliced(300);
        assert_eq!(client.sending[0].sent_count, 4);
        assert_eq!(client.sending[0].piece_last_checked[2], 300);
        assert_eq!(client.sending[0].last_checked, 300);

        client.retry_sliced(500);
        assert_eq!(
            client.sending[0].sent_count, 5,
            "unACKed block must be retried again; ACKed old blocks must not pin LastChecked"
        );
        assert_eq!(client.sending[0].piece_last_checked[2], 500);
        assert_eq!(client.sending[0].last_checked, 500);
    }

    #[test]
    fn sliced_ack_applies_only_first_matching_datagram_like_delphi() {
        let mut client = Client::new(dummy_cfg());

        let mut first = sent_sliced_with_lengths(&[10], 100);
        first.datagram_num = 7;
        let mut second = sent_sliced_with_lengths(&[10, 10], 100);
        second.datagram_num = 7;
        client.sending.push(first);
        client.sending.push(second);

        let mut ack = [0u8; 34];
        ack[0] = 0b0000_0001; // complete for first datagram, partial for second if wrongly applied.
        ack[32..34].copy_from_slice(&7u16.to_le_bytes());
        let mut delivered = Vec::new();
        let mut sink = DispatchSink::Buffer(&mut delivered);

        client.handle_udp_command(
            Command::SlicedACK,
            Command::SlicedACK as u8,
            &ack,
            &mut sink,
            false,
        );
        let copy_acks = client.get_copy_acks();
        client.apply_copy_acks(copy_acks, 100);

        assert_eq!(client.sending.len(), 1);
        assert_eq!(client.sending[0].blocks_count, 2);
        assert_eq!(
            client.sending[0].ack_flags[0], 0,
            "Delphi breaks after the first matching Sending item; a wrapped DatagramNum ACK must not mutate the next item"
        );
    }

    #[test]
    fn sliced_ack_reader_queues_writer_applies_like_delphi() {
        let mut client = Client::new(dummy_cfg());
        client.sending.push(sent_sliced_with_lengths(&[10], 100));

        let mut ack = [0u8; 34];
        ack[0] = 0b0000_0001;
        ack[32..34].copy_from_slice(&1u16.to_le_bytes());

        client.on_new_sliced_ack(&ack);
        assert_eq!(
            client.sending.len(),
            1,
            "Delphi OnNewSlicedACK only queues ACKs; ApplyACK is writer/CheckSeningData work"
        );

        let copy_acks = client.get_copy_acks();
        client.apply_copy_acks(copy_acks, 200);
        assert!(
            client.sending.is_empty(),
            "writer copy/apply phase must remove completed sliced datagram"
        );
    }
}

#[cfg(test)]
mod api_retry_tests {
    use super::*;

    fn dummy_cfg() -> ClientConfig {
        ClientConfig {
            server_ip: "127.0.0.1".to_string(),
            server_port: 3000,
            master_key: [0; 16],
            mac_key: [0; 16],
            mask_ver: 0,
            client_id: 0,
            ntp_host: None,
            refresh: RefreshConfig {
                update_markets_every: None,
                check_tags_every: None,
            },
        }
    }

    #[test]
    fn engine_api_sliced_requests_use_delphi_retry_count() {
        let client = Client::new(dummy_cfg());
        let raw = crate::commands::engine_request::query_hedge_mode();

        client.send_api_request(&raw);

        let (sliced, _, _) = client.take_send_queues_for_test();
        assert_eq!(sliced.len(), 1);
        assert_eq!(sliced[0].cmd, Command::API as u8);
        assert_eq!(sliced[0].priority, SendPriority::Sliced);
        assert_eq!(sliced[0].max_retries, 6);
        assert_eq!(sliced[0].retry_left, 5);
    }
}

#[cfg(test)]
mod send_queue_dedup_tests {
    use super::*;

    fn item(kind: u8, uid: u64, marker: u8) -> SendItem {
        SendItem {
            data: vec![marker],
            cmd: Command::Order as u8,
            encrypted: true,
            priority: SendPriority::High,
            retry_left: 2,
            max_retries: 3,
            msg_num: 0,
            last_sent_at: 0,
            u_key: UniqueKey { kind, uid },
        }
    }

    fn item_with_priority(kind: u8, uid: u64, marker: u8, priority: SendPriority) -> SendItem {
        SendItem {
            priority,
            ..item(kind, uid, marker)
        }
    }

    #[test]
    fn send_queue_dedup_keeps_last_item_for_same_u_key() {
        let mut items = vec![
            item(UK_NONE, 0, 0),
            item(UK_ORDER_MOVE, 7, 1),
            item(UK_ORDER_MOVE, 8, 2),
            item(UK_ORDER_MOVE, 7, 3),
            item(UK_NONE, 0, 4),
            item(UK_ORDER_MOVE, 8, 5),
        ];

        Client::dedup_send_items_by_u_key(&mut items);

        let markers: Vec<u8> = items.iter().map(|item| item.data[0]).collect();
        assert_eq!(
            markers,
            vec![0, 3, 4, 5],
            "Delphi SendCmdInt removes older queued items with same non-empty UKey",
        );
    }

    #[test]
    fn send_cmd_int_queue_removes_first_matching_sliced_or_high_before_append() {
        let mut queues = SendQueues::default();
        queues.push_send_cmd_int(item_with_priority(UK_ORDER_MOVE, 7, 1, SendPriority::High));
        queues.push_send_cmd_int(item_with_priority(UK_ORDER_MOVE, 8, 2, SendPriority::High));
        queues.push_send_cmd_int(item_with_priority(UK_ORDER_MOVE, 7, 3, SendPriority::High));
        queues.push_send_cmd_int(item_with_priority(
            UK_ORDER_MOVE,
            7,
            4,
            SendPriority::Sliced,
        ));

        assert_eq!(
            queues
                .high
                .iter()
                .map(|item| item.data[0])
                .collect::<Vec<_>>(),
            vec![2, 3],
            "Delphi SendCmdInt removes only from the selected High queue"
        );
        assert_eq!(
            queues
                .sliced
                .iter()
                .map(|item| item.data[0])
                .collect::<Vec<_>>(),
            vec![4],
            "Sliced queue has its own UKey scope"
        );
    }

    #[test]
    fn send_cmd_int_queue_does_not_dedup_low_priority_like_delphi() {
        let mut queues = SendQueues::default();
        queues.push_send_cmd_int(item_with_priority(UK_ORDER_MOVE, 7, 1, SendPriority::Low));
        queues.push_send_cmd_int(item_with_priority(UK_ORDER_MOVE, 7, 2, SendPriority::Low));

        assert_eq!(
            queues
                .low
                .iter()
                .map(|item| item.data[0])
                .collect::<Vec<_>>(),
            vec![1, 2],
            "Delphi SendCmdInt UKey removal is only for Sliced and High"
        );
    }
}

#[cfg(test)]
mod active_library_helpers_tests {
    use super::*;
    use crate::commands::engine_api::EngineMethod;
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Mutex};

    fn dummy_cfg() -> ClientConfig {
        ClientConfig {
            server_ip: "127.0.0.1".to_string(),
            server_port: 3000,
            master_key: [0; 16],
            mac_key: [0; 16],
            mask_ver: 0,
            client_id: 0,
            ntp_host: None,
            refresh: RefreshConfig {
                update_markets_every: None,
                check_tags_every: None,
            },
        }
    }

    /// Сериализует тесты которые трогают `CLOCK_JUMP_GENERATION` (process-global atomic).
    /// Cargo test запускает тесты в параллельных thread'ах — без этой блокировки
    /// race на generation даёт flaky failures.
    static CLOCK_JUMP_TEST_LOCK: Mutex<()> = Mutex::new(());

    // =====================================================================
    //  check_clock_jump
    // =====================================================================

    #[test]
    fn clock_jump_check_triggers_force_disconnect() {
        let _lock = CLOCK_JUMP_TEST_LOCK.lock().unwrap();
        CLOCK_JUMP_GENERATION.store(0, Ordering::Relaxed); // reset
        let mut client = Client::new(dummy_cfg());
        assert!(!client.force_disconnect);
        CLOCK_JUMP_GENERATION.store(1, Ordering::Relaxed);
        client.check_clock_jump();
        assert!(
            client.force_disconnect,
            "clock jump flag → force_disconnect = true"
        );
        assert_eq!(client.seen_clock_jump_generation, 1);
    }

    #[test]
    fn clock_jump_check_noop_when_flag_clear() {
        let _lock = CLOCK_JUMP_TEST_LOCK.lock().unwrap();
        CLOCK_JUMP_GENERATION.store(0, Ordering::Relaxed);
        let mut client = Client::new(dummy_cfg());
        client.check_clock_jump();
        assert!(!client.force_disconnect, "без флага — никаких изменений");
    }

    #[test]
    fn clock_jump_check_idempotent_after_swap() {
        // Двойной вызов в том же generation — второй раз должен быть no-op.
        let _lock = CLOCK_JUMP_TEST_LOCK.lock().unwrap();
        CLOCK_JUMP_GENERATION.store(0, Ordering::Relaxed); // reset
        let mut client = Client::new(dummy_cfg());
        CLOCK_JUMP_GENERATION.store(1, Ordering::Relaxed);
        client.check_clock_jump();
        assert!(
            client.force_disconnect,
            "первый вызов с flag=true → force_disconnect"
        );
        client.force_disconnect = false; // reset для второй проверки
        client.check_clock_jump();
        assert!(
            !client.force_disconnect,
            "тот же generation — второй вызов no-op"
        );
    }

    #[test]
    fn clock_jump_generation_is_seen_by_each_client() {
        let _lock = CLOCK_JUMP_TEST_LOCK.lock().unwrap();
        CLOCK_JUMP_GENERATION.store(0, Ordering::Relaxed);
        let mut a = Client::new(dummy_cfg());
        let mut b = Client::new(dummy_cfg());

        CLOCK_JUMP_GENERATION.store(1, Ordering::Relaxed);
        a.check_clock_jump();
        b.check_clock_jump();

        assert!(
            a.force_disconnect,
            "первый Client должен увидеть generation"
        );
        assert!(
            b.force_disconnect,
            "второй Client не должен терять global signal"
        );
        assert_eq!(a.seen_clock_jump_generation, 1);
        assert_eq!(b.seen_clock_jump_generation, 1);
    }

    #[test]
    fn clock_jump_detector_ignores_normal_elapsed_time() {
        let base = Instant::now();
        let prev_wall = 45_000.0;
        let now_wall = prev_wall + 120.0 / 86_400.0;

        assert!(
            !is_clock_jump(prev_wall, base, now_wall, base + Duration::from_secs(120)),
            "обычные 120 секунд elapsed не являются скачком wall-clock",
        );
    }

    #[test]
    fn clock_jump_detector_catches_wall_clock_step() {
        let base = Instant::now();
        let prev_wall = 45_000.0;
        let now_wall = prev_wall + 180.0 / 86_400.0;

        assert!(
            is_clock_jump(prev_wall, base, now_wall, base + Duration::from_secs(10)),
            "wall-clock ушёл на 180с при 10с monotonic elapsed",
        );
    }

    #[test]
    fn bind_failed_event_waits_for_elapsed_threshold() {
        let mut client = Client::new(dummy_cfg());
        let events = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&events);
        client.on_lifecycle(Box::new(move |ev| sink.lock().unwrap().push(ev)));

        client.record_bind_failure(1_000);
        client.record_bind_failure(1_005);
        client.record_bind_failure(1_010);
        assert!(
            events.lock().unwrap().is_empty(),
            "три быстрые серии bind errors не должны сразу шуметь в UI",
        );

        client.record_bind_failure(16_000);
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], LifecycleEvent::BindFailed { .. }));
    }

    #[test]
    fn bind_failed_event_repeats_only_after_throttle_window() {
        let mut client = Client::new(dummy_cfg());
        let events = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&events);
        client.on_lifecycle(Box::new(move |ev| sink.lock().unwrap().push(ev)));

        client.record_bind_failure(0);
        client.record_bind_failure(15_000);
        client.record_bind_failure(20_000);
        assert_eq!(events.lock().unwrap().len(), 1);

        client.record_bind_failure(65_000);
        assert_eq!(events.lock().unwrap().len(), 2);
    }

    #[test]
    fn bind_failure_tracking_resets_after_successful_bind() {
        let mut client = Client::new(dummy_cfg());
        client.record_bind_failure(0);
        client.record_bind_failure(15_000);
        assert!(client.bind_failure_streak > 0);

        client.reset_bind_failure_tracking();

        assert_eq!(client.bind_failure_streak, 0);
        assert_eq!(client.first_bind_failure_ms, NEVER_TIME_MS);
        assert_eq!(client.last_bind_failed_event_ms, NEVER_TIME_MS);
    }

    // =====================================================================
    //  check_indexes_fetch_timeout
    // =====================================================================

    #[test]
    fn indexes_fetch_timeout_does_nothing_when_not_in_flight() {
        let mut client = Client::new(dummy_cfg());
        client.indexes_fetch_in_flight = false;
        client.indexes_fetch_started_ms = 0;
        client.check_indexes_fetch_timeout(100_000_000);
        assert!(!client.indexes_fetch_in_flight);
    }

    #[test]
    fn indexes_fetch_timeout_preserves_in_flight_within_window() {
        let mut client = Client::new(dummy_cfg());
        client.indexes_fetch_in_flight = true;
        client.indexes_fetch_started_ms = 0;
        // 5 секунд прошло — меньше 12с timeout.
        client.check_indexes_fetch_timeout(5_000);
        assert!(
            client.indexes_fetch_in_flight,
            "в пределах timeout — флаг сохраняется"
        );
    }

    #[test]
    fn indexes_fetch_timeout_clears_in_flight_after_window() {
        let mut client = Client::new(dummy_cfg());
        client.indexes_fetch_in_flight = true;
        client.indexes_fetch_started_ms = 0;
        client.peer_app_token = 0; // не triggers re-send (нет mismatch)
        client.tracked_indexes_peer_app_token = 0;
        // 13 секунд — больше 12с timeout.
        client.check_indexes_fetch_timeout(13_000);
        assert!(
            !client.indexes_fetch_in_flight,
            "после timeout без peer_app_token mismatch — флаг сбрасывается"
        );
    }

    #[test]
    fn indexes_fetch_timeout_does_not_retry_without_init_intent() {
        let mut client = Client::new(dummy_cfg());
        client.indexes_fetch_in_flight = true;
        client.indexes_fetch_started_ms = 0;
        // PeerAppToken расходится, но единственный Init ещё не заказывал индексы.
        client.peer_app_token = 0xABC;
        client.tracked_indexes_peer_app_token = 0xDEF;
        client.domain_ready = true;
        client.check_indexes_fetch_timeout(13_000);
        assert!(
            !client.indexes_fetch_in_flight,
            "timeout cleanup только сбрасывает marker"
        );
        assert_eq!(
            client.indexes_fetch_started_ms, 0,
            "no re-send means started timestamp is unchanged"
        );
        assert!(client.drain_app_events_for_test().is_empty());
        let (sliced, high, low) = client.take_send_queues_for_test();
        assert!(sliced.is_empty() && high.is_empty() && low.is_empty());
    }

    #[test]
    fn indexes_fetch_timeout_retries_after_init_intent() {
        let mut client = Client::new(dummy_cfg());
        client.indexes_fetch_in_flight = true;
        client.indexes_fetch_started_ms = 0;
        client.peer_app_token = 0xABC;
        client.tracked_indexes_peer_app_token = 0xDEF;
        client.domain_ready = true;
        client.domain_restore.fetch_indexes = true;

        client.check_indexes_fetch_timeout(13_000);

        assert!(client.indexes_fetch_in_flight);
        assert_eq!(client.indexes_fetch_started_ms, 13_000);
        let (sliced, _, _) = client.take_send_queues_for_test();
        assert_eq!(
            sliced.len(),
            1,
            "post-init timeout must retry GetMarketsIndexes"
        );
        assert_eq!(sliced[0].cmd, Command::API as u8);
        assert_eq!(
            sliced[0].data.get(11).copied(),
            Some(EngineMethod::GetMarketsIndexes as u8)
        );
    }

    #[test]
    fn indexes_fetch_timeout_zero_peer_token_does_not_re_send() {
        // Если peer_app_token = 0 (никогда не подключались) → не re-send даже если mismatch.
        let mut client = Client::new(dummy_cfg());
        client.indexes_fetch_in_flight = true;
        client.indexes_fetch_started_ms = 0;
        client.peer_app_token = 0;
        client.tracked_indexes_peer_app_token = 0xABC;
        client.check_indexes_fetch_timeout(13_000);
        assert!(
            !client.indexes_fetch_in_flight,
            "peer_app_token=0 (не подключены) → не re-send, флаг сброшен"
        );
    }
}

#[cfg(test)]
mod registry_subscription_restore_tests {
    use super::*;
    use crate::commands::engine_api::EngineMethod;

    fn dummy_cfg() -> ClientConfig {
        ClientConfig {
            server_ip: "127.0.0.1".to_string(),
            server_port: 3000,
            master_key: [0; 16],
            mac_key: [0; 16],
            mask_ver: 0,
            client_id: 0,
            ntp_host: None,
            refresh: RefreshConfig {
                update_markets_every: None,
                check_tags_every: None,
            },
        }
    }

    /// Извлекает `EngineMethod` ID из wire-payload Engine request'а.
    /// Header: CmdId(1) + ver(2) + UID(8) = 11 байт → Method на offset 11.
    fn method_id(payload: &[u8]) -> Option<u8> {
        if payload.len() < 12 {
            return None;
        }
        Some(payload[11])
    }

    fn command_uid(payload: &[u8]) -> Option<u64> {
        payload
            .get(3..11)
            .and_then(|s| s.try_into().ok())
            .map(u64::from_le_bytes)
    }

    fn subscribe_all_trades_want_mm(payload: &[u8]) -> Option<bool> {
        if method_id(payload)? != EngineMethod::SubscribeAllTrades as u8 {
            return None;
        }
        payload.last().map(|v| *v != 0)
    }

    /// Дренирует send queues клиента, собирая wire-payload'ы отправленных API-запросов.
    fn drain_api_requests(client: &Client) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let (sliced, high, low) = client.take_send_queues_for_test();
        for item in sliced.into_iter().chain(high).chain(low) {
            if item.cmd == Command::API as u8 {
                out.push(item.data);
            }
        }
        out
    }

    fn drain_send_items(client: &Client) -> Vec<SendItem> {
        let (mut sliced, mut high, mut low) = client.take_send_queues_for_test();
        sliced.append(&mut high);
        sliced.append(&mut low);
        sliced
    }

    #[test]
    fn restore_with_empty_registry_sends_nothing() {
        let mut client = Client::new(dummy_cfg());
        client.server_token = 0xCAFE;
        client.restore_registry_subscriptions();
        let sent = drain_api_requests(&client);
        assert!(sent.is_empty(), "пустой registry → 0 wire-запросов");
    }

    #[test]
    fn restore_trades_only_sends_single_subscribe_all_trades() {
        let mut client = Client::new(dummy_cfg());
        client.subscription_registry.trades_sub = Some(TradesSubscription { want_mm: true });
        client.server_token = 1;
        client.restore_registry_subscriptions();
        let sent = drain_api_requests(&client);
        assert_eq!(sent.len(), 1, "только trades → 1 wire-запрос");
        assert_eq!(
            method_id(&sent[0]),
            Some(EngineMethod::SubscribeAllTrades as u8)
        );
        assert_eq!(subscribe_all_trades_want_mm(&sent[0]), Some(true));
    }

    #[test]
    fn restore_trades_uses_latest_mm_orders_flag() {
        let mut client = Client::new(dummy_cfg());
        client.subscription_registry.trades_sub = Some(TradesSubscription { want_mm: false });
        client.subscription_registry.mm_orders_sub = Some(true);
        client.server_token = 1;
        client.restore_registry_subscriptions();
        let sent = drain_api_requests(&client);
        assert_eq!(sent.len(), 1);
        assert_eq!(
            method_id(&sent[0]),
            Some(EngineMethod::SubscribeAllTrades as u8)
        );
        assert_eq!(subscribe_all_trades_want_mm(&sent[0]), Some(true));
    }

    #[test]
    fn restore_mm_orders_without_trades_sends_ui_subscription() {
        let mut client = Client::new(dummy_cfg());
        client.subscription_registry.mm_orders_sub = Some(true);
        client.server_token = 1;
        client.restore_registry_subscriptions();
        let sent = drain_send_items(&client);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].cmd, Command::UI as u8);
        assert_eq!(sent[0].priority, SendPriority::High);
        let uid = command_uid(&sent[0].data).expect("wire command UID");
        assert_eq!(sent[0].u_key, UniqueKey::turn_mm_detection_for(uid));
        assert_eq!(sent[0].data.first().copied(), Some(5));
        assert_eq!(sent[0].data.last().copied(), Some(1));
    }

    #[test]
    fn restore_orderbooks_are_batched_into_single_request() {
        let mut client = Client::new(dummy_cfg());
        client
            .subscription_registry
            .orderbook_subs
            .insert("BTC".to_string());
        client
            .subscription_registry
            .orderbook_subs
            .insert("ETH".to_string());
        client
            .subscription_registry
            .orderbook_subs
            .insert("XRP".to_string());
        client.server_token = 1;
        client.restore_registry_subscriptions();
        let sent = drain_api_requests(&client);
        // Все три подписки должны уйти ОДНИМ batch'ем, не тремя.
        assert_eq!(sent.len(), 1, "3 orderbook подписки → 1 batch wire-запрос");
        assert_eq!(
            method_id(&sent[0]),
            Some(EngineMethod::SubscribeOrderBook as u8)
        );
    }

    #[test]
    fn restore_orderbooks_dedup_by_market_name() {
        let mut client = Client::new(dummy_cfg());
        assert!(client
            .subscription_registry
            .orderbook_subs
            .insert("BTC".to_string()));
        assert!(!client
            .subscription_registry
            .orderbook_subs
            .insert("BTC".to_string()));
        client.server_token = 1;
        client.restore_registry_subscriptions();
        let sent = drain_api_requests(&client);
        assert_eq!(sent.len(), 1, "same market is one server-side subscription");
        assert_eq!(
            method_id(&sent[0]),
            Some(EngineMethod::SubscribeOrderBook as u8)
        );
    }

    #[test]
    fn restore_combined_sends_trades_plus_orderbook_batches() {
        let mut client = Client::new(dummy_cfg());
        client.subscription_registry.trades_sub = Some(TradesSubscription { want_mm: false });
        client
            .subscription_registry
            .orderbook_subs
            .insert("BTC".to_string());
        client
            .subscription_registry
            .orderbook_subs
            .insert("XRP".to_string());
        client.server_token = 1;
        client.restore_registry_subscriptions();
        let sent = drain_api_requests(&client);
        assert_eq!(sent.len(), 2, "1 trades + 1 orderbook batch = 2 запроса");
        let methods: Vec<Option<u8>> = sent.iter().map(|p| method_id(p)).collect();
        // Один из запросов — SubscribeAllTrades.
        assert!(methods.contains(&Some(EngineMethod::SubscribeAllTrades as u8)));
        // Один запрос — SubscribeOrderBook batch.
        let book_count = methods
            .iter()
            .filter(|m| **m == Some(EngineMethod::SubscribeOrderBook as u8))
            .count();
        assert_eq!(book_count, 1);
    }
}

#[cfg(test)]
mod refresh_tick_tests {
    use super::*;

    fn dummy_cfg(refresh: RefreshConfig) -> ClientConfig {
        ClientConfig {
            server_ip: "127.0.0.1".to_string(),
            server_port: 3000,
            master_key: [0; 16],
            mac_key: [0; 16],
            mask_ver: 0,
            client_id: 0,
            ntp_host: None,
            refresh,
        }
    }

    fn drain_api_methods(client: &Client) -> Vec<u8> {
        let mut out = Vec::new();
        let (sliced, high, low) = client.take_send_queues_for_test();
        for item in sliced.into_iter().chain(high).chain(low) {
            if item.cmd == Command::API as u8 && item.data.len() >= 12 {
                out.push(item.data[11]);
            }
        }
        out
    }

    #[test]
    fn refresh_config_defaults() {
        // Документированные дефолты: Delphi-worker cadence, gated by domain_ready.
        let cfg = RefreshConfig::default();
        assert_eq!(cfg.update_markets_every, Some(Duration::from_secs(2)));
        assert_eq!(cfg.check_tags_every, Some(Duration::from_secs(60)));
    }

    #[test]
    fn run_loop_does_not_refresh_between_auth_done_and_domain_init() {
        let mut client = Client::new(dummy_cfg(RefreshConfig {
            update_markets_every: Some(Duration::from_millis(1)),
            check_tags_every: Some(Duration::from_millis(1)),
        }));
        client.socket = Some(std::net::UdpSocket::bind("127.0.0.1:0").unwrap());
        client.need_connect = false;
        client.authorized = true;
        client.auth_status = AuthStatus::AuthDone;

        let mut dispatcher = crate::events::EventDispatcher::new();
        let initial_markets_ms = client.last_update_markets_ms;
        let initial_tags_ms = client.last_check_tags_ms;

        client.run_with_dispatcher_queued(Duration::from_millis(20), &mut dispatcher);

        assert_eq!(
            client.last_update_markets_ms, initial_markets_ms,
            "AuthDone before run_init_sequence must not start UpdateMarketsList refresh"
        );
        assert_eq!(
            client.last_check_tags_ms, initial_tags_ms,
            "AuthDone before run_init_sequence must not start CheckBinanceTags refresh"
        );
        assert!(
            drain_api_methods(&client).is_empty(),
            "pre-init run loop must not enqueue background Engine API requests"
        );

        client.testing_set_domain_ready(true);
        client.run_with_dispatcher_queued(Duration::from_millis(20), &mut dispatcher);

        assert_ne!(
            client.last_update_markets_ms, initial_markets_ms,
            "after domain init the same refresh config should become active"
        );
        assert_ne!(
            client.last_check_tags_ms, initial_tags_ms,
            "after domain init the same refresh config should become active"
        );
    }

    #[test]
    fn default_refresh_starts_after_domain_init() {
        let mut client = Client::new(dummy_cfg(RefreshConfig::default()));
        client.socket = Some(std::net::UdpSocket::bind("127.0.0.1:0").unwrap());
        client.need_connect = false;
        client.authorized = true;
        client.auth_status = AuthStatus::AuthDone;
        client.testing_set_domain_ready(true);

        let mut dispatcher = crate::events::EventDispatcher::new();
        let initial_markets_ms = client.last_update_markets_ms;
        let initial_tags_ms = client.last_check_tags_ms;

        client.run_with_dispatcher_queued(Duration::from_millis(20), &mut dispatcher);

        assert_ne!(client.last_update_markets_ms, initial_markets_ms);
        assert_ne!(client.last_check_tags_ms, initial_tags_ms);
    }

    #[test]
    fn tick_sends_first_time_immediately() {
        // last_update_markets_ms = i64::MIN/2 ("никогда") → первый тик должен сразу
        // зафиксировать timestamp (что эквивалентно отправке запроса; реальная отправка
        // в socket=None ветке log warn'ит, но логика update состоялась).
        let mut client = Client::new(dummy_cfg(RefreshConfig {
            update_markets_every: Some(Duration::from_millis(100)),
            check_tags_every: None,
        }));
        let before = client.last_update_markets_ms;
        assert_eq!(before, i64::MIN / 2);
        client.tick_periodic_refresh(0);
        assert_eq!(
            client.last_update_markets_ms, 0,
            "первый тик должен зафиксировать timestamp 0"
        );
    }

    #[test]
    fn tick_respects_interval() {
        let mut client = Client::new(dummy_cfg(RefreshConfig {
            update_markets_every: Some(Duration::from_millis(100)),
            check_tags_every: None,
        }));
        client.last_update_markets_ms = 50;

        // 50ms прошло из 100ms required — не должен слать.
        client.tick_periodic_refresh(100);
        assert_eq!(
            client.last_update_markets_ms, 50,
            "interval не прошёл — last_update_markets_ms не меняется"
        );

        // 100ms прошло — отправка.
        client.tick_periodic_refresh(150);
        assert_eq!(
            client.last_update_markets_ms, 150,
            "100ms прошло — отправка состоялась"
        );
    }

    #[test]
    fn tick_does_nothing_when_both_disabled() {
        let mut client = Client::new(dummy_cfg(RefreshConfig {
            update_markets_every: None,
            check_tags_every: None,
        }));
        let was_markets = client.last_update_markets_ms;
        let was_tags = client.last_check_tags_ms;
        client.tick_periodic_refresh(1_000_000);
        assert_eq!(
            client.last_update_markets_ms, was_markets,
            "update_markets выключен — last_update_markets_ms не меняется"
        );
        assert_eq!(
            client.last_check_tags_ms, was_tags,
            "check_tags выключен — last_check_tags_ms не меняется"
        );
    }

    #[test]
    fn tick_check_tags_independent_from_update_markets() {
        let mut client = Client::new(dummy_cfg(RefreshConfig {
            update_markets_every: None,
            check_tags_every: Some(Duration::from_millis(200)),
        }));
        let was_markets = client.last_update_markets_ms;
        client.tick_periodic_refresh(1_000_000);
        assert_eq!(
            client.last_update_markets_ms, was_markets,
            "update_markets выключен — не трогаем"
        );
        assert_eq!(
            client.last_check_tags_ms, 1_000_000,
            "check_tags включен — трогаем"
        );
    }

    #[test]
    fn first_check_tags_tick_initializes_hour_without_burst() {
        let mut client = Client::new(dummy_cfg(RefreshConfig {
            update_markets_every: None,
            check_tags_every: Some(Duration::from_secs(60)),
        }));
        assert_eq!(client.check_tags_hour_slot, i64::MIN);

        client.tick_periodic_refresh_at(0, 42);
        assert_eq!(client.check_tags_hour_slot, 42);
        assert_eq!(client.check_tags_burst_sent, CHECK_TAGS_BURST_COUNT);
        assert_eq!(
            drain_api_methods(&client),
            vec![EngineMethod::CheckBinanceTags as u8],
        );

        client.tick_periodic_refresh_at(200, 42);
        assert!(
            drain_api_methods(&client).is_empty(),
            "initial tick is not a burst"
        );
    }

    #[test]
    fn tick_both_intervals_independent() {
        // Оба включены, но с разными интервалами — каждый тикает по своему.
        let mut client = Client::new(dummy_cfg(RefreshConfig {
            update_markets_every: Some(Duration::from_millis(100)),
            check_tags_every: Some(Duration::from_millis(500)),
        }));
        client.last_update_markets_ms = 0;
        client.last_check_tags_ms = 0;

        // 150ms: update_markets должен сработать (100ms прошло), check_tags нет.
        client.tick_periodic_refresh(150);
        assert_eq!(client.last_update_markets_ms, 150);
        assert_eq!(client.last_check_tags_ms, 0);

        // 600ms: update_markets должен сработать (450ms с прошлого), check_tags тоже (600ms с прошлого).
        client.tick_periodic_refresh(600);
        assert_eq!(client.last_update_markets_ms, 600);
        assert_eq!(client.last_check_tags_ms, 600);
    }

    #[test]
    fn check_tags_hourly_burst_sends_four_requests_with_spacing() {
        let mut client = Client::new(dummy_cfg(RefreshConfig {
            update_markets_every: None,
            check_tags_every: Some(Duration::from_secs(60)),
        }));
        client.check_tags_hour_slot = 10;
        client.last_check_tags_ms = 1_000;
        client.check_tags_burst_sent = CHECK_TAGS_BURST_COUNT;
        drain_api_methods(&client);

        client.tick_periodic_refresh_at(10_000, 11);
        assert_eq!(
            drain_api_methods(&client),
            vec![EngineMethod::CheckBinanceTags as u8],
        );
        assert_eq!(client.check_tags_burst_sent, 1);

        client.tick_periodic_refresh_at(10_100, 11);
        assert!(
            drain_api_methods(&client).is_empty(),
            "200ms spacing not reached"
        );

        client.tick_periodic_refresh_at(10_200, 11);
        client.tick_periodic_refresh_at(10_400, 11);
        client.tick_periodic_refresh_at(10_600, 11);
        assert_eq!(
            drain_api_methods(&client),
            vec![
                EngineMethod::CheckBinanceTags as u8,
                EngineMethod::CheckBinanceTags as u8,
                EngineMethod::CheckBinanceTags as u8,
            ],
        );
        assert_eq!(client.check_tags_burst_sent, CHECK_TAGS_BURST_COUNT);

        client.tick_periodic_refresh_at(10_800, 11);
        assert!(
            drain_api_methods(&client).is_empty(),
            "no fifth burst request"
        );
    }
}

#[cfg(test)]
mod server_info_tests {
    use super::*;
    use crate::commands::engine_api::ServerInfo;

    fn dummy_cfg() -> ClientConfig {
        ClientConfig {
            server_ip: "127.0.0.1".to_string(),
            server_port: 3000,
            master_key: [0; 16],
            mac_key: [0; 16],
            mask_ver: 0,
            client_id: 0,
            ntp_host: None, // no NTP worker needed for this unit test
            refresh: RefreshConfig {
                update_markets_every: None,
                check_tags_every: None,
            },
        }
    }

    #[test]
    fn server_info_default_on_new_client() {
        let client = Client::new(dummy_cfg());
        assert_eq!(client.server_info(), &ServerInfo::default());
        assert!(!client.server_info().has_identity());
    }

    #[test]
    fn set_server_info_updates_storage_and_is_retrievable_via_getter() {
        let mut client = Client::new(dummy_cfg());
        let info = ServerInfo {
            bot_id: Some(0x1234_5678),
            server_name: Some("Test Server".to_string()),
            exchange_code: Some(1),
            exchange_name: Some("Binance Futures".to_string()),
            base_currency_name: Some("USDT".to_string()),
            base_currency_code: Some(1),
            ..Default::default()
        };
        client.set_server_info(info.clone());
        assert_eq!(client.server_info(), &info);
        assert_eq!(client.server_info().bot_id, Some(0x1234_5678));
        assert_eq!(
            client.server_info().exchange_name.as_deref(),
            Some("Binance Futures")
        );
        assert!(client.server_info().has_identity());
    }

    #[test]
    fn server_info_independent_across_clients() {
        // Multi-server: два Client'а с разными server_info никак не должны
        // влиять друг на друга. Это база для multi-server терминала.
        let mut client_a = Client::new(dummy_cfg());
        let mut client_b = Client::new(dummy_cfg());

        client_a.set_server_info(ServerInfo {
            bot_id: Some(100),
            exchange_name: Some("Binance".to_string()),
            ..Default::default()
        });
        client_b.set_server_info(ServerInfo {
            bot_id: Some(200),
            exchange_name: Some("Bybit".to_string()),
            ..Default::default()
        });

        assert_eq!(client_a.server_info().bot_id, Some(100));
        assert_eq!(client_b.server_info().bot_id, Some(200));
        assert_eq!(
            client_a.server_info().exchange_name.as_deref(),
            Some("Binance")
        );
        assert_eq!(
            client_b.server_info().exchange_name.as_deref(),
            Some("Bybit")
        );
    }

    #[test]
    fn trade_ctx_requires_base_check_route_fields() {
        let client = Client::new(dummy_cfg());

        let err = client
            .trade_ctx(0x0102_0304_0506_0708)
            .expect_err("new client has no BaseCheck route");
        assert!(err.missing_exchange_code);
        assert!(err.missing_base_currency_code);
    }

    #[test]
    fn trade_ctx_uses_server_info_route_fields() {
        let mut client = Client::new(dummy_cfg());
        client.set_server_info(ServerInfo {
            exchange_code: Some(9),
            base_currency_code: Some(17),
            ..Default::default()
        });

        let ctx = client
            .trade_ctx(0x0102_0304_0506_0708)
            .expect("route fields are present");

        assert_eq!(ctx.uid, 0x0102_0304_0506_0708);
        assert_eq!(ctx.currency, 17);
        assert_eq!(ctx.platform, 9);
    }
}

#[cfg(test)]
mod subscription_registry_tests {
    use super::*;

    #[test]
    fn registry_default_is_empty() {
        let r = SubscriptionRegistry::default();
        assert!(r.orderbook_subs.is_empty());
        assert!(r.trades_sub.is_none());
    }

    #[test]
    fn registry_orderbook_insert_dedups() {
        let mut r = SubscriptionRegistry::default();
        assert!(r.orderbook_subs.insert("BTCUSDT".to_string()));
        assert!(!r.orderbook_subs.insert("BTCUSDT".to_string()));
        assert!(r.orderbook_subs.insert("ETHUSDT".to_string()));
        assert_eq!(r.orderbook_subs.len(), 2);
    }

    #[test]
    fn trades_subscription_round_trip() {
        let sub = TradesSubscription { want_mm: true };
        assert!(sub.want_mm);
        let sub_off = TradesSubscription { want_mm: false };
        assert!(!sub_off.want_mm);
    }

    /// Verify что Connected{fresh:true} срабатывает только на ПЕРВОМ Authenticated
    /// в жизни Client'а. После этого все последующие = fresh:false.
    /// Тестируем through state-machine simulation (без полного Client::new).
    #[test]
    fn lifecycle_event_connected_fresh_flag_semantics() {
        // Симулируем: при первом переходе → fresh=true. При втором → fresh=false.
        let mut was_ever_connected = false;
        let first = LifecycleEvent::Connected {
            fresh: !was_ever_connected,
        };
        was_ever_connected = true;
        let second = LifecycleEvent::Connected {
            fresh: !was_ever_connected,
        };
        assert_eq!(first, LifecycleEvent::Connected { fresh: true });
        assert_eq!(second, LifecycleEvent::Connected { fresh: false });
    }
}

#[cfg(test)]
mod event_loop_fairness_tests {
    use super::*;
    use crate::events::EventDispatcher;

    fn dummy_cfg() -> ClientConfig {
        ClientConfig {
            server_ip: "127.0.0.1".to_string(),
            server_port: 3000,
            master_key: [0; 16],
            mac_key: [0; 16],
            mask_ver: 0,
            client_id: 0,
            ntp_host: None,
            refresh: RefreshConfig {
                update_markets_every: None,
                check_tags_every: None,
            },
        }
    }

    fn stale_recv_event() -> ClientEvent {
        ClientEvent::Recv(RecvMsg {
            cmd: Command::OrderBook as u8,
            payload: Vec::new(),
            recv_bytes: 0,
            timestamp_ms: 0,
            handled_in_reader: false,
            err_emu_drop: false,
            reader_dataread_core_done: false,
            epoch: u32::MAX,
        })
    }

    #[test]
    fn send_phase_runs_with_recv_backlog() {
        let mut client = Client::new(dummy_cfg());
        let mut dispatcher = EventDispatcher::new();

        for _ in 0..(EVENT_DRAIN_BUDGET / 2) {
            client.event_tx.send(stale_recv_event()).unwrap();
        }
        client
            .event_tx
            .send(ClientEvent::Send(SendMsg {
                item: SendItem {
                    data: vec![1, 2, 3, 4],
                    cmd: Command::UI as u8,
                    priority: SendPriority::Sliced,
                    encrypted: false,
                    retry_left: 0,
                    max_retries: 0,
                    msg_num: 0,
                    last_sent_at: 0,
                    u_key: UniqueKey::none(),
                },
            }))
            .unwrap();
        for _ in 0..EVENT_DRAIN_BUDGET {
            client.event_tx.send(stale_recv_event()).unwrap();
        }

        client.run_with_dispatcher_queued(Duration::from_millis(5), &mut dispatcher);

        assert!(
            !client.sending.is_empty(),
            "send phase must run even when recv backlog remains possible"
        );
    }

    #[test]
    fn app_send_queue_is_not_blocked_by_reader_backlog() {
        let mut client = Client::new(dummy_cfg());
        let mut dispatcher = EventDispatcher::new();

        for _ in 0..(EVENT_DRAIN_BUDGET * 2) {
            client.event_tx.send(stale_recv_event()).unwrap();
        }
        client.send_cmd(
            vec![1, 2, 3, 4],
            Command::UI,
            SendPriority::Sliced,
            false,
            0,
        );

        client.run_with_dispatcher_queued(Duration::from_millis(5), &mut dispatcher);

        assert!(
            !client.sending.is_empty(),
            "app/user sends must use the separate outgoing queue, not wait behind reader backlog"
        );
    }

    #[test]
    fn err_emu_drop_updates_valid_packet_stats_before_protocol_drop() {
        let mut client = Client::new(dummy_cfg());
        let delivered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let delivered_cb = Arc::clone(&delivered);
        let mut mode = RunMode::Callback {
            on_data: Box::new(move |_, _| {
                delivered_cb.fetch_add(1, Ordering::Relaxed);
            }),
        };

        client.process_recv_event(
            RecvMsg {
                cmd: Command::OrderBook as u8,
                payload: vec![1, 2, 3],
                recv_bytes: 1234,
                timestamp_ms: 777,
                handled_in_reader: false,
                err_emu_drop: true,
                reader_dataread_core_done: false,
                epoch: client.current_reader_epoch,
            },
            777,
            &mut mode,
        );

        assert!(client.connected);
        assert_eq!(client.auth_status, AuthStatus::Connected);
        assert_eq!(client.total_recv, 1234);
        assert_eq!(client.last_online, 777);
        assert_eq!(
            delivered.load(Ordering::Relaxed),
            0,
            "ErrEmu drop must happen after Delphi stats side effects but before protocol delivery"
        );
    }

    #[test]
    fn recv_event_is_processed_in_drain_position_like_delphi() {
        let mut client = Client::new(dummy_cfg());
        let delivered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let delivered_cb = Arc::clone(&delivered);
        let mut mode = RunMode::Callback {
            on_data: Box::new(move |cmd, payload| {
                assert_eq!(cmd, Command::UI);
                assert_eq!(payload, &[0xAA]);
                delivered_cb.fetch_add(1, Ordering::Relaxed);
            }),
        };
        let mut sliced = Vec::new();
        let mut h_items = Vec::new();
        let mut l_items = Vec::new();
        let mut control_events = Vec::new();

        client.handle_main_event(
            ClientEvent::Recv(RecvMsg {
                cmd: Command::UI as u8,
                payload: vec![0xAA],
                recv_bytes: 1,
                timestamp_ms: 777,
                handled_in_reader: false,
                err_emu_drop: false,
                reader_dataread_core_done: false,
                epoch: client.current_reader_epoch,
            }),
            777,
            &mut mode,
            &mut sliced,
            &mut h_items,
            &mut l_items,
            &mut control_events,
        );

        assert_eq!(
            delivered.load(Ordering::Relaxed),
            1,
            "Delphi UDPRead calls DataRead/DataReadInt before the writer tick; Recv must not wait in a post-send deferred queue"
        );
    }

    #[test]
    fn reader_decoded_sliced_payload_bypasses_recv_event_backlog() {
        let mut client = Client::new(dummy_cfg());
        let datagram_num = 77;
        client.slicer.lock().unwrap().receiving.insert(
            datagram_num,
            crate::protocol::slicing::SlicedData::new(datagram_num, 0),
        );
        client
            .slicer
            .lock()
            .unwrap()
            .receiving
            .remove(&datagram_num);

        let delivered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let delivered_cb = Arc::clone(&delivered);
        let slicer_for_cb = Arc::clone(&client.slicer);
        let mut mode = RunMode::Callback {
            on_data: Box::new(move |cmd, payload| {
                assert_eq!(cmd, Command::UI);
                assert_eq!(payload, &[0xAA, 0xBB]);
                assert!(
                    slicer_for_cb
                        .lock()
                        .unwrap()
                        .receiving
                        .get(&datagram_num)
                        .is_none(),
                    "reader has already run DataReadInt core and removed Receiving before queued user delivery"
                );
                delivered_cb.fetch_add(1, Ordering::Relaxed);
            }),
        };

        for _ in 0..(EVENT_DRAIN_BUDGET * 2) {
            client.event_tx.send(stale_recv_event()).unwrap();
        }
        client
            .pending_reader_decoded
            .lock()
            .unwrap()
            .push(ReaderDecodedMsg {
                cmd: Command::UI as u8,
                payload: Some(vec![0xAA, 0xBB]),
                recv_bytes: 321,
                timestamp_ms: 123,
                epoch: client.current_reader_epoch,
                apply_recv_effects: true,
                sliced_stats: Some(ReaderSlicedStats {
                    dup_count: 1,
                    blocks_count: 4,
                }),
                ping_update: None,
                handshake_update: None,
            });

        client.drain_reader_decoded(123, &mut mode);

        assert_eq!(delivered.load(Ordering::Relaxed), 1);
        assert!(
            !client
                .slicer
                .lock()
                .unwrap()
                .receiving
                .contains_key(&datagram_num),
            "Receiving entry must be removed after DataReadInt"
        );
        assert_eq!(client.avg_dup_count, 25.0);
        assert_eq!(client.total_recv, 321);
        assert_eq!(client.last_online, 123);
    }

    #[test]
    fn reader_decoded_grouped_payload_applies_recv_effects_once() {
        let mut client = Client::new(dummy_cfg());
        let mut grouped = Vec::new();
        grouped.push(Command::UI as u8);
        grouped.extend_from_slice(&1u16.to_le_bytes());
        grouped.push(0xAA);
        grouped.push(Command::Balance as u8);
        grouped.extend_from_slice(&1u16.to_le_bytes());
        grouped.push(0xBB);

        let decoded = Client::reader_decode_data_packets(
            &client.reader_protocol,
            Command::Grouped as u8,
            &grouped,
            77,
            456,
            client.current_reader_epoch,
        );
        assert_eq!(decoded.len(), 2);
        assert!(decoded[0].apply_recv_effects);
        assert!(!decoded[1].apply_recv_effects);
        client
            .pending_reader_decoded
            .lock()
            .unwrap()
            .extend(decoded);

        let delivered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let delivered_cb = Arc::clone(&delivered);
        let mut mode = RunMode::Callback {
            on_data: Box::new(move |cmd, payload| {
                match delivered_cb.load(Ordering::Relaxed) {
                    0 => {
                        assert_eq!(cmd, Command::UI);
                        assert_eq!(payload, &[0xAA]);
                    }
                    1 => {
                        assert_eq!(cmd, Command::Balance);
                        assert_eq!(payload, &[0xBB]);
                    }
                    _ => panic!("unexpected extra grouped payload"),
                }
                delivered_cb.fetch_add(1, Ordering::Relaxed);
            }),
        };

        client.drain_reader_decoded(456, &mut mode);

        assert_eq!(delivered.load(Ordering::Relaxed), 2);
        assert_eq!(client.total_recv, 77);
        assert_eq!(client.last_online, 456);
    }
}

#[cfg(test)]
mod service_cmd_tests {
    use super::*;
    use moonproto_transport::{
        outer_light_crypt, ClientMsgHeader, MacContext, ServerMsgHeader, TRANSPORT_VER,
    };

    static ERR_EMU_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct ErrEmuTestGuard {
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl Drop for ErrEmuTestGuard {
        fn drop(&mut self) {
            set_err_emu(0);
        }
    }

    fn err_emu_test_guard() -> ErrEmuTestGuard {
        let guard = ERR_EMU_TEST_LOCK.lock().unwrap();
        set_err_emu(0);
        ErrEmuTestGuard { _lock: guard }
    }

    fn test_recv_msg(cmd: Command) -> RecvMsg {
        RecvMsg {
            cmd: cmd as u8,
            payload: Vec::new(),
            recv_bytes: 0,
            timestamp_ms: 0,
            handled_in_reader: false,
            err_emu_drop: false,
            reader_dataread_core_done: false,
            epoch: 0,
        }
    }

    fn dummy_cfg_for_server(server_addr: SocketAddr) -> ClientConfig {
        ClientConfig {
            server_ip: server_addr.ip().to_string(),
            server_port: server_addr.port(),
            master_key: [0; 16],
            mac_key: [0x11; 16],
            mask_ver: 0,
            client_id: 0x1234_5678_9ABC_DEF0,
            ntp_host: None,
            refresh: RefreshConfig::default(),
        }
    }

    fn pack_server_packet(mac_key: &MoonKey, cmd: Command, payload: &[u8]) -> Vec<u8> {
        let hdr = ServerMsgHeader {
            rnd: 0x5A,
            checksum: 0,
            ver: TRANSPORT_VER,
            cmd: cmd as u8,
        };
        let mut buf = hdr.to_bytes().to_vec();
        buf.extend_from_slice(payload);
        let mac_ctx = MacContext::new(mac_key);
        let mac = mac_ctx.mac(&buf);
        buf[1..5].copy_from_slice(&mac.to_le_bytes());
        outer_light_crypt(&mut buf, mac_key);
        buf
    }

    fn unpack_client_packet(mac_key: &MoonKey, raw: &[u8]) -> (ClientMsgHeader, Vec<u8>) {
        const CLIENT_HDR_SIZE: usize = 15;
        let mut buf = raw.to_vec();
        outer_light_crypt(&mut buf, mac_key);
        let hdr = ClientMsgHeader::from_bytes(&buf).unwrap();
        let saved = [buf[1], buf[2], buf[3], buf[4]];
        buf[1..5].copy_from_slice(&0u32.to_le_bytes());
        let mac = MacContext::new(mac_key).mac(&buf);
        assert_eq!(mac, hdr.checksum);
        buf[1..5].copy_from_slice(&saved);
        (hdr, buf[CLIENT_HDR_SIZE..].to_vec())
    }

    fn recv_client_packet(
        server_sock: &UdpSocket,
        mac_key: &MoonKey,
    ) -> (ClientMsgHeader, Vec<u8>) {
        let mut ack_buf = [0u8; 2048];
        let (n, _from) = server_sock.recv_from(&mut ack_buf).unwrap();
        unpack_client_packet(mac_key, &ack_buf[..n])
    }

    fn spawn_reader_test_client() -> (UdpSocket, SocketAddr, Client) {
        let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        let server_addr = server_sock.local_addr().unwrap();

        let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        client_sock
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        let client_addr = client_sock.local_addr().unwrap();

        let mut client = Client::new(dummy_cfg_for_server(server_addr));
        client.socket = Some(client_sock);
        client.spawn_reader();

        (server_sock, client_addr, client)
    }

    fn pop_reader_decoded(client: &Client) -> ReaderDecodedMsg {
        let deadline = Instant::now() + Duration::from_secs(1);
        while client.pending_reader_decoded.lock().unwrap().is_empty() && Instant::now() < deadline
        {
            thread::sleep(Duration::from_millis(1));
        }
        client.pending_reader_decoded.lock().unwrap().pop().unwrap()
    }

    fn service_ping_payload(
        trip_delay: i32,
        pmtu: u16,
        global_timing_orders: u16,
        overheat: u8,
        rsq: u8,
    ) -> Vec<u8> {
        let mut payload = vec![0u8; 50];
        payload[16..20].copy_from_slice(&trip_delay.to_le_bytes());
        payload[20..22].copy_from_slice(&pmtu.to_le_bytes());
        payload[22..24].copy_from_slice(&global_timing_orders.to_le_bytes());
        payload[24] = overheat;
        payload[41] = rsq;
        payload
    }

    fn encrypted_server_hello(
        master_key: &MoonKey,
        client_id: u64,
        server_token: u64,
        peer_app_token: u64,
    ) -> Vec<u8> {
        let mut hello = handshake::Hello::new(0x1111, peer_app_token);
        hello.server_token = server_token;
        hello.app_token = peer_app_token;
        hello.timestamp = delphi_now();
        let aad = client_id.to_le_bytes();
        crypto::encrypt(master_key, &hello.to_bytes_packed(), &aad)
    }

    #[test]
    fn service_cmds_include_handshake_and_keepalive() {
        for cmd in [
            Command::Ping,
            Command::WantNewHello,
            Command::WrongHello,
            Command::WhoAreYou,
            Command::Fine,
            Command::NeedHelloAgain,
            Command::SizeTest,
            Command::ProbeMTU,
            Command::SlicedACK,
        ] {
            assert!(is_service_cmd(cmd as u8), "{cmd:?} must be service");
        }
    }

    #[test]
    fn data_channels_are_not_service_cmds() {
        for cmd in [
            Command::Order,
            Command::UI,
            Command::Strat,
            Command::API,
            Command::Balance,
            Command::TradesStream,
            Command::OrderBook,
        ] {
            assert!(!is_service_cmd(cmd as u8), "{cmd:?} must stay data");
        }
    }

    #[test]
    fn sliced_is_not_err_emu_service() {
        assert!(
            !is_service_cmd(Command::Sliced as u8),
            "ErrEmu must drop MPC_Sliced with the full configured rate like Delphi"
        );
    }

    #[test]
    fn recv_event_queue_has_no_capacity_cap_for_data_packets() {
        let (tx, rx) = mpsc::channel();

        for _ in 0..4096 {
            let result = enqueue_recv_event(&tx, test_recv_msg(Command::OrderBook), Instant::now());
            assert_eq!(result, RecvEnqueue::Delivered);
        }

        assert_eq!(rx.try_iter().count(), 4096);
    }

    #[test]
    fn recv_event_queue_has_no_capacity_cap_for_accepted_sliced_packets() {
        let (tx, rx) = mpsc::channel();

        for _ in 0..4096 {
            let result = enqueue_recv_event(&tx, test_recv_msg(Command::Sliced), Instant::now());
            assert_eq!(result, RecvEnqueue::Delivered);
        }

        match rx.recv_timeout(Duration::from_secs(1)).unwrap() {
            ClientEvent::Recv(msg) => assert_eq!(msg.cmd, Command::Sliced as u8),
            _ => panic!("expected Sliced Recv event"),
        }
        assert_eq!(rx.try_iter().count(), 4095);
    }

    #[test]
    fn reader_work_wake_is_coalesced_until_reader_queue_drain() {
        let (tx, rx) = mpsc::channel();
        let wake_pending = AtomicBool::new(false);

        for _ in 0..4096 {
            assert_eq!(
                notify_reader_work(&tx, &wake_pending),
                RecvEnqueue::Delivered
            );
        }

        match rx.recv_timeout(Duration::from_secs(1)).unwrap() {
            ClientEvent::Wake => {}
            _ => panic!("expected a single coalesced reader Wake"),
        }
        assert_eq!(
            rx.try_iter().count(),
            0,
            "reader-side DataReadInt progress must not create an empty Wake backlog"
        );

        wake_pending.store(false, Ordering::Release);
        assert_eq!(
            notify_reader_work(&tx, &wake_pending),
            RecvEnqueue::Delivered
        );
        match rx.recv_timeout(Duration::from_secs(1)).unwrap() {
            ClientEvent::Wake => {}
            _ => panic!("expected a new Wake after drain cleared the level flag"),
        }
    }

    #[test]
    fn reader_sends_sliced_ack_without_main_loop_tick() {
        let _err_emu_guard = err_emu_test_guard();
        let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        server_sock
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let server_addr = server_sock.local_addr().unwrap();

        let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        client_sock
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        let client_addr = client_sock.local_addr().unwrap();

        let mut client = Client::new(dummy_cfg_for_server(server_addr));
        client.socket = Some(client_sock);
        client.spawn_reader();

        let slice_payload = vec![
            0x2A,
            0x00, // DatagramNum = 42
            0x00, // BlockNum = 0
            0x00, // MaxBlockNum = 0
            Command::API as u8,
            0xDE,
            0xAD,
        ];
        let packet = pack_server_packet(&client.cfg.mac_key, Command::Sliced, &slice_payload);
        server_sock.send_to(&packet, client_addr).unwrap();

        let mut ack_buf = [0u8; 256];
        let (n, _from) = server_sock.recv_from(&mut ack_buf).unwrap();
        let (hdr, ack_payload) = unpack_client_packet(&client.cfg.mac_key, &ack_buf[..n]);

        client.reader_shutdown.store(true, Ordering::Relaxed);

        assert_eq!(hdr.cmd, Command::SlicedACK as u8);
        assert_eq!(ack_payload.len(), slicing::ACK256_WIRE_SIZE);
        assert_eq!(ack_payload[0] & 0x01, 0x01);
        assert_eq!(&ack_payload[32..34], &42u16.to_le_bytes());
        let decoded_deadline = Instant::now() + Duration::from_secs(1);
        while client.pending_reader_decoded.lock().unwrap().is_empty()
            && Instant::now() < decoded_deadline
        {
            thread::sleep(Duration::from_millis(1));
        }
        let decoded = client.pending_reader_decoded.lock().unwrap().pop().unwrap();
        assert_eq!(decoded.cmd, Command::API as u8);
        assert_eq!(decoded.payload.as_deref(), Some(&[0xDE, 0xAD][..]));
        assert!(
            !client.slicer.lock().unwrap().receiving.contains_key(&42),
            "reader must remove Receiving after DataReadInt core, before main-loop delivery"
        );
        let deadline = Instant::now() + Duration::from_secs(1);
        while client.total_sent() == 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        assert!(
            client.total_sent() > 0,
            "reader-side ACK must go to UDP immediately, before run_inner/write tick"
        );
    }

    #[test]
    fn reader_handles_sliced_ack_without_recv_event_backlog() {
        let _err_emu_guard = err_emu_test_guard();
        let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        let server_addr = server_sock.local_addr().unwrap();

        let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        client_sock
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        let client_addr = client_sock.local_addr().unwrap();

        let mut client = Client::new(dummy_cfg_for_server(server_addr));
        client.socket = Some(client_sock);
        client.spawn_reader();

        let datagram_num = 0x3344u16;
        let mut ack_payload = vec![0u8; slicing::ACK256_WIRE_SIZE];
        ack_payload[0] = 0b1010_0101;
        ack_payload[32..34].copy_from_slice(&datagram_num.to_le_bytes());
        let packet = pack_server_packet(&client.cfg.mac_key, Command::SlicedACK, &ack_payload);
        server_sock.send_to(&packet, client_addr).unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);
        while client.incoming_sliced_acks.lock().unwrap().is_empty() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        let ack = client.incoming_sliced_acks.lock().unwrap().pop().unwrap();
        let decoded = pop_reader_decoded(&client);
        client.reader_shutdown.store(true, Ordering::Relaxed);

        assert_eq!(ack.datagram_num, datagram_num);
        assert_eq!(ack.flags[0], 0b1010_0101);
        assert_eq!(decoded.cmd, Command::SlicedACK as u8);
        assert!(decoded.payload.is_none());
        assert!(decoded.apply_recv_effects);
        match client
            .event_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
        {
            ClientEvent::Wake => {}
            ClientEvent::Recv(_) => panic!("SlicedACK must not enter generic recv backlog"),
            #[cfg(test)]
            ClientEvent::Send(_) => panic!("unexpected send event"),
            _ => panic!("unexpected control event"),
        }
    }

    #[test]
    fn reader_handles_partial_sliced_without_recv_event_backlog() {
        let _err_emu_guard = err_emu_test_guard();
        let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        server_sock
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let server_addr = server_sock.local_addr().unwrap();

        let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        client_sock
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        let client_addr = client_sock.local_addr().unwrap();

        let mut client = Client::new(dummy_cfg_for_server(server_addr));
        client.socket = Some(client_sock);
        client.spawn_reader();

        let datagram_num = 43u16;
        let slice_payload = vec![
            datagram_num as u8,
            (datagram_num >> 8) as u8,
            0x00, // BlockNum = 0
            0x01, // MaxBlockNum = 1, so this packet is only a partial datagram
            Command::API as u8,
            0xCA,
            0xFE,
        ];
        let packet = pack_server_packet(&client.cfg.mac_key, Command::Sliced, &slice_payload);
        server_sock.send_to(&packet, client_addr).unwrap();

        let (hdr, ack_payload) = recv_client_packet(&server_sock, &client.cfg.mac_key);
        let decoded = pop_reader_decoded(&client);
        client.reader_shutdown.store(true, Ordering::Relaxed);

        assert_eq!(hdr.cmd, Command::SlicedACK as u8);
        assert_eq!(ack_payload.len(), slicing::ACK256_WIRE_SIZE);
        assert_eq!(ack_payload[0] & 0x01, 0x01);
        assert_eq!(&ack_payload[32..34], &datagram_num.to_le_bytes());
        assert!(
            client
                .slicer
                .lock()
                .unwrap()
                .receiving
                .contains_key(&datagram_num),
            "partial Sliced datagram must stay in Receiving until completed or cleaned"
        );
        assert_eq!(decoded.cmd, Command::Sliced as u8);
        assert!(decoded.payload.is_none());
        assert!(decoded.apply_recv_effects);
        match client
            .event_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
        {
            ClientEvent::Wake => {}
            ClientEvent::Recv(_) => panic!("partial Sliced must not enter generic recv backlog"),
            #[cfg(test)]
            ClientEvent::Send(_) => panic!("unexpected send event"),
            _ => panic!("unexpected control event"),
        }
    }

    #[test]
    fn reader_handles_size_test_without_main_loop_tick() {
        let _err_emu_guard = err_emu_test_guard();
        let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        server_sock
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let server_addr = server_sock.local_addr().unwrap();

        let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        client_sock
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        let client_addr = client_sock.local_addr().unwrap();

        let mut client = Client::new(dummy_cfg_for_server(server_addr));
        client.socket = Some(client_sock);
        client.spawn_reader();

        let size = 64u16;
        let packet_num = 9u16;
        let series = 0xBEEFu16;
        let mut size_test = Vec::new();
        size_test.extend_from_slice(&size.to_le_bytes());
        size_test.extend_from_slice(&packet_num.to_le_bytes());
        size_test.extend_from_slice(&series.to_le_bytes());
        let packet = pack_server_packet(&client.cfg.mac_key, Command::SizeTest, &size_test);
        server_sock.send_to(&packet, client_addr).unwrap();

        let (hdr, ack_payload) = recv_client_packet(&server_sock, &client.cfg.mac_key);
        let decoded = pop_reader_decoded(&client);
        client.reader_shutdown.store(true, Ordering::Relaxed);

        assert_eq!(hdr.cmd, Command::SizeAck as u8);
        assert_eq!(ack_payload.len(), size as usize);
        assert_eq!(&ack_payload[0..2], &size.to_le_bytes());
        assert_eq!(&ack_payload[4..6], &series.to_le_bytes());
        assert_eq!(
            client
                .reader_protocol
                .lock()
                .unwrap()
                .data_size_ack_series_num,
            series
        );
        assert_eq!(decoded.cmd, Command::SizeTest as u8);
        assert!(decoded.payload.is_none());
        assert!(decoded.apply_recv_effects);
        match client
            .event_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
        {
            ClientEvent::Wake => {}
            ClientEvent::Recv(_) => panic!("SizeTest must not enter generic recv backlog"),
            #[cfg(test)]
            ClientEvent::Send(_) => panic!("unexpected send event"),
            _ => panic!("unexpected control event"),
        }
    }

    #[test]
    fn reader_handles_probe_mtu_without_main_loop_tick() {
        let _err_emu_guard = err_emu_test_guard();
        let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        server_sock
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let server_addr = server_sock.local_addr().unwrap();

        let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        client_sock
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        let client_addr = client_sock.local_addr().unwrap();

        let mut client = Client::new(dummy_cfg_for_server(server_addr));
        client.socket = Some(client_sock);
        client.spawn_reader();

        let probe_id = 0x1234u16;
        let probe_index = 1u8;
        let test_size = 80u16;
        let mut probe = Vec::new();
        probe.extend_from_slice(&probe_id.to_le_bytes());
        probe.push(probe_index);
        probe.extend_from_slice(&test_size.to_le_bytes());
        let packet = pack_server_packet(&client.cfg.mac_key, Command::ProbeMTU, &probe);
        server_sock.send_to(&packet, client_addr).unwrap();

        let (hdr, ack_payload) = recv_client_packet(&server_sock, &client.cfg.mac_key);
        let decoded = pop_reader_decoded(&client);
        client.reader_shutdown.store(true, Ordering::Relaxed);

        assert_eq!(hdr.cmd, Command::ProbeMTUAck as u8);
        assert_eq!(ack_payload.len(), test_size as usize);
        assert_eq!(&ack_payload[0..2], &probe_id.to_le_bytes());
        assert_eq!(ack_payload[2], probe_index);
        assert_eq!(&ack_payload[3..5], &test_size.to_le_bytes());
        assert_eq!(decoded.cmd, Command::ProbeMTU as u8);
        assert!(decoded.payload.is_none());
        assert!(decoded.apply_recv_effects);
        match client
            .event_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
        {
            ClientEvent::Wake => {}
            ClientEvent::Recv(_) => panic!("ProbeMTU must not enter generic recv backlog"),
            #[cfg(test)]
            ClientEvent::Send(_) => panic!("unexpected send event"),
            _ => panic!("unexpected control event"),
        }
    }

    #[test]
    fn reader_handles_ping_response_without_main_loop_tick() {
        let _err_emu_guard = err_emu_test_guard();
        let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        server_sock
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let server_addr = server_sock.local_addr().unwrap();

        let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        client_sock
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        let client_addr = client_sock.local_addr().unwrap();

        let mut client = Client::new(dummy_cfg_for_server(server_addr));
        client.total_sent.store(777, Ordering::Relaxed);
        client.socket = Some(client_sock);
        client.spawn_reader();

        let ping = service_ping_payload(123, 8_224, 456, 7, 128);
        let packet = pack_server_packet(&client.cfg.mac_key, Command::Ping, &ping);
        server_sock.send_to(&packet, client_addr).unwrap();

        let (hdr, response) = recv_client_packet(&server_sock, &client.cfg.mac_key);
        let decoded = pop_reader_decoded(&client);
        client.reader_shutdown.store(true, Ordering::Relaxed);

        assert_eq!(hdr.cmd, Command::Ping as u8);
        assert_eq!(response.len(), 50);
        assert_eq!(
            u64::from_le_bytes(response[25..33].try_into().unwrap()),
            777
        );
        assert_eq!(
            u64::from_le_bytes(response[33..41].try_into().unwrap()),
            packet.len() as u64,
            "Delphi SendPing writes TotalRecvBytes after UDPRead counted the current packet"
        );
        assert_eq!(
            u64::from_le_bytes(response[42..50].try_into().unwrap()),
            2048,
            "empty MPSlider BuildAckHalf still writes the tail-half AckStart"
        );
        assert_eq!(decoded.cmd, Command::Ping as u8);
        assert_eq!(decoded.payload.as_deref(), Some(&ping[..]));
        assert!(decoded.apply_recv_effects);
        assert!(decoded.ping_update.is_some());
        match client
            .event_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
        {
            ClientEvent::Wake => {}
            ClientEvent::Recv(_) => panic!("Ping must not enter generic recv backlog"),
            #[cfg(test)]
            ClientEvent::Send(_) => panic!("unexpected send event"),
            _ => panic!("unexpected control event"),
        }

        let delivered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let delivered_cb = Arc::clone(&delivered);
        let mut mode = RunMode::Callback {
            on_data: Box::new(move |cmd, payload| {
                assert_eq!(cmd, Command::Ping);
                assert_eq!(payload, &ping[..]);
                delivered_cb.fetch_add(1, Ordering::Relaxed);
            }),
        };
        client.process_reader_decoded(decoded, 0, &mut mode);

        assert_eq!(delivered.load(Ordering::Relaxed), 1);
        assert_eq!(client.round_trip_delay_ms(), 123);
        assert_eq!(client.actual_pmtu(), 8_224);
        assert_eq!(client.global_timing_orders(), 456);
        assert_eq!(client.ping_count(), 1);
        assert_eq!(client.total_recv(), packet.len() as u64);
        assert!(!client.need_connect);
    }

    #[test]
    fn reader_handles_who_are_you_imfriend_without_main_loop_tick() {
        let _err_emu_guard = err_emu_test_guard();
        let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        server_sock
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let server_addr = server_sock.local_addr().unwrap();

        let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        client_sock
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        let client_addr = client_sock.local_addr().unwrap();

        let mut client = Client::new(dummy_cfg_for_server(server_addr));
        let token_before = client.client_token;
        let app_token = client.app_token;
        let server_token = 0x2222_3333_4444_5555;
        let peer_app_token = 0xAAAA_BBBB_CCCC_DDDD;
        client.socket = Some(client_sock);
        client.spawn_reader();

        let who = encrypted_server_hello(
            &client.cfg.master_key,
            client.cfg.client_id,
            server_token,
            peer_app_token,
        );
        let packet = pack_server_packet(&client.cfg.mac_key, Command::WhoAreYou, &who);
        server_sock.send_to(&packet, client_addr).unwrap();

        let (hdr1, imfriend1) = recv_client_packet(&server_sock, &client.cfg.mac_key);
        let (hdr2, imfriend2) = recv_client_packet(&server_sock, &client.cfg.mac_key);
        let decoded = pop_reader_decoded(&client);
        client.reader_shutdown.store(true, Ordering::Relaxed);

        assert_eq!(hdr1.cmd, Command::ImFriend as u8);
        assert_eq!(hdr2.cmd, Command::ImFriend as u8);
        assert_eq!(
            imfriend1, imfriend2,
            "Delphi sends the same prepared ImFriend payload twice with Sleep(32)"
        );
        let (encode_key, decode_key) =
            crypto::generate_sub_keys(&client.cfg.master_key, server_token);
        let aad = client.cfg.client_id.to_le_bytes();
        let decrypted = crypto::decrypt(&encode_key, &imfriend1, &aad)
            .expect("ImFriend decrypts with client encode key");
        let im = handshake::Hello::from_bytes(&decrypted).expect("valid ImFriend Hello");
        assert_eq!(im.mix_ts, token_before.wrapping_add(1));
        assert_eq!(im.app_token, app_token);

        assert_eq!(decoded.cmd, Command::WhoAreYou as u8);
        assert!(decoded.payload.is_none());
        assert!(decoded.apply_recv_effects);
        assert!(decoded.handshake_update.is_some());
        match client
            .event_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
        {
            ClientEvent::Wake => {}
            ClientEvent::Recv(_) => panic!("WhoAreYou must not wait in generic recv backlog"),
            #[cfg(test)]
            ClientEvent::Send(_) => panic!("unexpected send event"),
            _ => panic!("unexpected control event"),
        }

        let mut mode = RunMode::Callback {
            on_data: Box::new(|_, _| panic!("WhoAreYou must not deliver user payload")),
        };
        client.process_reader_decoded(decoded.clone(), decoded.timestamp_ms, &mut mode);
        assert_eq!(client.server_token, server_token);
        assert_eq!(client.peer_app_token, peer_app_token);
        assert_eq!(client.client_token, token_before.wrapping_add(1));
        assert_eq!(client.encode_key, encode_key);
        assert_eq!(client.decode_key, decode_key);
    }

    #[test]
    fn reader_handles_fine_auth_done_without_recv_event_backlog() {
        let _err_emu_guard = err_emu_test_guard();
        let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        let server_addr = server_sock.local_addr().unwrap();

        let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        client_sock
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        let client_addr = client_sock.local_addr().unwrap();

        let mut client = Client::new(dummy_cfg_for_server(server_addr));
        client.need_connect = true;
        client.waiting_hello = true;
        client.socket = Some(client_sock);
        client.spawn_reader();

        let fine =
            encrypted_server_hello(&client.cfg.master_key, client.cfg.client_id, 0x2222, 0x3333);
        let packet = pack_server_packet(&client.cfg.mac_key, Command::Fine, &fine);
        server_sock.send_to(&packet, client_addr).unwrap();

        let decoded = pop_reader_decoded(&client);
        client.reader_shutdown.store(true, Ordering::Relaxed);

        assert_eq!(decoded.cmd, Command::Fine as u8);
        assert!(decoded.payload.is_none());
        assert!(decoded.apply_recv_effects);
        assert!(decoded.handshake_update.is_some());
        match client
            .event_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
        {
            ClientEvent::Wake => {}
            ClientEvent::Recv(_) => panic!("Fine must not wait in generic recv backlog"),
            #[cfg(test)]
            ClientEvent::Send(_) => panic!("unexpected send event"),
            _ => panic!("unexpected control event"),
        }

        let mut mode = RunMode::Callback {
            on_data: Box::new(|_, _| panic!("Fine must not deliver user payload")),
        };
        client.process_reader_decoded(decoded.clone(), decoded.timestamp_ms, &mut mode);
        assert!(client.authorized);
        assert_eq!(client.auth_status, AuthStatus::AuthDone);
        assert!(!client.need_connect);
        assert!(!client.waiting_hello);
    }

    #[test]
    fn reader_handles_wrong_hello_without_recv_event_backlog() {
        let _err_emu_guard = err_emu_test_guard();
        let (server_sock, client_addr, mut client) = spawn_reader_test_client();
        client.auth_status = AuthStatus::Offline;

        let packet = pack_server_packet(&client.cfg.mac_key, Command::WrongHello, &[]);
        server_sock.send_to(&packet, client_addr).unwrap();

        let decoded = pop_reader_decoded(&client);
        client.reader_shutdown.store(true, Ordering::Relaxed);

        assert_eq!(decoded.cmd, Command::WrongHello as u8);
        assert!(decoded.handshake_update.is_some());
        match client
            .event_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
        {
            ClientEvent::Wake => {}
            ClientEvent::Recv(_) => panic!("WrongHello must not wait in generic recv backlog"),
            #[cfg(test)]
            ClientEvent::Send(_) => panic!("unexpected send event"),
            _ => panic!("unexpected control event"),
        }

        let mut mode = RunMode::Callback {
            on_data: Box::new(|_, _| panic!("WrongHello must not deliver user payload")),
        };
        client.process_reader_decoded(decoded.clone(), decoded.timestamp_ms, &mut mode);
        assert_eq!(client.auth_status, AuthStatus::Connected);
    }

    #[test]
    fn reader_handles_want_new_hello_without_recv_event_backlog() {
        let _err_emu_guard = err_emu_test_guard();
        let (server_sock, client_addr, mut client) = spawn_reader_test_client();
        client.authorized = true;
        client.need_connect = false;
        client.soft_reconnect = true;
        client.last_sent_hello = 12345;

        let packet = pack_server_packet(&client.cfg.mac_key, Command::WantNewHello, &[]);
        server_sock.send_to(&packet, client_addr).unwrap();

        let decoded = pop_reader_decoded(&client);
        client.reader_shutdown.store(true, Ordering::Relaxed);

        assert_eq!(decoded.cmd, Command::WantNewHello as u8);
        assert!(decoded.handshake_update.is_some());
        match client
            .event_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
        {
            ClientEvent::Wake => {}
            ClientEvent::Recv(_) => panic!("WantNewHello must not wait in generic recv backlog"),
            #[cfg(test)]
            ClientEvent::Send(_) => panic!("unexpected send event"),
            _ => panic!("unexpected control event"),
        }

        let mut mode = RunMode::Callback {
            on_data: Box::new(|_, _| panic!("WantNewHello must not deliver user payload")),
        };
        client.process_reader_decoded(decoded.clone(), decoded.timestamp_ms, &mut mode);
        assert_eq!(client.last_sent_hello, NEVER_SENT_MS);
        assert_eq!(client.auth_status, AuthStatus::Connected);
        assert!(!client.authorized);
        assert!(client.need_connect);
        assert!(!client.soft_reconnect);
    }

    #[test]
    fn reader_handles_need_hello_again_without_recv_event_backlog() {
        let _err_emu_guard = err_emu_test_guard();
        let (server_sock, client_addr, mut client) = spawn_reader_test_client();
        client.waiting_hello = false;
        client.last_sent_hello = 12345;

        let packet = pack_server_packet(&client.cfg.mac_key, Command::NeedHelloAgain, &[]);
        server_sock.send_to(&packet, client_addr).unwrap();

        let decoded = pop_reader_decoded(&client);
        client.reader_shutdown.store(true, Ordering::Relaxed);

        assert_eq!(decoded.cmd, Command::NeedHelloAgain as u8);
        assert!(decoded.handshake_update.is_some());
        match client
            .event_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
        {
            ClientEvent::Wake => {}
            ClientEvent::Recv(_) => panic!("NeedHelloAgain must not wait in generic recv backlog"),
            #[cfg(test)]
            ClientEvent::Send(_) => panic!("unexpected send event"),
            _ => panic!("unexpected control event"),
        }

        let mut mode = RunMode::Callback {
            on_data: Box::new(|_, _| panic!("NeedHelloAgain must not deliver user payload")),
        };
        client.process_reader_decoded(decoded.clone(), decoded.timestamp_ms, &mut mode);
        assert!(client.waiting_hello);
        assert_eq!(client.waiting_hello_start, decoded.timestamp_ms);
        assert_eq!(client.last_need_hello_again, decoded.timestamp_ms);
        assert_eq!(client.last_sent_hello, NEVER_SENT_MS);
    }

    #[test]
    fn reader_decodes_regular_data_without_recv_event_backlog() {
        let _err_emu_guard = err_emu_test_guard();
        let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        let server_addr = server_sock.local_addr().unwrap();

        let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        client_sock
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        let client_addr = client_sock.local_addr().unwrap();

        let mut client = Client::new(dummy_cfg_for_server(server_addr));
        client.socket = Some(client_sock);
        client.spawn_reader();

        let packet = pack_server_packet(&client.cfg.mac_key, Command::UI, &[0xAA, 0xBB]);
        server_sock.send_to(&packet, client_addr).unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);
        while client.pending_reader_decoded.lock().unwrap().is_empty() && Instant::now() < deadline
        {
            thread::sleep(Duration::from_millis(1));
        }
        client.reader_shutdown.store(true, Ordering::Relaxed);

        let decoded = client.pending_reader_decoded.lock().unwrap().pop().unwrap();
        assert_eq!(decoded.cmd, Command::UI as u8);
        assert_eq!(decoded.payload.as_deref(), Some(&[0xAA, 0xBB][..]));
        assert!(decoded.apply_recv_effects);
        match client
            .event_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
        {
            ClientEvent::Wake => {}
            ClientEvent::Recv(_) => panic!("regular data must not enter generic recv backlog"),
            #[cfg(test)]
            ClientEvent::Send(_) => panic!("unexpected send event"),
            _ => panic!("unexpected control event"),
        }
    }

    #[test]
    fn reader_err_emu_drop_updates_stats_without_recv_event_backlog() {
        let _err_emu_guard = err_emu_test_guard();
        set_err_emu(100);
        let server_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        let server_addr = server_sock.local_addr().unwrap();

        let client_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        client_sock
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        let client_addr = client_sock.local_addr().unwrap();

        let mut client = Client::new(dummy_cfg_for_server(server_addr));
        client.socket = Some(client_sock);
        client.spawn_reader();

        let packet = pack_server_packet(&client.cfg.mac_key, Command::UI, &[0xAA, 0xBB]);
        server_sock.send_to(&packet, client_addr).unwrap();

        let decoded = pop_reader_decoded(&client);
        client.reader_shutdown.store(true, Ordering::Relaxed);

        assert_eq!(decoded.cmd, Command::UI as u8);
        assert!(decoded.payload.is_none());
        assert!(decoded.apply_recv_effects);
        match client
            .event_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
        {
            ClientEvent::Wake => {}
            ClientEvent::Recv(_) => panic!("ErrEmu drop must not enter generic recv backlog"),
            #[cfg(test)]
            ClientEvent::Send(_) => panic!("unexpected send event"),
            _ => panic!("unexpected control event"),
        }

        let mut mode = RunMode::Callback {
            on_data: Box::new(|_, _| panic!("ErrEmu drop must not deliver user payload")),
        };
        client.process_reader_decoded(decoded.clone(), decoded.timestamp_ms, &mut mode);
        assert!(client.connected);
        assert_eq!(client.auth_status, AuthStatus::Connected);
        assert_eq!(client.total_recv, decoded.recv_bytes);
        assert_eq!(client.last_online, decoded.timestamp_ms);
    }

    #[test]
    fn datagram_too_large_errors_are_non_fatal_pmtu_feedback() {
        for code in [90, 10040] {
            let err = std::io::Error::from_raw_os_error(code);
            assert!(is_datagram_too_large_error(&err), "os error {code}");
        }
        let bsd_emsgsize = std::io::Error::from_raw_os_error(40);
        assert_eq!(
            is_datagram_too_large_error(&bsd_emsgsize),
            cfg!(any(
                target_os = "macos",
                target_os = "ios",
                target_os = "freebsd",
                target_os = "openbsd",
                target_os = "netbsd",
            )),
        );

        let permission = std::io::Error::from_raw_os_error(13);
        assert!(!is_datagram_too_large_error(&permission));
    }

    #[test]
    fn generic_send_error_logs_without_force_disconnect() {
        let mut client = Client::new(ClientConfig {
            server_ip: "127.0.0.1".to_string(),
            server_port: 3000,
            master_key: [0; 16],
            mac_key: [0; 16],
            mask_ver: 0,
            client_id: 0,
            ntp_host: None,
            refresh: RefreshConfig::default(),
        });
        client.socket = Some(std::net::UdpSocket::bind("127.0.0.1:0").unwrap());
        let incompatible_addr: SocketAddr = "[::1]:9".parse().unwrap();

        client.dispatch_send(Command::Ping as u8, &[0xAA], None, incompatible_addr);

        assert_eq!(client.total_sent(), 0, "IPv4 socket → IPv6 addr must fail");
        assert!(
            !client.force_disconnect,
            "Delphi send error only logs; it must not start reconnect"
        );
    }
}

#[cfg(test)]
mod reconnect_timing_tests {
    use super::*;
    use crate::commands::engine_api::EngineMethod;
    use crate::commands::market::build_markets_indexes_response;
    use crate::events::{Event, EventDispatcher};

    fn dummy_client() -> Client {
        Client::new(ClientConfig {
            server_ip: "127.0.0.1".to_string(),
            server_port: 3000,
            master_key: [0; 16],
            mac_key: [0; 16],
            mask_ver: 0,
            client_id: 0,
            ntp_host: None,
            refresh: RefreshConfig {
                update_markets_every: None,
                check_tags_every: None,
            },
        })
    }

    fn callback_sink<'a>(cb: &'a mut OnDataFn) -> DispatchSink<'a> {
        DispatchSink::Callback(cb)
    }

    fn install_session_key(client: &mut Client) {
        client.server_token = 1;
        client.encode_cipher = Some(crypto::cipher_from_key(&[0; 16]));
    }

    fn encrypted_hello(client: &Client, server_token: u64, peer_app_token: u64) -> Vec<u8> {
        let mut hello = handshake::Hello::new(client.client_token, client.app_token);
        hello.server_token = server_token;
        hello.app_token = peer_app_token;
        hello.timestamp = delphi_now();
        let aad = client.cfg.client_id.to_le_bytes();
        crypto::encrypt(&client.cfg.master_key, &hello.to_bytes_packed(), &aad)
    }

    fn method_id(payload: &[u8]) -> Option<u8> {
        payload.get(11).copied()
    }

    fn drain_send_items(client: &Client) -> Vec<SendItem> {
        let (mut sliced, mut high, mut low) = client.take_send_queues_for_test();
        sliced.append(&mut high);
        sliced.append(&mut low);
        sliced
    }

    fn api_methods(items: &[SendItem]) -> Vec<u8> {
        items
            .iter()
            .filter(|item| item.cmd == Command::API as u8)
            .filter_map(|item| method_id(&item.data))
            .collect()
    }

    fn build_engine_response_payload(
        request_uid: u64,
        method: EngineMethod,
        data: &[u8],
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(1u8);
        buf.extend_from_slice(&3u16.to_le_bytes());
        buf.extend_from_slice(&0xAABB_CCDD_u64.to_le_bytes());
        buf.extend_from_slice(&request_uid.to_le_bytes());
        buf.push(method as u8);
        buf.push(1u8);
        buf.extend_from_slice(&0i32.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes());
        buf.push(0u8);
        buf.extend_from_slice(&(data.len() as i32).to_le_bytes());
        buf.extend_from_slice(data);
        buf
    }

    #[test]
    fn want_new_hello_allows_immediate_hello_on_young_client_clock() {
        let mut client = dummy_client();
        let mut cb: OnDataFn = Box::new(|_, _| {});
        let mut sink = callback_sink(&mut cb);

        client.handle_udp_command(
            Command::WantNewHello,
            Command::WantNewHello as u8,
            &[],
            &mut sink,
            false,
        );

        assert_eq!(client.last_sent_hello, NEVER_SENT_MS);
        client.check_hello_send(100);

        assert_eq!(
            client.last_sent_hello, 100,
            "Delphi LastSentHello=0 означает немедленный retry; Rust Instant clock не должен ждать 2с",
        );
        assert!(client.waiting_hello);
    }

    #[test]
    fn early_hello_again_uses_master_key_before_whoareyou() {
        let mut client = dummy_client();
        let token_before = client.client_token;
        let payload = client.build_hello_again_packet();
        let aad = client.cfg.client_id.to_le_bytes();
        let decrypted = crypto::decrypt(&client.cfg.master_key, &payload, &aad)
            .expect("early HelloAgain must be encrypted with MasterKey");
        let hello = handshake::Hello::from_bytes(&decrypted).expect("valid HelloAgain payload");

        assert_eq!(client.client_token, token_before + 1);
        assert_eq!(hello.mix_ts, client.client_token);
        assert_eq!(
            hello.peer_mix,
            crypto::mix_values(&hello.rnd, hello.mix_ts, 0),
            "before WhoAreYou Delphi computes PeerMix with ServerToken=0",
        );
    }

    #[test]
    fn fine_requires_master_key_hello_payload_like_delphi() {
        let mut client = dummy_client();

        client.handle_handshake(Command::Fine, b"not an encrypted hello");

        assert!(!client.authorized);
        assert_ne!(client.auth_status, AuthStatus::AuthDone);

        let mut hello = handshake::Hello::new(client.client_token, client.app_token);
        hello.timestamp = delphi_now();
        let aad = client.cfg.client_id.to_le_bytes();
        let payload = crypto::encrypt(&client.cfg.master_key, &hello.to_bytes_packed(), &aad);

        client.handle_handshake(Command::Fine, &payload);

        assert!(client.authorized);
        assert_eq!(client.auth_status, AuthStatus::AuthDone);
        assert!(!client.need_connect);
    }

    #[test]
    fn first_fine_before_init_does_not_send_engine_api_or_restore_subscriptions() {
        let mut client = dummy_client();
        client.domain_ready = false;
        client.peer_app_token = 0xABCD;
        client.tracked_indexes_peer_app_token = 0;
        client.subscription_registry.trades_sub = Some(TradesSubscription { want_mm: true });
        client.subscription_registry.mm_orders_sub = Some(true);
        client
            .subscription_registry
            .orderbook_subs
            .insert("BTCUSDT".to_string());

        let mut hello = handshake::Hello::new(client.client_token, client.app_token);
        hello.timestamp = delphi_now();
        let aad = client.cfg.client_id.to_le_bytes();
        let payload = crypto::encrypt(&client.cfg.master_key, &hello.to_bytes_packed(), &aad);

        client.handle_handshake(Command::Fine, &payload);

        assert!(client.authorized);
        assert_eq!(client.auth_status, AuthStatus::AuthDone);
        assert!(
            client.drain_app_events_for_test().is_empty(),
            "first Fine must not restore; restore starts only after a completed Init session",
        );
    }

    #[test]
    fn post_init_reconnect_restores_domain_without_second_init_and_reopens_stream_gate() {
        let mut client = dummy_client();

        // Simulate a Client that already connected once and completed its single Init.
        client.domain_ready = true;
        client.was_ever_connected = true;
        client.auth_status = AuthStatus::AuthDone;
        client.prev_auth_status = AuthStatus::AuthDone;
        client.authorized = true;
        client.peer_app_token = 0x1000;
        client.tracked_indexes_peer_app_token = 0x1000;
        client.domain_restore = DomainRestoreIntent {
            fetch_indexes: true,
        };
        client.subscription_registry.trades_sub = Some(TradesSubscription { want_mm: false });
        client
            .subscription_registry
            .orderbook_subs
            .insert("BTCUSDT".to_string());

        let who = encrypted_hello(&client, 0x2222, 0x2000);
        client.handle_handshake(Command::WhoAreYou, &who);
        let fine = encrypted_hello(&client, 0x2222, 0x2000);
        client.handle_handshake(Command::Fine, &fine);

        assert!(client.authorized);
        assert_eq!(client.auth_status, AuthStatus::AuthDone);
        assert!(
            client.indexes_fetch_in_flight,
            "post-init reconnect must request fresh indexes without user re-running Init"
        );

        let sent = drain_send_items(&client);
        let methods = api_methods(&sent);
        assert!(
            methods.contains(&(EngineMethod::GetMarketsIndexes as u8)),
            "subscriptions need fresh indexes after reconnect"
        );
        assert!(
            methods.contains(&(EngineMethod::SubscribeAllTrades as u8)),
            "trades subscription must be replayed by the library"
        );
        assert!(
            methods.contains(&(EngineMethod::SubscribeOrderBook as u8)),
            "orderbook subscription must be replayed by the library"
        );
        assert!(
            !methods.contains(&(EngineMethod::BaseCheck as u8))
                && !methods.contains(&(EngineMethod::AuthCheck as u8))
                && !methods.contains(&(EngineMethod::GetMarketsList as u8))
                && !methods.contains(&(EngineMethod::GetMarketsBalanceFull as u8)),
            "reconnect restore is not a second Init"
        );
        assert!(
            sent.iter().all(|item| {
                item.cmd != Command::Order as u8
                    && item.cmd != Command::UI as u8
                    && item.cmd != Command::Balance as u8
                    && item.cmd != Command::Strat as u8
            }),
            "Delphi post-init resync is not repeated by the client on reconnect"
        );

        let response_data = build_markets_indexes_response(&["BTCUSDT".to_string()]);
        let response_payload =
            build_engine_response_payload(0x7777, EngineMethod::GetMarketsIndexes, &response_data);
        let mut buffered = Vec::new();
        {
            let mut sink = DispatchSink::Buffer(&mut buffered);
            client.data_read_int(Command::API as u8, &response_payload, &mut sink);
        }
        assert!(!client.indexes_fetch_in_flight);
        assert!(client.market_indexes_current_for_peer());
        let after_indexes_sent = drain_send_items(&client);
        let after_indexes_methods = api_methods(&after_indexes_sent);
        assert!(
            after_indexes_methods.contains(&(EngineMethod::UpdateMarketsList as u8)),
            "after reconnect index sync, library must refresh market prices like Delphi UpdateMarketsList"
        );

        let mut dispatcher = EventDispatcher::new();
        let mut out = Vec::new();
        let (cmd, payload) = buffered.pop().expect("API response must reach dispatcher");
        dispatcher.dispatch_into_active(cmd, &payload, client.now_ms(), &mut out, &mut client);
        assert!(
            dispatcher.markets().indexes_synchronized,
            "fresh GetMarketsIndexes response reopens indexed stream gate"
        );

        out.clear();
        dispatcher.dispatch_into_active(
            Command::OrderBook,
            &[],
            client.now_ms(),
            &mut out,
            &mut client,
        );
        assert!(
            out.iter().any(|ev| matches!(
                ev,
                Event::ParseFailed {
                    cmd: Command::OrderBook,
                    ..
                }
            )),
            "after index restore, orderbook packets reach parser instead of being silently gated"
        );
    }

    #[test]
    fn initial_waiting_hello_sends_hello_again_like_delphi() {
        let mut client = dummy_client();
        let token_before = client.client_token;

        client.check_hello_send(100);
        assert_eq!(client.last_sent_hello, 100);
        assert!(client.waiting_hello);

        client.check_offline_reconnect(350);

        assert_eq!(client.auth_status, AuthStatus::Offline);
        assert_eq!(
            client.last_sent_hello, 350,
            "Delphi sends HelloAgain 200ms after the first waiting Hello even before WhoAreYou",
        );
        assert_eq!(
            client.client_token,
            token_before + 2,
            "first Hello increments token once; early HelloAgain increments it again",
        );
    }

    #[test]
    fn need_hello_again_allows_immediate_retry_on_young_client_clock() {
        let mut client = dummy_client();
        install_session_key(&mut client);
        let mut cb: OnDataFn = Box::new(|_, _| {});
        let mut sink = callback_sink(&mut cb);

        client.handle_udp_command(
            Command::NeedHelloAgain,
            Command::NeedHelloAgain as u8,
            &[],
            &mut sink,
            false,
        );

        assert_eq!(client.last_sent_hello, NEVER_SENT_MS);
        client.check_offline_reconnect(100);

        assert_eq!(
            client.last_sent_hello, 100,
            "NeedHelloAgain должен обходить минимум 200мс после Delphi-сброса LastSentHello в ноль",
        );
        assert!(client.waiting_hello);
    }
}

/// Global NTP time offset (days). Set once at startup by ntp::get_best_ntp.
/// Matches Delphi GlobalMPTimeOffset.
static NTP_OFFSET_DAYS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Set the process-global NTP correction in seconds.
///
/// `ClientConfig::new` normally starts the managed NTP syncer automatically.
/// This function is exposed for tests and custom tools that manage time sync
/// outside the client.
pub fn set_ntp_offset(offset_seconds: f64) {
    let bits = (offset_seconds / 86400.0).to_bits();
    NTP_OFFSET_DAYS.store(bits, std::sync::atomic::Ordering::Relaxed);
}

fn current_utc_hour_slot() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .checked_div(3600)
        .unwrap_or(0) as i64
}

fn get_ntp_offset_days() -> f64 {
    f64::from_bits(NTP_OFFSET_DAYS.load(std::sync::atomic::Ordering::Relaxed))
}

/// Back-compat fallback для low-level `EventDispatcher::dispatch_into` callers
/// которые не привязали per-client `ServerTimeDelta` source. Рекомендуемый
/// active path auto-link'ает `EventDispatcher` к `Client::server_time_delta_handle`
/// через `dispatch_into_active` и **не использует** это global значение.
///
/// DEVIATION #23 закрыт: multi-Client больше не страдает от перезаписи —
/// каждый Client имеет свой `Arc<AtomicU64>` handle.
static SERVER_TIME_DELTA_DAYS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Установить fallback server_time_delta (в днях, как TDateTime).
/// Вызывается из `Client::handle_ping` (back-compat write); потребитель НЕ должен
/// вызывать напрямую — используй `client.server_time_delta_handle()` для multi-Client.
pub(crate) fn set_server_time_delta_global(delta_days: f64) {
    SERVER_TIME_DELTA_DAYS.store(delta_days.to_bits(), std::sync::atomic::Ordering::Relaxed);
}

/// Получить fallback server_time_delta (дни). Используется `EventDispatcher` когда
/// per-Client source не привязан (single-Client back-compat).
pub(crate) fn get_server_time_delta_global() -> f64 {
    f64::from_bits(SERVER_TIME_DELTA_DAYS.load(std::sync::atomic::Ordering::Relaxed))
}

/// Delphi raw `Now` as UTC TDateTime (days since 1899-12-30), without NTP offset.
/// Used for `ServerTimeDelta := Ping.InitialTime - Now`.
fn delphi_now_raw() -> f64 {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    25569.0 + secs / 86400.0
}

/// Delphi TDateTime corrected by NTP offset.
/// Matches: `Now - GlobalMPTimeZoneOffset + GlobalMPTimeOffset`.
/// We use UTC directly (no timezone offset needed — TDateTime in MoonProto = UTC).
///
/// **Clock-jump sanity check** (audit_robustness H6): SystemTime подвержен NTP step и
/// suspend/resume скачкам. Если wall-clock уходит от monotonic elapsed больше чем
/// на 60 секунд — поднимается process-wide generation; каждый Client выполнит
/// force reconnect один раз на это поколение. Сам результат возвращаем как есть —
/// иначе handshake/order timestamps будут противоречить серверу.
fn delphi_now() -> f64 {
    let now = delphi_now_raw() + get_ntp_offset_days();

    // Детектор скачка: сравниваем wall-clock delta с монотонным elapsed.
    // Обычный простой клиента >60с не считается clock-jump; NTP step и suspend,
    // где wall-clock ушёл относительно monotonic, поднимают process-wide generation.
    let mono_now = Instant::now();
    if let Ok(mut state) = CLOCK_JUMP_STATE.lock() {
        if let Some((prev_now, prev_mono)) = *state {
            if is_clock_jump(prev_now, prev_mono, now, mono_now) {
                let delta_secs = clock_jump_drift_secs(prev_now, prev_mono, now, mono_now);
                log::warn!(target: "moonproto::client",
                    "delphi_now clock jump detected: {:.1}s — forcing reconnect to re-sync handshake timestamps",
                    delta_secs);
                // audit_robustness H5: при clock-jump (NTP step / suspend-resume на mobile)
                // прежний handshake timestamp устарел; сервер reject'нёт hello по
                // anti-replay window. Без сброса клиент впадает в permanent retry loop с тем
                // же stale timestamp. Generation читается каждым Client отдельно, чтобы
                // multi-server процесс не терял сигнал после первого обработчика.
                CLOCK_JUMP_GENERATION.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        *state = Some((now, mono_now));
    }
    now
}

fn clock_jump_drift_secs(
    prev_wall_days: f64,
    prev_mono: Instant,
    now_wall_days: f64,
    now_mono: Instant,
) -> f64 {
    let wall_delta_secs = (now_wall_days - prev_wall_days) * 86400.0;
    let mono_delta_secs = now_mono.saturating_duration_since(prev_mono).as_secs_f64();
    wall_delta_secs - mono_delta_secs
}

fn is_clock_jump(
    prev_wall_days: f64,
    prev_mono: Instant,
    now_wall_days: f64,
    now_mono: Instant,
) -> bool {
    clock_jump_drift_secs(prev_wall_days, prev_mono, now_wall_days, now_mono).abs() > 60.0
}

/// Последнее wall-clock/monotonic значение `delphi_now` для детектора скачка часов.
static CLOCK_JUMP_STATE: std::sync::Mutex<Option<(f64, Instant)>> = std::sync::Mutex::new(None);

/// Process-global поколение скачка системных часов. Каждый Client хранит последнее
/// обработанное значение и делает force reconnect один раз на поколение.
static CLOCK_JUMP_GENERATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

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
///
/// IPv4 vs IPv6: option name на IPv6 socket'е другой — `IP_DONTFRAGMENT` (v4) НЕ работает
/// на AF_INET6, нужен `IPV6_DONTFRAG` (или `IPV6_MTU_DISCOVER` на Linux). Без этого dual-stack
/// клиент (Android/iOS) silently failед бы PMTU detection. См. rust_quality audit #5.
///
/// Return value setsockopt проверяется и при ошибке логируется warn (раньше silently
/// ignored — fingerprinting'у проблемы было не оставлено следов).
fn set_dont_fragment_for_socket(sock: &UdpSocket, enable: bool) {
    // Определяем IPv6 vs IPv4 по local address. Если local_addr вернул ошибку — fallback на IPv4
    // semantics (большая часть систем — IPv4 по умолчанию).
    let is_v6 = sock.local_addr().map(|a| a.is_ipv6()).unwrap_or(false);

    #[cfg(target_os = "windows")]
    {
        use std::os::windows::io::AsRawSocket;
        let raw = sock.as_raw_socket();
        let val: i32 = if enable { 1 } else { 0 };
        // IPPROTO_IP=0, IP_DONTFRAGMENT=14; IPPROTO_IPV6=41, IPV6_DONTFRAG=14 (Win 10+ same value).
        let (level, optname) = if is_v6 { (41, 14) } else { (0, 14) };
        let rc = unsafe {
            extern "system" {
                fn setsockopt(
                    s: usize,
                    level: i32,
                    optname: i32,
                    optval: *const i8,
                    optlen: i32,
                ) -> i32;
            }
            setsockopt(
                raw as usize,
                level,
                optname,
                &val as *const i32 as *const i8,
                4,
            )
        };
        if rc != 0 {
            log::warn!(target: "moonproto::client",
                "set_dont_fragment_for_socket: setsockopt(level={level}, optname={optname}, v6={is_v6}) failed rc={rc} (Windows); PMTU discovery may be inaccurate");
        }
    }
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        use std::os::fd::AsRawFd;
        let fd = sock.as_raw_fd();
        // IPv4: IPPROTO_IP=0, IP_MTU_DISCOVER=10, IP_PMTUDISC_DO=2 / DONT=0
        // IPv6: IPPROTO_IPV6=41, IPV6_MTU_DISCOVER=23, same PMTUDISC values
        let val: i32 = if enable { 2 } else { 0 };
        let (level, optname) = if is_v6 { (41, 23) } else { (0, 10) };
        let rc = unsafe {
            extern "C" {
                fn setsockopt(
                    s: i32,
                    level: i32,
                    optname: i32,
                    optval: *const i8,
                    optlen: u32,
                ) -> i32;
            }
            setsockopt(fd, level, optname, &val as *const i32 as *const i8, 4)
        };
        if rc != 0 {
            log::warn!(target: "moonproto::client",
                "set_dont_fragment_for_socket: setsockopt(level={level}, optname={optname}, v6={is_v6}) failed rc={rc} (Linux/Android); PMTU discovery may be inaccurate");
        }
    }
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        use std::os::fd::AsRawFd;
        let fd = sock.as_raw_fd();
        // IPv4: IPPROTO_IP=0, IP_DONTFRAG=28
        // IPv6: IPPROTO_IPV6=41, IPV6_DONTFRAG=62
        let val: i32 = if enable { 1 } else { 0 };
        let (level, optname) = if is_v6 { (41, 62) } else { (0, 28) };
        let rc = unsafe {
            extern "C" {
                fn setsockopt(
                    s: i32,
                    level: i32,
                    optname: i32,
                    optval: *const i8,
                    optlen: u32,
                ) -> i32;
            }
            setsockopt(fd, level, optname, &val as *const i32 as *const i8, 4)
        };
        if rc != 0 {
            log::warn!(target: "moonproto::client",
                "set_dont_fragment_for_socket: setsockopt(level={level}, optname={optname}, v6={is_v6}) failed rc={rc} (macOS/iOS); PMTU discovery may be inaccurate");
        }
    }
    #[cfg(not(any(
        target_os = "windows",
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    )))]
    {
        // Other platforms (BSD, etc.) — no-op для безопасности, PMTU discovery не работает.
        let _ = (sock, enable, is_v6);
    }
}
