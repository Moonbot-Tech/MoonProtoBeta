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
mod diagnostics;
mod init;
mod metrics;
mod protocol_core;
mod sender;
mod socket;
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

#[inline]
fn initial_retry_left(encrypted: bool, max_retries: i32) -> i32 {
    if encrypted {
        (max_retries - 1).max(0)
    } else {
        0
    }
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

    fn is_empty(&self) -> bool {
        self.sliced.is_empty() && self.high.is_empty() && self.low.is_empty()
    }
}

/// Delphi `TMoonProtoBaseNet.SendLock` shared state.
///
/// The writer snapshots `DataToSend*`, `ACKs`, and `TmpSlider` under one lock,
/// then performs all heavy protocol work outside it. Receive-side code may only
/// append/copy small already-decoded values here.
#[derive(Default)]
pub(crate) struct SendLockState {
    send_queues: SendQueues,
    incoming_sliced_acks: Vec<SlicedAck>,
    tmp_slider: Slider,
}

impl SendLockState {
    fn push_send_cmd_int(&mut self, item: SendItem) {
        self.send_queues.push_send_cmd_int(item);
    }

    fn take_send_snapshot(
        &mut self,
        sliced: &mut Vec<SendItem>,
        high: &mut Vec<SendItem>,
        low: &mut Vec<SendItem>,
        acks: &mut Vec<SlicedAck>,
    ) -> Option<Slider> {
        self.send_queues.take_into(sliced, high, low);
        acks.append(&mut self.incoming_sliced_acks);
        let recvd = self.copy_tmp_slider();
        recvd
    }

    fn push_sliced_ack(&mut self, ack: SlicedAck) {
        self.incoming_sliced_acks.push(ack);
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

    fn reset_tmp_slider(&mut self) {
        self.tmp_slider = Slider::new();
    }

    fn is_empty(&self) -> bool {
        self.send_queues.is_empty()
    }
}

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

    fn start_inline_reader_session(&mut self) {
        self.recv_slicer = slicing::SlicingReceiver::new();
        self.register_recv_poller();
    }

    fn clear_recv_poller(&mut self) {
        if let (Some(poller), Some(sock)) = (self.recv_poller.as_ref(), self.socket.as_ref()) {
            if let Err(e) = poller.delete(sock) {
                log::warn!(target: "moonproto::reader", "UDP poller delete failed: {e}");
            }
        }
        self.recv_poller = None;
        self.recv_events.clear();
    }

    fn register_recv_poller(&mut self) {
        self.clear_recv_poller();
        let Some(sock) = self.socket.as_ref() else {
            return;
        };
        if let Err(e) = sock.set_nonblocking(true) {
            log::warn!(target: "moonproto::reader", "set_nonblocking(true) failed: {e}");
            return;
        }
        let poller = match Poller::new() {
            Ok(poller) => poller,
            Err(e) => {
                log::warn!(target: "moonproto::reader",
                    "UDP poller create failed: {e}; falling back to 5ms nonblocking recv probe");
                return;
            }
        };
        // Safety: the client owns this UDP socket and deletes it from the
        // poller before replacing or dropping the socket.
        let add_result = unsafe { poller.add(sock, PollEvent::readable(1)) };
        if let Err(e) = add_result {
            log::warn!(target: "moonproto::reader",
                "UDP poller add failed: {e}; falling back to 5ms nonblocking recv probe");
            return;
        }
        self.recv_poller = Some(poller);
    }

    /// Public API: queue a command for sending through the owning client loop.
    ///
    /// The command is appended directly to the unbounded Delphi-style
    /// `DataToSend*` queue for its priority, separate from accepted UDP packets
    /// and receive-decoded delivery. This API has no local capacity-drop branch.
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
            cmd: cmd.to_byte(),
            encrypted,
            priority,
            retry_left: initial_retry_left(encrypted, max_retries),
            max_retries,
            msg_num: 0,
            last_sent_at: 0,
            u_key,
        };
        // Delphi `SendCmdInt`: append into DataToSend/DataToSendH/DataToSendL
        // under SendLock. The writer tick later copies those lists; raw sends do
        // not wait behind reader delivery.
        if let Err(err) = self.enqueue_send_item(item) {
            match err {
                SubscribeError::Disconnected => {
                    log::error!(target: "moonproto::client",
                        "send_cmd: send queues closed (client dropped?) — packet cmd={:?} priority={:?} dropped",
                        cmd, priority);
                }
                SubscribeError::DomainNotReady => {
                    log::warn!(target: "moonproto::client",
                        "send_cmd: domain gate is closed before InitDone — packet cmd={:?} priority={:?} dropped",
                        cmd, priority);
                }
            }
        }
    }

    fn enqueue_send_item(&self, item: SendItem) -> Result<(), SubscribeError> {
        if !self.app_queue_alive.load(Ordering::Relaxed) {
            return Err(SubscribeError::Disconnected);
        }
        if !self.domain_ready && !outgoing_allowed_before_domain_ready(item.cmd, &item.data) {
            return Err(SubscribeError::DomainNotReady);
        }
        self.send_lock.lock().unwrap().push_send_cmd_int(item);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn take_send_queues_for_test(
        &self,
    ) -> (Vec<SendItem>, Vec<SendItem>, Vec<SendItem>) {
        let mut sliced = Vec::new();
        let mut high = Vec::new();
        let mut low = Vec::new();
        self.send_lock
            .lock()
            .unwrap()
            .send_queues
            .take_into(&mut sliced, &mut high, &mut low);
        (sliced, high, low)
    }

    #[cfg(test)]
    pub(crate) fn with_subscription_registry<R>(
        &self,
        f: impl FnOnce(&SubscriptionRegistry) -> R,
    ) -> R {
        let registry = self.subscription_registry.lock().unwrap();
        f(&registry)
    }

    #[cfg(test)]
    pub(crate) fn with_subscription_registry_mut<R>(
        &self,
        f: impl FnOnce(&mut SubscriptionRegistry) -> R,
    ) -> R {
        let mut registry = self.subscription_registry.lock().unwrap();
        let result = f(&mut registry);
        self.refresh_subscription_summary(&registry);
        result
    }

    fn refresh_subscription_summary(&self, registry: &SubscriptionRegistry) {
        refresh_subscription_summary(
            &self.subscription_summary,
            &self.subscription_trades_scope,
            registry,
        );
    }

    /// Convenience: send an Engine API request (MPS_Sliced, encrypted, MaxRetries=6).
    /// Matches Delphi: `TEngineRequest` has explicit `MoonCmdPriority(MPS_Sliced)`,
    /// and `TCommandRegistry.InitRegistry` gives Sliced commands `MaxRetries=6`.
    pub fn send_api_request(&self, request_payload: &[u8]) {
        self.send_api_request_at(request_payload, self.now_ms());
    }

    fn mark_engine_request_queued_at(&self, request_payload: &[u8], now_ms: i64) {
        match engine_request_method(request_payload) {
            Some(EngineMethod::SubscribeAllTrades) => {
                self.last_trades_subscribe_request_ms
                    .store(now_ms, Ordering::Relaxed);
            }
            Some(EngineMethod::SubscribeOrderBook) => {
                self.last_orderbook_subscribe_request_ms
                    .store(now_ms, Ordering::Relaxed);
                self.last_orderbook_subscribe_request_uid.store(
                    engine_request_uid(request_payload).unwrap_or(NO_PENDING_ENGINE_REQUEST_UID),
                    Ordering::Relaxed,
                );
            }
            _ => {}
        }
    }

    fn send_api_request_at(&self, request_payload: &[u8], now_ms: i64) {
        self.mark_engine_request_queued_at(request_payload, now_ms);
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
    ///
    /// Before `domain_ready`, only the mandatory Init Engine API requests are
    /// queued. Other raw Engine API requests are rejected before `api_pending`
    /// registration; because this method is non-fallible, it returns a closed
    /// receiver in that case.
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
        if !self.domain_ready
            && !outgoing_allowed_before_domain_ready(Command::API.to_byte(), request_payload)
        {
            log::warn!(target: "moonproto::client",
                "send_api_request_async: domain gate is closed before InitDone — Engine API request uid={} method={:?} not queued",
                uid,
                engine_request_method(request_payload).unwrap_or(EngineMethod::None));
            let (_tx, rx) = mpsc::channel();
            return rx;
        }
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
        let auth = self.request_engine_parsed(
            dispatcher,
            &crate::commands::engine_request::auth_check(),
            timeout,
            parse_auth_check_response,
        )?;
        self.set_auth_info(auth.clone());
        Ok(auth)
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
    /// Pending slot lives until complete/error, session reset, another request
    /// with the same UID replaces it, or a one-shot caller timeout removes it.
    /// Delphi likewise does not cancel `CandlesRequestUID` when the UI wait
    /// loop stops after `Markets.LastChunkTime` timeout.
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
            Ok(merged) => {
                dispatcher.apply_candles_snapshot(&merged.markets);
                Ok(merged)
            }
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
            shared: Arc::new(ClientSenderShared {
                app_queue_alive: Arc::clone(&self.app_queue_alive),
                domain_ready: Arc::clone(&self.domain_ready_flag),
                send_lock: Arc::clone(&self.send_lock),
                subscription_registry: Arc::clone(&self.subscription_registry),
                subscription_summary: Arc::clone(&self.subscription_summary),
                subscription_trades_scope: Arc::clone(&self.subscription_trades_scope),
                server_update_sent: Arc::clone(&self.server_update_sent),
                last_trades_subscribe_request_ms: Arc::clone(
                    &self.last_trades_subscribe_request_ms,
                ),
                last_orderbook_subscribe_request_ms: Arc::clone(
                    &self.last_orderbook_subscribe_request_ms,
                ),
                last_orderbook_subscribe_request_uid: Arc::clone(
                    &self.last_orderbook_subscribe_request_uid,
                ),
            }),
            start: self._start,
        }
    }

    /// Hidden FireTest hook: when enabled, no outgoing datagrams are sent.
    ///
    /// Normal applications must not use this. The live FireTest uses it to make
    /// the MoonBot server stop hearing from this client, then verifies that the
    /// library reconnects and restores subscriptions after the flag is cleared.
    #[doc(hidden)]
    pub fn debug_set_outgoing_blackhole(&mut self, enabled: bool) {
        self.debug_outgoing_blackhole
            .store(enabled, Ordering::Relaxed);
    }

    /// Subscribe to the orderbook stream for one market name.
    ///
    /// This is a fire-and-forget convenience wrapper around
    /// `self.sender().subscribe_orderbook(...)`. It records the intent in the
    /// shared registry and appends the resulting wire request directly into the
    /// Delphi-style send queues; a warning is logged only if the client is gone.
    /// Use `client.sender().try_subscribe_orderbook(...)` when the caller needs
    /// explicit failure feedback.
    ///
    /// The subscription is stored in the registry. Before init, reconnect does
    /// not send it. After init, reconnect restores it automatically without a
    /// second init; after a server restart, replay waits for fresh
    /// `GetMarketsIndexes` for the current `PeerAppToken`, matching Delphi
    /// `CheckBookTopics`. The server resolves `market_name -> market_idx`, so
    /// callers may subscribe before `emk_GetMarketsList` has completed. The
    /// call is idempotent; futures and spot books are distinguished by incoming
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
    /// This clears the reconnect registry and sends one batched
    /// `emk_UnsubscribeOrderBook` request for the market names that were actually
    /// remembered. Prefer this high-level method over raw Engine API calls; the
    /// raw call does not update the registry and reconnect would restore stale
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

    /// Subscribe to all-trades on the wire, but keep retained Active Lib data
    /// only for selected markets.
    ///
    /// Empty `market_names` means all markets.
    pub fn subscribe_trades_for<I, S>(&self, want_mm: bool, market_names: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.sender().subscribe_trades_for(want_mm, market_names);
    }

    /// Unsubscribe from the all-trades stream and remove the registry intent.
    pub fn unsubscribe_all_trades(&self) {
        self.sender().unsubscribe_all_trades();
    }

    #[cfg(test)]
    fn outgoing_mm_orders_subscribe_intent(item: &SendItem) -> Option<bool> {
        if item.cmd != Command::UI.to_byte() || item.u_key.kind != UK_TURN_MM_DETECTION {
            return None;
        }
        if item.data.first().copied() != Some(5) {
            return None;
        }
        item.data.last().map(|v| *v != 0)
    }

    fn apply_mm_orders_subscribe_intent(&mut self, subscribe: bool) {
        let mut registry = self.subscription_registry.lock().unwrap();
        registry.mm_orders_sub = Some(subscribe);
        self.refresh_subscription_summary(&registry);
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
            || self.subscription_summary.trades_subscribed()
            || self.subscription_summary.has_orderbook_subs()
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

        let orderbooks_need_fresh_indexes = self.subscription_summary.has_orderbook_subs()
            && !self.market_indexes_current_for_peer();
        if orderbooks_need_fresh_indexes {
            self.restore_orderbooks_after_indexes = true;
        }

        if self.domain_restore_needs_indexes() {
            self.send_markets_indexes_restore_request(self.now_ms());
        }

        self.restore_registry_subscriptions_without_delayed_orderbooks(
            orderbooks_need_fresh_indexes,
            true,
        );
    }

    /// Batch restore helper for the subscription registry.
    ///
    /// OrderBook подписки отправляются одним `emk_SubscribeOrderBook` batch'ем:
    /// в Delphi wire request нет `OrderBookKind`, только список имён рынков.
    #[cfg(test)]
    fn restore_registry_subscriptions(&mut self) {
        self.restore_registry_subscriptions_without_delayed_orderbooks(false, false);
    }

    fn restore_registry_subscriptions_without_delayed_orderbooks(
        &mut self,
        delay_orderbooks: bool,
        delay_trades: bool,
    ) {
        let (trades_sub, mm_orders_sub, orderbook_subs) = {
            let registry = self.subscription_registry.lock().unwrap();
            (
                registry.trades_sub,
                registry.mm_orders_sub,
                registry.orderbook_subs.iter().cloned().collect::<Vec<_>>(),
            )
        };

        if let Some(sub) = trades_sub {
            if delay_trades {
                // Reconnect path is handled by `tick_trades_reconnect_sequence`:
                // Delphi does not just replay SubscribeAllTrades; it first sends
                // UnsubscribeAllTrades, waits 100ms, then subscribes again.
            } else {
                let want_mm = sub.want_mm;
                self.send_api_request(&crate::commands::engine_request::subscribe_all_trades(
                    want_mm,
                ));
                if let Some(mm_orders) = mm_orders_sub {
                    if mm_orders != want_mm {
                        self.send_mm_orders_subscribe_cmd(mm_orders);
                    }
                }
            }
        } else if let Some(subscribe) = mm_orders_sub {
            self.send_mm_orders_subscribe_cmd(subscribe);
        }
        if delay_orderbooks {
            return;
        }
        self.restore_orderbook_subscriptions_as_reconnect_batch(orderbook_subs, self.now_ms());
    }

    fn registry_trades_want_mm(&self) -> Option<bool> {
        let registry = self.subscription_registry.lock().unwrap();
        let sub = registry.trades_sub?;
        Some(sub.want_mm)
    }

    fn registry_trades_mm_orders_intent(&self) -> Option<bool> {
        let registry = self.subscription_registry.lock().unwrap();
        registry.mm_orders_sub
    }

    fn start_trades_reconnect_sequence(&mut self, now_ms: i64) {
        if self.registry_trades_want_mm().is_none() {
            return;
        }
        self.last_trades_reconnect_check_ms = now_ms;
        let payload = crate::commands::engine_request::unsubscribe_all_trades();
        let request_uid = engine_request_uid(&payload).unwrap_or(NO_PENDING_ENGINE_REQUEST_UID);
        self.send_api_request_at(&payload, now_ms);
        self.pending_trades_unsubscribe = Some(PendingTradesUnsubscribe {
            request_uid,
            sent_ms: now_ms,
        });
        self.pending_trades_resubscribe_after_ms = None;
    }

    fn tick_trades_reconnect_sequence(&mut self, now_ms: i64, trades_server_token: u64) {
        if !self.domain_ready {
            return;
        }

        let last_subscribe_request_ms = self
            .last_trades_subscribe_request_ms
            .load(Ordering::Relaxed);
        if last_subscribe_request_ms != NEVER_TIME_MS
            && (now_ms - last_subscribe_request_ms).abs()
                < crate::api_pending::DEFAULT_PENDING_TIMEOUT_MS
        {
            return;
        }

        if let Some(pending) = self.pending_trades_unsubscribe {
            if (now_ms - pending.sent_ms).abs() < crate::api_pending::DEFAULT_PENDING_TIMEOUT_MS {
                return;
            }
            self.pending_trades_unsubscribe = None;
            self.pending_trades_resubscribe_after_ms =
                Some(now_ms + TRADES_RECONNECT_RESUBSCRIBE_DELAY_MS);
            return;
        }

        if let Some(due_ms) = self.pending_trades_resubscribe_after_ms {
            if now_ms >= due_ms {
                self.pending_trades_resubscribe_after_ms = None;
                if let Some(want_mm) = self.registry_trades_want_mm() {
                    self.send_api_request_at(
                        &crate::commands::engine_request::subscribe_all_trades(want_mm),
                        now_ms,
                    );
                    if let Some(mm_orders) = self.registry_trades_mm_orders_intent() {
                        if mm_orders != want_mm {
                            self.send_mm_orders_subscribe_cmd(mm_orders);
                        }
                    }
                }
            }
            return;
        }

        if self.registry_trades_want_mm().is_none() || self.server_token == 0 {
            return;
        }
        if self.server_token == trades_server_token {
            return;
        }
        if (now_ms - self.last_trades_reconnect_check_ms).abs() < TRADES_RECONNECT_THROTTLE_MS {
            return;
        }
        self.start_trades_reconnect_sequence(now_ms);
    }

    fn close_trades_unsubscribe_wait_if_matches(&mut self, request_uid: u64) {
        let Some(pending) = self.pending_trades_unsubscribe else {
            return;
        };
        if pending.request_uid != request_uid {
            return;
        }
        self.pending_trades_unsubscribe = None;
        self.pending_trades_resubscribe_after_ms =
            Some(self.now_ms() + TRADES_RECONNECT_RESUBSCRIBE_DELAY_MS);
    }

    fn tick_orderbook_reconnect_sequence(&mut self, now_ms: i64) -> bool {
        if !self.domain_ready || self.server_token == 0 || !self.market_indexes_current_for_peer() {
            return false;
        }
        if self.server_token == self.subscribed_book_server_token {
            return false;
        }
        let last_subscribe_request_ms = self
            .last_orderbook_subscribe_request_ms
            .load(Ordering::Relaxed);
        if last_subscribe_request_ms != NEVER_TIME_MS
            && (now_ms - last_subscribe_request_ms).abs()
                < crate::api_pending::DEFAULT_PENDING_TIMEOUT_MS
        {
            return false;
        }
        if (now_ms - self.last_book_reconnect_check_ms).abs() < ORDERBOOK_RECONNECT_THROTTLE_MS {
            return false;
        }
        let orderbook_subs = {
            let registry = self.subscription_registry.lock().unwrap();
            registry.orderbook_subs.iter().cloned().collect::<Vec<_>>()
        };
        if orderbook_subs.is_empty() {
            return false;
        }

        self.restore_orderbook_subscriptions_as_reconnect_batch(orderbook_subs, now_ms)
    }

    fn restore_orderbook_subscriptions_as_reconnect_batch(
        &mut self,
        orderbook_subs: Vec<String>,
        now_ms: i64,
    ) -> bool {
        self.last_book_reconnect_check_ms = now_ms;
        match self.send_orderbook_subscribe_batch(orderbook_subs, now_ms) {
            Some(uid) => {
                self.pending_orderbook_resubscribe_uid = Some(uid);
                true
            }
            None => false,
        }
    }

    fn send_orderbook_subscribe_batch(
        &self,
        orderbook_subs: Vec<String>,
        now_ms: i64,
    ) -> Option<u64> {
        let refs: Vec<&str> = orderbook_subs.iter().map(String::as_str).collect();
        if !refs.is_empty() {
            let payload = crate::commands::engine_request::subscribe_order_book(&refs);
            let uid = engine_request_uid(&payload);
            self.send_api_request_at(&payload, now_ms);
            return uid;
        }
        None
    }

    fn close_orderbook_subscribe_wait_if_matches(&self, request_uid: u64) {
        if self
            .last_orderbook_subscribe_request_uid
            .load(Ordering::Relaxed)
            == request_uid
        {
            self.last_orderbook_subscribe_request_ms
                .store(NEVER_TIME_MS, Ordering::Relaxed);
            self.last_orderbook_subscribe_request_uid
                .store(NO_PENDING_ENGINE_REQUEST_UID, Ordering::Relaxed);
        }
    }

    fn restore_orderbook_subscriptions_from_registry(&mut self) {
        let orderbook_subs = {
            let registry = self.subscription_registry.lock().unwrap();
            registry.orderbook_subs.iter().cloned().collect::<Vec<_>>()
        };
        self.restore_orderbook_subscriptions_as_reconnect_batch(orderbook_subs, self.now_ms());
    }

    /// Flush subscription intents collected before the one-time Init opened
    /// `domain_ready`.
    ///
    /// `send_post_init_resync` already sends the current MM-orders flag, so this
    /// helper sends only stream subscriptions: all-trades and orderbooks.
    fn send_registry_subscriptions_after_init(&mut self) {
        if !self.domain_ready {
            return;
        }

        let (trades_sub, orderbook_subs) = {
            let registry = self.subscription_registry.lock().unwrap();
            (
                registry.trades_sub,
                registry.orderbook_subs.iter().cloned().collect::<Vec<_>>(),
            )
        };

        if let Some(sub) = trades_sub {
            let want_mm = sub.want_mm;
            self.send_api_request(&crate::commands::engine_request::subscribe_all_trades(
                want_mm,
            ));
            let mut registry = self.subscription_registry.lock().unwrap();
            registry.mm_orders_sub = Some(want_mm);
        }

        let refs: Vec<&str> = orderbook_subs.iter().map(String::as_str).collect();
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

    fn send_domain_cmd(
        &self,
        data: Vec<u8>,
        cmd: Command,
        priority: SendPriority,
        encrypted: bool,
        max_retries: i32,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        self.send_cmd(data, cmd, priority, encrypted, max_retries);
        true
    }

    fn send_domain_cmd_keyed(
        &self,
        data: Vec<u8>,
        cmd: Command,
        priority: SendPriority,
        encrypted: bool,
        max_retries: i32,
        u_key: UniqueKey,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        self.send_cmd_keyed(data, cmd, priority, encrypted, max_retries, u_key);
        true
    }

    fn send_trade(&self, payload: Vec<u8>, max_retries: i32) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        self.send_cmd(
            payload,
            Command::Order,
            SendPriority::High,
            true,
            max_retries,
        );
        true
    }

    /// `send_trade` с UniqueKey — для команд имеющих `[MoonCmdUnique(UK_*)]` атрибут.
    /// Старые pending команды с тем же UKey удаляются из `self.sending`/`self.pending_h`
    /// (matches Delphi SendCmdInt:780-785 + CheckSendingData).
    fn send_trade_keyed(&self, payload: Vec<u8>, max_retries: i32, u_key: UniqueKey) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        self.send_cmd_keyed(
            payload,
            Command::Order,
            SendPriority::High,
            true,
            max_retries,
            u_key,
        );
        true
    }

    fn send_order_cancel_request(&self, request: crate::state::orders::OrderCancelSend) {
        match request {
            crate::state::orders::OrderCancelSend::PendingReplaceThenCancel {
                ctx,
                market,
                price,
            } => {
                let replace = crate::commands::trade::build_order_replace(
                    ctx,
                    &market,
                    crate::commands::trade::OrderType::Buy,
                    price,
                );
                self.send_trade_keyed(replace, 3, UniqueKey::order_move(ctx.uid));
                let cancel = crate::commands::trade::build_order_cancel(
                    ctx,
                    &market,
                    0,
                    crate::commands::trade::OrderWorkerStatus::None,
                );
                self.send_trade_keyed(cancel, 3, UniqueKey::order_move(ctx.uid));
            }
            crate::state::orders::OrderCancelSend::Cancel {
                ctx,
                market,
                status,
            } => {
                let raw = crate::commands::trade::build_order_cancel(ctx, &market, 0, status);
                self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
            }
        }
    }

    fn send_panic_sell_request(&self, request: crate::state::orders::PanicSellSend) {
        let raw = crate::commands::trade::build_turn_panic_sell(
            request.ctx,
            &request.market,
            request.turn_on,
        );
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(request.ctx.uid));
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

    /// Delphi local replace request + `TOrderReplaceCommand` (CmdId=6,
    /// `UK_OrderMove`) with a new price.
    ///
    /// Requires the local `Orders` read model. The wrapper derives market route
    /// and order type from the local order and repeats the Delphi
    /// `ReplaceSentTime = 0` gate.
    pub fn replace_order(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        new_price: f64,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let Some((ctx, market, order_type, price)) =
            orders.send_replace_if_requested(uid, new_price, self.now_ms())
        else {
            return false;
        };
        let raw = crate::commands::trade::build_order_replace(ctx, &market, order_type, price);
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
        true
    }

    /// Replace an order already tracked by `EventDispatcher::orders()`.
    pub fn replace_tracked_order(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        new_price: f64,
    ) -> bool {
        self.replace_order(orders, uid, new_price)
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
            self.run_with_dispatcher_worker_queued(tick, dispatcher);
        }
    }

    /// Delphi local cancel request + `TOrderCancelCommand` (CmdId=10,
    /// `UK_OrderMove`) for one order.
    ///
    /// Requires the local `Orders` read model. The wrapper derives current
    /// status from the local order and clears the local request after queueing.
    pub fn cancel_order(&self, orders: &mut crate::state::Orders, uid: u64) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let Some(request) = orders.send_cancel_if_requested(uid, self.now_ms()) else {
            return false;
        };
        self.send_order_cancel_request(request);
        true
    }

    /// Cancel an order already tracked by `EventDispatcher::orders()`.
    pub fn cancel_tracked_order(&self, orders: &mut crate::state::Orders, uid: u64) -> bool {
        self.cancel_order(orders, uid)
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

    /// `TMoveAllSellsCommand` (CmdId=13), gated like Delphi active-client UI.
    ///
    /// The move mode, price, zone and side live in [`crate::commands::trade::MoveAllSellsParams`]
    /// to keep the public API resistant to swapped positional arguments.
    pub fn move_all_sells(
        &self,
        orders: &crate::state::Orders,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        params: crate::commands::trade::MoveAllSellsParams,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        if !orders.has_move_all_sells_candidate(market, params) {
            return false;
        }
        let raw = crate::commands::trade::build_move_all_sells(ctx, market, params);
        self.send_trade(raw, 3);
        true
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

    /// Delphi `SendStopsIfChanged` + `TOrderStopsUpdate` (CmdId=20,
    /// UK_OrderMove).
    ///
    /// Requires the local `Orders` read model: if the UID is unknown or the
    /// stop record did not change, Delphi would not put a packet on the wire.
    pub fn update_order_stops(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        stops: &crate::commands::trade::StopSettings,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let Some((ctx, market, status, stops)) = orders.send_stops_if_changed(uid, stops) else {
            return false;
        };
        let raw = crate::commands::trade::build_order_stops_update(ctx, &market, 0, status, &stops);
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
        true
    }

    /// Update stops for an order already tracked by `EventDispatcher::orders()`.
    pub fn update_tracked_order_stops(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        stops: &crate::commands::trade::StopSettings,
    ) -> bool {
        self.update_order_stops(orders, uid, stops)
    }

    /// Delphi `TOrdersWorkers.TurnPanicSell`: set panic sell for every local
    /// active sell order in `market_name`.
    pub fn turn_panic_sell(
        &self,
        orders: &mut crate::state::Orders,
        market_name: &str,
        turn_on: bool,
    ) -> usize {
        if !self.domain_ready_for_typed_send() {
            return 0;
        }
        let requests = orders.turn_panic_sell_by_market(market_name, turn_on);
        let queued = requests.len();
        for request in requests {
            self.send_panic_sell_request(request);
        }
        queued
    }

    /// Delphi `TOrdersWorkers.SwitchPanicSellByMarket` button semantics.
    pub fn switch_panic_sell_by_market(
        &self,
        orders: &mut crate::state::Orders,
        market_name: &str,
        turn_on: bool,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let (panic_sell_on, requests) = orders.switch_panic_sell_by_market(market_name, turn_on);
        for request in requests {
            self.send_panic_sell_request(request);
        }
        panic_sell_on
    }

    /// Delphi per-worker panic-sell flag + `TTurnPanicSellCommand` (CmdId=21,
    /// UK_OrderMove).
    pub fn turn_order_panic_sell(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        turn_on: bool,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let Some(request) = orders.send_panic_sell_if_changed(uid, turn_on) else {
            return false;
        };
        self.send_panic_sell_request(request);
        true
    }

    /// Toggle panic sell for an order already tracked by
    /// `EventDispatcher::orders()`.
    pub fn turn_tracked_order_panic_sell(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        turn_on: bool,
    ) -> bool {
        self.turn_order_panic_sell(orders, uid, turn_on)
    }

    /// Apply Delphi `SetImmuneClicks` locally and send `TSetImmuneCommand`
    /// (CmdId=22, `UK_ImmuneClicks`) for found active orders.
    ///
    /// The dedup UID is `sum(items[].uid)`, matching Delphi
    /// `TSetImmuneCommand.SetUKey`.
    pub fn set_immune(
        &self,
        orders: &mut crate::state::Orders,
        items: &[crate::commands::trade::ImmuneItem],
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let applied = orders.set_immune_clicks(items);
        if applied.is_empty() {
            return false;
        }
        let raw = crate::commands::trade::build_set_immune(rand::random(), &applied);
        let items_uid_sum: u64 = applied
            .iter()
            .fold(0u64, |acc, it| acc.wrapping_add(it.uid));
        self.send_trade_keyed(raw, 3, UniqueKey::immune_clicks(items_uid_sum));
        true
    }

    /// `TMoveAllBuysCommand` (CmdId=27), gated like Delphi active-client UI.
    pub fn move_all_buys(
        &self,
        orders: &crate::state::Orders,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        cmd_type: crate::commands::trade::MoveAllBuysCmdType,
        move_kind: crate::commands::trade::ReplaceMultiKind,
        price: f64,
        side: crate::commands::trade::FixedPosition,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        if !orders.has_move_all_buys_candidate(market, cmd_type, move_kind, side) {
            return false;
        }
        let raw = crate::commands::trade::build_move_all_buys(
            ctx, market, cmd_type, move_kind, price, side,
        );
        self.send_trade(raw, 3);
        true
    }

    /// Delphi `SendVStopIfChanged` + `TVStopUpdate` (CmdId=29, `UK_OrderMove`).
    ///
    /// Requires the local `Orders` read model: the wrapper derives the current
    /// worker status, mutates local VStop state, and queues nothing if the value
    /// did not change.
    pub fn update_vstop(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        vstop_on: bool,
        vstop_fixed: bool,
        vstop_level: f64,
        vstop_vol: f64,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let Some((ctx, market, params)) =
            orders.send_vstop_if_changed(uid, vstop_on, vstop_fixed, vstop_level, vstop_vol)
        else {
            return false;
        };
        let raw = crate::commands::trade::build_vstop_update(ctx, &market, 0, params);
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
        true
    }

    /// Update VStop for an order already tracked by `EventDispatcher::orders()`.
    pub fn update_tracked_order_vstop(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        vstop_on: bool,
        vstop_fixed: bool,
        vstop_level: f64,
        vstop_vol: f64,
    ) -> bool {
        self.update_vstop(orders, uid, vstop_on, vstop_fixed, vstop_level, vstop_vol)
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
        self.send_domain_cmd_keyed(
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
        self.send_domain_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// Request the current UI settings snapshot and wait for the next
    /// `TClientSettingsCommand` while pumping the UDP loop.
    ///
    /// This is the UI-channel counterpart to [`Self::run_until_response`] for
    /// Engine API calls. `TSettingsRequest` does not carry a request/response
    /// UID pair on the wire: Delphi answers by sending a fresh
    /// `TClientSettingsCommand`. The helper therefore waits until
    /// `EventDispatcher` observes the next applied settings snapshot; the
    /// snapshot UID is not required to change because the server may resend the
    /// current settings object unchanged. The low-level Delphi command is
    /// fire-and-forget, so this helper reissues `TSettingsRequest` every few
    /// seconds while waiting.
    pub fn request_client_settings(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        timeout: Duration,
    ) -> Result<crate::commands::ui::ClientSettingsCommand, mpsc::RecvTimeoutError> {
        const TICK: Duration = Duration::from_millis(50);

        let first_new_event = dispatcher.queued_event_count();
        let start = Instant::now();
        let mut next_request_at = start + Duration::from_millis(SETTINGS_HELPER_RETRY_PAUSE_MS);
        self.ui_settings_request();

        loop {
            if queued_client_settings_updated_since(dispatcher, first_new_event) {
                if let Some(settings) = dispatcher.settings().client_settings.as_ref() {
                    return Ok(settings.clone());
                }
            }

            let Some(remaining) = timeout_remaining(start, timeout) else {
                return Err(mpsc::RecvTimeoutError::Timeout);
            };

            let now = Instant::now();
            if now >= next_request_at {
                self.ui_settings_request();
                next_request_at = now + Duration::from_millis(SETTINGS_HELPER_RETRY_PAUSE_MS);
            }

            let tick = remaining.min(TICK);
            self.run_with_dispatcher_worker_queued(tick, dispatcher);
        }
    }

    /// Send `TStratStartStopCommand` (UI CmdId=3, High) to start or stop all
    /// strategies.
    pub fn ui_strat_start_stop(&self, is_start: bool) {
        let raw = crate::commands::ui::build_strat_start_stop(rand::random(), is_start);
        self.send_domain_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// Send `TStratStartStopCommandV2` (UI CmdId=4, High) with an explicit
    /// checked delta.
    ///
    /// Regular active-library callers should prefer
    /// `EventDispatcher::ui_strat_start_stop_v2`, which builds the delta from
    /// owned strategy state like Delphi `TStratStartStopCommandV2.Create`.
    pub fn ui_strat_start_stop_v2(
        &self,
        is_start: bool,
        items: &[crate::commands::strat::StratCheckedItem],
    ) {
        let raw = crate::commands::ui::build_strat_start_stop_v2(rand::random(), is_start, items);
        self.send_domain_cmd(raw, Command::UI, SendPriority::High, true, 3);
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
        if self.send_domain_cmd(raw, Command::UI, SendPriority::High, true, 3) {
            self.mark_server_update_sent();
        }
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
        self.send_domain_cmd(raw, Command::UI, SendPriority::Sliced, true, 6);
    }

    /// Send `TLevManageCommand` (UI CmdId=9, Sliced,
    /// `UK_LevManageSettings`) with leverage-management settings.
    pub fn ui_lev_manage(&self, cmd: &crate::commands::ui::LevManage) {
        let uid: u64 = rand::random();
        let raw = crate::commands::ui::build_lev_manage(uid, cmd);
        self.send_domain_cmd_keyed(
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
        self.send_domain_cmd(raw, Command::UI, SendPriority::Sliced, true, 6);
    }

    /// Send `TResetProfitCommand` (UI CmdId=11, High) to reset profit counters.
    pub fn ui_reset_profit(&self, kind: u8) {
        let raw = crate::commands::ui::build_reset_profit(rand::random(), kind);
        self.send_domain_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// Send `TArbActivateNotify` (UI CmdId=12, High) with an arbitration-valid
    /// timestamp.
    pub fn ui_arb_activate_notify(&self, arb_valid: f64) {
        let raw = crate::commands::ui::build_arb_activate_notify(rand::random(), arb_valid);
        self.send_domain_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// Send `TSwitchDexCommand` (UI CmdId=13, High, `UK_DexSwitch`).
    ///
    /// The DEX name is truncated to the Delphi 15-byte short-string payload.
    pub fn ui_switch_dex(&self, dex_name: &str) {
        let uid = rand::random();
        let raw = crate::commands::ui::build_switch_dex(uid, dex_name);
        if self.send_domain_cmd_keyed(
            raw,
            Command::UI,
            SendPriority::High,
            true,
            3,
            UniqueKey::dex_switch_for(uid),
        ) {
            self.mark_server_update_sent();
        }
    }

    /// Send `TSwitchSpotCommand` (UI CmdId=14, High, `UK_SpotSwitch`) to select
    /// the spot mode.
    pub fn ui_switch_spot(&self, spot_index: u8) {
        let uid = rand::random();
        let raw = crate::commands::ui::build_switch_spot(uid, spot_index);
        if self.send_domain_cmd_keyed(
            raw,
            Command::UI,
            SendPriority::High,
            true,
            3,
            UniqueKey::spot_switch_for(uid),
        ) {
            self.mark_server_update_sent();
        }
    }

    // ====================================================================
    //  High-level Strat wrappers (Command::Strat, encrypted=true)
    //  Покрывают MClient.SendStratCmd(T*Command.Create(...)) семантику Delphi.
    //  Аудит docs_api B-02: было 5 build_* функций без Client-обёрток.
    // ====================================================================

    /// Send `TStratSnapshotRequest` (Strat CmdId=1, High).
    ///
    /// Protocol/testing tool only: Delphi server ignores this command when it
    /// is received from a client. Normal active-library flow answers the server
    /// request through `EventDispatcher`.
    pub fn strat_snapshot_request(&self) {
        let raw = crate::commands::strat::build_snapshot_request(rand::random());
        self.send_domain_cmd(raw, Command::Strat, SendPriority::High, true, 3);
    }

    /// Send `TStratSchemaRequest` (Strat CmdId=7, High).
    ///
    /// Agreed active-library behavior: one-time Init requests the live Delphi
    /// strategy schema from the server and stores the decoded result in
    /// `EventDispatcher::strats().strategy_schema()`. Public callers normally
    /// read that state instead of sending this manually.
    pub fn strat_schema_request(&self) {
        let raw = crate::commands::strat::build_schema_request(rand::random());
        self.send_domain_cmd(raw, Command::Strat, SendPriority::High, true, 3);
    }

    fn send_strat_snapshot_command(&self, raw: Vec<u8>) {
        self.send_domain_cmd_keyed(
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
    /// and sends a valid CmdId=2 packet. `schema` must be the live
    /// `TStratSchema` fetched during Init.
    pub fn strat_send_snapshot_batch(
        &self,
        server_epoch: u64,
        full: bool,
        schema: &crate::commands::strategy_schema::StrategySchema,
        strategies: &[crate::commands::strategy_serializer::StrategySnapshot],
    ) {
        let uid: u64 = rand::random();
        let raw = crate::commands::strat::build_snapshot_from_strategies(
            uid,
            server_epoch,
            full,
            schema,
            strategies,
        );
        self.send_strat_snapshot_command(raw);
    }

    /// Send `TStratDelete` (Strat CmdId=3, High) for one strategy or folder.
    pub fn strat_delete(&self, strategy_id: u64, folder_path: &str) {
        let raw = crate::commands::strat::build_delete(rand::random(), strategy_id, folder_path);
        self.send_domain_cmd(raw, Command::Strat, SendPriority::High, true, 3);
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
        self.send_domain_cmd_keyed(
            raw,
            Command::Strat,
            SendPriority::High,
            true,
            3,
            UniqueKey::strat_sell_price_update(strategy_id),
        );
    }

    /// Send `TStratCheckedSync` (Strat CmdId=5, Sliced) with explicit checked
    /// items.
    ///
    /// `is_delta = false` sends a full list; `true` sends a delta.
    /// Regular active-library callers should prefer
    /// `EventDispatcher::send_strategy_checked_delta`, which builds Delphi
    /// `TStrategies.GetCheckedDelta` from owned strategy state.
    pub fn strat_checked_sync(
        &self,
        items: &[crate::commands::strat::StratCheckedItem],
        is_delta: bool,
    ) {
        let raw = crate::commands::strat::build_checked_sync(rand::random(), items, is_delta);
        self.send_domain_cmd(raw, Command::Strat, SendPriority::Sliced, true, 6);
    }

    /// Send `TStratCheckedEcho` (Strat CmdId=6, High) with explicit items.
    ///
    /// This is normally a server response path; public use is for protocol tools
    /// that already own the exact Delphi `Items` array.
    pub fn strat_checked_echo(&self, items: &[crate::commands::strat::StratCheckedItem]) {
        let raw = crate::commands::strat::build_checked_echo(rand::random(), items);
        self.send_domain_cmd(raw, Command::Strat, SendPriority::High, true, 3);
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
        self.send_domain_cmd(raw, Command::Balance, SendPriority::High, true, 3);
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
            self.run_with_dispatcher_worker_queued(tick, dispatcher);
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
    /// — ~30-100ns per call. На hot path receive loop (50K pps на пике TradesStream)
    /// это давало 1-5 мс/сек wasted CPU + потенциальный wall-clock jump при NTP-step
    /// (ломал бы diff'ы). `Instant::elapsed()` использует CLOCK_MONOTONIC (на Linux/Mac)
    /// либо QueryPerformanceCounter (Windows) — стабильный, ~5-20ns per call, не
    /// подвержен NTP-корректировкам.
    ///
    /// **Semantic change vs предыдущая версия:** возвращает ms since process start,
    /// не ms since UNIX_EPOCH. Все callers используют **diff** между двумя `now_ms()`,
    /// так что absolute-base разница не имеет значения.
    ///
    /// MUST use same time base everywhere (receive, send, slicing) —
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

    /// Run the client protocol loop for `duration`.
    /// Matches TMoonProtoUDPClient.Execute.
    pub fn run(&mut self, duration: Duration, on_data: OnDataFn) {
        // Low-level raw API для потребителей которым НЕ нужны active-library
        // auto-actions (RequestOrderBookFull, trades resend tail-check, и т.п.).
        // User callback выполняется через app queue, а не внутри protocol tick.
        let (app_tx, app_rx) = mpsc::channel::<RawAppEvent>();
        let lifecycle_pair = self.lifecycle_cb.take().map(|cb| {
            let (tx, rx) = mpsc::channel::<LifecycleEvent>();
            *self.lifecycle_app_tx.lock().unwrap() = Some(tx);
            (rx, cb)
        });
        let lifecycle_app_tx = Arc::clone(&self.lifecycle_app_tx);
        let mut restored_lifecycle_cb: Option<LifecycleFn> = None;
        thread::scope(|scope| {
            let lifecycle_handle = lifecycle_pair.map(|(rx, cb)| {
                scope.spawn(move || {
                    let mut cb = cb;
                    while let Ok(event) = rx.recv() {
                        cb(event);
                    }
                    cb
                })
            });
            let app_handle = scope.spawn(move || {
                let mut on_data = on_data;
                while let Ok((cmd, payload)) = app_rx.recv() {
                    on_data(cmd, &payload);
                }
            });
            {
                let mut mode = RunMode::CallbackQueue { app_tx };
                ProtocolCore { client: self }.run(duration, &mut mode);
            }
            *lifecycle_app_tx.lock().unwrap() = None;
            app_handle
                .join()
                .expect("moonproto app callback thread panicked");
            if let Some(handle) = lifecycle_handle {
                restored_lifecycle_cb = Some(
                    handle
                        .join()
                        .expect("moonproto lifecycle callback thread panicked"),
                );
            }
        });
        if restored_lifecycle_cb.is_some() {
            self.lifecycle_cb = restored_lifecycle_cb;
        }
    }

    /// Send LogOff and close socket. Call when done.
    /// Matches TMoonProtoBaseClient.Disconnect (Common.pas:290-298)
    pub fn disconnect(&mut self) {
        self.need_connect = false;
        self.force_disconnect = true;
        self.authorized = false;
        self.auth_status = AuthStatus::Base;
        self.set_domain_ready(false);
    }

    /// Active-library entry point: run the client with an integrated
    /// `EventDispatcher`.
    ///
    /// Unlike [`Self::run`], this method routes incoming payloads through
    /// `dispatcher.dispatch_into_active` and performs active-library work:
    ///   - orderbook corrupted-cache recovery sends `RequestOrderBookFull`
    ///     without surfacing a separate callback event;
    ///   - trades gap recovery checks after valid trades packets and sends
    ///     `TradesResend` batches;
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
        // Protocol loop owns transport only. The active-library dispatcher is
        // processed by a worker thread: this mirrors Delphi `TThread.Queue`
        // boundaries for heavy domain work and keeps user callbacks away from
        // UDP receive / ACK / retry progress.
        let sender = self.sender();
        let protocol_metrics = Arc::clone(&self.protocol_metrics);
        let trades_server_token_mirror = Arc::clone(&self.dispatcher_trades_server_token);
        let api_pending = Arc::clone(&self.api_pending);
        let (app_tx, app_rx) = mpsc::channel::<crate::events::Event>();
        let (work_tx, work_rx) = mpsc::channel::<DispatcherWorkItem>();
        let lifecycle_pair = self.lifecycle_cb.take().map(|cb| {
            let (tx, rx) = mpsc::channel::<LifecycleEvent>();
            *self.lifecycle_app_tx.lock().unwrap() = Some(tx);
            (rx, cb)
        });
        let lifecycle_app_tx = Arc::clone(&self.lifecycle_app_tx);
        let mut restored_lifecycle_cb: Option<LifecycleFn> = None;
        thread::scope(|scope| {
            let lifecycle_handle = lifecycle_pair.map(|(rx, cb)| {
                scope.spawn(move || {
                    let mut cb = cb;
                    while let Ok(event) = rx.recv() {
                        cb(event);
                    }
                    cb
                })
            });
            let app_handle = scope.spawn(move || {
                let mut on_event = on_event;
                while let Ok(event) = app_rx.recv() {
                    on_event(&event);
                }
            });
            let dispatcher_handle = scope.spawn(move || {
                run_dispatcher_worker(
                    work_rx,
                    dispatcher,
                    DispatcherEventFn::QueueToCallback(app_tx),
                    sender,
                    api_pending,
                    protocol_metrics,
                    trades_server_token_mirror,
                );
            });
            {
                let mut mode = RunMode::DispatcherWorker {
                    tx: work_tx,
                    payload_buf: Vec::with_capacity(4),
                };
                ProtocolCore { client: self }.run(duration, &mut mode);
            }
            *lifecycle_app_tx.lock().unwrap() = None;
            dispatcher_handle
                .join()
                .expect("moonproto dispatcher worker thread panicked");
            app_handle
                .join()
                .expect("moonproto app callback thread panicked");
            if let Some(handle) = lifecycle_handle {
                restored_lifecycle_cb = Some(
                    handle
                        .join()
                        .expect("moonproto lifecycle callback thread panicked"),
                );
            }
        });
        if restored_lifecycle_cb.is_some() {
            self.lifecycle_cb = restored_lifecycle_cb;
        }
    }

    /// Same as [`Self::run_with_dispatcher`], but the callback also receives an
    /// updated read-only [`crate::events::EventDispatcherSnapshot`].
    ///
    /// This is useful for UI events that carry only an id, such as
    /// `OrderEvent::Updated(uid)`: the callback can immediately read the
    /// current order from the state snapshot. The callback runs from the
    /// application callback queue and does not block protocol ACK/retry/send
    /// progress.
    pub fn run_with_dispatcher_state(
        &mut self,
        duration: Duration,
        dispatcher: &mut crate::events::EventDispatcher,
        on_event: EventWithStateFn,
    ) {
        let sender = self.sender();
        let protocol_metrics = Arc::clone(&self.protocol_metrics);
        let trades_server_token_mirror = Arc::clone(&self.dispatcher_trades_server_token);
        let api_pending = Arc::clone(&self.api_pending);
        let (app_tx, app_rx) = mpsc::channel::<StateAppEvent>();
        let (work_tx, work_rx) = mpsc::channel::<DispatcherWorkItem>();
        let lifecycle_pair = self.lifecycle_cb.take().map(|cb| {
            let (tx, rx) = mpsc::channel::<LifecycleEvent>();
            *self.lifecycle_app_tx.lock().unwrap() = Some(tx);
            (rx, cb)
        });
        let lifecycle_app_tx = Arc::clone(&self.lifecycle_app_tx);
        let mut restored_lifecycle_cb: Option<LifecycleFn> = None;
        thread::scope(|scope| {
            let lifecycle_handle = lifecycle_pair.map(|(rx, cb)| {
                scope.spawn(move || {
                    let mut cb = cb;
                    while let Ok(event) = rx.recv() {
                        cb(event);
                    }
                    cb
                })
            });
            let app_handle = scope.spawn(move || {
                let mut on_event = on_event;
                while let Ok((event, snapshot)) = app_rx.recv() {
                    on_event(&event, snapshot.as_ref());
                }
            });
            let dispatcher_handle = scope.spawn(move || {
                run_dispatcher_worker(
                    work_rx,
                    dispatcher,
                    DispatcherEventFn::QueueToStateCallback(app_tx),
                    sender,
                    api_pending,
                    protocol_metrics,
                    trades_server_token_mirror,
                );
            });
            {
                let mut mode = RunMode::DispatcherWorker {
                    tx: work_tx,
                    payload_buf: Vec::with_capacity(4),
                };
                ProtocolCore { client: self }.run(duration, &mut mode);
            }
            *lifecycle_app_tx.lock().unwrap() = None;
            dispatcher_handle
                .join()
                .expect("moonproto dispatcher worker thread panicked");
            app_handle
                .join()
                .expect("moonproto app callback thread panicked");
            if let Some(handle) = lifecycle_handle {
                restored_lifecycle_cb = Some(
                    handle
                        .join()
                        .expect("moonproto lifecycle callback thread panicked"),
                );
            }
        });
        if restored_lifecycle_cb.is_some() {
            self.lifecycle_cb = restored_lifecycle_cb;
        }
    }

    #[cfg(test)]
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

    fn run_with_dispatcher_worker_queued(
        &mut self,
        duration: Duration,
        dispatcher: &mut crate::events::EventDispatcher,
    ) {
        let sender = self.sender();
        let protocol_metrics = Arc::clone(&self.protocol_metrics);
        let trades_server_token_mirror = Arc::clone(&self.dispatcher_trades_server_token);
        let api_pending = Arc::clone(&self.api_pending);
        let (work_tx, work_rx) = mpsc::channel::<DispatcherWorkItem>();
        let lifecycle_pair = self.lifecycle_cb.take().map(|cb| {
            let (tx, rx) = mpsc::channel::<LifecycleEvent>();
            *self.lifecycle_app_tx.lock().unwrap() = Some(tx);
            (rx, cb)
        });
        let lifecycle_app_tx = Arc::clone(&self.lifecycle_app_tx);
        let mut restored_lifecycle_cb: Option<LifecycleFn> = None;
        thread::scope(|scope| {
            let lifecycle_handle = lifecycle_pair.map(|(rx, cb)| {
                scope.spawn(move || {
                    let mut cb = cb;
                    while let Ok(event) = rx.recv() {
                        cb(event);
                    }
                    cb
                })
            });
            let dispatcher_handle = scope.spawn(move || {
                run_dispatcher_worker(
                    work_rx,
                    dispatcher,
                    DispatcherEventFn::Queue,
                    sender,
                    api_pending,
                    protocol_metrics,
                    trades_server_token_mirror,
                );
            });
            {
                let mut mode = RunMode::DispatcherWorker {
                    tx: work_tx,
                    payload_buf: Vec::with_capacity(4),
                };
                ProtocolCore { client: self }.run(duration, &mut mode);
            }
            *lifecycle_app_tx.lock().unwrap() = None;
            dispatcher_handle
                .join()
                .expect("moonproto dispatcher worker thread panicked");
            if let Some(handle) = lifecycle_handle {
                restored_lifecycle_cb = Some(
                    handle
                        .join()
                        .expect("moonproto lifecycle callback thread panicked"),
                );
            }
        });
        if restored_lifecycle_cb.is_some() {
            self.lifecycle_cb = restored_lifecycle_cb;
        }
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
        let sender = self.sender();
        let protocol_metrics = Arc::clone(&self.protocol_metrics);
        let trades_server_token_mirror = Arc::clone(&self.dispatcher_trades_server_token);
        let api_pending = Arc::clone(&self.api_pending);
        let (work_tx, work_rx) = mpsc::channel::<DispatcherWorkItem>();
        let lifecycle_pair = self.lifecycle_cb.take().map(|cb| {
            let (tx, rx) = mpsc::channel::<LifecycleEvent>();
            *self.lifecycle_app_tx.lock().unwrap() = Some(tx);
            (rx, cb)
        });
        let lifecycle_app_tx = Arc::clone(&self.lifecycle_app_tx);
        let mut restored_lifecycle_cb: Option<LifecycleFn> = None;
        let mut result: Option<Result<T, mpsc::RecvTimeoutError>> = None;

        thread::scope(|scope| {
            let lifecycle_handle = lifecycle_pair.map(|(rx, cb)| {
                scope.spawn(move || {
                    let mut cb = cb;
                    while let Ok(event) = rx.recv() {
                        cb(event);
                    }
                    cb
                })
            });
            let dispatcher_handle = scope.spawn(move || {
                run_dispatcher_worker(
                    work_rx,
                    dispatcher,
                    DispatcherEventFn::Queue,
                    sender,
                    api_pending,
                    protocol_metrics,
                    trades_server_token_mirror,
                );
            });
            {
                let barrier_tx = work_tx.clone();
                let mut mode = RunMode::DispatcherWorker {
                    tx: work_tx,
                    payload_buf: Vec::with_capacity(4),
                };
                loop {
                    match rx.try_recv() {
                        Ok(resp) => {
                            wait_dispatcher_worker_barrier(&barrier_tx);
                            result = Some(Ok(resp));
                            break;
                        }
                        Err(mpsc::TryRecvError::Disconnected) => {
                            result = Some(Err(mpsc::RecvTimeoutError::Disconnected));
                            break;
                        }
                        Err(mpsc::TryRecvError::Empty) => {}
                    }
                    let Some(remaining) = timeout_remaining(start, timeout) else {
                        result = Some(Err(mpsc::RecvTimeoutError::Timeout));
                        break;
                    };
                    let tick = remaining.min(Duration::from_millis(DELPHI_SEND_AND_WAIT_POLL_MS));
                    ProtocolCore { client: self }.run(tick, &mut mode);
                }
            }
            *lifecycle_app_tx.lock().unwrap() = None;
            dispatcher_handle
                .join()
                .expect("moonproto dispatcher worker thread panicked");
            if let Some(handle) = lifecycle_handle {
                restored_lifecycle_cb = Some(
                    handle
                        .join()
                        .expect("moonproto lifecycle callback thread panicked"),
                );
            }
        });
        if restored_lifecycle_cb.is_some() {
            self.lifecycle_cb = restored_lifecycle_cb;
        }
        result.expect("run_until_response loop must always set result")
    }

    /// Test-only inline dispatcher oracle. Production active-library paths use
    /// `DispatcherWorker`; this remains only for focused unit tests that need a
    /// synchronous dispatcher without spawning worker/app queues.
    #[cfg(test)]
    fn run_inner(&mut self, duration: Duration, mut mode: RunMode<'_>) {
        let lifecycle_pair = self.lifecycle_cb.take().map(|cb| {
            let (tx, rx) = mpsc::channel::<LifecycleEvent>();
            *self.lifecycle_app_tx.lock().unwrap() = Some(tx);
            (rx, cb)
        });
        let lifecycle_app_tx = Arc::clone(&self.lifecycle_app_tx);
        let mut restored_lifecycle_cb: Option<LifecycleFn> = None;
        thread::scope(|scope| {
            let lifecycle_handle = lifecycle_pair.map(|(rx, cb)| {
                scope.spawn(move || {
                    let mut cb = cb;
                    while let Ok(event) = rx.recv() {
                        cb(event);
                    }
                    cb
                })
            });
            ProtocolCore { client: self }.run(duration, &mut mode);
            *lifecycle_app_tx.lock().unwrap() = None;
            if let Some(handle) = lifecycle_handle {
                restored_lifecycle_cb = Some(
                    handle
                        .join()
                        .expect("moonproto lifecycle callback thread panicked"),
                );
            }
        });
        if restored_lifecycle_cb.is_some() {
            self.lifecycle_cb = restored_lifecycle_cb;
        }
    }

    #[cfg(test)]
    pub(crate) fn apply_active_actions<I>(&self, actions: I)
    where
        I: IntoIterator<Item = crate::events::ActiveAction>,
    {
        if !self.domain_ready_for_typed_send() {
            return;
        }
        for action in actions {
            match action {
                crate::events::ActiveAction::RequestMarketsList => {
                    self.send_api_request(&crate::commands::engine_request::get_markets_list());
                }
                crate::events::ActiveAction::RequestUpdateMarketsList => {
                    self.send_api_request(&crate::commands::engine_request::update_markets_list());
                }
                crate::events::ActiveAction::RequestStrategySchema => {
                    self.strat_schema_request();
                }
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
                crate::events::ActiveAction::OrderCancel { request } => {
                    self.send_order_cancel_request(request);
                }
                crate::events::ActiveAction::TradesResend { payload } => {
                    self.send_api_request(&payload);
                }
            }
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

    pub(crate) fn subscribed_book_server_token(&self) -> u64 {
        self.subscribed_book_server_token
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
