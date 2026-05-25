//! Accumulating live stress client for MoonProto.
//!
//! Run two independent client instances against one server and keep several
//! subscriptions plus Engine API, Binance tags checks, API-key expiration checks,
//! and chunked candles requests in flight at the same time. If order state is
//! present, the client also keeps safe tracked-order status refreshes in flight:
//! the final verdict is a protocol-health gate, not just "process did not crash".
//! It checks request success, latency, throughput, TradesStream gap recovery,
//! Sliced/backlog pressure, and payload sanity.
//!
//!   cargo run --example stress_client --release -- "<key_base64>" "207.148.91.186:3000" BTCUSDT 180 0 post_init
//!
//! Arguments:
//! - key_base64: exported MoonBot key.
//! - host:port: server address, default 207.148.91.186:3000.
//! - market: market used for orderbook/candles, default BTCUSDT.
//! - duration_secs: load phase duration after init, default 180.
//! - err_emu_pct: optional client-side incoming packet drop percent, default 0.
//! - err_emu_phase: `post_init` (default) enables loss after both clients finish init;
//!   `pre_connect` enables loss before handshake to stress authorization/reconnect.

use std::collections::{HashMap, VecDeque};
use std::env;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Condvar, Mutex,
};
use std::thread;
use std::time::{Duration, Instant};

use moonproto::client::{Client, ClientConfig, ClientSender, LifecycleEvent, MergedCandles};
use moonproto::commands::candles::{parse_coin_card_candles_response, DeepHistoryKind};
use moonproto::commands::engine_api::{
    parse_api_expiration_time_response, EngineMethod, EngineResponse, ServerInfo,
};
use moonproto::commands::market::parse_token_tags_response;
use moonproto::commands::strategy_serializer::parse_strategy_batch;
use moonproto::commands::ui::ClientSettingsCommand;
use moonproto::commands::{parse_get_balance_response, parse_query_hedge_mode_response};
use moonproto::events::{Event, EventDispatcher};
use moonproto::key_import;
use moonproto::state::{OrderBookEvent, StratEvent, TradesEvent};
use moonproto::{run_init_sequence, InitConfig};

const DEFAULT_HOST: &str = "207.148.91.186:3000";
const DEFAULT_MARKET: &str = "BTCUSDT";
const DEFAULT_DURATION_SECS: u64 = 180;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const INIT_TIMEOUT: Duration = Duration::from_secs(12);
const TICK: Duration = Duration::from_millis(250);
const API_WARN_TIMEOUT: Duration = Duration::from_secs(20);
const API_HARD_TIMEOUT: Duration = Duration::from_secs(90);
const CANDLES_WARN_TIMEOUT: Duration = Duration::from_secs(35);
const CANDLES_HARD_TIMEOUT: Duration = Duration::from_secs(90);
const HELPER_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_PENDING_API: usize = 48;
const MAX_PENDING_CANDLES: usize = 4;
const TRACKED_STATUS_BATCH: usize = 4;
const PROTOCOL_MIN_RECV_MBPS: f64 = 0.50;
const PROTOCOL_MAX_API_LATENCY_MS: u64 = 20_000;
const PROTOCOL_MAX_CANDLES_LATENCY_MS: u64 = 35_000;
const PROTOCOL_MAX_TRADES_BUCKETS: u64 = 40;
const PROTOCOL_MAX_TRADES_LOSS_FACTOR: f64 = 3.0;

#[derive(Default)]
struct GapPacketDiag {
    first_missing_ms: u64,
    resend_requests: u8,
    last_request_ms: Option<u64>,
    closed_ms: Option<u64>,
    closed_retry_count: Option<u8>,
    applied_ms: Option<u64>,
    applied_before_close: bool,
    applied_late_after_close: bool,
}

#[derive(Default)]
struct TradesGapDiagnostics {
    packets: HashMap<u16, GapPacketDiag>,
    closed_untracked_packets: u64,
}

impl TradesGapDiagnostics {
    fn on_gap_detected(&mut self, start: u16, end: u16, now_ms: u64) {
        let mut packet = start;
        loop {
            self.packets.entry(packet).or_insert(GapPacketDiag {
                first_missing_ms: now_ms,
                ..GapPacketDiag::default()
            });
            if packet == end {
                break;
            }
            packet = packet.wrapping_add(1);
        }
    }

    fn on_resend_requested(&mut self, packet_nums: &[u16], now_ms: u64) {
        for &packet in packet_nums {
            let entry = self.packets.entry(packet).or_insert(GapPacketDiag {
                first_missing_ms: now_ms,
                ..GapPacketDiag::default()
            });
            entry.resend_requests = entry.resend_requests.saturating_add(1);
            entry.last_request_ms = Some(now_ms);
        }
    }

    fn on_bucket_closed(&mut self, start: u16, end: u16, retry_count: u8, now_ms: u64) {
        let mut packet = start;
        loop {
            if let Some(entry) = self.packets.get_mut(&packet) {
                entry.closed_ms = Some(now_ms);
                entry.closed_retry_count = Some(retry_count);
                if entry.applied_ms.is_some() {
                    entry.applied_before_close = true;
                }
            } else {
                // GapBucket range can contain live packets between two missing ranges
                // (Delphi FindBucketForPacket WantExtend marks them received internally).
                // They are not recovery losses, so keep them out of probability math.
                self.closed_untracked_packets = self.closed_untracked_packets.saturating_add(1);
            }
            if packet == end {
                break;
            }
            packet = packet.wrapping_add(1);
        }
    }

    fn on_gap_filled(&mut self, packet: u16, now_ms: u64) {
        let entry = self.packets.entry(packet).or_insert(GapPacketDiag {
            first_missing_ms: now_ms,
            ..GapPacketDiag::default()
        });
        entry.applied_ms = Some(now_ms);
        if entry.closed_ms.is_some() {
            entry.applied_late_after_close = true;
        } else {
            entry.applied_before_close = true;
        }
    }

    fn summarize(&self) -> TradesGapSummary {
        let mut summary = TradesGapSummary::default();
        summary.closed_untracked_packets = self.closed_untracked_packets;
        for entry in self.packets.values() {
            summary.unique_gap_packets += 1;
            summary.resend_packet_requests += u64::from(entry.resend_requests);
            if entry.resend_requests > summary.max_requests_for_one_packet {
                summary.max_requests_for_one_packet = entry.resend_requests;
            }
            if entry.closed_ms.is_some() {
                let req_idx = resend_bucket(entry.resend_requests);
                summary.closed_packets += 1;
                summary.closed_by_resend_requests[req_idx] += 1;
                if let (Some(closed_ms), Some(last_request_ms)) =
                    (entry.closed_ms, entry.last_request_ms)
                {
                    let lifetime_ms = closed_ms.saturating_sub(entry.first_missing_ms);
                    summary.closed_lifetime_count += 1;
                    summary.closed_lifetime_sum_ms =
                        summary.closed_lifetime_sum_ms.saturating_add(lifetime_ms);
                    if lifetime_ms > summary.closed_lifetime_max_ms {
                        summary.closed_lifetime_max_ms = lifetime_ms;
                    }
                    let wait_ms = closed_ms.saturating_sub(last_request_ms);
                    if summary.closed_after_last_request_count == 0
                        || wait_ms < summary.closed_after_last_request_min_ms
                    {
                        summary.closed_after_last_request_min_ms = wait_ms;
                    }
                    summary.closed_after_last_request_count += 1;
                    summary.closed_after_last_request_sum_ms = summary
                        .closed_after_last_request_sum_ms
                        .saturating_add(wait_ms);
                    if wait_ms > summary.closed_after_last_request_max_ms {
                        summary.closed_after_last_request_max_ms = wait_ms;
                    }
                } else {
                    summary.closed_without_resend_request += 1;
                }
                if entry.applied_late_after_close {
                    summary.not_applied_at_close += 1;
                    summary.not_applied_at_close_by_resend_requests[req_idx] += 1;
                    summary.record_not_applied_close_wait(entry);
                    summary.applied_late_after_close += 1;
                    summary.applied_late_after_close_by_resend_requests[req_idx] += 1;
                    if let (Some(applied_ms), Some(closed_ms)) = (entry.applied_ms, entry.closed_ms)
                    {
                        let delay_ms = applied_ms.saturating_sub(closed_ms);
                        summary.late_after_close_delay_sum_ms = summary
                            .late_after_close_delay_sum_ms
                            .saturating_add(delay_ms);
                        if delay_ms > summary.late_after_close_delay_max_ms {
                            summary.late_after_close_delay_max_ms = delay_ms;
                        }
                    }
                } else if entry.applied_ms.is_some() {
                    summary.applied_before_close += 1;
                    summary.applied_before_close_by_resend_requests[req_idx] += 1;
                } else {
                    summary.not_applied_at_close += 1;
                    summary.not_applied_at_close_by_resend_requests[req_idx] += 1;
                    summary.record_not_applied_close_wait(entry);
                    summary.never_applied_after_close += 1;
                    summary.never_applied_after_close_by_resend_requests[req_idx] += 1;
                }
            }
        }
        summary
    }
}

fn resend_bucket(requests: u8) -> usize {
    usize::from(requests.min(4))
}

#[derive(Default)]
struct TradesGapSummary {
    unique_gap_packets: u64,
    closed_packets: u64,
    applied_before_close: u64,
    applied_late_after_close: u64,
    never_applied_after_close: u64,
    not_applied_at_close: u64,
    closed_by_resend_requests: [u64; 5],
    applied_before_close_by_resend_requests: [u64; 5],
    applied_late_after_close_by_resend_requests: [u64; 5],
    never_applied_after_close_by_resend_requests: [u64; 5],
    not_applied_at_close_by_resend_requests: [u64; 5],
    resend_packet_requests: u64,
    max_requests_for_one_packet: u8,
    closed_without_resend_request: u64,
    closed_untracked_packets: u64,
    closed_lifetime_count: u64,
    closed_lifetime_max_ms: u64,
    closed_lifetime_sum_ms: u64,
    closed_after_last_request_count: u64,
    closed_after_last_request_min_ms: u64,
    closed_after_last_request_max_ms: u64,
    closed_after_last_request_sum_ms: u64,
    not_applied_close_after_last_request_count: u64,
    not_applied_close_after_last_request_min_ms: u64,
    not_applied_close_after_last_request_max_ms: u64,
    not_applied_close_after_last_request_sum_ms: u64,
    late_after_close_delay_max_ms: u64,
    late_after_close_delay_sum_ms: u64,
}

impl TradesGapSummary {
    fn record_not_applied_close_wait(&mut self, entry: &GapPacketDiag) {
        let (Some(closed_ms), Some(last_request_ms)) = (entry.closed_ms, entry.last_request_ms)
        else {
            return;
        };
        let wait_ms = closed_ms.saturating_sub(last_request_ms);
        if self.not_applied_close_after_last_request_count == 0
            || wait_ms < self.not_applied_close_after_last_request_min_ms
        {
            self.not_applied_close_after_last_request_min_ms = wait_ms;
        }
        self.not_applied_close_after_last_request_count += 1;
        self.not_applied_close_after_last_request_sum_ms = self
            .not_applied_close_after_last_request_sum_ms
            .saturating_add(wait_ms);
        if wait_ms > self.not_applied_close_after_last_request_max_ms {
            self.not_applied_close_after_last_request_max_ms = wait_ms;
        }
    }
}

#[derive(Default)]
struct SharedStats {
    label: Mutex<String>,
    stress_started_at: Mutex<Option<Instant>>,
    trades_gap_diag: Mutex<TradesGapDiagnostics>,
    client_id: Mutex<u64>,
    server_info: Mutex<ServerInfo>,
    protocol_err_emu_pct: AtomicU64,
    authorized: AtomicBool,
    init_ok: AtomicBool,
    lifecycle_connected_fresh: AtomicU64,
    lifecycle_connected_again: AtomicU64,
    lifecycle_reconnecting: AtomicU64,
    lifecycle_server_restart: AtomicU64,
    lifecycle_bind_failed: AtomicU64,
    protocol_runtime_ms: AtomicU64,
    protocol_total_sent_bytes: AtomicU64,
    protocol_total_recv_bytes: AtomicU64,
    protocol_max_sent_bytes: AtomicU64,
    protocol_max_recv_bytes: AtomicU64,
    protocol_max_sliced_in_flight: AtomicU64,
    protocol_max_sliced_blocks: AtomicU64,
    protocol_max_pending_h: AtomicU64,
    protocol_max_rtt_ms: AtomicU64,
    protocol_max_net_lag_ms: AtomicU64,
    protocol_max_overheat_milli_pct: AtomicU64,
    protocol_max_rs_drop_ppm: AtomicU64,
    protocol_min_pmtu: AtomicU64,
    events_total: AtomicU64,
    trades_apply: AtomicU64,
    trades_gap: AtomicU64,
    trades_gap_packets: AtomicU64,
    trades_gap_filled: AtomicU64,
    trades_gap_bucket_closed_ok: AtomicU64,
    trades_gap_bucket_closed_lost: AtomicU64,
    trades_gap_lost_packets: AtomicU64,
    trades_gap_out_of_order_resend: AtomicU64,
    trades_resend_ticks: AtomicU64,
    trades_resend_packet_requests: AtomicU64,
    trades_active_buckets_final: AtomicU64,
    trades_active_buckets_max: AtomicU64,
    trades_dup: AtomicU64,
    orderbook_apply: AtomicU64,
    orderbook_full: AtomicU64,
    balance_events: AtomicU64,
    order_events: AtomicU64,
    settings_events: AtomicU64,
    market_events: AtomicU64,
    engine_events: AtomicU64,
    strat_events: AtomicU64,
    strat_snapshot_full: AtomicU64,
    strat_snapshot_partial: AtomicU64,
    strat_snapshot_requested: AtomicU64,
    server_logs: AtomicU64,
    parse_failed: AtomicU64,
    api_sent: AtomicU64,
    api_ok: AtomicU64,
    api_error: AtomicU64,
    api_overdue: AtomicU64,
    api_completed_after_overdue: AtomicU64,
    api_timeout: AtomicU64,
    api_disconnected: AtomicU64,
    api_max_latency_ms: AtomicU64,
    api_latency_sum_ms: AtomicU64,
    candles_chunked_sent: AtomicU64,
    candles_chunked_ok: AtomicU64,
    candles_chunked_overdue: AtomicU64,
    candles_chunked_completed_after_overdue: AtomicU64,
    candles_chunked_timeout: AtomicU64,
    candles_chunked_disconnected: AtomicU64,
    candles_chunked_empty: AtomicU64,
    candles_chunked_max_latency_ms: AtomicU64,
    candles_chunked_latency_sum_ms: AtomicU64,
    max_pending_candles: AtomicU64,
    helper_settings_sent: AtomicU64,
    helper_settings_ok: AtomicU64,
    helper_settings_timeout: AtomicU64,
    helper_settings_disconnected: AtomicU64,
    helper_balance_sent: AtomicU64,
    helper_balance_ok: AtomicU64,
    helper_balance_timeout: AtomicU64,
    helper_balance_disconnected: AtomicU64,
    helper_orders_sent: AtomicU64,
    helper_orders_ok: AtomicU64,
    helper_orders_timeout: AtomicU64,
    helper_orders_disconnected: AtomicU64,
    helper_queued_events: AtomicU64,
    helper_max_queued_events: AtomicU64,
    tracked_status_rounds: AtomicU64,
    tracked_status_sent: AtomicU64,
    tracked_status_empty: AtomicU64,
    binance_tags_sent: AtomicU64,
    binance_tags_ok: AtomicU64,
    binance_tags_empty: AtomicU64,
    binance_tags_malformed: AtomicU64,
    binance_tags_max_items: AtomicU64,
    settings_requests: AtomicU64,
    balance_refresh_requests: AtomicU64,
    subscription_ops: AtomicU64,
    invalid_numbers: AtomicU64,
    max_pending_api: AtomicU64,
}

struct PendingApi {
    method: EngineMethod,
    sent_at: Instant,
    warned_timeout: bool,
    rx: std::sync::mpsc::Receiver<EngineResponse>,
}

struct PendingCandles {
    sent_at: Instant,
    warned_timeout: bool,
    rx: std::sync::mpsc::Receiver<MergedCandles>,
}

fn record_max(target: &AtomicU64, value: u64) {
    let mut prev = target.load(Ordering::Relaxed);
    while value > prev {
        match target.compare_exchange(prev, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(next) => prev = next,
        }
    }
}

fn duration_ms(value: Duration) -> u64 {
    value.as_millis().min(u128::from(u64::MAX)) as u64
}

fn event_elapsed_ms(stats: &SharedStats) -> u64 {
    stats
        .stress_started_at
        .lock()
        .unwrap()
        .as_ref()
        .map(|started| duration_ms(started.elapsed()))
        .unwrap_or(0)
}

fn gap_span(start: u16, end: u16) -> u64 {
    end.wrapping_sub(start) as u64 + 1
}

fn record_min_nonzero(target: &AtomicU64, value: u64) {
    if value == 0 {
        return;
    }
    let mut prev = target.load(Ordering::Relaxed);
    loop {
        if prev != 0 && value >= prev {
            break;
        }
        match target.compare_exchange(prev, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(next) => prev = next,
        }
    }
}

fn store_protocol_totals(stats: &SharedStats, sent: u64, recv: u64) {
    stats
        .protocol_total_sent_bytes
        .store(sent, Ordering::Relaxed);
    stats
        .protocol_total_recv_bytes
        .store(recv, Ordering::Relaxed);
    record_max(&stats.protocol_max_sent_bytes, sent);
    record_max(&stats.protocol_max_recv_bytes, recv);
}

fn record_protocol_sample(
    client: &Client,
    dispatcher: &EventDispatcher,
    stats: &SharedStats,
    runtime: Duration,
) {
    stats
        .protocol_runtime_ms
        .store(duration_ms(runtime), Ordering::Relaxed);
    store_protocol_totals(stats, client.total_sent(), client.total_recv());
    record_max(
        &stats.protocol_max_sliced_in_flight,
        client.sliced_in_flight_count() as u64,
    );
    record_max(
        &stats.protocol_max_sliced_blocks,
        client.sliced_in_flight_blocks() as u64,
    );
    record_max(
        &stats.protocol_max_pending_h,
        client.pending_high_count() as u64,
    );
    record_max(
        &stats.protocol_max_rtt_ms,
        client.round_trip_delay_ms().max(0) as u64,
    );
    record_max(
        &stats.protocol_max_net_lag_ms,
        client.net_lag_ping_ms().max(0) as u64,
    );
    record_max(
        &stats.protocol_max_overheat_milli_pct,
        (client.avg_over_heat().max(0.0) * 1000.0).round() as u64,
    );
    let rs_drop = (1.0 - client.rs()).clamp(0.0, 1.0);
    record_max(
        &stats.protocol_max_rs_drop_ppm,
        (rs_drop * 1_000_000.0).round() as u64,
    );
    record_min_nonzero(&stats.protocol_min_pmtu, client.actual_pmtu() as u64);
    let active_buckets = dispatcher.trades().used_buckets() as u64;
    stats
        .trades_active_buckets_final
        .store(active_buckets, Ordering::Relaxed);
    record_max(&stats.trades_active_buckets_max, active_buckets);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ErrEmuPhase {
    PostInit,
    PreConnect,
}

impl ErrEmuPhase {
    fn parse(value: Option<&String>) -> Self {
        let Some(value) = value else {
            return Self::PostInit;
        };
        match value.trim().replace('-', "_").to_ascii_lowercase().as_str() {
            "" | "post" | "post_init" | "after_init" => Self::PostInit,
            "pre" | "pre_connect" | "preconnect" | "before_connect" => Self::PreConnect,
            other => {
                eprintln!("invalid err_emu_phase '{other}', expected post_init or pre_connect");
                std::process::exit(1);
            }
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::PostInit => "post_init",
            Self::PreConnect => "pre_connect",
        }
    }
}

#[derive(Default)]
struct LossGateState {
    ready: usize,
    enabled: bool,
    aborted: bool,
}

struct LossGate {
    expected: usize,
    err_emu_pct: u8,
    phase: ErrEmuPhase,
    state: Mutex<LossGateState>,
    changed: Condvar,
}

impl LossGate {
    fn new(expected: usize, err_emu_pct: u8, phase: ErrEmuPhase) -> Self {
        Self {
            expected,
            err_emu_pct,
            phase,
            state: Mutex::new(LossGateState::default()),
            changed: Condvar::new(),
        }
    }

    fn wait_after_init(&self, label: &str) -> bool {
        if self.err_emu_pct == 0 || self.phase != ErrEmuPhase::PostInit {
            return true;
        }

        let mut state = self.state.lock().unwrap();
        state.ready += 1;
        println!(
            "[{label}] waiting for post-init err_emu gate ({}/{})",
            state.ready, self.expected
        );

        if state.ready >= self.expected && !state.enabled {
            moonproto::client::set_err_emu(self.err_emu_pct);
            state.enabled = true;
            println!(
                "[main] client-side err_emu={} enabled after all clients init",
                self.err_emu_pct
            );
            self.changed.notify_all();
            return true;
        }

        while !state.enabled && !state.aborted {
            state = self.changed.wait(state).unwrap();
        }
        state.enabled
    }

    fn abort(&self) {
        if self.err_emu_pct == 0 || self.phase != ErrEmuPhase::PostInit {
            return;
        }
        let mut state = self.state.lock().unwrap();
        state.aborted = true;
        self.changed.notify_all();
    }
}

#[derive(Clone)]
struct Args {
    key_b64: String,
    host: String,
    port: u16,
    market: String,
    duration: Duration,
    err_emu_pct: u8,
    err_emu_phase: ErrEmuPhase,
}

fn parse_host(value: Option<&String>) -> (String, u16) {
    let raw = value.map(String::as_str).unwrap_or(DEFAULT_HOST);
    let Some((host, port)) = raw.split_once(':') else {
        return (raw.to_string(), 3000);
    };
    (host.to_string(), port.parse().unwrap_or(3000))
}

fn parse_args() -> Args {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!(
            "Usage: stress_client <key_base64> [host:port] [market] [duration_secs] [err_emu_pct] [err_emu_phase]"
        );
        eprintln!("  err_emu_phase: post_init (default) | pre_connect");
        std::process::exit(1);
    }
    let (host, port) = parse_host(args.get(2));
    let market = args
        .get(3)
        .cloned()
        .unwrap_or_else(|| DEFAULT_MARKET.to_string());
    let duration = Duration::from_secs(
        args.get(4)
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_DURATION_SECS),
    );
    let err_emu_pct = args
        .get(5)
        .and_then(|v| v.parse::<u8>().ok())
        .unwrap_or(0)
        .min(100);
    let err_emu_phase = ErrEmuPhase::parse(args.get(6));
    Args {
        key_b64: args[1].clone(),
        host,
        port,
        market,
        duration,
        err_emu_pct,
        err_emu_phase,
    }
}

fn spawn_subscription_churn(
    label: String,
    sender: ClientSender,
    market: String,
    stop: Arc<AtomicBool>,
    stats: Arc<SharedStats>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut iter = 0u64;
        while !stop.load(Ordering::Relaxed) {
            match iter % 6 {
                0 => sender.subscribe_orderbook(&market),
                1 => sender.subscribe_all_trades(false),
                2 => sender.subscribe_orderbook(&market),
                3 => sender.subscribe_all_trades(true),
                4 => sender.unsubscribe_orderbook(&market),
                _ => sender.subscribe_orderbook(&market),
            }
            stats.subscription_ops.fetch_add(1, Ordering::Relaxed);
            iter = iter.wrapping_add(1);
            println!("[{label}] subscription churn op #{iter}");
            thread::sleep(Duration::from_secs(7));
        }
    })
}

fn push_pending(
    pending: &mut VecDeque<PendingApi>,
    stats: &SharedStats,
    method: EngineMethod,
    rx: std::sync::mpsc::Receiver<EngineResponse>,
) {
    stats.api_sent.fetch_add(1, Ordering::Relaxed);
    if matches!(method, EngineMethod::CheckBinanceTags) {
        stats.binance_tags_sent.fetch_add(1, Ordering::Relaxed);
    }
    pending.push_back(PendingApi {
        method,
        sent_at: Instant::now(),
        warned_timeout: false,
        rx,
    });
    let len = pending.len() as u64;
    let mut prev = stats.max_pending_api.load(Ordering::Relaxed);
    while len > prev {
        match stats.max_pending_api.compare_exchange(
            prev,
            len,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(next) => prev = next,
        }
    }
}

fn schedule_safe_burst(
    client: &mut Client,
    pending: &mut VecDeque<PendingApi>,
    pending_candles: &mut VecDeque<PendingCandles>,
    stats: &SharedStats,
    market: &str,
    burst_no: u64,
    allow_candles: bool,
) {
    if pending.len() >= MAX_PENDING_API {
        return;
    }

    push_pending(
        pending,
        stats,
        EngineMethod::UpdateMarketsList,
        client.api_update_markets_list(),
    );
    push_pending(
        pending,
        stats,
        EngineMethod::GetMarketsIndexes,
        client.api_get_markets_indexes(),
    );
    push_pending(
        pending,
        stats,
        EngineMethod::QueryHedgeMode,
        client.api_query_hedge_mode(),
    );
    push_pending(
        pending,
        stats,
        EngineMethod::GetBalance,
        client.api_get_balance("USDT"),
    );
    push_pending(
        pending,
        stats,
        EngineMethod::CheckAPIExpirationTime,
        client.api_check_expiration_time(),
    );
    push_pending(
        pending,
        stats,
        EngineMethod::CheckBinanceTags,
        client.api_check_binance_tags(),
    );

    if burst_no.is_multiple_of(3) {
        push_pending(
            pending,
            stats,
            EngineMethod::GetCoinCardCandles,
            client.api_get_coin_card_candles(market, DeepHistoryKind::Hour1),
        );
    }

    if allow_candles && burst_no % 5 == 1 && pending_candles.len() < MAX_PENDING_CANDLES {
        let rx = client.api_request_candles_data_async();
        stats.candles_chunked_sent.fetch_add(1, Ordering::Relaxed);
        pending_candles.push_back(PendingCandles {
            sent_at: Instant::now(),
            warned_timeout: false,
            rx,
        });
        let len = pending_candles.len() as u64;
        let mut prev = stats.max_pending_candles.load(Ordering::Relaxed);
        while len > prev {
            match stats.max_pending_candles.compare_exchange(
                prev,
                len,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(next) => prev = next,
            }
        }
    }

    client.ui_settings_request();
    stats.settings_requests.fetch_add(1, Ordering::Relaxed);
    client.balance_request_refresh();
    stats
        .balance_refresh_requests
        .fetch_add(1, Ordering::Relaxed);
}

fn drain_pending(label: &str, pending: &mut VecDeque<PendingApi>, stats: &SharedStats) {
    let now = Instant::now();
    let mut kept = VecDeque::with_capacity(pending.len());

    while let Some(mut item) = pending.pop_front() {
        let age = now.duration_since(item.sent_at);
        match item.rx.try_recv() {
            Ok(resp) => {
                let latency_ms = duration_ms(age);
                record_max(&stats.api_max_latency_ms, latency_ms);
                stats
                    .api_latency_sum_ms
                    .fetch_add(latency_ms, Ordering::Relaxed);
                if item.warned_timeout {
                    stats
                        .api_completed_after_overdue
                        .fetch_add(1, Ordering::Relaxed);
                }
                if resp.success {
                    stats.api_ok.fetch_add(1, Ordering::Relaxed);
                } else {
                    stats.api_error.fetch_add(1, Ordering::Relaxed);
                    println!(
                        "[{label}] api error method={:?} code={} msg={}",
                        resp.method, resp.error_code, resp.error_msg
                    );
                }
                validate_response(label, &resp, stats);
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                if age >= API_HARD_TIMEOUT {
                    stats.api_timeout.fetch_add(1, Ordering::Relaxed);
                    println!(
                        "[{label}] api hard timeout method={:?} age={}ms",
                        item.method,
                        duration_ms(age)
                    );
                } else {
                    if age >= API_WARN_TIMEOUT && !item.warned_timeout {
                        item.warned_timeout = true;
                        stats.api_overdue.fetch_add(1, Ordering::Relaxed);
                        println!(
                            "[{label}] api overdue method={:?} age={}ms; keep waiting",
                            item.method,
                            duration_ms(age)
                        );
                    }
                    kept.push_back(item);
                }
            }
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                stats.api_disconnected.fetch_add(1, Ordering::Relaxed);
                println!("[{label}] api disconnected method={:?}", item.method);
            }
        }
    }

    *pending = kept;
}

fn drain_pending_candles(label: &str, pending: &mut VecDeque<PendingCandles>, stats: &SharedStats) {
    let now = Instant::now();
    let mut kept = VecDeque::with_capacity(pending.len());

    while let Some(mut item) = pending.pop_front() {
        let age = now.duration_since(item.sent_at);
        match item.rx.try_recv() {
            Ok(merged) => {
                let latency_ms = duration_ms(age);
                record_max(&stats.candles_chunked_max_latency_ms, latency_ms);
                stats
                    .candles_chunked_latency_sum_ms
                    .fetch_add(latency_ms, Ordering::Relaxed);
                if item.warned_timeout {
                    stats
                        .candles_chunked_completed_after_overdue
                        .fetch_add(1, Ordering::Relaxed);
                }
                stats.candles_chunked_ok.fetch_add(1, Ordering::Relaxed);
                validate_chunked_candles(label, &merged, stats);
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                if age >= CANDLES_HARD_TIMEOUT {
                    stats
                        .candles_chunked_timeout
                        .fetch_add(1, Ordering::Relaxed);
                    println!(
                        "[{label}] chunked candles hard timeout age={}ms",
                        duration_ms(age)
                    );
                } else {
                    if age >= CANDLES_WARN_TIMEOUT && !item.warned_timeout {
                        item.warned_timeout = true;
                        stats
                            .candles_chunked_overdue
                            .fetch_add(1, Ordering::Relaxed);
                        println!(
                            "[{label}] chunked candles overdue age={}ms; keep waiting",
                            duration_ms(age)
                        );
                    }
                    kept.push_back(item);
                }
            }
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                stats
                    .candles_chunked_disconnected
                    .fetch_add(1, Ordering::Relaxed);
                println!("[{label}] chunked candles disconnected");
            }
        }
    }

    *pending = kept;
}

fn validate_response(label: &str, resp: &EngineResponse, stats: &SharedStats) {
    match resp.method {
        EngineMethod::GetBalance => {
            if let Some(balance) = parse_get_balance_response(&resp.data) {
                if !balance.is_finite() || balance < 0.0 {
                    stats.invalid_numbers.fetch_add(1, Ordering::Relaxed);
                    println!("[{label}] invalid balance value={balance}");
                }
            } else if resp.success {
                stats.invalid_numbers.fetch_add(1, Ordering::Relaxed);
                println!(
                    "[{label}] malformed GetBalance response: {} bytes",
                    resp.data.len()
                );
            }
        }
        EngineMethod::QueryHedgeMode
            if parse_query_hedge_mode_response(&resp.data).is_none() && resp.success =>
        {
            stats.invalid_numbers.fetch_add(1, Ordering::Relaxed);
            println!(
                "[{label}] malformed QueryHedgeMode response: {} bytes",
                resp.data.len()
            );
        }
        EngineMethod::CheckAPIExpirationTime => {
            if let Some(expiration) = parse_api_expiration_time_response(&resp.data) {
                let raw = expiration.delphi_time();
                if !raw.is_finite() || raw < 0.0 {
                    stats.invalid_numbers.fetch_add(1, Ordering::Relaxed);
                    println!("[{label}] invalid API expiration time raw_delphi_time={raw}");
                }
            } else if resp.success {
                stats.invalid_numbers.fetch_add(1, Ordering::Relaxed);
                println!(
                    "[{label}] malformed CheckAPIExpirationTime response: {} bytes",
                    resp.data.len()
                );
            }
        }
        EngineMethod::CheckBinanceTags => {
            if let Some(items) = parse_token_tags_response(&resp.data) {
                stats.binance_tags_ok.fetch_add(1, Ordering::Relaxed);
                record_max(&stats.binance_tags_max_items, items.len() as u64);
                if items.is_empty() {
                    stats.binance_tags_empty.fetch_add(1, Ordering::Relaxed);
                }
                for item in items {
                    if item.market_name.is_empty() || item.tags.is_empty() {
                        stats.binance_tags_malformed.fetch_add(1, Ordering::Relaxed);
                        stats.invalid_numbers.fetch_add(1, Ordering::Relaxed);
                        println!(
                            "[{label}] malformed CheckBinanceTags item market='{}' tags={:#x}",
                            item.market_name,
                            item.tags.bits()
                        );
                        break;
                    }
                }
            } else if resp.success {
                stats.binance_tags_malformed.fetch_add(1, Ordering::Relaxed);
                stats.invalid_numbers.fetch_add(1, Ordering::Relaxed);
                println!(
                    "[{label}] malformed CheckBinanceTags response: {} bytes",
                    resp.data.len()
                );
            }
        }
        EngineMethod::GetCoinCardCandles => {
            if let Some(candles) = parse_coin_card_candles_response(&resp.data) {
                for candle in candles {
                    let prices = [candle.open_p, candle.max_p, candle.min_p, candle.close_p];
                    let bad = prices.iter().any(|v| !v.is_finite() || *v < 0.0)
                        || !candle.vol.is_finite()
                        || candle.vol < 0.0
                        || !candle.time.is_finite();
                    if bad {
                        stats.invalid_numbers.fetch_add(1, Ordering::Relaxed);
                        println!(
                            "[{label}] invalid candle open={} high={} low={} close={} vol={} time={}",
                            candle.open_p,
                            candle.max_p,
                            candle.min_p,
                            candle.close_p,
                            candle.vol,
                            candle.time
                        );
                        break;
                    }
                }
            } else if resp.success {
                stats.invalid_numbers.fetch_add(1, Ordering::Relaxed);
                println!(
                    "[{label}] malformed GetCoinCardCandles response: {} bytes",
                    resp.data.len()
                );
            }
        }
        _ => {}
    }
}

fn validate_chunked_candles(label: &str, merged: &MergedCandles, stats: &SharedStats) {
    if merged.zipped_data.is_empty() || merged.markets.is_empty() {
        stats.candles_chunked_empty.fetch_add(1, Ordering::Relaxed);
        println!(
            "[{label}] chunked candles uid={} returned empty result zipped={} markets={}",
            merged.uid,
            merged.zipped_data.len(),
            merged.markets.len()
        );
        return;
    }

    let mut candle_count = 0usize;
    for market in &merged.markets {
        candle_count += market.candles_5m.len();
        for candle in &market.candles_5m {
            let prices = [candle.open_p, candle.max_p, candle.min_p, candle.close_p];
            let bad = prices.iter().any(|v| !v.is_finite() || *v < 0.0)
                || !candle.vol.is_finite()
                || candle.vol < 0.0
                || !candle.time.is_finite();
            if bad {
                stats.invalid_numbers.fetch_add(1, Ordering::Relaxed);
                println!(
                    "[{label}] invalid chunked candle uid={} market={} open={} high={} low={} close={} vol={} time={}",
                    merged.uid,
                    market.market_name,
                    candle.open_p,
                    candle.max_p,
                    candle.min_p,
                    candle.close_p,
                    candle.vol,
                    candle.time
                );
                return;
            }
        }
    }

    if candle_count == 0 {
        stats.candles_chunked_empty.fetch_add(1, Ordering::Relaxed);
        println!(
            "[{label}] chunked candles uid={} returned {} markets but 0 candles",
            merged.uid,
            merged.markets.len()
        );
    }
}

fn validate_settings_snapshot(label: &str, settings: &ClientSettingsCommand, stats: &SharedStats) {
    let floats64 = [settings.fixed_sell_price, settings.g_take_profit];
    let floats32 = [
        settings.price_drop_level,
        settings.trailing_drop,
        settings.s_price[0],
        settings.s_price[1],
        settings.s_price[2],
        settings.s_price[3],
        settings.s_price[4],
        settings.s_price[5],
    ];
    let bad = floats64.iter().any(|v| !v.is_finite())
        || floats32.iter().any(|v| !v.is_finite())
        || settings.temp_bl_times.iter().any(|v| !v.is_finite());

    if bad {
        stats.invalid_numbers.fetch_add(1, Ordering::Relaxed);
        println!(
            "[{label}] invalid settings uid={} fixed_sell={} take_profit={} temp_bl={}",
            settings.uid,
            settings.fixed_sell_price,
            settings.g_take_profit,
            settings.temp_bl_times.len()
        );
    }
}

fn validate_balance_snapshot(
    label: &str,
    balances: &moonproto::state::BalancesState,
    stats: &SharedStats,
) {
    let globals = [
        balances.global.btc_balance_total,
        balances.global.btc_balance_locked,
        balances.global.btc_balance_full,
        balances.global.special_coin_balance,
    ];
    if globals.iter().any(|v| !v.is_finite()) {
        stats.invalid_numbers.fetch_add(1, Ordering::Relaxed);
        println!(
            "[{label}] invalid balance globals epoch={} total={} locked={} full={} special={}",
            balances.last_epoch,
            balances.global.btc_balance_total,
            balances.global.btc_balance_locked,
            balances.global.btc_balance_full,
            balances.global.special_coin_balance
        );
        return;
    }

    for (_, item) in balances.iter() {
        let values = [
            item.initial_balance,
            item.locked_balance,
            item.pos_size,
            item.pos_price,
            item.liq_price,
            item.long_pos_size,
            item.long_pos_price,
            item.long_liq_price,
            item.short_pos_size,
            item.short_pos_price,
            item.short_liq_price,
            item.asset_balance,
            item.asset_balance_full,
            item.total_profit_b,
            item.total_profit_l,
            item.total_profit_s,
            item.max_value,
        ];
        if values.iter().any(|v| !v.is_finite()) {
            stats.invalid_numbers.fetch_add(1, Ordering::Relaxed);
            println!(
                "[{label}] invalid balance market={} epoch={} initial={} locked={} pos={} asset_full={}",
                item.market_name,
                balances.last_epoch,
                item.initial_balance,
                item.locked_balance,
                item.pos_size,
                item.asset_balance_full
            );
            return;
        }
    }
}

fn validate_order_compact(
    label: &str,
    uid: u64,
    side: &str,
    order: &moonproto::commands::trade::OrderCompact,
    stats: &SharedStats,
) {
    let quantity = order.quantity;
    let quantity_remaining = order.quantity_remaining;
    let total_btc = order.total_btc;
    let spent_btc = order.spent_btc;
    let open_time = order.open_time;
    let close_time = order.close_time;
    let actual_price = order.actual_price;
    let mean_price = order.mean_price;
    let quantity_base = order.quantity_base;
    let actual_q = order.actual_q;
    let tmp_btc = order.tmp_btc;
    let create_time = order.create_time;
    let panic_sell_down = order.panic_sell_down;
    let values = [
        quantity,
        quantity_remaining,
        total_btc,
        spent_btc,
        open_time,
        close_time,
        actual_price,
        mean_price,
        quantity_base,
        actual_q,
        tmp_btc,
        create_time,
    ];
    let bad = values.iter().any(|v| !v.is_finite())
        || !panic_sell_down.is_finite()
        || actual_price < 0.0
        || mean_price < 0.0;

    if bad {
        stats.invalid_numbers.fetch_add(1, Ordering::Relaxed);
        println!(
            "[{label}] invalid order uid={} side={} qty={} remain={} actual={} mean={}",
            uid, side, quantity, quantity_remaining, actual_price, mean_price
        );
    }
}

fn validate_order_snapshot(label: &str, orders: &[moonproto::state::Order], stats: &SharedStats) {
    for order in orders {
        if order.market_name.is_empty()
            || !order.vstop_level.is_finite()
            || !order.vstop_vol.is_finite()
            || !order.corridor_price_down.is_finite()
            || !order.corridor_price_up.is_finite()
        {
            stats.invalid_numbers.fetch_add(1, Ordering::Relaxed);
            println!(
                "[{label}] invalid order snapshot uid={} market='{}' status={:?}",
                order.uid, order.market_name, order.status
            );
            return;
        }
        validate_order_compact(label, order.uid, "buy", &order.buy_order, stats);
        validate_order_compact(label, order.uid, "sell", &order.sell_order, stats);
    }
}

fn record_helper_error(
    label: &str,
    helper: &str,
    err: std::sync::mpsc::RecvTimeoutError,
    timeout: &AtomicU64,
    disconnected: &AtomicU64,
) {
    match err {
        std::sync::mpsc::RecvTimeoutError::Timeout => {
            timeout.fetch_add(1, Ordering::Relaxed);
            println!("[{label}] helper {helper} timeout");
        }
        std::sync::mpsc::RecvTimeoutError::Disconnected => {
            disconnected.fetch_add(1, Ordering::Relaxed);
            println!("[{label}] helper {helper} disconnected");
        }
    }
}

fn drain_helper_queued_events(label: &str, dispatcher: &mut EventDispatcher, stats: &SharedStats) {
    let events = dispatcher.take_queued_events();
    if events.is_empty() {
        return;
    }

    let count = events.len() as u64;
    stats
        .helper_queued_events
        .fetch_add(count, Ordering::Relaxed);
    record_max(&stats.helper_max_queued_events, count);
    for event in &events {
        handle_event(label, event, stats);
    }
}

fn send_tracked_status_requests(
    label: &str,
    client: &Client,
    dispatcher: &EventDispatcher,
    stats: &SharedStats,
) {
    stats.tracked_status_rounds.fetch_add(1, Ordering::Relaxed);
    let mut sent = 0u64;

    for order in dispatcher.orders().iter().take(TRACKED_STATUS_BATCH) {
        client.request_tracked_order_status(order);
        sent += 1;
    }

    if sent == 0 {
        stats.tracked_status_empty.fetch_add(1, Ordering::Relaxed);
        println!("[{label}] tracked status refresh skipped: no tracked orders");
    } else {
        stats.tracked_status_sent.fetch_add(sent, Ordering::Relaxed);
        println!("[{label}] tracked status refresh sent {sent} request(s)");
    }
}

fn run_one_shot_helper_round(
    label: &str,
    client: &mut Client,
    dispatcher: &mut EventDispatcher,
    stats: &SharedStats,
    round: u64,
) {
    match round % 3 {
        0 => {
            stats.helper_settings_sent.fetch_add(1, Ordering::Relaxed);
            match client.request_client_settings(dispatcher, HELPER_TIMEOUT) {
                Ok(settings) => {
                    stats.helper_settings_ok.fetch_add(1, Ordering::Relaxed);
                    validate_settings_snapshot(label, &settings, stats);
                    println!(
                        "[{label}] helper settings ok queued={}",
                        dispatcher.queued_event_count()
                    );
                }
                Err(err) => record_helper_error(
                    label,
                    "settings",
                    err,
                    &stats.helper_settings_timeout,
                    &stats.helper_settings_disconnected,
                ),
            }
        }
        1 => {
            stats.helper_balance_sent.fetch_add(1, Ordering::Relaxed);
            match client.request_balance_snapshot(dispatcher, HELPER_TIMEOUT) {
                Ok(balances) => {
                    stats.helper_balance_ok.fetch_add(1, Ordering::Relaxed);
                    validate_balance_snapshot(label, &balances, stats);
                    println!(
                        "[{label}] helper balance ok rows={} queued={}",
                        balances.len(),
                        dispatcher.queued_event_count()
                    );
                }
                Err(err) => record_helper_error(
                    label,
                    "balance",
                    err,
                    &stats.helper_balance_timeout,
                    &stats.helper_balance_disconnected,
                ),
            }
        }
        _ => {
            stats.helper_orders_sent.fetch_add(1, Ordering::Relaxed);
            match client.request_order_snapshot(dispatcher, HELPER_TIMEOUT) {
                Ok(orders) => {
                    stats.helper_orders_ok.fetch_add(1, Ordering::Relaxed);
                    validate_order_snapshot(label, &orders, stats);
                    stats.tracked_status_rounds.fetch_add(1, Ordering::Relaxed);
                    for order in orders.iter().take(TRACKED_STATUS_BATCH) {
                        client.request_tracked_order_status(order);
                        stats.tracked_status_sent.fetch_add(1, Ordering::Relaxed);
                    }
                    if orders.is_empty() {
                        stats.tracked_status_empty.fetch_add(1, Ordering::Relaxed);
                    }
                    println!(
                        "[{label}] helper orders ok count={} queued={} tracked_status_sent={}",
                        orders.len(),
                        dispatcher.queued_event_count(),
                        orders.len().min(TRACKED_STATUS_BATCH)
                    );
                }
                Err(err) => record_helper_error(
                    label,
                    "orders",
                    err,
                    &stats.helper_orders_timeout,
                    &stats.helper_orders_disconnected,
                ),
            }
        }
    }

    drain_helper_queued_events(label, dispatcher, stats);
}

fn handle_event(label: &str, event: &Event, stats: &SharedStats) {
    stats.events_total.fetch_add(1, Ordering::Relaxed);
    match event {
        Event::Trade(trades) => match trades {
            TradesEvent::Apply(pkt) => {
                stats.trades_apply.fetch_add(1, Ordering::Relaxed);
                for section in &pkt.sections {
                    if let moonproto::commands::TradeSection::Trades(items) = section {
                        for trade in items {
                            if !trade.price.is_finite()
                                || !trade.qty.is_finite()
                                || trade.price <= 0.0
                                || trade.qty == 0.0
                            {
                                stats.invalid_numbers.fetch_add(1, Ordering::Relaxed);
                                println!(
                                    "[{label}] invalid trade packet={} price={} qty={}",
                                    pkt.packet_num, trade.price, trade.qty
                                );
                                return;
                            }
                        }
                    }
                }
            }
            TradesEvent::GapDetected { start, end } => {
                let now_ms = event_elapsed_ms(stats);
                stats.trades_gap.fetch_add(1, Ordering::Relaxed);
                stats
                    .trades_gap_packets
                    .fetch_add(gap_span(*start, *end), Ordering::Relaxed);
                stats
                    .trades_gap_diag
                    .lock()
                    .unwrap()
                    .on_gap_detected(*start, *end, now_ms);
                println!("[{label}] trades gap detected {start}..{end}");
            }
            TradesEvent::GapFilled {
                packet_num,
                bucket_seq_range,
            } => {
                let now_ms = event_elapsed_ms(stats);
                stats.trades_gap_filled.fetch_add(1, Ordering::Relaxed);
                stats
                    .trades_gap_diag
                    .lock()
                    .unwrap()
                    .on_gap_filled(*packet_num, now_ms);
                println!(
                    "[{label}] trades gap filled packet={} bucket={}..{}",
                    packet_num, bucket_seq_range.0, bucket_seq_range.1
                );
            }
            TradesEvent::ResendRequested { packet_nums } => {
                let now_ms = event_elapsed_ms(stats);
                stats.trades_resend_ticks.fetch_add(1, Ordering::Relaxed);
                stats
                    .trades_resend_packet_requests
                    .fetch_add(packet_nums.len() as u64, Ordering::Relaxed);
                stats
                    .trades_gap_diag
                    .lock()
                    .unwrap()
                    .on_resend_requested(packet_nums, now_ms);
            }
            TradesEvent::BucketClosed {
                start,
                end,
                all_received,
                retry_count,
            } => {
                let now_ms = event_elapsed_ms(stats);
                stats.trades_gap_diag.lock().unwrap().on_bucket_closed(
                    *start,
                    *end,
                    *retry_count,
                    now_ms,
                );
                if *all_received {
                    stats
                        .trades_gap_bucket_closed_ok
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    let lost = gap_span(*start, *end);
                    stats
                        .trades_gap_bucket_closed_lost
                        .fetch_add(1, Ordering::Relaxed);
                    stats
                        .trades_gap_lost_packets
                        .fetch_add(lost, Ordering::Relaxed);
                    println!(
                        "[{label}] trades gap LOST bucket {start}..{end} lost_packets={lost} retry_count={retry_count}"
                    );
                }
            }
            TradesEvent::OutOfOrder { packet_num } => {
                let now_ms = event_elapsed_ms(stats);
                stats
                    .trades_gap_out_of_order_resend
                    .fetch_add(1, Ordering::Relaxed);
                stats
                    .trades_gap_diag
                    .lock()
                    .unwrap()
                    .on_gap_filled(*packet_num, now_ms);
                println!("[{label}] trades resend out-of-order packet={packet_num}");
            }
            TradesEvent::Duplicate => {
                stats.trades_dup.fetch_add(1, Ordering::Relaxed);
            }
        },
        Event::OrderBook(OrderBookEvent::Apply {
            is_full,
            buys,
            sells,
            market_index,
            seq,
            ..
        }) => {
            stats.orderbook_apply.fetch_add(1, Ordering::Relaxed);
            if *is_full {
                stats.orderbook_full.fetch_add(1, Ordering::Relaxed);
            }
            for level in buys.iter().chain(sells.iter()) {
                if !level.rate.is_finite()
                    || !level.quantity.is_finite()
                    || level.rate <= 0.0
                    || level.quantity < 0.0
                {
                    stats.invalid_numbers.fetch_add(1, Ordering::Relaxed);
                    println!(
                        "[{label}] invalid orderbook idx={} seq={} price={} qty={}",
                        market_index, seq, level.rate, level.quantity
                    );
                    return;
                }
            }
        }
        Event::OrderBook(_) => {}
        Event::Balance(_) => {
            stats.balance_events.fetch_add(1, Ordering::Relaxed);
        }
        Event::Order(_) => {
            stats.order_events.fetch_add(1, Ordering::Relaxed);
        }
        Event::Settings(_) => {
            stats.settings_events.fetch_add(1, Ordering::Relaxed);
        }
        Event::Markets(_) => {
            stats.market_events.fetch_add(1, Ordering::Relaxed);
        }
        Event::Strat(strat) => {
            stats.strat_events.fetch_add(1, Ordering::Relaxed);
            match strat {
                StratEvent::SnapshotRequested { uid } => {
                    stats
                        .strat_snapshot_requested
                        .fetch_add(1, Ordering::Relaxed);
                    println!("[{label}] strat snapshot requested by server uid={uid}");
                }
                StratEvent::SnapshotFull {
                    server_epoch,
                    raw_data,
                } => {
                    let seq = stats.strat_snapshot_full.fetch_add(1, Ordering::Relaxed) + 1;
                    log_strat_snapshot(label, "full", seq, *server_epoch, raw_data);
                }
                StratEvent::SnapshotPartial {
                    server_epoch,
                    raw_data,
                } => {
                    let seq = stats.strat_snapshot_partial.fetch_add(1, Ordering::Relaxed) + 1;
                    log_strat_snapshot(label, "partial", seq, *server_epoch, raw_data);
                }
                other => {
                    println!("[{label}] strat event {other:?}");
                }
            }
        }
        Event::EngineResponse(resp) => {
            stats.engine_events.fetch_add(1, Ordering::Relaxed);
            if !resp.success {
                println!(
                    "[{label}] unclaimed engine response error method={:?} code={} msg={}",
                    resp.method, resp.error_code, resp.error_msg
                );
            }
        }
        Event::ServerLog { msg, .. } => {
            stats.server_logs.fetch_add(1, Ordering::Relaxed);
            let trimmed = msg.trim();
            if should_print_server_log(trimmed) {
                println!("[{label}] server log: {trimmed}");
            }
        }
        Event::ParseFailed { cmd, len, .. } => {
            stats.parse_failed.fetch_add(1, Ordering::Relaxed);
            println!("[{label}] parse failed cmd={cmd:?} len={len}");
        }
        _ => {}
    }
}

fn log_strat_snapshot(label: &str, kind: &str, seq: u64, server_epoch: u64, raw_data: &[u8]) {
    match parse_strategy_batch(raw_data) {
        Some(batch) => {
            println!(
                "[{label}] strat snapshot {kind}#{seq} epoch={server_epoch} raw={}B strategies={} names={} paths={} rebuilt=skipped_no_schema",
                raw_data.len(),
                batch.strategies.len(),
                batch.names.len(),
                batch.paths.len(),
            );
            dump_strat_payload(label, kind, seq, server_epoch, "raw", raw_data);
        }
        None => println!(
            "[{label}] strat snapshot {kind}#{seq} epoch={server_epoch} raw={}B parse_failed",
            raw_data.len()
        ),
    }
}

fn dump_strat_payload(
    label: &str,
    kind: &str,
    seq: u64,
    server_epoch: u64,
    suffix: &str,
    bytes: &[u8],
) {
    let Some(dir) = env::var_os("MOONPROTO_STRESS_DUMP_STRATS_DIR") else {
        return;
    };
    let dir = std::path::PathBuf::from(dir);
    if std::fs::create_dir_all(&dir).is_err() {
        return;
    }
    let file_name = format!("{label}_strat_{kind}_{seq}_epoch_{server_epoch}_{suffix}.bin");
    let path = dir.join(file_name);
    if let Err(err) = std::fs::write(&path, bytes) {
        println!(
            "[{label}] failed to dump strat payload {} bytes to {}: {err}",
            bytes.len(),
            path.display()
        );
    }
}

fn should_print_server_log(msg: &str) -> bool {
    if msg.is_empty() || msg.len() >= 220 {
        return false;
    }
    let lower = msg.to_ascii_lowercase();
    lower.contains("error")
        || lower.contains("fail")
        || lower.contains("exception")
        || lower.contains("restart")
        || lower.contains("wrong")
        || lower.contains("timeout")
}

fn run_one_client(
    label: &'static str,
    args: Args,
    keys: key_import::ImportedKeys,
    stats: Arc<SharedStats>,
    loss_gate: Arc<LossGate>,
) {
    *stats.label.lock().unwrap() = label.to_string();
    stats
        .protocol_err_emu_pct
        .store(u64::from(args.err_emu_pct), Ordering::Relaxed);
    let client_id = rand::random::<u64>();
    *stats.client_id.lock().unwrap() = client_id;

    let cfg = ClientConfig::new(args.host.clone(), args.port, keys.master_key, keys.mac_key)
        .with_client_id(client_id);
    let mut client = Client::new(cfg);
    let mut dispatcher = EventDispatcher::new();

    {
        let stats = Arc::clone(&stats);
        client.on_lifecycle(Box::new(move |event| match event {
            LifecycleEvent::Connected { fresh: true } => {
                stats.authorized.store(true, Ordering::Relaxed);
                stats
                    .lifecycle_connected_fresh
                    .fetch_add(1, Ordering::Relaxed);
            }
            LifecycleEvent::Connected { fresh: false } => {
                stats.authorized.store(true, Ordering::Relaxed);
                stats
                    .lifecycle_connected_again
                    .fetch_add(1, Ordering::Relaxed);
            }
            LifecycleEvent::Reconnecting => {
                stats.lifecycle_reconnecting.fetch_add(1, Ordering::Relaxed);
            }
            LifecycleEvent::ServerRestart => {
                stats
                    .lifecycle_server_restart
                    .fetch_add(1, Ordering::Relaxed);
            }
            LifecycleEvent::BindFailed { .. } => {
                stats.lifecycle_bind_failed.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }));
    }

    println!(
        "[{label}] connecting client_id={client_id:#x} to {}:{}",
        args.host, args.port
    );
    client.run_with_dispatcher(CONNECT_TIMEOUT, &mut dispatcher, Box::new(|_| {}));
    if !client.is_authorized() {
        println!(
            "[{label}] FAIL: authorization timeout status={:?} sent={} recv={}",
            client.auth_status(),
            client.total_sent(),
            client.total_recv()
        );
        loss_gate.abort();
        return;
    }

    let init = InitConfig {
        mm_orders_subscribe: None,
        subscribe_trades: Some(false),
        subscribe_orderbooks: vec![args.market.clone()],
        step_timeout: Some(INIT_TIMEOUT),
    };
    match run_init_sequence(&mut client, &mut dispatcher, init) {
        Ok(result) => {
            stats.init_ok.store(true, Ordering::Relaxed);
            for err in &result.errors {
                println!("[{label}] init note: {err}");
            }
            println!(
                "[{label}] init ok base={} auth={} markets={}B",
                result.base_check_ok, result.auth_check_ok, result.markets_response_bytes
            );
        }
        Err(err) => {
            println!(
                "[{label}] FAIL: init error {err} status={:?} sent={} recv={}",
                client.auth_status(),
                client.total_sent(),
                client.total_recv()
            );
            loss_gate.abort();
            return;
        }
    }
    *stats.server_info.lock().unwrap() = client.server_info().clone();

    if !loss_gate.wait_after_init(label) {
        println!("[{label}] FAIL: post-init err_emu gate aborted");
        return;
    }

    client.subscribe_all_trades(false);
    client.subscribe_orderbook(&args.market);
    client.ui_mm_subscribe(true);
    client.ui_settings_request();
    client.balance_request_refresh();

    let stop_churn = Arc::new(AtomicBool::new(false));
    let churn = spawn_subscription_churn(
        label.to_string(),
        client.sender(),
        args.market.clone(),
        Arc::clone(&stop_churn),
        Arc::clone(&stats),
    );

    let stress_started = Instant::now();
    *stats.stress_started_at.lock().unwrap() = Some(stress_started);
    let deadline = stress_started + args.duration;
    let mut pending = VecDeque::new();
    let mut pending_candles = VecDeque::new();
    let mut burst_no = 0u64;
    let mut next_burst = Instant::now();
    let mut helper_round = 0u64;
    let mut next_helper = Instant::now() + Duration::from_secs(10);
    let mut next_tracked_status = Instant::now() + Duration::from_secs(18);
    let mut next_report = Instant::now() + Duration::from_secs(15);

    println!(
        "[{label}] stress loop for {}s market={}",
        args.duration.as_secs(),
        args.market
    );

    while Instant::now() < deadline {
        let now = Instant::now();
        let remaining = deadline.saturating_duration_since(now);
        if now >= next_burst && remaining > API_WARN_TIMEOUT + Duration::from_secs(2) {
            schedule_safe_burst(
                &mut client,
                &mut pending,
                &mut pending_candles,
                &stats,
                &args.market,
                burst_no,
                remaining > CANDLES_WARN_TIMEOUT + Duration::from_secs(2),
            );
            burst_no = burst_no.wrapping_add(1);
            next_burst = now + Duration::from_secs(2);
        }

        if now >= next_helper && remaining > HELPER_TIMEOUT + Duration::from_secs(2) {
            run_one_shot_helper_round(label, &mut client, &mut dispatcher, &stats, helper_round);
            helper_round = helper_round.wrapping_add(1);
            next_helper = Instant::now() + Duration::from_secs(23);
        }

        if now >= next_tracked_status {
            send_tracked_status_requests(label, &client, &dispatcher, &stats);
            next_tracked_status = Instant::now() + Duration::from_secs(19);
        }

        drain_pending(label, &mut pending, &stats);
        drain_pending_candles(label, &mut pending_candles, &stats);

        let tick = TICK.min(deadline.saturating_duration_since(Instant::now()));
        let stats_cb = Arc::clone(&stats);
        client.run_with_dispatcher(
            tick,
            &mut dispatcher,
            Box::new(move |event| handle_event(label, event, &stats_cb)),
        );
        record_protocol_sample(&client, &dispatcher, &stats, stress_started.elapsed());

        if Instant::now() >= next_report {
            println!(
                "[{label}] stats events={} trades={} ob={} api_ok={} api_overdue={} api_timeout={} pending={} candles_ok={} candles_overdue={} candles_timeout={} candles_pending={} helper_ok={} helper_queued={} sent={} recv={} rtt={}ms pmtu={} rs={:.3} overheat={:.2}% trade_buckets={} sliced={}/{} pending_h={}",
                stats.events_total.load(Ordering::Relaxed),
                stats.trades_apply.load(Ordering::Relaxed),
                stats.orderbook_apply.load(Ordering::Relaxed),
                stats.api_ok.load(Ordering::Relaxed),
                stats.api_overdue.load(Ordering::Relaxed),
                stats.api_timeout.load(Ordering::Relaxed),
                pending.len(),
                stats.candles_chunked_ok.load(Ordering::Relaxed),
                stats.candles_chunked_overdue.load(Ordering::Relaxed),
                stats.candles_chunked_timeout.load(Ordering::Relaxed),
                pending_candles.len(),
                stats.helper_settings_ok.load(Ordering::Relaxed)
                    + stats.helper_balance_ok.load(Ordering::Relaxed)
                    + stats.helper_orders_ok.load(Ordering::Relaxed),
                stats.helper_queued_events.load(Ordering::Relaxed),
                client.total_sent(),
                client.total_recv(),
                client.round_trip_delay_ms(),
                client.actual_pmtu(),
                client.rs(),
                client.avg_over_heat(),
                dispatcher.trades().used_buckets(),
                client.sliced_in_flight_count(),
                client.sliced_in_flight_blocks(),
                client.pending_high_count(),
            );
            next_report = Instant::now() + Duration::from_secs(15);
        }
    }

    stop_churn.store(true, Ordering::Relaxed);

    let drain_window = if API_HARD_TIMEOUT > CANDLES_HARD_TIMEOUT {
        API_HARD_TIMEOUT
    } else {
        CANDLES_HARD_TIMEOUT
    };
    let drain_deadline = Instant::now() + drain_window;
    if !pending.is_empty() || !pending_candles.is_empty() {
        println!(
            "[{label}] draining in-flight work api_pending={} candles_pending={} hard_window={}s",
            pending.len(),
            pending_candles.len(),
            drain_window.as_secs()
        );
    }
    while (!pending.is_empty() || !pending_candles.is_empty()) && Instant::now() < drain_deadline {
        drain_pending(label, &mut pending, &stats);
        drain_pending_candles(label, &mut pending_candles, &stats);

        if pending.is_empty() && pending_candles.is_empty() {
            break;
        }

        let tick = TICK.min(drain_deadline.saturating_duration_since(Instant::now()));
        if tick.is_zero() {
            break;
        }
        let stats_cb = Arc::clone(&stats);
        client.run_with_dispatcher(
            tick,
            &mut dispatcher,
            Box::new(move |event| handle_event(label, event, &stats_cb)),
        );
        record_protocol_sample(&client, &dispatcher, &stats, stress_started.elapsed());
    }

    drain_pending(label, &mut pending, &stats);
    drain_pending_candles(label, &mut pending_candles, &stats);
    for item in pending {
        let age = Instant::now().duration_since(item.sent_at);
        println!(
            "[{label}] pending at shutdown method={:?} age={}ms",
            item.method,
            duration_ms(age)
        );
        stats.api_timeout.fetch_add(1, Ordering::Relaxed);
    }
    for _ in pending_candles {
        println!("[{label}] chunked candles pending at shutdown");
        stats
            .candles_chunked_timeout
            .fetch_add(1, Ordering::Relaxed);
    }

    record_protocol_sample(&client, &dispatcher, &stats, stress_started.elapsed());
    let _ = churn.join();
    client.disconnect();
    client.run_with_dispatcher(
        Duration::from_millis(200),
        &mut dispatcher,
        Box::new(|_| {}),
    );
    println!(
        "[{label}] done status={:?} ping={} sent={} recv={}",
        client.auth_status(),
        client.ping_count(),
        client.total_sent(),
        client.total_recv()
    );
}

fn avg_ms(sum: u64, count: u64) -> u64 {
    if count == 0 {
        0
    } else {
        sum / count
    }
}

fn mbps(bytes: u64, runtime_ms: u64) -> f64 {
    if runtime_ms == 0 {
        0.0
    } else {
        bytes as f64 * 8_000.0 / runtime_ms as f64 / 1_000_000.0
    }
}

fn success_pct(ok: u64, sent: u64) -> f64 {
    if sent == 0 {
        100.0
    } else {
        ok as f64 * 100.0 / sent as f64
    }
}

fn pct(part: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        part as f64 * 100.0 / total as f64
    }
}

fn retry_loss_pct(loss_p: f64, tries: i32) -> f64 {
    loss_p.powi(tries) * 100.0
}

fn factor(actual_pct: f64, theory_pct: f64) -> f64 {
    if theory_pct <= f64::EPSILON {
        0.0
    } else {
        actual_pct / theory_pct
    }
}

fn trades_loss_gate(actual_lost: u64, gap_packets: u64, observed_live_loss_p: f64) -> (bool, f64) {
    let expected_lost = gap_packets as f64 * observed_live_loss_p.powi(3);
    let ok =
        actual_lost <= 1 || actual_lost as f64 <= expected_lost * PROTOCOL_MAX_TRADES_LOSS_FACTOR;
    (ok, expected_lost)
}

fn print_report(stats_a: &SharedStats, stats_b: &SharedStats) -> bool {
    println!();
    println!("========== STRESS REPORT ==========");
    let mut ok = true;

    for stats in [stats_a, stats_b] {
        let label = stats.label.lock().unwrap().clone();
        let client_id = *stats.client_id.lock().unwrap();
        let info = stats.server_info.lock().unwrap().clone();
        let authorized = stats.authorized.load(Ordering::Relaxed);
        let init_ok = stats.init_ok.load(Ordering::Relaxed);
        let trades = stats.trades_apply.load(Ordering::Relaxed);
        let ob = stats.orderbook_apply.load(Ordering::Relaxed);
        let api_sent = stats.api_sent.load(Ordering::Relaxed);
        let api_ok = stats.api_ok.load(Ordering::Relaxed);
        let api_error = stats.api_error.load(Ordering::Relaxed);
        let api_overdue = stats.api_overdue.load(Ordering::Relaxed);
        let api_timeout = stats.api_timeout.load(Ordering::Relaxed);
        let api_disconnected = stats.api_disconnected.load(Ordering::Relaxed);
        let candles_sent = stats.candles_chunked_sent.load(Ordering::Relaxed);
        let candles_ok = stats.candles_chunked_ok.load(Ordering::Relaxed);
        let candles_overdue = stats.candles_chunked_overdue.load(Ordering::Relaxed);
        let candles_timeout = stats.candles_chunked_timeout.load(Ordering::Relaxed);
        let candles_disconnected = stats.candles_chunked_disconnected.load(Ordering::Relaxed);
        let candles_empty = stats.candles_chunked_empty.load(Ordering::Relaxed);
        let helper_timeout = stats.helper_settings_timeout.load(Ordering::Relaxed)
            + stats.helper_balance_timeout.load(Ordering::Relaxed)
            + stats.helper_orders_timeout.load(Ordering::Relaxed);
        let helper_disconnected = stats.helper_settings_disconnected.load(Ordering::Relaxed)
            + stats.helper_balance_disconnected.load(Ordering::Relaxed)
            + stats.helper_orders_disconnected.load(Ordering::Relaxed);
        let parse_failed = stats.parse_failed.load(Ordering::Relaxed);
        let invalid_numbers = stats.invalid_numbers.load(Ordering::Relaxed);
        let runtime_ms = stats.protocol_runtime_ms.load(Ordering::Relaxed);
        let sent_bytes = stats
            .protocol_total_sent_bytes
            .load(Ordering::Relaxed)
            .max(stats.protocol_max_sent_bytes.load(Ordering::Relaxed));
        let recv_bytes = stats
            .protocol_total_recv_bytes
            .load(Ordering::Relaxed)
            .max(stats.protocol_max_recv_bytes.load(Ordering::Relaxed));
        let recv_mbps = mbps(recv_bytes, runtime_ms);
        let sent_mbps = mbps(sent_bytes, runtime_ms);
        let min_pmtu = stats.protocol_min_pmtu.load(Ordering::Relaxed);
        let rs_min =
            1.0 - stats.protocol_max_rs_drop_ppm.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let overheat_max = stats
            .protocol_max_overheat_milli_pct
            .load(Ordering::Relaxed) as f64
            / 1000.0;
        let api_max_latency = stats.api_max_latency_ms.load(Ordering::Relaxed);
        let candles_max_latency = stats.candles_chunked_max_latency_ms.load(Ordering::Relaxed);
        let trades_bucket_lost = stats.trades_gap_bucket_closed_lost.load(Ordering::Relaxed);
        let trades_lost_bucket_span = stats.trades_gap_lost_packets.load(Ordering::Relaxed);
        let trades_gap_packets = stats.trades_gap_packets.load(Ordering::Relaxed);
        let trades_gap_filled = stats.trades_gap_filled.load(Ordering::Relaxed);
        let trades_out_of_order = stats.trades_gap_out_of_order_resend.load(Ordering::Relaxed);
        let trades_diag = stats.trades_gap_diag.lock().unwrap().summarize();
        let trades_active_max = stats.trades_active_buckets_max.load(Ordering::Relaxed);
        let max_pending_api = stats.max_pending_api.load(Ordering::Relaxed);
        let max_pending_candles = stats.max_pending_candles.load(Ordering::Relaxed);
        let live_delivered_est = trades.saturating_sub(trades_gap_filled + trades_out_of_order);
        let observed_live_loss_p = if live_delivered_est + trades_gap_packets == 0 {
            0.0
        } else {
            trades_gap_packets as f64 / (live_delivered_est + trades_gap_packets) as f64
        };
        let configured_loss_p = stats.protocol_err_emu_pct.load(Ordering::Relaxed) as f64 / 100.0;
        let theory_3req_config_pct = retry_loss_pct(configured_loss_p, 3);
        let theory_3req_observed_pct = retry_loss_pct(observed_live_loss_p, 3);
        let theory_2window_observed_pct = retry_loss_pct(observed_live_loss_p, 2);
        let fact_lost_at_close_pct = pct(trades_diag.not_applied_at_close, trades_gap_packets);
        let fact_never_applied_after_close_pct =
            pct(trades_diag.never_applied_after_close, trades_gap_packets);
        let (loss_gate_close_ok, expected_lost_3req) = trades_loss_gate(
            trades_diag.not_applied_at_close,
            trades_gap_packets,
            observed_live_loss_p,
        );
        let (loss_gate_final_ok, _) = trades_loss_gate(
            trades_diag.never_applied_after_close,
            trades_gap_packets,
            observed_live_loss_p,
        );
        let close_wait_avg = avg_ms(
            trades_diag.closed_after_last_request_sum_ms,
            trades_diag.closed_after_last_request_count,
        );
        let closed_lifetime_avg = avg_ms(
            trades_diag.closed_lifetime_sum_ms,
            trades_diag.closed_lifetime_count,
        );
        let not_applied_close_wait_avg = avg_ms(
            trades_diag.not_applied_close_after_last_request_sum_ms,
            trades_diag.not_applied_close_after_last_request_count,
        );
        let late_delay_avg = avg_ms(
            trades_diag.late_after_close_delay_sum_ms,
            trades_diag.applied_late_after_close,
        );

        println!("[{label}] client_id={client_id:#x}");
        println!(
            "[{label}] auth={} init={} fresh={} reconnects={} server_restarts={} bind_failed={}",
            authorized,
            init_ok,
            stats.lifecycle_connected_fresh.load(Ordering::Relaxed),
            stats.lifecycle_reconnecting.load(Ordering::Relaxed),
            stats.lifecycle_server_restart.load(Ordering::Relaxed),
            stats.lifecycle_bind_failed.load(Ordering::Relaxed),
        );
        println!(
            "[{label}] events={} trades={} trades_gap={} dup={} ob={} ob_full={} balance={} order={} settings={} markets={} engine={} strat={} strat_full={} strat_partial={} strat_req={} logs={} parse_failed={}",
            stats.events_total.load(Ordering::Relaxed),
            trades,
            stats.trades_gap.load(Ordering::Relaxed),
            stats.trades_dup.load(Ordering::Relaxed),
            ob,
            stats.orderbook_full.load(Ordering::Relaxed),
            stats.balance_events.load(Ordering::Relaxed),
            stats.order_events.load(Ordering::Relaxed),
            stats.settings_events.load(Ordering::Relaxed),
            stats.market_events.load(Ordering::Relaxed),
            stats.engine_events.load(Ordering::Relaxed),
            stats.strat_events.load(Ordering::Relaxed),
            stats.strat_snapshot_full.load(Ordering::Relaxed),
            stats.strat_snapshot_partial.load(Ordering::Relaxed),
            stats.strat_snapshot_requested.load(Ordering::Relaxed),
            stats.server_logs.load(Ordering::Relaxed),
            parse_failed,
        );
        println!(
            "[{label}] protocol bytes sent={:.2}MiB recv={:.2}MiB sent_mbps={:.3} recv_mbps={:.3} runtime={}ms rtt_max={}ms net_lag_max={}ms pmtu_min={} rs_min={:.3} overheat_max={:.2}% sliced_max={}/{} pending_h_max={}",
            sent_bytes as f64 / 1_048_576.0,
            recv_bytes as f64 / 1_048_576.0,
            sent_mbps,
            recv_mbps,
            runtime_ms,
            stats.protocol_max_rtt_ms.load(Ordering::Relaxed),
            stats.protocol_max_net_lag_ms.load(Ordering::Relaxed),
            min_pmtu,
            rs_min,
            overheat_max,
            stats.protocol_max_sliced_in_flight.load(Ordering::Relaxed),
            stats.protocol_max_sliced_blocks.load(Ordering::Relaxed),
            stats.protocol_max_pending_h.load(Ordering::Relaxed),
        );
        println!(
            "[{label}] trades_recovery gap_events={} gap_packets={} filled_packets={} buckets_ok={} buckets_lost={} lost_packets_at_close={} lost_bucket_span_at_close={} active_final={} active_max={} out_of_order_resend={} resend_ticks={} resend_packet_requests={}",
            stats.trades_gap.load(Ordering::Relaxed),
            trades_gap_packets,
            trades_gap_filled,
            stats.trades_gap_bucket_closed_ok.load(Ordering::Relaxed),
            trades_bucket_lost,
            trades_diag.not_applied_at_close,
            trades_lost_bucket_span,
            stats.trades_active_buckets_final.load(Ordering::Relaxed),
            trades_active_max,
            trades_out_of_order,
            stats.trades_resend_ticks.load(Ordering::Relaxed),
            stats.trades_resend_packet_requests.load(Ordering::Relaxed),
        );
        println!(
            "[{label}] trades_loss_model live_delivered_est={} observed_live_loss={:.3}% configured_loss={:.3}% theory_3req_config={:.4}% theory_3req_observed={:.4}% theory_2window_observed={:.3}% fact_lost_at_close={:.3}% fact_never_applied_after_close={:.3}% fact_over_theory_3req_observed={:.1}x final_over_theory_3req_observed={:.1}x",
            live_delivered_est,
            observed_live_loss_p * 100.0,
            configured_loss_p * 100.0,
            theory_3req_config_pct,
            theory_3req_observed_pct,
            theory_2window_observed_pct,
            fact_lost_at_close_pct,
            fact_never_applied_after_close_pct,
            factor(fact_lost_at_close_pct, theory_3req_observed_pct),
            factor(
                fact_never_applied_after_close_pct,
                theory_3req_observed_pct
            ),
        );
        println!(
            "[{label}] trades_loss_gate expected_lost_3req={:.2} max_factor={:.1} actual_close={} actual_final={} close_ok={} final_ok={}",
            expected_lost_3req,
            PROTOCOL_MAX_TRADES_LOSS_FACTOR,
            trades_diag.not_applied_at_close,
            trades_diag.never_applied_after_close,
            loss_gate_close_ok,
            loss_gate_final_ok,
        );
        println!(
            "[{label}] trades_gap_timeline unique_gap_packets={} closed_packets={} applied_before_close={} applied_late_after_close={} never_applied_after_close={} not_applied_at_close={} avg_resend_requests_per_gap={:.2} max_requests_for_one_packet={} close_lifetime_ms={}/{} close_after_last_request_ms={}/{}/{} lost_close_after_last_request_ms={}/{}/{} late_after_close_delay_ms={}/{} closed_without_resend_request={} closed_untracked_packets={}",
            trades_diag.unique_gap_packets,
            trades_diag.closed_packets,
            trades_diag.applied_before_close,
            trades_diag.applied_late_after_close,
            trades_diag.never_applied_after_close,
            trades_diag.not_applied_at_close,
            if trades_diag.unique_gap_packets == 0 {
                0.0
            } else {
            trades_diag.resend_packet_requests as f64 / trades_diag.unique_gap_packets as f64
            },
            trades_diag.max_requests_for_one_packet,
            closed_lifetime_avg,
            trades_diag.closed_lifetime_max_ms,
            trades_diag.closed_after_last_request_min_ms,
            close_wait_avg,
            trades_diag.closed_after_last_request_max_ms,
            trades_diag.not_applied_close_after_last_request_min_ms,
            not_applied_close_wait_avg,
            trades_diag.not_applied_close_after_last_request_max_ms,
            late_delay_avg,
            trades_diag.late_after_close_delay_max_ms,
            trades_diag.closed_without_resend_request,
            trades_diag.closed_untracked_packets,
        );
        println!(
            "[{label}] trades_resend_distribution buckets=0req/1req/2req/3req/4plus closed={:?} applied_before_close={:?} late_after_close={:?} never_applied={:?} not_applied_at_close={:?}",
            trades_diag.closed_by_resend_requests,
            trades_diag.applied_before_close_by_resend_requests,
            trades_diag.applied_late_after_close_by_resend_requests,
            trades_diag.never_applied_after_close_by_resend_requests,
            trades_diag.not_applied_at_close_by_resend_requests,
        );
        println!(
            "[{label}] api sent={} ok={} success={:.2}% error={} overdue={} completed_after_overdue={} timeout={} disconnected={} max_pending={} avg_latency_ms={} max_latency_ms={} settings_req={} balance_refresh={} sub_ops={} invalid_numbers={}",
            api_sent,
            api_ok,
            success_pct(api_ok, api_sent),
            api_error,
            api_overdue,
            stats.api_completed_after_overdue.load(Ordering::Relaxed),
            api_timeout,
            api_disconnected,
            max_pending_api,
            avg_ms(stats.api_latency_sum_ms.load(Ordering::Relaxed), api_ok + api_error),
            api_max_latency,
            stats.settings_requests.load(Ordering::Relaxed),
            stats.balance_refresh_requests.load(Ordering::Relaxed),
            stats.subscription_ops.load(Ordering::Relaxed),
            invalid_numbers,
        );
        println!(
            "[{label}] candles_chunked sent={} ok={} success={:.2}% overdue={} completed_after_overdue={} timeout={} disconnected={} empty={} max_pending={} avg_latency_ms={} max_latency_ms={}",
            candles_sent,
            candles_ok,
            success_pct(candles_ok, candles_sent),
            candles_overdue,
            stats
                .candles_chunked_completed_after_overdue
                .load(Ordering::Relaxed),
            candles_timeout,
            candles_disconnected,
            candles_empty,
            max_pending_candles,
            avg_ms(
                stats.candles_chunked_latency_sum_ms.load(Ordering::Relaxed),
                candles_ok,
            ),
            candles_max_latency,
        );
        println!(
            "[{label}] helpers settings={}/{}/{}/{} balance={}/{}/{}/{} orders={}/{}/{}/{} queued_events={} max_queued_batch={}",
            stats.helper_settings_sent.load(Ordering::Relaxed),
            stats.helper_settings_ok.load(Ordering::Relaxed),
            stats.helper_settings_timeout.load(Ordering::Relaxed),
            stats.helper_settings_disconnected.load(Ordering::Relaxed),
            stats.helper_balance_sent.load(Ordering::Relaxed),
            stats.helper_balance_ok.load(Ordering::Relaxed),
            stats.helper_balance_timeout.load(Ordering::Relaxed),
            stats.helper_balance_disconnected.load(Ordering::Relaxed),
            stats.helper_orders_sent.load(Ordering::Relaxed),
            stats.helper_orders_ok.load(Ordering::Relaxed),
            stats.helper_orders_timeout.load(Ordering::Relaxed),
            stats.helper_orders_disconnected.load(Ordering::Relaxed),
            stats.helper_queued_events.load(Ordering::Relaxed),
            stats.helper_max_queued_events.load(Ordering::Relaxed),
        );
        println!(
            "[{label}] tracked_status rounds={} sent={} empty_rounds={}",
            stats.tracked_status_rounds.load(Ordering::Relaxed),
            stats.tracked_status_sent.load(Ordering::Relaxed),
            stats.tracked_status_empty.load(Ordering::Relaxed),
        );
        println!(
            "[{label}] binance_tags sent={} ok={} empty={} malformed={} max_items={}",
            stats.binance_tags_sent.load(Ordering::Relaxed),
            stats.binance_tags_ok.load(Ordering::Relaxed),
            stats.binance_tags_empty.load(Ordering::Relaxed),
            stats.binance_tags_malformed.load(Ordering::Relaxed),
            stats.binance_tags_max_items.load(Ordering::Relaxed),
        );
        println!(
            "[{label}] server_info bot_id={:?} name={:?} exchange={:?} base={:?} version={:?}",
            info.bot_id,
            info.server_name,
            info.exchange_name,
            info.base_currency_name,
            info.server_version,
        );

        if !authorized || !init_ok {
            ok = false;
        }
        if trades == 0 {
            println!("[{label}] FAIL: subscribed trades stream produced no apply events");
            ok = false;
        }
        if ob == 0 {
            println!("[{label}] FAIL: subscribed orderbook produced no apply events");
            ok = false;
        }
        if api_timeout > 0
            || api_disconnected > 0
            || api_error > 0
            || api_overdue > 0
            || candles_timeout > 0
            || candles_disconnected > 0
            || candles_empty > 0
            || candles_overdue > 0
            || helper_timeout > 0
            || helper_disconnected > 0
            || parse_failed > 0
            || invalid_numbers > 0
        {
            ok = false;
        }
        if api_sent > 0 && api_ok != api_sent {
            println!("[{label}] FAIL: not every Engine API request completed successfully");
            ok = false;
        }
        if candles_sent > 0 && candles_ok != candles_sent {
            println!("[{label}] FAIL: not every chunked candles snapshot completed");
            ok = false;
        }
        if api_max_latency > PROTOCOL_MAX_API_LATENCY_MS {
            println!(
                "[{label}] FAIL: Engine API max latency {}ms exceeds protocol gate {}ms",
                api_max_latency, PROTOCOL_MAX_API_LATENCY_MS
            );
            ok = false;
        }
        if candles_max_latency > PROTOCOL_MAX_CANDLES_LATENCY_MS {
            println!(
                "[{label}] FAIL: chunked candles max latency {}ms exceeds protocol gate {}ms",
                candles_max_latency, PROTOCOL_MAX_CANDLES_LATENCY_MS
            );
            ok = false;
        }
        if runtime_ms >= 30_000 && recv_mbps < PROTOCOL_MIN_RECV_MBPS {
            println!(
                "[{label}] FAIL: recv throughput {:.3} Mbps below protocol floor {:.3} Mbps",
                recv_mbps, PROTOCOL_MIN_RECV_MBPS
            );
            ok = false;
        }
        if !loss_gate_close_ok || !loss_gate_final_ok {
            println!("[{label}] FAIL: TradesStream gap recovery worse than p^3 loss gate");
            ok = false;
        }
        if trades_active_max > PROTOCOL_MAX_TRADES_BUCKETS {
            println!(
                "[{label}] FAIL: TradesStream active gap buckets hit {} (gate {})",
                trades_active_max, PROTOCOL_MAX_TRADES_BUCKETS
            );
            ok = false;
        }
        if max_pending_api >= MAX_PENDING_API as u64 {
            println!("[{label}] FAIL: stress hit MAX_PENDING_API cap");
            ok = false;
        }
        if max_pending_candles >= MAX_PENDING_CANDLES as u64 {
            println!("[{label}] FAIL: stress hit MAX_PENDING_CANDLES cap");
            ok = false;
        }
    }

    let a_id = *stats_a.client_id.lock().unwrap();
    let b_id = *stats_b.client_id.lock().unwrap();
    if a_id == b_id {
        println!("FAIL: client identities collided: {a_id:#x}");
        ok = false;
    }

    let a_info = stats_a.server_info.lock().unwrap().clone();
    let b_info = stats_b.server_info.lock().unwrap().clone();
    if a_info.bot_id != b_info.bot_id {
        println!(
            "WARN: bot_id differs between clients: A={:?} B={:?}",
            a_info.bot_id, b_info.bot_id
        );
    }

    println!("========== VERDICT ==========");
    if ok {
        println!(
            "PASS: protocol health is green: throughput, latency, loss recovery, payload validity, and pending/backlog gates passed."
        );
    } else {
        println!("FAIL: see counters above.");
    }
    ok
}

fn main() {
    let args = parse_args();
    if args.err_emu_pct > 0 {
        match args.err_emu_phase {
            ErrEmuPhase::PreConnect => {
                moonproto::client::set_err_emu(args.err_emu_pct);
                println!(
                    "[main] client-side err_emu={} enabled before connect",
                    args.err_emu_pct
                );
            }
            ErrEmuPhase::PostInit => {
                moonproto::client::set_err_emu(0);
                println!(
                    "[main] client-side err_emu={} will be enabled after both clients init",
                    args.err_emu_pct
                );
            }
        }
    }
    let keys = key_import::import_key(&args.key_b64).expect("invalid key");

    println!(
        "[main] stress target {}:{} market={} duration={}s err_emu={} phase={}",
        args.host,
        args.port,
        args.market,
        args.duration.as_secs(),
        args.err_emu_pct,
        args.err_emu_phase.as_str()
    );

    let stats_a = Arc::new(SharedStats::default());
    let stats_b = Arc::new(SharedStats::default());
    let loss_gate = Arc::new(LossGate::new(2, args.err_emu_pct, args.err_emu_phase));
    let args_a = args.clone();
    let args_b = args;
    let keys_a = keys;
    let keys_b = keys;
    let stats_a_thread = Arc::clone(&stats_a);
    let stats_b_thread = Arc::clone(&stats_b);
    let loss_gate_a = Arc::clone(&loss_gate);
    let loss_gate_b = Arc::clone(&loss_gate);

    let a = thread::spawn(move || run_one_client("A", args_a, keys_a, stats_a_thread, loss_gate_a));
    thread::sleep(Duration::from_millis(250));
    let b = thread::spawn(move || run_one_client("B", args_b, keys_b, stats_b_thread, loss_gate_b));

    let a_result = a.join();
    let b_result = b.join();
    if a_result.is_err() {
        println!("FAIL: client A thread panicked");
    }
    if b_result.is_err() {
        println!("FAIL: client B thread panicked");
    }

    let ok = a_result.is_ok() && b_result.is_ok() && print_report(&stats_a, &stats_b);
    std::process::exit(if ok { 0 } else { 1 });
}
