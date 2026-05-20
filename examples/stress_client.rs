//! Accumulating live stress client for MoonProto.
//!
//! Run two independent client instances against one server and keep several
//! subscriptions plus Engine API and chunked candles requests in flight at the
//! same time:
//!
//!   cargo run --example stress_client --release -- "<key_base64>" "207.148.91.186:3000" BTCUSDT 180 0
//!
//! Arguments:
//! - key_base64: exported MoonBot key.
//! - host:port: server address, default 207.148.91.186:3000.
//! - market: market used for orderbook/candles, default BTCUSDT.
//! - duration_secs: load phase duration after init, default 180.
//! - err_emu_pct: optional client-side packet drop percent, default 0.

use std::collections::VecDeque;
use std::env;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::{Duration, Instant};

use moonproto::client::{Client, ClientConfig, ClientSender, LifecycleEvent, MergedCandles};
use moonproto::commands::candles::{parse_coin_card_candles_response, DeepHistoryKind};
use moonproto::commands::{
    parse_get_balance_response, parse_query_hedge_mode_response,
};
use moonproto::commands::engine_api::{EngineMethod, EngineResponse, ServerInfo};
use moonproto::events::{Event, EventDispatcher};
use moonproto::key_import;
use moonproto::state::{OrderBookEvent, TradesEvent};
use moonproto::{run_init_sequence, InitConfig};

const DEFAULT_HOST: &str = "207.148.91.186:3000";
const DEFAULT_MARKET: &str = "BTCUSDT";
const DEFAULT_DURATION_SECS: u64 = 180;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const INIT_TIMEOUT: Duration = Duration::from_secs(12);
const TICK: Duration = Duration::from_millis(250);
const API_TIMEOUT: Duration = Duration::from_secs(20);
const CANDLES_TIMEOUT: Duration = Duration::from_secs(35);
const MAX_PENDING_API: usize = 48;
const MAX_PENDING_CANDLES: usize = 4;

#[derive(Default)]
struct SharedStats {
    label: Mutex<String>,
    client_id: Mutex<u64>,
    server_info: Mutex<ServerInfo>,
    authorized: AtomicBool,
    init_ok: AtomicBool,
    lifecycle_connected_fresh: AtomicU64,
    lifecycle_connected_again: AtomicU64,
    lifecycle_reconnecting: AtomicU64,
    lifecycle_server_restart: AtomicU64,
    lifecycle_bind_failed: AtomicU64,
    events_total: AtomicU64,
    trades_apply: AtomicU64,
    trades_gap: AtomicU64,
    trades_dup: AtomicU64,
    orderbook_apply: AtomicU64,
    orderbook_full: AtomicU64,
    orderbook_req_full: AtomicU64,
    balance_events: AtomicU64,
    order_events: AtomicU64,
    settings_events: AtomicU64,
    market_events: AtomicU64,
    engine_events: AtomicU64,
    server_logs: AtomicU64,
    parse_failed: AtomicU64,
    api_sent: AtomicU64,
    api_ok: AtomicU64,
    api_error: AtomicU64,
    api_timeout: AtomicU64,
    api_disconnected: AtomicU64,
    candles_chunked_sent: AtomicU64,
    candles_chunked_ok: AtomicU64,
    candles_chunked_timeout: AtomicU64,
    candles_chunked_disconnected: AtomicU64,
    candles_chunked_empty: AtomicU64,
    max_pending_candles: AtomicU64,
    settings_requests: AtomicU64,
    balance_refresh_requests: AtomicU64,
    strat_snapshot_requests: AtomicU64,
    subscription_ops: AtomicU64,
    invalid_numbers: AtomicU64,
    max_pending_api: AtomicU64,
}

struct PendingApi {
    method: EngineMethod,
    sent_at: Instant,
    rx: std::sync::mpsc::Receiver<EngineResponse>,
}

struct PendingCandles {
    sent_at: Instant,
    rx: std::sync::mpsc::Receiver<MergedCandles>,
}

#[derive(Clone)]
struct Args {
    key_b64: String,
    host: String,
    port: u16,
    market: String,
    duration: Duration,
    err_emu_pct: u8,
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
            "Usage: stress_client <key_base64> [host:port] [market] [duration_secs] [err_emu_pct]"
        );
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
    Args {
        key_b64: args[1].clone(),
        host,
        port,
        market,
        duration,
        err_emu_pct,
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
    pending.push_back(PendingApi {
        method,
        sent_at: Instant::now(),
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

    if burst_no % 3 == 0 {
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
    stats.balance_refresh_requests.fetch_add(1, Ordering::Relaxed);

    if burst_no % 4 == 0 {
        client.strat_snapshot_request();
        stats.strat_snapshot_requests.fetch_add(1, Ordering::Relaxed);
    }
}

fn drain_pending(label: &str, pending: &mut VecDeque<PendingApi>, stats: &SharedStats) {
    let now = Instant::now();
    let mut kept = VecDeque::with_capacity(pending.len());

    while let Some(item) = pending.pop_front() {
        match item.rx.try_recv() {
            Ok(resp) => {
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
                if now.duration_since(item.sent_at) >= API_TIMEOUT {
                    stats.api_timeout.fetch_add(1, Ordering::Relaxed);
                    println!("[{label}] api timeout method={:?}", item.method);
                } else {
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

fn drain_pending_candles(
    label: &str,
    pending: &mut VecDeque<PendingCandles>,
    stats: &SharedStats,
) {
    let now = Instant::now();
    let mut kept = VecDeque::with_capacity(pending.len());

    while let Some(item) = pending.pop_front() {
        match item.rx.try_recv() {
            Ok(merged) => {
                stats.candles_chunked_ok.fetch_add(1, Ordering::Relaxed);
                validate_chunked_candles(label, &merged, stats);
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                if now.duration_since(item.sent_at) >= CANDLES_TIMEOUT {
                    stats.candles_chunked_timeout.fetch_add(1, Ordering::Relaxed);
                    println!("[{label}] chunked candles timeout");
                } else {
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
                println!("[{label}] malformed GetBalance response: {} bytes", resp.data.len());
            }
        }
        EngineMethod::QueryHedgeMode => {
            if parse_query_hedge_mode_response(&resp.data).is_none() && resp.success {
                stats.invalid_numbers.fetch_add(1, Ordering::Relaxed);
                println!("[{label}] malformed QueryHedgeMode response: {} bytes", resp.data.len());
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
    if merged.candles.is_empty() {
        stats.candles_chunked_empty.fetch_add(1, Ordering::Relaxed);
        println!("[{label}] chunked candles uid={} returned 0 candles", merged.uid);
        return;
    }

    for candle in &merged.candles {
        let prices = [candle.open_p, candle.max_p, candle.min_p, candle.close_p];
        let bad = prices.iter().any(|v| !v.is_finite() || *v < 0.0)
            || !candle.vol.is_finite()
            || candle.vol < 0.0
            || !candle.time.is_finite();
        if bad {
            stats.invalid_numbers.fetch_add(1, Ordering::Relaxed);
            println!(
                "[{label}] invalid chunked candle uid={} open={} high={} low={} close={} vol={} time={}",
                merged.uid,
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
                stats.trades_gap.fetch_add(1, Ordering::Relaxed);
                println!("[{label}] trades gap detected {start}..{end}");
            }
            TradesEvent::Duplicate => {
                stats.trades_dup.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        },
        Event::OrderBook(book) => match book {
            OrderBookEvent::Apply {
                is_full,
                buys,
                sells,
                market_index,
                seq,
                ..
            } => {
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
            OrderBookEvent::RequestFullNeeded { market_index, book_kind } => {
                stats.orderbook_req_full.fetch_add(1, Ordering::Relaxed);
                println!(
                    "[{label}] orderbook full requested idx={} kind={}",
                    market_index, book_kind
                );
            }
            _ => {}
        },
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
        Event::ParseFailed { cmd, len } => {
            stats.parse_failed.fetch_add(1, Ordering::Relaxed);
            println!("[{label}] parse failed cmd={cmd:?} len={len}");
        }
        _ => {}
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
) {
    *stats.label.lock().unwrap() = label.to_string();
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
                stats.lifecycle_connected_fresh.fetch_add(1, Ordering::Relaxed);
            }
            LifecycleEvent::Connected { fresh: false } => {
                stats.authorized.store(true, Ordering::Relaxed);
                stats.lifecycle_connected_again.fetch_add(1, Ordering::Relaxed);
            }
            LifecycleEvent::Reconnecting => {
                stats.lifecycle_reconnecting.fetch_add(1, Ordering::Relaxed);
            }
            LifecycleEvent::ServerRestart => {
                stats.lifecycle_server_restart.fetch_add(1, Ordering::Relaxed);
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
        return;
    }

    let init = InitConfig {
        base_check: true,
        auth_check: true,
        fetch_markets: true,
        fetch_balance: true,
        subscribe_trades: Some(false),
        subscribe_orderbooks: vec![args.market.clone()],
        step_timeout: Some(INIT_TIMEOUT),
    };
    match run_init_sequence(&mut client, &mut dispatcher, init) {
        Ok(result) => {
            stats.init_ok.store(true, Ordering::Relaxed);
            for err in &result.errors {
                println!("[{label}] init non-critical error: {err}");
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
            return;
        }
    }
    *stats.server_info.lock().unwrap() = client.server_info().clone();

    client.subscribe_all_trades(false);
    client.subscribe_orderbook(&args.market);
    client.ui_mm_subscribe(true);
    client.ui_settings_request();
    client.balance_request_refresh();
    client.strat_snapshot_request();

    let stop_churn = Arc::new(AtomicBool::new(false));
    let churn = spawn_subscription_churn(
        label.to_string(),
        client.sender(),
        args.market.clone(),
        Arc::clone(&stop_churn),
        Arc::clone(&stats),
    );

    let deadline = Instant::now() + args.duration;
    let mut pending = VecDeque::new();
    let mut pending_candles = VecDeque::new();
    let mut burst_no = 0u64;
    let mut next_burst = Instant::now();
    let mut next_report = Instant::now() + Duration::from_secs(15);

    println!(
        "[{label}] stress loop for {}s market={}",
        args.duration.as_secs(),
        args.market
    );

    while Instant::now() < deadline {
        let now = Instant::now();
        let remaining = deadline.saturating_duration_since(now);
        if now >= next_burst && remaining > API_TIMEOUT + Duration::from_secs(2) {
            schedule_safe_burst(
                &mut client,
                &mut pending,
                &mut pending_candles,
                &stats,
                &args.market,
                burst_no,
                remaining > CANDLES_TIMEOUT + Duration::from_secs(2),
            );
            burst_no = burst_no.wrapping_add(1);
            next_burst = now + Duration::from_secs(2);
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

        if Instant::now() >= next_report {
            println!(
                "[{label}] stats events={} trades={} ob={} api_ok={} api_timeout={} pending={} candles_ok={} candles_timeout={} candles_pending={} sent={} recv={}",
                stats.events_total.load(Ordering::Relaxed),
                stats.trades_apply.load(Ordering::Relaxed),
                stats.orderbook_apply.load(Ordering::Relaxed),
                stats.api_ok.load(Ordering::Relaxed),
                stats.api_timeout.load(Ordering::Relaxed),
                pending.len(),
                stats.candles_chunked_ok.load(Ordering::Relaxed),
                stats.candles_chunked_timeout.load(Ordering::Relaxed),
                pending_candles.len(),
                client.total_sent(),
                client.total_recv(),
            );
            next_report = Instant::now() + Duration::from_secs(15);
        }
    }

    drain_pending(label, &mut pending, &stats);
    drain_pending_candles(label, &mut pending_candles, &stats);
    for item in pending {
        println!("[{label}] pending at shutdown method={:?}", item.method);
        stats.api_timeout.fetch_add(1, Ordering::Relaxed);
    }
    for _ in pending_candles {
        println!("[{label}] chunked candles pending at shutdown");
        stats.candles_chunked_timeout.fetch_add(1, Ordering::Relaxed);
    }

    stop_churn.store(true, Ordering::Relaxed);
    let _ = churn.join();
    client.disconnect();
    client.run_with_dispatcher(Duration::from_millis(200), &mut dispatcher, Box::new(|_| {}));
    println!(
        "[{label}] done status={:?} ping={} sent={} recv={}",
        client.auth_status(),
        client.ping_count(),
        client.total_sent(),
        client.total_recv()
    );
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
        let api_timeout = stats.api_timeout.load(Ordering::Relaxed);
        let api_disconnected = stats.api_disconnected.load(Ordering::Relaxed);
        let candles_timeout = stats.candles_chunked_timeout.load(Ordering::Relaxed);
        let candles_disconnected = stats.candles_chunked_disconnected.load(Ordering::Relaxed);
        let candles_empty = stats.candles_chunked_empty.load(Ordering::Relaxed);
        let parse_failed = stats.parse_failed.load(Ordering::Relaxed);
        let invalid_numbers = stats.invalid_numbers.load(Ordering::Relaxed);

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
            "[{label}] events={} trades={} trades_gap={} dup={} ob={} ob_full={} ob_req_full={} balance={} order={} settings={} markets={} engine={} logs={} parse_failed={}",
            stats.events_total.load(Ordering::Relaxed),
            trades,
            stats.trades_gap.load(Ordering::Relaxed),
            stats.trades_dup.load(Ordering::Relaxed),
            ob,
            stats.orderbook_full.load(Ordering::Relaxed),
            stats.orderbook_req_full.load(Ordering::Relaxed),
            stats.balance_events.load(Ordering::Relaxed),
            stats.order_events.load(Ordering::Relaxed),
            stats.settings_events.load(Ordering::Relaxed),
            stats.market_events.load(Ordering::Relaxed),
            stats.engine_events.load(Ordering::Relaxed),
            stats.server_logs.load(Ordering::Relaxed),
            parse_failed,
        );
        println!(
            "[{label}] api sent={} ok={} error={} timeout={} disconnected={} max_pending={} settings_req={} balance_refresh={} strat_req={} sub_ops={} invalid_numbers={}",
            stats.api_sent.load(Ordering::Relaxed),
            stats.api_ok.load(Ordering::Relaxed),
            stats.api_error.load(Ordering::Relaxed),
            api_timeout,
            api_disconnected,
            stats.max_pending_api.load(Ordering::Relaxed),
            stats.settings_requests.load(Ordering::Relaxed),
            stats.balance_refresh_requests.load(Ordering::Relaxed),
            stats.strat_snapshot_requests.load(Ordering::Relaxed),
            stats.subscription_ops.load(Ordering::Relaxed),
            invalid_numbers,
        );
        println!(
            "[{label}] candles_chunked sent={} ok={} timeout={} disconnected={} empty={} max_pending={}",
            stats.candles_chunked_sent.load(Ordering::Relaxed),
            stats.candles_chunked_ok.load(Ordering::Relaxed),
            candles_timeout,
            candles_disconnected,
            candles_empty,
            stats.max_pending_candles.load(Ordering::Relaxed),
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
            || candles_timeout > 0
            || candles_disconnected > 0
            || candles_empty > 0
            || parse_failed > 0
            || invalid_numbers > 0
        {
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
        println!("PASS: two clients stayed authorized, streamed data, and completed queued API load.");
    } else {
        println!("FAIL: see counters above.");
    }
    ok
}

fn main() {
    let args = parse_args();
    if args.err_emu_pct > 0 {
        moonproto::client::set_err_emu(args.err_emu_pct);
        println!("[main] client-side err_emu={}%", args.err_emu_pct);
    }
    let keys = key_import::import_key(&args.key_b64).expect("invalid key");

    println!(
        "[main] stress target {}:{} market={} duration={}s",
        args.host,
        args.port,
        args.market,
        args.duration.as_secs()
    );

    let stats_a = Arc::new(SharedStats::default());
    let stats_b = Arc::new(SharedStats::default());
    let args_a = args.clone();
    let args_b = args;
    let keys_a = keys.clone();
    let keys_b = keys;
    let stats_a_thread = Arc::clone(&stats_a);
    let stats_b_thread = Arc::clone(&stats_b);

    let a = thread::spawn(move || run_one_client("A", args_a, keys_a, stats_a_thread));
    thread::sleep(Duration::from_millis(250));
    let b = thread::spawn(move || run_one_client("B", args_b, keys_b, stats_b_thread));

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
