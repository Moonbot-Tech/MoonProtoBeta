//! Live-server integration smoke test for the public `MoonClient` happy path.
//!
//! Run:
//! ```powershell
//! $env:MOONPROTO_LIVE_SERVER = "HOST:PORT"
//! $env:MOONPROTO_KEY = "<exported MoonBot key>"
//! cargo test --test integration_smoke -- --ignored --nocapture
//! ```
//!
//! This test intentionally uses the same API shape expected from desktop/UI
//! apps: one `MoonClient`, no manual pump duration, no low-level dispatcher.

use std::env;
use std::thread;
use std::time::{Duration, Instant};

use moonproto::state::OrderBookEvent;
use moonproto::{
    parse_key_info, ClientConfig, ConnectConfig, Event, InitConfig, InitialStrategies,
    ExchangeCode, LifecycleEvent, MoonClient, TradesStreamMode,
};

const STREAM_DURATION_SECS: u64 = 15;

#[test]
fn exchange_code_stable_id_is_public_without_diagnostics() {
    assert_eq!(ExchangeCode::Binance.stable_id(), 3);
    assert_eq!(ExchangeCode::FBinance.stable_id(), 4);
    assert_eq!(ExchangeCode::FBybit.stable_id(), 2);
}

fn wait_ready(client: &MoonClient, timeout: Duration) -> Vec<LifecycleEvent> {
    let deadline = Instant::now() + timeout;
    let mut lifecycle = Vec::new();
    while Instant::now() < deadline {
        for event in client.drain_lifecycle_events() {
            if let LifecycleEvent::ConnectFailed { error } = &event {
                panic!("FAIL: MoonClient connect/init failed: {error}");
            }
            let ready = matches!(event, LifecycleEvent::Ready);
            lifecycle.push(event);
            if ready {
                return lifecycle;
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("FAIL: MoonClient did not emit Ready within {timeout:?}; lifecycle={lifecycle:?}");
}

fn load_env() -> Option<(String, u16, String)> {
    let server = env::var("MOONPROTO_LIVE_SERVER").ok()?;
    let (ip, port_str) = server.rsplit_once(':')?;
    let port: u16 = port_str.parse().ok()?;
    let key_b64 = env::var("MOONPROTO_KEY").ok()?;
    Some((ip.to_string(), port, key_b64))
}

#[test]
#[ignore = "live-server required; set MOONPROTO_LIVE_SERVER + MOONPROTO_KEY env"]
fn runtime_smoke_full_happy_path() {
    let (ip, port, key_b64) = match load_env() {
        Some(v) => v,
        None => {
            eprintln!("RUNTIME_SMOKE_SKIP: env MOONPROTO_LIVE_SERVER + MOONPROTO_KEY not set");
            return;
        }
    };

    println!("=== MoonClient smoke starting against {ip}:{port} ===");
    let info = parse_key_info(&key_b64).expect("invalid MOONPROTO_KEY");
    println!("OK: key_import label={}", info.display_name);

    let cfg = ClientConfig::new(&ip, port, info.keys.master_key, info.keys.mac_key);
    let init = InitConfig {
        initial_strategies: Some(InitialStrategies::new(0, Vec::new())),
        subscribe_trades: Some(TradesStreamMode::TradesOnly),
        subscribe_orderbooks: vec!["BTCUSDT".to_string()],
        step_timeout: None,
        ..Default::default()
    };
    let client = MoonClient::connect(
        cfg,
        ConnectConfig::new(init).with_connect_timeout(Duration::from_secs(15)),
    )
    .expect("FAIL: MoonClient::connect failed to start runtime");

    let lifecycle = wait_ready(&client, Duration::from_secs(20));
    assert!(
        lifecycle
            .iter()
            .any(|event| matches!(event, LifecycleEvent::Connected { fresh: true })),
        "FAIL: MoonClient did not expose initial Connected lifecycle event: {lifecycle:?}"
    );
    println!("OK: lifecycle_events {:?}", lifecycle);

    let snapshot = client
        .snapshot()
        .expect("FAIL: no initial MoonStateSnapshot after connect");
    assert!(
        snapshot.markets().market_count() > 0,
        "FAIL: markets are empty after Init"
    );
    assert!(
        snapshot.markets().indexes_synchronized(),
        "FAIL: market indexes gate is closed after Init"
    );
    println!(
        "OK: initial_snapshot markets={} orders={} total_pnl={} strategies={}",
        snapshot.markets().market_count(),
        snapshot.orders().len(),
        snapshot.balances().global().total_pnl,
        snapshot.strategy_snapshots().count()
    );

    client
        .balances()
        .refresh()
        .expect("FAIL: MoonClient balances().refresh failed");
    let balance_deadline = Instant::now() + Duration::from_secs(15);
    let mut balance_events = 0u32;
    while Instant::now() < balance_deadline {
        for event in client.drain_events() {
            match event {
                Event::Balance(_) => {
                    balance_events += 1;
                    break;
                }
                _ => {}
            }
        }
        if balance_events > 0 {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        balance_events > 0,
        "FAIL: no Balance event after request_balance_snapshot"
    );
    let snapshot = client
        .snapshot()
        .expect("FAIL: no snapshot after balance refresh");
    let global = snapshot.balances().global();
    assert!(
        global.btc_balance_total.abs()
            + global.btc_balance_full.abs()
            + global.special_coin_balance.abs()
            > 0.0,
        "FAIL: maintained global balances are zero after balance refresh"
    );
    println!(
        "OK: maintained_balance_state events={balance_events} btc_total={:.8} btc_full={:.8} special_coin={:.8}",
        global.btc_balance_total, global.btc_balance_full, global.special_coin_balance
    );

    let mut trades_packets = 0u32;
    let mut orderbook_applied = 0u32;
    let deadline = Instant::now() + Duration::from_secs(STREAM_DURATION_SECS);

    while Instant::now() < deadline {
        for event in client.drain_events() {
            match event {
                Event::Trade(_) => trades_packets += 1,
                Event::OrderBook(OrderBookEvent::Apply { .. }) => orderbook_applied += 1,
                _ => {}
            }
        }
        thread::sleep(Duration::from_millis(50));
    }

    println!("streaming stats: trades={trades_packets} books={orderbook_applied}");
    assert!(
        trades_packets > 0,
        "FAIL: 0 Trades packets in {STREAM_DURATION_SECS}s"
    );
    println!("OK: trades_stream ({trades_packets} packets)");
    assert!(
        orderbook_applied > 0,
        "FAIL: 0 OrderBook Apply events in {STREAM_DURATION_SECS}s"
    );
    println!("OK: order_book_stream ({orderbook_applied} apply events)");
    client
        .disconnect()
        .expect("FAIL: MoonClient::disconnect failed");
    client
        .wait_finished()
        .expect("FAIL: MoonClient::wait_finished failed");
    println!("OK: stop");
    println!("\nRUNTIME_SMOKE_PASS: MoonClient happy path passed against {ip}:{port}");
}
