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
//! apps: one `MoonClient`, no manual pump duration, no exposed
//! `Client + EventDispatcher`.

use std::env;
use std::time::{Duration, Instant};

use moonproto::state::OrderBookEvent;
use moonproto::{
    parse_key_info, ClientConfig, ConnectConfig, Event, InitConfig, InitialStrategies,
    LifecycleEvent, MoonClient,
};

const STREAM_DURATION_SECS: u64 = 15;

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
        subscribe_trades: Some(false),
        subscribe_orderbooks: vec!["BTCUSDT".to_string()],
        step_timeout: None,
        ..Default::default()
    };
    let client = MoonClient::connect(
        cfg,
        ConnectConfig::new(init).with_connect_timeout(Duration::from_secs(15)),
    )
    .expect("FAIL: MoonClient::connect/connect+init failed");
    println!("OK: moonclient_connect_init");

    let lifecycle = client.drain_lifecycle_events();
    assert!(
        lifecycle
            .iter()
            .any(|event| matches!(event, LifecycleEvent::Connected { fresh: true })),
        "FAIL: MoonClient did not expose initial Connected lifecycle event: {lifecycle:?}"
    );
    println!("OK: lifecycle_events {:?}", lifecycle);

    let snapshot = client
        .snapshot()
        .expect("FAIL: no initial EventDispatcherSnapshot after connect");
    assert!(
        snapshot.markets().market_count() > 0,
        "FAIL: markets are empty after Init"
    );
    assert!(
        snapshot.markets().indexes_synchronized(),
        "FAIL: market indexes gate is closed after Init"
    );
    println!(
        "OK: initial_snapshot markets={} orders={} balances={} strategies={}",
        snapshot.markets().market_count(),
        snapshot.orders().len(),
        snapshot.balances().len(),
        snapshot.strategy_snapshot_vec().len()
    );

    let balance = client
        .request_balance("USDT", Duration::from_secs(15))
        .expect("FAIL: MoonClient::request_balance failed");
    println!("OK: request_balance USDT={balance}");

    let mut trades_packets = 0u32;
    let mut orderbook_applied = 0u32;
    let mut parse_failures = 0u32;
    let deadline = Instant::now() + Duration::from_secs(STREAM_DURATION_SECS);

    while Instant::now() < deadline {
        match client.recv_event_timeout(Duration::from_millis(500)) {
            Ok(Event::Trade(_)) => trades_packets += 1,
            Ok(Event::OrderBook(OrderBookEvent::Apply { .. })) => orderbook_applied += 1,
            Ok(Event::ParseFailed { cmd, len, .. }) => {
                parse_failures += 1;
                eprintln!("WARN: ParseFailed cmd={cmd:?} len={len}");
            }
            Ok(_) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    println!(
        "streaming stats: trades={trades_packets} books={orderbook_applied} parse_failed={parse_failures}"
    );
    assert!(
        trades_packets > 0,
        "FAIL: 0 Trades packets за {STREAM_DURATION_SECS}с"
    );
    println!("OK: trades_stream ({trades_packets} packets)");
    assert!(
        orderbook_applied > 0,
        "FAIL: 0 OrderBook Apply events за {STREAM_DURATION_SECS}с"
    );
    println!("OK: order_book_stream ({orderbook_applied} apply events)");
    assert!(
        parse_failures == 0,
        "FAIL: {parse_failures} ParseFailed events"
    );
    println!("OK: no_parse_failures");

    client.stop().expect("FAIL: MoonClient::stop failed");
    println!("OK: stop");
    println!("\nRUNTIME_SMOKE_PASS: MoonClient happy path passed against {ip}:{port}");
}
