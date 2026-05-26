//! Multi-client smoke test through the normal `MoonClient` API.
//!
//! Run:
//!   cargo run --example multi_client_test --release -- "<key_base64>" [host:port]

use std::env;
use std::thread;
use std::time::{Duration, Instant};

use moonproto::Event;

mod common;

#[derive(Debug, Default)]
struct ClientStats {
    label: &'static str,
    connected: bool,
    events: u64,
    trade_events: u64,
    orderbook_events: u64,
    markets: usize,
    orders: usize,
    error: Option<String>,
}

fn run_client(label: &'static str, key: String, endpoint: Option<String>) -> ClientStats {
    let mut stats = ClientStats {
        label,
        ..Default::default()
    };
    let endpoint_ref = endpoint.as_ref();
    let mut init = common::init_config();
    init.subscribe_trades = Some(false);

    let client = match common::connect(&key, endpoint_ref, init) {
        Ok(client) => client,
        Err(err) => {
            stats.error = Some(err.to_string());
            return stats;
        }
    };
    stats.connected = true;

    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        match client.recv_event_timeout(Duration::from_millis(500)) {
            Ok(Event::Trade(_)) => {
                stats.events += 1;
                stats.trade_events += 1;
            }
            Ok(Event::OrderBook(_)) => {
                stats.events += 1;
                stats.orderbook_events += 1;
            }
            Ok(_) => stats.events += 1,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    if let Some(snapshot) = client.snapshot() {
        stats.markets = snapshot.markets().market_count();
        stats.orders = snapshot.orders().len();
    }
    stats
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: multi_client_test <key_base64> [host:port]");
        std::process::exit(1);
    }

    let key_a = args[1].clone();
    let key_b = args[1].clone();
    let endpoint_a = args.get(2).cloned();
    let endpoint_b = args.get(2).cloned();

    println!("[main] spawning two MoonClient runtimes");
    let a = thread::spawn(move || run_client("A", key_a, endpoint_a));
    thread::sleep(Duration::from_millis(200));
    let b = thread::spawn(move || run_client("B", key_b, endpoint_b));

    let stats_a = a.join().unwrap_or_else(|_| ClientStats {
        label: "A",
        error: Some("thread panicked".to_string()),
        ..Default::default()
    });
    let stats_b = b.join().unwrap_or_else(|_| ClientStats {
        label: "B",
        error: Some("thread panicked".to_string()),
        ..Default::default()
    });

    println!("\n========== MULTI-CLIENT REPORT ==========");
    for stats in [&stats_a, &stats_b] {
        println!(
            "[{}] connected={} events={} trades={} books={} markets={} orders={} error={:?}",
            stats.label,
            stats.connected,
            stats.events,
            stats.trade_events,
            stats.orderbook_events,
            stats.markets,
            stats.orders,
            stats.error
        );
    }

    if stats_a.connected && stats_b.connected && stats_a.markets > 0 && stats_b.markets > 0 {
        println!("PASS: two independent MoonClient runtimes connected and kept state.");
    } else {
        println!("FAIL: one of the runtimes did not reach usable active state.");
        std::process::exit(1);
    }
}
