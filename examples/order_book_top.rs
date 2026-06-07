//! Subscribe to one orderbook and print current best bid/ask from snapshots.
//!
//! Run:
//!   cargo run --example order_book_top --release -- "<key_base64>" [host:port] [market] [watch_seconds]

use std::env;
use std::time::{Duration, Instant};

use moonproto::state::OrderBookEvent;
use moonproto::Event;

mod common;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: order_book_top <key_base64> [host:port] [market] [watch_seconds]");
        std::process::exit(1);
    }

    let market = args
        .get(3)
        .cloned()
        .unwrap_or_else(|| "BTCUSDT".to_string());
    let watch_secs: u64 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(30);
    let mut init = common::init_config();
    init.subscribe_orderbooks.push(market.clone());

    let client = match common::connect(&args[1], args.get(2), init) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("[connect/init] failed: {err}");
            std::process::exit(2);
        }
    };

    println!("[subscribe] orderbook market={market}");
    let deadline = Instant::now() + Duration::from_secs(watch_secs);
    let mut updates = 0u64;

    while Instant::now() < deadline {
        for event in client.drain_events() {
            if let Event::OrderBook(OrderBookEvent::Apply {
                market_name,
                kind,
                top,
                ..
            }) = event
            {
                let Some(name) = market_name.as_deref() else {
                    continue;
                };
                if name != market {
                    continue;
                }
                updates += 1;
                let bid = top
                    .bid
                    .map(|level| format!("{} @ {}", level.quantity, level.rate))
                    .unwrap_or_else(|| "none".to_string());
                let ask = top
                    .ask
                    .map(|level| format!("{} @ {}", level.quantity, level.rate))
                    .unwrap_or_else(|| "none".to_string());
                println!("[top] {name} {} bid={} ask={}", kind.as_str(), bid, ask);
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    println!("[done] top-updates={updates}");
}
