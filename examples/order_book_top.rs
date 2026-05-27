//! Subscribe to one orderbook and print current best bid/ask from snapshots.
//!
//! Run:
//!   cargo run --example order_book_top --release -- "<key_base64>" [host:port] [market] [watch_seconds]

use std::env;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use moonproto::state::{OrderBookEvent, OrderBookKind};
use moonproto::Event;

mod common;

fn kind_name(kind: OrderBookKind) -> &'static str {
    match kind {
        OrderBookKind::Futures => "futures",
        OrderBookKind::Spot => "spot",
    }
}

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
        match client.recv_event_timeout(Duration::from_millis(500)) {
            Ok(Event::OrderBook(OrderBookEvent::Apply {
                market_name,
                kind,
                seq,
                top,
                ..
            })) => {
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
                println!(
                    "[top] {name} {} seq={} bid={} ask={}",
                    kind_name(kind),
                    seq,
                    bid,
                    ask
                );
            }
            Ok(_) => {}
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    println!("[done] top-updates={updates}");
}
