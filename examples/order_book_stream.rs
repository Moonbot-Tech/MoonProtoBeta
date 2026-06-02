//! Subscribe to one orderbook stream through `MoonClient` and print updates.
//!
//! Run:
//!   cargo run --example order_book_stream --release -- "<key_base64>" [host:port] [market] [watch_seconds]

use std::env;
use std::time::{Duration, Instant};

use moonproto::state::OrderBookEvent;
use moonproto::Event;

mod common;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: order_book_stream <key_base64> [host:port] [market] [watch_seconds]");
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
    let mut applies = 0u64;
    let mut fulls = 0u64;
    let deadline = Instant::now() + Duration::from_secs(watch_secs);

    while Instant::now() < deadline {
        for event in client.drain_events() {
            if let Event::OrderBook(event) = event {
                match event {
                    OrderBookEvent::Apply {
                        market_name,
                        kind,
                        is_full,
                        top,
                        ..
                    } => {
                        applies += 1;
                        if is_full {
                            fulls += 1;
                        }
                        let name = market_name.as_deref().unwrap_or("<unknown>");
                        let bid = top
                            .bid
                            .map(|level| format!("{} @ {}", level.quantity, level.rate))
                            .unwrap_or_else(|| "none".to_string());
                        let ask = top
                            .ask
                            .map(|level| format!("{} @ {}", level.quantity, level.rate))
                            .unwrap_or_else(|| "none".to_string());
                        println!(
                            "[book] market={} kind={} full={} top_bid={} top_ask={}",
                            name,
                            kind.as_str(),
                            is_full,
                            bid,
                            ask
                        );
                    }
                }
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    println!("[done] applies={applies} fulls={fulls}");
}
