//! Subscribe to one orderbook stream through `MoonClient` and print updates.
//!
//! Run:
//!   cargo run --example order_book_stream --release -- "<key_base64>" [host:port] [market] [watch_seconds]

use std::env;
use std::time::{Duration, Instant};

use moonproto::state::OrderBookEvent;
use moonproto::Event;

mod common;

fn book_kind_name(kind: u8) -> &'static str {
    match kind {
        0 => "futures",
        1 => "spot",
        _ => "unknown",
    }
}

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
            match event {
                Event::OrderBook(event) => match event {
                    OrderBookEvent::Apply {
                        market_name,
                        kind,
                        is_full,
                        seq,
                        top,
                        buys,
                        sells,
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
                        "[book] market={} kind={} full={} seq={} bids={} asks={} top_bid={} top_ask={}",
                        name,
                        book_kind_name(kind.as_u8()),
                        is_full,
                        seq,
                        buys.len(),
                        sells.len(),
                        bid,
                        ask
                    );
                    }
                    OrderBookEvent::Ignored {
                        market_index,
                        kind,
                        seq,
                        reason,
                    } => {
                        println!(
                            "[book] ignored idx={} kind={} seq={} reason={reason:?}",
                            market_index,
                            book_kind_name(kind.as_u8()),
                            seq
                        );
                    }
                    OrderBookEvent::RequestFullNeeded { .. } => {}
                },
                _ => {}
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    println!("[done] applies={applies} fulls={fulls}");
}
