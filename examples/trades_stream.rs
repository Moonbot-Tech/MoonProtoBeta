//! Subscribe to all-trades through `MoonClient` and print stream signals.
//!
//! Run:
//!   cargo run --example trades_stream --release -- "<key_base64>" [host:port] [market|all] [watch_seconds]

use std::env;
use std::time::{Duration, Instant};

use moonproto::state::{
    MarketHandle, MarketHistoryReaders, SeqRingCursor, TradeHistoryRow, TradesEvent,
};
use moonproto::{Event, TradesStreamMode};

mod common;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: trades_stream <key_base64> [host:port] [market|all] [watch_seconds]");
        std::process::exit(1);
    }

    let market_filter = match args.get(3).map(String::as_str) {
        Some("all") | None => None,
        Some(name) => Some(name.to_string()),
    };
    let watch_secs: u64 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(30);

    let client = match common::connect(&args[1], args.get(2), common::init_config()) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("[connect/init] failed: {err}");
            std::process::exit(2);
        }
    };

    if let Some(market) = market_filter.as_ref() {
        client
            .streams()
            .subscribe_trades_for(TradesStreamMode::TradesOnly, [market.as_str()])
            .expect("runtime stopped");
        println!("[subscribe] all-trades, retained market={market}");
    } else {
        client
            .streams()
            .subscribe_all_trades(TradesStreamMode::TradesOnly)
            .expect("runtime stopped");
        println!("[subscribe] all-trades, retained all markets");
    }

    let selected_market: Option<MarketHandle> = market_filter.as_deref().and_then(|name| {
        client
            .snapshot()
            .and_then(|snapshot| snapshot.markets().get(name))
    });
    if market_filter.is_some() && selected_market.is_none() {
        eprintln!(
            "[warn] selected market was not found in the current market snapshot; \
             waiting for stream signals only"
        );
    }

    let mut signals = 0u64;
    let mut trades = 0u64;
    let mut printed = 0u64;
    let mut readers: Option<MarketHistoryReaders> = None;
    let mut cursor: Option<SeqRingCursor> = None;
    let mut rows: Vec<TradeHistoryRow> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(watch_secs);

    while Instant::now() < deadline {
        for event in client.drain_events() {
            match event {
                Event::Trade(TradesEvent::Applied { .. }) => {
                    signals += 1;
                    if let Some(market) = selected_market.as_ref() {
                        let Some(snapshot) = client.snapshot() else {
                            continue;
                        };
                        if readers.is_none() {
                            readers = snapshot.market_history_readers_for(market);
                        }
                        let Some(reader) = readers.as_ref().and_then(|r| r.futures_trades.clone())
                        else {
                            continue;
                        };
                        let cursor = cursor.get_or_insert_with(|| reader.cursor_from_oldest());
                        rows.clear();
                        let meta = reader.copy_new_since(cursor, 4096, &mut rows);
                        if meta.clipped {
                            println!("[retained-gap] local cursor fell behind retained history");
                        }
                        trades += rows.len() as u64;
                        let remaining_to_print = 25u64.saturating_sub(printed) as usize;
                        for row in rows.iter().take(remaining_to_print) {
                            let side = if row.is_buy() { "buy" } else { "sell" };
                            println!(
                                "[retained-trade] {} {side} price={} qty={} time_ms={}",
                                market.name(),
                                row.price,
                                row.quantity(),
                                row.unix_millis()
                            );
                            printed += 1;
                            if printed >= 25 {
                                break;
                            }
                        }
                    } else {
                        trades += 1;
                        if printed < 25 {
                            printed += 1;
                            println!("[trade-signal] retained rows updated");
                        }
                    }
                }
                _ => {}
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    println!("[done] update_signals={signals} visible_updates={trades}");
}
