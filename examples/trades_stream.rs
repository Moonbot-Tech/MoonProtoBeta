//! Subscribe to all-trades through `MoonClient` and print stream signals.
//!
//! Run:
//!   cargo run --example trades_stream --release -- "<key_base64>" [host:port] [market|all] [watch_seconds]

use std::env;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use moonproto::state::TradesEvent;
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
            .subscribe_trades_for(TradesStreamMode::TradesOnly, [market.as_str()])
            .expect("runtime stopped");
        println!("[subscribe] all-trades, retained market={market}");
    } else {
        client
            .subscribe_all_trades(TradesStreamMode::TradesOnly)
            .expect("runtime stopped");
        println!("[subscribe] all-trades, retained all markets");
    }

    let mut packets = 0u64;
    let mut trades = 0u64;
    let mut gaps = 0u64;
    let mut printed = 0u64;
    let deadline = Instant::now() + Duration::from_secs(watch_secs);

    while Instant::now() < deadline {
        match client.recv_event_timeout(Duration::from_millis(500)) {
            Ok(Event::Trade(TradesEvent::Applied { packet_num, .. })) => {
                packets += 1;
                if let Some(name) = market_filter.as_deref() {
                    let Some(snapshot) = client.snapshot() else {
                        continue;
                    };
                    let Some(tail) = snapshot.markets().trade_state(name) else {
                        continue;
                    };
                    if tail.last_trade_price <= 0.0 {
                        continue;
                    }
                    trades += 1;
                    if printed < 25 {
                        printed += 1;
                        let side = if tail.last_trade_was_sell {
                            "sell"
                        } else {
                            "buy"
                        };
                        println!(
                            "[trade-tail] pkt={} {name} {} price={}",
                            packet_num, side, tail.last_trade_price
                        );
                    }
                } else {
                    trades += 1;
                    if printed < 25 {
                        printed += 1;
                        println!("[trade-signal] pkt={packet_num}");
                    }
                }
            }
            Ok(Event::Trade(TradesEvent::GapDetected { start, end })) => {
                gaps += 1;
                println!("[trade] gap detected {start}..{end}");
            }
            Ok(Event::Trade(TradesEvent::GapFilled { packet_num, .. })) => {
                println!("[trade] gap filled packet={packet_num}");
            }
            Ok(_) => {}
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    println!("[done] packets={packets} trades={trades} gaps={gaps}");
}
