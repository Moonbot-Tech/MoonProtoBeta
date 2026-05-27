//! Fetch one batch of historical candles through `MoonClient`.
//!
//! Run:
//!   cargo run --example history_bars --release -- "<key_base64>" [host:port] [market] [1m|5m|30m|1h|4h|1d]

use std::env;
use std::time::Duration;

use moonproto::commands::candles::{DeepHistoryKind, DeepPrice};

mod common;

fn parse_kind(value: Option<&String>) -> DeepHistoryKind {
    let Some(value) = value else {
        return DeepHistoryKind::Hour1;
    };
    match value.to_ascii_lowercase().as_str() {
        "1m" | "min1" => DeepHistoryKind::Min1,
        "5m" | "min5" => DeepHistoryKind::Min5,
        "30m" | "min30" => DeepHistoryKind::Min30,
        "1h" | "hour1" => DeepHistoryKind::Hour1,
        "4h" | "hour4" => DeepHistoryKind::Hour4,
        "1d" | "day1" => DeepHistoryKind::Day1,
        _ => DeepHistoryKind::Hour1,
    }
}

fn print_candle(label: &str, candle: &DeepPrice) {
    println!(
        "{label}: unix={} open={} high={} low={} close={} vol={}",
        candle.time_delphi().unix_seconds().unwrap_or(0.0).round() as i64,
        candle.open(),
        candle.high(),
        candle.low(),
        candle.close(),
        candle.vol
    );
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: history_bars <key_base64> [host:port] [market] [1m|5m|30m|1h|4h|1d]");
        std::process::exit(1);
    }

    let market = args.get(3).map(String::as_str).unwrap_or("BTCUSDT");
    let kind = parse_kind(args.get(4));
    let client = match common::connect(&args[1], args.get(2), common::init_config()) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("[connect/init] failed: {err}");
            std::process::exit(2);
        }
    };

    println!("[request] candles market={market} kind={kind:?}");
    let candles =
        match client.blocking_request_coin_card_candles(market, kind, Duration::from_secs(15)) {
            Ok(candles) => candles,
            Err(err) => {
                eprintln!("[request] failed: {err}");
                std::process::exit(3);
            }
        };
    println!("[response] {} candles", candles.len());
    if let Some(first) = candles.first() {
        print_candle("first", first);
    }
    if let Some(last) = candles.last() {
        print_candle("last", last);
    }
}
