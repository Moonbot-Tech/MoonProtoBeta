//! Fetch one batch of historical candles through the public library API.
//!
//! Demonstrates `run_init_sequence` + `api_get_coin_card_candles` +
//! `Client::run_until_response` (the recommended single-thread pattern for
//! waiting on Engine API responses).
//!
//! Run:
//!   cargo run --example history_bars --release -- "<key_base64>" "host:port" BTCUSDT 1h

use std::env;
use std::time::Duration;

use moonproto::client::{
    run_init_sequence, Client, ClientConfig, InitConfig,
};
use moonproto::commands::candles::{
    parse_coin_card_candles_response, DeepHistoryKind, DeepPrice,
};
use moonproto::events::EventDispatcher;
use moonproto::key_import;

fn parse_host(value: Option<&String>) -> (String, u16) {
    let Some(value) = value else {
        return ("127.0.0.1".to_string(), 3000);
    };
    let Some((host, port)) = value.split_once(':') else {
        return (value.clone(), 3000);
    };
    (host.to_string(), port.parse().unwrap_or(3000))
}

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

fn delphi_to_unix_secs(value: f64) -> i64 {
    ((value - 25_569.0) * 86_400.0).round() as i64
}

fn print_candle(label: &str, candle: &DeepPrice) {
    println!(
        "{label}: unix={} open={} high={} low={} close={} vol={}",
        delphi_to_unix_secs(candle.time),
        candle.open_p,
        candle.max_p,
        candle.min_p,
        candle.close_p,
        candle.vol
    );
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: history_bars <key_base64> [host:port] [market] [1m|5m|30m|1h|4h|1d]");
        std::process::exit(1);
    }

    let keys = key_import::import_key(&args[1]).expect("invalid key");
    let (server_ip, server_port) = parse_host(args.get(2));
    let market = args.get(3).map(String::as_str).unwrap_or("BTCUSDT");
    let kind = parse_kind(args.get(4));

    let cfg = ClientConfig::new(server_ip, server_port, keys.master_key, keys.mac_key);
    let mut client = Client::new(cfg);
    let mut dispatcher = EventDispatcher::new();

    println!("[connect] waiting for protocol authorization...");
    client.run_with_dispatcher(Duration::from_secs(15), &mut dispatcher, Box::new(|_| {}));
    if !client.is_authorized() {
        eprintln!("[connect] authorization timeout");
        std::process::exit(2);
    }

    let init = InitConfig {
        base_check: true,
        auth_check: true,
        step_timeout: Some(Duration::from_secs(10)),
        ..Default::default()
    };
    if let Err(err) = run_init_sequence(&mut client, &mut dispatcher, init) {
        eprintln!("[init] failed: {err}");
        std::process::exit(3);
    }

    if let Some(name) = client.server_info().server_name.as_deref() {
        println!("[server] {name}");
    }

    println!("[request] candles market={market} kind={kind:?}");
    let rx = client.api_get_coin_card_candles(market, kind);
    let resp = match client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(15)) {
        Ok(resp) => resp,
        Err(err) => {
            eprintln!("[request] timeout/disconnected: {err:?}");
            std::process::exit(4);
        }
    };

    if !resp.success {
        eprintln!("[response] error {}: {}", resp.error_code, resp.error_msg);
        std::process::exit(5);
    }

    let candles = parse_coin_card_candles_response(&resp.data)
        .expect("server returned malformed candle payload");
    println!("[response] {} candles", candles.len());
    if let Some(first) = candles.first() {
        print_candle("first", first);
    }
    if let Some(last) = candles.last() {
        print_candle("last", last);
    }

    client.disconnect();
}
