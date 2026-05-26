//! Request the full chunked candles stream through `MoonClient`.
//!
//! Run:
//!   cargo run --example request_candles_data --release -- "<key_base64>" [host:port] [timeout_secs] [err_emu_pct]

use std::env;
use std::time::Duration;

use moonproto::RefreshConfig;

mod common;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!(
            "Usage: request_candles_data <key_base64> [host:port] [timeout_secs] [err_emu_pct]"
        );
        std::process::exit(1);
    }

    let timeout = args
        .get(3)
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(30));
    let err_emu_pct = args
        .get(4)
        .and_then(|s| s.parse::<u8>().ok())
        .unwrap_or(0)
        .min(100);

    let client = match common::connect_with_refresh(
        &args[1],
        args.get(2),
        common::init_config(),
        RefreshConfig {
            update_markets_every: None,
            check_tags_every: None,
        },
    ) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("[connect/init] failed: {err}");
            std::process::exit(2);
        }
    };

    if err_emu_pct > 0 {
        moonproto::client::set_err_emu(err_emu_pct);
        eprintln!("client-side err_emu={err_emu_pct}% after init");
    }

    match client.request_candles_data(timeout) {
        Ok(merged) => {
            let candles: usize = merged.markets.iter().map(|m| m.candles_5m.len()).sum();
            println!(
                "ok uid={} zipped={} markets={} candles={}",
                merged.uid,
                merged.zipped_data.len(),
                merged.markets.len(),
                candles
            );
        }
        Err(err) => {
            eprintln!("[request] failed: {err}");
            std::process::exit(3);
        }
    }
}
