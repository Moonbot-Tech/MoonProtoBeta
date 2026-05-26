//! Fetch one asset balance through the high-level Active Lib runtime.
//!
//! Run:
//!   cargo run --example get_balance --release -- "<key_base64>" [host:port] [asset]

use std::env;
use std::time::Duration;

mod common;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: get_balance <key_base64> [host:port] [asset]");
        std::process::exit(1);
    }

    let asset = args.get(3).map(String::as_str).unwrap_or("USDT");
    let client = match common::connect(&args[1], args.get(2), common::init_config()) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("[connect/init] failed: {err}");
            std::process::exit(2);
        }
    };

    println!("[request] balance asset={asset}");
    match client.request_balance(asset, Duration::from_secs(15)) {
        Ok(quantity) => println!("[response] {asset} balance={quantity}"),
        Err(err) => {
            eprintln!("[request] failed: {err}");
            std::process::exit(3);
        }
    }
}
