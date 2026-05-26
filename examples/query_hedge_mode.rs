//! Query account hedge mode through the high-level Active Lib runtime.
//!
//! Run:
//!   cargo run --example query_hedge_mode --release -- "<key_base64>" [host:port]

use std::env;
use std::time::Duration;

mod common;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: query_hedge_mode <key_base64> [host:port]");
        std::process::exit(1);
    }

    let client = match common::connect(&args[1], args.get(2), common::init_config()) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("[connect/init] failed: {err}");
            std::process::exit(2);
        }
    };

    match client.request_hedge_mode(Duration::from_secs(15)) {
        Ok(value) => println!("[response] hedge_mode={value}"),
        Err(err) => {
            eprintln!("[request] failed: {err}");
            std::process::exit(3);
        }
    }
}
