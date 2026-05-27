//! Connect with `MoonClient` and print the market catalog from the latest snapshot.
//!
//! Run:
//!   cargo run --example list_markets --release -- "<key_base64>" [host:port] [limit]

use std::env;

mod common;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: list_markets <key_base64> [host:port] [limit]");
        std::process::exit(1);
    }

    let limit: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(20);
    let client = match common::connect(&args[1], args.get(2), common::init_config()) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("[connect/init] failed: {err}");
            std::process::exit(2);
        }
    };

    let Some(snapshot) = client.snapshot() else {
        eprintln!("[markets] no snapshot published");
        std::process::exit(3);
    };
    let markets = snapshot.markets();
    println!(
        "[markets] count={} corr={}",
        markets.market_count(),
        markets.corr_count()
    );

    if markets.market_count() == 0 {
        eprintln!("[markets] empty market list");
        std::process::exit(4);
    }

    for market in markets.iter().take(limit) {
        market.with(|market| {
            println!(
                "[market] {} base={} status={} trading={} max_lev={} tick={} step={}",
                market.bn_market_name,
                market.base_currency,
                market.bn_status,
                market.status_trading,
                market.max_leverage,
                market.bn_tick_size,
                market.bn_step_size
            );
        });
    }
}
