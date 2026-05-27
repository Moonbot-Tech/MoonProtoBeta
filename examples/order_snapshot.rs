//! Request and print the current order snapshot through `MoonClient`.
//!
//! Run:
//!   cargo run --example order_snapshot --release -- "<key_base64>" [host:port]

use std::env;
use std::time::Duration;

mod common;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: order_snapshot <key_base64> [host:port]");
        std::process::exit(1);
    }

    let client = match common::connect(&args[1], args.get(2), common::init_config()) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("[connect/init] failed: {err}");
            std::process::exit(2);
        }
    };

    let mut orders = match client.blocking_request_order_snapshot(Duration::from_secs(15)) {
        Ok(orders) => orders,
        Err(err) => {
            eprintln!("[request] failed: {err}");
            std::process::exit(3);
        }
    };

    orders.sort_by_key(|order| order.uid);
    println!("[orders] count={}", orders.len());
    for order in orders.iter().take(20) {
        println!(
            "[order] uid={} market={} status={:?} side={} strat={} from_cache={} vstop={}",
            order.uid,
            order.market_name,
            order.status,
            if order.is_short { "short" } else { "long" },
            order.strat_id,
            order.from_cache,
            order.vstop_on,
        );
    }
    if orders.len() > 20 {
        println!("[orders] ... {} more", orders.len() - 20);
    }
}
