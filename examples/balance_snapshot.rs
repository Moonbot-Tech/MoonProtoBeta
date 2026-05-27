//! Request a fresh full balance snapshot through `MoonClient`.
//!
//! Run:
//!   cargo run --example balance_snapshot --release -- "<key_base64>" [host:port] [timeout_seconds]

use std::env;
use std::time::Duration;

use moonproto::commands::balance::BalanceItem;

mod common;

fn has_visible_balance(item: &BalanceItem) -> bool {
    item.initial_balance != 0.0
        || item.locked_balance != 0.0
        || item.pos_size != 0.0
        || item.long_pos_size != 0.0
        || item.short_pos_size != 0.0
        || item.asset_balance != 0.0
        || item.asset_balance_full != 0.0
        || item.total_profit_b != 0.0
        || item.total_profit_l != 0.0
        || item.total_profit_s != 0.0
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: balance_snapshot <key_base64> [host:port] [timeout_seconds]");
        std::process::exit(1);
    }

    let timeout_secs = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(15);
    let client = match common::connect(&args[1], args.get(2), common::init_config()) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("[connect/init] failed: {err}");
            std::process::exit(2);
        }
    };

    let balances = match client.blocking_request_balance_snapshot(Duration::from_secs(timeout_secs))
    {
        Ok(balances) => balances,
        Err(err) => {
            eprintln!("[request] failed: {err}");
            std::process::exit(3);
        }
    };

    let g = &balances.global;
    println!(
        "[snapshot] epoch={} rows={} btc_total={} btc_locked={} btc_full={} special_coin={}",
        balances.last_epoch,
        balances.len(),
        g.btc_balance_total,
        g.btc_balance_locked,
        g.btc_balance_full,
        g.special_coin_balance
    );

    let mut visible: Vec<_> = balances
        .iter()
        .filter(|(_, item)| has_visible_balance(item))
        .collect();
    visible.sort_by(|a, b| a.0.cmp(b.0));

    for (market, item) in visible.into_iter().take(10) {
        println!(
            "[market] {market} init={} locked={} pos={} long={} short={} asset_full={} lev={}",
            item.initial_balance,
            item.locked_balance,
            item.pos_size,
            item.long_pos_size,
            item.short_pos_size,
            item.asset_balance_full,
            item.leverage_x
        );
    }
}
