//! Request a fresh full balance snapshot through the public `MoonClient`
//! event/snapshot path.
//!
//! Run:
//!   cargo run --example balance_snapshot --release -- "<key_base64>" [host:port] [timeout_seconds]

use std::env;
use std::time::Duration;

use moonproto::state::{BalanceEvent, MarketBalancePosition};
use moonproto::Event;

mod common;

fn has_visible_balance(pos: MarketBalancePosition) -> bool {
    pos.initial_balance != 0.0
        || pos.locked_balance != 0.0
        || pos.pos_size != 0.0
        || pos.long_pos_size != 0.0
        || pos.short_pos_size != 0.0
        || pos.asset_balance != 0.0
        || pos.asset_balance_full != 0.0
        || pos.total_profit_b != 0.0
        || pos.total_profit_l != 0.0
        || pos.total_profit_s != 0.0
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: balance_snapshot <key_base64> [host:port] [timeout_seconds]");
        std::process::exit(1);
    }

    let timeout = Duration::from_secs(args.get(3).and_then(|s| s.parse().ok()).unwrap_or(15));
    let client = match common::connect(&args[1], args.get(2), common::init_config()) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("[connect/init] failed: {err}");
            std::process::exit(2);
        }
    };

    if let Err(err) = client.balances().refresh() {
        eprintln!("[request] failed: {err}");
        std::process::exit(3);
    }
    let ready = common::wait_until(timeout, || {
        client
            .drain_events()
            .into_iter()
            .any(|event| matches!(event, Event::Balance(BalanceEvent::SnapshotApplied { .. })))
    });
    if !ready {
        eprintln!("[request] timed out waiting for BalanceEvent::SnapshotApplied");
        std::process::exit(3);
    }

    let snapshot = client
        .snapshot()
        .expect("balance event must publish state snapshot");
    let balances = snapshot.balances();
    let g = balances.global();
    println!(
        "[snapshot] markets={} total_pnl={} btc_total={} btc_locked={} btc_full={} special_coin={}",
        snapshot.markets().market_count(),
        g.total_pnl,
        g.btc_balance_total,
        g.btc_balance_locked,
        g.btc_balance_full,
        g.special_coin_balance
    );

    let mut visible = Vec::new();
    for handle in snapshot.markets().iter() {
        let name = handle.with(|market| market.symbol().to_owned());
        let pos = handle.balance_position();
        if has_visible_balance(pos) {
            visible.push((name, pos));
        }
    }
    visible.sort_by(|a, b| a.0.cmp(&b.0));

    for (market, pos) in visible.into_iter().take(10) {
        println!(
            "[market] {market} init={} locked={} pos={} entry={} liq={} long={} short={} asset_full={} lev={} pnl={}",
            pos.initial_balance,
            pos.locked_balance,
            pos.pos_size,
            pos.pos_price,
            pos.liq_price,
            pos.long_pos_size,
            pos.short_pos_size,
            pos.asset_balance_full,
            pos.leverage_x,
            pos.total_profit()
        );
    }
}
