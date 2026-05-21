//! Request a fresh full balance snapshot through the Balance channel.
//!
//! This is the high-level consumer path for the same post-init operation the
//! Delphi terminal performs with `TRequestBalanceRefresh`: the application does
//! not manually wait for `Event::Balance`; it gets an applied `BalancesState`.
//!
//! Run:
//!   cargo run --example balance_snapshot --release -- "<key_base64>" "host:port" 15

use std::env;
use std::time::Duration;

use moonproto::commands::balance::BalanceItem;
use moonproto::{
    connect_and_init, import_key, Client, ClientConfig, ConnectConfig, EventDispatcher, InitConfig,
};

fn parse_host(value: Option<&String>) -> (String, u16) {
    let Some(value) = value else {
        return ("127.0.0.1".to_string(), 3000);
    };
    let Some((host, port)) = value.split_once(':') else {
        return (value.clone(), 3000);
    };
    (host.to_string(), port.parse().unwrap_or(3000))
}

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

    let keys = import_key(&args[1]).expect("invalid key");
    let (server_ip, server_port) = parse_host(args.get(2));
    let timeout_secs = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(15);
    let timeout = Duration::from_secs(timeout_secs);

    let cfg = ClientConfig::new(server_ip, server_port, keys.master_key, keys.mac_key);
    let mut client = Client::new(cfg);
    let mut dispatcher = EventDispatcher::new();

    println!("[connect] waiting for authorization and init...");
    let init = InitConfig {
        base_check: true,
        auth_check: true,
        fetch_markets: true,
        step_timeout: None,
        ..Default::default()
    };
    let init_result = match connect_and_init(
        &mut client,
        &mut dispatcher,
        ConnectConfig::new(init).with_connect_timeout(Duration::from_secs(15)),
    ) {
        Ok(result) => result,
        Err(err) => {
            eprintln!("[init] failed: {err}");
            std::process::exit(2);
        }
    };
    for err in &init_result.errors {
        eprintln!("[init] non-critical error: {err}");
    }

    if let Some(name) = client.server_info().server_name.as_deref() {
        println!("[server] {name}");
    }

    println!("[request] balance snapshot timeout={timeout_secs}s");
    let balances = match client.request_balance_snapshot(&mut dispatcher, timeout) {
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

    client.disconnect();
}
