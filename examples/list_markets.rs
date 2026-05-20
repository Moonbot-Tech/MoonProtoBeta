//! Fetch the market catalog and print a compact summary.
//!
//! Demonstrates `connect_and_init` for the common consumer path: create a
//! client, wait until it is ready, run BaseCheck/AuthCheck/GetMarketsList, then
//! read the resulting state from `EventDispatcher`.
//!
//! Run:
//!   cargo run --example list_markets --release -- "<key_base64>" "host:port" 20

use std::env;
use std::time::Duration;

use moonproto::{
    connect_and_init, import_key, Client, ClientConfig, ConnectConfig, EventDispatcher,
    InitConfig,
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

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: list_markets <key_base64> [host:port] [limit]");
        std::process::exit(1);
    }

    let keys = import_key(&args[1]).expect("invalid key");
    let (server_ip, server_port) = parse_host(args.get(2));
    let limit: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(20);

    let cfg = ClientConfig::new(server_ip, server_port, keys.master_key, keys.mac_key);
    let mut client = Client::new(cfg);
    let mut dispatcher = EventDispatcher::new();

    let init = InitConfig {
        base_check: true,
        auth_check: true,
        fetch_markets: true,
        step_timeout: Some(Duration::from_secs(15)),
        ..Default::default()
    };

    println!("[connect] waiting for ready client and market snapshot...");
    let result = match connect_and_init(
        &mut client,
        &mut dispatcher,
        ConnectConfig::new(init).with_connect_timeout(Duration::from_secs(15)),
    ) {
        Ok(result) => result,
        Err(err) => {
            eprintln!("[connect] failed: {err}");
            std::process::exit(2);
        }
    };

    for err in &result.errors {
        eprintln!("[init] non-critical error: {err}");
    }

    if let Some(name) = client.server_info().server_name.as_deref() {
        println!("[server] {name}");
    }

    let markets = dispatcher.markets();
    println!(
        "[markets] count={} corr={} payload={} bytes",
        markets.market_count(),
        markets.corr_count(),
        result.markets_response_bytes
    );

    if markets.market_count() == 0 {
        eprintln!("[markets] empty market list");
        std::process::exit(3);
    }

    for market in markets.markets.iter().take(limit) {
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
    }

    client.disconnect();
}
