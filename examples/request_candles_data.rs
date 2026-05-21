//! Request the full chunked candles stream and print the first result.
//!
//! Run:
//!   cargo run --example request_candles_data --release -- "<key_base64>" "host:port" 30 0

use std::env;
use std::time::Duration;

use moonproto::{
    connect_and_init, import_key, Client, ClientConfig, ConnectConfig, EventDispatcher, InitConfig,
    RefreshConfig,
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
        eprintln!(
            "Usage: request_candles_data <key_base64> [host:port] [timeout_secs] [err_emu_pct]"
        );
        std::process::exit(1);
    }

    let keys = import_key(&args[1]).expect("invalid key");
    let (server_ip, server_port) = parse_host(args.get(2));
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

    let cfg = ClientConfig::new(server_ip, server_port, keys.master_key, keys.mac_key)
        .with_refresh(RefreshConfig {
            update_markets_every: None,
            check_tags_every: None,
        });
    let mut client = Client::new(cfg);
    let mut dispatcher = EventDispatcher::new();

    let init = InitConfig {
        step_timeout: None,
        ..Default::default()
    };

    if let Err(err) = connect_and_init(
        &mut client,
        &mut dispatcher,
        ConnectConfig::new(init).with_connect_timeout(Duration::from_secs(30)),
    ) {
        eprintln!("connect/init failed: {err}");
        std::process::exit(2);
    }
    if err_emu_pct > 0 {
        moonproto::client::set_err_emu(err_emu_pct);
        eprintln!("client-side err_emu={err_emu_pct}% after init");
    }

    match client.request_candles_data(&mut dispatcher, timeout) {
        Ok(merged) => {
            let candles: usize = merged.markets.iter().map(|m| m.candles_5m.len()).sum();
            println!(
                "ok uid={} zipped={} markets={} candles={}",
                merged.uid,
                merged.zipped_data.len(),
                merged.markets.len(),
                candles
            );
            client.disconnect();
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            eprintln!("disconnected");
            client.disconnect();
            std::process::exit(3);
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            eprintln!("timeout");
            client.disconnect();
            std::process::exit(4);
        }
    }
}
