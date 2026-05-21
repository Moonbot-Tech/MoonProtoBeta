//! Request the exchange API-key expiration time.
//!
//! Demonstrates `Client::request_api_expiration_time`, the typed helper for
//! `emk_CheckAPIExpirationTime`. The consumer does not need to parse the raw
//! Delphi `TDateTime` double from `EngineResponse::data`.
//!
//! Run:
//!   cargo run --example api_expiration_time --release -- "<key_base64>" "host:port"

use std::env;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use moonproto::{import_key, run_init_sequence, Client, ClientConfig, EventDispatcher, InitConfig};

fn parse_host(value: Option<&String>) -> (String, u16) {
    let Some(value) = value else {
        return ("127.0.0.1".to_string(), 3000);
    };
    let Some((host, port)) = value.split_once(':') else {
        return (value.clone(), 3000);
    };
    (host.to_string(), port.parse().unwrap_or(3000))
}

fn unix_seconds(time: SystemTime) -> Option<u64> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: api_expiration_time <key_base64> [host:port]");
        std::process::exit(1);
    }

    let keys = import_key(&args[1]).expect("invalid key");
    let (server_ip, server_port) = parse_host(args.get(2));

    let cfg = ClientConfig::new(server_ip, server_port, keys.master_key, keys.mac_key);
    let mut client = Client::new(cfg);
    let mut dispatcher = EventDispatcher::new();

    println!("[connect] waiting for authorization...");
    client.run_with_dispatcher(Duration::from_secs(15), &mut dispatcher, Box::new(|_| {}));
    if !client.is_authorized() {
        eprintln!("[connect] authorization timeout");
        std::process::exit(2);
    }

    let init = InitConfig {
        step_timeout: None,
        ..Default::default()
    };
    if let Err(err) = run_init_sequence(&mut client, &mut dispatcher, init) {
        eprintln!("[init] failed: {err}");
        std::process::exit(3);
    }

    if let Some(name) = client.server_info().server_name.as_deref() {
        println!("[server] {name}");
    }

    println!("[request] API-key expiration time");
    let expiration =
        match client.request_api_expiration_time(&mut dispatcher, Duration::from_secs(15)) {
            Ok(value) => value,
            Err(err) => {
                eprintln!("[request] failed: {err}");
                std::process::exit(4);
            }
        };

    if let Some(time) = expiration.system_time() {
        let unix = unix_seconds(time).unwrap_or_default();
        let days = expiration.days_until(SystemTime::now()).unwrap_or_default();
        println!(
            "[expiration] unix_seconds={unix} days_until={days} raw_delphi_time={}",
            expiration.delphi_time()
        );
    } else {
        println!(
            "[expiration] not reported by server raw_delphi_time={}",
            expiration.delphi_time()
        );
    }

    client.disconnect();
}
