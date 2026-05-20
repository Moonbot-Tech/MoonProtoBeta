//! Query the account hedge mode through the public Engine API.
//!
//! Demonstrates `run_init_sequence` + `Client::request_hedge_mode`. The consumer
//! does not need to know that the server payload is one Delphi `Boolean`.
//!
//! Run:
//!   cargo run --example query_hedge_mode --release -- "<key_base64>" "host:port"

use std::env;
use std::time::Duration;

use moonproto::{
    import_key, run_init_sequence, Client, ClientConfig, EventDispatcher, InitConfig,
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
        eprintln!("Usage: query_hedge_mode <key_base64> [host:port]");
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
        base_check: true,
        auth_check: true,
        step_timeout: Some(Duration::from_secs(10)),
        ..Default::default()
    };
    if let Err(err) = run_init_sequence(&mut client, &mut dispatcher, init) {
        eprintln!("[init] failed: {err}");
        std::process::exit(3);
    }

    if let Some(name) = client.server_info().server_name.as_deref() {
        println!("[server] {name}");
    }

    println!("[request] query hedge mode");
    let hedge_mode = match client.request_hedge_mode(&mut dispatcher, Duration::from_secs(15)) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("[request] failed: {err}");
            std::process::exit(4);
        }
    };
    println!("[response] hedge_mode={hedge_mode}");

    client.disconnect();
}
