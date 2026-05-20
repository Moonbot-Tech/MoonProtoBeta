//! Request the current order snapshot.
//!
//! Demonstrates `Client::request_order_snapshot`, the high-level helper for
//! `TAllStatusesReq`. The consumer does not need to provide a protocol UID or
//! manually handle `CleanupMissingWorkers` follow-up requests.
//!
//! Run:
//!   cargo run --example order_snapshot --release -- "<key_base64>" "host:port"

use std::env;
use std::time::Duration;

use moonproto::{import_key, Client, ClientConfig, EventDispatcher};

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
        eprintln!("Usage: order_snapshot <key_base64> [host:port]");
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

    println!("[request] order snapshot");
    let mut orders = match client.request_order_snapshot(
        &mut dispatcher,
        Duration::from_secs(15),
    ) {
        Ok(orders) => orders,
        Err(err) => {
            eprintln!("[request] timeout/disconnected: {err:?}");
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

    client.disconnect();
}
