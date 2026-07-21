//! Request current orders and optionally cancel one through `MoonClient`.
//!
//! By default this example is a dry run. Pass `--send` to actually queue the
//! cancel intent.
//!
//! Run:
//!   cargo run --example cancel_open_order --release -- "<key_base64>" [host:port] [order_uid] [--send]

use std::env;
use std::time::{Duration, Instant};

use moonproto::state::OrderEvent;
use moonproto::Event;

mod common;

fn parse_uid(value: &str) -> Option<u64> {
    value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .map_or_else(
            || value.parse().ok(),
            |hex| u64::from_str_radix(hex, 16).ok(),
        )
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: cancel_open_order <key_base64> [host:port] [order_uid] [--send]");
        std::process::exit(1);
    }

    let send = args.iter().any(|arg| arg == "--send");
    let host_arg = args.iter().skip(2).find(|arg| arg.contains(':'));
    let target_uid = args
        .iter()
        .skip(2)
        .filter(|arg| arg.as_str() != "--send")
        .find_map(|arg| parse_uid(arg));

    let client = match common::connect(&args[1], host_arg, common::init_config()) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("[connect/init] failed: {err}");
            std::process::exit(2);
        }
    };

    if let Err(err) = client.orders().request_snapshot() {
        eprintln!("[orders] snapshot request failed: {err}");
        std::process::exit(3);
    }
    let ready = common::wait_until(Duration::from_secs(15), || {
        client
            .drain_events()
            .into_iter()
            .any(|event| matches!(event, Event::Order(OrderEvent::Snapshot)))
    });
    if !ready {
        eprintln!("[orders] timed out waiting for OrderEvent::Snapshot");
        std::process::exit(3);
    }

    let mut orders: Vec<_> = client
        .snapshot()
        .map(|snapshot| snapshot.orders().iter().cloned().collect())
        .unwrap_or_default();
    orders.retain(|order| !order.status.is_terminal());
    orders.sort_by_key(|order| order.uid);

    let Some(order) = target_uid
        .and_then(|uid| orders.iter().find(|order| order.uid == uid))
        .or_else(|| orders.first())
        .cloned()
    else {
        println!("[orders] no active orders to cancel");
        return;
    };

    println!(
        "[target] uid={} market={} status={:?} side={}",
        order.uid,
        order.market_name,
        order.status,
        if order.is_short { "short" } else { "long" },
    );

    if !send {
        println!("[dry-run] cancel was not sent; pass --send to queue cancel intent");
        return;
    }

    match client.orders().cancel(&order) {
        Ok(()) => println!("[send] cancel intent queued; listening briefly for order updates..."),
        Err(err) => {
            eprintln!("[send] failed: {err}");
            std::process::exit(4);
        }
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        for event in client.drain_events() {
            match event {
                Event::Order(OrderEvent::Updated(order)) => {
                    println!("[order] updated uid={}", order.uid)
                }
                Event::Order(OrderEvent::Removed(order)) => {
                    println!("[order] removed uid={}", order.uid)
                }
                _ => {}
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}
