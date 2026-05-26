//! Request current orders and optionally cancel one through `MoonClient`.
//!
//! By default this example is a dry run. Pass `--send` to actually queue the
//! cancel intent.
//!
//! Run:
//!   cargo run --example cancel_open_order --release -- "<key_base64>" [host:port] [order_uid] [--send]

use std::env;
use std::sync::mpsc;
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

    let mut orders = match client.request_order_snapshot(Duration::from_secs(15)) {
        Ok(orders) => orders,
        Err(err) => {
            eprintln!("[orders] snapshot failed: {err}");
            std::process::exit(3);
        }
    };
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
        "[target] uid={} market={} status={:?} side={} from_cache={}",
        order.uid,
        order.market_name,
        order.status,
        if order.is_short { "short" } else { "long" },
        order.from_cache,
    );

    if !send {
        println!("[dry-run] cancel was not sent; pass --send to queue cancel intent");
        return;
    }

    match client.orders().cancel(order.uid) {
        Ok(true) => println!("[send] cancel queued; listening briefly for order updates..."),
        Ok(false) => {
            println!("[send] cancel was not queued; live order state is no longer cancellable");
            return;
        }
        Err(err) => {
            eprintln!("[send] failed: {err}");
            std::process::exit(4);
        }
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match client.recv_event_timeout(Duration::from_millis(500)) {
            Ok(Event::Order(OrderEvent::Updated(uid))) => println!("[order] updated uid={uid}"),
            Ok(Event::Order(OrderEvent::Removed(uid))) => println!("[order] removed uid={uid}"),
            Ok(Event::Order(OrderEvent::Ignored { uid, reason })) => {
                println!("[order] ignored uid={uid} reason={reason:?}");
            }
            Ok(_) => {}
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
}
