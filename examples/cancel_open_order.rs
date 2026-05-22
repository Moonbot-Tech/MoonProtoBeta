//! Request the current order snapshot and cancel one tracked order.
//!
//! By default this example is a dry run. Pass `--send` to actually queue the
//! cancel command on the server.
//!
//! Run:
//!   cargo run --example cancel_open_order --release -- "<key_base64>" "host:port" [order_uid] [--send]

use std::env;
use std::time::Duration;

use moonproto::state::OrderEvent;
use moonproto::{
    connect_and_init, import_key, Client, ClientConfig, ConnectConfig, Event, EventDispatcher,
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

    let keys = import_key(&args[1]).expect("invalid key");
    let (server_ip, server_port) = parse_host(host_arg);

    let cfg = ClientConfig::new(server_ip, server_port, keys.master_key, keys.mac_key);
    let mut client = Client::new(cfg);
    let mut dispatcher = EventDispatcher::new();

    let init = InitConfig {
        step_timeout: None,
        ..Default::default()
    };

    println!("[connect] waiting for ready client...");
    if let Err(err) = connect_and_init(
        &mut client,
        &mut dispatcher,
        ConnectConfig::new(init).with_connect_timeout(Duration::from_secs(15)),
    ) {
        eprintln!("[connect] failed: {err}");
        std::process::exit(2);
    }

    println!("[orders] requesting snapshot...");
    let mut orders = match client.request_order_snapshot(&mut dispatcher, Duration::from_secs(15)) {
        Ok(orders) => orders,
        Err(err) => {
            eprintln!("[orders] snapshot failed: {err:?}");
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
        client.disconnect();
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
        println!("[dry-run] cancel was not sent; pass --send to queue TOrderCancelCommand");
        client.disconnect();
        return;
    }

    if !client.cancel_tracked_order(dispatcher.orders_mut(), order.uid) {
        println!("[send] cancel was not queued; local order state is no longer cancellable");
        client.disconnect();
        return;
    }
    println!("[send] cancel queued; listening briefly for order updates...");

    client.run_with_dispatcher(
        Duration::from_secs(5),
        &mut dispatcher,
        Box::new(|event| {
            if let Event::Order(order_event) = event {
                match order_event {
                    OrderEvent::Updated(uid) => println!("[order] updated uid={uid}"),
                    OrderEvent::Removed(uid) => println!("[order] removed uid={uid}"),
                    OrderEvent::Ignored { uid, reason } => {
                        println!("[order] ignored uid={uid} reason={reason:?}");
                    }
                    _ => {}
                }
            }
        }),
    );

    client.disconnect();
}
