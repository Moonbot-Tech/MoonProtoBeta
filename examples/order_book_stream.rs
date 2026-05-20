//! Subscribe to one orderbook stream and print incoming updates.
//!
//! This is the high-level consumer path: the application subscribes by market
//! name, while the library owns the registry, reconnect replay, index gating
//! and full-book recovery. `OrderBookKind` is read from incoming events; it is
//! not an input to subscribe/unsubscribe.
//!
//! Run:
//!   cargo run --example order_book_stream --release -- "<key_base64>" "host:port" BTCUSDT 30

use std::env;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use moonproto::{
    import_key, run_init_sequence, Client, ClientConfig, Event, EventDispatcher, InitConfig,
};
use moonproto::state::OrderBookEvent;

fn parse_host(value: Option<&String>) -> (String, u16) {
    let Some(value) = value else {
        return ("127.0.0.1".to_string(), 3000);
    };
    let Some((host, port)) = value.split_once(':') else {
        return (value.clone(), 3000);
    };
    (host.to_string(), port.parse().unwrap_or(3000))
}

fn book_kind_name(kind: u8) -> &'static str {
    match kind {
        0 => "futures",
        1 => "spot",
        _ => "unknown",
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: order_book_stream <key_base64> [host:port] [market] [watch_seconds]");
        std::process::exit(1);
    }

    let keys = import_key(&args[1]).expect("invalid key");
    let (server_ip, server_port) = parse_host(args.get(2));
    let market = args.get(3).cloned().unwrap_or_else(|| "BTCUSDT".to_string());
    let watch_secs: u64 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(30);

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
        fetch_markets: true,
        subscribe_orderbooks: vec![market.clone()],
        step_timeout: Some(Duration::from_secs(10)),
        ..Default::default()
    };
    let init_result = match run_init_sequence(&mut client, &mut dispatcher, init) {
        Ok(result) => result,
        Err(err) => {
            eprintln!("[init] failed: {err}");
            std::process::exit(3);
        }
    };
    for err in &init_result.errors {
        eprintln!("[init] non-critical error: {err}");
    }

    if let Some(name) = client.server_info().server_name.as_deref() {
        println!("[server] {name}");
    }
    println!("[subscribe] orderbook market={market}");

    let applies = Arc::new(AtomicU64::new(0));
    let fulls = Arc::new(AtomicU64::new(0));
    let full_requests = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + Duration::from_secs(watch_secs);

    while Instant::now() < deadline {
        let applies_seen = Arc::clone(&applies);
        let fulls_seen = Arc::clone(&fulls);
        let full_requests_seen = Arc::clone(&full_requests);
        client.run_with_dispatcher(
            Duration::from_secs(5).min(deadline.saturating_duration_since(Instant::now())),
            &mut dispatcher,
            Box::new(move |event| {
                if let Event::OrderBook(event) = event {
                    match event {
                        OrderBookEvent::Apply {
                            market_index,
                            book_kind,
                            is_full,
                            seq,
                            buys,
                            sells,
                        } => {
                            applies_seen.fetch_add(1, Ordering::Relaxed);
                            if *is_full {
                                fulls_seen.fetch_add(1, Ordering::Relaxed);
                            }
                            println!(
                                "[book] idx={} kind={} full={} seq={} bids={} asks={}",
                                market_index,
                                book_kind_name(*book_kind),
                                is_full,
                                seq,
                                buys.len(),
                                sells.len()
                            );
                        }
                        OrderBookEvent::RequestFullNeeded { market_index, book_kind } => {
                            full_requests_seen.fetch_add(1, Ordering::Relaxed);
                            println!(
                                "[book] request-full idx={} kind={}",
                                market_index,
                                book_kind_name(*book_kind)
                            );
                        }
                        OrderBookEvent::Ignored { market_index, book_kind, seq, reason } => {
                            println!(
                                "[book] ignored idx={} kind={} seq={} reason={reason:?}",
                                market_index,
                                book_kind_name(*book_kind),
                                seq
                            );
                        }
                    }
                }
            }),
        );
    }

    println!(
        "[done] applies={} fulls={} full-requests={}",
        applies.load(Ordering::Relaxed),
        fulls.load(Ordering::Relaxed),
        full_requests.load(Ordering::Relaxed)
    );
    client.disconnect();
}
