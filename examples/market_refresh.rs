//! Observe market prices and token tags through the public active-library API.
//!
//! The example intentionally does not call `api_update_markets_list` or
//! `api_check_binance_tags` on a timer. `ClientConfig::default().refresh` owns
//! that work; consumer code only reads `MarketsEvent` and `dispatcher.markets()`.
//!
//! Run:
//!   cargo run --example market_refresh --release -- "<key_base64>" "host:port" BTCUSDT 75

use std::env;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use moonproto::{
    import_key, run_init_sequence, Client, ClientConfig, Event, EventDispatcher, InitConfig,
};
use moonproto::state::MarketsEvent;

fn parse_host(value: Option<&String>) -> (String, u16) {
    let Some(value) = value else {
        return ("127.0.0.1".to_string(), 3000);
    };
    let Some((host, port)) = value.split_once(':') else {
        return (value.clone(), 3000);
    };
    (host.to_string(), port.parse().unwrap_or(3000))
}

fn print_market(dispatcher: &EventDispatcher, market: &str) {
    let state = dispatcher.markets();
    let price = state.price(market);
    let tags = state.tags(market);

    match price {
        Some(price) => println!(
            "[state] {market} bid={} ask={} mark={} funding={} tags=0x{:x}",
            price.bid,
            price.ask,
            price.mark_price,
            price.funding_rate,
            tags.bits()
        ),
        None => println!(
            "[state] {market} not found yet; markets={} tags=0x{:x}",
            state.market_count(),
            tags.bits()
        ),
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: market_refresh <key_base64> [host:port] [market] [watch_seconds]");
        std::process::exit(1);
    }

    let keys = import_key(&args[1]).expect("invalid key");
    let (server_ip, server_port) = parse_host(args.get(2));
    let market = args.get(3).map(String::as_str).unwrap_or("BTCUSDT");
    let watch_secs: u64 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(75);

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
    print_market(&dispatcher, market);

    let prices = Arc::new(AtomicU64::new(0));
    let tags = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + Duration::from_secs(watch_secs);

    while Instant::now() < deadline {
        let prices_seen = Arc::clone(&prices);
        let tags_seen = Arc::clone(&tags);
        client.run_with_dispatcher(
            Duration::from_secs(5).min(deadline.saturating_duration_since(Instant::now())),
            &mut dispatcher,
            Box::new(move |event| {
                if let Event::Markets(event) = event {
                    match event {
                        MarketsEvent::PricesUpdated { count, .. } => {
                            prices_seen.fetch_add(1, Ordering::Relaxed);
                            println!("[event] prices updated: {count}");
                        }
                        MarketsEvent::TokenTagsUpdated { count } => {
                            tags_seen.fetch_add(1, Ordering::Relaxed);
                            println!("[event] token tags updated: {count}");
                        }
                        other => println!("[event] markets: {other:?}"),
                    }
                }
            }),
        );
        print_market(&dispatcher, market);
    }

    println!(
        "[done] price updates={} tag updates={}",
        prices.load(Ordering::Relaxed),
        tags.load(Ordering::Relaxed)
    );
    client.disconnect();
}
