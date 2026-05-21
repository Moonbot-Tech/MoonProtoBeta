//! Subscribe to one orderbook and print the current best bid/ask.
//!
//! This is the consumer-facing path for a terminal widget: the application asks
//! for a market by name, while the library resolves indexes, applies full/diff
//! packets into an orderbook read model, replays the subscription after
//! reconnect, and requests a fresh full snapshot on gaps.
//!
//! Run:
//!   cargo run --example order_book_top --release -- "<key_base64>" "host:port" BTCUSDT 30

use std::env;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use moonproto::state::{OrderBookEvent, OrderBookKind};
use moonproto::{
    connect_and_init, import_key, Client, ClientConfig, ConnectConfig, Event,
    EventDispatcher, InitConfig,
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

fn kind_name(kind: OrderBookKind) -> &'static str {
    match kind {
        OrderBookKind::Futures => "futures",
        OrderBookKind::Spot => "spot",
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: order_book_top <key_base64> [host:port] [market] [watch_seconds]");
        std::process::exit(1);
    }

    let keys = import_key(&args[1]).expect("invalid key");
    let (server_ip, server_port) = parse_host(args.get(2));
    let market = args.get(3).cloned().unwrap_or_else(|| "BTCUSDT".to_string());
    let watch_secs: u64 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(30);

    let cfg = ClientConfig::new(server_ip, server_port, keys.master_key, keys.mac_key);
    let mut client = Client::new(cfg);
    let mut dispatcher = EventDispatcher::new();

    let init = InitConfig {
        base_check: true,
        auth_check: true,
        fetch_markets: true,
        subscribe_orderbooks: vec![market.clone()],
        step_timeout: Some(Duration::from_secs(10)),
        ..Default::default()
    };

    println!("[connect] init and subscribe market={market}");
    let init_result = connect_and_init(
        &mut client,
        &mut dispatcher,
        ConnectConfig::new(init).with_connect_timeout(Duration::from_secs(15)),
    )?;
    for err in &init_result.errors {
        eprintln!("[init] non-critical error: {err}");
    }

    if let Some(name) = client.server_info().server_name.as_deref() {
        println!("[server] {name}");
    }

    let updates = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + Duration::from_secs(watch_secs);

    while Instant::now() < deadline {
        let target_market = market.clone();
        let updates_seen = Arc::clone(&updates);
        let tick = Duration::from_secs(5).min(deadline.saturating_duration_since(Instant::now()));

        client.run_with_dispatcher_state(
            tick,
            &mut dispatcher,
            Box::new(move |event, state| match event {
                Event::OrderBook(OrderBookEvent::Apply { market_index, book_kind, seq, .. }) => {
                    let Some(name) = state.markets().market_name_by_index(*market_index) else {
                        return;
                    };
                    if name != target_market {
                        return;
                    }
                    let Some(kind) = OrderBookKind::from_u8(*book_kind) else {
                        return;
                    };
                    let Some(top) = state.order_books().top_of_book(*market_index, kind) else {
                        return;
                    };
                    updates_seen.fetch_add(1, Ordering::Relaxed);
                    let bid = top
                        .bid
                        .map(|level| format!("{} @ {}", level.quantity, level.rate))
                        .unwrap_or_else(|| "none".to_string());
                    let ask = top
                        .ask
                        .map(|level| format!("{} @ {}", level.quantity, level.rate))
                        .unwrap_or_else(|| "none".to_string());
                    println!("[top] {name} {} seq={} bid={} ask={}", kind_name(kind), seq, bid, ask);
                }
                _ => {}
            }),
        );
    }

    println!(
        "[done] top-updates={}",
        updates.load(Ordering::Relaxed)
    );
    client.disconnect();
    Ok(())
}
