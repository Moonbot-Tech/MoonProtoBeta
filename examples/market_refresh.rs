//! Observe market price/tag refresh through `MoonClient` snapshots and events.
//!
//! Run:
//!   cargo run --example market_refresh --release -- "<key_base64>" [host:port] [market] [watch_seconds]

use std::env;
use std::time::{Duration, Instant};

use moonproto::state::MarketsEvent;
use moonproto::{Event, RefreshConfig};

mod common;

fn print_market(client: &moonproto::MoonClient, market: &str) {
    let Some(snapshot) = client.snapshot() else {
        println!("[state] no snapshot");
        return;
    };
    let state = snapshot.markets();
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

    let market = args.get(3).map(String::as_str).unwrap_or("BTCUSDT");
    let watch_secs: u64 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(75);
    let client = match common::connect_with_refresh(
        &args[1],
        args.get(2),
        common::init_config(),
        RefreshConfig {
            update_markets_every: Some(Duration::from_secs(2)),
            check_tags_every: Some(Duration::from_secs(60)),
        },
    ) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("[connect/init] failed: {err}");
            std::process::exit(2);
        }
    };

    print_market(&client, market);
    let mut prices = 0u64;
    let mut tags = 0u64;
    let deadline = Instant::now() + Duration::from_secs(watch_secs);

    while Instant::now() < deadline {
        for event in client.drain_events() {
            match event {
                Event::Markets(MarketsEvent::PricesUpdated { count, .. }) => {
                    prices += 1;
                    println!("[event] prices updated: {count}");
                    print_market(&client, market);
                }
                Event::Markets(MarketsEvent::TokenTagsUpdated { count }) => {
                    tags += 1;
                    println!("[event] token tags updated: {count}");
                    print_market(&client, market);
                }
                Event::Markets(other) => println!("[event] markets: {other:?}"),
                _ => {}
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    println!("[done] price updates={prices} tag updates={tags}");
}
