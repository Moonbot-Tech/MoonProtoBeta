//! Compact end-to-end Active Lib flow for application developers.
//!
//! It uses only `MoonClient`: connect/init, subscriptions, one-shot reads,
//! snapshots/events, and order intents.
//!
//! Run:
//!   cargo run --example trading_flow --release -- "<key_base64>" [host:port] [market]

use std::env;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use moonproto::Event;

mod common;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: trading_flow <key_base64> [host:port] [market]");
        std::process::exit(1);
    }

    let market = args.get(3).map(String::as_str).unwrap_or("BTCUSDT");
    let mut init = common::init_config();
    init.subscribe_orderbooks.push(market.to_string());

    let client = match common::connect(&args[1], args.get(2), init) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("[connect/init] failed: {err}");
            std::process::exit(2);
        }
    };

    client
        .subscribe_trades_for(false, [market])
        .expect("runtime stopped");

    if let Some(snapshot) = client.snapshot() {
        println!(
            "[snapshot] markets={} orders={} balances={} strategies={}",
            snapshot.markets().market_count(),
            snapshot.orders().len(),
            snapshot.balances().len(),
            snapshot.strategy_snapshot_vec().len()
        );
    }

    match client.request_balance("USDT", Duration::from_secs(15)) {
        Ok(balance) => println!("[balance] USDT={balance}"),
        Err(err) => println!("[balance] request failed: {err}"),
    }

    match client.request_client_settings(Duration::from_secs(15)) {
        Ok(settings) => println!(
            "[settings] uid={} manual_strategy={} stop_market={}",
            settings.uid, settings.use_manual_strategy, settings.use_stop_market
        ),
        Err(err) => println!("[settings] request failed: {err}"),
    }

    match client.request_order_snapshot(Duration::from_secs(15)) {
        Ok(orders) => {
            println!("[orders] count={}", orders.len());
            if let Some(order) = orders.first() {
                println!(
                    "[orders] first uid={} market={} status={:?}; order intents use client.orders()",
                    order.uid, order.market_name, order.status
                );
                // Example only: uncomment in a real trading UI after user action.
                // client.orders().cancel(order.uid)?;
            }
        }
        Err(err) => println!("[orders] request failed: {err}"),
    }

    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        match client.recv_event_timeout(Duration::from_millis(500)) {
            Ok(Event::Order(event)) => println!("[event] order: {event:?}"),
            Ok(Event::OrderBook(event)) => println!("[event] orderbook: {event:?}"),
            Ok(Event::Trade(event)) => println!("[event] trade: {event:?}"),
            Ok(Event::Markets(event)) => println!("[event] markets: {event:?}"),
            Ok(_) => {}
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    client
        .unsubscribe_orderbook(market)
        .expect("runtime stopped");
    client.unsubscribe_all_trades().expect("runtime stopped");
}
