//! Compact end-to-end Active Lib flow for application developers.
//!
//! It uses only `MoonClient`: connect/init, subscriptions, async refreshes,
//! snapshots/events, and order intents.
//!
//! Run:
//!   cargo run --example trading_flow --release -- "<key_base64>" [host:port] [market]

use std::env;
use std::time::{Duration, Instant};

use moonproto::{Event, TradesStreamMode};

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

    for lifecycle in client.drain_lifecycle_events() {
        println!("[lifecycle] {lifecycle:?}");
    }

    client
        .streams()
        .subscribe_trades_for(TradesStreamMode::TradesOnly, [market])
        .expect("runtime stopped");

    if let Some(snapshot) = client.snapshot() {
        println!(
            "[snapshot] markets={} orders={} total_pnl={} strategies={}",
            snapshot.markets().market_count(),
            snapshot.orders().len(),
            snapshot.balances().global().total_pnl,
            snapshot.strategy_snapshot_vec().len()
        );
    }

    if let Err(err) = client.balances().refresh() {
        println!("[balance] request queue failed: {err}");
    }

    // Example only: uncomment in a real trading UI after explicit user action.
    // client.trade().new_order(
    //     moonproto::NewOrderParams::new(market, moonproto::OrderSide::Long, 50_000.0, 0.001),
    // )?;

    if let Err(err) = client.settings().refresh() {
        println!("[settings] request queue failed: {err}");
    }

    if let Err(err) = client.orders().request_snapshot() {
        println!("[orders] request queue failed: {err}");
    }

    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        for event in client.drain_events() {
            match event {
                Event::Order(moonproto::state::OrderEvent::Snapshot) => {
                    if let Some(snapshot) = client.snapshot() {
                        println!("[orders] count={}", snapshot.orders().len());
                        if let Some(order) = snapshot.orders().iter().next() {
                            println!(
                                "[orders] first uid={} market={} status={:?}; order intents use client.orders()",
                                order.uid, order.market_name, order.status
                            );
                        }
                    }
                }
                Event::Order(event) => println!("[event] order: {event:?}"),
                Event::Balance(event) => {
                    println!("[event] balance: {event:?}");
                    if let Some(snapshot) = client.snapshot() {
                        let global = snapshot.balances().global();
                        println!(
                            "[balance] btc_total={:.8} btc_full={:.8} special_coin={:.8}",
                            global.btc_balance_total,
                            global.btc_balance_full,
                            global.special_coin_balance
                        );
                    }
                }
                Event::OrderBook(event) => println!("[event] orderbook: {event:?}"),
                Event::Trade(event) => println!("[event] trade: {event:?}"),
                Event::Markets(event) => println!("[event] markets: {event:?}"),
                Event::Settings(moonproto::state::SettingsEvent::ClientSettingsUpdated) => {
                    if let Some(snapshot) = client.snapshot() {
                        if let Some(settings) = &snapshot.settings().client_settings {
                            println!(
                                "[settings] uid={} manual_strategy={} stop_market={}",
                                settings.uid,
                                settings.use_manual_strategy,
                                settings.use_stop_market
                            );
                        }
                    }
                }
                _ => {}
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    client
        .streams()
        .unsubscribe_orderbook(market)
        .expect("runtime stopped");
    client
        .streams()
        .unsubscribe_all_trades()
        .expect("runtime stopped");
}
