//! Subscribe to the all-trades stream and print incoming trades with market names.
//!
//! Run:
//!   cargo run --example trades_stream --release -- "<key_base64>" "host:port" [market|all] [watch_seconds]

use std::env;
use std::sync::{atomic::{AtomicU64, Ordering}, Arc};
use std::time::{Duration, Instant};

use moonproto::commands::TradeSection;
use moonproto::state::TradesEvent;
use moonproto::{import_key, run_init_sequence, Client, ClientConfig, Event, EventDispatcher, InitConfig};

fn parse_host(value: Option<&String>) -> (String, u16) {
    let Some(value) = value else {
        return ("127.0.0.1".to_string(), 3000);
    };
    match value.split_once(':') {
        Some((host, port)) => (host.to_string(), port.parse().unwrap_or(3000)),
        None => (value.clone(), 3000),
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: trades_stream <key_base64> [host:port] [market|all] [watch_seconds]");
        std::process::exit(1);
    }

    let keys = import_key(&args[1]).expect("invalid key");
    let (server_ip, server_port) = parse_host(args.get(2));
    let market_filter = match args.get(3).map(String::as_str) {
        Some("all") | None => None,
        Some(name) => Some(name.to_string()),
    };
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
        subscribe_trades: Some(false),
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

    if !dispatcher.markets().indexes_synchronized {
        client.run_with_dispatcher(Duration::from_secs(5), &mut dispatcher, Box::new(|_| {}));
    }

    if let Some(name) = client.server_info().server_name.as_deref() {
        println!("[server] {name}");
    }
    let scope = market_filter.as_deref().unwrap_or("all markets");
    println!("[subscribe] all-trades, printing {scope}");

    let packets = Arc::new(AtomicU64::new(0));
    let trades = Arc::new(AtomicU64::new(0));
    let gaps = Arc::new(AtomicU64::new(0));
    let printed = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + Duration::from_secs(watch_secs);

    while Instant::now() < deadline {
        let packets_seen = Arc::clone(&packets);
        let trades_seen = Arc::clone(&trades);
        let gaps_seen = Arc::clone(&gaps);
        let printed_seen = Arc::clone(&printed);
        let target = market_filter.clone();

        client.run_with_dispatcher_state(
            Duration::from_secs(5).min(deadline.saturating_duration_since(Instant::now())),
            &mut dispatcher,
            Box::new(move |event, state| match event {
                Event::Trade(TradesEvent::Apply(packet)) => {
                    packets_seen.fetch_add(1, Ordering::Relaxed);
                    for section in &packet.sections {
                        let TradeSection::Trades(items) = section else {
                            continue;
                        };
                        for trade in items {
                            let name = state
                                .markets()
                                .market_name_by_index(trade.market_index)
                                .unwrap_or("<unknown>");
                            if target.as_deref().is_some_and(|wanted| wanted != name) {
                                continue;
                            }
                            trades_seen.fetch_add(1, Ordering::Relaxed);
                            if printed_seen.fetch_add(1, Ordering::Relaxed) < 25 {
                                let stream = if trade.is_spot { "spot" } else { "futures" };
                                let side = if trade.qty < 0.0 { "sell" } else { "buy" };
                                println!(
                                    "[trade] pkt={} {name} {stream} {} price={} qty={}",
                                    packet.packet_num,
                                    side,
                                    trade.price,
                                    trade.qty.abs()
                                );
                            }
                        }
                    }
                }
                Event::Trade(TradesEvent::GapDetected { start, end }) => {
                    gaps_seen.fetch_add(1, Ordering::Relaxed);
                    println!("[trade] gap detected {start}..{end}");
                }
                Event::Trade(TradesEvent::GapFilled { packet_num, .. }) => {
                    println!("[trade] gap filled packet={packet_num}");
                }
                _ => {}
            }),
        );
    }

    println!(
        "[done] packets={} trades={} gaps={}",
        packets.load(Ordering::Relaxed),
        trades.load(Ordering::Relaxed),
        gaps.load(Ordering::Relaxed)
    );
    client.disconnect();
}
