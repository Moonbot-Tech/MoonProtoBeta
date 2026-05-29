//! Hedge-mode refresh through the public `MoonClient` event/snapshot path.
//!
//! Regular UI code should call `refresh_hedge_mode()` and read
//! `snapshot().account().hedge_mode()` after `Event::Account`.
//!
//! Run:
//!   cargo run --example query_hedge_mode --release -- "<key_base64>" [host:port]

use std::env;
use std::time::Duration;

use moonproto::state::AccountEvent;
use moonproto::Event;

mod common;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: query_hedge_mode <key_base64> [host:port]");
        std::process::exit(1);
    }

    let client = match common::connect(&args[1], args.get(2), common::init_config()) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("[connect/init] failed: {err}");
            std::process::exit(2);
        }
    };

    if let Err(err) = client.account().refresh_hedge_mode() {
        eprintln!("[request] failed: {err}");
        std::process::exit(3);
    }
    let ready = common::wait_until(Duration::from_secs(15), || {
        client
            .drain_events()
            .into_iter()
            .any(|event| matches!(event, Event::Account(AccountEvent::HedgeModeUpdated { .. })))
    });
    if !ready {
        eprintln!("[request] timed out waiting for Event::Account(HedgeModeUpdated)");
        std::process::exit(3);
    }

    let hedge_mode = client
        .snapshot()
        .and_then(|snapshot| snapshot.account().hedge_mode())
        .expect("hedge-mode event must publish account snapshot");
    println!("[response] hedge_mode={hedge_mode}");
}
