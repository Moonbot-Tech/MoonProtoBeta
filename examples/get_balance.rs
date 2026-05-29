//! Refresh transferable asset balances through the public `MoonClient`
//! event/snapshot path.
//!
//! Regular UI code follows the same model: queue an Active Lib refresh, keep the
//! UI alive, then read maintained state from `MoonClient::snapshot()`.
//!
//! Run:
//!   cargo run --example get_balance --release -- "<key_base64>" [host:port] [asset]

use std::env;
use std::time::Duration;

use moonproto::{Event, TransferAssetsEvent};

mod common;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: get_balance <key_base64> [host:port] [asset]");
        std::process::exit(1);
    }

    let asset = args.get(3).map(String::as_str).unwrap_or("USDT");
    let client = match common::connect(&args[1], args.get(2), common::init_config()) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("[connect/init] failed: {err}");
            std::process::exit(2);
        }
    };

    println!("[request] refresh transferable assets, asset={asset}");
    if let Err(err) = client.balances().refresh_transfer_assets() {
        eprintln!("[request] failed: {err}");
        std::process::exit(3);
    }

    let ready = common::wait_until(Duration::from_secs(15), || {
        client.drain_events().into_iter().any(|event| {
            matches!(
                event,
                Event::TransferAssets(TransferAssetsEvent::RefreshCompleted { failed: 0, .. })
            )
        })
    });
    if !ready {
        eprintln!("[request] timed out waiting for TransferAssetsEvent::RefreshCompleted");
        std::process::exit(3);
    }

    let snapshot = client
        .snapshot()
        .expect("transfer-assets event must publish dispatcher snapshot");
    let mut printed = 0usize;
    for (kind, rows) in snapshot.transfer_assets().iter() {
        for row in rows {
            if row.currency.eq_ignore_ascii_case(asset) {
                println!(
                    "[asset] kind={} currency={} amount={} total={}",
                    kind.name(),
                    row.currency,
                    row.amount,
                    row.total
                );
                printed += 1;
            }
        }
    }
    if printed == 0 {
        println!("[asset] {asset} not present in refreshed transferable-asset lists");
    }
}
