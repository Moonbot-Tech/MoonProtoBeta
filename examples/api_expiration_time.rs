//! API-key expiration refresh through the public `MoonClient` event/snapshot path.
//!
//! Regular UI code should call `refresh_api_expiration_time()` and read
//! `snapshot().account().api_expiration()` after `Event::Account`.
//!
//! Run:
//!   cargo run --example api_expiration_time --release -- "<key_base64>" [host:port]

use std::env;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use moonproto::state::AccountEvent;
use moonproto::Event;

mod common;

fn unix_seconds(time: SystemTime) -> Option<u64> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: api_expiration_time <key_base64> [host:port]");
        std::process::exit(1);
    }

    let client = match common::connect(&args[1], args.get(2), common::init_config()) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("[connect/init] failed: {err}");
            std::process::exit(2);
        }
    };

    if let Err(err) = client.account().refresh_api_expiration_time() {
        eprintln!("[request] failed: {err}");
        std::process::exit(3);
    }
    let ready = common::wait_until(Duration::from_secs(15), || {
        client.drain_events().into_iter().any(|event| {
            matches!(
                event,
                Event::Account(AccountEvent::ApiExpirationUpdated { .. })
            )
        })
    });
    if !ready {
        eprintln!("[request] timed out waiting for Event::Account(ApiExpirationUpdated)");
        std::process::exit(3);
    }
    let expiration = client
        .snapshot()
        .and_then(|snapshot| snapshot.account().api_expiration())
        .expect("expiration event must publish account snapshot");

    if let Some(time) = expiration.system_time() {
        let unix = unix_seconds(time).unwrap_or_default();
        let days = expiration.days_until(SystemTime::now()).unwrap_or_default();
        println!("[expiration] unix_seconds={unix} days_until={days}");
    } else {
        println!("[expiration] not reported");
    }
}
