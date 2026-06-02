//! Request the current UI/settings snapshot through the public `MoonClient`
//! event/snapshot path.
//!
//! Run:
//!   cargo run --example request_client_settings --release -- "<key_base64>" [host:port]

use std::env;
use std::time::Duration;

use moonproto::state::SettingsEvent;
use moonproto::Event;

mod common;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: request_client_settings <key_base64> [host:port]");
        std::process::exit(1);
    }

    let client = match common::connect(&args[1], args.get(2), common::init_config()) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("[connect/init] failed: {err}");
            std::process::exit(2);
        }
    };

    if let Err(err) = client.settings().refresh() {
        eprintln!("[request] failed: {err}");
        std::process::exit(3);
    }
    let ready = common::wait_until(Duration::from_secs(15), || {
        client
            .drain_events()
            .into_iter()
            .any(|event| matches!(event, Event::Settings(SettingsEvent::ClientSettingsUpdated)))
    });
    if !ready {
        eprintln!("[request] timed out waiting for SettingsEvent::ClientSettingsUpdated");
        std::process::exit(3);
    }

    let snapshot = client
        .snapshot()
        .expect("settings event must publish state snapshot");
    let settings = snapshot
        .settings()
        .client_settings
        .as_ref()
        .expect("settings event must store ClientSettingsCommand");

    println!(
        "[settings] take_profit_percent={} x_sell={} x_sell_scalp={} stop_loss={} use_take_profit={} take_profit={}",
        settings.effective_take_profit_percent(),
        settings.x_sell,
        settings.x_sell_scalp,
        settings.price_drop_level,
        settings.use_g_take_profit,
        settings.g_take_profit,
    );
    println!(
        "[settings] manual_strategy={} stop_market={} join_sell_mode={:?} temp_blacklist_rows={}",
        settings.use_manual_strategy,
        settings.use_stop_market,
        settings.join_sell_mode(),
        settings.temp_blacklist_entries().count(),
    );
}
