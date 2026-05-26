//! Request the current UI/settings snapshot through `MoonClient`.
//!
//! Run:
//!   cargo run --example request_client_settings --release -- "<key_base64>" [host:port]

use std::env;
use std::time::Duration;

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

    let settings = match client.request_client_settings(Duration::from_secs(15)) {
        Ok(settings) => settings,
        Err(err) => {
            eprintln!("[request] failed: {err}");
            std::process::exit(3);
        }
    };

    println!(
        "[settings] uid={} x_sell={} x_sell_scalp={} stop_loss={} use_take_profit={} take_profit={}",
        settings.uid,
        settings.x_sell,
        settings.x_sell_scalp,
        settings.price_drop_level,
        settings.use_g_take_profit,
        settings.g_take_profit,
    );
    println!(
        "[settings] manual_strategy={} stop_market={} join_sell_kind={} temp_blacklist={}",
        settings.use_manual_strategy,
        settings.use_stop_market,
        settings.join_sell_kind,
        settings.temp_bl_symbols.len(),
    );
}
