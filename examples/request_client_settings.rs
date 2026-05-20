//! Request the current UI settings snapshot.
//!
//! Demonstrates `Client::request_client_settings`, the high-level helper for
//! `TSettingsRequest` + `TClientSettingsCommand`. The consumer does not need to
//! wait for `SettingsEvent::ClientSettingsUpdated` manually.
//!
//! Run:
//!   cargo run --example request_client_settings --release -- "<key_base64>" "host:port"

use std::env;
use std::time::Duration;

use moonproto::{
    import_key, run_init_sequence, Client, ClientConfig, EventDispatcher, InitConfig,
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

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: request_client_settings <key_base64> [host:port]");
        std::process::exit(1);
    }

    let keys = import_key(&args[1]).expect("invalid key");
    let (server_ip, server_port) = parse_host(args.get(2));

    let cfg = ClientConfig::new(server_ip, server_port, keys.master_key, keys.mac_key);
    let mut client = Client::new(cfg);
    let mut dispatcher = EventDispatcher::new();
    client.on_lifecycle(Box::new(|event| println!("[lifecycle] {event:?}")));

    println!("[connect] waiting for authorization...");
    client.run_with_dispatcher(Duration::from_secs(15), &mut dispatcher, Box::new(|_| {}));
    if !client.is_authorized() {
        eprintln!("[connect] authorization timeout, status={:?}", client.auth_status());
        std::process::exit(2);
    }

    let init = InitConfig {
        base_check: true,
        auth_check: true,
        step_timeout: Some(Duration::from_secs(10)),
        ..Default::default()
    };
    if let Err(err) = run_init_sequence(&mut client, &mut dispatcher, init) {
        eprintln!("[init] failed: {err}");
        std::process::exit(3);
    }

    if let Some(name) = client.server_info().server_name.as_deref() {
        println!("[server] {name}");
    }

    println!("[request] client settings");
    let settings = match client.request_client_settings(&mut dispatcher, Duration::from_secs(15)) {
        Ok(settings) => settings,
        Err(err) => {
            eprintln!("[request] timeout/disconnected: {err:?}");
            std::process::exit(4);
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

    client.disconnect();
}
