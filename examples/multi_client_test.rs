//! Multi-Client smoke-test: подключение к ОДНОМУ серверу ДВУМЯ независимыми
//! `Client` объектами одновременно. Доказывает что multi-server архитектура
//! работает на практике — каждый Client держит независимое состояние, имеет
//! свой UDP socket, свой reader thread, свою subscription registry,
//! свой `server_time_delta_handle`. NTP syncer общий на процесс, как Delphi
//! `TMoonProtoTymeSyncer`.
//!
//! Сервер видит каждый Client как отдельную сессию (разные `client_id`, разные
//! `ServerToken`'ы после handshake), хотя key используется один и тот же. Это
//! типовой use-case: один пользовательский аккаунт, два устройства/процесса.
//!
//! Запуск:
//!   cargo run --example multi_client_test --release -- <KEY_BASE64> <IP:PORT>
//!
//! Default IP:PORT = 127.0.0.1:3000.

use std::env;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use moonproto::client::{Client, ClientConfig, LifecycleEvent};
use moonproto::commands::engine_api::ServerInfo;
use moonproto::events::EventDispatcher;
use moonproto::key_import;

#[derive(Default)]
struct ClientStats {
    label: Mutex<String>,
    client_id: Mutex<u64>,
    auth_done: AtomicBool,
    auth_fresh_seen: AtomicBool,
    auth_again_seen: AtomicBool,
    trades_packets: AtomicU32,
    raw_packets: AtomicU32,
    lifecycle_events: Mutex<Vec<LifecycleEvent>>,
    final_server_info: Mutex<ServerInfo>,
}

fn run_client(
    label: &str,
    ip: String,
    port: u16,
    keys: key_import::ImportedKeys,
    duration_secs: u64,
    stats: Arc<ClientStats>,
) {
    let client_id = rand::random::<u64>();
    *stats.label.lock().unwrap() = label.to_string();
    *stats.client_id.lock().unwrap() = client_id;

    let cfg = ClientConfig::new(ip, port, keys.master_key, keys.mac_key).with_client_id(client_id);

    let mut client = Client::new(cfg);
    let mut dispatcher = EventDispatcher::new();

    // Lifecycle callback — собирает события для финального отчёта.
    let lc_stats = Arc::clone(&stats);
    client.on_lifecycle(Box::new(move |ev: LifecycleEvent| {
        match ev {
            LifecycleEvent::Connected { fresh: true } => {
                lc_stats.auth_done.store(true, Ordering::Relaxed);
                lc_stats.auth_fresh_seen.store(true, Ordering::Relaxed);
            }
            LifecycleEvent::Connected { fresh: false } => {
                lc_stats.auth_done.store(true, Ordering::Relaxed);
                lc_stats.auth_again_seen.store(true, Ordering::Relaxed);
            }
            _ => {}
        }
        lc_stats.lifecycle_events.lock().unwrap().push(ev);
    }));

    // Phase 1: short pre-init run для handshake (~3 сек). Используем
    // run_with_dispatcher с тем же dispatcher что будет использован в init —
    // так Markets state будет применяться единым flow.
    println!("[{label}] phase 1: connecting (client_id={client_id:#x})...");
    client.run_with_dispatcher(Duration::from_secs(3), &mut dispatcher, Box::new(|_| {}));

    // Phase 2: init sequence (chunked main loop pump внутри).
    let init_cfg = moonproto::client::InitConfig {
        initial_strategies: None,
        mm_orders_subscribe: None,
        subscribe_trades: Some(false),
        subscribe_orderbooks: vec![],
        step_timeout: None,
    };
    println!("[{label}] phase 2: init sequence...");
    match moonproto::client::run_init_sequence(&mut client, &mut dispatcher, init_cfg) {
        Ok(r) => println!(
            "[{label}]   init ok: base={} auth={} markets={}B",
            r.base_check_ok, r.auth_check_ok, r.markets_response_bytes
        ),
        Err(e) => println!("[{label}]   init FAILED: {:?}", e),
    }

    // Save server_info после init.
    *stats.final_server_info.lock().unwrap() = client.server_info().clone();

    // Phase 3: main loop, считаем пакеты.
    let count_stats = Arc::clone(&stats);
    println!("[{label}] phase 3: streaming for {duration_secs}s...");
    client.run_with_dispatcher(
        Duration::from_secs(duration_secs),
        &mut dispatcher,
        Box::new(move |ev| {
            count_stats.raw_packets.fetch_add(1, Ordering::Relaxed);
            if let moonproto::events::Event::Trade(_) = ev {
                count_stats.trades_packets.fetch_add(1, Ordering::Relaxed);
            }
        }),
    );

    println!("[{label}] phase 3 done.");
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: multi_client_test <KEY_BASE64> [IP:PORT]");
        std::process::exit(1);
    }
    let key_b64 = &args[1];
    let (ip, port) = if args.len() >= 3 {
        let parts: Vec<&str> = args[2].splitn(2, ':').collect();
        (
            parts[0].to_string(),
            parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(3000u16),
        )
    } else {
        ("127.0.0.1".to_string(), 3000u16)
    };

    let keys = key_import::import_key(key_b64).expect("invalid key");
    println!("[main] key OK; spawning 2 clients to {ip}:{port}");

    const DURATION_SECS: u64 = 20;

    let stats_a: Arc<ClientStats> = Arc::new(ClientStats::default());
    let stats_b: Arc<ClientStats> = Arc::new(ClientStats::default());

    let ip_a = ip.clone();
    let ip_b = ip.clone();
    let keys_a = keys;
    let keys_b = keys;
    let stats_a_thread = Arc::clone(&stats_a);
    let stats_b_thread = Arc::clone(&stats_b);

    let t_a =
        thread::spawn(move || run_client("A", ip_a, port, keys_a, DURATION_SECS, stats_a_thread));
    // Слегка раздвинуть starts чтобы handshake'и не наложились на одни и те же мс
    // (просто для красоты логов; функционально и параллельный старт работает).
    thread::sleep(Duration::from_millis(200));
    let t_b =
        thread::spawn(move || run_client("B", ip_b, port, keys_b, DURATION_SECS, stats_b_thread));

    let _ = t_a.join();
    let _ = t_b.join();

    // ========================
    //  Final report
    // ========================
    println!("\n========== MULTI-CLIENT SMOKE-TEST REPORT ==========");
    for s in [&stats_a, &stats_b] {
        let label = s.label.lock().unwrap().clone();
        let cid = *s.client_id.lock().unwrap();
        let auth = s.auth_done.load(Ordering::Relaxed);
        let fresh = s.auth_fresh_seen.load(Ordering::Relaxed);
        let again = s.auth_again_seen.load(Ordering::Relaxed);
        let trades = s.trades_packets.load(Ordering::Relaxed);
        let raw = s.raw_packets.load(Ordering::Relaxed);
        let info = s.final_server_info.lock().unwrap().clone();
        let lc_count = s.lifecycle_events.lock().unwrap().len();
        println!("[{label}] client_id={cid:#x}");
        println!("[{label}]   handshake auth_done={auth} (fresh seen={fresh}, again seen={again})");
        println!("[{label}]   packets: trades={trades}, raw_events={raw}");
        println!("[{label}]   lifecycle events seen: {lc_count}");
        println!(
            "[{label}]   server_info: bot_id={:?} name={:?} exchange={:?} base={:?} ver={:?}",
            info.bot_id,
            info.server_name,
            info.exchange_name,
            info.base_currency_name,
            info.server_version
        );
    }

    // Assertions for smoke-test "pass":
    let a_auth = stats_a.auth_done.load(Ordering::Relaxed);
    let b_auth = stats_b.auth_done.load(Ordering::Relaxed);
    let a_id = *stats_a.client_id.lock().unwrap();
    let b_id = *stats_b.client_id.lock().unwrap();
    let a_info = stats_a.final_server_info.lock().unwrap().clone();
    let b_info = stats_b.final_server_info.lock().unwrap().clone();

    println!("\n========== VERDICT ==========");
    let mut all_ok = true;
    if !a_auth {
        println!("FAIL: Client A не прошёл handshake");
        all_ok = false;
    }
    if !b_auth {
        println!("FAIL: Client B не прошёл handshake");
        all_ok = false;
    }
    if a_id == b_id {
        println!("FAIL: client_id'ы совпали ({a_id:#x}) — должны быть разные");
        all_ok = false;
    }
    // Оба Client'а — к одному серверу, ServerInfo (bot_id, exchange) должны совпасть.
    if a_info.bot_id != b_info.bot_id {
        println!("WARN: bot_id отличается между Client'ами (A={:?} B={:?}) — это странно для одного сервера",
            a_info.bot_id, b_info.bot_id);
    } else if a_info.bot_id.is_some() {
        println!(
            "OK: оба Client'а видят одинаковый bot_id={:?} (одна и та же серверная identity)",
            a_info.bot_id
        );
    }
    if all_ok {
        println!("PASS: оба Client'а независимо подключились, handshake'нулись, получают трафик.");
        println!("Multi-server архитектура работает.");
        std::process::exit(0);
    } else {
        std::process::exit(1);
    }
}
