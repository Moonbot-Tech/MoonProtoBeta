//! Live-server integration smoke test — закрывает HANDOFF §4.3.
//!
//! **Запуск:**
//! ```powershell
//! $env:MOONPROTO_LIVE_SERVER = "207.148.91.186:3000"
//! $env:MOONPROTO_KEY = "v3oshQy/OLZSjsCkpZIOuy4y7aWoD7U12kIXJSx7h8cBKiRjEVPSrBB8WVO7yCjC..."
//! cargo test --test integration_smoke -- --ignored --nocapture
//! ```
//!
//! Без env-переменных тест помечен `#[ignore]` — обычный `cargo test --lib` его пропускает.
//!
//! **Зачем.** Iter-7 урок: 5 параллельных статических аудитов (`audit5`) НЕ нашли
//! CRITICAL баг в `parse_engine_response` (читал request_uid с offset 0 вместо 11 —
//! ВСЕ Engine API responses терялись). Статика смотрит docstrings / wire-format /
//! correctness, но не запускает код. Этот тест ловит **runtime regressions** в
//! критичных хэппи-pathах:
//!
//! 1. UDP socket bind + reader thread spawn.
//! 2. Handshake (Hello → WhoAreYou → ImFriend → Fine).
//! 3. `LifecycleEvent::Connected { fresh: true }` дошёл до callback'а.
//! 4. `run_init_sequence` (BaseCheck + AuthCheck + GetMarketsList + GetMarketsIndexes) успешен.
//! 5. `ServerInfo` корректно заполнен (multi-server identity).
//! 6. `MarketsState.indexes_synchronized = true` после GetMarketsIndexes.
//! 7. SubscribeAllTrades — реально получили хотя бы 1 TradesStream пакет.
//! 8. SubscribeOrderBook — реально получили хотя бы 1 OrderBook Apply event.
//!
//! Регресс любого из этих пунктов = блокер публикации.
//!
//! **Маркеры успеха.** Каждый шаг печатает `OK: <step>` в stdout. Pipeline-task
//! `audit_runtime` парсит output и считает все ожидаемые маркеры. Финальный
//! `RUNTIME_SMOKE_PASS` (или `RUNTIME_SMOKE_FAIL`) — финальный verdict.

use std::env;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use moonproto::client::{run_init_sequence, Client, ClientConfig, InitConfig, LifecycleEvent};
use moonproto::events::{Event, EventDispatcher};
use moonproto::key_import;

const STREAM_DURATION_SECS: u64 = 15;

fn load_env() -> Option<(String, u16, String)> {
    let server = env::var("MOONPROTO_LIVE_SERVER").ok()?;
    let (ip, port_str) = server.split_once(':')?;
    let port: u16 = port_str.parse().ok()?;
    let key_b64 = env::var("MOONPROTO_KEY").ok()?;
    Some((ip.to_string(), port, key_b64))
}

#[test]
#[ignore = "live-server required; set MOONPROTO_LIVE_SERVER + MOONPROTO_KEY env"]
fn runtime_smoke_full_happy_path() {
    let (ip, port, key_b64) = match load_env() {
        Some(v) => v,
        None => {
            eprintln!("RUNTIME_SMOKE_SKIP: env MOONPROTO_LIVE_SERVER + MOONPROTO_KEY not set");
            return;
        }
    };

    println!("=== runtime_smoke starting against {ip}:{port} ===");

    let keys = key_import::import_key(&key_b64).expect("invalid MOONPROTO_KEY");
    println!("OK: key_import");

    // === Phase 0: build Client (новый builder API — codex merge) ===
    let cfg = ClientConfig::new(&ip[..], port, keys.master_key, keys.mac_key);
    let mut client = Client::new(cfg);
    let mut dispatcher = EventDispatcher::new();
    println!("OK: client_new");

    // === Phase 1: lifecycle capture ===
    let fresh_seen = Arc::new(AtomicBool::new(false));
    let disconnected = Arc::new(AtomicBool::new(false));
    let lifecycle_log = Arc::new(Mutex::new(Vec::<LifecycleEvent>::new()));

    {
        let f = Arc::clone(&fresh_seen);
        let d = Arc::clone(&disconnected);
        let l = Arc::clone(&lifecycle_log);
        client.on_lifecycle(Box::new(move |ev: LifecycleEvent| {
            match &ev {
                LifecycleEvent::Connected { fresh: true } => f.store(true, Ordering::Relaxed),
                LifecycleEvent::Disconnected => d.store(true, Ordering::Relaxed),
                _ => {}
            }
            if let Ok(mut g) = l.lock() {
                g.push(ev);
            }
        }));
    }

    // === Phase 2: handshake (~3с до Connected{fresh:true}) ===
    client.run_with_dispatcher(Duration::from_secs(5), &mut dispatcher, Box::new(|_| {}));

    assert!(
        fresh_seen.load(Ordering::Relaxed),
        "FAIL: LifecycleEvent::Connected {{ fresh: true }} не получен за 5с handshake'а"
    );
    println!("OK: handshake_connected_fresh");

    assert!(
        client.is_authorized(),
        "FAIL: client.is_authorized() == false после handshake"
    );
    println!("OK: client_authorized");

    // === Phase 3: init sequence ===
    let init_cfg = InitConfig {
        mm_orders_subscribe: None,
        subscribe_trades: Some(false),
        subscribe_orderbooks: vec!["BTCUSDT".to_string()],
        step_timeout: None,
    };
    let init_result = run_init_sequence(&mut client, &mut dispatcher, init_cfg)
        .expect("FAIL: run_init_sequence returned Err");

    assert!(
        init_result.base_check_ok,
        "FAIL: BaseCheck failed: {:?}",
        init_result.errors
    );
    println!("OK: base_check");

    assert!(
        init_result.auth_check_ok,
        "FAIL: AuthCheck failed: {:?}",
        init_result.errors
    );
    println!("OK: auth_check");

    assert!(
        init_result.markets_response_bytes > 0,
        "FAIL: GetMarketsList вернул 0 байт payload'а"
    );
    println!(
        "OK: get_markets_list ({} bytes)",
        init_result.markets_response_bytes
    );

    // === Phase 4: ServerInfo заполнен ===
    let info = client.server_info();
    assert!(
        info.has_identity(),
        "FAIL: ServerInfo пустой после BaseCheck (server старый или регресс парсера)"
    );
    println!(
        "OK: server_info bot_id={:?} exchange={:?} ver={:?}",
        info.bot_id, info.exchange_name, info.server_version
    );

    // === Phase 5: MarketsState.indexes_synchronized (gate) ===
    // run_init_sequence теперь делает GetMarketsIndexes в init, когда он включен
    // явно или нужен для подписок на indexed streams.
    assert!(
        init_result.indexes_response_bytes > 0,
        "FAIL: GetMarketsIndexes вернул 0 байт payload'а"
    );
    assert!(
        dispatcher.markets().indexes_synchronized,
        "FAIL: indexes_synchronized = false после GetMarketsIndexes (gate не снят)"
    );
    println!("OK: indexes_synchronized");

    // === Phase 6: streaming — реально получаем TradesStream + OrderBook ===
    let trades_packets = Arc::new(AtomicU32::new(0));
    let orderbook_applied = Arc::new(AtomicU32::new(0));
    let parse_failures = Arc::new(AtomicU32::new(0));

    {
        let t = Arc::clone(&trades_packets);
        let o = Arc::clone(&orderbook_applied);
        let p = Arc::clone(&parse_failures);

        client.run_with_dispatcher(
            Duration::from_secs(STREAM_DURATION_SECS),
            &mut dispatcher,
            Box::new(move |ev| match ev {
                Event::Trade(_) => {
                    t.fetch_add(1, Ordering::Relaxed);
                }
                Event::OrderBook(moonproto::state::OrderBookEvent::Apply { .. }) => {
                    o.fetch_add(1, Ordering::Relaxed);
                }
                Event::ParseFailed { cmd, len, .. } => {
                    eprintln!("WARN: ParseFailed cmd={cmd:?} len={len}");
                    p.fetch_add(1, Ordering::Relaxed);
                }
                _ => {}
            }),
        );
    }

    let n_trades = trades_packets.load(Ordering::Relaxed);
    let n_books = orderbook_applied.load(Ordering::Relaxed);
    let n_failed = parse_failures.load(Ordering::Relaxed);

    println!("streaming stats: trades={n_trades} books={n_books} parse_failed={n_failed}");

    assert!(
        n_trades > 0,
        "FAIL: 0 Trades packets за {STREAM_DURATION_SECS}с — подписка не сработала или gate не снят"
    );
    println!("OK: trades_stream ({n_trades} packets)");

    assert!(
        n_books > 0,
        "FAIL: 0 OrderBook Apply events за {STREAM_DURATION_SECS}с — подписка на BTCUSDT не сработала"
    );
    println!("OK: order_book_stream ({n_books} apply events)");

    assert!(
        n_failed == 0,
        "FAIL: {n_failed} ParseFailed events — wire-format регресс или сервер шлёт неизвестное"
    );
    println!("OK: no_parse_failures");

    // === Phase 7: graceful disconnect ===
    client.disconnect();
    // Дать main loop'у обработать disconnect.
    client.run_with_dispatcher(
        Duration::from_millis(500),
        &mut dispatcher,
        Box::new(|_| {}),
    );
    assert!(
        disconnected.load(Ordering::Relaxed),
        "FAIL: LifecycleEvent::Disconnected не получен после client.disconnect()"
    );
    println!("OK: graceful_disconnect");

    // Финальная сводка lifecycle (для отладки).
    if let Ok(g) = lifecycle_log.lock() {
        eprintln!("--- lifecycle events seen ({}) ---", g.len());
        for ev in g.iter() {
            eprintln!("  {ev:?}");
        }
    }

    println!("\nRUNTIME_SMOKE_PASS: all 11 checkpoints passed against {ip}:{port}");
}
