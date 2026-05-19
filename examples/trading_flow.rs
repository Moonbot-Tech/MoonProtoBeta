//! Полный пример торгового flow: подключение → подписка → ордер → cancel.
//!
//! Покрывает:
//! - Импорт ключа из base64 (MoonBot Settings → Export Key)
//! - NTP sync (рекомендуется до старта)
//! - Создание Client + ClientConfig
//! - Lifecycle callback (Connecting / Authenticated / ServerRestart / ...)
//! - EventDispatcher для авто-парсинга входящих в типизированные события
//! - Stack команд после Authenticated (api_subscribe_all_trades, balance refresh,
//!   strat snapshot, settings request)
//! - Engine API async response через mpsc::Receiver
//! - Trade команды (new_order, cancel_order)
//! - UI команды (mm_subscribe, switch_dex)
//! - Strat команды (strat_snapshot_request, strat_sell_price_update)
//!
//! Запуск:
//!   cargo run --example trading_flow --release -- "<key_base64>" "host:port"
//!
//! ВНИМАНИЕ: пример отправляет тестовые команды на сервер. Используй тестовый
//! ключ / тестовый сервер. Никаких реальных торговых операций по умолчанию
//! (комментарий `/* uncomment to send */`).

use std::env;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::time::{Duration, Instant};

use moonproto::client::{Client, ClientConfig, LifecycleEvent, set_ntp_offset};
use moonproto::events::{EventDispatcher, Event};
use moonproto::key_import;
use moonproto::ntp;
use moonproto::protocol::Command;
use moonproto::state::OrderEvent;
use moonproto::commands::trade::TradeCtx;

fn now_ms_for_dispatch() -> i64 {
    use std::time::SystemTime;
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

fn main() {
    // env_logger опционально — раскомментируй если нужны log message'и:
    /* env_logger::init(); */
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: trading_flow <key_b64> [host:port]");
        std::process::exit(1);
    }
    let key_b64 = &args[1];
    let (ip, port) = if args.len() >= 3 {
        let parts: Vec<&str> = args[2].splitn(2, ':').collect();
        (parts[0].to_string(), parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(3000u16))
    } else { ("127.0.0.1".to_string(), 3000u16) };

    // ---- 1. Импорт ключа ----
    let keys = key_import::import_key(key_b64).expect("invalid key");
    println!("[setup] key imported");

    // ---- 2. NTP sync ----
    let ntp_result = ntp::get_best_ntp("pool.ntp.org", 4);
    if ntp_result.synced {
        set_ntp_offset(ntp_result.time_offset);
        println!("[setup] NTP offset={:.1}ms rtt={}ms", ntp_result.time_offset * 1000.0, ntp_result.round_trip_ms);
    } else {
        println!("[setup] NTP failed, continuing with system clock");
    }

    // ---- 3. Client config ----
    let cfg = ClientConfig {
        server_ip:   ip,
        server_port: port,
        master_key:  keys.master_key,
        mac_key:     keys.mac_key,
        mask_ver:    0,
        client_id:   rand::random(),
    };
    let mut client = Client::new(cfg);

    // ---- 4. Lifecycle callback ----
    let authenticated = Arc::new(AtomicBool::new(false));
    let auth_flag = authenticated.clone();
    client.on_lifecycle(Box::new(move |ev: LifecycleEvent| {
        println!("[lifecycle] {:?}", ev);
        match ev {
            LifecycleEvent::Authenticated => {
                auth_flag.store(true, Ordering::Relaxed);
                println!("[lifecycle] >>> AUTHENTICATED — sending initial subscriptions");
            }
            LifecycleEvent::ServerRestart => {
                println!("[lifecycle] >>> SERVER RESTART — market indexes invalidated, would re-subscribe");
                // В реальном клиенте: сбросить кэшированный market_idx → name mapping,
                // вызвать client.api_get_markets_indexes(), client.api_reload_order_book().
            }
            LifecycleEvent::Disconnected => {
                println!("[lifecycle] >>> FINAL DISCONNECT");
            }
            _ => {}
        }
    }));

    // ---- 5. EventDispatcher ----
    let mut dispatcher = EventDispatcher::new();

    // ---- 6. Phase 1: подключение и авторизация (до 15 сек) ----
    println!("[phase 1] connecting...");
    let phase_start = Instant::now();
    let auth_flag2 = authenticated.clone();
    client.run(Duration::from_secs(15), Box::new(move |cmd, payload| {
        for ev in dispatcher.dispatch(cmd, payload, now_ms_for_dispatch()) {
            // На phase 1 печатаем только Order events (не спамим Ping/etc).
            if let Event::Order(oe) = &ev {
                println!("[phase 1] order event: {:?}", oe.variant_name());
            }
        }
        if auth_flag2.load(Ordering::Relaxed) && phase_start.elapsed() > Duration::from_secs(2) {
            // Авторизовались + дали 2 сек на serv-init — можно завершать phase.
            // (run() сам выйдет через duration, это просто marker)
        }
    }));

    if !authenticated.load(Ordering::Relaxed) {
        eprintln!("[phase 1] FAILED: not authenticated after 15s. Check key/host.");
        std::process::exit(1);
    }

    // ---- 7. Phase 2: initial subscriptions (after Authenticated) ----
    println!("\n[phase 2] sending initial subscriptions...");

    // Engine API: получить список рынков (async response).
    let markets_rx = client.api_get_markets_list();

    // Trade подписки.
    let _ = client.api_subscribe_all_trades(false);

    // Settings + balance + strats — initial sync.
    client.ui_settings_request();
    client.balance_request_refresh();
    client.strat_snapshot_request();

    // UI команды.
    client.ui_mm_subscribe(true);
    /* client.ui_switch_dex("Binance"); */    // uncomment если нужно сменить DEX
    /* client.ui_strat_start_stop(true); */   // uncomment если нужно запустить все стратегии

    // Engine API: ждать ответ на api_get_markets_list (timeout 5с).
    match markets_rx.recv_timeout(Duration::from_secs(5)) {
        Ok(resp) if resp.success => {
            println!("[phase 2] got markets list ({} bytes payload)", resp.data.len());
        }
        Ok(resp) => {
            println!("[phase 2] markets list error: {}", resp.error_msg);
        }
        Err(e) => {
            println!("[phase 2] markets list timeout/error: {:?}", e);
        }
    }

    // ---- 8. Phase 3: пример торговой операции (закомментировано — не отправляем по умолчанию) ----
    println!("\n[phase 3] example trade operations (commented out, uncomment to send) ...");
    let _example_ctx = TradeCtx { uid: rand::random(), currency: 0u8, platform: 0u8 };

    // Новый ордер (запрещено на чужом сервере — uncomment только для теста).
    /*
    client.new_order(
        _example_ctx,
        "BTCUSDT",       // market
        false,           // is_short
        50_000.0,        // price
        0,               // strategy_id (0 = manual)
        0.001,           // order_size
    );
    println!("[phase 3] sent new_order BTCUSDT @ 50000 size 0.001");
    */

    // Penalty (новая команда iter-2):
    /* client.penalty(_example_ctx, "BTCUSDT"); */

    // Strat sell price update:
    /* client.strat_sell_price_update(strategy_id, 51_000.0); */

    // ---- 9. Phase 4: passive monitoring (60 сек): печатать события ----
    println!("\n[phase 4] passive monitoring for 60s — listening for events...");
    let mut dispatcher2 = EventDispatcher::new();
    let total_events = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let te2 = total_events.clone();
    client.run(Duration::from_secs(60), Box::new(move |cmd, payload| {
        for ev in dispatcher2.dispatch(cmd, payload, now_ms_for_dispatch()) {
            te2.fetch_add(1, Ordering::Relaxed);
            // Печатаем только важные события.
            match ev {
                Event::Order(oe) => println!("[event] Order::{}", oe.variant_name()),
                Event::Balance(_) => println!("[event] Balance update"),
                Event::Strat(_) => println!("[event] Strat update"),
                Event::EngineResponse(r) => {
                    println!("[event] EngineResponse method={:?} success={}", r.method, r.success);
                }
                Event::ServerLog { time: _, msg } => println!("[server log] {}", msg),
                _ => {} // Trades / OrderBook / etc — слишком частые
            }
        }
        if cmd != Command::Ping {
            // Сырой счётчик не-Ping команд.
        }
    }));

    println!("\n[done] total dispatched events: {}", total_events.load(Ordering::Relaxed));

    // ---- 10. Disconnect ----
    println!("[done] disconnecting...");
    client.disconnect();
}

// Helper: OrderEvent variant name для краткого логирования.
trait OrderEventName {
    fn variant_name(&self) -> &'static str;
}
impl OrderEventName for OrderEvent {
    fn variant_name(&self) -> &'static str {
        match self {
            OrderEvent::Created(_)            => "Created",
            OrderEvent::Updated(_)            => "Updated",
            OrderEvent::Removed(_)            => "Removed",
            OrderEvent::BulkReplaced { .. }   => "BulkReplaced",
            OrderEvent::TracePoint { .. }     => "TracePoint",
            OrderEvent::CorridorChanged(_)    => "CorridorChanged",
            OrderEvent::VStopChanged(_)       => "VStopChanged",
            OrderEvent::StopsChanged(_)       => "StopsChanged",
            OrderEvent::PanicSellChanged(_)   => "PanicSellChanged",
            OrderEvent::Snapshot              => "Snapshot",
            OrderEvent::Ignored { .. }        => "Ignored",
        }
    }
}
