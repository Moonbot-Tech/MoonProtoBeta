//! Полный пример торгового flow: подключение → подписка → ордер → cancel.
//!
//! Показывает **active library API** через `Client::run_with_dispatcher` —
//! рекомендуемый главный entry-point. Либа сама делает:
//! - transport reconnect/handshake до `Fine`;
//! - one-time init/API-driven подписки и `GetMarketsIndexes`;
//! - post-init reconnect restore без повторного Init;
//! - блокировку indexed `TradesStream`/`OrderBook` пакетов до синхронизации indexes;
//! - auto-send `emk_RequestOrderBookFull` при corrupted orderbook cache /
//!   missing Full snapshot — потребитель НЕ должен это вызывать сам
//! - Delphi-style trades resend tail-check после valid TradesStream packets
//! - timeout protection для init/API indexes request marker (UDP-loss recovery, см.
//!   `check_indexes_fetch_timeout`)
//! - ServerTimeDelta application через глобальный atomic
//!
//! App запускает init/API шаги один раз после первого connect. После reconnect
//! либа сама восстанавливает сохранённый intent.
//!
//! Запуск:
//!   cargo run --example trading_flow --release -- "<key_base64>" "host:port"
//!
//! ВНИМАНИЕ: пример отправляет тестовые команды на сервер. Используй тестовый
//! ключ / тестовый сервер. Никаких реальных торговых операций по умолчанию
//! (комментарий `/* uncomment to send */`).

use std::env;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;

use moonproto::client::{set_ntp_offset, Client, ClientConfig, LifecycleEvent};
use moonproto::events::{Event, EventDispatcher};
use moonproto::key_import;
use moonproto::ntp;
use moonproto::state::OrderEvent;

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
        (
            parts[0].to_string(),
            parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(3000u16),
        )
    } else {
        ("127.0.0.1".to_string(), 3000u16)
    };

    // ---- 1. Импорт ключа ----
    let keys = key_import::import_key(key_b64).expect("invalid key");
    println!("[setup] key imported");

    // ---- 2. NTP sync ----
    let ntp_result = ntp::get_best_ntp("pool.ntp.org", 4);
    if ntp_result.synced {
        set_ntp_offset(ntp_result.time_offset);
        println!(
            "[setup] NTP offset={:.1}ms rtt={}ms",
            ntp_result.time_offset * 1000.0,
            ntp_result.round_trip_ms
        );
    } else {
        println!("[setup] NTP failed, continuing with system clock");
    }

    // ---- 3. Client config (NTP default; background Engine API starts after Init) ----
    let cfg = ClientConfig::new(ip, port, keys.master_key, keys.mac_key);
    let mut client = Client::new(cfg);

    // ---- 4. Lifecycle callback ----
    let authenticated = Arc::new(AtomicBool::new(false));
    let auth_flag = authenticated.clone();
    client.on_lifecycle(Box::new(move |ev: LifecycleEvent| {
        println!("[lifecycle] {:?}", ev);
        match ev {
            LifecycleEvent::Connected { fresh } => {
                auth_flag.store(true, Ordering::Relaxed);
                if fresh {
                    println!("[lifecycle] >>> FIRST CONNECTED — app may now send init commands");
                } else {
                    println!("[lifecycle] >>> RE-CONNECTED — library restores post-init intent");
                }
            }
            LifecycleEvent::ServerRestart => {
                println!("[lifecycle] >>> SERVER RESTART — restore is handled after reconnect");
            }
            LifecycleEvent::Disconnected => {
                println!("[lifecycle] >>> FINAL DISCONNECT");
            }
            _ => {}
        }
    }));

    // ---- 5. EventDispatcher — переиспользуется между всеми phase'ами ----
    // Один и тот же dispatcher держит state (markets, orders, ...) между фазами.
    let mut dispatcher = EventDispatcher::new();

    // ---- 6. Phase 1: подключение и авторизация (до 15 сек) ----
    println!("[phase 1] connecting...");
    let phase1_orders = Arc::new(AtomicU64::new(0));
    let p1_orders = phase1_orders.clone();
    client.run_with_dispatcher(
        Duration::from_secs(15),
        &mut dispatcher,
        Box::new(move |ev: &Event| {
            // На phase 1 печатаем только Order events (Ping/etc спам не нужен).
            if let Event::Order(oe) = ev {
                p1_orders.fetch_add(1, Ordering::Relaxed);
                println!("[phase 1] order event: {:?}", oe.variant_name());
            }
        }),
    );

    if !authenticated.load(Ordering::Relaxed) {
        eprintln!("[phase 1] FAILED: not authenticated after 15s. Check key/host.");
        std::process::exit(1);
    }
    println!(
        "[phase 1] OK, order events seen: {}",
        phase1_orders.load(Ordering::Relaxed)
    );

    // ---- 7. Phase 2: initial subscriptions ----
    //
    // Все методы ниже требуют `&mut Client`. После выхода из run_with_dispatcher
    // (phase 1 завершилась через 15с) borrow checker разрешает их вызвать.
    // Если бы потребитель хотел отправлять команды ВО ВРЕМЯ run_with_dispatcher
    // (например по user action из UI thread'а) — это уже Stage 3 (см. HANDOFF
    // Roadmap, "subscribe_* thread-safe API"). На текущий момент паттерн:
    // run-pause-issue-resume.
    println!("\n[phase 2] sending initial subscriptions...");

    // Active library API: `subscribe_all_trades` запоминается в registry; после Init
    // reconnect восстановит этот intent без повторного Init.
    client.subscribe_all_trades(false);

    // Engine API: получить список рынков (async response). Receiver можно
    // дождаться в этом же потоке между phase 2 и phase 4.
    let markets_rx = client.api_get_markets_list();

    // Settings + balance — explicit UI refreshes после Connected{fresh:true}.
    // Strategy snapshot sync is part of Delphi-compatible post-init resync:
    // the library sends local `TStratSnapshot` automatically after InitDone.
    client.ui_settings_request();
    client.balance_request_refresh();

    // UI команды.
    client.ui_mm_subscribe(true);
    /* client.ui_switch_dex("Binance"); */
    // uncomment если нужно сменить DEX
    /* client.ui_strat_start_stop(true); */   // uncomment если нужно запустить все стратегии

    // Подписка на orderbook через registry-aware API. Resolve `market_name →
    // market_idx` делает сервер — поэтому можно вызвать ДО получения
    // `emk_GetMarketsList`.
    {
        client.subscribe_orderbook("BTCUSDT");
    }

    // ---- 8. Phase 3: ждать ответ на api_get_markets_list ----
    //
    // Receiver `markets_rx` остался валидным; ответы поступают через
    // EventDispatcher → pending-response registry → sender. На этой фазе чтобы их доставить
    // нужен короткий run_with_dispatcher (5с timeout).
    println!("\n[phase 3] waiting for markets list response...");
    client.run_with_dispatcher(
        Duration::from_secs(5),
        &mut dispatcher,
        Box::new(|_ev: &Event| { /* silent — ждём только api response */ }),
    );
    match markets_rx.try_recv() {
        Ok(resp) if resp.success => {
            println!(
                "[phase 3] got markets list ({} bytes payload)",
                resp.data.len()
            );
        }
        Ok(resp) => {
            println!("[phase 3] markets list error: {}", resp.error_msg);
        }
        Err(e) => {
            println!("[phase 3] markets list timeout/disconnected: {:?}", e);
        }
    }

    // ---- 9. Phase 4: пример торговой операции (закомментировано) ----
    println!("\n[phase 4] example trade operations (commented out, uncomment to send) ...");
    let _example_ctx = client.random_trade_ctx();
    if let Err(err) = &_example_ctx {
        println!("[phase 4] trade route unavailable: {err}");
    }

    // Новый ордер (запрещено на чужом сервере — uncomment только для теста).
    /*
    let ctx = client
        .random_trade_ctx()
        .expect("run BaseCheck before sending market trade commands");
    client.new_order(
        ctx,
        "BTCUSDT",       // market
        false,           // is_short
        50_000.0,        // price
        0,               // strategy_id (0 = manual)
        0.001,           // order_size
    );
    println!("[phase 4] sent new_order BTCUSDT @ 50000 size 0.001");
    */

    /* client.penalty(_example_ctx, "BTCUSDT"); */
    /* client.strat_sell_price_update(strategy_id, 51_000.0); */

    // ---- 10. Phase 5: passive monitoring (60 сек): печатать события ----
    //
    // В этом цикле:
    //   - dispatch_into_active авто-отправит emk_RequestOrderBookFull при
    //     corrupted orderbook cache
    //   - trades tail-check автоматически восстановит потерянные TradesStream
    //     пакеты через emk_TradesResend после valid trades packets
    //   - check_indexes_fetch_timeout раз за тик защитит от UDP-потери ответа
    //     emk_GetMarketsIndexes
    println!("\n[phase 5] passive monitoring for 60s — active library в действии...");
    let total_events = Arc::new(AtomicU64::new(0));
    let te = total_events.clone();
    client.run_with_dispatcher(
        Duration::from_secs(60),
        &mut dispatcher,
        Box::new(move |ev: &Event| {
            te.fetch_add(1, Ordering::Relaxed);
            // Печатаем только семантически важные события — Trades/OrderBook
            // слишком частые для console output.
            match ev {
                Event::Order(oe) => println!("[event] Order::{}", oe.variant_name()),
                Event::Balance(_) => println!("[event] Balance update"),
                Event::Strat(_) => println!("[event] Strat update"),
                Event::EngineResponse(r) => {
                    println!(
                        "[event] EngineResponse method={:?} success={}",
                        r.method, r.success
                    );
                }
                Event::ServerLog { time: _, msg } => println!("[server log] {}", msg),
                _ => {} // Trades / OrderBook / Ping — слишком частые
            }
        }),
    );

    println!(
        "\n[done] total dispatched events: {}",
        total_events.load(Ordering::Relaxed)
    );

    // ---- 11. Disconnect ----
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
            OrderEvent::Created(_) => "Created",
            OrderEvent::Updated(_) => "Updated",
            OrderEvent::Removed(_) => "Removed",
            OrderEvent::BulkReplaced { .. } => "BulkReplaced",
            OrderEvent::TracePoint { .. } => "TracePoint",
            OrderEvent::CorridorChanged(_) => "CorridorChanged",
            OrderEvent::VStopChanged(_) => "VStopChanged",
            OrderEvent::StopsChanged(_) => "StopsChanged",
            OrderEvent::Snapshot => "Snapshot",
            OrderEvent::Ignored { .. } => "Ignored",
        }
    }
}
