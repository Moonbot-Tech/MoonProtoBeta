//! Полный пример торгового flow: подключение → подписка → ордер → cancel.
//!
//! Показывает **active library API** через `Client::run_with_dispatcher` —
//! рекомендуемый главный entry-point. Liба сама делает:
//! - auto-replay подписок при reconnect / ServerToken change (через `SubscriptionRegistry`)
//! - auto-fetch markets indexes при `PeerAppToken change` + блокировка
//!   `TradesStream`/`OrderBook` пакетов до завершения синхронизации
//! - auto-send `emk_RequestOrderBookFull` при `RequestFullNeeded` (corruption /
//!   missing Full snapshot) — потребитель НЕ должен это вызывать сам
//! - periodic `trades.tick()` каждые ~100мс для resend missing TradesStream пакетов
//! - timeout protection для auto-fetch indexes (UDP-loss recovery, см.
//!   `check_indexes_fetch_timeout`)
//! - ServerTimeDelta application через глобальный atomic
//!
//! App ловит lifecycle events только для UI индикатора + дёргает trade-команды
//! по user actions. Никаких ручных recovery шагов от app не требуется.
//!
//! Запуск:
//!   cargo run --example trading_flow --release -- "<key_base64>" "host:port"
//!
//! ВНИМАНИЕ: пример отправляет тестовые команды на сервер. Используй тестовый
//! ключ / тестовый сервер. Никаких реальных торговых операций по умолчанию
//! (комментарий `/* uncomment to send */`).

use std::env;
use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}};
use std::time::Duration;

use moonproto::client::{Client, ClientConfig, LifecycleEvent, set_ntp_offset};
use moonproto::events::{EventDispatcher, Event};
use moonproto::key_import;
use moonproto::ntp;
use moonproto::state::OrderEvent;
use moonproto::commands::trade::TradeCtx;

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
        println!("[setup] NTP offset={:.1}ms rtt={}ms",
                 ntp_result.time_offset * 1000.0, ntp_result.round_trip_ms);
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
        // Active library F8: либа сама spawn'ит NTP-sync thread в Client::new
        // и завершает в Drop. Без отдельного ntp::spawn_sync_thread снаружи.
        ntp_host:    Some("pool.ntp.org".to_string()),
        // F6/F7: периодический pull свежих prices (дефолт каждые 60с).
        refresh:     moonproto::client::RefreshConfig::default(),
    };
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
                    println!("[lifecycle] >>> RE-CONNECTED — registry auto-replayed subscriptions, ничего делать не надо");
                }
            }
            LifecycleEvent::ServerRestart => {
                println!("[lifecycle] >>> SERVER RESTART — либа сама re-fetch'нет indexes и replay'нет subscriptions");
                // App ничего не должен делать — это info-event для UI индикатора.
            }
            LifecycleEvent::Disconnected => {
                println!("[lifecycle] >>> FINAL DISCONNECT");
            }
            _ => {}
        }
    }));

    // ---- 5. EventDispatcher — переиспользуется между всеми phase'ами ----
    // Один и тот же dispatcher держит state (markets, orders, ...) и подхватывает
    // auto-action'ы либы через `run_with_dispatcher`.
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
    println!("[phase 1] OK, order events seen: {}", phase1_orders.load(Ordering::Relaxed));

    // ---- 7. Phase 2: initial subscriptions ----
    //
    // Все методы ниже требуют `&mut Client`. После выхода из run_with_dispatcher
    // (phase 1 завершилась через 15с) borrow checker разрешает их вызвать.
    // Если бы потребитель хотел отправлять команды ВО ВРЕМЯ run_with_dispatcher
    // (например по user action из UI thread'а) — это уже Stage 3 (см. HANDOFF
    // Roadmap, "subscribe_* thread-safe API"). На текущий момент паттерн:
    // run-pause-issue-resume.
    println!("\n[phase 2] sending initial subscriptions...");

    // Active library API: `subscribe_all_trades` запоминается в registry — при
    // следующем reconnect / ServerToken change либа auto-replay'ит. App ничего
    // не делает.
    client.subscribe_all_trades(false);

    // Engine API: получить список рынков (async response). Receiver можно
    // дождаться в этом же потоке между phase 2 и phase 4.
    let markets_rx = client.api_get_markets_list();

    // Settings + balance + strats — initial sync (после Connected{fresh:true}).
    client.ui_settings_request();
    client.balance_request_refresh();
    client.strat_snapshot_request();

    // UI команды.
    client.ui_mm_subscribe(true);
    /* client.ui_switch_dex("Binance"); */    // uncomment если нужно сменить DEX
    /* client.ui_strat_start_stop(true); */   // uncomment если нужно запустить все стратегии

    // Подписка на orderbook через registry-aware API. Resolve `market_name →
    // market_idx` делает сервер — поэтому можно вызвать ДО получения
    // `emk_GetMarketsList`.
    {
        use moonproto::state::OrderBookKind;
        client.subscribe_orderbook("BTCUSDT", OrderBookKind::Futures);
    }

    // ---- 8. Phase 3: ждать ответ на api_get_markets_list ----
    //
    // Receiver `markets_rx` остался валидным; ответы поступают через
    // EventDispatcher → ApiPending → sender. На этой фазе чтобы их доставить
    // нужен короткий run_with_dispatcher (5с timeout).
    println!("\n[phase 3] waiting for markets list response...");
    client.run_with_dispatcher(
        Duration::from_secs(5),
        &mut dispatcher,
        Box::new(|_ev: &Event| { /* silent — ждём только api response */ }),
    );
    match markets_rx.try_recv() {
        Ok(resp) if resp.success => {
            println!("[phase 3] got markets list ({} bytes payload)", resp.data.len());
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
    println!("[phase 4] sent new_order BTCUSDT @ 50000 size 0.001");
    */

    /* client.penalty(_example_ctx, "BTCUSDT"); */
    /* client.strat_sell_price_update(strategy_id, 51_000.0); */

    // ---- 10. Phase 5: passive monitoring (60 сек): печатать события ----
    //
    // В этом цикле:
    //   - dispatch_into_active авто-отправит emk_RequestOrderBookFull при
    //     RequestFullNeeded (corruption detection)
    //   - periodic trades.tick() каждые 100мс автоматически восстановит
    //     потерянные TradesStream пакеты через emk_TradesResend
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
                    println!("[event] EngineResponse method={:?} success={}", r.method, r.success);
                }
                Event::ServerLog { time: _, msg } => println!("[server log] {}", msg),
                _ => {} // Trades / OrderBook / Ping — слишком частые
            }
        }),
    );

    println!("\n[done] total dispatched events: {}", total_events.load(Ordering::Relaxed));

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
