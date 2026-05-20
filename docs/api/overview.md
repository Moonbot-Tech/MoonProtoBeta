# moonproto — обзор библиотеки

Rust клиент **MoonProto** wire protocol — UDP-протокол связи с торговым ботом
**MoonBot** (Delphi-сервер на VPS). Byte-exact порт `MoonProto/*.pas`.

Криптография AES-128-GCM, аутентифицированный HMAC-CRC32C MAC, replay protection
через sliding bitmap window, reliable delivery поверх UDP (Sliced+ACK256),
PMTU discovery, port rotation, опциональный extended transport mode 1/2 для
DPI bypass через [`moonext`](#extended-transport).

## Active session manager — главный принцип

**Если работа делается одинаково у всех потребителей либы — это работа либы, не app.**
`moonproto` — не пассивный transport, а **active session manager**:

- **Subscription registry** — `subscribe_orderbook(name, kind)` запоминается и
  автоматически переотправляется после любого hard-reconnect.
- **Auto-refetch markets indexes** при server restart + 12с timeout protection.
- **EventDispatcher блокирует** TradesStream/OrderBook парсинг пока
  `MarketsState.indexes_synchronized = false`.
- **Auto-request OrderBookFull** при corruption (dedup).
- **Periodic trades tick** каждые ~100мс — gap recovery без участия app.
- **NTP self-managed** — `Client::new` сам spawn'ит NTP-thread (если задан `ntp_host`).
- **Per-Client ServerTimeDelta** — auto-applied к Orders state (`Arc<AtomicU64>` handle).
- **Clock-jump recovery** — при jump >60с → force_disconnect → fresh handshake.
- **Bind socket forever-retry** + `LifecycleEvent::BindFailed` уведомление.
- **OOM caps** для всех state-структур (Orders/Strats/Balances/Sliced).

**App** содержит **только UI/business решения** — что подписать, какой ордер,
какие настройки. Никаких recovery/reconnect-handling в callback'ах.

См. [client.md](client.md) и [lifecycle.md](lifecycle.md) для деталей.

## Архитектура слоёв

```
┌─────────────────────────────────────────────────────────────┐
│                     Ваше приложение                          │
│         (UI, бизнес-логика, потребитель state'ов)            │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                       moonproto                              │
│  ┌──────────────────────────────────────────────────────┐    │
│  │  High-level API                                       │    │
│  │  - Client::run_with_dispatcher (главный entry-point)  │    │
│  │  - run_init_sequence (BaseCheck → AuthCheck → ...)    │    │
│  │  - EventDispatcher (auto-apply 7 state'ов + actions)  │    │
│  │  - LifecycleEvent callbacks                           │    │
│  │  - ClientSender (thread-safe subscribe)               │    │
│  │  - 29 Engine API wrappers (api_get_markets_list, ...) │    │
│  │  - 18 Trade action wrappers (cancel_order, ...)       │    │
│  │  - ApiPending registry (async Receiver)               │    │
│  │  - CandlesAggregator (chunked candles)                │    │
│  └──────────────────────────────────────────────────────┘    │
│  ┌──────────────────────────────────────────────────────┐    │
│  │  Sync state models (state::*)                         │    │
│  │  Orders, OrderBooks, TradesState, BalancesState,      │    │
│  │  StratsState, SettingsState, MarketsState             │    │
│  └──────────────────────────────────────────────────────┘    │
│  ┌──────────────────────────────────────────────────────┐    │
│  │  Wire-парсеры/билдеры (commands::*)                   │    │
│  │  trade, balance, order_book, trades_stream, strat,    │    │
│  │  arb, ui, market, engine_api, engine_request,         │    │
│  │  strategy_serializer, candles, registry               │    │
│  └──────────────────────────────────────────────────────┘    │
│  ┌──────────────────────────────────────────────────────┐    │
│  │  Client transport + crypto                            │    │
│  │  Client (UDP, handshake, retry, slicing, UKey dedup)  │    │
│  │  Crypto (AES-128-GCM, SHAKE-128, HMAC-CRC32C)         │    │
│  │  Compression (SynLZ)                                  │    │
│  │  Protocol (handshake, slicing, slider, crypted)       │    │
│  │  NTP background sync thread (self-managed)            │    │
│  └──────────────────────────────────────────────────────┘    │
└──────────────────────────┬──────────────────────────────────┘
                           │ UDP + MoonProto wire format
                           ▼
                  ┌─────────────────────┐
                  │   MoonBot Delphi    │
                  │   (VPS server)       │
                  └─────────────────────┘
```

## Каналы команд

| Канал | Что | Direction | Docs |
|---|---|---|---|
| Order | Торговые ордера (30 подкоманд) | both | [orders.md](orders.md) |
| OrderBook | Стаканы биржи | S→C | [order_books.md](order_books.md) |
| TradesStream | Трейды биржи + Resend | S→C | [trades.md](trades.md) |
| Balance | Балансы аккаунта/маркетов | S→C | [balances.md](balances.md) |
| Strat | Стратегии (snapshot + дельты) | both | [strats.md](strats.md) |
| Arb | Arbitrage prices | S→C | [arb.md](arb.md) |
| UI | UI настройки (14 подкоманд) | both | [ui.md](ui.md) |
| Market | Engine API ответы маркетов | S→C | [markets.md](markets.md) |
| Engine API | RPC requests/responses | both | [engine_api.md](engine_api.md) |
| StrategySerializer | RTTI bin-формат стратегий | — | (внутри strats.md) |
| Candles | Исторические свечи | S→C | [candles.md](candles.md) |

## High-level API

| Документ | Что |
|---|---|
| [client.md](client.md) | `Client` lifecycle, `run_with_dispatcher`, NTP, subscribe API |
| [events.md](events.md) | `EventDispatcher` — auto-apply state + типизированные Event |
| [lifecycle.md](lifecycle.md) | `LifecycleEvent` — Connecting/Connected{fresh}/Reconnecting/SendBacklogCritical/BindFailed/ServerRestart/Disconnected |
| [trade_actions.md](trade_actions.md) | 18 trade wrappers с UKey dedup |
| [candles.md](candles.md) | DeepPrice + chunked aggregator |
| [multi_server.md](multi_server.md) | Подключение к нескольким серверам одновременно |

## Quick start

```rust
use std::time::Duration;
use moonproto::client::{Client, ClientConfig, LifecycleEvent, RefreshConfig, InitConfig, run_init_sequence};
use moonproto::events::{EventDispatcher, Event};
use moonproto::key_import;
use moonproto::state::{OrderBookKind, OrderEvent};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Импорт ключа из base64-экспорта MoonBot (Settings → Export Key).
    let keys = key_import::import_key("v3oshQy/OLZSjsCkpZIOuy4y7aWoD7U12kIXJSx7h8cBKiRjEVPSrBB8WVO7yCjC...")
        .ok_or("invalid key")?;

    // 2. Конфиг клиента. Liба сама spawn'ит NTP thread (если ntp_host=Some).
    // ClientConfig::new устанавливает production-defaults: mask_ver=0, client_id=random,
    // ntp_host=Some("pool.ntp.org"), refresh=RefreshConfig::default() (UpdateMarketsList /60с).
    // Builder methods для overrides: .with_transport_mode(1), .without_ntp(), и т.д.
    let cfg = ClientConfig::new("207.148.91.186", 3000, keys.master_key, keys.mac_key);
    let mut client = Client::new(cfg);

    // 3. Lifecycle callback (опционально) — UI индикатор.
    client.on_lifecycle(Box::new(|ev: LifecycleEvent| {
        match ev {
            LifecycleEvent::Connecting                       => println!("→ connecting"),
            LifecycleEvent::Connected { fresh: true }        => println!("→ connected (first time)"),
            LifecycleEvent::Connected { fresh: false }       => println!("→ reconnected"),
            LifecycleEvent::Reconnecting                     => println!("→ reconnecting"),
            LifecycleEvent::ServerRestart                    => println!("→ server restarted (liба сама восстановит state)"),
            LifecycleEvent::SendBacklogCritical { cmd, u_key_uid } =>
                eprintln!("⚠ critical: pending command dropped (cmd={cmd}, uid={u_key_uid})"),
            LifecycleEvent::BindFailed { consecutive_failures } =>
                eprintln!("⚠ cannot bind UDP socket ({} consecutive failures)", consecutive_failures),
            LifecycleEvent::Disconnected                     => println!("→ disconnected"),
        }
    }));

    let mut dispatcher = EventDispatcher::new();

    // 4. Phase 1: short run для handshake (~3с до Connected{fresh:true}).
    client.run_with_dispatcher(Duration::from_secs(3), &mut dispatcher, Box::new(|_| {}));

    // 5. Phase 2: init sequence (BaseCheck → AuthCheck → GetMarketsList → подписки).
    let init = InitConfig {
        base_check: true,
        auth_check: true,
        fetch_markets: true,
        fetch_balance: false,
        subscribe_trades: Some(false),                       // false = без MM ордеров
        subscribe_orderbooks: vec![
            ("BTCUSDT".to_string(), OrderBookKind::Futures),
        ],
        ..Default::default()
    };
    let _result = run_init_sequence(&mut client, &mut dispatcher, init)?;

    // 6. Phase 3: long-running stream — типизированные события автоматически.
    client.run_with_dispatcher(Duration::from_secs(3600), &mut dispatcher, Box::new(|ev| {
        match ev {
            Event::Order(OrderEvent::Created(uid)) => println!("new order {uid}"),
            Event::OrderBook(_)  => { /* redraw orderbook */ }
            Event::Trade(_)      => { /* update chart */ }
            Event::ServerLog { msg, .. } => eprintln!("[srv] {msg}"),
            _ => {}
        }
    }));

    Ok(())
}
```

Полный рабочий пример — [`examples/multi_client_test.rs`](https://github.com/anthropics/moonkernel/blob/main/moonproto/examples/multi_client_test.rs) и [`examples/trading_flow.rs`](https://github.com/anthropics/moonkernel/blob/main/moonproto/examples/trading_flow.rs).

## Низкоуровневый вариант (без EventDispatcher)

Для случаев когда нужен **сырой** канал без auto-apply state:

```rust
use moonproto::client::Client;
use moonproto::protocol::Command;
use std::time::Duration;

let mut client = Client::new(cfg);
client.run(Duration::from_secs(60), Box::new(|cmd: Command, payload: &[u8]| {
    println!("got {:?}: {} bytes", cmd, payload.len());
    // Сам parse'ишь через commands::* parsers.
}));
```

Применимо для специализированных задач (трейд-логгер, отладка wire-format).
В большинстве случаев `run_with_dispatcher` удобнее — он же даёт active-library auto-actions.

## Thread-safe subscribe (UI thread)

```rust
use moonproto::client::Client;
use moonproto::state::OrderBookKind;
use std::thread;

let mut client = Client::new(cfg);
let sender = client.sender();    // ClientSender — clone'абельный handle

// UI thread:
thread::spawn(move || {
    sender.subscribe_orderbook("DOGEUSDT", OrderBookKind::Futures);
    sender.subscribe_all_trades(true);
});

// Main thread:
client.run_with_dispatcher(Duration::from_secs(3600), &mut dispatcher, Box::new(|_| {}));
```

`ClientSender::subscribe_*` fire-and-forget. Для обратной связи — `try_subscribe_*` варианты
возвращают `Result<(), SubscribeError>`. Подробнее — [client.md → thread-safe subscribe](client.md#thread-safe-subscribe-api-f4).

## Что **не** входит в либу

- **Бизнес-логика трейдинга**: worker'ы, расчёт сигналов, ML — задача потребителя.
- **UI рендеринг**: чарт, стакан, таблицы — это **moonkernel-terminal** (отдельный Tauri repo, ещё не начат).
- **TStrategy.* setters с побочными эффектами**: state хранит wire-данные как observer-snapshot.

## Extended transport

`moonext` — опциональная **closed-source** библиотека (~100 строк, .dll/.so/.dylib),
добавляющая extended transport **mode 1** и **2** (DPI bypass для сложных сетей).

- **V0 (open-source) работает standalone** — все сервера MoonBot принимают V0 как fallback.
- **Mode 1/2** — для случаев когда V0 фильтруется DPI/firewall.
  Включается `cfg.mask_ver = 1` или `2` + наличие `moonext.dll/.so/.dylib` рядом с exe.
- Без `moonext` бинарника при `mask_ver != 0` клиент откатывается на V0.

Скачать → Releases в GitHub репо проекта.

## Cargo features

Нет feature flags — все компоненты обязательны и собираются всегда.

## Версии

- **Rust**: edition 2021, stable 1.95+.
- **Зависимости**: `aes-gcm`, `sha3`, `flate2`, `crc32c`, `rand`, `socket2`, `libloading`, `log`, `base64`.

## См. также

- [client.md](client.md) — детали `Client`, NTP, subscribe API.
- [events.md](events.md) — `EventDispatcher`, `Event` enum.
- [lifecycle.md](lifecycle.md) — все 7 `LifecycleEvent` variants.
- [multi_server.md](multi_server.md) — несколько `Client` в одном процессе.
- [engine_api.md](engine_api.md) — RPC к Engine + `ServerInfo`.
- [trade_actions.md](trade_actions.md) — 18 trade wrappers.
- [candles.md](candles.md) — chunked candles aggregator.
- DEVIATION.md (в корне репозитория) — реестр отклонений от Delphi.
- MAPPING.md (в `moonproto/` и `moonproto-transport/`) — построчная сверка с Delphi.
