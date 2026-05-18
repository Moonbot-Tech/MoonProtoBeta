# moonproto — обзор библиотеки

Rust client library для **MoonProto** wire protocol. Byte-exact порт Delphi `MoonProto/*.pas`. Используется в `MoonKernel` (кросс-платформенный трейдинг-терминал к MoonBot Delphi-серверу).

## Архитектура

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
│  │  Stage 3: High-level API                              │    │
│  │  - EventDispatcher (auto-apply 7 state'ов)            │    │
│  │  - LifecycleEvent callbacks                           │    │
│  │  - 29 Engine API wrappers (api_get_markets_list, ...) │    │
│  │  - 17 Trade action wrappers (cancel_order, ...)       │    │
│  │  - ApiPending registry (async Receiver)               │    │
│  │  - CandlesAggregator (chunked candles)                │    │
│  └──────────────────────────────────────────────────────┘    │
│  ┌──────────────────────────────────────────────────────┐    │
│  │  Stage 2: Sync state models (state::*)                │    │
│  │  Orders, OrderBooks, TradesState, BalancesState,      │    │
│  │  StratsState, SettingsState, MarketsState             │    │
│  └──────────────────────────────────────────────────────┘    │
│  ┌──────────────────────────────────────────────────────┐    │
│  │  Stage 2: Wire-парсеры/билдеры (commands::*)          │    │
│  │  trade, balance, order_book, trades_stream, strat,    │    │
│  │  arb, ui, market, engine_api, engine_request,         │    │
│  │  strategy_serializer, candles, registry               │    │
│  └──────────────────────────────────────────────────────┘    │
│  ┌──────────────────────────────────────────────────────┐    │
│  │  Stage 1: Client transport                            │    │
│  │  Client (UDP, handshake, retry, slicing, UKey dedup)  │    │
│  │  Crypto (AES-128-GCM, SHAKE-128, HMAC-CRC32C)         │    │
│  │  Compression (SynLZ)                                  │    │
│  │  Protocol (handshake, slicing, slider, crypted)       │    │
│  │  NTP background sync thread                           │    │
│  └──────────────────────────────────────────────────────┘    │
└──────────────────────────┬──────────────────────────────────┘
                           │ UDP + MoonProto wire format
                           ▼
                  ┌─────────────────────┐
                  │   MoonBot Delphi    │
                  │   (VPS server)       │
                  └─────────────────────┘
```

## Каналы (Stage 2)

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

## High-level API (Stage 3)

| Документ | Что |
|---|---|
| [client.md](client.md) | Client lifecycle, send/run, observability |
| [events.md](events.md) | EventDispatcher — auto-apply state + типизированные Event |
| [lifecycle.md](lifecycle.md) | LifecycleEvent { Connecting / Authenticated / Reconnecting / ServerRestart / Disconnected } |
| [trade_actions.md](trade_actions.md) | 17 trade wrappers с UKey dedup |
| [candles.md](candles.md) | DeepPrice + CandlesAggregator (chunked) |

## Что предоставляет либа

### Stage 1: Wire protocol + crypto + handshake
Полный byte-exact порт. Тестировано на live сервере `207.148.91.186:3000`.

### Stage 2: Wire-парсеры и sync state
Каждая MoonProto-команда имеет:
- Парсер: `commands::<ch>::<Cmd>::parse(payload) -> Option<Self>`.
- Builder: `commands::<ch>::build_<cmd>(uid, params) -> Vec<u8>`.

Для каждого канала есть `state::<X>State` с автоматическим применением входящих команд.

### Stage 3: High-level Client API
- **EventDispatcher** — один callback вместо ручной маршрутизации 9 каналов.
- **Trade wrappers** — `client.cancel_order(ctx, ...)` вместо `build_*` + `send_cmd` + UKey ручной.
- **Engine API wrappers** — `client.api_get_markets_list()` вместо ручной отправки + регистрация UID.
- **Lifecycle callbacks** — типизированные события Connecting/Authenticated/Reconnecting/ServerRestart/Disconnected.
- **CandlesAggregator** — собирает chunked candles из множественных response'ов.

## Quick start

### Минимальный вариант (raw `on_data`)

```rust
use moonproto::*;
use moonproto::commands::*;
use moonproto::state::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let key = key_import::import_master_key("v3oshQy/...")?;
    let cfg = ClientConfig {
        server_ip: "207.148.91.186".to_string(),
        server_port: 3000,
        master_key: key.master_key,
        mac_key: key.mac_key,
        mask_ver: 0,
        client_id: rand::random(),
    };

    let mut client = Client::new(cfg);
    client.run(Duration::from_secs(60), Box::new(|cmd, payload| {
        println!("got {:?}: {} bytes", cmd, payload.len());
    }));
    Ok(())
}
```

### Рекомендуемый вариант (EventDispatcher + lifecycle + auto-state)

```rust
use moonproto::*;
use moonproto::events::{EventDispatcher, Event};
use moonproto::client::LifecycleEvent;
use moonproto::ntp;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let key = key_import::import_master_key("v3oshQy/...")?;
    let cfg = /* ClientConfig as above */;
    let mut client = Client::new(cfg);

    // 1. NTP background sync (daemon thread)
    ntp::spawn_sync_thread("pool.ntp.org".into(), client::set_ntp_offset);

    // 2. Lifecycle UI status
    client.on_lifecycle(Box::new(|ev| match ev {
        LifecycleEvent::Connecting     => println!("→ connecting"),
        LifecycleEvent::Authenticated  => println!("→ ready"),
        LifecycleEvent::Reconnecting   => println!("→ reconnecting"),
        LifecycleEvent::ServerRestart  => println!("→ server restarted, cache cleared"),
        LifecycleEvent::Disconnected   => println!("→ disconnected"),
    }));

    // 3. Event-based state management
    let mut dispatcher = EventDispatcher::new();
    client.run(Duration::from_secs(60), Box::new(move |cmd, payload| {
        let now_ms = chrono::Utc::now().timestamp_millis();
        for ev in dispatcher.dispatch(cmd, payload, now_ms) {
            match ev {
                Event::Order(o) => println!("order event: {:?}", o),
                Event::OrderBook(_) => { /* redraw */ }
                Event::Trades(_) => { /* update chart */ }
                Event::ServerLog { msg, .. } => eprintln!("[srv] {}", msg),
                _ => {}
            }
        }
    }));

    Ok(())
}
```

### Trade actions

```rust
use moonproto::commands::trade::{TradeCtx, OrderType, OrderWorkerStatus};

let ctx = TradeCtx::new(order_uid);  // ctx.uid = TaskID ордера для UKey dedup
client.replace_order(ctx, "BTCUSDT", epoch, status, OrderType::Sell, 50100.0);
client.cancel_order(ctx, "BTCUSDT", epoch, status);
```

### Engine API async

```rust
let rx = client.api_get_markets_list();
let resp = rx.recv_timeout(Duration::from_secs(10))?;
// resp.data — already DEFLATE-decompressed
let list = parse_markets_list_response(&resp.data, 2)?;
```

## Не входит в либу

- **Бизнес-логика трейдинга**: worker'ы, расчёт сигналов, ML — задача потребителя.
- **UI рендеринг**: чарт, стакан, таблицы — это **moonkernel-terminal** (отдельный Tauri repo, ещё не начат).
- **TStrategy.* setters с побочными эффектами**: state хранит wire-данные как observer-snapshot (DEVIATION #5).
- **DPI bypass**: реализуется в опциональной closed-source библиотеке `moonext` (mode 1/2). V0 (open) работает standalone.

## Cargo features

Нет feature flags — все компоненты обязательны.

## Версии

- Rust: edition 2021, stable 1.95+ (тестировано на portable Rust 1.95.0).
- Зависимости: `aes-gcm`, `sha3`, `flate2`, `crc32c`, `rand`, `libloading`.

## См. также

- [client.md](client.md) — детали Client transport.
- [engine_api.md](engine_api.md) — RPC к Engine на сервере.
- [events.md](events.md) — EventDispatcher.
- [lifecycle.md](lifecycle.md) — Lifecycle callbacks.
- [trade_actions.md](trade_actions.md) — Trade wrappers.
- [candles.md](candles.md) — Candles aggregator.
- DEVIATION.md (в корне репозитория) — 18 архитектурных отклонений.
- MAPPING.md (в moonproto/ и moonproto-transport/) — построчная сверка с Delphi.
