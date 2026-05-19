# moonproto

Rust клиент протокола MoonProto — UDP-связь с торговым ботом MoonBot
(Delphi-сервер на VPS).

## Что делает

- Подключение к MoonBot серверу через зашифрованный UDP
- Полный цикл соединения: handshake, keepalive, soft/hard reconnect, NAT-rebind recovery
- Приём рыночных данных: trades stream, order book, balances, market info
- Отправка торговых команд: новый ордер, отмена, replace, stops, panic-sell, и т.д.
- Engine API: 25+ RPC-методов (subscribe trades, request candles, set leverage, ...)
- Большие сообщения через slicing + ACK
- PMTU discovery, adaptive rate, replay protection
- Cross-platform: Windows / Linux / macOS / Android / iOS

## Quick start

```toml
[dependencies]
moonproto = { path = "../moonproto" }
```

### Подключение к серверу

```rust
use std::time::Duration;
use moonproto::client::{Client, ClientConfig, LifecycleEvent};
use moonproto::events::EventDispatcher;
use moonproto::key_import;
use moonproto::ntp;

// 1. Импорт ключа из base64-экспорта MoonBot
//    (в MoonBot UI: Settings → Export Key → копируешь base64 строку).
let keys = key_import::import_key(KEY_B64).expect("invalid key");

// 2. NTP sync — рекомендуется для корректных timestamps в ордерах.
let ntp_result = ntp::get_best_ntp("pool.ntp.org", 4);
if ntp_result.synced {
    moonproto::client::set_ntp_offset(ntp_result.time_offset);
}

// 3. Конфигурация клиента.
let cfg = ClientConfig {
    server_ip:   "127.0.0.1".to_string(),
    server_port: 3000,
    master_key:  keys.master_key,
    mac_key:     keys.mac_key,
    mask_ver:    0,                 // 0 = base, 1/2 требует moonext binary
    client_id:   rand::random(),
};
let mut client = Client::new(cfg);

// 4. Lifecycle callback (опционально).
client.on_lifecycle(Box::new(|ev: LifecycleEvent| {
    println!("[lifecycle] {:?}", ev);
}));

// 5. EventDispatcher — авто-парсит входящие в типизированные события + sync state.
let mut dispatcher = EventDispatcher::new();

// 6. Запуск (блокирует поток на duration).
client.run(Duration::from_secs(60), Box::new(move |cmd, payload| {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64;
    for event in dispatcher.dispatch(cmd, payload, now_ms) {
        // обработка типизированных событий — см. `moonproto::events::Event`
        let _ = event;
    }
}));
```

Полный рабочий пример: `examples/client_test.rs`.

### Отправка команд (после Authenticated)

```rust
// Engine API (async response через mpsc::Receiver)
let rx = client.api_get_markets_list();
// в другом месте: rx.recv_timeout(Duration::from_secs(5))

// Trade команды
client.new_order(ctx, "BTCUSDT", false, 50000.0, strategy_id, 0.001);
client.cancel_order(ctx, "BTCUSDT", epoch, status);
client.replace_order(ctx, "BTCUSDT", epoch, status, order_type, 51000.0);

// UI команды
client.ui_send_settings(&client_settings);
client.ui_mm_subscribe(true);
client.ui_switch_dex("Binance");

// Strategy команды
client.strat_snapshot_request();
client.strat_sell_price_update(strategy_id, new_price);

// Balance
client.balance_request_refresh();
```

Полный список методов — в doc comments на каждом методе `Client` (`cargo doc --open`).

## Setup / Initialization Flow

Полная последовательность для написания клиента с нуля:

1. **Получи base64-строку ключа** в MoonBot: Settings → Export Key → копируй.
2. **Импорт ключа**: `key_import::import_key(base64_str)` → `Keys { master_key, mac_key }`.
3. **NTP sync** (рекомендуется): `ntp::get_best_ntp("pool.ntp.org", 4)` → если `synced`,
   передай offset в `client::set_ntp_offset(offset)`. Без NTP timestamps в ордерах
   будут с uncorrected системным временем (расхождение часов клиент/сервер).
4. **Конфигурация**: `ClientConfig` — IP, порт, оба ключа, `mask_ver` (0 для базового
   транспорта; 1/2 если используешь DPI-bypass через `moonext.dll`/`.so`/`.dylib`).
5. **Client::new(cfg)** — конструктор. Сокет ещё не создан.
6. **Callbacks (опционально)**:
   - `client.on_lifecycle(cb)` — Connecting / Authenticated / Reconnecting / Disconnected
     / ServerRestart.
7. **Подготовь EventDispatcher** (`EventDispatcher::new()`) — он держит sync-state
   (orders, order_books, trades, balances, strats, settings, markets) и парсит
   входящие команды.
8. **`client.run(duration, on_data)`** — БЛОКИРУЕТ поток на `duration`. Из callback
   `on_data` вызывай `dispatcher.dispatch(cmd, payload, now_ms)` чтобы получить
   `Vec<Event>` — типизированные события (OrderEvent, TradesEvent, ...).
9. **На `LifecycleEvent::Authenticated`** — обычно нужно: подписаться на trades
   (`client.api_subscribe_all_trades()`), запросить статусы ордеров
   (`client.request_all_statuses(uid)`), запросить snapshot стратегий
   (`client.strat_snapshot_request()`), settings (`client.ui_settings_request()`).
10. **Завершение**: `client.disconnect()` отправит LogOff и закроет сокет.

## Gotchas

- `client.run()` **блокирует** поток на duration — для async оборачивай в
  `std::thread::spawn`.
- **NTP sync обязателен** для корректных timestamps. Без него ордера будут с
  uncorrected системным временем (расхождение видно в UI).
- **PMTU стартует с 508 байт** и растёт через probe. Первые Sliced-сообщения
  фрагментируются мелко, нормально.
- **UKey dedup**: команды с одним UniqueKey замещают друг друга в очереди отправки.
  `replace_order` 5 раз подряд → сервер увидит только последний. Полезное
  свойство для UI (drag-replace), но знай об этом.
- **`LifecycleEvent::ServerRestart`** — сервер перезагрузился, market indexes
  невалидны. Сбрось кэши, re-subscribe на order books, request fresh snapshot.
- **`LifecycleEvent::Reconnecting`** — клиент сам soft-reconnect'ится, ничего не
  делай. `Disconnected` — финальное, нужен новый `Client`.
- **Compression** автоматическая для payload > 64 байт — не управляешь вручную.
- **Order phases**: `None → BuySet → BuyDone → SellSet → SelLAlmostDone → SelLDone`.
  Terminal states: `SelLDone`, `BuyCancel`, `BuyFail`, `SellCancel`, `SellFail`.
- **Reading EngineResponse**: для большинства Engine API методов есть
  `parse_*_response(&resp.data)` в соответствующих модулях (`commands::markets`,
  `commands::candles`, ...). Pending registry автоматически dispatches response в
  `Receiver<EngineResponse>` который вернул `api_*_async` метод.
- **CandlesAggregator**: `api_request_candles_data` возвращает **chunked** response
  — несколько `EngineResponse` пакетов. Pending registry не подходит, используй
  обычный `on_data` callback + `commands::candles::CandlesAggregator::on_chunk`.

## Архитектура

```
moonproto (this crate)
├── client/         — Client struct, lifecycle, handshake, retry, NTP, lifecycle events
├── crypto/         — AES-128-GCM, SHAKE-128 key derivation
├── protocol/       — Slider (replay), SlicingReceiver (re-assembly), CryptedHeader
├── commands/       — wire-format builders/parsers для 11 каналов
├── state/          — sync-state модели (Orders, OrderBooks, Trades, Balances, ...)
├── events/         — EventDispatcher — типизированные события
├── api_pending/    — registry для async-ответов Engine API
├── compression/    — SynLZ (byte-exact с mORMot)
├── ntp/            — SNTP клиент с background thread (TryCount=4)
└── key_import/     — парсинг base64-ключа из MoonBot

depends on:
moonproto-transport — packet framing, MAC, обфускация, ext_loader для moonext
```

## Transport modes

| Mode | Описание | Требует |
|------|----------|---------|
| 0 | Base transport (xoshiro128+ обфускация + HMAC-CRC32C) | Ничего |
| 1 | Extended transport — DPI bypass mode 1 | `moonext` library |
| 2 | Extended transport — DPI bypass mode 2 | `moonext` library |

Mode определяется конфигурацией сервера. Mode 0 работает без дополнительных файлов.
Mode 1/2 требует `moonext.dll`/`.so`/`.dylib` рядом с exe (см. moonext Releases).

## Protocol overview

### Connection flow
```
Client                          Server
  |--- Hello (AES-GCM/MasterKey) ->|
  |<-- WhoAreYou (AES-GCM) --------|   server token + app token
  |--- ImFriend ----------------->|    (отправляется дважды с паузой 32ms)
  |<-- Fine ----------------------|    authenticated
  |                                |
  |<-- Ping (~1s) ----------------|    keepalive + channel quality
  |<-- Crypted/Sliced commands ---|    application data
  |--- SlicedACK ---------------->|
  |--- Crypted commands --------->|
```

### Key types

- **MasterKey** (16 b): pre-shared, handshake encryption + AAD=ClientID для GCM tag.
- **MacKey** (16 b): HMAC-CRC32C для transport integrity + xoshiro128+ seed для обфускации.
- **SubKey[true/false]** (16 b): session-derived через SHAKE-128 + 5 раундов XOR-fold.
  Direction-specific: `false` = client→server encrypted commands, `true` = server→client.

## API reference

См. `cargo doc --open` для полной документации публичного API. Ключевые модули:

- [`client::Client`] — главный entry point, lifecycle, send/receive
- [`events::EventDispatcher`] / [`events::Event`] — типизированные события
- [`commands::trade`] / [`commands::ui`] / [`commands::strat`] / ... — wire builders/parsers
- [`commands::engine_api::EngineMethod`] / [`commands::engine_api::EngineResponse`] — RPC

## Building

Rust 1.75+, без системных зависимостей.

```bash
cargo build --release
cargo test
```

## Test tools (`examples/`)

Debug / load-testing утилиты, не production:

- `client_test` — минимальный CLI: handshake + subscribe + receive trades.
- `loss_logger` — детальный лосс-логгер с опциональной симуляцией client-side drop.
  Полезен для верификации gap recovery, reconnect, slicing retry под degraded network.

```bash
# Обычный запуск:
cargo run --example loss_logger --release -- <key_b64> 127.0.0.1:3000 loss.log

# Stress test с 75% client-side packet drop:
cargo run --example loss_logger --release -- <key_b64> 127.0.0.1:3000 loss.log 75
```

### `client::set_err_emu(percent)` — **TEST USE ONLY**

Зеркало серверного debug-флага: дропает входящие UDP-пакеты с указанной долей после
MAC/version validation. Service commands (Ping / handshake / ACK) дропаются в 2 раза
реже чтобы соединение не развалилось. Default `0`. **Не использовать в production.**

## Performance

Замеры на x86_64 release:

| Компонент | Throughput |
|-----------|-----------|
| Packet obfuscation (xoshiro128+) | ~920 MB/s |
| Packet MAC (HMAC-CRC32C) | ~5-7 GB/s |
| AES-128-GCM | Hardware-accelerated (AES-NI) |

Hot-path функции inlined через cross-crate boundary (см. `#[inline]` маркеры в
`moonproto-transport`). Кэшированный `Aes128Gcm` cipher в `Client` устраняет
key schedule expansion на каждый зашифрованный пакет.

## License

Open source. См. LICENSE.
