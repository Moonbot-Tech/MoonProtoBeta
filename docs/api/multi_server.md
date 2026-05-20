# Multi-server — несколько `Client` в одном процессе

Один пользователь, несколько серверов MoonBot (например Binance Futures +
Bybit + HyperLiquid). Терминал хочет показывать все одновременно. `moonproto`
поддерживает это через **несколько независимых `Client`** в одном процессе.

В Delphi-боте этого нет — один процесс = один сервер. Это **расширение
функциональности** Rust-порта (proven на live-сервере, см.
`examples/multi_client_test.rs`).

## Когда нужен multi-server

- **Multi-exchange терминал**: один аккаунт, несколько серверов под разные биржи
  (Binance Futures, Bybit, HyperLiquid).
- **Multi-account on one exchange**: несколько серверов под одну биржу с разными
  API ключами.
- **Главный + резервный**: production-сервер + dev-сервер для тестирования.
- **Региональные шарды**: серверы в разных регионах для latency-optimization.

## Структура

Каждый `Client` — независимая UDP-сессия:
- свой UDP socket (свой локальный порт);
- свой reader thread;
- свой state (handshake, slider, slicing, subscription registry);
- свой `server_time_delta_handle` (per-instance `Arc<AtomicU64>`);
- свой `server_info` (idenitity сервера из BaseCheck);
- свой NTP thread (если `cfg.ntp_host = Some`).

Process-global только то что реально machine-wide:
- `NTP_OFFSET_DAYS` — offset системного UTC от NTP (один для всего процесса).
- `IV_COUNTER` + `IV_MASK` — atomic IV счётчик для AES-GCM (никогда не повторяется
  на одном ключе; разные Client'ы используют разные ключи → коллизий нет).
- `CLOCK_JUMP_DETECTED` — system-wide clock jump flag.

## Минимальный пример

```rust
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use moonproto::client::{Client, ClientConfig, RefreshConfig, InitConfig, run_init_sequence};
use moonproto::events::EventDispatcher;
use moonproto::key_import;

fn run_one_server(label: &str, ip: String, port: u16, keys: key_import::ImportedKeys) {
    let cfg = ClientConfig::new(ip, port, keys.master_key, keys.mac_key);
    let mut client = Client::new(cfg);
    let mut dispatcher = EventDispatcher::new();

    // Phase 1: handshake.
    client.run_with_dispatcher(Duration::from_secs(3), &mut dispatcher, Box::new(|_| {}));

    // Phase 2: init (заполнит client.server_info).
    let _ = run_init_sequence(&mut client, &mut dispatcher, InitConfig {
        base_check: true,
        auth_check: true,
        fetch_markets: true,
        subscribe_trades: Some(false),
        ..Default::default()
    });

    let info = client.server_info().clone();
    println!("[{label}] bot_id={:?} exchange={:?}",
             info.bot_id, info.exchange_name);

    // Phase 3: long-running stream.
    client.run_with_dispatcher(
        Duration::from_secs(3600),
        &mut dispatcher,
        Box::new(move |_ev| {
            // routing в UI по label или client.server_info().bot_id
        }),
    );
}

fn main() {
    let keys = key_import::import_key("v3oshQy/...").expect("invalid key");

    // Параллельно запускаем 3 Client'а — в 3 thread'ах.
    let h1 = {
        let k = keys.clone();
        thread::spawn(move || run_one_server("Binance", "10.0.0.1".to_string(), 3000, k))
    };
    let h2 = {
        let k = keys.clone();
        thread::spawn(move || run_one_server("Bybit",   "10.0.0.2".to_string(), 3000, k))
    };
    let h3 = {
        let k = keys.clone();
        thread::spawn(move || run_one_server("HL",      "10.0.0.3".to_string(), 3000, k))
    };

    let _ = h1.join();
    let _ = h2.join();
    let _ = h3.join();
}
```

## ServerInfo — идентификация серверов

После успешного BaseCheck `client.server_info()` несёт identity (заполняется
автоматически в `run_init_sequence`):

```rust
let info = client.server_info();

println!("Bot {}: {} ({}, version {})",
    info.bot_id.unwrap_or(0),
    info.server_name.as_deref().unwrap_or("?"),
    info.exchange_name.as_deref().unwrap_or("?"),
    info.server_version.unwrap_or(0));

if info.supports(moonproto::commands::engine_api::exchange_type_flags::FUTURES) {
    // показать UI для futures-only функций
}
```

Поля `ServerInfo`:

| Поле | Тип | Что |
|---|---|---|
| `bot_id` | `Option<i64>` | Стабильный уникальный ID сервера (основной ключ для routing UI). |
| `server_name` | `Option<String>` | `"Binance Main"` / `"Bybit Dev"` (для UI tab title). |
| `exchange_code` | `Option<u8>` | Ord `TBotPlatform` enum. |
| `exchange_name` | `Option<String>` | `"Binance Futures"` / `"Hyper"`. |
| `exchange_type_mask` | `Option<u8>` | Bitmask: Spot(0x01) / Futures(0x02) / DEX(0x04) / Predict(0x08). |
| `dex_name` | `Option<String>` | HIP-3 dex name для HyperLiquid. |
| `base_currency_name` | `Option<String>` | `"USDT"` / `"BTC"` / `"USDC"`. |
| `base_currency_code` | `Option<u8>` | Ord `TBaseCurrency`. |
| `server_version` | `Option<u32>` | `763` для v7.63. |
| `moonproto_version` | `Option<u32>` | `IntMoonProtoTCPCurrentVer`. |

**Все поля `Option`** — старый сервер до multi-server расширения вернёт пустой
payload и все поля будут `None`. `info.has_identity()` = `bot_id.is_some()` для
быстрой проверки.

Forward-compat: парсер толерантен к truncate в любом месте payload'а — заполненные
поля сохраняются, остальные = `None`.

## UI pattern — `HashMap<bot_id, ClientHandle>`

```rust
use std::collections::HashMap;
use std::sync::Mutex;

struct ClientHandle {
    sender:      moonproto::client::ClientSender,
    server_info: moonproto::commands::engine_api::ServerInfo,
    // event channel для UI thread'а:
    event_rx:    std::sync::mpsc::Receiver<UiEvent>,
}

enum UiEvent {
    OrderUpdate { bot_id: i64, /* ... */ },
    TradeUpdate { bot_id: i64, /* ... */ },
}

let clients: Mutex<HashMap<i64, ClientHandle>> = Mutex::new(HashMap::new());

// В каждом per-client thread'е:
//   client.run_with_dispatcher(...) с callback который пушит UiEvent { bot_id, ... }
//   в общий channel UI thread'а.
```

UI thread читает channel и роутит по `bot_id` в соответствующий таб/вкладку.

## Per-Client ServerTimeDelta (DEVIATION #23)

**Каждый Client имеет свой ServerTimeDelta** — разные серверы имеют разный clock
drift. Без per-Client delta все Order timestamps были бы скошены на delta
последнего активного Client'а.

В Rust порте это решено через `Arc<AtomicU64>` handle:

```rust
let handle: Arc<AtomicU64> = client.server_time_delta_handle();
// handle.load() возвращает f64 (через to_bits/from_bits) — delta в днях.
```

Каждый Client обновляет свой handle в `handle_ping`. EventDispatcher должен
читать **именно этот** handle для своего Client'а:

### Auto-link (рекомендуется)

При `client.run_with_dispatcher(...)` или `dispatcher.dispatch_into_active(&mut client, ...)`
линковка делается **автоматически** на первом вызове. Manual работы нет:

```rust
let mut client = Client::new(cfg);
let mut dispatcher = EventDispatcher::new();
// При первом dispatch_into_active внутри run_with_dispatcher:
// dispatcher.server_time_delta_source = Some(client.server_time_delta_handle())
client.run_with_dispatcher(Duration::from_secs(3600), &mut dispatcher, Box::new(|_| {}));
```

### Manual link (custom dispatch pattern)

Если используешь `dispatch` / `dispatch_into` (без `_active` варианта) — линкуй вручную:

```rust
let mut dispatcher = EventDispatcher::new();
dispatcher.set_server_time_delta_source(client.server_time_delta_handle());
// Теперь Event::Order применяет именно delta этого Client'а.
```

### Что будет без link'а

`dispatcher` падает обратно на global `SERVER_TIME_DELTA_DAYS` (back-compat для
single-Client потребителей). При multi-Client global перезаписывается последним
активным Client'ом → все остальные EventDispatcher'ы видят чужую delta → Order
timestamps off by 50-1000ms.

См. `examples/multi_client_test.rs` для proven multi-Client smoke-теста и unit-тесты
`two_dispatchers_with_distinct_handles_are_isolated` в `events.rs`.

## Что делать если ServerInfo пустой

Старый сервер до расширения (`emk_BaseCheck` возвращал пустой payload):

```rust
let info = client.server_info();
if !info.has_identity() {
    // Старый сервер. Различай Client'ы по своему label / config (например ip:port).
    // НЕ полагайся на bot_id (он None).
}
```

Это нормальная ситуация — сервер постепенно обновится.

## Independent lifecycle

Каждый Client имеет свой LifecycleEvent stream. UI должен показывать индикатор
**на каждую вкладку**:

```rust
client.on_lifecycle(Box::new(move |ev| {
    let bot_id = saved_bot_id;    // copy from client.server_info().bot_id
    ui_thread.send(UiEvent::Lifecycle { bot_id, ev });
}));
```

`SendBacklogCritical` и `BindFailed` — критические события **на конкретный
сервер**, не всю систему. UI должен показать алерт **в той вкладке**.

## Independent subscription registry

Каждый Client держит свой `subscription_registry` — `subscribe_orderbook("BTCUSDT", ...)`
на Client A не подписывает Client B на тот же рынок. Это правильно (разные серверы
независимы), но UI должен подписывать оба независимо.

## Что общего между Client'ами

- **NTP offset** — process-global (один NTP-источник на машину; если все Client'ы
  имеют `ntp_host = Some("pool.ntp.org")` — они spawn'ят отдельные threads но пишут
  в общий global). Лучше: один NTP host достаточен, остальные `ntp_host = None`.
- **IV counter / mask** — atomic process-global (security — никогда не повторяется
  на одном ключе; разные Client'ы — разные ключи).
- **`set_err_emu(percent)`** — process-global эмуляция packet loss (тестовый
  инструмент).

## Performance considerations

- 3 Client'а = 3 reader thread + 3 main loop thread = 6 OS threads. На modern
  hardware (4+ cores) overhead незаметный.
- Каждый Client держит ~MB heap (буферы Sliced/Trades cache/etc.). Считай ~2-5MB
  на Client.
- Bandwidth scales linearly — каждый Client сам управляет своим AIMD-контролем
  скорости.

Тестировалось до 2 Client'ов параллельно (`examples/multi_client_test.rs`).
Теоретический лимит — десятки Client'ов на машину (FD limits + reader thread'ы).

## Live-tested example

```powershell
cd X:\proj-X\xGit\MoonKernel\moonproto
cargo run --release --example multi_client_test -- "v3oshQy/..." "207.148.91.186:3000"
```

Ожидаемый вывод:
```
[main] key OK; spawning 2 clients to 207.148.91.186:3000
[A] phase 1: connecting (client_id=0x...)...
[B] phase 1: connecting (client_id=0x...)...
[A] phase 2: init sequence...
[A]   init ok: base=true auth=true markets=NNN B
[B] phase 2: init sequence...
[B]   init ok: base=true auth=true markets=NNN B
[A] phase 3: streaming for 20s...
[B] phase 3: streaming for 20s...
...
========== VERDICT ==========
PASS: оба Client'а независимо подключились, handshake'нулись, получают трафик.
Multi-server архитектура работает.
```

## См. также

- [client.md](client.md) — Client lifecycle, `server_info()`, `server_time_delta_handle()`.
- [engine_api.md](engine_api.md) — `ServerInfo` wire-format + `exchange_type_flags`.
- [events.md](events.md) — `EventDispatcher::set_server_time_delta_source`.
- [lifecycle.md](lifecycle.md) — per-Client lifecycle events.
- `examples/multi_client_test.rs` — proven multi-Client smoke-test.
- `DEVIATION.md #23` — обоснование per-Client ServerTimeDelta + история фикса.
