# Client — главная точка входа

`moonproto::client::Client` — handle для подключения к одному MoonProto-серверу.
Управляет всем что делает либа сама (active session manager): UDP socket с port
rotation, handshake, heartbeat, slicing, replay protection, retry с UKey dedup,
NTP sync, subscription registry, reconnect.

## Создание и запуск

```rust
use std::time::Duration;
use moonproto::client::{Client, ClientConfig, LifecycleEvent, RefreshConfig};
use moonproto::events::EventDispatcher;
use moonproto::key_import;

let keys = key_import::import_key("v3oshQy/OLZSjsCkpZIOuy4y7aWoD7U12kIXJSx7h8cBKiRjEVPSrBB8WVO7yCjC...")
    .expect("invalid key");

// `ClientConfig::new` устанавливает production-defaults: mask_ver=0, client_id=random,
// ntp_host=Some("pool.ntp.org"), refresh=RefreshConfig::default().
// Overrides — через builder methods (`.with_transport_mode(2)`, `.without_ntp()` и т.д.).
let cfg = ClientConfig::new("207.148.91.186", 3000, keys.master_key, keys.mac_key);
let mut client = Client::new(cfg);
let mut dispatcher = EventDispatcher::new();

// Рекомендуемый entry-point — типизированные Event'ы + active-library auto-actions.
client.run_with_dispatcher(
    Duration::from_secs(3600),
    &mut dispatcher,
    Box::new(|ev| {
        // ev: &moonproto::events::Event — обрабатывай что нужно.
    }),
);
```

## Три варианта main loop

### `Client::run_with_dispatcher` (рекомендуется)

Внутри: парсит каждый входящий пакет через `EventDispatcher::dispatch_into_active`,
делает auto-actions (RequestOrderBookFull dedup, periodic trades tick, indexes
sync gate), эмитит типизированные `Event` в callback.

```rust
fn run_with_dispatcher(
    &mut self,
    duration: Duration,
    dispatcher: &mut EventDispatcher,
    on_event: EventFn,    // = Box<dyn FnMut(&Event) + Send>
);
```

### `Client::run_with_dispatcher_state` (UI ergonomic)

То же что `run_with_dispatcher`, но callback дополнительно получает read-only
`&EventDispatcher` — удобно для events с id-only payload типа
`OrderEvent::Updated(uid)`: сразу читай `dispatcher.orders().by_id.get(&uid)` без
второго dispatch'а.

```rust
fn run_with_dispatcher_state(
    &mut self,
    duration: Duration,
    dispatcher: &mut EventDispatcher,
    on_event: EventWithStateFn,    // = Box<dyn FnMut(&Event, &EventDispatcher) + Send>
);
```

### `Client::run` (raw callback)

Низкоуровневый вариант: callback получает `(Command, &[u8])` — потребитель сам
парсит через `commands::*`. Active-library auto-actions **не выполняются**.
Используй только для специализированных задач (трейд-логгер, отладка).

```rust
fn run(&mut self, duration: Duration, on_data: OnDataFn);
```

## `Client::run_until_response` — single-thread API wait

Большинство `client.api_*()` методов возвращают `mpsc::Receiver<T>`. **Response
доставляется только пока Client работает.** Прямой `rx.recv_timeout(...)` в том
же thread'е что владеет Client'ом обычно timeout'ит — main loop стоит во время
блокирующего wait.

`run_until_response` решает: крутит короткие `run_with_dispatcher` тики (~50мс) до
получения значения, disconnect'а или общего timeout. Generic, работает с любым
`Receiver<T>` — Engine API responses, `MergedCandles` aggregator, и т.д.

```rust
let rx = client.api_get_markets_list();
let resp = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(10))?;

// Или candles aggregator:
let rx = client.api_request_candles_data_async();
let candles = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(30))?;
```

Если `Client` уже запущен в другом thread'е — обычный `rx.recv_timeout(...)`
работает (main loop работает параллельно).

## Конфигурация

```rust
pub struct ClientConfig {
    /// "207.148.91.186" / "10.0.0.1" / "[2001:db8::1]" (IPv6 в скобках).
    pub server_ip:   String,
    pub server_port: u16,
    /// 16 байт AES-128 master key (из key_import::import_key).
    pub master_key:  MoonKey,
    /// 16 байт HMAC-CRC32C MAC key (тоже из key_import).
    pub mac_key:     MoonKey,
    /// 0 = base transport V0 (open source, всегда работает).
    /// 1/2 = extended (требует moonext.dll/.so рядом с exe).
    pub mask_ver:    u8,
    /// Per-installation unique ID (генерируется один раз, сохраняется на диск).
    pub client_id:   u64,
    /// `Some(host)` → liба сама spawn'ит NTP thread (рекомендуется).
    /// `None` → отключить (или управлять NTP вручную через `ntp::spawn_sync_thread`).
    pub ntp_host:    Option<String>,
    /// Periodic refresh — auto UpdateMarketsList / CheckBinanceTags.
    pub refresh:     RefreshConfig,
}

pub struct RefreshConfig {
    /// Default `Some(60s)` — auto UpdateMarketsList для свежих prices/funding.
    pub update_markets_every: Option<Duration>,
    /// Default `None` — Binance-специфичная проверка futures permissions.
    pub check_tags_every:     Option<Duration>,
}
```

`MoonKey = [u8; 16]` — re-export из `moonproto_transport`.

`ClientConfig::Debug` — `master_key` и `mac_key` redacted в логах (`<REDACTED>`).

IPv6: bind_address выбирается автоматически по наличию `:` в `server_ip` —
`[::]:port` для IPv6, `0.0.0.0:port` для IPv4.

### Builder API

Для типичных случаев используй `ClientConfig::new()` + builder methods:

```rust
// Production defaults: mask_ver=0, client_id=random, ntp=pool.ntp.org, refresh=60s.
let cfg = ClientConfig::new("207.148.91.186", 3000, master_key, mac_key);

// Builder overrides:
let cfg = ClientConfig::new("10.0.0.1", 3000, master_key, mac_key)
    .with_transport_mode(2)           // extended mode (требует moonext.dll/.so)
    .with_client_id(0x1234_5678)      // фиксированный ID (для воспроизводимых тестов)
    .without_ntp()                    // отключить NTP thread (test/offline режим)
    .with_refresh(RefreshConfig {     // custom periodic refresh
        update_markets_every: None,
        check_tags_every: Some(Duration::from_secs(300)),
    });
```

Полный список builder methods: `with_transport_mode`, `with_client_id`,
`with_ntp_host`, `without_ntp`, `with_refresh`.

Struct literal (`ClientConfig { ... }`) тоже работает — все поля `pub`, builder
просто более concise.

## Lifecycle

Подключай callback через `Client::on_lifecycle`. Полная таблица событий и
семантика переходов — [lifecycle.md](lifecycle.md).

```rust
use moonproto::client::LifecycleEvent;

client.on_lifecycle(Box::new(|ev| match ev {
    LifecycleEvent::Connecting                       => println!("→ connecting"),
    LifecycleEvent::Connected { fresh: true }        => println!("→ ready (first connect)"),
    LifecycleEvent::Connected { fresh: false }       => println!("→ reconnected"),
    LifecycleEvent::Reconnecting                     => println!("→ reconnecting (либа сама)"),
    LifecycleEvent::ServerRestart                    => println!("→ server restart detected"),
    LifecycleEvent::SendBacklogCritical { cmd, u_key_uid } =>
        eprintln!("⚠ pending dropped: cmd={cmd} uid={u_key_uid}"),
    LifecycleEvent::BindFailed { consecutive_failures } =>
        eprintln!("⚠ cannot bind UDP ({} failures)", consecutive_failures),
    LifecycleEvent::Disconnected                     => println!("→ disconnected"),
}));
```

**Active library**: app только красит UI индикатор. Recovery/reconnect/re-subscribe
делает либа сама. Единственные events требующие реакции app — `SendBacklogCritical`
(торговый риск: дропнута cancel/replace команда) и `BindFailed` (OS network problem).

## Init sequence

После handshake (Connected{fresh:true}) обычно нужно: BaseCheck → AuthCheck →
GetMarketsList → подписки. Это **один логический шаг** — упакован в
`run_init_sequence`:

```rust
use moonproto::client::{InitConfig, run_init_sequence};
use moonproto::state::OrderBookKind;
use std::time::Duration;

// Phase 1: handshake до Connected{fresh:true}.
client.run_with_dispatcher(Duration::from_secs(3), &mut dispatcher, Box::new(|_| {}));

// Phase 2: init.
let cfg = InitConfig {
    base_check: true,
    auth_check: true,
    fetch_markets: true,
    fetch_balance: false,
    subscribe_trades: Some(false),                       // false = без MM ордеров
    subscribe_orderbooks: vec![
        ("BTCUSDT".to_string(), OrderBookKind::Futures),
    ],
    step_timeout: Some(Duration::from_secs(5)),
};
match run_init_sequence(&mut client, &mut dispatcher, cfg) {
    Ok(r) => println!("init ok: base={} auth={} markets={}B",
                       r.base_check_ok, r.auth_check_ok, r.markets_response_bytes),
    Err(e) => panic!("init failed: {e}"),
}

// Phase 3: long stream.
client.run_with_dispatcher(Duration::from_secs(3600), &mut dispatcher, Box::new(|ev| { /* ... */ }));
```

Внутри: chunked main loop pump (~50мс тики) между `api_*().recv_timeout` — без
этого main loop не работает во время `wait_for_api_response` и ответы не доходят.

```rust
pub struct InitConfig {
    pub base_check:           bool,
    pub auth_check:           bool,
    pub fetch_markets:        bool,
    pub fetch_balance:        bool,
    pub subscribe_trades:     Option<bool>,                                 // None = пропустить, Some(want_mm)
    pub subscribe_orderbooks: Vec<(String, OrderBookKind)>,
    pub step_timeout:         Option<Duration>,                             // default = 12с
}

pub struct InitResult {
    pub base_check_ok:           bool,
    pub auth_check_ok:           bool,
    pub markets_response_bytes:  usize,
    pub balances_response_bytes: usize,
    pub trades_subscribed:       bool,
    pub orderbooks_subscribed:   usize,
    pub errors:                  Vec<String>,    // non-critical step errors
}

pub enum InitError {
    SendChannelClosed,
    CriticalStepTimedOut(&'static str),    // BaseCheck/AuthCheck timeout
    NotAuthenticated,                       // нужен Connected{fresh:true} перед init
}
```

`BaseCheck` ответ парсится в `ServerInfo` и сохраняется в
`client.server_info()` (для multi-server идентификации — см.
[multi_server.md](multi_server.md)).

## Отправка команд

### High-level trade actions

18 методов с автоматическим UKey dedup и правильными retry settings — см.
[trade_actions.md](trade_actions.md).

```rust
use moonproto::commands::trade::{TradeCtx, OrderType, OrderWorkerStatus};

let ctx = TradeCtx::new(order_uid);
client.replace_order(ctx, "BTCUSDT", OrderWorkerStatus::SellSet, OrderType::Sell, 50100.0);
client.cancel_order(ctx, "BTCUSDT", OrderWorkerStatus::SellSet);
client.do_close_position(ctx, "BTCUSDT", true);
```

### Engine API (RPC)

29 high-level wrappers возвращают `mpsc::Receiver<EngineResponse>` — см.
[engine_api.md](engine_api.md).

```rust
use std::time::Duration;

let rx = client.api_get_markets_list();
let resp = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(10))?;
// resp.data — уже DEFLATE-decompressed.
```

### UI / Strat commands

15 UI-методов (`ui_*`) + 6 Strat-методов (`strat_*`) — см. [ui.md](ui.md) и
[strats.md](strats.md).

### Низкоуровневые `send_cmd` / `send_cmd_keyed`

```rust
use moonproto::client::{SendPriority, UniqueKey};
use moonproto::protocol::Command;

// Без dedup:
client.send_cmd(payload.clone(), Command::Order, SendPriority::High, true, 3);

// С UKey dedup (старая pending команда того же UKey удалится):
client.send_cmd_keyed(payload, Command::Order, SendPriority::High, true, 3,
                       UniqueKey::order_move(order_uid));
```

## Thread-safe subscribe API (F4)

Subscribe/unsubscribe доступны как `&self` методы — можно вызывать из любого
thread'а **во время** `run_with_dispatcher` (без `&mut Client` lock).

```rust
use moonproto::client::Client;
use moonproto::state::OrderBookKind;
use std::thread;

let mut client = Client::new(cfg);
let sender = client.sender();    // ClientSender — Clone, Send + Sync

// UI thread:
thread::spawn(move || {
    sender.subscribe_orderbook("BTCUSDT", OrderBookKind::Futures);
    sender.subscribe_all_trades(true);
});

// Main thread:
client.run_with_dispatcher(Duration::from_secs(3600), &mut dispatcher, Box::new(|_| {}));
```

Внутри: subscribe методы пушат `ClientEvent::Subscribe*` в bounded mpsc
(capacity 1024). Main loop в `run_with_dispatcher` дренирует каждый тик
(~5мс), применяет к `subscription_registry`, отправляет wire-запрос.
Идемпотентно — повторный subscribe для уже подписанной пары no-op.

**Auto-replay на reconnect.** При смене `ServerToken` либа сама шлёт все
зарегистрированные подписки заново — app **не должно** дублировать на
`LifecycleEvent::ServerRestart`.

**Channel overflow.** Если main loop забит и channel переполнен —
fire-and-forget `subscribe_*` пишет warning в log и теряет событие.
Для обратной связи — `try_*` варианты:

```rust
use moonproto::client::SubscribeError;

match client.sender().try_subscribe_orderbook("BTCUSDT", OrderBookKind::Futures) {
    Ok(())                              => {}
    Err(SubscribeError::ChannelFull)    => { /* retry через несколько ms */ }
    Err(SubscribeError::Disconnected)   => { /* Client уже дропнут */ }
}
```

### `Client::sender()` vs `Client::event_sender()`

- `Client::sender()` → **`ClientSender`** (высокоуровневый, с typed-методами
  `subscribe_*`/`try_subscribe_*`) — главный публичный API.
- `Client::event_sender()` → **raw `SyncSender<ClientEvent>`** для custom-протокольных
  сценариев (отправка `ClientEvent::Send(SendMsg { item })` напрямую). Использовать редко.

### Convenience-методы на `&Client`

Те же subscribe методы доступны прямо на `&Client` (внутри пушат через
`sender()`):

```rust
client.subscribe_orderbook("BTCUSDT", OrderBookKind::Futures);
client.subscribe_all_trades(true);
client.unsubscribe_orderbook("BTCUSDT", OrderBookKind::Futures);
client.unsubscribe_all_trades();
```

## Periodic refresh (F6/F7)

`ClientConfig.refresh: RefreshConfig` управляет автоматическими refresh-командами
которые либа сама шлёт в main loop:

```rust
RefreshConfig {
    update_markets_every: Some(Duration::from_secs(60)),  // дефолт
    check_tags_every:     None,                            // дефолт
}
```

**`update_markets_every`** — раз в указанный интервал шлёт `emk_UpdateMarketsList`
(fire-and-forget). Сервер обновляет `cfg.Markets` (prices, funding) каждые ~60с —
без этого пинга клиент будет показывать stale prices через час сессии. Parity с Delphi.

**`check_tags_every`** — Binance-специфичная проверка futures permissions через
`emk_CheckBinanceTags`. По умолчанию выключено. Включай если используешь Binance
API и нужна периодическая валидация прав.

Отключить refresh целиком:
```rust
RefreshConfig { update_markets_every: None, check_tags_every: None }
```

Тики обрабатываются только когда `auth_status = AuthDone` — до handshake запрос
потеряется впустую.

## NTP синхронизация

Liба сама spawn'ит NTP thread если `cfg.ntp_host = Some(host)`. Поток автоматически
останавливается в `Drop for Client`. По дефолту `Some("pool.ntp.org")` — рекомендуется
для корректных timestamp'ов в торговых командах.

Управление NTP вручную (если `ntp_host = None`):

```rust
use moonproto::ntp;
use moonproto::client::set_ntp_offset;

// Синхронный запрос — best of 4 attempts.
let result = ntp::get_best_ntp("pool.ntp.org", 4);
if result.synced {
    set_ntp_offset(result.time_offset);
}

// Или daemon thread (то же что Client делает сам если ntp_host=Some):
let shutdown = ntp::spawn_sync_thread("pool.ntp.org".to_string(), set_ntp_offset);
// shutdown.store(true, Ordering::Relaxed) — остановить.
```

**NTP-poisoning защита.** Если NTP сервер вернул offset `|offset| > 1 day`
(possible MITM / broken RTC) — offset rejected, лог warn. См. `ntp::is_reasonable_offset`.

## Server identity (multi-server)

После успешного `run_init_sequence` `client.server_info()` несёт идентификацию
сервера (`bot_id`, `exchange_name`, `base_currency_name`, версии). Это нужно для
multi-server терминалов — см. [multi_server.md](multi_server.md) и
[engine_api.md → ServerInfo](engine_api.md#serverinfo--multi-server-identification).

```rust
let info = client.server_info();
if let (Some(id), Some(name)) = (info.bot_id, &info.exchange_name) {
    println!("Bot #{} = {}", id, name);
}
```

До первого BaseCheck `server_info()` возвращает `ServerInfo::default()`
(`has_identity()=false`).

`client.set_server_info(info)` — manual override (если init делается своим
pattern'ом без `run_init_sequence`).

## ApiPending registry

Pending Engine API requests маршрутизируются автоматически. Большинство API
вызывается через high-level wrappers (`client.api_*()`) которые сами регистрируют
UID и возвращают `Receiver`.

Если нужен низкий уровень:

```rust
use std::time::Duration;
use moonproto::commands::engine_request;

let raw = engine_request::base_check();
let uid = u64::from_le_bytes(raw[3..11].try_into().unwrap());
let rx  = client.api_pending.register(uid, /* registered_at_ms = */ client_now_ms);
client.send_api_request(&raw);

// В однопоточном коде используй run_until_response — main loop не работает
// во время блокирующего rx.recv_timeout, ответ никогда не придёт.
match client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(10)) {
    Ok(resp) => process(resp),
    Err(_)   => { client.api_pending.remove(uid); /* timeout */ }
}
```

`Client::send_api_request_async(raw) -> Receiver<EngineResponse>` — удобная
оболочка над `register` + `send_api_request`.

**Auto-cleanup** устаревших pending slots делает либа сама из main loop
(default age = 12с, parity с Delphi engine).

## Observability

```rust
client.is_authorized();                  // bool — handshake завершён
client.auth_status();                    // AuthStatus { Base, Connected, AuthDone, Offline }
client.ping_count();                     // u32 — сколько Ping'ов обработано
client.total_sent() / .total_recv();     // u64 — суммарно байт
client.bytes_per_sec_sent();             // u64 — среднее за ~10с (EMA, O(1))
client.bytes_per_sec_recv();
client.round_trip_delay_ms();            // i64 — последний измеренный RTT
client.actual_pmtu();                    // u16 — текущий PMTU
client.rs();                             // f64 — receive success ratio (для AIMD)
client.avg_over_heat();                  // f64 % retransmission overhead
client.server_time_delta_days();         // f64 — TDateTime offset от server
client.net_lag_ping_ms();                // i64 — abs(NTP-corrected time − server time)
client.global_timing_orders();           // u16
client.server_token();                   // u64 — текущий ServerToken
client.peer_app_token();                 // u64 — PeerAppToken (для server-restart detection)
```

Log throttle (anti-spam для warning'ов):

```rust
if client.should_log("transport_mismatch", 1000) {
    eprintln!("warn: transport version mismatch");
}
```

## Завершение

```rust
client.disconnect();    // явный завершить main loop → LifecycleEvent::Disconnected.
```

`Drop for Client` сигналит reader thread'у И self-managed NTP-thread'у
завершиться. Reader выйдет через макс ~1с (read_timeout), NTP — через ~100мс.

## См. также

- [overview.md](overview.md) — общий обзор.
- [events.md](events.md) — `EventDispatcher` (auto-apply state).
- [lifecycle.md](lifecycle.md) — детали lifecycle.
- [multi_server.md](multi_server.md) — multi-Client терминал.
- [engine_api.md](engine_api.md) — RPC + ServerInfo.
- [trade_actions.md](trade_actions.md) — 18 trade wrappers.
- [candles.md](candles.md) — chunked candles aggregator.
- DEVIATION.md (в корне) — реестр архитектурных отклонений от Delphi.
