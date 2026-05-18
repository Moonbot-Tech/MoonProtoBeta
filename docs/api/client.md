# Client — главная точка входа

`moonproto::Client` — handle для подключения к MoonProto серверу. Управляет:
- UDP socket'ом и port rotation.
- Handshake'ом (Hello → WhoAreYou → ImFriend → Fine).
- Heartbeat'ами (Ping каждую секунду).
- Slicing'ом больших сообщений + ACK256.
- Replay protection через sliding window slider.
- Retry'ями (H priority + Sliced) с UKey dedup.
- NTP offset для синхронизации времени с сервером.
- Reconnect при потере связи.
- Lifecycle callbacks для UI status.

## Создание и запуск

```rust
use moonproto::{Client, key_import::import_master_key, ClientConfig};

let key = import_master_key("v3oshQy/OLZSjsCkpZIOuy4y7aWoD7U12kIXJSx7h8cBKiRjEVPSrBB8WVO7yCjC...")
    .expect("invalid key");

let cfg = ClientConfig {
    server_ip: "207.148.91.186".to_string(),
    server_port: 3000,
    master_key: key.master_key,
    mac_key: key.mac_key,
    mask_ver: 0,
    client_id: rand::random(),
};
let mut client = Client::new(cfg);

// on_data callback — для каждого декодированного MPC_* пакета.
client.run(Duration::from_secs(60), Box::new(|cmd_class, payload| {
    println!("Received {} bytes for {:?}", payload.len(), cmd_class);
}));
```

Для **auto-apply** state'ов — использовать [`EventDispatcher`](events.md):

```rust
let mut dispatcher = EventDispatcher::new();
client.run(Duration::from_secs(60), Box::new(move |cmd, payload| {
    for ev in dispatcher.dispatch(cmd, payload, current_ms()) {
        match ev { /* ... */ }
    }
}));
```

## Lifecycle

См. [lifecycle.md](lifecycle.md) для подробностей.

```rust
use moonproto::client::LifecycleEvent;

client.on_lifecycle(Box::new(|ev| match ev {
    LifecycleEvent::Connecting     => ui_status("Подключение..."),
    LifecycleEvent::Authenticated  => ui_status("Подключено"),
    LifecycleEvent::Reconnecting   => ui_status("Переподключение..."),
    LifecycleEvent::ServerRestart  => cache.invalidate(),
    LifecycleEvent::Disconnected   => ui_status("Отключено"),
}));
```

## Lifecycle переходов

1. **Bind**: UDP socket с port rotation. При неудаче 200 раз подряд → `LifecycleEvent::Disconnected`.
2. **Handshake**:
   - C→S `Hello(56 bytes, encrypted with master_key)`
   - S→C `WhoAreYou(challenge)` — **детекция `ServerRestart`** по изменению `peer_app_token`
   - C→S `ImFriend(answer)` — **двойная отправка** с 32ms паузой (DPI-resistant)
   - S→C `Fine` → `LifecycleEvent::Authenticated`
3. **Steady-state**: Ping каждую секунду, команды каналов, retry, heartbeat.
4. **Sliced**: пакеты > PMTU режутся на 256б куски + ACK256 на каждый.
5. **Reconnect**: при отсутствии трафика > 7s → `LifecycleEvent::Reconnecting` → re-handshake → `Connecting` → `Authenticated`.

## Отправка данных

### Низкоуровневое

```rust
// High priority с retry (encrypted, MaxRetries=3)
client.send_cmd(payload.clone(), Command::Order, SendPriority::High, true, 3);

// С UKey dedup (старая pending команда того же UKey удалится)
client.send_cmd_keyed(payload, Command::Order, SendPriority::High, true, 3, u_key);
```

### Engine API (RPC)

См. [engine_api.md](engine_api.md). 29 high-level wrappers:

```rust
let rx = client.api_get_markets_list();           // GetMarketsList
let rx = client.api_get_balance("USDT");          // GetBalance
let rx = client.api_set_leverage("BTCUSDT", 10);  // SetLeverage
// ... и т.д.
let resp = rx.recv_timeout(Duration::from_secs(5))?;
```

### Trade actions

См. [trade_actions.md](trade_actions.md). 17 высокоуровневых методов:

```rust
let ctx = TradeCtx::new(order_uid);
client.replace_order(ctx, "BTCUSDT", epoch, status, OrderType::Sell, 50100.0);
client.cancel_order(ctx, "BTCUSDT", epoch, status);
client.do_close_position(ctx, "BTCUSDT", true);
// ... 14 ещё ...
```

### Candles

См. [candles.md](candles.md):

```rust
let rx = client.api_get_coin_card_candles("BTCUSDT", DeepHistoryKind::Hour1);
// ... или chunked:
client.api_request_candles_data();
```

## ApiPending registry

Pending Engine API requests маршрутизируются автоматически. `send_api_request_async` возвращает `Receiver`:

```rust
let rx = client.send_api_request_async(&engine_request::base_check());
let resp = rx.recv_timeout(Duration::from_secs(5))?;
```

Если потребитель хочет вручную:

```rust
let raw = engine_request::get_markets_list();
let uid = u64::from_le_bytes(raw[3..11].try_into().unwrap());
let rx = client.api_pending.register(uid);
client.send_api_request(&raw);
// потом recv через rx
```

Не забыть `client.api_pending.remove(uid)` при timeout — иначе sender зависнет в map.

## NTP синхронизация

```rust
use moonproto::ntp;
use moonproto::client::set_ntp_offset;

// Сделать запрос к NTP-серверу (синхронный)
let result = ntp::get_best_ntp("pool.ntp.org", 4);
if result.synced {
    set_ntp_offset(result.time_offset);
}

// Или daemon thread (рекомендуется):
ntp::spawn_sync_thread("pool.ntp.org".to_string(), set_ntp_offset);
// Thread сам делает init + cycle с уточнением, byte-exact с TMoonProtoTymeSyncer.
```

## Observability

```rust
client.ping_count();             // u32 — сколько Ping'ов обработано
client.total_sent();             // u64 — суммарно отправлено
client.total_recv();             // u64 — суммарно получено
client.bytes_per_sec_sent();     // u64 — среднее за последние 10 сек
client.bytes_per_sec_recv();
client.avg_over_heat();          // f64 % retransmission overhead (Sliced)
client.is_authorized();
client.auth_status();            // AuthStatus enum
```

Log throttle (anti-spam для warning'ов):

```rust
if client.should_log("transport_mismatch", 1000) {
    eprintln!("warn: transport version mismatch");
}
```

## Cross-thread safety

- **Reader thread** принимает UDP, пушит `ClientEvent::Recv` в mpsc channel.
- **Main thread** (`run()`) обрабатывает: handle Ping/Sliced/Crypted, retry, heartbeat, on_data, lifecycle callbacks.
- **API pending registry** (`client.api_pending`) — thread-safe (`Arc<Mutex<HashMap>>`), можно клонировать в любой поток.

`on_data` и `on_lifecycle` callback'и вызываются в main thread. **Правило:** лёгкие callback'и (ms-уровень). Тяжёлая обработка — в отдельный thread через channel.

## Конфигурация

```rust
pub struct ClientConfig {
    pub server_ip: String,        // "207.148.91.186" или "[2001:db8::1]" для IPv6
    pub server_port: u16,
    pub master_key: MoonKey,      // 16 байт AES-128 key
    pub mac_key: MoonKey,         // 16 байт HMAC-CRC32C key
    pub mask_ver: u8,             // 0=V0 open, 1/2=extended (требует moonext.dll/.so)
    pub client_id: u64,
}
```

IPv6: bind_address выбирается автоматически по наличию `:` в `server_ip` — `[::]:port` для IPv6, `0.0.0.0:port` для IPv4.

## См. также

- [overview.md](overview.md) — общий обзор библиотеки.
- [events.md](events.md) — EventDispatcher (auto-apply state).
- [lifecycle.md](lifecycle.md) — детали lifecycle callbacks.
- [engine_api.md](engine_api.md) — RPC методы.
- [trade_actions.md](trade_actions.md) — Trade high-level wrappers.
- DEVIATION.md — список архитектурных отклонений (mpsc channel, IV mask, NTP thread и т.д.).
