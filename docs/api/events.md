# EventDispatcher — auto-apply state

`EventDispatcher` — высокоуровневая обёртка поверх raw data callback'а. Парсит
входящие пакеты по каналам, автоматически применяет к sync-state'ам и возвращает
типизированные `Event` потребителю.

В режиме `dispatch_into_active` (используется `Client::run_with_dispatcher`)
выполняет **active-library auto-actions**: auto-RequestOrderBookFull при
corruption, gate TradesStream/OrderBook парсинг пока markets indexes не sync,
auto-echo strat snapshot, auto-link per-Client ServerTimeDelta.

## Зачем

Без `EventDispatcher` потребитель пишет руками для каждого канала:

```rust
client.run(duration, Box::new(|cmd, payload| {
    match cmd {
        Command::Order => {
            if let Some(tc) = TradeCommand::parse(payload) {
                let (_, ev) = orders.apply(tc);
                // ... handle ev ...
            }
        }
        Command::OrderBook => {
            if let Some(pkt) = parse_order_book_packet(payload) {
                let evs = books.on_packet(pkt, now_ms());
                for ev in evs {
                    if let OrderBookEvent::RequestFullNeeded { market_index, book_kind } = ev {
                        // нужно отправить emk_RequestOrderBookFull
                        client.send_api_request(/* ... */);
                    }
                }
            }
        }
        Command::Balance => { /* sub-cmd 2/3/4 vs 6 dispatch */ }
        // ... 9 каналов вручную ...
    }
}));
```

С `EventDispatcher` — одна строка:

```rust
let mut dispatcher = EventDispatcher::new();
client.run_with_dispatcher(duration, &mut dispatcher, Box::new(|ev| match ev {
    Event::Order(o) => { /* ... */ }
    Event::OrderBook(ob) => { /* ... */ }
    _ => {}
}));
```

И **либа сама** отправляет `RequestOrderBookFull` при corruption, переподписывает
streams после reconnect, и т.д.

## Структура

```rust
pub struct EventDispatcher {
    // Все поля pub(crate) — read-only снаружи через getters.
    // Мутация — только через dispatch_into / dispatch_into_active.
}

impl EventDispatcher {
    pub fn new() -> Self;

    // === Read-only state getters ===
    pub fn orders(&self)      -> &Orders;
    pub fn order_books(&self) -> &OrderBooks;
    pub fn trades(&self)      -> &TradesState;
    pub fn balances(&self)    -> &BalancesState;
    pub fn strats(&self)      -> &StratsState;
    pub fn settings(&self)    -> &SettingsState;
    pub fn markets(&self)     -> &MarketsState;

    // === Dispatch APIs ===
    pub fn dispatch(&mut self, cmd: Command, payload: &[u8], now_ms: i64) -> Vec<Event>;
    pub fn dispatch_into(&mut self, cmd: Command, payload: &[u8], now_ms: i64, out: &mut Vec<Event>);
    pub fn dispatch_into_active(
        &mut self,
        cmd: Command,
        payload: &[u8],
        now_ms: i64,
        out: &mut Vec<Event>,
        client: &mut Client,
    );

    // === Manual ticking (если нужен custom main loop) ===
    pub fn tick_trades(&mut self, rtt_ms: i64, now_ms: i64) -> Vec<Vec<u8>>;
    pub fn tick_trades_with_events(&mut self, rtt_ms: i64, now_ms: i64) -> (Vec<Vec<u8>>, Vec<TradesEvent>);

    // === Multi-Client ServerTimeDelta source ===
    pub fn set_server_time_delta_source(&mut self, handle: Arc<AtomicU64>);
}
```

## Три варианта dispatch

### `dispatch_into_active(&mut Client)` — рекомендуется

**Active library mode**. Использовать когда есть `&mut Client`. Делает:

1. Lazy-link `server_time_delta_source` к Client'у (multi-Client safety).
2. Hard-reconnect detection через `client.server_token()` — при смене токена
   `trades.full_reset()` + `order_books.clear()` ДО применения нового пакета
   (иначе stale `last_packet_num` даст ложные `GapDetected`).
3. Вызов `dispatch_into` (обычный парсинг по каналам).
4. **Auto-action 1**: `OrderBookEvent::RequestFullNeeded` → автоматически
   отправляется `api_request_order_book_full` (dedup в пределах одного
   dispatch вызова — Grouped payload может содержать несколько RequestFullNeeded
   для одной книги, шлём один запрос).
5. **Auto-action 2**: `StratEvent::SnapshotRequested` → если есть кэшированный
   `last_full_snapshot_raw` — автоматически шлёт его обратно через
   `client.strat_send_snapshot`.

Используется внутри `Client::run_with_dispatcher` — потребитель этот метод
обычно сам не вызывает.

### `dispatch_into(out: &mut Vec<Event>)` — zero-alloc

Базовый dispatch без active-library auto-actions. Удобен когда `&mut Client`
недоступен (например в test'ах) или для performance-sensitive потребителей
которые переиспользуют буфер событий:

```rust
let mut buf = Vec::with_capacity(8);
loop {
    buf.clear();
    dispatcher.dispatch_into(cmd, payload, now_ms, &mut buf);
    for ev in &buf { /* handle */ }
}
```

**NB**: gating логики (block TradesStream/OrderBook пока `markets.indexes_synchronized = false`)
работает и здесь — это часть `dispatch_into`, не `dispatch_into_active`. Но
auto-actions (RequestFullNeeded → send_api_request) не выполняются — потребитель
должен сам обрабатывать `OrderBookEvent::RequestFullNeeded` events.

### `dispatch` — backwards-compat обёртка

`dispatch(cmd, payload, now_ms) -> Vec<Event>`. Внутри `dispatch_into` с
allocation нового `Vec` на каждый вызов. Используй только если **не** держишь
hot loop (где `dispatch_into` экономит alloc).

## Event enum

```rust
pub enum Event {
    /// MPC_Order: создание/обновление/удаление ордера.
    Order(OrderEvent),
    /// MPC_OrderBook: применение snapshot/diff либо RequestFullNeeded.
    OrderBook(OrderBookEvent),
    /// MPC_TradesStream / MPC_TradesResendResponse: одно событие за раз
    /// (Apply / Duplicate / GapDetected / GapFilled / BucketClosed / OutOfOrder).
    /// Если один пакет породил несколько TradesEvent (например Apply + GapFilled),
    /// они пушатся в `out` как отдельные `Event::Trade(...)` — без nested Vec
    /// (audit_rust_quality #11, экономия 50K Vec alloc/sec на пиковой нагрузке).
    Trade(TradesEvent),
    /// MPC_Balance sub-cmd 2/3/4: обновление балансов.
    Balance(BalanceEvent),
    /// MPC_Balance sub-cmd 6: raw arb-prices payload.
    Arb { uid: u64, payload: Vec<u8> },
    /// MPC_Strat: snapshot/delete/sell-price update/checked-sync.
    Strat(StratEvent),
    /// MPC_UI: settings updated, MM subscribe changed, etc.
    Settings(SettingsEvent),
    /// MPC_API (markets-related): auto-apply Markets state + событие.
    Markets(MarketsEvent),
    /// MPC_API: RPC response (если не markets-related или одновременно с Markets).
    EngineResponse(EngineResponse),
    /// MPC_LogMsg: server-side log message.
    ServerLog { time: f64, msg: String },
    /// Канал без специальной обработки (Reserved, etc.).
    Raw { cmd: Command, payload: Vec<u8> },
    /// Payload не распарсился (повреждение / wrong version).
    ParseFailed { cmd: Command, len: usize },
}
```

## Гайд по variant'ам

| Variant | Канал | Что | Per packet |
|---|---|---|---|
| `Order` | MPC_Order | Создан / обновлён / удалён ордер | 1 |
| `OrderBook` | MPC_OrderBook | Apply (snapshot/diff) либо RequestFullNeeded либо Ignored | 1+ (drain cache) |
| `Trade` | MPC_TradesStream | Apply / Duplicate / GapDetected / GapFilled / BucketClosed | 1 на каждый sub-event (Apply + GapFilled → 2 events) |
| `Balance` | MPC_Balance sub 2/3/4 | Обновление балансов | 1 |
| `Arb` | MPC_Balance sub 6 | Raw arb payload (декодер — Stage 3+) | 1 |
| `Strat` | MPC_Strat | Snapshot / delete / sell-price / checked-sync | 1 |
| `Settings` | MPC_UI | 14 UI подкоманд | 1 |
| `Markets` | MPC_API (markets-related) | Auto-apply MarketsState | 1 (+ EngineResponse параллельно) |
| `EngineResponse` | MPC_API | RPC response (всегда) | 1 |
| `ServerLog` | MPC_LogMsg | `[server] <time>: <msg>` | 1 |
| `Raw` | прочие | Канал без обработки (Reserved1/2, etc.) | 1 |
| `ParseFailed` | любой | Payload не распарсился | 1 |

## Пример полного цикла

```rust
use std::time::Duration;
use moonproto::client::{Client, ClientConfig, RefreshConfig};
use moonproto::events::{EventDispatcher, Event};
use moonproto::state::{OrderEvent, OrderBookEvent, TradesEvent};

let mut client = Client::new(cfg);
let mut dispatcher = EventDispatcher::new();

client.run_with_dispatcher(
    Duration::from_secs(3600),
    &mut dispatcher,
    Box::new(|ev| match ev {
        Event::Order(OrderEvent::Created(uid)) => {
            // dispatcher.orders().by_id.get(uid) — детали (state read-only через getter).
            println!("new order {uid}");
        }
        Event::Order(OrderEvent::Updated(uid)) => {
            println!("order {uid} updated");
        }
        Event::Order(OrderEvent::Removed(uid)) => {
            println!("order {uid} closed");
        }
        Event::OrderBook(OrderBookEvent::Apply { market_index, is_full, .. }) => {
            // Перерисовать стакан.
            let _ = (market_index, is_full);
        }
        Event::OrderBook(OrderBookEvent::RequestFullNeeded { .. }) => {
            // НЕ нужно ничего слать — dispatch_into_active уже отправил
            // api_request_order_book_full. Это для UI awareness "загружаем стакан".
        }
        Event::Trade(te) => match te {
            TradesEvent::Apply(pkt) => { /* раздать pkt.sections по UI */ }
            TradesEvent::GapDetected { .. } => { /* log only — recover автоматически */ }
            _ => {}
        },
        Event::ServerLog { time, msg } => eprintln!("[server@{time}] {msg}"),
        Event::ParseFailed { cmd, len } => eprintln!("WARN: parse {cmd:?} {len}B failed"),
        _ => {}
    }),
);
```

## Семантика per-канал

### `Command::Order`
- Парсит через `TradeCommand::parse(payload)`.
- Применяет к `Orders` через `orders.apply(cmd)` → `OrderEvent`.
- `dispatch_into` **auto-applies** текущий `server_time_delta` (per-Client или
  global fallback) к Orders state — иначе AdjustTime даст 0 delta = silent bug.
- Один `Event::Order` на пакет.

### `Command::OrderBook`
- **Gated**: если `markets.indexes_synchronized = false` → silent drop (event не эмитится).
  Это критичный инвариант — `market_index` от сервера может быть по новой
  нумерации (после server restart), без resync применили бы к старому by_index =
  silent data corruption.
- Парсит `parse_order_book_packet(payload)`.
- Применяет `order_books.on_packet(pkt, now_ms)`.
- Может быть **несколько** `Event::OrderBook` если drain reorder cache.

### `Command::TradesStream`
- **Gated** так же как `OrderBook`.
- Парсит `parse_trades_packet(payload)`.
- `trades.on_packet(pkt, now_ms)` → `Vec<TradesEvent>`.
- **Каждый TradesEvent** пушится в `out` как отдельный `Event::Trade(...)` —
  без nested Vec (audit_rust_quality #11).

### `Command::TradesResendResponse`
- Batch: `parse_trades_resend_response(payload)` → `Vec<inner_payloads>`.
- Для каждого inner: `parse_trades_packet` → `trades.on_packet_resend` (НЕ
  двигает `last_packet_num`).
- Каждый sub-event → отдельный `Event::Trade(...)` в `out`.

### `Command::Balance`
Sub-cmd dispatch по `payload[0]`:
- `2 | 3 | 4` → `parse_balance(sub, &payload[11..])` → `balances.apply` → `Event::Balance`.
- `6` → `parse_arb_prices(payload)` → `Event::Arb { uid, payload }`.
- Иначе → `Event::Raw`.

### `Command::API` (Engine RPC response)

Если успешный `EngineResponse` и `method` — markets-related, **auto-apply** к
`MarketsState` + эмит ДВУХ событий: `Event::Markets(...)` + `Event::EngineResponse(resp)`.

Auto-applied методы:
- `GetMarketsList` → `markets.apply_markets_list`
- `UpdateMarketsList` → `markets.apply_markets_prices`
- `GetMarketsIndexes` → `markets.apply_markets_indexes` (взводит `indexes_synchronized=true`)
- `CheckBinanceTags` → `markets.apply_token_tags`

Прочие методы → один `Event::EngineResponse`.

**NB про версию parsing'а**: `parse_markets_list_response` требует `ver` параметр
(v2 добавила FuturesType byte). EventDispatcher использует `ASSUMED_VER=2`
(текущая версия live-сервера). Если сервер обновится — правка в `events.rs`.

### `Command::LogMsg`
- Парсит `time:f64 + utf8 bytes`.
- `Event::ServerLog { time, msg }`.

## Active library — `markets().indexes_synchronized`

Ключевой инвариант для TradesStream/OrderBook. Поток:

```
1. Cold start или ServerRestart       → indexes_synchronized = false
                                        (TradesStream/OrderBook dropped silently)
2. dispatch_into_active отправляет
   api_get_markets_indexes (auto)
3. Сервер шлёт ответ через MPC_API   → markets.apply_markets_indexes ставит
                                        indexes_synchronized = true
4. Следующие TradesStream/OrderBook  → проходят gate, dispatch'ятся в state.
```

App может проверять статус через `dispatcher.markets().indexes_synchronized`
для UI индикатора "ещё загружаем рынки".

## Periodic trades tick

В режиме `run_with_dispatcher` либа **сама** вызывает `dispatcher.tick_trades(rtt, now)`
каждые ~100мс — это нужно для gap recovery (TradesResend retry с exponential backoff).

При **custom main loop** через `run + dispatch_into` потребитель должен вызывать
вручную:

```rust
let resend_payloads = dispatcher.tick_trades(client.round_trip_delay_ms(), now_ms);
for raw in resend_payloads {
    client.send_api_request(&raw);
}
```

Variant с emitted events:
```rust
let (resend_payloads, trade_events) = dispatcher.tick_trades_with_events(rtt, now);
// trade_events содержит BucketClosed / GapFilled — для observability.
```

## Multi-Client ServerTimeDelta

При multi-Client архитектуре `EventDispatcher` должен быть привязан к
конкретному Client'у через `Arc<AtomicU64>` handle — иначе все диспетчеры читают
один глобальный atomic и timestamps в Orders будут off (последний Client
перезаписывает delta всех остальных).

```rust
dispatcher.set_server_time_delta_source(client.server_time_delta_handle());
```

**Auto-link**: при `Client::run_with_dispatcher` или `dispatch_into_active(&mut client)`
линковка делается автоматически на первом вызове. Manual нужен только при
custom dispatch pattern'е без `&mut Client`. См. [multi_server.md](multi_server.md).

## Когда **не** использовать EventDispatcher

- Если нужен **только один канал** — проще вызвать парсер напрямую через `Client::run`.
- Если **state не нужен** (например trade-only logger) — `Client::run` raw достаточно.
- Если требуется **параллельная обработка нескольких подключений** с разделяемыми
  state — нужен свой dispatcher на каждый Client (см. multi_server.md), share state
  через `Arc<Mutex<...>>` в app layer.

## См. также

- [orders.md](orders.md), [order_books.md](order_books.md), [trades.md](trades.md),
  [balances.md](balances.md), [strats.md](strats.md), [markets.md](markets.md),
  [ui.md](ui.md), [arb.md](arb.md) — wire-формат каждого канала.
- [client.md](client.md) — Client transport + `run_with_dispatcher` + lifecycle.
- [engine_api.md](engine_api.md) — RPC requests/responses.
- [multi_server.md](multi_server.md) — multi-Client + per-Client ServerTimeDelta.
