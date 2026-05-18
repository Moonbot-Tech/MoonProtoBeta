# EventDispatcher — auto-apply state

`EventDispatcher` — высокоуровневая обёртка поверх `Client::on_data` callback'а. Парсит входящие пакеты по каналам, автоматически применяет к sync-state'ам и возвращает типизированные `Event` потребителю.

## Зачем

Без `EventDispatcher` потребитель пишет руками для каждого канала:

```rust
client.on_data(|cmd, payload| {
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
                // ...
            }
        }
        Command::Balance => {
            // sub-cmd 2/3/4 vs 6 dispatch...
        }
        // ... 8 каналов вручную ...
    }
});
```

С `EventDispatcher` — одна строка:

```rust
let mut dispatcher = EventDispatcher::new();
client.on_data(move |cmd, payload| {
    for ev in dispatcher.dispatch(cmd, payload, current_ms()) {
        match ev { /* ... */ }
    }
});
```

## Структура

```rust
pub struct EventDispatcher {
    pub orders:      Orders,
    pub order_books: OrderBooks,
    pub trades:      TradesState,
    pub balances:    BalancesState,
    pub strats:      StratsState,
    pub settings:    SettingsState,
    pub markets:     MarketsState,
}
```

Все state'ы — public, можно читать напрямую (`dispatcher.orders.by_id`, `dispatcher.markets.by_name`, etc.).

## API

```rust
impl EventDispatcher {
    pub fn new() -> Self;
    pub fn dispatch(&mut self, cmd: Command, payload: &[u8], now_ms: i64) -> Vec<Event>;
}
```

Один пакет может породить **несколько событий** (Vec):
- `OrderBook` channel при drain'е reordering cache → несколько `Event::OrderBook`.
- `TradesResendResponse` (batch) → одно `Event::Trades` на каждый вложенный payload.
- `Command::API` с auto-apply markets → 2 события: `Event::Markets` + `Event::EngineResponse`.

## Event enum

```rust
pub enum Event {
    Order(OrderEvent),
    OrderBook(OrderBookEvent),
    Trades(Vec<TradesEvent>),
    Balance(BalanceEvent),
    Arb { uid: u64, payload: Vec<u8> },
    Strat(StratEvent),
    Settings(SettingsEvent),
    Markets(MarketsEvent),
    EngineResponse(EngineResponse),
    ServerLog { time: f64, msg: String },
    Raw { cmd: Command, payload: Vec<u8> },
    ParseFailed { cmd: Command, len: usize },
}
```

| Variant | Канал | Что |
|---|---|---|
| `Order` | MPC_Order | Создан / обновлён / удалён ордер |
| `OrderBook` | MPC_OrderBook | Применён snapshot или диф; либо `RequestFullNeeded` если gap |
| `Trades(Vec)` | MPC_TradesStream / MPC_TradesResendResponse | Поток сделок (последовательный, gap, duplicate, etc.) |
| `Balance` | MPC_Balance sub_cmd_id 2/3/4 | Обновление балансов |
| `Arb` | MPC_Balance sub_cmd_id 6 | Raw arb-prices payload (декодер — Stage 3+) |
| `Strat` | MPC_Strat | Snapshot, delete, sell-price update, checked-sync |
| `Settings` | MPC_UI | 14 UI подкоманд |
| `Markets` | MPC_API (GetMarketsList/UpdateMarketsList/etc.) | Auto-apply ответа на MarketsState |
| `EngineResponse` | MPC_API | RPC response (если не markets-related или одновременно с `Markets`) |
| `ServerLog` | MPC_LogMsg | `[server] <time>: <msg>` |
| `Raw` | прочие | Канал без специальной обработки (fallback) |
| `ParseFailed` | любой | Payload не распарсился (повреждение / wrong version) |

## Пример

```rust
use moonproto::events::{EventDispatcher, Event};
use moonproto::state::OrderEvent;

let mut dispatcher = EventDispatcher::new();

client.on_data(move |cmd, payload| {
    let now_ms = chrono::Utc::now().timestamp_millis();
    for ev in dispatcher.dispatch(cmd, payload, now_ms) {
        match ev {
            Event::Order(OrderEvent::Created(order)) => {
                println!("New order {}: {} @ {}", order.uid, order.market_name, order.buy_order.price);
            }
            Event::Order(OrderEvent::Updated(uid)) => {
                if let Some(o) = dispatcher.orders.by_id.get(&uid) {
                    println!("Order {} updated, status: {:?}", uid, o.status);
                }
            }
            Event::Order(OrderEvent::Removed(uid)) => {
                println!("Order {} closed", uid);
            }
            Event::OrderBook(ob_ev) => {
                // Перерисовать стакан
            }
            Event::Trades(trade_events) => {
                for te in trade_events {
                    // Обработать каждое event пакета трейдов
                }
            }
            Event::ServerLog { time, msg } => {
                eprintln!("[server@{}] {}", time, msg);
            }
            Event::ParseFailed { cmd, len } => {
                eprintln!("WARN: failed to parse {} bytes for {:?}", len, cmd);
            }
            _ => {}
        }
    }
});
```

## Семантика per-канал

### `Command::Order`
- Парсит через `TradeCommand::parse(payload)`.
- Применяет `self.orders.apply(cmd)` → `OrderEvent`.
- Один `Event::Order` на пакет.

### `Command::OrderBook`
- Парсит `parse_order_book_packet(payload)`.
- Применяет `self.order_books.on_packet(pkt, now_ms)`.
- **Может быть несколько `Event::OrderBook`** если drain reorder cache.

### `Command::TradesStream`
- Парсит `parse_trades_packet(payload)`.
- `self.trades.on_packet(pkt, now_ms)` → `Vec<TradesEvent>`.
- Один `Event::Trades(events)` на пакет.

### `Command::TradesResendResponse`
- Batch: `parse_trades_resend_response(payload)` → `Vec<inner_payloads>`.
- Для каждого inner: `parse_trades_packet` → `trades.on_packet_resend` (НЕ двигает `last_packet_num`).
- Один `Event::Trades` на каждый inner.

### `Command::Balance`
Sub-cmd dispatch по `payload[0]` (CmdId внутри header):
- `2 | 3 | 4` → `parse_balance(sub, &payload[11..])` → `balances.apply` → `Event::Balance`.
- `6` → `parse_arb_prices(payload)` → `Event::Arb { uid, payload }`.
- Иначе → `Event::Raw`.

### `Command::API` (Engine RPC response)
Если успешный `EngineResponse` и `method` — markets-related, **auto-apply** к `MarketsState` + эмит `Event::Markets` + `Event::EngineResponse` (двойное событие).

Auto-applied методы:
- `GetMarketsList` → `apply_markets_list`
- `UpdateMarketsList` → `apply_markets_prices`
- `GetMarketsIndexes` → `apply_markets_indexes`
- `CheckBinanceTags` → `apply_token_tags`

Прочие методы → один `Event::EngineResponse`.

**NB про версию:** парсер `parse_markets_list_response` требует `ver` параметр (v2 добавила FuturesType byte). EventDispatcher использует `ASSUMED_VER=2` (текущая версия live-сервера). Если сервер обновится — править `events.rs`.

### `Command::LogMsg`
- Парсит `time:f64 + utf8 bytes`.
- `Event::ServerLog { time, msg }`.
- Matches `MoonProtoClient.pas:298-306`.

## Когда НЕ использовать EventDispatcher

- Если нужен **только один канал** — проще вызвать парсер напрямую.
- Если **state не нужен** (например trade-only logger) — `on_data` raw достаточно.
- Если требуется **параллельная обработка нескольких подключений** с разделяемыми state — нужен свой dispatcher с `Arc<Mutex<...>>`.

## См. также

- [orders.md](orders.md), [order_books.md](order_books.md), [trades.md](trades.md), [balances.md](balances.md), [strats.md](strats.md), [markets.md](markets.md), [ui.md](ui.md), [arb.md](arb.md) — wire-формат каждого канала.
- [client.md](client.md) — Client transport + `api_pending` + lifecycle.
- [engine_api.md](engine_api.md) — RPC requests/responses.
