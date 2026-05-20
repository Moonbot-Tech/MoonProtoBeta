# OrderBooks channel (MPC_OrderBook)

Канал стаканов биржи: real-time best-N bid/ask для подписанных маркетов.

## Что это

Сервер шлёт стакан как **Full** snapshot (полный набор уровней) или **Diff**
(только изменения). Пакеты приходят пронумерованными по `seq: u16` (wrapping)
для каждой пары `(market_index, book_kind)`. Клиент должен:

- Применять Full → как новое состояние.
- Применять Diff → только если seq на 1 больше предыдущего.
- Если Diff пришёл раньше Full → запросить Full (`emk_RequestOrderBookFull`).
- Если seq нелинейный (gap) → отложить в кэш до прихода пропущенных, либо
  сбросить и запросить Full.

Всё это автоматически делает `OrderBooks` sync state. При `run_with_dispatcher`
`EventDispatcher::dispatch_into_active` сам отправляет `RequestOrderBookFull`
при corruption (с dedup).

## Подписка

```rust
use moonproto::state::OrderBookKind;

// Через ClientSender (thread-safe из любого thread'а):
let sender = client.sender();
sender.subscribe_orderbook("BTCUSDT", OrderBookKind::Futures);
sender.subscribe_orderbook("ETHUSDT", OrderBookKind::Spot);

// Или сразу на &Client:
client.subscribe_orderbook("BTCUSDT", OrderBookKind::Futures);

// Отписаться:
client.unsubscribe_orderbook("BTCUSDT", OrderBookKind::Futures);
```

`OrderBookKind`:

```rust
#[repr(u8)]
pub enum OrderBookKind {
    Futures = 0,
    Spot    = 1,
}
```

**Liба сама** auto-replay'ит подписки через `subscription_registry` после любого
hard-reconnect / ServerToken change. App **не должно** дублировать на
`LifecycleEvent::ServerRestart`.

## EventDispatcher (рекомендуемый pattern)

```rust
use moonproto::events::{EventDispatcher, Event};
use moonproto::state::OrderBookEvent;

let mut dispatcher = EventDispatcher::new();

client.run_with_dispatcher(duration, &mut dispatcher, Box::new(|ev| match ev {
    Event::OrderBook(OrderBookEvent::Apply { market_index, book_kind, is_full, seq, buys, sells }) => {
        // Перерисовать стакан в UI.
        if is_full {
            ui.replace_orderbook(market_index, book_kind, buys, sells);
        } else {
            ui.apply_diff(market_index, book_kind, buys, sells, seq);
        }
    }
    Event::OrderBook(OrderBookEvent::RequestFullNeeded { market_index, book_kind }) => {
        // НЕ нужно ничего слать — dispatch_into_active уже отправил api_request_order_book_full.
        // Event для UI awareness: "загружаем стакан BTCUSDT".
        ui.show_loading(market_index, book_kind);
    }
    Event::OrderBook(OrderBookEvent::Ignored { reason, .. }) => {
        // stale / no_full_yet / cached — обычно ничего делать не нужно.
        log::trace!("orderbook ignored: {reason:?}");
    }
    _ => {}
}));
```

**Active library**: `RequestFullNeeded` автоматически дедуплицируется в пределах
одного dispatch вызова (Grouped payload может содержать несколько RequestFullNeeded
для одной книги — шлём один запрос).

## Низкоуровневый pattern (без EventDispatcher)

```rust
use moonproto::commands::order_book::parse_order_book_packet;
use moonproto::commands::engine_request::request_order_book_full;
use moonproto::state::{OrderBooks, OrderBookEvent};

let mut state = OrderBooks::new();
let now_ms = /* current ms */;

if let Some(pkt) = parse_order_book_packet(&payload) {
    let events = state.on_packet(pkt, now_ms);
    for ev in events {
        match ev {
            OrderBookEvent::Apply { market_index, is_full, seq, buys, sells, .. } => {
                // Обновить локальную модель.
            }
            OrderBookEvent::RequestFullNeeded { market_index, book_kind } => {
                let raw = request_order_book_full(market_index, book_kind);
                client.send_api_request(&raw);
            }
            OrderBookEvent::Ignored { .. } => { /* skip */ }
        }
    }
}
```

## OrderBookEvent

```rust
pub enum OrderBookEvent {
    /// Пакет применён — обновить локальный orderbook.
    Apply {
        market_index: u16,
        book_kind:    u8,            // 0=Futures, 1=Spot (raw для wire compat)
        is_full:      bool,
        seq:          u16,
        buys:         Vec<OrderLevel>,
        sells:        Vec<OrderLevel>,
    },
    /// Нужно отправить emk_RequestOrderBookFull (throttle уже учтён).
    /// При dispatch_into_active либа отправит сама — это event только для UI awareness.
    RequestFullNeeded {
        market_index: u16,
        book_kind:    u8,
    },
    /// Пакет проигнорирован (stale / no full yet / cached).
    Ignored {
        market_index: u16,
        book_kind:    u8,
        seq:          u16,
        reason:       ApplyResult,
    },
}

pub enum ApplyResult {
    Applied,            // seq == expected, применили
    AppliedFromCache,   // cached → drain → применили
    Cached,             // seq > expected, отложили
    Stale,              // seq < expected, отбросили
    NoFullYet,          // diff до первого Full — отбросили
}
```

## OrderLevel structure

```rust
pub struct OrderLevel {
    pub rate:     f32,    // цена уровня (НЕ f64 — wire 4 байта)
    pub quantity: f32,    // объём на уровне
}
```

## OrderBookUpdate (внутренняя)

```rust
pub struct OrderBookUpdate {
    pub market_index: u16,
    pub seq:          u16,
    pub is_full:      bool,
    pub book_kind:    u8,
    pub buys:         Vec<OrderLevel>,
    pub sells:        Vec<OrderLevel>,
}
```

Получается из `parse_order_book_packet(raw)` — раскрывает SynLZ + парсит wire-format.

## Логика OrderBooks state

Per-`(market_index, book_kind)` кэш `OrderBookCache`:

- **Reordering buffer** до 64 пакетов (`BOOK_CACHE_MAX_PACKETS`), insertion-sorted by seq.
- `BOOK_EXPIRED_TIMEOUT = 800ms` — старый Diff в кэше отбрасывается → mark corrupted.
- `BOOK_FULL_REQUEST_THROTTLE = 5000ms` — повторный запрос Full не чаще 1×/5с.
- **Diff до Full** → `RequestFullNeeded` (throttled).
- **Gap в seq** → ждать заполнения кэша; при превышении BOOK_CACHE_MAX_PACKETS →
  mark corrupted → request Full.
- **MAX_ORDERBOOK_CACHES = 4096** — DoS guard на количество (market_index, book_kind)
  ключей. При превышении — LRU evict по `last_apply_ms`.

## Wire format

```
[MPC_OrderBook payload — SynLZ-compressed]:
  MarketIndex: u16 LE
  Seq:         u16 LE
  Flags:       u8           // bit 0: 1=Full/0=Diff; bit 1: book_kind (0=Futures/1=Spot)
  BuyCount:    u16 LE
  Buys[BuyCount]: { rate:f32, qty:f32 } (8 bytes each)
  Sells[N]:       { rate:f32, qty:f32 } (8 bytes each)   // N = remaining / 8
```

## Active library — gate через `MarketsState.indexes_synchronized`

`EventDispatcher::dispatch_into` для `Command::OrderBook` **молча дропает** пакет
если `markets.indexes_synchronized = false`:

```rust
if !self.markets.indexes_synchronized {
    return;
}
```

Это критичный инвариант: `market_index` от сервера может быть по **новой
нумерации** после server restart. Без resync применили бы к старой таблице
by_index → silent data corruption в UI.

После `markets.apply_markets_indexes(...)` гейт снимается, входящие пакеты
парсятся как обычно.

App может проверять статус через `dispatcher.markets().indexes_synchronized`
для UI индикатора "ещё загружаем рынки".

## См. также

- [trades.md](trades.md) — поток сделок (отдельный канал, тоже подписка).
- [engine_api.md](engine_api.md) — `api_subscribe_order_book`, `api_request_order_book_full`,
  `api_reload_order_book`.
- [events.md](events.md) — EventDispatcher автоматизирует auto-request Full.
- [markets.md](markets.md) — `indexes_synchronized` инвариант.
