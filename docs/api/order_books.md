# OrderBooks channel (MPC_OrderBook)

Канал стаканов биржи: real-time обновления best-N bid/ask для подписанных маркетов.

## Что это

Сервер шлёт стакан как **Full** snapshot (полный набор уровней) или **Diff** (только изменения). Пакеты приходят пронумерованными по `seq` для каждой пары (`market_idx`, `book_kind`). Клиент должен:
- Применять Full → как новое состояние.
- Применять Diff → только если seq на 1 больше предыдущего.
- Если Diff пришёл раньше Full → запросить Full (`emk_RequestOrderBookFull`).
- Если seq нелинейный (gap) → отложить в буфер до прихода пропущенных, или сбросить и запросить Full.

Всё это автоматически делает `OrderBooks` sync state.

## Подписка

```rust
use moonproto::commands::engine_request::*;

// Подписаться на все стаканы:
let raw = build_subscribe_order_book();
client.send(MPC_API, &raw).await?;

// Запросить полный snapshot по конкретному маркету:
let raw = build_request_order_book_full(market_idx, book_kind);
client.send(MPC_API, &raw).await?;
```

`book_kind`: 0=Futures, 1=Spot (соответствует Delphi `TOrderBookKind`).

## Парсинг и применение

```rust
use moonproto::commands::order_book::parse_order_book_packet;
use moonproto::state::OrderBooks;
use std::time::SystemTime;

let mut books = OrderBooks::new();

if let Some(pkt) = parse_order_book_packet(&payload) {
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    let events = books.on_packet(pkt, now_ms);
    for ev in events {
        match ev {
            OrderBookEvent::Applied { market_idx, book_kind } => {
                // Стакан обновлён, можно перерисовать UI
                let snap = books.get(market_idx, book_kind).unwrap();
                redraw(snap.buy_levels, snap.sell_levels);
            }
            OrderBookEvent::RequestFullNeeded { market_idx, book_kind } => {
                // Diff пришёл до Full или потеря синхронизации.
                let raw = build_request_order_book_full(market_idx, book_kind);
                client.send(MPC_API, &raw).await?;
            }
        }
    }
}
```

## Логика OrderBooks state

- Per-(market_idx, book_kind) кэш `TOrderBookCache`.
- Reordering buffer: до 64 пакетов, insertion-sorted by seq.
- `BOOK_EXPIRED_TIMEOUT = 800ms` — старая Diff отбрасывается.
- `BOOK_FULL_REQUEST_THROTTLE = 5000ms` — повторный запрос Full не чаще раза в 5 секунд.
- Diff до Full → `RequestFullNeeded` (throttled).
- Gap в seq → ждать заполнения buffer, либо запросить Full.

## Структуры

```rust
pub struct OrderBookUpdate {
    pub market_idx: u16,
    pub book_kind: u8,        // 0=Futures, 1=Spot
    pub seq: u16,
    pub is_full: bool,         // true=Full snapshot, false=Diff
    pub buy_levels: Vec<OrderLevel>,
    pub sell_levels: Vec<OrderLevel>,
}

pub struct OrderLevel {
    pub price: f64,
    pub quantity: f64,
}
```

## Wire format

```
[MPC_OrderBook payload — SynLZ-compressed]:
  MarketIndex: u16
  Seq: u16
  Flags: u8        // bit 0: 1=Full, 0=Diff
  BuyCount: u8
  Buy[BuyCount]: { Price:f64, Qty:f64 } (16 bytes)
  Sell[N]: { Price:f64, Qty:f64 } (16 bytes) — N = (remaining_bytes / 16)
```

## См. также

- [trades.md](trades.md) — поток сделок (отдельная подписка)
- [engine_api.md](engine_api.md) — `subscribe_order_book`, `request_order_book_full`, `reload_order_book`
