# OrderBooks

The orderbook channel delivers full snapshots and diffs for subscribed markets.
Use the high-level subscription API and `EventDispatcher`; the library handles
cache ordering and full-snapshot recovery.

## Subscribe

```rust
use moonproto::state::OrderBookKind;

client.subscribe_orderbook("BTCUSDT", OrderBookKind::Futures);
client.subscribe_orderbook("ETHUSDT", OrderBookKind::Spot);
```

Subscriptions are stored in the client registry and replayed automatically after
hard reconnect. Do not resubscribe from `ServerRestart`.

## Events

```rust
use moonproto::{Event, EventDispatcher};
use moonproto::state::OrderBookEvent;

client.run_with_dispatcher(duration, &mut dispatcher, Box::new(|event| {
    if let Event::OrderBook(book_event) = event {
        match book_event {
            OrderBookEvent::Apply {
                market_index,
                book_kind,
                is_full,
                seq,
                buys,
                sells,
            } => {
                redraw_book(*market_index, *book_kind, *is_full, *seq, buys, sells);
            }
            OrderBookEvent::RequestFullNeeded { market_index, book_kind } => {
                show_loading_book(*market_index, *book_kind);
            }
            OrderBookEvent::Ignored { .. } => {}
        }
    }
}));
```

`RequestFullNeeded` is emitted for UI awareness. When using
`run_with_dispatcher`, the actual `emk_RequestOrderBookFull` request is already
sent by the library.

## Public Types

```rust
pub enum OrderBookKind {
    Futures = 0,
    Spot = 1,
}

pub struct OrderBookUpdate {
    pub market_index: u16,
    pub seq: u16,
    pub is_full: bool,
    pub book_kind: u8,
    pub buys: Vec<OrderLevel>,
    pub sells: Vec<OrderLevel>,
}

pub struct OrderLevel {
    pub rate: f32,
    pub quantity: f32,
}

pub enum OrderBookEvent {
    Apply {
        market_index: u16,
        book_kind: u8,
        is_full: bool,
        seq: u16,
        buys: Vec<OrderLevel>,
        sells: Vec<OrderLevel>,
    },
    RequestFullNeeded { market_index: u16, book_kind: u8 },
    Ignored { market_index: u16, book_kind: u8, seq: u16, reason: ApplyResult },
}
```

## Recovery Behavior

`OrderBooks` keeps an independent cache per `(market_index, book_kind)`.

- Full snapshot resets the sequence state.
- Sequential diff is applied immediately.
- Out-of-order diff is cached.
- Stale diff is ignored.
- Long-lived cache gaps enter corrupted mode and request a full snapshot no more
  often than every 5 seconds.
- The first diff may be applied without a prior full snapshot when the local
  sequence is still zero, matching the server-side Delphi behavior.

`EventDispatcher` drops orderbook packets until `MarketsState.indexes_synchronized`
is true, preventing new server indexes from being applied to old local mappings
after a restart.

## Low-Level Parser

Advanced tools can parse payloads directly:

```rust
use moonproto::commands::order_book::parse_order_book_packet;
use moonproto::state::OrderBooks;

let mut books = OrderBooks::new();
let packet = parse_order_book_packet(payload).expect("bad orderbook packet");
let events = books.on_packet(packet, now_ms);
```

Regular applications should prefer `Client::run_with_dispatcher`.
