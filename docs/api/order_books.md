# OrderBooks

The orderbook channel delivers full snapshots and diffs for subscribed markets.
Use the high-level subscription API and `EventDispatcher`; the library handles
cache ordering, full-snapshot recovery, and the applied current-book read model.

## Subscribe

```rust
client.subscribe_orderbook("BTCUSDT");
client.subscribe_orderbooks(["ETHUSDT", "SOLUSDT"]);
client.unsubscribe_orderbook("ETHUSDT");
client.unsubscribe_orderbooks(["SOLUSDT"]);
client.unsubscribe_all_orderbooks();
```

Subscriptions are stored in the client registry. Before Init, reconnect does not
emit subscription traffic. After the one-time Init completes, reconnect replays
the registry automatically and refetches indexes when needed. Unsubscribe
removes that market from the registry and sends `emk_UnsubscribeOrderBook` when
the client loop is running.
Use the batched helpers when a UI toggles several markets at once; they preserve
one registry update and one batched Engine API request. Use
`unsubscribe_all_orderbooks` to clear the registry and send the protocol's
empty-market-list unsubscribe request.

The public call queues the subscription intent locally. On the wire,
`emk_SubscribeOrderBook` / `emk_UnsubscribeOrderBook` are Engine API requests
and their success or failure is reported as a later `Event::EngineResponse`.
Orderbook snapshots and diffs then arrive asynchronously on the `MPC_OrderBook`
stream.

The server subscription is per market name. `OrderBookKind` is not part of the
subscribe request; it is carried by incoming orderbook packets and by full-book
recovery requests.

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
            OrderBookEvent::Ignored { .. } => {}
            OrderBookEvent::RequestFullNeeded { .. } => {}
        }
    }
}));
```

When using `run_with_dispatcher` or `run_with_dispatcher_state`, corrupted-cache
recovery is fully internal: the library sends `emk_RequestOrderBookFull` and does
not surface a separate callback event for that request. The low-level
`RequestFullNeeded` variant exists for manual `dispatch_into` / `OrderBooks`
users that do not pass a `Client` to the dispatcher.

The dispatcher applies each `Apply` event before invoking the callback. If the
UI needs the current book, prefer `run_with_dispatcher_state` and read it from
`state.order_books()`:

```rust
use moonproto::{Event, EventDispatcher};
use moonproto::state::{OrderBookEvent, OrderBookKind};

client.run_with_dispatcher_state(duration, &mut dispatcher, Box::new(|event, state| {
    if let Event::OrderBook(OrderBookEvent::Apply { market_index, book_kind, .. }) = event {
        let Some(kind) = OrderBookKind::from_u8(*book_kind) else { return; };
        let Some(top) = state.order_books().top_of_book(*market_index, kind) else { return; };
        println!("bid={:?} ask={:?}", top.bid, top.ask);
    }
}));
```

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

pub struct OrderBookLevel {
    pub rate: f64,
    pub quantity: f64,
}

pub struct OrderBookSnapshot {
    pub market_index: u16,
    pub book_kind: u8,
    pub seq: u16,
    pub buys: Vec<OrderBookLevel>,
    pub sells: Vec<OrderBookLevel>,
}

pub struct TopOfBook {
    pub bid: Option<OrderBookLevel>,
    pub ask: Option<OrderBookLevel>,
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

`RequestFullNeeded` is a low-level control event. The active dispatcher consumes
it internally before invoking the application callback.

Wire updates use `OrderLevel` (`f32`) because the protocol writes Delphi
`Single` values. `OrderBookSnapshot` stores applied levels as `f64`, matching
Delphi `TOrderGlass` state.

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
