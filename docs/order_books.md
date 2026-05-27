# OrderBooks

The orderbook channel delivers full snapshots and diffs for subscribed markets.
Use `MoonClient` subscription methods; the library handles cache ordering,
full-snapshot recovery, and the applied current-book read model.

## Subscribe

```rust
client.subscribe_orderbook("BTCUSDT")?;
client.subscribe_orderbooks(["ETHUSDT", "SOLUSDT"])?;
client.unsubscribe_orderbook("ETHUSDT")?;
client.unsubscribe_orderbooks(["SOLUSDT"])?;
client.unsubscribe_all_orderbooks()?;
```

Subscriptions are stored in the client registry. Before Init, public subscribe
and unsubscribe calls update only that registry and send nothing. The one-time
Init flushes the current registry once. After Init,
reconnect replays the registry automatically and refetches indexes when needed.
Orderbook replay waits until fresh `GetMarketsIndexes` has completed for the
current `PeerAppToken`, matching Delphi `CheckBookTopics`: packets using new
server indexes are not allowed to race old local index mappings.
After a reconnect, the library repeats the full registry subscribe batch every
5 seconds until a successful response confirms the current `ServerToken`. A
normal one-market subscribe response does not stop this reconnect replay unless
it is the first successful orderbook subscribe in the session.
Unsubscribe removes that market from the registry and sends the unsubscribe
request only after `domain_ready`.
Use the batched helpers when a UI toggles several markets at once; they preserve
one registry update and one batched Engine API request. Use
`unsubscribe_all_orderbooks` to clear the registry and send one batched
unsubscribe request for the market names that were remembered locally. If the
registry is already empty, it sends nothing.

The public call always updates the reconnect registry immediately. Once Init is
open, changed subscriptions also queue the server request. Its success or
failure is reported as a later `Event::EngineResponse`. Orderbook snapshots and
diffs then arrive asynchronously as `Event::OrderBook`.

The server subscription is per market name. `OrderBookKind` is not part of the
subscribe request; it is carried by incoming orderbook packets and by full-book
recovery requests.

## Events

```rust
use moonproto::Event;
use moonproto::state::OrderBookEvent;

for event in client.drain_events() {
    if let Event::OrderBook(book_event) = event {
        match book_event {
            OrderBookEvent::Apply {
                market_name,
                kind,
                is_full,
                seq,
                top,
                ..
            } => {
                redraw_top(market_name.as_deref(), kind, top, is_full, seq);
            }
            OrderBookEvent::Ignored { .. } => {}
            OrderBookEvent::RequestFullNeeded { .. } => {}
        }
    }
}
```

When using `MoonClient`, corrupted-cache
recovery is fully internal: the library requests a fresh full orderbook and does
not surface a separate callback event for that request. The low-level
`RequestFullNeeded` variant exists for manual `dispatch_into` / `OrderBooks`
users that do not pass a `Client` to the dispatcher.

The runtime applies each `Apply` event before the event is published. The event
already carries the resolved market name when it came through `MoonClient` /
`EventDispatcher`, the typed `OrderBookKind`, and the current best bid/ask after
the diff was applied:

```rust
use moonproto::Event;
use moonproto::state::OrderBookEvent;

for event in client.drain_events() {
    if let Event::OrderBook(OrderBookEvent::Apply { market_name, kind, top, .. }) = event {
        println!(
            "{} {kind:?} bid={:?} ask={:?}",
            market_name.as_deref().unwrap_or("<unknown>"),
            top.bid,
            top.ask,
        );
    }
}
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

pub enum ApplyResult {
    Applied,
    AppliedFromCache,
    Cached,
    Stale,
}

pub enum OrderBookEvent {
    Apply {
        market_index: u16,
        market_name: Option<String>,
        book_kind: u8,
        kind: OrderBookKind,
        is_full: bool,
        seq: u16,
        top: TopOfBook,
        buys: Vec<OrderLevel>,
        sells: Vec<OrderLevel>,
    },
    RequestFullNeeded { market_index: u16, book_kind: u8 },
    Ignored { market_index: u16, book_kind: u8, seq: u16, reason: ApplyResult },
}
```

`market_index`, raw `book_kind`, `buys`, and `sells` are kept for diagnostics
and low-level tools. For normal UI code, prefer `market_name`, `kind`, and
`top`; diff packet rows are not the full applied book. `RequestFullNeeded` is a
low-level control event. The active dispatcher consumes it internally before
invoking the application callback.

Incoming updates use `OrderLevel` (`f32`) because the server sends compact
single-precision values. `OrderBookSnapshot` stores applied levels as `f64` for
the current-book read model.

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
- Applied book depth is not capped by the Rust library; full snapshots and diffs
  keep the same level count that the Delphi active code keeps.

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

Regular applications should prefer `MoonClient`.
