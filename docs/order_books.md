# Order Books

MoonProto keeps an applied current order book for every subscribed market. The
application subscribes by market name; the library handles server indexes,
out-of-order diffs, full-book recovery, reconnect replay, and the retained
current-book state.

## Subscribe

```rust
client.subscribe_orderbook("BTCUSDT")?;
client.subscribe_orderbooks(["ETHUSDT", "SOLUSDT"])?;
client.unsubscribe_orderbook("ETHUSDT")?;
client.unsubscribe_orderbooks(["SOLUSDT"])?;
client.unsubscribe_all_orderbooks()?;
```

Subscriptions are stored in the reconnect registry. Before Init, these calls
only update the registry. After Init, changed subscriptions also queue the
server request. Reconnect replays the registry automatically after fresh market
indexes are known.

The server subscription is per market name. `OrderBookKind` is not part of the
subscribe request; it identifies incoming futures/spot book packets and current
snapshot reads.

## Events

```rust
use moonproto::Event;
use moonproto::state::OrderBookEvent;

for event in client.drain_events() {
    if let Event::OrderBook(OrderBookEvent::Apply {
        market_name,
        kind,
        is_full,
        seq,
        top,
        ..
    }) = event {
        redraw_top(
            market_name.as_deref().unwrap_or("<unknown>"),
            kind,
            top.bid,
            top.ask,
            is_full,
            seq,
        );
    }
}
```

`Apply` is emitted after the retained current book has already been updated.
For UI code, use the resolved `market_name`, typed `kind`, and `top` fields.
The event also contains decoded diff rows for diagnostics, but those rows are
not the full applied book.

`RequestFullNeeded` is a low-level recovery signal. `MoonClient` consumes it
internally and requests a fresh full book automatically.

## Reading Current Book

For rendering the full current book, read it from the latest snapshot by market
name:

```rust
use moonproto::state::OrderBookKind;

let Some(state) = client.snapshot() else { return; };

if let Some(book) = state.order_book("BTCUSDT", OrderBookKind::Futures) {
    draw_orderbook(&book.buys, &book.sells);
}

if let Some(top) = state.top_of_book("BTCUSDT", OrderBookKind::Futures) {
    draw_top(top.bid, top.ask);
}
```

UI-relevant fields on the applied current book:

```rust
pub struct OrderBookLevel {
    pub rate: f64,
    pub quantity: f64,
}

pub struct OrderBookSnapshot {
    // raw index fields are retained for diagnostics
    pub seq: u16,
    pub buys: Vec<OrderBookLevel>,
    pub sells: Vec<OrderBookLevel>,
}

pub struct TopOfBook {
    pub bid: Option<OrderBookLevel>,
    pub ask: Option<OrderBookLevel>,
}
```

Incoming wire rows are compact `f32`, but the retained current-book snapshot
stores levels as `f64`.

## Recovery

The orderbook state keeps one reorder cache per market/kind pair:

- a full snapshot resets sequence state;
- an in-order diff applies immediately;
- out-of-order diffs are cached;
- stale diffs are ignored;
- long-lived gaps switch the cache to corrupted mode and request a full book no
  more often than every 5 seconds;
- the first diff can apply without a prior full snapshot when the local sequence
  is zero, matching the server behavior.

After reconnect, orderbook packets are ignored until fresh market indexes are
synchronized for the current server session. This prevents a new server
`market_index` map from racing old local indexes.

## Low-Level Tools

`commands::order_book::parse_order_book_packet`, `OrderBookUpdate`, raw
`market_index`, and raw `book_kind` are for protocol tests, replay tools, and
custom runtimes. Regular applications should subscribe by name, react to
`OrderBookEvent::Apply`, and read current books from `MoonClient` snapshots.
