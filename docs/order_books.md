# Order Books

MoonProto keeps an applied current order book for every subscribed market. The
application subscribes by market name; the library handles server indexes,
out-of-order diffs, full-book recovery, reconnect replay, and the retained
current-book state.

## Subscribe

```rust
client.streams().subscribe_orderbook("BTCUSDT")?;
client.streams().subscribe_orderbooks(["ETHUSDT", "SOLUSDT"])?;
client.streams().unsubscribe_orderbook("ETHUSDT")?;
client.streams().unsubscribe_orderbooks(["SOLUSDT"])?;
client.streams().unsubscribe_all_orderbooks()?;
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
        top,
        ..
    }) = event {
        redraw_top(
            market_name.as_deref().unwrap_or("<unknown>"),
            kind,
            top.bid,
            top.ask,
            is_full,
        );
    }
}
```

`Apply` is emitted after the retained current book has already been updated.
For UI code, use the resolved `market_name`, typed `kind`, and `top` fields.
For the full book, read the retained `OrderBookSnapshot` by market name.

Recovery control and ignored-packet notifications are hidden diagnostic
telemetry. `MoonClient` consumes them internally and requests fresh full books
automatically. Application code observes the maintained book snapshot and
applied top-of-book events.

## Reading Current Book

For rendering the full current book, keep the selected `MarketHandle` and read
from the latest snapshot through that handle:

```rust
use moonproto::state::OrderBookKind;

let Some(state) = client.snapshot() else { return; };
let Some(market) = state.markets().get("BTCUSDT") else { return; };

if let Some(book) = state.order_book_for(&market, OrderBookKind::Futures) {
    draw_orderbook(&book.buys, &book.sells);
}

if let Some(top) = state.top_of_book_for(&market, OrderBookKind::Futures) {
    draw_top(top.bid, top.ask);
}
```

Use `OrderBookKind::as_str()` for stable UI/log labels.

UI-relevant fields on the applied current book:

```rust
pub struct OrderBookLevel {
    pub rate: f64,
    pub quantity: f64,
}

pub struct OrderBookSnapshot {
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

## Protocol Data

The internal orderbook parser, `OrderBookUpdate`, raw packet rows, raw
`market_index`, and raw orderbook kind bytes are for protocol tests and replay
tools. Regular applications should subscribe by name, react to
`OrderBookEvent::Apply`, and read current books from `MoonClient` snapshots.
