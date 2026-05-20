# EventDispatcher

`EventDispatcher` turns decoded MoonProto channel payloads into typed `Event`
values and maintains read-only state models.

Use it through `Client::run_with_dispatcher` unless you are writing a custom
low-level loop.

## Recommended Use

```rust
use moonproto::{Event, EventDispatcher};

let mut dispatcher = EventDispatcher::new();

client.run_with_dispatcher(duration, &mut dispatcher, Box::new(|event| {
    match event {
        Event::Order(order_event) => println!("order: {order_event:?}"),
        Event::OrderBook(book_event) => println!("book: {book_event:?}"),
        Event::Trade(trade_event) => println!("trade event: {trade_event:?}"),
        Event::Markets(markets_event) => println!("markets: {markets_event:?}"),
        Event::EngineResponse(resp) if !resp.success => {
            eprintln!("engine error {}: {}", resp.error_code, resp.error_msg);
        }
        Event::ParseFailed { cmd, len } => eprintln!("parse failed: {cmd:?}, {len} bytes"),
        _ => {}
    }
}));
```

Use `Client::run_with_dispatcher_state` when the callback needs the read-only
state after the event has been applied:

```rust
client.run_with_dispatcher_state(duration, &mut dispatcher, Box::new(|event, state| {
    if let Event::Order(order_event) = event {
        println!("orders={}", state.orders().len());
        let _ = order_event;
    }
}));
```

`run_with_dispatcher` calls `dispatch_into_active`, which also:

- links the dispatcher to this client's `ServerTimeDelta`;
- resets per-session trades/orderbook state after server-token change;
- sends `RequestOrderBookFull` when an orderbook gap requires a full snapshot;
- emits strategy snapshot requests and, when a strategy snapshot provider is
  registered, sends a fresh application-owned snapshot reply;
- cooperates with the client loop's periodic trades-gap tick.

## Event Enum

```rust
pub enum Event {
    Order(OrderEvent),
    OrderBook(OrderBookEvent),
    /// One `TradesEvent` per packet sub-event. If a packet yields multiple
    /// `TradesEvent`s (e.g. `Apply` + `GapFilled`), each is pushed as its own
    /// `Event::Trade(...)` — no nested `Vec` allocation (audit_rust_quality #11).
    Trade(TradesEvent),
    Balance(BalanceEvent),
    Arb { uid: u64, payload: ArbPayload },
    Strat(StratEvent),
    Settings(SettingsEvent),
    Markets(MarketsEvent),
    EngineResponse(EngineResponse),
    ServerLog { time: f64, msg: String },
    Raw { cmd: Command, payload: Vec<u8> },
    ParseFailed { cmd: Command, len: usize },
}
```

`Command::API` may produce two events: `Event::Markets(...)` when a
markets-related response was applied, followed by `Event::EngineResponse(...)`.

## Reading State

State fields are encapsulated. Read them through getters:

```rust
if let Some(order) = dispatcher.orders().get(order_uid) {
    println!("{} {:?}", order.market_name, order.status);
}

if let Some(price) = dispatcher.markets().price("BTCUSDT") {
    println!("bid={} ask={}", price.bid, price.ask);
}

println!("orders={}", dispatcher.orders().len());
println!("markets={}", dispatcher.markets().market_count());
```

The dispatcher updates these states automatically when relevant packets arrive.

## Channel Behavior

| Command | Dispatcher behavior |
|---|---|
| `Order` | Parses `TradeCommand`, applies `Orders`, emits `Event::Order`. |
| `OrderBook` | Drops until market indexes are synchronized, applies `OrderBooks`, emits one or more `Event::OrderBook`. |
| `TradesStream` | Drops until market indexes are synchronized, applies `TradesState`, emits one `Event::Trade(TradesEvent)` per sub-event (`Apply` / `GapDetected` / `Duplicate` / `OutOfOrder` / `GapFilled` / `BucketClosed`). Duplicate packets also emit `Apply` for their payload. |
| `TradesResendResponse` | Parses the batch and applies each historical trades packet without advancing the live packet counter; late packets outside active buckets still emit `Apply` after `OutOfOrder`. |
| `Balance` | Applies balance subcommands `2/3/4`; subcommand `6` becomes typed `Event::Arb`. |
| `Strat` | Applies strategy snapshot/update/delete state and emits `Event::Strat`. |
| `UI` | Applies settings state and emits `Event::Settings`. |
| `API` | Parses `EngineResponse`; applies markets responses when the method is markets-related. |
| `LogMsg` | Emits `Event::ServerLog`. |

## Low-Level Dispatch

`dispatch` and `dispatch_into` remain public for tools that already own a custom
main loop:

```rust
let mut out = Vec::with_capacity(8);
out.clear();
dispatcher.dispatch_into(cmd, payload, now_ms, &mut out);
```

If you call these directly, you do not get `Client`-backed auto-actions. In
normal applications, prefer `Client::run_with_dispatcher`.

## Trades Tick

`tick_trades` and `tick_trades_with_events` are low-level hooks for custom loops.
`Client::run_with_dispatcher` calls the tick automatically about every 100 ms and
emits tick-generated `BucketClosed` diagnostics through the normal
`Event::Trade(...)` callback.
