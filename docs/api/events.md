# EventDispatcher

`EventDispatcher` turns decoded MoonProto channel payloads into typed `Event`
values and maintains read-only state models.

Regular applications receive these events from `MoonClient::drain_events`,
`try_recv_event`, or `recv_event_timeout`. `EventDispatcher` remains public for
custom low-level runtimes and tests.

## Recommended Use

```rust
use moonproto::Event;

for event in client.drain_events() {
    match event {
        Event::Order(order_event) => println!("order: {order_event:?}"),
        Event::OrderBook(book_event) => println!("book: {book_event:?}"),
        Event::Trade(trade_event) => println!("trade event: {trade_event:?}"),
        Event::Markets(markets_event) => println!("markets: {markets_event:?}"),
        Event::EngineResponse(resp) if !resp.success => {
            eprintln!("engine error {}: {}", resp.error_code, resp.error_msg);
        }
        Event::ParseFailed { cmd, len, payload } => {
            eprintln!("parse failed: {cmd:?}, {len} bytes, head={:02X?}", &payload[..payload.len().min(16)]);
        }
        _ => {}
    }
}
```

Use `MoonClient::snapshot` when UI code needs the latest read-only state:

```rust
if let Some(state) = client.snapshot() {
    println!("orders={}", state.orders().len());
}
```

`MoonClient` owns the protocol loop and runs until explicit `stop()` or drop.
Applications do not choose a protocol-loop duration. Decoded domain payloads are
handed to an internal dispatcher worker, which owns active-library parsing/state
apply and queues public events. UI code should update its own read model from
events and render from that local copy; `snapshot()` is a convenient immutable
read model, not a mutable state owner.

`MoonClient::snapshot` returns `EventDispatcherSnapshot`. It has the same
read-only getters used by UI code (`orders()`, `order_books()`, `trades()`,
`balances()`, `strats()`, `settings()`, `markets()`,
`strategy_snapshot_vec()`), but it is not the live dispatcher and cannot mutate
protocol state.

The client-level domain gate runs before dispatcher delivery. Until Init opens
`domain_ready`, `Order`, `Strat`, `Balance`, `TradesStream`,
`TradesResendResponse`, `OrderBook`, and `UI` packets are dropped and do not
become events. `API`, `LogMsg`, and transport service packets are not gated.
After Init, `TradesStream` and `TradesResendResponse` additionally require an
explicit all-trades subscription intent from `InitConfig::subscribe_trades` or
`Client::subscribe_all_trades`; otherwise they are treated as unexpected and
dropped.

`MoonClient` uses the active action path, which also:

- links the dispatcher to this client's `ServerTimeDelta`;
- resets per-session trades/orderbook state after server-token change;
- sends `RequestOrderBookFull` when an orderbook gap requires a full snapshot;
- emits strategy snapshot requests and, when a strategy snapshot provider is
  registered, sends a fresh application-owned snapshot reply;
- checks trades-gap recovery after successfully parsed live/resend trades
  packets and sends generated resend requests;
- queues decoded trades/MM/liquidation stream batches into retained history
  when an all-trades subscription is active. By default the dispatcher lazily
  owns a `MarketHistoryWorker`; `set_market_history_handle` is only for custom
  capacities or an externally owned worker.

Retained history is driven by the all-trades subscription:

```rust
client.subscribe_all_trades(false)?;
if let Some(snapshot) = client.snapshot() {
    let btc_tail = snapshot.markets().trade_state("BTCUSDT");
}
```

The worker owns retained stores and the runtime queues decoded stream batches
to it. `subscribe_all_trades` creates stores for known markets;
`subscribe_trades_for` creates stores only for the selected market names.
Custom low-level runtimes can still use `MarketHistoryWorker::spawn` and
`EventDispatcher::set_market_history_handle` before subscription when they need
custom capacities.

## Event Enum

```rust
pub enum Event {
    Order(OrderEvent),
    OrderBook(OrderBookEvent),
    /// One `TradesEvent` per packet sub-event. `Applied` is a lightweight
    /// signal; rows are already in market state / retained SeqRing storage.
    /// If a packet yields multiple `TradesEvent`s (e.g. `Applied` +
    /// `GapFilled`), each is pushed as its own `Event::Trade(...)`.
    Trade(TradesEvent),
    /// Typed HyperDex watcher fills from a TradesStream WatcherFills section.
    /// Time is already shifted the same way Delphi fills `TWSFill.Time`.
    WatcherFills(WatcherFillsEvent),
    Balance(BalanceEvent), // full/incremental balance read-model updates
    Arb { uid: u64, payload: ArbPayload },
    Strat(StratEvent),
    Settings(SettingsEvent),
    Markets(MarketsEvent),
    EngineResponse(EngineResponse),
    ServerLog { time: f64, msg: String },
    Raw { cmd: Command, payload: Vec<u8> },
    ParseFailed { cmd: Command, len: usize, payload: Vec<u8> },
}
```

`WatcherFillsEvent` contains `market_index`, `market_name`, the 20-byte
HyperDex `user` address, and `fills: Vec<WatcherFillEvent>`. Each fill carries
`time_ms`, `time`, `price`, `qty`, `z_btc`, `position`, raw `OrderType`, and
the decoded `is_short` / `is_open` / `is_taker` flags.

`Command::API` may produce two events: `Event::Markets(...)` when a
markets-related response was applied, followed by `Event::EngineResponse(...)`.
`Event::ParseFailed` carries the raw failed payload. The clone happens only on
the failure path and exists so live diagnostics can dump exact bytes instead of
guessing from `cmd/len`.

For `Command::UI`, future-version UI commands and unknown UI subcommand ids are
recognized by the low-level parser as `UICommand::Skipped` or
`UICommand::Unknown`, but the active dispatcher ignores them. They do not emit
`Event::Settings`.

`Command` identifies the decoded top-level channel. Known channels are
constants such as `Command::Order`; unknown channel bytes are preserved in
`Event::Raw`/`Event::ParseFailed` for diagnostics. Use
`Command::from_byte(raw)` and `cmd.to_byte()` for low-level access.

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

## Queued One-Shot Events

`MoonClient::request_*` helpers keep the runtime pumping while they wait and
return after the observed response/event has been applied to Active Lib state.
Unrelated packets received during that wait are still applied and later appear
through the normal event receiver:

```rust
let settings = client.request_client_settings(timeout)?;
for event in client.drain_events() {
    handle_event(event);
}
```

Low-level custom runtimes that own `Client + EventDispatcher` directly can use
the dispatcher's queued-event helpers instead of the `MoonClient` event
receiver.

## Channel Behavior

| Command | Dispatcher behavior |
|---|---|
| `Order` | Parses `TradeCommand`, applies `Orders`, emits `Event::Order`. `TAllStatuses` applies each contained status through the same order-command path, then emits a final `OrderEvent::Snapshot`. |
| `OrderBook` | Drops until market indexes are synchronized, applies `OrderBooks`, emits one or more `Event::OrderBook`. |
| `TradesStream` | Drops until market indexes are synchronized, applies `TradesState`, updates market tail / retained history, emits one `Event::Trade(TradesEvent)` per sub-event (`Applied` signal plus diagnostic gap/duplicate/out-of-order events). Duplicate packets also emit `Applied` for their payload. |
| `TradesResendResponse` | Parses the batch and applies each historical trades packet without advancing the live packet counter; late packets outside active buckets still emit `Applied` after `OutOfOrder`. |
| `Balance` | Applies full/incremental balance updates to `BalancesState`. Arbitrage relay payloads are exposed separately as typed `Event::Arb` after filtering records through the current server market-index map. Internal/base/request balance packets are consumed without user-facing events. |
| `Strat` | Applies strategy snapshot/update/delete state and emits `Event::Strat`. Future-version, unknown, and client-inapplicable incoming strat commands are skipped like Delphi base-class commands. |
| `UI` | Applies settings state and emits `Event::Settings`. Old append-only settings snapshots are parsed with the current settings snapshot as fallback. Inbound listing notifications are an internal refresh wake-up; user code sees `MarketsEvent::NewMarketsAdded` only after a refreshed market list actually inserts new markets. |
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
particular, direct `EventDispatcher::dispatch` / `dispatch_into` calls do not
know `Client::domain_ready` or the subscription registry. In normal
applications, prefer `MoonClient`.

For historical or truncated settings payloads, seed the dispatcher's local
settings fallback before dispatching:

```rust
dispatcher.set_client_settings_fallback(local_settings.clone());
```

Missing compatible tail fields keep the current local values instead of being
reset to Rust defaults. Each received full settings snapshot becomes the next
fallback automatically.

## Trades Tick

`tick_trades` and `tick_trades_with_events` are low-level hooks for custom loops.
Call them after a valid `TradesStream` / `TradesResendResponse` packet, using
the packet timestamp and current RTT. `MoonClient` and the low-level active
runtime path do this tail-check automatically. Gap lifecycle events are diagnostics for
logging/telemetry; the library performs recovery without requiring the
application to react to them.
