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
        Event::ParseFailed { cmd, len, payload } => {
            eprintln!("parse failed: {cmd:?}, {len} bytes, head={:02X?}", &payload[..payload.len().min(16)]);
        }
        _ => {}
    }
}));
```

Use `Client::run_with_dispatcher_state` when the callback needs a read-only
state snapshot after the event has been applied:

```rust
client.run_with_dispatcher_state(duration, &mut dispatcher, Box::new(|event, state| {
    if let Event::Order(order_event) = event {
        println!("orders={}", state.orders().len());
        let _ = order_event;
    }
}));
```

`run_with_dispatcher` and `run_with_dispatcher_state` block the caller for the
requested duration while the MoonProto protocol loop runs on that caller thread.
Event callbacks run through an application
callback queue after protocol state is updated. Slow callbacks delay return from
the run call because the queue is drained before return, but they do not block
ACK/retry/send progress inside the protocol loop. For
`run_with_dispatcher_state`, building the state snapshot is still protocol-loop
work; use the plain event callback for hot paths that do not need state reads.

`run_with_dispatcher_state` receives `EventDispatcherSnapshot`. It has the same
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

`run_with_dispatcher` uses the active action path, which also:

- links the dispatcher to this client's `ServerTimeDelta`;
- resets per-session trades/orderbook state after server-token change;
- sends `RequestOrderBookFull` when an orderbook gap requires a full snapshot;
- emits strategy snapshot requests and, when a strategy snapshot provider is
  registered, sends a fresh application-owned snapshot reply;
- checks trades-gap recovery after successfully parsed `TradesStream` /
  `TradesResendResponse` packets and sends generated `emk_TradesResend`
  requests;
- queues decoded trades/MM/liquidation stream batches into the optional
  retained-history worker when `set_market_history_handle` is configured.

Retained history is opt-in:

```rust
use moonproto::EventDispatcher;
use moonproto::state::{MarketHistoryConfig, MarketHistoryWorker};

let worker = MarketHistoryWorker::spawn(MarketHistoryConfig::default());
let btc = worker.ensure_market("BTCUSDT").expect("history worker alive");

let mut dispatcher = EventDispatcher::new();
dispatcher.set_market_history_handle(worker.handle());
```

The worker owns retained stores and the dispatcher only queues decoded stream
batches to it. Stream packets do not allocate history for unknown/not-enabled
markets; call `ensure_market` for each market you want retained readers for.

## Event Enum

```rust
pub enum Event {
    Order(OrderEvent),
    OrderBook(OrderBookEvent),
    /// One `TradesEvent` per packet sub-event. If a packet yields multiple
    /// `TradesEvent`s (e.g. `Apply` + `GapFilled`), each is pushed as its own
    /// `Event::Trade(...)` — no nested `Vec` allocation (audit_rust_quality #11).
    Trade(TradesEvent),
    Balance(BalanceEvent), // full/incremental balance updates (cmd_id 3/4)
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

`Command::API` may produce two events: `Event::Markets(...)` when a
markets-related response was applied, followed by `Event::EngineResponse(...)`.
`Event::ParseFailed` carries the raw failed payload. The clone happens only on
the failure path and exists so live diagnostics can dump exact bytes instead of
guessing from `cmd/len`.

`Command` is a raw one-byte Delphi `TMoonProtoCommand` ordinal wrapper. Known
channels are constants such as `Command::Order`; unknown channel bytes are
preserved in `Event::Raw`/`Event::ParseFailed`. Use `Command::from_byte(raw)`
and `cmd.to_byte()` for raw access.

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

One-shot helpers such as `Client::request_balance`,
`Client::request_client_settings`, `Client::request_order_snapshot`,
`Client::request_balance_snapshot`, and `Client::run_until_response` keep the
UDP loop running while they wait. If unrelated packets arrive during that wait,
their state changes are applied immediately and the produced `Event` values are
stored in the dispatcher:

```rust
let settings = client.request_client_settings(&mut dispatcher, timeout)?;
for event in dispatcher.take_queued_events() {
    handle_event(event);
}
```

Use `queued_events()` for a borrowed view, `queued_event_count()` and
`queued_event_max_count()` for diagnostics, `take_queued_events()` to drain, and
`clear_queued_events()` to discard. The queue has no fixed capacity and no drop
policy; if it grows, diagnostics report that fact instead of losing events.
`Client::run_with_dispatcher` does not use this queue because it delivers events
directly to its callback.

## Channel Behavior

| Command | Dispatcher behavior |
|---|---|
| `Order` | Parses `TradeCommand`, applies `Orders`, emits `Event::Order`. `TAllStatuses` applies each contained status through the same order-command path, then emits a final `OrderEvent::Snapshot`. |
| `OrderBook` | Drops until market indexes are synchronized, applies `OrderBooks`, emits one or more `Event::OrderBook`. |
| `TradesStream` | Drops until market indexes are synchronized, applies `TradesState`, emits one `Event::Trade(TradesEvent)` per sub-event (`Apply` plus diagnostic gap/duplicate/out-of-order events). Duplicate packets also emit `Apply` for their payload. |
| `TradesResendResponse` | Parses the batch and applies each historical trades packet without advancing the live packet counter; late packets outside active buckets still emit `Apply` after `OutOfOrder`. |
| `Balance` | Parses subcommand `2` but does not mutate state; applies subcommands `3/4`; subcommand `6` becomes typed `Event::Arb` after filtering arb records through the current server `mIndex` map. |
| `Strat` | Applies strategy snapshot/update/delete state and emits `Event::Strat`. |
| `UI` | Applies settings state and emits `Event::Settings`. Old append-only `TClientSettingsCommand` packets are parsed with the current settings snapshot as Delphi `cfg` fallback. |
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
applications, prefer `Client::run_with_dispatcher`.

For historical or truncated `TClientSettingsCommand` payloads, seed the
dispatcher's local settings fallback before dispatching:

```rust
dispatcher.set_client_settings_fallback(local_settings.clone());
```

The fallback mirrors Delphi `cfg`: missing soft-tail fields keep the current
local values instead of being reset to Rust defaults. Each received full settings
snapshot becomes the next fallback automatically.

## Trades Tick

`tick_trades` and `tick_trades_with_events` are low-level hooks for custom loops.
Call them after a valid `TradesStream` / `TradesResendResponse` packet, using
the packet timestamp and current RTT. `Client::run_with_dispatcher` does this
tail-check automatically. Gap lifecycle events are diagnostics for
logging/telemetry; the library performs recovery without requiring the
application to react to them.
