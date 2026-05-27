# Events And Snapshots

`MoonClient` publishes typed events and immutable snapshots. Events tell the
application what changed; snapshots let UI code read the current retained state.

Connection lifecycle is a separate stream. Use `drain_lifecycle_events`,
`try_recv_lifecycle_event`, or `recv_lifecycle_event_timeout` for connection
status. Use `drain_events`, `try_recv_event`, or `recv_event_timeout` for domain
events.

## Recommended Loop

```rust
use moonproto::Event;

for event in client.drain_events() {
    match event {
        Event::Order(order_event) => handle_order_event(order_event),
        Event::OrderBook(book_event) => handle_orderbook_event(book_event),
        Event::Trade(trade_event) => handle_trade_signal(trade_event),
        Event::Markets(markets_event) => handle_markets_event(markets_event),
        Event::Balance(balance_event) => handle_balance_event(balance_event),
        Event::CandlesSnapshot(candles_event) => handle_candles_ready(candles_event),
        Event::Strat(strat_event) => handle_strategy_event(strat_event),
        Event::Settings(settings_event) => handle_settings_event(settings_event),
        Event::EngineResponse(resp) if !resp.success => {
            show_engine_error(resp.error_code, &resp.error_msg);
        }
        Event::ServerLog { time, msg } => append_server_log(time, msg),
        Event::ParseFailed { cmd, len, .. } => log_parse_failure(cmd, len),
        _ => {}
    }
}
```

`MoonClient` owns the protocol loop and runs until explicit `stop()` or drop.
Applications do not choose a protocol-loop duration.

## Snapshots

```rust
let Some(state) = client.snapshot() else { return; };

for order in state.orders().iter() {
    redraw_order(order);
}

if let Some(price) = state.markets().price("BTCUSDT") {
    redraw_price(price.bid, price.ask);
}

if let Some(book) = state.order_book("BTCUSDT", OrderBookKind::Futures) {
    redraw_book(&book.buys, &book.sells);
}
```

`snapshot()` returns a read-only `EventDispatcherSnapshot`. It is not the live
dispatcher and cannot mutate protocol state. UI code can keep snapshots for
rendering, while stateful commands go back through `MoonClient` handles such as
`client.orders()` and `client.trade()`.

## Event Shape

```rust
pub enum Event {
    Order(OrderEvent),
    OrderBook(OrderBookEvent),
    Trade(TradesEvent),
    WatcherFills(WatcherFillsEvent),
    Balance(BalanceEvent),
    TransferAssets(TransferAssetsEvent),
    CoinCardCandles(CoinCardCandlesEvent),
    CandlesSnapshot(CandlesSnapshotEvent),
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

Normal UI code usually handles the typed domain events and ignores `Raw` /
`ParseFailed`. `ParseFailed` includes the failed bytes so diagnostics can dump
exact payloads; it is not a recovery mechanism for application code.

`TradesEvent::Applied` is a signal that retained trade/history state has been
updated. Read actual rows from `MarketHistoryReaders`.

`CandlesSnapshotEvent::Ready` is emitted after the initial full 5m candles
snapshot has been processed by the history worker. At that point
`market_history_readers(market).candles_5m` already sees the retained rows.

`WatcherFillsEvent` contains `market_name`, HyperDex user address, decoded fill
rows, and helper methods for flags/time conversion.

`ServerLog.time` is a Delphi day value. Use `event.server_log_time()` when the
UI needs Unix milliseconds or `SystemTime`.

## Domain Gate

Before Init opens the domain gate, trading-domain packets are dropped rather
than delivered to UI code. After Init, trades packets additionally require an
explicit trades subscription intent. This keeps pre-init or unexpected stream
data from creating partial UI state.

When the server token changes after reconnect, MoonProto resets per-token
trades/orderbook sync state before applying new indexed stream packets.

## One-Shot Requests

Some explicit script/diagnostic helpers keep the runtime pumping while they
wait. They are named with a `blocking_` prefix. Any unrelated packets received
during the wait are still applied and remain available through the normal event
receiver:

```rust
let qty = client.blocking_request_balance("USDT", timeout)?;
for event in client.drain_events() {
    handle_event(event);
}
```

Regular UI refreshes such as `request_client_settings()` return immediately and
publish completion through domain events plus snapshots.

## Retained History

When all-trades storage is enabled, trade stream packets are queued to the
retained history worker. UI code reads per-market rings from snapshots:

```rust
if let Some(readers) = client
    .snapshot()
    .and_then(|state| state.market_history_readers("BTCUSDT"))
{
    if let Some(reader) = readers.futures_trades {
        let mut rows = Vec::new();
        reader.copy_last(1000, &mut rows);
        redraw_tape(&rows);
    }
}
```

`subscribe_all_trades` creates retained stores for all known markets.
`subscribe_trades_for` creates them only for the requested market names.

## Low-Level Dispatcher

`EventDispatcher`, `dispatch`, and `dispatch_into` remain public for protocol
tests and custom runtimes. Direct dispatcher calls do not get `Client`-backed
auto-actions such as Init gating, orderbook full requests, strategy snapshot
answers, or trades resend sends. Regular applications should use `MoonClient`.
