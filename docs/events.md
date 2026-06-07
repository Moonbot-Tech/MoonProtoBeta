# Events And Snapshots

`MoonClient` publishes typed events and immutable snapshots. Events tell the
application what changed; snapshots let UI code read the current retained state.

The single public delivery model is `MoonEventSink`: the runtime publishes
`MoonClientEvent::Lifecycle` and `MoonClientEvent::Domain` into a sink and never
blocks on UI work. Queue draining is only one adapter for that sink, not a
separate runtime mode.

## Event Sink

Frameworks with a native event system normally use a callback sink and post into
their own main loop:

```rust
use moonproto::{ConnectConfig, MoonClient, MoonClientEvent, MoonEventSink};

let sink = MoonEventSink::callback(move |event| {
    // For callback-oriented hosts: post/emit into the framework loop.
    // Keep this callback quick; do not render or wait here.
    post_to_ui(event);
});

let client = MoonClient::connect_with_sink(cfg, ConnectConfig::new(init), sink)?;
```

`MoonEventSink::callback` has its own delivery worker: the protocol/runtime
thread only queues the event and returns. Keep the callback quick anyway, or the
delivery worker can build a backlog.

Immediate-mode UIs and CLI/tools can use the standard queue adapter. In egui,
use `queue_with_waker` and call `request_repaint()` from the waker:

```rust
let (sink, events) = moonproto::MoonEventSink::queue_with_waker({
    let ctx = egui_ctx.clone();
    move || ctx.request_repaint()
});

let client = moonproto::MoonClient::connect_with_sink(cfg, connect, sink)?;

let mut lifecycle_buf = Vec::new();
let mut event_buf = Vec::new();

events.drain_lifecycle_events_into(&mut lifecycle_buf);
for lifecycle in lifecycle_buf.drain(..) {
    handle_lifecycle(lifecycle);
}

events.drain_events_into(&mut event_buf);
for event in event_buf.drain(..) {
    handle_event(event);
}
```

`MoonClient::connect(...)` is the convenience constructor that installs this
queue adapter internally and exposes `client.drain_lifecycle_events()` /
`client.drain_events()` for simple apps and tests. Hot UI loops can use
`drain_lifecycle_events_into` / `drain_events_into` to reuse buffers.

The queue adapter is intentionally unbounded: the runtime must not drop
already-produced domain events because of a hidden capacity cap. The application
side must therefore drain it from its UI tick/event bridge. If a queue adapter
is never drained, pending events keep memory until the process hits memory
pressure. Callback sinks have the same practical rule: keep the callback quick
and post into the framework loop; blocking there only moves the backlog to the
delivery worker.

Timeout waits exist only as hidden diagnostic/script helpers.

## Domain Events

```rust
use moonproto::Event;

fn handle_event(event: Event) {
    match event {
        Event::Order(order_event) => handle_order_event(order_event),
        Event::OrderBook(book_event) => handle_orderbook_event(book_event),
        Event::Trade(trade_event) => handle_trade_signal(trade_event),
        Event::Markets(markets_event) => handle_markets_event(markets_event),
        Event::Balance(balance_event) => handle_balance_event(balance_event),
        Event::Account(account_event) => handle_account_event(account_event),
        Event::CandlesSnapshot(candles_event) => handle_candles_ready(candles_event),
        Event::Strat(strat_event) => handle_strategy_event(strat_event),
        Event::Detect(detect_event) => handle_detect_fact(detect_event),
        Event::ChartAlert(alert_event) => handle_alert_object_state(alert_event),
        Event::ChartText(rows) => redraw_chart_text(rows),
        Event::ClosedSellOrderReport(report) => sync_report_db(report.db_id, &report.sql),
        Event::Settings(settings_event) => handle_settings_event(settings_event),
        Event::EngineAction(action) => handle_engine_action(action),
        Event::ServerLog(log) => append_server_log(log.time(), &log.msg),
        _ => {}
    }
}
```

`MoonClient` owns the protocol loop and runs until explicit `disconnect()`/drop.
Applications do not choose a protocol-loop duration. `MoonClient::connect`
starts the runtime and returns immediately; wait for `LifecycleEvent::Ready`
through the same non-blocking event path before treating snapshots as fully
initialized.

## Snapshots

```rust
let Some(state) = client.snapshot() else { return; };
let Some(market) = state.markets().get("BTCUSDT") else { return; };

for order in state.orders().iter() {
    redraw_order(order);
}

market.with(|market| redraw_price(market.price.bid, market.price.ask));

if let Some(book) = state.order_book_for(&market, OrderBookKind::Futures) {
    redraw_book(&book.buys, &book.sells);
}
```

`snapshot()` returns a read-only `MoonStateSnapshot`. It is not the live runtime
state and cannot mutate protocol state. UI code can keep snapshots for
rendering, while stateful commands go back through `MoonClient` handles such as
`client.orders()` and `client.trade()`.

For hot UI loops that prepare larger draw buffers, use
`snapshot_versioned()` and keep the last seen revision:

```rust
if let Some(state) = client.snapshot_versioned() {
    if Some(state.revision()) != last_revision {
        rebuild_cached_draw_data(&state);
        last_revision = Some(state.revision());
    }
}
```

The revision is local to one `MoonClient` runtime and increases whenever the
runtime publishes a fresh immutable snapshot.

## Event Shape

```rust
pub enum Event {
    Order(OrderEvent),
    OrderBook(OrderBookEvent),
    Trade(TradesEvent),
    WatcherFills(WatcherFillsEvent),
    Balance(BalanceEvent),
    Account(AccountEvent),
    TransferAssets(TransferAssetsEvent),
    CoinCardCandles(CoinCardCandlesEvent),
    CandlesSnapshot(CandlesSnapshotEvent),
    Arb(ArbEvent),
    Strat(StratEvent),
    Detect(DetectEvent),
    ChartAlert(ChartAlertEvent),
    ChartText(ChartTextSnapshot),
    ClosedSellOrderReport(ClosedSellOrderReportEvent),
    Settings(SettingsEvent),
    Markets(MarketsEvent),
    EngineAction(EngineActionEvent),
    ServerLog(ServerLogEvent),
}
```

Low-level diagnostic builds may also receive hidden raw/parse-failure/raw Engine
API response events. They are for FireTest/protocol dumps only; they are not a
recovery mechanism or normal application control flow.

`TradesEvent::Applied` is a signal that retained trade/history state has been
updated. Read actual rows from `MarketHistoryReaders`.

`CandlesSnapshotEvent::Ready` is emitted after the initial full 5m candles
snapshot has been processed by the history worker. At that point
`market_history_readers_for(&market).candles_5m` already sees the retained rows.

`DetectEvent` is a detect/watch/chart-alert fact built by the core. It covers
ordinary strategy detect messages, watcher rows, chart-only markers, and
chart-alert fires. UI code displays/logs it and may consult the local strategy
snapshot for user-facing labels, but it does not recompute the detect.

`ChartAlertEvent` reports the accepted chart-alert object state. Read the
retained set from `snapshot().chart_alerts().for_market(...)`; the event is the
change signal, not a raw chart-object parser contract.

`ChartTextSnapshot` is a full replacement of ready chart text rows for the
currently requested chart-text market. Late snapshots for an older selected
market are dropped. Read the latest rows from `snapshot().chart_text().get(...)`
when repainting the selected chart.

`ClosedSellOrderReportEvent` carries the exact expanded Orders SQL that the
core wrote for a closed sell order report, plus the MoonBot Orders DB row id.
Use `db_id` as the mirror/update key: later SQL for price changes, partial
fills, or final execution updates the same DB record. This event is for external
report/DB sync; it does not mutate the retained `Orders` model and is not a
second order schema.

`ArbEvent` is only a change signal/summary. Incoming arb data is applied to the
selected market state, so UI code reads
`MarketHandle::arb_slot(ArbPlatformCode::...)` /
`arb_now(ArbPlatformCode::...)` from the selected market instead of handling raw
server `market_index` blocks.

`WatcherFillsEvent` contains a shared `market_name` (`event.market_name.as_ref()`),
HyperDex user address (`event.user_hex()` for display/logging), decoded fill
rows, and `MoonTime` helpers.

`ServerLogEvent` contains the server log text and typed time helpers. Use
`log.time()` / `log.unix_millis()` for UI timestamps.

## Domain Gate

Before Init opens the domain gate, trading-domain packets are dropped rather
than delivered to UI code. After Init, trades packets additionally require an
explicit trades subscription intent. This keeps pre-init or unexpected stream
data from creating partial UI state.

When the server token changes after reconnect, MoonProto resets per-token
trades/orderbook sync state before applying new indexed stream packets.

## One-Shot Requests

Normal UI code queues refresh intents and consumes the resulting EventSink
events. Any unrelated packets received while the runtime is active are still
applied and remain available through the same event path. With the default queue
adapter that looks like this:

```rust
client.balances().refresh()?;

for event in client.drain_events() {
    handle_event(event);
}
```

Regular UI refreshes such as `settings().refresh()`,
`account().refresh_hedge_mode()`, and
`account().refresh_api_expiration_time()` return immediately and publish completion
through domain events plus snapshots. Account refresh results are readable from
`snapshot().account()`.

## Retained History

When all-trades storage is enabled, trade stream packets are queued to the
retained history worker. UI code reads per-market rings from snapshots:

```rust
// Keep `market`, `readers`, and per-panel cursors in UI state after the user
// selects a symbol. Do not repeat string lookup on every paint tick.
let Some(state) = client.snapshot() else { return; };
let Some(market) = selected_market.as_ref() else { return; };
if let Some(readers) = state.market_history_readers_for(market) {
    if let Some(reader) = readers.futures_trades {
        let mut rows = Vec::new();
        reader.copy_last(1000, &mut rows);
        redraw_tape(&rows);
    }
}
```

`streams().subscribe_all_trades` creates retained stores for all known markets.
`streams().subscribe_trades_for` creates them only for the requested market names.

## Runtime Ownership

`MoonClient` owns the mutable read model in the normal Active Lib path.
Applications read immutable snapshots and receive events; they do not drive
state-apply ticks or protocol pumps themselves.
