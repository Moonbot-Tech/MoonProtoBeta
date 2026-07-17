# Orders

MoonProto keeps a retained order read model inside `MoonClient`. UI code reads
orders from snapshots, reacts to lightweight order events, and sends user
intents back through `client.orders()`. The application does not build raw
order packets and does not mutate the live order state directly.

## Reading Orders

```rust
if let Some(snapshot) = client.snapshot() {
    for order in snapshot.orders().iter() {
        redraw_order(order);
    }
}
```

For incremental UI updates, drain events and refresh only the affected rows:

```rust
use moonproto::Event;
use moonproto::state::OrderEvent;

for event in client.drain_events() {
    if let Event::Order(order_event) = event {
        match &order_event {
            OrderEvent::Removed(order) => {
                show_final_order_state(order);
                remove_order_from_ui(order.uid);
            }
            OrderEvent::Snapshot => {
                if let Some(state) = client.snapshot() {
                    redraw_all_orders(state.orders().iter());
                }
            }
            _ => {
                if let Some(order) = order_event.order() {
                    redraw_order(order);
                }
            }
        }
    }
}
```

`OrderEvent::order()` carries an Arc-backed order row for `Created`, `Updated`,
and `Removed`, so an event-driven UI does not lose the final terminal status if
the latest snapshot has already removed the order from the live list. UI code
that already redraws at its own frame rate can still ignore individual events
and read the latest snapshot each frame.

## Orders History

Use `client.orders().request_history("BTCUSDT")` when the terminal needs the
core to run its orders-history flow for a market, or
`request_history_for_market(&market)` when the UI already keeps a
`MarketHandle`.

This is a fire-and-forget UI intent. It does not return a paired snapshot over
MoonProto; the core owns the history/export side effect. Regular order tables
and chart overlays continue to read live retained orders from
`snapshot().orders()`.

## Closed Sell Reports

`Event::ClosedSellOrderReport` is deprecated. It remains a compatibility path
for existing consumers that already execute the core's expanded report SQL.
New application-owned report databases should use `MoonClient::reports()` and
`Event::Report`; see `reports.md` for the replication and migration contract.

When the core closes a sell order or later updates that report row, it can emit
`Event::ClosedSellOrderReport`. The payload is:

```rust
pub struct ClosedSellOrderReportEvent {
    pub db_id: i64,
    pub sql: String,
}
```

`db_id` is the MoonBot Orders database row id, not an order worker UID and not
the exchange order id. Use it as the stable key when mirroring the report DB:
the same closed sell can receive more SQL after price changes, partial fills,
or final execution, and those commands must update the same DB row.

`sql` is the same expanded SQL text built by the core for its own Orders report
writer. It is the exact insert/update that MoonBot wrote or would write for
that Orders row. Active Lib does not parse this SQL back into `Order` fields and
does not update retained orders from it; regular trading UI should keep using
`snapshot().orders()`. The report event is for external report/DB sync tools
that need the exact row operation the core wrote.

Do not combine this stream with typed report replication in one database.
`db_id` is not `newRecID`, and the two paths can report the same closed sell.

## Order Fields

`Order` is the user-facing retained order object. The most common fields are:

```rust
pub struct Order {
    pub uid: u64,
    pub market_name: String,
    pub currency: BaseCurrency,
    pub platform: ExchangeCode,
    pub status: OrderWorkerStatus,
    pub buy_order: ExchangeOrder,
    pub sell_order: ExchangeOrder,
    pub buy_price: f64,
    pub sell_price: f64,
    pub stops: StopSettings,
    pub vstop_on: bool,
    pub vstop_fixed: bool,
    pub vstop_level: f64,
    pub vstop_vol: f64,
    pub panic_sell: bool,
    pub is_moon_shot: bool,
    pub corridor_price_down: f32,
    pub corridor_price_up: f32,
    pub immune_for_clicks: bool,
    pub is_short: bool,
    pub sell_reason: SellReason,
    pub strat_id: u64,
    pub db_id: i32,
    pub from_cache: bool,
    pub emulator_mode: bool,
    pub pending_cancel: bool,
    pub bulk_replace_buy: bool,
    pub bulk_replace_sell: bool,
    pub buy_trace_line: Option<OrderTraceLine>,
    pub sell_trace_line: Option<OrderTraceLine>,
    pub job_is_done: bool,
    pub cancel_request: bool,
    pub server_forced_remove: bool,
}
```

`currency` and `platform` are typed route values retained from the server order
state. Normal code does not write them manually; order actions use them to route
the action back to the correct market/exchange context.

`buy_order` and `sell_order` are `ExchangeOrder` values. They contain
exchange-side order values such as
`actual_price`, `quantity`, `quantity_remaining`, `mean_price`, `leverage`, and
open/close/create times. `order_type` and `sub_type` are typed values; use
their `name()` helpers for labels. The local `buy_price` and `sell_price`
fields are the desired replace prices tracked by the active client, not
exchange execution prices.

Order timestamps are wire time values, but raw time fields are not the normal
terminal API. Use `open_time()`, `close_time()`, and `create_time()` on
`ExchangeOrder`.
For exchange-order flags, use `is_opened()`, `is_closed()`, `canceled()`, and
`is_short()` on `ExchangeOrder`; the underlying packed boolean bytes and
packed-record byte IO are wire details kept inside Active Lib/tests.

`sell_reason` is a typed `SellReason` value. Use
`order.sell_reason.description()` for a MoonBot-compatible UI label.

Field groups for terminal UI:

| UI area | Read from |
|---|---|
| order table identity/routing | `uid`, `market_name`, `currency`, `platform`, `strat_id`, `db_id`, `from_cache`, `emulator_mode`, `is_short` |
| lifecycle/status columns | `status.name()`, `status.is_terminal()`, `job_is_done`, `cancel_request`, `server_forced_remove` |
| exchange-side buy/sell details | `buy_order`, `sell_order` (`actual_price`, `mean_price`, `quantity`, `quantity_remaining`, `leverage`, times) |
| local user intents | `buy_price`, `sell_price`, `pending_cancel`, `bulk_replace_buy`, `bulk_replace_sell`, `immune_for_clicks`, `panic_sell` |
| stops/VStop editor | `stops`, `vstop_on`, `vstop_fixed`, `vstop_level`, `vstop_vol` |
| chart overlays | `is_moon_shot`, `corridor_price_down/up`, `buy_trace_line`, `sell_trace_line` |

## Status

```rust
pub struct OrderWorkerStatus;

impl OrderWorkerStatus {
    pub const None: Self;
    pub const BuyFail: Self;
    pub const BuySet: Self;
    pub const BuyCancel: Self;
    pub const BuyDone: Self;
    pub const SellFail: Self;
    pub const SellSet: Self;
    pub const SellCancel: Self;
    pub const SellDone: Self;
    pub const SellAlmostDone: Self;

    pub const fn name(self) -> &'static str;
    pub const fn is_known(self) -> bool;
    pub const fn is_terminal(self) -> bool;
}
```

`OrderWorkerStatus::is_terminal()` returns true for final states. Unknown future
status bytes are preserved instead of being rejected; normal terminal UI uses
the typed constants, `name()`, and `is_terminal()`.

## Actions

Order actions go through `client.orders()`:

```rust
use moonproto::{StopSettings, VStopParams};

let Some(snapshot) = client.snapshot() else { return; };
let Some(order) = snapshot.orders().get(ui_state.selected_order_uid()) else { return; };
let stops = StopSettings::disabled()
    .with_stop_loss_percent(2.5, 0.1)
    .with_take_profit_price(50_500.0);

client.orders().move_order(order, new_price)?;
client.orders().cancel(order)?;
client.orders().update_stops(order, stops)?;
client.orders().update_vstop(
    order,
    VStopParams::percent(50_000.0, 12.0),
)?;
client.orders().turn_panic_sell(order, true)?;
client.orders().set_immune_for_orders([order], true)?;
client.orders().request_status(order)?;
```

Desktop UI should pass the visible `&Order` from its current snapshot. The
handle also accepts a raw UID for CLI tools and scripts, but in both cases the
runtime resolves the current live order state before sending. That live check is
important: pending-cancel, replace-in-flight, stop/VStop changes, panic-sell
flags, and click immunity are all stateful.

## Events

```rust
pub enum OrderEvent {
    Created(Arc<Order>),
    Updated(Arc<Order>),
    Removed(Arc<Order>),
    BulkReplaced { order_type: OrderType, uids: Vec<u64> },
    TracePoint { uid: u64 },
    CorridorChanged(u64),
    VStopChanged(u64),
    StopsChanged(u64),
    Snapshot,
}
```

`order()` returns the order row captured at the moment of `Created`, `Updated`,
or `Removed`. Use it in event-driven UI code: terminal statuses can move an
order out of the live snapshot before the application drains the async event
queue. `changed_uid()` and `removed_uid()` are still available for code that
only needs identities. `Snapshot` means a full order snapshot was applied and
the UI should reconcile the whole list.

Low-level ignored/not-applicable telemetry is available only in
`test`/`diagnostics` builds. Normal terminal code should redraw from retained
state instead of branching on internal apply-result reasons.

## Trace Lines

Server trace points are applied into `buy_trace_line` and `sell_trace_line`.
These fields are the chart-ready read model; the public order state does not
expose a raw inbound packet history. Long trace lines are shrunk with the same
800-line chart policy used by the MoonBot core.

For chart timestamps, use `OrderTraceChartPoint::time()` or `unix_millis()`.
When a sell trace carries a stop line, `OrderTraceLine::stop_price` and
`stop_time` give the price and time endpoint for that dotted stop segment.

## Lifecycle Notes

On reconnect, the server sends a fresh order snapshot. MoonProto accepts full
snapshots only in increasing generation order within the current hard session,
then emits `OrderEvent::Snapshot` after per-order events. A large older sliced
snapshot therefore cannot roll retained orders back after a newer snapshot has
already arrived. Missing tracked orders can trigger follow-up status requests
automatically.

Stops and VStop are independently versioned settings, not order-lifecycle
transitions. A full order snapshot can recover a lost settings update, while an
older full snapshot cannot overwrite a newer stop/VStop echo. Likewise, a
same-phase server full or replace acknowledgement does not overwrite the local
replace target currently owned by the UI; a real lifecycle phase change seeds
that target from the new exchange-side state.

Terminal order updates are removed after the current receive batch. Sell-done
orders keep a short grace window so immediately following visual trace packets
can still attach to the order. Recently completed UIDs are retained for the
current hard session so a delayed sliced snapshot cannot resurrect a removed
terminal order.

## Protocol Data

The crate-internal order wire model and `Orders::apply` path exist for tests and
packet replay. Regular applications should use `MoonClient`, snapshots, events,
and the `client.orders()` / `client.trade()` handles.
