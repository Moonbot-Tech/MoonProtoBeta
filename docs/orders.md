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
        if let Some(uid) = order_event.changed_uid() {
            if let Some(state) = client.snapshot() {
                if let Some(order) = state.orders().get(uid) {
                    redraw_order(order);
                }
            }
        } else if let Some(uid) = order_event.removed_uid() {
            remove_order_from_ui(uid);
        } else if matches!(order_event, OrderEvent::Snapshot) {
            if let Some(state) = client.snapshot() {
                redraw_all_orders(state.orders().iter());
            }
        }
    }
}
```

`OrderEvent` carries UIDs instead of cloning full orders into every event. This
keeps the hot event path cheap. UI code that already redraws at its own frame
rate can also ignore individual events and read the latest snapshot each frame.

## Order Fields

`Order` is the user-facing retained order object. The most common fields are:

```rust
pub struct Order {
    pub uid: u64,
    pub market_name: String,
    pub currency: BaseCurrency,
    pub platform: ExchangeCode,
    pub status: OrderWorkerStatus,
    pub buy_order: OrderCompact,
    pub sell_order: OrderCompact,
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

`currency` and `platform` are typed Delphi route values retained from the
server order state. Normal code does not write them manually; order actions use
them to build the correct wire header.

`buy_order` and `sell_order` contain exchange-side order values such as
`actual_price`, `quantity`, `quantity_remaining`, `mean_price`, `leverage`, and
open/close/create times. The local `buy_price` and `sell_price` fields are the
desired replace prices tracked by the active client, not exchange execution
prices.

Order timestamps are Delphi `TDateTime` values on the wire. Use
`open_time_delphi()`, `close_time_delphi()`, and `create_time_delphi()` on
`OrderCompact` instead of interpreting raw `f64` fields directly.
For exchange-order flags, use `is_opened()`, `is_closed()`, `canceled()`, and
`is_short()` on `OrderCompact`; the underlying packed boolean bytes are a wire
detail.

`sell_reason` is a typed `SellReason` value. Use
`order.sell_reason.description()` for a Delphi-compatible UI label, and use
`to_byte()` only in low-level protocol diagnostics.

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
status bytes are preserved instead of being rejected. Raw
`OrderWorkerStatus::from_byte(raw)` / `to_byte()` exist for protocol tools, not
for normal terminal UI.

## Actions

Order actions go through `client.orders()`:

```rust
use moonproto::{StopSettings, VStopParams};

let Some(snapshot) = client.snapshot() else { return; };
let Some(order) = snapshot.orders().get(ui_state.selected_order_uid()) else { return; };
let stops = StopSettings::disabled()
    .with_stop_loss(true, false, 2.5, 0.1)
    .with_take_profit(true, 50_500.0);

client.orders().move_order(order, new_price)?;
client.orders().cancel(order)?;
client.orders().update_stops(order, stops)?;
client.orders().update_vstop(
    order,
    VStopParams {
        enabled: true,
        fixed: false,
        level: 50_000.0,
        volume: 12.0,
    },
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
    Created(u64),
    Updated(u64),
    Removed(u64),
    BulkReplaced { order_type: OrderType, uids: Vec<u64> },
    TracePoint { uid: u64 },
    CorridorChanged(u64),
    VStopChanged(u64),
    StopsChanged(u64),
    Snapshot,
    Ignored { uid: u64, reason: ApplyResult },
}
```

`changed_uid()` returns UIDs for events that should normally redraw one order.
`removed_uid()` returns UIDs for removed rows. `Snapshot` means a full order
snapshot was applied and the UI should reconcile the whole list.

`Ignored` is mainly useful for direct low-level state tests. The active runtime
does not emit user-visible ignored events for client-originated raw commands.

## Trace Lines

Server trace points are retained in two forms:

`buy_trace_line` and `sell_trace_line` are the chart-ready read model. The raw
inbound trace packet log is retained for diagnostics but is not the normal chart
API.

For chart timestamps, use `OrderTraceChartPoint::time_delphi()`.

## Lifecycle Notes

On reconnect, the server sends a fresh order snapshot. MoonProto applies it to
the retained order state and emits `OrderEvent::Snapshot` after per-order
events. Missing tracked orders can trigger follow-up status requests
automatically.

Terminal order updates are removed after the current receive batch. Sell-done
orders keep a short grace window so immediately following visual trace packets
can still attach to the order, matching the Delphi client behavior.

## Protocol Data

The internal `commands::trade` wire model and `Orders::apply` exist for tests
and packet replay. Regular applications should use `MoonClient`, snapshots,
events, and the `client.orders()` / `client.trade()` handles.
