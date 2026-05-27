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
    pub immune_for_clicks: bool,
    pub is_short: bool,
    pub strat_id: u64,
    pub emulator_mode: bool,
}
```

`buy_order` and `sell_order` contain exchange-side order values such as
`actual_price`, `quantity`, `quantity_remaining`, `mean_price`, `leverage`, and
open/close/create times. The local `buy_price` and `sell_price` fields are the
desired replace prices tracked by the active client, not exchange execution
prices.

Order timestamps are Delphi `TDateTime` values on the wire. Use
`open_time_delphi()`, `close_time_delphi()`, and `create_time_delphi()` on
`OrderCompact` instead of interpreting raw `f64` fields directly.

## Status

```rust
pub struct OrderWorkerStatus(pub u8);

impl OrderWorkerStatus {
    pub const None: Self = Self(0);
    pub const BuyFail: Self = Self(1);
    pub const BuySet: Self = Self(2);
    pub const BuyCancel: Self = Self(3);
    pub const BuyDone: Self = Self(4);
    pub const SellFail: Self = Self(5);
    pub const SellSet: Self = Self(6);
    pub const SellCancel: Self = Self(7);
    pub const SelLDone: Self = Self(8);
    pub const SelLAlmostDone: Self = Self(9);
}
```

`OrderWorkerStatus::is_terminal()` returns true for final states. Unknown future
status bytes are preserved as `OrderWorkerStatus(n)` instead of being rejected.

## Actions

Order actions go through `client.orders()`:

```rust
let Some(snapshot) = client.snapshot() else { return; };
let Some(order) = snapshot.orders().get(order_uid) else { return; };

client.orders().move_order(order, new_price)?;
client.orders().cancel(order)?;
client.orders().update_stops(order, stops)?;
client.orders().update_vstop(order, true, false, 50_000.0, 12.0)?;
client.orders().turn_panic_sell(order, true)?;
client.orders().request_status(order)?;
```

The handle accepts either `&Order` from a snapshot or a raw UID. In both cases
the runtime resolves the current live order state before sending. That live
check is important: pending-cancel, replace-in-flight, stop/VStop changes,
panic-sell flags, and click immunity are all stateful.

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

`Ignored` is mainly useful for direct low-level state tests. The active
dispatcher does not emit user-visible ignored events for client-originated raw
commands.

## Trace Lines

Server trace points are retained in two forms:

- `buy_trace_line` and `sell_trace_line` are the chart-ready read model.
- `trace_points` is the raw inbound diagnostic log.

For chart timestamps, use `OrderTraceChartPoint::time_delphi()`.

## Lifecycle Notes

On reconnect, the server sends a fresh order snapshot. MoonProto applies it to
the retained order state and emits `OrderEvent::Snapshot` after per-order
events. Missing tracked orders can trigger follow-up status requests
automatically.

Terminal order updates are removed after the current receive batch. Sell-done
orders keep a short grace window so immediately following visual trace packets
can still attach to the order, matching the Delphi client behavior.

## Low-Level Tools

`commands::trade::*`, `TradeCommand::parse`, and `Orders::apply` remain public
for protocol tests, packet replay, and custom runtimes. Regular applications
should use `MoonClient`, snapshots, events, and the `client.orders()` /
`client.trade()` handles.
