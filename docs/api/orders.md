# Orders

The order channel mirrors server-side trading orders. Applications normally read
orders through `EventDispatcher::orders()` and react to `Event::Order`.

## Reading Orders

For a one-shot active-order snapshot, use `Client::request_order_snapshot`:

```rust
let orders = client.request_order_snapshot(
    &mut dispatcher,
    Duration::from_secs(12),
)?;
```

The helper sends `TAllStatusesReq`, keeps the UDP loop running, and waits for
the dispatcher to finish missing-order cleanup requests. For continuous UI
updates, read order events from the dispatcher:

```rust
use moonproto::Event;
use moonproto::state::OrderEvent;

client.run_with_dispatcher_state(duration, &mut dispatcher, Box::new(|event, state| {
    if let Event::Order(order_event) = event {
        match order_event {
            OrderEvent::Created(uid) | OrderEvent::Updated(uid) => {
                if let Some(order) = state.orders().get(*uid) {
                    redraw_order(order);
                }
            }
            OrderEvent::Removed(uid) => remove_order_from_ui(*uid),
            OrderEvent::Snapshot => redraw_all_orders(state.orders().iter()),
            OrderEvent::Ignored { uid, reason } => log_ignored_order(*uid, *reason),
            _ => {}
        }
    }
}));
```

`Orders::iter()` yields read-only `&Order` values. The dispatcher mutates the
state internally as packets arrive.

## `Order`

Important fields:

```rust
pub struct Order {
    pub uid: u64,
    pub market_name: String,
    pub currency: u8,
    pub platform: u8,
    pub status: OrderWorkerStatus,
    pub buy_order: OrderCompact,
    pub sell_order: OrderCompact,
    pub stops: StopSettings,
    pub vstop_on: bool,
    pub vstop_fixed: bool,
    pub vstop_level: f64,
    pub vstop_vol: f64,
    pub corridor_price_down: f32,
    pub corridor_price_up: f32,
    pub strat_id: u64,
    pub is_short: bool,
    pub db_id: i32,
    pub from_cache: bool,
    pub emulator_mode: bool,
    pub immune_for_clicks: bool,
    pub panic_sell: bool,
    pub bulk_replace_buy: bool,
    pub bulk_replace_sell: bool,
    pub trace_points: VecDeque<OrderTracePoint>,
    pub job_is_done: bool,
    pub server_forced_remove: bool,
    pub sell_reason_code: u8,
}
```

Use `order.sell_reason()` to convert `sell_reason_code` into `SellReason`.

## Status Values

```rust
pub enum OrderWorkerStatus {
    None = 0,
    BuyFail = 1,
    BuySet = 2,
    BuyCancel = 3,
    BuyDone = 4,
    SellFail = 5,
    SellSet = 6,
    SellCancel = 7,
    SelLDone = 8,
    SelLAlmostDone = 9,
}
```

Terminal statuses are `SelLDone`, `BuyCancel`, `BuyFail`, `SellCancel`, and
`SellFail`.

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
    PanicSellChanged(u64),
    Snapshot,
    Ignored { uid: u64, reason: ApplyResult },
}
```

## Time Correction

Order timestamps arrive as Delphi `TDateTime` values in server-local time. The
client updates a per-client `ServerTimeDelta` from Ping packets; `EventDispatcher`
links that value into `Orders` automatically when using `run_with_dispatcher`.

For custom loops, call:

```rust
dispatcher.set_server_time_delta_source(client.server_time_delta_handle());
```

## Low-Level State

Advanced consumers can apply parsed trade commands directly:

```rust
use moonproto::commands::trade::TradeCommand;
use moonproto::state::Orders;

let mut orders = Orders::new();
let command = TradeCommand::parse(payload).expect("bad order payload");
let (result, event) = orders.apply(command);
```

`TradeCommand::OrderStatus` and `TradeCommand::OrderReplaceResponse` carry
boxed payloads (`Box<OrderStatus>` / `Box<OrderReplaceResponse>`) because those
records are much larger than the other variants. Deref access works normally in
matches, and `Orders::apply` consumes the enum directly.

Regular applications should use `EventDispatcher`.
