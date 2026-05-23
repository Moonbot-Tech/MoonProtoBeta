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
            _ => {}
        }
    }
}));
```

`Orders::iter()` yields read-only `&Order` values. The dispatcher mutates the
state internally as packets arrive.
When a server `TAllStatuses` snapshot arrives, the dispatcher follows the Delphi
order: it advances the snapshot flag, applies each contained `TOrderStatus`
through the same order-command path as live updates, emits the per-order events,
then emits `OrderEvent::Snapshot` for redraw / missing-order cleanup. Cleanup
treats every order still present in the read model as a Delphi `WCache` worker:
terminal entries waiting for deferred removal can still produce a follow-up
`TOrderStatusRequest` when they are absent from the fresh snapshot. Any incoming
`TBaseMarketCommand` descendant that reaches Delphi `ProcessCommandOrder` and
finds an existing local worker refreshes that worker's snapshot mark before
epoch/phase checks. `TAllStatusesReq` and `TSetImmuneCommand` do not do this,
and the special `TBulkReplaceNotify` branch only touches the UIDs listed in the
notify without refreshing snapshot presence.
`TOrderStatus` responses marked `FromCache=true` update only an already tracked
order; they do not create a new active order entry when the UID is unknown.
For a new non-cache `TOrderStatus`, the dispatcher also requires the market
name to be present in `MarketsState`; otherwise the packet is dropped without
creating an order, matching Delphi's `Cmd.m <> nil` worker-create guard.
Incoming client-originated order commands such as `TSetImmuneCommand`,
`TTurnPanicSellCommand`, join/split/close/sell/move commands, and raw request
commands are not applied and do not emit `OrderEvent`: in the Delphi receive
flow they are not server state updates. If such a received command is a
`TTradeEpochCommand` descendant and the order exists, `Orders::apply` still
performs Delphi's hidden `AcceptServerCommand` epoch/phase side effect before
returning `NotApplicable`; this can make a later lower-epoch server update
stale. Non-epoch commands remain a pure silent no-op. For outgoing UI clicks,
`Client::set_immune` takes `EventDispatcher::orders_mut()`, immediately updates
`immune_for_clicks` on found active orders, and then queues `TSetImmuneCommand`.
The command header UID is generated internally like Delphi `TBaseCommand.Create`;
the target order UIDs are the command items. Later `TOrderStatus` snapshots can
refresh the same field from the server.
Skipped server-state packets also do not emit active dispatcher events: unknown
UID updates, stale epoch packets, phase rollbacks, and empty `TBulkReplaceNotify`
effects match Delphi's log/free/exit receive path. The diagnostic
`OrderEvent::Ignored` value is returned only by direct low-level
`Orders::apply` calls.
Terminal statuses and `TOrderNotFound` are removed in a deferred flush after the
current reader batch. `SelLDone` has an additional 400 ms grace window matching
Delphi `DoTheJobVirtual`, which runs two `Sleep(200); ProcessCommands` passes
before removing the worker from `WCache`. The worker remains addressable long
enough for immediately following visual packets such as `TOrderTracePoint`,
then `OrderEvent::Removed` is emitted.
`TBulkReplaceNotify` sets `bulk_replace_buy` / `bulk_replace_sell` for found
orders only; its `BulkReplaced.uids` event lists only those actually found
locally. `TOrderReplaceResponse` clears the matching flag; if no response
arrives, the active dispatcher clears the flag after 5000 ms, matching Delphi's
`ReplaceSentTime` timeout.
For replace-response and bulk-replace side selection, Delphi treats only
`OrderType::Buy` (`O_BUY`) as the buy side; `Sell`, `BuyStop`, and `BuyLimit`
all use the sell side in the order read model.

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
    pub strat_id: u64,
    pub is_short: bool,
    pub db_id: i32,
    pub from_cache: bool,
    pub emulator_mode: bool,
    pub immune_for_clicks: bool,
    pub has_local_visual_order: bool,
    pub pending_buy_cond_price: Option<f64>,
    pub pending_cancel: bool,
    pub bulk_replace_buy: bool,
    pub bulk_replace_sell: bool,
    pub buy_trace_line: Option<OrderTraceLine>,
    pub sell_trace_line: Option<OrderTraceLine>,
    pub trace_points: VecDeque<OrderTracePoint>,
    pub job_is_done: bool,
    pub cancel_request: bool,
    pub server_forced_remove: bool,
    pub sell_reason_code: u8,
}
```

```rust
pub struct OrderTraceLine {
    pub order_type: OrderType,
    pub order_id: i64,
    pub prevent_delete: bool,
    pub points: Vec<OrderTraceChartPoint>,
    pub tmp_point: Option<OrderTraceChartPoint>,
    pub can_finish: bool,
    pub stop_price: Option<f32>,
}

pub struct OrderTraceChartPoint {
    pub time: f64,
    pub price: f32,
}
```

Use `order.sell_reason()` to convert `sell_reason_code` into `SellReason`.
`SellReason::description()` returns the same strings as Delphi
`SellReasonCodeToStr`, including compact names such as `PanicSell`,
`StopLoss`, and `TakeProfit`.
Incoming `TOrderStatusUpdate` changes this code only when the wire
`SellReasonCode` is non-zero, matching Delphi's `FPrevSellReasonCode` guard.
A later update with `SellReasonCode = 0` leaves the previous reason visible.
`buy_price` and `sell_price` mirror Delphi `pBuyOrder.Price` /
`pSellOrder.Price`: they are local desired/replace prices, distinct from
`buy_order.actual_price` / `sell_order.actual_price` and not present in
`TOrderCompact` wire data.
`has_local_visual_order` mirrors Delphi `BOrderWorker.vOrder <> nil`. It is set
automatically for a new server-created pending `TOrderStatus(Status=None)`.
Applications that create their own local visual-order equivalent before sending
a new order should call `Orders::mark_local_visual_order(uid)` once the server
UID is known. Stop/VStop outgoing helpers require this marker because Delphi
`SendStopsIfChanged` and `SendVStopIfChanged` exit when `vOrder = nil`.
`panic_sell` mirrors Delphi `BOrderWorker.FPanicSell`, the local outgoing
panic-sell intent used by market-level `turn_panic_sell` /
`switch_panic_sell_by_market` and per-order `turn_order_panic_sell`.
`TCorridorUpdate` mirrors Delphi `HandleServerCommand`: it sets
`is_moon_shot = true` and stores `corridor_price_down` /
`corridor_price_up` as Delphi `TestPriceDown` / `TestPriceUp`.
`TOrderTracePoint` is exposed in two forms. `trace_points` is the raw inbound
diagnostic log. `buy_trace_line` and `sell_trace_line` mirror Delphi
`coBuy` / `coSell` `TOrderLine` state: only an initial trace creates a line,
non-initial traces without an existing line do not create one, temporary points
are stored as the line temp point, and finish traces mutate the last drawable
point only when Delphi `CanFinish` would allow it.
`pending_buy_cond_price` mirrors Delphi `vOrder.BuyCondPrice` for pending
`OS_None` orders. A new server-created `TOrderStatus(Status=None)` creates this
pending value from `BuyOrder.MeanPrice`, matching Delphi `OnMServerOrder`
creating a visual pending order. For an already tracked order, full
`TOrderStatus(Status=None)` updates `buy_order` but does not create or overwrite
the pending visual price. `TOrderStatusUpdate(Status=None)` updates this field
from `UpdateData.MeanPrice` only while the local entry already represents
Delphi's pending `vOrder`; it does not create pending state for a non-pending
worker and does not apply the rest of `UpdateData` to `buy_order`.
`pending_cancel` mirrors Delphi `vOrder.PendingCancel`. Calling
`cancel_order` for a pending `OS_None` order sets this flag and follows
Delphi's `CheckReplaceFlag` pending path.
`TOrderNotFound` sets `cancel_request` and `server_forced_remove` immediately.
`job_is_done` is a read-model terminal marker; it is not used as Delphi
`BOrderWorker.JobIsDone` for missing-order cleanup while the entry is still
waiting for deferred removal.

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

Terminal statuses are `SelLDone`, `SelLAlmostDone`, `BuyCancel`, `BuyFail`,
`SellCancel`, and `SellFail`.

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

For `TAllStatuses`, expect zero or more per-order events before the final
`Snapshot` marker. For terminal order updates, expect an `Updated` event first
and a later deferred `Removed` event after the receive batch is drained; for
`SelLDone`, removal is delayed by the Delphi 400 ms final-trace grace window.
`Ignored` is a low-level diagnostic from `Orders::apply`; `EventDispatcher`
does not emit it as an active order event.

## Time Correction

Order timestamps arrive as Delphi `TDateTime` values in server-local time. The
client updates a per-client `ServerTimeDelta` from Ping packets; `EventDispatcher`
links that value into `Orders` automatically when using `run_with_dispatcher`.
For `TOrderCompact` and `TOrderUpdateData`, only valid Delphi dates (`> 1`) are
shifted, matching Delphi `AdjustTime`; zero/missing timestamps stay zero.

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
