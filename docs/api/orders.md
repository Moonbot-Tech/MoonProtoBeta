# Orders

The order channel mirrors server-side trading orders. Applications normally read
orders through immutable `MoonClient::snapshot()` values and react to
`Event::Order`.

## Reading Orders

For a live UI, read from the latest `MoonClient` snapshot:

```rust
if let Some(snapshot) = client.snapshot() {
    for order in snapshot.orders().iter() {
        redraw_order(order);
    }
}
```

For continuous UI updates, drain order events and then read the latest snapshot:

```rust
use moonproto::Event;
use moonproto::state::OrderEvent;

for event in client.drain_events() {
    if let Event::Order(order_event) = event {
        match order_event {
            OrderEvent::Created(uid) | OrderEvent::Updated(uid) => {
                if let Some(state) = client.snapshot() {
                    if let Some(order) = state.orders().get(uid) {
                        redraw_order(order);
                    }
                }
            }
            OrderEvent::Removed(uid) => remove_order_from_ui(uid),
            OrderEvent::Snapshot => {
                if let Some(state) = client.snapshot() {
                    redraw_all_orders(state.orders().iter());
                }
            }
            _ => {}
        }
    }
}
```

`Orders::iter()` yields read-only `&Order` values. The dispatcher mutates the
state internally as packets arrive.
When a server `TAllStatuses` snapshot arrives, the dispatcher follows the Delphi
order: it advances the snapshot flag, applies each contained `TOrderStatus`
through the same order-command path as live updates, emits the per-order events,
then emits `OrderEvent::Snapshot` for redraw / missing-order cleanup. Cleanup
treats every order still present in the read model as a Delphi `WCache` worker:
terminal entries waiting for deferred removal can still produce a follow-up
`TOrderStatusRequest` when they are absent from the fresh snapshot. This matches
MoonProto virtual workers: Delphi `JobIsDone` becomes true only after
`DoTheJobVirtual` returns, while `RemoveWorkerFromCache` happens before that.
The active `MoonClient` path sends those follow-up requests automatically.
Low-level raw `EventDispatcher::dispatch_into` users receive only
the typed state/events; after `OrderEvent::Snapshot` they can call
`EventDispatcher::missing_order_status_requests_after_snapshot()` and pass each
returned `(ctx, market_name)` to `Client::request_order_status`.
Any incoming market-scoped server update that finds an existing local order
refreshes that order's snapshot mark before epoch/phase checks. Request packets
and local click-immune commands do not do this, and bulk-replace notifications
only touch the UIDs listed in the notify without refreshing snapshot presence.
Order-status responses marked `FromCache=true` update only an already tracked
order; they do not create a new active order entry when the UID is unknown.
For a new non-cache order status, the dispatcher also requires the market name
to be present in `MarketsState`; otherwise the packet is dropped without
creating an order, matching the active-client worker-create guard.
Worker identity fields from full order status (`market_name`,
`currency`, `platform`, `strat_id`, `is_short`, `db_id`, `from_cache`,
`emulator_mode`) are set when the local worker is created. Later full statuses
for the same UID update compact buy/sell orders, stops, immune flag, status,
and price side effects, but do not rewrite those worker-level fields, matching
Delphi `BOrderWorker.HandleServerCommand`.
Incoming client-originated order commands such as click-immune, panic-sell,
join/split/close/sell/move commands, and raw request commands are not applied
and do not emit `OrderEvent`: they are not server state updates. If such a
received command carries epoch/phase data and the order exists, `Orders::apply`
still performs the hidden epoch/phase side effect before returning
`NotApplicable`; this can make a later lower-epoch server update stale.
Non-epoch commands remain a pure silent no-op. For outgoing UI clicks,
`Client::set_immune` takes `EventDispatcher::orders_mut()`, immediately updates
`immune_for_clicks` on found active orders, and then queues the click-immune
command. The command UID is generated internally; the target order UIDs are the
command items. Later order-status snapshots can refresh the same field from the
server.
Skipped server-state packets also do not emit active dispatcher events: unknown
UID updates, stale epoch packets, phase rollbacks, and empty bulk-replace
effects match Delphi's log/free/exit receive path. The diagnostic
`OrderEvent::Ignored` value is returned only by direct low-level
`Orders::apply` calls.
Terminal statuses and `TOrderNotFound` are removed in a deferred flush after the
current receive batch. `SelLDone` has an additional 400 ms grace window matching
Delphi `DoTheJobVirtual`, which runs two `Sleep(200); ProcessCommands` passes
before removing the worker from `WCache`. The worker remains addressable long
enough for immediately following visual packets such as `TOrderTracePoint`,
then `OrderEvent::Removed` is emitted.
Bulk-replace notification sets `bulk_replace_buy` / `bulk_replace_sell` for found
orders only; its `BulkReplaced.uids` event lists only those actually found
locally. The in-flight timer is Delphi's single worker-level `ReplaceSentTime`,
not a separate timer per side. `TOrderReplaceResponse` clears only the matching
flag; the active dispatcher clears `ReplaceSentTime` or the current-side flag
from the worker tick, matching Delphi `CheckReplaceFlag`.
Bulk-replace and incoming click-immune counted arrays keep compatibility with
short tails: after a valid command prefix, the declared count is kept, complete
elements are decoded, and short tail elements are zero-filled. The active
dispatcher still exposes only found/relevant order side effects.
Low-level trade command parsers keep the same Delphi split for malformed tails:
market/order strings are strict `ReadBuffer` fields and reject the whole command
when the declared bytes are missing; fixed scalar/record fields after a valid
string use `TMemoryStream.Read` semantics, consuming available bytes and
zero-filling missing little-endian tail bytes.
For replace-response and bulk-replace side selection, Delphi treats only
`OrderType::Buy` (`O_BUY`) as the buy side; `Sell`, `BuyStop`, and `BuyLimit`
all use the sell side in the order read model. `OrderType` preserves the raw
Delphi ordinal byte, so future/unknown values are represented as
`OrderType(n)` / debug `Unknown(n)` and also use the sell side unless `n == 1`.

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
Incoming order-status updates change this code only when `SellReasonCode` is
non-zero, matching Delphi's `FPrevSellReasonCode` guard.
A later update with `SellReasonCode = 0` leaves the previous reason visible.
`buy_price` and `sell_price` mirror Delphi `pBuyOrder.Price` /
`pSellOrder.Price`: they are local desired/replace prices, distinct from
`buy_order.actual_price` / `sell_order.actual_price` and not present in the
compact order snapshot.
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
Delphi's `CheckReplaceFlag` pending path. While the order stays pending,
`MoonClient` keeps repeating the replace-then-cancel pair from its active order
tick no more often than Delphi's 32 ms worker loop.
`TOrderNotFound` sets `cancel_request` and `server_forced_remove` immediately
while the entry is still present. It does not rewrite the compact buy/sell
orders at receive time: Delphi only changes those records later from
`BOrderWorker.DoTheJobVirtual.finally`, after the worker exits its loop.
`job_is_done` is a read-model terminal marker, not Delphi's thread-lifetime
`BOrderWorker.JobIsDone`; during the deferred removal window the Rust entry
still corresponds to a virtual worker that has not returned from
`DoTheJobVirtual`.

## Status Values

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

`OrderWorkerStatus` intentionally preserves the raw Delphi ordinal byte. Known
values use the constants above; future/unknown values remain available through
`.0` / `to_byte()` and are rendered as `Unknown(n)` in debug output.

Terminal statuses are `SelLDone`, `SelLAlmostDone`, `BuyCancel`, `BuyFail`,
`SellCancel`, and `SellFail`.

## Events

```rust
pub enum ApplyResult {
    Applied,
    OutOfOrder,
    PhaseRollback,
    OrderNotFound,
    NotApplicable,
}

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
client updates a per-client `ServerTimeDelta` from Ping packets; the active
runtime links that value into `Orders` automatically.
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

Regular applications should read orders through `MoonClient::snapshot()`.
