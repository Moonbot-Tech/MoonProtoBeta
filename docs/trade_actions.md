# Trade Actions

`MoonClient` provides the normal order-intent API. UI code reads immutable
order snapshots and sends user actions back to the runtime by passing the
visible `&Order`:

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
        level: 50000.0,
        volume: 12.0,
    },
)?;
client.orders().set_immune_for_orders([order], true)?;
client.orders().turn_panic_sell(order, true)?;
client.orders().request_status(order)?;
client.orders().switch_panic_sell_by_market("BTCUSDT", true)?;
```

The runtime owner applies the intent to the live `Orders` state first, then
queues the protocol command only when the current order state allows it. This is
the Rust Active Lib equivalent of Delphi UI/worker behavior: the application
does not mutate a snapshot and does not pass `&mut Orders` around.

## UI Pattern

```rust
if let Some(snapshot) = client.snapshot() {
    if let Some(order) = snapshot.orders().get(ui_state.selected_order_uid()) {
        println!(
            "buy_actual={} buy_qty={} sell_actual={} sell_qty={}",
            order.buy_order.actual_price,
            order.buy_order.quantity,
            order.sell_order.actual_price,
            order.sell_order.quantity
        );
        client.orders().move_order(order, new_price)?;
    }
}
```

Snapshots are display/read models. They are safe to keep in UI state, but they
are not the live order-worker state. The live state remains inside the runtime,
where replace-in-flight, pending cancel, previous Stops/VStop, panic, and immune
flags are checked exactly once before sending.

## Market Trade Intents

New orders and market-level actions use `client.trade()`. User code does not
build `TradeCtx`; the runtime derives the route bytes learned during Init
BaseCheck:

```rust
use moonproto::{NewOrderParams, OrderSide};

let ticket = client.trade().new_order(
    NewOrderParams::new("BTCUSDT", OrderSide::Long, 50_000.0, 0.001)
        .with_strategy_id(strategy_id),
)?;
println!("queued new-order request uid={}", ticket.request_uid);

client.trade().join_orders("BTCUSDT", OrderSide::Long)?;
client.trade().limit_close_position("BTCUSDT", OrderSide::Long)?;
client.trade().penalty("BTCUSDT")?;
```

Bulk buy/sell moves use named constructors for the trader-visible mode. The
runtime still serializes the exact Delphi packet mode internally:

```rust
use moonproto::{FixedPosition, MoveAllBuysParams, ReplaceMultiKind};

client.trade().move_all_buys(
    "BTCUSDT",
    MoveAllBuysParams::replace_kind(ReplaceMultiKind::TopVol, 50_100.0, FixedPosition::Long),
)?;
```

If Init/BaseCheck route fields are unavailable, these methods return
`MoonClientError::TradeContext` instead of exposing `TradeCtx` to application
code.

`new_order` returns a client-side ticket. Its `request_uid` is the UID written
into the outgoing command and can be used to correlate the user click with the
server-created order when the order appears in `snapshot().orders()`.

Order intent handles also accept a raw UID for CLI tools and scripts that only
have an identifier. Desktop UI should prefer the visible `&Order` it already
draws; the runtime still resolves that selector against the live order state
before sending.

## Init Gate

`MoonClient::connect` starts the runtime immediately, while the one-time
connect/init sequence finishes in that runtime thread. UI code may enqueue order
intents during startup; the runtime handles them only after the retained state is
ready. If the live order no longer exists or its current state does not allow the
requested action, the action is ignored and an `OrderEvent::Ignored` diagnostic
event may be published for that UID.

After Init, actions append to the Delphi-style unbounded send queues and
reconnect keeps the session state alive automatically.

## Command Semantics

- `move_order` derives market route, order type, current status, and dedup key
  from live `Orders`.
- `cancel` derives the current status from live `Orders`; pending orders use the
  Delphi replace-then-cancel path.
- `update_stops` and `update_vstop` compare against previous local values and
  send only when something changed.
- `set_immune_for_orders` updates only found active local orders and sends
  nothing if no target order exists.
- panic-sell methods update live local panic flags before sending.
- `client.trade().new_order`, join/split/close/sell/penalty commands derive
  `TradeCtx` from the session route and do not require caller-supplied protocol
  ordinals.
- `move_all_sells` and `move_all_buys` read the live order state and send only
  when the same Delphi active-client pre-send gates find a candidate order.

Epoch/status/route fields are intentionally not caller-supplied in the normal
API. They come from BaseCheck and the tracked order state.

## Retry Counts

Most trade/order actions use the Delphi retry policy for the matching command.
Position-changing commands that must not be duplicated by retries use the lower
retry count from the wire command definition. The high-level API selects this
automatically; applications should not choose retry counts for normal trading
actions.
