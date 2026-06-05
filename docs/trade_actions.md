# Trade Actions

`MoonClient` provides the normal order-intent API. UI code reads immutable
order snapshots and sends user actions back to the runtime by passing the
visible `&Order`:

```rust
use moonproto::{StopSettings, VStopParams};

let Some(snapshot) = client.snapshot() else { return; };
let Some(order) = snapshot.orders().get(ui_state.selected_order_uid()) else { return; };
let Some(market) = snapshot.markets().find("BTC") else { return; };
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
client.orders().set_immune_for_orders([order], true)?;
client.orders().turn_panic_sell(order, true)?;
client.orders().request_status(order)?;
client.orders().switch_panic_sell_for_market(&market, true)?;
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
use moonproto::{ClosePositionParams, NewOrderParams, OrderSide, SplitOrderParams};

let Some(snapshot) = client.snapshot() else { return; };
let Some(market) = snapshot.markets().find("BTC") else { return; };

let _ticket = client.trade().new_order(
    NewOrderParams::for_market(&market, OrderSide::Long, 50_000.0, 0.001)
        .with_strategy_id(strategy_id),
)?;

client.trade().join_orders_for_market(&market, OrderSide::Long)?;
client.trade().split_order(SplitOrderParams::for_market(&market, 3))?;
client.trade().close_position(ClosePositionParams::for_market(&market))?;
client.trade().close_position(ClosePositionParams::market_order_for_market(&market))?;
client.trade().limit_close_position_for_market(&market, OrderSide::Long)?;
client.trade().penalty_for_market(&market)?;
```

Bulk buy/sell moves use named constructors for the trader-visible mode. The
runtime still serializes the exact Delphi packet mode internally:

```rust
use moonproto::{BulkMoveKind, MoveAllBuysParams, PositionFilter};

client.trade().move_all_buys_for_market(
    &market,
    MoveAllBuysParams::replace_kind(BulkMoveKind::TopVolume, 50_100.0, PositionFilter::Long),
)?;
```

If Init/BaseCheck route fields are unavailable, these methods return
`MoonClientError::TradeContext` instead of exposing `TradeCtx` to application
code.

Manual strategy mode is an application decision, matching MoonBot UI behavior.
When settings say `use_manual_strategy` and the trader selected
`manual_strategy_id`, pass that id with `NewOrderParams::with_strategy_id`.
Leaving the strategy id empty means a pure manual order without strategy
management; it is not the same as "manual order under the selected strategy".
If the manual strategy sell-percent control changes, send the retained strategy
update through `client.strategies().sell_price_update(...)`.

`new_order` returns a client-side ticket with an outbound/local
`client_order_id`. The typed order stream does not echo this value, so it is not
a reliable click-to-order UID mapping. Normal order tables should treat the
order snapshot as the source of truth, key real orders by server `uid`, and
redraw from `snapshot().orders()`.

Order intent handles also accept a raw UID for CLI tools and scripts that only
have an identifier. Desktop UI should prefer the visible `&Order` it already
draws; the runtime still resolves that selector against the live order state
before sending.

Market-level helpers have the same split: terminal UI should keep the selected
`MarketHandle` and call `*_for_market` methods or `...Params::for_market`.
String-keyed methods remain for scripts and one-shot tools.

`SplitOrderParams::for_market` means the normal equal-parts split. The small
strategy-piece buttons use `strategy_piece_for_market` or
`strategy_piece_and_sell_for_market`, so application code does not pass raw
split-mode booleans.

`ClosePositionParams::for_market` means the Delphi default: place closing limit
orders for the current position. Use `market_order_for_market` only for the
explicit force-market-close button.

## Init Gate

`MoonClient::connect` starts the runtime immediately, while the one-time
connect/init sequence finishes in that runtime thread. UI code may enqueue order
intents during startup; the runtime handles them only after the retained state is
ready. If the live order no longer exists or its current state does not allow the
requested action, the action becomes a no-op. Normal UI code keeps rendering the
retained order snapshot; low-level rejected-action telemetry is available only
in `test`/`diagnostics` builds.

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
