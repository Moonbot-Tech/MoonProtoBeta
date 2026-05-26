# Trade Actions

`MoonClient` provides the normal order-intent API. UI code reads immutable
order snapshots, keeps the order UID, and sends user actions back to the runtime:

```rust
client.orders().move_order(order_uid, new_price)?;
client.orders().cancel(order_uid)?;
client.orders().update_stops(order_uid, stops)?;
client.orders().update_vstop(order_uid, true, false, 50000.0, 12.0)?;
client.orders().set_immune(items)?;
client.orders().turn_panic_sell(order_uid, true)?;
client.orders().switch_panic_sell_by_market("BTCUSDT", true)?;
```

The runtime owner applies the intent to the live `Orders` state first, then
queues the protocol command only when the current order state allows it. This is
the Rust Active Lib equivalent of Delphi UI/worker behavior: the application
does not mutate a snapshot and does not pass `&mut Orders` around.

## UI Pattern

```rust
if let Some(snapshot) = client.snapshot() {
    if let Some(order) = snapshot.orders().get(order_uid) {
        println!("price={} qty={}", order.price, order.quantity);
    }
}

client.orders().move_order(order_uid, new_price)?;
```

Snapshots are display/read models. They are safe to keep in UI state, but they
are not the live order-worker state. The live state remains inside the runtime,
where replace-in-flight, pending cancel, previous Stops/VStop, panic, and immune
flags are checked exactly once before sending.

## Init Gate

Trade actions are gated by Init. Before the one-time Init opens `domain_ready`,
typed order actions return `false` and queue no server command. After Init,
actions append to the Delphi-style unbounded send queues and reconnect keeps the
session state alive automatically.

## Command Semantics

- `move_order` derives market route, order type, current status, and dedup key
  from live `Orders`.
- `cancel` derives the current status from live `Orders`; pending orders use the
  Delphi replace-then-cancel path.
- `update_stops` and `update_vstop` compare against previous local values and
  send only when something changed.
- `set_immune` updates only found active local orders and sends nothing if no
  target order exists.
- panic-sell methods update live local panic flags before sending.

Epoch/status/route fields are intentionally not caller-supplied in the normal
API. They come from BaseCheck and the tracked order state.

## Advanced Tools

Low-level `Client`, `ClientSender`, `commands::trade::*`, `TradeCtx`, and
`&mut Orders` helpers remain available for custom runtimes, protocol tests, and
wire-format tools. They are not the regular UI integration model. If a tool owns
`Client + EventDispatcher` directly, it is also responsible for keeping the
protocol pump alive and for applying stateful order actions to the live
dispatcher state.

One-shot helpers that wait for an applied state change, such as the low-level
order snapshot request, still live on `Client` because they intentionally pump a
caller-owned runtime while waiting.

## Retry Counts

Most trade/order actions use the Delphi retry policy for the matching command.
Position-changing commands that must not be duplicated by retries use the lower
retry count from the wire command definition. The high-level API selects this
automatically; applications should not choose retry counts for normal trading
actions.
