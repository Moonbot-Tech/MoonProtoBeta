# Trade Actions

`Client` provides high-level wrappers for outgoing order commands. Use these
instead of manually building `commands::trade::*` payloads: the wrappers set the
right server route, send priority, retry policy, and pending-command
deduplication.

Trade wrappers are typed domain API and are gated by Init. Before
`run_init_sequence` / `connect_and_init` opens `domain_ready`, market-level
wrappers queue no server command, boolean wrappers return `false`, and
market-level multi-order wrappers return `0` where applicable. Stateful helpers
that require `&mut Orders` also do not mutate the local order cache before Init.
Raw `send_cmd` / `send_cmd_keyed` obey the same gate; before Init they reject
non-init commands instead of queueing them.

## Trade Context

```rust
let ctx = client.random_trade_ctx()
    .expect("run BaseCheck before market trade commands");
```

Trade command headers include the server's base-currency and exchange ordinals.
For market-level commands, derive them from the active session with
`Client::trade_ctx(uid)` or `Client::random_trade_ctx()`. These methods return
`TradeContextError` until BaseCheck has filled `client.server_info()`;
`connect_and_init` does that automatically during mandatory init.

For order-keyed commands, `ctx.uid` must be the server task id from `Order.uid`;
that id is what replace/cancel/stops/panic/vstop deduplication uses.
If the command targets an order already present in `EventDispatcher::orders()`,
prefer `order.trade_ctx()` or the `*_tracked_order` wrappers below. They also
preserve the currency/platform bytes carried by the server-side order state.

`TradeCtx::with_route(uid, currency, platform)` is available for low-level tools
that intentionally provide raw server route bytes. There is no default
Binance/USDT shortcut: route bytes must come from `BaseCheck`, tracked order
state, or explicit protocol-tool input.

## Wrappers

| Method | What it sends |
|---|---|
| `trade_ctx(uid)` | Build a `TradeCtx` from `server_info()` route fields. |
| `random_trade_ctx()` | Build a session-derived `TradeCtx` with a random command UID. |
| `new_order(ctx, market, is_short, price, strat_id, order_size)` | Open a new order. |
| `replace_order(&mut orders, uid, new_price)` | Apply Delphi replace request and move an order price if `ReplaceSentTime` allows it. Returns `true` when a command was queued. |
| `replace_tracked_order(&mut orders, uid, new_price)` | Same replace helper for tracked-order call sites. |
| `request_order_snapshot(&mut dispatcher, timeout)` | Request and wait for the current order snapshot. |
| `request_all_statuses(uid)` | Low-level `TAllStatusesReq`; regular consumers should use `request_order_snapshot`. |
| `cancel_order(&mut orders, uid)` | Apply Delphi cancel request and queue cancel for a local pending/buy/sell order. Pending `OS_None` repeats Delphi's replace-then-cancel path. Returns `true` when a command was queued. |
| `cancel_tracked_order(&mut orders, uid)` | Same cancel helper for tracked-order call sites. |
| `join_orders(ctx, market, is_short)` | Join open orders. |
| `split_order(ctx, market, split_parts, split_small, split_small_sell)` | Split an order. |
| `split_tracked_order(order, split_parts, split_small, split_small_sell)` | Split a tracked order. |
| `move_all_sells(&orders, ctx, market, params)` | Move sell orders in bulk if the local order state passes the Delphi active-client send gate. Returns `true` when a command was queued. |
| `do_close_position(ctx, market, market_sell)` | Close a position. |
| `do_limit_close_position(ctx, market, is_short)` | Close through a limit order. |
| `do_split_position(ctx, market, is_short)` | Split a position. |
| `do_sell_order(ctx, market, price, size)` | Send immediate sell command. |
| `request_order_status(ctx, market)` | Request one order status. |
| `request_tracked_order_status(order)` | Request one tracked order status. |
| `update_order_stops(&mut orders, uid, &stops)` | Apply Delphi `SendStopsIfChanged` and update stop settings if changed. Returns `true` when a command was queued. |
| `update_tracked_order_stops(&mut orders, uid, &stops)` | Same stop update helper for tracked-order call sites. |
| `turn_panic_sell(&mut orders, market, turn_on)` | Delphi `TOrdersWorkers.TurnPanicSell`: set panic sell for every local active sell order in the market. Returns the number of queued commands. |
| `switch_panic_sell_by_market(&mut orders, market, turn_on)` | Delphi `TOrdersWorkers.SwitchPanicSellByMarket`: button toggle semantics. Returns whether panic sell is now on. |
| `turn_order_panic_sell(&mut orders, uid, turn_on)` | Apply Delphi per-worker `FPanicSell`/`PrevPanicSell` gate for one local sell order. Returns `true` when a command was queued. |
| `turn_tracked_order_panic_sell(&mut orders, uid, turn_on)` | Same per-order panic-sell helper for tracked-order call sites. |
| `set_immune(&mut orders, items)` | Mark found active orders as click-immune locally and send the matching server command. Returns `true` when a command was queued. |
| `penalty(ctx, market)` | Mark market penalty/cooldown. |
| `move_all_buys(&orders, ctx, market, cmd_type, move_kind, price, side)` | Move buy orders in bulk if the local order state passes the Delphi active-client send gate. Returns `true` when a command was queued. |
| `update_vstop(&mut orders, uid, on, fixed, level, vol)` | Apply Delphi `SendVStopIfChanged` and update volume stop if changed. Returns `true` when a command was queued. |
| `update_tracked_order_vstop(&mut orders, uid, on, fixed, level, vol)` | Same VStop update helper for tracked-order call sites. |
| `do_market_split_position(ctx, market, is_short)` | Market-split a position. |

Epoch is intentionally not part of the public outgoing wrappers. Replace and
panic-sell commands also do not take caller-supplied status: the library derives
the necessary market route/order type from `Orders`.

Replace, cancel, panic-sell, stop/VStop, and `set_immune` are outgoing UI/order
actions with local side effects. They are not raw public sends: UI code should
mutate local order state through these helpers, and the library sends only when
the local state allows it.

`replace_order` requires `&mut Orders` and a local order UID. It derives the
server order type from the current local order: pending `OS_None` sends `O_BUY`,
`OS_BuySet` uses the local buy order type, and `OS_SellSet` uses the local sell
order type. If a replace is already in flight (`ReplaceSentTime > 0`), Rust
updates the local desired price but queues no second packet, matching Delphi.

`cancel_order` derives the current status from the local order. For a pending
`OS_None` order it marks pending cancel locally and performs the same
replace-then-cancel sequence used by the active order worker. If the writer has
not copied the first command yet, send-queue dedup leaves the final cancel in
the queue. In active dispatcher mode the library repeats this pending
replace-then-cancel pair from the order tick every 32 ms or later until the
order leaves `OS_None`.

`turn_panic_sell` is market-level like Delphi `TOrdersWorkers.TurnPanicSell`:
it sets `FPanicSell` for all local active `OS_SellSet` orders in the market, and
queues a panic-sell command only for orders whose `PrevPanicSell` differs.
`switch_panic_sell_by_market` repeats the chart button logic: if any active sell
order in the market already has panic sell on, it turns all of them off;
otherwise, when `turn_on` is true, it turns all of them on. Use
`turn_order_panic_sell` only when the caller intentionally targets one tracked
order worker.

`set_immune` is an outgoing UI/order action with a local side effect. It sets
`immune_for_clicks` before sending and sends nothing if no local active order is
found. Pass `&mut Orders`; the wrapper mutates found active orders before
queueing the command. The command's internal UID is generated by the library and
is not the target order UID.

`update_order_stops` and `update_vstop` are also state-aware outgoing actions.
They require `&mut Orders` and a local order UID, because Delphi does not expose
raw stop/VStop sends from UI code: `BOrderWorker.SendStopsIfChanged` and
`SendVStopIfChanged` first require a local `vOrder`, compare the new values with
`FPrevStops` / `FPrevVStop*`, update the local cache, then send. Rust represents
that `vOrder <> nil` gate as `Order::has_local_visual_order`; server-created
pending orders set it automatically, and callers with their own local
visual-order equivalent can set it through `Orders::mark_local_visual_order(uid)`
after the server UID is known. Rust returns `false` and queues nothing when the
local order is absent, has no local visual-order marker, or the values did not
change. The outgoing status, market name, route, and deduplication key are
derived from the local order.

`move_all_sells` and `move_all_buys` require the current `Orders` read model.
This mirrors Delphi active-client UI code: bulk move commands are not queued
until the client has a matching local order worker. `MoveKind` modes reject
`ReplaceMultiKind::None` and skip immune orders; sell `PriceZone` mode checks for
any non-immune active sell on the market; percent (`Pers`) modes check only that
an active buy/sell exists on the market.

Buy bulk move uses `MoveAllBuysCmdType`, which has only `MoveKind = 0` and
`Pers = 2`. Delphi has no buy-side `PriceZone` mode, and the server buy branch
does not process `CmdType = 1`.

The low-level trade action option types preserve Delphi raw enum ordinals:
`FixedPosition(pub u8)`, `MoveAllCmdType(pub u8)`,
`MoveAllBuysCmdType(pub u8)`, and `ReplaceMultiKind(pub u8)`. Use their named
constants for regular API calls; unknown ordinals can still be parsed and
round-tripped by low-level tools instead of becoming `ParseFailed`.

`move_all_sells` intentionally takes a parameter struct instead of a long
positional argument list. This is part of the public API:

- `MoveAllSellsParams` groups `cmd_type`, `move_kind`, `price`, `price_zone`,
  and `side`.

Low-level builders follow the same shape:
`build_move_all_sells(ctx, market, params)`,
`build_move_all_buys(ctx, market, cmd_type, move_kind, price, side)`, and
`build_vstop_update(ctx, market, epoch, params)`. `VStopUpdateParams` remains
the raw builder parameter type; high-level wrappers derive its `status` from
`Orders`.

## Example

```rust
use moonproto::commands::trade::{
    FixedPosition, ImmuneItem, MoveAllBuysCmdType, MoveAllCmdType, MoveAllSellsParams,
    PriceZone, ReplaceMultiKind,
};

client.replace_tracked_order(dispatcher.orders_mut(), order_uid, 50100.0);
client.cancel_tracked_order(dispatcher.orders_mut(), order_uid);

let ctx = client.random_trade_ctx()
    .expect("run BaseCheck before market trade commands");

client.move_all_sells(
    dispatcher.orders(),
    ctx,
    "BTCUSDT",
    MoveAllSellsParams {
        cmd_type: MoveAllCmdType::PriceZone,
        move_kind: ReplaceMultiKind::All,
        price: 50100.0,
        price_zone: PriceZone { min_p: 49500.0, max_p: 50500.0 },
        side: FixedPosition::Long,
    },
);

client.move_all_buys(
    dispatcher.orders(),
    ctx,
    "BTCUSDT",
    MoveAllBuysCmdType::MoveKind,
    ReplaceMultiKind::TopVol,
    50000.0,
    FixedPosition::Long,
);

let items = [
    ImmuneItem { uid: 100, value: true },
    ImmuneItem { uid: 200, value: true },
];
client.set_immune(dispatcher.orders_mut(), &items);

let mut stops = dispatcher
    .orders()
    .get(order_uid)
    .expect("known order")
    .stops;
stops.stop_loss_on = 1;
stops.sl_level = 49500.0;
dispatcher.orders_mut().mark_local_visual_order(order_uid);
client.update_order_stops(dispatcher.orders_mut(), order_uid, &stops);

client.update_vstop(dispatcher.orders_mut(), order_uid, true, false, 50000.0, 12.0);
```

## Sending While The Client Is Running

`Client` trade wrappers take `&self`, but the long-running pump methods take
`&mut self` for the duration of the run tick. If a terminal sends commands from
another UI thread while `run_with_dispatcher` is active, clone
`client.sender()` before entering the run loop.

`ClientSender` mirrors the fire-and-forget high-level trade wrappers, so UI code
does not need to know send priorities, retry counts, encryption flags, or
deduplication details:

```rust
let sender = client.sender();

std::thread::spawn(move || {
    sender.ui_mm_subscribe(true);
    sender.balance_request_refresh();
});
```

`ClientSender::move_all_sells` and `ClientSender::move_all_buys` take the same
`&Orders` argument as `Client`; replace/cancel/panic/stop/VStop/immune helpers
take the same `&mut Orders` argument as `Client`. Boolean helpers return
`false` and queue nothing when the Delphi active-client gate does not find a
matching local order or when a send-if-changed/replace-in-flight check
suppresses the packet. They also return `false` before Init, without mutating
`Orders`. Market-level `turn_panic_sell` returns the number of queued per-order
commands, or `0` before Init; `switch_panic_sell_by_market` returns the
resulting button state after Init and `false` before Init.
Because `Orders` is the local Delphi-equivalent order-state owner, call these
state-aware helpers on the code path that owns mutable dispatcher state. If UI
code runs on another thread, marshal the intent to that owner instead of
sending raw order-action packets.

One-shot helpers that must wait for an applied state change, such as
`request_order_snapshot`, still require mutable access to `Client` and an
`EventDispatcher` because they pump the UDP loop while waiting.

Raw `ClientSender::send_cmd` / `send_cmd_keyed` remain available for advanced
tools that intentionally build custom payloads with `commands::*`. These calls
append directly into the client's Delphi-style send queues; they do not wait
behind accepted UDP packets or subscription-control events and do not enforce
the typed-domain Init gate.

## Pending Dedup

Some order commands replace older pending commands for the same order before
they leave the local send queue, and the server applies the same idea on its
side. For per-order replace, there is also a stricter in-flight gate: once a
replace was queued for an order, another `replace_order` call updates the local
desired price but does not queue a second packet until the replace response or
timeout clears the local flag.

Wrappers that deduplicate by order UID:

- `replace_order`;
- `cancel_order`;
- `update_order_stops`;
- `turn_panic_sell`;
- `switch_panic_sell_by_market`;
- `turn_order_panic_sell`;
- `update_vstop`.

Replace/cancel/panic/stop/VStop wrappers derive `TradeCtx`, market name,
current status, and order type from `Orders` before queueing, matching Delphi
active-client order workers.

`set_immune` deduplicates by the set of found target order UIDs. Items whose
local active order is not found are not sent. Its internal command UID is not
the target order UID.

## Retry Counts

Most trade wrappers use `MaxRetries = 3`. Position-changing commands that must
not be duplicated by retries use `MaxRetries = 1`:

- `do_close_position`;
- `do_limit_close_position`;
- `do_split_position`;
- `do_sell_order`;
- `do_market_split_position`.
