# Trade Actions

`Client` provides high-level wrappers for outgoing order commands. Use these
instead of manually building `commands::trade::*` payloads: the wrappers set the
correct command class, encryption, priority, retry count, and UKey dedup.

## Trade Context

```rust
use moonproto::commands::trade::TradeCtx;

let ctx = TradeCtx::new(order_uid);
```

For order-keyed commands, `ctx.uid` must be the server task id from `Order.uid`.
That id is what UKey dedup uses for replace/cancel/stops/panic/vstop commands.

## Wrappers

| Method | What it sends |
|---|---|
| `new_order(ctx, market, is_short, price, strat_id, order_size)` | Open a new order. |
| `replace_order(ctx, market, order_type, new_price)` | Move an order price. |
| `request_all_statuses(uid)` | Request full order snapshot. |
| `cancel_order(ctx, market, status)` | Cancel an order. |
| `join_orders(ctx, market, is_short)` | Join open orders. |
| `split_order(ctx, market, split_parts, split_small, split_small_sell)` | Split an order. |
| `move_all_sells(ctx, market, cmd_type, move_kind, price, zone, side)` | Move sell orders in bulk. |
| `do_close_position(ctx, market, market_sell)` | Close a position. |
| `do_limit_close_position(ctx, market, is_short)` | Close through a limit order. |
| `do_split_position(ctx, market, is_short)` | Split a position. |
| `do_sell_order(ctx, market, price, size)` | Send immediate sell command. |
| `request_order_status(ctx, market)` | Request one order status. |
| `update_order_stops(ctx, market, status, &stops)` | Update stop settings. |
| `turn_panic_sell(ctx, market, turn_on)` | Toggle panic sell. |
| `set_immune(uid, items)` | Mark orders immune to clicks. |
| `penalty(ctx, market)` | Mark market penalty/cooldown. |
| `move_all_buys(ctx, market, cmd_type, move_kind, price, side)` | Move buy orders in bulk. |
| `update_vstop(ctx, market, status, on, fixed, level, vol)` | Update volume stop. |
| `do_market_split_position(ctx, market, is_short)` | Market-split a position. |

Epoch is intentionally not part of the public outgoing wrappers. For replace and
panic-sell commands, status is not public either: the Delphi client writes
`epoch = 0` and `status = OS_None` for those commands.

## Example

```rust
use moonproto::commands::trade::{
    FixedPosition, ImmuneItem, MoveAllCmdType, OrderType, PriceZone, ReplaceMultiKind,
    TradeCtx,
};

let order = dispatcher.orders().get(order_uid).expect("known order");
let ctx = TradeCtx::new(order.uid);

client.replace_order(
    ctx,
    &order.market_name,
    OrderType::Sell,
    50100.0,
);

client.cancel_order(ctx, &order.market_name, order.status);

client.move_all_sells(
    ctx,
    "BTCUSDT",
    MoveAllCmdType::PriceZone,
    ReplaceMultiKind::All,
    50100.0,
    PriceZone { min_p: 49500.0, max_p: 50500.0 },
    FixedPosition::Long,
);

let items = [
    ImmuneItem { uid: 100, value: true },
    ImmuneItem { uid: 200, value: true },
];
client.set_immune(rand::random(), &items);
```

## UKey Dedup

Commands with the same UKey replace older pending commands in the client queues,
and the server also deduplicates by UKey. This matters for UI actions like
dragging an order price: many quick `replace_order` calls collapse to the latest
intent instead of executing as independent actions.

Wrappers that use `UK_OrderMove(ctx.uid)`:

- `replace_order`;
- `cancel_order`;
- `update_order_stops`;
- `turn_panic_sell`;
- `update_vstop`.

`set_immune` uses `UK_ImmuneClicks(sum(items[].uid))`.

## Retry Counts

Most trade wrappers use `MaxRetries = 3`. Position-changing commands that must
not be duplicated by retries use `MaxRetries = 1`:

- `do_close_position`;
- `do_limit_close_position`;
- `do_split_position`;
- `do_sell_order`;
- `do_market_split_position`.
