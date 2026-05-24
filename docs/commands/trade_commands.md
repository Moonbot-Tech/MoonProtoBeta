# MPC_Order — Trading Commands

`MPC_Order` (channel byte 28) is the trading command channel. It contains 30
sub-types, each identified by a unique `CmdId` (1..30). The channel is
bidirectional: the client sends commands such as new order, cancel, and replace;
the server sends updates such as order status, full order snapshots, and
not-found notifications.

`CmdId` values map to variants of the `commands::trade::TradeCommand` enum.
The large server-to-client variants are boxed in the public enum:
`TradeCommand::OrderStatus(Box<OrderStatus>)` and
`TradeCommand::OrderReplaceResponse(Box<OrderReplaceResponse>)`. This keeps
`TradeCommand` cheap to move through event/state queues without changing the
wire format or the inner structs.

## Wire format

### Common Header

```
[CmdId]          — 1 byte  — sub-command identifier (1..30)
[ver=3]          — 2 bytes LE — protocol version
[UID]            — 8 bytes LE — order task_id, or random for non-keyed commands
[class-specific payload...] — variable
```

Version gate: when `ver > 3`, the command is parsed as
`TradeCommand::Unknown { cmd_id, uid }` and skipped for forward compatibility.
When `ver <= 3`, the command is parsed fully.

Packed record wire formats are described in `SPEC.md §10.2`:
- `OrderCompact` — 117 bytes (used by `OrderStatus`, `OrderStatusUpdate`, and `AllStatuses`)
- `StopSettings` — 46 bytes (used by `OrderStopsUpdate`)
- `OrderUpdateData` — 66 bytes (used by `OrderStatusUpdate`)
- `PriceZone` — 16 bytes (used by `CorridorUpdate`)
- `ImmuneItem` — 9 bytes (used by `SetImmune`)

## CmdId Table

The `CmdId` values are taken directly from `TradeCommand::parse`
(`commands/trade.rs`). The version 3 parser knows all 30 values.

| CmdId | Variant | Direction | Description | Rust struct |
|-------|---------|-----------|----------|-------------|
| 1 | `BaseMarket` | n/a | Ancestor type (raw `MarketCommandHeader`); not used as a standalone wire command. | `MarketCommandHeader` |
| 2 | `TradeEpoch` | n/a | Ancestor type (raw `TradeEpochHeader`); not used as a standalone wire command. | `TradeEpochHeader` |
| 3 | `NewOrder` | **C→S** | Open a new order. | `NewOrderCommand` |
| 4 | `OrderStatus` | **S→C** | Full order snapshot, used for creation and reconnect recovery. | `OrderStatus` (contains 117-byte `OrderCompact`) |
| 5 | `OrderStatusUpdate` | **S→C** | Delta update for order fields. | `OrderStatusUpdate` (contains 66-byte `OrderUpdateData`) |
| 6 | `OrderReplace` | **C→S** | Replace an order with a new price. | `OrderReplaceCommand` |
| 7 | `OrderReplaceResponse` | **S→C** | Server acknowledgement for an order replace. | `OrderReplaceResponse` |
| 8 | `AllStatuses` | **S→C** | Snapshot of all orders, used by client-side `CleanupMissing`. | `AllStatuses` (`OrderCompact` array) |
| 9 | `AllStatusesRequest` | **C→S** | Request all order statuses. | `BaseCommandHeader` |
| 10 | `OrderCancel` | **C→S** | Cancel an order. | `OrderCancelCommand` |
| 11 | `JoinOrders` | **C→S** | Join open orders into one position. | `JoinOrdersCommand` |
| 12 | `SplitOrder` | **C→S** | Split a position into N parts. | `SplitOrderCommand` |
| 13 | `MoveAllSells` | **C→S** | Batch-move all sell orders. | `MoveAllSellsCommand` |
| 14 | `DoClosePosition` | **C→S** | Close a position with a market order. | `DoClosePositionCommand` |
| 15 | `DoLimitClosePosition` | **C→S** | Close a position with a limit order. | `JoinOrdersCommand` (reuses payload format) |
| 16 | `DoSplitPosition` | **C→S** | Split-close a position. | `JoinOrdersCommand` (reused) |
| 17 | `DoSellOrder` | **C→S** | Direct sell order with price and size. | `DoSellOrderCommand` |
| 18 | `OrderStatusRequest` | **C→S** | Request a specific order by UID for `CleanupMissing`. | `TradeEpochHeader` |
| 19 | `OrderNotFound` | **S→C** | The server reports that the order with this UID was not found. | `TradeEpochHeader` |
| 20 | `OrderStopsUpdate` | **C→S** or **S→C** | Stop settings update (SL/TP). The client sends changes; the server sends echo/notify updates. | `OrderStopsUpdate` (contains 46-byte `StopSettings`) |
| 21 | `TurnPanicSell` | **C→S** | Enable or disable panic-sell mode. | `TurnPanicSellCommand` |
| 22 | `SetImmune` | **C→S** | Mark orders as immune to UI clicks. | `SetImmuneCommand` (`ImmuneItem` array, 9 bytes each) |
| 23 | `Penalty` | **C→S** | Mark a market as penalized or cooled down. | `MarketCommandHeader` |
| 24 | `TradeVisual` | **S→C** | Visual-only command, used as a base type for diagnostic packets. | `MarketCommandHeader` |
| 25 | `OrderTracePoint` | **S→C** | Point in an order trace chart for UI visualization. | `OrderTracePoint` |
| 26 | `CorridorUpdate` | **S→C** | Price corridor update for a position. | `CorridorUpdate` (contains 16-byte `PriceZone`) |
| 27 | `MoveAllBuys` | **C→S** | Batch-move all buy orders. | `MoveAllBuysCommand` |
| 28 | `BulkReplaceNotify` | **S→C** | Notification with bulk replace results. | `BulkReplaceNotify` |
| 29 | `VStopUpdate` | **C→S** or **S→C** | Virtual stop update. | `VStopUpdate` |
| 30 | `DoMarketSplitPosition` | **C→S** | Market-split a position. | `JoinOrdersCommand` (reused) |

**Direction note:** some commands (`OrderStopsUpdate`, `VStopUpdate`) travel in
both directions. The client sends local updates, and the server sends echo or
notify updates for changes made by another client or by the engine.

## Bulk Move CmdType

`MoveAllSellsCommand` uses `MoveAllCmdType`:

- `0` = `MoveKind`;
- `1` = `PriceZone`;
- `2` = `Pers`.

`MoveAllBuysCommand` uses `MoveAllBuysCmdType`:

- `0` = `MoveKind`;
- `2` = `Pers`.

There is no buy-side `PriceZone` mode in Delphi. The Rust public builder and
client wrappers use the separate `MoveAllBuysCmdType` type so regular API code
does not name a buy command with `CmdType = 1`. The wire types preserve raw
Delphi ordinals, so parsed future/unknown byte values remain available instead
of being collapsed or rejected.

## Order state machine

See the `OrderWorkerStatus` doc comment in
`commands::trade::OrderWorkerStatus`:

```text
None ──► BuySet ──► BuyDone ──► SellSet ──► SelLAlmostDone ──► SelLDone
          │           │           │            │
          ▼           ▼           ▼            ▼
       BuyFail    BuyCancel   SellFail    SellCancel
```

**Terminal states:** `SelLDone`, `SelLAlmostDone`, `BuyFail`, `BuyCancel`,
`SellFail`, `SellCancel`.

## UKey Deduplication

Some commands have the `[MoonCmdUnique]` attribute in Delphi, which enables
unique-key deduplication in the send queue. If `replace_order` is sent five
times in quick succession, only the **last version** remains queued
(`UK_OrderMove` dedup by `task_id`). This is useful for UI drag-replace flows:
the UI may generate a stream of updates, but only the final value is sent to the
server.

| Command | UKey |
|---------|------|
| `OrderReplace` (CmdId 6) | `UK_OrderMove(task_id)` |
| `OrderCancel` (CmdId 10) | `UK_OrderMove(task_id)` |
| `OrderStopsUpdate` (CmdId 20) | `UK_OrderMove(task_id)` |
| `TurnPanicSell` (CmdId 21) | `UK_OrderMove(task_id)` |
| `VStopUpdate` (CmdId 29) | `UK_OrderMove(task_id)` |
| `SetImmune` (CmdId 22) | `UK_ImmuneClicks(items_uid_sum)` |

All other commands are sent without deduplication.

## Priority and Retries

All Order commands are sent as:
- **Encrypted** (envelope Crypted + AES-GCM)
- **Priority** = High (fast delivery, ACK piggybacked through Ping)
- **MaxRetries** = 3, except `DoClose*` (CmdId 14-17, 30), where
  **MaxRetries = 1** because close-position commands are dangerous to retry many
  times.

These parameters are built into the `Client` wrappers (`client.new_order`,
`client.cancel_order`, and others); consumers do not set them manually.

## EventDispatcher and Typed Events

`EventDispatcher` automatically parses incoming `MPC_Order` packets and updates
the `Orders` sync state. Consumers receive `Event::Order(OrderEvent)` through
`dispatcher.dispatch(cmd, payload, now_ms)`.

`OrderEvent` variants (see `state::orders::OrderEvent`):
- `Created` — new order after the first `OrderStatus` for this `task_id`
- `Updated` — field update from `OrderStatusUpdate` or a repeated `OrderStatus`
- `Removed` — emitted after deferred cleanup for `OrderNotFound`, a terminal status, or an explicit server removal
- `TracePoint` — received `OrderTracePoint`
- ... see the full list in `state/orders.rs`

**Note:** `OrderEvent` is a **high-level** API for UI consumers, not a raw wire
command. The wire `CmdId` to `OrderEvent` mapping is handled inside
`Orders::apply`.

## Wire Format Details

Full byte layouts for each sub-command and packed record:
- **OrderCompact** (117 bytes) — contains UID, `task_id`, `market_id`, status,
  price, size, filled amount, stops, and related order fields.
- **StopSettings** (46 bytes) — SL/TP prices and flags.
- **OrderUpdateData** (66 bytes) — delta fields for `OrderStatusUpdate`.
- **PriceZone** (16 bytes) — top and bottom prices of the corridor.
- **ImmuneItem** (9 bytes) — order UID plus the immune flag.

Exact offsets and field types are listed in the internal `SPEC.md §10.2`,
together with Delphi source references used to verify byte-exact wire
compatibility.
