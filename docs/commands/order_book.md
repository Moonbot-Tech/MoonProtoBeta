# MPC_OrderBook (cmd=36) — Order Book Updates

Server → Client. Order book snapshots and diffs. Always SynLZ-compressed.

## Packet Structure (after SynLZ decompression)

```
MarketIndex  (2 bytes, u16 LE) — market identifier
Seq          (2 bytes, u16 LE) — monotonic sequence per market+bookKind
Flags        (1 byte):
  bit 0: IsFull (1=full snapshot, 0=diff)
  bit 1: BookKind (0=Futures, 1=Spot)
```

## Glass Data (after header)

```
BuyCount     (2 bytes, u16 LE)
Buy levels   (BuyCount × 8 bytes):
  Rate       (4 bytes, f32 LE) — price level
  Quantity   (4 bytes, f32 LE) — volume at level
Sell levels  (remaining bytes ÷ 8):
  Rate       (4 bytes, f32 LE)
  Quantity   (4 bytes, f32 LE)
```

Note: Sell count is implicit — calculated from remaining bytes after buy levels.

## Full vs Diff

### Full Snapshot (IsFull=1)
- Buy/Sell arrays represent the complete order book state
- Client replaces its cached glass entirely

### Diff (IsFull=0)
- Buy/Sell arrays contain changed levels
- Qty=0 means remove level
- Qty>0 means add/update level
- Client applies diff to its cached glass using merge-by-rate

## Sequence Ordering

- `CompareSeq(a, b) = SmallInt(a - b)` — wrapping-safe comparison
- Client tracks expected seq per market+bookKind
- Sequential: apply immediately
- Out-of-order: cache (up to 64 packets)
- Gap > 800ms: enter CORRUPTED mode, request Full

## Client Cache States

| State | Behavior |
|-------|----------|
| NORMAL | Apply sequential diffs, cache out-of-order |
| CORRUPTED | Apply diffs best-effort, request Full with 5s throttle |
| FULL received | Apply snapshot, drain cache, return to NORMAL |

## Subscription

Client subscribes via Engine API:
- `emk_SubscribeOrderBook` — batch subscribe (array of market names)
- `emk_UnsubscribeOrderBook` — batch unsubscribe
- `emk_RequestOrderBookFull` — force full snapshot
- `emk_ReloadOrderBook` — force exchange re-request
