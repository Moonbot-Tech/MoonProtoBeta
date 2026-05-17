# MPC_Balance (cmd=32) — Account Balance Updates

Server → Client. Balance snapshots and incremental updates with bitmask optimization.

## Sub-commands

| CmdId | Class | Direction | Description |
|-------|-------|-----------|-------------|
| 001 | Base | S→C | Base with epoch + global balances |
| 002 | Command | S→C | Full items (legacy) |
| 003 | SnapshotFull | S→C | Full map (every 10 sec, sliced) |
| 004 | IncrUpdate | S→C | Incremental changes only (high priority) |
| 005 | RequestRefresh | C→S | Trigger server to send fresh snapshot |
| 006 | ArbPrices | S→C | Raw payload relay (low priority) |

## Balance Snapshot/Command (CmdId 002/003)

```
[Command header: CmdId(1) + ver(2) + UID(8)]
Epoch              (2 bytes, u16 LE)
BTCBalanceTotal    (8 bytes, f64 LE)
BTCBalanceLocked   (8 bytes, f64 LE)
BTCBalanceFull     (8 bytes, f64 LE)
SpecialCoinBalance (8 bytes, f64 LE)
Count              (4 bytes, i32 LE)
Items[]            (Count × TBalanceItem)
```

## Incremental Update (CmdId 004)

```
[Command header: CmdId(1) + ver(2) + UID(8)]
Epoch              (2 bytes, u16 LE)
GlobalChanged      (1 byte, bool)
[if GlobalChanged]:
  BTCBalanceTotal    (8 bytes, f64 LE)
  BTCBalanceLocked   (8 bytes, f64 LE)
  BTCBalanceFull     (8 bytes, f64 LE)
  SpecialCoinBalance (8 bytes, f64 LE)
Count              (4 bytes, i32 LE)
Items[]            (Count × TBalanceItem)
```

## TBalanceItem — Bitmask-Optimized Per-Market Data

```
MarketName   (2+N bytes, UTF-8 string with u16 length prefix)
BalanceHash  (8 bytes, u64 LE) — hash for change detection
Flags        (4 bytes, u32 LE) — bitmask: which fields are present
[Fields]     — only fields with corresponding bit set in Flags
```

### Field Order (bit index in Flags):

| Bit | Field | Type | Default | Description |
|-----|-------|------|---------|-------------|
| 0 | InitialBalance | f64 | 0 | Available balance |
| 1 | LockedBalance | f64 | 0 | Locked in orders |
| 2 | FPosSize | f64 | 0 | BOTH position size |
| 3 | FPosPrice | f64 | 0 | BOTH position entry price |
| 4 | FLiqPrice | f64 | 0 | BOTH liquidation price |
| 5 | FPosDir | u8 | 0 | BOTH position direction |
| 6 | LongPosSize | f64 | 0 | Long position size |
| 7 | LongPosPrice | f64 | 0 | Long entry price |
| 8 | LongLiqPrice | f64 | 0 | Long liquidation price |
| 9 | LongPositionType | u8 | 0 | Long position type |
| 10 | ShortPosSize | f64 | 0 | Short position size |
| 11 | ShortPosPrice | f64 | 0 | Short entry price |
| 12 | ShortLiqPrice | f64 | 0 | Short liquidation price |
| 13 | ShortPositionType | u8 | 0 | Short position type |
| 14 | AssetBalance | f64 | 0 | Spot asset balance |
| 15 | AssetBalanceFull | f64 | 0 | Spot full balance |
| 16 | FTotalProfitB | f64 | 0 | Total profit (BOTH) |
| 17 | FTotalProfitL | f64 | 0 | Total profit (Long) |
| 18 | FTotalProfitS | f64 | 0 | Total profit (Short) |
| 19 | bnMaxValue | f64 | 0 | Max position value |
| 20 | LeverageX | i32 | 1 | Leverage multiplier |
| 21 | PositionType | u8 | 0 | Position type (Cross/Isolated) |

Only fields where value ≠ default are written. Reader checks bit before reading.
This reduces typical balance update from ~180 bytes to ~30-50 bytes.
