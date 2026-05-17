# MPC_TradesStream (cmd=33) — Real-time Trade Feed

Server → Client. High-frequency market trades, liquidations, and market maker orders.

## Packet Structure

The raw packet has a **flags byte at the END** (last byte of the stream):

```
[Compressed or raw data] + Flags (1 byte)
```

### Flags byte (last byte):
| Bit | Name | Meaning |
|-----|------|---------|
| 0 | COMPRESSED | Payload is SynLZ-compressed |
| 1 | HAS_TAKER | MMOrders contain Taker address (20 bytes per order) |

## After Decompression

```
BaseTime     (8 bytes, f64 TDateTime) — reference time for all trades in packet
PacketNum    (2 bytes, u16 LE)        — monotonic counter for gap detection
Sections[]                            — repeated until end of stream
```

## Section Format

Each section starts with:
```
MarketIndexAndFlags (2 bytes, u16 LE):
  bits 0-13:  MarketIndex (0..16383)
  bits 14-15: SectionType:
    00 = Futures trades
    01 = MMOrders (market maker)
    10 = Spot trades
    11 = Extended section
```

### SectionType 00/10 — Trades (Futures/Spot)

```
Count        (1 byte, u8, 0..255)
Per trade (10 bytes each):
  TimeDelta  (2 bytes, i16 LE) — milliseconds offset from BaseTime
  Price      (4 bytes, f32 LE)
  Qty        (4 bytes, f32 LE) — negative = SELL, positive = BUY
```

### SectionType 01 — MMOrders

```
Count        (1 byte, u8)
Per order:
  TimeDelta  (2 bytes, i16 LE)
  vol        (4 bytes, f32 LE)
  Q          (4 bytes, f32 LE)
  [if HAS_TAKER flag]: Taker (20 bytes, address)
```

### SectionType 11 — Extended

```
ExtType      (1 byte, u8)
```

#### ExtType 0: Liquidation Orders
```
Count        (1 byte, u8)
Per order (10 bytes):
  TimeDelta  (2 bytes, i16 LE)
  Price      (4 bytes, f32 LE)
  Qty        (4 bytes, f32 LE) — signed = direction
```

#### ExtType 1: Watcher Fills
```
User         (20 bytes, address)
Count        (1 byte, u8)
Per fill (20 bytes):
  TimeDelta  (2 bytes, i16 LE)
  Price      (4 bytes, f32 LE)
  Qty        (4 bytes, f32 LE)
  zBTC       (4 bytes, f32 LE)
  Position   (4 bytes, f32 LE)
  oType      (1 byte)
  Flags      (1 byte): bit0=IsShort, bit1=IsOpen, bit2=IsTaker
```

## Gap Detection

PacketNum is monotonic (wraps at u16 max). Client tracks:
- Sequential packet: just advance counter
- Gap detected: request resend via emk_TradesResend
- Out-of-order: fill gap bucket

## Resend Response (MPC_TradesResendResponse, cmd=35)

```
BatchCount   (1 byte, u8)
Per packet:
  sz         (2 bytes, u16 LE)
  data       (sz bytes) — original packet data
```
