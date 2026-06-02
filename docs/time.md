# Time Values

MoonProto public API uses `MoonTime`: a compact Unix-milliseconds timestamp.
It is cheap to copy, works naturally with Rust/UI code, and can be converted to
`SystemTime` when a framework needs it.

The protocol still uses Delphi `TDateTime` on the wire (`f64` days since
`1899-12-30`). That is converted at packet boundaries. Application code should
not store or compare Delphi-day floats.

```rust
use moonproto::MoonTime;

let now = MoonTime::now();
let unix_ms = now.unix_millis();
let unix_seconds = now.unix_seconds();
let system_time = now.system_time();
```

Common retained rows expose helper methods:

```rust
let trade_ms = trade.time().unix_millis();
let candle_ms = candle.time().unix_millis();
let last_price_time = point.time().system_time();
let order_open_ms = order.buy_order.open_time().unix_millis();
let trace_ms = chart_point.time().unix_millis();
```

Diagnostic builds keep hidden Delphi-time helpers for byte-level protocol tests.
They are not the normal terminal API.
