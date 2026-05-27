# Time Values

MoonProto uses Delphi `TDateTime` on the wire: `f64` days since `1899-12-30`.
That value is not Unix time.

Public history rows keep the raw Delphi value where it matches the protocol and
keeps storage dense. Application code should convert through `DelphiTime` or row
helpers instead of treating the raw number as seconds or milliseconds.

```rust
use moonproto::DelphiTime;

let dt = DelphiTime::from_days(raw_days);
let unix_ms = dt.unix_millis();
let system_time = dt.system_time();
```

Common retained rows expose helper methods:

```rust
let trade_ms = trade.time_delphi().unix_millis();
let candle_ms = candle.time_delphi().unix_millis();
let last_price_time = point.time_delphi().system_time();
```

`DelphiTime::as_days()` returns the exact raw Delphi value when a diagnostic or
protocol tool needs byte-level compatibility.
