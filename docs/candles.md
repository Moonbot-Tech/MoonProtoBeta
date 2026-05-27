# Candles

For normal Active Lib usage, candles are retained market history, not a raw
chunked response object.

When an application subscribes to trades through `subscribe_all_trades` or
`subscribe_trades_for`, the runtime also requests the initial full 5m candles
snapshot. After the chunked response is merged and parsed, Active Lib applies it
to retained per-market history. Incoming trades then maintain the current 5m
candle and derived candle/volume state.

```rust
use moonproto::TradesStreamMode;

client.subscribe_trades_for(TradesStreamMode::TradesOnly, ["BTCUSDT"])?;

// Later, after events/snapshot refresh:
let Some(state) = client.snapshot() else { return; };

if let Some(readers) = state.market_history_readers("BTCUSDT") {
    if let Some(candles) = readers.candles_5m {
        let mut last = Vec::new();
        candles.copy_last(200, &mut last);
        println!("candles={}", last.len());
    }
}
```

If the subscription scope is limited, candles are retained only for that scope.
If trades storage is disabled, chunked candles are not kept in market history.

## Candle Row

```rust
pub struct Candle5mRow {
    pub time:    f64,
    pub open_p:  f32,
    pub close_p: f32,
    pub max_p:   f32,
    pub min_p:   f32,
    pub vol:     f32,
}
```

`time` is a Delphi-compatible day value, not Unix time. Prefer the helper
methods in application code:

```rust
let high = candle.high();
let unix_ms = candle.time_delphi().unix_millis();
```

## Explicit Refresh

Use `MoonClient::refresh_candles(timeout)` when the UI wants to force a fresh
full 5m snapshot. The helper returns the number of parsed market entries and
applies candles to retained Active Lib history when storage is active:

```rust
let markets_received = client.refresh_candles(std::time::Duration::from_secs(30))?;
println!("candles refreshed for {markets_received} markets");
```

Low-level diagnostic tools can still call the hidden chunked request helpers and
inspect the merged `MergedCandles` object. Chart UI should not use that raw
chunk/zlib state; it should read retained candles through
`market_history_readers`.

For one market and one history kind, `request_coin_card_candles` returns the
server response as `Vec<DeepPrice>` and does not replace retained 5m history.
`DeepPrice` has the same `open()`, `high()`, `low()`, `close()`, and
`time_delphi()` helpers.
