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

`Candle5mRow` stores Delphi-compatible raw fields internally, but application
code should use the OHLCV/time helpers instead of the raw Delphi field names:

```rust
let open = candle.open();
let high = candle.high();
let low = candle.low();
let close = candle.close();
let volume = candle.volume();
let unix_ms = candle.time_delphi().unix_millis();
```

The raw time value is a Delphi day value, not Unix time.

## Explicit Refresh

`MoonClient::refresh_candles(timeout)` is currently an explicit one-shot helper:
it waits for a full 5m snapshot, returns the number of parsed market entries,
and applies candles to retained Active Lib history when storage is active:

```rust
let markets_received = client.refresh_candles(std::time::Duration::from_secs(30))?;
println!("candles refreshed for {markets_received} markets");
```

Low-level diagnostic tools can still call the hidden chunked request helpers and
inspect the merged `MergedCandles` object. Chart UI should not use that raw
chunk/zlib state; it should read retained candles through
`market_history_readers`.

## CoinCard History

`request_coin_card_candles(market, kind)` is a demand-driven, non-blocking UI
request. It mirrors Delphi's CoinCard path: UI marks the market as needing deep
history, a background owner calls blocking `Engine.getDeepHistory`, then
`TMarket.CoinCardCandles` is updated.

These rows are separate from retained 5m history. Typical UI usage:

```rust
use moonproto::commands::candles::DeepHistoryKind;
use moonproto::Event;

let ticket = client.request_coin_card_candles("BTCUSDT", DeepHistoryKind::Hour4)?;

for event in client.drain_events() {
    if let Event::CoinCardCandles(ev) = event {
        println!("coin-card candles event: {ev:?}");
    }
}

if let Some(snapshot) = client.snapshot() {
    if let Some(rows) = snapshot
        .coin_card_candles()
        .get("BTCUSDT", DeepHistoryKind::Hour4)
    {
        println!("rows={}", rows.len());
    }
}
```

`DeepPrice` has the same `open()`, `high()`, `low()`, `close()`, and
`volume()` / `time_delphi()` helpers.

`blocking_request_coin_card_candles(market, kind, timeout)` exists for scripts,
tests, and diagnostics that deliberately need a synchronous `Vec<DeepPrice>`.
Desktop UI code should use the non-blocking request plus event/snapshot state.
