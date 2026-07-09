# Candles

For normal Active Lib usage, candles are retained market history, not a raw
chunked response object.

When an application subscribes to trades through `InitConfig.subscribe_trades`,
`streams().subscribe_all_trades`, or `streams().subscribe_trades_for`, the runtime requests the
initial full 5m candles snapshot for the active storage scope. If the chunked
request or history-worker barrier times out, Active Lib emits
`Event::CandlesSnapshot(CandlesSnapshotEvent::Failed { .. })` and retries while
that trades scope remains active; after a successful snapshot it does not
request the same scope again. After the chunked response is merged and parsed,
Active Lib applies it to retained
per-market history and emits `Event::CandlesSnapshot` only after the history
worker acknowledges the barrier. Incoming trades then maintain the current 5m
candle and derived candle/volume state.

```rust
use moonproto::TradesStreamMode;

client.streams().subscribe_trades_for(TradesStreamMode::TradesOnly, ["BTCUSDT"])?;

// Later, after events/snapshot refresh:
let Some(state) = client.snapshot() else { return; };
let Some(market) = state.markets().get("BTCUSDT") else { return; };

if let Some(readers) = state.market_history_readers_for(&market) {
    if let Some(candles) = readers.candles_5m {
        let mut last = Vec::new();
        candles.copy_last(200, &mut last);
        println!("candles={}", last.len());
    }
}
```

The in-progress candle is exposed through the derived snapshot. It is separate
from the sealed 5m ring and is the live chart bar that incoming trades keep
updating between sealed 5m rows:

```rust
if let Some(derived) = state.market_history_derived_snapshot_now_for(&market) {
    if let Some(live) = derived.current_candle {
        draw_live_candle(live.open(), live.high(), live.low(), live.close(), live.volume());
    }
}
```

Snapshot history reads are non-blocking. They return `None` until the retained
history worker has published the market's read handle; the UI should try again
on the next event/tick instead of waiting.

If the subscription scope is limited, candles are retained only for that scope.
If trades storage is disabled, chunked candles are not kept in market history.

## Candle Row

`Candle5mRow` stores wire-compatible raw fields internally, but application
code should use the OHLCV/time helpers instead of raw protocol field names:

```rust
let open = candle.open();
let high = candle.high();
let low = candle.low();
let close = candle.close();
let volume = candle.volume();
let unix_ms = candle.time().unix_millis();
```

The raw wire time is converted at the packet boundary; the public helper returns
`MoonTime`.

## Full Snapshot Refresh

Normal UI code does not manually request "all candles for all markets". It
subscribes to trades, waits for `Event::CandlesSnapshot`, then reads retained 5m
candles from `market_history_readers`. The raw chunked/zlib response object is a
diagnostic/protocol detail.

`CandlesSnapshotEvent::Ready` carries a small summary:

```rust
pub struct CandlesSnapshotApplySummary {
    pub received_markets: usize,
    pub received_candles: usize,
    pub retained_markets: usize,
    pub retained_candles: usize,
}
```

`received_*` counts describe the decoded server snapshot. `retained_*` counts
describe the rows that entered the active trade-storage scope and are visible
through market-history readers. A filtered `subscribe_trades_for` scope can
therefore retain fewer markets/candles than the server returned.

## CoinCard History

`candles().request_coin_card_for(&market, kind)` is a demand-driven,
non-blocking UI request for the selected retained market. It mirrors the core
CoinCard flow: UI asks for deep history for the current market, the runtime
performs the blocking history request off the UI path, and the retained
CoinCard candles are updated only after the response is applied. The string-keyed
`request_coin_card(market, kind)` variant remains useful for scripts and
one-shot tools.

These rows are separate from retained 5m history. Typical UI usage:

```rust
use moonproto::DeepHistoryKind;
use moonproto::Event;

let Some(snapshot) = client.snapshot() else { return; };
let Some(market) = snapshot.markets().get("BTCUSDT") else { return; };
let ticket = client.candles().request_coin_card_for(&market, DeepHistoryKind::Hour4)?;

for event in client.drain_events() {
    if let Event::CoinCardCandles(ev) = event {
        println!("coin-card candles event: {ev:?}");
    }
}

if let Some(snapshot) = client.snapshot() {
    if let Some(rows) = snapshot.coin_card_candles_for(&market, DeepHistoryKind::Hour4) {
        println!("rows={}", rows.len());
    }
}
```

`DeepPrice` has the same `open()`, `high()`, `low()`, `close()`, `volume()`,
and `time()` helpers.

Desktop UI code should use the non-blocking request plus event/snapshot state.

## Live TF Candle Updates

After a chart loads its base TF history, subscribe to live updates for the
visible markets:

```rust
use moonproto::DeepHistoryKind;
use moonproto::Event;

client
    .streams()
    .subscribe_candles_for([&market], DeepHistoryKind::Hour4)?;

for event in client.drain_events() {
    if let Event::LiveCandle(ev) = event {
        if ev.applied_to_history {
            redraw_chart(ev.market_name, ev.kind, ev.history_revision);
        }
    }
}

if let Some(rows) = client
    .snapshot()
    .and_then(|s| s.tf_candles_for(&market, DeepHistoryKind::Hour4).map(|r| r.to_vec()))
{
    draw_tf_candles(&rows);
}
```

Live TF updates do not create a full history by themselves. They replace the
last loaded row or append the next row using the core candle-window rule. If the
base `request_coin_card_for(..., kind)` history has not been applied yet,
`Event::LiveCandle` still reports the pushed candle, but
`applied_to_history == false` and `tf_candles_for` remains empty.
