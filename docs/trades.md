# Trades

The trades stream carries exchange trades, market-maker rows, liquidation rows,
and watcher fills. `MoonClient` owns the protocol recovery logic: it detects
packet gaps, sends resend requests, applies usable resend payloads, and keeps
the live stream moving when old gaps cannot be recovered.

## Subscribe

```rust
use moonproto::TradesStreamMode;

client.streams().subscribe_all_trades(TradesStreamMode::TradesOnly)?;
client.streams().subscribe_trades_for(
    TradesStreamMode::TradesOnly,
    ["BTCUSDT", "ETHUSDT"],
)?;
client.streams().unsubscribe_all_trades()?;
```

`subscribe_all_trades(mode)` is the full Active Lib mode. Once the market list
is known, the library creates retained storage for all known markets and keeps
trades, liquidations, market-maker rows, LastPrice rows, 5-minute candles,
mini-candles, and derived analytics for them.

`subscribe_trades_for(mode, markets)` sends the same server subscription, but
retains/calculates data only for the listed markets. Passing an empty market
list means all markets. This filtered storage mode is an accepted Rust API
deviation for UI clients that want lower memory usage.

Unlike MoonBot UI, the Rust library does not subscribe to all trades unless the
application asks for it. Without a trades subscription intent, incoming trade
stream packets are treated as unexpected and are dropped instead of becoming
public events.

Before Init, subscribe/unsubscribe calls update only the reconnect registry.
After Init, changed intent also queues the server request. Reconnect restores
the trade stream automatically and waits until a trade packet proves that it
belongs to the current server token.

## Events

```rust
use moonproto::Event;
use moonproto::state::{MarketHistoryReaders, SeqRingCursor, TradesEvent};

let mut cursor: Option<SeqRingCursor> = None;
let mut readers: Option<MarketHistoryReaders> = None;
let mut rows = Vec::new();
let Some(snapshot) = client.snapshot() else { return; };
let Some(market) = snapshot.markets().get("BTCUSDT") else { return; };

for event in client.drain_events() {
    if let Event::Trade(trade_event) = event {
        match trade_event {
            TradesEvent::Applied { .. } => {
                let Some(state) = client.snapshot() else { continue; };
                if readers.is_none() {
                    readers = state.market_history_readers_for(&market);
                }
                let Some(reader) = readers
                    .as_ref()
                    .and_then(|readers| readers.futures_trades.clone())
                else {
                    continue;
                };

                let cursor = cursor.get_or_insert_with(|| reader.cursor_from_now());
                rows.clear();
                let meta = reader.copy_new_since(cursor, 4096, &mut rows);
                if meta.clipped {
                    on_retained_history_gap();
                }
                on_new_trades(&rows);
            }
            _ => {}
        }
    }
}
```

`TradesEvent::Applied` is a signal, not a payload carrier. By the time it is
emitted, Active Lib has already updated live market tails and queued retained
history writes. A UI normally resolves the selected `MarketHandle` once, keeps
the `MarketHistoryReaders` once they become available, and advances its own
`SeqRingCursor` on each signal. Re-searching by string or rebuilding readers on
every paint/event tick is unnecessary.

Gap, duplicate, out-of-order, resend, and bucket-close notifications are hidden
diagnostic telemetry. Applications do not drive recovery from them;
`MoonClient` does that automatically.

Watcher fills are emitted as `Event::WatcherFills(WatcherFillsEvent)` because
they are domain events rather than retained trade-history rows. The event carries
a shared market name; use `event.market_name.as_ref()` when matching it with UI
state.

## Retained Readers

Read retained history from the latest snapshot:

```rust
let Some(snapshot) = client.snapshot() else { return; };
let Some(market) = snapshot.markets().get("BTCUSDT") else { return; };
let Some(readers) = snapshot.market_history_readers_for(&market) else { return; };

if let Some(reader) = readers.futures_trades {
    let mut last = Vec::new();
    reader.copy_last(500, &mut last);
    draw_recent_trades(&last);
}

if let Some(candles) = readers.candles_5m {
    let mut rows = Vec::new();
    candles.copy_last(300, &mut rows);
    draw_candles(&rows);
}
```

Available readers:

```rust
pub struct MarketHistoryReaders {
    pub futures_trades: Option<SeqRingReader<TradeHistoryRow>>,
    pub spot_trades: Option<SeqRingReader<TradeHistoryRow>>,
    pub liquidations: Option<SeqRingReader<TradeHistoryRow>>,
    pub mm_orders: Option<SeqRingReader<MMOrderHistoryRow>>,
    pub mm_order_companion: Option<SeqRingReader<MMOrderCompanionData>>,
    pub last_prices: Option<SeqRingReader<LastPricePoint>>,
    pub mark_prices: Option<SeqRingReader<MarkPricePoint>>,
    pub mini_candles: Option<SeqRingReader<MiniCandle>>,
    pub candles_5m: Option<SeqRingReader<Candle5mRow>>,
}
```

Each `SeqRingReader` supports:

```rust
reader.copy_last(limit, &mut out);
reader.copy_from_time(time, limit, &mut out);
reader.copy_time_range(from_time, to_time, limit, &mut out);
let cursor = reader.cursor_at_or_after_time(time);
reader.copy_from_cursor(cursor, limit, &mut out);
reader.with_from_cursor(cursor, limit, |view| { /* zero-copy slices */ });
reader.copy_new_since(&mut cursor, limit, &mut out);
```

`SeqRingCursor` is the application-side "index" into a retained history. A chart
can get one from `cursor_at_or_after_time(...)` and then read from it with
`copy_from_cursor(...)` or `with_from_cursor(...)`. For "only new rows", every
consumer owns its own cursor and advances it with `copy_new_since(...)`. Do not
share one cursor between independent UI panels, strategy code, and logs. If
`copy_new_since` returns `meta.clipped = true`, that consumer was slower than the
retained ring capacity; returned rows start from the oldest still retained row.
Raw sequence-number helpers are diagnostics/test-only; normal terminal code uses
cursor and time APIs.

Retained rows preserve receive/store order. UDP resend rows can arrive late, so
timestamp order is not guaranteed. Time-range reads scan/filter retained rows
instead of assuming monotonic timestamps.

## Row Types

```rust
pub struct TradeHistoryRow {
    pub time: MoonTime,
    pub price: f32,
    pub qty: f32,
}

impl TradeHistoryRow {
    pub fn time(self) -> MoonTime;
    pub fn unix_millis(self) -> i64;
    pub fn quantity(self) -> f32;
    pub fn is_buy(self) -> bool;
    pub fn same_direction(self, other: Self) -> bool;
    pub fn traded_value(self) -> f32;
}

pub struct MMOrderHistoryRow {
    pub time: MoonTime,
    pub volume: f64,
    pub q: f64,
}

impl MMOrderHistoryRow {
    pub fn time(self) -> MoonTime;
    pub fn unix_millis(self) -> i64;
}

pub struct MMOrderCompanionData {
    /* private fields */
}

impl MMOrderCompanionData {
    pub fn taker(&self) -> &[u8; 20];
    pub fn taker_hex(&self) -> String;
    pub fn color_argb(&self) -> u32;
}

pub struct MiniCandle {
    pub time: MoonTime,
    pub cnt: i32,
    pub min_price: f32,
    pub max_price: f32,
    pub buy_vol: f32,
    pub sell_vol: f32,
}

impl MiniCandle {
    pub fn time(self) -> MoonTime;
    pub fn unix_millis(self) -> i64;
    pub fn low(self) -> f32;
    pub fn high(self) -> f32;
    pub fn buy_volume(self) -> f32;
    pub fn sell_volume(self) -> f32;
}
```

Row `time` fields are `MoonTime`. Use `time().unix_millis()` or
`time().system_time()` before displaying wall-clock time.

Futures trade direction uses the raw `qty` sign bit: sign bit clear means buy,
sign bit set means sell. Use `quantity()` for absolute quantity and `is_buy()`
for side.

Old detailed futures rows evicted from the retained futures trade ring are
compacted into `MiniCandle` rows. This keeps older chart context available
without retaining every old trade forever.

`mm_order_companion` mirrors MoonBot's `TMMOrderData` side buffer. It is aligned
by slot with `mm_orders` and carries the HyperDex taker address plus the
MoonBot-compatible display color. Use `taker_hex()` for taker logs/tooltips and
`color_argb()` for chart coloring.

## Candles And Derived Analytics

When trades storage is enabled, Active Lib also maintains:

- current 5-minute candle and retained 5-minute candles;
- retained LastPrice line from market updates;
- retained MarkPrice line from market updates;
- rolling 1/3/5-minute trade volumes;
- candle volumes for 5m, 15m, 30m, 1h, 2h, 3h, 24h, and 72h;
- trade, candle, LastPrice, and combined delta snapshots.

Read derived state from the snapshot:

```rust
let Some(snapshot) = client.snapshot() else { return; };
let Some(market) = snapshot.markets().get("BTCUSDT") else { return; };

if let Some(derived) = snapshot.market_history_derived_snapshot_now_for(&market) {
    draw_volume(derived.trade_volumes.five_minutes);
    draw_delta(derived.deltas.one_hour);
}

let signed = market.delta_state();
let global = snapshot.markets().global_deltas();
draw_btc_market_signals(signed.coin_1h_delta, global.btc_1h_delta, global.exchange_1h_delta);
```

For normal chart panels, read this snapshot once per UI tick for the selected
market and render volume/delta labels from it. Re-scanning retained trade or
candle rings separately for every 1m/3m/5m/1h label is unnecessary. Manual
retained-history scans are for custom analytics that intentionally differ from
the Active Lib read model.

`MarketDerivedSnapshot::deltas` is range/max-move chart analytics. MoonBot's
signed `Coin1hDelta`, `BTC1hDelta`, and `Exchange1hDelta` live in
`MarketHandle::delta_state()` and `MarketsState::global_deltas()`. Use the
signed state for BTC/exchange blink, panic, and restart guards; do not substitute
`derived.deltas.one_hour`.

```rust
pub struct RollingTradeVolumeSnapshot {
    pub one_minute: TradeVolumeTotals,
    pub three_minutes: TradeVolumeTotals,
    pub five_minutes: TradeVolumeTotals,
}

pub struct MarketDerivedSnapshot {
    pub trade_volumes: RollingTradeVolumeSnapshot,
    pub candle_volumes: CandleVolumeSnapshot,
    pub trade_deltas: DerivedDeltaSnapshot,
    pub candle_deltas: DerivedDeltaSnapshot,
    pub last_price_deltas: DerivedDeltaSnapshot,
    pub deltas: DerivedDeltaSnapshot,
    pub current_candle: Option<Candle5mRow>,
}
```

Rolling trade volumes use 5-second buckets and update only from newly accepted
trades. Candle-derived windows are recalculated from retained 5-minute candles
plus the current candle.

## Storage Configuration

```rust
use moonproto::{ClientConfig, state::{MarketHistoryConfig, MarketHistorySizing}};

pub struct MarketHistoryConfig {
    pub futures_trades_capacity: usize,
    pub spot_trades_capacity: usize,
    pub liquidation_capacity: usize,
    pub mm_orders_capacity: usize,
    pub last_price_capacity: usize,
    pub mini_candles_capacity: usize,
    pub candles_5m_capacity: usize,
}

impl MarketHistoryConfig {
    pub fn from_system_memory(market_count: usize) -> Self;
    pub fn from_total_memory_bytes(total_memory_bytes: usize, market_count: usize) -> Self;
    pub fn history_budget_bytes(total_memory_bytes: usize) -> usize;
    pub fn estimated_bytes_per_market(&self) -> usize;
}

let cfg = ClientConfig::new(host, port, master_key, mac_key)
    .with_market_history(MarketHistorySizing::Auto);

let cfg = ClientConfig::new(host, port, master_key, mac_key)
    .with_market_history(MarketHistorySizing::fixed(MarketHistoryConfig {
        futures_trades_capacity: 100_000,
        spot_trades_capacity: 20_000,
        liquidation_capacity: 10_000,
        mm_orders_capacity: 10_000,
        last_price_capacity: 20_000,
        mini_candles_capacity: 20_000,
        candles_5m_capacity: 20_000,
    }));
```

Each capacity is a row count per retained market, not a byte count. For example,
`futures_trades_capacity: 100_000` keeps up to 100,000 futures trade rows for
each market whose trades are retained. Capacities set to `0` disable that
retained public history category.
`mm_orders_capacity` governs both the MM-order ring and its taker/color
companion ring; they push and evict in lockstep so an order and its companion
can never desync. This keeps the same dense hot-path shape as the production
Delphi core: compact rows, predictable scans, and no per-item allocation.
`MarketHistorySizing::Auto` is the default: `MoonClient` waits until the market
list and the requested trade-storage scope are known, then sizes per-market
rings from system memory. Use `MarketHistorySizing::fixed(config)` when the
application wants exact capacities or wants to disable selected retained
categories with `0`.

`MoonClient` creates and owns the default history worker automatically when the
trades subscription scope becomes active. Regular applications use
`MoonClient` snapshots and readers; they do not create workers manually.

The retained-history worker queue is intentionally unbounded. It must not
backpressure the protocol reader or silently drop trade/order/LastPrice rows
because of a Rust-only internal cap. Under normal load the worker owns the
dense rings and applies batches quickly; if an application enables very large
retained scopes and the worker is kept overloaded for longer than the incoming
stream can be processed, memory can grow. Keep event callbacks light, use sane
history capacities/scopes, and use FireTest/diagnostics CPU summaries to catch
worker overload during integration.

## Recovery Policy

MoonClient's trades recovery state maintains up to 50 gap buckets. Missing
packet numbers are requested for up to three bucket retry cycles with a delay
based on current RTT. If a bucket is still incomplete after its retry budget, it
is closed and the live stream continues. This is intentional: the protocol
should not flood the channel forever for old trade packets.

`MoonClient` runs the recovery tick after successfully parsed live/resend trade
packets and throttles it to roughly 100 ms. Applications should not send resend
requests manually.

## Protocol Data

Raw packet parsers, resend-state helpers, and the mutable trades recovery state
are internal protocol-test machinery. Normal applications subscribe through
`MoonClient`, react to `TradesEvent::Applied`, and read retained rows from
`MarketHistoryReaders`.
