# Trades

The trades stream carries exchange trades, market-maker rows, liquidation rows,
and watcher fills. `MoonClient` owns the protocol recovery logic: it detects
packet gaps, sends resend requests, applies usable resend payloads, and keeps
the live stream moving when old gaps cannot be recovered.

## Subscribe

```rust
use moonproto::TradesStreamMode;

// Choose the tape shape the UI needs.
client.streams().subscribe_all_trades(TradesStreamMode::TradesOnly)?;
// Or, for MoonBot-style heat-map rows with HyperLiquid taker wallets:
client
    .streams()
    .subscribe_all_trades(TradesStreamMode::TradesAndMarketMakers)?;
client.streams().subscribe_trades_for(
    TradesStreamMode::TradesAndMarketMakers,
    ["BTCUSDT", "ETHUSDT"],
)?;
client.streams().unsubscribe_all_trades()?;
```

`subscribe_all_trades(mode)` is the full Active Lib mode. Once the market list
is known, the library creates retained storage for all known markets and keeps
trades, liquidations, market-maker rows, LastPrice rows, 5-minute candles,
mini-candles, and derived analytics for them.

`TradesOnly` is the exchange tape: time, price, quantity, and side. HyperLiquid
wallet/taker addresses for the MoonBot heat-map are not fields on
`TradeHistoryRow`. They arrive in market-maker sections when the stream is
subscribed with `TradesStreamMode::TradesAndMarketMakers` or when the
MM-orders subscription is enabled. Read them from the retained `mm_orders` ring
and its slot-aligned `mm_order_companion` ring.

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
history writes. It is not a retained-history barrier: the worker may publish the
new rows just after the event, so an immediate bounded read can legitimately
return zero rows. A UI normally resolves the selected `MarketHandle` once,
keeps the `MarketHistoryReaders` once they become available, and advances its
own `SeqRingCursor` on the event or next normal update. Re-searching by string or
rebuilding readers on every paint/event tick is unnecessary.

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
reader.copy_from_time_ms(from_ms, limit, &mut out);
reader.copy_time_range(from_time, to_time, limit, &mut out);
reader.copy_time_range_ms(from_ms, to_ms, limit, &mut out);
let cursor = reader.cursor_at_or_after_time(time);
reader.copy_from_cursor(cursor, limit, &mut out);
reader.with_from_cursor(cursor, limit, |view| { /* zero-copy slices */ });
reader.copy_new_since(&mut cursor, limit, &mut out);
let drain = reader.drain_new_bounded(&mut cursor, limit, &mut out);
let (range, meta) = reader.price_range_from_cursor(cursor, limit);
let (range, meta) = reader.price_range_time_ms(from_ms, to_ms, limit);
let (volume, meta) = reader.qty_sum_time_ms(from_ms, to_ms, limit);
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

Time-based copy methods always return `SeqRingReadMeta`. An empty time window or
an empty retained ring is reported as `meta.copied = 0`, not as an error. If the
requested time is older than retained memory, `meta.clipped = true` and the rows
start from the oldest retained point.

For high-throughput consumers that drain in bounded batches, prefer
`drain_new_bounded`. It returns a compact public status:

```rust
pub struct SeqRingDrainMeta {
    pub copied: usize,
    pub clipped: bool,
    pub caught_up: bool,
    pub concurrent_miss: bool,
}
```

`caught_up = false` means the retained stream still had more rows than the
requested `limit`; call the drain again with the same cursor if you want to
catch up immediately. `clipped = true` means the cursor was older than the
retained capacity and the read restarted from the oldest row still available.
The dense locked backend reports `concurrent_miss = false`; the flag is reserved
for future backends that cannot keep the read range stable without retry.

For common analytics, prefer MoonProto's built-in aggregate helpers:

```rust
pub struct PriceRange {
    pub min: f32,
    pub max: f32,
    pub count: usize,
}

pub struct QtySum {
    pub sum: f64,
    pub count: usize,
}

reader.price_range_from_cursor(cursor, limit);
reader.price_range_time(from_time, to_time, limit);
reader.price_range_time_ms(from_ms, to_ms, limit);
reader.qty_sum_from_cursor(cursor, limit);
reader.qty_sum_time(from_time, to_time, limit);
reader.qty_sum_time_ms(from_ms, to_ms, limit);
```

Price ranges are available for trades, LastPrice, MarkPrice, 5m candles, and
mini-candles. Quantity/volume sums are available for trades, MM-order quantity,
5m candle volume, and mini-candle buy+sell volume. These helpers run short
tight loops inside the library and return ready aggregates, so callers do not
need a custom callback under the retained ring lock for normal min/max/sum
queries.

`scan_from_cursor` remains available for custom retained range queries that
should not build a second long-lived history. It visits rows under the ring read
lock in retained sequence order and returns caller-defined aggregate state plus
the same read metadata. The closure must be short and non-blocking: do simple
CPU work over the row, not UI rendering, logging, I/O, sleeps, or calls back
into client code. Use copy methods when the caller needs owned rows or wants to
do heavier work after releasing the ring read lock.

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

`mm_order_companion` is aligned by slot with `mm_orders` and carries the HyperDex
taker address plus the MoonBot-compatible display color. Use `taker_hex()` for
taker logs/tooltips and `color_argb()` for chart coloring.

For the heat-map / wallet map UI, drain both rings with the same cursor window:

```rust
let Some(readers) = snapshot.market_history_readers_for(&market) else { return; };
let (Some(mm_orders), Some(mm_companion)) =
    (readers.mm_orders.clone(), readers.mm_order_companion.clone())
else {
    return;
};

let mut orders = Vec::new();
let mut takers = Vec::new();
mm_orders.copy_from_cursor(cursor, limit, &mut orders);
mm_companion.copy_from_cursor(cursor, limit, &mut takers);

for (order, taker) in orders.iter().zip(takers.iter()) {
    draw_heatmap_point(order.time(), order.volume, taker.color_argb());
    show_taker_tooltip(taker.taker_hex());
}
```

## Diagnostics Fixture

When built with `feature = "diagnostics"`, `MoonClient` exposes a hidden
retained-history fixture hook for terminal stress tests:

```rust
client.diag_fill_market_history_to_capacity(
    "BTCUSDT",
    now_ms,
    moonproto::client::DIAG_MARKET_HISTORY_FILL_SPAN_MS,
)?;
```

The hook is not compiled into regular builds. It asks the retained-history
worker to fill every configured history ring for the market to its effective
capacity with chronological synthetic rows. Existing live rows remain at the
newest end; synthetic rows are inserted before them. When the library has
already seen a LastPrice, MarkPrice, trade, or candle for the market, generated
prices stay near that live price scale, so chart tests use the same Y-axis range
as the real market. After the call returns, normal `copy_*`,
`drain_new_bounded`, and aggregate reads see the fixture as ordinary retained
history, so a terminal can test full-capacity GPU upload and tail eviction
without a second fake data path.

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

Rolling trade volumes use fixed 5-second buckets. LastPrice ranges use
5-second buckets for 1m/5m and 1-minute buckets for 15m/30m/1h. Both are
updated only by newly accepted rows, so retained chart depth does not increase
their CPU cost. Closed-candle aggregates are rebuilt only after a candles
snapshot, a 5-minute seal, or a 5-minute expiry boundary. The current candle is
then overlaid without rescanning closed history.

Derived candle calculation uses at most the newest 500 sealed 5-minute candles,
even when the public chart ring retains more. The `seventy_two_hours` fields
therefore describe the available long tail; at the full 500-candle calculation
limit that tail is about 41 hours 40 minutes.

By default, short delta labels do not use raw trade extrema. This matches the
normal core setting: `trade_volumes` are always maintained, while
`trade_deltas` stay zero and `derived.deltas` is built from candle/LastPrice
derived paths. If a terminal intentionally wants the legacy
`DeltasByTrades` behavior, opt in explicitly:

```rust
client.streams().set_deltas_by_trades(true)?;
```

Use this as a chart-analytics policy switch, not as a substitute for signed
market signal deltas. `BTC1hDelta`, `Exchange1hDelta`, and per-market signed
signal deltas remain in `snapshot.markets()`.

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
    pub fn from_system_memory_with_budget_percent(
        market_count: usize,
        budget_percent: u16,
    ) -> Self;
    pub fn from_total_memory_bytes(total_memory_bytes: usize, market_count: usize) -> Self;
    pub fn from_total_memory_bytes_with_budget_percent(
        total_memory_bytes: usize,
        market_count: usize,
        budget_percent: u16,
    ) -> Self;
    pub fn history_budget_bytes(total_memory_bytes: usize) -> usize;
    pub fn history_budget_bytes_with_budget_percent(
        total_memory_bytes: usize,
        budget_percent: u16,
    ) -> usize;
    pub fn estimated_bytes_per_market(&self) -> usize;
}

let cfg = ClientConfig::new(host, port, master_key, mac_key)
    .with_market_history(MarketHistorySizing::Auto);

let cfg = ClientConfig::new(host, port, master_key, mac_key)
    .with_market_history(MarketHistorySizing::auto_with_budget_percent(300));

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
core: compact rows, predictable scans, and no per-item allocation.
`MarketHistorySizing::Auto` is the default: `MoonClient` waits until the market
list and the requested trade-storage scope are known, then sizes per-market
rings from system memory. `MarketHistorySizing::auto_with_budget_percent(value)`
keeps the same memory-aware split but scales the retained-history budget; values
are clamped to `100..=800`, with `100` equal to the default. Use
`MarketHistorySizing::fixed(config)` when the application wants exact capacities
or wants to disable selected retained categories with `0`.
`MarketHistorySizing` is non-exhaustive: application code that matches it should
include a wildcard branch so new sizing policies do not become a source-level
break.

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
