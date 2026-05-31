# Trades

The trades stream carries exchange trades, market-maker rows, liquidation rows,
and watcher fills. `MoonClient` owns the protocol recovery logic: it detects
packet gaps, sends resend requests, applies usable resend payloads, and keeps
the live stream moving when old gaps cannot be recovered.

## Subscribe

```rust
use moonproto::TradesStreamMode;

client.streams().subscribe_all_trades(TradesStreamMode::TradesOnly);
client.streams().subscribe_trades_for(
    TradesStreamMode::TradesOnly,
    ["BTCUSDT", "ETHUSDT"],
);
client.streams().unsubscribe_all_trades();
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
use moonproto::state::{SeqRingCursor, TradesEvent};

let mut cursor: Option<SeqRingCursor> = None;
let mut rows = Vec::new();

for event in client.drain_events() {
    if let Event::Trade(trade_event) = event {
        match trade_event {
            TradesEvent::Applied { packet_num, .. } => {
                let Some(state) = client.snapshot() else { continue; };
                let Some(market) = state.markets().get("BTCUSDT") else { continue; };
                let Some(readers) = state.market_history_readers_for(&market) else { continue; };
                let Some(reader) = readers.futures_trades else { continue; };

                let cursor = cursor.get_or_insert_with(|| reader.cursor_from_now());
                rows.clear();
                let meta = reader.copy_new_since(cursor, 4096, &mut rows);
                if meta.clipped {
                    on_retained_history_gap(meta.actual_start_seq);
                }
                on_new_trades(packet_num, &rows);
            }
            TradesEvent::GapDetected { start, end } => log_gap(*start, *end),
            TradesEvent::Duplicate => log_duplicate_packet(),
            TradesEvent::OutOfOrder { packet_num } => log_out_of_order(*packet_num),
            TradesEvent::GapFilled { packet_num, .. } => log_gap_filled(*packet_num),
            TradesEvent::ResendRequested { packet_nums } => log_resend(packet_nums),
            TradesEvent::BucketClosed { .. } => {}
        }
    }
}
```

`TradesEvent::Applied` is a signal, not a payload carrier. By the time it is
emitted, Active Lib has already updated live market tails and queued retained
history writes. Applications read rows from `MarketHistoryReaders` with their
own `SeqRingCursor`.

Gap, duplicate, out-of-order, resend, and bucket-close events are diagnostic
telemetry. Applications do not drive recovery from them; `MoonClient` does that
automatically.

Watcher fills are emitted as `Event::WatcherFills(WatcherFillsEvent)` because
they are domain events rather than retained trade-history rows.

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
reader.copy_from_time(time_days, limit, &mut out);
reader.copy_time_range(from_days, to_days, limit, &mut out);
reader.copy_new_since(&mut cursor, limit, &mut out);
```

For "only new rows", every consumer owns its own `SeqRingCursor`. Do not share
one cursor between independent UI panels, strategy code, and logs. If
`copy_new_since` returns `meta.clipped = true`, that consumer was slower than
the retained ring capacity; returned rows start from the oldest still retained
row.

Retained rows preserve receive/store order. UDP resend rows can arrive late, so
timestamp order is not guaranteed. Time-range reads scan/filter retained rows
instead of assuming monotonic timestamps.

## Row Types

```rust
pub struct TradeHistoryRow {
    pub time: f64,
    pub price: f32,
    pub qty: f32,
}

impl TradeHistoryRow {
    pub fn time_delphi(self) -> DelphiTime;
    pub fn unix_millis(self) -> Option<i64>;
    pub fn quantity(self) -> f32;
    pub fn is_buy(self) -> bool;
    pub fn same_direction(self, other: Self) -> bool;
    pub fn traded_value(self) -> f32;
}

pub struct MMOrderHistoryRow {
    pub time: f64,
    pub volume: f64,
    pub q: f64,
}

impl MMOrderHistoryRow {
    pub fn time_delphi(self) -> DelphiTime;
    pub fn unix_millis(self) -> Option<i64>;
}

pub struct MiniCandle {
    pub time: f64,
    pub cnt: i32,
    pub min_price: f32,
    pub max_price: f32,
    pub buy_vol: f32,
    pub sell_vol: f32,
}

impl MiniCandle {
    pub fn time_delphi(self) -> DelphiTime;
    pub fn unix_millis(self) -> Option<i64>;
    pub fn low(self) -> f32;
    pub fn high(self) -> f32;
    pub fn buy_volume(self) -> f32;
    pub fn sell_volume(self) -> f32;
}
```

Row `time` fields are Delphi `TDateTime` day values. Use `time_delphi()` or row
helpers such as `unix_millis()` before displaying them as wall-clock time.

Futures trade direction uses the raw `qty` sign bit: sign bit clear means buy,
sign bit set means sell. Use `quantity()` for absolute quantity and `is_buy()`
for side.

Old detailed futures rows evicted from the retained futures trade ring are
compacted into `MiniCandle` rows. This keeps older chart context available
without retaining every old trade forever.

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
```

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
```

Capacities set to `0` disable that retained public history category.
`mm_orders_capacity` governs both the MM-order ring and its taker/color
companion ring; they push and evict in lockstep so an order and its companion
can never desync (matching Delphi's single-size `TStreamableRingBuffer`).
`from_system_memory(market_count)` is the recommended helper when the
application wants a memory-aware default.

`MoonClient` creates and owns the default history worker automatically when the
trades subscription scope becomes active. Custom runtimes can attach their own
`MarketHistoryWorker`, but regular applications should use `MoonClient`
snapshots and readers.

## Recovery Policy

`TradesState` maintains up to 50 gap buckets. Missing packet numbers are
requested for up to three bucket retry cycles with a delay based on current RTT.
If a bucket is still incomplete after its retry budget, it is closed and the
live stream continues. This is intentional: the protocol should not flood the
channel forever for old trade packets.

`MoonClient` runs the recovery tick after successfully parsed live/resend trade
packets and throttles it to roughly 100 ms. Applications should not send resend
requests manually.

## Protocol Data

Raw packet parsers and resend-state helpers are internal protocol-test
machinery. Normal applications should subscribe through `MoonClient`, react to
`TradesEvent::Applied`, and read retained rows from `MarketHistoryReaders`.
