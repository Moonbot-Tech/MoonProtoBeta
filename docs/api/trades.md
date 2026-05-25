# Trades Stream

The trades stream carries exchange trades, market-maker orders, liquidation
orders, and watcher fills. Packets are numbered with wrapping `u16` packet
numbers. The library detects gaps and requests resend batches automatically when
you run through `Client::run_with_dispatcher`.

## Subscribe

```rust
client.subscribe_all_trades(false); // false = trades only, true = include MM orders
client.subscribe_trades_for(false, ["BTCUSDT", "ETHUSDT"]); // retain Active Lib data only for these markets
client.unsubscribe_all_trades();
```

The subscription is registry-aware. Before Init, subscribe and unsubscribe calls
update only the registry and send no Engine API wire packet. The one-time Init
flushes the current registry once using the exact stored `want_mm` value; the
post-init `TMMOrdersSubscribeCommand` does not rewrite this all-trades value.
After Init, reconnect replays it automatically using the Delphi reconnect
sequence: if the current `ServerToken` has not yet been observed on
`MPC_TradesStream`, the maintenance tick sends `emk_UnsubscribeAllTrades`, waits
100 ms, then sends `emk_SubscribeAllTrades`. The sequence is retried no more
often than once per 5000 ms until a trades packet for the current `ServerToken`
reaches the parser.
Queuing `emk_SubscribeAllTrades` arms that gate, and a successful response
refreshes it again. This preserves the Delphi `SendAndWait` effect: the library
does not immediately unsubscribe from a stream it has just subscribed to while
waiting for the first trades packet.
Unsubscribe removes the registry intent and sends `emk_UnsubscribeAllTrades`
only after `domain_ready`.

Unlike Delphi MoonBot, the Rust library does not subscribe to all-trades unless
the application asks for it. This is an accepted author decision for the public
library API. If no all-trades intent is present in the registry, incoming
`MPC_TradesStream` and `MPC_TradesResendResponse` packets are considered
unexpected and are dropped instead of being emitted as public events.

`subscribe_all_trades(want_mm)` is the Delphi-shaped default: once the market
list is known, Active Lib creates retained storage for every known market and
maintains trades, MM/liquidation rows, LastPrice, 5m candles, and derived
analytics for them. `subscribe_trades_for(want_mm, markets)` is the accepted
Rust API deviation for UI clients that want less local memory. It sends the
same wire `emk_SubscribeAllTrades` command, but Active Lib emits/stores/calculates
only the listed markets. Passing an empty market list means all markets. Raw
diagnostic callbacks remain raw protocol callbacks and are not filtered by this
retained-storage scope.

The public call always updates the reconnect registry immediately. Once Init is
open, changed subscription intent also appends the wire Engine API request to
the Delphi-style send queues. On the wire, `emk_SubscribeAllTrades` /
`emk_UnsubscribeAllTrades` are Engine API requests and their success or failure
is reported as a later `Event::EngineResponse`. Trades packets then arrive
asynchronously on the `MPC_TradesStream` channel.

## Events

```rust
use moonproto::Event;
use moonproto::state::TradesEvent;
use moonproto::commands::trades_stream::TradeSection;

client.run_with_dispatcher_state(duration, &mut dispatcher, Box::new(|event, state| {
    if let Event::Trade(trade_event) = event {
        match trade_event {
            TradesEvent::Apply(packet) => {
                for section in &packet.sections {
                    match section {
                        TradeSection::Trades(trades) => {
                            for trade in trades {
                                let market_name = state.markets().market_name_by_index(trade.market_index);
                                on_trade(market_name, trade.is_spot, trade.price, trade.qty);
                            }
                        }
                        TradeSection::MMOrders(orders) => on_mm_orders(orders),
                        TradeSection::LiqOrders(orders) => on_liquidations(orders),
                        TradeSection::WatcherFills { market_index, user, .. } => {
                            let fills = section.watcher_fill_records()
                                .expect("library emits complete watcher-fill records");
                            on_watcher_fills(*market_index, user, &fills);
                        }
                    }
                }
            }
            TradesEvent::GapDetected { start, end } => log_gap(*start, *end),
            TradesEvent::Duplicate => log_duplicate_packet(),
            TradesEvent::OutOfOrder { packet_num } => log_out_of_order(*packet_num),
            TradesEvent::GapFilled { packet_num, .. } => log_gap_filled(*packet_num),
            TradesEvent::ResendRequested { packet_nums } => log_resend(packet_nums),
            TradesEvent::BucketClosed { .. } => {}
        }
    }
}));
```

`qty` sign encodes direction: positive is buy-side, negative is sell-side.
Use `MarketsState::market_name_by_index` / `market_by_index` to resolve the
stream `market_index` into the canonical market name. The dispatcher suppresses
stream events while indexes are stale after a server restart; after the one-time
Init, reconnect restore sends `GetMarketsIndexes` automatically when trades were
requested.

Watcher fills are not opaque for applications. The section keeps the original
raw `data` for compatibility with low-level tools, but regular consumers should
decode it through `TradeSection::watcher_fill_records()` or
`parse_watcher_fills(data)` and handle typed `WatcherFill` records together with
the section-level `market_index` and `user` address.

`GapDetected`, `ResendRequested`, `GapFilled`, `BucketClosed`, `Duplicate`, and
resend-side `OutOfOrder` are diagnostic events. They are useful for
logging/telemetry, but applications must not drive recovery from them:
`Client::run_with_dispatcher` sends resend requests and maintains buckets
automatically. `ResendRequested` means the Delphi-style tail check after a
valid trades packet queued `emk_TradesResend` for the listed packet numbers.
The dispatcher still emits `Apply(packet)` for duplicate/resend payloads when
Delphi would parse the payload.

Before an `Apply(packet)` event is emitted, `Client::run_with_dispatcher` also
updates `MarketsState::trade_state(market)` for every known futures/spot trade
row. This mirrors the bounded Delphi `ProcessTradesStream` tail: futures trades
update `LastGotAllTrades` and the `SetLastTradePrices` fields, while spot trades
only update `LastGotSpotTrades`.

## Public Types

```rust
pub struct TradesPacket {
    pub base_time: f64,
    pub packet_num: u16,
    pub sections: Vec<TradeSection>,
}

pub enum TradeSection {
    Trades(Vec<Trade>),
    MMOrders(Vec<MMOrder>),
    LiqOrders(Vec<LiqOrder>),
    WatcherFills { market_index: u16, user: [u8; 20], data: Vec<u8> },
}

pub struct Trade {
    pub market_index: u16,
    pub is_spot: bool,
    pub time_delta_ms: i16,
    pub price: f32,
    pub qty: f32,
}

pub struct WatcherFill {
    pub time_delta_ms: i16,
    pub price: f32,
    pub qty: f32,
    pub z_btc: f32,
    pub position: f32,
    pub order_type: u8,
    pub flags: u8,
}
```

`WatcherFill::is_short()`, `is_open()`, and `is_taker()` decode the Delphi flags
byte. `time_delta_ms` is relative to `TradesPacket::base_time`.

## Retained History Building Blocks

Retained history is Active Lib storage for trades, spot trades, liquidations,
MM orders, LastPrice, 5m candles, mini-candles, and derived analytics. The
protocol/event path
does not write these rings directly. It queues decoded stream batches into a
`MarketHistoryWorker`; that worker owns the per-market stores and is the single
writer. This keeps protocol receive work bounded while preserving Delphi's
storage meaning.

Storage is controlled by the all-trades subscription intent. Without
`subscribe_all_trades` / `subscribe_trades_for`, no retained trade/candle/derived
state is created. With `subscribe_all_trades`, the worker creates stores for
all markets from `GetMarketsList`. With `subscribe_trades_for`, it creates
stores only for the selected markets. Capacities set to `0` disable that
retained public history category.

```rust
pub struct TradeHistoryRow {
    pub time: f64,  // Delphi TDateTime
    pub price: f32,
    pub qty: f32,  // signed: sign bit clear = buy, sign bit set = sell
}

impl TradeHistoryRow {
    pub fn quantity(self) -> f32;        // Abs(qty)
    pub fn is_buy(self) -> bool;         // Delphi sign-bit check
    pub fn same_direction(self, other: Self) -> bool;
    pub fn traded_value(self) -> f32;    // price * Abs(qty)
}

pub struct MMOrderHistoryRow {
    pub time: f64, // Delphi TDateTime
    pub vol: f64,
    pub q: f64,
}

pub struct MMOrderCompanionData {
    pub taker: [u8; 20],
    pub color: u32,
}

pub fn hl_address_color_like_delphi(taker: [u8; 20]) -> u32;

pub struct MiniCandle {
    pub time: f64, // Delphi TDateTime
    pub cnt: i32,
    pub min_price: f32,
    pub max_price: f32,
    pub buy_vol: f32,
    pub sell_vol: f32,
}

pub struct Candle5mRow {
    pub open_p: f32,
    pub close_p: f32,
    pub max_p: f32,
    pub min_p: f32,
    pub vol: f32,
    pub time: f64,
}

pub struct MarketDerivedSnapshot {
    pub trade_volumes: RollingTradeVolumeSnapshot,
    pub candle_volumes: CandleVolumeSnapshot,
    pub trade_deltas: DerivedDeltaSnapshot,
    pub candle_deltas: DerivedDeltaSnapshot,
    pub last_price_deltas: DerivedDeltaSnapshot,
    pub deltas: DerivedDeltaSnapshot,
}

pub struct CandleVolumeSnapshot {
    pub five_minutes: f64,
    pub fifteen_minutes: f64,
    pub thirty_minutes: f64,
    pub one_hour: f64,
    pub two_hours: f64,
    pub three_hours: f64,
    pub twenty_four_hours: f64,
    pub seventy_two_hours: f64,
}

pub struct DerivedDeltaSnapshot {
    pub one_minute: f64,
    pub five_minutes: f64,
    pub fifteen_minutes: f64,
    pub thirty_minutes: f64,
    pub one_hour: f64,
    pub two_hours: f64,
    pub three_hours: f64,
    pub twenty_four_hours: f64,
    pub seventy_two_hours: f64,
}
```

`TradeHistoryRow` is the retained form for detailed trades and liquidations. It
matches Delphi `TTrade`: `Time: TDateTime`, `Price: Single`, signed
`Qty: Single`. Direction uses the raw `Qty` sign bit, so `-0.0` is sell-side,
matching Delphi `PCardinal(@Qty)^ and $80000000`.

`MMOrderHistoryRow` matches Delphi base `TMMOrder`: `Time`, `vol`, and `Q` are
stored as doubles. HyperDex taker address/color companion data is a separate
storage layer; it is not folded into the base row. `MMOrderCompanionData`
matches Delphi `TMMOrderData`: `Taker: THLAddress` and `Color: TColor`.
`hl_address_color_like_delphi` mirrors Delphi `HLAddressColor`: XOR address
bytes into R/G/B accumulators by index modulo 3, scale each channel with
`(x * 5 >> 3) + 80`, and set alpha to `0xFF`.

```rust
pub fn compact_trades_to_mini_candles_like_delphi(
    rows: &[TradeHistoryRow],
    last_mini_time: f64,
    now_time: f64,
    out: &mut Vec<MiniCandle>,
);
```

The compaction helper mirrors Delphi `TMarket.ResizeOrdersHistory`: it groups
detailed trades by a 5-second anchor window, calculates buy/sell volume as
`Price * Abs(Qty)`, maintains min/max price, and applies the same `T1`/`Now`
append gates used when old detailed trades are turned into `TMiniCandle`.

```rust
pub struct TradeVolumeTotals {
    pub buy_value: f64,
    pub sell_value: f64,
    pub buy_qty: f64,
    pub sell_qty: f64,
    pub trade_count: u32,
    pub min_price: f32,
    pub max_price: f32,
}

pub struct RollingTradeVolumeSnapshot {
    pub one_minute: TradeVolumeTotals,
    pub three_minutes: TradeVolumeTotals,
    pub five_minutes: TradeVolumeTotals,
}

pub struct RollingTradeVolumes;

impl RollingTradeVolumes {
    pub fn add_trade(&mut self, row: TradeHistoryRow);
    pub fn snapshot(&self, now_time: f64) -> RollingTradeVolumeSnapshot;
    pub fn window(&self, now_time: f64, window_seconds: i64) -> TradeVolumeTotals;
}
```

`RollingTradeVolumes` uses 5-second buckets and updates from newly received
trades. It is the active-library derived-state path for 1/3/5 minute buy/sell
volumes and short trade-price deltas; the intended precision loss is bounded by
one bucket width. Call
`MarketHistoryWorker::rolling_volumes(market, now_time)` or the same method on
`MarketHistoryHandle` to read the current derived totals for an active retained
market. Unknown or out-of-scope markets return `None` and are not allocated by a
read.

`MarketDerivedSnapshot::trade_deltas` is the futures-trade source. It is filled
from the same 5-second buckets as volumes, currently for 1m and 5m windows.
`MarketDerivedSnapshot::candle_deltas` and `candle_volumes` are the candle
source and are calculated in one pass over retained 5m candles plus the current
candle for 5m, 15m, 30m, 1h, 2h, 3h, 24h, and 72h windows. Candle volume is the
total candle quote volume and has no buy/sell split; use `trade_volumes` for
1m/3m/5m buy/sell totals.
`MarketDerivedSnapshot::last_price_deltas` is the retained LastPrice line
source. It follows Delphi's `UpdateMarketsList -> TMarket.AddFrom ->
HistoryPrice` path and feeds the 15m/30m/1h-style windows exposed to clients.
If the trades-storage scope is enabled after Init already applied
`UpdateMarketsList`, the active dispatcher backfills the retained LastPrice line
from the current market state so the first derived snapshot is not forced to
wait for the next periodic market update.

The long candle delta fields intentionally follow Delphi `RecalcPumpQ` naming,
not exact wall-clock names: `two_hours` is `Last2hDelta` (`h <= 2`, roughly
three hourly buckets), `three_hours` is `Last3hDelta` (`h <= 3`, roughly four
hourly buckets), and `twenty_four_hours` is `Last24hDelta` (`h <= 24`, roughly
25 hourly buckets). Candle windows use Delphi's strict old boundary, so a row
exactly at the window start is outside. Candle volume fields keep exact window
semantics.
`MarketDerivedSnapshot::deltas` is the combined view:
per field it keeps the larger value from trade, LastPrice-line, and candle
sources, which matches the Delphi habit of not lowering a hotter short-window
delta with a colder source. Matching Delphi `RecalcPumpQ`, combined 2h, 3h, and
24h deltas are also floored by the combined 1h delta; 72h remains its own
long-window source.

```rust
pub const DELPHI_SAME_TRADES_TIME_DAYS: f64; // 0.2 / 86400.0
pub const DELPHI_MSECS_PER_DAY: f64;         // 86400000.0

pub struct TradesPacketTimeShift;

impl TradesPacketTimeShift {
    pub fn new() -> Self;
    pub fn shift_days(&self) -> Option<f64>;
    pub fn apply_like_delphi(
        &mut self,
        base_time: f64,
        time_delta_ms: i16,
        now_time: f64,
    ) -> f64;
}

pub enum TradeJoinPush {
    Inserted,
    AggregatedPrev1,
    AggregatedPrev2,
    Full,
}

pub struct TradeJoinBuffer;

impl TradeJoinBuffer {
    pub fn new(capacity: usize) -> Self;
    pub fn capacity(&self) -> usize;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn push_like_delphi(
        &mut self,
        row: TradeHistoryRow,
        chart_price_step: f64,
        same_trades_time_days: f64,
    ) -> TradeJoinPush;
    pub fn drain_into(&mut self, out: &mut Vec<TradeHistoryRow>);
}
```

`TradesPacketTimeShift` mirrors Delphi `ProcessTradesStream`: the first
known/stored row in a packet fixes
`round((NowTimeX - (BaseTime + TimeDelta / MSecsPerDay)) * 24) / 24`, and every
later row in that packet reuses the same shift. Unknown-market sections that
Delphi skips do not fill the shift.

`TradeJoinBuffer` is the Rust active-library equivalent of the Delphi temporary
`tmpList/tmpTradesRead/tmpTradesWrite` path used by `AddTmpHOrder`. It keeps one
ring slot empty, returns `Full` instead of overwriting unread rows, and tries to
aggregate the new row into previous one or previous two rows before appending.
The aggregation checks the same direction, `ChartPriceStep`, and
`SameTradesTime`.

```rust
pub fn prepare_joined_trades_for_retained_append(
    rows: &mut Vec<TradeHistoryRow>,
);
```

This helper is used after draining `TradeJoinBuffer` and before appending rows
to retained `SeqRing` history. In the active Delphi path,
`BMarketHistoryWorker` calls `JoinHOrders(0, NowTime, false, true)`, so
`DontSort=true`: drained tmp-ring rows are appended in ring read order, without
sorting and without dropping late resend rows that are older than the retained
tail. The helper is intentionally a no-op marker for that machine effect.

```rust
pub struct MarketHistoryConfig {
    pub futures_trades_capacity: usize,
    pub spot_trades_capacity: usize,
    pub liquidation_capacity: usize,
    pub mm_orders_capacity: usize,
    pub mm_order_companion_capacity: usize,
    pub last_price_capacity: usize,
    pub mini_candles_capacity: usize,
    pub candles_5m_capacity: usize,
    pub trade_join_capacity: usize,
}

impl MarketHistoryConfig {
    pub fn from_system_memory(market_count: usize) -> Self;
    pub fn from_total_memory_bytes(total_memory_bytes: usize, market_count: usize) -> Self;
    pub fn history_budget_bytes(total_memory_bytes: usize) -> usize;
    pub fn estimated_bytes_per_market(&self) -> usize;
}

pub struct MarketHistoryReaders {
    pub futures_trades: Option<SeqRingReader<TradeHistoryRow>>,
    pub spot_trades: Option<SeqRingReader<TradeHistoryRow>>,
    pub liquidations: Option<SeqRingReader<TradeHistoryRow>>,
    pub mm_orders: Option<SeqRingReader<MMOrderHistoryRow>>,
    pub mm_order_companion: Option<SeqRingReader<MMOrderCompanionData>>,
    pub last_prices: Option<SeqRingReader<LastPricePoint>>,
    pub mini_candles: Option<SeqRingReader<MiniCandle>>,
    pub candles_5m: Option<SeqRingReader<Candle5mRow>>,
}

pub struct MarketHistoryWorker;
pub struct MarketHistoryHandle;

impl MarketHistoryWorker {
    pub fn spawn(default_config: MarketHistoryConfig) -> Self;
    pub fn handle(&self) -> MarketHistoryHandle;
    pub fn readers(&self, market_name: &str) -> Option<MarketHistoryReaders>;
    pub fn rolling_volumes(
        &self,
        market_name: &str,
        now_time: f64,
    ) -> Option<RollingTradeVolumeSnapshot>;
    pub fn derived_snapshot(
        &self,
        market_name: &str,
        now_time: f64,
    ) -> Option<MarketDerivedSnapshot>;
    pub fn flush(&self, now_time: f64) -> bool;
}

impl EventDispatcher {
    pub fn set_market_history_handle(&mut self, handle: MarketHistoryHandle);
    pub fn clear_market_history_handle(&mut self);
    pub fn apply_candles_snapshot(&mut self, markets: &[RequestCandlesMarket]) -> bool;
}

pub struct MarketHistoryLastPriceInput {
    pub market_name: String,
    pub current: f64,
    pub bid: f64,
    pub ask: f64,
    pub is_btc_market: bool,
    pub is_base_usdt_market: bool,
}

pub struct MarketHistoryLastPriceBatch {
    pub now_time: f64,
    pub rows: Vec<MarketHistoryLastPriceInput>,
}

impl MarketHistoryHandle {
    pub fn readers(&self, market_name: &str) -> Option<MarketHistoryReaders>;
    pub fn rolling_volumes(
        &self,
        market_name: &str,
        now_time: f64,
    ) -> Option<RollingTradeVolumeSnapshot>;
    pub fn derived_snapshot(
        &self,
        market_name: &str,
        now_time: f64,
    ) -> Option<MarketDerivedSnapshot>;
    pub fn send_stream_batch(&self, batch: MarketHistoryStreamBatch) -> bool;
    pub fn send_last_price_batch(&self, batch: MarketHistoryLastPriceBatch) -> bool;
    pub fn flush(&self, now_time: f64) -> bool;
}

pub struct SeqRingCursor;

impl<T> SeqRingReader<T> {
    pub fn cursor_from_oldest(&self) -> SeqRingCursor;
    pub fn cursor_from_now(&self) -> SeqRingCursor;
    pub fn copy_last(&self, limit: usize, out: &mut Vec<T>) -> SeqRingReadMeta;
    pub fn copy_from_time(
        &self,
        time: f64,
        limit: usize,
        out: &mut Vec<T>,
    ) -> Option<SeqRingReadMeta>;
    pub fn copy_time_range(
        &self,
        from_time: f64,
        to_time: f64,
        limit: usize,
        out: &mut Vec<T>,
    ) -> Option<SeqRingReadMeta>;
    pub fn copy_new_since(
        &self,
        cursor: &mut SeqRingCursor,
        limit: usize,
        out: &mut Vec<T>,
    ) -> SeqRingReadMeta;
}

pub struct MarketHistoryStore;
pub struct MarketHistoryRegistry;

impl MarketHistoryStore {
    pub fn new(config: MarketHistoryConfig) -> Self;
    pub fn readers(&self) -> MarketHistoryReaders;
    pub fn push_futures_trade_into_join_like_delphi(
        &mut self,
        row: TradeHistoryRow,
        chart_price_step: f64,
    );
    pub fn push_futures_stream_trade_like_delphi(
        &mut self,
        base_time: f64,
        time_delta_ms: i16,
        now_time: f64,
        price: f32,
        qty: f32,
        chart_price_step: f64,
        time_shift: &mut TradesPacketTimeShift,
    ) -> f64;
    pub fn drain_joined_futures_like_delphi(&mut self) -> usize;
    pub fn append_spot_trade_like_delphi(&mut self, row: TradeHistoryRow) -> Option<u64>;
    pub fn append_spot_stream_trade_like_delphi(
        &mut self,
        base_time: f64,
        time_delta_ms: i16,
        now_time: f64,
        price: f32,
        qty: f32,
        time_shift: &mut TradesPacketTimeShift,
    ) -> (f64, Option<u64>);
    pub fn append_liquidation_like_delphi(&mut self, row: TradeHistoryRow) -> Option<u64>;
    pub fn append_liquidation_stream_like_delphi(
        &mut self,
        base_time: f64,
        time_delta_ms: i16,
        now_time: f64,
        price: f32,
        qty: f32,
        time_shift: &mut TradesPacketTimeShift,
    ) -> (f64, Option<u64>);
    pub fn append_mm_order_like_delphi(&mut self, row: MMOrderHistoryRow) -> Option<u64>;
    pub fn append_mm_order_with_companion_like_delphi(
        &mut self,
        row: MMOrderHistoryRow,
        companion: Option<MMOrderCompanionData>,
    ) -> Option<u64>;
    pub fn append_mm_stream_order_like_delphi(
        &mut self,
        base_time: f64,
        time_delta_ms: i16,
        now_time: f64,
        vol: f32,
        q: f32,
        taker: Option<[u8; 20]>,
        time_shift: &mut TradesPacketTimeShift,
    ) -> (f64, Option<u64>);
    pub fn compact_evicted_futures_like_delphi(&mut self, now_time: f64) -> usize;
    pub fn rolling_volumes_snapshot(&self, now_time: f64) -> RollingTradeVolumeSnapshot;
    pub fn derived_snapshot(&self) -> MarketDerivedSnapshot;
}

impl MarketHistoryRegistry {
    pub fn new(default_config: MarketHistoryConfig) -> Self;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn contains_market(&self, market_name: &str) -> bool;
    pub fn get(&self, market_name: &str) -> Option<&MarketHistoryStore>;
    pub fn get_mut(&mut self, market_name: &str) -> Option<&mut MarketHistoryStore>;
    pub fn readers(&self, market_name: &str) -> Option<MarketHistoryReaders>;
}
```

`MarketHistoryWorker` is the retained-history writer. Attach its handle to the
dispatcher before running the client:

```rust
use moonproto::{Client, EventDispatcher};
use moonproto::state::{MarketHistoryConfig, MarketHistoryWorker};

let worker = MarketHistoryWorker::spawn(MarketHistoryConfig::default());

let mut dispatcher = EventDispatcher::new();
dispatcher.set_market_history_handle(worker.handle());
client.subscribe_all_trades(false);

client.run_with_dispatcher(duration, &mut dispatcher, Box::new(|event| {
    handle_event(event);
}));

let btc = worker.readers("BTCUSDT").expect("market storage was created by the trades subscription");
```

`MarketHistoryStore` is the per-market single-writer side owned by that worker.
Capacities set to `0` disable only that retained public history ring. Futures
trades first enter the Delphi-like temporary join buffer and are drained into
retained history in Delphi `DontSort=true` ring-read order. Rows evicted from
the futures retained ring are buffered for `TMiniCandle` compaction.
The `*_stream_*_like_delphi` helpers convert `BaseTime + TimeDeltaMS` through a
shared `TradesPacketTimeShift`, so all retained row types in one packet use the
same Delphi packet time correction. MM-order companion rows are aligned with
the base MM-order ring slot: when a taker address is present, the helper stores
`TMMOrderData` with Delphi `HLAddressColor`; otherwise it stores default
companion data for that slot.
LastPrice rows are produced from active `UpdateMarketsList`, not from trades:
the dispatcher computes `pLast = (Bid + Ask) / 2` in the same market-price
apply block and queues a `MarketHistoryLastPriceBatch`; the worker then applies
the Delphi `TMarket.AddFrom` gate before appending to the retained LastPrice
ring.
Readers are cloneable handles; application code reads last N rows, from time,
or a time range through `SeqRingReader` without knowing the writer internals.
Futures retained rows preserve append order, not guaranteed timestamp order:
`copy_from_time` starts from the first retained sequence whose row time is at or
after the requested time, and `copy_time_range` scans retained rows and returns
only rows inside the requested time interval.
For "only new rows", every consumer keeps its own `SeqRingCursor`; the library
does not have global consumed/unconsumed state, so UI, strategy code, and logs
can read the same history independently. Internally `SeqRing` stores rows as a
dense retained ring under short read/write locks; the protocol receive path is
not the retained-history writer.

Typical "only new trades" usage:

```rust
use moonproto::state::{SeqRingCursor, SeqRingReader, TradeHistoryRow};

struct MyTradeConsumer {
    cursor: SeqRingCursor,
    scratch: Vec<TradeHistoryRow>,
}

impl MyTradeConsumer {
    fn start_from_now(reader: &SeqRingReader<TradeHistoryRow>) -> Self {
        Self {
            cursor: reader.cursor_from_now(),
            scratch: Vec::new(),
        }
    }

    fn start_from_retained_tail(reader: &SeqRingReader<TradeHistoryRow>) -> Self {
        Self {
            cursor: reader.cursor_from_oldest(),
            scratch: Vec::new(),
        }
    }

    fn poll_new(&mut self, reader: &SeqRingReader<TradeHistoryRow>) {
        let meta = reader.copy_new_since(&mut self.cursor, 4096, &mut self.scratch);
        if meta.clipped {
            on_history_gap(meta.actual_start_seq);
        }
        for trade in &self.scratch {
            on_trade(*trade);
        }
    }
}
```

`cursor_from_now()` is for live consumers that do not want the already retained
tail. `cursor_from_oldest()` first drains everything currently retained. If
`copy_new_since` reports `clipped`, the consumer was slower than retention: the
returned rows start at the oldest still available row, and the missing older
rows cannot be recovered from this retained ring. This is per-consumer state;
do not share one cursor between independent UI panels, strategy workers, or
logging loops.

`MarketHistoryRegistry` is the worker-owned map of per-market stores. Active
Lib configures it from the current trades subscription and the known
`GetMarketsList` universe: all markets for `subscribe_all_trades`, or only the
requested subset for `subscribe_trades_for`. The active dispatcher queues rows
only for stores allowed by that scope.

`MarketHistoryConfig::from_system_memory(market_count)` is the recommended
RAM-budget helper for init/config code. It probes total physical RAM, falls
back to fixed `Default` if the OS probe fails, and then delegates to
`from_total_memory_bytes(total_memory_bytes, market_count)`. The helper budgets
about 20% of total memory for retained histories, or 25% on machines below 8
GiB, then splits that budget across the given market count and categories. When
futures retained history is enabled, the helper keeps the futures temporary
join ring at the Delphi `IntTradesBufSize = 1000` size; it must not shrink this
ring as a hidden memory optimization. `estimated_bytes_per_market` uses the
dense row sizes used by `SeqRing` and is intended for tests and config
diagnostics. `Default` is intentionally conservative because
`subscribe_all_trades` creates stores for all known markets; use
`from_system_memory(market_count)` or explicit capacities when a client wants a
larger retained tail.

## Recovery Behavior

`TradesState` maintains up to 50 gap buckets. Each bucket retries missing packet
numbers up to three times with the Delphi delay formula:

```text
PathDelay = min(1800, max(300, RTT * (1.2 + retry * 0.7))) ms
```

`Client::run_with_dispatcher` calls the trades recovery check after successfully
parsed live/resend trades packets, under the same 100 ms `LastCheckMissingTime`
throttle as Delphi, and sends the generated `emk_TradesResend` requests
automatically.

Recovery is best-effort, matching Delphi. Missing packet numbers are requested
for up to three bucket retry cycles. If the bucket is still incomplete after the
retry budget, it is closed and the live stream continues; the library does not
keep flooding the channel for old trades. A late resend packet can still be
parsed and emitted as `Apply(packet)`, but it no longer reopens the closed
bucket.

`EventDispatcher` also drops trades packets while market indexes are not
synchronized after a server restart.

## Low-Level Use

Custom tools can use the parser and state directly:

```rust
use moonproto::commands::trades_stream::parse_trades_packet;
use moonproto::state::{parse_trades_resend_response, TradesState};

let mut trades = TradesState::new();
let packet = parse_trades_packet(payload).expect("bad trades packet");
let events = trades.on_packet(packet, now_ms);

// Delphi-equivalent: call after a successfully parsed trades packet, not from
// an independent timer while no trades packets are arriving.
for resend_request in trades.tick(rtt_ms, now_ms) {
    client.send_api_request(&resend_request);
}

for raw_packet in parse_trades_resend_response(resend_response_payload) {
    if let Some(packet) = parse_trades_packet(&raw_packet) {
        let historical_events = trades.on_packet_resend(packet);
        for resend_request in trades.tick(rtt_ms, now_ms) {
            client.send_api_request(&resend_request);
        }
    }
}
```

Regular applications should not send resend requests manually.
