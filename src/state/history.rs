//! Active-library retained history row types.
//!
//! These rows are the typed payloads stored by [`crate::state::seq_ring`].
//! They intentionally mirror Delphi storage records where the record is a
//! user-visible/history concept rather than only a wire packet.

use crate::state::seq_ring::SeqRingTimedRow;
use crate::time::DelphiTime;

const SECONDS_PER_DAY: f64 = 86_400.0;
pub const DELPHI_MSECS_PER_DAY: f64 = 86_400_000.0;
const MINI_CANDLE_SPLIT_DAYS: f64 = 5.0 / SECONDS_PER_DAY;
const ROLLING_VOLUME_BUCKET_SECONDS: i64 = 5;
const ROLLING_VOLUME_BUCKETS: usize = 5 * 60 / ROLLING_VOLUME_BUCKET_SECONDS as usize;
pub const DELPHI_SAME_TRADES_TIME_DAYS: f64 = 0.2 / SECONDS_PER_DAY;

/// Delphi `ProcessTradesStream` per-packet time-shift state.
///
/// The first known/stored row in a packet fixes
/// `TimeShift := round((NowTimeX - RowTime) * 24) / 24`; every later row in the
/// packet uses the same shift. Unknown-market sections skipped by Delphi do not
/// fill this value.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub(crate) struct TradesPacketTimeShift {
    shift_days: Option<f64>,
}

impl TradesPacketTimeShift {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub(crate) fn shift_days(&self) -> Option<f64> {
        self.shift_days
    }

    pub(crate) fn apply_like_delphi(
        &mut self,
        base_time: f64,
        time_delta_ms: i16,
        now_time: f64,
    ) -> f64 {
        let row_time = base_time + f64::from(time_delta_ms) / DELPHI_MSECS_PER_DAY;
        let shift = *self
            .shift_days
            .get_or_insert_with(|| ((now_time - row_time) * 24.0).round() / 24.0);
        row_time + shift
    }
}

/// Delphi `TTrade`: detailed trade/liquidation row stored in market history.
///
/// Delphi layout is 16 bytes: `Time: TDateTime; Price: Single; Qty: Single`.
/// `Qty` is signed exactly like Delphi: sign bit clear means buy, sign bit set
/// means sell. This intentionally uses sign-bit checks, so `-0.0` has the same
/// machine effect as Delphi's `PCardinal(@Qty)^ and $80000000`.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct TradeHistoryRow {
    pub time: f64,
    pub price: f32,
    pub qty: f32,
}

impl TradeHistoryRow {
    #[inline]
    pub fn time_delphi(self) -> DelphiTime {
        DelphiTime::from_days(self.time)
    }

    #[inline]
    pub fn unix_millis(self) -> Option<i64> {
        self.time_delphi().unix_millis()
    }

    pub fn quantity(self) -> f32 {
        self.qty.abs()
    }

    pub fn is_buy(self) -> bool {
        self.qty.to_bits() & 0x8000_0000 == 0
    }

    pub fn same_direction(self, other: Self) -> bool {
        (self.qty.to_bits() ^ other.qty.to_bits()) & 0x8000_0000 == 0
    }

    pub fn traded_value(self) -> f32 {
        self.price * self.quantity()
    }
}

impl SeqRingTimedRow for TradeHistoryRow {
    fn seq_ring_time(&self) -> f64 {
        self.time
    }
}

/// Delphi `TMMOrder`: main market-maker history row.
///
/// Delphi layout is `Time: TDateTime; vol: Double; Q: Double`. Optional taker
/// address and color are companion data in Delphi
/// `TStreamableRingBuffer<TMMOrder, TMMOrderData>` and must be ported as a
/// separate companion layer, not silently folded into this base row.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct MMOrderHistoryRow {
    pub time: f64,
    pub vol: f64,
    pub q: f64,
}

impl MMOrderHistoryRow {
    #[inline]
    pub fn time_delphi(self) -> DelphiTime {
        DelphiTime::from_days(self.time)
    }
}

impl SeqRingTimedRow for MMOrderHistoryRow {
    fn seq_ring_time(&self) -> f64 {
        self.time
    }
}

/// Delphi `TMMOrderData`: companion data for `TMMOrder`.
///
/// Delphi layout is `Taker: THLAddress` (20 bytes) and `Color: TColor`. It is
/// stored beside the base `TMMOrder` row by slot, not inside the base row.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct MMOrderCompanionData {
    pub taker: [u8; 20],
    pub color: u32,
}

pub fn hl_address_color(taker: [u8; 20]) -> u32 {
    let mut r = 0u8;
    let mut g = 0u8;
    let mut b = 0u8;
    for (idx, byte) in taker.into_iter().enumerate() {
        match idx % 3 {
            0 => r ^= byte,
            1 => g ^= byte,
            _ => b ^= byte,
        }
    }

    let scale = |x: u8| -> u32 { ((u32::from(x) * 5) >> 3) + 80 };
    0xFF00_0000 | (scale(r) << 16) | (scale(g) << 8) | scale(b)
}

#[doc(hidden)]
pub fn hl_address_color_like_delphi(taker: [u8; 20]) -> u32 {
    hl_address_color(taker)
}

/// Delphi `THistoricalPrices` used by `Market.HistoryPrice`.
///
/// Delphi layout is `packed record current: Single; RealTime: TDateTime`.
/// MoonBot draws the brown LastPrice chart line from this history. The source
/// value is `UpdateMarketsList -> pLast = (Bid + Ask) / 2`, not the trades
/// stream last trade price.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct LastPricePoint {
    pub current: f32,
    pub real_time: f64,
}

impl LastPricePoint {
    #[inline]
    pub fn time_delphi(self) -> DelphiTime {
        DelphiTime::from_days(self.real_time)
    }
}

impl SeqRingTimedRow for LastPricePoint {
    fn seq_ring_time(&self) -> f64 {
        self.real_time
    }
}

/// Active Lib retained 5-minute candle row.
///
/// The source snapshot is Delphi `TDeepPrice` / `TDeepPricePack`, but the row
/// lives in state because applications read it together with trades and
/// derived analytics. `buyWall/sellWall` from the wire snapshot are deliberately
/// not retained here: the library decision is to expose candles, not wall UI
/// helpers.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct Candle5mRow {
    pub open_p: f32,
    pub close_p: f32,
    pub max_p: f32,
    pub min_p: f32,
    pub vol: f32,
    pub time: f64,
}

impl Candle5mRow {
    pub fn from_deep_price(row: crate::commands::candles::DeepPrice) -> Self {
        Self {
            open_p: row.open_p,
            close_p: row.close_p,
            max_p: row.max_p,
            min_p: row.min_p,
            vol: row.vol,
            time: row.time,
        }
    }

    #[inline]
    pub fn open(self) -> f32 {
        self.open_p
    }

    #[inline]
    pub fn close(self) -> f32 {
        self.close_p
    }

    #[inline]
    pub fn high(self) -> f32 {
        self.max_p
    }

    #[inline]
    pub fn low(self) -> f32 {
        self.min_p
    }

    #[inline]
    pub fn time_delphi(self) -> DelphiTime {
        DelphiTime::from_days(self.time)
    }

    #[inline]
    pub fn unix_millis(self) -> Option<i64> {
        self.time_delphi().unix_millis()
    }
}

impl SeqRingTimedRow for Candle5mRow {
    fn seq_ring_time(&self) -> f64 {
        self.time
    }
}

/// Delphi `TMiniCandle` used to compact evicted detailed trades.
///
/// Delphi layout is 24 bytes: `Time: TDateTime; Cnt: Integer; MinPrice,
/// MaxPrice, BuyVol, SellVol: Single`.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct MiniCandle {
    pub time: f64,
    pub cnt: i32,
    pub min_price: f32,
    pub max_price: f32,
    pub buy_vol: f32,
    pub sell_vol: f32,
}

impl MiniCandle {
    #[inline]
    pub fn time_delphi(self) -> DelphiTime {
        DelphiTime::from_days(self.time)
    }
}

impl SeqRingTimedRow for MiniCandle {
    fn seq_ring_time(&self) -> f64 {
        self.time
    }
}

/// Compact detailed trades into Delphi `TMiniCandle` groups.
///
/// This mirrors the `UseTradesCompression` body inside Delphi
/// `TMarket.ResizeOrdersHistory`: the group anchor is the first trade time, a
/// new candle starts when `abs(anchor - row.Time) > 5 / SecsPerDay`, split
/// groups are appended only when newer than `last_mini_time` and older than the
/// resize `now_time`, and the final group only checks `c.Time > last_mini_time`.
pub(crate) fn compact_trades_to_mini_candles_like_delphi(
    rows: &[TradeHistoryRow],
    last_mini_time: f64,
    now_time: f64,
    out: &mut Vec<MiniCandle>,
) {
    let Some(first) = rows.first() else {
        return;
    };

    let mut newest_mini_time = last_mini_time;
    let mut anchor_time = first.time;
    let mut candle = empty_mini_candle(anchor_time);

    for row in rows {
        if (anchor_time - row.time).abs() > MINI_CANDLE_SPLIT_DAYS && candle.cnt > 0 {
            if candle.time > newest_mini_time && candle.time < now_time {
                out.push(candle);
                newest_mini_time = candle.time;
            }

            anchor_time = row.time;
            candle = empty_mini_candle(anchor_time);
        }

        if row.is_buy() {
            candle.buy_vol += row.traded_value();
        } else {
            candle.sell_vol += row.traded_value();
        }
        if candle.cnt == 0 {
            candle.min_price = row.price;
        }
        candle.max_price = candle.max_price.max(row.price);
        candle.min_price = candle.min_price.min(row.price);
        candle.cnt += 1;
    }

    if candle.cnt > 0 && candle.time > newest_mini_time {
        out.push(candle);
    }
}

fn empty_mini_candle(time: f64) -> MiniCandle {
    MiniCandle {
        time,
        cnt: 0,
        min_price: 0.0,
        max_price: 0.0,
        buy_vol: 0.0,
        sell_vol: 0.0,
    }
}

/// Buy/sell rolling volume totals.
///
/// `*_value` is `Price * Abs(Qty)`, matching Delphi volume calculations over
/// `TTrade`. `*_qty` keeps the coin/base quantity separately for clients that
/// need it.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct TradeVolumeTotals {
    pub buy_value: f64,
    pub sell_value: f64,
    pub buy_qty: f64,
    pub sell_qty: f64,
    pub trade_count: u32,
    pub min_price: f32,
    pub max_price: f32,
}

impl TradeVolumeTotals {
    pub fn total_value(self) -> f64 {
        self.buy_value + self.sell_value
    }

    pub fn price_delta_percent(self) -> f64 {
        if self.min_price <= 0.0 || self.max_price <= 0.0 || self.max_price < self.min_price {
            return 0.0;
        }
        (f64::from(self.max_price) / f64::from(self.min_price) - 1.0) * 100.0
    }

    fn add_trade(&mut self, row: TradeHistoryRow) {
        let qty = row.quantity() as f64;
        let value = row.price as f64 * qty;
        if row.is_buy() {
            self.buy_value += value;
            self.buy_qty += qty;
        } else {
            self.sell_value += value;
            self.sell_qty += qty;
        }
        self.add_price(row.price);
        self.trade_count = self.trade_count.saturating_add(1);
    }

    fn add_price(&mut self, price: f32) {
        if price <= 0.0 {
            return;
        }
        if self.min_price <= 0.0 || price < self.min_price {
            self.min_price = price;
        }
        if price > self.max_price {
            self.max_price = price;
        }
    }

    fn add_totals(&mut self, other: Self) {
        self.buy_value += other.buy_value;
        self.sell_value += other.sell_value;
        self.buy_qty += other.buy_qty;
        self.sell_qty += other.sell_qty;
        self.trade_count = self.trade_count.saturating_add(other.trade_count);
        self.add_price(other.min_price);
        self.add_price(other.max_price);
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct RollingTradeVolumeSnapshot {
    pub one_minute: TradeVolumeTotals,
    pub three_minutes: TradeVolumeTotals,
    pub five_minutes: TradeVolumeTotals,
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
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

#[derive(Debug, Default, Clone, Copy, PartialEq)]
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

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct MarketDerivedSnapshot {
    pub trade_volumes: RollingTradeVolumeSnapshot,
    /// Total quote volume from retained 5m candles plus the current candle.
    /// Unlike trade volumes, candles do not carry buy/sell split.
    pub candle_volumes: CandleVolumeSnapshot,
    /// Deltas from retained/joined futures trades. Currently populated for
    /// 1m and 5m windows from the same 5-second buckets as volumes.
    pub trade_deltas: DerivedDeltaSnapshot,
    /// Deltas from retained 5m candles plus the current candle.
    ///
    /// Long candle delta fields follow Delphi `RecalcPumpQ` bucket semantics:
    /// `two_hours` is Delphi `Last2hDelta` (`h <= 2`, roughly three hours),
    /// `three_hours` is `Last3hDelta` (`h <= 3`, roughly four hours), and
    /// `twenty_four_hours` is `Last24hDelta` (`h <= 24`, roughly 25 hours).
    pub candle_deltas: DerivedDeltaSnapshot,
    /// Deltas from Delphi's retained LastPrice/HistoryPrice line.
    ///
    /// Delphi feeds this line from `UpdateMarketsList -> TMarket.AddFrom` and
    /// uses it for the 15m/30m/1h derived windows in `CheckHourlyValues`.
    pub last_price_deltas: DerivedDeltaSnapshot,
    /// Combined convenient view. For each field it is the max of the trade and
    /// retained-history sources for that window, matching Delphi's "do not
    /// lower a hotter delta with a colder source" shape.
    pub deltas: DerivedDeltaSnapshot,
}

/// Incremental rolling volumes for the Active Lib trade history.
///
/// Buckets are 5 seconds wide and cover 5 minutes total. This intentionally
/// differs from Delphi's expensive scan in `JoinHOrders`, but preserves the
/// public value being maintained: fast buy/sell trade volume over 1/3/5 minute
/// windows. The accepted precision loss is bounded by one bucket width.
#[derive(Debug, Clone)]
pub(crate) struct RollingTradeVolumes {
    buckets: [TradeVolumeBucket; ROLLING_VOLUME_BUCKETS],
}

#[derive(Debug, Clone, Copy)]
struct TradeVolumeBucket {
    bucket_id: i64,
    totals: TradeVolumeTotals,
}

impl Default for TradeVolumeBucket {
    fn default() -> Self {
        Self {
            bucket_id: i64::MIN,
            totals: TradeVolumeTotals::default(),
        }
    }
}

impl Default for RollingTradeVolumes {
    fn default() -> Self {
        Self {
            buckets: [TradeVolumeBucket::default(); ROLLING_VOLUME_BUCKETS],
        }
    }
}

impl RollingTradeVolumes {
    pub(crate) fn add_trade(&mut self, row: TradeHistoryRow) {
        let bucket_id = volume_bucket_id(row.time);
        let idx = volume_bucket_index(bucket_id);
        let bucket = &mut self.buckets[idx];
        if bucket.bucket_id != bucket_id {
            *bucket = TradeVolumeBucket {
                bucket_id,
                totals: TradeVolumeTotals::default(),
            };
        }
        bucket.totals.add_trade(row);
    }

    pub(crate) fn snapshot(&self, now_time: f64) -> RollingTradeVolumeSnapshot {
        let now_bucket = volume_bucket_id(now_time);
        let one_minute_oldest = oldest_volume_bucket(now_bucket, 60);
        let three_minutes_oldest = oldest_volume_bucket(now_bucket, 3 * 60);
        let five_minutes_oldest = oldest_volume_bucket(now_bucket, 5 * 60);

        let mut snapshot = RollingTradeVolumeSnapshot::default();
        for bucket in &self.buckets {
            if bucket.bucket_id < five_minutes_oldest || bucket.bucket_id > now_bucket {
                continue;
            }
            snapshot.five_minutes.add_totals(bucket.totals);
            if bucket.bucket_id >= three_minutes_oldest {
                snapshot.three_minutes.add_totals(bucket.totals);
            }
            if bucket.bucket_id >= one_minute_oldest {
                snapshot.one_minute.add_totals(bucket.totals);
            }
        }
        snapshot
    }
}

fn oldest_volume_bucket(now_bucket: i64, window_seconds: i64) -> i64 {
    let buckets_back =
        (window_seconds + ROLLING_VOLUME_BUCKET_SECONDS - 1) / ROLLING_VOLUME_BUCKET_SECONDS;
    now_bucket - buckets_back + 1
}

fn volume_bucket_id(time: f64) -> i64 {
    ((time * SECONDS_PER_DAY).floor() as i64).div_euclid(ROLLING_VOLUME_BUCKET_SECONDS)
}

fn volume_bucket_index(bucket_id: i64) -> usize {
    bucket_id.rem_euclid(ROLLING_VOLUME_BUCKETS as i64) as usize
}

#[cfg(test)]
mod tests;
