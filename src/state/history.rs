//! Active-library retained history row types.
//!
//! These rows are the typed payloads stored by [`crate::state::seq_ring`].
//! They intentionally mirror Delphi storage records where the record is a
//! user-visible/history concept rather than only a wire packet.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::state::seq_ring::{SeqRingRow, SeqRingRowSlot, SeqRingTimedRow};

const SECONDS_PER_DAY: f64 = 86_400.0;
const MINI_CANDLE_SPLIT_DAYS: f64 = 5.0 / SECONDS_PER_DAY;
const ROLLING_VOLUME_BUCKET_SECONDS: i64 = 5;
const ROLLING_VOLUME_BUCKETS: usize = 5 * 60 / ROLLING_VOLUME_BUCKET_SECONDS as usize;
pub const DELPHI_SAME_TRADES_TIME_DAYS: f64 = 0.2 / SECONDS_PER_DAY;

/// Delphi `TTrade`: detailed trade/liquidation row stored in market history.
///
/// Delphi layout is 16 bytes: `Time: TDateTime; Price: Single; Qty: Single`.
/// `Qty` is signed exactly like Delphi: sign bit clear means buy, sign bit set
/// means sell. This intentionally uses sign-bit checks, so `-0.0` has the same
/// machine effect as Delphi's `PCardinal(@Qty)^ and $80000000`.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct TradeHistoryRow {
    pub time: f64,
    pub price: f32,
    pub qty: f32,
}

impl TradeHistoryRow {
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

#[derive(Default)]
pub struct TradeHistoryRowSlot {
    time_bits: AtomicU64,
    price_bits: AtomicU32,
    qty_bits: AtomicU32,
}

impl SeqRingRow for TradeHistoryRow {
    type Slot = TradeHistoryRowSlot;
}

impl SeqRingTimedRow for TradeHistoryRow {
    fn seq_ring_time(&self) -> f64 {
        self.time
    }
}

impl SeqRingRowSlot for TradeHistoryRowSlot {
    type Row = TradeHistoryRow;

    fn store_row(&self, row: Self::Row) {
        self.time_bits.store(row.time.to_bits(), Ordering::Relaxed);
        self.price_bits
            .store(row.price.to_bits(), Ordering::Relaxed);
        self.qty_bits.store(row.qty.to_bits(), Ordering::Relaxed);
    }

    fn load_row(&self) -> Self::Row {
        TradeHistoryRow {
            time: f64::from_bits(self.time_bits.load(Ordering::Relaxed)),
            price: f32::from_bits(self.price_bits.load(Ordering::Relaxed)),
            qty: f32::from_bits(self.qty_bits.load(Ordering::Relaxed)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeJoinPush {
    Inserted,
    AggregatedPrev1,
    AggregatedPrev2,
    Full,
}

/// Delphi `AddTmpHOrder`-style temporary trade ring.
///
/// The ring keeps one slot empty, exactly like the Delphi check
/// `If nextWrite = tmpTradesRead then exit`. New rows aggregate into the
/// previous one or previous two rows when direction, price step, and
/// `SameTradesTime` match; otherwise they are appended at `tmpTradesWrite`.
pub struct TradeJoinBuffer {
    rows: Vec<TradeHistoryRow>,
    read: usize,
    write: usize,
    len: usize,
}

impl TradeJoinBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            rows: vec![TradeHistoryRow::default(); capacity],
            read: 0,
            write: 0,
            len: 0,
        }
    }

    pub fn capacity(&self) -> usize {
        self.rows.len()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn push_like_delphi(
        &mut self,
        row: TradeHistoryRow,
        chart_price_step: f64,
        same_trades_time_days: f64,
    ) -> TradeJoinPush {
        let capacity = self.capacity();
        if capacity == 0 {
            return TradeJoinPush::Full;
        }
        let next_write = (self.write + 1) % capacity;
        if next_write == self.read {
            return TradeJoinPush::Full;
        }

        let prev1 = (self.write + capacity - 1) % capacity;
        let prev2 = (self.write + capacity - 2) % capacity;

        if self.len >= 1
            && can_aggregate_tmp_trade(
                self.rows[prev1],
                row,
                chart_price_step,
                same_trades_time_days,
            )
        {
            self.rows[prev1].qty += row.qty;
            return TradeJoinPush::AggregatedPrev1;
        }

        if self.len >= 2
            && can_aggregate_tmp_trade(
                self.rows[prev2],
                row,
                chart_price_step,
                same_trades_time_days,
            )
        {
            self.rows[prev2].qty += row.qty;
            return TradeJoinPush::AggregatedPrev2;
        }

        self.rows[self.write] = row;
        self.write = next_write;
        self.len += 1;
        TradeJoinPush::Inserted
    }

    /// Drain retained temporary rows in read order, like `JoinHOrders` taking a
    /// snapshot from `tmpTradesRead` to `tmpTradesWrite`.
    pub fn drain_into(&mut self, out: &mut Vec<TradeHistoryRow>) {
        out.clear();
        out.reserve(self.len);
        let capacity = self.capacity();
        if capacity == 0 {
            return;
        }
        for offset in 0..self.len {
            out.push(self.rows[(self.read + offset) % capacity]);
        }
        self.read = self.write;
        self.len = 0;
    }
}

fn can_aggregate_tmp_trade(
    prev: TradeHistoryRow,
    row: TradeHistoryRow,
    chart_price_step: f64,
    same_trades_time_days: f64,
) -> bool {
    prev.time > 1.0
        && row.same_direction(prev)
        && ((prev.price - row.price).abs() as f64) < chart_price_step
        && (prev.time - row.time).abs() < same_trades_time_days
}

/// Delphi `TMMOrder`: main market-maker history row.
///
/// Delphi layout is `Time: TDateTime; vol: Double; Q: Double`. Optional taker
/// address and color are companion data in Delphi
/// `TStreamableRingBuffer<TMMOrder, TMMOrderData>` and must be ported as a
/// separate companion layer, not silently folded into this base row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MMOrderHistoryRow {
    pub time: f64,
    pub vol: f64,
    pub q: f64,
}

#[derive(Default)]
pub struct MMOrderHistoryRowSlot {
    time_bits: AtomicU64,
    vol_bits: AtomicU64,
    q_bits: AtomicU64,
}

impl SeqRingRow for MMOrderHistoryRow {
    type Slot = MMOrderHistoryRowSlot;
}

impl SeqRingTimedRow for MMOrderHistoryRow {
    fn seq_ring_time(&self) -> f64 {
        self.time
    }
}

impl SeqRingRowSlot for MMOrderHistoryRowSlot {
    type Row = MMOrderHistoryRow;

    fn store_row(&self, row: Self::Row) {
        self.time_bits.store(row.time.to_bits(), Ordering::Relaxed);
        self.vol_bits.store(row.vol.to_bits(), Ordering::Relaxed);
        self.q_bits.store(row.q.to_bits(), Ordering::Relaxed);
    }

    fn load_row(&self) -> Self::Row {
        MMOrderHistoryRow {
            time: f64::from_bits(self.time_bits.load(Ordering::Relaxed)),
            vol: f64::from_bits(self.vol_bits.load(Ordering::Relaxed)),
            q: f64::from_bits(self.q_bits.load(Ordering::Relaxed)),
        }
    }
}

/// Delphi `TMMOrderData`: companion data for `TMMOrder`.
///
/// Delphi layout is `Taker: THLAddress` (20 bytes) and `Color: TColor`. It is
/// stored beside the base `TMMOrder` row by slot, not inside the base row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MMOrderCompanionData {
    pub taker: [u8; 20],
    pub color: u32,
}

#[derive(Default)]
pub struct MMOrderCompanionDataSlot {
    taker0: AtomicU64,
    taker1: AtomicU64,
    taker2: AtomicU32,
    color: AtomicU32,
}

impl SeqRingRow for MMOrderCompanionData {
    type Slot = MMOrderCompanionDataSlot;
}

impl SeqRingRowSlot for MMOrderCompanionDataSlot {
    type Row = MMOrderCompanionData;

    fn store_row(&self, row: Self::Row) {
        self.taker0.store(
            u64::from_le_bytes(row.taker[0..8].try_into().unwrap()),
            Ordering::Relaxed,
        );
        self.taker1.store(
            u64::from_le_bytes(row.taker[8..16].try_into().unwrap()),
            Ordering::Relaxed,
        );
        self.taker2.store(
            u32::from_le_bytes(row.taker[16..20].try_into().unwrap()),
            Ordering::Relaxed,
        );
        self.color.store(row.color, Ordering::Relaxed);
    }

    fn load_row(&self) -> Self::Row {
        let mut taker = [0u8; 20];
        taker[0..8].copy_from_slice(&self.taker0.load(Ordering::Relaxed).to_le_bytes());
        taker[8..16].copy_from_slice(&self.taker1.load(Ordering::Relaxed).to_le_bytes());
        taker[16..20].copy_from_slice(&self.taker2.load(Ordering::Relaxed).to_le_bytes());
        MMOrderCompanionData {
            taker,
            color: self.color.load(Ordering::Relaxed),
        }
    }
}

/// Delphi `THistoricalPrices` used by `Market.HistoryPrice`.
///
/// Delphi layout is `packed record current: Single; RealTime: TDateTime`.
/// MoonBot draws the brown LastPrice chart line from this history. The source
/// value is `UpdateMarketsList -> pLast = (Bid + Ask) / 2`, not the trades
/// stream last trade price.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LastPricePoint {
    pub current: f32,
    pub real_time: f64,
}

#[derive(Default)]
pub struct LastPricePointSlot {
    current_bits: AtomicU32,
    real_time_bits: AtomicU64,
}

impl SeqRingRow for LastPricePoint {
    type Slot = LastPricePointSlot;
}

impl SeqRingTimedRow for LastPricePoint {
    fn seq_ring_time(&self) -> f64 {
        self.real_time
    }
}

impl SeqRingRowSlot for LastPricePointSlot {
    type Row = LastPricePoint;

    fn store_row(&self, row: Self::Row) {
        self.current_bits
            .store(row.current.to_bits(), Ordering::Relaxed);
        self.real_time_bits
            .store(row.real_time.to_bits(), Ordering::Relaxed);
    }

    fn load_row(&self) -> Self::Row {
        LastPricePoint {
            current: f32::from_bits(self.current_bits.load(Ordering::Relaxed)),
            real_time: f64::from_bits(self.real_time_bits.load(Ordering::Relaxed)),
        }
    }
}

/// Delphi `TMiniCandle` used to compact evicted detailed trades.
///
/// Delphi layout is 24 bytes: `Time: TDateTime; Cnt: Integer; MinPrice,
/// MaxPrice, BuyVol, SellVol: Single`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MiniCandle {
    pub time: f64,
    pub cnt: i32,
    pub min_price: f32,
    pub max_price: f32,
    pub buy_vol: f32,
    pub sell_vol: f32,
}

#[derive(Default)]
pub struct MiniCandleSlot {
    time_bits: AtomicU64,
    cnt: AtomicU32,
    min_price_bits: AtomicU32,
    max_price_bits: AtomicU32,
    buy_vol_bits: AtomicU32,
    sell_vol_bits: AtomicU32,
}

impl SeqRingRow for MiniCandle {
    type Slot = MiniCandleSlot;
}

impl SeqRingTimedRow for MiniCandle {
    fn seq_ring_time(&self) -> f64 {
        self.time
    }
}

impl SeqRingRowSlot for MiniCandleSlot {
    type Row = MiniCandle;

    fn store_row(&self, row: Self::Row) {
        self.time_bits.store(row.time.to_bits(), Ordering::Relaxed);
        self.cnt.store(row.cnt as u32, Ordering::Relaxed);
        self.min_price_bits
            .store(row.min_price.to_bits(), Ordering::Relaxed);
        self.max_price_bits
            .store(row.max_price.to_bits(), Ordering::Relaxed);
        self.buy_vol_bits
            .store(row.buy_vol.to_bits(), Ordering::Relaxed);
        self.sell_vol_bits
            .store(row.sell_vol.to_bits(), Ordering::Relaxed);
    }

    fn load_row(&self) -> Self::Row {
        MiniCandle {
            time: f64::from_bits(self.time_bits.load(Ordering::Relaxed)),
            cnt: self.cnt.load(Ordering::Relaxed) as i32,
            min_price: f32::from_bits(self.min_price_bits.load(Ordering::Relaxed)),
            max_price: f32::from_bits(self.max_price_bits.load(Ordering::Relaxed)),
            buy_vol: f32::from_bits(self.buy_vol_bits.load(Ordering::Relaxed)),
            sell_vol: f32::from_bits(self.sell_vol_bits.load(Ordering::Relaxed)),
        }
    }
}

/// Compact detailed trades into Delphi `TMiniCandle` groups.
///
/// This mirrors the `UseTradesCompression` body inside Delphi
/// `TMarket.ResizeOrdersHistory`: the group anchor is the first trade time, a
/// new candle starts when `abs(anchor - row.Time) > 5 / SecsPerDay`, split
/// groups are appended only when newer than `last_mini_time` and older than the
/// resize `now_time`, and the final group only checks `c.Time > last_mini_time`.
pub fn compact_trades_to_mini_candles_like_delphi(
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
}

impl TradeVolumeTotals {
    pub fn total_value(self) -> f64 {
        self.buy_value + self.sell_value
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
        self.trade_count = self.trade_count.saturating_add(1);
    }

    fn add_totals(&mut self, other: Self) {
        self.buy_value += other.buy_value;
        self.sell_value += other.sell_value;
        self.buy_qty += other.buy_qty;
        self.sell_qty += other.sell_qty;
        self.trade_count = self.trade_count.saturating_add(other.trade_count);
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct RollingTradeVolumeSnapshot {
    pub one_minute: TradeVolumeTotals,
    pub three_minutes: TradeVolumeTotals,
    pub five_minutes: TradeVolumeTotals,
}

/// Incremental rolling volumes for the Active Lib trade history.
///
/// Buckets are 5 seconds wide and cover 5 minutes total. This intentionally
/// differs from Delphi's expensive scan in `JoinHOrders`, but preserves the
/// public value being maintained: fast buy/sell trade volume over 1/3/5 minute
/// windows. The accepted precision loss is bounded by one bucket width.
#[derive(Debug, Clone)]
pub struct RollingTradeVolumes {
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
    pub fn add_trade(&mut self, row: TradeHistoryRow) {
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

    pub fn snapshot(&self, now_time: f64) -> RollingTradeVolumeSnapshot {
        RollingTradeVolumeSnapshot {
            one_minute: self.window(now_time, 60),
            three_minutes: self.window(now_time, 3 * 60),
            five_minutes: self.window(now_time, 5 * 60),
        }
    }

    pub fn window(&self, now_time: f64, window_seconds: i64) -> TradeVolumeTotals {
        let now_bucket = volume_bucket_id(now_time);
        let buckets_back =
            (window_seconds + ROLLING_VOLUME_BUCKET_SECONDS - 1) / ROLLING_VOLUME_BUCKET_SECONDS;
        let oldest_bucket = now_bucket - buckets_back + 1;

        let mut totals = TradeVolumeTotals::default();
        for bucket in &self.buckets {
            if bucket.bucket_id >= oldest_bucket && bucket.bucket_id <= now_bucket {
                totals.add_totals(bucket.totals);
            }
        }
        totals
    }
}

fn volume_bucket_id(time: f64) -> i64 {
    ((time * SECONDS_PER_DAY).floor() as i64).div_euclid(ROLLING_VOLUME_BUCKET_SECONDS)
}

fn volume_bucket_index(bucket_id: i64) -> usize {
    bucket_id.rem_euclid(ROLLING_VOLUME_BUCKETS as i64) as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::SeqRingWriter;

    #[test]
    fn last_price_point_roundtrips_through_seq_ring() {
        let (mut writer, reader) = SeqRingWriter::<LastPricePoint>::new(4).unwrap();
        writer.push(LastPricePoint {
            current: 123.5,
            real_time: 45_000.25,
        });

        let mut out = Vec::new();
        reader.copy_last(1, &mut out);

        assert_eq!(
            out,
            vec![LastPricePoint {
                current: 123.5,
                real_time: 45_000.25,
            }]
        );
    }

    #[test]
    fn mini_candle_roundtrips_through_seq_ring() {
        let (mut writer, reader) = SeqRingWriter::<MiniCandle>::new(2).unwrap();
        writer.push(MiniCandle {
            time: 45_000.0,
            cnt: 7,
            min_price: 10.0,
            max_price: 12.0,
            buy_vol: 100.0,
            sell_vol: 80.0,
        });

        assert_eq!(
            reader.read_at_seq(0),
            Some(MiniCandle {
                time: 45_000.0,
                cnt: 7,
                min_price: 10.0,
                max_price: 12.0,
                buy_vol: 100.0,
                sell_vol: 80.0,
            })
        );
    }

    #[test]
    fn trade_history_row_uses_delphi_qty_sign_bit() {
        let buy = TradeHistoryRow {
            time: 45_000.0,
            price: 100.0,
            qty: 2.5,
        };
        let sell = TradeHistoryRow {
            time: 45_000.1,
            price: 101.0,
            qty: -2.5,
        };
        let negative_zero = TradeHistoryRow {
            time: 45_000.2,
            price: 102.0,
            qty: -0.0,
        };

        assert_eq!(buy.quantity(), 2.5);
        assert!(buy.is_buy());
        assert!(!sell.is_buy());
        assert!(!negative_zero.is_buy());
        assert!(!buy.same_direction(sell));
        assert!(sell.same_direction(negative_zero));
        assert_eq!(sell.traded_value(), 252.5);
    }

    #[test]
    fn trade_history_row_roundtrips_through_seq_ring() {
        let (mut writer, reader) = SeqRingWriter::<TradeHistoryRow>::new(2).unwrap();
        writer.push(TradeHistoryRow {
            time: 45_000.0,
            price: 100.0,
            qty: 2.5,
        });
        writer.push(TradeHistoryRow {
            time: 45_000.25,
            price: 101.0,
            qty: -1.25,
        });

        let mut out = Vec::new();
        reader.copy_from_time(45_000.2, 10, &mut out).unwrap();

        assert_eq!(
            out,
            vec![TradeHistoryRow {
                time: 45_000.25,
                price: 101.0,
                qty: -1.25,
            }]
        );
    }

    #[test]
    fn trade_join_buffer_aggregates_previous_one_like_add_tmp_h_order() {
        let mut buf = TradeJoinBuffer::new(4);
        let t = 45_000.0;

        assert_eq!(
            buf.push_like_delphi(
                TradeHistoryRow {
                    time: t,
                    price: 100.0,
                    qty: 1.0,
                },
                0.1,
                DELPHI_SAME_TRADES_TIME_DAYS,
            ),
            TradeJoinPush::Inserted
        );
        assert_eq!(
            buf.push_like_delphi(
                TradeHistoryRow {
                    time: t + 0.1 / SECONDS_PER_DAY,
                    price: 100.05,
                    qty: 2.0,
                },
                0.1,
                DELPHI_SAME_TRADES_TIME_DAYS,
            ),
            TradeJoinPush::AggregatedPrev1
        );

        let mut out = Vec::new();
        buf.drain_into(&mut out);
        assert_eq!(
            out,
            vec![TradeHistoryRow {
                time: t,
                price: 100.0,
                qty: 3.0,
            }]
        );
    }

    #[test]
    fn trade_join_buffer_aggregates_previous_two_like_add_tmp_h_order() {
        let mut buf = TradeJoinBuffer::new(5);
        let t = 45_000.0;
        buf.push_like_delphi(
            TradeHistoryRow {
                time: t,
                price: 100.0,
                qty: 1.0,
            },
            0.1,
            DELPHI_SAME_TRADES_TIME_DAYS,
        );
        buf.push_like_delphi(
            TradeHistoryRow {
                time: t,
                price: 101.0,
                qty: -1.0,
            },
            0.1,
            DELPHI_SAME_TRADES_TIME_DAYS,
        );

        assert_eq!(
            buf.push_like_delphi(
                TradeHistoryRow {
                    time: t + 0.1 / SECONDS_PER_DAY,
                    price: 100.05,
                    qty: 2.0,
                },
                0.1,
                DELPHI_SAME_TRADES_TIME_DAYS,
            ),
            TradeJoinPush::AggregatedPrev2
        );

        let mut out = Vec::new();
        buf.drain_into(&mut out);
        assert_eq!(
            out,
            vec![
                TradeHistoryRow {
                    time: t,
                    price: 100.0,
                    qty: 3.0,
                },
                TradeHistoryRow {
                    time: t,
                    price: 101.0,
                    qty: -1.0,
                },
            ]
        );
    }

    #[test]
    fn trade_join_buffer_keeps_one_empty_slot_like_delphi_ring() {
        let mut buf = TradeJoinBuffer::new(3);
        let t = 45_000.0;
        for i in 0..2 {
            assert_eq!(
                buf.push_like_delphi(
                    TradeHistoryRow {
                        time: t + i as f64 / SECONDS_PER_DAY,
                        price: 100.0 + i as f32,
                        qty: 1.0,
                    },
                    0.0,
                    DELPHI_SAME_TRADES_TIME_DAYS,
                ),
                TradeJoinPush::Inserted
            );
        }
        assert_eq!(
            buf.push_like_delphi(
                TradeHistoryRow {
                    time: t + 2.0 / SECONDS_PER_DAY,
                    price: 102.0,
                    qty: 1.0,
                },
                0.0,
                DELPHI_SAME_TRADES_TIME_DAYS,
            ),
            TradeJoinPush::Full
        );
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn mm_order_history_row_roundtrips_through_seq_ring() {
        let (mut writer, reader) = SeqRingWriter::<MMOrderHistoryRow>::new(2).unwrap();
        writer.push(MMOrderHistoryRow {
            time: 45_000.0,
            vol: 50_000.25,
            q: 7.5,
        });
        writer.push(MMOrderHistoryRow {
            time: 45_000.5,
            vol: 51_000.5,
            q: 8.25,
        });

        let mut out = Vec::new();
        reader.copy_last(2, &mut out);

        assert_eq!(
            out,
            vec![
                MMOrderHistoryRow {
                    time: 45_000.0,
                    vol: 50_000.25,
                    q: 7.5,
                },
                MMOrderHistoryRow {
                    time: 45_000.5,
                    vol: 51_000.5,
                    q: 8.25,
                }
            ]
        );
    }

    #[test]
    fn mm_order_companion_data_roundtrips_through_seq_ring() {
        let (mut writer, reader) = SeqRingWriter::<MMOrderCompanionData>::new(2).unwrap();
        let row = MMOrderCompanionData {
            taker: [
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
            ],
            color: 0xAABB_CCDD,
        };
        writer.push(row);

        assert_eq!(reader.read_at_seq(0), Some(row));
    }

    #[test]
    fn compacts_trades_to_mini_candles_like_delphi_resize() {
        let t0 = 45_000.0;
        let rows = [
            TradeHistoryRow {
                time: t0,
                price: 100.0,
                qty: 2.0,
            },
            TradeHistoryRow {
                time: t0 + 4.0 / SECONDS_PER_DAY,
                price: 101.0,
                qty: -3.0,
            },
            TradeHistoryRow {
                time: t0 + 6.0 / SECONDS_PER_DAY,
                price: 102.0,
                qty: 4.0,
            },
        ];

        let mut out = Vec::new();
        compact_trades_to_mini_candles_like_delphi(&rows, 0.0, t0 + 1.0, &mut out);

        assert_eq!(
            out,
            vec![
                MiniCandle {
                    time: t0,
                    cnt: 2,
                    min_price: 100.0,
                    max_price: 101.0,
                    buy_vol: 200.0,
                    sell_vol: 303.0,
                },
                MiniCandle {
                    time: t0 + 6.0 / SECONDS_PER_DAY,
                    cnt: 1,
                    min_price: 102.0,
                    max_price: 102.0,
                    buy_vol: 408.0,
                    sell_vol: 0.0,
                }
            ]
        );
    }

    #[test]
    fn compact_trades_skips_split_group_not_newer_than_existing_mini() {
        let t0 = 45_000.0;
        let rows = [
            TradeHistoryRow {
                time: t0,
                price: 100.0,
                qty: 1.0,
            },
            TradeHistoryRow {
                time: t0 + 6.0 / SECONDS_PER_DAY,
                price: 101.0,
                qty: 1.0,
            },
        ];

        let mut out = Vec::new();
        compact_trades_to_mini_candles_like_delphi(
            &rows,
            t0 + 1.0 / SECONDS_PER_DAY,
            t0 + 1.0,
            &mut out,
        );

        assert_eq!(
            out,
            vec![MiniCandle {
                time: t0 + 6.0 / SECONDS_PER_DAY,
                cnt: 1,
                min_price: 101.0,
                max_price: 101.0,
                buy_vol: 101.0,
                sell_vol: 0.0,
            }]
        );
    }

    #[test]
    fn rolling_trade_volumes_maintain_one_three_five_minute_windows() {
        let now = 45_000.0;
        let mut volumes = RollingTradeVolumes::default();

        volumes.add_trade(TradeHistoryRow {
            time: now - 10.0 / SECONDS_PER_DAY,
            price: 100.0,
            qty: 2.0,
        });
        volumes.add_trade(TradeHistoryRow {
            time: now - 70.0 / SECONDS_PER_DAY,
            price: 200.0,
            qty: -3.0,
        });
        volumes.add_trade(TradeHistoryRow {
            time: now - 200.0 / SECONDS_PER_DAY,
            price: 300.0,
            qty: 4.0,
        });
        volumes.add_trade(TradeHistoryRow {
            time: now - 400.0 / SECONDS_PER_DAY,
            price: 400.0,
            qty: 5.0,
        });

        let snapshot = volumes.snapshot(now);

        assert_eq!(
            snapshot.one_minute,
            TradeVolumeTotals {
                buy_value: 200.0,
                sell_value: 0.0,
                buy_qty: 2.0,
                sell_qty: 0.0,
                trade_count: 1,
            }
        );
        assert_eq!(snapshot.three_minutes.buy_value, 200.0);
        assert_eq!(snapshot.three_minutes.sell_value, 600.0);
        assert_eq!(snapshot.three_minutes.trade_count, 2);
        assert_eq!(snapshot.five_minutes.buy_value, 1_400.0);
        assert_eq!(snapshot.five_minutes.sell_value, 600.0);
        assert_eq!(snapshot.five_minutes.trade_count, 3);
    }
}
