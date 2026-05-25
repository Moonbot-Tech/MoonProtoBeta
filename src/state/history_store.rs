//! Active-library retained history store.
//!
//! `MarketHistoryStore` is the per-market single-writer side that `StoreWorker`
//! will own. Public code receives cloneable [`SeqRingReader`] handles and reads
//! rows without taking locks on the writer path.

use crate::state::history::{
    compact_trades_to_mini_candles_like_delphi, prepare_joined_trades_for_retained_append,
    LastPricePoint, MMOrderHistoryRow, MiniCandle, RollingTradeVolumeSnapshot, RollingTradeVolumes,
    TradeHistoryRow, TradeJoinBuffer, DELPHI_SAME_TRADES_TIME_DAYS,
};
use crate::state::seq_ring::{SeqRingReader, SeqRingWriter};

const EPS_MARKET: f64 = 1e-12;
const DEFAULT_TRADE_JOIN_CAPACITY: usize = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketHistoryConfig {
    pub futures_trades_capacity: usize,
    pub spot_trades_capacity: usize,
    pub liquidation_capacity: usize,
    pub mm_orders_capacity: usize,
    pub last_price_capacity: usize,
    pub mini_candles_capacity: usize,
    pub trade_join_capacity: usize,
}

impl Default for MarketHistoryConfig {
    fn default() -> Self {
        Self {
            futures_trades_capacity: 100_000,
            spot_trades_capacity: 100_000,
            liquidation_capacity: 20_000,
            mm_orders_capacity: 20_000,
            last_price_capacity: 60_000,
            mini_candles_capacity: 20_000,
            trade_join_capacity: DEFAULT_TRADE_JOIN_CAPACITY,
        }
    }
}

#[derive(Clone, Default)]
pub struct MarketHistoryReaders {
    pub futures_trades: Option<SeqRingReader<TradeHistoryRow>>,
    pub spot_trades: Option<SeqRingReader<TradeHistoryRow>>,
    pub liquidations: Option<SeqRingReader<TradeHistoryRow>>,
    pub mm_orders: Option<SeqRingReader<MMOrderHistoryRow>>,
    pub last_prices: Option<SeqRingReader<LastPricePoint>>,
    pub mini_candles: Option<SeqRingReader<MiniCandle>>,
}

pub struct MarketHistoryStore {
    futures_trades: Option<SeqRingWriter<TradeHistoryRow>>,
    spot_trades: Option<SeqRingWriter<TradeHistoryRow>>,
    liquidations: Option<SeqRingWriter<TradeHistoryRow>>,
    mm_orders: Option<SeqRingWriter<MMOrderHistoryRow>>,
    last_prices: Option<SeqRingWriter<LastPricePoint>>,
    mini_candles: Option<SeqRingWriter<MiniCandle>>,
    readers: MarketHistoryReaders,
    futures_join: TradeJoinBuffer,
    joined_scratch: Vec<TradeHistoryRow>,
    evicted_futures_for_compaction: Vec<TradeHistoryRow>,
    mini_scratch: Vec<MiniCandle>,
    rolling_volumes: RollingTradeVolumes,
}

impl MarketHistoryStore {
    pub fn new(config: MarketHistoryConfig) -> Self {
        let (futures_trades, futures_reader) =
            optional_ring::<TradeHistoryRow>(config.futures_trades_capacity);
        let (spot_trades, spot_reader) =
            optional_ring::<TradeHistoryRow>(config.spot_trades_capacity);
        let (liquidations, liq_reader) =
            optional_ring::<TradeHistoryRow>(config.liquidation_capacity);
        let (mm_orders, mm_reader) = optional_ring::<MMOrderHistoryRow>(config.mm_orders_capacity);
        let (last_prices, last_reader) =
            optional_ring::<LastPricePoint>(config.last_price_capacity);
        let (mini_candles, mini_reader) = optional_ring::<MiniCandle>(config.mini_candles_capacity);

        Self {
            futures_trades,
            spot_trades,
            liquidations,
            mm_orders,
            last_prices,
            mini_candles,
            readers: MarketHistoryReaders {
                futures_trades: futures_reader,
                spot_trades: spot_reader,
                liquidations: liq_reader,
                mm_orders: mm_reader,
                last_prices: last_reader,
                mini_candles: mini_reader,
            },
            futures_join: TradeJoinBuffer::new(config.trade_join_capacity),
            joined_scratch: Vec::new(),
            evicted_futures_for_compaction: Vec::new(),
            mini_scratch: Vec::new(),
            rolling_volumes: RollingTradeVolumes::default(),
        }
    }

    pub fn readers(&self) -> MarketHistoryReaders {
        self.readers.clone()
    }

    pub fn rolling_volumes_snapshot(&self, now_time: f64) -> RollingTradeVolumeSnapshot {
        self.rolling_volumes.snapshot(now_time)
    }

    /// Delphi `TMarket.AddFrom` retained LastPrice row.
    ///
    /// The caller passes `p_last = (Bid + Ask) / 2` from `UpdateMarketsList`.
    /// Delphi appends only for BTC markets or base-USDT markets, only when a
    /// real bid/ask was present, and only after the caller checked `pLast`.
    pub fn append_last_price_like_delphi(
        &mut self,
        current: f64,
        real_time: f64,
        bid: f64,
        ask: f64,
        is_btc_market: bool,
        is_base_usdt_market: bool,
    ) -> Option<u64> {
        if current <= EPS_MARKET
            || (bid <= EPS_MARKET && ask <= EPS_MARKET)
            || (!is_btc_market && !is_base_usdt_market)
        {
            return None;
        }
        self.last_prices.as_mut().map(|writer| {
            writer.push(LastPricePoint {
                current: current as f32,
                real_time,
            })
        })
    }

    /// Delphi futures trade path: `AddTmpHOrder` first, retained append later
    /// through `JoinHOrders`.
    pub fn push_futures_trade_into_join_like_delphi(
        &mut self,
        row: TradeHistoryRow,
        chart_price_step: f64,
    ) {
        self.futures_join
            .push_like_delphi(row, chart_price_step, DELPHI_SAME_TRADES_TIME_DAYS);
    }

    /// Drain the temporary futures buffer into retained history, preserving the
    /// `JoinHOrders` sort/skip-tail shape before appending to the monotonic
    /// `SeqRing`.
    pub fn drain_joined_futures_like_delphi(&mut self) -> usize {
        self.futures_join.drain_into(&mut self.joined_scratch);
        let last_time = last_trade_time(self.readers.futures_trades.as_ref());
        prepare_joined_trades_for_retained_append(&mut self.joined_scratch, last_time);

        let mut appended = 0usize;
        let rows = std::mem::take(&mut self.joined_scratch);
        for row in &rows {
            self.push_retained_futures_trade(*row);
            self.rolling_volumes.add_trade(*row);
            appended += 1;
        }
        self.joined_scratch = rows;
        self.joined_scratch.clear();
        appended
    }

    pub fn append_spot_trade_like_delphi(&mut self, row: TradeHistoryRow) -> Option<u64> {
        self.spot_trades.as_mut().map(|writer| writer.push(row))
    }

    pub fn append_liquidation_like_delphi(&mut self, row: TradeHistoryRow) -> Option<u64> {
        self.liquidations.as_mut().map(|writer| writer.push(row))
    }

    pub fn append_mm_order_like_delphi(&mut self, row: MMOrderHistoryRow) -> Option<u64> {
        self.mm_orders.as_mut().map(|writer| writer.push(row))
    }

    /// Fold detailed futures rows evicted from `SeqRing` into retained
    /// `TMiniCandle` rows. StoreWorker should call this from its periodic
    /// derived-state tick.
    pub fn compact_evicted_futures_like_delphi(&mut self, now_time: f64) -> usize {
        if self.evicted_futures_for_compaction.is_empty() {
            return 0;
        }
        let last_mini_time = last_mini_time(self.readers.mini_candles.as_ref()).unwrap_or(0.0);
        compact_trades_to_mini_candles_like_delphi(
            &self.evicted_futures_for_compaction,
            last_mini_time,
            now_time,
            &mut self.mini_scratch,
        );
        let mut appended = 0usize;
        if let Some(writer) = self.mini_candles.as_mut() {
            for &candle in &self.mini_scratch {
                writer.push(candle);
                appended += 1;
            }
        }
        self.evicted_futures_for_compaction.clear();
        self.mini_scratch.clear();
        appended
    }

    pub fn pending_evicted_futures_for_compaction(&self) -> usize {
        self.evicted_futures_for_compaction.len()
    }

    fn push_retained_futures_trade(&mut self, row: TradeHistoryRow) {
        if let Some(writer) = self.futures_trades.as_mut() {
            if let Some(evicted) = writer.push_with_evicted(row).1 {
                self.evicted_futures_for_compaction.push(evicted);
            }
        }
    }
}

fn optional_ring<T>(capacity: usize) -> (Option<SeqRingWriter<T>>, Option<SeqRingReader<T>>)
where
    T: crate::state::seq_ring::SeqRingRow,
{
    if capacity == 0 {
        return (None, None);
    }
    let (writer, reader) =
        SeqRingWriter::<T>::new(capacity).expect("capacity was checked before creating SeqRing");
    (Some(writer), Some(reader))
}

fn last_trade_time(reader: Option<&SeqRingReader<TradeHistoryRow>>) -> Option<f64> {
    let reader = reader?;
    let mut out = Vec::new();
    reader.copy_last(1, &mut out);
    out.first().map(|row| row.time)
}

fn last_mini_time(reader: Option<&SeqRingReader<MiniCandle>>) -> Option<f64> {
    let reader = reader?;
    let mut out = Vec::new();
    reader.copy_last(1, &mut out);
    out.first().map(|row| row.time)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trade(time: f64, price: f32, qty: f32) -> TradeHistoryRow {
        TradeHistoryRow { time, price, qty }
    }

    #[test]
    fn last_price_appends_only_delphi_history_price_markets() {
        let mut store = MarketHistoryStore::new(MarketHistoryConfig {
            futures_trades_capacity: 0,
            spot_trades_capacity: 0,
            liquidation_capacity: 0,
            mm_orders_capacity: 0,
            last_price_capacity: 4,
            mini_candles_capacity: 0,
            trade_join_capacity: 0,
        });

        assert_eq!(
            store.append_last_price_like_delphi(10.0, 45_000.0, 9.0, 11.0, false, false),
            None
        );
        assert_eq!(
            store.append_last_price_like_delphi(0.0, 45_000.0, 9.0, 11.0, true, false),
            None
        );
        assert_eq!(
            store.append_last_price_like_delphi(10.0, 45_000.0, 0.0, 0.0, true, false),
            None
        );
        assert_eq!(
            store.append_last_price_like_delphi(10.0, 45_000.0, 9.0, 11.0, true, false),
            Some(0)
        );

        let mut out = Vec::new();
        store.readers().last_prices.unwrap().copy_last(10, &mut out);
        assert_eq!(
            out,
            vec![LastPricePoint {
                current: 10.0,
                real_time: 45_000.0
            }]
        );
    }

    #[test]
    fn futures_join_sorts_skips_tail_and_updates_volumes() {
        let base = 45_000.0;
        let sec = |s: f64| base + s / 86_400.0;
        let mut store = MarketHistoryStore::new(MarketHistoryConfig {
            futures_trades_capacity: 8,
            spot_trades_capacity: 0,
            liquidation_capacity: 0,
            mm_orders_capacity: 0,
            last_price_capacity: 0,
            mini_candles_capacity: 0,
            trade_join_capacity: 6,
        });

        store.push_futures_trade_into_join_like_delphi(trade(sec(10.0), 100.0, 1.0), 0.01);
        assert_eq!(store.drain_joined_futures_like_delphi(), 1);

        store.push_futures_trade_into_join_like_delphi(trade(sec(9.0), 90.0, 1.0), 0.01);
        store.push_futures_trade_into_join_like_delphi(trade(sec(12.0), 120.0, -2.0), 0.01);
        store.push_futures_trade_into_join_like_delphi(trade(sec(11.0), 110.0, 3.0), 0.01);
        assert_eq!(store.drain_joined_futures_like_delphi(), 2);

        let mut out = Vec::new();
        store
            .readers()
            .futures_trades
            .unwrap()
            .copy_last(8, &mut out);
        assert_eq!(
            out,
            vec![
                trade(sec(10.0), 100.0, 1.0),
                trade(sec(11.0), 110.0, 3.0),
                trade(sec(12.0), 120.0, -2.0)
            ]
        );

        let volumes = store.rolling_volumes_snapshot(sec(12.0));
        assert_eq!(volumes.five_minutes.buy_value, 430.0);
        assert_eq!(volumes.five_minutes.sell_value, 240.0);
        assert_eq!(volumes.five_minutes.trade_count, 3);
    }

    #[test]
    fn evicted_futures_compact_to_mini_candles() {
        let mut store = MarketHistoryStore::new(MarketHistoryConfig {
            futures_trades_capacity: 2,
            spot_trades_capacity: 0,
            liquidation_capacity: 0,
            mm_orders_capacity: 0,
            last_price_capacity: 0,
            mini_candles_capacity: 8,
            trade_join_capacity: 8,
        });

        for i in 0..4 {
            store.push_futures_trade_into_join_like_delphi(
                trade(10.0 + i as f64 / 86_400.0, 100.0 + i as f32, 1.0),
                0.0,
            );
        }
        assert_eq!(store.drain_joined_futures_like_delphi(), 4);
        assert_eq!(store.pending_evicted_futures_for_compaction(), 2);
        assert_eq!(store.compact_evicted_futures_like_delphi(20.0), 1);

        let mut out = Vec::new();
        store.readers().mini_candles.unwrap().copy_last(8, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].cnt, 2);
        assert_eq!(out[0].min_price, 100.0);
        assert_eq!(out[0].max_price, 101.0);
        assert_eq!(out[0].buy_vol, 201.0);
    }
}
