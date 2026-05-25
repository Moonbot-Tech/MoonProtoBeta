//! Active-library retained history store.
//!
//! `MarketHistoryStore` is the per-market single-writer side owned by
//! `MarketHistoryWorker`. Public code receives cloneable [`SeqRingReader`]
//! handles; the dense retained rings use short read/write locks, but the UDP
//! protocol receive path is not the history writer.

use std::collections::HashMap;
use std::mem::size_of;

use crate::state::history::{
    compact_trades_to_mini_candles_like_delphi, hl_address_color_like_delphi,
    prepare_joined_trades_for_retained_append, LastPricePoint, MMOrderCompanionData,
    MMOrderHistoryRow, MiniCandle, RollingTradeVolumeSnapshot, RollingTradeVolumes,
    TradeHistoryRow, TradeJoinBuffer, TradesPacketTimeShift, DELPHI_SAME_TRADES_TIME_DAYS,
};
use crate::state::seq_ring::{SeqRingReader, SeqRingWriter};

const EPS_MARKET: f64 = 1e-12;
const DEFAULT_TRADE_JOIN_CAPACITY: usize = 1_000;
const GIB: usize = 1024 * 1024 * 1024;
const TRADE_SLOT_BYTES: usize = size_of::<TradeHistoryRow>();
const MM_ORDER_SLOT_BYTES: usize = size_of::<MMOrderHistoryRow>();
const MM_COMPANION_SLOT_BYTES: usize = size_of::<MMOrderCompanionData>();
const LAST_PRICE_SLOT_BYTES: usize = size_of::<LastPricePoint>();
const MINI_CANDLE_SLOT_BYTES: usize = size_of::<MiniCandle>();
const TRADE_JOIN_ROW_BYTES: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketHistoryConfig {
    pub futures_trades_capacity: usize,
    pub spot_trades_capacity: usize,
    pub liquidation_capacity: usize,
    pub mm_orders_capacity: usize,
    pub mm_order_companion_capacity: usize,
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
            mm_order_companion_capacity: 20_000,
            last_price_capacity: 60_000,
            mini_candles_capacity: 20_000,
            trade_join_capacity: DEFAULT_TRADE_JOIN_CAPACITY,
        }
    }
}

impl MarketHistoryConfig {
    pub fn from_total_memory_bytes(total_memory_bytes: usize, market_count: usize) -> Self {
        let market_count = market_count.max(1);
        let budget = Self::history_budget_bytes(total_memory_bytes);
        let per_market_budget = budget / market_count;

        let futures_trades_capacity =
            capacity_from_share(per_market_budget, 35, 100, TRADE_SLOT_BYTES, 200_000);
        let spot_trades_capacity =
            capacity_from_share(per_market_budget, 20, 100, TRADE_SLOT_BYTES, 150_000);
        let liquidation_capacity =
            capacity_from_share(per_market_budget, 8, 100, TRADE_SLOT_BYTES, 50_000);
        let mm_orders_capacity =
            capacity_from_share(per_market_budget, 8, 100, MM_ORDER_SLOT_BYTES, 50_000);
        let mm_order_companion_capacity =
            capacity_from_share(per_market_budget, 8, 100, MM_COMPANION_SLOT_BYTES, 50_000);
        let last_price_capacity =
            capacity_from_share(per_market_budget, 10, 100, LAST_PRICE_SLOT_BYTES, 80_000);
        let mini_candles_capacity =
            capacity_from_share(per_market_budget, 8, 100, MINI_CANDLE_SLOT_BYTES, 50_000);
        let trade_join_capacity = futures_trades_capacity
            .min(DEFAULT_TRADE_JOIN_CAPACITY)
            .max(usize::from(futures_trades_capacity > 0) * 8);

        Self {
            futures_trades_capacity,
            spot_trades_capacity,
            liquidation_capacity,
            mm_orders_capacity,
            mm_order_companion_capacity,
            last_price_capacity,
            mini_candles_capacity,
            trade_join_capacity,
        }
    }

    pub fn history_budget_bytes(total_memory_bytes: usize) -> usize {
        if total_memory_bytes < 8 * GIB {
            total_memory_bytes / 4
        } else {
            total_memory_bytes / 5
        }
    }

    pub fn estimated_bytes_per_market(&self) -> usize {
        self.futures_trades_capacity * TRADE_SLOT_BYTES
            + self.spot_trades_capacity * TRADE_SLOT_BYTES
            + self.liquidation_capacity * TRADE_SLOT_BYTES
            + self.mm_orders_capacity * MM_ORDER_SLOT_BYTES
            + self.mm_order_companion_capacity * MM_COMPANION_SLOT_BYTES
            + self.last_price_capacity * LAST_PRICE_SLOT_BYTES
            + self.mini_candles_capacity * MINI_CANDLE_SLOT_BYTES
            + self.trade_join_capacity * TRADE_JOIN_ROW_BYTES
    }
}

fn capacity_from_share(
    budget: usize,
    numerator: usize,
    denominator: usize,
    row_bytes: usize,
    max_capacity: usize,
) -> usize {
    if budget == 0 || row_bytes == 0 || denominator == 0 {
        return 0;
    }
    ((budget / denominator) * numerator / row_bytes).min(max_capacity)
}

#[derive(Clone, Default)]
pub struct MarketHistoryReaders {
    pub futures_trades: Option<SeqRingReader<TradeHistoryRow>>,
    pub spot_trades: Option<SeqRingReader<TradeHistoryRow>>,
    pub liquidations: Option<SeqRingReader<TradeHistoryRow>>,
    pub mm_orders: Option<SeqRingReader<MMOrderHistoryRow>>,
    pub mm_order_companion: Option<SeqRingReader<MMOrderCompanionData>>,
    pub last_prices: Option<SeqRingReader<LastPricePoint>>,
    pub mini_candles: Option<SeqRingReader<MiniCandle>>,
}

#[derive(Default)]
pub struct MarketHistoryRegistry {
    default_config: MarketHistoryConfig,
    stores: HashMap<String, MarketHistoryStore>,
}

impl MarketHistoryRegistry {
    pub fn new(default_config: MarketHistoryConfig) -> Self {
        Self {
            default_config,
            stores: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.stores.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stores.is_empty()
    }

    pub fn contains_market(&self, market_name: &str) -> bool {
        self.stores.contains_key(market_name)
    }

    pub fn get(&self, market_name: &str) -> Option<&MarketHistoryStore> {
        self.stores.get(market_name)
    }

    pub fn get_mut(&mut self, market_name: &str) -> Option<&mut MarketHistoryStore> {
        self.stores.get_mut(market_name)
    }

    pub fn ensure_market(&mut self, market_name: &str) -> &mut MarketHistoryStore {
        self.stores
            .entry(market_name.to_string())
            .or_insert_with(|| MarketHistoryStore::new(self.default_config))
    }

    pub fn readers(&self, market_name: &str) -> Option<MarketHistoryReaders> {
        self.stores
            .get(market_name)
            .map(MarketHistoryStore::readers)
    }

    pub fn drain_joined_futures_like_delphi(&mut self) -> usize {
        self.stores
            .values_mut()
            .map(MarketHistoryStore::drain_joined_futures_like_delphi)
            .sum()
    }

    pub fn compact_evicted_futures_like_delphi(&mut self, now_time: f64) -> usize {
        self.stores
            .values_mut()
            .map(|store| store.compact_evicted_futures_like_delphi(now_time))
            .sum()
    }
}

pub struct MarketHistoryStore {
    futures_trades: Option<SeqRingWriter<TradeHistoryRow>>,
    spot_trades: Option<SeqRingWriter<TradeHistoryRow>>,
    liquidations: Option<SeqRingWriter<TradeHistoryRow>>,
    mm_orders: Option<SeqRingWriter<MMOrderHistoryRow>>,
    mm_order_companion: Option<SeqRingWriter<MMOrderCompanionData>>,
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
        let (mm_order_companion, mm_companion_reader) =
            optional_ring::<MMOrderCompanionData>(config.mm_order_companion_capacity);
        let (last_prices, last_reader) =
            optional_ring::<LastPricePoint>(config.last_price_capacity);
        let (mini_candles, mini_reader) = optional_ring::<MiniCandle>(config.mini_candles_capacity);

        Self {
            futures_trades,
            spot_trades,
            liquidations,
            mm_orders,
            mm_order_companion,
            last_prices,
            mini_candles,
            readers: MarketHistoryReaders {
                futures_trades: futures_reader,
                spot_trades: spot_reader,
                liquidations: liq_reader,
                mm_orders: mm_reader,
                mm_order_companion: mm_companion_reader,
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

    pub fn push_futures_stream_trade_like_delphi(
        &mut self,
        base_time: f64,
        time_delta_ms: i16,
        now_time: f64,
        price: f32,
        qty: f32,
        chart_price_step: f64,
        time_shift: &mut TradesPacketTimeShift,
    ) -> f64 {
        let time = time_shift.apply_like_delphi(base_time, time_delta_ms, now_time);
        self.push_futures_trade_into_join_like_delphi(
            TradeHistoryRow { time, price, qty },
            chart_price_step,
        );
        time
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

    pub fn append_spot_stream_trade_like_delphi(
        &mut self,
        base_time: f64,
        time_delta_ms: i16,
        now_time: f64,
        price: f32,
        qty: f32,
        time_shift: &mut TradesPacketTimeShift,
    ) -> (f64, Option<u64>) {
        let time = time_shift.apply_like_delphi(base_time, time_delta_ms, now_time);
        let seq = self.append_spot_trade_like_delphi(TradeHistoryRow { time, price, qty });
        (time, seq)
    }

    pub fn append_liquidation_like_delphi(&mut self, row: TradeHistoryRow) -> Option<u64> {
        self.liquidations.as_mut().map(|writer| writer.push(row))
    }

    pub fn append_liquidation_stream_like_delphi(
        &mut self,
        base_time: f64,
        time_delta_ms: i16,
        now_time: f64,
        price: f32,
        qty: f32,
        time_shift: &mut TradesPacketTimeShift,
    ) -> (f64, Option<u64>) {
        let time = time_shift.apply_like_delphi(base_time, time_delta_ms, now_time);
        let seq = self.append_liquidation_like_delphi(TradeHistoryRow { time, price, qty });
        (time, seq)
    }

    pub fn append_mm_order_like_delphi(&mut self, row: MMOrderHistoryRow) -> Option<u64> {
        self.append_mm_order_with_companion_like_delphi(row, None)
    }

    pub fn append_mm_order_with_companion_like_delphi(
        &mut self,
        row: MMOrderHistoryRow,
        companion: Option<MMOrderCompanionData>,
    ) -> Option<u64> {
        let seq = self.mm_orders.as_mut().map(|writer| writer.push(row))?;
        if let Some(writer) = self.mm_order_companion.as_mut() {
            writer.push(companion.unwrap_or_default());
        }
        Some(seq)
    }

    pub fn append_mm_stream_order_like_delphi(
        &mut self,
        base_time: f64,
        time_delta_ms: i16,
        now_time: f64,
        vol: f32,
        q: f32,
        taker: Option<[u8; 20]>,
        time_shift: &mut TradesPacketTimeShift,
    ) -> (f64, Option<u64>) {
        let time = time_shift.apply_like_delphi(base_time, time_delta_ms, now_time);
        let companion = taker.map(|taker| MMOrderCompanionData {
            taker,
            color: hl_address_color_like_delphi(taker),
        });
        let seq = self.append_mm_order_with_companion_like_delphi(
            MMOrderHistoryRow {
                time,
                vol: f64::from(vol),
                q: f64::from(q),
            },
            companion,
        );
        (time, seq)
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
    fn memory_sized_config_stays_inside_budget() {
        let total = 16 * GIB;
        let market_count = 1_000;
        let cfg = MarketHistoryConfig::from_total_memory_bytes(total, market_count);
        let total_estimated = cfg.estimated_bytes_per_market() * market_count;

        assert!(cfg.futures_trades_capacity > cfg.spot_trades_capacity);
        assert!(cfg.trade_join_capacity <= DEFAULT_TRADE_JOIN_CAPACITY);
        assert!(
            total_estimated <= MarketHistoryConfig::history_budget_bytes(total),
            "history defaults should fit the configured memory budget"
        );
    }

    #[test]
    fn small_memory_config_uses_larger_fraction() {
        let small = 4 * GIB;
        let large = 16 * GIB;
        assert_eq!(MarketHistoryConfig::history_budget_bytes(small), small / 4);
        assert_eq!(MarketHistoryConfig::history_budget_bytes(large), large / 5);
    }

    #[test]
    fn registry_allocates_market_history_on_demand() {
        let mut registry = MarketHistoryRegistry::new(MarketHistoryConfig {
            futures_trades_capacity: 2,
            spot_trades_capacity: 0,
            liquidation_capacity: 0,
            mm_orders_capacity: 0,
            mm_order_companion_capacity: 0,
            last_price_capacity: 2,
            mini_candles_capacity: 0,
            trade_join_capacity: 4,
        });

        assert!(registry.is_empty());
        assert!(registry.readers("BTCUSDT").is_none());

        registry
            .ensure_market("BTCUSDT")
            .append_last_price_like_delphi(100.0, 45_000.0, 99.0, 101.0, true, false);
        registry
            .ensure_market("ETHUSDT")
            .push_futures_trade_into_join_like_delphi(trade(45_000.0, 10.0, 1.0), 0.01);

        assert_eq!(registry.len(), 2);
        assert!(registry.contains_market("BTCUSDT"));
        assert!(registry.contains_market("ETHUSDT"));

        let mut last_prices = Vec::new();
        registry
            .readers("BTCUSDT")
            .unwrap()
            .last_prices
            .unwrap()
            .copy_last(10, &mut last_prices);
        assert_eq!(
            last_prices,
            vec![LastPricePoint {
                current: 100.0,
                real_time: 45_000.0,
            }]
        );
    }

    #[test]
    fn last_price_appends_only_delphi_history_price_markets() {
        let mut store = MarketHistoryStore::new(MarketHistoryConfig {
            futures_trades_capacity: 0,
            spot_trades_capacity: 0,
            liquidation_capacity: 0,
            mm_orders_capacity: 0,
            mm_order_companion_capacity: 0,
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
            mm_order_companion_capacity: 0,
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
    fn stream_append_helpers_share_delphi_packet_time_shift() {
        let base = 45_000.0;
        let now = base + 2.0 / 24.0 + 3.0 / 86_400.0;
        let mut shift = TradesPacketTimeShift::new();
        let mut store = MarketHistoryStore::new(MarketHistoryConfig {
            futures_trades_capacity: 8,
            spot_trades_capacity: 8,
            liquidation_capacity: 8,
            mm_orders_capacity: 8,
            mm_order_companion_capacity: 8,
            last_price_capacity: 0,
            mini_candles_capacity: 0,
            trade_join_capacity: 8,
        });

        let fut_time = store
            .push_futures_stream_trade_like_delphi(base, 100, now, 100.0, 1.0, 0.01, &mut shift);
        let taker = [7u8; 20];
        let (mm_time, mm_seq) = store.append_mm_stream_order_like_delphi(
            base,
            200,
            base - 10.0,
            5.0,
            -2.0,
            Some(taker),
            &mut shift,
        );
        let (spot_time, spot_seq) = store.append_spot_stream_trade_like_delphi(
            base,
            -300,
            base - 10.0,
            90.0,
            -1.0,
            &mut shift,
        );
        assert_eq!(store.drain_joined_futures_like_delphi(), 1);

        assert_eq!(shift.shift_days(), Some(2.0 / 24.0));
        assert_eq!(fut_time, base + 100.0 / 86_400_000.0 + 2.0 / 24.0);
        assert_eq!(mm_time, base + 200.0 / 86_400_000.0 + 2.0 / 24.0);
        assert_eq!(spot_time, base - 300.0 / 86_400_000.0 + 2.0 / 24.0);
        assert_eq!(mm_seq, Some(0));
        assert_eq!(spot_seq, Some(0));

        let readers = store.readers();
        let mut trades = Vec::new();
        readers.futures_trades.unwrap().copy_last(1, &mut trades);
        assert_eq!(trades[0].time, fut_time);

        let mut mm_orders = Vec::new();
        readers.mm_orders.unwrap().copy_last(1, &mut mm_orders);
        assert_eq!(
            mm_orders,
            vec![MMOrderHistoryRow {
                time: mm_time,
                vol: 5.0,
                q: -2.0,
            }]
        );

        let mut companions = Vec::new();
        readers
            .mm_order_companion
            .unwrap()
            .copy_last(1, &mut companions);
        assert_eq!(
            companions,
            vec![MMOrderCompanionData {
                taker,
                color: hl_address_color_like_delphi(taker),
            }]
        );
    }

    #[test]
    fn evicted_futures_compact_to_mini_candles() {
        let mut store = MarketHistoryStore::new(MarketHistoryConfig {
            futures_trades_capacity: 2,
            spot_trades_capacity: 0,
            liquidation_capacity: 0,
            mm_orders_capacity: 0,
            mm_order_companion_capacity: 0,
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
