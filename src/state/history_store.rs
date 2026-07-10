//! Active-library retained history store.
//!
//! `MarketHistoryStore` is the per-market single-writer side owned by
//! `MarketHistoryWorker`. Public code receives cloneable [`SeqRingReader`]
//! handles; the dense retained rings use short read/write locks, but the UDP
//! protocol receive path is not the history writer.

use std::sync::Arc;

use crate::state::eps::EpsProfile;
use crate::state::history::{
    compact_trades_to_mini_candles, hl_address_color, Candle5mRow, DerivedDeltaSnapshot,
    LastPricePoint, MMOrderCompanionData, MMOrderHistoryRow, MarkPricePoint, MarketDerivedSnapshot,
    MiniCandle, RollingPriceRanges, RollingTradeVolumeSnapshot, RollingTradeVolumes,
    TradeHistoryRow, TradesPacketTimeShift,
};
use crate::state::seq_ring::{SeqRingReader, SeqRingWriter};
#[cfg(any(test, feature = "diagnostics"))]
use crate::state::seq_ring::{SeqRingRow, SeqRingTimedRow};
use crate::MoonTime;
use parking_lot::RwLock;

const FIVE_MINUTES_MS: i64 = 5 * 60 * 1_000;
const STALE_CANDLES_SNAPSHOT_MS: i64 = 11 * 60 * 1_000;

mod config;
mod derived;
mod registry;

pub(crate) use self::config::TradeStorageScope;
#[cfg(test)]
use self::config::GIB;
pub use self::config::{MarketHistoryConfig, MarketHistorySizing};
#[cfg(test)]
use self::derived::combine_deltas;
pub(crate) use self::registry::MarketHistoryRegistry;

type SharedMarketName = Arc<str>;

#[derive(Clone, Default)]
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

#[derive(Clone, Default)]
pub(crate) struct MarketHistoryReadHandle {
    inner: Arc<RwLock<MarketHistoryReadState>>,
}

#[derive(Clone, Default)]
struct MarketHistoryReadState {
    readers: MarketHistoryReaders,
    rolling_volumes: RollingTradeVolumes,
    derived: MarketDerivedSnapshot,
}

impl MarketHistoryReadHandle {
    fn new(readers: MarketHistoryReaders) -> Self {
        Self {
            inner: Arc::new(RwLock::new(MarketHistoryReadState {
                readers,
                ..MarketHistoryReadState::default()
            })),
        }
    }

    pub(crate) fn readers(&self) -> MarketHistoryReaders {
        self.inner.read().readers.clone()
    }

    pub(crate) fn rolling_volumes(&self, now_time: MoonTime) -> RollingTradeVolumeSnapshot {
        self.inner.read().rolling_volumes.snapshot(now_time)
    }

    pub(crate) fn derived_snapshot(&self) -> MarketDerivedSnapshot {
        self.inner.read().derived
    }

    fn publish(&self, rolling_volumes: &RollingTradeVolumes, derived: MarketDerivedSnapshot) {
        let mut state = self.inner.write();
        state.rolling_volumes = rolling_volumes.clone();
        state.derived = derived;
    }

    fn publish_derived(&self, derived: MarketDerivedSnapshot) {
        self.inner.write().derived = derived;
    }
}

pub(crate) struct MarketHistoryStore {
    futures_trades: Option<SeqRingWriter<TradeHistoryRow>>,
    spot_trades: Option<SeqRingWriter<TradeHistoryRow>>,
    liquidations: Option<SeqRingWriter<TradeHistoryRow>>,
    mm_orders: Option<SeqRingWriter<MMOrderHistoryRow>>,
    mm_order_companion: Option<SeqRingWriter<MMOrderCompanionData>>,
    last_prices: Option<SeqRingWriter<LastPricePoint>>,
    mark_prices: Option<SeqRingWriter<MarkPricePoint>>,
    mini_candles: Option<SeqRingWriter<MiniCandle>>,
    candles_5m: Option<SeqRingWriter<Candle5mRow>>,
    readers: MarketHistoryReaders,
    read_handle: MarketHistoryReadHandle,
    evicted_futures_for_compaction: Vec<TradeHistoryRow>,
    mini_scratch: Vec<MiniCandle>,
    rolling_volumes: RollingTradeVolumes,
    rolling_volumes_publish_dirty: bool,
    rolling_last_price_ranges: RollingPriceRanges,
    /// In-progress 5m candle: a separate accumulator, NOT stored in the
    /// `candles_5m` ring (which holds only sealed, end-stamped candles).
    current_candle: Option<Candle5mRow>,
    trade_analytics_dirty: bool,
    last_price_analytics_dirty: bool,
    short_analytics_bucket: Option<i64>,
    sealed_candle_analytics_dirty: bool,
    current_candle_analytics_dirty: bool,
    sealed_candle_analytics_bucket: Option<i64>,
    sealed_candle_derived: Option<derived::CandleDerivedAccumulator>,
    derived: MarketDerivedSnapshot,
    eps_profile: EpsProfile,
    deltas_by_trades: bool,
    #[cfg(test)]
    last_refresh_work: derived::DerivedRefreshWork,
}

impl MarketHistoryStore {
    #[cfg(test)]
    pub(crate) fn new(config: MarketHistoryConfig) -> Self {
        Self::new_with_eps_profile(config, EpsProfile::default())
    }

    pub(crate) fn new_with_eps_profile(
        config: MarketHistoryConfig,
        eps_profile: EpsProfile,
    ) -> Self {
        let (futures_trades, futures_reader) =
            optional_ring::<TradeHistoryRow>(config.futures_trades_capacity);
        let (spot_trades, spot_reader) =
            optional_ring::<TradeHistoryRow>(config.spot_trades_capacity);
        let (liquidations, liq_reader) =
            optional_ring::<TradeHistoryRow>(config.liquidation_capacity);
        let (mm_orders, mm_reader) = optional_ring::<MMOrderHistoryRow>(config.mm_orders_capacity);
        // Companion ring shares the MM-order capacity so the two rings push and
        // evict in lockstep (Delphi single-`FSize` `TStreamableRingBuffer<T, T2>`).
        let (mm_order_companion, mm_companion_reader) =
            optional_ring::<MMOrderCompanionData>(config.mm_orders_capacity);
        let (last_prices, last_reader) =
            optional_ring::<LastPricePoint>(config.last_price_capacity);
        let (mark_prices, mark_reader) =
            optional_ring::<MarkPricePoint>(config.last_price_capacity);
        let (mini_candles, mini_reader) = optional_ring::<MiniCandle>(config.mini_candles_capacity);
        let (candles_5m, candles_reader) = optional_ring::<Candle5mRow>(config.candles_5m_capacity);

        let readers = MarketHistoryReaders {
            futures_trades: futures_reader,
            spot_trades: spot_reader,
            liquidations: liq_reader,
            mm_orders: mm_reader,
            mm_order_companion: mm_companion_reader,
            last_prices: last_reader,
            mark_prices: mark_reader,
            mini_candles: mini_reader,
            candles_5m: candles_reader,
        };
        let read_handle = MarketHistoryReadHandle::new(readers.clone());

        Self {
            futures_trades,
            spot_trades,
            liquidations,
            mm_orders,
            mm_order_companion,
            last_prices,
            mark_prices,
            mini_candles,
            candles_5m,
            readers,
            read_handle,
            evicted_futures_for_compaction: Vec::new(),
            mini_scratch: Vec::new(),
            rolling_volumes: RollingTradeVolumes::default(),
            rolling_volumes_publish_dirty: false,
            rolling_last_price_ranges: RollingPriceRanges::default(),
            current_candle: None,
            trade_analytics_dirty: false,
            last_price_analytics_dirty: false,
            short_analytics_bucket: None,
            sealed_candle_analytics_dirty: false,
            current_candle_analytics_dirty: false,
            sealed_candle_analytics_bucket: None,
            sealed_candle_derived: None,
            derived: MarketDerivedSnapshot::default(),
            eps_profile,
            deltas_by_trades: false,
            #[cfg(test)]
            last_refresh_work: derived::DerivedRefreshWork::default(),
        }
    }

    pub(crate) fn set_eps_profile(&mut self, eps_profile: EpsProfile) {
        if self.eps_profile == eps_profile {
            return;
        }
        self.eps_profile = eps_profile;
        self.last_price_analytics_dirty = true;
        self.sealed_candle_analytics_dirty = true;
        self.current_candle_analytics_dirty = true;
    }

    pub(crate) fn set_deltas_by_trades(&mut self, enabled: bool) {
        if self.deltas_by_trades == enabled {
            return;
        }
        self.deltas_by_trades = enabled;
        self.trade_analytics_dirty = true;
        self.derived.trade_deltas = DerivedDeltaSnapshot::default();
    }

    #[cfg(test)]
    pub(crate) fn readers(&self) -> MarketHistoryReaders {
        self.readers.clone()
    }

    pub(crate) fn read_handle(&self) -> MarketHistoryReadHandle {
        self.read_handle.clone()
    }

    #[cfg(test)]
    pub(crate) fn rolling_volumes_snapshot(
        &self,
        now_time: MoonTime,
    ) -> RollingTradeVolumeSnapshot {
        self.rolling_volumes.snapshot(now_time)
    }

    #[cfg(test)]
    pub(crate) fn derived_snapshot(&self) -> MarketDerivedSnapshot {
        self.derived
    }

    #[cfg(any(test, feature = "diagnostics"))]
    pub(crate) fn diag_fill_to_capacity(&mut self, now_time: MoonTime, span_ms: i64) {
        let now_ms = now_time.unix_millis();
        let span_ms = span_ms.max(1);
        let price_anchor = self.diag_price_anchor();
        let futures_rows = diag_fill_timed_ring(
            &mut self.futures_trades,
            self.readers.futures_trades.as_ref(),
            now_ms,
            span_ms,
            |idx, count, time| synthetic_trade_row(idx, count, time, price_anchor),
        );
        diag_fill_timed_ring(
            &mut self.spot_trades,
            self.readers.spot_trades.as_ref(),
            now_ms,
            span_ms,
            |idx, count, time| synthetic_spot_trade_row(idx, count, time, price_anchor),
        );
        diag_fill_timed_ring(
            &mut self.liquidations,
            self.readers.liquidations.as_ref(),
            now_ms,
            span_ms,
            |idx, count, time| synthetic_liquidation_row(idx, count, time, price_anchor),
        );
        self.diag_fill_mm_orders_to_capacity(now_ms, span_ms, price_anchor);
        let last_price_rows = diag_fill_timed_ring(
            &mut self.last_prices,
            self.readers.last_prices.as_ref(),
            now_ms,
            span_ms,
            |idx, count, time| synthetic_last_price_point(idx, count, time, price_anchor),
        );
        diag_fill_timed_ring(
            &mut self.mark_prices,
            self.readers.mark_prices.as_ref(),
            now_ms,
            span_ms,
            |idx, count, time| synthetic_mark_price_point(idx, count, time, price_anchor),
        );
        diag_fill_timed_ring(
            &mut self.mini_candles,
            self.readers.mini_candles.as_ref(),
            now_ms,
            span_ms,
            |idx, count, time| synthetic_mini_candle(idx, count, time, price_anchor),
        );
        diag_fill_timed_ring(
            &mut self.candles_5m,
            self.readers.candles_5m.as_ref(),
            now_ms,
            span_ms,
            |idx, count, time| synthetic_candle_5m(idx, count, time, price_anchor),
        );

        if let Some(rows) = futures_rows {
            self.rolling_volumes = RollingTradeVolumes::default();
            for row in rows {
                self.rolling_volumes.add_trade(row);
            }
            self.rolling_volumes_publish_dirty = true;
        }
        if let Some(rows) = last_price_rows {
            self.rolling_last_price_ranges = RollingPriceRanges::default();
            for row in rows {
                self.rolling_last_price_ranges
                    .add_price(row.time, row.current);
            }
        }
        self.evicted_futures_for_compaction.clear();
        self.trade_analytics_dirty = true;
        self.last_price_analytics_dirty = true;
        self.sealed_candle_analytics_dirty = true;
        self.current_candle_analytics_dirty = true;
        self.refresh_derived_analytics(now_time);
    }

    pub(crate) fn replace_candles_5m_from_snapshot(
        &mut self,
        candles: &[Candle5mRow],
        now_time: MoonTime,
    ) {
        // Snapshot = sealed candles only (Delphi `Deep5m`; `StoreCandlesToZip`
        // serializes Deep5m, and Recalc5mCandle writes there only on seal — the
        // server does not send the in-progress `FCandle`). Push them all into
        // the ring as-is (end-stamped). Do NOT continue the last one as
        // in-progress — its period is closed; the client accumulates the current
        // period itself from the trade stream into a separate `current_candle`
        // (Delphi FCandle).
        let last_time = candles.last().map(|candle| candle.time).unwrap_or_default();
        let candles = if candles_snapshot_is_stale(last_time, now_time) {
            &[][..]
        } else {
            candles
        };
        self.current_candle = None;
        self.sealed_candle_analytics_dirty = true;
        self.current_candle_analytics_dirty = true;
        if let Some(writer) = self.candles_5m.as_mut() {
            writer.clear();
            writer.push_batch(candles);
        }
        self.refresh_derived_analytics(if now_time != MoonTime::ZERO {
            now_time
        } else {
            last_time
        });
    }

    /// Retained LastPrice row from market-price updates.
    ///
    /// The caller passes `p_last = (Bid + Ask) / 2` from `UpdateMarketsList`.
    /// Active Lib appends only for BTC markets or base-USDT markets, only when a
    /// real bid/ask was present, and only after the caller checked `pLast`.
    // parity: MoonBot MarketsU.pas:TMarket.AddFrom
    pub(crate) fn append_last_price(
        &mut self,
        current: f64,
        real_time: MoonTime,
        bid: f64,
        ask: f64,
        is_btc_market: bool,
        is_base_usdt_market: bool,
    ) -> Option<u64> {
        // F2 (sverka #14): market-price comparisons use Delphi `_epsM`, not the
        // generic `_eps` (Unit1.pas:4715-4780 profile table). pLast/bid/ask are
        // market prices.
        let eps_m = self.eps_profile.eps_m;
        if current <= eps_m
            || (bid <= eps_m && ask <= eps_m)
            || (!is_btc_market && !is_base_usdt_market)
        {
            return None;
        }
        let row = LastPricePoint {
            current: current as f32,
            time: real_time,
        };
        let seq = self.last_prices.as_mut()?.push(row);
        self.rolling_last_price_ranges
            .add_price(row.time, row.current);
        self.last_price_analytics_dirty = true;
        Some(seq)
    }

    pub(crate) fn append_mark_price(
        &mut self,
        current: f64,
        real_time: MoonTime,
        mark_price_found: bool,
    ) -> Option<u64> {
        // F2 (sverka #14): mark price is a market price -> Delphi `_epsM`.
        if !mark_price_found || current <= self.eps_profile.eps_m {
            return None;
        }
        self.mark_prices.as_mut().map(|writer| {
            writer.push(MarkPricePoint {
                current: current as f32,
                time: real_time,
            })
        })
    }

    // parity: MoonBot MoonProtoEngine.pas:ProcessTradesStream (futures trade -> m.FuturesTrades)
    pub(crate) fn append_futures_trade(&mut self, row: TradeHistoryRow) -> Option<u64> {
        let seq = self.push_retained_futures_trade(row)?;
        let quantity = row.quantity();
        self.rolling_volumes.add_trade_with_quantity(row, quantity);
        self.rolling_volumes_publish_dirty = true;
        self.trade_analytics_dirty = true;
        self.update_current_candle_from_trade(row, row.price * quantity);
        Some(seq)
    }

    // parity: MoonBot MoonProtoEngine.pas:ProcessTradesStream (futures section -> m.FuturesTrades)
    pub(crate) fn append_futures_stream_trade(
        &mut self,
        base_time: f64,
        time_delta_ms: i16,
        now_time: f64,
        price: f32,
        qty: f32,
        time_shift: &mut TradesPacketTimeShift,
    ) -> MoonTime {
        let time = time_shift.shifted_time(base_time, time_delta_ms, now_time);
        self.append_futures_trade(TradeHistoryRow { time, price, qty });
        time
    }

    // parity: MoonBot MoonProtoEngine.pas:ProcessTradesStream (spot trade -> m.SpotTrades)
    pub(crate) fn append_spot_trade(&mut self, row: TradeHistoryRow) -> Option<u64> {
        self.spot_trades.as_mut().map(|writer| writer.push(row))
    }

    // parity: MoonBot MoonProtoEngine.pas:ProcessTradesStream (spot section -> m.SpotTrades)
    pub(crate) fn append_spot_stream_trade(
        &mut self,
        base_time: f64,
        time_delta_ms: i16,
        now_time: f64,
        price: f32,
        qty: f32,
        time_shift: &mut TradesPacketTimeShift,
    ) -> (MoonTime, Option<u64>) {
        let time = time_shift.shifted_time(base_time, time_delta_ms, now_time);
        let seq = self.append_spot_trade(TradeHistoryRow { time, price, qty });
        (time, seq)
    }

    // parity: MoonBot MoonProtoEngine.pas:ProcessTradesStream (liq section -> m.LiqOrders)
    pub(crate) fn append_liquidation(&mut self, row: TradeHistoryRow) -> Option<u64> {
        self.liquidations.as_mut().map(|writer| writer.push(row))
    }

    // parity: MoonBot MoonProtoEngine.pas:ProcessTradesStream (liq section -> m.LiqOrders)
    pub(crate) fn append_liquidation_stream(
        &mut self,
        base_time: f64,
        time_delta_ms: i16,
        now_time: f64,
        price: f32,
        qty: f32,
        time_shift: &mut TradesPacketTimeShift,
    ) -> (MoonTime, Option<u64>) {
        let time = time_shift.shifted_time(base_time, time_delta_ms, now_time);
        let seq = self.append_liquidation(TradeHistoryRow { time, price, qty });
        (time, seq)
    }

    // parity: MoonBot MoonProtoEngine.pas:ProcessTradesStream (m.MMOrders.Add(MMOrder, moData))
    pub(crate) fn append_mm_order_with_companion(
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

    // parity: MoonBot MoonProtoEngine.pas:ProcessTradesStream (MMOrders section + HLAddressColor)
    pub(crate) fn append_mm_stream_order(
        &mut self,
        base_time: f64,
        time_delta_ms: i16,
        now_time: f64,
        vol: f32,
        q: f32,
        taker: Option<[u8; 20]>,
        time_shift: &mut TradesPacketTimeShift,
    ) -> (MoonTime, Option<u64>) {
        let time = time_shift.shifted_time(base_time, time_delta_ms, now_time);
        let companion = taker.map(|taker| MMOrderCompanionData {
            taker,
            color: hl_address_color(taker),
        });
        let seq = self.append_mm_order_with_companion(
            MMOrderHistoryRow {
                time,
                volume: f64::from(vol),
                q: f64::from(q),
            },
            companion,
        );
        (time, seq)
    }

    /// Fold detailed futures rows evicted from `SeqRing` into retained
    /// `TMiniCandle` rows. StoreWorker should call this from its periodic
    /// derived-state tick.
    // parity: MoonBot MarketsU.pas:TMarket.ResizeOrdersHistory
    pub(crate) fn compact_evicted_futures(&mut self, now_time: MoonTime) -> usize {
        if self.evicted_futures_for_compaction.is_empty() {
            return 0;
        }
        let last_mini_time =
            last_mini_time(self.readers.mini_candles.as_ref()).unwrap_or(MoonTime::MIN);
        compact_trades_to_mini_candles(
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

    #[cfg(test)]
    pub(crate) fn pending_evicted_futures_for_compaction(&self) -> usize {
        self.evicted_futures_for_compaction.len()
    }

    fn push_retained_futures_trade(&mut self, row: TradeHistoryRow) -> Option<u64> {
        if let Some(writer) = self.futures_trades.as_mut() {
            let (seq, evicted) = writer.push_with_evicted(row);
            if let Some(evicted) = evicted {
                self.evicted_futures_for_compaction.push(evicted);
            }
            Some(seq)
        } else {
            None
        }
    }

    #[cfg(any(test, feature = "diagnostics"))]
    fn diag_price_anchor(&self) -> f32 {
        diag_last_reader_price(self.readers.last_prices.as_ref(), |row| row.current)
            .or_else(|| {
                diag_last_reader_price(self.readers.mark_prices.as_ref(), |row| row.current)
            })
            .or_else(|| {
                diag_last_reader_price(self.readers.futures_trades.as_ref(), |row| row.price)
            })
            .or_else(|| diag_last_reader_price(self.readers.spot_trades.as_ref(), |row| row.price))
            .or_else(|| {
                self.current_candle
                    .and_then(|row| valid_diag_price(row.close))
            })
            .or_else(|| diag_last_reader_price(self.readers.candles_5m.as_ref(), |row| row.close))
            .or_else(|| {
                diag_last_reader_price(self.readers.mini_candles.as_ref(), |row| {
                    (row.min_price + row.max_price) * 0.5
                })
            })
            .unwrap_or(100.0)
    }

    #[cfg(any(test, feature = "diagnostics"))]
    fn diag_fill_mm_orders_to_capacity(&mut self, now_ms: i64, span_ms: i64, price_base: f32) {
        let (Some(writer), Some(reader)) =
            (self.mm_orders.as_mut(), self.readers.mm_orders.as_ref())
        else {
            return;
        };
        let capacity = reader.capacity();
        if capacity == 0 {
            return;
        }

        let mut existing_orders = Vec::new();
        reader.copy_last(capacity, &mut existing_orders);
        if existing_orders.len() >= capacity {
            return;
        }

        let mut existing_companions = Vec::new();
        if let Some(companion_reader) = self.readers.mm_order_companion.as_ref() {
            companion_reader.copy_last(capacity, &mut existing_companions);
        }
        if existing_companions.len() < existing_orders.len() {
            existing_companions.resize(existing_orders.len(), MMOrderCompanionData::default());
        }

        let fill = capacity - existing_orders.len();
        let end_ms = existing_orders
            .first()
            .map(|row| row.seq_ring_time_ms().saturating_sub(1))
            .unwrap_or(now_ms);
        let mut orders = Vec::with_capacity(capacity);
        let mut companions = Vec::with_capacity(capacity);
        for idx in 0..fill {
            let time = diag_synthetic_time(end_ms, span_ms, idx, fill);
            orders.push(synthetic_mm_order_row(idx, fill, time, price_base));
            companions.push(synthetic_mm_companion(idx));
        }
        orders.extend_from_slice(&existing_orders);
        companions.extend_from_slice(&existing_companions[..existing_orders.len()]);

        writer.clear();
        writer.push_batch(&orders);
        if let Some(companion_writer) = self.mm_order_companion.as_mut() {
            companion_writer.clear();
            companion_writer.push_batch(&companions);
        }
    }
}

pub(crate) fn candles_snapshot_is_stale(last_time: MoonTime, now_time: MoonTime) -> bool {
    if last_time == MoonTime::ZERO || now_time == MoonTime::ZERO {
        return false;
    }
    (now_time.unix_millis() - last_time.unix_millis()).abs() > STALE_CANDLES_SNAPSHOT_MS
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

fn last_mini_time(reader: Option<&SeqRingReader<MiniCandle>>) -> Option<MoonTime> {
    let reader = reader?;
    let mut out = Vec::new();
    reader.copy_last(1, &mut out);
    out.first().map(|row| row.time)
}

#[cfg(any(test, feature = "diagnostics"))]
fn diag_fill_timed_ring<T, F>(
    writer: &mut Option<SeqRingWriter<T>>,
    reader: Option<&SeqRingReader<T>>,
    now_ms: i64,
    span_ms: i64,
    make_row: F,
) -> Option<Vec<T>>
where
    T: SeqRingRow + SeqRingTimedRow,
    F: Fn(usize, usize, MoonTime) -> T,
{
    let (Some(writer), Some(reader)) = (writer.as_mut(), reader) else {
        return None;
    };
    let capacity = reader.capacity();
    if capacity == 0 {
        return Some(Vec::new());
    }

    let mut existing = Vec::new();
    reader.copy_last(capacity, &mut existing);
    if existing.len() >= capacity {
        return Some(existing);
    }

    let fill = capacity - existing.len();
    let end_ms = existing
        .first()
        .map(|row| row.seq_ring_time_ms().saturating_sub(1))
        .unwrap_or(now_ms);
    let mut rows = Vec::with_capacity(capacity);
    for idx in 0..fill {
        let time = diag_synthetic_time(end_ms, span_ms, idx, fill);
        rows.push(make_row(idx, fill, time));
    }
    rows.extend_from_slice(&existing);

    writer.clear();
    writer.push_batch(&rows);
    Some(rows)
}

#[cfg(any(test, feature = "diagnostics"))]
fn diag_synthetic_time(end_ms: i64, span_ms: i64, idx: usize, count: usize) -> MoonTime {
    let start_ms = end_ms.saturating_sub(span_ms);
    let offset = if count <= 1 {
        0
    } else {
        ((span_ms as i128) * (idx as i128) / ((count - 1) as i128)) as i64
    };
    MoonTime::from_unix_millis(start_ms.saturating_add(offset))
}

#[cfg(any(test, feature = "diagnostics"))]
fn valid_diag_price(price: f32) -> Option<f32> {
    if price.is_finite() && price > 0.0 {
        Some(price)
    } else {
        None
    }
}

#[cfg(any(test, feature = "diagnostics"))]
fn diag_last_reader_price<T, F>(reader: Option<&SeqRingReader<T>>, price: F) -> Option<f32>
where
    T: SeqRingRow,
    F: Fn(&T) -> f32,
{
    let reader = reader?;
    let mut rows = Vec::new();
    reader.copy_last(1, &mut rows);
    rows.first().and_then(|row| valid_diag_price(price(row)))
}

#[cfg(any(test, feature = "diagnostics"))]
fn synthetic_price(idx: usize, count: usize, base: f32) -> f32 {
    let base = valid_diag_price(base).unwrap_or(100.0);
    let progress = if count <= 1 {
        1.0
    } else {
        idx as f32 / (count - 1) as f32
    };
    let trend = (progress - 1.0) * 0.003;
    let wave = ((idx % 17) as f32 - 8.0) * 0.00025;
    (base * (1.0 + trend + wave)).max(f32::MIN_POSITIVE)
}

#[cfg(any(test, feature = "diagnostics"))]
fn synthetic_trade_row(
    idx: usize,
    count: usize,
    time: MoonTime,
    price_base: f32,
) -> TradeHistoryRow {
    let price = synthetic_price(idx, count, price_base);
    let qty = 0.01 + (idx % 13) as f32 * 0.0025;
    TradeHistoryRow {
        time,
        price,
        qty: if idx % 2 == 0 { qty } else { -qty },
    }
}

#[cfg(any(test, feature = "diagnostics"))]
fn synthetic_spot_trade_row(
    idx: usize,
    count: usize,
    time: MoonTime,
    price_base: f32,
) -> TradeHistoryRow {
    let mut row = synthetic_trade_row(idx, count, time, price_base);
    row.price *= 1.0003;
    row
}

#[cfg(any(test, feature = "diagnostics"))]
fn synthetic_liquidation_row(
    idx: usize,
    count: usize,
    time: MoonTime,
    price_base: f32,
) -> TradeHistoryRow {
    let mut row = synthetic_trade_row(idx, count, time, price_base);
    row.price *= 0.9991;
    row.qty *= 3.0;
    row
}

#[cfg(any(test, feature = "diagnostics"))]
fn synthetic_mm_order_row(
    idx: usize,
    count: usize,
    time: MoonTime,
    price_base: f32,
) -> MMOrderHistoryRow {
    let q = 1.0 + (idx % 11) as f64 * 0.25;
    MMOrderHistoryRow {
        time,
        volume: synthetic_price(idx, count, price_base) as f64,
        q: if idx % 2 == 0 { q } else { -q },
    }
}

#[cfg(any(test, feature = "diagnostics"))]
fn synthetic_mm_companion(idx: usize) -> MMOrderCompanionData {
    let mut taker = [0u8; 20];
    for (byte_idx, byte) in taker.iter_mut().enumerate() {
        *byte = (idx as u8).wrapping_mul(31).wrapping_add(byte_idx as u8);
    }
    MMOrderCompanionData {
        taker,
        color: hl_address_color(taker),
    }
}

#[cfg(any(test, feature = "diagnostics"))]
fn synthetic_last_price_point(
    idx: usize,
    count: usize,
    time: MoonTime,
    price_base: f32,
) -> LastPricePoint {
    LastPricePoint {
        current: synthetic_price(idx, count, price_base),
        time,
    }
}

#[cfg(any(test, feature = "diagnostics"))]
fn synthetic_mark_price_point(
    idx: usize,
    count: usize,
    time: MoonTime,
    price_base: f32,
) -> MarkPricePoint {
    MarkPricePoint {
        current: synthetic_price(idx, count, price_base) * 1.0002,
        time,
    }
}

#[cfg(any(test, feature = "diagnostics"))]
fn synthetic_mini_candle(idx: usize, count: usize, time: MoonTime, price_base: f32) -> MiniCandle {
    let mid = synthetic_price(idx, count, price_base);
    let half_range = (mid * (0.0008 + (idx % 5) as f32 * 0.00005)).max(0.0001);
    MiniCandle {
        time,
        cnt: 1 + (idx % 17) as i32,
        min_price: mid - half_range,
        max_price: mid + half_range,
        buy_vol: 10.0 + (idx % 23) as f32,
        sell_vol: 8.0 + (idx % 19) as f32,
    }
}

#[cfg(any(test, feature = "diagnostics"))]
fn synthetic_candle_5m(idx: usize, count: usize, time: MoonTime, price_base: f32) -> Candle5mRow {
    let open = synthetic_price(idx, count, price_base);
    let close = open * if idx % 2 == 0 { 1.0004 } else { 0.9997 };
    let range = (open * 0.0009).max(0.0001);
    let high = open.max(close) + range;
    let low = open.min(close) - range;
    Candle5mRow {
        open,
        close,
        high,
        low,
        volume: 1_000.0 + (idx % 37) as f32 * 11.0,
        time,
    }
}

#[cfg(test)]
mod tests;
