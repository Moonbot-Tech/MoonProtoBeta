//! Active-library retained history store.
//!
//! `MarketHistoryStore` is the per-market single-writer side owned by
//! `MarketHistoryWorker`. Public code receives cloneable [`SeqRingReader`]
//! handles; the dense retained rings use short read/write locks, but the UDP
//! protocol receive path is not the history writer.

use std::sync::Arc;

use crate::state::eps::EpsProfile;
use crate::state::history::{
    compact_trades_to_mini_candles, hl_address_color, Candle5mRow, LastPricePoint,
    MMOrderCompanionData, MMOrderHistoryRow, MarkPricePoint, MarketDerivedSnapshot, MiniCandle,
    RollingTradeVolumeSnapshot, RollingTradeVolumes, TradeHistoryRow, TradesPacketTimeShift,
};
use crate::state::seq_ring::{SeqRingReader, SeqRingWriter};
use parking_lot::RwLock;

const SECONDS_PER_DAY: f64 = 86_400.0;
const FIVE_MINUTES_DAYS: f64 = 5.0 / (24.0 * 60.0);

mod config;
mod derived;
mod registry;

#[cfg(test)]
use self::config::GIB;
pub use self::config::{MarketHistoryConfig, TradeStorageScope};
#[cfg(test)]
use self::derived::combine_deltas;
pub use self::registry::MarketHistoryRegistry;

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

    pub(crate) fn rolling_volumes(&self, now_time: f64) -> RollingTradeVolumeSnapshot {
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
}

pub struct MarketHistoryStore {
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
    /// In-progress 5m candle (Delphi `TMarket.FCandle`) — a separate
    /// accumulator, NOT stored in the `candles_5m` ring (which holds only
    /// sealed, end-stamped candles).
    current_candle: Option<Candle5mRow>,
    candle_deltas_dirty: bool,
    candle_deltas_bucket: Option<i64>,
    derived: MarketDerivedSnapshot,
    eps_profile: EpsProfile,
}

impl MarketHistoryStore {
    pub fn new(config: MarketHistoryConfig) -> Self {
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
            current_candle: None,
            candle_deltas_dirty: false,
            candle_deltas_bucket: None,
            derived: MarketDerivedSnapshot::default(),
            eps_profile,
        }
    }

    pub(crate) fn set_eps_profile(&mut self, eps_profile: EpsProfile) {
        self.eps_profile = eps_profile;
    }

    pub fn readers(&self) -> MarketHistoryReaders {
        self.readers.clone()
    }

    pub(crate) fn read_handle(&self) -> MarketHistoryReadHandle {
        self.read_handle.clone()
    }

    pub fn rolling_volumes_snapshot(&self, now_time: f64) -> RollingTradeVolumeSnapshot {
        self.rolling_volumes.snapshot(now_time)
    }

    pub fn derived_snapshot(&self) -> MarketDerivedSnapshot {
        self.derived
    }

    pub fn replace_candles_5m_from_snapshot(&mut self, candles: &[Candle5mRow]) {
        // Snapshot = sealed candles only (Delphi `Deep5m`; `StoreCandlesToZip`
        // serializes Deep5m, and Recalc5mCandle writes there only on seal — the
        // server does not send the in-progress `FCandle`). Push them all into
        // the ring as-is (end-stamped). Do NOT continue the last one as
        // in-progress — its period is closed; the client accumulates the current
        // period itself from the trade stream into a separate `current_candle`
        // (Delphi FCandle).
        let last_time = candles.last().map(|candle| candle.time).unwrap_or_default();
        self.current_candle = None;
        self.candle_deltas_dirty = true;
        if let Some(writer) = self.candles_5m.as_mut() {
            writer.clear();
            writer.push_batch(candles);
        }
        self.refresh_derived_analytics(last_time);
    }

    /// Delphi `TMarket.AddFrom` retained LastPrice row.
    ///
    /// The caller passes `p_last = (Bid + Ask) / 2` from `UpdateMarketsList`.
    /// Delphi appends only for BTC markets or base-USDT markets, only when a
    /// real bid/ask was present, and only after the caller checked `pLast`.
    // parity: MoonBot MarketsU.pas:TMarket.AddFrom
    pub(crate) fn append_last_price(
        &mut self,
        current: f64,
        real_time: f64,
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
        self.last_prices.as_mut().map(|writer| {
            writer.push(LastPricePoint {
                current: current as f32,
                real_time,
            })
        })
    }

    pub(crate) fn append_mark_price(
        &mut self,
        current: f64,
        real_time: f64,
        mark_price_found: bool,
    ) -> Option<u64> {
        // F2 (sverka #14): mark price is a market price -> Delphi `_epsM`.
        if !mark_price_found || current <= self.eps_profile.eps_m {
            return None;
        }
        self.mark_prices.as_mut().map(|writer| {
            writer.push(MarkPricePoint {
                current: current as f32,
                real_time,
            })
        })
    }

    // parity: MoonBot MoonProtoEngine.pas:ProcessTradesStream (futures trade -> m.FuturesTrades)
    pub(crate) fn append_futures_trade(&mut self, row: TradeHistoryRow) -> Option<u64> {
        let seq = self.push_retained_futures_trade(row)?;
        self.rolling_volumes.add_trade(row);
        self.update_current_candle_from_trade(row);
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
    ) -> f64 {
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
    ) -> (f64, Option<u64>) {
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
    ) -> (f64, Option<u64>) {
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
    ) -> (f64, Option<u64>) {
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
    pub(crate) fn compact_evicted_futures(&mut self, now_time: f64) -> usize {
        if self.evicted_futures_for_compaction.is_empty() {
            return 0;
        }
        let last_mini_time = last_mini_time(self.readers.mini_candles.as_ref()).unwrap_or(0.0);
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

    pub fn pending_evicted_futures_for_compaction(&self) -> usize {
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

fn last_mini_time(reader: Option<&SeqRingReader<MiniCandle>>) -> Option<f64> {
    let reader = reader?;
    let mut out = Vec::new();
    reader.copy_last(1, &mut out);
    out.first().map(|row| row.time)
}

#[cfg(test)]
mod tests;
