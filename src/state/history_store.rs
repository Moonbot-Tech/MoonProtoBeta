//! Active-library retained history store.
//!
//! `MarketHistoryStore` is the per-market single-writer side owned by
//! `MarketHistoryWorker`. Public code receives cloneable [`SeqRingReader`]
//! handles; the dense retained rings use short read/write locks, but the UDP
//! protocol receive path is not the history writer.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::state::history::{
    compact_trades_to_mini_candles_like_delphi, hl_address_color_like_delphi, Candle5mRow,
    CandleVolumeSnapshot, DerivedDeltaSnapshot, LastPricePoint, MMOrderCompanionData,
    MMOrderHistoryRow, MarketDerivedSnapshot, MiniCandle, RollingTradeVolumeSnapshot,
    RollingTradeVolumes, TradeHistoryRow, TradesPacketTimeShift,
};
use crate::state::seq_ring::{SeqRingReader, SeqRingWriter};

const EPS_MARKET: f64 = 1e-12;
const SECONDS_PER_DAY: f64 = 86_400.0;
const FIVE_MINUTES_DAYS: f64 = 5.0 / (24.0 * 60.0);

mod config;

#[cfg(test)]
use self::config::GIB;
pub use self::config::{MarketHistoryConfig, TradeStorageScope};

type SharedMarketName = Arc<str>;

#[derive(Clone, Default)]
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

#[derive(Default)]
pub struct MarketHistoryRegistry {
    default_config: MarketHistoryConfig,
    stores: HashMap<SharedMarketName, MarketHistoryStore>,
    stores_by_index: Vec<Option<SharedMarketName>>,
}

impl MarketHistoryRegistry {
    pub fn new(default_config: MarketHistoryConfig) -> Self {
        Self {
            default_config,
            stores: HashMap::new(),
            stores_by_index: Vec::new(),
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

    pub fn get_mut_by_server_index(
        &mut self,
        market_index: u16,
    ) -> Option<&mut MarketHistoryStore> {
        let market_name = self
            .stores_by_index
            .get(market_index as usize)?
            .as_deref()?;
        self.stores.get_mut(market_name)
    }

    fn insert_configured_market(
        &mut self,
        market_name: SharedMarketName,
    ) -> &mut MarketHistoryStore {
        self.stores
            .entry(market_name)
            .or_insert_with(|| MarketHistoryStore::new(self.default_config))
    }

    pub fn configure_markets(
        &mut self,
        market_names: &[String],
        scope: Option<&TradeStorageScope>,
    ) -> usize {
        self.configure_market_index_slot_names(
            market_names.iter().map(|name| Some(name.as_str())),
            scope,
        )
    }

    pub fn configure_market_index_slots<S>(
        &mut self,
        market_slots: &[Option<S>],
        scope: Option<&TradeStorageScope>,
    ) -> usize
    where
        S: AsRef<str>,
    {
        self.configure_market_index_slot_names(
            market_slots
                .iter()
                .map(|slot| slot.as_ref().map(AsRef::as_ref)),
            scope,
        )
    }

    fn configure_market_index_slot_names<'a, I>(
        &mut self,
        market_slots: I,
        scope: Option<&TradeStorageScope>,
    ) -> usize
    where
        I: IntoIterator<Item = Option<&'a str>>,
    {
        let Some(scope) = scope else {
            self.stores.clear();
            self.stores_by_index.clear();
            return 0;
        };

        let market_slots = market_slots.into_iter();
        let (slot_count, _) = market_slots.size_hint();
        self.stores_by_index.clear();
        self.stores_by_index.reserve(slot_count);
        let mut desired = HashSet::with_capacity(slot_count);
        for slot in market_slots {
            let Some(name) = slot else {
                self.stores_by_index.push(None);
                continue;
            };
            if !scope.contains(name) {
                self.stores_by_index.push(None);
                continue;
            }
            let name = SharedMarketName::from(name);
            self.stores_by_index.push(Some(Arc::clone(&name)));
            desired.insert(name);
        }
        self.stores.retain(|name, _| desired.contains(name));
        for name in desired {
            self.insert_configured_market(name);
        }
        self.stores.len()
    }

    pub fn readers(&self, market_name: &str) -> Option<MarketHistoryReaders> {
        self.stores
            .get(market_name)
            .map(MarketHistoryStore::readers)
    }

    pub fn compact_evicted_futures_like_delphi(&mut self, now_time: f64) -> usize {
        self.stores
            .values_mut()
            .map(|store| store.compact_evicted_futures_like_delphi(now_time))
            .sum()
    }

    pub fn refresh_derived_analytics(&mut self, now_time: f64) {
        for store in self.stores.values_mut() {
            store.refresh_derived_analytics(now_time);
        }
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
    candles_5m: Option<SeqRingWriter<Candle5mRow>>,
    readers: MarketHistoryReaders,
    evicted_futures_for_compaction: Vec<TradeHistoryRow>,
    mini_scratch: Vec<MiniCandle>,
    rolling_volumes: RollingTradeVolumes,
    current_candle: Option<Candle5mRow>,
    current_candle_seq: Option<u64>,
    candle_deltas_dirty: bool,
    candle_deltas_bucket: Option<i64>,
    derived: MarketDerivedSnapshot,
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
        let (candles_5m, candles_reader) = optional_ring::<Candle5mRow>(config.candles_5m_capacity);

        Self {
            futures_trades,
            spot_trades,
            liquidations,
            mm_orders,
            mm_order_companion,
            last_prices,
            mini_candles,
            candles_5m,
            readers: MarketHistoryReaders {
                futures_trades: futures_reader,
                spot_trades: spot_reader,
                liquidations: liq_reader,
                mm_orders: mm_reader,
                mm_order_companion: mm_companion_reader,
                last_prices: last_reader,
                mini_candles: mini_reader,
                candles_5m: candles_reader,
            },
            evicted_futures_for_compaction: Vec::new(),
            mini_scratch: Vec::new(),
            rolling_volumes: RollingTradeVolumes::default(),
            current_candle: None,
            current_candle_seq: None,
            candle_deltas_dirty: false,
            candle_deltas_bucket: None,
            derived: MarketDerivedSnapshot::default(),
        }
    }

    pub fn readers(&self) -> MarketHistoryReaders {
        self.readers.clone()
    }

    pub fn rolling_volumes_snapshot(&self, now_time: f64) -> RollingTradeVolumeSnapshot {
        self.rolling_volumes.snapshot(now_time)
    }

    pub fn derived_snapshot(&self) -> MarketDerivedSnapshot {
        self.derived
    }

    pub fn replace_candles_5m_from_snapshot(&mut self, candles: &[Candle5mRow]) {
        self.current_candle = candles.last().copied();
        self.current_candle_seq = None;
        self.candle_deltas_dirty = true;
        if let Some(writer) = self.candles_5m.as_mut() {
            writer.clear();
            writer.push_batch(candles);
            if self.current_candle.is_some() {
                let bounds = writer.bounds();
                if bounds.len > 0 {
                    self.current_candle_seq = Some(bounds.next_seq - 1);
                }
            }
        }
        self.refresh_derived_analytics(
            self.current_candle
                .map(|candle| candle.time)
                .unwrap_or_default(),
        );
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

    pub fn append_futures_trade_like_delphi(&mut self, row: TradeHistoryRow) -> Option<u64> {
        let seq = self.push_retained_futures_trade(row)?;
        self.rolling_volumes.add_trade(row);
        self.update_current_candle_from_trade(row);
        Some(seq)
    }

    pub fn append_futures_stream_trade_like_delphi(
        &mut self,
        base_time: f64,
        time_delta_ms: i16,
        now_time: f64,
        price: f32,
        qty: f32,
        time_shift: &mut TradesPacketTimeShift,
    ) -> f64 {
        let time = time_shift.apply_like_delphi(base_time, time_delta_ms, now_time);
        self.append_futures_trade_like_delphi(TradeHistoryRow { time, price, qty });
        time
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

    pub fn refresh_derived_analytics(&mut self, now_time: f64) {
        self.seal_current_candle_if_due(now_time);
        let volumes = self.rolling_volumes.snapshot(now_time);
        let trade_deltas = trade_deltas_from_rolling_volumes(volumes);
        let last_price_deltas = self.last_price_deltas_one_pass(now_time);
        let candle_bucket = candle_delta_bucket(now_time);
        if self.candle_deltas_dirty || self.candle_deltas_bucket != Some(candle_bucket) {
            let (deltas, volumes) = self.candle_derived_one_pass(now_time);
            self.derived.candle_deltas = deltas;
            self.derived.candle_volumes = volumes;
            self.candle_deltas_bucket = Some(candle_bucket);
            self.candle_deltas_dirty = false;
        }

        self.derived.trade_volumes = volumes;
        self.derived.trade_deltas = trade_deltas;
        self.derived.last_price_deltas = last_price_deltas;
        self.derived.deltas =
            combine_deltas(trade_deltas, self.derived.candle_deltas, last_price_deltas);
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

    fn update_current_candle_from_trade(&mut self, row: TradeHistoryRow) {
        if row.time <= 0.0 || row.price <= 0.0 {
            return;
        }
        self.seal_current_candle_if_due(row.time);
        let traded_value = row.traded_value();
        let mut candle = self.current_candle.unwrap_or_else(|| {
            self.current_candle_seq = None;
            Candle5mRow {
                open_p: row.price,
                close_p: row.price,
                max_p: row.price,
                min_p: row.price,
                vol: 0.0,
                time: row.time,
            }
        });
        candle.close_p = row.price;
        candle.max_p = candle.max_p.max(row.price);
        candle.min_p = if candle.min_p <= 0.0 {
            row.price
        } else {
            candle.min_p.min(row.price)
        };
        candle.vol += traded_value;
        self.current_candle = Some(candle);
        self.candle_deltas_dirty = true;
        self.publish_current_candle();
    }

    fn seal_current_candle_if_due(&mut self, now_time: f64) {
        let Some(candle) = self.current_candle else {
            return;
        };
        if now_time > 0.0 && now_time - candle.time >= FIVE_MINUTES_DAYS {
            if self.current_candle_seq.is_none() {
                self.publish_current_candle();
            }
            self.current_candle = None;
            self.current_candle_seq = None;
            self.candle_deltas_dirty = true;
        }
    }

    fn publish_current_candle(&mut self) {
        let Some(candle) = self.current_candle else {
            self.current_candle_seq = None;
            return;
        };
        let Some(writer) = self.candles_5m.as_mut() else {
            self.current_candle_seq = None;
            return;
        };
        if let Some(seq) = self.current_candle_seq {
            if writer.replace_seq(seq, candle) {
                return;
            }
        }
        self.current_candle_seq = Some(writer.push(candle));
    }

    fn candle_derived_one_pass(
        &self,
        now_time: f64,
    ) -> (DerivedDeltaSnapshot, CandleVolumeSnapshot) {
        let mut acc = CandleDerivedAccumulator::new(now_time);
        if let Some(reader) = self.readers.candles_5m.as_ref() {
            reader.with_last(reader.capacity(), |view| {
                view.for_each(|row| acc.add(*row));
            });
        }
        if self.current_candle_seq.is_none() {
            if let Some(candle) = self.current_candle {
                acc.add(candle);
            }
        }
        acc.finish()
    }

    fn last_price_deltas_one_pass(&self, now_time: f64) -> DerivedDeltaSnapshot {
        let mut acc = LastPriceDeltaAccumulator::new(now_time);
        if let Some(reader) = self.readers.last_prices.as_ref() {
            reader.with_last(reader.capacity(), |view| {
                view.for_each(|row| acc.add(*row));
            });
        }
        acc.finish()
    }
}

fn delta_percent(min_price: f64, max_price: f64) -> f64 {
    if min_price <= EPS_MARKET || max_price <= EPS_MARKET || max_price < min_price {
        return 0.0;
    }
    (max_price / min_price - 1.0) * 100.0
}

fn trade_deltas_from_rolling_volumes(volumes: RollingTradeVolumeSnapshot) -> DerivedDeltaSnapshot {
    DerivedDeltaSnapshot {
        one_minute: volumes.one_minute.price_delta_percent(),
        five_minutes: volumes.five_minutes.price_delta_percent(),
        ..DerivedDeltaSnapshot::default()
    }
}

fn combine_deltas(
    trade_deltas: DerivedDeltaSnapshot,
    candle_deltas: DerivedDeltaSnapshot,
    last_price_deltas: DerivedDeltaSnapshot,
) -> DerivedDeltaSnapshot {
    let one_hour = trade_deltas
        .one_hour
        .max(candle_deltas.one_hour)
        .max(last_price_deltas.one_hour);
    DerivedDeltaSnapshot {
        one_minute: trade_deltas
            .one_minute
            .max(candle_deltas.one_minute)
            .max(last_price_deltas.one_minute),
        five_minutes: trade_deltas
            .five_minutes
            .max(candle_deltas.five_minutes)
            .max(last_price_deltas.five_minutes),
        fifteen_minutes: trade_deltas
            .fifteen_minutes
            .max(candle_deltas.fifteen_minutes)
            .max(last_price_deltas.fifteen_minutes),
        thirty_minutes: trade_deltas
            .thirty_minutes
            .max(candle_deltas.thirty_minutes)
            .max(last_price_deltas.thirty_minutes),
        one_hour,
        two_hours: one_hour.max(
            trade_deltas
                .two_hours
                .max(candle_deltas.two_hours)
                .max(last_price_deltas.two_hours),
        ),
        three_hours: one_hour.max(
            trade_deltas
                .three_hours
                .max(candle_deltas.three_hours)
                .max(last_price_deltas.three_hours),
        ),
        twenty_four_hours: trade_deltas
            .twenty_four_hours
            .max(candle_deltas.twenty_four_hours)
            .max(last_price_deltas.twenty_four_hours)
            .max(one_hour),
        seventy_two_hours: trade_deltas
            .seventy_two_hours
            .max(candle_deltas.seventy_two_hours)
            .max(last_price_deltas.seventy_two_hours),
    }
}

fn candle_delta_bucket(now_time: f64) -> i64 {
    if now_time <= 0.0 {
        return i64::MIN;
    }
    (now_time * SECONDS_PER_DAY / (5.0 * 60.0)).floor() as i64
}

#[derive(Clone, Copy)]
struct CandleWindow {
    window_days: f64,
    min_price: f32,
    max_price: f32,
    volume: f64,
}

impl CandleWindow {
    fn new(window_seconds: f64) -> Self {
        Self {
            window_days: window_seconds / SECONDS_PER_DAY,
            min_price: 0.0,
            max_price: 0.0,
            volume: 0.0,
        }
    }

    fn add(&mut self, now_time: f64, candle: Candle5mRow) {
        // Delphi checks are strict on the old boundary:
        // `abs(Now-Time) < 15/MinsInDay`, `h < 72`, `h <= 2` -> age < 3h.
        if candle.time <= now_time - self.window_days || candle.time > now_time {
            return;
        }
        if candle.min_p > 0.0 && (self.min_price <= 0.0 || candle.min_p < self.min_price) {
            self.min_price = candle.min_p;
        }
        if candle.max_p > self.max_price {
            self.max_price = candle.max_p;
        }
        if candle.vol > 0.0 {
            self.volume += f64::from(candle.vol);
        }
    }

    fn finish_delta(self) -> f64 {
        delta_percent(f64::from(self.min_price), f64::from(self.max_price))
    }
}

struct CandleDerivedAccumulator {
    now_time: f64,
    five_minutes: CandleWindow,
    fifteen_minutes: CandleWindow,
    thirty_minutes: CandleWindow,
    one_hour: CandleWindow,
    two_hours_volume: CandleWindow,
    three_hours_volume: CandleWindow,
    twenty_four_hours_volume: CandleWindow,
    seventy_two_hours: CandleWindow,
    last2h_delta_like_delphi: CandleWindow,
    last3h_delta_like_delphi: CandleWindow,
    last24h_delta_like_delphi: CandleWindow,
}

impl CandleDerivedAccumulator {
    fn new(now_time: f64) -> Self {
        Self {
            now_time,
            five_minutes: CandleWindow::new(5.0 * 60.0),
            fifteen_minutes: CandleWindow::new(15.0 * 60.0),
            thirty_minutes: CandleWindow::new(30.0 * 60.0),
            one_hour: CandleWindow::new(60.0 * 60.0),
            two_hours_volume: CandleWindow::new(2.0 * 60.0 * 60.0),
            three_hours_volume: CandleWindow::new(3.0 * 60.0 * 60.0),
            twenty_four_hours_volume: CandleWindow::new(24.0 * 60.0 * 60.0),
            seventy_two_hours: CandleWindow::new(72.0 * 60.0 * 60.0),
            last2h_delta_like_delphi: CandleWindow::new(3.0 * 60.0 * 60.0),
            last3h_delta_like_delphi: CandleWindow::new(4.0 * 60.0 * 60.0),
            last24h_delta_like_delphi: CandleWindow::new(25.0 * 60.0 * 60.0),
        }
    }

    fn add(&mut self, candle: Candle5mRow) {
        self.five_minutes.add(self.now_time, candle);
        self.fifteen_minutes.add(self.now_time, candle);
        self.thirty_minutes.add(self.now_time, candle);
        self.one_hour.add(self.now_time, candle);
        self.two_hours_volume.add(self.now_time, candle);
        self.three_hours_volume.add(self.now_time, candle);
        self.twenty_four_hours_volume.add(self.now_time, candle);
        self.seventy_two_hours.add(self.now_time, candle);
        self.last2h_delta_like_delphi.add(self.now_time, candle);
        self.last3h_delta_like_delphi.add(self.now_time, candle);
        self.last24h_delta_like_delphi.add(self.now_time, candle);
    }

    fn finish(self) -> (DerivedDeltaSnapshot, CandleVolumeSnapshot) {
        let one_hour_delta = self.one_hour.finish_delta();
        (
            DerivedDeltaSnapshot {
                five_minutes: self.five_minutes.finish_delta(),
                fifteen_minutes: self.fifteen_minutes.finish_delta(),
                thirty_minutes: self.thirty_minutes.finish_delta(),
                one_hour: one_hour_delta,
                two_hours: one_hour_delta.max(self.last2h_delta_like_delphi.finish_delta()),
                three_hours: one_hour_delta.max(self.last3h_delta_like_delphi.finish_delta()),
                twenty_four_hours: one_hour_delta
                    .max(self.last24h_delta_like_delphi.finish_delta()),
                seventy_two_hours: self.seventy_two_hours.finish_delta(),
                ..DerivedDeltaSnapshot::default()
            },
            CandleVolumeSnapshot {
                five_minutes: self.five_minutes.volume,
                fifteen_minutes: self.fifteen_minutes.volume,
                thirty_minutes: self.thirty_minutes.volume,
                one_hour: self.one_hour.volume,
                two_hours: self.two_hours_volume.volume,
                three_hours: self.three_hours_volume.volume,
                twenty_four_hours: self.twenty_four_hours_volume.volume,
                seventy_two_hours: self.seventy_two_hours.volume,
            },
        )
    }
}

#[derive(Clone, Copy)]
struct LastPriceWindow {
    window_days: f64,
    min_price: f32,
    max_price: f32,
}

impl LastPriceWindow {
    fn new(window_seconds: f64) -> Self {
        Self {
            window_days: window_seconds / SECONDS_PER_DAY,
            min_price: 0.0,
            max_price: 0.0,
        }
    }

    fn add(&mut self, now_time: f64, row: LastPricePoint) {
        if row.real_time <= now_time - self.window_days || row.real_time > now_time {
            return;
        }
        if row.current <= 0.0 {
            return;
        }
        if self.min_price <= 0.0 || row.current < self.min_price {
            self.min_price = row.current;
        }
        if row.current > self.max_price {
            self.max_price = row.current;
        }
    }

    fn finish_delta(self) -> f64 {
        delta_percent(f64::from(self.min_price), f64::from(self.max_price))
    }
}

struct LastPriceDeltaAccumulator {
    now_time: f64,
    one_minute: LastPriceWindow,
    five_minutes: LastPriceWindow,
    fifteen_minutes: LastPriceWindow,
    thirty_minutes: LastPriceWindow,
    one_hour: LastPriceWindow,
}

impl LastPriceDeltaAccumulator {
    fn new(now_time: f64) -> Self {
        Self {
            now_time,
            one_minute: LastPriceWindow::new(60.0),
            five_minutes: LastPriceWindow::new(5.0 * 60.0),
            fifteen_minutes: LastPriceWindow::new(15.0 * 60.0),
            thirty_minutes: LastPriceWindow::new(30.0 * 60.0),
            one_hour: LastPriceWindow::new(60.0 * 60.0),
        }
    }

    fn add(&mut self, row: LastPricePoint) {
        self.one_minute.add(self.now_time, row);
        self.five_minutes.add(self.now_time, row);
        self.fifteen_minutes.add(self.now_time, row);
        self.thirty_minutes.add(self.now_time, row);
        self.one_hour.add(self.now_time, row);
    }

    fn finish(self) -> DerivedDeltaSnapshot {
        DerivedDeltaSnapshot {
            one_minute: self.one_minute.finish_delta(),
            five_minutes: self.five_minutes.finish_delta(),
            fifteen_minutes: self.fifteen_minutes.finish_delta(),
            thirty_minutes: self.thirty_minutes.finish_delta(),
            one_hour: self.one_hour.finish_delta(),
            ..DerivedDeltaSnapshot::default()
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
