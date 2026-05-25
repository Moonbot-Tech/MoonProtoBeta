//! Active-library retained history store.
//!
//! `MarketHistoryStore` is the per-market single-writer side owned by
//! `MarketHistoryWorker`. Public code receives cloneable [`SeqRingReader`]
//! handles; the dense retained rings use short read/write locks, but the UDP
//! protocol receive path is not the history writer.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::mem::size_of;

use crate::state::history::{
    compact_trades_to_mini_candles_like_delphi, hl_address_color_like_delphi,
    prepare_joined_trades_for_retained_append, Candle5mRow, CandleVolumeSnapshot,
    DerivedDeltaSnapshot, LastPricePoint, MMOrderCompanionData, MMOrderHistoryRow,
    MarketDerivedSnapshot, MiniCandle, RollingTradeVolumeSnapshot, RollingTradeVolumes,
    TradeHistoryRow, TradeJoinBuffer, TradesPacketTimeShift, DELPHI_SAME_TRADES_TIME_DAYS,
};
use crate::state::seq_ring::{SeqRingReader, SeqRingWriter};

const EPS_MARKET: f64 = 1e-12;
const SECONDS_PER_DAY: f64 = 86_400.0;
const FIVE_MINUTES_DAYS: f64 = 5.0 / (24.0 * 60.0);
const DELPHI_INT_TRADES_BUF_SIZE: usize = 1_000;
const GIB: usize = 1024 * 1024 * 1024;
const TRADE_SLOT_BYTES: usize = size_of::<TradeHistoryRow>();
const MM_ORDER_SLOT_BYTES: usize = size_of::<MMOrderHistoryRow>();
const MM_COMPANION_SLOT_BYTES: usize = size_of::<MMOrderCompanionData>();
const LAST_PRICE_SLOT_BYTES: usize = size_of::<LastPricePoint>();
const MINI_CANDLE_SLOT_BYTES: usize = size_of::<MiniCandle>();
const CANDLE_5M_SLOT_BYTES: usize = size_of::<Candle5mRow>();
const TRADE_JOIN_ROW_BYTES: usize = 16;

/// Active-library retained-history scope for the all-trades stream.
///
/// Delphi `SubscribeAllTrades` has no per-market scope: all known markets are
/// maintained once the stream is enabled. Rust additionally exposes an accepted
/// API deviation for UI clients that want to retain only a subset locally while
/// keeping the same wire `emk_SubscribeAllTrades` command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TradeStorageScope {
    All,
    Markets(BTreeSet<String>),
}

impl Default for TradeStorageScope {
    fn default() -> Self {
        Self::All
    }
}

impl TradeStorageScope {
    pub fn all() -> Self {
        Self::All
    }

    pub fn from_markets<I, S>(market_names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let names = market_names
            .into_iter()
            .map(|name| name.as_ref().to_string())
            .filter(|name| !name.is_empty())
            .collect::<BTreeSet<_>>();
        if names.is_empty() {
            Self::All
        } else {
            Self::Markets(names)
        }
    }

    pub fn contains(&self, market_name: &str) -> bool {
        match self {
            Self::All => true,
            Self::Markets(names) => names.contains(market_name),
        }
    }

    pub fn is_all(&self) -> bool {
        matches!(self, Self::All)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

impl Default for MarketHistoryConfig {
    fn default() -> Self {
        Self {
            futures_trades_capacity: 10_000,
            spot_trades_capacity: 5_000,
            liquidation_capacity: 2_000,
            mm_orders_capacity: 2_000,
            mm_order_companion_capacity: 2_000,
            last_price_capacity: 5_000,
            mini_candles_capacity: 5_000,
            candles_5m_capacity: 5_000,
            trade_join_capacity: DELPHI_INT_TRADES_BUF_SIZE,
        }
    }
}

impl MarketHistoryConfig {
    pub fn from_system_memory(market_count: usize) -> Self {
        system_total_memory_bytes()
            .map(|total| Self::from_total_memory_bytes(total, market_count))
            .unwrap_or_default()
    }

    pub fn from_total_memory_bytes(total_memory_bytes: usize, market_count: usize) -> Self {
        let market_count = market_count.max(1);
        let budget = Self::history_budget_bytes(total_memory_bytes);
        let per_market_budget = budget / market_count;

        let futures_trades_capacity =
            capacity_from_share(per_market_budget, 32, 100, TRADE_SLOT_BYTES, 200_000);
        let spot_trades_capacity =
            capacity_from_share(per_market_budget, 18, 100, TRADE_SLOT_BYTES, 150_000);
        let liquidation_capacity =
            capacity_from_share(per_market_budget, 7, 100, TRADE_SLOT_BYTES, 50_000);
        let mm_orders_capacity =
            capacity_from_share(per_market_budget, 7, 100, MM_ORDER_SLOT_BYTES, 50_000);
        let mm_order_companion_capacity =
            capacity_from_share(per_market_budget, 7, 100, MM_COMPANION_SLOT_BYTES, 50_000);
        let last_price_capacity =
            capacity_from_share(per_market_budget, 8, 100, LAST_PRICE_SLOT_BYTES, 80_000);
        let mini_candles_capacity =
            capacity_from_share(per_market_budget, 6, 100, MINI_CANDLE_SLOT_BYTES, 50_000);
        let candles_5m_capacity =
            capacity_from_share(per_market_budget, 3, 100, CANDLE_5M_SLOT_BYTES, 20_000);
        let trade_join_capacity = if futures_trades_capacity > 0 {
            DELPHI_INT_TRADES_BUF_SIZE
        } else {
            0
        };

        Self {
            futures_trades_capacity,
            spot_trades_capacity,
            liquidation_capacity,
            mm_orders_capacity,
            mm_order_companion_capacity,
            last_price_capacity,
            mini_candles_capacity,
            candles_5m_capacity,
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
            + self.candles_5m_capacity * CANDLE_5M_SLOT_BYTES
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

fn system_total_memory_bytes() -> Option<usize> {
    system_total_memory_bytes_impl()
}

#[cfg(windows)]
fn system_total_memory_bytes_impl() -> Option<usize> {
    #[repr(C)]
    struct MemoryStatusEx {
        dw_length: u32,
        dw_memory_load: u32,
        ull_total_phys: u64,
        ull_avail_phys: u64,
        ull_total_page_file: u64,
        ull_avail_page_file: u64,
        ull_total_virtual: u64,
        ull_avail_virtual: u64,
        ull_avail_extended_virtual: u64,
    }

    #[link(name = "kernel32")]
    extern "system" {
        fn GlobalMemoryStatusEx(buffer: *mut MemoryStatusEx) -> i32;
    }

    let mut status = MemoryStatusEx {
        dw_length: size_of::<MemoryStatusEx>() as u32,
        dw_memory_load: 0,
        ull_total_phys: 0,
        ull_avail_phys: 0,
        ull_total_page_file: 0,
        ull_avail_page_file: 0,
        ull_total_virtual: 0,
        ull_avail_virtual: 0,
        ull_avail_extended_virtual: 0,
    };

    let ok = unsafe { GlobalMemoryStatusEx(&mut status) };
    if ok == 0 {
        return None;
    }
    usize::try_from(status.ull_total_phys).ok()
}

#[cfg(unix)]
fn system_total_memory_bytes_impl() -> Option<usize> {
    let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if pages <= 0 || page_size <= 0 {
        return None;
    }
    let pages = usize::try_from(pages).ok()?;
    let page_size = usize::try_from(page_size).ok()?;
    pages.checked_mul(page_size)
}

#[cfg(not(any(windows, unix)))]
fn system_total_memory_bytes_impl() -> Option<usize> {
    None
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
    pub candles_5m: Option<SeqRingReader<Candle5mRow>>,
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

    pub(crate) fn ensure_market(&mut self, market_name: &str) -> &mut MarketHistoryStore {
        self.stores
            .entry(market_name.to_string())
            .or_insert_with(|| MarketHistoryStore::new(self.default_config))
    }

    pub fn configure_markets(
        &mut self,
        market_names: &[String],
        scope: Option<&TradeStorageScope>,
    ) -> usize {
        let Some(scope) = scope else {
            self.stores.clear();
            return 0;
        };

        let desired = market_names
            .iter()
            .filter(|name| scope.contains(name))
            .cloned()
            .collect::<HashSet<_>>();
        self.stores.retain(|name, _| desired.contains(name));
        for name in desired {
            self.ensure_market(&name);
        }
        self.stores.len()
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
    futures_join: TradeJoinBuffer,
    joined_scratch: Vec<TradeHistoryRow>,
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
            futures_join: TradeJoinBuffer::new(config.trade_join_capacity),
            joined_scratch: Vec::new(),
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
    /// active Delphi `JoinHOrders(..., DontSort=true)` copy-direct shape.
    pub fn drain_joined_futures_like_delphi(&mut self) -> usize {
        self.futures_join.drain_into(&mut self.joined_scratch);
        prepare_joined_trades_for_retained_append(&mut self.joined_scratch);

        let mut appended = 0usize;
        let rows = std::mem::take(&mut self.joined_scratch);
        for row in &rows {
            self.push_retained_futures_trade(*row);
            self.rolling_volumes.add_trade(*row);
            self.update_current_candle_from_trade(*row);
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

    fn push_retained_futures_trade(&mut self, row: TradeHistoryRow) {
        if let Some(writer) = self.futures_trades.as_mut() {
            if let Some(evicted) = writer.push_with_evicted(row).1 {
                self.evicted_futures_for_compaction.push(evicted);
            }
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
        assert_eq!(cfg.trade_join_capacity, DELPHI_INT_TRADES_BUF_SIZE);
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
    fn default_config_is_safe_for_all_markets_subscription() {
        let cfg = MarketHistoryConfig::default();
        let assumed_large_market_universe = 1_000;
        assert!(
            cfg.estimated_bytes_per_market() * assumed_large_market_universe
                < MarketHistoryConfig::history_budget_bytes(8 * GIB),
            "default config must not become multi-GB when subscribe_all_trades creates stores for all markets"
        );
    }

    #[test]
    fn memory_sized_config_keeps_delphi_trade_join_capacity_when_enabled() {
        let cfg = MarketHistoryConfig::from_total_memory_bytes(8 * GIB, 10_000);
        assert!(cfg.futures_trades_capacity > 0);
        assert_eq!(cfg.trade_join_capacity, DELPHI_INT_TRADES_BUF_SIZE);

        let disabled = MarketHistoryConfig::from_total_memory_bytes(1, usize::MAX);
        assert_eq!(disabled.futures_trades_capacity, 0);
        assert_eq!(disabled.trade_join_capacity, 0);
    }

    #[test]
    fn registry_configures_trade_storage_scope_from_known_markets() {
        let mut registry = MarketHistoryRegistry::new(MarketHistoryConfig {
            futures_trades_capacity: 1,
            spot_trades_capacity: 0,
            liquidation_capacity: 0,
            mm_orders_capacity: 0,
            mm_order_companion_capacity: 0,
            last_price_capacity: 0,
            mini_candles_capacity: 0,
            candles_5m_capacity: 0,
            trade_join_capacity: 0,
        });
        let markets = vec![
            "BTCUSDT".to_string(),
            "ETHUSDT".to_string(),
            "SOLUSDT".to_string(),
        ];

        assert_eq!(
            registry.configure_markets(&markets, Some(&TradeStorageScope::All)),
            3
        );
        assert!(registry.contains_market("BTCUSDT"));
        assert!(registry.contains_market("ETHUSDT"));

        let scope = TradeStorageScope::from_markets(["ETHUSDT"]);
        assert_eq!(registry.configure_markets(&markets, Some(&scope)), 1);
        assert!(!registry.contains_market("BTCUSDT"));
        assert!(registry.contains_market("ETHUSDT"));

        assert_eq!(registry.configure_markets(&markets, None), 0);
        assert!(registry.is_empty());
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
            candles_5m_capacity: 0,
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
            candles_5m_capacity: 0,
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
    fn last_price_history_feeds_delphi_hourly_delta_windows() {
        let now = 45_000.0;
        let mut store = MarketHistoryStore::new(MarketHistoryConfig {
            futures_trades_capacity: 0,
            spot_trades_capacity: 0,
            liquidation_capacity: 0,
            mm_orders_capacity: 0,
            mm_order_companion_capacity: 0,
            last_price_capacity: 8,
            mini_candles_capacity: 0,
            candles_5m_capacity: 0,
            trade_join_capacity: 0,
        });

        store.append_last_price_like_delphi(
            100.0,
            now - 50.0 / SECONDS_PER_DAY,
            99.0,
            101.0,
            true,
            false,
        );
        store.append_last_price_like_delphi(130.0, now - 14.0 / 1440.0, 129.0, 131.0, true, false);
        store.append_last_price_like_delphi(170.0, now - 59.0 / 1440.0, 169.0, 171.0, true, false);
        store.append_last_price_like_delphi(250.0, now - 60.0 / 1440.0, 249.0, 251.0, true, false);

        store.refresh_derived_analytics(now);
        let derived = store.derived_snapshot();

        assert!((derived.last_price_deltas.one_minute - 0.0).abs() < 1e-9);
        assert!((derived.last_price_deltas.fifteen_minutes - 30.0).abs() < 1e-9);
        assert!((derived.last_price_deltas.thirty_minutes - 30.0).abs() < 1e-9);
        assert!((derived.last_price_deltas.one_hour - 70.0).abs() < 1e-9);
        assert!((derived.deltas.one_hour - 70.0).abs() < 1e-9);
    }

    #[test]
    fn futures_join_drains_direct_like_dontsort_and_updates_volumes() {
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
            candles_5m_capacity: 0,
            trade_join_capacity: 6,
        });

        store.push_futures_trade_into_join_like_delphi(trade(sec(10.0), 100.0, 1.0), 0.01);
        assert_eq!(store.drain_joined_futures_like_delphi(), 1);

        store.push_futures_trade_into_join_like_delphi(trade(sec(9.0), 90.0, 1.0), 0.01);
        store.push_futures_trade_into_join_like_delphi(trade(sec(12.0), 120.0, -2.0), 0.01);
        store.push_futures_trade_into_join_like_delphi(trade(sec(11.0), 110.0, 3.0), 0.01);
        assert_eq!(store.drain_joined_futures_like_delphi(), 3);

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
                trade(sec(9.0), 90.0, 1.0),
                trade(sec(12.0), 120.0, -2.0),
                trade(sec(11.0), 110.0, 3.0),
            ]
        );

        let volumes = store.rolling_volumes_snapshot(sec(12.0));
        assert_eq!(volumes.five_minutes.buy_value, 520.0);
        assert_eq!(volumes.five_minutes.sell_value, 240.0);
        assert_eq!(volumes.five_minutes.trade_count, 4);
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
            candles_5m_capacity: 0,
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
            candles_5m_capacity: 0,
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

    #[test]
    fn candles_snapshot_replaces_retained_5m_rows_and_feeds_deltas() {
        let mut store = MarketHistoryStore::new(MarketHistoryConfig {
            futures_trades_capacity: 8,
            spot_trades_capacity: 0,
            liquidation_capacity: 0,
            mm_orders_capacity: 0,
            mm_order_companion_capacity: 0,
            last_price_capacity: 0,
            mini_candles_capacity: 0,
            candles_5m_capacity: 8,
            trade_join_capacity: 8,
        });
        let now = 45_000.0;
        store.replace_candles_5m_from_snapshot(&[
            Candle5mRow {
                time: now - 10.0 / 1440.0,
                min_p: 90.0,
                max_p: 110.0,
                close_p: 100.0,
                open_p: 95.0,
                vol: 1_000.0,
            },
            Candle5mRow {
                time: now,
                min_p: 100.0,
                max_p: 120.0,
                close_p: 115.0,
                open_p: 105.0,
                vol: 2_000.0,
            },
        ]);

        let mut candles = Vec::new();
        store
            .readers()
            .candles_5m
            .unwrap()
            .copy_last(8, &mut candles);
        assert_eq!(candles.len(), 2);

        store
            .push_futures_trade_into_join_like_delphi(trade(now + 1.0 / 86_400.0, 125.0, 2.0), 0.0);
        assert_eq!(store.drain_joined_futures_like_delphi(), 1);
        candles.clear();
        store
            .readers()
            .candles_5m
            .unwrap()
            .copy_last(8, &mut candles);
        assert_eq!(candles.len(), 2);
        assert_eq!(candles[1].close_p, 125.0);
        assert_eq!(candles[1].max_p, 125.0);
        assert_eq!(candles[1].vol, 2_250.0);

        store.refresh_derived_analytics(now);
        let derived = store.derived_snapshot();
        assert!((derived.candle_deltas.fifteen_minutes - 38.8888888889).abs() < 1e-6);
        assert_eq!(derived.candle_volumes.fifteen_minutes, 3_250.0);
        assert_eq!(derived.candle_volumes.one_hour, 3_250.0);
        assert_eq!(derived.trade_deltas.fifteen_minutes, 0.0);
        assert!((derived.deltas.fifteen_minutes - 38.8888888889).abs() < 1e-6);
    }

    #[test]
    fn retained_trades_update_current_candle_and_derived_volumes() {
        let mut store = MarketHistoryStore::new(MarketHistoryConfig {
            futures_trades_capacity: 8,
            spot_trades_capacity: 0,
            liquidation_capacity: 0,
            mm_orders_capacity: 0,
            mm_order_companion_capacity: 0,
            last_price_capacity: 0,
            mini_candles_capacity: 0,
            candles_5m_capacity: 8,
            trade_join_capacity: 8,
        });
        let now = 45_000.0;
        store.push_futures_trade_into_join_like_delphi(
            trade(now - 10.0 / 86_400.0, 100.0, 2.0),
            0.0,
        );
        store.push_futures_trade_into_join_like_delphi(
            trade(now - 5.0 / 86_400.0, 110.0, -1.0),
            0.0,
        );
        assert_eq!(store.drain_joined_futures_like_delphi(), 2);
        store.refresh_derived_analytics(now);

        let derived = store.derived_snapshot();
        assert_eq!(derived.trade_volumes.one_minute.buy_value, 200.0);
        assert_eq!(derived.trade_volumes.one_minute.sell_value, 110.0);
        assert_eq!(derived.trade_volumes.one_minute.min_price, 100.0);
        assert_eq!(derived.trade_volumes.one_minute.max_price, 110.0);
        assert!((derived.trade_deltas.one_minute - 10.0).abs() < 1e-9);
        assert_eq!(derived.candle_deltas.one_minute, 0.0);
        assert_eq!(derived.candle_volumes.five_minutes, 310.0);
        assert!((derived.deltas.one_minute - 10.0).abs() < 1e-9);
    }

    #[test]
    fn combined_long_deltas_do_not_drop_below_one_hour_like_delphi() {
        let trade = DerivedDeltaSnapshot {
            one_hour: 12.0,
            ..DerivedDeltaSnapshot::default()
        };
        let candles = DerivedDeltaSnapshot {
            two_hours: 4.0,
            three_hours: 5.0,
            twenty_four_hours: 6.0,
            seventy_two_hours: 7.0,
            ..DerivedDeltaSnapshot::default()
        };
        let last_price = DerivedDeltaSnapshot {
            fifteen_minutes: 13.0,
            one_hour: 14.0,
            ..DerivedDeltaSnapshot::default()
        };

        let combined = combine_deltas(trade, candles, last_price);

        assert_eq!(combined.fifteen_minutes, 13.0);
        assert_eq!(combined.one_hour, 14.0);
        assert_eq!(combined.two_hours, 14.0);
        assert_eq!(combined.three_hours, 14.0);
        assert_eq!(combined.twenty_four_hours, 14.0);
        assert_eq!(
            combined.seventy_two_hours, 7.0,
            "Delphi RecalcPumpQ only floors 2h/3h/24h by Last1hDelta; 72h stays its own source"
        );
    }

    #[test]
    fn candle_long_delta_windows_match_delphi_trunc_hour_buckets() {
        let now = 45_000.0;
        let mut store = MarketHistoryStore::new(MarketHistoryConfig {
            futures_trades_capacity: 0,
            spot_trades_capacity: 0,
            liquidation_capacity: 0,
            mm_orders_capacity: 0,
            mm_order_companion_capacity: 0,
            last_price_capacity: 0,
            mini_candles_capacity: 0,
            candles_5m_capacity: 8,
            trade_join_capacity: 0,
        });
        store.replace_candles_5m_from_snapshot(&[
            Candle5mRow {
                time: now - 2.5 / 24.0,
                min_p: 100.0,
                max_p: 130.0,
                close_p: 100.0,
                open_p: 100.0,
                vol: 1.0,
            },
            Candle5mRow {
                time: now - 3.0 / 24.0,
                min_p: 100.0,
                max_p: 190.0,
                close_p: 100.0,
                open_p: 100.0,
                vol: 8.0,
            },
            Candle5mRow {
                time: now - 3.5 / 24.0,
                min_p: 100.0,
                max_p: 140.0,
                close_p: 100.0,
                open_p: 100.0,
                vol: 2.0,
            },
            Candle5mRow {
                time: now - 24.5 / 24.0,
                min_p: 100.0,
                max_p: 150.0,
                close_p: 100.0,
                open_p: 100.0,
                vol: 4.0,
            },
            Candle5mRow {
                time: now - 25.0 / 24.0,
                min_p: 100.0,
                max_p: 220.0,
                close_p: 100.0,
                open_p: 100.0,
                vol: 16.0,
            },
        ]);

        store.refresh_derived_analytics(now);
        let derived = store.derived_snapshot();

        assert!((derived.candle_deltas.two_hours - 30.0).abs() < 1e-9);
        assert!((derived.candle_deltas.three_hours - 90.0).abs() < 1e-9);
        assert!((derived.candle_deltas.twenty_four_hours - 90.0).abs() < 1e-9);
        assert_eq!(
            derived.candle_volumes.twenty_four_hours, 11.0,
            "candle volumes keep exact 24h semantics; only Delphi long delta fields use h<= bucket windows"
        );
    }

    #[test]
    fn candle_windows_exclude_exact_old_boundary_like_delphi() {
        let now = 45_000.0;
        let mut store = MarketHistoryStore::new(MarketHistoryConfig {
            futures_trades_capacity: 0,
            spot_trades_capacity: 0,
            liquidation_capacity: 0,
            mm_orders_capacity: 0,
            mm_order_companion_capacity: 0,
            last_price_capacity: 0,
            mini_candles_capacity: 0,
            candles_5m_capacity: 8,
            trade_join_capacity: 0,
        });
        store.replace_candles_5m_from_snapshot(&[
            Candle5mRow {
                time: now - 15.0 / 1440.0,
                min_p: 100.0,
                max_p: 200.0,
                close_p: 100.0,
                open_p: 100.0,
                vol: 5.0,
            },
            Candle5mRow {
                time: now - (15.0 * 60.0 - 1.0) / SECONDS_PER_DAY,
                min_p: 100.0,
                max_p: 150.0,
                close_p: 100.0,
                open_p: 100.0,
                vol: 3.0,
            },
        ]);

        store.refresh_derived_analytics(now);
        let derived = store.derived_snapshot();

        assert!((derived.candle_deltas.fifteen_minutes - 50.0).abs() < 1e-9);
        assert_eq!(derived.candle_volumes.fifteen_minutes, 3.0);
    }
}
