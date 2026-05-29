//! Retained-history writer worker.
//!
//! The protocol/event-dispatch path must not own hot history locks. It sends
//! typed stream batches through this unbounded handle; the worker thread owns
//! [`MarketHistoryRegistry`] and is the single writer for all per-market
//! [`MarketHistoryStore`](crate::state::history_store::MarketHistoryStore)
//! instances.

use std::collections::HashMap;
use std::fmt;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use crate::state::eps::EpsProfile;
use crate::state::history::{
    Candle5mRow, MarketDerivedSnapshot, RollingTradeVolumeSnapshot, TradesPacketTimeShift,
};
use crate::state::history_store::{
    MarketHistoryConfig, MarketHistoryReadHandle, MarketHistoryReaders, MarketHistoryRegistry,
    TradeStorageScope,
};

const STORE_WORKER_MAINTENANCE_INTERVAL: Duration = Duration::from_millis(250);
const STORE_WORKER_RECV_TIMEOUT: Duration = Duration::from_millis(50);

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MarketHistoryTradeInput {
    pub time_delta_ms: i16,
    pub price: f32,
    pub qty: f32,
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MarketHistoryMMOrderInput {
    pub time_delta_ms: i16,
    pub volume: f32,
    pub q: f32,
    pub taker: Option<[u8; 20]>,
}

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq)]
pub struct MarketHistoryLastPriceInput {
    pub market_name: Arc<str>,
    pub current: f64,
    pub bid: f64,
    pub ask: f64,
    pub mark_price: f64,
    pub mark_price_found: bool,
    pub is_btc_market: bool,
    pub is_base_usdt_market: bool,
}

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq)]
pub struct MarketHistoryLastPriceBatch {
    /// Delphi `NowTimeX := Now` captured in `UpdateMarketsList`.
    pub now_time: f64,
    pub rows: Vec<MarketHistoryLastPriceInput>,
}

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq)]
pub struct MarketHistoryStreamSection {
    pub market_index: u16,
    pub kind: MarketHistoryStreamSectionKind,
    pub start: usize,
    pub len: usize,
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketHistoryStreamSectionKind {
    FuturesTrades,
    SpotTrades,
    Liquidations,
    MMOrders,
}

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq)]
pub struct MarketHistoryStreamBatch {
    pub base_time: f64,
    /// Delphi `NowTimeX := Now` captured at the packet-processing boundary.
    pub now_time: f64,
    /// Section order from the original `TradesStream` packet. Trade sections
    /// index `trade_rows`; MM sections index `mm_order_rows`.
    pub sections: Vec<MarketHistoryStreamSection>,
    pub trade_rows: Vec<MarketHistoryTradeInput>,
    pub mm_order_rows: Vec<MarketHistoryMMOrderInput>,
}

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq)]
pub struct MarketHistoryCandlesSnapshot {
    pub market_name: String,
    pub candles_5m: Vec<Candle5mRow>,
}

type MarketHistoryReadIndex = Arc<RwLock<HashMap<Arc<str>, MarketHistoryReadHandle>>>;

#[derive(Clone)]
pub struct MarketHistoryHandle {
    tx: mpsc::Sender<MarketHistoryCommand>,
    read_index: MarketHistoryReadIndex,
}

impl fmt::Debug for MarketHistoryHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MarketHistoryHandle")
            .finish_non_exhaustive()
    }
}

pub struct MarketHistoryWorker {
    handle: MarketHistoryHandle,
    join: Option<thread::JoinHandle<()>>,
}

enum MarketHistoryCommand {
    SetEpsProfile(EpsProfile),
    ConfigureMarkets {
        market_slots: Vec<Option<Arc<str>>>,
        scope: Option<TradeStorageScope>,
    },
    Readers {
        market_name: String,
        reply: mpsc::SyncSender<Option<MarketHistoryReaders>>,
    },
    RollingVolumes {
        market_name: String,
        now_time: f64,
        reply: mpsc::SyncSender<Option<RollingTradeVolumeSnapshot>>,
    },
    DerivedSnapshot {
        market_name: String,
        now_time: f64,
        reply: mpsc::SyncSender<Option<MarketDerivedSnapshot>>,
    },
    StreamBatch(MarketHistoryStreamBatch),
    LastPriceBatch(MarketHistoryLastPriceBatch),
    CandlesSnapshot(Vec<MarketHistoryCandlesSnapshot>),
    Barrier {
        reply: mpsc::SyncSender<()>,
    },
    Flush {
        now_time: f64,
        reply: mpsc::SyncSender<()>,
    },
    Stop,
}

impl MarketHistoryWorker {
    pub fn spawn(default_config: MarketHistoryConfig) -> Self {
        let (tx, rx) = mpsc::channel::<MarketHistoryCommand>();
        let read_index = Arc::new(RwLock::new(HashMap::new()));
        let worker_read_index = Arc::clone(&read_index);
        let join = thread::spawn(move || worker_loop(default_config, rx, worker_read_index));
        Self {
            handle: MarketHistoryHandle { tx, read_index },
            join: Some(join),
        }
    }

    pub fn handle(&self) -> MarketHistoryHandle {
        self.handle.clone()
    }

    pub fn configure_markets(
        &self,
        market_names: Vec<String>,
        scope: Option<TradeStorageScope>,
    ) -> bool {
        self.handle.configure_markets(market_names, scope)
    }

    pub fn readers(&self, market_name: &str) -> Option<MarketHistoryReaders> {
        self.handle.readers(market_name)
    }

    pub fn rolling_volumes(
        &self,
        market_name: &str,
        now_time: f64,
    ) -> Option<RollingTradeVolumeSnapshot> {
        self.handle.rolling_volumes(market_name, now_time)
    }

    pub fn rolling_volumes_at(
        &self,
        market_name: &str,
        now_time: crate::DelphiTime,
    ) -> Option<RollingTradeVolumeSnapshot> {
        self.handle.rolling_volumes_at(market_name, now_time)
    }

    pub fn rolling_volumes_now(&self, market_name: &str) -> Option<RollingTradeVolumeSnapshot> {
        self.handle.rolling_volumes_now(market_name)
    }

    pub fn derived_snapshot(
        &self,
        market_name: &str,
        now_time: f64,
    ) -> Option<MarketDerivedSnapshot> {
        self.handle.derived_snapshot(market_name, now_time)
    }

    pub fn derived_snapshot_at(
        &self,
        market_name: &str,
        now_time: crate::DelphiTime,
    ) -> Option<MarketDerivedSnapshot> {
        self.handle.derived_snapshot_at(market_name, now_time)
    }

    pub fn derived_snapshot_now(&self, market_name: &str) -> Option<MarketDerivedSnapshot> {
        self.handle.derived_snapshot_now(market_name)
    }

    pub fn apply_candles_snapshot(&self, markets: Vec<MarketHistoryCandlesSnapshot>) -> bool {
        self.handle.apply_candles_snapshot(markets)
    }

    pub fn flush(&self, now_time: f64) -> bool {
        self.handle.flush(now_time)
    }
}

impl Drop for MarketHistoryWorker {
    fn drop(&mut self) {
        let _ = self.handle.tx.send(MarketHistoryCommand::Stop);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl MarketHistoryHandle {
    pub(crate) fn set_eps_profile(&self, eps_profile: EpsProfile) -> bool {
        self.tx
            .send(MarketHistoryCommand::SetEpsProfile(eps_profile))
            .is_ok()
    }

    pub fn configure_markets(
        &self,
        market_names: Vec<String>,
        scope: Option<TradeStorageScope>,
    ) -> bool {
        let market_slots = market_names
            .into_iter()
            .map(|name| Some(Arc::<str>::from(name)))
            .collect();
        self.configure_market_index_slots(market_slots, scope)
    }

    pub(crate) fn configure_market_index_slots(
        &self,
        market_slots: Vec<Option<Arc<str>>>,
        scope: Option<TradeStorageScope>,
    ) -> bool {
        self.tx
            .send(MarketHistoryCommand::ConfigureMarkets {
                market_slots,
                scope,
            })
            .is_ok()
    }

    pub fn readers(&self, market_name: &str) -> Option<MarketHistoryReaders> {
        if let Some(read_handle) = self.read_handle(market_name) {
            return Some(read_handle.readers());
        }

        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.tx
            .send(MarketHistoryCommand::Readers {
                market_name: market_name.to_string(),
                reply: reply_tx,
            })
            .ok()?;
        reply_rx.recv().ok().flatten()
    }

    pub fn try_readers(&self, market_name: &str) -> Option<MarketHistoryReaders> {
        self.read_handle(market_name)
            .map(|read_handle| read_handle.readers())
    }

    fn read_handle(&self, market_name: &str) -> Option<MarketHistoryReadHandle> {
        self.read_index.read().get(market_name).cloned()
    }

    pub fn rolling_volumes(
        &self,
        market_name: &str,
        now_time: f64,
    ) -> Option<RollingTradeVolumeSnapshot> {
        if let Some(read_handle) = self.read_handle(market_name) {
            return Some(read_handle.rolling_volumes(now_time));
        }

        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.tx
            .send(MarketHistoryCommand::RollingVolumes {
                market_name: market_name.to_string(),
                now_time,
                reply: reply_tx,
            })
            .ok()?;
        reply_rx.recv().ok().flatten()
    }

    pub fn try_rolling_volumes(
        &self,
        market_name: &str,
        now_time: f64,
    ) -> Option<RollingTradeVolumeSnapshot> {
        self.read_handle(market_name)
            .map(|read_handle| read_handle.rolling_volumes(now_time))
    }

    pub fn rolling_volumes_at(
        &self,
        market_name: &str,
        now_time: crate::DelphiTime,
    ) -> Option<RollingTradeVolumeSnapshot> {
        self.rolling_volumes(market_name, now_time.as_days())
    }

    pub fn rolling_volumes_now(&self, market_name: &str) -> Option<RollingTradeVolumeSnapshot> {
        self.rolling_volumes_at(market_name, crate::DelphiTime::now())
    }

    pub fn derived_snapshot(
        &self,
        market_name: &str,
        now_time: f64,
    ) -> Option<MarketDerivedSnapshot> {
        if let Some(read_handle) = self.read_handle(market_name) {
            let _ = now_time;
            return Some(read_handle.derived_snapshot());
        }

        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.tx
            .send(MarketHistoryCommand::DerivedSnapshot {
                market_name: market_name.to_string(),
                now_time,
                reply: reply_tx,
            })
            .ok()?;
        reply_rx.recv().ok().flatten()
    }

    pub fn try_derived_snapshot(
        &self,
        market_name: &str,
        _now_time: f64,
    ) -> Option<MarketDerivedSnapshot> {
        self.read_handle(market_name)
            .map(|read_handle| read_handle.derived_snapshot())
    }

    pub fn derived_snapshot_at(
        &self,
        market_name: &str,
        now_time: crate::DelphiTime,
    ) -> Option<MarketDerivedSnapshot> {
        self.derived_snapshot(market_name, now_time.as_days())
    }

    pub fn derived_snapshot_now(&self, market_name: &str) -> Option<MarketDerivedSnapshot> {
        self.derived_snapshot_at(market_name, crate::DelphiTime::now())
    }

    /// Queue one decoded trades packet for retained-history storage.
    ///
    /// The channel is intentionally unbounded: retained history must not drop
    /// packets because of an internal Rust-only capacity cap.
    pub fn send_stream_batch(&self, batch: MarketHistoryStreamBatch) -> bool {
        self.tx
            .send(MarketHistoryCommand::StreamBatch(batch))
            .is_ok()
    }

    /// Queue `UpdateMarketsList -> TMarket.AddFrom -> HistoryPrice` rows.
    ///
    /// The channel is intentionally unbounded for the same reason as stream
    /// batches: retained history must not drop rows because of a hidden
    /// Rust-only capacity cap.
    pub fn send_last_price_batch(&self, batch: MarketHistoryLastPriceBatch) -> bool {
        self.tx
            .send(MarketHistoryCommand::LastPriceBatch(batch))
            .is_ok()
    }

    pub fn apply_candles_snapshot(&self, markets: Vec<MarketHistoryCandlesSnapshot>) -> bool {
        self.tx
            .send(MarketHistoryCommand::CandlesSnapshot(markets))
            .is_ok()
    }

    /// Queue a pure worker barrier. When the returned receiver yields, every
    /// command sent before the barrier has been processed by the worker.
    pub fn barrier_async(&self) -> Option<mpsc::Receiver<()>> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.tx
            .send(MarketHistoryCommand::Barrier { reply: reply_tx })
            .ok()?;
        Some(reply_rx)
    }

    /// Test/tool barrier: all previously sent batches are processed before this
    /// call returns, then evicted futures rows are compacted into mini-candles.
    pub fn flush(&self, now_time: f64) -> bool {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        if self
            .tx
            .send(MarketHistoryCommand::Flush {
                now_time,
                reply: reply_tx,
            })
            .is_err()
        {
            return false;
        }
        reply_rx.recv().is_ok()
    }
}

fn worker_loop(
    default_config: MarketHistoryConfig,
    rx: mpsc::Receiver<MarketHistoryCommand>,
    read_index: MarketHistoryReadIndex,
) {
    let mut registry = MarketHistoryRegistry::new(default_config);
    let mut last_maintenance = Instant::now();
    let mut last_now_time = 0.0;

    loop {
        match rx.recv_timeout(STORE_WORKER_RECV_TIMEOUT) {
            Ok(MarketHistoryCommand::SetEpsProfile(eps_profile)) => {
                registry.set_eps_profile(eps_profile);
            }
            Ok(MarketHistoryCommand::ConfigureMarkets {
                market_slots,
                scope,
            }) => {
                registry.configure_market_index_slots(&market_slots, scope.as_ref());
                publish_read_index(&read_index, &registry);
            }
            Ok(MarketHistoryCommand::Readers { market_name, reply }) => {
                let reply_value = registry
                    .read_handle(&market_name)
                    .map(|handle| handle.readers());
                let _ = reply.send(reply_value);
            }
            Ok(MarketHistoryCommand::RollingVolumes {
                market_name,
                now_time,
                reply,
            }) => {
                let _ = reply.send(
                    registry
                        .get(&market_name)
                        .map(|store| store.rolling_volumes_snapshot(now_time)),
                );
            }
            Ok(MarketHistoryCommand::DerivedSnapshot {
                market_name,
                now_time,
                reply,
            }) => {
                if let Some(store) = registry.get_mut(&market_name) {
                    store.refresh_derived_analytics(now_time);
                    let _ = reply.send(Some(store.derived_snapshot()));
                } else {
                    let _ = reply.send(None);
                }
            }
            Ok(MarketHistoryCommand::StreamBatch(batch)) => {
                last_now_time = batch.now_time;
                process_stream_batch(&mut registry, batch);
            }
            Ok(MarketHistoryCommand::LastPriceBatch(batch)) => {
                last_now_time = batch.now_time;
                process_last_price_batch(&mut registry, batch);
            }
            Ok(MarketHistoryCommand::CandlesSnapshot(markets)) => {
                process_candles_snapshot(&mut registry, markets);
            }
            Ok(MarketHistoryCommand::Barrier { reply }) => {
                let _ = reply.send(());
            }
            Ok(MarketHistoryCommand::Flush { now_time, reply }) => {
                last_now_time = now_time;
                run_store_maintenance(&mut registry, now_time);
                last_maintenance = Instant::now();
                let _ = reply.send(());
            }
            Ok(MarketHistoryCommand::Stop) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                read_index.write().clear();
                break;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }

        if last_maintenance.elapsed() >= STORE_WORKER_MAINTENANCE_INTERVAL {
            run_store_maintenance(&mut registry, last_now_time);
            last_maintenance = Instant::now();
        }
    }
}

fn publish_read_index(read_index: &MarketHistoryReadIndex, registry: &MarketHistoryRegistry) {
    let handles = registry.read_handles();
    let mut index = read_index.write();
    index.clear();
    index.reserve(handles.len());
    index.extend(handles);
}

fn process_last_price_batch(
    registry: &mut MarketHistoryRegistry,
    batch: MarketHistoryLastPriceBatch,
) {
    for row in batch.rows {
        let Some(store) = registry.get_mut(row.market_name.as_ref()) else {
            continue;
        };
        store.append_last_price_like_delphi(
            row.current,
            batch.now_time,
            row.bid,
            row.ask,
            row.is_btc_market,
            row.is_base_usdt_market,
        );
        store.append_mark_price(row.mark_price, batch.now_time, row.mark_price_found);
    }
}

fn process_stream_batch(registry: &mut MarketHistoryRegistry, batch: MarketHistoryStreamBatch) {
    let mut time_shift = TradesPacketTimeShift::new();

    for section in batch.sections {
        match section.kind {
            MarketHistoryStreamSectionKind::FuturesTrades => {
                let Some(store) = registry.get_mut_by_server_index(section.market_index) else {
                    continue;
                };
                let end = section.start.saturating_add(section.len);
                for &row in batch.trade_rows.get(section.start..end).unwrap_or_default() {
                    store.append_futures_stream_trade_like_delphi(
                        batch.base_time,
                        row.time_delta_ms,
                        batch.now_time,
                        row.price,
                        row.qty,
                        &mut time_shift,
                    );
                }
            }
            MarketHistoryStreamSectionKind::SpotTrades => {
                let Some(store) = registry.get_mut_by_server_index(section.market_index) else {
                    continue;
                };
                let end = section.start.saturating_add(section.len);
                for &row in batch.trade_rows.get(section.start..end).unwrap_or_default() {
                    store.append_spot_stream_trade_like_delphi(
                        batch.base_time,
                        row.time_delta_ms,
                        batch.now_time,
                        row.price,
                        row.qty,
                        &mut time_shift,
                    );
                }
            }
            MarketHistoryStreamSectionKind::Liquidations => {
                let Some(store) = registry.get_mut_by_server_index(section.market_index) else {
                    continue;
                };
                let end = section.start.saturating_add(section.len);
                for &row in batch.trade_rows.get(section.start..end).unwrap_or_default() {
                    store.append_liquidation_stream_like_delphi(
                        batch.base_time,
                        row.time_delta_ms,
                        batch.now_time,
                        row.price,
                        row.qty,
                        &mut time_shift,
                    );
                }
            }
            MarketHistoryStreamSectionKind::MMOrders => {
                let Some(store) = registry.get_mut_by_server_index(section.market_index) else {
                    continue;
                };
                let end = section.start.saturating_add(section.len);
                for &row in batch
                    .mm_order_rows
                    .get(section.start..end)
                    .unwrap_or_default()
                {
                    store.append_mm_stream_order_like_delphi(
                        batch.base_time,
                        row.time_delta_ms,
                        batch.now_time,
                        row.volume,
                        row.q,
                        row.taker,
                        &mut time_shift,
                    );
                }
            }
        }
    }
}

fn process_candles_snapshot(
    registry: &mut MarketHistoryRegistry,
    markets: Vec<MarketHistoryCandlesSnapshot>,
) {
    for market in markets {
        let Some(store) = registry.get_mut(&market.market_name) else {
            continue;
        };
        store.replace_candles_5m_from_snapshot(&market.candles_5m);
    }
}

fn run_store_maintenance(registry: &mut MarketHistoryRegistry, now_time: f64) {
    if now_time > 0.0 {
        registry.compact_evicted_futures_like_delphi(now_time);
        registry.refresh_derived_analytics(now_time);
    }
}

#[cfg(test)]
mod tests;
