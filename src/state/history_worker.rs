//! Retained-history writer worker.
//!
//! The protocol/event-dispatch path must not own hot history locks. It sends
//! typed stream batches through this unbounded handle; the worker thread owns
//! [`MarketHistoryRegistry`] and is the single writer for all per-market
//! [`MarketHistoryStore`](crate::state::history_store::MarketHistoryStore)
//! instances.

use std::collections::HashMap;
use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use crate::state::eps::EpsProfile;
use crate::state::history::{
    moon_time_from_delphi_days, Candle5mRow, MarketDerivedSnapshot, RollingTradeVolumeSnapshot,
    TradesPacketTimeShift,
};
use crate::state::history_store::{
    MarketHistoryConfig, MarketHistoryReadHandle, MarketHistoryReaders, MarketHistoryRegistry,
    TradeStorageScope,
};
use crate::MoonTime;

const STORE_WORKER_MAINTENANCE_INTERVAL: Duration = Duration::from_millis(250);
const STORE_WORKER_RECV_TIMEOUT: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct MarketHistoryTradeInput {
    pub(crate) time_delta_ms: i16,
    pub(crate) price: f32,
    pub(crate) qty: f32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct MarketHistoryMMOrderInput {
    pub(crate) time_delta_ms: i16,
    pub(crate) volume: f32,
    pub(crate) q: f32,
    pub(crate) taker: Option<[u8; 20]>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MarketHistoryLastPriceInput {
    pub(crate) market_name: Arc<str>,
    pub(crate) current: f64,
    pub(crate) bid: f64,
    pub(crate) ask: f64,
    pub(crate) mark_price: f64,
    pub(crate) mark_price_found: bool,
    pub(crate) is_btc_market: bool,
    pub(crate) is_base_usdt_market: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MarketHistoryLastPriceBatch {
    /// Packet-boundary timestamp captured in `UpdateMarketsList`.
    pub(crate) now_time: f64,
    pub(crate) rows: Vec<MarketHistoryLastPriceInput>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MarketHistoryStreamSection {
    pub(crate) market_index: u16,
    pub(crate) kind: MarketHistoryStreamSectionKind,
    pub(crate) start: usize,
    pub(crate) len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MarketHistoryStreamSectionKind {
    FuturesTrades,
    SpotTrades,
    Liquidations,
    MMOrders,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MarketHistoryStreamBatch {
    pub(crate) base_time: f64,
    /// Packet-processing boundary timestamp.
    pub(crate) now_time: f64,
    /// Section order from the original `TradesStream` packet. Trade sections
    /// index `trade_rows`; MM sections index `mm_order_rows`.
    pub(crate) sections: Vec<MarketHistoryStreamSection>,
    pub(crate) trade_rows: Vec<MarketHistoryTradeInput>,
    pub(crate) mm_order_rows: Vec<MarketHistoryMMOrderInput>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MarketHistoryCandlesSnapshot {
    pub(crate) market_name: String,
    pub(crate) candles_5m: Vec<Candle5mRow>,
}

type MarketHistoryReadIndex = Arc<RwLock<HashMap<Arc<str>, MarketHistoryReadHandle>>>;

#[derive(Clone)]
pub(crate) struct MarketHistoryHandle {
    tx: mpsc::Sender<MarketHistoryCommand>,
    read_index: MarketHistoryReadIndex,
}

impl fmt::Debug for MarketHistoryHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MarketHistoryHandle")
            .finish_non_exhaustive()
    }
}

pub(crate) struct MarketHistoryWorker {
    handle: MarketHistoryHandle,
    join: Option<thread::JoinHandle<()>>,
}

enum MarketHistoryCommand {
    SetEpsProfile(EpsProfile),
    ConfigureMarkets {
        market_slots: Vec<Option<Arc<str>>>,
        scope: Option<TradeStorageScope>,
    },
    #[cfg(test)]
    Readers {
        market_name: String,
        reply: mpsc::SyncSender<Option<MarketHistoryReaders>>,
    },
    #[cfg(test)]
    RollingVolumes {
        market_name: String,
        now_time: MoonTime,
        reply: mpsc::SyncSender<Option<RollingTradeVolumeSnapshot>>,
    },
    StreamBatch(MarketHistoryStreamBatch),
    LastPriceBatch(MarketHistoryLastPriceBatch),
    CandlesSnapshot {
        now_time: MoonTime,
        markets: Vec<MarketHistoryCandlesSnapshot>,
    },
    Barrier {
        reply: mpsc::SyncSender<()>,
    },
    #[cfg(test)]
    Flush {
        now_time: MoonTime,
        reply: mpsc::SyncSender<()>,
    },
    #[cfg(test)]
    PanicOnce,
    Stop,
}

impl MarketHistoryWorker {
    pub(crate) fn spawn(default_config: MarketHistoryConfig) -> Self {
        let (tx, rx) = mpsc::channel::<MarketHistoryCommand>();
        let read_index = Arc::new(RwLock::new(HashMap::new()));
        let worker_read_index = Arc::clone(&read_index);
        let join = thread::spawn(move || {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
                worker_loop(default_config, rx, worker_read_index)
            })) {
                log::error!(
                    target: "moonproto::history_worker",
                    "moonproto-history-worker panicked: {}",
                    panic_payload_message(payload.as_ref())
                );
            }
        });
        Self {
            handle: MarketHistoryHandle { tx, read_index },
            join: Some(join),
        }
    }

    pub(crate) fn handle(&self) -> MarketHistoryHandle {
        self.handle.clone()
    }

    #[cfg(test)]
    pub(crate) fn configure_markets(
        &self,
        market_names: Vec<String>,
        scope: Option<TradeStorageScope>,
    ) -> bool {
        self.handle.configure_markets(market_names, scope)
    }

    #[cfg(test)]
    pub(crate) fn readers(&self, market_name: &str) -> Option<MarketHistoryReaders> {
        self.handle.readers(market_name)
    }

    #[cfg(test)]
    pub(crate) fn rolling_volumes(
        &self,
        market_name: &str,
        now_time: MoonTime,
    ) -> Option<RollingTradeVolumeSnapshot> {
        self.handle.rolling_volumes(market_name, now_time)
    }

    #[cfg(test)]
    pub(crate) fn apply_candles_snapshot(
        &self,
        now_time: MoonTime,
        markets: Vec<MarketHistoryCandlesSnapshot>,
    ) -> bool {
        self.handle.apply_candles_snapshot(now_time, markets)
    }

    #[cfg(test)]
    pub(crate) fn flush(&self, now_time: MoonTime) -> bool {
        self.handle.flush(now_time)
    }
}

fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(value) = payload.downcast_ref::<&'static str>() {
        (*value).to_string()
    } else if let Some(value) = payload.downcast_ref::<String>() {
        value.clone()
    } else {
        "non-string panic payload".to_string()
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

    #[cfg(test)]
    pub(crate) fn configure_markets(
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

    #[cfg(test)]
    pub(crate) fn readers(&self, market_name: &str) -> Option<MarketHistoryReaders> {
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

    pub(crate) fn try_readers(&self, market_name: &str) -> Option<MarketHistoryReaders> {
        self.read_handle(market_name)
            .map(|read_handle| read_handle.readers())
    }

    fn read_handle(&self, market_name: &str) -> Option<MarketHistoryReadHandle> {
        self.read_index.read().get(market_name).cloned()
    }

    #[cfg(test)]
    pub(crate) fn rolling_volumes(
        &self,
        market_name: &str,
        now_time: MoonTime,
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

    pub(crate) fn try_rolling_volumes(
        &self,
        market_name: &str,
        now_time: MoonTime,
    ) -> Option<RollingTradeVolumeSnapshot> {
        self.read_handle(market_name)
            .map(|read_handle| read_handle.rolling_volumes(now_time))
    }

    pub(crate) fn try_derived_snapshot(
        &self,
        market_name: &str,
        _now_time: MoonTime,
    ) -> Option<MarketDerivedSnapshot> {
        self.read_handle(market_name)
            .map(|read_handle| read_handle.derived_snapshot())
    }

    /// Queue one decoded trades packet for retained-history storage.
    ///
    /// The channel is intentionally unbounded: retained history must not drop
    /// packets because of an internal Rust-only capacity cap. Sustained worker
    /// overload is a memory-pressure condition, not a silent data-loss policy.
    pub(crate) fn send_stream_batch(&self, batch: MarketHistoryStreamBatch) -> bool {
        self.tx
            .send(MarketHistoryCommand::StreamBatch(batch))
            .is_ok()
    }

    /// Queue `UpdateMarketsList -> TMarket.AddFrom -> HistoryPrice` rows.
    ///
    /// The channel is intentionally unbounded for the same reason as stream
    /// batches: retained history must not drop rows because of a hidden
    /// Rust-only capacity cap. If this queue grows, the fix is reducing worker
    /// cost/scope, not dropping LastPrice rows behind the user's back.
    pub(crate) fn send_last_price_batch(&self, batch: MarketHistoryLastPriceBatch) -> bool {
        self.tx
            .send(MarketHistoryCommand::LastPriceBatch(batch))
            .is_ok()
    }

    pub(crate) fn apply_candles_snapshot(
        &self,
        now_time: MoonTime,
        markets: Vec<MarketHistoryCandlesSnapshot>,
    ) -> bool {
        self.tx
            .send(MarketHistoryCommand::CandlesSnapshot { now_time, markets })
            .is_ok()
    }

    /// Queue a pure worker barrier. When the returned receiver yields, every
    /// command sent before the barrier has been processed by the worker.
    pub(crate) fn barrier_async(&self) -> Option<mpsc::Receiver<()>> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.tx
            .send(MarketHistoryCommand::Barrier { reply: reply_tx })
            .ok()?;
        Some(reply_rx)
    }

    /// Test/tool barrier: all previously sent batches are processed before this
    /// call returns, then evicted futures rows are compacted into mini-candles.
    #[cfg(test)]
    pub(crate) fn flush(&self, now_time: MoonTime) -> bool {
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

    #[cfg(test)]
    pub(crate) fn panic_once_for_test(&self) -> bool {
        self.tx.send(MarketHistoryCommand::PanicOnce).is_ok()
    }
}

fn worker_loop(
    default_config: MarketHistoryConfig,
    rx: mpsc::Receiver<MarketHistoryCommand>,
    read_index: MarketHistoryReadIndex,
) {
    let mut registry = MarketHistoryRegistry::new(default_config);
    let mut last_maintenance = Instant::now();
    let mut last_now_time = MoonTime::ZERO;

    loop {
        let keep_running = catch_unwind(AssertUnwindSafe(|| {
            handle_worker_command(
                rx.recv_timeout(STORE_WORKER_RECV_TIMEOUT),
                &mut registry,
                &read_index,
                &mut last_now_time,
                &mut last_maintenance,
            )
        }));
        match keep_running {
            Ok(true) => {}
            Ok(false) => break,
            Err(payload) => {
                log::error!(
                    target: "moonproto::history_worker",
                    "moonproto-history-worker command panicked; dropping command and continuing: {}",
                    panic_payload_message(payload.as_ref())
                );
            }
        }

        if last_maintenance.elapsed() >= STORE_WORKER_MAINTENANCE_INTERVAL {
            let maintenance = catch_unwind(AssertUnwindSafe(|| {
                run_store_maintenance(&mut registry, last_now_time);
                last_maintenance = Instant::now();
            }));
            if let Err(payload) = maintenance {
                log::error!(
                    target: "moonproto::history_worker",
                    "moonproto-history-worker maintenance panicked; skipping tick and continuing: {}",
                    panic_payload_message(payload.as_ref())
                );
                last_maintenance = Instant::now();
            }
        }
    }
}

fn handle_worker_command(
    command: Result<MarketHistoryCommand, mpsc::RecvTimeoutError>,
    registry: &mut MarketHistoryRegistry,
    read_index: &MarketHistoryReadIndex,
    last_now_time: &mut MoonTime,
    _last_maintenance: &mut Instant,
) -> bool {
    match command {
        Ok(MarketHistoryCommand::SetEpsProfile(eps_profile)) => {
            registry.set_eps_profile(eps_profile);
        }
        Ok(MarketHistoryCommand::ConfigureMarkets {
            market_slots,
            scope,
        }) => {
            registry.configure_market_index_slots(&market_slots, scope.as_ref());
            publish_read_index(read_index, registry);
        }
        #[cfg(test)]
        Ok(MarketHistoryCommand::Readers { market_name, reply }) => {
            let reply_value = registry
                .read_handle(&market_name)
                .map(|handle| handle.readers());
            let _ = reply.send(reply_value);
        }
        #[cfg(test)]
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
        Ok(MarketHistoryCommand::StreamBatch(batch)) => {
            *last_now_time = moon_time_from_delphi_days(batch.now_time);
            process_stream_batch(registry, batch);
        }
        Ok(MarketHistoryCommand::LastPriceBatch(batch)) => {
            *last_now_time = moon_time_from_delphi_days(batch.now_time);
            process_last_price_batch(registry, batch);
        }
        Ok(MarketHistoryCommand::CandlesSnapshot { now_time, markets }) => {
            process_candles_snapshot(registry, now_time, markets);
        }
        Ok(MarketHistoryCommand::Barrier { reply }) => {
            let _ = reply.send(());
        }
        #[cfg(test)]
        Ok(MarketHistoryCommand::Flush { now_time, reply }) => {
            *last_now_time = now_time;
            run_store_maintenance(registry, now_time);
            *_last_maintenance = Instant::now();
            let _ = reply.send(());
        }
        #[cfg(test)]
        Ok(MarketHistoryCommand::PanicOnce) => {
            panic!("test history worker panic");
        }
        Ok(MarketHistoryCommand::Stop) | Err(mpsc::RecvTimeoutError::Disconnected) => {
            read_index.write().clear();
            return false;
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {}
    }
    true
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
    let now_time = moon_time_from_delphi_days(batch.now_time);
    for row in batch.rows {
        let Some(store) = registry.get_mut(row.market_name.as_ref()) else {
            continue;
        };
        store.append_last_price(
            row.current,
            now_time,
            row.bid,
            row.ask,
            row.is_btc_market,
            row.is_base_usdt_market,
        );
        store.append_mark_price(row.mark_price, now_time, row.mark_price_found);
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
                    store.append_futures_stream_trade(
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
                    store.append_spot_stream_trade(
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
                    store.append_liquidation_stream(
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
                    store.append_mm_stream_order(
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
    now_time: MoonTime,
    markets: Vec<MarketHistoryCandlesSnapshot>,
) {
    for market in markets {
        let Some(store) = registry.get_mut(&market.market_name) else {
            continue;
        };
        store.replace_candles_5m_from_snapshot(&market.candles_5m, now_time);
    }
}

fn run_store_maintenance(registry: &mut MarketHistoryRegistry, now_time: MoonTime) {
    if now_time != MoonTime::ZERO {
        registry.compact_evicted_futures(now_time);
        registry.refresh_derived_analytics(now_time);
    }
}

#[cfg(test)]
mod tests;
