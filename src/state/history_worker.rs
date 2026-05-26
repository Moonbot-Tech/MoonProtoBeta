//! Retained-history writer worker.
//!
//! The protocol/event-dispatch path must not own hot history locks. It sends
//! typed stream batches through this unbounded handle; the worker thread owns
//! [`MarketHistoryRegistry`] and is the single writer for all per-market
//! [`MarketHistoryStore`] instances.

use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use crate::state::history::{
    Candle5mRow, MarketDerivedSnapshot, RollingTradeVolumeSnapshot, TradesPacketTimeShift,
};
use crate::state::history_store::{
    MarketHistoryConfig, MarketHistoryReaders, MarketHistoryRegistry, TradeStorageScope,
};

const STORE_WORKER_MAINTENANCE_INTERVAL: Duration = Duration::from_millis(250);
const STORE_WORKER_RECV_TIMEOUT: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MarketHistoryTradeInput {
    pub time_delta_ms: i16,
    pub price: f32,
    pub qty: f32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MarketHistoryMMOrderInput {
    pub time_delta_ms: i16,
    pub vol: f32,
    pub q: f32,
    pub taker: Option<[u8; 20]>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MarketHistoryLastPriceInput {
    pub market_name: String,
    pub current: f64,
    pub bid: f64,
    pub ask: f64,
    pub is_btc_market: bool,
    pub is_base_usdt_market: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MarketHistoryLastPriceBatch {
    /// Delphi `NowTimeX := Now` captured in `UpdateMarketsList`.
    pub now_time: f64,
    pub rows: Vec<MarketHistoryLastPriceInput>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MarketHistoryStreamSection {
    FuturesTrades {
        market_name: String,
        rows: Vec<MarketHistoryTradeInput>,
    },
    SpotTrades {
        market_name: String,
        rows: Vec<MarketHistoryTradeInput>,
    },
    Liquidations {
        market_name: String,
        rows: Vec<MarketHistoryTradeInput>,
    },
    MMOrders {
        market_name: String,
        rows: Vec<MarketHistoryMMOrderInput>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct MarketHistoryStreamBatch {
    pub base_time: f64,
    /// Delphi `NowTimeX := Now` captured at the packet-processing boundary.
    pub now_time: f64,
    pub sections: Vec<MarketHistoryStreamSection>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MarketHistoryCandlesSnapshot {
    pub market_name: String,
    pub candles_5m: Vec<Candle5mRow>,
}

#[derive(Clone)]
pub struct MarketHistoryHandle {
    tx: mpsc::Sender<MarketHistoryCommand>,
}

pub struct MarketHistoryWorker {
    handle: MarketHistoryHandle,
    join: Option<thread::JoinHandle<()>>,
}

enum MarketHistoryCommand {
    ConfigureMarkets {
        market_names: Vec<String>,
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
    Flush {
        now_time: f64,
        reply: mpsc::SyncSender<()>,
    },
    Stop,
}

impl MarketHistoryWorker {
    pub fn spawn(default_config: MarketHistoryConfig) -> Self {
        let (tx, rx) = mpsc::channel::<MarketHistoryCommand>();
        let join = thread::spawn(move || worker_loop(default_config, rx));
        Self {
            handle: MarketHistoryHandle { tx },
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

    pub fn derived_snapshot(
        &self,
        market_name: &str,
        now_time: f64,
    ) -> Option<MarketDerivedSnapshot> {
        self.handle.derived_snapshot(market_name, now_time)
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
    pub fn configure_markets(
        &self,
        market_names: Vec<String>,
        scope: Option<TradeStorageScope>,
    ) -> bool {
        self.tx
            .send(MarketHistoryCommand::ConfigureMarkets {
                market_names,
                scope,
            })
            .is_ok()
    }

    pub fn readers(&self, market_name: &str) -> Option<MarketHistoryReaders> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.tx
            .send(MarketHistoryCommand::Readers {
                market_name: market_name.to_string(),
                reply: reply_tx,
            })
            .ok()?;
        reply_rx.recv().ok().flatten()
    }

    pub fn rolling_volumes(
        &self,
        market_name: &str,
        now_time: f64,
    ) -> Option<RollingTradeVolumeSnapshot> {
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

    pub fn derived_snapshot(
        &self,
        market_name: &str,
        now_time: f64,
    ) -> Option<MarketDerivedSnapshot> {
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

fn worker_loop(default_config: MarketHistoryConfig, rx: mpsc::Receiver<MarketHistoryCommand>) {
    let mut registry = MarketHistoryRegistry::new(default_config);
    let mut last_maintenance = Instant::now();
    let mut last_now_time = 0.0;

    loop {
        match rx.recv_timeout(STORE_WORKER_RECV_TIMEOUT) {
            Ok(MarketHistoryCommand::ConfigureMarkets {
                market_names,
                scope,
            }) => {
                registry.configure_markets(&market_names, scope.as_ref());
            }
            Ok(MarketHistoryCommand::Readers { market_name, reply }) => {
                let _ = reply.send(registry.readers(&market_name));
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
            Ok(MarketHistoryCommand::Flush { now_time, reply }) => {
                last_now_time = now_time;
                run_store_maintenance(&mut registry, now_time);
                last_maintenance = Instant::now();
                let _ = reply.send(());
            }
            Ok(MarketHistoryCommand::Stop) | Err(mpsc::RecvTimeoutError::Disconnected) => {
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

fn process_last_price_batch(
    registry: &mut MarketHistoryRegistry,
    batch: MarketHistoryLastPriceBatch,
) {
    for row in batch.rows {
        let Some(store) = registry.get_mut(&row.market_name) else {
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
    }
}

fn process_stream_batch(registry: &mut MarketHistoryRegistry, batch: MarketHistoryStreamBatch) {
    let mut time_shift = TradesPacketTimeShift::new();

    for section in batch.sections {
        match section {
            MarketHistoryStreamSection::FuturesTrades { market_name, rows } => {
                let Some(store) = registry.get_mut(&market_name) else {
                    continue;
                };
                for row in rows {
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
            MarketHistoryStreamSection::SpotTrades { market_name, rows } => {
                let Some(store) = registry.get_mut(&market_name) else {
                    continue;
                };
                for row in rows {
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
            MarketHistoryStreamSection::Liquidations { market_name, rows } => {
                let Some(store) = registry.get_mut(&market_name) else {
                    continue;
                };
                for row in rows {
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
            MarketHistoryStreamSection::MMOrders { market_name, rows } => {
                let Some(store) = registry.get_mut(&market_name) else {
                    continue;
                };
                for row in rows {
                    store.append_mm_stream_order_like_delphi(
                        batch.base_time,
                        row.time_delta_ms,
                        batch.now_time,
                        row.vol,
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
mod tests {
    use super::*;
    use crate::state::history::DELPHI_MSECS_PER_DAY;
    use crate::state::MarketHistoryConfig;

    #[test]
    fn worker_stores_enabled_futures_trades_after_flush() {
        let worker = MarketHistoryWorker::spawn(MarketHistoryConfig {
            futures_trades_capacity: 8,
            spot_trades_capacity: 0,
            liquidation_capacity: 0,
            mm_orders_capacity: 0,
            mm_order_companion_capacity: 0,
            last_price_capacity: 0,
            mini_candles_capacity: 0,
            candles_5m_capacity: 0,
        });
        assert!(worker.configure_markets(vec!["BTCUSDT".to_string()], Some(TradeStorageScope::All)));
        let readers = worker.readers("BTCUSDT").unwrap();
        let futures = readers.futures_trades.unwrap();

        let now_time = 45_000.0 + 1.0 / 24.0 + 1.0 / 86_400.0;
        worker.handle().send_stream_batch(MarketHistoryStreamBatch {
            base_time: 45_000.0,
            now_time,
            sections: vec![MarketHistoryStreamSection::FuturesTrades {
                market_name: "BTCUSDT".to_string(),
                rows: vec![MarketHistoryTradeInput {
                    time_delta_ms: 250,
                    price: 100.0,
                    qty: 2.0,
                }],
            }],
        });
        assert!(worker.flush(now_time));

        let mut out = Vec::new();
        futures.copy_last(8, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].price, 100.0);
        assert_eq!(out[0].qty, 2.0);
        assert_eq!(
            out[0].time,
            45_000.0 + 250.0 / DELPHI_MSECS_PER_DAY + 1.0 / 24.0
        );

        let volumes = worker
            .rolling_volumes("BTCUSDT", out[0].time)
            .expect("enabled market should expose rolling volumes");
        assert_eq!(volumes.one_minute.buy_value, 200.0);
        assert_eq!(volumes.one_minute.sell_value, 0.0);
        assert_eq!(volumes.five_minutes.trade_count, 1);
        assert!(
            worker.rolling_volumes("ETHUSDT", out[0].time).is_none(),
            "rolling volume reads must not allocate unknown markets"
        );
    }

    #[test]
    fn worker_does_not_create_market_from_stream_batch() {
        let worker = MarketHistoryWorker::spawn(MarketHistoryConfig::default());

        worker.handle().send_stream_batch(MarketHistoryStreamBatch {
            base_time: 45_000.0,
            now_time: 45_000.0,
            sections: vec![MarketHistoryStreamSection::SpotTrades {
                market_name: "ETHUSDT".to_string(),
                rows: vec![MarketHistoryTradeInput {
                    time_delta_ms: 0,
                    price: 10.0,
                    qty: 1.0,
                }],
            }],
        });
        assert!(worker.flush(45_000.0));

        assert!(
            worker.readers("ETHUSDT").is_none(),
            "stream batches must not allocate retained histories for every market"
        );
    }

    #[test]
    fn worker_stores_last_price_batch_for_enabled_market() {
        let worker = MarketHistoryWorker::spawn(MarketHistoryConfig {
            futures_trades_capacity: 0,
            spot_trades_capacity: 0,
            liquidation_capacity: 0,
            mm_orders_capacity: 0,
            mm_order_companion_capacity: 0,
            last_price_capacity: 4,
            mini_candles_capacity: 0,
            candles_5m_capacity: 0,
        });
        assert!(worker.configure_markets(vec!["BTCUSDT".to_string()], Some(TradeStorageScope::All)));
        let readers = worker.readers("BTCUSDT").unwrap();
        let last_prices = readers.last_prices.unwrap();

        worker
            .handle()
            .send_last_price_batch(MarketHistoryLastPriceBatch {
                now_time: 45_000.25,
                rows: vec![MarketHistoryLastPriceInput {
                    market_name: "BTCUSDT".to_string(),
                    current: 100.5,
                    bid: 100.0,
                    ask: 101.0,
                    is_btc_market: true,
                    is_base_usdt_market: false,
                }],
            });
        assert!(worker.flush(45_000.25));

        let mut out = Vec::new();
        last_prices.copy_last(4, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].current, 100.5);
        assert_eq!(out[0].real_time, 45_000.25);
    }

    #[test]
    fn worker_applies_candles_snapshot_only_for_configured_scope() {
        let worker = MarketHistoryWorker::spawn(MarketHistoryConfig {
            futures_trades_capacity: 0,
            spot_trades_capacity: 0,
            liquidation_capacity: 0,
            mm_orders_capacity: 0,
            mm_order_companion_capacity: 0,
            last_price_capacity: 0,
            mini_candles_capacity: 0,
            candles_5m_capacity: 4,
        });
        assert!(worker.configure_markets(
            vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
            Some(TradeStorageScope::from_markets(["BTCUSDT"]))
        ));
        assert!(worker.apply_candles_snapshot(vec![
            MarketHistoryCandlesSnapshot {
                market_name: "BTCUSDT".to_string(),
                candles_5m: vec![Candle5mRow {
                    open_p: 100.0,
                    close_p: 101.0,
                    max_p: 102.0,
                    min_p: 99.0,
                    vol: 10.0,
                    time: 45_000.0,
                }],
            },
            MarketHistoryCandlesSnapshot {
                market_name: "ETHUSDT".to_string(),
                candles_5m: vec![Candle5mRow {
                    open_p: 10.0,
                    close_p: 11.0,
                    max_p: 12.0,
                    min_p: 9.0,
                    vol: 1.0,
                    time: 45_000.0,
                }],
            },
        ]));
        assert!(worker.flush(45_000.0));

        let btc = worker.readers("BTCUSDT").unwrap().candles_5m.unwrap();
        let mut out = Vec::new();
        btc.copy_last(4, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].close_p, 101.0);
        assert!(worker.readers("ETHUSDT").is_none());
    }
}
