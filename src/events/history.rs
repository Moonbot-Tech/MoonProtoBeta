//! Retained-history worker wiring for `EventDispatcher`.

use super::*;
#[cfg(any(test, feature = "diagnostics"))]
use crate::client::metrics::{ProfilePhase, ProtocolMetrics};
use crate::state::markets::CandleDeltaBaseline;
use crate::time::MILLIS_PER_HOUR;
use std::collections::HashSet;
#[cfg(any(test, feature = "diagnostics"))]
use std::time::{Duration, Instant};

impl EventDispatcher {
    pub(crate) fn set_market_history_sizing(&mut self, sizing: MarketHistorySizing) {
        if self.market_history_sizing == sizing {
            return;
        }
        self.market_history_sizing = sizing;
        if self.owned_market_history.is_some() {
            self.market_history = None;
            self.owned_market_history = None;
        }
        self.last_market_history_scope = None;
        self.last_market_history_markets_version = None;
        self.sync_market_history_storage();
    }

    /// Attach a retained-history writer worker.
    ///
    /// The dispatcher does not mutate retained history directly. In active
    /// dispatch mode it only queues typed `TradesStream` batches into this
    /// handle; `MarketHistoryWorker` owns the actual `MarketHistoryStore`s.
    #[cfg(test)]
    pub(crate) fn set_market_history_handle(&mut self, handle: MarketHistoryHandle) {
        self.owned_market_history = None;
        self.market_history_auto_enabled = false;
        handle.set_eps_profile(self.eps_profile);
        self.market_history = Some(handle);
        self.last_market_history_scope = None;
        self.last_market_history_markets_version = None;
        self.sync_market_history_storage();
    }

    #[cfg(test)]
    pub(crate) fn market_history_readers(&self, market_name: &str) -> Option<MarketHistoryReaders> {
        self.market_history.as_ref()?.try_readers(market_name)
    }

    #[cfg(test)]
    pub(crate) fn flush_market_history(&self, now_time: crate::MoonTime) -> bool {
        self.market_history
            .as_ref()
            .is_some_and(|handle| handle.flush(now_time))
    }

    /// Apply a full `emk_RequestCandlesData` snapshot to retained Active Lib
    /// candle storage. The dispatcher keeps the same trades subscription scope:
    /// if trades storage is disabled or the market is outside
    /// `subscribe_trades_for`, the snapshot row is ignored.
    pub(crate) fn apply_candles_snapshot(
        &mut self,
        markets: &[crate::commands::candles::RequestCandlesMarket],
        now_ms: i64,
        #[cfg(any(test, feature = "diagnostics"))] metrics: Option<&ProtocolMetrics>,
    ) -> Option<crate::state::CandlesSnapshotApplySummary> {
        #[cfg(any(test, feature = "diagnostics"))]
        let sync_start = Instant::now();
        self.sync_market_history_storage();
        #[cfg(any(test, feature = "diagnostics"))]
        record_candles_snapshot_profile(
            metrics,
            ProfilePhase::CandlesSnapshotSync,
            sync_start.elapsed(),
            markets.len(),
        );
        let Some(handle) = &self.market_history else {
            return None;
        };
        let received_markets = markets.len();
        let received_candles = markets.iter().map(|market| market.candles_5m.len()).sum();
        let now_time = crate::MoonTime::now();
        #[cfg(any(test, feature = "diagnostics"))]
        let build_rows_start = Instant::now();
        let mut rows = Vec::new();
        let mut baselines = Vec::new();
        rows.try_reserve(markets.len()).ok()?;
        baselines.try_reserve(markets.len()).ok()?;
        for market in markets {
            if !self.active_trade_storage_allows_market(&market.market_name) {
                continue;
            }
            let (candles_5m, baseline) =
                build_candle_rows_and_baseline(&market.candles_5m, now_time);
            rows.push(MarketHistoryCandlesSnapshot {
                market_name: market.market_name.clone(),
                candles_5m,
            });
            baselines.push(baseline);
        }
        let retained_markets = rows.len();
        let retained_candles = rows.iter().map(|market| market.candles_5m.len()).sum();
        #[cfg(any(test, feature = "diagnostics"))]
        record_candles_snapshot_profile(
            metrics,
            ProfilePhase::CandlesSnapshotBuildRows,
            build_rows_start.elapsed(),
            retained_candles,
        );
        #[cfg(any(test, feature = "diagnostics"))]
        let baselines_start = Instant::now();
        self.markets.apply_candles_delta_baselines_precomputed(
            rows.iter()
                .zip(baselines.iter().copied())
                .filter_map(|(market, baseline)| {
                    baseline.map(|baseline| (market.market_name.as_str(), baseline))
                }),
            now_ms,
        );
        #[cfg(any(test, feature = "diagnostics"))]
        record_candles_snapshot_profile(
            metrics,
            ProfilePhase::CandlesSnapshotBaselines,
            baselines_start.elapsed(),
            retained_candles,
        );
        let summary = crate::state::CandlesSnapshotApplySummary {
            received_markets,
            received_candles,
            retained_markets,
            retained_candles,
        };
        #[cfg(any(test, feature = "diagnostics"))]
        let queue_start = Instant::now();
        let queued = rows.is_empty() || handle.apply_candles_snapshot(now_time, rows);
        #[cfg(any(test, feature = "diagnostics"))]
        record_candles_snapshot_profile(
            metrics,
            ProfilePhase::CandlesSnapshotQueue,
            queue_start.elapsed(),
            retained_markets,
        );
        if queued {
            Some(summary)
        } else {
            None
        }
    }

    pub(crate) fn market_history_barrier_async(&self) -> Option<std::sync::mpsc::Receiver<()>> {
        self.market_history.as_ref()?.barrier_async()
    }

    pub(crate) fn queue_candles_snapshot_event(
        &mut self,
        event: crate::state::CandlesSnapshotEvent,
    ) {
        self.queued_events
            .extend([crate::events::Event::CandlesSnapshot(event)]);
    }

    pub(crate) fn set_trade_storage_scope(
        &mut self,
        scope: Option<&TradeStorageScope>,
        now_time_days: f64,
    ) {
        if self.trade_storage_scope.as_ref() != scope {
            self.trade_storage_scope = scope.cloned();
            self.last_market_history_scope = None;
            self.sync_market_history_storage();
            if self.trade_storage_scope.is_some() {
                self.queue_current_last_price_history(now_time_days);
            }
        }
    }

    fn ensure_default_market_history_worker(&mut self, active_market_count: usize) {
        if self.trade_storage_scope.is_none() {
            if self.owned_market_history.is_some() {
                self.market_history = None;
                self.owned_market_history = None;
                self.last_market_history_scope = None;
                self.last_market_history_markets_version = None;
            }
            return;
        }
        if active_market_count == 0 {
            return;
        }
        if !self.market_history_auto_enabled || self.market_history.is_some() {
            return;
        }
        let worker =
            MarketHistoryWorker::spawn(self.market_history_sizing.resolve(active_market_count));
        self.market_history = Some(worker.handle());
        self.owned_market_history = Some(worker);
        if let Some(handle) = &self.market_history {
            handle.set_eps_profile(self.eps_profile);
        }
        self.last_market_history_scope = None;
        self.last_market_history_markets_version = None;
    }

    fn market_history_market_slots(&self) -> Vec<Option<Arc<str>>> {
        if self.markets.indexes_synchronized && !self.markets.market_indexes.is_empty() {
            return self
                .markets
                .market_indexes
                .iter()
                .map(|name| {
                    self.markets
                        .handles_by_name
                        .get(name.as_str())
                        .map(|handle| handle.name_arc())
                })
                .collect();
        }
        self.markets
            .markets
            .iter()
            .map(|market| Some(market.name_arc()))
            .collect()
    }

    fn active_market_history_market_count(&self, market_slots: &[Option<Arc<str>>]) -> usize {
        let Some(scope) = self.trade_storage_scope.as_ref() else {
            return 0;
        };
        let mut names = HashSet::new();
        for market_name in market_slots.iter().filter_map(Option::as_deref) {
            if scope.contains(market_name) {
                names.insert(market_name);
            }
        }
        names.len()
    }

    pub(super) fn sync_market_history_storage(&mut self) {
        let markets_version = self.markets.markets_version();
        if self.last_market_history_scope == self.trade_storage_scope
            && self.last_market_history_markets_version == Some(markets_version)
        {
            return;
        }
        let market_slots = self.market_history_market_slots();
        let active_market_count = self.active_market_history_market_count(&market_slots);
        self.ensure_default_market_history_worker(active_market_count);
        let Some(handle) = &self.market_history else {
            return;
        };
        handle.configure_market_index_slots(market_slots, self.trade_storage_scope.clone());
        self.last_market_history_scope = self.trade_storage_scope.clone();
        self.last_market_history_markets_version = Some(markets_version);
    }

    pub(super) fn active_trade_storage_allows_market(&self, market_name: &str) -> bool {
        self.trade_storage_scope
            .as_ref()
            .is_some_and(|scope| scope.contains(market_name))
    }

    pub(super) fn trade_section_visible_to_active_lib(&self, market_name: &str) -> bool {
        self.trade_storage_scope
            .as_ref()
            .is_none_or(|scope| scope.contains(market_name))
    }
}

fn build_candle_rows_and_baseline(
    candles: &[crate::commands::candles::DeepPrice],
    now_time: crate::MoonTime,
) -> (Vec<Candle5mRow>, Option<CandleDeltaBaseline>) {
    let mut rows = Vec::with_capacity(candles.len());
    let can_build_baseline = candles.len() >= 3 && candles.last().is_some_and(|c| c.time > 0.0);
    let now_ms = now_time.unix_millis();
    let mut coin_1h_sum = 0.0;
    let mut coin_1h_count = 0usize;
    let mut coin_24h_sum = 0.0;
    let mut coin_24h_count = 0usize;
    let mut btc_72h_sum = 0.0;
    let mut btc_72h_count = 0usize;

    for source in candles.iter().copied() {
        let row = Candle5mRow::from_deep_price(source);
        if can_build_baseline && row.time != crate::MoonTime::ZERO {
            let age_ms = now_ms - row.time.unix_millis();
            if age_ms >= 0 {
                let h = age_ms / MILLIS_PER_HOUR;
                let mean = f64::from(row.open + row.close + row.high + row.low) * 0.25;
                if h == 0 {
                    coin_1h_sum += mean;
                    coin_1h_count += 1;
                }
                if h <= 24 {
                    coin_24h_sum += mean;
                    coin_24h_count += 1;
                }
                if h < 72 {
                    btc_72h_sum += mean;
                    btc_72h_count += 1;
                }
            }
        }
        rows.push(row);
    }

    let baseline = can_build_baseline.then(|| CandleDeltaBaseline {
        coin_1h_avg: avg_or_zero(coin_1h_sum, coin_1h_count),
        coin_24h_avg: avg_or_zero(coin_24h_sum, coin_24h_count),
        btc_1h_avg: avg_or_zero(coin_1h_sum, coin_1h_count),
        btc_24h_avg: avg_or_zero(coin_24h_sum, coin_24h_count),
        btc_72h_avg: avg_or_zero(btc_72h_sum, btc_72h_count),
    });
    (rows, baseline)
}

fn avg_or_zero(sum: f64, count: usize) -> f64 {
    if count == 0 {
        0.0
    } else {
        sum / count as f64
    }
}

#[cfg(any(test, feature = "diagnostics"))]
fn record_candles_snapshot_profile(
    metrics: Option<&ProtocolMetrics>,
    phase: ProfilePhase,
    duration: Duration,
    payload_len: usize,
) {
    if let Some(metrics) = metrics {
        metrics.record_profile_phase_labeled(phase, duration, u8::MAX, u8::MAX, payload_len);
    }
}
