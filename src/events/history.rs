//! Retained-history worker wiring for `EventDispatcher`.

use super::*;
use std::collections::HashSet;

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
    ) -> Option<crate::state::CandlesSnapshotApplySummary> {
        self.sync_market_history_storage();
        let Some(handle) = &self.market_history else {
            return None;
        };
        let received_markets = markets.len();
        let received_candles = markets.iter().map(|market| market.candles_5m.len()).sum();
        let now_time = crate::MoonTime::now();
        let retained_source = markets
            .iter()
            .filter(|market| self.active_trade_storage_allows_market(&market.market_name))
            .collect::<Vec<_>>();
        self.markets.apply_candles_delta_baselines(
            retained_source
                .iter()
                .map(|market| (market.market_name.as_str(), market.candles_5m.as_slice())),
            now_time,
            now_ms,
        );
        let rows = retained_source
            .iter()
            .map(|market| MarketHistoryCandlesSnapshot {
                market_name: market.market_name.clone(),
                candles_5m: market
                    .candles_5m
                    .iter()
                    .copied()
                    .map(Candle5mRow::from_deep_price)
                    .collect(),
            })
            .collect::<Vec<_>>();
        let retained_markets = rows.len();
        let retained_candles = rows.iter().map(|market| market.candles_5m.len()).sum();
        let summary = crate::state::CandlesSnapshotApplySummary {
            received_markets,
            received_candles,
            retained_markets,
            retained_candles,
        };
        if rows.is_empty() || handle.apply_candles_snapshot(rows) {
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
