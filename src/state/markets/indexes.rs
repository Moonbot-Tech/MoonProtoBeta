//! Server market-index mapping and stale-index gates.

use super::{MarketsEvent, MarketsState, EPS_MARKET};

impl MarketsState {
    /// Применить ответ `emk_GetMarketsIndexes`.
    /// Помечает `indexes_synchronized = true` — после этого EventDispatcher разблокирует
    /// обработку TradesStream / OrderBook пакетов.
    pub fn apply_markets_indexes(&mut self, names: Vec<String>) -> MarketsEvent {
        let count = names.len();
        self.market_indexes = std::sync::Arc::new(names);
        self.indexes_synchronized = true;
        MarketsEvent::IndexesUpdated { count }
    }

    /// Mark current market indexes as stale after server process restart.
    ///
    /// The old `market_indexes` vector is intentionally kept for diagnostics and for
    /// consumers that need to show the last known mapping, but stream parsing must be
    /// gated until a fresh `emk_GetMarketsIndexes` response arrives.
    pub(crate) fn mark_indexes_stale(&mut self) {
        self.indexes_synchronized = false;
    }

    pub(crate) fn has_server_market_index(&self, m_index: u16) -> bool {
        if !self.indexes_synchronized {
            return false;
        }
        self.market_indexes
            .get(m_index as usize)
            .is_some_and(|name| self.by_name.contains_key(name))
    }

    pub(super) fn local_pos_for_server_index(&self, m_index: u16) -> Option<usize> {
        let server_pos = m_index as usize;
        if self.indexes_synchronized {
            let market_name = self.market_indexes.get(server_pos)?;
            return self.by_name.get(market_name).copied();
        }

        // Cold-start compatibility: before the first explicit indexes response,
        // `GetMarketsList` arrives in server order. Once a mapping exists but is
        // marked stale, direct fallback would silently apply prices to old names.
        if self.market_indexes.is_empty() && server_pos < self.prices.len() {
            Some(server_pos)
        } else {
            None
        }
    }

    pub(super) fn price_row_points_to_missing_market(&self, m_index: u16) -> bool {
        let server_pos = m_index as usize;
        if self.indexes_synchronized {
            return self
                .market_indexes
                .get(server_pos)
                .is_none_or(|name| !self.by_name.contains_key(name));
        }
        self.market_indexes.is_empty() && server_pos >= self.prices.len()
    }

    /// Delphi `AddNewAksPrice` from `GlassUpdated`: keep `ChartPriceStep` fresh
    /// when orderbook updates move the best ask before the next price refresh.
    pub(crate) fn update_chart_price_step_from_server_index(
        &mut self,
        m_index: u16,
        ask: f64,
    ) -> bool {
        if ask <= EPS_MARKET {
            return false;
        }
        let Some(idx) = self.local_pos_for_server_index(m_index) else {
            return false;
        };
        let Some(slot) = self.prices.get_mut(idx) else {
            return false;
        };
        slot.chart_price_step = EPS_MARKET.max(ask / 5000.0);
        true
    }
}
