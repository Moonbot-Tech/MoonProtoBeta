//! Server market-index mapping and stale-index gates.

use std::collections::HashMap;
use std::sync::Arc;

use super::{MarketsEvent, MarketsState};

impl MarketsState {
    /// Apply the `emk_GetMarketsIndexes` response.
    /// Sets `indexes_synchronized = true` — after this the EventDispatcher unblocks
    /// processing of TradesStream / OrderBook packets.
    pub fn apply_markets_indexes(&mut self, names: Vec<String>) -> MarketsEvent {
        let count = names.len();
        self.replace_market_indexes_like_delphi_cow(names);
        self.indexes_synchronized = true;
        MarketsEvent::IndexesUpdated { count }
    }

    pub(super) fn replace_market_indexes_like_delphi_cow(&mut self, names: Vec<String>) {
        let mut by_name = HashMap::with_capacity(names.len());
        for (idx, name) in names.iter().enumerate() {
            if let Ok(idx) = u16::try_from(idx) {
                by_name.insert(name.clone(), idx);
            }
        }
        self.market_indexes = Arc::new(names);
        self.market_index_by_name = Arc::new(by_name);
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
        if self.market_indexes.is_empty() && server_pos < self.markets.len() {
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
        self.market_indexes.is_empty() && server_pos >= self.markets.len()
    }

    /// Delphi `AddNewAksPrice` from `GlassUpdated`: keep `ChartPriceStep` fresh
    /// when orderbook updates move the best ask before the next price refresh.
    pub(crate) fn update_chart_price_step_from_server_index(&self, m_index: u16, ask: f64) -> bool {
        // Delphi `AddNewAksPrice` (MarketsU.pas:8510,8516) gates and computes
        // ChartPriceStep against `_epsM`, not `_eps`.
        if ask <= self.eps_profile.eps_m {
            return false;
        }
        let Some(idx) = self.local_pos_for_server_index(m_index) else {
            return false;
        };
        let Some(handle) = self.markets.get(idx) else {
            return false;
        };
        // `&self` + per-market `with_mut`: an order-book datagram must not trigger
        // a copy-on-write clone of the markets container. The price lives on the
        // shared `Market` object (Delphi `TMarket.ChartPriceStep`).
        let eps_m = self.eps_profile.eps_m;
        handle.with_mut(|market| {
            market.price.chart_price_step = eps_m.max(ask / 5000.0);
        });
        true
    }
}
