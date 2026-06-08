//! `MarketsState` read-model accessors.

use super::*;
use std::sync::Arc;

impl MarketsState {
    /// Iterate stable market handles in current server-index order.
    ///
    /// The returned handles may be kept across listing refreshes. The enclosing
    /// list/dictionaries are COW-replaced, while each handle points at the live
    /// market object.
    pub fn iter(&self) -> impl Iterator<Item = &MarketHandle> {
        self.markets.iter()
    }

    /// Current server-index mapping is fresh for the active server token.
    pub fn indexes_synchronized(&self) -> bool {
        self.indexes_synchronized
    }

    /// Current server `mIndex -> market_name` mapping.
    ///
    /// This is empty before Init completes and may be stale only while
    /// [`Self::indexes_synchronized`] is false.
    #[cfg(test)]
    pub(crate) fn market_index_names(&self) -> &[String] {
        self.market_indexes.as_slice()
    }

    /// Get a stable market handle by name.
    ///
    /// The handle remains valid after listing refresh because the surrounding
    /// list/dictionaries are COW-replaced while the market object itself stays
    /// alive and is mutated in place.
    pub fn get(&self, market_name: &str) -> Option<MarketHandle> {
        self.handles_by_name.get(market_name).cloned()
    }

    /// Find the best market for terminal search-box input.
    ///
    /// This is the normal Active Lib chart-search flow: users may type a full
    /// market name (`BTCUSDT`) or a coin/token symbol (`BTC`, `SOL`). The
    /// result is a stable [`MarketHandle`] that UI can keep for the selected
    /// chart instead of re-searching by name on every frame.
    pub fn find(&self, input: &str) -> Option<MarketHandle> {
        self.search(input, 1).into_iter().next()
    }

    /// Search markets for popup/autocomplete UI.
    ///
    /// Ordering is stable in current market-list order:
    /// exact symbol/name/canonical matches first, then prefix matches, then
    /// contains matches. `limit = 0` returns an empty vector.
    pub fn search(&self, input: &str, limit: usize) -> Vec<MarketHandle> {
        if limit == 0 {
            return Vec::new();
        }
        let query = input.trim();
        if query.is_empty() {
            return Vec::new();
        }

        let mut exact = Vec::new();
        let mut prefix = Vec::new();
        let mut contains = Vec::new();
        for handle in self.markets.iter() {
            match handle.with(|market| market_search_quality(market, query)) {
                MarketSearchQuality::Exact => exact.push(handle.clone()),
                MarketSearchQuality::Prefix => prefix.push(handle.clone()),
                MarketSearchQuality::Contains => contains.push(handle.clone()),
                MarketSearchQuality::None => {}
            }
        }

        let mut out = Vec::with_capacity(limit.min(exact.len() + prefix.len() + contains.len()));
        append_limited(&mut out, exact, limit);
        append_limited(&mut out, prefix, limit);
        append_limited(&mut out, contains, limit);
        out
    }

    /// Get an owned market snapshot by name.
    pub fn market_snapshot(&self, market_name: &str) -> Option<Market> {
        self.get(market_name).map(|handle| handle.snapshot())
    }

    /// Resolve a server `mIndex` into the canonical market name.
    ///
    /// Returns `None` while indexes are stale after a server restart. During
    /// that window `EventDispatcher` also gates market-index streams, so regular
    /// consumers do not see trades/orderbook events with an old mapping.
    pub(crate) fn market_name_by_index(&self, m_index: u16) -> Option<&str> {
        if !self.indexes_synchronized {
            return None;
        }
        self.market_indexes
            .get(m_index as usize)
            .map(String::as_str)
    }

    /// Resolve a server `mIndex` into a stable market handle.
    pub(crate) fn market_by_index(&self, m_index: u16) -> Option<MarketHandle> {
        let name = self.market_name_by_index(m_index)?;
        self.get(name)
    }

    /// Resolve a market name into the current server `mIndex`.
    ///
    /// Uses the canonical server-index mapping rather than the `markets` vector
    /// position, because this is the index carried by stream packets.
    pub(crate) fn market_index_by_name(&self, market_name: &str) -> Option<u16> {
        if !self.indexes_synchronized {
            return None;
        }
        self.market_index_by_name.get(market_name).copied()
    }

    /// Get a market price by `mIndex`.
    ///
    /// The price lives on the retained `Market`; it is returned as a copy under
    /// a short read-lock, like [`MarketHandle::balance_position`].
    #[cfg(test)]
    pub(crate) fn price_by_index(&self, m_index: u16) -> Option<MarketPrice> {
        let idx = self.local_pos_for_server_index(m_index)?;
        self.markets.get(idx).map(|handle| handle.with(|m| m.price))
    }

    /// Get a market price by name (via the by_name lookup).
    pub fn price(&self, market_name: &str) -> Option<MarketPrice> {
        self.handles_by_name
            .get(market_name)
            .map(|handle| handle.with(|m| m.price))
    }

    /// BTC correlation-market reference for a known market.
    pub fn ref_btc_corr_market(&self, market_name: &str) -> Option<&CorrMarket> {
        let corr_name = self.ref_btc_corr_markets.get(market_name)?;
        self.corr_markets.get(corr_name)
    }

    /// Base-currency price entry for a base currency.
    pub fn base_currency_price(&self, base_currency: &str) -> Option<&BaseCurrencyPrice> {
        self.base_currency_prices.get(base_currency).or_else(|| {
            self.base_currency_prices
                .iter()
                .find(|(key, _)| same_text_ascii(key, base_currency))
                .map(|(_, value)| value)
        })
    }

    /// Live trade-tail state for a known market.
    pub fn trade_state(&self, market_name: &str) -> Option<MarketTradeState> {
        self.handles_by_name
            .get(market_name)
            .map(|handle| handle.with(|market| market.trade_tail))
    }

    /// Signed market-delta state for a known market.
    pub fn delta_state(&self, market_name: &str) -> Option<MarketDeltaState> {
        self.handles_by_name
            .get(market_name)
            .map(|handle| handle.with(|market| market.delta_state))
    }

    /// Global signed BTC/exchange deltas used by terminal blink/panic filters.
    pub fn global_deltas(&self) -> MarketGlobalDeltas {
        self.global_deltas
    }

    /// Whether blacklisted markets are excluded from `Exchange1hDelta` /
    /// `Exchange24hDelta`.
    ///
    /// This is local Active Lib policy, not a MoonProto wire field. Active Lib
    /// exposes it through
    /// `MoonSettings::set_exclude_blacklisted_markets_from_exchange_delta`.
    pub fn exclude_blacklisted_markets_from_exchange_delta(&self) -> bool {
        self.exclude_blacklisted_markets_from_exchange_delta
    }

    /// Apply the coin blacklist text used by retained market analytics.
    ///
    /// `MarketBlackListedCfg` is true whenever the market currency appears in
    /// the comma-list. This is intentionally separate from the UI/trading
    /// blacklist checkbox: the list can still be used by exchange-delta
    /// analytics even when trade filtering is disabled.
    // parity: MoonBot MarketsU.pas:TMarket.IsBlackListed
    pub(crate) fn set_coin_blacklist_text(&mut self, text: &str) -> bool {
        let parsed = parse_coin_blacklist_text(text);
        if parsed.as_slice() == self.coin_blacklist.as_slice() {
            return false;
        }
        self.coin_blacklist = Arc::new(parsed);
        self.rebuild_market_blacklisted_cfg();
        self.refresh_exchange_signed_deltas();
        true
    }

    /// Set whether exchange-delta analytics skip markets from the coin blacklist.
    // parity: MoonBot Bworks.pas Exchange1hDelta filter
    pub(crate) fn set_exclude_blacklisted_markets_from_exchange_delta(
        &mut self,
        exclude: bool,
    ) -> bool {
        if self.exclude_blacklisted_markets_from_exchange_delta == exclude {
            return false;
        }
        self.exclude_blacklisted_markets_from_exchange_delta = exclude;
        self.refresh_exchange_signed_deltas();
        true
    }

    pub(super) fn rebuild_market_blacklisted_cfg(&self) {
        for handle in self.markets.iter() {
            handle.with_mut(|market| {
                market.market_blacklisted_cfg = self
                    .coin_blacklist
                    .iter()
                    .any(|word| same_text_ascii(word, &market.market_currency));
            });
        }
    }

    /// Market tags (empty if `apply_token_tags` was never called).
    pub fn tags(&self, market_name: &str) -> TokenTags {
        self.token_tags
            .get(market_name)
            .copied()
            .unwrap_or(TokenTags::empty())
    }

    pub fn market_count(&self) -> usize {
        self.markets.len()
    }

    pub fn corr_count(&self) -> usize {
        self.corr_markets.len()
    }
}

fn parse_coin_blacklist_text(text: &str) -> Vec<String> {
    text.split(',')
        .map(str::trim)
        .filter(|word| !word.is_empty())
        .map(str::to_string)
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MarketSearchQuality {
    None,
    Contains,
    Prefix,
    Exact,
}

fn market_search_quality(market: &Market, query: &str) -> MarketSearchQuality {
    let exact_fields = [
        market.bn_market_name.as_str(),
        market.market_name.as_str(),
        market.market_name_mb_classic.as_str(),
        market.market_currency.as_str(),
        market.market_currency_canonic.as_str(),
        market.market_currency_long.as_str(),
    ];
    if exact_fields
        .iter()
        .any(|field| same_text_ascii(field, query))
    {
        return MarketSearchQuality::Exact;
    }

    let prefix_fields = [
        market.market_currency_canonic.as_str(),
        market.market_currency.as_str(),
        market.bn_market_name.as_str(),
        market.market_name.as_str(),
        market.market_name_mb_classic.as_str(),
    ];
    if prefix_fields
        .iter()
        .any(|field| starts_text_ascii(field, query))
    {
        return MarketSearchQuality::Prefix;
    }

    if prefix_fields
        .iter()
        .chain([market.market_currency_long.as_str()].iter())
        .any(|field| contains_text_ascii(field, query))
    {
        return MarketSearchQuality::Contains;
    }

    MarketSearchQuality::None
}

fn append_limited(out: &mut Vec<MarketHandle>, items: Vec<MarketHandle>, limit: usize) {
    let remaining = limit.saturating_sub(out.len());
    out.extend(items.into_iter().take(remaining));
}
