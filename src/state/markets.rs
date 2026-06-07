//! Markets sync state maintained from Engine API responses.
//!
//! Delphi source: `MarketsU.pas` (`TMarket`, `TCorrMarket`) plus
//! `MoonProtoEngineServer.pas`.
//!
//! Update flow:
//! - startup sends `emk_GetMarketsList` and receives the full markets plus CorrMarkets list;
//! - periodic `emk_UpdateMarketsList` updates prices and funding;
//! - cold init derives server `mIndex` order from `emk_GetMarketsList`;
//! - `emk_GetMarketsIndexes` refreshes that order after reconnect/server restart;
//! - periodic `emk_CheckBinanceTags` updates token tags.

use std::collections::HashMap;
use std::sync::Arc;

use crate::commands::market::{BaseCurrency, CorrMarket, Market, MarketTokenTags, TokenTags};
use crate::state::eps::EpsProfile;

mod accessors;
mod arb;
mod balances;
mod currency;
mod indexes;
mod list;
mod prices;
mod tags;
mod text;
mod types;

use self::text::{contains_text_ascii, same_text_ascii, starts_text_ascii};
pub(crate) use self::types::MarketLastPriceHistoryInput;
#[cfg(feature = "diagnostics")]
#[doc(hidden)]
pub use self::types::MarketsListApplyTiming;
#[cfg(not(feature = "diagnostics"))]
pub(crate) use self::types::MarketsListApplyTiming;
pub use self::types::{
    BaseCurrencyPrice, MarketBalancePosition, MarketGlobalDeltas, MarketHandle, MarketsEvent,
};
// The live trade tail and price now live on the `Market` object itself (Delphi
// `TMarket` shape); re-export them here so the public `state::markets` path is stable.
pub use crate::commands::market::{MarketDeltaState, MarketPrice, MarketTradeState};

#[derive(Debug, Clone, Default)]
pub struct MarketsState {
    /// Markets in `mIndex` order as received from `emk_GetMarketsList`.
    ///
    /// Each item is a stable `MarketHandle`, matching Delphi `TMarket` object
    /// references stored in `TMarkets = TSlowSafeList<TMarket>`.
    pub(crate) markets: Arc<Vec<MarketHandle>>,
    /// `market_name` -> index in `markets` for internal parallel arrays.
    pub(crate) by_name: Arc<HashMap<String, usize>>,
    /// COW `market_name` → stable handle lookup exposed by [`Self::get`].
    pub(crate) handles_by_name: Arc<HashMap<String, MarketHandle>>,
    /// COW `market_name` -> current server mIndex.
    ///
    /// Delphi rebuilds `SrvMarkets` as an index array of `TMarket` references
    /// after `GetMarketsIndexes`; this reverse map keeps the public
    /// name-to-index helper O(1) instead of scanning the whole index vector.
    pub(crate) market_index_by_name: Arc<HashMap<String, u16>>,
    /// Correlation markets used for BTC/reference calculations, keyed by `bn_market_name`.
    ///
    /// Fat per-market/aggregate fields are `Arc`-wrapped (like `by_name`,
    /// `markets`, `market_indexes` above) so a `&mut` apply that only touches one
    /// of them does not deep-clone the rest on copy-on-write. A published snapshot
    /// keeps `MarketsState` at refcount >= 2, so without this an
    /// `emk_UpdateMarketsList` price apply would clone `token_tags` (one entry per
    /// market) and the correlation maps even though it never mutates them.
    pub(crate) corr_markets: Arc<HashMap<String, CorrMarket>>,
    /// Current CorrMarket prices keyed by `bn_market_name`.
    pub(crate) corr_prices: Arc<HashMap<String, f64>>,
    /// Delphi `BaseCurDict`: base currency name -> price/ref state.
    pub(crate) base_currency_prices: Arc<HashMap<String, BaseCurrencyPrice>>,
    /// Delphi `TMarket.refBTCMarket`, represented as market name -> CorrMarket name.
    pub(crate) ref_btc_corr_markets: Arc<HashMap<String, String>>,
    /// Token tags keyed by `market_name`.
    pub(crate) token_tags: Arc<HashMap<String, TokenTags>>,
    /// Canonical `mIndex` -> market name mapping.
    ///
    /// Cold init fills this from `emk_GetMarketsList`, matching Delphi
    /// `TMoonProtoEngine.GetMarketsList -> SrvMarkets.Rebuild(IndexMap)`.
    /// After reconnect/server restart it is refreshed by `emk_GetMarketsIndexes`.
    pub(crate) market_indexes: Arc<Vec<String>>,
    /// True when the server-index map belongs to the current `PeerAppToken`.
    ///
    /// After a server restart, market indexes may change. Until fresh indexes
    /// arrive, `EventDispatcher` drops `TradesStream` and `OrderBook` packets so
    /// new server indexes cannot corrupt the old local map. This mirrors Delphi
    /// `MoonProtoEngine.pas:1580`.
    pub(crate) indexes_synchronized: bool,
    /// Delphi `NewMarketFound` analogue: set when a price row points at a server
    /// market index/name that is not present in the current market list.
    ///
    /// It is intentionally kept true after scheduling `GetMarketsList` and is
    /// cleared only by a successful list apply, matching Delphi's synchronous
    /// `Engine.GetMarketsList()` path.
    pub(crate) markets_list_refresh_needed: bool,
    /// Delphi `ES_MaxLevInGetMarkets in EngineProp`: existing markets copy
    /// `MaxLeverage` from `GetMarketsList` only for platforms that set this
    /// support flag. New markets still receive the incoming value because they
    /// are inserted as whole `TMarket` objects.
    copy_max_leverage_from_markets_list: bool,
    /// Count of markets newly added by the last successful `NewMarketFound`
    /// list refresh. Active dispatcher consumes this to request immediate
    /// `UpdateMarketsList`, like Delphi `Engine.NewMarkets.Count > 0`.
    new_markets_pending_price_refresh: usize,
    /// Names of markets inserted by the last successful listing refresh.
    ///
    /// This is emitted by the active dispatcher as a user-facing
    /// `MarketsEvent::NewMarketsAdded` after the market list state is already
    /// updated.
    new_markets_added: Vec<String>,
    /// Monotonic marker for changes to the retained market-name universe.
    ///
    /// Active history storage uses this to avoid cloning all market names on
    /// every packet. Price/tag updates do not change it.
    markets_version: u64,
    server_base_currency_name: Option<String>,
    server_base_currency_code: Option<BaseCurrency>,
    last_markets_list_timing: Option<MarketsListApplyTiming>,
    eps_profile: EpsProfile,
    global_deltas: MarketGlobalDeltas,
    last_update_delta500_ms: i64,
    coin_blacklist: Arc<Vec<String>>,
    exclude_blacklisted_markets_from_exchange_delta: bool,
}

impl MarketsState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply the Delphi `ProcessTradesStream` live market tail side effects for
    /// one already-known trade row.
    ///
    /// Gap tracking remains in `TradesState`. This mirrors only the bounded
    /// per-market tail fields: futures trades call the `SetLastTradePrices`
    /// tail and update `LastGotAllTrades`; spot trades update only
    /// `LastGotSpotTrades`.
    // parity: MoonBot MoonProtoEngine.pas:ProcessTradesStream (per-market live tail)
    pub(crate) fn apply_trade_tail_row(
        &self,
        market_index: u16,
        is_spot: bool,
        price: f32,
        qty: f32,
        now_ms: i64,
    ) {
        if !self.indexes_synchronized {
            return;
        }
        let Some(name) = self
            .market_indexes
            .get(market_index as usize)
            .map(String::as_str)
        else {
            return;
        };
        // Mutate the live `TMarket` trade tail in place through its own lock.
        // `&self` here is deliberate: the trades datagram must not trigger a
        // copy-on-write clone of the whole `MarketsState`, exactly like the
        // per-market balance apply path. The market objects are structurally
        // shared with any published snapshot, matching Delphi's shared `TMarket`.
        let Some(handle) = self.handles_by_name.get(name) else {
            return;
        };
        let eps = self.eps_profile.eps;
        handle.with_mut(|market| {
            if is_spot {
                market.trade_tail.apply_spot_trade(now_ms);
            } else {
                market.trade_tail.apply_futures_trade(
                    f64::from(price),
                    f64::from(qty),
                    now_ms,
                    eps,
                );
            }
        });
    }

    pub(crate) fn set_eps_profile(&mut self, eps_profile: EpsProfile) {
        self.eps_profile = eps_profile;
    }

    pub fn markets_list_refresh_needed(&self) -> bool {
        self.markets_list_refresh_needed
    }

    pub(crate) fn take_new_markets_pending_price_refresh(&mut self) -> usize {
        let count = self.new_markets_pending_price_refresh;
        self.new_markets_pending_price_refresh = 0;
        count
    }

    pub(crate) fn take_new_markets_added(&mut self) -> Vec<String> {
        std::mem::take(&mut self.new_markets_added)
    }

    pub(crate) fn markets_version(&self) -> u64 {
        self.markets_version
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn last_markets_list_apply_timing(&self) -> Option<MarketsListApplyTiming> {
        self.last_markets_list_timing
    }

    pub(crate) fn set_copy_max_leverage_from_markets_list(&mut self, enabled: bool) {
        self.copy_max_leverage_from_markets_list = enabled;
    }
}

#[cfg(test)]
mod tests;
