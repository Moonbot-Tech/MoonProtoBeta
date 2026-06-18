//! Market read-model types and stable handles.

use std::sync::Arc;

use parking_lot::RwLock;

use crate::commands::market::{
    ArbPlatformCode, Market, MarketArbNowEntry, MarketArbSlot, MarketDeltaState, MarketPrice,
    MarketTradeState, PositionType,
};
use crate::commands::trade::OrderType;

/// Stable handle to one retained market object.
///
/// The market universe uses a COW container model: listing refresh may replace
/// the surrounding lists/dictionaries, but existing market objects stay alive
/// and are mutated in place. Callers may keep `MarketHandle` across a listing
/// refresh and read the same live market object later.
#[derive(Debug, Clone)]
pub struct MarketHandle {
    pub(super) name: Arc<str>,
    pub(super) inner: Arc<RwLock<Market>>,
}

impl MarketHandle {
    pub(super) fn new(market: Market) -> Self {
        let name = Arc::<str>::from(market.bn_market_name.as_str());
        Self {
            name,
            inner: Arc::new(RwLock::new(market)),
        }
    }

    /// Read the current market object under a short read lock.
    pub fn with<R>(&self, f: impl FnOnce(&Market) -> R) -> R {
        let market = self.inner.read();
        f(&market)
    }

    /// Canonical market name for this stable handle.
    ///
    /// Terminal UI usually resolves a market once and keeps `MarketHandle`.
    /// This accessor lets higher-level read helpers use that handle without
    /// forcing another name lookup in UI code.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return an owned snapshot for code that does not want to hold a handle.
    pub fn snapshot(&self) -> Market {
        self.with(Clone::clone)
    }

    /// Copy only the live balance/position fields used by chart and order UI.
    ///
    /// This avoids cloning the whole `Market` object when the consumer only
    /// needs account fields such as liquidation price, position size, leverage,
    /// and PnL.
    pub fn balance_position(&self) -> MarketBalancePosition {
        self.with(MarketBalancePosition::from_market)
    }

    /// Copy the live price row used by chart, funding, and mark-price UI.
    pub fn price(&self) -> MarketPrice {
        self.with(|market| market.price)
    }

    /// Copy the live trade-tail state.
    pub fn trade_state(&self) -> MarketTradeState {
        self.with(|market| market.trade_tail)
    }

    /// Copy the signed market-delta state.
    ///
    /// This is the UI-facing counterpart of `Coin1hDelta`, `Coin1hDeltaEMA`,
    /// `Coin24hDelta`, and their retained moving averages. It is intentionally
    /// separate from `MarketDerivedSnapshot::deltas`, which stores positive
    /// min/max movement over retained history windows.
    pub fn delta_state(&self) -> MarketDeltaState {
        self.with(|market| market.delta_state)
    }

    /// Copy one arbitrage slot by platform code.
    pub fn arb_slot(&self, platform_code: ArbPlatformCode) -> Option<MarketArbSlot> {
        self.with(|market| market.arb_slots.get(&platform_code).cloned())
    }

    /// Copy the latest arbitrage price entry by platform code.
    pub fn arb_now(&self, platform_code: ArbPlatformCode) -> Option<MarketArbNowEntry> {
        self.with(|market| market.arb_slots.get(&platform_code).map(|slot| slot.now))
    }

    /// True when two handles point at the same live market object.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub(crate) fn name_arc(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }

    pub(super) fn name_str(&self) -> &str {
        &self.name
    }

    pub(super) fn with_mut<R>(&self, f: impl FnOnce(&mut Market) -> R) -> R {
        let mut market = self.inner.write();
        f(&mut market)
    }
}

/// Small copy of live market balance/position fields.
///
/// Balance packets still mutate the live `Market` object; this type is only a
/// convenience snapshot for UI code that should not clone the whole market.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct MarketBalancePosition {
    pub initial_balance: f64,
    pub locked_balance: f64,
    pub pos_size: f64,
    pub pos_price: f64,
    pub liq_price: f64,
    pub pos_dir: OrderType,
    pub long_pos_size: f64,
    pub long_pos_price: f64,
    pub long_liq_price: f64,
    pub long_position_type: PositionType,
    pub short_pos_size: f64,
    pub short_pos_price: f64,
    pub short_liq_price: f64,
    pub short_position_type: PositionType,
    pub asset_balance: f64,
    pub asset_balance_full: f64,
    pub total_profit_b: f64,
    pub total_profit_l: f64,
    pub total_profit_s: f64,
    pub max_value: f64,
    pub leverage_x: i32,
    pub position_type: PositionType,
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub balance_hash: u64,
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub last_balance_epoch: u16,
}

impl MarketBalancePosition {
    pub fn total_profit(self) -> f64 {
        self.total_profit_b + self.total_profit_l + self.total_profit_s
    }

    fn from_market(market: &Market) -> Self {
        Self {
            initial_balance: market.initial_balance,
            locked_balance: market.locked_balance,
            pos_size: market.pos_size,
            pos_price: market.pos_price,
            liq_price: market.liq_price,
            pos_dir: market.pos_dir,
            long_pos_size: market.long_pos_size,
            long_pos_price: market.long_pos_price,
            long_liq_price: market.long_liq_price,
            long_position_type: market.long_position_type,
            short_pos_size: market.short_pos_size,
            short_pos_price: market.short_pos_price,
            short_liq_price: market.short_liq_price,
            short_position_type: market.short_position_type,
            asset_balance: market.asset_balance,
            asset_balance_full: market.asset_balance_full,
            total_profit_b: market.total_profit_b,
            total_profit_l: market.total_profit_l,
            total_profit_s: market.total_profit_s,
            max_value: market.max_value(),
            leverage_x: market.leverage_x,
            position_type: market.position_type,
            #[cfg(any(test, feature = "diagnostics"))]
            balance_hash: market.balance_hash,
            #[cfg(any(test, feature = "diagnostics"))]
            last_balance_epoch: market.last_balance_epoch,
        }
    }
}

/// Last `GetMarketsList` apply phase timing.
///
/// Diagnostic only: it never gates protocol behavior. FireTest prints this when
/// investigating CPU red flags around large market-list payloads.
#[cfg(any(test, feature = "diagnostics"))]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MarketsListApplyTiming {
    pub payload_len: usize,
    pub market_count: usize,
    pub corr_count: usize,
    pub total_ns: u64,
    pub market_loop_ns: u64,
    pub index_rebuild_ns: u64,
    pub corr_loop_ns: u64,
    pub ref_passes_ns: u64,
}

/// Last `GetMarketsList` apply phase timing placeholder for normal builds.
///
/// Normal applications do not receive this diagnostic type; the apply path also
/// skips per-phase timers outside `test`/`diagnostics`.
#[cfg(not(any(test, feature = "diagnostics")))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct MarketsListApplyTiming;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MarketLastPriceHistoryInput {
    pub market_name: Arc<str>,
    pub current: f64,
    pub bid: f64,
    pub ask: f64,
    pub mark_price: f64,
    pub mark_price_found: bool,
    pub is_btc_market: bool,
    pub is_base_usdt_market: bool,
}

/// Global signed market deltas retained by Active Lib.
///
/// These are the terminal-visible `BTC1hDelta` / `Exchange1hDelta` family from
/// MoonBot. They are signed trend/deviation signals, not the positive min/max
/// `Last*Delta` movement values exposed in `MarketDerivedSnapshot`.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct MarketGlobalDeltas {
    pub btc_1h_avg: f64,
    pub btc_24h_avg: f64,
    pub btc_72h_avg: f64,
    pub btc_1h_delta: f64,
    pub btc_24h_delta: f64,
    pub btc_72h_delta: f64,
    pub exchange_1h_delta: f64,
    pub exchange_24h_delta: f64,
    pub exchange_market_count: usize,
}

/// Base-currency price row, keyed by `base_currency`.
///
/// The reference fields store market names instead of raw pointers. They are
/// assigned by the production core's base-currency reference rules and
/// intentionally are not cleared when a later scan does not find a replacement.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct BaseCurrencyPrice {
    pub base_currency: String,
    pub last_price: f64,
    pub usdt_market: Option<String>,
    pub usdt_rev_market: Option<String>,
    pub usdt_corr_market: Option<String>,
    pub usdt_rev_corr_market: Option<String>,
}

impl BaseCurrencyPrice {
    pub(super) fn new(base_currency: String) -> Self {
        Self {
            base_currency,
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone)]
pub enum MarketsEvent {
    /// A full market-list refresh was applied.
    /// Variant name is historical; repeated calls merge with current tags.
    MarketsListReplaced { count: usize, corr_count: usize },
    /// A listing refresh inserted new markets into the local market universe.
    ///
    /// Emitted only after the refreshed market list has actually added markets.
    /// The transport notification that triggered the refresh is internal; UI
    /// code should react to this retained-state event.
    NewMarketsAdded { names: Vec<String> },
    /// Live prices/funding/correlation prices were updated.
    PricesUpdated {
        count: usize,
        included_funding: bool,
        included_corr: bool,
    },
    /// The server-index market-name map was updated.
    IndexesUpdated { count: usize },
    /// Token tags were updated.
    TokenTagsUpdated { count: usize },
}
