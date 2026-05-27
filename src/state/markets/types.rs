//! Market read-model types and stable handles.

use std::sync::Arc;

use parking_lot::RwLock;

use crate::commands::market::{Market, MarketArbNowEntry, MarketArbSlot};
use crate::time::DelphiTime;

/// Stable Delphi-like handle to one `TMarket` object.
///
/// Delphi `TMarkets` stores `TMarket` object references and replaces only the
/// surrounding list/dictionaries on listing changes. Rust mirrors that with an
/// `Arc` handle: callers may keep `MarketHandle` across a listing refresh and
/// read the same live market object later.
#[derive(Debug, Clone)]
pub struct MarketHandle {
    pub(super) inner: Arc<RwLock<Market>>,
}

impl MarketHandle {
    pub(super) fn new(market: Market) -> Self {
        Self {
            inner: Arc::new(RwLock::new(market)),
        }
    }

    /// Read the current market object under a short read lock.
    pub fn with<R>(&self, f: impl FnOnce(&Market) -> R) -> R {
        let market = self.inner.read();
        f(&market)
    }

    /// Return an owned snapshot for code that does not want to hold a handle.
    pub fn snapshot(&self) -> Market {
        self.with(Clone::clone)
    }

    /// Copy only the live balance/position fields used by chart and order UI.
    ///
    /// This avoids cloning the whole `Market` object when the consumer only
    /// needs the Delphi `TMarket` account fields such as liquidation price,
    /// position size, leverage, and PnL.
    pub fn balance_position(&self) -> MarketBalancePosition {
        self.with(MarketBalancePosition::from_market)
    }

    /// Copy one arbitrage slot by platform code.
    pub fn arb_slot(&self, platform_code: u8) -> Option<MarketArbSlot> {
        self.with(|market| market.arb_slots.get(&platform_code).cloned())
    }

    /// Copy the latest arbitrage price entry by platform code.
    pub fn arb_now(&self, platform_code: u8) -> Option<MarketArbNowEntry> {
        self.with(|market| market.arb_slots.get(&platform_code).map(|slot| slot.now))
    }

    /// True when two handles point at the same live market object.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub(super) fn with_mut<R>(&self, f: impl FnOnce(&mut Market) -> R) -> R {
        let mut market = self.inner.write();
        f(&mut market)
    }
}

/// Small copy of live Delphi `TMarket` balance/position fields.
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
    pub pos_dir: u8,
    pub long_pos_size: f64,
    pub long_pos_price: f64,
    pub long_liq_price: f64,
    pub long_position_type: u8,
    pub short_pos_size: f64,
    pub short_pos_price: f64,
    pub short_liq_price: f64,
    pub short_position_type: u8,
    pub asset_balance: f64,
    pub asset_balance_full: f64,
    pub total_profit_b: f64,
    pub total_profit_l: f64,
    pub total_profit_s: f64,
    pub leverage_x: i32,
    pub position_type: u8,
    pub balance_hash: u64,
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
            leverage_x: market.leverage_x,
            position_type: market.position_type,
            balance_hash: market.balance_hash,
            last_balance_epoch: market.last_balance_epoch,
        }
    }
}

/// Last `GetMarketsList` apply phase timing.
///
/// Diagnostic only: it never gates protocol behavior. FireTest prints this when
/// investigating CPU red flags around large market-list payloads.
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

/// Per-market price snapshot updated by `emk_UpdateMarketsList`.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct MarketPrice {
    /// Best bid price.
    pub bid: f64,
    /// Best ask price.
    pub ask: f64,
    /// Delphi `TMarket.LastBid`, updated from `Bid` by `UpdateMarketsList`.
    pub last_bid: f64,
    /// Delphi `TMarket.LastAsk`, updated from `Ask` by `UpdateMarketsList`.
    pub last_ask: f64,
    /// Delphi `TMarket.pLast = (Bid + Ask) / 2`.
    pub p_last: f64,
    /// Delphi `TMarket.MinLotSize`.
    pub min_lot_size: f64,
    /// Delphi `TMarket.ChartPriceStep`, updated by `AddNewAksPrice(Ask)`.
    ///
    /// Futures retained trade join uses this value for same-price aggregation.
    /// Delphi updates it only when `Ask > eps`; otherwise the previous value is
    /// kept.
    pub chart_price_step: f64,
    /// Funding rate for perpetual futures, for example `0.0001` = 0.01%.
    pub funding_rate: f64,
    /// Client-local Delphi `TDateTime` for the next funding charge.
    pub funding_time: f64,
    /// Exchange mark price used for PnL/liquidation calculations.
    pub mark_price: f64,
    /// Whether the latest update carried a mark price.
    pub mark_price_found: bool,
}

impl MarketPrice {
    pub fn funding_time_delphi(self) -> DelphiTime {
        DelphiTime::from_days(self.funding_time)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MarketLastPriceHistoryInput {
    pub market_name: String,
    pub current: f64,
    pub bid: f64,
    pub ask: f64,
    pub is_btc_market: bool,
    pub is_base_usdt_market: bool,
}

/// Delphi `TBaseCurrencyPrice` analogue, keyed by `base_currency`.
///
/// The reference fields store market names instead of raw pointers. They are
/// assigned by the same `CheckCurrencyRefMarkets` conditions and intentionally
/// are not cleared when a later scan does not find a replacement.
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

/// Delphi `TMarket` live trade tail fields maintained from `MPC_TradesStream`.
///
/// This is intentionally separate from the wire `Market` snapshot: Delphi does
/// not send these fields in `GetMarketsList`, but it mutates them inline while
/// processing trades.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct MarketTradeState {
    /// Delphi `TMarket.LastGotAllTrades` (`GetTimeMS`) for futures trades.
    pub last_got_all_trades_ms: i64,
    /// Delphi `TMarket.LastGotSpotTrades` (`GetTimeMS`) for spot trades.
    pub last_got_spot_trades_ms: i64,
    /// Delphi `TMarket.LastTradePrice`.
    pub last_trade_price: f64,
    /// Delphi `TMarket.LastBuyPrice`; yes, Delphi updates this on `O_Sell`.
    pub last_buy_price: f64,
    /// Delphi `TMarket.LastSellPrice`; Delphi updates this on `O_Buy`.
    pub last_sell_price: f64,
    /// Delphi `TMarket.LastTradePriceEMA15`.
    pub last_trade_price_ema15: f64,
    /// Delphi `TMarket.LastTradePriceEMA5`.
    pub last_trade_price_ema5: f64,
    /// Delphi `TMarket.LastTradeKind = O_Sell`.
    pub last_trade_was_sell: bool,
}

impl MarketTradeState {
    pub(super) fn apply_futures_trade_like_delphi(
        &mut self,
        price: f64,
        qty: f64,
        now_ms: i64,
        eps: f64,
    ) {
        let is_sell = qty < 0.0;
        self.last_got_all_trades_ms = now_ms;
        self.last_trade_price = price;
        self.last_trade_was_sell = is_sell;

        if self.last_trade_price_ema15 < eps {
            self.last_trade_price_ema15 = price;
        }
        if self.last_trade_price_ema5 < eps {
            self.last_trade_price_ema5 = price;
        }
        self.last_trade_price_ema15 = (self.last_trade_price_ema15 * 15.0 + price) / 16.0;
        self.last_trade_price_ema5 = (self.last_trade_price_ema5 * 5.0 + price) / 6.0;

        if is_sell {
            self.last_buy_price = price;
        } else {
            self.last_sell_price = price;
        }
    }

    pub(super) fn apply_spot_trade_like_delphi(&mut self, now_ms: i64) {
        self.last_got_spot_trades_ms = now_ms;
    }
}

#[derive(Debug, Clone)]
pub enum MarketsEvent {
    /// A `GetMarketsList` response was applied.
    /// Variant name is historical; repeated calls merge like Delphi.
    MarketsListReplaced { count: usize, corr_count: usize },
    /// A listing refresh inserted new markets into the local market universe.
    ///
    /// Emitted only after the refreshed `GetMarketsList` has actually added
    /// markets. `TNewMarketNotifyCommand` itself is internal and only forces
    /// that refresh.
    NewMarketsAdded { names: Vec<String> },
    /// Prices were updated by `emk_UpdateMarketsList`.
    PricesUpdated {
        count: usize,
        included_funding: bool,
        included_corr: bool,
    },
    /// The server-index market-name map was updated by `emk_GetMarketsIndexes`.
    IndexesUpdated { count: usize },
    /// Token tags were updated by `emk_CheckBinanceTags`.
    TokenTagsUpdated { count: usize },
}
