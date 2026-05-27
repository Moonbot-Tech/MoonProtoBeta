//! Market read-model types and stable handles.

use std::sync::Arc;

use parking_lot::RwLock;

use crate::commands::market::Market;

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

    /// True when two handles point at the same live market object.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub(super) fn with_mut<R>(&self, f: impl FnOnce(&mut Market) -> R) -> R {
        let mut market = self.inner.write();
        f(&mut market)
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

/// Per-market price snapshot (обновляется через `emk_UpdateMarketsList`).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct MarketPrice {
    /// Лучшая цена покупки (top of bid side).
    pub bid: f64,
    /// Лучшая цена продажи (top of ask side).
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
    /// Funding rate (для perpetual futures), дробь — например `0.0001` = 0.01%.
    pub funding_rate: f64,
    /// Client-local Delphi `TDateTime` момента следующего funding взимания.
    pub funding_time: f64,
    /// Mark price (используется биржей для PnL/liquidation расчётов, может отличаться от last/bid/ask).
    pub mark_price: f64,
    /// Был ли получен mark_price в последнем апдейте (биржи могут не присылать на каждом тике).
    pub mark_price_found: bool,
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
    /// Применён список маркетов (после `emk_GetMarketsList`).
    /// Variant name is historical; repeated calls merge like Delphi.
    MarketsListReplaced { count: usize, corr_count: usize },
    /// A listing refresh inserted new markets into the local market universe.
    ///
    /// Emitted only after the refreshed `GetMarketsList` has actually added
    /// markets. `TNewMarketNotifyCommand` itself is internal and only forces
    /// that refresh.
    NewMarketsAdded { names: Vec<String> },
    /// Обновлены цены (через `emk_UpdateMarketsList`).
    PricesUpdated {
        count: usize,
        included_funding: bool,
        included_corr: bool,
    },
    /// Получен список имён маркетов (`emk_GetMarketsIndexes`).
    IndexesUpdated { count: usize },
    /// Обновлены теги монет (`emk_CheckBinanceTags`).
    TokenTagsUpdated { count: usize },
}
