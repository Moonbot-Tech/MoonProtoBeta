//! Markets sync state — snapshot маркетов, поддерживается через Engine API ответы.
//!
//! Источник Delphi: `MarketsU.pas` (TMarket, TCorrMarket) + `MoonProtoEngineServer.pas`.
//!
//! ## Поток обновлений
//! - При запуске клиент шлёт `emk_GetMarketsList` → получает полный список (Markets + CorrMarkets).
//! - Периодически (~2 секунды по Delphi worker cadence) `emk_UpdateMarketsList` → обновление цен/funding.
//! - `emk_GetMarketsIndexes` → имена в порядке индексов (mIndex).
//! - Периодически (~60 секунд + hourly burst) `emk_CheckBinanceTags` → теги монет.

use std::collections::HashMap;
use std::sync::Arc;

use crate::commands::market::{CorrMarket, Market, MarketTokenTags, TokenTags};
const EPS_MARKET: f64 = 1e-12;

mod accessors;
mod currency;
mod indexes;
mod list;
mod prices;
mod tags;
mod text;
mod types;

use self::text::same_text_ascii;
pub(crate) use self::types::MarketLastPriceHistoryInput;
pub use self::types::{
    BaseCurrencyPrice, MarketHandle, MarketPrice, MarketTradeState, MarketsEvent,
    MarketsListApplyTiming,
};

#[derive(Debug, Clone, Default)]
pub struct MarketsState {
    /// Маркеты в порядке `mIndex` (как они приходят в `emk_GetMarketsList`).
    ///
    /// Each item is a stable `MarketHandle`, matching Delphi `TMarket` object
    /// references stored in `TMarkets = TSlowSafeList<TMarket>`.
    pub(crate) markets: Arc<Vec<MarketHandle>>,
    /// `market_name` → индекс в `markets` (internal fast lookup for parallel arrays).
    pub(crate) by_name: HashMap<String, usize>,
    /// COW `market_name` → stable handle lookup exposed by [`Self::get`].
    pub(crate) handles_by_name: Arc<HashMap<String, MarketHandle>>,
    /// Корреляционные маркеты (BTC-маркеты для расчётов), key = `bn_market_name`.
    pub(crate) corr_markets: HashMap<String, CorrMarket>,
    /// Цены маркетов по `mIndex` (параллельный массив, обновляется prices apply).
    pub(crate) prices: Vec<MarketPrice>,
    /// Текущие цены CorrMarkets, key = `bn_market_name`.
    pub(crate) corr_prices: HashMap<String, f64>,
    /// Delphi `BaseCurDict`: base currency name -> price/ref state.
    pub(crate) base_currency_prices: HashMap<String, BaseCurrencyPrice>,
    /// Delphi `TMarket.refBTCMarket`, represented as market name -> CorrMarket name.
    pub(crate) ref_btc_corr_markets: HashMap<String, String>,
    /// Live trade tail state keyed by `bn_market_name`.
    ///
    /// Delphi stores these fields directly on `TMarket`; Rust keeps the wire
    /// market snapshot clean and stores the non-wire live tail here.
    pub(crate) trade_states: HashMap<String, MarketTradeState>,
    /// Теги монет, key = `market_name`.
    pub(crate) token_tags: HashMap<String, TokenTags>,
    /// Канонический mIndex → имя маркета (из `emk_GetMarketsIndexes`).
    pub(crate) market_indexes: Vec<String>,
    /// `true` если последняя пачка `emk_GetMarketsIndexes` была получена для текущего
    /// `PeerAppToken`. При server-restart (`PeerAppToken` сменился) Client сбрасывает в
    /// `false` и отправляет fresh `api_get_markets_indexes()`. До получения ответа
    /// `EventDispatcher` дропает входящие `TradesStream` / `OrderBook` пакеты — они
    /// несут market_idx по новой нумерации, локальные state ещё знают старую.
    ///
    /// Аналог Delphi `MoonProtoEngine.pas:1580 If FLastServerAppToken <> PeerAppToken then exit`.
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
    server_base_currency_code: Option<u8>,
    last_markets_list_timing: Option<MarketsListApplyTiming>,
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
    pub(crate) fn apply_trade_tail_row_like_delphi(
        &mut self,
        market_index: u16,
        is_spot: bool,
        price: f32,
        qty: f32,
        now_ms: i64,
    ) {
        let Some(name) = self.market_name_by_index(market_index).map(str::to_owned) else {
            return;
        };
        if !self.by_name.contains_key(&name) {
            return;
        }
        let state = self.trade_states.entry(name).or_default();
        if is_spot {
            state.apply_spot_trade_like_delphi(now_ms);
        } else {
            state.apply_futures_trade_like_delphi(f64::from(price), f64::from(qty), now_ms);
        }
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

    pub fn last_markets_list_apply_timing(&self) -> Option<MarketsListApplyTiming> {
        self.last_markets_list_timing
    }

    pub(crate) fn set_copy_max_leverage_from_markets_list(&mut self, enabled: bool) {
        self.copy_max_leverage_from_markets_list = enabled;
    }
}

#[cfg(test)]
mod tests;
