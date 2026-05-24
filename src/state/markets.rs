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

use crate::commands::market::{
    CorrMarket, Market, MarketTokenTags, MarketsListResponse, MarketsPricesResponse, TokenTags,
};

const EPS_MARKET: f64 = 1e-12;

/// Per-market price snapshot (обновляется через `emk_UpdateMarketsList`).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct MarketPrice {
    /// Лучшая цена покупки (top of bid side).
    pub bid: f64,
    /// Лучшая цена продажи (top of ask side).
    pub ask: f64,
    /// Funding rate (для perpetual futures), дробь — например `0.0001` = 0.01%.
    pub funding_rate: f64,
    /// Client-local Delphi `TDateTime` момента следующего funding взимания.
    pub funding_time: f64,
    /// Mark price (используется биржей для PnL/liquidation расчётов, может отличаться от last/bid/ask).
    pub mark_price: f64,
    /// Был ли получен mark_price в последнем апдейте (биржи могут не присылать на каждом тике).
    pub mark_price_found: bool,
}

#[derive(Debug, Clone, Default)]
pub struct MarketsState {
    /// Маркеты в порядке `mIndex` (как они приходят в `emk_GetMarketsList`).
    pub markets: Vec<Market>,
    /// `market_name` → индекс в `markets` (для быстрого lookup).
    pub by_name: HashMap<String, usize>,
    /// Корреляционные маркеты (BTC-маркеты для расчётов), key = `bn_market_name`.
    pub corr_markets: HashMap<String, CorrMarket>,
    /// Цены маркетов по `mIndex` (параллельный массив, обновляется prices apply).
    pub prices: Vec<MarketPrice>,
    /// Текущие цены CorrMarkets, key = `bn_market_name`.
    pub corr_prices: HashMap<String, f64>,
    /// Теги монет, key = `market_name`.
    pub token_tags: HashMap<String, TokenTags>,
    /// Канонический mIndex → имя маркета (из `emk_GetMarketsIndexes`).
    pub market_indexes: Vec<String>,
    /// `true` если последняя пачка `emk_GetMarketsIndexes` была получена для текущего
    /// `PeerAppToken`. При server-restart (`PeerAppToken` сменился) Client сбрасывает в
    /// `false` и отправляет fresh `api_get_markets_indexes()`. До получения ответа
    /// `EventDispatcher` дропает входящие `TradesStream` / `OrderBook` пакеты — они
    /// несут market_idx по новой нумерации, локальные state ещё знают старую.
    ///
    /// Аналог Delphi `MoonProtoEngine.pas:1580 If FLastServerAppToken <> PeerAppToken then exit`.
    pub indexes_synchronized: bool,
    /// Delphi `NewMarketFound` analogue: set when a price row points at a server
    /// market index/name that is not present in the current market list.
    ///
    /// It is intentionally kept true after scheduling `GetMarketsList` and is
    /// cleared only by a successful list apply, matching Delphi's synchronous
    /// `Engine.GetMarketsList()` path.
    pub markets_list_refresh_needed: bool,
    /// Delphi `ES_MaxLevInGetMarkets in EngineProp`: existing markets copy
    /// `MaxLeverage` from `GetMarketsList` only for platforms that set this
    /// support flag. New markets still receive the incoming value because they
    /// are inserted as whole `TMarket` objects.
    copy_max_leverage_from_markets_list: bool,
}

#[derive(Debug, Clone)]
pub enum MarketsEvent {
    /// Применён список маркетов (после `emk_GetMarketsList`).
    /// Variant name is historical; repeated calls merge like Delphi.
    MarketsListReplaced { count: usize, corr_count: usize },
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

impl MarketsState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Применить ответ `emk_GetMarketsList`.
    ///
    /// Delphi does not replace the whole market universe on a repeated list
    /// response. Existing `TMarket` objects are updated through
    /// `CopyFromMarket`, old live price state is preserved, absent old markets
    /// stay in `Markets`, and CorrMarkets are add/update-only.
    pub fn apply_markets_list(&mut self, resp: MarketsListResponse) -> MarketsEvent {
        let first_create_markets = self.markets.is_empty();
        let new_market_found = self.markets_list_refresh_needed;
        let allow_new_markets = first_create_markets || new_market_found;
        let incoming_count = resp.markets.len();
        let corr_count = resp.corr_markets.len();

        let old_markets = std::mem::take(&mut self.markets);
        let old_prices = std::mem::take(&mut self.prices);
        let incoming_by_name = resp
            .markets
            .iter()
            .enumerate()
            .map(|(idx, m)| (m.bn_market_name.clone(), idx))
            .collect::<HashMap<_, _>>();
        let mut consumed = HashMap::with_capacity(resp.markets.len());

        let mut markets = Vec::with_capacity(old_markets.len().max(incoming_count));
        let mut prices = Vec::with_capacity(old_markets.len().max(incoming_count));

        for (old_idx, mut market) in old_markets.into_iter().enumerate() {
            let mut price = old_prices
                .get(old_idx)
                .copied()
                .unwrap_or_else(|| market_price_from_market(&market));
            if let Some(&incoming_idx) = incoming_by_name.get(&market.bn_market_name) {
                let incoming = &resp.markets[incoming_idx];
                merge_market_like_delphi_get_markets_list(
                    &mut market,
                    incoming,
                    self.copy_max_leverage_from_markets_list,
                );
                price.funding_time = market.funding_time;
                consumed.insert(market.bn_market_name.clone(), true);
            }
            markets.push(market);
            prices.push(price);
        }

        for market in resp.markets {
            if consumed.contains_key(&market.bn_market_name) {
                continue;
            }
            if !allow_new_markets {
                continue;
            }
            prices.push(market_price_from_market(&market));
            markets.push(market);
        }

        self.by_name.clear();
        self.by_name.reserve(markets.len());
        for (i, m) in markets.iter().enumerate() {
            self.by_name.insert(m.bn_market_name.clone(), i);
        }

        self.token_tags
            .retain(|name, _| self.by_name.contains_key(name));

        self.markets = markets;
        self.prices = prices;

        for cm in resp.corr_markets {
            if cm.base_currency_name.is_empty() {
                continue;
            }
            self.corr_markets.insert(cm.bn_market_name.clone(), cm);
        }
        self.markets_list_refresh_needed = false;

        MarketsEvent::MarketsListReplaced {
            count: self.markets.len(),
            corr_count,
        }
    }

    /// Применить ответ `emk_UpdateMarketsList`.
    /// Обновляет цену рынка, резолвя server `mIndex` через `emk_GetMarketsIndexes`.
    /// Если mapping неизвестен или stale после server restart — запись пропускается.
    pub fn apply_markets_prices(&mut self, resp: MarketsPricesResponse) -> MarketsEvent {
        let count = resp.prices.len();
        let mut missing_market_seen = false;
        for slot in &mut self.prices {
            slot.mark_price_found = false;
        }
        for p in &resp.prices {
            if let Some(idx) = self.local_pos_for_server_index(p.m_index) {
                let slot = &mut self.prices[idx];
                slot.bid = p.bid;
                slot.ask = p.ask;
                if resp.send_funding {
                    slot.funding_rate = p.funding_rate;
                    slot.funding_time = p.funding_time;
                }
                slot.mark_price = p.mark_price;
                slot.mark_price_found = p.mark_price_found;
            } else if self.price_row_points_to_missing_market(p.m_index) {
                missing_market_seen = true;
            }
        }
        if missing_market_seen {
            self.markets_list_refresh_needed = true;
        }
        if resp.send_corr_markets {
            for c in &resp.corr_prices {
                self.corr_prices
                    .insert(c.bn_market_name.clone(), c.last_price);
            }
        }
        MarketsEvent::PricesUpdated {
            count,
            included_funding: resp.send_funding,
            included_corr: resp.send_corr_markets,
        }
    }

    /// Применить ответ `emk_GetMarketsIndexes`.
    /// Помечает `indexes_synchronized = true` — после этого EventDispatcher разблокирует
    /// обработку TradesStream / OrderBook пакетов.
    pub fn apply_markets_indexes(&mut self, names: Vec<String>) -> MarketsEvent {
        let count = names.len();
        self.market_indexes = names;
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

    /// Применить ответ `emk_CheckBinanceTags`.
    ///
    /// Delphi `TMoonProtoEngine.CheckBinanceTags` clears seen state for all
    /// markets, applies tags for markets present in the response, then clears
    /// tags for every market not seen in that response.
    pub fn apply_token_tags(&mut self, items: Vec<MarketTokenTags>) -> MarketsEvent {
        self.token_tags.clear();
        let mut count = 0usize;
        for it in items {
            if self.by_name.contains_key(&it.market_name) {
                self.token_tags.insert(it.market_name, it.tags);
                count += 1;
            }
        }
        MarketsEvent::TokenTagsUpdated { count }
    }

    /// Получить маркет по имени.
    pub fn get(&self, market_name: &str) -> Option<&Market> {
        self.by_name.get(market_name).map(|&i| &self.markets[i])
    }

    /// Resolve a server `mIndex` into the canonical market name from
    /// `emk_GetMarketsIndexes`.
    ///
    /// Returns `None` while indexes are stale after a server restart. During
    /// that window `EventDispatcher` also gates market-index streams, so regular
    /// consumers do not see trades/orderbook events with an old mapping.
    pub fn market_name_by_index(&self, m_index: u16) -> Option<&str> {
        if !self.indexes_synchronized {
            return None;
        }
        self.market_indexes
            .get(m_index as usize)
            .map(String::as_str)
    }

    /// Resolve a server `mIndex` into the full market snapshot.
    pub fn market_by_index(&self, m_index: u16) -> Option<&Market> {
        let name = self.market_name_by_index(m_index)?;
        self.get(name)
    }

    /// Resolve a market name into the current server `mIndex`.
    ///
    /// Uses the canonical `emk_GetMarketsIndexes` mapping rather than the
    /// `markets` vector position, because this is the index carried by stream
    /// packets.
    pub fn market_index_by_name(&self, market_name: &str) -> Option<u16> {
        if !self.indexes_synchronized {
            return None;
        }
        self.market_indexes
            .iter()
            .position(|name| name == market_name)
            .and_then(|idx| u16::try_from(idx).ok())
    }

    /// Получить цену маркета по `mIndex`.
    pub fn price_by_index(&self, m_index: u16) -> Option<&MarketPrice> {
        let idx = self.local_pos_for_server_index(m_index)?;
        self.prices.get(idx)
    }

    pub(crate) fn has_server_market_index(&self, m_index: u16) -> bool {
        if !self.indexes_synchronized {
            return false;
        }
        self.market_indexes
            .get(m_index as usize)
            .is_some_and(|name| self.by_name.contains_key(name))
    }

    /// Получить цену маркета по имени (через by_name lookup).
    pub fn price(&self, market_name: &str) -> Option<&MarketPrice> {
        self.by_name
            .get(market_name)
            .and_then(|&i| self.prices.get(i))
    }

    /// Теги маркета (пустые если не было `apply_token_tags`).
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

    fn local_pos_for_server_index(&self, m_index: u16) -> Option<usize> {
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

    fn price_row_points_to_missing_market(&self, m_index: u16) -> bool {
        let server_pos = m_index as usize;
        if self.indexes_synchronized {
            return self
                .market_indexes
                .get(server_pos)
                .is_none_or(|name| !self.by_name.contains_key(name));
        }
        self.market_indexes.is_empty() && server_pos >= self.prices.len()
    }

    pub fn markets_list_refresh_needed(&self) -> bool {
        self.markets_list_refresh_needed
    }

    pub(crate) fn set_copy_max_leverage_from_markets_list(&mut self, enabled: bool) {
        self.copy_max_leverage_from_markets_list = enabled;
    }
}

fn merge_market_like_delphi_get_markets_list(
    dst: &mut Market,
    src: &Market,
    copy_max_leverage: bool,
) {
    dst.bn_tick_size = src.bn_tick_size;
    dst.bn_step_size = src.bn_step_size;
    dst.bn_min_price = src.bn_min_price;
    dst.bn_max_price = src.bn_max_price;
    dst.bn_min_qty = src.bn_min_qty;
    dst.bn_max_qty = src.bn_max_qty;
    dst.bn_min_notional = src.bn_min_notional;
    if src.bn_max_value > EPS_MARKET {
        dst.bn_max_value = src.bn_max_value;
    }
    dst.bn_iceberg_parts = src.bn_iceberg_parts;
    dst.bn_iceberg = src.bn_iceberg;
    dst.bn_multiplier_down = src.bn_multiplier_down;
    dst.bn_multiplier_up = src.bn_multiplier_up;
    dst.bn_price_precision = src.bn_price_precision;
    dst.bn_quantity_precision = src.bn_quantity_precision;
    dst.status_trading = src.status_trading;
    dst.bn_only_isolated = src.bn_only_isolated;
    dst.bn_margin_table_id = src.bn_margin_table_id;
    dst.bid_multiplier_up = src.bid_multiplier_up;
    dst.bid_multiplier_down = src.bid_multiplier_down;
    dst.ask_multiplier_up = src.ask_multiplier_up;
    dst.ask_multiplier_down = src.ask_multiplier_down;
    if copy_max_leverage {
        dst.max_leverage = src.max_leverage;
    }
    dst.funding_time = src.funding_time;
    dst.volume = src.volume;
}

fn market_price_from_market(m: &Market) -> MarketPrice {
    MarketPrice {
        bid: 0.0,
        ask: 0.0,
        funding_rate: m.funding_rate,
        funding_time: m.funding_time,
        mark_price: 0.0,
        mark_price_found: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::market::{BaseCurrency, CorrMarketPriceUpdate, MarketPriceUpdate};

    fn mk_market(name: &str, idx: u16) -> Market {
        Market {
            bn_market_name: name.to_string(),
            market_currency: name.to_string(),
            bn_market_currency: name.to_string(),
            base_currency: "USDT".to_string(),
            market_currency_long: name.to_string(),
            market_currency_canonic: name.to_string(),
            market_name: format!("{}USDT", name),
            market_name_mb_classic: format!("{}USDT", name),
            bn_status: "TRADING".to_string(),
            leading1000: String::new(),
            bn_price_precision: 2,
            bn_quantity_precision: 5,
            max_leverage: 50,
            k1000: 1,
            bn_iceberg_parts: 0,
            bn_margin_table_id: 0,
            bn_delivery_time: 0,
            bn_tick_size: 0.01,
            bn_step_size: 0.01,
            bn_min_qty: 0.0,
            bn_max_qty: 0.0,
            bn_min_notional: 0.0,
            bn_max_notional: 0.0,
            bn_contract_size: 0.0,
            bn_min_price: 0.0,
            bn_max_price: 0.0,
            bn_max_value: 0.0,
            bn_multiplier_up: 0.0,
            bn_multiplier_down: 0.0,
            bid_multiplier_up: 0.0,
            bid_multiplier_down: 0.0,
            ask_multiplier_up: 0.0,
            ask_multiplier_down: 0.0,
            int_bn_max_qty: 0.0,
            funding_rate: 0.0001 * idx as f64,
            funding_time: 45000.0 + idx as f64,
            volume: 0.0,
            is_btc_market: false,
            status_trading: true,
            bn_is_fucking_shib: false,
            bn_iceberg: false,
            bn_only_isolated: false,
            futures_type: BaseCurrency::USDT,
        }
    }

    #[test]
    fn apply_markets_list_initial_populates_state() {
        let mut st = MarketsState::new();
        let resp = MarketsListResponse {
            markets: vec![mk_market("BTC", 0), mk_market("ETH", 1)],
            corr_markets: vec![],
        };
        let ev = st.apply_markets_list(resp);
        assert!(matches!(
            ev,
            MarketsEvent::MarketsListReplaced {
                count: 2,
                corr_count: 0
            }
        ));
        assert_eq!(st.market_count(), 2);
        assert_eq!(st.get("BTC").unwrap().bn_market_name, "BTC");
        assert_eq!(st.get("ETH").unwrap().bn_market_name, "ETH");
        assert!(st.get("DOGE").is_none());
    }

    #[test]
    fn apply_markets_list_preserves_absent_existing_markets_like_delphi() {
        let mut st = MarketsState::new();
        st.apply_markets_list(MarketsListResponse {
            markets: vec![mk_market("BTCUSDT", 0), mk_market("DOGEUSDT", 1)],
            corr_markets: vec![],
        });
        st.apply_token_tags(vec![
            MarketTokenTags {
                market_name: "BTCUSDT".to_string(),
                tags: TokenTags::MONITORING,
            },
            MarketTokenTags {
                market_name: "DOGEUSDT".to_string(),
                tags: TokenTags::GAMING,
            },
        ]);

        st.apply_markets_list(MarketsListResponse {
            markets: vec![mk_market("BTCUSDT", 0)],
            corr_markets: vec![],
        });

        assert!(
            st.get("DOGEUSDT").is_some(),
            "Delphi GetMarketsList updates/adds but does not delete old Markets entries"
        );
        assert!(st.tags("BTCUSDT").contains(TokenTags::MONITORING));
        assert!(
            st.tags("DOGEUSDT").contains(TokenTags::GAMING),
            "absent old markets keep their token tags because the market is still present"
        );
    }

    #[test]
    fn apply_markets_list_does_not_add_new_market_without_new_market_found_like_delphi() {
        let mut st = MarketsState::new();
        st.apply_markets_list(MarketsListResponse {
            markets: vec![mk_market("BTCUSDT", 0)],
            corr_markets: vec![],
        });

        st.apply_markets_list(MarketsListResponse {
            markets: vec![mk_market("BTCUSDT", 0), mk_market("DOGEUSDT", 1)],
            corr_markets: vec![],
        });

        assert!(st.get("BTCUSDT").is_some());
        assert!(
            st.get("DOGEUSDT").is_none(),
            "Delphi frees unknown TMarket when not FirstCreateMarkets and not NewMarketFound"
        );
    }

    #[test]
    fn apply_markets_list_adds_new_market_and_clears_new_market_found_like_delphi() {
        let mut st = MarketsState::new();
        st.apply_markets_list(MarketsListResponse {
            markets: vec![mk_market("BTCUSDT", 0)],
            corr_markets: vec![],
        });
        st.markets_list_refresh_needed = true;

        st.apply_markets_list(MarketsListResponse {
            markets: vec![mk_market("BTCUSDT", 0), mk_market("DOGEUSDT", 1)],
            corr_markets: vec![],
        });

        assert!(st.get("DOGEUSDT").is_some());
        assert!(
            !st.markets_list_refresh_needed(),
            "Delphi clears NewMarketFound only after successful GetMarketsList apply"
        );
    }

    #[test]
    fn apply_markets_list_merges_existing_market_and_preserves_live_price_like_delphi() {
        let mut st = MarketsState::new();
        let mut old = mk_market("BTCUSDT", 1);
        old.bn_max_value = 123.0;
        old.funding_rate = 0.0007;
        old.funding_time = 45000.0;
        st.apply_markets_list(MarketsListResponse {
            markets: vec![old],
            corr_markets: vec![],
        });
        st.apply_markets_prices(MarketsPricesResponse {
            send_funding: false,
            prices: vec![MarketPriceUpdate {
                m_index: 0,
                bid: 50000.0,
                ask: 50001.0,
                funding_rate: 0.0,
                funding_time: 0.0,
                mark_price: 50000.5,
                mark_price_found: true,
            }],
            send_corr_markets: false,
            corr_prices: vec![],
        });

        let mut incoming = mk_market("BTCUSDT", 2);
        incoming.bn_tick_size = 0.25;
        incoming.bn_max_value = 0.0;
        incoming.funding_rate = 0.0999;
        incoming.funding_time = 46000.0;
        st.apply_markets_list(MarketsListResponse {
            markets: vec![incoming],
            corr_markets: vec![],
        });

        let market = st.get("BTCUSDT").unwrap();
        assert_eq!(market.bn_tick_size, 0.25);
        assert_eq!(
            market.bn_max_value, 123.0,
            "Delphi CopyFromMarket ignores non-positive bnMaxValue"
        );
        assert_eq!(
            market.funding_rate, 0.0007,
            "Delphi GetMarketsList CopyFromMarket does not overwrite FundingRate"
        );
        assert_eq!(market.funding_time, 46000.0);

        let price = st.price("BTCUSDT").unwrap();
        assert_eq!(price.bid, 50000.0);
        assert_eq!(price.ask, 50001.0);
        assert_eq!(price.funding_rate, 0.0007);
        assert_eq!(price.funding_time, 46000.0);
        assert!(price.mark_price_found);
    }

    #[test]
    fn apply_markets_list_keeps_existing_max_leverage_without_delphi_engine_flag() {
        let mut st = MarketsState::new();
        let mut old = mk_market("BTCUSDT", 1);
        old.max_leverage = 25;
        st.apply_markets_list(MarketsListResponse {
            markets: vec![old],
            corr_markets: vec![],
        });

        let mut incoming = mk_market("BTCUSDT", 2);
        incoming.max_leverage = 125;
        st.apply_markets_list(MarketsListResponse {
            markets: vec![incoming],
            corr_markets: vec![],
        });

        assert_eq!(
            st.get("BTCUSDT").unwrap().max_leverage,
            25,
            "Delphi CopyFromMarket copies MaxLeverage only when ES_MaxLevInGetMarkets is set"
        );
    }

    #[test]
    fn apply_markets_list_copies_existing_max_leverage_with_delphi_engine_flag() {
        let mut st = MarketsState::new();
        st.set_copy_max_leverage_from_markets_list(true);
        let mut old = mk_market("BTCUSDT", 1);
        old.max_leverage = 25;
        st.apply_markets_list(MarketsListResponse {
            markets: vec![old],
            corr_markets: vec![],
        });

        let mut incoming = mk_market("BTCUSDT", 2);
        incoming.max_leverage = 125;
        st.apply_markets_list(MarketsListResponse {
            markets: vec![incoming],
            corr_markets: vec![],
        });

        assert_eq!(st.get("BTCUSDT").unwrap().max_leverage, 125);
    }

    #[test]
    fn apply_prices_updates_by_index() {
        let mut st = MarketsState::new();
        st.apply_markets_list(MarketsListResponse {
            markets: vec![mk_market("BTC", 0), mk_market("ETH", 1)],
            corr_markets: vec![],
        });

        let prices = MarketsPricesResponse {
            send_funding: false,
            prices: vec![
                MarketPriceUpdate {
                    m_index: 0,
                    bid: 50000.0,
                    ask: 50001.0,
                    funding_rate: 0.0,
                    funding_time: 0.0,
                    mark_price: 50000.5,
                    mark_price_found: true,
                },
                MarketPriceUpdate {
                    m_index: 1,
                    bid: 3000.0,
                    ask: 3000.5,
                    funding_rate: 0.0,
                    funding_time: 0.0,
                    mark_price: 3000.25,
                    mark_price_found: true,
                },
            ],
            send_corr_markets: false,
            corr_prices: vec![],
        };
        let ev = st.apply_markets_prices(prices);
        assert!(matches!(
            ev,
            MarketsEvent::PricesUpdated {
                count: 2,
                included_funding: false,
                ..
            }
        ));
        assert_eq!(st.price("BTC").unwrap().bid, 50000.0);
        assert_eq!(st.price("BTC").unwrap().ask, 50001.0);
        assert_eq!(st.price("ETH").unwrap().mark_price, 3000.25);
    }

    #[test]
    fn apply_prices_resets_mark_price_found_before_each_batch_like_delphi() {
        let mut st = MarketsState::new();
        st.apply_markets_list(MarketsListResponse {
            markets: vec![mk_market("BTC", 0), mk_market("ETH", 1)],
            corr_markets: vec![],
        });

        st.apply_markets_prices(MarketsPricesResponse {
            send_funding: false,
            prices: vec![
                MarketPriceUpdate {
                    m_index: 0,
                    bid: 10.0,
                    ask: 11.0,
                    funding_rate: 0.0,
                    funding_time: 0.0,
                    mark_price: 10.5,
                    mark_price_found: true,
                },
                MarketPriceUpdate {
                    m_index: 1,
                    bid: 20.0,
                    ask: 21.0,
                    funding_rate: 0.0,
                    funding_time: 0.0,
                    mark_price: 20.5,
                    mark_price_found: true,
                },
            ],
            send_corr_markets: false,
            corr_prices: vec![],
        });
        assert!(st.price("BTC").unwrap().mark_price_found);
        assert!(st.price("ETH").unwrap().mark_price_found);

        st.apply_markets_prices(MarketsPricesResponse {
            send_funding: false,
            prices: vec![MarketPriceUpdate {
                m_index: 1,
                bid: 22.0,
                ask: 23.0,
                funding_rate: 0.0,
                funding_time: 0.0,
                mark_price: 22.5,
                mark_price_found: true,
            }],
            send_corr_markets: false,
            corr_prices: vec![],
        });

        assert!(
            !st.price("BTC").unwrap().mark_price_found,
            "Delphi clears CurrentMarkPriceFound before reading each UpdateMarketsList batch"
        );
        assert!(st.price("ETH").unwrap().mark_price_found);
    }

    #[test]
    fn apply_prices_uses_server_index_mapping() {
        let mut st = MarketsState::new();
        st.apply_markets_list(MarketsListResponse {
            markets: vec![mk_market("BTCUSDT", 0), mk_market("ETHUSDT", 1)],
            corr_markets: vec![],
        });
        st.apply_markets_indexes(vec!["ETHUSDT".to_string(), "BTCUSDT".to_string()]);

        let prices = MarketsPricesResponse {
            send_funding: false,
            prices: vec![MarketPriceUpdate {
                m_index: 0,
                bid: 3000.0,
                ask: 3001.0,
                funding_rate: 0.0,
                funding_time: 0.0,
                mark_price: 3000.5,
                mark_price_found: true,
            }],
            send_corr_markets: false,
            corr_prices: vec![],
        };
        st.apply_markets_prices(prices);

        assert_eq!(st.price("ETHUSDT").unwrap().bid, 3000.0);
        assert_eq!(st.price("BTCUSDT").unwrap().bid, 0.0);
        assert_eq!(st.price_by_index(0).unwrap().bid, 3000.0);
    }

    #[test]
    fn apply_prices_skips_stale_server_index_mapping() {
        let mut st = MarketsState::new();
        st.apply_markets_list(MarketsListResponse {
            markets: vec![mk_market("BTCUSDT", 0), mk_market("ETHUSDT", 1)],
            corr_markets: vec![],
        });
        st.apply_markets_indexes(vec!["ETHUSDT".to_string(), "BTCUSDT".to_string()]);
        st.mark_indexes_stale();

        let prices = MarketsPricesResponse {
            send_funding: false,
            prices: vec![MarketPriceUpdate {
                m_index: 0,
                bid: 3000.0,
                ask: 3001.0,
                funding_rate: 0.0,
                funding_time: 0.0,
                mark_price: 3000.5,
                mark_price_found: true,
            }],
            send_corr_markets: false,
            corr_prices: vec![],
        };
        st.apply_markets_prices(prices);

        assert_eq!(st.price("ETHUSDT").unwrap().bid, 0.0);
        assert!(st.price_by_index(0).is_none());
    }

    #[test]
    fn apply_prices_marks_refresh_needed_for_unknown_indexed_market_like_delphi() {
        let mut st = MarketsState::new();
        st.apply_markets_list(MarketsListResponse {
            markets: vec![mk_market("BTCUSDT", 0)],
            corr_markets: vec![],
        });
        st.apply_markets_indexes(vec!["DOGEUSDT".to_string()]);

        st.apply_markets_prices(MarketsPricesResponse {
            send_funding: false,
            prices: vec![MarketPriceUpdate {
                m_index: 0,
                bid: 0.1,
                ask: 0.2,
                funding_rate: 0.0,
                funding_time: 0.0,
                mark_price: 0.15,
                mark_price_found: true,
            }],
            send_corr_markets: false,
            corr_prices: vec![],
        });

        assert!(
            st.markets_list_refresh_needed(),
            "Delphi sets NewMarketFound when SrvMarkets.FindByServerIndex returns nil"
        );
        assert!(
            st.price("BTCUSDT").unwrap().bid == 0.0,
            "unknown market row must not be applied to a wrong local market"
        );
    }

    #[test]
    fn apply_prices_marks_refresh_needed_for_out_of_range_index_like_delphi() {
        let mut st = MarketsState::new();
        st.apply_markets_list(MarketsListResponse {
            markets: vec![mk_market("BTCUSDT", 0)],
            corr_markets: vec![],
        });
        st.apply_markets_indexes(vec!["BTCUSDT".to_string()]);

        st.apply_markets_prices(MarketsPricesResponse {
            send_funding: false,
            prices: vec![MarketPriceUpdate {
                m_index: 2,
                bid: 0.1,
                ask: 0.2,
                funding_rate: 0.0,
                funding_time: 0.0,
                mark_price: 0.15,
                mark_price_found: true,
            }],
            send_corr_markets: false,
            corr_prices: vec![],
        });

        assert!(
            st.markets_list_refresh_needed(),
            "Delphi SrvMarkets.FindByServerIndex(out-of-range) returns nil and sets NewMarketFound"
        );
        assert_eq!(st.price("BTCUSDT").unwrap().bid, 0.0);
    }

    #[test]
    fn apply_markets_list_clears_refresh_needed_after_listing_refresh() {
        let mut st = MarketsState::new();
        st.markets_list_refresh_needed = true;
        st.apply_markets_list(MarketsListResponse {
            markets: vec![mk_market("DOGEUSDT", 0)],
            corr_markets: vec![],
        });

        assert!(!st.markets_list_refresh_needed());
        assert!(st.get("DOGEUSDT").is_some());
    }

    #[test]
    fn apply_prices_with_funding_updates_funding() {
        let mut st = MarketsState::new();
        st.apply_markets_list(MarketsListResponse {
            markets: vec![mk_market("BTC", 0)],
            corr_markets: vec![],
        });
        // Initial funding from Market.funding_rate
        let initial_funding = st.price("BTC").unwrap().funding_rate;
        assert_eq!(initial_funding, 0.0);

        let prices = MarketsPricesResponse {
            send_funding: true,
            prices: vec![MarketPriceUpdate {
                m_index: 0,
                bid: 50000.0,
                ask: 50001.0,
                funding_rate: 0.0005,
                funding_time: 45123.5,
                mark_price: 50000.5,
                mark_price_found: true,
            }],
            send_corr_markets: false,
            corr_prices: vec![],
        };
        st.apply_markets_prices(prices);
        assert_eq!(st.price("BTC").unwrap().funding_rate, 0.0005);
        assert_eq!(st.price("BTC").unwrap().funding_time, 45123.5);
    }

    #[test]
    fn apply_prices_without_funding_keeps_existing() {
        let mut st = MarketsState::new();
        st.apply_markets_list(MarketsListResponse {
            markets: vec![mk_market("BTC", 5)], // funding_rate = 0.0005 from constructor
            corr_markets: vec![],
        });
        let pre = st.price("BTC").unwrap().funding_rate;
        assert_eq!(pre, 0.0005); // из Market.funding_rate

        let prices = MarketsPricesResponse {
            send_funding: false, // funding не передан
            prices: vec![MarketPriceUpdate {
                m_index: 0,
                bid: 50000.0,
                ask: 50001.0,
                funding_rate: 99.0,
                funding_time: 99.0, // эти значения должны быть проигнорированы
                mark_price: 50000.5,
                mark_price_found: true,
            }],
            send_corr_markets: false,
            corr_prices: vec![],
        };
        st.apply_markets_prices(prices);
        // funding_rate должен сохраниться (send_funding=false)
        assert_eq!(st.price("BTC").unwrap().funding_rate, 0.0005);
    }

    #[test]
    fn apply_prices_out_of_range_skipped() {
        let mut st = MarketsState::new();
        st.apply_markets_list(MarketsListResponse {
            markets: vec![mk_market("BTC", 0)],
            corr_markets: vec![],
        });
        let prices = MarketsPricesResponse {
            send_funding: false,
            prices: vec![MarketPriceUpdate {
                m_index: 999, // out of range
                bid: 1.0,
                ask: 1.0,
                funding_rate: 0.0,
                funding_time: 0.0,
                mark_price: 0.0,
                mark_price_found: false,
            }],
            send_corr_markets: false,
            corr_prices: vec![],
        };
        // Не должно паниковать
        let _ = st.apply_markets_prices(prices);
        assert_eq!(st.price("BTC").unwrap().bid, 0.0); // не обновился
    }

    #[test]
    fn apply_markets_list_skips_corr_market_with_empty_base_currency_like_delphi() {
        let mut st = MarketsState::new();
        st.apply_markets_list(MarketsListResponse {
            markets: vec![],
            corr_markets: vec![CorrMarket {
                bn_market_name: "DOGEBTC".to_string(),
                bn_market_currency: "DOGE".to_string(),
                bn_tick_size: 0.0,
                base_currency_name: String::new(),
            }],
        });

        assert_eq!(
            st.corr_count(),
            0,
            "Delphi calls AddOrSetCorrMarket only when BaseCur is not empty"
        );
    }

    #[test]
    fn apply_corr_prices_merges_like_delphi() {
        let mut st = MarketsState::new();
        st.apply_markets_list(MarketsListResponse {
            markets: vec![],
            corr_markets: vec![CorrMarket {
                bn_market_name: "DOGEBTC".to_string(),
                bn_market_currency: "DOGE".to_string(),
                bn_tick_size: 0.0,
                base_currency_name: "BTC".to_string(),
            }],
        });
        st.corr_prices.insert("ETHBTC".to_string(), 0.07);
        assert_eq!(st.corr_count(), 1);

        let prices = MarketsPricesResponse {
            send_funding: false,
            prices: vec![],
            send_corr_markets: true,
            corr_prices: vec![CorrMarketPriceUpdate {
                bn_market_name: "DOGEBTC".to_string(),
                last_price: 0.00000123,
            }],
        };
        st.apply_markets_prices(prices);
        assert_eq!(st.corr_prices.get("DOGEBTC").copied(), Some(0.00000123));
        assert_eq!(
            st.corr_prices.get("ETHBTC").copied(),
            Some(0.07),
            "Delphi updates sent corr prices but does not clear absent ones"
        );
    }

    #[test]
    fn apply_token_tags_clears_missing_markets_like_delphi_check_binance_tags() {
        let mut st = MarketsState::new();
        st.apply_markets_list(MarketsListResponse {
            markets: vec![
                mk_market("BTCUSDT", 0),
                mk_market("DOGEUSDT", 1),
                mk_market("ETHUSDT", 2),
            ],
            corr_markets: vec![],
        });

        let ev = st.apply_token_tags(vec![
            MarketTokenTags {
                market_name: "BTCUSDT".to_string(),
                tags: TokenTags::MONITORING,
            },
            MarketTokenTags {
                market_name: "DOGEUSDT".to_string(),
                tags: TokenTags::GAMING | TokenTags::NEW,
            },
        ]);
        assert!(matches!(ev, MarketsEvent::TokenTagsUpdated { count: 2 }));
        assert!(st.tags("BTCUSDT").contains(TokenTags::MONITORING));
        assert!(st.tags("DOGEUSDT").contains(TokenTags::GAMING));
        assert!(st.tags("NOPE").is_empty());

        let ev = st.apply_token_tags(vec![
            MarketTokenTags {
                market_name: "ETHUSDT".to_string(),
                tags: TokenTags::ALPHA,
            },
            MarketTokenTags {
                market_name: "UNKNOWN".to_string(),
                tags: TokenTags::FAN,
            },
        ]);
        assert!(matches!(ev, MarketsEvent::TokenTagsUpdated { count: 1 }));
        assert!(
            st.tags("BTCUSDT").is_empty(),
            "Delphi clears TokenTags for markets not seen in the latest response"
        );
        assert!(st.tags("ETHUSDT").contains(TokenTags::ALPHA));
        assert!(st.tags("UNKNOWN").is_empty());
    }

    #[test]
    fn apply_markets_indexes() {
        let mut st = MarketsState::new();
        let names = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
        let ev = st.apply_markets_indexes(names.clone());
        assert!(matches!(ev, MarketsEvent::IndexesUpdated { count: 2 }));
        assert_eq!(st.market_indexes, names);
    }

    #[test]
    fn apply_markets_indexes_sets_synchronized_flag() {
        // Active library: indexes_synchronized = false по умолчанию (init состояние).
        // EventDispatcher блокирует TradesStream/OrderBook до этого момента.
        let mut st = MarketsState::new();
        assert!(!st.indexes_synchronized, "default: not synchronized");
        st.apply_markets_indexes(vec!["A".to_string()]);
        assert!(st.indexes_synchronized, "after apply: synchronized");
    }

    #[test]
    fn market_index_helpers_use_server_mapping() {
        let mut st = MarketsState::new();
        st.apply_markets_list(MarketsListResponse {
            markets: vec![mk_market("BTCUSDT", 0), mk_market("ETHUSDT", 1)],
            corr_markets: vec![],
        });
        st.apply_markets_indexes(vec!["ETHUSDT".to_string(), "BTCUSDT".to_string()]);

        assert_eq!(st.market_name_by_index(0), Some("ETHUSDT"));
        assert_eq!(st.market_by_index(1).unwrap().bn_market_name, "BTCUSDT");
        assert_eq!(st.market_index_by_name("BTCUSDT"), Some(1));
        assert_eq!(st.market_index_by_name("NOPE"), None);
    }

    #[test]
    fn market_index_helpers_hide_stale_mapping() {
        let mut st = MarketsState::new();
        st.apply_markets_list(MarketsListResponse {
            markets: vec![mk_market("BTCUSDT", 0)],
            corr_markets: vec![],
        });
        st.apply_markets_indexes(vec!["BTCUSDT".to_string()]);
        st.mark_indexes_stale();

        assert_eq!(st.market_name_by_index(0), None);
        assert_eq!(st.market_by_index(0), None);
        assert_eq!(st.market_index_by_name("BTCUSDT"), None);
    }
}
