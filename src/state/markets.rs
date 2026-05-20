//! Markets sync state — snapshot маркетов, поддерживается через Engine API ответы.
//!
//! Источник Delphi: `MarketsU.pas` (TMarket, TCorrMarket) + `MoonProtoEngineServer.pas`.
//!
//! ## Поток обновлений
//! - При запуске клиент шлёт `emk_GetMarketsList` → получает полный список (Markets + CorrMarkets).
//! - Периодически (1 раз в минуту по серверной логике) `emk_UpdateMarketsList` → обновление цен/funding.
//! - `emk_GetMarketsIndexes` → имена в порядке индексов (mIndex).
//! - `emk_CheckBinanceTags` → теги монет.

use std::collections::HashMap;

use crate::commands::market::{
    Market, CorrMarket, MarketsListResponse, MarketsPricesResponse,
    MarketTokenTags, TokenTags,
};

/// Per-market price snapshot (обновляется через `emk_UpdateMarketsList`).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct MarketPrice {
    /// Лучшая цена покупки (top of bid side).
    pub bid:               f64,
    /// Лучшая цена продажи (top of ask side).
    pub ask:               f64,
    /// Funding rate (для perpetual futures), дробь — например `0.0001` = 0.01%.
    pub funding_rate:      f64,
    /// UTC unix time момента следующего funding взимания (в днях, как Delphi TDateTime).
    pub funding_time_utc:  f64,
    /// Mark price (используется биржей для PnL/liquidation расчётов, может отличаться от last/bid/ask).
    pub mark_price:        f64,
    /// Был ли получен mark_price в последнем апдейте (биржи могут не присылать на каждом тике).
    pub mark_price_found:  bool,
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
}

#[derive(Debug, Clone)]
pub enum MarketsEvent {
    /// Полная замена списка маркетов (после `emk_GetMarketsList`).
    MarketsListReplaced { count: usize, corr_count: usize },
    /// Обновлены цены (через `emk_UpdateMarketsList`).
    PricesUpdated { count: usize, included_funding: bool, included_corr: bool },
    /// Получен список имён маркетов (`emk_GetMarketsIndexes`).
    IndexesUpdated { count: usize },
    /// Обновлены теги монет (`emk_CheckBinanceTags`).
    TokenTagsUpdated { count: usize },
}

impl MarketsState {
    pub fn new() -> Self { Self::default() }

    /// Применить ответ `emk_GetMarketsList` — полная замена markets + corr_markets.
    /// Так же обнуляет prices до размера нового списка (с сохранением значений из
    /// `Market.funding_rate/funding_time/volume`, которые приходят в самом маркете).
    pub fn apply_markets_list(&mut self, resp: MarketsListResponse) -> MarketsEvent {
        let count = resp.markets.len();
        let corr_count = resp.corr_markets.len();

        self.by_name.clear();
        self.by_name.reserve(count);
        for (i, m) in resp.markets.iter().enumerate() {
            self.by_name.insert(m.bn_market_name.clone(), i);
        }

        // Initialize prices from market fields (funding_rate/funding_time доступны прямо в Market).
        self.prices = resp.markets.iter().map(|m| MarketPrice {
            bid: 0.0,
            ask: 0.0,
            funding_rate: m.funding_rate,
            funding_time_utc: m.funding_time,
            mark_price: 0.0,
            mark_price_found: false,
        }).collect();

        self.markets = resp.markets;

        self.corr_markets.clear();
        self.corr_markets.reserve(corr_count);
        for cm in resp.corr_markets {
            self.corr_markets.insert(cm.bn_market_name.clone(), cm);
        }

        MarketsEvent::MarketsListReplaced { count, corr_count }
    }

    /// Применить ответ `emk_UpdateMarketsList`.
    /// Обновляет `self.prices[mIndex]` для каждой записи.
    /// Если `mIndex` выходит за рамки текущего списка — запись пропускается (out-of-order race).
    pub fn apply_markets_prices(&mut self, resp: MarketsPricesResponse) -> MarketsEvent {
        let count = resp.prices.len();
        for p in &resp.prices {
            let idx = p.m_index as usize;
            if idx < self.prices.len() {
                let slot = &mut self.prices[idx];
                slot.bid = p.bid;
                slot.ask = p.ask;
                if resp.send_funding {
                    slot.funding_rate = p.funding_rate;
                    slot.funding_time_utc = p.funding_time_utc;
                }
                slot.mark_price = p.mark_price;
                slot.mark_price_found = p.mark_price_found;
            }
        }
        if resp.send_corr_markets {
            // Полная замена corr_prices только из переданного набора
            // (так делает Delphi — пишет ВСЕ corr с актуальными ценами).
            self.corr_prices.clear();
            self.corr_prices.reserve(resp.corr_prices.len());
            for c in &resp.corr_prices {
                self.corr_prices.insert(c.bn_market_name.clone(), c.last_price);
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
    /// **Полная замена**: маркеты не в списке → теги удаляются (соответствует серверной
    /// семантике "сервер шлёт только маркеты с не-пустыми тегами, остальные = пусто").
    pub fn apply_token_tags(&mut self, items: Vec<MarketTokenTags>) -> MarketsEvent {
        let count = items.len();
        self.token_tags.clear();
        self.token_tags.reserve(count);
        for it in items {
            self.token_tags.insert(it.market_name, it.tags);
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
        self.market_indexes.get(m_index as usize).map(String::as_str)
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
        self.prices.get(m_index as usize)
    }

    pub(crate) fn has_server_market_index(&self, m_index: u16) -> bool {
        self.market_indexes
            .get(m_index as usize)
            .is_some_and(|name| self.by_name.contains_key(name))
    }

    /// Получить цену маркета по имени (через by_name lookup).
    pub fn price(&self, market_name: &str) -> Option<&MarketPrice> {
        self.by_name.get(market_name).and_then(|&i| self.prices.get(i))
    }

    /// Теги маркета (пустые если не было `apply_token_tags`).
    pub fn tags(&self, market_name: &str) -> TokenTags {
        self.token_tags.get(market_name).copied().unwrap_or(TokenTags::empty())
    }

    pub fn market_count(&self) -> usize { self.markets.len() }
    pub fn corr_count(&self) -> usize { self.corr_markets.len() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::market::{BaseCurrency, MarketPriceUpdate, CorrMarketPriceUpdate};

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
            bn_price_precision: 2, bn_quantity_precision: 5, max_leverage: 50,
            k1000: 1, bn_iceberg_parts: 0, bn_margin_table_id: 0,
            bn_delivery_time: 0,
            bn_tick_size: 0.01, bn_step_size: 0.01, bn_min_qty: 0.0,
            bn_max_qty: 0.0, bn_min_notional: 0.0, bn_max_notional: 0.0,
            bn_contract_size: 0.0, bn_min_price: 0.0, bn_max_price: 0.0,
            bn_max_value: 0.0,
            bn_multiplier_up: 0.0, bn_multiplier_down: 0.0,
            bid_multiplier_up: 0.0, bid_multiplier_down: 0.0,
            ask_multiplier_up: 0.0, ask_multiplier_down: 0.0,
            int_bn_max_qty: 0.0, funding_rate: 0.0001 * idx as f64,
            funding_time: 45000.0 + idx as f64,
            volume: 0.0,
            is_btc_market: false, status_trading: true,
            bn_is_fucking_shib: false, bn_iceberg: false, bn_only_isolated: false,
            futures_type: BaseCurrency::USDT,
        }
    }

    #[test]
    fn apply_markets_list_replaces_full() {
        let mut st = MarketsState::new();
        let resp = MarketsListResponse {
            markets: vec![mk_market("BTC", 0), mk_market("ETH", 1)],
            corr_markets: vec![],
        };
        let ev = st.apply_markets_list(resp);
        assert!(matches!(ev, MarketsEvent::MarketsListReplaced { count: 2, corr_count: 0 }));
        assert_eq!(st.market_count(), 2);
        assert_eq!(st.get("BTC").unwrap().bn_market_name, "BTC");
        assert_eq!(st.get("ETH").unwrap().bn_market_name, "ETH");
        assert!(st.get("DOGE").is_none());
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
                    bid: 50000.0, ask: 50001.0,
                    funding_rate: 0.0, funding_time_utc: 0.0,
                    mark_price: 50000.5, mark_price_found: true,
                },
                MarketPriceUpdate {
                    m_index: 1,
                    bid: 3000.0, ask: 3000.5,
                    funding_rate: 0.0, funding_time_utc: 0.0,
                    mark_price: 3000.25, mark_price_found: true,
                },
            ],
            send_corr_markets: false,
            corr_prices: vec![],
        };
        let ev = st.apply_markets_prices(prices);
        assert!(matches!(ev, MarketsEvent::PricesUpdated { count: 2, included_funding: false, .. }));
        assert_eq!(st.price("BTC").unwrap().bid, 50000.0);
        assert_eq!(st.price("BTC").unwrap().ask, 50001.0);
        assert_eq!(st.price("ETH").unwrap().mark_price, 3000.25);
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
                bid: 50000.0, ask: 50001.0,
                funding_rate: 0.0005, funding_time_utc: 45123.5,
                mark_price: 50000.5, mark_price_found: true,
            }],
            send_corr_markets: false,
            corr_prices: vec![],
        };
        st.apply_markets_prices(prices);
        assert_eq!(st.price("BTC").unwrap().funding_rate, 0.0005);
        assert_eq!(st.price("BTC").unwrap().funding_time_utc, 45123.5);
    }

    #[test]
    fn apply_prices_without_funding_keeps_existing() {
        let mut st = MarketsState::new();
        st.apply_markets_list(MarketsListResponse {
            markets: vec![mk_market("BTC", 5)],   // funding_rate = 0.0005 from constructor
            corr_markets: vec![],
        });
        let pre = st.price("BTC").unwrap().funding_rate;
        assert_eq!(pre, 0.0005); // из Market.funding_rate

        let prices = MarketsPricesResponse {
            send_funding: false,  // funding не передан
            prices: vec![MarketPriceUpdate {
                m_index: 0,
                bid: 50000.0, ask: 50001.0,
                funding_rate: 99.0, funding_time_utc: 99.0,  // эти значения должны быть проигнорированы
                mark_price: 50000.5, mark_price_found: true,
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
                bid: 1.0, ask: 1.0,
                funding_rate: 0.0, funding_time_utc: 0.0,
                mark_price: 0.0, mark_price_found: false,
            }],
            send_corr_markets: false,
            corr_prices: vec![],
        };
        // Не должно паниковать
        let _ = st.apply_markets_prices(prices);
        assert_eq!(st.price("BTC").unwrap().bid, 0.0); // не обновился
    }

    #[test]
    fn apply_corr_prices_replaces() {
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
        assert_eq!(st.corr_count(), 1);

        let prices = MarketsPricesResponse {
            send_funding: false,
            prices: vec![],
            send_corr_markets: true,
            corr_prices: vec![
                CorrMarketPriceUpdate { bn_market_name: "DOGEBTC".to_string(), last_price: 0.00000123 },
            ],
        };
        st.apply_markets_prices(prices);
        assert_eq!(st.corr_prices.get("DOGEBTC").copied(), Some(0.00000123));
    }

    #[test]
    fn apply_token_tags_replaces() {
        let mut st = MarketsState::new();
        let ev = st.apply_token_tags(vec![
            MarketTokenTags { market_name: "BTCUSDT".to_string(), tags: TokenTags::MONITORING },
            MarketTokenTags { market_name: "DOGEUSDT".to_string(), tags: TokenTags::GAMING | TokenTags::NEW },
        ]);
        assert!(matches!(ev, MarketsEvent::TokenTagsUpdated { count: 2 }));
        assert!(st.tags("BTCUSDT").contains(TokenTags::MONITORING));
        assert!(st.tags("DOGEUSDT").contains(TokenTags::GAMING));
        assert!(st.tags("NOPE").is_empty());

        // Replace
        st.apply_token_tags(vec![
            MarketTokenTags { market_name: "ETHUSDT".to_string(), tags: TokenTags::ALPHA },
        ]);
        // BTCUSDT убрался полностью (replace semantics)
        assert!(st.tags("BTCUSDT").is_empty());
        assert!(st.tags("ETHUSDT").contains(TokenTags::ALPHA));
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
