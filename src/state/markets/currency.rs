//! Base-currency and CorrMarket reference maintenance.

use std::sync::Arc;

use crate::commands::market::BaseCurrency;

use super::{
    text::{replace_text_ascii_case_insensitive, same_text_ascii},
    BaseCurrencyPrice, CorrMarket, MarketHandle, MarketsState,
};

impl MarketsState {
    pub(super) fn apply_one_corr_market_from_list(&mut self, cm: CorrMarket) {
        if cm.base_currency_name.is_empty() {
            return;
        }
        self.ensure_base_currency_price(&cm.base_currency_name);
        if let Some(existing) = Arc::make_mut(&mut self.corr_markets).get_mut(&cm.bn_market_name) {
            existing.bn_tick_size = cm.bn_tick_size;
            existing.base_currency_name = cm.base_currency_name;
        } else {
            Arc::make_mut(&mut self.corr_markets).insert(cm.bn_market_name.clone(), cm);
        }
    }

    fn ensure_base_currency_price(&mut self, base_currency: &str) {
        if base_currency.is_empty() || self.base_currency_prices.contains_key(base_currency) {
            return;
        }
        Arc::make_mut(&mut self.base_currency_prices).insert(
            base_currency.to_string(),
            BaseCurrencyPrice::new(base_currency.to_string()),
        );
    }

    pub(crate) fn set_server_base_currency(
        &mut self,
        name: Option<&str>,
        code: Option<BaseCurrency>,
    ) {
        let next_name = name.map(ToOwned::to_owned);
        if self.server_base_currency_name == next_name && self.server_base_currency_code == code {
            return;
        }
        self.server_base_currency_name = next_name;
        self.server_base_currency_code = code;
        self.check_corr_markets_like_delphi();
        self.check_currency_ref_markets_like_delphi();
        self.update_currency_prices_like_delphi();
    }

    pub(super) fn check_corr_markets_like_delphi(&mut self) {
        if self.server_base_is_btc_like_delphi() {
            return;
        }
        let Some(currency) = self.server_base_currency_name.as_deref() else {
            return;
        };
        if currency.is_empty() {
            return;
        }
        for handle in self.markets.iter() {
            let market_name = handle.name_str();
            if market_name.is_empty() {
                continue;
            }
            let corr_name = replace_text_ascii_case_insensitive(market_name, currency, "BTC");
            if self.corr_markets.contains_key(&corr_name) {
                Arc::make_mut(&mut self.ref_btc_corr_markets)
                    .insert(market_name.to_string(), corr_name);
            } else {
                Arc::make_mut(&mut self.ref_btc_corr_markets).remove(market_name);
            }
        }
    }

    pub(super) fn check_currency_ref_markets_like_delphi(&mut self) {
        let keys = self
            .base_currency_prices
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let market_refs = collect_market_currency_refs_like_delphi(&self.markets);
        for key in keys {
            let mut usdt_market = None;
            let mut usdt_rev_market = None;
            for market in &market_refs {
                let direct = same_text_ascii(&market.base_currency, "USDT")
                    && same_text_ascii(&market.bn_market_currency, &key);
                let reverse = same_text_ascii(&market.bn_market_currency, "USDT")
                    && same_text_ascii(&market.base_currency, &key);
                if direct {
                    usdt_market = Some(market.name.clone());
                }
                if reverse {
                    usdt_rev_market = Some(market.name.clone());
                }
            }

            let mut usdt_corr_market = None;
            let mut usdt_rev_corr_market = None;
            for cm in self.corr_markets.values() {
                if same_text_ascii(&cm.base_currency_name, "USDT")
                    && same_text_ascii(&cm.bn_market_currency, &key)
                {
                    usdt_corr_market = Some(cm.bn_market_name.clone());
                }
                if same_text_ascii(&cm.bn_market_currency, "USDT")
                    && same_text_ascii(&cm.base_currency_name, &key)
                {
                    usdt_rev_corr_market = Some(cm.bn_market_name.clone());
                }
            }

            let Some(bc) = Arc::make_mut(&mut self.base_currency_prices).get_mut(&key) else {
                continue;
            };
            if let Some(name) = usdt_market {
                bc.usdt_market = Some(name);
            }
            if let Some(name) = usdt_rev_market {
                bc.usdt_rev_market = Some(name);
            }
            if let Some(name) = usdt_corr_market {
                bc.usdt_corr_market = Some(name);
            }
            if let Some(name) = usdt_rev_corr_market {
                bc.usdt_rev_corr_market = Some(name);
            }
        }
    }

    pub(super) fn update_currency_prices_like_delphi(&mut self) {
        let keys = self
            .base_currency_prices
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            let next_price = self
                .base_currency_prices
                .get(&key)
                .and_then(|bc| self.next_base_currency_price_like_delphi(bc));
            if let Some(price) = next_price {
                if let Some(bc) = Arc::make_mut(&mut self.base_currency_prices).get_mut(&key) {
                    bc.last_price = price;
                }
            }
        }
    }

    fn next_base_currency_price_like_delphi(&self, bc: &BaseCurrencyPrice) -> Option<f64> {
        // Delphi `TMarkets.UpdateCurrencyPrices` (MarketsU.pas:2882-2894) gates all
        // four branches (UsdtMarket/UsdtRev/UsdtCorr/UsdtRevCorr) against `_epsM`, not `_eps`.
        if let Some(price) = bc
            .usdt_market
            .as_deref()
            .and_then(|name| self.price(name))
            .map(|p| p.ask)
            .filter(|ask| *ask > self.eps_profile.eps_m)
        {
            return Some(price);
        }
        if let Some(price) = bc
            .usdt_rev_market
            .as_deref()
            .and_then(|name| self.price(name))
            .map(|p| p.ask)
            .filter(|ask| *ask > self.eps_profile.eps_m)
        {
            return Some(1.0 / price);
        }
        if let Some(price) = bc
            .usdt_corr_market
            .as_deref()
            .and_then(|name| self.corr_prices.get(name))
            .copied()
            .filter(|price| *price > self.eps_profile.eps_m)
        {
            return Some(price);
        }
        if let Some(price) = bc
            .usdt_rev_corr_market
            .as_deref()
            .and_then(|name| self.corr_prices.get(name))
            .copied()
            .filter(|price| *price > self.eps_profile.eps_m)
        {
            return Some(1.0 / price);
        }
        if same_text_ascii(&bc.base_currency, "USDT") {
            return Some(1.0);
        }
        None
    }

    fn server_base_is_btc_like_delphi(&self) -> bool {
        self.server_base_currency_code == Some(BaseCurrency::BTC)
            || self
                .server_base_currency_name
                .as_deref()
                .is_some_and(|name| same_text_ascii(name, "BTC"))
    }
}

struct MarketCurrencyRef {
    name: String,
    bn_market_currency: String,
    base_currency: String,
}

fn collect_market_currency_refs_like_delphi(markets: &[MarketHandle]) -> Vec<MarketCurrencyRef> {
    markets
        .iter()
        .map(|handle| {
            let name = handle.name_str().to_string();
            let (bn_market_currency, base_currency) = handle.with(|market| {
                (
                    market.bn_market_currency.clone(),
                    market.base_currency.clone(),
                )
            });
            MarketCurrencyRef {
                name,
                bn_market_currency,
                base_currency,
            }
        })
        .collect()
}
