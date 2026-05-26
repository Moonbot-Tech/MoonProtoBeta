//! Base-currency and CorrMarket reference maintenance.

use std::collections::HashMap;

use crate::commands::market::BaseCurrency;

use super::{
    text::{norm_text_ascii, replace_text_ascii_case_insensitive, same_text_ascii},
    BaseCurrencyPrice, CorrMarket, MarketsState, EPS_MARKET,
};

impl MarketsState {
    pub(super) fn apply_one_corr_market_from_list(&mut self, cm: CorrMarket) {
        if cm.base_currency_name.is_empty() {
            return;
        }
        self.ensure_base_currency_price(&cm.base_currency_name);
        if let Some(existing) = self.corr_markets.get_mut(&cm.bn_market_name) {
            existing.bn_tick_size = cm.bn_tick_size;
            existing.base_currency_name = cm.base_currency_name;
        } else {
            self.corr_markets.insert(cm.bn_market_name.clone(), cm);
        }
    }

    fn ensure_base_currency_price(&mut self, base_currency: &str) {
        if base_currency.is_empty() || self.base_currency_prices.contains_key(base_currency) {
            return;
        }
        self.base_currency_prices.insert(
            base_currency.to_string(),
            BaseCurrencyPrice::new(base_currency.to_string()),
        );
    }

    pub(crate) fn set_server_base_currency(&mut self, name: Option<&str>, code: Option<u8>) {
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
            let (market_name, corr_name) = handle.with(|market| {
                (
                    market.bn_market_name.clone(),
                    replace_text_ascii_case_insensitive(&market.bn_market_name, currency, "BTC"),
                )
            });
            if market_name.is_empty() {
                continue;
            }
            if self.corr_markets.contains_key(&corr_name) {
                self.ref_btc_corr_markets
                    .insert(market_name.clone(), corr_name);
            } else {
                self.ref_btc_corr_markets.remove(&market_name);
            }
        }
    }

    pub(super) fn check_currency_ref_markets_like_delphi(&mut self) {
        // Same final assignments as Delphi nested scans, but indexed first so
        // the protocol tick does not scale as BaseCurDict * CorrDict in Rust.
        let mut usdt_market_by_key = HashMap::new();
        let mut usdt_rev_market_by_key = HashMap::new();
        for handle in self.markets.iter() {
            let (base_currency, bn_market_currency, bn_market_name) = handle.with(|market| {
                (
                    market.base_currency.clone(),
                    market.bn_market_currency.clone(),
                    market.bn_market_name.clone(),
                )
            });
            if same_text_ascii(&base_currency, "USDT") {
                usdt_market_by_key
                    .insert(norm_text_ascii(&bn_market_currency), bn_market_name.clone());
            }
            if same_text_ascii(&bn_market_currency, "USDT") {
                usdt_rev_market_by_key.insert(norm_text_ascii(&base_currency), bn_market_name);
            }
        }

        let mut usdt_corr_market_by_key = HashMap::new();
        let mut usdt_rev_corr_market_by_key = HashMap::new();
        for cm in self.corr_markets.values() {
            if same_text_ascii(&cm.base_currency_name, "USDT") {
                usdt_corr_market_by_key.insert(
                    norm_text_ascii(&cm.bn_market_currency),
                    cm.bn_market_name.clone(),
                );
            }
            if same_text_ascii(&cm.bn_market_currency, "USDT") {
                usdt_rev_corr_market_by_key.insert(
                    norm_text_ascii(&cm.base_currency_name),
                    cm.bn_market_name.clone(),
                );
            }
        }

        let keys = self
            .base_currency_prices
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            let norm_key = norm_text_ascii(&key);
            let usdt_market = usdt_market_by_key.get(&norm_key).cloned();
            let usdt_rev_market = usdt_rev_market_by_key.get(&norm_key).cloned();
            let usdt_corr_market = usdt_corr_market_by_key.get(&norm_key).cloned();
            let usdt_rev_corr_market = usdt_rev_corr_market_by_key.get(&norm_key).cloned();

            let Some(bc) = self.base_currency_prices.get_mut(&key) else {
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
                if let Some(bc) = self.base_currency_prices.get_mut(&key) {
                    bc.last_price = price;
                }
            }
        }
    }

    fn next_base_currency_price_like_delphi(&self, bc: &BaseCurrencyPrice) -> Option<f64> {
        if let Some(price) = bc
            .usdt_market
            .as_deref()
            .and_then(|name| self.price(name))
            .map(|p| p.ask)
            .filter(|ask| *ask > EPS_MARKET)
        {
            return Some(price);
        }
        if let Some(price) = bc
            .usdt_rev_market
            .as_deref()
            .and_then(|name| self.price(name))
            .map(|p| p.ask)
            .filter(|ask| *ask > EPS_MARKET)
        {
            return Some(1.0 / price);
        }
        if let Some(price) = bc
            .usdt_corr_market
            .as_deref()
            .and_then(|name| self.corr_prices.get(name))
            .copied()
            .filter(|price| *price > EPS_MARKET)
        {
            return Some(price);
        }
        if let Some(price) = bc
            .usdt_rev_corr_market
            .as_deref()
            .and_then(|name| self.corr_prices.get(name))
            .copied()
            .filter(|price| *price > EPS_MARKET)
        {
            return Some(1.0 / price);
        }
        if same_text_ascii(&bc.base_currency, "USDT") {
            return Some(1.0);
        }
        None
    }

    fn server_base_is_btc_like_delphi(&self) -> bool {
        self.server_base_currency_code == Some(BaseCurrency::BTC.to_byte())
            || self
                .server_base_currency_name
                .as_deref()
                .is_some_and(|name| same_text_ascii(name, "BTC"))
    }
}
