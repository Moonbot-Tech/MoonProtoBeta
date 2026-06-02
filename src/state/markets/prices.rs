//! `UpdateMarketsList` price-apply path.

use std::sync::Arc;

use crate::commands::candles::current_local_time_shift_minutes;
use crate::commands::market::{
    apply_delphi_local_funding_shift, CorrMarketPriceUpdate, EngineStreamReader, Market,
    MarketPriceUpdate, MarketsPricesResponse, CORR_PRICE_ROW_MIN_SIZE,
    MARKET_PRICE_ROW_MIN_SIZE_NO_FUNDING, MARKET_PRICE_ROW_MIN_SIZE_WITH_FUNDING,
    MAX_MARKET_PRICE_UPDATE_ROWS,
};

use super::{same_text_ascii, MarketLastPriceHistoryInput, MarketsEvent, MarketsState};

impl MarketsState {
    /// Apply the `emk_UpdateMarketsList` response.
    /// Updates the market price, resolving the server `mIndex` via `emk_GetMarketsIndexes`.
    /// If the mapping is unknown or stale after a server restart — the row is skipped.
    pub fn apply_markets_prices(&mut self, resp: MarketsPricesResponse) -> MarketsEvent {
        let count = resp.prices.len();
        for handle in self.markets.iter() {
            handle.with_mut(|m| m.price.mark_price_found = false);
        }
        let base_usdt_context = self.base_usdt_market_context();
        for p in &resp.prices {
            self.apply_one_market_price_update(p, resp.send_funding, &base_usdt_context);
        }
        if resp.send_corr_markets {
            for c in &resp.corr_prices {
                self.apply_one_corr_price_update(c);
            }
        }
        self.update_currency_prices();
        MarketsEvent::PricesUpdated {
            count,
            included_funding: resp.send_funding,
            included_corr: resp.send_corr_markets,
        }
    }

    /// Active-library direct counterpart of Delphi `UpdateMarketsList`.
    ///
    /// Delphi mutates market prices inside the read loop. If a later corr-market
    /// string read raises, already-applied prices remain. The pure parser remains
    /// a low-level command helper; dispatcher uses this method for protocol state.
    // parity: MoonBot MoonProtoEngine.pas:UpdateMarketsList
    pub(crate) fn apply_markets_prices_payload(&mut self, data: &[u8]) -> Option<MarketsEvent> {
        self.apply_markets_prices_payload_collecting_last_price(data, None)
    }

    // parity: MoonBot MoonProtoEngine.pas:UpdateMarketsList (+ LastPrice history collect)
    pub(crate) fn apply_markets_prices_payload_collecting_last_price(
        &mut self,
        data: &[u8],
        last_price_rows: Option<&mut Vec<MarketLastPriceHistoryInput>>,
    ) -> Option<MarketsEvent> {
        self.apply_markets_prices_payload_with_local_shift(
            data,
            current_local_time_shift_minutes(),
            last_price_rows,
        )
    }

    pub(super) fn apply_markets_prices_payload_with_local_shift(
        &mut self,
        data: &[u8],
        local_shift_minutes: f64,
        mut last_price_rows: Option<&mut Vec<MarketLastPriceHistoryInput>>,
    ) -> Option<MarketsEvent> {
        for handle in self.markets.iter() {
            handle.with_mut(|m| m.price.mark_price_found = false);
        }

        let mut r = EngineStreamReader::new(data);
        let send_funding = r.read_bool()?;
        let min_price_row_size = if send_funding {
            MARKET_PRICE_ROW_MIN_SIZE_WITH_FUNDING
        } else {
            MARKET_PRICE_ROW_MIN_SIZE_NO_FUNDING
        };
        let count = r.read_count_bounded(
            min_price_row_size,
            MAX_MARKET_PRICE_UPDATE_ROWS,
            "UpdateMarketsList.prices",
        )?;
        let base_usdt_context = self.base_usdt_market_context();

        for _ in 0..count {
            let update = read_market_price_update(&mut r, send_funding, local_shift_minutes)?;
            if let Some(row) =
                self.apply_one_market_price_update(&update, send_funding, &base_usdt_context)
            {
                if let Some(rows) = last_price_rows.as_deref_mut() {
                    rows.push(row);
                }
            }
        }

        let send_corr_markets = r.read_bool()?;
        if send_corr_markets {
            let corr_count = r.read_count_bounded(
                CORR_PRICE_ROW_MIN_SIZE,
                usize::MAX,
                "UpdateMarketsList.corr_prices",
            )?;
            for _ in 0..corr_count {
                let update = read_corr_price_update(&mut r)?;
                self.apply_one_corr_price_update(&update);
            }
        }

        self.update_currency_prices();
        Some(MarketsEvent::PricesUpdated {
            count,
            included_funding: send_funding,
            included_corr: send_corr_markets,
        })
    }

    /// Build retained LastPrice rows from the current market-price state.
    ///
    /// This is the Active Lib backfill for the common order:
    /// Init `UpdateMarketsList` first, `subscribe_all_trades` later. Delphi has
    /// one always-live `TMarket.HistoryPrice`; Rust creates retained stores only
    /// after the agreed trades-storage opt-in, so the already-known `pLast`
    /// values must be copied once when the storage scope becomes active.
    // parity: MoonBot MarketsU.pas:TMarket.AddFrom (LastPrice history backfill)
    pub(crate) fn current_last_price_history_rows(&self) -> Vec<MarketLastPriceHistoryInput> {
        let mut rows = Vec::new();
        let base_usdt_context = self.base_usdt_market_context();
        for handle in self.markets.iter() {
            let market_name = handle.name_arc();
            let (price, is_btc_market, is_base_usdt_market) = handle.with(|market| {
                (
                    market.price,
                    market.is_btc_market,
                    base_usdt_context.is_base_usdt_market(market),
                )
            });
            rows.push(MarketLastPriceHistoryInput {
                market_name,
                current: price.p_last,
                bid: price.bid,
                ask: price.ask,
                mark_price: price.mark_price,
                mark_price_found: price.mark_price_found,
                is_btc_market,
                is_base_usdt_market,
            });
        }
        rows
    }

    fn apply_one_market_price_update(
        &mut self,
        p: &MarketPriceUpdate,
        send_funding: bool,
        base_usdt_context: &BaseUsdtMarketContext,
    ) -> Option<MarketLastPriceHistoryInput> {
        if let Some(idx) = self.local_pos_for_server_index(p.m_index) {
            let handle = self.markets.get(idx).cloned()?;
            let market_name = handle.name_arc();
            // Delphi `AddNewAksPrice` (MarketsU.pas:8510,8516) gates and computes
            // ChartPriceStep against `_epsM`, not `_eps`.
            let eps_m = self.eps_profile.eps_m;
            // The price lives on the `Market` (Delphi `TMarket`): write on the shared object
            // through a per-market lock, without cloning the markets container on price-apply.
            let row = handle.with_mut(|market| {
                if send_funding {
                    market.funding_rate = p.funding_rate;
                    market.funding_time = p.funding_time;
                }
                let is_btc_market = market.is_btc_market;
                let is_base_usdt_market = base_usdt_context.is_base_usdt_market(market);
                let bn_step_size = market.bn_step_size;
                let bn_min_qty = market.bn_min_qty;
                let bn_min_notional = market.bn_min_notional;
                let price = &mut market.price;
                price.bid = p.bid;
                price.ask = p.ask;
                price.last_bid = price.bid;
                price.last_ask = price.ask;
                price.p_last = (price.bid + price.ask) * 0.5;
                price.min_lot_size =
                    (bn_step_size.max(bn_min_qty) * price.p_last).max(bn_min_notional);
                if price.ask > eps_m {
                    price.chart_price_step = eps_m.max(price.ask / 5000.0);
                }
                if send_funding {
                    price.funding_rate = p.funding_rate;
                    price.funding_time = p.funding_time;
                }
                price.mark_price = p.mark_price;
                price.mark_price_found = p.mark_price_found;
                MarketLastPriceHistoryInput {
                    market_name,
                    current: price.p_last,
                    bid: price.bid,
                    ask: price.ask,
                    mark_price: price.mark_price,
                    mark_price_found: price.mark_price_found,
                    is_btc_market,
                    is_base_usdt_market,
                }
            });
            Some(row)
        } else if self.price_row_points_to_missing_market(p.m_index) {
            self.markets_list_refresh_needed = true;
            None
        } else {
            None
        }
    }

    // parity: MoonBot MarketsU.pas:TMarkets (base/USDT market resolution)
    fn base_usdt_market_context(&self) -> BaseUsdtMarketContext {
        let server_base_currency = self.server_base_currency_name.clone();
        let (usdt_market, usdt_rev_market) = server_base_currency
            .as_deref()
            .and_then(|base_currency| self.base_currency_price(base_currency))
            .map(|base_price| {
                (
                    base_price.usdt_market.clone(),
                    base_price.usdt_rev_market.clone(),
                )
            })
            .unwrap_or_default();
        BaseUsdtMarketContext {
            server_base_currency,
            usdt_market,
            usdt_rev_market,
        }
    }

    fn apply_one_corr_price_update(&mut self, c: &CorrMarketPriceUpdate) {
        if self.corr_markets.contains_key(&c.bn_market_name) {
            Arc::make_mut(&mut self.corr_prices).insert(c.bn_market_name.clone(), c.last_price);
        }
    }
}

struct BaseUsdtMarketContext {
    server_base_currency: Option<String>,
    usdt_market: Option<String>,
    usdt_rev_market: Option<String>,
}

impl BaseUsdtMarketContext {
    // parity: MoonBot MarketsU.pas:TMarket.IsBaseUSDTMarket
    fn is_base_usdt_market(&self, market: &Market) -> bool {
        let market_name = market.bn_market_name.as_str();
        if let Some(base_currency) = self.server_base_currency.as_deref() {
            if self
                .usdt_market
                .as_deref()
                .is_some_and(|name| same_text_ascii(name, market_name))
                || self
                    .usdt_rev_market
                    .as_deref()
                    .is_some_and(|name| same_text_ascii(name, market_name))
            {
                return true;
            }
            if !same_text_ascii(base_currency, "USDT")
                && same_text_ascii(&market.bn_market_currency, base_currency)
                && same_text_ascii(&market.base_currency, "USDT")
            {
                return true;
            }
        }

        same_text_ascii(market_name, "BTCUSDT")
            || same_text_ascii(market_name, "BTC_USDT")
            || (market.is_btc_market && same_text_ascii(&market.base_currency, "USDT"))
    }
}

// parity: MoonBot MoonProtoEngine.pas:UpdateMarketsList (per-market price record read)
fn read_market_price_update(
    r: &mut EngineStreamReader<'_>,
    send_funding: bool,
    local_shift_minutes: f64,
) -> Option<MarketPriceUpdate> {
    let m_index = r.read_word()?;
    let bid = r.read_double()?;
    let ask = r.read_double()?;
    let (funding_rate, funding_time) = if send_funding {
        (
            r.read_double()?,
            apply_delphi_local_funding_shift(r.read_double()?, local_shift_minutes),
        )
    } else {
        (0.0, 0.0)
    };
    let mark_price = r.read_double()?;
    let mark_price_found = r.read_bool()?;
    Some(MarketPriceUpdate {
        m_index,
        bid,
        ask,
        funding_rate,
        funding_time,
        mark_price,
        mark_price_found,
    })
}

// parity: MoonBot MoonProtoEngine.pas:UpdateMarketsList (corr-market price record read)
fn read_corr_price_update(r: &mut EngineStreamReader<'_>) -> Option<CorrMarketPriceUpdate> {
    let bn_market_name = r.read_str()?;
    let last_price = r.read_double()?;
    Some(CorrMarketPriceUpdate {
        bn_market_name,
        last_price,
    })
}
