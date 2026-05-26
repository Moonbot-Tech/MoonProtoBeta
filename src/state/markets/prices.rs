//! `UpdateMarketsList` price-apply path.

use crate::commands::candles::current_local_time_shift_minutes;
use crate::commands::market::{
    apply_delphi_local_funding_shift, CorrMarketPriceUpdate, EngineStreamReader, Market,
    MarketPriceUpdate, MarketsPricesResponse,
};

use super::{same_text_ascii, MarketLastPriceHistoryInput, MarketsEvent, MarketsState, EPS_MARKET};

impl MarketsState {
    /// Применить ответ `emk_UpdateMarketsList`.
    /// Обновляет цену рынка, резолвя server `mIndex` через `emk_GetMarketsIndexes`.
    /// Если mapping неизвестен или stale после server restart — запись пропускается.
    pub fn apply_markets_prices(&mut self, resp: MarketsPricesResponse) -> MarketsEvent {
        let count = resp.prices.len();
        for slot in &mut self.prices {
            slot.mark_price_found = false;
        }
        for p in &resp.prices {
            self.apply_one_market_price_update(p, resp.send_funding);
        }
        if resp.send_corr_markets {
            for c in &resp.corr_prices {
                self.apply_one_corr_price_update(c);
            }
        }
        self.update_currency_prices_like_delphi();
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
    pub(crate) fn apply_markets_prices_payload_like_delphi(
        &mut self,
        data: &[u8],
    ) -> Option<MarketsEvent> {
        self.apply_markets_prices_payload_collecting_last_price_like_delphi(data, None)
    }

    pub(crate) fn apply_markets_prices_payload_collecting_last_price_like_delphi(
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
        for slot in &mut self.prices {
            slot.mark_price_found = false;
        }

        let mut r = EngineStreamReader::new(data);
        let send_funding = r.read_bool()?;
        let count = r.read_count()?;

        for _ in 0..count {
            let update =
                read_market_price_update_like_delphi(&mut r, send_funding, local_shift_minutes)?;
            if let Some(row) = self.apply_one_market_price_update(&update, send_funding) {
                if let Some(rows) = last_price_rows.as_deref_mut() {
                    rows.push(row);
                }
            }
        }

        let send_corr_markets = r.read_bool()?;
        if send_corr_markets {
            let corr_count = r.read_count()?;
            for _ in 0..corr_count {
                let update = read_corr_price_update_like_delphi(&mut r)?;
                self.apply_one_corr_price_update(&update);
            }
        }

        self.update_currency_prices_like_delphi();
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
    pub(crate) fn current_last_price_history_rows_like_delphi(
        &self,
    ) -> Vec<MarketLastPriceHistoryInput> {
        let mut rows = Vec::new();
        for (idx, handle) in self.markets.iter().enumerate() {
            let Some(slot) = self.prices.get(idx) else {
                continue;
            };
            let (market_name, is_btc_market, is_base_usdt_market) = handle.with(|market| {
                (
                    market.bn_market_name.clone(),
                    market.is_btc_market,
                    self.market_is_base_usdt_market_like_delphi(market),
                )
            });
            rows.push(MarketLastPriceHistoryInput {
                market_name,
                current: slot.p_last,
                bid: slot.bid,
                ask: slot.ask,
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
    ) -> Option<MarketLastPriceHistoryInput> {
        if let Some(idx) = self.local_pos_for_server_index(p.m_index) {
            let handle = self.markets.get(idx).cloned()?;
            let (market_name, is_btc_market, is_base_usdt_market) = handle.with(|market| {
                (
                    market.bn_market_name.clone(),
                    market.is_btc_market,
                    self.market_is_base_usdt_market_like_delphi(market),
                )
            });
            let (bn_step_size, bn_min_qty, bn_min_notional) = handle.with_mut(|market| {
                if send_funding {
                    market.funding_rate = p.funding_rate;
                    market.funding_time = p.funding_time;
                }
                (
                    market.bn_step_size,
                    market.bn_min_qty,
                    market.bn_min_notional,
                )
            });
            let slot = &mut self.prices[idx];
            slot.bid = p.bid;
            slot.ask = p.ask;
            slot.last_bid = slot.bid;
            slot.last_ask = slot.ask;
            slot.p_last = (slot.bid + slot.ask) * 0.5;
            slot.min_lot_size = (bn_step_size.max(bn_min_qty) * slot.p_last).max(bn_min_notional);
            if slot.ask > EPS_MARKET {
                slot.chart_price_step = EPS_MARKET.max(slot.ask / 5000.0);
            }
            if send_funding {
                slot.funding_rate = p.funding_rate;
                slot.funding_time = p.funding_time;
            }
            slot.mark_price = p.mark_price;
            slot.mark_price_found = p.mark_price_found;
            Some(MarketLastPriceHistoryInput {
                market_name,
                current: slot.p_last,
                bid: slot.bid,
                ask: slot.ask,
                is_btc_market,
                is_base_usdt_market,
            })
        } else if self.price_row_points_to_missing_market(p.m_index) {
            self.markets_list_refresh_needed = true;
            None
        } else {
            None
        }
    }

    fn market_is_base_usdt_market_like_delphi(&self, market: &Market) -> bool {
        let market_name = market.bn_market_name.as_str();
        if let Some(base_currency) = self.server_base_currency_name.as_deref() {
            if let Some(base_price) = self.base_currency_price(base_currency) {
                if base_price
                    .usdt_market
                    .as_deref()
                    .is_some_and(|name| same_text_ascii(name, market_name))
                    || base_price
                        .usdt_rev_market
                        .as_deref()
                        .is_some_and(|name| same_text_ascii(name, market_name))
                {
                    return true;
                }
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

    fn apply_one_corr_price_update(&mut self, c: &CorrMarketPriceUpdate) {
        if self.corr_markets.contains_key(&c.bn_market_name) {
            self.corr_prices
                .insert(c.bn_market_name.clone(), c.last_price);
        }
    }
}

fn read_market_price_update_like_delphi(
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

fn read_corr_price_update_like_delphi(
    r: &mut EngineStreamReader<'_>,
) -> Option<CorrMarketPriceUpdate> {
    let bn_market_name = r.read_str()?;
    let last_price = r.read_double()?;
    Some(CorrMarketPriceUpdate {
        bn_market_name,
        last_price,
    })
}
