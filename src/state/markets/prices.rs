//! `UpdateMarketsList` price-apply path.

use std::sync::Arc;

use crate::commands::candles::current_local_time_shift_minutes;
#[cfg(test)]
use crate::commands::candles::DeepPrice;
use crate::commands::market::{
    apply_delphi_local_funding_shift, CorrMarketPriceUpdate, EngineStreamReader, Market,
    MarketPriceUpdate, MarketsPricesResponse, CORR_PRICE_ROW_MIN_SIZE,
    MARKET_PRICE_ROW_MIN_SIZE_NO_FUNDING, MARKET_PRICE_ROW_MIN_SIZE_WITH_FUNDING,
    MAX_MARKET_PRICE_UPDATE_ROWS,
};
#[cfg(test)]
use crate::time::MILLIS_PER_HOUR;
#[cfg(test)]
use crate::MoonTime;

use super::{
    same_text_ascii, MarketHandle, MarketLastPriceHistoryInput, MarketsEvent, MarketsState,
};

impl MarketsState {
    /// Apply the `emk_UpdateMarketsList` response.
    /// Updates the market price, resolving the server `mIndex` via `emk_GetMarketsIndexes`.
    /// If the mapping is unknown or stale after a server restart — the row is skipped.
    pub fn apply_markets_prices(&mut self, resp: MarketsPricesResponse) -> MarketsEvent {
        self.apply_markets_prices_at(resp, 0)
    }

    pub(crate) fn apply_markets_prices_at(
        &mut self,
        resp: MarketsPricesResponse,
        now_ms: i64,
    ) -> MarketsEvent {
        let count = resp.prices.len();
        for handle in self.markets.iter() {
            handle.with_mut(|m| m.price.mark_price_found = false);
        }
        let base_usdt_context = self.base_usdt_market_context();
        for p in &resp.prices {
            self.apply_one_market_price_update(p, resp.send_funding, &base_usdt_context, now_ms);
        }
        if resp.send_corr_markets {
            for c in &resp.corr_prices {
                self.apply_one_corr_price_update(c);
            }
        }
        self.update_currency_prices();
        self.refresh_exchange_signed_deltas();
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
    #[cfg(test)]
    pub(crate) fn apply_markets_prices_payload(&mut self, data: &[u8]) -> Option<MarketsEvent> {
        self.apply_markets_prices_payload_collecting_last_price(data, None)
    }

    // parity: MoonBot MoonProtoEngine.pas:UpdateMarketsList (+ LastPrice history collect)
    #[cfg(test)]
    pub(crate) fn apply_markets_prices_payload_collecting_last_price(
        &mut self,
        data: &[u8],
        last_price_rows: Option<&mut Vec<MarketLastPriceHistoryInput>>,
    ) -> Option<MarketsEvent> {
        self.apply_markets_prices_payload_collecting_last_price_at(data, last_price_rows, 0)
    }

    pub(crate) fn apply_markets_prices_payload_collecting_last_price_at(
        &mut self,
        data: &[u8],
        last_price_rows: Option<&mut Vec<MarketLastPriceHistoryInput>>,
        now_ms: i64,
    ) -> Option<MarketsEvent> {
        self.apply_markets_prices_payload_with_local_shift_at(
            data,
            current_local_time_shift_minutes(),
            last_price_rows,
            now_ms,
        )
    }

    #[cfg(test)]
    pub(super) fn apply_markets_prices_payload_with_local_shift(
        &mut self,
        data: &[u8],
        local_shift_minutes: f64,
        last_price_rows: Option<&mut Vec<MarketLastPriceHistoryInput>>,
    ) -> Option<MarketsEvent> {
        self.apply_markets_prices_payload_with_local_shift_at(
            data,
            local_shift_minutes,
            last_price_rows,
            0,
        )
    }

    pub(super) fn apply_markets_prices_payload_with_local_shift_at(
        &mut self,
        data: &[u8],
        local_shift_minutes: f64,
        mut last_price_rows: Option<&mut Vec<MarketLastPriceHistoryInput>>,
        now_ms: i64,
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
            if let Some(row) = self.apply_one_market_price_update(
                &update,
                send_funding,
                &base_usdt_context,
                now_ms,
            ) {
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
        self.refresh_exchange_signed_deltas();
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
        now_ms: i64,
    ) -> Option<MarketLastPriceHistoryInput> {
        if let Some(idx) = self.local_pos_for_server_index(p.m_index) {
            let handle = self.markets.get(idx).cloned()?;
            let market_name = handle.name_arc();
            // Delphi `AddNewAksPrice` (MarketsU.pas:8510,8516) gates and computes
            // ChartPriceStep against `_epsM`, not `_eps`.
            let eps_m = self.eps_profile.eps_m;
            // The price lives on the `Market` (Delphi `TMarket`): write on the shared object
            // through a per-market lock, without cloning the markets container on price-apply.
            let (row, is_global_btc_base_market) = handle.with_mut(|market| {
                if send_funding {
                    market.funding_rate = p.funding_rate;
                    market.funding_time = p.funding_time;
                }
                let is_btc_market = market.is_btc_market;
                let is_base_usdt_market = base_usdt_context.is_base_usdt_market(market);
                let is_global_btc_base_market = base_usdt_context.is_global_btc_base_market(market);
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
                let p_mean = if price.bid > eps_m && price.ask > eps_m {
                    (price.bid + price.ask) * 0.5
                } else {
                    price.bid.max(price.ask)
                };
                market
                    .delta_state
                    .apply_price_mean(p_mean, now_ms, self.eps_profile.eps, eps_m);
                if send_funding {
                    price.funding_rate = p.funding_rate;
                    price.funding_time = p.funding_time;
                }
                price.mark_price = p.mark_price;
                price.mark_price_found = p.mark_price_found;
                (
                    MarketLastPriceHistoryInput {
                        market_name,
                        current: price.p_last,
                        bid: price.bid,
                        ask: price.ask,
                        mark_price: price.mark_price,
                        mark_price_found: price.mark_price_found,
                        is_btc_market,
                        is_base_usdt_market,
                    },
                    is_global_btc_base_market,
                )
            });
            if is_global_btc_base_market {
                self.apply_global_btc_delta_from_base_price(row.current, now_ms);
            }
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

    #[cfg(test)]
    pub(crate) fn apply_candles_delta_baselines<'a, I>(
        &mut self,
        markets: I,
        now_time: MoonTime,
        now_ms: i64,
    ) where
        I: IntoIterator<Item = (&'a str, &'a [DeepPrice])>,
    {
        let base_usdt_context = self.base_usdt_market_context();
        for (market_name, candles) in markets {
            let Some(handle) = self.handles_by_name.get(market_name).cloned() else {
                continue;
            };
            let is_global_btc_base_market =
                handle.with(|market| base_usdt_context.is_global_btc_base_market(market));
            let Some(baseline) =
                candle_delta_baseline(candles, now_time, is_global_btc_base_market)
            else {
                continue;
            };
            self.apply_candle_delta_baseline(handle, baseline, now_ms, is_global_btc_base_market);
        }
        self.refresh_exchange_signed_deltas();
    }

    pub(crate) fn apply_candles_delta_baselines_precomputed<'a, I>(
        &mut self,
        markets: I,
        now_ms: i64,
    ) where
        I: IntoIterator<Item = (&'a str, CandleDeltaBaseline)>,
    {
        let base_usdt_context = self.base_usdt_market_context();
        for (market_name, baseline) in markets {
            let Some(handle) = self.handles_by_name.get(market_name).cloned() else {
                continue;
            };
            let is_global_btc_base_market =
                handle.with(|market| base_usdt_context.is_global_btc_base_market(market));
            self.apply_candle_delta_baseline(handle, baseline, now_ms, is_global_btc_base_market);
        }
        self.refresh_exchange_signed_deltas();
    }

    fn apply_candle_delta_baseline(
        &mut self,
        handle: MarketHandle,
        baseline: CandleDeltaBaseline,
        now_ms: i64,
        is_global_btc_base_market: bool,
    ) {
        handle.with_mut(|market| {
            market.delta_state.coin_1h_avg = baseline.coin_1h_avg;
            market.delta_state.coin_24h_avg = baseline.coin_24h_avg;
            market.delta_state.last_update_avg_ms = now_ms;
            if baseline.coin_1h_avg <= self.eps_profile.eps {
                market.delta_state.coin_1h_delta = 0.0;
                market.delta_state.coin_1h_delta_ema = 0.0;
            }
            if baseline.coin_24h_avg <= self.eps_profile.eps {
                market.delta_state.coin_24h_delta = 0.0;
                market.delta_state.coin_24h_delta_ema = 0.0;
            }
        });
        if is_global_btc_base_market {
            if baseline.btc_1h_avg > self.eps_profile.eps {
                self.global_deltas.btc_1h_avg = baseline.btc_1h_avg;
            }
            if baseline.btc_24h_avg > self.eps_profile.eps {
                self.global_deltas.btc_24h_avg = baseline.btc_24h_avg;
            }
            if baseline.btc_72h_avg > self.eps_profile.eps {
                self.global_deltas.btc_72h_avg = baseline.btc_72h_avg;
            }
            self.last_update_delta500_ms = now_ms;
        }
    }

    fn apply_global_btc_delta_from_base_price(&mut self, x_avg: f64, now_ms: i64) {
        if x_avg <= self.eps_profile.eps_m {
            return;
        }
        if self.global_deltas.btc_1h_avg > self.eps_profile.eps
            && (self.last_update_delta500_ms - now_ms).abs() > 30_000
        {
            self.last_update_delta500_ms = now_ms;
            self.global_deltas.btc_1h_avg = x_avg * 0.01 + self.global_deltas.btc_1h_avg * 0.99;
        }
        if self.global_deltas.btc_1h_avg > self.eps_profile.eps {
            self.global_deltas.btc_1h_delta =
                (x_avg - self.global_deltas.btc_1h_avg) / self.global_deltas.btc_1h_avg * 100.0;
        }
        if self.global_deltas.btc_24h_avg > self.eps_profile.eps {
            self.global_deltas.btc_24h_delta =
                (x_avg - self.global_deltas.btc_24h_avg) / self.global_deltas.btc_24h_avg * 100.0;
        }
        if self.global_deltas.btc_72h_avg > self.eps_profile.eps {
            self.global_deltas.btc_72h_delta =
                (x_avg - self.global_deltas.btc_72h_avg) / self.global_deltas.btc_72h_avg * 100.0;
        }
    }

    pub(super) fn refresh_exchange_signed_deltas(&mut self) {
        let mut exchange_1h = 0.0;
        let mut exchange_24h = 0.0;
        let mut count = 0usize;
        let exclude_blacklisted = self.exclude_blacklisted_markets_from_exchange_delta;
        for handle in self.markets.iter() {
            handle.with(|market| {
                if !market.is_btc_market
                    || !market.status_trading
                    || (exclude_blacklisted && market.market_blacklisted_cfg)
                    || same_text_ascii(&market.market_currency, "TUSD")
                    || same_text_ascii(&market.market_currency, "PAXG")
                {
                    return;
                }
                count += 1;
                exchange_1h += market.delta_state.coin_1h_delta;
                exchange_24h += market.delta_state.coin_24h_delta_ema;
            });
        }
        // Delphi keeps the old "trimmed average" denominator even though the
        // min/max subtraction is currently commented out in `Bworks.pas`.
        // Machine-effect parity is therefore: sum included markets, then divide
        // by `count - 2` only when that denominator is positive.
        let denom = count.saturating_sub(2);
        if denom > 0 {
            exchange_1h /= denom as f64;
            exchange_24h /= denom as f64;
        }
        self.global_deltas.exchange_1h_delta = exchange_1h;
        self.global_deltas.exchange_24h_delta = exchange_24h;
        self.global_deltas.exchange_market_count = count;
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct CandleDeltaBaseline {
    pub(crate) coin_1h_avg: f64,
    pub(crate) coin_24h_avg: f64,
    pub(crate) btc_1h_avg: f64,
    pub(crate) btc_24h_avg: f64,
    pub(crate) btc_72h_avg: f64,
}

#[cfg(test)]
fn candle_delta_baseline(
    candles: &[DeepPrice],
    now_time: MoonTime,
    is_base_usdt_market: bool,
) -> Option<CandleDeltaBaseline> {
    if candles.len() < 3 || candles.last().is_none_or(|c| c.time <= 0.0) {
        return None;
    }
    let now_ms = now_time.unix_millis();
    let mut coin_1h_sum = 0.0;
    let mut coin_1h_count = 0usize;
    let mut coin_24h_sum = 0.0;
    let mut coin_24h_count = 0usize;
    let mut btc_1h_sum = 0.0;
    let mut btc_1h_count = 0usize;
    let mut btc_24h_sum = 0.0;
    let mut btc_24h_count = 0usize;
    let mut btc_72h_sum = 0.0;
    let mut btc_72h_count = 0usize;

    for candle in candles.iter().rev() {
        if candle.time <= 0.0 {
            continue;
        }
        let candle_time_ms = candle.time().unix_millis();
        let h = ((now_ms - candle_time_ms) as f64 / MILLIS_PER_HOUR as f64).trunc() as i32;
        if h < 0 {
            continue;
        }
        let mean = f64::from(candle.open() + candle.close() + candle.high() + candle.low()) * 0.25;
        if h == 0 {
            coin_1h_sum += mean;
            coin_1h_count += 1;
        }
        if h <= 24 {
            coin_24h_sum += mean;
            coin_24h_count += 1;
        }
        if h < 72 && is_base_usdt_market {
            if h == 0 {
                btc_1h_sum += mean;
                btc_1h_count += 1;
            }
            if h <= 24 {
                btc_24h_sum += mean;
                btc_24h_count += 1;
            }
            btc_72h_sum += mean;
            btc_72h_count += 1;
        }
    }

    Some(CandleDeltaBaseline {
        coin_1h_avg: avg_or_zero(coin_1h_sum, coin_1h_count),
        coin_24h_avg: avg_or_zero(coin_24h_sum, coin_24h_count),
        btc_1h_avg: avg_or_zero(btc_1h_sum, btc_1h_count),
        btc_24h_avg: avg_or_zero(btc_24h_sum, btc_24h_count),
        btc_72h_avg: avg_or_zero(btc_72h_sum, btc_72h_count),
    })
}

#[cfg(test)]
fn avg_or_zero(sum: f64, count: usize) -> f64 {
    if count == 0 {
        0.0
    } else {
        sum / count as f64
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

    // parity: MoonBot MarketsU.pas:GetUSDMarket + TMarket.IsBaseUSDTMarket
    fn is_global_btc_base_market(&self, market: &Market) -> bool {
        let market_name = market.bn_market_name.as_str();
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
        same_text_ascii(market_name, "USDT-BTC")
            || same_text_ascii(market_name, "BTCUSDT")
            || same_text_ascii(market_name, "BTC_USDT")
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
