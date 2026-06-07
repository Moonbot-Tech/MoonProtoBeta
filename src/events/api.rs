//! Active `MPC_API` response dispatch.
//!
//! Mirrors Delphi `ProcessApiCommand`: unmatched/fire-and-forget responses apply
//! active side effects first. Diagnostics builds may also emit the original
//! raw response; normal application code observes typed state/events instead.
//! Responses consumed by a Delphi-style pending caller are applied by that
//! caller after `SendAndWait`/receiver completion.

use super::{copy_max_leverage_from_markets_list, Event, EventDispatcher};
use crate::commands::engine_api::{
    parse_base_check_response, parse_engine_response, EngineMethod, EngineResponse,
};
use crate::commands::market::parse_markets_indexes_response;
use crate::protocol::Command;
use crate::state::eps::EpsProfile;
use crate::state::markets::MarketLastPriceHistoryInput;
use crate::state::{MarketHistoryLastPriceBatch, MarketHistoryLastPriceInput, MarketsEvent};

impl EventDispatcher {
    pub(super) fn client_new_data_api(
        &mut self,
        payload: &[u8],
        now_ms: i64,
        history_now_time_days: Option<f64>,
        out: &mut Vec<Event>,
    ) {
        match parse_engine_response(payload) {
            Some(resp) => self.process_api_command(resp, now_ms, history_now_time_days, out),
            None => Self::push_parse_failed(out, Command::API, payload),
        }
    }

    /// Active dispatcher counterpart of Delphi `TMoonProtoNetClient.ProcessApiCommand`.
    fn process_api_command(
        &mut self,
        resp: EngineResponse,
        now_ms: i64,
        history_now_time_days: Option<f64>,
        out: &mut Vec<Event>,
    ) {
        if resp.success {
            match resp.method {
                EngineMethod::GetMarketsList => {
                    self.apply_get_markets_list_response(&resp, out);
                }
                EngineMethod::UpdateMarketsList => {
                    self.apply_update_markets_list_response(
                        &resp,
                        now_ms,
                        history_now_time_days,
                        out,
                    );
                }
                EngineMethod::GetMarketsIndexes => {
                    if let Some(names) = parse_markets_indexes_response(&resp.data) {
                        let ev = self.markets.apply_markets_indexes(names);
                        out.push(Event::Markets(ev));
                    }
                }
                EngineMethod::CheckBinanceTags => {
                    if let Some(ev) = self.markets.apply_token_tags_payload(&resp.data) {
                        out.push(Event::Markets(ev));
                    }
                }
                EngineMethod::BaseCheck => {
                    let info = parse_base_check_response(&resp.data);
                    self.set_eps_profile(EpsProfile::from_exchange_code(info.exchange_code));
                    self.markets.set_copy_max_leverage_from_markets_list(
                        copy_max_leverage_from_markets_list(&info),
                    );
                    self.markets.set_server_base_currency(
                        info.base_currency_name.as_deref(),
                        info.base_currency_code,
                    );
                }
                _ => {}
            }
        }
        #[cfg(any(test, feature = "diagnostics"))]
        out.push(Event::EngineResponse(resp));
    }

    // parity: MoonBot MoonProtoEngine.pas:ProcessApiCommand (emk_GetMarketsList)
    pub(crate) fn apply_get_markets_list_response(
        &mut self,
        resp: &EngineResponse,
        out: &mut Vec<Event>,
    ) -> bool {
        if !resp.success {
            return false;
        }
        let Some(ev) = self
            .markets
            .apply_markets_list_payload(&resp.data, resp.ver)
        else {
            return false;
        };
        out.push(Event::Markets(ev));
        let new_markets = self.markets.take_new_markets_added();
        if !new_markets.is_empty() {
            out.push(Event::Markets(MarketsEvent::NewMarketsAdded {
                names: new_markets,
            }));
        }
        true
    }

    // parity: MoonBot MoonProtoEngine.pas:ProcessApiCommand (emk_UpdateMarketsList)
    pub(crate) fn apply_update_markets_list_response(
        &mut self,
        resp: &EngineResponse,
        now_ms: i64,
        history_now_time_days: Option<f64>,
        out: &mut Vec<Event>,
    ) -> bool {
        if !resp.success {
            return false;
        }
        let wants_history = self.market_history.is_some()
            && history_now_time_days.is_some()
            && self.trade_storage_scope.is_some();
        let mut last_price_rows = Vec::new();
        let ev = if wants_history {
            self.markets
                .apply_markets_prices_payload_collecting_last_price_at(
                    &resp.data,
                    Some(&mut last_price_rows),
                    now_ms,
                )
        } else {
            self.markets
                .apply_markets_prices_payload_collecting_last_price_at(&resp.data, None, now_ms)
        };
        let Some(ev) = ev else {
            return false;
        };
        if wants_history {
            self.queue_last_price_history(history_now_time_days, last_price_rows);
        }
        out.push(Event::Markets(ev));
        true
    }

    // parity: MoonBot MarketsU.pas:TMarket.AddFrom (LastPrice history backfill)
    fn queue_last_price_history(
        &self,
        history_now_time_days: Option<f64>,
        rows: Vec<MarketLastPriceHistoryInput>,
    ) {
        let (Some(handle), Some(now_time)) = (&self.market_history, history_now_time_days) else {
            return;
        };
        if rows.is_empty() {
            return;
        }
        let rows: Vec<MarketHistoryLastPriceInput> = rows
            .into_iter()
            .filter(|row| self.active_trade_storage_allows_market(row.market_name.as_ref()))
            .map(|row| MarketHistoryLastPriceInput {
                market_name: row.market_name,
                current: row.current,
                bid: row.bid,
                ask: row.ask,
                mark_price: row.mark_price,
                mark_price_found: row.mark_price_found,
                is_btc_market: row.is_btc_market,
                is_base_usdt_market: row.is_base_usdt_market,
            })
            .collect();
        if rows.is_empty() {
            return;
        }
        handle.send_last_price_batch(MarketHistoryLastPriceBatch { now_time, rows });
    }

    // parity: MoonBot MarketsU.pas:TMarket.AddFrom (LastPrice history backfill)
    pub(super) fn queue_current_last_price_history(&self, now_time_days: f64) {
        let rows = self.markets.current_last_price_history_rows();
        self.queue_last_price_history(Some(now_time_days), rows);
    }
}
