//! Active `MPC_API` response dispatch.
//!
//! Mirrors Delphi `ProcessApiCommand`: apply market/index/tag/base-check side
//! effects first, then emit the original `EngineResponse`.

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
        history_now_time_days: Option<f64>,
        out: &mut Vec<Event>,
    ) {
        match parse_engine_response(payload) {
            Some(resp) => self.process_api_command(resp, history_now_time_days, out),
            None => out.push(Self::parse_failed(Command::API, payload)),
        }
    }

    /// Active dispatcher counterpart of Delphi `TMoonProtoNetClient.ProcessApiCommand`.
    fn process_api_command(
        &mut self,
        resp: EngineResponse,
        history_now_time_days: Option<f64>,
        out: &mut Vec<Event>,
    ) {
        if resp.success {
            match resp.method {
                EngineMethod::GetMarketsList | EngineMethod::UpdateMarketsList => {
                    if resp.method == EngineMethod::GetMarketsList {
                        if let Some(ev) = self
                            .markets
                            .apply_markets_list_payload_like_delphi(&resp.data, resp.ver)
                        {
                            out.push(Event::Markets(ev));
                            let new_markets = self.markets.take_new_markets_added();
                            if !new_markets.is_empty() {
                                out.push(Event::Markets(MarketsEvent::NewMarketsAdded {
                                    names: new_markets,
                                }));
                            }
                        }
                    } else {
                        let wants_history = self.market_history.is_some()
                            && history_now_time_days.is_some()
                            && self.trade_storage_scope.is_some();
                        let mut last_price_rows = Vec::new();
                        let ev = if wants_history {
                            self.markets
                                .apply_markets_prices_payload_collecting_last_price_like_delphi(
                                    &resp.data,
                                    Some(&mut last_price_rows),
                                )
                        } else {
                            self.markets
                                .apply_markets_prices_payload_like_delphi(&resp.data)
                        };
                        if let Some(ev) = ev {
                            if wants_history {
                                self.queue_last_price_history_like_delphi(
                                    history_now_time_days,
                                    last_price_rows,
                                );
                            }
                            out.push(Event::Markets(ev));
                        }
                    }
                }
                EngineMethod::GetMarketsIndexes => {
                    if let Some(names) = parse_markets_indexes_response(&resp.data) {
                        let ev = self.markets.apply_markets_indexes(names);
                        out.push(Event::Markets(ev));
                    }
                }
                EngineMethod::CheckBinanceTags => {
                    if let Some(ev) = self
                        .markets
                        .apply_token_tags_payload_like_delphi(&resp.data)
                    {
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
        out.push(Event::EngineResponse(resp));
    }

    fn queue_last_price_history_like_delphi(
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
            .filter(|row| self.active_trade_storage_allows_market(&row.market_name))
            .map(|row| MarketHistoryLastPriceInput {
                market_name: row.market_name,
                current: row.current,
                bid: row.bid,
                ask: row.ask,
                is_btc_market: row.is_btc_market,
                is_base_usdt_market: row.is_base_usdt_market,
            })
            .collect();
        if rows.is_empty() {
            return;
        }
        handle.send_last_price_batch(MarketHistoryLastPriceBatch { now_time, rows });
    }

    pub(super) fn queue_current_last_price_history_like_delphi(&self, now_time_days: f64) {
        let rows = self.markets.current_last_price_history_rows_like_delphi();
        self.queue_last_price_history_like_delphi(Some(now_time_days), rows);
    }
}
