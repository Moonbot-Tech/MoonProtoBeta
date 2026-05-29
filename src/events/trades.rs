//! Active `MPC_TradesStream` / `MPC_TradesResendResponse` dispatch.
//!
//! This file keeps the Delphi `ProcessTradesStream` machine-effect block
//! together: packet-number recovery, known-market gating, retained-history
//! append, watcher fills, and signal events.

use super::{Event, EventDispatcher, WatcherFillEvent, WatcherFillsEvent};
use crate::commands::trades_stream::{
    decode_trades_packet, parse_watcher_fills, DecodedTradesPacket, TradeSectionRef,
};
use crate::protocol::Command;
use crate::state::{
    iter_trades_resend_response, MarketHistoryMMOrderInput, MarketHistoryStreamBatch,
    MarketHistoryStreamSection, MarketHistoryStreamSectionKind, MarketHistoryTradeInput,
    TradesPacketEffect, DELPHI_MSECS_PER_DAY,
};

impl EventDispatcher {
    pub(super) fn client_new_data_trades_stream(
        &mut self,
        payload: &[u8],
        now_ms: i64,
        history_now_time_days: Option<f64>,
        out: &mut Vec<Event>,
    ) {
        // Active library: block TradesStream processing until the market indexes are synced.
        if !self.markets.indexes_synchronized {
            return;
        }
        match decode_trades_packet(payload) {
            Some(decoded) => {
                let effects = self.trades.on_packet_header(decoded.packet_num, now_ms);
                self.collect_known_trades_events_like_delphi(
                    &decoded,
                    effects,
                    now_ms,
                    history_now_time_days,
                    out,
                );
            }
            None => out.push(Self::parse_failed(Command::TradesStream, payload)),
        }
    }

    pub(super) fn client_new_data_trades_resend_response(
        &mut self,
        payload: &[u8],
        now_ms: i64,
        history_now_time_days: Option<f64>,
        out: &mut Vec<Event>,
    ) {
        // Delphi `ProcessTradesResendBatch` feeds every inner packet back into
        // `ProcessTradesStream(..., False)`, so the same fresh-index gate applies.
        if !self.markets.indexes_synchronized {
            return;
        }
        for inner in iter_trades_resend_response(payload) {
            match decode_trades_packet(inner) {
                Some(decoded) => {
                    let effects = self.trades.on_packet_resend_header(decoded.packet_num);
                    self.collect_known_trades_events_like_delphi(
                        &decoded,
                        effects,
                        now_ms,
                        history_now_time_days,
                        out,
                    );
                }
                None => out.push(Self::parse_failed(Command::TradesResendResponse, inner)),
            }
        }
    }

    fn ensure_trades_packet_time_shift_like_delphi(
        base_time: f64,
        time_delta_ms: i16,
        now_time_days: Option<f64>,
        packet_time_shift: &mut Option<f64>,
    ) {
        if packet_time_shift.is_none() {
            if let Some(now_time) = now_time_days {
                let event_time = base_time + f64::from(time_delta_ms) / DELPHI_MSECS_PER_DAY;
                *packet_time_shift = Some(((now_time - event_time) * 24.0).round() / 24.0);
            }
        }
    }

    fn trades_packet_shifted_time_like_delphi(
        base_time: f64,
        time_delta_ms: i16,
        now_time_days: Option<f64>,
        packet_time_shift: &mut Option<f64>,
    ) -> f64 {
        let event_time = base_time + f64::from(time_delta_ms) / DELPHI_MSECS_PER_DAY;
        if packet_time_shift.is_none() {
            if let Some(now_time) = now_time_days {
                *packet_time_shift = Some(((now_time - event_time) * 24.0).round() / 24.0);
            }
        }
        event_time + packet_time_shift.unwrap_or(0.0)
    }

    fn apply_known_trades_sections_like_delphi(
        &mut self,
        decoded: &DecodedTradesPacket<'_>,
        now_ms: Option<i64>,
        history_now_time_days: Option<f64>,
        out: &mut Vec<Event>,
    ) {
        let collect_history =
            history_now_time_days.is_some() && self.market_history.is_some() && now_ms.is_some();
        let mut history_sections = Vec::new();
        let mut history_trade_rows = Vec::new();
        let mut history_mm_order_rows = Vec::new();
        let mut packet_time_shift: Option<f64> = None;
        for section in decoded.sections() {
            match section {
                TradeSectionRef::Trades(rows) => {
                    let market_index = rows.market_index();
                    let is_spot = rows.is_spot();
                    let row_count = rows.len();
                    if row_count == 0 || self.markets.has_server_market_index(market_index) {
                        let market_name = if row_count > 0 {
                            self.markets.market_name_by_index(market_index)
                        } else {
                            None
                        };
                        if let Some(market_name) = market_name {
                            if !self.trade_section_visible_to_active_lib(market_name) {
                                continue;
                            }
                        }
                        let collect_market_history = market_name.is_some_and(|name| {
                            collect_history && self.active_trade_storage_allows_market(name)
                        });
                        let history_start = history_trade_rows.len();
                        if collect_market_history {
                            history_trade_rows.reserve(row_count);
                        }
                        for trade in rows {
                            Self::ensure_trades_packet_time_shift_like_delphi(
                                decoded.base_time,
                                trade.time_delta_ms,
                                history_now_time_days,
                                &mut packet_time_shift,
                            );
                            if let Some(now_ms) = now_ms {
                                self.markets.apply_trade_tail_row_like_delphi(
                                    trade.market_index,
                                    trade.is_spot,
                                    trade.price,
                                    trade.qty,
                                    now_ms,
                                );
                            }
                            if collect_market_history {
                                history_trade_rows.push(MarketHistoryTradeInput {
                                    time_delta_ms: trade.time_delta_ms,
                                    price: trade.price,
                                    qty: trade.qty,
                                });
                            }
                        }
                        let history_len = history_trade_rows.len() - history_start;
                        if collect_market_history && history_len > 0 {
                            history_sections.push(MarketHistoryStreamSection {
                                market_index,
                                kind: if is_spot {
                                    MarketHistoryStreamSectionKind::SpotTrades
                                } else {
                                    MarketHistoryStreamSectionKind::FuturesTrades
                                },
                                start: history_start,
                                len: history_len,
                            });
                        }
                    }
                }
                TradeSectionRef::MMOrders(rows) => {
                    let market_index = rows.market_index();
                    let row_count = rows.len();
                    if row_count == 0 || self.markets.has_server_market_index(market_index) {
                        let market_name = if row_count > 0 {
                            self.markets.market_name_by_index(market_index)
                        } else {
                            None
                        };
                        if let Some(market_name) = market_name {
                            if !self.trade_section_visible_to_active_lib(market_name) {
                                continue;
                            }
                        }
                        let collect_market_history = market_name.is_some_and(|name| {
                            collect_history && self.active_trade_storage_allows_market(name)
                        });
                        let history_start = history_mm_order_rows.len();
                        if collect_market_history {
                            history_mm_order_rows.reserve(row_count);
                        }
                        for row in rows {
                            Self::ensure_trades_packet_time_shift_like_delphi(
                                decoded.base_time,
                                row.time_delta_ms,
                                history_now_time_days,
                                &mut packet_time_shift,
                            );
                            if collect_market_history {
                                history_mm_order_rows.push(MarketHistoryMMOrderInput {
                                    time_delta_ms: row.time_delta_ms,
                                    volume: row.vol,
                                    q: row.q,
                                    taker: row.taker,
                                });
                            }
                        }
                        let history_len = history_mm_order_rows.len() - history_start;
                        if collect_market_history && history_len > 0 {
                            history_sections.push(MarketHistoryStreamSection {
                                market_index,
                                kind: MarketHistoryStreamSectionKind::MMOrders,
                                start: history_start,
                                len: history_len,
                            });
                        }
                    }
                }
                TradeSectionRef::LiqOrders(rows) => {
                    let market_index = rows.market_index();
                    let row_count = rows.len();
                    if row_count == 0 || self.markets.has_server_market_index(market_index) {
                        let market_name = if row_count > 0 {
                            self.markets.market_name_by_index(market_index)
                        } else {
                            None
                        };
                        if let Some(market_name) = market_name {
                            if !self.trade_section_visible_to_active_lib(market_name) {
                                continue;
                            }
                        }
                        let collect_market_history = market_name.is_some_and(|name| {
                            collect_history && self.active_trade_storage_allows_market(name)
                        });
                        let history_start = history_trade_rows.len();
                        if collect_market_history {
                            history_trade_rows.reserve(row_count);
                        }
                        for trade in rows {
                            Self::ensure_trades_packet_time_shift_like_delphi(
                                decoded.base_time,
                                trade.time_delta_ms,
                                history_now_time_days,
                                &mut packet_time_shift,
                            );
                            if collect_market_history {
                                history_trade_rows.push(MarketHistoryTradeInput {
                                    time_delta_ms: trade.time_delta_ms,
                                    price: trade.price,
                                    qty: trade.qty,
                                });
                            }
                        }
                        let history_len = history_trade_rows.len() - history_start;
                        if collect_market_history && history_len > 0 {
                            history_sections.push(MarketHistoryStreamSection {
                                market_index,
                                kind: MarketHistoryStreamSectionKind::Liquidations,
                                start: history_start,
                                len: history_len,
                            });
                        }
                    }
                }
                TradeSectionRef::WatcherFills {
                    market_index,
                    user,
                    data,
                } => {
                    if self.markets.has_server_market_index(market_index) {
                        let Some(market_name) = self.markets.market_name_by_index(market_index)
                        else {
                            continue;
                        };
                        if !self.trade_section_visible_to_active_lib(market_name) {
                            continue;
                        }
                        let Some(records) = parse_watcher_fills(data) else {
                            continue;
                        };
                        let mut fills = Vec::with_capacity(records.len());
                        for fill in records {
                            let time = Self::trades_packet_shifted_time_like_delphi(
                                decoded.base_time,
                                fill.time_delta_ms,
                                history_now_time_days,
                                &mut packet_time_shift,
                            );
                            fills.push(WatcherFillEvent {
                                time_ms: (time * DELPHI_MSECS_PER_DAY).round() as i64,
                                time,
                                price: fill.price,
                                qty: fill.qty,
                                z_btc: fill.z_btc,
                                position: fill.position,
                                order_type: fill.order_type,
                                is_short: fill.is_short(),
                                is_open: fill.is_open(),
                                is_taker: fill.is_taker(),
                            });
                        }
                        if !fills.is_empty() {
                            out.push(Event::WatcherFills(WatcherFillsEvent {
                                market_index,
                                market_name: market_name.to_string(),
                                user,
                                fills,
                            }));
                        }
                    }
                }
            }
        }
        if let (Some(handle), Some(now_time)) = (&self.market_history, history_now_time_days) {
            if !history_sections.is_empty() {
                handle.send_stream_batch(MarketHistoryStreamBatch {
                    base_time: decoded.base_time,
                    now_time,
                    sections: history_sections,
                    trade_rows: history_trade_rows,
                    mm_order_rows: history_mm_order_rows,
                });
            }
        }
    }

    fn collect_known_trades_events_like_delphi(
        &mut self,
        decoded: &DecodedTradesPacket<'_>,
        effects: Vec<TradesPacketEffect>,
        now_ms: i64,
        history_now_time_days: Option<f64>,
        out: &mut Vec<Event>,
    ) {
        let mut applied_sections = false;
        for effect in effects {
            if matches!(&effect, TradesPacketEffect::Apply) && !applied_sections {
                self.apply_known_trades_sections_like_delphi(
                    decoded,
                    Some(now_ms),
                    history_now_time_days,
                    out,
                );
                applied_sections = true;
            }
            out.push(Event::Trade(
                effect.into_event(decoded.packet_num, decoded.base_time),
            ));
        }
    }
}
