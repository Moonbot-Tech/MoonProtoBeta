//! Low-level `ProcessCommandOrder` apply helpers.

use super::*;
use crate::commands::market::{BaseCurrency, ExchangeCode};
use crate::commands::trade::DelphiBool;

impl Orders {
    pub(super) fn accept_epoch_and_phase(
        entry: &mut Order,
        header: &TradeEpochHeader,
    ) -> Result<(), ApplyResult> {
        let phase_idx = header.status.to_byte() as usize;
        if phase_idx < entry.server_latest_epoch.len() {
            if !epoch_is_ok(entry.server_latest_epoch[phase_idx], header.epoch) {
                return Err(ApplyResult::OutOfOrder);
            }
            entry.server_latest_epoch[phase_idx] = header.epoch;
        }

        let new_phase = status_phase(header.status);
        let cur_phase = status_phase(entry.status);
        if new_phase > 0 && cur_phase > 0 && new_phase < cur_phase {
            return Err(ApplyResult::PhaseRollback);
        }

        Ok(())
    }

    pub(super) fn apply_status_inner(
        entry: &mut Order,
        st: &OrderStatus,
        server_time_delta: f64,
        new_order: bool,
        price_eps: f64,
    ) {
        let mut buy = st.buy_order;
        let mut sell = st.sell_order;
        buy.adjust_time(server_time_delta);
        sell.adjust_time(server_time_delta);

        let had_pending_vorder = entry.pending_buy_cond_price.is_some();
        let was_status_changed = st.epoch_header.status != entry.status;
        entry.status = st.epoch_header.status;
        if new_order {
            entry.market_name = st.epoch_header.market.market_name.clone();
            entry.currency = BaseCurrency::from_byte(st.epoch_header.market.currency);
            entry.platform = ExchangeCode::from_byte(st.epoch_header.market.platform);
            entry.strat_id = st.strat_id;
            entry.is_short = st.is_short;
            entry.db_id = st.db_id;
            entry.from_cache = st.from_cache;
            entry.emulator_mode = st.emulator_mode;
        }
        entry.buy_order = buy;
        entry.sell_order = sell;
        entry.stops = st.stops;
        entry.immune_for_clicks = st.immune_for_clicks;
        entry.job_is_done = st.epoch_header.status.is_terminal();
        if st.epoch_header.status == OrderWorkerStatus::None {
            if new_order {
                entry.pending_buy_cond_price = Some(entry.buy_order.mean_price);
            } else if !had_pending_vorder {
                entry.pending_buy_cond_price = None;
            }
        } else {
            entry.pending_buy_cond_price = None;
            entry.pending_cancel = false;
        }

        if was_status_changed {
            entry.buy_price = entry.buy_order.actual_price;
            entry.sell_price = entry.sell_order.actual_price;
            entry.last_buy_actual_price = entry.buy_order.actual_price;
            entry.last_sell_actual_price = entry.sell_order.actual_price;
        } else {
            if (entry.buy_order.actual_price - entry.last_buy_actual_price).abs() > price_eps {
                entry.buy_price = entry.buy_order.actual_price;
                entry.last_buy_actual_price = entry.buy_order.actual_price;
            }
            if (entry.sell_order.actual_price - entry.last_sell_actual_price).abs() > price_eps {
                entry.sell_price = entry.sell_order.actual_price;
                entry.last_sell_actual_price = entry.sell_order.actual_price;
            }
        }

        if st.epoch_header.status == OrderWorkerStatus::SellDone {
            Self::apply_sell_done_flags(entry);
        }
    }

    pub(super) fn apply_sell_done_flags(entry: &mut Order) {
        // Delphi `BOrderWorker.SetDoneFlags` branch for `Status = OS_SelLDone`.
        entry.sell_order.is_closed = DelphiBool::TRUE;
        entry.sell_order.is_opened = DelphiBool::FALSE;
        entry.bulk_replace_sell = false;

        entry.buy_order.is_opened = DelphiBool::FALSE;
        entry.bulk_replace_buy = false;
        if entry.buy_order.is_closed.is_false() {
            entry.buy_order.canceled = DelphiBool::TRUE;
        }
    }

    pub(super) fn apply_trace_line(entry: &mut Order, tp: &OrderTracePoint) {
        let is_buy_side = order_type_uses_buy_side(tp.ord_type);
        let order_id = if is_buy_side {
            entry.buy_order.int_id
        } else {
            entry.sell_order.int_id
        };
        let create_time = if is_buy_side {
            entry.buy_order.create_time
        } else {
            entry.sell_order.create_time
        };

        let line_slot = if is_buy_side {
            &mut entry.buy_trace_line
        } else {
            &mut entry.sell_trace_line
        };

        if tp.is_finish() {
            if let Some(line) = line_slot.as_mut() {
                line.set_point_trade(tp.trace_time, tp.trace_price, false, true);
            }
            return;
        }

        if line_slot
            .as_ref()
            .is_some_and(|line| line.order_type != tp.ord_type)
        {
            *line_slot = None;
        }

        if line_slot.is_none() && tp.is_initial() {
            let mut line = OrderTraceLine::new(tp.ord_type, order_id);
            line.set_point_trade(create_time, tp.base_price, false, false);
            *line_slot = Some(line);
        }

        if let Some(line) = line_slot.as_mut() {
            line.set_point_trade(tp.trace_time, tp.trace_price, tp.is_temp(), false);
            line.order_id = order_id;
        }

        if tp.stop_price > 0.0 {
            if let Some(line) = entry.sell_trace_line.as_mut() {
                line.stop_price = Some(tp.stop_price);
            }
        }
    }
}
