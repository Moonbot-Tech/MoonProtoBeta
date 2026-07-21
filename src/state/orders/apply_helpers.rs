//! Chart-only order stream helpers retained outside the canonical state.

use super::*;

impl OrderState {
    pub(super) fn apply_trace_line(entry: &mut Order, tp: &OrderTracePoint) {
        let buy_side = order_type_uses_buy_side(tp.ord_type);
        let order_id = if buy_side {
            entry.buy_order.int_id
        } else {
            entry.sell_order.int_id
        };
        let create_time = if buy_side {
            entry.buy_order.create_time
        } else {
            entry.sell_order.create_time
        };
        let line_slot = if buy_side {
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
                line.stop_time = Some(tp.trace_time());
            }
        }
    }
}
