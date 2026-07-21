//! Deferred terminal cleanup and inexpensive projection maintenance.

use super::*;

impl OrderState {
    pub(super) fn mark_pending_removal(&mut self, uid: u64, now_ms: i64, delay_ms: i64) {
        let Some(order) = self.read.map.get(&uid) else {
            return;
        };
        let due_ms = now_ms.saturating_add(delay_ms.max(0));
        if let Some(existing) = self
            .pending_removals
            .iter_mut()
            .find(|pending| pending.uid == uid && pending.instance_id == order.instance_id)
        {
            existing.due_ms = existing.due_ms.max(due_ms);
        } else {
            self.pending_removals.push(PendingRemoval {
                uid,
                due_ms,
                instance_id: order.instance_id,
            });
        }
        self.next_pending_removal_ms = Some(
            self.next_pending_removal_ms
                .map_or(due_ms, |current| current.min(due_ms)),
        );
    }

    pub(crate) fn drain_pending_removals_due(&mut self, now_ms: i64) -> Vec<Arc<Order>> {
        if self
            .next_pending_removal_ms
            .is_none_or(|due_ms| now_ms < due_ms)
        {
            return Vec::new();
        }
        let pending = std::mem::take(&mut self.pending_removals);
        let mut keep = Vec::new();
        let mut removed = Vec::new();
        for item in pending {
            if now_ms < item.due_ms {
                keep.push(item);
                continue;
            }
            let owned_terminal = self.read.map.get(&item.uid).is_some_and(|order| {
                order.instance_id == item.instance_id && order.status.is_terminal()
            });
            if !owned_terminal {
                continue;
            }
            self.mirrors.remove(&item.uid);
            self.record_tombstone(item.uid);
            if let Some(order) = self.remove_order_arc(item.uid) {
                removed.push(order);
            }
        }
        self.pending_removals = keep;
        self.next_pending_removal_ms = self.pending_removals.iter().map(|item| item.due_ms).min();
        removed
    }

    pub(crate) fn tick_bulk_replace_timeouts(&mut self, now_ms: i64) -> Vec<OrderEvent> {
        if self
            .next_replace_timeout_ms
            .is_none_or(|due_ms| now_ms < due_ms)
        {
            return Vec::new();
        }
        let uids: Vec<u64> = self
            .read
            .map
            .values()
            .filter(|order| {
                order.replace_sent_time_ms > 0
                    && (now_ms - order.replace_sent_time_ms).abs() > TARGET_CONFIRM_TIMEOUT_MS
            })
            .map(|order| order.uid)
            .collect();
        let mut events = Vec::with_capacity(uids.len());
        for uid in uids {
            if let Some(order) = self.order_mut(uid) {
                order.replace_sent_time_ms = 0;
                order.bulk_replace_buy = false;
                order.bulk_replace_sell = false;
            }
            if let Some(order) = self.order_arc(uid) {
                events.push(OrderEvent::Updated(order));
            }
        }
        self.next_replace_timeout_ms = self
            .read
            .map
            .values()
            .filter_map(|order| {
                (order.replace_sent_time_ms > 0).then(|| {
                    order
                        .replace_sent_time_ms
                        .saturating_add(TARGET_CONFIRM_TIMEOUT_MS)
                        .saturating_add(1)
                })
            })
            .min();
        events
    }

    pub(crate) fn tick_order_trace_line_shrink(&mut self, now_ms: i64) -> Vec<OrderEvent> {
        if now_ms < self.next_order_line_shrink_ms {
            return Vec::new();
        }
        self.next_order_line_shrink_ms = now_ms.saturating_add(ORDER_TRACE_LINE_SHRINK_INTERVAL_MS);
        let mut events = Vec::new();
        let uids: Vec<u64> = self
            .read
            .map
            .values()
            .filter(|entry| {
                entry
                    .buy_trace_line
                    .as_ref()
                    .is_some_and(|line| line.needs_shrink(ORDER_TRACE_LINE_SHRINK_TO))
                    || entry
                        .sell_trace_line
                        .as_ref()
                        .is_some_and(|line| line.needs_shrink(ORDER_TRACE_LINE_SHRINK_TO))
            })
            .map(|entry| entry.uid)
            .collect();
        for uid in uids {
            let Some(entry) = self.order_mut(uid) else {
                continue;
            };
            let needs_shrink = entry
                .buy_trace_line
                .as_ref()
                .is_some_and(|line| line.needs_shrink(ORDER_TRACE_LINE_SHRINK_TO))
                || entry
                    .sell_trace_line
                    .as_ref()
                    .is_some_and(|line| line.needs_shrink(ORDER_TRACE_LINE_SHRINK_TO));
            if !needs_shrink {
                continue;
            }
            let mut shrunk = false;
            if let Some(line) = entry.buy_trace_line.as_mut() {
                shrunk |= line.shrink_points(ORDER_TRACE_LINE_SHRINK_TO) > 0;
            }
            if let Some(line) = entry.sell_trace_line.as_mut() {
                shrunk |= line.shrink_points(ORDER_TRACE_LINE_SHRINK_TO) > 0;
            }
            if shrunk {
                events.push(OrderEvent::TracePoint { uid: entry.uid });
            }
        }
        events
    }

    pub(crate) fn has_due_tick_work(&self, now_ms: i64) -> bool {
        self.next_pending_removal_ms
            .is_some_and(|due_ms| now_ms >= due_ms)
            || self
                .next_replace_timeout_ms
                .is_some_and(|due_ms| now_ms >= due_ms)
            || now_ms >= self.next_order_line_shrink_ms
    }
}
