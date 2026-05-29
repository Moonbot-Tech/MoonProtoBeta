//! Deferred cleanup and worker-loop maintenance for order state.

use super::*;

impl Orders {
    pub(super) fn mark_pending_removal(&mut self, uid: u64, now_ms: i64, delay_ms: i64) {
        let due_ms = now_ms.saturating_add(delay_ms.max(0));
        if let Some(existing) = self.pending_removals.iter_mut().find(|p| p.uid == uid) {
            existing.due_ms = existing.due_ms.max(due_ms);
        } else {
            self.pending_removals.push(PendingRemoval { uid, due_ms });
        }
    }

    /// Remove orders whose worker would leave `WCache` after the current
    /// `ProcessCommandOrder`/worker-loop batch, and return removed UID's.
    ///
    /// Delphi does not remove the worker from `WCache` inside
    /// `TMoonProtoNetClient.ProcessCommandOrder` when a terminal status or
    /// `TOrderNotFound` arrives. It marks/queues the worker command, and
    /// `BOrderWorker.DoTheJobVirtual` removes it later. This deferred drain is
    /// the Rust active-library counterpart: callers should run it after a
    /// reader-decoded batch so visual commands that arrived immediately after
    /// the terminal packet can still target the same order.
    pub fn drain_pending_removals(&mut self) -> Vec<u64> {
        let pending = std::mem::take(&mut self.pending_removals);
        let mut removed = Vec::with_capacity(pending.len());
        for pending in pending {
            if self.remove_order(pending.uid).is_some() {
                removed.push(pending.uid);
            }
        }
        removed
    }

    pub(crate) fn drain_pending_removals_due(&mut self, now_ms: i64) -> Vec<u64> {
        let pending = std::mem::take(&mut self.pending_removals);
        let mut keep = Vec::new();
        let mut removed = Vec::new();
        for pending in pending {
            if now_ms >= pending.due_ms {
                if self.remove_order(pending.uid).is_some() {
                    removed.push(pending.uid);
                }
            } else {
                keep.push(pending);
            }
        }
        self.pending_removals = keep;
        removed
    }

    /// Delphi `BOrderWorker.DoTheJobVirtual.CheckReplaceFlag` clears a pending
    /// replace flag when no replace response arrived for 5000 ms.
    pub(crate) fn tick_bulk_replace_timeouts(&mut self, now_ms: i64) -> Vec<OrderEvent> {
        let mut events = Vec::new();
        for entry in self.map.values_mut() {
            let entry = std::sync::Arc::make_mut(entry);
            let Some(current_replace_flag) = (match entry.status {
                OrderWorkerStatus::BuySet => Some(&mut entry.bulk_replace_buy),
                OrderWorkerStatus::SellSet => Some(&mut entry.bulk_replace_sell),
                _ => None,
            }) else {
                continue;
            };

            if entry.replace_sent_time_ms > 0 && !*current_replace_flag {
                entry.replace_sent_time_ms = 0;
                continue;
            }

            if *current_replace_flag
                && entry.replace_sent_time_ms > 0
                && (now_ms - entry.replace_sent_time_ms).abs() > BULK_REPLACE_TIMEOUT_MS
            {
                *current_replace_flag = false;
                entry.replace_sent_time_ms = 0;
                events.push(OrderEvent::Updated(entry.uid));
            }
        }
        events
    }

    /// After `TAllStatuses`, find orders that were absent from the fresh
    /// snapshot.
    ///
    /// These UID's must be explicitly requested through
    /// `build_order_status_request`. Matches
    /// `MoonProtoClient.pas:637-666 CleanupMissingWorkers`.
    ///
    /// Delphi checks `not Worker.JobIsDone`, but MoonProto virtual workers set
    /// `JobIsDone` only after `DoTheJobVirtual` returns. While Rust keeps a
    /// terminal entry for deferred removal, it still mirrors a worker that is
    /// physically present in `WCache`, so it remains a cleanup candidate.
    pub fn missing_after_snapshot(&self) -> Vec<u64> {
        let flag = self.current_snapshot_flag;
        self.map
            .values()
            .filter(|o| o.snapshot_flag != flag)
            .map(|o| o.uid)
            .collect()
    }

    /// Set `ServerTimeDelta`; called when Ping updates
    /// `server_time_delta = initial_time - now`.
    pub fn set_server_time_delta(&mut self, delta: f64) {
        self.server_time_delta = delta;
    }

    /// Remove one order by UID.
    pub fn remove(&mut self, uid: u64) -> Option<Order> {
        self.pending_local_visual_orders.remove(&uid);
        self.remove_order(uid)
    }

    /// Clear all order state on reconnect / `WantNewHello`.
    pub fn clear(&mut self) {
        self.map.clear();
        self.pending_local_visual_orders.clear();
        self.pending_removals.clear();
        self.current_snapshot_flag = 0;
    }
}
