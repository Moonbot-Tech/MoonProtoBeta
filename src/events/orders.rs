//! Active `MPC_Order` dispatch.
//!
//! Keeps the Delphi `ProcessCommandOrder` / `CleanupMissingWorkers` block
//! together so order worker creation, snapshot apply, and follow-up requests are
//! audited in one place.

use super::{ActiveAction, Event, EventDispatcher};
use crate::commands::trade::{AllStatuses, TradeCommand};
use crate::protocol::Command;
use crate::state::{ApplyResult, OrderEvent};

impl EventDispatcher {
    pub(super) fn client_new_data_order(
        &mut self,
        payload: &[u8],
        now_ms: i64,
        out: &mut Vec<Event>,
    ) {
        match TradeCommand::parse(payload) {
            Some(TradeCommand::AllStatuses(snap)) => self.process_all_statuses(snap, now_ms, out),
            Some(tc) => self.process_command_order(tc, now_ms, out),
            None => out.push(Self::parse_failed(Command::Order, payload)),
        }
    }

    /// Delphi equivalent: `ClientNewData(MPC_Order)` / `TAllStatuses` branch.
    fn process_all_statuses(&mut self, snap: AllStatuses, now_ms: i64, out: &mut Vec<Event>) {
        self.orders.begin_snapshot();
        for status in snap.orders {
            self.process_command_order(TradeCommand::OrderStatus(Box::new(status)), now_ms, out);
        }
        out.push(Event::Order(OrderEvent::Snapshot));
    }

    /// Delphi equivalent: `TMoonProtoNetClient.ProcessCommandOrder`.
    pub(super) fn process_command_order(
        &mut self,
        tc: TradeCommand,
        now_ms: i64,
        out: &mut Vec<Event>,
    ) {
        if self.drop_new_order_status_without_worker(&tc) {
            return;
        }

        // audit_responsibility A5 / active library: автоматически подхватываем
        // server_time_delta. При наличии per-Client `server_time_delta_source`
        // (multi-Client) — читаем оттуда. Иначе fallback на global для raw
        // dispatch без Client source. Без этого Orders::apply применяет AdjustTime со старым
        // delta=0 — order timestamps сдвинуты на 0.5-2 сек (silent bug).
        // Multi-client safe ServerTimeDelta source is linked by the active path.
        self.orders
            .set_server_time_delta(self.current_server_time_delta());
        let (apply_result, ev) = self.orders.apply_at(tc, now_ms);
        if apply_result == ApplyResult::Applied {
            out.push(Event::Order(ev));
        }
    }

    pub fn tick_orders(&mut self, now_ms: i64) -> Vec<Event> {
        let mut out = Vec::new();
        self.tick_orders_into(now_ms, &mut out);
        out
    }

    pub(crate) fn tick_orders_into(&mut self, now_ms: i64, out: &mut Vec<Event>) {
        for ev in self.orders.tick_bulk_replace_timeouts(now_ms) {
            out.push(Event::Order(ev));
        }
        self.drain_deferred_order_removals_due(now_ms, out);
    }

    pub(crate) fn tick_orders_active_actions(
        &mut self,
        now_ms: i64,
        out: &mut Vec<Event>,
        actions: &mut Vec<ActiveAction>,
    ) {
        self.tick_orders_into(now_ms, out);
        for request in self.orders.tick_pending_cancel_resends(now_ms) {
            actions.push(ActiveAction::OrderCancel { request });
        }
    }

    /// Delphi `ProcessCommandOrder` first tries `WCache.TryFind(TaskUID)`.
    /// Only an unknown, non-cache `TOrderStatus` may create a worker, and only
    /// when `Cmd.m <> nil` (the market name resolved in local `Markets`).
    fn drop_new_order_status_without_worker(&self, tc: &TradeCommand) -> bool {
        let TradeCommand::OrderStatus(st) = tc else {
            return false;
        };

        let uid = st.epoch_header.market.base.uid;
        if self.orders.get(uid).is_some() {
            return false;
        }

        if st.from_cache {
            return true;
        }

        let market_name = &st.epoch_header.market.market_name;
        if self.markets.get(market_name).is_none() {
            log::warn!(
                target: "moonproto::orders",
                "Drop order <{}>: market not found locally ({})",
                uid,
                market_name
            );
            return true;
        }

        false
    }

    /// Delphi equivalent: `TMoonProtoNetClient.CleanupMissingWorkers`.
    pub(super) fn cleanup_missing_workers(&self, actions: &mut Vec<ActiveAction>) {
        for request in self.missing_order_status_requests_after_snapshot() {
            actions.push(ActiveAction::RequestOrderStatus {
                ctx: request.ctx,
                market_name: request.market_name,
            });
        }
    }
}
