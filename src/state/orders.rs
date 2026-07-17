//! Orders sync state — applies inbound `TBaseTradeCommand` values to the local
//! active order read-model.
//!
//! ## Module Role
//!
//! This is the client-side mirror of server order workers: inbound commands are
//! applied to retained order state and produce typed events. Active Lib keeps
//! the retained order cache and cleanup/status-recovery behavior internally.
//!
//! Supported behavior:
//! - One server-worker epoch watermark with full-status baselining.
//! - Full-status-only phase transitions and phase-gated delta updates.
//! - Independent replace-state and replace-ack epoch judges.
//! - Snapshot flag mechanism (`current_snapshot_flag` is incremented on
//!   `TAllStatuses`; orders without the fresh flag are returned by
//!   `missing_after_snapshot()`).
//! - BulkReplace tracking.
//! - Trace line chart state.
//! - Corridor state.
//! - VStop state.
//! - Deferred removal on terminal statuses / `TOrderNotFound`.
//! - `ServerTimeDelta` correction for all `TDateTime` fields.

use crate::commands::trade::*;
use crate::state::eps::EpsProfile;
use std::collections::HashMap;
use std::sync::Arc;

mod accessors;
mod actions;
mod apply_helpers;
mod maintenance;
mod model;
mod types;

use self::apply_helpers::ServerEpochCommandKind;

pub use self::model::Order;
#[cfg(any(test, feature = "diagnostics"))]
#[doc(hidden)]
pub use self::types::ApplyResult;
#[cfg(not(any(test, feature = "diagnostics")))]
pub(crate) use self::types::ApplyResult;
pub use self::types::{
    MarketPositionProtection, OrderEvent, OrderTraceChartPoint, OrderTraceLine,
    PositionProtectionSide, SellReason,
};
pub(crate) use self::types::{OrderCancelSend, PanicSellSend};

const BULK_REPLACE_TIMEOUT_MS: i64 = 5000;
const SELL_DONE_REMOVAL_GRACE_MS: i64 = 400;
const PENDING_CANCEL_REPEAT_MS: i64 = 32;
const ORDER_TRACE_LINE_SHRINK_TO: usize = 800;
const ORDER_TRACE_LINE_SHRINK_INTERVAL_MS: i64 = 30_000;
const ORDER_TOMBSTONE_COUNT: usize = 128;

use super::epoch::epoch_is_ok;

fn order_type_uses_buy_side(order_type: OrderType) -> bool {
    order_type == OrderType::Buy
}

fn terminal_removal_delay_ms(status: OrderWorkerStatus) -> i64 {
    if status == OrderWorkerStatus::SellDone {
        SELL_DONE_REMOVAL_GRACE_MS
    } else {
        0
    }
}

fn command_marks_existing_worker_snapshot_flag(cmd: &TradeCommand) -> bool {
    matches!(
        cmd,
        TradeCommand::OrderStatus(_)
            | TradeCommand::OrderStatusUpdate(_)
            | TradeCommand::OrderReplace(_)
            | TradeCommand::OrderReplaceResponse(_)
            | TradeCommand::OrderCancel(_)
            | TradeCommand::JoinOrders(_)
            | TradeCommand::SplitOrder(_)
            | TradeCommand::MoveAllSells(_)
            | TradeCommand::DoClosePosition(_)
            | TradeCommand::DoLimitClosePosition(_)
            | TradeCommand::DoSplitPosition(_)
            | TradeCommand::DoSellOrder(_)
            | TradeCommand::OrderStatusRequest(_)
            | TradeCommand::OrderNotFound(_)
            | TradeCommand::OrderStopsUpdate(_)
            | TradeCommand::TurnPanicSell(_)
            | TradeCommand::Penalty(_)
            | TradeCommand::TradeVisual(_)
            | TradeCommand::OrderTracePoint(_)
            | TradeCommand::CorridorUpdate(_)
            | TradeCommand::MoveAllBuys(_)
            | TradeCommand::VStopUpdate(_)
            | TradeCommand::DoMarketSplitPosition(_)
            | TradeCommand::BaseMarket(_)
            | TradeCommand::TradeEpoch(_)
            | TradeCommand::NewOrder(_)
    )
}

#[derive(Debug, Clone, Copy)]
struct PendingRemoval {
    uid: u64,
    due_ms: i64,
    instance_id: u64,
    tombstone: bool,
    server_session_token: u64,
}

/// Main retained orders collection.
///
/// Single-owner state: modified only by the client runtime owner. Consumer code
/// reads it through `iter()` / `get()` or through `MoonClient` snapshots.
#[derive(Debug, Clone)]
pub struct Orders {
    map: HashMap<u64, Arc<Order>>,
    /// UID's already marked as finishing, but not removed from retained order
    /// state yet.
    pending_removals: Vec<PendingRemoval>,
    /// Recently completed UID's in the current hard session. A delayed sliced
    /// snapshot captured before the terminal full must not recreate them.
    tombstones: [u64; ORDER_TOMBSTONE_COUNT],
    tombstone_index: usize,
    tombstone_session_token: u64,
    next_instance_id: u64,
    /// Incremented on every `TAllStatuses`.
    current_snapshot_flag: u8,
    /// `ServerTimeDelta = InitialTime(server) - Now(client)`, applied to
    /// command `TDateTime` fields.
    pub server_time_delta: f64,
    eps_profile: EpsProfile,
    /// Last periodic trace-line shrink pass.
    last_order_line_shrink_ms: i64,
}

impl Default for Orders {
    fn default() -> Self {
        Self::new()
    }
}

impl Orders {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            pending_removals: Vec::new(),
            tombstones: [0; ORDER_TOMBSTONE_COUNT],
            tombstone_index: 0,
            tombstone_session_token: 0,
            next_instance_id: 0,
            current_snapshot_flag: 0,
            server_time_delta: 0.0,
            eps_profile: EpsProfile::default(),
            last_order_line_shrink_ms: 0,
        }
    }

    pub(crate) fn set_eps_profile(&mut self, eps_profile: EpsProfile) {
        self.eps_profile = eps_profile;
    }

    fn order_mut(&mut self, uid: u64) -> Option<&mut Order> {
        self.map.get_mut(&uid).map(Arc::make_mut)
    }

    fn remove_order_arc(&mut self, uid: u64) -> Option<Arc<Order>> {
        self.map.remove(&uid)
    }

    fn remove_order(&mut self, uid: u64) -> Option<Order> {
        self.remove_order_arc(uid)
            .map(|order| Arc::try_unwrap(order).unwrap_or_else(|order| (*order).clone()))
    }

    fn order_arc(&self, uid: u64) -> Option<Arc<Order>> {
        self.map.get(&uid).cloned()
    }

    fn allocate_instance_id(&mut self) -> u64 {
        self.next_instance_id = self.next_instance_id.wrapping_add(1);
        if self.next_instance_id == 0 {
            self.next_instance_id = 1;
        }
        self.next_instance_id
    }

    fn sync_tombstone_session(&mut self, server_token: u64) {
        if server_token != 0 && server_token != self.tombstone_session_token {
            self.tombstones.fill(0);
            self.tombstone_index = 0;
            self.tombstone_session_token = server_token;
        }
    }

    fn is_tombstoned(&self, uid: u64) -> bool {
        uid != 0 && self.tombstones.contains(&uid)
    }

    fn record_tombstone(&mut self, uid: u64, server_session_token: u64) {
        if uid == 0 || server_session_token != self.tombstone_session_token {
            return;
        }
        self.tombstones[self.tombstone_index] = uid;
        self.tombstone_index = (self.tombstone_index + 1) & (ORDER_TOMBSTONE_COUNT - 1);
    }

    fn created_event(&self, uid: u64) -> Option<OrderEvent> {
        self.order_arc(uid).map(OrderEvent::Created)
    }

    fn updated_event(&self, uid: u64) -> Option<OrderEvent> {
        self.order_arc(uid).map(OrderEvent::Updated)
    }

    /// Advance snapshot flag before a `TAllStatuses` item loop.
    pub(crate) fn begin_snapshot(&mut self) -> u8 {
        self.current_snapshot_flag = self.current_snapshot_flag.wrapping_add(1);
        self.current_snapshot_flag
    }

    /// Apply one inbound `MPC_Order` command and return the resulting event.
    ///
    /// This is the main state-transition entry point:
    /// 1. hard-session epoch reset and shared watermark check;
    /// 2. full-status-only phase transition gate;
    /// 3. update or create `Order`;
    /// 4. `ServerTimeDelta` correction for `TDateTime` fields;
    /// 5. deferred removal on terminal status / `TOrderNotFound`;
    /// 6. snapshot flag mechanics (CleanupMissing) through dispatcher-level
    ///    `TAllStatuses` handling.
    /// 7. event generation.
    ///
    /// Diagnostics/tests keep the ignored reason visible. Normal terminal code
    /// reads order state and user-facing events only; stale/outgoing packets are
    /// internal state-machine facts, not UI events.
    #[cfg(any(test, feature = "diagnostics"))]
    pub fn apply(&mut self, cmd: TradeCommand) -> (ApplyResult, OrderEvent) {
        let uid = cmd.uid();
        let (result, event) = self.apply_at(cmd, 0);
        (
            result,
            event.unwrap_or(OrderEvent::Ignored {
                uid,
                reason: result,
            }),
        )
    }

    #[cfg(any(test, feature = "diagnostics"))]
    pub(crate) fn apply_at(
        &mut self,
        cmd: TradeCommand,
        now_ms: i64,
    ) -> (ApplyResult, Option<OrderEvent>) {
        self.apply_at_with_server_token(cmd, now_ms, 0)
    }

    pub(crate) fn apply_at_with_server_token(
        &mut self,
        cmd: TradeCommand,
        now_ms: i64,
        server_token: u64,
    ) -> (ApplyResult, Option<OrderEvent>) {
        self.sync_tombstone_session(server_token);
        let uid = cmd.uid();
        let current_snapshot_flag = self.current_snapshot_flag;
        let server_time_delta = self.server_time_delta;
        if command_marks_existing_worker_snapshot_flag(&cmd) {
            if let Some(entry) = self.order_mut(uid) {
                entry.snapshot_flag = current_snapshot_flag;
            }
        }
        match cmd {
            // --- Full status create/update ---
            TradeCommand::OrderStatus(st) => {
                let new_order = !self.map.contains_key(&uid);
                let status = st.epoch_header.status;
                if new_order && st.from_cache {
                    return ignored_order_event(uid, ApplyResult::OrderNotFound);
                }
                if new_order && self.is_tombstoned(uid) {
                    return ignored_order_event(uid, ApplyResult::OutOfOrder);
                }
                let mut lifecycle_rejected = None;
                let mut settings_applied = false;
                let is_done = {
                    if new_order {
                        let instance_id = self.allocate_instance_id();
                        self.map
                            .insert(uid, Arc::new(Order::from_status(&st, instance_id)));
                    }
                    let entry = self.order_mut(uid).expect("order inserted or existed");

                    match Self::accept_server_epoch(
                        entry,
                        &st.epoch_header,
                        ServerEpochCommandKind::FullStatus,
                        server_token,
                    ) {
                        Ok(_) => {
                            Self::apply_status_inner(entry, &st, server_time_delta, new_order);
                            Self::apply_full_stops_vstop(entry, &st);
                            entry.replace_epoch_buy = st.epoch_header.epoch;
                            entry.replace_epoch_sell = st.epoch_header.epoch;
                            entry.snapshot_flag = current_snapshot_flag;
                        }
                        Err(reason) => {
                            // The full's lifecycle payload is stale, but its
                            // independently judged settings may still repair a
                            // lost stops/VStop echo.
                            settings_applied = Self::apply_full_stops_vstop(entry, &st);
                            lifecycle_rejected = Some(reason);
                        }
                    }
                    entry.job_is_done
                };
                if let Some(reason) = lifecycle_rejected {
                    if settings_applied {
                        return (ApplyResult::Applied, self.updated_event(uid));
                    }
                    return ignored_order_event(uid, reason);
                }
                if is_done {
                    self.mark_pending_removal(uid, now_ms, terminal_removal_delay_ms(status), true);
                } else {
                    self.cancel_terminal_removal(uid);
                }

                if new_order {
                    (ApplyResult::Applied, self.created_event(uid))
                } else {
                    (ApplyResult::Applied, self.updated_event(uid))
                }
            }

            // --- Delta-update ---
            TradeCommand::OrderStatusUpdate(up) => {
                {
                    let Some(entry) = self.order_mut(uid) else {
                        return ignored_order_event(uid, ApplyResult::OrderNotFound);
                    };

                    if let Err(reason) = Self::accept_server_epoch(
                        entry,
                        &up.epoch_header,
                        ServerEpochCommandKind::Other,
                        server_token,
                    ) {
                        return ignored_order_event(uid, reason);
                    }

                    if matches!(
                        up.epoch_header.status,
                        OrderWorkerStatus::BuySet | OrderWorkerStatus::SellSet
                    ) {
                        let target = if up.epoch_header.status == OrderWorkerStatus::SellSet {
                            &mut entry.sell_order
                        } else {
                            &mut entry.buy_order
                        };
                        Self::apply_update_data(target, up.update_data, server_time_delta);
                    }

                    if up.epoch_header.status == OrderWorkerStatus::None {
                        // Delphi updates vOrder.BuyCondPrice only in the
                        // pending-worker branch: `(Status = OS_None) and
                        // IsPending and (vOrder <> nil)`.
                        if entry.pending_buy_cond_price.is_some() {
                            entry.pending_buy_cond_price = Some(up.update_data.mean_price);
                        }
                    }
                    let sell_reason = SellReason::from_byte(up.sell_reason_code);
                    if up.sell_reason_code != 0 && sell_reason != entry.sell_reason {
                        entry.sell_reason = sell_reason;
                    }
                }

                (ApplyResult::Applied, self.updated_event(uid))
            }

            // --- Replace response ---
            TradeCommand::OrderReplaceResponse(rr) => {
                let rr = *rr;
                let Some(entry) = self.order_mut(uid) else {
                    return ignored_order_event(uid, ApplyResult::OrderNotFound);
                };

                let fresh_epoch = match Self::accept_server_epoch(
                    entry,
                    &rr.epoch_header,
                    ServerEpochCommandKind::ReplaceResponse,
                    server_token,
                ) {
                    Ok(fresh) => fresh,
                    Err(reason) => return ignored_order_event(uid, reason),
                };

                if order_type_uses_buy_side(rr.order_type) {
                    if fresh_epoch {
                        Self::apply_update_data(
                            &mut entry.buy_order,
                            rr.update_data,
                            server_time_delta,
                        );
                    }
                    if epoch_is_ok(entry.replace_epoch_buy, rr.epoch_header.epoch) {
                        entry.replace_epoch_buy = rr.epoch_header.epoch;
                        if rr.quantity_base > 0.0 {
                            entry.buy_order.quantity_base = rr.quantity_base;
                        }
                    }
                    if !entry.ack_seeded_buy
                        || epoch_is_ok(entry.ack_epoch_buy, rr.epoch_header.epoch)
                    {
                        entry.ack_seeded_buy = true;
                        entry.ack_epoch_buy = rr.epoch_header.epoch;
                        entry.bulk_replace_buy = false;
                    }
                } else {
                    if fresh_epoch {
                        Self::apply_update_data(
                            &mut entry.sell_order,
                            rr.update_data,
                            server_time_delta,
                        );
                    }
                    if epoch_is_ok(entry.replace_epoch_sell, rr.epoch_header.epoch) {
                        entry.replace_epoch_sell = rr.epoch_header.epoch;
                        if rr.quantity_base > 0.0 {
                            entry.sell_order.quantity_base = rr.quantity_base;
                        }
                    }
                    if !entry.ack_seeded_sell
                        || epoch_is_ok(entry.ack_epoch_sell, rr.epoch_header.epoch)
                    {
                        entry.ack_seeded_sell = true;
                        entry.ack_epoch_sell = rr.epoch_header.epoch;
                        entry.bulk_replace_sell = false;
                    }
                }

                (ApplyResult::Applied, self.updated_event(uid))
            }

            // --- Stops update ---
            TradeCommand::OrderStopsUpdate(su) => {
                let Some(entry) = self.order_mut(uid) else {
                    return ignored_order_event(uid, ApplyResult::OrderNotFound);
                };
                Self::adopt_server_session(entry, server_token);
                if entry.stops_seeded && !epoch_is_ok(entry.stops_epoch, su.epoch_header.epoch) {
                    return ignored_order_event(uid, ApplyResult::OutOfOrder);
                }
                entry.stops_seeded = true;
                entry.stops_epoch = su.epoch_header.epoch;
                entry.stops = su.stops;
                (ApplyResult::Applied, Some(OrderEvent::StopsChanged(uid)))
            }

            // --- VStop update ---
            TradeCommand::VStopUpdate(vs) => {
                let Some(entry) = self.order_mut(uid) else {
                    return ignored_order_event(uid, ApplyResult::OrderNotFound);
                };
                Self::adopt_server_session(entry, server_token);
                if entry.vstop_seeded && !epoch_is_ok(entry.vstop_epoch, vs.epoch_header.epoch) {
                    return ignored_order_event(uid, ApplyResult::OutOfOrder);
                }
                entry.vstop_seeded = true;
                entry.vstop_epoch = vs.epoch_header.epoch;
                entry.vstop_on = vs.vstop_on;
                entry.vstop_fixed = vs.vstop_fixed;
                entry.vstop_level = vs.vstop_level;
                entry.vstop_vol = vs.vstop_vol;
                (ApplyResult::Applied, Some(OrderEvent::VStopChanged(uid)))
            }

            // --- Corridor update ---
            TradeCommand::CorridorUpdate(cu) => {
                let Some(entry) = self.order_mut(uid) else {
                    return ignored_order_event(uid, ApplyResult::OrderNotFound);
                };
                entry.is_moon_shot = true;
                entry.corridor_price_down = cu.price_down;
                entry.corridor_price_up = cu.price_up;
                (ApplyResult::Applied, Some(OrderEvent::CorridorChanged(uid)))
            }

            // --- Trace point ---
            TradeCommand::OrderTracePoint(mut tp) => {
                let Some(entry) = self.order_mut(uid) else {
                    return ignored_order_event(uid, ApplyResult::OrderNotFound);
                };
                tp.adjust_time(server_time_delta);
                Self::apply_trace_line(entry, &tp);
                (ApplyResult::Applied, Some(OrderEvent::TracePoint { uid }))
            }

            // --- Bulk replace notification ---
            TradeCommand::BulkReplaceNotify(brn) => {
                let mut affected = Vec::new();
                for &uid_replaced in &brn.uids {
                    if let Some(entry) = self.order_mut(uid_replaced) {
                        if order_type_uses_buy_side(brn.order_type) {
                            entry.bulk_replace_buy = true;
                        } else {
                            entry.bulk_replace_sell = true;
                        }
                        entry.replace_sent_time_ms = now_ms.max(1);
                        affected.push(uid_replaced);
                    }
                }
                if affected.is_empty() {
                    return ignored_order_event(uid, ApplyResult::OrderNotFound);
                }
                (
                    ApplyResult::Applied,
                    Some(OrderEvent::BulkReplaced {
                        order_type: brn.order_type,
                        uids: affected,
                    }),
                )
            }

            // --- Order not found (server forced remove) ---
            TradeCommand::OrderNotFound(h) => {
                let uid = h.market.base.uid;
                let found = if let Some(entry) = self.order_mut(uid) {
                    entry.server_forced_remove = true;
                    entry.cancel_request = true;
                    true
                } else {
                    false
                };
                if found {
                    self.mark_pending_removal(uid, now_ms, 0, false);
                    (ApplyResult::Applied, self.updated_event(uid))
                } else {
                    ignored_order_event(uid, ApplyResult::OrderNotFound)
                }
            }

            // --- Dispatcher-level payloads, handled before ProcessCommandOrder ---
            TradeCommand::AllStatuses(_)
            | TradeCommand::ClosedSellOrderReport(_)
            | TradeCommand::ReportRowUpsert(_)
            | TradeCommand::ReportRowDelete(_)
            | TradeCommand::ReportSyncRequest(_)
            | TradeCommand::ReportSchemaRequest(_)
            | TradeCommand::ReportSchema(_)
            | TradeCommand::ReportSyncPage(_)
            | TradeCommand::ReportCheckRowsRequest(_) => {
                ignored_order_event(uid, ApplyResult::NotApplicable)
            }

            // --- Client-originated outgoing commands: ignored by state ---
            TradeCommand::OrderReplace(c) => {
                self.apply_noop_trade_epoch(uid, &c.epoch_header, server_token)
            }
            TradeCommand::OrderCancel(c) => {
                self.apply_noop_trade_epoch(uid, &c.epoch_header, server_token)
            }
            TradeCommand::OrderStatusRequest(h) => {
                self.apply_noop_trade_epoch(uid, &h, server_token)
            }
            TradeCommand::TurnPanicSell(c) => {
                self.apply_noop_trade_epoch(uid, &c.epoch_header, server_token)
            }
            TradeCommand::TradeEpoch(h) => self.apply_noop_trade_epoch(uid, &h, server_token),

            TradeCommand::AllStatusesRequest(_)
            | TradeCommand::JoinOrders(_)
            | TradeCommand::SplitOrder(_)
            | TradeCommand::MoveAllSells(_)
            | TradeCommand::MoveAllBuys(_)
            | TradeCommand::DoClosePosition(_)
            | TradeCommand::DoLimitClosePosition(_)
            | TradeCommand::DoSplitPosition(_)
            | TradeCommand::DoMarketSplitPosition(_)
            | TradeCommand::DoSellOrder(_)
            | TradeCommand::NewOrder(_)
            | TradeCommand::SetImmune(_) => ignored_order_event(uid, ApplyResult::NotApplicable),

            // --- Other commands ---
            TradeCommand::Penalty(_)
            | TradeCommand::TradeVisual(_)
            | TradeCommand::BaseMarket(_) => ignored_order_event(uid, ApplyResult::NotApplicable),

            TradeCommand::Unknown { uid, .. } => {
                ignored_order_event(uid, ApplyResult::NotApplicable)
            }
        }
    }

    fn apply_noop_trade_epoch(
        &mut self,
        uid: u64,
        header: &TradeEpochHeader,
        server_token: u64,
    ) -> (ApplyResult, Option<OrderEvent>) {
        let Some(entry) = self.order_mut(uid) else {
            return ignored_order_event(uid, ApplyResult::OrderNotFound);
        };

        if let Err(reason) =
            Self::accept_server_epoch(entry, header, ServerEpochCommandKind::Other, server_token)
        {
            return ignored_order_event(uid, reason);
        }

        ignored_order_event(uid, ApplyResult::NotApplicable)
    }
}

fn ignored_order_event(uid: u64, reason: ApplyResult) -> (ApplyResult, Option<OrderEvent>) {
    #[cfg(any(test, feature = "diagnostics"))]
    {
        (reason, Some(OrderEvent::Ignored { uid, reason }))
    }
    #[cfg(not(any(test, feature = "diagnostics")))]
    {
        let _ = uid;
        (reason, None)
    }
}

#[cfg(test)]
mod tests;
