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
//! - Epoch protection (per-status `server_latest_epoch`).
//! - Phase rollback protection.
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

/// Wrapping-safe epoch comparison.
/// Matches MoonProtoFunc.pas:188-203 `EpochIsOK`:
///   if LastEpoch = NewEpoch then Result := false;   // duplicate
///   backDist := LastEpoch - NewEpoch;               // Word wrapping subtraction
///   if backDist <= 100 then Result := false         // stale, up to 100 behind
///   else Result := true;                            // ACCEPT
///
/// Returns `true` when `new` is actually new: not duplicate and not stale.
/// Used by `AcceptServerCommand` in `BOrderWorker` (TaskWorkers.pas:1440).
// `epoch_is_ok` is shared through `state::epoch::epoch_is_ok`
// (audit_rust_quality #1). The stale window is 100, from Delphi
// `MoonProtoFunc.pas:188-203`.
use super::epoch::epoch_is_ok;

/// Mapping from worker status to phase number.
/// Matches TaskWorkers.pas:546-555 `StatusPhase`:
///   OS_BuySet              → 1
///   OS_BuyDone             → 2
///   OS_SellSet             → 3
///   OS_SelLAlmostDone / OS_SelLDone (`SellAlmostDone` / `SellDone`) → 4
///   all other statuses (None, BuyFail, BuyCancel, SellFail, SellCancel) -> 0
///
/// Phase rollback is checked only when both `new_phase > 0` and
/// `cur_phase > 0`; terminal phase-0 statuses are not checked.
fn status_phase(s: OrderWorkerStatus) -> u8 {
    match s {
        OrderWorkerStatus::BuySet => 1,
        OrderWorkerStatus::BuyDone => 2,
        OrderWorkerStatus::SellSet => 3,
        OrderWorkerStatus::SellAlmostDone | OrderWorkerStatus::SellDone => 4,
        _ => 0,
    }
}

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
}

/// Main retained orders collection.
///
/// Single-owner state: modified only by the client runtime owner. Consumer code
/// reads it through `iter()` / `get()` or through `MoonClient` snapshots.
#[derive(Debug, Clone, Default)]
pub struct Orders {
    map: HashMap<u64, Arc<Order>>,
    /// UID's already marked as finishing, but not removed from retained order
    /// state yet.
    pending_removals: Vec<PendingRemoval>,
    /// Incremented on every `TAllStatuses`.
    current_snapshot_flag: u8,
    /// `ServerTimeDelta = InitialTime(server) - Now(client)`, applied to
    /// command `TDateTime` fields.
    pub server_time_delta: f64,
    eps_profile: EpsProfile,
    /// Last periodic trace-line shrink pass.
    last_order_line_shrink_ms: i64,
}

impl Orders {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            pending_removals: Vec::new(),
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
    /// 1. epoch check;
    /// 2. phase rollback check;
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

    pub(crate) fn apply_at(
        &mut self,
        cmd: TradeCommand,
        now_ms: i64,
    ) -> (ApplyResult, Option<OrderEvent>) {
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
                let eps_m = self.eps_profile.eps_m;
                let is_done = {
                    if new_order {
                        self.map.insert(uid, Arc::new(Order::from_status(&st)));
                    }
                    let entry = self.order_mut(uid).expect("order inserted or existed");

                    // Delphi new-order path goes ProcessCommandOrder ->
                    // OnMServerOrder -> HandleServerCommand(Cmd), bypassing
                    // AcceptServerCommand and therefore not touching
                    // FServerLatestEpoch for the first full status.
                    if !new_order {
                        if let Err(reason) = Self::accept_epoch_and_phase(entry, &st.epoch_header) {
                            return ignored_order_event(uid, reason);
                        }
                    }

                    Self::apply_status_inner(entry, &st, server_time_delta, new_order, eps_m);
                    entry.snapshot_flag = current_snapshot_flag;
                    entry.job_is_done
                };
                if is_done {
                    self.mark_pending_removal(uid, now_ms, terminal_removal_delay_ms(status));
                }

                if new_order {
                    (ApplyResult::Applied, self.created_event(uid))
                } else {
                    (ApplyResult::Applied, self.updated_event(uid))
                }
            }

            // --- Delta-update ---
            TradeCommand::OrderStatusUpdate(up) => {
                let status = up.epoch_header.status;
                let is_terminal = status.is_terminal();
                {
                    let Some(entry) = self.order_mut(uid) else {
                        return ignored_order_event(uid, ApplyResult::OrderNotFound);
                    };

                    if let Err(reason) = Self::accept_epoch_and_phase(entry, &up.epoch_header) {
                        return ignored_order_event(uid, reason);
                    }

                    if matches!(
                        up.epoch_header.status,
                        OrderWorkerStatus::BuySet | OrderWorkerStatus::SellSet
                    ) {
                        // Apply delta-update. Delphi applies UpdateData only
                        // for OS_BuySet and OS_SellSet; terminal statuses only
                        // move Status/SellReason and do not overwrite order
                        // compact fields.
                        let mut data = up.update_data;
                        data.adjust_time(server_time_delta);

                        let target = if up.epoch_header.status == OrderWorkerStatus::SellSet {
                            &mut entry.sell_order
                        } else {
                            &mut entry.buy_order
                        };

                        target.int_id = data.int_id;
                        target.actual_price = data.actual_price;
                        target.open_time = data.open_time;
                        target.quantity = data.quantity;
                        target.quantity_remaining = data.quantity_remaining;
                        target.actual_q = data.actual_q;
                        target.total_btc = data.total_btc;
                        target.mean_price = data.mean_price;
                        target.partial_done = data.partial_done;
                        target.stop_flag = data.stop_flag;
                    }

                    if up.epoch_header.status == OrderWorkerStatus::None {
                        // Delphi updates vOrder.BuyCondPrice only in the
                        // pending-worker branch: `(Status = OS_None) and
                        // IsPending and (vOrder <> nil)`.
                        if entry.pending_buy_cond_price.is_some() {
                            entry.pending_buy_cond_price = Some(up.update_data.mean_price);
                        }
                    } else {
                        entry.pending_buy_cond_price = None;
                        entry.pending_cancel = false;
                    }
                    entry.status = up.epoch_header.status;
                    let sell_reason = SellReason::from_byte(up.sell_reason_code);
                    if up.sell_reason_code != 0 && sell_reason != entry.sell_reason {
                        entry.sell_reason = sell_reason;
                    }

                    if is_terminal {
                        entry.job_is_done = true;
                    }
                    if status == OrderWorkerStatus::SellDone {
                        Self::apply_sell_done_flags(entry);
                    }
                }

                if is_terminal {
                    self.mark_pending_removal(uid, now_ms, terminal_removal_delay_ms(status));
                    return (ApplyResult::Applied, self.updated_event(uid));
                }

                (ApplyResult::Applied, self.updated_event(uid))
            }

            // --- Replace response ---
            TradeCommand::OrderReplaceResponse(rr) => {
                let rr = *rr;
                let Some(entry) = self.order_mut(uid) else {
                    return ignored_order_event(uid, ApplyResult::OrderNotFound);
                };

                if let Err(reason) = Self::accept_epoch_and_phase(entry, &rr.epoch_header) {
                    return ignored_order_event(uid, reason);
                }

                let mut data = rr.update_data;
                data.adjust_time(server_time_delta);

                let target = if order_type_uses_buy_side(rr.order_type) {
                    &mut entry.buy_order
                } else {
                    &mut entry.sell_order
                };

                target.int_id = data.int_id;
                target.actual_price = data.actual_price;
                target.open_time = data.open_time;
                target.quantity = data.quantity;
                target.quantity_remaining = data.quantity_remaining;
                target.actual_q = data.actual_q;
                target.total_btc = data.total_btc;
                target.mean_price = data.mean_price;
                target.partial_done = data.partial_done;
                target.stop_flag = data.stop_flag;
                if rr.quantity_base > 0.0 {
                    target.quantity_base = rr.quantity_base;
                }

                // Clear the bulk_replace flag for this side: replace is acknowledged.
                if order_type_uses_buy_side(rr.order_type) {
                    entry.buy_price = rr.price;
                    entry.bulk_replace_buy = false;
                } else {
                    entry.sell_price = rr.price;
                    entry.bulk_replace_sell = false;
                }

                (ApplyResult::Applied, self.updated_event(uid))
            }

            // --- Stops update ---
            TradeCommand::OrderStopsUpdate(su) => {
                let Some(entry) = self.order_mut(uid) else {
                    return ignored_order_event(uid, ApplyResult::OrderNotFound);
                };
                if let Err(reason) = Self::accept_epoch_and_phase(entry, &su.epoch_header) {
                    return ignored_order_event(uid, reason);
                }
                entry.stops = su.stops;
                (ApplyResult::Applied, Some(OrderEvent::StopsChanged(uid)))
            }

            // --- VStop update ---
            TradeCommand::VStopUpdate(vs) => {
                let Some(entry) = self.order_mut(uid) else {
                    return ignored_order_event(uid, ApplyResult::OrderNotFound);
                };
                if let Err(reason) = Self::accept_epoch_and_phase(entry, &vs.epoch_header) {
                    return ignored_order_event(uid, reason);
                }
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
                    self.mark_pending_removal(uid, now_ms, 0);
                    (ApplyResult::Applied, self.updated_event(uid))
                } else {
                    ignored_order_event(uid, ApplyResult::OrderNotFound)
                }
            }

            // --- Dispatcher-level payloads, handled before ProcessCommandOrder ---
            TradeCommand::AllStatuses(_) | TradeCommand::ClosedSellOrderReport(_) => {
                ignored_order_event(uid, ApplyResult::NotApplicable)
            }

            // --- Client-originated outgoing commands: ignored by state ---
            TradeCommand::OrderReplace(c) => self.apply_noop_trade_epoch(uid, &c.epoch_header),
            TradeCommand::OrderCancel(c) => self.apply_noop_trade_epoch(uid, &c.epoch_header),
            TradeCommand::OrderStatusRequest(h) => self.apply_noop_trade_epoch(uid, &h),
            TradeCommand::TurnPanicSell(c) => self.apply_noop_trade_epoch(uid, &c.epoch_header),
            TradeCommand::TradeEpoch(h) => self.apply_noop_trade_epoch(uid, &h),

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
    ) -> (ApplyResult, Option<OrderEvent>) {
        let Some(entry) = self.order_mut(uid) else {
            return ignored_order_event(uid, ApplyResult::OrderNotFound);
        };

        if let Err(reason) = Self::accept_epoch_and_phase(entry, header) {
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
