//! Orders sync state — auto-apply входящих TBaseTradeCommand к локальной модели.
//!
//! Источник Delphi: `MoonProtoClient.pas:513-666` (ProcessCommandOrder + CleanupMissingWorkers)
//! + `TaskWorkers.pas:1428-1509` (AcceptServerCommand + HandleServerCommand) + `DoTheJobVirtual`.
//!
//! ## Что делает этот модуль
//!
//! Зеркало серверных ордеров — клиент применяет команды и получает события.
//! Это **полная** замена клиентского `BOrderWorker.DoTheJobVirtual` + `WCache` + `CleanupMissingWorkers`.
//!
//! Поддерживается:
//! - Epoch protection (per-status `server_latest_epoch`).
//! - Phase rollback protection (нельзя откатить в более раннюю фазу).
//! - Snapshot flag mechanism (`current_snapshot_flag` инкрементируется при TAllStatuses,
//!   ордера без свежего флага → запрашиваются через `missing_after_snapshot()`).
//! - BulkReplace tracking (set replace-pending flag на UID'ах).
//! - Trace points accumulation.
//! - Corridor state.
//! - VStop state.
//! - Deferred removal на терминальном статусе / TOrderNotFound.
//! - ServerTimeDelta correction для всех TDateTime полей.

use crate::commands::trade::*;
use crate::state::eps::EpsProfile;
use std::collections::{HashMap, HashSet};

mod accessors;
mod actions;
mod apply_helpers;
mod maintenance;
mod model;
mod types;

pub use self::model::Order;
pub use self::types::{ApplyResult, OrderEvent, OrderTraceChartPoint, OrderTraceLine, SellReason};
pub(crate) use self::types::{OrderCancelSend, PanicSellSend};

const BULK_REPLACE_TIMEOUT_MS: i64 = 5000;
const SELL_DONE_REMOVAL_GRACE_MS: i64 = 400;
const PENDING_CANCEL_REPEAT_MS: i64 = 32;

/// Wrapping-safe epoch comparison.
/// Соответствует MoonProtoFunc.pas:188-203 `EpochIsOK`:
///   if LastEpoch = NewEpoch then Result := false;   // ДУБЛИКАТ
///   backDist := LastEpoch - NewEpoch;               // Word wrapping subtraction
///   if backDist <= 100 then Result := false         // STALE (до 100 назад)
///   else Result := true;                            // ACCEPT
///
/// Возвращает `true` если new — действительно новое значение (не дубликат, не stale).
/// Используется AcceptServerCommand в BOrderWorker (TaskWorkers.pas:1440).
// `epoch_is_ok` теперь общий через `state::epoch::epoch_is_ok` (audit_rust_quality #1).
// Окно stale = 100 взято из Delphi `MoonProtoFunc.pas:188-203`.
use super::epoch::epoch_is_ok;

/// Маппинг status → phase number.
/// Соответствует TaskWorkers.pas:546-555 `StatusPhase`:
///   OS_BuySet              → 1
///   OS_BuyDone             → 2
///   OS_SellSet             → 3
///   OS_SelLAlmostDone /
///   OS_SelLDone            → 4
///   все остальные (None, BuyFail, BuyCancel, SellFail, SellCancel) → 0
///
/// Phase rollback применяется только когда оба `new_phase > 0` и `cur_phase > 0`
/// (терминальные статусы с phase=0 не проверяются).
fn status_phase(s: OrderWorkerStatus) -> u8 {
    match s {
        OrderWorkerStatus::BuySet => 1,
        OrderWorkerStatus::BuyDone => 2,
        OrderWorkerStatus::SellSet => 3,
        OrderWorkerStatus::SelLAlmostDone | OrderWorkerStatus::SelLDone => 4,
        _ => 0,
    }
}

fn order_type_uses_buy_side(order_type: OrderType) -> bool {
    order_type == OrderType::Buy
}

fn terminal_removal_delay_ms(status: OrderWorkerStatus) -> i64 {
    if status == OrderWorkerStatus::SelLDone {
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

/// Главная коллекция ордеров.
///
/// **Однопоточная** — модифицируется только из main thread клиента.
/// Юзер получает read-only ссылки через `iter()`, `get()`.
#[derive(Debug, Clone, Default)]
pub struct Orders {
    map: HashMap<u64, Order>,
    /// Local/UI visual-order markers registered before the first server
    /// `TOrderStatus` creates the read-model entry.
    pending_local_visual_orders: HashSet<u64>,
    /// UID'ы, которые Delphi worker уже пометил бы как завершающиеся, но ещё
    /// не удалил бы из `WCache` прямо внутри `ProcessCommandOrder`.
    pending_removals: Vec<PendingRemoval>,
    /// Инкрементируется при каждом TAllStatuses (CurrentSnapshotFlag в Delphi).
    current_snapshot_flag: u8,
    /// ServerTimeDelta = InitialTime(server) - Now(client). Применяется к временам в командах.
    pub server_time_delta: f64,
    eps_profile: EpsProfile,
}

impl Orders {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            pending_local_visual_orders: HashSet::new(),
            pending_removals: Vec::new(),
            current_snapshot_flag: 0,
            server_time_delta: 0.0,
            eps_profile: EpsProfile::default(),
        }
    }

    pub(crate) fn set_eps_profile(&mut self, eps_profile: EpsProfile) {
        self.eps_profile = eps_profile;
    }

    /// Delphi `Inc(CurrentSnapshotFlag)` before `TAllStatuses` item loop.
    pub(crate) fn begin_snapshot(&mut self) -> u8 {
        self.current_snapshot_flag = self.current_snapshot_flag.wrapping_add(1);
        self.current_snapshot_flag
    }

    /// Применить команду из канала MPC_Order. Возвращает событие для UI/каллера.
    ///
    /// Это **главная** функция модуля. Внутри:
    /// 1. Проверка epoch (anti out-of-order).
    /// 2. Проверка phase rollback.
    /// 3. Применение к Order (или создание нового).
    /// 4. ServerTimeDelta correction для TDateTime полей.
    /// 5. Deferred removal при terminal status / TOrderNotFound.
    /// 6. Snapshot flag mechanics (CleanupMissing) through dispatcher-level
    ///    `TAllStatuses` handling.
    /// 7. Генерация события.
    ///
    /// **Замечание**: команды-запросы от клиента (AllStatusesRequest, OrderStatusRequest)
    /// возвращают `Ignored / NotApplicable` — это **исходящие** команды, не входящие.
    pub fn apply(&mut self, cmd: TradeCommand) -> (ApplyResult, OrderEvent) {
        self.apply_at(cmd, 0)
    }

    pub(crate) fn apply_at(&mut self, cmd: TradeCommand, now_ms: i64) -> (ApplyResult, OrderEvent) {
        let uid = cmd.uid();
        if command_marks_existing_worker_snapshot_flag(&cmd) {
            if let Some(entry) = self.map.get_mut(&uid) {
                entry.snapshot_flag = self.current_snapshot_flag;
            }
        }
        match cmd {
            // --- Full status (создание или обновление) ---
            TradeCommand::OrderStatus(st) => {
                let new_order = !self.map.contains_key(&uid);
                let status = st.epoch_header.status;
                if new_order && st.from_cache {
                    return (
                        ApplyResult::OrderNotFound,
                        OrderEvent::Ignored {
                            uid,
                            reason: ApplyResult::OrderNotFound,
                        },
                    );
                }
                let pending_local_visual_order = self.pending_local_visual_orders.remove(&uid);
                let is_done = {
                    let entry = self
                        .map
                        .entry(uid)
                        .or_insert_with(|| Order::from_status(&st));

                    // Delphi new-order path goes ProcessCommandOrder ->
                    // OnMServerOrder -> HandleServerCommand(Cmd), bypassing
                    // AcceptServerCommand and therefore not touching
                    // FServerLatestEpoch for the first full status.
                    if !new_order {
                        if let Err(reason) = Self::accept_epoch_and_phase(entry, &st.epoch_header) {
                            return (reason, OrderEvent::Ignored { uid, reason });
                        }
                    }

                    Self::apply_status_inner(
                        entry,
                        &st,
                        self.server_time_delta,
                        new_order,
                        pending_local_visual_order,
                        self.eps_profile.eps_m,
                    );
                    entry.snapshot_flag = self.current_snapshot_flag;
                    entry.job_is_done
                };
                if is_done {
                    self.mark_pending_removal(uid, now_ms, terminal_removal_delay_ms(status));
                }

                if new_order {
                    (ApplyResult::Applied, OrderEvent::Created(uid))
                } else if is_done {
                    (ApplyResult::Applied, OrderEvent::Updated(uid))
                } else {
                    (ApplyResult::Applied, OrderEvent::Updated(uid))
                }
            }

            // --- Delta-update ---
            TradeCommand::OrderStatusUpdate(up) => {
                let status = up.epoch_header.status;
                let is_terminal = status.is_terminal();
                {
                    let Some(entry) = self.map.get_mut(&uid) else {
                        return (
                            ApplyResult::OrderNotFound,
                            OrderEvent::Ignored {
                                uid,
                                reason: ApplyResult::OrderNotFound,
                            },
                        );
                    };

                    if let Err(reason) = Self::accept_epoch_and_phase(entry, &up.epoch_header) {
                        return (reason, OrderEvent::Ignored { uid, reason });
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
                        data.adjust_time(self.server_time_delta);

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
                    if up.sell_reason_code != 0 && up.sell_reason_code != entry.sell_reason_code {
                        entry.sell_reason_code = up.sell_reason_code;
                    }

                    if is_terminal {
                        entry.job_is_done = true;
                    }
                    if status == OrderWorkerStatus::SelLDone {
                        Self::apply_sell_done_flags(entry);
                    }
                }

                if is_terminal {
                    self.mark_pending_removal(uid, now_ms, terminal_removal_delay_ms(status));
                    return (ApplyResult::Applied, OrderEvent::Updated(uid));
                }

                (ApplyResult::Applied, OrderEvent::Updated(uid))
            }

            // --- Replace response ---
            TradeCommand::OrderReplaceResponse(rr) => {
                let rr = *rr;
                let Some(entry) = self.map.get_mut(&uid) else {
                    return (
                        ApplyResult::OrderNotFound,
                        OrderEvent::Ignored {
                            uid,
                            reason: ApplyResult::OrderNotFound,
                        },
                    );
                };

                if let Err(reason) = Self::accept_epoch_and_phase(entry, &rr.epoch_header) {
                    return (reason, OrderEvent::Ignored { uid, reason });
                }

                let mut data = rr.update_data;
                data.adjust_time(self.server_time_delta);

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

                // Сбрасываем bulk_replace флаг на этой стороне (replace подтверждён).
                if order_type_uses_buy_side(rr.order_type) {
                    entry.buy_price = rr.price;
                    entry.bulk_replace_buy = false;
                } else {
                    entry.sell_price = rr.price;
                    entry.bulk_replace_sell = false;
                }

                (ApplyResult::Applied, OrderEvent::Updated(uid))
            }

            // --- Stops update ---
            TradeCommand::OrderStopsUpdate(su) => {
                let Some(entry) = self.map.get_mut(&uid) else {
                    return (
                        ApplyResult::OrderNotFound,
                        OrderEvent::Ignored {
                            uid,
                            reason: ApplyResult::OrderNotFound,
                        },
                    );
                };
                if let Err(reason) = Self::accept_epoch_and_phase(entry, &su.epoch_header) {
                    return (reason, OrderEvent::Ignored { uid, reason });
                }
                entry.stops = su.stops;
                (ApplyResult::Applied, OrderEvent::StopsChanged(uid))
            }

            // --- VStop update ---
            TradeCommand::VStopUpdate(vs) => {
                let Some(entry) = self.map.get_mut(&uid) else {
                    return (
                        ApplyResult::OrderNotFound,
                        OrderEvent::Ignored {
                            uid,
                            reason: ApplyResult::OrderNotFound,
                        },
                    );
                };
                if let Err(reason) = Self::accept_epoch_and_phase(entry, &vs.epoch_header) {
                    return (reason, OrderEvent::Ignored { uid, reason });
                }
                entry.vstop_on = vs.vstop_on;
                entry.vstop_fixed = vs.vstop_fixed;
                entry.vstop_level = vs.vstop_level;
                entry.vstop_vol = vs.vstop_vol;
                (ApplyResult::Applied, OrderEvent::VStopChanged(uid))
            }

            // --- Corridor update ---
            TradeCommand::CorridorUpdate(cu) => {
                let Some(entry) = self.map.get_mut(&uid) else {
                    return (
                        ApplyResult::OrderNotFound,
                        OrderEvent::Ignored {
                            uid,
                            reason: ApplyResult::OrderNotFound,
                        },
                    );
                };
                entry.is_moon_shot = true;
                entry.corridor_price_down = cu.price_down;
                entry.corridor_price_up = cu.price_up;
                (ApplyResult::Applied, OrderEvent::CorridorChanged(uid))
            }

            // --- Trace point ---
            TradeCommand::OrderTracePoint(mut tp) => {
                let Some(entry) = self.map.get_mut(&uid) else {
                    return (
                        ApplyResult::OrderNotFound,
                        OrderEvent::Ignored {
                            uid,
                            reason: ApplyResult::OrderNotFound,
                        },
                    );
                };
                tp.adjust_time(self.server_time_delta);
                Self::apply_trace_line(entry, &tp);
                entry.trace_points.push_back(tp);
                (ApplyResult::Applied, OrderEvent::TracePoint { uid })
            }

            // --- Bulk replace notification ---
            TradeCommand::BulkReplaceNotify(brn) => {
                let mut affected = Vec::new();
                for &uid_replaced in &brn.uids {
                    if let Some(entry) = self.map.get_mut(&uid_replaced) {
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
                    return (
                        ApplyResult::OrderNotFound,
                        OrderEvent::Ignored {
                            uid,
                            reason: ApplyResult::OrderNotFound,
                        },
                    );
                }
                (
                    ApplyResult::Applied,
                    OrderEvent::BulkReplaced {
                        order_type: brn.order_type,
                        uids: affected,
                    },
                )
            }

            // --- Order not found (server forced remove) ---
            TradeCommand::OrderNotFound(h) => {
                let uid = h.market.base.uid;
                let found = if let Some(entry) = self.map.get_mut(&uid) {
                    entry.server_forced_remove = true;
                    entry.cancel_request = true;
                    true
                } else {
                    false
                };
                if found {
                    self.mark_pending_removal(uid, now_ms, 0);
                    (ApplyResult::Applied, OrderEvent::Updated(uid))
                } else {
                    (
                        ApplyResult::OrderNotFound,
                        OrderEvent::Ignored {
                            uid,
                            reason: ApplyResult::OrderNotFound,
                        },
                    )
                }
            }

            // --- Dispatcher-level aggregate, handled before ProcessCommandOrder ---
            TradeCommand::AllStatuses(_) => (
                ApplyResult::NotApplicable,
                OrderEvent::Ignored {
                    uid,
                    reason: ApplyResult::NotApplicable,
                },
            ),

            // --- Client-originated команды (исходящие) — игнорируются в state ---
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
            | TradeCommand::SetImmune(_) => (
                ApplyResult::NotApplicable,
                OrderEvent::Ignored {
                    uid,
                    reason: ApplyResult::NotApplicable,
                },
            ),

            // --- Прочие ---
            TradeCommand::Penalty(_)
            | TradeCommand::TradeVisual(_)
            | TradeCommand::BaseMarket(_) => (
                ApplyResult::NotApplicable,
                OrderEvent::Ignored {
                    uid,
                    reason: ApplyResult::NotApplicable,
                },
            ),

            TradeCommand::Unknown { uid, .. } => (
                ApplyResult::NotApplicable,
                OrderEvent::Ignored {
                    uid,
                    reason: ApplyResult::NotApplicable,
                },
            ),
        }
    }

    fn apply_noop_trade_epoch(
        &mut self,
        uid: u64,
        header: &TradeEpochHeader,
    ) -> (ApplyResult, OrderEvent) {
        let Some(entry) = self.map.get_mut(&uid) else {
            return (
                ApplyResult::OrderNotFound,
                OrderEvent::Ignored {
                    uid,
                    reason: ApplyResult::OrderNotFound,
                },
            );
        };

        if let Err(reason) = Self::accept_epoch_and_phase(entry, header) {
            return (reason, OrderEvent::Ignored { uid, reason });
        }

        (
            ApplyResult::NotApplicable,
            OrderEvent::Ignored {
                uid,
                reason: ApplyResult::NotApplicable,
            },
        )
    }
}

#[cfg(test)]
mod tests;
