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
use std::collections::{HashMap, HashSet, VecDeque};

mod types;

pub use self::types::{ApplyResult, OrderEvent, OrderTraceChartPoint, OrderTraceLine, SellReason};
pub(crate) use self::types::{OrderCancelSend, PanicSellSend};

const BULK_REPLACE_TIMEOUT_MS: i64 = 5000;
const PRICE_EPS: f64 = 0.000000009;
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

/// Один ордер с зеркальным состоянием.
///
/// Поля соответствуют BOrderWorker fields, которые приходят от сервера через
/// TOrderStatus / TOrderStatusUpdate / TOrderReplaceResponse / TOrderStopsUpdate /
/// TVStopUpdate / TCorridorUpdate / TOrderTracePoint.
#[derive(Debug, Clone)]
pub struct Order {
    /// Уникальный ID ордера = task UID (MServerTag в Delphi).
    pub uid: u64,
    /// Имя маркета (например "BTCUSDT").
    pub market_name: String,
    /// Base currency byte copied from the order command market header.
    pub currency: u8,
    /// Exchange/platform byte copied from the order command market header.
    pub platform: u8,
    /// Текущая фаза lifecycle.
    pub status: OrderWorkerStatus,
    /// Buy ордер на бирже.
    pub buy_order: OrderCompact,
    /// Sell ордер на бирже.
    pub sell_order: OrderCompact,
    /// Delphi `pBuyOrder.Price`: desired/local replace price, not part of
    /// `TOrderCompact` wire data.
    pub buy_price: f64,
    /// Delphi `pSellOrder.Price`: desired/local replace price, not part of
    /// `TOrderCompact` wire data.
    pub sell_price: f64,
    /// Настройки стопов.
    pub stops: StopSettings,
    /// VStop состояние.
    pub vstop_on: bool,
    pub vstop_fixed: bool,
    pub vstop_level: f64,
    pub vstop_vol: f64,
    /// Delphi `BOrderWorker.FPanicSell`, local outgoing panic-sell intent.
    pub panic_sell: bool,
    /// Delphi `BOrderWorker.IsMoonShot`, raised by `TCorridorUpdate`.
    pub is_moon_shot: bool,
    /// Корридор цен (последний апдейт).
    pub corridor_price_down: f32,
    pub corridor_price_up: f32,
    /// Связь со стратегией.
    pub strat_id: u64,
    pub is_short: bool,
    pub db_id: i32,
    /// True если order пришёл из server cache (восстановление после reconnect).
    pub from_cache: bool,
    /// True если ордер торгуется в emulator mode.
    pub emulator_mode: bool,
    /// True если UI клики должны игнорироваться (server-forced).
    pub immune_for_clicks: bool,
    /// Rust read-model marker for Delphi `BOrderWorker.vOrder <> nil`.
    ///
    /// Stop/VStop outgoing worker actions require this marker, because Delphi
    /// `SendStopsIfChanged` / `SendVStopIfChanged` exit immediately when no
    /// visual order is attached to the worker.
    pub has_local_visual_order: bool,
    /// Delphi `vOrder.BuyCondPrice` for pending `OS_None` orders.
    pub pending_buy_cond_price: Option<f64>,
    /// Delphi `vOrder.PendingCancel` for pending `OS_None` orders.
    pub pending_cancel: bool,
    /// Тип ордера, на котором установлен BulkReplace.
    pub bulk_replace_buy: bool,
    pub bulk_replace_sell: bool,
    /// Delphi `coBuy` order-line state built by `ApplyServerTrace`.
    pub buy_trace_line: Option<OrderTraceLine>,
    /// Delphi `coSell` order-line state built by `ApplyServerTrace`.
    pub sell_trace_line: Option<OrderTraceLine>,
    /// Trace points (визуализация решения сервера).
    ///
    /// This is the raw inbound packet log. For Delphi-equivalent chart state,
    /// use `buy_trace_line` / `sell_trace_line`.
    pub trace_points: VecDeque<OrderTracePoint>,
    /// True если ордер терминален и ожидает deferred removal.
    pub job_is_done: bool,
    /// Delphi `CancellRequest`: server requested worker cancellation.
    pub cancel_request: bool,
    /// Server-forced removal (TOrderNotFound пришёл).
    pub server_forced_remove: bool,
    /// Reason code последней продажи.
    pub sell_reason_code: u8,

    // --- Internal sync state (не нужно потребителю) ---
    /// Per-status monotonic epoch (anti out-of-order). Размер по количеству статусов.
    server_latest_epoch: [u16; 10],
    /// Snapshot flag — обновляется при TAllStatuses.
    pub(crate) snapshot_flag: u8,
    replace_sent_time_ms: i64,
    pending_cancel_sent_ms: i64,
    prev_panic_sell: bool,
    last_buy_actual_price: f64,
    last_sell_actual_price: f64,
}

impl Order {
    /// Build the outgoing trade context for commands that target this tracked
    /// order.
    ///
    /// The context preserves the currency/platform bytes received from the
    /// server-side order state. This avoids hard-coding the current exchange
    /// configuration in consumers.
    pub fn trade_ctx(&self) -> TradeCtx {
        TradeCtx::with_route(self.uid, self.currency, self.platform)
    }

    /// Причина закрытия как enum. Удобный getter для UI.
    /// См. [`SellReason`] для описания всех значений.
    pub fn sell_reason(&self) -> SellReason {
        SellReason::from_u8(self.sell_reason_code)
    }

    /// Создать новый Order из TOrderStatus.
    fn from_status(status_cmd: &OrderStatus) -> Self {
        Self {
            uid: status_cmd.epoch_header.market.base.uid,
            market_name: status_cmd.epoch_header.market.market_name.clone(),
            currency: status_cmd.epoch_header.market.currency,
            platform: status_cmd.epoch_header.market.platform,
            status: OrderWorkerStatus::None,
            buy_order: status_cmd.buy_order,
            sell_order: status_cmd.sell_order,
            buy_price: 0.0,
            sell_price: 0.0,
            stops: status_cmd.stops,
            vstop_on: false,
            vstop_fixed: false,
            vstop_level: 0.0,
            vstop_vol: 0.0,
            panic_sell: false,
            is_moon_shot: false,
            corridor_price_down: 0.0,
            corridor_price_up: 0.0,
            strat_id: status_cmd.strat_id,
            is_short: status_cmd.is_short,
            db_id: status_cmd.db_id,
            from_cache: status_cmd.from_cache,
            emulator_mode: status_cmd.emulator_mode,
            immune_for_clicks: status_cmd.immune_for_clicks,
            has_local_visual_order: false,
            pending_buy_cond_price: None,
            pending_cancel: false,
            bulk_replace_buy: false,
            bulk_replace_sell: false,
            buy_trace_line: None,
            sell_trace_line: None,
            trace_points: VecDeque::new(),
            job_is_done: status_cmd.epoch_header.status.is_terminal(),
            cancel_request: false,
            server_forced_remove: false,
            sell_reason_code: 0,
            server_latest_epoch: [0; 10],
            snapshot_flag: 0,
            replace_sent_time_ms: 0,
            pending_cancel_sent_ms: 0,
            prev_panic_sell: false,
            last_buy_actual_price: 0.0,
            last_sell_actual_price: 0.0,
        }
    }
}

impl From<&Order> for TradeCtx {
    fn from(order: &Order) -> Self {
        order.trade_ctx()
    }
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
}

impl Orders {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            pending_local_visual_orders: HashSet::new(),
            pending_removals: Vec::new(),
            current_snapshot_flag: 0,
            server_time_delta: 0.0,
        }
    }

    /// Получить ордер по UID.
    pub fn get(&self, uid: u64) -> Option<&Order> {
        self.map.get(&uid)
    }

    /// Итератор по всем ордерам.
    pub fn iter(&self) -> impl Iterator<Item = &Order> {
        self.map.values()
    }

    /// Количество ордеров.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Текущее значение snapshot flag.
    pub fn current_snapshot_flag(&self) -> u8 {
        self.current_snapshot_flag
    }

    /// Mark an order UID as having a Delphi local `vOrder`.
    ///
    /// This is the public counterpart of UI/local order paths that assign
    /// `NewOrder.vOrder := vo` before worker-side stop/VStop actions can send.
    /// If the server order has not arrived yet, the marker is stored and applied
    /// to the first `TOrderStatus` with the same UID.
    pub fn mark_local_visual_order(&mut self, uid: u64) -> bool {
        if let Some(order) = self.map.get_mut(&uid) {
            order.has_local_visual_order = true;
            true
        } else {
            self.pending_local_visual_orders.insert(uid);
            false
        }
    }

    fn is_proper(order: &Order, market_name: &str, side: FixedPosition) -> bool {
        if order.market_name != market_name {
            return false;
        }
        match side {
            FixedPosition::Both => true,
            FixedPosition::Long => !order.is_short,
            FixedPosition::Short => order.is_short,
            _ => true,
        }
    }

    /// Delphi active-client pre-send gate for
    /// `TOrdersWorkers.MoveAllSells`.
    ///
    /// `MoveKind` mode checks side + non-immune and rejects `RM_None`;
    /// `PriceZone` mode checks only market/status/non-immune before sending;
    /// `%` (`Pers`) mode ignores immunity, matching the separate Delphi overload.
    pub fn has_move_all_sells_candidate(
        &self,
        market_name: &str,
        params: MoveAllSellsParams,
    ) -> bool {
        match params.cmd_type {
            MoveAllCmdType::MoveKind => {
                params.move_kind != ReplaceMultiKind::None
                    && self.map.values().any(|order| {
                        Self::is_proper(order, market_name, params.side)
                            && order.status == OrderWorkerStatus::SellSet
                            && !order.immune_for_clicks
                    })
            }
            MoveAllCmdType::PriceZone => self.map.values().any(|order| {
                order.market_name == market_name
                    && order.status == OrderWorkerStatus::SellSet
                    && !order.immune_for_clicks
            }),
            MoveAllCmdType::Pers => self.map.values().any(|order| {
                order.market_name == market_name && order.status == OrderWorkerStatus::SellSet
            }),
            _ => false,
        }
    }

    /// Delphi active-client pre-send gate for
    /// `TOrdersWorkers.MoveAllBuys`.
    ///
    /// Buy bulk move has only `MoveKind` and `%` modes in Delphi; there is no
    /// buy-side `PriceZone` command.
    pub fn has_move_all_buys_candidate(
        &self,
        market_name: &str,
        cmd_type: MoveAllBuysCmdType,
        move_kind: ReplaceMultiKind,
        side: FixedPosition,
    ) -> bool {
        match cmd_type {
            MoveAllBuysCmdType::MoveKind => {
                move_kind != ReplaceMultiKind::None
                    && self.map.values().any(|order| {
                        Self::is_proper(order, market_name, side)
                            && order.status == OrderWorkerStatus::BuySet
                            && !order.immune_for_clicks
                    })
            }
            MoveAllBuysCmdType::Pers => self.map.values().any(|order| {
                order.market_name == market_name && order.status == OrderWorkerStatus::BuySet
            }),
            _ => false,
        }
    }

    /// Delphi `TOrdersWorkers.SetImmuneClicks` local side effect.
    ///
    /// Returns only items whose local active order was found and mutated. The
    /// caller should send exactly these items in `TSetImmuneCommand`; an empty
    /// list means Delphi would not put anything on the wire.
    pub fn set_immune_clicks(&mut self, items: &[ImmuneItem]) -> Vec<ImmuneItem> {
        let mut applied = Vec::new();
        for item in items {
            let Some(order) = self.map.get_mut(&item.uid) else {
                continue;
            };
            if order.status.is_terminal() {
                continue;
            }
            order.immune_for_clicks = item.value;
            applied.push(*item);
        }
        applied
    }

    /// Delphi `BOrderWorker.SendStopsIfChanged` local machine effect.
    ///
    /// Returns the wire context only when a local worker exists and the stop
    /// record differs from the last applied/sent value. The comparison uses
    /// `StopSettings::eq`, which is bit-exact like Delphi `CompareMem`.
    pub(crate) fn send_stops_if_changed(
        &mut self,
        uid: u64,
        stops: &StopSettings,
    ) -> Option<(TradeCtx, String, OrderWorkerStatus, StopSettings)> {
        let order = self.map.get_mut(&uid)?;
        if !order.has_local_visual_order {
            return None;
        }
        if order.stops == *stops {
            return None;
        }
        order.stops = *stops;
        Some((
            order.trade_ctx(),
            order.market_name.clone(),
            order.status,
            *stops,
        ))
    }

    /// Delphi `BOrderWorker.SendVStopIfChanged` local machine effect.
    ///
    /// The outgoing packet uses the current worker status, not a caller-provided
    /// status. The local VStop state is updated before queueing, just as Delphi
    /// updates `FPrevVStop*` before `MClient.SendOrderCmd`.
    pub(crate) fn send_vstop_if_changed(
        &mut self,
        uid: u64,
        vstop_on: bool,
        vstop_fixed: bool,
        vstop_level: f64,
        vstop_vol: f64,
    ) -> Option<(TradeCtx, String, VStopUpdateParams)> {
        let order = self.map.get_mut(&uid)?;
        if !order.has_local_visual_order {
            return None;
        }
        if order.vstop_on == vstop_on
            && order.vstop_fixed == vstop_fixed
            && order.vstop_level == vstop_level
            && order.vstop_vol == vstop_vol
        {
            return None;
        }

        order.vstop_on = vstop_on;
        order.vstop_fixed = vstop_fixed;
        order.vstop_level = vstop_level;
        order.vstop_vol = vstop_vol;

        Some((
            order.trade_ctx(),
            order.market_name.clone(),
            VStopUpdateParams {
                status: order.status,
                vstop_on,
                vstop_fixed,
                vstop_level,
                vstop_vol,
            },
        ))
    }

    /// Delphi `BOrderWorker.DoTheJobVirtual.CheckReplaceFlag` replace part.
    ///
    /// This combines the local UI intent (`p*Order.Price` +
    /// `p*Order.OrderReplace := true`) with the worker tick that sends only when
    /// `ReplaceSentTime = 0`. If a replace is already in flight, the local
    /// desired price is updated but no new packet is queued.
    pub(crate) fn send_replace_if_requested(
        &mut self,
        uid: u64,
        new_price: f64,
        now_ms: i64,
    ) -> Option<(TradeCtx, String, OrderType, f64)> {
        let order = self.map.get_mut(&uid)?;
        let order_type = match order.status {
            OrderWorkerStatus::None => {
                let prev = order.pending_buy_cond_price?;
                if (prev - new_price).abs() <= PRICE_EPS {
                    return None;
                }
                order.pending_buy_cond_price = Some(new_price);
                OrderType::Buy
            }
            OrderWorkerStatus::BuySet => {
                let order_type = OrderType::from_byte(order.buy_order.order_type);
                if order.replace_sent_time_ms > 0 && !order.bulk_replace_buy {
                    order.replace_sent_time_ms = 0;
                }
                order.buy_price = new_price;
                order.bulk_replace_buy = true;
                if order.replace_sent_time_ms > 0 {
                    return None;
                }
                order.replace_sent_time_ms = now_ms.max(1);
                order_type
            }
            OrderWorkerStatus::SellSet => {
                let order_type = OrderType::from_byte(order.sell_order.order_type);
                if order.replace_sent_time_ms > 0 && !order.bulk_replace_sell {
                    order.replace_sent_time_ms = 0;
                }
                order.sell_price = new_price;
                order.bulk_replace_sell = true;
                if order.replace_sent_time_ms > 0 {
                    return None;
                }
                order.replace_sent_time_ms = now_ms.max(1);
                order_type
            }
            _ => return None,
        };

        Some((
            order.trade_ctx(),
            order.market_name.clone(),
            order_type,
            new_price,
        ))
    }

    /// Delphi `BOrderWorker.DoTheJobVirtual.CheckReplaceFlag` cancel part.
    ///
    /// Active buy/sell orders use local `FOrder.CancelRequest` and clear it
    /// after queueing. Pending `OS_None` orders use `vOrder.PendingCancel` and
    /// keep that flag set like Delphi.
    pub(crate) fn send_cancel_if_requested(
        &mut self,
        uid: u64,
        now_ms: i64,
    ) -> Option<OrderCancelSend> {
        let order = self.map.get_mut(&uid)?;
        match order.status {
            OrderWorkerStatus::None => {
                let price = order.pending_buy_cond_price?;
                order.pending_cancel = true;
                order.pending_cancel_sent_ms = now_ms.max(1);
                let out = OrderCancelSend::PendingReplaceThenCancel {
                    ctx: order.trade_ctx(),
                    market: order.market_name.clone(),
                    price,
                };
                return Some(out);
            }
            OrderWorkerStatus::BuySet | OrderWorkerStatus::SellSet => {}
            _ => return None,
        }

        order.cancel_request = true;
        let out = OrderCancelSend::Cancel {
            ctx: order.trade_ctx(),
            market: order.market_name.clone(),
            status: order.status,
        };
        order.cancel_request = false;
        Some(out)
    }

    /// Delphi `DoTheJobVirtual.CheckReplaceFlag` pending cancel branch.
    ///
    /// Once `vOrder.PendingCancel` is true, Delphi keeps sending the
    /// replace-then-cancel pair from the 32 ms worker loop until status leaves
    /// `OS_None`.
    pub(crate) fn tick_pending_cancel_resends(&mut self, now_ms: i64) -> Vec<OrderCancelSend> {
        let mut sends = Vec::new();
        for order in self.map.values_mut() {
            if order.status != OrderWorkerStatus::None || !order.pending_cancel {
                continue;
            }
            let Some(price) = order.pending_buy_cond_price else {
                continue;
            };
            if order.pending_cancel_sent_ms > 0
                && (now_ms - order.pending_cancel_sent_ms).abs() < PENDING_CANCEL_REPEAT_MS
            {
                continue;
            }
            order.pending_cancel_sent_ms = now_ms.max(1);
            sends.push(OrderCancelSend::PendingReplaceThenCancel {
                ctx: order.trade_ctx(),
                market: order.market_name.clone(),
                price,
            });
        }
        sends
    }

    /// Delphi `CheckReplaceFlag` panic-sell part.
    ///
    /// Sends only for `OS_SellSet` and only when `FPanicSell` differs from
    /// `PrevPanicSell`; then updates the previous value before queueing.
    pub(crate) fn send_panic_sell_if_changed(
        &mut self,
        uid: u64,
        turn_on: bool,
    ) -> Option<PanicSellSend> {
        let order = self.map.get_mut(&uid)?;
        if order.status != OrderWorkerStatus::SellSet {
            return None;
        }
        order.panic_sell = turn_on;
        if order.prev_panic_sell == order.panic_sell {
            return None;
        }
        order.prev_panic_sell = order.panic_sell;
        Some(PanicSellSend {
            ctx: order.trade_ctx(),
            market: order.market_name.clone(),
            turn_on: order.panic_sell,
        })
    }

    /// Delphi `TOrdersWorkers.TurnPanicSell(m, AValue)`.
    ///
    /// Sets `FPanicSell := AValue` on all active `OS_SellSet` workers in the
    /// market. The returned sends are exactly the workers whose
    /// `PrevPanicSell` differs and whose `CheckReplaceFlag` would enqueue
    /// `TTurnPanicSellCommand` on its next tick.
    pub(crate) fn turn_panic_sell_by_market(
        &mut self,
        market_name: &str,
        turn_on: bool,
    ) -> Vec<PanicSellSend> {
        let uids: Vec<u64> = self
            .map
            .values()
            .filter(|order| {
                order.market_name == market_name && order.status == OrderWorkerStatus::SellSet
            })
            .map(|order| order.uid)
            .collect();

        let mut sends = Vec::new();
        for uid in uids {
            if let Some(send) = self.send_panic_sell_if_changed(uid, turn_on) {
                sends.push(send);
            }
        }
        sends
    }

    /// Delphi `TOrdersWorkers.SwitchPanicSellByMarket(m, TurnON)`.
    ///
    /// If any active sell worker in the market already has `FPanicSell`, the
    /// function turns panic sell off for all active sell workers and returns
    /// `false`. Otherwise, when `TurnON` is true, it turns panic sell on for all
    /// active sell workers and returns `true`. When `TurnON` is false and
    /// nothing was active, it does nothing and returns `false`.
    pub(crate) fn switch_panic_sell_by_market(
        &mut self,
        market_name: &str,
        turn_on: bool,
    ) -> (bool, Vec<PanicSellSend>) {
        let mut was_turned_off = false;
        let mut was_turned_on = false;

        for order in self.map.values() {
            if order.market_name == market_name && order.status == OrderWorkerStatus::SellSet {
                if order.panic_sell {
                    was_turned_off = true;
                    break;
                } else if turn_on {
                    was_turned_on = true;
                    break;
                }
            }
        }

        if was_turned_off {
            (false, self.turn_panic_sell_by_market(market_name, false))
        } else if was_turned_on {
            (true, self.turn_panic_sell_by_market(market_name, true))
        } else {
            (false, Vec::new())
        }
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

    fn accept_epoch_and_phase(
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

    fn apply_status_inner(
        entry: &mut Order,
        st: &OrderStatus,
        server_time_delta: f64,
        new_order: bool,
        pending_local_visual_order: bool,
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
            entry.currency = st.epoch_header.market.currency;
            entry.platform = st.epoch_header.market.platform;
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
        if pending_local_visual_order {
            entry.has_local_visual_order = true;
        }
        if st.epoch_header.status == OrderWorkerStatus::None {
            if new_order {
                entry.has_local_visual_order = true;
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
            if (entry.buy_order.actual_price - entry.last_buy_actual_price).abs() > PRICE_EPS {
                entry.buy_price = entry.buy_order.actual_price;
                entry.last_buy_actual_price = entry.buy_order.actual_price;
            }
            if (entry.sell_order.actual_price - entry.last_sell_actual_price).abs() > PRICE_EPS {
                entry.sell_price = entry.sell_order.actual_price;
                entry.last_sell_actual_price = entry.sell_order.actual_price;
            }
        }

        if st.epoch_header.status == OrderWorkerStatus::SelLDone {
            Self::apply_sell_done_flags(entry);
        }
    }

    fn apply_sell_done_flags(entry: &mut Order) {
        // Delphi `BOrderWorker.SetDoneFlags` branch for `Status = OS_SelLDone`.
        entry.sell_order.is_closed = 1;
        entry.sell_order.is_opened = 0;
        entry.bulk_replace_sell = false;

        entry.buy_order.is_opened = 0;
        entry.bulk_replace_buy = false;
        if entry.buy_order.is_closed == 0 {
            entry.buy_order.canceled = 1;
        }
    }

    fn apply_trace_line(entry: &mut Order, tp: &OrderTracePoint) {
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

    fn mark_pending_removal(&mut self, uid: u64, now_ms: i64, delay_ms: i64) {
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
            if self.map.remove(&pending.uid).is_some() {
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
                if self.map.remove(&pending.uid).is_some() {
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

    /// После TAllStatuses найти ордера, которых **нет** в свежем snapshot.
    /// Эти UID'ы нужно явно запросить через `build_order_status_request`.
    /// Соответствует `MoonProtoClient.pas:637-666 CleanupMissingWorkers`.
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

    /// Установить ServerTimeDelta. Должно вызываться при апдейте Ping
    /// (`server_time_delta = initial_time - now`).
    pub fn set_server_time_delta(&mut self, delta: f64) {
        self.server_time_delta = delta;
    }

    /// Принудительно удалить ордер по UID (например, по решению UI).
    pub fn remove(&mut self, uid: u64) -> Option<Order> {
        self.pending_local_visual_orders.remove(&uid);
        self.map.remove(&uid)
    }

    /// Очистить весь state (при reconnect / WantNewHello).
    pub fn clear(&mut self) {
        self.map.clear();
        self.pending_local_visual_orders.clear();
        self.pending_removals.clear();
        self.current_snapshot_flag = 0;
    }
}

#[cfg(test)]
mod tests;
