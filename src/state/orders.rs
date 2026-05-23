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

const BULK_REPLACE_TIMEOUT_MS: i64 = 5000;
const PRICE_EPS: f64 = 0.000000009;
const SELL_DONE_REMOVAL_GRACE_MS: i64 = 400;
const PENDING_CANCEL_REPEAT_MS: i64 = 32;

/// Причина закрытия ордера. Соответствует Delphi `TSellReasonCode` (MarketsU.pas:245-261).
///
/// Сервер может выставить код в поле `sell_reason_code` у `OrderStatusUpdate`.
/// Delphi обновляет локальную причину продажи только когда код ненулевой и
/// отличается от предыдущего. Терминал хранит строку, но по wire идёт byte-код.
/// Используйте `SellReason::from_u8(order.sell_reason_code)` или `Order::sell_reason()`.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SellReason {
    /// Неизвестная / не выставлена.
    Unknown = 0,
    /// Продажа по установленной цене (дефолт).
    SellPrice = 1,
    /// Auto Price Down — автоматический спуск цены.
    AutoPriceDown = 2,
    /// Sell Level — продажа по уровню.
    SellLevel = 3,
    /// SellSpread — продажа по спреду.
    SellSpread = 4,
    /// SellShot — снайперская продажа.
    SellShot = 5,
    /// Global / Manual PanicSell.
    PanicSell = 6,
    /// StopLoss активирован.
    StopLoss = 7,
    /// Trailing Stop сработал.
    Trailing = 8,
    /// Market Stop.
    MarketStop = 9,
    /// Manual Sell (price < 95% от ожидания).
    ManualSell = 10,
    /// JoinedSell — объединённая продажа.
    JoinedSell = 11,
    /// SellFromAssets — продажа из активов.
    SellFromAssets = 12,
    /// BV/SV Stop.
    BvSvStop = 13,
    /// TakeProfit достигнут.
    TakeProfit = 14,
}

impl SellReason {
    /// Преобразовать byte в enum. Неизвестные коды (>14) → `Unknown`.
    pub fn from_u8(b: u8) -> Self {
        match b {
            1 => Self::SellPrice,
            2 => Self::AutoPriceDown,
            3 => Self::SellLevel,
            4 => Self::SellSpread,
            5 => Self::SellShot,
            6 => Self::PanicSell,
            7 => Self::StopLoss,
            8 => Self::Trailing,
            9 => Self::MarketStop,
            10 => Self::ManualSell,
            11 => Self::JoinedSell,
            12 => Self::SellFromAssets,
            13 => Self::BvSvStop,
            14 => Self::TakeProfit,
            _ => Self::Unknown,
        }
    }

    /// Человекочитаемое название (для UI отображения).
    pub fn description(&self) -> &'static str {
        match self {
            Self::Unknown => "Unknown",
            Self::SellPrice => "Sell Price",
            Self::AutoPriceDown => "Auto Price Down",
            Self::SellLevel => "Sell Level",
            Self::SellSpread => "SellSpread",
            Self::SellShot => "SellShot",
            Self::PanicSell => "PanicSell",
            Self::StopLoss => "StopLoss",
            Self::Trailing => "Trailing",
            Self::MarketStop => "Market Stop",
            Self::ManualSell => "Manual Sell",
            Self::JoinedSell => "JoinedSell",
            Self::SellFromAssets => "SellFromAssets",
            Self::BvSvStop => "BV/SV Stop",
            Self::TakeProfit => "TakeProfit",
        }
    }
}

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

fn delphi_now_days() -> f64 {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    25569.0 + secs / 86400.0
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

#[derive(Debug, Clone)]
pub(crate) enum OrderCancelSend {
    PendingReplaceThenCancel {
        ctx: TradeCtx,
        market: String,
        price: f64,
    },
    Cancel {
        ctx: TradeCtx,
        market: String,
        status: OrderWorkerStatus,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct PanicSellSend {
    pub ctx: TradeCtx,
    pub market: String,
    pub turn_on: bool,
}

/// One chart point in Delphi `TOrderLine.Points`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OrderTraceChartPoint {
    pub time: f64,
    pub price: f32,
}

/// Read-model counterpart of Delphi `coBuy` / `coSell` `TOrderLine`.
#[derive(Debug, Clone)]
pub struct OrderTraceLine {
    pub order_type: OrderType,
    pub order_id: i64,
    pub prevent_delete: bool,
    pub points: Vec<OrderTraceChartPoint>,
    pub tmp_point: Option<OrderTraceChartPoint>,
    pub can_finish: bool,
    pub stop_price: Option<f32>,
}

impl OrderTraceLine {
    fn new(order_type: OrderType, order_id: i64) -> Self {
        Self {
            order_type,
            order_id,
            prevent_delete: true,
            points: Vec::new(),
            tmp_point: None,
            can_finish: false,
            stop_price: None,
        }
    }

    fn set_point_trade(&mut self, time: f64, price: f32, is_temp: bool, is_finish: bool) {
        if is_finish {
            if self.points.len() > 1 && self.can_finish {
                if let Some(last) = self.points.last_mut() {
                    last.price = price;
                }
            }
            self.can_finish = false;
            return;
        }

        let point = OrderTraceChartPoint { time, price };
        if is_temp {
            self.tmp_point = Some(point);
            return;
        }

        if self.points.is_empty() {
            self.points.push(point);
            return;
        }

        let mut same_price_at_new_time = *self.points.last().unwrap();
        same_price_at_new_time.time = time;
        self.points.push(same_price_at_new_time);
        self.points.push(self.tmp_point.unwrap_or_default());

        let mut final_point = same_price_at_new_time;
        final_point.price = price;
        self.points.push(final_point);

        self.tmp_point = None;
        self.can_finish = true;
    }
}

impl Default for OrderTraceChartPoint {
    fn default() -> Self {
        Self {
            time: 0.0,
            price: 0.0,
        }
    }
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

/// Результат применения одной команды.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ApplyResult {
    /// Команда применена, state обновился.
    Applied,
    /// Команда устаревшая (epoch < server_latest_epoch для этого status).
    OutOfOrder,
    /// Phase rollback — команда из старой фазы пришла позже.
    PhaseRollback,
    /// Ордер не найден в state (например, TOrderStatusUpdate без предыдущего TOrderStatus).
    OrderNotFound,
    /// Команда не относится к Orders state (например, AllStatusesRequest от клиента).
    NotApplicable,
    /// Зарезервировано для обратной совместимости старых match-выражений.
    /// Текущий Delphi-parity state не отбрасывает новые ордера по внутреннему cap.
    Rejected,
}

/// Событие, которое сгенерировалось в результате apply.
/// Юзер получает через callback и реагирует (UI update / logic).
#[derive(Debug, Clone)]
pub enum OrderEvent {
    /// Новый ордер появился.
    Created(u64),
    /// Существующий ордер обновился (status / update / replace_response).
    Updated(u64),
    /// Ордер удалён после deferred cleanup terminal status / TOrderNotFound.
    Removed(u64),
    /// Bulk replace notification.
    BulkReplaced {
        order_type: OrderType,
        uids: Vec<u64>,
    },
    /// Trace point добавлен.
    TracePoint { uid: u64 },
    /// Корридор обновлён.
    CorridorChanged(u64),
    /// VStop изменился.
    VStopChanged(u64),
    /// Стопы изменились.
    StopsChanged(u64),
    /// TAllStatuses snapshot применён.
    Snapshot,
    /// Команда проигнорирована (out-of-order / phase rollback / unknown).
    Ignored { uid: u64, reason: ApplyResult },
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
#[derive(Debug, Default)]
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
                let order_type = OrderType::from_byte(order.buy_order.order_type)?;
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
                let order_type = OrderType::from_byte(order.sell_order.order_type)?;
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
                    let close_time = delphi_now_days();
                    entry.buy_order.is_opened = 0;
                    entry.buy_order.canceled = 1;
                    entry.buy_order.is_closed = 1;
                    entry.buy_order.close_time = close_time;
                    entry.bulk_replace_buy = false;
                    entry.sell_order.is_opened = 0;
                    entry.sell_order.canceled = 1;
                    entry.sell_order.is_closed = 1;
                    entry.sell_order.close_time = close_time;
                    entry.bulk_replace_sell = false;
                    entry.replace_sent_time_ms = 0;
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
        let phase_idx = header.status as usize;
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
        entry.market_name = st.epoch_header.market.market_name.clone();
        entry.currency = st.epoch_header.market.currency;
        entry.platform = st.epoch_header.market.platform;
        entry.buy_order = buy;
        entry.sell_order = sell;
        entry.stops = st.stops;
        entry.strat_id = st.strat_id;
        entry.is_short = st.is_short;
        entry.db_id = st.db_id;
        entry.from_cache = st.from_cache;
        entry.emulator_mode = st.emulator_mode;
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
mod tests {
    use super::*;

    fn make_base(uid: u64, ver: u16) -> BaseCommandHeader {
        BaseCommandHeader {
            cmd_id: 4,
            ver,
            uid,
        }
    }

    fn make_market(uid: u64, ver: u16, market_name: &str) -> MarketCommandHeader {
        MarketCommandHeader {
            base: make_base(uid, ver),
            currency: 1,
            platform: 4,
            market_name: market_name.to_string(),
        }
    }

    fn make_epoch(
        uid: u64,
        ver: u16,
        market: &str,
        epoch: u16,
        status: OrderWorkerStatus,
    ) -> TradeEpochHeader {
        TradeEpochHeader {
            market: make_market(uid, ver, market),
            epoch,
            status,
        }
    }

    fn make_status(uid: u64, market: &str, status: OrderWorkerStatus, epoch: u16) -> OrderStatus {
        OrderStatus {
            epoch_header: make_epoch(uid, 3, market, epoch, status),
            buy_order: OrderCompact::default(),
            sell_order: OrderCompact::default(),
            stops: StopSettings::default(),
            strat_id: 0,
            is_short: false,
            db_id: 0,
            from_cache: false,
            emulator_mode: false,
            immune_for_clicks: false,
        }
    }

    fn order_status_cmd(status: OrderStatus) -> TradeCommand {
        TradeCommand::OrderStatus(Box::new(status))
    }

    fn trace_point(
        uid: u64,
        order_type: OrderType,
        flags: u8,
        time: f64,
        price: f32,
        base: f32,
        stop: f32,
    ) -> OrderTracePoint {
        OrderTracePoint {
            market: make_market(uid, 3, "BTCUSDT"),
            trace_time: time,
            trace_price: price,
            base_price: base,
            stop_price: stop,
            ord_type: order_type,
            flags,
        }
    }

    #[test]
    fn sell_reason_descriptions_match_delphi_sell_reason_code_to_str() {
        let cases = [
            (SellReason::Unknown, "Unknown"),
            (SellReason::SellPrice, "Sell Price"),
            (SellReason::AutoPriceDown, "Auto Price Down"),
            (SellReason::SellLevel, "Sell Level"),
            (SellReason::SellSpread, "SellSpread"),
            (SellReason::SellShot, "SellShot"),
            (SellReason::PanicSell, "PanicSell"),
            (SellReason::StopLoss, "StopLoss"),
            (SellReason::Trailing, "Trailing"),
            (SellReason::MarketStop, "Market Stop"),
            (SellReason::ManualSell, "Manual Sell"),
            (SellReason::JoinedSell, "JoinedSell"),
            (SellReason::SellFromAssets, "SellFromAssets"),
            (SellReason::BvSvStop, "BV/SV Stop"),
            (SellReason::TakeProfit, "TakeProfit"),
        ];

        for (reason, expected) in cases {
            assert_eq!(reason.description(), expected);
        }
    }

    fn order_replace_response_cmd(response: OrderReplaceResponse) -> TradeCommand {
        TradeCommand::OrderReplaceResponse(Box::new(response))
    }

    #[test]
    fn terminal_status_marks_done_then_deferred_removal() {
        let mut orders = Orders::new();
        let s1 = make_status(42, "BTCUSDT", OrderWorkerStatus::BuySet, 1);
        let (res, ev) = orders.apply(order_status_cmd(s1));
        assert_eq!(res, ApplyResult::Applied);
        assert!(matches!(ev, OrderEvent::Created(42)));
        assert!(orders.get(42).is_some());

        let s2 = make_status(42, "BTCUSDT", OrderWorkerStatus::SelLDone, 1);
        let (_, ev) = orders.apply(order_status_cmd(s2));
        assert!(matches!(ev, OrderEvent::Updated(42)));
        assert!(orders.get(42).unwrap().job_is_done);

        let removed = orders.drain_pending_removals();
        assert_eq!(removed, vec![42]);
        assert!(orders.get(42).is_none());
    }

    #[test]
    fn from_cache_status_does_not_create_unknown_order_like_delphi() {
        let mut orders = Orders::new();
        let mut cached = make_status(42, "BTCUSDT", OrderWorkerStatus::BuySet, 1);
        cached.from_cache = true;

        let (res, ev) = orders.apply(order_status_cmd(cached));

        assert_eq!(res, ApplyResult::OrderNotFound);
        assert!(matches!(
            ev,
            OrderEvent::Ignored {
                uid: 42,
                reason: ApplyResult::OrderNotFound
            }
        ));
        assert!(orders.get(42).is_none());
    }

    #[test]
    fn incoming_set_immune_is_not_applied_by_process_command_order_like_delphi() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            42,
            "BTCUSDT",
            OrderWorkerStatus::BuySet,
            1,
        )));

        let set_immune = SetImmuneCommand {
            header: make_base(777, 3),
            items: vec![ImmuneItem {
                uid: 42,
                value: true,
            }],
        };
        let (res, ev) = orders.apply(TradeCommand::SetImmune(set_immune));

        assert_eq!(res, ApplyResult::NotApplicable);
        assert!(matches!(
            ev,
            OrderEvent::Ignored {
                uid: 777,
                reason: ApplyResult::NotApplicable
            }
        ));
        assert!(!orders.get(42).unwrap().immune_for_clicks);
    }

    #[test]
    fn outgoing_set_immune_clicks_mutates_only_found_active_orders_like_delphi() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            42,
            "BTCUSDT",
            OrderWorkerStatus::BuySet,
            1,
        )));
        orders.apply(order_status_cmd(make_status(
            43,
            "BTCUSDT",
            OrderWorkerStatus::SelLDone,
            1,
        )));

        let applied = orders.set_immune_clicks(&[
            ImmuneItem {
                uid: 42,
                value: true,
            },
            ImmuneItem {
                uid: 43,
                value: true,
            },
            ImmuneItem {
                uid: 44,
                value: true,
            },
        ]);

        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].uid, 42);
        assert!(orders.get(42).unwrap().immune_for_clicks);
        assert!(!orders.get(43).unwrap().immune_for_clicks);
        assert!(orders.get(44).is_none());
    }

    #[test]
    fn outgoing_send_stops_if_changed_matches_delphi_change_gate() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            42,
            "BTCUSDT",
            OrderWorkerStatus::BuySet,
            1,
        )));

        assert!(
            orders
                .send_stops_if_changed(404, &StopSettings::default())
                .is_none(),
            "Delphi exits when vOrder/local worker is absent"
        );
        assert!(
            orders
                .send_stops_if_changed(42, &StopSettings::default())
                .is_none(),
            "Delphi exits when Cur == FPrevStops"
        );

        let stops = StopSettings {
            stop_loss_on: 1,
            sl_level: 12.5,
            trailing_on: 1,
            trailing_level: -0.0,
            ..StopSettings::default()
        };
        assert!(
            orders.send_stops_if_changed(42, &stops).is_none(),
            "Delphi exits when worker.vOrder is nil even if stops changed"
        );
        assert!(orders.mark_local_visual_order(42));
        let (ctx, market, status, sent_stops) = orders
            .send_stops_if_changed(42, &stops)
            .expect("changed stops should be sent");

        assert_eq!(ctx.uid, 42);
        assert_eq!(ctx.currency, 1);
        assert_eq!(ctx.platform, 4);
        assert_eq!(market, "BTCUSDT");
        assert_eq!(status, OrderWorkerStatus::BuySet);
        assert_eq!(sent_stops, stops);
        assert_eq!(orders.get(42).unwrap().stops, stops);
        assert!(
            orders.send_stops_if_changed(42, &stops).is_none(),
            "FPrevStops was updated before sending"
        );

        let same_by_float_math = StopSettings {
            trailing_level: 0.0,
            ..stops
        };
        assert!(
            orders
                .send_stops_if_changed(42, &same_by_float_math)
                .is_some(),
            "Delphi TStopSettings.Equal is CompareMem, so -0.0 and +0.0 differ"
        );
    }

    #[test]
    fn local_visual_order_marker_can_be_registered_before_first_status() {
        let mut orders = Orders::new();
        assert!(
            !orders.mark_local_visual_order(42),
            "no read-model entry exists yet, marker is stored for the first status"
        );

        let mut cached = make_status(42, "BTCUSDT", OrderWorkerStatus::BuySet, 1);
        cached.from_cache = true;
        let (res, _) = orders.apply(order_status_cmd(cached));
        assert_eq!(res, ApplyResult::OrderNotFound);
        assert!(orders.get(42).is_none());

        orders.apply(order_status_cmd(make_status(
            42,
            "BTCUSDT",
            OrderWorkerStatus::BuySet,
            2,
        )));

        assert!(orders.get(42).unwrap().has_local_visual_order);
    }

    #[test]
    fn outgoing_send_vstop_if_changed_matches_delphi_change_gate() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            42,
            "BTCUSDT",
            OrderWorkerStatus::SellSet,
            1,
        )));

        assert!(
            orders
                .send_vstop_if_changed(404, false, false, 0.0, 0.0)
                .is_none(),
            "Delphi exits when vOrder/local worker is absent"
        );
        assert!(
            orders
                .send_vstop_if_changed(42, false, false, 0.0, 0.0)
                .is_none(),
            "Delphi exits when VStop fields equal FPrevVStop*"
        );

        assert!(
            orders
                .send_vstop_if_changed(42, true, false, 12.5, 100.0)
                .is_none(),
            "Delphi exits when worker.vOrder is nil even if VStop changed"
        );
        assert!(orders.mark_local_visual_order(42));
        let (ctx, market, params) = orders
            .send_vstop_if_changed(42, true, false, 12.5, 100.0)
            .expect("changed VStop should be sent");

        assert_eq!(ctx.uid, 42);
        assert_eq!(ctx.currency, 1);
        assert_eq!(ctx.platform, 4);
        assert_eq!(market, "BTCUSDT");
        assert_eq!(params.status, OrderWorkerStatus::SellSet);
        assert!(params.vstop_on);
        assert!(!params.vstop_fixed);
        assert_eq!(params.vstop_level, 12.5);
        assert_eq!(params.vstop_vol, 100.0);
        assert!(orders.get(42).unwrap().vstop_on);
        assert_eq!(orders.get(42).unwrap().vstop_level, 12.5);
        assert!(
            orders
                .send_vstop_if_changed(42, true, false, 12.5, 100.0)
                .is_none(),
            "FPrevVStop* was updated before sending"
        );
    }

    #[test]
    fn outgoing_send_replace_if_requested_matches_delphi_gate() {
        let mut orders = Orders::new();
        let mut buy_status = make_status(42, "BTCUSDT", OrderWorkerStatus::BuySet, 1);
        buy_status.buy_order.order_type = OrderType::Buy as u8;
        orders.apply(order_status_cmd(buy_status));

        assert!(
            orders.send_replace_if_requested(404, 10.0, 1000).is_none(),
            "Delphi exits when local worker is absent"
        );

        let (ctx, market, order_type, price) = orders
            .send_replace_if_requested(42, 10.5, 1000)
            .expect("first replace should be sent");
        assert_eq!(ctx.uid, 42);
        assert_eq!(ctx.currency, 1);
        assert_eq!(ctx.platform, 4);
        assert_eq!(market, "BTCUSDT");
        assert_eq!(order_type, OrderType::Buy);
        assert_eq!(price, 10.5);
        let order = orders.get(42).unwrap();
        assert_eq!(order.buy_price, 10.5);
        assert!(order.bulk_replace_buy);
        assert_eq!(order.replace_sent_time_ms, 1000);

        assert!(
            orders.send_replace_if_requested(42, 10.7, 1001).is_none(),
            "ReplaceSentTime gate suppresses another packet while replace is in flight"
        );
        assert_eq!(orders.get(42).unwrap().buy_price, 10.7);

        let mut pending = make_status(43, "BTCUSDT", OrderWorkerStatus::None, 1);
        pending.buy_order.mean_price = 9.0;
        orders.apply(order_status_cmd(pending));
        assert!(
            orders.send_replace_if_requested(43, 9.0, 1000).is_none(),
            "pending replace sends only when BuyCondPrice changes"
        );
        let (_, _, order_type, price) = orders
            .send_replace_if_requested(43, 9.1, 1000)
            .expect("changed pending price should send O_BUY replace");
        assert_eq!(order_type, OrderType::Buy);
        assert_eq!(price, 9.1);
        assert_eq!(orders.get(43).unwrap().pending_buy_cond_price, Some(9.1));
    }

    #[test]
    fn outgoing_send_cancel_if_requested_matches_delphi_gate() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            42,
            "BTCUSDT",
            OrderWorkerStatus::SellSet,
            1,
        )));
        orders.apply(order_status_cmd(make_status(
            43,
            "BTCUSDT",
            OrderWorkerStatus::SelLDone,
            1,
        )));

        assert!(orders.send_cancel_if_requested(404, 1000).is_none());
        assert!(orders.send_cancel_if_requested(43, 1000).is_none());

        let send = orders
            .send_cancel_if_requested(42, 1000)
            .expect("sell-set cancel should be sent");
        match send {
            OrderCancelSend::Cancel {
                ctx,
                market,
                status,
            } => {
                assert_eq!(ctx.uid, 42);
                assert_eq!(market, "BTCUSDT");
                assert_eq!(status, OrderWorkerStatus::SellSet);
            }
            other => panic!("unexpected cancel send: {other:?}"),
        }
        assert!(
            !orders.get(42).unwrap().cancel_request,
            "Delphi clears FOrder.CancelRequest after sending"
        );

        let mut pending = make_status(44, "BTCUSDT", OrderWorkerStatus::None, 1);
        pending.buy_order.mean_price = 9.25;
        orders.apply(order_status_cmd(pending));
        let send = orders
            .send_cancel_if_requested(44, 1000)
            .expect("pending cancel should be sent");
        match send {
            OrderCancelSend::PendingReplaceThenCancel { ctx, market, price } => {
                assert_eq!(ctx.uid, 44);
                assert_eq!(market, "BTCUSDT");
                assert_eq!(price, 9.25);
            }
            other => panic!("unexpected pending cancel send: {other:?}"),
        }
        assert!(
            orders.get(44).unwrap().pending_cancel,
            "Delphi leaves vOrder.PendingCancel set on the pending order"
        );
        assert!(
            orders.tick_pending_cancel_resends(1031).is_empty(),
            "Delphi worker loop sleeps 32 ms between pending cancel sends"
        );
        let sends = orders.tick_pending_cancel_resends(1032);
        assert_eq!(sends.len(), 1);
        match &sends[0] {
            OrderCancelSend::PendingReplaceThenCancel { ctx, market, price } => {
                assert_eq!(ctx.uid, 44);
                assert_eq!(market, "BTCUSDT");
                assert_eq!(*price, 9.25);
            }
            other => panic!("unexpected pending cancel resend: {other:?}"),
        }
    }

    #[test]
    fn outgoing_send_panic_sell_if_changed_matches_delphi_gate() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            42,
            "BTCUSDT",
            OrderWorkerStatus::SellSet,
            1,
        )));
        orders.apply(order_status_cmd(make_status(
            43,
            "BTCUSDT",
            OrderWorkerStatus::BuySet,
            1,
        )));

        assert!(
            orders.send_panic_sell_if_changed(43, true).is_none(),
            "Delphi sends panic-sell only from OS_SellSet workers"
        );
        assert!(
            orders.send_panic_sell_if_changed(42, false).is_none(),
            "initial PrevPanicSell=false suppresses redundant false"
        );

        let send = orders
            .send_panic_sell_if_changed(42, true)
            .expect("false -> true should be sent");
        assert_eq!(send.ctx.uid, 42);
        assert_eq!(send.market, "BTCUSDT");
        assert!(send.turn_on);
        assert!(orders.get(42).unwrap().panic_sell);

        assert!(
            orders.send_panic_sell_if_changed(42, true).is_none(),
            "PrevPanicSell was updated before sending"
        );
        let send = orders
            .send_panic_sell_if_changed(42, false)
            .expect("true -> false should be sent");
        assert!(!send.turn_on);
        assert!(!orders.get(42).unwrap().panic_sell);
    }

    #[test]
    fn outgoing_market_panic_sell_matches_delphi_workers_toggle() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            42,
            "BTCUSDT",
            OrderWorkerStatus::SellSet,
            1,
        )));
        orders.apply(order_status_cmd(make_status(
            43,
            "BTCUSDT",
            OrderWorkerStatus::SellSet,
            1,
        )));
        orders.apply(order_status_cmd(make_status(
            44,
            "ETHUSDT",
            OrderWorkerStatus::SellSet,
            1,
        )));
        orders.apply(order_status_cmd(make_status(
            45,
            "BTCUSDT",
            OrderWorkerStatus::BuySet,
            1,
        )));

        let sends = orders.turn_panic_sell_by_market("BTCUSDT", true);
        assert_eq!(sends.len(), 2);
        assert!(orders.get(42).unwrap().panic_sell);
        assert!(orders.get(43).unwrap().panic_sell);
        assert!(!orders.get(44).unwrap().panic_sell);
        assert!(!orders.get(45).unwrap().panic_sell);

        let (panic_sell_on, sends) = orders.switch_panic_sell_by_market("BTCUSDT", true);
        assert!(!panic_sell_on);
        assert_eq!(sends.len(), 2);
        assert!(sends.iter().all(|send| !send.turn_on));
        assert!(!orders.get(42).unwrap().panic_sell);
        assert!(!orders.get(43).unwrap().panic_sell);

        let (panic_sell_on, sends) = orders.switch_panic_sell_by_market("BTCUSDT", true);
        assert!(panic_sell_on);
        assert_eq!(sends.len(), 2);
        assert!(sends.iter().all(|send| send.turn_on));
    }

    #[test]
    fn incoming_turn_panic_sell_is_not_applied_by_process_command_order_like_delphi() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            42,
            "BTCUSDT",
            OrderWorkerStatus::SellSet,
            1,
        )));

        let turn = TurnPanicSellCommand {
            epoch_header: make_epoch(42, 3, "BTCUSDT", 2, OrderWorkerStatus::SellSet),
            turn_on: true,
        };
        let (res, ev) = orders.apply(TradeCommand::TurnPanicSell(turn));

        assert_eq!(res, ApplyResult::NotApplicable);
        assert!(matches!(
            ev,
            OrderEvent::Ignored {
                uid: 42,
                reason: ApplyResult::NotApplicable
            }
        ));
        assert_eq!(orders.get(42).unwrap().status, OrderWorkerStatus::SellSet);
    }

    #[test]
    fn incoming_noop_trade_epoch_still_updates_epoch_like_delphi_accept_server_command() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            42,
            "BTCUSDT",
            OrderWorkerStatus::SellSet,
            1,
        )));

        let turn = TurnPanicSellCommand {
            epoch_header: make_epoch(42, 3, "BTCUSDT", 2, OrderWorkerStatus::SellSet),
            turn_on: true,
        };
        let (res, _) = orders.apply(TradeCommand::TurnPanicSell(turn));
        assert_eq!(res, ApplyResult::NotApplicable);

        let stale_update = OrderStatusUpdate {
            epoch_header: make_epoch(42, 3, "BTCUSDT", 1, OrderWorkerStatus::SellSet),
            update_data: OrderUpdateData {
                actual_price: 123.0,
                ..OrderUpdateData::default()
            },
            sell_reason_code: 0,
        };
        let (res, _) = orders.apply(TradeCommand::OrderStatusUpdate(stale_update));

        assert_eq!(
            res,
            ApplyResult::OutOfOrder,
            "Delphi AcceptServerCommand updates FServerLatestEpoch even for no-op TTradeEpochCommand receive"
        );
        let sell_actual = orders.get(42).unwrap().sell_order.actual_price;
        assert_eq!(sell_actual, 0.0);
    }

    #[test]
    fn move_all_sells_candidate_gate_matches_delphi_active_client_overloads() {
        let mut orders = Orders::new();
        let mut immune_short = make_status(1, "BTCUSDT", OrderWorkerStatus::SellSet, 1);
        immune_short.is_short = true;
        immune_short.immune_for_clicks = true;
        orders.apply(order_status_cmd(immune_short));

        let move_kind = MoveAllSellsParams {
            cmd_type: MoveAllCmdType::MoveKind,
            move_kind: ReplaceMultiKind::TopVol,
            price: 10.0,
            price_zone: PriceZone::default(),
            side: FixedPosition::Short,
        };
        assert!(
            !orders.has_move_all_sells_candidate("BTCUSDT", move_kind),
            "MoveKind overload checks not ImmuneForClicks before wire send"
        );

        let pers = MoveAllSellsParams {
            cmd_type: MoveAllCmdType::Pers,
            ..move_kind
        };
        assert!(
            orders.has_move_all_sells_candidate("BTCUSDT", pers),
            "percent overload ignores ImmuneForClicks in Delphi"
        );

        let mut long = make_status(2, "BTCUSDT", OrderWorkerStatus::SellSet, 1);
        long.is_short = false;
        orders.apply(order_status_cmd(long));

        let price_zone = MoveAllSellsParams {
            cmd_type: MoveAllCmdType::PriceZone,
            side: FixedPosition::Short,
            ..move_kind
        };
        assert!(
            orders.has_move_all_sells_candidate("BTCUSDT", price_zone),
            "PriceZone active-client send gate ignores ASide and checks only market/status/non-immune"
        );

        let none = MoveAllSellsParams {
            move_kind: ReplaceMultiKind::None,
            ..move_kind
        };
        assert!(
            !orders.has_move_all_sells_candidate("BTCUSDT", none),
            "RM_None exits before sending"
        );
    }

    #[test]
    fn move_all_buys_candidate_gate_matches_delphi_active_client_overloads() {
        let mut orders = Orders::new();
        let mut immune_long = make_status(1, "BTCUSDT", OrderWorkerStatus::BuySet, 1);
        immune_long.is_short = false;
        immune_long.immune_for_clicks = true;
        orders.apply(order_status_cmd(immune_long));

        assert!(
            !orders.has_move_all_buys_candidate(
                "BTCUSDT",
                MoveAllBuysCmdType::MoveKind,
                ReplaceMultiKind::TopVol,
                FixedPosition::Long,
            ),
            "MoveKind overload checks not ImmuneForClicks before wire send"
        );
        assert!(
            orders.has_move_all_buys_candidate(
                "BTCUSDT",
                MoveAllBuysCmdType::Pers,
                ReplaceMultiKind::None,
                FixedPosition::Short,
            ),
            "percent overload checks only active market BuySet"
        );

        let mut short = make_status(2, "BTCUSDT", OrderWorkerStatus::BuySet, 1);
        short.is_short = true;
        orders.apply(order_status_cmd(short));

        assert!(
            orders.has_move_all_buys_candidate(
                "BTCUSDT",
                MoveAllBuysCmdType::MoveKind,
                ReplaceMultiKind::LastSet,
                FixedPosition::Short,
            ),
            "MoveKind gate honors ASide"
        );
        assert!(
            !orders.has_move_all_buys_candidate(
                "BTCUSDT",
                MoveAllBuysCmdType::MoveKind,
                ReplaceMultiKind::None,
                FixedPosition::Both,
            ),
            "RM_None exits before sending"
        );
    }

    #[test]
    fn sell_almost_done_is_terminal() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            42,
            "BTCUSDT",
            OrderWorkerStatus::SellSet,
            1,
        )));

        let s2 = make_status(42, "BTCUSDT", OrderWorkerStatus::SelLAlmostDone, 2);
        let (res, ev) = orders.apply(order_status_cmd(s2));
        assert_eq!(res, ApplyResult::Applied);
        assert!(matches!(ev, OrderEvent::Updated(42)));
        assert!(orders.get(42).unwrap().job_is_done);
        assert_eq!(orders.drain_pending_removals(), vec![42]);
        assert!(orders.get(42).is_none());
    }

    #[test]
    fn visual_trace_after_terminal_status_is_accepted_before_deferred_removal_like_delphi() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            42,
            "BTCUSDT",
            OrderWorkerStatus::SellSet,
            1,
        )));

        orders.apply(order_status_cmd(make_status(
            42,
            "BTCUSDT",
            OrderWorkerStatus::SelLDone,
            2,
        )));

        let trace = OrderTracePoint {
            market: make_market(42, 3, "BTCUSDT"),
            trace_time: 45_000.0,
            trace_price: 101.0,
            base_price: 100.0,
            stop_price: 0.0,
            ord_type: OrderType::Sell,
            flags: trace_flags::IS_FINISH,
        };
        let (res, ev) = orders.apply(TradeCommand::OrderTracePoint(trace));

        assert_eq!(res, ApplyResult::Applied);
        assert!(matches!(ev, OrderEvent::TracePoint { uid: 42 }));
        assert_eq!(orders.get(42).unwrap().trace_points.len(), 1);
        assert_eq!(orders.drain_pending_removals(), vec![42]);
        assert!(orders.get(42).is_none());
    }

    #[test]
    fn trace_points_are_not_capped_like_former_rust_ring_buffer() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            42,
            "BTCUSDT",
            OrderWorkerStatus::SellSet,
            1,
        )));

        for n in 0..300 {
            let trace = OrderTracePoint {
                market: make_market(42, 3, "BTCUSDT"),
                trace_time: 45_000.0 + n as f64,
                trace_price: 100.0 + n as f32,
                base_price: 100.0,
                stop_price: 0.0,
                ord_type: OrderType::Sell,
                flags: 0,
            };
            let (res, ev) = orders.apply(TradeCommand::OrderTracePoint(trace));
            assert_eq!(res, ApplyResult::Applied);
            assert!(matches!(ev, OrderEvent::TracePoint { uid: 42 }));
        }

        assert_eq!(orders.get(42).unwrap().trace_points.len(), 300);
    }

    #[test]
    fn trace_line_ignores_non_initial_without_existing_line_like_delphi() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            42,
            "BTCUSDT",
            OrderWorkerStatus::SellSet,
            1,
        )));

        let (res, ev) = orders.apply(TradeCommand::OrderTracePoint(trace_point(
            42,
            OrderType::Sell,
            0,
            45_000.0,
            101.0,
            100.0,
            0.0,
        )));

        assert_eq!(res, ApplyResult::Applied);
        assert!(matches!(ev, OrderEvent::TracePoint { uid: 42 }));
        let order = orders.get(42).unwrap();
        assert_eq!(order.trace_points.len(), 1);
        assert!(order.sell_trace_line.is_none());
    }

    #[test]
    fn trace_line_initial_temp_and_finish_match_delphi_order_line() {
        let mut orders = Orders::new();
        let mut status = make_status(42, "BTCUSDT", OrderWorkerStatus::SellSet, 1);
        status.sell_order.create_time = 44_000.0;
        status.sell_order.int_id = 77;
        orders.apply(order_status_cmd(status));

        orders.apply(TradeCommand::OrderTracePoint(trace_point(
            42,
            OrderType::Sell,
            trace_flags::IS_INITIAL,
            45_000.0,
            101.0,
            100.0,
            99.0,
        )));
        {
            let line = orders.get(42).unwrap().sell_trace_line.as_ref().unwrap();
            assert_eq!(line.order_type, OrderType::Sell);
            assert_eq!(line.order_id, 77);
            assert!(line.prevent_delete);
            assert_eq!(line.stop_price, Some(99.0));
            assert_eq!(
                line.points,
                vec![
                    OrderTraceChartPoint {
                        time: 44_000.0,
                        price: 100.0,
                    },
                    OrderTraceChartPoint {
                        time: 45_000.0,
                        price: 100.0,
                    },
                    OrderTraceChartPoint::default(),
                    OrderTraceChartPoint {
                        time: 45_000.0,
                        price: 101.0,
                    },
                ]
            );
            assert!(line.can_finish);
        }

        orders.apply(TradeCommand::OrderTracePoint(trace_point(
            42,
            OrderType::Sell,
            trace_flags::IS_TEMP,
            45_010.0,
            102.0,
            100.0,
            0.0,
        )));
        {
            let line = orders.get(42).unwrap().sell_trace_line.as_ref().unwrap();
            assert_eq!(
                line.tmp_point,
                Some(OrderTraceChartPoint {
                    time: 45_010.0,
                    price: 102.0,
                })
            );
            assert_eq!(line.points.len(), 4);
        }

        orders.apply(TradeCommand::OrderTracePoint(trace_point(
            42,
            OrderType::Sell,
            0,
            45_020.0,
            103.0,
            100.0,
            0.0,
        )));
        {
            let line = orders.get(42).unwrap().sell_trace_line.as_ref().unwrap();
            assert_eq!(
                &line.points[4..],
                &[
                    OrderTraceChartPoint {
                        time: 45_020.0,
                        price: 101.0,
                    },
                    OrderTraceChartPoint {
                        time: 45_010.0,
                        price: 102.0,
                    },
                    OrderTraceChartPoint {
                        time: 45_020.0,
                        price: 103.0,
                    },
                ]
            );
            assert!(line.can_finish);
        }

        orders.apply(TradeCommand::OrderTracePoint(trace_point(
            42,
            OrderType::Sell,
            trace_flags::IS_FINISH,
            45_030.0,
            104.0,
            100.0,
            0.0,
        )));
        let line = orders.get(42).unwrap().sell_trace_line.as_ref().unwrap();
        assert_eq!(line.points.last().unwrap().price, 104.0);
        assert!(!line.can_finish);
    }

    #[test]
    fn order_not_found_marks_server_forced_then_deferred_removal_like_delphi() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            42,
            "BTCUSDT",
            OrderWorkerStatus::BuySet,
            1,
        )));
        {
            let order = orders.map.get_mut(&42).unwrap();
            order.bulk_replace_buy = true;
            order.bulk_replace_sell = true;
            order.replace_sent_time_ms = 1000;
        }

        let not_found = make_epoch(42, 3, "BTCUSDT", 0, OrderWorkerStatus::None);
        let (res, ev) = orders.apply(TradeCommand::OrderNotFound(not_found));

        assert_eq!(res, ApplyResult::Applied);
        assert!(matches!(ev, OrderEvent::Updated(42)));
        let order = orders.get(42).unwrap();
        assert!(order.server_forced_remove);
        assert!(order.cancel_request);
        let buy_is_opened = order.buy_order.is_opened;
        let buy_canceled = order.buy_order.canceled;
        let buy_is_closed = order.buy_order.is_closed;
        let buy_close_time = order.buy_order.close_time;
        let sell_is_opened = order.sell_order.is_opened;
        let sell_canceled = order.sell_order.canceled;
        let sell_is_closed = order.sell_order.is_closed;
        let sell_close_time = order.sell_order.close_time;
        assert_eq!((buy_is_opened, buy_canceled, buy_is_closed), (0, 1, 1));
        assert_eq!((sell_is_opened, sell_canceled, sell_is_closed), (0, 1, 1));
        assert!(buy_close_time > 1.0);
        assert_eq!(buy_close_time, sell_close_time);
        assert!(!order.bulk_replace_buy);
        assert!(!order.bulk_replace_sell);
        assert!(
            !order.job_is_done,
            "Delphi TOrderNotFound sets CancellRequest, not JobIsDone, inside ProcessCommandOrder"
        );
        assert_eq!(orders.drain_pending_removals(), vec![42]);
        assert!(orders.get(42).is_none());
    }

    #[test]
    fn phase_rollback_rejected() {
        let mut orders = Orders::new();
        // SellSet (phase 3) → потом BuySet (phase 1) → rollback
        orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::SellSet,
            5,
        )));
        let (res, _) = orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::BuySet,
            6,
        )));
        assert_eq!(res, ApplyResult::PhaseRollback);
    }

    #[test]
    fn phase_rollback_not_applied_for_terminal() {
        // BuySet (phase 1) → BuyCancel (phase 0): NOT rollback потому что новая phase = 0
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::BuySet,
            5,
        )));
        let (res, _) = orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::BuyCancel,
            6,
        )));
        // BuyCancel terminal → marked for deferred removal.
        assert_eq!(res, ApplyResult::Applied);
        assert!(orders.get(1).unwrap().job_is_done);
        assert_eq!(orders.drain_pending_removals(), vec![1]);
    }

    #[test]
    fn epoch_out_of_order_rejected() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::BuySet,
            10,
        )));
        // Первый full status при создании worker'а в Delphi идёт через
        // OnMServerOrder -> HandleServerCommand и не заполняет
        // FServerLatestEpoch. Следующая команда этого status уже проходит
        // AcceptServerCommand и выставляет latest=10.
        let (res, _) = orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::BuySet,
            10,
        )));
        assert_eq!(res, ApplyResult::Applied);
        // epoch 5 после 10: backDist=10-5=5 <= 100 → stale
        let (res, _) = orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::BuySet,
            5,
        )));
        assert_eq!(res, ApplyResult::OutOfOrder);
    }

    #[test]
    fn epoch_duplicate_rejected() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::BuySet,
            10,
        )));
        let (res, _) = orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::BuySet,
            10,
        )));
        assert_eq!(res, ApplyResult::Applied);
        // Тот же epoch после AcceptServerCommand latest=10 — дубликат,
        // отвергается (Delphi EpochIsOK: LastEpoch=NewEpoch → false).
        let (res, _) = orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::BuySet,
            10,
        )));
        assert_eq!(res, ApplyResult::OutOfOrder);
    }

    #[test]
    fn first_same_epoch_after_new_order_is_accepted_like_delphi() {
        let mut orders = Orders::new();
        let mut status = make_status(1, "X", OrderWorkerStatus::BuySet, 10);
        status.buy_order.actual_price = 10.0;
        orders.apply(order_status_cmd(status));

        let same_epoch_update = OrderStatusUpdate {
            epoch_header: make_epoch(1, 3, "X", 10, OrderWorkerStatus::BuySet),
            update_data: OrderUpdateData {
                actual_price: 11.0,
                ..Default::default()
            },
            sell_reason_code: 0,
        };
        let (res, ev) = orders.apply(TradeCommand::OrderStatusUpdate(same_epoch_update));

        assert_eq!(
            res,
            ApplyResult::Applied,
            "Delphi first TOrderStatus bypasses AcceptServerCommand, so latest epoch is still zero"
        );
        assert!(matches!(ev, OrderEvent::Updated(1)));
        let actual = orders.get(1).unwrap().buy_order.actual_price;
        assert_eq!(actual, 11.0);
    }

    #[test]
    fn epoch_wrap_around_accepted() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::BuySet,
            65500,
        )));
        // wrap: 65500 → 200, backDist = 65500-200 = 65300 > 100 → accept (новое сообщение)
        let (res, _) = orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::BuySet,
            200,
        )));
        assert_eq!(res, ApplyResult::Applied);
    }

    #[test]
    fn replace_response_updates_epoch_slot() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::BuySet,
            10,
        )));

        let rr = OrderReplaceResponse {
            epoch_header: make_epoch(1, 3, "X", 11, OrderWorkerStatus::BuySet),
            order_type: OrderType::Buy,
            price: 123.0,
            update_data: OrderUpdateData::default(),
            quantity_base: 0.0,
        };

        let (res, _) = orders.apply(order_replace_response_cmd(rr.clone()));
        assert_eq!(res, ApplyResult::Applied);

        let (res, _) = orders.apply(order_replace_response_cmd(rr));
        assert_eq!(res, ApplyResult::OutOfOrder);
    }

    #[test]
    fn stops_update_uses_epoch_guard() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::BuySet,
            10,
        )));
        let (res, _) = orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::BuySet,
            10,
        )));
        assert_eq!(res, ApplyResult::Applied);

        let stops = StopSettings {
            stop_loss_on: 1,
            ..Default::default()
        };
        let stale = OrderStopsUpdate {
            epoch_header: make_epoch(1, 3, "X", 5, OrderWorkerStatus::BuySet),
            stops,
        };

        let (res, _) = orders.apply(TradeCommand::OrderStopsUpdate(stale));
        assert_eq!(res, ApplyResult::OutOfOrder);
        assert_eq!(orders.get(1).unwrap().stops.stop_loss_on, 0);
    }

    #[test]
    fn vstop_update_uses_phase_guard() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::SellSet,
            10,
        )));

        let rollback = VStopUpdate {
            epoch_header: make_epoch(1, 3, "X", 200, OrderWorkerStatus::BuySet),
            vstop_on: true,
            vstop_fixed: false,
            vstop_level: 42.0,
            vstop_vol: 1.0,
        };

        let (res, _) = orders.apply(TradeCommand::VStopUpdate(rollback));
        assert_eq!(res, ApplyResult::PhaseRollback);
        assert!(!orders.get(1).unwrap().vstop_on);
    }

    #[test]
    fn terminal_status_update_does_not_apply_update_data_like_delphi() {
        let mut orders = Orders::new();
        let mut status = make_status(1, "X", OrderWorkerStatus::SellSet, 10);
        status.sell_order.actual_price = 10.0;
        orders.apply(order_status_cmd(status));

        let terminal_update = OrderStatusUpdate {
            epoch_header: make_epoch(1, 3, "X", 11, OrderWorkerStatus::SelLDone),
            update_data: OrderUpdateData {
                actual_price: 999.0,
                mean_price: 999.0,
                quantity: 999.0,
                ..Default::default()
            },
            sell_reason_code: 14,
        };
        let (res, ev) = orders.apply(TradeCommand::OrderStatusUpdate(terminal_update));

        assert_eq!(res, ApplyResult::Applied);
        assert!(matches!(ev, OrderEvent::Updated(1)));
        let order = orders.get(1).unwrap();
        let sell_actual = order.sell_order.actual_price;
        let sell_mean = order.sell_order.mean_price;
        assert_eq!(sell_actual, 10.0);
        assert_eq!(sell_mean, 0.0);
        assert_eq!(order.sell_reason_code, 14);
        assert!(order.job_is_done);
    }

    #[test]
    fn sell_done_status_update_applies_set_done_flags_like_delphi() {
        let mut orders = Orders::new();
        let mut status = make_status(1, "X", OrderWorkerStatus::SellSet, 10);
        status.buy_order.is_opened = 1;
        status.buy_order.is_closed = 0;
        status.buy_order.canceled = 0;
        status.sell_order.is_opened = 1;
        status.sell_order.is_closed = 0;
        status.sell_order.canceled = 0;
        orders.apply(order_status_cmd(status));

        {
            let order = orders.map.get_mut(&1).unwrap();
            order.bulk_replace_buy = true;
            order.bulk_replace_sell = true;
        }

        let terminal_update = OrderStatusUpdate {
            epoch_header: make_epoch(1, 3, "X", 11, OrderWorkerStatus::SelLDone),
            update_data: Default::default(),
            sell_reason_code: 0,
        };
        let (res, _) = orders.apply(TradeCommand::OrderStatusUpdate(terminal_update));

        assert_eq!(res, ApplyResult::Applied);
        let order = orders.get(1).unwrap();
        assert_eq!(order.sell_order.is_opened, 0);
        assert_eq!(order.sell_order.is_closed, 1);
        assert_eq!(
            order.sell_order.canceled, 0,
            "SetDoneFlags does not mark sell side canceled"
        );
        assert_eq!(order.buy_order.is_opened, 0);
        assert_eq!(order.buy_order.is_closed, 0);
        assert_eq!(
            order.buy_order.canceled, 1,
            "SetDoneFlags cancels buy side only when it was not already closed"
        );
        assert!(!order.bulk_replace_buy);
        assert!(!order.bulk_replace_sell);
    }

    #[test]
    fn sell_done_full_status_applies_set_done_flags_like_delphi() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::SellSet,
            10,
        )));

        {
            let order = orders.map.get_mut(&1).unwrap();
            order.bulk_replace_buy = true;
            order.bulk_replace_sell = true;
        }

        let mut done = make_status(1, "X", OrderWorkerStatus::SelLDone, 11);
        done.buy_order.is_opened = 1;
        done.buy_order.is_closed = 1;
        done.buy_order.canceled = 0;
        done.sell_order.is_opened = 1;
        done.sell_order.is_closed = 0;
        done.sell_order.canceled = 0;
        let (res, _) = orders.apply(order_status_cmd(done));

        assert_eq!(res, ApplyResult::Applied);
        let order = orders.get(1).unwrap();
        assert_eq!(order.sell_order.is_opened, 0);
        assert_eq!(order.sell_order.is_closed, 1);
        assert_eq!(order.sell_order.canceled, 0);
        assert_eq!(order.buy_order.is_opened, 0);
        assert_eq!(
            order.buy_order.canceled, 0,
            "already closed buy side is not marked canceled by SetDoneFlags"
        );
        assert!(!order.bulk_replace_buy);
        assert!(!order.bulk_replace_sell);
    }

    #[test]
    fn zero_sell_reason_update_keeps_previous_reason_like_delphi() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::SellSet,
            10,
        )));

        let first_reason = OrderStatusUpdate {
            epoch_header: make_epoch(1, 3, "X", 11, OrderWorkerStatus::SellSet),
            update_data: Default::default(),
            sell_reason_code: 14,
        };
        orders.apply(TradeCommand::OrderStatusUpdate(first_reason));
        assert_eq!(orders.get(1).unwrap().sell_reason_code, 14);

        let zero_reason = OrderStatusUpdate {
            epoch_header: make_epoch(1, 3, "X", 12, OrderWorkerStatus::SellSet),
            update_data: Default::default(),
            sell_reason_code: 0,
        };
        orders.apply(TradeCommand::OrderStatusUpdate(zero_reason));
        assert_eq!(
            orders.get(1).unwrap().sell_reason_code,
            14,
            "Delphi ignores SellReasonCode=0 and keeps FPrevSellReasonCode/SellReason"
        );

        let changed_reason = OrderStatusUpdate {
            epoch_header: make_epoch(1, 3, "X", 13, OrderWorkerStatus::SellSet),
            update_data: Default::default(),
            sell_reason_code: 9,
        };
        orders.apply(TradeCommand::OrderStatusUpdate(changed_reason));
        assert_eq!(orders.get(1).unwrap().sell_reason_code, 9);
    }

    #[test]
    fn pending_status_update_tracks_vorder_buy_cond_price_like_delphi() {
        let mut orders = Orders::new();
        let mut status = make_status(1, "X", OrderWorkerStatus::None, 10);
        status.buy_order.mean_price = 10.0;
        orders.apply(order_status_cmd(status));

        assert_eq!(orders.get(1).unwrap().pending_buy_cond_price, Some(10.0));
        let initial_buy_mean = orders.get(1).unwrap().buy_order.mean_price;
        assert_eq!(initial_buy_mean, 10.0);

        let pending_update = OrderStatusUpdate {
            epoch_header: make_epoch(1, 3, "X", 11, OrderWorkerStatus::None),
            update_data: OrderUpdateData {
                mean_price: 11.0,
                actual_price: 999.0,
                ..Default::default()
            },
            sell_reason_code: 0,
        };
        let (res, ev) = orders.apply(TradeCommand::OrderStatusUpdate(pending_update));

        assert_eq!(res, ApplyResult::Applied);
        assert!(matches!(ev, OrderEvent::Updated(1)));
        let order = orders.get(1).unwrap();
        let buy_mean = order.buy_order.mean_price;
        let buy_actual = order.buy_order.actual_price;
        assert_eq!(order.pending_buy_cond_price, Some(11.0));
        assert_eq!(
            buy_mean, 10.0,
            "OS_None update changes vOrder.BuyCondPrice, not pBuyOrder"
        );
        assert_eq!(buy_actual, 0.0);

        let buy_set_update = OrderStatusUpdate {
            epoch_header: make_epoch(1, 3, "X", 12, OrderWorkerStatus::BuySet),
            update_data: OrderUpdateData {
                mean_price: 12.0,
                actual_price: 12.0,
                ..Default::default()
            },
            sell_reason_code: 0,
        };
        orders.apply(TradeCommand::OrderStatusUpdate(buy_set_update));
        assert_eq!(orders.get(1).unwrap().pending_buy_cond_price, None);
    }

    #[test]
    fn os_none_update_without_pending_vorder_does_not_create_pending_price_like_delphi() {
        let mut orders = Orders::new();
        let mut status = make_status(1, "X", OrderWorkerStatus::BuySet, 10);
        status.buy_order.mean_price = 10.0;
        orders.apply(order_status_cmd(status));

        let non_pending_none = OrderStatusUpdate {
            epoch_header: make_epoch(1, 3, "X", 11, OrderWorkerStatus::None),
            update_data: OrderUpdateData {
                mean_price: 77.0,
                actual_price: 88.0,
                ..Default::default()
            },
            sell_reason_code: 0,
        };
        let (res, ev) = orders.apply(TradeCommand::OrderStatusUpdate(non_pending_none));

        assert_eq!(res, ApplyResult::Applied);
        assert!(matches!(ev, OrderEvent::Updated(1)));
        let order = orders.get(1).unwrap();
        assert_eq!(order.status, OrderWorkerStatus::None);
        assert_eq!(
            order.pending_buy_cond_price, None,
            "Delphi changes vOrder.BuyCondPrice only when IsPending and vOrder exists"
        );
        let buy_mean_price = order.buy_order.mean_price;
        assert_eq!(
            buy_mean_price, 10.0,
            "OS_None update still must not ApplyTo(pBuyOrder)"
        );
    }

    #[test]
    fn full_os_none_status_for_existing_pending_keeps_vorder_price_like_delphi() {
        let mut orders = Orders::new();
        let mut pending = make_status(1, "X", OrderWorkerStatus::None, 10);
        pending.buy_order.mean_price = 10.0;
        orders.apply(order_status_cmd(pending));
        assert_eq!(orders.get(1).unwrap().pending_buy_cond_price, Some(10.0));

        let mut full_status = make_status(1, "X", OrderWorkerStatus::None, 11);
        full_status.buy_order.mean_price = 77.0;
        let (res, ev) = orders.apply(order_status_cmd(full_status));

        assert_eq!(res, ApplyResult::Applied);
        assert!(matches!(ev, OrderEvent::Updated(1)));
        let order = orders.get(1).unwrap();
        assert_eq!(
            order.pending_buy_cond_price,
            Some(10.0),
            "Delphi TOrderStatus does not copy BuyOrder.MeanPrice into existing vOrder.BuyCondPrice"
        );
        let buy_mean = order.buy_order.mean_price;
        assert_eq!(
            buy_mean, 77.0,
            "Delphi still applies Cmd.BuyOrder to pBuyOrder"
        );
    }

    #[test]
    fn full_os_none_status_for_existing_non_pending_does_not_create_vorder_like_delphi() {
        let mut orders = Orders::new();
        let mut status = make_status(1, "X", OrderWorkerStatus::BuySet, 10);
        status.buy_order.mean_price = 10.0;
        orders.apply(order_status_cmd(status));

        let mut full_none = make_status(1, "X", OrderWorkerStatus::None, 11);
        full_none.buy_order.mean_price = 88.0;
        let (res, ev) = orders.apply(order_status_cmd(full_none));

        assert_eq!(res, ApplyResult::Applied);
        assert!(matches!(ev, OrderEvent::Updated(1)));
        let order = orders.get(1).unwrap();
        assert_eq!(order.status, OrderWorkerStatus::None);
        assert_eq!(
            order.pending_buy_cond_price, None,
            "Delphi creates pending vOrder only on the new OnMServerOrder path"
        );
        let buy_mean = order.buy_order.mean_price;
        assert_eq!(buy_mean, 88.0);
    }

    #[test]
    fn corridor_update_marks_order_as_moon_shot_like_delphi() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::BuySet,
            10,
        )));
        assert!(!orders.get(1).unwrap().is_moon_shot);

        let (res, ev) = orders.apply(TradeCommand::CorridorUpdate(CorridorUpdate {
            market: make_market(1, 3, "X"),
            price_down: 10.5,
            price_up: 12.25,
        }));

        assert_eq!(res, ApplyResult::Applied);
        assert!(matches!(ev, OrderEvent::CorridorChanged(1)));
        let order = orders.get(1).unwrap();
        assert!(order.is_moon_shot);
        assert_eq!(order.corridor_price_down, 10.5);
        assert_eq!(order.corridor_price_up, 12.25);
    }

    #[test]
    fn replace_response_quantity_base_zero_preserves_existing_value_like_delphi() {
        let mut orders = Orders::new();
        let mut status = make_status(1, "X", OrderWorkerStatus::BuySet, 10);
        status.buy_order.quantity_base = 12.5;
        orders.apply(order_status_cmd(status));

        let rr = OrderReplaceResponse {
            epoch_header: make_epoch(1, 3, "X", 11, OrderWorkerStatus::BuySet),
            order_type: OrderType::Buy,
            price: 123.0,
            update_data: OrderUpdateData {
                actual_price: 123.0,
                ..Default::default()
            },
            quantity_base: 0.0,
        };
        let (res, ev) = orders.apply(order_replace_response_cmd(rr));

        assert_eq!(res, ApplyResult::Applied);
        assert!(matches!(ev, OrderEvent::Updated(1)));
        let quantity_base = orders.get(1).unwrap().buy_order.quantity_base;
        assert_eq!(quantity_base, 12.5);
        assert_eq!(orders.get(1).unwrap().buy_price, 123.0);
    }

    #[test]
    fn replace_response_buy_stop_uses_sell_side_like_delphi() {
        let mut orders = Orders::new();
        let mut status = make_status(1, "X", OrderWorkerStatus::BuySet, 10);
        status.buy_order.actual_price = 111.0;
        status.sell_order.actual_price = 222.0;
        orders.apply(order_status_cmd(status));

        let rr = OrderReplaceResponse {
            epoch_header: make_epoch(1, 3, "X", 11, OrderWorkerStatus::SellSet),
            order_type: OrderType::BuyStop,
            price: 456.0,
            update_data: OrderUpdateData {
                actual_price: 456.0,
                ..Default::default()
            },
            quantity_base: 7.5,
        };
        let (res, ev) = orders.apply(order_replace_response_cmd(rr));

        assert_eq!(res, ApplyResult::Applied);
        assert!(matches!(ev, OrderEvent::Updated(1)));
        let order = orders.get(1).unwrap();
        let buy_actual_price = order.buy_order.actual_price;
        let sell_actual_price = order.sell_order.actual_price;
        let sell_quantity_base = order.sell_order.quantity_base;
        assert_eq!(buy_actual_price, 111.0);
        assert_eq!(order.buy_price, 111.0);
        assert_eq!(sell_actual_price, 456.0);
        assert_eq!(sell_quantity_base, 7.5);
        assert_eq!(order.sell_price, 456.0);
    }

    #[test]
    fn bulk_replace_timeout_clears_flag_after_5000ms_like_delphi() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::BuySet,
            10,
        )));

        let notify = BulkReplaceNotify {
            market: make_market(0, 3, "X"),
            order_type: OrderType::Buy,
            uids: vec![1],
        };
        let (res, ev) = orders.apply_at(TradeCommand::BulkReplaceNotify(notify), 1000);
        assert_eq!(res, ApplyResult::Applied);
        assert!(matches!(ev, OrderEvent::BulkReplaced { .. }));
        assert!(orders.get(1).unwrap().bulk_replace_buy);

        assert!(orders.tick_bulk_replace_timeouts(6000).is_empty());
        assert!(orders.get(1).unwrap().bulk_replace_buy);

        let events = orders.tick_bulk_replace_timeouts(6001);
        assert!(matches!(events.as_slice(), [OrderEvent::Updated(1)]));
        assert!(!orders.get(1).unwrap().bulk_replace_buy);
    }

    #[test]
    fn replace_response_clears_flag_then_tick_clears_shared_sent_time_like_delphi() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::BuySet,
            10,
        )));

        assert!(orders.send_replace_if_requested(1, 123.0, 1000).is_some());
        assert!(orders.get(1).unwrap().bulk_replace_buy);
        assert_eq!(orders.get(1).unwrap().replace_sent_time_ms, 1000);

        let rr = OrderReplaceResponse {
            epoch_header: make_epoch(1, 3, "X", 11, OrderWorkerStatus::BuySet),
            order_type: OrderType::Buy,
            price: 123.0,
            update_data: OrderUpdateData::default(),
            quantity_base: 0.0,
        };
        let (res, _) = orders.apply(order_replace_response_cmd(rr));
        assert_eq!(res, ApplyResult::Applied);

        let order = orders.get(1).unwrap();
        assert!(!order.bulk_replace_buy);
        assert_eq!(
            order.replace_sent_time_ms, 1000,
            "Delphi TOrderReplaceResponse clears p*Order.OrderReplace, not ReplaceSentTime"
        );

        assert!(orders.tick_bulk_replace_timeouts(1001).is_empty());
        assert_eq!(
            orders.get(1).unwrap().replace_sent_time_ms,
            0,
            "Delphi CheckReplaceFlag clears ReplaceSentTime when current FOrder flag is false"
        );
    }

    #[test]
    fn bulk_replace_tick_checks_only_current_side_like_delphi_forder() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::BuySet,
            10,
        )));

        let notify = BulkReplaceNotify {
            market: make_market(0, 3, "X"),
            order_type: OrderType::BuyStop,
            uids: vec![1],
        };
        let (res, _) = orders.apply_at(TradeCommand::BulkReplaceNotify(notify), 1000);
        assert_eq!(res, ApplyResult::Applied);
        assert!(orders.get(1).unwrap().bulk_replace_sell);
        assert_eq!(orders.get(1).unwrap().replace_sent_time_ms, 1000);

        assert!(orders.tick_bulk_replace_timeouts(6001).is_empty());
        let order = orders.get(1).unwrap();
        assert!(order.bulk_replace_sell);
        assert_eq!(
            order.replace_sent_time_ms, 0,
            "Delphi current FOrder=buy clears only ReplaceSentTime; opposite side flag is untouched"
        );
    }

    #[test]
    fn bulk_replace_notify_reports_only_found_workers_like_delphi() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::BuySet,
            10,
        )));

        let notify = BulkReplaceNotify {
            market: make_market(0, 3, "X"),
            order_type: OrderType::Buy,
            uids: vec![1, 2],
        };
        let (res, ev) = orders.apply_at(TradeCommand::BulkReplaceNotify(notify), 1000);

        assert_eq!(res, ApplyResult::Applied);
        assert!(orders.get(1).unwrap().bulk_replace_buy);
        assert!(matches!(
            ev,
            OrderEvent::BulkReplaced {
                order_type: OrderType::Buy,
                uids
            } if uids == vec![1]
        ));

        let missing_notify = BulkReplaceNotify {
            market: make_market(0, 3, "X"),
            order_type: OrderType::Sell,
            uids: vec![2],
        };
        let (res, ev) = orders.apply_at(TradeCommand::BulkReplaceNotify(missing_notify), 1000);

        assert_eq!(res, ApplyResult::OrderNotFound);
        assert!(matches!(
            ev,
            OrderEvent::Ignored {
                uid: 0,
                reason: ApplyResult::OrderNotFound
            }
        ));
    }

    #[test]
    fn bulk_replace_notify_buy_stop_uses_sell_side_like_delphi() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::SellSet,
            10,
        )));

        let notify = BulkReplaceNotify {
            market: make_market(0, 3, "X"),
            order_type: OrderType::BuyStop,
            uids: vec![1],
        };
        let (res, ev) = orders.apply_at(TradeCommand::BulkReplaceNotify(notify), 1000);

        assert_eq!(res, ApplyResult::Applied);
        assert!(matches!(ev, OrderEvent::BulkReplaced { .. }));
        let order = orders.get(1).unwrap();
        assert!(!order.bulk_replace_buy);
        assert!(order.bulk_replace_sell);
    }

    #[test]
    fn order_status_maintains_local_price_fields_like_delphi() {
        let mut orders = Orders::new();
        let mut status = make_status(1, "X", OrderWorkerStatus::BuySet, 10);
        status.buy_order.actual_price = 10.0;
        status.sell_order.actual_price = 20.0;
        orders.apply(order_status_cmd(status));

        let order = orders.get(1).unwrap();
        assert_eq!(order.buy_price, 10.0);
        assert_eq!(order.sell_price, 20.0);

        let mut repeated = make_status(1, "X", OrderWorkerStatus::BuySet, 11);
        repeated.buy_order.actual_price = 11.0;
        repeated.sell_order.actual_price = 21.0;
        orders.apply(order_status_cmd(repeated));

        let order = orders.get(1).unwrap();
        assert_eq!(order.buy_price, 11.0);
        assert_eq!(order.sell_price, 21.0);
    }

    #[test]
    fn epoch_is_ok_unit() {
        // Delphi: backDist := last - new (Word wrapping); accept = backDist > 100.

        // duplicate
        assert!(!epoch_is_ok(10, 10));
        // stale близко: backDist = 100-50 = 50 <= 100 → reject.
        assert!(!epoch_is_ok(100, 50));
        // accept forward через wrap: backDist = 100-250 = 65386 > 100 → accept.
        assert!(epoch_is_ok(100, 250));
        // wrap-around forward далеко: last=65500, new=200. backDist = 65300 > 100 → accept.
        assert!(epoch_is_ok(65500, 200));
        // last=200, new=65500. backDist = 200-65500 (wrap) = 236 > 100 → accept.
        assert!(epoch_is_ok(200, 65500));
        // Ближний stale: last=10, new=65500. backDist = 10-65500 (wrap) = 46 <= 100 → reject.
        assert!(!epoch_is_ok(10, 65500));
        // Граница окна: backDist = 100 → НЕ accept (требуется СТРОГО > 100).
        assert!(!epoch_is_ok(500, 400));
        // На один больше границы → accept.
        assert!(epoch_is_ok(500, 399));
    }

    #[test]
    fn missing_after_snapshot_returns_old_orders_after_dispatcher_style_status_loop() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::BuySet,
            1,
        )));
        orders.apply(order_status_cmd(make_status(
            2,
            "Y",
            OrderWorkerStatus::BuySet,
            1,
        )));

        orders.begin_snapshot();
        orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::SellSet,
            2,
        )));

        let missing = orders.missing_after_snapshot();
        assert_eq!(missing, vec![2]);
    }

    #[test]
    fn existing_order_command_refreshes_snapshot_flag_before_epoch_guard_like_delphi() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::BuySet,
            10,
        )));
        let (res, _) = orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::BuySet,
            10,
        )));
        assert_eq!(res, ApplyResult::Applied);

        orders.begin_snapshot();
        let duplicate_update = OrderStatusUpdate {
            epoch_header: make_epoch(1, 3, "X", 10, OrderWorkerStatus::BuySet),
            update_data: OrderUpdateData::default(),
            sell_reason_code: 0,
        };
        let (res, _) = orders.apply(TradeCommand::OrderStatusUpdate(duplicate_update));

        assert_eq!(res, ApplyResult::OutOfOrder);
        assert!(
            orders.missing_after_snapshot().is_empty(),
            "Delphi sets Worker.SnapshotFlag before AcceptServerCommand can reject the command"
        );
    }

    #[test]
    fn bulk_replace_notify_does_not_refresh_snapshot_flag_like_delphi_special_branch() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::BuySet,
            10,
        )));

        orders.begin_snapshot();
        let notify = BulkReplaceNotify {
            market: make_market(0, 3, "X"),
            order_type: OrderType::Buy,
            uids: vec![1],
        };
        let (res, _) = orders.apply_at(TradeCommand::BulkReplaceNotify(notify), 1000);

        assert_eq!(res, ApplyResult::Applied);
        assert_eq!(
            orders.missing_after_snapshot(),
            vec![1],
            "Delphi TBulkReplaceNotify exits before the general WCache SnapshotFlag assignment"
        );
    }

    #[test]
    fn non_base_market_trade_command_does_not_refresh_snapshot_flag_like_delphi() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            1,
            "X",
            OrderWorkerStatus::BuySet,
            10,
        )));

        orders.begin_snapshot();
        let (res, _) = orders.apply(TradeCommand::AllStatusesRequest(make_base(1, 3)));

        assert_eq!(res, ApplyResult::NotApplicable);
        assert_eq!(
            orders.missing_after_snapshot(),
            vec![1],
            "Delphi ClientNewData calls ProcessCommandOrder only for TBaseMarketCommand descendants"
        );
    }

    #[test]
    fn missing_after_snapshot_keeps_terminal_entry_until_deferred_removal_like_delphi_wcache() {
        let mut orders = Orders::new();
        orders.apply_at(
            order_status_cmd(make_status(1, "X", OrderWorkerStatus::SellSet, 1)),
            1000,
        );
        orders.apply_at(
            order_status_cmd(make_status(1, "X", OrderWorkerStatus::SelLDone, 2)),
            1001,
        );
        assert!(orders.get(1).unwrap().job_is_done);

        orders.begin_snapshot();

        assert_eq!(
            orders.missing_after_snapshot(),
            vec![1],
            "Delphi virtual worker is still in WCache and not JobIsDone until DoTheJobVirtual returns"
        );
        assert_eq!(orders.drain_pending_removals_due(1401), vec![1]);
        assert!(orders.missing_after_snapshot().is_empty());
    }

    #[test]
    fn direct_all_statuses_is_not_hidden_batch_inside_process_command_order() {
        let mut orders = Orders::new();
        let snap = AllStatuses {
            header: make_base(0, 3),
            orders: vec![make_status(1, "X", OrderWorkerStatus::SellSet, 2)],
        };

        let (res, ev) = orders.apply(TradeCommand::AllStatuses(snap));

        assert_eq!(res, ApplyResult::NotApplicable);
        assert!(matches!(
            ev,
            OrderEvent::Ignored {
                uid: 0,
                reason: ApplyResult::NotApplicable
            }
        ));
        assert!(orders.is_empty());
        assert_eq!(orders.current_snapshot_flag(), 0);
    }

    #[test]
    fn accepts_more_than_former_rust_order_cap() {
        const FORMER_MAX_ORDERS: u64 = 50_000;
        let mut orders = Orders::new();
        for uid in 1..=FORMER_MAX_ORDERS + 1 {
            let (res, ev) = orders.apply(order_status_cmd(make_status(
                uid,
                "X",
                OrderWorkerStatus::BuySet,
                1,
            )));
            assert_eq!(res, ApplyResult::Applied);
            assert!(matches!(ev, OrderEvent::Created(id) if id == uid));
        }

        assert_eq!(orders.len(), (FORMER_MAX_ORDERS + 1) as usize);
        assert!(orders.get(FORMER_MAX_ORDERS + 1).is_some());
    }
}
