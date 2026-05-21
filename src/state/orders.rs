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
//! - Trace points accumulation (последние N точек).
//! - Corridor state.
//! - VStop state.
//! - Auto-removal на терминальном статусе.
//! - ServerTimeDelta correction для всех TDateTime полей.

use std::collections::{HashMap, VecDeque};
use crate::commands::trade::*;

/// Причина закрытия ордера. Соответствует Delphi `TSellReasonCode` (MarketsU.pas:245-261).
///
/// Сервер выставляет код в поле `sell_reason_code` каждого `OrderStatusUpdate`. Терминал
/// должен показывать пользователю причину закрытия (напр. "Stop Loss", "Take Profit",
/// "Panic Sell"). Используйте `SellReason::from_u8(order.sell_reason_code)` или
/// `Order::sell_reason()`.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SellReason {
    /// Неизвестная / не выставлена.
    Unknown        = 0,
    /// Продажа по установленной цене (дефолт).
    SellPrice      = 1,
    /// Auto Price Down — автоматический спуск цены.
    AutoPriceDown  = 2,
    /// Sell Level — продажа по уровню.
    SellLevel      = 3,
    /// SellSpread — продажа по спреду.
    SellSpread     = 4,
    /// SellShot — снайперская продажа.
    SellShot       = 5,
    /// Global / Manual PanicSell.
    PanicSell      = 6,
    /// StopLoss активирован.
    StopLoss       = 7,
    /// Trailing Stop сработал.
    Trailing       = 8,
    /// Market Stop.
    MarketStop     = 9,
    /// Manual Sell (price < 95% от ожидания).
    ManualSell     = 10,
    /// JoinedSell — объединённая продажа.
    JoinedSell     = 11,
    /// SellFromAssets — продажа из активов.
    SellFromAssets = 12,
    /// BV/SV Stop.
    BvSvStop       = 13,
    /// TakeProfit достигнут.
    TakeProfit     = 14,
}

impl SellReason {
    /// Преобразовать byte в enum. Неизвестные коды (>14) → `Unknown`.
    pub fn from_u8(b: u8) -> Self {
        match b {
            1  => Self::SellPrice,
            2  => Self::AutoPriceDown,
            3  => Self::SellLevel,
            4  => Self::SellSpread,
            5  => Self::SellShot,
            6  => Self::PanicSell,
            7  => Self::StopLoss,
            8  => Self::Trailing,
            9  => Self::MarketStop,
            10 => Self::ManualSell,
            11 => Self::JoinedSell,
            12 => Self::SellFromAssets,
            13 => Self::BvSvStop,
            14 => Self::TakeProfit,
            _  => Self::Unknown,
        }
    }

    /// Человекочитаемое название (для UI отображения).
    pub fn description(&self) -> &'static str {
        match self {
            Self::Unknown        => "Unknown",
            Self::SellPrice      => "Sell Price",
            Self::AutoPriceDown  => "Auto Price Down",
            Self::SellLevel      => "Sell Level",
            Self::SellSpread     => "Sell Spread",
            Self::SellShot       => "Sell Shot",
            Self::PanicSell      => "Panic Sell",
            Self::StopLoss       => "Stop Loss",
            Self::Trailing       => "Trailing Stop",
            Self::MarketStop     => "Market Stop",
            Self::ManualSell     => "Manual Sell",
            Self::JoinedSell     => "Joined Sell",
            Self::SellFromAssets => "Sell From Assets",
            Self::BvSvStop       => "BV/SV Stop",
            Self::TakeProfit     => "Take Profit",
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
    /// Настройки стопов.
    pub stops: StopSettings,
    /// VStop состояние.
    pub vstop_on: bool,
    pub vstop_fixed: bool,
    pub vstop_level: f64,
    pub vstop_vol: f64,
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
    /// True если включена паник-распродажа.
    pub panic_sell: bool,
    /// Тип ордера, на котором установлен BulkReplace.
    pub bulk_replace_buy: bool,
    pub bulk_replace_sell: bool,
    /// Trace points (визуализация решения сервера).
    /// Ring-buffer trace points (audit_rust_quality #5 + audit_robustness M5):
    /// `VecDeque` вместо `Vec` чтобы `pop_front()` был O(1) вместо O(N) при превышении лимита.
    /// При 100 ордерах × 10 trace_points/sec этот O(N) был 250K memmove ops/sec.
    pub trace_points: VecDeque<OrderTracePoint>,
    /// True если ордер терминален (закроется при очередном tick).
    pub job_is_done: bool,
    /// Server-forced removal (TOrderNotFound пришёл).
    pub server_forced_remove: bool,
    /// Reason code последней продажи.
    pub sell_reason_code: u8,

    // --- Internal sync state (не нужно потребителю) ---
    /// Per-status monotonic epoch (anti out-of-order). Размер по количеству статусов.
    server_latest_epoch: [u16; 10],
    /// Snapshot flag — обновляется при TAllStatuses.
    pub(crate) snapshot_flag: u8,
}

impl Order {
    /// Build the outgoing trade context for commands that target this tracked
    /// order.
    ///
    /// The context preserves the currency/platform bytes received from the
    /// server-side order state. This avoids hard-coding the current exchange
    /// configuration in consumers.
    pub fn trade_ctx(&self) -> TradeCtx {
        TradeCtx {
            uid: self.uid,
            currency: self.currency,
            platform: self.platform,
        }
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
            status: status_cmd.epoch_header.status,
            buy_order: status_cmd.buy_order,
            sell_order: status_cmd.sell_order,
            stops: status_cmd.stops,
            vstop_on: false,
            vstop_fixed: false,
            vstop_level: 0.0,
            vstop_vol: 0.0,
            corridor_price_down: 0.0,
            corridor_price_up: 0.0,
            strat_id: status_cmd.strat_id,
            is_short: status_cmd.is_short,
            db_id: status_cmd.db_id,
            from_cache: status_cmd.from_cache,
            emulator_mode: status_cmd.emulator_mode,
            immune_for_clicks: status_cmd.immune_for_clicks,
            panic_sell: false,
            bulk_replace_buy: false,
            bulk_replace_sell: false,
            trace_points: VecDeque::new(),
            job_is_done: status_cmd.epoch_header.status.is_terminal(),
            server_forced_remove: false,
            sell_reason_code: 0,
            server_latest_epoch: [0; 10],
            snapshot_flag: 0,
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
    /// Создание нового entry отвергнуто — `map.len() >= MAX_ORDERS` (DoS защита).
    /// Существующие entries обновляются без cap-check.
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
    /// Ордер удалён — terminal status, TOrderNotFound или server_forced_remove.
    Removed(u64),
    /// Bulk replace notification.
    BulkReplaced { order_type: OrderType, uids: Vec<u64> },
    /// Trace point добавлен.
    TracePoint { uid: u64 },
    /// Корридор обновлён.
    CorridorChanged(u64),
    /// VStop изменился.
    VStopChanged(u64),
    /// Стопы изменились.
    StopsChanged(u64),
    /// Panic sell изменился.
    PanicSellChanged(u64),
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

/// Верхний лимит количества активных ордеров в state. При попытке вставить
/// новый uid когда `map.len() >= MAX_ORDERS` — insert отвергается с warn-логом.
/// Защита от DoS / OOM в случае:
///   - враждебный/скомпрометированный сервер шлёт поток `TOrderStatus` с
///     уникальными uid'ами (35 MB/sec при 50K msg/sec);
///   - сервер забыл прислать терминальный статус и ордера накапливаются;
///   - очень долгая сессия с десятками тысяч ордеров.
/// Реальный бот держит сотни-единицы тысяч активных ордеров; 50_000 — щедрый
/// запас. См. `audit_robustness` C-1.
pub const MAX_ORDERS: usize = 50_000;

/// Главная коллекция ордеров.
///
/// **Однопоточная** — модифицируется только из main thread клиента.
/// Юзер получает read-only ссылки через `iter()`, `get()`.
#[derive(Debug, Default)]
pub struct Orders {
    map: HashMap<u64, Order>,
    /// Инкрементируется при каждом TAllStatuses (CurrentSnapshotFlag в Delphi).
    current_snapshot_flag: u8,
    /// ServerTimeDelta = InitialTime(server) - Now(client). Применяется к временам в командах.
    pub server_time_delta: f64,
    /// Max количество trace points на ордер (ring buffer).
    pub max_trace_points: usize,
}

impl Orders {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            current_snapshot_flag: 0,
            server_time_delta: 0.0,
            max_trace_points: 256,
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

    /// Применить команду из канала MPC_Order. Возвращает событие для UI/каллера.
    ///
    /// Это **главная** функция модуля. Внутри:
    /// 1. Проверка epoch (anti out-of-order).
    /// 2. Проверка phase rollback.
    /// 3. Применение к Order (или создание нового).
    /// 4. ServerTimeDelta correction для TDateTime полей.
    /// 5. Снятие job_is_done / удаление при terminal status / TOrderNotFound.
    /// 6. Snapshot flag mechanics (CleanupMissing).
    /// 7. Генерация события.
    ///
    /// **Замечание**: команды-запросы от клиента (AllStatusesRequest, OrderStatusRequest)
    /// возвращают `Ignored / NotApplicable` — это **исходящие** команды, не входящие.
    pub fn apply(&mut self, cmd: TradeCommand) -> (ApplyResult, OrderEvent) {
        let uid = cmd.uid();
        match cmd {
            // --- Snapshot ---
            TradeCommand::AllStatuses(snap) => {
                self.current_snapshot_flag = self.current_snapshot_flag.wrapping_add(1);
                let new_flag = self.current_snapshot_flag;
                for st in &snap.orders {
                    let order_uid = st.epoch_header.market.base.uid;
                    // DoS guard (audit_robustness C-1): cap при создании новых
                    // entries. Существующие uid'ы — update пропускаем без cap-check.
                    if !self.map.contains_key(&order_uid) && self.map.len() >= MAX_ORDERS {
                        log::warn!(target: "moonproto::orders",
                            "Orders.map at MAX_ORDERS ({MAX_ORDERS}) — rejecting snapshot uid={order_uid}");
                        continue;
                    }
                    let entry = self.map.entry(order_uid).or_insert_with(|| Order::from_status(st));
                    Self::apply_status_inner(entry, st, self.server_time_delta);
                    entry.snapshot_flag = new_flag;
                }
                (ApplyResult::Applied, OrderEvent::Snapshot)
            }

            // --- Full status (создание или обновление) ---
            TradeCommand::OrderStatus(st) => {
                if !st.from_cache {
                    // Inc snapshot flag — это новый ордер из live update'а.
                }
                let new_order = !self.map.contains_key(&uid);
                // DoS guard (audit_robustness C-1): cap новых ордеров. Update
                // existing — пропускаем без cap-check.
                if new_order && self.map.len() >= MAX_ORDERS {
                    log::warn!(target: "moonproto::orders",
                        "Orders.map at MAX_ORDERS ({MAX_ORDERS}) — rejecting new uid={uid}");
                    return (ApplyResult::Rejected, OrderEvent::Ignored { uid, reason: ApplyResult::Rejected });
                }
                let entry = self.map.entry(uid).or_insert_with(|| Order::from_status(&st));

                if let Err(reason) = Self::accept_epoch_and_phase(entry, &st.epoch_header) {
                    return (reason, OrderEvent::Ignored { uid, reason });
                }

                Self::apply_status_inner(entry, &st, self.server_time_delta);
                entry.snapshot_flag = self.current_snapshot_flag;

                if entry.job_is_done {
                    self.map.remove(&uid);
                    (ApplyResult::Applied, OrderEvent::Removed(uid))
                } else if new_order {
                    (ApplyResult::Applied, OrderEvent::Created(uid))
                } else {
                    (ApplyResult::Applied, OrderEvent::Updated(uid))
                }
            }

            // --- Delta-update ---
            TradeCommand::OrderStatusUpdate(up) => {
                let Some(entry) = self.map.get_mut(&uid) else {
                    return (ApplyResult::OrderNotFound, OrderEvent::Ignored { uid, reason: ApplyResult::OrderNotFound });
                };

                if let Err(reason) = Self::accept_epoch_and_phase(entry, &up.epoch_header) {
                    return (reason, OrderEvent::Ignored { uid, reason });
                }

                // Apply delta-update.
                let mut data = up.update_data;
                data.adjust_time(self.server_time_delta);

                // Routing по status — какой ордер обновлять.
                let target = if matches!(up.epoch_header.status, OrderWorkerStatus::SellSet | OrderWorkerStatus::SelLAlmostDone | OrderWorkerStatus::SelLDone | OrderWorkerStatus::SellCancel | OrderWorkerStatus::SellFail) {
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

                entry.status = up.epoch_header.status;
                entry.sell_reason_code = up.sell_reason_code;

                if up.epoch_header.status.is_terminal() {
                    entry.job_is_done = true;
                    self.map.remove(&uid);
                    return (ApplyResult::Applied, OrderEvent::Removed(uid));
                }

                (ApplyResult::Applied, OrderEvent::Updated(uid))
            }

            // --- Replace response ---
            TradeCommand::OrderReplaceResponse(rr) => {
                let Some(entry) = self.map.get_mut(&uid) else {
                    return (ApplyResult::OrderNotFound, OrderEvent::Ignored { uid, reason: ApplyResult::OrderNotFound });
                };

                if let Err(reason) = Self::accept_epoch_and_phase(entry, &rr.epoch_header) {
                    return (reason, OrderEvent::Ignored { uid, reason });
                }

                let mut data = rr.update_data;
                data.adjust_time(self.server_time_delta);

                let target = if rr.order_type == OrderType::Sell {
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
                target.quantity_base = rr.quantity_base;

                // Сбрасываем bulk_replace флаг на этой стороне (replace подтверждён).
                if rr.order_type == OrderType::Sell {
                    entry.bulk_replace_sell = false;
                } else {
                    entry.bulk_replace_buy = false;
                }

                (ApplyResult::Applied, OrderEvent::Updated(uid))
            }

            // --- Stops update ---
            TradeCommand::OrderStopsUpdate(su) => {
                let Some(entry) = self.map.get_mut(&uid) else {
                    return (ApplyResult::OrderNotFound, OrderEvent::Ignored { uid, reason: ApplyResult::OrderNotFound });
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
                    return (ApplyResult::OrderNotFound, OrderEvent::Ignored { uid, reason: ApplyResult::OrderNotFound });
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
                    return (ApplyResult::OrderNotFound, OrderEvent::Ignored { uid, reason: ApplyResult::OrderNotFound });
                };
                entry.corridor_price_down = cu.price_down;
                entry.corridor_price_up = cu.price_up;
                (ApplyResult::Applied, OrderEvent::CorridorChanged(uid))
            }

            // --- Trace point ---
            TradeCommand::OrderTracePoint(mut tp) => {
                let Some(entry) = self.map.get_mut(&uid) else {
                    return (ApplyResult::OrderNotFound, OrderEvent::Ignored { uid, reason: ApplyResult::OrderNotFound });
                };
                tp.adjust_time(self.server_time_delta);
                entry.trace_points.push_back(tp);
                if entry.trace_points.len() > self.max_trace_points {
                    entry.trace_points.pop_front();
                }
                (ApplyResult::Applied, OrderEvent::TracePoint { uid })
            }

            // --- Bulk replace notification ---
            TradeCommand::BulkReplaceNotify(brn) => {
                for &uid_replaced in &brn.uids {
                    if let Some(entry) = self.map.get_mut(&uid_replaced) {
                        if brn.order_type == OrderType::Sell {
                            entry.bulk_replace_sell = true;
                        } else {
                            entry.bulk_replace_buy = true;
                        }
                    }
                }
                (ApplyResult::Applied, OrderEvent::BulkReplaced { order_type: brn.order_type, uids: brn.uids.clone() })
            }

            // --- Order not found (server forced remove) ---
            TradeCommand::OrderNotFound(h) => {
                let uid = h.market.base.uid;
                if let Some(mut entry) = self.map.remove(&uid) {
                    entry.server_forced_remove = true;
                    entry.job_is_done = true;
                    (ApplyResult::Applied, OrderEvent::Removed(uid))
                } else {
                    (ApplyResult::OrderNotFound, OrderEvent::Ignored { uid, reason: ApplyResult::OrderNotFound })
                }
            }

            // --- Panic sell turn on/off ---
            TradeCommand::TurnPanicSell(ts) => {
                let Some(entry) = self.map.get_mut(&uid) else {
                    return (ApplyResult::OrderNotFound, OrderEvent::Ignored { uid, reason: ApplyResult::OrderNotFound });
                };
                if let Err(reason) = Self::accept_epoch_and_phase(entry, &ts.epoch_header) {
                    return (reason, OrderEvent::Ignored { uid, reason });
                }
                if entry.panic_sell != ts.turn_on {
                    entry.panic_sell = ts.turn_on;
                    return (ApplyResult::Applied, OrderEvent::PanicSellChanged(uid));
                }
                (ApplyResult::Applied, OrderEvent::Updated(uid))
            }

            // --- Set immune (массово) ---
            TradeCommand::SetImmune(si) => {
                for it in &si.items {
                    if let Some(entry) = self.map.get_mut(&it.uid) {
                        entry.immune_for_clicks = it.value;
                    }
                }
                (ApplyResult::Applied, OrderEvent::Updated(uid))
            }

            // --- Client-originated команды (исходящие) — игнорируются в state ---
            TradeCommand::OrderReplace(_)
            | TradeCommand::OrderCancel(_)
            | TradeCommand::AllStatusesRequest(_)
            | TradeCommand::OrderStatusRequest(_)
            | TradeCommand::JoinOrders(_)
            | TradeCommand::SplitOrder(_)
            | TradeCommand::MoveAllSells(_)
            | TradeCommand::MoveAllBuys(_)
            | TradeCommand::DoClosePosition(_)
            | TradeCommand::DoLimitClosePosition(_)
            | TradeCommand::DoSplitPosition(_)
            | TradeCommand::DoMarketSplitPosition(_)
            | TradeCommand::DoSellOrder(_)
            | TradeCommand::NewOrder(_) => {
                (ApplyResult::NotApplicable, OrderEvent::Ignored { uid, reason: ApplyResult::NotApplicable })
            }

            // --- Прочие ---
            TradeCommand::Penalty(_)
            | TradeCommand::TradeVisual(_)
            | TradeCommand::BaseMarket(_)
            | TradeCommand::TradeEpoch(_) => {
                (ApplyResult::NotApplicable, OrderEvent::Ignored { uid, reason: ApplyResult::NotApplicable })
            }

            TradeCommand::Unknown { uid, .. } => {
                (ApplyResult::NotApplicable, OrderEvent::Ignored { uid, reason: ApplyResult::NotApplicable })
            }
        }
    }

    fn accept_epoch_and_phase(entry: &mut Order, header: &TradeEpochHeader) -> Result<(), ApplyResult> {
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

    fn apply_status_inner(entry: &mut Order, st: &OrderStatus, server_time_delta: f64) {
        let mut buy = st.buy_order;
        let mut sell = st.sell_order;
        buy.adjust_time(server_time_delta);
        sell.adjust_time(server_time_delta);

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
    }

    /// После TAllStatuses найти ордера, которых **нет** в свежем snapshot.
    /// Эти UID'ы нужно явно запросить через `build_order_status_request`.
    /// Соответствует `MoonProtoClient.pas:637-666 CleanupMissingWorkers`.
    pub fn missing_after_snapshot(&self) -> Vec<u64> {
        let flag = self.current_snapshot_flag;
        self.map.values()
            .filter(|o| o.snapshot_flag != flag && !o.job_is_done)
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
        self.map.remove(&uid)
    }

    /// Очистить весь state (при reconnect / WantNewHello).
    pub fn clear(&mut self) {
        self.map.clear();
        self.current_snapshot_flag = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_base(uid: u64, ver: u16) -> BaseCommandHeader {
        BaseCommandHeader { cmd_id: 4, ver, uid }
    }

    fn make_market(uid: u64, ver: u16, market_name: &str) -> MarketCommandHeader {
        MarketCommandHeader {
            base: make_base(uid, ver),
            currency: 1,
            platform: 4,
            market_name: market_name.to_string(),
        }
    }

    fn make_epoch(uid: u64, ver: u16, market: &str, epoch: u16, status: OrderWorkerStatus) -> TradeEpochHeader {
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

    #[test]
    fn create_then_remove_on_terminal() {
        let mut orders = Orders::new();
        let s1 = make_status(42, "BTCUSDT", OrderWorkerStatus::BuySet, 1);
        let (res, ev) = orders.apply(TradeCommand::OrderStatus(s1));
        assert_eq!(res, ApplyResult::Applied);
        assert!(matches!(ev, OrderEvent::Created(42)));
        assert!(orders.get(42).is_some());

        let s2 = make_status(42, "BTCUSDT", OrderWorkerStatus::SelLDone, 1);
        let (_, ev) = orders.apply(TradeCommand::OrderStatus(s2));
        assert!(matches!(ev, OrderEvent::Removed(42)));
        assert!(orders.get(42).is_none());
    }

    #[test]
    fn sell_almost_done_is_terminal() {
        let mut orders = Orders::new();
        orders.apply(TradeCommand::OrderStatus(make_status(42, "BTCUSDT", OrderWorkerStatus::SellSet, 1)));

        let s2 = make_status(42, "BTCUSDT", OrderWorkerStatus::SelLAlmostDone, 2);
        let (res, ev) = orders.apply(TradeCommand::OrderStatus(s2));
        assert_eq!(res, ApplyResult::Applied);
        assert!(matches!(ev, OrderEvent::Removed(42)));
        assert!(orders.get(42).is_none());
    }

    #[test]
    fn phase_rollback_rejected() {
        let mut orders = Orders::new();
        // SellSet (phase 3) → потом BuySet (phase 1) → rollback
        orders.apply(TradeCommand::OrderStatus(make_status(1, "X", OrderWorkerStatus::SellSet, 5)));
        let (res, _) = orders.apply(TradeCommand::OrderStatus(make_status(1, "X", OrderWorkerStatus::BuySet, 6)));
        assert_eq!(res, ApplyResult::PhaseRollback);
    }

    #[test]
    fn phase_rollback_not_applied_for_terminal() {
        // BuySet (phase 1) → BuyCancel (phase 0): NOT rollback потому что новая phase = 0
        let mut orders = Orders::new();
        orders.apply(TradeCommand::OrderStatus(make_status(1, "X", OrderWorkerStatus::BuySet, 5)));
        let (res, _) = orders.apply(TradeCommand::OrderStatus(make_status(1, "X", OrderWorkerStatus::BuyCancel, 6)));
        // BuyCancel terminal → removed
        assert_eq!(res, ApplyResult::Applied);
    }

    #[test]
    fn epoch_out_of_order_rejected() {
        let mut orders = Orders::new();
        orders.apply(TradeCommand::OrderStatus(make_status(1, "X", OrderWorkerStatus::BuySet, 10)));
        // epoch 5 после 10: backDist=10-5=5 <= 100 → stale
        let (res, _) = orders.apply(TradeCommand::OrderStatus(make_status(1, "X", OrderWorkerStatus::BuySet, 5)));
        assert_eq!(res, ApplyResult::OutOfOrder);
    }

    #[test]
    fn epoch_duplicate_rejected() {
        let mut orders = Orders::new();
        orders.apply(TradeCommand::OrderStatus(make_status(1, "X", OrderWorkerStatus::BuySet, 10)));
        // Тот же epoch — дубликат, отвергается (Delphi EpochIsOK: LastEpoch=NewEpoch → false)
        let (res, _) = orders.apply(TradeCommand::OrderStatus(make_status(1, "X", OrderWorkerStatus::BuySet, 10)));
        assert_eq!(res, ApplyResult::OutOfOrder);
    }

    #[test]
    fn epoch_wrap_around_accepted() {
        let mut orders = Orders::new();
        orders.apply(TradeCommand::OrderStatus(make_status(1, "X", OrderWorkerStatus::BuySet, 65500)));
        // wrap: 65500 → 200, backDist = 65500-200 = 65300 > 100 → accept (новое сообщение)
        let (res, _) = orders.apply(TradeCommand::OrderStatus(make_status(1, "X", OrderWorkerStatus::BuySet, 200)));
        assert_eq!(res, ApplyResult::Applied);
    }

    #[test]
    fn replace_response_updates_epoch_slot() {
        let mut orders = Orders::new();
        orders.apply(TradeCommand::OrderStatus(make_status(1, "X", OrderWorkerStatus::BuySet, 10)));

        let rr = OrderReplaceResponse {
            epoch_header: make_epoch(1, 3, "X", 11, OrderWorkerStatus::BuySet),
            order_type: OrderType::Buy,
            price: 123.0,
            update_data: OrderUpdateData::default(),
            quantity_base: 0.0,
        };

        let (res, _) = orders.apply(TradeCommand::OrderReplaceResponse(rr.clone()));
        assert_eq!(res, ApplyResult::Applied);

        let (res, _) = orders.apply(TradeCommand::OrderReplaceResponse(rr));
        assert_eq!(res, ApplyResult::OutOfOrder);
    }

    #[test]
    fn stops_update_uses_epoch_guard() {
        let mut orders = Orders::new();
        orders.apply(TradeCommand::OrderStatus(make_status(1, "X", OrderWorkerStatus::BuySet, 10)));

        let mut stops = StopSettings::default();
        stops.stop_loss_on = 1;
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
        orders.apply(TradeCommand::OrderStatus(make_status(1, "X", OrderWorkerStatus::SellSet, 10)));

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
    fn epoch_is_ok_unit() {
        // Delphi: backDist := last - new (Word wrapping); accept = backDist > 100.

        // duplicate
        assert_eq!(epoch_is_ok(10, 10), false);
        // stale близко: backDist = 100-50 = 50 <= 100 → reject.
        assert_eq!(epoch_is_ok(100, 50), false);
        // accept forward через wrap: backDist = 100-250 = 65386 > 100 → accept.
        assert_eq!(epoch_is_ok(100, 250), true);
        // wrap-around forward далеко: last=65500, new=200. backDist = 65300 > 100 → accept.
        assert_eq!(epoch_is_ok(65500, 200), true);
        // last=200, new=65500. backDist = 200-65500 (wrap) = 236 > 100 → accept.
        assert_eq!(epoch_is_ok(200, 65500), true);
        // Ближний stale: last=10, new=65500. backDist = 10-65500 (wrap) = 46 <= 100 → reject.
        assert_eq!(epoch_is_ok(10, 65500), false);
        // Граница окна: backDist = 100 → НЕ accept (требуется СТРОГО > 100).
        assert_eq!(epoch_is_ok(500, 400), false);
        // На один больше границы → accept.
        assert_eq!(epoch_is_ok(500, 399), true);
    }

    #[test]
    fn missing_after_snapshot_returns_old_orders() {
        let mut orders = Orders::new();
        orders.apply(TradeCommand::OrderStatus(make_status(1, "X", OrderWorkerStatus::BuySet, 1)));
        orders.apply(TradeCommand::OrderStatus(make_status(2, "Y", OrderWorkerStatus::BuySet, 1)));

        let snap = AllStatuses {
            header: make_base(0, 3),
            orders: vec![make_status(1, "X", OrderWorkerStatus::SellSet, 2)],
        };
        orders.apply(TradeCommand::AllStatuses(snap));

        let missing = orders.missing_after_snapshot();
        assert_eq!(missing, vec![2]);
    }
}
