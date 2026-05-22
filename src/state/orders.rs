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
use std::collections::{HashMap, VecDeque};

const PRICE_EPS: f64 = 0.000000009;
const BULK_REPLACE_TIMEOUT_MS: i64 = 5000;

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
            Self::SellSpread => "Sell Spread",
            Self::SellShot => "Sell Shot",
            Self::PanicSell => "Panic Sell",
            Self::StopLoss => "Stop Loss",
            Self::Trailing => "Trailing Stop",
            Self::MarketStop => "Market Stop",
            Self::ManualSell => "Manual Sell",
            Self::JoinedSell => "Joined Sell",
            Self::SellFromAssets => "Sell From Assets",
            Self::BvSvStop => "BV/SV Stop",
            Self::TakeProfit => "Take Profit",
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
    /// Delphi `vOrder.BuyCondPrice` for pending `OS_None` orders.
    pub pending_buy_cond_price: Option<f64>,
    /// Тип ордера, на котором установлен BulkReplace.
    pub bulk_replace_buy: bool,
    pub bulk_replace_sell: bool,
    /// Trace points (визуализация решения сервера).
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
    bulk_replace_buy_sent_ms: i64,
    bulk_replace_sell_sent_ms: i64,
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
            corridor_price_down: 0.0,
            corridor_price_up: 0.0,
            strat_id: status_cmd.strat_id,
            is_short: status_cmd.is_short,
            db_id: status_cmd.db_id,
            from_cache: status_cmd.from_cache,
            emulator_mode: status_cmd.emulator_mode,
            immune_for_clicks: status_cmd.immune_for_clicks,
            pending_buy_cond_price: None,
            bulk_replace_buy: false,
            bulk_replace_sell: false,
            trace_points: VecDeque::new(),
            job_is_done: status_cmd.epoch_header.status.is_terminal(),
            cancel_request: false,
            server_forced_remove: false,
            sell_reason_code: 0,
            server_latest_epoch: [0; 10],
            snapshot_flag: 0,
            bulk_replace_buy_sent_ms: 0,
            bulk_replace_sell_sent_ms: 0,
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
    /// UID'ы, которые Delphi worker уже пометил бы как завершающиеся, но ещё
    /// не удалил бы из `WCache` прямо внутри `ProcessCommandOrder`.
    pending_removals: Vec<u64>,
    /// Инкрементируется при каждом TAllStatuses (CurrentSnapshotFlag в Delphi).
    current_snapshot_flag: u8,
    /// ServerTimeDelta = InitialTime(server) - Now(client). Применяется к временам в командах.
    pub server_time_delta: f64,
}

impl Orders {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
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
        match cmd {
            // --- Full status (создание или обновление) ---
            TradeCommand::OrderStatus(st) => {
                let new_order = !self.map.contains_key(&uid);
                if new_order && st.from_cache {
                    return (
                        ApplyResult::OrderNotFound,
                        OrderEvent::Ignored {
                            uid,
                            reason: ApplyResult::OrderNotFound,
                        },
                    );
                }
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

                    Self::apply_status_inner(entry, &st, self.server_time_delta);
                    entry.snapshot_flag = self.current_snapshot_flag;
                    entry.job_is_done
                };
                if is_done {
                    self.mark_pending_removal(uid);
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
                let is_terminal = up.epoch_header.status.is_terminal();
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
                        entry.pending_buy_cond_price = Some(up.update_data.mean_price);
                    } else {
                        entry.pending_buy_cond_price = None;
                    }
                    entry.status = up.epoch_header.status;
                    if up.sell_reason_code != 0 && up.sell_reason_code != entry.sell_reason_code {
                        entry.sell_reason_code = up.sell_reason_code;
                    }

                    if is_terminal {
                        entry.job_is_done = true;
                    }
                }

                if is_terminal {
                    self.mark_pending_removal(uid);
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
                if rr.quantity_base > 0.0 {
                    target.quantity_base = rr.quantity_base;
                }

                // Сбрасываем bulk_replace флаг на этой стороне (replace подтверждён).
                if rr.order_type == OrderType::Sell {
                    entry.sell_price = rr.price;
                    entry.bulk_replace_sell = false;
                    entry.bulk_replace_sell_sent_ms = 0;
                } else {
                    entry.buy_price = rr.price;
                    entry.bulk_replace_buy = false;
                    entry.bulk_replace_buy_sent_ms = 0;
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
                entry.trace_points.push_back(tp);
                (ApplyResult::Applied, OrderEvent::TracePoint { uid })
            }

            // --- Bulk replace notification ---
            TradeCommand::BulkReplaceNotify(brn) => {
                let mut affected = Vec::new();
                for &uid_replaced in &brn.uids {
                    if let Some(entry) = self.map.get_mut(&uid_replaced) {
                        if brn.order_type == OrderType::Sell {
                            entry.bulk_replace_sell = true;
                            entry.bulk_replace_sell_sent_ms = now_ms.max(1);
                        } else {
                            entry.bulk_replace_buy = true;
                            entry.bulk_replace_buy_sent_ms = now_ms.max(1);
                        }
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
                    self.mark_pending_removal(uid);
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
            TradeCommand::OrderReplace(_)
            | TradeCommand::OrderCancel(_)
            | TradeCommand::AllStatusesRequest(_)
            | TradeCommand::OrderStatusRequest(_)
            | TradeCommand::TurnPanicSell(_)
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
            | TradeCommand::BaseMarket(_)
            | TradeCommand::TradeEpoch(_) => (
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

    fn apply_status_inner(entry: &mut Order, st: &OrderStatus, server_time_delta: f64) {
        let mut buy = st.buy_order;
        let mut sell = st.sell_order;
        buy.adjust_time(server_time_delta);
        sell.adjust_time(server_time_delta);

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
        if st.epoch_header.status == OrderWorkerStatus::None {
            entry.pending_buy_cond_price = Some(entry.buy_order.mean_price);
        } else {
            entry.pending_buy_cond_price = None;
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
    }

    fn mark_pending_removal(&mut self, uid: u64) {
        if !self.pending_removals.contains(&uid) {
            self.pending_removals.push(uid);
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
        for uid in pending {
            if self.map.remove(&uid).is_some() {
                removed.push(uid);
            }
        }
        removed
    }

    /// Delphi `BOrderWorker.DoTheJobVirtual.CheckReplaceFlag` clears a pending
    /// replace flag when no replace response arrived for 5000 ms.
    pub(crate) fn tick_bulk_replace_timeouts(&mut self, now_ms: i64) -> Vec<OrderEvent> {
        let mut events = Vec::new();
        for entry in self.map.values_mut() {
            let mut changed = false;

            if entry.bulk_replace_buy
                && entry.bulk_replace_buy_sent_ms > 0
                && (now_ms - entry.bulk_replace_buy_sent_ms).abs() > BULK_REPLACE_TIMEOUT_MS
            {
                entry.bulk_replace_buy = false;
                entry.bulk_replace_buy_sent_ms = 0;
                changed = true;
            }

            if entry.bulk_replace_sell
                && entry.bulk_replace_sell_sent_ms > 0
                && (now_ms - entry.bulk_replace_sell_sent_ms).abs() > BULK_REPLACE_TIMEOUT_MS
            {
                entry.bulk_replace_sell = false;
                entry.bulk_replace_sell_sent_ms = 0;
                changed = true;
            }

            if changed {
                events.push(OrderEvent::Updated(entry.uid));
            }
        }
        events
    }

    /// После TAllStatuses найти ордера, которых **нет** в свежем snapshot.
    /// Эти UID'ы нужно явно запросить через `build_order_status_request`.
    /// Соответствует `MoonProtoClient.pas:637-666 CleanupMissingWorkers`.
    pub fn missing_after_snapshot(&self) -> Vec<u64> {
        let flag = self.current_snapshot_flag;
        self.map
            .values()
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
    fn order_not_found_marks_server_forced_then_deferred_removal_like_delphi() {
        let mut orders = Orders::new();
        orders.apply(order_status_cmd(make_status(
            42,
            "BTCUSDT",
            OrderWorkerStatus::BuySet,
            1,
        )));

        let not_found = make_epoch(42, 3, "BTCUSDT", 0, OrderWorkerStatus::None);
        let (res, ev) = orders.apply(TradeCommand::OrderNotFound(not_found));

        assert_eq!(res, ApplyResult::Applied);
        assert!(matches!(ev, OrderEvent::Updated(42)));
        let order = orders.get(42).unwrap();
        assert!(order.server_forced_remove);
        assert!(order.cancel_request);
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
