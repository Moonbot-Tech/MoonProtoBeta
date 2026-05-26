//! Order read-model and action/event types.

use crate::commands::trade::{OrderType, OrderWorkerStatus, TradeCtx};

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
    /// Преобразовать byte в enum. Неизвестные коды (>14) -> `Unknown`.
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

impl Default for OrderTraceChartPoint {
    fn default() -> Self {
        Self {
            time: 0.0,
            price: 0.0,
        }
    }
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
    pub(super) fn new(order_type: OrderType, order_id: i64) -> Self {
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

    pub(super) fn set_point_trade(
        &mut self,
        time: f64,
        price: f32,
        is_temp: bool,
        is_finish: bool,
    ) {
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
