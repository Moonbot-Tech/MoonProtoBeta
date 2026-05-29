//! Order read-model and action/event types.

use crate::commands::trade::{OrderType, OrderWorkerStatus, TradeCtx};

/// Order close reason, matching Delphi `TSellReasonCode`
/// (MarketsU.pas:245-261).
///
/// The server may set this byte in `OrderStatusUpdate.sell_reason_code`.
/// Delphi updates the local sell reason only when the code is non-zero and
/// differs from the previous value. Unknown bytes are preserved like Delphi
/// enum storage and display as `Unknown`.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct SellReason(u8);

#[allow(non_upper_case_globals)]
impl SellReason {
    /// Unknown or unset.
    pub const Unknown: Self = Self(0);
    /// Sell at configured price.
    pub const SellPrice: Self = Self(1);
    /// Auto Price Down.
    pub const AutoPriceDown: Self = Self(2);
    /// Sell Level.
    pub const SellLevel: Self = Self(3);
    /// SellSpread.
    pub const SellSpread: Self = Self(4);
    /// SellShot.
    pub const SellShot: Self = Self(5);
    /// Global / Manual PanicSell.
    pub const PanicSell: Self = Self(6);
    /// StopLoss activated.
    pub const StopLoss: Self = Self(7);
    /// Trailing Stop fired.
    pub const Trailing: Self = Self(8);
    /// Market Stop.
    pub const MarketStop: Self = Self(9);
    /// Manual Sell (price < 95% of expected).
    pub const ManualSell: Self = Self(10);
    /// JoinedSell.
    pub const JoinedSell: Self = Self(11);
    /// SellFromAssets.
    pub const SellFromAssets: Self = Self(12);
    /// BV/SV Stop.
    pub const BvSvStop: Self = Self(13);
    /// TakeProfit reached.
    pub const TakeProfit: Self = Self(14);

    /// Preserve a raw Delphi reason byte.
    pub const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    /// Backward-compatible alias for callers that parse raw command bytes.
    pub const fn from_u8(b: u8) -> Self {
        Self::from_byte(b)
    }

    pub const fn to_byte(self) -> u8 {
        self.0
    }

    pub const fn is_known(self) -> bool {
        self.0 <= Self::TakeProfit.0
    }

    /// Human-readable UI label.
    pub const fn description(self) -> &'static str {
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
            _ => "Unknown",
        }
    }
}

impl Default for SellReason {
    fn default() -> Self {
        Self::Unknown
    }
}

impl std::fmt::Debug for SellReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_known() {
            f.write_str(self.description())
        } else {
            write!(f, "Unknown({})", self.0)
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

impl OrderTraceChartPoint {
    /// Point time as Delphi `TDateTime`.
    pub fn time_delphi(self) -> crate::DelphiTime {
        crate::DelphiTime::from_days(self.time)
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

/// Result of applying one order command.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ApplyResult {
    /// Command was applied and state changed.
    Applied,
    /// Command is stale for this status epoch.
    OutOfOrder,
    /// Command would roll the worker phase back.
    PhaseRollback,
    /// Order was not found in state.
    OrderNotFound,
    /// Command is not applicable to order state.
    NotApplicable,
}

/// Event produced by applying an order command.
#[derive(Debug, Clone)]
pub enum OrderEvent {
    /// A new order appeared.
    Created(u64),
    /// An existing order changed.
    Updated(u64),
    /// Order was removed after deferred terminal cleanup / `TOrderNotFound`.
    Removed(u64),
    /// Bulk replace notification.
    BulkReplaced {
        order_type: OrderType,
        uids: Vec<u64>,
    },
    /// Trace point was added.
    TracePoint { uid: u64 },
    /// Corridor state changed.
    CorridorChanged(u64),
    /// VStop state changed.
    VStopChanged(u64),
    /// Stop settings changed.
    StopsChanged(u64),
    /// `TAllStatuses` snapshot was applied.
    Snapshot,
    /// Command was ignored.
    Ignored { uid: u64, reason: ApplyResult },
}

impl OrderEvent {
    pub fn uid(&self) -> Option<u64> {
        match self {
            Self::Created(uid)
            | Self::Updated(uid)
            | Self::Removed(uid)
            | Self::TracePoint { uid }
            | Self::CorridorChanged(uid)
            | Self::VStopChanged(uid)
            | Self::StopsChanged(uid)
            | Self::Ignored { uid, .. } => Some(*uid),
            Self::BulkReplaced { .. } | Self::Snapshot => None,
        }
    }

    pub fn changed_uid(&self) -> Option<u64> {
        match self {
            Self::Created(uid)
            | Self::Updated(uid)
            | Self::TracePoint { uid }
            | Self::CorridorChanged(uid)
            | Self::VStopChanged(uid)
            | Self::StopsChanged(uid) => Some(*uid),
            _ => None,
        }
    }

    pub fn removed_uid(&self) -> Option<u64> {
        match self {
            Self::Removed(uid) => Some(*uid),
            _ => None,
        }
    }
}
