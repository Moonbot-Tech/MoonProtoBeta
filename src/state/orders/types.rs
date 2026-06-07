//! Order read-model and action/event types.

use crate::commands::trade::{OrderType, OrderWorkerStatus, PositionFilter, TradeCtx};
#[cfg(any(test, feature = "diagnostics"))]
use crate::time::DelphiTime;
use crate::MoonTime;

/// Order close reason byte used by the MoonBot order stream.
///
/// The server may set this byte in `OrderStatusUpdate.sell_reason_code`.
/// Active Lib updates the local sell reason only when the code is non-zero and
/// differs from the previous value. Unknown bytes are preserved and display as
/// `Unknown`.
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

    /// Preserve a raw reason byte while applying inbound order status.
    pub(crate) const fn from_byte(b: u8) -> Self {
        Self(b)
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

/// One side of the chart "unprotected position" calculation.
///
/// `difference = position_size - closing_sell_quantity`: positive means the
/// position is not fully covered by active sell-close orders; negative means
/// sell-close orders exceed the current position. UI can highlight either
/// mismatch.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PositionProtectionSide {
    pub side: PositionFilter,
    pub position_size: f64,
    pub closing_sell_quantity: f64,
    pub difference: f64,
    pub missing_quantity: f64,
    pub has_warning: bool,
}

impl Default for PositionProtectionSide {
    fn default() -> Self {
        Self {
            side: PositionFilter::Both,
            position_size: 0.0,
            closing_sell_quantity: 0.0,
            difference: 0.0,
            missing_quantity: 0.0,
            has_warning: false,
        }
    }
}

/// Chart position-protection snapshot for one market.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct MarketPositionProtection {
    pub both: PositionProtectionSide,
    pub long: PositionProtectionSide,
    pub short: PositionProtectionSide,
}

/// One chart point in an order trace line.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OrderTraceChartPoint {
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub time: f64,
    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) time: f64,
    pub price: f32,
}

impl OrderTraceChartPoint {
    pub fn time(self) -> MoonTime {
        MoonTime::from_delphi_days(self.time).unwrap_or(MoonTime::ZERO)
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn time_delphi(self) -> DelphiTime {
        DelphiTime::from_days(self.time)
    }

    pub fn unix_millis(self) -> i64 {
        self.time().unix_millis()
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

/// Read-model counterpart of buy/sell order trace lines.
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

    /// Number of order-line segments represented by `points`.
    ///
    /// The wire chart line stores one anchor point and then three chart points
    /// per segment; shrinking uses the same `(Count - 1) / 3` formula.
    pub fn line_count(&self) -> usize {
        self.points.len().saturating_sub(1) / 3
    }

    pub(crate) fn needs_shrink(&self, to_count: usize) -> bool {
        self.points.len() >= 4 && (self.line_count() as f64) > (to_count as f64) * 1.2
    }

    pub(crate) fn shrink_points(&mut self, to_count: usize) -> usize {
        if !self.needs_shrink(to_count) {
            return 0;
        }

        let remove_lines = self.line_count().saturating_sub(to_count);
        let remove_points = 3 * remove_lines;
        self.points.drain(0..remove_points);
        remove_lines
    }
}

/// Result of applying one order command.
#[allow(unreachable_pub)]
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
    /// Command was ignored by the low-level order state machine.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
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
            | Self::StopsChanged(uid) => Some(*uid),
            #[cfg(any(test, feature = "diagnostics"))]
            Self::Ignored { uid, .. } => Some(*uid),
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
