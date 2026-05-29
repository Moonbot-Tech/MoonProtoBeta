//! Order read-model storage.

use super::{OrderTraceLine, SellReason};
use crate::commands::market::{BaseCurrency, ExchangeCode};
use crate::commands::trade::*;
use std::collections::VecDeque;

/// One retained order with Delphi worker-equivalent state.
///
/// Fields mirror `BOrderWorker` data received from the server through
/// `TOrderStatus`, `TOrderStatusUpdate`, `TOrderReplaceResponse`,
/// `TOrderStopsUpdate`, `TVStopUpdate`, `TCorridorUpdate`, and
/// `TOrderTracePoint`.
#[derive(Debug, Clone)]
pub struct Order {
    /// Unique order id = task UID (`MServerTag` in Delphi).
    pub uid: u64,
    /// Market name, for example `BTCUSDT`.
    pub market_name: String,
    /// Base currency copied from the order command market header.
    pub currency: BaseCurrency,
    /// Exchange/platform copied from the order command market header.
    pub platform: ExchangeCode,
    /// Current worker lifecycle status.
    pub status: OrderWorkerStatus,
    /// Exchange buy-side order.
    pub buy_order: OrderCompact,
    /// Exchange sell-side order.
    pub sell_order: OrderCompact,
    /// Delphi `pBuyOrder.Price`: desired/local replace price, not part of
    /// `TOrderCompact` wire data.
    pub buy_price: f64,
    /// Delphi `pSellOrder.Price`: desired/local replace price, not part of
    /// `TOrderCompact` wire data.
    pub sell_price: f64,
    /// Stop settings.
    pub stops: StopSettings,
    /// VStop state.
    pub vstop_on: bool,
    pub vstop_fixed: bool,
    pub vstop_level: f64,
    pub vstop_vol: f64,
    /// Delphi `BOrderWorker.FPanicSell`, local outgoing panic-sell intent.
    pub panic_sell: bool,
    /// Delphi `BOrderWorker.IsMoonShot`, raised by `TCorridorUpdate`.
    pub is_moon_shot: bool,
    /// Last corridor price range update.
    pub corridor_price_down: f32,
    pub corridor_price_up: f32,
    /// Strategy linkage.
    pub strat_id: u64,
    pub is_short: bool,
    pub db_id: i32,
    /// True when the order was restored from server cache after reconnect.
    pub from_cache: bool,
    /// True when the order runs in emulator mode.
    pub emulator_mode: bool,
    /// True when UI clicks must be ignored.
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
    /// Side on which BulkReplace is currently marked.
    pub bulk_replace_buy: bool,
    pub bulk_replace_sell: bool,
    /// Delphi `coBuy` order-line state built by `ApplyServerTrace`.
    pub buy_trace_line: Option<OrderTraceLine>,
    /// Delphi `coSell` order-line state built by `ApplyServerTrace`.
    pub sell_trace_line: Option<OrderTraceLine>,
    /// Raw trace points for server-decision visualization.
    ///
    /// This is the raw inbound packet log. For Delphi-equivalent chart state,
    /// use `buy_trace_line` / `sell_trace_line`.
    pub(crate) trace_points: VecDeque<OrderTracePoint>,
    /// True when the order is terminal and awaits deferred removal.
    pub job_is_done: bool,
    /// Delphi `CancellRequest`: server requested worker cancellation.
    pub cancel_request: bool,
    /// Server-forced removal (`TOrderNotFound` arrived).
    pub server_forced_remove: bool,
    /// Last sell reason.
    pub sell_reason: SellReason,

    // --- Internal sync state ---
    /// Per-status monotonic epoch used for anti out-of-order checks.
    pub(super) server_latest_epoch: [u16; 10],
    /// Snapshot flag updated by `TAllStatuses`.
    pub(crate) snapshot_flag: u8,
    pub(super) replace_sent_time_ms: i64,
    pub(super) pending_cancel_sent_ms: i64,
    pub(super) prev_panic_sell: bool,
    pub(super) last_buy_actual_price: f64,
    pub(super) last_sell_actual_price: f64,
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

    /// Close reason as an enum for UI code.
    pub fn sell_reason(&self) -> SellReason {
        self.sell_reason
    }

    /// Raw inbound trace packet log retained for diagnostics.
    ///
    /// Normal chart code should use [`Self::buy_trace_line`] /
    /// [`Self::sell_trace_line`], which mirror Delphi order-line state.
    #[doc(hidden)]
    pub fn trace_points(&self) -> &VecDeque<OrderTracePoint> {
        &self.trace_points
    }

    /// Create a new `Order` from `TOrderStatus`.
    pub(super) fn from_status(status_cmd: &OrderStatus) -> Self {
        Self {
            uid: status_cmd.epoch_header.market.base.uid,
            market_name: status_cmd.epoch_header.market.market_name.clone(),
            currency: BaseCurrency::from_byte(status_cmd.epoch_header.market.currency),
            platform: ExchangeCode::from_byte(status_cmd.epoch_header.market.platform),
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
            sell_reason: SellReason::Unknown,
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
