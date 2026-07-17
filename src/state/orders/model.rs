//! Order read-model storage.

use super::{OrderTraceLine, SellReason};
use crate::commands::market::{BaseCurrency, ExchangeCode};
use crate::commands::trade::*;

/// One retained order with core-equivalent state.
///
/// This is the UI/read-model object for one live or recently finished worker:
/// exchange legs, local replace/cancel/stops intents, trace lines, strategy
/// linkage, and cleanup flags are kept together so terminal code does not have
/// to stitch packet-shaped fragments by hand.
#[derive(Debug, Clone)]
pub struct Order {
    /// Unique order id = server task UID.
    pub uid: u64,
    /// Market name, for example `BTCUSDT`.
    pub market_name: String,
    /// Base currency copied from the order command market header.
    pub currency: BaseCurrency,
    /// Exchange/platform copied from the order command market header.
    pub platform: ExchangeCode,
    /// Current worker lifecycle status.
    pub status: OrderWorkerStatus,
    /// Exchange buy-side order leg.
    pub buy_order: ExchangeOrder,
    /// Exchange sell-side order leg.
    pub sell_order: ExchangeOrder,
    /// Desired/local buy replace price, not part of exchange-order wire data.
    pub buy_price: f64,
    /// Desired/local sell replace price, not part of exchange-order wire data.
    pub sell_price: f64,
    /// Stop settings.
    pub stops: StopSettings,
    /// VStop state.
    pub vstop_on: bool,
    pub vstop_fixed: bool,
    pub vstop_level: f64,
    pub vstop_vol: f64,
    /// Local outgoing panic-sell intent.
    pub panic_sell: bool,
    /// Moon-shot marker from corridor updates.
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
    /// Pending buy condition price for `OS_None` orders.
    pub pending_buy_cond_price: Option<f64>,
    /// Pending-cancel flag for `OS_None` orders.
    pub pending_cancel: bool,
    /// Side on which BulkReplace is currently marked.
    pub bulk_replace_buy: bool,
    pub bulk_replace_sell: bool,
    /// Buy-side order-line state built by server trace packets.
    pub buy_trace_line: Option<OrderTraceLine>,
    /// Sell-side order-line state built by server trace packets.
    pub sell_trace_line: Option<OrderTraceLine>,
    /// True when the order is terminal and awaits deferred removal.
    pub job_is_done: bool,
    /// Server requested worker cancellation.
    pub cancel_request: bool,
    /// Server-forced removal.
    pub server_forced_remove: bool,
    /// Last sell reason.
    pub sell_reason: SellReason,

    // --- Internal sync state ---
    /// Stable identity of this retained worker instance. A delayed terminal
    /// cleanup may remove only the instance that scheduled it, never a newer
    /// worker which reused the same server UID.
    pub(super) instance_id: u64,
    /// Hard-session token under which the server epoch watermarks were seen.
    pub(super) server_session_token: u64,
    /// Highest accepted epoch in the server worker command stream.
    pub(super) server_watermark: u16,
    /// Whether a full status has seeded `server_watermark` in this session.
    pub(super) server_baselined: bool,
    /// Per-side watermark for replace state (`Price` / `QuantityBase`).
    pub(super) replace_epoch_buy: u16,
    pub(super) replace_epoch_sell: u16,
    /// Per-side watermark for replace acknowledgements.
    pub(super) ack_epoch_buy: u16,
    pub(super) ack_epoch_sell: u16,
    pub(super) ack_seeded_buy: bool,
    pub(super) ack_seeded_sell: bool,
    /// Stops and VStop are independent monotonic streams. They intentionally
    /// do not participate in the shared lifecycle watermark.
    pub(super) stops_epoch: u16,
    pub(super) stops_seeded: bool,
    pub(super) vstop_epoch: u16,
    pub(super) vstop_seeded: bool,
    /// Latest full order-status snapshot marker.
    pub(crate) snapshot_flag: u8,
    pub(super) replace_sent_time_ms: i64,
    pub(super) pending_cancel_sent_ms: i64,
    pub(super) prev_panic_sell: bool,
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

    /// Create a new retained order from a full status row.
    pub(super) fn from_status(status_cmd: &OrderStatus, instance_id: u64) -> Self {
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
            // Full stops/VStop are applied through their own epoch judges after
            // the lifecycle full passes its independent gate.
            stops: StopSettings::default(),
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
            pending_buy_cond_price: None,
            pending_cancel: false,
            bulk_replace_buy: false,
            bulk_replace_sell: false,
            buy_trace_line: None,
            sell_trace_line: None,
            job_is_done: status_cmd.epoch_header.status.is_terminal(),
            cancel_request: false,
            server_forced_remove: false,
            sell_reason: SellReason::Unknown,
            instance_id,
            server_session_token: 0,
            server_watermark: 0,
            server_baselined: false,
            replace_epoch_buy: 0,
            replace_epoch_sell: 0,
            ack_epoch_buy: 0,
            ack_epoch_sell: 0,
            ack_seeded_buy: false,
            ack_seeded_sell: false,
            stops_epoch: 0,
            stops_seeded: false,
            vstop_epoch: 0,
            vstop_seeded: false,
            snapshot_flag: 0,
            replace_sent_time_ms: 0,
            pending_cancel_sent_ms: 0,
            prev_panic_sell: false,
        }
    }

    pub(super) fn reset_server_epochs(&mut self) {
        self.server_watermark = 0;
        self.server_baselined = false;
        self.replace_epoch_buy = 0;
        self.replace_epoch_sell = 0;
        self.ack_epoch_buy = 0;
        self.ack_epoch_sell = 0;
        self.ack_seeded_buy = false;
        self.ack_seeded_sell = false;
        self.stops_epoch = 0;
        self.stops_seeded = false;
        self.vstop_epoch = 0;
        self.vstop_seeded = false;
    }
}

impl From<&Order> for TradeCtx {
    fn from(order: &Order) -> Self {
        order.trade_ctx()
    }
}
