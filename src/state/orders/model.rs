//! Order read-model storage.

use super::{OrderTraceLine, SellReason};
use crate::commands::trade::*;
use std::collections::VecDeque;

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
    pub(super) server_latest_epoch: [u16; 10],
    /// Snapshot flag — обновляется при TAllStatuses.
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

    /// Причина закрытия как enum. Удобный getter для UI.
    /// См. [`SellReason`] для описания всех значений.
    pub fn sell_reason(&self) -> SellReason {
        SellReason::from_u8(self.sell_reason_code)
    }

    /// Создать новый Order из TOrderStatus.
    pub(super) fn from_status(status_cmd: &OrderStatus) -> Self {
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
