//! Order projection exposed to terminal code.

use super::{OrderTraceLine, SellReason};
use crate::commands::market::{BaseCurrency, ExchangeCode};
use crate::commands::trade::{
    CanonicalOrderState, DelphiBool, ExchangeOrder, OrderDescription, OrderSubType, OrderType,
    OrderWorkerStatus, StopSettings, OFL_IMMUNE, OFL_PANIC_AUTO, OFL_PANIC_ON,
    ORDER_SECTION_ALL_MASK, OSEC_BUY_EXEC, OSEC_BUY_PLACEMENT, OSEC_BUY_SLOW, OSEC_BUY_TARGET,
    OSEC_FLAGS, OSEC_PHASE, OSEC_PLANNED, OSEC_SELL_EXEC, OSEC_SELL_PLACEMENT, OSEC_SELL_SLOW,
    OSEC_SELL_TARGET, OSEC_STOPS, OSEC_VSTOP,
};
use crate::MoonTime;

/// One retained order materialized from the canonical OrdersProto state.
#[derive(Debug, Clone)]
pub struct Order {
    pub uid: u64,
    pub market_name: String,
    /// Session route metadata used for terminal grouping. Canonical order
    /// actions identify existing orders by `uid`.
    pub currency: BaseCurrency,
    pub platform: ExchangeCode,
    pub status: OrderWorkerStatus,
    pub buy_order: ExchangeOrder,
    pub sell_order: ExchangeOrder,
    /// Local draft/accepted buy target shown by the terminal.
    pub buy_price: f64,
    /// Accepted pending-order size.
    pub buy_size: f64,
    /// Local draft/accepted sell target shown by the terminal.
    pub sell_price: f64,
    pub stops: StopSettings,
    pub vstop_on: bool,
    pub vstop_fixed: bool,
    pub vstop_level: f64,
    pub vstop_vol: f64,
    pub panic_sell: bool,
    pub panic_sell_auto: bool,
    pub is_moon_shot: bool,
    pub corridor_price_down: f32,
    pub corridor_price_up: f32,
    pub strat_id: u64,
    pub is_short: bool,
    pub emulator_mode: bool,
    pub immune_for_clicks: bool,
    /// Pending-order trigger from the canonical buy-target section.
    ///
    /// This is populated only while `status == OrderWorkerStatus::None` and is
    /// cleared when the order enters an exchange phase.
    pub pending_buy_cond_price: Option<f64>,
    pub pending_cancel: bool,
    pub bulk_replace_buy: bool,
    pub bulk_replace_sell: bool,
    pub buy_trace_line: Option<OrderTraceLine>,
    pub sell_trace_line: Option<OrderTraceLine>,
    pub job_is_done: bool,
    pub sell_reason: SellReason,
    pub planned_sell_price: f64,
    pub use_market_stop: bool,

    pub(super) instance_id: u64,
    pub(super) replace_sent_time_ms: i64,
    pub(super) last_sent_target_is_buy: bool,
    pub(super) last_sent_target_price: f64,
    pub(super) last_sent_target_size: f64,
}

impl Order {
    pub(super) fn new(
        uid: u64,
        desc: &OrderDescription,
        instance_id: u64,
        currency: BaseCurrency,
        platform: ExchangeCode,
    ) -> Self {
        let leg_is_short = DelphiBool::from_bool(desc.is_short());
        Self {
            uid,
            market_name: desc.market_name(),
            currency,
            platform,
            status: OrderWorkerStatus::None,
            buy_order: ExchangeOrder {
                is_short: leg_is_short,
                ..ExchangeOrder::default()
            },
            sell_order: ExchangeOrder {
                is_short: leg_is_short,
                ..ExchangeOrder::default()
            },
            buy_price: 0.0,
            buy_size: 0.0,
            sell_price: 0.0,
            stops: StopSettings::default(),
            vstop_on: false,
            vstop_fixed: false,
            vstop_level: 0.0,
            vstop_vol: 0.0,
            panic_sell: false,
            panic_sell_auto: false,
            is_moon_shot: false,
            corridor_price_down: 0.0,
            corridor_price_up: 0.0,
            strat_id: 0,
            is_short: desc.is_short(),
            emulator_mode: desc.emulator(),
            immune_for_clicks: false,
            pending_buy_cond_price: None,
            pending_cancel: false,
            bulk_replace_buy: false,
            bulk_replace_sell: false,
            buy_trace_line: None,
            sell_trace_line: None,
            job_is_done: false,
            sell_reason: SellReason::Unknown,
            planned_sell_price: 0.0,
            use_market_stop: false,
            instance_id,
            replace_sent_time_ms: 0,
            last_sent_target_is_buy: true,
            last_sent_target_price: 0.0,
            last_sent_target_size: 0.0,
        }
    }

    pub(super) fn from_canonical(
        uid: u64,
        desc: &OrderDescription,
        instance_id: u64,
        currency: BaseCurrency,
        platform: ExchangeCode,
        state: &CanonicalOrderState,
    ) -> Self {
        let mut order = Self::new(uid, desc, instance_id, currency, platform);
        order.apply_canonical(state, ORDER_SECTION_ALL_MASK);

        // Initial mirror attachment seeds local line drafts after all canonical
        // sections are materialized. This is also required for a pending
        // Status=None image, where there is no phase transition to do the seed.
        order.buy_price = order.buy_order.actual_price;
        order.sell_price = order.sell_order.actual_price;
        order
    }

    /// Close reason as an enum for UI code.
    pub fn sell_reason(&self) -> SellReason {
        self.sell_reason
    }

    pub(super) fn apply_canonical(&mut self, state: &CanonicalOrderState, mask: u16) {
        if mask & (1 << OSEC_FLAGS) != 0 {
            let reason = state.read_u8(9);
            if reason != 0 {
                self.sell_reason = SellReason::from_byte(reason);
            }
            let flags = state.read_u8(10);
            self.immune_for_clicks = flags & OFL_IMMUNE != 0;
            self.panic_sell = flags & OFL_PANIC_ON != 0;
            self.panic_sell_auto = flags & OFL_PANIC_AUTO != 0;
        }

        if mask & (1 << OSEC_BUY_TARGET) != 0 {
            let price = state.read_f64(11);
            let size = state.read_f64(19);
            if self.status == OrderWorkerStatus::None {
                self.pending_buy_cond_price = Some(price);
            }
            if size > 0.0 {
                self.buy_size = size;
            }
            self.bulk_replace_buy = state.read_u8(27) != 0;
        }
        if mask & (1 << OSEC_SELL_TARGET) != 0 {
            self.bulk_replace_sell = state.read_u8(36) != 0;
        }

        if mask & (1 << OSEC_BUY_EXEC) != 0 {
            apply_exec(state, 37, &mut self.buy_order);
        }
        if mask & (1 << OSEC_BUY_PLACEMENT) != 0 {
            apply_placement(state, 70, &mut self.buy_order);
        }
        if mask & (1 << OSEC_BUY_SLOW) != 0 {
            apply_slow(state, 133, &mut self.buy_order);
        }
        if mask & (1 << OSEC_SELL_EXEC) != 0 {
            apply_exec(state, 153, &mut self.sell_order);
        }
        if mask & (1 << OSEC_SELL_PLACEMENT) != 0 {
            apply_placement(state, 186, &mut self.sell_order);
        }
        if mask & (1 << OSEC_SELL_SLOW) != 0 {
            apply_slow(state, 249, &mut self.sell_order);
        }

        if mask & (1 << OSEC_STOPS) != 0 {
            self.stops = state.stops();
        }
        if mask & (1 << OSEC_VSTOP) != 0 {
            self.vstop_on = state.read_u8(315) != 0;
            self.vstop_fixed = state.read_u8(316) != 0;
            self.vstop_level = state.read_f64(317);
            self.vstop_vol = state.read_f64(325);
        }
        if mask & (1 << OSEC_PLANNED) != 0 {
            self.planned_sell_price = state.read_f64(333);
            self.use_market_stop = state.read_u8(341) != 0;
        }

        if mask & (1 << OSEC_PHASE) != 0 {
            self.strat_id = state.read_u64(1);
            let status = OrderWorkerStatus::from_byte(state.read_u8(0));
            if status != self.status {
                self.status = status;
                self.buy_price = self.buy_order.actual_price;
                self.sell_price = self.sell_order.actual_price;
                self.replace_sent_time_ms = 0;
                self.pending_cancel = false;
                if status != OrderWorkerStatus::None {
                    self.pending_buy_cond_price = None;
                }
            }
            self.job_is_done = status.is_terminal();
        }

        self.confirm_target(state, mask);
    }

    fn confirm_target(&mut self, state: &CanonicalOrderState, mask: u16) {
        if self.replace_sent_time_ms == 0 {
            return;
        }
        let confirmed = if self.last_sent_target_is_buy && mask & (1 << OSEC_BUY_TARGET) != 0 {
            state.read_f64(11) == self.last_sent_target_price
                && state.read_f64(19) == self.last_sent_target_size
        } else if !self.last_sent_target_is_buy && mask & (1 << OSEC_SELL_TARGET) != 0 {
            state.read_f64(28) == self.last_sent_target_price
        } else {
            false
        };
        if confirmed {
            self.replace_sent_time_ms = 0;
        }
    }
}

fn apply_exec(state: &CanonicalOrderState, offset: usize, order: &mut ExchangeOrder) {
    order.quantity_remaining = state.read_f64(offset);
    order.actual_q = state.read_f64(offset + 8);
    order.total_btc = state.read_f64(offset + 16);
    order.mean_price = state.read_f64(offset + 24);
    order.partial_done = state.read_u8(offset + 32);
}

fn apply_placement(state: &CanonicalOrderState, offset: usize, order: &mut ExchangeOrder) {
    order.int_id = state.read_i64(offset);
    order.actual_price = state.read_f64(offset + 8);
    order.open_time = wire_time_days(state.read_i64(offset + 16));
    order.quantity = state.read_f64(offset + 24);
    order.quantity_base = state.read_f64(offset + 32);
    order.close_time = wire_time_days(state.read_i64(offset + 40));
    order.create_time = wire_time_days(state.read_i64(offset + 48));
    order.stop_flag = state.read_u8(offset + 56);
    order.order_type = OrderType::from_byte(state.read_u8(offset + 57));
    order.sub_type = OrderSubType::from_byte(state.read_u8(offset + 58));
    order.leverage = state.read_u8(offset + 59);
    order.is_opened = DelphiBool::from_byte(state.read_u8(offset + 60));
    order.is_closed = DelphiBool::from_byte(state.read_u8(offset + 61));
    order.canceled = DelphiBool::from_byte(state.read_u8(offset + 62));
}

fn apply_slow(state: &CanonicalOrderState, offset: usize, order: &mut ExchangeOrder) {
    order.spent_btc = state.read_f64(offset);
    order.tmp_btc = state.read_f64(offset + 8);
    order.panic_sell_down = state.read_f32(offset + 16);
}

fn wire_time_days(unix_ms: i64) -> f64 {
    if unix_ms == 0 {
        0.0
    } else {
        MoonTime::from_unix_millis(unix_ms).to_delphi_days()
    }
}
