//! Public order projection values and the fixed stop-settings wire record.
//!
//! Canonical order images are decoded in `order_v2`; only `StopSettings` still
//! needs a private packed mirror because it is also an outbound command value.

use zerocopy::byteorder::little_endian::F64 as LeF64;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use super::enums::{OrderSubType, OrderType};
#[cfg(any(test, feature = "diagnostics"))]
use crate::time::DelphiTime;
use crate::MoonTime;

/// Packed protocol boolean byte.
///
/// `ms.Read(record, SizeOf(record))` preserves the raw byte. The wrapper keeps
/// that byte for protocol parity while giving UI/API code a named type instead
/// of naked `u8` flags.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct DelphiBool(u8);

impl DelphiBool {
    pub const FALSE: Self = Self(0);
    pub const TRUE: Self = Self(1);

    pub(crate) const fn from_byte(raw: u8) -> Self {
        Self(raw)
    }

    pub const fn from_bool(value: bool) -> Self {
        if value {
            Self::TRUE
        } else {
            Self::FALSE
        }
    }

    pub(crate) const fn to_byte(self) -> u8 {
        self.0
    }

    pub const fn get(self) -> bool {
        self.0 != 0
    }

    pub const fn is_true(self) -> bool {
        self.get()
    }

    pub const fn is_false(self) -> bool {
        !self.get()
    }
}

impl From<bool> for DelphiBool {
    fn from(value: bool) -> Self {
        Self::from_bool(value)
    }
}

impl From<DelphiBool> for bool {
    fn from(value: DelphiBool) -> Self {
        value.get()
    }
}

/// Inclusive price interval used by bulk sell moves.
#[derive(Debug, Clone, Copy, Default)]
pub struct PriceZone {
    pub min_p: f64,
    pub max_p: f64,
}

/// Exchange-side order leg materialized from canonical order sections.
#[derive(Debug, Clone, Copy, Default)]
pub struct ExchangeOrder {
    pub int_id: i64,
    pub quantity: f64,
    pub quantity_remaining: f64,
    pub total_btc: f64,
    pub spent_btc: f64,
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub open_time: f64,
    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) open_time: f64,
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub close_time: f64,
    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) close_time: f64,
    pub actual_price: f64,
    pub mean_price: f64,
    pub quantity_base: f64,
    pub actual_q: f64,
    pub tmp_btc: f64,
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub create_time: f64,
    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) create_time: f64,
    pub panic_sell_down: f32,
    pub order_type: OrderType,
    pub sub_type: OrderSubType,
    pub stop_flag: u8,
    pub partial_done: u8,
    pub leverage: u8,
    pub(crate) is_opened: DelphiBool,
    pub(crate) is_closed: DelphiBool,
    pub(crate) canceled: DelphiBool,
    pub(crate) is_short: DelphiBool,
}

impl ExchangeOrder {
    pub fn open_time(self) -> MoonTime {
        MoonTime::from_delphi_days(self.open_time).unwrap_or(MoonTime::ZERO)
    }

    pub fn close_time(self) -> MoonTime {
        MoonTime::from_delphi_days(self.close_time).unwrap_or(MoonTime::ZERO)
    }

    pub fn create_time(self) -> MoonTime {
        MoonTime::from_delphi_days(self.create_time).unwrap_or(MoonTime::ZERO)
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn open_time_delphi(self) -> DelphiTime {
        DelphiTime::from_days(self.open_time)
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn close_time_delphi(self) -> DelphiTime {
        DelphiTime::from_days(self.close_time)
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn create_time_delphi(self) -> DelphiTime {
        DelphiTime::from_days(self.create_time)
    }

    pub fn is_opened(self) -> bool {
        self.is_opened.get()
    }

    pub fn is_closed(self) -> bool {
        self.is_closed.get()
    }

    pub fn canceled(self) -> bool {
        self.canceled.get()
    }

    pub fn is_short(self) -> bool {
        self.is_short.get()
    }

    /// Apply `ServerTimeDelta = InitialTime - Now` to time fields.
    ///
    /// Only valid legacy day-count timestamps (`> 1`) are adjusted.
    pub fn adjust_time(&mut self, delta: f64) {
        if self.open_time > 1.0 {
            self.open_time -= delta;
        }
        if self.close_time > 1.0 {
            self.close_time -= delta;
        }
        if self.create_time > 1.0 {
            self.create_time -= delta;
        }
    }
}

/// Packed stop-settings record retained inside an order snapshot.
#[derive(Debug, Clone, Copy, Default)]
pub struct StopSettings {
    pub(crate) stop_loss_on: DelphiBool,
    pub(crate) sl_fixed: DelphiBool,
    pub(crate) sl_level: f64,
    pub(crate) sl_spread: f64,
    pub(crate) trailing_on: DelphiBool,
    pub(crate) trailing_fixed: DelphiBool,
    pub(crate) trailing_level: f64,
    pub(crate) ts_spread: f64,
    pub(crate) use_take_profit: DelphiBool,
    pub(crate) take_profit: f64,
    /// "Trader explicitly set the take-profit" latch. On the inbound order state
    /// this is the server's value; on outbound stops the runtime computes it (see
    /// `Orders::send_stops_if_changed`) so callers never set it by hand. The
    /// server auto-defaults TP on the SELL transition only while this is false
    /// on the server-side SELL transition.
    pub(crate) take_profit_changed: DelphiBool,
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
struct WireStopSettings {
    stop_loss_on: u8,
    sl_fixed: u8,
    sl_level: LeF64,
    sl_spread: LeF64,
    trailing_on: u8,
    trailing_fixed: u8,
    trailing_level: LeF64,
    ts_spread: LeF64,
    use_take_profit: u8,
    take_profit: LeF64,
    take_profit_changed: u8,
}

pub(crate) const STOP_SETTINGS_SIZE: usize = std::mem::size_of::<WireStopSettings>();
const _: [(); 46] = [(); STOP_SETTINGS_SIZE];

impl PartialEq for StopSettings {
    fn eq(&self, other: &Self) -> bool {
        self.stop_loss_on == other.stop_loss_on
            && self.sl_fixed == other.sl_fixed
            && self.sl_level.to_bits() == other.sl_level.to_bits()
            && self.sl_spread.to_bits() == other.sl_spread.to_bits()
            && self.trailing_on == other.trailing_on
            && self.trailing_fixed == other.trailing_fixed
            && self.trailing_level.to_bits() == other.trailing_level.to_bits()
            && self.ts_spread.to_bits() == other.ts_spread.to_bits()
            && self.use_take_profit == other.use_take_profit
            && self.take_profit.to_bits() == other.take_profit.to_bits()
            && self.take_profit_changed == other.take_profit_changed
    }
}

impl StopSettings {
    /// Empty/disabled stop settings.
    pub fn disabled() -> Self {
        Self::default()
    }

    /// Configure percentage stop-loss fields.
    pub fn with_stop_loss_percent(self, level: f64, spread: f64) -> Self {
        self.with_stop_loss_fields(true, false, level, spread)
    }

    /// Configure fixed-price stop-loss fields.
    pub fn with_stop_loss_fixed(self, level: f64, spread: f64) -> Self {
        self.with_stop_loss_fields(true, true, level, spread)
    }

    /// Disable stop-loss while preserving trailing/take-profit fields.
    pub fn without_stop_loss(self) -> Self {
        self.with_stop_loss_fields(false, false, 0.0, 0.0)
    }

    fn with_stop_loss_fields(
        mut self,
        enabled: bool,
        fixed: bool,
        level: f64,
        spread: f64,
    ) -> Self {
        self.stop_loss_on = DelphiBool::from_bool(enabled);
        self.sl_fixed = DelphiBool::from_bool(fixed);
        self.sl_level = level;
        self.sl_spread = spread;
        self
    }

    /// Configure percentage trailing-stop fields.
    pub fn with_trailing_percent(self, level: f64, spread: f64) -> Self {
        self.with_trailing_fields(true, false, level, spread)
    }

    /// Configure fixed-price trailing-stop fields.
    pub fn with_trailing_fixed(self, level: f64, spread: f64) -> Self {
        self.with_trailing_fields(true, true, level, spread)
    }

    /// Disable trailing stop while preserving stop-loss/take-profit fields.
    pub fn without_trailing(self) -> Self {
        self.with_trailing_fields(false, false, 0.0, 0.0)
    }

    fn with_trailing_fields(mut self, enabled: bool, fixed: bool, level: f64, spread: f64) -> Self {
        self.trailing_on = DelphiBool::from_bool(enabled);
        self.trailing_fixed = DelphiBool::from_bool(fixed);
        self.trailing_level = level;
        self.ts_spread = spread;
        self
    }

    /// Configure take-profit price.
    ///
    /// The outbound `take_profit_changed` latch is still computed by the
    /// runtime against the live order state before send, so callers set the
    /// desired TP value without hand-maintaining protocol latch bits.
    pub fn with_take_profit_price(self, take_profit: f64) -> Self {
        self.with_take_profit_fields(true, take_profit)
    }

    /// Disable take-profit while preserving stop-loss/trailing fields.
    pub fn without_take_profit(self) -> Self {
        self.with_take_profit_fields(false, 0.0)
    }

    fn with_take_profit_fields(mut self, enabled: bool, take_profit: f64) -> Self {
        self.use_take_profit = DelphiBool::from_bool(enabled);
        self.take_profit = take_profit;
        self
    }

    pub fn stop_loss_enabled(self) -> bool {
        self.stop_loss_on.get()
    }

    pub fn stop_loss_fixed(self) -> bool {
        self.sl_fixed.get()
    }

    pub fn stop_loss_level(self) -> f64 {
        self.sl_level
    }

    pub fn stop_loss_spread(self) -> f64 {
        self.sl_spread
    }

    pub fn trailing_enabled(self) -> bool {
        self.trailing_on.get()
    }

    pub fn trailing_fixed(self) -> bool {
        self.trailing_fixed.get()
    }

    pub fn trailing_level(self) -> f64 {
        self.trailing_level
    }

    pub fn trailing_spread(self) -> f64 {
        self.ts_spread
    }

    pub fn take_profit_enabled(self) -> bool {
        self.use_take_profit.get()
    }

    pub fn take_profit(self) -> f64 {
        self.take_profit
    }

    fn from_wire(wire: WireStopSettings) -> Self {
        Self {
            stop_loss_on: DelphiBool::from_byte(wire.stop_loss_on),
            sl_fixed: DelphiBool::from_byte(wire.sl_fixed),
            sl_level: wire.sl_level.get(),
            sl_spread: wire.sl_spread.get(),
            trailing_on: DelphiBool::from_byte(wire.trailing_on),
            trailing_fixed: DelphiBool::from_byte(wire.trailing_fixed),
            trailing_level: wire.trailing_level.get(),
            ts_spread: wire.ts_spread.get(),
            use_take_profit: DelphiBool::from_byte(wire.use_take_profit),
            take_profit: wire.take_profit.get(),
            take_profit_changed: DelphiBool::from_byte(wire.take_profit_changed),
        }
    }

    fn to_wire(self) -> WireStopSettings {
        WireStopSettings {
            stop_loss_on: self.stop_loss_on.to_byte(),
            sl_fixed: self.sl_fixed.to_byte(),
            sl_level: LeF64::new(self.sl_level),
            sl_spread: LeF64::new(self.sl_spread),
            trailing_on: self.trailing_on.to_byte(),
            trailing_fixed: self.trailing_fixed.to_byte(),
            trailing_level: LeF64::new(self.trailing_level),
            ts_spread: LeF64::new(self.ts_spread),
            use_take_profit: self.use_take_profit.to_byte(),
            take_profit: LeF64::new(self.take_profit),
            take_profit_changed: self.take_profit_changed.to_byte(),
        }
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < STOP_SETTINGS_SIZE {
            return None;
        }
        Some(Self::from_wire(
            WireStopSettings::read_from_bytes(&data[..STOP_SETTINGS_SIZE]).ok()?,
        ))
    }

    pub(super) fn read_from_delphi_stream(r: &mut &[u8]) -> Self {
        let bytes = read_zero_tail::<STOP_SETTINGS_SIZE>(r);
        let wire =
            WireStopSettings::read_from_bytes(&bytes).expect("fixed in-memory stop settings");
        Self::from_wire(wire)
    }

    pub(crate) fn write_to(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.to_wire().as_bytes());
    }
}

/// One explicit immune-clicks update intent.
#[derive(Debug, Clone, Copy)]
pub struct ImmuneItem {
    pub uid: u64,
    pub value: bool,
}

pub(super) fn read_zero_tail<const N: usize>(r: &mut &[u8]) -> [u8; N] {
    let mut out = [0u8; N];
    let n = r.len().min(N);
    if n > 0 {
        out[..n].copy_from_slice(&r[..n]);
        *r = &r[n..];
    }
    out
}

pub(super) fn read_u8_zero_tail(r: &mut &[u8]) -> u8 {
    read_zero_tail::<1>(r)[0]
}

pub(super) fn read_f32_zero_tail(r: &mut &[u8]) -> f32 {
    f32::from_le_bytes(read_zero_tail::<4>(r))
}

pub(super) fn read_f64_zero_tail(r: &mut &[u8]) -> f64 {
    f64::from_le_bytes(read_zero_tail::<8>(r))
}
