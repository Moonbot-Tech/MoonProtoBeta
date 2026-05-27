//! Trade visual/trace command payloads.

use super::*;

/// Trace flags (TradeStruct.pas:234): bit0=IsTemp, bit1=IsFinish, bit2=IsInitial.
pub mod trace_flags {
    pub const IS_TEMP: u8 = 0x01;
    pub const IS_FINISH: u8 = 0x02;
    pub const IS_INITIAL: u8 = 0x04;
}

/// `TOrderTracePoint` (TradeStruct.pas:237-252).
#[derive(Debug, Clone)]
pub struct OrderTracePoint {
    pub market: MarketCommandHeader,
    pub trace_time: f64, // TDateTime
    pub trace_price: f32,
    pub base_price: f32,
    pub stop_price: f32,
    pub ord_type: OrderType,
    pub flags: u8,
}

impl OrderTracePoint {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        let market = MarketCommandHeader::read(r)?;
        let trace_time = read_f64_zero_tail(r);
        let trace_price = read_f32_zero_tail(r);
        let base_price = read_f32_zero_tail(r);
        let stop_price = read_f32_zero_tail(r);
        let ord_type = OrderType::from_byte(read_u8_zero_tail(r));
        let flags = read_u8_zero_tail(r);
        Some(Self {
            market,
            trace_time,
            trace_price,
            base_price,
            stop_price,
            ord_type,
            flags,
        })
    }

    pub fn is_temp(&self) -> bool {
        (self.flags & trace_flags::IS_TEMP) != 0
    }
    pub fn is_finish(&self) -> bool {
        (self.flags & trace_flags::IS_FINISH) != 0
    }
    pub fn is_initial(&self) -> bool {
        (self.flags & trace_flags::IS_INITIAL) != 0
    }

    /// Trace time as Delphi `TDateTime`.
    pub fn trace_time_delphi(&self) -> crate::DelphiTime {
        crate::DelphiTime::from_days(self.trace_time)
    }

    pub fn adjust_time(&mut self, delta: f64) {
        self.trace_time -= delta;
    }
}

/// `TCorridorUpdate` (TradeStruct.pas:255-262). Priority=Low.
#[derive(Debug, Clone)]
pub struct CorridorUpdate {
    pub market: MarketCommandHeader,
    pub price_down: f32,
    pub price_up: f32,
}

impl CorridorUpdate {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        let market = MarketCommandHeader::read(r)?;
        let price_down = read_f32_zero_tail(r);
        let price_up = read_f32_zero_tail(r);
        Some(Self {
            market,
            price_down,
            price_up,
        })
    }
}

/// `TBulkReplaceNotify` (TradeStruct.pas:275-284).
///
/// Notification that these UIDs are being bulk-replaced; UI should show them
/// as moving/in-flight.
#[derive(Debug, Clone)]
pub struct BulkReplaceNotify {
    pub market: MarketCommandHeader,
    pub order_type: OrderType,
    pub uids: Vec<u64>,
}

impl BulkReplaceNotify {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        let market = MarketCommandHeader::read(r)?;
        let order_type = OrderType::from_byte(read_u8_zero_tail(r));
        let count = read_u16_zero_tail(r) as usize;
        let mut uids = Vec::with_capacity(count);
        for _ in 0..count {
            uids.push(read_u64_zero_tail(r));
        }
        Some(Self {
            market,
            order_type,
            uids,
        })
    }
}
