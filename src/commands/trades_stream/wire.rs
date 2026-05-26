use zerocopy::byteorder::little_endian::{F32 as LeF32, F64 as LeF64, I16 as LeI16, U16 as LeU16};
use zerocopy::{FromBytes, Immutable, KnownLayout, Unaligned};

// Flags byte (last byte of raw packet)
pub(crate) const TRADES_FLAG_COMPRESSED: u8 = 0x01;
pub(crate) const TRADES_FLAG_HAS_TAKER: u8 = 0x02;

/// Size in bytes of one Delphi watcher-fill record inside a watcher-fills
/// extended section.
pub const WATCHER_FILL_RECORD_SIZE: usize = std::mem::size_of::<WireWatcherFill>();
const _: [(); 20] = [(); WATCHER_FILL_RECORD_SIZE];
pub(crate) const TRADES_PACKET_HEADER_SIZE: usize = std::mem::size_of::<WireTradesPacketHeader>();
const _: [(); 10] = [(); TRADES_PACKET_HEADER_SIZE];
pub(crate) const TRADE_ROW_SIZE: usize = std::mem::size_of::<WireTradeRow>();
const _: [(); 10] = [(); TRADE_ROW_SIZE];

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
pub(crate) struct WireTradesPacketHeader {
    pub(crate) base_time: LeF64,
    pub(crate) packet_num: LeU16,
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
pub(crate) struct WireTradeRow {
    pub(crate) time_delta_ms: LeI16,
    pub(crate) a: LeF32,
    pub(crate) b: LeF32,
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
pub(crate) struct WireWatcherFill {
    pub(crate) time_delta_ms: LeI16,
    pub(crate) price: LeF32,
    pub(crate) qty: LeF32,
    pub(crate) z_btc: LeF32,
    pub(crate) position: LeF32,
    pub(crate) order_type: u8,
    pub(crate) flags: u8,
}

pub(crate) fn read_trades_packet_header(data: &[u8]) -> Option<WireTradesPacketHeader> {
    if data.len() < TRADES_PACKET_HEADER_SIZE {
        return None;
    }
    WireTradesPacketHeader::read_from_bytes(&data[..TRADES_PACKET_HEADER_SIZE]).ok()
}

#[cfg(test)]
pub(crate) fn read_trade_row(data: &[u8], pos: &mut usize) -> Option<WireTradeRow> {
    if *pos + TRADE_ROW_SIZE > data.len() {
        return None;
    }
    let row = WireTradeRow::read_from_bytes(&data[*pos..*pos + TRADE_ROW_SIZE]).ok()?;
    *pos += TRADE_ROW_SIZE;
    Some(row)
}
