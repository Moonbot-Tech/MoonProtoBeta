/// MPC_OrderBook unpacker — byte-exact port of MoonProtoOrderBook.pas client side.
/// Source: MoonProtoOrderBook.pas:576-680 (ReadAndApplyFull/Diff)
///
/// Packets arrive SynLZ-compressed. After decompression:
///   MarketIndex(2) + Seq(2) + Flags(1) + Glass data
use crate::compression;
use zerocopy::byteorder::little_endian::F32 as LeF32;
use zerocopy::{FromBytes, Immutable, KnownLayout, Unaligned};

/// One price level in the order book
#[derive(Debug, Clone, Copy)]
pub struct OrderLevel {
    pub rate: f32,
    pub quantity: f32,
}

/// Delphi `TOrderGlass` wire row in `MoonProtoOrderBook.pas`.
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C, packed)]
struct WireOrderLevel {
    rate: LeF32,
    quantity: LeF32,
}

const ORDER_LEVEL_SIZE: usize = std::mem::size_of::<WireOrderLevel>();
const _: [(); 8] = [(); ORDER_LEVEL_SIZE];

impl From<WireOrderLevel> for OrderLevel {
    fn from(wire: WireOrderLevel) -> Self {
        Self {
            rate: wire.rate.get(),
            quantity: wire.quantity.get(),
        }
    }
}

/// Parsed order book update
#[derive(Debug, Clone)]
pub struct OrderBookUpdate {
    pub market_index: u16,
    pub seq: u16,
    pub is_full: bool,
    pub book_kind: u8, // 0=Futures, 1=Spot
    pub buys: Vec<OrderLevel>,
    pub sells: Vec<OrderLevel>,
}

/// Wrapping-safe sequence comparison (matches Delphi CompareSeq)
pub(crate) fn compare_seq(a: u16, b: u16) -> i16 {
    a.wrapping_sub(b) as i16
}

/// Parse a raw MPC_OrderBook payload (SynLZ compressed).
pub(crate) fn parse_order_book_packet(raw: &[u8]) -> Option<OrderBookUpdate> {
    // Decompress (OrderBook is always compressed)
    let data = compression::mp_decompress(raw)?;

    if data.len() < 5 {
        return None;
    }

    let market_index = u16::from_le_bytes([data[0], data[1]]);
    let seq = u16::from_le_bytes([data[2], data[3]]);
    let flags = data[4];
    let is_full = (flags & 1) != 0;
    let book_kind = (flags >> 1) & 1;

    let mut pos = 5;

    // BuyCount + Buy levels
    if pos + 2 > data.len() {
        return None;
    }
    let buy_count_raw = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
    pos += 2;

    let buy_bytes = buy_count_raw.checked_mul(ORDER_LEVEL_SIZE)?;
    if buy_bytes > data.len().saturating_sub(pos) {
        return None;
    }
    let buy_count = buy_count_raw;

    let mut buys = Vec::with_capacity(buy_count);
    for _ in 0..buy_count {
        if pos + ORDER_LEVEL_SIZE > data.len() {
            break;
        }
        let wire = WireOrderLevel::read_from_bytes(&data[pos..pos + ORDER_LEVEL_SIZE]).ok()?;
        pos += ORDER_LEVEL_SIZE;
        buys.push(wire.into());
    }

    // Sells: remaining bytes / 8
    let sell_count = (data.len() - pos) / ORDER_LEVEL_SIZE;
    let mut sells = Vec::with_capacity(sell_count);
    for _ in 0..sell_count {
        if pos + ORDER_LEVEL_SIZE > data.len() {
            break;
        }
        let wire = WireOrderLevel::read_from_bytes(&data[pos..pos + ORDER_LEVEL_SIZE]).ok()?;
        pos += ORDER_LEVEL_SIZE;
        sells.push(wire.into());
    }

    Some(OrderBookUpdate {
        market_index,
        seq,
        is_full,
        book_kind,
        buys,
        sells,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn push_level(out: &mut Vec<u8>, rate: f32, quantity: f32) {
        out.extend_from_slice(&rate.to_le_bytes());
        out.extend_from_slice(&quantity.to_le_bytes());
    }

    fn compressed_packet(buy_count: u16, levels: &[(f32, f32)]) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&1u16.to_le_bytes());
        data.extend_from_slice(&2u16.to_le_bytes());
        data.push(1);
        data.extend_from_slice(&buy_count.to_le_bytes());
        for (rate, quantity) in levels {
            push_level(&mut data, *rate, *quantity);
        }
        compression::synlz_compress(&data)
    }

    #[test]
    fn parse_order_book_uses_declared_buy_count_without_rust_cap() {
        let raw = compressed_packet(2, &[(10.0, 1.0), (9.0, 2.0), (11.0, 3.0)]);

        let pkt = parse_order_book_packet(&raw).expect("valid orderbook packet");

        assert_eq!(pkt.market_index, 1);
        assert_eq!(pkt.seq, 2);
        assert!(pkt.is_full);
        assert_eq!(pkt.book_kind, 0);
        assert_eq!(pkt.buys.len(), 2);
        assert_eq!(pkt.sells.len(), 1);
        assert_eq!(pkt.buys[0].rate, 10.0);
        assert_eq!(pkt.buys[1].quantity, 2.0);
        assert_eq!(pkt.sells[0].rate, 11.0);
    }

    #[test]
    fn parse_order_book_rejects_truncated_buy_section_instead_of_reclassifying_tail() {
        let raw = compressed_packet(3, &[(10.0, 1.0), (9.0, 2.0)]);

        assert!(parse_order_book_packet(&raw).is_none());
    }
}
