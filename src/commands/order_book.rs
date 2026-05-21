/// MPC_OrderBook unpacker — byte-exact port of MoonProtoOrderBook.pas client side.
/// Source: MoonProtoOrderBook.pas:576-680 (ReadAndApplyFull/Diff)
///
/// Packets arrive SynLZ-compressed. After decompression:
///   MarketIndex(2) + Seq(2) + Flags(1) + Glass data
use crate::compression;

/// One price level in the order book
#[derive(Debug, Clone, Copy)]
pub struct OrderLevel {
    pub rate: f32,
    pub quantity: f32,
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
pub fn compare_seq(a: u16, b: u16) -> i16 {
    a.wrapping_sub(b) as i16
}

/// Parse a raw MPC_OrderBook payload (SynLZ compressed).
pub fn parse_order_book_packet(raw: &[u8]) -> Option<OrderBookUpdate> {
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

    // Cap по реально доступным байтам — защита от malicious `buy_count = 65535` который
    // на пустом payload запросил бы `Vec::with_capacity(65535)` = 512 KB на пакет ×
    // burst rate = аллокатор thrash. См. robustness audit H7.
    let max_buys_by_payload = (data.len() - pos) / 8;
    let buy_count = buy_count_raw.min(max_buys_by_payload);

    let mut buys = Vec::with_capacity(buy_count);
    for _ in 0..buy_count {
        if pos + 8 > data.len() {
            break;
        }
        let rate = f32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        let qty = f32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap());
        pos += 8;
        buys.push(OrderLevel {
            rate,
            quantity: qty,
        });
    }

    // Sells: remaining bytes / 8
    let sell_count = (data.len() - pos) / 8;
    let mut sells = Vec::with_capacity(sell_count);
    for _ in 0..sell_count {
        if pos + 8 > data.len() {
            break;
        }
        let rate = f32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        let qty = f32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap());
        pos += 8;
        sells.push(OrderLevel {
            rate,
            quantity: qty,
        });
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
