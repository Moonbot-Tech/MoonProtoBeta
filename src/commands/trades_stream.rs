/// MPC_TradesStream unpacker — byte-exact port of TMoonProtoEngine.ProcessTradesStream.
/// Source: MoonProtoEngine.pas:1553-1895
///
/// Handles: decompression, section parsing (Futures/Spot/MMOrders/Extended),
/// gap detection (packet numbering).
use crate::compression;

// Flags byte (last byte of raw packet)
const TRADES_FLAG_COMPRESSED: u8 = 0x01;
const TRADES_FLAG_HAS_TAKER: u8 = 0x02;

/// One parsed trade
#[derive(Debug, Clone)]
pub struct Trade {
    pub market_index: u16,
    pub is_spot: bool,
    pub time_delta_ms: i16, // offset from BaseTime in milliseconds
    pub price: f32,
    pub qty: f32, // negative = SELL, positive = BUY
}

/// One MMOrder
#[derive(Debug, Clone)]
pub struct MMOrder {
    pub market_index: u16,
    pub time_delta_ms: i16,
    pub vol: f32,
    pub q: f32,
    pub taker: Option<[u8; 20]>,
}

/// One liquidation order
#[derive(Debug, Clone)]
pub struct LiqOrder {
    pub market_index: u16,
    pub time_delta_ms: i16,
    pub price: f32,
    pub qty: f32,
}

/// Parsed section from a trades packet
#[derive(Debug, Clone)]
pub enum TradeSection {
    Trades(Vec<Trade>),
    MMOrders(Vec<MMOrder>),
    LiqOrders(Vec<LiqOrder>),
    WatcherFills {
        market_index: u16,
        user: [u8; 20],
        data: Vec<u8>,
    },
}

/// Full parsed trades packet
#[derive(Debug, Clone)]
pub struct TradesPacket {
    pub base_time: f64, // TDateTime
    pub packet_num: u16,
    pub sections: Vec<TradeSection>,
}

/// Parse a raw MPC_TradesStream payload.
/// Returns parsed packet or None on error.
pub fn parse_trades_packet(raw: &[u8]) -> Option<TradesPacket> {
    if raw.is_empty() {
        return None;
    }

    // Flags byte is at the END
    let flags = raw[raw.len() - 1];
    let data_size = raw.len() - 1;

    // B-V2-10 fix: Cow вместо безусловного to_vec для не-compressed случая.
    // TradesStream — самый частый hot-input на пике; trades_packet может приходить
    // 50K+ раз/сек. Большинство пакетов uncompressed (мелкие batches). Раньше каждый
    // делал alloc 200-1500б "просто чтобы owned" — теперь zero alloc для borrow case.
    use std::borrow::Cow;
    let decompressed: Cow<'_, [u8]> = if flags & TRADES_FLAG_COMPRESSED != 0 {
        Cow::Owned(compression::mp_decompress(&raw[..data_size])?)
    } else {
        Cow::Borrowed(&raw[..data_size])
    };

    let has_taker = (flags & TRADES_FLAG_HAS_TAKER) != 0;
    let data: &[u8] = &decompressed;

    // Header: BaseTime(8) + PacketNum(2)
    if data.len() < 10 {
        return None;
    }
    let base_time = f64::from_le_bytes(data[0..8].try_into().unwrap());
    let packet_num = u16::from_le_bytes(data[8..10].try_into().unwrap());
    let mut pos = 10usize;

    let mut sections = Vec::new();

    // Parse sections
    while pos + 2 <= data.len() {
        let market_index_and_flags = u16::from_le_bytes([data[pos], data[pos + 1]]);
        pos += 2;

        // bits 14-15 = section type, bits 0-13 = market index (14 бит, max 16383).
        // ИНВАРИАНТ: mIndex < 16384 (бот обслуживает сотни рынков, не десятки тысяч).
        // ПРИМЕЧАНИЕ для исторического контекста: до фикса в MoonProtoTradesStream.pas:531
        // Delphi сервер не применял `and $3FFF` для MMOrders sub-stream (другие применяли).
        // Здесь mask `& 0x3FFF` применяется ЕДИНООБРАЗНО для всех section_type — это компенсирует
        // забагованный Delphi сервер на mIndex < 16384 (где маска не имела видимого эффекта) и
        // корректно работает с исправленным сервером. См. ARCHITECTURE.md OPEN-QUESTIONS §8 (ЗАКРЫТО).
        let section_type = (market_index_and_flags >> 14) & 0x03;
        let market_idx = market_index_and_flags & 0x3FFF;

        match section_type {
            0 | 2 => {
                // Futures (0) or Spot (2) trades
                if pos >= data.len() {
                    break;
                }
                let count = data[pos] as usize;
                pos += 1;

                let is_spot = section_type == 2;
                let mut trades = Vec::with_capacity(count);

                for _ in 0..count {
                    if pos + 10 > data.len() {
                        break;
                    }
                    let time_delta = i16::from_le_bytes([data[pos], data[pos + 1]]);
                    let price = f32::from_le_bytes(data[pos + 2..pos + 6].try_into().unwrap());
                    let qty = f32::from_le_bytes(data[pos + 6..pos + 10].try_into().unwrap());
                    pos += 10;

                    trades.push(Trade {
                        market_index: market_idx,
                        is_spot,
                        time_delta_ms: time_delta,
                        price,
                        qty,
                    });
                }
                sections.push(TradeSection::Trades(trades));
            }
            1 => {
                // MMOrders
                if pos >= data.len() {
                    break;
                }
                let count = data[pos] as usize;
                pos += 1;

                let bytes_per_order = 10 + if has_taker { 20 } else { 0 };
                let mut orders = Vec::with_capacity(count);

                for _ in 0..count {
                    if pos + bytes_per_order > data.len() {
                        break;
                    }
                    let time_delta = i16::from_le_bytes([data[pos], data[pos + 1]]);
                    let vol = f32::from_le_bytes(data[pos + 2..pos + 6].try_into().unwrap());
                    let q = f32::from_le_bytes(data[pos + 6..pos + 10].try_into().unwrap());
                    pos += 10;

                    let taker = if has_taker {
                        let mut t = [0u8; 20];
                        t.copy_from_slice(&data[pos..pos + 20]);
                        pos += 20;
                        Some(t)
                    } else {
                        None
                    };

                    orders.push(MMOrder {
                        market_index: market_idx,
                        time_delta_ms: time_delta,
                        vol,
                        q,
                        taker,
                    });
                }
                sections.push(TradeSection::MMOrders(orders));
            }
            3 => {
                // Extended section
                if pos >= data.len() {
                    break;
                }
                let ext_type = data[pos];
                pos += 1;

                match ext_type {
                    0 => {
                        // LiqOrders
                        if pos >= data.len() {
                            break;
                        }
                        let count = data[pos] as usize;
                        pos += 1;

                        let mut orders = Vec::with_capacity(count);
                        for _ in 0..count {
                            if pos + 10 > data.len() {
                                break;
                            }
                            let time_delta = i16::from_le_bytes([data[pos], data[pos + 1]]);
                            let price =
                                f32::from_le_bytes(data[pos + 2..pos + 6].try_into().unwrap());
                            let qty =
                                f32::from_le_bytes(data[pos + 6..pos + 10].try_into().unwrap());
                            pos += 10;
                            orders.push(LiqOrder {
                                market_index: market_idx,
                                time_delta_ms: time_delta,
                                price,
                                qty,
                            });
                        }
                        sections.push(TradeSection::LiqOrders(orders));
                    }
                    1 => {
                        // WatcherFills
                        if pos + 21 > data.len() {
                            break;
                        }
                        let mut user = [0u8; 20];
                        user.copy_from_slice(&data[pos..pos + 20]);
                        pos += 20;

                        let count = data[pos] as usize;
                        pos += 1;

                        // 20 bytes per fill
                        let fill_bytes = count * 20;
                        if pos + fill_bytes > data.len() {
                            break;
                        }
                        let fill_data = data[pos..pos + fill_bytes].to_vec();
                        pos += fill_bytes;

                        sections.push(TradeSection::WatcherFills {
                            market_index: market_idx,
                            user,
                            data: fill_data,
                        });
                    }
                    _ => {
                        // Unknown ExtType — cannot determine section size, bail out
                        break;
                    }
                }
            }
            _ => break,
        }
    }

    Some(TradesPacket {
        base_time,
        packet_num,
        sections,
    })
}
