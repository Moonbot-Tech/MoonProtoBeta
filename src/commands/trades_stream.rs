//! `MPC_TradesStream` unpacker.
//!
//! This module parses the public trades stream payload: exchange trades,
//! market-maker orders, liquidation orders, watcher fills, packet numbering, and
//! the packet-level compression flag. Gap tracking lives in
//! [`crate::state::TradesState`].

use crate::compression;
use zerocopy::byteorder::little_endian::{F32 as LeF32, F64 as LeF64, I16 as LeI16, U16 as LeU16};
use zerocopy::{FromBytes, Immutable, KnownLayout, Unaligned};

// Flags byte (last byte of raw packet)
const TRADES_FLAG_COMPRESSED: u8 = 0x01;
const TRADES_FLAG_HAS_TAKER: u8 = 0x02;

/// Size in bytes of one Delphi watcher-fill record inside a watcher-fills
/// extended section.
pub const WATCHER_FILL_RECORD_SIZE: usize = std::mem::size_of::<WireWatcherFill>();
const _: [(); 20] = [(); WATCHER_FILL_RECORD_SIZE];
const TRADES_PACKET_HEADER_SIZE: usize = std::mem::size_of::<WireTradesPacketHeader>();
const _: [(); 10] = [(); TRADES_PACKET_HEADER_SIZE];
const TRADE_ROW_SIZE: usize = std::mem::size_of::<WireTradeRow>();
const _: [(); 10] = [(); TRADE_ROW_SIZE];

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
struct WireTradesPacketHeader {
    base_time: LeF64,
    packet_num: LeU16,
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
struct WireTradeRow {
    time_delta_ms: LeI16,
    a: LeF32,
    b: LeF32,
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
struct WireWatcherFill {
    time_delta_ms: LeI16,
    price: LeF32,
    qty: LeF32,
    z_btc: LeF32,
    position: LeF32,
    order_type: u8,
    flags: u8,
}

/// Flags stored in a watcher-fill record.
pub mod watcher_fill_flags {
    /// Fill belongs to a short position.
    pub const IS_SHORT: u8 = 0x01;
    /// Fill opens position exposure rather than closing it.
    pub const IS_OPEN: u8 = 0x02;
    /// Fill was taker-side.
    pub const IS_TAKER: u8 = 0x04;
}

/// One exchange trade record from a futures or spot section.
#[derive(Debug, Clone)]
pub struct Trade {
    /// Server market index from the current `GetMarketsIndexes` mapping.
    pub market_index: u16,
    /// `true` for spot trades, `false` for futures trades.
    pub is_spot: bool,
    /// Milliseconds offset from `TradesPacket::base_time`.
    pub time_delta_ms: i16,
    /// Trade price encoded as Delphi `Single`.
    pub price: f32,
    /// Signed quantity: positive means buy-side, negative means sell-side.
    pub qty: f32,
}

/// One market-maker order record.
#[derive(Debug, Clone)]
pub struct MMOrder {
    /// Server market index from the current `GetMarketsIndexes` mapping.
    pub market_index: u16,
    /// Milliseconds offset from `TradesPacket::base_time`.
    pub time_delta_ms: i16,
    /// Maker volume encoded as Delphi `Single`.
    pub vol: f32,
    /// Maker quantity encoded as Delphi `Single`.
    pub q: f32,
    /// Optional taker address when the packet-level `HAS_TAKER` flag is set.
    pub taker: Option<[u8; 20]>,
}

/// One liquidation order record.
#[derive(Debug, Clone)]
pub struct LiqOrder {
    /// Server market index from the current `GetMarketsIndexes` mapping.
    pub market_index: u16,
    /// Milliseconds offset from `TradesPacket::base_time`.
    pub time_delta_ms: i16,
    /// Liquidation price encoded as Delphi `Single`.
    pub price: f32,
    /// Signed liquidation quantity.
    pub qty: f32,
}

/// One decoded watcher fill inside a watcher-fills section.
///
/// The enclosing [`TradeSection::WatcherFills`] carries the `market_index` and
/// HyperLiquid user address for the whole batch. Each record stores the
/// per-fill fields written by Delphi `WriteFillsBatch`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WatcherFill {
    /// Milliseconds offset from `TradesPacket::base_time`.
    pub time_delta_ms: i16,
    /// Fill price encoded as Delphi `Single`.
    pub price: f32,
    /// Fill quantity encoded as Delphi `Single`.
    pub qty: f32,
    /// BTC value encoded as Delphi `Single`.
    pub z_btc: f32,
    /// Position value encoded as Delphi `Single`.
    pub position: f32,
    /// Delphi `TOrderType` ordinal.
    pub order_type: u8,
    /// Raw flags byte: bit0 short, bit1 open, bit2 taker.
    pub flags: u8,
}

impl WatcherFill {
    /// Return whether the fill belongs to a short position.
    pub fn is_short(&self) -> bool {
        self.flags & watcher_fill_flags::IS_SHORT != 0
    }

    /// Return whether the fill opens exposure rather than closing it.
    pub fn is_open(&self) -> bool {
        self.flags & watcher_fill_flags::IS_OPEN != 0
    }

    /// Return whether the fill was taker-side.
    pub fn is_taker(&self) -> bool {
        self.flags & watcher_fill_flags::IS_TAKER != 0
    }
}

/// Parsed section from a trades packet.
#[derive(Debug, Clone)]
pub enum TradeSection {
    /// Futures or spot exchange trades.
    Trades(Vec<Trade>),
    /// Market-maker order rows.
    MMOrders(Vec<MMOrder>),
    /// Liquidation rows.
    LiqOrders(Vec<LiqOrder>),
    /// Watcher-fill batch.
    ///
    /// `data` keeps the original `Count * 20` bytes for backward-compatible
    /// low-level tools. Use [`parse_watcher_fills`] or
    /// [`TradeSection::watcher_fill_records`] to decode it into typed records.
    WatcherFills {
        /// Server market index from the current `GetMarketsIndexes` mapping.
        market_index: u16,
        /// HyperLiquid user address shared by all records in `data`.
        user: [u8; 20],
        /// Raw watcher-fill records, each [`WATCHER_FILL_RECORD_SIZE`] bytes.
        data: Vec<u8>,
    },
}

impl TradeSection {
    /// Decode watcher-fill records when this section is
    /// [`TradeSection::WatcherFills`].
    ///
    /// Returns `None` for non-watcher sections and for malformed raw watcher
    /// data whose length is not a multiple of 20 bytes.
    pub fn watcher_fill_records(&self) -> Option<Vec<WatcherFill>> {
        match self {
            Self::WatcherFills { data, .. } => parse_watcher_fills(data),
            _ => None,
        }
    }
}

/// Full parsed trades packet.
#[derive(Debug, Clone)]
pub struct TradesPacket {
    /// Delphi `TDateTime` base used by per-record millisecond offsets.
    pub base_time: f64,
    /// Wrapping packet number used by trades gap recovery.
    pub packet_num: u16,
    /// Parsed payload sections in wire order.
    pub sections: Vec<TradeSection>,
}

fn read_trades_packet_header(data: &[u8]) -> Option<WireTradesPacketHeader> {
    if data.len() < TRADES_PACKET_HEADER_SIZE {
        return None;
    }
    WireTradesPacketHeader::read_from_bytes(&data[..TRADES_PACKET_HEADER_SIZE]).ok()
}

fn read_trade_row(data: &[u8], pos: &mut usize) -> Option<WireTradeRow> {
    if *pos + TRADE_ROW_SIZE > data.len() {
        return None;
    }
    let row = WireTradeRow::read_from_bytes(&data[*pos..*pos + TRADE_ROW_SIZE]).ok()?;
    *pos += TRADE_ROW_SIZE;
    Some(row)
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
    let header = read_trades_packet_header(data)?;
    let base_time = header.base_time.get();
    let packet_num = header.packet_num.get();
    let mut pos = TRADES_PACKET_HEADER_SIZE;

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
                    let Some(row) = read_trade_row(data, &mut pos) else {
                        break;
                    };

                    trades.push(Trade {
                        market_index: market_idx,
                        is_spot,
                        time_delta_ms: row.time_delta_ms.get(),
                        price: row.a.get(),
                        qty: row.b.get(),
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

                let bytes_per_order = TRADE_ROW_SIZE + if has_taker { 20 } else { 0 };
                let mut orders = Vec::with_capacity(count);

                for _ in 0..count {
                    if pos + bytes_per_order > data.len() {
                        break;
                    }
                    let row = read_trade_row(data, &mut pos)?;

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
                        time_delta_ms: row.time_delta_ms.get(),
                        vol: row.a.get(),
                        q: row.b.get(),
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
                            let Some(row) = read_trade_row(data, &mut pos) else {
                                break;
                            };
                            orders.push(LiqOrder {
                                market_index: market_idx,
                                time_delta_ms: row.time_delta_ms.get(),
                                price: row.a.get(),
                                qty: row.b.get(),
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

/// Decode the raw record bytes from [`TradeSection::WatcherFills`].
///
/// Delphi writes each watcher fill as:
/// `TimeDelta:i16 + price:f32 + qty:f32 + zBTC:f32 + position:f32 +
/// order_type:u8 + flags:u8`.
pub fn parse_watcher_fills(data: &[u8]) -> Option<Vec<WatcherFill>> {
    if data.len() % WATCHER_FILL_RECORD_SIZE != 0 {
        return None;
    }

    let mut fills = Vec::with_capacity(data.len() / WATCHER_FILL_RECORD_SIZE);
    for chunk in data.chunks_exact(WATCHER_FILL_RECORD_SIZE) {
        let wire = WireWatcherFill::read_from_bytes(chunk).ok()?;
        fills.push(WatcherFill {
            time_delta_ms: wire.time_delta_ms.get(),
            price: wire.price.get(),
            qty: wire.qty.get(),
            z_btc: wire.z_btc.get(),
            position: wire.position.get(),
            order_type: wire.order_type,
            flags: wire.flags,
        });
    }
    Some(fills)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn watcher_fill_bytes() -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&(-12i16).to_le_bytes());
        data.extend_from_slice(&123.5f32.to_le_bytes());
        data.extend_from_slice(&(-0.25f32).to_le_bytes());
        data.extend_from_slice(&0.03125f32.to_le_bytes());
        data.extend_from_slice(&4.5f32.to_le_bytes());
        data.push(7);
        data.push(
            watcher_fill_flags::IS_SHORT
                | watcher_fill_flags::IS_OPEN
                | watcher_fill_flags::IS_TAKER,
        );
        data
    }

    #[test]
    fn trades_stream_rows_use_private_wire_structs() {
        assert_eq!(std::mem::size_of::<WireTradesPacketHeader>(), 10);
        assert_eq!(TRADES_PACKET_HEADER_SIZE, 10);
        assert_eq!(std::mem::size_of::<WireTradeRow>(), 10);
        assert_eq!(TRADE_ROW_SIZE, 10);
        assert_eq!(std::mem::size_of::<WireWatcherFill>(), 20);
        assert_eq!(WATCHER_FILL_RECORD_SIZE, 20);

        let mut packet = Vec::new();
        packet.extend_from_slice(&45_000.0f64.to_le_bytes());
        packet.extend_from_slice(&7u16.to_le_bytes());
        let header = read_trades_packet_header(&packet).expect("header");
        assert_eq!(header.base_time.get(), 45_000.0);
        assert_eq!(header.packet_num.get(), 7);

        packet.extend_from_slice(&(-12i16).to_le_bytes());
        packet.extend_from_slice(&123.5f32.to_le_bytes());
        packet.extend_from_slice(&(-0.25f32).to_le_bytes());
        let mut pos = TRADES_PACKET_HEADER_SIZE;
        let row = read_trade_row(&packet, &mut pos).expect("trade row");
        assert_eq!(pos, TRADES_PACKET_HEADER_SIZE + TRADE_ROW_SIZE);
        assert_eq!(row.time_delta_ms.get(), -12);
        assert_eq!(row.a.get(), 123.5);
        assert_eq!(row.b.get(), -0.25);
    }

    #[test]
    fn parse_watcher_fills_decodes_delphi_records() {
        let fills = parse_watcher_fills(&watcher_fill_bytes()).expect("watcher fill");

        assert_eq!(fills.len(), 1);
        let fill = fills[0];
        assert_eq!(fill.time_delta_ms, -12);
        assert_eq!(fill.price, 123.5);
        assert_eq!(fill.qty, -0.25);
        assert_eq!(fill.z_btc, 0.03125);
        assert_eq!(fill.position, 4.5);
        assert_eq!(fill.order_type, 7);
        assert!(fill.is_short());
        assert!(fill.is_open());
        assert!(fill.is_taker());
    }

    #[test]
    fn parse_watcher_fills_rejects_partial_record() {
        let mut data = watcher_fill_bytes();
        data.pop();
        assert!(parse_watcher_fills(&data).is_none());
    }

    #[test]
    fn trades_packet_exposes_typed_watcher_fill_helper() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&45_000.0f64.to_le_bytes());
        payload.extend_from_slice(&42u16.to_le_bytes());
        payload.extend_from_slice(&(0xC000u16 | 5).to_le_bytes());
        payload.push(1); // ExtType WatcherFills
        payload.extend_from_slice(&[0xAB; 20]);
        payload.push(1);
        payload.extend_from_slice(&watcher_fill_bytes());
        payload.push(0); // packet flags

        let packet = parse_trades_packet(&payload).expect("trades packet");
        assert_eq!(packet.packet_num, 42);
        let TradeSection::WatcherFills {
            market_index,
            user,
            data,
        } = &packet.sections[0]
        else {
            panic!("expected watcher fills");
        };
        assert_eq!(*market_index, 5);
        assert_eq!(*user, [0xAB; 20]);
        assert_eq!(data.len(), WATCHER_FILL_RECORD_SIZE);

        let records = packet.sections[0]
            .watcher_fill_records()
            .expect("typed watcher fills");
        assert_eq!(records[0].order_type, 7);
    }
}
