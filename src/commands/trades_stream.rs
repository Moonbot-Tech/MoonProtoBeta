//! `MPC_TradesStream` unpacker.
//!
//! This module parses the public trades stream payload: exchange trades,
//! market-maker orders, liquidation orders, watcher fills, packet numbering, and
//! the packet-level compression flag. Gap tracking lives in
//! [`crate::state::TradesState`].

use std::borrow::Cow;

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

/// Header + decoded byte buffer for one `MPC_TradesStream` payload.
///
/// This is the zero-copy entry point that mirrors Delphi's `DataStream` walk:
/// compressed packets own the decompressed buffer, plain packets borrow the
/// incoming UDP payload without allocating.
pub struct DecodedTradesPacket<'a> {
    data: Cow<'a, [u8]>,
    has_taker: bool,
    pub base_time: f64,
    pub packet_num: u16,
}

impl<'a> DecodedTradesPacket<'a> {
    /// Iterate sections in wire order.
    pub fn sections(&self) -> TradeSectionIter<'_> {
        TradeSectionIter {
            data: &self.data,
            pos: TRADES_PACKET_HEADER_SIZE,
            has_taker: self.has_taker,
            done: false,
        }
    }
}

/// Borrowed section view from a decoded trades packet.
#[derive(Debug, Clone, Copy)]
pub enum TradeSectionRef<'a> {
    /// Futures or spot exchange trades.
    Trades(TradeRows<'a>),
    /// Market-maker order rows.
    MMOrders(MMOrderRows<'a>),
    /// Liquidation rows.
    LiqOrders(TradeRows<'a>),
    /// Watcher-fill batch.
    WatcherFills {
        market_index: u16,
        user: [u8; 20],
        data: &'a [u8],
    },
}

impl<'a> TradeSectionRef<'a> {
    pub fn market_index(&self) -> u16 {
        match self {
            Self::Trades(rows) | Self::LiqOrders(rows) => rows.market_index(),
            Self::MMOrders(rows) => rows.market_index(),
            Self::WatcherFills { market_index, .. } => *market_index,
        }
    }

    fn into_owned(self) -> TradeSection {
        match self {
            Self::Trades(rows) => TradeSection::Trades(rows.collect()),
            Self::MMOrders(rows) => TradeSection::MMOrders(rows.collect()),
            Self::LiqOrders(rows) => TradeSection::LiqOrders(
                rows.map(|trade| LiqOrder {
                    market_index: trade.market_index,
                    time_delta_ms: trade.time_delta_ms,
                    price: trade.price,
                    qty: trade.qty,
                })
                .collect(),
            ),
            Self::WatcherFills {
                market_index,
                user,
                data,
            } => TradeSection::WatcherFills {
                market_index,
                user,
                data: data.to_vec(),
            },
        }
    }
}

/// Borrowed iterator over futures/spot trade rows or liquidation rows.
#[derive(Debug, Clone, Copy)]
pub struct TradeRows<'a> {
    market_index: u16,
    is_spot: bool,
    data: &'a [u8],
    pos: usize,
}

impl<'a> TradeRows<'a> {
    pub fn market_index(&self) -> u16 {
        self.market_index
    }

    pub fn is_spot(&self) -> bool {
        self.is_spot
    }

    pub fn len(&self) -> usize {
        (self.data.len().saturating_sub(self.pos)) / TRADE_ROW_SIZE
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Iterator for TradeRows<'_> {
    type Item = Trade;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos + TRADE_ROW_SIZE > self.data.len() {
            return None;
        }
        let row =
            WireTradeRow::read_from_bytes(&self.data[self.pos..self.pos + TRADE_ROW_SIZE]).ok()?;
        self.pos += TRADE_ROW_SIZE;
        Some(Trade {
            market_index: self.market_index,
            is_spot: self.is_spot,
            time_delta_ms: row.time_delta_ms.get(),
            price: row.a.get(),
            qty: row.b.get(),
        })
    }
}

/// Borrowed iterator over market-maker order rows.
#[derive(Debug, Clone, Copy)]
pub struct MMOrderRows<'a> {
    market_index: u16,
    has_taker: bool,
    data: &'a [u8],
    pos: usize,
}

impl<'a> MMOrderRows<'a> {
    pub fn market_index(&self) -> u16 {
        self.market_index
    }

    pub fn len(&self) -> usize {
        let row_size = TRADE_ROW_SIZE + if self.has_taker { 20 } else { 0 };
        (self.data.len().saturating_sub(self.pos)) / row_size
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Iterator for MMOrderRows<'_> {
    type Item = MMOrder;

    fn next(&mut self) -> Option<Self::Item> {
        let row_size = TRADE_ROW_SIZE + if self.has_taker { 20 } else { 0 };
        if self.pos + row_size > self.data.len() {
            return None;
        }
        let row =
            WireTradeRow::read_from_bytes(&self.data[self.pos..self.pos + TRADE_ROW_SIZE]).ok()?;
        self.pos += TRADE_ROW_SIZE;

        let taker = if self.has_taker {
            let mut t = [0u8; 20];
            t.copy_from_slice(&self.data[self.pos..self.pos + 20]);
            self.pos += 20;
            Some(t)
        } else {
            None
        };

        Some(MMOrder {
            market_index: self.market_index,
            time_delta_ms: row.time_delta_ms.get(),
            vol: row.a.get(),
            q: row.b.get(),
            taker,
        })
    }
}

/// Iterator over borrowed trades sections.
pub struct TradeSectionIter<'a> {
    data: &'a [u8],
    pos: usize,
    has_taker: bool,
    done: bool,
}

impl<'a> TradeSectionIter<'a> {
    fn complete_rows(&self, count: usize, row_size: usize) -> usize {
        count.min((self.data.len().saturating_sub(self.pos)) / row_size)
    }
}

impl<'a> Iterator for TradeSectionIter<'a> {
    type Item = TradeSectionRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done || self.pos + 2 > self.data.len() {
            return None;
        }

        let market_index_and_flags =
            u16::from_le_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;

        // Bits 14-15 = section type, bits 0-13 = market index. Current Delphi
        // masks every section as `MarketIndexAndFlags and $3FFF`.
        let section_type = (market_index_and_flags >> 14) & 0x03;
        let market_index = market_index_and_flags & 0x3FFF;

        match section_type {
            0 | 2 => {
                if self.pos >= self.data.len() {
                    self.done = true;
                    return None;
                }
                let count = self.data[self.pos] as usize;
                self.pos += 1;
                let rows = self.complete_rows(count, TRADE_ROW_SIZE);
                let start = self.pos;
                self.pos += rows * TRADE_ROW_SIZE;
                Some(TradeSectionRef::Trades(TradeRows {
                    market_index,
                    is_spot: section_type == 2,
                    data: &self.data[start..self.pos],
                    pos: 0,
                }))
            }
            1 => {
                if self.pos >= self.data.len() {
                    self.done = true;
                    return None;
                }
                let count = self.data[self.pos] as usize;
                self.pos += 1;
                let row_size = TRADE_ROW_SIZE + if self.has_taker { 20 } else { 0 };
                let rows = self.complete_rows(count, row_size);
                let start = self.pos;
                self.pos += rows * row_size;
                Some(TradeSectionRef::MMOrders(MMOrderRows {
                    market_index,
                    has_taker: self.has_taker,
                    data: &self.data[start..self.pos],
                    pos: 0,
                }))
            }
            3 => {
                if self.pos >= self.data.len() {
                    self.done = true;
                    return None;
                }
                let ext_type = self.data[self.pos];
                self.pos += 1;
                match ext_type {
                    0 => {
                        if self.pos >= self.data.len() {
                            self.done = true;
                            return None;
                        }
                        let count = self.data[self.pos] as usize;
                        self.pos += 1;
                        let rows = self.complete_rows(count, TRADE_ROW_SIZE);
                        let start = self.pos;
                        self.pos += rows * TRADE_ROW_SIZE;
                        Some(TradeSectionRef::LiqOrders(TradeRows {
                            market_index,
                            is_spot: false,
                            data: &self.data[start..self.pos],
                            pos: 0,
                        }))
                    }
                    1 => {
                        if self.pos + 21 > self.data.len() {
                            self.done = true;
                            return None;
                        }
                        let mut user = [0u8; 20];
                        user.copy_from_slice(&self.data[self.pos..self.pos + 20]);
                        self.pos += 20;
                        let count = self.data[self.pos] as usize;
                        self.pos += 1;
                        let fill_bytes = count * WATCHER_FILL_RECORD_SIZE;
                        if self.pos + fill_bytes > self.data.len() {
                            self.done = true;
                            return None;
                        }
                        let start = self.pos;
                        self.pos += fill_bytes;
                        Some(TradeSectionRef::WatcherFills {
                            market_index,
                            user,
                            data: &self.data[start..self.pos],
                        })
                    }
                    _ => {
                        self.done = true;
                        None
                    }
                }
            }
            _ => {
                self.done = true;
                None
            }
        }
    }
}

fn read_trades_packet_header(data: &[u8]) -> Option<WireTradesPacketHeader> {
    if data.len() < TRADES_PACKET_HEADER_SIZE {
        return None;
    }
    WireTradesPacketHeader::read_from_bytes(&data[..TRADES_PACKET_HEADER_SIZE]).ok()
}

#[cfg(test)]
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
pub fn decode_trades_packet(raw: &[u8]) -> Option<DecodedTradesPacket<'_>> {
    if raw.is_empty() {
        return None;
    }

    // Flags byte is at the END
    let flags = raw[raw.len() - 1];
    let data_size = raw.len() - 1;

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

    Some(DecodedTradesPacket {
        data: decompressed,
        has_taker,
        base_time,
        packet_num,
    })
}

/// Parse a raw MPC_TradesStream payload.
/// Returns parsed packet or None on error.
pub fn parse_trades_packet(raw: &[u8]) -> Option<TradesPacket> {
    let decoded = decode_trades_packet(raw)?;
    Some(TradesPacket {
        base_time: decoded.base_time,
        packet_num: decoded.packet_num,
        sections: decoded
            .sections()
            .map(TradeSectionRef::into_owned)
            .collect(),
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

    fn trade_row_bytes(time_delta_ms: i16, a: f32, b: f32) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&time_delta_ms.to_le_bytes());
        data.extend_from_slice(&a.to_le_bytes());
        data.extend_from_slice(&b.to_le_bytes());
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

    #[test]
    fn section_iter_decodes_all_section_types_without_collecting_first() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&45_000.0f64.to_le_bytes());
        payload.extend_from_slice(&77u16.to_le_bytes());

        payload.extend_from_slice(&5u16.to_le_bytes()); // futures trades
        payload.push(2);
        payload.extend_from_slice(&trade_row_bytes(1, 100.0, 0.5));
        payload.extend_from_slice(&trade_row_bytes(2, 101.0, -0.25));

        payload.extend_from_slice(&(0x4000u16 | 6).to_le_bytes()); // MMOrders
        payload.push(1);
        payload.extend_from_slice(&trade_row_bytes(3, 12.0, 34.0));
        payload.extend_from_slice(&[0x11; 20]);

        payload.extend_from_slice(&(0x8000u16 | 7).to_le_bytes()); // spot trades
        payload.push(1);
        payload.extend_from_slice(&trade_row_bytes(4, 102.0, 0.75));

        payload.extend_from_slice(&(0xC000u16 | 8).to_le_bytes()); // LiqOrders
        payload.push(0);
        payload.push(1);
        payload.extend_from_slice(&trade_row_bytes(5, 99.0, -1.0));

        payload.extend_from_slice(&(0xC000u16 | 9).to_le_bytes()); // WatcherFills
        payload.push(1);
        payload.extend_from_slice(&[0xAB; 20]);
        payload.push(1);
        payload.extend_from_slice(&watcher_fill_bytes());

        payload.push(TRADES_FLAG_HAS_TAKER);

        let decoded = decode_trades_packet(&payload).expect("decoded packet");
        assert_eq!(decoded.base_time, 45_000.0);
        assert_eq!(decoded.packet_num, 77);

        let mut sections = decoded.sections();
        let TradeSectionRef::Trades(rows) = sections.next().expect("futures section") else {
            panic!("expected futures trades");
        };
        assert_eq!(rows.market_index(), 5);
        assert!(!rows.is_spot());
        assert_eq!(rows.len(), 2);
        let trades: Vec<_> = rows.collect();
        assert_eq!(trades[1].qty, -0.25);

        let TradeSectionRef::MMOrders(rows) = sections.next().expect("mm section") else {
            panic!("expected mm orders");
        };
        assert_eq!(rows.market_index(), 6);
        let orders: Vec<_> = rows.collect();
        assert_eq!(orders[0].taker, Some([0x11; 20]));

        let TradeSectionRef::Trades(rows) = sections.next().expect("spot section") else {
            panic!("expected spot trades");
        };
        assert_eq!(rows.market_index(), 7);
        assert!(rows.is_spot());
        assert_eq!(rows.collect::<Vec<_>>()[0].price, 102.0);

        let TradeSectionRef::LiqOrders(rows) = sections.next().expect("liq section") else {
            panic!("expected liq orders");
        };
        assert_eq!(rows.market_index(), 8);
        assert_eq!(rows.collect::<Vec<_>>()[0].qty, -1.0);

        let TradeSectionRef::WatcherFills {
            market_index,
            user,
            data,
        } = sections.next().expect("watcher section")
        else {
            panic!("expected watcher fills");
        };
        assert_eq!(market_index, 9);
        assert_eq!(user, [0xAB; 20]);
        assert_eq!(data.len(), WATCHER_FILL_RECORD_SIZE);
        assert!(sections.next().is_none());
    }

    #[test]
    fn section_iter_keeps_only_complete_rows_from_truncated_tail_like_old_parser() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&45_000.0f64.to_le_bytes());
        payload.extend_from_slice(&78u16.to_le_bytes());
        payload.extend_from_slice(&5u16.to_le_bytes());
        payload.push(2);
        payload.extend_from_slice(&trade_row_bytes(1, 100.0, 0.5));
        payload.push(0xEE); // one byte of the second row; not enough for a row or next section
        payload.push(0);

        let decoded = decode_trades_packet(&payload).expect("decoded packet");
        let TradeSectionRef::Trades(rows) = decoded.sections().next().expect("trades section")
        else {
            panic!("expected trades");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows.collect::<Vec<_>>()[0].price, 100.0);

        let owned = parse_trades_packet(&payload).expect("owned packet");
        let TradeSection::Trades(trades) = &owned.sections[0] else {
            panic!("expected owned trades");
        };
        assert_eq!(trades.len(), 1);
    }
}
