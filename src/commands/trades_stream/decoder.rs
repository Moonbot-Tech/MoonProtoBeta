use std::borrow::Cow;

use super::types::{LiqOrder, MMOrder, Trade, TradeSection};
use super::wire::{
    read_trades_packet_header, WireTradeRow, TRADES_FLAG_COMPRESSED, TRADES_FLAG_HAS_TAKER,
    TRADES_PACKET_HEADER_SIZE, TRADE_ROW_SIZE, WATCHER_FILL_RECORD_SIZE,
};
use crate::compression;
use zerocopy::FromBytes;

/// Header + decoded byte buffer for one `MPC_TradesStream` payload.
///
/// This is the zero-copy entry point that mirrors Delphi's `DataStream` walk:
/// compressed packets own the decompressed buffer, plain packets borrow the
/// incoming UDP payload without allocating.
pub(crate) struct DecodedTradesPacket<'a> {
    data: Cow<'a, [u8]>,
    has_taker: bool,
    pub base_time: f64,
    pub packet_num: u16,
}

impl<'a> DecodedTradesPacket<'a> {
    /// Iterate sections in wire order.
    pub(crate) fn sections(&self) -> TradeSectionIter<'_> {
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
pub(crate) enum TradeSectionRef<'a> {
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
    pub(super) fn into_owned(self) -> TradeSection {
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
pub(crate) struct TradeRows<'a> {
    market_index: u16,
    is_spot: bool,
    data: &'a [u8],
    pos: usize,
}

impl<'a> TradeRows<'a> {
    pub(crate) fn market_index(&self) -> u16 {
        self.market_index
    }

    pub(crate) fn is_spot(&self) -> bool {
        self.is_spot
    }

    pub(crate) fn len(&self) -> usize {
        (self.data.len().saturating_sub(self.pos)) / TRADE_ROW_SIZE
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
pub(crate) struct MMOrderRows<'a> {
    market_index: u16,
    has_taker: bool,
    data: &'a [u8],
    pos: usize,
}

impl<'a> MMOrderRows<'a> {
    pub(crate) fn market_index(&self) -> u16 {
        self.market_index
    }

    pub(crate) fn len(&self) -> usize {
        let row_size = TRADE_ROW_SIZE + if self.has_taker { 20 } else { 0 };
        (self.data.len().saturating_sub(self.pos)) / row_size
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
pub(crate) struct TradeSectionIter<'a> {
    data: &'a [u8],
    pos: usize,
    has_taker: bool,
    done: bool,
}

impl<'a> TradeSectionIter<'a> {
    fn take_complete_row_bytes(&mut self, count: usize, row_size: usize) -> &'a [u8] {
        let available = self.data.len().saturating_sub(self.pos);
        let rows = count.min(available / row_size);
        let start = self.pos;
        let end = start + rows * row_size;
        self.pos = end;
        if count * row_size > available {
            self.pos = self.data.len();
            self.done = true;
        }
        &self.data[start..end]
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
                let data = self.take_complete_row_bytes(count, TRADE_ROW_SIZE);
                Some(TradeSectionRef::Trades(TradeRows {
                    market_index,
                    is_spot: section_type == 2,
                    data,
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
                let data = self.take_complete_row_bytes(count, row_size);
                Some(TradeSectionRef::MMOrders(MMOrderRows {
                    market_index,
                    has_taker: self.has_taker,
                    data,
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
                        let data = self.take_complete_row_bytes(count, TRADE_ROW_SIZE);
                        Some(TradeSectionRef::LiqOrders(TradeRows {
                            market_index,
                            is_spot: false,
                            data,
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

/// Decode a raw `MPC_TradesStream` payload into a borrowed packet view.
/// Returns `None` on malformed payload.
pub(crate) fn decode_trades_packet(raw: &[u8]) -> Option<DecodedTradesPacket<'_>> {
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
