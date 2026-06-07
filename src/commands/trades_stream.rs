//! `MPC_TradesStream` unpacker.
//!
//! This module parses the public trades stream payload: exchange trades,
//! market-maker orders, liquidation orders, watcher fills, packet numbering, and
//! the packet-level compression flag. Gap tracking is owned by the active
//! client runtime.

mod decoder;
mod owned;
mod types;
mod wire;

#[allow(unused_imports)]
pub(crate) use decoder::{
    decode_trades_packet, DecodedTradesPacket, MMOrderRows, TradeRows, TradeSectionIter,
    TradeSectionRef,
};
#[cfg(test)]
pub(crate) use owned::parse_trades_packet;
pub(crate) use owned::parse_watcher_fills;
#[cfg(test)]
pub(crate) use types::TradeSection;
#[cfg(test)]
pub(crate) use types::TradesPacket;
#[allow(unused_imports)]
pub(crate) use types::{watcher_fill_flags, LiqOrder, MMOrder, WatcherFillFlags};
#[allow(unused_imports)]
pub(crate) use wire::WATCHER_FILL_RECORD_SIZE;

#[cfg(test)]
pub(super) use wire::{
    read_trade_row, read_trades_packet_header, WireTradeRow, WireTradesPacketHeader,
    WireWatcherFill, TRADES_FLAG_HAS_TAKER, TRADES_PACKET_HEADER_SIZE, TRADE_ROW_SIZE,
};

#[cfg(test)]
mod tests;
