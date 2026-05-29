use super::decoder::{decode_trades_packet, TradeSectionRef};
use super::types::{TradeSection, TradesPacket, WatcherFill, WatcherFillFlags};
use super::wire::{WireWatcherFill, WATCHER_FILL_RECORD_SIZE};
use crate::commands::trade::OrderType;
use zerocopy::FromBytes;

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

/// Parse a raw `MPC_TradesStream` payload into an owned packet.
///
/// Active dispatch uses [`decode_trades_packet`] and borrowed section iteration;
/// this helper is for low-level tools that explicitly need owned rows.
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
            order_type: OrderType::from_byte(wire.order_type),
            flags: WatcherFillFlags::from_bits(wire.flags),
        });
    }
    Some(fills)
}
