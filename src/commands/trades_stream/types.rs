use crate::commands::trade::OrderType;

/// Flags stored in a watcher-fill record.
#[derive(Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct WatcherFillFlags(u8);

impl WatcherFillFlags {
    /// Fill belongs to a short position.
    pub const IS_SHORT: Self = Self(0x01);
    /// Fill opens position exposure rather than closing it.
    pub const IS_OPEN: Self = Self(0x02);
    /// Fill was taker-side.
    pub const IS_TAKER: Self = Self(0x04);

    pub const fn from_bits(bits: u8) -> Self {
        Self(bits)
    }

    pub const fn bits(self) -> u8 {
        self.0
    }

    pub const fn contains(self, flag: Self) -> bool {
        self.0 & flag.0 != 0
    }

    pub const fn is_short(self) -> bool {
        self.contains(Self::IS_SHORT)
    }

    pub const fn is_open(self) -> bool {
        self.contains(Self::IS_OPEN)
    }

    pub const fn is_taker(self) -> bool {
        self.contains(Self::IS_TAKER)
    }
}

impl std::ops::BitOr for WatcherFillFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl std::fmt::Debug for WatcherFillFlags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WatcherFillFlags({:#04X})", self.0)
    }
}

pub(crate) mod watcher_fill_flags {
    use super::WatcherFillFlags;

    /// Fill belongs to a short position.
    #[allow(dead_code)]
    pub(crate) const IS_SHORT: WatcherFillFlags = WatcherFillFlags::IS_SHORT;
    /// Fill opens position exposure rather than closing it.
    #[allow(dead_code)]
    pub(crate) const IS_OPEN: WatcherFillFlags = WatcherFillFlags::IS_OPEN;
    /// Fill was taker-side.
    #[allow(dead_code)]
    pub(crate) const IS_TAKER: WatcherFillFlags = WatcherFillFlags::IS_TAKER;
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
    /// Delphi `TOrderType` ordinal, preserving unknown bytes like Delphi.
    pub order_type: OrderType,
    /// Watcher fill bitmask: short/open/taker.
    pub flags: WatcherFillFlags,
}

impl WatcherFill {
    /// Return whether the fill belongs to a short position.
    pub fn is_short(&self) -> bool {
        self.flags.is_short()
    }

    /// Return whether the fill opens exposure rather than closing it.
    pub fn is_open(&self) -> bool {
        self.flags.is_open()
    }

    /// Return whether the fill was taker-side.
    pub fn is_taker(&self) -> bool {
        self.flags.is_taker()
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
    /// `data` keeps the original `Count * 20` bytes for low-level tools that
    /// want the raw wire records. Use [`crate::commands::trades_stream::parse_watcher_fills`]
    /// or [`TradeSection::watcher_fill_records`] to decode it into typed records.
    WatcherFills {
        /// Server market index from the current `GetMarketsIndexes` mapping.
        market_index: u16,
        /// HyperLiquid user address shared by all records in `data`.
        user: [u8; 20],
        /// Raw watcher-fill records, each
        /// `crate::commands::trades_stream::WATCHER_FILL_RECORD_SIZE` bytes.
        data: Vec<u8>,
    },
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
