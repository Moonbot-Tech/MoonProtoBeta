//! OrderBook public/read-model types.

use std::sync::Arc;

/// Orderbook kind: futures or spot. Wire format is one byte.
///
/// Matches Delphi `TBookKind` (MoonProtoOrderBook.pas:5): 0=`bk_Futures`,
/// 1=`bk_Spot`. Used in incoming orderbook packets, full-book recovery
/// requests, and internal state keys.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum OrderBookKind {
    /// Futures orderbook (`bk_Futures = 0`).
    #[default]
    Futures = 0,
    /// Spot orderbook (`bk_Spot = 1`).
    Spot = 1,
}

impl OrderBookKind {
    /// Convert to the wire byte used by Engine API requests and state keys.
    #[inline]
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// Parse a wire byte. Unknown values return `None`.
    pub fn from_u8(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::Futures),
            1 => Some(Self::Spot),
            _ => None,
        }
    }

    /// Stable lowercase label for UI logs and examples.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Futures => "futures",
            Self::Spot => "spot",
        }
    }
}

/// Internal cache key: `(market_index, raw book_kind)`.
pub(crate) type BookKey = (u16, u8);

/// One applied orderbook level stored in the client read model.
///
/// Wire packets carry `Single` (`f32`) values for compactness, while Delphi
/// applies them into `TOrderGlass` (`double`). The public snapshot follows the
/// applied-state side and stores `f64`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OrderBookLevel {
    pub rate: f64,
    pub quantity: f64,
}

impl From<crate::commands::order_book::OrderLevel> for OrderBookLevel {
    fn from(level: crate::commands::order_book::OrderLevel) -> Self {
        Self {
            rate: level.rate as f64,
            quantity: level.quantity as f64,
        }
    }
}

/// Best visible bid/ask from an applied orderbook snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct TopOfBook {
    pub bid: Option<OrderBookLevel>,
    pub ask: Option<OrderBookLevel>,
}

/// Applied current book for one `(market_index, book_kind)` pair.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct OrderBookSnapshot {
    pub(crate) market_index: u16,
    pub kind: OrderBookKind,
    pub seq: u16,
    pub buys: Vec<OrderBookLevel>,
    pub sells: Vec<OrderBookLevel>,
}

impl OrderBookSnapshot {
    /// Server-local market index retained for protocol diagnostics and custom
    /// low-level runtimes. Regular UI code should resolve books by market name
    /// through `EventDispatcherSnapshot::order_book`.
    #[inline]
    pub fn market_index(&self) -> u16 {
        self.market_index
    }

    pub fn top(&self) -> TopOfBook {
        TopOfBook {
            bid: self.buys.first().copied(),
            ask: self.sells.first().copied(),
        }
    }
}

/// Result of applying one packet to the orderbook cache.
#[derive(Debug, Clone, PartialEq)]
pub enum ApplyResult {
    /// Packet was applied immediately (`seq == expected`).
    Applied,
    /// Packet was applied from cache after earlier sequence numbers arrived.
    AppliedFromCache,
    /// Packet was cached (`seq > expected`).
    Cached,
    /// Packet was stale (`seq < expected`) and was dropped.
    Stale,
}

/// Orderbook event emitted by the read model.
#[derive(Debug, Clone)]
pub enum OrderBookEvent {
    /// Packet was applied; `OrderBooks` has already updated the read model.
    ///
    /// `market_index` is kept for diagnostics and low-level tools. Normal UI
    /// code should use `market_name`, `kind`, and `top`, or read the full
    /// applied book from the snapshot by market name.
    Apply {
        market_index: u16,
        market_name: Option<Arc<str>>,
        kind: OrderBookKind,
        is_full: bool,
        seq: u16,
        top: TopOfBook,
    },
    /// Low-level control event: send `emk_RequestOrderBookFull` (throttle already
    /// applied). `EventDispatcher::dispatch_into_active` consumes this internally
    /// before invoking application callbacks.
    #[doc(hidden)]
    RequestFullNeeded {
        market_index: u16,
        kind: OrderBookKind,
    },
    /// Packet was ignored (stale / no full yet / cache).
    #[doc(hidden)]
    Ignored {
        market_index: u16,
        kind: OrderBookKind,
        seq: u16,
        reason: ApplyResult,
    },
}

impl OrderBookEvent {
    pub fn market_index(&self) -> u16 {
        match self {
            Self::Apply { market_index, .. }
            | Self::RequestFullNeeded { market_index, .. }
            | Self::Ignored { market_index, .. } => *market_index,
        }
    }

    pub fn book_kind_raw(&self) -> u8 {
        self.kind().as_u8()
    }

    pub fn kind(&self) -> OrderBookKind {
        match self {
            Self::Apply { kind, .. }
            | Self::RequestFullNeeded { kind, .. }
            | Self::Ignored { kind, .. } => *kind,
        }
    }

    pub fn market_name(&self) -> Option<&str> {
        match self {
            Self::Apply { market_name, .. } => market_name.as_deref(),
            _ => None,
        }
    }

    pub fn top(&self) -> Option<TopOfBook> {
        match self {
            Self::Apply { top, .. } => Some(*top),
            _ => None,
        }
    }
}
