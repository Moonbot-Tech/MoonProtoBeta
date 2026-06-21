//! OrderBook public/read-model types.

use std::sync::Arc;

pub type OrderBookReadGuard =
    parking_lot::ArcRwLockReadGuard<parking_lot::RawRwLock, OrderBookSnapshot>;

/// Orderbook kind: futures or spot. Wire format is one byte.
///
/// Wire values are stable: `0` = futures book, `1` = spot book. Used in incoming
/// orderbook packets, full-book recovery requests, and internal state keys.
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
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// Convert to the wire byte used by Engine API requests and state keys.
    #[inline]
    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) fn as_u8(self) -> u8 {
        self as u8
    }

    /// Parse a wire byte. Unknown values return `None`.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn from_u8(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::Futures),
            1 => Some(Self::Spot),
            _ => None,
        }
    }

    /// Parse a wire byte. Unknown values return `None`.
    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) fn from_u8(b: u8) -> Option<Self> {
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

/// Internal orderbook side effects produced by cache/recovery state.
pub(crate) enum OrderBookControl {
    RequestFullNeeded {
        market_index: u16,
        kind: OrderBookKind,
    },
}

/// One applied orderbook level stored in the client read model.
///
/// Wire packets carry compact `f32` values. The retained read-model stores
/// `f64`, matching the applied-state side used by chart/orderbook UI code.
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

/// Applied current book for one market/book-kind pair.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct OrderBookSnapshot {
    pub(crate) market_index: u16,
    pub kind: OrderBookKind,
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub seq: u16,
    pub(crate) revision: u64,
    pub buys: Vec<OrderBookLevel>,
    pub sells: Vec<OrderBookLevel>,
}

impl OrderBookSnapshot {
    pub(crate) fn mark_applied(&mut self, seq: u16) {
        #[cfg(any(test, feature = "diagnostics"))]
        {
            self.seq = seq;
        }
        #[cfg(not(any(test, feature = "diagnostics")))]
        let _ = seq;
        self.revision = self.revision.wrapping_add(1);
    }

    /// Monotonic local revision of this applied current book.
    ///
    /// The value changes every time MoonProto applies a full or diff packet to
    /// this retained book. UI code can keep the last seen revision and skip
    /// expensive orderbook rebuilds while it stays unchanged. It is local to
    /// the current client runtime and is not a wire sequence number.
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Server-local market index retained for protocol diagnostics and custom
    /// low-level runtimes. Regular UI code should resolve books by market name
    /// once, keep `MarketHandle`, and read through `MoonStateSnapshot::order_book_for`.
    #[inline]
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
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

/// Diagnostic reason for a packet that did not update the visible orderbook.
#[derive(Debug, Clone, PartialEq)]
#[cfg(any(test, feature = "diagnostics"))]
pub enum ApplyResult {
    /// Packet was cached (`seq > expected`).
    Cached,
    /// Packet was stale (`seq < expected`) and was dropped.
    Stale,
}

/// Diagnostic reason for a packet that did not update the visible orderbook.
#[derive(Debug, Clone, PartialEq)]
#[cfg(not(any(test, feature = "diagnostics")))]
pub(crate) enum ApplyResult {
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
    /// applied book from the snapshot by retained `MarketHandle`.
    Apply {
        #[cfg(any(test, feature = "diagnostics"))]
        #[doc(hidden)]
        market_index: u16,
        market_name: Option<Arc<str>>,
        kind: OrderBookKind,
        is_full: bool,
        #[cfg(any(test, feature = "diagnostics"))]
        #[doc(hidden)]
        seq: u16,
        top: TopOfBook,
    },
    /// Low-level control event: send `emk_RequestOrderBookFull` (throttle already
    /// applied). `EventDispatcher::dispatch_into_active` consumes this internally
    /// before invoking application callbacks.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    RequestFullNeeded {
        market_index: u16,
        kind: OrderBookKind,
    },
    /// Packet was ignored (stale / no full yet / cache).
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    Ignored {
        market_index: u16,
        kind: OrderBookKind,
        seq: u16,
        reason: ApplyResult,
    },
}

impl OrderBookEvent {
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn market_index(&self) -> u16 {
        match self {
            Self::Apply { market_index, .. } => *market_index,
            #[cfg(any(test, feature = "diagnostics"))]
            Self::RequestFullNeeded { market_index, .. } => *market_index,
            #[cfg(any(test, feature = "diagnostics"))]
            Self::Ignored { market_index, .. } => *market_index,
        }
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn book_kind_raw(&self) -> u8 {
        self.kind().as_u8()
    }

    pub fn kind(&self) -> OrderBookKind {
        match self {
            Self::Apply { kind, .. } => *kind,
            #[cfg(any(test, feature = "diagnostics"))]
            Self::RequestFullNeeded { kind, .. } => *kind,
            #[cfg(any(test, feature = "diagnostics"))]
            Self::Ignored { kind, .. } => *kind,
        }
    }

    pub fn market_name(&self) -> Option<&str> {
        match self {
            Self::Apply { market_name, .. } => market_name.as_deref(),
            #[cfg(any(test, feature = "diagnostics"))]
            _ => None,
        }
    }

    pub fn top(&self) -> Option<TopOfBook> {
        match self {
            Self::Apply { top, .. } => Some(*top),
            #[cfg(any(test, feature = "diagnostics"))]
            _ => None,
        }
    }
}
