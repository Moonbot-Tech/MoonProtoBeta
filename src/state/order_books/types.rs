//! OrderBook public/read-model types.

use crate::commands::order_book::OrderLevel;

/// Тип orderbook'а: фьючерсы или spot. Wire-формат — 1 байт.
///
/// Соответствует Delphi `TBookKind` (MoonProtoOrderBook.pas:5) с ord-кодами
/// 0=`bk_Futures`, 1=`bk_Spot`. Используется в incoming orderbook packets,
/// full-book recovery requests and internal state keys.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderBookKind {
    /// Фьючерсный orderbook (`bk_Futures = 0`).
    Futures = 0,
    /// Spot orderbook (`bk_Spot = 1`).
    Spot = 1,
}

impl OrderBookKind {
    /// Конвертация в wire-байт (для engine_request / state cache key).
    #[inline]
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// Распарсить из wire-байт. Неизвестное значение → None (вызывающая логика
    /// должна решить — дропать пакет или fallback'нуть).
    pub fn from_u8(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::Futures),
            1 => Some(Self::Spot),
            _ => None,
        }
    }
}

/// Ключ кэша: `(market_index, book_kind)`. `book_kind`: 0=Futures, 1=Spot.
pub type BookKey = (u16, u8);

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

impl From<OrderLevel> for OrderBookLevel {
    fn from(level: OrderLevel) -> Self {
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
    pub market_index: u16,
    pub book_kind: u8,
    pub seq: u16,
    pub buys: Vec<OrderBookLevel>,
    pub sells: Vec<OrderBookLevel>,
}

impl OrderBookSnapshot {
    pub fn top(&self) -> TopOfBook {
        TopOfBook {
            bid: self.buys.first().copied(),
            ask: self.sells.first().copied(),
        }
    }
}

/// Результат применения пакета.
#[derive(Debug, Clone, PartialEq)]
pub enum ApplyResult {
    /// Пакет применён немедленно (seq == expected).
    Applied,
    /// Пакет применён из кэша (после применения младших seq).
    AppliedFromCache,
    /// Пакет положен в кэш (seq > expected, ждём промежуточных).
    Cached,
    /// Пакет stale (seq < expected) — отброшен.
    Stale,
}

/// Событие для потребителя.
#[derive(Debug, Clone)]
pub enum OrderBookEvent {
    /// Пакет применён; `OrderBooks` уже обновил applied read model.
    Apply {
        market_index: u16,
        book_kind: u8,
        is_full: bool,
        seq: u16,
        buys: Vec<OrderLevel>,
        sells: Vec<OrderLevel>,
    },
    /// Low-level control event: send `emk_RequestOrderBookFull` (throttle already
    /// applied). `EventDispatcher::dispatch_into_active` consumes this internally
    /// before invoking application callbacks.
    RequestFullNeeded { market_index: u16, book_kind: u8 },
    /// Пакет проигнорирован (stale / no full yet / cache).
    Ignored {
        market_index: u16,
        book_kind: u8,
        seq: u16,
        reason: ApplyResult,
    },
}
