//! Per-book out-of-order packet cache.

use super::{BOOK_EXPIRED_TIMEOUT, BOOK_FULL_REQUEST_THROTTLE};
use crate::commands::order_book::{compare_seq, OrderBookUpdate};
use std::collections::VecDeque;

/// Один кэшированный пакет (out-of-order).
#[derive(Debug, Clone)]
pub(super) struct CachedPacket {
    pub(super) seq: u16,
    pub(super) pkt: OrderBookUpdate,
}

/// Per-(market_index, book_kind) кэш. Соответствует Delphi `TOrderBookCache`.
#[derive(Debug, Clone, Default)]
pub(super) struct OrderBookCache {
    /// Следующий ожидаемый seq.
    pub(super) expected_seq: u16,
    /// Последний применённый seq. Аналог Delphi `m.MoonProtoBookSeq[bookKind]`.
    /// Начальное значение 0 важно: normal-mode применяет первый Diff без Full
    /// (Delphi `MoonProtoEngine.pas:2066-2071`). Раньше использовалось `has_full: bool`,
    /// что заставляло клиента отправлять лишний `RequestOrderBookFull` и
    /// terminale не показывали degraded diff-view.
    pub(super) last_applied_seq: u16,
    /// Сортированный по seq список накопленных out-of-order пакетов.
    /// audit_rust_quality #5 + audit_robustness M5: `VecDeque` чтобы `pop_front()` в `drain_cache`
    /// был O(1) вместо O(N) на `Vec::remove(0)`. На burst recovery 64 пакета это снимает
    /// ~2080 memmove ops (64+63+62+…+1).
    pub(super) packets: VecDeque<CachedPacket>,
    /// Помечен ли кэш как corrupted (нужен Full).
    pub(super) corrupted: bool,
    /// Время последнего отправленного `RequestOrderBookFull` (throttle).
    pub(super) last_full_request_ms: i64,
    /// Время, когда кэш стал непустым (0 если пуст). Используется в `is_expired`.
    pub(super) cache_not_empty_since_ms: i64,
}

impl OrderBookCache {
    /// A-17 fix: используем стандартный `partition_point` вместо самописного binary search.
    /// Wrapping-safe сравнение через `compare_seq` — оставляем (стандартный `binary_search`
    /// не годится из-за u16 wrap-around).
    fn binary_search_insert(&mut self, seq: u16) -> usize {
        // VecDeque не имеет `partition_point` на ring-форме; make_contiguous() выравнивает
        // в один slice (O(1) если ring не wrapped, O(N) только при reset wrapped state).
        // Для нашего N≤64 — пренебрежимо.
        self.packets.make_contiguous();
        self.packets
            .as_slices()
            .0
            .partition_point(|p| compare_seq(p.seq, seq) < 0)
    }

    pub(super) fn add(&mut self, seq: u16, pkt: OrderBookUpdate, now_ms: i64) {
        if self.packets.is_empty() {
            self.cache_not_empty_since_ms = now_ms;
        }
        let pos = self.binary_search_insert(seq);
        self.packets.insert(pos, CachedPacket { seq, pkt });
    }

    pub(super) fn drop_oldest(&mut self) {
        self.packets.pop_front();
        if self.packets.is_empty() {
            self.cache_not_empty_since_ms = 0;
        }
    }

    pub(super) fn check_cache_empty(&mut self) {
        if self.packets.is_empty() {
            self.cache_not_empty_since_ms = 0;
        }
    }

    /// `MoonProtoOrderBook.pas:526-530 IsExpired`.
    pub(super) fn is_expired(&self, now_ms: i64) -> bool {
        self.cache_not_empty_since_ms > 0
            && (now_ms - self.cache_not_empty_since_ms).abs() > BOOK_EXPIRED_TIMEOUT
    }

    /// `MoonProtoOrderBook.pas:532-539 TryRequestFull` — true если надо отправить запрос.
    pub(super) fn try_request_full(&mut self, now_ms: i64) -> bool {
        if !self.corrupted {
            return false;
        }
        if (now_ms - self.last_full_request_ms).abs() <= BOOK_FULL_REQUEST_THROTTLE {
            return false;
        }
        self.last_full_request_ms = now_ms;
        true
    }

    pub(super) fn clear(&mut self) {
        self.packets.clear();
        self.cache_not_empty_since_ms = 0;
    }
}
