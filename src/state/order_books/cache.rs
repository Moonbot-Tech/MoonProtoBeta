//! Per-book out-of-order packet cache.

use super::{BOOK_EXPIRED_TIMEOUT, BOOK_FULL_REQUEST_THROTTLE};
use crate::commands::order_book::{compare_seq, OrderBookUpdate};
use std::collections::VecDeque;

/// A single cached packet (out-of-order).
#[derive(Debug, Clone)]
pub(super) struct CachedPacket {
    pub(super) seq: u16,
    pub(super) pkt: OrderBookUpdate,
}

/// Per-(market_index, book_kind) cache. Matches Delphi `TOrderBookCache`.
#[derive(Debug, Clone, Default)]
pub(super) struct OrderBookCache {
    /// Next expected seq.
    pub(super) expected_seq: u16,
    /// Last applied seq. Analogue of Delphi `m.MoonProtoBookSeq[bookKind]`.
    /// The initial value 0 matters: normal-mode applies the first Diff without a
    /// Full (Delphi `MoonProtoEngine.pas:2066-2071`). Previously a `has_full: bool`
    /// was used, which forced the client to send a redundant `RequestOrderBookFull`
    /// and terminals did not show the degraded diff view.
    pub(super) last_applied_seq: u16,
    /// List of accumulated out-of-order packets, sorted by seq.
    /// `VecDeque` keeps `pop_front()` in `drain_cache` O(1) instead of the
    /// O(N) `Vec::remove(0)` shift on every packet in a recovery burst.
    pub(super) packets: VecDeque<CachedPacket>,
    /// Whether the cache is marked as corrupted (a Full is needed).
    pub(super) corrupted: bool,
    /// Time of the last sent `RequestOrderBookFull` (throttle).
    pub(super) last_full_request_ms: i64,
    /// Time when the cache became non-empty (0 if empty). Used in `is_expired`.
    pub(super) cache_not_empty_since_ms: i64,
}

impl OrderBookCache {
    /// A-17 fix: use the standard `partition_point` instead of a hand-written binary search.
    /// Wrapping-safe comparison via `compare_seq` — kept (the standard `binary_search`
    /// is unusable due to the u16 wrap-around).
    fn binary_search_insert(&mut self, seq: u16) -> usize {
        // VecDeque has no `partition_point` in ring form; make_contiguous() aligns
        // it into a single slice (O(1) if the ring is not wrapped, O(N) only when
        // resetting a wrapped state). For our N≤64 this is negligible.
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

    /// `MoonProtoOrderBook.pas:532-539 TryRequestFull` — true if a request must be sent.
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
