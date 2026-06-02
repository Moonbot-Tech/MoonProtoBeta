//! OrderBook sync state: reordering buffer, gap detection, and auto full-refresh request.
//!
//! Delphi source: `MoonProtoOrderBook.pas:32-720`
//! (`TOrderBookCache` plus `MoonProto_TryApplyCached`).
//!
//! What this module does:
//!
//! The server sends `MPC_OrderBook` packets with a wrapping `seq: u16`.
//! When a UDP packet is lost, the cache accumulates out-of-order packets. If a
//! cache remains non-empty longer than `BOOK_EXPIRED_TIMEOUT`, it becomes
//! corrupted and the client requests a full orderbook snapshot, throttled to 5s.
//!
//! Each `(market_index, book_kind)` pair has an independent cache.
//!
//! Applications read current books through `MoonStateSnapshot` helpers by market
//! name or retained `MarketHandle`. Packet apply/cache recovery is owned by the
//! Active Lib runtime, not by terminal UI code.

use std::collections::HashMap;
use std::sync::Arc;

use crate::commands::order_book::{compare_seq, OrderBookUpdate};
use crate::state::eps::EpsProfile;

mod apply;
mod cache;
mod types;

#[cfg(test)]
pub(crate) use self::apply::apply_order_book_diff_keep_zero;
use self::apply::{apply_cached_packet, apply_diff_book, apply_full_book};
use self::cache::OrderBookCache;
pub(crate) use self::types::{ApplyResult, BookKey, OrderBookControl};
pub use self::types::{
    OrderBookEvent, OrderBookKind, OrderBookLevel, OrderBookSnapshot, TopOfBook,
};

/// Cache becomes corrupted if it stays non-empty longer than this threshold.
/// Matches `MoonProtoOrderBook.pas:9 BOOK_EXPIRED_TIMEOUT = 800`.
const BOOK_EXPIRED_TIMEOUT: i64 = 800;

/// Minimum interval between full-snapshot requests.
/// Matches `MoonProtoOrderBook.pas:10 BOOK_FULL_REQUEST_THROTTLE = 5000`.
const BOOK_FULL_REQUEST_THROTTLE: i64 = 5000;

/// Cache size limit. Matches `MoonProtoOrderBook.pas:11 BOOK_CACHE_MAX_PACKETS = 64`.
const BOOK_CACHE_MAX_PACKETS: usize = 64;

/// Orderbook sync state with one cache per `(market_index, book_kind)`.
#[derive(Debug, Clone, Default)]
pub struct OrderBooks {
    caches: HashMap<BookKey, OrderBookCache>,
    books: HashMap<BookKey, Arc<OrderBookSnapshot>>,
    diff_scratch: Vec<OrderBookLevel>,
    eps_profile: EpsProfile,
}

impl OrderBooks {
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self {
            caches: HashMap::new(),
            books: HashMap::new(),
            diff_scratch: Vec::new(),
            eps_profile: EpsProfile::default(),
        }
    }

    pub(crate) fn set_eps_profile(&mut self, eps_profile: EpsProfile) {
        self.eps_profile = eps_profile;
    }

    /// Process one decoded `MPC_OrderBook` packet and return generated events.
    #[must_use = "OrderBookEvent values must be processed; ignoring RequestFullNeeded can leave a low-level orderbook corrupted"]
    #[cfg(test)]
    pub(crate) fn on_packet(&mut self, pkt: OrderBookUpdate, now_ms: i64) -> Vec<OrderBookEvent> {
        let mut events = Vec::new();
        let mut controls = Vec::new();
        self.on_packet_into(pkt, now_ms, &mut events, &mut controls);
        events
    }

    /// Process one packet into a caller-owned event buffer.
    pub(crate) fn on_packet_into(
        &mut self,
        pkt: OrderBookUpdate,
        now_ms: i64,
        events: &mut Vec<OrderBookEvent>,
        controls: &mut Vec<OrderBookControl>,
    ) {
        let key: BookKey = (pkt.market_index, pkt.book_kind);
        let kind = OrderBookKind::from_u8(pkt.book_kind).unwrap_or(OrderBookKind::Futures);

        let cache = self.caches.entry(key).or_default();

        // === 1. Full snapshot — always applied (this is a cache reset) ===
        if pkt.is_full {
            let top = apply_full_book(&mut self.books, key, pkt.seq, &pkt.buys, &pkt.sells);
            cache.corrupted = false;
            cache.last_applied_seq = pkt.seq;
            cache.expected_seq = pkt.seq.wrapping_add(1);
            // Clean the cache of stale seq < expected_seq.
            cache
                .packets
                .retain(|p| compare_seq(p.seq, cache.expected_seq) >= 0);
            cache.check_cache_empty();
            events.push(OrderBookEvent::Apply {
                #[cfg(any(test, feature = "diagnostics"))]
                market_index: pkt.market_index,
                market_name: None,
                kind,
                is_full: true,
                #[cfg(any(test, feature = "diagnostics"))]
                seq: pkt.seq,
                top,
            });
            // Try to apply the accumulated diffs from the cache.
            self.drain_cache(key, events);
            return;
        }

        // === 2. Corrupted mode — Delphi MoonProtoEngine.pas:2021-2039 ===
        // While waiting for a fresh Full snapshot, apply incoming diffs as-is for
        // a degraded live view + keep requesting Full (throttle). Previously diffs
        // were dropped in this mode — the UI froze while waiting.
        if cache.corrupted {
            let seq = pkt.seq;
            let cached_pkt = pkt.clone();
            let top = apply_diff_book(
                &mut self.books,
                &mut self.diff_scratch,
                key,
                seq,
                &pkt.buys,
                &pkt.sells,
                self.eps_profile,
            );
            cache.last_applied_seq = seq;
            events.push(OrderBookEvent::Apply {
                #[cfg(any(test, feature = "diagnostics"))]
                market_index: pkt.market_index,
                market_name: None,
                kind,
                is_full: false,
                #[cfg(any(test, feature = "diagnostics"))]
                seq,
                top,
            });
            if cache.packets.len() >= BOOK_CACHE_MAX_PACKETS {
                cache.drop_oldest();
            }
            cache.add(seq, cached_pkt, now_ms);
            if cache.try_request_full(now_ms) {
                push_request_full_needed(events, controls, key.0, kind);
            }
            return;
        }

        let cmp = compare_seq(pkt.seq, cache.expected_seq);

        // === 3. In-order OR first Diff without a Full ===
        // Delphi `MoonProtoEngine.pas:2066-2071`: if `MoonProtoBookSeq = 0`
        // (last applied seq = 0) — apply the first Diff without waiting for a
        // Full. Previously we dropped it → an extra RequestFullNeeded request.
        if cmp == 0 || cache.last_applied_seq == 0 {
            let top = apply_diff_book(
                &mut self.books,
                &mut self.diff_scratch,
                key,
                pkt.seq,
                &pkt.buys,
                &pkt.sells,
                self.eps_profile,
            );
            cache.expected_seq = pkt.seq.wrapping_add(1);
            cache.last_applied_seq = pkt.seq;
            events.push(OrderBookEvent::Apply {
                #[cfg(any(test, feature = "diagnostics"))]
                market_index: pkt.market_index,
                market_name: None,
                kind,
                is_full: false,
                #[cfg(any(test, feature = "diagnostics"))]
                seq: pkt.seq,
                top,
            });
            // The cache may hold the following seq values — drain.
            self.drain_cache(key, events);
            return;
        }

        // === 4. Stale: seq < expected → drop ===
        if cmp < 0 {
            push_ignored(events, pkt.market_index, kind, pkt.seq, ApplyResult::Stale);
            return;
        }

        // === 5. Gap: seq > expected → put in cache, check expired/corrupted ===
        let seq = pkt.seq;
        cache.add(seq, pkt.clone(), now_ms);
        if cache.is_expired(now_ms) || cache.packets.len() > BOOK_CACHE_MAX_PACKETS {
            cache.corrupted = true;
        }
        if cache.try_request_full(now_ms) {
            push_request_full_needed(events, controls, key.0, kind);
        }
        push_ignored(events, pkt.market_index, kind, seq, ApplyResult::Cached);
    }

    /// Drain cache by applying consecutive packets starting at `expected_seq`.
    /// Matches `MoonProto_TryApplyCached` in `MoonProtoOrderBook.pas:682-720`.
    fn drain_cache(&mut self, key: BookKey, events: &mut Vec<OrderBookEvent>) {
        let cache = match self.caches.get_mut(&key) {
            Some(c) => c,
            None => return,
        };

        // Drop garbage (seq < expected).
        cache
            .packets
            .retain(|p| compare_seq(p.seq, cache.expected_seq) >= 0);

        while let Some(p) = cache.packets.front() {
            if p.seq != cache.expected_seq {
                // A gap remains — stop.
                break;
            }

            // O(1) pop_front instead of O(N) remove(0).
            let entry = cache.packets.pop_front().unwrap();
            let top = apply_cached_packet(
                &mut self.books,
                &mut self.diff_scratch,
                key,
                &entry.pkt,
                self.eps_profile,
            );
            cache.expected_seq = entry.seq.wrapping_add(1);
            cache.last_applied_seq = entry.seq;
            events.push(OrderBookEvent::Apply {
                #[cfg(any(test, feature = "diagnostics"))]
                market_index: entry.pkt.market_index,
                market_name: None,
                kind: OrderBookKind::from_u8(entry.pkt.book_kind).unwrap_or(OrderBookKind::Futures),
                is_full: entry.pkt.is_full,
                #[cfg(any(test, feature = "diagnostics"))]
                seq: entry.seq,
                top,
            });
        }
        cache.check_cache_empty();
    }

    /// Delphi `TMoonProtoEngine.ResetOrderBookCaches`: clear out-of-order
    /// caches and reset per-book sequence state without wiping the visible book
    /// levels. `BookSubbed` lives in `Client`'s subscription registry, so the
    /// Rust analogue resets all local cache entries; absent entries will be
    /// recreated with seq=0 on the next packet.
    pub(crate) fn reset_caches_keep_books(&mut self) {
        for (_, c) in self.caches.iter_mut() {
            c.clear();
            c.corrupted = false;
            c.expected_seq = 0;
            c.last_applied_seq = 0;
        }
        self.caches.clear();
    }

    /// Number of active caches.
    pub fn len(&self) -> usize {
        self.caches.len()
    }

    pub fn is_empty(&self) -> bool {
        self.caches.is_empty()
    }

    /// Get the applied current book by raw wire kind (`0 = futures`, `1 = spot`).
    pub(crate) fn book_by_kind(
        &self,
        market_index: u16,
        book_kind: u8,
    ) -> Option<&OrderBookSnapshot> {
        self.books.get(&(market_index, book_kind)).map(Arc::as_ref)
    }

    /// Get the applied current book.
    pub(crate) fn book(
        &self,
        market_index: u16,
        book_kind: OrderBookKind,
    ) -> Option<&OrderBookSnapshot> {
        self.book_by_kind(market_index, book_kind.as_u8())
    }
}

fn push_request_full_needed(
    events: &mut Vec<OrderBookEvent>,
    controls: &mut Vec<OrderBookControl>,
    market_index: u16,
    kind: OrderBookKind,
) {
    controls.push(OrderBookControl::RequestFullNeeded { market_index, kind });
    #[cfg(any(test, feature = "diagnostics"))]
    events.push(OrderBookEvent::RequestFullNeeded { market_index, kind });
    #[cfg(not(any(test, feature = "diagnostics")))]
    let _ = events;
}

fn push_ignored(
    events: &mut Vec<OrderBookEvent>,
    market_index: u16,
    kind: OrderBookKind,
    seq: u16,
    reason: ApplyResult,
) {
    #[cfg(any(test, feature = "diagnostics"))]
    events.push(OrderBookEvent::Ignored {
        market_index,
        kind,
        seq,
        reason,
    });
    #[cfg(not(any(test, feature = "diagnostics")))]
    let _ = (events, market_index, kind, seq, reason);
}

#[cfg(test)]
mod tests;
