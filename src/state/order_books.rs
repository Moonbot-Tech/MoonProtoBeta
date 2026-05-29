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
//! Low-level usage:
//!
//! ```ignore
//! let mut state = OrderBooks::new();
//! let events = state.on_packet(packet);
//! for ev in events {
//!     match ev {
//!         OrderBookEvent::Apply { top, .. } => /* redraw best bid/ask or read state.book(...) */,
//!         OrderBookEvent::RequestFullNeeded { market_index, kind } => {
//!             // Low-level mode only: send emk_RequestOrderBookFull yourself.
//!             // EventDispatcher::dispatch_into_active consumes this internally.
//!             let req = commands::engine_request::request_order_book_full(market_index, kind.as_u8());
//!             client.send_api_request(&req);
//!         }
//!     }
//! }
//! ```

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
pub use self::types::{
    ApplyResult, BookKey, OrderBookEvent, OrderBookKind, OrderBookLevel, OrderBookSnapshot,
    TopOfBook,
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
    pub fn new() -> Self {
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
    pub fn on_packet(&mut self, pkt: OrderBookUpdate, now_ms: i64) -> Vec<OrderBookEvent> {
        let mut events = Vec::new();
        self.on_packet_into(pkt, now_ms, &mut events);
        events
    }

    /// Process one packet into a caller-owned event buffer.
    pub fn on_packet_into(
        &mut self,
        pkt: OrderBookUpdate,
        now_ms: i64,
        events: &mut Vec<OrderBookEvent>,
    ) {
        let key: BookKey = (pkt.market_index, pkt.book_kind);
        let kind = OrderBookKind::from_u8(pkt.book_kind).unwrap_or(OrderBookKind::Futures);

        let cache = self.caches.entry(key).or_default();

        // === 1. Full snapshot — всегда применяется (это reset кэша) ===
        if pkt.is_full {
            apply_full_book(&mut self.books, key, pkt.seq, &pkt.buys, &pkt.sells);
            cache.corrupted = false;
            cache.last_applied_seq = pkt.seq;
            cache.expected_seq = pkt.seq.wrapping_add(1);
            // Чистим cache от старых seq < expected_seq.
            cache
                .packets
                .retain(|p| compare_seq(p.seq, cache.expected_seq) >= 0);
            cache.check_cache_empty();
            events.push(OrderBookEvent::Apply {
                market_index: pkt.market_index,
                market_name: None,
                kind,
                is_full: true,
                seq: pkt.seq,
                top: self
                    .books
                    .get(&key)
                    .map(|book| book.top())
                    .unwrap_or_default(),
                buys: pkt.buys,
                sells: pkt.sells,
            });
            // Попробовать применить накопленные diff из cache.
            self.drain_cache(key, events);
            return;
        }

        // === 2. Corrupted mode — Delphi MoonProtoEngine.pas:2021-2039 ===
        // Пока ждём свежий Full snapshot, применяем приходящие diff'ы as-is для
        // degraded live view + продолжаем требовать Full (throttle). Раньше Diff
        // в этом режиме отбрасывались — UI замораживался на время ожидания.
        if cache.corrupted {
            let seq = pkt.seq;
            let cached_pkt = pkt.clone();
            apply_diff_book(
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
                market_index: pkt.market_index,
                market_name: None,
                kind,
                is_full: false,
                seq,
                top: self
                    .books
                    .get(&key)
                    .map(|book| book.top())
                    .unwrap_or_default(),
                buys: pkt.buys,
                sells: pkt.sells,
            });
            if cache.packets.len() >= BOOK_CACHE_MAX_PACKETS {
                cache.drop_oldest();
            }
            cache.add(seq, cached_pkt, now_ms);
            if cache.try_request_full(now_ms) {
                events.push(OrderBookEvent::RequestFullNeeded {
                    market_index: key.0,
                    kind,
                });
            }
            return;
        }

        let cmp = compare_seq(pkt.seq, cache.expected_seq);

        // === 3. In-order OR первый Diff без Full ===
        // Delphi `MoonProtoEngine.pas:2066-2071`: если `MoonProtoBookSeq = 0`
        // (последний применённый seq = 0) — применяет первый Diff без ожидания
        // Full. Раньше мы отбрасывали → лишний RequestFullNeeded request.
        if cmp == 0 || cache.last_applied_seq == 0 {
            apply_diff_book(
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
                market_index: pkt.market_index,
                market_name: None,
                kind,
                is_full: false,
                seq: pkt.seq,
                top: self
                    .books
                    .get(&key)
                    .map(|book| book.top())
                    .unwrap_or_default(),
                buys: pkt.buys,
                sells: pkt.sells,
            });
            // Может быть в cache следующие seq — drain.
            self.drain_cache(key, events);
            return;
        }

        // === 4. Stale: seq < expected → отброс ===
        if cmp < 0 {
            events.push(OrderBookEvent::Ignored {
                market_index: pkt.market_index,
                kind,
                seq: pkt.seq,
                reason: ApplyResult::Stale,
            });
            return;
        }

        // === 5. Gap: seq > expected → положить в cache, проверить expired/corrupted ===
        let seq = pkt.seq;
        cache.add(seq, pkt.clone(), now_ms);
        if cache.is_expired(now_ms) || cache.packets.len() > BOOK_CACHE_MAX_PACKETS {
            cache.corrupted = true;
        }
        if cache.try_request_full(now_ms) {
            events.push(OrderBookEvent::RequestFullNeeded {
                market_index: key.0,
                kind,
            });
        }
        events.push(OrderBookEvent::Ignored {
            market_index: pkt.market_index,
            kind,
            seq,
            reason: ApplyResult::Cached,
        });
    }

    /// Drain cache by applying consecutive packets starting at `expected_seq`.
    /// Matches `MoonProto_TryApplyCached` in `MoonProtoOrderBook.pas:682-720`.
    fn drain_cache(&mut self, key: BookKey, events: &mut Vec<OrderBookEvent>) {
        let cache = match self.caches.get_mut(&key) {
            Some(c) => c,
            None => return,
        };

        // Удалить мусор (seq < expected).
        cache
            .packets
            .retain(|p| compare_seq(p.seq, cache.expected_seq) >= 0);

        while let Some(p) = cache.packets.front() {
            if p.seq != cache.expected_seq {
                // Gap остался — остановиться.
                break;
            }

            // O(1) pop_front вместо O(N) remove(0).
            let entry = cache.packets.pop_front().unwrap();
            apply_cached_packet(
                &mut self.books,
                &mut self.diff_scratch,
                key,
                &entry.pkt,
                self.eps_profile,
            );
            cache.expected_seq = entry.seq.wrapping_add(1);
            cache.last_applied_seq = entry.seq;
            events.push(OrderBookEvent::Apply {
                market_index: entry.pkt.market_index,
                market_name: None,
                kind: OrderBookKind::from_u8(entry.pkt.book_kind).unwrap_or(OrderBookKind::Futures),
                is_full: entry.pkt.is_full,
                seq: entry.seq,
                top: self
                    .books
                    .get(&key)
                    .map(|book| book.top())
                    .unwrap_or_default(),
                buys: entry.pkt.buys,
                sells: entry.pkt.sells,
            });
        }
        cache.check_cache_empty();
    }

    /// Clear the whole state, for example on reconnect or `WantNewHello`.
    pub fn clear(&mut self) {
        for (_, c) in self.caches.iter_mut() {
            c.clear();
            c.corrupted = false;
            c.expected_seq = 0;
            c.last_applied_seq = 0;
        }
        self.caches.clear();
        self.books.clear();
        self.diff_scratch.clear();
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
    pub fn book(&self, market_index: u16, book_kind: OrderBookKind) -> Option<&OrderBookSnapshot> {
        self.book_by_kind(market_index, book_kind.as_u8())
    }

    /// Get best bid/ask from the applied current book.
    pub fn top_of_book(&self, market_index: u16, book_kind: OrderBookKind) -> Option<TopOfBook> {
        self.book(market_index, book_kind).map(|book| book.top())
    }

    /// Iterate over applied current books.
    pub fn iter_books(&self) -> impl Iterator<Item = &OrderBookSnapshot> {
        self.books.values().map(Arc::as_ref)
    }
}

#[cfg(test)]
mod tests;
