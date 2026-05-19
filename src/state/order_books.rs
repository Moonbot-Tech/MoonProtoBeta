//! OrderBook sync state — reordering buffer + gap detection + auto-request-full.
//!
//! Источник Delphi: `MoonProtoOrderBook.pas:32-720` (TOrderBookCache + MoonProto_TryApplyCached).
//!
//! ## Что делает этот модуль
//!
//! Сервер шлёт `MPC_OrderBook` пакеты с `seq:u16` (wrapping). При потере UDP-пакета пропадает
//! seq → кэш накапливает out-of-order пакеты. Если cache долго непустой (> `BOOK_EXPIRED_TIMEOUT`),
//! помечается corrupted → клиент отправляет `emk_RequestOrderBookFull` (throttle 5s).
//!
//! Каждый (market_index, book_kind) имеет свой кэш — pairs обслуживаются независимо.
//!
//! ## Использование
//!
//! ```ignore
//! let mut state = OrderBooks::new();
//! let events = state.on_packet(packet);
//! for ev in events {
//!     match ev {
//!         OrderBookEvent::Apply { .. } => /* apply to local model */,
//!         OrderBookEvent::RequestFullNeeded { market_index, book_kind } => {
//!             // отправить emk_RequestOrderBookFull через client.send_api_request
//!             let req = commands::engine_request::request_order_book_full(market_index, book_kind);
//!             client.send_api_request(&req);
//!         }
//!     }
//! }
//! ```

use std::collections::{HashMap, VecDeque};
use crate::commands::order_book::{OrderBookUpdate, compare_seq};

/// Кэш считается corrupted, если непустой дольше этого порога (мс).
/// Соответствует `MoonProtoOrderBook.pas:9` `BOOK_EXPIRED_TIMEOUT = 800`.
const BOOK_EXPIRED_TIMEOUT: i64 = 800;

/// Минимальный интервал между запросами Full snapshot (мс).
/// Соответствует `MoonProtoOrderBook.pas:10` `BOOK_FULL_REQUEST_THROTTLE = 5000`.
const BOOK_FULL_REQUEST_THROTTLE: i64 = 5000;

/// Лимит размера кэша. Соответствует `MoonProtoOrderBook.pas:11` `BOOK_CACHE_MAX_PACKETS = 64`.
const BOOK_CACHE_MAX_PACKETS: usize = 64;

/// Ключ кэша: `(market_index, book_kind)`. `book_kind`: 0=Futures, 1=Spot.
pub type BookKey = (u16, u8);

/// Один кэшированный пакет (out-of-order).
#[derive(Debug, Clone)]
struct CachedPacket {
    seq: u16,
    pkt: OrderBookUpdate,
}

/// Per-(market_index, book_kind) кэш. Соответствует Delphi `TOrderBookCache`.
#[derive(Debug, Default)]
struct OrderBookCache {
    /// Следующий ожидаемый seq.
    expected_seq: u16,
    /// Был ли получен Full snapshot. До первого Full — все Diff игнорируются.
    has_full: bool,
    /// Сортированный по seq список накопленных out-of-order пакетов.
    /// audit_rust_quality #5 + audit_robustness M5: `VecDeque` чтобы `pop_front()` в `drain_cache`
    /// был O(1) вместо O(N) на `Vec::remove(0)`. На burst recovery 64 пакета это снимает
    /// ~2080 memmove ops (64+63+62+…+1).
    packets: VecDeque<CachedPacket>,
    /// Помечен ли кэш как corrupted (нужен Full).
    corrupted: bool,
    /// Время последнего отправленного `RequestOrderBookFull` (throttle).
    last_full_request_ms: i64,
    /// Время, когда кэш стал непустым (0 если пуст). Используется в `is_expired`.
    cache_not_empty_since_ms: i64,
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
        self.packets.as_slices().0.partition_point(|p| compare_seq(p.seq, seq) < 0)
    }

    fn add(&mut self, seq: u16, pkt: OrderBookUpdate, now_ms: i64) {
        if self.packets.is_empty() {
            self.cache_not_empty_since_ms = now_ms;
        }
        let pos = self.binary_search_insert(seq);
        // Защита от дубликатов: не добавлять если уже есть тот же seq.
        if pos < self.packets.len() && self.packets[pos].seq == seq {
            return;
        }
        self.packets.insert(pos, CachedPacket { seq, pkt });

        // D-V2-11 fix: при переполнении кэша **не дропаем середину** sequence (раньше
        // `remove(0)` снимало самый старый = самый нужный для recovery → скрытые gap'ы
        // в orderbook → corrupted UI state). Вместо этого — помечаем corrupted + сбрасываем
        // кэш. На следующем packet'е orderbook_cache_handler запросит fresh Full snapshot.
        if self.packets.len() > BOOK_CACHE_MAX_PACKETS {
            log::warn!(target: "moonproto::order_books",
                "OrderBook cache overflow ({} packets) — clearing + will request Full",
                self.packets.len());
            self.corrupted = true;
            self.packets.clear();
            self.cache_not_empty_since_ms = 0;
        }
    }

    fn check_cache_empty(&mut self) {
        if self.packets.is_empty() {
            self.cache_not_empty_since_ms = 0;
        }
    }

    /// `MoonProtoOrderBook.pas:526-530 IsExpired`.
    fn is_expired(&self, now_ms: i64) -> bool {
        self.cache_not_empty_since_ms > 0
            && (now_ms - self.cache_not_empty_since_ms).abs() > BOOK_EXPIRED_TIMEOUT
    }

    /// `MoonProtoOrderBook.pas:532-539 TryRequestFull` — true если надо отправить запрос.
    /// `last_full_request_ms == 0` означает "никогда не запрашивали" — пропускаем throttle.
    fn try_request_full(&mut self, now_ms: i64) -> bool {
        if !self.corrupted {
            return false;
        }
        if self.last_full_request_ms != 0
            && (now_ms - self.last_full_request_ms).abs() <= BOOK_FULL_REQUEST_THROTTLE
        {
            return false;
        }
        self.last_full_request_ms = now_ms;
        true
    }

    fn clear(&mut self) {
        self.packets.clear();
        self.cache_not_empty_since_ms = 0;
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
    /// Diff пришёл до Full — отброшен (нужен Full).
    NoFullYet,
}

/// Событие для потребителя.
#[derive(Debug, Clone)]
pub enum OrderBookEvent {
    /// Пакет применён — потребитель должен обновить локальный orderbook.
    Apply {
        market_index: u16,
        book_kind: u8,
        is_full: bool,
        seq: u16,
        buys: Vec<crate::commands::order_book::OrderLevel>,
        sells: Vec<crate::commands::order_book::OrderLevel>,
    },
    /// Клиент должен отправить `emk_RequestOrderBookFull` (throttle уже учтён).
    /// Используй `commands::engine_request::request_order_book_full(market_index, book_kind)`.
    RequestFullNeeded {
        market_index: u16,
        book_kind: u8,
    },
    /// Пакет проигнорирован (stale / no full yet / cache).
    Ignored {
        market_index: u16,
        book_kind: u8,
        seq: u16,
        reason: ApplyResult,
    },
}

/// DoS guard: верхний лимит уникальных (market_index, book_kind) ключей.
/// На реальной бирже сотни маркетов × 2 book_kind = единицы тысяч. 4096 — щедрый запас,
/// закрывает медленный grow-attack когда злой/багнутый сервер отправляет Diff пакеты
/// с разными market_index чтобы наполнить HashMap до OOM.
pub const MAX_ORDERBOOK_CACHES: usize = 4096;

/// Главный sync state — кэш на каждый (market_index, book_kind).
#[derive(Debug, Default)]
pub struct OrderBooks {
    caches: HashMap<BookKey, OrderBookCache>,
}

impl OrderBooks {
    pub fn new() -> Self {
        Self { caches: HashMap::new() }
    }

    /// Обработать один распарсенный `MPC_OrderBook` пакет.
    /// `now_ms` — текущее время в миллисекундах (из `client.now_ms()`).
    /// Возвращает список событий: Apply (может быть несколько при разгребании cache), RequestFullNeeded, Ignored.
    pub fn on_packet(&mut self, pkt: OrderBookUpdate, now_ms: i64) -> Vec<OrderBookEvent> {
        let key: BookKey = (pkt.market_index, pkt.book_kind);
        let mut events = Vec::new();

        // DoS guard: если caches заполнен и пришёл пакет на новый ключ — выкидываем самый
        // старый по last_full_request_ms (LRU-like). Защита от slow-grow attack через distinct
        // market_index. На реальной бирже размер << MAX_ORDERBOOK_CACHES.
        if !self.caches.contains_key(&key) && self.caches.len() >= MAX_ORDERBOOK_CACHES {
            if let Some((evict_key, _)) = self.caches.iter()
                .min_by_key(|(_, c)| c.last_full_request_ms)
                .map(|(k, c)| (*k, c.last_full_request_ms))
            {
                log::warn!(target: "moonproto::order_books",
                    "caches saturated ({}); evicting {:?} (LRU)", self.caches.len(), evict_key);
                self.caches.remove(&evict_key);
            }
        }

        let cache = self.caches.entry(key).or_default();

        // === 1. Full snapshot — всегда применяется (это reset кэша) ===
        if pkt.is_full {
            cache.has_full = true;
            cache.corrupted = false;
            cache.expected_seq = pkt.seq.wrapping_add(1);
            // Чистим cache от старых seq < expected_seq.
            cache.packets.retain(|p| compare_seq(p.seq, cache.expected_seq) >= 0);
            cache.check_cache_empty();
            events.push(OrderBookEvent::Apply {
                market_index: pkt.market_index,
                book_kind: pkt.book_kind,
                is_full: true,
                seq: pkt.seq,
                buys: pkt.buys,
                sells: pkt.sells,
            });
            // Попробовать применить накопленные diff из cache.
            self.drain_cache(key, now_ms, &mut events);
            return events;
        }

        // === 2. Diff до первого Full — игнорируем + запрашиваем Full ===
        if !cache.has_full {
            cache.corrupted = true;
            if cache.try_request_full(now_ms) {
                events.push(OrderBookEvent::RequestFullNeeded {
                    market_index: pkt.market_index,
                    book_kind: pkt.book_kind,
                });
            }
            events.push(OrderBookEvent::Ignored {
                market_index: pkt.market_index,
                book_kind: pkt.book_kind,
                seq: pkt.seq,
                reason: ApplyResult::NoFullYet,
            });
            return events;
        }

        let cmp = compare_seq(pkt.seq, cache.expected_seq);

        // === 3. Stale: seq < expected → отброс ===
        if cmp < 0 {
            events.push(OrderBookEvent::Ignored {
                market_index: pkt.market_index,
                book_kind: pkt.book_kind,
                seq: pkt.seq,
                reason: ApplyResult::Stale,
            });
            return events;
        }

        // === 4. In-order: seq == expected → применить ===
        if cmp == 0 {
            cache.expected_seq = pkt.seq.wrapping_add(1);
            events.push(OrderBookEvent::Apply {
                market_index: pkt.market_index,
                book_kind: pkt.book_kind,
                is_full: false,
                seq: pkt.seq,
                buys: pkt.buys,
                sells: pkt.sells,
            });
            // Может быть в cache следующие seq — drain.
            self.drain_cache(key, now_ms, &mut events);
            return events;
        }

        // === 5. Gap: seq > expected → положить в cache, проверить expired/corrupted ===
        let seq = pkt.seq;
        cache.add(seq, pkt.clone(), now_ms);
        if cache.is_expired(now_ms) {
            cache.corrupted = true;
        }
        if cache.try_request_full(now_ms) {
            events.push(OrderBookEvent::RequestFullNeeded {
                market_index: key.0,
                book_kind: key.1,
            });
        }
        events.push(OrderBookEvent::Ignored {
            market_index: pkt.market_index,
            book_kind: pkt.book_kind,
            seq,
            reason: ApplyResult::Cached,
        });
        events
    }

    /// Разгрести cache — применить все последовательные пакеты `expected_seq, +1, +2 ...`.
    /// Соответствует `MoonProto_TryApplyCached` (MoonProtoOrderBook.pas:682-720).
    fn drain_cache(&mut self, key: BookKey, _now_ms: i64, events: &mut Vec<OrderBookEvent>) {
        let cache = match self.caches.get_mut(&key) {
            Some(c) => c,
            None => return,
        };

        // Удалить мусор (seq < expected).
        cache.packets.retain(|p| compare_seq(p.seq, cache.expected_seq) >= 0);

        loop {
            let head_seq = match cache.packets.front() {
                Some(p) => p.seq,
                None => break,
            };
            if head_seq == cache.expected_seq {
                // O(1) pop_front вместо O(N) remove(0).
                let entry = cache.packets.pop_front().unwrap();
                cache.expected_seq = entry.seq.wrapping_add(1);
                events.push(OrderBookEvent::Apply {
                    market_index: entry.pkt.market_index,
                    book_kind: entry.pkt.book_kind,
                    is_full: entry.pkt.is_full,
                    seq: entry.seq,
                    buys: entry.pkt.buys,
                    sells: entry.pkt.sells,
                });
            } else {
                // Gap остался — остановиться.
                break;
            }
        }
        cache.check_cache_empty();
    }

    /// Сбросить весь state (например при reconnect / WantNewHello).
    pub fn clear(&mut self) {
        for (_, c) in self.caches.iter_mut() {
            c.clear();
            c.has_full = false;
            c.corrupted = false;
            c.expected_seq = 0;
        }
        self.caches.clear();
    }

    /// Количество активных кэшей.
    pub fn len(&self) -> usize {
        self.caches.len()
    }

    pub fn is_empty(&self) -> bool {
        self.caches.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::order_book::OrderLevel;

    fn make_pkt(market_idx: u16, book_kind: u8, seq: u16, is_full: bool) -> OrderBookUpdate {
        OrderBookUpdate {
            market_index: market_idx,
            seq,
            is_full,
            book_kind,
            buys: vec![OrderLevel { rate: 100.0, quantity: 1.0 }],
            sells: vec![OrderLevel { rate: 101.0, quantity: 2.0 }],
        }
    }

    #[test]
    fn full_then_inorder_diffs() {
        let mut ob = OrderBooks::new();
        let events = ob.on_packet(make_pkt(1, 0, 10, true), 1000);
        assert!(matches!(events[0], OrderBookEvent::Apply { is_full: true, seq: 10, .. }));

        let events = ob.on_packet(make_pkt(1, 0, 11, false), 1010);
        assert!(matches!(events[0], OrderBookEvent::Apply { is_full: false, seq: 11, .. }));

        let events = ob.on_packet(make_pkt(1, 0, 12, false), 1020);
        assert!(matches!(events[0], OrderBookEvent::Apply { is_full: false, seq: 12, .. }));
    }

    #[test]
    fn diff_before_full_ignored_and_requests_full() {
        let mut ob = OrderBooks::new();
        let events = ob.on_packet(make_pkt(2, 0, 5, false), 1000);
        // Должен быть RequestFullNeeded + Ignored(NoFullYet).
        let has_req = events.iter().any(|e| matches!(e, OrderBookEvent::RequestFullNeeded { .. }));
        let has_ignored = events.iter().any(|e| matches!(e, OrderBookEvent::Ignored { reason: ApplyResult::NoFullYet, .. }));
        assert!(has_req && has_ignored);
    }

    #[test]
    fn gap_caches_then_drains() {
        let mut ob = OrderBooks::new();
        ob.on_packet(make_pkt(3, 0, 10, true), 1000);
        // Получен seq 12 — gap. Положить в cache.
        let events = ob.on_packet(make_pkt(3, 0, 12, false), 1010);
        assert!(events.iter().any(|e| matches!(e, OrderBookEvent::Ignored { reason: ApplyResult::Cached, seq: 12, .. })));
        // Получен seq 11 — применить + drain seq 12.
        let events = ob.on_packet(make_pkt(3, 0, 11, false), 1020);
        let applied_seqs: Vec<u16> = events.iter().filter_map(|e| match e {
            OrderBookEvent::Apply { seq, .. } => Some(*seq),
            _ => None,
        }).collect();
        assert_eq!(applied_seqs, vec![11, 12]);
    }

    #[test]
    fn stale_diff_rejected() {
        let mut ob = OrderBooks::new();
        ob.on_packet(make_pkt(4, 0, 20, true), 1000); // Full, expected_seq = 21
        let events = ob.on_packet(make_pkt(4, 0, 19, false), 1010); // seq 19 < 21
        assert!(events.iter().any(|e| matches!(e, OrderBookEvent::Ignored { reason: ApplyResult::Stale, .. })));
    }

    #[test]
    fn corrupted_throttle() {
        let mut ob = OrderBooks::new();
        // Diff без Full — первый запрос Full отправлен.
        let events = ob.on_packet(make_pkt(5, 0, 1, false), 1000);
        assert!(events.iter().any(|e| matches!(e, OrderBookEvent::RequestFullNeeded { .. })));
        // Второй diff через 100мс — НЕ должен отправить второй request (throttle 5000мс).
        let events = ob.on_packet(make_pkt(5, 0, 2, false), 1100);
        assert!(!events.iter().any(|e| matches!(e, OrderBookEvent::RequestFullNeeded { .. })));
        // Через 5100мс — должен отправить.
        let events = ob.on_packet(make_pkt(5, 0, 3, false), 6200);
        assert!(events.iter().any(|e| matches!(e, OrderBookEvent::RequestFullNeeded { .. })));
    }

    #[test]
    fn separate_pairs_independent() {
        let mut ob = OrderBooks::new();
        ob.on_packet(make_pkt(1, 0, 10, true), 1000); // Futures
        ob.on_packet(make_pkt(1, 1, 20, true), 1000); // Spot
        // Diff for spot at seq 21 — должен примениться.
        let events = ob.on_packet(make_pkt(1, 1, 21, false), 1010);
        assert!(events.iter().any(|e| matches!(e, OrderBookEvent::Apply { is_full: false, seq: 21, book_kind: 1, .. })));
        // Diff для futures at seq 11 — должен примениться независимо.
        let events = ob.on_packet(make_pkt(1, 0, 11, false), 1010);
        assert!(events.iter().any(|e| matches!(e, OrderBookEvent::Apply { is_full: false, seq: 11, book_kind: 0, .. })));
    }
}
