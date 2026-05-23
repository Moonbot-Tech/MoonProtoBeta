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
//!         OrderBookEvent::Apply { .. } => /* read state.book_by_kind(...) */,
//!         OrderBookEvent::RequestFullNeeded { market_index, book_kind } => {
//!             // Low-level mode only: send emk_RequestOrderBookFull yourself.
//!             // EventDispatcher::dispatch_into_active consumes this internally.
//!             let req = commands::engine_request::request_order_book_full(market_index, book_kind);
//!             client.send_api_request(&req);
//!         }
//!     }
//! }
//! ```

use std::collections::{HashMap, VecDeque};

use crate::commands::order_book::{compare_seq, OrderBookUpdate, OrderLevel};

const EPS: f64 = 0.00000001;
const EPS_M: f64 = 0.000000009;

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
    /// Последний применённый seq. Аналог Delphi `m.MoonProtoBookSeq[bookKind]`.
    /// Начальное значение 0 важно: normal-mode применяет первый Diff без Full
    /// (Delphi `MoonProtoEngine.pas:2066-2071`). Раньше использовалось `has_full: bool`,
    /// что заставляло клиента отправлять лишний `RequestOrderBookFull` и
    /// terminale не показывали degraded diff-view.
    last_applied_seq: u16,
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
        self.packets
            .as_slices()
            .0
            .partition_point(|p| compare_seq(p.seq, seq) < 0)
    }

    fn add(&mut self, seq: u16, pkt: OrderBookUpdate, now_ms: i64) {
        if self.packets.is_empty() {
            self.cache_not_empty_since_ms = now_ms;
        }
        let pos = self.binary_search_insert(seq);
        self.packets.insert(pos, CachedPacket { seq, pkt });
    }

    fn drop_oldest(&mut self) {
        self.packets.pop_front();
        if self.packets.is_empty() {
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
    fn try_request_full(&mut self, now_ms: i64) -> bool {
        if !self.corrupted {
            return false;
        }
        if (now_ms - self.last_full_request_ms).abs() <= BOOK_FULL_REQUEST_THROTTLE {
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
    /// Legacy reason: больше не эмитится после strict Delphi parity fix.
    /// Delphi `MoonProtoEngine.pas:2066-2071` применяет первый Diff если
    /// `MoonProtoBookSeq = 0`. Сохранено для backwards-compat enum match'ей.
    NoFullYet,
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
        buys: Vec<crate::commands::order_book::OrderLevel>,
        sells: Vec<crate::commands::order_book::OrderLevel>,
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

/// Главный sync state — кэш на каждый (market_index, book_kind).
#[derive(Debug, Default)]
pub struct OrderBooks {
    caches: HashMap<BookKey, OrderBookCache>,
    books: HashMap<BookKey, OrderBookSnapshot>,
}

impl OrderBooks {
    pub fn new() -> Self {
        Self {
            caches: HashMap::new(),
            books: HashMap::new(),
        }
    }

    /// Обработать один распарсенный `MPC_OrderBook` пакет.
    /// `now_ms` — текущее время в миллисекундах (из `client.now_ms()`).
    /// Возвращает список событий: Apply (может быть несколько при разгребании cache), RequestFullNeeded, Ignored.
    #[must_use = "OrderBookEvent's must be processed — low-level пропуск RequestFullNeeded ведёт к persistent corrupted orderbook"]
    pub fn on_packet(&mut self, pkt: OrderBookUpdate, now_ms: i64) -> Vec<OrderBookEvent> {
        let key: BookKey = (pkt.market_index, pkt.book_kind);
        let mut events = Vec::new();

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
                book_kind: pkt.book_kind,
                is_full: true,
                seq: pkt.seq,
                buys: pkt.buys,
                sells: pkt.sells,
            });
            // Попробовать применить накопленные diff из cache.
            self.drain_cache(key, &mut events);
            return events;
        }

        // === 2. Corrupted mode — Delphi MoonProtoEngine.pas:2021-2039 ===
        // Пока ждём свежий Full snapshot, применяем приходящие diff'ы as-is для
        // degraded live view + продолжаем требовать Full (throttle). Раньше Diff
        // в этом режиме отбрасывались — UI замораживался на время ожидания.
        if cache.corrupted {
            let seq = pkt.seq;
            let cached_pkt = pkt.clone();
            apply_diff_book(&mut self.books, key, seq, &pkt.buys, &pkt.sells);
            cache.last_applied_seq = seq;
            events.push(OrderBookEvent::Apply {
                market_index: pkt.market_index,
                book_kind: pkt.book_kind,
                is_full: false,
                seq,
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
                    book_kind: key.1,
                });
            }
            return events;
        }

        let cmp = compare_seq(pkt.seq, cache.expected_seq);

        // === 3. In-order OR первый Diff без Full ===
        // Delphi `MoonProtoEngine.pas:2066-2071`: если `MoonProtoBookSeq = 0`
        // (последний применённый seq = 0) — применяет первый Diff без ожидания
        // Full. Раньше мы отбрасывали → лишний RequestFullNeeded request.
        if cmp == 0 || cache.last_applied_seq == 0 {
            apply_diff_book(&mut self.books, key, pkt.seq, &pkt.buys, &pkt.sells);
            cache.expected_seq = pkt.seq.wrapping_add(1);
            cache.last_applied_seq = pkt.seq;
            events.push(OrderBookEvent::Apply {
                market_index: pkt.market_index,
                book_kind: pkt.book_kind,
                is_full: false,
                seq: pkt.seq,
                buys: pkt.buys,
                sells: pkt.sells,
            });
            // Может быть в cache следующие seq — drain.
            self.drain_cache(key, &mut events);
            return events;
        }

        // === 4. Stale: seq < expected → отброс ===
        if cmp < 0 {
            events.push(OrderBookEvent::Ignored {
                market_index: pkt.market_index,
                book_kind: pkt.book_kind,
                seq: pkt.seq,
                reason: ApplyResult::Stale,
            });
            return events;
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
            apply_cached_packet(&mut self.books, key, &entry.pkt);
            cache.expected_seq = entry.seq.wrapping_add(1);
            cache.last_applied_seq = entry.seq;
            events.push(OrderBookEvent::Apply {
                market_index: entry.pkt.market_index,
                book_kind: entry.pkt.book_kind,
                is_full: entry.pkt.is_full,
                seq: entry.seq,
                buys: entry.pkt.buys,
                sells: entry.pkt.sells,
            });
        }
        cache.check_cache_empty();
    }

    /// Сбросить весь state (например при reconnect / WantNewHello).
    pub fn clear(&mut self) {
        for (_, c) in self.caches.iter_mut() {
            c.clear();
            c.corrupted = false;
            c.expected_seq = 0;
            c.last_applied_seq = 0;
        }
        self.caches.clear();
        self.books.clear();
    }

    /// Количество активных кэшей.
    pub fn len(&self) -> usize {
        self.caches.len()
    }

    pub fn is_empty(&self) -> bool {
        self.caches.is_empty()
    }

    /// Get the applied current book by raw wire kind (`0 = futures`, `1 = spot`).
    pub fn book_by_kind(&self, market_index: u16, book_kind: u8) -> Option<&OrderBookSnapshot> {
        self.books.get(&(market_index, book_kind))
    }

    /// Get the applied current book.
    pub fn book(&self, market_index: u16, book_kind: OrderBookKind) -> Option<&OrderBookSnapshot> {
        self.book_by_kind(market_index, book_kind.as_u8())
    }

    /// Get best bid/ask from the applied current book.
    pub fn top_of_book(&self, market_index: u16, book_kind: OrderBookKind) -> Option<TopOfBook> {
        self.book(market_index, book_kind)
            .map(OrderBookSnapshot::top)
    }

    /// Iterate over applied current books.
    pub fn iter_books(&self) -> impl Iterator<Item = &OrderBookSnapshot> {
        self.books.values()
    }
}

fn apply_cached_packet(
    books: &mut HashMap<BookKey, OrderBookSnapshot>,
    key: BookKey,
    pkt: &OrderBookUpdate,
) {
    if pkt.is_full {
        apply_full_book(books, key, pkt.seq, &pkt.buys, &pkt.sells);
    } else {
        apply_diff_book(books, key, pkt.seq, &pkt.buys, &pkt.sells);
    }
}

fn apply_full_book(
    books: &mut HashMap<BookKey, OrderBookSnapshot>,
    key: BookKey,
    seq: u16,
    buys: &[OrderLevel],
    sells: &[OrderLevel],
) {
    let book = books.entry(key).or_insert_with(|| OrderBookSnapshot {
        market_index: key.0,
        book_kind: key.1,
        seq: 0,
        buys: Vec::new(),
        sells: Vec::new(),
    });
    book.seq = seq;
    book.buys.clear();
    book.buys
        .extend(buys.iter().copied().map(OrderBookLevel::from));
    book.sells.clear();
    book.sells
        .extend(sells.iter().copied().map(OrderBookLevel::from));
}

fn apply_diff_book(
    books: &mut HashMap<BookKey, OrderBookSnapshot>,
    key: BookKey,
    seq: u16,
    buy_diff: &[OrderLevel],
    sell_diff: &[OrderLevel],
) {
    let book = books.entry(key).or_insert_with(|| OrderBookSnapshot {
        market_index: key.0,
        book_kind: key.1,
        seq: 0,
        buys: Vec::new(),
        sells: Vec::new(),
    });
    apply_order_book_diff_keep_zero(&mut book.buys, buy_diff, sell_diff, true);
    apply_order_book_diff_keep_zero(&mut book.sells, sell_diff, buy_diff, false);
    book.seq = seq;
}

fn apply_order_book_diff_keep_zero(
    book: &mut Vec<OrderBookLevel>,
    diff: &[OrderLevel],
    shrink: &[OrderLevel],
    is_buy_book: bool,
) {
    if diff.is_empty() {
        return;
    }

    let mut new_book = Vec::with_capacity(book.len() + diff.len());
    let mut k = 0usize;

    for diff_level in diff {
        let diff_rate = diff_level.rate as f64;

        if is_buy_book {
            while k < book.len() && book[k].rate > diff_rate + EPS_M {
                new_book.push(book[k]);
                k += 1;
            }
        } else {
            while k < book.len() && book[k].rate < diff_rate - EPS_M {
                new_book.push(book[k]);
                k += 1;
            }
        }

        if (diff_level.quantity as f64) > EPS {
            new_book.push((*diff_level).into());
        }

        if k < book.len() && (book[k].rate - diff_rate).abs() < EPS_M {
            k += 1;
        }
    }

    while k < book.len() {
        new_book.push(book[k]);
        k += 1;
    }

    let mut cut_price = -1.0;
    for level in shrink {
        let rate = level.rate as f64;
        if rate > EPS_M {
            cut_price = rate;
            break;
        }
    }

    if cut_price > 0.0 {
        let mut cut = 0usize;
        if is_buy_book {
            while cut < new_book.len() && new_book[cut].rate >= cut_price {
                cut += 1;
            }
        } else {
            while cut < new_book.len() && new_book[cut].rate <= cut_price {
                cut += 1;
            }
        }
        if cut > 0 {
            new_book.drain(0..cut);
        }
    }

    *book = new_book;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::order_book::OrderLevel;

    fn level(rate: f32, quantity: f32) -> OrderLevel {
        OrderLevel { rate, quantity }
    }

    fn make_pkt(market_idx: u16, book_kind: u8, seq: u16, is_full: bool) -> OrderBookUpdate {
        OrderBookUpdate {
            market_index: market_idx,
            seq,
            is_full,
            book_kind,
            buys: vec![level(100.0, 1.0)],
            sells: vec![level(101.0, 2.0)],
        }
    }

    #[test]
    fn full_then_inorder_diffs() {
        let mut ob = OrderBooks::new();
        let events = ob.on_packet(make_pkt(1, 0, 10, true), 1000);
        assert!(matches!(
            events[0],
            OrderBookEvent::Apply {
                is_full: true,
                seq: 10,
                ..
            }
        ));

        let events = ob.on_packet(make_pkt(1, 0, 11, false), 1010);
        assert!(matches!(
            events[0],
            OrderBookEvent::Apply {
                is_full: false,
                seq: 11,
                ..
            }
        ));

        let events = ob.on_packet(make_pkt(1, 0, 12, false), 1020);
        assert!(matches!(
            events[0],
            OrderBookEvent::Apply {
                is_full: false,
                seq: 12,
                ..
            }
        ));
    }

    #[test]
    fn first_diff_without_full_is_applied_like_delphi() {
        // Delphi MoonProtoEngine.pas:2066-2071:
        // Если `last_applied_seq = 0` (никогда ещё не применяли) — применяем
        // первый Diff без ожидания Full. Раньше отбрасывали + просили Full.
        let mut ob = OrderBooks::new();
        let events = ob.on_packet(make_pkt(2, 0, 5, false), 1000);
        assert!(
            events.iter().any(|e| matches!(
                e,
                OrderBookEvent::Apply {
                    is_full: false,
                    seq: 5,
                    ..
                }
            )),
            "первый Diff с last_applied_seq=0 должен примениться (Delphi normal-mode)"
        );
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, OrderBookEvent::RequestFullNeeded { .. })),
            "RequestFullNeeded не нужен — Delphi не запрашивает Full в этом сценарии"
        );
    }

    #[test]
    fn gap_caches_then_drains() {
        let mut ob = OrderBooks::new();
        let _ = ob.on_packet(make_pkt(3, 0, 10, true), 1000);
        // Получен seq 12 — gap. Положить в cache.
        let events = ob.on_packet(make_pkt(3, 0, 12, false), 1010);
        assert!(events.iter().any(|e| matches!(
            e,
            OrderBookEvent::Ignored {
                reason: ApplyResult::Cached,
                seq: 12,
                ..
            }
        )));
        // Получен seq 11 — применить + drain seq 12.
        let events = ob.on_packet(make_pkt(3, 0, 11, false), 1020);
        let applied_seqs: Vec<u16> = events
            .iter()
            .filter_map(|e| match e {
                OrderBookEvent::Apply { seq, .. } => Some(*seq),
                _ => None,
            })
            .collect();
        assert_eq!(applied_seqs, vec![11, 12]);
    }

    #[test]
    fn stale_diff_rejected() {
        let mut ob = OrderBooks::new();
        let _ = ob.on_packet(make_pkt(4, 0, 20, true), 1000); // Full, expected_seq = 21
        let events = ob.on_packet(make_pkt(4, 0, 19, false), 1010); // seq 19 < 21
        assert!(events.iter().any(|e| matches!(
            e,
            OrderBookEvent::Ignored {
                reason: ApplyResult::Stale,
                ..
            }
        )));
    }

    #[test]
    fn corrupted_throttle() {
        // Throttle RequestFullNeeded после cache.is_expired() триггерит corrupted.
        let mut ob = OrderBooks::new();
        // Full + gap → cache.add, не corrupted ещё.
        let _ = ob.on_packet(make_pkt(5, 0, 1, true), 10_000);
        let _ = ob.on_packet(make_pkt(5, 0, 10, false), 10_010); // cache_not_empty_since=10010
                                                                 // 890ms прошло — is_expired (>800ms) → corrupted=true → первый RequestFullNeeded.
        let events = ob.on_packet(make_pkt(5, 0, 11, false), 10_900);
        assert!(
            events
                .iter()
                .any(|e| matches!(e, OrderBookEvent::RequestFullNeeded { .. })),
            "is_expired (890>800ms) → corrupted=true → первый RequestFullNeeded"
        );
        // Через 100мс в corrupted ветке — НЕ должен отправить (throttle 5000мс).
        let events = ob.on_packet(make_pkt(5, 0, 12, false), 11_000);
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, OrderBookEvent::RequestFullNeeded { .. })),
            "throttle 5000ms блокирует второй RequestFullNeeded"
        );
        // Через >5000ms с момента первого запроса — throttle снимается.
        let events = ob.on_packet(make_pkt(5, 0, 13, false), 16_001);
        assert!(
            events
                .iter()
                .any(|e| matches!(e, OrderBookEvent::RequestFullNeeded { .. })),
            "через 5000ms throttle снимается"
        );
    }

    #[test]
    fn initial_full_request_throttle_matches_delphi_zero_timestamp() {
        // Delphi `TOrderBookCache.Create` sets FLastFullRequestTime = 0, and
        // TryRequestFull still applies the same <=5000ms throttle against that
        // zero. Rust must not special-case 0 as "never requested".
        let mut ob = OrderBooks::new();
        let _ = ob.on_packet(make_pkt(15, 0, 1, true), 0);
        let _ = ob.on_packet(make_pkt(15, 0, 10, false), 10);

        let events = ob.on_packet(make_pkt(15, 0, 11, false), 900);
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, OrderBookEvent::RequestFullNeeded { .. })),
            "0..5000ms after process start must be throttled exactly like Delphi"
        );

        let events = ob.on_packet(make_pkt(15, 0, 12, false), 5001);
        assert!(
            events
                .iter()
                .any(|e| matches!(e, OrderBookEvent::RequestFullNeeded { .. })),
            ">5000ms releases the first RequestOrderBookFull"
        );
    }

    #[test]
    fn corrupted_mode_applies_diffs_while_waiting_for_full() {
        // Delphi MoonProtoEngine.pas:2021-2039: в corrupted режиме клиент
        // применяет diff'ы as-is для degraded live view, а не замораживает UI.
        let mut ob = OrderBooks::new();
        // Full + Diff в order, потом gap → corrupted.
        let _ = ob.on_packet(make_pkt(6, 0, 10, true), 10_000);
        let _ = ob.on_packet(make_pkt(6, 0, 12, false), 10_010); // gap [11]
                                                                 // Через 890ms is_expired → corrupted.
        let events = ob.on_packet(make_pkt(6, 0, 13, false), 10_900);
        assert!(events
            .iter()
            .any(|e| matches!(e, OrderBookEvent::RequestFullNeeded { .. })));

        // Следующий Diff в corrupted — должен примениться (degraded view).
        let events = ob.on_packet(make_pkt(6, 0, 14, false), 10_910);
        assert!(
            events.iter().any(|e| matches!(
                e,
                OrderBookEvent::Apply {
                    is_full: false,
                    seq: 14,
                    ..
                }
            )),
            "corrupted mode должен продолжать показывать degraded live view"
        );
    }

    #[test]
    fn separate_pairs_independent() {
        let mut ob = OrderBooks::new();
        let _ = ob.on_packet(make_pkt(1, 0, 10, true), 1000); // Futures
        let _ = ob.on_packet(make_pkt(1, 1, 20, true), 1000); // Spot
                                                              // Diff for spot at seq 21 — должен примениться.
        let events = ob.on_packet(make_pkt(1, 1, 21, false), 1010);
        assert!(events.iter().any(|e| matches!(
            e,
            OrderBookEvent::Apply {
                is_full: false,
                seq: 21,
                book_kind: 1,
                ..
            }
        )));
        // Diff для futures at seq 11 — должен примениться независимо.
        let events = ob.on_packet(make_pkt(1, 0, 11, false), 1010);
        assert!(events.iter().any(|e| matches!(
            e,
            OrderBookEvent::Apply {
                is_full: false,
                seq: 11,
                book_kind: 0,
                ..
            }
        )));
    }

    #[test]
    fn book_seq_zero_overrides_stale_compare_like_delphi() {
        // Delphi normal-mode условие проверяет `m.MoonProtoBookSeq = 0` до
        // stale-drop. Поэтому при начальном seq=0 пакет 65535 всё равно
        // применяется, хотя CompareSeq(65535, 0) < 0.
        let mut ob = OrderBooks::new();
        let events = ob.on_packet(make_pkt(9, 0, u16::MAX, false), 1000);
        assert!(
            events
                .iter()
                .any(|e| matches!(e, OrderBookEvent::Apply { seq: u16::MAX, .. })),
            "MoonProtoBookSeq=0 должен применить Diff до stale-check"
        );
    }

    #[test]
    fn duplicate_gap_packets_are_cached_like_delphi() {
        let mut ob = OrderBooks::new();
        let _ = ob.on_packet(make_pkt(10, 0, 1, true), 1000);
        let _ = ob.on_packet(make_pkt(10, 0, 3, false), 1010);
        let _ = ob.on_packet(make_pkt(10, 0, 3, false), 1020);

        let cache = ob.caches.get(&(10, 0)).unwrap();
        assert_eq!(
            cache.packets.len(),
            2,
            "TOrderBookCache.Add inserts duplicate seq packets; stale cleanup happens during drain"
        );
    }

    #[test]
    fn normal_gap_overflow_enters_corrupted_without_clearing_cache() {
        let mut ob = OrderBooks::new();
        let _ = ob.on_packet(make_pkt(11, 0, 1, true), 10_000);

        let mut request_full_seen = false;
        for seq in 3..=67 {
            let events = ob.on_packet(make_pkt(11, 0, seq, false), 10_000 + seq as i64);
            request_full_seen |= events
                .iter()
                .any(|e| matches!(e, OrderBookEvent::RequestFullNeeded { .. }));
        }

        let cache = ob.caches.get(&(11, 0)).unwrap();
        assert!(
            cache.corrupted,
            "Count > BOOK_CACHE_MAX_PACKETS переводит cache в corrupted"
        );
        assert!(
            request_full_seen,
            "TryRequestFull должен сработать при входе в corrupted"
        );
        assert_eq!(
            cache.packets.len(),
            65,
            "Delphi normal-mode не очищает cache при overflow; 65-й gap-пакет остаётся в списке"
        );
    }

    #[test]
    fn corrupted_mode_drops_oldest_before_add() {
        let mut ob = OrderBooks::new();
        let _ = ob.on_packet(make_pkt(12, 0, 1, true), 1000);
        for seq in 3..=67 {
            let _ = ob.on_packet(make_pkt(12, 0, seq, false), 1000 + seq as i64);
        }

        let _ = ob.on_packet(make_pkt(12, 0, 68, false), 2000);
        let cache = ob.caches.get(&(12, 0)).unwrap();
        assert_eq!(cache.packets.len(), 65);
        assert_eq!(
            cache.packets.front().map(|p| p.seq),
            Some(4),
            "в corrupted mode Delphi DropOldest выполняется перед Add нового diff"
        );
    }

    #[test]
    fn full_snapshot_updates_applied_read_model() {
        let mut ob = OrderBooks::new();
        let pkt = OrderBookUpdate {
            market_index: 1,
            seq: 10,
            is_full: true,
            book_kind: 0,
            buys: vec![level(100.0, 1.5), level(99.0, 2.0)],
            sells: vec![level(101.0, 1.25), level(102.0, 3.0)],
        };
        let _ = ob.on_packet(pkt, 1000);

        let book = ob.book(1, OrderBookKind::Futures).unwrap();
        assert_eq!(book.seq, 10);
        assert_eq!(
            book.top().bid,
            Some(OrderBookLevel {
                rate: 100.0,
                quantity: 1.5
            })
        );
        assert_eq!(
            book.top().ask,
            Some(OrderBookLevel {
                rate: 101.0,
                quantity: 1.25
            })
        );
        assert_eq!(book.buys.len(), 2);
        assert_eq!(book.sells.len(), 2);
    }

    #[test]
    fn diff_updates_inserts_and_deletes_applied_levels() {
        let mut ob = OrderBooks::new();
        let _ = ob.on_packet(
            OrderBookUpdate {
                market_index: 2,
                seq: 1,
                is_full: true,
                book_kind: 0,
                buys: vec![level(100.0, 1.0), level(99.0, 1.0)],
                sells: vec![level(101.0, 1.0), level(102.0, 1.0)],
            },
            1000,
        );

        let _ = ob.on_packet(
            OrderBookUpdate {
                market_index: 2,
                seq: 2,
                is_full: false,
                book_kind: 0,
                buys: vec![level(100.0, 2.0), level(98.0, 4.0)],
                sells: vec![level(101.0, 0.0), level(103.0, 3.0)],
            },
            1010,
        );

        let book = ob.book(2, OrderBookKind::Futures).unwrap();
        assert_eq!(
            book.buys,
            vec![
                OrderBookLevel {
                    rate: 100.0,
                    quantity: 2.0
                },
                OrderBookLevel {
                    rate: 99.0,
                    quantity: 1.0
                },
                OrderBookLevel {
                    rate: 98.0,
                    quantity: 4.0
                },
            ]
        );
        assert_eq!(
            book.sells,
            vec![
                OrderBookLevel {
                    rate: 102.0,
                    quantity: 1.0
                },
                OrderBookLevel {
                    rate: 103.0,
                    quantity: 3.0
                },
            ]
        );
    }

    #[test]
    fn diff_uses_opposite_side_shrink_like_delphi() {
        let mut ob = OrderBooks::new();
        let _ = ob.on_packet(
            OrderBookUpdate {
                market_index: 3,
                seq: 1,
                is_full: true,
                book_kind: 0,
                buys: vec![level(101.0, 1.0), level(99.0, 1.0)],
                sells: vec![level(102.0, 1.0)],
            },
            1000,
        );

        let _ = ob.on_packet(
            OrderBookUpdate {
                market_index: 3,
                seq: 2,
                is_full: false,
                book_kind: 0,
                buys: vec![level(99.5, 2.0)],
                sells: vec![level(100.0, 3.0)],
            },
            1010,
        );

        let book = ob.book(3, OrderBookKind::Futures).unwrap();
        assert_eq!(
            book.buys,
            vec![
                OrderBookLevel {
                    rate: 99.5,
                    quantity: 2.0
                },
                OrderBookLevel {
                    rate: 99.0,
                    quantity: 1.0
                },
            ]
        );
        assert_eq!(
            book.sells,
            vec![
                OrderBookLevel {
                    rate: 100.0,
                    quantity: 3.0
                },
                OrderBookLevel {
                    rate: 102.0,
                    quantity: 1.0
                },
            ]
        );
    }

    #[test]
    fn order_book_kind_roundtrip() {
        assert_eq!(OrderBookKind::Futures.as_u8(), 0);
        assert_eq!(OrderBookKind::Spot.as_u8(), 1);
        assert_eq!(OrderBookKind::from_u8(0), Some(OrderBookKind::Futures));
        assert_eq!(OrderBookKind::from_u8(1), Some(OrderBookKind::Spot));
        assert_eq!(OrderBookKind::from_u8(2), None);
        assert_eq!(OrderBookKind::from_u8(255), None);
    }
}
