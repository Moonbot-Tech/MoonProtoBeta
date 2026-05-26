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
#[derive(Debug, Clone, Default)]
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
#[derive(Debug, Clone, Default)]
pub struct OrderBooks {
    caches: HashMap<BookKey, OrderBookCache>,
    books: HashMap<BookKey, OrderBookSnapshot>,
    diff_scratch: Vec<OrderBookLevel>,
}

impl OrderBooks {
    pub fn new() -> Self {
        Self {
            caches: HashMap::new(),
            books: HashMap::new(),
            diff_scratch: Vec::new(),
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
            apply_diff_book(
                &mut self.books,
                &mut self.diff_scratch,
                key,
                seq,
                &pkt.buys,
                &pkt.sells,
            );
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
            apply_diff_book(
                &mut self.books,
                &mut self.diff_scratch,
                key,
                pkt.seq,
                &pkt.buys,
                &pkt.sells,
            );
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
            apply_cached_packet(&mut self.books, &mut self.diff_scratch, key, &entry.pkt);
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
    scratch: &mut Vec<OrderBookLevel>,
    key: BookKey,
    pkt: &OrderBookUpdate,
) {
    if pkt.is_full {
        apply_full_book(books, key, pkt.seq, &pkt.buys, &pkt.sells);
    } else {
        apply_diff_book(books, scratch, key, pkt.seq, &pkt.buys, &pkt.sells);
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
    scratch: &mut Vec<OrderBookLevel>,
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
    apply_order_book_diff_keep_zero(&mut book.buys, scratch, buy_diff, sell_diff, true);
    apply_order_book_diff_keep_zero(&mut book.sells, scratch, sell_diff, buy_diff, false);
    book.seq = seq;
}

fn apply_order_book_diff_keep_zero(
    book: &mut Vec<OrderBookLevel>,
    scratch: &mut Vec<OrderBookLevel>,
    diff: &[OrderLevel],
    shrink: &[OrderLevel],
    is_buy_book: bool,
) {
    if diff.is_empty() {
        return;
    }

    scratch.clear();
    scratch.extend_from_slice(book);
    book.clear();
    book.reserve(scratch.len() + diff.len());
    let mut k = 0usize;

    for diff_level in diff {
        let diff_rate = diff_level.rate as f64;

        if is_buy_book {
            while k < scratch.len() && scratch[k].rate > diff_rate + EPS_M {
                book.push(scratch[k]);
                k += 1;
            }
        } else {
            while k < scratch.len() && scratch[k].rate < diff_rate - EPS_M {
                book.push(scratch[k]);
                k += 1;
            }
        }

        if (diff_level.quantity as f64) > EPS {
            book.push((*diff_level).into());
        }

        if k < scratch.len() && (scratch[k].rate - diff_rate).abs() < EPS_M {
            k += 1;
        }
    }

    while k < scratch.len() {
        book.push(scratch[k]);
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
            while cut < book.len() && book[cut].rate >= cut_price {
                cut += 1;
            }
        } else {
            while cut < book.len() && book[cut].rate <= cut_price {
                cut += 1;
            }
        }
        if cut > 0 {
            book.drain(0..cut);
        }
    }
}

#[cfg(test)]
mod tests;
