//! TradesStream sync state — gap detection + resend protocol + batch response parser.
//!
//! Источник Delphi: `MoonProtoEngine.pas:21-36, 1364-1549, 1553-1921` (TGapBucket + ResetGapBuckets
//! + CreateGapBucket + FindBucketForPacket + CheckMissingTradesPackets + ProcessTradesStream
//! + ProcessTradesResendBatch).
//!
//! ## Что делает этот модуль
//!
//! Сервер шлёт `MPC_TradesStream` пакеты с `packet_num:u16` (wrapping). Клиент следит за
//! последовательностью. При gap (потерянный пакет) — создаётся **GapBucket**, который
//! запрашивает resend через `emk_TradesResend` (батч до 200 номеров) до 3 retry с
//! exponential backoff. Сервер отвечает `MPC_TradesResendResponse` (batch формата:
//! `Byte(count) + [Word(sz) + raw_packet] × count`), который мы распарсиваем обратно
//! в `TradesPacket` через `parse_trades_resend_response`.
//!
//! ## Использование
//!
//! ```ignore
//! let mut trades = TradesState::new();
//!
//! // 1. Поступление обычного MPC_TradesStream пакета:
//! let events = trades.on_packet(parsed_trades_packet, now_ms);
//! for ev in events {
//!     match ev {
//!         TradesEvent::Apply(pkt) => /* apply trades to local model */,
//!         TradesEvent::GapDetected { start, end } => /* лог только */,
//!     }
//! }
//!
//! // 2. Поступление MPC_TradesResendResponse — распарсить + apply каждого:
//! for raw_pkt in parse_trades_resend_response(payload) {
//!     if let Some(tp) = commands::trades_stream::parse_trades_packet(&raw_pkt) {
//!         let _evts = trades.on_packet_resend(tp);  // НЕ tracks (resend пакеты не должны двигать last_packet_num)
//!     }
//! }
//!
//! // 3. Периодический tick (раз в ~100ms) для проверки retry:
//! if let Some(resend_payload) = trades.tick(rtt_ms, now_ms) {
//!     client.send_api_request(&resend_payload);  // отправит emk_TradesResend
//! }
//! ```

use crate::commands::engine_request;
use crate::commands::trades_stream::TradesPacket;

const MAX_GAP_BUCKETS: usize = 50;
const DEFAULT_RECVD_SIZE: usize = 100;
const MAX_RECVD_SIZE: usize = 3000;
const MAX_RETRY_COUNT: u8 = 3;
/// Пауза, после которой клиент сбрасывает gap-state и начинает заново (мс).
/// Delphi: `TRADES_PAUSE_TIMEOUT = 30 / 86400` (30 сек).
const TRADES_PAUSE_TIMEOUT_MS: i64 = 30_000;

/// Один gap-bucket — диапазон [start_num, end_num] пропущенных packet_num.
#[derive(Debug, Clone)]
struct GapBucket {
    active: bool,
    start_num: u16,
    end_num: u16,
    created_ms: i64,
    last_retry_ms: i64,
    retry_count: u8,
    /// Битовая маска полученных packets внутри диапазона (recvd[i] = packet (start_num+i) получен).
    recvd: Vec<bool>,
}

impl Default for GapBucket {
    fn default() -> Self {
        Self {
            active: false,
            start_num: 0,
            end_num: 0,
            created_ms: 0,
            last_retry_ms: 0,
            retry_count: 0,
            recvd: vec![false; DEFAULT_RECVD_SIZE],
        }
    }
}

impl GapBucket {
    fn gap_size(&self) -> usize {
        // Используем wrapping для u16, +1 (inclusive).
        self.end_num.wrapping_sub(self.start_num) as usize + 1
    }
}

/// Wrapping-safe проверка: packet попадает в диапазон [start, end] (включительно).
fn is_packet_in_range(packet: u16, start: u16, end: u16) -> bool {
    // wrap-safe: gap_size = end - start + 1 (wrapping)
    let offset = packet.wrapping_sub(start);
    let span = end.wrapping_sub(start);
    offset <= span
}

/// Результат применения пакета.
#[derive(Debug, Clone)]
pub enum TradesEvent {
    /// Пакет применён — потребитель должен раздать trades по маркетам.
    Apply(TradesPacket),
    /// Обнаружен gap: пропущены packet_num в `[start..=end]`. Bucket создан, retry начнётся через tick().
    GapDetected { start: u16, end: u16 },
    /// Пакет был фактически дубликат (packet_num == last) — отброшен.
    Duplicate,
    /// Пакет пришёл вне диапазона — может быть после reset, отображает packet_num.
    OutOfOrder { packet_num: u16 },
    /// Принят out-of-order пакет, который был помечен в одном из gap-bucket'ов (recvd[i]=true).
    GapFilled { packet_num: u16, bucket_seq_range: (u16, u16) },
    /// Bucket закрыт: получены все trades или исчерпан retry лимит.
    BucketClosed { start: u16, end: u16, all_received: bool, retry_count: u8 },
}

/// Главный sync state для TradesStream.
pub struct TradesState {
    buckets: [GapBucket; MAX_GAP_BUCKETS],
    used_buckets: usize,
    last_packet_num: u16,
    last_packet_time_ms: i64,
    trades_started: bool,
    last_check_missing_ms: i64,
}

impl Default for TradesState {
    fn default() -> Self {
        Self::new()
    }
}

impl TradesState {
    pub fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| GapBucket::default()),
            used_buckets: 0,
            last_packet_num: 0,
            last_packet_time_ms: 0,
            trades_started: false,
            last_check_missing_ms: 0,
        }
    }

    /// Сбросить все buckets (Delphi `ResetGapBuckets` MoonProtoEngine.pas:1364-1378).
    pub fn reset_buckets(&mut self) {
        for b in self.buckets.iter_mut() {
            b.active = false;
        }
        self.used_buckets = 0;
    }

    /// Полный reset state (например при ServerToken change / reconnect).
    pub fn full_reset(&mut self) {
        self.reset_buckets();
        self.last_packet_num = 0;
        self.last_packet_time_ms = 0;
        self.trades_started = false;
    }

    /// Создать новый gap bucket (Delphi `CreateGapBucket` MoonProtoEngine.pas:1380-1430).
    fn create_bucket(&mut self, start_num: u16, end_num: u16, now_ms: i64) {
        let gap_size = end_num.wrapping_sub(start_num) as usize + 1;
        let gap_size = gap_size.min(MAX_RECVD_SIZE);

        // Сначала ищем пустой слот.
        for b in self.buckets.iter_mut() {
            if !b.active {
                b.active = true;
                b.start_num = start_num;
                b.end_num = end_num;
                b.created_ms = now_ms;
                b.last_retry_ms = now_ms;
                b.retry_count = 0;
                if b.recvd.len() < gap_size {
                    b.recvd.resize(gap_size, false);
                } else {
                    for r in b.recvd[..gap_size].iter_mut() {
                        *r = false;
                    }
                }
                self.used_buckets += 1;
                return;
            }
        }

        // Все заняты — вытесняем самый старый.
        let oldest_idx = self.buckets.iter().enumerate()
            .min_by_key(|(_, b)| b.created_ms)
            .map(|(i, _)| i).unwrap_or(0);
        let b = &mut self.buckets[oldest_idx];
        b.start_num = start_num;
        b.end_num = end_num;
        b.created_ms = now_ms;
        b.last_retry_ms = now_ms;
        b.retry_count = 0;
        if b.recvd.len() < gap_size {
            b.recvd.resize(gap_size, false);
        } else {
            for r in b.recvd[..gap_size].iter_mut() {
                *r = false;
            }
        }
        // used_buckets не меняется (slot был занят, остался занят).
    }

    /// Найти bucket для packet_num (только in-range, без extend для простоты).
    /// Возвращает index или None.
    fn find_bucket(&self, packet_num: u16) -> Option<usize> {
        if self.used_buckets == 0 {
            return None;
        }
        for (i, b) in self.buckets.iter().enumerate() {
            if b.active && is_packet_in_range(packet_num, b.start_num, b.end_num) {
                return Some(i);
            }
        }
        None
    }

    /// Обработать MPC_TradesStream пакет (track packets = true).
    /// Делает то же что Delphi `ProcessTradesStream(TrackPackets=True)` MoonProtoEngine.pas:1553+.
    #[must_use = "TradesEvent's must be processed — пропуск Apply ведёт к потере trades для UI/strategy"]
    pub fn on_packet(&mut self, pkt: TradesPacket, now_ms: i64) -> Vec<TradesEvent> {
        let mut events = Vec::new();
        let packet_num = pkt.packet_num;

        // === Первый пакет ИЛИ долгая пауза → reset ===
        let pause_detected = self.trades_started
            && self.last_packet_time_ms != 0
            && (now_ms - self.last_packet_time_ms).abs() > TRADES_PAUSE_TIMEOUT_MS;

        if !self.trades_started || pause_detected {
            self.reset_buckets();
            self.trades_started = true;
            self.last_packet_num = packet_num;
            self.last_packet_time_ms = now_ms;
            events.push(TradesEvent::Apply(pkt));
            return events;
        }

        // === Дубликат ===
        if packet_num == self.last_packet_num {
            events.push(TradesEvent::Duplicate);
            return events;
        }

        // === Sequential: packet_num == last + 1 ===
        if packet_num == self.last_packet_num.wrapping_add(1) {
            self.last_packet_num = packet_num;
            self.last_packet_time_ms = now_ms;
            events.push(TradesEvent::Apply(pkt));
            return events;
        }

        // === Out-of-order или Gap ===
        let last = self.last_packet_num;
        // packet_num > last+1 → новый gap. Missing range is [last+1 .. packet_num-1].
        let gap_size = packet_num.wrapping_sub(last.wrapping_add(1)) as usize;

        // Если packet_num фактически "впереди" last (forward gap), создаём bucket.
        // Wrap-safe forward detection: packet_num != last && packet_num != last+1.
        // Различаем forward gap (gap_size небольшой) от backward (resend matching).

        // Сначала проверяем — это packet из существующего bucket?
        if let Some(idx) = self.find_bucket(packet_num) {
            let b = &mut self.buckets[idx];
            let recvd_idx = packet_num.wrapping_sub(b.start_num) as usize;
            if recvd_idx < b.recvd.len() {
                b.recvd[recvd_idx] = true;
            }
            let bucket_range = (b.start_num, b.end_num);
            events.push(TradesEvent::GapFilled { packet_num, bucket_seq_range: bucket_range });
            events.push(TradesEvent::Apply(pkt));
            return events;
        }

        // Иначе — forward gap.
        let new_gap_start = last.wrapping_add(1);
        let new_gap_end = packet_num.wrapping_sub(1);

        // === EXTEND existing bucket (Delphi FindBucketForPacket WantExtend, MoonProtoEngine.pas:1461-1479) ===
        // Если есть bucket с `end_num == new_gap_start - 2` — это значит был sequential
        // пакет `new_gap_start - 1` между bucket'ом и текущим. Расширяем bucket чтобы
        // покрыть оба gap'а как один — иначе при packet-loss быстро упрёмся в MAX_GAP_BUCKETS.
        // packet at position oldSize (= old_end + 1 = sequential packet, который был получен)
        // помечается как received.
        let target_end = new_gap_start.wrapping_sub(2); // = last.wrapping_sub(1)
        let mut extended = false;
        for b in self.buckets.iter_mut() {
            if !b.active { continue; }
            if b.end_num != target_end { continue; }
            let new_size = new_gap_end.wrapping_sub(b.start_num) as usize + 1;
            if new_size > MAX_RECVD_SIZE { continue; }
            let old_size = b.end_num.wrapping_sub(b.start_num) as usize + 1;
            if b.recvd.len() < new_size {
                b.recvd.resize(new_size, false);
            }
            // packet ровно перед NewGapStart (= last sequential, который двинул last_packet_num)
            // был получен → mark as recvd.
            if old_size < b.recvd.len() {
                b.recvd[old_size] = true;
            }
            // zero the rest (после oldSize до newSize)
            if old_size + 1 < new_size {
                for r in b.recvd[(old_size + 1)..new_size].iter_mut() {
                    *r = false;
                }
            }
            b.end_num = new_gap_end;
            extended = true;
            events.push(TradesEvent::GapDetected { start: new_gap_start, end: new_gap_end });
            break;
        }

        if !extended {
            // Проверяем размер. Слишком большой gap или buckets переполнены.
            if gap_size > MAX_RECVD_SIZE || self.used_buckets >= MAX_GAP_BUCKETS {
                // Delphi MoonProtoEngine.pas:1649-1658: при overflow сбрасывает buckets,
                // НЕ обновляет LastTradesPacketNum, но текущий пакет всё равно дальше
                // применяется к рынкам. Следующий обычный пакет заново стартует tracking.
                //
                // Старый "anti-DoS H8" drop+warn был самодеятельностью: ServerToken
                // change уже handled через `EventDispatcher.last_known_server_token`
                // ДО применения пакета, поэтому здесь нет adversarial vector — есть
                // легитимный backpressure от сервера (например после restart).
                log::warn!(target: "moonproto::trades",
                    "packet_num jump {} -> {} (gap_size={} > MAX_RECVD_SIZE={} or buckets full); resetting gap buckets like Delphi",
                    last, packet_num, gap_size, MAX_RECVD_SIZE);
                self.reset_buckets();
                self.trades_started = false;
                self.last_packet_time_ms = now_ms;
                events.push(TradesEvent::Apply(pkt));
                return events;
            }

            self.create_bucket(new_gap_start, new_gap_end, now_ms);
            events.push(TradesEvent::GapDetected { start: new_gap_start, end: new_gap_end });
        }

        self.last_packet_num = packet_num;
        self.last_packet_time_ms = now_ms;
        events.push(TradesEvent::Apply(pkt));
        events
    }

    /// Обработать пакет из MPC_TradesResendResponse (track packets = false).
    /// Не двигает last_packet_num, только помечает recvd в buckets.
    /// Delphi `ProcessTradesStream(TrackPackets=False)` ветка (MoonProtoEngine.pas:1667-1675).
    pub fn on_packet_resend(&mut self, pkt: TradesPacket) -> Vec<TradesEvent> {
        let mut events = Vec::new();
        if let Some(idx) = self.find_bucket(pkt.packet_num) {
            let b = &mut self.buckets[idx];
            let recvd_idx = pkt.packet_num.wrapping_sub(b.start_num) as usize;
            if recvd_idx < b.recvd.len() {
                b.recvd[recvd_idx] = true;
            }
            let bucket_range = (b.start_num, b.end_num);
            events.push(TradesEvent::GapFilled { packet_num: pkt.packet_num, bucket_seq_range: bucket_range });
            events.push(TradesEvent::Apply(pkt));
        } else {
            // Resend пришёл для давно закрытого bucket'а — игнор.
            events.push(TradesEvent::OutOfOrder { packet_num: pkt.packet_num });
        }
        events
    }

    /// Аналог `tick` но возвращает дополнительно `BucketClosed`-события (recovered/lost).
    /// Используется для прикладного слоя который хочет логировать закрытие bucket'ов.
    /// Стандартный `tick` остаётся обратно-совместимым (возвращает только resend payload'ы).
    pub fn tick_with_events(&mut self, rtt_ms: i64, now_ms: i64) -> (Vec<Vec<u8>>, Vec<TradesEvent>) {
        let mut events: Vec<TradesEvent> = Vec::new();
        let payloads = self.tick_impl(rtt_ms, now_ms, &mut events);
        (payloads, events)
    }

    /// Periodic tick — проверка просроченных bucket'ов + сборка resend payload.
    /// Возвращает `Some(payload)` если нужно отправить `emk_TradesResend` (через `client.send_api_request`).
    /// `rtt_ms` — текущий RoundTripDelay в миллисекундах.
    /// Delphi `CheckMissingTradesPackets` MoonProtoEngine.pas:1483-1549.
    pub fn tick(&mut self, rtt_ms: i64, now_ms: i64) -> Vec<Vec<u8>> {
        let mut events: Vec<TradesEvent> = Vec::new();
        self.tick_impl(rtt_ms, now_ms, &mut events)
    }

    fn tick_impl(&mut self, rtt_ms: i64, now_ms: i64, events: &mut Vec<TradesEvent>) -> Vec<Vec<u8>> {
        // Early-exit без throttle (соответствует Delphi MoonProtoEngine.pas:1494-1495 —
        // `If UsedBuckets = 0 then exit;` СНАЧАЛА, throttle на стороне caller'а).
        if self.used_buckets == 0 {
            return Vec::new();
        }
        // Throttle: не чаще 1 раза в 100мс (между реальными проверками).
        if (now_ms - self.last_check_missing_ms).abs() < 100 {
            return Vec::new();
        }
        self.last_check_missing_ms = now_ms;

        let retry_delay_ms: f64 = rtt_ms.max(250) as f64;
        let min_delay_ms: f64 = 300.0;
        let mut packet_nums: Vec<u16> = Vec::new();

        for b in self.buckets.iter_mut() {
            if !b.active {
                continue;
            }
            let gap_size = b.gap_size();
            let all_recvd = b.recvd.iter().take(gap_size).all(|&r| r);

            if all_recvd || b.retry_count >= MAX_RETRY_COUNT {
                events.push(TradesEvent::BucketClosed {
                    start: b.start_num,
                    end: b.end_num,
                    all_received: all_recvd,
                    retry_count: b.retry_count,
                });
                b.active = false;
                self.used_buckets = self.used_buckets.saturating_sub(1);
                continue;
            }

            // PathDelay = min(1800, max(MinDelay, RetryDelay * (1.2 + retry*0.7)))
            let path_delay_ms: f64 = (retry_delay_ms * (1.2 + b.retry_count as f64 * 0.7))
                .max(min_delay_ms)
                .min(1800.0);

            if ((now_ms - b.last_retry_ms).abs() as f64) > path_delay_ms {
                for j in 0..gap_size {
                    if !b.recvd[j] {
                        packet_nums.push(b.start_num.wrapping_add(j as u16));
                    }
                }
                b.retry_count = b.retry_count.saturating_add(1);
                b.last_retry_ms = now_ms;
            }
        }

        if packet_nums.is_empty() {
            return Vec::new();
        }
        engine_request::trades_resend_batches(&packet_nums)
    }

    /// Количество активных buckets.
    pub fn used_buckets(&self) -> usize {
        self.used_buckets
    }

    pub fn last_packet_num(&self) -> u16 {
        self.last_packet_num
    }
}

/// Распарсить `MPC_TradesResendResponse` payload — список сырых TradesStream пакетов.
/// Wire format (MoonProtoEngine.pas:1897-1921 + MoonProtoCommon.pas:1066-1110):
/// `Byte(count) + [Word(sz_le) + raw_packet_bytes(sz)] × count`.
/// Каждый `raw_packet_bytes` — это полный TradesStream payload (с compressed-flag в конце),
/// который потом можно передать в `commands::trades_stream::parse_trades_packet`.
pub fn parse_trades_resend_response(payload: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    if payload.is_empty() {
        return out;
    }
    let count = payload[0] as usize;
    let mut pos = 1;
    for _ in 0..count {
        if pos + 2 > payload.len() {
            break;
        }
        let sz = u16::from_le_bytes([payload[pos], payload[pos + 1]]) as usize;
        pos += 2;
        if pos + sz > payload.len() {
            break;
        }
        out.push(payload[pos..pos + sz].to_vec());
        pos += sz;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::trades_stream::TradesPacket;

    fn make_pkt(packet_num: u16) -> TradesPacket {
        TradesPacket {
            base_time: 0.0,
            packet_num,
            sections: Vec::new(),
        }
    }

    #[test]
    fn first_packet_starts_state() {
        let mut s = TradesState::new();
        let evs = s.on_packet(make_pkt(100), 1000);
        assert_eq!(evs.len(), 1);
        assert!(matches!(evs[0], TradesEvent::Apply(_)));
        assert_eq!(s.last_packet_num(), 100);
    }

    #[test]
    fn sequential_packets_applied() {
        let mut s = TradesState::new();
        let _ = s.on_packet(make_pkt(100), 1000);
        let evs = s.on_packet(make_pkt(101), 1010);
        assert!(matches!(evs[0], TradesEvent::Apply(_)));
        assert_eq!(s.last_packet_num(), 101);
        assert_eq!(s.used_buckets(), 0);
    }

    #[test]
    fn duplicate_detected() {
        let mut s = TradesState::new();
        let _ = s.on_packet(make_pkt(100), 1000);
        let evs = s.on_packet(make_pkt(100), 1010);
        assert!(matches!(evs[0], TradesEvent::Duplicate));
    }

    #[test]
    fn gap_creates_bucket() {
        let mut s = TradesState::new();
        let _ = s.on_packet(make_pkt(100), 1000);
        let evs = s.on_packet(make_pkt(103), 1010); // gap: 101, 102
        let has_gap = evs.iter().any(|e| matches!(e, TradesEvent::GapDetected { start: 101, end: 102 }));
        let has_apply = evs.iter().any(|e| matches!(e, TradesEvent::Apply(_)));
        assert!(has_gap && has_apply);
        assert_eq!(s.used_buckets(), 1);
    }

    #[test]
    fn out_of_order_fills_gap() {
        let mut s = TradesState::new();
        let _ = s.on_packet(make_pkt(100), 1000);
        let _ = s.on_packet(make_pkt(103), 1010); // creates bucket [101, 102]
        let evs = s.on_packet(make_pkt(101), 1020); // fills bucket
        let has_filled = evs.iter().any(|e| matches!(e, TradesEvent::GapFilled { packet_num: 101, .. }));
        assert!(has_filled);
    }

    #[test]
    fn tick_emits_resend_after_path_delay() {
        let mut s = TradesState::new();
        let _ = s.on_packet(make_pkt(100), 1000);
        let _ = s.on_packet(make_pkt(105), 1010); // gap [101..104]
        // Через 500мс с RTT 250 — PathDelay = 250 * 1.2 = 300мс → 500 > 300 → resend.
        let payloads = s.tick(250, 1500);
        assert_eq!(payloads.len(), 1, "должен быть один батч resend");
        // payload должен содержать 4 packet_nums (101, 102, 103, 104).
    }

    #[test]
    fn tick_throttles_within_100ms() {
        let mut s = TradesState::new();
        let _ = s.on_packet(make_pkt(100), 1000);
        let _ = s.on_packet(make_pkt(105), 1010);
        let _ = s.tick(250, 1500);
        // Сразу же — throttle 100мс ещё активен.
        let payloads = s.tick(250, 1550);
        assert!(payloads.is_empty());
    }

    #[test]
    fn bucket_closes_after_max_retries() {
        let mut s = TradesState::new();
        let _ = s.on_packet(make_pkt(100), 1000);
        let _ = s.on_packet(make_pkt(105), 1010);
        // 3 retry — после 4-го tick'а bucket должен быть закрыт.
        for i in 0..MAX_RETRY_COUNT as i64 + 1 {
            let _ = s.tick(250, 1500 + i * 5000);
        }
        // Bucket должен быть закрыт.
        assert_eq!(s.used_buckets(), 0);
    }

    #[test]
    fn parse_resend_response_simple() {
        // count=2, 2 пакета по 3 байта.
        let payload: Vec<u8> = vec![
            2,             // count
            3, 0,          // sz=3
            0xAA, 0xBB, 0xCC,
            3, 0,
            0x11, 0x22, 0x33,
        ];
        let packets = parse_trades_resend_response(&payload);
        assert_eq!(packets.len(), 2);
        assert_eq!(packets[0], vec![0xAA, 0xBB, 0xCC]);
        assert_eq!(packets[1], vec![0x11, 0x22, 0x33]);
    }

    #[test]
    fn parse_resend_response_truncated() {
        // count=2, но второй пакет не помещается.
        let payload: Vec<u8> = vec![2, 3, 0, 0xAA, 0xBB, 0xCC, 5, 0, 0x11];
        let packets = parse_trades_resend_response(&payload);
        assert_eq!(packets.len(), 1);
    }

    #[test]
    fn consecutive_gaps_extend_existing_bucket() {
        // Сценарий: пакеты 100, [gap 101..104], 105 (sequential!), [gap 106..109], 110.
        // Должны получить ОДИН расширенный bucket [101..109], а не два.
        let mut s = TradesState::new();
        let _ = s.on_packet(make_pkt(100), 1000);
        let _ = s.on_packet(make_pkt(105), 1010); // gap [101..104] → bucket1
        assert_eq!(s.used_buckets(), 1);
        let _ = s.on_packet(make_pkt(110), 1020); // gap [106..109] → extend bucket1 до [101..109]
        // Bucket должен расшириться, а не создать второй.
        assert_eq!(s.used_buckets(), 1, "extend должен переиспользовать существующий bucket");
        // Найдём bucket и проверим что end_num = 109, и Recvd[4] (= packet 105) = true.
        let bucket = s.buckets.iter().find(|b| b.active).unwrap();
        assert_eq!(bucket.start_num, 101);
        assert_eq!(bucket.end_num, 109);
        assert!(bucket.recvd[4], "packet 105 (sequential между gap'ами) должен быть помечен как received");
        // Запросы resend пойдут только за [101..104, 106..109] (8 packets).
    }

    #[test]
    fn overflow_gap_resets_buckets_but_applies_packet_like_delphi() {
        // Если gap превышает MAX_RECVD_SIZE, Delphi сбрасывает buckets, но не
        // выбрасывает текущий пакет.
        let mut s = TradesState::new();
        let _ = s.on_packet(make_pkt(0), 1000);
        let _ = s.on_packet(make_pkt(2900), 1010); // bucket [1..2899]
        assert_eq!(s.used_buckets(), 1);

        // Теперь новый gap [2901..N] больше MAX_RECVD_SIZE → reset + Apply.
        let evs = s.on_packet(make_pkt(7000), 1020);
        assert_eq!(s.used_buckets(), 0);
        assert!(evs.iter().any(|e| matches!(e, TradesEvent::Apply(pkt) if pkt.packet_num == 7000)));
        assert!(!evs.iter().any(|e| matches!(e, TradesEvent::GapDetected { .. })));

        // Следующий пакет стартует tracking заново, потому что reset оставил
        // trades_started=false как в Delphi ResetGapBuckets.
        let evs = s.on_packet(make_pkt(7001), 1030);
        assert!(evs.iter().any(|e| matches!(e, TradesEvent::Apply(pkt) if pkt.packet_num == 7001)));
        assert_eq!(s.last_packet_num(), 7001);
    }

    #[test]
    fn max_sized_gap_is_accepted() {
        // gap_size = packet_num - last - 1 (missing range [last+1 .. packet_num-1]).
        // Если gap_size == MAX_RECVD_SIZE — bucket должен создаться без overflow.
        let mut s = TradesState::new();
        let first = 100u16;
        let next = first.wrapping_add(MAX_RECVD_SIZE as u16 + 1);
        let _ = s.on_packet(make_pkt(first), 1000);

        let evs = s.on_packet(make_pkt(next), 1010);

        assert!(
            evs.iter().any(|e| matches!(e, TradesEvent::GapDetected { start, end }
                if *start == first.wrapping_add(1) && *end == next.wrapping_sub(1))),
            "gap with exactly MAX_RECVD_SIZE missing packets must create a bucket"
        );
        assert_eq!(s.used_buckets(), 1);
    }

    #[test]
    fn pause_resets_buckets() {
        let mut s = TradesState::new();
        let _ = s.on_packet(make_pkt(100), 1000);
        let _ = s.on_packet(make_pkt(105), 1010); // creates bucket
        assert_eq!(s.used_buckets(), 1);
        // Через 31 сек — пауза.
        let evs = s.on_packet(make_pkt(200), 1000 + 31_000);
        assert_eq!(s.used_buckets(), 0); // reset
        assert!(evs.iter().any(|e| matches!(e, TradesEvent::Apply(_))));
        assert_eq!(s.last_packet_num(), 200);
    }
}
