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
//! `Byte(count) + [Word(sz) + raw_packet] × count`), который active dispatcher
//! проходит без копирования через `iter_trades_resend_response`.
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
//!         TradesEvent::Applied { packet_num, .. } => /* read new rows from SeqRing */,
//!         TradesEvent::GapDetected { start, end } => /* лог только */,
//!     }
//! }
//!
//! // 2. Поступление MPC_TradesResendResponse — пройти каждый inner packet + apply:
//! for raw_pkt in iter_trades_resend_response(payload) {
//!     if let Some(tp) = commands::trades_stream::parse_trades_packet(raw_pkt) {
//!         let _evts = trades.on_packet_resend(tp);  // НЕ tracks (resend пакеты не должны двигать last_packet_num)
//!     }
//! }
//!
//! // 3. Delphi-equivalent tail check after a successfully parsed trades packet:
//! for resend_payload in trades.tick(rtt_ms, now_ms) {
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
    refund_used: bool,
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
            refund_used: false,
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

/// Результат применения TradesStream packet-number state.
#[derive(Debug, Clone)]
pub enum TradesEvent {
    /// Пакет применён.
    ///
    /// Active Lib уже раздаёт rows по market state и retained `SeqRing`
    /// storage до эмита этого события. Событие является лёгким сигналом
    /// "новые rows доступны"; оно намеренно не содержит owned `TradesPacket`,
    /// чтобы hot path не собирал `Vec` ради public callback.
    Applied { packet_num: u16, base_time: f64 },
    /// Обнаружен gap: пропущены packet_num в `[start..=end]`. Bucket создан, retry проверяется через `tick()`.
    GapDetected { start: u16, end: u16 },
    /// Пакет был фактически дубликат (packet_num == last).
    /// Delphi не двигает gap-state для него, но всё равно применяет payload дальше.
    Duplicate,
    /// Пакет пришёл вне диапазона — может быть после reset, отображает packet_num.
    OutOfOrder { packet_num: u16 },
    /// Принят out-of-order пакет, который был помечен в одном из gap-bucket'ов (`recvd[i]=true`).
    GapFilled {
        packet_num: u16,
        bucket_seq_range: (u16, u16),
    },
    /// Recovery tick requested these packet numbers through `emk_TradesResend`.
    ///
    /// This is diagnostic only. The active client sends the request
    /// automatically; applications must not send their own duplicate request
    /// because they saw this event.
    ResendRequested { packet_nums: Vec<u16> },
    /// Bucket закрыт: получены все trades или исчерпан retry лимит.
    BucketClosed {
        start: u16,
        end: u16,
        all_received: bool,
        retry_count: u8,
    },
}

/// Packet-number effect produced before row/state application.
///
/// Delphi decides gap/duplicate/resend bookkeeping from `PacketNum` first and
/// then continues reading the stream rows. Keeping this separate lets the
/// dispatcher iterate decoded sections in wire order and emit only a lightweight
/// applied signal after Active Lib state/storage has been updated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TradesPacketEffect {
    Apply,
    GapDetected {
        start: u16,
        end: u16,
    },
    Duplicate,
    OutOfOrder {
        packet_num: u16,
    },
    GapFilled {
        packet_num: u16,
        bucket_seq_range: (u16, u16),
    },
}

impl TradesPacketEffect {
    pub(crate) fn into_event(self, packet_num: u16, base_time: f64) -> TradesEvent {
        match self {
            Self::Apply => TradesEvent::Applied {
                packet_num,
                base_time,
            },
            Self::GapDetected { start, end } => TradesEvent::GapDetected { start, end },
            Self::Duplicate => TradesEvent::Duplicate,
            Self::OutOfOrder { packet_num } => TradesEvent::OutOfOrder { packet_num },
            Self::GapFilled {
                packet_num,
                bucket_seq_range,
            } => TradesEvent::GapFilled {
                packet_num,
                bucket_seq_range,
            },
        }
    }
}

fn materialize_packet_effects(
    effects: Vec<TradesPacketEffect>,
    pkt: TradesPacket,
) -> Vec<TradesEvent> {
    let packet_num = pkt.packet_num;
    let base_time = pkt.base_time;
    effects
        .into_iter()
        .map(|effect| effect.into_event(packet_num, base_time))
        .collect()
}

/// Главный sync state для TradesStream.
#[derive(Debug, Clone)]
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
        self.reset_gap_buckets(self.last_packet_time_ms);
    }

    fn reset_gap_buckets(&mut self, now_ms: i64) {
        for b in self.buckets.iter_mut() {
            b.active = false;
        }
        self.used_buckets = 0;
        self.last_packet_time_ms = now_ms;
        self.trades_started = false;
    }

    /// Полный reset state (например при ServerToken change / reconnect).
    pub fn full_reset(&mut self) {
        self.full_reset_at(0);
    }

    pub(crate) fn full_reset_at(&mut self, now_ms: i64) {
        self.reset_gap_buckets(now_ms);
        self.last_packet_num = 0;
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
                b.refund_used = false;
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
        let oldest_idx = self
            .buckets
            .iter()
            .enumerate()
            .min_by_key(|(_, b)| b.created_ms)
            .map(|(i, _)| i)
            .unwrap_or(0);
        let b = &mut self.buckets[oldest_idx];
        b.start_num = start_num;
        b.end_num = end_num;
        b.created_ms = now_ms;
        b.last_retry_ms = now_ms;
        b.retry_count = 0;
        b.refund_used = false;
        if b.recvd.len() < gap_size {
            b.recvd.resize(gap_size, false);
        } else {
            for r in b.recvd[..gap_size].iter_mut() {
                *r = false;
            }
        }
        // used_buckets не меняется (slot был занят, остался занят).
    }

    /// Найти bucket для packet_num (Delphi `FindBucketForPacket`).
    ///
    /// With `want_extend=true`, this also performs Delphi's adjacent-bucket
    /// extension and updates `last_packet_num` inside the method, matching the
    /// Delphi side effect.
    fn find_bucket_for_packet(
        &mut self,
        packet_num: u16,
        want_extend: bool,
        new_gap_start: u16,
        new_gap_end: u16,
    ) -> Option<usize> {
        if self.used_buckets == 0 {
            return None;
        }
        for (i, b) in self.buckets.iter().enumerate() {
            if b.active && is_packet_in_range(packet_num, b.start_num, b.end_num) {
                return Some(i);
            }
        }
        if !want_extend {
            return None;
        }
        for (i, b) in self.buckets.iter_mut().enumerate() {
            if !b.active {
                continue;
            }
            if b.retry_count >= 2 || b.end_num != new_gap_start.wrapping_sub(2) {
                continue;
            }
            let old_size = b.end_num.wrapping_sub(b.start_num) as usize + 1;
            let new_size = new_gap_end.wrapping_sub(b.start_num) as usize + 1;
            if new_size > MAX_RECVD_SIZE {
                continue;
            }
            if b.recvd.len() < new_size {
                b.recvd.resize(new_size, false);
            }
            if old_size < b.recvd.len() {
                b.recvd[old_size] = true;
            }
            if old_size + 1 < new_size {
                for recvd in b.recvd[(old_size + 1)..new_size].iter_mut() {
                    *recvd = false;
                }
            }
            b.end_num = new_gap_end;
            if b.retry_count >= 1 && !b.refund_used {
                b.retry_count = b.retry_count.saturating_sub(1);
                b.refund_used = true;
            }
            self.last_packet_num = packet_num;
            return Some(i);
        }
        None
    }

    /// Обработать MPC_TradesStream packet-number state (track packets = true).
    /// Делает то же что Delphi `ProcessTradesStream(TrackPackets=True)` MoonProtoEngine.pas:1553+.
    ///
    /// Low-level callers that still parse owned [`TradesPacket`] get only a
    /// lightweight [`TradesEvent::Applied`] notification; row storage belongs
    /// to the active dispatcher/SeqRing path.
    #[must_use = "TradesEvents must be processed for diagnostics and gap recovery"]
    pub fn on_packet(&mut self, pkt: TradesPacket, now_ms: i64) -> Vec<TradesEvent> {
        let effects = self.on_packet_header(pkt.packet_num, now_ms);
        materialize_packet_effects(effects, pkt)
    }

    /// Packet-number branch of `ProcessTradesStream(TrackPackets=True)`.
    ///
    /// This deliberately takes only `packet_num`. Delphi performs this
    /// bookkeeping before the row-reading loop, so the Rust dispatcher can do
    /// the same and apply decoded sections directly without building an owned
    /// packet for public callbacks.
    pub(crate) fn on_packet_header(
        &mut self,
        packet_num: u16,
        now_ms: i64,
    ) -> Vec<TradesPacketEffect> {
        let mut events = Vec::new();

        // === Первый пакет ИЛИ долгая пауза → reset ===
        let pause_detected = self.trades_started
            && self.last_packet_time_ms != 0
            && (now_ms - self.last_packet_time_ms).abs() > TRADES_PAUSE_TIMEOUT_MS;

        if !self.trades_started || pause_detected {
            self.reset_gap_buckets(now_ms);
            self.trades_started = true;
            self.last_packet_num = packet_num;
            self.last_packet_time_ms = now_ms;
            events.push(TradesPacketEffect::Apply);
            return events;
        }

        // === Дубликат ===
        // Delphi `ProcessTradesStream`: ветка `PacketNum = LastTradesPacketNum`
        // только логирует duplicate; после tracking-блока процедура всё равно
        // читает секции и применяет trades. Сохраняем это: сначала diagnostic
        // event, затем Apply того же payload.
        if packet_num == self.last_packet_num {
            self.last_packet_time_ms = now_ms;
            events.push(TradesPacketEffect::Duplicate);
            events.push(TradesPacketEffect::Apply);
            return events;
        }

        // === Sequential: packet_num == last + 1 ===
        if packet_num == self.last_packet_num.wrapping_add(1) {
            self.last_packet_num = packet_num;
            self.last_packet_time_ms = now_ms;
            events.push(TradesPacketEffect::Apply);
            return events;
        }

        // === Out-of-order или Gap ===
        let last = self.last_packet_num;
        // packet_num > last+1 → новый gap. Missing range is [last+1 .. packet_num-1].
        let gap_size = packet_num.wrapping_sub(last.wrapping_add(1)) as usize;

        // Если packet_num фактически "впереди" last (forward gap), создаём bucket.
        // Wrap-safe forward detection: packet_num != last && packet_num != last+1.
        // Различаем forward gap (gap_size небольшой) от backward (resend matching).

        let new_gap_start = last.wrapping_add(1);
        let new_gap_end = packet_num.wrapping_sub(1);

        // Сначала проверяем — это packet из существующего bucket или соседний
        // gap, который Delphi `FindBucketForPacket(... WantExtend=True ...)`
        // расширит внутри того же метода.
        if let Some(idx) = self.find_bucket_for_packet(packet_num, true, new_gap_start, new_gap_end)
        {
            let b = &mut self.buckets[idx];
            if is_packet_in_range(packet_num, b.start_num, b.end_num) {
                let recvd_idx = packet_num.wrapping_sub(b.start_num) as usize;
                if recvd_idx < b.recvd.len() {
                    b.recvd[recvd_idx] = true;
                }
                let bucket_range = (b.start_num, b.end_num);
                self.last_packet_time_ms = now_ms;
                events.push(TradesPacketEffect::GapFilled {
                    packet_num,
                    bucket_seq_range: bucket_range,
                });
                events.push(TradesPacketEffect::Apply);
                return events;
            }
            events.push(TradesPacketEffect::GapDetected {
                start: new_gap_start,
                end: new_gap_end,
            });
        }
        if !events
            .iter()
            .any(|ev| matches!(ev, TradesPacketEffect::GapDetected { .. }))
        {
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
                self.reset_gap_buckets(now_ms);
                events.push(TradesPacketEffect::Apply);
                return events;
            }

            self.create_bucket(new_gap_start, new_gap_end, now_ms);
            events.push(TradesPacketEffect::GapDetected {
                start: new_gap_start,
                end: new_gap_end,
            });
        }

        self.last_packet_num = packet_num;
        self.last_packet_time_ms = now_ms;
        events.push(TradesPacketEffect::Apply);
        events
    }

    /// Обработать пакет из MPC_TradesResendResponse (track packets = false).
    /// Не двигает last_packet_num, только помечает recvd в buckets.
    /// Delphi `ProcessTradesStream(TrackPackets=False)` ветка (MoonProtoEngine.pas:1667-1675).
    pub fn on_packet_resend(&mut self, pkt: TradesPacket) -> Vec<TradesEvent> {
        let effects = self.on_packet_resend_header(pkt.packet_num);
        materialize_packet_effects(effects, pkt)
    }

    /// Packet-number branch of `ProcessTradesStream(TrackPackets=False)`.
    pub(crate) fn on_packet_resend_header(&mut self, packet_num: u16) -> Vec<TradesPacketEffect> {
        let mut events = Vec::new();
        if let Some(idx) = self.find_bucket_for_packet(packet_num, false, 0, 0) {
            let b = &mut self.buckets[idx];
            let recvd_idx = packet_num.wrapping_sub(b.start_num) as usize;
            if recvd_idx < b.recvd.len() {
                b.recvd[recvd_idx] = true;
            }
            let bucket_range = (b.start_num, b.end_num);
            events.push(TradesPacketEffect::GapFilled {
                packet_num,
                bucket_seq_range: bucket_range,
            });
        } else {
            // Resend пришёл для давно закрытого bucket'а. Delphi TrackPackets=False
            // не помечает bucket, но всё равно ниже разбирает секции и применяет
            // trades; поэтому отдаём diagnostic OutOfOrder + Apply.
            events.push(TradesPacketEffect::OutOfOrder { packet_num });
        }
        events.push(TradesPacketEffect::Apply);
        events
    }

    /// Аналог `tick` но возвращает дополнительно `BucketClosed`-события (recovered/lost).
    /// Используется для прикладного слоя который хочет логировать закрытие bucket'ов.
    /// Стандартный `tick` остаётся обратно-совместимым (возвращает только resend payload'ы).
    pub fn tick_with_events(
        &mut self,
        rtt_ms: i64,
        now_ms: i64,
    ) -> (Vec<Vec<u8>>, Vec<TradesEvent>) {
        let mut events: Vec<TradesEvent> = Vec::new();
        let payloads = self.tick_impl(rtt_ms, now_ms, &mut events);
        (payloads, events)
    }

    /// Tail tick — проверка просроченных bucket'ов + сборка resend payload.
    ///
    /// Delphi вызывает `CheckMissingTradesPackets` только в хвосте успешного
    /// `ProcessTradesStream`, под внешним `LastCheckMissingTime` throttle 100мс.
    /// Поэтому active library вызывает этот метод после valid live/resend
    /// trades-пакета, а не по независимому таймеру в тишине канала.
    /// Возвращает `Some(payload)` если нужно отправить `emk_TradesResend` (через `client.send_api_request`).
    /// `rtt_ms` — текущий RoundTripDelay в миллисекундах.
    /// Delphi `CheckMissingTradesPackets` MoonProtoEngine.pas:1483-1549.
    pub fn tick(&mut self, rtt_ms: i64, now_ms: i64) -> Vec<Vec<u8>> {
        let mut events: Vec<TradesEvent> = Vec::new();
        self.tick_impl(rtt_ms, now_ms, &mut events)
    }

    fn tick_impl(
        &mut self,
        rtt_ms: i64,
        now_ms: i64,
        events: &mut Vec<TradesEvent>,
    ) -> Vec<Vec<u8>> {
        // Delphi caller:
        // `If (NowTimeX - LastCheckMissingTime) > 100/MSecsPerDay then begin
        //    CheckMissingTradesPackets;
        //    LastCheckMissingTime := NowTimeX;
        //  end;`
        if now_ms - self.last_check_missing_ms <= 100 {
            return Vec::new();
        }
        self.last_check_missing_ms = now_ms;
        if self.used_buckets == 0 {
            return Vec::new();
        }

        let retry_delay_ms: f64 = rtt_ms.max(250) as f64;
        let min_delay_ms: f64 = 300.0;
        let mut packet_nums: Vec<u16> = Vec::new();

        for b in self.buckets.iter_mut() {
            if !b.active {
                continue;
            }
            let gap_size = b.gap_size();
            let all_recvd = b.recvd.iter().take(gap_size).all(|&r| r);
            // PathDelay = min(1800, max(MinDelay, RetryDelay * (1.2 + retry*0.7)))
            let path_delay_ms: f64 = (retry_delay_ms * (1.2 + b.retry_count as f64 * 0.7))
                .max(min_delay_ms)
                .min(1800.0);

            if all_recvd {
                events.push(TradesEvent::BucketClosed {
                    start: b.start_num,
                    end: b.end_num,
                    all_received: true,
                    retry_count: b.retry_count,
                });
                b.active = false;
                self.used_buckets = self.used_buckets.saturating_sub(1);
                continue;
            }

            if b.retry_count >= MAX_RETRY_COUNT {
                if ((now_ms - b.last_retry_ms).abs() as f64) > path_delay_ms {
                    events.push(TradesEvent::BucketClosed {
                        start: b.start_num,
                        end: b.end_num,
                        all_received: false,
                        retry_count: b.retry_count,
                    });
                    b.active = false;
                    self.used_buckets = self.used_buckets.saturating_sub(1);
                }
                continue;
            }

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
        events.push(TradesEvent::ResendRequested {
            packet_nums: packet_nums.clone(),
        });
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

/// Zero-copy iterator over raw TradesStream packets inside `MPC_TradesResendResponse`.
#[derive(Debug, Clone)]
pub struct TradesResendResponsePackets<'a> {
    payload: &'a [u8],
    pos: usize,
    remaining: usize,
}

impl<'a> Iterator for TradesResendResponsePackets<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        if self.pos + 2 > self.payload.len() {
            self.remaining = 0;
            return None;
        }
        let sz = u16::from_le_bytes([self.payload[self.pos], self.payload[self.pos + 1]]) as usize;
        self.pos += 2;
        if self.pos + sz > self.payload.len() {
            self.remaining = 0;
            return None;
        }
        let packet = &self.payload[self.pos..self.pos + sz];
        self.pos += sz;
        self.remaining -= 1;
        Some(packet)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.remaining))
    }
}

/// Пройти `MPC_TradesResendResponse` payload без копирования inner TradesStream packets.
/// Wire format (MoonProtoEngine.pas:1897-1921 + MoonProtoCommon.pas:1066-1110):
/// `Byte(count) + [Word(sz_le) + raw_packet_bytes(sz)] × count`.
/// Каждый `raw_packet_bytes` — это полный TradesStream payload (с compressed-flag в конце),
/// который потом можно передать в `commands::trades_stream::parse_trades_packet`.
pub fn iter_trades_resend_response(payload: &[u8]) -> TradesResendResponsePackets<'_> {
    if payload.is_empty() {
        TradesResendResponsePackets {
            payload,
            pos: 0,
            remaining: 0,
        }
    } else {
        TradesResendResponsePackets {
            payload,
            pos: 1,
            remaining: payload[0] as usize,
        }
    }
}

#[cfg(test)]
mod tests;
