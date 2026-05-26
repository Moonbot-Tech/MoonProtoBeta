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

use crate::commands::trades_stream::TradesPacket;

mod gap_bucket;
mod packet_tracking;
mod recovery;
mod resend_response;
mod types;

use self::gap_bucket::{is_packet_in_range, GapBucket};
pub use self::resend_response::{iter_trades_resend_response, TradesResendResponsePackets};
pub use self::types::TradesEvent;
pub(crate) use self::types::TradesPacketEffect;

const MAX_GAP_BUCKETS: usize = 50;
const DEFAULT_RECVD_SIZE: usize = 100;
const MAX_RECVD_SIZE: usize = 3000;
const MAX_RETRY_COUNT: u8 = 3;
/// Пауза, после которой клиент сбрасывает gap-state и начинает заново (мс).
/// Delphi: `TRADES_PAUSE_TIMEOUT = 30 / 86400` (30 сек).
const TRADES_PAUSE_TIMEOUT_MS: i64 = 30_000;

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

    /// Количество активных buckets.
    pub fn used_buckets(&self) -> usize {
        self.used_buckets
    }

    pub fn last_packet_num(&self) -> u16 {
        self.last_packet_num
    }
}

#[cfg(test)]
mod tests;
