//! TradesStream sync state — gap detection + resend protocol + batch response parser.
//!
//! Delphi source:
//! `MoonProtoEngine.pas:21-36, 1364-1549, 1553-1921` (`TGapBucket`,
//! `ResetGapBuckets`, `CreateGapBucket`, `FindBucketForPacket`,
//! `CheckMissingTradesPackets`, `ProcessTradesStream`,
//! `ProcessTradesResendBatch`).
//!
//! The server sends `MPC_TradesStream` packets with wrapping `packet_num: u16`.
//! The client tracks sequence gaps. A missing range creates a `GapBucket`; the
//! tail check sends `TradesResend` requests for missing packet numbers, with up
//! to three retries and exponential backoff. The server answers with
//! `MPC_TradesResendResponse`, a batch of raw inner TradesStream packets.
//!
//! Low-level usage:
//!
//! ```ignore
//! let mut trades = TradesState::new();
//!
//! // 1. Normal MPC_TradesStream packet:
//! let events = trades.on_packet(parsed_trades_packet, now_ms);
//! for ev in events {
//!     match ev {
//!         TradesEvent::Applied { packet_num, .. } => /* read new rows from SeqRing */,
//!         TradesEvent::GapDetected { start, end } => /* log only */,
//!     }
//! }
//!
//! // 2. MPC_TradesResendResponse: iterate inner packets and apply them:
//! for raw_pkt in iter_trades_resend_response(payload) {
//!     if let Some(tp) = commands::trades_stream::parse_trades_packet(raw_pkt) {
//!         let _evts = trades.on_packet_resend(tp); // resend does not advance last_packet_num
//!     }
//! }
//!
//! // 3. Delphi-equivalent tail check after a successfully parsed trades packet:
//! for resend_payload in trades.tick(rtt_ms, now_ms) {
//!     client.send_api_request(&resend_payload);
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
/// Pause after which the client resets gap state and starts tracking anew.
///
/// Delphi: `TRADES_PAUSE_TIMEOUT = 30 / 86400` (30 seconds).
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

/// TradesStream sequence/gap recovery state.
#[derive(Debug, Clone)]
pub struct TradesState {
    buckets: [GapBucket; MAX_GAP_BUCKETS],
    used_buckets: usize,
    last_packet_num: u16,
    last_packet_time_ms: i64,
    trades_started: bool,
    last_check_missing_ms: i64,
    /// Delphi `LastLargeRecvdTime`: момент последнего роста `recvd` выше
    /// `DEFAULT_RECVD_SIZE`. Используется для ленивого урезания памяти раз в 30 мин.
    last_large_recvd_ms: i64,
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
            last_large_recvd_ms: 0,
        }
    }

    /// Reset all gap buckets.
    ///
    /// Delphi source: `ResetGapBuckets`, `MoonProtoEngine.pas:1364-1378`.
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

    /// Full reset, for example after server token change or reconnect.
    pub fn full_reset(&mut self) {
        self.full_reset_at(0);
    }

    pub(crate) fn full_reset_at(&mut self, now_ms: i64) {
        self.reset_gap_buckets(now_ms);
        self.last_packet_num = 0;
    }

    /// Number of active gap buckets.
    pub fn used_buckets(&self) -> usize {
        self.used_buckets
    }

    pub fn last_packet_num(&self) -> u16 {
        self.last_packet_num
    }
}

#[cfg(test)]
mod tests;
