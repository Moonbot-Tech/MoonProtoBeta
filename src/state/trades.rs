//! TradesStream recovery state — gap detection + resend protocol + batch response parser.
//!
//! Delphi source:
//! `MoonProtoEngine.pas:21-36, 1364-1549, 1553-1921` (`TGapBucket`,
//! `ResetGapBuckets`, `CreateGapBucket`, `FindBucketForPacket`,
//! `CheckMissingTradesPackets`, `ProcessTradesStream`,
//! `ProcessTradesResendBatch`).
//!
//! The server sends `MPC_TradesStream` packets with wrapping `packet_num: u16`.
//! The active client owns the sequence-gap bookkeeping, sends `TradesResend`
//! requests, applies resend payloads, and queues retained-history work before it
//! publishes the lightweight `TradesEvent::Applied` signal. The event is not a
//! history-worker completion barrier. Applications do not
//! feed packets or drive recovery manually; they subscribe through `MoonClient`
//! and read retained rows from `MarketHistoryReaders`.

#[cfg(test)]
use crate::commands::trades_stream::TradesPacket;

mod gap_bucket;
mod packet_tracking;
mod recovery;
mod resend_response;
mod types;

use self::gap_bucket::{is_packet_in_range, GapBucket};
pub(crate) use self::resend_response::iter_trades_resend_response;
pub use self::types::TradesEvent;
pub(crate) use self::types::{TradesPacketEffect, TradesPacketEffects};

const MAX_GAP_BUCKETS: usize = 50;
const DEFAULT_RECVD_SIZE: usize = 100;
const MAX_RECVD_SIZE: usize = 3000;
const MAX_RETRY_COUNT: u8 = 3;
/// Pause after which the client resets gap state and starts tracking anew.
///
/// Delphi: `TRADES_PAUSE_TIMEOUT = 30 / 86400` (30 seconds).
const TRADES_PAUSE_TIMEOUT_MS: i64 = 30_000;

#[cfg(test)]
fn materialize_packet_effects(effects: TradesPacketEffects, pkt: TradesPacket) -> Vec<TradesEvent> {
    let packet_num = pkt.packet_num;
    let base_time = pkt.base_time;
    let mut events = Vec::with_capacity(effects.len());
    for effect in effects.iter() {
        effect.push_event(packet_num, base_time, &mut events);
    }
    events
}

/// TradesStream sequence/gap recovery state.
#[derive(Debug, Clone)]
pub(crate) struct TradesState {
    buckets: [GapBucket; MAX_GAP_BUCKETS],
    used_buckets: usize,
    last_packet_num: u16,
    last_packet_time_ms: i64,
    trades_started: bool,
    last_check_missing_ms: i64,
    /// Delphi `LastLargeRecvdTime`: timestamp of the last time `recvd` grew
    /// above `DEFAULT_RECVD_SIZE`. Used for the lazy memory shrink every 30 min.
    last_large_recvd_ms: i64,
}

impl Default for TradesState {
    fn default() -> Self {
        Self::new()
    }
}

impl TradesState {
    pub(crate) fn new() -> Self {
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

    fn reset_gap_buckets(&mut self, now_ms: i64) {
        for b in self.buckets.iter_mut() {
            b.active = false;
        }
        self.used_buckets = 0;
        self.last_packet_time_ms = now_ms;
        self.trades_started = false;
    }

    pub(crate) fn full_reset_at(&mut self, now_ms: i64) {
        self.reset_gap_buckets(now_ms);
        self.last_packet_num = 0;
    }

    /// Number of active gap buckets.
    #[cfg(test)]
    pub(crate) fn used_buckets(&self) -> usize {
        self.used_buckets
    }

    #[cfg(test)]
    pub(crate) fn last_packet_num(&self) -> u16 {
        self.last_packet_num
    }
}

#[cfg(test)]
mod tests;
