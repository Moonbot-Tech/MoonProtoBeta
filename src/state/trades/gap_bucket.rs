//! Trades resend gap bucket.

use super::DEFAULT_RECVD_SIZE;

/// Один gap-bucket — диапазон [start_num, end_num] пропущенных packet_num.
#[derive(Debug, Clone)]
pub(super) struct GapBucket {
    pub(super) active: bool,
    pub(super) start_num: u16,
    pub(super) end_num: u16,
    pub(super) created_ms: i64,
    pub(super) last_retry_ms: i64,
    pub(super) retry_count: u8,
    pub(super) refund_used: bool,
    /// Битовая маска полученных packets внутри диапазона (recvd[i] = packet (start_num+i) получен).
    pub(super) recvd: Vec<bool>,
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
    pub(super) fn gap_size(&self) -> usize {
        // Используем wrapping для u16, +1 (inclusive).
        self.end_num.wrapping_sub(self.start_num) as usize + 1
    }
}

/// Wrapping-safe проверка: packet попадает в диапазон [start, end] (включительно).
pub(super) fn is_packet_in_range(packet: u16, start: u16, end: u16) -> bool {
    // wrap-safe: gap_size = end - start + 1 (wrapping)
    let offset = packet.wrapping_sub(start);
    let span = end.wrapping_sub(start);
    offset <= span
}
