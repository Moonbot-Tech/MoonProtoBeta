use super::UniqueKey;
use crate::protocol::slider::Slider;
#[derive(Clone)]
pub(crate) struct ReaderSlicedStats {
    pub(crate) datagram_num: u16,
    pub(crate) dup_count: u8,
    pub(crate) blocks_count: usize,
}

/// Sent Sliced datagram awaiting ACK (matches TMoonProtoSlicedData in Sending list)
pub(crate) struct SentSliced {
    pub(crate) datagram_num: u16,
    pub(crate) slices: Vec<Vec<u8>>, // each slice payload (SliceHeader + data)
    pub(crate) piece_last_checked: Vec<i64>, // per-piece LastChecked timestamp
    pub(crate) ack_flags: [u8; 32],  // which blocks ACK'd
    pub(crate) blocks_count: usize,
    pub(crate) sent_count: usize,
    pub(crate) last_checked: i64, // Min of all piece_last_checked
    pub(crate) retry_count: i32,
    pub(crate) last_retry_inc: i64,
    pub(crate) max_retry_count: i32,
    pub(crate) u_key: UniqueKey, // for UKey dedup (matches TMoonProtoSlicedData.UKey)
}

impl SentSliced {
    #[inline]
    pub(crate) fn is_block_acked(&self, block_num: usize) -> bool {
        self.ack_flags[block_num / 8] & (1 << (block_num % 8)) != 0
    }

    pub(crate) fn merge_ack_flags(&mut self, flags: [u8; 32]) -> bool {
        // Set union, like Delphi `Flags := Flags + ACK.Flags`.
        let mut changed = false;
        for (dst, src) in self.ack_flags.iter_mut().zip(flags) {
            let before = *dst;
            *dst |= src;
            changed |= before != *dst;
        }
        changed
    }

    pub(crate) fn is_fully_acked(&self) -> bool {
        (0..self.blocks_count).all(|block| self.is_block_acked(block))
    }

    pub(crate) fn acked_count(&self) -> usize {
        (0..self.blocks_count)
            .filter(|&block| self.is_block_acked(block))
            .count()
    }

    pub(crate) fn block_due_for_retry(
        &self,
        block_num: usize,
        prev_last_checked: i64,
        cur_tm: i64,
        path_delay: i64,
    ) -> bool {
        if self.is_block_acked(block_num) {
            return false;
        }
        if self.piece_last_checked[block_num] != prev_last_checked {
            return false;
        }
        (cur_tm - self.piece_last_checked[block_num]).abs() > path_delay
    }

    pub(crate) fn mark_block_sent(&mut self, block_num: usize, cur_tm: i64) -> bool {
        let was_retry = self.piece_last_checked[block_num] > 0;
        self.piece_last_checked[block_num] = cur_tm;
        self.sent_count += 1;
        was_retry
    }

    pub(crate) fn should_increment_retry(
        &self,
        prev_last_checked: i64,
        cur_tm: i64,
        path_delay: i64,
        sent_on_path_delay: bool,
    ) -> bool {
        prev_last_checked != self.last_checked
            && sent_on_path_delay
            && (self.last_retry_inc - cur_tm).abs() > path_delay
    }

    pub(crate) fn refresh_last_checked_from_unacked(&mut self, fallback: i64) {
        self.last_checked = (0..self.blocks_count)
            .filter(|&block| !self.is_block_acked(block))
            .map(|block| self.piece_last_checked[block])
            .min()
            .unwrap_or(fallback);
    }
}

#[cfg(test)]
mod sent_sliced_tests {
    use super::*;

    fn sent_sliced(blocks_count: usize) -> SentSliced {
        SentSliced {
            datagram_num: 7,
            slices: vec![Vec::new(); blocks_count],
            piece_last_checked: vec![0; blocks_count],
            ack_flags: [0; 32],
            blocks_count,
            sent_count: 0,
            last_checked: 0,
            retry_count: 0,
            last_retry_inc: 0,
            max_retry_count: 0,
            u_key: UniqueKey::none(),
        }
    }

    #[test]
    fn merge_ack_flags_reports_only_real_progress() {
        let mut s = sent_sliced(10);
        let mut flags = [0u8; 32];
        flags[0] = 0b0000_1010;

        assert!(s.merge_ack_flags(flags));
        assert_eq!(s.acked_count(), 2);
        assert!(!s.is_fully_acked());
        assert!(!s.merge_ack_flags(flags));
    }

    #[test]
    fn full_ack_uses_blocks_count_not_spare_flag_bits() {
        let mut s = sent_sliced(9);
        let mut flags = [0u8; 32];
        flags[0] = 0xFF;
        flags[1] = 0x01;

        assert!(s.merge_ack_flags(flags));
        assert_eq!(s.acked_count(), 9);
        assert!(s.is_fully_acked());
    }

    #[test]
    fn block_due_for_retry_matches_delphi_piece_clock() {
        let mut s = sent_sliced(3);
        s.piece_last_checked = vec![100, 90, 100];
        s.ack_flags[0] = 0b0000_0100;

        assert!(
            s.block_due_for_retry(0, 100, 221, 120),
            "same timestamp group and abs > path delay is due"
        );
        assert!(
            !s.block_due_for_retry(0, 100, 220, 120),
            "Delphi threshold is strict: abs == path_delay is not due"
        );
        assert!(
            !s.block_due_for_retry(1, 100, 221, 120),
            "different timestamp group waits for its own pass"
        );
        assert!(
            !s.block_due_for_retry(2, 100, 221, 120),
            "ACKed block is skipped"
        );
    }

    #[test]
    fn mark_block_sent_reports_retry_and_advances_counters() {
        let mut s = sent_sliced(2);
        assert!(!s.mark_block_sent(0, 50));
        assert_eq!(s.sent_count, 1);
        assert_eq!(s.piece_last_checked[0], 50);

        assert!(s.mark_block_sent(0, 200));
        assert_eq!(s.sent_count, 2);
        assert_eq!(s.piece_last_checked[0], 200);
    }

    #[test]
    fn retry_increment_guard_matches_delphi_conditions() {
        let mut s = sent_sliced(1);
        s.last_checked = 200;
        s.last_retry_inc = 0;

        assert!(s.should_increment_retry(100, 250, 120, true));
        assert!(!s.should_increment_retry(200, 250, 120, true));
        assert!(!s.should_increment_retry(100, 250, 120, false));
        s.last_retry_inc = 200;
        assert!(!s.should_increment_retry(100, 250, 120, true));
    }
}

#[derive(Clone, Copy)]
pub(crate) struct SlicedAck {
    pub(crate) flags: [u8; 32],
    pub(crate) datagram_num: u16,
    pub(crate) session: u32,
}

pub(crate) struct DataReadState {
    pub(crate) decode_cipher: Option<crate::crypto::Aes128Gcm>,
    pub(crate) slider: Slider,
    pub(crate) data_size_ack_series_num: u16,
}

/// Receive/replay state carved out of [`super::Client`].
///
/// Groups the two distinct receive-side sliders that survive a soft reconnect:
/// the Delphi `DataReadInt` receive state ([`DataReadState`] — MPSlider replay
/// bitmap, SizeAck series, decode cipher) and the `RecvdSlider` server-ACK
/// bitmap. They are reset together on `full_reset`, but through different code
/// (`data_read_state.reset()` vs replacing `recvd_slider`), so they stay as two
/// fields here rather than being folded into [`DataReadState`] — that keeps the
/// `DataReadState` contract (DataReadInt receive state) intact. Field names and
/// types are unchanged from when they lived directly on `Client`.
pub(crate) struct RecvState {
    /// Delphi DataReadInt receive state that survives soft reconnect: MPSlider
    /// replay/ACK bitmap, SizeAck series, and decode cipher. TmpSlider lives in
    /// SendLockState so the send phase copies it atomically with ACK queues.
    pub(crate) data_read_state: DataReadState,
    /// Delphi RecvdSlider/TmpSlider: server ACK bitmap from incoming MPC_Ping.
    /// Reader/DataReadInt writes TmpSlider; writer CheckSeningData copies it to
    /// RecvdSlider and only then drops ACKed PendingH.
    pub(crate) recvd_slider: Slider,
}

impl RecvState {
    pub(crate) fn new() -> Self {
        Self {
            data_read_state: DataReadState::new(),
            recvd_slider: Slider::new(),
        }
    }
}

impl DataReadState {
    pub(crate) fn new() -> Self {
        Self {
            decode_cipher: None,
            slider: Slider::new(),
            data_size_ack_series_num: 0,
        }
    }

    pub(crate) fn reset(&mut self) {
        self.slider = Slider::new();
        self.data_size_ack_series_num = 0;
    }

    pub(crate) fn set_decode_cipher(&mut self, cipher: crate::crypto::Aes128Gcm) {
        self.decode_cipher = Some(cipher);
    }

    pub(crate) fn build_ack_half(&self) -> (u64, Vec<u64>) {
        self.slider.build_ack_half()
    }

    pub(crate) fn update_data_size_ack_series_num(&mut self, series_num: u16) -> u16 {
        if self.data_size_ack_series_num != series_num {
            self.data_size_ack_series_num = series_num;
        }
        self.data_size_ack_series_num
    }
}
