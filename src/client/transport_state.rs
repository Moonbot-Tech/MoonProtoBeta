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

    pub(crate) fn refresh_last_checked_from_unacked(&mut self, fallback: i64) {
        self.last_checked = (0..self.blocks_count)
            .filter(|&block| !self.is_block_acked(block))
            .map(|block| self.piece_last_checked[block])
            .min()
            .unwrap_or(fallback);
    }
}

#[derive(Clone, Copy)]
pub(crate) struct SlicedAck {
    pub(crate) flags: [u8; 32],
    pub(crate) datagram_num: u16,
}

pub(crate) struct DataReadState {
    pub(crate) decode_cipher: Option<crate::crypto::Aes128Gcm>,
    pub(crate) slider: Slider,
    pub(crate) data_size_ack_series_num: u16,
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
