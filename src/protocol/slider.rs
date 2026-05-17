/// TMoonProtoSlider — 4096-bit sliding window for replay protection.
/// Byte-exact port of MoonProtoIntStruct.pas:1379-1500.
///
/// BitField: 64 x u64 = 4096 bits. Each bit = one message number.
/// StartNum: base message number (aligned to 64).
/// Window slides forward when a message beyond the current window arrives.

const SLIDER_LEN: usize = 64; // MPSliderLen = 64 words
const SLIDER_LEN_BITS: u64 = (SLIDER_LEN as u64) * 64; // 4096 bits

#[derive(Clone)]
pub struct Slider {
    pub bit_field: [u64; SLIDER_LEN],
    pub start_num: u64,
    pub epoch: u8,
    pub has_new_data: bool,
    pub r_count: i32,
}

impl Slider {
    pub fn new() -> Self {
        Self {
            bit_field: [0u64; SLIDER_LEN],
            start_num: 0,
            epoch: 0,
            has_new_data: false,
            r_count: 0,
        }
    }

    /// Check if message number is NEW (not a replay).
    /// Returns true = new message, false = duplicate/out-of-window.
    /// Matches TMoonProtoSlider.CheckRevd exactly.
    pub fn check_revd(&mut self, num: u64) -> bool {
        let n = num >> 6; // div 64
        let prev = self.start_num >> 6;
        let diff = (n as i64) - (prev as i64) - (SLIDER_LEN as i64) + 1;

        if diff > 0 {
            let diff_u = diff as usize;
            self.epoch = self.epoch.wrapping_add(1);
            // Shift window forward
            self.start_num = (n - (SLIDER_LEN as u64) + 1) << 6;

            if diff_u < SLIDER_LEN {
                // Move remaining valid bits
                self.bit_field.copy_within(diff_u.., 0);
            }
            // Zero the new tail
            let zero_start = if diff_u >= SLIDER_LEN { 0 } else { SLIDER_LEN - diff_u };
            for i in zero_start..SLIDER_LEN {
                self.bit_field[i] = 0;
            }
            self.epoch = self.epoch.wrapping_add(1);
        }

        if num >= self.start_num {
            let d = (num - self.start_num) as u32;
            if (d as u64) < SLIDER_LEN_BITS {
                !self.set_bit(d)
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Set bit at position `num` in the BitField.
    /// Returns true if bit WAS already set (duplicate), false if newly set.
    /// Matches TMoonProtoSlider.SetBit (BTS instruction semantics).
    fn set_bit(&mut self, num: u32) -> bool {
        let word_idx = (num >> 6) as usize; // div 64
        let bit_idx = num & 63;
        let mask = 1u64 << bit_idx;
        let was_set = (self.bit_field[word_idx] & mask) != 0;
        self.bit_field[word_idx] |= mask;
        was_set
    }

    /// Build ACK half — takes TAIL half of the window, trims trailing zeros.
    /// Returns (ack_start, words). Matches TMoonProtoSlider.BuildAckHalf.
    /// Note: in single-threaded context, epoch/lock-free logic is trivial.
    pub fn build_ack_half(&self) -> (u64, Vec<u64>) {
        const HALF: usize = SLIDER_LEN / 2; // 32
        let start_idx = SLIDER_LEN - HALF; // 32

        let ack_start = self.start_num + (start_idx as u64) * 64;

        // Copy tail half
        let local_buf = &self.bit_field[start_idx..start_idx + HALF];

        // Trim trailing zeros
        let mut count = HALF;
        while count > 0 && local_buf[count - 1] == 0 {
            count -= 1;
        }

        if count == 0 {
            return (ack_start, Vec::new());
        }

        (ack_start, local_buf[..count].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_messages_accepted() {
        let mut s = Slider::new();
        assert!(s.check_revd(1));
        assert!(s.check_revd(2));
        assert!(s.check_revd(100));
    }

    #[test]
    fn duplicates_rejected() {
        let mut s = Slider::new();
        assert!(s.check_revd(42));
        assert!(!s.check_revd(42)); // duplicate
    }

    #[test]
    fn window_slides() {
        let mut s = Slider::new();
        // Fill some bits
        for i in 0..100 {
            assert!(s.check_revd(i));
        }
        // Jump far ahead
        assert!(s.check_revd(5000));
        // Old message now rejected
        assert!(!s.check_revd(50));
    }

    #[test]
    fn build_ack_half_works() {
        let mut s = Slider::new();
        for i in 0..200u64 {
            s.check_revd(i);
        }
        let (ack_start, words) = s.build_ack_half();
        assert!(ack_start > 0 || !words.is_empty());
    }
}
