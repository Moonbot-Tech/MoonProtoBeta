use crate::commands::registry::read_string;

/// Safe sequential reader for the `TEngineResponse.DataStream` payload.
#[doc(hidden)]
pub(crate) struct EngineStreamReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> EngineStreamReader<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    #[cfg(test)]
    pub(crate) fn position(&self) -> usize {
        self.pos
    }
    pub(crate) fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    pub(crate) fn read_byte(&mut self) -> Option<u8> {
        Some(self.read_zero_tail::<1>()[0])
    }
    pub(crate) fn read_bool(&mut self) -> Option<bool> {
        self.read_byte().map(|b| b != 0)
    }

    pub(crate) fn read_word(&mut self) -> Option<u16> {
        Some(u16::from_le_bytes(self.read_zero_tail::<2>()))
    }

    pub(crate) fn read_int(&mut self) -> Option<i32> {
        Some(i32::from_le_bytes(self.read_zero_tail::<4>()))
    }

    pub(crate) fn read_int64(&mut self) -> Option<i64> {
        Some(i64::from_le_bytes(self.read_zero_tail::<8>()))
    }

    pub(crate) fn read_double(&mut self) -> Option<f64> {
        Some(f64::from_le_bytes(self.read_zero_tail::<8>()))
    }

    pub(crate) fn read_str(&mut self) -> Option<String> {
        read_string(self.data, &mut self.pos)
    }

    /// Read i32 count like Delphi `resp.ReadInt`.
    pub(crate) fn read_count(&mut self) -> Option<usize> {
        let raw = self.read_int()?;
        if raw < 0 {
            log::warn!(target: "moonproto::commands",
                "read_count: negative count {} rejected", raw);
            return None;
        }
        Some(raw as usize)
    }

    pub(crate) fn bounded_count_capacity(&self, count: usize, min_elem_size: usize) -> usize {
        self.remaining()
            .checked_div(min_elem_size)
            .map_or(count, |max| count.min(max))
    }

    pub(crate) fn read_count_bounded(
        &mut self,
        min_elem_size: usize,
        hard_max: usize,
        label: &str,
    ) -> Option<usize> {
        let count = self.read_count()?;
        if count > hard_max {
            log::warn!(target: "moonproto::commands",
                "{}: count {} exceeds hard max {}", label, count, hard_max);
            return None;
        }
        let max_by_remaining = self.remaining().checked_div(min_elem_size).unwrap_or(0);
        if count > max_by_remaining {
            log::warn!(target: "moonproto::commands",
                "{}: count {} exceeds remaining payload: remaining={}, min_elem_size={}, max={}",
                label, count, self.remaining(), min_elem_size, max_by_remaining);
            return None;
        }
        Some(count)
    }

    fn read_zero_tail<const N: usize>(&mut self) -> [u8; N] {
        let mut out = [0u8; N];
        let available = self.remaining().min(N);
        if available > 0 {
            out[..available].copy_from_slice(&self.data[self.pos..self.pos + available]);
            self.pos += available;
        }
        out
    }
}
