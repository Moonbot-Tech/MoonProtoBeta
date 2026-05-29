use crate::commands::registry::read_string;

/// Safe sequential reader for the `TEngineResponse.DataStream` payload.
#[doc(hidden)]
pub struct EngineStreamReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> EngineStreamReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub fn position(&self) -> usize {
        self.pos
    }
    pub fn len(&self) -> usize {
        self.data.len()
    }
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    pub fn read_u8(&mut self) -> Option<u8> {
        Some(self.read_zero_tail::<1>()[0])
    }
    pub fn read_bool(&mut self) -> Option<bool> {
        self.read_u8().map(|b| b != 0)
    }
    pub fn read_byte(&mut self) -> Option<u8> {
        self.read_u8()
    }

    pub fn read_u16(&mut self) -> Option<u16> {
        Some(u16::from_le_bytes(self.read_zero_tail::<2>()))
    }
    pub fn read_word(&mut self) -> Option<u16> {
        self.read_u16()
    }

    pub fn read_i32(&mut self) -> Option<i32> {
        Some(i32::from_le_bytes(self.read_zero_tail::<4>()))
    }
    pub fn read_int(&mut self) -> Option<i32> {
        self.read_i32()
    }

    pub fn read_i64(&mut self) -> Option<i64> {
        Some(i64::from_le_bytes(self.read_zero_tail::<8>()))
    }
    pub fn read_int64(&mut self) -> Option<i64> {
        self.read_i64()
    }

    pub fn read_f64(&mut self) -> Option<f64> {
        Some(f64::from_le_bytes(self.read_zero_tail::<8>()))
    }
    pub fn read_double(&mut self) -> Option<f64> {
        self.read_f64()
    }

    pub fn read_str(&mut self) -> Option<String> {
        read_string(self.data, &mut self.pos)
    }

    /// Read i32 count like Delphi `resp.ReadInt`.
    ///
    /// Do not pre-reject `count * elem_size > remaining`: Delphi readers do not
    /// check collection size up front and fail only at the concrete field read.
    /// Callers should use [`Self::bounded_count_capacity`] for allocation only.
    pub fn read_count(&mut self) -> Option<usize> {
        let raw = self.read_int()?;
        if raw < 0 {
            log::warn!(target: "moonproto::commands",
                "read_count: negative count {} rejected", raw);
            return None;
        }
        Some(raw as usize)
    }

    pub fn bounded_count_capacity(&self, count: usize, min_elem_size: usize) -> usize {
        self.remaining()
            .checked_div(min_elem_size)
            .map_or(count, |max| count.min(max))
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
