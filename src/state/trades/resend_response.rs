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
