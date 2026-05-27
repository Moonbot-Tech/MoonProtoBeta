//! Built-in MoonProto transport modes V1/V2.
//!
//! Ported from Delphi `MoonProtoFunc.pas`:
//! - `WrapInSTUN` / `UnwrapFromSTUN` for MaskVer=1;
//! - `DNS_WARMUP_REQUEST`, `DNS_WARMUP_RESPONSE`, `IsDNSWarmup`, and
//!   `DNS_WARMUP_INTERVAL` for MaskVer=2.

const STUN_MAGIC: [u8; 4] = [0x21, 0x12, 0xA4, 0x42];

const DNS_WARMUP_REQUEST: [u8; 17] = [
    0x4D, 0x50, // Transaction ID = "MP"
    0x01, 0x00, // Flags: Standard query
    0x00, 0x01, // QDCOUNT = 1
    0x00, 0x00, // ANCOUNT = 0
    0x00, 0x00, // NSCOUNT = 0
    0x00, 0x00, // ARCOUNT = 0
    0x00, // QNAME: root "."
    0x00, 0x01, // QTYPE = A
    0x00, 0x01, // QCLASS = IN
];

#[cfg(test)]
const DNS_WARMUP_RESPONSE: [u8; 17] = [
    0x4D, 0x50, // Transaction ID = "MP"
    0x81, 0x80, // Flags: response
    0x00, 0x01, // QDCOUNT = 1
    0x00, 0x00, // ANCOUNT = 0
    0x00, 0x00, // NSCOUNT = 0
    0x00, 0x00, // ARCOUNT = 0
    0x00, // QNAME: root "."
    0x00, 0x01, // QTYPE = A
    0x00, 0x01, // QCLASS = IN
];

const DNS_WARMUP_INTERVAL: u32 = 100;

/// Per-client transport-mode state.
///
/// Delphi keeps `SentCountDNS` in `TMoonProtoUDPClient` and resets it before
/// each reconnect bind. Rust keeps the same machine effect here.
#[derive(Debug, Clone, Default)]
pub(crate) struct ClientTransportModeState {
    sent_count_dns: u32,
}

impl ClientTransportModeState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&mut self) {
        self.sent_count_dns = 0;
    }

    fn next_dns_warmup_packet(&mut self) -> Option<Vec<u8>> {
        let packet = if self.sent_count_dns % DNS_WARMUP_INTERVAL == 0 {
            Some(DNS_WARMUP_REQUEST.to_vec())
        } else {
            None
        };
        self.sent_count_dns = self.sent_count_dns.wrapping_add(1);
        packet
    }
}

pub(crate) fn wrap_client_packet(
    data: &mut Vec<u8>,
    mask_ver: u8,
    state: &mut ClientTransportModeState,
) -> Option<Vec<u8>> {
    match mask_ver {
        1 => {
            wrap_in_stun(data, false);
            None
        }
        2 => state.next_dns_warmup_packet(),
        _ => None,
    }
}

pub(crate) fn unwrap_server_packet(raw: &[u8], mask_ver: u8) -> Option<Vec<u8>> {
    match mask_ver {
        1 => unwrap_from_stun(raw),
        2 if is_dns_warmup(raw) => None,
        _ => Some(raw.to_vec()),
    }
}

fn wrap_in_stun(data: &mut Vec<u8>, is_server: bool) {
    let data_len = data.len();
    if data_len == 0 {
        return;
    }

    let type_bytes = if is_server {
        [0x01, 0x01] // Binding Response
    } else {
        [0x00, 0x01] // Binding Request
    };

    if data_len <= 11 {
        let mut wrapped = vec![0u8; 20];
        wrapped[0..2].copy_from_slice(&type_bytes);
        wrapped[2] = 0;
        wrapped[3] = 0;
        wrapped[4..8].copy_from_slice(&STUN_MAGIC);
        wrapped[8] = data_len as u8;
        wrapped[9..9 + data_len].copy_from_slice(data);
        *data = wrapped;
    } else {
        let attr_len = 4 + (data_len - 12);
        let mut wrapped = vec![0u8; 20 + attr_len];
        wrapped[0..2].copy_from_slice(&type_bytes);
        wrapped[2] = (attr_len >> 8) as u8;
        wrapped[3] = (attr_len & 0xFF) as u8;
        wrapped[4..8].copy_from_slice(&STUN_MAGIC);
        wrapped[8..20].copy_from_slice(&data[..12]);
        wrapped[20] = 0x00;
        wrapped[21] = 0x13;
        let payload_len = data_len - 12;
        wrapped[22] = (payload_len >> 8) as u8;
        wrapped[23] = (payload_len & 0xFF) as u8;
        wrapped[24..24 + payload_len].copy_from_slice(&data[12..]);
        *data = wrapped;
    }
}

fn unwrap_from_stun(raw: &[u8]) -> Option<Vec<u8>> {
    if raw.len() < 20 {
        return None;
    }
    if raw[4..8] != STUN_MAGIC {
        return None;
    }
    if !((raw[0] == 0x00 && raw[1] == 0x01) || (raw[0] == 0x01 && raw[1] == 0x01)) {
        return None;
    }

    let len = ((raw[2] as usize) << 8) | raw[3] as usize;
    if len == 0 {
        let payload_len = raw[8] as usize;
        if !(1..=11).contains(&payload_len) {
            return None;
        }
        return Some(raw[9..9 + payload_len].to_vec());
    }

    if raw.len() < 24 {
        return None;
    }
    if raw[20] != 0x00 || raw[21] != 0x13 {
        return None;
    }
    let attr_len = ((raw[22] as usize) << 8) | raw[23] as usize;
    if raw.len() < 24 + attr_len {
        return None;
    }

    let mut unwrapped = Vec::with_capacity(12 + attr_len);
    unwrapped.extend_from_slice(&raw[8..20]);
    unwrapped.extend_from_slice(&raw[24..24 + attr_len]);
    Some(unwrapped)
}

fn is_dns_warmup(data: &[u8]) -> bool {
    if data.len() < 17 {
        return false;
    }
    if data[0] != 0x4D || data[1] != 0x50 {
        return false;
    }
    if !((data[2] == 0x01 && data[3] == 0x00) || (data[2] == 0x81 && data[3] == 0x80)) {
        return false;
    }
    if data[4] != 0x00 || data[5] != 0x01 {
        return false;
    }
    if data[12] != 0x00 {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stun_wrap_short_matches_delphi_layout() {
        let mut data = vec![0xAA, 0xBB, 0xCC];
        wrap_in_stun(&mut data, false);

        assert_eq!(data.len(), 20);
        assert_eq!(&data[0..2], &[0x00, 0x01]);
        assert_eq!(&data[2..4], &[0x00, 0x00]);
        assert_eq!(&data[4..8], &STUN_MAGIC);
        assert_eq!(data[8], 3);
        assert_eq!(&data[9..12], &[0xAA, 0xBB, 0xCC]);
        assert_eq!(&data[12..20], &[0; 8]);
        assert_eq!(
            unwrap_from_stun(&data).expect("valid STUN"),
            vec![0xAA, 0xBB, 0xCC]
        );
    }

    #[test]
    fn stun_wrap_large_matches_delphi_layout() {
        let original: Vec<u8> = (0..32).collect();
        let mut data = original.clone();
        wrap_in_stun(&mut data, true);

        assert_eq!(data.len(), 44);
        assert_eq!(&data[0..2], &[0x01, 0x01]);
        assert_eq!(&data[2..4], &[0x00, 0x18]);
        assert_eq!(&data[4..8], &STUN_MAGIC);
        assert_eq!(&data[8..20], &original[..12]);
        assert_eq!(&data[20..24], &[0x00, 0x13, 0x00, 0x14]);
        assert_eq!(&data[24..], &original[12..]);
        assert_eq!(unwrap_from_stun(&data).expect("valid STUN"), original);
    }

    #[test]
    fn dns_warmup_matches_delphi_layout_and_interval() {
        assert!(is_dns_warmup(&DNS_WARMUP_REQUEST));
        assert!(is_dns_warmup(&DNS_WARMUP_RESPONSE));

        let mut state = ClientTransportModeState::new();
        assert_eq!(
            state.next_dns_warmup_packet(),
            Some(DNS_WARMUP_REQUEST.to_vec())
        );
        for _ in 1..100 {
            assert_eq!(state.next_dns_warmup_packet(), None);
        }
        assert_eq!(
            state.next_dns_warmup_packet(),
            Some(DNS_WARMUP_REQUEST.to_vec())
        );
        state.reset();
        assert_eq!(
            state.next_dns_warmup_packet(),
            Some(DNS_WARMUP_REQUEST.to_vec())
        );
    }
}
