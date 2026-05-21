use super::slider::Slider;
use crate::crypto;
use crate::crypto::Aes128Gcm;
/// MPC_Crypted envelope — decryption of encrypted commands.
/// Byte-exact port of TMoonProtoClient.DeCrypt from MoonProtoIntStruct.pas:1127-1150.
///
/// Wire format of encrypted payload:
///   AES-GCM envelope: IV(12) + Tag(16) + Ciphertext
///   After decryption, plaintext starts with TMoonProtoCryptoHeader (12 bytes):
///     Rnd: u16 (2 bytes, random padding)
///     MsgNum: u64 (8 bytes, monotonic message counter)
///     cmd: u8 (1 byte, real command)
///     WantACK: u8 (1 byte, boolean)
///   Followed by the actual command payload.
use log::warn;

pub const CRYPTO_HEADER_SIZE: usize = 12;

/// TMoonProtoCryptoHeader — 12 bytes packed
#[derive(Debug, Clone, Copy)]
pub struct CryptoHeader {
    pub rnd: u16,
    pub msg_num: u64,
    pub cmd: u8,
    pub want_ack: bool,
}

impl CryptoHeader {
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < CRYPTO_HEADER_SIZE {
            return None;
        }
        Some(Self {
            rnd: u16::from_le_bytes(data[0..2].try_into().unwrap()),
            msg_num: u64::from_le_bytes(data[2..10].try_into().unwrap()),
            cmd: data[10],
            want_ack: data[11] != 0,
        })
    }
}

/// Decrypt an MPC_Crypted payload.
/// Returns (cmd, payload, want_ack) or None if decryption/replay fails.
/// Matches TMoonProtoClient.DeCrypt exactly.
///
/// `decode_cipher` — кэшированный `Aes128Gcm` (B-V2-03), построенный из
/// `MPKeys[not ServerSide]`. Для клиента (ServerSide=false) это `MPKeys[true]`.
/// Хранится в `Client::decode_cipher` и обновляется при handshake.
pub fn decrypt_command(
    decode_cipher: &Aes128Gcm,
    encrypted_data: &[u8],
    slider: &mut Slider,
) -> Option<(u8, Vec<u8>, bool)> {
    let mut plaintext = match crypto::decrypt_with_cipher(decode_cipher, encrypted_data, &[]) {
        Some(pt) => pt,
        None => {
            // GCM tag mismatch / PKCS7 fail — corrupt packet или wrong key.
            // Throttle на caller'е, здесь — обычный warn target для фильтрации.
            warn!(target: "moonproto::crypted", "AES-GCM decrypt failed (tag mismatch or bad padding)");
            return None;
        }
    };

    if plaintext.len() < CRYPTO_HEADER_SIZE {
        warn!(target: "moonproto::crypted", "decrypted plaintext too short: {} < {}", plaintext.len(), CRYPTO_HEADER_SIZE);
        return None;
    }

    let hdr = CryptoHeader::from_bytes(&plaintext)?;

    // Replay protection via slider
    let is_new = slider.check_revd(hdr.msg_num);
    if !is_new {
        warn!(target: "moonproto::crypted", "replay/duplicate detected: msg_num={} cmd={}", hdr.msg_num, hdr.cmd);
        return None;
    }

    // B-04 fix: drain первые 12 байт вместо `plaintext[12..].to_vec()` —
    // переиспользуем owned Vec, на одну аллокацию меньше per Crypted packet.
    plaintext.drain(..CRYPTO_HEADER_SIZE);
    Some((hdr.cmd, plaintext, hdr.want_ack))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crypto_header_parse() {
        let data = [
            0x12, 0x34, // rnd
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // msg_num = 1
            0x0A, // cmd
            0x01, // want_ack = true
            0xDE, 0xAD, // payload
        ];
        let hdr = CryptoHeader::from_bytes(&data).unwrap();
        assert_eq!(hdr.msg_num, 1);
        assert_eq!(hdr.cmd, 0x0A);
        assert!(hdr.want_ack);
    }
}
