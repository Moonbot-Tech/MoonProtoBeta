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
use zerocopy::byteorder::little_endian::{U16 as LeU16, U64 as LeU64};
use zerocopy::{FromBytes, Immutable, KnownLayout, Unaligned};

pub(crate) const CRYPTO_HEADER_SIZE: usize = std::mem::size_of::<WireCryptoHeader>();
const _: [(); 12] = [(); CRYPTO_HEADER_SIZE];

/// TMoonProtoCryptoHeader — 12 bytes packed
#[derive(Debug, Clone, Copy)]
pub(crate) struct CryptoHeader {
    // Parsed from the wire (random padding) but not read downstream; kept to map
    // the exact 12-byte TMoonProtoCryptoHeader layout. Do not delete/reorder.
    #[allow(dead_code)]
    pub rnd: u16,
    pub msg_num: u64,
    pub cmd: u8,
    pub want_ack: bool,
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
struct WireCryptoHeader {
    rnd: LeU16,
    msg_num: LeU64,
    cmd: u8,
    want_ack: u8,
}

impl CryptoHeader {
    fn from_wire(wire: WireCryptoHeader) -> Self {
        Self {
            rnd: wire.rnd.get(),
            msg_num: wire.msg_num.get(),
            cmd: wire.cmd,
            want_ack: wire.want_ack != 0,
        }
    }

    pub(crate) fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < CRYPTO_HEADER_SIZE {
            return None;
        }
        Some(Self::from_wire(
            WireCryptoHeader::read_from_bytes(&data[..CRYPTO_HEADER_SIZE]).ok()?,
        ))
    }
}

/// Decrypt an MPC_Crypted payload.
/// Returns (cmd, payload, want_ack) or None if decryption/replay fails.
/// Matches TMoonProtoClient.DeCrypt exactly.
///
/// `decode_cipher` is the cached `Aes128Gcm`, built from `MPKeys[not ServerSide]`.
/// For the client (ServerSide=false) this is `MPKeys[true]`. It is stored in
/// `Client::decode_cipher` and updated on handshake.
pub(crate) fn decrypt_command(
    decode_cipher: &Aes128Gcm,
    encrypted_data: &[u8],
    slider: &mut Slider,
) -> Option<(u8, Vec<u8>, bool)> {
    let (hdr, mut plaintext) = decrypt_command_no_replay(decode_cipher, encrypted_data)?;

    // Replay protection via slider. Duplicates are expected with UDP retries and
    // are a normal drop path, so keep them out of the default warning log.
    let is_new = slider.check_revd(hdr.msg_num);
    if !is_new {
        log::trace!(
            target: "moonproto::crypted",
            "replay/duplicate detected: msg_num={} cmd={}",
            hdr.msg_num,
            hdr.cmd
        );
        return None;
    }

    // B-04 fix: drain the first 12 bytes instead of `plaintext[12..].to_vec()` —
    // reuse the owned Vec, one fewer allocation per Crypted packet.
    plaintext.drain(..CRYPTO_HEADER_SIZE);
    Some((hdr.cmd, plaintext, hdr.want_ack))
}

/// Decrypt an `MPC_Crypted` payload without touching the receive slider.
///
/// This is only for the Fine-wait reorder buffer: the server can send session
/// packets after it accepts `ImFriend` while the client's `MPC_Fine` is still in
/// flight. We may inspect and keep those bytes, but the replay window must move
/// only after `AuthDone`.
pub(crate) fn decrypt_command_no_replay(
    decode_cipher: &Aes128Gcm,
    encrypted_data: &[u8],
) -> Option<(CryptoHeader, Vec<u8>)> {
    let plaintext = match crypto::decrypt_with_cipher(decode_cipher, encrypted_data, &[]) {
        Some(pt) => pt,
        None => {
            // GCM tag mismatch — corrupt packet or wrong key.
            // Throttling is on the caller; here it's a plain warn target for filtering.
            warn!(target: "moonproto::crypted", "AES-GCM decrypt failed (tag mismatch)");
            return None;
        }
    };

    if plaintext.len() < CRYPTO_HEADER_SIZE {
        warn!(target: "moonproto::crypted", "decrypted plaintext too short: {} < {}", plaintext.len(), CRYPTO_HEADER_SIZE);
        return None;
    }

    let hdr = CryptoHeader::from_bytes(&plaintext)?;
    Some((hdr, plaintext))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crypto_header_parse() {
        assert_eq!(std::mem::size_of::<WireCryptoHeader>(), 12);
        assert_eq!(CRYPTO_HEADER_SIZE, 12);

        let data = [
            0x12, 0x34, // rnd
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // msg_num = 1
            0x0A, // cmd
            0x01, // want_ack = true
            0xDE, 0xAD, // payload
        ];
        let hdr = CryptoHeader::from_bytes(&data).unwrap();
        assert_eq!(hdr.rnd, 0x3412);
        assert_eq!(hdr.msg_num, 1);
        assert_eq!(hdr.cmd, 0x0A);
        assert!(hdr.want_ack);
    }
}
