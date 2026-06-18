use crate::MoonKey;
use aes_gcm::aead::AeadInPlace;
use aes_gcm::{Aes128Gcm, KeyInit, Nonce, Tag};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use zerocopy::byteorder::little_endian::{U32 as LeU32, U64 as LeU64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

static IV_COUNTER: AtomicU64 = AtomicU64::new(1);
pub(crate) const IV_SIZE: usize = 12;
pub(crate) const GCM_TAG_SIZE: usize = 16;
pub(crate) const PKCS7_BLOCK_SIZE: usize = 16;
pub(crate) const MAX_PKCS7_PADDING: usize = PKCS7_BLOCK_SIZE;

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C, packed)]
struct WireMoonProtoIv {
    r1: LeU64,
    r2: LeU32,
}

const _: () = assert!(core::mem::size_of::<WireMoonProtoIv>() == IV_SIZE);

/// `GlobalAESIVMask` — a random u64 initialized on the first encrypt (≡ Delphi
/// initialization section MoonProtoFunc.pas:834 `GlobalAESIVMask := random64`).
/// Used for XOR with the IV counter — obfuscates packet ordering on the wire.
static IV_MASK: OnceLock<u64> = OnceLock::new();

#[inline(always)]
fn iv_mask() -> u64 {
    *IV_MASK.get_or_init(rand::random::<u64>)
}

/// Pseudo-RDTSC: 64-bit timestamp counter with ~ns resolution.
/// On x86_64 uses the real RDTSC (≡ Delphi `GetCPUTimeStamp` MoonProtoFunc.pas:152-156).
/// On other architectures falls back to `SystemTime::nanos_since(UNIX_EPOCH)`.
#[inline(always)]
fn cpu_timestamp() -> u64 {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_rdtsc()
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        let d = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        (d.as_secs() as u64) * 1_000_000_000 + d.subsec_nanos() as u64
    }
}

/// Construct a reusable `Aes128Gcm` cipher from a 16-byte key.
///
/// The key is fixed for the whole session and changes only on handshake, so the
/// key schedule is expanded once and reused for every encrypted packet instead
/// of being rebuilt in the send loop. `Aes128Gcm` is `Send + Sync`, so it can be
/// held in `Client`.
#[inline]
pub(crate) fn cipher_from_key(key: &MoonKey) -> Aes128Gcm {
    Aes128Gcm::new(key.into())
}

#[inline(always)]
fn pkcs7_padding_len(len: usize) -> usize {
    let padding = PKCS7_BLOCK_SIZE - (len & (PKCS7_BLOCK_SIZE - 1));
    if padding == 0 {
        PKCS7_BLOCK_SIZE
    } else {
        padding
    }
}

#[inline]
fn check_pkcs7_padding(buf: &[u8]) -> Option<usize> {
    let padding = *buf.last()? as usize;
    if padding == 0 || padding > PKCS7_BLOCK_SIZE || padding > buf.len() {
        return None;
    }
    let start = buf.len() - padding;
    if buf[start..].iter().all(|&b| b as usize == padding) {
        Some(padding)
    } else {
        None
    }
}

/// AES-128-GCM encrypt with MoonProto PKCS7 padding — hot path version with a reusable cipher.
///
/// Callers pass the session cipher in, so packet encrypt does not rebuild the
/// AES key schedule on the hot path.
///
/// The padding is part of MoonProto wire parity with the reference server. The
/// server uses the mORMot `TAesGcm` AVX fast path, which requires block-aligned
/// input; PKCS7 keeps encrypted packets on that fast path instead of falling
/// back to the slower arbitrary-length engine. RustCrypto handles tails, but it
/// is not the same 8x interleaved AES/GHASH backend. A future backend benchmark
/// can compare `aws-lc-rs`/`ring` while keeping this wire shape unchanged.
pub(crate) fn encrypt_with_cipher(cipher: &Aes128Gcm, plaintext: &[u8], aad: &[u8]) -> Vec<u8> {
    // Build IV: (counter XOR mask)(8) + RDTSC[low 32 bits](4)
    let counter = IV_COUNTER.fetch_add(1, Ordering::Relaxed);
    let r1 = counter ^ iv_mask();
    let r2 = (cpu_timestamp() & 0xFFFF_FFFF) as u32;
    let wire_iv = WireMoonProtoIv {
        r1: LeU64::new(r1),
        r2: LeU32::new(r2),
    };
    let mut iv_bytes = [0u8; IV_SIZE];
    iv_bytes.copy_from_slice(wire_iv.as_bytes());
    let padding = pkcs7_padding_len(plaintext.len());
    let padded_len = plaintext.len() + padding;

    let cipher_offset = IV_SIZE + GCM_TAG_SIZE;
    let total_len = cipher_offset + padded_len;
    let mut output = Vec::with_capacity(total_len);
    output.extend_from_slice(&iv_bytes);
    output.resize(cipher_offset, 0);
    output.extend_from_slice(plaintext);
    output.resize(total_len, padding as u8);
    let nonce = Nonce::from_slice(&iv_bytes);

    // Encrypt in-place — `expect` invariant: AES-GCM fails only at a ≥ 16 EiB
    // payload, far beyond MoonProto direct/sliced payload limits.
    let tag = cipher
        .encrypt_in_place_detached(nonce, aad, &mut output[cipher_offset..])
        .expect("AES-GCM payload < 16 EiB — invariant satisfied by protocol limits");

    // Output: IV(12) + Tag(16) + Ciphertext
    output[IV_SIZE..cipher_offset].copy_from_slice(tag.as_slice());
    output
}

/// AES-128-GCM decrypt with a reusable cipher — hot path version.
/// See `encrypt_with_cipher` for the cached-cipher context.
pub(crate) fn decrypt_with_cipher(cipher: &Aes128Gcm, data: &[u8], aad: &[u8]) -> Option<Vec<u8>> {
    if data.len() < IV_SIZE + GCM_TAG_SIZE {
        return None;
    }

    let iv_bytes = &data[0..IV_SIZE];
    let tag_bytes = &data[IV_SIZE..IV_SIZE + GCM_TAG_SIZE];
    let ciphertext = &data[IV_SIZE + GCM_TAG_SIZE..];

    if ciphertext.is_empty() {
        return None;
    }

    let nonce = Nonce::from_slice(iv_bytes);
    let tag = Tag::from_slice(tag_bytes);

    let mut buf = ciphertext.to_vec();
    cipher
        .decrypt_in_place_detached(nonce, aad, &mut buf, tag)
        .ok()?;

    let padding = check_pkcs7_padding(&buf)?;
    let plain_len = buf.len() - padding;
    buf.truncate(plain_len);
    Some(buf)
}

/// AES-128-GCM encrypt with MoonProto PKCS7 padding — convenience wrapper for the rare
/// cases (handshake) where the cipher is not cached. Each call creates the cipher
/// anew — acceptable only when called a handful of times per session.
///
/// Output layout: IV(12) + Tag(16) + Ciphertext(PKCS7-padded plaintext length)
///
/// IV construction (byte-exact with Delphi MoonProtoFunc.pas:584-587):
/// - `R1 = atomic_inc(counter) XOR iv_mask` (8 bytes LE)
/// - `R2 = GetCPUTimeStamp (RDTSC)` (4 low-order bytes)
pub(crate) fn encrypt(key: &MoonKey, plaintext: &[u8], aad: &[u8]) -> Vec<u8> {
    encrypt_with_cipher(&cipher_from_key(key), plaintext, aad)
}

/// AES-128-GCM decrypt, verifies tag, removes MoonProto PKCS7 padding, and
/// returns the original plaintext. On the hot path use `decrypt_with_cipher`.
pub(crate) fn decrypt(key: &MoonKey, data: &[u8], aad: &[u8]) -> Option<Vec<u8>> {
    decrypt_with_cipher(&cipher_from_key(key), data, aad)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let key: MoonKey = [
            0x30, 0x1b, 0x92, 0x12, 0x09, 0xae, 0x79, 0xa5, 0x10, 0x86, 0xb1, 0x80, 0xd3, 0x25,
            0xcb, 0xd6,
        ];
        let plaintext = b"Hello MoonProto!";
        let aad = 12345u64.to_le_bytes();

        let encrypted = encrypt(&key, plaintext, &aad);
        let decrypted = decrypt(&key, &encrypted, &aad).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn ciphertext_length_matches_pkcs7_padded_plaintext() {
        let key: MoonKey = [7; 16];
        for len in [0usize, 1, 15, 16, 17, 31, 32, 33, 91, 1601] {
            let plaintext = vec![0xA5; len];
            let encrypted = encrypt(&key, &plaintext, &[]);
            assert_eq!(
                encrypted.len(),
                IV_SIZE + GCM_TAG_SIZE + len + pkcs7_padding_len(len)
            );
            let decrypted = decrypt(&key, &encrypted, &[]).unwrap();
            assert_eq!(decrypted, plaintext);
        }
    }

    #[test]
    fn invalid_pkcs7_padding_fails_after_valid_gcm_tag() {
        let key: MoonKey = [7; 16];
        let cipher = cipher_from_key(&key);
        let mut encrypted = encrypt(&key, b"secret", &[]);
        let mut iv_bytes = [0u8; IV_SIZE];
        iv_bytes.copy_from_slice(&encrypted[0..IV_SIZE]);
        let nonce = Nonce::from_slice(&iv_bytes);
        let cipher_offset = IV_SIZE + GCM_TAG_SIZE;
        let payload = &mut encrypted[cipher_offset..];
        payload.fill(0);
        let tag = cipher
            .encrypt_in_place_detached(nonce, &[], payload)
            .unwrap();
        encrypted[IV_SIZE..cipher_offset].copy_from_slice(tag.as_slice());

        assert!(decrypt(&key, &encrypted, &[]).is_none());
    }

    #[test]
    fn wrong_key_fails() {
        let key: MoonKey = [1; 16];
        let wrong_key: MoonKey = [2; 16];
        let encrypted = encrypt(&key, b"secret", &[]);
        assert!(decrypt(&wrong_key, &encrypted, &[]).is_none());
    }

    #[test]
    fn wrong_aad_fails() {
        let key: MoonKey = [1; 16];
        let encrypted = encrypt(&key, b"secret", &[1, 2, 3]);
        assert!(decrypt(&key, &encrypted, &[4, 5, 6]).is_none());
    }

    #[test]
    fn moonproto_iv_wire_layout_is_fixed() {
        let wire = WireMoonProtoIv {
            r1: LeU64::new(0x8877_6655_4433_2211),
            r2: LeU32::new(0xccbb_aa99),
        };

        let bytes = wire.as_bytes();
        assert_eq!(
            bytes,
            &[0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc]
        );

        let parsed = WireMoonProtoIv::read_from_bytes(bytes).unwrap();
        assert_eq!(parsed.r1.get(), 0x8877_6655_4433_2211);
        assert_eq!(parsed.r2.get(), 0xccbb_aa99);
    }
}
