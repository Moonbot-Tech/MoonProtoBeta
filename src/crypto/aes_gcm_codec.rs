use crate::MoonKey;
use aes_gcm::aead::AeadInPlace;
use aes_gcm::{Aes128Gcm, KeyInit, Nonce, Tag};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use zerocopy::byteorder::little_endian::{U32 as LeU32, U64 as LeU64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

static IV_COUNTER: AtomicU64 = AtomicU64::new(1);
const IV_SIZE: usize = 12;

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C, packed)]
struct WireMoonProtoIv {
    r1: LeU64,
    r2: LeU32,
}

const _: () = assert!(core::mem::size_of::<WireMoonProtoIv>() == IV_SIZE);

/// `GlobalAESIVMask` — random u64 заводится при первом encrypt'е (≡ Delphi
/// initialization секция MoonProtoFunc.pas:834 `GlobalAESIVMask := random64`).
/// Используется для XOR с IV counter — обфускация порядка пакетов на проводе.
static IV_MASK: OnceLock<u64> = OnceLock::new();

#[inline(always)]
fn iv_mask() -> u64 {
    *IV_MASK.get_or_init(rand::random::<u64>)
}

/// Pseudo-RDTSC: 64-bit timestamp counter с ~ns-резолюцией.
/// На x86_64 использует реальный RDTSC (≡ Delphi `GetCPUTimeStamp` MoonProtoFunc.pas:152-156).
/// На других архитектурах fallback на `SystemTime::nanos_since(UNIX_EPOCH)`.
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

/// Сконструировать переиспользуемый `Aes128Gcm` cipher из 16-байтного ключа.
/// B-V2-03 fix: ключ фиксирован на всю сессию (меняется только при handshake),
/// key schedule расширяется один раз, дальше cipher используется на каждый пакет.
/// `Aes128Gcm` — Send+Sync, можно держать в `Client`.
#[inline]
pub fn cipher_from_key(key: &MoonKey) -> Aes128Gcm {
    Aes128Gcm::new(key.into())
}

/// AES-128-GCM encrypt with PKCS7 padding — hot path версия с переиспользуемым cipher.
/// B-V2-03: на hot path callers держат cipher в Client и передают сюда — экономим
/// `Aes128Gcm::new` (key schedule expansion) на каждый encrypt'е (50K pps на пике).
pub fn encrypt_with_cipher(cipher: &Aes128Gcm, plaintext: &[u8], aad: &[u8]) -> Vec<u8> {
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
    let nonce = Nonce::from_slice(&iv_bytes);

    // PKCS7 padding
    let block_size = 16usize;
    let padding = block_size - (plaintext.len() % block_size);
    let mut padded = Vec::with_capacity(plaintext.len() + padding);
    padded.extend_from_slice(plaintext);
    padded.resize(plaintext.len() + padding, padding as u8);

    // Encrypt in-place — `expect` invariant: AES-GCM fails только при ≥ 16 EiB payload,
    // что невозможно в MoonProto (PMTU < 8KB → одно сообщение).
    let tag = cipher
        .encrypt_in_place_detached(nonce, aad, &mut padded)
        .expect("AES-GCM payload < 16 EiB — invariant satisfied by MTU");

    // Output: IV(12) + Tag(16) + Ciphertext
    let mut output = Vec::with_capacity(IV_SIZE + 16 + padded.len());
    output.extend_from_slice(&iv_bytes);
    output.extend_from_slice(tag.as_slice());
    output.extend_from_slice(&padded);
    output
}

/// AES-128-GCM decrypt с переиспользуемым cipher — hot path версия.
/// См. `encrypt_with_cipher` для контекста B-V2-03.
pub fn decrypt_with_cipher(cipher: &Aes128Gcm, data: &[u8], aad: &[u8]) -> Option<Vec<u8>> {
    if data.len() < IV_SIZE + 16 {
        return None;
    }

    let iv_bytes = &data[0..IV_SIZE];
    let tag_bytes = &data[IV_SIZE..IV_SIZE + 16];
    let ciphertext = &data[IV_SIZE + 16..];

    if ciphertext.is_empty() {
        return None;
    }

    let nonce = Nonce::from_slice(iv_bytes);
    let tag = Tag::from_slice(tag_bytes);

    let mut buf = ciphertext.to_vec();
    cipher
        .decrypt_in_place_detached(nonce, aad, &mut buf, tag)
        .ok()?;

    // Strip PKCS7 padding
    let padding = *buf.last()? as usize;
    if padding == 0 || padding > 16 || padding > buf.len() {
        return None;
    }
    for &b in &buf[buf.len() - padding..] {
        if b as usize != padding {
            return None;
        }
    }
    buf.truncate(buf.len() - padding);
    Some(buf)
}

/// AES-128-GCM encrypt with PKCS7 padding — convenience-обёртка для редких
/// случаев (handshake) где cipher не закэширован. Каждый вызов создаёт cipher
/// заново — допустимо только когда вызывается несколько раз за сессию.
///
/// Output layout: IV(12) + Tag(16) + Ciphertext(padded)
///
/// IV construction (byte-exact с Delphi MoonProtoFunc.pas:584-587):
/// - `R1 = atomic_inc(counter) XOR iv_mask` (8 bytes LE)
/// - `R2 = GetCPUTimeStamp (RDTSC)` (4 младших байта)
pub fn encrypt(key: &MoonKey, plaintext: &[u8], aad: &[u8]) -> Vec<u8> {
    encrypt_with_cipher(&cipher_from_key(key), plaintext, aad)
}

/// AES-128-GCM decrypt, verifies tag, strips PKCS7 padding — convenience-обёртка
/// для handshake. На hot path используй `decrypt_with_cipher`.
pub fn decrypt(key: &MoonKey, data: &[u8], aad: &[u8]) -> Option<Vec<u8>> {
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
