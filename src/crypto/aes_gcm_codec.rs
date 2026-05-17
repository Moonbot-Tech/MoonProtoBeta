use aes_gcm::{Aes128Gcm, KeyInit, Nonce, Tag};
use aes_gcm::aead::AeadInPlace;
use crate::MoonKey;
use std::sync::atomic::{AtomicU64, Ordering};

static IV_COUNTER: AtomicU64 = AtomicU64::new(1);

/// AES-128-GCM encrypt with PKCS7 padding.
/// Output layout: IV(12) + Tag(16) + Ciphertext(padded)
pub fn encrypt(key: &MoonKey, plaintext: &[u8], aad: &[u8]) -> Vec<u8> {
    let cipher = Aes128Gcm::new(key.into());

    // Build IV: counter(8) + timestamp-like(4)
    let counter = IV_COUNTER.fetch_add(1, Ordering::Relaxed);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let mut iv_bytes = [0u8; 12];
    iv_bytes[0..8].copy_from_slice(&counter.to_le_bytes());
    iv_bytes[8..12].copy_from_slice(&ts.to_le_bytes());
    let nonce = Nonce::from_slice(&iv_bytes);

    // PKCS7 padding
    let block_size = 16usize;
    let padding = block_size - (plaintext.len() % block_size);
    let mut padded = Vec::with_capacity(plaintext.len() + padding);
    padded.extend_from_slice(plaintext);
    padded.resize(plaintext.len() + padding, padding as u8);

    // Encrypt in-place
    let tag = cipher
        .encrypt_in_place_detached(nonce, aad, &mut padded)
        .expect("AES-GCM encrypt failed");

    // Output: IV(12) + Tag(16) + Ciphertext
    let mut output = Vec::with_capacity(12 + 16 + padded.len());
    output.extend_from_slice(&iv_bytes);
    output.extend_from_slice(tag.as_slice());
    output.extend_from_slice(&padded);
    output
}

/// AES-128-GCM decrypt, verifies tag, strips PKCS7 padding.
/// Input layout: IV(12) + Tag(16) + Ciphertext
pub fn decrypt(key: &MoonKey, data: &[u8], aad: &[u8]) -> Option<Vec<u8>> {
    if data.len() < 28 {
        return None; // minimum: 12 IV + 16 tag + 0 ciphertext (but that's empty)
    }

    let iv_bytes = &data[0..12];
    let tag_bytes = &data[12..28];
    let ciphertext = &data[28..];

    if ciphertext.is_empty() {
        return None;
    }

    let cipher = Aes128Gcm::new(key.into());
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
    // Verify all padding bytes
    for &b in &buf[buf.len() - padding..] {
        if b as usize != padding {
            return None;
        }
    }
    buf.truncate(buf.len() - padding);
    Some(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let key: MoonKey = [0x30, 0x1b, 0x92, 0x12, 0x09, 0xae, 0x79, 0xa5,
                            0x10, 0x86, 0xb1, 0x80, 0xd3, 0x25, 0xcb, 0xd6];
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
}
