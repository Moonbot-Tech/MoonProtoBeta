//! AES-GCM payload crypto and MoonProto session-key derivation helpers.
//!
//! The transport MAC/obfuscation layer lives in [`crate::transport`]; this
//! module is the inner payload encryption used after the MoonProto handshake has
//! derived per-session encode/decode keys.

mod aes_gcm_codec;
mod keys;

pub(crate) use aes_gcm::Aes128Gcm;
pub(crate) use aes_gcm_codec::{
    cipher_from_key, decrypt, decrypt_with_cipher, encrypt, encrypt_with_cipher,
};
pub(crate) use keys::{generate_sub_keys, mix_values};
