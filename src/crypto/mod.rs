//! AES-GCM payload crypto and MoonProto session-key derivation helpers.
//!
//! The transport MAC/obfuscation layer lives in [`crate::transport`]; this
//! module is the inner payload encryption used after the MoonProto handshake has
//! derived per-session encode/decode keys.
//!
//! ## Threat model
//!
//! MoonProto connects a trader's terminal to their execution core running on a
//! VPS; both ends belong to the same user (their own personal VPS, not a shared
//! developer server). This is a thin client: all trading mechanics (orders,
//! stops, strategies, risk) live in the core; the client renders the core's
//! state and relays the user's intent as AEAD-authenticated commands, and
//! executes nothing off the market feed itself.
//!
//! `MasterKey`/`MacKey` are the user's pre-shared secret and never travel on the
//! wire — the handshake runs *under* `MasterKey`, it does not transmit it. Both
//! endpoints are trusted; the adversary is the network path between them (hoster,
//! ISP, transit), which can read, inject, replay and drop UDP datagrams but holds
//! no keys. A *malicious* server is out of the threat model: it already holds
//! direct authority over funds, so its correctness is the server's
//! responsibility, not the client's. (Robustness against malformed input is
//! still maintained as a quality property — bounded decompression, panic-averse
//! parsers — it is just not a security boundary against the server.)
//!
//! One input sits outside the two-endpoint model: an optional, default-on NTP
//! sync feeds a soft time offset (see [`crate::client::clock::delphi_now`]). The
//! client gates no security decision on it — anti-replay is counter-based and
//! order times use the raw clock — so a spoofed offset is an availability-only
//! concern.

mod aes_gcm_codec;
mod keys;

pub(crate) use aes_gcm::Aes128Gcm;
pub(crate) use aes_gcm_codec::{
    cipher_from_key, decrypt, decrypt_with_cipher, encrypt, encrypt_with_cipher, GCM_TAG_SIZE,
    IV_SIZE, MAX_PKCS7_PADDING,
};
pub(crate) use keys::{derive_obfuscation_key, generate_sub_keys, mix_values};
