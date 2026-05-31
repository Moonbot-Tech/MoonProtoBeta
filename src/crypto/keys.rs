use crate::MoonKey;
use sha3::{
    digest::{ExtendableOutput, Update, XofReader},
    Shake128,
};

const KEY_LEN: usize = 16;
const XOR_CONST_ENCODE: u64 = 0xE59DA7C3B8D49E25;
const XOR_CONST_DECODE: u64 = 0xA3DE5C49E365A5D7;

/// Generate a single sub-key using SHAKE-128 with 5 rounds of XOR-fold.
/// Matches Delphi GenerateSubKey exactly.
fn generate_sub_key(master_key: &MoonKey, token: u64) -> MoonKey {
    let token_bytes = token.to_le_bytes();

    // first = SHAKE-128 initialized with master_key
    // Round 1: mac = first.clone(); mac.update(token); tmp = mac.final(128 bits)
    let mut hasher = Shake128::default();
    hasher.update(master_key);
    let first_state = hasher.clone();

    let mut mac = first_state.clone();
    mac.update(&token_bytes);
    let mut tmp = [0u8; KEY_LEN];
    mac.finalize_xof().read(&mut tmp);

    let mut result = tmp;

    // Rounds 2-5: mac = first.clone(); mac.update(tmp); tmp = mac.final(128 bits); result ^= tmp
    for _ in 2..=5 {
        let mut mac = first_state.clone();
        mac.update(&tmp);
        mac.finalize_xof().read(&mut tmp);
        for i in 0..KEY_LEN {
            result[i] ^= tmp[i];
        }
    }

    result
}

/// Derive the pair of session keys from the master key + server token.
///
/// Returns `(encode_key, decode_key)` for the **client-side** (the client encrypts its
/// outgoing traffic with encode_key and decrypts incoming traffic with decode_key).
///
/// **Algorithm:** SHAKE-128(master_key) → initial state; then `server_token`
/// is XOR'd with a constant (encode/decode-specific) → 5 rounds of XOR-fold through SHAKE-128
/// for each side. The final assembly is `(keys[false], keys[true])` for the client
/// (Delphi convention: server encodes with `keys[true]`, decodes with `keys[false]`).
///
/// **When to call:** after receiving WhoAreYou from the server (the `server_token` is known).
/// These keys are stable until the next full handshake (a new Hello/WhoAreYou cycle).
/// The cached `Aes128Gcm` cipher in Client is built from these keys and rebuilt
/// when they change.
///
/// A-14 (docs_api iter-2): doc comment expanded.
pub(crate) fn generate_sub_keys(master_key: &MoonKey, server_token: u64) -> (MoonKey, MoonKey) {
    let key_true = generate_sub_key(master_key, server_token ^ XOR_CONST_ENCODE);
    let key_false = generate_sub_key(master_key, server_token ^ XOR_CONST_DECODE);
    // Client (ServerSide=false): encode with keys[false], decode with keys[true]
    (key_false, key_true)
}

/// Domain separator for the outer-obfuscation key derivation. Must match the
/// Delphi side: `GenerateSubKey(MacKey, OBFUSCATION_KEY_TOKEN)`.
const OBFUSCATION_KEY_TOKEN: u64 = 0x4F42_4655_5343_4154; // "OBFUSCAT"

/// Derive the `outer_light_crypt` whitening key from `mac_key` via the SHAKE-128
/// KDF (same primitive as the session sub-keys).
///
/// **Security (F1):** the transport MAC and the obfuscation MUST NOT share a key.
/// The whitening keystream is exposed on the wire wherever the plaintext is known
/// (e.g. the zero-padded PMTU acks), and its PRNG (xoroshiro128+) is reversible —
/// so a shared key would let an on-path attacker recover the MAC key from the
/// keystream. This one-way derivation breaks that link: recovering the
/// obfuscation key reveals nothing about `mac_key`, so the SipHash MAC stays
/// secret even if the whitening is peeled off.
pub(crate) fn derive_obfuscation_key(mac_key: &MoonKey) -> MoonKey {
    generate_sub_key(mac_key, OBFUSCATION_KEY_TOKEN)
}

/// `MixValues` — SHAKE-128 hash of `(key, token1, token2)` with a 5-round XOR-fold,
/// then assembly of the two u64 halves via addition.
///
/// **Used in:**
/// - handshake `PeerMix` computation (see SPEC.md §3.1 / §3.3) — server and client
///   verify that both sides know the session token + ClientID.
/// - Additional MITM protection (an attacker without knowledge of SubKey cannot
///   forge a matching `MixValues(rnd, mix_ts, server_token)`).
///
/// **Not a cryptographically strong MAC** — this is a lightweight session authenticator;
/// the real packet integrity protection is `calculate_mac32` (SipHash MAC).
///
/// A-14 (docs_api iter-2): doc comment expanded.
pub(crate) fn mix_values(key: &MoonKey, token1: u64, token2: u64) -> u64 {
    let token1_bytes = token1.to_le_bytes();
    let token2_bytes = token2.to_le_bytes();

    let mut hasher = Shake128::default();
    hasher.update(key);
    let first_state = hasher.clone();

    // Round 1
    let mut mac = first_state.clone();
    mac.update(&token1_bytes);
    mac.update(&token2_bytes);
    let mut tmp = [0u8; KEY_LEN];
    mac.finalize_xof().read(&mut tmp);

    let mut result = [0u8; KEY_LEN];
    result.copy_from_slice(&tmp);

    // Rounds 2-5
    for _ in 2..=5 {
        let mut mac = first_state.clone();
        mac.update(&tmp);
        mac.finalize_xof().read(&mut tmp);
        for i in 0..KEY_LEN {
            result[i] ^= tmp[i];
        }
    }

    // Combine: Lo + Hi (two u64 halves added)
    let lo = u64::from_le_bytes(result[0..8].try_into().unwrap());
    let hi = u64::from_le_bytes(result[8..16].try_into().unwrap());
    lo.wrapping_add(hi)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sub_keys_deterministic() {
        let key: MoonKey = [
            0x30, 0x1b, 0x92, 0x12, 0x09, 0xae, 0x79, 0xa5, 0x10, 0x86, 0xb1, 0x80, 0xd3, 0x25,
            0xcb, 0xd6,
        ];
        let (enc1, dec1) = generate_sub_keys(&key, 12345);
        let (enc2, dec2) = generate_sub_keys(&key, 12345);
        assert_eq!(enc1, enc2);
        assert_eq!(dec1, dec2);
        assert_ne!(enc1, dec1);
    }

    #[test]
    fn mix_values_deterministic() {
        let key: MoonKey = [1; 16];
        let v1 = mix_values(&key, 100, 200);
        let v2 = mix_values(&key, 100, 200);
        assert_eq!(v1, v2);
        assert_ne!(v1, 0);
    }
}
