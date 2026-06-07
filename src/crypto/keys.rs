use crate::MoonKey;
use sha3::{
    digest::{ExtendableOutput, Update, XofReader},
    Shake128,
};
use zeroize::Zeroize;

const KEY_LEN: usize = 16;
const XOR_CONST_ENCODE: u64 = 0xE59DA7C3B8D49E25;
const XOR_CONST_DECODE: u64 = 0xA3DE5C49E365A5D7;
pub(crate) const ACK_SESSION_SALT: u64 = 0x3153_5345_534B_4341; // Delphi AckSessionSalt

/// Generate a single sub-key using SHAKE-128 with 5 rounds of XOR-fold.
/// Matches Delphi GenerateSubKey exactly.
fn generate_sub_key(master_key: &MoonKey, token: u64) -> MoonKey {
    let mut token_bytes = token.to_le_bytes();

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

    tmp.zeroize();
    token_bytes.zeroize();
    result
}

/// Generate a session sub-key using the current Delphi L3 KDF:
/// SHAKE-128(master_key) followed by ClientID, ServerToken, SessionRnd, and a
/// direction/domain token, then the same 5-round XOR-fold as `GenerateSubKey`.
pub(crate) fn generate_session_sub_key(
    master_key: &MoonKey,
    client_id: u64,
    server_token: u64,
    session_rnd: &[u8; 16],
    direction_token: u64,
) -> MoonKey {
    let mut client_id_bytes = client_id.to_le_bytes();
    let mut server_token_bytes = server_token.to_le_bytes();
    let mut direction_token_bytes = direction_token.to_le_bytes();

    let mut hasher = Shake128::default();
    hasher.update(master_key);
    let first_state = hasher.clone();

    let mut mac = first_state.clone();
    mac.update(&client_id_bytes);
    mac.update(&server_token_bytes);
    mac.update(session_rnd);
    mac.update(&direction_token_bytes);
    let mut tmp = [0u8; KEY_LEN];
    mac.finalize_xof().read(&mut tmp);

    let mut result = tmp;
    for _ in 2..=5 {
        let mut mac = first_state.clone();
        mac.update(&tmp);
        mac.finalize_xof().read(&mut tmp);
        for i in 0..KEY_LEN {
            result[i] ^= tmp[i];
        }
    }

    tmp.zeroize();
    client_id_bytes.zeroize();
    server_token_bytes.zeroize();
    direction_token_bytes.zeroize();
    result
}

/// Derive client-side session encode/decode keys from master key, ClientID,
/// ServerToken, and SessionRnd. Client encode uses Delphi `keys[false]`; client
/// decode uses `keys[true]`.
pub(crate) fn generate_session_sub_keys(
    master_key: &MoonKey,
    client_id: u64,
    server_token: u64,
    session_rnd: &[u8; 16],
) -> (MoonKey, MoonKey) {
    let key_true = generate_session_sub_key(
        master_key,
        client_id,
        server_token,
        session_rnd,
        XOR_CONST_ENCODE,
    );
    let key_false = generate_session_sub_key(
        master_key,
        client_id,
        server_token,
        session_rnd,
        XOR_CONST_DECODE,
    );
    (key_false, key_true)
}

/// Delphi `TMoonProtoClient.RefreshAckSession32`: derive a compact session tag
/// from `MacKey`, `ClientID`, `ServerToken`, and `SessionRnd`, then fold the two
/// u64 halves into a u32. The value is cached on hard-session creation and only
/// compared on Ping/SlicedACK hot paths.
pub(crate) fn ack_session32(
    mac_key: &MoonKey,
    client_id: u64,
    server_token: u64,
    session_rnd: &[u8; 16],
) -> u32 {
    if server_token == 0 {
        return 0;
    }
    let mut sk = generate_session_sub_key(
        mac_key,
        client_id,
        server_token,
        session_rnd,
        ACK_SESSION_SALT,
    );
    let lo = u64::from_le_bytes(sk[0..8].try_into().unwrap());
    let hi = u64::from_le_bytes(sk[8..16].try_into().unwrap());
    sk.zeroize();
    lo.wrapping_add(hi) as u32
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

/// Delphi `CalculateHelloAgainPeerMix`: one SHAKE-128 pass over
/// `Rnd || MixTS || ServerToken || SessionRnd`, folded as `lo + hi`.
pub(crate) fn calculate_hello_again_peer_mix(
    rnd: &[u8; 16],
    mix_ts: u64,
    server_token: u64,
    session_rnd: &[u8; 16],
) -> u64 {
    let mut mix_ts_bytes = mix_ts.to_le_bytes();
    let mut server_token_bytes = server_token.to_le_bytes();
    let mut hasher = Shake128::default();
    hasher.update(rnd);
    hasher.update(&mix_ts_bytes);
    hasher.update(&server_token_bytes);
    hasher.update(session_rnd);
    let mut result = [0u8; KEY_LEN];
    hasher.finalize_xof().read(&mut result);

    let lo = u64::from_le_bytes(result[0..8].try_into().unwrap());
    let hi = u64::from_le_bytes(result[8..16].try_into().unwrap());
    let out = lo.wrapping_add(hi);
    result.zeroize();
    mix_ts_bytes.zeroize();
    server_token_bytes.zeroize();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_sub_keys_depend_on_client_randomness() {
        let key: MoonKey = [0x42; 16];
        let rnd_a = [0x11; 16];
        let rnd_b = [0x22; 16];
        let (enc_a, dec_a) = generate_session_sub_keys(&key, 0x1234, 0x5678, &rnd_a);
        let (enc_b, dec_b) = generate_session_sub_keys(&key, 0x1234, 0x5678, &rnd_b);

        assert_ne!(enc_a, enc_b);
        assert_ne!(dec_a, dec_b);
        assert_ne!(enc_a, dec_a);
        assert_eq!(
            generate_session_sub_keys(&key, 0x1234, 0x5678, &rnd_a),
            (enc_a, dec_a)
        );
    }

    #[test]
    fn hello_again_peer_mix_depends_on_session_rnd() {
        let rnd = [0x31; 16];
        let session_a = [0x41; 16];
        let session_b = [0x42; 16];
        let a = calculate_hello_again_peer_mix(&rnd, 100, 200, &session_a);
        let b = calculate_hello_again_peer_mix(&rnd, 100, 200, &session_b);

        assert_ne!(a, b);
        assert_eq!(
            a,
            calculate_hello_again_peer_mix(&rnd, 100, 200, &session_a)
        );
    }
}
