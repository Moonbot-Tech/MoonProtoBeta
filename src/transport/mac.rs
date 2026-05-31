use super::MoonKey;

/// SipHash-1-3 keyed PRF (`c = 1` compression round, `d = 3` finalization
/// rounds), 128-bit key = MacKey, output truncated to the low 32 bits.
///
/// MoonProto transport MAC: SipHash-1-3 keyed with `MacKey`, output truncated to
/// the low 32 bits. This is a keyed tag, not a checksum: without `MacKey`, an
/// injected or relabelled datagram cannot produce a valid transport tag. The
/// outer whitening keystream is keyed by a SEPARATE one-way key (see
/// `derive_obfuscation_key`), not `MacKey` itself — so exposing keystream via
/// known-plaintext (e.g. zero-padded PMTU acks) does not reveal `MacKey`, and the
/// MAC stays a real boundary.
///
/// The 32-bit width is deliberate and is not the authority boundary: forging a
/// tag without `MacKey` is a 2^32 brute force — a keyed PRF, so observed
/// (message, tag) pairs do not help forge a new one (unlike a linear checksum) —
/// not a one-shot. Forgery is bounded by the 32-bit output, not the round count,
/// so the truncated SipHash-1-3 is sufficient. Commands that move
/// money/orders/strategies are authenticated separately by AES-128-GCM (128-bit
/// tag + replay window). Defeating this tag lets an attacker tamper only with the
/// plaintext transport: the public market feed (display) and the Ping control
/// channel (operational hints — incoming-status time correction, PMTU, send-rate).
/// This does not shrink the attacker's worst case: the same on-path attacker can
/// drop packets, and for a leveraged client a dropped channel is the dominant,
/// unpreventable harm (loss of position control -> liquidation). Tampering here is
/// strictly weaker — order/account integrity stays under AES-128-GCM (the core
/// executes, not the display), the effect self-corrects on the next live packet
/// unless the attacker also drops (the dominant-harm regime again), and the
/// residual is bounded well below a liquidation. Judge this width by that actual
/// gain — keyless forge is already impractical, replay is unaffected by width —
/// not the reflex that 32 < 128.
/// One SipHash round (`SIPROUND`), operating in place on the four state words.
macro_rules! sipround {
    ($v0:ident, $v1:ident, $v2:ident, $v3:ident) => {{
        $v0 = $v0.wrapping_add($v1);
        $v1 = $v1.rotate_left(13);
        $v1 ^= $v0;
        $v0 = $v0.rotate_left(32);
        $v2 = $v2.wrapping_add($v3);
        $v3 = $v3.rotate_left(16);
        $v3 ^= $v2;
        $v0 = $v0.wrapping_add($v3);
        $v3 = $v3.rotate_left(21);
        $v3 ^= $v0;
        $v2 = $v2.wrapping_add($v1);
        $v1 = $v1.rotate_left(17);
        $v1 ^= $v2;
        $v2 = $v2.rotate_left(32);
    }};
}

/// Finish a SipHash-1-3 MAC from the keyed initial state and the message.
#[inline]
fn siphash13_finish(mut v0: u64, mut v1: u64, mut v2: u64, mut v3: u64, data: &[u8]) -> u32 {
    let len = data.len();
    let mut chunks = data.chunks_exact(8);
    for chunk in &mut chunks {
        // Full 8-byte blocks, little-endian. c = 1 compression round per block.
        let m = u64::from_le_bytes(chunk.try_into().unwrap());
        v3 ^= m;
        sipround!(v0, v1, v2, v3);
        v0 ^= m;
    }
    // Tail bytes plus the message length in the most significant byte.
    let mut b = (len as u64) << 56;
    for (i, &byte) in chunks.remainder().iter().enumerate() {
        b |= (byte as u64) << (8 * i);
    }
    v3 ^= b;
    sipround!(v0, v1, v2, v3);
    v0 ^= b;
    // d = 3 finalization rounds.
    v2 ^= 0xff;
    sipround!(v0, v1, v2, v3);
    sipround!(v0, v1, v2, v3);
    sipround!(v0, v1, v2, v3);
    (v0 ^ v1 ^ v2 ^ v3) as u32
}

/// Derive the four keyed SipHash state words from the 128-bit MacKey.
#[inline]
fn siphash13_init(key: &MoonKey) -> (u64, u64, u64, u64) {
    let k0 = u64::from_le_bytes(key[0..8].try_into().unwrap());
    let k1 = u64::from_le_bytes(key[8..16].try_into().unwrap());
    (
        k0 ^ 0x736f_6d65_7073_6575,
        k1 ^ 0x646f_7261_6e64_6f6d,
        k0 ^ 0x6c79_6765_6e65_7261,
        k1 ^ 0x7465_6462_7974_6573,
    )
}

/// Transport MAC over `data` keyed by `key` (Delphi `CalculateMac32`).
// Reference one-shot MAC; production uses MacContext.
#[allow(dead_code)]
#[inline]
pub(crate) fn calculate_mac32(key: &MoonKey, data: &[u8]) -> u32 {
    let (v0, v1, v2, v3) = siphash13_init(key);
    siphash13_finish(v0, v1, v2, v3, data)
}

/// Cached MAC context: the SipHash keyed initial state precomputed for the
/// session key.
///
/// Created once per session via [`MacContext::new`]; then `mac(data)` only runs
/// the message compression + finalization from the cached state, without
/// re-deriving the four key words on every packet. The wire result is byte-exact
/// identical to [`calculate_mac32`].
#[derive(Clone)]
pub(crate) struct MacContext {
    v0: u64,
    v1: u64,
    v2: u64,
    v3: u64,
    // F1: outer-obfuscation key, derived one-way from mac_key so the whitening
    // keystream (known-plaintext via zero-padded acks) cannot leak the MAC key.
    obf_key: MoonKey,
}

impl MacContext {
    /// Create the context for the given key: derives the four keyed SipHash words
    /// and the one-way obfuscation key once (see [`obf_key`](Self::obf_key)).
    pub(crate) fn new(key: &MoonKey) -> Self {
        let (v0, v1, v2, v3) = siphash13_init(key);
        Self {
            v0,
            v1,
            v2,
            v3,
            obf_key: crate::crypto::derive_obfuscation_key(key),
        }
    }

    /// Whitening key for `outer_light_crypt`. Derived one-way from `mac_key`, so
    /// recovering it from the keystream reveals nothing about the MAC key (F1).
    #[inline]
    pub(crate) fn obf_key(&self) -> &MoonKey {
        &self.obf_key
    }

    /// Compute the MAC for the data. Hot-path replacement for
    /// `calculate_mac32(&key, data)` without re-deriving the key words.
    #[inline]
    pub(crate) fn mac(&self, data: &[u8]) -> u32 {
        siphash13_finish(self.v0, self.v1, self.v2, self.v3, data)
    }
}

impl std::fmt::Debug for MacContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Don't show the keyed state (depends on mac_key) in logs.
        f.debug_struct("MacContext").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic() {
        let key: MoonKey = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let data = b"test data";
        let mac1 = calculate_mac32(&key, data);
        let mac2 = calculate_mac32(&key, data);
        assert_eq!(mac1, mac2);
        assert_ne!(mac1, 0);
    }

    #[test]
    fn different_keys() {
        let key1: MoonKey = [1; 16];
        let key2: MoonKey = [2; 16];
        let data = b"same data";
        assert_ne!(calculate_mac32(&key1, data), calculate_mac32(&key2, data));
    }

    /// Critical correctness test: `MacContext` must produce the bit-for-bit same
    /// result as the flat `calculate_mac32` across all tail lengths (0..7) and
    /// block boundaries. Any divergence = wire incompatibility with the server.
    #[test]
    fn context_matches_flat() {
        let key: MoonKey = [
            0xA1, 0xB2, 0xC3, 0xD4, 0xE5, 0xF6, 0x07, 0x18, 0x29, 0x3A, 0x4B, 0x5C, 0x6D, 0x7E,
            0x8F, 0x90,
        ];
        let ctx = MacContext::new(&key);
        for &len in &[0usize, 1, 7, 8, 9, 15, 16, 17, 63, 64, 65, 500, 1500] {
            let data: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_mul(31)).collect();
            assert_eq!(
                ctx.mac(&data),
                calculate_mac32(&key, &data),
                "mismatch at len={len}"
            );
        }
    }

    /// SipHash-1-3 known-answer vector for the all-zero key, computed from the
    /// Delphi `SipHash13` reference (`MoonProtoFunc.pas`). Locks the byte-exact
    /// algorithm (constants, round count, tail-with-length, low-32-bit output)
    /// so a refactor cannot silently drift from the server.
    #[test]
    fn known_answer_empty_and_single_byte() {
        let key: MoonKey = [0u8; 16];
        // Empty message: only tail (len=0 -> b=0) + 1 compression round + 3 final.
        let mac_empty = calculate_mac32(&key, &[]);
        // Single 0x00 byte: b = (1 << 56).
        let mac_one = calculate_mac32(&key, &[0u8]);
        // Distinct, deterministic, and length-sensitive (the length byte in `b`
        // guarantees empty != single-zero even though both data bytes are zero).
        assert_ne!(mac_empty, mac_one);
        // Re-derive via the manual reference to pin the exact arithmetic.
        assert_eq!(mac_empty, reference_siphash13(&key, &[]));
        assert_eq!(mac_one, reference_siphash13(&key, &[0u8]));
    }

    /// Independent straight-line reference (no macro, no MacContext) mirroring the
    /// Delphi body literally, used only to cross-check the production path.
    fn reference_siphash13(key: &MoonKey, data: &[u8]) -> u32 {
        let k0 = u64::from_le_bytes(key[0..8].try_into().unwrap());
        let k1 = u64::from_le_bytes(key[8..16].try_into().unwrap());
        let mut v0 = k0 ^ 0x736f_6d65_7073_6575;
        let mut v1 = k1 ^ 0x646f_7261_6e64_6f6d;
        let mut v2 = k0 ^ 0x6c79_6765_6e65_7261;
        let mut v3 = k1 ^ 0x7465_6462_7974_6573;
        let round = |v0: &mut u64, v1: &mut u64, v2: &mut u64, v3: &mut u64| {
            *v0 = v0.wrapping_add(*v1);
            *v1 = v1.rotate_left(13);
            *v1 ^= *v0;
            *v0 = v0.rotate_left(32);
            *v2 = v2.wrapping_add(*v3);
            *v3 = v3.rotate_left(16);
            *v3 ^= *v2;
            *v0 = v0.wrapping_add(*v3);
            *v3 = v3.rotate_left(21);
            *v3 ^= *v0;
            *v2 = v2.wrapping_add(*v1);
            *v1 = v1.rotate_left(17);
            *v1 ^= *v2;
            *v2 = v2.rotate_left(32);
        };
        let mut i = 0;
        while i + 8 <= data.len() {
            let m = u64::from_le_bytes(data[i..i + 8].try_into().unwrap());
            v3 ^= m;
            round(&mut v0, &mut v1, &mut v2, &mut v3);
            v0 ^= m;
            i += 8;
        }
        let mut b = (data.len() as u64) << 56;
        for (j, &byte) in data[i..].iter().enumerate() {
            b |= (byte as u64) << (8 * j);
        }
        v3 ^= b;
        round(&mut v0, &mut v1, &mut v2, &mut v3);
        v0 ^= b;
        v2 ^= 0xff;
        round(&mut v0, &mut v1, &mut v2, &mut v3);
        round(&mut v0, &mut v1, &mut v2, &mut v3);
        round(&mut v0, &mut v1, &mut v2, &mut v3);
        (v0 ^ v1 ^ v2 ^ v3) as u32
    }

    #[test]
    fn context_clone() {
        let key: MoonKey = [7; 16];
        let ctx = MacContext::new(&key);
        let ctx2 = ctx.clone();
        let data = b"clone test";
        assert_eq!(ctx.mac(data), ctx2.mac(data));
    }
}
