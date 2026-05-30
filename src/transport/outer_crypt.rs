use super::MoonKey;

/// Advance the xoroshiro128+ state and return the whitening keystream word.
///
/// The keystream is `(s0 + s1)` passed through two xorshift mixes, exactly as
/// Delphi `OuterLightCrypt` (`MoonProtoFunc.pas:347-378`). The keystream is read
/// from the current state *before* the state is advanced.
#[inline]
fn next_keystream(s0: &mut u64, s1: &mut u64) -> u64 {
    let mut ks = s0.wrapping_add(*s1);
    ks ^= ks >> 11;
    ks ^= ks >> 23;
    let t = *s1 ^ *s0;
    *s0 = s0.rotate_left(55) ^ t ^ (t << 14);
    *s1 = t.rotate_left(36);
    ks
}

/// OuterLightCrypt — xoroshiro128+-based whitening for packet obfuscation (DPI).
///
/// `buf[0]` is the seed (left unchanged, not encrypted); the remaining bytes are
/// XOR'd with the keystream. The keystream is produced one `u64` (QWORD) per
/// state update over the full 8-byte chunks, and byte-wise over the `<8` tail —
/// byte-exact with Delphi `OuterLightCrypt`. The operation is its own inverse
/// (XOR with the same seed/key), so encrypt and decrypt are the same call.
///
/// `#[inline]` is mandatory: the function is called per-packet (tens of thousands
/// of times/sec at peak) across a cross-crate boundary from `moonproto`. Audit
/// B-V2-04. Do NOT remove without switching to LTO-fat.
#[inline]
pub fn outer_light_crypt(buf: &mut [u8], key: &MoonKey) {
    if buf.len() <= 1 {
        return;
    }

    let seed = buf[0] as u64;
    // State from key XOR seed (seed = buf[0], not encrypted).
    let mut s0 = u64::from_le_bytes(key[0..8].try_into().unwrap()) ^ seed;
    let mut s1 = u64::from_le_bytes(key[8..16].try_into().unwrap())
        ^ seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);

    let data = &mut buf[1..];
    let mut chunks = data.chunks_exact_mut(8);
    for chunk in &mut chunks {
        // One u64 keystream word per 8-byte block, little-endian.
        let ks = next_keystream(&mut s0, &mut s1);
        let word = u64::from_le_bytes(chunk.try_into().unwrap()) ^ ks;
        chunk.copy_from_slice(&word.to_le_bytes());
    }
    for byte in chunks.into_remainder() {
        // Tail (<8 bytes): one state update per byte, low byte of the keystream.
        let ks = next_keystream(&mut s0, &mut s1);
        *byte ^= ks as u8;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let key: MoonKey = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        // Cover several lengths around the 8-byte chunk boundary + a long buffer.
        for len in [2usize, 8, 9, 16, 17, 23, 64, 1500] {
            let original: Vec<u8> = (0..len)
                .map(|i| (i as u8).wrapping_mul(37).wrapping_add(5))
                .collect();
            let mut buf = original.clone();
            outer_light_crypt(&mut buf, &key);
            assert_ne!(buf[1..], original[1..], "len={len}: data must change");
            assert_eq!(buf[0], original[0], "len={len}: seed byte unchanged");
            outer_light_crypt(&mut buf, &key);
            assert_eq!(buf, original, "len={len}: XOR is its own inverse");
        }
    }

    #[test]
    fn empty_and_single() {
        let key: MoonKey = [0u8; 16];
        let mut empty: Vec<u8> = vec![];
        outer_light_crypt(&mut empty, &key);
        assert!(empty.is_empty());

        let mut single = vec![0x42];
        outer_light_crypt(&mut single, &key);
        assert_eq!(single, vec![0x42]); // seed byte unchanged, no data to XOR
    }

    /// The QWORD path and a byte-wise reference must agree on the chunk part, and
    /// the tail must be the low bytes of the same keystream words — pinning the
    /// byte-exact QWORD layout against the Delphi reference.
    #[test]
    fn qword_matches_reference() {
        let key: MoonKey = [
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE,
            0xFF, 0x01,
        ];
        let len = 1 + 8 * 3 + 5; // seed + 3 full QWORDs + 5-byte tail
        let mut buf: Vec<u8> = (0..len).map(|i| i as u8).collect();
        let expected = reference_crypt(&buf, &key);
        outer_light_crypt(&mut buf, &key);
        assert_eq!(buf, expected);
    }

    /// Straight-line reference mirroring the Delphi body (per-chunk QWORD XOR,
    /// per-byte tail, state advanced once per chunk/byte).
    fn reference_crypt(input: &[u8], key: &MoonKey) -> Vec<u8> {
        let mut buf = input.to_vec();
        if buf.len() <= 1 {
            return buf;
        }
        let seed = buf[0] as u64;
        let mut s0 = u64::from_le_bytes(key[0..8].try_into().unwrap()) ^ seed;
        let mut s1 = u64::from_le_bytes(key[8..16].try_into().unwrap())
            ^ seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let mut pos = 1usize;
        while pos + 8 <= buf.len() {
            let mut ks = s0.wrapping_add(s1);
            ks ^= ks >> 11;
            ks ^= ks >> 23;
            let w = u64::from_le_bytes(buf[pos..pos + 8].try_into().unwrap()) ^ ks;
            buf[pos..pos + 8].copy_from_slice(&w.to_le_bytes());
            let t = s1 ^ s0;
            s0 = s0.rotate_left(55) ^ t ^ (t << 14);
            s1 = t.rotate_left(36);
            pos += 8;
        }
        while pos < buf.len() {
            let mut ks = s0.wrapping_add(s1);
            ks ^= ks >> 11;
            ks ^= ks >> 23;
            buf[pos] ^= ks as u8;
            let t = s1 ^ s0;
            s0 = s0.rotate_left(55) ^ t ^ (t << 14);
            s1 = t.rotate_left(36);
            pos += 1;
        }
        buf
    }
}
