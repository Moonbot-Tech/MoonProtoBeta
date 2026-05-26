use super::MoonKey;

/// OuterLightCrypt — xoshiro128+-based stream cipher for packet obfuscation.
/// `buf[0]` is the seed (left unchanged), remaining bytes are XOR'd with keystream.
/// Identical operation for encrypt and decrypt (XOR is symmetric).
///
/// `#[inline]` обязателен: функция зовётся per-packet (десятки тысяч раз/сек на
/// пике), а вызов идёт через cross-crate границу из `moonproto`. Без явного
/// `#[inline]` LLVM не инлайнит cross-crate без `lto = "fat"`. Тело компактное,
/// I-cache pressure минимальный. Аудит B-V2-04. НЕ удалять без замены на LTO-fat.
#[inline]
pub fn outer_light_crypt(buf: &mut [u8], key: &MoonKey) {
    if buf.len() <= 1 {
        return;
    }

    let seed = buf[0] as u64;

    // Load state from key (two u64 halves)
    let mut s0 = u64::from_le_bytes(key[0..8].try_into().unwrap());
    let mut s1 = u64::from_le_bytes(key[8..16].try_into().unwrap());

    // Mix seed into state
    s0 ^= seed;
    s1 ^= seed.wrapping_mul(0x9E3779B97F4A7C15);

    for byte in &mut buf[1..] {
        // Generate keystream byte
        let result = s0.wrapping_add(s1);
        let mixed = result ^ (result >> 11);
        let ks = (mixed ^ (mixed >> 23)) as u8;

        *byte ^= ks;

        // xoshiro128+ state update
        let t = s1 ^ s0;
        s0 = s0.rotate_left(55) ^ t ^ (t << 14);
        s1 = t.rotate_left(36);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let key: MoonKey = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let original = b"Hello, MoonProto!".to_vec();
        let mut buf = original.clone();

        outer_light_crypt(&mut buf, &key);
        assert_ne!(buf[1..], original[1..]);

        outer_light_crypt(&mut buf, &key);
        assert_eq!(buf, original);
    }

    #[test]
    fn empty_and_single() {
        let key: MoonKey = [0u8; 16];
        let mut empty: Vec<u8> = vec![];
        outer_light_crypt(&mut empty, &key);
        assert!(empty.is_empty());

        let mut single = vec![0x42];
        outer_light_crypt(&mut single, &key);
        assert_eq!(single, vec![0x42]); // seed byte unchanged
    }
}
