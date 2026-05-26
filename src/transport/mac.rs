use super::MoonKey;

/// mORMot THmacCrc32c: CRC32C(ipad_block || message || opad_block)
/// NOT standard HMAC! It's a single continuous CRC32C stream.
///
/// Algorithm:
///   k0 = key zero-padded to 64 bytes
///   ipad_block = k0 XOR 0x36 (per byte), 64 bytes
///   opad_block = k0 XOR 0x5C (per byte), 64 bytes
///   result = CRC32C(0, ipad_block) → CRC32C(prev, message) → CRC32C(prev, opad_block)
///
/// `#[inline]` обязателен: hot path (MAC проверяется на каждом принятом и MAC
/// строится на каждом отправляемом пакете), cross-crate вызов из `moonproto`.
/// Без явного inline LLVM не инлайнит cross-crate. Аудит B-V2-04.
#[inline]
pub fn calculate_mac32(key: &MoonKey, data: &[u8]) -> u32 {
    const BLOCK_SIZE: usize = 64;

    let mut k0 = [0u8; BLOCK_SIZE];
    k0[..16].copy_from_slice(key);

    let mut ipad_block = [0u8; BLOCK_SIZE];
    let mut opad_block = [0u8; BLOCK_SIZE];
    for i in 0..BLOCK_SIZE {
        ipad_block[i] = k0[i] ^ 0x36;
        opad_block[i] = k0[i] ^ 0x5C;
    }

    // Single continuous CRC32C stream: ipad || data || opad
    let mut crc = crc32c::crc32c(&ipad_block);
    crc = crc32c::crc32c_append(crc, data);
    crc = crc32c::crc32c_append(crc, &opad_block);
    crc
}

/// Cached MAC context: pre-computed CRC32C(ipad) и opad_block для сессионного ключа.
///
/// Создаётся один раз на сессию через [`MacContext::new`], затем `mac(data)` выполняет
/// только `crc32c_append(cached, data) + crc32c_append(prev, opad_block)` — без пересчёта
/// ipad/opad на каждом пакете. audit_rust_quality #3: ~20K XOR/сек убраны на пиковой
/// нагрузке (50K MAC ops × 128 XOR байт = 6.4M ops/sec → 0).
///
/// Wire-результат byte-exact идентичен [`calculate_mac32`]: один и тот же continuous
/// CRC32C stream (`CRC32C(ipad || data || opad)`), просто промежуточное значение после
/// ipad закэшировано.
#[derive(Clone)]
pub struct MacContext {
    crc_after_ipad: u32,
    opad_block: [u8; 64],
}

impl MacContext {
    /// Создать контекст для данного ключа. Делает 128 XOR + один `crc32c(ipad)` —
    /// тяжёлая часть. Затем `mac()` выполняет только финализацию.
    pub fn new(key: &MoonKey) -> Self {
        const BLOCK_SIZE: usize = 64;
        let mut k0 = [0u8; BLOCK_SIZE];
        k0[..16].copy_from_slice(key);

        let mut ipad_block = [0u8; BLOCK_SIZE];
        let mut opad_block = [0u8; BLOCK_SIZE];
        for i in 0..BLOCK_SIZE {
            ipad_block[i] = k0[i] ^ 0x36;
            opad_block[i] = k0[i] ^ 0x5C;
        }
        Self {
            crc_after_ipad: crc32c::crc32c(&ipad_block),
            opad_block,
        }
    }

    /// Вычислить MAC для данных. На hot path заменяет `calculate_mac32(&key, data)`:
    /// одна и та же байт-точная функция, но без 128 XOR + `crc32c(ipad)` per call.
    #[inline]
    pub fn mac(&self, data: &[u8]) -> u32 {
        let crc = crc32c::crc32c_append(self.crc_after_ipad, data);
        crc32c::crc32c_append(crc, &self.opad_block)
    }
}

impl std::fmt::Debug for MacContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Не показываем opad_block (зависит от mac_key) в логах.
        f.debug_struct("MacContext")
            .field("crc_after_ipad", &"<cached>")
            .finish()
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

    /// Critical correctness test (audit_rust_quality #3): MacContext должен давать
    /// бит-в-бит тот же результат что и плоская `calculate_mac32`. Любое расхождение =
    /// wire incompatibility c сервером.
    #[test]
    fn context_matches_flat() {
        let key: MoonKey = [
            0xA1, 0xB2, 0xC3, 0xD4, 0xE5, 0xF6, 0x07, 0x18, 0x29, 0x3A, 0x4B, 0x5C, 0x6D, 0x7E,
            0x8F, 0x90,
        ];
        let ctx = MacContext::new(&key);
        for &len in &[0usize, 1, 15, 16, 17, 63, 64, 65, 500, 1500] {
            let data: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_mul(31)).collect();
            assert_eq!(
                ctx.mac(&data),
                calculate_mac32(&key, &data),
                "mismatch at len={}",
                len
            );
        }
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
