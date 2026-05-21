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

/// Деривация пары session keys из master key + server token.
///
/// Возвращает `(encode_key, decode_key)` для **client-side** (клиент шифрует своими
/// исходящими encode_key и расшифровывает входящие decode_key).
///
/// **Algorithm:** SHAKE-128(master_key) → начальное состояние; затем `server_token`
/// XOR'ится с константой (encode/decode-specific) → 5 раундов XOR-fold через SHAKE-128
/// для каждой стороны. Сборка финальная — `(keys[false], keys[true])` для клиента
/// (Delphi convention: server encodes with `keys[true]`, decodes с `keys[false]`).
///
/// **Когда вызывать:** после получения WhoAreYou от сервера (получили `server_token`).
/// Эти ключи стабильны до следующего полного handshake (нового Hello/WhoAreYou cycle).
/// Кэшированный `Aes128Gcm` cipher в Client построен из этих ключей и пересоздаётся
/// при изменении.
///
/// A-14 (docs_api iter-2): doc comment расширен.
pub fn generate_sub_keys(master_key: &MoonKey, server_token: u64) -> (MoonKey, MoonKey) {
    let key_true = generate_sub_key(master_key, server_token ^ XOR_CONST_ENCODE);
    let key_false = generate_sub_key(master_key, server_token ^ XOR_CONST_DECODE);
    // Client (ServerSide=false): encode with keys[false], decode with keys[true]
    (key_false, key_true)
}

/// `MixValues` — SHAKE-128 хэш от `(key, token1, token2)` с 5-раундовым XOR-fold,
/// затем сборка двух u64 половин через сложение.
///
/// **Используется в:**
/// - handshake `PeerMix` вычисление (см. SPEC.md §3.1 / §3.3) — server и client
///   сверяют что обе стороны знают session token + ClientID.
/// - Дополнительная защита от MITM (атакующий без знания SubKey не сможет
///   подделать `MixValues(rnd, mix_ts, server_token)` совпадение).
///
/// **Не криптографически стойкий MAC** — это лёгкий аутентификатор сессии,
/// настоящая защита целостности пакета — `calculate_mac32` (HMAC-CRC32C).
///
/// A-14 (docs_api iter-2): doc comment расширен.
pub fn mix_values(key: &MoonKey, token1: u64, token2: u64) -> u64 {
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
