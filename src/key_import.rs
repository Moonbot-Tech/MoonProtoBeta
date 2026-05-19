/// Key import — parse MoonBot exported key (base64 encrypted container).
/// Byte-exact port of MoonProtoUnit.pas:194-262 (bPasteKeyClick) + sfunc.pas:169-284 (DecodeBuffer).
///
/// Export format: base64(ts:i64_le + checksum:i64_le + EncodeBuffer(ts2:i64_le + TMoonProtoKeyContainer))
/// Password: 'F$xC' + ts.ToString() + 'aR#d' (truncated to 25 chars = Delphi TCode = string[25])
///
/// TMoonProtoKeyContainer (72 bytes):
///   rnd: string[16] (1 len + 16 chars = 17 bytes)
///   Filled: boolean (1 byte)
///   Date: TDateTime (8 bytes, f64)
///   BotID: integer (4 bytes, i32)
///   Ver: byte (1)
///   Flags: byte (1)
///   FKey: THash128 (16 bytes) ← MasterKey
///   FMacKey: THash128 (16 bytes) ← MacKey
///   hash: int64 (8 bytes)

use crate::MoonKey;

/// Decoded key pair from MoonBot export.
///
/// `Copy` — все поля Copy (`MoonKey = [u8; 16]`, bool, u8). Упрощает API:
/// потребитель не пишет `keys.clone()` для передачи в несколько мест.
#[derive(Debug, Clone, Copy)]
pub struct ImportedKeys {
    pub master_key: MoonKey,
    pub mac_key: MoonKey,
    pub filled: bool,
    pub ver: u8,
}

/// Import MoonBot key from base64 export string.
/// Returns (MasterKey, MacKey) or None if parsing fails.
pub fn import_key(base64_str: &str) -> Option<ImportedKeys> {
    use base64::Engine;
    let raw = base64::engine::general_purpose::STANDARD.decode(base64_str.trim()).ok()?;
    if raw.len() < 16 { return None; }

    // Extract ts (first 8 bytes, SIGNED i64) and checksum (next 8 bytes)
    let ts = i64::from_le_bytes(raw[0..8].try_into().unwrap());
    let _checksum = i64::from_le_bytes(raw[8..16].try_into().unwrap());
    let mut encrypted = raw[16..].to_vec();

    // Build password: 'F$xC' + ts.ToString() + 'aR#d', truncated to 25 chars
    // Delphi TCode = string[25]
    let password_str = format!("F$xC{}aR#d", ts);
    let password: Vec<u8> = password_str.bytes().take(25).collect();

    // DecodeBuffer — byte-exact port of sfunc.pas:169-284 (x64 ASM)
    decode_buffer(&mut encrypted, &password);

    // After decryption: ts2(8) + TMoonProtoKeyContainer(72)
    if encrypted.len() < 80 { return None; }

    let container_offset = 8;

    // TMoonProtoKeyContainer fields:
    // rnd: string[16] at +0 (1 byte len + 16 bytes chars)
    let rnd_len = encrypted[container_offset] as usize;
    if rnd_len > 16 { return None; }

    let filled = encrypted[container_offset + 17];
    // Date at +18 (8 bytes, f64)
    // BotID at +26 (4 bytes, i32)
    let ver = encrypted[container_offset + 30];
    // Flags at +31

    // FKey (MasterKey) at container+32
    let mut master_key = [0u8; 16];
    master_key.copy_from_slice(&encrypted[container_offset + 32..container_offset + 48]);

    // FMacKey at container+48
    let mut mac_key = [0u8; 16];
    mac_key.copy_from_slice(&encrypted[container_offset + 48..container_offset + 64]);

    // Sanity check
    if filled != 1 || ver < 1 {
        return None;
    }

    // Verify rnd is readable ASCII
    let rnd_bytes = &encrypted[container_offset + 1..container_offset + 1 + rnd_len];
    if !rnd_bytes.iter().all(|&b| b >= 32 && b < 127) {
        return None;
    }

    Some(ImportedKeys {
        master_key,
        mac_key,
        filled: filled == 1,
        ver,
    })
}

/// DecodeBuffer — byte-exact port of sfunc.pas x64 ASM (lines 169-284).
/// Decrypts in-place using password bytes.
fn decode_buffer(buf: &mut [u8], code: &[u8]) {
    let code_len = code.len();
    if code_len == 0 || buf.is_empty() { return; }

    for (counter, byte) in buf.iter_mut().enumerate() {
        let al = (counter & 0xFF) as u8;
        let ah = ((counter >> 8) & 0xFF) as u8;
        let mut b = *byte;

        // step 1-3
        b = b.wrapping_sub(ah);
        b = b.wrapping_sub(al);
        b ^= al;

        // step 4-6: cl = code[2]
        let c2 = if code_len > 2 { code[2] } else { 0 };
        let c2_mod = c2 & 7;
        b = b.rotate_right(c2_mod as u32);
        b = b.wrapping_sub(al);
        b = b.rotate_left(c2_mod as u32);

        // step 7: sub (code[1] ^ ah)
        let c1 = if code_len > 1 { code[1] } else { 0 };
        b = b.wrapping_sub(c1 ^ ah);

        // step 8: xor code[0]
        let c0 = if code_len > 0 { code[0] } else { 0 };
        b ^= c0;

        // step 9: sub code[3]
        let c3 = if code_len > 3 { code[3] } else { 0 };
        b = b.wrapping_sub(c3);

        // step 10-13 (nibble = counter & 0xF)
        let nibble = (counter & 0xF) as usize;
        let cn2 = if code_len > nibble + 2 { code[nibble + 2] } else { 0 };
        let cl_val = cn2.wrapping_add(nibble as u8);
        b ^= cl_val;

        let cn1 = if code_len > nibble + 1 { code[nibble + 1] } else { 0 };
        let cn1_mod = cn1 & 7;
        b = b.rotate_right(cn1_mod as u32);
        b = b.wrapping_sub(nibble as u8);

        let cn0 = if code_len > nibble { code[nibble] } else { 0 };
        let cl_val2 = cn0.wrapping_add(cn1).wrapping_add(1);
        b ^= cl_val2;

        // step 14-15
        b = b.wrapping_sub(ah);
        b = b.wrapping_sub(al);

        *byte = b;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn import_test_key() {
        let key_b64 = "v3oshQy/OLZSjsCkpZIOuy4y7aWoD7U12kIXJSx7h8cBKiRjEVPSrBB8WVO7yCjC+/91HC9M+/rtivjR9IA9uN+pdyLbFSeAkYiWWIC/7oefi1dHu9zUy8Jf+Rom/4Md";
        let keys = import_key(key_b64).expect("Failed to import key");
        assert!(keys.filled);
        assert_eq!(keys.ver, 1);
        assert_eq!(keys.master_key, [0x30, 0x1b, 0x92, 0x12, 0x09, 0xae, 0x79, 0xa5,
                                      0x10, 0x86, 0xb1, 0x80, 0xd3, 0x25, 0xcb, 0xd6]);
        assert_eq!(keys.mac_key, [0x29, 0x05, 0xa9, 0xc4, 0x13, 0x10, 0xe4, 0x3f,
                                   0x07, 0x04, 0x93, 0x63, 0x40, 0xfa, 0x45, 0xa5]);
    }
}
