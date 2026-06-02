//! MoonBot exported-key importer.
//!
//! [`import_key`] decodes the base64 encrypted container copied from MoonBot
//! and returns the two 128-bit keys required by [`crate::ClientConfig`]:
//! the AES-GCM master key and the transport MAC/obfuscation key. Current
//! MoonBot exports can also include endpoint and transport-mode metadata.
//! Use [`parse_key_info`] when UI code wants to show the key name and suggest
//! those connection settings to the user.
//!
//! The parser is a byte-exact port of `TMoonProtoForm.bPasteKeyClick`.
//! It tries the current V1 password head first, then falls back to the legacy
//! key-only password head. Both branches verify the Delphi stream checksum after
//! decryption.
//!
//! `TMoonProtoKeyContainer` is 72 bytes and contains the fixed-size random
//! marker, filled flag, Delphi `TDateTime`, bot id, version, flags, master key,
//! MAC key, and checksum.
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use crate::client::TransportMode;
#[cfg(any(test, feature = "diagnostics"))]
use crate::time::DelphiTime;
use crate::time::MoonTime;
use crate::MoonKey;
use zeroize::Zeroize;

const OLD_PWD_HEAD: &str = "F$xC";
const NEW_PWD_HEAD: &str = "F$xC2";
const PWD_TAIL: &str = "aR#d";
const FMT_VER_CUR: u8 = 1;
const KEY_CONTAINER_SIZE: usize = 72;
const OLD_PLAIN_SIZE: usize = 8 + KEY_CONTAINER_SIZE;
const NEW_PLAIN_SIZE: usize = 8 + 1 + KEY_CONTAINER_SIZE + 2 + 1 + 4 + 16 + 1;
const ID_IPV4: u8 = 0;
const ID_IPV6: u8 = 1;

/// MoonBot exported-key container format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportedKeyFormat {
    /// Legacy export: only `TMoonProtoKeyContainer`.
    Legacy,
    /// Current export version 1: key container plus endpoint and transport mode.
    V1,
}

/// IP version selected in the MoonBot export payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportedIpVersion {
    V4,
    V6,
}

/// Endpoint and transport metadata carried by current MoonBot key exports.
///
/// `address` can be `None`: Delphi applies the exported port/transport mode
/// even when the active IP branch contains a zero address, and leaves the
/// previously configured IP untouched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImportedNetworkConfig {
    /// Active IP version in the exported payload.
    pub ip_version: ImportedIpVersion,
    /// Exported server IP, if the active IP branch was non-zero.
    pub address: Option<IpAddr>,
    /// Exported server UDP port.
    pub port: u16,
    /// Exported MoonProto transport mode (`V0`, `V1`, or `V2`).
    pub transport_mode: TransportMode,
}

/// Full parsed MoonBot key export for UI/config screens.
///
/// UI can show [`Self::display_name`] exactly like MoonBot's key field and
/// pre-fill server/mode controls from [`Self::network`]. The final connection
/// still uses the endpoint and transport mode selected by the user.
#[derive(Debug, Clone)]
pub struct ImportedKeyInfo {
    /// Cryptographic keys used by the MoonProto session.
    pub keys: ImportedKeys,
    /// Export container format used by MoonBot.
    pub format: ImportedKeyFormat,
    /// `TMoonProtoKeyContainer.rnd`.
    pub rnd: String,
    /// Raw Delphi `TDateTime` from the key container, retained for diagnostics.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub date: f64,
    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) date: f64,
    /// UI label equivalent to MoonBot:
    /// `rnd + "  " + FormatDateTime("dd.mm.yyyy hh:nn", Date)`.
    pub display_name: String,
    /// Suggested endpoint/transport settings from current V1 exports.
    pub network: Option<ImportedNetworkConfig>,
}

impl ImportedKeyInfo {
    /// Key container date as a normal Rust-facing MoonProto timestamp.
    pub fn date(&self) -> MoonTime {
        MoonTime::from_delphi_days(self.date).unwrap_or(MoonTime::ZERO)
    }

    /// Key container date as typed Delphi `TDateTime`.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn date_delphi(&self) -> DelphiTime {
        DelphiTime::from_days(self.date)
    }
}

/// Decoded cryptographic keys from MoonBot export.
///
/// This type is `Copy` because all fields are small value types. Applications
/// can pass the imported keys into one or more `ClientConfig` builders without
/// needing explicit clones.
#[derive(Clone, Copy)]
pub struct ImportedKeys {
    /// AES-GCM master key used by the MoonProto session handshake.
    pub master_key: MoonKey,
    /// Transport MAC/obfuscation key.
    pub mac_key: MoonKey,
    filled: bool,
    container_version: u8,
}

impl std::fmt::Debug for ImportedKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImportedKeys")
            .field("master_key", &"<REDACTED>")
            .field("mac_key", &"<REDACTED>")
            .field("filled", &self.filled)
            .field("container_version", &self.container_version)
            .finish()
    }
}

/// Import MoonBot key from base64 export string.
/// Returns the cryptographic keys needed for [`crate::ClientConfig`]. UI code
/// should use [`parse_key_info`] to get the display name and suggested
/// connection fields.
pub fn import_key(base64_str: &str) -> Option<ImportedKeys> {
    parse_key_info(base64_str).map(|info| info.keys)
}

/// Parse MoonBot key export for UI/config screens.
pub fn parse_key_info(base64_str: &str) -> Option<ImportedKeyInfo> {
    use base64::Engine;
    let compact = base64_str
        .chars()
        .filter(|ch| !ch.is_ascii_whitespace())
        .collect::<String>();
    let raw = base64::engine::general_purpose::STANDARD
        .decode(compact)
        .ok()?;
    if raw.len() < 16 + OLD_PLAIN_SIZE {
        return None;
    }

    let ts = i64::from_le_bytes(raw[0..8].try_into().unwrap());
    let checksum = i64::from_le_bytes(raw[8..16].try_into().unwrap());
    let encrypted = &raw[16..];

    if let Some(mut plain) = try_decrypt(encrypted, NEW_PWD_HEAD, ts, checksum) {
        let parsed = parse_v1_plain(&plain);
        plain.zeroize();
        return parsed;
    }

    let mut plain = try_decrypt(encrypted, OLD_PWD_HEAD, ts, checksum)?;
    let parsed = parse_legacy_plain(&plain);
    plain.zeroize();
    parsed
}

fn try_decrypt(encrypted: &[u8], password_head: &str, ts: i64, checksum: i64) -> Option<Vec<u8>> {
    let mut password = password_bytes(password_head, ts);
    let mut plain = encrypted.to_vec();
    decode_buffer(&mut plain, &password);
    let ok = calculate_checksum_w(&plain) == checksum;
    password.zeroize();
    if ok {
        Some(plain)
    } else {
        plain.zeroize();
        None
    }
}

fn password_bytes(password_head: &str, ts: i64) -> Vec<u8> {
    // Delphi TCode = string[25], so the short string payload is truncated to
    // the first 25 bytes after ordinary ANSI assignment.
    format!("{password_head}{ts}{PWD_TAIL}")
        .bytes()
        .take(25)
        .collect()
}

fn parse_legacy_plain(plain: &[u8]) -> Option<ImportedKeyInfo> {
    if plain.len() < OLD_PLAIN_SIZE {
        return None;
    }
    let container = parse_key_container(plain, 8)?;
    Some(imported_key_info(
        container,
        ImportedKeyFormat::Legacy,
        None,
    ))
}

fn imported_key_info(
    container: KeyContainerFields,
    format: ImportedKeyFormat,
    network: Option<ImportedNetworkConfig>,
) -> ImportedKeyInfo {
    let display_date = format_delphi_datetime(container.date);
    let display_name = format!("{}  {}", container.rnd, display_date);
    let keys = ImportedKeys {
        master_key: container.master_key,
        mac_key: container.mac_key,
        filled: container.filled,
        container_version: container.ver,
    };
    ImportedKeyInfo {
        keys,
        format,
        rnd: container.rnd,
        date: container.date,
        display_name,
        network,
    }
}

fn parse_v1_plain(plain: &[u8]) -> Option<ImportedKeyInfo> {
    if plain.len() < NEW_PLAIN_SIZE {
        return None;
    }
    let ver = plain[8];
    if ver != FMT_VER_CUR {
        return None;
    }

    let container_offset = 9;
    let container = parse_key_container(plain, container_offset)?;

    let mut offset = container_offset + KEY_CONTAINER_SIZE;
    let port = u16::from_le_bytes(plain[offset..offset + 2].try_into().unwrap());
    offset += 2;
    let ip_ver = plain[offset];
    offset += 1;
    let ip4 = u32::from_le_bytes(plain[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let ip6: [u8; 16] = plain[offset..offset + 16].try_into().unwrap();
    offset += 16;
    let mask_ver = plain[offset];

    if ip_ver != ID_IPV4 && ip_ver != ID_IPV6 {
        return None;
    }
    if mask_ver > 2 {
        return None;
    }

    let (ip_version, address) = if ip_ver == ID_IPV4 {
        let address = (ip4 != 0).then_some(IpAddr::V4(Ipv4Addr::from(ip4)));
        (ImportedIpVersion::V4, address)
    } else {
        let address = (ip6 != [0; 16]).then_some(IpAddr::V6(Ipv6Addr::from(ip6)));
        (ImportedIpVersion::V6, address)
    };

    Some(imported_key_info(
        container,
        ImportedKeyFormat::V1,
        Some(ImportedNetworkConfig {
            ip_version,
            address,
            port,
            transport_mode: TransportMode::from_byte(mask_ver),
        }),
    ))
}

struct KeyContainerFields {
    master_key: MoonKey,
    mac_key: MoonKey,
    filled: bool,
    ver: u8,
    rnd: String,
    date: f64,
}

fn parse_key_container(plain: &[u8], container_offset: usize) -> Option<KeyContainerFields> {
    if plain.len() < container_offset + KEY_CONTAINER_SIZE {
        return None;
    }

    let rnd_len = plain[container_offset] as usize;
    if rnd_len > 16 {
        return None;
    }

    let filled = plain[container_offset + 17];
    let date = f64::from_le_bytes(
        plain[container_offset + 18..container_offset + 26]
            .try_into()
            .unwrap(),
    );
    let ver = plain[container_offset + 30];

    let mut master_key = [0u8; 16];
    master_key.copy_from_slice(&plain[container_offset + 32..container_offset + 48]);

    let mut mac_key = [0u8; 16];
    mac_key.copy_from_slice(&plain[container_offset + 48..container_offset + 64]);

    if filled != 1 || ver < 1 {
        return None;
    }

    let rnd_bytes = &plain[container_offset + 1..container_offset + 1 + rnd_len];
    if !rnd_bytes.iter().all(|&b| (32..127).contains(&b)) {
        return None;
    }
    let rnd = String::from_utf8(rnd_bytes.to_vec()).ok()?;

    Some(KeyContainerFields {
        master_key,
        mac_key,
        filled: filled == 1,
        ver,
        rnd,
        date,
    })
}

fn format_delphi_datetime(value: f64) -> String {
    if !value.is_finite() {
        return "00.00.0000 00:00".to_string();
    }
    let total_millis = (value * 86_400_000.0).round() as i64;
    let days = total_millis.div_euclid(86_400_000);
    let millis_of_day = total_millis.rem_euclid(86_400_000);
    let unix_days = days - 25_569;
    let (year, month, day) = civil_from_days(unix_days);
    let hour = millis_of_day / 3_600_000;
    let minute = (millis_of_day % 3_600_000) / 60_000;
    format!("{day:02}.{month:02}.{year:04} {hour:02}:{minute:02}")
}

fn civil_from_days(days_since_unix_epoch: i64) -> (i32, u32, u32) {
    let z = days_since_unix_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = y + i64::from(month <= 2);
    (year as i32, month as u32, day as u32)
}

/// Delphi `sfunc.pas` `CalculateCheckSumW` x64 algorithm.
fn calculate_checksum_w(buf: &[u8]) -> i64 {
    let mut rax = 0u64;
    for &byte in buf {
        let bl = byte ^ 0b1010_1010;
        let bh = byte ^ 0b0011_1001;

        let (al, cf) = add8(rax as u8, bl);
        rax = set_al(rax, al);

        let (next_rax, cf) = rcl64_1(rax, cf);
        rax = next_rax;

        let (al, _cf) = adc8(rax as u8, 0, cf);
        rax = set_al(rax, al);

        let (next_rax, cf) = rol64_8(rax);
        rax = next_rax;

        let ah = ((rax >> 8) & 0xFF) as u8;
        let (ah, cf) = adc8(ah, bh, cf);
        rax = set_ah(rax, ah);

        let (al, _cf) = adc8(rax as u8, 0, cf);
        rax = set_al(rax, al);

        let (next_rax, cf) = rol64_8(rax);
        rax = next_rax;

        let (al, _cf) = adc8(rax as u8, 0, cf);
        rax = set_al(rax, al);
    }
    rax as i64
}

fn add8(a: u8, b: u8) -> (u8, bool) {
    let sum = a as u16 + b as u16;
    (sum as u8, sum > 0xFF)
}

fn adc8(a: u8, b: u8, carry: bool) -> (u8, bool) {
    let sum = a as u16 + b as u16 + u16::from(carry);
    (sum as u8, sum > 0xFF)
}

fn rcl64_1(value: u64, carry: bool) -> (u64, bool) {
    ((value << 1) | u64::from(carry), (value & (1 << 63)) != 0)
}

fn rol64_8(value: u64) -> (u64, bool) {
    let rotated = value.rotate_left(8);
    (rotated, (rotated & 1) != 0)
}

fn set_al(value: u64, al: u8) -> u64 {
    (value & !0xFF) | u64::from(al)
}

fn set_ah(value: u64, ah: u8) -> u64 {
    (value & !(0xFF << 8)) | (u64::from(ah) << 8)
}

/// DecodeBuffer — byte-exact port of sfunc.pas x64 ASM (lines 169-284).
/// Decrypts in-place using password bytes.
fn decode_buffer(buf: &mut [u8], code: &[u8]) {
    let code_len = code.len();
    if code_len == 0 || buf.is_empty() {
        return;
    }

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
        let nibble = counter & 0xF;
        let cn2 = if code_len > nibble + 2 {
            code[nibble + 2]
        } else {
            0
        };
        let cl_val = cn2.wrapping_add(nibble as u8).wrapping_add(ah);
        b ^= cl_val;

        let cn1 = if code_len > nibble + 1 {
            code[nibble + 1]
        } else {
            0
        };
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

    fn encode_buffer(buf: &mut [u8], code: &[u8]) {
        let code_len = code.len();
        for (counter, byte) in buf.iter_mut().enumerate() {
            let al = (counter & 0xFF) as u8;
            let ah = ((counter >> 8) & 0xFF) as u8;
            let nibble = counter & 0xF;
            let c2 = if code_len > 2 { code[2] } else { 0 };
            let c2_mod = c2 & 7;
            let c1 = if code_len > 1 { code[1] } else { 0 };
            let c0 = if code_len > 0 { code[0] } else { 0 };
            let c3 = if code_len > 3 { code[3] } else { 0 };
            let cn2 = if code_len > nibble + 2 {
                code[nibble + 2]
            } else {
                0
            };
            let cn1 = if code_len > nibble + 1 {
                code[nibble + 1]
            } else {
                0
            };
            let cn0 = if code_len > nibble { code[nibble] } else { 0 };
            let cl_val = cn2.wrapping_add(nibble as u8).wrapping_add(ah);
            let cn1_mod = cn1 & 7;
            let cl_val2 = cn0.wrapping_add(cn1).wrapping_add(1);

            let mut b = *byte;
            b = b.wrapping_add(al);
            b = b.wrapping_add(ah);
            b ^= cl_val2;
            b = b.wrapping_add(nibble as u8);
            b = b.rotate_left(cn1_mod as u32);
            b ^= cl_val;
            b = b.wrapping_add(c3);
            b ^= c0;
            b = b.wrapping_add(c1 ^ ah);
            b = b.rotate_right(c2_mod as u32);
            b = b.wrapping_add(al);
            b = b.rotate_left(c2_mod as u32);
            b ^= al;
            b = b.wrapping_add(al);
            b = b.wrapping_add(ah);
            *byte = b;
        }
    }

    fn write_container(clear: &mut [u8], container: usize, master_key: MoonKey, mac_key: MoonKey) {
        let rnd = b"TESTKEY";
        clear[container] = rnd.len() as u8;
        clear[container + 1..container + 1 + rnd.len()].copy_from_slice(rnd);
        clear[container + 17] = 1; // filled
        clear[container + 18..container + 26].copy_from_slice(&25_569.0_f64.to_le_bytes());
        clear[container + 30] = 1; // key container version
        clear[container + 32..container + 48].copy_from_slice(&master_key);
        clear[container + 48..container + 64].copy_from_slice(&mac_key);
    }

    fn wrap_export(mut clear: Vec<u8>, password_head: &str, outer_ts: i64) -> String {
        use base64::Engine;

        let checksum = calculate_checksum_w(&clear);
        let password = password_bytes(password_head, outer_ts);
        encode_buffer(&mut clear, &password);

        let mut raw = Vec::with_capacity(16 + clear.len());
        raw.extend_from_slice(&outer_ts.to_le_bytes());
        raw.extend_from_slice(&checksum.to_le_bytes());
        raw.extend_from_slice(&clear);
        base64::engine::general_purpose::STANDARD.encode(raw)
    }

    fn build_legacy_test_export(master_key: MoonKey, mac_key: MoonKey) -> String {
        let inner_ts = 11_111_111_i64;
        let outer_ts = 12_345_678_i64;
        let mut clear = vec![0u8; OLD_PLAIN_SIZE];
        clear[0..8].copy_from_slice(&inner_ts.to_le_bytes());
        write_container(&mut clear, 8, master_key, mac_key);
        wrap_export(clear, OLD_PWD_HEAD, outer_ts)
    }

    fn build_v1_ipv4_test_export(
        master_key: MoonKey,
        mac_key: MoonKey,
        ip4: u32,
        port: u16,
        mask_ver: u8,
    ) -> String {
        let inner_ts = 22_222_222_i64;
        let outer_ts = 87_654_321_i64;
        let mut clear = vec![0u8; NEW_PLAIN_SIZE];
        clear[0..8].copy_from_slice(&inner_ts.to_le_bytes());
        clear[8] = FMT_VER_CUR;
        write_container(&mut clear, 9, master_key, mac_key);
        let mut offset = 9 + KEY_CONTAINER_SIZE;
        clear[offset..offset + 2].copy_from_slice(&port.to_le_bytes());
        offset += 2;
        clear[offset] = ID_IPV4;
        offset += 1;
        clear[offset..offset + 4].copy_from_slice(&ip4.to_le_bytes());
        offset += 4 + 16;
        clear[offset] = mask_ver;
        wrap_export(clear, NEW_PWD_HEAD, outer_ts)
    }

    #[test]
    fn import_legacy_key() {
        let master_key = [
            0x30, 0x1b, 0x92, 0x12, 0x09, 0xae, 0x79, 0xa5, 0x10, 0x86, 0xb1, 0x80, 0xd3, 0x25,
            0xcb, 0xd6,
        ];
        let mac_key = [
            0x29, 0x05, 0xa9, 0xc4, 0x13, 0x10, 0xe4, 0x3f, 0x07, 0x04, 0x93, 0x63, 0x40, 0xfa,
            0x45, 0xa5,
        ];
        let key_b64 = build_legacy_test_export(master_key, mac_key);
        let keys = import_key(&key_b64).expect("Failed to import key");
        assert!(keys.filled);
        assert_eq!(keys.container_version, 1);
        assert_eq!(keys.master_key, master_key);
        assert_eq!(keys.mac_key, mac_key);

        let info = parse_key_info(&key_b64).expect("Failed to parse key info");
        assert_eq!(info.format, ImportedKeyFormat::Legacy);
        assert_eq!(info.rnd, "TESTKEY");
        assert_eq!(info.display_name, "TESTKEY  01.01.1970 00:00");
        assert_eq!(info.network, None);
    }

    #[test]
    fn import_v1_key_with_network() {
        let master_key = [0x11; 16];
        let mac_key = [0x22; 16];
        let key_b64 = build_v1_ipv4_test_export(master_key, mac_key, 0x7F00_0001, 3000, 2);
        let keys = import_key(&key_b64).expect("Failed to import V1 key");
        assert_eq!(keys.master_key, master_key);
        assert_eq!(keys.mac_key, mac_key);
        let info = parse_key_info(&key_b64).expect("Failed to parse V1 key info");
        assert_eq!(info.format, ImportedKeyFormat::V1);
        assert_eq!(
            info.network,
            Some(ImportedNetworkConfig {
                ip_version: ImportedIpVersion::V4,
                address: Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
                port: 3000,
                transport_mode: TransportMode::V2,
            })
        );

        let network = info.network.expect("V1 endpoint expected");
        let address = network.address.expect("V1 IP expected");
        let cfg = crate::ClientConfig::new(
            address.to_string(),
            network.port,
            keys.master_key,
            keys.mac_key,
        )
        .with_transport_mode(network.transport_mode);
        assert_eq!(cfg.server_ip, "127.0.0.1");
        assert_eq!(cfg.server_port, 3000);
        assert_eq!(cfg.transport_mode, TransportMode::V2);
    }

    #[test]
    fn import_v1_zero_ip_keeps_network_metadata_for_fallback() {
        let master_key = [0x33; 16];
        let mac_key = [0x44; 16];
        let key_b64 = build_v1_ipv4_test_export(master_key, mac_key, 0, 4000, 0);
        let keys = import_key(&key_b64).expect("Failed to import V1 key");
        let info = parse_key_info(&key_b64).expect("Failed to parse V1 key info");
        assert_eq!(
            info.network,
            Some(ImportedNetworkConfig {
                ip_version: ImportedIpVersion::V4,
                address: None,
                port: 4000,
                transport_mode: TransportMode::V0,
            })
        );
        let network = info.network.expect("V1 network metadata expected");
        let cfg = crate::ClientConfig::new(
            network
                .address
                .map(|addr| addr.to_string())
                .unwrap_or_else(|| "example.com".to_string()),
            network.port,
            keys.master_key,
            keys.mac_key,
        )
        .with_transport_mode(network.transport_mode);
        assert_eq!(cfg.server_ip, "example.com");
        assert_eq!(cfg.server_port, 4000);
        assert_eq!(cfg.transport_mode, TransportMode::V0);
    }

    #[test]
    fn checksum_mismatch_rejects_key() {
        let key_b64 = build_legacy_test_export([0x55; 16], [0x66; 16]);
        use base64::Engine;
        let mut raw = base64::engine::general_purpose::STANDARD
            .decode(key_b64)
            .unwrap();
        raw[8] ^= 0x01;
        let corrupted = base64::engine::general_purpose::STANDARD.encode(raw);
        assert!(import_key(&corrupted).is_none());
    }

    #[test]
    #[ignore = "set MOONPROTO_TEST_KEY to inspect one exported MoonBot key locally"]
    fn parse_env_key_info() {
        let key = std::env::var("MOONPROTO_TEST_KEY").expect("MOONPROTO_TEST_KEY is required");
        let info = parse_key_info(&key).expect("invalid MoonBot key");
        eprintln!(
            "KEY_INFO format={:?} display_name={:?} network={:?} filled={} key_ver={}",
            info.format,
            info.display_name,
            info.network,
            info.keys.filled,
            info.keys.container_version
        );
    }
}
