//! # moonproto::transport
//!
//! Low-level wire layer for the MoonProto protocol. This module does not
//! implement application logic, handshake, or payload encryption.
//!
//! ## What It Does
//!
//! - the client send path packs a command into a wire-ready UDP datagram:
//!   header, SipHash MAC, obfuscation using a xoroshiro128+ keystream XOR,
//!   and selected transport mode handling.
//! - [`transport_unpack`] performs the reverse operation: MAC verification,
//!   de-obfuscation, and header parsing.
//! - [`outer_light_crypt`] and [`calculate_mac32`] are standalone helpers for
//!   code that needs to work with packets without full packing or unpacking.
//! - V0, V1, and V2 are built in. Unsupported mode values are treated as V0.
//!
//! ## What It Does Not Do
//!
//! - It does not encrypt payloads. AES-128-GCM lives above the transport layer in
//!   `moonproto::crypto`.
//! - It does not perform the handshake; see `moonproto::client`.
//! - It does not parse application commands; the public API exposes retained
//!   state and typed events instead of raw command parsers.
//!
//! ## Example
//!
//! ```ignore
//! use moonproto::transport::{transport_unpack, MoonKey};
//!
//! let mac_key: MoonKey = [0u8; 16];
//! let payload = b"Hello MoonProto";
//! let cmd: u8 = 1;   // Command::Hello
//! let client_id: u64 = 0x1234_5678_ABCD_EF00;
//!
//! // S → C (receive side)
//! // let mut buf = [0u8; 65535];
//! // let n = socket.recv(&mut buf).unwrap();
//! // if let Some((hdr, payload)) = transport_unpack(&mac_key, &buf[..n], 0) {
//! //     // hdr.cmd, hdr.ver, and payload are ready for upper-layer handling.
//! // }
//! ```
//!
//! ## Performance
//!
//! Hot-path functions (`transport_unpack`/`outer_light_crypt`/
//! `calculate_mac32`/header serializers) remain marked `#[inline]`; keep that
//! unless a measured full-LTO profile replaces the current fast dev workflow.

mod extended;
mod header;
mod mac;
mod outer_crypt;

use log::warn;

pub(crate) use extended::ClientTransportModeState;
pub(crate) use header::{ClientMsgHeader, ServerMsgHeader, CLIENT_HDR_SIZE, TRANSPORT_VER};
pub(crate) use mac::MacContext;
pub(crate) use outer_crypt::outer_light_crypt;

/// MoonProto transport MAC key: 16 bytes used by SipHash-1-3.
///
/// The outer obfuscation key is derived one-way from this key and cached with
/// the MAC state; pack/unpack never pass the raw MAC key into
/// `outer_light_crypt` on the hot path.
pub type MoonKey = [u8; 16];

/// Pack one client command into a wire-ready UDP datagram.
///
/// This is intentionally stateful: V2 must use the per-client
/// [`ClientTransportModeState`] counter, matching Delphi `SentCountDNS`.
#[inline]
pub(crate) fn pack_client_packet(
    buf: &mut Vec<u8>,
    mac_ctx: &MacContext,
    cmd: u8,
    client_id: u64,
    payload: &[u8],
    mask_ver: u8,
    mode_state: &mut ClientTransportModeState,
) -> Option<Vec<u8>> {
    let hdr = ClientMsgHeader::new(cmd, client_id);

    buf.clear();
    buf.reserve(header::CLIENT_HDR_SIZE + payload.len());
    buf.extend_from_slice(&hdr.to_bytes());
    buf.extend_from_slice(payload);

    // MAC: cached SipHash state, with no per-packet key-state recomputation.
    // Checksum bytes are already zeroed by hdr.to_bytes(), so no extra clearing is needed.
    let mac = mac_ctx.mac(buf);
    buf[1..5].copy_from_slice(&mac.to_le_bytes());

    // Obfuscation (always, all modes). Keyed by the one-way obf_key, not mac_key (F1).
    outer_light_crypt(buf, mac_ctx.obf_key());

    extended::wrap_client_packet(buf, normalize_mode(mask_ver), mode_state)
}

/// Unpack a received UDP datagram. Verifies MAC and version.
/// Returns (header, payload) or None if invalid.
///
/// The returned payload reuses the same owned buffer after header removal, so
/// the common V0 receive path does not allocate a second `Vec` for payload bytes.
/// At high packet rates that keeps receive-side allocator work tied to real
/// payload ownership instead of copying bytes only to hand them to the parser.
// Convenience wrapper that builds a fresh `MacContext` per call. Production uses
// `transport_unpack_with_mac` with a cached context; this one-shot form is kept
// for the transport unit tests and as the documented simple entry point.
#[allow(dead_code)]
#[inline]
pub(crate) fn transport_unpack(
    mac_key: &MoonKey,
    raw: &[u8],
    mask_ver: u8,
) -> Option<(ServerMsgHeader, Vec<u8>)> {
    let mac_ctx = MacContext::new(mac_key);
    transport_unpack_with_mac(&mac_ctx, raw, mask_ver)
}

/// Hot-path unpack with a cached [`MacContext`]: derive the SipHash keyed initial
/// state once per client and reuse it for every datagram. This removes repeated
/// key-state setup from receive processing; each packet only pays for the MAC
/// work that depends on its bytes.
///
/// The `outer_light_crypt` whitening key lives in the cached [`MacContext`]
/// (one-way-derived from mac_key), so no separate key argument is needed.
#[inline]
pub(crate) fn transport_unpack_with_mac(
    mac_ctx: &MacContext,
    raw: &[u8],
    mask_ver: u8,
) -> Option<(ServerMsgHeader, Vec<u8>)> {
    let mut buf: Vec<u8> = extended::unwrap_server_packet(raw, normalize_mode(mask_ver))?;

    if buf.len() < header::SERVER_HDR_SIZE {
        return None;
    }

    // De-obfuscate (always, all modes). Keyed by the one-way obf_key, not mac_key (F1).
    outer_light_crypt(&mut buf, mac_ctx.obf_key());

    // Parse header
    let hdr = ServerMsgHeader::from_bytes(&buf[..header::SERVER_HDR_SIZE])?;

    // Verify MAC through the cached context. Patch and restore checksum bytes in
    // place, so MAC verification does not allocate scratch memory per datagram.
    let orig_checksum = hdr.checksum;
    let saved_csum_bytes = [buf[1], buf[2], buf[3], buf[4]];
    buf[1..5].copy_from_slice(&0u32.to_le_bytes());
    let computed = mac_ctx.mac(&buf);
    buf[1..5].copy_from_slice(&saved_csum_bytes);
    if computed != orig_checksum {
        // Foreign packet, corrupted MAC, or wrong key: common enough to avoid log spam.
        // The caller (client.rs) throttles with its should_log counter.
        warn!(target: "moonproto::transport", "MAC mismatch cmd={} ver={}", hdr.cmd, hdr.ver);
        return None;
    }

    if hdr.ver != TRANSPORT_VER {
        warn!(target: "moonproto::transport", "transport version mismatch: got={} expected={}", hdr.ver, TRANSPORT_VER);
        return None;
    }

    // Move payload bytes to the front inside the existing buffer. This avoids a
    // second Vec while keeping the parser-facing payload contiguous.
    buf.drain(..header::SERVER_HDR_SIZE);
    Some((hdr, buf))
}

#[inline]
fn normalize_mode(mask_ver: u8) -> u8 {
    match mask_ver {
        1 | 2 => mask_ver,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_server_packet(mac_key: &MoonKey, cmd: u8, payload: &[u8]) -> Vec<u8> {
        let mac_ctx = MacContext::new(mac_key);
        let hdr = ServerMsgHeader {
            rnd: 0,
            checksum: 0,
            ver: TRANSPORT_VER,
            cmd,
        };
        let mut packet = Vec::with_capacity(header::SERVER_HDR_SIZE + payload.len());
        packet.extend_from_slice(&hdr.to_bytes());
        packet.extend_from_slice(payload);
        let mac = mac_ctx.mac(&packet);
        packet[1..5].copy_from_slice(&mac.to_le_bytes());
        outer_light_crypt(&mut packet, mac_ctx.obf_key());
        packet
    }

    #[test]
    fn unsupported_transport_mode_unpacks_as_v0() {
        let mac_key = [9u8; 16];
        let payload = b"unsupported-mode-payload";
        let packet = build_server_packet(&mac_key, 43, payload);
        let (hdr, decoded) = transport_unpack(&mac_key, &packet, 7).expect("V0 fallback");

        assert_eq!(hdr.cmd, 43);
        assert_eq!(decoded, payload);
    }

    #[test]
    fn raw_mac_key_obfuscation_is_rejected() {
        let mac_key = [5u8; 16];
        let mac_ctx = MacContext::new(&mac_key);
        let payload = b"old-raw-mac-key-obfuscation";
        let hdr = ServerMsgHeader {
            rnd: 0,
            checksum: 0,
            ver: TRANSPORT_VER,
            cmd: 77,
        };
        let mut packet = Vec::with_capacity(header::SERVER_HDR_SIZE + payload.len());
        packet.extend_from_slice(&hdr.to_bytes());
        packet.extend_from_slice(payload);
        let mac = mac_ctx.mac(&packet);
        packet[1..5].copy_from_slice(&mac.to_le_bytes());

        // Old broken shape: the MAC is correct, but the whitening uses raw
        // MacKey instead of MacContext::obf_key(). New unpack must reject it.
        outer_light_crypt(&mut packet, &mac_key);

        assert!(transport_unpack(&mac_key, &packet, 0).is_none());
    }

    #[test]
    fn mode1_stun_roundtrip_unpacks_packet() {
        let mac_key = [7u8; 16];
        let payload = b"mode1-stun-payload";
        let mut packet = build_server_packet(&mac_key, 42, payload);
        extended::wrap_client_packet(&mut packet, 1, &mut ClientTransportModeState::new());

        let (hdr, decoded) = transport_unpack(&mac_key, &packet, 1).expect("mode1 unpack");

        assert_eq!(hdr.cmd, 42);
        assert_eq!(decoded, payload);
    }

    #[test]
    fn mode2_dns_warmup_is_ignored_and_normal_packet_unpacks() {
        let mac_key = [7u8; 16];
        let payload = b"mode2-payload";
        let mut state = ClientTransportModeState::new();
        let extra = {
            let mut buf = Vec::new();
            let mac_ctx = MacContext::new(&mac_key);
            pack_client_packet(&mut buf, &mac_ctx, 44, 123, payload, 2, &mut state)
        };
        let packet = build_server_packet(&mac_key, 44, payload);

        assert!(transport_unpack(&mac_key, extra.as_deref().unwrap(), 2).is_none());
        let (hdr, decoded) = transport_unpack(&mac_key, &packet, 2).expect("mode2 packet");
        assert_eq!(hdr.cmd, 44);
        assert_eq!(decoded, payload);
    }
}
