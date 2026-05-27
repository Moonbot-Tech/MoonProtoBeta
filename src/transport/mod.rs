//! # moonproto::transport
//!
//! Low-level wire layer for the MoonProto protocol. This module does not
//! implement application logic, handshake, or payload encryption.
//!
//! ## What It Does
//!
//! - [`transport_pack`] packs a command into a wire-ready UDP datagram: header,
//!   HMAC-CRC32C MAC, obfuscation using a xoshiro128+ keystream XOR, and
//!   selected transport mode handling.
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
//! - It does not parse application commands; see `moonproto::commands`.
//!
//! ## Example
//!
//! ```ignore
//! use moonproto::transport::{transport_pack, transport_unpack, MoonKey};
//!
//! let mac_key: MoonKey = [0u8; 16];
//! let payload = b"Hello MoonProto";
//! let cmd: u8 = 1;   // Command::Hello
//! let client_id: u64 = 0x1234_5678_ABCD_EF00;
//!
//! // C → S
//! let (packet, _extra) = transport_pack(&mac_key, cmd, client_id, payload, 0);
//! // socket.send_to(&packet, server_addr).unwrap();
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
//! Hot-path functions (`transport_pack`/`transport_unpack`/`outer_light_crypt`/
//! `calculate_mac32`/header serializers) remain marked `#[inline]`; keep that
//! unless a measured full-LTO profile replaces the current fast dev workflow.

mod extended;
mod header;
mod mac;
mod outer_crypt;

use log::warn;

pub use extended::TransportModeState;
pub use header::{ClientMsgHeader, ServerMsgHeader, TRANSPORT_VER};
pub use mac::{calculate_mac32, MacContext};
pub use outer_crypt::outer_light_crypt;

/// MoonProto transport key: 16 bytes used by the MAC and outer obfuscation
/// layer.
pub type MoonKey = [u8; 16];

/// Pack a command into a wire-ready UDP datagram.
/// mask_ver: 0 = V0/base transport, 1/2 = V1/V2.
/// Unsupported values are treated as mode 0.
/// Returns: (main_packet, optional_extra_packet)
/// If extended transport needs an additional packet sent, it's returned as the second element.
///
/// **Hot-path note**: this allocates a new `Vec` and computes the HMAC ipad/opad
/// state from scratch for each packet (128 XOR operations plus CRC32C). For
/// thousands of packets per second, prefer [`transport_pack_into_with_mac`]: it
/// reuses the output buffer and a cached [`MacContext`] (one `crc32c_append`
/// instead of ipad+data+opad).
///
/// `#[inline]` is required here because this is the library's single send path
/// and is called from `moonproto::client` across the crate boundary. The body is
/// not tiny, but inlining lets LLVM optimize it together with the caller, whose
/// next step is typically sending the buffer to a socket. This improves register
/// allocation. Audit B-V2-04. Do not remove.
#[inline]
pub fn transport_pack(
    mac_key: &MoonKey,
    cmd: u8,
    client_id: u64,
    payload: &[u8],
    mask_ver: u8,
) -> (Vec<u8>, Option<Vec<u8>>) {
    let mut buf = Vec::with_capacity(header::CLIENT_HDR_SIZE + payload.len());
    let mac_ctx = MacContext::new(mac_key);
    let mut mode_state = TransportModeState::new();
    let extra = transport_pack_into_with_mac_and_state(
        &mut buf,
        &mac_ctx,
        mac_key,
        cmd,
        client_id,
        payload,
        mask_ver,
        &mut mode_state,
    );
    (buf, extra)
}

/// Zero-alloc pack: writes a wire-ready UDP datagram into the provided `buf`.
/// The caller can reuse one `Vec<u8>` across sends, avoiding allocator churn.
/// Uses a cached [`MacContext`] instead of recomputing the HMAC ipad/opad state
/// for every packet.
///
/// `mac_key` is still required for `outer_light_crypt`; the xoshiro128+
/// keystream is initialized directly from the 16-byte key, not from
/// `MacContext`.
///
/// **Contract**: this function calls `buf.clear()` internally and overwrites the
/// contents. Capacity is preserved, which is where the allocation saving comes
/// from.
///
/// Returns an optional extra packet for `mask_ver` 1/2 when the selected
/// transport mode requires one. For long-lived V2 sessions prefer
/// [`transport_pack_into_with_mac_and_state`] so the per-client warmup counter
/// matches Delphi `SentCountDNS`.
#[inline]
pub fn transport_pack_into_with_mac(
    buf: &mut Vec<u8>,
    mac_ctx: &MacContext,
    mac_key: &MoonKey,
    cmd: u8,
    client_id: u64,
    payload: &[u8],
    mask_ver: u8,
) -> Option<Vec<u8>> {
    let mut mode_state = TransportModeState::new();
    transport_pack_into_with_mac_and_state(
        buf,
        mac_ctx,
        mac_key,
        cmd,
        client_id,
        payload,
        mask_ver,
        &mut mode_state,
    )
}

/// Stateful zero-alloc pack used by `Client`.
#[inline]
pub fn transport_pack_into_with_mac_and_state(
    buf: &mut Vec<u8>,
    mac_ctx: &MacContext,
    mac_key: &MoonKey,
    cmd: u8,
    client_id: u64,
    payload: &[u8],
    mask_ver: u8,
    mode_state: &mut TransportModeState,
) -> Option<Vec<u8>> {
    let hdr = ClientMsgHeader::new(cmd, client_id);

    buf.clear();
    buf.reserve(header::CLIENT_HDR_SIZE + payload.len());
    buf.extend_from_slice(&hdr.to_bytes());
    buf.extend_from_slice(payload);

    // MAC: cached context (cached ipad CRC + opad block), with no per-packet recomputation.
    // Checksum bytes are already zeroed by hdr.to_bytes(), so no extra clearing is needed.
    let mac = mac_ctx.mac(buf);
    buf[1..5].copy_from_slice(&mac.to_le_bytes());

    // Obfuscation (always, all modes)
    outer_light_crypt(buf, mac_key);

    extended::wrap_outgoing_client(buf, normalized_transport_mode(mask_ver), mode_state)
}

/// Unpack a received UDP datagram. Verifies MAC and version.
/// Returns (header, payload) or None if invalid.
///
/// `#[inline]` is required because this is the receive path for all incoming UDP
/// packets and a hot path (~10K pps at peak). It is called from
/// `moonproto::client::run` across the crate boundary; without inlining LLVM
/// cannot optimize it together with the caller. The body is medium-sized, but the
/// alternative (`lto = "fat"`) makes development builds much slower. Do not
/// remove.
///
/// B-V2-02 fix: this used to allocate twice: `raw.to_vec()` plus
/// `buf[SERVER_HDR_SIZE..].to_vec()`. It now performs one allocation for
/// `mask_ver = 0`; the payload is produced with `drain(..hdr_size)`, which moves
/// bytes inside the already allocated `Vec` without a second allocation. At
/// 10K pps this saves 10K alloc/dealloc pairs per second and about 5-15 MB/s of
/// allocator pressure.
#[inline]
pub fn transport_unpack(
    mac_key: &MoonKey,
    raw: &[u8],
    mask_ver: u8,
) -> Option<(ServerMsgHeader, Vec<u8>)> {
    let mac_ctx = MacContext::new(mac_key);
    transport_unpack_with_mac(&mac_ctx, mac_key, raw, mask_ver)
}

/// Hot-path unpack with a cached [`MacContext`]: one `crc32c_append(cached, data)`
/// instead of recomputing ipad+data+opad from scratch for each packet.
///
/// `mac_key` is still required for `outer_light_crypt` (xoshiro128+ keystream).
#[inline]
pub fn transport_unpack_with_mac(
    mac_ctx: &MacContext,
    mac_key: &MoonKey,
    raw: &[u8],
    mask_ver: u8,
) -> Option<(ServerMsgHeader, Vec<u8>)> {
    let mut buf: Vec<u8> =
        extended::unwrap_incoming_client(raw, normalized_transport_mode(mask_ver))?;

    if buf.len() < header::SERVER_HDR_SIZE {
        return None;
    }

    // De-obfuscate (always, all modes)
    outer_light_crypt(&mut buf, mac_key);

    // Parse header
    let hdr = ServerMsgHeader::from_bytes(&buf[..header::SERVER_HDR_SIZE])?;

    // Verify MAC through the cached context. Restore checksum bytes after the
    // calculation (B-01 mini-fix: one allocation instead of two).
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

    // B-V2-02: draining the header moves the remaining bytes to the front without a second allocation.
    buf.drain(..header::SERVER_HDR_SIZE);
    Some((hdr, buf))
}

#[inline]
fn normalized_transport_mode(mask_ver: u8) -> u8 {
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
        outer_light_crypt(&mut packet, mac_key);
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
    fn mode1_stun_roundtrip_unpacks_packet() {
        let mac_key = [7u8; 16];
        let payload = b"mode1-stun-payload";
        let mut packet = build_server_packet(&mac_key, 42, payload);
        extended::wrap_outgoing_client(&mut packet, 1, &mut TransportModeState::new());

        let (hdr, decoded) = transport_unpack(&mac_key, &packet, 1).expect("mode1 unpack");

        assert_eq!(hdr.cmd, 42);
        assert_eq!(decoded, payload);
    }

    #[test]
    fn mode2_dns_warmup_is_ignored_and_normal_packet_unpacks() {
        let mac_key = [7u8; 16];
        let payload = b"mode2-payload";
        let mut state = TransportModeState::new();
        let extra = {
            let mut buf = Vec::new();
            let mac_ctx = MacContext::new(&mac_key);
            transport_pack_into_with_mac_and_state(
                &mut buf, &mac_ctx, &mac_key, 44, 123, payload, 2, &mut state,
            )
        };
        let packet = build_server_packet(&mac_key, 44, payload);

        assert!(transport_unpack(&mac_key, extra.as_deref().unwrap(), 2).is_none());
        let (hdr, decoded) = transport_unpack(&mac_key, &packet, 2).expect("mode2 packet");
        assert_eq!(hdr.cmd, 44);
        assert_eq!(decoded, payload);
    }
}
