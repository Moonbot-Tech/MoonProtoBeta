use zerocopy::byteorder::little_endian::{U32 as LeU32, U64 as LeU64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

/// Current MoonProto transport header version.
pub(crate) const TRANSPORT_VER: u8 = 3;
/// Size in bytes of the server-to-client UDP transport header.
pub(crate) const SERVER_HDR_SIZE: usize = 7;
/// Size in bytes of the client-to-server UDP transport header.
pub(crate) const CLIENT_HDR_SIZE: usize = 15;

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C, packed)]
struct WireServerMsgHeader {
    rnd: u8,
    checksum: LeU32,
    ver: u8,
    cmd: u8,
}

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C, packed)]
struct WireClientMsgHeader {
    rnd: u8,
    checksum: LeU32,
    ver: u8,
    cmd: u8,
    client_id: LeU64,
}

const _: () = assert!(core::mem::size_of::<WireServerMsgHeader>() == SERVER_HDR_SIZE);
const _: () = assert!(core::mem::size_of::<WireClientMsgHeader>() == CLIENT_HDR_SIZE);

/// Server -> Client header (7 bytes)
#[derive(Debug, Clone, Copy)]
pub struct ServerMsgHeader {
    /// Random byte used as the seed for outer obfuscation.
    pub rnd: u8,
    /// HMAC-CRC32C checksum stored in little-endian order on the wire.
    pub checksum: u32,
    /// Transport version. Valid packets use [`TRANSPORT_VER`].
    pub ver: u8,
    /// MoonProto command byte.
    pub cmd: u8,
}

/// Client -> Server header (15 bytes)
#[derive(Debug, Clone, Copy)]
pub(crate) struct ClientMsgHeader {
    /// Random byte used as the seed for outer obfuscation.
    pub(crate) rnd: u8,
    /// HMAC-CRC32C checksum stored in little-endian order on the wire.
    pub(crate) checksum: u32,
    /// Transport version. Valid packets use [`TRANSPORT_VER`].
    pub(crate) ver: u8,
    /// MoonProto command byte.
    pub(crate) cmd: u8,
    /// Client identifier carried in client-to-server packets.
    pub(crate) client_id: u64,
}

// The small header parsers/serializers are called per-packet across a cross-crate
// boundary. `#[inline]` is mandatory — without it LLVM won't inline cross-crate (it would
// need lto=fat, which breaks fast dev builds). The body is ≤ 7-15 bytes of copies, code-bloat
// when inlined into callers is minimal. Audit B-V2-04. Do NOT remove.

impl ServerMsgHeader {
    /// Parse a server-to-client header from the beginning of `data`.
    ///
    /// Returns `None` when `data` is shorter than `SERVER_HDR_SIZE`.
    #[inline]
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < SERVER_HDR_SIZE {
            return None;
        }
        let wire = WireServerMsgHeader::read_from_bytes(&data[..SERVER_HDR_SIZE]).ok()?;
        Some(Self {
            rnd: wire.rnd,
            checksum: wire.checksum.get(),
            ver: wire.ver,
            cmd: wire.cmd,
        })
    }

    /// Serialize this header to the exact 7-byte wire layout.
    #[inline]
    pub fn to_bytes(&self) -> [u8; SERVER_HDR_SIZE] {
        let wire = WireServerMsgHeader {
            rnd: self.rnd,
            checksum: LeU32::new(self.checksum),
            ver: self.ver,
            cmd: self.cmd,
        };
        let mut buf = [0u8; SERVER_HDR_SIZE];
        buf.copy_from_slice(wire.as_bytes());
        buf
    }
}

impl ClientMsgHeader {
    /// Create a client-to-server header for `cmd` and `client_id`.
    ///
    /// The checksum is initialized to zero; packing code fills it after the
    /// payload has been appended.
    #[inline]
    pub(crate) fn new(cmd: u8, client_id: u64) -> Self {
        Self {
            rnd: rand_byte(),
            checksum: 0,
            ver: TRANSPORT_VER,
            cmd,
            client_id,
        }
    }

    /// Serialize this header to the exact 15-byte wire layout.
    #[inline]
    pub(crate) fn to_bytes(&self) -> [u8; CLIENT_HDR_SIZE] {
        let wire = WireClientMsgHeader {
            rnd: self.rnd,
            checksum: LeU32::new(self.checksum),
            ver: self.ver,
            cmd: self.cmd,
            client_id: LeU64::new(self.client_id),
        };
        let mut buf = [0u8; CLIENT_HDR_SIZE];
        buf.copy_from_slice(wire.as_bytes());
        buf
    }

    /// Parse a client-to-server header from the beginning of `data`.
    ///
    /// Returns `None` when `data` is shorter than `CLIENT_HDR_SIZE`.
    // Only the transport/PMTU/service-command unit tests parse a client header;
    // production never reads its own outgoing header back. Kept for those tests.
    #[allow(dead_code)]
    #[inline]
    pub(crate) fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < CLIENT_HDR_SIZE {
            return None;
        }
        let wire = WireClientMsgHeader::read_from_bytes(&data[..CLIENT_HDR_SIZE]).ok()?;
        Some(Self {
            rnd: wire.rnd,
            checksum: wire.checksum.get(),
            ver: wire.ver,
            cmd: wire.cmd,
            client_id: wire.client_id.get(),
        })
    }
}

/// Obfuscation seed byte (`Rnd`) for the wire header.
///
/// Byte-exact with Delphi `MoonProtoCommon.pas:383,447 Rnd := Random(255)`:
/// Delphi `Random(255)` returns `0..254`, so the wire never carries `Rnd = 255`
/// from a Delphi client. The range is matched here so a passive observer profiling
/// the plaintext seed byte cannot distinguish this client from Delphi.
///
/// Source: `rand::thread_rng()` is rand 0.8 `ReseedingRng<ChaCha12Core, OsRng>`
/// (a ChaCha12 CSPRNG reseeded from the OS), not a weak PRNG. It replaces the old
/// `SystemTime::now()` seed, which cost an OS syscall (~50-300 ns) on the send hot
/// path and could repeat nanoseconds across back-to-back batch sends.
#[inline]
fn rand_byte() -> u8 {
    use rand::Rng;
    rand::thread_rng().gen_range(0u8..255)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_header_wire_layout_is_fixed() {
        let hdr = ServerMsgHeader {
            rnd: 0x11,
            checksum: 0x5544_3322,
            ver: 0x66,
            cmd: 0x77,
        };

        let bytes = hdr.to_bytes();
        assert_eq!(bytes, [0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77]);

        let parsed = ServerMsgHeader::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.rnd, hdr.rnd);
        assert_eq!(parsed.checksum, hdr.checksum);
        assert_eq!(parsed.ver, hdr.ver);
        assert_eq!(parsed.cmd, hdr.cmd);
        assert!(ServerMsgHeader::from_bytes(&bytes[..SERVER_HDR_SIZE - 1]).is_none());
    }

    #[test]
    fn client_header_wire_layout_is_fixed() {
        let hdr = ClientMsgHeader {
            rnd: 0x11,
            checksum: 0x5544_3322,
            ver: 0x66,
            cmd: 0x77,
            client_id: 0xffee_ddcc_bbaa_9988,
        };

        let bytes = hdr.to_bytes();
        assert_eq!(
            bytes,
            [
                0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
                0xff,
            ]
        );

        let parsed = ClientMsgHeader::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.rnd, hdr.rnd);
        assert_eq!(parsed.checksum, hdr.checksum);
        assert_eq!(parsed.ver, hdr.ver);
        assert_eq!(parsed.cmd, hdr.cmd);
        assert_eq!(parsed.client_id, hdr.client_id);
        assert!(ClientMsgHeader::from_bytes(&bytes[..CLIENT_HDR_SIZE - 1]).is_none());
    }
}
