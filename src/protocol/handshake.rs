use rand::Rng;

/// Handshake AEAD associated data: `{client_id: u64 LE, cmd: u8}` packed = 9
/// bytes, matching Delphi `THandShakeAAD` / `HandShakeAAD`
/// (`MoonProtoDataStruct.pas:115-141`). Binding the command into the AAD means a
/// relabel of the header command (e.g. WhoAreYou -> Fine) breaks the GCM tag, so
/// Decode fails. Every handshake encode/decode must use the same `cmd`.
pub(crate) fn handshake_aad(client_id: u64, cmd: u8) -> [u8; 9] {
    let mut aad = [0u8; 9];
    aad[..8].copy_from_slice(&client_id.to_le_bytes());
    aad[8] = cmd;
    aad
}
use zerocopy::byteorder::little_endian::U64 as LeU64;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

// Hello timestamp is set by caller (client.rs::delphi_now adds NTP offset).

/// TMoonProtoHello — 56 bytes packed record
/// Layout: Rnd(16) + MixTS(8) + TimeStamp(8) + ServerToken(8) + PeerMix(8) + AppToken(8)
pub(crate) const HELLO_SIZE: usize = std::mem::size_of::<WireHello>();
const _: [(); 56] = [(); HELLO_SIZE];

#[derive(Debug, Clone)]
pub(crate) struct Hello {
    pub(crate) rnd: [u8; 16],
    pub(crate) mix_ts: u64,
    pub(crate) timestamp: f64, // TDateTime = Double
    pub(crate) server_token: u64,
    pub(crate) peer_mix: u64,
    pub(crate) app_token: u64,
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
struct WireHello {
    rnd: [u8; 16],
    mix_ts: LeU64,
    timestamp_packed: LeU64,
    server_token: LeU64,
    peer_mix: LeU64,
    app_token: LeU64,
}

impl Hello {
    pub(crate) fn new(client_token: u64, app_token: u64) -> Self {
        let mut rng = rand::thread_rng();
        let mut rnd = [0u8; 16];
        rng.fill(&mut rnd);

        Self {
            rnd,
            mix_ts: client_token,
            timestamp: 0.0, // caller MUST overwrite with delphi_now() (NTP-corrected)
            server_token: rng.gen(),
            peer_mix: rng.gen(),
            app_token,
        }
    }

    fn to_wire(&self) -> WireHello {
        let ts_bits = self.timestamp.to_bits();
        WireHello {
            rnd: self.rnd,
            mix_ts: LeU64::new(self.mix_ts),
            timestamp_packed: LeU64::new(ts_bits.wrapping_add(self.mix_ts)),
            server_token: LeU64::new(self.server_token),
            peer_mix: LeU64::new(self.peer_mix),
            app_token: LeU64::new(self.app_token),
        }
    }

    fn from_wire(wire: WireHello) -> Self {
        let mix_ts = wire.mix_ts.get();
        let ts_bits = wire.timestamp_packed.get().wrapping_sub(mix_ts);
        Self {
            rnd: wire.rnd,
            mix_ts,
            timestamp: f64::from_bits(ts_bits),
            server_token: wire.server_token.get(),
            peer_mix: wire.peer_mix.get(),
            app_token: wire.app_token.get(),
        }
    }

    /// Pack: TimeStamp bits plus MixTS, matching `TMoonProtoHello.pack`.
    pub(crate) fn to_bytes_packed(&self) -> [u8; HELLO_SIZE] {
        let mut buf = [0u8; HELLO_SIZE];
        buf.copy_from_slice(self.to_wire().as_bytes());
        buf
    }

    /// Unpack from bytes (reverses timestamp packing).
    pub(crate) fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < HELLO_SIZE {
            return None;
        }
        Some(Self::from_wire(
            WireHello::read_from_bytes(&data[..HELLO_SIZE]).ok()?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_uses_private_wire_struct() {
        assert_eq!(std::mem::size_of::<WireHello>(), 56);
        assert_eq!(HELLO_SIZE, 56);

        let hello = Hello {
            rnd: [0xAB; 16],
            mix_ts: 0x0102_0304_0506_0708,
            timestamp: 45_000.25,
            server_token: 0x1112_1314_1516_1718,
            peer_mix: 0x2122_2324_2526_2728,
            app_token: 0x3132_3334_3536_3738,
        };
        let bytes = hello.to_bytes_packed();

        assert_eq!(&bytes[0..16], &[0xAB; 16]);
        assert_eq!(&bytes[16..24], &0x0102_0304_0506_0708u64.to_le_bytes());
        assert_eq!(
            &bytes[24..32],
            &hello
                .timestamp
                .to_bits()
                .wrapping_add(hello.mix_ts)
                .to_le_bytes()
        );

        let parsed = Hello::from_bytes(&bytes).expect("valid TMoonProtoHello");
        assert_eq!(parsed.rnd, hello.rnd);
        assert_eq!(parsed.mix_ts, hello.mix_ts);
        assert_eq!(parsed.timestamp.to_bits(), hello.timestamp.to_bits());
        assert_eq!(parsed.server_token, hello.server_token);
        assert_eq!(parsed.peer_mix, hello.peer_mix);
        assert_eq!(parsed.app_token, hello.app_token);
    }
}
