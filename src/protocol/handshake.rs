use crate::MoonKey;
use crate::crypto;
use rand::Rng;

/// TMoonProtoHello — 56 bytes packed record
/// Layout: Rnd(16) + MixTS(8) + TimeStamp(8) + ServerToken(8) + PeerMix(8) + AppToken(8)
pub const HELLO_SIZE: usize = 56;

#[derive(Debug, Clone)]
pub struct Hello {
    pub rnd: [u8; 16],
    pub mix_ts: u64,
    pub timestamp: f64, // TDateTime = Double
    pub server_token: u64,
    pub peer_mix: u64,
    pub app_token: u64,
}

impl Hello {
    pub fn new(client_token: u64, app_token: u64) -> Self {
        let mut rng = rand::thread_rng();
        let mut rnd = [0u8; 16];
        rng.fill(&mut rnd);

        Self {
            rnd,
            mix_ts: client_token,
            timestamp: delphi_now(),
            server_token: rng.gen(),
            peer_mix: rng.gen(),
            app_token,
        }
    }

    /// Pack: TimeStamp XOR'd with MixTS (as u64 overlay on f64 bits)
    pub fn to_bytes_packed(&self) -> [u8; HELLO_SIZE] {
        let mut buf = [0u8; HELLO_SIZE];
        buf[0..16].copy_from_slice(&self.rnd);
        buf[16..24].copy_from_slice(&self.mix_ts.to_le_bytes());

        // Pack timestamp: raw f64 bits XOR'd with MixTS
        let ts_bits = self.timestamp.to_bits();
        let packed_ts = ts_bits.wrapping_add(self.mix_ts);
        buf[24..32].copy_from_slice(&packed_ts.to_le_bytes());

        buf[32..40].copy_from_slice(&self.server_token.to_le_bytes());
        buf[40..48].copy_from_slice(&self.peer_mix.to_le_bytes());
        buf[48..56].copy_from_slice(&self.app_token.to_le_bytes());
        buf
    }

    /// Unpack from bytes (reverses timestamp packing)
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < HELLO_SIZE {
            return None;
        }
        let mut rnd = [0u8; 16];
        rnd.copy_from_slice(&data[0..16]);
        let mix_ts = u64::from_le_bytes(data[16..24].try_into().unwrap());
        let packed_ts = u64::from_le_bytes(data[24..32].try_into().unwrap());
        let ts_bits = packed_ts.wrapping_sub(mix_ts);
        let timestamp = f64::from_bits(ts_bits);
        let server_token = u64::from_le_bytes(data[32..40].try_into().unwrap());
        let peer_mix = u64::from_le_bytes(data[40..48].try_into().unwrap());
        let app_token = u64::from_le_bytes(data[48..56].try_into().unwrap());

        Some(Self { rnd, mix_ts, timestamp, server_token, peer_mix, app_token })
    }
}

/// Build the Hello packet ready to send (encrypted with MasterKey).
/// NOTE: AAD is NOT actually used due to mORMot's AesGcmReset clearing AAD state
/// before Encrypt. Both client and server have this behavior, so they're consistent.
pub fn build_hello_packet(master_key: &MoonKey, client_id: u64, client_token: &mut u64, app_token: u64) -> Vec<u8> {
    *client_token += 1;
    let mut hello = Hello::new(*client_token, app_token);
    hello.timestamp = delphi_now();
    let packed = hello.to_bytes_packed();
    let aad = client_id.to_le_bytes();
    crypto::encrypt(master_key, &packed, &aad)
}

/// Build HelloAgain packet (encrypted with SESSION key, includes PeerMix proof)
pub fn build_hello_again_packet(
    encode_key: &MoonKey,
    master_key_as_rnd: &MoonKey,
    client_id: u64,
    client_token: &mut u64,
    server_token: u64,
    app_token: u64,
) -> Vec<u8> {
    *client_token += 1;
    let mut hello = Hello::new(*client_token, app_token);
    hello.timestamp = delphi_now();
    // PeerMix = MixValues(Hello.Rnd, MixTS, ServerToken)
    hello.peer_mix = crypto::mix_values(master_key_as_rnd, hello.mix_ts, server_token);
    let packed = hello.to_bytes_packed();
    crypto::encrypt(encode_key, &packed, &client_id.to_le_bytes())
}

/// Delphi TDateTime: days since 1899-12-30 as f64.
/// We approximate with UTC now.
fn delphi_now() -> f64 {
    use std::time::SystemTime;
    let secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    // Unix epoch = 1970-01-01 = Delphi day 25569
    25569.0 + secs / 86400.0
}
