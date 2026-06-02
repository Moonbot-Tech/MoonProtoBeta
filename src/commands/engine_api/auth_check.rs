//! `emk_AuthCheck` response parser.

use super::{read_i32_zero_tail, read_string};
use crate::commands::registry::decode_utf8_delphi;
use zerocopy::byteorder::little_endian::U16 as LeU16;
use zerocopy::{FromBytes, Immutable, KnownLayout, Unaligned};

/// Hyperliquid DEX info.
///
/// Wire layout matches Delphi `THLDexInfo`: `Name: string[15]` as a Pascal
/// short string (1 length byte + 15 data bytes) followed by a little-endian
/// `CollateralToken: Word`. The packed size is 18 bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DexInfo {
    /// DEX name. Empty string means the default USDC validator.
    pub name: String,
    /// Collateral token id. Known values include `0` = USDC, `360` = USDH,
    /// `235` = USDE, and `268` = USDT0.
    pub collateral_token: u16,
}

#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C, packed)]
struct WireDexInfo {
    short_string_name: [u8; 16],
    collateral_token: LeU16,
}

const DEX_INFO_SIZE: usize = std::mem::size_of::<WireDexInfo>();
const _: [(); 18] = [(); DEX_INFO_SIZE];

impl From<WireDexInfo> for DexInfo {
    fn from(wire: WireDexInfo) -> Self {
        let name_len = wire.short_string_name[0] as usize;
        // Guard: name_len is <= 15 by contract. If larger it is corrupt, so use 15.
        let effective_len = name_len.min(15);
        let name_bytes = &wire.short_string_name[1..1 + effective_len];
        Self {
            name: decode_utf8_delphi(name_bytes),
            collateral_token: wire.collateral_token.get(),
        }
    }
}

/// Index into `AuthCheckResponse::known_dexes`.
///
/// Delphi stores current Hyperliquid futures/spot DEX selection as a byte
/// index (`cfg.HLDexMarket` / `cfg.HLSpotDexMarket`). The wrapper keeps the
/// wire value exact while public code does not have to pass around a naked
/// magic `u8`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct HyperDexIndex(u8);

impl HyperDexIndex {
    pub const DEFAULT: Self = Self(0);

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub const fn to_byte(self) -> u8 {
        self.0
    }

    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) const fn to_byte(self) -> u8 {
        self.0
    }

    pub const fn as_usize(self) -> usize {
        self.0 as usize
    }
}

/// Decoded `EngineMethod::AuthCheck` response.
///
/// This is per-account data: exchange account id, wallet/address strings,
/// sub-account flag, and optional Hyperliquid DEX metadata. Older servers may
/// omit the optional tail fields.
///
/// Source: `MoonProtoEngine.pas:605-639`.
#[derive(Debug, Clone, PartialEq)]
pub struct AuthCheckResponse {
    /// Binance account id when the connected server uses Binance; otherwise 0.
    pub binance_account_id: i64,
    /// BTC address attached to the account.
    pub btc_address: String,
    /// Spot referral config. Usually zero.
    pub spot_ref: i32,
    /// `true` for sub-accounts.
    pub is_sub_account: bool,
    /// Account id string, for example a Hyperliquid wallet address.
    pub account_id: String,
    /// Server-advertised maximum payload size, if provided.
    pub recvd_max_payload: Option<i32>,
    /// Known Hyperliquid DEX entries for UI switching.
    pub known_dexes: Vec<DexInfo>,
    /// Current active Hyperliquid futures DEX index.
    pub hl_dex_market: Option<HyperDexIndex>,
    /// Current active Hyperliquid spot DEX index.
    pub hl_spot_market: Option<HyperDexIndex>,
}

/// Parse `EngineResponse.data` for `EngineMethod::AuthCheck`.
///
/// Returns `None` when the payload is corrupt or shorter than the mandatory
/// field prefix. Optional tail fields are parsed only while bytes remain; their
/// absence means an older server and still returns `Some` with mandatory data.
/// DEX tail follows Delphi's soft stream-read shape: the declared `cnt` is read,
/// `SetLength(KnownDexes, cnt)` creates zero-filled records, and each
/// `TMemoryStream.Read` partially overwrites one 18-byte `THLDexInfo` slot.
pub(crate) fn parse_auth_check_response(data: &[u8]) -> Option<AuthCheckResponse> {
    let mut pos = 0usize;

    // Required fields.
    if data.len() < 8 {
        return None;
    }
    let binance_account_id = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
    pos += 8;

    let btc_address = read_string(data, &mut pos)?;

    if pos + 4 > data.len() {
        return None;
    }
    let spot_ref = i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
    pos += 4;

    if pos + 1 > data.len() {
        return None;
    }
    let is_sub_account = data[pos] != 0;
    pos += 1;

    let account_id = read_string(data, &mut pos)?;

    // Optional Phase 2 extensions (read if !EOF).
    let recvd_max_payload = if pos < data.len() {
        Some(read_i32_zero_tail(data, &mut pos))
    } else {
        None
    };

    let mut known_dexes: Vec<DexInfo> = Vec::new();
    let mut hl_dex_market: Option<HyperDexIndex> = None;
    let mut hl_spot_market: Option<HyperDexIndex> = None;

    if recvd_max_payload.is_some() && pos < data.len() {
        let cnt = data[pos] as usize;
        pos += 1;
        known_dexes.reserve(cnt);
        for _ in 0..cnt {
            let mut dex = [0u8; DEX_INFO_SIZE];
            let available = data.len().saturating_sub(pos).min(DEX_INFO_SIZE);
            if available > 0 {
                dex[..available].copy_from_slice(&data[pos..pos + available]);
                pos += available;
            }
            // THLDexInfo packed: [u8 length][15 bytes name][u16 collateral_token]
            let wire = WireDexInfo::read_from_bytes(&dex).ok()?;
            known_dexes.push(wire.into());
        }
        // hl_dex_market and hl_spot_market follow immediately after the array.
        if pos < data.len() {
            hl_dex_market = Some(HyperDexIndex::from_byte(data[pos]));
            pos += 1;
            if pos < data.len() {
                hl_spot_market = Some(HyperDexIndex::from_byte(data[pos]));
                // pos += 1;  // no longer used
            }
        }
    }

    Some(AuthCheckResponse {
        binance_account_id,
        btc_address,
        spot_ref,
        is_sub_account,
        account_id,
        recvd_max_payload,
        known_dexes,
        hl_dex_market,
        hl_spot_market,
    })
}
