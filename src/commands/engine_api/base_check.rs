//! `emk_BaseCheck` response parser.

use super::read_string;
use crate::commands::market::{BaseCurrency, ExchangeCode};
use std::ops::{BitOr, BitOrAssign};

/// Bitmask for exchange capabilities returned by BaseCheck.
///
/// Several bits may be set at once, for example Spot + Futures when the server
/// exposes both trading modes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct ExchangeTypeMask(u8);

impl ExchangeTypeMask {
    /// Spot trading is available.
    pub const SPOT: Self = Self(0x01);
    /// Futures trading is available.
    pub const FUTURES: Self = Self(0x02);
    /// The server works with a DEX backend such as Hyperliquid.
    pub const DEX: Self = Self(0x04);
    /// Prediction / outcome markets.
    pub const PREDICT: Self = Self(0x08);

    pub const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    pub const fn to_byte(self) -> u8 {
        self.0
    }

    pub const fn contains(self, flag: Self) -> bool {
        (self.0 & flag.0) != 0
    }
}

impl BitOr for ExchangeTypeMask {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl BitOrAssign for ExchangeTypeMask {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

impl std::fmt::Debug for ExchangeTypeMask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ExchangeTypeMask(0x{:02X})", self.0)
    }
}

/// Named flags for `ServerInfo::exchange_type_mask`.
pub(crate) mod exchange_type_flags {
    use super::ExchangeTypeMask;

    /// Spot trading is available.
    #[allow(dead_code)]
    pub(crate) const SPOT: ExchangeTypeMask = ExchangeTypeMask::SPOT;
    /// Futures trading is available.
    #[allow(dead_code)]
    pub(crate) const FUTURES: ExchangeTypeMask = ExchangeTypeMask::FUTURES;
    /// The server works with a DEX backend such as Hyperliquid.
    #[allow(dead_code)]
    pub(crate) const DEX: ExchangeTypeMask = ExchangeTypeMask::DEX;
    /// Prediction / outcome markets.
    #[allow(dead_code)]
    pub(crate) const PREDICT: ExchangeTypeMask = ExchangeTypeMask::PREDICT;
}

/// Server identity returned by `EngineMethod::BaseCheck`.
///
/// `BaseCheck` is the first Engine API request in Init, so it carries server
/// identity used by multi-server applications: bot id, exchange name, base
/// currency, and protocol/version fields. Per-account fields are returned by
/// `AuthCheck` instead.
///
/// Every field is optional for forward compatibility. Older servers may return
/// an empty response; newer servers can append fields while older clients keep
/// already-read fields and leave the rest as `None`.
///
/// Wire payload order:
/// 1. `bot_id` — i64 LE (`cfg.UniqueBotID`);
/// 2. `server_name` — u16-length UTF-8 string;
/// 3. `exchange_code` — Delphi `TBotPlatform` ordinal;
/// 4. `exchange_name` — UI name such as "Binance Futures";
/// 5. `exchange_type_mask` — bitmask, see `exchange_type_flags`;
/// 6. `dex_name` — HIP-3 DEX name for Hyperliquid futures, otherwise empty;
/// 7. `base_currency_name` — "USDT", "BTC", etc.;
/// 8. `base_currency_code` — Delphi `TBaseCurrency` ordinal;
/// 9. `server_version` — MoonBot version number;
/// 10. `moonproto_version` — MoonProto protocol version.
///
/// Source: `MoonProtoEngineServer.pas:244-273`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServerInfo {
    /// Stable 64-bit server id (`cfg.UniqueBotID`), if the server sends it.
    pub bot_id: Option<i64>,
    /// Human-readable server name for UI.
    pub server_name: Option<String>,
    /// Delphi `TBotPlatform` ordinal.
    pub exchange_code: Option<ExchangeCode>,
    /// Human-readable exchange name.
    pub exchange_name: Option<String>,
    /// Available exchange capabilities. See `exchange_type_flags`.
    pub exchange_type_mask: Option<ExchangeTypeMask>,
    /// HIP-3 DEX name for Hyperliquid futures. Other exchanges usually send an
    /// empty string.
    pub dex_name: Option<String>,
    /// Base currency name such as "USDT", "USD", or "BTC".
    pub base_currency_name: Option<String>,
    /// Delphi `TBaseCurrency` ordinal.
    pub base_currency_code: Option<BaseCurrency>,
    /// MoonBot version number.
    pub server_version: Option<u32>,
    /// MoonProto protocol version.
    pub moonproto_version: Option<u32>,
}

impl ServerInfo {
    /// `true` when the server reported at least its stable identity id.
    pub fn has_identity(&self) -> bool {
        self.bot_id.is_some()
    }

    /// Check one `exchange_type_mask` bit. Missing masks return `false`.
    pub fn supports(&self, flag: ExchangeTypeMask) -> bool {
        match self.exchange_type_mask {
            Some(mask) => mask.contains(flag),
            None => false,
        }
    }
}

/// Parse `EngineResponse.data` for `EngineMethod::BaseCheck`.
///
/// Empty payload is valid for old servers and returns `ServerInfo::default()`.
/// Truncated optional tails keep fields parsed before the truncation and leave
/// the rest as `None`, matching Delphi's `if not EOF` optional-field pattern.
pub(crate) fn parse_base_check_response(data: &[u8]) -> ServerInfo {
    let mut info = ServerInfo::default();
    let mut pos = 0usize;

    // 1. bot_id (i64 LE) — Delphi WriteInt64(cfg.UniqueBotID)
    if pos + 8 > data.len() {
        return info;
    }
    info.bot_id = Some(i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()));
    pos += 8;

    // 2. server_name (string)
    match read_string(data, &mut pos) {
        Some(s) => info.server_name = Some(s),
        None => return info,
    }

    // 3. exchange_code (u8) — Ord(cfg.Header.Current)
    if pos + 1 > data.len() {
        return info;
    }
    info.exchange_code = Some(ExchangeCode::from_byte(data[pos]));
    pos += 1;

    // 4. exchange_name (string)
    match read_string(data, &mut pos) {
        Some(s) => info.exchange_name = Some(s),
        None => return info,
    }

    // 5. exchange_type_mask (u8)
    if pos + 1 > data.len() {
        return info;
    }
    info.exchange_type_mask = Some(ExchangeTypeMask::from_byte(data[pos]));
    pos += 1;

    // 6. dex_name (string)
    match read_string(data, &mut pos) {
        Some(s) => info.dex_name = Some(s),
        None => return info,
    }

    // 7. base_currency_name (string)
    match read_string(data, &mut pos) {
        Some(s) => info.base_currency_name = Some(s),
        None => return info,
    }

    // 8. base_currency_code (u8) — Ord(cfg.BaseCurrency)
    if pos + 1 > data.len() {
        return info;
    }
    info.base_currency_code = Some(BaseCurrency::from_byte(data[pos]));
    pos += 1;

    // 9. server_version (i32 LE → u32) — Delphi WriteInt(Current_Version_Num_X).
    // Trust the server: version values grow monotonically from small positive
    // numbers (for example 763 for v7.63), so the signed/unsigned difference does not matter.
    if pos + 4 > data.len() {
        return info;
    }
    info.server_version = Some(i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as u32);
    pos += 4;

    // 10. moonproto_version (i32 LE → u32)
    if pos + 4 > data.len() {
        return info;
    }
    info.moonproto_version =
        Some(i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as u32);
    // pos += 4;  // no longer used

    info
}
