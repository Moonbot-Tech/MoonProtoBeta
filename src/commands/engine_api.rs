/// `MPC_API` Engine RPC helpers.
///
/// This module is a byte-exact port of Delphi `MoonProtoEngineStruct.pas`.
/// Source: MoonProtoEngineStruct.pas:364-403 (TEngineResponse.CreateFromStream)
///
/// Request: client → server (CmdId=002)
/// Response: server → client (CmdId=001)
use std::time::{Duration, SystemTime};

use super::registry::read_string;
#[cfg(any(test, feature = "diagnostics"))]
use crate::time::DelphiTime;
use crate::MoonTime;
mod auth_check;
mod base_check;
mod method;
mod response;

pub(crate) use self::auth_check::parse_auth_check_response;
pub use self::auth_check::{AuthCheckResponse, DexInfo, HyperDexIndex};
#[allow(unused_imports)]
pub(crate) use self::base_check::{exchange_type_flags, parse_base_check_response};
pub use self::base_check::{ExchangeTypeMask, ServerInfo};
pub use self::method::EngineMethod;
pub(crate) use self::response::parse_engine_response;
pub use self::response::EngineResponse;

#[cfg(test)]
const DELPHI_UNIX_EPOCH_DAYS: f64 = 25_569.0;
const SECONDS_PER_DAY: f64 = 86_400.0;

fn read_zero_tail<const N: usize>(data: &[u8], pos: &mut usize) -> [u8; N] {
    let mut out = [0u8; N];
    if *pos < data.len() {
        let n = (data.len() - *pos).min(N);
        out[..n].copy_from_slice(&data[*pos..*pos + n]);
        *pos += n;
    }
    out
}

fn read_u8_zero_tail(data: &[u8], pos: &mut usize) -> u8 {
    read_zero_tail::<1>(data, pos)[0]
}

fn read_i32_zero_tail(data: &[u8], pos: &mut usize) -> i32 {
    i32::from_le_bytes(read_zero_tail::<4>(data, pos))
}

fn read_u64_zero_tail(data: &[u8], pos: &mut usize) -> u64 {
    u64::from_le_bytes(read_zero_tail::<8>(data, pos))
}

/// Parse `EngineResponse.data` for `emk_QueryHedgeMode`
/// (`EngineMethod::QueryHedgeMode`).
///
/// The Delphi server writes one `Boolean` byte on success:
/// `MoonProtoEngineServer.pas:341-344` (`resp.WriteBool(hedgeMode)`). Extra
/// trailing bytes are ignored for forward compatibility.
pub(crate) fn parse_query_hedge_mode_response(data: &[u8]) -> Option<bool> {
    let mut pos = 0usize;
    Some(read_u8_zero_tail(data, &mut pos) != 0)
}

/// API-key expiration time returned by `emk_CheckAPIExpirationTime`.
///
/// The raw wire value is Delphi `TDateTime`: days since 1899-12-30 with a
/// fractional day part. A value of `0.0` means that the server did not report
/// an expiration time.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ApiExpirationTime {
    delphi_time: f64,
}

impl ApiExpirationTime {
    pub fn from_time(time: MoonTime) -> Self {
        Self {
            delphi_time: time.to_delphi_days(),
        }
    }

    /// Build from the raw Delphi `TDateTime` value.
    pub(crate) fn from_delphi_time(delphi_time: f64) -> Self {
        Self { delphi_time }
    }

    /// API expiration time when the server reported a known value.
    pub fn time(&self) -> Option<MoonTime> {
        self.is_known()
            .then(|| MoonTime::from_delphi_days(self.delphi_time))
            .flatten()
    }

    /// API expiration as a typed Delphi `TDateTime` helper.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn time_delphi(&self) -> DelphiTime {
        DelphiTime::from_days(self.delphi_time)
    }

    /// Raw Delphi `TDateTime` value retained for exact diagnostics.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn delphi_time(&self) -> f64 {
        self.delphi_time
    }

    /// Returns false when the server reported no known expiration time.
    pub fn is_known(&self) -> bool {
        self.delphi_time.is_finite() && self.delphi_time > 0.0
    }

    /// Convert to whole Unix seconds when the value is known and representable
    /// by `SystemTime` on the Unix side of the epoch.
    pub fn unix_seconds(&self) -> Option<i64> {
        let seconds = self.time()?.unix_seconds().round();
        (seconds.is_finite() && seconds >= 0.0 && seconds <= i64::MAX as f64)
            .then_some(seconds as i64)
    }

    /// Convert to `SystemTime` when the value is known and not before the Unix epoch.
    pub fn system_time(&self) -> Option<SystemTime> {
        let seconds = self.unix_seconds()?;
        SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(seconds as u64))
    }

    /// Rounded signed number of days until expiration relative to `now`.
    pub fn days_until(&self, now: SystemTime) -> Option<i64> {
        let expiration = self.system_time()?;
        match expiration.duration_since(now) {
            Ok(duration) => Some((duration.as_secs_f64() / SECONDS_PER_DAY).round() as i64),
            Err(err) => {
                let days = (err.duration().as_secs_f64() / SECONDS_PER_DAY).round() as i64;
                Some(-days)
            }
        }
    }
}

/// Parse `EngineResponse.data` for `emk_CheckAPIExpirationTime`
/// (`EngineMethod::CheckAPIExpirationTime`).
///
/// The Delphi server writes exactly one little-endian `Double` on success:
/// `MoonProtoEngineServer.pas → TEngineWorker.ProcessRequest` branch
/// `emk_CheckAPIExpirationTime` (`resp.WriteDouble(ExpTime)`). Extra trailing
/// bytes are ignored so newer servers can append fields without breaking old
/// consumers.
pub(crate) fn parse_api_expiration_time_response(data: &[u8]) -> Option<ApiExpirationTime> {
    let mut pos = 0usize;
    Some(ApiExpirationTime::from_delphi_time(f64::from_le_bytes(
        read_zero_tail::<8>(data, &mut pos),
    )))
}

/// One transferable asset row returned by `emk_UpdateTransferAssets`.
///
/// Delphi source:
/// `MoonProtoEngineServer.pas` writes `Currency`, `Ammount`, and `Total` from
/// `TAssetItem`; `MoonProtoEngine.pas` reads the same fields back into
/// `Markets.FAssets[EKind]`.
#[derive(Debug, Clone, PartialEq)]
pub struct TransferAsset {
    /// Asset symbol, for example `"USDT"` or `"BTC"`.
    pub currency: String,
    /// Transferable amount reported by the exchange.
    ///
    /// The field name in Delphi is `Ammount`; Rust exposes the corrected
    /// spelling while preserving the wire meaning.
    pub amount: f64,
    /// Total balance reported for this transfer asset.
    pub total: f64,
}

/// Parse `EngineResponse.data` for `emk_UpdateTransferAssets`
/// (`EngineMethod::UpdateTransferAssets`).
///
/// Wire format:
///
/// ```text
/// count: i32 LE
/// items[count]:
///   currency: string (u16 length + UTF-8)
///   amount:   f64 LE
///   total:    f64 LE
/// ```
pub(crate) fn parse_update_transfer_assets_response(data: &[u8]) -> Option<Vec<TransferAsset>> {
    let mut pos = 0usize;
    let count_raw = read_i32_zero_tail(data, &mut pos);
    if count_raw <= 0 {
        return Some(Vec::new());
    }

    let count = count_raw as usize;
    let mut assets = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        let currency = read_string(data, &mut pos)?;
        let amount = f64::from_le_bytes(read_zero_tail::<8>(data, &mut pos));
        let total = f64::from_le_bytes(read_zero_tail::<8>(data, &mut pos));
        assets.push(TransferAsset {
            currency,
            amount,
            total,
        });
    }
    Some(assets)
}

#[cfg(test)]
mod tests;
