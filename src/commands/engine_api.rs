/// `MPC_API` Engine RPC helpers.
///
/// This module is a byte-exact port of Delphi `MoonProtoEngineStruct.pas`.
/// Source: MoonProtoEngineStruct.pas:364-403 (TEngineResponse.CreateFromStream)
///
/// Request: client → server (CmdId=002)
/// Response: server → client (CmdId=001)
use std::time::{Duration, SystemTime};

#[cfg(test)]
use super::registry::write_string;
use super::registry::{decode_utf8_delphi, read_string};
use flate2::read::DeflateDecoder;

const DELPHI_UNIX_EPOCH_DAYS: f64 = 25_569.0;
const SECONDS_PER_DAY: f64 = 86_400.0;

/// Engine RPC method identifiers.
///
/// Each method has a corresponding builder in [`super::engine_request`] and a
/// `Client::api_*` wrapper. Most wrappers return an `mpsc::Receiver<EngineResponse>`
/// for asynchronous handling through the pending-response registry.
///
/// `EngineResponse::data` is method-specific. Method-specific parsers live next
/// to the related protocol module, for example `commands::market`,
/// `commands::candles`, or this module for small scalar responses.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct EngineMethod(pub u8);

#[allow(non_upper_case_globals)]
impl EngineMethod {
    /// Empty method (`emk_None`).
    pub const None: Self = Self(0);
    /// `BaseCheck`: engine health and server-identity check.
    pub const BaseCheck: Self = Self(1);
    /// `AuthCheck`: exchange API-key authorization check.
    pub const AuthCheck: Self = Self(2);
    /// `GetMarketsList`: full list of tradable markets.
    ///
    /// The response contains market records parsed by
    /// [`crate::commands::market::parse_markets_list_response`].
    pub const GetMarketsList: Self = Self(3);
    /// `UpdateMarketsList`: refresh market prices, funding, mark price, and
    /// correlation prices.
    pub const UpdateMarketsList: Self = Self(4);
    /// `GetMarketsIndexes`: compact server `mIndex -> market name` mapping used
    /// by indexed streams.
    pub const GetMarketsIndexes: Self = Self(5);
    /// `GetBalance`: current quantity for one currency. Parse with
    /// [`parse_get_balance_response`].
    pub const GetBalance: Self = Self(6);
    /// `GetMarketsBalanceFull`: server-side full balance refresh.
    ///
    /// Current Delphi `MoonProtoEngineServer.pas → ProcessRequest` calls
    /// `Engine.GetMarketsBalanceFull`, but the response writer is still a TODO
    /// (`WriteBalancesToStream` is not implemented), so a successful response has
    /// an empty `data` payload.
    pub const GetMarketsBalanceFull: Self = Self(7);
    /// `GetOrder` — enum value exists in `TEngineMethodKind`.
    ///
    /// The current Delphi reference server has no `ProcessRequest` branch for this
    /// method, so it returns `Unknown method` (error 400). Raw wrapper is kept for
    /// protocol completeness / future server versions.
    pub const GetOrder: Self = Self(8);
    /// `GetOpenOrders` — enum value exists in `TEngineMethodKind`.
    ///
    /// The current Delphi reference server has no request-handler branch for this
    /// method and returns `Unknown method` (error 400).
    pub const GetOpenOrders: Self = Self(9);
    /// `GetActiveOrders` — enum value exists in `TEngineMethodKind`.
    ///
    /// The current Delphi reference server has no request-handler branch for this
    /// method and returns `Unknown method` (error 400).
    pub const GetActiveOrders: Self = Self(10);
    /// `CancelAllOrders`: cancel all open orders.
    pub const CancelAllOrders: Self = Self(11);
    /// `SetLeverage`: set leverage for one market.
    pub const SetLeverage: Self = Self(12);
    /// `SetHedgeMode`: enable or disable hedge mode.
    pub const SetHedgeMode: Self = Self(13);
    /// `QueryHedgeMode`: current hedge-mode flag. Parse with
    /// [`parse_query_hedge_mode_response`].
    pub const QueryHedgeMode: Self = Self(14);
    /// `CheckAPIExpirationTime`: exchange API-key expiration as a Delphi
    /// `TDateTime`, parsed by [`parse_api_expiration_time_response`].
    pub const CheckAPIExpirationTime: Self = Self(15);
    /// `CheckBinanceTags`: Binance token permission tags.
    pub const CheckBinanceTags: Self = Self(16);
    /// `TradesResend`: request resend for missing TradesStream packet numbers.
    pub const TradesResend: Self = Self(17);
    /// `SubscribeAllTrades`: subscribe to the all-trades stream.
    pub const SubscribeAllTrades: Self = Self(18);
    /// `UnsubscribeAllTrades`: unsubscribe from the all-trades stream.
    pub const UnsubscribeAllTrades: Self = Self(19);
    /// `SubscribeOrderBook`: subscribe to orderbooks for market names.
    pub const SubscribeOrderBook: Self = Self(20);
    /// `UnsubscribeOrderBook`: unsubscribe from orderbooks for market names.
    pub const UnsubscribeOrderBook: Self = Self(21);
    /// `RequestOrderBookFull`: request a full snapshot for one indexed orderbook.
    pub const RequestOrderBookFull: Self = Self(22);
    /// `ReloadOrderBook`: force reload of subscribed orderbooks.
    pub const ReloadOrderBook: Self = Self(23);
    /// `RequestCandlesData`: request full historical candle data.
    ///
    /// The response is chunked: multiple `EngineResponse` packets share one UID.
    /// Prefer `Client::request_candles_data` or `Client::api_request_candles_data_async`.
    pub const RequestCandlesData: Self = Self(24);
    /// `ChangePositionType`: change isolated/cross position type for a market.
    pub const ChangePositionType: Self = Self(25);
    /// `ConvertDustBNB`: convert dust balances to BNB.
    pub const ConvertDustBNB: Self = Self(26);
    /// `ConfirmRiskLimit`: confirm risk limit for a market.
    pub const ConfirmRiskLimit: Self = Self(27);
    /// `SetMAMode`: enable or disable Binance Multi-Assets mode.
    pub const SetMAMode: Self = Self(28);
    /// `DoTransferAsset`: transfer one asset between exchange wallet kinds.
    pub const DoTransferAsset: Self = Self(29);
    /// `UpdateTransferAssets`: refresh the transferable asset list for one
    /// exchange wallet kind. Parse with [`parse_update_transfer_assets_response`].
    pub const UpdateTransferAssets: Self = Self(30);
    /// `GetCoinCardCandles`: short candle history for a coin-card UI component.
    pub const GetCoinCardCandles: Self = Self(31);

    /// Сохранить raw Delphi ordinal byte. Delphi читает/пишет
    /// `TEngineMethodKind` через `ms.Read/Stream.Write` и не превращает
    /// unknown ordinal в `emk_None`.
    pub const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    pub const fn to_byte(self) -> u8 {
        self.0
    }

    pub const fn is_known(self) -> bool {
        self.0 <= Self::GetCoinCardCandles.0
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "None",
            Self::BaseCheck => "BaseCheck",
            Self::AuthCheck => "AuthCheck",
            Self::GetMarketsList => "GetMarketsList",
            Self::UpdateMarketsList => "UpdateMarketsList",
            Self::GetMarketsIndexes => "GetMarketsIndexes",
            Self::GetBalance => "GetBalance",
            Self::GetMarketsBalanceFull => "GetMarketsBalanceFull",
            Self::GetOrder => "GetOrder",
            Self::GetOpenOrders => "GetOpenOrders",
            Self::GetActiveOrders => "GetActiveOrders",
            Self::CancelAllOrders => "CancelAllOrders",
            Self::SetLeverage => "SetLeverage",
            Self::SetHedgeMode => "SetHedgeMode",
            Self::QueryHedgeMode => "QueryHedgeMode",
            Self::CheckAPIExpirationTime => "CheckAPIExpirationTime",
            Self::CheckBinanceTags => "CheckBinanceTags",
            Self::TradesResend => "TradesResend",
            Self::SubscribeAllTrades => "SubscribeAllTrades",
            Self::UnsubscribeAllTrades => "UnsubscribeAllTrades",
            Self::SubscribeOrderBook => "SubscribeOrderBook",
            Self::UnsubscribeOrderBook => "UnsubscribeOrderBook",
            Self::RequestOrderBookFull => "RequestOrderBookFull",
            Self::ReloadOrderBook => "ReloadOrderBook",
            Self::RequestCandlesData => "RequestCandlesData",
            Self::ChangePositionType => "ChangePositionType",
            Self::ConvertDustBNB => "ConvertDustBNB",
            Self::ConfirmRiskLimit => "ConfirmRiskLimit",
            Self::SetMAMode => "SetMAMode",
            Self::DoTransferAsset => "DoTransferAsset",
            Self::UpdateTransferAssets => "UpdateTransferAssets",
            Self::GetCoinCardCandles => "GetCoinCardCandles",
            _ => "Unknown",
        }
    }
}

impl std::fmt::Debug for EngineMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_known() {
            f.write_str(self.name())
        } else {
            write!(f, "Unknown({})", self.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_method_known_bytes() {
        assert_eq!(EngineMethod::from_byte(1), EngineMethod::BaseCheck);
        assert_eq!(
            EngineMethod::from_byte(31),
            EngineMethod::GetCoinCardCandles
        );
        assert_eq!(EngineMethod::from_byte(0), EngineMethod::None);
    }

    #[test]
    fn engine_method_unknown_preserves_raw_ordinal_like_delphi() {
        // Delphi `ms.Read(Method, SizeOf(Method))` keeps the raw enum byte.
        let method = EngineMethod::from_byte(99);
        assert_eq!(method.to_byte(), 99);
        assert_eq!(method.name(), "Unknown");
        assert!(!method.is_known());
        assert_eq!(EngineMethod::from_byte(255).to_byte(), 255);
    }
}

/// Parsed Engine API response (`TEngineResponse`, server to client).
#[derive(Debug, Clone)]
pub struct EngineResponse {
    /// Delphi `TBaseCommand.ver` from the response header.
    pub ver: u16,
    /// UID of the original `TEngineRequest`.
    pub request_uid: u64,
    /// Engine method echoed by the server.
    pub method: EngineMethod,
    /// Server success flag.
    pub success: bool,
    /// Server error code when `success == false`.
    pub error_code: i32,
    /// Server error message when `success == false`.
    pub error_msg: String,
    /// Method-specific response payload, already DEFLATE-decompressed when the
    /// wire response was compressed.
    pub data: Vec<u8>,
}

/// Parse TEngineResponse from command payload.
///
/// **Wire-format** (после Crypted decrypt + CryptoHeader strip, payload **с** Engine
/// TBaseCommand header):
/// ```text
/// [CmdId(1)=1][ver(2)][own_UID(8)][RequestUID(8)][Method(1)][Success(1)][ErrorCode(4)][ErrorMsg(string)][IsCompressed(1)][DataSize(4)][Data]
/// ```
///
/// Engine TBaseCommand header (11 байт: `CmdId + ver + own_UID`) **пропускается**
/// до чтения `RequestUID` — соответствует Delphi `TEngineResponse.CreateFromStream`
/// который через `inherited CreateFromStream` (TBaseCommand) сначала читает ver+UID,
/// потом own fields.
///
/// **Историческая ошибка** (исправлено): раньше парсер начинал с `pos=0`, читая
/// `[ver][own_UID first 5 bytes]` как `request_uid` — никогда не совпадало с
/// зарегистрированным uid → все Engine API responses терялись (BaseCheck/AuthCheck/
/// GetMarketsList timeouts).
///
/// Matches `MoonProtoEngineStruct.pas:364-403`.
pub fn parse_engine_response(data: &[u8]) -> Option<EngineResponse> {
    if data.len() < 11 {
        return None;
    }
    let ver = u16::from_le_bytes(data[1..3].try_into().unwrap());
    // Skip Engine TBaseCommand header: CmdId(1) + ver(2) + own_UID(8) = 11 bytes.
    let mut pos = 11usize;

    if pos + 8 > data.len() {
        return None;
    }
    let request_uid = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
    pos += 8;

    if pos + 1 > data.len() {
        return None;
    }
    let method = EngineMethod::from_byte(data[pos]);
    pos += 1;

    if pos + 1 > data.len() {
        return None;
    }
    let success = data[pos] != 0;
    pos += 1;

    if pos + 4 > data.len() {
        return None;
    }
    let error_code = i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
    pos += 4;

    let error_msg = read_string(data, &mut pos)?;

    // IsCompressed + data size
    if pos + 1 > data.len() {
        return None;
    }
    let is_compressed = data[pos] != 0;
    pos += 1;

    if pos + 4 > data.len() {
        return None;
    }
    let sz = i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
    pos += 4;

    let response_data = if sz > 0 {
        let sz = sz as usize;
        let Some(end) = pos.checked_add(sz) else {
            log::warn!(target: "moonproto::engine_api",
                "EngineResponse uid={} declares overflowing data size {}",
                request_uid, sz);
            return None;
        };
        if end > data.len() {
            log::warn!(target: "moonproto::engine_api",
                "EngineResponse uid={} declares data size {} but only {} bytes remain",
                request_uid, sz, data.len().saturating_sub(pos));
            return None;
        }

        let raw = &data[pos..end];
        if is_compressed {
            use std::io::Read;
            let mut decoder = DeflateDecoder::new(raw);
            let mut decompressed = Vec::new();
            match decoder.read_to_end(&mut decompressed) {
                Ok(_) => decompressed,
                Err(e) => {
                    log::warn!(target: "moonproto::engine_api",
                        "DEFLATE decompress failed for EngineResponse uid={}: {}",
                        request_uid, e);
                    return None;
                }
            }
        } else {
            raw.to_vec()
        }
    } else {
        Vec::new()
    };

    Some(EngineResponse {
        ver,
        request_uid,
        method,
        success,
        error_code,
        error_msg,
        data: response_data,
    })
}

/// Parse `EngineResponse.data` for `emk_GetBalance` (`EngineMethod::GetBalance`).
///
/// The Delphi server writes exactly one little-endian `Double` on success:
/// `MoonProtoEngineServer.pas:315-319` (`resp.WriteDouble(q)`). Extra trailing
/// bytes are ignored so newer servers can append fields without breaking old
/// consumers.
pub fn parse_get_balance_response(data: &[u8]) -> Option<f64> {
    if data.len() < 8 {
        return None;
    }
    Some(f64::from_le_bytes(data[0..8].try_into().unwrap()))
}

/// Parse `EngineResponse.data` for `emk_QueryHedgeMode`
/// (`EngineMethod::QueryHedgeMode`).
///
/// The Delphi server writes one `Boolean` byte on success:
/// `MoonProtoEngineServer.pas:341-344` (`resp.WriteBool(hedgeMode)`). Extra
/// trailing bytes are ignored for forward compatibility.
pub fn parse_query_hedge_mode_response(data: &[u8]) -> Option<bool> {
    data.first().map(|&v| v != 0)
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
    /// Build from the raw Delphi `TDateTime` value.
    pub fn from_delphi_time(delphi_time: f64) -> Self {
        Self { delphi_time }
    }

    /// Raw Delphi `TDateTime` value retained for exact diagnostics.
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
        if !self.is_known() {
            return None;
        }
        let seconds = ((self.delphi_time - DELPHI_UNIX_EPOCH_DAYS) * SECONDS_PER_DAY).round();
        if !seconds.is_finite() || seconds < 0.0 || seconds > i64::MAX as f64 {
            return None;
        }
        Some(seconds as i64)
    }

    /// Convert to `SystemTime` when the value is known and not before the Unix epoch.
    pub fn system_time(&self) -> Option<SystemTime> {
        let seconds = self.unix_seconds()?;
        Some(SystemTime::UNIX_EPOCH + Duration::from_secs(seconds as u64))
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
pub fn parse_api_expiration_time_response(data: &[u8]) -> Option<ApiExpirationTime> {
    if data.len() < 8 {
        return None;
    }
    Some(ApiExpirationTime::from_delphi_time(f64::from_le_bytes(
        data[0..8].try_into().unwrap(),
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
pub fn parse_update_transfer_assets_response(data: &[u8]) -> Option<Vec<TransferAsset>> {
    let mut pos = 0usize;
    if data.len() < 4 {
        return None;
    }
    let count_raw = i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
    pos += 4;
    if count_raw < 0 {
        log::warn!(target: "moonproto::engine_api",
            "UpdateTransferAssets: negative count {} rejected",
            count_raw);
        return None;
    }

    let count = count_raw as usize;
    let mut assets = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        let currency = read_string(data, &mut pos)?;
        if pos + 16 > data.len() {
            return None;
        }
        let amount = f64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let total = f64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        assets.push(TransferAsset {
            currency,
            amount,
            total,
        });
    }
    Some(assets)
}

// =============================================================================
//  AuthCheck response parser (audit_api_docs C4)
// =============================================================================

/// Hyperliquid DEX info — wire-layout соответствует Delphi `THLDexInfo` (Vars.pas:43-46):
///   `Name: string[15]` (Pascal shortstring: 1 byte length + 15 bytes data = 16 байт)
///   + `CollateralToken: word` (u16 LE = 2 байт) = **18 байт packed**.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DexInfo {
    /// Имя DEX (например `"BTCUSD-USDC"`); пустая строка для default validator (USDC).
    pub name: String,
    /// Token используемый как collateral. Известные значения:
    /// `0` = USDC, `360` = USDH, `235` = USDE, `268` = USDT0.
    pub collateral_token: u16,
}

/// Распакованный ответ на `emk_AuthCheck` (Engine method 2).
///
/// Содержит данные привязки клиента к биржевому аккаунту + информацию о доступных
/// Hyperliquid DEX (опционально, появилось в Phase 2 — поля `recvd_max_payload`,
/// `known_dexes`, `hl_dex_market`, `hl_spot_market` могут отсутствовать в старых
/// серверах — это `Option`).
///
/// Используется потребителем после `Client::api_auth_check()`:
/// ```ignore
/// let rx = client.api_auth_check();
/// let resp = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(12))?;
/// if let Some(auth) = parse_auth_check_response(&resp.data) {
///     println!("Account: {}, BTC addr: {}", auth.account_id, auth.btc_address);
///     for dex in &auth.known_dexes {
///         println!("  DEX: {} (collateral={})", dex.name, dex.collateral_token);
///     }
/// }
/// ```
///
/// Source: `MoonProtoEngine.pas:605-639`.
#[derive(Debug, Clone)]
pub struct AuthCheckResponse {
    /// ID аккаунта на Binance (если используется Binance API; иначе 0).
    pub binance_account_id: i64,
    /// Bitcoin адрес привязанный к аккаунту (для рефералов / wallet binding).
    pub btc_address: String,
    /// Spot referral config (исторический параметр; обычно 0).
    pub spot_ref: i32,
    /// True если это sub-аккаунт головного.
    pub is_sub_account: bool,
    /// ID аккаунта (Hyperliquid wallet address / Binance account string).
    pub account_id: String,
    /// Максимальный поддерживаемый payload (от сервера). None если старый сервер.
    pub recvd_max_payload: Option<i32>,
    /// Список известных Hyperliquid DEX (Phase 2; для UI меню переключения DEX).
    /// Пустой Vec если старый сервер.
    pub known_dexes: Vec<DexInfo>,
    /// Индекс текущего активного HL DEX (futures). None если старый сервер.
    pub hl_dex_market: Option<u8>,
    /// Индекс текущего активного HL DEX (spot). None если старый сервер.
    pub hl_spot_market: Option<u8>,
}

/// Распарсить `EngineResponse.data` для `emk_AuthCheck` (`EngineMethod::AuthCheck`).
///
/// Returns `None` если payload corrupt или короче минимального размера для обязательных полей.
/// Опциональные поля (Phase 2 расширения) парсятся `if !EOF`; их отсутствие = старый сервер,
/// `parse_auth_check_response` всё равно возвращает `Some` с заполненными обязательными.
/// DEX tail follows Delphi's soft stream-read shape: the declared `cnt` is read,
/// complete `THLDexInfo` records are preserved, and a truncated tail does not
/// reject the whole AuthCheck payload.
///
/// Byte-exact с Delphi: `MoonProtoEngine.pas:610-633`.
pub fn parse_auth_check_response(data: &[u8]) -> Option<AuthCheckResponse> {
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

    // Optional Phase 2 extensions (читаем if !EOF).
    let recvd_max_payload = if pos + 4 <= data.len() {
        let v = i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;
        Some(v)
    } else {
        None
    };

    let mut known_dexes: Vec<DexInfo> = Vec::new();
    let mut hl_dex_market: Option<u8> = None;
    let mut hl_spot_market: Option<u8> = None;

    if recvd_max_payload.is_some() && pos < data.len() {
        let cnt = data[pos] as usize;
        pos += 1;
        const DEX_INFO_SIZE: usize = 18;
        known_dexes.reserve(cnt.min(data.len().saturating_sub(pos) / DEX_INFO_SIZE));
        for _ in 0..cnt {
            if pos + DEX_INFO_SIZE > data.len() {
                break;
            }
            // THLDexInfo packed: [u8 length][15 bytes name][u16 collateral_token]
            let name_len = data[pos] as usize;
            // Защита: name_len по контракту ≤ 15. Если больше — corrupt, используем 15.
            let effective_len = name_len.min(15);
            let name_bytes = &data[pos + 1..pos + 1 + effective_len];
            let name = decode_utf8_delphi(name_bytes);
            let collateral_token = u16::from_le_bytes([data[pos + 16], data[pos + 17]]);
            pos += DEX_INFO_SIZE;
            known_dexes.push(DexInfo {
                name,
                collateral_token,
            });
        }
        // hl_dex_market и hl_spot_market следуют сразу после массива.
        if pos < data.len() {
            hl_dex_market = Some(data[pos]);
            pos += 1;
            if pos < data.len() {
                hl_spot_market = Some(data[pos]);
                // pos += 1;  // больше не используется
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

// =============================================================================
//  BaseCheck response parser — multi-server identification
// =============================================================================

/// Bitmask flags для `ServerInfo::exchange_type_mask`. Несколько бит могут быть
/// установлены одновременно (например Spot+Futures если сервер обслуживает оба).
pub mod exchange_type_flags {
    /// Spot trading доступен.
    pub const SPOT: u8 = 0x01;
    /// Futures (perpetual / dated) доступны.
    pub const FUTURES: u8 = 0x02;
    /// Сервер работает с DEX (Hyperliquid и подобные).
    pub const DEX: u8 = 0x04;
    /// Predict / outcome markets (HL Spot где `HLSpotMarket = 1`; Polymarket в будущем).
    pub const PREDICT: u8 = 0x08;
}

/// Распакованная identity сервера, возвращаемая в ответе на `emk_BaseCheck`
/// (`EngineMethod::BaseCheck`).
///
/// **Назначение.** Когда клиент подключается к нескольким `MoonBot`-серверам
/// одновременно, ему нужно различать их (показать в UI имя биржи, базовую валюту,
/// версии для compat-проверок). `emk_BaseCheck` — первый Engine-вызов в init
/// sequence, поэтому именно он несёт **server identity** (`emk_AuthCheck` идёт
/// после и несёт **per-account** информацию — `binance_account_id`, `is_sub_account`,
/// `account_id`).
///
/// **Forward-compat.** Все поля `Option`. Старые сервера (до multi-server расширения)
/// шлют пустой response — все поля будут `None`. Новый сервер постепенно дополняется
/// полями — клиент читает пока есть данные, остальные остаются `None`.
///
/// **Wire-format** (порядок в payload, все поля опциональные через `if !EOF`):
/// 1. `bot_id`                  — i64 LE (`cfg.UniqueBotID`, уникальный идентификатор сервера)
/// 2. `server_name`             — string LE u16 length + UTF-8 ("Binance Main", default `"Server"`)
/// 3. `exchange_code`           — u8 (`Ord(cfg.Header.Current)` — `TBotPlatform` enum)
/// 4. `exchange_name`           — string ("Binance Futures", "Hyper", ...)
/// 5. `exchange_type_mask`      — u8 bitmask (см. [`exchange_type_flags`])
/// 6. `dex_name`                — string (HIP-3 dex name для HL futures; `""` иначе)
/// 7. `base_currency_name`      — string ("USDT", "BTC", ...)
/// 8. `base_currency_code`      — u8 (`Ord(cfg.BaseCurrency)` — `TBaseCurrency` enum, BC_USDT=1)
/// 9. `server_version`          — i32 LE (`Current_Version_Num_X`, например 763 = v7.63)
/// 10. `moonproto_version`      — i32 LE (`IntMoonProtoTCPCurrentVer`)
///
/// Source: `MoonProtoEngineServer.pas:244-273`.
///
/// Пример использования:
/// ```ignore
/// use moonproto::commands::engine_api::{parse_base_check_response, exchange_type_flags};
/// let rx = client.api_base_check();
/// let resp = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(12))?;
/// if resp.success {
///     let info = parse_base_check_response(&resp.data);
///     if let (Some(name), Some(mask)) = (&info.exchange_name, info.exchange_type_mask) {
///         let futures = (mask & exchange_type_flags::FUTURES) != 0;
///         println!("Connected to {} (futures: {}, base: {:?})",
///             name, futures, info.base_currency_name);
///     }
/// }
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServerInfo {
    /// `cfg.UniqueBotID` — уникальный 64-битный идентификатор сервера, стабильный
    /// через перезапуски. `None` если сервер не передал (старая версия). Это
    /// основной ID для multi-server идентификации в UI.
    pub bot_id: Option<i64>,
    /// Human-readable имя сервера для UI ("Binance Main", "My Bybit Test"). Сервер
    /// присылает `cfg.BotName`; если пустое — `"Server"`.
    pub server_name: Option<String>,
    /// Числовой код биржи (`Ord(cfg.Header.Current)` — Delphi enum `TBotPlatform`).
    /// Финальный список значений ведёт сервер.
    pub exchange_code: Option<u8>,
    /// Человеко-читаемое имя биржи для UI ("Binance Futures", "Hyper", ...).
    pub exchange_name: Option<String>,
    /// Bitmask что доступно на этом сервере. См. [`exchange_type_flags`].
    pub exchange_type_mask: Option<u8>,
    /// HIP-3 dex name для Hyperliquid futures (`GetHIP3DexName`). Пустая строка
    /// для остальных бирж (`""`, не `None` — сервер всё равно пишет поле).
    pub dex_name: Option<String>,
    /// Имя базовой валюты ("USDT" / "USD" / "BTC" / ...). В Delphi —
    /// `cfg.Currency` (string), может меняться при переключении HL DEX.
    pub base_currency_name: Option<String>,
    /// Код базовой валюты (`Ord(cfg.BaseCurrency)` — Delphi enum `TBaseCurrency`,
    /// BC_USDT=1). Дополняет `base_currency_name` для type-safe сравнений.
    pub base_currency_code: Option<u8>,
    /// Версия MoonBot (`Current_Version_Num_X`, например `763` для v7.63). Wire-тип
    /// Delphi `Int` (i32); храним как `u32` для semantic clarity (версия беззнаковая).
    pub server_version: Option<u32>,
    /// Версия протокола MoonProto (`IntMoonProtoTCPCurrentVer`). Резерв на будущие
    /// breaking changes wire-format'а.
    pub moonproto_version: Option<u32>,
}

impl ServerInfo {
    /// `true` если `bot_id` заполнено — сервер минимум сообщил свою identity.
    /// Старые серверы вернут `false` (вся структура с `None`).
    pub fn has_identity(&self) -> bool {
        self.bot_id.is_some()
    }

    /// Удобный helper: возвращает `true` если в `exchange_type_mask` установлен
    /// соответствующий бит. При `None` (старый сервер не передал mask) —
    /// возвращает `false`.
    pub fn supports(&self, flag: u8) -> bool {
        match self.exchange_type_mask {
            Some(mask) => (mask & flag) != 0,
            None => false,
        }
    }
}

/// Распарсить `EngineResponse.data` для `emk_BaseCheck` (`EngineMethod::BaseCheck`).
///
/// Возвращает `ServerInfo` со всеми заполненными полями которые удалось прочитать.
/// Никогда не возвращает `None` — пустой payload (старый сервер) валиден,
/// результат = `ServerInfo::default()` (все поля `None`).
///
/// Парсинг **толерантен к truncate'у**: если payload обрывается посередине, поля
/// до точки обрыва заполнены, остальные = `None`. Это соответствует Delphi-паттерну
/// `If not resp.EOF then` для опциональных полей.
///
/// Byte-exact с серверной частью `MoonProtoEngineServer.pas:244-273`.
pub fn parse_base_check_response(data: &[u8]) -> ServerInfo {
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
    info.exchange_code = Some(data[pos]);
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
    info.exchange_type_mask = Some(data[pos]);
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
    info.base_currency_code = Some(data[pos]);
    pos += 1;

    // 9. server_version (i32 LE → u32) — Delphi WriteInt(Current_Version_Num_X).
    // Trust серверу: значения версий монотонно растут с малых положительных
    // чисел (например 763 для v7.63), поэтому signed/unsigned различие не влияет.
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
    // pos += 4;  // больше не используется

    info
}

#[cfg(test)]
mod parse_engine_response_tests {
    use super::*;

    /// Helper: builds a fake response wire-payload as the server would emit
    /// (after Crypted decrypt + CryptoHeader strip). Layout:
    /// [CmdId(1)=1][ver(2)=3][own_UID(8)][RequestUID(8)][Method(1)][Success(1)]
    /// [ErrorCode(4)][ErrorMsg_len(2)][ErrorMsg][IsCompressed(1)][DataSize(4)][Data]
    fn build_wire_response(
        own_uid: u64,
        request_uid: u64,
        method: EngineMethod,
        success: bool,
        error_code: i32,
        error_msg: &str,
        data: &[u8],
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(1u8); // CmdId = 1
        buf.extend_from_slice(&3u16.to_le_bytes()); // ver = 3
        buf.extend_from_slice(&own_uid.to_le_bytes()); // own_UID
        buf.extend_from_slice(&request_uid.to_le_bytes()); // RequestUID (echo)
        buf.push(method.to_byte()); // Method
        buf.push(success as u8); // Success
        buf.extend_from_slice(&error_code.to_le_bytes()); // ErrorCode
        write_string(&mut buf, error_msg);
        buf.push(0u8); // IsCompressed = false
        buf.extend_from_slice(&(data.len() as i32).to_le_bytes()); // DataSize
        buf.extend_from_slice(data);
        buf
    }

    #[test]
    fn parse_skips_basecmd_header_and_reads_request_uid_correctly() {
        // Регрессия от critical bug: парсер ДО fix начинал с offset 0
        // и читал request_uid из `[CmdId][ver][own_UID 5 bytes]` — garbage.
        let request_uid = 0x12_34_56_78_9A_BC_DE_F0u64;
        let payload = build_wire_response(
            0xAAAA_BBBB_CCCC_DDDD, // own_UID (random)
            request_uid,           // RequestUID (echo)
            EngineMethod::BaseCheck,
            true,
            0,
            "",
            &[],
        );
        let resp = parse_engine_response(&payload).expect("parse ok");
        assert_eq!(resp.ver, 3);
        assert_eq!(resp.request_uid, request_uid);
        assert_eq!(resp.method, EngineMethod::BaseCheck);
        assert!(resp.success);
        assert_eq!(resp.error_code, 0);
        assert!(resp.error_msg.is_empty());
        assert!(resp.data.is_empty());
    }

    #[test]
    fn parse_carries_method_byte_after_request_uid() {
        // Каждый method byte корректно читается с offset 19 (после header + request_uid).
        for method in [
            EngineMethod::AuthCheck,
            EngineMethod::GetMarketsList,
            EngineMethod::GetMarketsIndexes,
            EngineMethod::SubscribeAllTrades,
            EngineMethod::GetOpenOrders,
        ] {
            let payload = build_wire_response(0xDEAD, 0xBEEF, method, true, 0, "", &[]);
            let resp = parse_engine_response(&payload).expect("parse ok");
            assert_eq!(resp.method, method, "method mismatch for {:?}", method);
            assert_eq!(resp.request_uid, 0xBEEF);
        }
    }

    #[test]
    fn parse_preserves_unknown_method_ordinal_like_delphi() {
        let payload = build_wire_response(
            0xDEAD,
            0xBEEF,
            EngineMethod::from_byte(99),
            false,
            400,
            "Unknown method",
            &[],
        );
        let resp = parse_engine_response(&payload).expect("parse ok");
        assert_eq!(resp.method.to_byte(), 99);
        assert_eq!(resp.method.name(), "Unknown");
        assert_eq!(resp.request_uid, 0xBEEF);
        assert_eq!(resp.error_code, 400);
        assert_eq!(resp.error_msg, "Unknown method");
    }

    #[test]
    fn parse_carries_error_payload() {
        let payload = build_wire_response(
            1,
            42,
            EngineMethod::AuthCheck,
            false, // success = false
            -123,  // error_code
            "Invalid API key",
            &[],
        );
        let resp = parse_engine_response(&payload).expect("parse ok");
        assert!(!resp.success);
        assert_eq!(resp.error_code, -123);
        assert_eq!(resp.error_msg, "Invalid API key");
        assert_eq!(resp.request_uid, 42);
    }

    #[test]
    fn response_helper_writes_error_msg_like_delphi_string() {
        let payload = build_wire_response(
            1,
            42,
            EngineMethod::AuthCheck,
            false,
            -123,
            &"E".repeat(65_537),
            &[],
        );
        let resp = parse_engine_response(&payload).expect("parse ok");
        assert_eq!(resp.error_msg, "E");
        assert_eq!(resp.request_uid, 42);
    }

    #[test]
    fn parse_carries_uncompressed_data() {
        let blob = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x12, 0x34, 0x56, 0x78];
        let payload = build_wire_response(0, 100, EngineMethod::GetMarketsList, true, 0, "", &blob);
        let resp = parse_engine_response(&payload).expect("parse ok");
        assert_eq!(resp.data, blob);
        assert_eq!(resp.method, EngineMethod::GetMarketsList);
    }

    #[test]
    fn parse_handles_negative_data_size_without_panic() {
        let blob = [0xAA, 0xBB, 0xCC];
        let mut payload =
            build_wire_response(0, 100, EngineMethod::GetMarketsList, true, 0, "", &blob);
        let size_pos = payload.len() - blob.len() - 4;
        payload[size_pos..size_pos + 4].copy_from_slice(&(-1i32).to_le_bytes());
        payload.truncate(size_pos + 4);

        let resp = parse_engine_response(&payload).expect("parse ok");
        assert!(resp.data.is_empty());
    }

    #[test]
    fn parse_inflates_compressed_response_data() {
        use flate2::{write::DeflateEncoder, Compression};
        use std::io::Write;

        let plain = b"compressed engine response payload".repeat(4);
        let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&plain).unwrap();
        let compressed = encoder.finish().unwrap();

        let mut payload = build_wire_response(
            0,
            101,
            EngineMethod::UpdateMarketsList,
            true,
            0,
            "",
            &compressed,
        );
        let size_pos = payload.len() - compressed.len() - 4;
        payload[size_pos - 1] = 1; // IsCompressed = true

        let resp = parse_engine_response(&payload).expect("parse ok");
        assert_eq!(resp.data, plain);
    }

    #[test]
    fn parse_get_balance_response_reads_delphi_double() {
        let mut data = 1234.5f64.to_le_bytes().to_vec();
        data.extend_from_slice(&[0xAA, 0xBB]);

        assert_eq!(parse_get_balance_response(&data), Some(1234.5));
        assert_eq!(parse_get_balance_response(&data[..7]), None);
    }

    #[test]
    fn parse_query_hedge_mode_response_reads_delphi_bool() {
        assert_eq!(parse_query_hedge_mode_response(&[0]), Some(false));
        assert_eq!(parse_query_hedge_mode_response(&[1]), Some(true));
        assert_eq!(parse_query_hedge_mode_response(&[2, 0xAA]), Some(true));
        assert_eq!(parse_query_hedge_mode_response(&[]), None);
    }

    #[test]
    fn parse_api_expiration_time_response_reads_delphi_datetime() {
        let mut data = 45_000.25f64.to_le_bytes().to_vec();
        data.extend_from_slice(&[0xAA, 0xBB]);

        let parsed = parse_api_expiration_time_response(&data).unwrap();
        assert_eq!(parsed.delphi_time(), 45_000.25);
        assert_eq!(parse_api_expiration_time_response(&data[..7]), None);
    }

    #[test]
    fn api_expiration_time_converts_unix_epoch() {
        let parsed = ApiExpirationTime::from_delphi_time(DELPHI_UNIX_EPOCH_DAYS);
        assert!(parsed.is_known());
        assert_eq!(parsed.unix_seconds(), Some(0));
        assert_eq!(parsed.system_time(), Some(SystemTime::UNIX_EPOCH));
        assert_eq!(
            parsed.days_until(SystemTime::UNIX_EPOCH + Duration::from_secs(2 * 86_400)),
            Some(-2)
        );

        let unknown = ApiExpirationTime::from_delphi_time(0.0);
        assert!(!unknown.is_known());
        assert_eq!(unknown.system_time(), None);
    }

    #[test]
    fn parse_update_transfer_assets_response_reads_delphi_rows() {
        let mut data = Vec::new();
        data.extend_from_slice(&(2i32).to_le_bytes());
        data.extend_from_slice(&(4u16).to_le_bytes());
        data.extend_from_slice(b"USDT");
        data.extend_from_slice(&(12.5f64).to_le_bytes());
        data.extend_from_slice(&(100.0f64).to_le_bytes());
        data.extend_from_slice(&(3u16).to_le_bytes());
        data.extend_from_slice(b"BTC");
        data.extend_from_slice(&(0.25f64).to_le_bytes());
        data.extend_from_slice(&(1.0f64).to_le_bytes());

        let parsed = parse_update_transfer_assets_response(&data).unwrap();
        assert_eq!(
            parsed,
            vec![
                TransferAsset {
                    currency: "USDT".to_string(),
                    amount: 12.5,
                    total: 100.0,
                },
                TransferAsset {
                    currency: "BTC".to_string(),
                    amount: 0.25,
                    total: 1.0,
                },
            ]
        );
    }

    #[test]
    fn parse_update_transfer_assets_response_rejects_bad_payloads() {
        assert_eq!(parse_update_transfer_assets_response(&[]), None);
        assert_eq!(
            parse_update_transfer_assets_response(&(-1i32).to_le_bytes()),
            None
        );

        let mut truncated = Vec::new();
        truncated.extend_from_slice(&(1i32).to_le_bytes());
        truncated.extend_from_slice(&(4u16).to_le_bytes());
        truncated.extend_from_slice(b"USDT");
        truncated.extend_from_slice(&(12.5f64).to_le_bytes());
        assert_eq!(parse_update_transfer_assets_response(&truncated), None);
    }

    #[test]
    fn parse_returns_none_on_short_payload() {
        // < 11 bytes header не парсится.
        let too_short = vec![0u8; 10];
        assert!(parse_engine_response(&too_short).is_none());
    }

    #[test]
    fn parse_returns_none_when_truncated_at_request_uid() {
        // header (11) + 4 bytes (вместо 8 для request_uid) → None.
        let mut buf = vec![0u8; 11];
        buf.extend_from_slice(&[1, 2, 3, 4]);
        assert!(parse_engine_response(&buf).is_none());
    }

    #[test]
    fn parse_returns_none_when_error_msg_body_is_truncated_like_delphi_readbuffer() {
        let mut payload = build_wire_response(0, 100, EngineMethod::AuthCheck, false, 401, "", &[]);
        let error_msg_len_pos = 11 + 8 + 1 + 1 + 4;
        payload.truncate(error_msg_len_pos);
        payload.extend_from_slice(&(4u16).to_le_bytes());
        payload.extend_from_slice(b"NO");

        assert!(parse_engine_response(&payload).is_none());
    }

    #[test]
    fn parse_returns_none_when_compression_flag_is_missing() {
        let mut payload = build_wire_response(0, 100, EngineMethod::BaseCheck, true, 0, "", &[]);
        payload.truncate(11 + 8 + 1 + 1 + 4 + 2);

        assert!(parse_engine_response(&payload).is_none());
    }

    #[test]
    fn parse_returns_none_when_data_size_is_missing() {
        let mut payload = build_wire_response(0, 100, EngineMethod::BaseCheck, true, 0, "", &[]);
        payload.truncate(11 + 8 + 1 + 1 + 4 + 2 + 1);

        assert!(parse_engine_response(&payload).is_none());
    }

    #[test]
    fn parse_returns_none_when_declared_data_body_is_truncated() {
        let mut payload = build_wire_response(
            0,
            100,
            EngineMethod::GetMarketsList,
            true,
            0,
            "",
            &[0xAA, 0xBB],
        );
        let size_pos = payload.len() - 2 - 4;
        payload[size_pos..size_pos + 4].copy_from_slice(&(8i32).to_le_bytes());

        assert!(parse_engine_response(&payload).is_none());
    }
}

#[cfg(test)]
mod base_check_tests {
    use super::*;

    /// Helper: build wire-payload for BaseCheck response from a fully-populated `ServerInfo`.
    /// Reverse of `parse_base_check_response` for round-trip testing.
    ///
    /// Поля пишутся в том же порядке что и сервер (`MoonProtoEngineServer.pas:262-271`).
    /// Каждое поле пишется только если `Some(...)`; первый `None` обрывает запись
    /// (это соответствует семантике truncate'а — следующие поля становятся
    /// "недоступными" для парсера).
    fn encode_full(info: &ServerInfo) -> Vec<u8> {
        let mut buf = Vec::new();
        let Some(id) = info.bot_id else { return buf };
        buf.extend_from_slice(&id.to_le_bytes());
        let Some(name) = &info.server_name else {
            return buf;
        };
        write_string(&mut buf, name);
        let Some(ex_code) = info.exchange_code else {
            return buf;
        };
        buf.push(ex_code);
        let Some(ex_name) = &info.exchange_name else {
            return buf;
        };
        write_string(&mut buf, ex_name);
        let Some(mask) = info.exchange_type_mask else {
            return buf;
        };
        buf.push(mask);
        let Some(dex) = &info.dex_name else {
            return buf;
        };
        write_string(&mut buf, dex);
        let Some(bc_name) = &info.base_currency_name else {
            return buf;
        };
        write_string(&mut buf, bc_name);
        let Some(bc_code) = info.base_currency_code else {
            return buf;
        };
        buf.push(bc_code);
        let Some(sv) = info.server_version else {
            return buf;
        };
        buf.extend_from_slice(&(sv as i32).to_le_bytes());
        let Some(mp) = info.moonproto_version else {
            return buf;
        };
        buf.extend_from_slice(&(mp as i32).to_le_bytes());
        buf
    }

    #[test]
    fn parse_empty_payload_returns_all_none() {
        // Старый сервер до multi-server расширения шлёт пустой response.
        // Парсер не должен падать — возвращает дефолт со всеми None.
        let info = parse_base_check_response(&[]);
        assert_eq!(info, ServerInfo::default());
        assert!(!info.has_identity());
        assert!(info.bot_id.is_none());
        assert!(info.moonproto_version.is_none());
    }

    #[test]
    fn parse_full_payload_returns_all_fields() {
        let original = ServerInfo {
            bot_id: Some(0x12_34_56_78_9A_BC_DE_F0_i64),
            server_name: Some("Binance Main".to_string()),
            exchange_code: Some(1),
            exchange_name: Some("Binance Futures".to_string()),
            exchange_type_mask: Some(exchange_type_flags::FUTURES),
            dex_name: Some(String::new()), // не HL futures → пусто
            base_currency_name: Some("USDT".to_string()),
            base_currency_code: Some(1), // BC_USDT
            server_version: Some(763),   // v7.63
            moonproto_version: Some(3),
        };
        let payload = encode_full(&original);
        let parsed = parse_base_check_response(&payload);
        assert_eq!(parsed, original);
        assert!(parsed.has_identity());
        assert!(parsed.supports(exchange_type_flags::FUTURES));
        assert!(!parsed.supports(exchange_type_flags::SPOT));
    }

    #[test]
    fn base_check_helper_writes_strings_like_delphi() {
        let original = ServerInfo {
            bot_id: Some(123456789),
            server_name: Some("S".repeat(65_537)),
            exchange_code: Some(3),
            exchange_name: Some("Exchange".to_string()),
            exchange_type_mask: Some(exchange_type_flags::SPOT | exchange_type_flags::FUTURES),
            dex_name: Some("Dex".to_string()),
            base_currency_name: Some("USDT".to_string()),
            base_currency_code: Some(1),
            server_version: Some(763),
            moonproto_version: Some(3),
        };

        let payload = encode_full(&original);
        let parsed = parse_base_check_response(&payload);
        assert_eq!(parsed.server_name, Some("S".to_string()));
        assert_eq!(parsed.exchange_code, Some(3));
        assert_eq!(parsed.moonproto_version, Some(3));
    }

    #[test]
    fn parse_hl_futures_with_hip3_dex_name() {
        // Hyperliquid futures с HIP-3 dex — все 4 типа в mask + непустой dex_name.
        let original = ServerInfo {
            bot_id: Some(42),
            server_name: Some("Hyper Test".to_string()),
            exchange_code: Some(7),
            exchange_name: Some("Hyper".to_string()),
            exchange_type_mask: Some(exchange_type_flags::FUTURES | exchange_type_flags::DEX),
            dex_name: Some("HIP3-PERPS".to_string()),
            base_currency_name: Some("USDC".to_string()),
            base_currency_code: Some(5),
            server_version: Some(763),
            moonproto_version: Some(3),
        };
        let payload = encode_full(&original);
        let parsed = parse_base_check_response(&payload);
        assert_eq!(parsed, original);
        assert!(parsed.supports(exchange_type_flags::FUTURES));
        assert!(parsed.supports(exchange_type_flags::DEX));
        assert!(!parsed.supports(exchange_type_flags::SPOT));
        assert!(!parsed.supports(exchange_type_flags::PREDICT));
    }

    #[test]
    fn parse_truncated_at_server_name_returns_only_bot_id() {
        // bot_id есть, server_name обрезан в середине строкового заголовка.
        let mut buf = Vec::new();
        buf.extend_from_slice(&(42_i64).to_le_bytes());
        buf.push(0x05); // частичный u16 length для server_name (только 1 байт)
        let info = parse_base_check_response(&buf);
        assert_eq!(info.bot_id, Some(42));
        assert!(info.server_name.is_none());
        assert!(info.exchange_code.is_none());
    }

    #[test]
    fn parse_truncated_at_exchange_code_returns_three_fields() {
        // bot_id + server_name есть, exchange_code (1 байт) обрезан.
        let mut buf = Vec::new();
        buf.extend_from_slice(&(7_i64).to_le_bytes());
        buf.extend_from_slice(&(4u16.to_le_bytes()));
        buf.extend_from_slice(b"name");
        // exchange_code (1 byte) отсутствует.
        let info = parse_base_check_response(&buf);
        assert_eq!(info.bot_id, Some(7));
        assert_eq!(info.server_name.as_deref(), Some("name"));
        assert!(info.exchange_code.is_none());
        assert!(info.exchange_name.is_none());
    }

    #[test]
    fn parse_truncated_at_server_version_keeps_eight_fields() {
        // Восемь полей заполнены, на server_version (i32) данных не хватает.
        let info_partial = ServerInfo {
            bot_id: Some(1),
            server_name: Some("y".to_string()),
            exchange_code: Some(2),
            exchange_name: Some("Bybit".to_string()),
            exchange_type_mask: Some(exchange_type_flags::FUTURES),
            dex_name: Some(String::new()),
            base_currency_name: Some("USD".to_string()),
            base_currency_code: Some(3),
            server_version: None,
            moonproto_version: None,
        };
        let mut payload = encode_full(&info_partial);
        // Добавим обрезанные 2 байта вместо полных 4 для server_version.
        payload.extend_from_slice(&[0xAA, 0xBB]);
        let parsed = parse_base_check_response(&payload);
        assert_eq!(parsed.bot_id, Some(1));
        assert_eq!(parsed.base_currency_code, Some(3));
        assert!(parsed.server_version.is_none());
        assert!(parsed.moonproto_version.is_none());
    }

    #[test]
    fn parse_only_moonproto_version_missing() {
        // Все 9 полей кроме последнего.
        let info_partial = ServerInfo {
            bot_id: Some(0xABC_i64),
            server_name: Some("Test".to_string()),
            exchange_code: Some(4),
            exchange_name: Some("Hyper".to_string()),
            exchange_type_mask: Some(exchange_type_flags::DEX | exchange_type_flags::FUTURES),
            dex_name: Some("DEX-NAME".to_string()),
            base_currency_name: Some("USDC".to_string()),
            base_currency_code: Some(5),
            server_version: Some(763),
            moonproto_version: None,
        };
        let payload = encode_full(&info_partial);
        let parsed = parse_base_check_response(&payload);
        assert_eq!(parsed, info_partial);
        assert!(parsed.has_identity());
        assert!(parsed.moonproto_version.is_none());
    }

    #[test]
    fn parse_predict_market_bit() {
        let info = ServerInfo {
            bot_id: Some(99),
            server_name: Some("HL Predict".to_string()),
            exchange_code: Some(7),
            exchange_name: Some("Hyper".to_string()),
            exchange_type_mask: Some(exchange_type_flags::DEX | exchange_type_flags::PREDICT),
            dex_name: Some(String::new()),
            base_currency_name: Some("USDC".to_string()),
            base_currency_code: Some(5),
            server_version: Some(763),
            moonproto_version: Some(3),
        };
        let parsed = parse_base_check_response(&encode_full(&info));
        assert!(parsed.supports(exchange_type_flags::PREDICT));
        assert!(parsed.supports(exchange_type_flags::DEX));
        assert!(!parsed.supports(exchange_type_flags::FUTURES));
        assert!(!parsed.supports(exchange_type_flags::SPOT));
    }

    #[test]
    fn server_info_default_has_no_identity_and_no_flags() {
        let info = ServerInfo::default();
        assert!(!info.has_identity());
        assert!(!info.supports(exchange_type_flags::SPOT));
        assert!(!info.supports(exchange_type_flags::FUTURES));
    }

    #[test]
    fn parse_zero_length_strings_are_some_empty() {
        // Сервер может явно прислать пустую строку (например `dex_name` для не-HL
        // биржи). `Some("")` отличается от `None`.
        let mut buf = Vec::new();
        buf.extend_from_slice(&(1_i64).to_le_bytes());
        buf.extend_from_slice(&(0u16.to_le_bytes())); // server_name = ""
        let info = parse_base_check_response(&buf);
        assert_eq!(info.bot_id, Some(1));
        assert_eq!(info.server_name.as_deref(), Some(""));
        assert!(info.exchange_code.is_none());
    }

    #[test]
    fn parse_does_not_panic_on_random_garbage() {
        // Стресс: рандом-байты не должны вызвать panic.
        // Delphi-style decoder подменит невалидные байты на '?'.
        let garbage: Vec<u8> = (0..200).map(|i| ((i * 7) ^ 0xA5) as u8).collect();
        let _info = parse_base_check_response(&garbage);
        // Парсер выживает; конкретные значения зависят от random pattern.
    }
}

#[cfg(test)]
mod auth_check_tests {
    use super::*;

    #[test]
    fn parse_minimal_auth_check() {
        // BinanceAccountID(8) + BTCAddress("")(2) + spot_ref(4) + is_sub_account(1) + AccountID("acc")(2+3)
        let mut data = Vec::new();
        data.extend_from_slice(&(123i64).to_le_bytes());
        data.extend_from_slice(&(0u16).to_le_bytes()); // empty BTCAddress
        data.extend_from_slice(&(7i32).to_le_bytes()); // spot_ref
        data.push(1); // is_sub_account=true
        data.extend_from_slice(&(3u16).to_le_bytes()); // AccountID length
        data.extend_from_slice(b"acc");
        let resp = parse_auth_check_response(&data).unwrap();
        assert_eq!(resp.binance_account_id, 123);
        assert_eq!(resp.btc_address, "");
        assert_eq!(resp.spot_ref, 7);
        assert!(resp.is_sub_account);
        assert_eq!(resp.account_id, "acc");
        assert!(resp.recvd_max_payload.is_none());
        assert!(resp.known_dexes.is_empty());
    }

    #[test]
    fn parse_with_dexes() {
        let mut data = Vec::new();
        data.extend_from_slice(&(0i64).to_le_bytes());
        data.extend_from_slice(&(0u16).to_le_bytes());
        data.extend_from_slice(&(0i32).to_le_bytes());
        data.push(0);
        data.extend_from_slice(&(0u16).to_le_bytes());
        // Phase 2:
        data.extend_from_slice(&(1024i32).to_le_bytes()); // recvd_max_payload
        data.push(2); // cnt=2 dexes
                      // Dex 0: name="" + collateral=0 (USDC)
        let mut dex0 = vec![0u8; 18];
        dex0[0] = 0; // name length=0
                     // collateral_token (offset 16..18) = 0 LE → already zero
        data.extend_from_slice(&dex0);
        // Dex 1: name="usdh", collateral=360 (USDH)
        let mut dex1 = vec![0u8; 18];
        dex1[0] = 4; // name length=4
        dex1[1..5].copy_from_slice(b"usdh");
        dex1[16..18].copy_from_slice(&(360u16).to_le_bytes());
        data.extend_from_slice(&dex1);
        data.push(7); // hl_dex_market=7
        data.push(3); // hl_spot_market=3

        let resp = parse_auth_check_response(&data).unwrap();
        assert_eq!(resp.recvd_max_payload, Some(1024));
        assert_eq!(resp.known_dexes.len(), 2);
        assert_eq!(resp.known_dexes[0].collateral_token, 0);
        assert_eq!(resp.known_dexes[1].name, "usdh");
        assert_eq!(resp.known_dexes[1].collateral_token, 360);
        assert_eq!(resp.hl_dex_market, Some(7));
        assert_eq!(resp.hl_spot_market, Some(3));
    }

    #[test]
    fn auth_check_dex_count_keeps_complete_records_on_truncated_tail_like_delphi_loop() {
        let mut data = Vec::new();
        data.extend_from_slice(&(0i64).to_le_bytes());
        data.extend_from_slice(&(0u16).to_le_bytes());
        data.extend_from_slice(&(0i32).to_le_bytes());
        data.push(0);
        data.extend_from_slice(&(0u16).to_le_bytes());
        data.extend_from_slice(&(1024i32).to_le_bytes());
        data.push(2);

        let mut dex0 = vec![0u8; 18];
        dex0[0] = 4;
        dex0[1..5].copy_from_slice(b"usdc");
        dex0[16..18].copy_from_slice(&(0u16).to_le_bytes());
        data.extend_from_slice(&dex0);

        let resp = parse_auth_check_response(&data).unwrap();
        assert_eq!(resp.recvd_max_payload, Some(1024));
        assert_eq!(resp.known_dexes.len(), 1);
        assert_eq!(resp.known_dexes[0].name, "usdc");
        assert_eq!(resp.hl_dex_market, None);
        assert_eq!(resp.hl_spot_market, None);
    }

    #[test]
    fn parse_dex_invalid_utf8_uses_delphi_question_mark_fallback() {
        let mut data = Vec::new();
        data.extend_from_slice(&(0i64).to_le_bytes());
        data.extend_from_slice(&(0u16).to_le_bytes());
        data.extend_from_slice(&(0i32).to_le_bytes());
        data.push(0);
        data.extend_from_slice(&(0u16).to_le_bytes());
        data.extend_from_slice(&(1024i32).to_le_bytes());
        data.push(1);

        let mut dex = vec![0u8; 18];
        dex[0] = 3;
        dex[1..4].copy_from_slice(&[b'd', 0xFF, b'x']);
        dex[16..18].copy_from_slice(&(7u16).to_le_bytes());
        data.extend_from_slice(&dex);

        let resp = parse_auth_check_response(&data).unwrap();
        assert_eq!(resp.known_dexes[0].name, "d?x");
        assert_eq!(resp.known_dexes[0].collateral_token, 7);
    }
}
