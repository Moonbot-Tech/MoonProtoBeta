/// MPC_API (Engine RPC) — byte-exact port of MoonProtoEngineStruct.pas.
/// Source: MoonProtoEngineStruct.pas:364-403 (TEngineResponse.CreateFromStream)
///
/// Request: client → server (CmdId=002)
/// Response: server → client (CmdId=001)

use super::registry::{read_string};
use flate2::read::DeflateDecoder;
use std::io::Read;

/// Engine RPC method identifiers
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineMethod {
    None = 0,
    BaseCheck = 1,
    AuthCheck = 2,
    GetMarketsList = 3,
    UpdateMarketsList = 4,
    GetMarketsIndexes = 5,
    GetBalance = 6,
    GetMarketsBalanceFull = 7,
    GetOrder = 8,
    GetOpenOrders = 9,
    GetActiveOrders = 10,
    CancelAllOrders = 11,
    SetLeverage = 12,
    SetHedgeMode = 13,
    QueryHedgeMode = 14,
    CheckAPIExpirationTime = 15,
    CheckBinanceTags = 16,
    TradesResend = 17,
    SubscribeAllTrades = 18,
    UnsubscribeAllTrades = 19,
    SubscribeOrderBook = 20,
    UnsubscribeOrderBook = 21,
    RequestOrderBookFull = 22,
    ReloadOrderBook = 23,
    RequestCandlesData = 24,
    ChangePositionType = 25,
    ConvertDustBNB = 26,
    ConfirmRiskLimit = 27,
    SetMAMode = 28,
    DoTransferAsset = 29,
    UpdateTransferAssets = 30,
    GetCoinCardCandles = 31,
}

impl EngineMethod {
    /// `EngineMethod` имеет типизированный `None` вариант (=`0`) — сохраняем как есть
    /// (не `Option<Self>` поскольку None — известный факт). При неизвестном byte > 0
    /// логируем warn — это означает что server добавил новый метод которого нет в порте (A-02).
    pub fn from_byte(b: u8) -> Self {
        match b {
            1 => Self::BaseCheck,
            2 => Self::AuthCheck,
            3 => Self::GetMarketsList,
            4 => Self::UpdateMarketsList,
            5 => Self::GetMarketsIndexes,
            6 => Self::GetBalance,
            7 => Self::GetMarketsBalanceFull,
            8 => Self::GetOrder,
            9 => Self::GetOpenOrders,
            10 => Self::GetActiveOrders,
            11 => Self::CancelAllOrders,
            12 => Self::SetLeverage,
            13 => Self::SetHedgeMode,
            14 => Self::QueryHedgeMode,
            15 => Self::CheckAPIExpirationTime,
            16 => Self::CheckBinanceTags,
            17 => Self::TradesResend,
            18 => Self::SubscribeAllTrades,
            19 => Self::UnsubscribeAllTrades,
            20 => Self::SubscribeOrderBook,
            21 => Self::UnsubscribeOrderBook,
            22 => Self::RequestOrderBookFull,
            23 => Self::ReloadOrderBook,
            24 => Self::RequestCandlesData,
            25 => Self::ChangePositionType,
            26 => Self::ConvertDustBNB,
            27 => Self::ConfirmRiskLimit,
            28 => Self::SetMAMode,
            29 => Self::DoTransferAsset,
            30 => Self::UpdateTransferAssets,
            31 => Self::GetCoinCardCandles,
            0 => Self::None,
            _ => {
                log::warn!(target: "moonproto::engine_api", "unknown EngineMethod byte: {b} (server-side extension?)");
                Self::None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_method_known_bytes() {
        assert_eq!(EngineMethod::from_byte(1), EngineMethod::BaseCheck);
        assert_eq!(EngineMethod::from_byte(31), EngineMethod::GetCoinCardCandles);
        assert_eq!(EngineMethod::from_byte(0), EngineMethod::None);
    }

    #[test]
    fn engine_method_unknown_falls_to_none_with_warn() {
        // Не can directly assert warn вывод без логгер-impl,
        // но проверяем что значение fallback на None.
        assert_eq!(EngineMethod::from_byte(99), EngineMethod::None);
        assert_eq!(EngineMethod::from_byte(255), EngineMethod::None);
    }
}

/// Parsed Engine Response (server → client)
#[derive(Debug, Clone)]
pub struct EngineResponse {
    pub request_uid: u64,
    pub method: EngineMethod,
    pub success: bool,
    pub error_code: i32,
    pub error_msg: String,
    pub data: Vec<u8>,  // decompressed response payload
}

/// Parse TEngineResponse from command payload (after CmdId+ver+UID header).
/// Matches MoonProtoEngineStruct.pas:364-403.
pub fn parse_engine_response(data: &[u8]) -> Option<EngineResponse> {
    let mut pos = 0usize;

    if pos + 8 > data.len() { return None; }
    let request_uid = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
    pos += 8;

    if pos + 1 > data.len() { return None; }
    let method = EngineMethod::from_byte(data[pos]);
    pos += 1;

    if pos + 1 > data.len() { return None; }
    let success = data[pos] != 0;
    pos += 1;

    if pos + 4 > data.len() { return None; }
    let error_code = i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
    pos += 4;

    let error_msg = read_string(data, &mut pos).unwrap_or_default();

    // IsCompressed + data size
    if pos + 1 > data.len() { return Some(EngineResponse { request_uid, method, success, error_code, error_msg, data: Vec::new() }); }
    let is_compressed = data[pos] != 0;
    pos += 1;

    if pos + 4 > data.len() { return Some(EngineResponse { request_uid, method, success, error_code, error_msg, data: Vec::new() }); }
    let sz = i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;

    let response_data = if sz > 0 && pos + sz <= data.len() {
        let raw = &data[pos..pos + sz];
        if is_compressed {
            let mut decoder = DeflateDecoder::new(raw);
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed).unwrap_or(0);
            decompressed
        } else {
            raw.to_vec()
        }
    } else {
        Vec::new()
    };

    Some(EngineResponse {
        request_uid,
        method,
        success,
        error_code,
        error_msg,
        data: response_data,
    })
}
