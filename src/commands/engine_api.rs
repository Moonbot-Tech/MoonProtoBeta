/// MPC_API (Engine RPC) — byte-exact port of MoonProtoEngineStruct.pas.
/// Source: MoonProtoEngineStruct.pas:364-403 (TEngineResponse.CreateFromStream)
///
/// Request: client → server (CmdId=002)
/// Response: server → client (CmdId=001)

use super::registry::{read_string};
use flate2::read::DeflateDecoder;
use std::io::Read;

/// Engine RPC method identifiers — 31 метод торгового API.
///
/// Каждый метод имеет соответствующий builder в [`super::engine_request`] (например,
/// `build_engine_request` + специализированные функции) и Client-обёртку
/// `Client::api_<method>()` (см. `moonproto::client::Client`). Большинство возвращают
/// `mpsc::Receiver<EngineResponse>` для async-обработки через pending registry.
///
/// **Формат `EngineResponse::data`** различается per-метод — описан рядом с каждым
/// variant'ом ниже. Парсеры для специфичных форматов — в соответствующих модулях
/// (`commands::markets`, `commands::candles`, `commands::orders`).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineMethod {
    /// Нулевой/неизвестный метод. Возвращается из `from_byte` при unknown id (server
    /// прислал метод которого нет в порте — server-side extension).
    None = 0,
    /// `BaseCheck` — sanity check связи с engine. Без параметров. Ответ: success/fail
    /// без data. Обычно вызывается первым после Authenticated.
    BaseCheck = 1,
    /// `AuthCheck` — проверка прав API-ключа на бирже (ping биржевого API через engine).
    /// Без параметров. Ответ: success если ключи валидны.
    AuthCheck = 2,
    /// `GetMarketsList` — список всех торгуемых рынков (для futures+spot). Без параметров.
    /// Ответ: data содержит `TMarket` записи в формате
    /// [`commands::markets::parse_markets_list_response`].
    GetMarketsList = 3,
    /// `UpdateMarketsList` — запросить дельту изменений рынков с последнего вызова.
    /// Ответ парсится тем же [`commands::markets::parse_markets_list_response`].
    /// Используется для инкрементальной синхронизации больших списков (deflate compressed).
    UpdateMarketsList = 4,
    /// `GetMarketsIndexes` — компактный mapping `market_name → market_index` (u16).
    /// Без параметров. Ответ: пары strings+indexes, используется в TradesStream для
    /// декодирования market_index в имя.
    GetMarketsIndexes = 5,
    /// `GetBalance` — текущий баланс по конкретной валюте. Параметр: `currency` (string).
    /// Ответ: float баланс + locked + другие поля валюты.
    GetBalance = 6,
    /// `GetMarketsBalanceFull` — полный balance snapshot по всем рынкам (`TBalanceItem`
    /// записи). Без параметров. Используется для initial sync после Authenticated.
    GetMarketsBalanceFull = 7,
    /// `GetOrder` — детали конкретного ордера. Параметр: `order_uid` (u64).
    /// Ответ: полный `OrderCompact` (117 байт wire-format).
    GetOrder = 8,
    /// `GetOpenOrders` — все открытые (non-terminal) ордера. Без параметров. Ответ:
    /// массив `OrderCompact`.
    GetOpenOrders = 9,
    /// `GetActiveOrders` — ордера с активным state-machine (фильтр PartiallyFilled
    /// и т.д., в отличие от GetOpenOrders). Ответ: массив `OrderCompact`.
    GetActiveOrders = 10,
    /// `CancelAllOrders` — отменить все открытые ордера. Без параметров. Опасная команда.
    CancelAllOrders = 11,
    /// `SetLeverage` — установить leverage для рынка. Параметры: `market` (string),
    /// `new_lev` (i32). Не все биржи поддерживают.
    SetLeverage = 12,
    /// `SetHedgeMode` — включить/выключить hedge mode (Binance Futures). Параметр: bool.
    SetHedgeMode = 13,
    /// `QueryHedgeMode` — текущий статус hedge mode. Без параметров. Ответ: bool.
    QueryHedgeMode = 14,
    /// `CheckAPIExpirationTime` — срок действия API ключа на бирже. Без параметров.
    /// Ответ: timestamp + дни до истечения. Полезно для UI warning'а.
    CheckAPIExpirationTime = 15,
    /// `CheckBinanceTags` — verification Binance API tags (futures permissions, etc.).
    /// Specific Binance debug. Без параметров.
    CheckBinanceTags = 16,
    /// `TradesResend` — запросить повторную отправку trades batch'ей по номерам пакетов.
    /// Параметры: массив `packet_nums` (u16). Используется для gap recovery в TradesStream.
    /// См. [`commands::trades_stream`] gap detection.
    TradesResend = 17,
    /// `SubscribeAllTrades` — подписаться на весь поток сделок. Без параметров. После
    /// этого сервер начинает слать `MPC_TradesStream` пакеты. Обычно вызывается на
    /// `LifecycleEvent::Authenticated`.
    SubscribeAllTrades = 18,
    /// `UnsubscribeAllTrades` — отписаться. После — TradesStream больше не приходит.
    UnsubscribeAllTrades = 19,
    /// `SubscribeOrderBook` — подписаться на orderbook конкретных рынков. Параметр:
    /// массив `markets` (strings). После — приходят `MPC_OrderBook` пакеты (Full
    /// snapshot + Diff'ы).
    SubscribeOrderBook = 20,
    /// `UnsubscribeOrderBook` — отписаться от orderbook. Параметр: массив рынков.
    UnsubscribeOrderBook = 21,
    /// `RequestOrderBookFull` — запросить полный snapshot orderbook'а конкретного рынка.
    /// Параметры: `market_idx` (u16), `book_kind` (u8). Используется при gap recovery
    /// (потеря Diff пакетов → запрос Full для пересинхронизации).
    RequestOrderBookFull = 22,
    /// `ReloadOrderBook` — принудительно пересоздать все подписанные order books. Без
    /// параметров. Сервер пришлёт свежие snapshot'ы.
    ReloadOrderBook = 23,
    /// `RequestCandlesData` — запросить исторические свечи для рынка/таймфрейма.
    /// Ответ — **chunked** (несколько `EngineResponse` пакетов с одним и тем же UID).
    /// Pending registry не подходит — используй обычный `on_data` callback +
    /// [`commands::candles::CandlesAggregator::on_chunk`] для сборки.
    RequestCandlesData = 24,
    /// `ChangePositionType` — сменить тип позиции (isolated ↔ cross). Параметры:
    /// `market` (string), `pos_type` (u8 — 0=isolated, 1=cross), `new_market` (bool).
    ChangePositionType = 25,
    /// `ConvertDustBNB` — Binance-specific: конвертировать пыль на BNB. Без параметров.
    ConvertDustBNB = 26,
    /// `ConfirmRiskLimit` — подтвердить risk limit для рынка (Bybit-specific обычно).
    /// Параметр: `market` (string).
    ConfirmRiskLimit = 27,
    /// `SetMAMode` — Multi-Assets mode (Binance Futures: разрешить использовать USDT,
    /// USDC и BFUSD как залог). Параметр: bool.
    SetMAMode = 28,
    /// `DoTransferAsset` — перевод актива между sub-account'ами / wallet'ами биржи.
    /// Параметры: `asset` (string), `qty` (f64), `from` (u8 — wallet id), `to` (u8).
    DoTransferAsset = 29,
    /// `UpdateTransferAssets` — пересчитать список доступных для перевода активов.
    /// Параметр: `kind` (u8 — направление перевода).
    UpdateTransferAssets = 30,
    /// `GetCoinCardCandles` — короткая история свечей для coin-card UI компонента.
    /// Параметры: `market` (string), `ticks` (DeepHistoryKind enum). Использует
    /// специализированный парсер [`commands::candles::parse_coin_card_candles_response`].
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
            // DoS protection: cap decompressed size (anti-bomb).
            // Engine API responses (markets list, balances, candles) реально 1-5 MB.
            // 64 MB — щедрый запас, закрывает adversarial expansion.
            use std::io::Read;
            const MAX_ENGINE_RESPONSE_BYTES: u64 = 64 * 1024 * 1024;
            let mut decoder = DeflateDecoder::new(raw).take(MAX_ENGINE_RESPONSE_BYTES);
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
        request_uid,
        method,
        success,
        error_code,
        error_msg,
        data: response_data,
    })
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
/// let resp = rx.recv_timeout(Duration::from_secs(10))?;
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
///
/// Byte-exact с Delphi: `MoonProtoEngine.pas:610-633`.
pub fn parse_auth_check_response(data: &[u8]) -> Option<AuthCheckResponse> {
    let mut pos = 0usize;

    // Required fields.
    if data.len() < 8 { return None; }
    let binance_account_id = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
    pos += 8;

    let btc_address = read_string(data, &mut pos)?;

    if pos + 4 > data.len() { return None; }
    let spot_ref = i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
    pos += 4;

    if pos + 1 > data.len() { return None; }
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

    if recvd_max_payload.is_some() && pos + 1 <= data.len() {
        let cnt = data[pos] as usize;
        pos += 1;
        // DoS guard: каждый DEX = 18 байт, cnt * 18 не должно превышать remaining.
        const DEX_INFO_SIZE: usize = 18;
        if cnt.saturating_mul(DEX_INFO_SIZE) > data.len().saturating_sub(pos) {
            log::warn!(target: "moonproto::engine_api",
                "AuthCheck: dex count {} requires {} bytes but only {} remain",
                cnt, cnt * DEX_INFO_SIZE, data.len() - pos);
            return None;
        }
        known_dexes.reserve(cnt);
        for _ in 0..cnt {
            // THLDexInfo packed: [u8 length][15 bytes name][u16 collateral_token]
            let name_len = data[pos] as usize;
            // Защита: name_len по контракту ≤ 15. Если больше — corrupt, используем 15.
            let effective_len = name_len.min(15);
            let name_bytes = &data[pos + 1..pos + 1 + effective_len];
            let name = String::from_utf8_lossy(name_bytes).into_owned();
            let collateral_token = u16::from_le_bytes([data[pos + 16], data[pos + 17]]);
            pos += DEX_INFO_SIZE;
            known_dexes.push(DexInfo { name, collateral_token });
        }
        // hl_dex_market и hl_spot_market следуют сразу после массива.
        if pos + 1 <= data.len() {
            hl_dex_market = Some(data[pos]);
            pos += 1;
            if pos + 1 <= data.len() {
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

#[cfg(test)]
mod auth_check_tests {
    use super::*;

    #[test]
    fn parse_minimal_auth_check() {
        // BinanceAccountID(8) + BTCAddress("")(2) + spot_ref(4) + is_sub_account(1) + AccountID("acc")(2+3)
        let mut data = Vec::new();
        data.extend_from_slice(&(123i64).to_le_bytes());
        data.extend_from_slice(&(0u16).to_le_bytes());           // empty BTCAddress
        data.extend_from_slice(&(7i32).to_le_bytes());           // spot_ref
        data.push(1);                                            // is_sub_account=true
        data.extend_from_slice(&(3u16).to_le_bytes());           // AccountID length
        data.extend_from_slice(b"acc");
        let resp = parse_auth_check_response(&data).unwrap();
        assert_eq!(resp.binance_account_id, 123);
        assert_eq!(resp.btc_address, "");
        assert_eq!(resp.spot_ref, 7);
        assert_eq!(resp.is_sub_account, true);
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
        data.extend_from_slice(&(1024i32).to_le_bytes());        // recvd_max_payload
        data.push(2);                                            // cnt=2 dexes
        // Dex 0: name="" + collateral=0 (USDC)
        let mut dex0 = vec![0u8; 18];
        dex0[0] = 0;                                              // name length=0
        // collateral_token (offset 16..18) = 0 LE → already zero
        data.extend_from_slice(&dex0);
        // Dex 1: name="usdh", collateral=360 (USDH)
        let mut dex1 = vec![0u8; 18];
        dex1[0] = 4;                                              // name length=4
        dex1[1..5].copy_from_slice(b"usdh");
        dex1[16..18].copy_from_slice(&(360u16).to_le_bytes());
        data.extend_from_slice(&dex1);
        data.push(7);                                            // hl_dex_market=7
        data.push(3);                                            // hl_spot_market=3

        let resp = parse_auth_check_response(&data).unwrap();
        assert_eq!(resp.recvd_max_payload, Some(1024));
        assert_eq!(resp.known_dexes.len(), 2);
        assert_eq!(resp.known_dexes[0].collateral_token, 0);
        assert_eq!(resp.known_dexes[1].name, "usdh");
        assert_eq!(resp.known_dexes[1].collateral_token, 360);
        assert_eq!(resp.hl_dex_market, Some(7));
        assert_eq!(resp.hl_spot_market, Some(3));
    }
}
