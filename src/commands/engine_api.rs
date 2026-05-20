/// MPC_API (Engine RPC) — byte-exact port of MoonProtoEngineStruct.pas.
/// Source: MoonProtoEngineStruct.pas:364-403 (TEngineResponse.CreateFromStream)
///
/// Request: client → server (CmdId=002)
/// Response: server → client (CmdId=001)

use super::registry::{read_string};
use flate2::read::DeflateDecoder;

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
    // Skip Engine TBaseCommand header: CmdId(1) + ver(2) + own_UID(8) = 11 bytes.
    let mut pos = 11usize;

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
/// let resp = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(10))?;
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
/// let resp = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(10))?;
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
    if pos + 8 > data.len() { return info; }
    info.bot_id = Some(i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()));
    pos += 8;

    // 2. server_name (string)
    match read_string(data, &mut pos) {
        Some(s) => info.server_name = Some(s),
        None => return info,
    }

    // 3. exchange_code (u8) — Ord(cfg.Header.Current)
    if pos + 1 > data.len() { return info; }
    info.exchange_code = Some(data[pos]);
    pos += 1;

    // 4. exchange_name (string)
    match read_string(data, &mut pos) {
        Some(s) => info.exchange_name = Some(s),
        None => return info,
    }

    // 5. exchange_type_mask (u8)
    if pos + 1 > data.len() { return info; }
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
    if pos + 1 > data.len() { return info; }
    info.base_currency_code = Some(data[pos]);
    pos += 1;

    // 9. server_version (i32 LE → u32) — Delphi WriteInt(Current_Version_Num_X).
    // Trust серверу: значения версий монотонно растут с малых положительных
    // чисел (например 763 для v7.63), поэтому signed/unsigned различие не влияет.
    if pos + 4 > data.len() { return info; }
    info.server_version = Some(i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as u32);
    pos += 4;

    // 10. moonproto_version (i32 LE → u32)
    if pos + 4 > data.len() { return info; }
    info.moonproto_version = Some(i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as u32);
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
        buf.push(1u8);                                              // CmdId = 1
        buf.extend_from_slice(&3u16.to_le_bytes());                 // ver = 3
        buf.extend_from_slice(&own_uid.to_le_bytes());              // own_UID
        buf.extend_from_slice(&request_uid.to_le_bytes());          // RequestUID (echo)
        buf.push(method as u8);                                     // Method
        buf.push(success as u8);                                    // Success
        buf.extend_from_slice(&error_code.to_le_bytes());           // ErrorCode
        buf.extend_from_slice(&(error_msg.len() as u16).to_le_bytes()); // ErrorMsg len
        buf.extend_from_slice(error_msg.as_bytes());
        buf.push(0u8);                                              // IsCompressed = false
        buf.extend_from_slice(&(data.len() as i32).to_le_bytes());  // DataSize
        buf.extend_from_slice(data);
        buf
    }

    #[test]
    fn parse_skips_basecmd_header_and_reads_request_uid_correctly() {
        // Регрессия от critical bug: парсер ДО fix начинал с offset 0
        // и читал request_uid из `[CmdId][ver][own_UID 5 bytes]` — garbage.
        let request_uid = 0x12_34_56_78_9A_BC_DE_F0u64;
        let payload = build_wire_response(
            0xAAAA_BBBB_CCCC_DDDD,         // own_UID (random)
            request_uid,                    // RequestUID (echo)
            EngineMethod::BaseCheck,
            true,
            0,
            "",
            &[],
        );
        let resp = parse_engine_response(&payload).expect("parse ok");
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
    fn parse_carries_error_payload() {
        let payload = build_wire_response(
            1, 42,
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
    fn parse_carries_uncompressed_data() {
        let blob = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x12, 0x34, 0x56, 0x78];
        let payload = build_wire_response(0, 100, EngineMethod::GetMarketsList, true, 0, "", &blob);
        let resp = parse_engine_response(&payload).expect("parse ok");
        assert_eq!(resp.data, blob);
        assert_eq!(resp.method, EngineMethod::GetMarketsList);
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
        let Some(name) = &info.server_name else { return buf };
        buf.extend_from_slice(&(name.len() as u16).to_le_bytes());
        buf.extend_from_slice(name.as_bytes());
        let Some(ex_code) = info.exchange_code else { return buf };
        buf.push(ex_code);
        let Some(ex_name) = &info.exchange_name else { return buf };
        buf.extend_from_slice(&(ex_name.len() as u16).to_le_bytes());
        buf.extend_from_slice(ex_name.as_bytes());
        let Some(mask) = info.exchange_type_mask else { return buf };
        buf.push(mask);
        let Some(dex) = &info.dex_name else { return buf };
        buf.extend_from_slice(&(dex.len() as u16).to_le_bytes());
        buf.extend_from_slice(dex.as_bytes());
        let Some(bc_name) = &info.base_currency_name else { return buf };
        buf.extend_from_slice(&(bc_name.len() as u16).to_le_bytes());
        buf.extend_from_slice(bc_name.as_bytes());
        let Some(bc_code) = info.base_currency_code else { return buf };
        buf.push(bc_code);
        let Some(sv) = info.server_version else { return buf };
        buf.extend_from_slice(&(sv as i32).to_le_bytes());
        let Some(mp) = info.moonproto_version else { return buf };
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
            server_version: Some(763),    // v7.63
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
    fn parse_hl_futures_with_hip3_dex_name() {
        // Hyperliquid futures с HIP-3 dex — все 4 типа в mask + непустой dex_name.
        let original = ServerInfo {
            bot_id: Some(42),
            server_name: Some("Hyper Test".to_string()),
            exchange_code: Some(7),
            exchange_name: Some("Hyper".to_string()),
            exchange_type_mask: Some(
                exchange_type_flags::FUTURES | exchange_type_flags::DEX,
            ),
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
            exchange_type_mask: Some(
                exchange_type_flags::DEX | exchange_type_flags::FUTURES,
            ),
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
            exchange_type_mask: Some(
                exchange_type_flags::DEX | exchange_type_flags::PREDICT,
            ),
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
        // (utf8_lossy подменит невалидные байты на U+FFFD — это OK для UI.)
        let garbage: Vec<u8> = (0..200).map(|i| (i * 7 ^ 0xA5) as u8).collect();
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
