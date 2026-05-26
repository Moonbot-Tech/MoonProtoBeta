//! `emk_BaseCheck` response parser.

use super::read_string;

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
