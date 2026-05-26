//! `emk_AuthCheck` response parser.

use super::{read_i32_zero_tail, read_string};
use crate::commands::registry::decode_utf8_delphi;
use zerocopy::byteorder::little_endian::U16 as LeU16;
use zerocopy::{FromBytes, Immutable, KnownLayout, Unaligned};

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
        // Защита: name_len по контракту <= 15. Если больше - corrupt, используем 15.
        let effective_len = name_len.min(15);
        let name_bytes = &wire.short_string_name[1..1 + effective_len];
        Self {
            name: decode_utf8_delphi(name_bytes),
            collateral_token: wire.collateral_token.get(),
        }
    }
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
#[derive(Debug, Clone, PartialEq)]
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
/// `SetLength(KnownDexes, cnt)` creates zero-filled records, and each
/// `TMemoryStream.Read` partially overwrites one 18-byte `THLDexInfo` slot.
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
    let recvd_max_payload = if pos < data.len() {
        Some(read_i32_zero_tail(data, &mut pos))
    } else {
        None
    };

    let mut known_dexes: Vec<DexInfo> = Vec::new();
    let mut hl_dex_market: Option<u8> = None;
    let mut hl_spot_market: Option<u8> = None;

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
