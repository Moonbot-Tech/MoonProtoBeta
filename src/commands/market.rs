//! Market канал — парсеры ответов Engine API связанных с маркетами.
//!
//! Источник Delphi: `MoonProto/MoonProtoSerialization.pas` + `MoonProto/MoonProtoEngineServer.pas`.
//!
//! ## Что покрыто
//! - `parse_markets_list_response` — для `emk_GetMarketsList` (полный список маркетов + CorrMarkets).
//! - `parse_markets_prices_response` — для `emk_UpdateMarketsList` (обновление Bid/Ask/Funding/MarkPrice).
//! - `parse_markets_indexes_response` — для `emk_GetMarketsIndexes` (список названий маркетов).
//! - `parse_token_tags_response` — для `emk_CheckBinanceTags` (теги монет).
//!
//! Все парсеры принимают `data: &[u8]` (содержимое `EngineResponse.data` после Deflate-декомпрессии).
//!
//! ## Wire-form примитивов TEngineResponse (Engine RPC stream)
//! - `WriteDouble`: 8 байт LE
//! - `WriteInt`: 4 байта LE i32
//! - `WriteWord`: 2 байта LE u16
//! - `WriteByte`: 1 байт u8
//! - `WriteInt64`: 8 байт LE i64
//! - `WriteBool`: 1 байт (0=false, иначе true)
//! - `WriteStr`: u16 LE prefix + UTF-8 bytes (как `registry::write_string`)

use super::candles::current_local_time_shift_minutes;
const MINS_IN_DAY: f64 = 1440.0;

mod reader;
mod token_tags;
pub use self::reader::EngineStreamReader;
pub use self::token_tags::{
    build_token_tags_response, parse_token_tags_response, MarketTokenTags, TokenTags,
};

// =============================================================================
//  TBaseCurrency ordinal (Vars.pas:40)
// =============================================================================

/// `TBaseCurrency` — raw ordinal of Delphi enum from `Vars.pas:40`.
///
/// Delphi stores this field as a one-byte enum ordinal and `WriteMarketToStream`
/// writes `Ord(m.FuturesType)`. Keep the raw byte instead of collapsing unknown
/// future ordinals to `BC_Unknown`, so parse + write preserves the exact wire
/// value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BaseCurrency(pub u8);

impl BaseCurrency {
    pub const BTC: Self = Self(0);
    pub const USDT: Self = Self(1);
    pub const ETH: Self = Self(2);
    pub const BNB: Self = Self(3);
    pub const AUD: Self = Self(4);
    pub const TUSD: Self = Self(5);
    pub const BRL: Self = Self(6);
    pub const USDH: Self = Self(7);
    pub const USDC: Self = Self(8);
    pub const FDUSD: Self = Self(9);
    pub const AEUR: Self = Self(10);
    pub const USD: Self = Self(11);
    pub const TRX: Self = Self(12);
    pub const RUB: Self = Self(13);
    pub const EUR: Self = Self(14);
    pub const HTX: Self = Self(15);
    pub const USDD: Self = Self(16);
    pub const IDR: Self = Self(17);
    pub const DOGE: Self = Self(18);
    pub const TRY: Self = Self(19);
    pub const USDE: Self = Self(20);
    pub const NEXT2: Self = Self(21);
    pub const NEXT3: Self = Self(22);
    pub const NEXT4: Self = Self(23);
    pub const NEXT5: Self = Self(24);
    pub const EMPTY: Self = Self(25);
    pub const UNKNOWN: Self = Self(26);

    #[allow(non_upper_case_globals)]
    pub const Next2: Self = Self::NEXT2;
    #[allow(non_upper_case_globals)]
    pub const Next3: Self = Self::NEXT3;
    #[allow(non_upper_case_globals)]
    pub const Next4: Self = Self::NEXT4;
    #[allow(non_upper_case_globals)]
    pub const Next5: Self = Self::NEXT5;
    #[allow(non_upper_case_globals)]
    pub const Unknown: Self = Self::UNKNOWN;

    pub const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    pub const fn to_byte(self) -> u8 {
        self.0
    }
}

// =============================================================================
//  TListedOnExchange ordinal (Vars.pas:58)
// =============================================================================

/// Delphi `TListedOnExchange` raw ordinal from `Vars.pas`.
///
/// This value is not sent in `WriteMarketToStream`. Delphi derives
/// `TMarket.ListedType` after `GetMarketsList` from `FuturesType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ListedType(pub u8);

impl ListedType {
    pub const UNKNOWN: Self = Self(0);
    pub const SPOT: Self = Self(1);
    pub const FUTURES: Self = Self(2);
    pub const BOTH: Self = Self(3);

    #[allow(non_upper_case_globals)]
    pub const Unknown: Self = Self::UNKNOWN;
    #[allow(non_upper_case_globals)]
    pub const Spot: Self = Self::SPOT;
    #[allow(non_upper_case_globals)]
    pub const Futures: Self = Self::FUTURES;
    #[allow(non_upper_case_globals)]
    pub const Both: Self = Self::BOTH;
}

// =============================================================================
//  Market struct (42 поля)
// =============================================================================

/// Полная информация о маркете, byte-exact с `WriteMarketToStream`
/// (MoonProtoSerialization.pas:42-98). 10 strings + 6 ints + 1 int64 + 20 doubles
/// + 5 bools + 1 byte (v2 FuturesType).
#[derive(Debug, Clone, PartialEq)]
pub struct Market {
    // --- Strings (10) ---
    pub bn_market_name: String,
    pub market_currency: String,
    pub bn_market_currency: String,
    pub base_currency: String,
    pub market_currency_long: String,
    pub market_currency_canonic: String,
    pub market_name: String,
    pub market_name_mb_classic: String,
    pub bn_status: String,
    pub leading1000: String,
    // --- Integers (6) ---
    pub bn_price_precision: i32,
    pub bn_quantity_precision: i32,
    pub max_leverage: i32,
    pub k1000: i32,
    pub bn_iceberg_parts: i32,
    pub bn_margin_table_id: i32,
    // --- Int64 (1) ---
    pub bn_delivery_time: i64,
    // --- Doubles (20) ---
    pub bn_tick_size: f64,
    pub bn_step_size: f64,
    pub bn_min_qty: f64,
    pub bn_max_qty: f64,
    pub bn_min_notional: f64,
    pub bn_max_notional: f64,
    pub bn_contract_size: f64,
    pub bn_min_price: f64,
    pub bn_max_price: f64,
    pub bn_max_value: f64,
    pub bn_multiplier_up: f64,
    pub bn_multiplier_down: f64,
    pub bid_multiplier_up: f64,
    pub bid_multiplier_down: f64,
    pub ask_multiplier_up: f64,
    pub ask_multiplier_down: f64,
    pub int_bn_max_qty: f64,
    pub funding_rate: f64,
    pub funding_time: f64,
    pub volume: f64,
    // --- Booleans (5) ---
    pub is_btc_market: bool,
    pub status_trading: bool,
    pub bn_is_fucking_shib: bool,
    pub bn_iceberg: bool,
    pub bn_only_isolated: bool,
    // --- v2: FuturesType ---
    pub futures_type: BaseCurrency,
}

impl Market {
    /// Delphi `GetMarketsList` post-pass:
    /// `FuturesType <> BC_EMPTY -> L_Both`, otherwise `L_Spot`.
    pub fn listed_type_like_delphi(&self) -> ListedType {
        if self.futures_type == BaseCurrency::EMPTY {
            ListedType::SPOT
        } else {
            ListedType::BOTH
        }
    }
}

/// Прочитать `TMarket` из EngineStreamReader (byte-exact с `ReadMarketFromStream`).
/// `ver` — версия команды `TEngineResponse` (если >= 2 — есть FuturesType byte).
pub fn read_market(r: &mut EngineStreamReader, ver: u16) -> Option<Market> {
    read_market_with_local_shift(r, ver, current_local_time_shift_minutes())
}

pub(crate) fn read_market_with_local_shift(
    r: &mut EngineStreamReader,
    ver: u16,
    local_shift_minutes: f64,
) -> Option<Market> {
    let bn_market_name = r.read_str()?;
    let market_currency = r.read_str()?;
    let bn_market_currency = r.read_str()?;
    let base_currency = r.read_str()?;
    let market_currency_long = r.read_str()?;
    let market_currency_canonic = r.read_str()?;
    let market_name = r.read_str()?;
    let mut market_name_mb_classic = r.read_str()?;
    let bn_status = r.read_str()?;
    let leading1000 = r.read_str()?;

    let bn_price_precision = r.read_int()?;
    let bn_quantity_precision = r.read_int()?;
    let max_leverage = r.read_int()?;
    let k1000 = r.read_int()?;
    let bn_iceberg_parts = r.read_int()?;
    let bn_margin_table_id = r.read_int()?;

    let bn_delivery_time = r.read_int64()?;

    let bn_tick_size = r.read_double()?;
    let bn_step_size = r.read_double()?;
    let bn_min_qty = r.read_double()?;
    let bn_max_qty = r.read_double()?;
    let bn_min_notional = r.read_double()?;
    let bn_max_notional = r.read_double()?;
    let bn_contract_size = r.read_double()?;
    let bn_min_price = r.read_double()?;
    let bn_max_price = r.read_double()?;
    let bn_max_value = r.read_double()?;
    let bn_multiplier_up = r.read_double()?;
    let bn_multiplier_down = r.read_double()?;
    let bid_multiplier_up = r.read_double()?;
    let bid_multiplier_down = r.read_double()?;
    let ask_multiplier_up = r.read_double()?;
    let ask_multiplier_down = r.read_double()?;
    let int_bn_max_qty = r.read_double()?;
    let funding_rate = r.read_double()?;
    let funding_time = apply_delphi_local_funding_shift(r.read_double()?, local_shift_minutes);
    let volume = r.read_double()?;

    let is_btc_market = r.read_bool()?;
    let status_trading = r.read_bool()?;
    let bn_is_fucking_shib = r.read_bool()?;
    let bn_iceberg = r.read_bool()?;
    let bn_only_isolated = r.read_bool()?;

    let futures_type = if ver >= 2 {
        BaseCurrency::from_byte(r.read_byte()?)
    } else {
        // Delphi starts from `TMarket.CreateBase`; v1 payload has no
        // FuturesType byte, so the constructor default `BC_EMPTY` remains.
        BaseCurrency::EMPTY
    };

    // Backfill MBClassic (см. ReadMarketFromStream MoonProtoSerialization.pas:160).
    if market_name_mb_classic.is_empty() {
        market_name_mb_classic = market_name.clone();
    }

    Some(Market {
        bn_market_name,
        market_currency,
        bn_market_currency,
        base_currency,
        market_currency_long,
        market_currency_canonic,
        market_name,
        market_name_mb_classic,
        bn_status,
        leading1000,
        bn_price_precision,
        bn_quantity_precision,
        max_leverage,
        k1000,
        bn_iceberg_parts,
        bn_margin_table_id,
        bn_delivery_time,
        bn_tick_size,
        bn_step_size,
        bn_min_qty,
        bn_max_qty,
        bn_min_notional,
        bn_max_notional,
        bn_contract_size,
        bn_min_price,
        bn_max_price,
        bn_max_value,
        bn_multiplier_up,
        bn_multiplier_down,
        bid_multiplier_up,
        bid_multiplier_down,
        ask_multiplier_up,
        ask_multiplier_down,
        int_bn_max_qty,
        funding_rate,
        funding_time,
        volume,
        is_btc_market,
        status_trading,
        bn_is_fucking_shib,
        bn_iceberg,
        bn_only_isolated,
        futures_type,
    })
}

pub(crate) fn apply_delphi_local_funding_shift(
    wire_funding_time: f64,
    local_shift_minutes: f64,
) -> f64 {
    if wire_funding_time > 0.0 {
        wire_funding_time + local_shift_minutes.round() / MINS_IN_DAY
    } else {
        0.0
    }
}

/// Сериализовать `Market` в `EngineStreamReader`-совместимый byte stream
/// (зеркально `WriteMarketToStream`). Используется для тестов и для опционального
/// клиентского ответа на pseudo-request от сервера.
///
/// **NB:** byte-exact с Delphi `WriteMarketToStream` (MoonProtoSerialization.pas:97):
/// FuturesType пишется **всегда** (без gate ver), как в оригинале. `ver` оставлен
/// в сигнатуре для зеркальности с `read_market`, но в writer'е не используется.
pub fn write_market(out: &mut Vec<u8>, m: &Market, _ver: u16) {
    write_market_with_local_shift(out, m, _ver, current_local_time_shift_minutes())
}

fn write_market_with_local_shift(
    out: &mut Vec<u8>,
    m: &Market,
    _ver: u16,
    local_shift_minutes: f64,
) {
    write_str(out, &m.bn_market_name);
    write_str(out, &m.market_currency);
    write_str(out, &m.bn_market_currency);
    write_str(out, &m.base_currency);
    write_str(out, &m.market_currency_long);
    write_str(out, &m.market_currency_canonic);
    write_str(out, &m.market_name);
    write_str(out, &m.market_name_mb_classic);
    write_str(out, &m.bn_status);
    write_str(out, &m.leading1000);

    out.extend_from_slice(&m.bn_price_precision.to_le_bytes());
    out.extend_from_slice(&m.bn_quantity_precision.to_le_bytes());
    out.extend_from_slice(&m.max_leverage.to_le_bytes());
    out.extend_from_slice(&m.k1000.to_le_bytes());
    out.extend_from_slice(&m.bn_iceberg_parts.to_le_bytes());
    out.extend_from_slice(&m.bn_margin_table_id.to_le_bytes());

    out.extend_from_slice(&m.bn_delivery_time.to_le_bytes());

    out.extend_from_slice(&m.bn_tick_size.to_le_bytes());
    out.extend_from_slice(&m.bn_step_size.to_le_bytes());
    out.extend_from_slice(&m.bn_min_qty.to_le_bytes());
    out.extend_from_slice(&m.bn_max_qty.to_le_bytes());
    out.extend_from_slice(&m.bn_min_notional.to_le_bytes());
    out.extend_from_slice(&m.bn_max_notional.to_le_bytes());
    out.extend_from_slice(&m.bn_contract_size.to_le_bytes());
    out.extend_from_slice(&m.bn_min_price.to_le_bytes());
    out.extend_from_slice(&m.bn_max_price.to_le_bytes());
    out.extend_from_slice(&m.bn_max_value.to_le_bytes());
    out.extend_from_slice(&m.bn_multiplier_up.to_le_bytes());
    out.extend_from_slice(&m.bn_multiplier_down.to_le_bytes());
    out.extend_from_slice(&m.bid_multiplier_up.to_le_bytes());
    out.extend_from_slice(&m.bid_multiplier_down.to_le_bytes());
    out.extend_from_slice(&m.ask_multiplier_up.to_le_bytes());
    out.extend_from_slice(&m.ask_multiplier_down.to_le_bytes());
    out.extend_from_slice(&m.int_bn_max_qty.to_le_bytes());
    out.extend_from_slice(&m.funding_rate.to_le_bytes());
    let wire_funding_time = remove_delphi_local_funding_shift(m.funding_time, local_shift_minutes);
    out.extend_from_slice(&wire_funding_time.to_le_bytes());
    out.extend_from_slice(&m.volume.to_le_bytes());

    out.push(m.is_btc_market as u8);
    out.push(m.status_trading as u8);
    out.push(m.bn_is_fucking_shib as u8);
    out.push(m.bn_iceberg as u8);
    out.push(m.bn_only_isolated as u8);

    // Delphi: WriteMarketToStream пишет FuturesType всегда (без guard ver).
    out.push(m.futures_type.to_byte());
}

fn remove_delphi_local_funding_shift(local_funding_time: f64, local_shift_minutes: f64) -> f64 {
    if local_funding_time > 0.0 {
        local_funding_time - local_shift_minutes.round() / MINS_IN_DAY
    } else {
        0.0
    }
}

pub(super) fn write_str(out: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    let len = bytes.len() as u16;
    let len_usize = usize::from(len);
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&bytes[..len_usize]);
}

// =============================================================================
//  CorrMarket struct
// =============================================================================

/// `TCorrMarket` (correlation market) — упрощённый вид маркета для расчётов.
/// Byte-exact с `WriteCorrMarketToStream` (MoonProtoSerialization.pas:169-178).
#[derive(Debug, Clone, PartialEq)]
pub struct CorrMarket {
    pub bn_market_name: String,
    pub bn_market_currency: String,
    pub bn_tick_size: f64,
    /// `BaseCurrency.BaseCurrency` (имя базовой валюты, '' если nil).
    pub base_currency_name: String,
}

pub fn read_corr_market(r: &mut EngineStreamReader) -> Option<CorrMarket> {
    let bn_market_name = r.read_str()?;
    let bn_market_currency = r.read_str()?;
    let bn_tick_size = r.read_double()?;
    let base_currency_name = r.read_str()?;
    Some(CorrMarket {
        bn_market_name,
        bn_market_currency,
        bn_tick_size,
        base_currency_name,
    })
}

pub fn write_corr_market(out: &mut Vec<u8>, c: &CorrMarket) {
    write_str(out, &c.bn_market_name);
    write_str(out, &c.bn_market_currency);
    out.extend_from_slice(&c.bn_tick_size.to_le_bytes());
    write_str(out, &c.base_currency_name);
}

// =============================================================================
//  MarketsListResponse — emk_GetMarketsList
// =============================================================================

/// Ответ на `emk_GetMarketsList`: полный список маркетов + CorrMarkets.
/// Wire-form (MoonProtoEngineServer.pas:60-82 `WriteMarketsToStream`):
///   `count:i32 + markets[count] + corr_count:i32 + corr_markets[corr_count]`.
#[derive(Debug, Clone)]
pub struct MarketsListResponse {
    pub markets: Vec<Market>,
    pub corr_markets: Vec<CorrMarket>,
}

/// Parse `EngineResponse.data` для `emk_GetMarketsList`.
pub fn parse_markets_list_response(data: &[u8], ver: u16) -> Option<MarketsListResponse> {
    parse_markets_list_response_with_local_shift(data, ver, current_local_time_shift_minutes())
}

fn parse_markets_list_response_with_local_shift(
    data: &[u8],
    ver: u16,
    local_shift_minutes: f64,
) -> Option<MarketsListResponse> {
    let mut r = EngineStreamReader::new(data);
    // Market минимум заведомо больше 16 байт; число используется только для
    // prealloc, не для Delphi-incompatible early reject.
    let count = r.read_count()?;
    let mut markets = Vec::with_capacity(r.bounded_count_capacity(count, 16));
    for _ in 0..count {
        markets.push(read_market_with_local_shift(
            &mut r,
            ver,
            local_shift_minutes,
        )?);
    }
    // CorrMarket минимум больше 8 байт; только bounded prealloc.
    let corr_count = r.read_count()?;
    let mut corr_markets = Vec::with_capacity(r.bounded_count_capacity(corr_count, 8));
    for _ in 0..corr_count {
        corr_markets.push(read_corr_market(&mut r)?);
    }
    Some(MarketsListResponse {
        markets,
        corr_markets,
    })
}

/// Опциональный билдер для тестов.
pub fn build_markets_list_response(resp: &MarketsListResponse, ver: u16) -> Vec<u8> {
    build_markets_list_response_with_local_shift(resp, ver, current_local_time_shift_minutes())
}

fn build_markets_list_response_with_local_shift(
    resp: &MarketsListResponse,
    ver: u16,
    local_shift_minutes: f64,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(1024);
    out.extend_from_slice(&(resp.markets.len() as i32).to_le_bytes());
    for m in &resp.markets {
        write_market_with_local_shift(&mut out, m, ver, local_shift_minutes);
    }
    out.extend_from_slice(&(resp.corr_markets.len() as i32).to_le_bytes());
    for c in &resp.corr_markets {
        write_corr_market(&mut out, c);
    }
    out
}

// =============================================================================
//  MarketsPricesResponse — emk_UpdateMarketsList
// =============================================================================

/// Обновление цены одного маркета (byte-exact с `WriteMarketPricesToStream`
/// MoonProtoSerialization.pas:195-209).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MarketPriceUpdate {
    pub m_index: u16,
    pub bid: f64,
    pub ask: f64,
    /// Если `MarketsPricesResponse.send_funding == false` — 0.0.
    pub funding_rate: f64,
    /// Delphi client-local `TDateTime` after adding local TZShift. If source
    /// `funding_time` was 0 → 0.
    pub funding_time: f64,
    pub mark_price: f64,
    pub mark_price_found: bool,
}

/// Обновление цены `CorrMarket`.
#[derive(Debug, Clone, PartialEq)]
pub struct CorrMarketPriceUpdate {
    pub bn_market_name: String,
    pub last_price: f64,
}

/// Полный ответ `emk_UpdateMarketsList`.
/// Wire-form (MoonProtoEngineServer.pas:84-111):
///   `send_funding:bool + count:i32 + prices[count] + send_corr_markets:bool +
///    (if send_corr_markets) corr_count:i32 + corr_prices[corr_count]`.
#[derive(Debug, Clone)]
pub struct MarketsPricesResponse {
    pub send_funding: bool,
    pub prices: Vec<MarketPriceUpdate>,
    pub send_corr_markets: bool,
    pub corr_prices: Vec<CorrMarketPriceUpdate>,
}

pub fn parse_markets_prices_response(data: &[u8]) -> Option<MarketsPricesResponse> {
    parse_markets_prices_response_with_local_shift(data, current_local_time_shift_minutes())
}

fn parse_markets_prices_response_with_local_shift(
    data: &[u8],
    local_shift_minutes: f64,
) -> Option<MarketsPricesResponse> {
    let mut r = EngineStreamReader::new(data);
    let send_funding = r.read_bool()?;
    // MarketPriceUpdate минимум: m_index(2) + bid(8) + ask(8) + mark_price(8) + mark_found(1) = 27 байт.
    // Если send_funding=true ещё +16. 27 используется только для bounded prealloc.
    let count = r.read_count()?;
    let mut prices = Vec::with_capacity(r.bounded_count_capacity(count, 27));
    for _ in 0..count {
        let m_index = r.read_word()?;
        let bid = r.read_double()?;
        let ask = r.read_double()?;
        let (funding_rate, funding_time) = if send_funding {
            (
                r.read_double()?,
                apply_delphi_local_funding_shift(r.read_double()?, local_shift_minutes),
            )
        } else {
            (0.0, 0.0)
        };
        let mark_price = r.read_double()?;
        let mark_price_found = r.read_bool()?;
        prices.push(MarketPriceUpdate {
            m_index,
            bid,
            ask,
            funding_rate,
            funding_time,
            mark_price,
            mark_price_found,
        });
    }
    let send_corr_markets = r.read_bool()?;
    let mut corr_prices = Vec::new();
    if send_corr_markets {
        // CorrMarketPriceUpdate: bn_market_name (string u16+chars) + last_price (8) = минимум 10 байт.
        let corr_count = r.read_count()?;
        corr_prices.reserve(r.bounded_count_capacity(corr_count, 10));
        for _ in 0..corr_count {
            let bn_market_name = r.read_str()?;
            let last_price = r.read_double()?;
            corr_prices.push(CorrMarketPriceUpdate {
                bn_market_name,
                last_price,
            });
        }
    }
    Some(MarketsPricesResponse {
        send_funding,
        prices,
        send_corr_markets,
        corr_prices,
    })
}

pub fn build_markets_prices_response(resp: &MarketsPricesResponse) -> Vec<u8> {
    build_markets_prices_response_with_local_shift(resp, current_local_time_shift_minutes())
}

fn build_markets_prices_response_with_local_shift(
    resp: &MarketsPricesResponse,
    local_shift_minutes: f64,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 + resp.prices.len() * 50);
    out.push(resp.send_funding as u8);
    out.extend_from_slice(&(resp.prices.len() as i32).to_le_bytes());
    for p in &resp.prices {
        out.extend_from_slice(&p.m_index.to_le_bytes());
        out.extend_from_slice(&p.bid.to_le_bytes());
        out.extend_from_slice(&p.ask.to_le_bytes());
        if resp.send_funding {
            out.extend_from_slice(&p.funding_rate.to_le_bytes());
            let wire_funding_time =
                remove_delphi_local_funding_shift(p.funding_time, local_shift_minutes);
            out.extend_from_slice(&wire_funding_time.to_le_bytes());
        }
        out.extend_from_slice(&p.mark_price.to_le_bytes());
        out.push(p.mark_price_found as u8);
    }
    out.push(resp.send_corr_markets as u8);
    if resp.send_corr_markets {
        out.extend_from_slice(&(resp.corr_prices.len() as i32).to_le_bytes());
        for c in &resp.corr_prices {
            write_str(&mut out, &c.bn_market_name);
            out.extend_from_slice(&c.last_price.to_le_bytes());
        }
    }
    out
}

// =============================================================================
//  MarketsIndexesResponse — emk_GetMarketsIndexes
// =============================================================================

/// Ответ `emk_GetMarketsIndexes`: список имён маркетов в том же порядке что в `Markets.FList`.
/// `index` = позиция в массиве (соответствует `mIndex` в Delphi).
/// Wire-form (MoonProtoEngineServer.pas:278-284):
///   `count:i32 + names[count] (UTF-8 strings)`.
pub fn parse_markets_indexes_response(data: &[u8]) -> Option<Vec<String>> {
    let mut r = EngineStreamReader::new(data);
    // Каждое имя — UTF-8 string с u16-prefix. Минимум 2 байта (пустая строка).
    let count = r.read_count()?;
    let mut names = Vec::with_capacity(r.bounded_count_capacity(count, 2));
    for _ in 0..count {
        names.push(r.read_str()?);
    }
    Some(names)
}

pub fn build_markets_indexes_response(names: &[String]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + names.iter().map(|s| 2 + s.len()).sum::<usize>());
    out.extend_from_slice(&(names.len() as i32).to_le_bytes());
    for n in names {
        write_str(&mut out, n);
    }
    out
}

// =============================================================================
//  Tests
// =============================================================================

#[cfg(test)]
mod tests;
