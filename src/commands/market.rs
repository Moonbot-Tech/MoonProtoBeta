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

use super::registry::read_string;

// =============================================================================
//  EngineStreamReader — helper для последовательного чтения примитивов
// =============================================================================

/// Безопасный последовательный reader для `TEngineResponse.DataStream` payload'а.
pub struct EngineStreamReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> EngineStreamReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub fn position(&self) -> usize { self.pos }
    pub fn len(&self) -> usize { self.data.len() }
    pub fn is_empty(&self) -> bool { self.data.is_empty() }
    pub fn remaining(&self) -> usize { self.data.len().saturating_sub(self.pos) }

    pub fn read_u8(&mut self) -> Option<u8> {
        if self.pos + 1 > self.data.len() { return None; }
        let v = self.data[self.pos];
        self.pos += 1;
        Some(v)
    }
    pub fn read_bool(&mut self) -> Option<bool> { self.read_u8().map(|b| b != 0) }
    pub fn read_byte(&mut self) -> Option<u8> { self.read_u8() }

    pub fn read_u16(&mut self) -> Option<u16> {
        if self.pos + 2 > self.data.len() { return None; }
        let v = u16::from_le_bytes(self.data[self.pos..self.pos + 2].try_into().unwrap());
        self.pos += 2;
        Some(v)
    }
    pub fn read_word(&mut self) -> Option<u16> { self.read_u16() }

    pub fn read_i32(&mut self) -> Option<i32> {
        if self.pos + 4 > self.data.len() { return None; }
        let v = i32::from_le_bytes(self.data[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Some(v)
    }
    pub fn read_int(&mut self) -> Option<i32> { self.read_i32() }

    pub fn read_i64(&mut self) -> Option<i64> {
        if self.pos + 8 > self.data.len() { return None; }
        let v = i64::from_le_bytes(self.data[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Some(v)
    }
    pub fn read_int64(&mut self) -> Option<i64> { self.read_i64() }

    pub fn read_f64(&mut self) -> Option<f64> {
        if self.pos + 8 > self.data.len() { return None; }
        let v = f64::from_le_bytes(self.data[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Some(v)
    }
    pub fn read_double(&mut self) -> Option<f64> { self.read_f64() }

    pub fn read_str(&mut self) -> Option<String> {
        read_string(self.data, &mut self.pos)
    }
}

// =============================================================================
//  TBaseCurrency enum (Vars.pas:40)
// =============================================================================

/// `TBaseCurrency` — базовая валюта рынка. Источник: `Vars.pas:40`.
/// На проводе передаётся как 1 байт ordinal'а (см. `FuturesType` в `WriteMarketToStream`).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BaseCurrency {
    BTC = 0, USDT = 1, ETH = 2, BNB = 3, AUD = 4, TUSD = 5, BRL = 6, USDH = 7,
    USDC = 8, FDUSD = 9, AEUR = 10, USD = 11, TRX = 12, RUB = 13, EUR = 14,
    HTX = 15, USDD = 16, IDR = 17, DOGE = 18, TRY = 19, USDE = 20,
    Next2 = 21, Next3 = 22, Next4 = 23, Next5 = 24,
    EMPTY = 25, Unknown = 26,
}

impl BaseCurrency {
    /// `BaseCurrency` имеет типизированный `Unknown` вариант — сохраняем как есть
    /// (не `Option<Self>` поскольку Unknown — это **известный** факт в системе типов).
    /// При unknown byte логируем warn! для диагностики server-side расширений (A-02).
    pub fn from_byte(b: u8) -> Self {
        match b {
            0 => Self::BTC, 1 => Self::USDT, 2 => Self::ETH, 3 => Self::BNB,
            4 => Self::AUD, 5 => Self::TUSD, 6 => Self::BRL, 7 => Self::USDH,
            8 => Self::USDC, 9 => Self::FDUSD, 10 => Self::AEUR, 11 => Self::USD,
            12 => Self::TRX, 13 => Self::RUB, 14 => Self::EUR, 15 => Self::HTX,
            16 => Self::USDD, 17 => Self::IDR, 18 => Self::DOGE, 19 => Self::TRY,
            20 => Self::USDE, 21 => Self::Next2, 22 => Self::Next3, 23 => Self::Next4,
            24 => Self::Next5, 25 => Self::EMPTY,
            _ => {
                log::warn!(target: "moonproto::market", "unknown BaseCurrency byte: {b} (server-side extension?)");
                Self::Unknown
            }
        }
    }
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
    pub bn_market_name:            String,
    pub market_currency:           String,
    pub bn_market_currency:        String,
    pub base_currency:             String,
    pub market_currency_long:      String,
    pub market_currency_canonic:   String,
    pub market_name:               String,
    pub market_name_mb_classic:    String,
    pub bn_status:                 String,
    pub leading1000:               String,
    // --- Integers (6) ---
    pub bn_price_precision:        i32,
    pub bn_quantity_precision:     i32,
    pub max_leverage:              i32,
    pub k1000:                     i32,
    pub bn_iceberg_parts:          i32,
    pub bn_margin_table_id:        i32,
    // --- Int64 (1) ---
    pub bn_delivery_time:          i64,
    // --- Doubles (20) ---
    pub bn_tick_size:              f64,
    pub bn_step_size:              f64,
    pub bn_min_qty:                f64,
    pub bn_max_qty:                f64,
    pub bn_min_notional:           f64,
    pub bn_max_notional:           f64,
    pub bn_contract_size:          f64,
    pub bn_min_price:              f64,
    pub bn_max_price:              f64,
    pub bn_max_value:              f64,
    pub bn_multiplier_up:          f64,
    pub bn_multiplier_down:        f64,
    pub bid_multiplier_up:         f64,
    pub bid_multiplier_down:       f64,
    pub ask_multiplier_up:         f64,
    pub ask_multiplier_down:       f64,
    pub int_bn_max_qty:            f64,
    pub funding_rate:              f64,
    pub funding_time:              f64,
    pub volume:                    f64,
    // --- Booleans (5) ---
    pub is_btc_market:             bool,
    pub status_trading:            bool,
    pub bn_is_fucking_shib:        bool,
    pub bn_iceberg:                bool,
    pub bn_only_isolated:          bool,
    // --- v2: FuturesType ---
    pub futures_type:              BaseCurrency,
}

/// Прочитать `TMarket` из EngineStreamReader (byte-exact с `ReadMarketFromStream`).
/// `ver` — версия команды `TEngineResponse` (если >= 2 — есть FuturesType byte).
pub fn read_market(r: &mut EngineStreamReader, ver: u16) -> Option<Market> {
    let bn_market_name            = r.read_str()?;
    let market_currency           = r.read_str()?;
    let bn_market_currency        = r.read_str()?;
    let base_currency             = r.read_str()?;
    let market_currency_long      = r.read_str()?;
    let market_currency_canonic   = r.read_str()?;
    let market_name               = r.read_str()?;
    let mut market_name_mb_classic = r.read_str()?;
    let bn_status                 = r.read_str()?;
    let leading1000               = r.read_str()?;

    let bn_price_precision        = r.read_int()?;
    let bn_quantity_precision     = r.read_int()?;
    let max_leverage              = r.read_int()?;
    let k1000                     = r.read_int()?;
    let bn_iceberg_parts          = r.read_int()?;
    let bn_margin_table_id        = r.read_int()?;

    let bn_delivery_time          = r.read_int64()?;

    let bn_tick_size              = r.read_double()?;
    let bn_step_size              = r.read_double()?;
    let bn_min_qty                = r.read_double()?;
    let bn_max_qty                = r.read_double()?;
    let bn_min_notional           = r.read_double()?;
    let bn_max_notional           = r.read_double()?;
    let bn_contract_size          = r.read_double()?;
    let bn_min_price              = r.read_double()?;
    let bn_max_price              = r.read_double()?;
    let bn_max_value              = r.read_double()?;
    let bn_multiplier_up          = r.read_double()?;
    let bn_multiplier_down        = r.read_double()?;
    let bid_multiplier_up         = r.read_double()?;
    let bid_multiplier_down       = r.read_double()?;
    let ask_multiplier_up         = r.read_double()?;
    let ask_multiplier_down       = r.read_double()?;
    let int_bn_max_qty            = r.read_double()?;
    let funding_rate              = r.read_double()?;
    let funding_time              = r.read_double()?;
    let volume                    = r.read_double()?;

    let is_btc_market             = r.read_bool()?;
    let status_trading            = r.read_bool()?;
    let bn_is_fucking_shib        = r.read_bool()?;
    let bn_iceberg                = r.read_bool()?;
    let bn_only_isolated          = r.read_bool()?;

    let futures_type = if ver >= 2 {
        BaseCurrency::from_byte(r.read_byte()?)
    } else {
        BaseCurrency::Unknown
    };

    // Backfill MBClassic (см. ReadMarketFromStream MoonProtoSerialization.pas:160).
    if market_name_mb_classic.is_empty() {
        market_name_mb_classic = market_name.clone();
    }

    Some(Market {
        bn_market_name, market_currency, bn_market_currency, base_currency,
        market_currency_long, market_currency_canonic, market_name, market_name_mb_classic,
        bn_status, leading1000,
        bn_price_precision, bn_quantity_precision, max_leverage, k1000,
        bn_iceberg_parts, bn_margin_table_id,
        bn_delivery_time,
        bn_tick_size, bn_step_size, bn_min_qty, bn_max_qty, bn_min_notional, bn_max_notional,
        bn_contract_size, bn_min_price, bn_max_price, bn_max_value,
        bn_multiplier_up, bn_multiplier_down,
        bid_multiplier_up, bid_multiplier_down, ask_multiplier_up, ask_multiplier_down,
        int_bn_max_qty, funding_rate, funding_time, volume,
        is_btc_market, status_trading, bn_is_fucking_shib, bn_iceberg, bn_only_isolated,
        futures_type,
    })
}

/// Сериализовать `Market` в `EngineStreamReader`-совместимый byte stream
/// (зеркально `WriteMarketToStream`). Используется для тестов и для опционального
/// клиентского ответа на pseudo-request от сервера.
///
/// **NB:** byte-exact с Delphi `WriteMarketToStream` (MoonProtoSerialization.pas:97):
/// FuturesType пишется **всегда** (без gate ver), как в оригинале. `ver` оставлен
/// в сигнатуре для зеркальности с `read_market`, но в writer'е не используется.
pub fn write_market(out: &mut Vec<u8>, m: &Market, _ver: u16) {
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
    out.extend_from_slice(&m.funding_time.to_le_bytes());
    out.extend_from_slice(&m.volume.to_le_bytes());

    out.push(m.is_btc_market as u8);
    out.push(m.status_trading as u8);
    out.push(m.bn_is_fucking_shib as u8);
    out.push(m.bn_iceberg as u8);
    out.push(m.bn_only_isolated as u8);

    // Delphi: WriteMarketToStream пишет FuturesType всегда (без guard ver).
    out.push(m.futures_type as u8);
}

fn write_str(out: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    out.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
    out.extend_from_slice(bytes);
}

// =============================================================================
//  CorrMarket struct
// =============================================================================

/// `TCorrMarket` (correlation market) — упрощённый вид маркета для расчётов.
/// Byte-exact с `WriteCorrMarketToStream` (MoonProtoSerialization.pas:169-178).
#[derive(Debug, Clone, PartialEq)]
pub struct CorrMarket {
    pub bn_market_name:     String,
    pub bn_market_currency: String,
    pub bn_tick_size:       f64,
    /// `BaseCurrency.BaseCurrency` (имя базовой валюты, '' если nil).
    pub base_currency_name: String,
}

pub fn read_corr_market(r: &mut EngineStreamReader) -> Option<CorrMarket> {
    let bn_market_name     = r.read_str()?;
    let bn_market_currency = r.read_str()?;
    let bn_tick_size       = r.read_double()?;
    let base_currency_name = r.read_str()?;
    Some(CorrMarket { bn_market_name, bn_market_currency, bn_tick_size, base_currency_name })
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
    pub markets:      Vec<Market>,
    pub corr_markets: Vec<CorrMarket>,
}

/// Parse `EngineResponse.data` для `emk_GetMarketsList`.
pub fn parse_markets_list_response(data: &[u8], ver: u16) -> Option<MarketsListResponse> {
    let mut r = EngineStreamReader::new(data);
    let count = r.read_int()? as usize;
    let mut markets = Vec::with_capacity(count);
    for _ in 0..count {
        markets.push(read_market(&mut r, ver)?);
    }
    let corr_count = r.read_int()? as usize;
    let mut corr_markets = Vec::with_capacity(corr_count);
    for _ in 0..corr_count {
        corr_markets.push(read_corr_market(&mut r)?);
    }
    Some(MarketsListResponse { markets, corr_markets })
}

/// Опциональный билдер для тестов.
pub fn build_markets_list_response(resp: &MarketsListResponse, ver: u16) -> Vec<u8> {
    let mut out = Vec::with_capacity(1024);
    out.extend_from_slice(&(resp.markets.len() as i32).to_le_bytes());
    for m in &resp.markets {
        write_market(&mut out, m, ver);
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
    pub m_index:           u16,
    pub bid:               f64,
    pub ask:               f64,
    /// Если `MarketsPricesResponse.send_funding == false` — 0.0.
    pub funding_rate:      f64,
    /// UTC time (без TZShift). Если `funding_time` в исходнике был 0 → 0.
    pub funding_time_utc:  f64,
    pub mark_price:        f64,
    pub mark_price_found:  bool,
}

/// Обновление цены `CorrMarket`.
#[derive(Debug, Clone, PartialEq)]
pub struct CorrMarketPriceUpdate {
    pub bn_market_name: String,
    pub last_price:     f64,
}

/// Полный ответ `emk_UpdateMarketsList`.
/// Wire-form (MoonProtoEngineServer.pas:84-111):
///   `send_funding:bool + count:i32 + prices[count] + send_corr_markets:bool +
///    (if send_corr_markets) corr_count:i32 + corr_prices[corr_count]`.
#[derive(Debug, Clone)]
pub struct MarketsPricesResponse {
    pub send_funding:     bool,
    pub prices:           Vec<MarketPriceUpdate>,
    pub send_corr_markets: bool,
    pub corr_prices:      Vec<CorrMarketPriceUpdate>,
}

pub fn parse_markets_prices_response(data: &[u8]) -> Option<MarketsPricesResponse> {
    let mut r = EngineStreamReader::new(data);
    let send_funding = r.read_bool()?;
    let count = r.read_int()? as usize;
    let mut prices = Vec::with_capacity(count);
    for _ in 0..count {
        let m_index = r.read_word()?;
        let bid = r.read_double()?;
        let ask = r.read_double()?;
        let (funding_rate, funding_time_utc) = if send_funding {
            (r.read_double()?, r.read_double()?)
        } else {
            (0.0, 0.0)
        };
        let mark_price = r.read_double()?;
        let mark_price_found = r.read_bool()?;
        prices.push(MarketPriceUpdate {
            m_index, bid, ask,
            funding_rate, funding_time_utc,
            mark_price, mark_price_found,
        });
    }
    let send_corr_markets = r.read_bool()?;
    let mut corr_prices = Vec::new();
    if send_corr_markets {
        let corr_count = r.read_int()? as usize;
        corr_prices.reserve(corr_count);
        for _ in 0..corr_count {
            let bn_market_name = r.read_str()?;
            let last_price = r.read_double()?;
            corr_prices.push(CorrMarketPriceUpdate { bn_market_name, last_price });
        }
    }
    Some(MarketsPricesResponse { send_funding, prices, send_corr_markets, corr_prices })
}

pub fn build_markets_prices_response(resp: &MarketsPricesResponse) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 + resp.prices.len() * 50);
    out.push(resp.send_funding as u8);
    out.extend_from_slice(&(resp.prices.len() as i32).to_le_bytes());
    for p in &resp.prices {
        out.extend_from_slice(&p.m_index.to_le_bytes());
        out.extend_from_slice(&p.bid.to_le_bytes());
        out.extend_from_slice(&p.ask.to_le_bytes());
        if resp.send_funding {
            out.extend_from_slice(&p.funding_rate.to_le_bytes());
            out.extend_from_slice(&p.funding_time_utc.to_le_bytes());
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
    let count = r.read_int()? as usize;
    let mut names = Vec::with_capacity(count);
    for _ in 0..count {
        names.push(r.read_str()?);
    }
    Some(names)
}

pub fn build_markets_indexes_response(names: &[String]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + names.iter().map(|s| 2 + s.len()).sum::<usize>());
    out.extend_from_slice(&(names.len() as i32).to_le_bytes());
    for n in names { write_str(&mut out, n); }
    out
}

// =============================================================================
//  TokenTags
// =============================================================================

/// `TTokenTag` flag set (Vars.pas:64). На проводе — i32 bitmask.
///
/// Биты соответствуют ordinal'ам Delphi enum'а `TTokenTag`:
/// `(tag_none, tag_Monitoring, tag_Fan, tag_seed, tag_launch, tag_gaming,
///   tag_New, tag_OLD, tag_BNB, tag_Alpha, tag_OICapped, tag_TradFi)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TokenTags(pub u32);

impl TokenTags {
    pub const NONE:       Self = Self(1 << 0);
    pub const MONITORING: Self = Self(1 << 1);
    pub const FAN:        Self = Self(1 << 2);
    pub const SEED:       Self = Self(1 << 3);
    pub const LAUNCH:     Self = Self(1 << 4);
    pub const GAMING:     Self = Self(1 << 5);
    pub const NEW:        Self = Self(1 << 6);
    pub const OLD:        Self = Self(1 << 7);
    pub const BNB:        Self = Self(1 << 8);
    pub const ALPHA:      Self = Self(1 << 9);
    pub const OI_CAPPED:  Self = Self(1 << 10);
    pub const TRAD_FI:    Self = Self(1 << 11);

    pub const fn empty() -> Self { Self(0) }
    pub const fn bits(self) -> u32 { self.0 }
    pub const fn from_bits(b: u32) -> Self { Self(b) }
    pub fn contains(self, other: Self) -> bool { (self.0 & other.0) == other.0 }
    pub fn is_empty(self) -> bool { self.0 == 0 }
}

impl core::ops::BitOr for TokenTags {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self { Self(self.0 | rhs.0) }
}

impl core::ops::BitAnd for TokenTags {
    type Output = Self;
    fn bitand(self, rhs: Self) -> Self { Self(self.0 & rhs.0) }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MarketTokenTags {
    pub market_name: String,
    pub tags:        TokenTags,
}

/// Ответ `emk_CheckBinanceTags`: список (market_name, tags).
/// Wire-form (MoonProtoEngineServer.pas:324-333):
///   `count:i32 + (market_name:string + tags:i32)[count]`.
pub fn parse_token_tags_response(data: &[u8]) -> Option<Vec<MarketTokenTags>> {
    let mut r = EngineStreamReader::new(data);
    let count = r.read_int()? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let market_name = r.read_str()?;
        let tags_int = r.read_int()? as u32;
        out.push(MarketTokenTags {
            market_name,
            tags: TokenTags::from_bits(tags_int),
        });
    }
    Some(out)
}

pub fn build_token_tags_response(items: &[MarketTokenTags]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + items.len() * 16);
    out.extend_from_slice(&(items.len() as i32).to_le_bytes());
    for it in items {
        write_str(&mut out, &it.market_name);
        out.extend_from_slice(&(it.tags.bits() as i32).to_le_bytes());
    }
    out
}

// =============================================================================
//  Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_market(name: &str, with_v2: bool) -> Market {
        Market {
            bn_market_name: name.to_string(),
            market_currency: "BTC".to_string(),
            bn_market_currency: "BTC".to_string(),
            base_currency: "USDT".to_string(),
            market_currency_long: "Bitcoin".to_string(),
            market_currency_canonic: "BTC".to_string(),
            market_name: format!("{}USDT", name),
            market_name_mb_classic: format!("{}_USDT", name),
            bn_status: "TRADING".to_string(),
            leading1000: String::new(),
            bn_price_precision: 2,
            bn_quantity_precision: 5,
            max_leverage: 125,
            k1000: 1,
            bn_iceberg_parts: 0,
            bn_margin_table_id: 0,
            bn_delivery_time: 0,
            bn_tick_size: 0.01,
            bn_step_size: 0.00001,
            bn_min_qty: 0.00001,
            bn_max_qty: 9000.0,
            bn_min_notional: 5.0,
            bn_max_notional: 0.0,
            bn_contract_size: 1.0,
            bn_min_price: 0.01,
            bn_max_price: 1000000.0,
            bn_max_value: 0.0,
            bn_multiplier_up: 1.05,
            bn_multiplier_down: 0.95,
            bid_multiplier_up: 0.0,
            bid_multiplier_down: 0.0,
            ask_multiplier_up: 0.0,
            ask_multiplier_down: 0.0,
            int_bn_max_qty: 0.0,
            funding_rate: 0.0001,
            funding_time: 45123.5,
            volume: 1234567.0,
            is_btc_market: true,
            status_trading: true,
            bn_is_fucking_shib: false,
            bn_iceberg: false,
            bn_only_isolated: false,
            futures_type: if with_v2 { BaseCurrency::USDT } else { BaseCurrency::Unknown },
        }
    }

    #[test]
    fn market_roundtrip_v1() {
        let m = sample_market("BTC", false);
        let mut buf = Vec::new();
        write_market(&mut buf, &m, 1);
        let mut r = EngineStreamReader::new(&buf);
        let m2 = read_market(&mut r, 1).unwrap();
        assert_eq!(m, m2);
    }

    #[test]
    fn market_roundtrip_v2_with_futures_type() {
        let m = sample_market("ETH", true);
        let mut buf = Vec::new();
        write_market(&mut buf, &m, 2);
        let mut r = EngineStreamReader::new(&buf);
        let m2 = read_market(&mut r, 2).unwrap();
        assert_eq!(m2.futures_type, BaseCurrency::USDT);
        assert_eq!(m, m2);
    }

    #[test]
    fn market_mb_classic_backfilled_when_empty() {
        // Если в payload `market_name_mb_classic = ""`, после чтения должен стать = market_name.
        let mut m = sample_market("LTC", true);
        m.market_name_mb_classic = String::new();
        m.market_name = "LTCUSDT".to_string();
        let mut buf = Vec::new();
        write_market(&mut buf, &m, 2);
        let mut r = EngineStreamReader::new(&buf);
        let m2 = read_market(&mut r, 2).unwrap();
        assert_eq!(m2.market_name_mb_classic, "LTCUSDT");
    }

    #[test]
    fn corr_market_roundtrip() {
        let c = CorrMarket {
            bn_market_name: "BTCUSDT".to_string(),
            bn_market_currency: "BTC".to_string(),
            bn_tick_size: 0.5,
            base_currency_name: "USDT".to_string(),
        };
        let mut buf = Vec::new();
        write_corr_market(&mut buf, &c);
        let mut r = EngineStreamReader::new(&buf);
        let c2 = read_corr_market(&mut r).unwrap();
        assert_eq!(c, c2);
    }

    #[test]
    fn markets_list_response_roundtrip() {
        let resp = MarketsListResponse {
            markets: vec![sample_market("BTC", true), sample_market("ETH", true)],
            corr_markets: vec![
                CorrMarket {
                    bn_market_name: "DOGEBTC".to_string(),
                    bn_market_currency: "DOGE".to_string(),
                    bn_tick_size: 0.00000001,
                    base_currency_name: "BTC".to_string(),
                },
            ],
        };
        let buf = build_markets_list_response(&resp, 2);
        let parsed = parse_markets_list_response(&buf, 2).unwrap();
        assert_eq!(parsed.markets.len(), 2);
        assert_eq!(parsed.markets[0].bn_market_name, "BTC");
        assert_eq!(parsed.markets[1].bn_market_name, "ETH");
        assert_eq!(parsed.corr_markets.len(), 1);
        assert_eq!(parsed.corr_markets[0].bn_market_name, "DOGEBTC");
    }

    #[test]
    fn markets_prices_response_with_funding() {
        let resp = MarketsPricesResponse {
            send_funding: true,
            prices: vec![
                MarketPriceUpdate {
                    m_index: 0,
                    bid: 50000.0,
                    ask: 50001.0,
                    funding_rate: 0.0001,
                    funding_time_utc: 45123.5,
                    mark_price: 50000.5,
                    mark_price_found: true,
                },
                MarketPriceUpdate {
                    m_index: 1,
                    bid: 3000.0,
                    ask: 3000.5,
                    funding_rate: -0.0002,
                    funding_time_utc: 45123.5,
                    mark_price: 3000.25,
                    mark_price_found: false,
                },
            ],
            send_corr_markets: true,
            corr_prices: vec![
                CorrMarketPriceUpdate { bn_market_name: "DOGEBTC".to_string(), last_price: 0.0000001 },
            ],
        };
        let buf = build_markets_prices_response(&resp);
        let parsed = parse_markets_prices_response(&buf).unwrap();
        assert!(parsed.send_funding);
        assert_eq!(parsed.prices.len(), 2);
        assert_eq!(parsed.prices[0].bid, 50000.0);
        assert_eq!(parsed.prices[1].funding_rate, -0.0002);
        assert!(parsed.send_corr_markets);
        assert_eq!(parsed.corr_prices.len(), 1);
        assert_eq!(parsed.corr_prices[0].last_price, 0.0000001);
    }

    #[test]
    fn markets_prices_response_no_funding_no_corr() {
        let resp = MarketsPricesResponse {
            send_funding: false,
            prices: vec![MarketPriceUpdate {
                m_index: 42,
                bid: 100.0,
                ask: 100.5,
                funding_rate: 0.0,
                funding_time_utc: 0.0,
                mark_price: 100.25,
                mark_price_found: true,
            }],
            send_corr_markets: false,
            corr_prices: vec![],
        };
        let buf = build_markets_prices_response(&resp);
        let parsed = parse_markets_prices_response(&buf).unwrap();
        assert!(!parsed.send_funding);
        assert_eq!(parsed.prices.len(), 1);
        assert_eq!(parsed.prices[0].m_index, 42);
        // funding_rate должен быть 0 при send_funding=false
        assert_eq!(parsed.prices[0].funding_rate, 0.0);
        assert!(!parsed.send_corr_markets);
    }

    #[test]
    fn markets_indexes_response_roundtrip() {
        let names = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string(), "DOGEUSDT".to_string()];
        let buf = build_markets_indexes_response(&names);
        let parsed = parse_markets_indexes_response(&buf).unwrap();
        assert_eq!(parsed, names);
    }

    #[test]
    fn token_tags_response_roundtrip() {
        let items = vec![
            MarketTokenTags {
                market_name: "BTCUSDT".to_string(),
                tags: TokenTags::MONITORING | TokenTags::ALPHA,
            },
            MarketTokenTags {
                market_name: "DOGEUSDT".to_string(),
                tags: TokenTags::GAMING | TokenTags::NEW,
            },
        ];
        let buf = build_token_tags_response(&items);
        let parsed = parse_token_tags_response(&buf).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].market_name, "BTCUSDT");
        assert!(parsed[0].tags.contains(TokenTags::MONITORING));
        assert!(parsed[0].tags.contains(TokenTags::ALPHA));
        assert!(parsed[1].tags.contains(TokenTags::GAMING));
        assert!(parsed[1].tags.contains(TokenTags::NEW));
    }

    #[test]
    fn base_currency_byte_mapping() {
        assert_eq!(BaseCurrency::from_byte(0), BaseCurrency::BTC);
        assert_eq!(BaseCurrency::from_byte(1), BaseCurrency::USDT);
        assert_eq!(BaseCurrency::from_byte(8), BaseCurrency::USDC);
        assert_eq!(BaseCurrency::from_byte(25), BaseCurrency::EMPTY);
        assert_eq!(BaseCurrency::from_byte(26), BaseCurrency::Unknown);
        // Out-of-range
        assert_eq!(BaseCurrency::from_byte(99), BaseCurrency::Unknown);
    }
}
