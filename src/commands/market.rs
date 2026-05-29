//! Market Active Lib types and low-level Engine API response parsers.
//!
//! Delphi sources: `MoonProto/MoonProtoSerialization.pas` and
//! `MoonProto/MoonProtoEngineServer.pas`.
//!
//! Regular applications should access markets through retained `MarketHandle`
//! values and `MarketsState` readers. The packet-shaped parsers/builders here
//! are protocol tools; they accept `EngineResponse.data` after optional DEFLATE
//! decompression and are hidden from normal rustdoc where they are not a useful
//! user-facing abstraction.
//!
//! Engine stream primitive layout follows Delphi: little-endian numeric fields,
//! one-byte booleans, and u16-length UTF-8 strings.

use std::collections::HashMap;

use super::candles::current_local_time_shift_minutes;
use super::trade::OrderType;
use crate::time::DelphiTime;
const MINS_IN_DAY: f64 = 1440.0;

mod indexes;
mod list;
mod prices;
mod reader;
mod token_tags;
#[doc(hidden)]
pub use self::indexes::{build_markets_indexes_response, parse_markets_indexes_response};
#[cfg(test)]
use self::list::build_markets_list_response_with_local_shift;
#[doc(hidden)]
pub use self::list::{
    build_markets_list_response, parse_markets_list_response, MarketsListResponse,
};
#[doc(hidden)]
pub use self::prices::{
    build_markets_prices_response, parse_markets_prices_response, CorrMarketPriceUpdate,
    MarketPriceUpdate, MarketsPricesResponse,
};
#[cfg(test)]
use self::prices::{
    build_markets_prices_response_with_local_shift, parse_markets_prices_response_with_local_shift,
};
#[doc(hidden)]
pub use self::reader::EngineStreamReader;
pub use self::token_tags::TokenTags;
#[doc(hidden)]
pub use self::token_tags::{build_token_tags_response, parse_token_tags_response, MarketTokenTags};

// =============================================================================
//  TBotPlatform ordinal (Vars.pas:24)
// =============================================================================

/// Delphi `TBotPlatform` raw ordinal from `Vars.pas`.
///
/// Server identity and trade route headers carry this as one byte. The wrapper
/// keeps unknown future ordinals byte-exact while the public API avoids naked
/// magic `u8` values.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct ExchangeCode(u8);

#[allow(non_upper_case_globals)]
impl ExchangeCode {
    pub const None: Self = Self(0);
    pub const WasBittrex: Self = Self(1);
    pub const FBybit: Self = Self(2);
    pub const Binance: Self = Self(3);
    pub const FBinance: Self = Self(4);
    pub const Huobi: Self = Self(5);
    pub const QBinance: Self = Self(6);
    pub const ByBit: Self = Self(7);
    pub const Gate: Self = Self(8);
    pub const FGate: Self = Self(9);
    pub const BitGet: Self = Self(10);
    pub const FBitGet: Self = Self(11);
    pub const Hyper: Self = Self(12);
    pub const FHyper: Self = Self(13);
    pub const Next5: Self = Self(14);
    pub const Next6: Self = Self(15);

    pub const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    pub const fn to_byte(self) -> u8 {
        self.0
    }

    pub const fn is_known(self) -> bool {
        self.0 <= Self::Next6.0
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "None",
            Self::WasBittrex => "WasBittrex",
            Self::FBybit => "FBybit",
            Self::Binance => "Binance",
            Self::FBinance => "FBinance",
            Self::Huobi => "Huobi",
            Self::QBinance => "QBinance",
            Self::ByBit => "ByBit",
            Self::Gate => "Gate",
            Self::FGate => "FGate",
            Self::BitGet => "BitGet",
            Self::FBitGet => "FBitGet",
            Self::Hyper => "Hyper",
            Self::FHyper => "FHyper",
            Self::Next5 => "Next5",
            Self::Next6 => "Next6",
            _ => "Unknown",
        }
    }
}

impl std::fmt::Debug for ExchangeCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_known() {
            f.write_str(self.name())
        } else {
            write!(f, "Unknown({})", self.0)
        }
    }
}

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
pub struct BaseCurrency(u8);

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
pub struct ListedType(u8);

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
//  TPositionType ordinal (MarketsU.pas:31)
// =============================================================================

/// Delphi `TPositionType` (`PT_Cross=0`, `PT_Isolated=1`).
///
/// Balance/market packets carry this as one raw byte. The Active Lib API exposes
/// the typed value so user code does not pass magic `0/1`, while raw parsers
/// still preserve unknown future ordinals.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct PositionType(u8);

#[allow(non_upper_case_globals)]
impl PositionType {
    pub const Cross: Self = Self(0);
    pub const Isolated: Self = Self(1);

    pub const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    pub const fn to_byte(self) -> u8 {
        self.0
    }

    pub const fn is_known(self) -> bool {
        self.0 <= Self::Isolated.0
    }

    pub const fn is_cross(self) -> bool {
        self.0 == Self::Cross.0
    }

    pub const fn is_isolated(self) -> bool {
        self.0 == Self::Isolated.0
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Cross => "Cross",
            Self::Isolated => "Isolated",
            _ => "Unknown",
        }
    }
}

impl std::fmt::Debug for PositionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_known() {
            f.write_str(self.name())
        } else {
            write!(f, "Unknown({})", self.0)
        }
    }
}

// =============================================================================
//  Arbitrage platform codes (ArbTypes.pas)
// =============================================================================

/// Arbitrage platform code used by Delphi `ArbSlotPlatforms`.
///
/// Regular exchange codes reuse `TBotPlatform` ordinals; arbitrage also has
/// special codes for Hyperliquid deployers and extra feeds. This is why the
/// type is separate from [`ExchangeCode`].
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct ArbPlatformCode(u8);

#[allow(non_upper_case_globals)]
impl ArbPlatformCode {
    pub const None: Self = Self(0);
    pub const WasBittrex: Self = Self(1);
    pub const FBybit: Self = Self(2);
    pub const Binance: Self = Self(3);
    pub const FBinance: Self = Self(4);
    pub const Huobi: Self = Self(5);
    pub const QBinance: Self = Self(6);
    pub const ByBit: Self = Self(7);
    pub const Gate: Self = Self(8);
    pub const FGate: Self = Self(9);
    pub const BitGet: Self = Self(10);
    pub const FBitGet: Self = Self(11);
    pub const HyperSpot: Self = Self(12);
    pub const HyperFutures: Self = Self(13);
    pub const Forex: Self = Self(100);
    pub const UpBit: Self = Self(101);
    pub const Okx: Self = Self(102);
    pub const BinAlpha: Self = Self(103);
    pub const HL_DEX_BASE: u8 = 50;

    pub const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    pub const fn from_exchange(code: ExchangeCode) -> Self {
        Self(code.to_byte())
    }

    pub const fn hyper_deployer(index: u8) -> Self {
        Self(Self::HL_DEX_BASE.wrapping_add(index))
    }

    pub const fn to_byte(self) -> u8 {
        self.0
    }

    pub const fn is_hyper_deployer(self) -> bool {
        self.0 >= Self::HL_DEX_BASE && self.0 < Self::Forex.0
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "None",
            Self::WasBittrex => "WasBittrex",
            Self::FBybit => "FBybit",
            Self::Binance => "Binance",
            Self::FBinance => "FBinance",
            Self::Huobi => "Huobi",
            Self::QBinance => "QBinance",
            Self::ByBit => "ByBit",
            Self::Gate => "Gate",
            Self::FGate => "FGate",
            Self::BitGet => "BitGet",
            Self::FBitGet => "FBitGet",
            Self::HyperSpot => "HyperSpot",
            Self::HyperFutures => "HyperFutures",
            Self::Forex => "Forex",
            Self::UpBit => "UpBit",
            Self::Okx => "OKX",
            Self::BinAlpha => "BinAlpha",
            _ => "Unknown",
        }
    }
}

impl std::fmt::Debug for ArbPlatformCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.name() != "Unknown" {
            f.write_str(self.name())
        } else if self.is_hyper_deployer() {
            write!(f, "HyperDeployer({})", self.0 - Self::HL_DEX_BASE)
        } else {
            write!(f, "Unknown({})", self.0)
        }
    }
}

/// Delphi `TArbSlot.IsolatedFlags`: bit0 deposit blocked, bit1 withdraw blocked.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct ArbIsolationFlags(u8);

#[allow(non_upper_case_globals)]
impl ArbIsolationFlags {
    pub const None: Self = Self(0);
    pub const DepositBlocked: Self = Self(0b0000_0001);
    pub const WithdrawBlocked: Self = Self(0b0000_0010);

    pub const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    pub const fn to_byte(self) -> u8 {
        self.0
    }

    pub const fn contains(self, flag: Self) -> bool {
        (self.0 & flag.0) != 0
    }

    pub const fn deposit_blocked(self) -> bool {
        self.contains(Self::DepositBlocked)
    }

    pub const fn withdraw_blocked(self) -> bool {
        self.contains(Self::WithdrawBlocked)
    }
}

impl std::fmt::Debug for ArbIsolationFlags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ArbIsolationFlags({:#04x})", self.0)
    }
}

// =============================================================================
//  Market struct
// =============================================================================

/// Live Active Lib market object.
///
/// The first fields are byte-exact with `WriteMarketToStream`
/// (MoonProtoSerialization.pas:42-98): 10 strings + 6 ints + 1 int64 +
/// 20 doubles + 5 bools + 1 byte for v2 `FuturesType`. The remaining fields
/// mirror Delphi `TMarket` live state maintained by other protocol commands and
/// are not serialized by `WriteMarketToStream`.
#[derive(Debug, Clone, PartialEq)]
pub struct Market {
    // --- Strings (10) ---
    #[doc(hidden)]
    pub bn_market_name: String,
    pub market_currency: String,
    #[doc(hidden)]
    pub bn_market_currency: String,
    pub base_currency: String,
    pub market_currency_long: String,
    pub market_currency_canonic: String,
    pub market_name: String,
    pub market_name_mb_classic: String,
    #[doc(hidden)]
    pub bn_status: String,
    pub leading1000: String,
    // --- Integers (6) ---
    #[doc(hidden)]
    pub bn_price_precision: i32,
    #[doc(hidden)]
    pub bn_quantity_precision: i32,
    pub max_leverage: i32,
    pub k1000: i32,
    #[doc(hidden)]
    pub bn_iceberg_parts: i32,
    #[doc(hidden)]
    pub bn_margin_table_id: i32,
    // --- Int64 (1) ---
    #[doc(hidden)]
    pub bn_delivery_time: i64,
    // --- Doubles (20) ---
    #[doc(hidden)]
    pub bn_tick_size: f64,
    #[doc(hidden)]
    pub bn_step_size: f64,
    #[doc(hidden)]
    pub bn_min_qty: f64,
    #[doc(hidden)]
    pub bn_max_qty: f64,
    #[doc(hidden)]
    pub bn_min_notional: f64,
    #[doc(hidden)]
    pub bn_max_notional: f64,
    #[doc(hidden)]
    pub bn_contract_size: f64,
    #[doc(hidden)]
    pub bn_min_price: f64,
    #[doc(hidden)]
    pub bn_max_price: f64,
    #[doc(hidden)]
    pub bn_max_value: f64,
    #[doc(hidden)]
    pub bn_multiplier_up: f64,
    #[doc(hidden)]
    pub bn_multiplier_down: f64,
    pub bid_multiplier_up: f64,
    pub bid_multiplier_down: f64,
    pub ask_multiplier_up: f64,
    pub ask_multiplier_down: f64,
    #[doc(hidden)]
    pub int_bn_max_qty: f64,
    pub funding_rate: f64,
    pub funding_time: f64,
    pub volume: f64,
    // --- Booleans (5) ---
    pub is_btc_market: bool,
    pub status_trading: bool,
    pub has_1000_prefix_alias: bool,
    #[doc(hidden)]
    pub bn_iceberg: bool,
    #[doc(hidden)]
    pub bn_only_isolated: bool,
    // --- v2: FuturesType ---
    pub futures_type: BaseCurrency,
    // --- Active Lib live balance / position state (Delphi TMarket fields) ---
    pub initial_balance: f64,
    pub locked_balance: f64,
    pub pos_size: f64,
    pub pos_price: f64,
    pub liq_price: f64,
    pub pos_dir: OrderType,
    pub long_pos_size: f64,
    pub long_pos_price: f64,
    pub long_liq_price: f64,
    pub long_position_type: PositionType,
    pub short_pos_size: f64,
    pub short_pos_price: f64,
    pub short_liq_price: f64,
    pub short_position_type: PositionType,
    pub asset_balance: f64,
    pub asset_balance_full: f64,
    pub total_profit_b: f64,
    pub total_profit_l: f64,
    pub total_profit_s: f64,
    pub leverage_x: i32,
    pub position_type: PositionType,
    pub balance_hash: u64,
    pub last_balance_epoch: u16,
    // --- Active Lib live trade tail state (Delphi TMarket trade fields) ---
    pub trade_tail: MarketTradeState,
    // --- Active Lib live arbitrage state (Delphi TMarket.ArbSlots/ArbNow) ---
    #[doc(hidden)]
    pub arb_slots: HashMap<ArbPlatformCode, MarketArbSlot>,
}

/// Delphi `TMarket` live trade tail fields maintained from `MPC_TradesStream`.
///
/// These are not part of the wire `Market` snapshot written by
/// `WriteMarketToStream`: Delphi does not send them in `GetMarketsList`, but it
/// mutates them inline while processing trades. They live on `Market` (like the
/// balance/position and arbitrage live state above) so a trades datagram updates
/// the per-market object in place through its own lock, instead of a parallel
/// `MarketsState` map that would force a full copy-on-write clone of the whole
/// markets container on every trades datagram.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct MarketTradeState {
    /// Delphi `TMarket.LastGotAllTrades` (`GetTimeMS`) for futures trades.
    pub last_got_all_trades_ms: i64,
    /// Delphi `TMarket.LastGotSpotTrades` (`GetTimeMS`) for spot trades.
    pub last_got_spot_trades_ms: i64,
    /// Delphi `TMarket.LastTradePrice`.
    pub last_trade_price: f64,
    /// Delphi `TMarket.LastBuyPrice`; yes, Delphi updates this on `O_Sell`.
    pub last_buy_price: f64,
    /// Delphi `TMarket.LastSellPrice`; Delphi updates this on `O_Buy`.
    pub last_sell_price: f64,
    /// Delphi `TMarket.LastTradePriceEMA15`.
    pub last_trade_price_ema15: f64,
    /// Delphi `TMarket.LastTradePriceEMA5`.
    pub last_trade_price_ema5: f64,
    /// Delphi `TMarket.LastTradeKind = O_Sell`.
    pub last_trade_was_sell: bool,
}

impl MarketTradeState {
    pub(crate) fn apply_futures_trade_like_delphi(
        &mut self,
        price: f64,
        qty: f64,
        now_ms: i64,
        eps: f64,
    ) {
        let is_sell = qty < 0.0;
        self.last_got_all_trades_ms = now_ms;
        self.last_trade_price = price;
        self.last_trade_was_sell = is_sell;

        if self.last_trade_price_ema15 < eps {
            self.last_trade_price_ema15 = price;
        }
        if self.last_trade_price_ema5 < eps {
            self.last_trade_price_ema5 = price;
        }
        self.last_trade_price_ema15 = (self.last_trade_price_ema15 * 15.0 + price) / 16.0;
        self.last_trade_price_ema5 = (self.last_trade_price_ema5 * 5.0 + price) / 6.0;

        if is_sell {
            self.last_buy_price = price;
        } else {
            self.last_sell_price = price;
        }
    }

    pub(crate) fn apply_spot_trade_like_delphi(&mut self, now_ms: i64) {
        self.last_got_spot_trades_ms = now_ms;
    }
}

impl Market {
    /// Exchange symbol used by MoonBot/MoonProto, for example `BTCUSDT`.
    pub fn symbol(&self) -> &str {
        &self.bn_market_name
    }

    /// Exchange-side market currency/symbol component.
    pub fn exchange_market_currency(&self) -> &str {
        &self.bn_market_currency
    }

    /// Exchange status string from the market-list payload.
    pub fn exchange_status(&self) -> &str {
        &self.bn_status
    }

    pub fn price_precision(&self) -> i32 {
        self.bn_price_precision
    }

    pub fn quantity_precision(&self) -> i32 {
        self.bn_quantity_precision
    }

    pub fn iceberg_parts(&self) -> i32 {
        self.bn_iceberg_parts
    }

    pub fn margin_table_id(&self) -> i32 {
        self.bn_margin_table_id
    }

    pub fn delivery_time_ms(&self) -> i64 {
        self.bn_delivery_time
    }

    pub fn tick_size(&self) -> f64 {
        self.bn_tick_size
    }

    pub fn step_size(&self) -> f64 {
        self.bn_step_size
    }

    pub fn min_qty(&self) -> f64 {
        self.bn_min_qty
    }

    pub fn max_qty(&self) -> f64 {
        self.bn_max_qty
    }

    pub fn min_notional(&self) -> f64 {
        self.bn_min_notional
    }

    pub fn max_notional(&self) -> f64 {
        self.bn_max_notional
    }

    pub fn contract_size(&self) -> f64 {
        self.bn_contract_size
    }

    pub fn min_price(&self) -> f64 {
        self.bn_min_price
    }

    pub fn max_price(&self) -> f64 {
        self.bn_max_price
    }

    pub fn max_value(&self) -> f64 {
        self.bn_max_value
    }

    pub fn multiplier_up(&self) -> f64 {
        self.bn_multiplier_up
    }

    pub fn multiplier_down(&self) -> f64 {
        self.bn_multiplier_down
    }

    pub fn internal_max_qty(&self) -> f64 {
        self.int_bn_max_qty
    }

    pub fn iceberg_enabled(&self) -> bool {
        self.bn_iceberg
    }

    pub fn only_isolated(&self) -> bool {
        self.bn_only_isolated
    }

    /// Listed-on-exchange kind derived from `futures_type`.
    pub fn listed_type(&self) -> ListedType {
        if self.futures_type == BaseCurrency::EMPTY {
            ListedType::SPOT
        } else {
            ListedType::BOTH
        }
    }

    /// Delphi `GetMarketsList` post-pass:
    /// `FuturesType <> BC_EMPTY -> L_Both`, otherwise `L_Spot`.
    #[doc(hidden)]
    pub fn listed_type_like_delphi(&self) -> ListedType {
        self.listed_type()
    }

    /// Delphi `TMarket.FTotalProfit`.
    pub fn total_profit(&self) -> f64 {
        self.total_profit_b + self.total_profit_l + self.total_profit_s
    }

    pub fn funding_time_delphi(&self) -> DelphiTime {
        DelphiTime::from_days(self.funding_time)
    }
}

pub const ARB_PRICE_RING_LEN: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct MarketArbPricePoint {
    pub price: f32,
    pub time: f64,
    pub my_price: f32,
}

impl MarketArbPricePoint {
    pub fn time_delphi(self) -> DelphiTime {
        DelphiTime::from_days(self.time)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct MarketArbNowEntry {
    pub price: f32,
    pub time: f64,
}

impl MarketArbNowEntry {
    pub fn time_delphi(self) -> DelphiTime {
        DelphiTime::from_days(self.time)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MarketArbSlot {
    pub(crate) ring: [MarketArbPricePoint; ARB_PRICE_RING_LEN],
    pub enabled: bool,
    pub(crate) head: u8,
    pub isolated_flags: ArbIsolationFlags,
    pub(crate) isolated_flags_tmp: ArbIsolationFlags,
    pub now: MarketArbNowEntry,
}

impl Default for MarketArbSlot {
    fn default() -> Self {
        Self {
            ring: [MarketArbPricePoint::default(); ARB_PRICE_RING_LEN],
            enabled: false,
            head: 0,
            isolated_flags: ArbIsolationFlags::None,
            isolated_flags_tmp: ArbIsolationFlags::None,
            now: MarketArbNowEntry::default(),
        }
    }
}

impl MarketArbSlot {
    /// Current write head inside the fixed Delphi 10-point arb ring.
    #[doc(hidden)]
    pub fn head_index(&self) -> usize {
        self.head as usize
    }

    /// Latest point written to the fixed Delphi ring.
    pub fn latest_point(&self) -> MarketArbPricePoint {
        self.ring[self.head_index()]
    }

    /// Return ring points in chronological order without exposing the raw
    /// ring cursor as public mutable state.
    pub fn points_oldest_first(&self) -> [MarketArbPricePoint; ARB_PRICE_RING_LEN] {
        let mut out = [MarketArbPricePoint::default(); ARB_PRICE_RING_LEN];
        let start = (self.head_index() + 1) % ARB_PRICE_RING_LEN;
        for (dst, src) in out.iter_mut().enumerate() {
            *src = self.ring[(start + dst) % ARB_PRICE_RING_LEN];
        }
        out
    }
}

/// Read `TMarket` from `EngineStreamReader`, byte-exact with Delphi
/// `ReadMarketFromStream`.
///
/// `ver >= 2` means the payload contains the trailing `FuturesType` byte.
#[doc(hidden)]
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
    let has_1000_prefix_alias = r.read_bool()?;
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
        has_1000_prefix_alias,
        bn_iceberg,
        bn_only_isolated,
        futures_type,
        initial_balance: 0.0,
        locked_balance: 0.0,
        pos_size: 0.0,
        pos_price: 0.0,
        liq_price: 0.0,
        pos_dir: OrderType::Sell,
        long_pos_size: 0.0,
        long_pos_price: 0.0,
        long_liq_price: 0.0,
        long_position_type: PositionType::Cross,
        short_pos_size: 0.0,
        short_pos_price: 0.0,
        short_liq_price: 0.0,
        short_position_type: PositionType::Cross,
        asset_balance: 0.0,
        asset_balance_full: 0.0,
        total_profit_b: 0.0,
        total_profit_l: 0.0,
        total_profit_s: 0.0,
        leverage_x: 1,
        position_type: PositionType::Cross,
        balance_hash: 0,
        last_balance_epoch: 0,
        trade_tail: MarketTradeState::default(),
        arb_slots: HashMap::new(),
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

/// Serialize `Market` into an `EngineStreamReader`-compatible byte stream.
///
/// This mirrors Delphi `WriteMarketToStream`. `FuturesType` is always written,
/// as in the reference implementation; `ver` is kept only for symmetry with
/// `read_market`.
#[doc(hidden)]
pub fn write_market(out: &mut Vec<u8>, m: &Market, _ver: u16) {
    write_market_with_local_shift(out, m, _ver, current_local_time_shift_minutes())
}

pub(super) fn write_market_with_local_shift(
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
    out.push(m.has_1000_prefix_alias as u8);
    out.push(m.bn_iceberg as u8);
    out.push(m.bn_only_isolated as u8);

    // Delphi: WriteMarketToStream пишет FuturesType всегда (без guard ver).
    out.push(m.futures_type.to_byte());
}

pub(super) fn remove_delphi_local_funding_shift(
    local_funding_time: f64,
    local_shift_minutes: f64,
) -> f64 {
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

/// Delphi `TCorrMarket` correlation-market row.
///
/// Byte-exact with `WriteCorrMarketToStream`
/// (`MoonProtoSerialization.pas:169-178`).
#[derive(Debug, Clone, PartialEq)]
#[doc(hidden)]
pub struct CorrMarket {
    pub bn_market_name: String,
    pub bn_market_currency: String,
    pub bn_tick_size: f64,
    /// Base-currency name; empty string when Delphi had `nil`.
    pub base_currency_name: String,
}

#[doc(hidden)]
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

#[doc(hidden)]
pub fn write_corr_market(out: &mut Vec<u8>, c: &CorrMarket) {
    write_str(out, &c.bn_market_name);
    write_str(out, &c.bn_market_currency);
    out.extend_from_slice(&c.bn_tick_size.to_le_bytes());
    write_str(out, &c.base_currency_name);
}

// =============================================================================
//  Tests
// =============================================================================

#[cfg(test)]
mod tests;
