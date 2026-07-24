//! Retained-history scope and capacity configuration.

use std::collections::BTreeSet;
use std::mem::size_of;

use crate::commands::market::ExchangeCode;
use crate::state::history::{
    Candle5mRow, LastPricePoint, MMOrderCompanionData, MMOrderHistoryRow, MarkPricePoint,
    MiniCandle, TradeHistoryRow,
};

pub(super) const GB: usize = 1_000_000_000;
const GIB: usize = 1024 * 1024 * 1024;
const DEFAULT_HISTORY_BUDGET_PERCENT: u16 = 100;
const MIN_HISTORY_BUDGET_PERCENT: u16 = 75;
const MAX_HISTORY_BUDGET_PERCENT: u16 = 800;
const DEFAULT_MM_ORDERS_CAPACITY: usize = 25_000;
const DEEP_5M_CAPACITY: usize = 500;
const TRADE_SLOT_BYTES: usize = size_of::<TradeHistoryRow>();
const MM_ORDER_SLOT_BYTES: usize = size_of::<MMOrderHistoryRow>();
const MM_COMPANION_SLOT_BYTES: usize = size_of::<MMOrderCompanionData>();
const LAST_PRICE_SLOT_BYTES: usize = size_of::<LastPricePoint>();
const MARK_PRICE_SLOT_BYTES: usize = size_of::<MarkPricePoint>();
const MINI_CANDLE_SLOT_BYTES: usize = size_of::<MiniCandle>();
const CANDLE_5M_SLOT_BYTES: usize = size_of::<Candle5mRow>();

/// Active-library retained-history scope for the all-trades stream.
///
/// The wire all-trades subscription is global. Active Lib additionally exposes
/// an accepted retained-storage scope for UI clients that want to keep only a
/// subset locally while still using the same wire `emk_SubscribeAllTrades`
/// command.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) enum TradeStorageScope {
    #[default]
    All,
    Markets(BTreeSet<String>),
}

impl TradeStorageScope {
    pub(crate) fn from_markets<I, S>(market_names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let names = market_names
            .into_iter()
            .map(|name| name.as_ref().to_string())
            .filter(|name| !name.is_empty())
            .collect::<BTreeSet<_>>();
        if names.is_empty() {
            Self::All
        } else {
            Self::Markets(names)
        }
    }

    pub(crate) fn contains(&self, market_name: &str) -> bool {
        match self {
            Self::All => true,
            Self::Markets(names) => names.contains(market_name),
        }
    }

    #[cfg(test)]
    pub(crate) fn is_all(&self) -> bool {
        matches!(self, Self::All)
    }
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketHistoryConfig {
    pub futures_trades_capacity: usize,
    pub spot_trades_capacity: usize,
    pub liquidation_capacity: usize,
    /// Capacity of the MM-order ring. The taker/color companion ring rides this
    /// same capacity because each MM-order row has a paired companion row, so
    /// an order and its companion always evict together and can never desync.
    pub mm_orders_capacity: usize,
    pub last_price_capacity: usize,
    pub mini_candles_capacity: usize,
    pub candles_5m_capacity: usize,
}

/// Capacity policy for Active Lib retained market history.
///
/// `Auto` derives per-market depth from system memory and the connected
/// exchange, then allocates each dense ring only when that market/category
/// receives data. Normal applications choose `Auto` or one percentage.
///
/// This enum is non-exhaustive because retained-history sizing can gain new
/// policies as UI memory controls become more precise. Match with a wildcard
/// outside this crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum MarketHistorySizing {
    #[default]
    Auto,
    /// Auto sizing with a user-visible retained-depth percentage.
    ///
    /// `100` is the production-core baseline. `75` keeps shorter heavy
    /// histories for memory-constrained terminals; values above `100` extend
    /// detailed trade history up to the same production caps.
    AutoBudgetPercent(u16),
    #[doc(hidden)]
    Fixed(MarketHistoryConfig),
}

impl MarketHistorySizing {
    pub const DEFAULT_BUDGET_PERCENT: u16 = DEFAULT_HISTORY_BUDGET_PERCENT;
    pub const MIN_BUDGET_PERCENT: u16 = MIN_HISTORY_BUDGET_PERCENT;
    pub const MAX_BUDGET_PERCENT: u16 = MAX_HISTORY_BUDGET_PERCENT;

    #[doc(hidden)]
    pub fn fixed(config: MarketHistoryConfig) -> Self {
        Self::Fixed(config)
    }

    /// Use the production memory/exchange policy with a retained-depth control.
    ///
    /// Values are clamped to [`Self::MIN_BUDGET_PERCENT`] through
    /// [`Self::MAX_BUDGET_PERCENT`]; `100` is the production baseline.
    pub fn auto_with_budget_percent(percent: u16) -> Self {
        Self::AutoBudgetPercent(Self::clamp_budget_percent(percent))
    }

    pub fn clamp_budget_percent(percent: u16) -> u16 {
        percent.clamp(MIN_HISTORY_BUDGET_PERCENT, MAX_HISTORY_BUDGET_PERCENT)
    }

    pub(crate) fn resolve(self, exchange_code: Option<ExchangeCode>) -> MarketHistoryConfig {
        match self {
            Self::Auto => MarketHistoryConfig::from_system_memory_for_exchange(exchange_code),
            Self::AutoBudgetPercent(percent) => {
                MarketHistoryConfig::from_system_memory_for_exchange_with_budget_percent(
                    exchange_code,
                    percent,
                )
            }
            Self::Fixed(config) => config,
        }
    }
}

impl From<MarketHistoryConfig> for MarketHistorySizing {
    fn from(config: MarketHistoryConfig) -> Self {
        Self::Fixed(config)
    }
}

impl Default for MarketHistoryConfig {
    fn default() -> Self {
        Self::from_total_memory_bytes_for_exchange(4 * GB, DEFAULT_HISTORY_BUDGET_PERCENT, None)
    }
}

impl MarketHistoryConfig {
    /// Build production-shaped capacities for the current machine.
    ///
    /// `market_count` is retained for source compatibility. Auto histories are
    /// now allocated per category on first use, so an unused market no longer
    /// needs a pre-divided share of one process-wide eager-allocation budget.
    pub fn from_system_memory(market_count: usize) -> Self {
        Self::from_system_memory_with_budget_percent(market_count, DEFAULT_HISTORY_BUDGET_PERCENT)
    }

    pub fn from_system_memory_with_budget_percent(
        _market_count: usize,
        budget_percent: u16,
    ) -> Self {
        Self::from_system_memory_for_exchange_with_budget_percent(None, budget_percent)
    }

    fn from_system_memory_for_exchange(exchange_code: Option<ExchangeCode>) -> Self {
        Self::from_system_memory_for_exchange_with_budget_percent(
            exchange_code,
            DEFAULT_HISTORY_BUDGET_PERCENT,
        )
    }

    fn from_system_memory_for_exchange_with_budget_percent(
        exchange_code: Option<ExchangeCode>,
        budget_percent: u16,
    ) -> Self {
        system_total_memory_bytes()
            .map(|total| {
                Self::from_total_memory_bytes_for_exchange(total, budget_percent, exchange_code)
            })
            .unwrap_or_default()
    }

    pub fn from_total_memory_bytes(total_memory_bytes: usize, market_count: usize) -> Self {
        Self::from_total_memory_bytes_with_budget_percent(
            total_memory_bytes,
            market_count,
            DEFAULT_HISTORY_BUDGET_PERCENT,
        )
    }

    pub fn from_total_memory_bytes_with_budget_percent(
        total_memory_bytes: usize,
        _market_count: usize,
        budget_percent: u16,
    ) -> Self {
        Self::from_total_memory_bytes_for_exchange(total_memory_bytes, budget_percent, None)
    }

    pub(super) fn from_total_memory_bytes_for_exchange(
        total_memory_bytes: usize,
        budget_percent: u16,
        exchange_code: Option<ExchangeCode>,
    ) -> Self {
        // The production policy chooses retained row depth from machine memory
        // and exchange traffic shape. It is not a fraction of total RAM per
        // known market: category-lazy rings make unused capacities free.
        let budget_percent = MarketHistorySizing::clamp_budget_percent(budget_percent);
        let base = base_history_len(total_memory_bytes);
        let gate = exchange_code == Some(ExchangeCode::Gate);
        let mut price_history_capacity = if gate {
            base.saturating_mul(3) / 2
        } else {
            base.saturating_mul(22) / 10 * 2
        };
        let base_trades_capacity = match exchange_code {
            Some(ExchangeCode::QBinance) => price_history_capacity.saturating_mul(4).min(58_000),
            Some(ExchangeCode::FBinance) => price_history_capacity.saturating_mul(3).min(48_000),
            Some(ExchangeCode::Gate) => price_history_capacity.saturating_mul(2).min(28_000),
            _ => price_history_capacity.saturating_mul(3).min(44_000),
        };

        let scale_extended = total_memory_bytes > 4 * GB || budget_percent < 100;
        let futures_trades_capacity = if scale_extended {
            scale_capacity_rounded_to_8(base_trades_capacity, budget_percent).min(98_000)
        } else {
            base_trades_capacity
        };
        let mini_candles_capacity = if scale_extended {
            scale_capacity_rounded_to_8(price_history_capacity, budget_percent).min(25_000)
        } else {
            price_history_capacity
        };

        if exchange_code == Some(ExchangeCode::QBinance) {
            price_history_capacity = price_history_capacity.saturating_mul(3);
        }
        if budget_percent < 100 {
            price_history_capacity =
                scale_capacity_rounded_to_8(price_history_capacity, budget_percent);
        }
        let mm_orders_capacity = if budget_percent < 100 {
            scale_capacity_rounded_to_8(DEFAULT_MM_ORDERS_CAPACITY, budget_percent)
        } else {
            DEFAULT_MM_ORDERS_CAPACITY
        };

        Self {
            futures_trades_capacity,
            spot_trades_capacity: futures_trades_capacity,
            liquidation_capacity: price_history_capacity,
            mm_orders_capacity,
            last_price_capacity: price_history_capacity,
            mini_candles_capacity,
            candles_5m_capacity: DEEP_5M_CAPACITY,
        }
    }

    /// Legacy eager-budget estimate retained for source compatibility.
    ///
    /// Auto sizing no longer divides this value between known markets. Dense
    /// histories are allocated lazily by category; use
    /// [`Self::estimated_bytes_per_market`] for the all-categories materialized
    /// size of a resolved configuration.
    #[deprecated(
        note = "Auto history is category-lazy; use estimated_bytes_per_market on the resolved config"
    )]
    pub fn history_budget_bytes(total_memory_bytes: usize) -> usize {
        legacy_history_budget_bytes(total_memory_bytes, DEFAULT_HISTORY_BUDGET_PERCENT)
    }

    #[deprecated(
        note = "Auto history is category-lazy; use estimated_bytes_per_market on the resolved config"
    )]
    pub fn history_budget_bytes_with_budget_percent(
        total_memory_bytes: usize,
        budget_percent: u16,
    ) -> usize {
        legacy_history_budget_bytes(total_memory_bytes, budget_percent)
    }

    pub fn estimated_bytes_per_market(&self) -> usize {
        self.futures_trades_capacity * TRADE_SLOT_BYTES
            + self.spot_trades_capacity * TRADE_SLOT_BYTES
            + self.liquidation_capacity * TRADE_SLOT_BYTES
            + self.mm_orders_capacity * MM_ORDER_SLOT_BYTES
            + self.mm_orders_capacity * MM_COMPANION_SLOT_BYTES
            + self.last_price_capacity * LAST_PRICE_SLOT_BYTES
            + self.last_price_capacity * MARK_PRICE_SLOT_BYTES
            + self.mini_candles_capacity * MINI_CANDLE_SLOT_BYTES
            + self.candles_5m_capacity * CANDLE_5M_SLOT_BYTES
    }
}

fn base_history_len(total_memory_bytes: usize) -> usize {
    // The production core computes TotalMemGB with decimal gigabytes.
    if total_memory_bytes < 3 * GB {
        1_200
    } else if total_memory_bytes < 4 * GB {
        1_600
    } else if total_memory_bytes < 6 * GB {
        2_400
    } else if total_memory_bytes < 9 * GB {
        4_000
    } else if total_memory_bytes < 18 * GB {
        5_200
    } else {
        6_200
    }
}

pub(super) fn scale_capacity_rounded_to_8(capacity: usize, percent: u16) -> usize {
    let numerator = capacity as u64 * u64::from(percent);
    let mut scaled_units = numerator / 800;
    let remainder = numerator % 800;
    if remainder > 400 || (remainder == 400 && scaled_units % 2 != 0) {
        scaled_units += 1;
    }
    usize::try_from(scaled_units.saturating_mul(8)).unwrap_or(usize::MAX)
}

fn legacy_history_budget_bytes(total_memory_bytes: usize, budget_percent: u16) -> usize {
    let budget_percent = MarketHistorySizing::clamp_budget_percent(budget_percent) as usize;
    if total_memory_bytes < 8 * GIB {
        (total_memory_bytes / 4).saturating_mul(budget_percent) / 100
    } else {
        (total_memory_bytes / 5).saturating_mul(budget_percent) / 100
    }
}

fn system_total_memory_bytes() -> Option<usize> {
    system_total_memory_bytes_impl()
}

#[cfg(windows)]
fn system_total_memory_bytes_impl() -> Option<usize> {
    #[repr(C)]
    struct MemoryStatusEx {
        dw_length: u32,
        dw_memory_load: u32,
        ull_total_phys: u64,
        ull_avail_phys: u64,
        ull_total_page_file: u64,
        ull_avail_page_file: u64,
        ull_total_virtual: u64,
        ull_avail_virtual: u64,
        ull_avail_extended_virtual: u64,
    }

    #[link(name = "kernel32")]
    extern "system" {
        fn GlobalMemoryStatusEx(buffer: *mut MemoryStatusEx) -> i32;
    }

    let mut status = MemoryStatusEx {
        dw_length: size_of::<MemoryStatusEx>() as u32,
        dw_memory_load: 0,
        ull_total_phys: 0,
        ull_avail_phys: 0,
        ull_total_page_file: 0,
        ull_avail_page_file: 0,
        ull_total_virtual: 0,
        ull_avail_virtual: 0,
        ull_avail_extended_virtual: 0,
    };
    let ok = unsafe { GlobalMemoryStatusEx(&mut status) };
    if ok == 0 {
        return None;
    }
    usize::try_from(status.ull_total_phys).ok()
}

#[cfg(unix)]
fn system_total_memory_bytes_impl() -> Option<usize> {
    let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if pages <= 0 || page_size <= 0 {
        return None;
    }
    let pages = usize::try_from(pages).ok()?;
    let page_size = usize::try_from(page_size).ok()?;
    pages.checked_mul(page_size)
}

#[cfg(not(any(windows, unix)))]
fn system_total_memory_bytes_impl() -> Option<usize> {
    None
}
