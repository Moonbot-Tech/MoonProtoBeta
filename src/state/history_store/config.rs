//! Retained-history scope and capacity configuration.

use std::collections::BTreeSet;
use std::mem::size_of;

use crate::state::history::{
    Candle5mRow, LastPricePoint, MMOrderCompanionData, MMOrderHistoryRow, MarkPricePoint,
    MiniCandle, TradeHistoryRow,
};

pub(super) const GIB: usize = 1024 * 1024 * 1024;
const DEFAULT_HISTORY_BUDGET_PERCENT: u16 = 100;
const MIN_HISTORY_BUDGET_PERCENT: u16 = 100;
const MAX_HISTORY_BUDGET_PERCENT: u16 = 800;
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
/// `Auto` sizes per-market rings from total system memory once the active trade
/// storage scope and known market list are available. `Fixed` uses the supplied
/// capacities verbatim; set individual capacities to `0` to disable that
/// retained public history category.
///
/// This enum is non-exhaustive because retained-history sizing can gain new
/// policies as UI memory controls become more precise. Match with a wildcard
/// outside this crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum MarketHistorySizing {
    #[default]
    Auto,
    /// Auto sizing with a user-visible memory budget multiplier.
    ///
    /// `100` means the default memory-aware budget. Larger values keep the same
    /// proportional split between rings but allow more retained rows, clamped to
    /// `100..=800` like MoonBot's chart/trade memory setting.
    AutoBudgetPercent(u16),
    Fixed(MarketHistoryConfig),
}

impl MarketHistorySizing {
    pub const DEFAULT_BUDGET_PERCENT: u16 = DEFAULT_HISTORY_BUDGET_PERCENT;
    pub const MIN_BUDGET_PERCENT: u16 = MIN_HISTORY_BUDGET_PERCENT;
    pub const MAX_BUDGET_PERCENT: u16 = MAX_HISTORY_BUDGET_PERCENT;

    pub fn fixed(config: MarketHistoryConfig) -> Self {
        Self::Fixed(config)
    }

    pub fn auto_with_budget_percent(percent: u16) -> Self {
        Self::AutoBudgetPercent(Self::clamp_budget_percent(percent))
    }

    pub fn clamp_budget_percent(percent: u16) -> u16 {
        percent.clamp(MIN_HISTORY_BUDGET_PERCENT, MAX_HISTORY_BUDGET_PERCENT)
    }

    pub(crate) fn resolve(self, market_count: usize) -> MarketHistoryConfig {
        match self {
            Self::Auto => MarketHistoryConfig::from_system_memory(market_count),
            Self::AutoBudgetPercent(percent) => {
                MarketHistoryConfig::from_system_memory_with_budget_percent(market_count, percent)
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
        Self {
            futures_trades_capacity: 10_000,
            spot_trades_capacity: 5_000,
            liquidation_capacity: 2_000,
            mm_orders_capacity: 2_000,
            last_price_capacity: 5_000,
            mini_candles_capacity: 5_000,
            candles_5m_capacity: 5_000,
        }
    }
}

impl MarketHistoryConfig {
    pub fn from_system_memory(market_count: usize) -> Self {
        Self::from_system_memory_with_budget_percent(market_count, DEFAULT_HISTORY_BUDGET_PERCENT)
    }

    pub fn from_system_memory_with_budget_percent(
        market_count: usize,
        budget_percent: u16,
    ) -> Self {
        system_total_memory_bytes()
            .map(|total| {
                Self::from_total_memory_bytes_with_budget_percent(
                    total,
                    market_count,
                    budget_percent,
                )
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
        market_count: usize,
        budget_percent: u16,
    ) -> Self {
        let market_count = market_count.max(1);
        let budget =
            Self::history_budget_bytes_with_budget_percent(total_memory_bytes, budget_percent);
        let per_market_budget = budget / market_count;

        let futures_trades_capacity =
            capacity_from_share(per_market_budget, 32, 100, TRADE_SLOT_BYTES, 200_000);
        let spot_trades_capacity =
            capacity_from_share(per_market_budget, 18, 100, TRADE_SLOT_BYTES, 150_000);
        let liquidation_capacity =
            capacity_from_share(per_market_budget, 7, 100, TRADE_SLOT_BYTES, 50_000);
        // Single MM-order ring carries both the row and its taker/color companion
        // (Delphi single-`FSize` parallel arrays). Budget the combined per-slot cost
        // with the former orders+companion share (7%+7%) so retained count and memory
        // footprint are unchanged versus the previous two-ring sizing.
        let mm_orders_capacity = capacity_from_share(
            per_market_budget,
            14,
            100,
            MM_ORDER_SLOT_BYTES + MM_COMPANION_SLOT_BYTES,
            50_000,
        );
        let last_price_capacity =
            capacity_from_share(per_market_budget, 8, 100, LAST_PRICE_SLOT_BYTES, 80_000);
        let mini_candles_capacity =
            capacity_from_share(per_market_budget, 6, 100, MINI_CANDLE_SLOT_BYTES, 50_000);
        let candles_5m_capacity =
            capacity_from_share(per_market_budget, 3, 100, CANDLE_5M_SLOT_BYTES, 20_000);

        Self {
            futures_trades_capacity,
            spot_trades_capacity,
            liquidation_capacity,
            mm_orders_capacity,
            last_price_capacity,
            mini_candles_capacity,
            candles_5m_capacity,
        }
    }

    pub fn history_budget_bytes(total_memory_bytes: usize) -> usize {
        Self::history_budget_bytes_with_budget_percent(
            total_memory_bytes,
            DEFAULT_HISTORY_BUDGET_PERCENT,
        )
    }

    pub fn history_budget_bytes_with_budget_percent(
        total_memory_bytes: usize,
        budget_percent: u16,
    ) -> usize {
        let budget_percent = MarketHistorySizing::clamp_budget_percent(budget_percent) as usize;
        if total_memory_bytes < 8 * GIB {
            (total_memory_bytes / 4).saturating_mul(budget_percent) / 100
        } else {
            (total_memory_bytes / 5).saturating_mul(budget_percent) / 100
        }
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

fn capacity_from_share(
    budget: usize,
    numerator: usize,
    denominator: usize,
    row_bytes: usize,
    max_capacity: usize,
) -> usize {
    if budget == 0 || row_bytes == 0 || denominator == 0 {
        return 0;
    }
    ((budget / denominator) * numerator / row_bytes).min(max_capacity)
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
