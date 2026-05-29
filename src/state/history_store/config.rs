//! Retained-history scope and capacity configuration.

use std::collections::BTreeSet;
use std::mem::size_of;

use crate::state::history::{
    Candle5mRow, LastPricePoint, MMOrderCompanionData, MMOrderHistoryRow, MarkPricePoint,
    MiniCandle, TradeHistoryRow,
};

pub(super) const GIB: usize = 1024 * 1024 * 1024;
const TRADE_SLOT_BYTES: usize = size_of::<TradeHistoryRow>();
const MM_ORDER_SLOT_BYTES: usize = size_of::<MMOrderHistoryRow>();
const MM_COMPANION_SLOT_BYTES: usize = size_of::<MMOrderCompanionData>();
const LAST_PRICE_SLOT_BYTES: usize = size_of::<LastPricePoint>();
const MARK_PRICE_SLOT_BYTES: usize = size_of::<MarkPricePoint>();
const MINI_CANDLE_SLOT_BYTES: usize = size_of::<MiniCandle>();
const CANDLE_5M_SLOT_BYTES: usize = size_of::<Candle5mRow>();

/// Active-library retained-history scope for the all-trades stream.
///
/// Delphi `SubscribeAllTrades` has no per-market scope: all known markets are
/// maintained once the stream is enabled. Rust additionally exposes an accepted
/// API deviation for UI clients that want to retain only a subset locally while
/// keeping the same wire `emk_SubscribeAllTrades` command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TradeStorageScope {
    All,
    Markets(BTreeSet<String>),
}

impl Default for TradeStorageScope {
    fn default() -> Self {
        Self::All
    }
}

impl TradeStorageScope {
    pub fn all() -> Self {
        Self::All
    }

    pub fn from_markets<I, S>(market_names: I) -> Self
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

    pub fn contains(&self, market_name: &str) -> bool {
        match self {
            Self::All => true,
            Self::Markets(names) => names.contains(market_name),
        }
    }

    pub fn is_all(&self) -> bool {
        matches!(self, Self::All)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketHistoryConfig {
    pub futures_trades_capacity: usize,
    pub spot_trades_capacity: usize,
    pub liquidation_capacity: usize,
    /// Capacity of the MM-order ring. The taker/color companion ring rides this
    /// same capacity (Delphi `TStreamableRingBuffer<TMMOrder, TMMOrderData>` has a
    /// single `FSize` for both parallel arrays), so an order and its companion
    /// always evict together and can never desync.
    pub mm_orders_capacity: usize,
    pub last_price_capacity: usize,
    pub mini_candles_capacity: usize,
    pub candles_5m_capacity: usize,
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
        system_total_memory_bytes()
            .map(|total| Self::from_total_memory_bytes(total, market_count))
            .unwrap_or_default()
    }

    pub fn from_total_memory_bytes(total_memory_bytes: usize, market_count: usize) -> Self {
        let market_count = market_count.max(1);
        let budget = Self::history_budget_bytes(total_memory_bytes);
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
        if total_memory_bytes < 8 * GIB {
            total_memory_bytes / 4
        } else {
            total_memory_bytes / 5
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
