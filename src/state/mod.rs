//! Read models maintained by `EventDispatcher`.
//!
//! Each MoonProto channel has a matching state module:
//! - `Orders` for trade-command state.
//! - `Strats` for strategy snapshots and updates.
//! - `Balances` for account and per-market balances.
//! - `Markets` for Engine API market list, indexes, prices, and tags.
//! - `OrderBooks` for snapshots, diffs, and reordering caches.
//! - `Trades` for stream packets and automatic gap recovery.
//! - `Settings` for settings snapshots and UI control events.
//!
//! Normal applications read these models through immutable `MoonClient`
//! snapshots. Custom runtimes can read them through `EventDispatcher` getters.
//! The per-channel guides live in `moonproto/docs/api/<channel>.md`.

pub mod balances;
pub mod epoch;
pub mod history;
pub mod history_store;
pub mod history_worker;
pub mod markets;
pub mod order_books;
pub mod orders;
pub mod seq_ring;
pub mod settings;
pub mod strats;
pub mod trades;

pub use balances::{BalanceEvent, BalancesState, GlobalBalance};
pub use history::{
    compact_trades_to_mini_candles_like_delphi, hl_address_color_like_delphi, Candle5mRow,
    CandleVolumeSnapshot, DerivedDeltaSnapshot, LastPricePoint, MMOrderCompanionData,
    MMOrderHistoryRow, MarketDerivedSnapshot, MiniCandle, RollingTradeVolumeSnapshot,
    RollingTradeVolumes, TradeHistoryRow, TradeVolumeTotals, TradesPacketTimeShift,
    DELPHI_MSECS_PER_DAY, DELPHI_SAME_TRADES_TIME_DAYS,
};
pub use history_store::{
    MarketHistoryConfig, MarketHistoryReaders, MarketHistoryRegistry, MarketHistoryStore,
    TradeStorageScope,
};
pub use history_worker::{
    MarketHistoryCandlesSnapshot, MarketHistoryHandle, MarketHistoryLastPriceBatch,
    MarketHistoryLastPriceInput, MarketHistoryMMOrderInput, MarketHistoryStreamBatch,
    MarketHistoryStreamSection, MarketHistoryTradeInput, MarketHistoryWorker,
};
pub use markets::{
    MarketHandle, MarketPrice, MarketTradeState, MarketsEvent, MarketsListApplyTiming, MarketsState,
};
pub use order_books::{
    ApplyResult as OrderBookApplyResult, OrderBookEvent, OrderBookKind, OrderBookLevel,
    OrderBookSnapshot, OrderBooks, TopOfBook,
};
pub use orders::{
    ApplyResult, Order, OrderEvent, OrderTraceChartPoint, OrderTraceLine, Orders, SellReason,
};
pub use seq_ring::{
    SeqRingBounds, SeqRingCursor, SeqRingError, SeqRingReadMeta, SeqRingReadView, SeqRingReader,
    SeqRingRow, SeqRingTimedRow, SeqRingWriter,
};
pub use settings::{SettingsEvent, SettingsState};
pub use strats::{StratEvent, StrategyInfo, StratsState};
pub(crate) use trades::TradesPacketEffect;
pub use trades::{
    iter_trades_resend_response, TradesEvent, TradesResendResponsePackets, TradesState,
};
