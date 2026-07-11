//! Read models maintained by `EventDispatcher`.
//!
//! Each MoonProto channel has a matching state module:
//! - `Orders` for trade-command state.
//! - `Strats` for strategy snapshots and updates.
//! - `Balances` for account and per-market balances.
//! - `Account` for account-mode/API-expiration refresh state.
//! - `Markets` for Engine API market list, indexes, prices, and tags.
//! - `OrderBooks` for snapshots, diffs, and reordering caches.
//! - `Trades` for stream packets and automatic gap recovery.
//! - `Settings` for settings snapshots and UI control events.
//!
//! Normal applications read these models through immutable `MoonClient`
//! snapshots. Custom runtimes can read them through `EventDispatcher` getters.
//! The per-channel guides live in `moonproto/docs/<channel>.md`.

pub(crate) mod account;
pub(crate) mod balances;
pub(crate) mod chart_ui;
pub(crate) mod coin_card_candles;
pub(crate) mod epoch;
pub(crate) mod eps;
pub(crate) mod history;
pub(crate) mod history_store;
pub(crate) mod history_worker;
pub(crate) mod markets;
pub(crate) mod order_books;
pub(crate) mod orders;
pub(crate) mod report;
pub(crate) mod seq_ring;
pub(crate) mod settings;
pub(crate) mod strats;
pub(crate) mod trades;
pub(crate) mod transfer_assets;

pub use account::{AccountEvent, AccountState};
pub use balances::{BalanceEvent, BalancesState, GlobalBalance};
pub use chart_ui::{
    ChartAlertEvent, ChartAlertObject, ChartAlertsState, ChartTextSnapshot, ChartTextState,
};
pub(crate) use coin_card_candles::LiveCandleApply;
pub use coin_card_candles::{CoinCardCandlesEvent, CoinCardCandlesState};
pub use history::{
    hl_address_color, hl_address_hex, Candle5mRow, CandleVolumeSnapshot,
    CandlesSnapshotApplySummary, CandlesSnapshotEvent, DerivedDeltaSnapshot, LastPricePoint,
    MMOrderCompanionData, MMOrderHistoryRow, MarkPricePoint, MarketDerivedSnapshot, MiniCandle,
    RollingTradeVolumeSnapshot, TradeHistoryRow, TradeVolumeTotals,
};
pub(crate) use history_store::TradeStorageScope;
pub use history_store::{MarketHistoryConfig, MarketHistoryReaders, MarketHistorySizing};
pub(crate) use history_worker::{
    MarketHistoryCandlesSnapshot, MarketHistoryHandle, MarketHistoryLastPriceBatch,
    MarketHistoryLastPriceInput, MarketHistoryMMOrderInput, MarketHistoryStreamBatch,
    MarketHistoryStreamSection, MarketHistoryStreamSectionKind, MarketHistoryTradeInput,
    MarketHistoryWorker,
};
#[cfg(feature = "diagnostics")]
#[doc(hidden)]
pub use markets::MarketsListApplyTiming;
pub use markets::{
    BaseCurrencyPrice, MarketBalancePosition, MarketDeltaState, MarketGlobalDeltas, MarketHandle,
    MarketPrice, MarketTradeState, MarketsEvent, MarketsState,
};
pub(crate) use order_books::OrderBookControl;
pub use order_books::{
    OrderBookEvent, OrderBookKind, OrderBookLevel, OrderBookReadGuard, OrderBookSnapshot,
    OrderBooks, TopOfBook,
};
#[cfg(any(test, feature = "diagnostics"))]
#[doc(hidden)]
pub use orders::ApplyResult;
#[cfg(not(any(test, feature = "diagnostics")))]
pub(crate) use orders::ApplyResult;
pub use orders::{
    MarketPositionProtection, Order, OrderEvent, OrderTraceChartPoint, OrderTraceLine, Orders,
    PositionProtectionSide, SellReason,
};
pub(crate) use report::{ReportControl, ReportPageApplyAction, ReportReplicationState};
pub use report::{
    ReportEvent, ReportFieldKind, ReportFieldValue, ReportHistoryDepth, ReportRow, ReportSchema,
    ReportSchemaField, ReportSyncComplete, ReportSyncPage, ReportSyncRequest, ReportSyncTicket,
    ReportValue,
};
#[cfg(test)]
pub(crate) use seq_ring::SeqRingWriter;
pub use seq_ring::{
    PriceRange, QtySum, SeqRingBounds, SeqRingCursor, SeqRingDrainMeta, SeqRingPriceRow,
    SeqRingQtyRow, SeqRingReadMeta, SeqRingReadView, SeqRingReader, SeqRingRow, SeqRingTimedRow,
};
pub use settings::{SettingsEvent, SettingsState};
pub use strats::{StratEvent, StrategyInfo, StratsState};
pub use trades::TradesEvent;
pub(crate) use trades::{
    iter_trades_resend_response, TradesPacketEffect, TradesPacketEffects, TradesState,
};
pub use transfer_assets::{ExchangeKind, TransferAssetsEvent, TransferAssetsState};
