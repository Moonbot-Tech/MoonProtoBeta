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
pub(crate) mod coin_card_candles;
pub(crate) mod epoch;
pub(crate) mod eps;
pub(crate) mod history;
pub(crate) mod history_store;
pub(crate) mod history_worker;
pub(crate) mod markets;
pub(crate) mod order_books;
pub(crate) mod orders;
pub(crate) mod seq_ring;
pub(crate) mod settings;
pub(crate) mod strats;
pub(crate) mod trades;
pub(crate) mod transfer_assets;

pub use account::{AccountEvent, AccountState};
pub use balances::{BalanceEvent, BalancesState, GlobalBalance};
pub use coin_card_candles::{CoinCardCandlesEvent, CoinCardCandlesState};
pub use history::{
    hl_address_color, Candle5mRow, CandleVolumeSnapshot, CandlesSnapshotApplySummary,
    CandlesSnapshotEvent, DerivedDeltaSnapshot, LastPricePoint, MMOrderCompanionData,
    MMOrderHistoryRow, MarkPricePoint, MarketDerivedSnapshot, MiniCandle,
    RollingTradeVolumeSnapshot, TradeHistoryRow, TradeVolumeTotals, DELPHI_MSECS_PER_DAY,
    DELPHI_SAME_TRADES_TIME_DAYS,
};
pub use history_store::{
    MarketHistoryConfig, MarketHistoryReaders, MarketHistoryRegistry, MarketHistoryStore,
    TradeStorageScope,
};
pub use history_worker::{
    MarketHistoryCandlesSnapshot, MarketHistoryHandle, MarketHistoryLastPriceBatch,
    MarketHistoryLastPriceInput, MarketHistoryMMOrderInput, MarketHistoryStreamBatch,
    MarketHistoryStreamSection, MarketHistoryStreamSectionKind, MarketHistoryTradeInput,
    MarketHistoryWorker,
};
pub use markets::{
    MarketBalancePosition, MarketHandle, MarketPrice, MarketTradeState, MarketsEvent,
    MarketsListApplyTiming, MarketsState,
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
pub use transfer_assets::{ExchangeKind, TransferAssetsEvent, TransferAssetsState};
