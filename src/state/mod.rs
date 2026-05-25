//! Read models maintained by `EventDispatcher`.
//!
//! Each MoonProto channel has a matching state module:
//! - `Orders` for `MPC_Order` trade-command state.
//! - `Strats` for `MPC_Strat` strategy snapshots and updates.
//! - `Balances` for `MPC_Balance` account and market balances.
//! - `Markets` for Engine API market list, indexes, prices, and tags.
//! - `OrderBooks` for `MPC_OrderBook` snapshots, diffs, and reordering caches.
//! - `Trades` for `MPC_TradesStream` packets and automatic gap recovery.
//! - `Settings` for `MPC_UI` settings snapshots and UI control events.
//!
//! Normal applications read these models through `EventDispatcher` getters
//! after running `Client::run_with_dispatcher` or
//! `Client::run_with_dispatcher_state`. The per-channel guides live in
//! `moonproto/docs/api/<channel>.md`.

pub mod balances;
pub mod epoch;
pub mod history;
pub mod markets;
pub mod order_books;
pub mod orders;
pub mod seq_ring;
pub mod settings;
pub mod strats;
pub mod trades;

pub use balances::{BalanceEvent, BalancesState, GlobalBalance};
pub use history::{
    compact_trades_to_mini_candles_like_delphi, LastPricePoint, MMOrderHistoryRow, MiniCandle,
    TradeHistoryRow,
};
pub use markets::{MarketPrice, MarketTradeState, MarketsEvent, MarketsState};
pub use order_books::{
    ApplyResult as OrderBookApplyResult, OrderBookEvent, OrderBookKind, OrderBookLevel,
    OrderBookSnapshot, OrderBooks, TopOfBook,
};
pub use orders::{
    ApplyResult, Order, OrderEvent, OrderTraceChartPoint, OrderTraceLine, Orders, SellReason,
};
pub use seq_ring::{
    SeqRingBounds, SeqRingError, SeqRingReadMeta, SeqRingReader, SeqRingRow, SeqRingRowSlot,
    SeqRingTimedRow, SeqRingWriter,
};
pub use settings::{SettingsEvent, SettingsState};
pub use strats::{StratEvent, StrategyInfo, StratsState};
pub use trades::{parse_trades_resend_response, TradesEvent, TradesState};
