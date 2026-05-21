//! Sync state модели — авто-применение входящих команд.
//!
//! Каждый канал MoonProto имеет соответствующий sync state:
//! - `Orders` → MPC_Order (TBaseTradeCommand sync state machine).
//! - `Strats` → MPC_Strat (TBaseStratCommand).
//! - `Balances` → MPC_Balance.
//! - `Markets` → MPC_API GetMarketsList response.
//! - `OrderBooks` → MPC_OrderBook (с reordering cache).
//! - `Trades` → MPC_TradesStream (с gap detection + resend).
//! - `Settings` → MPC_UI (TClientSettingsCommand snapshot).
//!
//! Каждый модуль документирован в `moonproto/docs/api/<channel>.md`.

pub mod balances;
pub mod epoch;
pub mod markets;
pub mod order_books;
pub mod orders;
pub mod settings;
pub mod strats;
pub mod trades;

pub use balances::{BalanceEvent, BalancesState, GlobalBalance};
pub use markets::{MarketPrice, MarketsEvent, MarketsState};
pub use order_books::{
    ApplyResult as OrderBookApplyResult, OrderBookEvent, OrderBookKind, OrderBookLevel,
    OrderBookSnapshot, OrderBooks, TopOfBook,
};
pub use orders::{ApplyResult, Order, OrderEvent, Orders, SellReason};
pub use settings::{SettingsEvent, SettingsState};
pub use strats::{StratEvent, StrategyInfo, StratsState};
pub use trades::{parse_trades_resend_response, TradesEvent, TradesState};
