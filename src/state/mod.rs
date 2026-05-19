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

pub mod orders;
pub mod order_books;
pub mod trades;
pub mod balances;
pub mod strats;
pub mod settings;
pub mod markets;

pub use orders::{Orders, Order, OrderEvent, ApplyResult, SellReason};
pub use order_books::{OrderBooks, OrderBookEvent, ApplyResult as OrderBookApplyResult};
pub use trades::{TradesState, TradesEvent, parse_trades_resend_response};
pub use balances::{BalancesState, BalanceEvent, GlobalBalance};
pub use strats::{StratsState, StratEvent, StrategyInfo};
pub use settings::{SettingsState, SettingsEvent};
pub use markets::{MarketsState, MarketsEvent, MarketPrice};
