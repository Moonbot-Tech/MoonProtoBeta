//! Byte-level builders and parsers for MoonProto command channels.
//!
//! Regular applications should prefer the high-level `Client` wrappers and
//! typed `EventDispatcher` events. These modules are public for advanced tools,
//! tests, custom protocol integrations, and consumers that need direct access to
//! the wire payloads.
//!
//! The builders and parsers preserve the Delphi wire formats: base command
//! header, command id, version, UID, per-command priority/retry semantics, and
//! exact field order. See `moonproto/docs/commands/` and `moonproto/docs/api/`
//! for consumer-facing guides.

pub mod arb;
pub mod balance;
pub mod candles;
pub mod engine_api;
pub mod engine_request;
pub mod market;
pub mod order_book;
pub mod registry;
pub mod strat;
pub mod strategy_schema;
pub mod strategy_serializer;
pub(crate) mod strict_read;
pub mod trade;
pub mod trades_stream;
pub mod ui;

// Re-exports
pub use arb::{ArbIsolationEntry, ArbPayload, ArbPriceBlock, ArbPriceItem};
pub use balance::{BalanceItem, BalanceUpdate};
pub use engine_api::{
    parse_auth_check_response, parse_get_balance_response, parse_query_hedge_mode_response,
    AuthCheckResponse, DexInfo, EngineMethod, EngineResponse,
};
pub use order_book::{OrderBookUpdate, OrderLevel};
pub use strategy_schema::{
    StrategyDynamicPicklist, StrategyFieldLayout, StrategyFieldType, StrategyFieldUiKind,
    StrategySchema, StrategySchemaField, StrategySchemaKind,
};
pub use trade::{
    AllStatuses, BulkReplaceNotify, CorridorUpdate, DoClosePositionCommand, DoSellOrderCommand,
    FixedPosition, ImmuneItem, JoinOrdersCommand, MoveAllBuysCmdType, MoveAllBuysCommand,
    MoveAllCmdType, MoveAllSellsCommand, NewOrderCommand, OrderCancelCommand, OrderCompact,
    OrderReplaceCommand, OrderReplaceResponse, OrderStatus, OrderStatusUpdate, OrderStopsUpdate,
    OrderTracePoint, OrderType, OrderUpdateData, OrderWorkerStatus, PriceZone, ReplaceMultiKind,
    SetImmuneCommand, SplitOrderCommand, StopSettings, TradeCommand, TradeCtx,
    TurnPanicSellCommand, VStopUpdate,
};
pub use trades_stream::{parse_watcher_fills, Trade, TradeSection, TradesPacket, WatcherFill};
