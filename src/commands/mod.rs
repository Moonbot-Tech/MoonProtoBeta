/// MoonProto Commands — deserialization of all command channels.
/// Byte-exact port of MoonProtoBaseStruct.pas + all *Struct.pas files.

pub mod registry;
pub mod trades_stream;
pub mod order_book;
pub mod balance;
pub mod engine_api;
pub mod engine_request;
pub mod trade;
pub mod strat;
pub mod arb;
pub mod ui;
pub mod market;
pub mod strategy_serializer;
pub mod candles;

// Re-exports
pub use trades_stream::{TradesPacket, Trade, TradeSection};
pub use order_book::{OrderBookUpdate, OrderLevel};
pub use balance::{BalanceUpdate, BalanceItem};
pub use arb::{ArbPayload, ArbPriceBlock, ArbPriceItem, ArbIsolationEntry};
pub use engine_api::{
    EngineResponse, EngineMethod, AuthCheckResponse, DexInfo, parse_auth_check_response,
    parse_get_balance_response,
};
pub use trade::{
    TradeCommand,
    OrderStatus, OrderStatusUpdate, OrderReplaceCommand, OrderReplaceResponse,
    OrderCancelCommand, AllStatuses, NewOrderCommand, OrderStopsUpdate,
    OrderTracePoint, CorridorUpdate, VStopUpdate, BulkReplaceNotify,
    TurnPanicSellCommand, SetImmuneCommand, ImmuneItem,
    JoinOrdersCommand, SplitOrderCommand, MoveAllSellsCommand, MoveAllBuysCommand,
    DoClosePositionCommand, DoSellOrderCommand,
    OrderType, OrderWorkerStatus, FixedPosition, ReplaceMultiKind, PriceZone,
    OrderCompact, StopSettings, OrderUpdateData,
    TradeCtx,
};
