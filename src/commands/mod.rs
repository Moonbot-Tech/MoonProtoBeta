pub mod arb;
pub mod balance;
pub mod candles;
pub mod engine_api;
pub mod engine_request;
pub mod market;
pub mod order_book;
/// MoonProto Commands — deserialization of all command channels.
/// Byte-exact port of MoonProtoBaseStruct.pas + all *Struct.pas files.
pub mod registry;
pub mod strat;
pub mod strategy_serializer;
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
pub use trade::{
    AllStatuses, BulkReplaceNotify, CorridorUpdate, DoClosePositionCommand, DoSellOrderCommand,
    FixedPosition, ImmuneItem, JoinOrdersCommand, MoveAllBuysCommand, MoveAllSellsCommand,
    NewOrderCommand, OrderCancelCommand, OrderCompact, OrderReplaceCommand, OrderReplaceResponse,
    OrderStatus, OrderStatusUpdate, OrderStopsUpdate, OrderTracePoint, OrderType, OrderUpdateData,
    OrderWorkerStatus, PriceZone, ReplaceMultiKind, SetImmuneCommand, SplitOrderCommand,
    StopSettings, TradeCommand, TradeCtx, TurnPanicSellCommand, VStopUpdate,
};
pub use trades_stream::{Trade, TradeSection, TradesPacket};
