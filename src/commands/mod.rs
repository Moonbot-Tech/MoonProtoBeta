//! Protocol data-model types for MoonProto command channels.
//!
//! Regular applications should use `MoonClient` intents, typed events, and
//! read-only snapshots. This module re-exports the data-model records, enums,
//! and command structs that appear in public signatures, snapshots, and events;
//! the byte-level builders and parsers themselves are crate-internal.
//!
//! These types preserve the Delphi wire formats: base command header, command
//! id, version, UID, per-command priority/retry semantics, and exact field
//! order. See `docs/` for public Active Lib/API guides.

pub(crate) mod arb;
pub(crate) mod balance;
pub(crate) mod candles;
pub(crate) mod engine_api;
pub(crate) mod engine_request;
pub(crate) mod inflate;
pub(crate) mod market;
pub(crate) mod order_book;
pub(crate) mod registry;
pub(crate) mod strat;
pub(crate) mod strategy_schema;
pub(crate) mod strategy_serializer;
pub(crate) mod strict_read;
pub(crate) mod trade;
pub(crate) mod trades_stream;
pub(crate) mod ui;

#[doc(hidden)]
pub use balance::BalanceUpdate;
// CoinCard candle data model (public) plus the low-level chunk parser/aggregator
// used by protocol diagnostics and the live FireTest harness (doc-hidden).
#[doc(hidden)]
pub use candles::{parse_request_candles_data_response, CandlesAggregator, RequestCandlesMarket};
pub use candles::{DeepHistoryKind, DeepPrice};
pub use engine_api::{
    parse_auth_check_response, parse_get_balance_response, parse_query_hedge_mode_response,
    AuthCheckResponse, DexInfo, EngineMethod, EngineResponse,
};
pub use market::{
    ArbIsolationFlags, ArbPlatformCode, BaseCurrency, ExchangeCode, PositionType, TokenTags,
};
#[doc(hidden)]
pub use order_book::{OrderBookUpdate, OrderLevel};
pub use strategy_schema::{
    StrategyDynamicPicklist, StrategyFieldLayout, StrategyFieldType, StrategyFieldUiKind,
    StrategySchema, StrategySchemaEditorSection, StrategySchemaEditorSectionKind,
    StrategySchemaField, StrategySchemaKind,
};
pub use strategy_serializer::{
    field_names, FieldValue, StrategyActiveMode, StrategyFields, StrategyKind, StrategySnapshot,
};
// Low-level strategy-batch decoder used by protocol diagnostics and the live
// FireTest harness; not part of the documented application surface.
#[doc(hidden)]
pub use strategy_serializer::parse_strategy_batch;
pub use trade::{
    AllStatuses, BulkReplaceNotify, CorridorUpdate, DelphiBool, DoClosePositionCommand,
    DoSellOrderCommand, FixedPosition, ImmuneItem, JoinOrdersCommand, MoveAllBuysCmdType,
    MoveAllBuysCommand, MoveAllCmdType, MoveAllSellsCommand, NewOrderCommand, OrderCancelCommand,
    OrderCompact, OrderReplaceCommand, OrderReplaceResponse, OrderStatus, OrderStatusUpdate,
    OrderStopsUpdate, OrderSubType, OrderTracePoint, OrderType, OrderUpdateData, OrderWorkerStatus,
    PriceZone, ReplaceMultiKind, SetImmuneCommand, SplitOrderCommand, StopSettings, TradeCommand,
    TradeCtx, TurnPanicSellCommand, VStopUpdate,
};
#[doc(hidden)]
pub use trades_stream::{parse_watcher_fills, Trade, TradeSection, TradesPacket, WatcherFill};
pub use ui::{
    ArbConfigCompact, ClientSettingsCommand, LevManage, ResetProfitKind, SpotMarketKind,
    TriggerAction, AS_CFG2_SIZE, AS_CFG_SIZE,
};
