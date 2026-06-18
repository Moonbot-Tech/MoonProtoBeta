//! Protocol data-model types for MoonProto command channels.
//!
//! Regular applications should use `MoonClient` intents, typed events, and
//! read-only snapshots. This module re-exports the data-model records, enums,
//! and command structs that appear in public signatures, snapshots, and events;
//! the byte-level builders and parsers themselves are crate-internal.
//!
//! These types preserve the production MoonProto wire contract: base command
//! header, command id, version, UID, per-command priority/retry semantics, and
//! exact field order. See `docs/` for public Active Lib/API guides.

#![cfg_attr(feature = "diagnostics", allow(unreachable_pub))]

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

#[cfg(feature = "diagnostics")]
#[doc(hidden)]
pub use candles::{parse_request_candles_data_response, CandlesAggregator, RequestCandlesMarket};
#[cfg(feature = "diagnostics")]
#[doc(hidden)]
pub use engine_api::EngineResponse;
#[cfg(feature = "diagnostics")]
#[doc(hidden)]
pub use strategy_serializer::parse_strategy_batch;
