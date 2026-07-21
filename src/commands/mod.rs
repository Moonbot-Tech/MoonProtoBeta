//! Byte-level MoonProto command implementation.
//!
//! This module is not the application API. Regular applications use
//! `MoonClient` handles, typed events, snapshots, and the reviewed data types
//! re-exported from the crate root. Wire command structs may be replaced or
//! removed when the protocol revision changes without removing the equivalent
//! high-level user intent.
//!
//! The module becomes externally visible only with the `diagnostics` feature so
//! protocol tests can inspect exact bytes. Builders, parsers, command ids,
//! versions, retry metadata, and field order remain protocol implementation
//! details even in that build.

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
pub(crate) mod report;
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
