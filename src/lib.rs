//! # MoonProto
//!
//! Rust client for the MoonProto UDP protocol used by MoonBot servers.
//! It ports the Delphi client behavior for the wire format, AES-128-GCM
//! payload encryption, SipHash transport MAC, handshake, retry, slicing,
//! ACK handling, PMTU discovery, and payload commands.
//!
//! MoonProto is for building **thin clients** over a MoonBot execution core:
//! the core owns all trading mechanics (orders, stops, strategies, risk); this
//! library renders the core's state and relays the user's intent to it.
//!
//! `moonproto` is an **active session manager**. Transport reconnect,
//! init-driven subscriptions and index/schema fetches, orderbook full requests,
//! trades gap recovery, pending Engine API routing, NTP sync, and candle chunk
//! merging are owned by the library. Before the first Init, transport handshake
//! readiness (`Fine`) does not send Engine API requests. After the single Init
//! for a `MoonClient` session, reconnect restores fresh indexes for indexed streams
//! and registry subscriptions automatically.
//!
//! ## Quick Start
//!
//! ```ignore
//! use moonproto::{
//!     import_key, ClientConfig, ConnectConfig, InitConfig, InitialStrategies,
//!     MoonClient, NewOrderParams, OrderSide, TradesStreamMode,
//! };
//!
//! let keys = import_key(KEY_B64).expect("invalid MoonBot key");
//! let cfg = ClientConfig::new("127.0.0.1", 3000, keys.master_key, keys.mac_key);
//!
//! let init = InitConfig {
//!     initial_strategies: Some(InitialStrategies::new(
//!         0,
//!         Vec::new(), // pass the current local strategy list if the app has one
//!     )),
//!     subscribe_trades: Some(TradesStreamMode::TradesOnly),
//!     subscribe_orderbooks: vec!["BTCUSDT".to_string()],
//!     ..Default::default()
//! };
//! let client = MoonClient::connect(cfg, ConnectConfig::new(init))?;
//! // `connect` starts the runtime and returns immediately. Wait for
//! // LifecycleEvent::Ready through the configured EventSink adapter.
//!
//! client.streams().subscribe_orderbook("ETHUSDT")?;
//! // After the user chooses a market/order side:
//! // if let Some(market) = client.snapshot().and_then(|s| s.markets().find("BTC")) {
//! //     client.trade().new_order(NewOrderParams::for_market(&market, OrderSide::Long, 50100.0, 0.001))?;
//! // }
//! // After an order appears in events/snapshots:
//! // client.orders().move_order(order, 50100.0)?; // UID selectors are for scripts/tools
//! for lifecycle in client.drain_lifecycle_events() {
//!     println!("lifecycle: {lifecycle:?}");
//! }
//! for event in client.drain_events() {
//!     println!("event: {event:?}");
//! }
//! client.disconnect()?;
//! client.wait_finished()?;
//! ```
//!
//! Account/UI refreshes are Active Lib intents:
//! [`MoonClient::account`], [`MoonClient::settings`], and
//! [`MoonClient::balances`] expose command handles that update
//! [`MoonStateSnapshot::account`] and emit [`Event::Account`].
//! Demand-driven CoinCard candles use non-blocking
//! [`MoonClient::candles`] and arrive as
//! [`Event::CoinCardCandles`]. The full 5m candles snapshot is requested
//! automatically after trades storage is enabled and arrives as
//! [`Event::CandlesSnapshot`]. Mutation helpers such as
//! [`MoonAccount::set_leverage`], [`MoonAccount::set_hedge_mode`], and
//! [`MoonAccount::cancel_all_orders`] also run inside the owned runtime. The
//! runtime keeps MoonProto pumping, checks server status or channel completion,
//! and parses or merges response payloads. Events are published through
//! [`MoonEventSink`]; the default [`MoonClient::connect`] queue adapter exposes
//! [`MoonClient::drain_events`] for immediate-mode UIs and tools.
//! Transfer UI should normally use [`MoonBalances::refresh_transfer_assets`] and
//! read [`MoonStateSnapshot::transfer_assets`] after
//! [`Event::TransferAssets`].
//!
//! Regular applications should start with [`MoonClient`]. Lower-level protocol
//! machinery is intentionally not the application model.
//!
//! ## Transport Modes
//!
//! [`ClientConfig::new`] selects [`TransportMode::V0`]. [`TransportMode::V1`]
//! and [`TransportMode::V2`] are built-in transport modes that must match the
//! server-side connection setting.
//!
//! Working examples: `examples/trading_flow.rs`, `examples/history_bars.rs`,
//! `examples/list_markets.rs`, `examples/get_balance.rs`, `examples/query_hedge_mode.rs`,
//! `examples/api_expiration_time.rs`,
//! `examples/request_client_settings.rs`,
//! `examples/order_snapshot.rs`,
//! `examples/cancel_open_order.rs`,
//! `examples/balance_snapshot.rs`,
//! `examples/trades_stream.rs`,
//! `examples/order_book_stream.rs`,
//! `examples/market_refresh.rs`,
//! and `examples/multi_client_test.rs`.
//!
//! ## Main Public Modules
//!
//! - [`client`] — [`MoonClient`], `ClientConfig` builder, lifecycle,
//!   EventSink adapters, snapshots, and high-level intents.
//! - [`events`] — typed [`Event`] values and the read-only
//!   [`MoonStateSnapshot`].
//! - [`state`] — Active Lib read models: Orders / OrderBooks / Trades /
//!   Balances / Strats / Settings / Markets.
//! - [`key_import`] — parser for base64 MoonBot exported keys.
//! - [`ntp`] — SNTP client and Delphi-style process-level syncer.
//!
//! The low-level wire layers (compression, crypto, framing, and transport modes
//! V0/V1/V2) are crate-internal implementation details; the high-level API above
//! is the application model.

// Clippy: deliberate project-wide patterns, not lints to chase.
// Protocol parsers/builders take many byte-field arguments by nature.
#![allow(clippy::too_many_arguments)]
// Explicit wire size arithmetic is kept manual: `is_multiple_of` (Rust 1.87) and
// `div_ceil` (1.81) would silently raise MSRV, and the manual form keeps the
// byte-exact intent obvious next to the protocol math.
#![allow(clippy::manual_is_multiple_of)]
#![allow(clippy::manual_div_ceil)]
// Runtime command / engine-action enums carry occasional large intent payloads;
// they are sent infrequently over the runtime channel, so boxing the large
// variant would only add indirection without a hot-path benefit.
#![allow(clippy::large_enum_variant)]
// Cleanup driver: flag every `pub` item not reachable from outside the crate —
// these should be `pub(crate)`. Shrinks the public surface to the real API.
#![warn(unreachable_pub)]

mod api_pending;
mod app_queue;
pub mod client;
#[cfg(feature = "diagnostics")]
#[doc(hidden)]
pub mod commands;
#[cfg(not(feature = "diagnostics"))]
#[allow(dead_code, unreachable_pub)]
mod commands;
pub mod events;
pub mod key_import;
#[cfg(feature = "diagnostics")]
#[doc(hidden)]
pub mod ntp;
#[cfg(not(feature = "diagnostics"))]
#[allow(dead_code, unreachable_pub)]
mod ntp;
pub mod state;
pub mod time;

// Low-level wire machinery: kept crate-internal. The high-level API
// (`client` / `events` / `state`) is the application model; the byte-level
// layers below are an implementation detail. User-facing configuration/state
// types stay re-exported below; raw channel ids are hidden for diagnostics.
mod compression;
mod crypto;
mod protocol;
mod transport;

#[cfg(feature = "diagnostics")]
#[doc(hidden)]
pub use client::ProtocolMetricsSnapshot;
pub use client::{
    ActiveSubscriptions, ClientConfig, ClosePositionParams, CoinCardCandlesTicket, ConnectConfig,
    ConnectError, EngineActionTicket, InitConfig, InitError, InitialStrategies, LifecycleEvent,
    MoonAccount, MoonBalances, MoonCandles, MoonClient, MoonClientError, MoonClientEvent,
    MoonClientSnapshot, MoonEmulator, MoonEventQueue, MoonEventSink, MoonOrders, MoonSettings,
    MoonStrategies, MoonStreams, MoonTrade, NewOrderParams, NewOrderTicket, OrderSide, OrderTarget,
    RefreshConfig, SellOrderParams, SplitOrderParams, TradeContextError, TradesStreamMode,
    TradesSubscription, TransportMode, VStopParams,
};
#[cfg(feature = "diagnostics")]
#[doc(hidden)]
pub use commands::engine_api::EngineMethod;
#[cfg(feature = "diagnostics")]
#[doc(hidden)]
pub use commands::engine_api::EngineResponse;
pub use commands::engine_api::{
    ApiExpirationTime, AuthCheckResponse, DexInfo, ExchangeTypeMask, HyperDexIndex, ServerInfo,
    TransferAsset,
};
pub use commands::market::{
    ArbIsolationFlags, ArbPlatformCode, BaseCurrency, ExchangeCode, MarketArbNowEntry,
    MarketArbPricePoint, MarketArbSlot, PositionType, TokenTags,
};
pub use commands::strategy_schema::{
    StrategyDynamicPicklist, StrategyFieldLayout, StrategyFieldType, StrategyFieldUiKind,
    StrategySchema, StrategySchemaEditorSection, StrategySchemaEditorSectionKind,
    StrategySchemaField, StrategySchemaKind,
};
pub use commands::strategy_serializer::{
    field_names, FieldValue, StrategyActiveMode, StrategyFields, StrategyKind, StrategySnapshot,
};
pub use commands::trade::{
    ExchangeOrder, FixedPosition, OrderSubType, OrderType, OrderWorkerStatus, ReplaceMultiKind,
    StopSettings,
};
pub use commands::ui::{
    ArbConfigCompact, AutoStartConfig, AutoStartConfig2, ClientSettingsCommand, JoinSellKind,
    LevManage, ResetProfitKind, SpotMarketKind, TempBlacklistEntry, TriggerAction,
};
// Parameter types named by public high-level handle methods but defined in
// command submodules (`MoonTrade::move_all_sells`/`move_all_buys`,
// `MoonCandles::request_coin_card`).
pub use commands::candles::{DeepHistoryKind, DeepPrice};
pub use commands::trade::{MoveAllBuysParams, MoveAllSellsParams};
pub use commands::ui::{EmuPencilPoint, EmuTradePoint};
pub use events::{
    ArbEvent, EngineActionEvent, EngineActionKind, Event, MoonStateSnapshot, ServerLogEvent,
    WatcherFillEvent, WatcherFillsEvent,
};
pub use key_import::{
    import_key, parse_key_info, ImportedIpVersion, ImportedKeyFormat, ImportedKeyInfo,
    ImportedKeys, ImportedNetworkConfig,
};
#[cfg(feature = "diagnostics")]
#[doc(hidden)]
pub use protocol::Command;
pub use state::{
    CoinCardCandlesEvent, CoinCardCandlesState, ExchangeKind, TransferAssetsEvent,
    TransferAssetsState,
};
#[cfg(any(test, feature = "diagnostics"))]
#[doc(hidden)]
pub use time::DelphiTime;
pub use time::MoonTime;
pub use transport::MoonKey;
