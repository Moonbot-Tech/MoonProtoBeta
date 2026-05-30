//! # MoonProto
//!
//! Rust client for the MoonProto UDP protocol used by MoonBot servers.
//! It ports the Delphi client behavior for the wire format, AES-128-GCM
//! payload encryption, HMAC-CRC32C transport MAC, handshake, retry, slicing,
//! ACK handling, PMTU discovery, and payload commands.
//!
//! `moonproto` is an **active session manager**. Transport reconnect,
//! init-driven subscriptions and index/schema fetches, orderbook full requests,
//! trades gap recovery, pending Engine API routing, NTP sync, and candle chunk
//! merging are owned by the library. Before the first Init, transport handshake
//! readiness (`Fine`) does not send Engine API requests. After the single Init
//! for a `Client` session, reconnect restores fresh indexes for indexed streams
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
//!         Vec::new(), // replace with your local strategy list if the app has one
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
//! // client.trade().new_order(NewOrderParams::new("BTCUSDT", OrderSide::Long, 50100.0, 0.001))?;
//! // After an order appears in events/snapshots:
//! // client.orders().move_order(order_uid, 50100.0)?; // also accepts &Order
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
//! [`EventDispatcherSnapshot::account`] and emit [`Event::Account`].
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
//! read [`EventDispatcherSnapshot::transfer_assets`] after
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
//!   [`EventDispatcherSnapshot`]. The mutable [`EventDispatcher`] is for
//!   custom runtimes and diagnostics.
//! - [`commands`] — wire-format builders and parsers for protocol diagnostics
//!   and custom low-level tools.
//! - [`state`] — Active Lib read models: Orders / OrderBooks / Trades /
//!   Balances / Strats / Settings / Markets.
//! - [`key_import`] — parser for base64 MoonBot exported keys.
//! - [`ntp`] — SNTP client and Delphi-style process-level syncer.
//! - [`compression`] — SynLZ/DEFLATE helpers for wire-format payloads.
//! - [`transport`] — low-level wire layer: MAC, obfuscation, headers, and
//!   transport modes V0/V1/V2.

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
pub mod commands;
pub mod events;
pub mod key_import;
pub mod ntp;
pub mod state;
pub mod time;

// Low-level wire machinery: kept crate-internal. The high-level API
// (`client` / `events` / `state`) is the application model; the byte-level
// layers below are an implementation detail. Specific types the public API
// needs (`MoonKey`, `Command`, `TransportMode`, …) are re-exported below.
mod compression;
mod crypto;
mod protocol;
mod transport;

pub use client::{
    ActiveSubscriptions, Client, ClientConfig, ClosePositionParams, CoinCardCandlesTicket,
    ConnectConfig, ConnectError, EngineActionTicket, InitConfig, InitError, InitialStrategies,
    LifecycleEvent, MoonAccount, MoonBalances, MoonCandles, MoonClient, MoonClientError,
    MoonClientEvent, MoonClientSnapshot, MoonEventQueue, MoonEventSink, MoonOrders, MoonSettings,
    MoonStrategies, MoonStreams, MoonTrade, NewOrderParams, NewOrderTicket, OrderSide, OrderTarget,
    ProtocolMetricsSnapshot, RefreshConfig, SellOrderParams, SendPriority, SplitOrderParams,
    TradeContextError, TradesStreamMode, TradesSubscription, TransportMode, UniqueKey, VStopParams,
};
#[doc(hidden)]
pub use client::{ClientSender, SubscribeError};
pub use commands::engine_api::{
    AuthCheckResponse, DexInfo, ExchangeTypeMask, HyperDexIndex, ServerInfo,
};
pub use commands::{
    field_names, ArbConfigCompact, ArbIsolationFlags, ArbPlatformCode, BaseCurrency,
    ClientSettingsCommand, ExchangeCode, FieldValue, LevManage, OrderType, PositionType,
    ResetProfitKind, SpotMarketKind, StrategyActiveMode, StrategyFields, StrategyKind,
    StrategySnapshot, TokenTags, TriggerAction,
};
// Parameter types named by public high-level handle methods but defined in
// command submodules (`MoonTrade::move_all_sells`/`move_all_buys`,
// `MoonCandles::request_coin_card`, `Client::ui_emu_trades`).
pub use commands::candles::DeepHistoryKind;
pub use commands::trade::{MoveAllBuysParams, MoveAllSellsParams};
pub use commands::ui::EmuTradePoint;
pub use events::{
    ArbEvent, EngineActionEvent, EngineActionKind, Event, EventDispatcher, EventDispatcherSnapshot,
    MissingOrderStatusRequest, StrategySnapshotReply, WatcherFillEvent, WatcherFillsEvent,
};
pub use key_import::{
    import_key, parse_key_info, ImportedIpVersion, ImportedKeyFormat, ImportedKeyInfo,
    ImportedKeys, ImportedNetworkConfig,
};
pub use protocol::Command;
pub use state::{
    CoinCardCandlesEvent, CoinCardCandlesState, ExchangeKind, TransferAssetsEvent,
    TransferAssetsState,
};
pub use time::DelphiTime;
pub use transport::{MoonKey, ServerMsgHeader};
