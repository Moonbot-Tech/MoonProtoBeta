//! # moonproto
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
//!
//! client.subscribe_orderbook("ETHUSDT")?;
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
//! client.stop()?;
//! ```
//!
//! For common one-shot Engine API operations, use [`MoonClient`] helpers such
//! as [`MoonClient::request_balance`], [`MoonClient::request_hedge_mode`],
//! [`MoonClient::request_api_expiration_time`],
//! [`MoonClient::request_transfer_assets`],
//! [`MoonClient::request_coin_card_candles`], and
//! [`MoonClient::refresh_candles`]. Mutation helpers such as
//! [`MoonClient::set_leverage`], [`MoonClient::set_hedge_mode`], and
//! [`MoonClient::cancel_all_orders`] also run inside the owned runtime. The
//! runtime keeps MoonProto pumping, checks the server status or channel
//! completion, and parses or merges the response payload. Events observed
//! during the wait remain available through
//! [`MoonClient::drain_events`].
//!
//! Regular applications should start with [`MoonClient`]. Lower-level
//! [`Client`] and [`EventDispatcher`] APIs remain
//! available for tests, protocol tools, and custom runtimes.
//!
//! Lower-level `Client::api_*` calls return receivers for custom async flows.
//! Custom runtimes must keep the client loop pumping while they wait for those
//! receivers; direct `rx.recv_timeout(...)` on the same thread stops protocol
//! progress.
//!
//! ## Transport Modes
//!
//! [`ClientConfig::new`] selects V0/base transport (`mask_ver = 0`). V0 does not
//! require the optional `moonext` binary. Extended transport modes V1/V2
//! (`mask_ver = 1` or `2`) require `moonext`; UI code should call
//! [`extended_transport_available`] before offering those modes. The public
//! builder falls back to V0 when `moonext` is absent.
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
//! and `examples/multi_client_test.rs`. `examples/loss_logger.rs` and
//! `examples/stress_client.rs` are diagnostic protocol tools, not normal
//! application templates.
//!
//! ## Main Public Modules
//!
//! - [`client`] — [`Client`], `ClientConfig` builder, lifecycle, init sequence,
//!   and high-level commands.
//! - [`events`] — [`EventDispatcher`] and typed [`Event`] values.
//! - [`commands`] — wire-format builders and parsers for protocol channels.
//! - [`state`] — sync-state models: Orders / OrderBooks / Trades / Balances /
//!   Strats / Settings / Markets.
//! - [`key_import`] — parser for base64 MoonBot exported keys.
//! - [`ntp`] — SNTP client and Delphi-style process-level syncer.
//! - [`compression`] — SynLZ/DEFLATE helpers for wire-format payloads.
//! - [`transport`] — low-level wire layer: MAC, obfuscation, headers, and
//!   optional `moonext` loading for extended transport modes 1/2.

mod api_pending;
mod app_queue;
pub mod client;
pub mod commands;
pub mod compression;
pub mod crypto;
pub mod events;
pub mod key_import;
pub mod ntp;
pub mod protocol;
pub mod state;
pub mod time;
pub mod transport;

pub use client::{
    connect_and_init, run_init_sequence, Client, ClientConfig, ClosePositionParams, ConnectConfig,
    ConnectError, EngineRequestError, EventFn, EventWithStateFn, InitConfig, InitError, InitResult,
    InitialStrategies, LifecycleEvent, MoonClient, MoonClientError, MoonOrders, MoonTrade,
    NewOrderParams, OrderSide, OrderTarget, ProtocolMetricsSnapshot, RefreshConfig,
    SellOrderParams, SendPriority, SplitOrderParams, TradeContextError, TradesStreamMode,
    UniqueKey,
};
#[doc(hidden)]
pub use client::{ClientSender, SubscribeError};
pub use events::{
    Event, EventDispatcher, EventDispatcherSnapshot, MissingOrderStatusRequest,
    StrategySnapshotReply, WatcherFillEvent, WatcherFillsEvent,
};
pub use key_import::{
    import_key, parse_key_info, ImportedIpVersion, ImportedKeyFormat, ImportedKeyInfo,
    ImportedKeys, ImportedNetworkConfig,
};
pub use protocol::Command;
pub use time::DelphiTime;
pub use transport::{ext_available as extended_transport_available, MoonKey, ServerMsgHeader};
