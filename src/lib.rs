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
//!     MoonClient,
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
//!     subscribe_trades: Some(false),
//!     subscribe_orderbooks: vec!["BTCUSDT".to_string()],
//!     ..Default::default()
//! };
//! let client = MoonClient::connect(cfg, ConnectConfig::new(init))?;
//!
//! client.subscribe_orderbook("ETHUSDT")?;
//! // After an order appears in events/snapshots:
//! // client.orders().move_order(order_uid, 50100.0)?;
//! for event in client.drain_events() {
//!     println!("event: {event:?}");
//! }
//! client.stop()?;
//! ```
//!
//! For common one-shot Engine API operations, use typed helpers such as
//! [`Client::request_balance`], [`Client::request_hedge_mode`],
//! [`Client::request_api_expiration_time`],
//! [`Client::request_transfer_assets`],
//! [`Client::request_coin_card_candles`], and
//! [`Client::request_candles_data`]. They keep the client loop running, check
//! the server status or channel completion, and parse or merge the response
//! payload. Events observed during the wait are queued in
//! [`EventDispatcher::queued_events`] and can be drained with
//! [`EventDispatcher::take_queued_events`].
//!
//! Regular applications should start with [`MoonClient`]. Lower-level
//! [`Client`] and [`EventDispatcher`](crate::events::EventDispatcher) APIs remain
//! available for tests, protocol tools, and custom runtimes.
//!
//! Lower-level `Client::api_*` calls return receivers for custom async flows.
//! In a single-threaded caller, wait for those receivers through
//! [`Client::run_until_response`], not direct `rx.recv_timeout(...)`; otherwise
//! the client loop is stopped while the caller waits.
//!
//! ## Transport Modes
//!
//! [`ClientConfig::new`] selects V0/base transport (`mask_ver = 0`). V0 does not
//! require the optional `moonext` binary. Extended transport modes V1/V2
//! (`mask_ver = 1` or `2`) require `moonext`; UI code should call
//! [`extended_transport_available`] before offering those modes. The public
//! builder falls back to V0 when `moonext` is absent.
//!
//! Working examples: `examples/client_test.rs`, `examples/trading_flow.rs`,
//! `examples/history_bars.rs`, `examples/list_markets.rs`, `examples/get_balance.rs`,
//! `examples/query_hedge_mode.rs`,
//! `examples/api_expiration_time.rs`,
//! `examples/request_client_settings.rs`,
//! `examples/order_snapshot.rs`,
//! `examples/cancel_open_order.rs`,
//! `examples/balance_snapshot.rs`,
//! `examples/trades_stream.rs`,
//! `examples/order_book_stream.rs`,
//! `examples/market_refresh.rs`,
//! `examples/multi_client_test.rs`,
//! `examples/stress_client.rs`.
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
pub mod transport;

pub use client::{
    connect_and_init, run_init_sequence, Client, ClientConfig, ConnectConfig, ConnectError,
    EngineRequestError, EventFn, EventWithStateFn, InitConfig, InitError, InitResult,
    InitialStrategies, LifecycleEvent, MoonClient, MoonClientError, MoonOrders,
    ProtocolMetricsSnapshot, RefreshConfig, SendPriority, TradeContextError, UniqueKey,
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
pub use transport::{ext_available as extended_transport_available, MoonKey, ServerMsgHeader};
