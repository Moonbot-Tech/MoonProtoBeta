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
//! use std::time::Duration;
//! use moonproto::{
//!     connect_and_init, import_key, Client, ClientConfig, ConnectConfig, Event,
//!     EventDispatcher, InitConfig, LifecycleEvent,
//! };
//! use moonproto::state::{OrderEvent, OrderBookEvent, TradesEvent};
//!
//! let keys = import_key(KEY_B64).expect("invalid MoonBot key");
//! let cfg = ClientConfig::new("127.0.0.1", 3000, keys.master_key, keys.mac_key);
//! let mut client = Client::new(cfg);
//! let mut dispatcher = EventDispatcher::new();
//!
//! client.on_lifecycle(Box::new(|ev| match ev {
//!     LifecycleEvent::Connected { fresh } => println!("connected fresh={fresh}"),
//!     LifecycleEvent::Reconnecting => println!("reconnecting"),
//!     LifecycleEvent::BindFailed { consecutive_failures } => {
//!         eprintln!("UDP bind failed {consecutive_failures} times");
//!     }
//!     _ => {}
//! }));
//!
//! let init = InitConfig {
//!     subscribe_trades: Some(false),
//!     subscribe_orderbooks: vec!["BTCUSDT".to_string()],
//!     ..Default::default()
//! };
//! connect_and_init(
//!     &mut client,
//!     &mut dispatcher,
//!     ConnectConfig::new(init).with_connect_timeout(Duration::from_secs(15)),
//! )?;
//!
//! // Long-running stream: typed events are produced automatically.
//! client.run_with_dispatcher(Duration::from_secs(3600), &mut dispatcher, Box::new(|event| {
//!     match event {
//!         Event::Order(OrderEvent::Created(uid)) => println!("new order {uid}"),
//!         Event::OrderBook(OrderBookEvent::Apply { market_index, .. }) => {
//!             // redraw orderbook
//!             let _ = market_index;
//!         }
//!         Event::Trade(TradesEvent::Applied { packet_num, .. }) => {
//!             // Signal only: new rows are already in market state / SeqRing.
//!             let _ = packet_num;
//!         }
//!         Event::EngineResponse(resp) if !resp.success => {
//!             eprintln!("engine error: {}", resp.error_msg);
//!         }
//!         _ => {}
//!     }
//! }));
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
//! For market-level trade commands, build [`commands::trade::TradeCtx`] from the
//! connected session with [`Client::trade_ctx`] or [`Client::random_trade_ctx`].
//! Existing-order actions should usually use tracked-order helpers such as
//! [`Client::cancel_tracked_order`] and [`Client::replace_tracked_order`].
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
    connect_and_init, run_init_sequence, Client, ClientConfig, ClientSender, ConnectConfig,
    ConnectError, EngineRequestError, EventFn, EventWithStateFn, InitConfig, InitError, InitResult,
    LifecycleEvent, ProtocolMetricsSnapshot, RefreshConfig, SendPriority, SubscribeError,
    TradeContextError, UniqueKey,
};
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
