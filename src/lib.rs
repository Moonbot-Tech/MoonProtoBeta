//! # moonproto
//!
//! Rust клиент UDP-протокола MoonProto для серверов MoonBot (Delphi на VPS).
//! Wire-format, криптография (AES-128-GCM + HMAC-CRC32C MAC), handshake, retry,
//! slicing, ACK'и, PMTU discovery, и payload commands — byte-exact с Delphi
//! референсом.
//!
//! `moonproto` — **active session manager**: subscription replay, recovery
//! при reconnect, markets-index resync, orderbook full requests, trades gap
//! recovery, pending API routing, NTP sync, candle chunk merging — всё внутри
//! либы. Приложение решает только что подписать и какие команды отправить.
//!
//! ## Quick Start
//!
//! ```ignore
//! use std::time::Duration;
//! use moonproto::{
//!     import_key, run_init_sequence, Client, ClientConfig, Event, EventDispatcher,
//!     InitConfig, LifecycleEvent,
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
//! // Phase 1: handshake до Connected{fresh:true}.
//! client.run_with_dispatcher(Duration::from_secs(5), &mut dispatcher, Box::new(|_| {}));
//! if !client.is_authorized() {
//!     return Err("authorization timeout".into());
//! }
//!
//! // Phase 2: init sequence (helper сам прокачивает main loop через dispatcher).
//! let init = InitConfig {
//!     base_check: true,
//!     auth_check: true,
//!     fetch_markets: true,
//!     fetch_balance: true,
//!     subscribe_trades: Some(false),
//!     subscribe_orderbooks: vec!["BTCUSDT".to_string()],
//!     ..Default::default()
//! };
//! run_init_sequence(&mut client, &mut dispatcher, init)?;
//!
//! // Phase 3: long-running stream — типизированные events автоматически.
//! client.run_with_dispatcher(Duration::from_secs(3600), &mut dispatcher, Box::new(|event| {
//!     match event {
//!         Event::Order(OrderEvent::Created(uid)) => println!("new order {uid}"),
//!         Event::OrderBook(OrderBookEvent::Apply { market_index, .. }) => {
//!             // redraw orderbook
//!             let _ = market_index;
//!         }
//!         Event::Trade(TradesEvent::Apply(pkt)) => {
//!             // process trades packet (pkt.sections)
//!             let _ = pkt;
//!         }
//!         Event::EngineResponse(resp) if !resp.success => {
//!             eprintln!("engine error: {}", resp.error_msg);
//!         }
//!         _ => {}
//!     }
//! }));
//! ```
//!
//! Для ожидания Engine API response в том же thread'е используй
//! [`Client::run_until_response`] — не прямой `rx.recv_timeout(...)` (main loop
//! не работает во время блокирующего wait).
//!
//! Полные working examples — `examples/client_test.rs`, `examples/trading_flow.rs`,
//! `examples/history_bars.rs`, `examples/get_balance.rs`,
//! `examples/query_hedge_mode.rs`,
//! `examples/request_client_settings.rs`,
//! `examples/order_snapshot.rs`,
//! `examples/order_book_stream.rs`,
//! `examples/market_refresh.rs`,
//! `examples/multi_client_test.rs`,
//! `examples/stress_client.rs`.
//!
//! ## Главные публичные модули
//!
//! - [`client`] — [`Client`], `ClientConfig` builder, lifecycle, init sequence,
//!   high-level команды.
//! - [`events`] — [`EventDispatcher`] и типизированные [`Event`] values.
//! - [`commands`] — wire-format builders/parsers для каналов протокола.
//! - [`state`] — sync-state модели: Orders / OrderBooks / Trades / Balances /
//!   Strats / Settings / Markets.
//! - [`key_import`] — парсер base64 MoonBot exported key.
//! - [`ntp`] — SNTP клиент для self-managed NTP thread.
//! - [`api_pending`] — registry для pending Engine API responses.
//! - [`compression`] — SynLZ/DEFLATE helpers для wire-format.
//!
//! Зависит от [`moonproto_transport`] — низкоуровневый wire-layer (MAC,
//! обфускация, headers, опциональная загрузка `moonext` для extended transport
//! mode 1/2).

pub mod crypto;
pub mod protocol;
pub mod client;
pub mod compression;
pub mod commands;
pub mod state;
pub mod key_import;
pub mod ntp;
pub mod api_pending;
pub mod events;

pub use moonproto_transport::{MoonKey, ServerMsgHeader};
pub use client::{
    run_init_sequence, Client, ClientConfig, EventFn, EventWithStateFn, InitConfig,
    InitError, InitResult, LifecycleEvent, RefreshConfig,
};
pub use events::{Event, EventDispatcher};
pub use key_import::{import_key, ImportedKeys};
