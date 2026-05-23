# moonproto API Overview

`moonproto` is the public Rust client library for MoonProto servers. It is not a
passive packet parser: it owns the session lifecycle and performs the recovery
work that every application would otherwise have to duplicate.

## Main Shape

```
Application
  decides: what to subscribe to, what commands to send, how to render state
        |
        v
moonproto
  Client              UDP, handshake, retry, reconnect, NTP, pending API
  EventDispatcher     typed events + read-only state models
  connect_and_init    ready connection + initial requests in one call
  run_init_sequence   BaseCheck/AuthCheck/markets/indexes/balances/post-init sync
  commands::*         byte-level parsers/builders
  state::*            orders, orderbooks, trades, balances, strategies, markets
        |
        v
MoonBot server
```

Use one `Client` plus one `EventDispatcher` per server connection.

## Recommended Lifecycle

```rust
use std::time::Duration;
use moonproto::{
    connect_and_init, import_key, Client, ClientConfig, ConnectConfig,
    EventDispatcher, InitConfig,
};

let keys = import_key(KEY_B64).expect("invalid key");
let cfg = ClientConfig::new("127.0.0.1", 3000, keys.master_key, keys.mac_key);

let mut client = Client::new(cfg);
let mut dispatcher = EventDispatcher::new();

let init = InitConfig {
    subscribe_trades: Some(false),
    subscribe_orderbooks: vec!["BTCUSDT".to_string()],
    ..Default::default()
};
connect_and_init(
    &mut client,
    &mut dispatcher,
    ConnectConfig::new(init).with_connect_timeout(Duration::from_secs(15)),
)?;

// Domain state is opened only after init succeeds. Initial server pushes that
// arrive earlier are dropped; the helper then requests fresh orders, settings,
// balance, and strategy state.
// All-trades is optional in the Rust public API; subscribe explicitly if the
// application expects trades-stream events.
// Init is one-time for this Client session; reconnect restore is automatic.

client.run_with_dispatcher(Duration::from_secs(3600), &mut dispatcher, Box::new(|event| {
    let _ = event;
}));
```

## What the Library Does Automatically

- Reconnects and re-handshakes.
- Fetches markets, market indexes, prices, and balances during the mandatory
  one-time init.
- After init, restores market indexes, refreshes prices, and replays registry
  subscriptions after reconnect without requiring a second Init.
- Blocks indexed streams while market indexes are stale.
- Sends orderbook full-snapshot requests when diff recovery requires them.
- Detects trades gaps and sends `TradesResend` requests from the
  Delphi-equivalent tail check after valid trades packets.
- Routes Engine API responses into one-shot `request_*` helpers or the
  `Receiver` returned by lower-level `api_*` calls.
- Provides typed helpers for common Engine API reads including balances,
  hedge mode, API-key expiration, transferable assets, and candles.
- Provides registry-aware single, batched, and all-clear helpers for orderbook
  subscriptions so reconnect restore follows the application's latest intent.
- Queues events produced during one-shot waits in `EventDispatcher` so
  notifications are not lost while the helper owns the run loop.
- Maintains per-client `ServerTimeDelta` for order timestamps.
- Runs the Delphi-style process-level NTP syncer by default with
  `ClientConfig::new` (`pool.ntp.org`). Use `with_ntp_host` to override the
  host, or `without_ntp` only for tests and tools that manage corrected time
  themselves.
- Aggregates chunked candle responses through `request_candles_data`.

## Wire Compatibility Notes

String fields sent by public helpers use the Delphi `WriteStringToStreamUtf8`
shape: UTF-8 bytes, `Word` length prefix, and exactly that declared number of
bytes in the packet body. If an input string is longer than `65535` bytes, the
wire length wraps to the low 16 bits and only that many leading bytes are sent,
matching Delphi.

String fields received from the wire use Delphi `TEncoding.UTF8.GetString`
replacement semantics: invalid UTF-8 bytes become ASCII `?`, not Unicode
replacement character `U+FFFD`.

Applications use lifecycle events for UI status and alerting, not for recovery.

## Public Entry Points

| API | Purpose |
|---|---|
| `client.md` | `Client`, config, run loop, init helper, subscriptions |
| `events.md` | `EventDispatcher`, typed events, read-only state |
| `lifecycle.md` | Connection and critical status events |
| `engine_api.md` | Engine RPC wrappers and response parsing |
| `trade_actions.md` | High-level trading commands |
| `orders.md` | Order state and order events |
| `balances.md` | Account and market balance snapshots |
| `order_books.md` | Orderbook updates and recovery |
| `trades.md` | Trades stream and gap recovery |
| `markets.md` | Markets list, prices, indexes, tags |
| `strats.md` | Strategy snapshots and updates |
| `candles.md` | Historical candles APIs |
| `multi_server.md` | Multiple independent connections |

## Low-Level Modules

`commands::*` and `state::*` remain public for custom tooling, tests, and advanced
consumers. Regular applications should prefer `Client::run_with_dispatcher`,
`connect_and_init`, typed `Client::request_*` helpers,
`Client::subscribe_*`, and the typed events.
