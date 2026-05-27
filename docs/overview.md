# MoonProto API Overview

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
  MoonClient          runtime thread + commands/events/snapshots
  Client              low-level UDP, handshake, retry, reconnect, pending API
  EventDispatcher     typed events + read-only state models owned by runtime
  connect_and_init    low-level ready connection + init helper
  run_init_sequence   BaseCheck/AuthCheck/markets/indexes/balances/post-init sync
  commands::*         byte-level parsers/builders
  state::*            orders, orderbooks, trades, balances, strategies, markets
        |
        v
MoonBot server
```

Use one `MoonClient` per server connection in regular applications.

## Recommended Lifecycle

```rust
use moonproto::{
    import_key, ClientConfig, ConnectConfig, InitConfig, InitialStrategies,
    MoonClient,
};

let keys = import_key(KEY_B64).expect("invalid key");
let cfg = ClientConfig::new("127.0.0.1", 3000, keys.master_key, keys.mac_key);

let init = InitConfig {
    initial_strategies: Some(InitialStrategies::new(
        0,
        Vec::new(), // replace with your local strategy list if the app has one
    )),
    subscribe_trades: Some(false),
    subscribe_orderbooks: vec!["BTCUSDT".to_string()],
    ..Default::default()
};
let client = MoonClient::connect(cfg, ConnectConfig::new(init))?;

// Domain state is opened only after init succeeds. Initial server pushes that
// arrive earlier are dropped; the helper then requests fresh orders, settings,
// balance, and strategy state.
// All-trades is optional in the Rust public API; subscribe explicitly if the
// application expects trades-stream events.
// Init is one-time for this Client session; reconnect restore is automatic.

client.subscribe_orderbook("ETHUSDT")?;
// After an order appears in events/snapshots:
// client.orders().move_order(order_uid, 50100.0)?;

if let Some(snapshot) = client.snapshot() {
    println!("orders={}", snapshot.orders().len());
}

for event in client.drain_events() {
    println!("{event:?}");
}
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
- Runs the active session until explicit `stop()` or drop; applications do not
  choose a protocol-loop duration.
- Publishes typed events and immutable snapshots for UI read models.
- Keeps chart-visible market state on the selected market/history model:
  balance/position/liquidation fields live on `Market`, arb prices live in
  `Market::arb_slots`, and retained trades/5m candles are available through
  `snapshot.market_history_readers(market)` when trades storage is enabled.
- Maintains per-client `ServerTimeDelta` for order timestamps.
- Runs the Delphi-style process-level NTP syncer by default with
  `ClientConfig::new` (`pool.ntp.org`). Use `with_ntp_host` to override the
  host, or `without_ntp` only for tests and tools that manage corrected time
  themselves.
- Aggregates chunked candle responses; trades subscription also schedules the
  initial 5m candles snapshot for retained history.

## String Compatibility Notes

String fields sent by public helpers use the Delphi `WriteStringToStreamUtf8`
shape: UTF-8 bytes, `Word` length prefix, and exactly that declared number of
bytes in the packet body. If an input string is longer than `65535` bytes, the
serialized length wraps to the low 16 bits and only that many leading bytes are sent,
matching Delphi.

String fields received from the server use Delphi `TEncoding.UTF8.GetString`
replacement semantics: invalid UTF-8 bytes become ASCII `?`, not Unicode
replacement character `U+FFFD`.

Applications use lifecycle events for UI status and alerting, not for recovery.

## Public Entry Points

| API | Purpose |
|---|---|
| `client.md` | `MoonClient`, config, init, subscriptions, requests |
| `events.md` | `MoonClient` events, immutable snapshots, low-level dispatcher |
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

`commands::*`, `state::*`, `Client`, and `EventDispatcher` remain public for
custom tooling, tests, and advanced runtimes. Regular applications should prefer
`MoonClient`, typed command handles, immutable snapshots, and typed events.
