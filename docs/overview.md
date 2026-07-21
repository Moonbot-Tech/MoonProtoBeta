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
  MoonEventSink       event delivery adapter for UI frameworks/tools
  Snapshot state      read-only orders/books/trades/balances/markets view
  Protocol core       UDP, handshake, retry, slicing, pending Engine API
  Runtime state       mutable Active Lib owner inside the runtime
  Init spine          BaseCheck/AuthCheck/markets/prices/schema/post-init flush
  command handles     typed user intents: streams/orders/settings/candles/etc.
  state snapshots     orders, orderbooks, trades, balances, strategies, markets
        |
        v
MoonBot server
```

Use one `MoonClient` per server connection in regular applications.

## Public API Boundary

Application code starts from `MoonClient`, its typed handles, events, snapshots,
and documented non-diagnostic types exported from the `moonproto` crate root. The
`moonproto::commands` module is exposed only by the `diagnostics` feature for
byte-level protocol tests. Public visibility in that diagnostic build does not
make its packet structs a supported terminal API; they can disappear together
with a retired wire command. In particular, trading code uses
`client.trade()` and `client.orders()`, not similarly named command structs.

`MoonClient` already owns the background protocol/runtime thread. A terminal
does not need an extra feed thread that periodically polls `drain_events()` and
`snapshot()`. Serious UI integrations should connect events to the host
framework with `MoonEventSink::callback` or `MoonEventSink::queue_with_waker`;
the `drain + sleep` loops in examples are bounded CLI/demo loops.

## Recommended Lifecycle

```rust
use moonproto::{
    import_key, ClientConfig, ConnectConfig, InitConfig, InitialStrategies,
    MoonClient, TradesStreamMode,
};

let keys = import_key(KEY_B64).expect("invalid key");
let cfg = ClientConfig::new("127.0.0.1", 3000, keys.master_key, keys.mac_key);

let init = InitConfig {
    initial_strategies: Some(InitialStrategies::new(
        0,
        Vec::new(), // pass the current local strategy list if the app has one
    )),
    subscribe_trades: Some(TradesStreamMode::TradesOnly),
    // Use TradesAndMarketMakers for MoonBot-style MM heat-map rows with
    // HyperLiquid taker wallet addresses.
    subscribe_orderbooks: vec!["BTCUSDT".to_string()],
    ..Default::default()
};
let client = MoonClient::connect(cfg, ConnectConfig::new(init))?;

// In a GUI this is handled by the normal event/update callback.
for lifecycle in client.drain_lifecycle_events() {
    if matches!(lifecycle, moonproto::LifecycleEvent::Ready) {
        println!("ready");
    }
}

// Ordinary mutable domain state opens only after init succeeds. Startup-safe
// runtime/license/news/schema payloads may arrive earlier. The helper also
// requests fresh orders, settings, balance, and strategy state before Ready;
// those replies may arrive later.
// All-trades is optional in the Rust public API; subscribe explicitly if the
// application expects trades-stream events.
// Init is one-time for this Client session; reconnect restore is automatic.

client.streams().subscribe_orderbook("ETHUSDT")?;
// After an order appears in events/snapshots, pass the visible &Order:
// client.orders().move_order(order, 50100.0)?;

if let Some(snapshot) = client.snapshot() {
    println!("orders={}", snapshot.orders().len());
}

for event in client.drain_events() {
    println!("{event:?}");
}
```

## What the Library Does Automatically

- Reconnects and re-handshakes.
- Fetches markets, builds the initial server-index map from the market list,
  fetches prices and strategy schema, then queues order/settings/balance/local-
  strategy resync before the one-time `Ready` event.
- After init, refreshes stale market indexes only after a changed
  `PeerAppToken`, refreshes prices, and replays registry subscriptions after
  reconnect without requiring a second Init.
- Blocks indexed streams while market indexes are stale.
- Sends orderbook full-snapshot requests when diff recovery requires them.
- Detects trades gaps and sends `TradesResend` requests from the
  MoonBot tail-check contract after valid trades packets.
- Routes Engine API responses into runtime-owned pending actions and publishes
  typed events/snapshots when the requested state is ready. Applications should
  use the typed `MoonClient` handles; lower-level `api_*` receivers are
  diagnostic machinery.
- Provides typed helpers for common Engine API reads including balances,
  hedge mode, API-key expiration, transferable assets, and candles.
- Provides registry-aware single, batched, and all-clear helpers for orderbook
  subscriptions so reconnect restore follows the application's latest intent.
- Runs the active session until explicit `disconnect()` or drop; applications do not
  choose a protocol-loop duration.
- Publishes typed events and immutable snapshots for UI read models.
- Keeps chart-visible market state on the selected market/history model:
  balance/position/liquidation fields live on `Market`, arb prices live in
  `MarketHandle::arb_slot`, unprotected-position state is read through
  `snapshot.position_protection_for(&market_handle)`, signed BTC/exchange
  signal deltas live in `snapshot.markets().global_deltas()` with the optional
  local blacklist-exclusion policy from `client.settings()`, and retained
  trades/5m candles are available through
  `snapshot.market_history_readers_for(&market_handle)` when trades storage is
  enabled.
- Maintains per-client `ServerTimeDelta` for order timestamps.
- Runs the process-level NTP syncer by default with
  `ClientConfig::new` (`pool.ntp.org`). Use `with_ntp_host` to override the
  host, or `without_ntp` only for tests and tools that manage corrected time
  themselves.
- Aggregates chunked candle responses; trades subscription also schedules the
  initial 5m candles snapshot for retained history.
- Receives core-built terminal facts: detect notifications, watcher rows,
  chart-alert fires, accepted chart-alert objects, and ready chart text rows.
  Applications render these facts/state; they do not recompute the kernel-side
  detect/chart-text logic locally.

## String Compatibility Notes

String fields sent by public helpers use the MoonBot wire string shape: UTF-8
bytes, `Word` length prefix, and exactly that declared number of bytes in the
packet body. If an input string is longer than `65535` bytes, the serialized
length wraps to the low 16 bits and only that many leading bytes are sent.

String fields received from the server use MoonBot wire replacement semantics:
invalid UTF-8 bytes become ASCII `?`, not Unicode replacement character
`U+FFFD`.

Applications use lifecycle events for UI status and alerting, not for recovery.

## Time Values

MoonProto public API uses `MoonTime`, a Unix-milliseconds timestamp. The
protocol wire-time floats are converted at packet boundaries, so UI code should
read row helper methods instead of carrying raw protocol time:

```rust
let unix_ms = candle.time().unix_millis();
let system_time = trade.time().system_time();
```

## Public Entry Points

| API | Purpose |
|---|---|
| `active_lib.md` | What `MoonClient` maintains automatically |
| `client.md` | `MoonClient`, config, init, subscriptions, requests |
| `events.md` | `MoonClient` events and immutable snapshots |
| `lifecycle.md` | Connection and critical status events |
| `time.md` | `MoonTime` and timestamp helpers |
| `engine_api.md` | Engine RPC wrappers and response parsing |
| `trade_actions.md` | High-level trading commands |
| `orders.md` | Order state and order events |
| `news.md` | Retained/live news and tags JSON |
| `balances.md` | Account and market balance snapshots |
| `order_books.md` | Orderbook updates and recovery |
| `trades.md` | Trades stream and gap recovery |
| `markets.md` | Markets list, prices, indexes, tags |
| `strats.md` | Strategy snapshots and updates |
| `candles.md` | Historical candles APIs |
| `multi_server.md` | Multiple independent connections |

## Advanced Modules

Some low-level data-model modules remain available for protocol diagnostics and
internal-style tooling, but they are not the normal application model. Regular
applications should start from `MoonClient`, typed command handles, immutable
snapshots, and typed events.
