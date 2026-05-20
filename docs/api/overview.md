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
  run_init_sequence   BaseCheck/AuthCheck/markets/balances/subscriptions
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
    import_key, run_init_sequence, Client, ClientConfig, EventDispatcher, InitConfig,
};

let keys = import_key(KEY_B64).expect("invalid key");
let cfg = ClientConfig::new("127.0.0.1", 3000, keys.master_key, keys.mac_key);

let mut client = Client::new(cfg);
let mut dispatcher = EventDispatcher::new();

client.run_with_dispatcher(Duration::from_secs(5), &mut dispatcher, Box::new(|_| {}));
assert!(client.is_authorized());

let init = InitConfig {
    base_check: true,
    auth_check: true,
    fetch_markets: true,
    fetch_balance: true,
    subscribe_trades: Some(false),
    subscribe_orderbooks: vec!["BTCUSDT".to_string()],
    ..Default::default()
};
run_init_sequence(&mut client, &mut dispatcher, init)?;

client.run_with_dispatcher(Duration::from_secs(3600), &mut dispatcher, Box::new(|event| {
    let _ = event;
}));
```

## What the Library Does Automatically

- Reconnects and re-handshakes.
- Replays registered trade/orderbook subscriptions after hard reconnect.
- Refetches market indexes after server restart and blocks indexed streams until they are synchronized.
- Sends orderbook full-snapshot requests when diff recovery requires them.
- Detects trades gaps and sends `TradesResend` requests on periodic ticks.
- Routes Engine API responses into the `Receiver` returned by the matching `api_*` call.
- Maintains per-client `ServerTimeDelta` for order timestamps.
- Runs the optional NTP sync thread when `ClientConfig::ntp_host` is set.
- Aggregates chunked candle responses through `api_request_candles_data_async`.

Applications use lifecycle events for UI status and alerting, not for recovery.

## Public Entry Points

| API | Purpose |
|---|---|
| [`client.md`](client.md) | `Client`, config, run loop, init helper, subscriptions |
| [`events.md`](events.md) | `EventDispatcher`, typed events, read-only state |
| [`lifecycle.md`](lifecycle.md) | Connection and critical status events |
| [`engine_api.md`](engine_api.md) | Engine RPC wrappers and response parsing |
| [`trade_actions.md`](trade_actions.md) | High-level trading commands |
| [`orders.md`](orders.md) | Order state and order events |
| [`order_books.md`](order_books.md) | Orderbook updates and recovery |
| [`trades.md`](trades.md) | Trades stream and gap recovery |
| [`markets.md`](markets.md) | Markets list, prices, indexes, tags |
| [`strats.md`](strats.md) | Strategy snapshots and updates |
| [`candles.md`](candles.md) | Historical candles APIs |
| [`multi_server.md`](multi_server.md) | Multiple independent connections |

## Low-Level Modules

`commands::*` and `state::*` remain public for custom tooling, tests, and advanced
consumers. Regular applications should prefer `Client::run_with_dispatcher`,
`run_init_sequence`, `Client::api_*`, `Client::subscribe_*`, and the typed events.
