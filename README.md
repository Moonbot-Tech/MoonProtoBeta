<p align="center">
  <a href="https://moonbot.pro">
    <img src="assets/moonbot-logo-full.svg" alt="Moonbot" width="199">
  </a>
</p>

# MoonProto

Rust client library for the MoonProto UDP protocol used by MoonBot servers.

The crate contains the transport layer, handshake, reconnect, reliable sliced
datagrams, typed command parsers/builders, read-model state, and the active
session API. The old separate `moonproto-transport` crate is no longer needed:
transport is available as `moonproto::transport`.

Current public prototype works without `moonext` in V0/base transport mode.
Extended V1/V2 modes require the optional closed binary
`moonext.dll` / `libmoonext.so` / `libmoonext.dylib`; until that binary is
present, keep transport mode `0`. `ClientConfig::with_transport_mode(1 | 2)`
falls back to V0 when `moonext` is absent; unsupported mode values also
normalize to V0.

## Credentials

Do not commit live keys or server addresses.

For examples, pass credentials on the command line:

```powershell
cargo run --release --example trading_flow -- "<exported MoonBot key>" "HOST:PORT"
```

`<exported MoonBot key>` is the base64 key string exported by MoonBot and parsed
by `moonproto::import_key`. Current MoonBot exports can also include suggested
UDP endpoint and transport mode; UI code can read those suggestions with
`moonproto::parse_key_info`, then still let the user edit host, port, and mode
before connecting.

For live tests, put the config outside this crate repo. By default FireTest reads
`../moonproto.firetest.conf` relative to the `moonproto/` directory. You can
override the path with `MOONPROTO_FIRETEST_CONFIG`.
Keep this file next to the checkout, not inside the public crate, so live
credentials never become part of commits or packages.

```text
server = HOST:PORT
key = <exported MoonBot key>
# mask_ver = 0
```

This config is only for live tests. A normal application can read the same
MoonBot key string from env/config/UI and pass it to `moonproto::import_key`.
The quick FireTest profile is the usual fast health gate during development.
The full FireTest is destructive/stress-oriented and requires
`allow_mutation = true`; optional FireTest knobs such as target market,
strategy field, and timeout overrides are documented in `tests/fire_test.rs`.

For the smaller live smoke test, use environment variables:

```powershell
$env:MOONPROTO_LIVE_SERVER = "HOST:PORT"
$env:MOONPROTO_KEY = "<exported MoonBot key>"
cargo test --test integration_smoke -- --ignored --nocapture
```

## Build

```powershell
cargo build
cargo build --release
cargo test --lib
cargo check --examples
```

Packaging sanity check:

```powershell
cargo package --allow-dirty
```

The package must contain only files from this crate. Root workspace secrets such
as local key files are intentionally outside `moonproto/`.

## Quick Run

The most useful manual examples:

```powershell
cargo run --release --example trading_flow -- "<key>" "HOST:PORT"
cargo run --release --example list_markets -- "<key>" "HOST:PORT" 20
cargo run --release --example order_book_top -- "<key>" "HOST:PORT" BTCUSDT 30
cargo run --release --example trades_stream -- "<key>" "HOST:PORT" all 30
cargo run --release --example history_bars -- "<key>" "HOST:PORT" BTCUSDT 1h
```

Basic application shape:

```rust
use moonproto::{
    import_key, ClientConfig, ConnectConfig, InitConfig, InitialStrategies,
    MoonClient, NewOrderParams, OrderSide, TradesStreamMode,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let key_b64 = std::env::var("MOONPROTO_KEY")?;
    let host = std::env::var("MOONPROTO_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = std::env::var("MOONPROTO_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3000);

    let keys = import_key(&key_b64).expect("invalid MoonBot key");
    let cfg = ClientConfig::new(host, port, keys.master_key, keys.mac_key)
        .with_transport_mode(0);

    let client = MoonClient::connect(
        cfg,
        ConnectConfig::new(InitConfig {
            initial_strategies: Some(InitialStrategies::new(
                0,
                Vec::new(), // replace with your local strategy list if the app has one
            )),
            subscribe_trades: Some(TradesStreamMode::TradesOnly),
            subscribe_orderbooks: vec!["BTCUSDT".to_string()],
            ..Default::default()
        }),
    )?;

    client.subscribe_orderbook("ETHUSDT")?;
    // After the user chooses a market/order side:
    // client.trade().new_order(NewOrderParams::new("BTCUSDT", OrderSide::Long, 50100.0, 0.001))?;
    // After an order appears in events/snapshots:
    // client.orders().move_order(order_uid, 50100.0)?; // also accepts &Order

    for lifecycle in client.drain_lifecycle_events() {
        println!("lifecycle: {lifecycle:?}");
    }

    if let Some(snapshot) = client.snapshot() {
        println!("orders={}", snapshot.orders().len());
    }

    client.stop()?;

    Ok(())
}
```

`MoonClient` owns the runtime thread. Init is one-time per session. After Init,
reconnect restore, market refresh, saved subscriptions, orderbook full resync,
trades gap recovery, and pending Engine API dispatch are owned by the library
until `stop()` or drop.
See `docs/active_lib.md` for the maintained-state contract.

Engine API helpers that mutate server/exchange state also run through the owned
runtime and return immediately after queuing the intent. Completion arrives as
`Event::EngineAction` / `Event::EngineResponse`. Examples:
`client.set_leverage(...)`, `client.set_hedge_mode(...)`,
`client.cancel_all_orders(...)`, `client.confirm_risk_limit(...)`, and
`client.transfer_asset(...)`.

## Tests

Deterministic tests:

```powershell
cargo test --lib
cargo test --test udp_polling
```

Live smoke:

```powershell
cargo test --test integration_smoke -- --ignored --nocapture
```

FireTest:

```powershell
$env:MOONPROTO_FIRETEST_PROFILE = "quick"
cargo test --release --test fire_test -- --ignored --nocapture
```

`tests/fire_test.rs` is the main live health test for the active library.

Quick profile target is under 30 seconds and checks one client:

- connect, AuthDone, InitDone;
- BaseCheck, AuthCheck, markets, indexes, market update;
- strategy schema receive/apply;
- trades and orderbook subscriptions;
- retained trades/LastPrice/derived history state;
- retained MarkPrice line and funding/balance/order UI state;
- ParseFailed equals zero;
- CPU summary for protocol/apply paths.

Full profile runs the destructive/stress gate:

- two live clients;
- client-side `err_emu=10%` before connect;
- full chunked candles snapshot under loss;
- settings/strategy broadcast between clients;
- `err_emu=50%` simple-operation/reconnect gate;
- forced reconnect and stream delivery after reconnect;
- detailed server-message, sliced, retry, parse, and CPU diagnostics.

FireTest writes strategy diagnostics under `target/` by default:

- `target/firetest_strategy_info_<profile>.txt`
- `target/firetest_strategy_raw/`

## Examples

Examples live in `examples/`.

Important ones:

- `trading_flow.rs`: compact `MoonClient` application flow.
- `list_markets.rs`: market catalog from `MoonClient::snapshot`.
- `market_refresh.rs`: background market refresh events/snapshots.
- `trades_stream.rs`: trades subscription and retained market tail.
- `order_book_stream.rs` / `order_book_top.rs`: orderbook stream/read model.
- `history_bars.rs`: retained candle/history read path.
- `request_candles_data.rs`: diagnostic chunked-candles protocol tool.
- `order_snapshot.rs`: fresh order snapshot through `MoonClient`.
- `cancel_open_order.rs`: tracked cancel intent through `client.orders()`.
- `multi_client_test.rs`: two independent `MoonClient` runtimes.

Diagnostic / protocol-tool examples intentionally use lower-level APIs:

- `loss_logger.rs`: live loss/gap diagnostics.
- `stress_client.rs`: two-client stress and protocol-loss diagnostics.

## API Docs

Public API notes live in `docs/`.

Start here:

- `docs/overview.md`
- `docs/client.md`
- `docs/events.md`
- `docs/lifecycle.md`
- `docs/time.md`
- `docs/markets.md`
- `docs/trades.md`
- `docs/order_books.md`
- `docs/orders.md`
- `docs/candles.md`
- `docs/engine_api.md`
- `docs/strats.md`
- `docs/multi_server.md`

## Repository Layout

```text
src/client/       active client/session, init, reconnect, send/receive paths
src/commands/     typed MoonProto command parsers/builders
src/events/       public events and dispatcher
src/state/        read-model state: markets, trades, books, orders, balances
src/transport/    built-in low-level transport and optional moonext loader
tests/            integration, polling, and FireTest
examples/         runnable live/manual examples
docs/             API documentation
```

## License

Licensed under the Apache License, Version 2.0. See `LICENSE`.

Redistributions must preserve the attribution notice from `NOTICE` according to
Apache-2.0 section 4(d).

## Development Notes

- V0/base transport must work without `moonext`.
- V1/V2 must only be offered when `moonproto::extended_transport_available()`
  returns `true`.
- Keep live credentials outside the public repo.
- Run quick FireTest at important checkpoints; run full FireTest before calling
  a protocol build stable.

---

<p align="center">
  <strong>Moonbot</strong><br>
  Advanced terminal for cryptocurrency trading<br>
  <a href="https://moonbot.pro">moonbot.pro</a>
</p>
