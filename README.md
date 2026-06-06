<p align="center">
  <a href="https://moonbot.pro">
    <img src="assets/moonbot-logo-full.svg" alt="Moonbot" width="199">
  </a>
</p>

# MoonProto

MoonProto is the client-side Rust SDK for building MoonBot-compatible terminals,
dashboards, and control tools.

A running MoonBot core remains the execution engine: it connects to exchanges,
owns orders, strategies, risk logic, balances, and trading state. This crate
implements the client runtime over the MoonProto protocol: connection,
authorization, reconnect, subscriptions, retained state, events, and typed
commands.

Your application provides the UI and product logic. MoonProto provides the live
client-side bridge to the MoonBot core:

```text
your trading UI / dashboard / control tool
        |
        v
MoonProto Rust library
        |
        v
MoonBot core with MoonProto enabled
        |
        v
exchange accounts, orders, strategies, balances
```

The crate contains the transport layer, handshake, reconnect, reliable sliced
datagrams, typed command parsers/builders, read-model state, and the owned
runtime session API. Application code usually works with:

- `MoonClient` as the connection/runtime owner;
- snapshots and events for current markets, balances, orders, trades,
  orderbooks, candles, strategies, settings, and UI/chart facts;
- typed intents such as subscribe, place/cancel/move order, refresh assets, and
  update settings.

MoonProto supports built-in transport modes V0, V1, and V2. The selected mode
must match the server-side connection setting. Unsupported mode values normalize
to V0.

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

The package contains only crate files. Keep credentials in local config files
outside the crate tree.

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
    parse_key_info, ClientConfig, ConnectConfig, InitConfig, InitialStrategies,
    MoonClient, NewOrderParams, OrderSide, TradesStreamMode, TransportMode,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let key_b64 = std::env::var("MOONPROTO_KEY")?;
    let info = parse_key_info(&key_b64).expect("invalid MoonBot key");
    let suggested_network = info.network;

    let host = std::env::var("MOONPROTO_HOST").ok().or_else(|| {
        suggested_network
            .and_then(|network| network.address.map(|ip| ip.to_string()))
    }).unwrap_or_else(|| "127.0.0.1".to_string());
    let port = std::env::var("MOONPROTO_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .or_else(|| suggested_network.map(|network| network.port))
        .unwrap_or(3000);
    let transport_mode = suggested_network
        .map(|network| network.transport_mode)
        .unwrap_or(TransportMode::V0);

    let cfg = ClientConfig::new(host, port, info.keys.master_key, info.keys.mac_key)
        .with_transport_mode(transport_mode);

    let client = MoonClient::connect(
        cfg,
        ConnectConfig::new(InitConfig {
            initial_strategies: Some(InitialStrategies::new(
                0,
                Vec::new(), // pass the current local strategy list if the app has one
            )),
            subscribe_trades: Some(TradesStreamMode::TradesOnly),
            subscribe_orderbooks: vec!["BTCUSDT".to_string()],
            ..Default::default()
        }),
    )?;

    // GUI apps do this from their normal update/event callback.
    for lifecycle in client.drain_lifecycle_events() {
        if matches!(lifecycle, moonproto::LifecycleEvent::Ready) {
            println!("MoonProto ready");
        }
    }

    client.streams().subscribe_orderbook("ETHUSDT")?;
    // After the user chooses a market/order side:
    // client.trade().new_order(NewOrderParams::new("BTCUSDT", OrderSide::Long, 50100.0, 0.001))?;
    // After an order appears in events/snapshots:
    // client.orders().move_order(&visible_order, 50100.0)?;

    for lifecycle in client.drain_lifecycle_events() {
        println!("lifecycle: {lifecycle:?}");
    }

    if let Some(snapshot) = client.snapshot() {
        println!("orders={}", snapshot.orders().len());
    }

    client.disconnect()?;
    client.wait_finished()?;

    Ok(())
}
```

`MoonClient` owns the runtime thread and `connect` returns immediately. Init is
one-time per session; readiness arrives as `LifecycleEvent::Ready`. After Init,
reconnect restore, market refresh, saved subscriptions, orderbook full resync,
trades gap recovery, and pending Engine API dispatch are owned by the library
until `disconnect()` or drop.
See `docs/active_lib.md` for the maintained-state contract.

Engine API helpers that mutate server/exchange state also run through the owned
runtime and return immediately after queuing the intent. Completion arrives as
`Event::EngineAction`, while retained state is updated through the matching
domain snapshots/events. Examples:
`client.account().set_leverage(...)`, `client.account().set_hedge_mode(...)`,
`client.account().cancel_all_orders(...)`, `client.account().confirm_risk_limit(...)`,
and `client.balances().transfer_asset(...)`.

## Tests

See `tests/README.md` for the test-layer map: public live pipeline tests,
internal protocol/state guards, and platform polling.

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
cargo test --release --features diagnostics --test fire_test -- --ignored --nocapture
```

For a baseline without client-side packet loss, set
`MOONPROTO_FIRETEST_ERR_EMU=0`; the default is 10%.

`tests/fire_test.rs` is the main live health test for the active library. It
requires the `diagnostics` feature because it deliberately enables packet-loss
emulation, protocol CPU counters, and test-only reconnect probes. Regular
applications do not need this feature.

Quick profile target is under 30 seconds and checks one client:

- connect, AuthDone, InitDone;
- BaseCheck, AuthCheck, markets/server-index map, market update;
- strategy schema receive/apply;
- trades and orderbook subscriptions;
- retained trades/LastPrice/derived history state;
- retained MarkPrice line and funding/balance/order UI state;
- ParseFailed equals zero;
- PMTU plus CPU summary for protocol/apply paths; `>5ms` in CPU-ish
  protocol/apply sections is a hard FireTest red flag.

Full profile runs the destructive/stress gate:

- two live clients;
- client-side `err_emu=10%` before connect;
- full chunked candles snapshot under loss;
- settings/strategy broadcast between clients;
- emulator-mode order lifecycle plus real non-emulator SOLUSDT cancel gate:
  place a $1000 long limit 5% below market, cancel it through the tracked
  ActiveLib order path, and separately verify live balance events/state without
  a manual balance request;
- `err_emu=50%` simple-operation/reconnect gate;
- forced reconnect and stream delivery after reconnect;
- detailed server-message, sliced, retry, parse, PMTU, and CPU diagnostics,
  including the same `>5ms` CPU hard gate.

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
- `order_snapshot.rs`: fresh order snapshot through `MoonClient`.
- `cancel_open_order.rs`: tracked cancel intent through `client.orders()`.
- `multi_client_test.rs`: two independent `MoonClient` runtimes.

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
src/events/       public events and immutable state snapshots
src/state/        read-model state: markets, trades, books, orders, balances
src/transport/    built-in low-level transport modes V0/V1/V2
tests/            integration, polling, and FireTest
examples/         runnable live/manual examples
docs/             API documentation
```

## License

Licensed under the Apache License, Version 2.0. See `LICENSE`.

Redistributions must preserve the attribution notice from `NOTICE` according to
Apache-2.0 section 4(d).

## Development Notes

- V0/V1/V2 transport modes are built in; selected client and server modes must match.
- Keep live credentials outside the public repo.
- Run quick FireTest at important checkpoints; run full FireTest before calling
  a protocol build stable.

---

<p align="center">
  <strong>Moonbot</strong><br>
  Advanced terminal for cryptocurrency trading<br>
  <a href="https://moonbot.pro">moonbot.pro</a>
</p>
