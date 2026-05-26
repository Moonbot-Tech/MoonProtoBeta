# moonproto

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
In the original private workspace this means the config lives in the workspace
root next to the `moonproto/` directory, not inside the public crate.

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
cargo run --release --example request_candles_data -- "<key>" "HOST:PORT" 90 0
```

Basic application shape:

```rust
use moonproto::{
    connect_and_init, import_key, Client, ClientConfig, ConnectConfig,
    EventDispatcher, InitConfig,
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

    let mut client = Client::new(cfg);
    let mut dispatcher = EventDispatcher::new();

    connect_and_init(
        &mut client,
        &mut dispatcher,
        ConnectConfig::new(InitConfig {
            subscribe_trades: Some(false),
            subscribe_orderbooks: vec!["BTCUSDT".to_string()],
            ..Default::default()
        }),
    )?;

    Ok(())
}
```

Init is one-time per `Client` session. After Init, reconnect restore, market
refresh, saved subscriptions, orderbook full resync, trades gap recovery, and
pending Engine API dispatch are owned by the library.

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

- `trading_flow.rs`: phased connect, Init, subscriptions, stream.
- `client_test.rs`: old low-level smoke/debug client.
- `list_markets.rs`: fetch market list.
- `market_refresh.rs`: observe background market refresh.
- `trades_stream.rs`: subscribe to trades.
- `order_book_stream.rs` / `order_book_top.rs`: orderbook stream/read model.
- `request_candles_data.rs` / `history_bars.rs`: candle/history requests.
- `order_snapshot.rs`: order snapshot helper.
- `cancel_open_order.rs`: tracked cancel helper.
- `stress_client.rs`: two-client stress and protocol-loss diagnostics.

## API Docs

Public API notes live in `docs/api/`.

Start here:

- `docs/api/overview.md`
- `docs/api/client.md`
- `docs/api/events.md`
- `docs/api/lifecycle.md`
- `docs/api/markets.md`
- `docs/api/trades.md`
- `docs/api/order_books.md`
- `docs/api/orders.md`
- `docs/api/candles.md`
- `docs/api/engine_api.md`
- `docs/api/strats.md`
- `docs/api/multi_server.md`

## Repository Layout

```text
src/client/       active client/session, init, reconnect, send/receive paths
src/commands/     typed MoonProto command parsers/builders
src/events/       public events and dispatcher
src/state/        read-model state: markets, trades, books, orders, balances
src/transport/    built-in low-level transport and optional moonext loader
tests/            integration, polling, and FireTest
examples/         runnable live/manual examples
docs/api/         API documentation
```

## Development Notes

- V0/base transport must work without `moonext`.
- V1/V2 must only be offered when `moonproto::extended_transport_available()`
  returns `true`.
- Keep live credentials outside the public repo.
- Run quick FireTest at important checkpoints; run full FireTest before calling
  a protocol build stable.
