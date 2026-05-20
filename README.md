# moonproto

Rust client library for the MoonProto UDP protocol used by MoonBot servers.

The crate implements the encrypted transport, handshake, keepalive, reconnect,
PMTU discovery, reliable sliced messages, typed command parsers/builders, state
models, and the high-level active session API.

## Install

```toml
[dependencies]
moonproto = "0.1"
```

For local development inside this workspace:

```toml
[dependencies]
moonproto = { path = "../moonproto" }
```

## Quick Start

```rust
use std::time::Duration;
use moonproto::{
    connect_and_init, import_key, Client, ClientConfig, ConnectConfig, Event,
    EventDispatcher, InitConfig, LifecycleEvent,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let keys = import_key(KEY_B64).expect("invalid MoonBot key");
    let cfg = ClientConfig::new("127.0.0.1", 3000, keys.master_key, keys.mac_key);

    let mut client = Client::new(cfg);
    let mut dispatcher = EventDispatcher::new();

    client.on_lifecycle(Box::new(|event| match event {
        LifecycleEvent::Connected { fresh } => eprintln!("connected fresh={fresh}"),
        LifecycleEvent::Reconnecting => eprintln!("reconnecting"),
        LifecycleEvent::ServerRestart => eprintln!("server restarted"),
        LifecycleEvent::BindFailed { consecutive_failures } => {
            eprintln!("UDP bind failed {consecutive_failures} times");
        }
        _ => {}
    }));

    let init = InitConfig {
        base_check: true,
        auth_check: true,
        fetch_markets: true,
        fetch_balance: true,
        subscribe_trades: Some(false),
        subscribe_orderbooks: vec!["BTCUSDT".to_string()],
        ..Default::default()
    };
    connect_and_init(
        &mut client,
        &mut dispatcher,
        ConnectConfig::new(init).with_connect_timeout(Duration::from_secs(15)),
    )?;

    client.run_with_dispatcher(Duration::from_secs(3600), &mut dispatcher, Box::new(|event| {
        match event {
            Event::Order(order_event) => println!("order: {order_event:?}"),
            Event::OrderBook(book_event) => println!("orderbook: {book_event:?}"),
            Event::Trade(trade_event) => println!("trade event: {trade_event:?}"),
            Event::EngineResponse(resp) if !resp.success => {
                eprintln!("engine error {}: {}", resp.error_code, resp.error_msg);
            }
            _ => {}
        }
    }));

    Ok(())
}
```

## Recommended Flow

1. Import keys with `import_key`.
2. Build configuration with `ClientConfig::new`.
3. Create `Client` and one `EventDispatcher` per connection.
4. Call `connect_and_init(&mut client, &mut dispatcher, ConnectConfig { ... })`.
5. Continue with `client.run_with_dispatcher(...)` for the long-running stream.

Use the lower-level `run_with_dispatcher` plus `run_init_sequence` pair only
when the UI needs to show custom progress between the transport connection and
the init requests.

`run_with_dispatcher` is the main high-level entry point. It dispatches incoming
payloads into typed events and performs library-owned recovery work:

- replaying registered subscriptions after reconnect;
- resynchronizing market indexes after server restart;
- blocking orderbook/trades packets until indexes are synchronized;
- sending `RequestOrderBookFull` when a gap requires a full snapshot;
- ticking trades gap recovery and sending resend requests;
- routing Engine API responses into pending receivers;
- applying per-client `ServerTimeDelta`;
- merging chunked candle responses.

When an event callback needs the already-updated read model, use
`run_with_dispatcher_state`:

```rust
client.run_with_dispatcher_state(duration, &mut dispatcher, Box::new(|event, state| {
    println!("orders in state: {}", state.orders().len());
    let _ = event;
}));
```

## Engine API Requests

For common one-shot Engine API calls, use the typed `request_*` helpers. They
send the request, keep the UDP loop running through `EventDispatcher`, check the
server status, and parse the response payload:

```rust
let qty = client.request_balance(&mut dispatcher, "USDT", Duration::from_secs(10))?;
let hedge_mode = client.request_hedge_mode(&mut dispatcher, Duration::from_secs(10))?;
```

Lower-level `Client::api_*` methods still return
`std::sync::mpsc::Receiver<EngineResponse>` for background-thread and custom
flows. If the same thread owns the `Client`, wait through
`run_until_response`; direct `rx.recv_timeout(...)` is only correct when another
thread is already running the client loop.

`Client::request_engine_response` owns the pending slot for its caller timeout
and removes it on timeout. The lower-level receiver path keeps the slot until a
matching response arrives, a reconnect clears the session, or the same UID is
registered again.

UI settings use the UI channel rather than Engine API pending responses:

```rust
let settings = client.request_client_settings(&mut dispatcher, Duration::from_secs(10))?;
println!("xSell={}", settings.x_sell);
```

Order snapshots use the order channel. The high-level helper sends
`TAllStatusesReq`, pumps the client loop, applies the snapshot into
`EventDispatcher::orders()`, and waits for missing-order follow-up requests:

```rust
let orders = client.request_order_snapshot(&mut dispatcher, Duration::from_secs(10))?;
println!("active orders={}", orders.len());
```

## Subscriptions

Use the registry-aware subscription API:

```rust
client.subscribe_all_trades(false);
client.subscribe_orderbook("BTCUSDT");
```

The library remembers these subscriptions and replays them automatically after a
hard reconnect. From another thread, clone `client.sender()` and call the same
typed methods on `ClientSender`.

## Multi-Server

Create one `Client` and one `EventDispatcher` per server. State, sockets,
pending API responses, subscriptions, and server-time delta are per client.

After `connect_and_init` with `base_check` enabled, `client.server_info()`
contains the optional server identity returned by `BaseCheck`: bot id, server
name, exchange name, base currency, and version fields.

See `docs/api/multi_server.md`.

## Transport Modes

Mode `0` is the open base transport and works by itself. Modes `1` and `2` use
the optional extended transport binary distributed separately for each platform.

Configure the mode with:

```rust
let cfg = ClientConfig::new(host, port, keys.master_key, keys.mac_key)
    .with_transport_mode(0);
```

## Examples

- `examples/client_test.rs` — basic live connection smoke test.
- `examples/trading_flow.rs` — phased handshake, init, subscriptions, and stream.
- `examples/history_bars.rs` — request and parse historical candles.
- `examples/list_markets.rs` — fetch the market catalog and print a summary.
- `examples/get_balance.rs` — request and parse one currency balance.
- `examples/query_hedge_mode.rs` — request and parse account hedge mode.
- `examples/request_client_settings.rs` — request the current UI settings snapshot.
- `examples/order_snapshot.rs` — request the current order snapshot.
- `examples/trades_stream.rs` — subscribe to the trades stream and resolve market names.
- `examples/order_book_stream.rs` — subscribe to one orderbook stream.
- `examples/market_refresh.rs` — observe automatic market refresh events.
- `examples/multi_client_test.rs` — two independent clients in one process.

## API Documentation

Detailed public API notes live in `docs/api/`:

- `overview.md`
- `client.md`
- `events.md`
- `lifecycle.md`
- `engine_api.md`
- `markets.md`
- `order_books.md`
- `trades.md`
- `orders.md`
- `trade_actions.md`
- `strats.md`
- `candles.md`
- `multi_server.md`

## Build

```bash
cargo build --release
cargo test
```
