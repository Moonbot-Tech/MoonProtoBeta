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
    import_key, run_init_sequence, Client, ClientConfig, Event, EventDispatcher,
    InitConfig, LifecycleEvent,
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
        LifecycleEvent::SendBacklogCritical { u_key_uid, .. } => {
            eprintln!("critical send backlog for order {u_key_uid}");
        }
        LifecycleEvent::BindFailed { consecutive_failures } => {
            eprintln!("UDP bind failed {consecutive_failures} times");
        }
        _ => {}
    }));

    client.run_with_dispatcher(Duration::from_secs(5), &mut dispatcher, Box::new(|_| {}));
    if !client.is_authorized() {
        return Err("authorization timeout".into());
    }

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
4. Run `client.run_with_dispatcher(...)` until authorization.
5. Call `run_init_sequence(&mut client, &mut dispatcher, InitConfig { ... })`.
6. Continue with `client.run_with_dispatcher(...)` for the long-running stream.

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

Most `Client::api_*` methods return `std::sync::mpsc::Receiver<EngineResponse>`.
If the same thread owns the `Client`, wait through `run_until_response` so UDP is
still pumped while the response is pending:

```rust
let rx = client.api_get_markets_list();
let resp = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(10))?;
```

Calling `rx.recv_timeout(...)` directly is only correct when another thread is
already running the client loop.

Typed parsers are provided for common response payloads:

```rust
use moonproto::commands::{parse_get_balance_response, parse_query_hedge_mode_response};

let rx = client.api_get_balance("USDT");
let resp = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(10))?;
let qty = parse_get_balance_response(&resp.data).expect("bad GetBalance payload");

let rx = client.api_query_hedge_mode();
let resp = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(10))?;
let hedge_mode = parse_query_hedge_mode_response(&resp.data).expect("bad hedge payload");
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

After `run_init_sequence`, `client.server_info()` contains the optional server
identity returned by `BaseCheck`: bot id, server name, exchange name, base
currency, and version fields.

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
- `examples/get_balance.rs` — request and parse one currency balance.
- `examples/query_hedge_mode.rs` — request and parse account hedge mode.
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
