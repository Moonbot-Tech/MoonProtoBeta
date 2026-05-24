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
4. Before init, call `dispatcher.set_local_strategies(&strategies)` if the
   application has local strategies. An empty list is valid.
5. Call `connect_and_init(&mut client, &mut dispatcher, ConnectConfig { ... })`.
6. Continue with `client.run_with_dispatcher(...)` for the long-running stream.

Before init completes, all client run modes drop domain snapshots and streams
that the server may push immediately after transport auth. After a successful
init the helper sends the same refresh set as the Delphi client: order snapshot
request, strategy snapshot reply from the dispatcher-owned strategy list,
settings request, MM-orders subscription state, and balance refresh request. If
the application did not provide strategies, the reply is a valid empty strategy
snapshot; later server snapshots fill the same read model.

Use the lower-level `run_with_dispatcher` plus `run_init_sequence` pair only
when the UI needs to show custom progress between the transport connection and
the init requests. Init is a one-time step for a `Client` session; after it
completes, reconnect restore is owned by the library.

`run_with_dispatcher` is the main high-level entry point. It dispatches incoming
payloads into typed events and performs library-owned transport/read-model work:

- running mandatory init through `connect_and_init` / `run_init_sequence`:
  BaseCheck, AuthCheck, markets, indexes, market prices, balance refresh, orders,
  strategies, and settings;
- restoring market indexes, refreshing prices, and replaying saved subscriptions
  after reconnect without a second Init;
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
let api_expiration = client.request_api_expiration_time(&mut dispatcher, Duration::from_secs(10))?;
let transfer_assets = client.request_transfer_assets(&mut dispatcher, 0, Duration::from_secs(10))?;
let candles = client.request_candles_data(&mut dispatcher, Duration::from_secs(30))?;
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

Events produced while a one-shot helper is waiting are stored in
`EventDispatcher::queued_events()`. Drain them after the helper if the
application has active subscriptions and needs every notification:

```rust
let qty = client.request_balance(&mut dispatcher, "USDT", Duration::from_secs(10))?;
for event in dispatcher.take_queued_events() {
    handle_event(event);
}
```

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

## Trade Actions

For market-level trade commands, build `TradeCtx` from the connected session so
the wire header uses the server's exchange and base-currency ordinals:

```rust
let ctx = client.random_trade_ctx()?;
client.new_order(ctx, "BTCUSDT", false, 50_000.0, 0, 0.001);
```

`random_trade_ctx` returns `TradeContextError` until `BaseCheck` has filled
`client.server_info()`. `connect_and_init` always does this during the mandatory
init sequence. For actions on existing orders, use tracked-order wrappers such as
`cancel_tracked_order` and `replace_tracked_order`.

## Subscriptions

Use the registry-aware subscription API:

```rust
client.subscribe_all_trades(false);
client.subscribe_orderbook("BTCUSDT");
```

The library remembers these subscription intents. Before Init, reconnect does
not emit subscription traffic; after the single Init completes, reconnect inside
the same `Client` session replays the registry automatically. From another
thread, clone `client.sender()` and call typed subscription, trade, UI,
strategy, or balance fire-and-forget methods on `ClientSender`.
All-trades is opt-in: without `InitConfig::subscribe_trades` or
`subscribe_all_trades`, incoming trades-stream packets are unexpected and are
dropped instead of being emitted as events.

## Multi-Server

Create one `Client` and one `EventDispatcher` per server. State, sockets,
pending API responses, subscriptions, and server-time delta are per client.

After `connect_and_init`, `client.server_info()` contains the optional server
identity returned by `BaseCheck`: bot id, server
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
- `examples/request_candles_data.rs` — request and merge the full chunked candles stream.
- `examples/list_markets.rs` — fetch the market catalog and print a summary.
- `examples/get_balance.rs` — request and parse one currency balance.
- `examples/query_hedge_mode.rs` — request and parse account hedge mode.
- `examples/api_expiration_time.rs` — request and parse API-key expiration time.
- `examples/request_client_settings.rs` — request the current UI settings snapshot.
- `examples/order_snapshot.rs` — request the current order snapshot.
- `examples/cancel_open_order.rs` — request open orders and optionally cancel one tracked order.
- `examples/balance_snapshot.rs` — request the current full balance snapshot.
- `examples/trades_stream.rs` — subscribe to the trades stream and resolve market names.
- `examples/order_book_stream.rs` — subscribe to one orderbook stream.
- `examples/order_book_top.rs` — subscribe to one orderbook and print best bid/ask from the applied read model.
- `examples/market_refresh.rs` — opt in to background market refresh and observe events.
- `examples/multi_client_test.rs` — two independent clients in one process.
- `examples/stress_client.rs` — two-client live stress with optional packet-loss emulation (`post_init` by default, `pre_connect` for handshake loss).

## FireTest

`tests/fire_test.rs` is the live health test for the active library. It is
ignored by normal `cargo test` because it needs a real MoonBot server and
mutates test-server settings/strategy state.

Put the local config outside this crate repo, one directory above `moonproto/`:

```text
moonproto.firetest.conf
server = 127.0.0.1:3000
key = <exported MoonBot key>
allow_mutation = true
market = BTCUSDT
strategy_field = Comment
# strategy_id = 123456789
# candles_timeout_secs = 30
# high_loss_timeout_secs = 60
```

Run:

```powershell
cargo test --test fire_test -- --ignored --nocapture
```

The test starts two clients, runs one Init per client, verifies settings,
strategies, trades, and the configured orderbook. It enables client-side
`err_emu=10%` before connect, requests the full chunked candles snapshot and
fails unless all chunks are merged and parsed. Then it raises client-side
`err_emu=50%` for a high-loss simple-operations gate: small Engine API
requests, settings, balances, order snapshots, live trades/orderbook delivery,
a forced reconnect, and stream delivery after reconnect. Heavy candles are
intentionally not requested at 50%.

When packet loss is enabled, FireTest prints measured protocol math for
incoming responses. For `MPC_Sliced` API/UI datagrams the log includes attempts,
delivered/dropped packets, missing blocks, and completed API method/UID/success
when reassembly succeeded. A timeout is not treated as random noise until the
log shows whether the server did not send, ErrEmu dropped all needed retries,
or the client received bytes but failed to apply them.

The high-loss gate is not a flaky-random smoke test. Delphi halves the loss
rate for service/handshake packets, so `err_emu=50%` means service packets drop
at 25% and deliver at 75%. Ten reconnect attempts therefore fail with
`0.25^10 = 0.000095%` for client-side loss, or about `0.0257%` even if both
client and server apply the same 50% emulator. A repeated failure here is a
protocol/reconnect bug until proven otherwise.

After the high-loss gate it disables the stochastic emulator, checks
settings/strategy broadcast from client A to client B, uses a hidden debug
send-blackhole hook to force reconnect, and verifies that trades and orderbook
continue afterwards.
The `--nocapture` log is intentionally diagnostic: lifecycle events, server
messages, EngineResponse/candle chunks, parse failures, and compact stream
summaries are printed without dumping keys or full payloads.

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
- `balances.md`
- `trade_actions.md`
- `ui.md`
- `strats.md`
- `arb.md`
- `candles.md`
- `multi_server.md`

## Build

```bash
cargo build --release
cargo test
```
