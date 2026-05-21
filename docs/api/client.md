# Client

`Client` is the main handle for one MoonProto connection. It owns the UDP socket,
handshake state, retry queues, pending Engine API registry, subscriptions, NTP
thread handle, per-client server-time delta, and server identity.

Create one `Client` per server.

## Configuration

Use `ClientConfig::new` for the common base-transport setup:

```rust
let keys = moonproto::import_key(KEY_B64).expect("invalid key");
let cfg = moonproto::ClientConfig::new(
    "207.148.91.186",
    3000,
    keys.master_key,
    keys.mac_key,
);
let mut client = moonproto::Client::new(cfg);
```

`ClientConfig::new` sets:

- `mask_ver = 0`;
- random `client_id`;
- `ntp_host = Some("pool.ntp.org")`;
- `refresh = RefreshConfig::default()`.

Override only what you need:

```rust
use std::time::Duration;
use moonproto::{ClientConfig, RefreshConfig};

let cfg = ClientConfig::new(host, port, master_key, mac_key)
    .with_transport_mode(0)
    .with_client_id(12345)
    .without_ntp()
    .with_refresh(RefreshConfig {
        update_markets_every: Some(Duration::from_secs(2)),
        check_tags_every: Some(Duration::from_secs(60)),
    });
```

Struct literals are still supported for full control.

## Running

The recommended run path is `run_with_dispatcher`:

```rust
use std::time::Duration;
use moonproto::{Event, EventDispatcher};

let mut dispatcher = EventDispatcher::new();

client.run_with_dispatcher(Duration::from_secs(3600), &mut dispatcher, Box::new(|event| {
    match event {
        Event::Order(order_event) => println!("order: {order_event:?}"),
        Event::OrderBook(book_event) => println!("book: {book_event:?}"),
        Event::Trade(trade_event) => println!("trade event: {trade_event:?}"),
        _ => {}
    }
}));
```

This path performs active-library work: state dispatch, per-client
`ServerTimeDelta` linking, orderbook full requests, trades gap ticks, market-index
gating, subscription replay, and Engine API pending routing.

If the callback needs to read the just-updated dispatcher state, use
`run_with_dispatcher_state`:

```rust
client.run_with_dispatcher_state(Duration::from_secs(3600), &mut dispatcher, Box::new(|event, state| {
    if let Event::Order(order_event) = event {
        println!("orders now={}", state.orders().len());
        let _ = order_event;
    }
}));
```

`Client::run(duration, on_data)` is the low-level raw callback path. Use it only
for protocol tools that intentionally bypass `EventDispatcher`; otherwise you are
responsible for the recovery actions that `run_with_dispatcher` normally performs.

## Connection Setup

For the common setup path, call `connect_and_init` with the init steps your
application needs:

```rust
use std::time::Duration;
use moonproto::{connect_and_init, ConnectConfig, InitConfig};

let init = InitConfig {
    base_check: true,
    auth_check: true,
    fetch_markets: true,
    fetch_balance: true,
    subscribe_trades: Some(false),
    subscribe_orderbooks: vec!["BTCUSDT".to_string()],
    ..Default::default()
};

let result = connect_and_init(
    &mut client,
    &mut dispatcher,
    ConnectConfig::new(init).with_connect_timeout(Duration::from_secs(15)),
)?;
println!("orderbooks subscribed: {}", result.orderbooks_subscribed);
```

The helper keeps the client loop running while it waits for the connection and
for each Engine API response. It also fills `client.server_info()` after
`BaseCheck`.

Use `run_with_dispatcher` plus `run_init_sequence` directly when an application
needs custom progress UI between connection readiness and the init requests.

## Engine API Requests

For common one-shot reads, prefer typed request helpers:

```rust
let qty = client.request_balance(&mut dispatcher, "USDT", Duration::from_secs(10))?;
let hedge_mode = client.request_hedge_mode(&mut dispatcher, Duration::from_secs(10))?;
```

These helpers send the request, keep the UDP loop running through
`EventDispatcher`, validate the server response, and parse the payload. They
return `EngineRequestError` for timeout, disconnect, server error, or malformed
payload.

If packets from active subscriptions arrive while a one-shot helper is waiting,
the helper still applies them to `EventDispatcher` and stores the produced
notifications in `dispatcher.queued_events()`. Drain them after the helper when
the UI needs every event:

```rust
let qty = client.request_balance(&mut dispatcher, "USDT", Duration::from_secs(10))?;
for event in dispatcher.take_queued_events() {
    handle_event(event);
}
```

Lower-level `Client::api_*` methods still return
`std::sync::mpsc::Receiver<EngineResponse>` for custom async flows:

```rust
let rx = client.api_get_markets_list();
let resp = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(10))?;
```

Do not call `rx.recv_timeout(...)` on the same thread that owns the `Client`.
The response is delivered only while the client loop is running. Direct
`recv_timeout` is correct only when another thread is already running the
client.
`run_until_response` uses the same queued-event behavior as typed one-shot
helpers.

Use `request_engine_response` when a custom Engine API payload needs
caller-scoped timeout cleanup. Raw `api_*` receivers keep their pending slot
until the response arrives, a reconnect clears the session, or the same UID is
registered again.

## UI Settings Request

The UI settings channel is not an Engine API request, so it has no pending
`Receiver`. Use `request_client_settings` for the common one-shot flow:

```rust
let settings = client.request_client_settings(
    &mut dispatcher,
    Duration::from_secs(10),
)?;
println!("xSell={}", settings.x_sell);
```

## Order Snapshot Request

Use `request_order_snapshot` when the application needs the current active
orders as a one-shot operation:

```rust
let orders = client.request_order_snapshot(
    &mut dispatcher,
    Duration::from_secs(10),
)?;
println!("active orders={}", orders.len());
```

The helper sends `TAllStatusesReq`, keeps the UDP loop running, applies the
snapshot into `EventDispatcher::orders()`, and waits until the dispatcher has
finished Delphi `CleanupMissingWorkers` follow-up requests for orders absent
from the fresh snapshot.

## Balance Snapshot Request

Use `request_balance_snapshot` when the application needs a fresh full balance
read model from the Balance channel:

```rust
let balances = client.request_balance_snapshot(
    &mut dispatcher,
    Duration::from_secs(15),
)?;
println!("balance markets={}", balances.len());
println!("btc total={}", balances.global.btc_balance_total);
```

The helper sends `TRequestBalanceRefresh`, keeps the UDP loop running, waits for
the next `TBalanceSnapshotFull`, and returns a cloned `BalancesState`.

## Subscriptions

Use registry-aware methods:

```rust
client.subscribe_all_trades(false);
client.subscribe_orderbook("BTCUSDT");
client.unsubscribe_orderbook("BTCUSDT");
client.unsubscribe_all_trades();
```

The registry is replayed automatically after a hard reconnect. Do not repeat
subscriptions from `LifecycleEvent::ServerRestart` or `Connected { fresh: false }`.
Orderbook subscriptions are per market name; incoming events carry `book_kind`
so the application can render futures and spot books separately.

For UI threads, clone a `ClientSender`:

```rust
let sender = client.sender();
std::thread::spawn(move || {
    sender.subscribe_orderbook("ETHUSDT");
});
```

Fire-and-forget methods log and drop on a full internal channel. Use `try_*`
methods when the UI needs explicit retry:

```rust
match client.sender().try_subscribe_orderbook("BTCUSDT") {
    Ok(()) => {}
    Err(err) => eprintln!("subscribe failed: {err:?}"),
}
```

## Periodic Refresh

`ClientConfig.refresh` controls automatic background Engine API requests:

```rust
RefreshConfig {
    update_markets_every: Some(Duration::from_secs(2)),
    check_tags_every: Some(Duration::from_secs(60)),
}
```

`update_markets_every` is enabled by default to keep prices and funding fresh in
long sessions. The default 2 second cadence follows the Delphi full-proxy client
market-details worker. `check_tags_every` is also enabled by default at 60 seconds
to mirror the Delphi heavy API worker. After an hourly boundary the client also
sends the Delphi-compatible four-request `CheckBinanceTags` burst with 200 ms
spacing. Set `check_tags_every` to `None` to disable token-tag refresh entirely.

## Observability

```rust
client.is_authorized();
client.auth_status();
client.ping_count();
client.total_sent();
client.total_recv();
client.bytes_per_sec_sent();
client.bytes_per_sec_recv();
client.round_trip_delay_ms();
client.actual_pmtu();
client.rs();
client.server_time_delta_days();
client.server_token();
client.peer_app_token();
client.server_info();
```

`server_info()` is populated by `connect_and_init` or `run_init_sequence` when
`base_check` is enabled.
For multi-server details, see [`multi_server.md`](multi_server.md).

## Shutdown

```rust
client.disconnect();
```

`disconnect` schedules `LogOff`, closes the socket path, and exits the current
run loop. To reconnect after final shutdown, create a new `Client`.
