# Client

`Client` is the main handle for one MoonProto connection. It owns the UDP socket,
handshake state, retry queues, pending Engine API registry, subscriptions, a
process-level NTP guard, per-client server-time delta, and server identity.

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
- `ntp_host = Some("pool.ntp.org")` and uses one shared NTP syncer per process;
- `refresh = RefreshConfig::default()` (`UpdateMarketsList` every 2 seconds and
  `CheckBinanceTags` every 60 seconds after Init).

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

NTP follows Delphi's process-global model: all clients share one corrected time
offset and one background syncer. Use the same `ntp_host` for every client in a
process; if a different host is requested while the syncer is already running,
the existing worker is reused.

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
gating, reconnect restore, and Engine API pending routing. Before the first Init,
transport reconnects do not emit background Engine API. After Init, reconnect
inside the same `Client` session maintains the user-requested active-lib state.

User/API sends append directly to the client's unbounded Delphi-style
`DataToSend` / `DataToSendH` / `DataToSendL` queues, separate from accepted UDP
packets and app/control events. Subscription intents still use a small
app-to-main control FIFO because they mutate the reconnect registry before
emitting wire commands. The public guarantee is no local capacity cap: dense
incoming streams do not drop queued user commands or Engine API requests.

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

## Packet Loss Test Hook

`moonproto::client::set_err_emu(percent)` enables Delphi-style client-side
packet loss emulation for tests. It is process-global, affects every `Client` in
the process, and drops only incoming packets after MoonProto transport
verification succeeds. A valid packet selected for emulated drop still updates
transport side effects first (`total_recv`, online timestamp, receive counters),
matching Delphi `UDPRead`; it is then withheld from the protocol dispatcher.
Outgoing packets are still sent normally.

Service packets use half of the configured drop rate, matching Delphi
`MoonProtoErrEmu`: `Ping`, handshake/reconnect commands, MTU probes, and
`SlicedACK`.

For stress tests that target Engine API, candles, and sliced response recovery,
enable `set_err_emu` after `connect_and_init` / `run_init_sequence`. Enabling it
before connection intentionally tests handshake/reconnect loss and can prevent
the client from reaching the API phase at all.

The `stress_client` example exposes this distinction explicitly:

```text
stress_client <key_base64> [host:port] [market] [duration_secs] [err_emu_pct] [err_emu_phase]
```

`err_emu_phase=post_init` is the default and enables loss after both stress
clients finish init. Use `pre_connect` only when the test target is
authorization/reconnect behavior.

Low-level packet diagnostics are compile-gated behind the `diagnostic-trace`
feature. Build with that feature and set `MOONPROTO_TRACE_IO=1` or
`MOONPROTO_TRACE_SLICES=1` to print transport send/receive and sliced reassembly
logs.

## Connection Setup

For the common setup path, call `connect_and_init`. The Delphi init contract is
mandatory: BaseCheck, AuthCheck, markets list, market indexes, price refresh,
balance refresh, order snapshot, strategy sync, and settings sync. `InitConfig`
only adds optional stream subscriptions and timing:

```rust
use std::time::Duration;
use moonproto::{connect_and_init, ConnectConfig, InitConfig};

let init = InitConfig {
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

Init is a one-time step for a `Client` session. After it succeeds, do not call
`run_init_sequence` again just because the UDP transport reconnected; the library
maintains the user-requested active-lib state for that `Client` session.

Init always sends `GetMarketsIndexes` and records the payload size in
`InitResult::indexes_response_bytes`, because trades, orderbooks, and
`UpdateMarketsList` price rows depend on the current server `mIndex` mapping.
Periodic market refresh starts only after init opens the domain gate, so
BaseCheck/AuthCheck are not delayed by early background refresh traffic.
Critical BaseCheck/AuthCheck waits use the same default as Delphi
`TMoonProtoEngine.FTimeout`: 12 seconds per Engine API request. Mandatory init
step timeouts/errors fail init and leave the domain gate closed.

`BaseCheck` retry follows Delphi exactly. A normal init sends one BaseCheck
request. If `client.mark_server_update_sent()` was called before init, the next
`run_init_sequence` consumes that marker, waits up to `34 * 300ms` for
`AuthDone`, sends BaseCheck once, and if it still fails retries it 10 times with
`2000ms` pauses. The high-level UI wrappers that match Delphi
`ServerUpdateSent` behavior call the marker automatically:
`ui_update_version`, `ui_switch_dex`, and `ui_switch_spot`.

Domain pushes received before init completion are ignored, matching the Delphi
`InitDone` gate. Once init succeeds, the helper sends the Delphi post-init
refresh set: order snapshot request, strategy snapshot reply from the
dispatcher-owned strategy list, settings request, MM-orders subscription state,
and balance refresh request. If no strategy provider is registered, the
dispatcher sends the current local strategy list; an empty list is a valid
reply. `SnapshotRequested` is still queued for UI/diagnostic awareness. Set
`InitConfig::mm_orders_subscribe` when the UI needs a heat-map MM-orders
subscription value independent from `subscribe_trades`.

Use `run_with_dispatcher` plus `run_init_sequence` directly when an application
needs custom progress UI between connection readiness and the one-time init
requests.

## Trade Context

Market-level trade commands need the active server route in their wire header:
`base_currency_code` and `exchange_code` from `server_info()`. After
`connect_and_init`, build that route with:

```rust
let ctx = client.random_trade_ctx()?;
client.new_order(ctx, "BTCUSDT", false, 50_000.0, 0, 0.001);
```

If the application uses a custom init flow, call `request_base_check` first or
set `server_info` manually from a parsed BaseCheck response. For actions on an
order already present in `EventDispatcher::orders()`, prefer the
`*_tracked_order` wrappers; they derive UID, market, route, and status from the
tracked order state.

## Engine API Requests

For common one-shot reads, prefer typed request helpers:

```rust
let qty = client.request_balance(&mut dispatcher, "USDT", Duration::from_secs(12))?;
let hedge_mode = client.request_hedge_mode(&mut dispatcher, Duration::from_secs(12))?;
let api_expiration = client.request_api_expiration_time(&mut dispatcher, Duration::from_secs(12))?;
let transfer_assets = client.request_transfer_assets(&mut dispatcher, 0, Duration::from_secs(12))?;
let candles = client.request_candles_data(&mut dispatcher, Duration::from_secs(30))?;
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
let qty = client.request_balance(&mut dispatcher, "USDT", Duration::from_secs(12))?;
for event in dispatcher.take_queued_events() {
    handle_event(event);
}
```

Lower-level `Client::api_*` methods still return
`std::sync::mpsc::Receiver<EngineResponse>` for custom async flows:

```rust
let rx = client.api_get_markets_list();
let resp = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(12))?;
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

Chunked candles use a dedicated aggregator rather than the normal one-response
pending slot. Use `request_candles_data` for the common one-shot flow:

```rust
let merged = client.request_candles_data(
    &mut dispatcher,
    Duration::from_secs(30),
)?;
println!("markets={}", merged.markets.len());
```

## UI Settings Request

The UI settings channel is not an Engine API request, so it has no pending
`Receiver`. Use `request_client_settings` for the common one-shot flow:

```rust
let settings = client.request_client_settings(
    &mut dispatcher,
    Duration::from_secs(12),
)?;
println!("xSell={}", settings.x_sell);
```

## Order Snapshot Request

Use `request_order_snapshot` when the application needs the current active
orders as a one-shot operation:

```rust
let orders = client.request_order_snapshot(
    &mut dispatcher,
    Duration::from_secs(12),
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
client.subscribe_orderbooks(["ETHUSDT", "SOLUSDT"]);
client.unsubscribe_orderbook("BTCUSDT");
client.unsubscribe_orderbooks(["ETHUSDT", "SOLUSDT"]);
client.unsubscribe_all_orderbooks();
client.unsubscribe_all_trades();
```

The registry records the latest subscription intent. Before Init, transport
`Fine` does not replay it. After the one-time Init completes, reconnect replays
the registry automatically, so streams continue without the application running
Init again.
Orderbook subscriptions are per market name; incoming events carry `book_kind`
so the application can render futures and spot books separately.
The batched orderbook helpers update the same registry and send one
`emk_SubscribeOrderBook` / `emk_UnsubscribeOrderBook` request for the changed
market names. Use `unsubscribe_all_orderbooks` instead of raw
`api_unsubscribe_order_book(&[])` when clearing the UI selection: the raw
Engine API call does not update the reconnect registry.

For UI threads, clone a `ClientSender`:

```rust
let sender = client.sender();
std::thread::spawn(move || {
    sender.subscribe_orderbooks(["ETHUSDT", "SOLUSDT"]);
});
```

Fire-and-forget command methods append into the same unbounded send queues as
`Client::send_cmd`. Subscription methods enqueue control intents first, then the
client loop updates the registry and appends the resulting wire commands into
those send queues. Neither path has a local capacity cap. Use `try_*` methods
when the UI needs explicit feedback that the client is still alive:

```rust
match client.sender().try_subscribe_orderbook("BTCUSDT") {
    Ok(()) => {}
    Err(err) => eprintln!("subscribe failed: {err:?}"),
}
```

`ClientSender` mirrors the fire-and-forget typed command wrappers for
subscriptions, trade actions, UI commands, strategy commands, and balance
refresh. Use it when another UI or worker thread needs to send commands while
the owning thread is inside `run_with_dispatcher`:

```rust
let sender = client.sender();
std::thread::spawn(move || {
    sender.replace_order(ctx, "BTCUSDT", OrderType::Sell, 50100.0);
    sender.ui_mm_subscribe(true);
    sender.strat_sell_price_update(strategy_id, 49900.0);
    sender.balance_request_refresh();
});
```

The sender also exposes raw `send_cmd`, `send_cmd_keyed`, and
`send_api_request` methods for tools that already have a serialized payload
from `commands::*` builders. `send_api_request` is fire-and-forget: it does not
register a pending receiver, so the response is delivered through the running
dispatcher as `Event::EngineResponse`.

```rust
use moonproto::{Command, SendPriority};
use moonproto::commands::engine_request;

let sender = client.sender();
sender.send_api_request(engine_request::check_binance_tags());

let raw = build_custom_ui_payload();
sender.send_cmd(raw, Command::UI, SendPriority::High, true, 3);
```

## Periodic Refresh

`ClientConfig.refresh` controls automatic background Engine API requests.
The default matches the Delphi active client cadence, but refresh ticks are
gated by Init: transport `Fine` alone never starts `UpdateMarketsList` or
`CheckBinanceTags`.

```rust
RefreshConfig {
    update_markets_every: Some(Duration::from_secs(2)),
    check_tags_every: Some(Duration::from_secs(60)),
}
```

Set either field to `None` if the application wants to own that refresh manually.
When `check_tags_every` is enabled, the hourly four-request `CheckBinanceTags`
burst uses 200 ms spacing.

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
client.sliced_in_flight_count();
client.sliced_in_flight_blocks();
client.pending_high_count();
client.avg_over_heat();
client.rs();
client.server_time_delta_days();
client.server_token();
client.peer_app_token();
client.server_info();
```

`server_info()` is populated by `connect_and_init` or `run_init_sequence`.
For multiple independent server connections, create one `Client` and one
`EventDispatcher` per server.

## Shutdown

```rust
client.disconnect();
```

`disconnect` schedules `LogOff`, closes the socket path, and exits the current
run loop. To reconnect after final shutdown, create a new `Client`.
