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
    host,
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

`mask_ver = 0` is the base transport and does not require `moonext`. Transport
modes `1` and `2` are extended modes and require the optional `moonext` binary to
be available to the process. UI code should call
`moonproto::extended_transport_available()` before offering V1/V2; if it returns
`false`, only V0 should be selectable. The normal builder
`ClientConfig::with_transport_mode(1 | 2)` also falls back to V0 when `moonext`
is absent, so a public prototype without the closed binary still runs in base
transport mode. Unsupported mode values also normalize to V0.

The transport implementation is built into `moonproto` as `moonproto::transport`.
Consumers do not need a separate `moonproto-transport` crate.

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

`run`, `run_with_dispatcher`, and `run_with_dispatcher_state` block the caller
for the requested duration and run the MoonProto writer/orchestrator loop on
that caller thread. UDP receive is owned by the same `ProtocolCore` loop: it
waits with a nonblocking UDP poller and drains readable packets until
`WouldBlock`.
`run` raw callbacks and ordinary
`run_with_dispatcher` event callbacks are delivered through the application
callback queue after protocol/domain state is updated, so slow UI work does not
block ACK/retry/send progress. The call returns after the queued callbacks from
that run are drained. `Client::on_lifecycle` notifications use the same queued
delivery during run calls. `run_with_dispatcher_state` also uses the application
callback queue; it receives an `EventDispatcherSnapshot`, not the live
dispatcher, so slow UI work cannot stall protocol ACK/retry/send progress. The
snapshot copy itself is dispatcher-worker work; for high-rate hot paths prefer
`run_with_dispatcher` unless the callback needs the read model.

User/API sends append directly to the client's unbounded Delphi-style
`DataToSend` / `DataToSendH` / `DataToSendL` queues, separate from accepted UDP
packets and receive-decoded delivery. Typed domain helpers are gated by Init:
before `domain_ready`, subscriptions update only the reconnect registry and
order/UI/strategy/balance wrappers queue no wire command. After Init, the same
typed helpers append their Engine API/UI/domain wire commands to the send
queues. The public guarantee is no local capacity cap: dense incoming streams
do not drop queued user commands or Engine API requests.

If the callback needs to read the just-updated dispatcher state, use
`run_with_dispatcher_state`. The `state` argument is a read-only snapshot copied
after the dispatcher applied the event:

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

For reconnect/handshake health gates this matters mathematically. With
`set_err_emu(50)`, service packets are dropped at 25% and delivered at 75%.
Ten reconnect attempts fail with `0.25^10 = 0.000095%` when only the Rust client
emulates loss. If a test server also runs the same 50% emulator on its side, one
attempt needs both directions and succeeds with `0.75 * 0.75 = 56.25%`; ten
attempts fail with about `0.0257%`. A repeated failure of simple reconnect/API
operations under this gate should be treated as a protocol bug until proven
otherwise, not dismissed as random FireTest noise.

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

For live health tests, `Client::err_emu_diagnostics_snapshot()` returns
loss counters collected while `set_err_emu` is enabled. Use
`Client::reset_err_emu_diagnostics()` to start a new measurement window without
changing the loss rate.

The snapshot includes total valid/delivered/dropped incoming packets,
per-command counters, outgoing packets skipped by the hidden FireTest blackhole
hook, and per-`MPC_Sliced` datagram counters. For sliced datagrams the API
reports:

- `datagram_num`, `blocks_count`, delivered/dropped packet attempts, and
  per-block delivered/dropped counters;
- `block0_wire_cmd` and `block0_ui_cmd_id` when block 0 was observed;
- `completed_cmd`, `completed_payload_len`, and for completed UI settings the
  `completed_ui_cmd_id`;
- for completed Engine API responses:
  `completed_api_method`, `completed_api_uid`, and `completed_api_success`.

This is diagnostic API, not production control flow. Its purpose is to
distinguish three cases in tests: the server did not send a response, the server
sent/retried it but all needed packets were dropped by emulation, or packets
arrived but reassembly/parsing failed.

Low-level packet diagnostics are compile-gated behind the `diagnostic-trace`
feature. Build with that feature and set `MOONPROTO_TRACE_IO=1` or
`MOONPROTO_TRACE_SLICES=1` to print transport send/receive and sliced reassembly
logs.

## Protocol Metrics

`Client::protocol_metrics_snapshot()` returns passive protocol-loop counters:
UDP receive count, receive-side protocol nanoseconds, writer tick nanoseconds,
and send/maintenance nanoseconds. The old internal receive-decoded bridge is
not part of the public metrics API because production decoded delivery is
direct.

`Client::protocol_metrics_snapshot_with_dispatcher(&dispatcher)` adds the
current `EventDispatcher` public event queue length to the same snapshot.

The snapshot also separates CPU-ish protocol work from wall-clock waits:
`writer_cpu_*` excludes the fixed Delphi-style 5 ms sleep, `reader_protocol_*`
is the protocol recv path, and `active_dispatch_*` / `app_enqueue_*` measure the
typed active-library worker path before user callbacks. In
`run_with_dispatcher*`, `connect_and_init`, `run_init_sequence`, and the
one-shot wait helpers, heavy domain parsing/state apply is worker-side work. It
is still measured because millisecond samples are performance red flags, but it
does not block ACK/retry/send progress in the protocol recv loop. The
`*_over_100us`, `*_over_1ms`, `*_over_5ms` counters are coarse red flags for
unexpectedly heavy blocks. These are wall-clock durations of code sections, not
OS CPU counters, but they intentionally exclude network waits and user callback
body time.

For the current maximum samples, the snapshot carries diagnostic attribution:
`reader_protocol_max_cmd/payload_len`, `active_dispatch_max_cmd/payload_len`
plus `events/actions`, and `app_enqueue_max_cmd/payload_len` plus `events/mode`.
`cmd == u8::MAX` means the sample was not tied to a decoded incoming command.

These metrics are diagnostics only. They never affect retry, ACK, reconnect,
queueing, or drop decisions.

## Connection Setup

For the common setup path, call `connect_and_init`. The Delphi init contract is
mandatory: BaseCheck, AuthCheck, markets list, market indexes, price refresh,
balance refresh, order snapshot, client strategy snapshot, and settings sync.
`InitConfig` only adds optional stream subscriptions and timing:

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
for each Engine API response. Domain apply work is handed to the dispatcher
worker; when a wait helper returns, a FIFO barrier has already confirmed that
dispatcher state/events queued before the response were applied. It fills
`client.server_info()` after `BaseCheck` and `client.auth_info()` after a
successful `AuthCheck`.

Init is a one-time step for a `Client` session. After it succeeds, do not call
`run_init_sequence` again just because the UDP transport reconnected; the library
maintains the user-requested active-lib state for that `Client` session.

Init always sends `GetMarketsIndexes` and records the payload size in
`InitResult::indexes_response_bytes`, because trades, orderbooks, and
`UpdateMarketsList` price rows depend on the current server `mIndex` mapping.
Init also sends `TStratSchemaRequest` and records
`InitResult::strategy_schema_raw_bytes`,
`InitResult::strategy_schema_kind_count`, and
`InitResult::strategy_schema_field_count`. The decoded schema is stored in
`dispatcher.strats().strategy_schema()` and contains strategy kinds, fields,
TypeIDs, UI kind, picklists, visibility, and chapter/layout markers. This is
agreed active-library behavior: clients use the live server schema for strategy
UI metadata and typed `TStrategySerializer` snapshot writes instead of a
hardcoded Rust copy of Delphi `TStrategy` fields/defaults.
Periodic market refresh starts only after init opens the domain gate, so
BaseCheck/AuthCheck are not delayed by early background refresh traffic.
Critical BaseCheck/AuthCheck waits use the same default as Delphi
`TMoonProtoEngine.FTimeout`: 12 seconds per Engine API request. Mandatory init
step timeouts/errors fail init and leave the domain gate closed.

`AuthCheck` follows Delphi's result ordering: a successful server response opens
the next init step even if the optional account payload cannot be parsed. When
the payload is valid, `InitResult::auth_info` and `client.auth_info()` contain
the parsed account metadata (`account_id`, `btc_address`, sub-account flag,
transfer payload limit, and Hyperliquid DEX tail). When a successful AuthCheck
payload is malformed, `auth_check_ok` remains true, `auth_info` stays `None`,
and `InitResult::errors` receives a non-fatal parse note, matching Delphi's
`AuthCheck parse` log path.

If the first BaseCheck/AuthCheck block fails, init follows Delphi `InitInt`:
wait 200 ms, send one more BaseCheck, then send AuthCheck again. The retry
branch's final gate is the second AuthCheck result; the second BaseCheck still
updates `client.server_info()` if it succeeds.

`BaseCheck` retry follows Delphi exactly. A normal init sends one BaseCheck
request. If `client.mark_server_update_sent()` was called before init, the next
`run_init_sequence` consumes that marker, waits up to `34 * 300ms` for
`AuthDone`, sends BaseCheck once, and if it still fails retries it 10 times with
`2000ms` pauses. The high-level UI wrappers that match Delphi
`ServerUpdateSent` behavior call the marker automatically:
`ui_update_version`, `ui_switch_dex`, and `ui_switch_spot`.

Domain pushes received before init completion are ignored in every client run
mode, including the raw `Client::run` callback. This matches the Delphi
`InitDone` gate for `Order`, `Strat`, `Balance`, `TradesStream`,
`TradesResendResponse`, `OrderBook`, and `UI` pushes. Engine API responses and
transport service packets are not part of this domain gate, because Init itself
depends on Engine API. Once the Engine API init block succeeds, the helper opens
the domain gate, requests `TStratSchema`, then sends the post-init refresh set:
order snapshot request, full client strategy snapshot from the dispatcher-owned
local strategy list, settings request, MM-orders subscription state, and balance
refresh request. When the server later sends `TStratSnapshotRequest`, the
dispatcher replies from the same current local strategy list; an empty list is a
valid non-empty serializer payload.
`SnapshotRequested` is still queued for UI/diagnostic awareness. Set
`InitConfig::mm_orders_subscribe` when the UI needs a heat-map MM-orders
subscription value. If it is `None`, a previously queued `ui_mm_subscribe`
intent is used; otherwise the post-init UI command sends `false`. It never
falls back to `subscribe_trades`, because Delphi uses `cfg.ShowHeatMap` for
`TMMOrdersSubscribeCommand` and uses a separate
`Strats.HasActivityStrat or cfg.ShowHeatMap` value for `SubscribeAllTrades`.
If all-trades was queued before Init, the later registry flush still sends its
own stored `want_mm`; the post-init UI command does not rewrite that value.

Typed outgoing domain helpers use the same Init gate. Before Init:
`subscribe_*` / `unsubscribe_*` record the latest registry intent but do not put
subscription packets on the wire; trade wrappers, UI wrappers, strategy
wrappers, and `balance_request_refresh` queue nothing. Stateful order helpers
such as replace/cancel/stop/VStop/immune also do not mutate the local `Orders`
cache before Init. Raw `send_cmd`, `send_cmd_keyed`, and raw `api_*` helpers do
not bypass this gate: until Init opens the domain, the only Engine API requests
accepted by the raw path are the mandatory init primitives `BaseCheck`,
`AuthCheck`, `GetMarketsList`, `GetMarketsIndexes`, and `UpdateMarketsList`.
Balance bootstrap uses the post-init `TRequestBalanceRefresh`, matching the
MoonProto Delphi client where `GetMarketsBalanceFull` returns success without a
wire request.

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
While the client loop is active, registered Engine API responses are delivered
to their receiver by the active dispatcher worker after the same payload has
updated `EventDispatcher` state. The low-level raw `run` path still dispatches
pending receivers from receive-side DataReadInt because it intentionally has no
active dispatcher worker.
`run_until_response` uses the same dispatcher-worker queued-event behavior as
typed one-shot helpers and returns only after the worker has processed all
earlier queued domain work.

Use `request_engine_response` when a custom Engine API payload needs
caller-scoped timeout cleanup. Raw `api_*` receivers keep their pending slot
until the response arrives, a reconnect clears the session, or the same UID is
registered again.

Chunked candles use a dedicated aggregator rather than the normal one-response
pending slot. Use `request_candles_data` for the common one-shot flow:

Registered candle chunks are aggregated from the receive-side DataReadInt path
while the client loop is active; consumed chunks do not produce raw callback or
dispatcher events.

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

`request_client_settings` completes on the next applied
`TClientSettingsCommand`. It does not require the command UID to change because
the server may answer with the current settings object unchanged. The low-level
UI request is fire-and-forget, so this helper may reissue `TSettingsRequest`
inside the same timeout window.

If an application already has local UI settings before connecting, pass them to
the dispatcher with `set_client_settings_fallback`. This preserves Delphi
soft-read behavior for old `TClientSettingsCommand` packets: missing tail fields
keep the current `cfg` values (`FreePositionCheck`, `VolDropLevel`,
`UseStopMarket`, auto-start blobs, hotkey prices, `JoinSellKind`, and
`SignOrders` for `ver<2`) instead of being reset to Rust defaults.

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
client.subscribe_trades_for(false, ["BTCUSDT", "ETHUSDT"]);
client.subscribe_orderbook("BTCUSDT");
client.subscribe_orderbooks(["ETHUSDT", "SOLUSDT"]);
client.unsubscribe_orderbook("BTCUSDT");
client.unsubscribe_orderbooks(["ETHUSDT", "SOLUSDT"]);
client.unsubscribe_all_orderbooks();
client.unsubscribe_all_trades();
```

The registry records the latest subscription intent. Before Init, public
subscription calls update that registry but do not send Engine API/UI
subscription packets. The one-time Init flushes the pre-init registry once, and
later reconnects replay the registry automatically, so streams continue without
the application running Init again. After a server restart, orderbook replay is
delayed until fresh market indexes have been received for the current
`PeerAppToken`; this prevents new server `market_index` values from racing the
old local index map.
All-trades reconnect follows Delphi `NeedReconnectAllTrades`: until a
`TradesStream` packet is seen with the current `ServerToken`, the library sends
`UnsubscribeAllTrades`, waits 100 ms, then sends `SubscribeAllTrades`, retrying
that sequence no more often than once per 5000 ms. A queued
`SubscribeAllTrades` request arms the same gate, and a successful response
refreshes it, so the active library waits for the first trades packet before
deciding that the stream needs another reconnect cycle.
Orderbook reconnect follows Delphi `NeedResubscribeOrderBooks`: until a
successful full-registry `SubscribeOrderBook` response confirms the current
`ServerToken`, the library repeats the batched subscribe no more often than
once per 5000 ms. The retry resets local orderbook sequence/cache state but
keeps the last visible snapshot levels, matching Delphi `ResetOrderBookCaches`.
All-trades is opt-in in the Rust library. If the registry has no all-trades
subscription intent, incoming `TradesStream` / `TradesResendResponse` packets
are treated as unexpected and are dropped instead of becoming public events.
Orderbook subscriptions are per market name; incoming events carry `book_kind`
so the application can render futures and spot books separately.
The batched orderbook helpers update the same registry and send one
`emk_SubscribeOrderBook` / `emk_UnsubscribeOrderBook` request for the changed
market names. Use `unsubscribe_all_orderbooks` instead of raw
Engine API calls when clearing the UI selection: the raw Engine API call does
not update the reconnect registry. The high-level helper sends one batched
unsubscribe for the names that were remembered locally; if none were
remembered, it sends nothing.

For UI threads, clone a `ClientSender`:

```rust
let sender = client.sender();
std::thread::spawn(move || {
    sender.subscribe_orderbooks(["ETHUSDT", "SOLUSDT"]);
});
```

Fire-and-forget typed command methods append into the same unbounded send queues
as `Client::send_cmd` after Init. Before Init, typed domain methods are gated:
subscription methods update the shared reconnect registry only, and trade/UI/
strategy/balance wrappers queue nothing. Neither path has a local capacity cap.
Use `try_*` methods
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
    sender.ui_mm_subscribe(true);
    sender.strat_sell_price_update(strategy_id, 49900.0);
    sender.balance_request_refresh();
});
```

Order actions with Delphi-local side effects, such as replace/cancel/panic,
stop/VStop, and immune clicks, require mutable access to `Orders`. Send those
from the code path that owns the dispatcher/order state, or marshal the UI
intent there before calling the matching `Client`/`ClientSender` helper.

The sender also exposes raw `send_cmd`, `send_cmd_keyed`, and
`send_api_request` methods for tools that already have a serialized payload
from `commands::*` builders. These raw methods do not update typed library
state, but they still obey `domain_ready`; before Init, fallible raw methods
return `SubscribeError::DomainNotReady` for non-init commands. Normal
applications should prefer the typed helpers. `send_api_request` is
fire-and-forget: it does not register a pending receiver, so the response is
delivered through the running dispatcher as `Event::EngineResponse`.
`Client::send_api_request_async` is non-fallible; before Init it queues only
mandatory Init Engine API requests, and for other methods returns a closed
receiver without registering `api_pending`.

```rust
use moonproto::{Command, SendPriority};
use moonproto::commands::engine_request;

let sender = client.sender();
sender.send_api_request(engine_request::check_binance_tags());

let raw = build_custom_ui_payload();
sender.send_cmd(raw, Command::UI, SendPriority::High, true, 3);
```

`Command` is not a closed Rust enum; it preserves Delphi wire ordinals. Use
`Command::from_byte(raw)` and `cmd.to_byte()` when building low-level tools.

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
