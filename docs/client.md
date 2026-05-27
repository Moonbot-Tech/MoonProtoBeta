# Client

`MoonClient` is the recommended application handle for one MoonProto connection.
It owns a runtime thread and keeps the active session alive until `stop()` or
drop.

`Client` is the lower-level session object used by `MoonClient`, tests, protocol
tools, and custom runtimes. It owns the UDP socket, handshake state, retry
queues, pending Engine API registry, subscriptions, a process-level NTP guard,
per-client server-time delta, and server identity.

Create one `MoonClient` per server in regular applications.

## Configuration

Use the imported MoonBot key plus the endpoint/settings selected by the user:

```rust
let keys = moonproto::import_key(KEY_B64).expect("invalid key");
let cfg = moonproto::ClientConfig::new(
    host,
    3000,
    keys.master_key,
    keys.mac_key,
)
.with_transport_mode(mask_ver);
let client = moonproto::MoonClient::connect(
    cfg,
    moonproto::ConnectConfig::new(moonproto::InitConfig::default()),
)?;
```

For UI/config screens, call `moonproto::parse_key_info(KEY_B64)`. It returns the
same cryptographic keys plus `display_name` equivalent to MoonBot's
`rnd + "  " + FormatDateTime("dd.mm.yyyy hh:nn", Date)` and optional suggested
endpoint/transport settings from current key exports. Those suggestions are not
mandatory: applications can pre-fill controls from them and then connect with
whatever host, port, and mode the user selected.

```rust
let info = moonproto::parse_key_info(KEY_B64).expect("invalid key");
ui.key_label = info.display_name;
if let Some(network) = info.network {
    ui.host = network.address.map(|ip| ip.to_string()).unwrap_or_default();
    ui.port = network.port;
    ui.mask_ver = network.mask_ver;
}
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

The recommended run path is `MoonClient::connect`:

```rust
use moonproto::{ConnectConfig, InitConfig, InitialStrategies, MoonClient, TradesStreamMode};

let client = MoonClient::connect(cfg, ConnectConfig::new(InitConfig {
    initial_strategies: Some(InitialStrategies::new(
        0,
        Vec::new(), // replace with your local strategy list if the app has one
    )),
    subscribe_trades: Some(TradesStreamMode::TradesOnly),
    subscribe_orderbooks: vec!["BTCUSDT".to_string()],
    ..Default::default()
}))?;

client.subscribe_orderbook("ETHUSDT")?;
// After an order appears in events/snapshots:
// client.orders().move_order(order_uid, 50100.0)?; // also accepts &Order

for event in client.drain_events() {
    println!("event: {event:?}");
}
for lifecycle in client.drain_lifecycle_events() {
    println!("lifecycle: {lifecycle:?}");
}

client.stop()?;
```

This path performs active-library work: state dispatch, per-client
`ServerTimeDelta` linking, orderbook full requests, trades gap ticks, market-index
gating, reconnect restore, and Engine API pending routing. Before the first Init,
transport reconnects do not emit background Engine API. After Init, reconnect
inside the same `Client` session maintains the user-requested active-lib state.

`MoonClient` owns the runtime thread. Applications do not choose a finite
protocol-loop duration; the session runs until explicit `stop()` or drop. UI
code reads typed events, lifecycle events, and immutable snapshots:

```rust
if let Some(snapshot) = client.snapshot() {
    for order in snapshot.orders().iter() {
        println!("{order:?}");
    }
}
```

New order and market-level trade actions are user intents too. They are
marshalled into the runtime owner, which derives the Delphi route bytes from the
active session:

```rust
use moonproto::{NewOrderParams, OrderSide};

client.trade().new_order(
    NewOrderParams::new("BTCUSDT", OrderSide::Long, 50_000.0, 0.001),
)?;
client.trade().join_orders("BTCUSDT", OrderSide::Long)?;
```

Existing-order actions are applied to the live `Orders` state first and only
then converted to protocol commands:

```rust
client.orders().move_order(order_uid, new_price)?;
client.orders().cancel(order_uid)?;
```

Those calls also accept `&Order` from a snapshot, so UI code can act on the
visible order object without treating the UID as a protocol detail:

```rust
if let Some(snapshot) = client.snapshot() {
    if let Some(order) = snapshot.orders().get(order_uid) {
        client.orders().move_order(order, new_price)?;
    }
}
```

One-shot Engine API helpers also run inside the owned runtime. Read helpers
parse their payloads; mutation helpers return `Ok(())` after the server accepts
the operation and convert server failures into `MoonClientError`:

```rust
let balance = client.request_balance("USDT", Duration::from_secs(15))?;
client.set_leverage("BTCUSDT", 20, Duration::from_secs(15))?;
client.set_hedge_mode(true, Duration::from_secs(15))?;
client.cancel_all_orders(Duration::from_secs(15))?;
```

Advanced protocol tools can still own `Client + EventDispatcher` directly, but
that is not the regular desktop/UI application shape. Such tools must keep the
protocol pump alive themselves; normal applications should not model the session
as "run for N seconds".

User/API sends append directly to the client's unbounded Delphi-style
`DataToSend` / `DataToSendH` / `DataToSendL` queues, separate from accepted UDP
packets and receive-decoded delivery. Typed domain helpers are gated by Init:
before `domain_ready`, subscriptions update only the reconnect registry and
order/UI/strategy/balance wrappers queue no server command. After Init, the same
typed helpers append their Engine API/UI/domain commands to the send queues. The
public guarantee is no local capacity cap: dense incoming streams do not drop
queued user commands or Engine API requests.

Low-level raw callbacks are for diagnostics/protocol tools that intentionally
bypass the Active Lib. If an application does that, it also owns the recovery
work that `MoonClient` normally performs automatically.

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
per-command counters, test-only outgoing drop counters, and per-sliced-datagram
counters. For sliced datagrams the API
reports:

- `datagram_num`, `blocks_count`, delivered/dropped packet attempts, and
  per-block delivered/dropped counters;
- optional raw diagnostic identifiers when the first block or completed payload
  was observed;
- `completed_cmd` and `completed_payload_len`;
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
`MoonClient`, `connect_and_init`, `run_init_sequence`, and the one-shot wait
helpers keep heavy domain parsing/state apply on the worker side. It
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

For regular applications, `MoonClient::connect` owns the setup path:

```rust
let client = MoonClient::connect(cfg, ConnectConfig::new(init))?;
```

The Delphi init contract is mandatory: BaseCheck, AuthCheck, markets list,
market indexes, price refresh, balance refresh, order snapshot, client strategy
snapshot, and settings sync. `InitConfig` only adds local strategies, optional
stream subscriptions, and timing.

For custom low-level runtimes that deliberately own `Client + EventDispatcher`,
the same setup is available as `connect_and_init`:

```rust
use std::time::Duration;
use moonproto::{connect_and_init, ConnectConfig, InitConfig, TradesStreamMode};

let init = InitConfig {
    subscribe_trades: Some(TradesStreamMode::TradesOnly),
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
the active strategy state and contains strategy kinds, fields,
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
`request_version_update`, `switch_dex`, and `switch_spot`.

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
subscription value. If it is `None`, a previously queued
`set_mm_orders_subscription` intent is used; otherwise the post-init UI command
sends `false`. It never
falls back to `subscribe_trades`: the MM-orders UI flag and the all-trades
subscription `want_mm` flag are separate intents.
If all-trades was queued before Init, the later registry flush still sends its
own stored `want_mm`; the post-init UI command does not rewrite that value.

Typed outgoing domain helpers use the same Init gate. Before Init:
`subscribe_*` / `unsubscribe_*` record the latest registry intent but do not put
subscription packets into the send queue; trade wrappers, UI wrappers, strategy
wrappers, and `balance_request_refresh` queue nothing. Stateful order helpers
such as replace/cancel/stop/VStop/immune also do not mutate the local `Orders`
cache before Init. Raw `send_cmd`, `send_cmd_keyed`, and raw `api_*` helpers do
not bypass this gate: until Init opens the domain, the only Engine API requests
accepted by the raw path are the mandatory init primitives `BaseCheck`,
`AuthCheck`, `GetMarketsList`, `GetMarketsIndexes`, and `UpdateMarketsList`.
Balance bootstrap uses the post-init `TRequestBalanceRefresh`, matching the
MoonProto Delphi client where `GetMarketsBalanceFull` returns success without a
serialized balance snapshot.

Use lower-level `Client` plus `run_init_sequence` directly only when an
application deliberately implements its own custom runtime/progress UI between
connection readiness and the one-time init requests.

## Trade Context

This is a low-level/custom-runtime topic. Regular UI actions should not build
or pass `TradeCtx`:

```rust
client.trade().new_order(NewOrderParams::new(
    "BTCUSDT",
    OrderSide::Long,
    50_000.0,
    0.001,
))?;
client.orders().move_order(order_uid, new_price)?; // or pass &Order from a snapshot
```

The runtime derives `TradeCtx` from `base_currency_code` and `exchange_code`
learned during Init/BaseCheck. If a custom low-level runtime calls raw
`Client::new_order(ctx, ...)` directly, it must call `request_base_check` first
or set `server_info` manually from a parsed BaseCheck response.

## Engine API Requests

For common one-shot reads and mutations, use `MoonClient` request helpers. The
runtime keeps the UDP loop alive while the caller waits for the request timeout:

```rust
let qty = client.request_balance("USDT", Duration::from_secs(12))?;
let hedge_mode = client.request_hedge_mode(Duration::from_secs(12))?;
let api_expiration = client.request_api_expiration_time(Duration::from_secs(12))?;
let transfer_assets = client.request_transfer_assets(0, Duration::from_secs(12))?;
let markets_received = client.refresh_candles(Duration::from_secs(30))?;
client.set_leverage("BTCUSDT", 20, Duration::from_secs(12))?;
client.set_hedge_mode(true, Duration::from_secs(12))?;
client.confirm_risk_limit("BTCUSDT", Duration::from_secs(12))?;
```

These helpers validate the server response and parse the payload. Engine API
failures are returned as `MoonClientError::EngineRequest`. Mutation helpers
return `Ok(())` because most mutation replies are acknowledgements rather than
typed data snapshots.

Low-level `Client::api_*` receivers remain only for custom runtimes and
diagnostic tools. A normal application should not wait on raw receivers from the
UI thread.

Chunked candles use a dedicated aggregator rather than the normal one-response
pending slot. Use `refresh_candles` for the common one-shot flow:

Registered candle chunks are aggregated from the receive-side DataReadInt path
while the client loop is active; consumed chunks do not produce raw callback or
dispatcher events.

```rust
let markets_received = client.refresh_candles(Duration::from_secs(30))?;
println!("markets={markets_received}");
```

`request_candles_data` is kept for diagnostics that need the merged raw protocol
payload; chart UI should read retained candles from market history readers.

## UI Settings Request

The UI settings channel is not an Engine API request, so it has no pending
`Receiver`. Use `request_client_settings` for the common one-shot flow:

```rust
let settings = client.request_client_settings(Duration::from_secs(12))?;
println!("xSell={}", settings.x_sell);
```

`request_client_settings` completes on the next applied settings snapshot. It
does not require the command UID to change because the server may answer with
the current settings object unchanged. The low-level UI request is
fire-and-forget, so this helper may reissue the refresh request inside the same
timeout window.

If an application already has local UI settings before connecting, pass them to
the dispatcher with `set_client_settings_fallback`. This preserves Delphi
soft-read behavior for old settings snapshots: missing tail fields keep the
current local values (`FreePositionCheck`, `VolDropLevel`, `UseStopMarket`,
auto-start blobs, hotkey prices, `JoinSellKind`, and `SignOrders` for old
versions) instead of being reset to Rust defaults.

## Order Snapshot Request

Use `MoonClient::request_order_snapshot` when the application needs the current active
orders as a one-shot operation:

```rust
let orders = client.request_order_snapshot(Duration::from_secs(12))?;
println!("active orders={}", orders.len());
```

The helper requests the fresh snapshot, applies it to runtime `Orders`, and
waits until the dispatcher has finished Delphi missing-worker follow-up requests
for orders absent from the fresh snapshot.

## Balance Snapshot Request

Use `MoonClient::request_balance_snapshot` when the application needs a fresh full balance
read model from the Balance channel:

```rust
let balances = client.request_balance_snapshot(Duration::from_secs(15))?;
println!("balance markets={}", balances.len());
println!("btc total={}", balances.global().btc_balance_total);
```

The helper sends `TRequestBalanceRefresh`, keeps the UDP loop running, waits for
the next `TBalanceSnapshotFull`, and returns a cloned `BalancesState`.

## Subscriptions

Use registry-aware methods:

```rust
use moonproto::TradesStreamMode;

client.subscribe_all_trades(TradesStreamMode::TradesOnly)?;
client.subscribe_trades_for(
    TradesStreamMode::TradesOnly,
    ["BTCUSDT", "ETHUSDT"],
)?;
client.subscribe_orderbook("BTCUSDT")?;
client.subscribe_orderbooks(["ETHUSDT", "SOLUSDT"])?;
client.unsubscribe_orderbook("BTCUSDT")?;
client.unsubscribe_orderbooks(["ETHUSDT", "SOLUSDT"])?;
client.unsubscribe_all_orderbooks()?;
client.unsubscribe_all_trades()?;
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
subscribe/unsubscribe request for the changed market names. Use
`unsubscribe_all_orderbooks` instead of raw
Engine API calls when clearing the UI selection: the raw Engine API call does
not update the reconnect registry. The high-level helper sends one batched
unsubscribe for the names that were remembered locally; if none were
remembered, it sends nothing.

Regular applications call the `MoonClient` handle from their UI/runtime layer:

```rust
client.subscribe_orderbooks(["ETHUSDT", "SOLUSDT"])?;
client.set_mm_orders_subscription(true)?;
client.refresh_balances()?;
client.strat_sell_price_update(strategy_id, sell_price)?;
```

Typed command methods append into the same unbounded Delphi-style send queues
after Init. Before Init, subscriptions update only the reconnect registry and
other domain commands queue nothing. Neither path has a local capacity cap.

Order actions with Delphi-local side effects, such as replace/cancel/panic,
stop/VStop, and immune clicks, are intents on `client.orders()`. The runtime
owner applies them to live `Orders` before queueing protocol commands.

`ClientSender` and raw `send_cmd` / `send_cmd_keyed` remain available only for
custom low-level runtimes and protocol tools that already own
`Client + EventDispatcher` directly. They are not the regular UI application
model.

`Command` is not a closed Rust enum; it preserves unknown raw channel
identifiers. Use `Command::from_byte(raw)` and `cmd.to_byte()` when building
low-level tools.

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

These raw observability methods are on low-level `Client` for diagnostic tools.
Regular applications observe connection/domain state through `MoonClient`
events, lifecycle events, immutable snapshots, and request results. For
multiple independent server connections, create one `MoonClient` per server.

## Shutdown

```rust
client.stop()?;
```

`stop` schedules `LogOff`, closes the socket path, and joins the runtime thread.
Dropping `MoonClient` performs the same shutdown best-effort. To reconnect after
final shutdown, create a new `MoonClient`.
