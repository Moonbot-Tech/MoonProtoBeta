# Client

`MoonClient` is the recommended application handle for one MoonProto connection.
It owns a runtime thread and keeps the active session alive until
`disconnect()` or drop.

The low-level UDP session object is an internal/diagnostic layer behind
`MoonClient`. Regular applications should not own it directly.

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
.with_transport_mode(transport_mode);
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
If the UI needs the key date separately, use `info.date()`.

```rust
let info = moonproto::parse_key_info(KEY_B64).expect("invalid key");
ui.key_label = info.display_name;
if let Some(network) = info.network {
    ui.host = network.address.map(|ip| ip.to_string()).unwrap_or_default();
    ui.port = network.port;
    ui.transport_mode = network.transport_mode;
}
```

`ClientConfig::new` sets:

- `transport_mode = TransportMode::V0`;
- random `client_id`;
- `ntp_host = Some("pool.ntp.org")` and uses one shared NTP syncer per process;
- `refresh = RefreshConfig::default()` (`UpdateMarketsList` every 2 seconds and
  `CheckBinanceTags` every 60 seconds after Init).

`TransportMode::V0` is the base transport. `TransportMode::V1` and
`TransportMode::V2` select the extended built-in transports. The selected mode
must match the server-side connection setting.

Path MTU discovery is automatic. If diagnostics mention a too-large
`SizeAck`/`ProbeMTUAck` packet, that is the expected negative result of an
internal PMTU probe, especially on Linux where the OS reports `EMSGSIZE`
(`os error 90`). User data should not be handled by applications here: large
Engine/API payloads are sliced and retried by the runtime.

Override only what you need:

```rust
use std::time::Duration;
use moonproto::{ClientConfig, RefreshConfig, TransportMode};

let cfg = ClientConfig::new(host, port, master_key, mac_key)
    .with_transport_mode(TransportMode::V0)
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
        Vec::new(), // pass the current local strategy list if the app has one
    )),
    subscribe_trades: Some(TradesStreamMode::TradesOnly),
    subscribe_orderbooks: vec!["BTCUSDT".to_string()],
    ..Default::default()
}))?;

// In a GUI this check lives in the normal UI tick / event callback.
for lifecycle in client.drain_lifecycle_events() {
    if matches!(lifecycle, moonproto::LifecycleEvent::Ready) {
        println!("MoonProto is ready");
    }
}

client.streams().subscribe_orderbook("ETHUSDT")?;
// After an order appears in events/snapshots, pass the visible &Order:
// client.orders().move_order(order, 50100.0)?;

for event in client.drain_events() {
    println!("event: {event:?}");
}
for lifecycle in client.drain_lifecycle_events() {
    println!("lifecycle: {lifecycle:?}");
}

client.disconnect()?;
client.wait_finished()?;
```

This path performs active-library work: state dispatch, per-client
`ServerTimeDelta` linking, orderbook full requests, trades gap ticks, market-index
gating, reconnect restore, and Engine API pending routing. Before the first Init,
transport reconnects do not emit background Engine API. After Init, reconnect
inside the same `MoonClient` session maintains the user-requested active-lib state.

`MoonClient::connect` starts the owned runtime thread and returns immediately.
The background runtime performs AuthDone and the one-time Init sequence, then
emits `LifecycleEvent::Ready`. Startup failure arrives as
`LifecycleEvent::ConnectFailed`. UI code does not create its own protocol thread
and does not block a paint/input callback waiting for network readiness.

Command-line tools, scripts, and tests that do one-shot work after connect can
use `MoonClient::connect_blocking(cfg, connect, timeout)` instead: it blocks on a
single channel receive until `Ready` or failure (no busy polling). This is a
convenience for scripts, not the UI path — long-running applications use
`connect` and react to `LifecycleEvent::Ready` like the Delphi client gates work
on its async `InitDone` flag.
Applications do not choose a finite protocol-loop duration; the session runs
until explicit `disconnect()` or drop. UI code reads typed events, lifecycle
events, and immutable snapshots:

`Ready` covers the mandatory init spine: authorization, BaseCheck/AuthCheck,
markets list/server-index map, initial price refresh, and strategy schema.
Strategy schema is
requested after AuthCheck and may be received while the market init requests are
still running; `Ready` is emitted only after the schema is applied. It does not
wait for retained 5m candles, CoinCard candles, transfer assets, or the first
stream packet; those arrive as normal domain events and snapshot updates.

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

let Some(snapshot) = client.snapshot() else { return; };
let Some(market) = snapshot.markets().find("BTC") else { return; };

let _ticket = client.trade().new_order(
    NewOrderParams::for_market(&market, OrderSide::Long, 50_000.0, 0.001),
)?;
client.trade().join_orders_for_market(&market, OrderSide::Long)?;
```

Existing-order actions are applied to the live `Orders` state first and only
then converted to protocol commands:

UI code should normally act on the visible `&Order` from a snapshot. Raw UID is
accepted as a selector fallback, but the runtime still resolves the live order
before sending:

```rust
if let Some(snapshot) = client.snapshot() {
    if let Some(order) = snapshot.orders().get(ui_state.selected_order_uid()) {
        client.orders().move_order(order, new_price)?;
        client.orders().cancel(order)?;
    }
}
```

Regular UI code reads maintained state and uses non-blocking Active Lib intents.
Mutations return an `EngineActionTicket` after queuing the request, and
completion arrives later as `Event::EngineAction`. Retained state updates arrive
through the matching domain snapshots/events.

```rust
client.account().refresh_hedge_mode()?;
let ticket = client.account().set_leverage("BTCUSDT", 20)?;
client.account().set_hedge_mode(true)?;
client.account().cancel_all_orders()?;
```

User/API sends append directly to the client's unbounded Delphi-style
`DataToSend` / `DataToSendH` / `DataToSendL` queues, separate from accepted UDP
packets and receive-decoded delivery. Typed domain helpers are gated by Init:
before `domain_ready`, subscriptions update only the reconnect registry and
order/UI/strategy/balance wrappers queue no server command. After Init, the same
typed helpers append their Engine API/UI/domain commands to the send queues. The
public guarantee is no local capacity cap: dense incoming streams do not drop
queued user commands or Engine API requests.

The regular application model is `MoonClient`: one runtime owner, event sink,
snapshots, and fire-and-forget user intents. Public code should not model the
session as "run the protocol for N seconds".

## Packet Loss Test Hook

This section is available only when the crate is built with
`--features diagnostics`. Regular applications should leave that feature off;
the production Active Lib surface does not expose packet-loss emulation,
per-packet CPU counters, or debug blackhole hooks.

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
enable `set_err_emu` after the runtime reaches `LifecycleEvent::Ready`. Enabling
it before connection intentionally tests handshake/reconnect loss and can
prevent the client from reaching the API phase at all.

For live health tests, `err_emu_diagnostics_snapshot()` returns loss counters
collected while `set_err_emu` is enabled. It is available on both the low-level
`Client` and the high-level `MoonClient`, so a health/stress harness on either
path reads the same counters. Use the matching hidden reset hook in diagnostic
tests to start a new measurement window without changing the loss rate. This
whole facility is a test/diagnostic hook; production applications never enable
ErrEmu.

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

This section also requires `--features diagnostics`.

`MoonClient::protocol_metrics_snapshot()` returns passive protocol-loop counters:
UDP receive count, the last PMTU value reported by server `Ping`,
receive-side protocol nanoseconds, writer tick nanoseconds, and
send/maintenance nanoseconds. The old internal receive-decoded bridge is not
part of the public metrics API because production decoded delivery is direct.

The snapshot also separates CPU-ish protocol work from wall-clock waits:
`writer_cpu_*` excludes the fixed Delphi-style 5 ms sleep, `reader_protocol_*`
is the protocol recv path excluding deliberate Delphi-compatible protocol
barriers, and `reader_protocol_wait_*` accounts for those barriers separately
(currently the `WhoAreYou` -> duplicate `ImFriend` 32 ms wait).
`active_dispatch_*` / `app_enqueue_*` measure typed Active Lib state apply and
event enqueue before user callbacks. In `MoonClient`, the runtime owner applies
protocol/domain payloads directly to Active Lib state, publishes a snapshot,
then emits events through the configured sink. Millisecond samples are
performance red flags. The
`*_over_100us`, `*_over_1ms`, `*_over_5ms` counters are coarse red flags for
unexpectedly heavy blocks. These are wall-clock durations of code sections, not
OS CPU counters, but they intentionally exclude network waits and user callback
body time.

FireTest treats any `>5ms` sample in CPU-ish sections (`reader_protocol`,
`writer_cpu`, `active_dispatch`, `app_enqueue`, or send/maintenance phase) as a
hard health failure. `>1ms` samples stay visible in the summary as watch
signals, because large initial snapshots and balance/strategy payloads can sit
near that boundary while still matching the Delphi machine effect.

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

It starts the runtime immediately and reports completion as
`LifecycleEvent::Ready`; startup errors arrive as `LifecycleEvent::ConnectFailed`.

The Delphi init contract is mandatory: BaseCheck, AuthCheck, markets list
(which also builds the initial server-index map), price refresh, balance
refresh, order snapshot, client strategy
snapshot, and settings sync. `InitConfig` only adds local strategies, optional
stream subscriptions, and timing.

The runtime keeps the client loop running while it waits for the connection and
for each mandatory Engine API response. It applies state in the runtime owner,
publishes snapshots, and fills `client.server_info()` after `BaseCheck` and
`client.auth_info()` after a successful `AuthCheck`.

Init is a one-time step for a `MoonClient` session. After it succeeds, do not
start a second init just because the UDP transport reconnected; the library
maintains the user-requested active-lib state for that session.

Cold init does not send a separate `GetMarketsIndexes`: Delphi
`GetMarketsList` builds `SrvMarkets` from the server list order and stores the
current `PeerAppToken`. After reconnect/server-token changes, the library
refreshes `GetMarketsIndexes` before any `UpdateMarketsList` price refresh that
depends on server `mIndex` values. Init also sends `TStratSchemaRequest` after
AuthCheck. The decoded schema is stored in the active strategy state and
contains strategy kinds, fields,
TypeIDs, UI kind, picklists, visibility, and chapter/layout markers. This is
agreed active-library behavior: clients use the live server schema for strategy
UI metadata and typed `TStrategySerializer` snapshot writes instead of a
hardcoded Rust copy of Delphi `TStrategy` fields/defaults. Only this schema
request/response is allowed through the pre-Init Strat gate; regular Strat
commands remain closed until `domain_ready`.
Periodic market refresh starts only after init opens the domain gate, so
BaseCheck/AuthCheck are not delayed by early background refresh traffic.
Critical BaseCheck/AuthCheck waits use the same default as Delphi
`TMoonProtoEngine.FTimeout`: 12 seconds per Engine API request. Mandatory init
step timeouts/errors fail init and leave the domain gate closed.

`AuthCheck` follows Delphi's result ordering: a successful server response opens
the next init step even if the optional account payload cannot be parsed. When
the payload is valid, `client.auth_info()` contains
the parsed account metadata (`account_id`, `btc_address`, sub-account flag,
transfer payload limit, and Hyperliquid DEX tail). When a successful AuthCheck
payload is malformed, `auth_check_ok` remains true, `auth_info` stays `None`,
and an internal non-fatal parse note is recorded, matching Delphi's
`AuthCheck parse` log path.

If the first BaseCheck/AuthCheck block fails, init follows Delphi `InitInt`:
wait 200 ms, send one more BaseCheck, then send AuthCheck again. The retry
branch's final gate is the second AuthCheck result; the second BaseCheck still
updates `client.server_info()` if it succeeds.

`BaseCheck` retry follows Delphi exactly. A normal init sends one BaseCheck
request. If a version/switch action marked `ServerUpdateSent` before init, the
init spine consumes that marker, waits up to `34 * 300ms` for
`AuthDone`, sends BaseCheck once, and if it still fails retries it 10 times with
`2000ms` pauses. The high-level UI wrappers that match Delphi
`ServerUpdateSent` behavior call the marker automatically:
`settings().request_release_update`, `settings().request_version_update`,
`settings().switch_dex`, and
`settings().switch_spot`.

Domain pushes received before init completion are ignored in every client run
mode, including internal low-level test pumps. This matches the Delphi
`InitDone` gate for `Order`, `Strat`, `Balance`, `TradesStream`,
`TradesResendResponse`, `OrderBook`, and `UI` pushes. Engine API responses and
transport service packets are not part of this domain gate, because Init itself
depends on Engine API. Once the Engine API init block succeeds, the helper opens
the domain gate, requests `TStratSchema`, then sends the post-init refresh set:
order snapshot request, full client strategy snapshot from the runtime-owned
local strategy list, settings request, MM-orders subscription state, and balance
refresh request. When the server later sends `TStratSnapshotRequest`, the
runtime replies from the same current local strategy list; an empty list is a
valid non-empty serializer payload. Terminal code does not build or send this
reply manually. Set
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
cache before Init. The mandatory init primitives (`BaseCheck`, `AuthCheck`,
`GetMarketsList`, `UpdateMarketsList`, and the strategy schema request) are
owned by the runtime, not by application code.
Balance bootstrap uses the post-init `TRequestBalanceRefresh`, matching the
MoonProto Delphi client where `GetMarketsBalanceFull` returns success without a
serialized balance snapshot.

## Trade Context

Regular UI actions do not build or pass `TradeCtx`:

```rust
let Some(snapshot) = client.snapshot() else { return; };
let Some(market) = snapshot.markets().find("BTC") else { return; };

client.trade().new_order(NewOrderParams::for_market(
    &market,
    OrderSide::Long,
    50_000.0,
    0.001,
))?;
// Existing-order actions normally use &Order from snapshot.orders().
```

The runtime derives `TradeCtx` from `base_currency_code` and `exchange_code`
learned during Init/BaseCheck. `new_order` also returns `NewOrderTicket`; keep
it only when the UI wants to correlate a click with the later server-created
order. Normal order tables should read the created order from
`snapshot().orders()`.

## Engine API Requests

User-facing UI refreshes and mutations use non-blocking Active Lib intents:

```rust
client.account().refresh_hedge_mode()?; // async; read Event::Account + snapshot().account()
client.account().refresh_api_expiration_time()?; // async; read Event::Account + snapshot().account()
client.balances().refresh_transfer_assets()?; // async; read snapshot().transfer_assets()
if let Some(snapshot) = client.snapshot() {
    if let Some(market) = snapshot.markets().get("BTCUSDT") {
        let _coin_card_ticket = client
            .candles()
            .request_coin_card_for(&market, moonproto::DeepHistoryKind::Hour4)?;
    }
}
client.settings().refresh()?; // async; read Event::Settings + snapshot().settings()
client.account().set_leverage("BTCUSDT", 20)?;
client.account().set_hedge_mode(true)?;
client.account().confirm_risk_limit("BTCUSDT")?;
```

`candles().request_coin_card_for(&market, kind)` is intentionally non-blocking
even though the underlying Delphi `Engine.getDeepHistory` call is blocking:
Delphi UI sets a need flag on the selected `TMarket` and the background worker
fills `TMarket.CoinCardCandles`. In Rust, completion arrives as
`Event::CoinCardCandles` and the rows are readable through
`snapshot().coin_card_candles_for(&market, kind)`. The string-keyed
`request_coin_card(market, kind)` variant is a convenience for scripts/tools.

Chunked candles use a dedicated aggregator rather than the normal one-response
pending slot. Active Lib sends the full 5m snapshot request automatically after
trades storage is enabled, emits `Event::CandlesSnapshot` after the history
worker applies it, and chart UI reads retained rows from market history readers.
Lost chunked requests or history-worker barriers fail by event and are retried
for the same active trades scope instead of leaving the scope stuck forever.

## UI Settings Request

The UI settings channel is not an Engine API request, so it has no Engine API
pending `Receiver`. Regular UI code queues a refresh request and reacts to the
settings event:

```rust
client.settings().refresh()?;

for event in client.drain_events() {
    if matches!(
        event,
        moonproto::Event::Settings(moonproto::state::SettingsEvent::ClientSettingsUpdated)
    ) {
        if let Some(snapshot) = client.snapshot() {
            if let Some(settings) = &snapshot.settings().client_settings {
                println!("xSell={}", settings.x_sell);
            }
        }
    }
}
```

If an application already has local UI settings before connecting, pass them in
the active-library init/settings path. This preserves Delphi soft-read behavior
for old settings snapshots: missing tail fields keep the current local values
(`FreePositionCheck`, `VolDropLevel`, `UseStopMarket`, AutoStart settings, hotkey
prices, `JoinSellKind`, and `SignOrders` for old versions) instead of being
reset to Rust defaults.

## Order Snapshot Request

Regular UI code queues an order snapshot refresh and then reads the active order
model after order events:

```rust
client.orders().request_snapshot()?;

for event in client.drain_events() {
    if matches!(event, moonproto::Event::Order(moonproto::state::OrderEvent::Snapshot)) {
        if let Some(snapshot) = client.snapshot() {
            println!("active orders={}", snapshot.orders().len());
        }
    }
}
```

## Balance Snapshot Request

Regular UI code queues a full balance refresh and reads the balance model after
balance events:

```rust
client.balances().refresh()?;

for event in client.drain_events() {
    if matches!(event, moonproto::Event::Balance(_)) {
        if let Some(snapshot) = client.snapshot() {
            println!("markets={}", snapshot.markets().market_count());
            println!("btc total={}", snapshot.balances().global().btc_balance_total);
        }
    }
}
```

## Subscriptions

Use registry-aware methods:

```rust
use moonproto::TradesStreamMode;

client.streams().subscribe_all_trades(TradesStreamMode::TradesOnly)?;
client.streams().subscribe_trades_for(
    TradesStreamMode::TradesOnly,
    ["BTCUSDT", "ETHUSDT"],
)?;
client.streams().subscribe_orderbook("BTCUSDT")?;
client.streams().subscribe_orderbooks(["ETHUSDT", "SOLUSDT"])?;
client.streams().unsubscribe_orderbook("BTCUSDT")?;
client.streams().unsubscribe_orderbooks(["ETHUSDT", "SOLUSDT"])?;
client.streams().unsubscribe_all_orderbooks()?;
client.streams().unsubscribe_all_trades()?;
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
Orderbook subscriptions are per market name; incoming events carry typed
`OrderBookKind` so the application can render futures and spot books separately.
The batched orderbook helpers update the same registry and send one
subscribe/unsubscribe request for the changed market names. Use
`unsubscribe_all_orderbooks` instead of raw
Engine API calls when clearing the UI selection: the raw Engine API call does
not update the reconnect registry. The high-level helper sends one batched
unsubscribe for the names that were remembered locally; if none were
remembered, it sends nothing.

Regular applications call the `MoonClient` handle from their UI/runtime layer:

```rust
client.streams().subscribe_orderbooks(["ETHUSDT", "SOLUSDT"])?;
client.settings().set_mm_orders_subscription(true)?;
client.balances().refresh()?;
client.strategies().sell_price_update(strategy_id, sell_price)?;
```

Typed command methods append into the same unbounded Delphi-style send queues
after Init. Before Init, subscriptions update only the reconnect registry and
other domain commands queue nothing. Neither path has a local capacity cap.

Order actions with Delphi-local side effects, such as replace/cancel/panic,
stop/VStop, and immune clicks, are intents on `client.orders()`. The runtime
owner applies them to live `Orders` before queueing protocol commands.

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
let lifecycle_events = client.drain_lifecycle_events();
let domain_events = client.drain_events();
let revision = client.snapshot_revision();
let snapshot = client.snapshot();
let subscriptions = client.active_subscriptions();
let server_info = client.server_info();
let auth_info = client.auth_info();
```

Regular applications observe connection and domain state through `MoonClient`
events, lifecycle events, immutable snapshots, active subscription state, and
request-result events. Low-level packet counters/PMTU/sliced internals belong
to diagnostics and FireTest, not to the terminal runtime model. For multiple
independent server connections, create one `MoonClient` per server.

## Shutdown

```rust
client.disconnect()?;
client.wait_finished()?;
```

`disconnect` schedules final shutdown and returns immediately. `wait_finished`
is the explicit shutdown barrier; use it during application exit or scripts when
you need to wait until the runtime thread has actually exited. Disconnect also
sets the internal shutdown flag that interrupts startup/protocol waits, so
`wait_finished` is not supposed to sit through the full connect/init timeout.
Dropping `MoonClient` performs the same shutdown best-effort. To reconnect after
final shutdown, create a new `MoonClient`.
