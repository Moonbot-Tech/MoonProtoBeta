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

NTP follows the MoonBot core's process-global model: all clients share one corrected time
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
    // Use TradesAndMarketMakers when the UI needs heat-map MM-orders with
    // HyperLiquid taker wallet addresses.
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

For serious GUI applications, the event bridge should be tied to the UI
framework, not to a custom feed loop. Use `MoonEventSink::callback` for
callback/event-loop frameworks, or `MoonEventSink::queue_with_waker` for
immediate-mode frameworks. A `drain_events() + sleep(...)` loop is only a
CLI/demo pattern; it is not a recommended terminal architecture.

Command-line tools, scripts, and tests that do one-shot work after connect can
use `MoonClient::connect_blocking(cfg, connect, timeout)` instead: it blocks on a
single channel receive until `Ready` or failure (no busy polling). This is a
convenience for scripts, not the UI path — long-running applications use
`connect` and react to `LifecycleEvent::Ready`; UI work is gated by the async
ready state instead of blocking the caller.
Applications do not choose a finite protocol-loop duration; the session runs
until explicit `disconnect()` or drop. UI code reads typed events, lifecycle
events, and immutable snapshots:

`Ready` covers the mandatory init spine: authorization, BaseCheck/AuthCheck,
markets list/server-index map, initial price refresh, strategy schema, and the
post-init command flush. Strategy schema is
requested after AuthCheck and may be received while the market init requests are
still running; `Ready` is emitted only after the schema is applied. It does not
wait for the replies to the queued order/settings/balance/local-strategy resync,
retained 5m candles, CoinCard candles, transfer assets, or the first stream
packet; those arrive as normal domain events and snapshot updates.

```rust
if let Some(snapshot) = client.snapshot() {
    for order in snapshot.orders().iter() {
        println!("{order:?}");
    }
}
```

New order and market-level trade actions are user intents too. They are
marshalled into the runtime owner, which encodes the selected market in the
canonical v4 command:

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
if let Some(snapshot) = client.snapshot() {
    if let Some(market) = snapshot.markets().find("BTC") {
        let _ticket = client.account().set_leverage_for(&market, 20)?;
    }
}
client.account().set_hedge_mode(true)?;
client.account().cancel_all_orders()?;
```

User/API sends append directly to the client's unbounded priority send queues,
separate from accepted UDP packets and receive-decoded delivery. Typed domain
helpers are gated by Init:
before `domain_ready`, subscriptions update only the reconnect registry and
order/UI/strategy/balance wrappers queue no server command. After Init, the same
typed helpers append their Engine API/UI/domain commands to the send queues. The
public guarantee is no local capacity cap: dense incoming streams do not drop
queued user commands or Engine API requests.

The regular application model is `MoonClient`: one runtime owner, event sink,
snapshots, and fire-and-forget user intents. Public code should not model the
session as "run the protocol for N seconds".

## Diagnostics And FireTest

Production applications should build MoonProto without `--features diagnostics`.
The normal Active Lib surface does not expose packet-loss emulation, per-packet
CPU counters, or blackhole hooks.

The `diagnostics` feature exists for live health tests and protocol audits:
FireTest uses it to emulate packet loss, attribute sliced-response recovery, and
check that receive/apply/enqueue paths stay bounded. These counters are passive;
they never change retry, ACK, reconnect, queueing, or drop decisions.

For day-to-day development use the test guide in `tests/README.md`. Low-level
wire tracing is a separate `diagnostic-trace` feature and is intended only for
investigating concrete packet-flow bugs.

## Connection Setup

For regular applications, `MoonClient::connect` owns the setup path:

```rust
let client = MoonClient::connect(cfg, ConnectConfig::new(init))?;
```

It starts the runtime immediately and reports completion as
`LifecycleEvent::Ready`; startup errors arrive as `LifecycleEvent::ConnectFailed`.

The mandatory Ready barrier is authorization, BaseCheck, AuthCheck, the markets
list (which also builds the initial server-index map), price refresh, strategy
schema, and the post-init send flush. Before publishing `Ready`, the runtime also
queues the order snapshot, client strategy snapshot, settings, MM-orders, and
balance resync intents. Their replies are normal domain updates and may arrive
after `Ready`. `InitConfig` adds local strategies, optional stream
subscriptions, and timing.

The runtime keeps the client loop running while it waits for the connection and
for each Engine API response on the mandatory spine. It applies state in the
runtime owner, publishes snapshots, and fills `client.server_info()` after
`BaseCheck` and `client.auth_info()` after a successful `AuthCheck`.

Init is a one-time step for a `MoonClient` session. After it succeeds, do not
start a second init just because the UDP transport reconnected; the library
maintains the user-requested active-lib state for that session.

Cold init does not send a separate `GetMarketsIndexes`: `GetMarketsList` builds
the server-index map from the server list order and stores the current
`PeerAppToken`. After reconnect/server-token changes, the library
refreshes `GetMarketsIndexes` before any `UpdateMarketsList` price refresh that
depends on server `mIndex` values. Init also requests the live strategy schema
after AuthCheck. The decoded schema is stored in the active strategy state and
contains strategy kinds, fields, TypeIDs, UI kind, picklists, visibility, and
chapter/layout markers. This is agreed active-library behavior: clients use the
live server schema for strategy UI metadata and typed strategy snapshot writes
instead of a hardcoded Rust copy of core strategy fields/defaults. Only this
schema request/response is allowed through the pre-Init Strat gate; regular
strategy commands remain closed until `domain_ready`.
Periodic market refresh starts only after init opens the domain gate, so
BaseCheck/AuthCheck are not delayed by early background refresh traffic.
Critical BaseCheck/AuthCheck waits use the MoonBot core default timeout:
12 seconds per Engine API request. Mandatory init
step timeouts/errors fail init and leave the domain gate closed.

`AuthCheck` follows the MoonBot core result ordering: a successful server response opens
the next init step even if the optional account payload cannot be parsed. When
the payload is valid, `client.auth_info()` contains
the parsed account metadata (`account_id`, `btc_address`, sub-account flag,
transfer payload limit, and Hyperliquid DEX tail). When a successful AuthCheck
payload is malformed, `auth_check_ok` remains true, `auth_info` stays `None`,
and an internal non-fatal parse note is recorded.

If the first BaseCheck/AuthCheck block fails, init follows the MoonBot core
startup retry path:
wait 200 ms, send one more BaseCheck, then send AuthCheck again. The retry
branch's final gate is the second AuthCheck result; the second BaseCheck still
updates `client.server_info()` if it succeeds.

`BaseCheck` retry follows the MoonBot core behavior. A normal init sends one BaseCheck
request. If a version/switch action marked `ServerUpdateSent` before init, the
init spine consumes that marker, waits up to `34 * 300ms` for
`AuthDone`, sends BaseCheck once, and if it still fails retries it 10 times with
`2000ms` pauses. The high-level UI wrappers that trigger server-update behavior
set this marker automatically:
`settings().request_release_update`, `settings().request_version_update`,
`settings().switch_dex`, and
`settings().switch_spot`.

Before init completion, ordinary mutable domain pushes are ignored. The
startup-safe exceptions are strategy schema, strategy snapshot requests/runtime
state, core runtime/license state, and news/history payloads. A pre-init strategy
snapshot request is latched and answered after schema/state initialization;
startup-safe state/news payloads are applied immediately and can precede
`Ready`. Engine API responses and transport service packets are outside this
gate because Init itself depends on Engine API. Once the mandatory Engine API
block succeeds, the helper opens the general domain gate, then sends the
post-init refresh set: order snapshot request,
full client strategy snapshot from the runtime-owned local strategy list,
settings request, MM-orders subscription state, and balance refresh request.
When the server later asks for the client's current strategy list, the runtime
replies from the same owned list; an empty list is a valid non-empty serializer
payload. Terminal code does not build or send this reply manually. Set
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
Balance bootstrap uses the normal post-init balance refresh intent. It follows
the MoonBot core behavior where the initial balance API call can report success
without carrying a serialized balance snapshot; the retained per-market balance
state is then updated by the regular balance stream.

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

Canonical v4 order commands use market names and server order UIDs rather than
caller-built route records. The legacy `penalty` helper is the one remaining
trade action that derives `TradeCtx` from `base_currency_code` and
`exchange_code` learned during Init/BaseCheck. `new_order` also returns
`NewOrderTicket`.
`ticket.client_order_id` is an outbound/local label only: the server does not
echo it in the typed order stream, and the created order is identified by the
server `uid`. Normal order tables should read created orders from
`snapshot().orders()` and key them by `Order::uid`; do not attach fills, cancels,
or PnL to an optimistic row by `client_order_id`.

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
        client.account().set_leverage_for(&market, 20)?;
        client.account().confirm_risk_limit_for(&market)?;
    }
}
client.settings().refresh()?; // async; read Event::Settings + snapshot().settings()
client.account().set_hedge_mode(true)?;
```

`candles().request_coin_card_for(&market, kind)` is intentionally non-blocking:
the request marks the selected market as needing deep history, and the runtime
publishes completion after the background path fills the retained CoinCard
candles. In Rust, completion arrives as
`Event::CoinCardCandles` and the rows are readable through
`snapshot().coin_card_candles_for(&market, kind)`. The string-keyed
`request_coin_card(market, kind)` variant is a convenience for scripts/tools.
The same selected-market rule applies to account actions such as
`set_leverage_for`, `change_position_type_for`, and `confirm_risk_limit_for`;
string-keyed variants remain for scripts/tools that do not keep
`MarketHandle`s.

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
                println!(
                    "sell target = {:.4}%",
                    settings.effective_take_profit_percent()
                );
            }
        }
    }
}
```

If an application already has local UI settings before connecting, pass them in
the active-library init/settings path. This preserves MoonBot soft-read behavior
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
client.streams().subscribe_candles(["BTCUSDT"], moonproto::DeepHistoryKind::Hour4)?;
client.streams().set_deltas_by_trades(false)?;
client.streams().unsubscribe_orderbook("BTCUSDT")?;
client.streams().unsubscribe_orderbooks(["ETHUSDT", "SOLUSDT"])?;
client.streams().unsubscribe_candles(["BTCUSDT"])?;
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
All-trades reconnect follows the MoonBot stream-recovery gate: until a
`TradesStream` packet is seen with the current `ServerToken`, the library sends
`UnsubscribeAllTrades`, waits 100 ms, then sends `SubscribeAllTrades`, retrying
that sequence no more often than once per 5000 ms. A queued
`SubscribeAllTrades` request arms the same gate, and a successful response
refreshes it, so the active library waits for the first trades packet before
deciding that the stream needs another reconnect cycle.
Orderbook reconnect follows the MoonBot orderbook recovery gate: until a
successful full-registry `SubscribeOrderBook` response confirms the current
`ServerToken`, the library repeats the batched subscribe no more often than
once per 5000 ms. The retry resets local orderbook sequence/cache state but
keeps the last visible snapshot levels.
Live-candle subscriptions retain a separate timeframe for every market.
Calling `subscribe_candles` again changes only the listed markets; it does not
require an unsubscribe and does not alter other candle subscriptions. The core
broadcasts the effective per-market choice as
`Event::CandleTimeframeState`, including changes made by another client.
`active_subscriptions().live_candle_timeframes` is the current read model.
Hard reconnect groups the retained markets by timeframe and replays every
group before confirming the candle subscription watermark.
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

Typed command methods append into the same unbounded priority send queues
after Init. Before Init, subscriptions update only the reconnect registry and
other domain commands queue nothing. Neither path has a local capacity cap.

Order actions with local stateful effects, such as replace/cancel/panic,
stop/VStop, and immune clicks, are intents on `client.orders()`. The runtime
owner applies them to live `Orders` before queueing protocol commands.

## Periodic Refresh

`ClientConfig.refresh` controls automatic background Engine API requests.
The default matches the MoonBot active-client cadence, but refresh ticks are
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
