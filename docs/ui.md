# UI Channel

The UI channel carries bot settings and UI-originated control commands:
settings snapshots, strategy start/stop, market-maker subscription, version
update control, leverage management, trigger management, DEX/spot switching, and
arb activation notifications.

Applications normally receive UI updates through `Event::Settings` and send
user intents through `MoonClient` settings/update/switch helpers.

## Receiving Settings

```rust
use moonproto::Event;
use moonproto::state::SettingsEvent;

for event in client.drain_events() {
    if let Event::Settings(settings_event) = event {
        match settings_event {
            SettingsEvent::ClientSettingsUpdated => {
                let Some(state) = client.snapshot() else { continue; };
                if let Some(settings) = &state.settings().client_settings {
                    redraw_settings(settings);
                }
            }
            SettingsEvent::DexSwitched(cmd) => select_dex(&cmd.dex_name),
            SettingsEvent::SpotSwitched(cmd) => select_spot(cmd.spot_index),
            SettingsEvent::ArbActivated(cmd) => show_arb_valid_until(cmd.arb_valid),
            _ => {}
        }
    }
}
```

`SettingsState` stores the latest settings snapshot and small derived fields:
current DEX, current spot selector, MM subscription status, leverage management,
and arb validity time.

## Requesting Current Settings

For UI code, use `client.settings().refresh()`. It queues a settings refresh
request and returns immediately. The server answers by sending a
full settings snapshot; Active Lib applies it, emits
`Event::Settings(SettingsEvent::ClientSettingsUpdated)`, and stores the latest
value in `snapshot().settings().client_settings`.

```rust
client.settings().refresh()?;

for event in client.drain_events() {
    if matches!(
        event,
        moonproto::Event::Settings(moonproto::state::SettingsEvent::ClientSettingsUpdated)
    ) {
        if let Some(settings) = client
            .snapshot()
            .and_then(|state| state.settings().client_settings.clone())
        {
            println!("xSell={}", settings.x_sell);
        }
    }
}
```

## Sending UI Commands

Regular applications send UI commands through `MoonClient`:

```rust
use moonproto::SpotMarketKind;

client.settings().refresh()?;
if let Some(mut settings) = client
    .snapshot()
    .and_then(|state| state.settings().client_settings.clone())
{
    settings.x_sell = 50;
    client.settings().send(settings)?;
}
client.settings().set_mm_orders_subscription(true)?;
client.settings().request_release_update()?;             // release update button
client.settings().request_version_update("MoonBot-7")?;  // test/beta version name
client.settings().switch_dex("Main")?;
client.settings().switch_spot(SpotMarketKind::Crypto)?;
client.settings().set_triggers_for_markets(["BTCUSDT", "ETHUSDT"], &[1, 7])?;
client.settings().clear_triggers_for_all(&[3])?;
```

`settings().send(...)` sends a full settings snapshot. Normal UI code edits the
latest snapshot received from the server; constructing a fresh
`ClientSettingsCommand::default()` is useful for tests/tools, not for an already
configured terminal session. Normal application code should use the high-level
method names above.

For strategy start/stop with an explicit checked-state delta, normal UI code
uses `MoonClient`. The runtime owns strategy checked-state and sends only items
whose checked value changed:

```rust
client.strategies().set_checked(strategy_id, true)?;
client.strategies().start()?;
```

`set_mm_orders_subscription` is registry-aware: it records the latest MM-orders
value in the reconnect registry immediately. Before Init it sends nothing; the
one-time Init uses the latest registry value for the post-init MM-orders
subscription step. After Init, it queues the command for sending, and reconnect
restores the latest MM-orders intent automatically. It does not rewrite the
stored `subscribe_all_trades(TradesStreamMode::...)` value; all-trades
subscription content and MM-order display are two separate user intents.

### Version Update

`request_release_update()` asks the server to start the normal release update
flow. `request_version_update(version_name)` asks for a named beta/test build.

This is a remote-update command, not a passive "current client version"
notification. The two normal UI uses are:

- update button: call `request_release_update()`;
- beta/test install command: call `request_version_update("MoonBot-7")` after
  application-side validation/normalization.

When the server accepts this command, it also broadcasts the update request back
to connected clients. The Rust library does not download or restart the
application; it sends/parses the command and exposes an inbound request as
`SettingsEvent::VersionUpdate`.

Version update, `switch_dex`, and `switch_spot` are typed UI domain commands
and are gated by Init. Low-level diagnostic tools that send the same payload by
hand are responsible for preserving the matching `ServerUpdateSent` side
effect.

Low-level builders remain internal diagnostics/compatibility machinery; normal
applications should use the typed methods above.

### Leverage Management

Leverage management is a separate settings snapshot, just like in MoonBot. A
terminal normally edits the latest retained leverage settings and sends the
whole snapshot back:

```rust
if let Some(mut lev) = client
    .snapshot()
    .and_then(|state| state.settings().lev_manage.clone())
{
    lev.auto_fix_lev = true;
    lev.fix_lev = 5;
    client.settings().manage_leverage(&lev)?;
}
```

The wire UID and command-version fields are not user input. The runtime writes a
fresh UID and Delphi's current leverage command version when it queues the
command.

### Arbitrage Activation

`notify_arb_activation(...)` is the MoonBot arb-valid-until notification path.
Incoming notifications update `snapshot().settings().arb_valid_until_time()` and
emit `SettingsEvent::ArbActivated`.

### Chart Trade Emulator

The chart emulator is a normal UI feature, matching MoonBot's pencil
`EmulateTrades` mode. Terminal code builds emulated trade points from drawn
chart points and sends them through `client.emulator()`. The caller uses a
market name or a retained `MarketHandle`; Active Lib resolves the current server
market index internally.

```rust
use moonproto::{DelphiTime, EmuPencilPoint};

let Some(state) = client.snapshot() else { return Ok(()); };
let Some(sol) = state.markets().get("SOLUSDT") else { return Ok(()); };

let base_time = DelphiTime::now();
let at = |seconds: f64| DelphiTime::from_days(base_time.as_days() + seconds / 86_400.0);
let points = [
    EmuPencilPoint::new(base_time, 142.10),
    EmuPencilPoint::new(at(0.75), 142.05),
    EmuPencilPoint::new(at(1.50), 142.22),
];

client
    .emulator()
    .send_pencil_prices_for_market(&sol, base_time, points)?;
```

`send_pencil_prices_for_market` follows the Delphi UI algorithm: it starts from
the market's current `LastAsk`, converts falling pencil points to sell ticks,
skips points outside the `0..=65535` millisecond command window, and ignores an
empty result. `EmuTradePoint::buy` / `EmuTradePoint::sell` remain available for
explicit low-level tick injection, but chart tools should usually pass
`EmuPencilPoint` values and let Active Lib encode the trade side.

### Trigger Management

Terminal code selects markets by name or by a retained `MarketHandle`; it should
not pass server `mIndex` values. The high-level trigger helpers resolve current
market indexes inside Active Lib when the command is queued:

```rust
client
    .settings()
    .set_triggers_for_markets(["BTCUSDT", "ETHUSDT"], &[1, 2, 3])?;

client
    .settings()
    .clear_triggers_for_markets(["SOLUSDT"], &[7])?;

client.settings().set_triggers_for_all(&[1])?;
client.settings().clear_triggers_for_all(&[1, 2])?;
```

The hidden raw-index helper exists only for protocol diagnostics and parity
tests.

Inbound listing notifications are internal to the active library. They force an
immediate listing refresh, but they are not emitted as settings events. User
code gets the listing signal from the market domain only after the refreshed
market list actually inserts new markets:
`Event::Markets(MarketsEvent::NewMarketsAdded { names })`.

Internally, `UICommand::ClientSettings` stores the settings snapshot as
`Box<ClientSettingsCommand>` to keep the command envelope small. Normal
application code does not parse `UICommand` directly; it reads the applied
`SettingsState`.

## ClientSettings

`ClientSettingsCommand` is the full settings snapshot. It contains sell settings,
stop/trailing/take-profit settings, iceberg flags, order-signing flag, coin
blacklist fields, manual strategy id, stop-market settings, AutoStart blobs,
hotkey sell prices, multi-order join mode, and `ArbConfigCompact`.

AutoStart blobs are opaque byte arrays with fixed public sizes:

```rust
use moonproto::{AS_CFG2_SIZE, AS_CFG_SIZE};
```

## Pending Deduplication

Some UI commands intentionally collapse older pending commands before they are
sent, while others always keep the latest user action as a distinct command:

| Command | Pending behavior |
|---|---|
| `send_settings` | Only the latest pending settings snapshot is kept. |
| leverage-management settings snapshot | Only the latest pending snapshot is kept. |
| `set_mm_orders_subscription` | Rapid live subscribe/unsubscribe commands are queued as distinct commands. The reconnect registry still remembers the latest desired value. |
| `switch_dex` | Switch commands are queued as distinct commands. |
| `switch_spot` | Switch commands are queued as distinct commands. |

This matters for UI code that can emit rapid changes: settings and leverage are
"latest wins"; MM-orders, DEX, and Spot commands preserve the user's command
sequence.

## Low-Level Parsing

Inside the owned `MoonClient` runtime, UI payloads are parsed and applied to
`SettingsState` automatically for known, supported UI commands. Applications
normally read the resulting snapshot/events and do not instantiate protocol
state machinery themselves.

`UICommand::Skipped { .. }` and `UICommand::Unknown { .. }` are diagnostic
variants for forward compatibility. The active runtime ignores them: they do
not mutate `SettingsState` and do not emit `Event::Settings`.

`ClientSettingsCommand` is tolerant to old append-only settings snapshots:
missing optional tail fields keep the current settings fallback when possible.
Malformed UTF-8 strings remain a parse-failure path.
