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
`TClientSettingsCommand`; Active Lib applies it, emits
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
use moonproto::{ClientSettingsCommand, SpotMarketKind};

client.settings().refresh()?;
client.settings().send(ClientSettingsCommand::default())?;
client.settings().set_mm_orders_subscription(true)?;
client.settings().request_version_update("", true)?;            // release update button
client.settings().request_version_update("MoonBot-7", false)?;  // test/beta version name
client.settings().switch_dex("Main")?;
client.settings().switch_spot(SpotMarketKind::Crypto)?;
```

Low-level protocol tools can still build the same wire payloads through
`commands::ui`, but normal application code should use the high-level method
names above.

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

`request_version_update(version_name, is_release)` asks the server to start the
MoonBot update flow. It sends the version name and whether the release channel
should be used.

This is a remote-update command, not a passive "current client version"
notification. The two normal UI uses are:

- update button: sends `VersionName=""`, `IsRelease=true`;
- beta/test install command: sends a version name such as `MoonBot-7` with
  `IsRelease=false` after application-side validation/normalization.

When the server accepts this command, it also broadcasts the update request back
to connected clients. The Rust library does not download or restart the
application; it sends/parses the command and exposes an inbound request as
`SettingsEvent::VersionUpdate`.

`request_version_update`, `switch_dex`, and `switch_spot` are typed UI domain
commands and are gated by Init. Low-level diagnostic tools that send the same
payload by hand are responsible for preserving the matching `ServerUpdateSent`
side effect.

Low-level builders in `commands::ui` remain available for diagnostics and
compatibility tools, but normal applications should use the typed methods above.

Inbound listing notifications are internal to the active library. They force an
immediate listing refresh, but they are not emitted as settings events. User
code gets the listing signal from the market domain only after the refreshed
market list actually inserts new markets:
`Event::Markets(MarketsEvent::NewMarketsAdded { names })`.

Low-level `UICommand::ClientSettings` stores the settings snapshot as
`Box<ClientSettingsCommand>` to keep the internal command envelope small.
Normal application code does not parse `UICommand` directly; it reads the
applied `SettingsState`.

## ClientSettings

`ClientSettingsCommand` is the full settings snapshot. It contains sell settings,
stop/trailing/take-profit settings, iceberg flags, order-signing flag, coin
blacklist fields, manual strategy id, stop-market settings, AutoStart blobs,
hotkey sell prices, multi-order join mode, and `ArbConfigCompact`.

AutoStart blobs are opaque byte arrays with fixed public sizes:

```rust
use moonproto::commands::ui::{AS_CFG_SIZE, AS_CFG2_SIZE};
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

Inside the owned `MoonClient` runtime, the low-level dispatcher parses UI
payloads and applies `SettingsState` automatically for known, supported UI
commands. Applications normally read the resulting snapshot/events and do not
instantiate the dispatcher themselves.

`UICommand::Skipped { .. }` and `UICommand::Unknown { .. }` are diagnostic
variants for forward compatibility. The active dispatcher ignores them: they do
not mutate `SettingsState` and do not emit `Event::Settings`.

`ClientSettingsCommand` is tolerant to old append-only settings snapshots:
missing optional tail fields keep the current settings fallback when possible.
Malformed UTF-8 strings remain a parse-failure path.
