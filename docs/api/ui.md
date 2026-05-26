# UI Channel

The UI channel carries bot settings and UI-originated control commands:
settings snapshots, strategy start/stop, market-maker subscription, version
update control, leverage management, trigger management, DEX/spot switching, and
arb activation notifications.

Applications normally receive UI updates through `Event::Settings` and send UI
commands through `Client::ui_*` wrappers.

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

For a one-shot fetch, use `request_client_settings`. It sends
a settings refresh request, keeps the UDP loop running, and returns the next
`ClientSettingsCommand` snapshot applied by `EventDispatcher`. The returned
snapshot may have the same internal command UID as the previous snapshot; the
API guarantee is a newly received/applied settings packet, not UID monotonicity.
Because the settings refresh is fire-and-forget, the helper may reissue it while
the timeout is still open:

```rust
let settings = client.request_client_settings(
    &mut dispatcher,
    std::time::Duration::from_secs(12),
)?;
println!("xSell={}", settings.x_sell);
```

The lower-level event path remains useful for long-running UI screens that want
to react to every later settings update.

## Sending UI Commands

Prefer `Client` methods when the caller owns the client thread:

```rust
client.ui_settings_request();
client.ui_send_settings(&settings);
client.ui_strat_start_stop(true);
client.ui_mm_subscribe(true);
client.ui_update_version("", true);            // release update button
client.ui_update_version("MoonBot-7", false);  // test/beta version name
client.ui_lev_manage(&lev_manage);
client.ui_trigger_manage(action, all_markets, &markets, &keys);
client.ui_reset_profit(kind);
client.ui_arb_activate_notify(arb_valid_until);
client.ui_switch_dex("Main");
client.ui_switch_spot(0);
```

`ui_lev_manage` sends the library-supported leverage-management format.
`LevManage::cmd_ver` is kept for parsing received snapshots and does not change
the outgoing command produced by the high-level wrapper.

For strategy start/stop with an explicit checked-state delta, normal
active-library code should send through `EventDispatcher`, not by hand-building
the command items. The dispatcher owns strategy checked-state and sends only
items whose checked value changed:

```rust
dispatcher.set_strategy_checked(strategy_id, true);
dispatcher.ui_strat_start_stop_v2(&client, true);
```

Regular UI code sends commands through `MoonClient`:

```rust
client.ui_mm_subscribe(true)?;
client.ui_update_version("", true)?;
client.ui_switch_dex("Main")?;
```

`ui_mm_subscribe` is registry-aware: it records the latest MM-orders value in
the reconnect registry immediately. Before Init it sends nothing; the one-time
Init uses the latest registry value for the post-init MM-orders subscription
step. After Init, `ui_mm_subscribe` queues the command for sending, and
reconnect restores the latest MM-orders intent automatically. It does not
rewrite the stored `subscribe_all_trades(want_mm)` value; all-trades
subscription and MM-order display are two separate user intents.

### Version Update

`ui_update_version(version_name, is_release)` asks the server to start the
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

`ui_update_version`, `ui_switch_dex`, and `ui_switch_spot` are typed UI domain
commands and are gated by Init. After Init they also mark
`ServerUpdateSent` inside `Client`. The next `run_init_sequence` consumes that
marker and uses the update-aware BaseCheck retry path: `34 * 300ms` auth wait,
then one BaseCheck plus up to 10 retries with `2000ms` pauses and the normal
12s response timeout per attempt. If a diagnostic tool sends the same payload
through lower-level APIs, call `client.mark_server_update_sent()` manually.

Low-level builders in `commands::ui` remain available for diagnostics and
compatibility tools, but normal applications should use the typed methods above.

Inbound listing notifications are internal to the active library. They force an
immediate listing refresh, but they are not emitted as settings events. User
code gets the listing signal from the market domain only after the refreshed
market list actually inserts new markets:
`Event::Markets(MarketsEvent::NewMarketsAdded { names })`.

Low-level `UICommand::ClientSettings` stores the settings snapshot as
`Box<ClientSettingsCommand>`. This keeps the common `UICommand` envelope small
when events move through queues; application code can still use normal deref
access in matches:

```rust
if let moonproto::commands::ui::UICommand::ClientSettings(settings) = cmd {
    println!("xSell={}", settings.x_sell);
}
```

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
| `ui_send_settings` | Only the latest pending settings snapshot is kept. |
| `ui_lev_manage` | Only the latest pending leverage-management snapshot is kept. |
| `ui_mm_subscribe` | Rapid live subscribe/unsubscribe commands are queued as distinct commands. The reconnect registry still remembers the latest desired value. |
| `ui_switch_dex` | Switch commands are queued as distinct commands. |
| `ui_switch_spot` | Switch commands are queued as distinct commands. |

This matters for UI code that can emit rapid changes: settings and leverage are
"latest wins"; MM-orders, DEX, and Spot commands preserve the user's command
sequence.

## Low-Level Parsing

```rust
use moonproto::commands::ui::UICommand;

let command = UICommand::parse(payload).expect("bad UI payload");
```

`EventDispatcher` performs this parsing and applies `SettingsState`
automatically for known, supported UI commands.

`UICommand::Skipped { .. }` and `UICommand::Unknown { .. }` are diagnostic
variants for forward compatibility. The active dispatcher ignores them: they do
not mutate `SettingsState` and do not emit `Event::Settings`.

`ClientSettingsCommand` is tolerant to old append-only settings snapshots:
missing optional tail fields keep the current settings fallback when possible.
Malformed UTF-8 strings remain a parse-failure path.
