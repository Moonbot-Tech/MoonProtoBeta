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

client.run_with_dispatcher_state(duration, &mut dispatcher, Box::new(|event, state| {
    if let Event::Settings(settings_event) = event {
        match settings_event {
            SettingsEvent::ClientSettingsUpdated => {
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
}));
```

`SettingsState` stores the latest settings snapshot and small derived fields:
current DEX, current spot selector, MM subscription status, leverage management,
and arb validity time.

## Requesting Current Settings

For a one-shot fetch, use `request_client_settings`. It sends
`TSettingsRequest`, keeps the UDP loop running, and returns the next
`ClientSettingsCommand` snapshot applied by `EventDispatcher`:

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

Prefer `Client` methods:

```rust
client.ui_settings_request();
client.ui_send_settings(&settings);
client.ui_strat_start_stop(true);
client.ui_strat_start_stop_v2(true, &checked_items);
client.ui_mm_subscribe(true);
client.ui_update_version("", true);            // release update button
client.ui_update_version("MoonBot-7", false);  // test/beta version name
client.ui_new_market_notify();
client.ui_lev_manage(&lev_manage);
client.ui_trigger_manage(action, all_markets, &markets, &keys);
client.ui_reset_profit(kind);
client.ui_arb_activate_notify(arb_valid_until);
client.ui_switch_dex("Main");
client.ui_switch_spot(0);
```

`ui_mm_subscribe` records the latest MM-orders intent in the client registry.
Before Init, reconnect does not replay that flag. After the one-time Init
completes, reconnect restores the latest MM-orders intent automatically.

### Version Update

`ui_update_version(version_name, is_release)` is the Rust wrapper for Delphi
`TUpdateVersionCommand` (UI CmdId=6, High). It sends two fields:
`VersionName: string` and `IsRelease: bool`.

This is a remote-update command, not a passive "current client version"
notification. Delphi uses it in two places:

- update button: sends `VersionName=""`, `IsRelease=true`;
- beta/test install command: sends a version name such as `MoonBot-7` with
  `IsRelease=false` after Delphi-side validation/normalization.

On the Delphi server, receiving this command calls `HandleRemoteUpdateCommand`
and broadcasts the same `TUpdateVersionCommand` back to clients. On a Delphi
client, receiving it queues `HandleRemoteUpdateCommand`, which starts the local
updater flow. The Rust library does not download or restart the application; it
only sends/parses the wire command and exposes the inbound command as a
`SettingsEvent::VersionUpdate`.

`ui_update_version`, `ui_switch_dex`, and `ui_switch_spot` also mark the
Delphi `ServerUpdateSent` state inside `Client`. The next `run_init_sequence`
will consume that marker and use the Delphi BaseCheck update retry path:
`34 * 300ms` auth wait, then one BaseCheck plus up to 10 retries with `2000ms`
pauses and the normal 12s `SendAndWait` timeout per attempt. If a tool sends the
same raw UI payload through lower-level APIs, call
`client.mark_server_update_sent()` manually.

Low-level builders in `commands::ui` remain available for tools that need raw
payloads, but normal applications should not call `send_cmd` directly.

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

## Unique Keys

The high-level wrappers set the correct UKey behavior internally:

| Command | UKey behavior |
|---|---|
| `ui_send_settings` | `UK_BaseUISettings` |
| `ui_mm_subscribe` | `UK_TurnMMDetection` |
| `ui_lev_manage` | `UK_LevManageSettings` |
| `ui_switch_dex` | `UK_DexSwitch` |
| `ui_switch_spot` | `UK_SpotSwitch` |

Rapid repeated sends for the same UKey collapse to the latest pending intent.

## Low-Level Parsing

```rust
use moonproto::commands::ui::UICommand;

let command = UICommand::parse(payload).expect("bad UI payload");
```

`EventDispatcher` performs this parsing and applies `SettingsState`
automatically.
