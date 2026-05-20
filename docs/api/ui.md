# UI Channel

The UI channel carries bot settings and UI-originated control commands:
settings snapshots, strategy start/stop, market-maker subscription, version
notification, leverage management, trigger management, DEX/spot switching, and
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

## Sending UI Commands

Prefer `Client` methods:

```rust
client.ui_settings_request();
client.ui_send_settings(&settings);
client.ui_strat_start_stop(true);
client.ui_strat_start_stop_v2(true, &checked_items);
client.ui_mm_subscribe(true);
client.ui_update_version("1.2.3", true);
client.ui_new_market_notify();
client.ui_lev_manage(&lev_manage);
client.ui_trigger_manage(action, all_markets, &markets, &keys);
client.ui_reset_profit(kind);
client.ui_arb_activate_notify(arb_valid_until);
client.ui_switch_dex("Main");
client.ui_switch_spot(0);
```

Low-level builders in `commands::ui` remain available for tools that need raw
payloads, but normal applications should not call `send_cmd` directly.

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
