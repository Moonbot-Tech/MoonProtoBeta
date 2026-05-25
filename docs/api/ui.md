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
`ClientSettingsCommand` snapshot applied by `EventDispatcher`. The returned
snapshot may have the same command UID as the previous snapshot; the protocol
guarantee is a newly received/applied settings packet, not UID monotonicity.
Because `TSettingsRequest` is a fire-and-forget UI command, the helper may
reissue it while the timeout is still open:

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
client.ui_new_market_notify();
client.ui_lev_manage(&lev_manage);
client.ui_trigger_manage(action, all_markets, &markets, &keys);
client.ui_reset_profit(kind);
client.ui_arb_activate_notify(arb_valid_until);
client.ui_switch_dex("Main");
client.ui_switch_spot(0);
```

`ui_lev_manage` follows Delphi `TLevManageCommand.StoreToStream`: the outgoing
wire version byte is always `LevCmdVer = 1`. `LevManage::cmd_ver` is kept for
low-level parsing of received payloads and does not change the outgoing packet.

The low-level builders for `TStratStartStopCommandV2`, `TEmuTradesCommand`, and
`TTriggerManageCommand` mirror Delphi `Word Count` serialization: the count is
written as the low 16 bits, and only that declared number of elements is written
to the packet body.

For Delphi `TStratStartStopCommandV2`, normal active-library code should send
through `EventDispatcher`, not by hand-building `checked_items`. The dispatcher
owns strategy checked-state and builds `Items` as Delphi does:
`CheckedDirect != PrevChecked`.

```rust
dispatcher.set_strategy_checked(strategy_id, true);
dispatcher.ui_strat_start_stop_v2(&client, true);
```

When the UI sends commands from another thread while the client loop is running,
clone `client.sender()` and call the same fire-and-forget UI wrappers on
`ClientSender`:

```rust
let sender = client.sender();
std::thread::spawn(move || {
    sender.ui_mm_subscribe(true);
    sender.ui_update_version("", true);
    sender.ui_switch_dex("Main");
});
```

`ui_mm_subscribe` is registry-aware: it records the latest MM-orders value in
the reconnect registry immediately. Before Init it sends no wire command; the
one-time Init uses the latest registry value for the post-init
`TMMOrdersSubscribeCommand`. After Init, `ui_mm_subscribe` appends the wire
command to the High send queue, and reconnect restores the latest MM-orders
intent automatically. It does not rewrite the stored
`subscribe_all_trades(want_mm)` value; Delphi has two separate callers that can
write the same server MM-orders flag.

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

`ui_update_version`, `ui_switch_dex`, and `ui_switch_spot` are typed UI domain
commands and are gated by Init. After Init they also mark the Delphi
`ServerUpdateSent` state inside `Client`. The next `run_init_sequence` will
consume that marker and use the Delphi BaseCheck update retry path:
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

The high-level wrappers set the same UKey behavior as the Delphi command
objects:

| Command | UKey behavior |
|---|---|
| `ui_send_settings` | `UK_BaseUISettings` with fixed `UKey.UID = 1`; only the latest pending settings snapshot is kept. |
| `ui_lev_manage` | `UK_LevManageSettings` with fixed `UKey.UID = 1`; only the latest pending leverage-management snapshot is kept. |
| `ui_mm_subscribe` | `UK_TurnMMDetection` with the command's fresh wire UID; live rapid subscribe/unsubscribe commands do not collapse into one local slot. The client registry still remembers the latest desired value for reconnect restore. |
| `ui_switch_dex` | `UK_DexSwitch` with the command's fresh wire UID; switch commands do not collapse into one local slot. |
| `ui_switch_spot` | `UK_SpotSwitch` with the command's fresh wire UID; switch commands do not collapse into one local slot. |

This distinction matters because Delphi only overrides `SetUKey` for settings
and leverage-management snapshots here. `MMOrders`, DEX, and Spot commands carry
a unique command UID even though they have a non-`None` UKey kind.

## Low-Level Parsing

```rust
use moonproto::commands::ui::UICommand;

let command = UICommand::parse(payload).expect("bad UI payload");
```

`EventDispatcher` performs this parsing and applies `SettingsState`
automatically for known, supported UI commands.

`UICommand::Skipped { cmd_id, uid, ver }` means the command header is valid but
`ver` is newer than the current protocol command version. This mirrors Delphi
registry `FSkipped`. `UICommand::Unknown { cmd_id, uid }` means the version is
supported but no UI command class is registered for that `cmd_id`. The active
dispatcher ignores both variants: they do not mutate `SettingsState` and do not
emit `Event::Settings`, matching Delphi client behavior.

Counted low-level UI arrays mirror Delphi `TMemoryStream.Read` behavior. For
`StratStartStopV2.items`, `EmuTrades.points`, and `TriggerManage.markets/keys`,
the parser keeps the declared `Count`; if the payload tail is truncated, missing
bytes are zero/`false` and partial little-endian scalar bytes are preserved.

The same zero-tail rule applies to fixed scalar UI command bodies such as
`StratStartStop`, `MMOrdersSubscribe`, `ResetProfit`, `ArbActivateNotify`,
`SwitchDex`, and `SwitchSpot`. Malformed UTF-8 string lengths/content remain a
parse-failure path because Delphi uses `ReadBuffer` in the UTF-8 string helper.

`ClientSettingsCommand` follows the same Delphi split. UTF-8 strings must be
complete. Fixed fields after a valid string are soft-read with
`TMemoryStream.Read`: missing `UseCoinsBlackList`/`TempBLCount` bytes decode as
false/zero, `TempBLTimes` can zero-tail after a complete symbol string, and
append-only fields keep the current settings fallback unless at least one byte of
that field is present. For `VolDropLevel`, a partial read overwrites the low
little-endian bytes and preserves the high bytes from the fallback value.
