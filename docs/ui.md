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
            SettingsEvent::ArbActivated { arb_valid, .. } => show_arb_valid_until(arb_valid),
            SettingsEvent::VersionUpdate { version_name, is_release, .. } => {
                handle_remote_update(version_name, is_release);
            }
            SettingsEvent::LevManageUpdated => {
                let Some(state) = client.snapshot() else { continue; };
                if let Some(lev_manage) = &state.settings().lev_manage {
                    redraw_leverage_management(lev_manage);
                }
            }
            _ => {}
        }
    }
}
```

`SettingsState` stores the latest settings snapshot and small derived fields:
leverage management and arb validity time. Client-originated UI commands such as
MM-orders subscription, emulator ticks, trigger management, reset-profit, and
DEX/spot switching are sent through high-level handles; they are not inbound
settings state in the Delphi client receive path.

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
        if let Some(snapshot) = client.snapshot() {
            if let Some(settings) = &snapshot.settings().client_settings {
                println!("xSell={}", settings.x_sell);
            }
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

Leverage-management fields are UI controls, not protocol switches:

| Field | UI meaning |
|---|---|
| `auto_max_order` | Auto-calculate leverage from the configured maximum order size / market leverage brackets. |
| `auto_lev_up` | Allow automatic leverage increases; when off, automatic management only lowers leverage. |
| `auto_isolated` | Force isolated margin where supported. |
| `auto_cross` | Force cross margin where supported. |
| `auto_fix_lev` / `fix_lev` | Force a fixed target leverage value. |
| `tlg_report` | Send leverage-change reports to Telegram. |
| `lev_control` | Text configuration used by MoonBot's leverage-control table/commands. |

### Arbitrage Activation

`notify_arb_activation(...)` is the MoonBot arb-valid-until notification path.
Incoming notifications update `snapshot().settings().arb_valid_until_time()` and
emit `SettingsEvent::ArbActivated { arb_valid }`, where `arb_valid` is a
`MoonTime`.

For UI gating, use `snapshot().settings().arb_is_active_now()` or
`arb_is_active_at(now)`. This matches MoonBot's `cfg.ArbActive :=
cfg.ArbValid > Now` meaning without exposing the raw Delphi-day double as the
terminal model.

### Chart Trade Emulator

The chart emulator is a normal UI feature, matching MoonBot's pencil
`EmulateTrades` mode. Terminal code builds emulated trade points from drawn
chart points and sends them through `client.emulator()`. The caller uses a
market name or a retained `MarketHandle`; Active Lib resolves the current server
market index internally.

```rust
use moonproto::{EmuPencilPoint, MoonTime};

let Some(state) = client.snapshot() else { return Ok(()); };
let Some(sol) = state.markets().get("SOLUSDT") else { return Ok(()); };

let base_time = MoonTime::now();
let at = |seconds: i64| MoonTime::from_unix_millis(base_time.unix_millis() + seconds * 1000);
let points = [
    EmuPencilPoint::new(base_time, 142.10),
    EmuPencilPoint::new(at(1), 142.05),
    EmuPencilPoint::new(at(2), 142.22),
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
blacklist fields, manual strategy id, stop-market settings, AutoStart settings,
hotkey sell prices, multi-order join mode, and `ArbConfigCompact`.

Normal UI code clones the retained snapshot, changes the fields behind one UI
page/control, and sends the whole snapshot back:

```rust
if let Some(current) = &snapshot.settings().client_settings {
    let mut settings = current.clone();
    settings.use_g_take_profit = true;
    settings.g_take_profit = 2.5;
    client.settings().send(settings)?;
}
```

Useful helpers:

| UI area | API |
|---|---|
| Main sell / scalp / fixed-sell display value | `effective_take_profit_percent()` |
| Six fixed sell buttons | `fixed_sell_presets()`, `fixed_sell_preset_percent(slot)`, `selected_fixed_sell_slot()`, `selected_fixed_sell_percent()`, `set_selected_fixed_sell_slot(...)`, `set_fixed_sell_preset_price(...)` |
| Temporary blacklist rows | `temp_blacklist_entries()` returns symbol + `Duration`; `set_temp_blacklist_entries(...)` accepts symbol + `Duration` |
| Multi-order sell join combo | `JoinSellKind`, `join_sell_mode()`, `set_join_sell_mode(...)` |
| AutoStart settings page | `auto_start_config()`, `set_auto_start_config(...)`, `update_auto_start_config(...)` |
| AutoStart recovery/session page | `auto_start_config2()`, `set_auto_start_config2(...)`, `update_auto_start_config2(...)` |

Common settings controls:

| UI meaning | Suggested control | Fields/helpers |
|---|---|---|
| Main take-profit target | numeric percent input/slider | `x_sell`, `x_tmode`, `x_sell_scalp`, `effective_take_profit_percent()` |
| Fixed-sell mode | segmented control or toggle | `fixed_sell_mode`, fixed-sell read/set helpers; helpers keep `fixed_sell_price` synchronized like MoonBot `UpdateFixedButtons` |
| Stop-loss / trailing / global take-profit | numeric percent inputs + enable checkbox | `price_drop_level`, `trailing_drop`, `use_g_take_profit`, `g_take_profit` |
| Panic-on-price-drop protection | checkbox | `panic_if_price_drop` |
| Emulator mode | checkbox/toggle | `emu_mode` |
| Buy/sell iceberg flags | two checkboxes | `buy_iceberg`, `sell_iceberg` |
| Signed order ids | checkbox | `sign_orders` |
| Global coin blacklist | multiline/token text editor + enable checkbox | `coins_black_list_text`, `use_coins_black_list` |
| Exclude blacklist from market delta | checkbox | `client.settings().set_exclude_blacklisted_markets_from_exchange_delta(...)` |
| Temporary coin blacklist | editable table | `temp_blacklist_entries()` / `set_temp_blacklist_entries(...)` with normal Rust `Duration` values |
| Manual strategy override | checkbox + strategy selector | `use_manual_strategy`, `manual_strategy_id` |
| Position/stop-market options | checkboxes + small numeric input | `free_position_check`, `use_stop_market`, `vol_drop_level` |
| Multi-order join-sell mode | combo/segmented control | `JoinSellKind`, `join_sell_mode()`, `set_join_sell_mode(...)` |
| Arbitrage display options | platform checklist + display toggles | `arb_config.is_wanted(...)`, `set_wanted(...)`, `wanted_platforms()`, plus display flags |
| AutoStart pages | settings sub-panels | `auto_start_config()`, `auto_start_config2()` typed views |

`fixed_sell_price` is not the best source for drawing the selected fixed-sell
button: MoonBot derives the active fixed price from `s_price[sb_num]` after
applying settings. Use the fixed-sell helpers for UI display and edits; setter
helpers keep `fixed_sell_price` synchronized.

`set_exclude_blacklisted_markets_from_exchange_delta` is local Active Lib
policy, not a `TClientSettingsCommand` wire field. It mirrors Delphi
`cfg.ExcludeBlackListDelta`: when enabled, markets whose currency appears in
`coins_black_list_text` are skipped from `MarketsState::global_deltas()`
exchange-delta aggregation.

AutoStart is stored on the wire as two fixed Delphi blobs, but Active Lib keeps
that detail inside the retained settings snapshot. Normal UI code edits typed
views:

```rust
if let Some(current) = &snapshot.settings().client_settings {
    let mut settings = current.clone();
    settings.update_auto_start_config(|auto| {
        auto.auto_start = true;
        auto.strategies_on = true;
        auto.auto_update = true;
    });
    settings.update_auto_start_config2(|auto| {
        auto.restart_on_market = true;
        auto.rs_hours = 6;
    });
    client.settings().send(settings)?;
}
```

The hidden wire blobs are preserved for exact roundtrip and version
compatibility when the typed views are written back.

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

Unknown/future UI subcommands are diagnostic forward-compatibility cases. The
active runtime ignores them: they do not mutate `SettingsState` and do not emit
`Event::Settings`.

`ClientSettingsCommand` is tolerant to old append-only settings snapshots:
missing optional tail fields keep the current settings fallback when possible.
Malformed UTF-8 strings remain a parse-failure path.
