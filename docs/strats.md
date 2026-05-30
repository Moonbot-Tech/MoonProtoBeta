# Strategies

The strategy channel carries full strategy snapshots and compact updates:
delete, sell-price update, checked-state sync, and snapshot requests.

The active runtime maintains `StratsState` and emits `Event::Strat`. Snapshot
payloads are decoded automatically into both the lightweight `StrategyInfo`
state and full `StrategySnapshot` values. `last_server_epoch` advances only
after the snapshot serializer payload is decoded and applied successfully,
matching Delphi's `ApplyStratSnapshot` → `cfg.LocalStratEpoch := ServerEpoch`
order. A malformed snapshot is logged and is not reported as `SnapshotFull` /
`SnapshotPartial`.

Before init, user code gives the library its current local strategies through
`InitConfig::initial_strategies`. The runtime owns that list after that point:
Init sends it as the Delphi post-init `TStratSnapshot.CreateFromStrats(...)`,
the runtime answers server `TStratSnapshotRequest` automatically, and it
applies strategy snapshots/deletes/checked updates received from the server. If
user code provides an explicit empty list, the client has no local strategies;
the current server snapshot is still available through the same read API.
When the server asks for a client snapshot before Init is complete, the request
is remembered and answered during post-init resync after the strategy schema and
owned strategy state are ready.

Init also requests the live strategy schema with
`TStratSchemaRequest` and stores the decoded `TStratSchema` in
`StratsState`. This is agreed active-library behavior: Rust consumers read
strategy field metadata from the server instead of carrying a hardcoded copy of
Delphi `TStrategy` UI metadata. If the schema response is missing, malformed,
or cannot be decompressed, Init fails and the domain gate does not open.

Low-level strategy command parsing follows Delphi tail rules. Fixed fields in
`TStratSnapshot`, `TStratDelete`, and `TStratSellPriceUpdate` use
`TMemoryStream.Read` semantics after a valid header, so missing scalar bytes are
zero-filled. `TStratDelete.FolderPath` is a strict `ReadBuffer` string: if the
folder-path length/body is present but incomplete, the whole command is
rejected. For `TStratSchema` / `TStratSnapshot`, a declared data size larger
than the remaining bytes becomes an empty/malformed payload, matching Delphi's
`Data=nil` guard.

Inside the compressed `TStrategySerializer` payload, strategy string field
values are not `ReadBuffer` strings. Delphi reads the `Word` length, allocates a
`TBytes` of that exact length, and then calls `Stream.Read`; if the body is
short, the returned string keeps the available bytes and zero-filled tail. The
Rust parser mirrors that deterministic part and also treats skipped known-field
type mismatches like Delphi `SkipFieldByTypeID`: a truncated skipped value is
consumed up to EOF instead of rejecting the whole snapshot. Truncated serializer
dictionaries and incomplete scalar/header locals are still rejected by Rust,
because the exact Delphi effect can depend on uninitialized locals or stale
`NameBuf` bytes in malformed payloads.

## Reading Strategy State

```rust
use moonproto::Event;
use moonproto::state::StratEvent;

for event in client.drain_events() {
    if let Event::Strat(strat_event) = event {
        match strat_event {
            StratEvent::SnapshotFull { .. } => {
                let Some(state) = client.snapshot() else { continue; };
                println!("strategies={}", state.strategy_snapshot_vec().len());
                for strategy in state.strategy_snapshots() {
                    if let Some(name) = strategy.strategy_name() {
                        println!("{}: {}", strategy.strategy_id, name);
                    }
                }
            }
            StratEvent::Deleted {
                strategy_id,
                folder_path,
                strategy_deleted,
                folder_deleted,
            } => {
                if *strategy_deleted {
                    remove_strategy(*strategy_id);
                }
                if *folder_deleted {
                    remove_empty_folder(folder_path);
                }
            }
            StratEvent::CheckedSynced { changed, is_delta } => {
                println!("checked changed={changed} delta={is_delta}");
            }
            StratEvent::SnapshotRequested { .. } => {
                // Already answered by the dispatcher from its owned strategy list.
                // The event is emitted for UI/diagnostic awareness.
            }
            StratEvent::SchemaApplied { kind_count, field_count, .. } => {
                println!("strategy schema: kinds={kind_count} fields={field_count}");
            }
            _ => {}
        }
    }
}
```

`raw_data` is still present in snapshot events for diagnostics and custom
decoders, but normal applications should read `state.strategy_snapshot(...)` or
`state.strategy_snapshots()`. For logging without touching the raw bytes, use
`StratEvent::snapshot_server_epoch()` and `StratEvent::snapshot_raw_len()`.

## Strategy Schema

The schema is the decoded body of Delphi `StrategySchemaBuilder.BuildStrategySchemaBlob`.
It is sent by the server as `TStratSchema.Data`: raw DEFLATE bytes containing a
little-endian binary schema.

Public read API:

```rust
let Some(state) = client.snapshot() else { return; };
let schema = state
    .strats()
    .strategy_schema()
    .expect("schema is available after LifecycleEvent::Ready");

for kind in &schema.kinds {
    println!("kind {} {}", kind.ordinal, kind.name);
}

for field in &schema.fields {
    println!(
        "{} type={} ui={:?} visible_for={:?}",
        field.name,
        field.type_id.name(),
        field.ui_kind,
        field.visible_kind_ordinals
    );
}
```

`StrategySchema` exposes:

- `format_version`;
- `kinds`: strategy kind ordinal and server UI name;
- `fields`: field name, typed field kind, UI kind, default value when Delphi
  marked it non-zero, and visibility decoded to strategy-kind ordinals;
- `StrategyFieldLayout`: no layout marker, comment, filter class, or chapter
  class with its chapter name;
- `static_picklist`;
- `dynamic_picklist`: `UseHookStrategy` means local MoonHook strategies with an
  empty first item; `ComboStart` / `ComboEnd` mean all local strategies.

Use `field.visible_for_kind(raw_ordinal)` or
`field.visible_for_strategy_kind(kind)` for visibility checks. The internal
bitmask used by the serializer is not part of the public UI schema surface.

Schema TypeIDs use the same value model as strategy snapshots:

```rust
use moonproto::{
    StrategyDynamicPicklist, StrategyFieldLayout, StrategyFieldType,
    StrategyFieldUiKind, StrategySchema,
};
```

`StrategySchema::parse_compressed(data)` and `StrategySchema::parse_plain(data)`
are public for protocol tools, but normal clients should read the active
dispatcher state populated by Init.

## State

```rust
pub struct StrategyInfo {
    pub strategy_id: u64,
    pub strategy_ver: i32,
    pub last_date: u64,
    pub sell_price: f64,
    pub checked: bool,
    pub prev_checked: bool,
    pub folder_path: String,
}
```

`StrategyInfo` is a lightweight UI/index state. Full `TStrategy` fields are not
stored there; they are stored as `StrategySnapshot` values owned by the
dispatcher. `checked` is Delphi `CheckedDirect`; `prev_checked` is Delphi
`PrevChecked`. Checked deltas are pending while these fields differ and become
acknowledged only after server `TStratCheckedEcho` or `TStratCheckedSync`.
`sell_price` is copied from the decoded snapshot field `SellPrice` when that
field exists; incoming `TStratSellPriceUpdate` packets are not applied by the
active client because Delphi client has no receive branch for that command.
Incoming `TStratSnapshot` with `Full=true` does not delete local strategies
that are absent from the payload. Delphi keeps those strategies as local
"Own" entries; Rust keeps them in `StratsState` as well.

`TStratDelete` has two independent Delphi effects: delete `StrategyID` when it
is non-zero, then delete `FolderPath` when it names an existing empty non-root
folder. `StratEvent::Deleted` exposes both result flags. `strategy_deleted` and
`folder_deleted` tell which parts actually changed state; if both are false the
dispatcher emits `StratEvent::Ignored`.

Future-version strat commands, unknown strat command ids, incoming
`TStratSchemaRequest`, and incoming `TStratSellPriceUpdate` do not emit active
dispatcher events. Delphi turns those into a skipped/base command or has no
client-side branch, then frees the object without strategy side effects. The
low-level parser/state APIs still expose `StratCommand::Skipped`,
`StratCommand::Unknown`, and `StratEvent::Ignored` for explicit diagnostics.

## Active Predicates

`StrategySnapshot` exposes exact Delphi-compatible helpers for code that needs
to reason about active strategies without guessing that `checked == active`.
`is_active(mode)` mirrors `TStratForm.CheckActive` /
`bStartCheckedClick`: in `ActiveClient` mode a checked strategy is local-active
only when it cannot auto-buy and does not run detection on the kernel; in
`UsingMoonProto` mode the inverse side is active; in `Standalone` mode active is
just checked.

```rust
use moonproto::{StrategyActiveMode, StrategyKind};

let is_local = strategy.is_active(StrategyActiveMode::ActiveClient);
let kind = strategy.kind();

if kind == StrategyKind::NEW_LISTING && strategy.sell_from_asset() {
    println!("listing sell-from-asset strategy");
}
```

`StratsState` also exposes listing predicates:

```rust
let has_listing = dispatcher
    .strats()
    .has_listing_strategy(StrategyActiveMode::ActiveClient);

let needs_assets = state
    .strats()
    .has_listing_sell_strategy(StrategyActiveMode::ActiveClient, is_futures);
```

These are read helpers only. They do not make the active library send listing
automation requests by themselves.

```rust
use moonproto::StrategySnapshot;

let strategies: Vec<StrategySnapshot> = load_current_strategies();
let init = InitConfig {
    initial_strategies: Some(InitialStrategies::new(
        load_local_strategy_epoch(),
        strategies,
    )),
    ..Default::default()
};

let client = MoonClient::connect(cfg, ConnectConfig::new(init))?;
let all: Vec<StrategySnapshot> = client
    .snapshot()
    .map(|state| state.strategy_snapshot_vec())
    .unwrap_or_default();
```

The epoch passed to `InitialStrategies::new` is Delphi
`cfg.ServerStratEpoch` for this local client's strategy list. It is the value
written into outgoing `TStratSnapshot.ServerEpoch` for both post-init strategy
snapshot send and answers to `TStratSnapshotRequest`; it is not the remote
server epoch learned from incoming snapshots. If the application reloads its
whole local strategy list after `MoonClient::connect`, use
`client.strategies().send_snapshot_batch(strategies)`. The runtime updates the
library-owned local list and sends the Delphi `TStratSnapshot` batch from the
same schema that Init fetched from the server. The call queues intent and
returns immediately; server echo/update arrives later through `Event::Strat`.

## Strategy Fields

```rust
use moonproto::{field_names, FieldValue, StrategyFields};

let Some(state) = client.snapshot() else { return; };
for strategy in state.strategy_snapshots() {
    if let Some(name) = strategy.fields.get_string(field_names::STRATEGY_NAME) {
        println!("{}: {}", strategy.strategy_id, name);
    }
}
```

`StrategySnapshot.fields` is a `StrategyFields` container, not a standard
`HashMap`. It stores the decoded fields densely in received order, which avoids
hash work while parsing large snapshots. The reader path appends fields in the
schema serializer order; `insert` keeps replacement semantics for user-built
snapshots. Prefer typed getters and `field_names::*` constants for common fields
so UI code does not depend on unreviewed string literals. The public operations
are intentionally small and familiar:

```rust
let mut fields = StrategyFields::new();
fields.insert(field_names::STRATEGY_NAME, FieldValue::String("Local".to_string()));

if let Some(name) = fields.get_string(field_names::STRATEGY_NAME) {
    println!("{name}");
}

for (name, value) in fields.iter() {
    println!("{name} = {value:?}");
}
```

`FieldValue` variants:

```rust
Bool(bool)
Int32(i32)
Int64(i64)
Double(f64)
String(String)
Byte(u8)
Word(u16)
UInt32(u32)
UInt64(u64)
Single(f32)
```

Raw serializer parsers remain available for diagnostics and custom protocol
tools, but they are hidden from the normal API surface. Applications should use
decoded `StratsState` from `MoonClient::snapshot()`.

## Sending Strategy Commands

Regular applications use `client.strategies()`:

```rust
client.strategies().sell_price_update(strategy_id, sell_price)?;
client.strategies().delete(strategy_id, folder_path)?;
```

Do not send `TStratSnapshotRequest` from client code. It is a server-to-client
command in Delphi, and the Delphi server explicitly ignores it when received
from a client. The real flows are: post-init sends the current local strategy
list as `TStratSnapshot`, and later the server may send
`TStratSnapshotRequest`, which the dispatcher answers automatically.

`strat_sell_price_update` is the Delphi client-to-server command. The server
applies it to its local strategy if the strategy exists; the active client does
not treat the same command as a server-to-client state update.

Use the same handle for regular UI integration:

```rust
client.strategies().sell_price_update(strategy_id, sell_price)?;
client.strategies().set_checked(strategy_id, true)?;
client.strategies().send_checked_delta()?;
```

For normal active-library flow, pass the local list before init and let the
runtime answer server snapshot requests:

```rust
use moonproto::{InitConfig, InitialStrategies};

let init = InitConfig {
    initial_strategies: Some(InitialStrategies::new(
        load_local_strategy_epoch(),
        load_current_strategies(),
    )),
    ..Default::default()
};
```

Checked-state sends should also go through the active-library state. This
matches Delphi `TStrategies.GetCheckedDelta`: local UI changes update
`checked`, leave `prev_checked` untouched, and the outgoing delta contains only
items where `checked != prev_checked`.

```rust
client.strategies().set_checked(strategy_id, true)?;
client.strategies().send_checked_delta()?;
client.strategies().start()?;
```

`send_checked_delta` sends a checked-state delta only when the delta is
non-empty. `strategies().start()` always sends the start command after the
client's Init gate is open; the checked delta may be empty because the same
command also carries the start/stop action. Both helpers keep `prev_checked`
unchanged until the server confirms the checked-state change.

Low-level compatibility tools may still use raw checked-sync/start-stop and
snapshot helpers, but those helpers are hidden diagnostics. Regular
applications should prefer `MoonClient` helpers so the library-owned strategy
state stays authoritative. Checked-state echo messages are inbound only; client
code must not send them.

To replace the whole local strategy list after startup, use the same
active-library strategy handle:

```rust
client
    .strategies()
    .send_snapshot_batch(load_current_strategies())?;
```

This is still an Active Lib intent, not a raw protocol call: the runtime owns
the local list used for future `TStratSnapshotRequest` replies.

Strategy snapshot serialization mirrors Delphi `TStrategySerializer` lengths:
field-name and folder-path dictionary entries use a `Byte` length and write
only that declared number of UTF-8 bytes; string field values use a `Word`
length and write only that declared number of UTF-8 bytes. Strategy fields are
emitted in live-schema order, which is Delphi `TStrategy` public field
declaration order. Schema visibility is the Delphi `GetStrategyPropMask`
bitset, so fields hidden for the strategy kind are not written.

The typed writer applies the same field filter as Delphi `SaveStrategyToCompact`.
It writes only schema-known public `TStrategy` fields visible for the strategy
kind, only when the value has the schema TypeID, and skips values equal to the
schema default. Defaults come from Delphi `TStrategy.Create` through
`StrategySchemaBuilder`; runtime color defaults such as `SellOrderColor` and
`BuyOrderColor` are therefore not hardcoded in Rust.

When decoding a snapshot after schema is available, known Delphi strategy fields
also keep type checks through the same schema: if the incoming TypeID does not
match the schema/RTTI field type, the value is skipped instead of being exposed
as a wrongly typed field.

For advanced override replies, register a fresh snapshot provider on the
dispatcher:

```rust
use moonproto::{StrategySnapshot, StrategySnapshotReply};

let schema = dispatcher
    .strats()
    .strategy_schema()
    .expect("Init fetched TStratSchema")
    .clone();

dispatcher.set_strategy_snapshot_provider(move |_request_uid| {
    let strategies: Vec<StrategySnapshot> = load_current_strategies();
    let server_epoch = load_local_strategy_epoch();
    Some(StrategySnapshotReply::from_strategies(
        server_epoch,
        true,
        &schema,
        &strategies,
    ))
});
```

The provider must return current application-owned strategies. The dispatcher
falls back to its owned strategy list when the provider is absent or returns
`None`, using `local_strategy_epoch()` as outgoing `ServerEpoch`. If the server
requests a non-empty local snapshot before schema has arrived, the dispatcher
requests `TStratSchema` and sends the pending snapshot after `SchemaApplied`;
it does not use a stale Rust field table. A provider that returns
`StrategySnapshotReply::from_payload(..., Vec::new())` gets the same empty-list
normalization as the normal owned empty-strategy snapshot path.
