# Strategies

The strategy channel carries full strategy snapshots and compact updates:
delete, sell-price update, checked-state sync, and snapshot requests.

`EventDispatcher` maintains `StratsState` and emits `Event::Strat`. Snapshot
payloads are decoded automatically into both the lightweight `StrategyInfo`
state and full `StrategySnapshot` values. `last_server_epoch` advances only
after the snapshot serializer payload is decoded and applied successfully,
matching Delphi's `ApplyStratSnapshot` → `cfg.LocalStratEpoch := ServerEpoch`
order. A malformed snapshot is logged and is not reported as `SnapshotFull` /
`SnapshotPartial`.

Before init, user code may give the library its current local strategies with
`EventDispatcher::set_local_strategies`. The dispatcher owns that list after
that point: `run_init_sequence` sends it as the Delphi post-init
`TStratSnapshot.CreateFromStrats(...)`, the dispatcher answers server
`TStratSnapshotRequest` automatically, and it applies strategy
snapshots/deletes/checked updates received from the server. If user code does
not provide local strategies, the list starts empty and is filled only by server
snapshots; the current server snapshot is still available through the same read
API.

`run_init_sequence` also requests the live strategy schema with
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
use moonproto::commands::strategy_serializer::FieldValue;

client.run_with_dispatcher_state(duration, &mut dispatcher, Box::new(|event, state| {
    if let Event::Strat(strat_event) = event {
        match strat_event {
            StratEvent::SnapshotFull { .. } => {
                println!("strategies={}", state.strategy_snapshot_vec().len());
                for strategy in state.strategy_snapshots() {
                    if let Some(FieldValue::String(name)) = strategy.fields.get("StrategyName") {
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
}));
```

`raw_data` is still present in snapshot events for diagnostics and custom
decoders, but normal applications should read `state.strategy_snapshot(...)` or
`state.strategy_snapshots()`.

## Strategy Schema

The schema is the decoded body of Delphi `StrategySchemaBuilder.BuildStrategySchemaBlob`.
It is sent by the server as `TStratSchema.Data`: raw DEFLATE bytes containing a
little-endian binary schema.

Public read API:

```rust
let schema = dispatcher
    .strats()
    .strategy_schema()
    .expect("connect_and_init completed, so schema is available");

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
- `fields`: field name, Delphi TypeID, typed field kind, raw flags, UI kind,
  default value when Delphi marked it non-zero, and visibility bitset decoded
  to strategy-kind ordinals;
- `StrategyFieldLayout`: no layout marker, comment, filter class, or chapter
  class with its chapter name;
- `static_picklist_raw` and `static_picklist`;
- `dynamic_picklist`: `UseHookStrategy` means local MoonHook strategies with an
  empty first item; `ComboStart` / `ComboEnd` mean all local strategies.

Schema TypeIDs use the same value model as strategy snapshots:

```rust
use moonproto::commands::strategy_schema::{
    StrategyDynamicPicklist, StrategyFieldLayout, StrategyFieldType, StrategyFieldUiKind,
    StrategySchema,
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

`StrategySnapshot` exposes exact Delphi helpers for code that needs to reason
about active strategies without guessing that `checked == active`.
`active_like_delphi(mode)` mirrors `TStratForm.CheckActive` /
`bStartCheckedClick`: in `ActiveClient` mode a checked strategy is local-active
only when it cannot auto-buy and does not run detection on the kernel; in
`UsingMoonProto` mode the inverse side is active; in `Standalone` mode active is
just checked.

```rust
use moonproto::commands::strategy_serializer::{
    StrategyActiveMode, StrategyKind, StrategySnapshot,
};

let is_local = strategy.active_like_delphi(StrategyActiveMode::ActiveClient);
let kind = strategy.kind_like_delphi();

if kind == StrategyKind::NEW_LISTING && strategy.sell_from_asset_like_delphi() {
    println!("listing sell-from-asset strategy");
}
```

`StratsState` also exposes Delphi listing predicates:

```rust
let has_listing = dispatcher
    .strats()
    .is_there_listing_strat_like_delphi(StrategyActiveMode::ActiveClient);

let needs_assets = dispatcher
    .strats()
    .is_there_listing_sell_like_delphi(StrategyActiveMode::ActiveClient, is_futures);
```

These are read helpers only. They do not make the active library send listing
automation requests by themselves.

```rust
use moonproto::commands::strategy_serializer::StrategySnapshot;

// Before connect_and_init:
let strategies: Vec<StrategySnapshot> = load_current_strategies();
dispatcher.set_local_strategy_epoch(load_local_strategy_epoch());
dispatcher.set_local_strategies(&strategies);

// Later, read the current active-library view:
let all: Vec<StrategySnapshot> = dispatcher.strategy_snapshot_vec();
let one = dispatcher.strategy_snapshot(strategy_id);
```

`set_local_strategy_epoch` is Delphi `cfg.ServerStratEpoch` for this local
client's strategy list. It is the value written into outgoing
`TStratSnapshot.ServerEpoch` for both post-init strategy snapshot send and
answers to `TStratSnapshotRequest`; it is not the remote server epoch learned
from incoming snapshots. When user code edits local strategies, call
`mark_local_strategies_changed()` to mirror Delphi `Inc(cfg.ServerStratEpoch)`.

## Snapshot Decoder

```rust
use moonproto::commands::strategy_serializer::{
    parse_strategy_batch, FieldValue, StrategyFields,
};

let batch = parse_strategy_batch(raw_data).expect("bad strategy snapshot");
for strategy in &batch.strategies {
    if let Some(FieldValue::String(name)) = strategy.fields.get("StrategyName") {
        println!("{}: {}", strategy.strategy_id, name);
    }
}
```

`StrategySnapshot.fields` is a `StrategyFields` container, not a standard
`HashMap`. It stores the decoded fields densely in wire/read order, which avoids
hash work while parsing large snapshots. The reader path appends fields in the
Delphi serializer order; `insert` keeps replacement semantics for user-built
snapshots. The public operations are intentionally small and familiar:

```rust
let mut fields = StrategyFields::new();
fields.insert("StrategyName", FieldValue::String("Local".to_string()));

if let Some(FieldValue::String(name)) = fields.get("StrategyName") {
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

## Sending Strategy Commands

Prefer `Client` wrappers when the caller owns the client thread:

```rust
client.strat_sell_price_update(strategy_id, sell_price);
client.strat_delete(strategy_id, folder_path);
```

`strat_snapshot_request()` exists only as an explicit protocol/testing tool.
Delphi server ignores `TStratSnapshotRequest` received from a client; normal
active-library code should not call it. The real flows are: post-init sends the
current local strategy list as `TStratSnapshot`, and later the server may send
`TStratSnapshotRequest`, which the dispatcher answers automatically.

`strat_sell_price_update` is the Delphi client-to-server command. The server
applies it to its local strategy if the strategy exists; the active client does
not treat the same command as a server-to-client state update.

Use `ClientSender` for the same fire-and-forget strategy commands from UI or
worker threads while `run_with_dispatcher` is active:

```rust
let sender = client.sender();
std::thread::spawn(move || {
    sender.strat_sell_price_update(strategy_id, sell_price);
});
```

For normal active-library flow, set the local list before init and let the
dispatcher answer server snapshot requests:

```rust
use moonproto::commands::strategy_serializer::StrategySnapshot;

let strategies: Vec<StrategySnapshot> = load_current_strategies();
dispatcher.set_local_strategy_epoch(load_local_strategy_epoch());
dispatcher.set_local_strategies(&strategies);
connect_and_init(&mut client, &mut dispatcher, connect_cfg)?;
```

Checked-state sends should also go through the active-library state. This
matches Delphi `TStrategies.GetCheckedDelta`: local UI changes update
`checked`, leave `prev_checked` untouched, and the outgoing delta contains only
items where `checked != prev_checked`.

```rust
dispatcher.set_strategy_checked(strategy_id, true);
let pending = dispatcher.strategy_checked_delta();
let sent_count = dispatcher.send_strategy_checked_delta(&client);
let start_delta_count = dispatcher.ui_strat_start_stop_v2(&client, true);
```

`send_strategy_checked_delta` sends `TStratCheckedSync` only when the delta is
non-empty. `ui_strat_start_stop_v2` always sends the UI start/stop command after
the client's Init gate is open; the checked delta may be empty because the same
wire packet also carries the start/stop action. Both helpers keep
`prev_checked` unchanged until the server confirms with `TStratCheckedEcho` or
`TStratCheckedSync`.

The explicit `client.strat_checked_sync(&items, true)`,
`client.strat_checked_echo(&items)`, and
`client.ui_strat_start_stop_v2(is_start, &items)` methods remain available for
protocol tools that already have the exact Delphi `Items` array. Regular
applications should prefer the dispatcher helpers so the library-owned strategy
state stays authoritative.

Checked-item arrays are serialized with Delphi `Word Count` semantics: the
outgoing count is the low 16 bits, and only that declared number of items is
written to the packet body.

The lower-level typed batch API remains available for explicit strategy sends.
It serializes the `StrategySnapshot` values, computes `ClientMaxLastDate`, and
sends the full CmdId=2 `TStratSnapshot` wire body. Pass the live schema that
`run_init_sequence` stored in `dispatcher.strats().strategy_schema()`:

```rust
let schema = dispatcher
    .strats()
    .strategy_schema()
    .expect("Init fetched TStratSchema");

client.strat_send_snapshot_batch(server_epoch, true, schema, &strategies);
```

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
`BuyOrderColor` are therefore not hardcoded in Rust. If a caller already has the
exact compressed Delphi serializer bytes, prefer `strat_send_snapshot_payload(...)`.

When decoding a snapshot after schema is available, known Delphi strategy fields
also keep Delphi `ReadField` type checks through the same schema: if the wire
TypeID does not match the schema/RTTI field type, the value is skipped instead
of being exposed as a wrongly typed field. Generic `parse_strategy_batch(...)`
remains available for diagnostics when no schema is available.

If the application already has a compressed `TStrategySerializer` payload, use
`strat_send_snapshot_payload(server_epoch, client_max_last_date, full, data)`.
Passing an empty `data` slice means an empty strategy list; the library encodes
it as a valid non-empty `TStrategySerializer` payload instead of sending
wire `Size=0`.

For advanced override replies, register a fresh snapshot provider on the
dispatcher:

```rust
use moonproto::StrategySnapshotReply;
use moonproto::commands::strategy_serializer::StrategySnapshot;

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
normalization as `strat_send_snapshot_payload`.
