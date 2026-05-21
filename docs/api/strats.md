# Strategies

The strategy channel carries full strategy snapshots and compact updates:
delete, sell-price update, checked-state sync, and snapshot requests.

`EventDispatcher` maintains `StratsState` and emits `Event::Strat`. Snapshot
payloads are decoded automatically into both the lightweight `StrategyInfo`
state and the full `StrategySnapshot` map.

Before init, user code may give the library its current local strategies with
`EventDispatcher::set_local_strategies`. The dispatcher owns that list after
that point: it answers server `TStratSnapshotRequest` automatically and applies
strategy snapshots/deletes/checked updates received from the server. If user
code does not provide local strategies, the list starts empty and is filled only
by server snapshots; the current server snapshot is still available through the
same read API.

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
            StratEvent::SellPriceUpdated { strategy_id, sell_price } => {
                let info = state.strats().get(*strategy_id).expect("strategy state");
                println!("strategy {strategy_id} sell price={} checked={}", sell_price, info.checked);
            }
            StratEvent::Deleted { strategy_id } => remove_strategy(*strategy_id),
            StratEvent::CheckedSynced { changed, is_delta } => {
                println!("checked changed={changed} delta={is_delta}");
            }
            StratEvent::SnapshotRequested { .. } => {
                // Already answered by the dispatcher from its owned strategy list.
                // The event is emitted for UI/diagnostic awareness.
            }
            _ => {}
        }
    }
}));
```

`raw_data` is still present in snapshot events for diagnostics and custom
decoders, but normal applications should read `state.strategy_snapshot(...)` or
`state.strategy_snapshots()`.

## State

```rust
pub struct StrategyInfo {
    pub strategy_id: u64,
    pub strategy_ver: i32,
    pub last_date: u64,
    pub sell_price: f64,
    pub checked: bool,
    pub folder_path: String,
}
```

`StrategyInfo` is a lightweight UI/index state. Full `TStrategy` fields are not
stored there; they are stored as `StrategySnapshot` values owned by the
dispatcher.

```rust
use moonproto::commands::strategy_serializer::StrategySnapshot;

// Before connect_and_init:
let strategies: Vec<StrategySnapshot> = load_current_strategies();
dispatcher.set_local_strategies(&strategies);

// Later, read the current active-library view:
let all: Vec<StrategySnapshot> = dispatcher.strategy_snapshot_vec();
let one = dispatcher.strategy_snapshot(strategy_id);
```

## Snapshot Decoder

```rust
use moonproto::commands::strategy_serializer::{parse_strategy_batch, FieldValue};

let batch = parse_strategy_batch(raw_data).expect("bad strategy snapshot");
for strategy in &batch.strategies {
    if let Some(FieldValue::String(name)) = strategy.fields.get("StrategyName") {
        println!("{}: {}", strategy.strategy_id, name);
    }
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

Prefer `Client` wrappers:

```rust
client.strat_snapshot_request();
client.strat_sell_price_update(strategy_id, sell_price);
client.strat_delete(strategy_id, folder_path);
client.strat_checked_sync(&items, true);
client.strat_checked_echo(&items);
```

For normal active-library flow, set the local list before init and let the
dispatcher answer server snapshot requests:

```rust
use moonproto::commands::strategy_serializer::StrategySnapshot;

let strategies: Vec<StrategySnapshot> = load_current_strategies();
dispatcher.set_local_strategies(&strategies);
connect_and_init(&mut client, &mut dispatcher, connect_cfg)?;
```

The lower-level typed batch API remains available for explicit strategy sends.
It serializes the `StrategySnapshot` values, computes `ClientMaxLastDate`, and
sends the full CmdId=2 `TStratSnapshot` wire body:

```rust
client.strat_send_snapshot_batch(server_epoch, true, &strategies);
```

If the application already has a compressed `TStrategySerializer` payload, use
`strat_send_snapshot_payload(server_epoch, client_max_last_date, full, data)`.

For advanced override replies, register a fresh snapshot provider on the
dispatcher:

```rust
use moonproto::StrategySnapshotReply;
use moonproto::commands::strategy_serializer::StrategySnapshot;

dispatcher.set_strategy_snapshot_provider(move |_request_uid| {
    let strategies: Vec<StrategySnapshot> = load_current_strategies();
    let server_epoch = current_server_epoch();
    Some(StrategySnapshotReply::from_strategies(server_epoch, true, &strategies))
});
```

The provider must return current application-owned strategies. The dispatcher
falls back to its owned strategy list when the provider is absent or returns
`None`.
