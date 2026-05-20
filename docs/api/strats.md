# Strategies

The strategy channel carries full strategy snapshots and compact updates:
delete, sell-price update, checked-state sync, and snapshot requests.

`EventDispatcher` maintains `StratsState` and emits `Event::Strat`. Snapshot
payloads are decoded automatically into the lightweight `StrategyInfo` state; the
raw snapshot remains in the event for applications that need full field maps.

## Reading Strategy State

```rust
use moonproto::Event;
use moonproto::state::StratEvent;
use moonproto::commands::strategy_serializer::parse_strategy_batch;

client.run_with_dispatcher_state(duration, &mut dispatcher, Box::new(|event, state| {
    if let Event::Strat(strat_event) = event {
        match strat_event {
            StratEvent::SnapshotFull { raw_data, .. } => {
                let batch = parse_strategy_batch(raw_data).expect("bad strategy snapshot");
                println!("strategies={}", batch.strategies.len());
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
                // With a cached full snapshot, the library auto-echoes it.
                // This event is still emitted for UI/log visibility.
            }
            _ => {}
        }
    }
}));
```

If you only need the full field map, parse `raw_data` through
`commands::strategy_serializer::parse_strategy_batch`.

## State

```rust
pub struct StrategyInfo {
    pub strategy_id: u64,
    pub last_date: u64,
    pub sell_price: f64,
    pub checked: bool,
    pub folder_path: String,
}
```

`StrategyInfo` is a lightweight UI/index state. Full `TStrategy` fields are not
stored there; they are available in the decoded `StrategyBatch`.

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

To answer a snapshot request with application-owned strategy data, use the typed
batch API. It serializes the `StrategySnapshot` values, computes
`ClientMaxLastDate`, and sends the full CmdId=2 `TStratSnapshot` wire body:

```rust
use moonproto::commands::strategy_serializer::StrategySnapshot;

let strategies: Vec<StrategySnapshot> = load_current_strategies();
client.strat_send_snapshot_batch(server_epoch, true, &strategies);
```

If the application already has a compressed `TStrategySerializer` payload, use
`strat_send_snapshot_payload(server_epoch, client_max_last_date, full, data)`.

`EventDispatcher::dispatch_into_active` auto-echoes the last full snapshot when
the server sends `SnapshotRequested` and a cached full snapshot exists.

## Limits

`MAX_STRATEGIES = 50_000`. New strategy ids beyond the cap are rejected to avoid
unbounded memory growth; updates for existing ids remain allowed.
