# Strats channel (MPC_Strat)

Канал стратегий: snapshot полной структуры стратегий + дельты (sell-price update,
checked-sync, delete).

## Что это

`TStrategy` (Delphi) — большая структура (~200+ полей) с настройками одной
торговой стратегии. Сервер отправляет полный snapshot всех стратегий через
**RTTI-driven сериализацию** в `TStratSnapshot` команду, а дальше шлёт
компактные дельты:

- `TStratSellPriceUpdate` — изменилась цена продажи.
- `TStratCheckedSync` — синхронизация checked-флагов (UI чекбоксы старт/стоп).
- `TStratDelete` — стратегия удалена.

В Rust порте:
1. **Wire-парсеры подкоманд** в `commands::strat` (7 подкоманд CmdId 0..6).
2. **RTTI-driven payload decoder** в `commands::strategy_serializer` — парсит
   сжатый snapshot в `Vec<StrategySnapshot>` с `HashMap<FieldName, FieldValue>`
   для каждой.
3. **Sync state** в `state::StratsState` — `HashMap<strategy_id, StrategyInfo>` +
   автоматическое применение дельт.

## Подкоманды

| CmdId | Команда | Направление | Priority | Что |
|---|---|---|---|---|
| 1 | `SnapshotRequest` | C→S | High | "Пришли мне полный snapshot" |
| 2 | `Snapshot` | S→C | Sliced | Полный или partial snapshot (DEFLATE-compressed bin) |
| 3 | `Delete` | both | High | Стратегия удалена + folder_path |
| 4 | `SellPriceUpdate` | both | High (UK_StratSellPriceUpdate) | Изменилась цена продажи |
| 5 | `CheckedSync` | both | Sliced | Sync checked-флагов (full или delta) |
| 6 | `CheckedEcho` | C→S | High | ACK на дельту checked |

## Получение событий (через EventDispatcher)

```rust
use moonproto::events::{EventDispatcher, Event};
use moonproto::state::StratEvent;

let mut dispatcher = EventDispatcher::new();
client.run_with_dispatcher(duration, &mut dispatcher, Box::new(|ev| match ev {
    Event::Strat(StratEvent::SnapshotFull { server_epoch, raw_data }) => {
        // Декодировать полный snapshot:
        let batch = moonproto::commands::strategy_serializer::parse_strategy_batch(raw_data)
            .expect("parse_strategy_batch failed");
        for s in &batch.strategies {
            println!("Strategy {}: {} fields", s.strategy_id, s.fields.len());
        }
    }
    Event::Strat(StratEvent::SellPriceUpdated { strategy_id, sell_price }) => {
        // state уже обновлён — dispatcher.strats().by_id.get(strategy_id) даст актуальное.
    }
    Event::Strat(StratEvent::Deleted { strategy_id }) => { /* ... */ }
    Event::Strat(StratEvent::CheckedSynced { changed, is_delta }) => { /* ... */ }
    Event::Strat(StratEvent::SnapshotRequested { uid }) => {
        // Active library: если есть last_full_snapshot_raw в state —
        // dispatch_into_active уже отправил его автоматически через strat_send_snapshot.
        // Event для UI awareness.
    }
    _ => {}
}));
```

## Active library — auto-echo snapshot

Когда сервер шлёт `TStratSnapshotRequest` ("дай мне snapshot стратегий"), либа
**автоматически** echo'ит последний полученный `last_full_snapshot_raw` через
`client.strat_send_snapshot(...)`. App получает событие `SnapshotRequested` для
UI awareness, но **не обязан** слать reply сам.

Аналог Delphi `MoonProtoClient.pas:695-699` (там либа сама echo'ит).

Если `last_full_snapshot_raw = None` (никогда не получали snapshot) — event
эмитится, но auto-echo не происходит. Тогда app может сам обработать.

## Низкоуровневый pattern

```rust
use moonproto::commands::strat::StratCommand;
use moonproto::state::StratsState;

let mut state = StratsState::new();

if let Some(cmd) = StratCommand::parse(&payload) {
    let event = state.apply(cmd);
    // ... обработать event ...
}
```

## Sync state — StrategyInfo

`StratsState` — лёгкая sync-сводка для UI отображения списка стратегий.

```rust
pub struct StrategyInfo {
    pub strategy_id: u64,
    pub last_date:   u64,       // unix epoch ms — время последнего апдейта
    pub sell_price:  f64,       // последнее значение SellPrice
    pub checked:     bool,      // UI чекбокс старт/стоп
    pub folder_path: String,    // папка в дереве стратегий
}
```

**ВАЖНО:** Полные `TStrategy` поля (StrategyName, OrderSize, Comment,
CoinsBlackList, BuyVolume, ...) в `StrategyInfo` НЕ хранятся — это
observer-state, не полноценный кэш. Полный декодированный `StrategyBatch`
получается из `parse_strategy_batch(snapshot.raw_data)` — потребитель сам решает
что с ним делать (показать в UI, кэшировать, фильтровать).

## strategy_serializer — RTTI-driven decoder

`commands::strategy_serializer` парсит `TStratSnapshot.data` (DEFLATE-compressed
bin) в типизированный `StrategyBatch`.

### Wire format

После raw DEFLATE decompression (`-15` без zlib header):

```
NameDict:    Count:u16 + (NameLen:u8 + Name:bytes[NameLen]) * Count    // UTF-8 имена полей
PathDict:    Count:u16 + (PathLen:u8 + Path:bytes[PathLen]) * Count    // UTF-8 пути папок
StratCount:  u16
Strategies[StratCount]:
    StrategyID:        u64
    StrategyVer:       i32
    StrategyLastDate:  u64       // unix epoch ms
    Checked:           u8        // bool
    Kind:              u8        // TStrategyKind ordinal
    PathID:            u16       // index в PathDict
    FieldCount:        u16
    Fields[FieldCount]:
        FieldIdx:      u16       // index в NameDict
        TypeID:        u8        // (с возможным флагом TID_ZERO_FLAG = 0x80)
        [value]                  // отсутствует если ZERO_FLAG; иначе зависит от типа
```

### TypeID константы

| ID | Тип | Размер | Описание |
|---|---|---|---|
| 1  | `Bool`   | 1 byte | true/false |
| 2  | `Int32`  | 4 bytes | i32 LE |
| 3  | `Int64`  | 8 bytes | i64 LE |
| 4  | `Double` | 8 bytes | f64 LE |
| 5  | `String` | u16 + bytes | u16 LE prefix + UTF-8 |
| 6  | `Byte`   | 1 byte | u8 |
| 7  | `Word`   | 2 bytes | u16 LE |
| 8  | `UInt32` | 4 bytes | u32 LE |
| 9  | `UInt64` | 8 bytes | u64 LE |
| 10 | `Single` | 4 bytes | f32 LE |
| 0x80 | flag | — | ZERO_FLAG: значение = zero для типа, value bytes отсутствуют |

Unknown TypeID — fallback skip 8 байт, поле игнорируется.

### Использование

```rust
use moonproto::commands::strategy_serializer::*;

let batch = parse_strategy_batch(snapshot_raw_data).unwrap();
for s in &batch.strategies {
    if let Some(FieldValue::String(name)) = s.fields.get("StrategyName") {
        println!("Strategy {}: {}", s.strategy_id, name);
    }
    if let Some(FieldValue::Double(size)) = s.fields.get("OrderSize") {
        println!("  OrderSize: {}", size);
    }
}
```

### FieldValue enum

```rust
pub enum FieldValue {
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Double(f64),
    String(String),
    Byte(u8),
    Word(u16),
    UInt32(u32),
    UInt64(u64),
    Single(f32),
}
```

Хелперы: `.type_id()`, `.is_zero()`, `FieldValue::zero(type_id) -> Option<Self>`.

## Действия от клиента — Client methods

```rust
// Запросить snapshot
client.strat_snapshot_request();

// Отправить snapshot (auto-echo делает либа сама — обычно вручную не нужно)
client.strat_send_snapshot(&serialized_payload);

// Удалить стратегию
client.strat_delete(strategy_id, "folder/path");

// Изменить sell price
client.strat_sell_price_update(strategy_id, 50100.0);

// Sync checked state
let items = vec![
    moonproto::commands::strat::StratCheckedItem { strategy_id: 100, checked: true },
    moonproto::commands::strat::StratCheckedItem { strategy_id: 200, checked: false },
];
client.strat_checked_sync(&items, /* is_delta = */ true);

// Подтвердить delta checked (CheckedEcho)
client.strat_checked_echo(&items);
```

## UniqueKeys

| Команда | UKey |
|---|---|
| `Snapshot` (CmdId=2) | `UK_StratSnapshot` (UID=1, overlap) |
| `SellPriceUpdate` (CmdId=4) | `UK_StratSellPriceUpdate` (UID = strategy_id) |

## OOM cap

`StratsState` имеет `MAX_STRATEGIES = 50_000` — DoS guard от malicious server.

## См. также

- [ui.md](ui.md) — `client.ui_strat_start_stop()` посылает команды старт/стоп стратегий.
- [orders.md](orders.md) — ордера созданные стратегиями имеют `strat_id` поле.
- [events.md](events.md) — EventDispatcher + Event::Strat.
