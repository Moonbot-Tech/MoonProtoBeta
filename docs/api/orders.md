# Orders channel (MPC_Order)

Этот документ описывает API для работы с торговыми ордерами через MoonProto.

## Что это

MoonProto сервер (MoonBot VPS) исполняет торговые ордера на бирже. Клиент через MoonProto **получает** обновления состояния ордеров и **отправляет** управляющие команды (отмена, перемещение цены, стопы и т.д.).

В Rust либе это реализовано через два уровня:
1. **Wire-парсеры и билдеры** в `commands::trade` — байтовый протокол.
2. **Sync state** в `state::Orders` — высокоуровневая модель ордеров с автоматическим применением входящих команд.

Полностью покрывает функциональность Delphi `BOrderWorker.DoTheJobVirtual` ([TaskWorkers.pas:7836](../../X:/proj-X/MoonBot/src/TaskWorkers.pas#L7836)) + `MoonProtoClient.ProcessCommandOrder` + `CleanupMissingWorkers`. Юзеру **не нужно** писать свой воркер per-ордер.

---

## Состояния ордера

`OrderWorkerStatus` (соответствует Delphi `TOrderWorkerStatus` из [MarketsU.pas:39](../../X:/proj-X/MoonBot/src/MarketsU.pas#L39)):

| Значение | Что значит |
|---|---|
| `None` | Pending / ещё не активен |
| `BuyFail` | Buy ордер не удалось разместить (фильтр, нет денег и т.п.) — терминал |
| `BuySet` | Buy ордер выставлен на бирже |
| `BuyCancel` | Buy отменён — терминал (если не было частичной заливки) |
| `BuyDone` | Buy полностью исполнен |
| `SellFail` | Sell не удалось разместить — терминал |
| `SellSet` | Sell выставлен |
| `SellCancel` | Sell отменён — терминал |
| `SelLDone` | Sell полностью исполнен — финальное состояние |
| `SelLAlmostDone` | Sell почти исполнен (Quantity ниже MinLotSize) |

`is_terminal()` возвращает `true` для `SelLDone`, `BuyCancel`, `BuyFail`, `SellFail`, `SellCancel`. На терминальном статусе ордер автоматически удаляется из `Orders`.

State machine flow:
```
None → BuySet → BuyDone → SellSet → SelLAlmostDone → SelLDone (terminal)
       ↓                  ↓                          
       BuyCancel          SellCancel/SellFail (terminal)
       ↓
       BuyFail (terminal)
```

---

## Структура Order

```rust
pub struct Order {
    pub uid: u64,                       // = task UID (MServerTag в Delphi)
    pub market_name: String,            // "BTCUSDT" etc.
    pub status: OrderWorkerStatus,
    pub buy_order: OrderCompact,        // 117-byte packed record с биржевым buy ордером
    pub sell_order: OrderCompact,       // то же для sell
    pub stops: StopSettings,            // настройки стоп-лосса/трейлинга/тейк-профита
    pub vstop_on: bool,                 // volume stop включен
    pub vstop_fixed: bool,
    pub vstop_level: f64,
    pub vstop_vol: f64,
    pub corridor_price_down: f32,       // корридор цен
    pub corridor_price_up: f32,
    pub strat_id: u64,                  // ID стратегии или 0
    pub is_short: bool,                 // short позиция (futures)
    pub db_id: i32,                     // DB ID на сервере
    pub from_cache: bool,               // восстановлен из server cache
    pub emulator_mode: bool,            // emulator mode (не реальная торговля)
    pub immune_for_clicks: bool,        // UI клики должны игнорироваться
    pub panic_sell: bool,               // включена паник-распродажа
    pub bulk_replace_buy: bool,         // buy в процессе массового replace
    pub bulk_replace_sell: bool,        // sell в процессе массового replace
    pub trace_points: Vec<OrderTracePoint>,  // history решений сервера (ring buffer)
    pub job_is_done: bool,              // ордер закроется на след. tick
    pub server_forced_remove: bool,     // TOrderNotFound пришёл
    pub sell_reason_code: u8,           // причина последней продажи (TSellReasonCode)
}
```

### `OrderCompact` (117 байт packed)

Точный port `MarketsU.pas:180`. Поля:
- `int_id: i64` — биржевой ID ордера.
- `quantity, quantity_remaining: f64` — общее и оставшееся количество.
- `total_btc, spent_btc: f64` — суммы в базовой валюте.
- `open_time, close_time, create_time: f64` (TDateTime, дни с 1899-12-30) — серверные времена.
- `actual_price, mean_price, quantity_base, actual_q, tmp_btc: f64` — расчётные поля.
- `panic_sell_down: f32` — максимально допустимая просадка для panic sell (%).
- `order_type: u8` (OrderType), `sub_type: u8`, `stop_flag: u8`, `partial_done: u8` (0..100), `leverage: u8`.
- `is_opened, is_closed, canceled, is_short: u8` (boolean).

Метод `adjust_time(server_time_delta)` корректирует TDateTime поля относительно локального времени клиента. `Orders::apply` делает это автоматически.

### `StopSettings` (46 байт packed)

Точный port `MarketsU.pas:215`. Полевая:
- `stop_loss_on, sl_fixed: bool`, `sl_level, sl_spread: f64` — стоп-лосс.
- `trailing_on, trailing_fixed: bool`, `trailing_level, ts_spread: f64` — трейлинг.
- `use_take_profit: bool`, `take_profit: f64`, `take_profit_changed: bool` — тейк-профит.

### `OrderUpdateData` (66 байт packed)

Точный port `MarketsU.pas:263`. Используется в delta-апдейтах:
- `int_id, actual_price, open_time, quantity, quantity_remaining, actual_q, total_btc, mean_price: f64/i64`.
- `partial_done, stop_flag: u8`.

---

## События OrderEvent

После каждого вызова `Orders::apply(cmd)` возвращается `(ApplyResult, OrderEvent)`.

```rust
pub enum OrderEvent {
    Created(u64),                                   // новый ордер
    Updated(u64),                                   // обновлён state существующего
    Removed(u64),                                   // ордер удалён (terminal/NotFound)
    BulkReplaced { order_type: OrderType, uids: Vec<u64> },  // массовый replace
    TracePoint { uid: u64 },                        // добавлена trace point
    CorridorChanged(u64),
    VStopChanged(u64),
    StopsChanged(u64),
    PanicSellChanged(u64),
    Snapshot,                                       // TAllStatuses применён
    Ignored { uid: u64, reason: ApplyResult },     // команда отклонена
}
```

```rust
pub enum ApplyResult {
    Applied,                // команда применена
    OutOfOrder,             // epoch < server_latest_epoch
    PhaseRollback,          // статус из старой фазы
    OrderNotFound,          // ордер для апдейта не в state
    NotApplicable,          // команда не относится к state (client-side)
}
```

---

## Anti-replay механизмы

### 1. Epoch protection

Каждый статус (`BuySet`, `SellSet` и т.д.) имеет свой monotonic epoch.
Сервер инкрементирует `Epoch` при каждой смене status. При получении команды с `epoch < server_latest_epoch[status]` — команда отклоняется (`ApplyResult::OutOfOrder`).

Это защищает от out-of-order доставки UDP пакетов. Соответствует Delphi `FServerLatestEpoch[]` в `BOrderWorker`.

### 2. Phase rollback protection

Если `current_status = SellSet` и приходит команда со статусом `BuySet` — отклоняется (`ApplyResult::PhaseRollback`). Сервер монотонно движется вперёд по state machine, и старые команды никогда не должны откатывать клиента назад.

Соответствует `HandleServerCommand` логике в `TaskWorkers.pas:1475`.

---

## Snapshot mechanism (CleanupMissing)

После reconnect сервер шлёт `TAllStatuses` — снапшот всех активных ордеров.

```rust
let mut orders = Orders::new();
// ... в callback'е on_data:
let (_, ev) = orders.apply(trade_command);
if let OrderEvent::Snapshot = ev {
    // После snapshot:
    let missing_uids = orders.missing_after_snapshot();
    for uid in missing_uids {
        // Послать TOrderStatusRequest для каждого
        let pkt = build_order_status_request(TradeCtx::new(uid), &order.market_name);
        client.send_cmd(...);
    }
}
```

Внутри `Orders::apply(AllStatuses)`:
1. Инкрементирует `current_snapshot_flag`.
2. Для каждого ордера из snapshot — применяет `OrderStatus` и ставит `entry.snapshot_flag = current_snapshot_flag`.
3. Ордера с `snapshot_flag != current` (т.е. не пришли в snapshot) → возвращаются через `missing_after_snapshot()`.

Соответствует `CurrentSnapshotFlag` в `MoonProtoClient.pas:106`.

---

## ServerTimeDelta correction

Сервер и клиент работают в разных часовых поясах + NTP drift. Все `TDateTime` поля в командах — это локальное серверное время.

При получении `Ping`:
```rust
orders.set_server_time_delta(initial_time_from_ping - delphi_now());
```

После этого все входящие `open_time`, `close_time`, `trace_time` и т.д. автоматически корректируются в `Orders::apply` (через `OrderCompact::adjust_time` / `OrderUpdateData::adjust_time` / `OrderTracePoint::adjust_time`).

Соответствует `ServerTimeDelta` в `MoonProtoClient.pas:65` + `BuyOrder.AdjustTime` calls в `ProcessCommandOrder`.

---

## Builders для исходящих команд

Все методы возвращают `Vec<u8>` — payload для `client.send_cmd_keyed(payload, Command::Order, SendPriority::High, encrypted=true, max_retries=3, u_key=...)`.

### Управление ордером

```rust
// Запросить snapshot всех ордеров (после reconnect):
let pkt = build_all_statuses_request(uid);

// Запросить статус конкретного ордера (после snapshot, для missing):
let pkt = build_order_status_request(ctx, market_name);

// Отменить ордер:
let pkt = build_order_cancel(ctx, market_name, epoch, status);

// Переместить buy/sell цену:
let pkt = build_order_replace(ctx, market_name, epoch, status, OrderType::Buy, new_price);

// Включить/выключить panic sell:
let pkt = build_turn_panic_sell(ctx, market_name, epoch, status, turn_on);

// Обновить стопы:
let pkt = build_order_stops_update(ctx, market_name, epoch, status, &stops);

// Обновить VStop:
let pkt = build_vstop_update(ctx, market_name, epoch, status, on, fixed, level, vol);
```

### Управление позицией

```rust
// Объединить ордера на маркете:
let pkt = build_join_orders(ctx, market_name, is_short);

// Разделить ордер на N частей:
let pkt = build_split_order(ctx, market_name, parts, small, small_sell);

// Закрыть позицию (market sell или limit):
let pkt = build_do_close_position(ctx, market_name, market_sell);
let pkt = build_do_limit_close_position(ctx, market_name, is_short);

// Limit-split / market-split позицию:
let pkt = build_do_split_position(ctx, market_name, is_short);
let pkt = build_do_market_split_position(ctx, market_name, is_short);

// Выставить sell с конкретной ценой/размером:
let pkt = build_do_sell_order(ctx, market_name, price, size);
```

### Bulk операции на маркете

```rust
// Переместить все sell ордера:
let pkt = build_move_all_sells(ctx, market_name, cmd_type, move_kind, price, price_zone, side);

// Переместить все buy:
let pkt = build_move_all_buys(ctx, market_name, cmd_type, move_kind, price, side);

// Пометить ордера как immune (UI клики игнорируются):
let pkt = build_set_immune(uid, &items);
```

### Создание нового ордера

```rust
// Новый buy ордер:
let pkt = build_new_order(ctx, market_name, is_short, price, strat_id, order_size);
```

---

## UKey дедупликация

Команды управления имеют `UKey` (TMoonUniqueKey: Kind + UID). Если в очереди отправки уже есть команда с тем же UKey — она автоматически выкидывается при поступлении новой.

Используется для команд перемещения цены (UK_OrderMove): если юзер кликнул "replace" дважды с разными ценами — отправится только последняя.

Маппинг команд → UKey kinds:
- `OrderReplace`, `OrderCancel`, `OrderStopsUpdate`, `VStopUpdate`, `TurnPanicSell` → `UK_OrderMove`.
- `OrderStatus` → `UK_OrderStatus` (с UID = order UID).
- `OrderStatusUpdate` → `UK_OrderStatusShort`.
- `SetImmune` → `UK_ImmuneClicks` (UID = sum всех Items.UID).

При отправке через `client.send_cmd_keyed(...)` передавай `UniqueKey { kind: <X>, uid: order_uid }`.

---

## Retry mechanism

Команды с `FCryped = true` (все control команды зашифрованы) имеют `MaxRetries`:
- Большинство: 3 (default из TBaseCommand.SetDefaults).
- `TOrderReplaceResponse`: 4 (`[MoonCmdRetries(4)]`).
- `TDoClose*`, `TDoLimit*`, `TDoSplit*`, `TDoMarketSplit*`, `TDoSellOrder*`: 1 (одноразовые операции).

Retry реализован на транспортном уровне `Client` через `PendingH` + ACK через Ping. Юзеру не нужно заботиться.

---

## Полный пример использования (концептуальный)

```rust
use moonproto::client::*;
use moonproto::commands::trade::*;
use moonproto::state::Orders;
use moonproto::protocol::Command;
use std::sync::{Arc, Mutex};

let cfg = ClientConfig { ... };
let mut client = Client::new(cfg);
let orders = Arc::new(Mutex::new(Orders::new()));

let orders_clone = orders.clone();
client.run(duration, Box::new(move |cmd, payload| {
    if cmd != Command::Order { return; }
    let Some(tc) = TradeCommand::parse(payload) else { return; };

    let mut orders = orders_clone.lock().unwrap();
    let (result, event) = orders.apply(tc);

    match event {
        OrderEvent::Created(uid) | OrderEvent::Updated(uid) => {
            let order = orders.get(uid).unwrap();
            // render in UI
        }
        OrderEvent::Removed(uid) => { /* remove from UI */ }
        OrderEvent::Snapshot => {
            let missing = orders.missing_after_snapshot();
            // для каждого uid — отправить build_order_status_request
        }
        OrderEvent::BulkReplaced { uids, .. } => { /* mark replace-pending */ }
        OrderEvent::TracePoint { uid } => { /* draw chart point */ }
        _ => {}
    }
}));

// User action: cancel order
let pkt = build_order_cancel(TradeCtx::new(some_uid), "BTCUSDT", epoch, status);
client.send_cmd_keyed(pkt, Command::Order, SendPriority::High, true, 3,
    UniqueKey { kind: 3 /* UK_OrderMove */, uid: some_uid });
```

---

## Соответствие Delphi (audit trail)

| Rust | Delphi |
|---|---|
| `commands::trade::TradeCommand` | `TBaseTradeCommand` иерархия (TradeStruct.pas) |
| `commands::trade::build_*` | `SendMClient*` в `BOrderWorker` (TaskWorkers.pas:7836-7906) |
| `Order` struct | поля `BOrderWorker` относящиеся к state ордера |
| `Orders::apply(OrderStatus)` | `HandleServerCommand(TOrderStatus)` (TaskWorkers.pas:1475) |
| `Orders::apply(OrderStatusUpdate)` | `HandleServerCommand(TOrderStatusUpdate)` |
| `Orders::apply(OrderReplaceResponse)` | `HandleServerCommand(TOrderReplaceResponse)` |
| `Orders::apply(OrderStopsUpdate)` | `HandleServerCommand(TOrderStopsUpdate)` |
| `Orders::apply(VStopUpdate)` | `HandleServerCommand(TVStopUpdate)` |
| `Orders::apply(CorridorUpdate)` | `HandleServerCommand(TCorridorUpdate)` |
| `Orders::apply(OrderTracePoint)` | `HandleServerCommand(TOrderTracePoint)` |
| `Orders::apply(AllStatuses)` | `ProcessCommandOrder(TAllStatuses)` (MoonProtoClient.pas:317) |
| `Orders::apply(BulkReplaceNotify)` | `ProcessCommandOrder(TBulkReplaceNotify)` (MoonProtoClient.pas:528) |
| `Orders::apply(OrderNotFound)` | `ProcessCommandOrder(TOrderNotFound)` (MoonProtoClient.pas:589) |
| `Orders::missing_after_snapshot()` | `CleanupMissingWorkers` (MoonProtoClient.pas:637) |
| `server_latest_epoch[]` | `FServerLatestEpoch[]` в BOrderWorker |
| `current_snapshot_flag` | `CurrentSnapshotFlag` в MoonProtoClient.pas:106 |
| `snapshot_flag` per order | `Worker.SnapshotFlag` в BOrderWorker |
| `server_time_delta` | `MCLient.ServerTimeDelta` (MoonProtoClient.pas:65) |
| `OrderCompact.adjust_time` | `BuyOrder.AdjustTime(ServerTimeDelta)` (MoonProtoClient.pas:601-609) |
| `is_terminal()` | `OS_SelLDone / OS_BuyCancel / OS_BuyFail / OS_SellFail / OS_SellCancel` checks |

---

## Известные ограничения

1. **Pending state мост** (когда юзер на клиенте поставил pending order перед отправкой на сервер) — не реализован в этой версии. Пока сервер не подтвердил приём, ордер не виден в `Orders`. Это можно добавить отдельным "pre-pending" слотом.

2. **`OrderCompact.adjust_time`** корректирует только `open_time`, `close_time`, `create_time`. Если в TOrderCompact появятся новые TDateTime поля — нужно дополнить.

3. **Trace points** — ring buffer на 256 последних точек (настраивается через `Orders::max_trace_points`). Старые точки автоматически удаляются.

4. **`AllStatuses.orders[]` парсинг** — каждый order в Delphi пишется через `o.StoreToStream(Stream)` с **полным** header'ом (CmdId + ver + UID + ...). Текущая реализация в Rust предполагает что каждый элемент — `TOrderStatus` (CmdId=4). Это совпадает с Delphi seek-логикой, но если сервер начнёт класть туда другие подклассы — нужно адаптировать.
