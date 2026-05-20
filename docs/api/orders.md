# Orders channel (MPC_Order)

API для работы с торговыми ордерами через MoonProto.

## Что это

MoonProto сервер (MoonBot VPS) исполняет торговые ордера на бирже. Клиент через
MoonProto **получает** обновления состояния ордеров и **отправляет** управляющие
команды (отмена, replace, стопы, и т.д.).

Два уровня:
1. **Sync state** в `state::Orders` — высокоуровневая модель ордеров с
   автоматическим применением входящих команд (через EventDispatcher).
2. **Wire-парсеры/билдеры** в `commands::trade` — байтовый протокол.

Полностью покрывает функциональность Delphi `BOrderWorker.DoTheJobVirtual` +
`MoonProtoClient.ProcessCommandOrder` + `CleanupMissingWorkers`. Юзеру **не нужно**
писать свой воркер per-ордер.

Отправка команд — через [trade_actions.md](trade_actions.md) (18 high-level wrappers).

## Получение событий (рекомендуемый pattern)

```rust
use moonproto::events::{EventDispatcher, Event};
use moonproto::state::OrderEvent;

let mut dispatcher = EventDispatcher::new();
client.run_with_dispatcher(duration, &mut dispatcher, Box::new(|ev| match ev {
    Event::Order(OrderEvent::Created(uid)) => {
        let order = dispatcher.orders().by_id.get(&uid).unwrap();
        println!("new order {uid}: {} @ {}", order.market_name, order.buy_order.actual_price);
    }
    Event::Order(OrderEvent::Updated(uid)) => {
        let order = dispatcher.orders().by_id.get(&uid).unwrap();
        println!("order {uid} updated, status: {:?}", order.status);
    }
    Event::Order(OrderEvent::Removed(uid)) => {
        println!("order {uid} closed");
    }
    Event::Order(OrderEvent::Snapshot) => {
        let missing = dispatcher.orders().missing_after_snapshot();
        // для каждого uid — отправить request_order_status (или вообще игнорировать).
    }
    Event::Order(OrderEvent::BulkReplaced { uids, .. }) => { /* ... */ }
    Event::Order(OrderEvent::TracePoint { uid }) => { /* draw chart point */ }
    _ => {}
}));
```

Доступ к state — через getter `dispatcher.orders()` (read-only). Прямой мутации
снаружи нет — изменения только через `dispatch_*`.

## Состояния ордера

`OrderWorkerStatus` (соответствует Delphi `TOrderWorkerStatus` из `MarketsU.pas:39`):

| Значение | Что значит |
|---|---|
| `None` | Pending / ещё не активен |
| `BuyFail` | Buy ордер не удалось разместить — терминал |
| `BuySet` | Buy ордер выставлен на бирже |
| `BuyCancel` | Buy отменён — терминал (если не было частичной заливки) |
| `BuyDone` | Buy полностью исполнен |
| `SellFail` | Sell не удалось разместить — терминал |
| `SellSet` | Sell выставлен |
| `SellCancel` | Sell отменён — терминал |
| `SelLDone` | Sell полностью исполнен — финальное состояние |
| `SelLAlmostDone` | Sell почти исполнен (Quantity ниже MinLotSize) |

`is_terminal()` возвращает `true` для `SelLDone`, `BuyCancel`, `BuyFail`,
`SellFail`, `SellCancel`. На терминальном статусе ордер автоматически удаляется
из `Orders`.

State machine flow:
```
None → BuySet → BuyDone → SellSet → SelLAlmostDone → SelLDone (terminal)
       │                  │
       BuyCancel          SellCancel/SellFail (terminal)
       │
       BuyFail (terminal)
```

## Структура Order

```rust
pub struct Order {
    pub uid:                  u64,                       // = task UID (MServerTag в Delphi)
    pub market_name:          String,
    pub status:               OrderWorkerStatus,
    pub buy_order:            OrderCompact,              // 117-byte packed record
    pub sell_order:           OrderCompact,
    pub stops:                StopSettings,
    pub vstop_on:             bool,
    pub vstop_fixed:          bool,
    pub vstop_level:          f64,
    pub vstop_vol:            f64,
    pub corridor_price_down:  f32,
    pub corridor_price_up:    f32,
    pub strat_id:             u64,
    pub is_short:             bool,
    pub db_id:                i32,
    pub from_cache:           bool,
    pub emulator_mode:        bool,
    pub immune_for_clicks:    bool,
    pub panic_sell:           bool,
    pub bulk_replace_buy:     bool,
    pub bulk_replace_sell:    bool,
    pub trace_points:         Vec<OrderTracePoint>,
    pub job_is_done:          bool,
    pub server_forced_remove: bool,
    pub sell_reason_code:     u8,
}
```

### `OrderCompact` (117 байт packed)

Точный port `MarketsU.pas:180`. Поля:
- `int_id: i64` — биржевой ID ордера.
- `quantity, quantity_remaining: f64` — общее и оставшееся количество.
- `total_btc, spent_btc: f64` — суммы в базовой валюте.
- `open_time, close_time, create_time: f64` (TDateTime, дни с 1899-12-30).
- `actual_price, mean_price, quantity_base, actual_q, tmp_btc: f64`.
- `panic_sell_down: f32`.
- `order_type: u8`, `sub_type: u8`, `stop_flag: u8`, `partial_done: u8` (0..100), `leverage: u8`.
- `is_opened, is_closed, canceled, is_short: u8` (boolean).

Метод `adjust_time(server_time_delta)` корректирует TDateTime поля относительно
локального времени клиента. `Orders::apply` делает это автоматически.

### `StopSettings` (46 байт packed)

Точный port `MarketsU.pas:215`:
- `stop_loss_on, sl_fixed: bool`, `sl_level, sl_spread: f64` — стоп-лосс.
- `trailing_on, trailing_fixed: bool`, `trailing_level, ts_spread: f64` — трейлинг.
- `use_take_profit: bool`, `take_profit: f64`, `take_profit_changed: bool`.

### `OrderUpdateData` (66 байт packed)

Точный port `MarketsU.pas:263`. Используется в delta-апдейтах:
- `int_id, actual_price, open_time, quantity, quantity_remaining, actual_q, total_btc, mean_price: f64/i64`.
- `partial_done, stop_flag: u8`.

## OrderEvent

```rust
pub enum OrderEvent {
    Created(u64),                                              // новый ордер
    Updated(u64),                                              // обновлён state существующего
    Removed(u64),                                              // ордер удалён (terminal/NotFound)
    BulkReplaced { order_type: OrderType, uids: Vec<u64> },    // массовый replace
    TracePoint { uid: u64 },                                   // добавлена trace point
    CorridorChanged(u64),
    VStopChanged(u64),
    StopsChanged(u64),
    PanicSellChanged(u64),
    Snapshot,                                                  // TAllStatuses применён
    Ignored { uid: u64, reason: ApplyResult },                 // команда отклонена
}

pub enum ApplyResult {
    Applied,                // команда применена
    OutOfOrder,             // epoch < server_latest_epoch
    PhaseRollback,          // статус из старой фазы
    OrderNotFound,          // ордер для апдейта не в state
    NotApplicable,          // команда не относится к state (client-side)
}
```

## Anti-replay механизмы

### 1. Epoch protection

Каждый статус (`BuySet`, `SellSet` и т.д.) имеет свой monotonic epoch. Сервер
инкрементирует `Epoch` при каждой смене status. При получении команды с
`epoch < server_latest_epoch[status]` — команда отклоняется
(`ApplyResult::OutOfOrder`).

Защищает от out-of-order доставки UDP пакетов. Соответствует Delphi
`FServerLatestEpoch[]` в `BOrderWorker`.

### 2. Phase rollback protection

Если `current_status = SellSet` и приходит команда со статусом `BuySet` —
отклоняется (`ApplyResult::PhaseRollback`). Сервер монотонно движется вперёд по
state machine, старые команды никогда не должны откатывать клиента назад.

## Snapshot mechanism (CleanupMissing)

После reconnect сервер шлёт `TAllStatuses` — снапшот всех активных ордеров.
Через EventDispatcher:

```rust
Event::Order(OrderEvent::Snapshot) => {
    let missing_uids = dispatcher.orders().missing_after_snapshot();
    for uid in missing_uids {
        if let Some(order) = dispatcher.orders().by_id.get(&uid) {
            client.request_order_status(
                moonproto::commands::trade::TradeCtx::new(uid),
                &order.market_name,
            );
        }
    }
}
```

Внутри `Orders::apply(AllStatuses)`:
1. Инкрементирует `current_snapshot_flag`.
2. Для каждого ордера из snapshot — применяет `OrderStatus` и ставит
   `entry.snapshot_flag = current_snapshot_flag`.
3. Ордера с `snapshot_flag != current` (не пришли в snapshot) →
   возвращаются через `missing_after_snapshot()`.

## ServerTimeDelta correction

При получении `Ping` Client обновляет `server_time_delta`. `EventDispatcher`
auto-applies это к `Orders` через `set_server_time_delta(...)` перед каждым
`Orders::apply(TradeCommand)`. После этого все входящие `open_time` /
`close_time` / `trace_time` автоматически корректируются (через `OrderCompact::adjust_time` /
`OrderUpdateData::adjust_time` / `OrderTracePoint::adjust_time`).

**Multi-Client**: каждый Client имеет свой `server_time_delta_handle`,
EventDispatcher линкуется автоматически в `dispatch_into_active`. См.
[multi_server.md](multi_server.md).

## Отправка команд

Через high-level wrappers — см. [trade_actions.md](trade_actions.md):

```rust
use moonproto::commands::trade::{TradeCtx, OrderType, OrderWorkerStatus};

let ctx = TradeCtx::new(order_uid);
client.replace_order(ctx, "BTCUSDT", OrderWorkerStatus::SellSet,
                     OrderType::Sell, 50100.0);
client.cancel_order(ctx, "BTCUSDT", OrderWorkerStatus::SellSet);
client.do_close_position(ctx, "BTCUSDT", true);
```

UKey dedup и retry settings — встроены в wrappers, потребитель ничем не
управляет (см. trade_actions.md).

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

## OOM cap

`Orders` имеет `MAX_ORDERS = 50_000` — защита от malicious server / bug в
сервере. При превышении — drop oldest по timestamp.

## Известные ограничения

1. **Pending state мост** (когда юзер на клиенте поставил pending order перед
   отправкой на сервер) — не реализован. Пока сервер не подтвердил приём, ордер
   не виден в `Orders`.
2. **`OrderCompact.adjust_time`** корректирует только `open_time`, `close_time`,
   `create_time`. Если в TOrderCompact появятся новые TDateTime поля — нужно
   дополнить.
3. **Trace points** — ring buffer на 256 последних точек. Старые точки
   автоматически удаляются.

## См. также

- [trade_actions.md](trade_actions.md) — 18 high-level wrappers для отправки команд.
- [events.md](events.md) — EventDispatcher + Event::Order.
- [client.md](client.md) — Client transport + send_cmd_keyed + UKey types.
- [multi_server.md](multi_server.md) — per-Client ServerTimeDelta.
