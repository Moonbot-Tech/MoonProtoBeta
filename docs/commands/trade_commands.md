# MPC_Order — Trading Commands

`MPC_Order` (channel byte 28) — канал торговых команд. 30 sub-types, каждый с
уникальным `CmdId` (1..30). Двунаправленный: клиент шлёт команды (новый ордер,
отмена, replace), сервер шлёт обновления (статус ордера, snapshot всех ордеров,
notFound уведомления, и т.д.).

CmdId соответствуют variant'ам `commands::trade::TradeCommand` enum'а.

## Wire format

### Общий header

```
[CmdId]          — 1 byte  — sub-command identifier (1..30)
[ver=3]          — 2 bytes LE — protocol version
[UID]            — 8 bytes LE — task_id ордера (или random для not-keyed команд)
[class-specific payload...] — variable
```

Version gate: при `ver > 3` команда парсится как `TradeCommand::Unknown { cmd_id, uid }`
(forward-compatible skip). При `ver <= 3` — полный парсинг.

Wire-форматы packed records см. в `SPEC.md §10.2`:
- `OrderCompact` — 117 байт (поле в OrderStatus / OrderStatusUpdate / AllStatuses)
- `StopSettings` — 46 байт (поле в OrderStopsUpdate)
- `OrderUpdateData` — 66 байт (поле в OrderStatusUpdate)
- `PriceZone` — 16 байт (поле в CorridorUpdate)
- `ImmuneItem` — 9 байт (поле в SetImmune)

## CmdId таблица (полная, по wire-format)

CmdId'ы взяты непосредственно из `TradeCommand::parse` (`commands/trade.rs`).
Парсер версии 3 знает все 30 значений.

| CmdId | Variant | Direction | Описание | Rust struct |
|-------|---------|-----------|----------|-------------|
| 1 | `BaseMarket` | n/a | Ancestor type (raw `MarketCommandHeader`) — на проводе не используется отдельно. | `MarketCommandHeader` |
| 2 | `TradeEpoch` | n/a | Ancestor type (raw `TradeEpochHeader`) — на проводе не используется отдельно. | `TradeEpochHeader` |
| 3 | `NewOrder` | **C→S** | Открыть новый ордер. | `NewOrderCommand` |
| 4 | `OrderStatus` | **S→C** | Полный snapshot ордера (создание, либо после reconnect). | `OrderStatus` (содержит `OrderCompact` 117б) |
| 5 | `OrderStatusUpdate` | **S→C** | Delta-update полей ордера. | `OrderStatusUpdate` (содержит `OrderUpdateData` 66б) |
| 6 | `OrderReplace` | **C→S** | Replace ордера новой ценой. | `OrderReplaceCommand` |
| 7 | `OrderReplaceResponse` | **S→C** | Подтверждение от сервера на replace. | `OrderReplaceResponse` |
| 8 | `AllStatuses` | **S→C** | Снапшот всех ордеров (для `CleanupMissing` на клиенте). | `AllStatuses` (массив `OrderCompact`) |
| 9 | `AllStatusesRequest` | **C→S** | Запрос на получение всех ордеров. | `BaseCommandHeader` |
| 10 | `OrderCancel` | **C→S** | Отмена ордера. | `OrderCancelCommand` |
| 11 | `JoinOrders` | **C→S** | Объединить открытые ордера в одну позицию. | `JoinOrdersCommand` |
| 12 | `SplitOrder` | **C→S** | Разделить позицию на N частей. | `SplitOrderCommand` |
| 13 | `MoveAllSells` | **C→S** | Batch-move всех sell-ордеров. | `MoveAllSellsCommand` |
| 14 | `DoClosePosition` | **C→S** | Закрыть позицию (market-close). | `DoClosePositionCommand` |
| 15 | `DoLimitClosePosition` | **C→S** | Limit-закрытие позиции. | `JoinOrdersCommand` (re-use payload format) |
| 16 | `DoSplitPosition` | **C→S** | Разделить позицию (split-close). | `JoinOrdersCommand` (re-use) |
| 17 | `DoSellOrder` | **C→S** | Прямой sell-ордер (цена + размер). | `DoSellOrderCommand` |
| 18 | `OrderStatusRequest` | **C→S** | Запрос конкретного ордера по UID (CleanupMissing). | `TradeEpochHeader` |
| 19 | `OrderNotFound` | **S→C** | Сервер сообщает что ордер с этим UID не найден. | `TradeEpochHeader` |
| 20 | `OrderStopsUpdate` | **C→S** или **S→C** | Обновление stops (SL/TP) — клиент шлёт изменения, сервер шлёт echo/notify. | `OrderStopsUpdate` (содержит `StopSettings` 46б) |
| 21 | `TurnPanicSell` | **C→S** | Включить/выключить panic-sell режим. | `TurnPanicSellCommand` |
| 22 | `SetImmune` | **C→S** | Пометить ордера как immune от UI-кликов (защита от случайных). | `SetImmuneCommand` (массив `ImmuneItem` 9б каждый) |
| 23 | `Penalty` | **C→S** | Пометить маркет penalty (cooldown). | `MarketCommandHeader` |
| 24 | `TradeVisual` | **S→C** | Visual-only команда (base type для diagnostic пакетов). | `MarketCommandHeader` |
| 25 | `OrderTracePoint` | **S→C** | Точка трейс-графика ордера (для UI визуализации). | `OrderTracePoint` |
| 26 | `CorridorUpdate` | **S→C** | Обновление price corridor для позиции. | `CorridorUpdate` (содержит `PriceZone` 16б) |
| 27 | `MoveAllBuys` | **C→S** | Batch-move всех buy-ордеров. | `MoveAllBuysCommand` |
| 28 | `BulkReplaceNotify` | **S→C** | Уведомление о массовом replace результатах. | `BulkReplaceNotify` |
| 29 | `VStopUpdate` | **C→S** или **S→C** | Обновление virtual stop. | `VStopUpdate` |
| 30 | `DoMarketSplitPosition` | **C→S** | Market-split позиции. | `JoinOrdersCommand` (re-use) |

**Замечание по направлениям:** некоторые команды (`OrderStopsUpdate`, `VStopUpdate`)
ходят в обе стороны — клиент обновляет, сервер шлёт echo/notify об изменениях
сделанных другим клиентом или engine'ом.

## Order state machine

См. `OrderWorkerStatus` doc comment в `commands::trade::OrderWorkerStatus`:

```text
None ──► BuySet ──► BuyDone ──► SellSet ──► SelLAlmostDone ──► SelLDone
          │           │           │            │
          ▼           ▼           ▼            ▼
       BuyFail    BuyCancel   SellFail    SellCancel
```

**Terminal states:** `SelLDone`, `BuyFail`, `BuyCancel`, `SellFail`, `SellCancel`.

## UKey dedup для команд

Некоторые команды имеют `[MoonCmdUnique]` атрибут в Delphi — UniqueKey-based dedup
в очереди отправки. Если ты шлёшь `replace_order` 5 раз подряд (быстро), в очередь
попадёт только **последняя версия** (UK_OrderMove dedup по task_id). Полезно для
UI: drag-replace генерирует поток, на сервер уходит финальное значение.

| Команда | UKey |
|---------|------|
| `OrderReplace` (CmdId 6) | `UK_OrderMove(task_id)` |
| `OrderCancel` (CmdId 10) | `UK_OrderMove(task_id)` |
| `OrderStopsUpdate` (CmdId 20) | `UK_OrderMove(task_id)` |
| `TurnPanicSell` (CmdId 21) | `UK_OrderMove(task_id)` |
| `VStopUpdate` (CmdId 29) | `UK_OrderMove(task_id)` |
| `SetImmune` (CmdId 22) | `UK_ImmuneClicks(items_uid_sum)` |

Остальные команды отправляются без dedup.

## Priority и retries

Все Order команды отправляются как:
- **Encrypted** (envelope Crypted + AES-GCM)
- **Priority** = High (быстрая доставка, ACK piggyback через Ping)
- **MaxRetries** = 3 — кроме `DoClose*` (CmdId 14-17, 30) где **MaxRetries = 1**
  (опасные команды, не ретраить много раз чтобы случайно не закрыть позицию дважды).

Эти параметры зашиты в Client-обёртки (`client.new_order`, `client.cancel_order`, ...)
— потребитель не управляет ими вручную.

## EventDispatcher → типизированные события

EventDispatcher автоматически парсит входящие `MPC_Order` и обновляет `Orders` sync-state.
Потребитель получает `Event::Order(OrderEvent)` через `dispatcher.dispatch(cmd, payload, now_ms)`.

OrderEvent variants (см. `state::orders::OrderEvent`):
- `Created` — новый ордер (после первого OrderStatus с этим task_id)
- `Updated` — обновление полей (OrderStatusUpdate или повторный OrderStatus)
- `Removed` — удалён (OrderNotFound, terminal status, явное удаление сервером)
- `TracePoint` — пришёл OrderTracePoint
- ... (см. полный список в state/orders.rs)

**Note:** `OrderEvent` — это **высокоуровневое** API для UI потребителя, не сырая
wire-команда. Маппинг wire CmdId → OrderEvent делается внутри `Orders::apply`.

## Wire-format детали

Полный byte-layout каждой sub-команды + packed records:
- **OrderCompact** (117 байт) — содержит UID, task_id, market_id, status, price, size,
  filled, stops, и т.д.
- **StopSettings** (46 байт) — SL/TP цены + флаги.
- **OrderUpdateData** (66 байт) — delta-поля для OrderStatusUpdate.
- **PriceZone** (16 байт) — top/bottom цены corridor'а.
- **ImmuneItem** (9 байт) — UID ордера + immune flag.

Точные смещения и типы каждого поля — во внутреннем `SPEC.md §10.2` (вместе с
ссылками на Delphi-source для проверки byte-exact wire-совместимости).
