# Trade actions — high-level wrappers

18 высокоуровневых методов `Client` для отправки торговых команд через канал
`MPC_Order`. Все wrappers скрывают: priority, encryption, MaxRetries, UKey dedup.

## Когда использовать

Раньше потребитель писал:

```rust
let ctx = TradeCtx::new(order_uid);
let raw = commands::trade::build_order_cancel(ctx, "BTCUSDT", 0, status);
client.send_cmd(raw, Command::Order, SendPriority::High, true, 3);
// ❌ UKey dedup не работал → быстрые клики = дубликаты на сервере
```

Теперь:

```rust
let ctx = TradeCtx::new(order_uid);
client.cancel_order(ctx, "BTCUSDT", status);
// ✅ UK_OrderMove dedup активен — старая pending команда заменится новой
```

## TradeCtx

```rust
pub struct TradeCtx {
    pub uid:      u64,    // UID ордера (TaskID) — используется для UKey dedup
    pub currency: u8,
    pub platform: u8,
}

impl TradeCtx {
    pub fn new(uid: u64) -> Self;    // currency/platform = defaults
}
```

**ВАЖНО**: для order-keyed команд (replace/cancel/stops/panic/vstop) `ctx.uid`
должен быть **TaskID ордера на сервере** — это значение приходит в `Order.uid`
через `Event::Order(OrderEvent::Created)`. Если передать random — dedup сломается.

## Об Epoch

В Delphi приложение всегда передаёт `Epoch=0` в C→S командах — поле используется
только в server→client для filter out-of-order. Поэтому в Rust API epoch
**убран** из публичных сигнатур (`replace_order`/`cancel_order`/etc.) — внутри
build_* всегда передаём 0. Это упрощает API без потери функциональности.

## Список wrappers

| Метод | CmdId | UKey | MaxRetries | Что |
|---|---|---|---|---|
| `new_order(ctx, market, is_short, price, strat_id, order_size)` | 3 | — | 3 | Открыть новый ордер |
| `replace_order(ctx, market, status, order_type, new_price)` | 6 | `UK_OrderMove(ctx.uid)` | 3 | Replace ордера новой ценой |
| `request_all_statuses(uid)` | 9 | — | 3 | Запросить статусы всех ордеров |
| `cancel_order(ctx, market, status)` | 10 | `UK_OrderMove(ctx.uid)` | 3 | Отменить ордер |
| `join_orders(ctx, market, is_short)` | 11 | — | 3 | Объединить открытые ордера |
| `split_order(ctx, market, parts, small, small_sell)` | 12 | — | 3 | Разбить ордер |
| `move_all_sells(ctx, market, cmd_type, kind, price, zone, side)` | 13 | — | 3 | Двигать все sell ордера |
| `do_close_position(ctx, market, market_sell)` | 14 | — | **1** | Закрыть позицию (немедленно) |
| `do_limit_close_position(ctx, market, is_short)` | 15 | — | **1** | Закрыть лимит-ордером |
| `do_split_position(ctx, market, is_short)` | 16 | — | **1** | Разбить позицию |
| `do_sell_order(ctx, market, price, size)` | 17 | — | **1** | Sell немедленно |
| `request_order_status(ctx, market)` | 18 | — | 3 | Запросить статус ордера |
| `update_order_stops(ctx, market, status, &stops)` | 20 | `UK_OrderMove(ctx.uid)` | 3 | Обновить настройки стопов |
| `turn_panic_sell(ctx, market, status, turn_on)` | 21 | `UK_OrderMove(ctx.uid)` | 3 | Включить/выключить panic sell |
| `set_immune(uid, items)` | 22 | `UK_ImmuneClicks(sum)` | 3 | Пометить ордера как immune |
| `penalty(ctx, market)` | 23 | — | 3 | Пометить маркет penalty (cooldown) |
| `move_all_buys(ctx, market, cmd_type, kind, price, side)` | 27 | — | 3 | Двигать все buy ордера |
| `update_vstop(ctx, market, status, on, fixed, level, vol)` | 29 | `UK_OrderMove(ctx.uid)` | 3 | Обновить volume-stop |
| `do_market_split_position(ctx, market, is_short)` | 30 | — | **1** | Market-split позиции |

**MaxRetries=1** для команд которые **меняют живые ордера на бирже** — повторная
отправка опасна (double-fill).

## UKey dedup — что это

Команды с `[MoonCmdUnique(UK_*)]` атрибутом дедуплицируются на стороне клиента
и сервера. Когда юзер быстро кликает "Replace order" 3 раза:

1. Первый клик → команда добавлена в `self.sending` (Sliced) или `self.pending_h` (High).
2. Второй клик → старая команда **удаляется** из `sending`/`pending_h` (по
   совпадению `UKey`), новая добавляется.
3. Третий клик → аналогично.
4. На проводе уйдут все 3 (так как retry/ACK работает по отдельности), но сервер
   дедуплицирует через UKey window.

**Без UKey** все 3 replace'а исполнились бы на бирже → 3 операции вместо 1.

## Trade enums

### `OrderType` (Vars.pas:57)
```rust
pub enum OrderType { Sell = 0, Buy = 1, BuyStop = 2, BuyLimit = 3 }
```

### `OrderWorkerStatus` (MarketsU.pas:39)
```rust
pub enum OrderWorkerStatus {
    None = 0, BuyFail = 1, BuySet = 2, BuyCancel = 3, BuyDone = 4,
    SellFail = 5, SellSet = 6, SellCancel = 7, SelLDone = 8, SelLAlmostDone = 9,
}

impl OrderWorkerStatus {
    pub fn is_terminal(&self) -> bool;   // SelLDone, BuyCancel, BuyFail, SellFail, SellCancel
}
```

### `FixedPosition` (Vars.pas:52)
```rust
pub enum FixedPosition { Both = 0, Long = 1, Short = 2 }
```

### `ReplaceMultiKind` (Vars.pas:37)
```rust
pub enum ReplaceMultiKind {
    None = 0, Shift = 1, TopVol = 2, LowVol = 3,
    TopProfit = 4, All = 5, LastSet = 6, LastMoved = 7,
}
```

### `MoveAllCmdType` (TradeStruct.pas:148)
```rust
pub enum MoveAllCmdType {
    MoveKind  = 0,    // use ReplaceMultiKind selection
    PriceZone = 1,    // move orders in [price_zone.min_p, max_p]
    Pers      = 2,    // персональный режим
}
```

### `PriceZone` / `StopSettings` / `OrderCompact`
См. [orders.md](orders.md) — там полная wire-семантика.

## Пример

```rust
use moonproto::commands::trade::{
    TradeCtx, OrderType, OrderWorkerStatus,
    ReplaceMultiKind, FixedPosition, MoveAllCmdType, PriceZone, ImmuneItem,
};

// === Юзер кликнул "Replace sell @ 50100" ===
let order = dispatcher.orders().by_id.get(&order_uid).unwrap();
let ctx = TradeCtx::new(order.uid);
client.replace_order(ctx, &order.market_name, OrderWorkerStatus::SellSet,
                     OrderType::Sell, 50100.0);

// === Юзер кликнул "Cancel" на том же ордере ===
client.cancel_order(ctx, &order.market_name, OrderWorkerStatus::SellSet);

// === "Закрыть позицию by market" ===
client.do_close_position(ctx, "BTCUSDT", true);

// === "Двигать все sell ордера по зоне 49500..50500 на 50100" ===
client.move_all_sells(
    ctx, "BTCUSDT",
    MoveAllCmdType::PriceZone,
    ReplaceMultiKind::All,
    50100.0,
    PriceZone { min_p: 49500.0, max_p: 50500.0 },
    FixedPosition::Long,
);

// === "Включить panic sell" ===
client.turn_panic_sell(ctx, &order.market_name, OrderWorkerStatus::SellSet, true);

// === "Пометить immune (защита от clicks)" ===
let items = vec![
    ImmuneItem { uid: 100, value: true },
    ImmuneItem { uid: 200, value: true },
];
client.set_immune(rand::random(), &items);

// === "Penalty (cooldown) на маркет" ===
client.penalty(ctx, "BTCUSDT");
```

## Низкоуровневое (если нужен custom flow)

Builders в `commands::trade::build_*` возвращают `Vec<u8>` payload:

```rust
use moonproto::client::{SendPriority, UniqueKey};
use moonproto::protocol::Command;
use moonproto::commands::trade::{TradeCtx, build_order_cancel, OrderWorkerStatus};

let ctx = TradeCtx::new(order_uid);
let raw = build_order_cancel(ctx, "BTCUSDT", 0, OrderWorkerStatus::SellSet);
client.send_cmd_keyed(raw, Command::Order, SendPriority::High, true, 3,
                       UniqueKey::order_move(ctx.uid));
```

Это то что `client.cancel_order(ctx, market, status)` делает внутри.

## См. также

- [orders.md](orders.md) — wire-формат всех 30 sub-commands TBaseTradeCommand +
  Orders state apply logic.
- [events.md](events.md) — `Event::Order` для отслеживания изменений ордеров.
- [client.md](client.md) — Client transport + send_cmd_keyed + UKey types.
