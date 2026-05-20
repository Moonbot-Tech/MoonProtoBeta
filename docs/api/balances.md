# Balances channel (MPC_Balance)

Балансы аккаунта и маркетов: full snapshot + incremental updates.

## Что это

`TBalanceCommand` шлёт обновления балансов в трёх режимах:
- **cmd_id=2 (legacy snapshot)**: обновляются полученные маркеты, остальные **не сбрасываются** (merge update).
- **cmd_id=3 (full snapshot)**: маркеты не в snapshot **сбрасываются** в default; глобальные суммы обновляются.
- **cmd_id=4 (incremental)**: merge маркетов + опциональное обновление глобалов (gated `global_changed: bool`).

Sync state — `BalancesState`. Ключ — `market_name: String` (например `"BTCUSDT"`).

## Использование через EventDispatcher

```rust
use moonproto::events::{EventDispatcher, Event};
use moonproto::state::BalanceEvent;

let mut dispatcher = EventDispatcher::new();
client.run_with_dispatcher(duration, &mut dispatcher, Box::new(|ev| match ev {
    Event::Balance(BalanceEvent::SnapshotApplied { count, epoch }) => {
        println!("Full snapshot: {count} markets, epoch={epoch}");
    }
    Event::Balance(BalanceEvent::LegacySnapshotApplied { count, .. }) => {
        println!("Legacy merge snapshot: {count} markets");
    }
    Event::Balance(BalanceEvent::IncrementalApplied { count, global_changed, .. }) => {
        println!("Incremental: {count} markets, global={global_changed}");
    }
    Event::Balance(BalanceEvent::EpochStale { incoming, last }) => {
        log::warn!("stale balance epoch {incoming}, last {last}");
    }
    _ => {}
}));

// State доступен через getter:
let bs = dispatcher.balances();
if let Some(item) = bs.get("BTCUSDT") {
    println!("Pos size: {}, pos price: {}", item.pos_size, item.pos_price);
}
let g = &bs.global;
println!("BTC: total={} locked={} full={}",
         g.btc_balance_total, g.btc_balance_locked, g.btc_balance_full);
```

## Низкоуровневый pattern

```rust
use moonproto::commands::balance::parse_balance;
use moonproto::state::{BalancesState, BalanceEvent};

let mut balances = BalancesState::new();

// cmd_id берётся из payload[0], body — payload[11..] (после TBaseCommand header).
if let Some(update) = parse_balance(cmd_id, body) {
    let event = balances.apply(update);
    match event {
        BalanceEvent::SnapshotApplied { count, .. } => {}
        // ...
    }
}
```

## Epoch protection

Каждый update имеет `epoch: u16` — wrapping counter для защиты от out-of-order
пакетов. `BalancesState::apply` использует общий `state::epoch::epoch_is_ok`
(wrap-safe, RFC 1982 окно 32767, аналог Delphi `MoonProtoFunc.pas:188-203`):
если `new` это duplicate или stale — `EpochStale` событие, state не меняется.

## BalanceItem (поля)

```rust
pub struct BalanceItem {
    pub market_name:           String,    // ключ в HashMap by_market
    pub balance_hash:          u64,
    pub initial_balance:       f64,
    pub locked_balance:        f64,
    pub pos_size:              f64,
    pub pos_price:             f64,
    pub liq_price:             f64,
    pub pos_dir:               u8,
    pub long_pos_size:         f64,
    pub long_pos_price:        f64,
    pub long_liq_price:        f64,
    pub long_position_type:    u8,
    pub short_pos_size:        f64,
    pub short_pos_price:       f64,
    pub short_liq_price:       f64,
    pub short_position_type:   u8,
    pub asset_balance:         f64,
    pub asset_balance_full:    f64,
    pub total_profit_b:        f64,
    pub total_profit_l:        f64,
    pub total_profit_s:        f64,
    pub max_value:             f64,
    pub leverage_x:            i32,
    pub position_type:         u8,
}
```

## GlobalBalance

```rust
pub struct GlobalBalance {
    pub btc_balance_total:    f64,    // Доступный (свободный + locked, минус долги)
    pub btc_balance_locked:   f64,    // Заблокировано в ордерах / залогах
    pub btc_balance_full:     f64,    // Полный включая нереализованный PnL
    pub special_coin_balance: f64,    // USDT для futures / BUSD/USDC при MA mode
}
```

Все суммы в BTC equivalent. Обновляются в `cmd_id=4` только если `global_changed=true`.

## BalancesState API

```rust
pub struct BalancesState {
    pub global:     GlobalBalance,
    pub by_market:  HashMap<String, BalanceItem>,   // key = market_name
    pub last_epoch: u16,
    // ...
}

impl BalancesState {
    pub fn new() -> Self;
    pub fn apply(&mut self, upd: BalanceUpdate) -> BalanceEvent;
    pub fn get(&self, market_name: &str) -> Option<&BalanceItem>;
}
```

## События

```rust
pub enum BalanceEvent {
    SnapshotApplied        { count: usize, epoch: u16 },
    LegacySnapshotApplied  { count: usize, epoch: u16 },
    IncrementalApplied     { count: usize, epoch: u16, global_changed: bool },
    EpochStale             { incoming: u16, last: u16 },
}
```

## Refresh request

```rust
// Принудительно запросить full snapshot балансов:
client.balance_request_refresh();
```

Это `TBalanceRequestRefresh` (sub-cmd 5, UK_BalanceFull). Сервер ответит
`TBalanceCommand` с cmd_id=3 (full snapshot).

Альтернатива: `client.api_get_markets_balance_full()` через Engine API.

## OOM cap

`BalancesState` имеет `MAX_BALANCE_MARKETS = 20_000` — DoS guard. Реальная биржа
имеет сотни маркетов, 20K — щедрый запас.

## Wire format

```
TBalanceCommand (CmdId 2/3/4):
  Header: CmdId(1) + ver(2) + UID(8) = 11 bytes
  epoch:                u16
  global_changed:       bool (1)  [только cmd_id=4]
  btc_balance_total:    f64       [cmd_id=2/3 или cmd_id=4 если global_changed]
  btc_balance_locked:   f64       [...]
  btc_balance_full:     f64       [...]
  special_coin_balance: f64       [...]
  count:                i32
  items[count]:
    market_name:        string (u16-prefixed UTF-8)
    balance_hash:       u64
    flags:              u32       // bitmask какие поля присутствуют
    [field values по битам flags]
```

Bitmask `flags` определяет какие поля из BalanceItem присутствуют в payload.
Парсер заполняет только присутствующие; остальные остаются default (для нового
item) или сохраняют старое значение (для merge в существующий).

## См. также

- [arb.md](arb.md) — `TArbPricesCommand` тоже в канале MPC_Balance (CmdId=6).
- [markets.md](markets.md) — для расчёта balance_usdt нужна цена из MarketsState.
- [engine_api.md](engine_api.md) — `api_get_markets_balance_full`.
- [events.md](events.md) — EventDispatcher + Event::Balance.
