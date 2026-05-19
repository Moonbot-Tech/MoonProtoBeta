# Balances channel (MPC_Balance)

Балансы аккаунта и маркетов: full snapshot + incremental updates.

## Что это

`TBalanceCommand` шлёт обновления балансов в трёх режимах:
- **cmd_id=2 (legacy snapshot)**: обновляются полученные маркеты, остальные **не сбрасываются** (merge update).
- **cmd_id=3 (full snapshot)**: маркеты не в snapshot **сбрасываются** в default; глобальные суммы обновляются.
- **cmd_id=4 (incremental)**: merge маркетов + опциональное обновление глобалов (gated `global_changed: bool`).

Sync state — `BalancesState`. Ключ — `market_name: String` (например `"BTCUSDT"`).

## Использование

```rust
use moonproto::commands::balance::parse_balance;
use moonproto::state::{BalancesState, BalanceEvent};

let mut balances = BalancesState::new();

if let Some(update) = parse_balance(cmd_id, &payload) {
    let event = balances.apply(update);
    match event {
        BalanceEvent::SnapshotApplied { count, epoch } => {
            println!("Full snapshot: {} markets, epoch={}", count, epoch);
        }
        BalanceEvent::LegacySnapshotApplied { count, epoch } => {
            println!("Legacy merge snapshot: {} markets", count);
        }
        BalanceEvent::IncrementalApplied { count, epoch, global_changed } => {
            println!("Incremental: {} markets changed, global={}", count, global_changed);
        }
        BalanceEvent::EpochStale { incoming, last } => {
            // Старый пакет после reconnect/reorder — пропущен.
        }
    }
}

// Получить баланс конкретного маркета:
if let Some(item) = balances.get("BTCUSDT") {
    println!("Pos size: {}, pos price: {}", item.pos_size, item.pos_price);
}

// Глобальные суммы в BTC:
let g = &balances.global;
println!("BTC total: {}, locked: {}, full: {}", g.btc_balance_total, g.btc_balance_locked, g.btc_balance_full);
```

## Epoch protection

Каждый update имеет `epoch: u16` — wrapping counter для защиты от out-of-order пакетов.
`BalancesState::apply` использует `epoch_is_ok(last, new)` (wrap-safe, аналог Delphi
`MoonProtoFunc.pas:188-203`): если `new` это duplicate или stale (≤100 шагов назад
с учётом u16-wrap) — `EpochStale` событие, state не меняется.

## BalanceItem (поля)

```rust
pub struct BalanceItem {
    pub market_name:           String,  // ключ в HashMap by_market
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
    pub btc_balance_total:    f64,  // Доступный баланс (свободный + locked, минус долги).
    pub btc_balance_locked:   f64,  // Заблокировано в открытых ордерах / залогах.
    pub btc_balance_full:     f64,  // Полный включая нереализованный PnL.
    pub special_coin_balance: f64,  // USDT для futures / BUSD/USDC при MA mode.
}
```

Все суммы в BTC equivalent. Обновляются в `cmd_id=4` только если `global_changed=true`.

## API state

```rust
pub struct BalancesState {
    pub global:     GlobalBalance,
    pub by_market:  HashMap<String, BalanceItem>,  // key = market_name
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

## Wire format

```
TBalanceCommand (CmdId 2/3/4):
  Header: CmdId(1) + ver(2) + UID(8) = 11 bytes
  epoch:                u16
  global_changed:       bool (1)  [только cmd_id=4]
  btc_balance_total:    f64       [только cmd_id=2/3 или cmd_id=4 если global_changed=true]
  btc_balance_locked:   f64       [...]
  btc_balance_full:     f64       [...]
  special_coin_balance: f64       [...]
  count:                i32
  items[count]:
    market_name:        string (u16-prefixed UTF-8)
    balance_hash:       u64
    flags:              u32       [bitmask какие поля присутствуют]
    [field values по битам flags]
```

Bitmask `flags` определяет какие поля из BalanceItem присутствуют в payload. Парсер заполняет
только присутствующие; остальные остаются default (для нового item) или сохраняют старое
значение (для merge в существующий).

## См. также

- [arb.md](arb.md) — `TArbPricesCommand` тоже в канале MPC_Balance (CmdId=6)
- [markets.md](markets.md) — для расчёта balance_usdt нужна цена из MarketsState
- [engine_api.md](engine_api.md) — `balance_request_refresh` (CmdId=5)
