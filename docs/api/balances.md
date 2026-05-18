# Balances channel (MPC_Balance)

Балансы аккаунта и маркетов: full snapshot + incremental updates с bitmask-оптимизацией.

## Что это

`TBalanceCommand` шлёт обновления балансов в трёх режимах:
- **cmd_id=2 (legacy)**: merge-update, маркеты не в snapshot НЕ сбрасываются.
- **cmd_id=3 (full snapshot)**: маркеты не в snapshot **удаляются** (reset to default).
- **cmd_id=4 (incremental)**: merge + `global_changed`-gated обновление глобальных значений.

Каждое поле `BalanceItem` имеет bitmask-флаг — присутствует ли оно в payload. Если флаг 0 — значение отсутствует и старое сохраняется.

## Использование

```rust
use moonproto::commands::balance::parse_balance_update;
use moonproto::state::BalancesState;

let mut balances = BalancesState::new();

if let Some(update) = parse_balance_update(&payload) {
    let events = balances.apply(update);
    for ev in events {
        match ev {
            BalanceEvent::MarketUpdated { market_idx } => {
                let item = balances.by_idx.get(&market_idx).unwrap();
                println!("BTC balance: {}", item.q_token);
            }
            BalanceEvent::MarketRemoved { market_idx } => { /* cmd_id=3 reset */ }
            BalanceEvent::GlobalUpdated => {
                println!("Total USDT: {}", balances.global.total_usdt);
            }
        }
    }
}
```

## Epoch protection

Каждый update имеет `epoch: u16` — wrapping counter для устаревших пакетов. `BalancesState::apply` использует `epoch_is_ok(old, new)` для отклонения out-of-order:

- Если `new_epoch < current_epoch` (с учётом wrap-around 65536) → пакет отброшен.
- Иначе → применён.

Это защищает от старых пакетов после reconnect.

## BalanceItem (22 поля с bitmask)

```rust
pub struct BalanceItem {
    pub market_idx: u16,
    pub q_token: f64,         // количество базовой монеты
    pub q_usdt: f64,
    pub avg_price: f64,
    pub unrealized_pnl: f64,
    pub last_pos_open_time: f64,
    pub liquidation_price: f64,
    pub margin_ratio: f64,
    pub maint_margin: f64,
    pub init_margin: f64,
    pub mark_price: f64,
    pub pos_side: u8,
    pub leverage: u16,
    // ... всего 22 поля
}
```

Bitmask u32 определяет какие поля присутствуют. Парсер заполняет только те что в маске; остальные читаются из existing item в state (merge).

## GlobalBalance

```rust
pub struct GlobalBalance {
    pub total_usdt: f64,
    pub free_usdt: f64,
    pub total_pnl: f64,
    pub margin_balance: f64,
    // ...
}
```

Обновляется только если в пакете установлен флаг `global_changed`.

## Wire format

```
TBalanceCommand:
  Header: CmdId(1) + ver(2) + UID(8) = 11 bytes
  cmd_id_sub: u8 (2/3/4)
  epoch: u16
  global_changed: bool (1)
  global_data: bytes (если global_changed=true)
  count: u16
  items[count]:
    mask: u32
    market_idx: u16
    [field values по mask битам]
```

## См. также

- [arb.md](arb.md) — `TArbPricesCommand` тоже в канале MPC_Balance (CmdId=6)
- [markets.md](markets.md) — для расчёта balance_usdt нужна цена из MarketsState
- [engine_api.md](engine_api.md) — `get_balance`, `get_markets_balance_full`
