# Balances channel (MPC_Balance)

Account and market balances: full snapshots plus incremental updates.

## Overview

`TBalanceCommand` sends balance updates in three modes:
- **cmd_id=2 (legacy snapshot)**: updates the markets present in the packet; all other markets are **not reset** (merge update).
- **cmd_id=3 (full snapshot)**: markets missing from the snapshot are **reset** to default values; global totals are updated.
- **cmd_id=4 (incremental)**: merges market rows and optionally updates global totals (gated by `global_changed: bool`).

The sync state is `BalancesState`. The key is `market_name: String`, for example `"BTCUSDT"`.

## Usage

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
            // Unknown or explicitly rejected update.
        }
    }
}

// Read one market balance:
if let Some(item) = balances.get("BTCUSDT") {
    println!("Pos size: {}, pos price: {}", item.pos_size, item.pos_price);
}

// Global totals in BTC:
let g = &balances.global;
println!("BTC total: {}, locked: {}, full: {}", g.btc_balance_total, g.btc_balance_locked, g.btc_balance_full);
```

## Epoch protection

Each update carries `epoch: u16`, a wrapping counter. Incremental updates use
per-market epoch protection, matching Delphi `m.LastBalanceEpoch`: stale items
are skipped, while newer items from the same packet can still be applied. Full
snapshots are not rejected by a global epoch gate.

`BalancesState::apply` uses `epoch_is_ok(last, new)` matching Delphi
`MoonProtoFunc.pas:188-203`: duplicate epochs are rejected, and a wrapped
backward distance of `100` or less is treated as stale.
`IncrementalApplied.count` is the number of market rows actually applied after
that per-market stale filtering.

## BalanceItem Fields

```rust
pub struct BalanceItem {
    pub market_name:           String,  // Key in the by_market HashMap.
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
    pub btc_balance_total:    f64,  // Available balance: free plus locked, minus debts.
    pub btc_balance_locked:   f64,  // Locked in open orders or collateral.
    pub btc_balance_full:     f64,  // Full balance including unrealized PnL.
    pub special_coin_balance: f64,  // USDT for futures, or BUSD/USDC in MA mode.
}
```

All amounts are BTC equivalents. For `cmd_id=4`, these fields are updated only when `global_changed=true`.

## API state

```rust
pub struct BalancesState {
    pub global:     GlobalBalance,
    pub by_market:  HashMap<String, BalanceItem>,  // key = market_name
    pub last_epoch: u16, // diagnostic: last accepted balance packet epoch
    // ...
}

impl BalancesState {
    pub fn new() -> Self;
    pub fn apply(&mut self, upd: BalanceUpdate) -> BalanceEvent;
    pub fn get(&self, market_name: &str) -> Option<&BalanceItem>;
}
```

## Events

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
  global_changed:       bool (1)  [cmd_id=4 only]
  btc_balance_total:    f64       [cmd_id=2/3, or cmd_id=4 when global_changed=true]
  btc_balance_locked:   f64       [...]
  btc_balance_full:     f64       [...]
  special_coin_balance: f64       [...]
  count:                i32
  items[count]:
    market_name:        string (u16-prefixed UTF-8)
    balance_hash:       u64
    flags:              u32       [bitmask of fields present in this item]
    [field values selected by flags bits]
```

The `flags` bitmask defines which `BalanceItem` fields are present in the payload.
Omitted fields decode to their command defaults. Applying an item replaces the
stored row with the decoded item, except `max_value`: Delphi only updates
`bnMaxValue` when the incoming value is greater than `_eps`, so Rust preserves the
previous `max_value` when the decoded value is zero or otherwise not greater than
`1e-8`.

## See Also

- [arb.md](arb.md): `TArbPricesCommand` also uses the MPC_Balance channel (CmdId=6)
- [markets.md](markets.md): balance_usdt calculation needs prices from `MarketsState`
- [engine_api.md](engine_api.md): `balance_request_refresh` (CmdId=5)
