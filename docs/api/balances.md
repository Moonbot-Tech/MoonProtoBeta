# Balances channel (MPC_Balance)

Account and market balances: full snapshots plus incremental updates.

## Overview

The balance channel uses these incoming command IDs:
- **cmd_id=0/1/2/5 and unknown ids**: the Delphi registry can parse or
  base-class them, but the reference client does not apply them to balance
  state and the active dispatcher emits no event.
- **cmd_id=3 (full snapshot)**: markets missing from the snapshot are **reset** to default values; global totals are updated.
- **cmd_id=4 (incremental)**: merges market rows and optionally updates global totals (gated by `global_changed: bool`).
- **cmd_id=6 (`TArbPricesCommand`)**: compact arb relay, exposed as
  `Event::Arb` after active dispatcher filtering.

The sync state is `BalancesState`. The key is `market_name: String`, for example `"BTCUSDT"`.
When using `EventDispatcher`, balance rows are applied only for markets present
in the current `MarketsState`, matching Delphi `Markets.MarketByNameFast`.
Unknown market names are ignored.
In that active path, a full snapshot also creates or keeps a zero/default
`BalanceItem` for every market known to `MarketsState` but absent from the
snapshot. If that market already had a balance row, Delphi's preserved fields
are kept: `balance_hash`, `max_value` (`bnMaxValue`), and the per-market last
balance epoch. If the known market had no previous balance row, the default row
uses `leverage_x=1` and zero balance/position/PNL fields.

For the common one-shot flow, use `Client::request_balance_snapshot`:

```rust
let balances = client.request_balance_snapshot(
    &mut dispatcher,
    std::time::Duration::from_secs(15),
)?;
println!("markets with balance rows={}", balances.len());
```

For a fire-and-forget refresh from another UI or worker thread, clone
`client.sender()` and call `sender.balance_request_refresh()`. The next full
balance snapshot arrives through the normal `EventDispatcher` path.

Balance-channel delivery is enabled by the normal init flow. The Delphi server
sets its per-client balance-subscription flag when it handles
`emk_UpdateMarketsList`; `connect_and_init` / `run_init_sequence` performs that
market refresh before it waits for balance state. `TRequestBalanceRefresh`
forces the server's next full snapshot tick, but by itself it does not enable
delivery to a client that has never completed init or otherwise sent
`UpdateMarketsList`. Regular applications should call balance snapshot helpers
after init.

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
        BalanceEvent::IncrementalApplied { count, epoch, global_changed } => {
            println!("Incremental: {} markets changed, global={}", count, global_changed);
        }
        BalanceEvent::Ignored { cmd_id, epoch } => {
            println!("Ignored balance command id={} epoch={}", cmd_id, epoch);
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
snapshots are not rejected by a global epoch gate. For markets missing from a
full snapshot, Delphi does not update `LastBalanceEpoch`; Rust keeps the same
per-market epoch for an existing row as well, so a later stale incremental for
that market is still rejected. A newly created default row for a known market
starts with epoch `0`, matching the machine effect of a market object that was
reset by the snapshot but did not receive a balance item.

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
    IncrementalApplied     { count: usize, epoch: u16, global_changed: bool },
    Ignored                { cmd_id: u8, epoch: u16 },
    EpochStale             { incoming: u16, last: u16 },
}
```

## Wire format

```
TBalanceCommand family:
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

Commands whose `ver` is greater than the current MoonProto command version are
skipped before balance parsing, matching Delphi `TCommandRegistry.FromStream`.

`cmd_id=2` shares the full-snapshot wire layout, but `EventDispatcher` ignores
it because Delphi `ProcessBalanceCommand` only applies exact
`TBalanceSnapshotFull` and `TBalanceIncrUpdate` objects. The same active
dispatcher skip rule applies to `TBaseBalanceCommand` (`cmd_id=0`),
`TBalanceCommandBase` (`cmd_id=1`), `TRequestBalanceRefresh` (`cmd_id=5`), and
unknown balance subcommands: they do not become `Event::Raw` or
`Event::ParseFailed`.

The `flags` bitmask defines which `BalanceItem` fields are present in the payload.
Omitted fields decode to their command defaults. Applying an item replaces the
stored row with the decoded item, except `max_value`: Delphi only updates
`bnMaxValue` when the incoming value is greater than `_eps`, so Rust preserves the
previous `max_value` when the decoded value is zero or otherwise not greater than
`1e-8`.

Fixed scalar fields are read like Delphi `TMemoryStream.Read`: a short body keeps
the bytes that are present, zero-fills the missing high bytes, advances by the
bytes consumed, and does not make the command fail. `market_name` is different:
Delphi `ReadStringFromStreamUtf8` uses `ReadBuffer`, so an incomplete string
length or body rejects the whole balance command. If `count` reaches an item whose
string cannot be read, the active dispatcher reports `ParseFailed` and applies no
partial balance update.

## Related API Surface

`TArbPricesCommand` also uses the MPC_Balance channel (`CmdId=6`).
It follows the same server-side balance-subscription filter as balance
snapshots, so an initialized active client sees it through `Event::Arb`, while a
raw pre-init transport connection should not expect arb broadcasts yet.
On the `EventDispatcher` active path, arb price/isolation records are filtered
through the current server `mIndex` map; unknown market indexes are consumed but
not exposed, matching Delphi `SrvMarkets.FindByServerIndex`.
`balance_usdt` calculation needs prices from `MarketsState`.
`request_balance_snapshot` is the high-level full-snapshot helper.
`request_balance` requests one-currency balance through Engine API.
