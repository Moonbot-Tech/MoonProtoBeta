# Balances

The Active Lib keeps a live balance read model for the connected MoonBot
session. Application code normally does not parse balance packets directly:
run the client with an `EventDispatcher`, read `dispatcher.balances()`, and react
to `Event::Balance` when the state changes.

## What The Library Maintains

`BalancesState` stores:

- global account totals in BTC equivalent;
- one `BalanceItem` per known market;
- per-market position, liquidation, PnL, leverage, and spot asset values;
- per-market epoch tracking so stale incremental rows are ignored.

Rows are keyed by market name, for example `"BTCUSDT"`.

Incoming balance rows are applied only for markets already known to
`MarketsState`. This matches the Delphi client: an unknown market name does not
create an orphan balance row.

Full snapshots are authoritative for the current known market universe. If a
known market is missing from a full snapshot, the library keeps a default
zero-balance row for it with `leverage_x = 1`. Existing preserved Delphi fields
such as `balance_hash`, `max_value`, and the per-market last epoch are retained.

Incremental updates merge only the changed rows and optionally update global
totals. Stale incremental rows are skipped per market.

## Reading Current State

```rust
let balances = dispatcher.balances();

if let Some(btc) = balances.get("BTCUSDT") {
    println!(
        "pos={} entry={} liq={} lev={}x",
        btc.pos_size,
        btc.pos_price,
        btc.liq_price,
        btc.leverage_x
    );
}

let global = &balances.global;
println!(
    "btc_total={} locked={} full={}",
    global.btc_balance_total,
    global.btc_balance_locked,
    global.btc_balance_full
);
```

`dispatcher.balances()` is always the latest state after all events already
delivered by the dispatcher.

## Getting A Fresh Snapshot

Use `Client::request_balance_snapshot` when the application needs a fresh full
snapshot before continuing:

```rust
let balances = client.request_balance_snapshot(
    &mut dispatcher,
    std::time::Duration::from_secs(15),
)?;

println!("balance rows={}", balances.len());
```

The helper sends the library-level balance refresh request, pumps the UDP loop,
waits for the next full snapshot event, and returns a cloned `BalancesState`.

For fire-and-forget refresh in a custom low-level runtime:

```rust
let sender = client.sender();
sender.balance_request_refresh();
```

The next snapshot arrives through the normal dispatcher path.

## Events

```rust
use moonproto::{Event, state::BalanceEvent};

for event in client.drain_events() {
    match event {
        Event::Balance(BalanceEvent::SnapshotApplied { count, epoch }) => {
            println!("full balance snapshot: rows={count} epoch={epoch}");
        }
        Event::Balance(BalanceEvent::IncrementalApplied {
            count,
            epoch,
            global_changed,
        }) => {
            println!("balance increment: rows={count} epoch={epoch} global={global_changed}");
        }
        Event::Balance(BalanceEvent::Ignored { .. })
        | Event::Balance(BalanceEvent::EpochStale { .. }) => {
            // Diagnostic states. The read model remains valid.
        }
        _ => {}
    }
}
```

`SnapshotApplied.count` and `IncrementalApplied.count` are counts of rows that
actually changed the read model after market filtering and stale-epoch checks.

## BalanceItem Fields

```rust
pub struct BalanceItem {
    pub market_name:           String,
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

`max_value` follows Delphi behavior: a zero or near-zero incoming value does not
erase a previously known non-zero value.

## GlobalBalance

```rust
pub struct GlobalBalance {
    pub btc_balance_total:    f64,
    pub btc_balance_locked:   f64,
    pub btc_balance_full:     f64,
    pub special_coin_balance: f64,
}
```

All amounts are BTC equivalents. `special_coin_balance` is the server-selected
special coin balance, for example USDT on futures servers.

## API Surface

```rust
pub struct BalancesState {
    pub global:     GlobalBalance,
    pub by_market:  HashMap<String, BalanceItem>,
    pub last_epoch: u16, // diagnostic: last accepted balance packet epoch
}

impl BalancesState {
    pub fn new() -> Self;
    pub fn get(&self, market_name: &str) -> Option<&BalanceItem>;
    pub fn iter(&self) -> impl Iterator<Item = (&String, &BalanceItem)>;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
}
```
