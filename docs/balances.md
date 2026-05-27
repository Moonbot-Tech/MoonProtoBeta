# Balances

The Active Lib keeps live account and position state for the connected MoonBot
session. Application code normally does not parse balance packets directly.

For UI attached to a market chart, read position fields from `Market`, not from
a separate balance row. This mirrors Delphi: balance packets mutate the live
`TMarket` object, and chart/order UI reads that object.

## What The Library Maintains

`MarketsState` stores per-market balance/position fields directly on each live
`Market`:

- `initial_balance`, `locked_balance`;
- `pos_size`, `pos_price`, `liq_price`, `pos_dir`;
- long/short hedge position fields;
- `asset_balance`, `asset_balance_full`;
- `total_profit_b`, `total_profit_l`, `total_profit_s`;
- `bn_max_value`, `leverage_x`, `position_type`;
- `balance_hash`, `last_balance_epoch`.

`BalancesState` remains the account-level/low-level balance view:

- global account totals in BTC equivalent;
- one `BalanceItem` per known market;
- per-market epoch tracking so stale incremental rows are ignored.

Rows are keyed by market name, for example `"BTCUSDT"`.

Incoming balance rows are applied only for markets already known to
`MarketsState`. This matches the Delphi client: an unknown market name does not
create an orphan balance row.

Full snapshots are authoritative for the current known market universe. If a
known market is missing from a full snapshot, the library resets the live market
balance/position fields to zero and `leverage_x = 1`, while preserving
Delphi-preserved fields such as `balance_hash`, `bn_max_value`, and the
per-market last epoch.

Incremental updates merge only the changed rows and optionally update global
totals. Stale incremental rows are skipped per market.

## Reading Current State

```rust
let Some(state) = client.snapshot() else { return; };
let markets = state.markets();

if let Some(eth) = markets.get("ETHUSDT") {
    let pos = eth.balance_position();
    println!(
        "pos={} entry={} liq={} lev={}x",
        pos.pos_size,
        pos.pos_price,
        pos.liq_price,
        pos.leverage_x
    );
}

let global = state.balances().global();
println!(
    "btc_total={} locked={} full={}",
    global.btc_balance_total,
    global.btc_balance_locked,
    global.btc_balance_full
);
```

The snapshot is immutable and safe for UI code to keep. `MarketHandle` values
are stable across listing refreshes, so a chart can keep the handle for the
selected market and read fresh fields from it. Use `MarketHandle::with` for
zero-copy reads of many market fields, or `MarketHandle::balance_position` when
the UI only needs the live balance/position subset.

## Getting A Fresh Snapshot

Use `MoonClient::request_balance_snapshot` when the application needs a fresh
full snapshot before continuing:

```rust
let balances = client.request_balance_snapshot(std::time::Duration::from_secs(15))?;

println!("balance rows={}", balances.len());
```

The helper sends the library-level balance refresh request, keeps the runtime
pumping MoonProto, waits for the next full snapshot event, and returns a cloned
`BalancesState`.

For fire-and-forget refresh in the normal runtime:

```rust
client.refresh_balances()?;
```

The next snapshot arrives through the normal `MoonClient` event/snapshot path.

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

## Low-Level Rows

`BalanceItem` is the decoded packet row and the secondary balance-table view.
It is useful for diagnostics and account tables. Market chart/order UI should
normally read the same position/liquidation fields from `MarketHandle`, not from
`BalanceItem`.

`BalanceItem::max_value` follows Delphi behavior: a zero or near-zero incoming
value does not erase a previously known non-zero value on the live market.

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
impl BalancesState {
    pub fn global(&self) -> &GlobalBalance;
    pub fn get(&self, market_name: &str) -> Option<&BalanceItem>;
    pub fn iter(&self) -> impl Iterator<Item = (&String, &BalanceItem)>;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
}
```

The public struct still keeps the decoded secondary table for compatibility and
diagnostics, but chart UI should prefer `MarketHandle::balance_position()`.
