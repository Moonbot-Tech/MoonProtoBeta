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
- `pos_size`, `pos_price`, `liq_price`, `pos_dir` (`OrderType::Sell` /
  `OrderType::Buy`, matching Delphi `FPosDir`);
- long/short hedge position fields;
- `asset_balance`, `asset_balance_full`;
- `total_profit_b`, `total_profit_l`, `total_profit_s`;
- `max_value`, `leverage_x`, `position_type` (`PositionType::Cross` /
  `PositionType::Isolated`);
- `balance_hash`, `last_balance_epoch`.

`BalancesState` remains the account-level/low-level balance view:

- global account totals in BTC equivalent;
- a secondary decoded per-market table for account/diagnostic panels;
- per-market epoch tracking so stale incremental rows are ignored.

Rows are keyed by market name, for example `"BTCUSDT"`.

Transferable wallet assets are a different state model. They are not chart
position fields. Use `client.balances().refresh_transfer_assets()` and
`snapshot().transfer_assets()` for the Spot/Futures/Quarterly asset lists used
by transfer UI.

Incoming balance rows are applied only for markets already known to
`MarketsState`. This matches the Delphi client: an unknown market name does not
create an orphan balance row.

Full snapshots are authoritative for the current known market universe. If a
known market is missing from a full snapshot, the library resets the live market
balance/position fields to zero and `leverage_x = 1`, while preserving
Delphi-preserved fields such as `balance_hash`, max value, and the
per-market last epoch.

Incremental updates merge only the changed rows and optionally update global
totals. Stale incremental rows are skipped per market.

## Reading Current State

```rust
use moonproto::{OrderType, PositionType};

let Some(state) = client.snapshot() else { return; };
let markets = state.markets();

if let Some(eth) = markets.get("ETHUSDT") {
    let pos = eth.balance_position();
    let side = if pos.pos_dir == OrderType::Buy { "long" } else { "short" };
    if pos.position_type == PositionType::Isolated {
        println!("isolated liquidation line at {}", pos.liq_price);
    }
    println!(
        "pos={} entry={} liq={} lev={}x",
        pos.pos_size,
        pos.pos_price,
        pos.liq_price,
        pos.leverage_x
    );
    println!("direction={side}");
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

Regular UI code calls `client.balances().refresh()` and reads
`snapshot().balances()` after
`Event::Balance`:

```rust
client.balances().refresh()?;

for event in client.drain_events() {
    if matches!(event, moonproto::Event::Balance(_)) {
        if let Some(snapshot) = client.snapshot() {
            println!("balance rows={}", snapshot.balances().len());
        }
    }
}
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

## Transferable Assets

Delphi refreshes `Markets.FAssets[EX_Spot]`, `Markets.FAssets[EX_Futures]`,
and `Markets.FAssets[EX_QFutures]` by starting one worker per wallet kind.
Rust Active Lib exposes the same user effect without blocking the caller:

```rust
use moonproto::{Event, ExchangeKind};

client.balances().refresh_transfer_assets()?;

for event in client.drain_events() {
    if let Event::TransferAssets(ev) = event {
        println!("transfer assets event: {ev:?}");
    }
}

if let Some(snapshot) = client.snapshot() {
    for asset in snapshot.transfer_assets().get(ExchangeKind::Futures) {
        println!(
            "{} transferable={} total={}",
            asset.currency,
            asset.amount,
            asset.total
        );
    }
}
```

`balances().refresh_transfer_assets()` queues all three Engine API requests and returns
immediately. Each completed response updates the library-owned state and emits a
per-wallet `Event::TransferAssets::Updated` or `UpdateFailed`. After all three
requests have answered, Active Lib emits `TransferAssetsEvent::RefreshCompleted`;
that is the UI-safe point equivalent to Delphi `WaitUpdCount = 0`. Use
`balances().refresh_transfer_assets_kind(kind)` if the UI only needs one wallet.

## Diagnostic Rows

The decoded per-market balance rows are still available for protocol tools and
account tables through `snapshot().balances()`. They are not the chart/order UI
surface. For selected-market UI, keep a `MarketHandle` and read
`balance_position()`: it is the same state Delphi mutates on `TMarket`.

Delphi behavior is preserved when applying `max_value`: a zero or near-zero
incoming value does not erase a previously known non-zero value on the live
market.

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

## API Shape

Normal UI state is:

- `snapshot().markets().get("ETHUSDT") -> MarketHandle`;
- `MarketHandle::balance_position()` for position, liquidation, leverage, PnL;
- `snapshot().balances().global()` for account totals;
- `snapshot().transfer_assets()` for transferable wallet assets.

The decoded secondary balance table is intentionally exposed only through
accessors and should stay out of chart/order hot paths.
