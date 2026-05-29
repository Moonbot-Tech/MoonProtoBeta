# Arbitrage State

Arbitrage relay packets are low-level transport details. The normal Active Lib
state is per market.

When the current client settings enable an arbitrage platform, incoming compact
arb prices are applied to the live `Market` object:

```rust
use moonproto::ArbPlatformCode;

let Some(state) = client.snapshot() else { return; };

if let Some(btc) = state.markets().get("BTCUSDT") {
    if let Some(slot) = btc.arb_slot(ArbPlatformCode::ByBit) {
        println!(
            "price={} deposit_blocked={} withdraw_blocked={}",
            slot.now.price,
            slot.isolated_flags.deposit_blocked(),
            slot.isolated_flags.withdraw_blocked()
        );
    }
}
```

Arb slots are keyed by `ArbPlatformCode`. Each slot mirrors the useful parts of
Delphi `TMarket.ArbSlots` / `TMarket.ArbNow`; the temporary mark-and-sweep
staging byte is not public API:

```rust
pub struct MarketArbSlot {
    pub enabled: bool,
    pub isolated_flags: ArbIsolationFlags,
    pub now: MarketArbNowEntry,
}
```

Isolation snapshots are committed like Delphi: received temporary flags replace
the current `isolated_flags`, then the temporary staging flags are cleared.
Use `MarketHandle::arb_now(ArbPlatformCode::...)` when the UI only needs the
latest price/time. If the UI needs the 10-point Delphi ring, use
`MarketArbSlot::points_oldest_first()`; the raw storage ring and cursor are
internal.

## Events

`Event::Arb(ArbEvent)` is a signal/summary that compact arb data was applied.
It intentionally does not expose raw server `market_index` blocks as the normal
UI surface. Do not build chart UI around packet indexes; use the selected
`MarketHandle::arb_slot` / `arb_now` instead.
