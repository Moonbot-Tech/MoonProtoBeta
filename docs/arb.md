# Arbitrage State

Arbitrage relay packets are low-level transport details. The normal Active Lib
state is per market.

When the current client settings enable an arbitrage platform, incoming compact
arb prices are applied to the live `Market` object:

```rust
let Some(state) = client.snapshot() else { return; };

if let Some(btc) = state.markets().get("BTCUSDT") {
    if let Some(slot) = btc.arb_slot(7) {
        println!("price={} isolated={}", slot.now.price, slot.isolated_flags);
    }
}
```

`Market::arb_slots` is keyed by platform code. Each slot mirrors the useful
parts of Delphi `TMarket.ArbSlots` / `TMarket.ArbNow`:

```rust
pub struct MarketArbSlot {
    pub ring: [MarketArbPricePoint; 10],
    pub enabled: bool,
    pub head: u8,
    pub isolated_flags: u8,
    pub now: MarketArbNowEntry,
}
```

Isolation snapshots are committed like Delphi: received temporary flags replace
the current `isolated_flags`, then the temporary staging flags are cleared.
Use `MarketHandle::arb_now(platform_code)` when the UI only needs the latest
price/time and not the 10-point Delphi ring.

## Events

`Event::Arb(ArbEvent)` is a signal/summary that compact arb data was applied.
It intentionally does not expose raw server `market_index` blocks as the normal
UI surface. Do not build chart UI around packet indexes; use the selected
`MarketHandle::arb_slot` / `arb_now` instead.
