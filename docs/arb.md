# Arbitrage State

Arbitrage relay packets are low-level transport details. The normal Active Lib
state is per market.

When the current client settings enable an arbitrage platform, incoming compact
arb prices are applied to the live `Market` object:

```rust
let Some(state) = client.snapshot() else { return; };

if let Some(btc) = state.markets().get("BTCUSDT") {
    btc.with(|m| {
        if let Some(slot) = m.arb_slots.get(&7) {
            println!("price={} isolated={}", slot.now.price, slot.isolated_flags);
        }
    });
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

## Low-Level Events

`Event::Arb { uid, payload }` still exists as a diagnostic/protocol event.
Its `ArbPayload` contains compact packet blocks with server `market_index`
values. Do not build chart UI around those indexes; use the selected
`MarketHandle` and `Market::arb_slots` instead.
