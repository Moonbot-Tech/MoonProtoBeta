# Arbitrage Relay Events

The Active Lib can expose compact arbitrage price and isolation relay messages
as `Event::Arb`. This is not part of the normal balance read model, even though
the Delphi protocol transports it on the same internal channel as balances.

In the normal active-library flow, init enables this delivery together with the
balance/market refresh path. A raw transport-only client that has not completed
init should not expect arbitrage relay events.

## EventDispatcher Path

`EventDispatcher` filters relay records through the current server market-index
map. Records for unknown indexes are consumed but not exposed, matching the
Delphi client.

```rust
use moonproto::commands::arb::ArbPayload;
use moonproto::Event;

for event in client.drain_events() {
    if let Event::Arb { uid, payload } = event {
        match payload {
            ArbPayload::Price { blocks, .. } => {
                println!("arb prices uid={uid} markets={}", blocks.len());
            }
            ArbPayload::Isolation { entries, .. } => {
                println!("arb isolation uid={uid} entries={}", entries.len());
            }
        }
    }
}
```

## Public Types

```rust
pub enum ArbPayload {
    Price { version: u8, blocks: Vec<ArbPriceBlock> },
    Isolation { version: u8, entries: Vec<ArbIsolationEntry> },
}

pub struct ArbPriceBlock {
    pub market_index: u16,
    pub prices: Vec<ArbPriceItem>,
}

pub struct ArbPriceItem {
    pub platform_code: u8,
    pub price: f32,
}

pub struct ArbIsolationEntry {
    pub market_index: u16,
    pub platform_code: u8,
    pub flags: u8,
}
```
