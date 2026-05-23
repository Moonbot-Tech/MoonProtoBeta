# Arb Payloads

Arbitrage price updates arrive as `MPC_Balance` subcommand `6`. The public
library parses the MoonProto envelope and decodes the compact kernel-to-client
payload into price or isolation entries.

The server sends this channel only to clients that are balance-subscribed. In
the normal active-library flow this is handled by init: `connect_and_init` /
`run_init_sequence` sends `UpdateMarketsList`, which enables balance-channel
delivery on the Delphi server. A raw transport-only client that has not run init
should not expect `Event::Arb` yet.

## EventDispatcher Path

`EventDispatcher` is the active-library path. It matches the Delphi client:
price blocks and isolation entries whose `market_index` is not present in the
current server-index map are consumed but not exposed in `Event::Arb`.
Use `parse_arb_payload_compact` directly when you need raw wire inspection.

```rust
use moonproto::Event;
use moonproto::commands::arb::ArbPayload;

client.run_with_dispatcher(duration, &mut dispatcher, Box::new(|event| {
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
}));
```

## Low-Level Parser

```rust
use moonproto::commands::arb::{ArbPayload, parse_arb_payload_compact, parse_arb_prices};

let arb = parse_arb_prices(payload).expect("bad arb payload");
let compact = parse_arb_payload_compact(&arb.payload).expect("bad compact payload");
if let ArbPayload::Price { blocks, .. } = compact {
    println!("uid={} markets={}", arb.uid, blocks.len());
}
```

## Public Struct

```rust
pub struct ArbPricesCommand {
    pub uid: u64,
    pub payload: Vec<u8>,
}

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

`version <= 2` has an implicit price command. `version >= 3` carries an explicit
command byte: `1` for prices and `2` for isolation flags.
