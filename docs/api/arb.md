# Arb Payloads

Arbitrage price updates arrive as `MPC_Balance` subcommand `6`. The public
library parses the MoonProto envelope and exposes the compact arb payload as raw
bytes.

## EventDispatcher Path

```rust
use moonproto::Event;

client.run_with_dispatcher(duration, &mut dispatcher, Box::new(|event| {
    if let Event::Arb { uid, payload } = event {
        println!("arb update uid={uid} bytes={}", payload.len());
    }
}));
```

## Low-Level Parser

```rust
use moonproto::commands::arb::parse_arb_prices;

let arb = parse_arb_prices(payload).expect("bad arb payload");
println!("uid={} bytes={}", arb.uid, arb.payload.len());
```

## Public Struct

```rust
pub struct ArbPricesCommand {
    pub uid: u64,
    pub payload: Vec<u8>,
}
```

The inner compact arb table is server-specific application data. `moonproto`
keeps it raw until a stable public consumer contract for that table is required.
