# Multi-Server Connections

`moonproto` supports multiple independent server connections in one process.
Create one `MoonClient` per connection and keep events/snapshots tagged by the
application's session id.

## Basic Pattern

```rust
use moonproto::{ConnectConfig, InitConfig, MoonClient, TradesStreamMode};

struct Session {
    label: String,
    client: MoonClient,
}

let sessions: Vec<Session> = configs
    .into_iter()
    .map(|(label, cfg, init): (String, _, InitConfig)| {
        Ok(Session {
            label,
            client: MoonClient::connect(cfg, ConnectConfig::new(init))?,
        })
    })
    .collect::<Result<_, moonproto::MoonClientError>>()?;
```

Each `MoonClient` owns its runtime thread, UDP socket, reconnect state,
subscription registry, pending API registry, candle aggregators, runtime state
state, and server-time delta handle.

## Event Routing

Route events by the session that produced them:

```rust
for session in &sessions {
    for event in session.client.drain_events() {
        ui_queue.push((session.label.clone(), event));
    }
}
```

Use `session.client.snapshot()` for that same session's read model. Do not mix
snapshots from different servers.

## ServerTimeDelta Isolation

`MoonClient` automatically links each runtime state owner to the matching
client's `server_time_delta_handle`. That keeps order timestamps correct when
two servers have different clock drift.

## UI Subscriptions

Send intents to the matching session handle:

```rust
session.client.streams().subscribe_orderbook("BTCUSDT")?;
session.client.streams().subscribe_all_trades(TradesStreamMode::TradesOnly)?;
session.client.balances().refresh()?;
```

These subscriptions are per-client. Init flushes each registry once, and
reconnect restores each session independently.

## Server Identity and Exchange Flags

Each session exposes the server identity from its own `BaseCheck`/`AuthCheck`
through the `MoonClient` snapshot, so UI code never has to reach into the
low-level client:

```rust
if let Some(info) = session.client.server_info() {
    let on_binance = info.supports(moonproto::ExchangeTypeMask::SPOT);
    // info.bot_id, info.exchange_code, info.exchange_name,
    // info.exchange_type_mask, info.base_currency_name, ...
}

// Per-account metadata (account id, BTC address, sub-account flag, Hyperliquid
// DEX list) lands once the session authenticates:
if let Some(auth) = session.client.auth_info() {
    let sub = auth.is_sub_account;
}
```

The same values are reachable from a held snapshot via
`session.client.snapshot()?.server_info()` and `.auth_info()`. Before the first
`BaseCheck`, `server_info()` returns the all-empty default, and `auth_info()` is
`None` until authentication completes; both are always safe to read. Extended
exchange-specific UI should be enabled only after the corresponding server
metadata is known (for example, gate it on `info.has_identity()`).
