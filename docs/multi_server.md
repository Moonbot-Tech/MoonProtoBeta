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
subscription registry, pending API registry, candle aggregators, dispatcher
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

`MoonClient` automatically links each runtime dispatcher to the matching
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

## Exchange Type Flags

If UI code needs server identity/exchange flags, read them from the `MoonClient`
snapshot/lifecycle state for that session, or keep a user-selected label for
each session. Extended exchange-specific UI should be enabled only after the
corresponding server metadata is known.
