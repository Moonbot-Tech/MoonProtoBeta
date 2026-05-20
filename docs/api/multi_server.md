# Multi-Server Connections

`moonproto` supports multiple independent server connections in one process.
Create one `Client` and one `EventDispatcher` per connection. Do not share a
dispatcher between clients.

## Basic Pattern

```rust
use moonproto::{Client, ClientConfig, EventDispatcher};

struct Session {
    client: Client,
    dispatcher: EventDispatcher,
}

let mut sessions: Vec<Session> = configs
    .into_iter()
    .map(|cfg: ClientConfig| Session {
        client: Client::new(cfg),
        dispatcher: EventDispatcher::new(),
    })
    .collect();
```

Each `Client` owns its socket, reader thread, subscription registry, pending API
registry, candle aggregators, server-time delta handle, and server identity.

## Identity

Run `BaseCheck` during init to fill `client.server_info()`:

```rust
let init = InitConfig {
    base_check: true,
    auth_check: true,
    fetch_markets: true,
    ..Default::default()
};
run_init_sequence(&mut session.client, &mut session.dispatcher, init)?;

let info = session.client.server_info();
let label = info.server_name.as_deref().unwrap_or("Server");
let bot_id = info.bot_id.unwrap_or(0);
println!("{label} bot_id={bot_id}");
```

Older servers may return no identity payload. In that case `has_identity()` is
false and all fields are `None`; use the configured address or a user-provided
label as the UI key.

## Event Routing

Keep a stable application id per session:

```rust
let server_key = session
    .client
    .server_info()
    .bot_id
    .map(|id| id.to_string())
    .unwrap_or_else(|| configured_label.clone());
```

When running clients on separate threads, send `(server_key, event)` or a UI
message derived from the event into your application queue.

## ServerTimeDelta Isolation

`Client::run_with_dispatcher` and `run_with_dispatcher_state` automatically link
the dispatcher to the matching client's `server_time_delta_handle`. That keeps
order timestamps correct when two servers have different clock drift.

For custom loops:

```rust
dispatcher.set_server_time_delta_source(client.server_time_delta_handle());
```

Without this link, low-level `dispatch_into` falls back to a process-global value
kept for single-client compatibility.

## Thread-Safe UI Subscriptions

Clone a typed sender for UI threads:

```rust
let sender = session.client.sender();

ui_thread.spawn(move || {
    sender.subscribe_orderbook("BTCUSDT");
    sender.subscribe_all_trades(false);
});
```

These subscriptions are per-client. Reconnect replay also happens per-client.

## Exchange Type Flags

`ServerInfo::exchange_type_mask` is a bitmask:

```rust
use moonproto::commands::engine_api::exchange_type_flags;

if info.supports(exchange_type_flags::SPOT) {}
if info.supports(exchange_type_flags::FUTURES) {}
if info.supports(exchange_type_flags::DEX) {}
if info.supports(exchange_type_flags::PREDICT) {}
```

Use it to enable or hide exchange-specific UI actions.
