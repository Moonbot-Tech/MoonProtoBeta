# Lifecycle Events

`LifecycleEvent` reports connection state and critical transport conditions.
Init is run once per `Client` session. Before that Init, transport handshakes do
not emit Engine API. After Init, reconnect restore refreshes market indexes,
then sends `UpdateMarketsList`, and replays only the registry subscriptions the
application requested.

## Enum

```rust
pub enum LifecycleEvent {
    Connecting,
    Connected { fresh: bool },
    Disconnected,
    Reconnecting,
    BindFailed { consecutive_failures: u32 },
    ServerRestart,
}
```

## Handling

```rust
use moonproto::LifecycleEvent;

client.on_lifecycle(Box::new(|event| match event {
    LifecycleEvent::Connecting => ui_status("connecting"),
    LifecycleEvent::Connected { fresh: true } => ui_status("connected"),
    LifecycleEvent::Connected { fresh: false } => ui_status("reconnected"),
    LifecycleEvent::Reconnecting => ui_status("reconnecting"),
    LifecycleEvent::ServerRestart => ui_status("server restarted"),
    LifecycleEvent::Disconnected => ui_status("disconnected"),
    LifecycleEvent::BindFailed { consecutive_failures } => {
        show_network_alert(consecutive_failures);
    }
}));
```

## Semantics

| Event | Meaning | Application action |
|---|---|---|
| `Connecting` | A handshake attempt has started. | Update connection indicator. |
| `Connected { fresh: true }` | First successful authorization for this `Client`. | Run the one-time init if not already done. |
| `Connected { fresh: false }` | Re-handshake after reconnect. | UI only; the library restores indexes/market refresh and saved subscriptions. |
| `Reconnecting` | Traffic was silent long enough to trigger soft reconnect. | UI only. |
| `ServerRestart` | Server app token changed. | UI only; after reconnect the library refetches indexes, refreshes markets, and replays saved subscriptions. |
| `Disconnected` | Explicit shutdown through `client.disconnect()`. | Treat the client as finished. |
| `BindFailed` | UDP bind failed across the full port-rotation range for at least 15 seconds; repeat events are throttled to about 50 seconds. | Show OS/network permission or port exhaustion alert. The library keeps retrying. |

## State Flow

```text
Base
  -> Connecting
  -> Connected { fresh: true }
  -> [running]
  -> Reconnecting
  -> Connecting
  -> Connected { fresh: false }
```

`ServerRestart` is emitted during a successful handshake when the peer app token
changes. If the one-time Init has already completed, the following successful
reconnect restores required Engine API state automatically.

## Callback Cost

Lifecycle callbacks run on the same thread as the client main loop. Keep them
short and pass heavy work to another thread or queue.
