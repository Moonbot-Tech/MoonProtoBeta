# Lifecycle Events

`LifecycleEvent` reports connection state and critical transport conditions. Most
events are informational: the library already performs reconnect, resubscribe,
market-index resync, and stream recovery.

## Enum

```rust
pub enum LifecycleEvent {
    Connecting,
    Connected { fresh: bool },
    Disconnected,
    Reconnecting,
    SendBacklogCritical { cmd: u8, u_key_uid: u64 },
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
    LifecycleEvent::SendBacklogCritical { u_key_uid, .. } => {
        show_trading_alert(u_key_uid);
    }
    LifecycleEvent::BindFailed { consecutive_failures } => {
        show_network_alert(consecutive_failures);
    }
}));
```

## Semantics

| Event | Meaning | Application action |
|---|---|---|
| `Connecting` | A handshake attempt has started. | Update connection indicator. |
| `Connected { fresh: true }` | First successful authorization for this `Client`. | Run initial subscriptions/init if not already done. |
| `Connected { fresh: false }` | Re-handshake after reconnect. | UI only. The library replays subscriptions. |
| `Reconnecting` | Traffic was silent long enough to trigger soft reconnect. | UI only. |
| `ServerRestart` | Server app token changed. | UI only. The library refetches indexes and replays subscriptions. |
| `Disconnected` | Explicit shutdown through `client.disconnect()`. | Treat the client as finished. |
| `SendBacklogCritical` | Pending high-priority send queue overflowed and an old command was dropped. | Show a trading-risk alert; retry only if your app can prove the intended command is still needed. |
| `BindFailed` | UDP bind failed across the full port-rotation range repeatedly. | Show OS/network permission or port exhaustion alert. The library keeps retrying. |

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

`ServerRestart` is an informational event emitted during a successful handshake
when the peer app token changes. It does not require the application to clear
state or resubscribe.

## Callback Cost

Lifecycle callbacks run on the same thread as the client main loop. Keep them
short and pass heavy work to another thread or queue.
