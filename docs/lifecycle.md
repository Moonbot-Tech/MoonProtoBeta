# Lifecycle Events

`LifecycleEvent` reports connection state and critical transport conditions.
Init is run once per `Client` session. Before that Init, transport handshakes do
not emit Engine API. After Init, reconnect restore refreshes market indexes only
when `PeerAppToken` changed, then sends the needed market refresh/subscription
replay, and replays only the registry subscriptions the application requested.

## Enum

```rust
pub enum LifecycleEvent {
    Connecting,
    Connected { fresh: bool },
    Ready,
    InitStepCompleted { step: &'static str, elapsed_ms: u64 },
    ConnectFailed { error: String },
    Disconnected,
    Reconnecting,
    BindFailed { consecutive_failures: u32 },
    ServerRestart,
}
```

## Handling

```rust
use moonproto::LifecycleEvent;

for event in client.drain_lifecycle_events() {
    match event {
        LifecycleEvent::Connecting => ui_status("connecting"),
        LifecycleEvent::Connected { fresh: true } => ui_status("connected"),
        LifecycleEvent::Connected { fresh: false } => ui_status("reconnected"),
        LifecycleEvent::Ready => ui_status("ready"),
        LifecycleEvent::InitStepCompleted { step, .. } => show_init_step(step),
        LifecycleEvent::ConnectFailed { error } => show_connect_error(error),
        LifecycleEvent::Reconnecting => ui_status("reconnecting"),
        LifecycleEvent::ServerRestart => ui_status("server restarted"),
        LifecycleEvent::Disconnected => ui_status("disconnected"),
        LifecycleEvent::BindFailed { consecutive_failures } => {
            show_network_alert(consecutive_failures);
        }
    }
}
```

## Semantics

| Event | Meaning | Application action |
|---|---|---|
| `Connecting` | A handshake attempt has started. | Update connection indicator. |
| `Connected { fresh: true }` | First successful authorization for this `Client`. | UI status only for `MoonClient`; low-level `Client` owners may run one-time init here. |
| `Connected { fresh: false }` | Re-handshake after reconnect. | UI only; the library refreshes stale indexes after a changed `PeerAppToken`, refreshes markets, and restores saved subscriptions. |
| `Ready` | `MoonClient` finished its one-time connect/init sequence and published the initial snapshot. | UI can treat the Active Lib state as initialized. |
| `InitStepCompleted { step, elapsed_ms }` | One mandatory startup step finished; `elapsed_ms` is total wall-clock time since runtime startup, not the duration of that single step. Current cold-init steps: `BaseCheck`, `AuthCheck`, `GetMarketsList`, `UpdateMarketsList`, `StrategySchema`, `PostInitFlush`, `StartupSnapshot`, or `StartupEvents`. | Optional progress display/diagnostics only. |
| `ConnectFailed { error }` | Background `MoonClient` startup failed. | Show the error and create a new client when the user retries. |
| `Reconnecting` | Traffic was silent long enough to trigger soft reconnect. | UI only. |
| `ServerRestart` | Server app token changed. | UI only; after reconnect the library refetches indexes before indexed streams/price refresh and replays saved subscriptions. |
| `Disconnected` | Explicit shutdown through `client.disconnect()`. | Treat the client as finished. |
| `BindFailed` | UDP bind failed across the full port-rotation range for at least 15 seconds; repeat events are throttled to about 50 seconds. | Show OS/network permission or port exhaustion alert. The library keeps retrying. |

## State Flow

```text
Base
  -> Connecting
  -> Connected { fresh: true }
  -> Ready
  -> [running]
  -> Reconnecting
  -> Connecting
  -> Connected { fresh: false }
```

`ServerRestart` is emitted during a successful handshake when the peer app token
changes. If the one-time Init has already completed, the following successful
reconnect restores required Engine API state automatically.

`Ready` is not a "all background data is fully loaded" barrier. It waits for
the mandatory init spine: authorization, BaseCheck/AuthCheck, markets list with
the initial server-index map, price refresh, and strategy schema. The schema
request can overlap the market/price requests, but `Ready` is still emitted
only after the schema is applied. Retained 5m candles, CoinCard candles,
transfer assets, stream
packets, and later refreshes report their own domain events after `Ready`.

## Callback Cost

Regular applications receive lifecycle events from `MoonClient` through the
configured `MoonEventSink`. The default queue adapter exposes
`drain_lifecycle_events` / `try_recv_lifecycle_event`; callback integrations can
post lifecycle events directly into the host UI loop. Low-level protocol tools
that own `Client` directly can still register `Client::on_lifecycle`; that
callback is not the normal desktop/UI integration path.
