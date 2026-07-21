# MoonProto Tests

The test suite is split by the layer it protects. Do not treat all tests as
application examples: some are intentionally low-level protocol guards.

## Public Pipeline Tests

- `integration_smoke.rs` is the small live `MoonClient` happy-path smoke test.
  It uses the same shape a desktop app should use: start `MoonClient`, wait for
  `LifecycleEvent::Ready`, read snapshots/events, then disconnect.
- `fire_test.rs` is the live health/stress gate. It also uses the public
  `MoonClient` path, but it enables diagnostics and destructive scenarios to
  prove protocol recovery, retained state, and CPU gates. It is not a sample app.

## Runtime / Protocol Unit Tests

Tests under `src/client/tests/` protect protocol mechanics: handshake,
reconnect, PMTU, send queues, pending Engine API routing, subscriptions, and
wire-compatible timing/retry semantics. They may instantiate internal `Client` or
`EventDispatcher` directly because their job is to lock down machine-effect
parity, not to demonstrate the public API.

Tests under `src/events/tests.rs` and `src/state/**/tests.rs` protect Active Lib
state application and retained read models. Direct state/dispatcher use here is
intentional: these tests prove exact parser/apply behavior without needing a live
server.

## Platform Polling Test

`udp_polling.rs` verifies the OS UDP readiness contract used by the runtime
loop. It is separate from live MoonProto tests because the failure mode is in
socket polling/rearming, not in server behavior.

## Running

Fast deterministic checks:

```powershell
cargo test --lib
cargo test --test udp_polling
cargo check --examples
```

Live smoke:

```powershell
cargo test --test integration_smoke -- --ignored --nocapture
```

FireTest:

```powershell
$env:MOONPROTO_FIRETEST_PROFILE = "quick"
cargo test --release --features diagnostics --test fire_test -- --ignored --nocapture
```

Quick FireTest is the frequent development gate. Full FireTest is the
destructive/stress gate for “this is a good point” decisions.
