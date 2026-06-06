# Active Lib Contract

`MoonClient` is the normal application API. It owns a runtime thread and keeps
MoonProto alive until `disconnect()` or drop. `connect` returns immediately;
`LifecycleEvent::Ready` means the one-time Init finished. Applications choose
subscriptions and send commands; the library maintains protocol and trading
state.

## Session

- Connects, authorizes, and runs Init once per session.
- Keeps reconnect, re-handshake, Sliced ACK/retry, PMTU, and pending Engine API
  routing alive in the background.
- Handles Path MTU probing internally. A too-large `SizeAck`/`ProbeMTUAck`
  result is an expected failed probe; regular application payloads are sent
  through the normal Sliced/retry machinery when they do not fit one datagram.
- Does not ask applications to choose a protocol-loop duration.
- Blocks indexed streams while market indexes are stale after reconnect.

## Maintained State

- Markets, market indexes, prices, tags, funding, mark price, and listing
  refresh.
- Per-market chart-visible fields: position size/entry/liquidation/leverage,
  balance fields, arb slots, last trade tail, LastPrice line, MarkPrice line,
  retained trades, retained 5m candles, unprotected-position warning inputs,
  signed BTC/exchange signal deltas, and derived volume/delta snapshots when
  trades storage is enabled. The local
  `settings().set_exclude_blacklisted_markets_from_exchange_delta(...)` policy
  mirrors Delphi `ExcludeBlackListDelta` for the exchange-delta aggregate.
- Transferable wallet assets for Spot, Futures, and Quarterly wallets. These
  are separate from per-market balances and are refreshed by an explicit async
  Active Lib command.
- Account-level scalar state such as hedge mode and API-key expiration. UI
  queues async refresh intents and reads `snapshot().account()` after
  `Event::Account`.
- Orders and order traces, including local stateful effects for move/cancel,
  stops, vstop, panic, immune, and snapshot cleanup.
- Strategy schema and strategy snapshots. Applications can provide local
  strategies before Init; the runtime answers server snapshot requests from its
  owned strategy state. If the request arrives before Init opens the domain
  gate, it is latched and answered during post-init resync after schema/state
  are ready.
- Thin-terminal state from the kernel: detect facts, watcher rows, chart-alert
  fires, accepted armed chart-alert objects, and ready chart text rows. The
  terminal displays these facts; it does not run the kernel detect engine or
  rebuild chart text locally.
- Settings, lifecycle events, Engine API responses, and server logs.

## Subscriptions

- Orderbook subscription intent is registry-aware. Reconnect restores the latest
  requested set and requests full orderbooks when diff recovery needs it.
- Trades subscription is explicit in the Rust API. `TradesStreamMode` chooses
  trades-only vs trades plus market-maker sections.
  `streams().subscribe_all_trades` stores and calculates for all markets.
  `streams().subscribe_trades_for` sends the same
  server subscription but retains/calculates only the selected markets; an empty
  list means all markets.
- When trades storage is enabled, the runtime requests the initial 5m candles
  snapshot once for the active storage scope, emits `Event::CandlesSnapshot`
  only after the history worker has applied it, and then maintains the current
  candle from trades.

## UI Shape

- UI reads immutable snapshots and stable handles.
- UI sends intents through domain handles such as `client.streams()`,
  `client.orders()`, `client.trade()`, `client.balances()`, and
  `client.settings()`.
- Stateful order actions are marshalled to the runtime owner; application code
  does not mutate `Orders`.
- Asset-transfer UI calls `client.balances().refresh_transfer_assets()` and then reads
  `snapshot().transfer_assets()`. The command returns after queuing all wallet
  refresh requests; `Event::TransferAssets` reports each completed wallet and a
  final `RefreshCompleted` event after all requested wallet kinds have answered.
- Time fields exposed to applications use `MoonTime`. Use
  `row.time().unix_millis()` or `row.time().system_time()` for UI labels.
- Chart-alert and chart-text UI uses `client.terminal()` for user intents and
  `snapshot().thin_terminal()` for retained state.

Regular applications should start from `MoonClient`: it owns the protocol loop,
event sink, and retained state.
