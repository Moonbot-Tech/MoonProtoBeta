# Active Lib Contract

`MoonClient` is the normal application API. It owns a runtime thread and keeps
MoonProto alive until `stop()` or drop. Applications choose subscriptions and
send commands; the library maintains protocol and trading state.

## Session

- Connects, authorizes, and runs Init once per session.
- Keeps reconnect, re-handshake, Sliced ACK/retry, PMTU, and pending Engine API
  routing alive in the background.
- Does not ask applications to choose a protocol-loop duration.
- Blocks indexed streams while market indexes are stale after reconnect.

## Maintained State

- Markets, market indexes, prices, tags, funding, mark price, and listing
  refresh.
- Per-market chart-visible fields: position size/entry/liquidation/leverage,
  balance fields, arb slots, last trade tail, LastPrice line, MarkPrice line,
  retained trades, retained 5m candles, and derived volume/delta snapshots when
  trades storage is enabled.
- Orders and order traces, including local stateful effects for move/cancel,
  stops, vstop, panic, immune, and snapshot cleanup.
- Strategy schema and strategy snapshots. Applications can provide local
  strategies before Init; the runtime answers server snapshot requests from its
  owned strategy state.
- Settings, lifecycle events, Engine API responses, and server logs.

## Subscriptions

- Orderbook subscription intent is registry-aware. Reconnect restores the latest
  requested set and requests full orderbooks when diff recovery needs it.
- Trades subscription is explicit in the Rust API. `TradesStreamMode` chooses
  trades-only vs trades plus market-maker sections. `subscribe_all_trades`
  stores and calculates for all markets. `subscribe_trades_for` sends the same
  server subscription but retains/calculates only the selected markets; an empty
  list means all markets.
- When trades storage is enabled, the runtime also requests the initial 5m
  candles snapshot and then maintains the current candle from trades.

## UI Shape

- UI reads immutable snapshots and stable handles.
- UI sends intents through `MoonClient`, `client.orders()`, and
  `client.trade()`.
- Stateful order actions are marshalled to the runtime owner; application code
  does not mutate `Orders`.
- Time fields inherited from Delphi are day values, not Unix timestamps. Use
  `DelphiTime` helpers such as `row.time_delphi().unix_millis()`.

Low-level `Client`, `EventDispatcher`, `commands::*`, and `state::*` remain
available for tests, diagnostics, and custom runtimes, but regular applications
should start from `MoonClient`.
