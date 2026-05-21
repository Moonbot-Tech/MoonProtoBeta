# MoonProto Client Initialization Sequence

This document describes the startup sequence used by the Rust client and its
Delphi reference points. The transport handshake is automatic. The application
chooses which Engine API init steps and subscriptions to request.

## Recommended Rust Entry Point

Use `connect_and_init` for the common setup path:

```rust
use std::time::Duration;
use moonproto::{connect_and_init, ConnectConfig, InitConfig};

let init = InitConfig {
    base_check: true,
    auth_check: true,
    fetch_markets: true,
    fetch_indexes: true,
    fetch_balance: true,
    subscribe_trades: Some(false),
    subscribe_orderbooks: vec!["BTCUSDT".to_string()],
    step_timeout: Some(Duration::from_secs(15)),
    ..Default::default()
};

let result = connect_and_init(
    &mut client,
    &mut dispatcher,
    ConnectConfig::new(init).with_connect_timeout(Duration::from_secs(15)),
)?;
```

`connect_and_init` first pumps `Client::run_with_dispatcher` until the transport
is authenticated, then calls `run_init_sequence`. Applications that need their
own progress UI can run the handshake phase themselves and call
`run_init_sequence(&mut client, &mut dispatcher, init)` after authorization.

## Phase 0: Transport Handshake

```text
Client -> Server: MPC_Hello     encrypted with MasterKey, AAD=ClientID
Server -> Client: MPC_WhoAreYou encrypted with MasterKey, AAD=ClientID
Client -> Server: MPC_ImFriend  encrypted with SessionKey, AAD=ClientID
Server -> Client: MPC_Fine      encrypted with MasterKey, AAD=ClientID
```

After `MPC_Fine`, the client is authorized, session keys are active, and the
Ping exchange starts. This phase is handled by `Client`; applications normally
observe it through `LifecycleEvent::Connected { fresh: true }` or through
`connect_and_init`.

## Phase 1: BaseCheck

```text
Client -> Server: TEngineRequest(emk_BaseCheck) [MPC_API, Sliced, encrypted]
Server -> Client: TEngineResponse(success=true)
```

`BaseCheck` verifies that the server engine is ready to answer requests. Newer
servers can also return identity fields such as bot id, server name, exchange,
and MoonProto version. `run_init_sequence` parses those fields into
`client.server_info()` when `InitConfig::base_check` is enabled.

In the Rust init helper this is a critical step: a timeout stops the init
sequence with `InitError::CriticalStepTimedOut("BaseCheck")`.

Timing follows `TMoonProtoEngine.BaseCheck`/`SendAndWait`: each attempt uses the
12s Engine API timeout. A normal init sends one BaseCheck. If a previous UI
command marked Delphi `ServerUpdateSent` (`ui_update_version`, `ui_switch_dex`,
`ui_switch_spot`, or manual `client.mark_server_update_sent()`), init first
waits up to `34 * 300ms` for `AuthDone`, clears the marker, then sends
BaseCheck once and retries it up to 10 more times with `2000ms` pauses.

## Phase 2: AuthCheck

```text
Client -> Server: TEngineRequest(emk_AuthCheck) [MPC_API, Sliced, encrypted]
Server -> Client: TEngineResponse(success=true, data=account metadata)
```

The response payload is parsed by `parse_auth_check_response` and can contain:

| Field | Type | Notes |
|---|---|---|
| `binance_account_id` | `i64` | Binance account id when applicable. |
| `btc_address` | string | Wallet or referral binding data. |
| `spot_ref` | `i32` | Historical spot referral field. |
| `is_sub_account` | bool | Whether the account is a sub-account. |
| `account_id` | string | Exchange account id or wallet address. |
| `recvd_max_payload` | `Option<i32>` | Present on newer servers. |
| `known_dexes` | `Vec<DexInfo>` | Optional Hyperliquid DEX metadata. |
| `hl_dex_market` | `Option<u8>` | Current futures DEX index. |
| `hl_spot_market` | `Option<u8>` | Current spot DEX index. |

In the Rust init helper this is also critical. A timeout stops the remaining
init steps because later requests are not useful without a valid authenticated
engine session.

## Phase 3: Market Catalog

```text
Client -> Server: TEngineRequest(emk_GetMarketsList) [MPC_API, Sliced, encrypted]
Server -> Client: TEngineResponse(success=true, data=markets, compressed=deflate)
```

`GetMarketsList` returns the full market catalog. The Engine response parser
DEFLATE-decompresses `Data` when the response has `IsCompressed=true`, and
`EventDispatcher` applies the decoded payload to `MarketsState`.

`UpdateMarketsList` is a separate Engine method for price, funding, mark price,
and correlation updates. It is not one of the `run_init_sequence` steps. During
the long-running client loop, `ClientConfig::refresh` sends
`UpdateMarketsList` every two seconds by default and `CheckBinanceTags` every
sixty seconds by default.

Market fetch failures are non-critical in `run_init_sequence`: the error is
recorded in `InitResult::errors`, and init continues.

## Phase 4: Market Indexes

```text
Client -> Server: TEngineRequest(emk_GetMarketsIndexes) [MPC_API, Sliced, encrypted]
Server -> Client: TEngineResponse(success=true, data=market names by server index)
```

`InitConfig::fetch_indexes` requests the initial server index map. The helper
also forces this step when `subscribe_trades` or `subscribe_orderbooks` is set,
because trades and orderbook packets are gated until indexes are synchronized
for the current `PeerAppToken`.

Index fetch failures are non-critical in `run_init_sequence`; the error is
recorded in `InitResult::errors`, and init continues.

## Phase 5: Balance Refresh

```text
Client -> Server: TEngineRequest(emk_GetMarketsBalanceFull) [MPC_API, Sliced, encrypted]
Server -> Client: TEngineResponse(success=true)
```

The current Delphi server refreshes balances server-side for this request but
does not serialize a full balance payload into the response yet, so successful
responses usually have empty `Data`. Balance state updates arrive through the
Balance channel, and one-shot balance reads should use helpers such as
`Client::request_balance`.

Balance refresh failures are non-critical in `run_init_sequence` and are added
to `InitResult::errors`.

## Phase 6: Stream Subscriptions

```text
Client -> Server: TEngineRequest(emk_SubscribeAllTrades, params=want_mm)
Client -> Server: TEngineRequest(emk_SubscribeOrderBook, market_names=[...])
```

`subscribe_all_trades` and `subscribe_orderbook` are fire-and-forget
subscription intents. The client stores them in its subscription registry and
replays them after a hard reconnect or server-token change.

`run_init_sequence` drains the client loop briefly after registering
subscriptions so that the queued subscription commands are sent before the
helper returns. Applications should not resend these subscriptions manually on
reconnect; the library owns that recovery work.

## Engine Request Waiting

The Delphi client implements `SendAndWait` by creating a `TPendingRequest`,
sending the request as a sliced encrypted MoonProto command, and polling the
pending response until timeout.

The Rust client implements the same flow with a pending registry keyed by the
request UID:

- `Client::api_*` methods create the request and return a
  `Receiver<EngineResponse>`.
- `Client::run_until_response` keeps the UDP loop and `EventDispatcher` running
  while it waits for that receiver.
- `Client::request_engine_response` is the lower-level helper used by
  `run_init_sequence`.
- Typed helpers such as `request_base_check`, `request_auth_check`, and
  `request_balance` wrap this path and parse the response payload.

Do not block the same thread with `rx.recv_timeout(...)` while it owns the
`Client`. The client loop must continue running so that UDP packets, sliced
responses, decryption, and pending response routing can progress.

## Delphi Reference Points

- `Unit1.pas:4987` - `TCryptoPumpTool.InitInt` startup ordering.
- `MoonProtoEngine.pas:514` - `SendAndWait` polling loop.
- `MoonProtoEngine.pas:563-922` - BaseCheck, AuthCheck, GetMarketsList,
  UpdateMarketsList, and GetMarketsBalanceFull.
- `MoonProtoEngine.pas:267` - SubscribeAllTrades.
- `MoonProtoServer.pas:1043` - server-side `emk_SubscribeAllTrades` handling.
- `MoonProtoClient.pas:256-411` - client data dispatch.
- `MoonProtoClient.pas:802` - API response matching to pending requests.
