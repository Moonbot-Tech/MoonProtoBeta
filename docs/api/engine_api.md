# Engine API

`MPC_API` is the request/response RPC channel between the client and the server
engine. Requests and responses are correlated by `request_uid`.

Use typed `Client::request_*` helpers for common one-shot reads. Use
`Client::api_*` wrappers when you need the raw `EngineResponse` receiver for a
custom asynchronous flow.

The source-matched default wait is 12 seconds per Engine API response:
Delphi `TMoonProtoEngine.FTimeout = 12000` and `SendAndWait` sleeps in 10 ms
ticks until that timeout expires. `run_init_sequence` uses this default when
`InitConfig::step_timeout` is `None`.

## Waiting for a Response

```rust
use std::time::Duration;

let qty = client.request_balance(&mut dispatcher, "USDT", Duration::from_secs(12))?;
let hedge_mode = client.request_hedge_mode(&mut dispatcher, Duration::from_secs(12))?;
let api_expiration = client.request_api_expiration_time(&mut dispatcher, Duration::from_secs(12))?;
let candles = client.request_candles_data(&mut dispatcher, Duration::from_secs(30))?;
```

The one-shot helpers keep pumping the UDP loop through short
`run_with_dispatcher` ticks, validate `EngineResponse::success`, and parse the
method-specific payload. They return `EngineRequestError`.
Any other events produced during that wait are queued in
`EventDispatcher::queued_events()`; call `take_queued_events()` after the helper
when the application has live subscriptions and needs the notifications.

For custom flows, use the lower-level receiver path:

```rust
let rx = client.api_get_markets_list();
let resp = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(12))?;
```

Calling `rx.recv_timeout(...)` directly on the same thread usually times out
because no UDP packets are processed during that wait.

For custom raw payloads with caller-owned timeout cleanup, call
`Client::request_engine_response`. Raw `api_*` receivers keep their pending slot
until a matching response arrives, a reconnect clears the session, or the same
UID is registered again.

## Client Wrappers

| Group | Methods |
|---|---|
| One-shot typed reads | `request_base_check`, `request_auth_check`, `request_balance`, `request_hedge_mode`, `request_api_expiration_time`, `request_coin_card_candles` |
| Init/auth | `api_base_check`, `api_auth_check` |
| Markets | `api_get_markets_list`, `api_get_markets_indexes`, `api_update_markets_list`, `api_check_binance_tags` |
| Balance | `api_get_balance(currency)`, `api_get_markets_balance_full` |
| Orders | `api_cancel_all_orders` |
| Account settings | `api_set_leverage(market, lev)`, `api_set_hedge_mode(bool)`, `api_query_hedge_mode`, `api_check_expiration_time` |
| Trades | `api_subscribe_all_trades(want_mm_orders)`, `api_unsubscribe_all_trades`, `api_trades_resend_batches(packet_nums)` |
| Orderbooks | `api_subscribe_order_book(markets)`, `api_unsubscribe_order_book(markets)`, `api_request_order_book_full(market_idx, kind)`, `api_reload_order_book` |
| Position/transfer | `api_change_position_type`, `api_convert_dust_bnb`, `api_confirm_risk_limit`, `api_set_ma_mode`, `api_do_transfer_asset`, `api_update_transfer_assets` |
| Candles | `request_coin_card_candles`, `request_candles_data`, `api_get_coin_card_candles`, `api_request_candles_data_async` |

For subscriptions, prefer the registry-aware APIs:

```rust
client.subscribe_all_trades(false);
client.subscribe_orderbook("BTCUSDT");
```

Those APIs are replayed automatically after reconnect. Raw `api_subscribe_*`
calls are useful for custom tools but do not update the subscription registry.

## Balance

`request_balance(currency)` returns the current quantity for one currency:

```rust
let qty = client.request_balance(&mut dispatcher, "USDT", Duration::from_secs(12))?;
println!("USDT balance={qty}");
```

## Account Settings

`request_hedge_mode()` returns the current hedge-mode flag:

```rust
let hedge_mode = client.request_hedge_mode(&mut dispatcher, Duration::from_secs(12))?;
println!("hedge_mode={hedge_mode}");
```

`request_api_expiration_time()` returns an `ApiExpirationTime` wrapper around
the server's Delphi `TDateTime` value and exposes `system_time()`,
`unix_seconds()`, and `days_until(...)` helpers:

```rust
let expiration = client.request_api_expiration_time(&mut dispatcher, Duration::from_secs(12))?;
if let Some(unix) = expiration.unix_seconds() {
    println!("API key expires at unix_seconds={unix}");
}
```

For raw payload access, `api_get_balance`, `api_query_hedge_mode`, and
`api_check_expiration_time` remain available and return
`Receiver<EngineResponse>`.

`request_candles_data` is the high-level API for
`emk_RequestCandlesData`. It registers the chunk aggregator, keeps the client
loop running, and returns one `MergedCandles` value after all chunks are merged.
Use `api_request_candles_data_async` only for custom async flows that already
own a running client loop.

`api_get_markets_balance_full` is intentionally low-level. The current Delphi
server calls `Engine.GetMarketsBalanceFull`, but does not serialize the balance
snapshot yet, so a successful response has an empty `data` payload.

`api_get_order`, `api_get_open_orders`, and `api_get_active_orders` are retained
as raw wrappers because their enum values exist in `TEngineMethodKind`. The
current Delphi reference server has no request-handler branches for them and
returns `Unknown method` (error 400).

## EngineResponse

```rust
pub struct EngineResponse {
    pub request_uid: u64,
    pub method: EngineMethod,
    pub success: bool,
    pub error_code: i32,
    pub error_msg: String,
    pub data: Vec<u8>,
}
```

`data` is already DEFLATE-decompressed when the response was compressed on the
wire.

## Auto-Apply Through EventDispatcher

When an `EngineResponse` arrives through `run_with_dispatcher`, these methods are
applied to `dispatcher.markets()` automatically:

- `GetMarketsList`;
- `UpdateMarketsList`;
- `GetMarketsIndexes`;
- `CheckBinanceTags`.

The dispatcher emits `Event::Markets(...)` and `Event::EngineResponse(...)` for
these responses.

## ServerInfo

`BaseCheck` may return server identity fields used by multi-server applications.
`connect_and_init` and `run_init_sequence` parse and store this automatically
when `base_check` is enabled:

```rust
let info = client.server_info();
if let Some(bot_id) = info.bot_id {
    println!("bot={bot_id} exchange={:?}", info.exchange_name);
}
```

Manual parsing:

```rust
use moonproto::commands::engine_api::{exchange_type_flags, parse_base_check_response};

let rx = client.api_base_check();
let resp = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(12))?;
let info = parse_base_check_response(&resp.data);

if info.supports(exchange_type_flags::FUTURES) {
    enable_futures_ui();
}
```

One-shot parsing and storage:

```rust
let info = client.request_base_check(&mut dispatcher, Duration::from_secs(12))?;
```

Fields:

| Field | Type | Meaning |
|---|---|---|
| `bot_id` | `Option<i64>` | Stable server id. |
| `server_name` | `Option<String>` | Human-readable server name. |
| `exchange_code` | `Option<u8>` | Server exchange enum ordinal. |
| `exchange_name` | `Option<String>` | Human-readable exchange name. |
| `exchange_type_mask` | `Option<u8>` | Capability bitmask. |
| `dex_name` | `Option<String>` | DEX name when relevant. |
| `base_currency_name` | `Option<String>` | Base currency label. |
| `base_currency_code` | `Option<u8>` | Base currency enum ordinal. |
| `server_version` | `Option<u32>` | MoonBot server version. |
| `moonproto_version` | `Option<u32>` | MoonProto version. |

All fields are optional for forward/backward compatibility. Older servers may
return no identity payload.

Capability flags:

```rust
use moonproto::commands::engine_api::exchange_type_flags;

exchange_type_flags::SPOT;
exchange_type_flags::FUTURES;
exchange_type_flags::DEX;
exchange_type_flags::PREDICT;
```

## AuthCheck

`parse_auth_check_response` parses the payload returned by `api_auth_check`:

```rust
let auth = client.request_auth_check(&mut dispatcher, Duration::from_secs(12))?;
println!("account={}", auth.account_id);
```

## Low-Level Builders

`commands::engine_request` exposes byte-level builders such as
`base_check`, `auth_check`, `get_markets_list`, `request_order_book_full`, and
`trades_resend_batches`. They return raw request payloads for advanced tools.

Regular applications should use `Client::api_*` wrappers.
