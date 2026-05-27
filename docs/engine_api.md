# Engine API

Engine API is the request/response surface for one-shot server operations:
health checks, account reads, market list refreshes, candles, orderbook
snapshots, transfer assets, and account settings.

Use typed `MoonClient::request_*` helpers for common one-shot reads and
`MoonClient` mutation helpers for one-shot account operations. The runtime keeps
pumping MoonProto while the caller waits for the response timeout. Low-level
`Client::api_*` wrappers are hidden from rustdoc and kept only for custom
protocol tools that intentionally work with raw `EngineResponse` receivers.

`EngineMethod` is the public method identifier type. Known values are constants
(`EngineMethod::BaseCheck`, `EngineMethod::RequestCandlesData`, ...), and
unknown future server values are preserved. Use `EngineMethod::from_byte(raw)`,
`method.to_byte()`, `method.is_known()`, and `method.name()`; do not rely on
Rust enum casts.

The default wait is 12 seconds per Engine API response. `run_init_sequence` uses
this default when `InitConfig::step_timeout` is `None`.

## Waiting for a Response

```rust
use std::time::Duration;

let qty = client.request_balance("USDT", Duration::from_secs(12))?;
let hedge_mode = client.request_hedge_mode(Duration::from_secs(12))?;
let api_expiration = client.request_api_expiration_time(Duration::from_secs(12))?;
let transfer_assets =
    client.request_transfer_assets(moonproto::ExchangeKind::Spot, Duration::from_secs(12))?;
let history = client.request_coin_card_candles(
    "BTCUSDT",
    moonproto::commands::candles::DeepHistoryKind::Hour1,
    Duration::from_secs(12),
)?;
let markets_received = client.refresh_candles(Duration::from_secs(30))?;
```

The one-shot helpers validate `EngineResponse::success`. Read helpers parse the
method-specific payload. Mutation helpers return `Ok(())` after a successful
acknowledgement and wrap Engine API failures as
`MoonClientError::EngineRequest`.

Advanced protocol tools can use the lower-level receiver path. Normal
applications should not wait on raw receivers from the UI thread: a raw
`Client::api_*` call only registers/sends a request, and a custom runtime must
keep the client loop pumping until the matching `EngineResponse` is decoded.

When the client loop is already active and decodes a registered response, the
receiver is signalled immediately from the receive-side path, before the same
payload is later applied to `EventDispatcher`. This avoids waiting for a second
dispatcher drain. In the high-level `MoonClient` path the runtime thread keeps
doing that work automatically.

Chunked `RequestCandlesData` uses its own pending registry internally:
registered chunks are also consumed and merged from receive-side DataReadInt.
The normal public helper is `refresh_candles`, which hides the merged zlib
payload and applies parsed 5m rows to retained market history.

For custom raw payloads with caller-owned timeout cleanup, a low-level runtime
can call `Client::request_engine_response`. Raw `api_*` receivers keep their
pending slot until a matching response arrives, a reconnect clears the session,
or the same UID is registered again. Regular applications should not use this
path.

## Client Wrappers

| Group | Methods |
|---|---|
| `MoonClient` typed reads | `request_balance`, `request_hedge_mode`, `request_api_expiration_time`, `request_transfer_assets`, `request_coin_card_candles`, `request_client_settings`, `request_order_snapshot`, `request_balance_snapshot` |
| `MoonClient` async Active Lib refresh | `refresh_balances`, `refresh_transfer_assets`, `refresh_transfer_assets_kind` |
| `MoonClient` mutation/refresh helpers returning `Ok(())` on accepted server ack | `set_leverage`, `set_hedge_mode`, `cancel_all_orders`, `change_position_type`, `convert_dust_bnb`, `confirm_risk_limit`, `set_ma_mode`, `do_transfer_asset`, `request_markets_balance_full`, `reload_order_book` |
| Low-level custom-runtime init reads | `request_base_check`, `request_auth_check` |
| Low-level raw receiver wrappers hidden from rustdoc | `Client::api_*` |

## Blocking vs Async

Blocking helpers wait for a specific server response while the owned runtime
keeps MoonProto pumping. Use them when the caller truly needs the returned value
before continuing:

- `request_balance("USDT", timeout)`;
- `request_hedge_mode(timeout)`;
- `request_api_expiration_time(timeout)`;
- `request_coin_card_candles(market, kind, timeout)`;
- `refresh_candles(timeout)`;
- `request_balance_snapshot(timeout)` / `request_order_snapshot(timeout)`;
- account actions that need an acknowledgement, such as `set_leverage`,
  `set_hedge_mode`, and `cancel_all_orders`.

Async Active Lib commands return after queuing work and later update
snapshots/events:

- `refresh_balances()`;
- `refresh_transfer_assets()` / `refresh_transfer_assets_kind(kind)`;
- subscriptions and order/trade intents such as `subscribe_orderbook`,
  `subscribe_all_trades`, `client.orders().move_order(...)`, and
  `client.trade().new_order(...)`.

For subscriptions, prefer the registry-aware APIs:

```rust
use moonproto::TradesStreamMode;

client.subscribe_all_trades(TradesStreamMode::TradesOnly)?;
client.subscribe_orderbook("BTCUSDT")?;
client.subscribe_orderbooks(["ETHUSDT", "SOLUSDT"])?;
client.unsubscribe_all_orderbooks()?;
```

Those APIs update the subscription registry. Before Init, they do not send
subscription packets; the one-time Init flushes the current registry once.
After Init, reconnect restores registry-aware subscriptions automatically. Raw
`api_subscribe_*` calls and raw `api_unsubscribe_order_book(...)` are useful for
custom tools but do not update the subscription registry and do not enforce the
typed subscription gate.

## Balance

`request_balance(currency)` returns the current quantity for one currency:

```rust
let qty = client.request_balance("USDT", Duration::from_secs(12))?;
println!("USDT balance={qty}");
```

## Account Settings

`request_hedge_mode()` returns the current hedge-mode flag:

```rust
let hedge_mode = client.request_hedge_mode(Duration::from_secs(12))?;
println!("hedge_mode={hedge_mode}");
```

`request_api_expiration_time()` returns an `ApiExpirationTime` wrapper and
exposes `system_time()`, `unix_seconds()`, and `days_until(...)` helpers:

```rust
let expiration = client.request_api_expiration_time(Duration::from_secs(12))?;
if let Some(unix) = expiration.unix_seconds() {
    println!("API key expires at unix_seconds={unix}");
}
```

For raw payload access, hidden low-level `Client::api_*` methods remain
available and return `Receiver<EngineResponse>`. They are for custom runtimes
and diagnostics, not normal UI code.

`refresh_candles` is the normal explicit full candles refresh helper. It
registers the chunk aggregator, keeps the runtime pumping, hides the
chunked/zipped payload, and applies parsed 5m candles to retained market history
when trades storage is active. Normal chart UI reads candles from
`snapshot.market_history_readers(market)`.
Hidden low-level chunk helpers remain available for diagnostics that need
`MergedCandles`, but regular applications should not build chart state from raw
chunk/zlib payloads.

`request_markets_balance_full` asks the server to refresh the full balance
state. The current reference server normally acknowledges the request with an
empty payload; the actual balance data arrives through the normal balance
channel and is applied to `MarketsState` / `BalancesState`.

Hidden raw wrappers such as `api_get_order`, `api_get_open_orders`, and
`api_get_active_orders` are retained for compatibility tools. The current
reference server has no request-handler branches for them and returns
`Unknown method` (error 400).

`refresh_transfer_assets` is the normal Active Lib path for transfer UI. It
queues the three wallet refresh requests and returns immediately; completed
responses update `snapshot().transfer_assets()` and emit
`Event::TransferAssets`:

```rust
use moonproto::ExchangeKind;

client.refresh_transfer_assets()?;

if let Some(snapshot) = client.snapshot() {
    for asset in snapshot.transfer_assets().get(ExchangeKind::Futures) {
        println!("{} transferable={} total={}", asset.currency, asset.amount, asset.total);
    }
}
```

`request_transfer_assets(kind, timeout)` remains available as a direct blocking
request/response helper for scripts and diagnostics:

```rust
let assets =
    client.request_transfer_assets(ExchangeKind::Spot, Duration::from_secs(12))?;
```

The typed parser is also available as
`parse_update_transfer_assets_response`.

Typed scalar response parsers keep compatibility with server short-tail
behavior:

- `parse_get_balance_response`: reads one floating-point balance; an empty or
  short fixed tail becomes zero-filled compatibility data.
- `parse_query_hedge_mode_response`: reads one boolean; an empty payload is
  `false`.
- `parse_api_expiration_time_response`: reads one date/time value; an empty
  payload is `0.0` and therefore unknown.
- `parse_update_transfer_assets_response`: reads the returned asset rows.
  Truncated declared strings reject the response; short fixed numeric tails are
  zero-filled for compatibility.

## EngineResponse

```rust
pub struct EngineResponse {
    pub ver: u16,
    pub request_uid: u64,
    pub method: EngineMethod,
    pub success: bool,
    pub error_code: i32,
    pub error_msg: String,
    pub data: Vec<u8>,
}
```

`ver` is the response format version from the server header. `method` preserves
the exact method identifier that the server sent. `data` is already
DEFLATE-decompressed when the response was compressed. If the response metadata
is malformed, parsing fails before an `EngineResponse` is emitted.

Transfer asset rows stored in `TransferAssetsState` and returned by
`request_transfer_assets`:

```rust
pub struct TransferAsset {
    pub currency: String,
    pub amount: f64,
    pub total: f64,
}
```

`amount` is the transferable quantity; `total` is the exchange-reported total
for the same asset row.

Wallet kinds use Delphi `TExchangeKind` order:

```rust
pub enum ExchangeKind {
    Spot,
    Futures,
    Quarterly,
}
```

## Auto-Apply Through EventDispatcher

When an `EngineResponse` arrives through the active runtime, these methods are
applied to the markets read model automatically:

- `GetMarketsList`;
- `UpdateMarketsList`;
- `GetMarketsIndexes`;
- `CheckBinanceTags`.

The dispatcher emits `Event::Markets(...)` and `Event::EngineResponse(...)` for
these responses.

`UpdateTransferAssets` is applied by the active runtime when requested through
`refresh_transfer_assets`; because the response is consumed by the pending
request registry, the public notification is `Event::TransferAssets(...)`.

## ServerInfo

`BaseCheck` may return server identity fields used by multi-server applications.
`connect_and_init` and `run_init_sequence` parse and store this automatically:

```rust
let info = client.server_info();
if let Some(bot_id) = info.bot_id {
    println!("bot={bot_id} exchange={:?}", info.exchange_name);
}
```

Manual parsing for custom protocol tools:

```rust
use moonproto::commands::engine_api::{exchange_type_flags, parse_base_check_response};

let info = parse_base_check_response(&engine_response.data);

if info.supports(exchange_type_flags::FUTURES) {
    enable_futures_ui();
}
```

One-shot parsing and storage for low-level custom runtimes:

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
assert_eq!(client.auth_info().map(|a| a.account_id.as_str()), Some(auth.account_id.as_str()));
```

Mandatory fields are required. Optional Hyperliquid DEX metadata is parsed in a
backward-compatible way: a truncated optional tail does not reject the whole
AuthCheck response, but only fully available metadata is useful to callers.

`request_auth_check` stores the parsed response in `client.auth_info()`.
`run_init_sequence` does the same for its mandatory AuthCheck step and also
copies it to `InitResult::auth_info`. A successful AuthCheck response with a
malformed mandatory payload is still treated as AuthCheck success during init,
but no auth metadata is stored.

## Low-Level Builders

`commands::engine_request` exposes low-level builders such as
`base_check`, `auth_check`, `get_markets_list`, `request_order_book_full`, and
`trades_resend_batches`. They return request payloads for diagnostics and
compatibility tools.

Regular applications should prefer `MoonClient` state/events. Direct low-level
Engine API wrappers are for custom runtimes and diagnostics.
