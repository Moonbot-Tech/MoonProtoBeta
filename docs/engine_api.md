# Engine API

Engine API is the request/response wire surface for server operations:
health checks, account reads, market list refreshes, candles, orderbook
snapshots, transfer assets, and account settings.

Use explicit `MoonClient::blocking_*` helpers only when scripts, diagnostics, or
worker code really need the value in the same synchronous branch. User-facing UI
refreshes and mutations are Active Lib intents: they queue work, return
immediately, and publish events/snapshots later. Low-level
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
client.request_balance_snapshot()?;
client.refresh_hedge_mode()?;
client.refresh_api_expiration_time()?;
client.refresh_transfer_assets()?;
```

These calls queue work into the Active Lib runtime and return immediately.
Completion arrives as typed events, and the updated values are read from
`MoonClient::snapshot()`. This is the normal UI shape: the runtime keeps the
protocol loop alive and owns the state.

Explicit one-shot `blocking_*` helpers still exist for scripts and diagnostics.
They validate `EngineResponse::success`, parse method-specific payloads, and
wrap Engine API failures as `MoonClientError::EngineRequest`.

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
Normal chart UI does not call this request directly: Active Lib sends it once
after trades storage is enabled, applies parsed 5m rows to retained market
history, and emits `Event::CandlesSnapshot` after the history-worker barrier.

For custom raw payloads with caller-owned timeout cleanup, a low-level runtime
can call `Client::request_engine_response`. Raw `api_*` receivers keep their
pending slot until a matching response arrives, a reconnect clears the session,
or the same UID is registered again. Regular applications should not use this
path.

## Client Wrappers

| Group | Methods |
|---|---|
| `MoonClient` async Active Lib refresh | `request_client_settings` / `refresh_settings`, `refresh_hedge_mode`, `refresh_api_expiration_time`, `request_balance_snapshot` / `refresh_balances`, `request_order_snapshot`, `refresh_transfer_assets`, `refresh_transfer_assets_kind`, `request_coin_card_candles` |
| `MoonClient` non-blocking mutation/refresh intents | `set_leverage`, `set_hedge_mode`, `cancel_all_orders`, `change_position_type`, `convert_dust_bnb`, `confirm_risk_limit`, `set_ma_mode`, `transfer_asset` / `do_transfer_asset`, `refresh_markets_balance_full`, `reload_order_book` |
| Explicit blocking diagnostic helpers | `blocking_request_balance`, `blocking_request_hedge_mode`, `blocking_request_api_expiration_time`, `blocking_request_transfer_assets`, `blocking_request_client_settings`, `blocking_request_balance_snapshot`, `blocking_request_order_snapshot`, `blocking_request_coin_card_candles`, `blocking_set_leverage`, `blocking_set_hedge_mode`, `blocking_cancel_all_orders`, `blocking_change_position_type`, `blocking_convert_dust_bnb`, `blocking_confirm_risk_limit`, `blocking_set_ma_mode`, `blocking_do_transfer_asset`, `blocking_request_markets_balance_full`, `blocking_reload_order_book` |
| Low-level custom-runtime init reads | `request_base_check`, `request_auth_check` |
| Low-level raw receiver wrappers hidden from rustdoc | `Client::api_*` |

## Blocking vs Async

Blocking helpers wait for a specific server response while the owned runtime
keeps MoonProto pumping. They are intentionally named with `blocking_`. Use them
when the caller truly needs the returned value in the same synchronous branch
before continuing:

- scripts/diagnostics that intentionally need a synchronous scalar or snapshot.

Direct scalar reads such as `blocking_request_balance`,
`blocking_request_hedge_mode`, and `blocking_request_api_expiration_time` are
diagnostic/script helpers. Normal UI should read maintained state or queue the
async refresh listed below.

Async Active Lib commands return after queuing work and later update
snapshots/events:

- `refresh_balances()`;
- `request_balance_snapshot()`, which fills `snapshot().balances()` and emits
  `Event::Balance`;
- `refresh_hedge_mode()`, which fills `snapshot().account().hedge_mode()` and
  emits `Event::Account`;
- `refresh_api_expiration_time()`, which fills
  `snapshot().account().api_expiration()` and emits `Event::Account`;
- `request_order_snapshot()`, which fills `snapshot().orders()` and emits order
  events;
- `refresh_transfer_assets()` / `refresh_transfer_assets_kind(kind)`;
- `request_client_settings()` / `refresh_settings()`, which fills
  `snapshot().settings().client_settings` and emits `Event::Settings`;
- `request_coin_card_candles(market, kind)`, which fills
  `snapshot().coin_card_candles()` and emits `Event::CoinCardCandles`;
- the automatic full 5m candles snapshot after trades storage is enabled, which
  fills `market_history_readers(...).candles_5m` and emits
  `Event::CandlesSnapshot`;
- account actions such as `set_leverage`, `set_hedge_mode`,
  `cancel_all_orders`, `change_position_type`, `confirm_risk_limit`,
  `set_ma_mode`, `reload_order_book`, and `transfer_asset`;
- subscriptions and order/trade intents such as `subscribe_orderbook`,
  `subscribe_all_trades`, `client.orders().move_order(...)`, and
  `client.trade().new_order(...)`.

Rule for public API shape: when the Delphi UI wraps a blocking
`TMoonProtoEngine` call in `TThread.CreateAnonymousThread`, or the caller does
not need the result in the same synchronous method, the regular Rust Active Lib
method must be non-blocking. Blocking counterparts remain explicitly named with
`blocking_` for scripts, diagnostics, and custom tools.

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

`blocking_request_balance(currency)` is a direct diagnostic read for one
currency. It is not the normal chart/UI balance model. Regular UI reads
per-market balance/position fields from `snapshot().markets().get(...).balance_position()`
and account totals from `snapshot().balances()` after normal balance events.

```rust
client.request_balance_snapshot()?;

if let Some(snapshot) = client.snapshot() {
    let global = snapshot.balances().global();
    println!("available={}", global.btc_balance_total);
    if let Some(market) = snapshot.markets().get("BTCUSDT") {
        let pos = market.balance_position();
        println!("pos_size={} liq={}", pos.pos_size, pos.liq_price);
    }
}
```

## Account Settings

Regular UI queues account refreshes and reads retained account state:

```rust
client.refresh_hedge_mode()?;
client.refresh_api_expiration_time()?;

for event in client.drain_events() {
    if matches!(event, moonproto::Event::Account(_)) {
        if let Some(snapshot) = client.snapshot() {
            println!("hedge_mode={:?}", snapshot.account().hedge_mode());
            println!("api_expiration={:?}", snapshot.account().api_expiration());
        }
    }
}
```

The explicit blocking counterparts remain available for scripts/diagnostics
that really need a synchronous scalar:

```rust
let hedge_mode = client.blocking_request_hedge_mode(Duration::from_secs(12))?;
let expiration = client.blocking_request_api_expiration_time(Duration::from_secs(12))?;
if let Some(unix) = expiration.unix_seconds() {
    println!("API key expires at unix_seconds={unix}");
}
```

For raw payload access, hidden low-level `Client::api_*` methods remain
available and return `Receiver<EngineResponse>`. They are for custom runtimes
and diagnostics, not normal UI code.

Hidden low-level chunk helpers remain available for diagnostics that need
`MergedCandles`, but regular applications should not build chart state from raw
chunk/zlib payloads. Normal chart UI reads retained candles from
`snapshot.market_history_readers(market)`.

`request_markets_balance_full` asks the server to refresh the full balance
state. The current reference server normally acknowledges the request with an
empty payload; the actual balance data arrives through the normal balance
channel and is applied to `MarketsState` / `BalancesState`.

Hidden raw wrappers such as `api_get_order`, `api_get_open_orders`, and
`api_get_active_orders` are retained for compatibility tools. The current
reference server has no request-handler branches for them and returns
`Unknown method` (error 400).

`refresh_transfer_assets` is the normal Active Lib path for transfer UI. It
queues the three wallet refresh requests and returns immediately. Responses are
polled by the runtime owner, so waiting for Spot/Futures/Quarterly never blocks
protocol pumping or other Active Lib work. Each response updates
`snapshot().transfer_assets()` and emits a per-wallet `Event::TransferAssets`;
after all requested wallet kinds answer, `TransferAssetsEvent::RefreshCompleted`
is emitted:

```rust
use moonproto::ExchangeKind;

client.refresh_transfer_assets()?;

if let Some(snapshot) = client.snapshot() {
    for asset in snapshot.transfer_assets().get(ExchangeKind::Futures) {
        println!("{} transferable={} total={}", asset.currency, asset.amount, asset.total);
    }
}
```

`blocking_request_transfer_assets(kind, timeout)` remains available as a direct
blocking request/response helper for scripts and diagnostics:

```rust
let assets =
    client.blocking_request_transfer_assets(ExchangeKind::Spot, Duration::from_secs(12))?;
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
`blocking_request_transfer_assets`:

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
