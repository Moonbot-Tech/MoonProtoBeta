# Engine API

`MPC_API` is the request/response RPC channel between the client and the server
engine. Requests and responses are correlated by `request_uid`.

Use `Client::api_*` wrappers for normal application code.

## Waiting for a Response

```rust
use std::time::Duration;
use moonproto::commands::market::parse_markets_list_response;

let rx = client.api_get_markets_list();
let resp = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(10))?;

if resp.success {
    let markets = parse_markets_list_response(&resp.data, 2).expect("bad markets response");
    println!("markets={}", markets.markets.len());
}
```

`run_until_response` keeps pumping the UDP loop through short
`run_with_dispatcher` ticks. Calling `rx.recv_timeout(...)` directly on the same
thread usually times out because no UDP packets are processed during that wait.

## Client Wrappers

| Group | Methods |
|---|---|
| Init/auth | `api_base_check`, `api_auth_check` |
| Markets | `api_get_markets_list`, `api_get_markets_indexes`, `api_update_markets_list`, `api_check_binance_tags` |
| Balance | `api_get_balance(currency)`, `api_get_markets_balance_full` |
| Orders | `api_get_order(uid)`, `api_get_open_orders`, `api_get_active_orders`, `api_cancel_all_orders` |
| Account settings | `api_set_leverage(market, lev)`, `api_set_hedge_mode(bool)`, `api_query_hedge_mode`, `api_check_expiration_time` |
| Trades | `api_subscribe_all_trades(want_mm_orders)`, `api_unsubscribe_all_trades`, `api_trades_resend_batches(packet_nums)` |
| Orderbooks | `api_subscribe_order_book(markets)`, `api_unsubscribe_order_book(markets)`, `api_request_order_book_full(market_idx, kind)`, `api_reload_order_book` |
| Position/transfer | `api_change_position_type`, `api_convert_dust_bnb`, `api_confirm_risk_limit`, `api_set_ma_mode`, `api_do_transfer_asset`, `api_update_transfer_assets` |
| Candles | `api_get_coin_card_candles`, `api_request_candles_data_async` |

For subscriptions, prefer the registry-aware APIs:

```rust
client.subscribe_all_trades(false);
client.subscribe_orderbook("BTCUSDT");
```

Those APIs are replayed automatically after reconnect. Raw `api_subscribe_*`
calls are useful for custom tools but do not update the subscription registry.

## Balance

`api_get_balance(currency)` returns the current quantity for one currency. The
server payload is parsed with `parse_get_balance_response`:

```rust
use moonproto::commands::parse_get_balance_response;

let rx = client.api_get_balance("USDT");
let resp = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(10))?;

if resp.success {
    let qty = parse_get_balance_response(&resp.data).expect("bad balance response");
    println!("USDT balance={qty}");
}
```

## Account Settings

`api_query_hedge_mode()` returns the current hedge-mode flag. The server payload
is parsed with `parse_query_hedge_mode_response`:

```rust
use moonproto::commands::parse_query_hedge_mode_response;

let rx = client.api_query_hedge_mode();
let resp = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(10))?;

if resp.success {
    let hedge_mode = parse_query_hedge_mode_response(&resp.data).expect("bad hedge payload");
    println!("hedge_mode={hedge_mode}");
}
```

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
`run_init_sequence` parses and stores this automatically when `base_check` is
enabled:

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
let resp = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(10))?;
let info = parse_base_check_response(&resp.data);

if info.supports(exchange_type_flags::FUTURES) {
    enable_futures_ui();
}
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
use moonproto::commands::engine_api::parse_auth_check_response;

let rx = client.api_auth_check();
let resp = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(10))?;
if let Some(auth) = parse_auth_check_response(&resp.data) {
    println!("account={}", auth.account_id);
}
```

## Low-Level Builders

`commands::engine_request` exposes byte-level builders such as
`base_check`, `auth_check`, `get_markets_list`, `request_order_book_full`, and
`trades_resend_batches`. They return raw request payloads for advanced tools.

Regular applications should use `Client::api_*` wrappers.
