# MPC_API - Engine RPC

`MPC_API` (channel byte 31) is the request/response RPC channel used for calls
into the server-side trading engine: exchange connectivity checks, balances,
orders, candles, subscriptions, and related account operations. The method id is
defined by `commands::engine_api::EngineMethod`.

`EngineMethod` is a raw one-byte Delphi ordinal wrapper, not a closed Rust enum.
Known methods are exposed as constants such as `EngineMethod::BaseCheck`; use
`EngineMethod::from_byte`, `to_byte`, `is_known`, and `name` for raw access.
Unknown method bytes are preserved exactly like Delphi
`ms.Read(Method, SizeOf(Method))`; they are not mapped to `None`.

## Wire Format

### Request (C -> S)

`TEngineRequest` is a regular MoonProto command with a common command header and
the engine request body:

```text
[CmdId=2]            1 byte       request marker
[ver=3]              2 bytes LE   command protocol version
[UID]                8 bytes LE   request command uid
[Method]             1 byte       EngineMethod ordinal, raw byte preserved
[MarketName]         u16 + UTF-8  single-market argument, or an empty string
[MarketNamesCount]   i32 LE       number of batch market names
[MarketNames...]     repeated     each item is u16 length + UTF-8
[ParamsSize]         i32 LE       byte length of Params
[Params]             variable     method-specific payload
```

`UID` must be unique per request. The client stores a
`Receiver<EngineResponse>` in the pending registry under this uid. The server
copies it into `RequestUID` in the response, and the dispatcher delivers the
response to the registered receiver.

### Response (S -> C)

`TEngineResponse` also starts with the common command header. The header's
`own_UID` belongs to the response command itself; request matching uses
`RequestUID`.

```text
[CmdId=1]            1 byte       response marker
[ver=3]              2 bytes LE   command protocol version
[own_UID]            8 bytes LE   response command uid
[RequestUID]         8 bytes LE   copied from request UID
[Method]             1 byte       EngineMethod ordinal, raw byte preserved
[Success]            1 byte       Delphi Boolean (0 = error, non-zero = success)
[ErrorCode]          i32 LE       server-side diagnostic code
[ErrorMsg]           u16 + UTF-8  server-side diagnostic text
[IsCompressed]       1 byte       Delphi Boolean
[DataSize]           i32 LE       byte length of Data on the wire
[Data]               variable     method-specific response payload
```

`parse_engine_response` strips the common header, extracts `RequestUID`, and
DEFLATE-decompresses `Data` when `IsCompressed` is set. The client uses
`RequestUID` internally to match the response to the receiver returned by
`Client::api_*` wrappers. The public `EngineResponse::data` field contains the
decompressed payload.

The `Data` format is method-specific. See the doc comments on each
`EngineMethod` variant (`cargo doc --open` -> `EngineMethod`). Parsers for
common response payloads:

| Method | Parser |
|--------|--------|
| `BaseCheck` | [`commands::engine_api::parse_base_check_response`] -> `ServerInfo` |
| `AuthCheck` | [`commands::engine_api::parse_auth_check_response`] -> `AuthCheckResponse` |
| `GetMarketsList` / `UpdateMarketsList` | [`commands::market::parse_markets_list_response`] |
| `GetCoinCardCandles` | [`commands::candles::parse_coin_card_candles_response`] |
| `RequestCandlesData` | [`Client::request_candles_data`](../api/candles.md#emk_requestcandlesdata--chunked-response-one-shot-helper-recommended) |
| `GetMarketsIndexes` | Applied inline by `EventDispatcher` |

### BaseCheck Response - Multi-Server Identity

When `Success=1`, newer servers append ten optional fields to `Data` in this
order:

```text
[bot_id]              i64 LE                  cfg.UniqueBotID
[server_name]         u16 length + UTF-8      cfg.BotName, default "Server"
[exchange_code]       u8                      Ord(cfg.Header.Current)
[exchange_name]       u16 length + UTF-8      "Binance Futures", "Hyper", ...
[exchange_type_mask]  u8                      bit0=Spot, bit1=Futures, bit2=DEX, bit3=Predict
[dex_name]            u16 length + UTF-8      HIP-3 DEX name for HL futures, or ""
[base_currency_name]  u16 length + UTF-8      "USDT", "BTC", "USDC", ...
[base_currency_code]  u8                      Ord(cfg.BaseCurrency), BC_USDT=1
[server_version]      i32 LE                  Current_Version_Num_X
[moonproto_version]   i32 LE                  IntMoonProtoTCPCurrentVer
```

Forward compatibility: [`parse_base_check_response`] accepts truncation at any
field boundary or inside a later field. Fields decoded before the truncation are
filled, the rest stay `None`. Older servers can return an empty payload, which
parses as `ServerInfo::default()`. See
[`docs/api/engine_api.md`](../api/engine_api.md#serverinfo) for field semantics.

When `Success=0`, `Data` is empty.

## Chunked Responses

`RequestCandlesData` returns multiple `EngineResponse` packets with the same
`RequestUID` because candle payloads can exceed a single sliced response.
`Client::request_candles_data` is the normal public API for this method: it
registers the chunk aggregator, keeps the client loop running, merges all
chunks, and returns one `MergedCandles` value.

For custom async flows, `Client::api_request_candles_data_async` exposes the
same internal chunk registry as a `Receiver<MergedCandles>`. Manual
`CandlesAggregator` use is only needed by protocol tools that intentionally
route raw `EngineResponse` packets themselves. The merged bytes are the zlib
stream from Delphi `TMarkets.StoreCandlesToZip`; parse them with
`parse_request_candles_data_response`, not with the CoinCard candles parser.

## Client Wrappers

Every method has a high-level `Client` wrapper with automatic uid generation:

```rust
let rx = client.api_get_markets_list();
let rx = client.api_get_balance("USDT");
let rx = client.api_set_leverage("BTCUSDT", 10);

let response: EngineResponse =
    client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(5))?;

if response.success {
    let markets = parse_markets_list_response(&response.data, version)?;
    // ...
} else {
    eprintln!("server error {}: {}", response.error_code, response.error_msg);
}
```

For chunked candles, prefer the one-shot helper:

```rust
let merged = client.request_candles_data(&mut dispatcher, Duration::from_secs(30))?;
println!("markets={}", merged.markets.len());
```

For custom raw request payloads that need cleanup tied to the caller's timeout,
use `Client::request_engine_response`. Receiver-based `api_*` wrappers keep the
pending slot until a matching response arrives, a reconnect clears the session,
or the same UID is registered again.

Current reference-server gaps:

- `GetMarketsBalanceFull` calls the server-side refresh method, but
  `MoonProtoEngineServer.pas → ProcessRequest` still has `WriteBalancesToStream`
  as TODO, so successful responses carry empty `Data`.
- `GetOrder`, `GetOpenOrders`, and `GetActiveOrders` exist in
  `TEngineMethodKind`, but the current Delphi server has no request-handler
  branches for them and returns `Unknown method` (error 400).

The full list is available as `client.api_*` methods in generated Rust docs.

## Versioning

`ver` is currently `3`. Commands with `ver > 3` are skipped for forward
compatibility. Commands with `ver < 3` use method-specific backward-compatible
parsing where older formats exist.

## Errors

`Success=0` means the request reached the server and the trading engine returned
an error. `ErrorCode` and `ErrorMsg` carry server or exchange diagnostics; there
is no stable public enum of engine error codes.

Transport, parsing, and timeout failures are separate from `Success=0`. For
example, if the same uid is not received before the caller's timeout, waiting on
the response receiver returns a timeout error.
