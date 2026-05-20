# Candles

Historical candles are requested through Engine API. Two methods are exposed:
`emk_GetCoinCardCandles` (single response) and `emk_RequestCandlesData` (chunked,
wrapped by an async helper).

## DeepPrice

Packed record, **28 bytes**:

```rust
pub struct DeepPrice {
    pub open_p:  f32,    // 4 bytes
    pub close_p: f32,    // 4 bytes
    pub max_p:   f32,    // 4 bytes
    pub min_p:   f32,    // 4 bytes
    pub vol:     f32,    // 4 bytes
    pub time:    f64,    // 8 bytes — TDateTime (Delphi double, days since 1899-12-30 UTC)
}
```

Mirrors Delphi `MarketsU.pas:701-705 TDeepPrice = packed record`.

## DeepHistoryKind

```rust
pub enum DeepHistoryKind {
    Min1  = 0,
    Min5  = 1,
    Min30 = 2,
    Hour1 = 3,
    Hour4 = 4,
    Day1  = 5,
}
```

Mirrors `EngineBase.pas:60 TMarketDeepHistoryKind`.

**Note:** `Hour4` was added to Delphi later — the enum order is **wire-critical**,
ordinal is sent as a single byte.

## emk_GetCoinCardCandles — single response

```rust
use std::time::Duration;
use moonproto::commands::candles::DeepHistoryKind;

match client.request_coin_card_candles(
    &mut dispatcher,
    "BTCUSDT",
    DeepHistoryKind::Hour1,
    Duration::from_secs(10),
) {
    Ok(candles) => {
        for c in &candles {
            println!("{} open={} close={} vol={}",
                c.time, c.open_p, c.close_p, c.vol);
        }
    }
    Err(err) => eprintln!("request failed: {err}"),
}
```

Wire response (after DEFLATE decompression):
```
count: i32 LE
candles: N × TDeepPrice (28 bytes each)
```

## emk_RequestCandlesData — chunked response (async helper recommended)

The server replies with multiple `EngineResponse` packets carrying the same
`request_uid`. Each packet is a chunk of the form `ChunkIndex:u16 +
ChunkTotal:u16 + payload`. The library aggregates them through
`CandlesAggregator` and returns the merged result.

### Async API (recommended)

```rust
use std::time::Duration;

let rx = client.api_request_candles_data_async();
match client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(30)) {
    Ok(merged) => {
        // merged.uid     — request_uid
        // merged.candles — already-parsed Vec<DeepPrice>
        for c in &merged.candles {
            println!("{}: o={} c={}", c.time, c.open_p, c.close_p);
        }
    }
    Err(_) => eprintln!("candles timeout"),
}
```

`MergedCandles`:
```rust
pub struct MergedCandles {
    pub uid:     u64,
    pub candles: Vec<DeepPrice>,
}
```

Stale pending candles slots are auto-cleaned —
`DEFAULT_PENDING_CANDLES_TIMEOUT_MS = 30_000` (30 seconds). A slot is freed
either when all chunks have arrived or on timeout.

### Low-level CandlesAggregator

Use this only for custom flows that need fine-grained `request_uid` routing in
a multi-request scenario:

```rust
use moonproto::commands::candles::{CandlesAggregator, parse_coin_card_candles_response};

let mut agg = CandlesAggregator::new();

// Fire-and-forget request:
client.api_request_candles_data();

// Inside an `on_data` callback (or through EventDispatcher::Event::EngineResponse):
//   if resp.method == EngineMethod::RequestCandlesData {
//       if let Some(merged) = agg.on_chunk(&resp.data) {
//           let candles = parse_coin_card_candles_response(&merged).unwrap();
//           // process candles ...
//       }
//   }
```

### Chunk wire format

Each `EngineResponse.data` for `RequestCandlesData`:
```
ChunkIndex: u16 LE
ChunkTotal: u16 LE
chunk_payload: bytes (rest)
```

After all `ChunkTotal` chunks have arrived (in any order, by `ChunkIndex`
`0..ChunkTotal-1`), the `chunk_payload`s are concatenated into a final stream
with the same `count + candles` layout as `GetCoinCardCandles`.

### CandlesAggregator API

```rust
impl CandlesAggregator {
    pub fn new() -> Self;
    pub fn on_chunk(&mut self, response_data: &[u8]) -> Option<Vec<u8>>;
    pub fn reset(&mut self);
    pub fn progress(&self) -> (usize, usize);    // (received, total)
}
```

**Behavior:**
- **Out-of-order delivery**: chunks are accepted in any order, stored at `chunks[idx]`.
- **Duplicates**: a repeated chunk for the same `idx` is ignored.
- **Resize**: on the first chunk the internal `chunks: Vec<Option<Vec<u8>>>` is
  sized to `ChunkTotal`.
- **Auto-reset**: when `on_chunk` returns `Some(merged)` the internal state is
  cleared and ready for the next request.

### Requirements for manual `CandlesAggregator` callers

1. **DEFLATE decompression already done.** `response_data` is the
   `EngineResponse.data` _after_ decompression.
2. **`request_uid` filtering.** If you run multiple parallel `RequestCandlesData`
   through the manual aggregator, keep a separate `CandlesAggregator` per
   `request_uid` or call `reset()` between requests. The async API
   (`api_request_candles_data_async`) handles this for you through the internal
   `pending_candles` registry.

## Candle time

`DeepPrice.time` is a `TDateTime` (Delphi double, days since 1899-12-30 UTC).
Convert to unix timestamp:

```rust
fn delphi_to_unix_secs(td: f64) -> f64 {
    (td - 25569.0) * 86400.0    // 25569 = days between 1899-12-30 and 1970-01-01
}
```

## See also

- [engine_api.md](engine_api.md) — RPC channel, ApiPending registry.
- [events.md](events.md) — `Event::EngineResponse` for raw response tracking.
- [client.md](client.md) — `client.request_coin_card_candles()` /
  `api_get_coin_card_candles()` / `api_request_candles_data()` /
  `api_request_candles_data_async()`.
