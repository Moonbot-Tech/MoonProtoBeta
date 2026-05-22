# Candles

Historical candles are requested through Engine API. Two methods are exposed:
`emk_GetCoinCardCandles` (single response) and `emk_RequestCandlesData`
(chunked, wrapped by a one-shot helper).

## DeepPrice

Packed record, **28 bytes**:

```rust
pub struct DeepPrice {
    pub open_p:  f32,    // 4 bytes
    pub close_p: f32,    // 4 bytes
    pub max_p:   f32,    // 4 bytes
    pub min_p:   f32,    // 4 bytes
    pub vol:     f32,    // 4 bytes
    pub time:    f64,    // 8 bytes — TDateTime (Delphi double, days since 1899-12-30)
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
    Duration::from_secs(12),
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

## emk_RequestCandlesData — chunked response (one-shot helper recommended)

The server replies with multiple `EngineResponse` packets carrying the same
`request_uid`. Each packet is a chunk of the form `ChunkIndex:u16 +
ChunkTotal:u16 + payload`. The library aggregates them through
`CandlesAggregator` and returns the merged Delphi candles stream.

### One-Shot API (recommended)

```rust
use std::time::Duration;

match client.request_candles_data(&mut dispatcher, Duration::from_secs(30)) {
    Ok(merged) => {
        // merged.uid         — request_uid
        // merged.zipped_data — raw zlib stream from Delphi StoreCandlesToZip
        // merged.markets     — parsed per-market 5m candles and wall data
        for market in &merged.markets {
            println!("{}: {} candles", market.market_name, market.candles_5m.len());
        }
    }
    Err(_) => eprintln!("candles timeout"),
}
```

The helper registers the chunk aggregator, keeps the UDP loop running through
short dispatcher ticks, and removes the pending candles slot if the caller's
timeout expires before the final chunk.

When a reader thread is already active, registered `RequestCandlesData` chunks
are aggregated from the reader-side DataReadInt path. Completed streams signal
the `MergedCandles` receiver before the consumed chunks are considered for raw
callbacks or `EventDispatcher` delivery.

`MergedCandles`:
```rust
pub struct MergedCandles {
    pub uid:         u64,
    pub zipped_data: Vec<u8>,
    pub markets:     Vec<RequestCandlesMarket>,
}

pub struct RequestCandlesMarket {
    pub market_name: String,
    pub candles_5m: Vec<DeepPrice>,
    pub buy_wall: [WallItem; 4],
    pub sell_wall: [WallItem; 4],
}
```

Stale pending candles slots are auto-cleaned —
`DEFAULT_PENDING_CANDLES_TIMEOUT_MS = 15_000` (15 seconds) from the last
received chunk, matching Delphi `Markets.LastChunkTime`. A slot is freed either
when all chunks have arrived or on timeout.

### Async Receiver API

`api_request_candles_data_async` remains available for custom flows that already
run the client loop elsewhere:

```rust
let rx = client.api_request_candles_data_async();
let merged = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(30))?;
```

### Low-level CandlesAggregator

Use this only for custom flows that need fine-grained `request_uid` routing in
a multi-request scenario:

```rust
use moonproto::commands::candles::{
    CandlesAggregator,
    parse_request_candles_data_response,
};

let mut agg = CandlesAggregator::new();

// Fire-and-forget request:
client.api_request_candles_data();

// Inside an `on_data` callback (or through EventDispatcher::Event::EngineResponse):
//   if resp.method == EngineMethod::RequestCandlesData {
//       if let Some(merged) = agg.on_chunk(&resp.data) {
//           let markets = parse_request_candles_data_response(&merged).unwrap();
//           // process markets ...
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
`0..ChunkTotal-1`), the `chunk_payload`s are concatenated into the zlib stream
written by Delphi `TMarkets.StoreCandlesToZip`. This is not the same layout as
`GetCoinCardCandles`.

The merged stream carries the server timezone shift in minutes. The parser applies
the same correction as Delphi `TMarkets.ApplyRecvdStream`: each parsed 5m candle
time is adjusted by `(local_timezone_minutes - server_timezone_minutes) / 1440`.

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
- **Chunk count**: Delphi uses a `u16` `ChunkTotal` and has no extra 1024-chunk
  cap; the aggregator accepts the full wire range.
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

`DeepPrice.time` is a `TDateTime` (Delphi double, days since 1899-12-30). For
`RequestCandlesData`, the parsed 5m candle times are already shifted to the
client's local timezone, matching Delphi `ApplyRecvdStream`. `GetCoinCardCandles`
returns the raw `TDeepPrice` values from the response.

If the `TDateTime` value you are handling represents UTC, convert to unix
timestamp with:

```rust
fn delphi_to_unix_secs(td: f64) -> f64 {
    (td - 25569.0) * 86400.0    // 25569 = days between 1899-12-30 and 1970-01-01
}
```

## Related API Surface

`engine_api.md` documents the RPC channel and `EngineResponse` format.
`events.md` documents `Event::EngineResponse` for raw response tracking.
High-level candle helpers are `client.request_coin_card_candles()` and
`client.request_candles_data()`. Lower-level entry points are
`api_get_coin_card_candles()`, `api_request_candles_data()`, and
`api_request_candles_data_async()`.
