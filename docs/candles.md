# Candles

Historical candles are requested through Engine API. Two methods are exposed:
`request_coin_card_candles` for one market/history kind and
`request_candles_data` for the full multi-market candles snapshot.

## DeepPrice

`DeepPrice` is the public candle row:

```rust
pub struct DeepPrice {
    pub open_p:  f32,    // 4 bytes
    pub close_p: f32,    // 4 bytes
    pub max_p:   f32,    // 4 bytes
    pub min_p:   f32,    // 4 bytes
    pub vol:     f32,    // 4 bytes
    pub time:    f64,    // date/time as server-compatible day number
}
```

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

Use these constants through the enum names; do not rely on Rust enum casts.

## Coin Card Candles

```rust
use std::time::Duration;
use moonproto::commands::candles::DeepHistoryKind;

match client.request_coin_card_candles(
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

The `MoonClient` helper keeps the runtime pumping until the response arrives or
the timeout expires. A successful response returns a `Vec<DeepPrice>` sorted
exactly as the server sent it.

## Full Candles Snapshot

The full snapshot can be larger than one UDP response, so the server sends it in
chunks. The library aggregates chunks, parses the merged stream, and returns one
`MergedCandles` value.

### One-Shot API (recommended)

```rust
use std::time::Duration;

match client.request_candles_data(Duration::from_secs(30)) {
    Ok(merged) => {
        // merged.uid         — internal request id
        // merged.zipped_data — original compressed stream kept for diagnostics
        // merged.markets     — parsed per-market 5m candles and wall data
        for market in &merged.markets {
            println!("{}: {} candles", market.market_name, market.candles_5m.len());
        }
    }
    Err(_) => eprintln!("candles timeout"),
}
```

The helper registers the chunk aggregator, keeps the runtime pumping MoonProto,
and removes the pending candles slot if the caller's timeout expires before the
final chunk.

When the client loop is already active, registered `RequestCandlesData` chunks
are aggregated from the receive-side path. Completed streams signal the
`MergedCandles` receiver before the consumed chunks are considered for raw
callbacks or `EventDispatcher` delivery.

For live registered requests the client first tries the strict
`parse_request_candles_data_response` parser. If a merged zlib stream is
malformed after one or more complete market entries, the internal active path
falls back to a compatibility partial parser and keeps those complete prior
markets. Malformed data inside the currently broken market is not exposed as
initialized market data.

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

Pending candles slots are not auto-cancelled by an internal timer. A slot is
freed when all chunks arrive, when the server returns an error, when a one-shot
`request_candles_data` caller timeout removes it, when the session is reset, or
when another request with the same UID replaces it.

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

The public low-level `parse_request_candles_data_response` parser is strict: it
returns `None` if the merged stream is malformed. The partial fallback described
above is an internal active-library compatibility path for registered
`RequestCandlesData` requests.

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
  sized to the server-declared chunk count.
- **Auto-reset**: when `on_chunk` returns `Some(merged)` the internal state is
  cleared and ready for the next request.

### Requirements for manual `CandlesAggregator` callers

1. **DEFLATE decompression already done.** `response_data` is
   `EngineResponse.data` after decompression.
2. **`request_uid` filtering.** If you run multiple parallel `RequestCandlesData`
   through the manual aggregator, keep a separate `CandlesAggregator` per
   `request_uid` or call `reset()` between requests. The async API
   (`api_request_candles_data_async`) handles this for you through the internal
   `pending_candles` registry.

## Candle time

`DeepPrice.time` uses the server-compatible day-number timestamp. For
`request_candles_data`, parsed 5m candle times are already shifted to the
client's local timezone. `request_coin_card_candles` returns the timestamp values
from the response.

If the timestamp value you are handling represents UTC, convert to unix
timestamp with:

```rust
fn delphi_to_unix_secs(td: f64) -> f64 {
    (td - 25569.0) * 86400.0    // 25569 = days between 1899-12-30 and 1970-01-01
}
```

## Related API Surface

High-level candle helpers are `client.request_coin_card_candles()` and
`client.request_candles_data()`. Lower-level entry points are
`api_get_coin_card_candles()`, `api_request_candles_data()`, and
`api_request_candles_data_async()`.
