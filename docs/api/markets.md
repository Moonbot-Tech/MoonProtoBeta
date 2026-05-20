# Markets

Markets state is maintained from Engine API responses:

- `GetMarketsList` gives the full market list and correlation markets.
- `UpdateMarketsList` updates prices, funding, mark price, and correlation prices.
- `GetMarketsIndexes` gives the canonical `mIndex -> market name` mapping.
- `CheckBinanceTags` updates token tags.

When using `Client::run_with_dispatcher`, relevant responses are applied to
`EventDispatcher::markets()` automatically.

## Reading State

```rust
if let Some(market) = dispatcher.markets().get("BTCUSDT") {
    println!("tick={} max_lev={}", market.bn_tick_size, market.max_leverage);
}

if let Some(price) = dispatcher.markets().price("BTCUSDT") {
    println!("bid={} ask={} mark={}", price.bid, price.ask, price.mark_price);
}

let tags = dispatcher.markets().tags("BTCUSDT");
if tags.contains(TokenTags::ALPHA) {
    println!("BTCUSDT has ALPHA tag");
}
```

## Init and Refresh

Initial fetch:

```rust
let init = InitConfig {
    fetch_markets: true,
    ..Default::default()
};
run_init_sequence(&mut client, &mut dispatcher, init)?;
```

Long-running price refresh is controlled by `ClientConfig.refresh` and is enabled
by default through `RefreshConfig::default()`. The default also refreshes token
tags every 60 seconds and performs the Delphi-compatible hourly four-request
`CheckBinanceTags` burst; applications do not need their own timer for this.

See `examples/market_refresh.rs` for a compact consumer-side loop that reads
prices and tags from `EventDispatcher`.

## Events

```rust
pub enum MarketsEvent {
    MarketsListReplaced { count: usize, corr_count: usize },
    PricesUpdated { count: usize, included_funding: bool, included_corr: bool },
    IndexesUpdated { count: usize },
    TokenTagsUpdated { count: usize },
}
```

`MarketsState.indexes_synchronized` is a critical invariant. After server restart
the client sets it to `false`, refetches indexes, and `EventDispatcher` drops
orderbook/trades packets until fresh indexes are applied.

## Public State

```rust
pub struct MarketsState {
    pub markets: Vec<Market>,
    pub by_name: HashMap<String, usize>,
    pub corr_markets: HashMap<String, CorrMarket>,
    pub prices: Vec<MarketPrice>,
    pub corr_prices: HashMap<String, f64>,
    pub token_tags: HashMap<String, TokenTags>,
    pub market_indexes: Vec<String>,
    pub indexes_synchronized: bool,
}
```

Convenience methods:

```rust
dispatcher.markets().get("BTCUSDT");
dispatcher.markets().price("BTCUSDT");
dispatcher.markets().price_by_index(0);
dispatcher.markets().tags("BTCUSDT");
dispatcher.markets().market_count();
dispatcher.markets().corr_count();
```

## TokenTags

```rust
pub struct TokenTags(pub u32);

TokenTags::MONITORING;
TokenTags::FAN;
TokenTags::SEED;
TokenTags::LAUNCH;
TokenTags::GAMING;
TokenTags::NEW;
TokenTags::OLD;
TokenTags::BNB;
TokenTags::ALPHA;
TokenTags::OI_CAPPED;
TokenTags::TRAD_FI;
```

Use `contains`, `is_empty`, `bits`, and `from_bits` for bitset work.

## Low-Level Parsing

```rust
use moonproto::commands::market::{
    parse_markets_indexes_response, parse_markets_list_response,
    parse_markets_prices_response, parse_token_tags_response,
};

let list = parse_markets_list_response(&resp.data, 2).expect("bad markets list");
```

`EventDispatcher` currently uses protocol version `2` for `GetMarketsList`
responses, matching the live server format with `futures_type`.
