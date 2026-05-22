# Markets

Markets state is maintained from Engine API responses:

- `GetMarketsList` gives the full market list and correlation markets.
- `UpdateMarketsList` updates prices, funding, mark price, and correlation prices.
- `GetMarketsIndexes` gives the canonical `mIndex -> market name` mapping.
- `CheckBinanceTags` updates token tags.

When using `Client::run_with_dispatcher`, relevant responses are applied to
`EventDispatcher::markets()` automatically.

Low-level market response builders use Delphi string serialization: `Word`
UTF-8 byte length followed by exactly that declared number of bytes.

`CheckBinanceTags` follows the Delphi client: it updates only known markets that
are present in the response. Markets absent from the response keep their previous
token tags. A full `GetMarketsList` replacement prunes token tags for markets
that no longer exist.

`UpdateMarketsList` carries server `mIndex` values. Price updates and
`price_by_index` resolve those indexes through the current `GetMarketsIndexes`
mapping, so stale mappings after a server restart are not used.

## Reading State

```rust
if let Some(market) = dispatcher.markets().get("BTCUSDT") {
    println!("tick={} max_lev={}", market.bn_tick_size, market.max_leverage);
}

if let Some(price) = dispatcher.markets().price("BTCUSDT") {
    println!("bid={} ask={} mark={}", price.bid, price.ask, price.mark_price);
}

if let Some(name) = dispatcher.markets().market_name_by_index(0) {
    println!("mIndex 0 is {name}");
}

let tags = dispatcher.markets().tags("BTCUSDT");
if tags.contains(TokenTags::ALPHA) {
    println!("BTCUSDT has ALPHA tag");
}
```

## Init and Refresh

Initial fetch:

```rust
use moonproto::{connect_and_init, ConnectConfig, InitConfig};

let init = InitConfig {
    ..Default::default()
};
connect_and_init(&mut client, &mut dispatcher, ConnectConfig::new(init))?;
```

Long-running price refresh is controlled by `ClientConfig.refresh`. The default
uses the Delphi worker cadence, but ticks are gated by Init: transport `Fine`
does not start background Engine API. Set `update_markets_every` /
`check_tags_every` to `None` if the application owns those requests manually.

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

`MarketsState.indexes_synchronized` is a critical invariant.
The one-time Init always fetches the initial map. After server restart the
dispatcher can mark it stale. If the one-time Init already completed, reconnect
restore sends `GetMarketsIndexes` again automatically and then refreshes prices
with `UpdateMarketsList`. Until the fresh response arrives, `EventDispatcher`
drops orderbook/trades packets that depend on server indexes.
Price updates keyed by server `mIndex` are also skipped while a previously known
mapping is stale.

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
dispatcher.markets().market_name_by_index(0);
dispatcher.markets().market_by_index(0);
dispatcher.markets().market_index_by_name("BTCUSDT");
dispatcher.markets().price("BTCUSDT");
dispatcher.markets().price_by_index(0);
dispatcher.markets().tags("BTCUSDT");
dispatcher.markets().market_count();
dispatcher.markets().corr_count();
```

The index helpers return `None` while the mapping is stale after a server
restart. In the normal `run_with_dispatcher` path, trades and orderbook events
are gated until fresh indexes are received through init or an explicit
`GetMarketsIndexes` request.

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
