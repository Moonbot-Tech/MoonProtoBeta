# Markets

Markets state is maintained from Engine API responses:

- `GetMarketsList` gives the full market list and correlation markets.
- `UpdateMarketsList` updates prices, funding, mark price, and correlation prices.
- `GetMarketsIndexes` gives the canonical `mIndex -> market name` mapping.
- `CheckBinanceTags` updates token tags.

When using `MoonClient`, relevant responses are applied to the active markets
read model automatically.

The active dispatcher applies `GetMarketsList`, `UpdateMarketsList`, and
`CheckBinanceTags` directly while reading the payload, matching Delphi's
in-loop state updates. Applications should read the maintained state and events
from `EventDispatcher`, not parse market payloads themselves.

`CheckBinanceTags` follows the Delphi client: the latest successful response is
authoritative for tags. Known markets present in the response receive the new
tags; known markets absent from that response read back as empty tags. A late
payload read error is the only exception: already-read tags remain applied, and
old absent tags are not cleared because Delphi reaches the clear-unseen pass only
after the read loop completes.

`GetMarketsList` follows Delphi merge semantics. The first response populates
the market list. Later responses update known markets by name and leave old
names present if they are absent from the response; live price slots and token
tags for known markets are preserved. Unknown names from a later response are
added only when the list refresh was triggered by Delphi-style
`NewMarketFound`; otherwise they are ignored like Delphi frees the incoming
`TMarket`.

The server-index mapping is rebuilt from the `GetMarketsList` response order on
the first list and on a `NewMarketFound` refresh. A plain later
`GetMarketsList` updates known market fields but does not rewrite the current
`mIndex -> market name` mapping.

For existing markets, `max_leverage` is updated from `GetMarketsList` only when
the Delphi support flag `ES_MaxLevInGetMarkets` is active. In the active
library path this is inferred from `BaseCheck`: currently only
`Platform_FGate` (`exchange_code = 9`) enables it. New markets keep the value
from the incoming list because Delphi inserts the whole `TMarket`.

Correlation market definitions from `GetMarketsList` are inserted only when
their `base_currency_name` is non-empty, matching Delphi's `If not
BaseCur.IsEmpty then AddOrSetCorrMarket`. Repeated definitions for an existing
correlation market update `bn_tick_size` and `base_currency_name`, but keep the
original `bn_market_currency`, matching Delphi `AddOrSetCorrMarket`.
After a successful list, the active state also rebuilds Delphi
`TMarket.refBTCMarket` equivalents and `BaseCurDict` references. `refBTCMarket`
uses the current server base currency from `BaseCheck`: for a non-BTC base,
the library replaces that base currency text in the market name with `BTC` and
looks up the resulting CorrMarket name. For a BTC base it does nothing, like
Delphi `CheckCorrMarkets`.
Correlation market price updates are
merge-style for known correlation markets only: prices present in
`UpdateMarketsList` overwrite their entries, unknown names are ignored like
Delphi `GetCorrMarket(MName) = nil`, and absent known prices keep their
previous value. After each successful price update, `BaseCurrencyPrice.last_price`
is refreshed with Delphi priority: direct USDT market ask, reverse USDT market
ask inverse, direct USDT CorrMarket price, reverse USDT CorrMarket price inverse,
then `USDT = 1`.
For every applied market price row, `MarketPrice` also mirrors the Delphi
post-assign fields from `TMoonProtoEngine.UpdateMarketsList`:
`last_bid = bid`, `last_ask = ask`, `p_last = (bid + ask) / 2`, and
`min_lot_size = max(max(bn_step_size, bn_min_qty) * p_last, bn_min_notional)`.
`chart_price_step` mirrors Delphi `TMarket.ChartPriceStep` from
`AddNewAksPrice(Ask)`: both `UpdateMarketsList` and applied orderbook updates
can refresh it from the current ask; when `Ask > 0`, it becomes
`max(eps, Ask / 5000)`, and when `Ask` is zero/missing, the previous value is
kept.
When funding is included, the same row also updates
`Market::funding_rate` and `Market::funding_time`, matching Delphi's `TMarket`
mutation in the `HasFunding` branch.

Trades stream packets also update the bounded live trade tail kept by Delphi on
`TMarket`. For futures trade rows, the dispatcher updates
`MarketTradeState::last_got_all_trades_ms`, `last_trade_price`,
`last_buy_price`, `last_sell_price`, `last_trade_price_ema15`,
`last_trade_price_ema5`, and `last_trade_was_sell` before emitting the public
`TradesEvent::Applied` signal. Spot trade rows update only
`last_got_spot_trades_ms`, matching Delphi's spot branch which exits before
`SetLastTradePrices`.

If `UpdateMarketsList` refers to a server market index whose name is present in
`GetMarketsIndexes` but absent from the current market list, the active
dispatcher follows Delphi `NewMarketFound`: it schedules a fresh
`GetMarketsList` request automatically, throttled to roughly one request per
30 seconds while the unknown market condition persists. If that listing refresh
adds new markets, the active dispatcher emits
`MarketsEvent::NewMarketsAdded { names }` and immediately requests
`TAllStatusesReq` plus `UpdateMarketsList` again. The order snapshot mirrors
Delphi `AddNewMarket`: order pushes for an unknown market may have been dropped
before the local market object existed, so the full order snapshot is requested
again before the immediate price refresh.

Inbound listing notifications also force this listing refresh, but that command
is internal to the active library. User code should react to
`MarketsEvent::NewMarketsAdded { names }`, which is emitted only after
`GetMarketsList` actually inserted the named markets into `MarketsState`.

`UpdateMarketsList` carries server `mIndex` values. Price updates and
`price_by_index` resolve those indexes through the current `GetMarketsIndexes`
mapping, so stale mappings after a server restart are not used.

`MarketsState::last_markets_list_apply_timing()` is diagnostics only. It always
records coarse total/loop timing for the latest active `GetMarketsList` apply.
Per-row read/apply attribution is intentionally absent from production code:
thousands of timer calls inside the market/CorrMarket loops distort the CPU
path they are supposed to measure.

Funding timestamps match Delphi client state. The server serializes
`FundingTime - TZShift`; Rust parsers add the local client timezone shift back,
so `Market::funding_time` and `MarketPrice::funding_time` are client-local
Delphi `TDateTime` values. A zero funding time stays zero.

## Reading State

`MarketsState::get(name)` returns a stable `MarketHandle`, not a temporary
borrow. This mirrors Delphi `TMarkets = TSlowSafeList<TMarket>`: listing
refresh may replace the surrounding list/dictionaries, but existing `TMarket`
objects stay alive and are mutated in place. UI code may keep the handle after a
search and read it later without re-searching by name.

```rust
if let Some(market) = dispatcher.markets().get("BTCUSDT") {
    market.with(|market| {
        println!("tick={} max_lev={}", market.bn_tick_size, market.max_leverage);
    });
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
    // Historical name: emitted when a GetMarketsList response was applied.
    MarketsListReplaced { count: usize, corr_count: usize },
    NewMarketsAdded { names: Vec<String> },
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
    pub base_currency_prices: HashMap<String, BaseCurrencyPrice>,
    pub ref_btc_corr_markets: HashMap<String, String>,
    pub trade_states: HashMap<String, MarketTradeState>,
    pub token_tags: HashMap<String, TokenTags>,
    pub market_indexes: Vec<String>,
    pub indexes_synchronized: bool,
    pub markets_list_refresh_needed: bool,
}
```

```rust
pub struct MarketPrice {
    pub bid: f64,
    pub ask: f64,
    pub last_bid: f64,
    pub last_ask: f64,
    pub p_last: f64,
    pub min_lot_size: f64,
    pub chart_price_step: f64,
    pub funding_rate: f64,
    pub funding_time: f64,
    pub mark_price: f64,
    pub mark_price_found: bool,
}
```

```rust
pub struct BaseCurrencyPrice {
    pub base_currency: String,
    pub last_price: f64,
    pub usdt_market: Option<String>,
    pub usdt_rev_market: Option<String>,
    pub usdt_corr_market: Option<String>,
    pub usdt_rev_corr_market: Option<String>,
}
```

```rust
pub struct MarketTradeState {
    pub last_got_all_trades_ms: i64,
    pub last_got_spot_trades_ms: i64,
    pub last_trade_price: f64,
    pub last_buy_price: f64,
    pub last_sell_price: f64,
    pub last_trade_price_ema15: f64,
    pub last_trade_price_ema5: f64,
    pub last_trade_was_sell: bool,
}
```

The retained LastPrice line row is:

```rust
pub struct LastPricePoint {
    pub current: f32,
    pub real_time: f64, // Delphi TDateTime
}
```

This row mirrors Delphi `THistoricalPrices`. It is not the last trade price.
Delphi fills it from `UpdateMarketsList`: the server sends `Bid/Ask`, the
client computes `pLast = (Bid + Ask) / 2`, and the brown LastPrice chart line is
drawn from `Market.HistoryPrice`.

Retained storage uses `MarketHistoryStore::append_last_price_like_delphi`.
It appends a `LastPricePoint` only when Delphi `TMarket.AddFrom` would add a
`HistoryPrice` row: `pLast > 0`, bid or ask is present, and the market is a BTC
market or a base-USDT market.
When trades retained storage is active, `EventDispatcher` queues these rows into
its `MarketHistoryWorker` immediately. The default worker is lazy-created from
the all-trades subscription scope; `set_market_history_handle` is only needed
for custom capacities or externally owned storage. The UDP/protocol loop does
not write the retained ring directly.

`Market::futures_type` uses `BaseCurrency`, a small public wrapper that
preserves unknown future server values:

```rust
pub struct BaseCurrency(pub u8);

BaseCurrency::BTC;
BaseCurrency::USDT;
BaseCurrency::USDC;
BaseCurrency::EMPTY;
BaseCurrency::UNKNOWN;

let raw = market.futures_type.to_byte();
let value = BaseCurrency::from_byte(raw);
```

Known constants cover the currently named server values. Unknown future values
are preserved as their original byte instead of being collapsed to
`BaseCurrency::UNKNOWN`. For older servers that do not provide this field,
`Market::futures_type` is `BaseCurrency::EMPTY`.

`Market::listed_type_like_delphi()` returns the Delphi `TListedOnExchange`
post-processing result for `GetMarketsList`: `BaseCurrency::EMPTY` means
`ListedType::SPOT`; any other `futures_type` means `ListedType::BOTH`.
`ListedType` is a public ordinal wrapper for the derived listing kind.

Convenience methods:

```rust
let btc = dispatcher.markets().get("BTCUSDT"); // Option<MarketHandle>
let btc_snapshot = dispatcher.markets().market_snapshot("BTCUSDT");
dispatcher.markets().market_name_by_index(0);
dispatcher.markets().market_by_index(0);
dispatcher.markets().market_snapshot_by_index(0);
dispatcher.markets().market_index_by_name("BTCUSDT");
dispatcher.markets().price("BTCUSDT");
dispatcher.markets().price_by_index(0);
dispatcher.markets().ref_btc_corr_market("DOGEUSDT");
dispatcher.markets().base_currency_price("BTC");
dispatcher.markets().trade_state("BTCUSDT");
dispatcher.markets().tags("BTCUSDT");
dispatcher.markets().market_count();
dispatcher.markets().corr_count();
```

The index helpers return `None` while the mapping is stale after a server
restart. In the normal `MoonClient` path, trades and orderbook events are gated
until fresh indexes are received through init or an explicit `GetMarketsIndexes`
request.

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
