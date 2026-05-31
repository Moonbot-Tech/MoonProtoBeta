# Markets

Markets state is maintained from Engine API responses:

- `GetMarketsList` gives the full market list, correlation markets, and the
  initial `mIndex -> market name` order.
- `UpdateMarketsList` updates prices, funding, mark price, and correlation prices.
- `GetMarketsIndexes` refreshes that mapping after reconnect/server restart.
- `CheckBinanceTags` updates token tags.

When using `MoonClient`, relevant responses are applied to the active markets
read model automatically.

The active runtime applies `GetMarketsList`, `UpdateMarketsList`, and
`CheckBinanceTags` directly while reading the payload, matching Delphi's
in-loop state updates. Applications should read the maintained state and events
from `MoonClient` snapshots/events, not parse market payloads themselves.

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
`ExchangeCode::FGate` enables it. New markets keep the value
from the incoming list because Delphi inserts the whole `TMarket`.

Correlation market definitions from `GetMarketsList` are inserted only when
their `base_currency_name` is non-empty, matching Delphi's `If not
BaseCur.IsEmpty then AddOrSetCorrMarket`. Repeated definitions for an existing
correlation market update tick size and `base_currency_name`, but keep the
original exchange market currency, matching Delphi `AddOrSetCorrMarket`.
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
`min_lot_size = max(max(step_size, min_qty) * p_last, min_notional)`.
`chart_price_step` mirrors Delphi `TMarket.ChartPriceStep` from
`AddNewAksPrice(Ask)`: both `UpdateMarketsList` and applied orderbook updates
can refresh it from the current ask; when `Ask > 0`, it becomes
`max(eps, Ask / 5000)`, and when `Ask` is zero/missing, the previous value is
kept.
When funding is included, the same row also updates
`Market::funding_rate` and `Market::funding_time`, matching Delphi's `TMarket`
mutation in the `HasFunding` branch.

Trades stream packets also update the bounded live trade tail kept by Delphi on
`TMarket`. For futures trade rows, the runtime updates
`MarketTradeState::last_got_all_trades_ms`, `last_trade_price`,
`last_buy_price`, `last_sell_price`, `last_trade_price_ema15`,
`last_trade_price_ema5`, and `last_trade_was_sell` before emitting the public
`TradesEvent::Applied` signal. Spot trade rows update only
`last_got_spot_trades_ms`, matching Delphi's spot branch which exits before
`SetLastTradePrices`.

If `UpdateMarketsList` refers to a server market index whose name is present in
`GetMarketsIndexes` but absent from the current market list, the active
runtime follows Delphi `NewMarketFound`: it schedules a fresh
`GetMarketsList` request automatically, throttled to roughly one request per
30 seconds while the unknown market condition persists. If that listing refresh
adds new markets, the runtime emits
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

`MarketsState::last_markets_list_apply_timing()` is diagnostics only. In test
or `--features diagnostics` builds it records coarse total/loop timing for the
latest active `GetMarketsList` apply; regular builds return `None` and do not
pay for these timer reads.
Per-row read/apply attribution is intentionally absent from production code:
thousands of timer calls inside the market/CorrMarket loops distort the CPU
path they are supposed to measure.

Funding timestamps match Delphi client state. The server serializes
`FundingTime - TZShift`; Rust parsers add the local client timezone shift back,
so `Market::funding_time` and `MarketPrice::funding_time` are client-local
Delphi `TDateTime` values. A zero funding time stays zero.
They are not Unix timestamps; use `funding_time_delphi().unix_millis()` when
the UI needs wall-clock time.

## Reading State

`MarketsState::get(name)` returns a stable `MarketHandle`, not a temporary
borrow. This mirrors Delphi `TMarkets = TSlowSafeList<TMarket>`: listing
refresh may replace the surrounding list/dictionaries, but existing `TMarket`
objects stay alive and are mutated in place. UI code may keep the handle after a
search and read it later without re-searching by name.

```rust
use moonproto::TokenTags;

let Some(state) = client.snapshot() else { return; };
let markets = state.markets();

if let Some(market) = markets.get("BTCUSDT") {
    let pos = market.balance_position();
    let price = market.price();
    let tail = market.trade_state();
    market.with(|market| {
        println!("tick={} max_lev={}", market.tick_size(), market.max_leverage);
    });
    println!(
        "liq={} bid={} ask={} mark={} last_trade={} funding_ms={:?}",
        pos.liq_price,
        price.bid,
        price.ask,
        price.mark_price,
        tail.last_trade_price,
        price.funding_time_delphi().unix_millis()
    );
}

let tags = markets.tags("BTCUSDT");
if tags.contains(TokenTags::ALPHA) {
    println!("BTCUSDT has ALPHA tag");
}
```

Balance and position packets update these same live `Market` objects. For chart
UI this is the normal path: keep the selected `MarketHandle` and read fields
such as `pos_size`, `pos_price`, `liq_price`, `leverage_x`, `asset_balance`,
`total_profit_*`, and `max_value` from `balance_position()`. `BalancesState` is the account
totals view, not the primary per-market UI object.

For chart overlays that only need position fields, `MarketHandle::balance_position`
returns a small copy without cloning the whole market object.
For price/funding/mark-price and live trade-tail overlays, use
`MarketHandle::price()` and `MarketHandle::trade_state()` on the same retained
handle instead of resolving the market name again.

Arbitrage relay packets also apply to the live market. Use
`MarketHandle::arb_slot(ArbPlatformCode::...)` or
`arb_now(ArbPlatformCode::...)` from the
selected handle; raw arb `market_index` blocks are diagnostic protocol details.

## Init and Refresh

Initial fetch:

```rust
use moonproto::{ConnectConfig, InitConfig, MoonClient};

let init = InitConfig {
    ..Default::default()
};
let client = MoonClient::connect(cfg, ConnectConfig::new(init))?;
```

Long-running price refresh is controlled by `ClientConfig.refresh`. The default
uses the Delphi worker cadence, but ticks are gated by Init: transport `Fine`
does not start background Engine API. Set `update_markets_every` /
`check_tags_every` to `None` if the application owns those requests manually.

See `examples/market_refresh.rs` for a compact consumer-side loop that reads
prices and tags from `MoonClient`.

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

`MarketsState::indexes_synchronized()` is a critical invariant.
Cold Init builds the initial map from `GetMarketsList`, exactly like Delphi
`SrvMarkets.Rebuild(IndexMap)` inside `TMoonProtoEngine.GetMarketsList`. After
server restart the runtime can mark it stale. If the one-time Init already
completed, reconnect restore sends `GetMarketsIndexes` automatically and only
then refreshes prices with `UpdateMarketsList`. Until the fresh response
arrives, the active runtime drops orderbook/trades packets that depend on server
indexes.
Price updates keyed by server `mIndex` are also skipped while a previously known
mapping is stale.

## Public State

`MarketsState` is a read API over the live market catalog. Its internal COW
maps/lists are not public surface: use `iter()`, `get()`, `market_*`,
`price*`, `tags()`, `trade_state()`, and the count helpers instead of reaching
into storage fields.

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
let price = point.price();
let unix_ms = point.unix_millis();
let delphi_time = point.time_delphi();
```

The row keeps Delphi-compatible dense storage internally; UI code should use
`price()`, `time_delphi()`, or `unix_millis()` instead of treating the raw time
field as Unix time.

This row mirrors Delphi `THistoricalPrices`. It is not the last trade price.
Delphi fills it from `UpdateMarketsList`: the server sends `Bid/Ask`, the
client computes `pLast = (Bid + Ask) / 2`, and the brown LastPrice chart line is
drawn from `Market.HistoryPrice`.

The retained-history worker appends a `LastPricePoint` only when Delphi
`TMarket.AddFrom` would add a `HistoryPrice` row: `pLast > 0`, bid or ask is
present, and the market is a BTC market or a base-USDT market.

The retained MarkPrice line row has the same shape:

```rust
let mark_price = point.price();
let mark_time = point.time_delphi();
```

It is filled from `UpdateMarketsList -> MarketPrice.mark_price` when the server
marks the value as present. UI code can compare the MarkPrice line with the
LastPrice line for the same market; both are retained by the same
`MarketHistoryWorker`.

When trades retained storage is active, `MoonClient` queues these rows into its
retained-history worker immediately after applying market prices. The default
worker is lazy-created from the all-trades subscription scope. The UDP/protocol
loop does not write the retained ring directly.

`Market::futures_type` uses `BaseCurrency`, a small public wrapper that
preserves unknown future server values:

```rust
pub struct BaseCurrency;

BaseCurrency::BTC;
BaseCurrency::USDT;
BaseCurrency::USDC;
BaseCurrency::EMPTY;
BaseCurrency::UNKNOWN;

let label = market.futures_type.name();
```

Known constants cover the currently named server values. Unknown future values
are preserved as their original byte instead of being collapsed to
`BaseCurrency::UNKNOWN`. For older servers that do not provide this field,
`Market::futures_type` is `BaseCurrency::EMPTY`.
Use `BaseCurrency::name()` for UI labels; `to_byte()` / `from_byte()` are for
protocol diagnostics and roundtrip tests.

`Market::listed_type()` returns the Delphi `TListedOnExchange`
post-processing result for `GetMarketsList`: `BaseCurrency::EMPTY` means
`ListedType::SPOT`; any other `futures_type` means `ListedType::BOTH`.
`ListedType` is a public ordinal wrapper for the derived listing kind.

Convenience methods:

```rust
let Some(state) = client.snapshot() else { return; };
let markets = state.markets();

for handle in markets.iter() {
    handle.with(|market| {
        println!("{} {}", market.symbol(), market.status_trading);
    });
}

let btc = markets.get("BTCUSDT"); // Option<MarketHandle>
let btc_snapshot = markets.market_snapshot("BTCUSDT");
markets.price("BTCUSDT");
markets.ref_btc_corr_market("DOGEUSDT");
markets.base_currency_price("BTC");
markets.trade_state("BTCUSDT");
markets.tags("BTCUSDT");
markets.market_count();
markets.corr_count();
```

Server-index helpers such as `market_index_by_name`, `market_name_by_index`, and
`price_by_index` are diagnostic protocol tools. Normal UI code keeps a
`MarketHandle` or reads by market name. In the normal `MoonClient` path, trades
and orderbook events are gated until fresh indexes are rebuilt by cold-init
`GetMarketsList` or refreshed through `GetMarketsIndexes` after reconnect/server
restart.

## TokenTags

```rust
pub struct TokenTags;

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
