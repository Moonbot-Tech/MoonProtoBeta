# Markets

`MoonClient` maintains the market universe and live market read model for the
application. UI code searches by symbol once, keeps a stable `MarketHandle`, and
reads prices, funding, tags, balances/positions, arbitrage slots, and retained
history from snapshots/handles.

The runtime owns the server refreshes that feed this state: full market list,
incremental price/funding updates, token tags, correlation prices, and
server-index refresh after reconnect/server restart. Applications should read
the maintained state and events from `MoonClient` snapshots/events, not parse
market payloads or server indexes themselves.

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
    let deltas = market.delta_state();
    let protection = state.position_protection_for(&market);
    market.with(|market| {
        println!("tick={} max_lev={}", market.tick_size(), market.max_leverage);
    });
    println!(
        "liq={} bid={} ask={} mark={} last_trade={} coin1h={} protected={}",
        pos.liq_price,
        price.bid,
        price.ask,
        price.mark_price,
        tail.last_trade_price,
        deltas.coin_1h_delta,
        !protection.both.has_warning
    );
}

let global_deltas = markets.global_deltas();
println!("btc1h={} exchange1h={}", global_deltas.btc_1h_delta, global_deltas.exchange_1h_delta);

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
For the Delphi "unprotected position" warning, use
`snapshot.position_protection_for(&market)`: the library counts active
non-emulator `SellSet` close orders by side, and the UI only decides how to
draw/blink that warning.
For price/funding/mark-price and live trade-tail overlays, use
`MarketHandle::price()` and `MarketHandle::trade_state()` on the same retained
handle instead of resolving the market name again.
For signed MoonBot signal deltas, use `MarketHandle::delta_state()` for the
selected market and `MarketsState::global_deltas()` for BTC/exchange signals.
These are separate from retained-history range/max-move analytics. If the UI
wants Delphi's "Exclude blacklisted markets from the market delta calculation"
checkbox, call
`client.settings().set_exclude_blacklisted_markets_from_exchange_delta(true)`;
the runtime then applies `coins_black_list_text` to retained markets before
computing `Exchange1hDelta` / `Exchange24hDelta`.

Arbitrage relay packets also apply to the live market. Use
`MarketHandle::arb_slot(ArbPlatformCode::...)` or
`arb_now(ArbPlatformCode::...)` from the
selected handle; raw arb `market_index` blocks are diagnostic protocol details.
Arb price entries expose `time()` / `unix_millis()` helpers; the fixed ring
cursor is diagnostics/test-only.

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

## Public State

`MarketsState` is a read API over the live market catalog. Its internal COW
maps/lists and server-index helpers are not the terminal surface. Normal UI code
uses `iter()`, `get() -> MarketHandle`, `market_snapshot(name)`, `price(name)`,
`tags(name)`, `trade_state(name)`, `delta_state(name)`, `global_deltas()`,
`exclude_blacklisted_markets_from_exchange_delta()`, and the count helpers.
Selected-market UI should keep the `MarketHandle` returned by `get()` and read
through that handle.

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
    pub mark_price: f64,
    pub mark_price_found: bool,
}

impl MarketPrice {
    pub fn funding_time(self) -> MoonTime;
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

```rust
pub struct MarketDeltaState {
    pub last_price_ema: f64,
    pub coin_1h_avg: f64,
    pub coin_24h_avg: f64,
    pub coin_1h_delta: f64,
    pub coin_1h_delta_ema: f64,
    pub coin_24h_delta: f64,
    pub coin_24h_delta_ema: f64,
}

pub struct MarketGlobalDeltas {
    pub btc_1h_avg: f64,
    pub btc_24h_avg: f64,
    pub btc_72h_avg: f64,
    pub btc_1h_delta: f64,
    pub btc_24h_delta: f64,
    pub btc_72h_delta: f64,
    pub exchange_1h_delta: f64,
    pub exchange_24h_delta: f64,
    pub exchange_market_count: usize,
}
```

The retained LastPrice line row is:

```rust
let price = point.price();
let unix_ms = point.unix_millis();
let time = point.time();
```

UI code should use `price()`, `time()`, or `unix_millis()` instead of carrying
raw protocol time.

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
let mark_time = point.time();
```

It is filled from `UpdateMarketsList -> MarketPrice.mark_price` when the server
marks the value as present. UI code can compare the MarkPrice line with the
LastPrice line for the same market; both are retained in the same per-market
history model.

When trades retained storage is active, `MoonClient` appends these rows
immediately after applying market prices. Retained history is created lazily
from the active trades subscription scope, so markets outside
`subscribe_trades_for` do not allocate price-line rings.

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
Use `BaseCurrency::name()` for UI labels.

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
markets.delta_state("BTCUSDT");
markets.global_deltas();
markets.tags("BTCUSDT");
markets.market_count();
markets.corr_count();
```

Server-index mapping is runtime/diagnostic protocol state. Normal UI code keeps
a `MarketHandle` or reads by market name. In the normal `MoonClient` path,
trades and orderbook events are gated until fresh indexes are rebuilt by
cold-init `GetMarketsList` or refreshed through `GetMarketsIndexes` after
reconnect/server restart.

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

## Behavior Notes

These notes describe how the active runtime keeps the public read model current.
Regular UI code should still use `MarketHandle`, market history readers, and
typed events instead of server-index helpers.

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
Correlation market price updates are merge-style for known correlation markets
only: prices present in `UpdateMarketsList` overwrite their entries, unknown
names are ignored like Delphi `GetCorrMarket(MName) = nil`, and absent known
prices keep their previous value.

After each successful price update, `BaseCurrencyPrice.last_price` is refreshed
with Delphi priority: direct USDT market ask, reverse USDT market ask inverse,
direct USDT CorrMarket price, reverse USDT CorrMarket price inverse, then
`USDT = 1`.
For every applied market price row, `MarketPrice` also mirrors the Delphi
post-assign fields from `TMoonProtoEngine.UpdateMarketsList`:
`last_bid = bid`, `last_ask = ask`, `p_last = (bid + ask) / 2`, and
`min_lot_size = max(max(step_size, min_qty) * p_last, min_notional)`.
`chart_price_step` mirrors Delphi `TMarket.ChartPriceStep` from
`AddNewAksPrice(Ask)`: both `UpdateMarketsList` and applied orderbook updates
can refresh it from the current ask; when `Ask > 0`, it becomes
`max(eps, Ask / 5000)`, and when `Ask` is zero/missing, the previous value is
kept.
When funding is included, the same row also updates the retained market funding
rate/time, matching Delphi's `TMarket` mutation in the `HasFunding` branch.

Funding timestamps match Delphi client state. The server serializes
`FundingTime - TZShift`; Rust parsers add the local client timezone shift back,
so retained funding time is client-local Delphi `TDateTime`. A zero funding
time stays zero. It is not Unix time; UI code should use
`MarketHandle::price().funding_time().unix_millis()` or
`Market::funding_time()` instead of carrying a raw `f64` timestamp.

Trades stream packets also update the bounded live trade tail kept by Delphi on
`TMarket`. For futures trade rows, the runtime updates
`MarketTradeState::last_got_all_trades_ms`, `last_trade_price`,
`last_buy_price`, `last_sell_price`, `last_trade_price_ema15`,
`last_trade_price_ema5`, and `last_trade_was_sell` before emitting the public
`TradesEvent::Applied` signal. Spot trade rows update only
`last_got_spot_trades_ms`, matching Delphi's spot branch which exits before
`SetLastTradePrices`.

If `UpdateMarketsList` refers to a server market index whose name is present in
`GetMarketsIndexes` but absent from the current market list, the active runtime
follows Delphi `NewMarketFound`: it schedules a fresh `GetMarketsList` request
automatically, throttled to roughly one request per 30 seconds while the unknown
market condition persists. If that listing refresh adds new markets, the
runtime emits `MarketsEvent::NewMarketsAdded { names }` and immediately
requests a fresh full order-status snapshot plus `UpdateMarketsList` again.
Order pushes for an unknown market may have been dropped before the local market
object existed, so the full order snapshot is requested again before the
immediate price refresh.

Inbound listing notifications also force this listing refresh, but that command
is internal to the active library. User code should react to
`MarketsEvent::NewMarketsAdded { names }`, which is emitted only after
`GetMarketsList` actually inserted the named markets into `MarketsState`.

`UpdateMarketsList` carries server `mIndex` values. Price updates resolve those
indexes through the current `GetMarketsIndexes` mapping, so stale mappings after
a server restart are not used.
