# Trades Stream

The trades stream carries exchange trades, market-maker orders, liquidation
orders, and watcher fills. Packets are numbered with wrapping `u16` packet
numbers. The library detects gaps and requests resend batches automatically when
you run through `Client::run_with_dispatcher`.

## Subscribe

```rust
client.subscribe_all_trades(false); // false = trades only, true = include MM orders
client.unsubscribe_all_trades();
```

The subscription is registry-aware. Before Init, subscribe and unsubscribe calls
update only the registry and send no Engine API wire packet. The one-time Init
flushes the current registry once using the exact stored `want_mm` value; the
post-init `TMMOrdersSubscribeCommand` does not rewrite this all-trades value.
After Init, reconnect replays it automatically using the Delphi reconnect
sequence: if the current `ServerToken` has not yet been observed on
`MPC_TradesStream`, the maintenance tick sends `emk_UnsubscribeAllTrades`, waits
100 ms, then sends `emk_SubscribeAllTrades`. The sequence is retried no more
often than once per 5000 ms until a trades packet for the current `ServerToken`
reaches the parser.
Queuing `emk_SubscribeAllTrades` arms that gate, and a successful response
refreshes it again. This preserves the Delphi `SendAndWait` effect: the library
does not immediately unsubscribe from a stream it has just subscribed to while
waiting for the first trades packet.
Unsubscribe removes the registry intent and sends `emk_UnsubscribeAllTrades`
only after `domain_ready`.

Unlike Delphi MoonBot, the Rust library does not subscribe to all-trades unless
the application asks for it. This is an accepted author decision for the public
library API. If no all-trades intent is present in the registry, incoming
`MPC_TradesStream` and `MPC_TradesResendResponse` packets are considered
unexpected and are dropped instead of being emitted as public events.

The public call always updates the reconnect registry immediately. Once Init is
open, changed subscription intent also appends the wire Engine API request to
the Delphi-style send queues. On the wire, `emk_SubscribeAllTrades` /
`emk_UnsubscribeAllTrades` are Engine API requests and their success or failure
is reported as a later `Event::EngineResponse`. Trades packets then arrive
asynchronously on the `MPC_TradesStream` channel.

## Events

```rust
use moonproto::Event;
use moonproto::state::TradesEvent;
use moonproto::commands::trades_stream::TradeSection;

client.run_with_dispatcher_state(duration, &mut dispatcher, Box::new(|event, state| {
    if let Event::Trade(trade_event) = event {
        match trade_event {
            TradesEvent::Apply(packet) => {
                for section in &packet.sections {
                    match section {
                        TradeSection::Trades(trades) => {
                            for trade in trades {
                                let market_name = state.markets().market_name_by_index(trade.market_index);
                                on_trade(market_name, trade.is_spot, trade.price, trade.qty);
                            }
                        }
                        TradeSection::MMOrders(orders) => on_mm_orders(orders),
                        TradeSection::LiqOrders(orders) => on_liquidations(orders),
                        TradeSection::WatcherFills { market_index, user, .. } => {
                            let fills = section.watcher_fill_records()
                                .expect("library emits complete watcher-fill records");
                            on_watcher_fills(*market_index, user, &fills);
                        }
                    }
                }
            }
            TradesEvent::GapDetected { start, end } => log_gap(*start, *end),
            TradesEvent::Duplicate => log_duplicate_packet(),
            TradesEvent::OutOfOrder { packet_num } => log_out_of_order(*packet_num),
            TradesEvent::GapFilled { packet_num, .. } => log_gap_filled(*packet_num),
            TradesEvent::ResendRequested { packet_nums } => log_resend(packet_nums),
            TradesEvent::BucketClosed { .. } => {}
        }
    }
}));
```

`qty` sign encodes direction: positive is buy-side, negative is sell-side.
Use `MarketsState::market_name_by_index` / `market_by_index` to resolve the
stream `market_index` into the canonical market name. The dispatcher suppresses
stream events while indexes are stale after a server restart; after the one-time
Init, reconnect restore sends `GetMarketsIndexes` automatically when trades were
requested.

Watcher fills are not opaque for applications. The section keeps the original
raw `data` for compatibility with low-level tools, but regular consumers should
decode it through `TradeSection::watcher_fill_records()` or
`parse_watcher_fills(data)` and handle typed `WatcherFill` records together with
the section-level `market_index` and `user` address.

`GapDetected`, `ResendRequested`, `GapFilled`, `BucketClosed`, `Duplicate`, and
resend-side `OutOfOrder` are diagnostic events. They are useful for
logging/telemetry, but applications must not drive recovery from them:
`Client::run_with_dispatcher` sends resend requests and maintains buckets
automatically. `ResendRequested` means the Delphi-style tail check after a
valid trades packet queued `emk_TradesResend` for the listed packet numbers.
The dispatcher still emits `Apply(packet)` for duplicate/resend payloads when
Delphi would parse the payload.

Before an `Apply(packet)` event is emitted, `Client::run_with_dispatcher` also
updates `MarketsState::trade_state(market)` for every known futures/spot trade
row. This mirrors the bounded Delphi `ProcessTradesStream` tail: futures trades
update `LastGotAllTrades` and the `SetLastTradePrices` fields, while spot trades
only update `LastGotSpotTrades`.

## Public Types

```rust
pub struct TradesPacket {
    pub base_time: f64,
    pub packet_num: u16,
    pub sections: Vec<TradeSection>,
}

pub enum TradeSection {
    Trades(Vec<Trade>),
    MMOrders(Vec<MMOrder>),
    LiqOrders(Vec<LiqOrder>),
    WatcherFills { market_index: u16, user: [u8; 20], data: Vec<u8> },
}

pub struct Trade {
    pub market_index: u16,
    pub is_spot: bool,
    pub time_delta_ms: i16,
    pub price: f32,
    pub qty: f32,
}

pub struct WatcherFill {
    pub time_delta_ms: i16,
    pub price: f32,
    pub qty: f32,
    pub z_btc: f32,
    pub position: f32,
    pub order_type: u8,
    pub flags: u8,
}
```

`WatcherFill::is_short()`, `is_open()`, and `is_taker()` decode the Delphi flags
byte. `time_delta_ms` is relative to `TradesPacket::base_time`.

## Retained History Building Blocks

The active-library retained history storage is being wired separately from the
event stream. The public row types already mirror the Delphi storage records and
can be used by tools/tests that build retained history explicitly.

```rust
pub struct TradeHistoryRow {
    pub time: f64,  // Delphi TDateTime
    pub price: f32,
    pub qty: f32,  // signed: sign bit clear = buy, sign bit set = sell
}

impl TradeHistoryRow {
    pub fn quantity(self) -> f32;        // Abs(qty)
    pub fn is_buy(self) -> bool;         // Delphi sign-bit check
    pub fn same_direction(self, other: Self) -> bool;
    pub fn traded_value(self) -> f32;    // price * Abs(qty)
}

pub struct MMOrderHistoryRow {
    pub time: f64, // Delphi TDateTime
    pub vol: f64,
    pub q: f64,
}

pub struct MiniCandle {
    pub time: f64, // Delphi TDateTime
    pub cnt: i32,
    pub min_price: f32,
    pub max_price: f32,
    pub buy_vol: f32,
    pub sell_vol: f32,
}
```

`TradeHistoryRow` is the retained form for detailed trades and liquidations. It
matches Delphi `TTrade`: `Time: TDateTime`, `Price: Single`, signed
`Qty: Single`. Direction uses the raw `Qty` sign bit, so `-0.0` is sell-side,
matching Delphi `PCardinal(@Qty)^ and $80000000`.

`MMOrderHistoryRow` matches Delphi base `TMMOrder`: `Time`, `vol`, and `Q` are
stored as doubles. HyperDex taker address/color companion data is a separate
storage layer; it is not folded into the base row.

```rust
pub fn compact_trades_to_mini_candles_like_delphi(
    rows: &[TradeHistoryRow],
    last_mini_time: f64,
    now_time: f64,
    out: &mut Vec<MiniCandle>,
);
```

The compaction helper mirrors Delphi `TMarket.ResizeOrdersHistory`: it groups
detailed trades by a 5-second anchor window, calculates buy/sell volume as
`Price * Abs(Qty)`, maintains min/max price, and applies the same `T1`/`Now`
append gates used when old detailed trades are turned into `TMiniCandle`.

```rust
pub struct TradeVolumeTotals {
    pub buy_value: f64,
    pub sell_value: f64,
    pub buy_qty: f64,
    pub sell_qty: f64,
    pub trade_count: u32,
}

pub struct RollingTradeVolumeSnapshot {
    pub one_minute: TradeVolumeTotals,
    pub three_minutes: TradeVolumeTotals,
    pub five_minutes: TradeVolumeTotals,
}

pub struct RollingTradeVolumes;

impl RollingTradeVolumes {
    pub fn add_trade(&mut self, row: TradeHistoryRow);
    pub fn snapshot(&self, now_time: f64) -> RollingTradeVolumeSnapshot;
    pub fn window(&self, now_time: f64, window_seconds: i64) -> TradeVolumeTotals;
}
```

`RollingTradeVolumes` uses 5-second buckets and updates from newly received
trades. It is the active-library derived-state path for 1/3/5 minute buy/sell
volumes; the intended precision loss is bounded by one bucket width.

## Recovery Behavior

`TradesState` maintains up to 50 gap buckets. Each bucket retries missing packet
numbers up to three times with the Delphi delay formula:

```text
PathDelay = min(1800, max(300, RTT * (1.2 + retry * 0.7))) ms
```

`Client::run_with_dispatcher` calls the trades recovery check after successfully
parsed live/resend trades packets, under the same 100 ms `LastCheckMissingTime`
throttle as Delphi, and sends the generated `emk_TradesResend` requests
automatically.

Recovery is best-effort, matching Delphi. Missing packet numbers are requested
for up to three bucket retry cycles. If the bucket is still incomplete after the
retry budget, it is closed and the live stream continues; the library does not
keep flooding the channel for old trades. A late resend packet can still be
parsed and emitted as `Apply(packet)`, but it no longer reopens the closed
bucket.

`EventDispatcher` also drops trades packets while market indexes are not
synchronized after a server restart.

## Low-Level Use

Custom tools can use the parser and state directly:

```rust
use moonproto::commands::trades_stream::parse_trades_packet;
use moonproto::state::{parse_trades_resend_response, TradesState};

let mut trades = TradesState::new();
let packet = parse_trades_packet(payload).expect("bad trades packet");
let events = trades.on_packet(packet, now_ms);

// Delphi-equivalent: call after a successfully parsed trades packet, not from
// an independent timer while no trades packets are arriving.
for resend_request in trades.tick(rtt_ms, now_ms) {
    client.send_api_request(&resend_request);
}

for raw_packet in parse_trades_resend_response(resend_response_payload) {
    if let Some(packet) = parse_trades_packet(&raw_packet) {
        let historical_events = trades.on_packet_resend(packet);
        for resend_request in trades.tick(rtt_ms, now_ms) {
            client.send_api_request(&resend_request);
        }
    }
}
```

Regular applications should not send resend requests manually.
