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
flushes the current registry once. After Init, reconnect replays it
automatically. Unsubscribe removes the registry intent and sends
`emk_UnsubscribeAllTrades` only after `domain_ready`.

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

`GapDetected`, `GapFilled`, `BucketClosed`, `Duplicate`, and resend-side
`OutOfOrder` are diagnostic events. They are useful for logging/telemetry, but
applications must not drive recovery from them: `Client::run_with_dispatcher`
sends resend requests and maintains buckets automatically. The dispatcher still
emits `Apply(packet)` for duplicate/resend payloads when Delphi would parse the
payload.

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

## Recovery Behavior

`TradesState` maintains up to 50 gap buckets. Each bucket retries missing packet
numbers up to three times with the Delphi delay formula:

```text
PathDelay = min(1800, max(300, RTT * (1.2 + retry * 0.7))) ms
```

`Client::run_with_dispatcher` calls the trades tick about every 100 ms and sends
the generated `emk_TradesResend` requests automatically.

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

for resend_request in trades.tick(rtt_ms, now_ms) {
    client.send_api_request(&resend_request);
}

for raw_packet in parse_trades_resend_response(resend_response_payload) {
    if let Some(packet) = parse_trades_packet(&raw_packet) {
        let historical_events = trades.on_packet_resend(packet);
    }
}
```

Regular applications should not send resend requests manually.
