# Trades Stream

The trades stream carries exchange trades, market-maker orders, liquidation
orders, and watcher fills. Packets are numbered with wrapping `u16` packet
numbers. The library detects gaps and requests resend batches automatically when
you run through `Client::run_with_dispatcher`.

## Subscribe

```rust
client.subscribe_all_trades(false); // false = trades only, true = include MM orders
```

The subscription is registry-aware and is replayed after reconnect.

## Events

```rust
use moonproto::Event;
use moonproto::state::TradesEvent;
use moonproto::commands::trades_stream::TradeSection;

client.run_with_dispatcher(duration, &mut dispatcher, Box::new(|event| {
    if let Event::Trade(trade_event) = event {
        match trade_event {
            TradesEvent::Apply(packet) => {
                for section in &packet.sections {
                    match section {
                        TradeSection::Trades(trades) => {
                            for trade in trades {
                                on_trade(trade.market_index, trade.is_spot, trade.price, trade.qty);
                            }
                        }
                        TradeSection::MMOrders(orders) => on_mm_orders(orders),
                        TradeSection::LiqOrders(orders) => on_liquidations(orders),
                        TradeSection::WatcherFills { market_index, user, data } => {
                            on_watcher_fills(*market_index, user, data);
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

`Duplicate` and resend-side `OutOfOrder` are diagnostic events. The dispatcher
still emits `Apply(packet)` for the same packet payload, matching the Delphi
client's stream handling.

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
```

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
