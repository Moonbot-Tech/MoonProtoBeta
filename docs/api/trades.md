# TradesStream channel (MPC_TradesStream)

Real-time поток сделок с биржи + market-maker заявки + ликвидации + watcher fills.

## Что это

Сервер агрегирует трейды с биржи и шлёт пакетами по `packet_num: u16` (wrapping).
Каждый пакет содержит несколько секций:

- **Trades** (section_type 0, 2): обычные сделки. `0` = Futures, `2` = Spot.
- **MMOrders** (section_type 1): market-maker заявки (выставлены и отозваны).
- **Extended** (section_type 3): Liquidation orders (`ext_type=0`) или Watcher fills (`ext_type=1`).

`MoonProto` гарантирует доставку через **GapBucket logic** — если пакет потерян,
клиент автоматически запрашивает resend через `emk_TradesResend`. Liба делает
это сама в `run_with_dispatcher` (periodic `tick_trades` ~100мс).

## Подписка

```rust
use moonproto::state::OrderBookKind;

// Через ClientSender (thread-safe из любого thread'а):
let sender = client.sender();
sender.subscribe_all_trades(true);    // true = с MM ордерами, false = без

// Или сразу на &Client:
client.subscribe_all_trades(true);

// Отписаться:
client.unsubscribe_all_trades();
```

После подписки сервер начинает слать `MPC_TradesStream` пакеты. Liба сама
auto-replay'ит подписку при reconnect через subscription registry.

## EventDispatcher (рекомендуемый pattern)

```rust
use moonproto::events::{EventDispatcher, Event};
use moonproto::state::TradesEvent;

let mut dispatcher = EventDispatcher::new();

client.run_with_dispatcher(duration, &mut dispatcher, Box::new(|ev| match ev {
    Event::Trade(te) => match te {
        TradesEvent::Apply(pkt) => {
            // Раздать sections по UI
            for section in &pkt.sections {
                // ... match section { TradeSection::Trades(v) => ..., ... }
            }
        }
        TradesEvent::GapDetected { start, end } => {
            log::debug!("trades gap detected [{start}..={end}] — recover автоматически");
        }
        TradesEvent::Duplicate => { /* skip */ }
        TradesEvent::GapFilled { packet_num, .. } => {
            log::debug!("late packet {packet_num} arrived");
        }
        TradesEvent::BucketClosed { all_received, retry_count, .. } => {
            if !all_received {
                log::warn!("trades bucket gave up after {retry_count} retries");
            }
        }
        TradesEvent::OutOfOrder { .. } => {}
    },
    _ => {}
}));
```

При `run_with_dispatcher` либа сама вызывает `dispatcher.tick_trades(rtt, now)`
каждые ~100мс — gap recovery работает без участия app.

## Низкоуровневый pattern (без EventDispatcher)

```rust
use moonproto::commands::trades_stream::parse_trades_packet;
use moonproto::state::{TradesState, TradesEvent};

let mut trades = TradesState::new();
let rtt_ms = 280;    // последний измеренный RTT
let now_ms = /* current ms */;

if let Some(pkt) = parse_trades_packet(&payload) {
    let events = trades.on_packet(pkt, now_ms);
    for te in events {
        match te {
            TradesEvent::Apply(pkt) => { /* распакован пакет в pkt */ }
            TradesEvent::GapDetected { .. } => {}
            TradesEvent::Duplicate => {}
            _ => {}
        }
    }
}

// Периодический tick (раз в ~100мс) для retry:
for resend_payload in trades.tick(rtt_ms, now_ms) {
    client.send_api_request(&resend_payload);
}
```

`trades.tick(rtt_ms, now_ms)` возвращает `Vec<Vec<u8>>` — каждый элемент это
готовый `emk_TradesResend` payload (auto-batched до 200 packet_nums).

## TradesPacket structure

```rust
pub struct TradesPacket {
    pub base_time:  f64,             // TDateTime (Delphi double, дни с 1899-12-30)
    pub packet_num: u16,             // wrapping
    pub sections:   Vec<TradeSection>,
}

pub enum TradeSection {
    Trades(Vec<Trade>),
    MMOrders(Vec<MMOrder>),
    LiqOrders(Vec<LiqOrder>),
    WatcherFills { market_index: u16, user: [u8; 20], data: Vec<u8> },
}
```

`TradeSection` — **enum** (не struct). Match'и в коде:

```rust
for section in &pkt.sections {
    match section {
        TradeSection::Trades(trades)    => { /* обычные сделки */ }
        TradeSection::MMOrders(orders)  => { /* MM-ордера */ }
        TradeSection::LiqOrders(orders) => { /* ликвидации */ }
        TradeSection::WatcherFills { market_index, user, data } => { /* watcher */ }
    }
}
```

## Trade structure

```rust
pub struct Trade {
    pub market_index:  u16,    // 14-битный (max 16383) — мы маскируем bit_or 0x3FFF
    pub is_spot:       bool,   // true = Spot section (section_type=2), false = Futures (=0)
    pub time_delta_ms: i16,    // offset от pkt.base_time в миллисекундах
    pub price:         f32,    // не f64! — wire-формат 4 байта
    pub qty:           f32,    // ЗНАК = direction: negative = SELL, positive = BUY
}
```

**Внимание**: `qty` знаковый — отрицательное = SELL, положительное = BUY. Это
байт-экономия Delphi wire-формата (один f32 вместо bool+f32).

Время трейда: `actual_time = pkt.base_time + time_delta_ms / (86400 * 1000)`
(TDateTime в днях, time_delta_ms в милисекундах).

## MMOrder structure

```rust
pub struct MMOrder {
    pub market_index:  u16,
    pub time_delta_ms: i16,
    pub vol:           f32,
    pub q:             f32,
    pub taker:         Option<[u8; 20]>,    // только если flag TRADES_FLAG_HAS_TAKER в пакете
}
```

## LiqOrder structure

```rust
pub struct LiqOrder {
    pub market_index:  u16,
    pub time_delta_ms: i16,
    pub price:         f32,
    pub qty:           f32,
}
```

## WatcherFills structure

`TradeSection::WatcherFills { market_index, user, data }` — raw 20-byte fills,
формат специфичен app-layer (см. Delphi `WatcherU.pas`).

## TradesEvent

```rust
pub enum TradesEvent {
    /// Пакет применён — раздать pkt.sections по UI.
    Apply(TradesPacket),
    /// Обнаружен gap: пропущены packet_num в [start..=end]. Bucket создан, retry в tick().
    GapDetected { start: u16, end: u16 },
    /// Пакет дубликат (packet_num == last) — отброшен.
    Duplicate,
    /// Out-of-order пакет (пришёл после reset / wrap).
    OutOfOrder { packet_num: u16 },
    /// Принят out-of-order пакет, который был помечен в одном из gap-bucket'ов.
    GapFilled { packet_num: u16, bucket_seq_range: (u16, u16) },
    /// Bucket закрыт: получены все trades или исчерпан retry лимит.
    BucketClosed { start: u16, end: u16, all_received: bool, retry_count: u8 },
}
```

## GapBucket logic

- До **50 buckets** одновременно (`MAX_GAP_BUCKETS`).
- Retry до **3 раз** (`MAX_RETRY_COUNT`) с exponential backoff:
  `PathDelay = min(1800, max(300, RTT * (1.2 + retry * 0.7)))` мс.
- `TRADES_PAUSE_TIMEOUT_MS = 30_000` — если 30 сек без пакетов, buckets сбрасываются
  + reset `last_packet_num` (full reset).
- **Auto-batching** до 200 packet_nums в одном resend request.

## TradesResend protocol

При gap клиент сам шлёт `emk_TradesResend` с массивом потерянных `packet_num`.
Сервер отвечает `MPC_TradesResendResponse` (batch — несколько пакетов в одном payload).

Через EventDispatcher: ответы автоматически разбираются и попадают в
`Event::Trade` как `TradesEvent::Apply(...)` (но **НЕ двигают** `last_packet_num` —
это исторические данные).

Низкоуровневый pattern:
```rust
use moonproto::state::parse_trades_resend_response;
use moonproto::commands::trades_stream::parse_trades_packet;

let payloads = parse_trades_resend_response(&resend_response_payload);
for inner in payloads {
    if let Some(pkt) = parse_trades_packet(&inner) {
        let _events = trades.on_packet_resend(pkt);    // НЕ двигает last_packet_num
        // process pkt.sections...
    }
}
```

## Wire format

Пакет сжат **SynLZ** (если `flags & 0x01`). После decompression:

```
BaseTime:   f64 LE (8 bytes) — TDateTime
PacketNum:  u16 LE (2 bytes)
[Sections]:
  MarketIndexAndFlags: u16 LE
    bits 14-15 = section_type (0=Futures, 1=MMOrders, 2=Spot, 3=Extended)
    bits 0-13  = market_index (max 16383)
  match section_type:
    0 | 2 (Trades):
      count:u8
      trades[count]: { time_delta:i16, price:f32, qty:f32 } (10 bytes each)
    1 (MMOrders):
      count:u8
      orders[count]: { time_delta:i16, vol:f32, q:f32 } [+ taker:bytes[20] if HAS_TAKER]
    3 (Extended):
      ext_type:u8
      match ext_type:
        0 (LiqOrders):
          count:u8
          orders[count]: { time_delta:i16, price:f32, qty:f32 } (10 bytes each)
        1 (WatcherFills):
          user:bytes[20]
          count:u8
          fills:bytes[count*20]   // 20 bytes per fill (opaque)

TrailingFlags: u8 (последний байт)
  bit 0 = TRADES_FLAG_COMPRESSED (0x01)
  bit 1 = TRADES_FLAG_HAS_TAKER  (0x02)
```

## `TradesState::tick` — gap recovery

```rust
pub fn tick(&mut self, rtt_ms: i64, now_ms: i64) -> Vec<Vec<u8>>;
pub fn tick_with_events(&mut self, rtt_ms: i64, now_ms: i64) -> (Vec<Vec<u8>>, Vec<TradesEvent>);
```

`tick` возвращает payload'ы для отправки через `client.send_api_request`. Каждый
payload — готовый `emk_TradesResend` с auto-batched до 200 packet_nums.

`tick_with_events` дополнительно возвращает `BucketClosed` / `GapFilled` events
(useful для observability — эти events НЕ доходят до потребителя через `dispatch`).

При `run_with_dispatcher` ручной tick **не нужен** — либа делает его сама.
При custom main loop через `run + dispatch_into` вызывай каждые ~100мс:

```rust
let rtt = client.round_trip_delay_ms();
let now = /* current ms */;
let resends = dispatcher.tick_trades(rtt, now);
for raw in resends {
    client.send_api_request(&raw);
}
```

## Wire-format gotcha — `market_index` 14-bit

В Delphi сервере для MMOrders sub-stream **отсутствовала** маска `& 0x3FFF` при
упаковке `MarketIndexAndFlags` (баг, ARCHITECTURE.md OPEN-QUESTIONS §8 ЗАКРЫТО).
В Rust парсере мы **единообразно** применяем `& 0x3FFF` для всех section_type —
это компенсирует забагованный сервер на `mIndex < 16384` (где маска не имела
видимого эффекта) и корректно работает с исправленным.

## См. также

- [order_books.md](order_books.md) — стаканы (отдельный канал).
- [events.md](events.md) — EventDispatcher автоматизирует tick + gap recovery.
- [engine_api.md](engine_api.md) — `api_subscribe_all_trades`, `api_trades_resend_batches`.
- [multi_server.md](multi_server.md) — independent TradesState на Client.
