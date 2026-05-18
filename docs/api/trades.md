# TradesStream channel (MPC_TradesStream)

Real-time поток сделок с биржи + market-maker заявки + ликвидации + watcher fills.

## Что это

Сервер агрегирует трейды с биржи и шлёт пакетами по `packet_num` (монотонно растущий u32). Каждый пакет содержит несколько секций:

- **Trades** (sections 0, 2): обычные сделки. `0` = Futures, `2` = Spot.
- **MMOrders** (section 1): market-maker заявки (выставлены и отозваны).
- **Extended** (section 3): Liquidation orders + Watcher fills (если subscribed).

`MoonProto` гарантирует доставку через GapBucket logic — если пакет потерян, клиент автоматически запрашивает resend.

## Подписка

```rust
use moonproto::commands::engine_request::build_subscribe_all_trades;

let raw = build_subscribe_all_trades();
client.send(MPC_API, &raw).await?;
```

После подписки сервер начинает слать `MPC_TradesStream` пакеты.

## Парсинг и применение

```rust
use moonproto::commands::trades_stream::parse_trades_packet;
use moonproto::state::TradesState;

let mut trades = TradesState::new();
let rtt_ms = 280; // последний измеренный RTT (см. Ping)

if let Some(pkt) = parse_trades_packet(&payload) {
    let now_ms = current_ms();
    let event = trades.on_packet(pkt.clone(), now_ms);
    match event {
        TradesEvent::Sequential => { /* нормально */ }
        TradesEvent::Duplicate => { /* пропустить */ }
        TradesEvent::Gap { from, to } => {
            // Потеря — bucket уже создан, retry автоматически в tick()
        }
        TradesEvent::FilledGap => { /* gap закрыт */ }
        TradesEvent::Pause => {
            // Долгая тишина — buckets сброшены, нужен новый snapshot
        }
        TradesEvent::Reset => { /* clean restart */ }
    }
    // Обработать секции pkt
    for section in pkt.sections {
        for t in section.trades {
            on_trade(t);
        }
    }
}

// Периодически (например каждые 100мс) вызывать tick:
for resend_payload in trades.tick(rtt_ms, now_ms) {
    client.send(MPC_API, &resend_payload).await?;
}
```

## GapBucket logic

- До 50 buckets одновременно.
- Retry до 3 раз с exponential backoff: `PathDelay = min(1800, max(300, RTT * (1.2 + retry * 0.7)))` мс.
- `TRADES_PAUSE_TIMEOUT_MS = 30000` — если 30 сек без пакетов, buckets сбрасываются.
- Auto-batching до 200 packet_nums в одном resend request.

## TradesResend protocol

При gap клиент шлёт `emk_TradesResend` с массивом потерянных `packet_num`. Сервер отвечает `MPC_TradesResendResponse` (batch — несколько пакетов в одном payload).

```rust
use moonproto::state::parse_trades_resend_response;

if let Some(payloads) = parse_trades_resend_response(&resend_payload) {
    for p in payloads {
        if let Some(pkt) = parse_trades_packet(&p) {
            trades.on_packet_resend(pkt, now_ms);
        }
    }
}
```

`on_packet_resend` НЕ двигает `last_packet_num` — это отдельный путь для исторических данных.

## Структуры

```rust
pub struct TradesPacket {
    pub packet_num: u32,
    pub sections: Vec<TradeSection>,
}

pub struct TradeSection {
    pub kind: u8,             // 0/2=Trades, 1=MMOrders, 3=Extended
    pub market_idx: u16,
    pub has_taker: bool,      // флаг наличия Taker u8 в MMOrders
    pub trades: Vec<Trade>,
}

pub struct Trade {
    pub time: f64,            // TDateTime
    pub price: f64,
    pub quantity: f64,
    pub is_sell: bool,
    // ... + Liq/Watcher специфичные поля если Extended
}
```

## Wire format

Пакет сжат **SynLZ**. После decompression:

```
PacketNum: u32 (last 4 bytes — flags byte)
[Sections...]:
  Kind: u8
  MarketIdx: u16
  TradeCount: u16
  Trades[TradeCount]: zigzag-packed delta encoding (см. trades_stream.rs)
TrailingFlags: u8  // bit 0=Compressed, bit 1=HasTaker
```

## См. также

- [order_books.md](order_books.md) — стаканы (отдельный канал)
- [engine_api.md](engine_api.md) — `subscribe_all_trades`, `trades_resend_batches`
