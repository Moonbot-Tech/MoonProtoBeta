# Candles channel

Запрос исторических свечей через Engine API. Два метода: `emk_GetCoinCardCandles` (single response) и `emk_RequestCandlesData` (chunked).

## TDeepPrice

Packed record, **28 bytes**:

```rust
pub struct DeepPrice {
    pub open_p:  f32,   // 4 bytes
    pub close_p: f32,   // 4 bytes
    pub max_p:   f32,   // 4 bytes
    pub min_p:   f32,   // 4 bytes
    pub vol:     f32,   // 4 bytes
    pub time:    f64,   // 8 bytes — TDateTime (Delphi double, дни с 1899-12-30)
}
```

Соответствует Delphi `MarketsU.pas:701-705 TDeepPrice = packed record`.

## DeepHistoryKind

```rust
pub enum DeepHistoryKind {
    Min1   = 0,   // 1-минутные свечи
    Min5   = 1,
    Min30  = 2,
    Hour1  = 3,
    Hour4  = 4,
    Day1   = 5,   // дневные
}
```

Соответствует `EngineBase.pas:60 TMarketDeepHistoryKind = (hk_1m, hk_5m, hk_30m, hk_1h, hk_4h, hk_1d)`.

**ВНИМАНИЕ**: Hour4 был добавлен в Delphi — порядок enum'а **критичен** для wire-формата. Передаётся как 1 byte ordinal.

## emk_GetCoinCardCandles — single response

```rust
// Request: market name + 1 byte ticks kind.
let rx = client.api_get_coin_card_candles("BTCUSDT", DeepHistoryKind::Hour1);

match rx.recv_timeout(Duration::from_secs(10)) {
    Ok(resp) => {
        if resp.success {
            let candles = parse_coin_card_candles_response(&resp.data).unwrap();
            for c in &candles {
                println!("{} open={} close={} vol={}", c.time, c.open_p, c.close_p, c.vol);
            }
        }
    }
    Err(_) => eprintln!("timeout"),
}
```

Wire response (after DEFLATE decompression):
```
count: i32 LE
candles: N × TDeepPrice (28 bytes each)
```

## emk_RequestCandlesData — chunked response

Этот метод используется когда candles-данные слишком большие для одного UDP-пакета. Сервер режет на chunks и шлёт через множественные `EngineResponse`.

```rust
use moonproto::commands::candles::{CandlesAggregator, parse_coin_card_candles_response};

let mut agg = CandlesAggregator::new();

// Отправка request — fire-and-forget, ответы прилетают как несколько EngineResponse:
client.api_request_candles_data();

// В on_data callback'е (или через EventDispatcher::Event::EngineResponse):
//
//   if resp.method == EngineMethod::RequestCandlesData {
//       if let Some(merged) = agg.on_chunk(&resp.data) {
//           // Все чанки получены — merged готов
//           let candles = parse_coin_card_candles_response(&merged).unwrap();
//           // process candles ...
//       }
//   }
```

### Wire-формат chunk

Каждый `EngineResponse.data` (для метода RequestCandlesData):
```
ChunkIndex: u16 LE
ChunkTotal: u16 LE
chunk_payload: bytes (rest)
```

После сборки всех `ChunkTotal` чанков (по `ChunkIndex` 0..ChunkTotal-1), `chunk_payload`'ы конкатенируются в final stream, который имеет формат как у `GetCoinCardCandles` (count + candles).

### CandlesAggregator

```rust
impl CandlesAggregator {
    pub fn new() -> Self;
    pub fn on_chunk(&mut self, response_data: &[u8]) -> Option<Vec<u8>>;
    pub fn reset(&mut self);
    pub fn progress(&self) -> (usize, usize);  // (received, total)
}
```

**Поведение:**
- Out-of-order delivery: chunks принимаются в любом порядке, складываются в `chunks[idx]`.
- Duplicate: повторный chunk с тем же `idx` игнорируется (через `Option::is_none()` check).
- Resize: при первом chunk инициализируется `chunks: Vec<Option<Vec<u8>>>` размером `ChunkTotal`.
- Auto-reset: после `on_chunk` возвращает `Some(merged)` — внутреннее состояние сбрасывается, готов к новому запросу.

### Требования к caller'у

1. **DEFLATE decompression уже выполнен**. `response_data` — это `EngineResponse.data` _после_ распаковки (если `is_compressed=true`, `parse_engine_response` распакует автоматически).
2. **Фильтрация по `request_uid`**: если запущено несколько параллельных `RequestCandlesData`, нужно либо вести отдельный `CandlesAggregator` для каждого `request_uid`, либо вызывать `reset()` при смене запроса. В Delphi эта фильтрация делается через `resp.RequestUID == CandlesRequestUID` (MoonProtoClient.pas:814). Aggregator сам по себе **не проверяет** UID.

## Время свечей

`DeepPrice.time` — это `TDateTime` (Delphi double, дни с эпохи 1899-12-30 UTC). Для конвертации в unix timestamp:

```rust
fn delphi_to_unix_secs(td: f64) -> f64 {
    (td - 25569.0) * 86400.0  // 25569 = days между 1899-12-30 и 1970-01-01
}
```

## См. также

- [engine_api.md](engine_api.md) — RPC канал.
- [events.md](events.md) — `Event::EngineResponse` для отслеживания response'ов.
- [client.md](client.md) — `client.api_get_coin_card_candles()` / `api_request_candles_data()`.
