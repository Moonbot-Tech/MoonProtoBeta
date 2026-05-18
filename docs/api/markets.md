# Markets channel (Engine API responses)

Парсеры серверных ответов на запросы списка маркетов и обновления цен через Engine API.

## Что это

Сервер хранит список торговых маркетов (BTC-USDT, ETH-USDT, etc.) с их торговыми лимитами, точностями и текущими ценами. Клиент запрашивает этот список через `emk_GetMarketsList`, периодически обновляет цены через `emk_UpdateMarketsList`, и получает метаданные (теги монет, индексы) через дополнительные методы.

В либе реализовано:
1. **Wire-парсеры** в `commands::market` — конвертация сырого `EngineResponse.data` в типизированные структуры.
2. **Sync state** в `state::MarketsState` — snapshot маркетов с авто-применением обновлений.

---

## Engine методы Markets канала

| Метод | Возвращает |
|---|---|
| `emk_GetMarketsList` | Полный список Markets + CorrMarkets |
| `emk_UpdateMarketsList` | Обновление цен (Bid/Ask/Funding/MarkPrice) для всех маркетов |
| `emk_GetMarketsIndexes` | Список имён маркетов в порядке `mIndex` |
| `emk_CheckBinanceTags` | Теги (Monitoring/Fan/Seed/Launch/Gaming/New/OLD/BNB/Alpha/OICapped/TradFi) для каждой монеты |

---

## Парсинг ответов

```rust
use moonproto::commands::engine_api::{parse_engine_response, EngineMethod};
use moonproto::commands::market::*;

// На входе — payload канала MPC_API после расшифровки.
let resp = parse_engine_response(&payload).unwrap();

match resp.method {
    EngineMethod::GetMarketsList => {
        let list = parse_markets_list_response(&resp.data, resp_ver).unwrap();
        for m in &list.markets {
            println!("{}: tick={}, leverage={}", m.bn_market_name, m.bn_tick_size, m.max_leverage);
        }
    }
    EngineMethod::UpdateMarketsList => {
        let prices = parse_markets_prices_response(&resp.data).unwrap();
        for p in &prices.prices {
            println!("mIndex={}: bid={} ask={}", p.m_index, p.bid, p.ask);
        }
    }
    EngineMethod::GetMarketsIndexes => {
        let names = parse_markets_indexes_response(&resp.data).unwrap();
        for (i, name) in names.iter().enumerate() {
            println!("mIndex {} -> {}", i, name);
        }
    }
    EngineMethod::CheckBinanceTags => {
        let tags = parse_token_tags_response(&resp.data).unwrap();
        for t in &tags {
            if t.tags.contains(TokenTags::ALPHA) {
                println!("{} is ALPHA", t.market_name);
            }
        }
    }
    _ => {}
}
```

`resp_ver` — это `EngineResponse.ver` (или `1` если неизвестна). Версия v2 добавила поле `FuturesType` (1 байт) в конце каждого `Market`.

---

## Sync state

```rust
use moonproto::state::MarketsState;

let mut markets = MarketsState::new();

// После emk_GetMarketsList:
markets.apply_markets_list(list);

// После emk_UpdateMarketsList:
markets.apply_markets_prices(prices);

// Lookup:
if let Some(m) = markets.get("BTCUSDT") {
    println!("tick={}", m.bn_tick_size);
}
if let Some(price) = markets.price("BTCUSDT") {
    println!("bid={} ask={}", price.bid, price.ask);
}
```

### Семантика apply
- `apply_markets_list` — **полная замена** `markets`, `by_name`, `corr_markets`, и инициализация `prices` (Bid/Ask=0, funding из самого Market).
- `apply_markets_prices` — **обновление по `mIndex`**. Если `send_funding=false`, поля `funding_rate/funding_time_utc` не меняются. Если `send_corr_markets=true`, `corr_prices` полностью заменяется новым набором.
- `apply_token_tags` — **полная замена** `token_tags`. Сервер шлёт только маркеты с не-пустыми тегами; отсутствующие сбрасываются.
- `apply_markets_indexes` — **полная замена** `market_indexes`.

---

## Структура Market

`Market` содержит 42 поля (byte-exact с Delphi `TMarket` через `WriteMarketToStream`):

| Группа | Поля |
|---|---|
| Имена (10 strings) | `bn_market_name`, `market_currency`, `bn_market_currency`, `base_currency`, `market_currency_long`, `market_currency_canonic`, `market_name`, `market_name_mb_classic`, `bn_status`, `leading1000` |
| Точности и лимиты (6 i32) | `bn_price_precision`, `bn_quantity_precision`, `max_leverage`, `k1000`, `bn_iceberg_parts`, `bn_margin_table_id` |
| Контракт (1 i64) | `bn_delivery_time` |
| Floats (20 f64) | `bn_tick_size`, `bn_step_size`, `bn_min_qty`, `bn_max_qty`, `bn_min_notional`, `bn_max_notional`, `bn_contract_size`, `bn_min_price`, `bn_max_price`, `bn_max_value`, `bn_multiplier_up`, `bn_multiplier_down`, `bid_multiplier_up`, `bid_multiplier_down`, `ask_multiplier_up`, `ask_multiplier_down`, `int_bn_max_qty`, `funding_rate`, `funding_time`, `volume` |
| Флаги (5 bool) | `is_btc_market`, `status_trading`, `bn_is_fucking_shib`, `bn_iceberg`, `bn_only_isolated` |
| Версия v2 (1 byte) | `futures_type: BaseCurrency` |

Подробное описание полей — в Delphi `MarketsU.pas`.

---

## TokenTags

`TokenTags` — это bitset из 12 возможных тегов:

```rust
pub struct TokenTags(pub u32);

impl TokenTags {
    pub const NONE:       Self = Self(1 << 0);
    pub const MONITORING: Self = Self(1 << 1);
    pub const FAN:        Self = Self(1 << 2);
    pub const SEED:       Self = Self(1 << 3);
    pub const LAUNCH:     Self = Self(1 << 4);
    pub const GAMING:     Self = Self(1 << 5);
    pub const NEW:        Self = Self(1 << 6);
    pub const OLD:        Self = Self(1 << 7);
    pub const BNB:        Self = Self(1 << 8);
    pub const ALPHA:      Self = Self(1 << 9);
    pub const OI_CAPPED:  Self = Self(1 << 10);
    pub const TRAD_FI:    Self = Self(1 << 11);
}
```

Поддерживает `|`, `&`, `.contains(other)`, `.is_empty()`.

---

## Wire format reference

### Market record (byte-exact с `WriteMarketToStream`)
```
WriteStr × 10 (UTF-8, u16 LE prefix каждая)
WriteInt × 6 (i32 LE)
WriteInt64 (i64 LE)
WriteDouble × 20 (f64 LE)
WriteBool × 5 (1 byte)
if ver >= 2:
  WriteByte (u8 — FuturesType ordinal)
```

### CorrMarket
```
WriteStr (bn_market_name)
WriteStr (bn_market_currency)
WriteDouble (bn_tick_size)
WriteStr (base_currency_name, "" если nil)
```

### Markets prices update
```
WriteBool (send_funding)
WriteInt (count)
for each:
  WriteWord (m_index)
  WriteDouble (bid)
  WriteDouble (ask)
  if send_funding:
    WriteDouble (funding_rate)
    WriteDouble (funding_time_utc — без TZShift, чистый UTC)
  WriteDouble (mark_price)
  WriteBool (mark_price_found)
WriteBool (send_corr_markets)
if send_corr_markets:
  WriteInt (corr_count)
  for each:
    WriteStr (bn_market_name)
    WriteDouble (last_price)
```

### Markets indexes
```
WriteInt (count)
WriteStr × count
```

### Token tags
```
WriteInt (count)
for each:
  WriteStr (market_name)
  WriteInt (tags as i32 bitmask)
```

---

## См. также

- `commands::engine_api` — общая обёртка `EngineResponse` (compression + headers).
- `commands::engine_request` — `build_get_markets_list/update_markets_list/...` builders для отправки.
- `commands::balance` — отдельный канал для балансов аккаунта.
