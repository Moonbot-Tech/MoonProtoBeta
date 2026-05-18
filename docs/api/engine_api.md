# Engine API (MPC_API)

Request/Response RPC канал между клиентом и Engine на сервере. Через него клиент:
- Получает список маркетов (`get_markets_list`).
- Подписывается на стримы (trades, orderbook).
- Выполняет account-level операции (set_leverage, hedge mode, transfer asset).
- Запрашивает свечи (candles), теги, баланс.

## Архитектура

```
Клиент (TEngineRequest)  ────UID + Method + params────►  Engine на сервере
                                                                │
Клиент (TEngineResponse) ◄────UID + Success/Error + data────────┘
```

Request и Response связаны через **UID** (UInt64). Клиент посылает request с уникальным UID, ловит response с тем же UID.

## Engine методы

31 метод в enum `EngineMethod` (соответствуют Delphi `TEngineMethodKind`):

| Группа | Методы |
|---|---|
| Init / Auth | `BaseCheck`, `AuthCheck` |
| Markets | `GetMarketsList`, `UpdateMarketsList`, `GetMarketsIndexes` |
| Balance | `GetBalance`, `GetMarketsBalanceFull` |
| Orders info | `GetOrder`, `GetOpenOrders`, `GetActiveOrders`, `CancelAllOrders` |
| Account settings | `SetLeverage`, `SetHedgeMode`, `QueryHedgeMode`, `CheckAPIExpirationTime`, `CheckBinanceTags` |
| Trades streaming | `TradesResend`, `SubscribeAllTrades`, `UnsubscribeAllTrades` |
| OrderBook streaming | `SubscribeOrderBook`, `UnsubscribeOrderBook`, `RequestOrderBookFull`, `ReloadOrderBook` |
| Candles | `RequestCandlesData`, `GetCoinCardCandles` |
| Position | `ChangePositionType`, `ConvertDustBNB`, `ConfirmRiskLimit`, `SetMAMode`, `DoTransferAsset`, `UpdateTransferAssets` |

## Два уровня API

### Async-style (рекомендуется — Stage 3)

```rust
let rx = client.api_get_markets_list();              // отправлен, ждём response
let resp = rx.recv_timeout(Duration::from_secs(10))?;
// resp.data — уже распакованный DEFLATE payload
let list = parse_markets_list_response(&resp.data, 2)?;
```

29 high-level wrappers возвращают `mpsc::Receiver<EngineResponse>`:

| Группа | Методы |
|---|---|
| Init / Auth | `api_base_check`, `api_auth_check` |
| Markets | `api_get_markets_list`, `api_update_markets_list`, `api_get_markets_indexes` |
| Balance | `api_get_balance(curr)`, `api_get_markets_balance_full` |
| Orders | `api_get_order(uid)`, `api_get_open_orders`, `api_get_active_orders`, `api_cancel_all_orders` |
| Settings | `api_set_leverage(m, lev)`, `api_set_hedge_mode(b)`, `api_query_hedge_mode`, `api_check_expiration_time`, `api_check_binance_tags` |
| Trades | `api_subscribe_all_trades`, `api_unsubscribe_all_trades`, `api_trades_resend_batches(packets) -> Vec<Receiver>` |
| OrderBook | `api_subscribe_order_book(markets)`, `api_unsubscribe_order_book(markets)`, `api_request_order_book_full(idx, kind)`, `api_reload_order_book` |
| Candles | `api_get_coin_card_candles(market, kind)`, `api_request_candles_data()` (fire-and-forget chunked) |
| Position | `api_change_position_type(m, type, new)`, `api_convert_dust_bnb`, `api_confirm_risk_limit(m)`, `api_set_ma_mode(b)`, `api_do_transfer_asset(asset, q, from, to)`, `api_update_transfer_assets(kind)` |

`send_api_request_async(payload)` — низкоуровневый: возвращает Receiver, регистрирует UID в `api_pending` registry.

### Низкоуровневое (для custom requests)

`commands::engine_request` содержит 20+ pre-built builders:

```rust
use moonproto::commands::engine_request::*;

// Без параметров
let raw = build_base_check();
let raw = build_auth_check();
let raw = build_get_markets_list();
let raw = build_get_markets_indexes();
let raw = build_subscribe_all_trades();
let raw = build_cancel_all_orders();

// С параметрами
let raw = build_set_leverage("BTCUSDT", 10);
let raw = build_set_hedge_mode(true);
let raw = build_get_balance("USDT");
let raw = build_get_order(123456789);
let raw = build_request_order_book_full(market_idx, book_kind);
let raw = build_change_position_type("BTCUSDT", position_type, new_market);
let raw = build_do_transfer_asset("USDT", 100.0, EX_Spot, EX_Futures);

// Batch
let raw = build_trades_resend_batches(&packet_nums); // авто-batch по 200 за раз

client.send(MPC_API, &raw).await?;
```

### Custom params

Для нестандартных параметров — `commands::engine_request::params::write_*`:

```rust
use moonproto::commands::engine_request::{build_custom_request, params};

let mut params_buf = Vec::new();
params::write_str(&mut params_buf, "some_string");
params::write_int(&mut params_buf, 42);
params::write_double(&mut params_buf, 3.14);

let raw = build_custom_request(EngineMethod::SomeMethod, &params_buf);
```

## Парсинг response

```rust
use moonproto::commands::engine_api::{parse_engine_response, EngineMethod};

if let Some(resp) = parse_engine_response(&payload) {
    if !resp.success {
        eprintln!("Engine error {}: {}", resp.error_code, resp.error_msg);
        return;
    }
    match resp.method {
        EngineMethod::GetMarketsList => {
            use moonproto::commands::market::parse_markets_list_response;
            let list = parse_markets_list_response(&resp.data, ver).unwrap();
            // ... apply to MarketsState
        }
        EngineMethod::AuthCheck => {
            // resp.data содержит BinanceAccountID, BTCAddress, etc.
            // Парсить через EngineStreamReader
        }
        _ => {}
    }
}
```

## EngineResponse структура

```rust
pub struct EngineResponse {
    pub request_uid: u64,        // соответствует UID запроса
    pub method: EngineMethod,
    pub success: bool,
    pub error_code: i32,         // 0 если success
    pub error_msg: String,
    pub data: Vec<u8>,           // payload — уже decompressed (если is_compressed=true в wire)
}
```

`data` автоматически распаковано через DEFLATE если в payload был флаг `is_compressed=true`.

## Wire format

### Request
```
TBaseCommand header: CmdId=002 + ver:u16 + UID:u64
Method: u8 (TEngineMethodKind ordinal)
MarketName: UTF-8 string (u16 prefix)
MarketNamesCount: i32
MarketNames[count]: UTF-8 strings
ParamsSize: i32
Params: bytes(ParamsSize)
```

### Response
```
TBaseCommand header: CmdId=001 + ver:u16 + UID:u64
RequestUID: u64
Method: u8
Success: bool (1)
ErrorCode: i32
ErrorMsg: UTF-8 string
IsCompressed: bool (1)
DataSize: i32
Data: bytes(DataSize) — если IsCompressed → DEFLATE raw
```

## Auto-apply через EventDispatcher

Если используется [`EventDispatcher`](events.md), markets-related response'ы автоматически применяются к `MarketsState`:

```rust
let mut dispatcher = EventDispatcher::new();

// При получении EngineResponse с method = GetMarketsList:
// → markets.apply_markets_list() вызывается автоматически
// → эмитятся ДВА события: Event::Markets(MarketsEvent) и Event::EngineResponse(resp)

for ev in dispatcher.dispatch(cmd, payload, now_ms) {
    match ev {
        Event::Markets(_) => {
            // State уже обновлён — читать через dispatcher.markets
            for (name, market) in &dispatcher.markets.by_name {
                println!("{}: leverage={}", name, market.max_leverage);
            }
        }
        Event::EngineResponse(resp) => {
            // Дополнительная обработка raw response
        }
        _ => {}
    }
}
```

Auto-apply: `GetMarketsList`, `UpdateMarketsList`, `GetMarketsIndexes`, `CheckBinanceTags`.
Прочие методы — только `Event::EngineResponse`.

## Pending API registry

`client.api_pending` — thread-safe `Arc<Mutex<HashMap<u64, Sender<EngineResponse>>>>`:

```rust
// register UID → Receiver
let rx = client.api_pending.register(uid);

// dispatch (вызывается автоматически из reader thread)
client.api_pending.dispatch(resp);

// timeout cleanup
client.api_pending.remove(uid);

// общий cleanup (после reconnect)
client.api_pending.clear();
```

При получении `Command::API` пакета — Client автоматически делает `dispatch` → если UID найден, response уходит в `Receiver`, иначе пробрасывается через `on_data`.

## См. также

- [client.md](client.md) — все `api_*` wrappers + `api_pending` + `send_api_request_async`.
- [events.md](events.md) — auto-apply markets state из EngineResponse.
- [candles.md](candles.md) — `emk_GetCoinCardCandles` / `emk_RequestCandlesData` + chunked aggregator.
- [markets.md](markets.md) — парсеры ответов GetMarketsList/UpdateMarketsList/GetMarketsIndexes.
- [trades.md](trades.md) — `subscribe_all_trades`, `trades_resend_batches`.
- [order_books.md](order_books.md) — `subscribe_order_book`, `request_order_book_full`.
- [orders.md](orders.md) — trade канал использует отдельный CmdClass, не Engine API.
