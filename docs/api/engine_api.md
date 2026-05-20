# Engine API (MPC_API)

Request/Response RPC канал между клиентом и Engine на сервере. Через него клиент:
- Получает identity сервера (`BaseCheck`).
- Получает список маркетов (`GetMarketsList`).
- Подписывается на стримы (trades, orderbook).
- Выполняет account-level операции (set_leverage, hedge mode, transfer asset).
- Запрашивает свечи (candles), теги, баланс.

## Архитектура

```
Клиент (TEngineRequest)  ────UID + Method + params────►  Engine на сервере
                                                                │
Клиент (TEngineResponse) ◄────UID + Success/Error + data────────┘
```

Request и Response связаны через **UID** (UInt64). Клиент посылает request с
уникальным UID, ловит response с тем же UID через `mpsc::Receiver`.

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

## Init sequence helper (рекомендуется)

`run_init_sequence` упаковывает типовой init flow в один вызов:
BaseCheck → AuthCheck → GetMarketsList → GetMarketsBalanceFull → подписки.

```rust
use std::time::Duration;
use moonproto::client::{Client, ClientConfig, RefreshConfig, InitConfig, run_init_sequence};
use moonproto::events::EventDispatcher;
use moonproto::state::OrderBookKind;

let mut client = Client::new(cfg);
let mut dispatcher = EventDispatcher::new();

// Phase 1: handshake (~3с до Connected{fresh:true}).
client.run_with_dispatcher(Duration::from_secs(3), &mut dispatcher, Box::new(|_| {}));

// Phase 2: init (chunked main loop pump внутри).
let cfg = InitConfig {
    base_check: true,
    auth_check: true,
    fetch_markets: true,
    fetch_balance: true,
    subscribe_trades: Some(false),                       // false = без MM ордеров
    subscribe_orderbooks: vec![
        ("BTCUSDT".to_string(), OrderBookKind::Futures),
    ],
    step_timeout: Some(Duration::from_secs(5)),
};
let result = run_init_sequence(&mut client, &mut dispatcher, cfg)?;
println!("init: base={} auth={} markets={}B",
         result.base_check_ok, result.auth_check_ok, result.markets_response_bytes);

// Phase 3: long-running stream.
client.run_with_dispatcher(Duration::from_secs(3600), &mut dispatcher, Box::new(|_| {}));
```

При успешном BaseCheck автоматически парсится [`ServerInfo`](#serverinfo--multi-server-identification)
и сохраняется в `client.server_info()` — для multi-server терминалов.

См. [client.md → init sequence](client.md#init-sequence).

## Два уровня API (для custom flow)

### Async-style (рекомендуется)

29 high-level wrappers возвращают `mpsc::Receiver<EngineResponse>`:

```rust
let rx = client.api_get_markets_list();
let resp = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(10))?;
// resp.data — уже распакованный DEFLATE payload.
let list = parse_markets_list_response(&resp.data, 2)?;
```

| Группа | Методы |
|---|---|
| Init / Auth | `api_base_check`, `api_auth_check` |
| Markets | `api_get_markets_list`, `api_update_markets_list`, `api_get_markets_indexes` |
| Balance | `api_get_balance(curr)`, `api_get_markets_balance_full` |
| Orders | `api_get_order(uid)`, `api_get_open_orders`, `api_get_active_orders`, `api_cancel_all_orders` |
| Settings | `api_set_leverage(m, lev)`, `api_set_hedge_mode(b)`, `api_query_hedge_mode`, `api_check_expiration_time`, `api_check_binance_tags` |
| Trades | `api_subscribe_all_trades(want_mm)`, `api_unsubscribe_all_trades`, `api_trades_resend_batches(packets) -> Vec<Receiver>` |
| OrderBook | `api_subscribe_order_book(markets)`, `api_unsubscribe_order_book(markets)`, `api_request_order_book_full(idx, kind)`, `api_reload_order_book` |
| Candles | `api_get_coin_card_candles(market, kind)`, `api_request_candles_data()` (fire-and-forget chunked), `api_request_candles_data_async() -> Receiver<MergedCandles>` |
| Position | `api_change_position_type(m, type, new)`, `api_convert_dust_bnb`, `api_confirm_risk_limit(m)`, `api_set_ma_mode(b)`, `api_do_transfer_asset(asset, q, from, to)`, `api_update_transfer_assets(kind)` |

`send_api_request_async(payload) -> Receiver<EngineResponse>` — низкоуровневый
вариант: возвращает Receiver, регистрирует UID в `api_pending` registry.

### Низкоуровневое (для custom requests)

`commands::engine_request` содержит 20+ pre-built builders:

```rust
use moonproto::commands::engine_request::*;

// Без параметров
let raw = base_check();
let raw = auth_check();
let raw = get_markets_list();
let raw = get_markets_indexes();
let raw = subscribe_all_trades(true);
let raw = cancel_all_orders();

// С параметрами
let raw = set_leverage("BTCUSDT", 10);
let raw = set_hedge_mode(true);
let raw = get_balance("USDT");
let raw = get_order(123456789);
let raw = request_order_book_full(market_idx, book_kind);
let raw = change_position_type("BTCUSDT", position_type, new_market);
let raw = do_transfer_asset("USDT", 100.0, /* from = */ 1, /* to = */ 2);

// Batch
let raws = trades_resend_batches(&packet_nums);    // auto-batched до 200 за раз

client.send_api_request(&raw);
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
            let list = parse_markets_list_response(&resp.data, /* ver = */ 2).unwrap();
            // ... apply to MarketsState
        }
        EngineMethod::AuthCheck => {
            // resp.data содержит BinanceAccountID, BTCAddress, etc.
            // Парсить через EngineStreamReader / parse_auth_check_response.
        }
        _ => {}
    }
}
```

## ServerInfo — multi-server identification

`emk_BaseCheck` — первый Engine-вызов в init sequence. Кроме обычной проверки
success/fail, его response несёт **identity сервера** для приложений
подключающихся к нескольким MoonBot-серверам одновременно (разные биржи, разные
аккаунты).

```rust
use moonproto::commands::engine_api::{parse_base_check_response, exchange_type_flags};
use std::time::Duration;

let rx = client.api_base_check();
let resp = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(10))?;
if resp.success {
    let info = parse_base_check_response(&resp.data);
    if let (Some(id), Some(name)) = (info.bot_id, &info.exchange_name) {
        println!("Bot #{} = {} ({})", id, name,
            info.base_currency_name.as_deref().unwrap_or("?"));
    }
    if info.supports(exchange_type_flags::FUTURES) {
        // показать UI для futures-only функций
    }
}
```

**Auto-fill в run_init_sequence.** Если используешь `run_init_sequence` —
парсинг и сохранение в `client.server_info` делается автоматически. После init
читай через getter:

```rust
let info = client.server_info();
println!("Connected to bot {} v{}",
    info.bot_id.unwrap_or(0),
    info.server_version.unwrap_or(0));
```

### ServerInfo поля

| Поле | Тип | Семантика |
|---|---|---|
| `bot_id` | `Option<i64>` | `cfg.UniqueBotID` — стабильный 64-bit ID. Основной ключ для multi-server идентификации. |
| `server_name` | `Option<String>` | `cfg.BotName` для UI (`"Binance Main"`); если пусто — `"Server"`. |
| `exchange_code` | `Option<u8>` | `Ord(cfg.Header.Current)` — Delphi enum `TBotPlatform`. |
| `exchange_name` | `Option<String>` | Имя биржи (`"Binance Futures"`, `"Hyper"`). |
| `exchange_type_mask` | `Option<u8>` | Bitmask — см. [`exchange_type_flags`](#exchange_type_flags). |
| `dex_name` | `Option<String>` | HIP-3 dex name для Hyperliquid futures. |
| `base_currency_name` | `Option<String>` | `cfg.Currency` (`"USDT"`, `"BTC"`, `"USDC"`). |
| `base_currency_code` | `Option<u8>` | `Ord(cfg.BaseCurrency)` (BC_USDT=1, ...). |
| `server_version` | `Option<u32>` | `Current_Version_Num_X` (например `763` для v7.63). |
| `moonproto_version` | `Option<u32>` | `IntMoonProtoTCPCurrentVer`. |

`info.has_identity()` = `bot_id.is_some()` — quick check что сервер расширенный.

### exchange_type_flags

Bitmask константы для `ServerInfo::exchange_type_mask`. Несколько бит могут быть
установлены одновременно.

```rust
use moonproto::commands::engine_api::exchange_type_flags;

const SPOT: u8    = 0x01;
const FUTURES: u8 = 0x02;
const DEX: u8     = 0x04;
const PREDICT: u8 = 0x08;    // HL outcome markets

if info.supports(exchange_type_flags::DEX) { /* ... */ }
```

### Forward-compatibility

Все поля — `Option`. Старый сервер до multi-server расширения отвечает на
BaseCheck пустым payload — все поля `None`, `info.has_identity()` = `false`.
Парсер толерантен к **truncate в любом месте**: если payload обрывается,
заполненные поля сохраняются, остальные = `None`.

### Wire-format BaseCheck response (10 полей)

В порядке записи на сервере (`MoonProtoEngineServer.pas:244-273`):

```
1.  bot_id              i64 LE (8 bytes)        cfg.UniqueBotID
2.  server_name         u16 length + UTF-8      cfg.BotName (default "Server")
3.  exchange_code       u8                      Ord(cfg.Header.Current)
4.  exchange_name       u16 length + UTF-8      "Binance Futures", "Hyper", ...
5.  exchange_type_mask  u8                      bit0=Spot, bit1=Futures, bit2=DEX, bit3=Predict
6.  dex_name            u16 length + UTF-8      HIP-3 dex name (или "")
7.  base_currency_name  u16 length + UTF-8      "USDT", "BTC", ...
8.  base_currency_code  u8                      Ord(cfg.BaseCurrency) — BC_USDT=1
9.  server_version      i32 LE (4 bytes)        Current_Version_Num_X
10. moonproto_version   i32 LE (4 bytes)        IntMoonProtoTCPCurrentVer
```

`success=false` от сервера → payload пустой (поля не пишутся).

См. [multi_server.md](multi_server.md) для полного pattern'а multi-Client терминала.

## EngineResponse структура

```rust
pub struct EngineResponse {
    pub request_uid: u64,        // соответствует UID запроса
    pub method:      EngineMethod,
    pub success:     bool,
    pub error_code:  i32,        // 0 если success
    pub error_msg:   String,
    pub data:        Vec<u8>,    // payload — уже decompressed (если is_compressed=true в wire)
}
```

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

**Парсер skip'ает** 11-байтный TBaseCommand header `Engine` subprotocol'а перед
чтением `request_uid` (offset 11, не 0) — критично для корректной маршрутизации
ответов в pending API registry.

## Auto-apply через EventDispatcher

Markets-related response'ы автоматически применяются к `MarketsState`:

```rust
let mut dispatcher = EventDispatcher::new();

// При получении EngineResponse с method = GetMarketsList:
// → markets.apply_markets_list() вызывается автоматически
// → эмитятся ДВА события: Event::Markets(MarketsEvent) и Event::EngineResponse(resp)

client.run_with_dispatcher(duration, &mut dispatcher, Box::new(|ev| match ev {
    Event::Markets(_) => {
        // State уже обновлён — читать через dispatcher.markets()
        for (name, market) in &dispatcher.markets().by_name {
            println!("{}: leverage={}", name, dispatcher.markets().markets[*market].max_leverage);
        }
    }
    Event::EngineResponse(resp) => { /* доп. обработка raw response */ }
    _ => {}
}));
```

Auto-applied методы: `GetMarketsList`, `UpdateMarketsList`, `GetMarketsIndexes`,
`CheckBinanceTags`. Прочие методы — только `Event::EngineResponse`.

## Pending API registry

`client.api_pending` — `Arc<ApiPending>` (thread-safe `Mutex<HashMap>`):

```rust
use std::time::Duration;
use moonproto::commands::engine_request;

let raw = engine_request::get_markets_list();
let uid = u64::from_le_bytes(raw[3..11].try_into().unwrap());

// register UID → Receiver. Второй параметр — текущее время в ms (для auto-cleanup).
let now_ms = /* current ms из main loop */;
let rx = client.api_pending.register(uid, now_ms);

client.send_api_request(&raw);

match client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(10)) {
    Ok(resp) => process(resp),
    Err(_)   => { client.api_pending.remove(uid); }
}
```

`Client::send_api_request_async(raw) -> Receiver<EngineResponse>` — удобная
оболочка над `register` + `send_api_request` (использует `client.now_ms()` для timestamp).

**Auto-cleanup** устаревших pending slots делает либа сама из main loop —
default age = `DEFAULT_PENDING_TIMEOUT_MS` (12 сек, parity с Delphi engine).

## См. также

- [client.md](client.md) — все `api_*` wrappers + `api_pending` + `send_api_request_async`.
- [events.md](events.md) — auto-apply Markets state из EngineResponse.
- [candles.md](candles.md) — `emk_GetCoinCardCandles` / `emk_RequestCandlesData` + chunked aggregator.
- [markets.md](markets.md) — парсеры ответов GetMarketsList/UpdateMarketsList/GetMarketsIndexes.
- [trades.md](trades.md) — `api_subscribe_all_trades`, `api_trades_resend_batches`.
- [order_books.md](order_books.md) — `api_subscribe_order_book`, `api_request_order_book_full`.
- [multi_server.md](multi_server.md) — ServerInfo + multi-Client routing.
