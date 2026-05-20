# MPC_API — Engine RPC

`MPC_API` (channel byte 31) — RPC-канал для запросов к торговому engine'у сервера
(подключение к бирже, баланс, ордера, candles). 31 метод определён в перечислении
`commands::engine_api::EngineMethod`.

## Wire format

### Request (C → S)

```
[CmdId=2]        — 1 byte  — request marker
[ver=3]          — 2 bytes LE — protocol version
[UID]            — 8 bytes LE — unique request id (для match'а ответа)
[Method]         — 1 byte  — EngineMethod variant (см. `engine_api.rs`)
[params...]      — variable, per-method
```

UID — обязательно уникальный per-request. Клиент сохраняет `Receiver<EngineResponse>`
в pending registry под этим UID. Ответ от сервера приходит с тем же UID, диспетчер
доставляет в зарегистрированный `Receiver`.

### Response (S → C)

```
[CmdId=1]        — 1 byte  — response marker
[ver=3]          — 2 bytes LE
[UID]            — 8 bytes LE — echo от request'а
[Method]         — 1 byte  — EngineMethod (для каких метод этот ответ)
[Success]        — 1 byte  — boolean (0 = error, 1 = success)
[ErrorTextLen]   — 2 bytes LE
[ErrorText]      — UTF-8, ErrorTextLen байт — диагностика при Success=0
[Data]           — variable, формат зависит от Method
```

Формат `Data` различается per-метод. См. doc comments на каждом `EngineMethod`
variant'е (`cargo doc --open` → `EngineMethod`). Парсеры специфичных форматов:

| Method | Parser |
|--------|--------|
| `BaseCheck` | [`commands::engine_api::parse_base_check_response`] → `ServerInfo` |
| `AuthCheck` | [`commands::engine_api::parse_auth_check_response`] → `AuthCheckResponse` |
| `GetMarketsList` / `UpdateMarketsList` | [`commands::markets::parse_markets_list_response`] |
| `GetCoinCardCandles` | [`commands::candles::parse_coin_card_candles_response`] |
| `RequestCandlesData` | [`commands::candles::CandlesAggregator::on_chunk`] (см. ниже) |
| `GetMarketsIndexes` | inline в `EventDispatcher::dispatch` |

### BaseCheck response — multi-server identity

При `Success=1` сервер пишет 10 опциональных полей в `Data` (в этом порядке):

```
[bot_id]              i64 LE (8 bytes)         cfg.UniqueBotID
[server_name]         u16 length + UTF-8       cfg.BotName (default "Server")
[exchange_code]       u8                       Ord(cfg.Header.Current) (TBotPlatform)
[exchange_name]       u16 length + UTF-8       "Binance Futures", "Hyper", ...
[exchange_type_mask]  u8                       bit0=Spot, bit1=Futures, bit2=DEX, bit3=Predict
[dex_name]            u16 length + UTF-8       HIP-3 dex name (HL futures) или ""
[base_currency_name]  u16 length + UTF-8       "USDT", "BTC", "USDC", ...
[base_currency_code]  u8                       Ord(cfg.BaseCurrency) (BC_USDT=1)
[server_version]      i32 LE (4 bytes)         Current_Version_Num_X
[moonproto_version]   i32 LE (4 bytes)         IntMoonProtoTCPCurrentVer
```

Forward-compat: парсер ([`parse_base_check_response`]) толерантен к truncate в любом месте — поля до точки обрыва заполнены, остальные = `None`. Старый сервер без расширения пишет пустой payload — `ServerInfo::default()`. См. [`docs/api/engine_api.md`](../api/engine_api.md#serverinfo--multi-server-identification) для семантики полей.

`Success=0` → `Data` пустой (как раньше).

## Chunked responses

`RequestCandlesData` возвращает несколько `EngineResponse` пакетов с одним UID
(разбиение из-за размера). Pending registry **не подходит** — он удаляет sender
после первого ответа.

Используй обычный `on_data` callback + `CandlesAggregator::on_chunk(&resp.data)` —
вернёт `Some(merged)` когда все чанки получены.

## Client wrappers

Все методы имеют high-level Client-обёртку с автоматическим UID:

```rust
let rx = client.api_get_markets_list();        // ничего не передавать
let rx = client.api_get_order(order_uid);       // с параметрами
let rx = client.api_set_leverage("BTCUSDT", 10);

let response: EngineResponse = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(5))?;
if response.success {
    let markets = parse_markets_list_response(&response.data, version)?;
    // ...
} else {
    eprintln!("server error: {}", response.error_text);
}
```

Полный список — `client.api_*` методы (`cargo doc --open`).

## Versioning

`ver` поле — текущая версия 3. При получении `ver > 3` команда **пропускается**
(forward compatibility). При `ver < 3` — backward compatibility поведение зависит
от метода (большинство просто работают).

## Error codes

`Success=0` означает ошибку на стороне сервера/биржи. `ErrorText` — UTF-8 строка
для логирования / отображения в UI. Конкретные коды/типы ошибок не выделены — это
ad-hoc текстовые сообщения биржи через engine.

Если связь / парсинг ошибся (тот же UID не получен в течение reasonable timeout) —
это **другая ошибка** уровня транспорта, не Success=0. Обнаруживается через
`rx.recv_timeout()` → `Err`.
