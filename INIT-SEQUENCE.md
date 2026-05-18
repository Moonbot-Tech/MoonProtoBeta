# MoonProto Client Initialization Sequence (DRAFT)

> ЧЕРНОВИК. Потом подумать над структурой документов.

## Порядок инициализации (из TCryptoPumpTool.InitInt + TMoonProtoEngine)

MoonBot Delphi-клиент после подключения к серверу выполняет последовательность
API-запросов через MoonProto Engine RPC (`SendAndWait` = send + poll for response):

### Фаза 0: Transport Handshake (автоматически)

```
Client → Server: MPC_Hello (encrypted with MasterKey, AAD=ClientID)
Server → Client: MPC_WhoAreYou (encrypted with MasterKey, AAD=ClientID)
Client → Server: MPC_ImFriend (encrypted with SessionKey, AAD=ClientID)
Server → Client: MPC_Fine (encrypted with MasterKey, AAD=ClientID)
```

После Fine: `AuthStatus = AuthDone`, session keys active, Ping exchange starts.

### Фаза 1: BaseCheck

```
Client → Server: TEngineRequest(emk_BaseCheck)  [MPC_API, Sliced, Encrypted]
Server → Client: TEngineResponse(success=true)   [MPC_API, Sliced, Encrypted]
```

Проверяет что сервер жив и готов обрабатывать запросы. При ошибке — retry до 10 раз.

### Фаза 2: AuthCheck

```
Client → Server: TEngineRequest(emk_AuthCheck)  [MPC_API, Sliced, Encrypted]
Server → Client: TEngineResponse(success=true, data=[account info])
```

Response data:
- BinanceAccountID: i64
- BTCAddress: string
- SpotRef: i32
- IsSubAccount: bool
- AccountID: string
- RecvdMaxPayload: i32 (optional)
- KnownDexes: array (optional)
- HLDexMarket: byte (optional)
- HLSpotMarket: byte (optional)

### Фаза 3: GetMarketsList + UpdateMarketsList

```
Client → Server: TEngineRequest(emk_GetMarketsList)  [MPC_API, Sliced, Encrypted]
Server → Client: TEngineResponse(success=true, data=[markets], compressed=deflate)

Client → Server: TEngineRequest(emk_UpdateMarketsList) [MPC_API, Sliced, Encrypted]  
Server → Client: TEngineResponse(success=true, data=[updates], compressed=deflate)
```

**ВАЖНО**: Эти responses сжаты deflate (`IsCompressed=true`, raw deflate windowBits=-15).
Response data содержит полный список маркетов с параметрами (ReadMarketFromStream).
При получении UpdateMarketsList сервер также ставит `BalancesSubscribed=true` и шлёт первый Full balance snapshot.

### Фаза 4: GetMarketsBalanceFull (или GetBalance)

```
Client → Server: TEngineRequest(emk_GetMarketsBalanceFull) [MPC_API, Sliced, Encrypted]
Server → Client: TEngineResponse(success=true, data=[balances])
```

### Фаза 5: SubscribeAllTrades

```
Client → Server: TEngineRequest(emk_SubscribeAllTrades, params=[bool MMOrders])
Server → Client: TEngineResponse(success=true)
```

После этого сервер начинает стримить MPC_TradesStream каждые 5ms.

### Фаза 6: (опционально) SubscribeOrderBook

```
Client → Server: TEngineRequest(emk_SubscribeOrderBook, market_names=["BTCUSDT", ...])
Server → Client: TEngineResponse(success=true)
```

---

## SendAndWait механизм

Delphi-клиент:
1. Создаёт TPendingRequest(UID, name)
2. Добавляет в PendingRequests список
3. SendAPICmd(req) → SendCmd → DataToSend (Sliced, encrypted)
4. Поллит `pending.resp` каждые 10ms, timeout = 5000ms (default)
5. Когда response приходит (ClientNewData → ProcessApiCommand → pending.resp = resp)
6. Возвращает response

Rust-клиент:
- Нет PendingRequests механизма (fire-and-forget в текущей реализации)
- Ответ приходит в on_data callback как Command::API
- Для эмуляции SendAndWait нужен channel/oneshot

---

## Что уже работает в Rust:

- Transport handshake (Hello → Fine) ✅
- Ping exchange ✅  
- Прием данных (Order, UI, Strat, Balance, LogMsg) ✅
- Отправка API requests (send_api_request) ✅ (пакет уходит)

## Что НЕ работает:

- **API response не приходит** — request уходит, но response (inner_cmd=31) не виден.
  Гипотеза: сервер шлёт response, но SynLZ decompression ломается
  (response может приходить сжатым в MPC_Crypted|compressed обёртке).

## Ключевые файлы Delphi:

- `Unit1.pas:4987` — TCryptoPumpTool.InitInt (порядок вызовов)
- `MoonProtoEngine.pas:514` — SendAndWait (poll loop)
- `MoonProtoEngine.pas:563-922` — BaseCheck, AuthCheck, GetMarketsList, UpdateMarketsList, GetMarketsBalanceFull
- `MoonProtoEngine.pas:267` — SubscribeAllTrades
- `MoonProtoServer.pas:1043` — серверная обработка emk_SubscribeAllTrades
- `MoonProtoClient.pas:256-411` — ClientNewData (dispatch по cmd)
- `MoonProtoClient.pas:802` — ProcessApiCommand (matching response to pending)
