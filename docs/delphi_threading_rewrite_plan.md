# MoonProto Rust: рабочий план protocol machine-effect parity

Дата: 2026-05-22

Статус: рабочий документ для перестройки `moonproto`.

Рабочее правило Codex: не останавливаться, пока есть понятная следующая работа
по плану и нет вилки, требующей решения автора. Статус в чат — только на
узловых точках, при красном флаге или когда нужен выбор.

## Уточнение 2026-05-24: machine-effect важнее числа потоков

Два OS-потока Delphi не являются самостоятельным протокольным контрактом.
Контракт — тождественный machine effect: тот же порядок чтения байтов, проверки,
decrypt/decompress, мутации slider/token/ACK/gap/orderbook/trades/state,
immediate protocol replies и timer/send effects.

MoonProto спроектирован для среды, где порядок и тайминг доставки UDP не
определены. Ни один retry, gap bucket или epoch check не должен зависеть от
того, попал конкретный пакет в tick N или tick N+1. Граница batch'а — шум
внутри 200ms+ протокольных таймаутов.

Более точная формула: обработка prefix-а детерминирована. Эффект датаграммы
зависит от текущего state и самой датаграммы, но не от будущих датаграмм и не
от того, пришла она inline из `recv_from` или через очередь reader thread-а.
Если порядок полученных датаграмм сохранён, immediate side effects сохранены,
а send/maintenance phase не голодает, финальное protocol state должно совпасть.

Это граница доказательства для single-thread идеи: переход допустим только если
анализ Delphi-блоков подтверждает prefix-determinism для затронутого механизма
и тесты доказывают тот же machine effect.

## Вердикт

Старый вывод "надо обязательно два потока" заменён на более сильный критерий:
Rust-клиент должен повторять Delphi machine effect. Реализация может быть
reader+writer или single-thread non-blocking loop, если доказано то же состояние,
тот же порядок protocol side effects и те же timer/send решения.

Первичная Rust-проблема была не в одном потоке сама по себе, а в Rust-only
очередях и budgets: `EVENT_DRAIN_BUDGET`, deferred recv, смешивание user send
intents и server packets, зависимость API responses от `run_*`, расхождения в
SlicedACK/retry/Init timing.

Цель перестройки: одинаковый machine effect по блокам: кто читает байты, что
мутируется, когда ACK применяется, когда отправляется ответный пакет, какой
таймер двигается и какой state видит следующий шаг.

## Правила сверки для нового плана

Цель single-owner дизайна: код должен выглядеть ближе к Delphi — один владелец
protocol state, прямой доступ к структурам, минимум мостов упаковка->очередь->
распаковка. Так семантика обработки становится 1:1 в большем числе блоков, чем
в текущей reader/writer/shared-lock модели.

Каждый Delphi-блок получает классификацию:

- `INLINE`: быстрый bounded protocol/state effect. В Rust выполняется в
  `ProtocolCore` inline.
- `QUEUE`: Delphi делает `TThread.Queue`/UI-main work или действие потенциально
  тяжёлое. В Rust это задача для `AppQueue`, не для protocol loop.
- `SYNC`: Delphi реально блокируется (`TThread.Synchronize`, wait, sleep в
  domain path). Это красный флаг: отдельно доказать, переносить ли блокировку,
  заменить ли на очередь, или это не protocol-owned поведение.

`ProtocolCore` не вызывает пользовательский callback. Он может только:

- принять UDP, проверить, decrypt/decompress;
- обновить transport/protocol/domain state, если операция bounded;
- отправить immediate protocol reply (`SlicedACK`, Ping/PMTU replies);
- поставить send intent в свои очереди;
- положить public notification/task в `AppQueue`.

`AppQueue` — Rust-аналог Delphi `TThread.Queue`. Она выполняет user callbacks,
логи/UI-facing notifications, strategy/settings heavy apply, file IO и любые
действия, которые не должны задерживать protocol recv/send.

Доказываемость скорости обязательна. В `ProtocolCore` должны быть счётчики и
тайминги без влияния на поведение: `recv_count`, `protocol_ns`, `send_phase_ns`,
`max_tick_ns`, `active_dispatch_ns`, `app_enqueue_ns`,
`public_event_queue_len`. Если tick/phase выходит за ожидаемый budget — это
красный флаг, а не повод вводить drop/cap.

Перед переходом на `polling` нужен отдельный Windows UDP proof: socket
настраивается один раз, `Poller` будит по readable, timeout 5ms работает,
hot path не делает `set_nonblocking`/`set_read_timeout`, reconnect/rebind не
ломает регистрацию socket-а.

Zero-alloc trades/direct state write — второй этап после runtime-loop parity.
Сначала доказать `ProtocolCore + AppQueue`, потом заменять `Vec<Trade>`/event
payload delivery на Delphi-like direct ring-buffer write и notification-only
events.

## Delphi target model

Проверенные точки:

- `MoonProtoUDPClient.pas:454` - `TMoonProtoUDPClient.UDPRead`, reader thread.
- `MoonProtoUDPClient.pas:669` - `TMoonProtoUDPClient.Execute`, main/orchestrator/writer thread.
- `MoonProtoUDPClient.pas:738-746` - writer под `SendLock` копирует send queues, ACKs и ping ACK bitmap, потом вызывает `CheckSeningData`.
- `MoonProtoUDPClient.pas:848-852` - `UDPClient.ThreadedEvent := true`, Indy reader реально отдельный поток.
- `MoonProtoCommon.pas:488-541` - `DataReadInt`: decrypt/decompress, ping ACK bitmap в `TmpSlider`, затем `OnNewData`.
- `MoonProtoCommon.pas:667-707` - `OnNewSliced`: receive reassembly в reader, немедленный `MPC_SlicedACK`, затем `DataReadInt` для полного datagram.
- `MoonProtoCommon.pas:711-731` - `OnNewSlicedACK`: reader только складывает ACK в `ACKs`.
- `MoonProtoCommon.pas:733-741` - `GetCopyAcks`: writer копирует ACKs и очищает входной список.
- `MoonProtoCommon.pas:765-786` - `SendCmdInt`: user/app send intents кладутся в `DataToSend`, `DataToSendH`, `DataToSendL` под `SendLock`.
- `MoonProtoCommon.pas:869-1011` - `CheckSeningData`: writer создаёт Sliced, применяет ACKs, шлёт H/L, ретраит H и Sliced.
- `MoonProtoIntStruct.pas:844-876` - `ApplyRegularHLAck`: writer применяет regular H ACK bitmap.
- `MoonProtoIntStruct.pas:904-908` - `CopyRecvdData`: writer переносит `TmpSlider` в `RecvdSlider`.
- `MoonProtoIntStruct.pas:1200-1218` - `ApplyACK`: writer применяет SlicedACK к первому matching datagram и `break`.

Delphi model по факту:

1. Reader thread:
   - принимает UDP;
   - unwrap/decrypt outer transport;
   - checksum/ver/ErrEmu;
   - handshake receive branch;
   - ping branch с записью ACK bitmap в `TmpSlider`;
   - `MPC_Sliced`: собрать slice, немедленно отправить `MPC_SlicedACK`, при complete вызвать `DataReadInt`;
   - `MPC_SlicedACK`: только поставить ACK в ACK queue;
   - обычные packets: `DataRead`/`DataReadInt`/`OnNewData`.

2. Writer/orchestrator thread:
   - bind/rebind socket;
   - под `SendLock` сделать snapshot send queues, ACK queue, `TmpSlider`;
   - `CheckSeningData`: создать outgoing Sliced, применить ACKs, применить H ACK bitmap, отправить H/L, retry H/Sliced;
   - отправить Hello/HelloAgain;
   - reconnect/offline/dead-zone/force-disconnect;
   - sleep `DefaultNetThreadSleepTime`.

3. User/app send path:
   - не идёт через receive backlog;
   - `SendCmdInt` сразу пишет в send queues под `SendLock`;
   - writer потом копирует очереди и отправляет.

4. Domain receive path:
   - `OnNewData` вызывается из reader path;
   - часть обработчиков мутирует state сразу, часть делает `TThread.Queue` для UI/main-thread работы;
   - это надо проверять по каждому domain block отдельно, но transport/Sliced/ACK модель уже ясна.

## Current Rust model

Проверенные точки:

- `src/client.rs` - accepted UDP packets and user sends are not represented as
  event variants.
- `src/client.rs` - `SendLockState` holds `DataToSend*`,
  `incoming_sliced_acks`, and `TmpSlider`; production receive now calls
  `client_new_data` directly after decode. There is no production or test-owned
  pending decoded queue in `Client`, and no `ReaderDecodedMsg` test bridge.
- `src/client.rs` - `ProtocolCore::run` owns UDP receive, decoded delivery,
  `copy_send_ack_and_check_sening_data`, and send/maintenance in one caller
  thread.
- `src/client.rs` - the old `ReaderTransportState` mirror is gone. Receive-side
  stats, ping, handshake, reconnect flags, tokens, and keys are written
  directly into the single `Client` owner, matching Delphi's direct field
  mutation effect.
- `src/client.rs` - `DataReadState` is no longer a reader-runtime/shared object.
  MPSlider, decode cipher, and SizeAck series live directly in the single
  `Client` owner and are mutated inline from the `DataReadInt` path.
- `src/client.rs` - stale reader epoch guards are gone. They protected only the
  removed async reader closure; accepted UDP datagrams are now processed by the
  current single-owner `ProtocolCore` directly.
- `src/client.rs` - old production `spawn_reader` / `ReaderRuntime` path is
  removed. `ProtocolCore::recv_drain_phase` accepts UDP, then
  `process_datagram_inline` handles service commands, Sliced/SlicedACK,
  handshake, Ping, SizeTest/ProbeMTU, and data `DataReadInt` core.
- `src/client.rs` - immediate replies use `ProtocolCore::send_command`,
  matching the Delphi receive-side calls to `SendCommand` from
  SlicedACK/Ping/PMTU/ImFriend branches.
- `src/client.rs` - Ping handling mutates `Client` fields directly and writes
  `TmpSlider` inside `SendLockState`; writer later copies it and runs
  `ApplyRegularHLAck`.
- `src/client.rs` - handshake service commands no longer use
  `ReaderHandshakeUpdate`. `WrongHello`, `WantNewHello`, `NeedHelloAgain`,
  `WhoAreYou`, and `Fine` mutate the single `Client` owner directly in the
  receive block.
- `src/client.rs` - writer-owned `RecvdSlider` is a direct `Client` field.
  The Delphi order remains `TmpSlider` in `SendLockState` snapshot ->
  `RecvdSlider` -> `ApplyRegularHLAck`.
- `src/client.rs` - `pending_candles` is a direct `HashMap` in the single
  `Client` owner. The request registers the slot before send, and chunked
  response handling mutates/removes the slot inline from the receive/API path;
  only the final `mpsc::Sender` result crosses back to the API caller.
- `src/events.rs` - production active delivery uses
  `dispatch_into_active_actions` and an action outbox; old direct
  `dispatch_into_active(..., &mut Client)` production path is gone.

Текущее устройство Rust:

1. `ProtocolCore` receive phase:
   - принимает UDP;
   - делает outer unpack/checksum/ver/ErrEmu;
   - выполняет receive-side cleanup cadence;
   - обрабатывает handshake/control exits, Ping, SizeTest/ProbeMTU;
   - `MPC_Sliced`: собирает slice, немедленно отправляет `MPC_SlicedACK`, при
     complete выполняет общий `DataReadInt` decrypt/decompress core;
   - `MPC_SlicedACK`: кладёт ACK в writer/apply ACK queue;
   - обычные data packets и `MPC_Grouped`: выполняет `DataReadInt` core;
   - доставляет decoded payload/state updates напрямую в `client_new_data`
     до следующей UDP datagram.

2. `ProtocolCore` send/maintenance phase:
   - выполняет `OnNewData`/active delivery уже после каждого accepted datagram;
   - создаёт outgoing Sliced, применяет SlicedACK, ретраит, dispatch'ит active state;
   - API response приходит пользователю только пока этот loop крутится.

Decoded bridge is gone: production `DataReadInt`, unit tests, and run-loop tests
no longer do pack -> queue -> unpack before `client_new_data`.

## Главные расхождения, которые надо убрать архитектурно

### 1. Recv backlog не должен задерживать transport receive effects

В Delphi `MPC_Sliced` получает ACK немедленно из reader path. Rust уже отправляет
`MPC_SlicedACK` из receive phase и для полного Sliced выполняет общий
`DataReadInt` decrypt/decompress core в receive stack, затем удаляет
`Receiving`. `OnNewData`/active-library delivery теперь дренится после каждой
accepted datagram, а не после всего poll batch.

Target: `MPC_Sliced` обрабатывается в receive path:
`SlicingReceiver::on_new_sliced`, immediate `send_raw_packet(MPC_SlicedACK)`,
complete datagram идёт в `DataReadInt` path, а затем напрямую в
receive-owned `OnNewData`/active dispatch без зависимости от main-loop wake
budgeting. Следующий cleanup: убрать сам `ReaderDecodedMsg` bridge из
test-only scaffolding, если это можно сделать без потери тестовой
доказуемости. Test-owned `pending_reader_decoded` queue and `ReaderDecodedMsg`
уже удалены.

### 2. SlicedACK не должен применяться в reader

В Delphi reader складывает `MPC_SlicedACK` в `ACKs`, writer применяет ACK внутри `CheckSeningData`.
Rust now matches this part: reader parses ACK into `SendLockState`'s
`incoming_sliced_acks`; writer tick snapshots the same SendLock and then runs
`apply_copy_acks`.

Это важно для порядка: Delphi ACK применяется в одном writer cycle вместе с send/retry decisions.

### 3. Ping H ACK bitmap должен идти через TmpSlider -> RecvdSlider -> ApplyRegularHLAck

В Delphi `DataReadInt(MPC_Ping)` под `SendLock` пишет `TmpSlider`, writer копирует это через
`CopyRecvdData`, потом `ApplyRegularHLAck` чистит `PendingH`.

Rust now matches the copy/apply order: ping handling writes `TmpSlider` under
`SendLockState`, writer copies it to `RecvdSlider`, then `ApplyRegularHLAck`
removes ACKed `PendingH`.

### 4. User/app send intents не должны конкурировать с reader packets в общем event budget

В Delphi `SendCmdInt` пишет в send queues под lock. Входящий поток не может "съесть" бюджет обработки
и задержать постановку user команды в queue.

Target: public send APIs пишут в `SendQueues` напрямую через lock или через thin `ClientSender`, но не
через общий `ClientEvent` вместе с server recv.

### 5. `run_*` не должен быть мотором протокола

В Delphi transport работает пока жив thread. Блокирующий `SendAndWait` не обязан вручную качать UDP
receive path; reader и writer продолжают жить.

Target: `Client::start`/constructor поднимает worker'ы; `run_*` становится consumer'ом public events,
но не владельцем transport progress. `api_*` receiver должен получать response
без необходимости вызывать `run_until_response`.

### 6. Active lib сейчас сцеплена с `&mut Client`

На старте фазы `EventDispatcher::dispatch_into_active(..., client: &mut Client)`
делал auto-actions через `client.send_api_request`. Это мешало точной
двухпоточной модели: receive/domain обработчик и writer боролись за `&mut Client`.

Текущее состояние: production receive path уже вызывает
`EventDispatcher::dispatch_into_active_actions(...)`, передаёт snapshot
`ActiveDispatchContext`, получает `ActiveAction` outbox и только потом `Client`
применяет эти actions к send queues. Старый публичный
`dispatch_into_active(..., client)` удалён; тесты вызывают тот же action-outbox
шаг, который использует production path.

Target: active state должен выдавать `ClientAction`/`SendIntent` outbox, а не напрямую мутировать
transport client. Reader/domain path кладёт эти actions в send queues. User-visible events уходят в
отдельную public event queue.

## Целевая Rust-структура

### Типы

1. `Client`
   - public facade;
   - держит `ProtocolCore` owner/handle и `AppQueue` handle;
   - public API только ставит commands/subscriptions/requests в protocol queues
     через `ClientSender` и читает snapshots/events.

2. `ProtocolCore`
   - single owner protocol thread/loop;
   - владеет UDP socket, transport state, crypto sliders, send queues,
     Sliced receiver/sender, active state и reconnect state;
   - делает recv drain, app command drain, send/maintenance phase и wait 5ms;
   - не вызывает user callback и не делает blocking/heavy work.

3. `AppQueue`
   - отдельный worker/queue для Delphi `TThread.Queue`-класса действий;
   - выполняет user callbacks, public events, logs/UI-facing work, strategy/UI
     heavy tasks;
   - может ставить новые protocol send intents обратно через `ClientSender`;
   - не владеет protocol state.

4. `ClientSender`
   - единственный thread-safe вход из app/UI потоков;
   - не конкурирует с recv packets в общем event budget;
   - пишет command intent в protocol command queue/send queues без capacity cap
     и без drop branch.

5. `PublicEventQueue`
   - только для user-visible events;
   - не является частью transport progress;
   - если пользователь её не читает, transport всё равно работает.

6. `ProtocolMetrics`
   - диагностические counters/timings: recv packets, protocol time, send phase
     time, max tick, queue lengths;
   - не влияет на protocol decisions и не вводит budgets/drop.

### State ownership table

| State | Delphi owner/effect | Rust target owner/effect |
| --- | --- | --- |
| UDP receive buffer | reader thread | `ProtocolCore` local |
| Outer unpack/checksum/ver/ErrEmu | reader thread | `ProtocolCore` inline |
| Handshake receive state | reader writes client fields | `ProtocolCore` inline |
| Send queues H/S/L/Sliced intents | `SendCmdInt` under `SendLock`, writer copies | `ProtocolCore` owned queues; `ClientSender` sends intents |
| Incoming Sliced receiver | reader mutates `AClient.Receiving` | `ProtocolCore` mutates receive slicer state |
| Immediate SlicedACK | reader calls `SendCommand` | `ProtocolCore` sends direct ACK immediately |
| Incoming SlicedACKs | reader appends `ACKs`, writer copies/applies | `ProtocolCore` records/apply at matching send phase with Delphi order |
| Ping regular H ACK bitmap | reader writes `TmpSlider`, writer copies/applies | `ProtocolCore` preserves tmp->recvd->apply order inside loop |
| PendingH | writer owns in `CheckSeningData` | `ProtocolCore` owns/mutates in send phase |
| Outgoing `Sending` sliced | writer owns in `CheckSeningData` | `ProtocolCore` owns/mutates in send phase |
| Domain active state | reader path via `OnNewData`, some UI queued | `ProtocolCore` applies `INLINE`; `QUEUE` tasks go to `AppQueue` |
| Public callbacks/events | mixed: direct reader and `TThread.Queue` | `AppQueue`/public event queue only; never from protocol loop |

## Phase A-1: Delphi receive branch classification

Checked source:

- `MoonProtoClient.pas:256-449` — `TMoonProtoNetClient.ClientNewData`.
- `MoonProtoClient.pas:513-635` — `ProcessCommandOrder`.
- `MoonProtoClient.pas:689-805` — `ProcessStratCommand`.
- `MoonProtoClient.pas:807-901` — `ProcessApiCommand`.
- `MoonProtoEngine.pas:1216-1352` — balance snapshot/increment apply.
- `MoonProtoEngine.pas:1577-1919` — trades stream apply and gap tracking.
- `MoonProtoEngine.pas:1921-1945` — trades resend batch.
- `MoonProtoEngine.pas:1982-2044` — orderbook packet apply and full-request actions.
- `MoonProtoUDPClient.pas:857` + `IndyUDPHelper.pas:153-156` — client UDP uses
  `ThreadedEvent = true`; `Synchronize(UDPRead)` is not the active receive path.

Classification:

| Delphi branch | Class | Machine effect |
| --- | --- | --- |
| `MPC_Ping` | `INLINE` | Updates ping/time/pmtu/rate fields and immediately sends ping reply. |
| `MPC_Test`, `MPC_Test_Crypted` | `INLINE` | Updates test counters/log diagnostics only. |
| `MPC_LogMsg` | `QUEUE` | Parses server log, then `TThread.Queue` to UI log. |
| `MPC_Order/TAllStatuses` | `INLINE + actions` | Applies every order status through `ProcessCommandOrder`, updates snapshot flag, then `CleanupMissingWorkers` sends missing status requests. New worker UI notification is queued. |
| `MPC_Order/TBaseMarketCommand` | `INLINE + optional QUEUE` | Finds/creates local worker, adjusts server time, applies command inline; only new worker handoff uses `TThread.Queue`. |
| `MPC_Strat` | `QUEUE` | Entire `ProcessStratCommand` body runs in `TThread.Queue`: snapshot reply, snapshot apply/save, delete, checked sync/echo. |
| `MPC_API/RequestCandlesData` | `INLINE` | Stores candle chunks, merges when complete, flips market flags; no `TThread.Queue`. |
| `MPC_API/regular EngineResponse` | `INLINE` | Matches pending request by UID under lock and stores `p.resp`; no queued callback in this block. |
| `MPC_Balance/TArbPricesCommand` | `INLINE` | Parses compact arb payload immediately. |
| `MPC_Balance/snapshot-increment` | `INLINE` | Applies balances/positions to markets and recalculates total PnL immediately. |
| `MPC_TradesStream` | `INLINE + actions` | Decompresses/parses trades, mutates market trade buffers, gap buckets, detection state; queues only resize/UI helper tasks. Missing-packet resend is a protocol action after processing. |
| `MPC_TradesResendResponse` | `INLINE` | Splits batch and calls `ProcessTradesStream(..., False)` for every inner packet. |
| `MPC_OrderBook` | `INLINE + actions` | Decompresses/parses book, applies full/diff/cache state immediately; full request is a protocol action, redraw helpers may queue UI work. |
| `MPC_UI/TClientSettingsCommand` | `QUEUE` | `TThread.Queue` to `ApplySettingsFromServer`; command freed inside queued task. |
| `MPC_UI/TUpdateVersionCommand` | `QUEUE` | `TThread.Queue` to log and remote update handler. |
| `MPC_UI/TLevManageCommand` | `QUEUE` | `TThread.Queue` to apply leverage manager update. |
| `MPC_UI/TNewMarketNotifyCommand` | `INLINE` | Logs, triggers market-check event, frees command. |
| `MPC_UI/TArbActivateNotify` | `QUEUE` | `TThread.Queue` to apply arb activation notify. |

Phase A implication:

- `ProtocolCore` may inline bounded protocol/state effects for Order, Balance,
  Trades, TradesResend, OrderBook, API pending/candles and small UI notify.
- `AppQueue` must own the Delphi `TThread.Queue` class: server logs, full
  strategy command handling, settings/update/lev/arb UI commands, new-order UI
  handoff, resize/redraw/UI helper work.
- No active client receive branch was classified as `SYNC`; if later code reads
  find `TThread.Synchronize` under this receive graph, it is a red flag and must
  be added here before architecture changes.

## Новый порядок перестройки

### Phase A0 - short GOD-module split before proof work

Цель: уменьшить `src/client.rs` перед Phase A, но не трогать protocol
machine effect.

Разрешено только механическое выделение стабильных зон, которые не меняют
порядок вызовов, владение state, queues, timers, reconnect, ACK/retry и
runtime loop:

- diagnostics / ErrEmu / trace hooks;
- fixed wire structs with compile-time layout checks;
- маленькие pure helpers, если их границы уже очевидны и покрыты тестами.

Запрещено в A0:

- переносить reader/writer/runtime/reconnect/handshake;
- менять public API semantics;
- вводить новые queues, caps, budgets или callback boundaries.

Exit gate: `cargo test --lib`, FireTest/stress-ready build, diff проверен как
механический split без protocol behavior changes.

### Phase A - proof gates before code move

1. Сверить Delphi `OnNewData` branches и пометить каждый блок `INLINE`,
   `QUEUE` или `SYNC`.
2. Сделать Windows UDP `polling` prototype:
   - one-time nonblocking socket setup;
   - `Poller::wait(..., 5ms)` будит по readable;
   - recv drain до `WouldBlock`;
   - send/maintenance phase гарантированно выполняется;
   - rebind/reconnect не ломает регистрацию socket-а.
3. Добавить `ProtocolMetrics` в текущий Rust без изменения поведения:
   `recv_count`, `protocol_ns`, `send_phase_ns`, `max_tick_ns`, queue lengths.
4. Unit proof: одна последовательность decoded datagrams даёт одинаковый
   active/protocol state в текущей модели и в single-owner processor skeleton.

Exit gate: тесты зелёные, FireTest зелёный, metrics показывают bounded protocol
phase без callback blocking.

### Phase B - introduce `ProtocolCore` skeleton

Сначала без удаления старого runtime:

- выделить pure methods `recv_drain_once`, `process_datagram`,
  `drain_app_commands`, `send_maintenance_phase`, `wait_5ms`;
- оставить тот же parser/state код, но убрать лишние упаковка->очередь->
  распаковка внутри proof path;
- public callback не вызывается из `ProtocolCore`, только task/event enqueue.

Exit gate: unit equivalence tests + `cargo test` + FireTest.

### Phase C - introduce `AppQueue`

- заменить все потенциально blocking/user-facing callbacks на enqueue в
  `AppQueue`;
- классификация Delphi `TThread.Queue` должна быть записана рядом с кодом или
  в этом документе;
- `AppQueue` имеет no-cap семантику для correctness, diagnostics для длины
  очереди, но не drop policy.

Exit gate: тест, где user callback sleep/block не задерживает Ping/SlicedACK/API
response/retry.

Delphi classification checked on 2026-05-24:

- `MoonProtoCommon.pas:DataReadInt` decrypts/decompresses, applies Ping ACK
  bitmap, then calls `OnNewData` inline. This is still protocol/active state,
  not app callback.
- `MoonProtoClient.pas:ClientNewData` handles `MPC_Ping` inline and immediately
  calls `Client.SendPing(Ping)`.
- `MPC_TradesStream` and `MPC_TradesResendResponse` call
  `MainEngine.ProcessTradesStream/ProcessTradesResendBatch` inline; gap buckets
  and resend bookkeeping are protocol/domain state.
- `MPC_OrderBook` calls `OnOrderBookPacket(AStream)` inline.
- User/UI side effects go through `TThread.Queue`: server log UI,
  `TClientSettingsCommand`, remote update command, leverage manager, arb
  activation notification, new order worker UI callback
  `CryptoPumpTool.OnMServerOrder`, `ProcessStratCommand`, status-change
  callback, and orderbook predictor watch/unwatch.

Rust Phase C boundary:

- Keep protocol/domain machine effects inline with `ProtocolCore`: Ping,
  SlicedACK, API pending delivery, trades gap buckets/resend, orderbook cache
  recovery, order/balance/market state application.
- Move user-facing callbacks/notifications to `AppQueue`: raw `run` callback,
  `run_with_dispatcher` event callback, lifecycle callback, UI/log/status
  notifications. Until Phase D, `run_with_dispatcher_queued` already exercises
  the no-callback queued path.

### Phase D - switch live runtime to `ProtocolCore + AppQueue`

- single owner владеет socket/protocol/active state;
- `ClientSender` ставит intents без общего recv/app budget;
- `run_*` становится consumer public events, а не мотором transport progress;
- reconnect сохраняет Init-once и active-lib restore semantics.

Exit gate: full unit suite, examples check, FireTest, stress under `err_emu=10%`.

### Phase E - zero-alloc trades/direct state write

Делать только после runtime parity.

- `WireTrade`/`SectionIter` over bytes вместо `Vec<Trade>`;
- reusable decompress buffer;
- direct write в market ring buffers;
- callback/event только notification, данные читаются из state.

Exit gate: byte/wire tests, perf counters, FireTest/stress, API docs updated.

### Phase E2 - Active Lib `SeqRing` storage

Target storage model for hot historical data:

- `ProtocolCore` receives/decrypts/decompresses/parses packets and quickly
  hands typed batches to `StoreWorker`.
- `StoreWorker` is the single writer for hot retained history and immediately
  appends incoming rows into per-market `SeqRing`s.
- `SeqRing` is a single-writer / multi-reader retained ring: monotonic `seq`,
  independent readers, and retention clipping when a requested start is older
  than the oldest retained row. `seq`/cursor is an internal mechanism, not a
  mandatory public API shape.
- User API must not expose internal chunks/wrap/slots. A user that wants to draw
  retained trades asks for a simple view: last N rows, N rows from time T, a
  time range, or a position found by time.
- The protocol receive thread must not wait on history scans. Retained history
  writes are owned by `StoreWorker`, a separate writer thread/layer.
- Current implementation direction: dense `Vec<T>`/ring behind short
  `parking_lot::RwLock` sections. This keeps rows as a compact array for full
  scans, matches Delphi's dense history-array machine effect better than
  per-field atomics, and remains safe without exposing references to overwrite
  slots outside a read-locked closure.
- "Read only new rows" is per consumer: each user/UI/strategy thread owns its
  own `SeqRingCursor(next_seq)`. The ring does not have global consumed state.

Sizing:

- history capacities are configured from init/API with defaults;
- `0` disables retained public history for that category only;
- protocol-required state remains mandatory even when retained history is off.

Derived calculations:

- `StoreWorker` updates derived state at least every 250 ms.
- Rolling trade volumes for 1, 3 and 5 minutes use small accumulators, e.g.
  5-second buckets, and update only from new trades since the previous pass.
- Full scan over a `SeqRing` is a normal user-facing API for charts/analysis
  and is also allowed as a test oracle and CPU red-flag benchmark. Internal
  derived-state production code should still use the incremental form when it
  is straightforward and cheaper.
- Deltas `(max - min) / min * 100` are computed both on trades and candles.
  Trades use the same 5-second rolling buckets as 1/3/5 minute volumes.
  Candles are scanned in one pass over retained 5m rows plus the current candle.
- Candle volumes for 5m/15m/30m/1h/2h/3h/24h/72h are computed in that same
  candle pass. A second scan for another window is a CPU red flag.
- Active Lib maintains candles after the initial candle snapshot: trades update
  the current 5-minute candle; on window rollover the current candle is sealed,
  the next current candle starts, and old candles leave retention.
- Active Lib stores the current full orderbook, not historical orderbook arrays,
  unless a later API explicitly asks for history.
- Active Lib also stores Delphi's LastPrice line separately from detailed
  trades. Delphi draws the brown chart line from `Market.HistoryPrice`
  (`THistoricalPrices = current: single + RealTime: TDateTime`). The data
  source is `UpdateMarketsList`: the server sends `Bid/Ask`, the client
  computes `pLast = (Bid + Ask) / 2`, and Delphi `TMarket.AddFrom` appends
  `pLast` into `HistoryPrice`.
- Delphi compacts old detailed trades into `TMiniCandles` when the large trade
  array overflows. Rust must preserve that external meaning without array shift:
  `SeqRing` overflow compacts evicted rows into mini-candles. Exact Delphi
  thresholds/percentages must be checked before implementation.
- Detailed futures trades must not copy Delphi's temporary UI/storage bridge as
  the Rust production storage model. Delphi `AddTmpHOrder` writes to
  `tmpList/tmpTradesRead/tmpTradesWrite`, and the chart draws both retained
  `OrdersH` and the fresh tmp-ring before `BMarketHistoryWorker` moves rows into
  `OrdersH`. Active Lib should have one retained source: `StoreWorker` appends
  accepted futures rows directly into `SeqRing` and updates rolling
  volumes/current candle from that same stream. Late UDP/resend rows remain late;
  time-based public reads must scan/filter instead of relying on monotonic row
  timestamps.

### 2026-05-25 - SeqRing storage foundation

Done:

- Added `state::seq_ring`, a single-writer / multi-reader retained ring with
  monotonic sequences, retention clipping, last-N reads, sequence reads, and
  time-based helpers (`copy_from_time`, `copy_time_range`). Time-based helpers
  scan retained rows because futures trade timestamps are not guaranteed
  monotonic after UDP gap/resend recovery.
- Added `SeqRingWriter::push_with_evicted`, so StoreWorker can compact rows
  that leave retained detailed trade history into `TMiniCandle`-like aggregates
  instead of silently dropping them.
- Implemented the first storage without `unsafe`: initially row types provided
  atomic slots and a per-slot version word verified multi-field reads.
- Added `state::history::TradeHistoryRow`, matching Delphi `TTrade`
  (`Time: TDateTime`, `Price: Single`, signed `Qty: Single`) including Delphi's
  sign-bit `IsBuy` / `SameDirection` semantics.
- Added `state::history::MMOrderHistoryRow`, matching Delphi base `TMMOrder`
  (`Time: TDateTime`, `vol: Double`, `Q: Double`). Delphi taker/color companion
  data lives in `TStreamableRingBuffer<TMMOrder, TMMOrderData>` and remains a
  separate follow-up block.
- Added `state::history::MMOrderCompanionData`, matching Delphi `TMMOrderData`
  (`Taker: THLAddress`, `Color: TColor`) as a separate fixed companion row.
- Added `state::history::LastPricePoint`, matching Delphi `THistoricalPrices`
  (`current: Single`, `RealTime: TDateTime`), and `MiniCandle`, matching Delphi
  `TMiniCandle`.
- Added `compact_trades_to_mini_candles_like_delphi`, matching the
  `UseTradesCompression` body inside Delphi `TMarket.ResizeOrdersHistory`: group
  anchor, 5-second split, buy/sell volume, min/max price, and `T1`/`Now`
  append gates.
- Added `RollingTradeVolumes`: 5-second accumulator buckets for 1/3/5 minute
  buy/sell `Price * Abs(Qty)` values and quantities. This is the agreed Active
  Lib implementation direction for cheap derived volumes; precision error is
  bounded by one bucket width.
- Removed the wrong production `TradeJoinBuffer` direction. Delphi's temp ring
  is a UI/storage bridge, not the Active Lib retained-history model. StoreWorker
  now appends futures trades directly into retained `SeqRing`.
- Added `state::history_store::MarketHistoryStore`, the per-market single
  writer side intended for `StoreWorker`: retained futures/spot/liquidation/MM
  rings, LastPrice ring, mini-candle ring, rolling volumes, and evicted-futures
  buffering for later mini-candle compaction.
- Added `MarketHistoryStore::append_last_price_like_delphi`, matching the
  `UpdateMarketsList -> pLast -> TMarket.AddFrom -> HistoryPrice` append gate:
  append only when `pLast > 0`, bid/ask is present, and the market is BTC or
  base-USDT.
- Removed delayed temp-ring drain; the accepted target is direct StoreWorker
  append into retained `SeqRing`.
- Added `MarketPrice::chart_price_step`, matching Delphi
  `AddNewAksPrice(Ask)`: update to `Max(eps, Ask / 5000)` only when `Ask > eps`
  and otherwise keep the previous value. It remains market-price metadata; Rust
  retained trade storage no longer uses it for a temp join path.
- Added `TradesPacketTimeShift`, matching Delphi `ProcessTradesStream`
  per-packet time correction: first known/stored row fixes
  `round((NowTimeX - RowTime) * 24) / 24`, later rows reuse the same shift, and
  skipped unknown-market sections do not initialize it.
- Added `MarketHistoryStore::*_stream_*_like_delphi` helpers for
  futures/spot/liquidation/MM rows. These are the explicit bridge from
  `TradesStream` section rows to retained storage: compute Delphi shifted row
  time, then append through the correct retained path.
- Added aligned MM-order companion storage and `hl_address_color_like_delphi`.
  This mirrors `TStreamableRingBuffer<TMMOrder,TMMOrderData>` slot pairing and
  Delphi `HLAddressColor` instead of storing taker/color as a detached list.
- Added `MarketHistoryRegistry`, an on-demand map of per-market stores. Runtime
  integration must not allocate full retained rings for every market just
  because `GetMarketsList` returned it; stores are created for enabled
  markets/categories.
- Added `MarketHistoryConfig::from_total_memory_bytes`: default capacity sizing
  helper for future init/config wiring. It budgets ~20% of total memory for
  retained histories, or 25% below 8 GiB, then splits the per-market budget
  across categories using `SeqRing` dense row-size estimates.

### 2026-05-25 - SeqRing dense locked backend

Decision:

- Replaced the first atomic-field `SeqRing` backend with a dense ring:
  `Vec<T>`/ring state under `parking_lot::RwLock`.
- Reason: full scans over 100K trades are a first-class Active Lib use case.
  Atomic field per scalar made a scan do several atomic loads per row and hid
  the dense array from optimizer/cache behavior. That is worse than Delphi's
  compact history arrays for this layer.
- The protocol thread is not the writer for retained history. `StoreWorker`
  owns appends, so a short history lock cannot block UDP receive.
- Added `SeqRingCursor` and `SeqRingReader::copy_new_since`: every consumer
  keeps its own cursor, so "read only new rows" is per thread/user, not global.
- Added zero-copy read closures (`with_from_seq`, `with_last`) for fast scans
  while keeping the lock scoped to the closure.

Verification:

- `cargo test seq_ring --lib` OK: 16 tests.
- `cargo test history --lib` OK: 22 tests.
- `cargo test history_store --lib` OK: 7 tests.
- `cargo test --lib` OK: 695 tests.
- `cargo check --examples` OK.

### 2026-05-25 - Retained futures trades keep Delphi DontSort order

Superseded correction 2026-05-26: this block fixed a real sort/skip-tail bug,
but incorrectly treated Delphi `tmpList/tmpTradesRead/tmpTradesWrite` as the Rust
production storage model. Later Delphi chart reading showed the tmp-ring is a
fresh-trade overlay in addition to retained `OrdersH`. Active Lib target is
direct `StoreWorker -> SeqRing` append with scan/filter time reads.

Done:

- Re-checked the active Delphi path:
  `BMarketHistoryWorker.Execute -> m.JoinHOrders(0, NowTime, false, true)`.
  The final `true` is `DontSort`, so live futures trades are copied from
  `tmpList/tmpTradesRead/tmpTradesWrite` directly into retained history.
- Removed the Rust-only sort/skip-tail retained append step.
- `SeqRing` time-based helpers no longer assume timestamp monotonicity; they
  scan/filter retained rows because late UDP/resend rows remain late.
- The RAM-sized temp join-ring decision is obsolete and the production temp join
  capacity was removed under the direct `StoreWorker -> SeqRing` target.
- Recorded the decision in root `library_decisions.md` and the closed red flag
  in `spec_pipeline/work/хуйня.md §X.161`.

Verification:

- `cargo test seq_ring --lib` OK.
- `cargo test history --lib` OK.
- `cargo test --lib` OK: `704 passed`.
- `cargo check --examples` OK.
- Quick prod FireTest OK: `FIRETEST_QUICK_PASS after 26.08s`, `ParseFailed=0`,
  err_emu actual drop `10.57%`.

### 2026-05-25 - retained history worker first wiring

Decision:

- Added `MarketHistoryWorker`: a retained-history writer thread that owns
  `MarketHistoryRegistry` and all per-market `MarketHistoryStore` instances.
- `EventDispatcher` accepts a `MarketHistoryHandle` for custom worker/config
  setups. The normal active-lib path now lazily starts its own default
  `MarketHistoryWorker` once an all-trades storage scope becomes active, so
  `subscribe_all_trades` / `subscribe_trades_for` is the semantic storage gate,
  not a hidden extra worker-registration step.
- The active dispatch path converts applied `TradesStream` packets into typed
  `MarketHistoryStreamBatch` values and queues them to the worker.
- Superseded on 2026-05-25: `ensure_market` was removed from the normal API.
  Stores are now configured from the trades subscription scope: all known
  markets for `subscribe_all_trades`, or the selected subset for
  `subscribe_trades_for`. Without an all-trades subscription, retained
  trade/candle/derived storage stays disabled.
- The worker command channel is intentionally unbounded: retained history must
  not drop packets because of an internal Rust-only capacity cap. If memory
  pressure appears, it is a separate backpressure design task, not a hidden
  queue-full branch.
- The UDP protocol receive path still does not own history locks. It only
  parses/applies protocol state and queues the already-decoded batch.

Verification:

- `cargo test history_worker --lib` OK.
- `cargo test active_dispatch_queues_trades_into_history_worker_without_direct_store_write --lib` OK.
- `cargo test --lib` OK: 698 tests.
- `cargo check --examples` OK.

### 2026-05-25 - LastPrice history wired from UpdateMarketsList

Done:

- Re-checked Delphi `TMoonProtoEngine.UpdateMarketsList`: after applying
  `Bid/Ask`, it computes `pLast = (Bid + Ask) / 2`, updates
  `ChartPriceStep`/delta state, then calls `If m.pLast > _epsM then m.AddFrom`.
- Re-checked Delphi `TMarket.AddFrom`: `HistoryPrice` receives the brown
  LastPrice row only inside the `IsBTCMarket or IsBaseUSDTMarket` gate and only
  when the row has a real bid or ask.
- Rust active dispatcher now collects LastPrice rows during
  `UpdateMarketsList` apply and queues `MarketHistoryLastPriceBatch` into
  `MarketHistoryWorker`. The worker writes them through
  `MarketHistoryStore::append_last_price_like_delphi`.
- This closes the gap where Rust had the store helper but no live active path
  feeding it from `UpdateMarketsList`.

Verification:

- Added worker and active-dispatch tests for LastPrice batch storage.
- `cargo test last_price --lib --quiet` OK: 5 tests.
- `cargo fmt --all --check` OK.
- `cargo test --lib --quiet` OK: 707 tests.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest OK: `FIRETEST_QUICK_PASS after 24.11s`, `ParseFailed=0`,
  err_emu actual drop `9.97%`.
- Follow-up FireTest gate now asserts retained LastPrice from live
  `UpdateMarketsList`; quick prod OK: `FIRETEST_QUICK_PASS after 25.81s`,
  retained LastPrice `current=77568.00781250`, `ParseFailed=0`.
- Next FireTest gate enables tiny retained futures/spot rings for the target
  market and asserts that live `TradesStream` rows reach `MarketHistoryWorker`.
- Quick prod FireTest with this gate OK: `FIRETEST_QUICK_PASS after 22.89s`,
  retained target trades `futures=1 spot=0`, `ParseFailed=0`.
- Added dispatcher-to-worker unit coverage for all retained stream section
  kinds: futures, spot, liquidation, MM orders, and MM companion rows.
- Exposed worker/handle `rolling_volumes(market, now_time)` so the already
  maintained 1/3/5 minute volume accumulators are reachable from the public
  Active Lib API without allocating unknown markets.
- Added `MarketHistoryConfig::from_system_memory(market_count)`: OS physical
  RAM probe + fallback to fixed `Default`, then the existing per-market budget
  sizing helper. This is the API default path for retained history after Init
  knows market count.

### 2026-05-25 - retained candles and derived analytics live gate

Done:

- Active retained candles now receive the completed `RequestCandlesData`
  snapshot through `EventDispatcher::apply_candles_snapshot`.
- `MarketHistoryStore` keeps the last 5m candle as current candle and updates
  it from retained futures trades; rollover is handled by the 250ms
  `StoreWorker` maintenance path.
- A futures trade after the 5-minute boundary seals the previous current candle
  row and starts a new current candle row.
- Derived snapshots now expose trade volumes/deltas, candle deltas, candle
  volumes, LastPrice-line deltas, and the combined deltas view.
- Candle deltas and candle volumes are calculated in one pass over retained 5m
  rows plus the current candle.
- LastPrice-line deltas are calculated from the retained `HistoryPrice` line
  fed by `UpdateMarketsList -> TMarket.AddFrom`, covering the Delphi
  `CheckHourlyValues` 15m/30m/1h source.
- When trades storage is enabled after Init already applied `UpdateMarketsList`,
  Rust backfills retained LastPrice rows from current `MarketsState`; otherwise
  the Active Lib could expose empty `HistoryPrice` until the next periodic
  market update, which Delphi cannot do because `HistoryPrice` lives on
  `TMarket` from the start.
- The combined 2h/3h/24h delta fields are floored by the combined 1h delta,
  matching Delphi `RecalcPumpQ` (`Last2hDelta/Last3hDelta/Last24hDelta :=
  Max(Last1hDelta, raw_window)`).
- Long candle delta windows follow Delphi's `h := trunc(24*(Now-Time))`
  bucket conditions: `Last2hDelta` uses `h <= 2`, `Last3hDelta` uses `h <= 3`,
  and `Last24hDelta` uses `h <= 24`. Candle volume fields keep exact window
  semantics because Delphi's volume fields are separate (`HVol`, `HVol3`,
  `dVol`). All candle windows exclude the exact old boundary, matching Delphi
  checks like `abs(Now-Time) < 15/MinsInDay` and `h < 72`.
- Quick FireTest now also checks that retained futures trades feed the derived
  trade-volume snapshot, so "rows stored but analytics dead" is caught before
  the full candles stress.
- Full FireTest now also verifies the retained-candles side of the same path:
  the target parsed candle count must be retained up to ring capacity, and the
  retained target candles must produce positive one-hour candle volume.
- Active `EventDispatcher::dispatch_into_active_actions` now also enforces the
  agreed Rust all-trades opt-in gate. The live `Client` loop already dropped
  unexpected `TradesStream` packets before dispatcher delivery; the dispatcher
  active path now has the same guard for custom active loops.

Verification:

- `cargo test --lib` OK: 715 tests.
- `cargo check --examples` OK.
- Quick prod FireTest OK: `FIRETEST_QUICK_PASS after 23.72s`,
  retained target trades `futures=3 spot=0`, derived
  `trade_vol_5m=200.5955`, `ParseFailed=0`.
- After the dispatcher-level trades opt-in guard: `cargo test --lib` OK:
  717 tests; quick prod FireTest OK: `FIRETEST_QUICK_PASS after 25.60s`,
  retained target trades `futures=2 spot=0`, derived
  `trade_vol_5m=551.4910`, `ParseFailed=0`.
- After the retained-candles FireTest gate: `cargo test --lib` OK: 717 tests;
  `cargo check --examples` OK; quick prod FireTest OK:
  `FIRETEST_QUICK_PASS after 23.28s`; full prod FireTest OK after about 183s.
- After matching Delphi `RecalcPumpQ` long candle delta windows: targeted
  `candle_long_delta_windows_match_delphi_trunc_hour_buckets` OK;
  `cargo test --lib` OK: 718 tests; `cargo check --examples` OK; quick prod
  FireTest OK: `FIRETEST_QUICK_PASS after 23.39s`; full prod FireTest OK after
  about 174s.
- After adding LastPrice-line derived deltas and strict old-boundary candle
  windows: `cargo test --lib` OK: 720 tests; `cargo check --examples` OK;
  quick prod FireTest OK: `FIRETEST_QUICK_PASS after 27.17s`; `cargo test
  --test fire_test --no-run` OK.
- Candle rollover unit gate:
  `futures_trades_roll_current_candle_after_five_minutes` OK;
  `cargo test state::history_store --lib` OK: 17 tests; full
  `cargo test --lib` OK: 721 tests; `cargo check --examples` OK; `cargo test
  --test fire_test --no-run` OK.
- LastPrice backfill red flag fixed after quick FireTest caught an empty
  retained LastPrice ring when `UpdateMarketsList` happened before
  `subscribe_all_trades`: targeted
  `enabling_trade_storage_backfills_current_last_price_history` OK; full
  `cargo test --lib` OK: 722 tests; `cargo check --examples` OK; `cargo test
  --test fire_test --no-run` OK; quick prod FireTest OK:
  `FIRETEST_QUICK_PASS after 23.63s`; full prod FireTest OK after about 176s.

### 2026-05-26 - internal retained-store creation name cleaned up

Done:

- Re-checked the retained-history scope rule: stream batches, LastPrice batches,
  and candle snapshots use `registry.get_mut(...)` and do not allocate unknown
  or out-of-scope markets.
- Removed the internal `ensure_market` helper name from code. Store creation is
  now named and used only as `insert_configured_market` inside
  `configure_markets`, i.e. only from the `subscribe_all_trades` /
  `subscribe_trades_for` scope.
- Updated the registry unit test so it proves allocation from configured scope,
  not ad-hoc on-demand allocation.

Verification:

- `cargo test history_store --lib --quiet` OK: 17 tests.
- `cargo test history --lib --quiet` OK: 40 tests.
- `cargo test history_worker --lib --quiet` OK: 7 tests.
- `cargo test seq_ring --lib --quiet` OK: 18 tests.

### Phase Z - final full optimization pass

Делать в самом конце, после protocol/runtime parity и после того, как крупные
архитектурные мосты уже убраны. Это не optional cleanup, а обязательный gate.
Работа по порту не считается закрытой, пока этот проход не сделан по всему
protocol-owned коду, а не только по случайно найденным горячим местам.

Цель: разобрать и оптимизировать всё protocol-owned, что можно упростить или
ускорить без изменения Delphi machine effect:

- allocations/clones, особенно на больших strategy/market/order/trades данных;
- locks/channels/queues/snapshots и лишнюю упаковка->очередь->распаковка;
- binary parsers и packed wire structs;
- Sliced/ACK/retry loops;
- API response/candles parsing and delivery;
- active-lib state apply paths and event notification paths.
- all remaining CPU red flags from FireTest/stress/protocol metrics.

FireTest/stress CPU summaries становятся gate. Любой protocol-owned sample
`>1ms` должен получить точное объяснение и фикс, если его можно убрать. Если
sample относится к cold/non-protocol/app work, это надо доказать метриками и
вынести из protocol loop, а не списывать как "нормально". Никаких accepted
deviation без отдельного согласования.

Post-publication storage/access review goes after this optimization gate, not
inside the current parity rewrite: compare dense locked `SeqRing` against
page/RCU or other history backends on real metrics and only then decide whether
another backend is worth the complexity.

## Исторический план и progress log 2026-05-22

Ниже старые фазы reader/writer rewrite. Они оставлены как история уже
выполненных шагов и источник проверок, но новый target — `ProtocolCore +
AppQueue` при machine-effect parity.

### Phase 0 - freeze current behavior

Already done before this doc:

- committed current Rust fixes in nested `moonproto`;
- committed root working rules/docs;
- `cargo test`: 360 passed before this architecture doc;
- live FireTest after Sliced parity still depends on the Sliced retry/server
  ACK issue under `err_emu=10%`. On 2026-05-23 the Delphi server fix was
  changed again: ACK-progress resets `FRetryCount` and removes ACKed pieces, but
  preserves remaining pieces' `LastChecked` clocks. Rust
  `Client::apply_sliced_ack` mirrors that current machine effect.
  Budget/adaptive rate math remains test-pinned to Delphi. Full FireTest under
  the rebuilt live server still needs a fresh verification run.

### Phase 1 - extract pure Delphi-named blocks without changing behavior

Goal: make code movable.

Create Rust methods with Delphi-equivalent names and no behavior changes:

- `udp_read_transport_unpack`;
- `udp_read_handle_command`;
- `on_new_sliced`;
- `on_new_sliced_ack`;
- `copy_send_list`;
- `copy_acks`;
- `copy_recvd_data`;
- `check_sending_data`;
- `apply_regular_hl_ack`.

Tests:

- existing unit tests must stay green;
- add tests that `SlicedACK` branch can be switched from immediate apply to queued apply without changing final writer tick result.

### Phase 2 - introduce queues matching Delphi

Closed target: remove semantic dependence on `ClientEvent::Send` for user/API
commands:

- public `send_*` writes to `SendQueues`;
- `api_pending.register(uid)` still happens before queue insert;
- subscription/control events become send intents plus registry mutations, not app events in common recv queue;
- no capacity cap, no queue-full branch.

Tests:

- 5 parallel API commands enqueue independently and all appear in copied H/S/L queues;
- dense fake recv stream cannot delay enqueue of user/API command;
- UKey dedup matches `SendCmdInt`.

### Phase 3 - move Sliced receive into reader

Move only `MPC_Sliced` first, because it is the live FireTest red zone.

Reader must:

- parse slice;
- call receive slicer;
- send `MPC_SlicedACK` immediately;
- on complete datagram call the same `DataReadInt` path as non-sliced packets.

Writer must not be required for incoming Sliced progress except to send queued actions produced by
domain processing.

Tests:

- receiving one Sliced block emits ACK before any writer tick;
- complete incoming Sliced calls data dispatch without depending on main-loop
  event budgeting;
- `err_emu=10%` FireTest must receive full candle snapshot or produce a narrower logged failure.

### Phase 4 - move SlicedACK and ping ACK to Delphi copy/apply order

Reader:

- `MPC_SlicedACK` -> append parsed ACK to `AckQueues.sliced`;
- `MPC_Ping` -> update ping fields, send ping response, write `TmpSlider`.

Writer:

- in one tick: `copy_acks`, `copy_recvd_data`, then `check_sending_data`;
- inside `check_sending_data`: `ApplyRegularHLAck` and `ApplyACK`.

Tests:

- duplicate/no-new-flags ACK is no-op;
- ACK applies only to first matching datagram;
- H ACK does not remove `PendingH` until writer copy/apply tick;
- order of copy/apply/send/retry matches `MoonProtoCommon.pas:869-1011`.

### Phase 5 - split handshake/reconnect exactly

Reader keeps Delphi receive side:

- `WrongHello`, `WantNewHello`, `NeedHelloAgain`, `WhoAreYou`, `Fine`;
- state fields mutated with exact Delphi machine effect.

Writer keeps Delphi send/timer side:

- bind socket;
- initial Hello or HelloAgain;
- waiting hello throttle;
- offline reconnect;
- hello-again timeout socket recreate;
- dead zone;
- force disconnect.

Tests:

- block-by-block parity tests for each handshake command;
- BaseCheck after AuthDone still passes without artificial sleep/retry;
- reconnect preserves intended post-init state and does not rerun Init.

### Phase 6 - decouple active lib from `&mut Client`

Replace direct `&mut Client` active dispatch with an action outbox:

Old shape:

```rust
dispatcher.dispatch_into_active(cmd, payload, now_ms, out, self)
```

Target shape:

```rust
let ctx = ActiveDispatchContext::from_client(self);
dispatcher.dispatch_into_active_actions(cmd, payload, now_ms, out, &ctx, &mut actions);
self.apply_active_actions(actions.drain(..));
```

Rules:

- active state owns markets/indexes/balances/orders/strats/settings state;
- post-init resync sends `TStratSnapshot.CreateFromStrats` equivalent from
  library-owned strats state between `TAllStatusesReq` and settings request;
- strategy snapshot request is answered from library-owned strats state;
- reconnect maintenance actions are produced by active/transport state and queued to writer;
- user-visible events go to `PublicEventQueue`;
- no user callback while holding `TransportCore`/`ActiveCore` locks.

Tests:

- `TStratSnapshotRequest` produces snapshot reply from local strats state;
- post-init resync enqueues the full local strategy snapshot;
- `OrderBookEvent::RequestFullNeeded` queues `RequestOrderBookFull`;
- token change invalidates/rebuilds market indexes as Delphi does;
- public callback can stall without stopping transport receive/send.

### Phase 7 - demote `run_*` to event API

After worker threads own progress:

- `run(duration, cb)` consumes public events for duration;
- `run_with_dispatcher` either becomes a thin event consumer around internal active state or is intentionally removed;
- `run_until_response` no longer pumps protocol, it waits on receiver while workers continue.

API docs must be updated in the API docs themselves if signatures/semantics change.

Tests:

- call `api_base_check`, block on receiver, no manual `run_until_response`, response still arrives;
- callback not reading events does not stop ping/SlicedACK/retry;
- examples either continue working or are intentionally updated with docs.

### Phase 8 - remove Rust-only budget/defer machinery

Delete after previous phases are green. `EVENT_DRAIN_BUDGET`, recv
`ClientEvent` protocol progress, and app/control FIFO send paths are now gone
from live code:

- `EVENT_DRAIN_BUDGET`;
- recv `ClientEvent` as protocol progress mechanism;
- `deferred_recv`;
- app/recv shared arbitration in `run_inner`.

Keep only queues that have Delphi equivalents or explicit user-facing purpose.

Tests:

- `rg "EVENT_DRAIN_BUDGET|deferred_recv"` returns no production hits;
- all unit tests green;
- FireTest green under normal channel and `err_emu=10%`;
- live logs show immediate SlicedACK per incoming slice and full candle snapshot.

## Non-negotiable invariants

- No new `DEVIATION.md` entry unless user explicitly accepts it.
- No internal queue cap/drop unless Delphi has the same cap or user accepts a deviation.
- No "budget" wording as harmless detail until Delphi equivalence is proven.
- Incoming SlicedACK order: reader queues, writer applies.
- Incoming Sliced order: reader ACKs immediately.
- Ping H ACK order: reader writes tmp, writer copies/applies.
- Init remains one time per `Client` session. Reconnect does not rerun Init.
- Strategies are not proactively re-requested after reconnect; reply only to server request.
- Active lib maintains user-requested markets/indexes/balances/orders/strats/settings/subscriptions itself after Init.
- API docs are updated in-place for any public API or semantic change.

## Immediate next implementation block

Start with Phase A0, then Phase A from the new plan.

Do not start from public API cleanup or zero-alloc trades. First do only the
short mechanical GOD-module split, then prove the new runtime boundary:

```text
ProtocolCore fast bounded work:
  UDP recv/process/send-maintenance only

AppQueue:
  callbacks / logs / settings-strategy heavy apply / user-visible work
```

Required first artifacts:

- A0: `client` diagnostics/ErrEmu split with public paths preserved;
- Delphi branch classification: `INLINE` / `QUEUE` / `SYNC`;
- Windows UDP `polling` prototype;
- `ProtocolMetrics` added without behavior change;
- equivalence test skeleton for same datagram sequence -> same state.

## Progress log

### 2026-05-24 - Phase A0 started

Done:

- `client` diagnostics / ErrEmu / diagnostic-trace hooks moved to
  `src/client/diagnostics.rs`;
- public paths preserved: `moonproto::client::set_err_emu`,
  `ErrEmu*Diagnostics`, hidden `ERR_EMU_RATE`;
- runtime, reader/writer, reconnect, ACK/retry, send queues and callback
  boundaries were not changed by this split.

Checks:

- `cargo test --lib`: 596 passed;
- `cargo test --lib --features diagnostic-trace`: 596 passed;
- `cargo test --test fire_test --no-run`;
- `cargo check --examples`.

### 2026-05-24 - Phase A proof gates started

Done:

- A-1 Delphi receive branch classification added above.
- A-2 cross-platform UDP polling prototype added as `tests/udp_polling.rs`.
- A-3 passive `ProtocolMetrics` added without control effect.
- A-4 first decoded-batch equivalence proof added: the same ordered
  `TradesStream` decoded sequence processed one-by-one or drained as one batch
  must produce the same active state, same resend event, and same queued
  `emk_TradesResend` send intent.

Polling proof result:

- socket is configured nonblocking once;
- `Poller::wait(..., 5ms)` returns without readable events on an empty UDP socket;
- after several datagrams, one readable event lets the loop drain `recv_from`
  until `WouldBlock`;
- `Poller::modify` rearms the same socket after drain;
- `Poller::delete` + binding a fresh UDP socket + `Poller::add` works, proving
  reconnect/rebind can re-register a socket without changing socket options on
  the hot path.
- This test must run on every supported OS. Current local run proves Windows;
  Linux/macOS are expected to use `polling`'s epoll/kqueue backends, but remain
  unproven until the same test runs there.

Check:

- `cargo test --test udp_polling`: 1 passed on Windows.
- `cargo test --lib`: 598 passed.
- `cargo check --examples`: passed.
- `cargo test --test fire_test --no-run`: passed.
- `cargo test --test fire_test -- --ignored --nocapture`: passed against the
  configured live server, including `err_emu=10%` initial health and `err_emu=50%`
  high-loss simple-ops gates.

ProtocolMetrics proof:

- metrics live in `src/client/metrics.rs`;
- `Client::protocol_metrics_snapshot()` reports recv count, reader protocol ns,
  writer tick ns, send phase ns, and current/max receive-decoded queue length;
- `Client::protocol_metrics_snapshot_with_dispatcher(&dispatcher)` also reports
  dispatcher public event queue length;
- unit test proves queue lengths are observable while recv count remains
  unchanged; metrics do not affect ACK/retry/reconnect/drop decisions.

Decoded timestamp red flag closed:

- During A-4 proof Rust still passed writer `cur_tm` into active dispatcher for
  queued decoded payloads. Delphi `DataReadInt -> OnNewData ->
  ProcessTradesStream` runs in the UDP reader path and `ProcessTradesStream`
  takes `NowTimeX := Now` inside that immediate call.
- Rust now passes `ReaderDecodedMsg.timestamp_ms` into domain dispatch, so
  trades gap/retry timers use packet receive processing time, not the batch's
  writer tick time.
- Unit test `decoded_batch_uses_receive_timestamp_for_active_timers` would miss
  the expected `TradesResend` with the old writer-tick timestamping when three
  decoded packets were drained in one writer tick.

Linux cross-platform gate:

- 2026-05-24: the same `tests/udp_polling.rs` logic passed on the documented
  Linux VPS in a temporary crate because the VPS `/root/work/moonkernel`
  checkout is older and does not yet contain this test target.
- Result: Ubuntu Linux 6.8, `polling = 3.11.0`, `cargo test --test udp_polling
  --quiet`: 1 passed.
- Next time the VPS checkout is synchronized, run the real repo command
  `cargo test --test udp_polling --quiet` there too. This is a repository-sync
  gate, not a protocol-behavior blocker.

### 2026-05-24 - Phase B first skeleton split

Done:

- `ReaderRuntime::run` now delegates without behavior change to
  `recv_drain_once` and `process_datagram`.
- `WriterRuntime::run` now delegates without behavior change to
  `writer_tick_prologue`, `ensure_socket_bound`, `drain_app_commands`,
  `wait_5ms`, and `send_maintenance_phase`.
- At this point `WriterRuntime` was a compatibility alias to the first
  `ProtocolCore` skeleton. The alias was removed later in Phase D5; owned
  protocol/orchestrator methods live on `ProtocolCore`.

Reason:

- These names are the first `ProtocolCore` skeleton boundary. They keep the
  same call order and side effects, but give the next step exact blocks to move
  or prove against Delphi `UDPRead` / `Execute`.

Checks:

- `cargo test --lib --quiet`: 598 passed.
- `cargo test --test udp_polling --quiet`: 1 passed on Windows.
- VPS Linux temporary-crate copy of `tests/udp_polling.rs`: 1 passed.
- `ProtocolCore` alias step: `cargo fmt --check`, `cargo test --lib --quiet`,
  `cargo check --examples --quiet`, `cargo test --test fire_test --no-run
  --quiet`, and live `cargo test --test fire_test -- --ignored --nocapture`
  passed.
- `cargo check --examples --quiet`: passed.
- `cargo test --test fire_test --no-run --quiet`: passed.
- `cargo test --test fire_test -- --ignored --nocapture`: passed against the
  configured live server, including `err_emu=10%` initial health and `err_emu=50%`
  high-loss simple-ops gates.

### 2026-05-24 - Phase C0 AppQueue container

Done:

- Added explicit internal `AppQueue<T>` with no fixed capacity and no drop
  policy. It records maximum observed length as diagnostics only.
- `EventDispatcher` one-shot queued events now use `AppQueue<Event>` instead of
  a raw `Vec<Event>`.
- Public API docs now expose `queued_event_max_count()` next to
  `queued_event_count()`.

Reason:

- This is the first app/protocol boundary object for Phase C. It preserves the
  existing one-shot helper behavior, but makes the correctness rule explicit:
  app-side event accumulation is unbounded like Delphi `TThread.Queue`; growth
  is a metric, not a reason to drop events.

Checks:

- `cargo fmt --check`: passed.
- `cargo test --lib --quiet`: 599 passed.
- `cargo test app_queue_keeps_all_events_and_records_max_len_without_drop_policy
  --quiet`: passed.
- `cargo check --examples --quiet`: passed.
- `cargo test --test fire_test --no-run --quiet`: passed.

### 2026-05-24 - Phase C1 callback queue for common run paths

Done:

- Production `Client::run` now delivers raw `(Command, Vec<u8>)` through an
  unbounded application callback channel. The protocol writer sends owned
  payloads into the channel and continues.
- Production `Client::run_with_dispatcher` now delivers typed `Event` values
  through an unbounded application callback channel after `EventDispatcher`
  state and active-library actions are applied.
- `run_with_dispatcher_state` was left inline in this phase because its callback
  borrowed the live dispatcher state. This was closed in Phase C3 with
  `EventDispatcherSnapshot`.

Reason:

- This closes the direct user-callback blocking risk for the two common public
  run paths without changing protocol order: Ping/SlicedACK/API pending/trades
  and orderbook state still execute inside `ProtocolCore`; only user
  notification leaves through `AppQueue`.

Checks:

- `cargo fmt --check`: passed.
- `cargo test raw_run_callback_block_does_not_extend_protocol_writer_tick
  --quiet`: passed.
- `cargo test dispatcher_event_callback_block_does_not_extend_protocol_writer_tick
  --quiet`: passed.
- `cargo test --lib --quiet`: 601 passed.
- `cargo check --examples --quiet`: passed.
- `cargo test --test fire_test --no-run --quiet`: passed.
- live `cargo test --test fire_test -- --ignored --nocapture`: passed against
  the configured prod server, including `err_emu=10%` initial health and
  `err_emu=50%` high-loss simple-ops gates.

### 2026-05-24 - Phase C3 state callback snapshot queue

Done:

- `Client::run_with_dispatcher_state` now delivers callbacks through the same
  application callback boundary as `run_with_dispatcher`.
- The callback receives `EventDispatcherSnapshot`: an immutable copy of the
  dispatcher read models after state application, not a borrow of the live
  `EventDispatcher`.
- `Orders`, `OrderBooks`, `TradesState`, and `StratsState` now implement
  `Clone` so fixed read-model snapshots can be built without sharing mutable
  protocol state across threads.
- Full decoded `StrategySnapshot` storage inside `StratsState` is
  copy-on-write (`Arc<StrategySnapshot>`). `EventDispatcherSnapshot` clones the
  strategy index cheaply and only deep-clones full strategies when the
  application explicitly calls `strategy_snapshot_vec()`.

Reason:

- Delphi sends UI/application work through `TThread.Queue`. The Rust protocol
  owner must not wait for user callbacks. A snapshot preserves the machine
  effect of protocol state mutation while moving only notification work across
  the app boundary. Snapshot creation itself remains protocol-loop work and is
  covered by protocol tick metrics; later zero-alloc/read-model refactors should
  reduce that cost without changing the boundary.

Checks:

- `cargo test event_loop_fairness_tests::dispatcher_state_callback_block_does_not_extend_protocol_writer_tick -- --nocapture`: passed.
- `cargo test state::strats::tests::clone_shares_full_strategy_snapshots_until_mutation --lib -- --nocapture`: passed.
- `cargo fmt --check`: passed.
- `cargo test --lib --quiet`: 604 passed.
- `cargo check --examples --quiet`: passed.
- `cargo test --test fire_test --no-run --quiet`: passed.
- live `cargo test --test fire_test -- --ignored --nocapture`: passed against
  the configured prod server, including `err_emu=10%` initial health, full
  candles snapshot, `err_emu=50%` simple-ops gate, and reconnect/restore checks.

### 2026-05-24 - Phase C4 coarse protocol CPU red-flag metrics

Done:

- Added CPU-ish protocol timing counters that exclude the fixed writer
  `wait_5ms()` sleep:
  - reader protocol avg/max and `>100us/>1ms/>5ms` counters;
  - writer protocol CPU avg/max and `>100us/>1ms/>5ms` counters;
  - app enqueue/snapshot avg/max and `>100us/>1ms/>5ms` counters.
- FireTest now prints protocol CPU summaries at the initial health gate, after
  candles, after the high-loss simple-ops gate, and at final pass/fail.
- State-callback delivery now sends `Arc<EventDispatcherSnapshot>` through the
  app queue, so a batch does not deep-clone the same snapshot once per event.

Release FireTest measurement after the Arc batch fix:

- A final: reader avg/max `25us/32469us`, writer CPU avg/max
  `177us/663077us`, app enqueue avg/max `922us/2437us`.
- B final: reader avg/max `30us/32267us`, writer CPU avg/max
  `157us/69492us`, app enqueue avg/max `994us/3760us`.
- App enqueue improved versus the pre-Arc measurement (`max 50ms` observed
  before the fix) and no longer crossed `>5ms` in the final run.

Interpretation:

- Steady-state averages are in the expected microsecond range, but the max
  writer CPU samples are not Delphi-like. A protocol/domain block reaching
  tens/hundreds of milliseconds is a real red flag.
- The current coarse counters prove the problem class but not the exact command.
  While the runtime rewrite is still in progress, fix only obvious Rust-only
  overhead found on the way. The mandatory full attribution/optimization pass is
  Phase Z after protocol/runtime parity.

Checks:

- `cargo fmt --check`: passed.
- `cargo test --lib --quiet`: 604 passed.
- `cargo check --examples --quiet`: passed.
- `cargo test --test fire_test --no-run --quiet`: passed.
- `cargo test --release --test fire_test -- --ignored --nocapture`: passed and
  produced the CPU numbers above.

### 2026-05-24 - Phase D0 remove scoped writer thread

Done:

- `run`, `run_with_dispatcher`, `run_with_dispatcher_state`, and internal
  queued runs no longer spawn an extra scoped writer thread.
- The caller thread that entered `run*` now owns `ProtocolCore::run` for that
  call. User callbacks and lifecycle callbacks still run through their app
  queues, so blocking UI/user work does not enter protocol ACK/retry/send
  progress.
- The UDP reader thread still exists after this step. This is not the final
  single-owner runtime; it removes one Rust-only ceremony layer before moving
  recv/process into the same owner.

Reason:

- Public `run*` already blocks the caller for the requested duration. Spawning a
  second writer thread inside that blocking call added no Delphi machine effect;
  it only added a Rust-only thread boundary. Removing it makes the live runtime
  closer to the planned single-owner `ProtocolCore + AppQueue` shape.

Checks:

- `cargo fmt --check`: passed.
- `cargo test --lib --quiet`: 604 passed.
- `cargo check --examples --quiet`: passed.
- `cargo test --test fire_test --no-run --quiet`: passed.
- `cargo test --release --test fire_test -- --ignored --nocapture`: passed.

### 2026-05-24 - Phase D1 production UDP poller reader

Done:

- Promoted `polling` from dev-only proof dependency to runtime dependency.
- Current reader thread now uses `Poller + nonblocking UDP` and drains
  `recv_from` until `WouldBlock` after a readable event.
- The old blocking `recv_from` path remains only as a fallback if the production
  poller cannot be created/registered.
- No protocol ordering changed: SlicedACK, Ping/PMTU replies, decrypt/decompress
  and decoded delivery still run in the same reader receive branch as before.

Reason:

- This puts the live receive side on the exact waiting primitive required by
  the planned single-owner runtime, while keeping the reader thread boundary for
  this incremental step.
- The next Phase D step can move the already-proven poll/drain loop into
  `ProtocolCore` instead of changing socket waiting and state ownership at the
  same time.

Checks:

- `cargo fmt --check`: passed.
- `cargo test --lib --quiet`: 604 passed.
- `cargo check --examples --quiet`: passed.
- `cargo test --test udp_polling --quiet`: 1 passed.
- `cargo test --test fire_test --no-run --quiet`: passed.
- `cargo test --release --test fire_test -- --ignored --nocapture`: passed.

### 2026-05-24 - Phase D2 single-owner UDP receive

Done:

- Removed the production UDP reader thread. `ProtocolCore::run` now owns the
  UDP recv drain, packet unpack, ErrEmu accounting, immediate service replies
  (`Ping`, `SizeAck`, `ProbeMTUAck`, `SlicedACK`, `ImFriend`), decoded payload
  enqueue, send/maintenance, and app queue delivery in one caller thread.
- Kept the existing `ReaderDecodedMsg` delivery queue as the next temporary
  bridge: this step removes the thread boundary first, but does not yet inline
  every domain callback/event delivery.
- The live socket is registered in a `polling::Poller` once per socket/session;
  `wait_5ms` now waits for UDP readability or the Delphi 5ms timeout. If poller
  registration fails, the socket is still nonblocking and the loop probes recv
  once per 5ms tick.
- Deleted the old production `ReaderRuntime` / `spawn_reader` path instead of
  keeping a legacy alternative.
- Converted reader service tests to pump `ProtocolCore::recv_drain_phase()`
  directly, so tests cover the new path.

Reason:

- This is the main NextIdeas3 move: there is one protocol owner and no
  reader-thread queue/cap/lock boundary between UDP recv and protocol-owned
  side effects. The machine effect stays Delphi-shaped: each received datagram
  is processed independently; service replies and SlicedACKs are still sent
  immediately from the receive path; public/user callbacks remain outside the
  protocol loop.

Checks:

- First unit gate after the move: `cargo test --lib --quiet` OK: `605 passed`.
- Final gate for this working point, after the all-trades reconnect gate and
  SynLZ follow-up fixes below: `cargo test --lib --quiet` OK: `607 passed`,
  `cargo check --examples --quiet` OK, `cargo test --test fire_test --no-run
  --quiet` OK, live release FireTest on prod OK: `FIRETEST_PASS`,
  `ParseFailed=0`, `FAIL=0`.

### 2026-05-24 - Phase D3 per-datagram decoded delivery

Done:

- `ProtocolCore::recv_drain_phase` now completes decoded delivery after each
  accepted UDP datagram. It no longer waits until the whole poll-readable batch
  is drained.
- Service/receive tests were updated from "pop decoded record later" to the
  real machine effect: ACK/reply/state/callback are already applied by the time
  one datagram step returns, and `pending_reader_decoded` is empty.
- Production receive no longer pushes `ReaderDecodedMsg`: `DataReadInt` calls
  `client_new_data` directly for decoded data, ping payloads, and handshake
  control state. `drain_reader_decoded` still exists only for directly injected
  unit/internal cases while the bridge scaffolding is being removed.

Reason:

- Delphi `UDPRead -> DataReadInt -> OnNewData` completes the current datagram
  before the reader consumes the next UDP datagram. This step removes the
  Rust-only poll-batch boundary while keeping user callbacks outside protocol
  blocking paths.

Checks:

- `cargo fmt --check`: passed.
- `cargo test --lib --quiet`: 607 passed.
- `cargo check --examples --quiet`: passed.
- `cargo test --test fire_test --no-run --quiet`: passed.
- `MOONPROTO_FIRETEST_PROFILE=quick cargo test --release --test fire_test -- --ignored --nocapture`
  passed on prod in `23.96s`: `FIRETEST_QUICK_PASS`, `ParseFailed=0`.

Status of the two-thread-boilerplate problem:

- Root cause: the old Rust two-thread shape forced the receive side to avoid
  writing directly into `Client`/domain state. That created Rust-only
  pack -> queue -> unpack boilerplate around `ReaderDecodedMsg`,
  `pending_reader_decoded`, epoch gates and bridge tests.
- Current solution: move protocol ownership into one `ProtocolCore` owner.
  Receive can process a datagram, mutate protocol/domain state, send immediate
  replies and drain decoded delivery before the next datagram, while user
  callbacks stay outside the protocol loop through app queues.
- Progress now: production `ReaderRuntime`/`spawn_reader` is gone, UDP receive
  and writer/send maintenance share one owner, decoded delivery is direct and
  per-datagram. Production receive no longer uses or contains the
  `ReaderDecodedMsg` bridge; it is test-only scaffolding now. The remaining
  work is to remove or shrink the tests/helpers that exist only because of the
  old bridge.
- Expected result: roughly hundreds of lines of bridge boilerplate disappear
  (target order: ~800 lines once the bridge/test scaffolding is gone). Code
  should look more like Delphi: one owner, direct access to protocol-owned
  structures, and simpler block-by-block machine-effect comparison.

### 2026-05-24 - Phase D4 test-only decoded bridge

Done:

- `pending_reader_decoded`, `ReaderDecodedMsg`, `reader_decode_data_packets`,
  `drain_reader_decoded`, and `process_reader_decoded` are now compiled only
  under `cfg(test)`.
- Production still drains deferred order removals after receive/app delivery via
  a separate `drain_deferred_order_removals_due` helper, so removing the empty
  bridge does not drop that side effect.
- Public production metrics no longer expose the removed bridge. Dispatcher
  public event queue length is still reported separately.

Reason:

- After D3 no production receive block pushed into `pending_reader_decoded`.
  Keeping the field in release builds only preserved Rust-old-path state that
  Delphi never had. Moving it behind `cfg(test)` shrinks the runtime state
  without changing protocol machine effect.

Checks:

- `cargo fmt --check`: passed.
- `cargo check --examples --quiet`: passed.
- `cargo test --lib --quiet`: 607 passed.
- `MOONPROTO_FIRETEST_PROFILE=quick cargo test --release --test fire_test -- --ignored --nocapture`
  passed on prod in `21.08s`: `FIRETEST_QUICK_PASS`, `ParseFailed=0`.

### 2026-05-24 - Phase D5 remove `WriterRuntime` alias

Done:

- Removed the `WriterRuntime` compatibility type alias.
- Unit helpers/tests now instantiate `ProtocolCore` directly when they exercise
  send/orchestrator blocks.

Reason:

- After D2-D4 there is no separate writer runtime. Keeping the old type name in
  tests made the architecture look like it still had a hidden old path.

Checks:

- `cargo fmt --check`: passed.
- `cargo check --examples --quiet`: passed.
- `cargo test --lib --quiet`: 607 passed.
- Full prod FireTest after D3-D5:
  `cargo test --release --test fire_test -- --ignored --nocapture` passed in
  `185.8s` with `MOONPROTO_FIRETEST_PROFILE` unset (`full` profile).

### 2026-05-24 - Phase D6 public metrics API cleanup

Done:

- Removed `ProtocolMetricsSnapshot.app_queue_len` and `app_queue_max_len`.
- FireTest CPU summary no longer prints `appq/appq_max`.
- API docs now describe only production metrics: recv/protocol timing,
  writer/send timing, app enqueue timing, and dispatcher public event queue
  length.

Reason:

- D4 removed the receive-decoded bridge from production. Keeping bridge length
  fields in public API made a test-only scaffold look like live runtime state.

Checks:

- `cargo fmt --check`: passed.
- `cargo test --lib --quiet`: 607 passed.
- `cargo check --examples --quiet`: passed.
- `cargo test --test fire_test --no-run --quiet`: passed.
- `MOONPROTO_FIRETEST_PROFILE=quick cargo test --release --test fire_test -- --ignored --nocapture`
  passed on prod in `26.42s`: `FIRETEST_QUICK_PASS`, `ParseFailed=0`.

### 2026-05-24 - Phase D7 CPU metrics split

Done:

- Added `active_dispatch_*` metrics to split typed dispatcher/domain-state work
  from `app_enqueue_*` public event enqueue work.
- FireTest CPU summary now prints `active_dispatch(...)` separately.
- API docs describe the new split.

Reason:

- Quick/full FireTest showed `reader_protocol` samples above 1ms. Without this
  split, the number did not say whether the cost was protocol/state apply or
  public callback/event enqueue.

Checks:

- `cargo fmt --check`: passed.
- `cargo test --lib --quiet`: 607 passed.
- `cargo check --examples --quiet`: passed.
- `cargo test --test fire_test --no-run --quiet`: passed.
- `MOONPROTO_FIRETEST_PROFILE=quick cargo test --release --test fire_test -- --ignored --nocapture`
  passed on prod in `22.67s`: `FIRETEST_QUICK_PASS`, `ParseFailed=0`.

Observed quick CPU after the split:

- `reader avg/max = 580us / 32124us`, `>1ms = 61`.
- `active_dispatch avg/max = 283us / 21454us`, `>1ms = 2`, `>5ms = 1`.
- `app_enqueue avg/max = 885us / 2514us`, `>1ms = 59`, `>5ms = 0`.

Conclusion for Phase Z:

- The biggest single reader spike is mostly active/domain dispatch, not user
  callback execution. App enqueue also has many >1ms samples, but its max is
  much smaller. Both remain optimization targets; this commit only makes the
  red flag measurable.

### 2026-05-24 - Phase D8 removed test pending decoded queue

Done:

- Removed `Client.pending_reader_decoded` and `ProtocolCore::drain_reader_decoded`.
- Tests that need run-loop delivery now inject a real UDP datagram and exercise
  the production `recv_drain_phase -> process_datagram_inline ->
  client_new_data` path.
- Tests that need isolated receive-effect proof call the `cfg(test)`
  `process_reader_decoded` helper directly, without a queue stored in `Client`.

Reason:

- The production decoded bridge was already gone. Keeping even a test-owned
  pending decoded queue made the code look like an old Rust-only backlog model
  that Delphi never had.

Checks:

- `cargo fmt --check`: passed.
- `cargo test --lib --quiet`: 607 passed.
- `cargo check --examples --quiet`: passed.
- `cargo test --test fire_test --no-run --quiet`: passed.
- `MOONPROTO_FIRETEST_PROFILE=quick cargo test --release --test fire_test -- --ignored --nocapture`
  passed on prod in `22.51s`: `FIRETEST_QUICK_PASS`, `ParseFailed=0`.

Observed quick CPU:

- `reader avg/max = 575us / 32424us`, `>1ms = 71`.
- `active_dispatch avg/max = 258us / 19331us`, `>1ms = 2`, `>5ms = 1`.
- `app_enqueue avg/max = 931us / 2092us`, `>1ms = 69`, `>5ms = 0`.

Remaining at this checkpoint:

- `ReaderDecodedMsg` and `reader_decode_data_packets` still exist under
  `cfg(test)` as direct proof helpers. Next cleanup is to replace them with
  smaller direct helpers around `data_read_inline` / `client_new_data`, if this
  can be done without losing unit proof coverage. Closed by Phase D9 below.

### 2026-05-24 - Phase D9 removed decoded test container

Done:

- Removed `ReaderDecodedMsg`, `reader_decode_data_packets`, and
  `ProtocolCore::process_reader_decoded`.
- Replaced decoded-container tests with direct calls to the real production
  blocks: `data_read_int_inline`, `data_read_inline`, `client_new_data`,
  `apply_reader_ping_update`, and `apply_reader_handshake_update`.
- Removed one obsolete stale queued-decoded epoch test. That queued decoded
  output model no longer exists; stale reader epoch protection for shared
  protocol state remains covered by the inline reader/service tests.

Reason:

- After D8 there was no queue left, but the old decoded message type still made
  tests look like a separate Rust-only delivery layer. This step removes that
  layer completely.

Checks:

- `cargo fmt --check`: passed.
- `cargo test --lib --quiet`: 606 passed.

### 2026-05-24 - Phase D10 removed reader transport mirror

Done:

- Removed `ReaderTransportState`, `reader_transport_state`,
  `reader_transport_seen_seq`, `publish_transport_state_from_client`, and
  `sync_transport_state_from_reader`.
- Receive-side stats, Ping updates, handshake tokens/keys/status, reconnect
  flags, and lifecycle-visible auth state now mutate the single `Client` owner
  directly.
- At this checkpoint, kept the remaining epoch-gated shared protocol pieces:
  `ReaderProtocolState`, `SendLockState`, and `ReaderPingState`. These protect
  MPSlider/decode cipher, SlicedACK/TmpSlider, and ping adaptive-rate fields
  across socket/session replacement.

Reason:

- After the production reader thread was removed, the transport mirror no
  longer represented Delphi. It was a leftover Rust-only shared snapshot around
  fields that Delphi mutates directly.

Checks:

- `cargo fmt --check`: passed.
- `cargo test --lib --quiet`: 606 passed.
- `cargo check --examples --quiet`: passed.
- `cargo test --test fire_test --no-run --quiet`: passed.
- `MOONPROTO_FIRETEST_PROFILE=quick cargo test --release --test fire_test -- --ignored --nocapture`
  passed on prod in `25.54s`: `FIRETEST_QUICK_PASS`, `ParseFailed=0`.

Observed quick CPU:

- `reader avg/max = 661us / 32517us`, `>1ms = 91`.
- `active_dispatch avg/max = 218us / 22643us`, `>1ms = 2`, `>5ms = 1`.
- `app_enqueue avg/max = 1109us / 2322us`, `>1ms = 90`, `>5ms = 0`.

### 2026-05-24 - Phase D11 direct ReaderProtocolState

Done:

- Removed the `Arc<Mutex<_>>` wrapper from `ReaderProtocolState`.
- `MPSlider`, decode cipher, and `DataSizeAck` series now live directly in
  `Client` and are mutated from the inline receive path, matching Delphi's
  direct `DataReadInt` field effects more closely.
- At this checkpoint `active_reader_epoch` checks were still kept inside the
  state helpers. Phase D16 later removed them after proving they only protected
  the already-deleted async reader closure.

Reason:

- After D9/D10 removed the async decoded bridge and transport mirror,
  `ReaderProtocolState` no longer had a real concurrent owner. Keeping a mutex
  around decrypt/slider/SizeAck was Rust-only ceremony and made the code less
  Delphi-like.

Checks:

- `cargo fmt --check`: passed.
- `cargo test --lib --quiet`: 606 passed.
- `cargo check --examples --quiet`: passed.
- `cargo test --test fire_test --no-run --quiet`: passed.
- `MOONPROTO_FIRETEST_PROFILE=quick cargo test --test fire_test -- --ignored --nocapture`
  passed on prod in `23.03s`: `FIRETEST_QUICK_PASS`, `ParseFailed=0`.

Observed quick CPU:

- `reader avg/max = 1008us / 124628us`, `>1ms = 87`, `>5ms = 5`.
- `active_dispatch avg/max = 1052us / 115741us`, `>1ms = 4`, `>5ms = 2`.
- `app_enqueue avg/max = 1210us / 3063us`, `>1ms = 83`, `>5ms = 0`.

Note:

- The CPU spikes are still Phase Z work. This D11 step only removes a stale
  synchronization layer; it does not claim protocol hot-path optimization is
  complete.

### 2026-05-24 - Phase D12 direct RecvdSlider

Done:

- Removed `Arc<Mutex<_>>` from writer-owned `RecvdSlider`.
- Kept the exact Delphi ACK order: incoming Ping writes `TmpSlider` under
  `SendLockState`; writer snapshot copies `TmpSlider` to `RecvdSlider`;
  `ApplyRegularHLAck` consumes `RecvdSlider` and clears `has_new_data`.

Reason:

- After the runtime became single-owner, `RecvdSlider` had no second owner.
  The lock was Rust-only ceremony around writer-phase state.

Checks:

- `cargo fmt --check`: passed.
- `cargo test --lib --quiet`: 606 passed.
- `cargo check --examples --quiet`: passed.
- `cargo test --test fire_test --no-run --quiet`: passed.
- `MOONPROTO_FIRETEST_PROFILE=quick cargo test --test fire_test -- --ignored --nocapture`
  passed on prod in `22.97s`: `FIRETEST_QUICK_PASS`, `ParseFailed=0`.

Observed quick CPU:

- `reader avg/max = 1020us / 126809us`, `>1ms = 60`, `>5ms = 6`.
- `active_dispatch avg/max = 1366us / 115358us`, `>1ms = 4`, `>5ms = 2`.
- `app_enqueue avg/max = 1030us / 2973us`, `>1ms = 58`, `>5ms = 0`.

### 2026-05-24 - Phase D13 direct Ping state

Done:

- Removed `ReaderPingState` and `ReaderPingUpdate`.
- `PingCount`, `RoundTripDelay`, `ActualPMTU`, `RS`, `CanSendRate`, and
  `UsedSlicedLimit` now mutate directly on `Client`.
- Preserved the Delphi Ping order:
  `UDPRead Ping` fields/adaptive rate -> `DataReadInt(MPC_Ping)` writes
  `TmpSlider` -> `ClientNewData(MPC_Ping)` increments `PingCount`, updates time
  deltas, builds/sends Ping response.
- Preserved the Rust fix for lost `Fine`: Ping before `AuthDone` proves peer
  liveness but does not clear `need_connect`.

Reason:

- After the async reader was removed, `ReaderPingState` was a Rust-only mirror
  of fields Delphi stores directly on the client.

Checks:

- `cargo fmt --check`: passed.
- `cargo test --lib --quiet`: 606 passed.
- `cargo check --examples --quiet`: passed.
- `cargo test --test fire_test --no-run --quiet`: passed.
- `MOONPROTO_FIRETEST_PROFILE=quick cargo test --test fire_test -- --ignored --nocapture`
  passed on prod in `24.65s`: `FIRETEST_QUICK_PASS`, `ParseFailed=0`.

Observed quick CPU:

- `reader avg/max = 923us / 112838us`, `>1ms = 90`, `>5ms = 7`.
- `active_dispatch avg/max = 889us / 105196us`, `>1ms = 4`, `>5ms = 2`.
- `app_enqueue avg/max = 986us / 2405us`, `>1ms = 76`, `>5ms = 0`.

### 2026-05-24 - Phase D14 direct pending candles map

Done:

- Removed `Arc<Mutex<_>>` from `pending_candles`.
- `api_request_candles_data_async_registered` now inserts the partial candles
  slot directly into `Client`.
- `request_candles_data` removes the slot directly on timeout/error.
- Chunked candles receive handling now mutates the same direct `HashMap` from
  `dispatch_candles_chunk_inline` / `handle_candles_chunk_in_map`.
- The public async boundary remains the existing `mpsc::Sender<MergedCandles>`;
  this change only removes the Rust-only lock around protocol-owned state.

Reason:

- After the async reader was removed, candles aggregation is single-owner
  protocol state. The mutex no longer represented Delphi behavior or a real
  cross-thread protocol boundary.

Checks:

- `cargo fmt --check`: passed.
- `cargo test --lib --quiet`: 606 passed.
- `cargo check --examples --quiet`: passed.
- `cargo test --test fire_test --no-run --quiet`: passed.
- Full `cargo test --test fire_test -- --ignored --nocapture` passed on prod
  in `186.1s` with `err_emu=10%`; this covers the full candles snapshot gate.

### 2026-05-24 - Phase D15 direct handshake updates

Done:

- Removed `ReaderHandshakeUpdate`, `simple_handshake_update`,
  `fine_handshake_update`, and the old build-then-apply helper.
- `WrongHello`, `WantNewHello`, and `NeedHelloAgain` now mutate `Client`
  directly from the receive block.
- `WhoAreYou` now follows the Delphi machine-effect order more closely:
  clear `waiting_hello` before decode, apply `ServerToken`/`PeerAppToken`,
  increment `ClientToken`, build/pack the ImFriend hello, generate session keys,
  encrypt, then send the same `MPC_ImFriend` payload twice.
- `Fine` now clears `waiting_hello` before decode, like Delphi
  `UDPRead` does before entering `HandleHandShake`.
- Added regression tests for invalid `WhoAreYou`/`Fine`: invalid encrypted
  payload must still clear `waiting_hello`, but must not apply decoded fields or
  mark the client authorized.

Reason:

- The old update object was a leftover reader-thread mirror. During Delphi
  comparison it also exposed a real ordering mismatch: Delphi clears
  `FWaitingHello` before attempting to decode `MPC_WhoAreYou/MPC_Fine`; Rust
  cleared it only after successful decode.

Checks:

- Targeted handshake tests: `who_are_you`, `fine`, `need_hello_again`: passed.
- `cargo fmt --check`: passed.
- `cargo test --lib --quiet`: 608 passed.
- `cargo check --examples --quiet`: passed.
- `cargo test --test fire_test --no-run --quiet`: passed.
- `MOONPROTO_FIRETEST_PROFILE=quick cargo test --test fire_test -- --ignored --nocapture`
  passed on prod in `23.38s`: `FIRETEST_QUICK_PASS`.

### 2026-05-24 - Phase D16 removed stale reader epoch guards

Done:

- Removed `Client.current_reader_epoch`.
- Removed `active_reader_epoch` from `SendLockState` and `ReaderProtocolState`.
- Removed the `*_from_reader` epoch-gated helper layer for SlicedACK, Ping ACK
  bitmap, SizeAck series, decode, and TmpSlider reset.
- Renamed the remaining direct helpers to inline/direct names:
  `push_sliced_ack_payload`, `apply_ping_and_build_response`,
  `dispatch_api_pending_inline`, `dispatch_candles_chunk_inline`.
- Removed the obsolete stale-reader-epoch unit test.

Reason:

- The epoch guard protected against an already-removed async reader closure
  mutating shared protocol state after socket/session replacement. With the
  current single-owner receive loop, every accepted UDP datagram is processed by
  the current `ProtocolCore`; the old guard no longer represented a real
  machine state or Delphi behavior.

Checks:

- `cargo fmt --check`: passed.
- `cargo test --lib --quiet`: 607 passed.
- `cargo check --examples --quiet`: passed.
- `cargo test --test fire_test --no-run --quiet`: passed.
- `MOONPROTO_FIRETEST_PROFILE=quick cargo test --test fire_test -- --ignored --nocapture`
  passed on prod in `22.10s`: `FIRETEST_QUICK_PASS`.

### 2026-05-24 - Phase D17 renamed receive decode state

Done:

- Renamed the remaining production `ReaderProtocolState` object to
  `DataReadState`.
- Renamed the `Client` field from `reader_protocol` to `data_read_state`.
- Renamed stale `reader_decoded_*` test names to `data_read_*`.

Reason:

- Production reader/runtime/decoded queues are gone. Keeping `Reader*` names on
  direct `DataReadInt` state made the current single-owner code look like the old
  two-thread bridge and made Delphi block-by-block review noisier.
- This is a mechanical naming cleanup only: same fields, same reset/cipher/slider
  effects, same tests.

Checks:

- `cargo fmt --check`: passed.
- `cargo test --lib --quiet`: 607 passed.
- `cargo check --examples --quiet`: passed.
- `MOONPROTO_FIRETEST_PROFILE=quick cargo test --test fire_test -- --ignored --nocapture`
  passed on prod in `22.01s`: `FIRETEST_QUICK_PASS`, `ParseFailed=0`.

### 2026-05-24 - FireTest quick profile

Done:

- Added `MOONPROTO_FIRETEST_PROFILE=quick|full`.
- Default remains `full`, preserving the old complete scenario.
- `quick` is one-client, non-mutating, target `<=30s`, with client-side
  `err_emu=10%` enabled before connect.
- `quick` checks `connect_and_init`/AuthDone/InitDone, live Engine API methods
  `BaseCheck`, `AuthCheck`, `GetMarketsList`, `GetMarketsIndexes`,
  `UpdateMarketsList`, `SubscribeAllTrades`, `SubscribeOrderBook`, first useful
  trades/orderbook/MarketPrice state for the configured market, retained
  LastPrice from active `UpdateMarketsList`, retained target futures/spot
  trades through `MarketHistoryWorker`, `ParseFailed=0`, ErrEmu/Sliced
  diagnostics, and protocol CPU summary.
- `quick` intentionally skips the expensive gates: second client, full candles,
  high-loss 50%, order lifecycle, settings/strategy mutation, forced reconnect.
- Verification policy from this point:
  - use `quick` where earlier intermediate work would have triggered full
    FireTest;
  - use `full` only at the most important architecture gates, before major
    handoff/stable-point commits, and when a change touches candles/high-loss/
    reconnect/order lifecycle/mutation behavior directly.

Check:

- `MOONPROTO_FIRETEST_PROFILE=quick cargo test --release --test fire_test -- --ignored --nocapture`
  passed on prod in `21.84s` before direct decoded delivery and in `23.96s`
  after direct decoded delivery.

### 2026-05-24 - Phase C2 lifecycle callback queue

Done:

- `Client::on_lifecycle` notifications are now sent through a run-scoped
  lifecycle app channel when the client loop is active.
- The callback object is moved into the lifecycle app worker for the run and
  restored back into `Client` before the run call returns.
- If a lifecycle event is fired outside a run call, the old direct fallback is
  kept because there is no active app queue.

Reason:

- Delphi `TMoonProtoUDPClient.DoStatusChanged` uses `TThread.Queue`; status UI
  must not block protocol writer progress.

Checks:

- `cargo fmt --check`: passed.
- `cargo test lifecycle_callback_block_does_not_extend_protocol_writer_tick
  --quiet`: passed.
- `cargo test --lib --quiet`: 602 passed.
- `cargo check --examples --quiet`: passed.
- `cargo test --test fire_test --no-run --quiet`: passed.
- live `cargo test --test fire_test -- --ignored --nocapture`: passed against
  the configured prod server, including `err_emu=10%` initial health and
  `err_emu=50%` high-loss simple-ops gates.

### 2026-05-22 - Phase 3 partial

Done:

- `MPC_Sliced` receive state is shared with the reader thread.
- Reader calls the receive slicer and sends `MPC_SlicedACK` directly through UDP.
- Reader-side ACK path is covered by `reader_sends_sliced_ack_without_main_loop_tick`.
- `cargo test --lib`: 362 passed.

Superseded by later phases:

- Full datagram no longer reaches `DataReadInt` through an internal
  `ClientEvent::SlicedComplete`.
- Exact Delphi target was implemented for the transport-owned core:
  `UDPRead -> OnNewSliced -> SendCommand(MPC_SlicedACK) -> if complete DataReadInt`
  inside the reader path.

### 2026-05-22 - Night progress review

Done:

- `ClientEvent::SlicedComplete` was removed.
- Completed incoming sliced datagrams now go through a separate
  `pending_completed_sliced` queue and are drained before/inside/after the
  event-drain loop, not through the generic `ClientEvent::Recv` backlog.
- Added tests proving completed Sliced payloads bypass generic recv backlog and
  `Receiving` is removed only after `DataReadInt` returns.
- Reader-side Sliced cleanup moved toward Delphi packet cadence:
  accepted reader packets call `SlicingReceiver::do_cleanup` before
  command-specific handling.
- `DataReadInt(MPC_Ping)` now writes the ACK bitmap into `TmpSlider`; writer
  copies it via `copy_recvd_data` and applies it through `apply_regular_hl_ack`.
- Writer order moved closer to Delphi `CheckSeningData`: copy SlicedACKs,
  copy ping ACK bitmap, create outgoing Sliced, apply SlicedACK, apply regular
  H ACK, then High sends/retries and Low/Sliced retry phases.
- Former Rust-only caps were removed/covered by tests in several state paths:
  Sliced receive, balances, orders, and SynLZ decompression.
- `cargo fmt` was applied after review.
- `cargo test --lib`: 404 passed.

Superseded by later phases:

- Full incoming sliced datagrams are no longer processed through
  `pending_completed_sliced`; the reader now runs the transport-owned
  `OnNewSliced -> DataReadInt` core.
- `EVENT_DRAIN_BUDGET` and generic non-sliced `ClientEvent::Recv` were removed
  from live/test paths.
- Active dispatcher still calls back through `&mut Client`, so receive/domain
  processing cannot yet be moved fully into a reader-owned path.

### 2026-05-22 - Phase 2 partial: SendCmdInt queues

Done:

- Raw/user/API send paths now append directly to shared Delphi-style
  `SendQueues` (`DataToSend`, `DataToSendH`, `DataToSendL`) instead of the
  app/control FIFO.
- `SendQueues::push_send_cmd_int` matches Delphi `SendCmdInt`: unbounded lists,
  selected-priority queue, UKey dedup only for Sliced/High, remove the first
  older matching item, then append the new item.
- The run loop performs `get_copy_send_list` before `GetCopyAcks` /
  `CopyRecvdData`; user/API/UI commands have already appended directly to
  Delphi-style send queues.
- `ClientSender` raw/trade/UI/strategy/balance/subscription helpers use the
  same direct send queues. Subscription helpers mutate the shared reconnect
  registry immediately, then append the corresponding wire request.
- API docs were updated in-place for the changed queue semantics.
- `cargo test --lib`: 417 passed after removal of old event bridge tests.

Still not done:

- Active/user delivery still depends on the main loop; the registry/send-queue
  path no longer has a separate app/control FIFO.

### 2026-05-22 - Phase 3 partial: reader-side Sliced DataReadInt core

Done:

- `pending_completed_sliced` was removed from the live code path.
- Completed incoming Sliced datagrams now run the shared `DataReadInt`
  decrypt/decompress core inside the reader stack immediately after
  `MPC_SlicedACK` is sent.
- Regular non-service data packets (`Order`/`UI`/`API`/`Balance`/`Trades*`/
  `OrderBook`/unknown data commands) also run the same `DataReadInt`
  decrypt/decompress core in the reader stack and bypass generic recv backlog.
- `MPC_Grouped` is split in the reader stack; recv side effects are applied
  once for the UDP datagram, not once per grouped inner command.
- `MPC_Ping` now follows the Delphi reader path through Ping stats,
  `DataReadInt(MPC_Ping)` ACK-bitmap write to `TmpSlider`,
  `ClientNewData(MPC_Ping)`, and immediate `SendPing` response from the reader
  stack. The response writes `TotalRecvBytes` after counting the current
  accepted UDP packet, matching Delphi `UDPRead -> Inc(TotalRecvBytes) ->
  SendPing`.
- `MPC_SizeTest` now follows the Delphi `UDPRead -> SendSizeAck` block in the
  reader stack: it updates the client-side `DataSizeAck.SeriesNum` analogue,
  builds an `MPC_SizeAck` payload of the requested size, enables DontFragment
  around send, then disables it.
- `MPC_ProbeMTU` now follows the Delphi inline `UDPRead` block in the reader
  stack: it echoes `ProbeID`, `ProbeIndex`, and `ReceivedSize := TestSize`,
  sends `MPC_ProbeMTUAck` with payload size `TestSize`, and wraps the send in
  DontFragment.
- Reader removes `Receiving[DatagramNum]` after that core returns, matching the
  Delphi order around `OnNewSliced -> DataReadInt -> Receiving.Remove` for the
  protocol-owned state.
- `MPC_SlicedACK` now appends to the reader-to-writer ACK list in the reader
  stack and records receive side effects without entering generic recv backlog.
- Partial/incomplete `MPC_Sliced` packets now send immediate `MPC_SlicedACK`,
  keep `Receiving[DatagramNum]` alive, and record receive side effects without
  entering generic recv backlog.
- ErrEmu-dropped packets now mirror Delphi's `AddBytesCount`/`LastOnline` then
  `exit`: reader records receive side effects without sending a generic
  `ClientEvent::Recv` and without delivering payload to user/active code.
- Decoded payloads are put into a separate `pending_reader_decoded` queue only
  for user/active-library delivery; they still bypass generic recv backlog.
- Reader wakeups for `pending_reader_decoded` were removed in a later strict
  parity pass; writer/orchestrator polls the decoded queue directly.
- `MPC_WhoAreYou` now follows the Delphi reader-side handshake block for the
  network effect: reader decrypts the server Hello with `MasterKey`, derives
  session keys, builds `MPC_ImFriend`, sends the same payload twice with the
  agreed no-sleep duplicate deviation, and only queues the resulting state
  update for main-side fields.
- `MPC_Fine` now follows the Delphi reader-side handshake exit: reader validates
  the server Hello with `MasterKey` and queues an AuthDone update without
  entering generic recv backlog. Main-side application of that update keeps the
  Rust library-owned reconnect restore after AuthDone.
- `MPC_WrongHello`, `MPC_WantNewHello`, and `MPC_NeedHelloAgain` now follow the
  Delphi `UDPRead` control-command exits without entering generic recv backlog.
  `NeedHelloAgain` uses the reader receive timestamp for the 700ms throttle and
  `WaitingHelloStart`, instead of the later main-loop processing time.
- `TmpSlider`/`MPSlider`/decode cipher are now in shared reader protocol state,
  so reader-side `DataReadInt` core and main fallback use the same replay/ACK
  state.
- Tests cover immediate reader ACK, reader-side decoded Sliced payload,
  reader-side regular data decode, Grouped side effects, and `Receiving`
  removal before main-loop delivery, plus reader-side Ping ACK core and
  reader-side `SizeTest`/`ProbeMTU` ACKs without a main-loop tick.
- Targeted reader service tests: 4 passed after moving `SlicedACK`, partial
  `Sliced`, `SizeTest`, and `ProbeMTU` off generic recv backlog.
- Targeted ErrEmu reader drop test: passed.
- Targeted reader Ping response test: passed.
- Targeted writer polling test for `pending_reader_decoded`: passed.
- Targeted reader `WhoAreYou -> ImFriend, ImFriend` test: passed.
- Targeted reader `Fine -> AuthDone` test: passed.
- Targeted reader hello-control tests for `WrongHello`, `WantNewHello`, and
  `NeedHelloAgain`: passed.
- `cargo fmt --check`, `cargo check --examples`, `cargo test --lib`: 417 passed.

Still not done:

- `OnNewData`/active-library dispatch is still main-loop delivery, not yet
  Delphi reader-thread delivery, even though the `DataReadInt` core is now
  reader-side for data packets.
- Production accepted UDP packets no longer enqueue generic `ClientEvent::Recv`;
  the test-only fallback was also removed so tests exercise the live
  reader-decoded path.
- `EVENT_DRAIN_BUDGET` was removed; reader decoded delivery does not carry
  user/API/UI send work.

### 2026-05-22 - Phase 6 partial: active actions outbox

Done:

- `EventDispatcher` now has `ActiveDispatchContext` and `ActiveAction`.
- Production `WriterRuntime::client_new_data` snapshots the client
  context, calls `dispatch_into_active_actions`, then applies the returned
  action outbox.
- Active auto-actions are now data, not hidden direct `&mut Client` mutation:
  `RequestOrderBookFull`, `SendStrategySnapshot`, and missing-order
  `RequestOrderStatus`.
- The old public `dispatch_into_active(..., client)` wrapper was removed; tests
  now call the same `ActiveDispatchContext -> dispatch_into_active_actions ->
  apply_active_actions` path as production.
- `cargo fmt --check`, `cargo check --examples`, `cargo test --lib`: 417 passed.

Still not done:

- Active state is still owned by the caller-supplied `EventDispatcher` during
  `run_with_dispatcher`; it is not yet a reader-owned `ActiveCore`.
- User-visible events are still drained by `run_inner`; they are not yet a
  separate public event queue independent of transport progress.

### 2026-05-22 - Phase 7 partial: reader-side Engine API pending dispatch

Done:

- Reader-side decoded `Command::API` payloads now peek `RequestUID` and, only
  when that UID is registered in `ApiPending`, parse `TEngineResponse` and signal
  the waiting receiver immediately from the reader DataReadInt path.
- Main/dispatcher delivery keeps the same payload. If reader already consumed
  the pending receiver, Callback mode does not duplicate it, while Dispatcher
  mode still applies it to `EventDispatcher` for markets/indexes/tags and
  `Event::EngineResponse`.
- Large unregistered Engine API packets are not decompressed in reader just to
  discover that no `ApiPending` waiter exists; the reader does a cheap UID peek
  first.
- Registered `RequestCandlesData` chunks now use the same reader-side direction:
  reader peeks UID/method, consumes chunks only when a pending candle aggregator
  exists, signals `MergedCandles` on the final chunk, and prevents consumed
  chunks from being re-delivered to raw callbacks or `EventDispatcher`.
- `cargo fmt --check`, `cargo check --examples`, `cargo test --lib`: 422 passed.

Still not done:

- Single-threaded callers still need `run_until_response` because writer/send
  progress is still owned by `run_inner`; only the response delivery side moved
  reader-side.
- Active state is still caller-owned and user-visible event delivery is still
  drained by `run_inner`.

### 2026-05-22 - Phase 1 partial: named writer tick block

Done:

- The live main loop no longer spells the writer send order inline. It now calls
  `copy_send_ack_and_check_sening_data`, which performs Delphi's
  `GetCopySendList; GetCopyAcks; FClient.CopyRecvdData; CheckSeningData`
  sequence in one movable block.
- `check_sening_data` preserves the verified Delphi order: Sliced copy-send
  cleanup/create, queued SlicedACK apply, regular H ACK apply, High send/retry,
  first Low flush, Sliced retry, remaining Low flush.
- Added `writer_tick_copies_ack_queues_then_check_sening_data_like_delphi` to
  prove queued SlicedACK and ping `TmpSlider` ACKs do not affect `Sending` /
  `PendingH` until this writer block runs.
- Targeted test: `cargo test writer_tick_copies_ack_queues_then_check_sening_data_like_delphi --lib`
  passed.

Still not done:

- This is only an extraction step. The writer block still executes from
  `run_inner`; ownership has not yet moved to a background writer runtime.

### 2026-05-22 - Phase 1 partial: named reader `OnNewSliced` block

Done:

- Production reader `MPC_Sliced` handling is now isolated as
  `reader_on_new_sliced`.
- The block keeps the verified Delphi machine effect:
  `OnNewSliced -> SendCommand(MPC_SlicedACK) -> if complete DataReadInt ->
  Receiving.Remove -> queue decoded delivery`.
- Removed misleading "old/backwards" wording around the live raw callback path
  and active action outbox. The low-level `Client::run` API remains a real raw
  callback API, not an internal compatibility bridge.
- Targeted tests passed:
  `reader_handles_partial_sliced_without_recv_event_backlog`,
  `reader_decoded_sliced_payload_bypasses_recv_event_backlog`,
  `reader_sends_sliced_ack_without_main_loop_tick`.

Still not done:

- Other reader command branches are still inline in `spawn_reader`; they need the
  same Delphi-named extraction before the reader runtime can be moved cleanly.

### 2026-05-22 - Phase 1 partial: named reader `OnNewSlicedACK` block

Done:

- Production reader `MPC_SlicedACK` handling is now isolated as
  `reader_on_new_sliced_ack`.
- The block keeps Delphi's machine effect: append parsed ACK to the reader ->
  writer ACK list, record receive side-effect, and do not apply ACK in reader.
- Targeted tests passed:
  `reader_handles_sliced_ack_without_recv_event_backlog`,
  `sliced_ack_reader_queues_writer_applies_like_delphi`.

Still not done:

- The helper still feeds the current `incoming_sliced_acks` queue; ownership has
  not yet moved into a standalone writer runtime.

### 2026-05-22 - Phase 1 partial: named reader `MPC_Ping` block

Done:

- Production reader `MPC_Ping` handling is now isolated as
  `reader_on_new_ping`.
- The block keeps Delphi's machine effect: apply Ping receive state and ACK
  bitmap in the reader-side `DataReadInt` core, send the Ping response
  immediately from reader stack, then queue main-side field delivery.
- Targeted tests passed:
  `reader_handles_ping_response_without_main_loop_tick`,
  `ping_ack_does_not_drop_pending_h_until_writer_copy_apply`.

Still not done:

- Main-side application of the queued `ReaderPingUpdate` still mutates
  `Client` fields from `run_inner`; those fields need shared/worker ownership
  before this becomes a standalone reader-owned active core.

### 2026-05-22 - Phase 1 partial: named reader PMTU blocks

Done:

- Production reader `MPC_SizeTest` and `MPC_ProbeMTU` handling is now isolated
  as `reader_on_new_size_test` and `reader_on_new_probe_mtu`.
- Both blocks keep Delphi's machine effect: build the corresponding ACK payload,
  toggle DontFragment around the send, then record receive side-effect without
  entering a recv backlog.
- Targeted tests passed:
  `reader_handles_size_test_without_main_loop_tick`,
  `reader_handles_probe_mtu_without_main_loop_tick`.

Still not done:

- Handshake/control reader branches are still inline in `spawn_reader`; those are
  the next reader extraction target.

### 2026-05-22 - Phase 1 partial: named reader handshake-control block

Done:

- Production reader handling for `MPC_WrongHello`, `MPC_WantNewHello`, and
  `MPC_NeedHelloAgain` is now isolated as `reader_on_handshake_control`.
- The block keeps Delphi's machine effect: accepted packet side-effect plus the
  corresponding handshake state update, without generic recv backlog delivery.
- Targeted tests passed:
  `reader_handles_wrong_hello_without_recv_event_backlog`,
  `reader_handles_want_new_hello_without_recv_event_backlog`,
  `reader_handles_need_hello_again_without_recv_event_backlog`.

Still not done:

- `MPC_WhoAreYou` and `MPC_Fine` still need named reader blocks because they
  include decrypt/key side effects and duplicate `ImFriend` send timing.

### 2026-05-22 - Phase 1 partial: named reader handshake auth blocks

Done:

- Production reader `MPC_WhoAreYou` handling is now isolated as
  `reader_on_who_are_you`.
- Production reader `MPC_Fine` handling is now isolated as `reader_on_fine`.
- `reader_on_who_are_you` keeps Delphi's byte/state effect: decrypt server Hello
  with `MasterKey`, derive session keys, install reader decode cipher, build
  `ImFriend`, send it twice without blocking sleep per DEVIATION #37, then
  queue the handshake state update.
- `reader_on_fine` keeps Delphi's machine effect: validate encrypted server
  Hello with `MasterKey`, then queue AuthDone update without generic recv
  backlog delivery.
- Targeted tests passed:
  `reader_handles_who_are_you_imfriend_without_main_loop_tick`,
  `reader_handles_fine_auth_done_without_recv_event_backlog`.

Still not done:

- Main-side application of handshake updates still owns several protocol fields.
  The next architecture step is moving those fields/state transitions behind
  shared transport ownership so reader and writer runtimes can operate without
  `run_inner` as the protocol motor.

### 2026-05-22 - Phase 1 partial: named reader data/drop blocks

Done:

- ErrEmu-drop handling is now isolated as `reader_on_err_emu_drop`.
- Regular non-service data packet handling is now isolated as
  `reader_on_data_packet`.
- The production reader command match now delegates every protocol branch to a
  named block: Ping, handshake control/auth, PMTU probes, SlicedACK, Sliced,
  ErrEmu-drop, and regular data.
- Targeted tests passed:
  `reader_err_emu_drop_updates_stats_without_recv_event_backlog`,
  `reader_decodes_regular_data_without_recv_event_backlog`.

Still not done:

- These named blocks still live as `Client` helpers called by the reader thread
  closure. The next structural step is packaging the closure state into a
  `ReaderRuntime` owner.

### 2026-05-22 - Phase 1 partial: extracted transport ticks from `run_inner`

Done:

- `run_inner` no longer spells reader-wake wait, writer maintenance, and
  reconnect tail inline.
- Added `wait_for_reader_work_or_default_sleep`,
  `transport_writer_maintenance_tick`, and `transport_reconnect_tail_tick`.
- The order is unchanged for this phase: drain reader delivery, wait/drain wake,
  drain reader delivery again, writer maintenance (`CheckSeningData`, cleanup,
  indexes, refresh, clock-jump), active trades tick, reconnect tail. A later
  strict parity pass removes the wake FIFO and keeps direct polling.
- Targeted tests passed:
  `send_phase_runs_with_ready_send_queue`,
  `post_init_reconnect_restores_domain_without_second_init_and_reopens_stream_gate`.

Still not done:

- These are extraction methods only. The transport writer maintenance still runs
  from `run_inner`, not from a standalone writer thread.

### 2026-05-22 - Phase 1 partial: introduced `ReaderRuntime`

Done:

- `spawn_reader` is now only the reader-thread factory: it clones/captures the
  exact runtime state, creates `ReaderRuntime`, and starts `ReaderRuntime::run`.
- The UDP receive loop, transport unpack, ErrEmu drop branch, Sliced cleanup,
  command dispatch, and decoded-delivery enqueue now live inside `ReaderRuntime`.
- The command bodies are still the same named reader blocks extracted earlier:
  Ping, handshake control/auth, PMTU probes, SlicedACK, Sliced, ErrEmu-drop,
  and regular data. No protocol branch was added or reordered.

Still not done:

- `ReaderRuntime` still uses lower-level `Client` helpers for shared decode,
  packet building, and raw send. The next strict-parity step is to decide which
  of those helpers are pure shared helpers and which must become transport-owned
  runtime methods.
- The writer/orchestrator is still a `run_inner` tick extraction, not a
  dedicated Delphi-style writer runtime/thread.

### 2026-05-22 - Phase 1 partial: moved reader command blocks into `ReaderRuntime`

Done:

- Removed the `Client::reader_on_*` command blocks.
- `ReaderRuntime::handle_command` now calls its own `on_*` methods for Ping,
  handshake control/auth, PMTU probes, SlicedACK, Sliced, ErrEmu-drop, and
  regular data.
- `Client` still keeps pure/shared helpers used by tests and by the runtime:
  data decode, handshake payload build/decode, ACK parsing, reader side-effect
  enqueue, and raw packet send.

Still not done:

- Need split the remaining helper set by ownership: pure protocol helpers can
  stay shared, while socket/stateful transport helpers should move behind
  reader/writer runtime ownership.
- The writer/orchestrator is still not a standalone Delphi-style runtime/thread.

### 2026-05-22 - Phase 1 partial: introduced `WriterRuntime` shell

Done:

- Added `WriterRuntime` as the owner of the former `run_inner` loop body.
- `Client::run_inner` is now a thin wrapper that constructs `WriterRuntime` and
  calls `WriterRuntime::run`.
- The loop order is unchanged: lifecycle transition, ActualSleepTime EMA,
  bind/spawn reader, drain reader delivery, wait, drain again, writer
  maintenance, active trades tick, reconnect tail.

Still not done:

- Writer/orchestrator helper blocks still live as `Client` methods and are
  called through `WriterRuntime.client`.
- This is not yet a separately spawned writer thread; it is the explicit runtime
  for the caller thread that runs the Delphi writer/orchestrator loop.

### 2026-05-22 - Phase 1 partial: moved writer tick blocks into `WriterRuntime`

Done:

- Moved reader wait/sleep, writer maintenance tick, reconnect tail tick,
  copy-send/copy-ack/copy-recvd-data, and `CheckSeningData` ordering into
  `WriterRuntime`.
- The writer tick test now calls `WriterRuntime` directly instead of an old
  `Client` wrapper.
- `Client` still owns low-level mutation helpers (`get_copy_send_list`,
  `apply_copy_acks`, `send_h_item`, `retry_pending_h`, etc.), but the tick
  orchestration order is no longer a `Client` method.

Still not done:

- Need continue moving low-level writer-owned protocol mutation helpers behind
  the writer runtime boundary, while preserving exact Delphi method order.
- This is still the caller-thread writer/orchestrator runtime, not a spawned
  background writer thread.

### 2026-05-22 - Phase 1 partial: moved writer copy/apply helpers

Done:

- Moved `GetCopySendList`, `GetCopyAcks`, `CopyRecvdData`,
  `ApplyRegularHLAck`, queued SlicedACK apply, and UKey cleanup helpers into
  `WriterRuntime`.
- Unit tests that exercise those helpers now instantiate `WriterRuntime`
  directly instead of calling removed `Client` helper methods.
- `CheckSeningData` still keeps the same order: Sliced cleanup/create,
  SlicedACK apply, regular H ACK apply, High cleanup/send, H retry, Low/Sliced
  retry/Low flush.

Still not done:

- The actual send/retry low-level methods (`create_sliced_and_send`,
  `send_h_item`, `retry_pending_h`, `retry_sliced`, low batching/flush) still
  live on `Client`.
- This is still the caller-thread writer/orchestrator runtime, not a spawned
  background writer thread.

### 2026-05-22 - Phase 1 partial: routed send/retry tests through `WriterRuntime`

Done:

- `CheckSeningData` now calls writer send/retry wrapper methods on
  `WriterRuntime`.
- Unit tests for Sliced creation, H send, low batching/flush, and Sliced retry
  now exercise those operations through `WriterRuntime`.

Still not done:

- The wrappers still delegate to `Client` method bodies. The next mechanical
  step is moving those bodies into `WriterRuntime` one by one.
- This is still the caller-thread writer/orchestrator runtime, not a spawned
  background writer thread.

### 2026-05-22 - Phase 1 partial: moved Low/Sliced retry ordering body

Done:

- Moved the body of `send_low_items_around_sliced_retry` into `WriterRuntime`.
- The preserved order is Delphi `CheckSeningData`: first Low item, flush,
  Sliced retry, remaining Low items, final flush.

Still not done:

- `batch_send_direct`, `flush_send_batch`, and `retry_sliced` bodies still
  delegate to `Client`.
- This is still the caller-thread writer/orchestrator runtime, not a spawned
  background writer thread.

### 2026-05-22 - Phase 1 partial: moved H/Low batching writer bodies

Done:

- Moved `send_h_item`, `retry_pending_h`, `batch_send_direct`,
  `do_send_mp_data_wire`, tmp-send append, and `flush_send_batch` bodies into
  `WriterRuntime`.
- Removed those method bodies from `Client`.
- Preserved Delphi order for PendingH retry: clone/resend intent, decrement,
  drop exhausted entries, then resend cloned items through H send.

Still not done:

- `create_sliced_and_send` and `retry_sliced` bodies still delegate to `Client`.
- This is still the caller-thread writer/orchestrator runtime, not a spawned
  background writer thread.

### 2026-05-22 - Phase 1 partial: moved Sliced creation body

Done:

- Moved `create_sliced_and_send` / `CreateSlicedObject` body into
  `WriterRuntime`.
- Removed the old `Client` method body.
- Sliced datagram formation still preserves Delphi order: compression before
  max-size check, optional crypt, datagram number increment, block construction,
  priority insert by block count, `LastChecked` reset.

Still not done:

- `retry_sliced` body still delegates to `Client`.
- This is still the caller-thread writer/orchestrator runtime, not a spawned
  background writer thread.

### 2026-05-22 - Phase 1 partial: moved Sliced retry body

Done:

- Moved `retry_sliced` body into `WriterRuntime`.
- Removed the old `Client` method body.
- `WriterRuntime` now owns the Sliced writer side around `CheckSeningData`:
  Sliced enqueue, ACK apply, per-piece retry timing, `UsedSlicedLimit`, and
  actual `MPC_Sliced` retransmit send.

Still not done:

- This is still the caller-thread writer/orchestrator runtime, not a spawned
  background writer thread.
- Reader-side handshake state effects still need a strict Delphi placement
  check: currently decoded reader work is queued to the writer/runtime boundary
  instead of being proven as the same immediate reader-thread machine effect.

### 2026-05-22 - Phase 1 partial: moved reader delivery drain into writer runtime

Done:

- Moved `drain_reader_decoded`, `process_reader_decoded`, reader recv side
  effects, queued Ping state apply, queued handshake state apply, and
  dispatcher delivery from `Client` into `WriterRuntime`.
- Updated tests to exercise these paths through `WriterRuntime` instead of
  old `Client` helper bodies.
- Kept `Client::apply_active_actions` on `Client`, because it is part of the
  active-library action surface and is also used outside the writer runtime by
  dispatcher tests.

Still not done:

- This is still the caller-thread writer/orchestrator runtime, not a spawned
  background writer thread.
- Reader-side handshake/Ping writer-visible state is still applied from queued
  reader records at the writer boundary; this remains the next strict placement
  check against Delphi `UDPRead`, where the reader thread mutates those fields
  before returning.
- Test-only `handle_handshake` helper paths were removed in the later
  2026-05-22 cleanup block below.

### 2026-05-22 - Phase 1 partial: removed old test UDP command path

Done:

- Removed test-only `Client::handle_udp_command`.
- Removed now-dead test-only `data_read`, `handle_size_test`,
  `handle_probe_mtu`, and socket `set_dont_fragment` helper paths.
- Updated SlicedACK tests to use the real reader-to-writer ACK queue helper and
  handshake-control tests to use queued `ReaderDecodedMsg` through
  `WriterRuntime::process_reader_decoded`.

Still not done:

- This is still the caller-thread writer/orchestrator runtime, not a spawned
  background writer thread.
- Reader-side handshake/Ping writer-visible state placement remains unresolved
  against strict Delphi `UDPRead`.

### 2026-05-22 - Phase 1 partial: removed old test DataReadInt helper

Done:

- Removed test-only `Client::data_read_int` and `Client::decode_data_read_int_payload`.
- API delivery tests now call the current `client_new_data_decoded`
  delivery helper directly.
- The compressed-garbage test uses the shared production decoder
  `decode_data_read_int_payload_shared`, then the same delivery helper.

Still not done:

- This is still the caller-thread writer/orchestrator runtime, not a spawned
  background writer thread.
- Reader-side handshake/Ping writer-visible state placement remains unresolved
  against strict Delphi `UDPRead`.

### 2026-05-22 - Phase 1 partial: removed old test Ping helper

Done:

- Removed test-only `Client::handle_ping`, `handle_ping_at`,
  `handle_ping_with_reader_core`, and `Client::apply_ping_ack_bitmap`.
- Ping tests now use production reader helper
  `reader_build_ping_update_and_response` plus
  `WriterRuntime::process_reader_decoded`.
- Removed now-dead `ReaderPingState::sync_from_main` and the unused
  `DispatchSink::deliver` test helper.

Still not done:

- This is still the caller-thread writer/orchestrator runtime, not a spawned
  background writer thread.
- Reader-side handshake/Ping writer-visible state placement remains unresolved
  against strict Delphi `UDPRead`.

### 2026-05-22 - Phase 1 partial: removed old test Handshake helper

Done:

- Removed test-only `Client::handle_handshake`.
- Reconnect/handshake tests now decode with production
  `decode_handshake_hello`, build `WhoAreYou` updates with
  `build_who_are_you_imfriend`, and apply updates through
  `WriterRuntime::apply_reader_handshake_update`.
- Existing service reader tests still cover actual reader-thread sends such as
  duplicate `MPC_ImFriend`.

Still not done:

- This is still the caller-thread writer/orchestrator runtime, not a spawned
  background writer thread.
- Reader-side handshake/Ping writer-visible state placement remains unresolved
  against strict Delphi `UDPRead`.

### 2026-05-22 - Phase 1 partial: reader-owned transport state mirror

Done:

- Added a shared reader-owned transport state mirror for Delphi fields mutated
  inside `UDPRead`: recv accounting/online status, auth status, reconnect flags,
  handshake tokens/keys, Ping RTT/PMTU/rate fields, and Hello retry timestamps.
- Production reader paths now write that mirror immediately after successful
  packet unpack and inside the Ping/handshake branches. Queued
  `ReaderDecodedMsg` records from the real reader no longer re-apply recv
  side effects; writer/user delivery polls them after the reader already made
  the Delphi `UDPRead` state transition.
- Writer runtime synchronizes the mirror before lifecycle and reconnect writer
  ticks, and writer-side reconnect changes publish back into the mirror so
  reader decisions such as `NeedHelloAgain` see the current writer state.
- Writer runtime now owns the reconnect tail blocks: Hello/HelloAgain send,
  offline retry, reconnect timeout, dead-zone check, and force-disconnect.
- Writer runtime also has named send-command wrappers for its immediate wire
  sends; low-level packet packing remains on `Client` storage.
- `MPC_WantNewHello` now also resets reader-owned protocol pieces immediately
  in the reader path: decode/replay sliders, Ping session flag, incoming Sliced
  receiver, and shared receive byte counter.
- `MPC_WantNewHello` also resets `CryptedMsgCounter`,
  `AttemptedBytes`/`total_sent`, and `RecvdSlider` immediately from the reader
  path, matching the corresponding `TMoonProtoClient.Reset` assignments.
- Tests now assert the reader-owned state before any writer tick for Ping,
  `WhoAreYou`, `Fine`, `WrongHello`, `WantNewHello`, `NeedHelloAgain`, PMTU
  service commands, regular data, SlicedACK, and ErrEmu drop.

Still not done:

- This is still the caller-thread writer/orchestrator runtime, not a spawned
  background writer thread.
- Re-audit the full `TMoonProtoClient.Reset` list after the shared
  `CryptedMsgCounter`/`RecvdSlider` move and close any stale "writer-owned
  reset" wording that no longer applies.

### 2026-05-22 - Phase 1 partial: narrowed Rust reset to Delphi Reset

Done:

- Removed Rust-only cleanup from `Client::full_reset`: it no longer clears
  outgoing `Sending`, pending H commands, API waiters, or candle aggregators.
  Delphi `TMoonProtoClient.Reset` does not clear those structures.
- Added a reset parity test proving `Sending`, pending H, and API waiter slots
  survive reset while Delphi-reset fields such as crypt counter, attempted
  bytes, total recv, `RS`, `UsedSlicedLimit`, `RecvdSlider`, `LastOnline`, and
  `LastSentHello` are reset.

Still not done:

- This is still the caller-thread writer/orchestrator runtime, not a spawned
  background writer thread.

### 2026-05-22 - Phase 1 partial: completed reader-side Reset field placement

Done:

- Re-checked `TMoonProtoClient.Reset` against Rust state:
  - `Receiving.Clear`, `LastRecvdTS`, `LastCleanedReceived` -> shared
    `SlicingReceiver::new()` in reader and `full_reset`.
  - `CryptedMsgCounter := 0` -> shared atomic reset in reader and `full_reset`.
  - `AttemptedBytes := 0` -> shared `total_sent` reset in reader and
    `full_reset`.
  - `TotalRecvBytes := 0`, `RS := 1.0`, `UsedSlicedLimit := false`,
    `LastSentHello := 0`, `LastOnline := 0` -> reader transport mirror plus
    `full_reset`.
  - `MPSlider.Init` -> shared `ReaderProtocolState::reset()`.
  - `TmpSlider.Init` -> `SendLockState::reset_tmp_slider()`.
  - `RecvdSlider.Init` -> shared `Arc<Mutex<Slider>>` reset in reader and
    `full_reset`.
- `HSendAttempts`, `HRecvCount`, `PrevSentDown`, `PrevRemoteRecvDown`, and
  `LastRDownUpdateMS` have no Rust state equivalent in the current code.
- `MPC_WantNewHello` test now proves reader-side reset of crypt counter,
  attempted bytes, and `RecvdSlider` before the writer processes the queued
  update.

Still not done:

- This is still the caller-thread writer/orchestrator runtime, not a spawned
  background writer thread.

### 2026-05-22 - Phase 1 partial: run_inner uses a dedicated writer thread

Done:

- `Client::run_inner` now executes `WriterRuntime::run` inside a scoped writer
  thread instead of running the writer/orchestrator loop directly on the caller
  stack.
- Added a unit test proving `run(...)` delivers its decoded callback from a
  thread different from the caller thread, so the production run path now has
  the Delphi-shaped reader thread plus writer/Execute thread split.

Still not done:

- The writer thread is scoped to each `run_*` call and joined before the call
  returns. It is not yet a persistent worker owned by a long-lived public handle.
- Public callbacks/events are still executed by the writer runtime; they are
  not yet fully separated into a public event consumer queue.

### 2026-05-22 - Phase 1 partial: removed reader wake FIFO

Done:

- Removed the Rust-only `ClientEvent::Wake` channel and its coalescing flag.
- `ReaderRuntime` now only mutates reader-owned/shared protocol state and
  appends `ReaderDecodedMsg` records to `pending_reader_decoded`.
- `WriterRuntime` follows the Delphi-shaped poll/sleep tick: drain decoded
  delivery, sleep `DEFAULT_SLEEP_MS` only when the outgoing send queues are
  empty, drain decoded delivery again, then run writer maintenance and reconnect
  tail.
- User/API sends still append directly to unbounded Delphi-style send queues;
  they do not compete with reader decoded delivery.
- Tests now prove that writer delivery polls `pending_reader_decoded` without a
  wake FIFO and that app sends are not blocked by pending reader delivery.

Still not done:

- `pending_reader_decoded` is still a Rust bridge for user/active-library
  delivery. The next strict parity step is to decide, block-by-block, which
  `OnNewData`/active-library handlers must move to reader-thread execution and
  which Delphi handlers intentionally queue work elsewhere.

### 2026-05-22 - Phase 1 partial: named reader DataRead/DataReadInt blocks

Done:

- Production `ReaderRuntime` now owns `data_read` and `data_read_int` blocks
  corresponding to Delphi `TMoonProtoBaseNet.DataRead` and `DataReadInt`.
- Regular data packets call `ReaderRuntime::data_read`.
- `MPC_Grouped` is split inside `data_read`, with receive side effects attached
  only to the first emitted sub-packet, matching the previous machine effect.
- Completed incoming Sliced datagrams call `data_read_int`, then remove
  `Receiving[DatagramNum]`, preserving Delphi order at the named block level.

Still not done:

- `data_read_int` still queues `ReaderDecodedMsg` for public/active delivery
  instead of calling the full Delphi `OnNewData` body inline. That is the next
  block-by-block parity decision.

### 2026-05-22 - Phase 1 partial: named reader SendCommand block

Done:

- Reader-side immediate UDP replies now go through
  `ReaderRuntime::send_command`.
- `SizeAck`, `ProbeMTUAck`, `SlicedACK`, duplicate `ImFriend`, and Ping response
  all use that named reader block.
- The lower-level packet pack/send helper remains shared, but production reader
  method order now reads like Delphi: command branch -> `SendCommand(...)` ->
  next local side effect.

Still not done:

- Writer-side send helpers still need the same naming/ownership cleanup around
  the remaining `Client` methods called by `WriterRuntime`.

### 2026-05-22 - Phase 1 partial: moved reconnect tail into WriterRuntime

Done:

- Moved writer-owned reconnect tail blocks from `Client` into `WriterRuntime`:
  `send_hello`, `build_hello_again_packet`, `send_hello_again`,
  `check_hello_send`, `check_offline_reconnect`, `check_reconnect_timeout`,
  `check_dead_zone`, and `do_force_disconnect`.
- `transport_reconnect_tail_tick` now calls same-runtime methods instead of
  bouncing through `Client` helpers.
- Existing reconnect timing tests now exercise the writer runtime methods
  directly.

Still not done:

- Low-level packet send and shared storage still live on `Client`; the next
  writer parity passes should continue moving only protocol-owned ordering
  blocks, without changing public API shape yet.

### 2026-05-22 - Phase 1 partial: named writer SendCommand wrappers

Done:

- Writer-side direct wire sends now pass through
  `WriterRuntime::send_command` / `send_command_raw`.
- Hello, HelloAgain, LogOff, Sliced retry pieces, Grouped flushes, single-item
  flushes, and direct overflow sends now use writer-owned wrappers before
  reaching the shared packet pack/send helper.

Still not done:

- The actual socket, send buffer, byte accounting, and log throttling storage
  still live on `Client`. Moving those requires the larger `ClientShared` split.

### 2026-05-22 - Live Sliced ACK diagnostic under ErrEmu

Done:

- Ran a temporary diagnostic experiment behind `diagnostic-trace`: duplicate
  immediate partial `MPC_SlicedACK` sends and repeated last partial ACK sends
  for incomplete incoming datagrams. This alter-wire path remains feature-gated
  and off by default; production wire behavior stays Delphi-shaped.
- Live `request_candles_data` without client loss still receives the full
  snapshot. With client-side `err_emu=1%`, the request still times out even
  when the diagnostic repeated ACK is enabled.
- Trace proves repeated ACKs are actually sent for remaining holes such as
  `d=16 missing=240`, `d=17 missing=161,218`, and `d=19 missing=248`, but the
  live server does not resend those blocks before timeout.

Still not done:

- This does not justify a Rust protocol deviation. The remaining blocker needs
  server-side evidence: whether the live server receives those ACKs, whether its
  ACK queue/backlog drops the newest cumulative ACKs, or whether
  `CheckSeningData`/max-retry plus per-client `ClientLimit` drops the outgoing
  Sliced datagram before reaching the tail blocks. The budget itself is
  Delphi-identical in Rust: start `2MiB/s` per client, `8/15/22` full-size
  slices per tick at `5/10/15ms`, adaptive rate only after `UsedSlicedLimit` and
  ping feedback. This is a design red flag for the protocol/server algorithm,
  not a Rust-only deviation.

### 2026-05-23 - Superseded: SlicedACK progress resets remaining retry clocks

Superseded:

- This entry documented an intermediate Delphi/Rust experiment where
  ACK-progress reset clocks of remaining unACKed slices to `0`.
- The current Delphi diff later removed that clock reset. The current target is:
  ACK-progress resets `FRetryCount`, removes/ignores ACKed pieces, and preserves
  remaining pieces' `LastChecked`.
- Rust was updated to the current target in
  "current Delphi TradesStream/Sliced retry fixes ported" below.

### 2026-05-22 - Phase 1 partial: moved writer periodic helpers into WriterRuntime

Done:

- Moved the remaining writer tick helper bodies from `Client` into
  `WriterRuntime`: `tick_periodic_refresh`, `tick_periodic_refresh_at`,
  `check_indexes_fetch_timeout`, and `periodic_trades_tick`.
- `transport_writer_maintenance_tick` now calls these same-runtime methods.
- The method bodies were moved without changing the queue/send side effects:
  markets-index timeout retry, periodic market/tag refresh, and
  dispatcher-only trades tick keep the same state transitions and
  packet enqueue points.
- Unit tests for index timeout and periodic refresh now instantiate
  `WriterRuntime` directly, so they verify the writer/orchestrator owner rather
  than a `Client` shortcut.

Still not done:

- The actual send queue storage, socket send helper, and active-library
  dispatcher state still live on `Client`.
- `pending_reader_decoded` is still the Rust bridge between reader
  `DataReadInt` core and user/active delivery.

### 2026-05-22 - Phase 1 partial: named ClientNewData/ProcessApiCommand blocks

Done:

- Renamed the writer-side decoded delivery block to
  `WriterRuntime::client_new_data`, matching Delphi
  `TMoonProtoNetClient.ClientNewData` at the current architecture boundary.
- Renamed the shared decoded payload helper to `Client::client_new_data_decoded`.
- Split API response handling into `Client::process_api_command_decoded`,
  matching Delphi `TMoonProtoNetClient.ProcessApiCommand` as a named block.
- Reworded the raw callback comments; `Client::run` is the raw callback API,
  not a compatibility route.

Still not done:

- `client_new_data` still runs in the writer/orchestrator after
  `pending_reader_decoded`, not inline in the reader thread as Delphi
  `DataReadInt -> OnNewData`.
- Order/Strat/Balance/Trades/OrderBook/UI command bodies still need the same
  block-by-block parity split against Delphi `ClientNewData`.

### 2026-05-22 - Phase 1 partial: InitDone domain gate at ClientNewData boundary

Done:

- `WriterRuntime::client_new_data` now applies the Delphi `InitDone` /
  `domain_ready` gate before either raw callback delivery or typed dispatcher
  delivery.
- Before `domain_ready`, `Order`, `Strat`, `Balance`, `TradesStream`,
  `TradesResendResponse`, `OrderBook`, and `UI` packets are dropped. `API` and
  transport service packets are not gated, because Init depends on Engine API.
- `TradesStream` and `TradesResendResponse` now also require explicit
  all-trades subscription intent in the registry. This is the accepted
  author-decision deviation recorded in the root `DEVIATION.md`: unlike Delphi,
  the Rust public library may run without all-trades.

Still not done:

- `client_new_data` still runs in the writer/orchestrator after
  `pending_reader_decoded`, not inline in the reader thread as Delphi
  `DataReadInt -> OnNewData`.
- The per-command bodies under `ClientNewData` still need exact
  block-by-block split and reverse-equivalence checks.

### 2026-05-22 - Phase 1 partial: named dispatcher ClientNewData branches

Done:

- Split `EventDispatcher::dispatch_into` into named `client_new_data_*`
  branches for `Order`, `OrderBook`, `TradesStream`,
  `TradesResendResponse`, `Balance`, `Strat`, `UI`, `API`, and `LogMsg`.
- Extracted Rust equivalents of Delphi methods:
  `process_command_order`, `process_strat_command`, and
  `process_api_command`.
- This pass is behavior-preserving: it only names the current active-library
  parser/apply blocks so the next strict-porting passes can compare each block
  against `MoonProtoClient.pas → ClientNewData`,
  `ProcessCommandOrder`, `ProcessStratCommand`, and `ProcessApiCommand`
  directly.

Still not done:

- `client_new_data_*` blocks still run from the writer/orchestrator through
  `pending_reader_decoded`, not from the reader stack like Delphi
  `DataReadInt -> OnNewData`.
- The bodies now have stable names, but exact reverse-equivalence is still open
  per block: first priority is `Order` / `ProcessCommandOrder`, then `Strat`,
  `Balance`, `Trades`, `OrderBook`, `UI`, and API market/candles handling.

### 2026-05-22 - Phase 1 partial: Order TAllStatuses calls ProcessCommandOrder

Done:

- Fixed the first `Order` reverse-equivalence mismatch found after naming the
  blocks. Rust no longer applies `TAllStatuses` as a hidden batch inside
  `Orders::apply`.
- `client_new_data_order` now matches Delphi order for `TAllStatuses`: begin
  snapshot / increment snapshot flag, call `process_command_order` for each
  contained `TOrderStatus`, then emit the snapshot marker that drives
  `CleanupMissingWorkers`-equivalent active actions.
- The missing-order active action is now named `cleanup_missing_workers`, the
  Rust counterpart of Delphi `TMoonProtoNetClient.CleanupMissingWorkers`.
- API docs now state that a snapshot can emit per-order events before the final
  `OrderEvent::Snapshot`.

Still not done:

- `process_command_order` still delegates most worker-state semantics to
  `Orders::apply`; its internals need a separate reverse-equivalence pass
  against Delphi `ProcessCommandOrder` line by line.

### 2026-05-22 - Phase 1 partial: ProcessCommandOrder FromCache create guard

Done:

- Fixed a `ProcessCommandOrder` parity bug: Delphi creates a worker for unknown
  `TOrderStatus` only when `FromCache=false`; `FromCache=true` without an
  existing worker is freed/dropped.
- Rust `Orders::apply(TradeCommand::OrderStatus)` now ignores unknown
  `from_cache=true` statuses instead of creating a new active order entry.
- Added a unit test proving the Delphi guard.

Still not done:

- Remaining `ProcessCommandOrder` branches still need line-by-line reverse
  equivalence checks.

### 2026-05-22 - Phase 1 partial: ProcessCommandOrder SetImmune receive guard

Done:

- Fixed another `ProcessCommandOrder` parity bug: `TSetImmuneCommand` is
  client-to-server UI/order action in Delphi and is not applied by the Delphi
  client receive path.
- Rust `Orders::apply(TradeCommand::SetImmune)` now returns
  `NotApplicable` / `Ignored` and does not mutate `immune_for_clicks`.
- API docs state that incoming `SetImmune` is ignored by receive state. Outgoing
  `Client::set_immune` is handled separately as Delphi `SetImmuneClicks`:
  mutate local active orders first, then send the command.
- Added a unit test proving the Delphi receive-path guard.

Still not done:

- Continue side-effect parity for `BOrderWorker` UI/lifecycle behavior outside
  the already-covered `ProcessCommandOrder` queue/removal timing.

### 2026-05-22 - Phase 1 partial: deferred terminal order removal

Done:

- Fixed `ProcessCommandOrder` lifetime parity for terminal statuses and
  `TOrderNotFound`.
- Rust no longer removes an order entry synchronously inside `Orders::apply`.
  Terminal statuses mark the read-model terminal marker; `TOrderNotFound` marks
  `cancel_request` / `server_forced_remove`; both keep the entry addressable for
  the rest of the receive batch, then remove it through deferred flush.
- `EventDispatcher::drain_deferred_order_removals` emits the delayed
  `OrderEvent::Removed` after the reader-decoded batch, matching Delphi's
  "queue command into worker now, remove from WCache later" machine effect.
- Added tests proving `TOrderTracePoint` after terminal status is still applied
  before deferred removal.
- Removed Rust-only `max_trace_points` cap after the test exposed
  `EventDispatcher::default()` had effectively capped stored trace points at
  zero. Delphi `ApplyServerTrace` has no equivalent fixed cap in this block.
- Fixed `TOrderStatusUpdate` body parity: Rust now applies `UpdateData` only
  for `BuySet` / `SellSet`, matching Delphi `HandleServerCommand`; terminal
  statuses update status/sell reason/removal marker without overwriting compact
  order fields.
- Fixed `TOrderReplaceResponse.QuantityBase`: Rust now updates target
  `quantity_base` only when the response value is positive, matching Delphi's
  `if cmd.QuantityBase > 0 then ...`.
- Added Rust read-model equivalents of Delphi `pBuyOrder.Price` /
  `pSellOrder.Price` as `Order::buy_price` / `Order::sell_price`. These are
  maintained from `TOrderStatus` via Delphi's `FLast*ActualPrice` logic and
  from `TOrderReplaceResponse.Price`; they are separate from
  `TOrderCompact.ActualPrice`.

Still not done:

- Continue line-by-line reverse-equivalence for the remaining
  `ProcessCommandOrder` body: accepted/dropped class coverage still needs final
  sweep.

### 2026-05-22 - Phase 1 partial: TurnPanicSell receive guard

Done:

- Fixed a `ProcessCommandOrder` parity bug: `TTurnPanicSellCommand` is an
  outgoing client-to-server command in the Delphi client path. The Delphi
  client may enqueue it through the generic epoch-command gate, but
  `BOrderWorker.HandleServerCommand` has no `TTurnPanicSellCommand` branch, so
  it has no incoming read-model effect.
- Rust `Orders::apply(TradeCommand::TurnPanicSell)` now returns
  `NotApplicable` / `Ignored` and does not mutate order state.
- Removed the Rust-only incoming `OrderEvent::PanicSellChanged` path. Panic
  sell has no incoming read-model effect; the local `Order::panic_sell` field is
  used only as the outgoing Delphi `BOrderWorker.FPanicSell` intent for
  `CheckReplaceFlag`, not as a server-applied event.
- Added a unit test proving the Delphi receive-path guard.

Still not done:

- Continue line-by-line reverse-equivalence for the remaining
  `ProcessCommandOrder` body.

### 2026-05-22 - Phase 1 partial: CheckReplaceFlag outgoing order actions

Done:

- Fixed raw-send public wrappers for replace/cancel/panic-sell. They now require
  local `Orders` state, derive route/status/order type from the local order, and
  return without queueing when the matching Delphi worker would not send.
- `replace_order` repeats the Delphi `ReplaceSentTime = 0` gate and the 5000 ms
  timeout-owned local flags. A second replace while one is in flight updates the
  local desired price but queues no packet.
- `cancel_order` repeats both Delphi branches: active buy/sell sends one cancel
  with current status and clears `CancelRequest`; pending `OS_None` sets the
  `vOrder.PendingCancel` analogue and performs the `replace O_BUY current
  BuyCondPrice` then `cancel OS_None` send sequence.
- Panic sell now has the Delphi market-level shape:
  `turn_panic_sell(&mut Orders, market, value)` mirrors
  `TOrdersWorkers.TurnPanicSell`, and
  `switch_panic_sell_by_market(&mut Orders, market, turn_on)` mirrors the chart
  button `SwitchPanicSellByMarket` toggle. Per-order panic remains only as the
  explicit worker-level helper and still uses the same `FPanicSell` /
  `PrevPanicSell` gate.
- API docs were updated with the new state-aware signatures and side effects.
- Added unit tests for replace gate, active cancel, pending replace-then-cancel,
  per-order panic gate, and market-level panic toggle.

Still not done:

- Continue line-by-line reverse-equivalence for the remaining
  `ProcessCommandOrder` / virtual-worker side effects.

### 2026-05-22 - Phase 1 partial: BulkReplaceNotify timeout

Done:

- Fixed `TBulkReplaceNotify` worker-loop parity. Delphi sets
  `p*Order.OrderReplace := true` and `ReplaceSentTime := GetTimeMS`; later
  `BOrderWorker.DoTheJobVirtual.CheckReplaceFlag` clears the flag if no
  `TOrderReplaceResponse` arrived for more than 5000 ms.
- Rust stores Delphi's single worker-level `ReplaceSentTime`, sets it from the
  dispatcher `now_ms`, clears it only through the current-side
  `CheckReplaceFlag` analogue, and periodically clears stale flags through the
  dispatcher/order tick.
- The active client run loop now calls the order tick in dispatcher mode, next
  to the existing trades tick.
- Added state and dispatcher tests for the 5000 ms timeout.

Still not done:

- Continue line-by-line reverse-equivalence for the remaining
  `ProcessCommandOrder` / virtual-worker side effects.

### 2026-05-22 - Phase 1 partial: pending OS_None update data

Done:

- Fixed the remaining `TOrderStatusUpdate(Status=OS_None)` body semantics.
  Delphi does not apply `UpdateData` to `pBuyOrder` for this status; if
  `IsPending` and `vOrder <> nil`, it sets `vOrder.BuyCondPrice :=
  cmd.UpdateData.MeanPrice`.
- Rust now exposes `Order::pending_buy_cond_price` as the read-model analogue of
  Delphi `vOrder.BuyCondPrice`. Full `TOrderStatus(Status=None)` seeds it from
  `BuyOrder.MeanPrice`; `TOrderStatusUpdate(Status=None)` updates it from
  `UpdateData.MeanPrice` without mutating `buy_order`; non-`None` statuses clear
  it.
- Added a unit test proving the pending-price update and non-application of the
  rest of `UpdateData`.

Still not done:

- Continue line-by-line reverse-equivalence for the remaining
  `ProcessCommandOrder` / virtual-worker side effects.

### 2026-05-22 - Phase 1 partial: SellReasonCode zero guard

Done:

- Fixed `TOrderStatusUpdate.SellReasonCode` body semantics. Delphi
  `BOrderWorker.HandleServerCommand` updates `FPrevSellReasonCode`/`SellReason`
  only when `cmd.SellReasonCode <> 0` and differs from the previous code.
- Rust now keeps the previous `Order::sell_reason_code` when an incoming update
  carries `SellReasonCode = 0`; non-zero changed values are applied.
- Added a unit test for non-zero set, zero keep, and later non-zero change.

Still not done:

- Continue line-by-line reverse-equivalence for the remaining
  `ProcessCommandOrder` body.

### 2026-05-22 - Phase 1 partial: BulkReplaceNotify affected UID list

Done:

- Fixed the API event semantics around `TBulkReplaceNotify`. Delphi loops over
  `notify.UIDs` and mutates only workers found in `WCache`; missing UID's have
  no side effect.
- Rust now emits `OrderEvent::BulkReplaced.uids` with only the UID's that were
  actually found and flagged. If none are found, the command returns
  `OrderNotFound`/`Ignored` instead of reporting a fake bulk replace.
- Added a unit test for mixed found/missing and all-missing notify lists.

Still not done:

- Continue line-by-line reverse-equivalence for the remaining
  `ProcessCommandOrder` body.

### 2026-05-22 - Phase 1 partial: first new OrderStatus epoch bypass

Done:

- Fixed the first `TOrderStatus` create path. Delphi creates the virtual worker
  in `ProcessCommandOrder`, then `OnMServerOrder` calls `HandleServerCommand`
  directly; this bypasses `AcceptServerCommand`, so the first full status does
  not update `FServerLatestEpoch`.
- Rust now skips `accept_epoch_and_phase` only for a newly-created order's first
  `TOrderStatus`. Existing `TOrderStatus` and all compact epoch commands still
  use the epoch/phase guard.
- Updated epoch tests so duplicate/stale checks first prime the Delphi
  `FServerLatestEpoch` analogue, and added a test proving the first same-epoch
  compact update after creation is accepted.

Still not done:

- Continue line-by-line reverse-equivalence for the remaining
  `ProcessCommandOrder` body.

### 2026-05-22 - Phase 1 partial: OrderNotFound cancellation flags

Done:

- Fixed `TOrderNotFound` state semantics. Delphi `ProcessCommandOrder` sets
  `Worker.CancellRequest := true` and `Worker.ServerForcedRemove := true`, but
  does not set `JobIsDone` there; the virtual worker exits/removes itself later.
- Rust now exposes `Order::cancel_request`, sets it together with
  `server_forced_remove`, and leaves `job_is_done` unchanged for
  `TOrderNotFound` until deferred removal removes the entry.
- Updated the unit test to assert the exact immediate flags.

Still not done:

- Continue line-by-line reverse-equivalence for the remaining
  `ProcessCommandOrder` body.

### 2026-05-22 - Phase 1 partial: SellReason description strings

Done:

- Fixed `SellReason::description()` to match Delphi
  `SellReasonCodeToStr(TSellReasonCode)` exactly. The byte-code mapping was
  already correct, but several UI strings had Rust-only spaces (`Panic Sell`,
  `Stop Loss`, `Take Profit`) instead of Delphi's compact names.
- API docs now state that `description()` returns Delphi strings.
- Added a unit test covering every `TSellReasonCode` value.

Still not done:

- Continue line-by-line reverse-equivalence for remaining order state/API
  helpers against Delphi.

### 2026-05-22 - Phase 1 partial: new OrderStatus market guard

Done:

- Fixed the Delphi `Cmd.m <> nil` worker-create guard in the active dispatcher.
  Delphi resolves `TBaseMarketCommand.m` from local `Markets` while parsing; an
  unknown new `TOrderStatus` is logged/dropped and does not create a worker.
- Rust `EventDispatcher::process_command_order` now drops unknown new
  `TOrderStatus` before `Orders::apply` unless the UID already exists. Existing
  tracked orders still accept later status updates by UID, matching Delphi's
  `WCache.TryFind` first.
- Unknown `FromCache=true` statuses are also dropped without an order event in
  the dispatcher path, matching Delphi's `Worker = nil; FreeAndNil(Acmd); exit`.
- Added dispatcher tests for unknown-market and unknown-from-cache drops.

Still not done:

- Continue line-by-line reverse-equivalence for the remaining
  `ProcessCommandOrder` body.

### 2026-05-22 - Phase 1 partial: remove direct TAllStatuses state batch

Done:

- Removed the leftover direct `Orders::apply(TradeCommand::AllStatuses)` hidden
  batch path. `TAllStatuses` is now dispatcher-level only, matching Delphi
  `ClientNewData(MPC_Order)`: increment snapshot flag, call
  `ProcessCommandOrder` once per contained `TOrderStatus`, then run
  `CleanupMissingWorkers`.
- `Orders::apply(AllStatuses)` now returns `NotApplicable` / `Ignored` instead
  of silently mutating order state and emitting only a single `Snapshot` event.
- Updated the low-level snapshot test to use the dispatcher-style
  `begin_snapshot` + per-status loop, then `missing_after_snapshot`.
- Added a unit test proving direct `AllStatuses` is not a hidden
  `ProcessCommandOrder` batch.

Still not done:

- Continue line-by-line reverse-equivalence for the remaining
  `ProcessCommandOrder` body.

### 2026-05-22 - Phase 1 partial: replace side selection for non-`O_BUY`

Done:

- Fixed another `ProcessCommandOrder`/`HandleServerCommand` parity bug around
  order-side selection. Delphi uses the buy side only when
  `OrderType = O_BUY`; every other `TOrderType` goes through the sell-side
  branch.
- Rust `OrderReplaceResponse` and `BulkReplaceNotify` previously treated every
  non-`Sell` order type as buy-side, so `BuyStop`/`BuyLimit` had Rust-only
  machine effects.
- Rust now uses one helper with the exact Delphi predicate
  `order_type == OrderType::Buy` for both branches.
- Added unit tests for `OrderType::BuyStop` proving replace response and bulk
  replace notification mutate sell-side state, not buy-side state.
- Updated API docs for this exact side-selection rule and fixed the terminal
  status list to include `SelLAlmostDone`.

Still not done:

- Continue line-by-line reverse-equivalence for the remaining
  `ProcessCommandOrder` body.

### 2026-05-22 - Phase 1 partial: `SelLDone` final trace grace before removal

Done:

- Fixed deferred removal timing for `OS_SelLDone`. Delphi
  `BOrderWorker.DoTheJobVirtual` does not remove the virtual worker from
  `WCache` immediately after `Status = OS_SelLDone`: it runs two
  `Sleep(200); ProcessCommands; DoQCall` passes so late server visual commands
  such as `TOrderTracePoint` can still target the worker.
- Rust previously kept terminal orders only until the current reader batch was
  drained. That preserved same-batch trace packets but could drop trace/visual
  packets arriving during Delphi's 400 ms final window.
- Rust pending removals now carry a due timestamp. Non-`SelLDone` terminal
  states and `TOrderNotFound` keep the existing batch-deferred removal;
  `SelLDone` is due after 400 ms.
- `Client::run_with_dispatcher` drains due removals with the current loop time,
  and periodic order ticks also drain due removals when the grace window expires.
- Added a dispatcher test proving a second `TOrderTracePoint` at +399 ms is
  still accepted and removal happens at +400 ms.
- Updated API docs for the 400 ms final-trace grace.

Still not done:

- Continue line-by-line reverse-equivalence for the remaining
  `ProcessCommandOrder` / `DoTheJobVirtual` body.

### 2026-05-22 - Phase 1 partial: `CleanupMissingWorkers` uses WCache presence, not Rust terminal marker

Done:

- Fixed another `ProcessCommandOrder`/`CleanupMissingWorkers` parity bug around
  terminal orders that are still waiting for deferred removal.
- Delphi `CleanupMissingWorkers` iterates `WCache` and checks
  `not Worker.JobIsDone`. In `DoTheJobVirtual`, `SetDoneFlags` does not set
  `JobIsDone`; `JobIsDone` is set later by `Execute -> DoFinalSynCall`, after
  `RemoveWorkerFromCache`. Therefore a terminal virtual worker still present in
  `WCache` remains a missing-cleanup candidate.
- Rust had used `Order::job_is_done` as a terminal read-model marker and also
  filtered it out in `missing_after_snapshot()`. That skipped a follow-up
  `TOrderStatusRequest` that Delphi could still send before deferred removal.
- `missing_after_snapshot()` now treats Rust `Orders` presence as the WCache
  equivalent: if the entry is still present and its snapshot flag was not
  refreshed, it is missing. Once deferred removal runs, the entry leaves
  `Orders` and no longer participates.
- Updated API docs to make clear that `job_is_done` is a read-model terminal
  marker, not the Delphi `Worker.JobIsDone` gate for cleanup.
- Added a test proving a `SelLDone` entry still pending removal is returned by
  `missing_after_snapshot()`, then disappears after the due removal drain.

Still not done:

- Continue line-by-line reverse-equivalence for the remaining
  `ProcessCommandOrder` / `DoTheJobVirtual` body.

### 2026-05-22 - Phase 1 partial: `TCorridorUpdate` marks MoonShot state

Done:

- Fixed `TCorridorUpdate` read-model parity. Delphi
  `BOrderWorker.HandleServerCommand` sets `IsMoonShot := true` before updating
  `TestPriceDown` / `TestPriceUp`, and also mirrors those values into
  `PresaveMarketData`.
- Rust previously stored only `corridor_price_down` / `corridor_price_up`,
  losing the worker-level MoonShot flag.
- Added `Order::is_moon_shot` as the read-model counterpart of Delphi
  `BOrderWorker.IsMoonShot`; it starts `false` on `TOrderStatus` creation and
  becomes `true` on `TCorridorUpdate`.
- Updated API docs and added a unit test proving `TCorridorUpdate` sets the flag
  and stores both corridor prices.

Still not done:

- Continue line-by-line reverse-equivalence for the remaining
  `ProcessCommandOrder` / `DoTheJobVirtual` body, including visual trace side
  effects that are still represented only as stored trace points.

### 2026-05-22 - Phase 1 partial: `TOrderTracePoint` line state

Done:

- Fixed the `TOrderTracePoint` read-model shape. Delphi
  `BOrderWorker.ApplyServerTrace` does not just append raw packets: it mutates
  per-side `coBuy` / `coSell` `TOrderLine` objects.
- Added `OrderTraceLine` and `OrderTraceChartPoint`, plus
  `Order::buy_trace_line` / `Order::sell_trace_line`.
- The Rust line update now follows Delphi machine effects:
  only `OrderType::Buy` targets the buy side; finish updates only an existing
  line; a non-initial trace without a line is ignored for drawable line state;
  initial trace creates the line with the compact order `CreateTime` and
  `BasePrice`; temp trace stores `tmp_point`; normal trace appends the same
  point sequence as `TOrderLine.SetPointTrade`; finish mutates the last point
  only while `can_finish` is true.
- Raw `trace_points` remains as diagnostic inbound packet history, but API docs
  now direct UI users to the Delphi-equivalent line fields.
- Added unit tests for ignored non-initial trace and for initial/temp/finish
  sequence equivalence.

Still not done:

- Continue line-by-line reverse-equivalence for remaining order UI/lifecycle
  side effects outside `ApplyServerTrace`.

### 2026-05-22 - Phase 1 partial: bulk move outgoing gate

Done:

- Fixed outgoing bulk move parity against Delphi `TOrdersWorkers.MoveAllBuys` /
  `MoveAllSells` active-client branches.
- Rust `Client::move_all_sells` and `ClientSender::move_all_sells` now require
  the current `Orders` read model and return `false` without queueing when
  Delphi would not send: no matching local order, `RM_None`, side mismatch, or
  immune order for the overloads that check `not ImmuneForClicks`.
- Rust `Client::move_all_buys` and `ClientSender::move_all_buys` now use
  `MoveAllBuysCmdType` instead of the sell-side `MoveAllCmdType`; regular API
  code can no longer produce buy `CmdType=1`, which Delphi does not create and
  the server buy branch does not handle.
- Added unit tests for the Delphi send-gate predicates and queue-level wrapper
  behavior.

Still not done:

- Continue line-by-line reverse-equivalence for remaining outgoing order/UI
  actions: join/split/close/sell, per-order cancel/replace/stops/vstop/panic,
  and local-state side effects around `SetImmuneClicks`.

### 2026-05-22 - Phase 1 partial: `SetImmuneClicks` outgoing local side effect

Done:

- Fixed outgoing `SetImmune` parity against Delphi
  `TOrdersWorkers.SetImmuneClicks`.
- `Orders::apply(TradeCommand::SetImmune)` still ignores incoming packets,
  matching `ProcessCommandOrder`: `SetImmune` is client-to-server UI intent, not
  an incoming order-state update.
- The outgoing wrappers now take `&mut Orders`, call
  `Orders::set_immune_clicks`, mutate `immune_for_clicks` immediately for found
  active local orders, and send only those found items. If no local active order
  was found, they return `false` and queue nothing.
- Added `EventDispatcher::orders_mut()` for this Delphi-equivalent local UI
  mutation.
- Added unit tests for the local side effect, filtering unknown/terminal orders,
  and queueing only after a local worker match.

Still not done:

- Continue line-by-line reverse-equivalence for remaining outgoing order/UI
  actions: join/split/close/sell and per-order cancel/replace/stops/vstop/panic.

### 2026-05-22 - Phase 1 partial: stop/VStop outgoing send-if-changed

Done:

- Fixed outgoing stop/VStop parity against Delphi
  `BOrderWorker.SendStopsIfChanged` and `BOrderWorker.SendVStopIfChanged`.
- Rust high-level `update_order_stops` / `update_vstop` wrappers are no longer
  raw packet senders. They require `&mut Orders` and a local order UID, repeat
  the Delphi local-order gate, compare against the last applied/sent local
  state, mutate the local state before queueing, and return `false` when Delphi
  would not create a wire packet.
- `StopSettings` equality is now bit-exact, matching Delphi
  `TStopSettings.Equal = CompareMem(@A, @B, SizeOf(TStopSettings))`; this keeps
  raw double-bit differences such as `-0.0` vs `+0.0` significant for the send
  gate.
- Incoming `TOrderStopsUpdate` and `TVStopUpdate` still update the same local
  fields, matching Delphi `ApplyStops` / `ApplyVStop`, so outgoing gates see the
  last server-applied values.
- API docs now describe stop/VStop as state-aware actions and tell UI-thread
  callers to marshal these intents to the owner of mutable dispatcher/order
  state instead of bypassing the local gate.
- Added tests for the local send-if-changed predicates and queue-level wrapper
  behavior.

- Rechecked and tightened the previously missed `vOrder = nil` gate:
  `SendStopsIfChanged` and `SendVStopIfChanged` now require
  `Order::has_local_visual_order`, the Rust marker for Delphi
  `BOrderWorker.vOrder <> nil`.
- New pending `TOrderStatus(Status=None)` sets this marker automatically,
  matching `OnMServerOrder` creating a pending visual order. Other tracked
  orders do not infer it from status; local/UI paths can mark it explicitly with
  `Orders::mark_local_visual_order(uid)` after creating their own visual-order
  equivalent.
- Tests now prove changed stop/VStop values do not send without the marker and
  do send after the marker exists. API docs were updated for the marker and
  stop/VStop gate.

Still not done:

- Continue line-by-line reverse-equivalence for remaining outgoing order/UI
  actions: join/split/close/sell and per-order cancel/replace/panic.

### 2026-05-22 - Phase 1 partial: typed outgoing domain gate before Init

Done:

- Fixed the typed/high-level outgoing domain API so it cannot put domain wire
  commands into send queues before the one-time Init opens `domain_ready`.
- Added a shared `domain_ready` mirror for `ClientSender`; `Client` and sender
  now use the same gate.
- Registry-aware subscriptions still record the latest user intent before Init,
  but they send no Engine API/UI subscription packet until Init flushes the
  registry or a later post-Init call changes the intent.
- Stateful order helpers that mutate `Orders` are now gated before the local
  mutation, so pre-Init replace/cancel/stop/VStop/immune calls leave the local
  cache unchanged and queue nothing.
- Follow-up 2026-05-23: raw `send_cmd`, `send_cmd_keyed`, and raw Engine API
  helpers no longer bypass the Init gate. Before `domain_ready`, the raw path
  accepts only mandatory init Engine API methods (`BaseCheck`, `AuthCheck`,
  `GetMarketsList`, `GetMarketsIndexes`, `UpdateMarketsList`); all other raw
  sends are rejected as `SubscribeError::DomainNotReady`.
- Follow-up 2026-05-23: removed Rust-only init send of
  `emk_GetMarketsBalanceFull`. Delphi `TMoonProtoEngine.GetMarketsBalanceFull`
  returns `true` without a MoonProto wire request; active balance bootstrap is
  the post-InitDone `TRequestBalanceRefresh`.
- Added tests proving pre-Init subscription intent has no wire send,
  pre-Init stateful order actions do not mutate local orders, and Init flushes
  pre-Init registry subscriptions once.

Still not done:

- Continue line-by-line reverse-equivalence for the remaining outgoing
  order/UI/strategy/balance command wrappers against the Delphi active-client
  call sites.

### 2026-05-22 - Phase 1 partial: outgoing join/split/close/sell command parity audit

Done:

- Checked the active-client Delphi call sites for `TJoinOrdersCommand`,
  `TSplitOrderCommand`, `TDoClosePositionCommand`,
  `TDoLimitClosePositionCommand`, `TDoSplitPositionCommand`,
  `TDoMarketSplitPositionCommand`, and `TDoSellOrderCommand`.
- Checked Rust `Client` / `ClientSender` wrappers and the builders in
  `commands::trade` against the Delphi constructors and `StoreToStream`
  implementations.
- No protocol code change was required for this block: these commands are
  market-level wire intents, do not create or mutate a local order worker before
  send, and use the same payload fields and retry counts as Delphi.
- Confirmed the route bytes are session route bytes (`cfg.BaseCurrency` and
  `cfg.Header.Current` in Delphi; `Client::trade_ctx` /
  `Client::random_trade_ctx` in Rust). Existing-order wrappers continue to use
  `order.trade_ctx()` only where the Delphi command is tied to a worker UID.
- Cleaned one misleading `legacy` wording in the `TMoveAllBuysCommand` soft-read
  comment: the machine effect is Delphi backward-compatible default `Side=Both`
  when older payloads omit the side byte.

Still not done:

- Continue line-by-line reverse-equivalence for the remaining outgoing UI/order
  wrappers and server-receive consequences around these commands.

### 2026-05-22 - Phase 1 partial: client-originated order commands are silent on receive

Done:

- Fixed dispatcher-level `ProcessCommandOrder` delivery for commands that are
  client-originated or otherwise not server state updates.
- Low-level `Orders::apply` still returns `NotApplicable` for diagnostic direct
  calls, but `EventDispatcher::process_command_order` no longer publishes
  `OrderEvent::Ignored` for that result.
- This matches Delphi `TMoonProtoNetClient.ProcessCommandOrder`: such packets
  are freed/exited or queued into a worker without a separate public ignored
  event, and `BOrderWorker.HandleServerCommand` has no state branch for
  `TTurnPanicSellCommand` / `SetImmune` / join-split-close-sell style client
  intents.

Still not done:

- Continue line-by-line reverse-equivalence for remaining
  `ProcessCommandOrder` branches that do have server-state side effects.

### 2026-05-22 - Phase 1 partial: skipped order packets are silent on receive

Done:

- Tightened dispatcher-level order delivery one step further: only
  `ApplyResult::Applied` becomes an active `Event::Order`.
- This matches Delphi receive behavior for unknown-UID updates, stale epoch
  packets, phase rollbacks, and bulk-replace notifications with no local
  affected worker: Delphi logs or frees/exits, but does not raise a user-facing
  order event.
- Low-level `Orders::apply` keeps `OrderEvent::Ignored` for direct diagnostic
  callers; the active `EventDispatcher` suppresses it.

Still not done:

- Continue line-by-line reverse-equivalence for remaining applied
  `HandleServerCommand` state mutations and virtual-worker tick side effects.

### 2026-05-23 - Phase 1 partial: OS_None update pending-vOrder gate

Done:

- Fixed a `HandleServerCommand(TOrderStatusUpdate)` parity bug. Delphi changes
  `vOrder.BuyCondPrice := UpdateData.MeanPrice` only in the exact branch
  `(cmd.Status = OS_None) and IsPending and (vOrder <> nil)`.
- Rust previously created `pending_buy_cond_price` for any incoming
  `OrderStatusUpdate(Status=None)`, even when the local tracked order was not a
  pending visual order. That invented a Rust-only pending state.
- Rust now updates `pending_buy_cond_price` only when it already exists, which
  is the read-model equivalent of Delphi's local `vOrder` being present.
- Added a regression test for non-pending `OS_None` updates and updated the API
  docs for `pending_buy_cond_price`.

Still not done:

- Continue line-by-line reverse-equivalence for remaining
  `ProcessCommandOrder` / `HandleServerCommand` / `DoTheJobVirtual` effects.

### 2026-05-24 - FireTest diagnostics: ParseFailed raw payload

Done:

- Fixed ErrEmu Sliced diagnostics so reused `datagram_num` values with different
  `blocks_count` are not merged into impossible `blocks=X/Y` summaries.
- Changed `Event::ParseFailed` to carry raw `payload` in addition to `cmd/len`.
  This clone happens only on parser failure and does not touch the normal
  protocol hot path.
- FireTest now writes a raw temp dump for every `ParseFailed` and prints
  `cmd/len/head/dump` in the live log.
- API event docs were updated for the new `ParseFailed` shape and for the
  current caller-thread `ProtocolCore` run model.
- Closed `spec_pipeline/work/хуйня.md §X.129`: the observed `OrderBook`
  `ParseFailed` led to a real Rust-only SynLZ mismatch. mORMot hashes literals
  with `last_hashed < dst - 3`; Rust used `dst_pos - 4`, so some live
  OrderBook streams decoded to wrong bytes while preserving the expected length.
- `compression.rs::synlz_decompress_inner` now uses the exact mORMot condition,
  and the live OrderBook regression test compares exact decoded bytes instead of
  checking only `len == 63`.

Verification:

- `cargo fmt --check` OK.
- `cargo test --lib --quiet` OK: `607 passed`.
- `cargo test --lib synlz_decompress -- --nocapture` OK.
- `cargo check --examples --quiet` OK.
- `cargo test --test fire_test --no-run --quiet` OK.
- Live `cargo test --release --test fire_test -- --ignored --nocapture` on prod
  OK: `FIRETEST_PASS` in 176.75s, `ParseFailed=0`, `FAIL=0`.

Still not done:

- If `ParseFailed` reproduces after the SynLZ fix, treat it as a new
  sliced/server-payload candidate and investigate the dumped payload against
  Delphi orderbook parser/decompress/Sliced reassembly byte-for-byte.
- Continue runtime/protocol parity work. Phase Z remains mandatory at the end:
  full optimization attribution/fixes for all protocol-owned hot paths.

### 2026-05-24 - Phase 1 partial: stale reader epoch transport state

Done:

- Closed the `NextIdeas.md` epoch proof item for writer-visible transport
  state.
- Rust already tagged `ReaderDecodedMsg` with `epoch`, but the shared
  `reader_transport_state` could still be mutated by an async old reader after
  `spawn_reader()` moved the client to a new epoch. Delphi stops the UDP reader
  synchronously before reset/reconnect, so the old reader has no such writer
  state side effect.
- `ReaderTransportState` now carries `active_reader_epoch`. `spawn_reader()`
  increments `current_reader_epoch` before publishing writer state to the
  reader side. Reader-side recv, ping, and handshake writes are no-op unless
  their `reader_epoch` is still active.
- Recorded `spec_pipeline/work/хуйня.md §X.128`.

Verification:

- Added `old_reader_output_and_transport_state_are_discarded_after_new_reader_epoch`.
- Targeted test passed.

Still not done:

- Continue the `NextIdeas.md` lock/slicer work: prove and then collapse the
  Delphi `SendLock` snapshot pieces without adding heavy work under the lock.

### 2026-05-24 - Phase 1 partial: unified SendLock snapshot

Done:

- Collapsed the Delphi `SendLock` snapshot pieces into one Rust
  `SendLockState`.
- `DataToSend*` (`SendQueues`), reader `MPC_SlicedACK` queue, and `TmpSlider`
  now live under the same mutex. This matches Delphi's
  `AcquireSendLock; GetCopySendList; GetCopyAcks; FClient.CopyRecvdData;
  ReleaseSendLock`.
- Reader/user code still does only short push/copy work under this lock. Heavy
  parse/dispatch/send/retry logic remains outside the lock, matching the Delphi
  pattern.
- Recorded `spec_pipeline/work/хуйня.md §X.129`.

Verification:

- Added `send_lock_snapshot_copies_send_acks_and_tmp_slider_atomically_like_delphi`.
- Targeted tests passed:
  `send_lock_snapshot_copies_send_acks_and_tmp_slider_atomically_like_delphi`,
  `writer_tick_copies_ack_queues_then_check_sening_data_like_delphi`,
  `ping_ack_does_not_drop_pending_h_until_writer_copy_apply`.

Still not done:

- Continue the `NextIdeas.md` work: move `slicer` and remaining reader protocol
  state toward per-reader ownership only after preserving immediate ACK/send
  side effects.

### 2026-05-24 - Phase 1 partial: reader-owned Sliced receiver

Done:

- Moved incoming Sliced reassembly state from shared `Client.slicer:
  Arc<Mutex<SlicingReceiver>>` into per-thread `ReaderRuntime.slicer`.
- Each `spawn_reader()` now starts with a fresh `SlicingReceiver`, matching the
  Delphi lifecycle where the old UDP reader is stopped before a new reader can
  mutate `TMoonProtoClient.Receiving`.
- `WantNewHello` still resets the current reader's local receiver.
- Recorded `spec_pipeline/work/хуйня.md §X.130`.

Verification:

- `reader_sends_sliced_ack_without_main_loop_tick` passed.
- `reader_handles_partial_sliced_without_recv_event_backlog` now proves
  reader-owned partial reassembly by sending block 0 and block 1 of the same
  datagram and checking the completed decoded payload.
- `old_reader_output_and_transport_state_are_discarded_after_new_reader_epoch`
  still passed.

Still not done:

- Full removal of `ReaderProtocolState` mutex is not done yet. Delphi soft
  reconnect does not call `FClient.Reset`, so `MPSlider`/session keys must not
  be reset just because a new Rust reader thread is spawned.

### 2026-05-24 - Phase 1 partial: stale reader epoch protocol side effects

Done:

- Closed the next unsafe piece of the `NextIdeas.md` reader ownership item.
- After the first epoch fix, stale reader output was dropped by writer and
  transport-state writes were gated, but an async old reader could still touch
  protocol-owned shared state before its stale `ReaderDecodedMsg` was dropped:
  `ReaderProtocolState` (`MPSlider`, `DataSizeAck` series), `SendLockState`
  reader writes (`SlicedACK`, `TmpSlider` from Ping), and `ReaderPingState`.
- Delphi has no equivalent stale-reader side effect: `UDPClient.Active := false`
  stops the listener before reset/reconnect continues.
- Rust now publishes `active_reader_epoch` to the remaining reader-shared
  protocol state on every `spawn_reader()`. Reader-side recv processing exits
  early if its epoch is stale, and each remaining shared reader mutation checks
  the same epoch at the mutation point.
- This is a transitional parity fix, not a new architecture claim: it preserves
  soft-reconnect `MPSlider`/key lifetime while removing the Rust-only stale
  mutation effect.
- Recorded `spec_pipeline/work/хуйня.md §X.131`.

Verification:

- Added `stale_reader_epoch_cannot_mutate_reader_shared_protocol_state`.
- Targeted tests passed:
  `stale_reader_epoch_cannot_mutate_reader_shared_protocol_state`,
  `old_reader_output_and_transport_state_are_discarded_after_new_reader_epoch`,
  `reader_handles_size_test_without_main_loop_tick`,
  `ping_ack_does_not_drop_pending_h_until_writer_copy_apply`,
  `reader_sends_sliced_ack_without_main_loop_tick`,
  `reader_handles_partial_sliced_without_recv_event_backlog`,
  `send_lock_snapshot_copies_send_acks_and_tmp_slider_atomically_like_delphi`.

Still not done:

- Decide the exact final shape for `ReaderProtocolState`: per-reader ownership
  is only safe if the Rust port preserves Delphi soft reconnect semantics where
  `FClient.Reset` is skipped and replay/session state is carried forward.

### 2026-05-24 - Phase 1 partial: reader ping state atomics

Done:

- Removed the mutex around the small ping/adaptive-rate reader state.
- Delphi mutates `PingCount`, `CanSendRate`, and `UsedSlicedLimit` as ordinary
  shared client fields: writer marks `UsedSlicedLimit` when the sliced send
  budget was hit, reader consumes that flag on next `MPC_Ping`, adjusts
  `CanSendRate`, clears the flag, and emits the ping response.
- Rust now represents that exact shared-field effect with atomics:
  `active_reader_epoch`, `ping_count`, `can_send_rate`, and
  `used_sliced_limit`. The reader still computes the same adaptive-rate update
  at the same `MPC_Ping` point; writer still marks the flag at the same
  `CheckSeningData` budget point.
- Recorded `spec_pipeline/work/хуйня.md §X.132`.

Verification:

- `ping_adaptive_can_send_rate_uses_delphi_used_limit_gate` passed after the
  refactor.
- `stale_reader_epoch_cannot_mutate_reader_shared_protocol_state` passed after
  the refactor, proving stale reader epoch still cannot consume/mutate ping
  state.

Still not done:

- `ReaderTransportState` remains a mutex snapshot because it carries coherent
  multi-field handshake state (tokens/keys/status). Converting it to atomics
  needs a separate proof that Rust will not observe impossible mixed snapshots.

### 2026-05-24 - Phase 1 partial: Trades market-index gate and section filtering

Done:

- Fixed a `TradesStream` / `TradesResendResponse` parity bug in
  `EventDispatcher`.
- Delphi `ProcessTradesStream` exits while fresh market indexes are not synced,
  and `ProcessTradesResendBatch` feeds every inner packet back through
  `ProcessTradesStream(..., False)`, so resend packets use the same gate.
- Delphi also resolves each section through
  `SrvMarkets.FindByServerIndex(MarketIdx)` and skips that section payload when
  the server index is unknown.
- Rust already gated live `TradesStream`, but did not gate
  `TradesResendResponse` and could emit live/resend `TradeSection`s for unknown
  `mIndex`.
- Rust now filters parsed `TradesPacket.sections` through the current
  `emk_GetMarketsIndexes` mapping before applying/emitting trades. Packet
  numbers still reach `TradesState`, so gap/recovery tail behavior remains in
  the Delphi position.
- Recorded `spec_pipeline/work/хуйня.md §X.117`.

Verification:

- Added dispatcher tests for resend gating and live/resend unknown-section
  filtering.
- `cargo test trades --quiet` OK: `42 passed`.
- `cargo fmt --check` OK.
- `cargo test --quiet` OK: `563 passed`.
- `cargo check --examples --quiet` OK.

Still not done:

- Continue line-by-line reverse-equivalence for remaining protocol paths after
  the full verification pass.

### 2026-05-24 - Phase 1 partial: OS_SelLDone SetDoneFlags side effects

Done:

- Fixed a `DoTheJobVirtual.SetDoneFlags` parity bug for `OS_SelLDone`.
- Delphi does not only mark the virtual worker done. Before the final trace
  grace/removal window it sets `pSellOrder.IsClosed := true`,
  `pSellOrder.IsOpened := false`, clears both order replace flags, sets
  `pBuyOrder.IsOpened := false`, and marks buy canceled only if buy was not
  already closed.
- Rust previously marked terminal/deferred removal but left those compact-order
  flags and bulk replace flags unchanged during the grace window.
- Rust now applies the exact `SetDoneFlags` sell-done branch for both full
  `TOrderStatus(Status=OS_SelLDone)` and
  `TOrderStatusUpdate(Status=OS_SelLDone)`.
- Recorded `spec_pipeline/work/хуйня.md §X.118`.

Verification:

- Added state tests for full status and status-update `OS_SelLDone`.
- `cargo test sell_done --quiet` OK: `3 passed`.
- `cargo fmt --check` OK.
- `cargo test --quiet` OK: `565 passed`.
- `cargo check --examples --quiet` OK.

Still not done:

- Continue line-by-line reverse-equivalence for remaining
  `ProcessCommandOrder` / `HandleServerCommand` / `DoTheJobVirtual` effects.

### 2026-05-24 - Phase 1 partial: CheckBinanceTags clears missing markets

Done:

- Fixed an `emk_CheckBinanceTags` state parity bug.
- Delphi `TMoonProtoEngine.CheckBinanceTags` sets `FTokenTagsSeen := false` for
  every market, applies tags for response rows found by market name, then clears
  `m.TokenTags := []` for every market not seen in the latest response.
- Rust previously merged known tags into `MarketsState::token_tags` and kept
  old tags for markets absent from a later response.
- Rust now clears the token-tags map before applying the latest response, so
  absent or unknown markets read back as empty tags.
- Recorded `spec_pipeline/work/хуйня.md §X.119`.

Verification:

- Updated token-tags state test to assert missing markets are cleared.
- `cargo test token_tags --quiet` OK: `2 passed`.
- `cargo fmt --check` OK.
- `cargo test --quiet` OK: `565 passed`.
- `cargo check --examples --quiet` OK.

Still not done:

- Continue line-by-line reverse-equivalence for remaining `Balance`,
  `Trades`, `OrderBook`, `UI`, and API/market domain details.

### 2026-05-24 - Phase 1 partial: UpdateMarketsList out-of-range mIndex

Done:

- Fixed an `emk_UpdateMarketsList` state parity bug.
- Delphi handles every price row through `SrvMarkets.FindByServerIndex(mIndex)`;
  if it returns `nil`, including out-of-range index, it sets
  `NewMarketFound := true` and does not apply that row to any local market.
- Rust already handled the "mapped name exists but is missing locally" case, but
  did not set `markets_list_refresh_needed` when `mIndex` was outside the
  current `emk_GetMarketsIndexes` vector.
- Rust now treats out-of-range indexed price rows as missing-market rows and
  sets the refresh flag.
- Also corrected the `MarketsState` module comment: `UpdateMarketsList` cadence
  is ~2 seconds; ~60 seconds belongs to `CheckBinanceTags`.
- Recorded `spec_pipeline/work/хуйня.md §X.120`.

Verification:

- Added state coverage for out-of-range `mIndex`.
- `cargo test apply_prices_marks_refresh_needed --quiet` OK: `2 passed`.
- `cargo fmt --check` OK.
- `cargo test --quiet` OK: `566 passed`.
- `cargo check --examples --quiet` OK.

Still not done:

- Continue line-by-line reverse-equivalence for remaining market/API and
  protocol-domain details.

### 2026-05-24 - Phase 1 partial: Arb market-index filtering

Done:

- Fixed an `MPC_Balance` / `TArbPricesCommand` active-dispatch parity bug.
- Delphi `MoonProtoClient.pas` sends `TArbPricesCommand.Payload` to
  `ParseArbPayloadCompact`. `ArbClientU.pas` then resolves every compact
  price/isolation `idx` through `SrvMarkets.FindByServerIndex`; if the market is
  missing, the bytes are consumed but the record is not applied.
- Rust raw arb parsers remain raw, but `EventDispatcher` now filters
  `Event::Arb` price blocks and isolation entries through the current server
  `mIndex` mapping before exposing them to user code.
- Recorded `spec_pipeline/work/хуйня.md §X.121`.

Verification:

- Added dispatcher tests for unknown arb price blocks and unknown isolation
  entries.
- `cargo test arb --quiet` OK: `16 passed`.

Still not done:

- Continue line-by-line reverse-equivalence for remaining `Balance`, `UI`,
  `OrderBook`, order worker, and reconnect/maintenance protocol details.

### 2026-05-24 - Refactor-red-flag: fixed packed records need private wire structs

Decision:

- Fixed packed records must be represented as fixed wire structs with
  compile-time layout checks. This improves Delphi identity and removes
  boilerplate without changing behavior.
- Fixed Delphi packed records should use a private wire layer, not long
  hand-written `from_le_bytes` cursor blocks.
- Public/state structs such as `OrderCompact`, `StopSettings`, and
  `OrderUpdateData` must keep normal Rust field types (`i64`, `f64`, `u8`) for
  API/state ergonomics. Do not expose endian-wrapper fields such as
  `F64<LittleEndian>` in the public read model.
- Add private `Wire*` structs for wire parsing/writing, e.g.
  `WireOrderCompact`, `WireStopSettings`, `WireOrderUpdateData`, and
  `WirePriceZone`. These structs mirror Delphi `packed record` layout and use
  endian-aware field wrappers.
- Parse path: read one private `Wire*` from bytes, then convert to the public
  struct. Write path: convert public struct to private `Wire*`, then write the
  exact bytes.
- This applies only to fixed-size packed records that Delphi reads/writes with
  `ms.Read(X, SizeOf(X))` / `ms.Write(X, SizeOf(X))`. Do not apply it to
  variable formats: strings, arrays, count loops, bitmask fields, compressed
  payloads, or variant tails.

Rationale:

- The machine-effect invariant is stronger with an explicit wire struct:
  field order, size, and layout are checked mechanically instead of being
  implied by many repeated byte slices.
- The public API stays clean: user/state code continues to write
  `order.quantity`, not `order.quantity.get()`.

Work order:

1. Add the wire-struct dependency and prove it builds on the current Windows
   GNU toolchain.
2. Convert `PriceZone` first as the smallest fixed record.
3. Convert `OrderUpdateData`, then `StopSettings`, then `OrderCompact`.
4. For every converted record add/keep tests for size, parser roundtrip, writer
   roundtrip, and Delphi-specific bit semantics such as `StopSettings` bitwise
   equality.
5. Run full `cargo fmt --check`, `cargo test --quiet`, and
   `cargo check --examples --quiet` after each meaningful slice.

Current status:

- Converted `PriceZone`, `OrderUpdateData`, `StopSettings`, `OrderCompact`, and
  `EmuTradePoint` to private zerocopy-backed `Wire*` structs.
- Converted 9-byte packed array items `StratCheckedItem` and `ImmuneItem` to
  private zerocopy-backed `Wire*` structs.
- Converted opaque fixed UI setting blobs `TAutoStartConfig` and
  `TAutoStartConfig2` to private zerocopy-backed `Wire*` wrappers. They remain
  raw `Vec<u8>` in public API because Rust does not own those Delphi config
  fields, but the wire sizes are now mechanically checked.
- Converted candle fixed records `DeepPrice`, `DeepPricePack`, and
  `DeepPricePackOLD` to private zerocopy-backed `Wire*` structs.
- Converted trades-stream fixed rows/header (`TradesPacketHeader`, 10-byte
  trade/MM/liquidation rows, and 20-byte watcher-fill rows) to private
  zerocopy-backed `Wire*` structs.
- Converted orderbook `TOrderGlass` 8-byte level rows to private
  zerocopy-backed `WireOrderLevel` while preserving the existing declared
  `buyCount` corrupt-tail decision.
- Converted AuthCheck `THLDexInfo` 18-byte records to private
  zerocopy-backed `WireDexInfo`; the parser still keeps Delphi's zero-filled
  partial-read tail.
- Converted core fixed wire headers `Hello`, `CryptoHeader`, `SliceHeader`, and
  ACK256 payloads to private zerocopy-backed `Wire*` structs.
- Converted service packed records `Ping`, `SizeTest`, `ProbeMTU`, and
  `ProbeMTUAck` to private zerocopy-backed `Wire*` structs.
- Converted AES-GCM IV record `TMoonProtoIV` to a private zerocopy-backed
  `Wire*` struct.
- Converted transport packed headers in nested `moonproto-transport`
  (`ServerMsgHeader` 7 bytes and `ClientMsgHeader` 15 bytes) to private
  zerocopy-backed `Wire*` structs while keeping public header structs plain.
- Checked `TBalanceItem`: despite `packed record`, it is not written/read as
  fixed `SizeOf(TBalanceItem)` wire. Its wire format is UTF-8 string + hash +
  flags + bitmask-controlled scalar fields, so it stays in the variable-format
  parser path.
- Checked `TTradesPacketMapEntry`: it is an obsolete commented-out Delphi cache
  record, not live wire format.
- Checked `TMoonProtoEchoSTUN`: open Rust transport has no direct STUN-echo
  record to convert; mask modes 1/2 are delegated to `moonext`. If direct
  STUN-echo handling is ever moved into open Rust, it must be added as a
  private fixed wire struct with the same 20-byte layout.
- Public API/state structs still expose plain Rust fields; endian-aware wrappers
  are private to the wire layer.
- Added/kept tests for compile-time sizes and byte-for-byte roundtrip.

### 2026-05-23 - Correction: ProcessCommandOrder JobIsDone is not terminal status

Correction:

- The first reading of `MoonProtoClient.pas:589-666` was too literal:
  Delphi checks `not Worker.JobIsDone`, but MoonProto virtual workers do not
  set `JobIsDone` when status becomes terminal. `JobIsDone` is set only in
  `DoFinalSynCall`, after `DoTheJobVirtual` returns; `RemoveWorkerFromCache`
  happens before that.
- Therefore Rust `Order.job_is_done` is a read-model terminal marker, not the
  Delphi thread-lifetime flag. While a terminal Rust entry waits for deferred
  removal, it still represents a Delphi worker physically present in `WCache`.
- Kept/restored the correct behavior: terminal entries waiting for deferred
  removal can still be `CleanupMissingWorkers` candidates, and `TOrderNotFound`
  still sets `cancel_request` / `server_forced_remove` while the entry exists.
- Updated API docs to make this distinction explicit.

Verification:

- Targeted tests passed:
  `missing_after_snapshot_keeps_terminal_entry_until_deferred_removal_like_delphi_wcache`,
  `order_not_found_marks_server_forced_then_deferred_removal_like_delphi`,
  `visual_trace_after_terminal_status_is_accepted_before_deferred_removal_like_delphi`.
- `cargo fmt` OK.
- Full `cargo test` OK: `548 passed`; live/fire tests ignored by default.
- `cargo check --examples` OK.

Still not done:

- Continue line-by-line reverse-equivalence for remaining
  `ProcessCommandOrder` / `HandleServerCommand` / `DoTheJobVirtual` effects.

### 2026-05-23 - Phase 1 partial: OrderBook reconnect retry token parity

Done:

- Fixed `NeedResubscribeOrderBooks` parity. Delphi keeps
  `FSubscribedBookServerToken` and retries full `BookSubbed` batch subscribe
  every 5000 ms until a successful `DoSubscribeOrderBooks` response confirms
  the current `Client.ServerToken`.
- Rust previously replayed registry orderbooks once after reconnect/index sync.
  If the subscribe request or response was lost, the registry intent remained
  but no later retry happened.
- Rust now tracks `subscribed_book_server_token`,
  `last_book_reconnect_check_ms`, and the UID of the current full-registry
  replay. Only that replay response, or the first successful orderbook subscribe
  when the token is still zero, advances the confirmed token.
- Fixed `ResetOrderBookCaches` machine effect. Delphi clears out-of-order
  caches and resets per-book seq, but does not wipe visible orderbook levels.
  Rust now has `reset_caches_keep_books()` and uses it on ServerToken change
  and before reconnect orderbook replay.
- Added regression tests for 5000 ms retry throttle, wrong-UID subscribe
  success not stopping replay, successful replay confirmation, and cache reset
  preserving visible book snapshots. API docs updated.

Verification:

- `cargo fmt` OK.
- `cargo test` OK: `548 passed`; live/fire tests ignored by default.
- `cargo check --examples` OK.

Still not done:

- Continue line-by-line reverse-equivalence for remaining
  `ProcessCommandOrder` / `HandleServerCommand` / `DoTheJobVirtual` effects.

### 2026-05-23 - Phase 1 partial: StrategySerializer field order and default-skip parity

Done:

- Re-checked `StrategySerializer` against Delphi source.
- Historical check: the temporary `DELPHI_STRATEGY_FIELD_ORDER` and
  `DELPHI_STRATEGY_FIELD_TYPES` tables were compared against
  `Strategies.pas:TStrategy` public fields: `477/477`, no order/type
  mismatches. These static Rust tables were later removed after live
  `TStratSchema` support landed.
- Confirmed Rust writer now emits known fields in Delphi public-field order, not
  the old alphabetical `HashMap` order.
- Fixed the separate parity risk from `spec_pipeline/work/хуйня.md §X.90`:
  typed `StrategyBatchBuilder` now filters back to Delphi `SaveStrategyToCompact`
  semantics before writing. Unknown fields, wrong TypeID values, and values equal
  to `TStrategy.Create` defaults are not wire-visible. This initial fix used
  temporary Rust metadata; the current implementation uses the server schema
  defaults instead, including runtime color defaults.

Still not done:

- Continue line-by-line reverse-equivalence for remaining `ProcessStratCommand`
  branches and then `Balance`, `Trades`, `OrderBook`, `UI`, and API domain
  handling.

### 2026-05-23 - Phase 1 partial: full balance snapshot missing-market reset

Done:

- Fixed a `TMoonProtoEngine.OnBalanceSnapshot` parity bug.
- Delphi full balance snapshot does not delete a market object when it is absent
  from `cmd.Items`. It resets balance/position/PNL fields to defaults, but
  preserves `BalanceHash`, `bnMaxValue`, and `LastBalanceEpoch`.
- Rust previously replaced `BalancesState::by_market` with only incoming items.
  First pass fixed previous missing rows, preserving `balance_hash`,
  `max_value`, and per-market epoch.
- Second pass fixed the remaining machine-effect gap: Delphi iterates every
  current `TMarket`, so a known market with no previous balance row also becomes
  a visible zero/default balance after full snapshot. Rust active apply now
  receives the full current `MarketsState` name list and creates those default
  rows too. Unknown markets that are not present in current `MarketsState` are
  still ignored like Delphi `Markets.MarketByNameFast`.
- Added regression tests and recorded `spec_pipeline/work/хуйня.md §X.91` and
  `§X.111`.
- API docs now describe the missing-market reset semantics.

Still not done:

- Continue line-by-line reverse-equivalence for remaining `Balance` parser/state
  details, then `Trades`, `OrderBook`, `UI`, and API domain handling.

### 2026-05-23 - Phase 1 partial: balance command version gate

Done:

- Fixed a balance dispatcher parser parity bug.
- Delphi balance packets go through `TCommandRegistry.FromStream`, which skips
  any command with `ver > Current_Proto_CmdVer` before concrete class parsing.
- Rust `client_new_data_balance` previously ignored the common command `ver` and
  could apply a future-version full snapshot.
- Rust now skips future-version balance packets before `parse_balance`, matching
  registry behavior.
- Added a regression test and recorded `spec_pipeline/work/хуйня.md §X.92`.
- API docs now mention the balance version gate.

Still not done:

- Continue line-by-line reverse-equivalence for remaining `Balance` parser/state
  details, then `Trades`, `OrderBook`, `UI`, and API domain handling.

### 2026-05-24 - Phase 1 partial: balance Count parser loop

Done:

- Fixed a `TBalanceCommand.CreateFromStream` / `TBalanceIncrUpdate.CreateFromStream`
  parser parity bug.
- Delphi reads `Count`; if it is positive, it iterates items in order. It does
  not pre-drop the whole item list because `Count * min_item_size` exceeds the
  remaining bytes.
- Rust previously had a balance-only DoS guard that returned an empty item list
  before trying to parse any item. That changed malformed/partial packet machine
  effect from "keep already present readable items until parsing stops" to
  "drop all items".
- Rust now treats `Count <= 0` as no items and otherwise parses items in order
  until the first item cannot be read, matching the Delphi loop shape while
  still avoiding pre-allocation from untrusted `Count`.
- Added regression tests and recorded `spec_pipeline/work/хуйня.md §X.112`.

Still not done:

- Continue line-by-line reverse-equivalence for remaining `Balance` parser/state
  details, then `Trades`, `OrderBook`, `UI`, and API domain handling.

### 2026-05-26 - Balance base command is recognized and ignored

Done:

- Re-checked Delphi `MoonProtoClient.pas:ClientNewData` and
  `MoonProtoEngine.pas:ProcessBalanceCommand`.
- `TBalanceCommandBase` CmdId=1 is a known balance class with only `Epoch`.
  Delphi routes it through `ProcessBalanceCommand`, then ignores it because it
  is neither `TBalanceSnapshotFull` nor `TBalanceIncrUpdate`.
- Rust no longer emits `Event::Raw` for CmdId=1. It parses the known command and
  leaves state/events unchanged, matching the existing CmdId=2 exact-balance
  ignored path.
- Recorded `spec_pipeline/work/хуйня.md §X.168`.

Verification:

- Extended `dispatcher_ignores_exact_balance_command_id_2_like_delphi`.

### 2026-05-26 - UI skipped/unknown commands do not emit Settings events

Done:

- Re-checked Delphi `TCommandRegistry.FromStream`: if command `ver` is newer
  than `Current_Proto_CmdVer`, it returns a base command with `FSkipped := true`.
- Re-checked `MoonProtoClient.pas:ClientNewData(MPC_UI)`: skipped UI commands
  are log-only, and unknown UI `cmd_id` becomes `TBaseUICommand` then is freed
  without any settings/UI domain action.
- Rust low-level `UICommand::parse` now exposes future-version packets as
  `UICommand::Skipped { cmd_id, uid, ver }` instead of collapsing them into
  `Unknown`.
- Active `EventDispatcher` ignores both `UICommand::Skipped` and
  `UICommand::Unknown`, so they no longer emit public `Event::Settings`.
- Recorded `spec_pipeline/work/хуйня.md §X.170`; API docs describe the
  low-level parser variants and active-dispatcher skip rule.

Verification:

- Added dispatcher regression tests for future-version UI and unknown UI
  subcommand id.

### 2026-05-23 - Phase 1 partial: OrderBook recovery and reconnect replay order

Done:

- Fixed `TOrderBookCache.TryRequestFull` parity.
- Delphi initializes `FLastFullRequestTime := 0` and still applies
  `abs(GetTimeMS - FLastFullRequestTime) <= BOOK_FULL_REQUEST_THROTTLE`.
  Rust previously special-cased `0` as "never requested"; that branch is gone.
- Added regression coverage for the first 0..5000 ms throttle window and updated
  the synthetic orderbook tests to use `GetTimeMS`-like timestamps.
- Fixed post-reconnect orderbook replay order. Delphi `CheckBookTopics` exits
  while `FLastServerAppToken <> PeerAppToken`, so `SubscribeOrderBook` replay
  cannot run before fresh `GetMarketsIndexes`.
- Rust now delays only orderbook registry replay until successful fresh indexes.
  After that it sends `UpdateMarketsList` first and then batch
  `SubscribeOrderBook`, matching Delphi `UpdateMarketsList` + later
  `CheckBookTopics` order.
- Recorded `spec_pipeline/work/хуйня.md §X.93` and `§X.94`; API docs now state
  the delayed orderbook replay rule.

Still not done:

- Continue line-by-line reverse-equivalence for remaining `OrderBook` parser
  edge cases and then `Trades`, `UI`, and API domain handling.

### 2026-05-23 - Phase 1 partial: AllTrades reconnect sequence

Done:

- Fixed a reconnect replay parity bug for all-trades. Delphi
  `BMarketHistoryWorker.Execute` does not simply replay `SubscribeAllTrades`.
  When `NeedReconnectAllTrades` fires, it runs
  `UnSubscribeAllTrades -> ClearSenderState -> Sleep(100) ->
  DoSubscribeAllTrades(false)`.
- Rust reconnect restore no longer sends immediate `SubscribeAllTrades` from
  `Fine`. The active maintenance tick now tracks a Delphi-style
  `FTradesServerToken` analogue: it is updated only when a `TradesStream` packet
  reaches the parser for the current `ServerToken`.
- Until that happens, the library sends `UnsubscribeAllTrades`, waits 100 ms,
  sends `SubscribeAllTrades(want_mm)`, and retries the pair no more often than
  every 5000 ms. This applies only when the Rust opt-in all-trades registry has
  an active subscription intent.
- Added tests for delayed subscribe and 5000 ms retry throttle.

Still not done:

- Continue strict `TradesState` / `ProcessTradesStream` reverse-equivalence for
  resend buckets and remaining section side effects.

### 2026-05-23 - Phase 1 partial: TStratSnapshot epoch after successful apply

Done:

- Fixed a `ProcessStratCommand(TStratSnapshot)` order bug.
- Delphi applies the snapshot serializer first and only then assigns
  `cfg.LocalStratEpoch := cmd.ServerEpoch`. If `cmd.Data=nil` or the serializer
  fails, epoch is not advanced.
- Rust previously set `StratsState::last_server_epoch` as soon as the
  `StratCommand::Snapshot` was parsed, before dispatcher decode/apply.
- Rust now advances `last_server_epoch` only after
  `apply_snapshot_decoded_with_mode` succeeds. Malformed snapshots are logged
  and do not emit `SnapshotFull` / `SnapshotPartial`.
- Added dispatcher tests for a valid empty serializer snapshot and for invalid
  wire `Size=0`.

Still not done:

- Continue line-by-line reverse-equivalence for remaining
  `ProcessStratCommand` branches and their state/UI side effects.

### 2026-05-23 - Phase 1 partial: no-op incoming TTradeEpochCommand epoch side effect

Done:

- Fixed another `ProcessCommandOrder` parity bug in the "silent on receive"
  command group.
- Delphi still routes existing-worker no-op `TTradeEpochCommand` packets through
  `AcceptServerCommand`: epoch is checked and `FServerLatestEpoch[Status]` is
  updated before the later `HandleServerCommand` body finds no state branch.
- Rust previously returned `NotApplicable` for incoming `TOrderReplaceCommand`,
  `TOrderCancelCommand`, `TOrderStatusRequest`, `TTurnPanicSellCommand`, and raw
  `TTradeEpochCommand` without that epoch side effect.
- Rust now applies the Delphi epoch/phase side effect for those no-op incoming
  epoch commands and remains silent to public events.
- Added a regression test proving `TTurnPanicSellCommand(epoch=2)` makes a later
  `TOrderStatusUpdate(epoch=1)` stale, as Delphi would.

Still not done:

- Continue line-by-line reverse-equivalence for remaining
  `ProcessCommandOrder` / `HandleServerCommand` / `DoTheJobVirtual` effects.

### 2026-05-23 - Phase 1 partial: TAllStatuses nested command dispatch

Done:

- Fixed a `TAllStatuses` parser parity bug found during the order-block audit.
- Delphi `TAllStatuses.CreateFromStream` reads each nested item through
  `TBaseTradeCommand.FromStream(ms)` and then casts the result to
  `TOrderStatus`. Therefore every nested item must carry `CmdId=4`.
- Rust previously called `OrderStatus::read` directly and could accept a
  status-shaped nested payload whose header carried another `CmdId`.
- Rust now rejects `TAllStatuses` when any nested item is not `CmdId=4`.
- Added a unit test with a status-shaped nested `CmdId=5` payload.

Still not done:

- Continue line-by-line reverse-equivalence for remaining
  `ProcessCommandOrder` / `HandleServerCommand` / `DoTheJobVirtual` effects.

### 2026-05-23 - Author decision: TradesStream full-bucket wipe is intended

Author decision recorded in `spec_pipeline/work/INVARIANT.md` §1.7:

- Full wipe of all `MAX_GAP_BUCKETS = 50` gap buckets when the cap is reached is
  not a bug.
- Reaching the cap means the channel is already very bad; old recovery debt is
  competing with live flow.
- In that mode the library should free recovery state and spend channel bytes on
  new trades instead of continuing to resend old buckets.

This closes the temporary red-flag note about changing Rust to Delphi oldest
eviction. Do not "fix" Rust full-cap behavior to oldest eviction unless the
author changes this invariant.

### 2026-05-23 - Phase 1 partial: current Delphi TradesStream/Sliced retry fixes ported

Source diff checked manually:

- `X:\proj-X\MoonBot\src\MoonProto\bak\MoonProtoEngine.pas` ->
  `X:\proj-X\MoonBot\src\MoonProto\MoonProtoEngine.pas`
- `X:\proj-X\MoonBot\src\MoonProto\bak\MoonProtoCommon.pas` ->
  `X:\proj-X\MoonBot\src\MoonProto\MoonProtoCommon.pas`
- `X:\proj-X\MoonBot\src\MoonProto\bak\MoonProtoIntStruct.pas` ->
  `X:\proj-X\MoonBot\src\MoonProto\MoonProtoIntStruct.pas`

Delphi fixes to port to Rust exactly:

1. `TGapBucket` now has `RefundUsed: Boolean`.
2. `CreateGapBucket` initializes `RetryCount := 0`,
   `RefundUsed := False`, `LastRetryTime := NowTimeX`.
3. `FindBucketForPacket(... WantExtend=True ...)` extends only when
   `RetryCount < 2` and `EndNum = Word(NewGapStart - 2)`.
4. On extend with `RetryCount >= 1` and `not RefundUsed`, do exactly one retry
   budget refund: `Dec(RetryCount); RefundUsed := True`. Do not change
   `LastRetryTime`.
5. If `RetryCount >= 2`, do not extend; let caller create a fresh bucket or hit
   the intentional full-cap reset policy.
6. `CheckMissingTradesPackets` computes `PathDelay` before close decisions.
   `allReceived` closes immediately. If `RetryCount >= MAX_RETRY_COUNT`, do not
   send more resend requests and do not close immediately; close only after
   `abs(NowTimeX - LastRetryTime) > PathDelay`.
7. Sliced retry-counter: `TMoonProtoSlicedData` now has `FLastRetryInc`.
   `CheckSeningData` sets `SentOnPathDelay := True` only when a piece with
   `Piece.LastChecked > 0` is actually resent. Increment `FRetryCount` only when
   a timestamp group advanced, a real retry was sent, and
   `abs(FLastRetryInc - CurTm) > PathDelay`; then set `FLastRetryInc := CurTm`.
   Initial sends of a large sliced datagram must not burn retry budget.
8. `ApplyACK` still resets `FRetryCount := 0` on ACK progress, deletes ACKed
   pieces, and preserves remaining pieces' `LastChecked` values.

Review result: Delphi changes look mechanically correct for the intended
protocol behavior. One non-functional stale comment remains in Delphi
(`RetryCount: Byte; // сколько раз запрашивали (макс 2)`) while
`MAX_RETRY_COUNT = 3`; do not copy that comment into Rust docs.

Done:

- Ported all eight items into Rust:
  - `TradesState::GapBucket` has `refund_used`.
  - bucket creation resets `retry_count`, `refund_used`, and `last_retry_ms`.
  - extend is allowed only while `retry_count < 2`.
  - extend performs the one-time retry refund without moving `last_retry_ms`.
  - `retry_count >= 2` forces a fresh bucket or the intentional full-cap reset.
  - bucket close waits `PathDelay` after the final retry.
  - outgoing Sliced has `last_retry_inc`; primary timestamp groups no longer
    burn retry budget.
  - Sliced ACK-progress resets retry count and preserves remaining
    `LastChecked` clocks.
- Updated `stress_client` loss gate: TradesStream under configured loss is
  checked against the measured `p^3` model, not against impossible absolute
  zero packet loss.
- Verification:
  - `cargo fmt --check` OK.
  - `cargo test --lib` OK: `509 passed`.
  - `cargo check --example stress_client` OK.
  - prod `err_emu=0 pre_connect`, `180s`:
    - A `observed_live_loss=0.070%`, `fact_lost_at_close=0%`.
    - B `observed_live_loss=0.884%`, `fact_lost_at_close=0%`.
    - API `494/494`, candles `14/14`, verdict PASS.
  - prod `err_emu=10 pre_connect`, `180s` after gate update:
    - A `observed_live_loss=12.314%`, `theory_3req_observed=0.1867%`,
      `fact_lost_at_close=0.397%`, `fact_over_theory=2.1x`,
      `expected_lost_3req=0.94`, actual final lost `2`, gate OK.
    - B `observed_live_loss=13.226%`, `theory_3req_observed=0.2314%`,
      `fact_lost_at_close=0%`, `expected_lost_3req=1.25`, actual final lost
      `0`, gate OK.
    - API `494/494`, candles `14/14`, verdict PASS.

Still not done:

- Continue line-by-line reverse-equivalence for remaining
  `ProcessCommandOrder` / `HandleServerCommand` / `DoTheJobVirtual` effects.

### 2026-05-23 - Phase 1 partial: SnapshotFlag is refreshed before command guards

Done:

- Fixed a `ProcessCommandOrder` snapshot-flag parity bug. Delphi sets
  `Worker.SnapshotFlag := CurrentSnapshotFlag` immediately after a successful
  `WCache.TryFind(TaskUID)`, before `TOrderNotFound`, time correction,
  `JobIsDone`, type filtering, and `AcceptServerCommand` epoch/phase checks.
- Rust previously refreshed `snapshot_flag` only for applied `TOrderStatus`.
  A live `TOrderStatusUpdate` / `TOrderReplaceResponse` / visual command during
  a fresh snapshot window could therefore still leave the order marked missing
  in Rust, even though Delphi would mark the worker as present.
- Rust now refreshes the read-model snapshot mark for every incoming
  `TBaseMarketCommand`/`ProcessCommandOrder` equivalent that finds an existing
  entry before later guards can reject or ignore the command.
- Preserved the Delphi exclusions: `TAllStatusesReq` / `TSetImmuneCommand` are
  not `TBaseMarketCommand`, and `TBulkReplaceNotify` is handled in an early
  branch before the general `Worker.SnapshotFlag` assignment.
- Added tests proving a duplicate/stale `TOrderStatusUpdate` still refreshes
  snapshot presence before being rejected, while `TBulkReplaceNotify` and a
  non-`TBaseMarketCommand` request do not.

Still not done:

- Continue line-by-line reverse-equivalence for remaining
  `ProcessCommandOrder` / `HandleServerCommand` / `DoTheJobVirtual` effects.

### 2026-05-23 - Phase 1 partial: full OS_None status creates pending vOrder only on new order path

Done:

- Fixed a second pending-vOrder parity bug around full `TOrderStatus`.
- Delphi new-order path for `TOrderStatus(Status=OS_None)` is special:
  `ProcessCommandOrder` creates the worker, queues `OnMServerOrder`, and
  `OnMServerOrder` creates a visual pending order with
  `vo.BuyCondPrice := Cmd.BuyOrder.MeanPrice`.
- Existing-worker full `TOrderStatus(Status=OS_None)` does not create or update
  `vOrder.BuyCondPrice`; `HandleServerCommand(TOrderStatus)` only applies
  `Cmd.BuyOrder` / `Cmd.SellOrder`, stops, immune flag, and `Status`.
- Rust previously set `pending_buy_cond_price = BuyOrder.MeanPrice` for every
  full `OS_None` status. It now does that only for the new-order path. Existing
  pending entries keep their current visual price, and existing non-pending
  entries do not invent pending state.
- Added tests for both existing-pending and existing-non-pending full
  `OS_None` statuses and updated API docs.

Still not done:

- Continue line-by-line reverse-equivalence for remaining
  `ProcessCommandOrder` / `HandleServerCommand` / `DoTheJobVirtual` effects.

### 2026-05-23 - Phase 1 partial: Trades resend check is a ProcessTradesStream tail effect

Done:

- Fixed a `TradesStream` recovery scheduling parity bug. Delphi does not have a
  free-running trades-gap resend timer: `MoonProtoEngine.pas:1914-1918` calls
  `CheckMissingTradesPackets` only at the tail of `ProcessTradesStream`, after a
  successfully parsed live or resend trades packet.
- Rust active mode previously called `dispatcher.trades.tick_with_events(...)`
  from writer-loop every 100 ms. That could send `emk_TradesResend` during
  channel silence, where Delphi would send nothing.
- Rust now checks recovery from `EventDispatcher::dispatch_into_active_actions`
  only after a valid `TradesStream` / `TradesResendResponse` produced an
  applied trades signal. Generated `emk_TradesResend` payloads are sent through
  the same active action outbox as other protocol-owned sends.
- `TradesState::tick` now mirrors the Delphi caller throttle: if
  `now_ms - last_check_missing_ms <= 100`, it exits; otherwise it updates
  `last_check_missing_ms` first, then exits on `used_buckets=0` or runs the
  bucket retry/close pass.
- Added tests for active tail-check generation and for the no-bucket
  `LastCheckMissingTime` update. API docs/examples no longer describe active
  recovery as a periodic writer-loop tick.

Verification:

- `cargo fmt` OK.
- `cargo test --lib --quiet` OK: `533 passed`.
- `cargo check --examples --quiet` OK.

Still not done:

- Continue line-by-line reverse-equivalence for remaining
  `ProcessCommandOrder` / `HandleServerCommand` / `DoTheJobVirtual` effects.

### 2026-05-23 - Phase 1 partial: ReplaceSentTime is worker-level, not per-side

Done:

- Fixed another `DoTheJobVirtual.CheckReplaceFlag` parity bug.
- Delphi has one `ReplaceSentTime` per `BOrderWorker`; `pBuyOrder.OrderReplace`
  and `pSellOrder.OrderReplace` are side flags, but the in-flight clock is not
  per-side.
- Rust previously stored `bulk_replace_buy_sent_ms` /
  `bulk_replace_sell_sent_ms`, cleared the side timer in
  `TOrderReplaceResponse`, and timed out both sides independently.
- Rust now stores one `replace_sent_time_ms`: `TBulkReplaceNotify` sets side
  flag + shared timer, `TOrderReplaceResponse` clears only the side flag, and
  `tick_bulk_replace_timeouts` mirrors current-side `CheckReplaceFlag`.
- Recorded `spec_pipeline/work/хуйня.md §X.108`.

Verification:

- Targeted replace/bulk tests OK, including
  `replace_response_clears_flag_then_tick_clears_shared_sent_time_like_delphi`
  and `bulk_replace_tick_checks_only_current_side_like_delphi_forder`.
- `cargo fmt` OK.
- Full `cargo test` OK: `550 passed`; live/fire tests ignored by default.
- `cargo check --examples` OK.

Still not done:

- Continue line-by-line reverse-equivalence for remaining
  `ProcessCommandOrder` / `HandleServerCommand` / `DoTheJobVirtual` effects.

### 2026-05-23 - Phase 1 partial: ServerForcedRemove final cleanup

Done:

- Fixed a `DoTheJobVirtual finally` parity bug after `TOrderNotFound`.
- Delphi `ProcessCommandOrder` sets `CancellRequest` /
  `ServerForcedRemove`; then virtual-worker `finally` marks both `pBuyOrder`
  and `pSellOrder` as closed+canceled, sets `CloseTime := Now`, and clears
  `OrderReplace`.
- Rust `OrderNotFound` now performs that final cleanup before deferred removal:
  both compact orders get `is_opened=0`, `canceled=1`, `is_closed=1`, local
  Delphi `TDateTime` close-time, and both replace flags are cleared.
- Recorded `spec_pipeline/work/хуйня.md §X.109`.

Verification:

- Targeted `order_not_found_marks_server_forced_then_deferred_removal_like_delphi`
  OK.
- `cargo fmt` OK.
- Full `cargo test` OK: `550 passed`; live/fire tests ignored by default.
- `cargo check --examples` OK.

Still not done:

- Continue line-by-line reverse-equivalence for remaining
  `ProcessCommandOrder` / `HandleServerCommand` / `DoTheJobVirtual` effects.

### 2026-05-24 - Phase 1 partial: pending cancel repeats from worker loop

Done:

- Fixed a `DoTheJobVirtual.CheckReplaceFlag` pending-cancel parity bug.
- Delphi pending `OS_None` cancel is not a one-shot UI send. `CancelOrder` sets
  `vOrder.PendingCancel`, then the worker loop keeps sending
  `replace O_BUY BuyCondPrice` plus `cancel OS_None` after each 32 ms sleep
  while the order remains pending.
- Rust previously sent the pending replace-then-cancel pair only once from
  `Client::cancel_order`, leaving `pending_cancel=true` but with no active
  resend loop.
- Rust now records the first send time and `EventDispatcher` active order tick
  emits an `OrderCancel` active action every 32 ms or later while
  `pending_cancel && status == OS_None && pending_buy_cond_price.is_some()`.
- Recorded `spec_pipeline/work/хуйня.md §X.110`.

Verification:

- Added state-level coverage for the 32 ms pending resend gate.
- Added dispatcher-level coverage proving active order ticks emit the resend
  action, not only the first public wrapper call.

Still not done:

- Continue line-by-line reverse-equivalence for remaining
  `ProcessCommandOrder` / `HandleServerCommand` / `DoTheJobVirtual` effects.

### 2026-05-24 - Phase 1 partial: TAllStatuses Count parser loop

Done:

- Fixed another `TAllStatuses` parser parity bug found during the order-block
  audit.
- Delphi reads `N` and then loops `for k := 0 to N - 1`; there is no
  `N * min_status_size <= remaining` precheck. `N <= 0` produces an empty
  snapshot.
- Rust previously rejected the whole command for `count_raw < 0` or
  `count_raw * 11 > remaining`. That was the same class as the fixed balance
  `Count` guard drift: a Rust-only drop-all before the Delphi item loop.
- Rust now returns an empty `AllStatuses` for `Count <= 0`, and for positive
  counts reads nested `TOrderStatus` items until the payload ends or the next
  item cannot be parsed, preserving already parsed entries. The nested
  `CmdId=4` check remains, because Delphi dispatch inside
  `TBaseTradeCommand.FromStream` must produce a `TOrderStatus`.
- Recorded `spec_pipeline/work/хуйня.md §X.113`.

Verification:

- Added parser tests for negative count and overstated count.
- `cargo fmt --check` OK.
- `cargo test --quiet` OK: `558 passed`.
- `cargo check --examples --quiet` OK.

Still not done:

- Continue line-by-line reverse-equivalence for remaining
  `ProcessCommandOrder` / `HandleServerCommand` / `DoTheJobVirtual` effects.

### 2026-05-24 - Phase 1 partial: TBulkReplaceNotify Count parser loop

Done:

- Fixed a `TBulkReplaceNotify` parser parity bug in the
  `ProcessCommandOrder` early branch.
- Delphi reads `Count`, allocates `UIDs`, and reads UID values in a loop. There
  is no precheck that `Count * SizeOf(UInt64)` fits the remaining stream.
- Rust previously rejected the whole command when `count * 8 > remaining`,
  losing already present UID values.
- Rust now reads UID values until fewer than 8 bytes remain, preserving the
  complete UID entries that Delphi would already process in the notification
  loop.
- Recorded `spec_pipeline/work/хуйня.md §X.114`.

Verification:

- Added a parser test for overstated `Count`.
- `cargo fmt --check` OK.
- `cargo test --quiet` OK: `560 passed`.
- `cargo check --examples --quiet` OK.
- `cargo fmt --check` OK.
- `cargo test --quiet` OK: `558 passed`.
- `cargo check --examples --quiet` OK.

Still not done:

- Continue line-by-line reverse-equivalence for remaining
  `ProcessCommandOrder` / `HandleServerCommand` / `DoTheJobVirtual` effects.

### 2026-05-24 - Phase 1 partial: TSetImmuneCommand Count parser loop

Done:

- Fixed a `TSetImmuneCommand` parser parity bug in the order-command parser.
- Delphi reads `N: Byte`, allocates `Items`, and reads the packed item array
  without a `N * SizeOf(TImmuneItem) <= remaining` precheck.
- Rust previously rejected the whole command when `count * 9 > remaining`,
  producing `ParseFailed` before the natural `ProcessCommandOrder` ignore path.
- Rust now reads full 9-byte items until the payload tail is too short,
  preserving already present entries.
- Recorded `spec_pipeline/work/хуйня.md §X.115`.

Verification:

- Added a parser test for overstated `Count`.

Still not done:

- Continue line-by-line reverse-equivalence for remaining
  `ProcessCommandOrder` / `HandleServerCommand` / `DoTheJobVirtual` effects.

### 2026-05-24 - Phase 1 partial: UI word-count parser loops

Done:

- Fixed the same Rust-only precheck pattern in UI command parsers:
  `TStratStartStopCommandV2`, `TEmuTradesCommand`, and
  `TTriggerManageCommand`.
- Delphi reads `Count: Word` and then reads item arrays from the stream without
  a `Count * elem_size <= remaining` drop-all branch.
- Rust previously returned `None` for the whole command if the declared count
  did not fit the remaining payload.
- Rust now preserves complete leading items and stops on short tails. For
  `TriggerManage`, if the payload ends before `KeysCount`, the parser returns
  the already read `markets` and an empty `keys` array.
- Recorded `spec_pipeline/work/хуйня.md §X.116`.

Verification:

- Added a parser test covering overstated counts for all three UI command
  families.
- `cargo fmt --check` OK.
- `cargo test --quiet` OK: `560 passed`.
- `cargo check --examples --quiet` OK.

Still not done:

- Continue line-by-line reverse-equivalence for remaining
  `ProcessCommandOrder` / `HandleServerCommand` / `DoTheJobVirtual` effects.

### 2026-05-24 - Phase 1 partial: OrderWorkerStatus raw Delphi ordinal

Done:

- Fixed a parser-level `TTradeEpochCommand` parity bug found during the
  order-block audit.
- Delphi reads `Status: TOrderWorkerStatus` as a raw one-byte enum field and
  does not reject the packet when the ordinal is outside the current known
  range. `AcceptServerCommand` simply skips `FServerLatestEpoch[Status]` for an
  invalid ordinal, and `StatusPhase` returns `0`.
- Rust previously represented `OrderWorkerStatus` as a closed enum and returned
  `None` from `TradeEpochHeader::read` for unknown status bytes. That dropped
  the whole order command before `ProcessCommandOrder`-equivalent side effects
  such as snapshot-flag refresh.
- `OrderWorkerStatus` is now a raw-byte wrapper with named constants for the
  known Delphi values. Unknown ordinals are preserved and can round-trip.
- Recorded `spec_pipeline/work/хуйня.md §X.131`.

Verification:

- Added parser/state tests for unknown status ordinal preservation and
  Delphi-like snapshot/epoch-index behavior.

Still not done:

- Continue line-by-line reverse-equivalence for remaining
  `ProcessCommandOrder` / `HandleServerCommand` / `DoTheJobVirtual` effects.

### 2026-05-24 - Phase 1 partial: order-side enum raw ordinals

Done:

- Extended the same Delphi raw-ordinal rule to order-side enum fields:
  `OrderType`, `FixedPosition`, `MoveAllCmdType`, `MoveAllBuysCmdType`, and
  `ReplaceMultiKind`.
- Delphi reads and writes these fields as raw enum bytes inside
  `TOrderReplace*`, `TMoveAll*`, `TOrderTracePoint`, and
  `TBulkReplaceNotify`; unknown ordinals are not a parser-level packet drop.
- Rust now keeps those bytes in raw wrappers with named constants for known
  values. Existing public calls still use `OrderType::Buy`,
  `MoveAllCmdType::PriceZone`, etc.; parser paths can preserve unknown bytes.
- For state side-selection, only `OrderType::Buy` maps to the buy side, so
  unknown `OrderType` follows Delphi's `if = O_BUY then buy else sell` shape.
- Recorded `spec_pipeline/work/хуйня.md §X.132`.

Verification:

- Added parser tests proving unknown `OrderType`, `MoveKind`, and `Side`
  ordinals are preserved.
- Added state test proving unknown `OrderType` in `TBulkReplaceNotify` uses the
  sell side like Delphi.

Still not done:

- Continue line-by-line reverse-equivalence for remaining
  `ProcessCommandOrder` / `HandleServerCommand` / `DoTheJobVirtual` effects.

### 2026-05-24 - Phase 1 proof: skipped/unknown order class dispatch

Done:

- Proved the `TCommandRegistry` skipped-class path for `MPC_Order`.
- Delphi `ver > Current_Proto_CmdVer` returns skipped `TBaseTradeCommand`, and
  unknown current-version `CmdId` also falls back to `TBaseTradeCommand`.
  `ClientNewData(MPC_Order)` frees both because neither is `TBaseMarketCommand`;
  `ProcessCommandOrder` is not called and `SnapshotFlag` is not refreshed.
- Rust already had the same machine effect through `TradeCommand::Unknown`
  returning `NotApplicable` without snapshot-flag refresh.

Verification:

- Added dispatcher tests for future-version order command and unknown order
  `CmdId`; both assert no public event and no snapshot-flag refresh.

Still not done:

- Continue line-by-line reverse-equivalence for remaining
  `ProcessCommandOrder` / `HandleServerCommand` / `DoTheJobVirtual` effects.

### 2026-05-24 - Phase 1 partial: EngineMethod raw Delphi ordinal

Done:

- Extended the raw-ordinal rule to Engine API method ids.
- Delphi reads and writes `TEngineRequest.Method` and
  `TEngineResponse.Method` directly with `ms.Read(Method, SizeOf(Method))` and
  `Stream.Write(Method, SizeOf(Method))`.
- Delphi server creates `TEngineResponse(req.UID, req.Method, ...)`, so even an
  unknown/default `ErrorCode=400` response echoes the original method byte.
- Rust previously mapped unknown method bytes to `EngineMethod::None`, losing
  that wire state.
- `EngineMethod` is now a raw-byte wrapper with named constants for known
  Delphi values. Existing public calls still use `EngineMethod::BaseCheck`,
  etc.; parser paths preserve unknown bytes.
- Recorded `spec_pipeline/work/хуйня.md §X.133`.

Verification:

- Added tests proving unknown method ordinal preservation in
  `EngineMethod::from_byte` and full `TEngineResponse` parsing.

Still not done:

- Continue line-by-line reverse-equivalence for remaining
  `ProcessCommandOrder` / `HandleServerCommand` / `DoTheJobVirtual` effects.

### 2026-05-24 - Phase 1 partial: Command raw Delphi ordinal

Done:

- Extended raw-ordinal parity to the outer MoonProto channel byte.
- Delphi stores `TMoonProtoCommand` as a one-byte enum in packet headers and
  `GetRealCommand(cmd)` returns `TMoonProtoCommand(Ord(cmd) and $7F)`: only the
  compressed flag is stripped, unknown ordinals are preserved.
- Rust previously mapped unknown command bytes to `Command::None`.
- `Command` is now a raw-byte wrapper with named constants for known `MPC_*`
  values; `from_byte` strips bit 7 and preserves the remaining ordinal.
- Recorded `spec_pipeline/work/хуйня.md §X.134`.

Verification:

- Added protocol tests for unknown command preservation and compressed-flag
  stripping.
- Added dispatcher test proving `Event::Raw` carries the unknown command byte.

Still not done:

- Continue line-by-line reverse-equivalence for remaining
  `ProcessCommandOrder` / `HandleServerCommand` / `DoTheJobVirtual` effects.

### 2026-05-24 - Phase 1 partial: TOrderNotFound immediate effects

Done:

- Fixed `TOrderNotFound` immediate state parity.
- Delphi `TMoonProtoNetClient.ProcessCommandOrder(TOrderNotFound)` sets only
  `Worker.CancellRequest := true` and `Worker.ServerForcedRemove := true`,
  frees the command, and exits.
- Delphi closes/cancels the compact buy/sell records later in
  `BOrderWorker.DoTheJobVirtual.finally`, after the worker loop exits.
- Rust previously rewrote `buy_order`/`sell_order` and cleared replace flags
  immediately inside `Orders::apply(OrderNotFound)`.
- Rust now leaves compact orders and replace flags unchanged at receive time,
  and only sets the immediate cancel/server-forced flags.
- Recorded `spec_pipeline/work/хуйня.md §X.135`.

Verification:

- Updated the `OrderNotFound` regression test to prove compact order fields and
  replace flags are unchanged after receive, while `cancel_request` and
  `server_forced_remove` are set.

Still not done:

- Continue line-by-line reverse-equivalence for remaining
  `ProcessCommandOrder` / `HandleServerCommand` / `DoTheJobVirtual` effects.

### 2026-05-24 - Phase 1 partial: existing TOrderStatus worker fields

Done:

- Fixed another full `TOrderStatus` parity bug.
- Delphi uses `TOrderStatus.StratID`, `IsShort`, `EmulatorMode`, route/market
  identity, and related worker-level fields only on the new-worker path
  (`ProcessCommandOrder` create branch + `OnMServerOrder`).
- Existing-worker `BOrderWorker.HandleServerCommand(TOrderStatus)` applies
  buy/sell compact records, stops, `ImmuneForClicks`, status, and price side
  effects; it does not rewrite `FIsShort`, emulator mode, strategy, route, or
  cache/source flags.
- Rust previously rewrote `market_name`, `currency`, `platform`, `strat_id`,
  `is_short`, `db_id`, `from_cache`, and `emulator_mode` on every full status.
- Rust now treats those fields as set-on-create only. Existing full statuses
  still apply compact orders, stops, immune flag, status, and price side
  effects.
- Recorded `spec_pipeline/work/хуйня.md §X.136`.

Verification:

- Added a regression test where the second full `TOrderStatus` changes every
  worker-level field but only compact/stops/immune/status effects apply.

Still not done:

- Continue line-by-line reverse-equivalence for remaining
  `ProcessCommandOrder` / `HandleServerCommand` / `DoTheJobVirtual` effects.

### 2026-05-24 - Phase 1 partial: OrderBook buyCount cap removed

Done:

- Removed a Rust-only cap in `parse_order_book_packet`.
- Delphi uses declared `buyCount` when reading orderbook full/diff payloads and
  only computes sells after advancing through all declared buy levels.
- Rust previously used `min(buy_count_raw, remaining / 8)`, which could
  reclassify missing buy bytes as sell levels on a truncated/corrupt payload.
- Rust now uses the declared buy count for valid payloads and rejects truncated
  buy sections instead of silently applying a different book split.
- Recorded `spec_pipeline/work/хуйня.md §X.122`.

Verification:

- Added parser tests for declared buy-count split and truncated buy-section
  rejection.

Still not done:

- Corrupt `TMemoryStream.Read` partial-byte exactness is still not modeled:
  Delphi can leave unread bytes in local `Single` variables. That needs a
  separate decision instead of a hidden Rust-only cap.

### 2026-05-24 - Phase 1 partial: AuthCheck DEX count guard removed

Done:

- Removed a Rust-only drop-all guard in
  `parse_auth_check_response` for the optional AuthCheck DEX tail.
- Delphi reads `cnt: Byte`, allocates `KnownDexes`, and loops through
  `THLDexInfo` records with `TMemoryStream.Read`; there is no precheck that
  `cnt * SizeOf(THLDexInfo)` fits the remaining stream.
- 2026-05-26 follow-up: Rust now mirrors the deterministic part of that Delphi
  loop: declared `cnt` creates `cnt` zero-filled records, partial bytes
  overwrite the current `THLDexInfo`, and consumed partial bytes are not
  reinterpreted as `HLDexMarket`.
- 2026-05-26 follow-up: `RecvdMaxPayload` now follows Delphi `ReadInt`
  tail semantics too: if optional payload has 1-3 bytes after the mandatory
  fields, Rust keeps those low bytes and zero-fills the missing tail.
- Rust does not reject the whole AuthCheck response when the optional DEX tail
  is truncated.
- Recorded `spec_pipeline/work/хуйня.md §X.117`.

Verification:

- Added a parser test for declared count larger than the complete DEX records
  present in the payload.
- Added a parser test proving a partial `THLDexInfo` byte is consumed as record
  data and not reused as `HLDexMarket`.
- Added a parser test for partial optional `RecvdMaxPayload`.
- `cargo test engine_api --lib --quiet` OK: 42 tests.
- `cargo fmt --all -- --check` OK.
- `cargo test --lib --quiet` OK: 754 tests.
- `cargo check --examples --quiet` OK.
- `git diff --check` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 23.60s`, `ParseFailed=0`.

Remaining corrupt-tail decision:

- `cfg.HLDexMarket := resp.ReadByte` at EOF returns an uninitialized Delphi
  scalar `Result`. Rust exposes `None` for the absent scalar instead of
  inventing undefined stack bytes.

### 2026-05-24 - Phase 1 partial: market Engine API count precheck removed

Done:

- Removed the Rust-only count/remaining precheck from market Engine API
  response parsing.
- Delphi `GetMarketsList`, `GetMarketsIndexes`, `UpdateMarketsList`, and
  `CheckBinanceTags` read counts with `resp.ReadInt` and then enter the item
  loops. They do not reject the whole response just because
  `count * estimated_item_size` is larger than the bytes currently remaining.
- Rust `EngineStreamReader::read_count()` now only reads the signed count and
  rejects negative counts. Allocation sizing moved to
  `bounded_count_capacity`, which affects `Vec` capacity only and not parser
  acceptance.
- Recorded `spec_pipeline/work/хуйня.md §X.137`.

Verification:

- Added a unit test proving a declared count is preserved even when the
  estimated item bytes do not fit the remaining payload.

Still not done:

- Exact Delphi partial-apply semantics are still open for market APIs. Delphi
  mutates some market state while parsing and can leave already-read items
  applied before a later read exception. Rust still parses a complete response
  first and applies after parse success. This is a separate parity red flag,
  not an accepted deviation.

### 2026-05-24 - Phase 1 partial: unknown CorrMarket prices ignored

Done:

- Fixed a valid-packet market state mismatch in `UpdateMarketsList`.
- Delphi reads each CorrMarket price, then applies it only if
  `Markets.GetCorrMarket(MName)` returns a known corr market.
- Rust previously inserted every incoming corr price into `corr_prices`, even
  for names absent from `corr_markets`.
- Rust now ignores unknown corr price names and preserves merge semantics for
  known names.
- Recorded `spec_pipeline/work/хуйня.md §X.138`.

Verification:

- Added a regression test with one known and one unknown corr price.

Still not done:

- Continue the separate partial-apply parser/state red flag for malformed
  market Engine API payloads.

### 2026-05-24 - Phase 1 partial: direct market apply for update/tags

Done:

- Closed two active-dispatcher partial-apply mismatches for market Engine API.
- Delphi `UpdateMarketsList` clears `CurrentMarkPriceFound`, then applies each
  read price row immediately; if a later CorrMarket string read fails, already
  applied prices remain.
- Delphi `CheckBinanceTags` applies each read tag immediately and clears unseen
  tags only after the loop completes; if a later string read fails, already read
  tags remain and old absent tags are not cleared.
- Rust active dispatcher now uses direct payload apply for
  `UpdateMarketsList` and `CheckBinanceTags` instead of pure parse-then-apply.
  The pure parse helpers remain low-level command helpers.
- Recorded `spec_pipeline/work/хуйня.md §X.139`.

Verification:

- Added regression tests for late corr parse error after a read price row and
  late tag parse error after a read tag.

Still not done:

- `GetMarketsList` direct active apply is handled in the next entry.

### 2026-05-24 - Phase 1 partial: direct GetMarketsList active apply

Done:

- Closed the remaining active-dispatcher market partial-apply mismatch for
  `GetMarketsList`.
- Delphi reads and applies each market inside the market loop, rebuilds
  `SrvMarkets` after that loop, and only then reads CorrMarkets. If a later
  CorrMarket string read fails, already-read markets and rebuilt indexes remain.
- Rust active dispatcher now applies `GetMarketsList` directly from payload in
  the same order. Pure `parse_markets_list_response` remains for raw
  callers/tests.
- Recorded `spec_pipeline/work/хуйня.md §X.140`.

Verification:

- Added a regression test where the market row is complete but the following
  CorrMarket string is truncated; the market and index mapping remain applied.

Still not done:

- Continue broader `GetMarketsList` post-processing parity audit for Delphi
  fields that Rust does not model directly (`ListedType`, `CheckCorrMarkets`,
  `CheckCurrencyRefMarkets`, `NewMarkets` side list).

### 2026-05-24 - Phase 1 partial: NewMarkets immediate price refresh

Done:

- Fixed the active-lib continuation after a `NewMarketFound` listing refresh.
- Delphi `Bworks.pas` calls `Engine.GetMarketsList()` for
  `Engine.NewMarketFound`, then if `Engine.NewMarkets.Count > 0` immediately
  calls `Engine.UpdateMarketsList` so newly added markets get prices.
- Rust already requested `GetMarketsList` on an unknown indexed price row, but
  after adding new markets it waited for the next periodic price refresh.
- Rust now tracks how many markets were newly added by a `NewMarketFound`
  list refresh and emits `RequestUpdateMarketsList` immediately.
- Recorded `spec_pipeline/work/хуйня.md §X.141`.

Verification:

- Added an active action regression test: unknown price row requests
  `GetMarketsList`; a successful list that adds `DOGEUSDT` queues
  `UpdateMarketsList`.

Still not done:

- The heavier Delphi `NewMarkets` follow-up actions remain under audit:
  optional full balance refresh, listing-strategy price wait, leverage/position
  setup, and UI/log side effects.

### 2026-05-24 - Phase 1 partial: v1 Market FuturesType default and ListedType helper

Done:

- Fixed a versioned market parse mismatch.
- Delphi `TMarket.CreateBase` initializes `FuturesType := BC_EMPTY` and
  `ListedType := L_Unknown`. `ReadMarketFromStream` reads `FuturesType` only
  when `resp.ver >= 2`, so v1 payloads leave `FuturesType = BC_EMPTY`.
- Rust v1 parsing previously produced `BaseCurrency::UNKNOWN`; this could make
  the derived listing state look like futures/both-listed while Delphi treats it
  as spot-only after `GetMarketsList`.
- Rust now defaults v1 `Market::futures_type` to `BaseCurrency::EMPTY`.
- Added `Market::listed_type_like_delphi()` and `ListedType` raw ordinals for
  the exact Delphi post-pass rule: `BC_EMPTY -> L_Spot`, otherwise `L_Both`.
- Recorded `spec_pipeline/work/хуйня.md §X.142`.

Verification:

- Added regression coverage for v1 market parsing and derived listed type.

Still not done:

- Continue broader `GetMarketsList` post-processing parity audit:
  `CheckCorrMarkets`, `CheckCurrencyRefMarkets`, and heavier listing-strategy
  follow-ups remain open.

### 2026-05-24 - Phase 1 partial: CorrMarket repeated definition merge

Done:

- Fixed a Delphi `AddOrSetCorrMarket` merge mismatch.
- Delphi sets `TCorrMarket.bnMarketCurrency` only when a CorrMarket object is
  first created. On repeated `GetMarketsList` definitions for the same
  `bnMarketName`, it updates `bnTickSize` and `BaseCurrency`, but leaves the
  original `bnMarketCurrency`.
- Rust previously replaced the whole `CorrMarket` struct and could expose a
  different `bn_market_currency`.
- Rust now inserts new CorrMarkets, but for existing entries updates only
  `bn_tick_size` and `base_currency_name`.
- Recorded `spec_pipeline/work/хуйня.md §X.143`.

Verification:

- Added regression coverage for repeated CorrMarket definitions.

Still not done:

- Continue `CheckCorrMarkets` per-market BTC-correlation reference and
  `CheckCurrencyRefMarkets` / `UpdateCurrencyPrices` parity audit.

### 2026-05-24 - Phase 1 partial: CheckCorrMarkets and base currency refs

Done:

- Modeled the Delphi market-state post-processing that follows successful
  `GetMarketsList` / `UpdateMarketsList`.
- Delphi `CheckCorrMarkets` sets each market's `refBTCMarket` by replacing
  `cfg.Currency` in `bnMarketName` with `BTC` and looking up CorrMarkets when
  `cfg.BaseCurrency <> BC_BTC`. Rust now stores the same observable relation as
  `MarketsState.ref_btc_corr_markets` and exposes
  `MarketsState::ref_btc_corr_market`.
- Delphi `AddOrSetCorrMarket` creates `BaseCurDict` entries for CorrMarket
  base currencies. Rust now keeps `base_currency_prices`.
- Delphi `CheckCurrencyRefMarkets` assigns direct/reverse market and CorrMarket
  references without clearing old ones. Rust mirrors this with market names
  instead of pointers.
- Delphi `UpdateMarketsList` ends with `Markets.UpdateCurrencyPrices`. Rust now
  refreshes `BaseCurrencyPrice.last_price` at the same successful-protocol
  position using the same priority chain.

Verification:

- Added regression coverage for `refBTCMarket` derivation, BTC-base skip, direct
  USDT market base price, and CorrMarket fallback base price.

Still not done:

- Continue heavier `NewMarkets` listing-strategy follow-ups and any remaining
  `UpdateMarketsList` internal-market-field parity that is relevant to active
  library public state.

### 2026-05-24 - Phase 1 partial: UpdateMarketsList price-derived fields

Done:

- Modeled the Delphi price-derived market fields updated by
  `TMoonProtoEngine.UpdateMarketsList`.
- Delphi assigns `LastBid := Bid`, `LastAsk := Ask`,
  `pLast := (Bid + Ask) / 2`, and
  `MinLotSize := Max(Max(bnStepSize, bnminQty) * pLast, bnMinNotional)` in the
  same branch that applies one price row.
- Rust `MarketPrice` now exposes `last_bid`, `last_ask`, `p_last`, and
  `min_lot_size` and updates them in that same price-row branch.
- Recorded `spec_pipeline/work/хуйня.md §X.145`.

Verification:

- Added regression coverage for the Delphi `pLast` / `MinLotSize` formula.

Still not done:

- Continue heavier `NewMarkets` listing-strategy follow-ups and remaining
  active-lib public-state parity.
- Do not pull the broad Delphi market analytics tail into the active library by
  inertia. `TMarket.Emulating`, `SetEmuMinPrice` / `SetEmuMaxPrice`,
  `m.AddFrom` internals, weighted/avg price, bid/ask EMA, `HistoryPrice`,
  1m/5m avg, coin deltas, `LastPriceEMA`, hourly values, drop detection,
  `PriceZeroFlag`, resize tasks, history/detection, and `Markets.SetDelta500`
  are deferred to the final "keep or remove" pass.

### 2026-05-24 - Phase 1 check: NewMarkets heavier follow-ups classification

Checked:

- `Bworks.pas` after `Engine.NewMarkets.Count > 0` can call
  `Engine.GetMarketsBalanceFull`, sleep+retry `Engine.UpdateMarketsList` for
  active listing strategies, `Engine.GetBracketsInfo`, `ChangePositionType`,
  and `SetLeverage`.
- In the current `MoonProtoEngine.pas`, `GetMarketsBalanceFull` is a no-op
  (`Result := true`) and does not send `TRequestBalanceRefresh`.
- `GetBracketsInfo` is not a MoonProto Engine API method; `TMoonProtoEngine`
  inherits the base `TMarketEngine.GetBracketsInfo`, which returns `true`.
- `ChangePositionType` and `SetLeverage` are real synchronous MoonProto Engine
  API wrappers, but the listing calls are Bworks trading automation driven by
  user config (`AutoManageLev`, platform, futures mode), not automatic
  active-lib state maintenance.
- The sleep+retry price wait is gated by `strats.IsThereListingStrat`, which
  uses Delphi `sg.Active`. `Active` is not a raw snapshot field; Delphi derives
  it from `Checked`, `CanAutoBuy`, `RunDetectOnKernel`, current MoonProto mode,
  and local UI start semantics.

Conclusion:

- The active-lib protocol/state fix for NewMarkets remains the already-modeled
  immediate `UpdateMarketsList` after a listing refresh adds new markets.
- Do not add Rust-only automatic balance refresh, brackets fetch, leverage, or
  position-type changes from this Bworks block.
- Do not guess listing-strategy extra sleep/retry from `checked` alone; it needs
  a separate exact `sg.Active` model before any strategy-aware automation can be
  ported.

### 2026-05-24 - Phase 1 partial: exact strategy active predicates

Done:

- Added explicit read helpers for Delphi `TStratForm.CheckActive` /
  `bStartCheckedClick` semantics.
- `StrategySnapshot::active_like_delphi(mode)` requires the caller to choose
  `ActiveClient`, `UsingMoonProto`, or `Standalone`, so Rust code cannot silently
  collapse `Checked` into `Active`.
- `StrategySnapshot::can_auto_buy_like_delphi` mirrors
  `TStrategy.CanAutoBuy`: `(AutoBuy or StrategyKind = sk_MoonShot) and
  StrategyKind <> sk_Manual`.
- `StratsState::is_there_listing_strat_like_delphi` and
  `is_there_listing_sell_like_delphi` mirror `TStrategies.IsThereListingStrat`
  and `IsThereListingSell`, including the non-futures MoonShot/MoonHook short
  fallback.

Verification:

- Added regression coverage for ActiveClient vs UsingMoonProto active split,
  MoonShot `CanAutoBuy`, `RunDetectOnKernel`, NewListing `SellFromAsset`, and
  the spot-only short fallback.

Still not done:

- These helpers are intentionally read-only. No automatic listing sleep/retry or
  trading automation is enabled until a separate exact Delphi-owned action path
  is proven.

### 2026-05-24 - Phase 1 partial: UpdateMarketsList funding mutates Market

Done:

- Fixed a public state mismatch in `UpdateMarketsList`.
- Delphi updates `m.FundingRate` and `m.FundingTime` on the `TMarket` object
  itself when `HasFunding` is true.
- Rust previously updated only `MarketPrice.funding_rate/funding_time`, leaving
  `Market::funding_rate/funding_time` stale for API readers.
- Rust now mutates both `Market` and `MarketPrice` in the same price-row branch.

Verification:

- Added regression coverage that `Market` and `MarketPrice` funding fields move
  together after a funding-bearing price update.

### 2026-05-24 - Phase 1 partial: UpdateMarketsList clears MarkPriceFound before first read

Done:

- Fixed a malformed-payload machine-effect mismatch in the active
  `UpdateMarketsList` apply path.
- Delphi clears every market's `CurrentMarkPriceFound := false` before reading
  `HasFunding` and `Count` from the response stream. If the payload is truncated
  at that point, the clear already happened.
- Rust direct payload apply previously read `send_funding` and `count` first and
  cleared `mark_price_found` only after those reads succeeded.
- Rust now clears `MarketPrice.mark_price_found` before the first payload read in
  `apply_markets_prices_payload_like_delphi`.

Verification:

- Added regression coverage that an empty direct `UpdateMarketsList` payload
  returns `None` but still clears all existing `mark_price_found` flags, matching
  Delphi's clear-before-read order.

### 2026-05-24 - Phase 1 partial: EngineResponse carries real response version

Done:

- Fixed an active-dispatcher market-list version mismatch.
- Delphi `TEngineResponse` inherits `TBaseCommand.ver`, and
  `ReadMarketFromStream` reads `FuturesType` only when `resp.ver >= 2`.
- Rust parsed the response header but discarded `ver`; active dispatcher passed
  a constant `2` into `apply_markets_list_payload_like_delphi`.
- `EngineResponse` now exposes `ver`, `parse_engine_response` preserves it from
  the wire header, and active `GetMarketsList` apply passes `resp.ver`.
- API docs now document `EngineResponse::ver`.

Verification:

- Added parser coverage that `parse_engine_response` keeps `ver`.
- Added dispatcher regression coverage for an old v1 market-list payload without
  a `FuturesType` byte: Rust now applies it and keeps
  `Market::futures_type = BaseCurrency::EMPTY`, matching Delphi.

### 2026-05-24 - Phase 1 partial: malformed EngineResponse is dropped

Done:

- Fixed a parser mismatch where malformed `TEngineResponse` tails could become a
  valid Rust `EngineResponse` with empty `data`.
- Delphi reads `ErrorMsg` through `ReadStringFromStreamUtf8`, which uses
  `ReadBuffer`; truncation raises and `DataReadInt` catches/logs the read error
  instead of delivering a response.
- Positive `DataSize` declares an exact body. A shorter body is not a valid
  empty response and must not mutate active state.
- Rust now returns `None` for truncated `ErrorMsg`, missing `IsCompressed`,
  missing `DataSize`, and declared positive `DataSize` larger than the remaining
  bytes. Negative or zero `DataSize` still yields empty `data`, matching
  Delphi's `if sz > 0 then ...` branch.

Verification:

- Added parser regression coverage for every malformed-tail case.

### 2026-05-24 - Phase 1 partial: AuthCheck payload is retained

Done:

- Fixed an active-lib mismatch where init treated `AuthCheck` as only a success
  boolean and discarded the per-account payload.
- Delphi `TMoonProtoEngine.AuthCheck` stores `BinanceAccountID`, `BTCAddress`,
  `AccountID`, sub-account state, `RecvdMaxPayload`, and the Hyperliquid DEX
  tail in local engine/cfg state during init.
- Rust now stores the parsed `AuthCheckResponse` in `Client::auth_info()` and
  exposes the same value through `InitResult::auth_info`.
- `Client::request_auth_check` also stores the parsed value for custom init
  flows.
- Init keeps Delphi result ordering: `resp.Success` makes AuthCheck successful;
  if the mandatory auth payload is malformed, Rust records a non-fatal parse
  note and leaves `auth_info = None` instead of failing init.

Verification:

- Added storage/getter unit coverage.
- API docs updated for the new observable AuthCheck state.
- Quick prod FireTest passed: `FIRETEST_QUICK_PASS after 24.48s`,
  `ParseFailed=0`, successful `AuthCheck` payload observed (`data_len=220`) and
  init continued through markets/indexes/update/streams.

### 2026-05-24 - Phase 1 partial: InitInt BaseCheck/AuthCheck retry branch

Done:

- Fixed an init-control mismatch: Rust failed immediately after a failed
  BaseCheck/AuthCheck block.
- Delphi `TCryptoPumpTool.InitInt` does:
  `BaseCheck; AuthCheck; if not resBool then Sleep(200); BaseCheck; AuthCheck`.
  The retry branch assigns the final result from the second AuthCheck; the
  second BaseCheck still refreshes local server identity when it succeeds.
- Rust `run_init_sequence` now mirrors that branch. The existing
  `ServerUpdateSent` BaseCheck retry remains inside the first BaseCheck call, as
  in Delphi `TMoonProtoEngine.BaseCheck`.

Verification:

- Added unit coverage that a zero-timeout init queues
  `BaseCheck, BaseCheck, AuthCheck` and returns final `AuthCheck` timeout.
- Quick prod FireTest passed after the control-flow change:
  `FIRETEST_QUICK_PASS after 24.76s`, `ParseFailed=0`.

### 2026-05-24 - Phase 1 partial: post-init MMOrders source

Done:

- Fixed post-init `TMMOrdersSubscribeCommand` source value.
- Delphi `TCryptoPumpTool.NewData` sends
  `TMMOrdersSubscribeCommand.Create(cfg.ShowHeatMap)`.
- Delphi `TMoonProtoEngine.SubscribeAllTrades` separately sends
  `Strats.HasActivityStrat or cfg.ShowHeatMap`.
- Rust no longer falls back from `InitConfig::mm_orders_subscribe` to
  `subscribe_trades` for the UI command. It uses explicit
  `mm_orders_subscribe`, then a queued `ui_mm_subscribe` intent, then `false`.

Verification:

- Added unit coverage that `subscribe_trades=Some(true)` with no MMOrders/heatmap
  intent still sends post-init `TMMOrdersSubscribeCommand(false)`.
- `cargo fmt --all --check`, `cargo test --lib --quiet` (`649 passed`), and
  `cargo check --examples --quiet` passed.
- Quick prod FireTest passed after the post-init source fix:
  `FIRETEST_QUICK_PASS after 21.01s`, `ParseFailed=0`; log observed the
  post-init MMOrders subscription command independently from all-trades.

### 2026-05-24 - Phase 1 partial: MMOrders registry does not rewrite all-trades

Done:

- Fixed the second half of the MMOrders/all-trades mismatch.
- Delphi has two distinct callers that write the same server
  `IsMMOrdersSubscribed` flag:
  `TMMOrdersSubscribeCommand.Create(...)` and
  `emk_SubscribeAllTrades.WriteBool(...)`.
- The UI command does not mutate the stored all-trades subscription parameter.
  Rust no longer lets post-init `TMMOrdersSubscribeCommand(false)` overwrite a
  prequeued `SubscribeAllTrades(want_mm=true)`.
- `ui_mm_subscribe` and post-init MMOrders update only the MMOrders intent.
  `subscribe_all_trades(want_mm)` keeps its own exact replay bool.
- If reconnect replays all-trades and a later direct MMOrders intent differs,
  Rust sends the UI MMOrders command after the all-trades request so the final
  server flag matches the latest direct intent.

Verification:

- Added coverage for the exact bug: prequeued all-trades `want_mm=true` plus
  default post-init MMOrders `false` still flushes
  `emk_SubscribeAllTrades(true)`.
- Updated registry/reconnect tests for separate all-trades and MMOrders replay,
  including the delayed reconnect subscribe path.
- `cargo fmt --all --check`, `cargo test --lib --quiet` (`651 passed`), and
  `cargo check --examples --quiet` passed.
- Quick prod FireTest passed after the registry split:
  `FIRETEST_QUICK_PASS after 22.81s`, `ParseFailed=0`.

### 2026-05-24 - Phase 1 partial: unsubscribe_all_orderbooks sends real names

Done:

- Fixed a Rust-only orderbook API behavior bug.
- Delphi `TMoonProtoEngine.DoUnsubscribeOrderBooks` exits before sending when
  the market array is empty.
- The current Delphi server also treats an empty `emk_UnsubscribeOrderBook`
  market list as `success=false` and unsubscribes nothing.
- Rust `unsubscribe_all_orderbooks()` previously cleared the local registry and
  sent `emk_UnsubscribeOrderBook` with an empty market list. That could leave
  the server still subscribed while the library believed the registry was empty.
- The helper now drains the remembered registry names and sends one batched
  unsubscribe request for those names. If the registry was already empty, it
  sends no wire packet.

Verification:

- Updated sender/client tests to require non-empty market-name counts for
  `unsubscribe_all_orderbooks()` and no packet for an empty registry.
- `cargo fmt --all --check`, `cargo test --lib --quiet` (`652 passed`), and
  `cargo check --examples --quiet` passed.

### 2026-05-24 - Phase E partial: TradesStream live market tail state

Done:

- Fixed the first direct-state part of the `MPC_TradesStream` mismatch.
- Delphi `TMoonProtoEngine.ProcessTradesStream` tracks packet gaps first, then
  applies each known market trade inline through `wsParseOrdersHistoryAll_Int`.
  For futures rows that tail sets `LastGotAllTrades` and calls
  `TMarket.SetLastTradePrices`; for spot rows it updates `LastGotSpotTrades`
  and exits before `SetLastTradePrices`.
- Rust previously maintained only `TradesState` gap/retry state and emitted an
  owned packet event; it did not mutate any market live trade tail.
- `MarketsState` now has `MarketTradeState` keyed by market name. The dispatcher
  applies the bounded Delphi tail before emitting the public applied signal:
  `last_got_all_trades_ms`, `last_got_spot_trades_ms`, `last_trade_price`,
  `last_buy_price`, `last_sell_price`, `last_trade_price_ema15`,
  `last_trade_price_ema5`, and `last_trade_was_sell`.
- Recorded `spec_pipeline/work/хуйня.md §X.156`.

Verification:

- Added regression tests for futures `SetLastTradePrices` tail and spot not
  overwriting futures tail.

Still not done:

- Zero-alloc `SectionIter` remains Phase E work and is the next concrete
  protocol/state-shape cleanup.
- The old "not needed for active-lib" wording is obsolete after the
  2026-05-25 Active Lib storage decision. The bounded tail above is closed, but
  detailed history is now Phase E2 work: `wsParseOrdersHistoryAll_Int ->
  AddTmpHOrder -> JoinHOrders` aggregation/sorting, spot/liquidation/MM retained
  histories, `HistoryPrice`, rolling volumes, mini-candle compaction, and the
  keep/remove decision for broader analytics fields.

### Final pass - keep/remove broad Delphi market analytics tail

Before declaring protocol/state parity complete, explicitly decide what to keep
and what to leave out of the active library API/state model:

- `m.Emulating`;
- `SetEmuMinPrice` / `SetEmuMaxPrice`;
- `m.AddFrom` internals: weighted/avg price, bid/ask EMA, `HistoryPrice`,
  1m/5m avg, coin deltas, `LastPriceEMA`, hourly values, drop detection,
  `PriceZeroFlag`, resize through `TThread.Queue`;
- trades/history/detection buffers beyond the bounded public trade tail;
- `Markets.SetDelta500`.

Default for this pass: fields required by the Active Lib storage contract are no
longer optional. Remaining UI-only or strategy-detection-only fields still need
an explicit keep/remove decision before final parity is declared.

### Next concrete work - zero-alloc SectionIter for TradesStream

Problem:

- Delphi `ProcessTradesStream` reads `DataStream` section-by-section and
  row-by-row. For unknown markets it skips `Count * row_size` bytes; for known
  markets it applies each row immediately.
- Rust `parse_trades_packet` currently allocates `TradesPacket.sections`, then
  allocates per-section `Vec<Trade>` / `Vec<MMOrder>` / `Vec<LiqOrder>`, then
  the dispatcher filters those vectors before `TradesState` and market state
  consume them.
- This is machine-effect equivalent for current public events, but worse for
  the strict porting method: the hot path has an artificial collect/filter layer
  where Delphi has direct stream iteration.

Target shape:

- Add a borrowed decoded payload type in `commands::trades_stream`, owning only
  the decompressed buffer when compression was used.
- Expose `SectionIter` over raw section bytes:
  - `Trades { market_index, is_spot, rows }`, where rows yield the 10-byte
    Delphi row as typed fields;
  - `MMOrders { market_index, has_taker, rows }`;
  - `LiqOrders { market_index, rows }`;
  - `WatcherFills { market_index, user, records_raw }`.
- Keep current `parse_trades_packet -> TradesPacket` as an owned low-level
  collector over `SectionIter`.
- Then switch the active dispatcher hot path to `SectionIter`: gate unknown
  markets by skipping rows like Delphi, update `TradesState` by packet header,
  update market trade tail while iterating, and build public `TradesEvent` only
  for the API surface that still needs owned data.

Verification:

- Existing `parse_trades_packet` tests must keep passing.
- Add iterator tests for every section type, truncated rows, unknown ext type,
  watcher-fill raw record length, and exact position/skip behavior.
- Quick FireTest after dispatcher switch; full FireTest at the next major gate.

### 2026-05-25 - TradesStream SectionIter first slice

Done:

- Added `DecodedTradesPacket` and borrowed `TradeSectionIter`.
- `parse_trades_packet` is now an owned low-level collector over `SectionIter`,
  so raw protocol tools can still request an owned packet when they explicitly
  parse bytes.
- `EventDispatcher` no longer does collect-all then filter. It decodes the
  packet header/sections first and only collects rows for known markets, matching
  Delphi's `FindByServerIndex` + `Position += Count * row_size` shape more
  closely.
- Added iterator tests for all current section types and truncated tail rows.

Verification:

- `cargo test --lib` OK, 662 tests.
- Quick prod FireTest OK: `FIRETEST_QUICK_PASS after 23.58s`.

### 2026-05-25 - Trades packet effect split

Done:

- Added `TradesPacketEffect`: packet-number/gap/duplicate/resend decisions now
  can be produced from `packet_num` only, before row/state application.
- Kept the low-level state parser shape: `TradesState::on_packet(TradesPacket,
  now_ms)` and `on_packet_resend(TradesPacket)` still accept owned packets but
  now return the signal/diagnostic `TradesEvent` shape.
- Switched active dispatcher live/resend paths to call the packet-header
  decision first, then apply known sections directly when an `Apply` effect
  means Delphi would process the payload.

Verification:

- `cargo test trades --lib` OK: 58 tests.
- `cargo test dispatcher_ --lib` OK: 41 tests.
- `cargo test --lib` OK: 698 tests.
- `cargo check --examples` OK.
- Quick prod FireTest OK: `FIRETEST_QUICK_PASS after 23.89s`,
  `ParseFailed=0`, `err_emu=10%`, target market tail present for `BTCUSDT`.

Observed quick CPU remains an open Phase Z / `хуйня.md` CPU red flag, not a
closed item: reader avg/max `947us/108479us`, active_dispatch avg/max
`1449us/100399us`, app_enqueue avg/max `891us/2295us`.

### 2026-05-25 - CPU red flag attribution

Added max-sample attribution to `ProtocolMetricsSnapshot`:
`reader_protocol_max_cmd/payload_len`,
`active_dispatch_max_cmd/payload_len/events/actions`, and
`app_enqueue_max_cmd/payload_len/events/mode`.

Verification:

- `cargo test --lib --quiet` OK (`698 passed`).
- `cargo check --examples --quiet` OK.
- quick FireTest debug OK: `FIRETEST_QUICK_PASS after 23.32s`.
- quick FireTest release OK: `FIRETEST_QUICK_PASS after 22.77s`.

Release quick max samples before DEVIATION #37:

- `reader max=32384us cmd=WhoAreYou payload=92` — caused by Delphi-parity
  blocking `Sleep(32)` between duplicate `ImFriend`
  (`MoonProtoUDPClient.pas:433-435`), not CPU work.
- `active_dispatch max=24040us cmd=Strat payload=44460 events=1` — real
  boundary mismatch: Delphi queues `ProcessStratCommand` through
  `TThread.Queue`, Rust still decodes/applies the strategy snapshot inside
  active dispatch.
- `app_enqueue max=2341us cmd=OrderBook payload=112 mode=state` — Rust-only
  `run_with_dispatcher_state` snapshot clone inside protocol loop.

Conclusion at that point: CPU red flag was still open, but localized. Next fixes were:
move `MPC_Strat` heavy apply to the `AppQueue`/worker boundary, remove or make
cheap the state-callback snapshot clone, and split reader wall-clock blocking
from actual CPU while deciding how to preserve duplicate `ImFriend` semantics
without starving the single-owner loop.

Follow-up optimization:

- `StratsState` live apply now moves decoded `StrategySnapshot` values into
  state instead of cloning every snapshot after parsing.
- Targeted tests: `cargo test strats --lib --quiet` OK (`22 passed`) and
  `cargo test dispatcher_routes_strat_to_strats_state --lib --quiet` OK.
- quick FireTest release after this change: `FIRETEST_QUICK_PASS after 23.56s`;
  `active_dispatch max` fell from `24040us cmd=Strat payload=44460` to
  `3229us cmd=API payload=44050`. This proves the snapshot clone was real
  CPU waste, but the CPU red flag remains open for large init API parsing/apply
  and state snapshot clone. `WhoAreYou` blocking sleep is handled by
  DEVIATION #37 below.

Handshake follow-up:

- DEVIATION #37 approved: keep duplicate `MPC_ImFriend` packet but remove
  Delphi's blocking `Sleep(32)` between the two sends in Rust.
- Rationale: the duplicate can still cover loss of the first final handshake
  datagram; loss of `MPC_Fine` is recovered by normal HelloAgain/reconnect
  logic, because a duplicate `ImFriend` with the same `MixTS` does not make the
  server send `Fine` again after the first one was accepted.
- This removes the intentional 32ms wall-clock block from reader CPU metrics
  instead of teaching FireTest to ignore it.

Dispatcher-worker and Strat follow-up:

- `run_with_dispatcher` / `run_with_dispatcher_state` now hand decoded domain
  payloads to a dispatcher worker. The protocol owner enqueues the work item and
  continues ACK/retry/send progress; active parsing/apply, actions, event
  enqueue, and state-callback snapshot building happen in the worker.
- `StrategyFields` changed from per-strategy `HashMap<Arc<str>, FieldValue>` to
  a dense vector container with the same public operations used by the API
  (`new`, `insert`, `get`, `contains_key`, `iter`, `len`). This matches the
  Delphi serializer's ordered field stream more closely and removes hash work
  from large strategy snapshots.
- The hot deserializer path appends decoded fields directly instead of calling
  the public replacement-style `insert`. Delphi writes each RTTI field at most
  once per strategy; avoiding per-field duplicate scans removes the remaining
  O(n^2) parser waste for 762-strategy live snapshots.
- Follow-up CPU cleanup: Sliced completion now keeps the already-owned
  assembled payload instead of cloning it through the borrowed DataRead path;
  `StratsState::create_folders_for_path` returns early when the full folder is
  already known, avoiding repeated lowercase/split work on repeated strategy
  paths.
- Verification after the dense fields change:
  - `cargo test strategy --lib --quiet` OK (`29 passed`);
  - `cargo test --lib --quiet` OK (`698 passed`);
  - quick FireTest release OK: `FIRETEST_QUICK_PASS after 22.52s`.
  - full FireTest release OK (`178s`): Session A received full candles snapshot
    (`zipped=2026051`, `markets=664`, `candles=217500`), both sessions
    `parse_failed=0`, strategy rows `762`.
- Latest quick CPU after this step:
  - after the follow-up cleanup quick FireTest still passes after `23.47s`;
  - `reader max=3921us max_src=Sliced(17) payload=1442`;
  - `writer_cpu max=148us`;
  - `active_dispatch max=3783us max_src=Strat(30) payload=44459`;
  - `app_enqueue max=2217us max_src=TradesStream(33) payload=73 mode=state`.
- Follow-up worker-boundary fix: `run_until_response`, `connect_and_init`,
  `run_init_sequence`, and the public one-shot wait helpers now pump domain
  apply through the dispatcher worker instead of the old inline queued
  dispatcher path. `run_until_response` keeps one worker alive for the whole
  wait and uses a FIFO `Barrier` work item before returning a receiver value, so
  dispatcher state/events before that response are already applied. The old
  inline `RunMode::Dispatcher` path is `cfg(test)` only.
- Quick prod FireTest after this boundary fix:
  - `FIRETEST_QUICK_PASS after 22.83s`;
  - `reader max=1245us max_src=Sliced(17) payload=1442`;
  - `writer_cpu max=153us`;
  - `active_dispatch max=2813us max_src=API(31) payload=44031`;
  - `app_enqueue max=2021us max_src=TradesStream(33) payload=50 mode=state`.
- Follow-up API pending fix: in dispatcher-worker modes, registered Engine API
  receivers are now fulfilled by the dispatcher worker after the worker parsed
  the same `Event::EngineResponse`; the reader only does cheap UID/meta checks
  plus existing candles aggregation. This removes duplicate full
  `EngineResponse` parse/decompress from protocol recv and keeps heavy API
  payload work on the worker. The raw `Client::run` path still dispatches
  pending receivers from DataReadInt because it has no active dispatcher worker.
- Quick prod FireTest after pending moved to worker:
  - `FIRETEST_QUICK_PASS after 23.88s`;
  - `reader max=688us max_src=Sliced(17) payload=1442`;
  - `writer_cpu max=124us`;
  - `active_dispatch max=3010us max_src=API(31) payload=44025`;
  - `app_enqueue max=2041us max_src=TradesStream(33) payload=24 mode=state`.
- Result: the concrete `Strat` slow-parser boundary red flag is closed for the
  measured live snapshot path: it is worker-side, no longer max, and no longer
  blocks protocol recv in either long-running dispatcher mode or sync init/wait
  helpers. The protocol recv CPU red flag is also below 1ms in this quick run.
  The broader CPU red flag remains open for large worker-side API market
  parsing/apply and state snapshot enqueue cost.
- Follow-up Strat parser cleanup: `StratsState` now caches the live
  `TStratSchema` field-name -> TypeID map behind `Arc` when schema is applied.
  Live `TStratSnapshot` decode reuses that cache instead of rebuilding a
  477-field map for every snapshot. `StratsState` upsert now uses one
  dictionary entry lookup per strategy instead of `contains_key` + `entry`.
- Quick prod FireTest after the cache/apply cleanup:
  - `FIRETEST_QUICK_PASS after 23.96s`;
  - `reader max=692us max_src=Sliced(17) payload=1442`;
  - `writer_cpu max=158us`;
  - `active_dispatch max=2648us max_src=API(31) payload=44028`;
  - `app_enqueue max=1807us max_src=TradesStream(33) payload=50 mode=state`.
  This run did not make `Strat` the active-dispatch max; the remaining measured
  max is worker-side market API apply, not protocol recv.

Finished small Strat optimization step:

- Local `TStratSnapshotRequest` replies now use a cached serialized
  `TStrategySerializer` payload in `StratsState`. The cache is invalidated when
  schema/snapshot/checked state changes. This keeps the wire reply identical for
  unchanged state, but avoids rebuilding and raw-deflating all local strategies
  for every small request command.
- The active-only snapshot apply path now streams decoded strategies directly
  into `StratsState` instead of first building a public `StrategyBatch` vector.
  The public parser still returns `names/paths/strategies`; live apply does not
  need those public containers. Raw-deflate output is pre-sized from a bounded
  capacity hint to avoid repeated realloc/copy while inflating the known
  serializer stream.
- Quick prod FireTest after this finished step:
  - `FIRETEST_QUICK_PASS after 22.03s`;
  - `reader max=787us max_src=Sliced(17) payload=1442`;
  - `writer_cpu max=168us`;
  - `active_dispatch max=4196us max_src=Strat(30) payload=44462`;
  - `app_enqueue max=3517us max_src=LogMsg(27) payload=84 mode=state`.
- Follow-up full prod FireTest after fixing no-op `TStratCheckedSync` cache
  invalidation:
  - `FIRETEST_PASS`, `finished in 175.81s`;
  - full candles snapshot under `err_emu=10%` completed after `3.01s`;
  - Client A CPU: `reader max=698us`, `writer_cpu max=699us`,
    `active_dispatch max=3419us max_src=Strat(30) payload=44462`,
    `app_enqueue max=3215us max_src=TradesStream(33) mode=state`;
  - Client B CPU: `reader max=551us`, `writer_cpu max=120us`,
    `active_dispatch max=2245us max_src=API(31) payload=44042`,
    `app_enqueue max=2340us max_src=TradesStream(33) mode=state`.
  The previous full-run red flag where a small `TStratSnapshotRequest`
  (`payload=11`) triggered about `19ms` of full snapshot rebuild is closed:
  no-op checked sync no longer drops the cached serialized reply payload.
- Current accepted boundary: the remaining `~3.5-4.2ms` worker-side
  `TStrategySerializer` parse/apply cost is not protocol recv work. The measured
  live payload is about `44KB` compressed, `~1.5MB` after raw-deflate, `762`
  strategies and about `58K` fields.
- Phase Z must build a small Delphi console benchmark and a Rust benchmark that
  both read the exact same saved `TStratSnapshot.Data` file from FireTest/stress
  dumps and measure only serializer parse/apply, with no UDP, Sliced, callbacks,
  logging, or active-session machinery. Compare pure Delphi
  `TStrategySerializer.LoadStrategiesFromStream`/`ApplyStratSnapshot` timing
  against Rust `parse_strategy_batch*`/`StratsState` apply. Only after that
  decide whether parser changes are required.

### 2026-05-26 - CPU attribution detail for Phase Z

Done:

- FireTest CPU labels now record the Engine API method for worker-side
  `active_dispatch` and `app_enqueue` max samples. `API(31) payload=...` was
  too coarse for Phase Z; it now prints e.g.
  `API(31) method=GetMarketsList(3) payload=...`.
- FireTest now saves every live `TStratSnapshot.Data` as a raw file under
  `target/firetest_strategy_raw/` by default and prints the path in the
  `SERVER->... Strat Snapshot...` log line. This gives Phase Z one exact payload
  file for the Delphi console benchmark and the Rust benchmark.
- Quick/full initial health now requires at least one live `TStratSnapshot`
  event, not only `TStratSchema` plus trades/orderbook. This keeps the quick
  gate aligned with the "full live parse of what the server sends" rule and
  makes the raw strategy payload dump non-optional on normal live servers.
- Added an ignored Rust-side Phase Z benchmark:
  `cargo test --release bench_firetest_strategy_snapshot_payload --lib -- --ignored --nocapture`.
  It reads `MOONPROTO_STRAT_SNAPSHOT_BENCH` or the newest
  `target/firetest_strategy_raw/*.bin` and measures parse-only,
  active in-place cold apply, and active in-place warm apply on the exact live
  snapshot payload.
- This is diagnostics only. It does not affect protocol send/retry/drop logic.

Current quick prod FireTest:

- `FIRETEST_QUICK_PASS after 26.15s`, `ParseFailed=0`.
- Protocol-owned hot path is below 1ms in this run:
  `reader max=780us`, `writer_cpu max=146us`, `send_max=109us`.
- Remaining >1ms samples are worker/app-side:
  `active_dispatch max=4160us max_src=Strat(30) payload=44463`,
  `app_enqueue max=2236us max_src=Strat(30) payload=44463 mode=state`.
- Interpretation: current red flag is no longer UDP protocol ACK/retry
  starvation in quick profile. It is still Phase Z work for worker-side
  `TStrategySerializer` parse/apply and `run_with_dispatcher_state` snapshot
  clone cost.
- Rust pure serializer/apply benchmark on live payload after the raw-dump gate:
  payload `44431` bytes, `762` strategies, `500` release iterations:
  parse-only avg/max `2651us/4153us`, active cold apply avg/max
  `2569us/3914us`, active warm apply avg/max `2034us/3283us`.
  This confirmed the remaining `Strat` CPU red flag is real parser/apply work,
  not UDP/Sliced/logging noise.

Verification:

- `cargo fmt --all -- --check` OK.
- `cargo test --lib --quiet` OK: 759 tests.
- `cargo check --examples --quiet` OK.
- `cargo test --test fire_test --no-run --quiet` OK.
- Quick prod FireTest OK as above.

### 2026-05-26 - Delphi StrategySerializer benchmark

Done:

- Added `tools/delphi/StratSerializerBench.dpr`, a local Delphi console
  benchmark that reads the exact same raw `TStratSnapshot.Data` dump written by
  FireTest.
- The benchmark compiles the current Delphi `StrategySerializer.pas` and
  `Strategies.pas`, with the rest resolved from the existing MoonBot DCUs. This
  is diagnostics only; it does not change the Delphi server/client source.
- Important standalone-init finding: `TStrategy.Create -> rebuildProps` reads
  global `Strategies.Strats`. A standalone bench with that global left `nil`
  crashes before payload parsing. The bench now points the global at the same
  `TStrategies` container being loaded, matching live MoonBot machine effect.

Measured on the same `44431` byte / `762` strategy FireTest payload:

- Delphi `LoadStrategiesFromStream` cold new list, 500 optimized iterations:
  avg/max `45208us/56096us`.
- Delphi warm same list, 500 optimized iterations: avg/max `3536us/4491us`.
- Rust release benchmark rerun on the same payload, 500 iterations:
  parse-only avg/max `2643us/4604us`, cold apply avg/max `2593us/4686us`,
  warm apply avg/max `1981us/3599us`.

Interpretation:

- Rust is not slower than Delphi on pure warm snapshot apply; the live
  `active_dispatch` `~3-4ms` samples are consistent with real worker-side
  serializer/apply work plus normal scheduler/log/test noise.
- The protocol recv CPU red flag remains closed for the measured quick/full
  runs (`reader` below `1ms`). The open Phase Z work is broader CPU cleanup,
  especially worker/app-side API market parsing/apply and state snapshot enqueue
  cost.

Verification:

- Delphi bench compile OK with DCC64 12.2, optimized build.
- Delphi bench run OK on FireTest raw snapshot.
- Rust benchmark command OK:
  `cargo test --release bench_firetest_strategy_snapshot_payload --lib -- --ignored --nocapture`.
- `cargo fmt --all -- --check` OK.
- `cargo test --lib --quiet` OK: 759 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- `cargo test --test fire_test --no-run --quiet` OK.
- Quick prod FireTest release OK: `FIRETEST_QUICK_PASS after 26.64s`,
  `ParseFailed=0`; CPU `reader max=660us`, `writer_cpu max=136us`,
  `active_dispatch max=3508us max_src=Strat(30)`, `app_enqueue max=2604us`.

### 2026-05-25 - Trades market tail moved before owned event dependency

Done:

- `MarketsState` now exposes a row-level `apply_trade_tail_row_like_delphi`.
- Active dispatcher applies futures/spot market tail while it collects known
  trade rows from borrowed `DecodedTradesPacket` sections.
- `TradesEvent::Applied { packet_num, base_time }` is now a lightweight
  signal/diagnostic event. It is no longer the source of market-tail mutation
  and no longer carries an owned `TradesPacket`.

Verification:

- `cargo test trades --lib` OK: 58 tests.
- `cargo test dispatcher_ --lib` OK: 41 tests.
- `cargo test --lib` OK: 698 tests.
- `cargo check --examples` OK.

Follow-up after this step:

- Retained-history worker batches now come from the same borrowed
  `DecodedTradesPacket`/`TradeSectionIter` walk. Active retained storage no
  longer scans public `Event::Trade` payloads and no longer depends on public
  owned event construction.
- Public `TradesEvent` is signal/diagnostic only for active mode. Low-level
  `parse_trades_packet` / `TradeSection` remain available for raw protocol
  tools, but active callbacks read rows from market state and retained
  `SeqRing` readers.
- Verification after the retained-history change:
  `cargo test active_dispatch_queues_trades_into_history_worker_without_direct_store_write --lib`,
  `cargo test trades --lib`, and `cargo test dispatcher_ --lib` all pass.

### 2026-05-25 - TradesState FindBucketForPacket structural parity

Done:

- Re-checked Delphi `MoonProtoEngine.pas:ResetGapBuckets`,
  `FindBucketForPacket`, and `ProcessTradesStream` against Rust
  `state::trades`.
- Rust no longer keeps the adjacent-bucket extend logic inline inside
  `on_packet_header`. It now has one `find_bucket_for_packet(... want_extend
  ...)` block matching Delphi's method shape: in-range packet returns the
  bucket, adjacent gap may extend the bucket only while `RetryCount < 2`,
  one-shot retry refund is applied inside the method, and the extend path
  updates `last_packet_num` inside the method like Delphi.
- `reset_gap_buckets(now_ms)` now mirrors Delphi `ResetGapBuckets` by clearing
  buckets, setting `last_packet_time_ms`, and resetting `trades_started`.
  Server-token resets call `full_reset_at(now_ms)` so the reset timestamp is the
  packet-processing time, not an artificial zero.

Verification:

- `cargo test trades --lib` OK: 58 tests.
- `cargo test dispatcher_ --lib` OK: 41 tests.
- `cargo test --lib` OK: 700 tests.
- `cargo check --examples` OK.
- Quick prod FireTest OK: `FIRETEST_QUICK_PASS after 23.85s`;
  `reader max=641us`, `writer_cpu max=129us`,
  `active_dispatch max=3157us max_src=Strat(30) payload=44464`,
  `app_enqueue max=2341us max_src=TradesStream(33) mode=state`.

### 2026-05-25 - TradesStream truncated row tail does not become a fake section

Done:

- Re-checked Delphi `ProcessTradesStream` section loop. Normal trades,
  MMOrders, and LiqOrders read exactly the declared `Count` rows. If the stream
  is malformed/truncated inside that declared row range, Delphi reaches the end
  of the stream while reading those rows and does not reinterpret the remaining
  row bytes as a new section header.
- Rust `TradeSectionIter` previously yielded only complete rows but left a
  multi-byte malformed tail available to the next iterator step. A 3-byte tail
  after one complete row could become a fake empty section.
- `TradeSectionIter` now consumes the declared row byte range up to stream end:
  it still yields only complete typed rows, but marks the iterator done when the
  declared rows are truncated.

Verification:

- Added
  `section_iter_consumes_truncated_declared_rows_instead_of_reparsing_tail`.
- `cargo test section_iter --lib` OK: 3 tests.

### 2026-05-26 - TradesStream SectionIter malformed-section proof edges

Done:

- Added the remaining SectionIter proof tests promised in the Phase E target:
  unknown extended section type and truncated WatcherFills rows.
- Delphi `ProcessTradesStream` exits on an unknown `ExtType` and does not parse
  later bytes as another section. Rust `TradeSectionIter` now has a pinned test
  for the same stop behavior.
- Delphi reaches stream end while reading declared WatcherFills rows when the
  declared `Count * 20` bytes are truncated. Rust now has a pinned test that the
  partial watcher-fill tail is not reinterpreted as another section.

Verification:

- `cargo test section_iter --lib --quiet` OK: 5 tests.

### 2026-05-25 - Strategy schema agreed active-lib behavior

Delphi source:

- `StrategySchemaBuilder.pas` builds a raw-deflate schema blob from live
  `TStrategy` RTTI and `GetFieldPickInfo`.
- `MoonProtoStratStruct.pas` adds `TStratSchemaRequest` (CmdId=7) and
  `TStratSchema` (CmdId=8, Sliced).

Rust action:

- Added `commands::strategy_schema` parser for the schema blob: kinds, fields,
  TypeID, UI kind, layout/chapter markers, default values, visibility bitset,
  static picklists, and dynamic picklist source.
- Extended `MPC_Strat` parser/builders with CmdId 7/8.
- `StratsState` now stores the latest schema and raw blob.
- `run_init_sequence` requests schema once during Init after the domain gate
  opens and before post-init resync. Missing/malformed schema is a critical
  Init failure.
- FireTest records schema events and writes
  `target/firetest_strategy_info_<profile>.txt` with all known schema and
  decoded strategy snapshot data.

This is a user-approved active-library behavior, not a `DEVIATION.md` entry.

### 2026-05-25 - StrategySerializer now uses live schema, not Rust hardcode

Done:

- Removed the static Rust `TStrategy` field order/type/default tables from
  `commands::strategy_serializer`.
- `StrategyBatchBuilder` now requires a live `StrategySchema` for non-empty
  typed snapshots and writes fields in schema order, filtered by schema
  visibility (`GetStrategyPropMask`), schema TypeID, and schema defaults.
- `StratsState::apply_snapshot_decoded_with_mode` uses the stored schema for
  Delphi `ReadField` TypeID checks. Generic `parse_strategy_batch` remains only
  a diagnostics reader for payloads parsed without schema.
- If the server sends `TStratSnapshotRequest` before schema exists and the local
  strategy list is non-empty, active-lib stores the pending request, sends
  `TStratSchemaRequest`, and answers after `SchemaApplied`.
- Empty strategy-list payloads still serialize without schema because Delphi
  `FinalizeWrite` for an empty batch writes only empty dictionaries/body.

Verification:

- `cargo test --lib` OK, 660 tests.
- `cargo test --tests --no-run` OK.
- Quick prod FireTest OK: `FIRETEST_QUICK_PASS after 26.47s`.
- Full prod FireTest OK after fixing the test seed strategy kind.

Red flag closed:

- Full FireTest previously used `kind=0` (`sk_Unknown`) for the local mutable
  strategy while expecting `Comment` to roundtrip. Live schema marks `Comment`
  visible for real strategy kinds 1..23, not for `sk_Unknown`. After the
  serializer switched to schema visibility this became a correct failure of the
  test payload, not a protocol deviation. FireTest now seeds `sk_Telegram` and
  asserts that the configured field is visible for the seeded kind.

### 2026-05-26 - Strat parser malformed tails match Delphi stream reads

Done:

- Re-checked Delphi `MoonProtoStratStruct.pas` constructors and
  `MoonProtoClient.pas:ProcessStratCommand`.
- `TStratSnapshot` with declared `Size` larger than remaining bytes no longer
  becomes a generic Rust `ParseFailed`. It follows Delphi's invalid snapshot
  path: parsed command, empty/invalid `data`, serializer decode fails, no
  epoch/state apply and no SnapshotFull/Partial event.
- `TStratCheckedSync` / `TStratCheckedEcho` no longer pre-drop the whole
  command when `Count` overstates remaining bytes. They read the declared count
  with Delphi `ms.Read` semantics: dynamic-array zero tail, partial
  `StrategyID` low bytes, missing `Checked=false`.
- Recorded `spec_pipeline/work/хуйня.md §X.166`.

Verification:

- `cargo test strat --lib --quiet` OK: 67 tests.

Follow-up:

- Incoming `TStratSchemaRequest` is now ignored on the client side, matching
  Delphi `ProcessStratCommand` which has no client branch for that command.
  It remains a client->server builder/parser variant for the agreed Init schema
  request. Recorded `spec_pipeline/work/хуйня.md §X.167`.

### 2026-05-25 - OrderBook/Trades subscribe reconnect SendAndWait gate parity

Done:

- Re-checked Delphi `TMoonProtoEngine.SendAndWait`,
  `NeedReconnectAllTrades`, `NeedResubscribeOrderBooks`, and
  `BMarketHistoryWorker.Execute`.
- Rust async subscribe path now models the Delphi blocking window:
  `SubscribeAllTrades` and `SubscribeOrderBook` reconnect retries do not fire
  while the matching subscribe request is still inside the 12s
  `SendAndWait`-equivalent timeout.
- OrderBook reconnect now also stores the last subscribe UID. A non-matching
  `SubscribeOrderBook` response does not close the pending full-registry replay
  gate.
- Initial reconnect check timestamps use the `NEVER_TIME_MS` sentinel so the
  first check is immediate like Delphi `LastBookReconnectCheck = 0` against
  `GetTickCount64`.

Verification:

- `cargo test reconnect_timing_tests --lib` OK: 20 tests.
- `cargo test --lib` OK: 703 tests.
- `cargo check --examples` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 26.98s`, `ParseFailed=0`.

### 2026-05-26 - OrderBook diff has no 5000-level Rust cap

Done:

- Checked current Delphi `EngineBase.pas:TMarketEngine.ApplyOrderBookDiffKeepZero`.
  It computes `n := Min(5000, N)`, but the following `SetLength` and `Move`
  use `N`, not `n`, so the active code does not truncate the applied book to
  5000 levels.
- Removed the Rust-only `ORDER_BOOK_DIFF_MAX_LEVELS` truncation from
  `state::order_books::apply_order_book_diff_keep_zero`.
- Replaced the old mistaken regression test with coverage that proves a
  5001-level book can grow past 5000 after a diff, matching current Delphi
  machine effect.
- Removed the dead public `ApplyResult::NoFullYet` variant and its example
  counter: after the strict parity fix, first diff without full is applied like
  Delphi and this reason can no longer be produced.

Red flag closed:

- Recorded `spec_pipeline/work/хуйня.md §X.169`.

### 2026-05-26 - Orders API no longer exposes dead `Rejected`

Done:

- Removed the unused public `orders::ApplyResult::Rejected` variant. It was a
  backwards-compatibility stub for a former Rust-only internal cap and is never
  produced by the current Delphi-parity state machine.
- Updated `docs/api/orders.md` so the low-level `ApplyResult` shape is explicit
  in the self-contained API docs.

### 2026-05-26 - Strat skipped/base commands do not emit active events

Done:

- Checked Delphi `TCommandRegistry.FromStream` and
  `MoonProtoClient.pas:ProcessStratCommand`.
- Future-version strat packets now parse as
  `StratCommand::Skipped { cmd_id, uid, ver }`, matching Delphi `FSkipped`.
- Active `EventDispatcher` ignores `Skipped`, `Unknown`, incoming
  `TStratSchemaRequest`, and incoming `TStratSellPriceUpdate` because Delphi
  client has no branch for those base/inapplicable classes.
- Low-level parser/state diagnostics remain available; the normal active path
  emits no `Event::Strat(Ignored)` for those packets.

Red flag:

- Recorded `spec_pipeline/work/хуйня.md §X.171`.

### 2026-05-26 - Balance base/request/unknown commands are active-silent

Done:

- Checked Delphi `MoonProtoClient.pas:ClientNewData(MPC_Balance)` and
  `MoonProtoEngine.pas:ProcessBalanceCommand`.
- Active `EventDispatcher` now ignores incoming balance `CmdId=0/1/2/5` and
  unknown balance subcommands without producing `Event::Raw` or
  `Event::ParseFailed`.
- `CmdId=3/4` still apply balance state; `CmdId=6` still emits typed
  `Event::Arb` after payload parsing/filtering.

Red flag:

- Recorded `spec_pipeline/work/хуйня.md §X.172`.

### 2026-05-26 - Correction: UI counted arrays keep Delphi zero-tail

Correction:

- Re-checked `MoonProtoUIStruct.pas:TStratStartStopCommandV2`,
  `TEmuTradesCommand`, and `TTriggerManageCommand`.
- The previous §X.116 wording was too weak: Delphi does not shorten counted
  arrays to the number of complete elements actually present. It calls
  `SetLength(..., Count)` first, then `TMemoryStream.Read` copies whatever
  bytes remain into zero-initialized array storage.
- Rust now keeps the declared `Count` for those UI arrays and fills missing
  tail bytes with zero/false. Partial scalar bytes are kept in the low little
  endian bytes, matching Delphi memory writes.
- `TTriggerManageCommand` uses one local `Count` variable twice. If the second
  `ms.Read(Count, SizeOf(Count))` reaches EOF, Delphi reuses the previous
  count. Rust now preserves that machine effect instead of returning an empty
  `keys` array.

Red flag:

- Recorded `spec_pipeline/work/хуйня.md §X.173`.

### 2026-05-26 - UI fixed scalar short-body parity

Done:

- Re-checked simple UI constructors that read fixed scalar fields with
  `TMemoryStream.Read`.
- Rust no longer returns `ParseFailed` for short bodies of
  `TStratStartStopCommand`, `TMMOrdersSubscribeCommand`, the fixed prefix of
  `TEmuTradesCommand`, `TResetProfitCommand`, `TArbActivateNotify`,
  `TSwitchDexCommand`, `TSwitchSpotCommand`, and `TUpdateVersionCommand`
  `IsRelease` after a valid string.
- Those parsers now keep Delphi zero-tail/preserve-low-byte behavior.
- String `ReadStringFromStreamUtf8` malformed tails are not included in this
  fix because Delphi uses `ReadBuffer` there.

Red flag:

- Recorded `spec_pipeline/work/хуйня.md §X.174`.

### 2026-05-26 - Correction: Balance item tails follow Read vs ReadBuffer

Correction:

- Re-checked `MoonProtoBalanceStruct.pas:TBalanceCommand.CreateFromStream`,
  `TBalanceIncrUpdate.CreateFromStream`, `TBalanceItem.ReadFromStream`, and
  `Vars.pas:ReadStringFromStreamUtf8`.
- The earlier §X.112 fix removed the bad `Count * min_item_size` pre-drop, but
  its test overstated Delphi behavior. Delphi does not keep already parsed
  items if `Count` reaches a next item whose `MarketName` string cannot be read:
  `ReadStringFromStreamUtf8` uses `ReadBuffer`, raises, and `DataReadInt`
  catches/logs the command without applying it.
- Fixed Balance fixed-field parsing to match Delphi `TMemoryStream.Read`:
  `epoch`, global doubles, `count`, `BalanceHash`, `Flags`, and present flagged
  scalar bytes read partial low bytes, zero-fill missing tail bytes, and advance
  stream position instead of returning early or reusing tail bytes.
- `BalanceItem` string tails remain fail-fast because Delphi strings use
  `ReadBuffer`, not `Read`.

Red flag:

- Recorded `spec_pipeline/work/хуйня.md §X.175`.

Verification:

- `cargo test --lib --quiet` OK: 737 tests.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 25.88s`, `ParseFailed=0`.

### 2026-05-26 - Correction: Trade counted arrays keep Delphi zero-tail

Correction:

- Re-checked `MoonProtoTradeStruct.pas:TSetImmuneCommand.CreateFromStream` and
  `TBulkReplaceNotify.CreateFromStream`.
- The earlier §§X.114/X.115 fixes removed Rust-only drop-all guards, but still
  shortened arrays to complete elements actually present. Delphi allocates the
  declared `Count`/`N` first and reads into zero-initialized array memory with
  `TMemoryStream.Read`.
- `BulkReplaceNotify.uids` and `SetImmuneCommand.items` now keep the declared
  element count after a valid prefix. Missing tail bytes become zero/false;
  partial UID bytes occupy the low little-endian bytes.
- `TAllStatuses` was not folded into this correction: its repeated item path
  dispatches nested `TBaseTradeCommand.FromStream`, and a completely absent
  nested header depends on Delphi local scalar contents. That remains a separate
  analysis target, not an accepted deviation.

Red flag:

- Recorded `spec_pipeline/work/хуйня.md §X.176`.

Verification:

- `cargo test --lib --quiet` OK: 737 tests.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 27.62s`, `ParseFailed=0`.

### 2026-05-26 - Correction: ClientSettings soft tail Read semantics

Correction:

- Re-checked `MoonProtoUIStruct.pas:TClientSettingsCommand.CreateFromStream`.
- Rust still treated several post-string settings fields as fail-fast when
  Delphi reads them with `TMemoryStream.Read`: `UseCoinsBlackList`,
  `TempBLCount`, each `TempBLTimes[i]`, `UseManualStrategy`,
  `ManualStrategyID`, `FreePositionCheck`, `VolDropLevel`, and
  `UseStopMarket`.
- Fixed the parser split: `ReadStringFromStreamUtf8` fields remain fail-fast
  because Delphi uses `ReadBuffer`; fixed scalar fields after a valid string use
  Delphi soft-read/zero-tail. `VolDropLevel` preserves fallback high bytes on a
  partial read because Delphi assigns `VolDropLevel := cfg.VolDropLevel` before
  reading.

Red flag:

- Recorded `spec_pipeline/work/хуйня.md §X.177`.

Verification:

- `cargo test --lib --quiet` OK: 740 tests.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 24.46s`, `ParseFailed=0`.

### 2026-05-26 - Correction: Trade fixed tails follow Delphi Read semantics

Correction:

- Re-checked `MoonProtoTradeStruct.pas` constructors for `TTradeEpochCommand`,
  `TNewOrderCommand`, `TOrderStatus`, `TOrderStatusUpdate`,
  `TOrderReplaceCommand`, `TOrderReplaceResponse`, `TJoinOrdersCommand`,
  `TSplitOrderCommand`, `TMoveAllSellsCommand`, `TDoClosePositionCommand`,
  `TDoSellOrderCommand`, `TOrderStopsUpdate`, `TTurnPanicSellCommand`,
  `TOrderTracePoint`, `TCorridorUpdate`, `TMoveAllBuysCommand`,
  `TBulkReplaceNotify`, and `TVStopUpdate`.
- Rust still treated many fixed fields after a valid market string as
  fail-fast, while Delphi reads those fields with `TMemoryStream.Read`.
- Fixed the parser split: `ReadStringFromStreamUtf8` / market names remain
  fail-fast (`ReadBuffer`); fixed scalar and packed-record fields after a
  valid string now consume available bytes and zero-fill the missing tail.
- `TAllStatuses` and completely missing nested command headers remain a
  separate analysis target because Delphi dispatches each nested item through
  `TBaseTradeCommand.FromStream`.

Red flag:

- Recorded `spec_pipeline/work/хуйня.md §X.178`.

Verification:

- Added parser tests for short declared strings, empty fixed tails after valid
  market strings, empty epoch/fixed tails after valid market strings, and
  partial `TOrderStatus` compact-order bytes.
- `cargo test --lib --quiet` OK: 744 tests.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 26.20s`, `ParseFailed=0`.

### 2026-05-26 - Correction: EngineResponse tail follows Delphi Read/CopyFrom

Correction:

- Re-checked `MoonProtoEngineStruct.pas:TEngineResponse.CreateFromStream`.
- Rust treated missing `IsCompressed`, missing `DataSize`, and overdeclared
  uncompressed `DataSize` as parse failures after a valid `ErrorMsg`.
- Delphi reads `IsCompressed` and `sz` with `TMemoryStream.Read`, so missing tail
  bytes become zero. For uncompressed data Delphi uses `FStream.CopyFrom(ms,
  sz)`, which copies the available remaining bytes instead of requiring the
  declared size to fit. `ErrorMsg` remains strict because
  `ReadStringFromStreamUtf8` uses `ReadBuffer`.
- Rust `parse_engine_response` now keeps that split. Compressed partial bodies
  still fail if DEFLATE cannot decode them, matching Delphi decompressor
  exception handling through `DataReadInt`.

Red flag:

- Recorded `spec_pipeline/work/хуйня.md §X.179`.

Verification:

- Added parser tests for missing compression flag, missing data size, and short
  uncompressed response data after a valid `ErrorMsg`.
- `cargo test engine_api --lib --quiet` OK: 41 tests.
- `cargo test --lib --quiet` OK: 744 tests.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 22.06s`, `ParseFailed=0`.

### 2026-05-26 - Correction: Strat fixed tails and delete string split

Correction:

- Re-checked `MoonProtoStratStruct.pas:TStratSnapshot.CreateFromStream`,
  `TStratDelete.CreateFromStream`, `TStratSellPriceUpdate.CreateFromStream`,
  and `TStratSchema.CreateFromStream`.
- Rust treated short fixed tails in snapshot/delete/sell-price as parse
  failures, while Delphi reads those object fields with `TMemoryStream.Read`.
- Rust also used `unwrap_or_default()` for malformed `TStratDelete.FolderPath`.
  Delphi calls `ReadStringFromStreamUtf8` when any folder-path bytes remain, so
  an incomplete declared string raises and the command is not applied.
- Rust now zero-tails fixed strat fields, rejects truncated delete folder
  strings, and maps `TStratSchema` overdeclared `Size` to an empty malformed
  payload, matching Delphi `Data=nil` later-fail behavior.

Red flag:

- Recorded `spec_pipeline/work/хуйня.md §X.180`.

Verification:

- Added parser tests for short snapshot fixed tail, short delete strategy ID,
  truncated delete folder string, short sell-price update, and schema
  `Size > remaining`.
- `cargo test strat --lib --quiet` OK: 74 tests.
- `cargo test --lib --quiet` OK: 747 tests.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 27.11s`, `ParseFailed=0`.

### 2026-05-26 - Correction: Engine API domain scalar reads follow Delphi helpers

Correction:

- Re-checked `MoonProtoEngineStruct.pas:TEngineStreamCommand.ReadDouble`,
  `ReadInt`, `ReadWord`, `ReadInt64`, `ReadBool`, and `ReadByte`.
- These helpers call `FStream.Read(Result, SizeOf(Result))`. The string helper
  `ReadStr` calls `ReadStringFromStreamUtf8` and stays strict because that path
  uses `ReadBuffer`.
- Rust `commands::market::EngineStreamReader` treated all fixed scalar reads as
  fail-fast. That could turn a short fixed tail in `GetMarketsList`,
  `UpdateMarketsList`, `CheckBinanceTags`, or related domain payloads into
  `ParseFailed`, while Delphi would continue after consuming the available
  bytes.
- `EngineStreamReader` now keeps the Delphi split: scalar reads consume the
  available bytes and zero-fill the missing tail; strings remain fail-fast.

Red flag:

- Recorded `spec_pipeline/work/хуйня.md §X.181`.

Verification:

- Added parser/state tests for scalar zero-tail, strict string failure,
  short market fixed tail, short price row, and empty direct price payload.
- `cargo test market --lib --quiet` OK: 93 tests.
- `cargo test --lib --quiet` OK: 751 tests.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 26.00s`, `ParseFailed=0`.

### 2026-05-26 - Correction: small Engine API responses follow Delphi `Read` tails

Correction:

- Re-checked Delphi Engine API response readers:
  `GetBalance`, `QueryHedgeMode`, `CheckAPIExpirationTime`,
  `UpdateTransferAssets`, and `GetCoinCardCandles`.
- Fixed scalar fields read through Delphi `Read`: short fixed tails keep
  available low bytes and zero-fill the missing bytes. This applies to balance
  `Double`, hedge `Boolean`, expiration `Double`, transfer-asset `count`,
  transfer-asset `amount/total`, coin-card candle `count`, and packed
  `TDeepPrice` array bytes.
- Kept string fields strict where Delphi uses `ReadBuffer`. A truncated
  `UpdateTransferAssets.currency` declared string still rejects the response.

Red flag:

- Recorded `spec_pipeline/work/хуйня.md §X.182`.

Verification:

- Added parser tests for short scalar tails, strict transfer-asset string
  truncation, short transfer scalar row tail, empty/negative counts, partial
  coin-card candle records, and missing coin-card records.
- `cargo test engine_api --lib --quiet` OK: 41 tests.
- `cargo test candles --lib --quiet` OK: 24 tests.
- `cargo fmt --all -- --check` OK.
- `cargo test --lib --quiet` OK: 753 tests.
- `cargo check --examples --quiet` OK.
- `git diff --check` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 24.37s`, `ParseFailed=0`.

### 2026-05-26 - StrategySerializer deterministic malformed tails

Correction:

- Re-checked Delphi `StrategySerializer.pas:ReadField` and
  `SkipFieldByTypeID`.
- `TID_STRING` field values inside `TStrategySerializer` are not
  `ReadBuffer` strings. Delphi reads `Len`, calls `SetLength(b, Len)`, then
  `Stream.Read(b[0], Len)`. If the value body is short after a valid `Len`,
  `TBytes` keeps a zero-filled tail.
- `SkipFieldByTypeID` advances the stream position for skipped fixed-size
  fields and skipped string bodies. A truncated skipped known-field mismatch
  should not turn the whole snapshot into a Rust parse failure.
- Rust now zero-fills short serializer string-field bodies and saturates skipped
  fixed/string values to EOF.

Red flag:

- Recorded `spec_pipeline/work/хуйня.md §X.183`.
- Still open there: dictionary name/path reads, incomplete `Len` prefixes,
  incomplete strategy headers, and incomplete `FieldIdx`/`TypeID`. Delphi uses
  `Stream.Read` for those too, but the effect may depend on uninitialized local
  variables or stale `NameBuf`, so it is not claimed as closed parity.
- Follow-up re-check: the remaining strategy-serializer corrupt tails are not
  `ReadBuffer` strings. They are `Stream.Read` into Delphi locals or shared
  `NameBuf`; after EOF the next section can depend on stale/uninitialized bytes.
  Rust keeps the current safe failure/open-red-flag stance until an explicit
  deterministic policy is agreed. Valid live snapshots are still covered by
  FireTest full-parse (`ParseFailed=0`) and raw dumps.
- Follow-up fix: known-field fixed-size scalar values now use deterministic
  Delphi-shaped soft read: consume all available bytes, zero-fill the missing
  tail, and return the value. This closes the scalar `ReadField` no-consume
  mismatch without changing the valid wire path.

Verification:

- Added parser tests for short serializer string field bodies, short skipped
  fixed-size mismatches, short skipped string mismatches, and short scalar field
  values.
- `cargo test strategy_serializer --lib --quiet` OK: 21 tests.

### 2026-05-26 - RequestCandlesData malformed partial-apply red flag

- Re-checked Delphi `MarketsU.pas:TMarkets.ApplyRecvdStream`.
- Valid live candles are covered by the full FireTest gate, but malformed
  decompressed candle bodies were not yet proven 1:1.
- Delphi applies each known market while reading the stream. If a later market,
  candle row, or wall tail fails inside the outer `try/except`, already applied
  markets can remain mutated.
- Rust public `parse_request_candles_data_response` stays strict, but the
  registered active path now falls back to an internal partial parser when the
  strict parse fails.
- Closed deterministic prior-market case: a valid complete market followed by a
  truncated later market no longer cancels the complete prior market in the
  active `MergedCandles.markets` result.
- Still open in `хуйня.md §X.184`: exact corrupt tails inside the currently
  malformed market, especially `TDeepPricePack` partial `AStream.Read` effects
  and stale wall bytes, because Delphi may depend on local packed-record bytes
  or previous `TMarket` wall state that Rust must not invent as initialized
  data.
- Follow-up re-check: `ReadStringFromStream` used by candle market names is also
  `Read`, not `ReadBuffer`; a complete length with a short UTF-16 body depends on
  Delphi `String` storage after `SetLength`. Partial candle records read into
  local packed records, and partial wall data writes directly over existing
  `TMarket` wall fields. Those corrupt tails are state/stale-memory dependent;
  Rust only claims parity for deterministic sub-cases and leaves unsafe/stale
  current-market tails as an open safe-policy issue.
- Follow-up fix: if the current market has fully read candles and the corrupt
  tail begins at `TWallData`, the partial parser now keeps the market's candles
  and reads the wall tail with deterministic zero-fill. This matches the
  deterministic part of Delphi `ApplyRecvdStream`: `m.Deep5m` was already
  mutated before the wall reads.
- Recorded `spec_pipeline/work/хуйня.md §X.184`.

Verification:

- Added
  `request_candles_data_partial_parser_keeps_complete_prior_markets` and
  `request_candles_data_partial_parser_keeps_current_market_when_wall_tail_is_short`.
- `cargo test candles --lib --quiet` OK: 26 tests.

### 2026-05-26 - TradesEvent signal/diagnostic shape

Done:

- Removed the active-library dependency on the old public owned packet event.
- `TradesEvent::Applied { packet_num, base_time }` is now the public signal:
  rows are already applied to market tail and queued to retained
  `MarketHistoryWorker` / `SeqRing` storage before the signal is emitted.
- Active `TradesStream` dispatch walks borrowed `DecodedTradesPacket` /
  `TradeSectionIter` once and no longer allocates `Vec<TradeSection>` for the
  callback hot path. Low-level raw tools can still call `parse_trades_packet`
  when they explicitly need an owned packet.
- Updated FireTest/examples/API docs to describe `TradesEvent` as
  signal/diagnostic and retained rows as `SeqRing` reads.
- Added shared strict binary readers for strategy schema/serializer fixed reads.
- Added `StrategySchemaField::visible_kind_mask` for hot serializer visibility
  checks while keeping `visible_kind_ordinals` as the API-readable field.
- Collapsed `ClientSender` clone state from many independent `Arc` fields into
  one `Arc<ClientSenderShared>`.

Verification:

- `cargo test trades --lib --quiet` OK: 66 tests.
- `cargo test dispatcher_ --lib --quiet` OK: 46 tests.
- `cargo test strategy --lib --quiet` OK: 33 passed, 1 ignored.
- `cargo test --lib --quiet` OK: 759 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- `cargo test --test fire_test --no-run --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 24.31s`, `ParseFailed=0`,
  err_emu actual drop `9.71%`, retained futures rows present, derived snapshot
  present.

### 2026-05-26 - Futures retained trades use direct StoreWorker -> SeqRing

Correction:

- Re-checked Delphi chart/storage path after the first Rust implementation had
  copied `AddTmpHOrder` as a production retained-history buffer.
- Delphi `tmpList/tmpTradesRead/tmpTradesWrite` is a UI/storage bridge: chart
  draws retained `OrdersH` and then overlays the fresh tmp-ring until
  `BMarketHistoryWorker` moves rows into `OrdersH`.
- Active Lib target is not that historical two-source UI shape. It has one
  retained source: `StoreWorker` appends accepted futures rows directly into
  `SeqRing`, and derived state updates from the same accepted stream.

Done:

- Removed production `TradeJoinBuffer`, `TradeJoinPush`,
  `prepare_joined_trades_for_retained_append`, `trade_join_capacity`,
  `futures_join`, `joined_scratch`, and delayed
  `drain_joined_futures_like_delphi`.
- `MarketHistoryWorker` no longer drains a temp futures buffer during
  maintenance. `MarketHistoryStreamSection::FuturesTrades` now carries only the
  market name and rows; `chart_price_step` is no longer part of retained trade
  storage.
- `MarketHistoryStore::append_futures_stream_trade_like_delphi` applies
  `TradesPacketTimeShift`, appends directly into retained futures `SeqRing`,
  updates rolling volumes, and updates the current candle from that row.
- API docs and `library_decisions.md` now state explicitly that Rust Active Lib
  has no temporary futures join buffer and retained order is StoreWorker receive
  order.
- Recorded and closed `spec_pipeline/work/хуйня.md
  §X.ACTIVE_TRADES_TMP_RING`.

Verification:

- `cargo fmt --check` OK.
- `cargo test --lib` OK: 754 passed, 1 ignored.
- `cargo test --tests --no-run` OK.
- `cargo check --examples` OK.
- Quick prod FireTest release OK after this removal:
  `FIRETEST_QUICK_PASS after 22.78s`, `ParseFailed=0`, err_emu actual drop
  `9.07%`, retained futures rows present, derived snapshot present, reader max
  `590us`, writer CPU max `134us`.

### 2026-05-26 - journal_review.md closure pass

Done:

- `TNewMarketNotifyCommand` is internal Active Lib control flow. It now forces
  immediate `GetMarketsList` by bypassing the 30s listing throttle, but does not
  emit a settings event. User-facing listing signal is
  `MarketsEvent::NewMarketsAdded { names }`, emitted only after the refreshed
  list actually inserts markets into `MarketsState`.
- Removed normal public `ui_new_market_notify` wrappers from `Client` and
  `ClientSender`; the raw `commands::ui` parser remains the wire layer, while
  the builder is crate-internal test-only to avoid a production client-send API
  for this internal inbound command.
- Added raw `dispatch_into` helper
  `missing_order_status_requests_after_snapshot()` for explicit
  `CleanupMissingWorkers` follow-up requests when the caller is not using the
  active client path.
- `OrderBook` apply now refreshes `MarketPrice.chart_price_step` from the
  applied book best ask, matching Delphi `AddNewAksPrice` from `GlassUpdated`.
- `sync_market_history_storage` no longer clones all market names on every
  packet; it uses `MarketsState::markets_version` and clones names only when
  market scope/version changes.
- Balance apply no longer clones `MarketsState.by_name` keys into a temporary
  `Vec<String>` before filtering.
- `dispatch_into_active_actions` uses a small `Vec` dedup for internal
  `RequestOrderBookFull` actions instead of allocating an empty `HashSet` on
  the common path.
- `SeqRingWriter` has `push_batch_with_evicted` so batch writers can preserve
  eviction side effects; futures trade retention already compacts evicted rows
  to `MiniCandle`.
- `OrderBooks` keeps a reusable `diff_scratch` buffer instead of allocating a
  fresh diff `Vec` on every book apply.
- Updated `journal_review.md` with closed/open decisions for every reviewer
  point.

Verification:

- `cargo test --lib` OK: 759 passed, 1 ignored.
- `cargo check --examples` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 24.17s`, `ParseFailed=0`, err_emu actual drop
  `12.32%`, retained futures rows present, derived snapshot present, reader max
  `773us`, writer CPU max `131us`.

### 2026-05-26 - hot-path closure pass after journal_review.md

Done:

- Removed Rust-only `Arc` wrappers from `Client`-owned counters
  `crypt_msg_counter`, `total_sent`, and `total_recv_shared`; they were not
  shared handles and remain plain atomics with the same ordering.
- Added a subscription intent summary beside `SubscriptionRegistry`: atomic
  `trades_subscribed` / `has_orderbook_subs` plus a retained
  `TradeStorageScope` snapshot. Registry remains the source of truth; hot
  receive/tick checks no longer lock it for boolean intent checks.
- `ActiveDispatchContext` carries an `Arc<TradeStorageScope>` snapshot and
  `EventDispatcher` clones the concrete scope only when it changes, not on
  every packet.
- `copy_send_ack_and_check_sening_data` now reuses `Client` scratch buffers for
  copied Sliced/High/Low queues and SlicedACKs. The Delphi phase order is
  unchanged: `GetCopySendList -> GetCopyAcks -> CopyRecvdData -> CheckSeningData`.
- `flush_send_batch` keeps `tmp_send_buf` capacity after sending instead of
  dropping the buffer on every flush.
- `encrypt_with_cipher` uses one output buffer for `IV | Tag | Ciphertext` and
  encrypts the padded plaintext slice in-place. Wire layout is unchanged.

Follow-up done:

- Sliced receive reassembly now stores block payloads in one dense
  `block_payloads` buffer plus fixed `BlockNum -> span` metadata. Iterating
  block numbers `0..=255` keeps the Delphi sorted-list machine effect, including
  malformed `BlockNum > MaxBlockNum` behavior, while removing one heap `Vec` per
  received block.

### 2026-05-26 - FireTest uses event hot path, not per-event state snapshots

Done:

- FireTest `Session::pump` now uses production `Client::run_with_dispatcher`
  instead of `run_with_dispatcher_state`.
- The test reads dispatcher state once after each short pump slice. This keeps
  the health gate focused on protocol/active-lib CPU and avoids adding a full
  `EventDispatcherSnapshot` clone to every live trade/orderbook/log event.
- `run_with_dispatcher_state` remains available as a convenience UI API, but it
  is no longer the path used for hot FireTest CPU measurements.
- Updated `Audit/pipeline/FireTest.md` with the same rule.

Verification:

- `cargo check --test fire_test` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 25.43s`, `ParseFailed=0`,
  err_emu actual drop `10.85%`, retained futures rows present, derived snapshot
  present.
- Full prod FireTest release OK after the same pump change: exit code 0,
  `183.7s` wall-clock.
- CPU: `reader max=716us`, `writer_cpu max=153us`,
  `app_enqueue max=15us mode=callback`. The previous `app_enqueue mode=state`
  millisecond samples were FireTest harness self-noise, not protocol hot-path
  work.
- Remaining Phase Z CPU work: worker-side `TStrategySerializer` / large init API
  parse/apply (`active_dispatch max=3347us` in this run) and, separately, a
  decision whether `run_with_dispatcher_state` itself should be optimized or
  documented as non-hot convenience API.

### 2026-05-26 - Strategy snapshot CPU rerun after FireTest pump fix

Measured:

- Reran the ignored Rust Phase Z benchmark on the newest full FireTest raw dump:
  `A_strat_SnapshotPartial_event_003458_epoch_585_raw_44284.bin`.
- Payload: `44284` bytes, `761` strategies, `200` release iterations.
- Result:
  `parse_avg/max=3157us/4680us`,
  `apply_cold_avg/max=2659us/3672us`,
  `apply_warm_avg/max=1920us/2274us`.

Interpretation:

- The live `active_dispatch ~3ms` Strat samples are still real worker-side
  parser/apply work, not FireTest state-callback snapshot noise.
- Prior Delphi benchmark on a comparable `~44KB/762 strategy` payload measured
  warm `LoadStrategiesFromStream` around `3.5ms` average, so current Rust is not
  proven slower than Delphi here.
- Keep this in Phase Z, not protocol recv parity: protocol `reader` and
  `writer_cpu` are below `1ms`; the remaining decision is whether to optimize
  worker/app-side large payload parsing further for UX margin.

Verification:

- `cargo test --release bench_firetest_strategy_snapshot_payload --lib -- --ignored --nocapture`
  OK.

### 2026-05-26 - `client.rs` test-tail split

Done:

- Moved the large `#[cfg(test)]` tail from `src/client.rs` into
  `src/client/tests.rs`.
- This is a mechanical publication/refactor step only: production protocol code,
  call order, state ownership, and public API are unchanged.
- `client.rs` lost 7412 test lines; the remaining production-body split should
  proceed only by semantic blocks, not by arbitrary line ranges.

Verification:

- `cargo fmt --all -- --check` OK.
- `cargo test --lib --quiet` OK: 759 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `client.rs` semantic state/glue split series

Done:

- Moved public config/lifecycle/error types into `src/client/config.rs`.
- Moved app callback / dispatcher worker glue into `src/client/app_dispatch.rs`.
- Moved active-library subscription and restore intent state into
  `src/client/subscriptions.rs`.
- Moved candles async container state into `src/client/candles.rs`.
- Moved `ReaderSlicedStats`, `SentSliced`, `SlicedAck`, and `DataReadState`
  into `src/client/transport_state.rs`.
- Public paths stay re-exported from `client.rs`; crate-only fields/methods were
  widened only where the moved sibling modules/tests already needed the same
  direct access. No send/recv protocol order, queue order, ACK/retry mechanics,
  or wire format changed.

Verification:

- `cargo fmt --all -- --check` OK.
- `cargo test --lib --quiet` OK: 759 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 22.38s`, `ParseFailed=0`,
  `reader max=694us`, `writer_cpu max=165us`, `active_dispatch max=3650us`
  on `Strat(30)` payload. The active-dispatch sample is the known Strategy
  parser/apply Phase Z CPU item, not a protocol-loop regression.

### 2026-05-26 - `client.rs` ProtocolCore split

Done:

- Moved the whole `ProtocolCore` owner/tick implementation into
  `src/client/protocol_core.rs` as one block, preserving method order and call
  order inside the block.
- `client.rs` now keeps the public `Client` API/body while the protocol
  writer/reader tick lives in a dedicated file for Delphi parity review.
- `ProtocolCore` methods are `pub(crate)` only because existing unit tests call
  them as protocol oracles from the sibling `tests.rs` module. No public API was
  added.

Verification:

- `cargo fmt --all -- --check` OK.
- `cargo test --lib --quiet` OK: 759 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 22.90s`, `ParseFailed=0`,
  `reader max=580us`, `writer_cpu max=146us`, `active_dispatch max=3219us`
  on `Strat(30)`.

### 2026-05-26 - `client.rs` send queue/UKey split

Done:

- Moved `SendPriority`, `UniqueKey`, `UK_*`, `SendItem`, `SendQueues`,
  `SendLockState`, and `initial_retry_left` into `src/client/send_queue.rs`.
- Public low-level API paths stay re-exported from `client.rs`.
- Queue fields/methods that existing unit tests inspect remain crate-visible
  only. No Delphi `DataToSend*`, UKey dedup, ACK snapshot, or TmpSlider
  machine effect changed.

Verification:

- `cargo fmt --all -- --check` OK.
- `cargo test --lib --quiet` OK: 759 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 23.53s`, `ParseFailed=0`,
  `reader max=719us`, `writer_cpu max=138us`, `active_dispatch max=3977us`
  on `Strat(30)`.
- Quick prod FireTest release after the sender split OK:
  `FIRETEST_QUICK_PASS after 25.10s`, `ParseFailed=0`,
  `reader max=676us`, `writer_cpu max=153us`.

### 2026-05-26 - `client.rs` Engine API/candles wrapper split

Done:

- Moved public Engine API request helpers, typed request/parse helpers, and
  chunked candles async wrapper state methods into `src/client/engine_api.rs`.
- Public methods remain inherent `Client` methods; API call sites and wire
  behavior are unchanged.
- Internal helpers needed by the existing subscription/refresh paths and tests
  are crate-visible only.

Verification:

- `cargo fmt --all -- --check` OK.
- `cargo test --lib --quiet` OK: 759 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `client.rs` subscription/reconnect API split

Done:

- Moved `Client::sender`, public subscription helpers, subscription registry
  restore, trades reconnect, orderbook reconnect, and post-init subscription
  flush helpers into `src/client/subscription_api.rs`.
- This is the active-library intent layer: it still records exactly the same
  registry state and queues the same Engine API/UI restore commands. No Init,
  reconnect, trades, or orderbook protocol order changed.
- Helpers used by `init.rs`, `protocol_core.rs`, and unit oracle tests are
  crate-visible only; no public API was added.

Verification:

- `cargo fmt --all -- --check` OK.
- `cargo test --lib --quiet` OK: 759 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 25.73s`, `ParseFailed=0`,
  `reader max=798us`, `writer_cpu max=152us`, `active_dispatch max=3942us`
  on `Strat(30)`.

### 2026-05-26 - `client.rs` domain command wrapper split

Done:

- Moved high-level Order/UI/Strat/Balance `Client` wrappers into
  `src/client/domain_commands.rs`.
- The moved code still calls the same command builders, priorities, retry
  counts, UKeys, and domain-ready gate. Public methods remain inherent
  `Client` methods.
- `send_order_cancel_request` is crate-visible only because the active
  dispatcher action path in the runtime body calls the same helper.

Verification:

- `cargo fmt --all -- --check` OK.
- `cargo test --lib --quiet` OK: 759 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `client.rs` diagnostic/status API split

Done:

- Moved transport/domain status getters, protocol counters, diagnostic UI
  getters, BpsCounter accessors, and log throttle into
  `src/client/diagnostic_api.rs`.
- Public getter paths remain inherent `Client` methods.
- `track_sent` / `track_recv` are crate-visible only because runtime and
  `ProtocolCore` still update the same counters.

Verification:

- `cargo fmt --all -- --check` OK.
- `cargo test --lib --quiet` OK: 759 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `client.rs` raw send API split

Done:

- Moved raw `Client::send_cmd`, `Client::send_cmd_keyed`, internal
  `enqueue_send_item`, and test-only subscription registry helpers into
  `src/client/send_api.rs`.
- Queue behavior is unchanged: calls still append `SendItem` into the same
  unbounded Delphi-style `SendLockState` queues.
- `refresh_subscription_summary` is crate-visible only because the
  subscription API sibling module updates the same atomic mirrors.

Verification:

- `cargo fmt --all -- --check` OK.
- `cargo test --lib --quiet` OK: 759 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `client.rs` runtime API split

Done:

- Moved poller registration helpers, `now_ms`, cached server address resolve,
  `run`, `disconnect`, `run_with_dispatcher*`, `run_until_response`,
  worker-queued runtime helper, and the test-only active-action oracle into
  `src/client/runtime.rs`.
- Public runtime API remains inherent `Client` methods. Crate-visible widening
  is limited to helpers already called from `ProtocolCore`, `init.rs`, sibling
  modules, or oracle tests.
- The protocol loop still enters `ProtocolCore { client: self }.run(...)` at
  the same call sites with the same `RunMode` construction.

Verification:

- `cargo fmt --all -- --check` OK.
- `cargo test --lib --quiet` OK: 759 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 26.00s`, `ParseFailed=0`,
  `reader max=679us`, `writer_cpu max=150us`, `active_dispatch max=3759us`
  on `Strat(30)`.

### 2026-05-26 - `client.rs` protocol helper split

Done:

- Moved SlicedACK parsing/application, handshake helper decode/builders, Ping
  response application, DataRead decode, Engine API/candles inline dispatch,
  compression/wire send helpers, reset, bind, and bind-failure lifecycle
  tracking into `src/client/protocol_helpers.rs`.
- This is a block move of protocol helper methods used by `ProtocolCore` and
  oracle tests; all widened methods are crate-visible only.
- No protocol call order changed: `ProtocolCore` still calls the same
  `Client::*` helpers at the same points in its tick.

Verification:

- `cargo fmt --all -- --check` OK.
- `cargo test --lib --quiet` OK: 759 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 23.43s`, `ParseFailed=0`,
  `reader max=667us`, `writer_cpu max=154us`, `active_dispatch max=3277us`
  on `Strat(30)`.

### 2026-05-26 - `client.rs` ClientSender split

Done:

- Moved `SubscribeError`, `ClientSender`, and `ClientSenderShared` into
  `src/client/sender.rs`.
- `client::ClientSender` and `client::SubscribeError` remain re-exported from
  `client.rs`; public API paths are unchanged.
- The test-only direct construction path remains crate-visible so existing unit
  tests continue to prove Delphi queue/gate behavior without adding a public
  constructor.
- No send queue semantics changed: the moved code still appends the same
  `SendItem`s into the same Delphi-style `SendLockState` queues.

Verification:

- `cargo fmt --all -- --check` OK.
- `cargo test --lib --quiet` OK: 759 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release after the client split series OK:
  `FIRETEST_QUICK_PASS after 25.41s`, `ParseFailed=0`,
  `reader max=721us`, `writer_cpu max=127us`.

### 2026-05-26 - `client.rs` BpsCounter split

Done:

- Moved `BpsCounter` and its tests into `src/client/bps.rs`.
- `client::BpsCounter` remains re-exported from `client.rs`; no public path or
  runtime behavior changed.

Verification:

- `cargo fmt --all -- --check` OK.
- `cargo test --lib --quiet` OK: 759 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `client.rs` init/connect helper split

Done:

- Moved the MoonBot-compatible init/connect free-function layer into
  `src/client/init.rs`.
- Public API paths are preserved by re-export from `client.rs`:
  `connect_and_init`, `run_init_sequence`, `ConnectConfig`, `ConnectError`,
  `InitConfig`, `InitError`, `InitResult`.
- Internal test oracles for BaseCheck retry and post-init resync remain
  crate-visible only for unit tests; no public API was added.
- No send/recv protocol order changed. This only separates the domain init
  helper layer from the transport/runtime owner.

Verification:

- `cargo fmt --all -- --check` OK.
- `cargo test --lib --quiet` OK: 759 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `client.rs` clock/socket helper split

Done:

- Moved process clock/NTP/server-time-delta helpers into `src/client/clock.rs`.
- Moved UDP socket buffer and DontFragment option helpers into
  `src/client/socket.rs`.
- Public/crate paths are preserved by re-export from `client.rs`:
  `client::set_ntp_offset`, `client::delphi_now_raw`,
  `client::set_server_time_delta_global`,
  `client::get_server_time_delta_global`.
- No protocol order or send/recv state ownership changed; this is a semantic
  helper split only.

Verification:

- `cargo fmt --all -- --check` OK.
- `cargo test --lib --quiet` OK: 759 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - retained trades dispatch uses server index slots

Done:

- Closed the hot-path part of the `Arc<str>` market-name review finding without
  changing public API ownership: retained trades stream batches no longer carry
  cloned `market_name: String` per section.
- `MarketHistoryStreamSection` now carries `market_index: u16`; the
  `MarketHistoryWorker` resolves it through registry slots configured from
  `GetMarketsIndexes`.
- The registry stores sparse server-index slots, not a compressed filtered
  list. Unknown/missing index names keep their position as `None`, so a hole at
  `mIndex=0` cannot shift `mIndex=1` into the wrong market.
- Follow-up re-check of Delphi `TMarkets.Add`: list/dictionary updates use COW
  while `TMarket` objects stay alive. Rust mirrors the useful part in retained
  history by keeping worker-owned store keys and configured index slots as
  shared `Arc<str>` names. Public lookup/config APIs remain `&str`/`String`;
  this is an internal ownership optimization, not a protocol change.
- `EventDispatcher` still uses `MarketsState::market_name_by_index` for the
  Delphi visibility/storage-scope gate before enqueueing a retained batch; the
  worker side avoids the extra hot clone/hash lookup.

Verification:

- `cargo test active_dispatch_history_worker_uses_server_index_mapping_not_market_vector_order --lib --quiet` OK.
- `cargo test registry_resolves_stream_sections_by_configured_server_index --lib --quiet` OK.
- `cargo test registry_reconfigure_preserves_existing_store_like_delphi_market_cow --lib --quiet` OK.
- `cargo fmt --all -- --check` OK.
- `cargo test --lib --quiet` OK: 763 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 24.11s`, `ParseFailed=0`, err_emu actual drop
  `10.51%`, retained futures rows present, derived snapshot present,
  `reader max=742us`, `writer_cpu max=151us`.

### 2026-05-26 - `ProtocolCore` send-phase split

Done:

- Moved the Delphi writer/send phase methods from `src/client/protocol_core.rs`
  into `src/client/protocol_send.rs` as one mechanical block.
- The moved block starts at `send_command` and keeps the internal method order:
  handshake sends, reconnect checks, `copy_send_ack_and_check_sening_data`,
  ACK application, `CreateSlicedObject`, H retry, Sliced retry, Low phases,
  grouped flush, and `DoSendMPData` wire batching.
- `ProtocolCore::run`, receive drain, domain dispatch, periodic refresh, and
  reconnect tail remain in `protocol_core.rs`. No call site or protocol order
  changed; this is only a file boundary for review.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 763 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 27.59s`, `ParseFailed=0`, err_emu actual drop
  `11.41%`, retained futures rows present, derived snapshot present,
  `reader max=748us`, `writer_cpu max=176us`.

### 2026-05-26 - `ProtocolCore` receive-phase split

Done:

- Moved receive drain, transport unpack, ErrEmu decision, `DataRead` /
  `DataReadInt`, handshake control receive handlers, Sliced receive/SlicedACK,
  and Ping receive handling from `src/client/protocol_core.rs` into
  `src/client/protocol_recv.rs` as one mechanical block.
- The moved method order is unchanged. Calls from `ProtocolCore::run` and the
  writer/send phase still hit the same inherent `ProtocolCore` methods.
- `protocol_core.rs` now keeps the outer tick orchestration, app-command drain,
  active dispatch, periodic refresh, and reconnect tail.

Verification:

- `cargo test --lib --quiet` OK: 763 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 26.32s`, `ParseFailed=0`, err_emu actual drop
  `10.51%`, retained futures rows present, derived snapshot present,
  `reader max=613us`, `writer_cpu max=150us`.

### 2026-05-26 - Sliced retry resend removes temporary slice copy

Done:

- Removed the leftover `retry_sliced` TODO that copied every retransmitted
  slice into a temporary `Vec<u8>` only to satisfy borrowing.
- The retry loop now packs the selected `self.sending[idx].slices[block]`
  directly into the reusable transport `send_buf`, then calls the same
  `dispatch_send` path.
- Protocol behavior is unchanged: same `MPC_Sliced` packet bytes, same
  `ClientLimit`, same `UsedSlicedLimit`, same removal order. The change only
  removes one heap grow/copy per retransmitted slice.

Verification:

- `cargo fmt --all -- --check` OK.
- `cargo test --lib --quiet` OK: 763 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 23.38s`, `ParseFailed=0`, err_emu actual drop
  `12.39%`, retained futures rows present, derived snapshot present,
  `reader max=767us`, `writer_cpu max=220us`.

### 2026-05-26 - Full prod gate after protocol split / Sliced cleanup

Done:

- Ran the full release FireTest against the configured prod server after the
  send-phase split, receive-phase split, retained-trades index-slot change, and
  Sliced retry temp-copy removal.
- This is the stronger checkpoint for the current working point: two live
  clients, initial `err_emu=10%`, high-loss simple ops, retained trades/candles,
  strategy schema/snapshot, order/settings scenarios, and protocol CPU summary.

Verification:

- `cargo test --release --test fire_test -- --ignored --nocapture` OK:
  exit code 0 after 187.3s.

### 2026-05-26 - retained mini-candle compaction covered at worker boundary

Done:

- Added worker-level coverage for the agreed retained-history model: accepted
  futures rows append directly into the retained `SeqRing`, overwritten rows
  are buffered by the store, and `MarketHistoryWorker::flush` compacts them
  into retained `MiniCandle` rows.
- This proves the production worker boundary, not only the low-level
  `compact_trades_to_mini_candles_like_delphi` helper.

Verification:

- `cargo test worker_flush_compacts_evicted_futures_to_mini_candles --lib --quiet` OK.
- `cargo test history_worker --lib --quiet` OK: 10 passed.
- `cargo test mini_candle --lib --quiet` OK: 4 passed.
- `cargo fmt --all -- --check` OK.
- `cargo test --lib --quiet` OK: 764 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 24.50s`, `ParseFailed=0`, err_emu actual drop
  `9.19%`, retained futures rows present, derived snapshot present,
  `reader max=682us`, `writer_cpu max=138us`.

### 2026-05-26 - trades dispatch removes resend/event wrapper allocations

Done:

- Added zero-copy `iter_trades_resend_response`: active dispatch and low-level
  callers now walk `MPC_TradesResendResponse` inner `TradesStream` packets as
  borrowed slices instead of cloning every inner payload into `Vec<u8>`.
- Removed the owned `parse_trades_resend_response` wrapper; callers that need
  owned bytes can collect from the iterator explicitly.
- Removed the intermediate `Vec<TradesEvent>` in active trades dispatch.
  `TradesPacketEffect` values are converted straight into `Event::Trade`,
  while the first `Apply` still performs retained-state update before the
  `Applied` signal, preserving the previous Delphi machine-effect order.

Verification:

- `cargo test parse_resend_response --lib --quiet` OK.
- `cargo test trades_resend --lib --quiet` OK: 3 passed.
- `cargo test --lib --quiet` OK: 765 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 22.68s`, `ParseFailed=0`, err_emu actual drop
  `8.96%`, retained futures rows present, derived snapshot present,
  `reader max=768us`, `writer_cpu max=132us`.

### 2026-05-26 - WatcherFills active event restored

Done:

- Found and fixed `хуйня.md §X.185`: active `TradesStream` previously parsed
  WatcherFills at the low-level parser but dropped them in
  `EventDispatcher::apply_known_trades_sections_like_delphi`.
- Delphi `ProcessTradesStream` branch `ExtType=1` reads `THLAddress`, `Count`,
  `TWSFill` rows, applies the packet-level `TimeShift`, decodes `TOrderType`
  and flags, then calls `ProcessWatcherFillsDetect`.
- Rust now emits `Event::WatcherFills(WatcherFillsEvent)` with market index/name,
  user, shifted `time_ms`/`time`, price, qty, zBTC, position, raw `OrderType`,
  and `is_short` / `is_open` / `is_taker`.
- The packet-level TimeShift is filled by the first processed
  Trades/MM/Liq/Watcher row in the same packet, matching Delphi's shared
  `TimeShiftFilled` variable. Unknown/stale market indexes still skip the
  whole WatcherFills section.

Verification:

- `cargo test active_dispatch_emits_typed_watcher_fills_like_delphi_process_watcher_fills_detect --lib --quiet` OK.
- `cargo test watcher --lib --quiet` OK: 5 passed.
- `cargo fmt --all -- --check` OK.
- `cargo test --lib --quiet` OK: 766 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 24.48s`, `ParseFailed=0`, err_emu actual drop
  `10.88%`, retained futures rows present, derived snapshot present,
  `reader max=720us`, `writer_cpu max=141us`.

### 2026-05-26 - listing refresh requests order snapshot

Done:

- Closed `INVARIANT.md §3.3` listing/order-snapshot parity: after refreshed
  `GetMarketsList` actually inserts new markets, Active Lib now queues
  `TAllStatusesReq` before the immediate `UpdateMarketsList` price refresh.
- Delphi evidence: local `AddNewMarket` sends `TAllStatusesReq` during
  `GetMarketsList`, and `Bworks` calls `UpdateMarketsList` afterward when
  `Engine.NewMarkets.Count > 0`.
- This protects orders for a market whose earlier order-push could be dropped
  because the market did not exist locally yet.

Verification:

- `cargo test active_get_markets_list_emits_new_markets_added_after_actual_insert --lib --quiet` OK.
- `cargo test new_market_list_refresh_requests_immediate_prices_like_delphi_new_markets --lib --quiet` OK.
- `cargo test --lib --quiet` OK: 766 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 23.73s`, `ParseFailed=0`, err_emu actual drop
  `10.17%`, retained futures rows present, derived snapshot present,
  `reader max=728us`, `writer_cpu max=158us`.

### 2026-05-26 - retained history registry shared market names

Done:

- Re-checked Delphi `MarketsU.pas:TMarkets.Add`: normal listing-add builds a
  new `TList<TMarket>` and new dictionaries, puts old containers into delayed
  trash, and keeps existing `TMarket` objects alive.
- Rust Active Lib now mirrors the useful machine effect for retained history:
  reconfiguring known markets does not recreate existing per-market stores, and
  worker-owned registry keys / server-index slots use shared `Arc<str>` market
  names instead of owned `String` copies.
- This step was only the internal history-storage optimization. A later
  `TMarkets` COW step upgrades public market lookup to stable
  `MarketHandle`s while keeping name-based lookup as the ergonomic entry point.

Verification:

- `cargo test registry_reconfigure_preserves_existing_store_like_delphi_market_cow --lib --quiet` OK.
- `cargo test history_store:: --lib --quiet` OK: 18 passed.
- `cargo test history_worker:: --lib --quiet` OK: 5 passed.
- `cargo test --lib --quiet` OK: 767 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 23.23s`, `ParseFailed=0`, err_emu actual drop
  `9.73%`, retained futures rows present, derived snapshot present,
  `reader max=679us`, `writer_cpu max=155us`.

### 2026-05-26 - remove client-side TStratSnapshotRequest API

Done:

- Closed `хуйня.md §X.187`: Delphi server drops `TStratSnapshotRequest` when it
  receives it from a client (`клиент не должен слать это серверу`).
- Removed public `Client::strat_snapshot_request` and
  `ClientSender::strat_snapshot_request`.
- Kept the request builder only as `cfg(test)` crate-internal helper for
  server→client unit-test payloads. Normal Active Lib flow remains unchanged:
  server sends the request, dispatcher answers with `TStratSnapshot`.
- Updated strategy API docs to state this direction explicitly.

Verification:

- `cargo test parse_snapshot_request --lib --quiet` OK.
- `cargo test snapshot_requested --lib --quiet` OK: 4 passed.
- `cargo test --lib --quiet` OK: 767 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 23.18s`, `ParseFailed=0`, err_emu actual drop
  `8.01%`, retained futures rows present, derived snapshot present,
  `reader max=586us`, `writer_cpu max=144us`.

### 2026-05-26 - remove client-side TStratCheckedEcho API

Done:

- Closed `хуйня.md §X.188`: Delphi server creates `TStratCheckedEcho` only as
  ACK to a client `TStratCheckedSync(IsDelta=true)`, and has no incoming
  server-side branch for client-sent Echo.
- Removed public `Client::strat_checked_echo` and
  `ClientSender::strat_checked_echo`.
- Kept `build_checked_echo` only as `cfg(test)` crate-internal helper for
  parsing/server→client unit-test payloads.
- Fixed comments/API docs: Echo is inbound-only `S→C ACK`; checked changes are
  sent by `TStratCheckedSync`.

Verification:

- `cargo test checked_word_count_builders_write_only_declared_wrapped_count_like_delphi --lib --quiet` OK.
- `cargo test --lib --quiet` OK: 767 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - Delphi `TMarkets` COW as stable Rust handles

Delphi evidence:

- `TMarkets = class(TSlowSafeList<TMarket>)`; the list stores `TMarket` object
  references, not value snapshots.
- Normal `TMarkets.Add(MainList=false)` builds a new `TList<TMarket>` and new
  dictionaries, copies existing object references into them, adds the new
  object, then puts old list/dictionaries into delayed trash.
- Existing `TMarket` objects stay alive and mutable. UI/code may keep a market
  reference while listing refresh replaces the container around it.

Done:

- Replaced the public market object storage with Delphi-like stable
  `MarketHandle`s. Rust analogue:
  `TMarket*` -> `MarketHandle`; `TList<TMarket>` -> COW `Arc<Vec<MarketHandle>>`;
  `TDictionary<string,TMarket>` -> COW `Arc<HashMap<..., MarketHandle>>`; delayed
  trash -> normal `Arc` lifetime.
- Existing markets must be mutated through the same handle on
  `GetMarketsList`/`UpdateMarketsList`, not replaced by a new value.
- The payload/streaming `GetMarketsList` path builds new handles in one pending
  list/dict and replaces the containers once. This matches Delphi initial/main
  list build and avoids the Rust-only O(N^2) bug where COW was performed per
  market row.
- Public API now lets user code do one lookup once, hold the handle, and read
  current metadata later without re-searching by name/index:
  `let btc = markets.get("BTCUSDT")?; btc.with(|m| ...)`.
- Kept snapshot helpers for code that wants owned `Market` copies:
  `market_snapshot` and `market_snapshot_by_index`.
- Added tests proving handle identity survives listing refresh and sees later
  in-place mutations.

Verification:

- `cargo fmt --all -- --check` OK.
- `cargo test market_handle_survives_listing_cow_and_sees_in_place_updates_like_delphi --lib --quiet` OK.
- `cargo test apply_markets_list_payload_batches_new_market_cow_like_delphi_main_list_build --lib --quiet` OK.
- `cargo test --lib --quiet` OK: 768 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 23.10s`, `ParseFailed=0`, err_emu actual drop
  `10.62%`, retained futures rows present, derived snapshot present,
  `reader max=720us`, `writer_cpu max=142us`.

Open CPU note:

- The O(N^2) COW CPU bug is closed. Before the batch fix, phased FireTest showed
  `GetMarketsList apply total=12218us`, `market_apply=10290us`. After the fix:
  `total=2344us`, `markets=821us`, `market_read=325us`, `market_apply=436us`,
  `corr=1212us`, `ref=305us`.
- `active_dispatch max` for this large init payload is now about `3868us`.
  Protocol reader/writer stayed below `1ms`; the remaining large init apply
  budget is Phase Z optimization work, not socket receive/send work.

Final Phase Z item:

- Re-check `GetMarketsList apply total ~= 2344us` after the strict-parity work
  settles. This is not a current blocker because the O(N^2) Rust-only bug is
  removed and protocol reader/writer are below `1ms`, but `2.34ms` can still hide
  avoidable init-state overhead. Re-measure against the same FireTest phase
  counters and optimize remaining `corr/ref` or allocation costs if Delphi-style
  machine effect proves they should be cheaper.

### 2026-05-26 - cleanup of stale compatibility helpers

Done:

- Removed unused public one-slot `UniqueKey` helpers for
  `UK_TurnMMDetection`, `UK_DexSwitch`, and `UK_SpotSwitch`. The only helpers
  left for these UI commands are the Delphi-equivalent `(kind, command UID)`
  forms used by live code.
- Removed owned `parse_trades_resend_response`; resend responses are now exposed
  only through zero-copy `iter_trades_resend_response`. Low-level callers that
  need owned bytes can collect from the iterator explicitly.
- Reworded stale `compatibility/raw callers/tests` comments into final API
  language: borrowed hot path first, owned low-level helper only where it is a
  real public tool.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 26.14s`, `ParseFailed=0`, err_emu actual drop
  `11.59%`, retained futures rows present, derived snapshot present,
  `reader max=715us`, `writer_cpu max=157us`, `GetMarketsList apply=2429us`.

### 2026-05-26 - `EventDispatcher` TradesStream block split

Done:

- Moved the Delphi-shaped `ProcessTradesStream` /
  `ProcessTradesResendBatch` active-dispatch block from `events.rs` into
  `events/trades.rs`.
- Kept the machine-effect order unchanged: fresh index gate, decode packet,
  apply `TradesState` packet-number effect, apply rows only on first `Apply`,
  enqueue retained-history batch, then emit signal/diagnostic `TradesEvent`s.
- This is a readability/publication split only. It does not change wire parsing,
  retained-history ownership, gap recovery, or public event shape.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 27.51s`, `ParseFailed=0`, err_emu actual drop
  `10.70%`, retained futures rows present, derived snapshot present,
  `reader max=717us`, `writer_cpu max=196us`, `GetMarketsList apply=2566us`.

### 2026-05-26 - `commands::trade` Delphi ordinal split

Done:

- Moved the `MPC_Order` Delphi ordinal wrappers into
  `commands/trade/enums.rs`: `OrderType`, `OrderWorkerStatus`,
  `FixedPosition`, `MoveAllCmdType`, `MoveAllBuysCmdType`,
  `ReplaceMultiKind`.
- Kept public paths unchanged through `commands::trade::*` re-exports.
- No wire parsing/building moved in this step; packed records and command
  builders still sit in `trade.rs` for the next, separately verified split.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `commands::trade` packed-record split

Done:

- Moved fixed Delphi packed records from `trade.rs` into
  `commands/trade/records.rs`: `PriceZone`, `OrderCompact`, `StopSettings`,
  `OrderUpdateData`, `ImmuneItem`, and the zero-tail readers used by soft-tail
  command parsing.
- Kept public paths unchanged through `commands::trade::*` re-exports.
- Kept compile-time wire-size checks in the records module and kept tests
  asserting the same public wire-size constants / byte-for-byte roundtrips.
- `trade.rs` now focuses on command headers, command parsing, command structs,
  and builders; fixed wire structs are isolated for easier Delphi packed-record
  сверка.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `EventDispatcher` order block split

Done:

- Moved the Delphi-shaped `ProcessCommandOrder`, `TAllStatuses` snapshot apply,
  order ticks, pending-cancel resend tick, and `CleanupMissingWorkers` follow-up
  request logic into `events/orders.rs`.
- Kept the machine-effect order unchanged: parse order payload, drop forbidden
  new `FromCache`/unknown-market statuses like Delphi, set server-time delta,
  apply order state, emit only applied events, then run missing-worker follow-up
  after snapshots.
- This is a readability/publication split only; no order state, wire parsing,
  retry, or public event behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `EventDispatcher` API response block split

Done:

- Moved the Delphi-shaped `ProcessApiCommand` block into `events/api.rs`.
- Kept side-effect order unchanged: method-specific state apply and extra
  market events first, then original `Event::EngineResponse`.
- Kept `UpdateMarketsList -> LastPrice` retained-history queue with the API
  block, because it is part of the same Delphi `TMarket.AddFrom` side effect.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `EventDispatcher` OrderBook/Balance block split

Done:

- Moved `MPC_OrderBook` active dispatch into `events/order_book.rs`.
- Moved `MPC_Balance` balance/arb active dispatch into `events/balance.rs`.
- Kept machine-effect order unchanged:
  market-index gate -> parse -> state apply -> chart-price side effect ->
  typed event for order books; subcommand/version gate -> parse -> known-market
  filter -> typed event for balances/arb.
- This is a readability/publication split only. No protocol timing, parsing,
  retained state, or public event behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `EventDispatcher` Strat/UI/Log block split

Done:

- Moved `MPC_Strat` active dispatch into `events/strat.rs`.
- Moved `MPC_UI` and `MPC_LogMsg` active dispatch into `events/ui.rs`.
- Kept machine-effect order unchanged:
  strategy parse -> skip request/unknown kinds -> apply strategy state ->
  auto-decode snapshot payload -> emit `Event::Strat`; UI parse -> handle
  `TNewMarketNotifyCommand` internal refresh flags -> settings apply; LogMsg
  parse `TDateTime + UTF-8 rest`.
- This is a readability/publication split only. No protocol timing, parser
  behavior, settings state, strategy snapshot behavior, or public event shape
  changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 22.54s`, `ParseFailed=0`, err_emu actual drop
  `11.02%`, retained futures rows present, derived snapshot present,
  `reader max=628us`, `writer_cpu max=125us`, `GetMarketsList apply=2577us`.

### 2026-05-26 - `EventDispatcher` active-action block split

Done:

- Moved `ActiveDispatchContext`, `ActiveAction`, and
  `dispatch_into_active_actions` into `events/active.rs`.
- Kept the active outbox order unchanged: client context link -> domain/init
  gates -> token/index reset gates -> dispatch -> market-history sync ->
  market-list refresh actions -> orderbook full request dedup -> strategy
  snapshot/schema follow-up -> order snapshot cleanup -> trades resend tick.
- This is a readability/publication split only. No auto-action, reconnect gate,
  trades recovery, orderbook full refresh, strategy snapshot reply, or public
  event behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick prod FireTest release OK:
  `FIRETEST_QUICK_PASS after 22.68s`, `ParseFailed=0`, err_emu actual drop
  `10.24%`, retained futures rows present, derived snapshot present,
  `reader max=713us`, `writer_cpu max=127us`, `GetMarketsList apply=2407us`.

### 2026-05-26 - `Orders` read-model type split

Done:

- Moved order read-model helper types into `state/orders/types.rs`:
  `SellReason`, `OrderCancelSend`, `PanicSellSend`,
  `OrderTraceChartPoint`, `OrderTraceLine`, `ApplyResult`, `OrderEvent`.
- Kept public paths unchanged through `state::orders` / `state` re-exports.
- Left `Orders::apply_at` and the Delphi worker state-machine in
  `state/orders.rs` intact for one-piece protocol сверка.
- This is a readability/publication split only. No order apply behavior,
  event shape, pending-cancel action, panic-sell action, trace-line math, or
  public API behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `Orders` local worker-action split

Done:

- Moved local outgoing order-worker actions into `state/orders/actions.rs`:
  local visual marker, move-all pre-send gates, immune-click local mutation,
  stop/VStop change checks, replace/cancel pending sends, pending-cancel resend
  tick, and panic-sell local sends.
- Kept `Orders::apply_at` server-command state-machine in `state/orders.rs`
  intact for focused Delphi `ProcessCommandOrder` сверка.
- This is a readability/publication split only. No local mutation order,
  outgoing command gate, pending resend timing, panic-sell behavior, or public
  API behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `EngineMethod` ordinal split

Done:

- Moved the Delphi `TEngineMethodKind` ordinal wrapper into
  `commands/engine_api/method.rs`.
- Kept public paths unchanged through `commands::engine_api::EngineMethod`.
- Kept the ordinal preservation tests with the type: unknown method bytes still
  round-trip exactly like Delphi `ms.Read(Method, SizeOf(Method))`.
- This is a readability/publication split only. No API response parsing,
  request method ordinal, public constant, or Debug/name behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `TEngineResponse` parser split

Done:

- Moved `EngineResponse` and `parse_engine_response` into
  `commands/engine_api/response.rs`.
- Kept public paths unchanged through `commands::engine_api::*` re-exports.
- Kept the existing parent tests covering header skip, unknown method ordinals,
  deflate payloads, and Delphi zero-tail scalar reads.
- This is a readability/publication split only. No response parser behavior,
  soft-tail behavior, decompression behavior, or public API behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `MarketsState` read-model type split

Done:

- Moved market read-model helper types into `state/markets/types.rs`:
  `MarketHandle`, `MarketsListApplyTiming`, `MarketPrice`,
  `MarketLastPriceHistoryInput`, `BaseCurrencyPrice`, `MarketTradeState`,
  `MarketsEvent`.
- Kept public paths unchanged through `state::markets` / `state` re-exports.
- Left the Delphi-like COW list/dictionary merge and price/tag/index apply
  algorithms in `state/markets.rs` for focused сверка.
- This is a readability/publication split only. No COW handle behavior,
  market lookup behavior, trade-tail update math, event shape, or public API
  behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `MPC_Order` builder split

Done:

- Moved client-originated `MPC_Order` builders into
  `commands/trade/builders.rs`: `TradeCtx`, header write helpers, and every
  public `build_*` function.
- Kept public paths unchanged through `commands::trade::*` re-exports.
- Kept parser/command structs in `commands/trade.rs`, fixed packed records in
  `commands/trade/records.rs`, and Delphi ordinal wrappers in
  `commands/trade/enums.rs`.
- Private header write helpers remain visible only for the existing byte-level
  tests; they are not promoted to public API.
- This is a readability/publication split only. No outbound wire bytes,
  command IDs, header layout, `TradeCtx` shape, or public builder behavior
  changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `ClientSender` subscription split

Done:

- Moved `ClientSender` orderbook/trades subscription helpers into
  `client/sender/subscriptions.rs`.
- Kept the public sender API unchanged: same method names, arguments, fallible
  variants, and fire-and-forget logging.
- Kept subscription registry machine effect unchanged: update the reconnect
  registry first, refresh the shared summary/scope, then send the Delphi
  `TEngineRequest` only when the domain gate is ready.
- Kept trades storage scope as an active-library-only decision; the wire command
  is still `emk_SubscribeAllTrades`.
- This is a readability/publication split only. No subscription registry,
  reconnect restore, orderbook batch send, trades storage scope, request UID
  timestamping, or public sender behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `ClientSender` order helper split

Done:

- Moved `ClientSender` order/trade command helpers into
  `client/sender/orders.rs`.
- Kept the public sender API unchanged for new/replace/cancel/join/split/move
  orders, panic sell, immune clicks, VStop/stops, close/sell/split-position,
  status requests, and penalty commands.
- Kept the same builders, command kind (`MPC_Order`), priority (`High`),
  encryption flag, `MaxRetries`, and `UniqueKey` choices.
- Kept Delphi active-client local-state gates in the same order: check domain
  gate, mutate/query `Orders`, then queue the wire command only when the local
  helper returns a send request.
- This is a readability/publication split only. No outbound order bytes,
  order-cache mutation, keyed dedup behavior, retry count, or public sender
  behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `ClientSender` UI helper split

Done:

- Moved `ClientSender` UI command helpers into `client/sender/ui.rs`.
- Kept the public sender API unchanged for settings, strat start/stop UI
  commands, MM-order subscribe, update-version, emu trades, leverage/trigger
  manage, reset profit, arb notify, dex switch, and spot switch.
- Kept the same command kind (`MPC_UI`), priority, encryption flag,
  `MaxRetries`, and `UniqueKey` choices.
- Kept Delphi `ServerUpdateSent` side effect attached only to update/switch
  commands that successfully pass the domain gate and queue send work.
- This is a readability/publication split only. No outbound UI bytes,
  subscription registry mutation, domain gate behavior, server-update flag, or
  public sender behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `ClientSender` strategy/balance helper split

Done:

- Moved `ClientSender` strategy command helpers into `client/sender/strat.rs`.
- Moved `ClientSender` balance refresh helper into `client/sender/balance.rs`.
- Kept the public sender API unchanged for schema request, strategy snapshot
  send, strategy delete, sell-price update, checked sync, and balance refresh.
- Kept the same command kind (`MPC_Strat` / `MPC_Balance`), priority,
  encryption flag, `MaxRetries`, and `UniqueKey` choices.
- Kept `TStratSnapshot` as sliced/keyed by `UniqueKey::strat_snapshot()` and
  kept balance refresh as high-priority non-sliced `MPC_Balance`.
- This is a readability/publication split only. No outbound strategy/balance
  bytes, strategy snapshot serialization, domain gate behavior, or public
  sender behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick FireTest OK: `FIRETEST_QUICK_PASS after 23.59s`,
  `ParseFailed=0`, `rx_actual_drop=10.75%`, streams/API/market health OK,
  reader max `717us`, writer CPU max `124us`, GetMarketsList apply `2314us`.

### 2026-05-26 - `MPC_UI` builder split

Done:

- Moved outbound `MPC_UI` builders into `commands/ui/builders.rs`.
- Kept public builder paths unchanged through `commands::ui::*` re-exports.
- Kept UI types, parser, soft-tail client-settings fallback, and tests in the
  parent UI module for focused parser сверка.
- Kept `write_header` and `copy_blob_prefix` in the parent module because tests
  and packed-record fallback already use them directly.
- This is a readability/publication split only. No UI command ID, version byte,
  field order, packed record layout, `ServerUpdateSent` caller behavior, or
  public API behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `emk_AuthCheck` parser split

Done:

- Moved `AuthCheckResponse`, `DexInfo`, packed `THLDexInfo` wire layout, and
  `parse_auth_check_response` into `commands/engine_api/auth_check.rs`.
- Kept public paths unchanged through `commands::engine_api::*` re-exports.
- Kept the same parent zero-tail/string helpers and Delphi soft-tail parsing:
  mandatory fields first, optional Phase 2 fields only while not EOF, partial
  18-byte DEX record reads into zero-filled slots.
- This is a readability/publication split only. No `emk_AuthCheck` field
  order, soft-tail behavior, packed DEX layout, or public API behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `emk_BaseCheck` parser split

Done:

- Moved `ServerInfo`, `exchange_type_flags`, and
  `parse_base_check_response` into `commands/engine_api/base_check.rs`.
- Kept public paths unchanged through `commands::engine_api::*` re-exports.
- Kept Delphi optional-field parsing unchanged: read fields in server order,
  stop at the first missing/truncated field, and return the partially filled
  `ServerInfo`.
- This is a readability/publication split only. No `emk_BaseCheck` field order,
  truncate tolerance, identity helper behavior, or public API behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `MarketsState` accessor split

Done:

- Moved `MarketsState` read-model accessors into
  `state/markets/accessors.rs`.
- Kept apply/update algorithms in `state/markets.rs` for focused Delphi сверка.
- Kept exact lookup semantics from the current Rust code: stable handle lookup
  by exact market name, `mIndex` lookup gated by `indexes_synchronized`, prices
  through `by_name`, tags defaulting to `TokenTags::empty()`, and trade state
  only for known `by_name` markets.
- This is a readability/publication split only. No COW handle behavior,
  market-index gating, price/tag/trade-state lookup behavior, or public API
  behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `MarketHistoryStore` config split

Done:

- Moved retained-history scope/configuration into
  `state/history_store/config.rs`: `TradeStorageScope`,
  `MarketHistoryConfig`, slot-size budget constants, and system-memory probing.
- Kept runtime registry/store/apply logic in `state/history_store.rs` for
  focused work on trades/candles/derived analytics.
- Kept system memory behavior unchanged on Windows, Unix, and fallback targets;
  tests still use the same `GIB` helper through a test-only parent import.
- This is a readability/publication split only. No history capacity defaults,
  memory-budget math, scope semantics, or public API behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `MarketHistoryRegistry` split

Done:

- Moved market-level retained-history registry into
  `state/history_store/registry.rs`.
- Kept per-market `MarketHistoryStore` append/compact/candle/derived logic in
  `state/history_store.rs`.
- Kept exact registry behavior: `scope=None` clears all stores/index slots,
  scoped markets produce `None` index slots, existing stores are retained only
  when present in the desired set, and configured names are stored as `Arc<str>`.
- This is a readability/publication split only. No retained-history
  configuration, per-index lookup, reader fanout, compaction fanout, or public
  API behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick FireTest OK: `FIRETEST_QUICK_PASS after 26.69s`,
  `ParseFailed=0`, `rx_actual_drop=10.32%`, streams/API/market health OK,
  retained futures trades `42`, reader max `704us`, writer CPU max `874us`,
  GetMarketsList apply `2447us`.

### 2026-05-26 - `EventDispatcher` public type split

Done:

- Moved public event/read-model types into `events/types.rs`:
  `StrategySnapshotReply`, `MissingOrderStatusRequest`, watcher fill events,
  and `Event`.
- Kept dispatcher state, active dispatch, and channel dispatch logic in
  `events.rs` for focused Delphi/protocol сверка.
- Kept public paths unchanged through `events::*` re-exports.
- This is a readability/publication split only. No event variant shape,
  strategy snapshot normalization, watcher fill data, dispatch order, or public
  API behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `MPC_Order` trace payload split

Done:

- Moved trade visual/trace payloads into `commands/trade/trace.rs`:
  `trace_flags`, `OrderTracePoint`, `CorridorUpdate`, and
  `BulkReplaceNotify`.
- Kept public paths unchanged through `commands::trade::*` re-exports.
- Kept all readers byte-identical: same `MarketCommandHeader`, same zero-tail
  scalar readers, same flag bits, same UID vector count handling, and same
  `adjust_time` side effect.
- This is a readability/publication split only. No `MPC_Order` command ID,
  field order, trace flag semantics, parser behavior, or public API behavior
  changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick FireTest OK: `FIRETEST_QUICK_PASS after 23.82s`,
  `ParseFailed=0`, `rx_actual_drop=9.16%`, streams/API/market health OK,
  retained futures trades `6`, reader max `669us`, writer CPU max `186us`,
  GetMarketsList apply `2558us`.

### 2026-05-26 - `MPC_UI` parser dispatch split

Done:

- Moved the inbound `UICommand::parse` / `parse_with_client_settings_fallback`
  dispatch match into `commands/ui/parser.rs`.
- Kept UI payload structs, fixed packed records, builders, and the existing
  soft-tail `TClientSettingsCommand` helper block in `commands/ui.rs`.
- Kept every `CmdId` branch byte-identical: same version gate, same UID/header
  read, same zero-tail/preserve-tail readers, same fallback settings argument,
  and same public `UICommand` variants.
- This is a readability/publication split only. No `MPC_UI` command IDs,
  parser branch order, wire fields, fallback behavior, or public API behavior
  changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `MPC_UI` soft-tail parser helper split

Done:

- Moved `TClientSettingsCommand` parse helpers, ShortString[15] reader,
  zero-tail/preserve-tail scalar readers, and the inbound
  `EmuTradePoint::read_from_delphi_stream` helper into `commands/ui/parser.rs`.
- Kept payload structs, wire packed records, outbound builders, and the public
  `UICommand` enum in `commands/ui.rs`.
- Kept byte behavior unchanged: same mandatory settings block, same v2 branch,
  same fallback values, same Delphi `<` guard for ASCfg sized blobs, same
  compact ArbConfig parsing, and same zero-tail/preserve-tail semantics.
- This is a readability/publication split only. No settings wire fields,
  fallback behavior, builder behavior, or public API behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick FireTest OK: `FIRETEST_QUICK_PASS after 23.56s`,
  `ParseFailed=0`, `rx_actual_drop=10.51%`, streams/API/market health OK,
  retained futures trades `1`, reader max `723us`, writer CPU max `141us`,
  GetMarketsList apply `2415us`.

### 2026-05-26 - `MPC_Order` header split

Done:

- Moved Delphi-shaped `TBaseCommand`, `TBaseMarketCommand`, and
  `TTradeEpochCommand` Rust equivalents into `commands/trade/headers.rs`.
- Kept public paths unchanged through `commands::trade::*` re-exports:
  `BaseCommandHeader`, `MarketCommandHeader`, and `TradeEpochHeader`.
- Kept header behavior byte-identical: same 11-byte base header, same
  currency/platform bytes, same Delphi UTF-8 string reader/writer, same
  epoch/status tail, and same zero-tail status reads.
- This is a readability/publication split only. No `MPC_Order` command IDs,
  parser branch order, outbound builders, state apply behavior, or public API
  behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `MPC_Order` command envelope split

Done:

- Moved public `TradeCommand` enum, `TradeCommand::parse`, and `uid()` matcher
  into `commands/trade/command.rs`.
- Kept public path unchanged through `commands::trade::*` re-export.
- Kept dispatch behavior byte-identical: same future-version skip to
  `Unknown`, same `CmdId` match order, same boxed variants, same header readers,
  same UID extraction for every command variant, and same unknown UID fallback.
- This is a readability/publication split only. No `MPC_Order` wire fields,
  command variant shape, parser behavior, state apply behavior, or public API
  behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick FireTest OK: `FIRETEST_QUICK_PASS after 23.76s`,
  `ParseFailed=0`, `rx_actual_drop=10.78%`, streams/API/market health OK,
  retained futures trades `5`, reader max `756us`, writer CPU max `137us`,
  GetMarketsList apply `2450us`.

### 2026-05-26 - `EventDispatcherSnapshot` split

Done:

- Moved `EventDispatcherSnapshot` and `EventDispatcher::snapshot()` into
  `events/snapshot.rs`.
- Kept public path unchanged through `events::EventDispatcherSnapshot` re-export.
- Kept snapshot behavior unchanged: same cloned read models, same
  `local_strategy_epoch`, same strategy snapshot helper methods, and same
  exclusion of queued events/callback hooks from the read-only snapshot.
- This is a readability/publication split only. No dispatch behavior, callback
  queue behavior, read-model shape, or public API behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `EventDispatcher` retained-history split

Done:

- Moved retained-history worker wiring into `events/history.rs`: handle attach/
  clear/default-enable, reader/derived/volume getters, candle snapshot apply,
  trade-storage scope sync, default worker creation, market-index slots, and
  active-lib storage visibility gates.
- Kept exact current behavior: `Candle5mRow::from_deep_price`, singular
  `apply_candles_snapshot`, `MarketHistoryWorker::spawn(Default)`, LastPrice
  backfill on enabling storage, server-index-first slot configuration, and
  scoped `contains()` filtering.
- Kept sibling module visibility through `pub(super)` only for the helpers that
  active/API/trades dispatch already called before the split.
- This is a readability/publication split only. No retained-history state,
  scope behavior, worker lifecycle, stream filtering, or public API behavior
  changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick FireTest OK: `FIRETEST_QUICK_PASS after 23.75s`,
  `ParseFailed=0`, `rx_actual_drop=10.91%`, streams/API/market health OK,
  retained futures trades `3`, reader max `724us`, writer CPU max `146us`,
  GetMarketsList apply `2264us`.

### 2026-05-26 - `EventDispatcher` local strategy API split

Done:

- Moved local strategy API and `TStratSnapshotRequest` reply provider helpers
  into `events/local_strats.rs`.
- Kept public methods unchanged: local epoch, strategy list replace/upsert/
  clear/read, checked delta send, start/stop command helper, and snapshot
  provider registration.
- Kept active-path visibility only where existing code needs it:
  `strategy_snapshot_reply` stays `pub(super)` for active dispatch, and
  `local_strategy_snapshot_reply` stays `pub(crate)` for init.
- This is a readability/publication split only. No strategy snapshot payload,
  epoch, checked-delta, provider fallback, command sending, or public API
  behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `MarketsState` tags/indexes split

Done:

- Moved Delphi `emk_CheckBinanceTags` apply logic into
  `state/markets/tags.rs`.
- Moved server `mIndex` mapping, stale-index gate, and orderbook
  `ChartPriceStep` refresh helper into `state/markets/indexes.rs`.
- Kept behavior unchanged: same tag clear/apply/retain sequence, same late-read
  tag failure semantics, same `indexes_synchronized` gate, same cold-start
  direct-index fallback, and same missing-market refresh detection.
- This is a readability/publication split only. No market list apply, price
  apply, tag apply, stream gate, or public API behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `MarketsState` currency/ref split

Done:

- Moved CorrMarket list apply, base-currency dictionary maintenance,
  `refBTCMarket` resolution, and base-currency price recalculation into
  `state/markets/currency.rs`.
- Moved Delphi `SameText`-style ASCII market/currency helpers into
  `state/markets/text.rs`.
- Kept behavior unchanged: empty-base CorrMarkets still skip, existing
  CorrMarkets still update tick size/base currency only, server base currency
  changes still run `check_corr -> check_currency_ref -> update_prices`, and
  USDT/direct/reverse price fallback order is unchanged.
- This is a readability/publication split only. No market list apply, price
  apply, base-currency, CorrMarket, or public API behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick FireTest OK: `FIRETEST_QUICK_PASS after 25.60s`,
  `ParseFailed=0`, `rx_actual_drop=12.04%`, init/markets/indexes/tags/update,
  trades and orderbook health OK, retained futures trades `11`, reader max
  `698us`, writer CPU max `152us`, GetMarketsList apply `2547us`.

### 2026-05-26 - `Orders` maintenance split

Done:

- Moved deferred removal, bulk-replace timeout tick, missing-after-snapshot,
  `ServerTimeDelta` setter, `remove`, and `clear` into
  `state/orders/maintenance.rs`.
- Kept `ProcessCommandOrder`/`Orders::apply_at` in `orders.rs`; the split only
  separates post-apply worker-loop maintenance and read-model cleanup.
- Kept behavior unchanged: same deferred-removal due time merge, same
  `SelLDone` 400 ms grace use by caller, same 5000 ms bulk-replace timeout,
  same snapshot-flag cleanup candidate rule, and same state cleared on reset.
- This is a readability/publication split only. No order apply, deferred
  cleanup, bulk-replace, snapshot cleanup, or public API behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `Orders` apply helper split

Done:

- Moved low-level order apply helpers into `state/orders/apply_helpers.rs`:
  epoch/phase guard, full `TOrderStatus` field copy, `SelLDone` done flags,
  and server trace-line application.
- Kept the main `Orders::apply_at` match in `orders.rs` so the Delphi
  `ProcessCommandOrder` skeleton remains readable in one place.
- Kept behavior unchanged: same epoch stale window, same phase rollback check,
  same `OS_None` pending visual-order handling, same local price preservation,
  same done-flag mutation, and same trace-line initial/temp/finish handling.
- This is a readability/publication split only. No order apply, trace,
  terminal status, epoch, or public API behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
- Quick FireTest OK: `FIRETEST_QUICK_PASS after 24.80s`,
  `ParseFailed=0`, `rx_actual_drop=9.73%`, init/markets/indexes/tags/update,
  trades/orderbook/order path health OK, retained futures trades `2`, reader
  max `729us`, writer CPU max `133us`, GetMarketsList apply `2319us`.

### 2026-05-26 - `Client` constants/helpers split

Done:

- Moved Delphi timing/wire constants into `client/constants.rs`.
- Moved pure client helper functions into `client/helpers.rs`: domain-push
  gates, trades-stream gate, datagram-too-large OS mapping, Engine API request
  UID/method extraction, pre-Init API allowlist, timeout remaining, and queued
  settings-event check.
- Kept `client.rs` private imports (`use constants::*`, `use helpers::*`) so
  existing child modules/tests see the same names without changing call sites.
- This is a readability/publication split only. No timing constant, Init gate,
  send gate, reconnect, compression, or public API behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `Client` session/lifecycle API split

Done:

- Moved server identity, AuthCheck metadata, trade-route context builders, and
  `ServerTimeDelta` handle getter into `client/session_api.rs`.
- Moved lifecycle callback delivery and Delphi `ServerUpdateSent` marker into
  `client/lifecycle.rs`.
- Kept internal visibility unchanged for child modules: `fire_lifecycle`,
  `check_lifecycle_transition`, and `take_server_update_sent` are
  `pub(super)` because protocol/init modules call them.
- This is a readability/publication split only. No server identity storage,
  AuthCheck storage, trade route derivation, lifecycle event ordering,
  `ServerUpdateSent`, or public API behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `Client` diagnostic API consolidation

Done:

- Moved err_emu diagnostics and passive protocol metrics getters from
  `client.rs` into the existing `client/diagnostic_api.rs` module.
- Kept diagnostic API names and behavior unchanged:
  `err_emu_diagnostics_snapshot`, `reset_err_emu_diagnostics`,
  `protocol_metrics_snapshot`, and
  `protocol_metrics_snapshot_with_dispatcher`.
- This is a readability/publication split only. No diagnostics counters,
  FireTest visibility, protocol metrics, or public API behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `StrategySerializer` public type split

Done:

- Moved the public strategy snapshot data model into
  `commands/strategy_serializer/types.rs`: `FieldValue`, `StrategySnapshot`,
  `StrategyFields`, `StrategyKind`, and `StrategyActiveMode`.
- Kept the parent module as the wire-format reader/writer owner:
  dictionary reading, field TypeID checks, schema filtering, DEFLATE handling,
  and writer field order remain in `strategy_serializer.rs`.
- This is a readability/publication split only. No strategy snapshot wire
  bytes, TypeID mapping, zero/default filtering, schema behavior, or public API
  names changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `Client` domain send helper split

Done:

- Moved shared typed-send plumbing from `client/domain_commands.rs` into
  `client/domain_send.rs`: generic domain send, keyed domain send, order-channel
  send, keyed order-channel send, local cancel request send, and panic-sell send.
- Kept the public domain command wrappers in `domain_commands.rs`; they still
  build the same Delphi command payloads and call the same helper names.
- This is a readability/publication split only. No `Command`, priority,
  encryption flag, retry count, UniqueKey, local order mutation, or reconnect
  behavior changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `Client` balance command split

Done:

- Moved Balance-channel high-level wrappers into `client/domain_balance.rs`:
  `balance_request_refresh` and `request_balance_snapshot`.
- Kept the helpers as inherent `Client` methods with the same names and
  signatures, so callers and API docs remain unchanged.
- This is a readability/publication split only. No `Command::Balance` payload,
  retry/priority/encryption settings, dispatcher pump loop, timeout behavior,
  or balance snapshot detection changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `Client` strat command split

Done:

- Moved Strat-channel high-level wrappers into `client/domain_strat.rs`:
  schema request, snapshot payload/batch send, delete, sell-price update, and
  checked sync.
- Kept the Strat snapshot sender helper with the Strat wrappers because it is
  the `TStratSnapshot` `UK_StratSnapshot`/Sliced send path.
- This is a readability/publication split only. No `Command::Strat` payload,
  retry/priority/encryption settings, UniqueKey behavior, schema request,
  snapshot format, or public method signature changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `Client` UI command split

Done:

- Moved UI-channel high-level wrappers into `client/domain_ui.rs`: settings
  send/request, settings wait helper, strategy start/stop UI commands,
  MM-orders subscribe, update/version, emu trades, lev/trigger manage,
  profit reset, arb activate notify, DEX switch, and spot switch.
- Kept the wrappers as inherent `Client` methods with the same names and
  signatures. `ui_update_version`, `ui_switch_dex`, and `ui_switch_spot` still
  mark `ServerUpdateSent` only after a successful send helper call.
- This is a readability/publication split only. No `Command::UI` payload,
  retry/priority/encryption settings, UniqueKey behavior, settings retry loop,
  or public method signature changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.

### 2026-05-26 - `Client` trade command module rename

Done:

- Renamed the remaining `client/domain_commands.rs` module to
  `client/domain_trade.rs` after UI/Strat/Balance wrappers were split out.
- The file now contains only high-level Order/Trade wrappers over
  `commands::trade::build_*` plus the local order-state helper wrappers that
  match Delphi UI order actions.
- This is a readability/publication rename only. No `Command::Order` payload,
  retry/priority/encryption settings, UniqueKey behavior, local order cache
  mutation, or public method signature changed.

Verification:

- `cargo fmt --all` OK.
- `cargo test --lib --quiet` OK: 769 passed, 1 ignored.
- `cargo check --examples --quiet` OK.
