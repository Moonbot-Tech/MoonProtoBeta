# MoonProto Rust: рабочий план перехода на Delphi threading model

Дата: 2026-05-22

Статус: рабочий документ для перестройки `moonproto`.

## Вердикт

Да, Rust-клиент можно организовать по модели Delphi: отдельный reader thread и отдельный
orchestrator/writer thread. Более того, это надо сделать, потому что текущий Rust уже имеет
физический reader thread, но семантически протокол всё ещё однопоточный: reader только принимает
UDP-пакет и кладёт `ClientEvent::Recv` в очередь, а вся протокольная обработка живёт в
`Client::run_inner`.

Именно из-за этого появились Rust-only сущности: общий `EVENT_DRAIN_BUDGET`, deferred recv,
смешивание user send intents и server recv packets в одном оркестраторе, зависимость API responses
от того, крутит ли пользователь `run_*`, и расхождения в SlicedACK/retry/Init timing.

Цель перестройки: не "похоже на Delphi", а одинаковый machine effect по блокам:
какой поток читает байты, какой поток мутирует очереди, когда ACK применяется, когда отправляется
ответный пакет, какой таймер двигается, и какой state видит следующий шаг.

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

- `src/client.rs:175` - `EVENT_DRAIN_BUDGET = 512`.
- `src/client.rs:359` - `ClientEvent`.
- `src/client.rs:1085-1092` - `event_tx/event_rx` и `app_events`.
- `src/client.rs:3440` - `run_inner`.
- `src/client.rs:3487-3507` - main loop дренит app events и reader events одним бюджетом.
- `src/client.rs:3627` - `handle_main_event`.
- `src/client.rs:3662` - `process_recv_event`.
- `src/client.rs:3690` - `process_recv_msg`.
- `src/client.rs:3760` - `spawn_reader`.
- `src/client.rs:3942` - `Command::Sliced`.
- `src/client.rs:3963` - `Command::SlicedACK`.
- `src/client.rs:4242` - ping handling и immediate `pending_h.retain`.
- `src/events.rs:753` - `EventDispatcher::dispatch_into_active` требует `&mut Client` и может сам слать API через `client.send_api_request`.

Текущее устройство Rust:

1. Reader thread:
   - принимает UDP;
   - делает outer unpack/checksum/ver/ErrEmu;
   - отправляет `ClientEvent::Recv` в `event_tx`;
   - не делает Sliced reassembly;
   - не шлёт SlicedACK сам;
   - не применяет handshake/DataRead/domain packets.

2. Main loop:
   - дренит app send intents и recv packets через общий loop;
   - critical recv обрабатывает сразу, noncritical recv откладывает в `deferred_recv`;
   - создаёт Sliced, применяет SlicedACK, ретраит, dispatch'ит active state;
   - API response приходит пользователю только пока этот loop крутится.

Это не Delphi model. Это физически два потока, но протокольно один поток плюс очередь между UDP и протоколом.

## Главные расхождения, которые надо убрать архитектурно

### 1. Recv backlog не должен задерживать transport receive effects

В Delphi `MPC_Sliced` получает ACK немедленно из reader path. В Rust ACK уходит только когда main loop
дойдёт до `Command::Sliced`. При burst/large sliced/err_emu это меняет timing и может ломать recovery.

Target: `MPC_Sliced` обрабатывается в reader thread: `SlicingReceiver::on_new_sliced`, immediate
`send_raw_packet(MPC_SlicedACK)`, complete datagram идёт в `DataReadInt` path.

### 2. SlicedACK не должен применяться в reader

В Delphi reader складывает `MPC_SlicedACK` в `ACKs`, writer применяет ACK внутри `CheckSeningData`.
Сейчас Rust применяет ACK прямо в `Command::SlicedACK` branch текущего main loop. После перестройки:
reader парсит ACK и кладёт в `ack_queue`; writer в своём тике делает `copy_acks` и `apply_ack`.

Это важно для порядка: Delphi ACK применяется в одном writer cycle вместе с send/retry decisions.

### 3. Ping H ACK bitmap должен идти через TmpSlider -> RecvdSlider -> ApplyRegularHLAck

В Delphi `DataReadInt(MPC_Ping)` под `SendLock` пишет `TmpSlider`, writer копирует это через
`CopyRecvdData`, потом `ApplyRegularHLAck` чистит `PendingH`.

Сейчас Rust в `handle_ping` сразу делает `pending_h.retain`. Это поведенчески близко, но не тот же
machine effect и не тот же порядок относительно send queue snapshot.

Target: reader пишет `tmp_slider`; writer копирует и применяет H ACK в `check_sending_data`.

### 4. User/app send intents не должны конкурировать с reader packets в общем event budget

В Delphi `SendCmdInt` пишет в send queues под lock. Входящий поток не может "съесть" бюджет обработки
и задержать постановку user команды в queue.

Target: public send APIs пишут в `SendQueues` напрямую через lock или через thin `ClientSender`, но не
через общий `ClientEvent` вместе с server recv.

### 5. `run_*` не должен быть мотором протокола

В Delphi transport работает пока жив thread. Блокирующий `SendAndWait` не обязан вручную качать UDP
receive path; reader и writer продолжают жить.

Target: `Client::start`/constructor поднимает worker'ы; `run_*` становится consumer'ом public events
или compatibility pump, но не владельцем transport progress. `api_*` receiver должен получать response
без необходимости вызывать `run_until_response`.

### 6. Active lib сейчас сцеплена с `&mut Client`

`EventDispatcher::dispatch_into_active(..., client: &mut Client)` делает auto-actions через
`client.send_api_request`. Это мешает точной двухпоточной модели: receive/domain обработчик и writer
борются за `&mut Client`.

Target: active state должен выдавать `ClientAction`/`SendIntent` outbox, а не напрямую мутировать
transport client. Reader/domain path кладёт эти actions в send queues. User-visible events уходят в
отдельную public event queue.

## Целевая Rust-структура

### Типы

1. `Client`
   - public facade;
   - держит `Arc<ClientShared>`;
   - владеет JoinHandle'ами reader/writer;
   - public API только ставит commands/subscriptions/requests в queues и читает snapshots/events.

2. `ClientShared`
   - `Mutex<TransportCore>` для protocol state, где нужен общий доступ;
   - `Mutex<SendQueues>`: sliced/high/low, Delphi `DataToSend*`;
   - `Mutex<AckQueues>`: incoming SlicedACKs и ping `TmpSlider`;
   - `Mutex<ActiveCore>` или отдельный actor, если direct reader mutation нельзя сделать без больших locks;
   - atomics для lifecycle flags/shutdown.

3. `ReaderRuntime`
   - owns/clones UDP socket receive side;
   - делает `UDPRead` equivalent;
   - держит локальный recv buffer;
   - не блокируется на public callbacks;
   - никогда не кладёт raw accepted UDP packet в общий backlog как обязательный следующий шаг протокола.

4. `WriterRuntime`
   - owns send side или clone UDP socket;
   - делает `Execute` equivalent;
   - `copy_send_queues`, `copy_acks`, `copy_recvd_data`;
   - `check_sending_data`;
   - hello/reconnect/force disconnect.

5. `PublicEventQueue`
   - только для user-visible events;
   - не является частью transport progress;
   - если пользователь её не читает, transport всё равно работает.

### State ownership table

| State | Delphi owner/effect | Rust target owner/effect |
| --- | --- | --- |
| UDP receive buffer | reader thread | `ReaderRuntime` local |
| Outer unpack/checksum/ver/ErrEmu | reader thread | `ReaderRuntime` |
| Handshake receive state | reader writes client fields | `ReaderRuntime` mutates `TransportCore` under short lock |
| Send queues H/S/L/Sliced intents | `SendCmdInt` under `SendLock`, writer copies | `SendQueues` mutex, writer drains copy |
| Incoming Sliced receiver | reader mutates `AClient.Receiving` | reader mutates receive slicer state |
| Immediate SlicedACK | reader calls `SendCommand` | reader sends direct ACK through send socket helper |
| Incoming SlicedACKs | reader appends `ACKs`, writer copies/applies | reader appends `AckQueues.sliced`, writer copies/applies |
| Ping regular H ACK bitmap | reader writes `TmpSlider`, writer copies/applies | reader writes `tmp_slider`, writer copies/applies |
| PendingH | writer owns in `CheckSeningData` | writer owns/mutates |
| Outgoing `Sending` sliced | writer owns in `CheckSeningData` | writer owns/mutates |
| Domain active state | reader path via `OnNewData`, some UI queued | `ActiveCore` updated from receive path; UI/user events queued separately |
| Public callbacks/events | mixed: direct reader and `TThread.Queue` | never under core locks; public event queue/callback executor |

## Порядок перестройки

### Phase 0 - freeze current behavior

Already done before this doc:

- committed current Rust fixes in nested `moonproto`;
- committed root working rules/docs;
- `cargo test`: 360 passed before this architecture doc;
- live FireTest after Sliced parity still does not fully pass under `err_emu=10%`: outgoing candle request is sent and ACKed, server sends chunks, but only chunk `6/6` reaches full EngineResponse while large sliced chunks remain incomplete. Это не закрыто.

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

Replace current semantic dependence on `ClientEvent::Send` for user/API commands:

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
- complete incoming Sliced calls data dispatch without passing through `EVENT_DRAIN_BUDGET`;
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

Change `EventDispatcher::dispatch_into_active` shape:

Current:

```rust
dispatcher.dispatch_into_active(cmd, payload, now_ms, out, self)
```

Target:

```rust
let actions = active.dispatch_into_active(cmd, payload, now_ms, out);
send_queues.push_actions(actions);
```

Rules:

- active state owns markets/indexes/balances/orders/strats/settings state;
- strategy snapshot request is answered from library-owned strats state;
- reconnect maintenance actions are produced by active/transport state and queued to writer;
- user-visible events go to `PublicEventQueue`;
- no user callback while holding `TransportCore`/`ActiveCore` locks.

Tests:

- `TStratSnapshotRequest` produces snapshot reply from local strats state;
- `OrderBookEvent::RequestFullNeeded` queues `RequestOrderBookFull`;
- token change invalidates/rebuilds market indexes as Delphi does;
- public callback can stall without stopping transport receive/send.

### Phase 7 - demote `run_*` to compatibility/event API

After worker threads own progress:

- `run(duration, cb)` consumes public events for duration;
- `run_with_dispatcher` either becomes no-op wrapper around internal active state or a compatibility mirror;
- `run_until_response` no longer pumps protocol, it waits on receiver while workers continue.

API docs must be updated in the API docs themselves if signatures/semantics change.

Tests:

- call `api_base_check`, block on receiver, no manual `run_until_response`, response still arrives;
- callback not reading events does not stop ping/SlicedACK/retry;
- old examples either continue working or are intentionally updated with docs.

### Phase 8 - remove Rust-only budget/defer machinery

Delete after previous phases are green:

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

Start with Phase 1 + Phase 3 around Sliced receive, because FireTest currently proves the request is
outgoing and ACKed, but large incoming sliced candle chunks do not reliably assemble under `err_emu=10%`.

Do not start from public API cleanup. First make incoming Sliced machine effect match Delphi:

```text
UDPRead -> OnNewSliced -> SendCommand(MPC_SlicedACK) -> if complete DataReadInt
```

Only after that move SlicedACK/H ACK copy-apply order and then split the rest of the main loop.

## Progress log

### 2026-05-22 - Phase 3 partial

Done:

- `MPC_Sliced` receive state is shared with the reader thread.
- Reader calls the receive slicer and sends `MPC_SlicedACK` directly through UDP.
- Reader-side ACK path is covered by `reader_sends_sliced_ack_without_main_loop_tick`.
- `cargo test --lib`: 362 passed.

Not done:

- Full datagram still reaches `DataReadInt` through an internal `ClientEvent::SlicedComplete`.
- Exact Delphi target remains:
  `UDPRead -> OnNewSliced -> SendCommand(MPC_SlicedACK) -> if complete DataReadInt`
  inside the reader path, without `EVENT_DRAIN_BUDGET` as a protocol-progress dependency.
