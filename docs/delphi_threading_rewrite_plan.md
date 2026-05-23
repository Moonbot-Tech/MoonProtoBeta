# MoonProto Rust: рабочий план перехода на Delphi threading model

Дата: 2026-05-22

Статус: рабочий документ для перестройки `moonproto`.

## Вердикт

Да, Rust-клиент можно организовать по модели Delphi: отдельный reader thread и отдельный
orchestrator/writer thread. Более того, это надо сделать, потому что исходный Rust уже имел
физический reader thread, но семантически протокол был однопоточным: reader только принимал
UDP-пакет и клал `ClientEvent::Recv` в очередь, а вся протокольная обработка жила в
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

- `src/client.rs` - accepted UDP packets and user sends are not represented as
  event variants.
- `src/client.rs` - `send_queues`, `incoming_sliced_acks`,
  `pending_reader_decoded`.
- `src/client.rs` - `WriterRuntime::run` polls `pending_reader_decoded`, then calls
  `copy_send_ack_and_check_sening_data`, the Rust block matching Delphi
  `Execute -> GetCopySendList -> GetCopyAcks -> CopyRecvdData ->
  CheckSeningData`.
- `src/client.rs` - `spawn_reader` handles service commands, Sliced/SlicedACK,
  handshake, Ping, SizeTest/ProbeMTU, and data `DataReadInt` core in reader.
- `src/client.rs` - production reader data path is now named like Delphi:
  `ReaderRuntime::data_read` handles `MPC_Grouped` and calls
  `ReaderRuntime::data_read_int`; completed Sliced datagrams call
  `data_read_int` before `Receiving` removal.
- `src/client.rs` - reader immediate replies use
  `ReaderRuntime::send_command`, matching the Delphi reader-side calls to
  `SendCommand` from SlicedACK/Ping/PMTU/ImFriend branches.
- `src/client.rs` - ping handling writes shared `TmpSlider`; writer later copies
  it and runs `ApplyRegularHLAck`.
- `src/events.rs:753` - `EventDispatcher::dispatch_into_active` требует `&mut Client` и может сам слать API через `client.send_api_request`.

Текущее устройство Rust:

1. Reader thread:
   - принимает UDP;
   - делает outer unpack/checksum/ver/ErrEmu;
   - выполняет reader-side cleanup cadence;
   - обрабатывает handshake/control exits, Ping, SizeTest/ProbeMTU;
   - `MPC_Sliced`: собирает slice, немедленно отправляет `MPC_SlicedACK`, при
     complete выполняет общий `DataReadInt` decrypt/decompress core;
   - `MPC_SlicedACK`: кладёт ACK в reader→writer ACK queue;
   - обычные data packets и `MPC_Grouped`: выполняет `DataReadInt` core;
   - кладёт decoded payload/state updates в `pending_reader_decoded` для
     user/active delivery.

2. Writer/orchestrator loop:
   - дренит `pending_reader_decoded` и выполняет `OnNewData`/active delivery;
   - создаёт outgoing Sliced, применяет SlicedACK, ретраит, dispatch'ит active state;
   - API response приходит пользователю только пока этот loop крутится.

Это всё ещё не полный Delphi model: transport/DataReadInt core уже reader-side,
но `OnNewData`/active-library delivery ещё зависит от main/run loop.

## Главные расхождения, которые надо убрать архитектурно

### 1. Recv backlog не должен задерживать transport receive effects

В Delphi `MPC_Sliced` получает ACK немедленно из reader path. Rust уже отправляет
`MPC_SlicedACK` из reader thread и для полного Sliced выполняет общий
`DataReadInt` decrypt/decompress core в reader stack, затем удаляет `Receiving`.
Оставшийся разрыв: `OnNewData`/active-library delivery всё ещё доезжает до user
code через main/run очередь.

Target: `MPC_Sliced` обрабатывается в reader thread: `SlicingReceiver::on_new_sliced`, immediate
`send_raw_packet(MPC_SlicedACK)`, complete datagram идёт в `DataReadInt` path,
а затем в reader-owned `OnNewData`/active dispatch без зависимости от
main-loop wake budgeting.

### 2. SlicedACK не должен применяться в reader

В Delphi reader складывает `MPC_SlicedACK` в `ACKs`, writer применяет ACK внутри `CheckSeningData`.
Rust now matches this part: reader parses ACK into `incoming_sliced_acks`; writer
tick does `get_copy_acks` and `apply_copy_acks`.

Это важно для порядка: Delphi ACK применяется в одном writer cycle вместе с send/retry decisions.

### 3. Ping H ACK bitmap должен идти через TmpSlider -> RecvdSlider -> ApplyRegularHLAck

В Delphi `DataReadInt(MPC_Ping)` под `SendLock` пишет `TmpSlider`, writer копирует это через
`CopyRecvdData`, потом `ApplyRegularHLAck` чистит `PendingH`.

Rust now matches the copy/apply order: ping handling writes shared `TmpSlider`,
writer copies it to `RecvdSlider`, then `ApplyRegularHLAck` removes ACKed
`PendingH`.

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
  32ms pause, and only queues the resulting state update for main-side fields.
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
- Targeted reader `WhoAreYou -> ImFriend, Sleep(32), ImFriend` test: passed.
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
- `reader_on_who_are_you` keeps Delphi's machine effect: decrypt server Hello
  with `MasterKey`, derive session keys, install reader decode cipher, build
  `ImFriend`, send it twice with the blocking 32ms delay, then queue the
  handshake state update.
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
  - `MPSlider.Init`, `TmpSlider.Init` -> shared `ReaderProtocolState::reset()`.
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
- Rust now stores per-side bulk-replace sent timestamps, sets them from the
  dispatcher `now_ms`, clears them on `OrderReplaceResponse`, and periodically
  clears stale flags through the dispatcher/order tick.
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
- `DELPHI_STRATEGY_FIELD_ORDER` and `DELPHI_STRATEGY_FIELD_TYPES` were compared
  against `Strategies.pas:TStrategy` public fields: `477/477`, no order/type
  mismatches.
- Confirmed Rust writer now emits known fields in Delphi public-field order, not
  the old alphabetical `HashMap` order.
- Fixed the separate parity risk from `spec_pipeline/work/хуйня.md §X.90`:
  typed `StrategyBatchBuilder` now filters back to Delphi `SaveStrategyToCompact`
  semantics before writing. Unknown fields, wrong TypeID values, and values equal
  to `TStrategy.Create` defaults are not wire-visible. The only runtime-default
  caveat is `SellOrderColor`/`BuyOrderColor`: Delphi reads those defaults from
  current `Vars` color state, so Rust callers should omit them unless they are
  explicit overrides.

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
- Rust previously replaced `BalancesState::by_market` with only incoming items,
  which lost those preserved fields and could make a later incremental with
  `bnMaxValue=0` or stale epoch behave differently.
- Rust now keeps previous missing rows as reset/default rows while preserving
  `balance_hash`, `max_value`, and per-market epoch. Unknown markets that are
  not present in current `MarketsState` are still ignored like Delphi
  `Markets.MarketByNameFast`.
- Added a regression test and recorded `spec_pipeline/work/хуйня.md §X.91`.
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
  only after a valid `TradesStream` / `TradesResendResponse` produced
  `TradesEvent::Apply`. Generated `emk_TradesResend` payloads are sent through
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
