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
- strategy snapshot request is answered from library-owned strats state;
- reconnect maintenance actions are produced by active/transport state and queued to writer;
- user-visible events go to `PublicEventQueue`;
- no user callback while holding `TransportCore`/`ActiveCore` locks.

Tests:

- `TStratSnapshotRequest` produces snapshot reply from local strats state;
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

### 2026-05-22 - Phase 1 partial: moved writer periodic helpers into WriterRuntime

Done:

- Moved the remaining writer tick helper bodies from `Client` into
  `WriterRuntime`: `tick_periodic_refresh`, `tick_periodic_refresh_at`,
  `check_indexes_fetch_timeout`, `check_clock_jump`, and
  `periodic_trades_tick`.
- `transport_writer_maintenance_tick` now calls these same-runtime methods.
- The method bodies were moved without changing the queue/send side effects:
  markets-index timeout retry, periodic market/tag refresh, clock-jump forced
  reconnect, and dispatcher-only trades tick keep the same state transitions and
  packet enqueue points.
- Unit tests for clock-jump/index timeout and periodic refresh now instantiate
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
