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

- `src/client.rs` - `ClientEvent` has only coalesced reader `Wake`; accepted
  UDP packets and user sends are not represented as event variants.
- `src/client.rs` - `event_tx/event_rx`, `send_queues`,
  `pending_reader_decoded`.
- `src/client.rs` - `run_inner` drains coalesced reader wakeups and
  `pending_reader_decoded`, then calls
  `copy_send_ack_and_check_sening_data`, the Rust block matching Delphi
  `Execute -> GetCopySendList -> GetCopyAcks -> CopyRecvdData ->
  CheckSeningData`.
- `src/client.rs` - `spawn_reader` handles service commands, Sliced/SlicedACK,
  handshake, Ping, SizeTest/ProbeMTU, and data `DataReadInt` core in reader.
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
   - кладёт decoded payload/state updates в `pending_reader_decoded` и шлёт
     coalesced `ClientEvent::Wake` для user/active delivery.

2. Main loop:
   - дренит coalesced reader wakeups без `EVENT_DRAIN_BUDGET`;
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
- Reader wakeups for `pending_reader_decoded` are now level-triggered: dense
  reader-side `DataReadInt` progress coalesces into one `ClientEvent::Wake`
  until the main loop drains the decoded queue, so empty wake events no longer
  form a Rust-only backlog.
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
- Targeted reader wake coalescing test: passed.
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
- `EVENT_DRAIN_BUDGET` was removed; reader wake is level-triggered and does not
  carry user/API/UI send work.

### 2026-05-22 - Phase 6 partial: active actions outbox

Done:

- `EventDispatcher` now has `ActiveDispatchContext` and `ActiveAction`.
- Production `Client::process_decoded_data_read_int` snapshots the client
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
- The order is unchanged: drain reader delivery, wait/drain wake, drain reader
  delivery again, writer maintenance (`CheckSeningData`, cleanup, indexes,
  refresh, clock-jump), active trades tick, reconnect tail.
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
  command dispatch, and wake notification now live inside `ReaderRuntime`.
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

- Moved reader-wake wait, writer maintenance tick, reconnect tail tick,
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
