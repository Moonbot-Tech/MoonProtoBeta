use super::metrics::ProtocolMetrics;
use super::sender::ClientSender;
use crate::api_pending::ApiPending;
use crate::protocol::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Instant;
/// Raw callback used by [`crate::client::Client::run`].
///
/// This callback receives decoded MoonProto command payloads after transport
/// decrypt/decompress/group handling, but before `EventDispatcher` state
/// application. Regular applications should use [`crate::MoonClient`] instead.
/// The callback runs from the application callback queue, not inside the
/// protocol writer tick.
pub type OnDataFn = Box<dyn FnMut(Command, &[u8]) + Send>;
pub(crate) type RawAppEvent = (Command, Vec<u8>);
pub(crate) type StateAppEvent = (
    crate::events::Event,
    Arc<crate::events::EventDispatcherSnapshot>,
);

pub(crate) enum DispatcherWorkItem {
    Data {
        cmd: Command,
        payload: Vec<u8>,
        now_ms: i64,
        ctx: crate::events::ActiveDispatchContext,
    },
    DrainDeferredOrderRemovals {
        now_ms: i64,
    },
    TickOrders {
        now_ms: i64,
    },
    ResetOrderbookCachesKeepBooks,
    Barrier {
        done: mpsc::Sender<()>,
    },
}

/// Callback that receives typed events from a low-level active-library pump.
///
/// The callback runs from the application callback queue after dispatcher state
/// has been updated. Blocking this callback does not block protocol ACK/retry
/// progress.
pub type EventFn = Box<dyn FnMut(&crate::events::Event) + Send>;

/// Callback that receives an event plus the updated read-only dispatcher state.
///
/// Low-level callback variant for custom runtimes that need the applied read
/// model together with every event.
pub type EventWithStateFn =
    Box<dyn FnMut(&crate::events::Event, &crate::events::EventDispatcherSnapshot) + Send>;

/// Куда доставлять `Command + payload` после внутренней обработки (decrypt,
/// decompress, Grouped split, API pending dispatch). Два варианта:
///
/// * `Callback` — raw payload callback через `OnDataFn` (используется `Client::run`).
/// * `Buffer` — буфер (Command, Vec<u8>) для пост-обработки через
///   `EventDispatcher` (используется low-level active pump).
///
/// Этот enum позволяет одному delivery pipeline (`ProtocolCore` drain +
/// `client_new_data_decoded`) обслуживать оба сценария без
/// `Arc<Mutex>`-обходов borrow checker.
pub(crate) enum DispatchSink<'a> {
    #[cfg(test)]
    Callback(&'a mut OnDataFn),
    CallbackQueue(&'a mpsc::Sender<RawAppEvent>),
    Buffer(&'a mut Vec<(Command, Vec<u8>)>),
}

impl<'a> DispatchSink<'a> {
    #[inline]
    pub(crate) fn is_buffer(&self) -> bool {
        matches!(self, Self::Buffer(_))
    }

    /// Доставка с уже-владеемым Vec (avoid лишний `to_vec`, когда payload
    /// родился из decrypt/decompress и уже Owned).
    #[inline]
    pub(crate) fn deliver_owned(&mut self, cmd: Command, payload: Vec<u8>) {
        match self {
            #[cfg(test)]
            Self::Callback(cb) => cb(cmd, &payload),
            Self::CallbackQueue(tx) => {
                let _ = tx.send((cmd, payload));
            }
            Self::Buffer(buf) => buf.push((cmd, payload)),
        }
    }
}

/// Режим работы main loop — определяет как доставлять входящие data-пакеты
/// и нужны ли active-library auto-actions.
///
/// `CallbackQueue` — low-level raw path для `Client::run`. Потребитель получает
/// сырые `(Command, &[u8])` и сам решает что с ними делать (обычно — свой
/// `dispatcher.dispatch_into(...)`). Production delivery goes through app
/// queue.
///
/// `Dispatcher` — active-library path для low-level finite pump. Liба
/// сама пропускает data-пакеты через `EventDispatcher::dispatch_into_active_actions`,
/// делает auto-actions (RequestOrderBookFull, trades resend tail-check, indexes
/// sync gate), потребитель получает уже разобранные типизированные `Event`.
pub(crate) enum RunMode<'a> {
    #[cfg(test)]
    Callback {
        on_data: OnDataFn,
    },
    CallbackQueue {
        app_tx: mpsc::Sender<RawAppEvent>,
    },
    #[cfg(test)]
    Dispatcher {
        dispatcher: &'a mut crate::events::EventDispatcher,
        on_event: DispatcherEventFn,
        /// Переиспользуемый буфер событий (избегаем alloc per packet).
        event_buf: Vec<crate::events::Event>,
        /// Переиспользуемый буфер decoded payload'ов перед dispatcher.
        payload_buf: Vec<(Command, Vec<u8>)>,
        /// Переиспользуемый буфер active-library side effects.
        active_actions_buf: Vec<crate::events::ActiveAction>,
    },
    DispatcherWorker {
        tx: mpsc::Sender<DispatcherWorkItem>,
        /// Переиспользуемый буфер decoded payload'ов перед worker FIFO.
        payload_buf: Vec<(Command, Vec<u8>)>,
    },
    #[cfg(not(test))]
    _Lifetime(std::marker::PhantomData<&'a ()>),
}

/// Два варианта event callback'а: только `&Event` или `(&Event, &EventDispatcherSnapshot)`.
/// Изоляция позволяет иметь два low-level finite pump варианта без дубликации
/// main loop кода.
pub(crate) enum DispatcherEventFn {
    QueueToCallback(mpsc::Sender<crate::events::Event>),
    QueueToStateCallback(mpsc::Sender<StateAppEvent>),
    Queue,
}

impl DispatcherEventFn {
    pub(crate) fn drain_events(
        &mut self,
        events: &mut Vec<crate::events::Event>,
        dispatcher: &mut crate::events::EventDispatcher,
        protocol_metrics: &ProtocolMetrics,
        source_cmd: Option<Command>,
        source_api_method: u8,
        source_payload_len: usize,
    ) {
        if events.is_empty() {
            return;
        }
        let enqueue_start = Instant::now();
        let event_count = events.len();
        let mode = match self {
            Self::QueueToCallback(_) => 1,
            Self::QueueToStateCallback(_) => 2,
            Self::Queue => 3,
        };
        match self {
            Self::QueueToCallback(tx) => {
                for event in events.drain(..) {
                    let _ = tx.send(event);
                }
            }
            Self::QueueToStateCallback(tx) => {
                let snapshot = Arc::new(dispatcher.snapshot());
                for event in events.drain(..) {
                    let _ = tx.send((event, Arc::clone(&snapshot)));
                }
            }
            Self::Queue => {
                dispatcher.queue_events(events.drain(..));
            }
        }
        protocol_metrics.record_app_enqueue_labeled(
            enqueue_start.elapsed(),
            source_cmd.map_or(u8::MAX, Command::to_byte),
            source_api_method,
            source_payload_len,
            event_count,
            mode,
        );
    }
}

#[inline]
pub(crate) fn metric_api_method(cmd: Command, payload: &[u8]) -> u8 {
    if cmd == Command::API && payload.len() > 19 {
        payload[19]
    } else {
        u8::MAX
    }
}

pub(crate) fn run_dispatcher_worker(
    rx: mpsc::Receiver<DispatcherWorkItem>,
    dispatcher: &mut crate::events::EventDispatcher,
    mut on_event: DispatcherEventFn,
    sender: ClientSender,
    api_pending: Arc<ApiPending>,
    protocol_metrics: Arc<ProtocolMetrics>,
    trades_server_token_mirror: Arc<AtomicU64>,
) {
    let mut event_buf = Vec::with_capacity(8);
    let mut active_actions_buf = Vec::with_capacity(4);
    while let Ok(item) = rx.recv() {
        match item {
            DispatcherWorkItem::Data {
                cmd,
                payload,
                now_ms,
                ctx,
            } => {
                event_buf.clear();
                active_actions_buf.clear();
                let active_dispatch_start = Instant::now();
                dispatcher.dispatch_into_active_actions(
                    cmd,
                    &payload,
                    now_ms,
                    &mut event_buf,
                    &ctx,
                    &mut active_actions_buf,
                );
                trades_server_token_mirror
                    .store(dispatcher.trades_server_token(), Ordering::Relaxed);
                let event_count = event_buf.len();
                let action_count = active_actions_buf.len();
                sender.apply_active_actions(active_actions_buf.drain(..));
                dispatch_api_pending_from_events(&api_pending, &event_buf);
                protocol_metrics.record_active_dispatch_labeled(
                    active_dispatch_start.elapsed(),
                    cmd.to_byte(),
                    metric_api_method(cmd, &payload),
                    payload.len(),
                    event_count,
                    action_count,
                );
                on_event.drain_events(
                    &mut event_buf,
                    dispatcher,
                    &protocol_metrics,
                    Some(cmd),
                    metric_api_method(cmd, &payload),
                    payload.len(),
                );
            }
            DispatcherWorkItem::DrainDeferredOrderRemovals { now_ms } => {
                event_buf.clear();
                dispatcher.drain_deferred_order_removals_due(now_ms, &mut event_buf);
                on_event.drain_events(
                    &mut event_buf,
                    dispatcher,
                    &protocol_metrics,
                    None,
                    u8::MAX,
                    0,
                );
            }
            DispatcherWorkItem::TickOrders { now_ms } => {
                event_buf.clear();
                active_actions_buf.clear();
                dispatcher.tick_orders_active_actions(
                    now_ms,
                    &mut event_buf,
                    &mut active_actions_buf,
                );
                sender.apply_active_actions(active_actions_buf.drain(..));
                on_event.drain_events(
                    &mut event_buf,
                    dispatcher,
                    &protocol_metrics,
                    None,
                    u8::MAX,
                    0,
                );
            }
            DispatcherWorkItem::ResetOrderbookCachesKeepBooks => {
                dispatcher.reset_orderbook_caches_keep_books();
            }
            DispatcherWorkItem::Barrier { done } => {
                let _ = done.send(());
            }
        }
    }
}

pub(crate) fn dispatch_api_pending_from_events(
    api_pending: &ApiPending,
    events: &[crate::events::Event],
) -> bool {
    let mut consumed = false;
    for event in events {
        let crate::events::Event::EngineResponse(resp) = event else {
            continue;
        };
        if api_pending.contains(resp.request_uid) {
            consumed |= api_pending.dispatch(resp.clone()).is_none();
        }
    }
    consumed
}

pub(crate) fn wait_dispatcher_worker_barrier(tx: &mpsc::Sender<DispatcherWorkItem>) {
    let (done_tx, done_rx) = mpsc::channel();
    if tx
        .send(DispatcherWorkItem::Barrier { done: done_tx })
        .is_ok()
    {
        let _ = done_rx.recv();
    }
}
