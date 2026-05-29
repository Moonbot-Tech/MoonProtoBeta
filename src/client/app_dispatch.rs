use super::metrics::ProtocolMetrics;
use crate::protocol::Command;
#[cfg(test)]
use std::sync::mpsc;
use std::time::Instant;
/// Raw callback used by [`crate::client::Client::run`].
///
/// This callback receives decoded MoonProto command payloads after transport
/// decrypt/decompress/group handling, but before `EventDispatcher` state
/// application. Regular applications should use [`crate::MoonClient`] instead.
/// The callback runs from the application callback queue, not inside the
/// protocol writer tick.
#[cfg(test)]
pub(crate) type OnDataFn = Box<dyn FnMut(Command, &[u8]) + Send>;
#[cfg(test)]
pub(crate) type RawAppEvent = (Command, Vec<u8>);

/// Where to deliver `Command + payload` after internal handling (decrypt,
/// decompress, Grouped split, API pending dispatch). Two variants:
///
/// * `Callback` — raw payload callback via `OnDataFn` (used by `Client::run`).
/// * `Buffer` — a (Command, Vec<u8>) buffer for post-processing via
///   `EventDispatcher` (used by the low-level active pump).
///
/// This enum lets a single delivery pipeline (`ProtocolCore` drain +
/// `client_new_data_decoded`) serve both scenarios without
/// `Arc<Mutex>` workarounds for the borrow checker.
pub(crate) enum DispatchSink<'a> {
    #[cfg(test)]
    Callback(&'a mut OnDataFn),
    #[cfg(test)]
    CallbackQueue(&'a mpsc::Sender<RawAppEvent>),
    Buffer(&'a mut Vec<(Command, Vec<u8>)>),
}

impl<'a> DispatchSink<'a> {
    #[inline]
    pub(crate) fn is_buffer(&self) -> bool {
        matches!(self, Self::Buffer(_))
    }

    /// Delivery with an already-owned Vec (avoids a redundant `to_vec` when the
    /// payload originated from decrypt/decompress and is already owned).
    #[inline]
    pub(crate) fn deliver_owned(&mut self, cmd: Command, payload: Vec<u8>) {
        match self {
            #[cfg(test)]
            Self::Callback(cb) => cb(cmd, &payload),
            #[cfg(test)]
            Self::CallbackQueue(tx) => {
                let _ = tx.send((cmd, payload));
            }
            Self::Buffer(buf) => buf.push((cmd, payload)),
        }
    }
}

/// Main-loop run mode — defines how incoming data packets are delivered
/// and whether active-library auto-actions are needed.
///
/// `CallbackQueue` — low-level raw path for `Client::run`. The consumer receives
/// raw `(Command, &[u8])` and decides what to do with them (usually its own
/// `dispatcher.dispatch_into(...)`). Production delivery goes through the app
/// queue.
///
/// `Dispatcher` — active-library path. Runtime owns `EventDispatcher` directly,
/// applies packets to Active Lib state, runs auto-actions (RequestOrderBookFull,
/// trades resend tail-check, indexes sync gate), and queues typed `Event`
/// values after the state mutation.
pub(crate) enum RunMode<'a> {
    #[cfg(test)]
    Callback { on_data: OnDataFn },
    #[cfg(test)]
    CallbackQueue { app_tx: mpsc::Sender<RawAppEvent> },
    Dispatcher {
        dispatcher: &'a mut crate::events::EventDispatcher,
        on_event: DispatcherEventFn,
        /// Reusable event buffer (avoids alloc per packet).
        event_buf: Vec<crate::events::Event>,
        /// Reusable buffer of decoded payloads before the dispatcher.
        payload_buf: Vec<(Command, Vec<u8>)>,
        /// Reusable buffer of active-library side effects.
        active_actions_buf: Vec<crate::events::ActiveAction>,
    },
    #[cfg(not(test))]
    _Lifetime(std::marker::PhantomData<&'a ()>),
}

/// Event delivery target for the low-level active pump and production runtime.
pub(crate) enum DispatcherEventFn {
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
        let mode = 3;
        match self {
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
