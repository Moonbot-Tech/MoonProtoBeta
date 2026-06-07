use super::metrics::ProtocolMetrics;
use crate::protocol::Command;
#[cfg(any(test, feature = "diagnostics"))]
use std::time::Instant;

/// Single active receive delivery path.
///
/// The runtime owns one [`EventDispatcher`](crate::events::EventDispatcher) and
/// reusable buffers for decoded payloads, typed events, and active side effects.
/// Older raw callback modes were test-only leftovers; keeping the hot receive
/// path as one concrete shape makes the machine effect easier to audit and
/// avoids carrying dead polymorphism through every datagram.
pub(crate) struct RunMode<'a> {
    pub(crate) dispatcher: &'a mut crate::events::EventDispatcher,
    /// Reusable event buffer (avoids alloc per packet).
    pub(crate) event_buf: Vec<crate::events::Event>,
    /// Reusable buffer of decoded payloads before the dispatcher.
    pub(crate) payload_buf: Vec<(Command, Vec<u8>)>,
    /// Reusable buffer of active-library side effects.
    pub(crate) active_actions_buf: Vec<crate::events::ActiveAction>,
}

impl<'a> RunMode<'a> {
    #[cfg(test)]
    pub(crate) fn new(dispatcher: &'a mut crate::events::EventDispatcher) -> Self {
        Self::with_buffers(dispatcher, Vec::new(), Vec::new(), Vec::new())
    }

    pub(crate) fn with_buffers(
        dispatcher: &'a mut crate::events::EventDispatcher,
        event_buf: Vec<crate::events::Event>,
        payload_buf: Vec<(Command, Vec<u8>)>,
        active_actions_buf: Vec<crate::events::ActiveAction>,
    ) -> Self {
        Self {
            dispatcher,
            event_buf,
            payload_buf,
            active_actions_buf,
        }
    }

    pub(crate) fn into_buffers(
        self,
    ) -> (
        Vec<crate::events::Event>,
        Vec<(Command, Vec<u8>)>,
        Vec<crate::events::ActiveAction>,
    ) {
        (self.event_buf, self.payload_buf, self.active_actions_buf)
    }

    pub(crate) fn drain_events(
        &mut self,
        protocol_metrics: &ProtocolMetrics,
        source_cmd: Option<Command>,
        source_api_method: u8,
        source_payload_len: usize,
    ) {
        if self.event_buf.is_empty() {
            return;
        }
        #[cfg(not(any(test, feature = "diagnostics")))]
        let _ = (
            protocol_metrics,
            source_cmd,
            source_api_method,
            source_payload_len,
        );
        #[cfg(any(test, feature = "diagnostics"))]
        let enqueue_start = Instant::now();
        #[cfg(any(test, feature = "diagnostics"))]
        let event_count = self.event_buf.len();
        #[cfg(any(test, feature = "diagnostics"))]
        let mode = 3;
        self.dispatcher.queue_events(self.event_buf.drain(..));
        #[cfg(any(test, feature = "diagnostics"))]
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
