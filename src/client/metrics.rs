//! Passive protocol timing counters.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Passive snapshot of MoonProto protocol loop metrics.
///
/// These counters never influence send/retry/drop decisions. They exist only to
/// prove whether the protocol-owned work is bounded and fast enough for the
/// Delphi machine-effect parity plan.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProtocolMetricsSnapshot {
    /// UDP datagrams returned by `recv_from`, before MoonProto MAC/version
    /// acceptance.
    pub recv_count: u64,
    /// Total nanoseconds spent in the reader-side protocol packet path after
    /// `recv_from` returned.
    pub reader_protocol_count: u64,
    pub reader_protocol_ns: u64,
    /// Maximum single reader-side protocol packet duration, in nanoseconds.
    pub reader_protocol_max_ns: u64,
    /// Command and payload length that produced `reader_protocol_max_ns`.
    ///
    /// `cmd == u8::MAX` means the datagram was rejected before a MoonProto
    /// command byte was known.
    pub reader_protocol_max_cmd: u8,
    pub reader_protocol_max_payload_len: u64,
    /// Reader protocol packets slower than 100 us / 1 ms / 5 ms.
    pub reader_protocol_over_100us: u64,
    pub reader_protocol_over_1ms: u64,
    pub reader_protocol_over_5ms: u64,
    /// Writer/orchestrator loop iterations.
    pub writer_tick_count: u64,
    /// Total nanoseconds spent in writer/orchestrator loop iterations.
    pub writer_tick_ns: u64,
    /// Maximum single writer/orchestrator loop iteration, in nanoseconds.
    pub writer_tick_max_ns: u64,
    /// Writer/orchestrator CPU-ish work excluding the fixed Delphi 5 ms sleep.
    pub writer_cpu_count: u64,
    /// Total nanoseconds spent in writer/orchestrator CPU-ish work.
    pub writer_cpu_ns: u64,
    /// Maximum single writer/orchestrator CPU-ish segment, in nanoseconds.
    pub writer_cpu_max_ns: u64,
    /// Writer CPU-ish segments slower than 100 us / 1 ms / 5 ms.
    pub writer_cpu_over_100us: u64,
    pub writer_cpu_over_1ms: u64,
    pub writer_cpu_over_5ms: u64,
    /// App/event enqueue work done by the protocol owner before user callbacks.
    pub app_enqueue_count: u64,
    pub app_enqueue_ns: u64,
    pub app_enqueue_max_ns: u64,
    /// Source command/payload/event count/mode for `app_enqueue_max_ns`.
    ///
    /// `cmd == u8::MAX` means the enqueue came from a timer/deferred task
    /// rather than a specific incoming packet.
    pub app_enqueue_max_cmd: u8,
    pub app_enqueue_max_payload_len: u64,
    pub app_enqueue_max_events: u64,
    pub app_enqueue_max_mode: u8,
    /// App enqueue segments slower than 100 us / 1 ms / 5 ms.
    pub app_enqueue_over_100us: u64,
    pub app_enqueue_over_1ms: u64,
    pub app_enqueue_over_5ms: u64,
    /// Active/domain dispatch work before app/event enqueue.
    pub active_dispatch_count: u64,
    pub active_dispatch_ns: u64,
    pub active_dispatch_max_ns: u64,
    /// Source command/payload/events/actions for `active_dispatch_max_ns`.
    pub active_dispatch_max_cmd: u8,
    pub active_dispatch_max_payload_len: u64,
    pub active_dispatch_max_events: u64,
    pub active_dispatch_max_actions: u64,
    /// Active/domain dispatch segments slower than 100 us / 1 ms / 5 ms.
    pub active_dispatch_over_100us: u64,
    pub active_dispatch_over_1ms: u64,
    pub active_dispatch_over_5ms: u64,
    /// Total nanoseconds spent in the send/maintenance phase.
    pub send_phase_ns: u64,
    /// Maximum single send/maintenance phase duration, in nanoseconds.
    pub send_phase_max_ns: u64,
    /// Current public event queue length when a dispatcher-backed snapshot was
    /// requested; otherwise zero.
    pub public_event_queue_len: usize,
}

#[derive(Debug, Default)]
pub(crate) struct ProtocolMetrics {
    recv_count: AtomicU64,
    reader_protocol_count: AtomicU64,
    reader_protocol_ns: AtomicU64,
    reader_protocol_max_ns: AtomicU64,
    reader_protocol_max_cmd: AtomicU64,
    reader_protocol_max_payload_len: AtomicU64,
    reader_protocol_over_100us: AtomicU64,
    reader_protocol_over_1ms: AtomicU64,
    reader_protocol_over_5ms: AtomicU64,
    writer_tick_count: AtomicU64,
    writer_tick_ns: AtomicU64,
    writer_tick_max_ns: AtomicU64,
    writer_cpu_count: AtomicU64,
    writer_cpu_ns: AtomicU64,
    writer_cpu_max_ns: AtomicU64,
    writer_cpu_over_100us: AtomicU64,
    writer_cpu_over_1ms: AtomicU64,
    writer_cpu_over_5ms: AtomicU64,
    app_enqueue_count: AtomicU64,
    app_enqueue_ns: AtomicU64,
    app_enqueue_max_ns: AtomicU64,
    app_enqueue_max_cmd: AtomicU64,
    app_enqueue_max_payload_len: AtomicU64,
    app_enqueue_max_events: AtomicU64,
    app_enqueue_max_mode: AtomicU64,
    app_enqueue_over_100us: AtomicU64,
    app_enqueue_over_1ms: AtomicU64,
    app_enqueue_over_5ms: AtomicU64,
    active_dispatch_count: AtomicU64,
    active_dispatch_ns: AtomicU64,
    active_dispatch_max_ns: AtomicU64,
    active_dispatch_max_cmd: AtomicU64,
    active_dispatch_max_payload_len: AtomicU64,
    active_dispatch_max_events: AtomicU64,
    active_dispatch_max_actions: AtomicU64,
    active_dispatch_over_100us: AtomicU64,
    active_dispatch_over_1ms: AtomicU64,
    active_dispatch_over_5ms: AtomicU64,
    send_phase_ns: AtomicU64,
    send_phase_max_ns: AtomicU64,
}

impl ProtocolMetrics {
    pub(crate) fn record_recv_packet(&self) {
        self.recv_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn writer_tick_timer(&self) -> ProtocolMetricsTimer<'_> {
        ProtocolMetricsTimer {
            metrics: self,
            kind: TimerKind::WriterTick,
            start: Instant::now(),
        }
    }

    pub(crate) fn record_send_phase(&self, duration: Duration) {
        let ns = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
        self.send_phase_ns.fetch_add(ns, Ordering::Relaxed);
        let _ = store_max(&self.send_phase_max_ns, ns);
    }

    pub(crate) fn record_writer_cpu(&self, duration: Duration) {
        record_timing(
            &self.writer_cpu_count,
            &self.writer_cpu_ns,
            &self.writer_cpu_max_ns,
            &self.writer_cpu_over_100us,
            &self.writer_cpu_over_1ms,
            &self.writer_cpu_over_5ms,
            duration,
        );
    }

    pub(crate) fn record_app_enqueue_labeled(
        &self,
        duration: Duration,
        source_cmd: u8,
        payload_len: usize,
        event_count: usize,
        mode: u8,
    ) {
        if record_timing(
            &self.app_enqueue_count,
            &self.app_enqueue_ns,
            &self.app_enqueue_max_ns,
            &self.app_enqueue_over_100us,
            &self.app_enqueue_over_1ms,
            &self.app_enqueue_over_5ms,
            duration,
        ) {
            self.app_enqueue_max_cmd
                .store(u64::from(source_cmd), Ordering::Relaxed);
            self.app_enqueue_max_payload_len
                .store(payload_len as u64, Ordering::Relaxed);
            self.app_enqueue_max_events
                .store(event_count as u64, Ordering::Relaxed);
            self.app_enqueue_max_mode
                .store(u64::from(mode), Ordering::Relaxed);
        }
    }

    pub(crate) fn record_active_dispatch_labeled(
        &self,
        duration: Duration,
        source_cmd: u8,
        payload_len: usize,
        event_count: usize,
        action_count: usize,
    ) {
        if record_timing(
            &self.active_dispatch_count,
            &self.active_dispatch_ns,
            &self.active_dispatch_max_ns,
            &self.active_dispatch_over_100us,
            &self.active_dispatch_over_1ms,
            &self.active_dispatch_over_5ms,
            duration,
        ) {
            self.active_dispatch_max_cmd
                .store(u64::from(source_cmd), Ordering::Relaxed);
            self.active_dispatch_max_payload_len
                .store(payload_len as u64, Ordering::Relaxed);
            self.active_dispatch_max_events
                .store(event_count as u64, Ordering::Relaxed);
            self.active_dispatch_max_actions
                .store(action_count as u64, Ordering::Relaxed);
        }
    }

    pub(crate) fn snapshot(&self, public_event_queue_len: usize) -> ProtocolMetricsSnapshot {
        ProtocolMetricsSnapshot {
            recv_count: self.recv_count.load(Ordering::Relaxed),
            reader_protocol_count: self.reader_protocol_count.load(Ordering::Relaxed),
            reader_protocol_ns: self.reader_protocol_ns.load(Ordering::Relaxed),
            reader_protocol_max_ns: self.reader_protocol_max_ns.load(Ordering::Relaxed),
            reader_protocol_max_cmd: self.reader_protocol_max_cmd.load(Ordering::Relaxed) as u8,
            reader_protocol_max_payload_len: self
                .reader_protocol_max_payload_len
                .load(Ordering::Relaxed),
            reader_protocol_over_100us: self.reader_protocol_over_100us.load(Ordering::Relaxed),
            reader_protocol_over_1ms: self.reader_protocol_over_1ms.load(Ordering::Relaxed),
            reader_protocol_over_5ms: self.reader_protocol_over_5ms.load(Ordering::Relaxed),
            writer_tick_count: self.writer_tick_count.load(Ordering::Relaxed),
            writer_tick_ns: self.writer_tick_ns.load(Ordering::Relaxed),
            writer_tick_max_ns: self.writer_tick_max_ns.load(Ordering::Relaxed),
            writer_cpu_count: self.writer_cpu_count.load(Ordering::Relaxed),
            writer_cpu_ns: self.writer_cpu_ns.load(Ordering::Relaxed),
            writer_cpu_max_ns: self.writer_cpu_max_ns.load(Ordering::Relaxed),
            writer_cpu_over_100us: self.writer_cpu_over_100us.load(Ordering::Relaxed),
            writer_cpu_over_1ms: self.writer_cpu_over_1ms.load(Ordering::Relaxed),
            writer_cpu_over_5ms: self.writer_cpu_over_5ms.load(Ordering::Relaxed),
            app_enqueue_count: self.app_enqueue_count.load(Ordering::Relaxed),
            app_enqueue_ns: self.app_enqueue_ns.load(Ordering::Relaxed),
            app_enqueue_max_ns: self.app_enqueue_max_ns.load(Ordering::Relaxed),
            app_enqueue_max_cmd: self.app_enqueue_max_cmd.load(Ordering::Relaxed) as u8,
            app_enqueue_max_payload_len: self.app_enqueue_max_payload_len.load(Ordering::Relaxed),
            app_enqueue_max_events: self.app_enqueue_max_events.load(Ordering::Relaxed),
            app_enqueue_max_mode: self.app_enqueue_max_mode.load(Ordering::Relaxed) as u8,
            app_enqueue_over_100us: self.app_enqueue_over_100us.load(Ordering::Relaxed),
            app_enqueue_over_1ms: self.app_enqueue_over_1ms.load(Ordering::Relaxed),
            app_enqueue_over_5ms: self.app_enqueue_over_5ms.load(Ordering::Relaxed),
            active_dispatch_count: self.active_dispatch_count.load(Ordering::Relaxed),
            active_dispatch_ns: self.active_dispatch_ns.load(Ordering::Relaxed),
            active_dispatch_max_ns: self.active_dispatch_max_ns.load(Ordering::Relaxed),
            active_dispatch_max_cmd: self.active_dispatch_max_cmd.load(Ordering::Relaxed) as u8,
            active_dispatch_max_payload_len: self
                .active_dispatch_max_payload_len
                .load(Ordering::Relaxed),
            active_dispatch_max_events: self.active_dispatch_max_events.load(Ordering::Relaxed),
            active_dispatch_max_actions: self.active_dispatch_max_actions.load(Ordering::Relaxed),
            active_dispatch_over_100us: self.active_dispatch_over_100us.load(Ordering::Relaxed),
            active_dispatch_over_1ms: self.active_dispatch_over_1ms.load(Ordering::Relaxed),
            active_dispatch_over_5ms: self.active_dispatch_over_5ms.load(Ordering::Relaxed),
            send_phase_ns: self.send_phase_ns.load(Ordering::Relaxed),
            send_phase_max_ns: self.send_phase_max_ns.load(Ordering::Relaxed),
            public_event_queue_len,
        }
    }

    pub(crate) fn record_reader_protocol_labeled(
        &self,
        duration: Duration,
        source_cmd: u8,
        payload_len: usize,
    ) {
        if record_timing(
            &self.reader_protocol_count,
            &self.reader_protocol_ns,
            &self.reader_protocol_max_ns,
            &self.reader_protocol_over_100us,
            &self.reader_protocol_over_1ms,
            &self.reader_protocol_over_5ms,
            duration,
        ) {
            self.reader_protocol_max_cmd
                .store(u64::from(source_cmd), Ordering::Relaxed);
            self.reader_protocol_max_payload_len
                .store(payload_len as u64, Ordering::Relaxed);
        }
    }

    fn record_writer_tick(&self, duration: Duration) {
        let ns = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
        self.writer_tick_count.fetch_add(1, Ordering::Relaxed);
        self.writer_tick_ns.fetch_add(ns, Ordering::Relaxed);
        let _ = store_max(&self.writer_tick_max_ns, ns);
    }
}

pub(crate) struct ProtocolMetricsTimer<'a> {
    metrics: &'a ProtocolMetrics,
    kind: TimerKind,
    start: Instant,
}

impl Drop for ProtocolMetricsTimer<'_> {
    fn drop(&mut self) {
        let duration = self.start.elapsed();
        match self.kind {
            TimerKind::WriterTick => self.metrics.record_writer_tick(duration),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum TimerKind {
    WriterTick,
}

fn store_max(slot: &AtomicU64, value: u64) -> bool {
    let mut current = slot.load(Ordering::Relaxed);
    while value > current {
        match slot.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return true,
            Err(actual) => current = actual,
        }
    }
    false
}

fn record_timing(
    count: &AtomicU64,
    total: &AtomicU64,
    max: &AtomicU64,
    over_100us: &AtomicU64,
    over_1ms: &AtomicU64,
    over_5ms: &AtomicU64,
    duration: Duration,
) -> bool {
    let ns = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
    count.fetch_add(1, Ordering::Relaxed);
    total.fetch_add(ns, Ordering::Relaxed);
    let is_max = store_max(max, ns);
    if ns > 100_000 {
        over_100us.fetch_add(1, Ordering::Relaxed);
    }
    if ns > 1_000_000 {
        over_1ms.fetch_add(1, Ordering::Relaxed);
    }
    if ns > 5_000_000 {
        over_5ms.fetch_add(1, Ordering::Relaxed);
    }
    is_max
}
