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
    pub reader_protocol_ns: u64,
    /// Maximum single reader-side protocol packet duration, in nanoseconds.
    pub reader_protocol_max_ns: u64,
    /// Writer/orchestrator loop iterations.
    pub writer_tick_count: u64,
    /// Total nanoseconds spent in writer/orchestrator loop iterations.
    pub writer_tick_ns: u64,
    /// Maximum single writer/orchestrator loop iteration, in nanoseconds.
    pub writer_tick_max_ns: u64,
    /// Total nanoseconds spent in the send/maintenance phase.
    pub send_phase_ns: u64,
    /// Maximum single send/maintenance phase duration, in nanoseconds.
    pub send_phase_max_ns: u64,
    /// Current length of the reader-decoded queue. In the target architecture
    /// this becomes the AppQueue metric.
    pub app_queue_len: usize,
    /// Maximum observed reader-decoded queue length.
    pub app_queue_max_len: u64,
    /// Current public event queue length when a dispatcher-backed snapshot was
    /// requested; otherwise zero.
    pub public_event_queue_len: usize,
}

#[derive(Debug, Default)]
pub(crate) struct ProtocolMetrics {
    recv_count: AtomicU64,
    reader_protocol_ns: AtomicU64,
    reader_protocol_max_ns: AtomicU64,
    writer_tick_count: AtomicU64,
    writer_tick_ns: AtomicU64,
    writer_tick_max_ns: AtomicU64,
    send_phase_ns: AtomicU64,
    send_phase_max_ns: AtomicU64,
    app_queue_max_len: AtomicU64,
}

impl ProtocolMetrics {
    pub(crate) fn record_recv_packet(&self) {
        self.recv_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn reader_protocol_timer(&self) -> ProtocolMetricsTimer<'_> {
        ProtocolMetricsTimer {
            metrics: self,
            kind: TimerKind::ReaderProtocol,
            start: Instant::now(),
        }
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
        store_max(&self.send_phase_max_ns, ns);
    }

    pub(crate) fn record_app_queue_len(&self, len: usize) {
        store_max(&self.app_queue_max_len, len as u64);
    }

    pub(crate) fn snapshot(
        &self,
        app_queue_len: usize,
        public_event_queue_len: usize,
    ) -> ProtocolMetricsSnapshot {
        ProtocolMetricsSnapshot {
            recv_count: self.recv_count.load(Ordering::Relaxed),
            reader_protocol_ns: self.reader_protocol_ns.load(Ordering::Relaxed),
            reader_protocol_max_ns: self.reader_protocol_max_ns.load(Ordering::Relaxed),
            writer_tick_count: self.writer_tick_count.load(Ordering::Relaxed),
            writer_tick_ns: self.writer_tick_ns.load(Ordering::Relaxed),
            writer_tick_max_ns: self.writer_tick_max_ns.load(Ordering::Relaxed),
            send_phase_ns: self.send_phase_ns.load(Ordering::Relaxed),
            send_phase_max_ns: self.send_phase_max_ns.load(Ordering::Relaxed),
            app_queue_len,
            app_queue_max_len: self.app_queue_max_len.load(Ordering::Relaxed),
            public_event_queue_len,
        }
    }

    fn record_reader_protocol(&self, duration: Duration) {
        let ns = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
        self.reader_protocol_ns.fetch_add(ns, Ordering::Relaxed);
        store_max(&self.reader_protocol_max_ns, ns);
    }

    fn record_writer_tick(&self, duration: Duration) {
        let ns = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
        self.writer_tick_count.fetch_add(1, Ordering::Relaxed);
        self.writer_tick_ns.fetch_add(ns, Ordering::Relaxed);
        store_max(&self.writer_tick_max_ns, ns);
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
            TimerKind::ReaderProtocol => self.metrics.record_reader_protocol(duration),
            TimerKind::WriterTick => self.metrics.record_writer_tick(duration),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum TimerKind {
    ReaderProtocol,
    WriterTick,
}

fn store_max(slot: &AtomicU64, value: u64) {
    let mut current = slot.load(Ordering::Relaxed);
    while value > current {
        match slot.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => current = actual,
        }
    }
}
