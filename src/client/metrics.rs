//! Passive protocol timing counters.

#[cfg(any(test, feature = "diagnostics"))]
use super::diagnostics::ErrEmuDiagnosticsState;
#[cfg(any(test, feature = "diagnostics"))]
use parking_lot::Mutex;
use std::collections::HashMap;
#[cfg(any(test, feature = "diagnostics"))]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
#[cfg(any(test, feature = "diagnostics"))]
use std::sync::atomic::Ordering;
use std::sync::Arc;
#[cfg(any(test, feature = "diagnostics"))]
use std::time::{Duration, Instant};

/// Observability/diagnostics cluster carved out of [`super::Client`].
///
/// These fields never influence send/retry/drop decisions — they are byte/packet
/// accounting, the passive [`ProtocolMetrics`] sink, the log throttle table, the
/// client-side ErrEmu diagnostics, and the FireTest blackhole hook. They are
/// grouped here so the `Client` God object stays free of the observability
/// concern. Construction and field types are unchanged from when they lived
/// directly on `Client`.
pub(crate) struct ClientMetrics {
    /// Accepted UDP bytes received (main-thread counter, no sharing).
    pub(crate) total_recv: u64,
    /// Total UDP bytes sent (shared atomic, read by diagnostics).
    pub(crate) total_sent: AtomicU64,
    /// Accepted UDP bytes received (shared atomic mirror of `total_recv`).
    pub(crate) total_recv_shared: AtomicU64,
    /// Log throttle table: key -> last raise timestamp (anti-spam).
    pub(crate) log_last: HashMap<&'static str, i64>,
    /// Client-side ErrEmu packet-loss diagnostics (test-only loss emulator).
    #[cfg(any(test, feature = "diagnostics"))]
    pub(crate) err_emu_diagnostics: Arc<Mutex<ErrEmuDiagnosticsState>>,
    /// Passive protocol loop timing counters.
    pub(crate) protocol_metrics: Arc<ProtocolMetrics>,
    /// FireTest-only hook: drop every outgoing datagram before socket send.
    #[cfg(any(test, feature = "diagnostics"))]
    pub(crate) debug_outgoing_blackhole: Arc<AtomicBool>,
}

impl ClientMetrics {
    pub(crate) fn new_with_shared(
        #[cfg(any(test, feature = "diagnostics"))] err_emu_diagnostics: Arc<
            Mutex<ErrEmuDiagnosticsState>,
        >,
        protocol_metrics: Arc<ProtocolMetrics>,
        #[cfg(any(test, feature = "diagnostics"))] debug_outgoing_blackhole: Arc<AtomicBool>,
    ) -> Self {
        Self {
            total_recv: 0,
            total_sent: AtomicU64::new(0),
            total_recv_shared: AtomicU64::new(0),
            log_last: HashMap::new(),
            #[cfg(any(test, feature = "diagnostics"))]
            err_emu_diagnostics,
            protocol_metrics,
            #[cfg(any(test, feature = "diagnostics"))]
            debug_outgoing_blackhole,
        }
    }
}

/// Passive snapshot of MoonProto protocol loop metrics.
///
/// These counters never influence send/retry/drop decisions. They exist only to
/// prove whether the protocol-owned work is bounded and fast enough for the
/// Delphi machine-effect parity plan.
#[cfg(any(test, feature = "diagnostics"))]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProtocolMetricsSnapshot {
    /// UDP datagrams returned by `recv_from`, before MoonProto MAC/version
    /// acceptance.
    pub recv_count: u64,
    /// Total nanoseconds spent in the reader-side protocol packet path after
    /// `recv_from` returned, excluding deliberate protocol waits.
    pub reader_protocol_count: u64,
    pub reader_protocol_ns: u64,
    /// Maximum single reader-side protocol packet CPU-ish duration, in
    /// nanoseconds.
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
    /// OS thread CPU time for the same reader-side segments when the platform
    /// exposes a cheap per-thread clock. This excludes scheduler preemption
    /// that `Instant` wall timings include.
    pub reader_thread_cpu_count: u64,
    pub reader_thread_cpu_ns: u64,
    pub reader_thread_cpu_max_ns: u64,
    pub reader_thread_cpu_max_cmd: u8,
    pub reader_thread_cpu_max_payload_len: u64,
    pub reader_thread_cpu_over_100us: u64,
    pub reader_thread_cpu_over_1ms: u64,
    pub reader_thread_cpu_over_5ms: u64,
    /// Per-thread CPU cycles where the platform exposes them cheaply
    /// (Windows `QueryThreadCycleTime`). This is unitless but high-resolution,
    /// so it is useful when wall time spikes and duration CPU clocks are absent.
    pub reader_thread_cycles_count: u64,
    pub reader_thread_cycles_total: u64,
    pub reader_thread_cycles_max: u64,
    pub reader_thread_cycles_max_cmd: u8,
    pub reader_thread_cycles_max_payload_len: u64,
    /// Deliberate Delphi-compatible waits inside reader-side protocol handlers.
    ///
    /// These are not CPU work. The known example is the 32 ms `WhoAreYou` ->
    /// duplicate `ImFriend` barrier.
    pub reader_protocol_wait_count: u64,
    pub reader_protocol_wait_ns: u64,
    pub reader_protocol_wait_max_ns: u64,
    pub reader_protocol_wait_max_cmd: u8,
    pub reader_protocol_wait_max_payload_len: u64,
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
    /// OS thread CPU time for writer/orchestrator CPU-ish segments.
    pub writer_thread_cpu_count: u64,
    pub writer_thread_cpu_ns: u64,
    pub writer_thread_cpu_max_ns: u64,
    pub writer_thread_cpu_over_100us: u64,
    pub writer_thread_cpu_over_1ms: u64,
    pub writer_thread_cpu_over_5ms: u64,
    pub writer_thread_cycles_count: u64,
    pub writer_thread_cycles_total: u64,
    pub writer_thread_cycles_max: u64,
    /// App/event enqueue work done by the protocol owner before user callbacks.
    pub app_enqueue_count: u64,
    pub app_enqueue_ns: u64,
    pub app_enqueue_max_ns: u64,
    /// Source command/payload/event count/mode for `app_enqueue_max_ns`.
    ///
    /// `cmd == u8::MAX` means the enqueue came from a timer/deferred task
    /// rather than a specific incoming packet.
    pub app_enqueue_max_cmd: u8,
    /// Engine API method for API sources, or `u8::MAX` when not applicable.
    pub app_enqueue_max_api_method: u8,
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
    /// Engine API method for API sources, or `u8::MAX` when not applicable.
    pub active_dispatch_max_api_method: u8,
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
    /// Last PMTU value received from a server `Ping`.
    ///
    /// FireTest prints this so Linux/VPS runs prove the PMTU probing result
    /// instead of leaving it hidden in the transport state.
    pub last_pmtu: u16,
    /// Detailed diagnostics-only profile phases. These split the broad
    /// reader/writer/dispatch counters into concrete protocol sections.
    pub profile_phases: Vec<ProtocolProfilePhaseSnapshot>,
}

#[cfg(any(test, feature = "diagnostics"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolProfilePhaseSnapshot {
    pub name: &'static str,
    pub count: u64,
    pub total_ns: u64,
    pub max_ns: u64,
    pub max_cmd: u8,
    pub max_api_method: u8,
    pub max_payload_len: u64,
    pub over_100us: u64,
    pub over_1ms: u64,
    pub over_5ms: u64,
}

#[cfg(any(test, feature = "diagnostics"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProfilePhase {
    RecvUnpack,
    RecvRoute,
    SlicedRecv,
    DecodeShared,
    DecodeOwned,
    DispatchDecoded,
    ActiveDecode,
    ActiveDispatch,
    ActiveActions,
    DrainEvents,
    SendMaintenance,
    SendLockSnapshot,
    CheckSeningData,
    RetryPendingH,
    RetrySliced,
    InitStep,
    RuntimePending,
    PendingAutoCandles,
    PendingCoinCard,
    PendingTransferAssets,
    PendingAccount,
    PendingEngineActions,
    RuntimeCommandDrain,
    RuntimeCommandDispatch,
    StrategySnapshotState,
    StrategySnapshotSerialize,
    StrategySnapshotSend,
    SnapshotPublish,
    CandlesSnapshotSync,
    CandlesSnapshotBaselines,
    CandlesSnapshotBuildRows,
    CandlesSnapshotQueue,
}

#[cfg(any(test, feature = "diagnostics"))]
pub(crate) const RUNTIME_PROFILE_CMD: u8 = 254;

#[cfg(any(test, feature = "diagnostics"))]
const PROFILE_PHASE_COUNT: usize = 32;

#[cfg(any(test, feature = "diagnostics"))]
impl ProfilePhase {
    #[inline]
    fn idx(self) -> usize {
        self as usize
    }

    fn name(self) -> &'static str {
        match self {
            Self::RecvUnpack => "recv.unpack",
            Self::RecvRoute => "recv.route",
            Self::SlicedRecv => "sliced.recv",
            Self::DecodeShared => "decode.shared",
            Self::DecodeOwned => "decode.owned",
            Self::DispatchDecoded => "dispatch.decoded",
            Self::ActiveDecode => "active.decode",
            Self::ActiveDispatch => "active.dispatch",
            Self::ActiveActions => "active.actions",
            Self::DrainEvents => "events.drain",
            Self::SendMaintenance => "send.maintenance",
            Self::SendLockSnapshot => "send.lock_snapshot",
            Self::CheckSeningData => "send.check_sening_data",
            Self::RetryPendingH => "send.retry_pending_h",
            Self::RetrySliced => "send.retry_sliced",
            Self::InitStep => "init.step",
            Self::RuntimePending => "runtime.pending",
            Self::PendingAutoCandles => "pending.auto_candles",
            Self::PendingCoinCard => "pending.coin_card",
            Self::PendingTransferAssets => "pending.transfer_assets",
            Self::PendingAccount => "pending.account",
            Self::PendingEngineActions => "pending.engine_actions",
            Self::RuntimeCommandDrain => "runtime.command_drain",
            Self::RuntimeCommandDispatch => "runtime.command_dispatch",
            Self::StrategySnapshotState => "strategy_snapshot.state",
            Self::StrategySnapshotSerialize => "strategy_snapshot.serialize",
            Self::StrategySnapshotSend => "strategy_snapshot.send",
            Self::SnapshotPublish => "snapshot.publish",
            Self::CandlesSnapshotSync => "candles.snapshot.sync",
            Self::CandlesSnapshotBaselines => "candles.snapshot.baselines",
            Self::CandlesSnapshotBuildRows => "candles.snapshot.build_rows",
            Self::CandlesSnapshotQueue => "candles.snapshot.queue",
        }
    }

    fn all() -> [Self; PROFILE_PHASE_COUNT] {
        [
            Self::RecvUnpack,
            Self::RecvRoute,
            Self::SlicedRecv,
            Self::DecodeShared,
            Self::DecodeOwned,
            Self::DispatchDecoded,
            Self::ActiveDecode,
            Self::ActiveDispatch,
            Self::ActiveActions,
            Self::DrainEvents,
            Self::SendMaintenance,
            Self::SendLockSnapshot,
            Self::CheckSeningData,
            Self::RetryPendingH,
            Self::RetrySliced,
            Self::InitStep,
            Self::RuntimePending,
            Self::PendingAutoCandles,
            Self::PendingCoinCard,
            Self::PendingTransferAssets,
            Self::PendingAccount,
            Self::PendingEngineActions,
            Self::RuntimeCommandDrain,
            Self::RuntimeCommandDispatch,
            Self::StrategySnapshotState,
            Self::StrategySnapshotSerialize,
            Self::StrategySnapshotSend,
            Self::SnapshotPublish,
            Self::CandlesSnapshotSync,
            Self::CandlesSnapshotBaselines,
            Self::CandlesSnapshotBuildRows,
            Self::CandlesSnapshotQueue,
        ]
    }
}

#[cfg(any(test, feature = "diagnostics"))]
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
    reader_thread_cpu_count: AtomicU64,
    reader_thread_cpu_ns: AtomicU64,
    reader_thread_cpu_max_ns: AtomicU64,
    reader_thread_cpu_max_cmd: AtomicU64,
    reader_thread_cpu_max_payload_len: AtomicU64,
    reader_thread_cpu_over_100us: AtomicU64,
    reader_thread_cpu_over_1ms: AtomicU64,
    reader_thread_cpu_over_5ms: AtomicU64,
    reader_thread_cycles_count: AtomicU64,
    reader_thread_cycles_total: AtomicU64,
    reader_thread_cycles_max: AtomicU64,
    reader_thread_cycles_max_cmd: AtomicU64,
    reader_thread_cycles_max_payload_len: AtomicU64,
    reader_protocol_wait_count: AtomicU64,
    reader_protocol_wait_ns: AtomicU64,
    reader_protocol_wait_max_ns: AtomicU64,
    reader_protocol_wait_max_cmd: AtomicU64,
    reader_protocol_wait_max_payload_len: AtomicU64,
    writer_tick_count: AtomicU64,
    writer_tick_ns: AtomicU64,
    writer_tick_max_ns: AtomicU64,
    writer_cpu_count: AtomicU64,
    writer_cpu_ns: AtomicU64,
    writer_cpu_max_ns: AtomicU64,
    writer_cpu_over_100us: AtomicU64,
    writer_cpu_over_1ms: AtomicU64,
    writer_cpu_over_5ms: AtomicU64,
    writer_thread_cpu_count: AtomicU64,
    writer_thread_cpu_ns: AtomicU64,
    writer_thread_cpu_max_ns: AtomicU64,
    writer_thread_cpu_over_100us: AtomicU64,
    writer_thread_cpu_over_1ms: AtomicU64,
    writer_thread_cpu_over_5ms: AtomicU64,
    writer_thread_cycles_count: AtomicU64,
    writer_thread_cycles_total: AtomicU64,
    writer_thread_cycles_max: AtomicU64,
    app_enqueue_count: AtomicU64,
    app_enqueue_ns: AtomicU64,
    app_enqueue_max_ns: AtomicU64,
    app_enqueue_max_cmd: AtomicU64,
    app_enqueue_max_api_method: AtomicU64,
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
    active_dispatch_max_api_method: AtomicU64,
    active_dispatch_max_payload_len: AtomicU64,
    active_dispatch_max_events: AtomicU64,
    active_dispatch_max_actions: AtomicU64,
    active_dispatch_over_100us: AtomicU64,
    active_dispatch_over_1ms: AtomicU64,
    active_dispatch_over_5ms: AtomicU64,
    send_phase_ns: AtomicU64,
    send_phase_max_ns: AtomicU64,
    last_pmtu: AtomicU64,
    profile_count: [AtomicU64; PROFILE_PHASE_COUNT],
    profile_ns: [AtomicU64; PROFILE_PHASE_COUNT],
    profile_max_ns: [AtomicU64; PROFILE_PHASE_COUNT],
    profile_max_cmd: [AtomicU64; PROFILE_PHASE_COUNT],
    profile_max_api_method: [AtomicU64; PROFILE_PHASE_COUNT],
    profile_max_payload_len: [AtomicU64; PROFILE_PHASE_COUNT],
    profile_over_100us: [AtomicU64; PROFILE_PHASE_COUNT],
    profile_over_1ms: [AtomicU64; PROFILE_PHASE_COUNT],
    profile_over_5ms: [AtomicU64; PROFILE_PHASE_COUNT],
}

#[cfg(not(any(test, feature = "diagnostics")))]
#[derive(Debug, Default)]
pub(crate) struct ProtocolMetrics;

#[cfg(any(test, feature = "diagnostics"))]
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

    pub(crate) fn record_pmtu(&self, pmtu: u16) {
        self.last_pmtu.store(u64::from(pmtu), Ordering::Relaxed);
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

    pub(crate) fn record_writer_thread_cpu(&self, duration: Duration) {
        record_timing(
            &self.writer_thread_cpu_count,
            &self.writer_thread_cpu_ns,
            &self.writer_thread_cpu_max_ns,
            &self.writer_thread_cpu_over_100us,
            &self.writer_thread_cpu_over_1ms,
            &self.writer_thread_cpu_over_5ms,
            duration,
        );
    }

    pub(crate) fn record_writer_thread_cycles(&self, cycles: u64) {
        self.writer_thread_cycles_count
            .fetch_add(1, Ordering::Relaxed);
        self.writer_thread_cycles_total
            .fetch_add(cycles, Ordering::Relaxed);
        let _ = store_max(&self.writer_thread_cycles_max, cycles);
    }

    pub(crate) fn record_app_enqueue_labeled(
        &self,
        duration: Duration,
        source_cmd: u8,
        source_api_method: u8,
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
            self.app_enqueue_max_api_method
                .store(u64::from(source_api_method), Ordering::Relaxed);
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
        source_api_method: u8,
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
            self.active_dispatch_max_api_method
                .store(u64::from(source_api_method), Ordering::Relaxed);
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
            reader_thread_cpu_count: self.reader_thread_cpu_count.load(Ordering::Relaxed),
            reader_thread_cpu_ns: self.reader_thread_cpu_ns.load(Ordering::Relaxed),
            reader_thread_cpu_max_ns: self.reader_thread_cpu_max_ns.load(Ordering::Relaxed),
            reader_thread_cpu_max_cmd: self.reader_thread_cpu_max_cmd.load(Ordering::Relaxed) as u8,
            reader_thread_cpu_max_payload_len: self
                .reader_thread_cpu_max_payload_len
                .load(Ordering::Relaxed),
            reader_thread_cpu_over_100us: self.reader_thread_cpu_over_100us.load(Ordering::Relaxed),
            reader_thread_cpu_over_1ms: self.reader_thread_cpu_over_1ms.load(Ordering::Relaxed),
            reader_thread_cpu_over_5ms: self.reader_thread_cpu_over_5ms.load(Ordering::Relaxed),
            reader_thread_cycles_count: self.reader_thread_cycles_count.load(Ordering::Relaxed),
            reader_thread_cycles_total: self.reader_thread_cycles_total.load(Ordering::Relaxed),
            reader_thread_cycles_max: self.reader_thread_cycles_max.load(Ordering::Relaxed),
            reader_thread_cycles_max_cmd: self.reader_thread_cycles_max_cmd.load(Ordering::Relaxed)
                as u8,
            reader_thread_cycles_max_payload_len: self
                .reader_thread_cycles_max_payload_len
                .load(Ordering::Relaxed),
            reader_protocol_wait_count: self.reader_protocol_wait_count.load(Ordering::Relaxed),
            reader_protocol_wait_ns: self.reader_protocol_wait_ns.load(Ordering::Relaxed),
            reader_protocol_wait_max_ns: self.reader_protocol_wait_max_ns.load(Ordering::Relaxed),
            reader_protocol_wait_max_cmd: self.reader_protocol_wait_max_cmd.load(Ordering::Relaxed)
                as u8,
            reader_protocol_wait_max_payload_len: self
                .reader_protocol_wait_max_payload_len
                .load(Ordering::Relaxed),
            writer_tick_count: self.writer_tick_count.load(Ordering::Relaxed),
            writer_tick_ns: self.writer_tick_ns.load(Ordering::Relaxed),
            writer_tick_max_ns: self.writer_tick_max_ns.load(Ordering::Relaxed),
            writer_cpu_count: self.writer_cpu_count.load(Ordering::Relaxed),
            writer_cpu_ns: self.writer_cpu_ns.load(Ordering::Relaxed),
            writer_cpu_max_ns: self.writer_cpu_max_ns.load(Ordering::Relaxed),
            writer_cpu_over_100us: self.writer_cpu_over_100us.load(Ordering::Relaxed),
            writer_cpu_over_1ms: self.writer_cpu_over_1ms.load(Ordering::Relaxed),
            writer_cpu_over_5ms: self.writer_cpu_over_5ms.load(Ordering::Relaxed),
            writer_thread_cpu_count: self.writer_thread_cpu_count.load(Ordering::Relaxed),
            writer_thread_cpu_ns: self.writer_thread_cpu_ns.load(Ordering::Relaxed),
            writer_thread_cpu_max_ns: self.writer_thread_cpu_max_ns.load(Ordering::Relaxed),
            writer_thread_cpu_over_100us: self.writer_thread_cpu_over_100us.load(Ordering::Relaxed),
            writer_thread_cpu_over_1ms: self.writer_thread_cpu_over_1ms.load(Ordering::Relaxed),
            writer_thread_cpu_over_5ms: self.writer_thread_cpu_over_5ms.load(Ordering::Relaxed),
            writer_thread_cycles_count: self.writer_thread_cycles_count.load(Ordering::Relaxed),
            writer_thread_cycles_total: self.writer_thread_cycles_total.load(Ordering::Relaxed),
            writer_thread_cycles_max: self.writer_thread_cycles_max.load(Ordering::Relaxed),
            app_enqueue_count: self.app_enqueue_count.load(Ordering::Relaxed),
            app_enqueue_ns: self.app_enqueue_ns.load(Ordering::Relaxed),
            app_enqueue_max_ns: self.app_enqueue_max_ns.load(Ordering::Relaxed),
            app_enqueue_max_cmd: self.app_enqueue_max_cmd.load(Ordering::Relaxed) as u8,
            app_enqueue_max_api_method: self.app_enqueue_max_api_method.load(Ordering::Relaxed)
                as u8,
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
            active_dispatch_max_api_method: self
                .active_dispatch_max_api_method
                .load(Ordering::Relaxed) as u8,
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
            last_pmtu: self.last_pmtu.load(Ordering::Relaxed) as u16,
            profile_phases: self.profile_snapshot(),
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

    pub(crate) fn record_reader_thread_cpu_labeled(
        &self,
        duration: Duration,
        source_cmd: u8,
        payload_len: usize,
    ) {
        if record_timing(
            &self.reader_thread_cpu_count,
            &self.reader_thread_cpu_ns,
            &self.reader_thread_cpu_max_ns,
            &self.reader_thread_cpu_over_100us,
            &self.reader_thread_cpu_over_1ms,
            &self.reader_thread_cpu_over_5ms,
            duration,
        ) {
            self.reader_thread_cpu_max_cmd
                .store(u64::from(source_cmd), Ordering::Relaxed);
            self.reader_thread_cpu_max_payload_len
                .store(payload_len as u64, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_reader_thread_cycles_labeled(
        &self,
        cycles: u64,
        source_cmd: u8,
        payload_len: usize,
    ) {
        self.reader_thread_cycles_count
            .fetch_add(1, Ordering::Relaxed);
        self.reader_thread_cycles_total
            .fetch_add(cycles, Ordering::Relaxed);
        if store_max(&self.reader_thread_cycles_max, cycles) {
            self.reader_thread_cycles_max_cmd
                .store(u64::from(source_cmd), Ordering::Relaxed);
            self.reader_thread_cycles_max_payload_len
                .store(payload_len as u64, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_reader_protocol_wait_labeled(
        &self,
        duration: Duration,
        source_cmd: u8,
        payload_len: usize,
    ) {
        let ns = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
        self.reader_protocol_wait_count
            .fetch_add(1, Ordering::Relaxed);
        self.reader_protocol_wait_ns
            .fetch_add(ns, Ordering::Relaxed);
        if store_max(&self.reader_protocol_wait_max_ns, ns) {
            self.reader_protocol_wait_max_cmd
                .store(u64::from(source_cmd), Ordering::Relaxed);
            self.reader_protocol_wait_max_payload_len
                .store(payload_len as u64, Ordering::Relaxed);
        }
    }

    fn record_writer_tick(&self, duration: Duration) {
        let ns = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
        self.writer_tick_count.fetch_add(1, Ordering::Relaxed);
        self.writer_tick_ns.fetch_add(ns, Ordering::Relaxed);
        let _ = store_max(&self.writer_tick_max_ns, ns);
    }

    pub(crate) fn record_profile_phase_labeled(
        &self,
        phase: ProfilePhase,
        duration: Duration,
        source_cmd: u8,
        source_api_method: u8,
        payload_len: usize,
    ) {
        let idx = phase.idx();
        if record_timing(
            &self.profile_count[idx],
            &self.profile_ns[idx],
            &self.profile_max_ns[idx],
            &self.profile_over_100us[idx],
            &self.profile_over_1ms[idx],
            &self.profile_over_5ms[idx],
            duration,
        ) {
            self.profile_max_cmd[idx].store(u64::from(source_cmd), Ordering::Relaxed);
            self.profile_max_api_method[idx].store(u64::from(source_api_method), Ordering::Relaxed);
            self.profile_max_payload_len[idx].store(payload_len as u64, Ordering::Relaxed);
        }
    }

    fn profile_snapshot(&self) -> Vec<ProtocolProfilePhaseSnapshot> {
        ProfilePhase::all()
            .into_iter()
            .filter_map(|phase| {
                let idx = phase.idx();
                let count = self.profile_count[idx].load(Ordering::Relaxed);
                (count > 0).then(|| ProtocolProfilePhaseSnapshot {
                    name: phase.name(),
                    count,
                    total_ns: self.profile_ns[idx].load(Ordering::Relaxed),
                    max_ns: self.profile_max_ns[idx].load(Ordering::Relaxed),
                    max_cmd: self.profile_max_cmd[idx].load(Ordering::Relaxed) as u8,
                    max_api_method: self.profile_max_api_method[idx].load(Ordering::Relaxed) as u8,
                    max_payload_len: self.profile_max_payload_len[idx].load(Ordering::Relaxed),
                    over_100us: self.profile_over_100us[idx].load(Ordering::Relaxed),
                    over_1ms: self.profile_over_1ms[idx].load(Ordering::Relaxed),
                    over_5ms: self.profile_over_5ms[idx].load(Ordering::Relaxed),
                })
            })
            .collect()
    }
}

#[cfg(any(test, feature = "diagnostics"))]
pub(crate) struct ProtocolMetricsTimer<'a> {
    metrics: &'a ProtocolMetrics,
    kind: TimerKind,
    start: Instant,
}

#[cfg(any(test, feature = "diagnostics"))]
impl Drop for ProtocolMetricsTimer<'_> {
    fn drop(&mut self) {
        let duration = self.start.elapsed();
        match self.kind {
            TimerKind::WriterTick => self.metrics.record_writer_tick(duration),
        }
    }
}

#[cfg(any(test, feature = "diagnostics"))]
#[derive(Debug, Clone, Copy)]
enum TimerKind {
    WriterTick,
}

#[cfg(any(test, feature = "diagnostics"))]
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

#[cfg(any(test, feature = "diagnostics"))]
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
