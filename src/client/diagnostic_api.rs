use super::*;

impl Client {
    /// Snapshot client-side [`set_err_emu`] counters for live tests.
    ///
    /// This does not affect protocol behavior. FireTest uses it to distinguish
    /// "server did not send", "ErrEmu dropped all retries", and
    /// "Sliced reassembly/parse failed after packets arrived".
    pub fn err_emu_diagnostics_snapshot(&self) -> ErrEmuDiagnostics {
        let configured_rate = ERR_EMU_RATE.load(std::sync::atomic::Ordering::Relaxed);
        self.err_emu_diagnostics
            .lock()
            .unwrap()
            .snapshot(configured_rate)
    }

    pub(super) fn err_emu_diagnostics_handle(&self) -> Arc<Mutex<ErrEmuDiagnosticsState>> {
        Arc::clone(&self.err_emu_diagnostics)
    }

    /// Clear client-side [`set_err_emu`] counters without changing the loss rate.
    pub fn reset_err_emu_diagnostics(&self) {
        *self.err_emu_diagnostics.lock().unwrap() = ErrEmuDiagnosticsState::default();
    }

    /// Snapshot passive protocol loop metrics.
    ///
    /// These counters are diagnostics only. They never change retry, ACK,
    /// reconnect, queueing, or drop decisions. Use this to prove that
    /// receive-side protocol work and writer send/maintenance phases stay
    /// bounded while auditing Delphi machine-effect parity.
    pub fn protocol_metrics_snapshot(&self) -> ProtocolMetricsSnapshot {
        self.protocol_metrics.snapshot(0)
    }

    /// Snapshot protocol metrics and include the current dispatcher public
    /// event queue length.
    pub fn protocol_metrics_snapshot_with_dispatcher(
        &self,
        dispatcher: &crate::events::EventDispatcher,
    ) -> ProtocolMetricsSnapshot {
        self.protocol_metrics
            .snapshot(dispatcher.queued_event_count())
    }

    /// Returns true after the transport handshake has reached `AuthDone`.
    ///
    /// This is transport readiness, not full domain readiness. Use
    /// [`Self::is_domain_ready`] after the one-time init sequence when the
    /// application needs markets, indexes, settings, balances, and
    /// subscriptions initialized.
    pub fn is_authorized(&self) -> bool {
        self.authorized
    }
    /// Returns true after the MoonBot-compatible domain init has completed.
    pub fn is_domain_ready(&self) -> bool {
        self.domain_ready
    }
    /// Current low-level transport authorization state.
    pub fn auth_status(&self) -> AuthStatus {
        self.auth_status
    }
    /// Number of accepted Ping packets processed by this client.
    pub fn ping_count(&self) -> u32 {
        self.ping_count
    }
    /// Total UDP bytes sent by this client session.
    pub fn total_sent(&self) -> u64 {
        self.total_sent.load(Ordering::Relaxed)
    }
    /// Total accepted UDP bytes received by this client session.
    ///
    /// Valid packets selected by the test packet-loss emulator still contribute
    /// to this counter, matching Delphi side effects before `MoonProtoErrEmu`
    /// drops the packet from protocol dispatch.
    pub fn total_recv(&self) -> u64 {
        self.total_recv
    }

    /// Number of outgoing Sliced datagrams still waiting for `SlicedACK`.
    pub fn sliced_in_flight_count(&self) -> usize {
        self.sending.len()
    }

    /// Total Sliced blocks still waiting for `SlicedACK` across all datagrams.
    pub fn sliced_in_flight_blocks(&self) -> usize {
        self.sending.iter().map(|s| s.blocks_count).sum()
    }

    /// Number of H-priority encrypted commands still waiting for regular ACK.
    pub fn pending_high_count(&self) -> usize {
        self.pending_h.len()
    }

    /// EMA % retransmission overhead for Sliced packets (matches AvgOverHeat MoonProtoIntStruct.pas:220).
    /// 0 = ideal (no retries). >0 = forced retransmissions.
    pub fn avg_over_heat(&self) -> f64 {
        self.avg_over_heat
    }

    // ====================================================================
    //  Diagnostic getters (audit_responsibility A4)
    //
    //  In Delphi `TMoonProtoNetClient` these fields are public and read by the UI
    //  (MoonProtoUnit.pas:363 — "Ping: %d PMTU: %d RS: %d%%"). The Rust analog
    //  for building the terminal status line.
    // ====================================================================

    /// RTT in ms (last measured from Ping). Matches Delphi
    /// `TMoonProtoNetClient.RoundTripDelay` (MoonProtoClient.pas:62).
    pub fn round_trip_delay_ms(&self) -> i64 {
        self.round_trip_delay
    }

    /// Current Path MTU in bytes. Starts at 508; the runtime ProbeMTU can
    /// raise the value above 8000 in 32-byte steps.
    /// Matches Delphi `TMoonProtoNetClient.PMTU`.
    pub fn actual_pmtu(&self) -> u16 {
        self.actual_pmtu
    }

    /// Receive Status [0.0..1.0] — downlink channel quality. >0.92 = normal,
    /// <0.85 = critical, in between = gray zone. Matches Delphi
    /// `TMoonProtoNetClient.RS`.
    pub fn rs(&self) -> f64 {
        self.rs
    }

    /// `ServerTime - LocalTime` in days (like Delphi TDateTime). Applied
    /// automatically to incoming order timestamps via `Orders::apply`.
    /// External consumers usually do not need it — exposed publicly for diagnostics.
    pub fn server_time_delta_days(&self) -> f64 {
        self.server_time_delta
    }

    /// `|ServerTime - LocalTime|` in ms (absolute lag from the last Ping).
    /// Useful for a UI "server near / far" indicator.
    pub fn net_lag_ping_ms(&self) -> i64 {
        self.net_lag_ping
    }

    /// `Orders cycle ms` from the server — the recommended polling rate for order events.
    /// Matches Delphi `TMoonProtoNetClient.GlobalTimingOrders`.
    pub fn global_timing_orders(&self) -> u16 {
        self.global_timing_orders
    }

    /// Current `ServerToken` — changes on every hard handshake (Hello->WhoAreYou->Fine).
    /// Soft reconnect (HelloAgain) does NOT change this token. **Used inside the library for
    /// init/API subscription restore** — an external consumer usually does not need it,
    /// exposed for the diagnostic UI.
    pub fn server_token(&self) -> u64 {
        self.server_token
    }

    pub(crate) fn subscribed_book_server_token(&self) -> u64 {
        self.subscribed_book_server_token
    }

    /// `PeerAppToken` — generated when the server process starts. Changes on a server
    /// restart. **Used inside the library to check the freshness of markets indexes** — an
    /// external consumer usually does not need it, exposed for the diagnostic UI / event correlation.
    pub fn peer_app_token(&self) -> u64 {
        self.peer_app_token
    }

    pub(crate) fn market_indexes_current_for_peer(&self) -> bool {
        self.peer_app_token != 0 && self.peer_app_token == self.tracked_indexes_peer_app_token
    }

    // ====================================================================
    //  BytesPerSec — O(1) EMA counter (port of Delphi AddBytesCount)
    // ====================================================================
    //
    // Audit #5 (audit_delphi_deviation): previously a `VecDeque<(i64,u64)>` sliding
    // window was used. At a peak of 50K pps incoming, the VecDeque grew to ~500K entries × 16B = 8MB
    // for recv alone (+ another 8MB for sent). Plus 100K push_back/pop_front ops/sec.
    //
    // Delphi solves this in 24 bytes (3×u64) + 1 if + 1 add per packet — a byte-exact port of
    // `MoonProtoUDPClient.pas:113-138 AddBytesCount`. EMA formula: `ema = ema*9/10 + bucket`,
    // which in steady state yields `ema = 10*bytes_per_sec` (hence the division by 10 in the getter).

    pub(crate) fn track_sent(&mut self, bytes: u64, ts_ms: i64) {
        self.bps_sent.add(bytes, ts_ms);
    }

    pub(crate) fn track_recv(&mut self, bytes: u64, ts_ms: i64) {
        self.bps_recv.add(bytes, ts_ms);
    }

    /// Average bytes sent over the last ~10 seconds (B/s). O(1) EMA, see [`BpsCounter`].
    pub fn bytes_per_sec_sent(&self) -> u64 {
        self.bps_sent.bytes_per_sec()
    }
    /// Average bytes received over the last ~10 seconds (B/s). O(1) EMA.
    pub fn bytes_per_sec_recv(&self) -> u64 {
        self.bps_recv.bytes_per_sec()
    }

    // ====================================================================
    //  Log throttle — anti-spam helper for warnings.
    // ====================================================================

    /// Returns `true` if >= `interval_ms` has passed since the previous log with this `key`.
    /// Usage: wrap `eprintln!("...")` as `if client.should_log("X", 1000) { ... }`.
    /// `#[inline]`: called on EVERY warn/error in the send/recv paths.
    #[inline]
    pub fn should_log(&mut self, key: &'static str, interval_ms: i64) -> bool {
        let now_ms = self.now_ms();
        let last = self.log_last.entry(key).or_insert(0);
        if now_ms - *last >= interval_ms {
            *last = now_ms;
            true
        } else {
            false
        }
    }
}
