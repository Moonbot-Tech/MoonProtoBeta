use super::*;

impl Client {
    /// Clear client-side [`set_err_emu`] counters without changing the loss rate.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub(crate) fn reset_err_emu_diagnostics(&self) {
        *self.metrics.err_emu_diagnostics.lock() = ErrEmuDiagnosticsState::default();
    }

    /// Snapshot passive protocol loop metrics.
    ///
    /// These counters are diagnostics only. They never change retry, ACK,
    /// reconnect, queueing, or drop decisions. Use this to prove that
    /// receive-side protocol work and writer send/maintenance phases stay
    /// bounded while auditing Delphi machine-effect parity.
    #[cfg(test)]
    pub(crate) fn protocol_metrics_snapshot(&self) -> ProtocolMetricsSnapshot {
        self.metrics.protocol_metrics.snapshot(0)
    }

    /// Snapshot protocol metrics and include the current dispatcher public
    /// event queue length.
    #[cfg(test)]
    pub(crate) fn protocol_metrics_snapshot_with_dispatcher(
        &self,
        dispatcher: &crate::events::EventDispatcher,
    ) -> ProtocolMetricsSnapshot {
        self.metrics
            .protocol_metrics
            .snapshot(dispatcher.queued_event_count())
    }

    /// Returns true after the transport handshake has reached `AuthDone`.
    ///
    /// This is transport readiness, not full domain readiness. Use
    /// [`Self::is_domain_ready`] after the one-time init sequence when the
    /// application needs markets, indexes, settings, balances, and
    /// subscriptions initialized.
    pub(crate) fn is_authorized(&self) -> bool {
        self.authorized
    }
    /// Returns true after the MoonBot-compatible domain init has completed.
    pub(crate) fn is_domain_ready(&self) -> bool {
        self.subscriptions.domain_ready
    }
    /// Number of accepted Ping packets processed by this client.
    #[cfg(test)]
    pub(crate) fn ping_count(&self) -> u32 {
        self.ping_count
    }
    /// Total UDP bytes sent by this client session.
    #[cfg(test)]
    pub(crate) fn total_sent(&self) -> u64 {
        self.metrics.total_sent.load(Ordering::Relaxed)
    }
    /// Total accepted UDP bytes received by this client session.
    ///
    /// Valid packets selected by the test packet-loss emulator still contribute
    /// to this counter, matching Delphi side effects before `MoonProtoErrEmu`
    /// drops the packet from protocol dispatch.
    #[cfg(test)]
    pub(crate) fn total_recv(&self) -> u64 {
        self.metrics.total_recv
    }

    // ====================================================================
    //  Diagnostic getters for terminal status UI
    //
    //  In Delphi `TMoonProtoNetClient` these fields are public and read by the UI
    //  (MoonProtoUnit.pas:363 — "Ping: %d PMTU: %d RS: %d%%"). The Rust analog
    //  for building the terminal status line.
    // ====================================================================

    /// RTT in ms (last measured from Ping). Matches Delphi
    /// `TMoonProtoNetClient.RoundTripDelay` (MoonProtoClient.pas:62).
    pub(crate) fn round_trip_delay_ms(&self) -> i64 {
        self.round_trip_delay
    }

    pub(crate) fn kernel_health(&self) -> crate::state::KernelHealth {
        self.kernel_health
    }

    /// Current Path MTU in bytes. Starts at 508; the runtime ProbeMTU can
    /// raise the value above 8000 in 32-byte steps.
    /// Matches Delphi `TMoonProtoNetClient.PMTU`.
    #[cfg(test)]
    pub(crate) fn actual_pmtu(&self) -> u16 {
        self.actual_pmtu
    }

    /// `ServerTime - LocalTime` in days (like Delphi TDateTime). Applied
    /// automatically to incoming order timestamps via `Orders::apply`.
    /// External consumers usually do not need it — exposed publicly for diagnostics.
    #[cfg(test)]
    pub(crate) fn server_time_delta_days(&self) -> f64 {
        self.server_time_delta
    }

    /// `|ServerTime - LocalTime|` in ms (absolute lag from the last Ping).
    /// Useful for a UI "server near / far" indicator.
    #[cfg(test)]
    pub(crate) fn net_lag_ping_ms(&self) -> i64 {
        self.net_lag_ping
    }

    /// `Orders cycle ms` from the server — the recommended polling rate for order events.
    /// Matches Delphi `TMoonProtoNetClient.GlobalTimingOrders`.
    #[cfg(test)]
    pub(crate) fn global_timing_orders(&self) -> u16 {
        self.global_timing_orders
    }

    /// Current `ServerToken` — changes on every hard handshake (Hello->WhoAreYou->Fine).
    /// Soft reconnect (HelloAgain) does NOT change this token. **Used inside the library for
    /// init/API subscription restore** — an external consumer usually does not need it,
    /// exposed for the diagnostic UI.
    pub(crate) fn server_token(&self) -> u64 {
        self.server_token
    }

    pub(crate) fn subscribed_book_server_token(&self) -> u64 {
        self.reconnect.subscribed_book_server_token
    }

    /// `PeerAppToken` — generated when the server process starts. Changes on a server
    /// restart. **Used inside the library to check the freshness of markets indexes** — an
    /// external consumer usually does not need it, exposed for the diagnostic UI / event correlation.
    pub(crate) fn peer_app_token(&self) -> u64 {
        self.peer_app_token
    }

    pub(crate) fn market_indexes_current_for_peer(&self) -> bool {
        self.peer_app_token != 0
            && self.peer_app_token == self.reconnect.tracked_indexes_peer_app_token
    }

    // ====================================================================
    //  Log throttle — anti-spam helper for warnings.
    // ====================================================================

    /// Returns `true` if >= `interval_ms` has passed since the previous log with this `key`.
    /// Usage: wrap `eprintln!("...")` as `if client.should_log("X", 1000) { ... }`.
    /// `#[inline]`: called on EVERY warn/error in the send/recv paths.
    #[inline]
    pub(crate) fn should_log(&mut self, key: &'static str, interval_ms: i64) -> bool {
        let now_ms = self.now_ms();
        let last = self.metrics.log_last.entry(key).or_insert(0);
        if now_ms - *last >= interval_ms {
            *last = now_ms;
            true
        } else {
            false
        }
    }
}
