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

    /// EMA % retransmission overhead для Sliced пакетов (matches AvgOverHeat MoonProtoIntStruct.pas:220).
    /// 0 = идеально (no retries). >0 = вынужденные перепосылы.
    pub fn avg_over_heat(&self) -> f64 {
        self.avg_over_heat
    }

    // ====================================================================
    //  Diagnostic getters (audit_responsibility A4)
    //
    //  В Delphi `TMoonProtoNetClient` эти поля публичны и читаются UI
    //  (MoonProtoUnit.pas:363 — "Ping: %d PMTU: %d RS: %d%%"). Aналог в Rust
    //  для построения статус-строки терминала.
    // ====================================================================

    /// RTT в ms (последний измеренный из Ping). Соответствует Delphi
    /// `TMoonProtoNetClient.RoundTripDelay` (MoonProtoClient.pas:62).
    pub fn round_trip_delay_ms(&self) -> i64 {
        self.round_trip_delay
    }

    /// Текущий Path MTU в байтах. Стартует с 508; runtime ProbeMTU может
    /// увеличивать значение выше 8000 шагами по 32 байта.
    /// Соответствует Delphi `TMoonProtoNetClient.PMTU`.
    pub fn actual_pmtu(&self) -> u16 {
        self.actual_pmtu
    }

    /// Receive Status [0.0..1.0] — качество downlink канала. >0.92 = норма,
    /// <0.85 = критично, между = серая зона. Соответствует Delphi
    /// `TMoonProtoNetClient.RS`.
    pub fn rs(&self) -> f64 {
        self.rs
    }

    /// `ServerTime - LocalTime` в днях (как Delphi TDateTime). Применяется
    /// автоматически к timestamp'ам входящих ордеров через `Orders::apply`.
    /// Внешним потребителям обычно не нужен — выставлен публично для диагностики.
    pub fn server_time_delta_days(&self) -> f64 {
        self.server_time_delta
    }

    /// `|ServerTime - LocalTime|` в ms (абсолютный лаг от последнего Ping).
    /// Полезно для UI индикатора "сервер близко / далеко".
    pub fn net_lag_ping_ms(&self) -> i64 {
        self.net_lag_ping
    }

    /// `Orders cycle ms` от сервера — рекомендованный темп опроса ордерных событий.
    /// Соответствует Delphi `TMoonProtoNetClient.GlobalTimingOrders`.
    pub fn global_timing_orders(&self) -> u16 {
        self.global_timing_orders
    }

    /// Текущий `ServerToken` — меняется при каждом hard handshake (Hello→WhoAreYou→Fine).
    /// Soft reconnect (HelloAgain) НЕ меняет этот токен. **Внутри либы используется для
    /// init/API subscription restore** — внешнему потребителю обычно не нужен,
    /// выставлен для diagnostic UI.
    pub fn server_token(&self) -> u64 {
        self.server_token
    }

    pub(crate) fn subscribed_book_server_token(&self) -> u64 {
        self.subscribed_book_server_token
    }

    /// `PeerAppToken` — генерируется при старте серверного процесса. Меняется при перезапуске
    /// сервера. **Внутри либы используется для проверки свежести markets indexes** — внешнему
    /// потребителю обычно не нужен, выставлен для diagnostic UI / event correlation.
    pub fn peer_app_token(&self) -> u64 {
        self.peer_app_token
    }

    pub(crate) fn market_indexes_current_for_peer(&self) -> bool {
        self.peer_app_token != 0 && self.peer_app_token == self.tracked_indexes_peer_app_token
    }

    // ====================================================================
    //  BytesPerSec — O(1) EMA counter (порт Delphi AddBytesCount)
    // ====================================================================
    //
    // Аудит #5 (audit_delphi_deviation): ранее использовался `VecDeque<(i64,u64)>` sliding
    // window. На пике 50K pps входящих VecDeque раскручивался до ~500K entries × 16B = 8MB
    // только для recv (+ ещё 8MB для sent). Плюс 100K push_back/pop_front ops/sec.
    //
    // Delphi решает это за 24 байта (3×u64) + 1 if + 1 add per packet — byte-exact порт
    // `MoonProtoUDPClient.pas:113-138 AddBytesCount`. EMA формула: `ema = ema*9/10 + bucket`,
    // что в steady state даёт `ema = 10*bytes_per_sec` (отсюда деление на 10 в getter'е).

    pub(crate) fn track_sent(&mut self, bytes: u64, ts_ms: i64) {
        self.bps_sent.add(bytes, ts_ms);
    }

    pub(crate) fn track_recv(&mut self, bytes: u64, ts_ms: i64) {
        self.bps_recv.add(bytes, ts_ms);
    }

    /// Байт отправлено в среднем за последние ~10 секунд (B/s). O(1) EMA, see [`BpsCounter`].
    pub fn bytes_per_sec_sent(&self) -> u64 {
        self.bps_sent.bytes_per_sec()
    }
    /// Байт принято в среднем за последние ~10 секунд (B/s). O(1) EMA.
    pub fn bytes_per_sec_recv(&self) -> u64 {
        self.bps_recv.bytes_per_sec()
    }

    // ====================================================================
    //  Log throttle — anti-spam helper для warning'ов.
    // ====================================================================

    /// Возвращает `true` если с момента предыдущего лога с этим `key` прошло ≥ `interval_ms`.
    /// Применение: оборачивать `eprintln!("...")` через `if client.should_log("X", 1000) { ... }`.
    /// `#[inline]`: вызывается на КАЖДОМ warn/error в send/recv pathes.
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
