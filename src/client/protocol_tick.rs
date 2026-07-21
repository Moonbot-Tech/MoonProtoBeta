use super::protocol_core::ProtocolCore;
use super::*;

impl ProtocolCore<'_> {
    pub(crate) fn writer_tick_prologue(&mut self, cur_tm: i64) {
        // Emit lifecycle events on auth_status transitions.
        self.client.check_lifecycle_transition();

        // ActualSleepTime EMA (matches UDPClient.pas:725-734)
        if self.client.prev_cycle_tm != 0 {
            let raw = (cur_tm - self.client.prev_cycle_tm).abs();
            if raw > 0 && raw < 100 {
                if self.client.actual_sleep_time <= 0.0 {
                    self.client.actual_sleep_time = raw as f64;
                } else {
                    self.client.actual_sleep_time =
                        self.client.actual_sleep_time * 0.7 + raw as f64 * 0.3;
                }
            }
        }
        self.client.prev_cycle_tm = cur_tm;
    }

    pub(crate) fn ensure_socket_bound(&mut self, cur_tm: i64) -> bool {
        if self.client.transport.socket.is_none() && self.client.need_connect {
            self.client.bind_socket(cur_tm);
        }
        if self.client.transport.socket.is_some() && self.client.transport.recv_poller.is_none() {
            self.client.register_recv_poller();
        }
        self.client.transport.socket.is_some()
    }

    pub(crate) fn drain_app_commands(&mut self, cur_tm: i64, mode: &mut RunMode<'_>) {
        self.drain_post_receive_delivery(cur_tm, mode);
    }

    pub(crate) fn wait_5ms(&mut self) {
        // Delphi writer sleeps a fixed short tick when there is no outgoing
        // work. In the single-owner Rust loop this wait is also the UDP
        // readable wait; the next loop drains the socket before send phase.
        if !self.client.send_lock.lock().is_empty() {
            return;
        }
        let timeout = Some(Duration::from_millis(DEFAULT_SLEEP_MS));
        let Some(poller) = self.client.transport.recv_poller.as_ref() else {
            thread::sleep(Duration::from_millis(DEFAULT_SLEEP_MS));
            return;
        };
        self.client.transport.recv_events.clear();
        match poller.wait(&mut self.client.transport.recv_events, timeout) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => {
                log::warn!(target: "moonproto::reader",
                    "UDP poller wait failed: {e}; falling back to sleep for this tick");
                thread::sleep(Duration::from_millis(DEFAULT_SLEEP_MS));
            }
        }
    }

    pub(crate) fn send_maintenance_phase(
        &mut self,
        cur_tm: i64,
        mode: &mut RunMode<'_>,
        #[cfg(any(test, feature = "diagnostics"))] protocol_metrics: &ProtocolMetrics,
    ) {
        #[cfg(any(test, feature = "diagnostics"))]
        let send_phase_start = Instant::now();
        #[cfg(any(test, feature = "diagnostics"))]
        let send_maintenance_start = Instant::now();
        self.transport_writer_maintenance_tick(cur_tm);
        #[cfg(any(test, feature = "diagnostics"))]
        protocol_metrics.record_profile_phase_labeled(
            ProfilePhase::SendMaintenance,
            send_maintenance_start.elapsed(),
            u8::MAX,
            u8::MAX,
            0,
        );
        #[cfg(any(test, feature = "diagnostics"))]
        protocol_metrics.record_send_phase(send_phase_start.elapsed());

        // Active library: all-trades reconnect sequence lives on the
        // writer tick. Gap recovery itself is checked only after
        // successful trades packets, like Delphi `ProcessTradesStream`.
        self.periodic_trades_reconnect_tick(cur_tm, mode);
        self.periodic_orderbook_reconnect_tick(cur_tm, mode);
        self.periodic_report_replication_tick(cur_tm, mode);
        self.periodic_orders_tick(cur_tm, mode);

        self.transport_reconnect_tail_tick(cur_tm);
    }

    pub(crate) fn transport_writer_maintenance_tick(&mut self, cur_tm: i64) {
        self.copy_send_ack_and_check_sening_data(cur_tm);

        // Timeout protection for the init/API markets-index request marker.
        self.check_indexes_fetch_timeout(cur_tm);

        // F6/F7: periodic refresh prices + tags (optional, via ClientConfig.refresh).
        // Send only if auth_status == AuthDone (the server accepts the request only in
        // this phase; before it the request would be wasted).
        if matches!(self.client.auth_status, AuthStatus::AuthDone)
            && self.client.subscriptions.domain_ready
        {
            self.tick_periodic_refresh(cur_tm);
        }
    }

    /// F6/F7: check whether it is time to send periodic refresh commands.
    /// Called from the writer loop every tick (~5ms), but the actual send happens
    /// only once `update_markets_every` / `check_tags_every` has elapsed since the last time.
    ///
    /// Fire-and-forget: we use `send_api_request` without registering in the pending registry —
    /// the EventDispatcher automatically applies the response to MarketsState when it arrives.
    pub(crate) fn tick_periodic_refresh(&mut self, cur_tm: i64) {
        let hour_slot = if self.client.cfg.refresh.check_tags_every.is_some() {
            current_utc_hour_slot()
        } else {
            self.client.refresh_clocks.check_tags_hour_slot
        };
        self.tick_periodic_refresh_at(cur_tm, hour_slot);
    }

    pub(crate) fn tick_periodic_refresh_at(&mut self, cur_tm: i64, hour_slot: i64) {
        let market_indexes_stale = self.client.subscriptions.domain_ready
            && self.client.domain_restore_needs_indexes()
            && self.client.peer_app_token != 0
            && !self.client.market_indexes_current_for_peer();

        if market_indexes_stale {
            if !self.client.reconnect.indexes_fetch_in_flight {
                self.client.send_markets_indexes_restore_request(cur_tm);
            }
        } else if let Some(interval) = self.client.cfg.refresh.update_markets_every {
            let interval_ms = interval.as_millis() as i64;
            if (cur_tm - self.client.refresh_clocks.last_update_markets_ms) >= interval_ms {
                self.client
                    .send_api_request(&crate::commands::engine_request::update_markets_list());
                self.client.refresh_clocks.last_update_markets_ms = cur_tm;
            }
        }

        if let Some(interval) = self.client.cfg.refresh.check_tags_every {
            if self.client.refresh_clocks.check_tags_hour_slot == i64::MIN {
                self.client.refresh_clocks.check_tags_hour_slot = hour_slot;
            } else if hour_slot != self.client.refresh_clocks.check_tags_hour_slot {
                self.client.refresh_clocks.check_tags_hour_slot = hour_slot;
                self.client.refresh_clocks.check_tags_burst_sent = 0;
                self.client.refresh_clocks.last_check_tags_burst_ms = i64::MIN / 2;
            }

            let interval_ms = interval.as_millis() as i64;
            let burst_due = self.client.refresh_clocks.check_tags_burst_sent
                < CHECK_TAGS_BURST_COUNT
                && (cur_tm - self.client.refresh_clocks.last_check_tags_burst_ms)
                    >= CHECK_TAGS_BURST_SPACING_MS;
            let interval_due =
                (cur_tm - self.client.refresh_clocks.last_check_tags_ms) >= interval_ms;

            if burst_due || interval_due {
                self.client
                    .send_api_request(&crate::commands::engine_request::check_binance_tags());
                self.client.refresh_clocks.last_check_tags_ms = cur_tm;
                if self.client.refresh_clocks.check_tags_burst_sent < CHECK_TAGS_BURST_COUNT {
                    self.client.refresh_clocks.check_tags_burst_sent += 1;
                    self.client.refresh_clocks.last_check_tags_burst_ms = cur_tm;
                }
            }
        }
    }

    /// Periodic timeout cleanup/retry for an in-flight markets-index restore marker.
    /// The UDP response may be lost — without this check `indexes_fetch_in_flight = true`
    /// would stay set forever. Before Init the request is NOT sent; after Init, reconnect
    /// restore is allowed to repeat `GetMarketsIndexes`, because the user intent was already
    /// established by the single init pass.
    pub(crate) fn check_indexes_fetch_timeout(&mut self, now_ms: i64) {
        const INDEXES_FETCH_TIMEOUT_MS: i64 = 12_000;
        if self.client.reconnect.indexes_fetch_in_flight
            && now_ms - self.client.reconnect.indexes_fetch_started_ms > INDEXES_FETCH_TIMEOUT_MS
        {
            self.client.reconnect.indexes_fetch_in_flight = false;
            if self.client.subscriptions.domain_ready
                && self.client.domain_restore_needs_indexes()
                && self.client.peer_app_token != 0
                && !self.client.market_indexes_current_for_peer()
            {
                self.client.send_markets_indexes_restore_request(now_ms);
            }
        }
    }

    /// Periodic all-trades reconnect sequence (Dispatcher mode only).
    /// Trades gap recovery is not here: Delphi calls `CheckMissingTradesPackets`
    /// from the tail of `ProcessTradesStream`, and Rust mirrors that in
    /// `EventDispatcher::dispatch_into_active_actions`.
    pub(crate) fn periodic_trades_reconnect_tick(&mut self, cur_tm: i64, mode: &mut RunMode<'_>) {
        if cur_tm - self.client.reconnect.last_trades_tick_ms < 100 {
            return;
        }
        self.client.reconnect.last_trades_tick_ms = cur_tm;
        let trades_server_token = mode.dispatcher.trades_server_token();
        self.client
            .tick_trades_reconnect_sequence(cur_tm, trades_server_token);
    }

    pub(crate) fn periodic_orderbook_reconnect_tick(
        &mut self,
        cur_tm: i64,
        mode: &mut RunMode<'_>,
    ) {
        if self.client.tick_orderbook_reconnect_sequence(cur_tm) {
            mode.dispatcher.reset_orderbook_caches_keep_books();
        }
    }

    pub(crate) fn periodic_report_replication_tick(&mut self, cur_tm: i64, mode: &mut RunMode<'_>) {
        if !matches!(self.client.auth_status, AuthStatus::AuthDone)
            || !self.client.subscriptions.domain_ready
            || self.client.server_token == 0
        {
            return;
        }
        let sync_intent = self.client.report_sync_intent();
        let open_rows_intent = self.client.report_open_rows_intent();
        if sync_intent.is_none() && open_rows_intent.is_empty() {
            return;
        }

        if mode.dispatcher.report_schema().is_none() {
            if cur_tm.saturating_sub(
                self.client
                    .reconnect
                    .last_report_schema_request_ms
                    .load(Ordering::Relaxed),
            ) < crate::client::domain_report::REPORT_RESPONSE_TIMEOUT_MS
            {
                return;
            }
            if let Some(request) = sync_intent {
                let ticket = Client::next_report_sync_ticket();
                mode.dispatcher
                    .defer_report_sync_until_schema(ticket, request);
            }
            if !open_rows_intent.is_empty() {
                mode.dispatcher
                    .defer_report_open_rows_check_until_schema(open_rows_intent);
            }
            self.client.request_report_schema_at(cur_tm);
            return;
        }
        if !self.client.report_schema_is_current() {
            if cur_tm.saturating_sub(
                self.client
                    .reconnect
                    .last_report_schema_request_ms
                    .load(Ordering::Relaxed),
            ) >= crate::client::domain_report::REPORT_RESPONSE_TIMEOUT_MS
            {
                self.client.request_report_schema_at(cur_tm);
            }
            return;
        }

        if let Some(request) = sync_intent {
            let waiting_for_apply = mode.dispatcher.report_waiting_for_page_apply()
                || self
                    .client
                    .reconnect
                    .report_page_waiting_apply_uid
                    .load(Ordering::Relaxed)
                    != 0;
            if !waiting_for_apply {
                let pending_uid = self
                    .client
                    .reconnect
                    .pending_report_sync_uid
                    .load(Ordering::Relaxed);
                let pending_for_current_token = pending_uid != 0
                    && self
                        .client
                        .reconnect
                        .pending_report_server_token
                        .load(Ordering::Relaxed)
                        == self.client.server_token;
                let response_wait_active = pending_for_current_token
                    && cur_tm.saturating_sub(
                        self.client
                            .reconnect
                            .last_report_sync_request_ms
                            .load(Ordering::Relaxed),
                    ) < crate::client::domain_report::REPORT_RESPONSE_TIMEOUT_MS;
                if !response_wait_active {
                    if let Some((request_uid, active_request)) =
                        mode.dispatcher.retry_active_report_page()
                    {
                        self.client.set_report_sync_intent(active_request);
                        self.client
                            .send_report_sync_at(request_uid, active_request, cur_tm);
                    } else if self
                        .client
                        .reconnect
                        .subscribed_report_server_token
                        .load(Ordering::Relaxed)
                        != self.client.server_token
                    {
                        let ticket = Client::next_report_sync_ticket();
                        let request_uid = mode.dispatcher.begin_report_sync(ticket, request);
                        self.client
                            .send_report_sync_at(request_uid, request, cur_tm);
                    }
                }
            }
        }

        if open_rows_intent.is_empty() {
            return;
        }
        let check_token = self
            .client
            .reconnect
            .subscribed_report_check_server_token
            .load(Ordering::Relaxed);
        let sent_token = self
            .client
            .reconnect
            .pending_report_check_server_token
            .load(Ordering::Relaxed);
        let check_timed_out = cur_tm.saturating_sub(
            self.client
                .reconnect
                .last_report_check_request_ms
                .load(Ordering::Relaxed),
        ) >= crate::client::domain_report::REPORT_RESPONSE_TIMEOUT_MS;

        if sent_token != self.client.server_token {
            mode.dispatcher
                .begin_report_open_rows_check(Arc::clone(&open_rows_intent));
            self.client
                .send_report_open_rows_check_at(&open_rows_intent, cur_tm);
        } else if let Some(pending) = mode.dispatcher.pending_report_open_row_ids() {
            if check_timed_out {
                self.client.send_report_open_rows_check_at(&pending, cur_tm);
            }
        } else if check_token != self.client.server_token {
            mode.dispatcher
                .begin_report_open_rows_check(Arc::clone(&open_rows_intent));
            self.client
                .send_report_open_rows_check_at(&open_rows_intent, cur_tm);
        }
    }

    pub(crate) fn periodic_orders_tick(&mut self, cur_tm: i64, mode: &mut RunMode<'_>) {
        mode.event_buf.clear();
        mode.active_actions_buf.clear();
        mode.dispatcher.tick_orders_active_actions(
            cur_tm,
            &mut mode.event_buf,
            &mut mode.active_actions_buf,
        );
        self.client
            .apply_active_actions(mode.active_actions_buf.drain(..));
        mode.drain_events(&self.client.metrics.protocol_metrics, None, u8::MAX, 0);
    }

    pub(crate) fn transport_reconnect_tail_tick(&mut self, cur_tm: i64) {
        // Reconnect logic
        self.check_hello_send(cur_tm);
        self.check_offline_reconnect(cur_tm);
        self.check_reconnect_timeout(cur_tm);
        self.check_dead_zone(cur_tm);

        if self.client.force_disconnect {
            self.do_force_disconnect();
        }
    }
}
