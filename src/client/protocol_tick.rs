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
        if self.client.socket.is_none() && self.client.need_connect {
            self.client.bind_socket(cur_tm);
        }
        if self.client.socket.is_some() && self.client.recv_poller.is_none() {
            self.client.register_recv_poller();
        }
        self.client.socket.is_some()
    }

    pub(crate) fn drain_app_commands(&mut self, cur_tm: i64, mode: &mut RunMode<'_>) {
        self.drain_post_receive_delivery(cur_tm, mode);
    }

    pub(crate) fn wait_5ms(&mut self) {
        // Delphi writer sleeps a fixed short tick when there is no outgoing
        // work. In the single-owner Rust loop this wait is also the UDP
        // readable wait; the next loop drains the socket before send phase.
        if !self.client.send_lock.lock().unwrap().is_empty() {
            return;
        }
        let timeout = Some(Duration::from_millis(DEFAULT_SLEEP_MS));
        let Some(poller) = self.client.recv_poller.as_ref() else {
            thread::sleep(Duration::from_millis(DEFAULT_SLEEP_MS));
            return;
        };
        self.client.recv_events.clear();
        match poller.wait(&mut self.client.recv_events, timeout) {
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
        protocol_metrics: &ProtocolMetrics,
    ) {
        let send_phase_start = Instant::now();
        self.transport_writer_maintenance_tick(cur_tm);
        protocol_metrics.record_send_phase(send_phase_start.elapsed());

        // Active library: all-trades reconnect sequence lives on the
        // writer tick. Gap recovery itself is checked only after
        // successful trades packets, like Delphi `ProcessTradesStream`.
        self.periodic_trades_reconnect_tick(cur_tm, mode);
        self.periodic_orderbook_reconnect_tick(cur_tm, mode);
        self.periodic_orders_tick(cur_tm, mode);

        self.transport_reconnect_tail_tick(cur_tm);
    }

    pub(crate) fn transport_writer_maintenance_tick(&mut self, cur_tm: i64) {
        self.copy_send_ack_and_check_sening_data(cur_tm);

        // Timeout protection для init/API markets-index request marker.
        self.check_indexes_fetch_timeout(cur_tm);

        // F6/F7: periodic refresh prices + tags (опционально через ClientConfig.refresh).
        // Шлём только если auth_status == AuthDone (сервер примет запрос только в этой
        // фазе; до неё запрос потеряется впустую).
        if matches!(self.client.auth_status, AuthStatus::AuthDone) && self.client.domain_ready {
            self.tick_periodic_refresh(cur_tm);
        }
    }

    /// F6/F7: проверка пора ли слать periodic refresh-команды.
    /// Вызывается из writer loop каждый тик (~5мс), но реальная отправка происходит
    /// только когда прошёл `update_markets_every` / `check_tags_every` от последнего раза.
    ///
    /// Fire-and-forget: используем `send_api_request` без регистрации в pending registry —
    /// EventDispatcher автоматически применяет ответ к MarketsState когда он придёт.
    pub(crate) fn tick_periodic_refresh(&mut self, cur_tm: i64) {
        let hour_slot = if self.client.cfg.refresh.check_tags_every.is_some() {
            current_utc_hour_slot()
        } else {
            self.client.check_tags_hour_slot
        };
        self.tick_periodic_refresh_at(cur_tm, hour_slot);
    }

    pub(crate) fn tick_periodic_refresh_at(&mut self, cur_tm: i64, hour_slot: i64) {
        if self.client.domain_ready
            && self.client.domain_restore_needs_indexes()
            && self.client.peer_app_token != 0
            && !self.client.market_indexes_current_for_peer()
            && !self.client.indexes_fetch_in_flight
        {
            self.client.send_markets_indexes_restore_request(cur_tm);
        }

        if let Some(interval) = self.client.cfg.refresh.update_markets_every {
            let interval_ms = interval.as_millis() as i64;
            if (cur_tm - self.client.last_update_markets_ms) >= interval_ms {
                self.client
                    .send_api_request(&crate::commands::engine_request::update_markets_list());
                self.client.last_update_markets_ms = cur_tm;
            }
        }
        if let Some(interval) = self.client.cfg.refresh.check_tags_every {
            if self.client.check_tags_hour_slot == i64::MIN {
                self.client.check_tags_hour_slot = hour_slot;
            } else if hour_slot != self.client.check_tags_hour_slot {
                self.client.check_tags_hour_slot = hour_slot;
                self.client.check_tags_burst_sent = 0;
                self.client.last_check_tags_burst_ms = i64::MIN / 2;
            }

            let interval_ms = interval.as_millis() as i64;
            let burst_due = self.client.check_tags_burst_sent < CHECK_TAGS_BURST_COUNT
                && (cur_tm - self.client.last_check_tags_burst_ms) >= CHECK_TAGS_BURST_SPACING_MS;
            let interval_due = (cur_tm - self.client.last_check_tags_ms) >= interval_ms;

            if burst_due || interval_due {
                self.client
                    .send_api_request(&crate::commands::engine_request::check_binance_tags());
                self.client.last_check_tags_ms = cur_tm;
                if self.client.check_tags_burst_sent < CHECK_TAGS_BURST_COUNT {
                    self.client.check_tags_burst_sent += 1;
                    self.client.last_check_tags_burst_ms = cur_tm;
                }
            }
        }
    }

    /// Periodic timeout cleanup/retry for an in-flight markets-index restore marker.
    /// UDP-ответ может потеряться — без этой проверки `indexes_fetch_in_flight = true`
    /// остался бы навсегда. До Init запрос НЕ отправляется; после Init reconnect
    /// restore имеет право повторить `GetMarketsIndexes`, потому что пользовательский
    /// intent уже был задан единственным init-проходом.
    pub(crate) fn check_indexes_fetch_timeout(&mut self, now_ms: i64) {
        const INDEXES_FETCH_TIMEOUT_MS: i64 = 12_000;
        if self.client.indexes_fetch_in_flight
            && now_ms - self.client.indexes_fetch_started_ms > INDEXES_FETCH_TIMEOUT_MS
        {
            self.client.indexes_fetch_in_flight = false;
            if self.client.domain_ready
                && self.client.domain_restore_needs_indexes()
                && self.client.peer_app_token != 0
                && !self.client.market_indexes_current_for_peer()
            {
                self.client.send_markets_indexes_restore_request(now_ms);
            }
        }
    }

    /// Periodic all-trades reconnect sequence (только в Dispatcher mode).
    /// Trades gap recovery is not here: Delphi calls `CheckMissingTradesPackets`
    /// from the tail of `ProcessTradesStream`, and Rust mirrors that in
    /// `EventDispatcher::dispatch_into_active_actions`.
    pub(crate) fn periodic_trades_reconnect_tick(&mut self, cur_tm: i64, mode: &mut RunMode<'_>) {
        if cur_tm - self.client.last_trades_tick_ms < 100 {
            return;
        }
        self.client.last_trades_tick_ms = cur_tm;
        let trades_server_token = match mode {
            #[cfg(test)]
            RunMode::Dispatcher { dispatcher, .. } => dispatcher.trades_server_token(),
            RunMode::DispatcherWorker { .. } => self
                .client
                .dispatcher_trades_server_token
                .load(Ordering::Relaxed),
            #[cfg(test)]
            RunMode::Callback { .. } | RunMode::CallbackQueue { .. } => return,
            #[cfg(not(test))]
            RunMode::CallbackQueue { .. } => return,
            #[cfg(not(test))]
            RunMode::_Lifetime(_) => return,
        };
        self.client
            .tick_trades_reconnect_sequence(cur_tm, trades_server_token);
    }

    pub(crate) fn send_worker_item(mode: &RunMode<'_>, item: DispatcherWorkItem) {
        if let RunMode::DispatcherWorker { tx, .. } = mode {
            let _ = tx.send(item);
        }
    }

    pub(crate) fn periodic_orderbook_reconnect_tick(
        &mut self,
        cur_tm: i64,
        mode: &mut RunMode<'_>,
    ) {
        match mode {
            #[cfg(test)]
            RunMode::Dispatcher { dispatcher, .. } => {
                if self.client.tick_orderbook_reconnect_sequence(cur_tm) {
                    dispatcher.reset_orderbook_caches_keep_books();
                }
            }
            RunMode::DispatcherWorker { .. } => {
                if self.client.tick_orderbook_reconnect_sequence(cur_tm) {
                    Self::send_worker_item(mode, DispatcherWorkItem::ResetOrderbookCachesKeepBooks);
                }
            }
            #[cfg(test)]
            RunMode::Callback { .. } | RunMode::CallbackQueue { .. } => {}
            #[cfg(not(test))]
            RunMode::CallbackQueue { .. } => {}
            #[cfg(not(test))]
            RunMode::_Lifetime(_) => {}
        }
    }

    pub(crate) fn periodic_orders_tick(&mut self, cur_tm: i64, mode: &mut RunMode<'_>) {
        match mode {
            #[cfg(test)]
            RunMode::Dispatcher {
                dispatcher,
                on_event,
                event_buf,
                active_actions_buf,
                ..
            } => {
                event_buf.clear();
                active_actions_buf.clear();
                dispatcher.tick_orders_active_actions(cur_tm, event_buf, active_actions_buf);
                self.client
                    .apply_active_actions(active_actions_buf.drain(..));
                on_event.drain_events(
                    event_buf,
                    dispatcher,
                    &self.client.protocol_metrics,
                    None,
                    u8::MAX,
                    0,
                );
            }
            RunMode::DispatcherWorker { tx, .. } => {
                let _ = tx.send(DispatcherWorkItem::TickOrders { now_ms: cur_tm });
            }
            #[cfg(test)]
            RunMode::Callback { .. } | RunMode::CallbackQueue { .. } => {}
            #[cfg(not(test))]
            RunMode::CallbackQueue { .. } => {}
            #[cfg(not(test))]
            RunMode::_Lifetime(_) => {}
        }
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
