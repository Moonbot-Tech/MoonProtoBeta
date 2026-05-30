use super::*;

impl Client {
    /// Matches TMoonProtoClient.Reset (IntStruct.pas:972-1000)
    /// Does NOT reset: server_token, actual_pmtu, send_datagram_num, pending_h,
    /// sending, api_pending, pending_candles, trip_delay_k, can_send_rate.
    pub(crate) fn full_reset(&mut self) {
        self.crypt_msg_counter.store(0, Ordering::Relaxed);
        self.metrics.total_sent.store(0, Ordering::Relaxed);
        self.metrics.total_recv = 0;
        self.metrics.total_recv_shared.store(0, Ordering::Relaxed);
        self.rs = 1.0;
        self.used_sliced_limit = false;
        self.recv.data_read_state.reset();
        self.send_lock.lock().unwrap().reset_tmp_slider();
        self.recv.recvd_slider = Slider::new();
        self.transport.recv_slicer = slicing::SlicingReceiver::new();
        self.last_online = 0;
        self.last_sent_hello = NEVER_SENT_MS;
        self.clear_hello_wait_state();
    }

    pub(crate) fn bind_socket(&mut self, cur_tm: i64) {
        self.transport.transport_mode_state.reset();
        self.force_disconnect = false;
        if self.transport.next_port < 1024 || self.transport.next_port > 65000 {
            self.transport.next_port = 1024;
        }
        // The bind family is chosen by the server address. If the server is an IPv6 literal
        // `[2001:db8::1]:3000` or a DNS name resolving to AAAA — bind to `[::]:port`.
        // Otherwise IPv4 `0.0.0.0:port`.
        let bind_family = if self.cfg.server_ip.contains(':') {
            "[::]"
        } else {
            "0.0.0.0"
        };
        let mut last_err: Option<std::io::Error> = None;
        for _ in 0..200 {
            let addr = format!("{}:{}", bind_family, self.transport.next_port);
            match UdpSocket::bind(&addr) {
                Ok(sock) => {
                    if let Err(e) = sock.set_read_timeout(Some(Duration::from_secs(1))) {
                        warn!("set_read_timeout failed: {e}");
                    }
                    set_socket_buffers(&sock);
                    debug!(
                        "bound UDP socket on {}:{}",
                        bind_family, self.transport.next_port
                    );
                    self.transport.next_port += 1;
                    self.transport.socket = Some(sock);
                    // Reset the cached server address — it may change on reconnect via DNS.
                    self.transport.cached_server_addr = None;
                    self.start_inline_reader_session();
                    self.reset_bind_failure_tracking();
                    return;
                }
                Err(e) => {
                    last_err = Some(e);
                    self.transport.next_port += 1;
                    if self.transport.next_port > 65000 {
                        self.transport.next_port = 1024;
                    }
                }
            }
        }
        // All 200 bind attempts failed → we cannot create a socket ON THIS TICK.
        // Do NOT set need_connect=false (audit_responsibility H3): on mobile, port
        // exhaustion (CGNAT, iOS background, ulimit) plus Disconnected would force the app
        // to recreate the Client. Delphi (`MoonProtoUDPClient.pas:680+`) retries forever —
        // the active library must too.
        //
        // Throttled error log to avoid spam (once every 5s). The next tick of the main loop
        // will enter bind_socket again — usually the ports free up after a short time.
        if self.should_log("bind_socket_exhausted", 5000) {
            if let Some(ref e) = last_err {
                error!(target: "moonproto::client",
                    "UdpSocket::bind failed after 200 attempts on {}:*, last error: {} (will retry on next tick)",
                    bind_family, e);
            } else {
                error!(target: "moonproto::client",
                    "UdpSocket::bind failed after 200 attempts on {}:* (will retry on next tick)",
                    bind_family);
            }
        }

        self.record_bind_failure(cur_tm);

        // Leave auth_status as Base — the main loop will try to bind again after DEFAULT_SLEEP_MS.
        // If the app explicitly called disconnect() — it will set need_connect=false itself.
    }

    pub(crate) fn reset_bind_failure_tracking(&mut self) {
        self.transport.bind_failure_streak = 0;
        self.transport.first_bind_failure_ms = NEVER_TIME_MS;
        self.transport.last_bind_failed_event_ms = NEVER_TIME_MS;
    }

    pub(crate) fn record_bind_failure(&mut self, cur_tm: i64) {
        if self.transport.first_bind_failure_ms == NEVER_TIME_MS {
            self.transport.first_bind_failure_ms = cur_tm;
        }
        self.transport.bind_failure_streak = self.transport.bind_failure_streak.saturating_add(1);

        let first_due = cur_tm.saturating_sub(self.transport.first_bind_failure_ms)
            >= BIND_FAILED_FIRST_EVENT_MS;
        let repeat_due = self.transport.last_bind_failed_event_ms == NEVER_TIME_MS
            || cur_tm.saturating_sub(self.transport.last_bind_failed_event_ms)
                >= BIND_FAILED_REPEAT_EVENT_MS;

        if first_due && repeat_due {
            self.transport.last_bind_failed_event_ms = cur_tm;
            self.fire_lifecycle(LifecycleEvent::BindFailed {
                consecutive_failures: self.transport.bind_failure_streak,
            });
        }
    }
}
