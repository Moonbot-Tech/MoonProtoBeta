use super::*;

impl Client {
    /// Matches TMoonProtoClient.Reset (IntStruct.pas:972-1000)
    /// Does NOT reset: server_token, actual_pmtu, send_datagram_num, pending_h,
    /// sending, api_pending, pending_candles, trip_delay_k, can_send_rate.
    pub(crate) fn full_reset(&mut self) {
        self.crypt_msg_counter.store(0, Ordering::Relaxed);
        self.total_sent.store(0, Ordering::Relaxed);
        self.total_recv = 0;
        self.total_recv_shared.store(0, Ordering::Relaxed);
        self.rs = 1.0;
        self.used_sliced_limit = false;
        self.data_read_state.reset();
        self.send_lock.lock().unwrap().reset_tmp_slider();
        self.recvd_slider = Slider::new();
        self.recv_slicer = slicing::SlicingReceiver::new();
        self.last_online = 0;
        self.last_sent_hello = NEVER_SENT_MS;
    }

    pub(crate) fn bind_socket(&mut self, cur_tm: i64) {
        self.transport_mode_state.reset();
        self.force_disconnect = false;
        if self.next_port < 1024 || self.next_port > 65000 {
            self.next_port = 1024;
        }
        // Bind family выбирается по серверному адресу. Если сервер — IPv6 literal `[2001:db8::1]:3000`
        // или DNS name резолвящийся в AAAA — bindаемся `[::]:port`. Иначе IPv4 `0.0.0.0:port`.
        let bind_family = if self.cfg.server_ip.contains(':') {
            "[::]"
        } else {
            "0.0.0.0"
        };
        let mut last_err: Option<std::io::Error> = None;
        for _ in 0..200 {
            let addr = format!("{}:{}", bind_family, self.next_port);
            match UdpSocket::bind(&addr) {
                Ok(sock) => {
                    if let Err(e) = sock.set_read_timeout(Some(Duration::from_secs(1))) {
                        warn!("set_read_timeout failed: {e}");
                    }
                    set_socket_buffers(&sock);
                    debug!("bound UDP socket on {}:{}", bind_family, self.next_port);
                    self.next_port += 1;
                    self.socket = Some(sock);
                    // Сброс кэша адреса сервера — может измениться при reconnect через DNS.
                    self.cached_server_addr = None;
                    self.start_inline_reader_session();
                    self.reset_bind_failure_tracking();
                    return;
                }
                Err(e) => {
                    last_err = Some(e);
                    self.next_port += 1;
                    if self.next_port > 65000 {
                        self.next_port = 1024;
                    }
                }
            }
        }
        // Все 200 попыток bind упали → не можем создать сокет В ЭТОТ ТИК.
        // НЕ ставим need_connect=false (audit_responsibility H3): на mobile при port
        // exhaustion (CGNAT, iOS background, ulimit) Disconnected заставил бы app
        // пересоздавать Client. Delphi (`MoonProtoUDPClient.pas:680+`) ретраит forever —
        // active library тоже должна.
        //
        // Throttled error-лог чтобы не спамить (раз в 5 сек). Следующий тик main loop
        // снова войдёт в bind_socket — обычно через короткое время порты освободятся.
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

        // auth_status оставляем Base — main loop попробует bind ещё раз через DEFAULT_SLEEP_MS.
        // Если app явно вызвал disconnect() — он сам выставит need_connect=false.
    }

    pub(crate) fn reset_bind_failure_tracking(&mut self) {
        self.bind_failure_streak = 0;
        self.first_bind_failure_ms = NEVER_TIME_MS;
        self.last_bind_failed_event_ms = NEVER_TIME_MS;
    }

    pub(crate) fn record_bind_failure(&mut self, cur_tm: i64) {
        if self.first_bind_failure_ms == NEVER_TIME_MS {
            self.first_bind_failure_ms = cur_tm;
        }
        self.bind_failure_streak = self.bind_failure_streak.saturating_add(1);

        let first_due =
            cur_tm.saturating_sub(self.first_bind_failure_ms) >= BIND_FAILED_FIRST_EVENT_MS;
        let repeat_due = self.last_bind_failed_event_ms == NEVER_TIME_MS
            || cur_tm.saturating_sub(self.last_bind_failed_event_ms) >= BIND_FAILED_REPEAT_EVENT_MS;

        if first_due && repeat_due {
            self.last_bind_failed_event_ms = cur_tm;
            self.fire_lifecycle(LifecycleEvent::BindFailed {
                consecutive_failures: self.bind_failure_streak,
            });
        }
    }
}
