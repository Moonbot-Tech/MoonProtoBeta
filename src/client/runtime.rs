use super::*;

impl Client {
    pub(crate) fn start_inline_reader_session(&mut self) {
        self.recv_slicer = slicing::SlicingReceiver::new();
        self.register_recv_poller();
    }

    pub(crate) fn clear_recv_poller(&mut self) {
        if let (Some(poller), Some(sock)) = (self.recv_poller.as_ref(), self.socket.as_ref()) {
            if let Err(e) = poller.delete(sock) {
                log::warn!(target: "moonproto::reader", "UDP poller delete failed: {e}");
            }
        }
        self.recv_poller = None;
        self.recv_events.clear();
    }

    pub(crate) fn register_recv_poller(&mut self) {
        self.clear_recv_poller();
        let Some(sock) = self.socket.as_ref() else {
            return;
        };
        if let Err(e) = sock.set_nonblocking(true) {
            log::warn!(target: "moonproto::reader", "set_nonblocking(true) failed: {e}");
            return;
        }
        let poller = match Poller::new() {
            Ok(poller) => poller,
            Err(e) => {
                log::warn!(target: "moonproto::reader",
                    "UDP poller create failed: {e}; falling back to 5ms nonblocking recv probe");
                return;
            }
        };
        // Safety: the client owns this UDP socket and deletes it from the
        // poller before replacing or dropping the socket.
        let add_result = unsafe { poller.add(sock, PollEvent::readable(1)) };
        if let Err(e) = add_result {
            log::warn!(target: "moonproto::reader",
                "UDP poller add failed: {e}; falling back to 5ms nonblocking recv probe");
            return;
        }
        self.recv_poller = Some(poller);
    }

    /// GetTimeMS equivalent — монотонные миллисекунды с момента старта `Client` (matches
    /// Delphi GetTickCount64 семантикой "since some fixed past point").
    ///
    /// B-V3-02 fix: ранее использовался `SystemTime::now()` (clock_gettime CLOCK_REALTIME)
    /// — ~30-100ns per call. На hot path receive loop (50K pps на пике TradesStream)
    /// это давало 1-5 мс/сек wasted CPU + потенциальный wall-clock jump при NTP-step
    /// (ломал бы diff'ы). `Instant::elapsed()` использует CLOCK_MONOTONIC (на Linux/Mac)
    /// либо QueryPerformanceCounter (Windows) — стабильный, ~5-20ns per call, не
    /// подвержен NTP-корректировкам.
    ///
    /// **Semantic change vs предыдущая версия:** возвращает ms since process start,
    /// не ms since UNIX_EPOCH. Все callers используют **diff** между двумя `now_ms()`,
    /// так что absolute-base разница не имеет значения.
    ///
    /// MUST use same time base everywhere (receive, send, slicing) —
    /// гарантируется через общий `self._start: Instant`.
    #[inline]
    pub(crate) fn now_ms(&self) -> i64 {
        self._start.elapsed().as_millis() as i64
    }

    /// Получить кэшированный SocketAddr сервера. Резолвится один раз при `bind_socket` или
    /// первом вызове, далее используется без re-resolve. Закрывает B-05.
    /// При неудаче resolve — `None`, отправка пакетов не происходит (логируется).
    pub(crate) fn server_socket_addr(&mut self) -> Option<SocketAddr> {
        if let Some(addr) = self.cached_server_addr {
            return Some(addr);
        }
        let key = format!("{}:{}", self.cfg.server_ip, self.cfg.server_port);
        match key.to_socket_addrs() {
            Ok(mut iter) => {
                if let Some(addr) = iter.next() {
                    self.cached_server_addr = Some(addr);
                    return Some(addr);
                }
                if self.should_log("server_addr_empty", 5000) {
                    error!("server address resolve returned empty: {}", key);
                }
                None
            }
            Err(e) => {
                if self.should_log("server_addr_resolve_fail", 5000) {
                    error!("server address resolve failed for {}: {}", key, e);
                }
                None
            }
        }
    }

    /// Run the client protocol loop for `duration`.
    /// Matches TMoonProtoUDPClient.Execute.
    pub fn run(&mut self, duration: Duration, on_data: OnDataFn) {
        // Low-level raw API для потребителей которым НЕ нужны active-library
        // auto-actions (RequestOrderBookFull, trades resend tail-check, и т.п.).
        // User callback выполняется через app queue, а не внутри protocol tick.
        let (app_tx, app_rx) = mpsc::channel::<RawAppEvent>();
        let lifecycle_pair = self.lifecycle_cb.take().map(|cb| {
            let (tx, rx) = mpsc::channel::<LifecycleEvent>();
            *self.lifecycle_app_tx.lock().unwrap() = Some(tx);
            (rx, cb)
        });
        let lifecycle_app_tx = Arc::clone(&self.lifecycle_app_tx);
        let mut restored_lifecycle_cb: Option<LifecycleFn> = None;
        thread::scope(|scope| {
            let lifecycle_handle = lifecycle_pair.map(|(rx, cb)| {
                scope.spawn(move || {
                    let mut cb = cb;
                    while let Ok(event) = rx.recv() {
                        cb(event);
                    }
                    cb
                })
            });
            let app_handle = scope.spawn(move || {
                let mut on_data = on_data;
                while let Ok((cmd, payload)) = app_rx.recv() {
                    on_data(cmd, &payload);
                }
            });
            {
                let mut mode = RunMode::CallbackQueue { app_tx };
                ProtocolCore { client: self }.run(duration, &mut mode);
            }
            *lifecycle_app_tx.lock().unwrap() = None;
            app_handle
                .join()
                .expect("moonproto app callback thread panicked");
            if let Some(handle) = lifecycle_handle {
                restored_lifecycle_cb = Some(
                    handle
                        .join()
                        .expect("moonproto lifecycle callback thread panicked"),
                );
            }
        });
        if restored_lifecycle_cb.is_some() {
            self.lifecycle_cb = restored_lifecycle_cb;
        }
    }

    /// Send LogOff and close socket. Call when done.
    /// Matches TMoonProtoBaseClient.Disconnect (Common.pas:290-298)
    pub fn disconnect(&mut self) {
        self.need_connect = false;
        self.force_disconnect = true;
        self.authorized = false;
        self.auth_status = AuthStatus::Base;
        self.set_domain_ready(false);
    }
}
