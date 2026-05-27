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

    /// `GetTimeMS` equivalent: monotonic milliseconds since this `Client`
    /// started. This matches the Delphi `GetTickCount64` semantic of
    /// "milliseconds since some fixed point in the past".
    ///
    /// B-V3-02 fix: the old code used `SystemTime::now()` / realtime clock.
    /// On the receive hot path that cost extra CPU and could jump under NTP
    /// adjustments. `Instant::elapsed()` uses a monotonic clock and all callers
    /// compare differences rather than the absolute epoch.
    ///
    /// The same time base must be used for receive, send, and slicing; the
    /// shared `self._start: Instant` enforces that.
    #[inline]
    pub(crate) fn now_ms(&self) -> i64 {
        self._start.elapsed().as_millis() as i64
    }

    /// Return cached server `SocketAddr`.
    ///
    /// The address is resolved once on bind or first use and then reused without
    /// repeated DNS/`getaddrinfo` work. On resolve failure, packets are not sent
    /// and the failure is logged.
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

    /// Low-level finite protocol pump for tests and custom protocol tools.
    ///
    /// Regular applications should use [`MoonClient`](crate::MoonClient): it
    /// owns the runtime thread and has no user-selected loop duration.
    #[doc(hidden)]
    pub fn run(&mut self, duration: Duration, on_data: OnDataFn) {
        // Low-level raw API for consumers that intentionally do not use
        // active-library auto-actions such as RequestOrderBookFull or trades
        // resend tail-check. The user callback runs through the app queue, not
        // inside the protocol tick.
        let (app_tx, app_rx) = mpsc::channel::<RawAppEvent>();
        let lifecycle_pair = if self.lifecycle_event_sender_installed() {
            None
        } else {
            self.lifecycle_cb.take().map(|cb| {
                let (tx, rx) = mpsc::channel::<LifecycleEvent>();
                *self.lifecycle_app_tx.lock().unwrap() = Some(tx);
                (rx, cb)
            })
        };
        let clear_lifecycle_app_tx = lifecycle_pair.is_some();
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
            if clear_lifecycle_app_tx {
                *lifecycle_app_tx.lock().unwrap() = None;
            }
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
