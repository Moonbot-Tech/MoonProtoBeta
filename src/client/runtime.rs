use super::*;

impl Client {
    pub(crate) fn set_runtime_shutdown_flag(&mut self, flag: Arc<AtomicBool>) {
        self.lifecycle.runtime_shutdown = flag;
    }

    pub(crate) fn shutdown_requested(&self) -> bool {
        self.lifecycle.runtime_shutdown.load(Ordering::Relaxed)
    }

    pub(crate) fn start_inline_reader_session(&mut self) {
        self.transport.recv_slicer = slicing::SlicingReceiver::new();
        self.register_recv_poller();
    }

    pub(crate) fn clear_recv_poller(&mut self) {
        if let (Some(poller), Some(sock)) = (
            self.transport.recv_poller.as_ref(),
            self.transport.socket.as_ref(),
        ) {
            if let Err(e) = poller.delete(sock) {
                log::warn!(target: "moonproto::reader", "UDP poller delete failed: {e}");
            }
        }
        self.transport.recv_poller = None;
        self.transport.recv_events.clear();
    }

    pub(crate) fn register_recv_poller(&mut self) {
        self.clear_recv_poller();
        let Some(sock) = self.transport.socket.as_ref() else {
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
        self.transport.recv_poller = Some(poller);
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
        if let Some(addr) = self.transport.cached_server_addr {
            return Some(addr);
        }
        let key = format!("{}:{}", self.cfg.server_ip, self.cfg.server_port);
        match key.to_socket_addrs() {
            Ok(mut iter) => {
                if let Some(addr) = iter.next() {
                    self.transport.cached_server_addr = Some(addr);
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

    /// Send LogOff and close socket. Call when done.
    /// Matches TMoonProtoBaseClient.Disconnect (Common.pas:290-298)
    pub(crate) fn disconnect(&mut self) {
        self.lifecycle
            .runtime_shutdown
            .store(true, Ordering::Relaxed);
        self.need_connect = false;
        self.force_disconnect = true;
        self.authorized = false;
        self.auth_status = AuthStatus::Base;
        self.next_primary_hello_new_session = false;
        self.clear_hello_wait_state();
        self.set_domain_ready(false);
    }
}
