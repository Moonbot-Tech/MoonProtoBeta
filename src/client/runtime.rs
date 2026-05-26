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

    // ====================================================================
    //  Init helper УБРАН: дизайн `run_init_sequence` конфликтовал с
    //  `&mut Client` который держит `run()` — метод не мог быть вызван из
    //  обычного flow. Init шаги выполняются напрямую: вызови `subscribe_*` /
    //  `api_*` ДО `client.run_with_dispatcher` (методы требуют `&mut self` —
    //  это безопасно пока main loop не запущен), либо после `Connected{fresh}`
    //  через тот же `&mut Client` если используется single-thread runner.
    // ====================================================================

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

    /// Active-library entry point: run the client with an integrated
    /// `EventDispatcher`.
    ///
    /// Unlike [`Self::run`], this method routes incoming payloads through
    /// `dispatcher.dispatch_into_active` and performs active-library work:
    ///   - orderbook corrupted-cache recovery sends `RequestOrderBookFull`
    ///     without surfacing a separate callback event;
    ///   - trades gap recovery checks after valid trades packets and sends
    ///     `TradesResend` batches;
    ///   - market-index gating and per-client server-time delta are applied by
    ///     the dispatcher.
    ///
    /// The callback is informational: the dispatcher has already parsed the
    /// event and updated the read model.
    ///
    /// Basic pattern:
    /// ```ignore
    /// let mut client = Client::new(cfg);
    /// let mut dispatcher = EventDispatcher::new();
    /// client.run_with_dispatcher(
    ///     Duration::from_secs(3600),
    ///     &mut dispatcher,
    ///     Box::new(|ev| match ev {
    ///         Event::Order(o) => /* update UI */,
    ///         Event::EngineResponse(r) if !r.success => /* show error */,
    ///         _ => {}
    ///     })
    /// );
    /// ```
    pub fn run_with_dispatcher(
        &mut self,
        duration: Duration,
        dispatcher: &mut crate::events::EventDispatcher,
        on_event: EventFn,
    ) {
        // Protocol loop owns transport only. The active-library dispatcher is
        // processed by a worker thread: this mirrors Delphi `TThread.Queue`
        // boundaries for heavy domain work and keeps user callbacks away from
        // UDP receive / ACK / retry progress.
        let sender = self.sender();
        let protocol_metrics = Arc::clone(&self.protocol_metrics);
        let trades_server_token_mirror = Arc::clone(&self.dispatcher_trades_server_token);
        let api_pending = Arc::clone(&self.api_pending);
        let (app_tx, app_rx) = mpsc::channel::<crate::events::Event>();
        let (work_tx, work_rx) = mpsc::channel::<DispatcherWorkItem>();
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
                let mut on_event = on_event;
                while let Ok(event) = app_rx.recv() {
                    on_event(&event);
                }
            });
            let dispatcher_handle = scope.spawn(move || {
                run_dispatcher_worker(
                    work_rx,
                    dispatcher,
                    DispatcherEventFn::QueueToCallback(app_tx),
                    sender,
                    api_pending,
                    protocol_metrics,
                    trades_server_token_mirror,
                );
            });
            {
                let mut mode = RunMode::DispatcherWorker {
                    tx: work_tx,
                    payload_buf: Vec::with_capacity(4),
                };
                ProtocolCore { client: self }.run(duration, &mut mode);
            }
            *lifecycle_app_tx.lock().unwrap() = None;
            dispatcher_handle
                .join()
                .expect("moonproto dispatcher worker thread panicked");
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

    /// Same as [`Self::run_with_dispatcher`], but the callback also receives an
    /// updated read-only [`crate::events::EventDispatcherSnapshot`].
    ///
    /// This is useful for UI events that carry only an id, such as
    /// `OrderEvent::Updated(uid)`: the callback can immediately read the
    /// current order from the state snapshot. The callback runs from the
    /// application callback queue and does not block protocol ACK/retry/send
    /// progress.
    pub fn run_with_dispatcher_state(
        &mut self,
        duration: Duration,
        dispatcher: &mut crate::events::EventDispatcher,
        on_event: EventWithStateFn,
    ) {
        let sender = self.sender();
        let protocol_metrics = Arc::clone(&self.protocol_metrics);
        let trades_server_token_mirror = Arc::clone(&self.dispatcher_trades_server_token);
        let api_pending = Arc::clone(&self.api_pending);
        let (app_tx, app_rx) = mpsc::channel::<StateAppEvent>();
        let (work_tx, work_rx) = mpsc::channel::<DispatcherWorkItem>();
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
                let mut on_event = on_event;
                while let Ok((event, snapshot)) = app_rx.recv() {
                    on_event(&event, snapshot.as_ref());
                }
            });
            let dispatcher_handle = scope.spawn(move || {
                run_dispatcher_worker(
                    work_rx,
                    dispatcher,
                    DispatcherEventFn::QueueToStateCallback(app_tx),
                    sender,
                    api_pending,
                    protocol_metrics,
                    trades_server_token_mirror,
                );
            });
            {
                let mut mode = RunMode::DispatcherWorker {
                    tx: work_tx,
                    payload_buf: Vec::with_capacity(4),
                };
                ProtocolCore { client: self }.run(duration, &mut mode);
            }
            *lifecycle_app_tx.lock().unwrap() = None;
            dispatcher_handle
                .join()
                .expect("moonproto dispatcher worker thread panicked");
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

    #[cfg(test)]
    pub(crate) fn run_with_dispatcher_queued(
        &mut self,
        duration: Duration,
        dispatcher: &mut crate::events::EventDispatcher,
    ) {
        let mode = RunMode::Dispatcher {
            dispatcher,
            on_event: DispatcherEventFn::Queue,
            event_buf: Vec::with_capacity(8),
            payload_buf: Vec::with_capacity(4),
            active_actions_buf: Vec::with_capacity(4),
        };
        self.run_inner(duration, mode);
    }

    pub(crate) fn run_with_dispatcher_worker_queued(
        &mut self,
        duration: Duration,
        dispatcher: &mut crate::events::EventDispatcher,
    ) {
        let sender = self.sender();
        let protocol_metrics = Arc::clone(&self.protocol_metrics);
        let trades_server_token_mirror = Arc::clone(&self.dispatcher_trades_server_token);
        let api_pending = Arc::clone(&self.api_pending);
        let (work_tx, work_rx) = mpsc::channel::<DispatcherWorkItem>();
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
            let dispatcher_handle = scope.spawn(move || {
                run_dispatcher_worker(
                    work_rx,
                    dispatcher,
                    DispatcherEventFn::Queue,
                    sender,
                    api_pending,
                    protocol_metrics,
                    trades_server_token_mirror,
                );
            });
            {
                let mut mode = RunMode::DispatcherWorker {
                    tx: work_tx,
                    payload_buf: Vec::with_capacity(4),
                };
                ProtocolCore { client: self }.run(duration, &mut mode);
            }
            *lifecycle_app_tx.lock().unwrap() = None;
            dispatcher_handle
                .join()
                .expect("moonproto dispatcher worker thread panicked");
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

    /// Wait for a `Receiver<T>` while continuing to pump the UDP client loop.
    ///
    /// `Client::api_*` methods return `mpsc::Receiver<T>`, but the response is
    /// delivered only while the client loop is running. Calling
    /// `rx.recv_timeout(...)` directly on the same thread that owns the `Client`
    /// usually times out because UDP packets are not processed during that
    /// blocking wait.
    ///
    /// This helper runs short dispatcher ticks (10 ms, matching Delphi
    /// `SendAndWait` sleep) until a value arrives, the channel disconnects, or
    /// the overall timeout expires. Events produced while the helper waits are
    /// stored in
    /// [`EventDispatcher::queued_events`](crate::events::EventDispatcher::queued_events)
    /// and can be drained through
    /// [`EventDispatcher::take_queued_events`](crate::events::EventDispatcher::take_queued_events).
    /// It works with any receiver: Engine API responses, the candle aggregator,
    /// or custom registry slots.
    ///
    /// **Pattern**:
    /// ```ignore
    /// let rx = client.api_get_markets_list();
    /// let resp = client.run_until_response(&mut dispatcher, &rx, Duration::from_secs(12))?;
    /// ```
    pub fn run_until_response<T>(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        rx: &mpsc::Receiver<T>,
        timeout: Duration,
    ) -> Result<T, mpsc::RecvTimeoutError> {
        let start = Instant::now();
        let sender = self.sender();
        let protocol_metrics = Arc::clone(&self.protocol_metrics);
        let trades_server_token_mirror = Arc::clone(&self.dispatcher_trades_server_token);
        let api_pending = Arc::clone(&self.api_pending);
        let (work_tx, work_rx) = mpsc::channel::<DispatcherWorkItem>();
        let lifecycle_pair = self.lifecycle_cb.take().map(|cb| {
            let (tx, rx) = mpsc::channel::<LifecycleEvent>();
            *self.lifecycle_app_tx.lock().unwrap() = Some(tx);
            (rx, cb)
        });
        let lifecycle_app_tx = Arc::clone(&self.lifecycle_app_tx);
        let mut restored_lifecycle_cb: Option<LifecycleFn> = None;
        let mut result: Option<Result<T, mpsc::RecvTimeoutError>> = None;

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
            let dispatcher_handle = scope.spawn(move || {
                run_dispatcher_worker(
                    work_rx,
                    dispatcher,
                    DispatcherEventFn::Queue,
                    sender,
                    api_pending,
                    protocol_metrics,
                    trades_server_token_mirror,
                );
            });
            {
                let barrier_tx = work_tx.clone();
                let mut mode = RunMode::DispatcherWorker {
                    tx: work_tx,
                    payload_buf: Vec::with_capacity(4),
                };
                loop {
                    match rx.try_recv() {
                        Ok(resp) => {
                            wait_dispatcher_worker_barrier(&barrier_tx);
                            result = Some(Ok(resp));
                            break;
                        }
                        Err(mpsc::TryRecvError::Disconnected) => {
                            result = Some(Err(mpsc::RecvTimeoutError::Disconnected));
                            break;
                        }
                        Err(mpsc::TryRecvError::Empty) => {}
                    }
                    let Some(remaining) = timeout_remaining(start, timeout) else {
                        result = Some(Err(mpsc::RecvTimeoutError::Timeout));
                        break;
                    };
                    let tick = remaining.min(Duration::from_millis(DELPHI_SEND_AND_WAIT_POLL_MS));
                    ProtocolCore { client: self }.run(tick, &mut mode);
                }
            }
            *lifecycle_app_tx.lock().unwrap() = None;
            dispatcher_handle
                .join()
                .expect("moonproto dispatcher worker thread panicked");
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
        result.expect("run_until_response loop must always set result")
    }

    /// Test-only inline dispatcher oracle. Production active-library paths use
    /// `DispatcherWorker`; this remains only for focused unit tests that need a
    /// synchronous dispatcher without spawning worker/app queues.
    #[cfg(test)]
    fn run_inner(&mut self, duration: Duration, mut mode: RunMode<'_>) {
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
            ProtocolCore { client: self }.run(duration, &mut mode);
            *lifecycle_app_tx.lock().unwrap() = None;
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

    #[cfg(test)]
    pub(crate) fn apply_active_actions<I>(&self, actions: I)
    where
        I: IntoIterator<Item = crate::events::ActiveAction>,
    {
        if !self.domain_ready_for_typed_send() {
            return;
        }
        for action in actions {
            match action {
                crate::events::ActiveAction::RequestMarketsList => {
                    self.send_api_request(&crate::commands::engine_request::get_markets_list());
                }
                crate::events::ActiveAction::RequestUpdateMarketsList => {
                    self.send_api_request(&crate::commands::engine_request::update_markets_list());
                }
                crate::events::ActiveAction::RequestOrderSnapshot => {
                    self.request_all_statuses(rand::random());
                }
                crate::events::ActiveAction::RequestStrategySchema => {
                    self.strat_schema_request();
                }
                crate::events::ActiveAction::RequestOrderBookFull {
                    market_index,
                    book_kind,
                } => {
                    self.send_api_request(
                        &crate::commands::engine_request::request_order_book_full(
                            market_index,
                            book_kind,
                        ),
                    );
                }
                crate::events::ActiveAction::SendStrategySnapshot {
                    server_epoch,
                    client_max_last_date,
                    full,
                    data,
                } => {
                    self.strat_send_snapshot_payload(
                        server_epoch,
                        client_max_last_date,
                        full,
                        &data,
                    );
                }
                crate::events::ActiveAction::RequestOrderStatus { ctx, market_name } => {
                    self.request_order_status(ctx, &market_name);
                }
                crate::events::ActiveAction::OrderCancel { request } => {
                    self.send_order_cancel_request(request);
                }
                crate::events::ActiveAction::TradesResend { payload } => {
                    self.send_api_request(&payload);
                }
            }
        }
    }
}
