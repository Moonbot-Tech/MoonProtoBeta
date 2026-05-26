use super::*;

impl Client {
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
}
