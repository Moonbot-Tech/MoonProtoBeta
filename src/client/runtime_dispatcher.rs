use super::*;

impl Client {
    /// Low-level finite active-library pump for tests and custom runtimes.
    ///
    /// Regular applications should use [`MoonClient`](crate::MoonClient): it
    /// owns the runtime thread, publishes events/snapshots, and has no
    /// user-selected protocol-loop duration. Unlike [`Self::run`], this method
    /// routes incoming payloads through
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
    /// This is intentionally hidden from generated public docs so it does not
    /// look like the normal desktop/UI application model.
    #[doc(hidden)]
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
            if clear_lifecycle_app_tx {
                *lifecycle_app_tx.lock().unwrap() = None;
            }
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

    /// Low-level finite active-library pump whose callback also receives an
    /// updated read-only [`crate::events::EventDispatcherSnapshot`].
    ///
    /// This is useful for UI events that carry only an id, such as
    /// `OrderEvent::Updated(uid)`: the callback can immediately read the
    /// current order from the state snapshot. The callback runs from the
    /// application callback queue and does not block protocol ACK/retry/send
    /// progress.
    #[doc(hidden)]
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
            if clear_lifecycle_app_tx {
                *lifecycle_app_tx.lock().unwrap() = None;
            }
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
            if clear_lifecycle_app_tx {
                *lifecycle_app_tx.lock().unwrap() = None;
            }
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

    /// Test-only inline dispatcher oracle. Production active-library paths use
    /// `DispatcherWorker`; this remains only for focused unit tests that need a
    /// synchronous dispatcher without spawning worker/app queues.
    #[cfg(test)]
    fn run_inner(&mut self, duration: Duration, mut mode: RunMode<'_>) {
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
            ProtocolCore { client: self }.run(duration, &mut mode);
            if clear_lifecycle_app_tx {
                *lifecycle_app_tx.lock().unwrap() = None;
            }
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
}
