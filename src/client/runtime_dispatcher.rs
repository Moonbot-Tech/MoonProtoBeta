#![cfg(test)]

use super::*;

pub(crate) struct OwnedRuntimeStepper {
    event_buf: Vec<crate::events::Event>,
    payload_buf: Vec<(Command, Vec<u8>)>,
    active_actions_buf: Vec<crate::events::ActiveAction>,
}

impl OwnedRuntimeStepper {
    pub(crate) fn step(
        &mut self,
        client: &mut Client,
        dispatcher: &mut crate::events::EventDispatcher,
    ) -> bool {
        let mut mode = RunMode::Dispatcher {
            dispatcher,
            on_event: DispatcherEventFn::Queue,
            event_buf: std::mem::take(&mut self.event_buf),
            payload_buf: std::mem::take(&mut self.payload_buf),
            active_actions_buf: std::mem::take(&mut self.active_actions_buf),
        };
        let keep_running = (ProtocolCore { client }).run_step(&mut mode);
        let RunMode::Dispatcher {
            event_buf,
            payload_buf,
            active_actions_buf,
            ..
        } = mode
        else {
            unreachable!("dispatcher pump must use RunMode::Dispatcher");
        };
        self.event_buf = event_buf;
        self.payload_buf = payload_buf;
        self.active_actions_buf = active_actions_buf;
        keep_running
    }

    pub(crate) fn step_for(
        &mut self,
        client: &mut Client,
        dispatcher: &mut crate::events::EventDispatcher,
        duration: Duration,
    ) -> bool {
        let deadline = Instant::now() + duration;
        while Instant::now() < deadline {
            if !self.step(client, dispatcher) {
                return false;
            }
        }
        true
    }

    pub(crate) fn barrier(&self) {
        // Inline owned-state code has no worker barrier to wait for.
    }
}

impl Client {
    pub(crate) fn with_owned_runtime_stepper<R>(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        f: impl FnOnce(&mut Self, &mut OwnedRuntimeStepper, &mut crate::events::EventDispatcher) -> R,
    ) -> R {
        let lifecycle_pair = if self.lifecycle_event_sender_installed() {
            None
        } else {
            self.lifecycle.lifecycle_cb.take().map(|cb| {
                let (tx, rx) = mpsc::channel::<LifecycleEvent>();
                *self.lifecycle.lifecycle_app_tx.lock() = Some(tx);
                (rx, cb)
            })
        };
        let clear_lifecycle_app_tx = lifecycle_pair.is_some();
        let lifecycle_app_tx = Arc::clone(&self.lifecycle.lifecycle_app_tx);
        let mut restored_lifecycle_cb: Option<LifecycleFn> = None;
        let mut result = None;
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
            {
                let mut stepper = OwnedRuntimeStepper {
                    event_buf: Vec::with_capacity(8),
                    payload_buf: Vec::with_capacity(4),
                    active_actions_buf: Vec::with_capacity(4),
                };
                result = Some(f(self, &mut stepper, dispatcher));
                drop(stepper);
            }
            if clear_lifecycle_app_tx {
                *lifecycle_app_tx.lock() = None;
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
            self.lifecycle.lifecycle_cb = restored_lifecycle_cb;
        }
        result.expect("dispatcher pump closure must run")
    }

    #[cfg(test)]
    pub(crate) fn run_dispatcher_steps_for_test(
        &mut self,
        steps: usize,
        dispatcher: &mut crate::events::EventDispatcher,
    ) {
        let lifecycle_pair = if self.lifecycle_event_sender_installed() {
            None
        } else {
            self.lifecycle.lifecycle_cb.take().map(|cb| {
                let (tx, rx) = mpsc::channel::<LifecycleEvent>();
                *self.lifecycle.lifecycle_app_tx.lock() = Some(tx);
                (rx, cb)
            })
        };
        let clear_lifecycle_app_tx = lifecycle_pair.is_some();
        let lifecycle_app_tx = Arc::clone(&self.lifecycle.lifecycle_app_tx);
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
            let mut stepper = OwnedRuntimeStepper {
                event_buf: Vec::with_capacity(8),
                payload_buf: Vec::with_capacity(4),
                active_actions_buf: Vec::with_capacity(4),
            };
            for _ in 0..steps {
                if !stepper.step(self, dispatcher) {
                    break;
                }
            }
            if clear_lifecycle_app_tx {
                *lifecycle_app_tx.lock() = None;
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
            self.lifecycle.lifecycle_cb = restored_lifecycle_cb;
        }
    }
}
