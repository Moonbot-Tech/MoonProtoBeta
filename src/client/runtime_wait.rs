use super::*;

impl Client {
    /// Wait for a `Receiver<T>` while the owned runtime keeps stepping.
    ///
    /// Internal init/test helpers can register `mpsc::Receiver<T>` slots whose
    /// responses are delivered only while the client loop is running. Calling
    /// `rx.recv_timeout(...)` directly on the same thread that owns the `Client`
    /// would stop UDP processing.
    ///
    /// This helper runs the step-based owned runtime until a value
    /// arrives, the channel disconnects, or the overall timeout expires. Events
    /// produced while the helper waits are
    /// stored in
    /// [`EventDispatcher::queued_events`](crate::events::EventDispatcher::queued_events)
    /// and can be drained through
    /// [`EventDispatcher::take_queued_events`](crate::events::EventDispatcher::take_queued_events).
    /// It works with any receiver: Engine API responses, the candle aggregator,
    /// or internal registry slots. Regular applications use `MoonClient`, which
    /// owns the runtime thread and routes request completions into
    /// events/snapshots.
    #[cfg(test)]
    pub(crate) fn wait_for_receiver_in_owned_runtime<T>(
        &mut self,
        dispatcher: &mut crate::events::EventDispatcher,
        rx: &mpsc::Receiver<T>,
        timeout: Duration,
    ) -> Result<T, mpsc::RecvTimeoutError> {
        let start = Instant::now();
        self.with_owned_runtime_stepper(dispatcher, |client, stepper, dispatcher| loop {
            if client.shutdown_requested() {
                client.disconnect();
                return Err(mpsc::RecvTimeoutError::Disconnected);
            }
            match rx.try_recv() {
                Ok(resp) => {
                    stepper.barrier();
                    return Ok(resp);
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err(mpsc::RecvTimeoutError::Disconnected);
                }
                Err(mpsc::TryRecvError::Empty) => {}
            }
            if timeout_remaining(start, timeout).is_none() {
                return Err(mpsc::RecvTimeoutError::Timeout);
            }
            if !stepper.step(client, dispatcher) {
                return Err(mpsc::RecvTimeoutError::Disconnected);
            }
        })
    }
}
