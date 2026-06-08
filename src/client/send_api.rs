use super::*;

impl Client {
    /// Diagnostics/test raw send edge for an already-serialized payload.
    ///
    /// Normal application code must use `MoonClient` intents or typed `Client`
    /// wrappers, which resolve priority, encryption, retries, and UKey through
    /// the command registry. This raw edge is compiled only for tests and
    /// diagnostics because the caller must already know the exact wire metadata.
    ///
    /// The command is appended directly to the unbounded protocol send queue
    /// for its priority, separate from accepted UDP packets and receive-decoded
    /// delivery. This helper has no local capacity-drop branch.
    ///
    /// E-V2-06: returns `()`, **but** when the channel is closed (main loop has finished)
    /// it logs an error through the `log` crate. A lost command is a serious signal,
    /// but returning a Result would break the API of all Client wrappers (`client.new_order(...)`
    /// etc.). If the consumer needs guaranteed feedback, it can
    /// check the status via the `LifecycleEvent::Disconnected` callback and stop
    /// firing new commands after that.
    ///
    /// **QUEUE BEHAVIOR:** internal send queues are unbounded. User commands
    /// are appended to protocol queues without a fixed capacity cap. `send_cmd`
    /// does not block on local queue fullness and never silently drops a
    /// trading/API command because the Rust main loop is busy. If the client is
    /// gone, the command is rejected and the error is logged.
    #[cfg(any(test, feature = "diagnostics"))]
    #[allow(dead_code)]
    pub(crate) fn send_cmd(
        &self,
        data: Vec<u8>,
        cmd: Command,
        priority: SendPriority,
        encrypted: bool,
        max_retries: i32,
    ) {
        self.send_cmd_keyed(
            data,
            cmd,
            priority,
            encrypted,
            max_retries,
            UniqueKey::none(),
        );
    }

    /// Queue a raw command with an explicit UKey dedup key.
    ///
    /// Use this only from typed wrappers, diagnostics, or protocol tests that
    /// already know the correct UKey semantics. Regular applications use
    /// `MoonClient` intents, which choose the correct key, priority, encryption,
    /// and retry count.
    pub(crate) fn send_cmd_keyed(
        &self,
        data: Vec<u8>,
        cmd: Command,
        priority: SendPriority,
        encrypted: bool,
        max_retries: i32,
        u_key: UniqueKey,
    ) {
        let item = SendItem {
            data,
            cmd: cmd.to_byte(),
            encrypted,
            priority,
            retry_left: initial_retry_left(encrypted, max_retries),
            max_retries,
            msg_num: 0,
            last_sent_at: 0,
            u_key,
        };
        // Append into the priority send queues under SendLock. The writer tick
        // later snapshots those queues; raw sends do not wait behind reader
        // delivery.
        if let Err(err) = self.enqueue_send_item(item) {
            match err {
                SubscribeError::Disconnected => {
                    log::error!(target: "moonproto::client",
                        "send_cmd: send queues closed (client dropped?) — packet cmd={:?} priority={:?} dropped",
                        cmd, priority);
                }
                SubscribeError::DomainNotReady => {
                    log::warn!(target: "moonproto::client",
                        "send_cmd: domain gate is closed before InitDone — packet cmd={:?} priority={:?} dropped",
                        cmd, priority);
                }
            }
        }
    }

    fn enqueue_send_item(&self, item: SendItem) -> Result<(), SubscribeError> {
        if !self.lifecycle.app_queue_alive.load(Ordering::Relaxed) {
            return Err(SubscribeError::Disconnected);
        }
        if !self.subscriptions.domain_ready
            && !outgoing_allowed_before_domain_ready(item.cmd, &item.data)
        {
            return Err(SubscribeError::DomainNotReady);
        }
        self.send_lock.lock().push_send_cmd_int(item);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn take_send_queues_for_test(
        &self,
    ) -> (Vec<SendItem>, Vec<SendItem>, Vec<SendItem>) {
        let mut sliced = Vec::new();
        let mut high = Vec::new();
        let mut low = Vec::new();
        self.send_lock
            .lock()
            .send_queues
            .take_into(&mut sliced, &mut high, &mut low);
        (sliced, high, low)
    }

    #[cfg(test)]
    pub(crate) fn with_subscription_registry<R>(
        &self,
        f: impl FnOnce(&SubscriptionRegistry) -> R,
    ) -> R {
        let registry = self.subscriptions.subscription_registry.lock();
        f(&registry)
    }

    #[cfg(test)]
    pub(crate) fn with_subscription_registry_mut<R>(
        &self,
        f: impl FnOnce(&mut SubscriptionRegistry) -> R,
    ) -> R {
        let mut registry = self.subscriptions.subscription_registry.lock();
        let result = f(&mut registry);
        self.refresh_subscription_summary(&registry);
        result
    }

    pub(crate) fn refresh_subscription_summary(&self, registry: &SubscriptionRegistry) {
        refresh_subscription_summary(
            &self.subscriptions.subscription_summary,
            &self.subscriptions.subscription_trades_scope,
            registry,
        );
    }
}
