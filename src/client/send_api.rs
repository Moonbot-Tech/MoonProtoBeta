use super::*;

impl Client {
    /// Public API: queue a command for sending through the owning client loop.
    ///
    /// The command is appended directly to the unbounded Delphi-style
    /// `DataToSend*` queue for its priority, separate from accepted UDP packets
    /// and receive-decoded delivery. This API has no local capacity-drop branch.
    ///
    /// E-V2-06: возвращает `()`, **но** при закрытом канале (main loop завершён)
    /// логирует error через `log` crate. Потерянная команда — серьёзный сигнал,
    /// но возвращать Result сломало бы API всех Client wrappers (`client.new_order(...)`
    /// и т.д.). Если потребителю нужен гарантированный feedback — он может
    /// проверить статус через `LifecycleEvent::Disconnected` callback и не
    /// шарашить новые команды после.
    ///
    /// **QUEUE BEHAVIOR:** internal send queues are unbounded. This matches
    /// Delphi `MoonProtoCommon.pas:765 SendCmdInt`: user commands are appended
    /// to protocol queues without a fixed capacity cap. `send_cmd` does not
    /// block on local queue fullness and never silently drops a trading/API
    /// command because the Rust main loop is busy. If the client is gone, the
    /// command is rejected and the error is logged.
    pub fn send_cmd(
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

    /// Public API: queue a command with an explicit Delphi UKey dedup key.
    ///
    /// Use this only for advanced tools that already know the correct UKey
    /// semantics. Regular applications should use typed `Client` wrappers or
    /// [`ClientSender`], which choose the correct key, priority, encryption, and
    /// retry count.
    pub fn send_cmd_keyed(
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
        // Delphi `SendCmdInt`: append into DataToSend/DataToSendH/DataToSendL
        // under SendLock. The writer tick later copies those lists; raw sends do
        // not wait behind reader delivery.
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
        if !self.app_queue_alive.load(Ordering::Relaxed) {
            return Err(SubscribeError::Disconnected);
        }
        if !self.domain_ready && !outgoing_allowed_before_domain_ready(item.cmd, &item.data) {
            return Err(SubscribeError::DomainNotReady);
        }
        self.send_lock.lock().unwrap().push_send_cmd_int(item);
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
            .unwrap()
            .send_queues
            .take_into(&mut sliced, &mut high, &mut low);
        (sliced, high, low)
    }

    #[cfg(test)]
    pub(crate) fn with_subscription_registry<R>(
        &self,
        f: impl FnOnce(&SubscriptionRegistry) -> R,
    ) -> R {
        let registry = self.subscription_registry.lock().unwrap();
        f(&registry)
    }

    #[cfg(test)]
    pub(crate) fn with_subscription_registry_mut<R>(
        &self,
        f: impl FnOnce(&mut SubscriptionRegistry) -> R,
    ) -> R {
        let mut registry = self.subscription_registry.lock().unwrap();
        let result = f(&mut registry);
        self.refresh_subscription_summary(&registry);
        result
    }

    pub(crate) fn refresh_subscription_summary(&self, registry: &SubscriptionRegistry) {
        refresh_subscription_summary(
            &self.subscription_summary,
            &self.subscription_trades_scope,
            registry,
        );
    }
}
