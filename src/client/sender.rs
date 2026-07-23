//! Thread-safe typed send/subscription handle.
#![allow(dead_code)]

use super::*;

mod balance;
mod strat;
mod subscriptions;
mod ui;

/// Error returned by fallible [`ClientSender`] queueing methods.
///
/// Send/control queues are intentionally unbounded: typed application commands
/// do not fail because of a local queue capacity cap. Queueing can still be
/// rejected if the owning `Client` is gone, or if the caller tries to bypass the
/// `InitDone`/domain gate before the one-time init sequence completes.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubscribeError {
    /// The owning `Client` was dropped or the main loop exited, so this sender
    /// can no longer enqueue work.
    Disconnected,
    /// Domain gate is still closed. Only the mandatory init Engine API methods
    /// (`BaseCheck`, `AuthCheck`, `GetMarketsList`, `UpdateMarketsList`) are
    /// allowed before Init.
    DomainNotReady,
}

impl std::fmt::Display for SubscribeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disconnected => write!(f, "Client queues disconnected"),
            Self::DomainNotReady => write!(f, "Client domain gate is not ready"),
        }
    }
}

impl std::error::Error for SubscribeError {}

/// Thread-safe handle for UI and worker threads.
///
/// `MoonClient` is the normal application handle. This sender is kept for
/// internal protocol tests and legacy diagnostic tools that already own a
/// running `Client`.
/// Subscription helpers update the
/// active-library registry. Raw command helpers append already-serialized
/// command payloads directly into the protocol send queues used by `Client`
/// wrappers. The sender also mirrors fire-and-forget trade, UI, strategy, and
/// balance wrappers so terminal UI code can send typed actions without
/// rebuilding wire priorities, retry counts, or UKey values by hand.
///
/// Fire-and-forget methods log if the client is gone. `try_*` methods return
/// [`SubscribeError`] when the caller needs explicit feedback.
#[doc(hidden)]
#[derive(Clone)]
pub(crate) struct ClientSender {
    pub(crate) shared: Arc<ClientSenderShared>,
    pub(crate) start: Instant,
}

pub(crate) struct ClientSenderShared {
    pub(crate) app_queue_alive: Arc<AtomicBool>,
    pub(crate) domain_ready: Arc<AtomicBool>,
    pub(crate) send_lock: Arc<Mutex<SendLockState>>,
    pub(crate) subscription_registry: Arc<Mutex<SubscriptionRegistry>>,
    pub(crate) subscription_summary: Arc<SubscriptionRegistrySummary>,
    pub(crate) subscription_trades_scope:
        Arc<parking_lot::RwLock<Option<Arc<crate::state::TradeStorageScope>>>>,
    pub(crate) server_update_sent: Arc<AtomicBool>,
    pub(crate) last_trades_subscribe_request_ms: Arc<AtomicI64>,
    pub(crate) last_orderbook_subscribe_request_ms: Arc<AtomicI64>,
    pub(crate) last_orderbook_subscribe_request_uid: Arc<AtomicU64>,
    pub(crate) last_candle_subscribe_request_ms: Arc<AtomicI64>,
    pub(crate) pending_candle_subscribes: Arc<Mutex<super::subscriptions::PendingCandleSubscribes>>,
}

impl ClientSenderShared {
    fn refresh_subscription_summary(&self, registry: &SubscriptionRegistry) {
        refresh_subscription_summary(
            &self.subscription_summary,
            &self.subscription_trades_scope,
            registry,
        );
    }
}

impl ClientSender {
    #[inline]
    fn domain_ready_for_typed_send(&self) -> bool {
        self.shared.app_queue_alive.load(Ordering::Relaxed)
            && self.shared.domain_ready.load(Ordering::Relaxed)
    }

    /// Queue an already-serialized command payload for sending.
    ///
    /// This is a raw diagnostics/test edge. It does not consult the command
    /// registry and therefore requires the caller to pass the exact wire
    /// priority, encryption flag, retry count, and UKey. Normal code should use
    /// typed `MoonClient`/`ClientSender` actions instead.
    pub(crate) fn send_cmd(
        &self,
        data: Vec<u8>,
        cmd: Command,
        priority: SendPriority,
        encrypted: bool,
        max_retries: i32,
    ) {
        if let Err(e) = self.try_send_cmd(data, cmd, priority, encrypted, max_retries) {
            log::warn!(target: "moonproto::client",
                "ClientSender::send_cmd({cmd:?}) dropped: {e}");
        }
    }

    /// Fallible variant of [`Self::send_cmd`].
    pub(crate) fn try_send_cmd(
        &self,
        data: Vec<u8>,
        cmd: Command,
        priority: SendPriority,
        encrypted: bool,
        max_retries: i32,
    ) -> Result<(), SubscribeError> {
        self.try_send_cmd_keyed(
            data,
            cmd,
            priority,
            encrypted,
            max_retries,
            UniqueKey::none(),
        )
    }

    /// Queue an already-serialized command payload with an explicit UKey.
    ///
    /// This is the thread-safe raw counterpart of [`Client::send_cmd_keyed`].
    pub(crate) fn send_cmd_keyed(
        &self,
        data: Vec<u8>,
        cmd: Command,
        priority: SendPriority,
        encrypted: bool,
        max_retries: i32,
        u_key: UniqueKey,
    ) {
        if let Err(e) = self.try_send_cmd_keyed(data, cmd, priority, encrypted, max_retries, u_key)
        {
            log::warn!(target: "moonproto::client",
                "ClientSender::send_cmd_keyed({cmd:?}) dropped: {e}");
        }
    }

    /// Fallible variant of [`Self::send_cmd_keyed`].
    pub(crate) fn try_send_cmd_keyed(
        &self,
        data: Vec<u8>,
        cmd: Command,
        priority: SendPriority,
        encrypted: bool,
        max_retries: i32,
        u_key: UniqueKey,
    ) -> Result<(), SubscribeError> {
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
        self.try_enqueue_send_item(item)
    }

    /// Queue a fire-and-forget Engine API request from another thread.
    ///
    /// The payload must be a complete `TEngineRequest` body, for example from
    /// [`crate::commands::engine_request`]. This method does not register a
    /// pending response receiver; responses will surface as ordinary
    /// `Event::EngineResponse` values in the running dispatcher.
    pub(crate) fn send_api_request(&self, request_payload: Vec<u8>) {
        if let Err(e) = self.try_send_api_request(request_payload) {
            log::warn!(target: "moonproto::client",
                "ClientSender::send_api_request dropped: {e}");
        }
    }

    /// Fallible variant of [`Self::send_api_request`].
    pub(crate) fn try_send_api_request(
        &self,
        request_payload: Vec<u8>,
    ) -> Result<(), SubscribeError> {
        let method = engine_request_method(&request_payload);
        let request_uid = engine_request_uid(&request_payload);
        let result = self.try_send_typed_domain_cmd(request_payload, Command::API);
        if result.is_ok() {
            let now_ms = self.start.elapsed().as_millis() as i64;
            match method {
                Some(EngineMethod::SubscribeAllTrades) => {
                    self.shared
                        .last_trades_subscribe_request_ms
                        .store(now_ms, Ordering::Relaxed);
                }
                Some(EngineMethod::SubscribeOrderBook) => {
                    self.shared
                        .last_orderbook_subscribe_request_ms
                        .store(now_ms, Ordering::Relaxed);
                    self.shared.last_orderbook_subscribe_request_uid.store(
                        request_uid.unwrap_or(NO_PENDING_ENGINE_REQUEST_UID),
                        Ordering::Relaxed,
                    );
                }
                Some(EngineMethod::SubscribeCandles) => {
                    self.shared
                        .last_candle_subscribe_request_ms
                        .store(now_ms, Ordering::Relaxed);
                    if let Some(request_uid) = request_uid {
                        self.shared
                            .pending_candle_subscribes
                            .lock()
                            .insert(request_uid);
                    }
                }
                _ => {}
            }
        }
        result
    }

    fn send_typed_domain_cmd(&self, data: Vec<u8>, cmd: Command) -> bool {
        self.send_typed_domain_cmd_int(data, cmd, None).is_ok()
    }

    fn send_typed_domain_cmd_keyed(&self, data: Vec<u8>, cmd: Command, u_key: UniqueKey) -> bool {
        self.send_typed_domain_cmd_int(data, cmd, Some(u_key))
            .is_ok()
    }

    fn try_send_typed_domain_cmd(&self, data: Vec<u8>, cmd: Command) -> Result<(), SubscribeError> {
        self.send_typed_domain_cmd_int(data, cmd, None)
    }

    fn try_send_typed_domain_cmd_keyed(
        &self,
        data: Vec<u8>,
        cmd: Command,
        u_key: UniqueKey,
    ) -> Result<(), SubscribeError> {
        self.send_typed_domain_cmd_int(data, cmd, Some(u_key))
    }

    fn send_typed_domain_cmd_int(
        &self,
        data: Vec<u8>,
        cmd: Command,
        explicit_u_key: Option<UniqueKey>,
    ) -> Result<(), SubscribeError> {
        if !self.shared.app_queue_alive.load(Ordering::Relaxed) {
            return Err(SubscribeError::Disconnected);
        }
        if !self.shared.domain_ready.load(Ordering::Relaxed)
            && !outgoing_allowed_before_domain_ready(cmd.to_byte(), &data)
        {
            return Err(SubscribeError::DomainNotReady);
        }
        let Some(meta) = typed_send_metadata(cmd, &data, explicit_u_key) else {
            log::error!(target: "moonproto::client",
                "ClientSender::send_typed_domain_cmd: no descriptor/UKey for cmd={:?} payload_cmd_id={:?}",
                cmd,
                data.first().copied());
            return Err(SubscribeError::Disconnected);
        };
        self.try_send_cmd_keyed(
            data,
            cmd,
            meta.priority,
            meta.encrypted,
            meta.max_retries,
            meta.u_key,
        )
    }

    fn try_enqueue_send_item(&self, item: SendItem) -> Result<(), SubscribeError> {
        if !self.shared.app_queue_alive.load(Ordering::Relaxed) {
            return Err(SubscribeError::Disconnected);
        }
        if !self.shared.domain_ready.load(Ordering::Relaxed)
            && !outgoing_allowed_before_domain_ready(item.cmd, &item.data)
        {
            return Err(SubscribeError::DomainNotReady);
        }
        self.shared.send_lock.lock().push_send_cmd_int(item);
        Ok(())
    }
}
