//! Thread-safe typed send/subscription handle.

use super::*;

/// Error returned by fallible [`ClientSender`] queueing methods.
///
/// Send/control queues are intentionally unbounded to preserve the Delphi
/// no-local-cap behavior of `SendCmdInt`. Queueing can still be rejected if
/// the owning `Client` is gone, or if the caller tries to bypass the Delphi
/// `InitDone`/domain gate before the one-time init sequence completes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscribeError {
    /// The owning `Client` was dropped or the main loop exited, so this sender
    /// can no longer enqueue work.
    Disconnected,
    /// Domain gate is still closed. Only the mandatory init Engine API methods
    /// (`BaseCheck`, `AuthCheck`, `GetMarketsList`, `GetMarketsIndexes`,
    /// `UpdateMarketsList`) are allowed before Init.
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
/// Obtain it with [`Client::sender`], clone it freely, and send work while the
/// owning `Client` is running on another thread. Subscription helpers update the
/// active-library registry. Raw command helpers append already-serialized
/// command payloads directly into the Delphi-style send queues used by `Client`
/// wrappers. The sender also mirrors fire-and-forget trade, UI, strategy, and
/// balance wrappers so terminal UI code can send typed actions without
/// rebuilding wire priorities, retry counts, or UKey values by hand.
///
/// ```ignore
/// let mut client = Client::new(cfg);
/// let sender = client.sender();
/// // Move the sender into a UI thread:
/// thread::spawn(move || {
///     sender.subscribe_orderbook("DOGEUSDT");
/// });
/// // Main thread:
/// client.run_with_dispatcher(...);
/// ```
///
/// Fire-and-forget methods log if the client is gone. `try_*` methods return
/// [`SubscribeError`] when the caller needs explicit feedback.
#[derive(Clone)]
pub struct ClientSender {
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

    /// Subscribe to an orderbook stream and remember the intent for reconnect
    /// restore.
    pub fn subscribe_orderbook(&self, market_name: &str) {
        if let Err(e) = self.try_subscribe_orderbook(market_name) {
            log::warn!(target: "moonproto::client",
                "subscribe_orderbook({market_name}) dropped: {e}");
        }
    }

    /// Unsubscribe from an orderbook stream and update the reconnect registry.
    pub fn unsubscribe_orderbook(&self, market_name: &str) {
        if let Err(e) = self.try_unsubscribe_orderbook(market_name) {
            log::warn!(target: "moonproto::client",
                "unsubscribe_orderbook({market_name}) dropped: {e}");
        }
    }

    /// Subscribe to several orderbook streams and remember all intents for
    /// reconnect restore.
    ///
    /// This updates the shared reconnect registry immediately, deduplicates
    /// already remembered market names, and appends one batched
    /// `emk_SubscribeOrderBook` request for newly added markets.
    pub fn subscribe_orderbooks<I, S>(&self, market_names: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if let Err(e) = self.try_subscribe_orderbooks(market_names) {
            log::warn!(target: "moonproto::client",
                "subscribe_orderbooks dropped: {e}");
        }
    }

    /// Unsubscribe from several orderbook streams and update the reconnect
    /// registry.
    pub fn unsubscribe_orderbooks<I, S>(&self, market_names: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if let Err(e) = self.try_unsubscribe_orderbooks(market_names) {
            log::warn!(target: "moonproto::client",
                "unsubscribe_orderbooks dropped: {e}");
        }
    }

    /// Unsubscribe from all orderbook streams remembered by the registry.
    pub fn unsubscribe_all_orderbooks(&self) {
        if let Err(e) = self.try_unsubscribe_all_orderbooks() {
            log::warn!(target: "moonproto::client",
                "unsubscribe_all_orderbooks dropped: {e}");
        }
    }

    /// Subscribe to the all-trades stream and remember the intent for reconnect
    /// restore.
    pub fn subscribe_all_trades(&self, want_mm: bool) {
        if let Err(e) = self.try_subscribe_all_trades(want_mm) {
            log::warn!(target: "moonproto::client",
                "subscribe_all_trades(want_mm={want_mm}) dropped: {e}");
        }
    }

    /// Subscribe to the all-trades stream while retaining active-library
    /// history only for the selected markets.
    ///
    /// Empty `market_names` means all markets. The wire command is still
    /// Delphi-compatible `emk_SubscribeAllTrades`; the scope affects only
    /// Active Lib typed events, retained trades/candles, and derived analytics.
    pub fn subscribe_trades_for<I, S>(&self, want_mm: bool, market_names: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if let Err(e) = self.try_subscribe_trades_for(want_mm, market_names) {
            log::warn!(target: "moonproto::client",
                "subscribe_trades_for(want_mm={want_mm}) dropped: {e}");
        }
    }

    /// Unsubscribe from the all-trades stream and update the reconnect registry.
    pub fn unsubscribe_all_trades(&self) {
        if let Err(e) = self.try_unsubscribe_all_trades() {
            log::warn!(target: "moonproto::client",
                "unsubscribe_all_trades dropped: {e}");
        }
    }

    /// Fallible orderbook subscription.
    pub fn try_subscribe_orderbook(&self, market_name: &str) -> Result<(), SubscribeError> {
        if !self.shared.app_queue_alive.load(Ordering::Relaxed) {
            return Err(SubscribeError::Disconnected);
        }
        let market_name = market_name.to_string();
        let newly_added = {
            let mut registry = self.shared.subscription_registry.lock().unwrap();
            let newly_added = registry.orderbook_subs.insert(market_name.clone());
            self.shared.refresh_subscription_summary(&registry);
            newly_added
        };
        if newly_added && self.domain_ready_for_typed_send() {
            self.try_send_api_request(crate::commands::engine_request::subscribe_order_book(&[
                &market_name,
            ]))?;
        }
        Ok(())
    }

    /// Fallible orderbook unsubscribe.
    pub fn try_unsubscribe_orderbook(&self, market_name: &str) -> Result<(), SubscribeError> {
        if !self.shared.app_queue_alive.load(Ordering::Relaxed) {
            return Err(SubscribeError::Disconnected);
        }
        let market_name = market_name.to_string();
        let removed = {
            let mut registry = self.shared.subscription_registry.lock().unwrap();
            let removed = registry.orderbook_subs.remove(&market_name);
            self.shared.refresh_subscription_summary(&registry);
            removed
        };
        if removed && self.domain_ready_for_typed_send() {
            self.try_send_api_request(crate::commands::engine_request::unsubscribe_order_book(&[
                &market_name,
            ]))?;
        }
        Ok(())
    }

    /// Fallible batched orderbook subscription.
    pub fn try_subscribe_orderbooks<I, S>(&self, market_names: I) -> Result<(), SubscribeError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let market_names: Vec<String> = market_names
            .into_iter()
            .map(|name| name.as_ref().to_string())
            .collect();
        if market_names.is_empty() {
            return Ok(());
        }
        if !self.shared.app_queue_alive.load(Ordering::Relaxed) {
            return Err(SubscribeError::Disconnected);
        }
        let mut new_names = Vec::new();
        {
            let mut registry = self.shared.subscription_registry.lock().unwrap();
            for market_name in market_names {
                if registry.orderbook_subs.insert(market_name.clone()) {
                    new_names.push(market_name);
                }
            }
            self.shared.refresh_subscription_summary(&registry);
        }
        if !new_names.is_empty() && self.domain_ready_for_typed_send() {
            let refs: Vec<&str> = new_names.iter().map(String::as_str).collect();
            self.try_send_api_request(crate::commands::engine_request::subscribe_order_book(
                &refs,
            ))?;
        }
        Ok(())
    }

    /// Fallible batched orderbook unsubscribe.
    pub fn try_unsubscribe_orderbooks<I, S>(&self, market_names: I) -> Result<(), SubscribeError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let market_names: Vec<String> = market_names
            .into_iter()
            .map(|name| name.as_ref().to_string())
            .collect();
        if market_names.is_empty() {
            return Ok(());
        }
        if !self.shared.app_queue_alive.load(Ordering::Relaxed) {
            return Err(SubscribeError::Disconnected);
        }
        let mut removed_names = Vec::new();
        {
            let mut registry = self.shared.subscription_registry.lock().unwrap();
            for market_name in market_names {
                if registry.orderbook_subs.remove(&market_name) {
                    removed_names.push(market_name);
                }
            }
            self.shared.refresh_subscription_summary(&registry);
        }
        if !removed_names.is_empty() && self.domain_ready_for_typed_send() {
            let refs: Vec<&str> = removed_names.iter().map(String::as_str).collect();
            self.try_send_api_request(crate::commands::engine_request::unsubscribe_order_book(
                &refs,
            ))?;
        }
        Ok(())
    }

    /// Fallible all-orderbooks unsubscribe.
    pub fn try_unsubscribe_all_orderbooks(&self) -> Result<(), SubscribeError> {
        if !self.shared.app_queue_alive.load(Ordering::Relaxed) {
            return Err(SubscribeError::Disconnected);
        }
        let removed_names = {
            let mut registry = self.shared.subscription_registry.lock().unwrap();
            let removed_names = registry.orderbook_subs.drain().collect::<Vec<_>>();
            self.shared.refresh_subscription_summary(&registry);
            removed_names
        };
        if removed_names.is_empty() || !self.domain_ready_for_typed_send() {
            return Ok(());
        }
        let refs: Vec<&str> = removed_names.iter().map(String::as_str).collect();
        self.try_send_api_request(crate::commands::engine_request::unsubscribe_order_book(
            &refs,
        ))
    }

    /// Fallible all-trades subscription.
    pub fn try_subscribe_all_trades(&self, want_mm: bool) -> Result<(), SubscribeError> {
        self.try_subscribe_trades_with_scope(want_mm, crate::state::TradeStorageScope::All)
    }

    /// Fallible scoped all-trades subscription.
    pub fn try_subscribe_trades_for<I, S>(
        &self,
        want_mm: bool,
        market_names: I,
    ) -> Result<(), SubscribeError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.try_subscribe_trades_with_scope(
            want_mm,
            crate::state::TradeStorageScope::from_markets(market_names),
        )
    }

    fn try_subscribe_trades_with_scope(
        &self,
        want_mm: bool,
        storage_scope: crate::state::TradeStorageScope,
    ) -> Result<(), SubscribeError> {
        if !self.shared.app_queue_alive.load(Ordering::Relaxed) {
            return Err(SubscribeError::Disconnected);
        }
        let wire_changed = {
            let mut registry = self.shared.subscription_registry.lock().unwrap();
            let new_sub = Some(TradesSubscription { want_mm });
            let wire_changed =
                registry.trades_sub != new_sub || registry.mm_orders_sub != Some(want_mm);
            registry.trades_sub = Some(TradesSubscription { want_mm });
            registry.mm_orders_sub = Some(want_mm);
            registry.trades_storage_scope = storage_scope;
            self.shared.refresh_subscription_summary(&registry);
            wire_changed
        };
        if !wire_changed || !self.domain_ready_for_typed_send() {
            return Ok(());
        }
        self.try_send_api_request(crate::commands::engine_request::subscribe_all_trades(
            want_mm,
        ))
    }

    /// Fallible all-trades unsubscribe.
    pub fn try_unsubscribe_all_trades(&self) -> Result<(), SubscribeError> {
        if !self.shared.app_queue_alive.load(Ordering::Relaxed) {
            return Err(SubscribeError::Disconnected);
        }
        let had_subscription = {
            let mut registry = self.shared.subscription_registry.lock().unwrap();
            let had_subscription = registry.trades_sub.take().is_some();
            self.shared.refresh_subscription_summary(&registry);
            had_subscription
        };
        if had_subscription && self.domain_ready_for_typed_send() {
            self.try_send_api_request(crate::commands::engine_request::unsubscribe_all_trades())?;
        }
        Ok(())
    }

    /// Queue an already-serialized command payload for sending.
    ///
    /// This is the thread-safe counterpart of [`Client::send_cmd`]. It does not
    /// build protocol payloads for the caller; use typed builders in
    /// [`crate::commands`] or prefer high-level `Client` wrappers when the caller
    /// already owns the client thread.
    pub fn send_cmd(
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
    pub fn try_send_cmd(
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

    /// Queue an already-serialized command payload with a Delphi UKey dedup key.
    ///
    /// This is the thread-safe counterpart of [`Client::send_cmd_keyed`].
    pub fn send_cmd_keyed(
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
    pub fn try_send_cmd_keyed(
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
    pub fn send_api_request(&self, request_payload: Vec<u8>) {
        if let Err(e) = self.try_send_api_request(request_payload) {
            log::warn!(target: "moonproto::client",
                "ClientSender::send_api_request dropped: {e}");
        }
    }

    /// Fallible variant of [`Self::send_api_request`].
    pub fn try_send_api_request(&self, request_payload: Vec<u8>) -> Result<(), SubscribeError> {
        let method = engine_request_method(&request_payload);
        let request_uid = engine_request_uid(&request_payload);
        let result =
            self.try_send_cmd(request_payload, Command::API, SendPriority::Sliced, true, 6);
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
                _ => {}
            }
        }
        result
    }

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
                    self.send_api_request(crate::commands::engine_request::get_markets_list());
                }
                crate::events::ActiveAction::RequestUpdateMarketsList => {
                    self.send_api_request(crate::commands::engine_request::update_markets_list());
                }
                crate::events::ActiveAction::RequestStrategySchema => {
                    self.strat_schema_request();
                }
                crate::events::ActiveAction::RequestOrderBookFull {
                    market_index,
                    book_kind,
                } => {
                    self.send_api_request(
                        crate::commands::engine_request::request_order_book_full(
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
                    self.send_api_request(payload);
                }
            }
        }
    }

    fn send_domain_cmd(
        &self,
        data: Vec<u8>,
        cmd: Command,
        priority: SendPriority,
        encrypted: bool,
        max_retries: i32,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        self.send_cmd(data, cmd, priority, encrypted, max_retries);
        true
    }

    fn send_domain_cmd_keyed(
        &self,
        data: Vec<u8>,
        cmd: Command,
        priority: SendPriority,
        encrypted: bool,
        max_retries: i32,
        u_key: UniqueKey,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        self.send_cmd_keyed(data, cmd, priority, encrypted, max_retries, u_key);
        true
    }

    fn try_send_domain_cmd_keyed(
        &self,
        data: Vec<u8>,
        cmd: Command,
        priority: SendPriority,
        encrypted: bool,
        max_retries: i32,
        u_key: UniqueKey,
    ) -> Result<(), SubscribeError> {
        if !self.domain_ready_for_typed_send() {
            return Ok(());
        }
        self.try_send_cmd_keyed(data, cmd, priority, encrypted, max_retries, u_key)
    }

    fn send_trade(&self, payload: Vec<u8>, max_retries: i32) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        self.send_cmd(
            payload,
            Command::Order,
            SendPriority::High,
            true,
            max_retries,
        );
        true
    }

    fn send_trade_keyed(&self, payload: Vec<u8>, max_retries: i32, u_key: UniqueKey) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        self.send_cmd_keyed(
            payload,
            Command::Order,
            SendPriority::High,
            true,
            max_retries,
            u_key,
        );
        true
    }

    fn send_order_cancel_request(&self, request: crate::state::orders::OrderCancelSend) {
        match request {
            crate::state::orders::OrderCancelSend::PendingReplaceThenCancel {
                ctx,
                market,
                price,
            } => {
                let replace = crate::commands::trade::build_order_replace(
                    ctx,
                    &market,
                    crate::commands::trade::OrderType::Buy,
                    price,
                );
                self.send_trade_keyed(replace, 3, UniqueKey::order_move(ctx.uid));
                let cancel = crate::commands::trade::build_order_cancel(
                    ctx,
                    &market,
                    0,
                    crate::commands::trade::OrderWorkerStatus::None,
                );
                self.send_trade_keyed(cancel, 3, UniqueKey::order_move(ctx.uid));
            }
            crate::state::orders::OrderCancelSend::Cancel {
                ctx,
                market,
                status,
            } => {
                let raw = crate::commands::trade::build_order_cancel(ctx, &market, 0, status);
                self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
            }
        }
    }

    fn send_panic_sell_request(&self, request: crate::state::orders::PanicSellSend) {
        let raw = crate::commands::trade::build_turn_panic_sell(
            request.ctx,
            &request.market,
            request.turn_on,
        );
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(request.ctx.uid));
    }

    /// Send `TNewOrderCommand` from a thread-safe sender.
    ///
    /// This mirrors [`Client::new_order`]: `MPC_Order`, high priority,
    /// encrypted, `MaxRetries=3`.
    pub fn new_order(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        is_short: bool,
        price: f64,
        strat_id: u64,
        order_size: f64,
    ) {
        let raw = crate::commands::trade::build_new_order(
            ctx, market, is_short, price, strat_id, order_size,
        );
        self.send_trade(raw, 3);
    }

    #[inline]
    fn now_ms(&self) -> i64 {
        self.start.elapsed().as_millis() as i64
    }

    /// Apply Delphi replace request locally and send `TOrderReplaceCommand`.
    pub fn replace_order(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        new_price: f64,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let Some((ctx, market, order_type, price)) =
            orders.send_replace_if_requested(uid, new_price, self.now_ms())
        else {
            return false;
        };
        let raw = crate::commands::trade::build_order_replace(ctx, &market, order_type, price);
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
        true
    }

    /// Replace an order already tracked by `EventDispatcher::orders()`.
    pub fn replace_tracked_order(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        new_price: f64,
    ) -> bool {
        self.replace_order(orders, uid, new_price)
    }

    /// Send low-level `TAllStatusesReq`.
    ///
    /// This is fire-and-forget. Use [`Client::request_order_snapshot`] when the
    /// caller owns the `Client` and wants to wait for the applied snapshot.
    pub fn request_all_statuses(&self, uid: u64) {
        let raw = crate::commands::trade::build_all_statuses_request(uid);
        self.send_trade(raw, 3);
    }

    /// Apply Delphi cancel request locally and send `TOrderCancelCommand`.
    pub fn cancel_order(&self, orders: &mut crate::state::Orders, uid: u64) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let Some(request) = orders.send_cancel_if_requested(uid, self.now_ms()) else {
            return false;
        };
        self.send_order_cancel_request(request);
        true
    }

    /// Cancel an order already tracked by `EventDispatcher::orders()`.
    pub fn cancel_tracked_order(&self, orders: &mut crate::state::Orders, uid: u64) -> bool {
        self.cancel_order(orders, uid)
    }

    /// Send `TJoinOrdersCommand`.
    pub fn join_orders(&self, ctx: crate::commands::trade::TradeCtx, market: &str, is_short: bool) {
        let raw = crate::commands::trade::build_join_orders(ctx, market, is_short);
        self.send_trade(raw, 3);
    }

    /// Send `TSplitOrderCommand`.
    pub fn split_order(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        split_parts: i32,
        split_small: bool,
        split_small_sell: bool,
    ) {
        let raw = crate::commands::trade::build_split_order(
            ctx,
            market,
            split_parts,
            split_small,
            split_small_sell,
        );
        self.send_trade(raw, 3);
    }

    /// Split an order already tracked by `EventDispatcher::orders()`.
    pub fn split_tracked_order(
        &self,
        order: &crate::state::Order,
        split_parts: i32,
        split_small: bool,
        split_small_sell: bool,
    ) {
        self.split_order(
            order.trade_ctx(),
            &order.market_name,
            split_parts,
            split_small,
            split_small_sell,
        );
    }

    /// Send `TMoveAllSellsCommand` if Delphi active-client gate finds a candidate order.
    pub fn move_all_sells(
        &self,
        orders: &crate::state::Orders,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        params: crate::commands::trade::MoveAllSellsParams,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        if !orders.has_move_all_sells_candidate(market, params) {
            return false;
        }
        let raw = crate::commands::trade::build_move_all_sells(ctx, market, params);
        self.send_trade(raw, 3);
        true
    }

    /// Send `TDoClosePositionCommand` (`MaxRetries=1`).
    pub fn do_close_position(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        market_sell: bool,
    ) {
        let raw = crate::commands::trade::build_do_close_position(ctx, market, market_sell);
        self.send_trade(raw, 1);
    }

    /// Send `TDoLimitClosePositionCommand` (`MaxRetries=1`).
    pub fn do_limit_close_position(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        is_short: bool,
    ) {
        let raw = crate::commands::trade::build_do_limit_close_position(ctx, market, is_short);
        self.send_trade(raw, 1);
    }

    /// Send `TDoSplitPositionCommand` (`MaxRetries=1`).
    pub fn do_split_position(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        is_short: bool,
    ) {
        let raw = crate::commands::trade::build_do_split_position(ctx, market, is_short);
        self.send_trade(raw, 1);
    }

    /// Send `TDoSellOrderCommand` (`MaxRetries=1`).
    pub fn do_sell_order(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        price: f64,
        size: f64,
    ) {
        let raw = crate::commands::trade::build_do_sell_order(ctx, market, price, size);
        self.send_trade(raw, 1);
    }

    /// Send `TOrderStatusRequest`.
    pub fn request_order_status(&self, ctx: crate::commands::trade::TradeCtx, market: &str) {
        let raw = crate::commands::trade::build_order_status_request(ctx, market);
        self.send_trade(raw, 3);
    }

    /// Request a fresh status for an order already tracked by
    /// `EventDispatcher::orders()`.
    pub fn request_tracked_order_status(&self, order: &crate::state::Order) {
        self.request_order_status(order.trade_ctx(), &order.market_name);
    }

    /// Apply Delphi `SendStopsIfChanged` locally and send `TOrderStopsUpdate`.
    pub fn update_order_stops(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        stops: &crate::commands::trade::StopSettings,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let Some((ctx, market, status, stops)) = orders.send_stops_if_changed(uid, stops) else {
            return false;
        };
        let raw = crate::commands::trade::build_order_stops_update(ctx, &market, 0, status, &stops);
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
        true
    }

    /// Update stops for an order already tracked by `EventDispatcher::orders()`.
    pub fn update_tracked_order_stops(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        stops: &crate::commands::trade::StopSettings,
    ) -> bool {
        self.update_order_stops(orders, uid, stops)
    }

    /// Delphi `TOrdersWorkers.TurnPanicSell`: set panic sell for every local
    /// active sell order in `market_name`.
    pub fn turn_panic_sell(
        &self,
        orders: &mut crate::state::Orders,
        market_name: &str,
        turn_on: bool,
    ) -> usize {
        if !self.domain_ready_for_typed_send() {
            return 0;
        }
        let requests = orders.turn_panic_sell_by_market(market_name, turn_on);
        let queued = requests.len();
        for request in requests {
            self.send_panic_sell_request(request);
        }
        queued
    }

    /// Delphi `TOrdersWorkers.SwitchPanicSellByMarket` button semantics.
    pub fn switch_panic_sell_by_market(
        &self,
        orders: &mut crate::state::Orders,
        market_name: &str,
        turn_on: bool,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let (panic_sell_on, requests) = orders.switch_panic_sell_by_market(market_name, turn_on);
        for request in requests {
            self.send_panic_sell_request(request);
        }
        panic_sell_on
    }

    /// Apply Delphi per-worker panic-sell flag and send `TTurnPanicSellCommand`.
    pub fn turn_order_panic_sell(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        turn_on: bool,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let Some(request) = orders.send_panic_sell_if_changed(uid, turn_on) else {
            return false;
        };
        self.send_panic_sell_request(request);
        true
    }

    /// Toggle panic sell for an order already tracked by
    /// `EventDispatcher::orders()`.
    pub fn turn_tracked_order_panic_sell(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        turn_on: bool,
    ) -> bool {
        self.turn_order_panic_sell(orders, uid, turn_on)
    }

    /// Apply Delphi `SetImmuneClicks` locally and send `TSetImmuneCommand`.
    pub fn set_immune(
        &self,
        orders: &mut crate::state::Orders,
        items: &[crate::commands::trade::ImmuneItem],
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let applied = orders.set_immune_clicks(items);
        if applied.is_empty() {
            return false;
        }
        let raw = crate::commands::trade::build_set_immune(rand::random(), &applied);
        let items_uid_sum: u64 = applied
            .iter()
            .fold(0u64, |acc, it| acc.wrapping_add(it.uid));
        self.send_trade_keyed(raw, 3, UniqueKey::immune_clicks(items_uid_sum));
        true
    }

    /// Send `TMoveAllBuysCommand` if Delphi active-client gate finds a candidate order.
    pub fn move_all_buys(
        &self,
        orders: &crate::state::Orders,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        cmd_type: crate::commands::trade::MoveAllBuysCmdType,
        move_kind: crate::commands::trade::ReplaceMultiKind,
        price: f64,
        side: crate::commands::trade::FixedPosition,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        if !orders.has_move_all_buys_candidate(market, cmd_type, move_kind, side) {
            return false;
        }
        let raw = crate::commands::trade::build_move_all_buys(
            ctx, market, cmd_type, move_kind, price, side,
        );
        self.send_trade(raw, 3);
        true
    }

    /// Apply Delphi `SendVStopIfChanged` locally and send `TVStopUpdate`.
    pub fn update_vstop(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        vstop_on: bool,
        vstop_fixed: bool,
        vstop_level: f64,
        vstop_vol: f64,
    ) -> bool {
        if !self.domain_ready_for_typed_send() {
            return false;
        }
        let Some((ctx, market, params)) =
            orders.send_vstop_if_changed(uid, vstop_on, vstop_fixed, vstop_level, vstop_vol)
        else {
            return false;
        };
        let raw = crate::commands::trade::build_vstop_update(ctx, &market, 0, params);
        self.send_trade_keyed(raw, 3, UniqueKey::order_move(ctx.uid));
        true
    }

    /// Update VStop for an order already tracked by `EventDispatcher::orders()`.
    pub fn update_tracked_order_vstop(
        &self,
        orders: &mut crate::state::Orders,
        uid: u64,
        vstop_on: bool,
        vstop_fixed: bool,
        vstop_level: f64,
        vstop_vol: f64,
    ) -> bool {
        self.update_vstop(orders, uid, vstop_on, vstop_fixed, vstop_level, vstop_vol)
    }

    /// Send `TDoMarketSplitPositionCommand` (`MaxRetries=1`).
    pub fn do_market_split_position(
        &self,
        ctx: crate::commands::trade::TradeCtx,
        market: &str,
        is_short: bool,
    ) {
        let raw = crate::commands::trade::build_do_market_split_position(ctx, market, is_short);
        self.send_trade(raw, 1);
    }

    /// Send `TPenaltyCommand`.
    pub fn penalty(&self, ctx: crate::commands::trade::TradeCtx, market: &str) {
        let raw = crate::commands::trade::build_penalty(ctx, market);
        self.send_trade(raw, 3);
    }

    /// Mark Delphi `ServerUpdateSent` from a thread-safe sender.
    ///
    /// Call this when sending raw UI update/switch payloads through
    /// [`Self::send_cmd`] rather than the typed wrappers below.
    pub fn mark_server_update_sent(&self) {
        self.shared
            .server_update_sent
            .store(true, Ordering::Relaxed);
    }

    /// Send `TClientSettingsCommand`.
    pub fn ui_send_settings(&self, settings: &crate::commands::ui::ClientSettingsCommand) {
        let mut wire_settings = settings.clone();
        wire_settings.uid = rand::random();
        let raw = crate::commands::ui::build_client_settings(&wire_settings);
        self.send_domain_cmd_keyed(
            raw,
            Command::UI,
            SendPriority::Sliced,
            true,
            6,
            UniqueKey::base_ui_settings_slot(),
        );
    }

    /// Send `TSettingsRequest`.
    pub fn ui_settings_request(&self) {
        let raw = crate::commands::ui::build_settings_request(rand::random());
        self.send_domain_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// Send `TStratStartStopCommand`.
    pub fn ui_strat_start_stop(&self, is_start: bool) {
        let raw = crate::commands::ui::build_strat_start_stop(rand::random(), is_start);
        self.send_domain_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// Send `TStratStartStopCommandV2` with an explicit checked delta.
    ///
    /// Regular active-library callers should prefer
    /// `EventDispatcher::ui_strat_start_stop_v2`, which builds the delta from
    /// owned strategy state like Delphi `TStratStartStopCommandV2.Create`.
    pub fn ui_strat_start_stop_v2(
        &self,
        is_start: bool,
        items: &[crate::commands::strat::StratCheckedItem],
    ) {
        let raw = crate::commands::ui::build_strat_start_stop_v2(rand::random(), is_start, items);
        self.send_domain_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// Send `TMMOrdersSubscribeCommand`.
    pub fn ui_mm_subscribe(&self, subscribe: bool) {
        if let Err(e) = self.try_ui_mm_subscribe(subscribe) {
            log::warn!(target: "moonproto::client",
                "ui_mm_subscribe({subscribe}) dropped: {e}");
        }
    }

    /// Fallible `TMMOrdersSubscribeCommand`.
    pub fn try_ui_mm_subscribe(&self, subscribe: bool) -> Result<(), SubscribeError> {
        if !self.shared.app_queue_alive.load(Ordering::Relaxed) {
            return Err(SubscribeError::Disconnected);
        }
        {
            let mut registry = self.shared.subscription_registry.lock().unwrap();
            registry.mm_orders_sub = Some(subscribe);
        }
        let uid = rand::random();
        let raw = crate::commands::ui::build_mm_orders_subscribe(uid, subscribe);
        self.try_send_domain_cmd_keyed(
            raw,
            Command::UI,
            SendPriority::High,
            true,
            3,
            UniqueKey::turn_mm_detection_for(uid),
        )
    }

    /// Send `TUpdateVersionCommand` and mark Delphi `ServerUpdateSent`.
    pub fn ui_update_version(&self, version_name: &str, is_release: bool) {
        let raw =
            crate::commands::ui::build_update_version(rand::random(), version_name, is_release);
        if self.send_domain_cmd(raw, Command::UI, SendPriority::High, true, 3) {
            self.mark_server_update_sent();
        }
    }

    /// Send `TEmuTradesCommand`.
    pub fn ui_emu_trades(
        &self,
        m_index: u16,
        base_time: f64,
        points: &[crate::commands::ui::EmuTradePoint],
    ) {
        let raw = crate::commands::ui::build_emu_trades(rand::random(), m_index, base_time, points);
        self.send_domain_cmd(raw, Command::UI, SendPriority::Sliced, true, 6);
    }

    /// Send `TLevManageCommand`.
    pub fn ui_lev_manage(&self, cmd: &crate::commands::ui::LevManage) {
        let uid: u64 = rand::random();
        let raw = crate::commands::ui::build_lev_manage(uid, cmd);
        self.send_domain_cmd_keyed(
            raw,
            Command::UI,
            SendPriority::Sliced,
            true,
            6,
            UniqueKey::lev_manage_settings_slot(),
        );
    }

    /// Send `TTriggerManageCommand`.
    pub fn ui_trigger_manage(&self, action: u8, all_markets: bool, markets: &[u16], keys: &[u16]) {
        let raw = crate::commands::ui::build_trigger_manage(
            rand::random(),
            action,
            all_markets,
            markets,
            keys,
        );
        self.send_domain_cmd(raw, Command::UI, SendPriority::Sliced, true, 6);
    }

    /// Send `TResetProfitCommand`.
    pub fn ui_reset_profit(&self, kind: u8) {
        let raw = crate::commands::ui::build_reset_profit(rand::random(), kind);
        self.send_domain_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// Send `TArbActivateNotify`.
    pub fn ui_arb_activate_notify(&self, arb_valid: f64) {
        let raw = crate::commands::ui::build_arb_activate_notify(rand::random(), arb_valid);
        self.send_domain_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    /// Send `TSwitchDexCommand` and mark Delphi `ServerUpdateSent`.
    pub fn ui_switch_dex(&self, dex_name: &str) {
        let uid = rand::random();
        let raw = crate::commands::ui::build_switch_dex(uid, dex_name);
        if self.send_domain_cmd_keyed(
            raw,
            Command::UI,
            SendPriority::High,
            true,
            3,
            UniqueKey::dex_switch_for(uid),
        ) {
            self.mark_server_update_sent();
        }
    }

    /// Send `TSwitchSpotCommand` and mark Delphi `ServerUpdateSent`.
    pub fn ui_switch_spot(&self, spot_index: u8) {
        let uid = rand::random();
        let raw = crate::commands::ui::build_switch_spot(uid, spot_index);
        if self.send_domain_cmd_keyed(
            raw,
            Command::UI,
            SendPriority::High,
            true,
            3,
            UniqueKey::spot_switch_for(uid),
        ) {
            self.mark_server_update_sent();
        }
    }

    /// Send `TStratSnapshotRequest`.
    ///
    /// Protocol/testing tool only: Delphi server ignores this command when it
    /// is received from a client. Normal active-library flow answers the server
    /// request through `EventDispatcher`.
    pub fn strat_snapshot_request(&self) {
        let raw = crate::commands::strat::build_snapshot_request(rand::random());
        self.send_domain_cmd(raw, Command::Strat, SendPriority::High, true, 3);
    }

    /// Send `TStratSchemaRequest`.
    pub fn strat_schema_request(&self) {
        let raw = crate::commands::strat::build_schema_request(rand::random());
        self.send_domain_cmd(raw, Command::Strat, SendPriority::High, true, 3);
    }

    fn send_strat_snapshot_command(&self, raw: Vec<u8>) {
        self.send_domain_cmd_keyed(
            raw,
            Command::Strat,
            SendPriority::Sliced,
            true,
            6,
            UniqueKey::strat_snapshot(),
        );
    }

    /// Send `TStratSnapshot` from an already serialized strategy payload.
    pub fn strat_send_snapshot_payload(
        &self,
        server_epoch: u64,
        client_max_last_date: u64,
        full: bool,
        data: &[u8],
    ) {
        let uid: u64 = rand::random();
        let raw = crate::commands::strat::build_snapshot(
            uid,
            server_epoch,
            client_max_last_date,
            full,
            data,
        );
        self.send_strat_snapshot_command(raw);
    }

    /// Send `TStratSnapshot` from decoded strategy snapshots.
    ///
    /// `schema` must be the live `TStratSchema` fetched during Init; typed
    /// strategy serialization uses it for Delphi field order, PropMask
    /// visibility, TypeID checks, and defaults.
    pub fn strat_send_snapshot_batch(
        &self,
        server_epoch: u64,
        full: bool,
        schema: &crate::commands::strategy_schema::StrategySchema,
        strategies: &[crate::commands::strategy_serializer::StrategySnapshot],
    ) {
        let uid: u64 = rand::random();
        let raw = crate::commands::strat::build_snapshot_from_strategies(
            uid,
            server_epoch,
            full,
            schema,
            strategies,
        );
        self.send_strat_snapshot_command(raw);
    }

    /// Send `TStratDelete` for one strategy or folder.
    pub fn strat_delete(&self, strategy_id: u64, folder_path: &str) {
        let raw = crate::commands::strat::build_delete(rand::random(), strategy_id, folder_path);
        self.send_domain_cmd(raw, Command::Strat, SendPriority::High, true, 3);
    }

    /// Send `TStratSellPriceUpdate` for one strategy.
    pub fn strat_sell_price_update(&self, strategy_id: u64, sell_price: f64) {
        let raw = crate::commands::strat::build_sell_price_update(
            rand::random(),
            strategy_id,
            sell_price,
        );
        self.send_domain_cmd_keyed(
            raw,
            Command::Strat,
            SendPriority::High,
            true,
            3,
            UniqueKey::strat_sell_price_update(strategy_id),
        );
    }

    /// Send `TStratCheckedSync` with explicit items.
    ///
    /// Regular active-library callers should prefer
    /// `EventDispatcher::send_strategy_checked_delta`, which builds
    /// `TStrategies.GetCheckedDelta` from owned strategy state.
    pub fn strat_checked_sync(
        &self,
        items: &[crate::commands::strat::StratCheckedItem],
        is_delta: bool,
    ) {
        let raw = crate::commands::strat::build_checked_sync(rand::random(), items, is_delta);
        self.send_domain_cmd(raw, Command::Strat, SendPriority::Sliced, true, 6);
    }

    /// Send `TStratCheckedEcho` with explicit items.
    ///
    /// This is normally a server response path; public use is for protocol tools
    /// that already own the exact Delphi `Items` array.
    pub fn strat_checked_echo(&self, items: &[crate::commands::strat::StratCheckedItem]) {
        let raw = crate::commands::strat::build_checked_echo(rand::random(), items);
        self.send_domain_cmd(raw, Command::Strat, SendPriority::High, true, 3);
    }

    /// Send `TRequestBalanceRefresh`.
    pub fn balance_request_refresh(&self) {
        let raw = crate::commands::balance::build_request_balance_refresh(rand::random());
        self.send_domain_cmd(raw, Command::Balance, SendPriority::High, true, 3);
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
        self.shared
            .send_lock
            .lock()
            .unwrap()
            .push_send_cmd_int(item);
        Ok(())
    }
}
