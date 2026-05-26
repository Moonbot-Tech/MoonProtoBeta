use super::*;

impl Client {
    // ====================================================================
    //  Active library: subscription API (по market_name + registry)
    //
    //  F4: thread-safe API через [`ClientSender`]. Эти методы — **главный
    //  публичный API** для подписок. В отличие от `api_subscribe_order_book`
    //  (low-level) они:
    //   1. Запоминают подписку в `subscription_registry`.
    //   2. После единственного Init восстанавливаются самой либой при reconnect.
    //   3. Принимают `market_name` (стабилен через reindex), не market_idx.
    //   4. Работают на `&self` — доступны во время `run_with_dispatcher`
    //      через `client.sender()` clone из любого thread'а.
    //
    //  Аналог Delphi `MoonProtoEngine.pas:305-360 CheckBookTopics` с
    //  `BookSubbed: TSet<TMarket>` и `NeedResubscribeOrderBooks`.
    // ====================================================================

    /// Thread-safe sender handle for subscribing and sending commands from any
    /// thread.
    ///
    /// The returned `ClientSender` is cloneable and can live in a UI thread,
    /// worker thread, or any other owner. `Client::run_with_dispatcher` drains
    /// those intents from the client main loop.
    ///
    /// ```ignore
    /// let mut client = Client::new(cfg);
    /// let sender = client.sender();
    /// thread::spawn(move || {
    ///     sender.subscribe_orderbook("DOGEUSDT");
    /// });
    /// client.run_with_dispatcher(...);
    /// ```
    pub fn sender(&self) -> ClientSender {
        ClientSender {
            shared: Arc::new(ClientSenderShared {
                app_queue_alive: Arc::clone(&self.app_queue_alive),
                domain_ready: Arc::clone(&self.domain_ready_flag),
                send_lock: Arc::clone(&self.send_lock),
                subscription_registry: Arc::clone(&self.subscription_registry),
                subscription_summary: Arc::clone(&self.subscription_summary),
                subscription_trades_scope: Arc::clone(&self.subscription_trades_scope),
                server_update_sent: Arc::clone(&self.server_update_sent),
                last_trades_subscribe_request_ms: Arc::clone(
                    &self.last_trades_subscribe_request_ms,
                ),
                last_orderbook_subscribe_request_ms: Arc::clone(
                    &self.last_orderbook_subscribe_request_ms,
                ),
                last_orderbook_subscribe_request_uid: Arc::clone(
                    &self.last_orderbook_subscribe_request_uid,
                ),
            }),
            start: self._start,
        }
    }

    /// Hidden FireTest hook: when enabled, no outgoing datagrams are sent.
    ///
    /// Normal applications must not use this. The live FireTest uses it to make
    /// the MoonBot server stop hearing from this client, then verifies that the
    /// library reconnects and restores subscriptions after the flag is cleared.
    #[doc(hidden)]
    pub fn debug_set_outgoing_blackhole(&mut self, enabled: bool) {
        self.debug_outgoing_blackhole
            .store(enabled, Ordering::Relaxed);
    }

    /// Subscribe to the orderbook stream for one market name.
    ///
    /// This is a fire-and-forget convenience wrapper around
    /// `self.sender().subscribe_orderbook(...)`. It records the intent in the
    /// shared registry and appends the resulting wire request directly into the
    /// Delphi-style send queues; a warning is logged only if the client is gone.
    /// Use `client.sender().try_subscribe_orderbook(...)` when the caller needs
    /// explicit failure feedback.
    ///
    /// The subscription is stored in the registry. Before init, reconnect does
    /// not send it. After init, reconnect restores it automatically without a
    /// second init; after a server restart, replay waits for fresh
    /// `GetMarketsIndexes` for the current `PeerAppToken`, matching Delphi
    /// `CheckBookTopics`. The server resolves `market_name -> market_idx`, so
    /// callers may subscribe before `emk_GetMarketsList` has completed. The
    /// call is idempotent; futures and spot books are distinguished by incoming
    /// `book_kind`, not by the subscribe request.
    pub fn subscribe_orderbook(&self, market_name: &str) {
        self.sender().subscribe_orderbook(market_name);
    }

    /// Subscribe to several orderbook streams in one registry-aware batch.
    ///
    /// Already remembered market names are ignored. Newly added names are sent
    /// through one `emk_SubscribeOrderBook` request, matching the server's
    /// batch-oriented `MarketNames` field.
    pub fn subscribe_orderbooks<I, S>(&self, market_names: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.sender().subscribe_orderbooks(market_names);
    }

    /// Unsubscribe from one market's orderbook stream.
    ///
    /// See [`Client::subscribe_orderbook`] for registry and reconnect behavior.
    pub fn unsubscribe_orderbook(&self, market_name: &str) {
        self.sender().unsubscribe_orderbook(market_name);
    }

    /// Unsubscribe from several orderbook streams in one registry-aware batch.
    pub fn unsubscribe_orderbooks<I, S>(&self, market_names: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.sender().unsubscribe_orderbooks(market_names);
    }

    /// Unsubscribe from all remembered orderbook streams.
    ///
    /// This clears the reconnect registry and sends one batched
    /// `emk_UnsubscribeOrderBook` request for the market names that were actually
    /// remembered. Prefer this high-level method over raw Engine API calls; the
    /// raw call does not update the registry and reconnect would restore stale
    /// subscriptions.
    pub fn unsubscribe_all_orderbooks(&self) {
        self.sender().unsubscribe_all_orderbooks();
    }

    /// Subscribe to the all-trades stream.
    ///
    /// `want_mm` requests market-maker order sections. The subscription is
    /// stored in the registry and restored automatically after reconnect once
    /// init has completed. Calling it again with a different `want_mm` updates
    /// the remembered intent and sends a fresh subscribe request.
    pub fn subscribe_all_trades(&self, want_mm: bool) {
        self.sender().subscribe_all_trades(want_mm);
    }

    /// Subscribe to all-trades on the wire, but keep retained Active Lib data
    /// only for selected markets.
    ///
    /// Empty `market_names` means all markets.
    pub fn subscribe_trades_for<I, S>(&self, want_mm: bool, market_names: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.sender().subscribe_trades_for(want_mm, market_names);
    }

    /// Unsubscribe from the all-trades stream and remove the registry intent.
    pub fn unsubscribe_all_trades(&self) {
        self.sender().unsubscribe_all_trades();
    }

    #[cfg(test)]
    pub(crate) fn outgoing_mm_orders_subscribe_intent(item: &SendItem) -> Option<bool> {
        if item.cmd != Command::UI.to_byte() || item.u_key.kind != UK_TURN_MM_DETECTION {
            return None;
        }
        if item.data.first().copied() != Some(5) {
            return None;
        }
        item.data.last().map(|v| *v != 0)
    }

    pub(crate) fn apply_mm_orders_subscribe_intent(&mut self, subscribe: bool) {
        let mut registry = self.subscription_registry.lock().unwrap();
        registry.mm_orders_sub = Some(subscribe);
        self.refresh_subscription_summary(&registry);
    }

    pub(crate) fn send_mm_orders_subscribe_cmd(&self, subscribe: bool) {
        let uid = rand::random();
        let raw = crate::commands::ui::build_mm_orders_subscribe(uid, subscribe);
        self.send_cmd_keyed(
            raw,
            Command::UI,
            SendPriority::High,
            true,
            3,
            UniqueKey::turn_mm_detection_for(uid),
        );
    }

    pub(crate) fn domain_restore_needs_indexes(&self) -> bool {
        self.domain_restore.fetch_indexes
            || self.subscription_summary.trades_subscribed()
            || self.subscription_summary.has_orderbook_subs()
    }

    pub(crate) fn send_markets_indexes_restore_request(&mut self, now_ms: i64) {
        self.update_markets_after_indexes = true;
        if self.indexes_fetch_in_flight {
            return;
        }
        self.indexes_fetch_in_flight = true;
        self.indexes_fetch_started_ms = now_ms;
        self.send_api_request(&crate::commands::engine_request::get_markets_indexes());
    }

    /// Restore domain intent after reconnect inside an already initialized Client session.
    ///
    /// This is deliberately gated by `domain_ready`: before the single init pass `Fine`
    /// remains transport-only and must not emit Engine API traffic.
    pub(crate) fn restore_domain_after_reconnect(&mut self) {
        if !self.domain_ready {
            return;
        }

        let orderbooks_need_fresh_indexes = self.subscription_summary.has_orderbook_subs()
            && !self.market_indexes_current_for_peer();
        if orderbooks_need_fresh_indexes {
            self.restore_orderbooks_after_indexes = true;
        }

        if self.domain_restore_needs_indexes() {
            self.send_markets_indexes_restore_request(self.now_ms());
        }

        self.restore_registry_subscriptions_without_delayed_orderbooks(
            orderbooks_need_fresh_indexes,
            true,
        );
    }

    /// Batch restore helper for the subscription registry.
    ///
    /// OrderBook подписки отправляются одним `emk_SubscribeOrderBook` batch'ем:
    /// в Delphi wire request нет `OrderBookKind`, только список имён рынков.
    #[cfg(test)]
    pub(crate) fn restore_registry_subscriptions(&mut self) {
        self.restore_registry_subscriptions_without_delayed_orderbooks(false, false);
    }

    fn restore_registry_subscriptions_without_delayed_orderbooks(
        &mut self,
        delay_orderbooks: bool,
        delay_trades: bool,
    ) {
        let (trades_sub, mm_orders_sub, orderbook_subs) = {
            let registry = self.subscription_registry.lock().unwrap();
            (
                registry.trades_sub,
                registry.mm_orders_sub,
                registry.orderbook_subs.iter().cloned().collect::<Vec<_>>(),
            )
        };

        if let Some(sub) = trades_sub {
            if delay_trades {
                // Reconnect path is handled by `tick_trades_reconnect_sequence`:
                // Delphi does not just replay SubscribeAllTrades; it first sends
                // UnsubscribeAllTrades, waits 100ms, then subscribes again.
            } else {
                let want_mm = sub.want_mm;
                self.send_api_request(&crate::commands::engine_request::subscribe_all_trades(
                    want_mm,
                ));
                if let Some(mm_orders) = mm_orders_sub {
                    if mm_orders != want_mm {
                        self.send_mm_orders_subscribe_cmd(mm_orders);
                    }
                }
            }
        } else if let Some(subscribe) = mm_orders_sub {
            self.send_mm_orders_subscribe_cmd(subscribe);
        }
        if delay_orderbooks {
            return;
        }
        self.restore_orderbook_subscriptions_as_reconnect_batch(orderbook_subs, self.now_ms());
    }

    fn registry_trades_want_mm(&self) -> Option<bool> {
        let registry = self.subscription_registry.lock().unwrap();
        let sub = registry.trades_sub?;
        Some(sub.want_mm)
    }

    fn registry_trades_mm_orders_intent(&self) -> Option<bool> {
        let registry = self.subscription_registry.lock().unwrap();
        registry.mm_orders_sub
    }

    fn start_trades_reconnect_sequence(&mut self, now_ms: i64) {
        if self.registry_trades_want_mm().is_none() {
            return;
        }
        self.last_trades_reconnect_check_ms = now_ms;
        let payload = crate::commands::engine_request::unsubscribe_all_trades();
        let request_uid = engine_request_uid(&payload).unwrap_or(NO_PENDING_ENGINE_REQUEST_UID);
        self.send_api_request_at(&payload, now_ms);
        self.pending_trades_unsubscribe = Some(PendingTradesUnsubscribe {
            request_uid,
            sent_ms: now_ms,
        });
        self.pending_trades_resubscribe_after_ms = None;
    }

    pub(crate) fn tick_trades_reconnect_sequence(&mut self, now_ms: i64, trades_server_token: u64) {
        if !self.domain_ready {
            return;
        }

        let last_subscribe_request_ms = self
            .last_trades_subscribe_request_ms
            .load(Ordering::Relaxed);
        if last_subscribe_request_ms != NEVER_TIME_MS
            && (now_ms - last_subscribe_request_ms).abs()
                < crate::api_pending::DEFAULT_PENDING_TIMEOUT_MS
        {
            return;
        }

        if let Some(pending) = self.pending_trades_unsubscribe {
            if (now_ms - pending.sent_ms).abs() < crate::api_pending::DEFAULT_PENDING_TIMEOUT_MS {
                return;
            }
            self.pending_trades_unsubscribe = None;
            self.pending_trades_resubscribe_after_ms =
                Some(now_ms + TRADES_RECONNECT_RESUBSCRIBE_DELAY_MS);
            return;
        }

        if let Some(due_ms) = self.pending_trades_resubscribe_after_ms {
            if now_ms >= due_ms {
                self.pending_trades_resubscribe_after_ms = None;
                if let Some(want_mm) = self.registry_trades_want_mm() {
                    self.send_api_request_at(
                        &crate::commands::engine_request::subscribe_all_trades(want_mm),
                        now_ms,
                    );
                    if let Some(mm_orders) = self.registry_trades_mm_orders_intent() {
                        if mm_orders != want_mm {
                            self.send_mm_orders_subscribe_cmd(mm_orders);
                        }
                    }
                }
            }
            return;
        }

        if self.registry_trades_want_mm().is_none() || self.server_token == 0 {
            return;
        }
        if self.server_token == trades_server_token {
            return;
        }
        if (now_ms - self.last_trades_reconnect_check_ms).abs() < TRADES_RECONNECT_THROTTLE_MS {
            return;
        }
        self.start_trades_reconnect_sequence(now_ms);
    }

    pub(crate) fn close_trades_unsubscribe_wait_if_matches(&mut self, request_uid: u64) {
        let Some(pending) = self.pending_trades_unsubscribe else {
            return;
        };
        if pending.request_uid != request_uid {
            return;
        }
        self.pending_trades_unsubscribe = None;
        self.pending_trades_resubscribe_after_ms =
            Some(self.now_ms() + TRADES_RECONNECT_RESUBSCRIBE_DELAY_MS);
    }

    pub(crate) fn tick_orderbook_reconnect_sequence(&mut self, now_ms: i64) -> bool {
        if !self.domain_ready || self.server_token == 0 || !self.market_indexes_current_for_peer() {
            return false;
        }
        if self.server_token == self.subscribed_book_server_token {
            return false;
        }
        let last_subscribe_request_ms = self
            .last_orderbook_subscribe_request_ms
            .load(Ordering::Relaxed);
        if last_subscribe_request_ms != NEVER_TIME_MS
            && (now_ms - last_subscribe_request_ms).abs()
                < crate::api_pending::DEFAULT_PENDING_TIMEOUT_MS
        {
            return false;
        }
        if (now_ms - self.last_book_reconnect_check_ms).abs() < ORDERBOOK_RECONNECT_THROTTLE_MS {
            return false;
        }
        let orderbook_subs = {
            let registry = self.subscription_registry.lock().unwrap();
            registry.orderbook_subs.iter().cloned().collect::<Vec<_>>()
        };
        if orderbook_subs.is_empty() {
            return false;
        }

        self.restore_orderbook_subscriptions_as_reconnect_batch(orderbook_subs, now_ms)
    }

    fn restore_orderbook_subscriptions_as_reconnect_batch(
        &mut self,
        orderbook_subs: Vec<String>,
        now_ms: i64,
    ) -> bool {
        self.last_book_reconnect_check_ms = now_ms;
        match self.send_orderbook_subscribe_batch(orderbook_subs, now_ms) {
            Some(uid) => {
                self.pending_orderbook_resubscribe_uid = Some(uid);
                true
            }
            None => false,
        }
    }

    fn send_orderbook_subscribe_batch(
        &self,
        orderbook_subs: Vec<String>,
        now_ms: i64,
    ) -> Option<u64> {
        let refs: Vec<&str> = orderbook_subs.iter().map(String::as_str).collect();
        if !refs.is_empty() {
            let payload = crate::commands::engine_request::subscribe_order_book(&refs);
            let uid = engine_request_uid(&payload);
            self.send_api_request_at(&payload, now_ms);
            return uid;
        }
        None
    }

    pub(crate) fn close_orderbook_subscribe_wait_if_matches(&self, request_uid: u64) {
        if self
            .last_orderbook_subscribe_request_uid
            .load(Ordering::Relaxed)
            == request_uid
        {
            self.last_orderbook_subscribe_request_ms
                .store(NEVER_TIME_MS, Ordering::Relaxed);
            self.last_orderbook_subscribe_request_uid
                .store(NO_PENDING_ENGINE_REQUEST_UID, Ordering::Relaxed);
        }
    }

    pub(crate) fn restore_orderbook_subscriptions_from_registry(&mut self) {
        let orderbook_subs = {
            let registry = self.subscription_registry.lock().unwrap();
            registry.orderbook_subs.iter().cloned().collect::<Vec<_>>()
        };
        self.restore_orderbook_subscriptions_as_reconnect_batch(orderbook_subs, self.now_ms());
    }

    /// Flush subscription intents collected before the one-time Init opened
    /// `domain_ready`.
    ///
    /// `send_post_init_resync` already sends the current MM-orders flag, so this
    /// helper sends only stream subscriptions: all-trades and orderbooks.
    pub(crate) fn send_registry_subscriptions_after_init(&mut self) {
        if !self.domain_ready {
            return;
        }

        let (trades_sub, orderbook_subs) = {
            let registry = self.subscription_registry.lock().unwrap();
            (
                registry.trades_sub,
                registry.orderbook_subs.iter().cloned().collect::<Vec<_>>(),
            )
        };

        if let Some(sub) = trades_sub {
            let want_mm = sub.want_mm;
            self.send_api_request(&crate::commands::engine_request::subscribe_all_trades(
                want_mm,
            ));
            let mut registry = self.subscription_registry.lock().unwrap();
            registry.mm_orders_sub = Some(want_mm);
        }

        let refs: Vec<&str> = orderbook_subs.iter().map(String::as_str).collect();
        if !refs.is_empty() {
            self.send_api_request(&crate::commands::engine_request::subscribe_order_book(
                &refs,
            ));
        }
    }
}
