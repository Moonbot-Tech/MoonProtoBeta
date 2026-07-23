#[cfg(test)]
use super::send_queue::UK_TURN_MM_DETECTION;
use super::*;

impl Client {
    // ====================================================================
    //  Active library: subscription API (by market_name + registry)
    //
    //  F4: thread-safe API via [`ClientSender`]. In the public Active Lib
    //  users call the same-named methods on `MoonClient`; these low-level
    //  methods are needed by the runtime owner. Unlike the raw `api_subscribe_order_book`
    //  they:
    //   1. Remember the subscription in `subscription_registry`.
    //   2. Are restored by the library itself on reconnect after the single Init.
    //   3. Take a `market_name` (stable across reindex), not a market_idx.
    //   4. Operate on `&self` inside the runtime owner.
    //
    //  Analog of Delphi `MoonProtoEngine.pas:305-360 CheckBookTopics` with
    //  `BookSubbed: TSet<TMarket>` and `NeedResubscribeOrderBooks`.
    // ====================================================================

    /// Internal sender handle for low-level `Client` tests and runtime helpers.
    ///
    /// `MoonClient` is the normal subscription API. This lower-level sender is
    /// retained for internal runtime/tests.
    ///
    pub(crate) fn sender_internal(&self) -> ClientSender {
        ClientSender {
            shared: Arc::new(ClientSenderShared {
                app_queue_alive: Arc::clone(&self.lifecycle.app_queue_alive),
                domain_ready: Arc::clone(&self.subscriptions.domain_ready_flag),
                send_lock: Arc::clone(&self.send_lock),
                subscription_registry: Arc::clone(&self.subscriptions.subscription_registry),
                subscription_summary: Arc::clone(&self.subscriptions.subscription_summary),
                subscription_trades_scope: Arc::clone(
                    &self.subscriptions.subscription_trades_scope,
                ),
                server_update_sent: Arc::clone(&self.refresh_clocks.server_update_sent),
                last_trades_subscribe_request_ms: Arc::clone(
                    &self.reconnect.last_trades_subscribe_request_ms,
                ),
                last_orderbook_subscribe_request_ms: Arc::clone(
                    &self.reconnect.last_orderbook_subscribe_request_ms,
                ),
                last_orderbook_subscribe_request_uid: Arc::clone(
                    &self.reconnect.last_orderbook_subscribe_request_uid,
                ),
                last_candle_subscribe_request_ms: Arc::clone(
                    &self.reconnect.last_candle_subscribe_request_ms,
                ),
                pending_candle_subscribes: Arc::clone(&self.reconnect.pending_candle_subscribes),
            }),
            start: self._start,
        }
    }

    pub(crate) fn subscription_registry_handle(
        &self,
    ) -> Arc<parking_lot::Mutex<SubscriptionRegistry>> {
        Arc::clone(&self.subscriptions.subscription_registry)
    }

    #[cfg(test)]
    #[doc(hidden)]
    pub(crate) fn sender(&self) -> ClientSender {
        self.sender_internal()
    }

    /// Hidden FireTest hook: when enabled, no outgoing datagrams are sent.
    ///
    /// Normal applications must not use this. The live FireTest uses it to make
    /// the MoonBot server stop hearing from this client, then verifies that the
    /// library reconnects and restores subscriptions after the flag is cleared.
    #[doc(hidden)]
    #[cfg(any(test, feature = "diagnostics"))]
    pub(crate) fn debug_set_outgoing_blackhole(&mut self, enabled: bool) {
        self.metrics
            .debug_outgoing_blackhole
            .store(enabled, Ordering::Relaxed);
    }

    /// Subscribe to the orderbook stream for one market name.
    ///
    /// This records the intent in the shared registry and appends the resulting
    /// wire request directly into the Delphi-style send queues; a warning is
    /// logged only if the client is gone. Regular applications should use
    /// `MoonClient::streams().subscribe_orderbook(...)`.
    ///
    /// The subscription is stored in the registry. Before init, reconnect does
    /// not send it. After init, reconnect restores it automatically without a
    /// second init; after a server restart, replay waits for fresh
    /// `GetMarketsIndexes` for the current `PeerAppToken`, matching Delphi
    /// `CheckBookTopics`. The server resolves `market_name -> market_idx`, so
    /// callers may subscribe before `emk_GetMarketsList` has completed. The
    /// call is idempotent; futures and spot books are distinguished by incoming
    /// `book_kind`, not by the subscribe request.
    pub(crate) fn subscribe_orderbook(&self, market_name: &str) {
        self.sender_internal().subscribe_orderbook(market_name);
    }

    /// Subscribe to several orderbook streams in one registry-aware batch.
    ///
    /// Already remembered market names are ignored. Newly added names are sent
    /// through one `emk_SubscribeOrderBook` request, matching the server's
    /// batch-oriented `MarketNames` field.
    pub(crate) fn subscribe_orderbooks<I, S>(&self, market_names: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.sender_internal().subscribe_orderbooks(market_names);
    }

    /// Unsubscribe from one market's orderbook stream.
    ///
    /// See [`Client::subscribe_orderbook`] for registry and reconnect behavior.
    pub(crate) fn unsubscribe_orderbook(&self, market_name: &str) {
        self.sender_internal().unsubscribe_orderbook(market_name);
    }

    /// Unsubscribe from several orderbook streams in one registry-aware batch.
    pub(crate) fn unsubscribe_orderbooks<I, S>(&self, market_names: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.sender_internal().unsubscribe_orderbooks(market_names);
    }

    /// Unsubscribe from all remembered orderbook streams.
    ///
    /// This clears the reconnect registry and sends one batched
    /// `emk_UnsubscribeOrderBook` request for the market names that were actually
    /// remembered. Prefer this high-level method over raw Engine API calls; the
    /// raw call does not update the registry and reconnect would restore stale
    /// subscriptions.
    pub(crate) fn unsubscribe_all_orderbooks(&self) {
        self.sender_internal().unsubscribe_all_orderbooks();
    }

    /// Subscribe to the all-trades stream.
    ///
    /// `want_mm` requests market-maker order sections. The subscription is
    /// stored in the registry and restored automatically after reconnect once
    /// init has completed. Calling it again with a different `want_mm` updates
    /// the remembered intent and sends a fresh subscribe request.
    pub(crate) fn subscribe_all_trades(&self, want_mm: bool) {
        self.sender_internal().subscribe_all_trades(want_mm);
    }

    /// Subscribe to all-trades on the wire, but keep retained Active Lib data
    /// only for selected markets.
    ///
    /// Empty `market_names` means all markets.
    pub(crate) fn subscribe_trades_for<I, S>(&self, want_mm: bool, market_names: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.sender_internal()
            .subscribe_trades_for(want_mm, market_names);
    }

    /// Unsubscribe from the all-trades stream and remove the registry intent.
    pub(crate) fn unsubscribe_all_trades(&self) {
        self.sender_internal().unsubscribe_all_trades();
    }

    /// Subscribe to live TF candle updates for several markets.
    pub(crate) fn subscribe_candles<I, S>(
        &self,
        market_names: I,
        kind: crate::commands::candles::DeepHistoryKind,
    ) where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.sender_internal().subscribe_candles(market_names, kind);
    }

    /// Unsubscribe from live TF candle updates for several markets.
    pub(crate) fn unsubscribe_candles<I, S>(&self, market_names: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.sender_internal().unsubscribe_candles(market_names);
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
        let mut registry = self.subscriptions.subscription_registry.lock();
        registry.mm_orders_sub = Some(subscribe);
        self.refresh_subscription_summary(&registry);
    }

    pub(crate) fn send_mm_orders_subscribe_cmd(&self, subscribe: bool) {
        let uid = rand::random();
        let raw = crate::commands::ui::build_mm_orders_subscribe(uid, subscribe);
        self.send_typed_domain_cmd(raw, Command::UI);
    }

    pub(crate) fn domain_restore_needs_indexes(&self) -> bool {
        self.subscriptions.domain_restore.fetch_indexes
            || self.subscriptions.subscription_summary.trades_subscribed()
            || self.subscriptions.subscription_summary.has_orderbook_subs()
    }

    pub(crate) fn send_markets_indexes_restore_request(&mut self, now_ms: i64) {
        self.reconnect.update_markets_after_indexes = true;
        if self.reconnect.indexes_fetch_in_flight {
            return;
        }
        self.reconnect.indexes_fetch_in_flight = true;
        self.reconnect.indexes_fetch_started_ms = now_ms;
        self.send_api_request(&crate::commands::engine_request::get_markets_indexes());
    }

    /// Restore domain intent after reconnect inside an already initialized Client session.
    ///
    /// This is deliberately gated by `domain_ready`: before the single init pass `Fine`
    /// remains transport-only and must not emit Engine API traffic.
    pub(crate) fn restore_domain_after_reconnect(&mut self) {
        if !self.subscriptions.domain_ready {
            return;
        }

        // OrdersProto requires one full pull after every new authorized hard
        // session. The server's eager snapshot is useful latency-wise, but it
        // can race the Fine boundary; this independent request closes that
        // window and also reconciles orders missed while the transport was down.
        self.request_orders_snapshot();

        let indexes_stale = self.peer_app_token != 0 && !self.market_indexes_current_for_peer();
        let orderbooks_need_fresh_indexes =
            self.subscriptions.subscription_summary.has_orderbook_subs() && indexes_stale;
        if orderbooks_need_fresh_indexes {
            self.reconnect.restore_orderbooks_after_indexes = true;
        }

        if indexes_stale && self.domain_restore_needs_indexes() {
            self.send_markets_indexes_restore_request(self.now_ms());
        }

        self.restore_registry_subscriptions_without_delayed_orderbooks(
            orderbooks_need_fresh_indexes,
            true,
        );
    }

    /// Batch restore helper for the subscription registry.
    ///
    /// OrderBook subscriptions are sent as a single `emk_SubscribeOrderBook` batch:
    /// the Delphi wire request has no `OrderBookKind`, only a list of market names.
    #[cfg(test)]
    pub(crate) fn restore_registry_subscriptions(&mut self) {
        self.restore_registry_subscriptions_without_delayed_orderbooks(false, false);
    }

    fn restore_registry_subscriptions_without_delayed_orderbooks(
        &mut self,
        delay_orderbooks: bool,
        delay_trades: bool,
    ) {
        let (trades_sub, mm_orders_sub, orderbook_subs, candle_subs) = {
            let registry = self.subscriptions.subscription_registry.lock();
            (
                registry.trades_sub,
                registry.mm_orders_sub,
                registry.orderbook_subs.iter().cloned().collect::<Vec<_>>(),
                registry
                    .candle_subs
                    .iter()
                    .map(|(market, &kind)| (market.clone(), kind))
                    .collect::<Vec<_>>(),
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
        self.restore_candle_subscriptions(candle_subs, self.now_ms());
        if delay_orderbooks {
            return;
        }
        self.restore_orderbook_subscriptions_as_reconnect_batch(orderbook_subs, self.now_ms());
    }

    fn restore_candle_subscriptions(
        &self,
        candle_subs: Vec<(String, crate::commands::candles::DeepHistoryKind)>,
        now_ms: i64,
    ) {
        for kind in crate::commands::candles::DeepHistoryKind::ALL {
            let mut markets: Vec<&str> = candle_subs
                .iter()
                .filter_map(|(market, market_kind)| {
                    (*market_kind == kind).then_some(market.as_str())
                })
                .collect();
            if markets.is_empty() {
                continue;
            }
            markets.sort_unstable();
            self.send_api_request_at(
                &crate::commands::candles::subscribe_candles(&markets, kind),
                now_ms,
            );
        }
    }

    fn registry_trades_want_mm(&self) -> Option<bool> {
        let registry = self.subscriptions.subscription_registry.lock();
        let sub = registry.trades_sub?;
        Some(sub.want_mm)
    }

    fn registry_trades_mm_orders_intent(&self) -> Option<bool> {
        let registry = self.subscriptions.subscription_registry.lock();
        registry.mm_orders_sub
    }

    fn start_trades_reconnect_sequence(&mut self, now_ms: i64) {
        if self.registry_trades_want_mm().is_none() {
            return;
        }
        self.reconnect.last_trades_reconnect_check_ms = now_ms;
        let payload = crate::commands::engine_request::unsubscribe_all_trades();
        let request_uid = engine_request_uid(&payload).unwrap_or(NO_PENDING_ENGINE_REQUEST_UID);
        self.send_api_request_at(&payload, now_ms);
        self.reconnect.pending_trades_unsubscribe = Some(PendingTradesUnsubscribe {
            request_uid,
            sent_ms: now_ms,
        });
        self.reconnect.pending_trades_resubscribe_after_ms = None;
    }

    pub(crate) fn tick_trades_reconnect_sequence(&mut self, now_ms: i64, trades_server_token: u64) {
        if !self.subscriptions.domain_ready {
            return;
        }

        let last_subscribe_request_ms = self
            .reconnect
            .last_trades_subscribe_request_ms
            .load(Ordering::Relaxed);
        if last_subscribe_request_ms != NEVER_TIME_MS
            && (now_ms - last_subscribe_request_ms).abs()
                < crate::api_pending::DEFAULT_PENDING_TIMEOUT_MS
        {
            return;
        }

        if let Some(pending) = self.reconnect.pending_trades_unsubscribe {
            if (now_ms - pending.sent_ms).abs() < crate::api_pending::DEFAULT_PENDING_TIMEOUT_MS {
                return;
            }
            self.reconnect.pending_trades_unsubscribe = None;
            self.reconnect.pending_trades_resubscribe_after_ms =
                Some(now_ms + TRADES_RECONNECT_RESUBSCRIBE_DELAY_MS);
            return;
        }

        if let Some(due_ms) = self.reconnect.pending_trades_resubscribe_after_ms {
            if now_ms >= due_ms {
                self.reconnect.pending_trades_resubscribe_after_ms = None;
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
        if (now_ms - self.reconnect.last_trades_reconnect_check_ms).abs()
            < TRADES_RECONNECT_THROTTLE_MS
        {
            return;
        }
        self.start_trades_reconnect_sequence(now_ms);
    }

    pub(crate) fn close_trades_unsubscribe_wait_if_matches(&mut self, request_uid: u64) {
        let Some(pending) = self.reconnect.pending_trades_unsubscribe else {
            return;
        };
        if pending.request_uid != request_uid {
            return;
        }
        self.reconnect.pending_trades_unsubscribe = None;
        self.reconnect.pending_trades_resubscribe_after_ms =
            Some(self.now_ms() + TRADES_RECONNECT_RESUBSCRIBE_DELAY_MS);
    }

    pub(crate) fn tick_orderbook_reconnect_sequence(&mut self, now_ms: i64) -> bool {
        if !self.subscriptions.domain_ready
            || self.server_token == 0
            || !self.market_indexes_current_for_peer()
        {
            return false;
        }
        if self.server_token == self.reconnect.subscribed_book_server_token {
            return false;
        }
        let last_subscribe_request_ms = self
            .reconnect
            .last_orderbook_subscribe_request_ms
            .load(Ordering::Relaxed);
        if last_subscribe_request_ms != NEVER_TIME_MS
            && (now_ms - last_subscribe_request_ms).abs()
                < crate::api_pending::DEFAULT_PENDING_TIMEOUT_MS
        {
            return false;
        }
        if (now_ms - self.reconnect.last_book_reconnect_check_ms).abs()
            < ORDERBOOK_RECONNECT_THROTTLE_MS
        {
            return false;
        }
        let orderbook_subs = {
            let registry = self.subscriptions.subscription_registry.lock();
            registry.orderbook_subs.iter().cloned().collect::<Vec<_>>()
        };
        if orderbook_subs.is_empty() {
            return false;
        }

        self.restore_orderbook_subscriptions_as_reconnect_batch(orderbook_subs, now_ms)
    }

    pub(crate) fn tick_candle_reconnect_sequence(&mut self, now_ms: i64) {
        if !self.subscriptions.domain_ready || self.server_token == 0 {
            return;
        }
        let last_request_ms = self
            .reconnect
            .last_candle_subscribe_request_ms
            .load(Ordering::Relaxed);
        let request_wait_active = !self.reconnect.pending_candle_subscribes.lock().is_empty()
            && last_request_ms != NEVER_TIME_MS
            && (now_ms - last_request_ms).abs() < crate::api_pending::DEFAULT_PENDING_TIMEOUT_MS;
        if request_wait_active {
            return;
        }
        if last_request_ms != NEVER_TIME_MS {
            self.reconnect.pending_candle_subscribes.lock().clear();
            self.reconnect
                .last_candle_subscribe_request_ms
                .store(NEVER_TIME_MS, Ordering::Relaxed);
            self.reconnect.subscribed_candle_server_token = 0;
        }
        if self.server_token == self.reconnect.subscribed_candle_server_token {
            return;
        }
        let candle_subs = {
            let registry = self.subscriptions.subscription_registry.lock();
            registry
                .candle_subs
                .iter()
                .map(|(market, &kind)| (market.clone(), kind))
                .collect::<Vec<_>>()
        };
        if candle_subs.is_empty() {
            return;
        }
        if (now_ms - self.reconnect.last_candle_reconnect_check_ms).abs()
            < CANDLE_RECONNECT_THROTTLE_MS
        {
            return;
        }
        self.reconnect.last_candle_reconnect_check_ms = now_ms;
        self.reconnect.subscribed_candle_server_token = 0;
        self.restore_candle_subscriptions(candle_subs, now_ms);
    }

    fn restore_orderbook_subscriptions_as_reconnect_batch(
        &mut self,
        orderbook_subs: Vec<String>,
        now_ms: i64,
    ) -> bool {
        self.reconnect.last_book_reconnect_check_ms = now_ms;
        match self.send_orderbook_subscribe_batch(orderbook_subs, now_ms) {
            Some(uid) => {
                self.reconnect.pending_orderbook_resubscribe_uid = Some(uid);
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
            .reconnect
            .last_orderbook_subscribe_request_uid
            .load(Ordering::Relaxed)
            == request_uid
        {
            self.reconnect
                .last_orderbook_subscribe_request_ms
                .store(NEVER_TIME_MS, Ordering::Relaxed);
            self.reconnect
                .last_orderbook_subscribe_request_uid
                .store(NO_PENDING_ENGINE_REQUEST_UID, Ordering::Relaxed);
        }
    }

    pub(crate) fn restore_orderbook_subscriptions_from_registry(&mut self) {
        let orderbook_subs = {
            let registry = self.subscriptions.subscription_registry.lock();
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
        if !self.subscriptions.domain_ready {
            return;
        }

        let (trades_sub, orderbook_subs, candle_subs) = {
            let registry = self.subscriptions.subscription_registry.lock();
            (
                registry.trades_sub,
                registry.orderbook_subs.iter().cloned().collect::<Vec<_>>(),
                registry
                    .candle_subs
                    .iter()
                    .map(|(market, &kind)| (market.clone(), kind))
                    .collect::<Vec<_>>(),
            )
        };

        if let Some(sub) = trades_sub {
            let want_mm = sub.want_mm;
            self.send_api_request(&crate::commands::engine_request::subscribe_all_trades(
                want_mm,
            ));
            let mut registry = self.subscriptions.subscription_registry.lock();
            registry.mm_orders_sub = Some(want_mm);
        }

        let refs: Vec<&str> = orderbook_subs.iter().map(String::as_str).collect();
        if !refs.is_empty() {
            self.send_api_request(&crate::commands::engine_request::subscribe_order_book(
                &refs,
            ));
        }
        self.restore_candle_subscriptions(candle_subs, self.now_ms());
    }
}
