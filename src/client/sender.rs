//! Thread-safe typed send/subscription handle.

use super::*;

mod orders;
mod subscriptions;

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
                crate::events::ActiveAction::RequestOrderSnapshot => {
                    self.request_all_statuses(rand::random());
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
