//! Session identity and trade-route accessors.

use super::*;

impl Client {
    /// Server identity (`bot_id`, `exchange_name`, `base_currency_name`, versions, etc.).
    /// Filled automatically during Init after a successful `emk_BaseCheck`.
    ///
    /// Before the first successful BaseCheck it returns the default with all `None`. Used
    /// for the UI ("connected to Binance Futures, USDT") and for multi-server identification.
    ///
    /// See [`crate::commands::engine_api::ServerInfo`].
    pub fn server_info(&self) -> &crate::commands::engine_api::ServerInfo {
        &self.server_info
    }

    /// Per-account metadata from the last successful `emk_AuthCheck`.
    ///
    /// Filled automatically by the one-time Init sequence.
    /// Returns `None` before a successful AuthCheck, or if a successful response
    /// had a malformed mandatory AuthCheck payload.
    pub fn auth_info(&self) -> Option<&crate::commands::engine_api::AuthCheckResponse> {
        self.auth_info.as_ref()
    }

    /// Set `ServerInfo` manually. Usually not needed — Init does this
    /// automatically. Useful only for internal protocol tests.
    pub fn set_server_info(&mut self, info: crate::commands::engine_api::ServerInfo) {
        // opt #7 parity: cache the base currency name as Arc<str>, so per-packet
        // `from_client` clones a refcount instead of heap-cloning the string (Delphi reads cfg inline).
        self.server_base_currency_name_arc =
            info.base_currency_name.as_deref().map(std::sync::Arc::from);
        self.server_info = info;
    }

    /// Cheap per-packet handle to the base currency name (Arc refcount-bump, no heap-clone).
    pub(crate) fn server_base_currency_name_arc(&self) -> Option<std::sync::Arc<str>> {
        self.server_base_currency_name_arc.clone()
    }

    /// Set per-account AuthCheck metadata manually for custom init flows.
    pub fn set_auth_info(&mut self, info: crate::commands::engine_api::AuthCheckResponse) {
        self.auth_info = Some(info);
    }

    /// Build a trade command context from the active server route.
    ///
    /// This is the recommended path for market-level trade commands such as
    /// [`Self::new_order`], [`Self::move_all_sells`], or position close/split
    /// commands. It uses `ServerInfo::base_currency_code` and
    /// `ServerInfo::exchange_code`, which are filled during Init.
    ///
    /// Existing-order actions should usually use the `*_tracked_order` wrappers
    /// instead, because they derive the route and current status from
    /// `EventDispatcher::orders()` state.
    pub fn trade_ctx(
        &self,
        uid: u64,
    ) -> Result<crate::commands::trade::TradeCtx, TradeContextError> {
        match (
            self.server_info.base_currency_code,
            self.server_info.exchange_code,
        ) {
            (Some(currency), Some(platform)) => Ok(crate::commands::trade::TradeCtx::with_route(
                uid, currency, platform,
            )),
            _ => Err(TradeContextError::from_server_info(&self.server_info)
                .expect("route fields are missing")),
        }
    }

    /// Build a session-derived trade context with a random command UID.
    ///
    /// Use this for client-originated market commands where the UID only needs to
    /// be unique for the outgoing command. For actions on an existing order,
    /// prefer tracked-order wrappers because their UID must be the server order
    /// task id.
    pub fn random_trade_ctx(&self) -> Result<crate::commands::trade::TradeCtx, TradeContextError> {
        self.trade_ctx(rand::random())
    }

    /// Whether the active server route already has the fields required for
    /// market-level trade commands: `exchange_code` and `base_currency_code`,
    /// both learned from `emk_BaseCheck`.
    ///
    /// `Ok(())` means [`Self::trade_ctx`] / [`Self::new_order`] can build a route
    /// now; `Err` names the missing field(s). This is the cheap predicate to gate
    /// a UI trade affordance without constructing and discarding a `TradeCtx`.
    pub fn trade_route_status(&self) -> Result<(), TradeContextError> {
        match TradeContextError::from_server_info(&self.server_info) {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// `true` when [`Self::trade_route_status`] is `Ok` — the session learned the
    /// route fields needed for market-level trade commands.
    pub fn is_ready_to_trade(&self) -> bool {
        self.trade_route_status().is_ok()
    }

    /// Read the streams this session currently has subscribed (orderbooks,
    /// all-trades, market-maker orders).
    ///
    /// This reads the subscription registry — the intent the active library
    /// maintains and replays across reconnect — not the last received packet.
    pub fn active_subscriptions(&self) -> ActiveSubscriptions {
        self.subscription_registry
            .lock()
            .unwrap()
            .active_subscriptions()
    }

    /// Shareable handle to this client's `ServerTimeDelta` (days, f64 in u64-bits).
    ///
    /// Used to link with `EventDispatcher` in a multi-Client architecture:
    /// ```ignore
    /// let mut dispatcher = EventDispatcher::new();
    /// dispatcher.set_server_time_delta_source(client.server_time_delta_handle());
    /// ```
    ///
    /// `MoonClient` and the low-level active pump link this automatically.
    ///
    pub fn server_time_delta_handle(&self) -> Arc<std::sync::atomic::AtomicU64> {
        Arc::clone(&self.server_time_delta_handle)
    }
}
