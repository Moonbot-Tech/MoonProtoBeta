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
    pub(crate) fn server_info(&self) -> &crate::commands::engine_api::ServerInfo {
        &self.identity.server_info
    }

    /// Per-account metadata from the last successful `emk_AuthCheck`.
    ///
    /// Filled automatically by the one-time Init sequence.
    /// Returns `None` before a successful AuthCheck, or if a successful response
    /// had a malformed mandatory AuthCheck payload.
    pub(crate) fn auth_info(&self) -> Option<&crate::commands::engine_api::AuthCheckResponse> {
        self.identity.auth_info.as_ref()
    }

    /// Set `ServerInfo` manually. Usually not needed — Init does this
    /// automatically. Useful only for internal protocol tests.
    pub(crate) fn set_server_info(&mut self, info: crate::commands::engine_api::ServerInfo) {
        // opt #7 parity: cache the base currency name as Arc<str>, so per-packet
        // `from_client` clones a refcount instead of heap-cloning the string (Delphi reads cfg inline).
        self.identity.server_base_currency_name_arc =
            info.base_currency_name.as_deref().map(std::sync::Arc::from);
        self.identity.server_info = info;
    }

    /// Cheap per-packet handle to the base currency name (Arc refcount-bump, no heap-clone).
    pub(crate) fn server_base_currency_name_arc(&self) -> Option<std::sync::Arc<str>> {
        self.identity.server_base_currency_name_arc.clone()
    }

    /// Set per-account AuthCheck metadata manually for custom init flows.
    pub(crate) fn set_auth_info(&mut self, info: crate::commands::engine_api::AuthCheckResponse) {
        self.identity.auth_info = Some(info);
    }

    /// Build the legacy market-command context from the active server route.
    ///
    /// Canonical v4 order actions serialize a market name or server order UID
    /// and do not use this record. It remains for the legacy `Penalty` packet,
    /// whose market header still carries BaseCheck currency/exchange bytes.
    pub(crate) fn trade_ctx(
        &self,
        uid: u64,
    ) -> Result<crate::commands::trade::TradeCtx, TradeContextError> {
        match (
            self.identity.server_info.base_currency_code,
            self.identity.server_info.exchange_code,
        ) {
            (Some(currency), Some(platform)) => Ok(crate::commands::trade::TradeCtx::with_route(
                uid, currency, platform,
            )),
            _ => Err(
                TradeContextError::from_server_info(&self.identity.server_info)
                    .expect("route fields are missing"),
            ),
        }
    }

    /// Build a legacy market-command context with a random command UID.
    pub(crate) fn random_trade_ctx(
        &self,
    ) -> Result<crate::commands::trade::TradeCtx, TradeContextError> {
        self.trade_ctx(rand::random())
    }

    /// Whether the active server route already has the fields required for the
    /// legacy `Penalty` market header.
    ///
    /// `Err` names the missing BaseCheck field(s).
    #[cfg(test)]
    pub(crate) fn trade_route_status(&self) -> Result<(), TradeContextError> {
        match TradeContextError::from_server_info(&self.identity.server_info) {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// `true` when the session learned the legacy market-header route fields.
    #[cfg(test)]
    pub(crate) fn is_ready_to_trade(&self) -> bool {
        self.trade_route_status().is_ok()
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
    pub(crate) fn server_time_delta_handle(&self) -> Arc<std::sync::atomic::AtomicU64> {
        Arc::clone(&self.server_time_delta_handle)
    }
}
