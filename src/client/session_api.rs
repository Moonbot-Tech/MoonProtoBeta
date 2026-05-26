//! Session identity and trade-route accessors.

use super::*;

impl Client {
    /// Identity сервера (`bot_id`, `exchange_name`, `base_currency_name`, версии и т.д.).
    /// Заполняется автоматически в [`run_init_sequence`] после успешного `emk_BaseCheck`.
    ///
    /// До первого успешного BaseCheck возвращает дефолт со всеми `None`. Используется
    /// для UI ("подключён к Binance Futures, USDT") и для multi-server идентификации.
    ///
    /// См. [`crate::commands::engine_api::ServerInfo`].
    pub fn server_info(&self) -> &crate::commands::engine_api::ServerInfo {
        &self.server_info
    }

    /// Per-account metadata from the last successful `emk_AuthCheck`.
    ///
    /// Filled automatically by [`run_init_sequence`] and [`Self::request_auth_check`].
    /// Returns `None` before a successful AuthCheck, or if a successful response
    /// had a malformed mandatory AuthCheck payload.
    pub fn auth_info(&self) -> Option<&crate::commands::engine_api::AuthCheckResponse> {
        self.auth_info.as_ref()
    }

    /// Установить `ServerInfo` вручную. Обычно не нужно — `run_init_sequence` делает
    /// это автоматически. Полезно если приложение использует свой init pattern
    /// (минуя `run_init_sequence`) и хочет вручную распарсить ответ `api_base_check`.
    pub fn set_server_info(&mut self, info: crate::commands::engine_api::ServerInfo) {
        self.server_info = info;
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
    /// `ServerInfo::exchange_code`, which are filled by `connect_and_init` /
    /// `run_init_sequence`, or by [`Self::request_base_check`].
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

    /// Shareable handle на `ServerTimeDelta` этого клиента (days, f64 в u64-bits).
    ///
    /// Используется для линковки с `EventDispatcher` в multi-Client архитектуре:
    /// ```ignore
    /// let mut dispatcher = EventDispatcher::new();
    /// dispatcher.set_server_time_delta_source(client.server_time_delta_handle());
    /// ```
    ///
    /// `MoonClient` and the low-level active pump link this automatically.
    ///
    /// См. `DEVIATION.md #23` (single-Client → multi-Client refactor).
    pub fn server_time_delta_handle(&self) -> Arc<std::sync::atomic::AtomicU64> {
        Arc::clone(&self.server_time_delta_handle)
    }
}
