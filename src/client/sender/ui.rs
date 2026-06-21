//! `ClientSender` UI command helpers.
#![allow(dead_code)]

use super::*;

impl ClientSender {
    #[doc(hidden)]
    /// Mark Delphi `ServerUpdateSent` from a thread-safe sender.
    ///
    /// Call this when sending raw UI update/switch payloads through
    /// [`Self::send_cmd`] rather than the typed wrappers below.
    pub(crate) fn mark_server_update_sent(&self) {
        self.shared
            .server_update_sent
            .store(true, Ordering::Relaxed);
    }

    #[doc(hidden)]
    /// Send `TClientSettingsCommand`.
    pub(crate) fn ui_send_settings(&self, settings: &crate::commands::ui::ClientSettingsCommand) {
        let mut wire_settings = settings.clone();
        wire_settings.uid = rand::random();
        let raw = crate::commands::ui::build_client_settings(&wire_settings);
        self.send_typed_domain_cmd(raw, Command::UI);
    }

    #[doc(hidden)]
    /// Send `TSettingsRequest`.
    pub(crate) fn ui_settings_request(&self) {
        let raw = crate::commands::ui::build_settings_request(rand::random());
        self.send_typed_domain_cmd(raw, Command::UI);
    }

    #[doc(hidden)]
    /// Send `TStratStartStopCommand`.
    pub(crate) fn ui_strat_start_stop(&self, is_start: bool) {
        let raw = crate::commands::ui::build_strat_start_stop(rand::random(), is_start);
        self.send_typed_domain_cmd(raw, Command::UI);
    }

    #[doc(hidden)]
    /// Send `TStratStartStopCommandV2` with an explicit checked delta.
    ///
    /// Regular active-library callers should prefer
    /// `EventDispatcher::ui_strat_start_stop_v2`, which builds the delta from
    /// owned strategy state like Delphi `TStratStartStopCommandV2.Create`.
    pub(crate) fn ui_strat_start_stop_v2(
        &self,
        is_start: bool,
        items: &[crate::commands::strat::StratCheckedItem],
    ) {
        let raw = crate::commands::ui::build_strat_start_stop_v2(rand::random(), is_start, items);
        self.send_typed_domain_cmd(raw, Command::UI);
    }

    #[doc(hidden)]
    /// Send `TMMOrdersSubscribeCommand`.
    pub(crate) fn ui_mm_subscribe(&self, subscribe: bool) {
        if let Err(e) = self.try_ui_mm_subscribe(subscribe) {
            log::warn!(target: "moonproto::client",
                "ui_mm_subscribe({subscribe}) dropped: {e}");
        }
    }

    #[doc(hidden)]
    /// Fallible `TMMOrdersSubscribeCommand`.
    pub(crate) fn try_ui_mm_subscribe(&self, subscribe: bool) -> Result<(), SubscribeError> {
        if !self.shared.app_queue_alive.load(Ordering::Relaxed) {
            return Err(SubscribeError::Disconnected);
        }
        {
            let mut registry = self.shared.subscription_registry.lock();
            registry.mm_orders_sub = Some(subscribe);
        }
        let uid = rand::random();
        let raw = crate::commands::ui::build_mm_orders_subscribe(uid, subscribe);
        self.try_send_typed_domain_cmd(raw, Command::UI)
    }

    #[doc(hidden)]
    /// Send `TUpdateVersionCommand` and mark Delphi `ServerUpdateSent`.
    pub(crate) fn ui_update_version(&self, version_name: &str, is_release: bool) {
        let raw =
            crate::commands::ui::build_update_version(rand::random(), version_name, is_release);
        if self.send_typed_domain_cmd(raw, Command::UI) {
            self.mark_server_update_sent();
        }
    }

    #[doc(hidden)]
    /// Send `TEmuTradesCommand`.
    pub(crate) fn ui_emu_trades(
        &self,
        m_index: u16,
        base_time: f64,
        points: &[crate::commands::ui::EmuTradePoint],
    ) {
        let raw = crate::commands::ui::build_emu_trades(rand::random(), m_index, base_time, points);
        self.send_typed_domain_cmd(raw, Command::UI);
    }

    #[doc(hidden)]
    /// Send `TLevManageCommand`.
    pub(crate) fn ui_lev_manage(&self, cmd: &crate::commands::ui::LevManage) {
        let uid: u64 = rand::random();
        let raw = crate::commands::ui::build_lev_manage(uid, cmd);
        self.send_typed_domain_cmd(raw, Command::UI);
    }

    #[doc(hidden)]
    /// Send `TTriggerManageCommand`.
    pub(crate) fn ui_trigger_manage(
        &self,
        action: u8,
        all_markets: bool,
        markets: &[u16],
        keys: &[u16],
    ) {
        let raw = crate::commands::ui::build_trigger_manage(
            rand::random(),
            action,
            all_markets,
            markets,
            keys,
        );
        self.send_typed_domain_cmd(raw, Command::UI);
    }

    #[doc(hidden)]
    /// Send `TResetProfitCommand`.
    pub(crate) fn ui_reset_profit(&self, kind: u8) {
        let raw = crate::commands::ui::build_reset_profit(rand::random(), kind);
        self.send_typed_domain_cmd(raw, Command::UI);
    }

    #[doc(hidden)]
    /// Send `TArbActivateNotify`.
    pub(crate) fn ui_arb_activate_notify(&self, arb_valid: f64) {
        let raw = crate::commands::ui::build_arb_activate_notify(rand::random(), arb_valid);
        self.send_typed_domain_cmd(raw, Command::UI);
    }

    #[doc(hidden)]
    /// Send `TSwitchDexCommand` and mark Delphi `ServerUpdateSent`.
    pub(crate) fn ui_switch_dex(&self, dex_name: &str) {
        let uid = rand::random();
        let raw = crate::commands::ui::build_switch_dex(uid, dex_name);
        if self.send_typed_domain_cmd(raw, Command::UI) {
            self.mark_server_update_sent();
        }
    }

    #[doc(hidden)]
    /// Send `TSwitchSpotCommand` and mark Delphi `ServerUpdateSent`.
    pub(crate) fn ui_switch_spot(&self, spot_index: u8) {
        let uid = rand::random();
        let raw = crate::commands::ui::build_switch_spot(uid, spot_index);
        if self.send_typed_domain_cmd(raw, Command::UI) {
            self.mark_server_update_sent();
        }
    }

    #[doc(hidden)]
    /// Send `TOrdersHistoryRequestCommand`.
    pub(crate) fn ui_orders_history_request(&self, market_name: &str) {
        let raw = crate::commands::ui::build_orders_history_request(rand::random(), market_name);
        self.send_typed_domain_cmd(raw, Command::UI);
    }

    #[doc(hidden)]
    /// Send `TRestartNowCommand`.
    pub(crate) fn ui_restart_now(&self) {
        let raw = crate::commands::ui::build_restart_now(rand::random());
        self.send_typed_domain_cmd(raw, Command::UI);
    }
}
