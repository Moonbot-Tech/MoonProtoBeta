use super::*;

impl Client {
    // ====================================================================
    //  High-level UI wrappers (Command::UI, encrypted=true)
    //  Cover the Delphi MClient.SendUICmd(T*Command.Create(...)) semantics.
    //  UID is auto-generated via rand::random() — the consumer does not pass it.
    //  Priority/MaxRetries/UKey — from the attributes of the matching Delphi classes.
    // ====================================================================

    #[doc(hidden)]
    /// Send `TClientSettingsCommand` (UI CmdId=1, Sliced,
    /// `UK_BaseUISettings`).
    ///
    /// This sends a full client-settings snapshot and replaces any older
    /// pending settings packet with the same UKey slot.
    pub(crate) fn ui_send_settings(&self, settings: &crate::commands::ui::ClientSettingsCommand) {
        let mut wire_settings = settings.clone();
        wire_settings.uid = rand::random();
        let raw = crate::commands::ui::build_client_settings(&wire_settings);
        self.send_typed_domain_cmd(raw, Command::UI);
    }

    #[doc(hidden)]
    /// Send `TSettingsRequest` (UI CmdId=2, High) to request current settings.
    pub(crate) fn ui_settings_request(&self) {
        let raw = crate::commands::ui::build_settings_request(rand::random());
        self.send_typed_domain_cmd(raw, Command::UI);
    }

    #[doc(hidden)]
    /// Send `TStratStartStopCommandV2` (UI CmdId=4, High) with an explicit
    /// checked delta.
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
    /// Send `TMMOrdersSubscribeCommand` (UI CmdId=5, High,
    /// `UK_TurnMMDetection`) to set the market-maker orders subscription flag.
    pub(crate) fn ui_mm_subscribe(&self, subscribe: bool) {
        self.sender_internal().ui_mm_subscribe(subscribe);
    }

    #[doc(hidden)]
    /// `TUpdateVersionCommand` (UI CmdId=6, High) — request a MoonBot version update.
    ///
    /// Delphi uses this from the update UI:
    /// - release button sends `VersionName=""`, `IsRelease=true`;
    /// - beta/test install command sends the requested version name and release flag.
    ///
    /// The server handles the command and broadcasts the same UI command back to
    /// clients. Delphi clients then run their local updater in
    /// `HandleRemoteUpdateCommand`; this Rust wrapper only sends the protocol
    /// command and marks Delphi `ServerUpdateSent` so the next init uses the
    /// update-aware BaseCheck retry path.
    pub(crate) fn ui_update_version(&self, version_name: &str, is_release: bool) {
        let raw =
            crate::commands::ui::build_update_version(rand::random(), version_name, is_release);
        if self.send_typed_domain_cmd(raw, Command::UI) {
            self.mark_server_update_sent();
        }
    }

    #[doc(hidden)]
    /// Send `TEmuTradesCommand` (UI CmdId=7, Sliced) with emulated trades for a
    /// test market.
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
    /// Send `TLevManageCommand` (UI CmdId=9, Sliced,
    /// `UK_LevManageSettings`) with leverage-management settings.
    pub(crate) fn ui_lev_manage(&self, cmd: &crate::commands::ui::LevManage) {
        let uid: u64 = rand::random();
        let raw = crate::commands::ui::build_lev_manage(uid, cmd);
        self.send_typed_domain_cmd(raw, Command::UI);
    }

    #[doc(hidden)]
    /// Send `TTriggerManageCommand` (UI CmdId=10, Sliced) for batch trigger
    /// management over all markets or selected market/key pairs.
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
    /// Send `TResetProfitCommand` (UI CmdId=11, High) to reset profit counters.
    pub(crate) fn ui_reset_profit(&self, kind: u8) {
        let raw = crate::commands::ui::build_reset_profit(rand::random(), kind);
        self.send_typed_domain_cmd(raw, Command::UI);
    }

    #[doc(hidden)]
    /// Send `TArbActivateNotify` (UI CmdId=12, High) with an arbitration-valid
    /// timestamp.
    pub(crate) fn ui_arb_activate_notify(&self, arb_valid: f64) {
        let raw = crate::commands::ui::build_arb_activate_notify(rand::random(), arb_valid);
        self.send_typed_domain_cmd(raw, Command::UI);
    }

    #[doc(hidden)]
    /// Send `TSwitchDexCommand` (UI CmdId=13, High, `UK_DexSwitch`).
    ///
    /// The DEX name is truncated to the Delphi 15-byte short-string payload.
    pub(crate) fn ui_switch_dex(&self, dex_name: &str) {
        let uid = rand::random();
        let raw = crate::commands::ui::build_switch_dex(uid, dex_name);
        if self.send_typed_domain_cmd(raw, Command::UI) {
            self.mark_server_update_sent();
        }
    }

    #[doc(hidden)]
    /// Send `TSwitchSpotCommand` (UI CmdId=14, High, `UK_SpotSwitch`) to select
    /// the spot mode.
    pub(crate) fn ui_switch_spot(&self, spot_index: u8) {
        let uid = rand::random();
        let raw = crate::commands::ui::build_switch_spot(uid, spot_index);
        if self.send_typed_domain_cmd(raw, Command::UI) {
            self.mark_server_update_sent();
        }
    }

    #[doc(hidden)]
    /// Send `TAlertObjectCommand` (UI CmdId=15, Sliced).
    pub(crate) fn ui_alert_object(&self, cmd: &crate::commands::ui::AlertObjectCommand) {
        let raw = crate::commands::ui::build_alert_object(cmd);
        self.send_typed_domain_cmd(raw, Command::UI);
    }

    #[doc(hidden)]
    /// Send `TAlertSnapshotRequest` (UI CmdId=16, High).
    pub(crate) fn ui_alert_snapshot_request(&self) {
        let raw = crate::commands::ui::build_alert_snapshot_request(rand::random());
        self.send_typed_domain_cmd(raw, Command::UI);
    }

    #[doc(hidden)]
    /// Send `TChartTextStateCommand` (UI CmdId=17, High,
    /// `UK_ChartTextState`).
    pub(crate) fn ui_chart_text_state(&self, cmd: &crate::commands::ui::ChartTextStateCommand) {
        let raw = crate::commands::ui::build_chart_text_state(cmd);
        self.send_typed_domain_cmd(raw, Command::UI);
    }

    #[doc(hidden)]
    /// Send `TOrdersHistoryRequestCommand` (UI CmdId=19, High).
    pub(crate) fn ui_orders_history_request(&self, market_name: &str) {
        let raw = crate::commands::ui::build_orders_history_request(rand::random(), market_name);
        self.send_typed_domain_cmd(raw, Command::UI);
    }

    #[doc(hidden)]
    /// Send `TRestartNowCommand` (UI CmdId=21, High).
    pub(crate) fn ui_restart_now(&self) {
        let raw = crate::commands::ui::build_restart_now(rand::random());
        self.send_typed_domain_cmd(raw, Command::UI);
    }

    #[doc(hidden)]
    /// Send `TKernelLicenseStateRequest` (UI CmdId=23, High).
    pub(crate) fn ui_kernel_license_state_request(&self, activate_feature: i32) {
        let raw = crate::commands::ui::build_kernel_license_state_request(
            rand::random(),
            activate_feature,
        );
        self.send_typed_domain_cmd(raw, Command::UI);
    }
}
