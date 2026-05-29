use super::*;

impl Client {
    // ====================================================================
    //  High-level UI wrappers (Command::UI, encrypted=true)
    //  Покрывают MClient.SendUICmd(T*Command.Create(...)) семантику Delphi.
    //  UID авто-генерируется через rand::random() — потребитель не передаёт.
    //  Priority/MaxRetries/UKey — из атрибутов соответствующих Delphi-классов.
    //  Аудит docs_api B-01: было 14 build_* функций без Client-обёрток.
    // ====================================================================

    #[doc(hidden)]
    /// Send `TClientSettingsCommand` (UI CmdId=1, Sliced,
    /// `UK_BaseUISettings`).
    ///
    /// This sends a full client-settings snapshot and replaces any older
    /// pending settings packet with the same UKey slot.
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

    #[doc(hidden)]
    /// Send `TSettingsRequest` (UI CmdId=2, High) to request current settings.
    pub fn ui_settings_request(&self) {
        let raw = crate::commands::ui::build_settings_request(rand::random());
        self.send_domain_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    #[doc(hidden)]
    /// Send `TStratStartStopCommand` (UI CmdId=3, High) to start or stop all
    /// strategies.
    pub fn ui_strat_start_stop(&self, is_start: bool) {
        let raw = crate::commands::ui::build_strat_start_stop(rand::random(), is_start);
        self.send_domain_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    #[doc(hidden)]
    /// Send `TStratStartStopCommandV2` (UI CmdId=4, High) with an explicit
    /// checked delta.
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

    #[doc(hidden)]
    /// Send `TMMOrdersSubscribeCommand` (UI CmdId=5, High,
    /// `UK_TurnMMDetection`) to set the market-maker orders subscription flag.
    pub fn ui_mm_subscribe(&self, subscribe: bool) {
        self.sender().ui_mm_subscribe(subscribe);
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
    pub fn ui_update_version(&self, version_name: &str, is_release: bool) {
        let raw =
            crate::commands::ui::build_update_version(rand::random(), version_name, is_release);
        if self.send_domain_cmd(raw, Command::UI, SendPriority::High, true, 3) {
            self.mark_server_update_sent();
        }
    }

    #[doc(hidden)]
    /// Send `TEmuTradesCommand` (UI CmdId=7, Sliced) with emulated trades for a
    /// test market.
    pub fn ui_emu_trades(
        &self,
        m_index: u16,
        base_time: f64,
        points: &[crate::commands::ui::EmuTradePoint],
    ) {
        let raw = crate::commands::ui::build_emu_trades(rand::random(), m_index, base_time, points);
        self.send_domain_cmd(raw, Command::UI, SendPriority::Sliced, true, 6);
    }

    #[doc(hidden)]
    /// Send `TLevManageCommand` (UI CmdId=9, Sliced,
    /// `UK_LevManageSettings`) with leverage-management settings.
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

    #[doc(hidden)]
    /// Send `TTriggerManageCommand` (UI CmdId=10, Sliced) for batch trigger
    /// management over all markets or selected market/key pairs.
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

    #[doc(hidden)]
    /// Send `TResetProfitCommand` (UI CmdId=11, High) to reset profit counters.
    pub fn ui_reset_profit(&self, kind: u8) {
        let raw = crate::commands::ui::build_reset_profit(rand::random(), kind);
        self.send_domain_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    #[doc(hidden)]
    /// Send `TArbActivateNotify` (UI CmdId=12, High) with an arbitration-valid
    /// timestamp.
    pub fn ui_arb_activate_notify(&self, arb_valid: f64) {
        let raw = crate::commands::ui::build_arb_activate_notify(rand::random(), arb_valid);
        self.send_domain_cmd(raw, Command::UI, SendPriority::High, true, 3);
    }

    #[doc(hidden)]
    /// Send `TSwitchDexCommand` (UI CmdId=13, High, `UK_DexSwitch`).
    ///
    /// The DEX name is truncated to the Delphi 15-byte short-string payload.
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

    #[doc(hidden)]
    /// Send `TSwitchSpotCommand` (UI CmdId=14, High, `UK_SpotSwitch`) to select
    /// the spot mode.
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
}
