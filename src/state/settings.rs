//! Settings sync state — snapshot последних UI-настроек, полученных от сервера.
//!
//! Источник Delphi: `MoonProto/MoonProtoUIStruct.pas`. По модели DEVIATION #5
//! (observer-модель) — мы лишь храним самый свежий snapshot для каждой подкоманды.
//! Прикладная логика (применение настроек к engine/UI) — задача потребителя.
//!
//! ## Что отслеживается
//! - `ClientSettings` (CmdId=1): полный snapshot UI настроек (один global slot).
//! - `LevManage` (CmdId=9): автоматическое управление плечом (один global slot).
//! - `MMOrdersSubscribe` (CmdId=5): подписка на MM детекцию (bool).
//! - `SwitchDex` (CmdId=13): текущий выбранный DEX.
//! - `SwitchSpot` (CmdId=14): 0=Crypto / 1=Predict.
//! - `ArbActivateNotify` (CmdId=12): TDateTime когда истекает Arb лицензия.
//!
//! Action-команды (StratStartStop, ResetProfit, TriggerManage, EmuTrades,
//! UpdateVersion, NewMarketNotify, SettingsRequest) — пробрасываются как `SettingsEvent`
//! без фиксации в state.

use crate::commands::ui::{
    UICommand, ClientSettingsCommand, LevManage,
    StratStartStop, StratStartStopV2, UpdateVersion,
    EmuTrades, TriggerManage, ResetProfit, ArbActivateNotify, SwitchDex, SwitchSpot,
};

#[derive(Debug, Clone, Default)]
pub struct SettingsState {
    pub client_settings: Option<ClientSettingsCommand>,
    pub lev_manage:      Option<LevManage>,
    pub mm_orders_subscribed: bool,
    pub current_dex:     Option<String>,
    pub current_spot:    Option<u8>,
    /// `TDateTime` (Delphi double): момент когда истекает Arb лицензия.
    pub arb_valid_until: Option<f64>,
}

#[derive(Debug, Clone)]
pub enum SettingsEvent {
    /// Получен новый полный snapshot настроек.
    ClientSettingsUpdated,
    /// Изменился LevManage snapshot.
    LevManageUpdated,
    /// Изменилась подписка на MM ордера.
    MMSubscribeChanged(bool),
    /// Сервер запрашивает повторную отправку текущих настроек (CmdId=2).
    SettingsRequested { uid: u64 },
    /// Запрос на старт/стоп всех активных стратегий (v1).
    StratStartStopRequested(StratStartStop),
    /// Запрос на старт/стоп с дельтой checked (v2).
    StratStartStopV2Requested(StratStartStopV2),
    /// Уведомление об обновлении версии.
    VersionUpdate(UpdateVersion),
    /// Серия эмулированных тиков (Sliced).
    EmuTrades(EmuTrades),
    /// Появился новый маркет на бирже.
    NewMarketAvailable { uid: u64 },
    /// Изменения hotkey-триггеров.
    TriggerManaged(TriggerManage),
    /// Запрос на сброс профита (kind: 0=Cur, 1=All).
    ResetProfitRequested(ResetProfit),
    /// Arb лицензия активирована/обновлена.
    ArbActivated(ArbActivateNotify),
    /// Сменился текущий DEX.
    DexSwitched(SwitchDex),
    /// Сменился текущий spot (0=Crypto, 1=Predict).
    SpotSwitched(SwitchSpot),
    /// Неизвестная подкоманда (forward-compat).
    Unknown { cmd_id: u8, uid: u64 },
}

impl SettingsState {
    pub fn new() -> Self { Self::default() }

    /// Применить входящую UI-команду к state. Возвращает event для прикладного слоя.
    pub fn apply(&mut self, cmd: UICommand) -> SettingsEvent {
        match cmd {
            UICommand::ClientSettings(c) => {
                self.client_settings = Some(c);
                SettingsEvent::ClientSettingsUpdated
            }
            UICommand::SettingsRequest { uid } => SettingsEvent::SettingsRequested { uid },

            UICommand::StratStartStop(s)   => SettingsEvent::StratStartStopRequested(s),
            UICommand::StratStartStopV2(s) => SettingsEvent::StratStartStopV2Requested(s),

            UICommand::MMOrdersSubscribe(m) => {
                self.mm_orders_subscribed = m.subscribe;
                SettingsEvent::MMSubscribeChanged(m.subscribe)
            }

            UICommand::UpdateVersion(u) => SettingsEvent::VersionUpdate(u),

            UICommand::EmuTrades(e) => SettingsEvent::EmuTrades(e),

            UICommand::NewMarketNotify(n) => SettingsEvent::NewMarketAvailable { uid: n.uid },

            UICommand::LevManage(l) => {
                self.lev_manage = Some(l);
                SettingsEvent::LevManageUpdated
            }

            UICommand::TriggerManage(t) => SettingsEvent::TriggerManaged(t),

            UICommand::ResetProfit(r) => SettingsEvent::ResetProfitRequested(r),

            UICommand::ArbActivateNotify(a) => {
                self.arb_valid_until = Some(a.arb_valid);
                SettingsEvent::ArbActivated(a)
            }

            UICommand::SwitchDex(s) => {
                self.current_dex = Some(s.dex_name.clone());
                SettingsEvent::DexSwitched(s)
            }

            UICommand::SwitchSpot(s) => {
                self.current_spot = Some(s.spot_index);
                SettingsEvent::SpotSwitched(s)
            }

            UICommand::Unknown { cmd_id, uid } => SettingsEvent::Unknown { cmd_id, uid },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::ui::*;

    #[test]
    fn client_settings_stores_snapshot() {
        let mut st = SettingsState::new();
        let cmd = ClientSettingsCommand {
            uid: 1,
            x_sell: 50,
            x_sell_scalp: 10,
            x_tmode: false,
            fixed_sell_mode: false,
            fixed_sell_price: 0.0,
            price_drop_level: 0.0,
            trailing_drop: 0.0,
            g_take_profit: 0.0,
            use_g_take_profit: false,
            unused_spread: 0,
            panic_if_price_drop: false,
            emu_mode: false,
            buy_iceberg: false,
            sell_iceberg: false,
            sign_orders: true,
            coins_black_list_text: String::new(),
            use_coins_black_list: false,
            temp_bl_symbols: vec![],
            temp_bl_times: vec![],
            use_manual_strategy: false,
            manual_strategy_id: 0,
            free_position_check: false,
            vol_drop_level: 0,
            use_stop_market: false,
            as_cfg:  vec![0; AS_CFG_SIZE],
            as_cfg2: vec![0; AS_CFG2_SIZE],
            s_price: [0.0; 6],
            sb_num: 0,
            join_sell_kind: 0,
            arb_config: ArbConfigCompact::default(),
        };
        let ev = st.apply(UICommand::ClientSettings(cmd));
        assert!(matches!(ev, SettingsEvent::ClientSettingsUpdated));
        assert_eq!(st.client_settings.as_ref().unwrap().x_sell, 50);
    }

    #[test]
    fn mm_orders_subscribe_changes_state() {
        let mut st = SettingsState::new();
        assert!(!st.mm_orders_subscribed);
        let ev = st.apply(UICommand::MMOrdersSubscribe(MMOrdersSubscribe { uid: 1, subscribe: true }));
        assert!(matches!(ev, SettingsEvent::MMSubscribeChanged(true)));
        assert!(st.mm_orders_subscribed);

        let _ = st.apply(UICommand::MMOrdersSubscribe(MMOrdersSubscribe { uid: 2, subscribe: false }));
        assert!(!st.mm_orders_subscribed);
    }

    #[test]
    fn dex_switch_updates_current() {
        let mut st = SettingsState::new();
        assert!(st.current_dex.is_none());
        let ev = st.apply(UICommand::SwitchDex(SwitchDex { uid: 1, dex_name: "Uni".to_string() }));
        match ev {
            SettingsEvent::DexSwitched(s) => assert_eq!(s.dex_name, "Uni"),
            _ => panic!("wrong event"),
        }
        assert_eq!(st.current_dex.as_deref(), Some("Uni"));
    }

    #[test]
    fn spot_switch_updates_index() {
        let mut st = SettingsState::new();
        let _ = st.apply(UICommand::SwitchSpot(SwitchSpot { uid: 1, spot_index: 1 }));
        assert_eq!(st.current_spot, Some(1));
    }

    #[test]
    fn arb_activate_stores_valid_until() {
        let mut st = SettingsState::new();
        let _ = st.apply(UICommand::ArbActivateNotify(ArbActivateNotify { uid: 1, arb_valid: 45000.5 }));
        assert_eq!(st.arb_valid_until, Some(45000.5));
    }

    #[test]
    fn lev_manage_stores_snapshot() {
        let mut st = SettingsState::new();
        let lm = LevManage {
            uid: 1, cmd_ver: 1,
            auto_max_order: true, auto_lev_up: false,
            auto_isolated: true, auto_cross: false, auto_fix_lev: true,
            fix_lev: 10, tlg_report: false,
            lev_control: "BTC".to_string(),
        };
        let _ = st.apply(UICommand::LevManage(lm));
        assert!(st.lev_manage.is_some());
        assert_eq!(st.lev_manage.as_ref().unwrap().fix_lev, 10);
    }

    #[test]
    fn action_commands_pass_through_without_state() {
        let mut st = SettingsState::new();
        let ev = st.apply(UICommand::StratStartStop(StratStartStop { uid: 1, is_start: true }));
        assert!(matches!(ev, SettingsEvent::StratStartStopRequested(_)));
        // state не меняется
        assert!(st.client_settings.is_none());
    }
}
