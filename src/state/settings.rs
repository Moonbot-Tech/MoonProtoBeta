//! Settings sync state — snapshot последних UI-настроек, полученных от сервера.
//!
//! Источник Delphi: `MoonProto/MoonProtoUIStruct.pas`. По observer-модели мы
//! лишь храним самый свежий snapshot для каждой подкоманды.
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
//! UpdateVersion, SettingsRequest) — пробрасываются как `SettingsEvent` без
//! фиксации в state. `NewMarketNotify` является internal Active Lib trigger:
//! dispatcher uses it to force listing refresh, and user code gets a market
//! event only after a refreshed list actually inserts new markets.

use crate::commands::ui::{
    ArbActivateNotify, ClientSettingsCommand, EmuTrades, LevManage, ResetProfit, StratStartStop,
    StratStartStopV2, SwitchDex, SwitchSpot, TriggerManage, UICommand, UpdateVersion,
};
use crate::time::DelphiTime;

/// Sync state клиентских настроек — обновляется через `apply(UICommand)`.
///
/// Settings — это snapshot-state, не аккумулирующая история. Каждый принятый
/// `TClientSettingsCommand` от сервера полностью заменяет `client_settings`.
#[derive(Debug, Clone, Default)]
pub struct SettingsState {
    /// Последний полученный snapshot настроек клиента (None до первого `SettingsRequest` ответа).
    pub client_settings: Option<ClientSettingsCommand>,
    /// Текущий `cfg`-эквивалент для append-only soft-read хвоста `TClientSettingsCommand`.
    ///
    /// Delphi `CreateFromStream` подставляет значения из текущего `cfg`, если
    /// старый packet не содержит хвостовые поля. После каждого полного settings
    /// snapshot этот fallback обновляется автоматически; до первого snapshot
    /// application может заполнить его через
    /// [`SettingsState::set_client_settings_fallback`].
    pub client_settings_fallback: ClientSettingsCommand,
    /// Текущие настройки leverage management (None если ни одного `TLevManageCommand` не получено).
    pub lev_manage: Option<LevManage>,
    /// Активна ли подписка на market-maker ордера (изменяется через `ui_mm_subscribe`).
    pub mm_orders_subscribed: bool,
    /// Имя текущего выбранного DEX (None если не выбран / не futures-режим).
    pub current_dex: Option<String>,
    /// Индекс текущего spot (None если не выбран). Конкретные значения биржа-specific.
    pub current_spot: Option<u8>,
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
    /// Remote update command (UI CmdId=6): version name + release/test flag.
    ///
    /// Delphi clients treat this as a request to run their local updater. The
    /// Rust state layer only surfaces the wire command; application code decides
    /// whether/how to update itself.
    VersionUpdate(UpdateVersion),
    /// Серия эмулированных тиков (Sliced).
    EmuTrades(EmuTrades),
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
    /// Команда с будущей версией. Low-level state API can surface it, while
    /// `EventDispatcher` skips it like Delphi registry `FSkipped`.
    Skipped { cmd_id: u8, uid: u64, ver: u16 },
    /// Неизвестная подкоманда (forward-compat).
    Unknown { cmd_id: u8, uid: u64 },
}

impl SettingsState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn arb_valid_until_time(&self) -> Option<DelphiTime> {
        self.arb_valid_until.map(DelphiTime::from_days)
    }

    /// Seed Delphi `cfg` fallback used while parsing old `TClientSettingsCommand`
    /// payloads with missing append-only tail fields.
    pub fn set_client_settings_fallback(&mut self, fallback: ClientSettingsCommand) {
        self.client_settings_fallback = fallback;
    }

    pub(crate) fn client_settings_parse_fallback(&self) -> &ClientSettingsCommand {
        &self.client_settings_fallback
    }

    /// Применить входящую UI-команду к state.
    ///
    /// Returns `None` for internal commands that have no public settings event.
    pub fn apply(&mut self, cmd: UICommand) -> Option<SettingsEvent> {
        match cmd {
            UICommand::ClientSettings(c) => {
                let settings = *c;
                self.client_settings_fallback = settings.clone();
                self.client_settings = Some(settings);
                Some(SettingsEvent::ClientSettingsUpdated)
            }
            UICommand::SettingsRequest { uid } => Some(SettingsEvent::SettingsRequested { uid }),

            UICommand::StratStartStop(s) => Some(SettingsEvent::StratStartStopRequested(s)),
            UICommand::StratStartStopV2(s) => Some(SettingsEvent::StratStartStopV2Requested(s)),

            UICommand::MMOrdersSubscribe(m) => {
                self.mm_orders_subscribed = m.subscribe;
                Some(SettingsEvent::MMSubscribeChanged(m.subscribe))
            }

            UICommand::UpdateVersion(u) => Some(SettingsEvent::VersionUpdate(u)),

            UICommand::EmuTrades(e) => Some(SettingsEvent::EmuTrades(e)),

            UICommand::NewMarketNotify(_) => None,

            UICommand::LevManage(l) => {
                self.lev_manage = Some(l);
                Some(SettingsEvent::LevManageUpdated)
            }

            UICommand::TriggerManage(t) => Some(SettingsEvent::TriggerManaged(t)),

            UICommand::ResetProfit(r) => Some(SettingsEvent::ResetProfitRequested(r)),

            UICommand::ArbActivateNotify(a) => {
                self.arb_valid_until = Some(a.arb_valid);
                Some(SettingsEvent::ArbActivated(a))
            }

            UICommand::SwitchDex(s) => {
                self.current_dex = Some(s.dex_name.clone());
                Some(SettingsEvent::DexSwitched(s))
            }

            UICommand::SwitchSpot(s) => {
                self.current_spot = Some(s.spot_index);
                Some(SettingsEvent::SpotSwitched(s))
            }

            UICommand::Skipped { cmd_id, uid, ver } => {
                Some(SettingsEvent::Skipped { cmd_id, uid, ver })
            }

            UICommand::Unknown { cmd_id, uid } => Some(SettingsEvent::Unknown { cmd_id, uid }),
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
            as_cfg: vec![0; AS_CFG_SIZE],
            as_cfg2: vec![0; AS_CFG2_SIZE],
            s_price: [0.0; 6],
            sb_num: 0,
            join_sell_kind: 0,
            arb_config: ArbConfigCompact::default(),
        };
        let ev = st.apply(UICommand::ClientSettings(Box::new(cmd)));
        assert!(matches!(ev, Some(SettingsEvent::ClientSettingsUpdated)));
        assert_eq!(st.client_settings.as_ref().unwrap().x_sell, 50);
    }

    #[test]
    fn mm_orders_subscribe_changes_state() {
        let mut st = SettingsState::new();
        assert!(!st.mm_orders_subscribed);
        let ev = st.apply(UICommand::MMOrdersSubscribe(MMOrdersSubscribe {
            uid: 1,
            subscribe: true,
        }));
        assert!(matches!(ev, Some(SettingsEvent::MMSubscribeChanged(true))));
        assert!(st.mm_orders_subscribed);

        let _ = st.apply(UICommand::MMOrdersSubscribe(MMOrdersSubscribe {
            uid: 2,
            subscribe: false,
        }));
        assert!(!st.mm_orders_subscribed);
    }

    #[test]
    fn dex_switch_updates_current() {
        let mut st = SettingsState::new();
        assert!(st.current_dex.is_none());
        let ev = st.apply(UICommand::SwitchDex(SwitchDex {
            uid: 1,
            dex_name: "Uni".to_string(),
        }));
        match ev {
            Some(SettingsEvent::DexSwitched(s)) => assert_eq!(s.dex_name, "Uni"),
            _ => panic!("wrong event"),
        }
        assert_eq!(st.current_dex.as_deref(), Some("Uni"));
    }

    #[test]
    fn spot_switch_updates_index() {
        let mut st = SettingsState::new();
        let _ = st.apply(UICommand::SwitchSpot(SwitchSpot {
            uid: 1,
            spot_index: 1,
        }));
        assert_eq!(st.current_spot, Some(1));
    }

    #[test]
    fn arb_activate_stores_valid_until() {
        let mut st = SettingsState::new();
        let _ = st.apply(UICommand::ArbActivateNotify(ArbActivateNotify {
            uid: 1,
            arb_valid: 45000.5,
        }));
        assert_eq!(st.arb_valid_until, Some(45000.5));
    }

    #[test]
    fn lev_manage_stores_snapshot() {
        let mut st = SettingsState::new();
        let lm = LevManage {
            uid: 1,
            cmd_ver: 1,
            auto_max_order: true,
            auto_lev_up: false,
            auto_isolated: true,
            auto_cross: false,
            auto_fix_lev: true,
            fix_lev: 10,
            tlg_report: false,
            lev_control: "BTC".to_string(),
        };
        let _ = st.apply(UICommand::LevManage(lm));
        assert!(st.lev_manage.is_some());
        assert_eq!(st.lev_manage.as_ref().unwrap().fix_lev, 10);
    }

    #[test]
    fn action_commands_pass_through_without_state() {
        let mut st = SettingsState::new();
        let ev = st.apply(UICommand::StratStartStop(StratStartStop {
            uid: 1,
            is_start: true,
        }));
        assert!(matches!(
            ev,
            Some(SettingsEvent::StratStartStopRequested(_))
        ));
        // state не меняется
        assert!(st.client_settings.is_none());
    }
}
