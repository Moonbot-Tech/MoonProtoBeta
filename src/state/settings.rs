//! Settings sync state — latest UI/settings snapshots received from the server.
//!
//! The state layer keeps the latest snapshot for each supported subcommand;
//! applying those settings to an application UI/engine is the consumer's
//! responsibility.
//!
//! ## Tracked State
//! - `ClientSettings`: full UI settings snapshot.
//! - `LevManage`: leverage-management settings snapshot.
//! - `ArbActivateNotify`: arbitrage-valid-until timestamp.
//!
//! Client->server action commands (`SettingsRequest`, `StratStartStop`,
//! `MMOrdersSubscribe`, `EmuTrades`, `TriggerManage`, `ResetProfit`,
//! `SwitchDex`, `SwitchSpot`) are sent through high-level handles and ignored
//! if they ever arrive inbound.
//! `NewMarketNotify` is an internal Active Lib trigger: the dispatcher uses it
//! to force listing refresh, and user code receives a market event only after
//! the refreshed list actually inserts new markets.

use crate::commands::ui::{ClientSettingsCommand, LevManage, UICommand};
use crate::time::MoonTime;

/// Synchronized UI/settings state updated from inbound UI settings packets.
///
/// Settings are snapshot state, not accumulated history. Every accepted
/// full settings snapshot fully replaces `client_settings`.
#[derive(Debug, Clone, Default)]
pub struct SettingsState {
    /// Last received client settings snapshot.
    pub client_settings: Option<ClientSettingsCommand>,
    /// Current settings fallback for append-only packet tails.
    ///
    /// Old packets may omit append-only tail fields; those fields are filled
    /// from the current retained settings. After every full settings snapshot
    /// this fallback is refreshed automatically; before the first snapshot,
    /// low-level dispatcher tests/tools may seed it through the hidden fallback
    /// helper.
    client_settings_fallback: ClientSettingsCommand,
    /// Current leverage-management settings, if received.
    pub lev_manage: Option<LevManage>,
    /// Raw `TDateTime` days for diagnostics/parity tests.
    ///
    /// Normal terminal code should use [`Self::arb_valid_until_time`] and
    /// [`Self::arb_is_active_now`] instead of carrying wire day doubles.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub arb_valid_until: Option<f64>,
    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) arb_valid_until: Option<f64>,
}

#[derive(Debug, Clone)]
pub enum SettingsEvent {
    /// A fresh full settings snapshot was applied.
    ClientSettingsUpdated,
    /// Leverage-management snapshot changed.
    LevManageUpdated,
    /// Remote update command: version name + release/test flag.
    ///
    /// Terminal clients treat this as a request to run their local updater. The
    /// state layer only surfaces the wire command; application code decides
    /// whether/how to update itself.
    VersionUpdate {
        #[cfg(any(test, feature = "diagnostics"))]
        #[doc(hidden)]
        uid: u64,
        version_name: String,
        is_release: bool,
    },
    /// Arbitrage license was activated/refreshed.
    ArbActivated {
        #[cfg(any(test, feature = "diagnostics"))]
        #[doc(hidden)]
        uid: u64,
        arb_valid: MoonTime,
    },
    /// Command from a future protocol version. Low-level diagnostics can surface
    /// it, while `EventDispatcher` skips it without state changes.
    #[cfg(any(test, feature = "diagnostics"))]
    Skipped { cmd_id: u8, uid: u64, ver: u16 },
    /// Unknown subcommand for forward compatibility.
    #[cfg(any(test, feature = "diagnostics"))]
    Unknown { cmd_id: u8, uid: u64 },
}

impl SettingsState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn arb_valid_until_time(&self) -> Option<MoonTime> {
        self.arb_valid_until.and_then(MoonTime::from_delphi_days)
    }

    /// Whether the retained arb-valid-until timestamp is still in the future.
    pub fn arb_is_active_now(&self) -> bool {
        self.arb_is_active_at(MoonTime::now())
    }

    /// Whether the retained arb-valid-until timestamp is later than `now`.
    pub fn arb_is_active_at(&self, now: MoonTime) -> bool {
        self.arb_valid_until_time()
            .is_some_and(|valid_until| valid_until > now)
    }

    /// Seed settings fallback used while parsing old settings packets with
    /// missing append-only tail fields.
    #[doc(hidden)]
    #[cfg(test)]
    pub(crate) fn set_client_settings_fallback(&mut self, fallback: ClientSettingsCommand) {
        self.client_settings_fallback = fallback;
    }

    pub(crate) fn client_settings_parse_fallback(&self) -> &ClientSettingsCommand {
        &self.client_settings_fallback
    }

    /// Apply an inbound UI command to retained state.
    ///
    /// Returns `None` for internal commands that have no public settings event.
    pub(crate) fn apply(&mut self, cmd: UICommand) -> Option<SettingsEvent> {
        match cmd {
            UICommand::ClientSettings(c) => {
                let settings = *c;
                self.client_settings_fallback = settings.clone();
                self.client_settings = Some(settings);
                Some(SettingsEvent::ClientSettingsUpdated)
            }
            UICommand::SettingsRequest { .. }
            | UICommand::StratStartStop(_)
            | UICommand::StratStartStopV2(_)
            | UICommand::MMOrdersSubscribe(_)
            | UICommand::EmuTrades(_)
            | UICommand::TriggerManage(_)
            | UICommand::ResetProfit(_)
            | UICommand::SwitchDex(_)
            | UICommand::SwitchSpot(_)
            | UICommand::AlertObject(_)
            | UICommand::AlertSnapshotRequest { .. }
            | UICommand::ChartTextState(_)
            | UICommand::ChartTextSnapshot(_)
            | UICommand::OrdersHistoryRequest(_) => None,

            UICommand::UpdateVersion(u) => Some(SettingsEvent::VersionUpdate {
                #[cfg(any(test, feature = "diagnostics"))]
                uid: u.uid,
                version_name: u.version_name,
                is_release: u.is_release,
            }),

            UICommand::NewMarketNotify(_) => None,

            UICommand::LevManage(l) => {
                self.lev_manage = Some(l);
                Some(SettingsEvent::LevManageUpdated)
            }

            UICommand::ArbActivateNotify(a) => {
                self.arb_valid_until = Some(a.arb_valid);
                Some(SettingsEvent::ArbActivated {
                    #[cfg(any(test, feature = "diagnostics"))]
                    uid: a.uid,
                    arb_valid: MoonTime::from_delphi_days(a.arb_valid).unwrap_or(MoonTime::ZERO),
                })
            }

            UICommand::Skipped { cmd_id, uid, ver } => {
                #[cfg(any(test, feature = "diagnostics"))]
                {
                    Some(SettingsEvent::Skipped { cmd_id, uid, ver })
                }
                #[cfg(not(any(test, feature = "diagnostics")))]
                {
                    let _ = (cmd_id, uid, ver);
                    None
                }
            }

            UICommand::Unknown { cmd_id, uid } => {
                #[cfg(any(test, feature = "diagnostics"))]
                {
                    Some(SettingsEvent::Unknown { cmd_id, uid })
                }
                #[cfg(not(any(test, feature = "diagnostics")))]
                {
                    let _ = (cmd_id, uid);
                    None
                }
            }
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
    fn inbound_mm_orders_subscribe_is_ignored_like_delphi_client() {
        let mut st = SettingsState::new();
        let ev = st.apply(UICommand::MMOrdersSubscribe(MMOrdersSubscribe {
            uid: 1,
            subscribe: true,
        }));
        assert!(ev.is_none());

        assert!(st.client_settings.is_none());
    }

    #[test]
    fn inbound_dex_switch_is_ignored_like_delphi_client() {
        let mut st = SettingsState::new();
        let ev = st.apply(UICommand::SwitchDex(SwitchDex {
            uid: 1,
            dex_name: "Uni".to_string(),
        }));
        assert!(ev.is_none());
    }

    #[test]
    fn inbound_spot_switch_is_ignored_like_delphi_client() {
        let mut st = SettingsState::new();
        let ev = st.apply(UICommand::SwitchSpot(SwitchSpot {
            uid: 1,
            spot_index: SpotMarketKind::Predict,
        }));
        assert!(ev.is_none());
    }

    #[test]
    fn arb_activate_stores_valid_until() {
        let mut st = SettingsState::new();
        let ev = st.apply(UICommand::ArbActivateNotify(ArbActivateNotify {
            uid: 1,
            arb_valid: 45000.5,
        }));
        assert_eq!(st.arb_valid_until, Some(45000.5));
        assert!(matches!(
            ev,
            Some(SettingsEvent::ArbActivated { arb_valid, .. })
                if arb_valid == MoonTime::from_delphi_days(45000.5).unwrap()
        ));
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
        assert!(ev.is_none());
        // No retained state changes.
        assert!(st.client_settings.is_none());
    }
}
