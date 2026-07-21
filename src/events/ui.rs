//! Active `MPC_UI` and `MPC_LogMsg` dispatch.

use super::{Event, EventDispatcher, ServerLogEvent};
use crate::commands::registry::decode_utf8_delphi;
use crate::commands::ui::UICommand;
use crate::protocol::Command;

impl EventDispatcher {
    pub(super) fn client_new_data_ui(&mut self, payload: &[u8], out: &mut Vec<Event>) {
        match UICommand::parse_with_client_settings_fallback(
            payload,
            Some(self.settings.client_settings_parse_fallback()),
        ) {
            Some(UICommand::Skipped { .. } | UICommand::Unknown { .. }) => {}
            Some(UICommand::NewMarketNotify(_)) => {
                self.markets.markets_list_refresh_needed = true;
                self.force_markets_list_refresh = true;
            }
            Some(UICommand::AlertObject(cmd)) => {
                if let Some(ev) = self.chart_alerts.apply(cmd) {
                    out.push(Event::ChartAlert(ev));
                }
            }
            Some(UICommand::ChartTextSnapshot(cmd)) => {
                if let Some(snapshot) = self.chart_text.apply_snapshot(cmd) {
                    out.push(Event::ChartText(snapshot));
                }
            }
            Some(UICommand::AlertSnapshotRequest { .. } | UICommand::ChartTextState(_)) => {}
            Some(UICommand::NewsRelay(command)) => match self.news.apply_relay(command) {
                Ok(Some(event)) => out.push(Event::News(event)),
                Ok(None) => {}
                Err(()) => Self::push_parse_failed(out, Command::UI, payload),
            },
            Some(UICommand::NewsHistory(command)) => match self.news.apply_history(command) {
                Ok(event) => out.push(Event::News(event)),
                Err(()) => Self::push_parse_failed(out, Command::UI, payload),
            },
            Some(cmd_v) => {
                let coin_blacklist_text = match &cmd_v {
                    UICommand::ClientSettings(settings) => {
                        Some(settings.coins_black_list_text.clone())
                    }
                    _ => None,
                };
                let lev_manage = match &cmd_v {
                    UICommand::LevManage(lev) => Some(lev.clone()),
                    _ => None,
                };
                if let Some(ev) = self.settings.apply(cmd_v) {
                    out.push(Event::Settings(ev));
                }
                if let Some(text) = coin_blacklist_text {
                    self.markets.set_coin_blacklist_text(&text);
                }
                if let Some(lev) = lev_manage {
                    self.markets.apply_lev_manage_to_markets(&lev);
                }
            }
            None => Self::push_parse_failed(out, Command::UI, payload),
        }
    }

    pub(super) fn client_new_data_log_msg(&mut self, payload: &[u8], out: &mut Vec<Event>) {
        if payload.len() < 8 {
            Self::push_parse_failed(out, Command::LogMsg, payload);
            return;
        }
        let time = f64::from_le_bytes(payload[0..8].try_into().unwrap());
        let msg = decode_utf8_delphi(&payload[8..]);
        out.push(Event::ServerLog(ServerLogEvent::new(time, msg)));
    }
}
