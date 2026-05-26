//! Active `MPC_UI` and `MPC_LogMsg` dispatch.

use super::{Event, EventDispatcher};
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
            Some(cmd_v) => {
                if let Some(ev) = self.settings.apply(cmd_v) {
                    out.push(Event::Settings(ev));
                }
            }
            None => out.push(Self::parse_failed(Command::UI, payload)),
        }
    }

    pub(super) fn client_new_data_log_msg(&mut self, payload: &[u8], out: &mut Vec<Event>) {
        if payload.len() < 8 {
            out.push(Self::parse_failed(Command::LogMsg, payload));
            return;
        }
        let time = f64::from_le_bytes(payload[0..8].try_into().unwrap());
        let msg = decode_utf8_delphi(&payload[8..]);
        out.push(Event::ServerLog { time, msg });
    }
}
