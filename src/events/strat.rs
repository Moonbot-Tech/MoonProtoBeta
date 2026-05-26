//! Active `MPC_Strat` dispatch.
//!
//! Keeps strategy protocol effects together: parse `TStratCommand`, apply
//! snapshot/update state, and auto-decode serializer payloads into `StratsState`.

use super::{Event, EventDispatcher};
use crate::commands::strat::StratCommand;
use crate::protocol::Command;

impl EventDispatcher {
    pub(super) fn client_new_data_strat(&mut self, payload: &[u8], out: &mut Vec<Event>) {
        match StratCommand::parse(payload) {
            Some(cmd_v) => self.process_strat_command(cmd_v, out),
            None => out.push(Self::parse_failed(Command::Strat, payload)),
        }
    }

    /// Delphi equivalent: `TMoonProtoNetClient.ProcessStratCommand`.
    fn process_strat_command(&mut self, cmd_v: StratCommand, out: &mut Vec<Event>) {
        match &cmd_v {
            StratCommand::SellPriceUpdate(_)
            | StratCommand::SchemaRequest { .. }
            | StratCommand::Skipped { .. }
            | StratCommand::Unknown { .. } => return,
            _ => {}
        }
        let ev = self.strats.apply(cmd_v);
        // Active library: auto-decode strategy snapshot raw bytes
        // into `StratsState`. Раньше app должен был сам вызывать
        // `strats.apply_snapshot_decoded(raw_data)`; теперь либа
        // делает это сама на SnapshotFull/Partial event'ах.
        match &ev {
            crate::state::StratEvent::SnapshotFull {
                server_epoch,
                raw_data,
            } => {
                if self
                    .strats
                    .apply_snapshot_decoded_with_mode_in_place(raw_data, true)
                    .is_none()
                {
                    log::warn!(
                        target: "moonproto::events",
                        "failed to decode full strategy snapshot payload ({} bytes)",
                        raw_data.len()
                    );
                    return;
                }
                self.strats.last_server_epoch = *server_epoch;
            }
            crate::state::StratEvent::SnapshotPartial {
                server_epoch,
                raw_data,
            } => {
                if self
                    .strats
                    .apply_snapshot_decoded_with_mode_in_place(raw_data, false)
                    .is_none()
                {
                    log::warn!(
                        target: "moonproto::events",
                        "failed to decode partial strategy snapshot payload ({} bytes)",
                        raw_data.len()
                    );
                    return;
                }
                self.strats.last_server_epoch = *server_epoch;
            }
            _ => {}
        }
        out.push(Event::Strat(ev));
    }
}
