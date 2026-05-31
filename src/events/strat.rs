//! Active `MPC_Strat` dispatch.
//!
//! Keeps strategy protocol effects together: parse `TStratCommand`, apply
//! snapshot/update state, and auto-decode serializer payloads into `StratsState`.

use super::{Event, EventDispatcher};
use crate::commands::strat::StratCommand;
use crate::protocol::Command;
use crate::state::StratEvent;

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
        if let StratCommand::Snapshot(snap) = cmd_v {
            let raw_len = snap.data.len();
            if self
                .strats
                .apply_snapshot_decoded_with_mode_in_place(&snap.data, snap.full)
                .is_none()
            {
                log::warn!(
                    target: "moonproto::events",
                    "failed to decode {} strategy snapshot payload ({} bytes)",
                    if snap.full { "full" } else { "partial" },
                    raw_len
                );
                return;
            }
            self.strats.last_server_epoch = snap.server_epoch;
            let ev = if snap.full {
                StratEvent::SnapshotFull {
                    server_epoch: snap.server_epoch,
                    raw_len,
                    #[cfg(feature = "diagnostics")]
                    raw_data: snap.data,
                }
            } else {
                StratEvent::SnapshotPartial {
                    server_epoch: snap.server_epoch,
                    raw_len,
                    #[cfg(feature = "diagnostics")]
                    raw_data: snap.data,
                }
            };
            out.push(Event::Strat(ev));
            return;
        }

        let ev = self.strats.apply(cmd_v);
        out.push(Event::Strat(ev));
    }
}
