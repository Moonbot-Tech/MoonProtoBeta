//! Active `MPC_Balance` dispatch.
//!
//! Mirrors Delphi balance/arb receive routing: parse subcommand, apply balances
//! against known markets, and expose compact arbitrage payload only for known
//! market indexes.

use super::{Event, EventDispatcher};
use crate::commands::arb::{parse_arb_payload_compact, parse_arb_prices, ArbPayload};
use crate::commands::balance::parse_balance;
use crate::protocol::Command;

impl EventDispatcher {
    pub(super) fn client_new_data_balance(&mut self, payload: &[u8], out: &mut Vec<Event>) {
        if payload.len() < 11 {
            out.push(Self::parse_failed(Command::Balance, payload));
            return;
        }
        let sub_cmd_id = payload[0];
        let ver = u16::from_le_bytes([payload[1], payload[2]]);
        if ver > crate::commands::registry::CURRENT_PROTO_CMD_VER {
            return;
        }
        let body = &payload[11..];
        match sub_cmd_id {
            0 | 1 | 2 | 5 => {}
            3 | 4 => match parse_balance(sub_cmd_id, body) {
                Some(upd) => {
                    let ev = self
                        .balances
                        .apply_with_known_markets(upd, &self.markets.by_name);
                    out.push(Event::Balance(ev));
                }
                None => out.push(Self::parse_failed(Command::Balance, payload)),
            },
            6 => match parse_arb_prices(payload) {
                Some(arb) => {
                    if let Some(parsed) = parse_arb_payload_compact(&arb.payload) {
                        let parsed = self.filter_arb_payload_to_known_markets(parsed);
                        out.push(Event::Arb {
                            uid: arb.uid,
                            payload: parsed,
                        });
                    }
                }
                None => out.push(Self::parse_failed(Command::Balance, payload)),
            },
            _ => {}
        }
    }

    fn filter_arb_payload_to_known_markets(&self, payload: ArbPayload) -> ArbPayload {
        match payload {
            ArbPayload::Price {
                version,
                mut blocks,
            } => {
                blocks.retain(|block| self.markets.has_server_market_index(block.market_index));
                ArbPayload::Price { version, blocks }
            }
            ArbPayload::Isolation {
                version,
                mut entries,
            } => {
                entries.retain(|entry| self.markets.has_server_market_index(entry.market_index));
                ArbPayload::Isolation { version, entries }
            }
        }
    }
}
