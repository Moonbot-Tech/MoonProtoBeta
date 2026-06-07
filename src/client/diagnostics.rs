//! Client-side diagnostic hooks used by live stress tests.
#![cfg_attr(
    not(any(test, feature = "diagnostics")),
    allow(dead_code, unused_imports, unreachable_pub)
)]

use super::COMPRESSED_FLAG;
use crate::commands::engine_api::parse_engine_response;
use crate::commands::order_book::parse_order_book_packet;
use crate::protocol::{slicing, Command};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
#[cfg(feature = "diagnostic-trace")]
use std::sync::OnceLock;
#[cfg(feature = "diagnostic-trace")]
use std::time::Instant;

// =============================================================================
//  ErrEmu — TESTS ONLY. Client-side packet-loss simulation.
// =============================================================================
//
// ⚠️ **DO NOT USE IN PRODUCTION.** This is a tool for load-testing the
// gap-recovery / reconnect / extend-bucket logic via artificial UDP packet drops.
//
// Disabled by default (ERR_EMU_RATE = 0). Enabled by an explicit call to
// `set_err_emu(percent)` where percent ∈ [0..100].
//
// Mirror of the server-side `MoonProtoErrEmu` (Delphi `MoonProtoUDPClient.pas:534-541` and
// `MoonProtoUDPServer.pas:1281-1288`): the drop happens **after** the successful
// MAC and version check, and in the Delphi client also after the `TotalRecvBytes`
// / `LastOnline` side effects. Rust keeps the same order: a valid packet selected by ErrEmu
// for dropping still reaches the main loop, updates transport stats, and only
// then is not dispatched into the protocol layer. Service commands (Ping /
// handshake-related / ACK) are dropped at rate/2 so the handshake does not fall apart
// entirely.
//
// Usage (example: 75% loss):
//   moonproto::client::set_err_emu(75);
//   let client = MoonClient::connect(cfg, connect)?;
//   // then the usual MoonClient/EventSink pipeline.
/// Process-wide incoming packet-loss emulator rate, in percent.
///
/// This is a test hook for stress and FireTest-style scenarios. Prefer
/// [`set_err_emu`] instead of writing the atomic directly.
#[doc(hidden)]
pub static ERR_EMU_RATE: AtomicU8 = AtomicU8::new(0);

/// Set the client-side incoming packet-loss emulator percentage (`0..=100`).
///
/// `0` disables emulation and is the default. This hook is for tests only and
/// mirrors Delphi `MoonProtoErrEmu`.
#[doc(hidden)]
pub fn set_err_emu(percent: u8) {
    ERR_EMU_RATE.store(percent.min(100), Ordering::Relaxed);
}

/// Per-command packet counters collected while [`set_err_emu`] is non-zero.
#[doc(hidden)]
#[derive(Debug, Clone, Default)]
pub struct ErrEmuCommandDiagnostics {
    pub raw_cmd: u8,
    pub valid_packets: u64,
    pub delivered_packets: u64,
    pub dropped_packets: u64,
}

/// Per-block counters for one incoming `MPC_Sliced` datagram.
#[doc(hidden)]
#[derive(Debug, Clone, Default)]
pub struct ErrEmuSlicedBlockDiagnostics {
    pub block_num: u8,
    pub delivered_packets: u64,
    pub dropped_packets: u64,
}

/// Packet-loss counters for one incoming `MPC_Sliced` datagram.
#[doc(hidden)]
#[derive(Debug, Clone, Default)]
pub struct ErrEmuSlicedDatagramDiagnostics {
    pub datagram_num: u16,
    pub blocks_count: usize,
    pub block0_wire_cmd: Option<u8>,
    pub block0_ui_cmd_id: Option<u8>,
    pub completed_cmd: Option<u8>,
    pub completed_ui_cmd_id: Option<u8>,
    pub completed_strat_cmd_id: Option<u8>,
    pub completed_strat_uid: Option<u64>,
    pub completed_api_method: Option<u8>,
    pub completed_api_uid: Option<u64>,
    pub completed_api_success: Option<bool>,
    pub completed_orderbook_market_index: Option<u16>,
    pub completed_orderbook_kind: Option<u8>,
    pub completed_orderbook_seq: Option<u16>,
    pub completed_orderbook_is_full: Option<bool>,
    pub completed_orderbook_buys: Option<usize>,
    pub completed_orderbook_sells: Option<usize>,
    pub completed_payload_len: Option<usize>,
    pub completed_payload_head: Option<[u8; 8]>,
    pub completed_payload_head_len: usize,
    pub completed_payload_hash: Option<u64>,
    pub delivered_packets: u64,
    pub dropped_packets: u64,
    pub blocks: Vec<ErrEmuSlicedBlockDiagnostics>,
}

impl ErrEmuSlicedDatagramDiagnostics {
    pub fn delivered_unique_blocks(&self) -> usize {
        self.blocks
            .iter()
            .filter(|block| block.delivered_packets > 0)
            .count()
    }

    pub fn missing_blocks(&self) -> Vec<u8> {
        (0..self.blocks_count.min(256))
            .filter_map(|block| {
                let block = block as u8;
                let delivered = self
                    .blocks
                    .iter()
                    .find(|diag| diag.block_num == block)
                    .map(|diag| diag.delivered_packets)
                    .unwrap_or(0);
                (delivered == 0).then_some(block)
            })
            .collect()
    }

    pub fn block_drop_count(&self, block_num: u8) -> u64 {
        self.blocks
            .iter()
            .find(|diag| diag.block_num == block_num)
            .map(|diag| diag.dropped_packets)
            .unwrap_or(0)
    }
}

/// Snapshot of client-side packet-loss emulator counters.
#[doc(hidden)]
#[derive(Debug, Clone, Default)]
pub struct ErrEmuDiagnostics {
    pub configured_rate: u8,
    pub valid_packets: u64,
    pub delivered_packets: u64,
    pub dropped_packets: u64,
    pub by_cmd: Vec<ErrEmuCommandDiagnostics>,
    pub outgoing_packets: u64,
    pub outgoing_blackholed_packets: u64,
    pub outgoing_by_cmd: Vec<ErrEmuCommandDiagnostics>,
    pub outgoing_blackholed_by_cmd: Vec<ErrEmuCommandDiagnostics>,
    pub sliced: Vec<ErrEmuSlicedDatagramDiagnostics>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ErrEmuDropDecision {
    configured_rate: u8,
    pub(crate) dropped: bool,
}

#[derive(Debug)]
pub(crate) struct ErrEmuDiagnosticsState {
    configured_rate: u8,
    valid_by_cmd: [u64; 256],
    delivered_by_cmd: [u64; 256],
    dropped_by_cmd: [u64; 256],
    outgoing_by_cmd: [u64; 256],
    outgoing_blackholed_by_cmd: [u64; 256],
    sliced: HashMap<ErrEmuSlicedKey, ErrEmuSlicedDatagramState>,
}

impl Default for ErrEmuDiagnosticsState {
    fn default() -> Self {
        Self {
            configured_rate: 0,
            valid_by_cmd: [0; 256],
            delivered_by_cmd: [0; 256],
            dropped_by_cmd: [0; 256],
            outgoing_by_cmd: [0; 256],
            outgoing_blackholed_by_cmd: [0; 256],
            sliced: HashMap::new(),
        }
    }
}

#[derive(Debug, Default)]
struct ErrEmuSlicedDatagramState {
    block0_wire_cmd: Option<u8>,
    block0_ui_cmd_id: Option<u8>,
    completed_cmd: Option<u8>,
    completed_ui_cmd_id: Option<u8>,
    completed_strat_cmd_id: Option<u8>,
    completed_strat_uid: Option<u64>,
    completed_api_method: Option<u8>,
    completed_api_uid: Option<u64>,
    completed_api_success: Option<bool>,
    completed_orderbook_market_index: Option<u16>,
    completed_orderbook_kind: Option<u8>,
    completed_orderbook_seq: Option<u16>,
    completed_orderbook_is_full: Option<bool>,
    completed_orderbook_buys: Option<usize>,
    completed_orderbook_sells: Option<usize>,
    completed_payload_len: Option<usize>,
    completed_payload_head: Option<[u8; 8]>,
    completed_payload_head_len: usize,
    completed_payload_hash: Option<u64>,
    delivered_packets: u64,
    dropped_packets: u64,
    blocks: HashMap<u8, ErrEmuSlicedBlockState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ErrEmuSlicedKey {
    datagram_num: u16,
    blocks_count: usize,
}

#[derive(Debug, Default)]
struct ErrEmuSlicedBlockState {
    delivered_packets: u64,
    dropped_packets: u64,
}

impl ErrEmuDiagnosticsState {
    pub(crate) fn record_outgoing(&mut self, raw_cmd: u8, blackholed: bool) {
        let idx = raw_cmd as usize;
        if blackholed {
            self.outgoing_blackholed_by_cmd[idx] =
                self.outgoing_blackholed_by_cmd[idx].saturating_add(1);
        } else {
            self.outgoing_by_cmd[idx] = self.outgoing_by_cmd[idx].saturating_add(1);
        }
    }

    pub(crate) fn record_packet(
        &mut self,
        raw_cmd: u8,
        payload: &[u8],
        decision: ErrEmuDropDecision,
    ) {
        self.configured_rate = decision.configured_rate;
        let idx = raw_cmd as usize;
        self.valid_by_cmd[idx] = self.valid_by_cmd[idx].saturating_add(1);
        if decision.dropped {
            self.dropped_by_cmd[idx] = self.dropped_by_cmd[idx].saturating_add(1);
        } else {
            self.delivered_by_cmd[idx] = self.delivered_by_cmd[idx].saturating_add(1);
        }

        if Command::from_byte(raw_cmd) == Command::Sliced {
            self.record_sliced_packet(payload, decision.dropped);
        }
    }

    fn record_sliced_packet(&mut self, payload: &[u8], dropped: bool) {
        let Some(hdr) = slicing::SliceHeader::from_bytes(payload) else {
            return;
        };
        let block_data = &payload[slicing::SLICE_HEADER_SIZE..];
        let key = ErrEmuSlicedKey {
            datagram_num: hdr.datagram_num,
            blocks_count: (hdr.max_block_num as usize) + 1,
        };
        let dg = self.sliced.entry(key).or_default();
        if hdr.block_num == 0 {
            if let Some((&wire_cmd, rest)) = block_data.split_first() {
                dg.block0_wire_cmd = Some(wire_cmd);
                let base_cmd = wire_cmd & !COMPRESSED_FLAG;
                if Command::from_byte(base_cmd) == Command::UI && wire_cmd & COMPRESSED_FLAG == 0 {
                    dg.block0_ui_cmd_id = rest.first().copied();
                }
            }
        }
        if dropped {
            dg.dropped_packets = dg.dropped_packets.saturating_add(1);
        } else {
            dg.delivered_packets = dg.delivered_packets.saturating_add(1);
        }
        let block = dg.blocks.entry(hdr.block_num).or_default();
        if dropped {
            block.dropped_packets = block.dropped_packets.saturating_add(1);
        } else {
            block.delivered_packets = block.delivered_packets.saturating_add(1);
        }
    }

    pub(crate) fn record_sliced_complete(
        &mut self,
        datagram_num: u16,
        blocks_count: usize,
        cmd: u8,
        payload: &[u8],
    ) {
        let key = ErrEmuSlicedKey {
            datagram_num,
            blocks_count,
        };
        let dg = self.sliced.entry(key).or_default();
        dg.completed_cmd = Some(cmd);
        dg.completed_payload_len = Some(payload.len());
        let head_len = payload.len().min(8);
        let mut head = [0u8; 8];
        head[..head_len].copy_from_slice(&payload[..head_len]);
        dg.completed_payload_head = Some(head);
        dg.completed_payload_head_len = head_len;
        dg.completed_payload_hash = Some(fnv1a64(payload));
        if Command::from_byte(cmd) == Command::UI {
            dg.completed_ui_cmd_id = payload.first().copied();
        }
        if Command::from_byte(cmd) == Command::Strat {
            dg.completed_strat_cmd_id = payload.first().copied();
            dg.completed_strat_uid = payload
                .get(3..11)
                .and_then(|uid| uid.try_into().ok())
                .map(u64::from_le_bytes);
        }
        if Command::from_byte(cmd) == Command::API {
            if let Some(resp) = parse_engine_response(payload) {
                dg.completed_api_method = Some(resp.method.to_byte());
                dg.completed_api_uid = Some(resp.request_uid);
                dg.completed_api_success = Some(resp.success);
            }
        }
        if Command::from_byte(cmd) == Command::OrderBook {
            if let Some(pkt) = parse_order_book_packet(payload) {
                dg.completed_orderbook_market_index = Some(pkt.market_index);
                dg.completed_orderbook_kind = Some(pkt.book_kind);
                dg.completed_orderbook_seq = Some(pkt.seq);
                dg.completed_orderbook_is_full = Some(pkt.is_full);
                dg.completed_orderbook_buys = Some(pkt.buys.len());
                dg.completed_orderbook_sells = Some(pkt.sells.len());
            }
        }
    }

    pub(crate) fn snapshot(&self, configured_rate: u8) -> ErrEmuDiagnostics {
        let mut by_cmd = Vec::new();
        let mut outgoing_by_cmd = Vec::new();
        let mut outgoing_blackholed_by_cmd = Vec::new();
        let mut valid_packets = 0u64;
        let mut delivered_packets = 0u64;
        let mut dropped_packets = 0u64;
        let mut outgoing_packets = 0u64;
        let mut outgoing_blackholed_packets = 0u64;
        for raw_cmd in 0..=u8::MAX {
            let idx = raw_cmd as usize;
            let valid = self.valid_by_cmd[idx];
            let delivered = self.delivered_by_cmd[idx];
            let dropped = self.dropped_by_cmd[idx];
            let outgoing = self.outgoing_by_cmd[idx];
            let blackholed = self.outgoing_blackholed_by_cmd[idx];
            valid_packets = valid_packets.saturating_add(valid);
            delivered_packets = delivered_packets.saturating_add(delivered);
            dropped_packets = dropped_packets.saturating_add(dropped);
            outgoing_packets = outgoing_packets.saturating_add(outgoing);
            outgoing_blackholed_packets = outgoing_blackholed_packets.saturating_add(blackholed);
            if valid > 0 || delivered > 0 || dropped > 0 {
                by_cmd.push(ErrEmuCommandDiagnostics {
                    raw_cmd,
                    valid_packets: valid,
                    delivered_packets: delivered,
                    dropped_packets: dropped,
                });
            }
            if outgoing > 0 {
                outgoing_by_cmd.push(ErrEmuCommandDiagnostics {
                    raw_cmd,
                    valid_packets: outgoing,
                    delivered_packets: outgoing,
                    dropped_packets: 0,
                });
            }
            if blackholed > 0 {
                outgoing_blackholed_by_cmd.push(ErrEmuCommandDiagnostics {
                    raw_cmd,
                    valid_packets: blackholed,
                    delivered_packets: 0,
                    dropped_packets: blackholed,
                });
            }
        }

        let mut sliced: Vec<_> = self
            .sliced
            .iter()
            .map(|(&key, dg)| {
                let mut blocks: Vec<_> = dg
                    .blocks
                    .iter()
                    .map(|(&block_num, block)| ErrEmuSlicedBlockDiagnostics {
                        block_num,
                        delivered_packets: block.delivered_packets,
                        dropped_packets: block.dropped_packets,
                    })
                    .collect();
                blocks.sort_by_key(|block| block.block_num);
                ErrEmuSlicedDatagramDiagnostics {
                    datagram_num: key.datagram_num,
                    blocks_count: key.blocks_count,
                    block0_wire_cmd: dg.block0_wire_cmd,
                    block0_ui_cmd_id: dg.block0_ui_cmd_id,
                    completed_cmd: dg.completed_cmd,
                    completed_ui_cmd_id: dg.completed_ui_cmd_id,
                    completed_strat_cmd_id: dg.completed_strat_cmd_id,
                    completed_strat_uid: dg.completed_strat_uid,
                    completed_api_method: dg.completed_api_method,
                    completed_api_uid: dg.completed_api_uid,
                    completed_api_success: dg.completed_api_success,
                    completed_orderbook_market_index: dg.completed_orderbook_market_index,
                    completed_orderbook_kind: dg.completed_orderbook_kind,
                    completed_orderbook_seq: dg.completed_orderbook_seq,
                    completed_orderbook_is_full: dg.completed_orderbook_is_full,
                    completed_orderbook_buys: dg.completed_orderbook_buys,
                    completed_orderbook_sells: dg.completed_orderbook_sells,
                    completed_payload_len: dg.completed_payload_len,
                    completed_payload_head: dg.completed_payload_head,
                    completed_payload_head_len: dg.completed_payload_head_len,
                    completed_payload_hash: dg.completed_payload_hash,
                    delivered_packets: dg.delivered_packets,
                    dropped_packets: dg.dropped_packets,
                    blocks,
                }
            })
            .collect();
        sliced.sort_by_key(|dg| (dg.datagram_num, dg.blocks_count));

        ErrEmuDiagnostics {
            configured_rate: if configured_rate == 0 {
                self.configured_rate
            } else {
                configured_rate
            },
            valid_packets,
            delivered_packets,
            dropped_packets,
            by_cmd,
            outgoing_packets,
            outgoing_blackholed_packets,
            outgoing_by_cmd,
            outgoing_blackholed_by_cmd,
            sliced,
        }
    }
}

#[cfg(feature = "diagnostic-trace")]
pub(crate) fn trace_io_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var_os("MOONPROTO_TRACE_IO")
            .map(|v| {
                let v = v.to_string_lossy();
                !(v.is_empty() || v == "0" || v.eq_ignore_ascii_case("false"))
            })
            .unwrap_or(false)
    })
}

#[cfg(feature = "diagnostic-trace")]
pub(crate) fn trace_elapsed_ms() -> u128 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_millis()
}

#[cfg(feature = "diagnostic-trace")]
pub(crate) fn trace_head(bytes: &[u8], max_len: usize) -> String {
    let mut s = String::new();
    for (idx, byte) in bytes.iter().take(max_len).enumerate() {
        if idx > 0 {
            s.push(' ');
        }
        use std::fmt::Write;
        let _ = write!(s, "{byte:02X}");
    }
    s
}

#[cfg(feature = "diagnostic-trace")]
pub(crate) fn diagnostic_duplicate_sliced_acks() -> usize {
    static COUNT: OnceLock<usize> = OnceLock::new();
    *COUNT.get_or_init(|| {
        std::env::var("MOONPROTO_DIAG_DUP_SLICED_ACKS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0)
            .min(16)
    })
}

#[cfg(not(feature = "diagnostic-trace"))]
#[inline(always)]
pub(crate) fn diagnostic_duplicate_sliced_acks() -> usize {
    0
}

#[cfg(not(feature = "diagnostic-trace"))]
#[inline(always)]
pub(crate) fn trace_io_enabled() -> bool {
    false
}

#[cfg(not(feature = "diagnostic-trace"))]
#[inline(always)]
pub(crate) fn trace_elapsed_ms() -> u128 {
    0
}

#[cfg(not(feature = "diagnostic-trace"))]
#[inline(always)]
pub(crate) fn trace_head(_: &[u8], _: usize) -> String {
    String::new()
}

/// Commands for which dropRate is halved (service commands).
/// Exact match with Delphi MoonProtoUDPClient.pas:537-538.
#[inline]
pub(crate) fn is_service_cmd(cmd: u8) -> bool {
    matches!(
        Command::from_byte(cmd),
        Command::Ping
            | Command::WantNewHello
            | Command::WrongHello
            | Command::WhoAreYou
            | Command::Fine
            | Command::NeedHelloAgain
            | Command::SizeTest
            | Command::ProbeMTU
            | Command::SlicedACK
    )
}

#[inline]
pub(crate) fn err_emu_drop_decision(cmd: u8) -> Option<ErrEmuDropDecision> {
    let base_rate = ERR_EMU_RATE.load(Ordering::Relaxed);
    if base_rate == 0 {
        return None;
    }
    let drop_rate = err_emu_drop_rate_for_cmd(base_rate, cmd);
    let roll: u8 = rand::random::<u8>() % 100;
    Some(ErrEmuDropDecision {
        configured_rate: base_rate,
        dropped: roll < drop_rate,
    })
}

#[inline]
pub(crate) fn err_emu_drop_rate_for_cmd(base_rate: u8, cmd: u8) -> u8 {
    let base_rate = base_rate.min(100);
    if is_service_cmd(cmd) {
        base_rate / 2
    } else {
        base_rate
    }
}

pub(crate) fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sliced_payload(datagram_num: u16, block_num: u8, max_block_num: u8) -> Vec<u8> {
        let mut out = Vec::new();
        slicing::SliceHeader {
            datagram_num,
            block_num,
            max_block_num,
        }
        .write_to(&mut out);
        if block_num == 0 {
            out.push(Command::API.to_byte());
        }
        out
    }

    #[test]
    fn sliced_diagnostics_do_not_merge_reused_datagram_numbers_with_different_sizes() {
        let mut state = ErrEmuDiagnosticsState::default();
        let delivered = ErrEmuDropDecision {
            configured_rate: 10,
            dropped: false,
        };

        state.record_packet(
            Command::Sliced.to_byte(),
            &sliced_payload(5, 20, 26),
            delivered,
        );
        state.record_packet(
            Command::Sliced.to_byte(),
            &sliced_payload(5, 1, 14),
            delivered,
        );
        state.record_sliced_complete(5, 15, Command::API.to_byte(), &[]);

        let snapshot = state.snapshot(10);
        let same_num: Vec<_> = snapshot
            .sliced
            .iter()
            .filter(|dg| dg.datagram_num == 5)
            .collect();
        assert_eq!(same_num.len(), 2);
        assert!(same_num
            .iter()
            .any(|dg| dg.blocks_count == 27 && dg.delivered_unique_blocks() == 1));
        assert!(same_num.iter().any(|dg| {
            dg.blocks_count == 15
                && dg.delivered_unique_blocks() == 1
                && dg.completed_cmd == Some(Command::API.to_byte())
        }));
    }

    #[test]
    fn sliced_diagnostics_record_completed_payload_head_for_parse_failure_correlation() {
        let mut state = ErrEmuDiagnosticsState::default();
        let payload = [0x47, 0x43, 0x00, 0x00, 0x41, 0x10, 0x01, 0x00, 0x99];
        state.record_sliced_complete(191, 10, Command::OrderBook.to_byte(), &payload);

        let snapshot = state.snapshot(50);
        let dg = snapshot
            .sliced
            .iter()
            .find(|dg| dg.datagram_num == 191)
            .expect("completed sliced datagram");

        assert_eq!(dg.completed_cmd, Some(Command::OrderBook.to_byte()));
        assert_eq!(dg.completed_payload_len, Some(9));
        assert_eq!(dg.completed_payload_head_len, 8);
        assert_eq!(
            dg.completed_payload_head,
            Some([0x47, 0x43, 0x00, 0x00, 0x41, 0x10, 0x01, 0x00])
        );
        assert_eq!(dg.completed_payload_hash, Some(fnv1a64(&payload)));
    }
}
