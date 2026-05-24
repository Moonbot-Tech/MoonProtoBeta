//! Client-side diagnostic hooks used by live stress tests.

use super::COMPRESSED_FLAG;
use crate::commands::engine_api::parse_engine_response;
use crate::protocol::{slicing, Command};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
#[cfg(feature = "diagnostic-trace")]
use std::sync::OnceLock;

// =============================================================================
//  ErrEmu — ТОЛЬКО ДЛЯ ТЕСТОВ. Симуляция packet loss на стороне клиента.
// =============================================================================
//
// ⚠️ **НЕ ИСПОЛЬЗОВАТЬ В PRODUCTION.** Это инструмент для нагрузочного тестирования
// gap-recovery / reconnect / extend-bucket логики через искусственный дроп UDP-пакетов.
//
// По умолчанию выключено (ERR_EMU_RATE = 0). Включается явным вызовом
// `set_err_emu(percent)` где percent ∈ [0..100].
//
// Зеркало серверного `MoonProtoErrEmu` (Delphi `MoonProtoUDPClient.pas:534-541` и
// `MoonProtoUDPServer.pas:1281-1288`): дроп происходит **после** успешной проверки
// MAC и version, а в Delphi-клиенте ещё и после побочных эффектов `TotalRecvBytes`
// / `LastOnline`. Rust сохраняет тот же порядок: валидный packet, выбранный ErrEmu
// для дропа, всё равно доезжает до main-loop, обновляет transport stats, и только
// потом не dispatch'ится в protocol layer. Служебные команды (Ping /
// handshake-related / ACK) дропаются с rate/2 чтобы handshake не отваливался
// полностью.
//
// Использование (пример: 75% loss):
//   moonproto::client::set_err_emu(75);
//   let mut client = Client::new(cfg);
//   client.run(...);
//
// Используется в `examples/loss_logger.rs` — runtime-логгер потерь и восстановлений.
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
pub fn set_err_emu(percent: u8) {
    ERR_EMU_RATE.store(percent.min(100), Ordering::Relaxed);
}

/// Per-command packet counters collected while [`set_err_emu`] is non-zero.
#[derive(Debug, Clone, Default)]
pub struct ErrEmuCommandDiagnostics {
    pub raw_cmd: u8,
    pub valid_packets: u64,
    pub delivered_packets: u64,
    pub dropped_packets: u64,
}

/// Per-block counters for one incoming `MPC_Sliced` datagram.
#[derive(Debug, Clone, Default)]
pub struct ErrEmuSlicedBlockDiagnostics {
    pub block_num: u8,
    pub delivered_packets: u64,
    pub dropped_packets: u64,
}

/// Packet-loss counters for one incoming `MPC_Sliced` datagram.
#[derive(Debug, Clone, Default)]
pub struct ErrEmuSlicedDatagramDiagnostics {
    pub datagram_num: u16,
    pub blocks_count: usize,
    pub block0_wire_cmd: Option<u8>,
    pub block0_ui_cmd_id: Option<u8>,
    pub completed_cmd: Option<u8>,
    pub completed_ui_cmd_id: Option<u8>,
    pub completed_api_method: Option<u8>,
    pub completed_api_uid: Option<u64>,
    pub completed_api_success: Option<bool>,
    pub completed_payload_len: Option<usize>,
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
    completed_api_method: Option<u8>,
    completed_api_uid: Option<u64>,
    completed_api_success: Option<bool>,
    completed_payload_len: Option<usize>,
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
        if Command::from_byte(cmd) == Command::UI {
            dg.completed_ui_cmd_id = payload.first().copied();
        }
        if Command::from_byte(cmd) == Command::API {
            if let Some(resp) = parse_engine_response(payload) {
                dg.completed_api_method = Some(resp.method as u8);
                dg.completed_api_uid = Some(resp.request_uid);
                dg.completed_api_success = Some(resp.success);
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
                    completed_api_method: dg.completed_api_method,
                    completed_api_uid: dg.completed_api_uid,
                    completed_api_success: dg.completed_api_success,
                    completed_payload_len: dg.completed_payload_len,
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

/// Команды, для которых dropRate делится пополам (служебные).
/// Точное соответствие Delphi MoonProtoUDPClient.pas:537-538.
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
            out.push(Command::API as u8);
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

        state.record_packet(Command::Sliced as u8, &sliced_payload(5, 20, 26), delivered);
        state.record_packet(Command::Sliced as u8, &sliced_payload(5, 1, 14), delivered);
        state.record_sliced_complete(5, 15, Command::API as u8, &[]);

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
                && dg.completed_cmd == Some(Command::API as u8)
        }));
    }
}
