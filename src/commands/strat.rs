//! MPC_Strat channel — 9 TBaseStratCommand subcommands.
//!
//! Delphi source: `MoonProto/MoonProtoStratStruct.pas` (~408 lines).
//!
//! ## CmdId mapping
//! - 0 — TBaseStratCommand (base)
//! - 1 — TStratSnapshotRequest (empty, S→C)
//! - 2 — TStratSnapshot (both directions, Sliced, UK_StratSnapshot)
//! - 3 — TStratDelete (S↔C)
//! - 4 — TStratSellPriceUpdate (C→S, UK_StratSellPriceUpdate)
//! - 5 — TStratCheckedSync (S↔C, Sliced)
//! - 6 — TStratCheckedEcho (S→C ACK for the Checked delta)
//! - 7 — TStratSchemaRequest (C→S, empty)
//! - 8 — TStratSchema (S→C, Sliced, raw-deflate schema blob)
//!
//! ## Note on TStratSnapshot.Data
//! `Data: bytes(Size)` is the serialized `TStrategySerializer` bin format (RTTI-driven,
//! ~1118 lines). The decoder and writer live in `commands::strategy_serializer`.

use super::registry::{read_string, write_string, CURRENT_PROTO_CMD_VER};
use super::strategy_schema::StrategySchema;
use super::strategy_serializer::{
    StrategyBatchBuilder, StrategySnapshot as StrategySerializerSnapshot,
};
use zerocopy::byteorder::little_endian::U64 as LeU64;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

// parity: TBaseStratCommand ordinal base (head of the CMD_* series below)
#[allow(dead_code)]
const BASE_STRAT_CLASS_CMD_ID_BASE: u8 = 0;
const CMD_SNAPSHOT_REQUEST: u8 = 1;
const CMD_SNAPSHOT: u8 = 2;
const CMD_DELETE: u8 = 3;
const CMD_SELL_PRICE_UPDATE: u8 = 4;
const CMD_CHECKED_SYNC: u8 = 5;
const CMD_CHECKED_ECHO: u8 = 6;
const CMD_SCHEMA_REQUEST: u8 = 7;
const CMD_SCHEMA: u8 = 8;

pub(crate) fn is_snapshot_request_payload(payload: &[u8]) -> bool {
    payload.first().copied() == Some(CMD_SNAPSHOT_REQUEST)
}

pub(crate) fn is_schema_request_payload(payload: &[u8]) -> bool {
    payload.first().copied() == Some(CMD_SCHEMA_REQUEST)
}

pub(crate) fn is_schema_payload(payload: &[u8]) -> bool {
    payload.first().copied() == Some(CMD_SCHEMA)
}

/// A single TStratCheckedItem element: `StrategyID:UInt64 + Checked:bool` (9 bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StratCheckedItem {
    pub strategy_id: u64,
    pub checked: bool,
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
struct WireStratCheckedItem {
    strategy_id: LeU64,
    checked: u8,
}

pub(crate) const STRAT_CHECKED_ITEM_SIZE: usize = std::mem::size_of::<WireStratCheckedItem>();
const _: [(); 9] = [(); STRAT_CHECKED_ITEM_SIZE];

impl StratCheckedItem {
    #[cfg(test)]
    fn from_wire(wire: WireStratCheckedItem) -> Self {
        Self {
            strategy_id: wire.strategy_id.get(),
            checked: wire.checked != 0,
        }
    }

    fn to_wire(self) -> WireStratCheckedItem {
        WireStratCheckedItem {
            strategy_id: LeU64::new(self.strategy_id),
            checked: self.checked as u8,
        }
    }

    #[cfg(test)]
    pub(crate) fn read_from(data: &[u8], pos: &mut usize) -> Option<Self> {
        if *pos + STRAT_CHECKED_ITEM_SIZE > data.len() {
            return None;
        }
        let wire =
            WireStratCheckedItem::read_from_bytes(&data[*pos..*pos + STRAT_CHECKED_ITEM_SIZE])
                .ok()?;
        *pos += STRAT_CHECKED_ITEM_SIZE;
        Some(Self::from_wire(wire))
    }

    pub(crate) fn read_from_delphi_stream(data: &[u8], pos: &mut usize) -> Self {
        let mut strategy_id = [0u8; 8];
        let id_bytes = data.len().saturating_sub(*pos).min(8);
        if id_bytes > 0 {
            strategy_id[..id_bytes].copy_from_slice(&data[*pos..*pos + id_bytes]);
            *pos += id_bytes;
        }
        let checked = if *pos < data.len() {
            let checked = data[*pos] != 0;
            *pos += 1;
            checked
        } else {
            false
        };
        Self {
            strategy_id: u64::from_le_bytes(strategy_id),
            checked,
        }
    }

    pub(crate) fn write_to(self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.to_wire().as_bytes());
    }
}

/// `TStratSnapshot` (CmdId=2). Priority=Sliced. UKey=UK_StratSnapshot (UID is always = 1, overlap).
#[cfg_attr(feature = "diagnostics", allow(dead_code))]
#[derive(Debug, Clone)]
pub struct StratSnapshot {
    pub server_epoch: u64,
    pub client_max_last_date: u64,
    /// True if this is a full snapshot (Markets and Strats are replaced). False means partial.
    pub full: bool,
    /// Raw `TStrategySerializer` bin payload. Decoder/writer are in `commands::strategy_serializer`.
    pub data: Vec<u8>,
}

/// `TStratDelete` (CmdId=3).
#[derive(Debug, Clone)]
pub struct StratDelete {
    pub strategy_id: u64,
    /// Path in the strategy tree. Soft-read: absent in older versions.
    pub folder_path: String,
}

/// `TStratSellPriceUpdate` (CmdId=4). Client→server command.
/// UKey=UK_StratSellPriceUpdate (UID = strategy_id).
#[cfg_attr(feature = "diagnostics", allow(dead_code))]
#[derive(Debug, Clone, Copy)]
pub struct StratSellPriceUpdate {
    pub strategy_id: u64,
    pub sell_price: f64,
}

/// `TStratCheckedSync` (CmdId=5). Priority=Sliced.
/// `is_delta=true` means Items contains only entries changed relative to the previous sync.
#[derive(Debug, Clone)]
pub struct StratCheckedSync {
    pub items: Vec<StratCheckedItem>,
    pub is_delta: bool,
}

/// `TStratCheckedEcho` (CmdId=6) — server ACK for the client delta.
#[derive(Debug, Clone)]
pub struct StratCheckedEcho {
    pub items: Vec<StratCheckedItem>,
}

/// `TStratSchema` (CmdId=8). Priority=Sliced.
#[derive(Debug, Clone)]
pub struct StratSchema {
    /// Raw-deflate `StrategySchemaBuilder` blob.
    pub data: Vec<u8>,
}

/// All parseable incoming MPC_Strat subcommands.
#[cfg_attr(feature = "diagnostics", allow(dead_code))]
#[derive(Debug, Clone)]
pub enum StratCommand {
    SnapshotRequest {
        uid: u64,
    },
    Snapshot(StratSnapshot),
    Delete(StratDelete),
    SellPriceUpdate(StratSellPriceUpdate),
    CheckedSync(StratCheckedSync),
    CheckedEcho(StratCheckedEcho),
    SchemaRequest {
        uid: u64,
    },
    Schema(StratSchema),
    /// Header is valid, but protocol version is newer than this library can
    /// parse. Delphi registry marks this as `FSkipped` and returns the base
    /// command class.
    Skipped {
        cmd_id: u8,
        uid: u64,
        ver: u16,
    },
    Unknown {
        cmd_id: u8,
        uid: u64,
    },
}

impl StratCommand {
    /// Parse a TBaseStratCommand payload (after MPC_Strat dispatch in data_read_int).
    /// Wire-form: `cmd_id(1) + ver(2) + UID(8) + class-specific`.
    /// Version gate: if `ver > 3`, return Skipped (Delphi registry
    /// `FSkipped` forward-compat path).
    #[doc(hidden)]
    pub fn parse(payload: &[u8]) -> Option<Self> {
        if payload.len() < 11 {
            return None;
        }
        let cmd_id = payload[0];
        let ver = u16::from_le_bytes([payload[1], payload[2]]);
        let uid = u64::from_le_bytes(payload[3..11].try_into().unwrap());
        if ver > CURRENT_PROTO_CMD_VER {
            return Some(StratCommand::Skipped { cmd_id, uid, ver });
        }
        let mut pos = 11usize;

        match cmd_id {
            CMD_SNAPSHOT_REQUEST => Some(StratCommand::SnapshotRequest { uid }),
            CMD_SNAPSHOT => {
                // ServerEpoch:UInt64(8) + ClientMaxLastDate:UInt64(8) + Size:Cardinal(4) + Full:bool(1) + Data:bytes(Size)
                let server_epoch = read_u64_zero_tail(payload, &mut pos);
                let client_max_last_date = read_u64_zero_tail(payload, &mut pos);
                let size = read_u32_zero_tail(payload, &mut pos) as usize;
                let full = read_u8_zero_tail(payload, &mut pos) != 0;
                let data = if pos + size > payload.len() {
                    // Delphi `TStratSnapshot.CreateFromStream`: declared Size
                    // larger than remaining bytes sets `Data := nil`, seeks to
                    // stream end, and lets `ProcessStratCommand` reject the
                    // invalid snapshot without applying epoch/state. Rust has
                    // no nil stream in this public struct; an empty payload
                    // follows the same later path as a malformed Size=0
                    // snapshot: serializer decode fails and no Snapshot event
                    // is emitted.
                    Vec::new()
                } else {
                    payload[pos..pos + size].to_vec()
                };
                Some(StratCommand::Snapshot(StratSnapshot {
                    server_epoch,
                    client_max_last_date,
                    full,
                    data,
                }))
            }
            CMD_DELETE => {
                let strategy_id = read_u64_zero_tail(payload, &mut pos);
                // Soft-read folder_path
                let folder_path = if pos < payload.len() {
                    read_string(payload, &mut pos)?
                } else {
                    String::new()
                };
                Some(StratCommand::Delete(StratDelete {
                    strategy_id,
                    folder_path,
                }))
            }
            CMD_SELL_PRICE_UPDATE => {
                let strategy_id = read_u64_zero_tail(payload, &mut pos);
                let sell_price = f64::from_le_bytes(read_zero_tail::<8>(payload, &mut pos));
                Some(StratCommand::SellPriceUpdate(StratSellPriceUpdate {
                    strategy_id,
                    sell_price,
                }))
            }
            CMD_CHECKED_SYNC => {
                let (items, end_pos) = read_checked_items(payload, pos)?;
                pos = end_pos;
                let is_delta = if pos < payload.len() {
                    payload[pos] != 0
                } else {
                    true // default for older packets
                };
                Some(StratCommand::CheckedSync(StratCheckedSync {
                    items,
                    is_delta,
                }))
            }
            CMD_CHECKED_ECHO => {
                let (items, _) = read_checked_items(payload, pos)?;
                Some(StratCommand::CheckedEcho(StratCheckedEcho { items }))
            }
            CMD_SCHEMA_REQUEST => Some(StratCommand::SchemaRequest { uid }),
            CMD_SCHEMA => {
                if pos + 4 > payload.len() {
                    return None;
                }
                let size = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                let data = if pos + size > payload.len() {
                    Vec::new()
                } else {
                    payload[pos..pos + size].to_vec()
                };
                Some(StratCommand::Schema(StratSchema { data }))
            }
            _ => Some(StratCommand::Unknown { cmd_id, uid }),
        }
    }
}

fn read_zero_tail<const N: usize>(data: &[u8], pos: &mut usize) -> [u8; N] {
    let mut out = [0u8; N];
    if *pos < data.len() {
        let n = (data.len() - *pos).min(N);
        out[..n].copy_from_slice(&data[*pos..*pos + n]);
        *pos += n;
    }
    out
}

fn read_u8_zero_tail(data: &[u8], pos: &mut usize) -> u8 {
    read_zero_tail::<1>(data, pos)[0]
}

fn read_u32_zero_tail(data: &[u8], pos: &mut usize) -> u32 {
    u32::from_le_bytes(read_zero_tail::<4>(data, pos))
}

fn read_u64_zero_tail(data: &[u8], pos: &mut usize) -> u64 {
    u64::from_le_bytes(read_zero_tail::<8>(data, pos))
}

fn read_checked_items(payload: &[u8], mut pos: usize) -> Option<(Vec<StratCheckedItem>, usize)> {
    if pos + 2 > payload.len() {
        return None;
    }
    let count = u16::from_le_bytes([payload[pos], payload[pos + 1]]) as usize;
    pos += 2;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        items.push(StratCheckedItem::read_from_delphi_stream(payload, &mut pos));
    }
    Some((items, pos))
}

// ============================================================================
//  Builders
// ============================================================================

fn write_header(out: &mut Vec<u8>, cmd_id: u8, uid: u64) {
    out.push(cmd_id);
    out.extend_from_slice(&CURRENT_PROTO_CMD_VER.to_le_bytes());
    out.extend_from_slice(&uid.to_le_bytes());
}

/// `TStratSnapshotRequest` (CmdId=1, server → client).
///
/// This builder is crate-internal on purpose: Delphi server ignores the same
/// command when it is received from a client.
#[cfg(test)]
pub(crate) fn build_snapshot_request(uid: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(11);
    write_header(&mut out, CMD_SNAPSHOT_REQUEST, uid);
    out
}

/// `TStratSchemaRequest` (CmdId=7).
#[doc(hidden)]
pub(crate) fn build_schema_request(uid: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(11);
    write_header(&mut out, CMD_SCHEMA_REQUEST, uid);
    out
}

/// `TStratSnapshot` (CmdId=2).
///
/// `data` is the compressed `TStrategySerializer` payload (`TStratSnapshot.Data`),
/// not the full command body. This builder adds the Delphi fields
/// `ServerEpoch`, `ClientMaxLastDate`, `Size`, and `Full`.
///
/// An empty `data` slice is normalized to the valid Delphi meaning "empty
/// strategy list": a non-empty `TStrategySerializer` payload with zero
/// dictionaries and zero strategies. A wire `Size=0` snapshot is malformed for
/// normal client sends.
#[doc(hidden)]
pub(crate) fn build_snapshot(
    uid: u64,
    server_epoch: u64,
    client_max_last_date: u64,
    full: bool,
    data: &[u8],
) -> Vec<u8> {
    let empty_payload;
    let data = if data.is_empty() {
        empty_payload = StrategyBatchBuilder::empty_payload();
        empty_payload.as_slice()
    } else {
        data
    };

    let mut out = Vec::with_capacity(11 + 8 + 8 + 4 + 1 + data.len());
    write_header(&mut out, CMD_SNAPSHOT, uid);
    out.extend_from_slice(&server_epoch.to_le_bytes());
    out.extend_from_slice(&client_max_last_date.to_le_bytes());
    out.extend_from_slice(&(data.len() as u32).to_le_bytes());
    out.push(full as u8);
    out.extend_from_slice(data);
    out
}

/// Build a `TStratSnapshot` from decoded strategy snapshots.
///
/// This is the typed counterpart to Delphi `TStratSnapshot.CreateFromStrats` /
/// `CreateFromList`: it serializes strategies through `StrategyBatchBuilder`,
/// computes `ClientMaxLastDate`, and wraps the result as CmdId=2. The builder
/// needs live `TStratSchema` for Delphi field order/default parity.
#[doc(hidden)]
pub(crate) fn build_snapshot_from_strategies(
    uid: u64,
    server_epoch: u64,
    full: bool,
    schema: &StrategySchema,
    strategies: &[StrategySerializerSnapshot],
) -> Vec<u8> {
    let mut builder = StrategyBatchBuilder::new(schema);
    let mut client_max_last_date = 0u64;
    for strategy in strategies {
        client_max_last_date = client_max_last_date.max(strategy.last_date);
        builder.write_strategy(strategy);
    }
    let data = builder.finalize();
    build_snapshot(uid, server_epoch, client_max_last_date, full, &data)
}

/// `TStratDelete` (CmdId=3).
#[doc(hidden)]
pub(crate) fn build_delete(uid: u64, strategy_id: u64, folder_path: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_header(&mut out, CMD_DELETE, uid);
    out.extend_from_slice(&strategy_id.to_le_bytes());
    write_string(&mut out, folder_path);
    out
}

/// `TStratSellPriceUpdate` (CmdId=4).
#[doc(hidden)]
pub(crate) fn build_sell_price_update(uid: u64, strategy_id: u64, sell_price: f64) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_header(&mut out, CMD_SELL_PRICE_UPDATE, uid);
    out.extend_from_slice(&strategy_id.to_le_bytes());
    out.extend_from_slice(&sell_price.to_le_bytes());
    out
}

/// `TStratCheckedSync` (CmdId=5).
#[doc(hidden)]
pub(crate) fn build_checked_sync(uid: u64, items: &[StratCheckedItem], is_delta: bool) -> Vec<u8> {
    let count = items.len() as u16;
    let count_usize = usize::from(count);
    let mut out = Vec::with_capacity(11 + 2 + count_usize * 9 + 1);
    write_header(&mut out, CMD_CHECKED_SYNC, uid);
    out.extend_from_slice(&count.to_le_bytes());
    for it in items.iter().take(count_usize) {
        it.write_to(&mut out);
    }
    out.push(is_delta as u8);
    out
}

/// `TStratCheckedEcho` (CmdId=6, server → client).
///
/// Crate-internal test helper: Delphi clients receive this command, but do not
/// send it.
#[cfg(test)]
pub(crate) fn build_checked_echo(uid: u64, items: &[StratCheckedItem]) -> Vec<u8> {
    let count = items.len() as u16;
    let count_usize = usize::from(count);
    let mut out = Vec::with_capacity(11 + 2 + count_usize * 9);
    write_header(&mut out, CMD_CHECKED_ECHO, uid);
    out.extend_from_slice(&count.to_le_bytes());
    for it in items.iter().take(count_usize) {
        it.write_to(&mut out);
    }
    out
}

#[cfg(test)]
mod tests;
