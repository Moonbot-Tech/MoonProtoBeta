//! MPC_Strat канал — 7 подкоманд TBaseStratCommand.
//!
//! Источник Delphi: `MoonProto/MoonProtoStratStruct.pas` (~408 строк).
//!
//! ## CmdId маппинг
//! - 0 — TBaseStratCommand (base)
//! - 1 — TStratSnapshotRequest (empty, S→C)
//! - 2 — TStratSnapshot (both directions, Sliced, UK_StratSnapshot)
//! - 3 — TStratDelete (S↔C)
//! - 4 — TStratSellPriceUpdate (C→S, UK_StratSellPriceUpdate)
//! - 5 — TStratCheckedSync (S↔C, Sliced)
//! - 6 — TStratCheckedEcho (C→S ACK на дельту Checked)
//!
//! ## Замечание про TStratSnapshot.Data
//! `Data: bytes(Size)` — это сериализованный bin-формат `TStrategySerializer` (RTTI-driven,
//! ~1118 строк). Декодер и writer находятся в `commands::strategy_serializer`.

use super::registry::{read_string, write_string, CURRENT_PROTO_CMD_VER};
use super::strategy_serializer::{
    StrategyBatchBuilder, StrategySnapshot as StrategySerializerSnapshot,
};

const BASE_STRAT_CLASS_CMD_ID_BASE: u8 = 0; // TBaseStratCommand
const CMD_SNAPSHOT_REQUEST: u8 = 1;
const CMD_SNAPSHOT: u8 = 2;
const CMD_DELETE: u8 = 3;
const CMD_SELL_PRICE_UPDATE: u8 = 4;
const CMD_CHECKED_SYNC: u8 = 5;
const CMD_CHECKED_ECHO: u8 = 6;

/// Один элемент TStratCheckedItem: `StrategyID:UInt64 + Checked:bool` (9 байт).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StratCheckedItem {
    pub strategy_id: u64,
    pub checked: bool,
}

/// `TStratSnapshot` (CmdId=2). Priority=Sliced. UKey=UK_StratSnapshot (UID всегда = 1, overlap).
#[derive(Debug, Clone)]
pub struct StratSnapshot {
    pub server_epoch: u64,
    pub client_max_last_date: u64,
    /// True если это полный snapshot (Markets и Strats заменяются). False — частичный.
    pub full: bool,
    /// Сырой `TStrategySerializer` bin payload. Декодер/writer — в `commands::strategy_serializer`.
    pub data: Vec<u8>,
}

/// `TStratDelete` (CmdId=3).
#[derive(Debug, Clone)]
pub struct StratDelete {
    pub strategy_id: u64,
    /// Path в дереве стратегий. Soft-read: в старых версиях отсутствует.
    pub folder_path: String,
}

/// `TStratSellPriceUpdate` (CmdId=4). Client→server command.
/// UKey=UK_StratSellPriceUpdate (UID = strategy_id).
#[derive(Debug, Clone, Copy)]
pub struct StratSellPriceUpdate {
    pub strategy_id: u64,
    pub sell_price: f64,
}

/// `TStratCheckedSync` (CmdId=5). Priority=Sliced.
/// `is_delta=true` означает что Items — только изменённые относительно prev sync.
#[derive(Debug, Clone)]
pub struct StratCheckedSync {
    pub items: Vec<StratCheckedItem>,
    pub is_delta: bool,
}

/// `TStratCheckedEcho` (CmdId=6) — клиентский ACK на дельту.
#[derive(Debug, Clone)]
pub struct StratCheckedEcho {
    pub items: Vec<StratCheckedItem>,
}

/// Все парсимые входящие подкоманды MPC_Strat.
#[derive(Debug, Clone)]
pub enum StratCommand {
    SnapshotRequest { uid: u64 },
    Snapshot(StratSnapshot),
    Delete(StratDelete),
    SellPriceUpdate(StratSellPriceUpdate),
    CheckedSync(StratCheckedSync),
    CheckedEcho(StratCheckedEcho),
    Unknown { cmd_id: u8, uid: u64 },
}

impl StratCommand {
    /// Распарсить TBaseStratCommand payload (после dispatch'a по MPC_Strat в data_read_int).
    /// Wire-form: `cmd_id(1) + ver(2) + UID(8) + class-specific`.
    /// Version gate: если `ver > 3` — возвращаем Unknown (forward-compat skip).
    pub fn parse(payload: &[u8]) -> Option<Self> {
        if payload.len() < 11 {
            return None;
        }
        let cmd_id = payload[0];
        let ver = u16::from_le_bytes([payload[1], payload[2]]);
        let uid = u64::from_le_bytes(payload[3..11].try_into().unwrap());
        if ver > CURRENT_PROTO_CMD_VER {
            return Some(StratCommand::Unknown { cmd_id, uid });
        }
        let mut pos = 11usize;

        match cmd_id {
            CMD_SNAPSHOT_REQUEST => Some(StratCommand::SnapshotRequest { uid }),
            CMD_SNAPSHOT => {
                // ServerEpoch:UInt64(8) + ClientMaxLastDate:UInt64(8) + Size:Cardinal(4) + Full:bool(1) + Data:bytes(Size)
                if pos + 8 + 8 + 4 + 1 > payload.len() {
                    return None;
                }
                let server_epoch = u64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
                pos += 8;
                let client_max_last_date =
                    u64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
                pos += 8;
                let size = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                let full = payload[pos] != 0;
                pos += 1;
                if pos + size > payload.len() {
                    return None;
                }
                let data = payload[pos..pos + size].to_vec();
                Some(StratCommand::Snapshot(StratSnapshot {
                    server_epoch,
                    client_max_last_date,
                    full,
                    data,
                }))
            }
            CMD_DELETE => {
                if pos + 8 > payload.len() {
                    return None;
                }
                let strategy_id = u64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
                pos += 8;
                // Soft-read folder_path
                let folder_path = if pos < payload.len() {
                    read_string(payload, &mut pos).unwrap_or_default()
                } else {
                    String::new()
                };
                Some(StratCommand::Delete(StratDelete {
                    strategy_id,
                    folder_path,
                }))
            }
            CMD_SELL_PRICE_UPDATE => {
                if pos + 16 > payload.len() {
                    return None;
                }
                let strategy_id = u64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
                pos += 8;
                let sell_price = f64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
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
                    true // default для старых пакетов
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
            _ => Some(StratCommand::Unknown { cmd_id, uid }),
        }
    }
}

fn read_checked_items(payload: &[u8], mut pos: usize) -> Option<(Vec<StratCheckedItem>, usize)> {
    if pos + 2 > payload.len() {
        return None;
    }
    let count = u16::from_le_bytes([payload[pos], payload[pos + 1]]) as usize;
    pos += 2;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        if pos + 9 > payload.len() {
            return None;
        }
        let strategy_id = u64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let checked = payload[pos] != 0;
        pos += 1;
        items.push(StratCheckedItem {
            strategy_id,
            checked,
        });
    }
    Some((items, pos))
}

// ============================================================================
//  Builders (C→S)
// ============================================================================

fn write_header(out: &mut Vec<u8>, cmd_id: u8, uid: u64) {
    out.push(cmd_id);
    out.extend_from_slice(&CURRENT_PROTO_CMD_VER.to_le_bytes());
    out.extend_from_slice(&uid.to_le_bytes());
}

/// `TStratSnapshotRequest` (CmdId=1).
pub fn build_snapshot_request(uid: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(11);
    write_header(&mut out, CMD_SNAPSHOT_REQUEST, uid);
    out
}

/// `TStratSnapshot` (CmdId=2).
///
/// `data` is the compressed `TStrategySerializer` payload (`TStratSnapshot.Data`),
/// not the full command body. This builder adds the Delphi fields
/// `ServerEpoch`, `ClientMaxLastDate`, `Size`, and `Full`.
pub fn build_snapshot(
    uid: u64,
    server_epoch: u64,
    client_max_last_date: u64,
    full: bool,
    data: &[u8],
) -> Vec<u8> {
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
/// computes `ClientMaxLastDate`, and wraps the result as CmdId=2.
pub fn build_snapshot_from_strategies(
    uid: u64,
    server_epoch: u64,
    full: bool,
    strategies: &[StrategySerializerSnapshot],
) -> Vec<u8> {
    let mut builder = StrategyBatchBuilder::new();
    let mut client_max_last_date = 0u64;
    for strategy in strategies {
        client_max_last_date = client_max_last_date.max(strategy.last_date);
        builder.write_strategy(strategy);
    }
    let data = builder.finalize();
    build_snapshot(uid, server_epoch, client_max_last_date, full, &data)
}

/// `TStratDelete` (CmdId=3).
pub fn build_delete(uid: u64, strategy_id: u64, folder_path: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_header(&mut out, CMD_DELETE, uid);
    out.extend_from_slice(&strategy_id.to_le_bytes());
    write_string(&mut out, folder_path);
    out
}

/// `TStratSellPriceUpdate` (CmdId=4).
pub fn build_sell_price_update(uid: u64, strategy_id: u64, sell_price: f64) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_header(&mut out, CMD_SELL_PRICE_UPDATE, uid);
    out.extend_from_slice(&strategy_id.to_le_bytes());
    out.extend_from_slice(&sell_price.to_le_bytes());
    out
}

/// `TStratCheckedSync` (CmdId=5).
pub fn build_checked_sync(uid: u64, items: &[StratCheckedItem], is_delta: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(11 + 2 + items.len() * 9 + 1);
    write_header(&mut out, CMD_CHECKED_SYNC, uid);
    out.extend_from_slice(&(items.len() as u16).to_le_bytes());
    for it in items {
        out.extend_from_slice(&it.strategy_id.to_le_bytes());
        out.push(it.checked as u8);
    }
    out.push(is_delta as u8);
    out
}

/// `TStratCheckedEcho` (CmdId=6).
pub fn build_checked_echo(uid: u64, items: &[StratCheckedItem]) -> Vec<u8> {
    let mut out = Vec::with_capacity(11 + 2 + items.len() * 9);
    write_header(&mut out, CMD_CHECKED_ECHO, uid);
    out.extend_from_slice(&(items.len() as u16).to_le_bytes());
    for it in items {
        out.extend_from_slice(&it.strategy_id.to_le_bytes());
        out.push(it.checked as u8);
    }
    out
}

#[allow(dead_code)]
fn _silence_unused_const() {
    let _ = BASE_STRAT_CLASS_CMD_ID_BASE;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_snapshot_request() {
        // CmdId=1 + ver=3 + UID=42
        let mut payload = vec![CMD_SNAPSHOT_REQUEST, 0x03, 0x00];
        payload.extend_from_slice(&42u64.to_le_bytes());
        let cmd = StratCommand::parse(&payload).unwrap();
        match cmd {
            StratCommand::SnapshotRequest { uid } => assert_eq!(uid, 42),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_sell_price_update() {
        // CmdId=4 + ver=3 + UID=1 + strategy_id=99 + sell_price=123.45
        let mut payload = vec![CMD_SELL_PRICE_UPDATE, 0x03, 0x00];
        payload.extend_from_slice(&1u64.to_le_bytes());
        payload.extend_from_slice(&99u64.to_le_bytes());
        payload.extend_from_slice(&123.45f64.to_le_bytes());
        let cmd = StratCommand::parse(&payload).unwrap();
        match cmd {
            StratCommand::SellPriceUpdate(u) => {
                assert_eq!(u.strategy_id, 99);
                assert_eq!(u.sell_price, 123.45);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_checked_sync_with_items() {
        let items = vec![
            StratCheckedItem {
                strategy_id: 100,
                checked: true,
            },
            StratCheckedItem {
                strategy_id: 200,
                checked: false,
            },
        ];
        let payload = build_checked_sync(7, &items, true);
        let cmd = StratCommand::parse(&payload).unwrap();
        match cmd {
            StratCommand::CheckedSync(s) => {
                assert_eq!(s.items, items);
                assert!(s.is_delta);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_delete_with_folder() {
        let payload = build_delete(8, 555, "MyFolder/Sub");
        let cmd = StratCommand::parse(&payload).unwrap();
        match cmd {
            StratCommand::Delete(d) => {
                assert_eq!(d.strategy_id, 555);
                assert_eq!(d.folder_path, "MyFolder/Sub");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_snapshot_with_data() {
        // CmdId=2 + ver=3 + UID=1 + ServerEpoch=10 + ClientMaxLastDate=20 + Size=4 + Full=true + Data=[1,2,3,4]
        let mut payload = vec![CMD_SNAPSHOT, 0x03, 0x00];
        payload.extend_from_slice(&1u64.to_le_bytes());
        payload.extend_from_slice(&10u64.to_le_bytes());
        payload.extend_from_slice(&20u64.to_le_bytes());
        payload.extend_from_slice(&4u32.to_le_bytes());
        payload.push(1); // full
        payload.extend_from_slice(&[1, 2, 3, 4]);
        let cmd = StratCommand::parse(&payload).unwrap();
        match cmd {
            StratCommand::Snapshot(s) => {
                assert_eq!(s.server_epoch, 10);
                assert_eq!(s.client_max_last_date, 20);
                assert!(s.full);
                assert_eq!(s.data, vec![1, 2, 3, 4]);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn build_snapshot_wraps_serializer_payload() {
        let payload = [1, 2, 3, 4];
        let raw = build_snapshot(77, 10, 20, true, &payload);
        let cmd = StratCommand::parse(&raw).unwrap();
        match cmd {
            StratCommand::Snapshot(s) => {
                assert_eq!(s.server_epoch, 10);
                assert_eq!(s.client_max_last_date, 20);
                assert!(s.full);
                assert_eq!(s.data, payload);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn build_snapshot_from_strategies_computes_max_last_date() {
        use crate::commands::strategy_serializer::{FieldValue, StrategySnapshot};
        use std::collections::HashMap;

        let mut fields = HashMap::new();
        fields.insert("Name".to_string(), FieldValue::String("A".to_string()));
        let strategies = vec![
            StrategySnapshot {
                strategy_id: 1,
                strategy_ver: 1,
                last_date: 10,
                checked: true,
                kind: 1,
                path: "P".to_string(),
                fields: fields.clone(),
            },
            StrategySnapshot {
                strategy_id: 2,
                strategy_ver: 1,
                last_date: 30,
                checked: false,
                kind: 1,
                path: "P".to_string(),
                fields,
            },
        ];
        let raw = build_snapshot_from_strategies(78, 11, false, &strategies);
        let cmd = StratCommand::parse(&raw).unwrap();
        match cmd {
            StratCommand::Snapshot(s) => {
                assert_eq!(s.server_epoch, 11);
                assert_eq!(s.client_max_last_date, 30);
                assert!(!s.full);
                let batch = crate::commands::strategy_serializer::parse_strategy_batch(&s.data)
                    .expect("strategy payload must parse");
                assert_eq!(batch.strategies.len(), 2);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn version_gate_returns_unknown() {
        // ver=99 > CURRENT_PROTO_CMD_VER=3 → Unknown.
        let mut payload = vec![CMD_SNAPSHOT, 99, 0];
        payload.extend_from_slice(&77u64.to_le_bytes());
        let cmd = StratCommand::parse(&payload).unwrap();
        match cmd {
            StratCommand::Unknown { cmd_id, uid } => {
                assert_eq!(cmd_id, CMD_SNAPSHOT);
                assert_eq!(uid, 77);
            }
            _ => panic!("wrong variant"),
        }
    }
}
