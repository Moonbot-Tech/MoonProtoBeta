//! MPC_Strat канал — 9 подкоманд TBaseStratCommand.
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
//! - 7 — TStratSchemaRequest (C→S, empty)
//! - 8 — TStratSchema (S→C, Sliced, raw-deflate schema blob)
//!
//! ## Замечание про TStratSnapshot.Data
//! `Data: bytes(Size)` — это сериализованный bin-формат `TStrategySerializer` (RTTI-driven,
//! ~1118 строк). Декодер и writer находятся в `commands::strategy_serializer`.

use super::registry::{read_string, write_string, CURRENT_PROTO_CMD_VER};
use super::strategy_schema::StrategySchema;
use super::strategy_serializer::{
    StrategyBatchBuilder, StrategySnapshot as StrategySerializerSnapshot,
};
use zerocopy::byteorder::little_endian::U64 as LeU64;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

const BASE_STRAT_CLASS_CMD_ID_BASE: u8 = 0; // TBaseStratCommand
const CMD_SNAPSHOT_REQUEST: u8 = 1;
const CMD_SNAPSHOT: u8 = 2;
const CMD_DELETE: u8 = 3;
const CMD_SELL_PRICE_UPDATE: u8 = 4;
const CMD_CHECKED_SYNC: u8 = 5;
const CMD_CHECKED_ECHO: u8 = 6;
const CMD_SCHEMA_REQUEST: u8 = 7;
const CMD_SCHEMA: u8 = 8;

/// Один элемент TStratCheckedItem: `StrategyID:UInt64 + Checked:bool` (9 байт).
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

pub const STRAT_CHECKED_ITEM_SIZE: usize = std::mem::size_of::<WireStratCheckedItem>();
const _: [(); 9] = [(); STRAT_CHECKED_ITEM_SIZE];

impl StratCheckedItem {
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

    pub(crate) fn write_to(self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.to_wire().as_bytes());
    }
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

/// `TStratSchema` (CmdId=8). Priority=Sliced.
#[derive(Debug, Clone)]
pub struct StratSchema {
    /// Raw-deflate `StrategySchemaBuilder` blob.
    pub data: Vec<u8>,
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
    SchemaRequest { uid: u64 },
    Schema(StratSchema),
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
            CMD_SCHEMA_REQUEST => Some(StratCommand::SchemaRequest { uid }),
            CMD_SCHEMA => {
                if pos + 4 > payload.len() {
                    return None;
                }
                let size = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                if pos + size > payload.len() {
                    return None;
                }
                let data = payload[pos..pos + size].to_vec();
                Some(StratCommand::Schema(StratSchema { data }))
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
        let mut strategy_id = [0u8; 8];
        let id_bytes = payload.len().saturating_sub(pos).min(8);
        if id_bytes > 0 {
            strategy_id[..id_bytes].copy_from_slice(&payload[pos..pos + id_bytes]);
            pos += id_bytes;
        }
        let checked = if pos < payload.len() {
            let checked = payload[pos] != 0;
            pos += 1;
            checked
        } else {
            false
        };
        items.push(StratCheckedItem {
            strategy_id: u64::from_le_bytes(strategy_id),
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

/// `TStratSchemaRequest` (CmdId=7).
pub fn build_schema_request(uid: u64) -> Vec<u8> {
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
pub fn build_snapshot(
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
pub fn build_snapshot_from_strategies(
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

/// `TStratCheckedEcho` (CmdId=6).
pub fn build_checked_echo(uid: u64, items: &[StratCheckedItem]) -> Vec<u8> {
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

#[allow(dead_code)]
fn _silence_unused_const() {
    let _ = BASE_STRAT_CLASS_CMD_ID_BASE;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::strategy_schema::{
        StrategyFieldLayout, StrategyFieldType, StrategyFieldUiKind, StrategySchemaField,
        StrategySchemaKind,
    };

    fn schema_for_name_field() -> StrategySchema {
        StrategySchema {
            format_version: 1,
            kinds: vec![StrategySchemaKind {
                ordinal: 1,
                name: "Kind1".to_string(),
            }],
            fields: vec![StrategySchemaField {
                name: "Name".to_string(),
                raw_type_id: crate::commands::strategy_serializer::TID_STRING,
                type_id: StrategyFieldType::String,
                raw_flags: 0,
                ui_kind: StrategyFieldUiKind::Edit,
                layout: StrategyFieldLayout::None,
                default_value: None,
                visible_kind_ordinals: vec![1],
                static_picklist_raw: None,
                static_picklist: Vec::new(),
                dynamic_picklist: None,
            }],
        }
    }

    #[test]
    fn strat_checked_item_uses_private_wire_struct() {
        assert_eq!(std::mem::size_of::<WireStratCheckedItem>(), 9);
        assert_eq!(STRAT_CHECKED_ITEM_SIZE, 9);

        let item = StratCheckedItem {
            strategy_id: 0x0102_0304_0506_0708,
            checked: true,
        };
        let mut bytes = Vec::new();
        item.write_to(&mut bytes);

        let mut expected = Vec::new();
        expected.extend_from_slice(&0x0102_0304_0506_0708u64.to_le_bytes());
        expected.push(1);
        assert_eq!(bytes, expected);

        let mut pos = 0;
        let parsed = StratCheckedItem::read_from(&bytes, &mut pos).expect("valid item");
        assert_eq!(pos, STRAT_CHECKED_ITEM_SIZE);
        assert_eq!(parsed, item);
    }

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
    fn parse_schema_request() {
        let payload = build_schema_request(43);
        let cmd = StratCommand::parse(&payload).unwrap();
        match cmd {
            StratCommand::SchemaRequest { uid } => assert_eq!(uid, 43),
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
    fn checked_items_read_declared_count_with_zero_tail_like_delphi_stream() {
        let mut payload = vec![CMD_CHECKED_SYNC, 0x03, 0x00];
        payload.extend_from_slice(&7u64.to_le_bytes());
        payload.extend_from_slice(&3u16.to_le_bytes());
        StratCheckedItem {
            strategy_id: 100,
            checked: true,
        }
        .write_to(&mut payload);
        payload.extend_from_slice(&0x0102_0304u32.to_le_bytes());

        let cmd = StratCommand::parse(&payload).unwrap();
        match cmd {
            StratCommand::CheckedSync(s) => {
                assert_eq!(s.items.len(), 3);
                assert_eq!(
                    s.items[0],
                    StratCheckedItem {
                        strategy_id: 100,
                        checked: true
                    }
                );
                assert_eq!(
                    s.items[1],
                    StratCheckedItem {
                        strategy_id: 0x0102_0304,
                        checked: false
                    },
                    "Delphi dynamic array items are zero-initialized; a short Read leaves the missing high bytes and bool as zero"
                );
                assert_eq!(
                    s.items[2],
                    StratCheckedItem {
                        strategy_id: 0,
                        checked: false
                    }
                );
                assert!(
                    s.is_delta,
                    "missing trailing IsDelta byte keeps Delphi old-packet default"
                );
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn checked_word_count_builders_write_only_declared_wrapped_count_like_delphi() {
        let items: Vec<_> = (0..65_537u64)
            .map(|i| StratCheckedItem {
                strategy_id: i + 500,
                checked: i % 2 == 0,
            })
            .collect();

        let payload = build_checked_sync(7, &items, false);
        assert_eq!(payload.len(), 11 + 2 + 9 + 1);
        let cmd = StratCommand::parse(&payload).unwrap();
        match cmd {
            StratCommand::CheckedSync(s) => {
                assert_eq!(s.items, vec![items[0]]);
                assert!(!s.is_delta);
            }
            _ => panic!("wrong variant"),
        }

        let payload = build_checked_echo(8, &items);
        assert_eq!(payload.len(), 11 + 2 + 9);
        let cmd = StratCommand::parse(&payload).unwrap();
        match cmd {
            StratCommand::CheckedEcho(e) => {
                assert_eq!(e.items, vec![items[0]]);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_snapshot_declared_size_over_remaining_as_invalid_snapshot_like_delphi() {
        let mut payload = vec![CMD_SNAPSHOT, 0x03, 0x00];
        payload.extend_from_slice(&42u64.to_le_bytes());
        payload.extend_from_slice(&99u64.to_le_bytes());
        payload.extend_from_slice(&77u64.to_le_bytes());
        payload.extend_from_slice(&8u32.to_le_bytes());
        payload.push(1);
        payload.extend_from_slice(&[1, 2, 3]);

        let cmd = StratCommand::parse(&payload).unwrap();
        match cmd {
            StratCommand::Snapshot(s) => {
                assert_eq!(s.server_epoch, 99);
                assert_eq!(s.client_max_last_date, 77);
                assert!(s.full);
                assert!(
                    s.data.is_empty(),
                    "Delphi sets Data=nil and ProcessStratCommand rejects the snapshot without applying epoch/state"
                );
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
    fn parse_schema_with_data() {
        let mut payload = vec![CMD_SCHEMA, 0x03, 0x00];
        payload.extend_from_slice(&1u64.to_le_bytes());
        payload.extend_from_slice(&3u32.to_le_bytes());
        payload.extend_from_slice(&[9, 8, 7]);
        let cmd = StratCommand::parse(&payload).unwrap();
        match cmd {
            StratCommand::Schema(s) => assert_eq!(s.data, vec![9, 8, 7]),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_schema_rejects_truncated_data_like_delphi_nil_data_guard() {
        let mut payload = vec![CMD_SCHEMA, 0x03, 0x00];
        payload.extend_from_slice(&1u64.to_le_bytes());
        payload.extend_from_slice(&4u32.to_le_bytes());
        payload.extend_from_slice(&[9, 8, 7]);
        assert!(StratCommand::parse(&payload).is_none());
    }

    #[test]
    fn build_snapshot_from_strategies_computes_max_last_date() {
        use crate::commands::strategy_serializer::{FieldValue, StrategyFields, StrategySnapshot};
        let mut fields = StrategyFields::new();
        fields.insert("Name", FieldValue::String("A".to_string()));
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
        let schema = schema_for_name_field();
        let raw = build_snapshot_from_strategies(78, 11, false, &schema, &strategies);
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
    fn build_empty_snapshot_from_strategies_keeps_nonzero_serializer_payload() {
        let schema = schema_for_name_field();
        let raw = build_snapshot_from_strategies(79, 0, true, &schema, &[]);
        let cmd = StratCommand::parse(&raw).unwrap();
        match cmd {
            StratCommand::Snapshot(s) => {
                assert_eq!(s.server_epoch, 0);
                assert_eq!(s.client_max_last_date, 0);
                assert!(s.full);
                assert!(
                    !s.data.is_empty(),
                    "empty strategy list still serializes as a valid TStrategySerializer payload"
                );
                let batch = crate::commands::strategy_serializer::parse_strategy_batch(&s.data)
                    .expect("empty strategy payload must parse");
                assert!(batch.names.is_empty());
                assert!(batch.paths.is_empty());
                assert!(batch.strategies.is_empty());
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn build_snapshot_normalizes_empty_raw_payload_to_empty_serializer() {
        let raw = build_snapshot(79, 3, 0, true, &[]);
        let cmd = StratCommand::parse(&raw).unwrap();
        match cmd {
            StratCommand::Snapshot(s) => {
                assert_eq!(s.server_epoch, 3);
                assert_eq!(s.client_max_last_date, 0);
                assert!(s.full);
                assert!(
                    !s.data.is_empty(),
                    "public raw snapshot builder must not emit Size=0"
                );
                let batch = crate::commands::strategy_serializer::parse_strategy_batch(&s.data)
                    .expect("normalized empty payload must parse");
                assert!(batch.names.is_empty());
                assert!(batch.paths.is_empty());
                assert!(batch.strategies.is_empty());
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
