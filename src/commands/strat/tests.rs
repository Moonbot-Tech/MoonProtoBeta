use super::*;
use crate::commands::strategy_schema::{
    visible_kind_mask, StrategyFieldLayout, StrategyFieldType, StrategyFieldUiKind,
    StrategySchemaField, StrategySchemaKind,
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
            visible_kind_mask: visible_kind_mask(&[1]),
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
fn detect_signal_regular_roundtrip() {
    let raw = build_detect_signal_for_test(&DetectSignalCommand {
        market_name: "BTCUSDT".to_string(),
        strategy_id: 123,
        is_short: true,
        kind: 0,
        reserved: 0,
        msg: "pump".to_string(),
        pos_val: 0.0,
        val: 0.0,
        row_flags: 0,
        obj_uid: 0,
    });
    match StratCommand::parse(&raw).unwrap() {
        StratCommand::DetectSignal(cmd) => {
            assert_eq!(cmd.market_name, "BTCUSDT");
            assert_eq!(cmd.strategy_id, 123);
            assert!(cmd.is_short);
            assert!(cmd.is_regular_detect());
            assert_eq!(cmd.msg, "pump");
            assert!(!cmd.has_row());
            assert!(!cmd.has_alert());
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn detect_signal_conditional_row_and_alert_fields_roundtrip() {
    let raw = build_detect_signal_for_test(&DetectSignalCommand {
        market_name: "ETHUSDT".to_string(),
        strategy_id: 0,
        is_short: false,
        kind: DETECT_KIND_ROW | DETECT_KIND_ALERT,
        reserved: 0,
        msg: "alert".to_string(),
        pos_val: 12.5,
        val: 3.25,
        row_flags: 0b11,
        obj_uid: 777,
    });
    match StratCommand::parse(&raw).unwrap() {
        StratCommand::DetectSignal(cmd) => {
            assert_eq!(cmd.market_name, "ETHUSDT");
            assert_eq!(cmd.kind, DETECT_KIND_ROW | DETECT_KIND_ALERT);
            assert_eq!(cmd.msg, "alert");
            assert_eq!(cmd.pos_val, 12.5);
            assert_eq!(cmd.val, 3.25);
            assert!(cmd.row_is_open());
            assert!(cmd.row_is_taker());
            assert_eq!(cmd.obj_uid, 777);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn parse_runtime_state() {
    let mut payload = vec![CMD_RUNTIME_STATE, 0x03, 0x00];
    payload.extend_from_slice(&1u64.to_le_bytes());
    payload.push(1);

    match StratCommand::parse(&payload).unwrap() {
        StratCommand::RuntimeState(state) => assert!(state.strategies_running),
        _ => panic!("wrong variant"),
    }

    let mut payload = vec![CMD_RUNTIME_STATE, 0x03, 0x00];
    payload.extend_from_slice(&1u64.to_le_bytes());
    match StratCommand::parse(&payload).unwrap() {
        StratCommand::RuntimeState(state) => assert!(!state.strategies_running),
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
// parity: MoonBot MoonProtoStratStruct.pas:TStratSnapshot.CreateFromStream
fn snapshot_short_fixed_tail_zero_tails() {
    let mut payload = vec![CMD_SNAPSHOT, 0x03, 0x00];
    payload.extend_from_slice(&42u64.to_le_bytes());
    payload.extend_from_slice(&0x34u16.to_le_bytes());

    let cmd = StratCommand::parse(&payload).unwrap();
    match cmd {
        StratCommand::Snapshot(s) => {
            assert_eq!(s.server_epoch, 0x34);
            assert_eq!(s.client_max_last_date, 0);
            assert!(!s.full);
            assert!(s.data.is_empty());
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn delete_short_strategy_id_zero_tails_but_truncated_folder_string_rejects() {
    let mut short_id = vec![CMD_DELETE, 0x03, 0x00];
    short_id.extend_from_slice(&7u64.to_le_bytes());
    short_id.extend_from_slice(&0x1122u16.to_le_bytes());

    match StratCommand::parse(&short_id).unwrap() {
        StratCommand::Delete(d) => {
            assert_eq!(d.strategy_id, 0x1122);
            assert!(d.folder_path.is_empty());
        }
        _ => panic!("wrong variant"),
    }

    let mut bad_folder = build_delete(8, 555, "Folder");
    bad_folder.truncate(bad_folder.len() - 2);
    assert!(
            StratCommand::parse(&bad_folder).is_none(),
            "FolderPath uses ReadStringFromStreamUtf8/ReadBuffer and must reject a truncated declared string"
        );
}

#[test]
// parity: MoonBot MoonProtoStratStruct.pas:TStratSellPriceUpdate.CreateFromStream
fn sell_price_update_short_fixed_tail_zero_tails() {
    let mut payload = vec![CMD_SELL_PRICE_UPDATE, 0x03, 0x00];
    payload.extend_from_slice(&1u64.to_le_bytes());
    payload.extend_from_slice(&0x7788u16.to_le_bytes());

    let cmd = StratCommand::parse(&payload).unwrap();
    match cmd {
        StratCommand::SellPriceUpdate(u) => {
            assert_eq!(u.strategy_id, 0x7788);
            assert_eq!(u.sell_price, 0.0);
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
// parity: MoonBot MoonProtoStratStruct.pas:TStratCheckedSync.CreateFromStream
fn checked_items_read_declared_count_with_zero_tail() {
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
// parity: MoonBot MoonProtoStratStruct.pas:TStratCheckedSync.CreateFromStream
fn checked_word_count_builders_write_only_declared_wrapped_count() {
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
// parity: MoonBot MoonProtoStratStruct.pas:TStratSnapshot.CreateFromStream
fn parse_snapshot_declared_size_over_remaining_as_invalid() {
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
// parity: MoonBot MoonProtoStratStruct.pas:TStratSchema.CreateFromStream
fn parse_schema_size_over_remaining_becomes_empty_data() {
    let mut payload = vec![CMD_SCHEMA, 0x03, 0x00];
    payload.extend_from_slice(&1u64.to_le_bytes());
    payload.extend_from_slice(&4u32.to_le_bytes());
    payload.extend_from_slice(&[9, 8, 7]);
    match StratCommand::parse(&payload).unwrap() {
        StratCommand::Schema(s) => {
            assert!(
                s.data.is_empty(),
                "Delphi sets Data=nil when declared Size is larger than remaining bytes"
            );
        }
        _ => panic!("wrong variant"),
    }
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
            path: "P".into(),
            fields: fields.clone(),
        },
        StrategySnapshot {
            strategy_id: 2,
            strategy_ver: 1,
            last_date: 30,
            checked: false,
            kind: 1,
            path: "P".into(),
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
// parity: MoonBot MoonProtoBaseStruct.pas:TCommandRegistry.FromStream
fn version_gate_returns_skipped() {
    // ver=99 > CURRENT_PROTO_CMD_VER=3 → Delphi registry FSkipped.
    let mut payload = vec![CMD_SNAPSHOT, 99, 0];
    payload.extend_from_slice(&77u64.to_le_bytes());
    let cmd = StratCommand::parse(&payload).unwrap();
    match cmd {
        StratCommand::Skipped { cmd_id, uid, ver } => {
            assert_eq!(cmd_id, CMD_SNAPSHOT);
            assert_eq!(uid, 77);
            assert_eq!(ver, 99);
        }
        _ => panic!("wrong variant"),
    }
}
