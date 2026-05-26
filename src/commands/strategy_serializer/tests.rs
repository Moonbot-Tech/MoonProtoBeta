
use super::*;
use crate::commands::strategy_schema::{
    visible_kind_mask, StrategyFieldLayout, StrategyFieldType, StrategyFieldUiKind,
    StrategySchemaKind,
};

fn schema_field(
    name: &str,
    type_id: u8,
    default_value: Option<FieldValue>,
    visible_kind_ordinals: &[u8],
) -> StrategySchemaField {
    StrategySchemaField {
        name: name.to_string(),
        raw_type_id: type_id,
        type_id: StrategyFieldType::from_type_id(type_id),
        raw_flags: 0,
        ui_kind: StrategyFieldUiKind::Edit,
        layout: StrategyFieldLayout::None,
        default_value,
        visible_kind_ordinals: visible_kind_ordinals.to_vec(),
        visible_kind_mask: visible_kind_mask(visible_kind_ordinals),
        static_picklist_raw: None,
        static_picklist: Vec::new(),
        dynamic_picklist: None,
    }
}

fn schema_for_fields(fields: Vec<StrategySchemaField>) -> StrategySchema {
    StrategySchema {
        format_version: 1,
        kinds: vec![
            StrategySchemaKind {
                ordinal: 1,
                name: "Kind1".to_string(),
            },
            StrategySchemaKind {
                ordinal: 5,
                name: "Kind5".to_string(),
            },
        ],
        fields,
    }
}

fn sample_schema() -> StrategySchema {
    schema_for_fields(vec![
        schema_field("StrategyName", TID_STRING, None, &[1, 5]),
        schema_field("Comment", TID_STRING, None, &[1, 5]),
        schema_field("AcceptCommands", TID_BOOL, None, &[1, 5]),
        schema_field("KeepAlert", TID_INT32, Some(FieldValue::Int32(60)), &[1, 5]),
        schema_field("OrderSize", TID_DOUBLE, None, &[1, 5]),
        schema_field(
            "UseStopLoss",
            TID_BOOL,
            Some(FieldValue::Bool(true)),
            &[1, 5],
        ),
        schema_field(
            "StopLoss",
            TID_DOUBLE,
            Some(FieldValue::Double(-5.0)),
            &[1, 5],
        ),
        schema_field(
            "PendingOrderSpread",
            TID_DOUBLE,
            Some(FieldValue::Double(0.1)),
            &[1, 5],
        ),
        schema_field("DebugLog", TID_BOOL, None, &[1, 5]),
        schema_field(
            "SellOrderColor",
            TID_STRING,
            Some(FieldValue::String("00FF00".to_string())),
            &[1, 5],
        ),
        schema_field(
            "SignalType",
            TID_STRING,
            Some(FieldValue::String("DropsDetection".to_string())),
            &[1, 5],
        ),
    ])
}

fn sample_strategy(id: u64, name: &str, path: &str) -> StrategySnapshot {
    let mut fields = StrategyFields::new();
    fields.insert("StrategyName", FieldValue::String(name.to_string()));
    fields.insert("OrderSize", FieldValue::Double(123.45));
    fields.insert("KeepAlert", FieldValue::Int32(61));
    fields.insert("AcceptCommands", FieldValue::Bool(true));
    fields.insert("Comment", FieldValue::String("Test strategy".to_string()));
    StrategySnapshot {
        strategy_id: id,
        strategy_ver: 1,
        last_date: 1737000000000, // 2026-01-16 UTC ms
        checked: true,
        kind: 5,
        path: path.to_string(),
        fields,
    }
}

fn strategy_with_fields(
    kind: StrategyKind,
    checked: bool,
    fields: &[(&str, FieldValue)],
) -> StrategySnapshot {
    StrategySnapshot {
        strategy_id: 1,
        strategy_ver: 1,
        last_date: 1,
        checked,
        kind: kind.0,
        path: String::new(),
        fields: fields
            .iter()
            .map(|(name, value)| (Arc::<str>::from(*name), value.clone()))
            .collect(),
    }
}

#[test]
fn strategy_active_helpers_match_delphi_check_active_modes() {
    let listing = strategy_with_fields(StrategyKind::NEW_LISTING, true, &[]);
    assert!(listing.active_like_delphi(StrategyActiveMode::ActiveClient));
    assert!(!listing.active_like_delphi(StrategyActiveMode::UsingMoonProto));
    assert!(listing.active_like_delphi(StrategyActiveMode::Standalone));

    let moonshot = strategy_with_fields(StrategyKind::MOON_SHOT, true, &[]);
    assert!(
        moonshot.can_auto_buy_like_delphi(),
        "Delphi CanAutoBuy is true for MoonShot even when AutoBuy=false"
    );
    assert!(!moonshot.active_like_delphi(StrategyActiveMode::ActiveClient));
    assert!(moonshot.active_like_delphi(StrategyActiveMode::UsingMoonProto));

    let remote_kernel = strategy_with_fields(
        StrategyKind::NEW_LISTING,
        true,
        &[("RunDetectOnKernel", FieldValue::Bool(true))],
    );
    assert!(!remote_kernel.active_like_delphi(StrategyActiveMode::ActiveClient));
    assert!(remote_kernel.active_like_delphi(StrategyActiveMode::UsingMoonProto));
}

#[test]
fn empty_batch_roundtrip() {
    let compressed = StrategyBatchBuilder::empty_payload();
    let parsed = parse_strategy_batch(&compressed).unwrap();
    assert!(parsed.names.is_empty());
    assert!(parsed.paths.is_empty());
    assert!(parsed.strategies.is_empty());
}

#[test]
fn single_strategy_roundtrip() {
    let schema = sample_schema();
    let mut b = StrategyBatchBuilder::new(&schema);
    let s = sample_strategy(100, "Strat-1", "Folder/A");
    b.write_strategy(&s);
    let compressed = b.finalize();

    let parsed = parse_strategy_batch(&compressed).unwrap();
    assert_eq!(parsed.strategies.len(), 1);
    let ps = &parsed.strategies[0];
    assert_eq!(ps.strategy_id, 100);
    assert_eq!(ps.strategy_ver, 1);
    assert!(ps.checked);
    assert_eq!(ps.kind, 5);
    assert_eq!(ps.path, "Folder/A");
    assert_eq!(
        ps.fields.get("StrategyName"),
        Some(&FieldValue::String("Strat-1".to_string()))
    );
    assert_eq!(
        ps.fields.get("OrderSize"),
        Some(&FieldValue::Double(123.45))
    );
    assert_eq!(ps.fields.get("KeepAlert"), Some(&FieldValue::Int32(61)));
    assert_eq!(
        ps.fields.get("AcceptCommands"),
        Some(&FieldValue::Bool(true))
    );
}

#[test]
fn writer_uses_schema_field_order_for_name_dict() {
    let mut fields = StrategyFields::new();
    fields.insert("OrderSize", FieldValue::Double(1.0));
    fields.insert("StrategyName", FieldValue::String("A".to_string()));
    fields.insert("UnknownZ", FieldValue::Byte(1));
    fields.insert("AcceptCommands", FieldValue::Bool(true));
    fields.insert("UnknownA", FieldValue::Byte(2));
    fields.insert("Comment", FieldValue::String("C".to_string()));

    let schema = sample_schema();
    let mut b = StrategyBatchBuilder::new(&schema);
    b.write_strategy(&StrategySnapshot {
        strategy_id: 1,
        strategy_ver: 1,
        last_date: 0,
        checked: true,
        kind: 1,
        path: String::new(),
        fields,
    });

    let parsed = parse_strategy_batch(&b.finalize()).unwrap();
    assert_eq!(
        parsed.names,
        vec![
            "StrategyName".to_string(),
            "Comment".to_string(),
            "AcceptCommands".to_string(),
            "OrderSize".to_string(),
        ]
    );
}

#[test]
fn writer_skips_schema_defaults_unknown_fields_and_type_mismatches() {
    let mut fields = StrategyFields::new();
    fields.insert("StrategyName", FieldValue::String("Local".to_string()));
    fields.insert("KeepAlert", FieldValue::Int32(60));
    fields.insert("UseStopLoss", FieldValue::Bool(true));
    fields.insert("StopLoss", FieldValue::Double(-5.0));
    fields.insert("PendingOrderSpread", FieldValue::Double(0.1));
    fields.insert("DebugLog", FieldValue::Bool(false));
    fields.insert("UnknownA", FieldValue::Byte(7));
    fields.insert("OrderSize", FieldValue::String("wrong type".to_string()));
    fields.insert("SellOrderColor", FieldValue::String(String::new()));

    let schema = sample_schema();
    let mut b = StrategyBatchBuilder::new(&schema);
    b.write_strategy(&StrategySnapshot {
        strategy_id: 1,
        strategy_ver: 1,
        last_date: 0,
        checked: true,
        kind: 1,
        path: String::new(),
        fields,
    });

    let parsed = parse_strategy_batch(&b.finalize()).unwrap();
    assert_eq!(
        parsed.names,
        vec!["StrategyName".to_string(), "SellOrderColor".to_string()]
    );
    let ps = &parsed.strategies[0];
    assert_eq!(
        ps.fields.get("StrategyName"),
        Some(&FieldValue::String("Local".to_string()))
    );
    assert_eq!(
        ps.fields.get("SellOrderColor"),
        Some(&FieldValue::String(String::new()))
    );
    assert!(!ps.fields.contains_key("KeepAlert"));
    assert!(!ps.fields.contains_key("UseStopLoss"));
    assert!(!ps.fields.contains_key("StopLoss"));
    assert!(!ps.fields.contains_key("PendingOrderSpread"));
    assert!(!ps.fields.contains_key("DebugLog"));
    assert!(!ps.fields.contains_key("UnknownA"));
    assert!(!ps.fields.contains_key("OrderSize"));
}

#[test]
fn multiple_strategies_share_name_dict() {
    let schema = sample_schema();
    let mut b = StrategyBatchBuilder::new(&schema);
    b.write_strategy(&sample_strategy(1, "A", "Folder/X"));
    b.write_strategy(&sample_strategy(2, "B", "Folder/X")); // same path
    b.write_strategy(&sample_strategy(3, "C", "Folder/Y")); // new path
    let compressed = b.finalize();

    let parsed = parse_strategy_batch(&compressed).unwrap();
    assert_eq!(parsed.strategies.len(), 3);
    // Имена уникальны: StrategyName, OrderSize, KeepAlert, AcceptCommands, Comment — 5 имён.
    assert_eq!(parsed.names.len(), 5);
    // Пути уникальны: 2 штуки.
    assert_eq!(parsed.paths.len(), 2);
}

#[test]
fn zero_flag_encoded_for_zero_values() {
    let mut fields = StrategyFields::new();
    fields.insert("KeepAlert", FieldValue::Int32(0));
    fields.insert("UseStopLoss", FieldValue::Bool(false));
    fields.insert("SignalType", FieldValue::String(String::new()));
    fields.insert("DebugLog", FieldValue::Bool(false));

    let s = StrategySnapshot {
        strategy_id: 1,
        strategy_ver: 1,
        last_date: 0,
        checked: false,
        kind: 1,
        path: String::new(),
        fields,
    };

    let schema = sample_schema();
    let mut b = StrategyBatchBuilder::new(&schema);
    b.write_strategy(&s);
    let compressed = b.finalize();

    let parsed = parse_strategy_batch(&compressed).unwrap();
    let ps = &parsed.strategies[0];
    assert_eq!(ps.fields.get("KeepAlert"), Some(&FieldValue::Int32(0)));
    assert_eq!(ps.fields.get("UseStopLoss"), Some(&FieldValue::Bool(false)));
    assert_eq!(
        ps.fields.get("SignalType"),
        Some(&FieldValue::String(String::new()))
    );
    assert!(!ps.fields.contains_key("DebugLog"));
}

#[test]
fn all_primitive_types_roundtrip() {
    let values = [
        FieldValue::Bool(true),
        FieldValue::Byte(200),
        FieldValue::Word(60000),
        FieldValue::Int32(-12345),
        FieldValue::UInt32(3_000_000_000),
        FieldValue::Int64(-9_876_543_210),
        FieldValue::UInt64(12_345_678_901_234),
        FieldValue::Single(3.125),
        FieldValue::Double(2.75),
        FieldValue::String("Hello 世界 🚀".to_string()),
    ];

    for value in values {
        let mut bytes = Vec::new();
        write_field(&mut bytes, &value);
        let mut pos = 0usize;
        let type_id = read_u8(&bytes, &mut pos).unwrap();
        assert_eq!(type_id & 0x7F, value.type_id());
        let parsed = if (type_id & TID_ZERO_FLAG) != 0 {
            FieldValue::zero(type_id).unwrap()
        } else {
            try_read_field_value(&bytes, &mut pos, type_id).unwrap()
        };
        assert_eq!(parsed, value);
        assert_eq!(pos, bytes.len());
    }
}

#[test]
fn writer_wraps_name_path_and_string_lengths_like_delphi() {
    let long_name = "N".repeat(257);
    let long_path = "P".repeat(257);
    let long_value = "V".repeat(65_537);

    let mut name_bytes = Vec::new();
    write_u8_len_bytes(&mut name_bytes, long_name.as_bytes());
    assert_eq!(name_bytes, vec![1, b'N']);

    let mut fields = StrategyFields::new();
    fields.insert("Comment", FieldValue::String(long_value));

    let s = StrategySnapshot {
        strategy_id: 1000,
        strategy_ver: 1,
        last_date: 1737000000000,
        checked: true,
        kind: 1,
        path: long_path,
        fields,
    };

    let schema = sample_schema();
    let mut b = StrategyBatchBuilder::new(&schema);
    b.write_strategy(&s);
    let compressed = b.finalize();
    let parsed = parse_strategy_batch(&compressed).unwrap();
    let ps = &parsed.strategies[0];

    assert_eq!(ps.path, "P");
    assert_eq!(
        ps.fields.get("Comment"),
        Some(&FieldValue::String("V".to_string()))
    );
}

#[test]
fn missing_path_id_yields_empty() {
    // Конструируем raw plain payload где PathID=99 при пустом PathDict.
    let mut plain = Vec::new();
    // NameDict: 1 name "X"
    plain.extend_from_slice(&1u16.to_le_bytes());
    plain.push(1);
    plain.push(b'X');
    // PathDict: empty
    plain.extend_from_slice(&0u16.to_le_bytes());
    // StratCount: 1
    plain.extend_from_slice(&1u16.to_le_bytes());
    // Strategy
    plain.extend_from_slice(&42u64.to_le_bytes()); // id
    plain.extend_from_slice(&1i32.to_le_bytes()); // ver
    plain.extend_from_slice(&0u64.to_le_bytes()); // last_date
    plain.push(0); // checked
    plain.push(0); // kind
    plain.extend_from_slice(&99u16.to_le_bytes()); // path_id (OOR)
    plain.extend_from_slice(&0u16.to_le_bytes()); // field count

    let parsed = parse_strategy_batch_plain(&plain).unwrap();
    assert_eq!(parsed.strategies.len(), 1);
    assert_eq!(parsed.strategies[0].path, ""); // PathID out of range → empty
}

#[test]
fn unknown_type_id_skipped_8_bytes() {
    // FieldIdx=0, TypeID=99 (неизвестный) → reader должен пропустить 8 байт.
    // После этого должен корректно прочитать следующее поле.
    let mut plain = Vec::new();
    // NameDict: 2 names
    plain.extend_from_slice(&2u16.to_le_bytes());
    plain.push(1);
    plain.push(b'A');
    plain.push(1);
    plain.push(b'B');
    // PathDict
    plain.extend_from_slice(&0u16.to_le_bytes());
    // StratCount
    plain.extend_from_slice(&1u16.to_le_bytes());
    // Strategy header
    plain.extend_from_slice(&1u64.to_le_bytes());
    plain.extend_from_slice(&1i32.to_le_bytes());
    plain.extend_from_slice(&0u64.to_le_bytes());
    plain.push(0);
    plain.push(0);
    plain.extend_from_slice(&0u16.to_le_bytes());
    // FieldCount=2
    plain.extend_from_slice(&2u16.to_le_bytes());
    // Field 0: idx=0, typeID=99 (unknown), 8 bytes value (всё нули)
    plain.extend_from_slice(&0u16.to_le_bytes());
    plain.push(99);
    plain.extend_from_slice(&[0u8; 8]);
    // Field 1: idx=1, typeID=TID_INT32, value=42
    plain.extend_from_slice(&1u16.to_le_bytes());
    plain.push(TID_INT32);
    plain.extend_from_slice(&42i32.to_le_bytes());

    let parsed = parse_strategy_batch_plain(&plain).unwrap();
    let ps = &parsed.strategies[0];
    // Field A не разобран (unknown TypeID).
    assert_eq!(ps.fields.get("A"), None);
    // Field B разобран как Int32=42.
    assert_eq!(ps.fields.get("B"), Some(&FieldValue::Int32(42)));
}

#[test]
fn known_field_type_mismatch_is_skipped_like_delphi_read_field() {
    let mut plain = Vec::new();
    // NameDict: OrderSize expects TID_DOUBLE, Comment expects TID_STRING.
    plain.extend_from_slice(&2u16.to_le_bytes());
    plain.push(9);
    plain.extend_from_slice(b"OrderSize");
    plain.push(7);
    plain.extend_from_slice(b"Comment");
    // PathDict
    plain.extend_from_slice(&0u16.to_le_bytes());
    // StratCount
    plain.extend_from_slice(&1u16.to_le_bytes());
    // Strategy header
    plain.extend_from_slice(&1u64.to_le_bytes());
    plain.extend_from_slice(&1i32.to_le_bytes());
    plain.extend_from_slice(&0u64.to_le_bytes());
    plain.push(0);
    plain.push(0);
    plain.extend_from_slice(&0u16.to_le_bytes());
    // FieldCount=2
    plain.extend_from_slice(&2u16.to_le_bytes());
    // Field 0: OrderSize but wire type is String; Delphi skips it.
    plain.extend_from_slice(&0u16.to_le_bytes());
    plain.push(TID_STRING);
    plain.extend_from_slice(&3u16.to_le_bytes());
    plain.extend_from_slice(b"bad");
    // Field 1: Comment, correct string, proves skip consumed exact bytes.
    plain.extend_from_slice(&1u16.to_le_bytes());
    plain.push(TID_STRING);
    plain.extend_from_slice(&2u16.to_le_bytes());
    plain.extend_from_slice(b"ok");

    let schema = schema_for_fields(vec![
        schema_field("OrderSize", TID_DOUBLE, None, &[0]),
        schema_field("Comment", TID_STRING, None, &[0]),
    ]);
    let parsed = parse_strategy_batch_plain_with_schema(&plain, Some(&schema)).unwrap();
    let ps = &parsed.strategies[0];
    assert!(!ps.fields.contains_key("OrderSize"));
    assert_eq!(
        ps.fields.get("Comment"),
        Some(&FieldValue::String("ok".to_string()))
    );
}

#[test]
fn string_field_value_zero_fills_short_body_like_delphi_read_field() {
    let mut plain = Vec::new();
    // NameDict: one string field.
    plain.extend_from_slice(&1u16.to_le_bytes());
    plain.push(7);
    plain.extend_from_slice(b"Comment");
    // PathDict
    plain.extend_from_slice(&0u16.to_le_bytes());
    // StratCount
    plain.extend_from_slice(&1u16.to_le_bytes());
    // Strategy header
    plain.extend_from_slice(&1u64.to_le_bytes());
    plain.extend_from_slice(&1i32.to_le_bytes());
    plain.extend_from_slice(&0u64.to_le_bytes());
    plain.push(0);
    plain.push(0);
    plain.extend_from_slice(&0u16.to_le_bytes());
    // FieldCount=1
    plain.extend_from_slice(&1u16.to_le_bytes());
    // Field 0: declared string Len=3, but only one body byte is present.
    plain.extend_from_slice(&0u16.to_le_bytes());
    plain.push(TID_STRING);
    plain.extend_from_slice(&3u16.to_le_bytes());
    plain.push(b'a');

    let parsed = parse_strategy_batch_plain(&plain).unwrap();
    let ps = &parsed.strategies[0];
    assert_eq!(
        ps.fields.get("Comment"),
        Some(&FieldValue::String("a\0\0".to_string()))
    );
}

#[test]
fn scalar_field_value_zero_fills_short_body_like_delphi_stream_read() {
    let mut plain = Vec::new();
    // NameDict: one Int32 field.
    plain.extend_from_slice(&1u16.to_le_bytes());
    plain.push(9);
    plain.extend_from_slice(b"KeepAlert");
    // PathDict
    plain.extend_from_slice(&0u16.to_le_bytes());
    // StratCount
    plain.extend_from_slice(&1u16.to_le_bytes());
    // Strategy header
    plain.extend_from_slice(&1u64.to_le_bytes());
    plain.extend_from_slice(&1i32.to_le_bytes());
    plain.extend_from_slice(&0u64.to_le_bytes());
    plain.push(0);
    plain.push(0);
    plain.extend_from_slice(&0u16.to_le_bytes());
    // FieldCount=1
    plain.extend_from_slice(&1u16.to_le_bytes());
    // Field 0: declared Int32, but only low two bytes are present.
    plain.extend_from_slice(&0u16.to_le_bytes());
    plain.push(TID_INT32);
    plain.extend_from_slice(&0x1234u16.to_le_bytes());

    let parsed = parse_strategy_batch_plain(&plain).unwrap();
    let ps = &parsed.strategies[0];
    assert_eq!(ps.fields.get("KeepAlert"), Some(&FieldValue::Int32(0x1234)));
}

#[test]
fn known_field_type_mismatch_fixed_skip_consumes_short_tail_like_delphi() {
    let mut plain = Vec::new();
    // NameDict: OrderSize expects TID_DOUBLE.
    plain.extend_from_slice(&1u16.to_le_bytes());
    plain.push(9);
    plain.extend_from_slice(b"OrderSize");
    // PathDict
    plain.extend_from_slice(&0u16.to_le_bytes());
    // StratCount
    plain.extend_from_slice(&1u16.to_le_bytes());
    // Strategy header
    plain.extend_from_slice(&1u64.to_le_bytes());
    plain.extend_from_slice(&1i32.to_le_bytes());
    plain.extend_from_slice(&0u64.to_le_bytes());
    plain.push(0);
    plain.push(0);
    plain.extend_from_slice(&0u16.to_le_bytes());
    // FieldCount=1
    plain.extend_from_slice(&1u16.to_le_bytes());
    // Field 0: OrderSize but wire type is Int64; only one byte of the
    // skipped fixed-size value is present.
    plain.extend_from_slice(&0u16.to_le_bytes());
    plain.push(TID_INT64);
    plain.push(0xAA);

    let schema = schema_for_fields(vec![schema_field("OrderSize", TID_DOUBLE, None, &[0])]);
    let parsed = parse_strategy_batch_plain_with_schema(&plain, Some(&schema)).unwrap();
    assert!(parsed.strategies[0].fields.is_empty());
}

#[test]
fn known_field_type_mismatch_string_skip_consumes_short_body_like_delphi() {
    let mut plain = Vec::new();
    // NameDict: OrderSize expects TID_DOUBLE.
    plain.extend_from_slice(&1u16.to_le_bytes());
    plain.push(9);
    plain.extend_from_slice(b"OrderSize");
    // PathDict
    plain.extend_from_slice(&0u16.to_le_bytes());
    // StratCount
    plain.extend_from_slice(&1u16.to_le_bytes());
    // Strategy header
    plain.extend_from_slice(&1u64.to_le_bytes());
    plain.extend_from_slice(&1i32.to_le_bytes());
    plain.extend_from_slice(&0u64.to_le_bytes());
    plain.push(0);
    plain.push(0);
    plain.extend_from_slice(&0u16.to_le_bytes());
    // FieldCount=1
    plain.extend_from_slice(&1u16.to_le_bytes());
    // Field 0: OrderSize but wire type is String; Len is present, body is short.
    plain.extend_from_slice(&0u16.to_le_bytes());
    plain.push(TID_STRING);
    plain.extend_from_slice(&5u16.to_le_bytes());
    plain.push(b'x');

    let schema = schema_for_fields(vec![schema_field("OrderSize", TID_DOUBLE, None, &[0])]);
    let parsed = parse_strategy_batch_plain_with_schema(&plain, Some(&schema)).unwrap();
    assert!(parsed.strategies[0].fields.is_empty());
}

#[test]
fn invalid_utf8_dicts_and_string_fields_use_delphi_question_mark_fallback() {
    let mut plain = Vec::new();
    // NameDict: one invalid UTF-8 field name "N?me".
    plain.extend_from_slice(&1u16.to_le_bytes());
    plain.push(4);
    plain.extend_from_slice(&[b'N', 0xFF, b'm', b'e']);
    // PathDict: one invalid UTF-8 path "P?".
    plain.extend_from_slice(&1u16.to_le_bytes());
    plain.push(2);
    plain.extend_from_slice(&[b'P', 0x80]);
    // StratCount
    plain.extend_from_slice(&1u16.to_le_bytes());
    // Strategy header
    plain.extend_from_slice(&1u64.to_le_bytes());
    plain.extend_from_slice(&1i32.to_le_bytes());
    plain.extend_from_slice(&0u64.to_le_bytes());
    plain.push(0);
    plain.push(0);
    plain.extend_from_slice(&0u16.to_le_bytes());
    // FieldCount=1, field value "V?"
    plain.extend_from_slice(&1u16.to_le_bytes());
    plain.extend_from_slice(&0u16.to_le_bytes());
    plain.push(TID_STRING);
    plain.extend_from_slice(&2u16.to_le_bytes());
    plain.extend_from_slice(&[b'V', 0xFF]);

    let parsed = parse_strategy_batch_plain(&plain).unwrap();
    assert_eq!(parsed.names, vec!["N?me".to_string()]);
    assert_eq!(parsed.paths, vec!["P?".to_string()]);
    let ps = &parsed.strategies[0];
    assert_eq!(ps.path, "P?");
    assert_eq!(
        ps.fields.get("N?me"),
        Some(&FieldValue::String("V?".to_string()))
    );
}

#[test]
fn truncated_payload_returns_none() {
    let mut plain = Vec::new();
    // Только частичный NameDict header (нет данных)
    plain.extend_from_slice(&100u16.to_le_bytes()); // обещано 100 имён
                                                    // Но больше нет данных → должен вернуть None
    let parsed = parse_strategy_batch_plain(&plain);
    assert!(parsed.is_none());
}

#[test]
fn field_value_type_id_match() {
    assert_eq!(FieldValue::Bool(true).type_id(), TID_BOOL);
    assert_eq!(FieldValue::Byte(0).type_id(), TID_BYTE);
    assert_eq!(FieldValue::Word(0).type_id(), TID_WORD);
    assert_eq!(FieldValue::Int32(0).type_id(), TID_INT32);
    assert_eq!(FieldValue::UInt32(0).type_id(), TID_UINT32);
    assert_eq!(FieldValue::Int64(0).type_id(), TID_INT64);
    assert_eq!(FieldValue::UInt64(0).type_id(), TID_UINT64);
    assert_eq!(FieldValue::Single(0.0).type_id(), TID_SINGLE);
    assert_eq!(FieldValue::Double(0.0).type_id(), TID_DOUBLE);
    assert_eq!(FieldValue::String(String::new()).type_id(), TID_STRING);
}

#[test]
fn field_value_zero_for_each_type() {
    assert_eq!(FieldValue::zero(TID_BOOL), Some(FieldValue::Bool(false)));
    assert_eq!(FieldValue::zero(TID_INT32), Some(FieldValue::Int32(0)));
    assert_eq!(
        FieldValue::zero(TID_STRING),
        Some(FieldValue::String(String::new()))
    );
    assert_eq!(FieldValue::zero(TID_DOUBLE), Some(FieldValue::Double(0.0)));
    assert_eq!(FieldValue::zero(99), None);
}

#[test]
fn is_zero_for_each_type() {
    assert!(FieldValue::Bool(false).is_zero());
    assert!(!FieldValue::Bool(true).is_zero());
    assert!(FieldValue::Int32(0).is_zero());
    assert!(!FieldValue::Int32(1).is_zero());
    assert!(FieldValue::String(String::new()).is_zero());
    assert!(!FieldValue::String("x".to_string()).is_zero());
    assert!(FieldValue::Double(0.0).is_zero());
    assert!(FieldValue::Double(1e-15).is_zero()); // < 1e-10
    assert!(!FieldValue::Double(1e-5).is_zero());
}
