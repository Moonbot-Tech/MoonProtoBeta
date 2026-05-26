
use super::*;
use crate::commands::strat::{
    StratCheckedEcho, StratCheckedSync, StratDelete, StratSellPriceUpdate,
};
use crate::commands::strategy_schema::{
    StrategyFieldLayout, StrategyFieldType, StrategyFieldUiKind, StrategySchemaField,
    StrategySchemaKind,
};
use crate::commands::strategy_serializer::{FieldValue, StrategyFields};
use std::hint::black_box;
use std::path::PathBuf;
use std::time::Instant;

fn schema_for_strategy_name(kinds: &[u8]) -> StrategySchema {
    StrategySchema {
        format_version: 1,
        kinds: kinds
            .iter()
            .map(|kind| StrategySchemaKind {
                ordinal: *kind,
                name: format!("Kind{kind}"),
            })
            .collect(),
        fields: vec![StrategySchemaField {
            name: "StrategyName".to_string(),
            raw_type_id: crate::commands::strategy_serializer::TID_STRING,
            type_id: StrategyFieldType::String,
            raw_flags: 0,
            ui_kind: StrategyFieldUiKind::Edit,
            layout: StrategyFieldLayout::None,
            default_value: None,
            visible_kind_ordinals: kinds.to_vec(),
            visible_kind_mask: crate::commands::strategy_schema::visible_kind_mask(kinds),
            static_picklist_raw: None,
            static_picklist: Vec::new(),
            dynamic_picklist: None,
        }],
    }
}

fn latest_firetest_strategy_raw_dump() -> PathBuf {
    if let Some(path) = std::env::var_os("MOONPROTO_STRAT_SNAPSHOT_BENCH") {
        return PathBuf::from(path);
    }

    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("firetest_strategy_raw");
    let mut files = std::fs::read_dir(&dir)
            .unwrap_or_else(|err| {
                panic!(
                    "cannot read {}; run FireTest quick/full first or set MOONPROTO_STRAT_SNAPSHOT_BENCH: {err}",
                    dir.display()
                )
            })
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().map(|ty| ty.is_file()).unwrap_or(false))
            .filter_map(|entry| {
                let modified = entry.metadata().ok()?.modified().ok()?;
                Some((modified, entry.path()))
            })
            .collect::<Vec<_>>();
    files.sort_by_key(|(modified, _)| *modified);
    files.pop().map(|(_, path)| path).unwrap_or_else(|| {
            panic!(
                "no FireTest strategy raw dumps in {}; run FireTest quick/full first or set MOONPROTO_STRAT_SNAPSHOT_BENCH",
                dir.display()
            )
        })
}

fn bench_iters() -> usize {
    std::env::var("MOONPROTO_STRAT_BENCH_ITERS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|iters| *iters > 0)
        .unwrap_or(200)
}

fn measure_us<F>(iters: usize, mut f: F) -> (u128, u128, usize)
where
    F: FnMut() -> usize,
{
    let mut total_ns = 0u128;
    let mut max_ns = 0u128;
    let mut checksum = 0usize;
    for _ in 0..iters {
        let start = Instant::now();
        checksum = checksum.wrapping_add(black_box(f()));
        let ns = start.elapsed().as_nanos();
        total_ns += ns;
        max_ns = max_ns.max(ns);
    }
    (total_ns / iters as u128 / 1_000, max_ns / 1_000, checksum)
}

#[test]
#[ignore = "diagnostic CPU benchmark; run after FireTest writes target/firetest_strategy_raw/*.bin"]
fn bench_firetest_strategy_snapshot_payload() {
    let path = latest_firetest_strategy_raw_dump();
    let raw =
        std::fs::read(&path).unwrap_or_else(|err| panic!("cannot read {}: {err}", path.display()));
    let batch =
        crate::commands::strategy_serializer::parse_strategy_batch(&raw).unwrap_or_else(|| {
            panic!(
                "strategy snapshot payload is not parseable: {}",
                path.display()
            )
        });
    let strategy_count = batch.strategies.len();
    let iters = bench_iters();

    let (parse_avg_us, parse_max_us, parse_sum) = measure_us(iters, || {
        crate::commands::strategy_serializer::parse_strategy_batch(black_box(&raw))
            .map(|batch| batch.strategies.len())
            .unwrap_or(0)
    });

    let (apply_cold_avg_us, apply_cold_max_us, apply_cold_sum) = measure_us(iters, || {
        let mut state = StratsState::new();
        state
            .apply_snapshot_decoded_with_mode_in_place(black_box(&raw), false)
            .unwrap_or(0)
    });

    let mut warm_state = StratsState::new();
    let _ = warm_state
        .apply_snapshot_decoded_with_mode_in_place(&raw, false)
        .expect("warm-up strategy apply failed");
    let (apply_warm_avg_us, apply_warm_max_us, apply_warm_sum) = measure_us(iters, || {
        warm_state
            .apply_snapshot_decoded_with_mode_in_place(black_box(&raw), false)
            .unwrap_or(0)
    });

    println!(
            "STRAT_BENCH payload={} bytes={} strategies={} iters={} parse_avg/max={}us/{}us apply_cold_avg/max={}us/{}us apply_warm_avg/max={}us/{}us checksum={}",
            path.display(),
            raw.len(),
            strategy_count,
            iters,
            parse_avg_us,
            parse_max_us,
            apply_cold_avg_us,
            apply_cold_max_us,
            apply_warm_avg_us,
            apply_warm_max_us,
            parse_sum ^ apply_cold_sum ^ apply_warm_sum,
        );
}

#[test]
fn sell_price_update_is_ignored_like_delphi_client() {
    let mut s = StratsState::new();
    s.upsert(100, 0, "".into());
    let ev = s.apply(StratCommand::SellPriceUpdate(StratSellPriceUpdate {
        strategy_id: 100,
        sell_price: 50.5,
    }));
    assert!(matches!(ev, StratEvent::Ignored));
    assert_eq!(s.get(100).unwrap().sell_price, 0.0);
}

#[test]
fn incoming_schema_request_is_ignored_like_delphi_client() {
    let mut s = StratsState::new();
    let ev = s.apply(StratCommand::SchemaRequest { uid: 77 });
    assert!(matches!(ev, StratEvent::Ignored));
}

#[test]
fn snapshot_sets_visible_sell_price_when_field_is_present() {
    let mut s = StratsState::new();
    let mut fields = StrategyFields::new();
    fields.insert("SellPrice", FieldValue::Double(50.5));
    s.upsert_from_snapshot(&StrategySnapshot {
        strategy_id: 100,
        strategy_ver: 1,
        last_date: 1,
        checked: false,
        kind: 1,
        path: "F".into(),
        fields,
    });
    assert_eq!(s.get(100).unwrap().sell_price, 50.5);
}

fn snapshot_for_listing_checks(
    id: u64,
    kind: StrategyKind,
    checked: bool,
    fields: &[(&str, FieldValue)],
) -> StrategySnapshot {
    StrategySnapshot {
        strategy_id: id,
        strategy_ver: 1,
        last_date: id,
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
fn listing_strategy_helpers_match_delphi_active_predicates() {
    let mut s = StratsState::new();
    s.upsert_local_snapshot(snapshot_for_listing_checks(
        1,
        StrategyKind::NEW_LISTING,
        true,
        &[("SellFromAsset", FieldValue::Bool(true))],
    ));

    assert!(s.is_there_listing_strat_like_delphi(StrategyActiveMode::ActiveClient));
    assert!(s.is_there_listing_sell_like_delphi(StrategyActiveMode::ActiveClient, false));
    assert!(
            !s.is_there_listing_strat_like_delphi(StrategyActiveMode::UsingMoonProto),
            "plain listing strategy is local-active in ActiveClient mode, remote-active in UsingMoonProto mode"
        );
}

#[test]
fn listing_sell_helper_uses_short_moonshot_only_for_spot_like_delphi() {
    let mut s = StratsState::new();
    s.upsert_local_snapshot(snapshot_for_listing_checks(
        1,
        StrategyKind::MOON_SHOT,
        true,
        &[("Short", FieldValue::Bool(true))],
    ));

    assert!(s.is_there_listing_sell_like_delphi(StrategyActiveMode::UsingMoonProto, false));
    assert!(
        !s.is_there_listing_sell_like_delphi(StrategyActiveMode::UsingMoonProto, true),
        "Delphi skips the MoonShot/MoonHook Short fallback when cfg.IsFutures"
    );
}

#[test]
fn delete_removes_entry() {
    let mut s = StratsState::new();
    let mut fields = StrategyFields::new();
    fields.insert("StrategyName", FieldValue::String("A".to_string()));
    s.upsert_from_snapshot(&StrategySnapshot {
        strategy_id: 100,
        strategy_ver: 1,
        last_date: 1,
        checked: true,
        kind: 1,
        path: "F".into(),
        fields,
    });
    assert!(s.has_folder("F"));
    let ev = s.apply(StratCommand::Delete(StratDelete {
        strategy_id: 100,
        folder_path: "".into(),
    }));
    assert!(matches!(
        ev,
        StratEvent::Deleted {
            strategy_id: 100,
            strategy_deleted: true,
            folder_deleted: false,
            ..
        }
    ));
    assert!(s.get(100).is_none());
    assert!(s.snapshot(100).is_none());
    assert!(s.has_folder("F"));
}

#[test]
fn delete_with_folder_path_deletes_strategy_then_empty_folder_like_delphi() {
    let mut s = StratsState::new();
    s.upsert_from_snapshot(&StrategySnapshot {
        strategy_id: 100,
        strategy_ver: 1,
        last_date: 1,
        checked: true,
        kind: 1,
        path: "Root/Sub".into(),
        fields: StrategyFields::new(),
    });

    let ev = s.apply(StratCommand::Delete(StratDelete {
        strategy_id: 100,
        folder_path: "Root/Sub".into(),
    }));

    assert!(matches!(
        ev,
        StratEvent::Deleted {
            strategy_id: 100,
            ref folder_path,
            strategy_deleted: true,
            folder_deleted: true,
        } if folder_path == "Root/Sub"
    ));
    assert!(s.get(100).is_none());
    assert!(!s.has_folder("Root/Sub"));
    assert!(s.has_folder("Root"));
}

#[test]
fn delete_zero_strategy_id_can_delete_empty_folder_like_delphi() {
    let mut s = StratsState::new();
    s.upsert_from_snapshot(&StrategySnapshot {
        strategy_id: 100,
        strategy_ver: 1,
        last_date: 1,
        checked: false,
        kind: 1,
        path: "Root/Sub".into(),
        fields: StrategyFields::new(),
    });
    s.apply(StratCommand::Delete(StratDelete {
        strategy_id: 100,
        folder_path: "".into(),
    }));
    assert!(s.has_folder("Root/Sub"));

    let ev = s.apply(StratCommand::Delete(StratDelete {
        strategy_id: 0,
        folder_path: "root/sub".into(),
    }));

    assert!(matches!(
        ev,
        StratEvent::Deleted {
            strategy_id: 0,
            ref folder_path,
            strategy_deleted: false,
            folder_deleted: true,
        } if folder_path == "root/sub"
    ));
    assert!(!s.has_folder("Root/Sub"));
}

#[test]
fn delete_folder_path_keeps_non_empty_folder_like_delphi() {
    let mut s = StratsState::new();
    s.upsert_from_snapshot(&StrategySnapshot {
        strategy_id: 100,
        strategy_ver: 1,
        last_date: 1,
        checked: false,
        kind: 1,
        path: "Root/Sub".into(),
        fields: StrategyFields::new(),
    });
    s.upsert_from_snapshot(&StrategySnapshot {
        strategy_id: 200,
        strategy_ver: 1,
        last_date: 1,
        checked: false,
        kind: 1,
        path: "Root/Sub/Child".into(),
        fields: StrategyFields::new(),
    });

    let ev = s.apply(StratCommand::Delete(StratDelete {
        strategy_id: 100,
        folder_path: "Root/Sub".into(),
    }));

    assert!(matches!(
        ev,
        StratEvent::Deleted {
            strategy_id: 100,
            strategy_deleted: true,
            folder_deleted: false,
            ..
        }
    ));
    assert!(s.has_folder("Root/Sub"));
    assert!(s.has_folder("Root/Sub/Child"));
}

#[test]
fn delete_unknown_strategy_without_folder_change_is_ignored_like_delphi() {
    let mut s = StratsState::new();
    let ev = s.apply(StratCommand::Delete(StratDelete {
        strategy_id: 404,
        folder_path: "".into(),
    }));
    assert!(matches!(ev, StratEvent::Ignored));
}

#[test]
fn checked_sync_delta() {
    let mut s = StratsState::new();
    s.upsert(1, 0, "".into());
    s.upsert(2, 0, "".into());
    // Дельта: только id=1 → checked.
    let cmd = StratCommand::CheckedSync(StratCheckedSync {
        items: vec![StratCheckedItem {
            strategy_id: 1,
            checked: true,
        }],
        is_delta: true,
    });
    let ev = s.apply(cmd);
    assert!(matches!(
        ev,
        StratEvent::CheckedSynced {
            changed: 1,
            is_delta: true
        }
    ));
    assert!(s.get(1).unwrap().checked);
    assert!(s.get(1).unwrap().prev_checked);
    // id=2 не трогался.
    assert!(!s.get(2).unwrap().checked);
    assert!(!s.get(2).unwrap().prev_checked);
}

#[test]
fn checked_sync_accepts_more_than_former_rust_cap() {
    let mut s = StratsState::new();
    for strategy_id in 1..=50_001u64 {
        s.upsert_checked_items(&[StratCheckedItem {
            strategy_id,
            checked: true,
        }]);
    }

    assert_eq!(s.len(), 50_001);
    assert!(s.get(50_001).unwrap().checked);
}

#[test]
fn apply_snapshot_decoded_upserts_strategies() {
    use crate::commands::strategy_serializer::{FieldValue, StrategyBatchBuilder};

    let schema = schema_for_strategy_name(&[5, 6]);
    let mut b = StrategyBatchBuilder::new(&schema);
    let mut fields1 = StrategyFields::new();
    fields1.insert("StrategyName", FieldValue::String("Strat-A".to_string()));
    b.write_strategy(&StrategySnapshot {
        strategy_id: 100,
        strategy_ver: 1,
        last_date: 1737000000000,
        checked: true,
        kind: 5,
        path: "F/A".to_string(),
        fields: fields1,
    });
    let mut fields2 = StrategyFields::new();
    fields2.insert("StrategyName", FieldValue::String("Strat-B".to_string()));
    b.write_strategy(&StrategySnapshot {
        strategy_id: 200,
        strategy_ver: 2,
        last_date: 1737000000001,
        checked: false,
        kind: 6,
        path: "F/B".to_string(),
        fields: fields2,
    });

    let payload = b.finalize();

    let mut s = StratsState::new();
    let batch = s.apply_snapshot_decoded(&payload).unwrap();
    assert_eq!(batch.strategies.len(), 2);

    let info100 = s.get(100).unwrap();
    assert_eq!(info100.last_date, 1737000000000);
    assert_eq!(info100.folder_path, "F/A");
    assert!(info100.checked);
    assert!(info100.prev_checked);
    assert_eq!(
        s.snapshot(100)
            .and_then(|snap| snap.fields.get("StrategyName")),
        Some(&FieldValue::String("Strat-A".to_string()))
    );

    let info200 = s.get(200).unwrap();
    assert_eq!(info200.folder_path, "F/B");
    assert!(!info200.checked);
    assert!(!info200.prev_checked);

    // Поля стратегий доступны через возвращённый batch
    assert_eq!(
        batch.strategies[0].fields.get("StrategyName"),
        Some(&FieldValue::String("Strat-A".to_string()))
    );
    let cache = s
        .snapshot_payload_cache
        .as_ref()
        .expect("complete incoming snapshot seeds serialized reply cache");
    assert_eq!(cache.client_max_last_date, 1737000000001);
    assert_eq!(cache.data, payload);
}

#[test]
fn in_place_complete_snapshot_seeds_serialized_reply_cache() {
    use crate::commands::strategy_serializer::{FieldValue, StrategyBatchBuilder};

    let schema = schema_for_strategy_name(&[5]);
    let mut fields = StrategyFields::new();
    fields.insert("StrategyName", FieldValue::String("Cached".to_string()));
    let mut b = StrategyBatchBuilder::new(&schema);
    b.write_strategy(&StrategySnapshot {
        strategy_id: 777,
        strategy_ver: 1,
        last_date: 1737000000042,
        checked: true,
        kind: 5,
        path: "Cache".to_string(),
        fields,
    });
    let payload = b.finalize();

    let mut s = StratsState::new();
    let count = s
        .apply_snapshot_decoded_with_mode_in_place(&payload, false)
        .unwrap();

    assert_eq!(count, 1);
    let cache = s
        .snapshot_payload_cache
        .as_ref()
        .expect("active complete snapshot seeds serialized reply cache");
    assert_eq!(cache.client_max_last_date, 1737000000042);
    assert_eq!(cache.data, payload);

    let ev = s.apply(StratCommand::CheckedSync(StratCheckedSync {
        items: vec![StratCheckedItem {
            strategy_id: 777,
            checked: true,
        }],
        is_delta: false,
    }));
    assert!(matches!(
        ev,
        StratEvent::CheckedSynced {
            changed: 0,
            is_delta: false
        }
    ));
    assert!(
        s.snapshot_payload_cache.is_some(),
        "no-op checked sync must not discard serialized reply cache"
    );

    let ev = s.apply(StratCommand::CheckedSync(StratCheckedSync {
        items: vec![StratCheckedItem {
            strategy_id: 777,
            checked: false,
        }],
        is_delta: true,
    }));
    assert!(matches!(
        ev,
        StratEvent::CheckedSynced {
            changed: 1,
            is_delta: true
        }
    ));
    assert!(
        s.snapshot_payload_cache.is_none(),
        "real checked change mutates serialized snapshot payload"
    );
}

#[test]
fn apply_snapshot_decoded_corrupted_returns_none() {
    let mut s = StratsState::new();
    // Невалидный DEFLATE
    let result = s.apply_snapshot_decoded(&[0xFF, 0xFF, 0xFF, 0xFF]);
    assert!(result.is_none());
    assert!(s.is_empty());
}

#[test]
fn full_snapshot_preserves_missing_strategies_like_delphi() {
    use crate::commands::strategy_serializer::{FieldValue, StrategyBatchBuilder};

    let mut old_fields = StrategyFields::new();
    old_fields.insert("StrategyName", FieldValue::String("Old".to_string()));
    let mut s = StratsState::new();
    s.upsert_from_snapshot(&StrategySnapshot {
        strategy_id: 1,
        strategy_ver: 1,
        last_date: 1,
        checked: true,
        kind: 1,
        path: "OldPath".to_string(),
        fields: old_fields,
    });

    let mut new_fields = StrategyFields::new();
    new_fields.insert("StrategyName", FieldValue::String("New".to_string()));
    let schema = schema_for_strategy_name(&[1]);
    let mut builder = StrategyBatchBuilder::new(&schema);
    builder.write_strategy(&StrategySnapshot {
        strategy_id: 2,
        strategy_ver: 1,
        last_date: 2,
        checked: false,
        kind: 1,
        path: "NewPath".to_string(),
        fields: new_fields,
    });

    let payload = builder.finalize();
    s.apply_snapshot_decoded_with_mode(&payload, true).unwrap();

    assert!(s.get(1).is_some());
    assert!(s.snapshot(1).is_some());
    assert_eq!(
        s.snapshot(1)
            .and_then(|snap| snap.fields.get("StrategyName")),
        Some(&FieldValue::String("Old".to_string()))
    );
    assert!(s.get(2).is_some());
    assert!(s.snapshot(2).is_some());
    assert!(
        s.snapshot_payload_cache.is_none(),
        "a subset payload must not be reused as full local snapshot reply"
    );
}

#[test]
fn checked_sync_full_only_updates_items_like_delphi() {
    let mut s = StratsState::new();
    // Изначально id=1 и id=2 checked.
    s.upsert(1, 0, "".into());
    s.upsert(2, 0, "".into());
    s.by_id.get_mut(&1).unwrap().checked = true;
    s.by_id.get_mut(&1).unwrap().prev_checked = true;
    s.by_id.get_mut(&2).unwrap().checked = true;
    s.by_id.get_mut(&2).unwrap().prev_checked = true;
    // Delphi receive path does not clear omitted strategies. Full packets
    // are full because their constructor includes every strategy.
    let cmd = StratCommand::CheckedSync(StratCheckedSync {
        items: vec![StratCheckedItem {
            strategy_id: 1,
            checked: false,
        }],
        is_delta: false,
    });
    let ev = s.apply(cmd);
    assert!(matches!(
        ev,
        StratEvent::CheckedSynced {
            changed: 1,
            is_delta: false
        }
    ));
    assert!(!s.get(1).unwrap().checked);
    assert!(!s.get(1).unwrap().prev_checked);
    assert!(s.get(2).unwrap().checked);
    assert!(s.get(2).unwrap().prev_checked);
}

#[test]
fn checked_sync_ignores_unknown_strategy() {
    let mut s = StratsState::new();
    s.upsert(1, 0, "".into());
    let cmd = StratCommand::CheckedSync(StratCheckedSync {
        items: vec![
            StratCheckedItem {
                strategy_id: 1,
                checked: true,
            },
            StratCheckedItem {
                strategy_id: 999,
                checked: true,
            },
        ],
        is_delta: true,
    });
    let ev = s.apply(cmd);

    assert!(matches!(
        ev,
        StratEvent::CheckedSynced {
            changed: 1,
            is_delta: true
        }
    ));
    assert!(s.get(1).unwrap().checked);
    assert!(s.get(999).is_none());
}

#[test]
fn snapshot_does_not_roll_back_newer_existing_strategy() {
    use crate::commands::strategy_serializer::{FieldValue, StrategySnapshot};

    let mut s = StratsState::new();
    let mut fields = StrategyFields::new();
    fields.insert("StrategyName", FieldValue::String("Old".to_string()));
    s.upsert_from_snapshot(&StrategySnapshot {
        strategy_id: 100,
        strategy_ver: 7,
        last_date: 200,
        checked: true,
        kind: 1,
        path: "NewPath".to_string(),
        fields: fields.clone(),
    });

    let changed = s.upsert_from_snapshot(&StrategySnapshot {
        strategy_id: 100,
        strategy_ver: 6,
        last_date: 199,
        checked: false,
        kind: 1,
        path: "OldPath".to_string(),
        fields,
    });

    assert!(!changed);
    let info = s.get(100).unwrap();
    assert_eq!(info.strategy_ver, 7);
    assert_eq!(info.last_date, 200);
    assert_eq!(info.folder_path, "NewPath");
    assert!(info.checked);
    assert!(info.prev_checked);
}

#[test]
fn local_checked_delta_waits_for_matching_echo() {
    use crate::commands::strategy_serializer::{FieldValue, StrategySnapshot};

    let mut fields = StrategyFields::new();
    fields.insert("StrategyName", FieldValue::String("A".to_string()));
    let mut s = StratsState::new();
    s.upsert_from_snapshot(&StrategySnapshot {
        strategy_id: 100,
        strategy_ver: 1,
        last_date: 1,
        checked: true,
        kind: 1,
        path: "P".to_string(),
        fields,
    });
    assert!(s.checked_delta().is_empty());

    assert!(s.set_checked(100, false));
    assert_eq!(
        s.checked_delta(),
        vec![StratCheckedItem {
            strategy_id: 100,
            checked: false
        }]
    );

    let stale_echo = StratCommand::CheckedEcho(StratCheckedEcho {
        items: vec![StratCheckedItem {
            strategy_id: 100,
            checked: true,
        }],
    });
    assert!(matches!(
        s.apply(stale_echo),
        StratEvent::CheckedEcho { count: 1 }
    ));
    assert_eq!(
        s.checked_delta(),
        vec![StratCheckedItem {
            strategy_id: 100,
            checked: false
        }]
    );

    let matching_echo = StratCommand::CheckedEcho(StratCheckedEcho {
        items: vec![StratCheckedItem {
            strategy_id: 100,
            checked: false,
        }],
    });
    s.apply(matching_echo);
    assert!(s.checked_delta().is_empty());
    assert!(!s.get(100).unwrap().prev_checked);
}

#[test]
fn snapshot_vec_preserves_delphi_list_order() {
    use crate::commands::strategy_serializer::StrategySnapshot;

    let mut s = StratsState::new();
    for strategy_id in [30, 10, 20] {
        s.upsert_local_snapshot(StrategySnapshot {
            strategy_id,
            strategy_ver: 1,
            last_date: strategy_id,
            checked: false,
            kind: 1,
            path: String::new(),
            fields: StrategyFields::new(),
        });
    }

    let ids: Vec<u64> = s
        .snapshot_vec()
        .into_iter()
        .map(|snapshot| snapshot.strategy_id)
        .collect();
    assert_eq!(ids, vec![30, 10, 20]);
}

#[test]
fn clone_shares_full_strategy_snapshots_until_mutation() {
    use crate::commands::strategy_serializer::{FieldValue, StrategySnapshot};

    let mut s = StratsState::new();
    let mut fields = StrategyFields::new();
    fields.insert(
        "Comment",
        FieldValue::String("heavy snapshot stays shared".to_string()),
    );
    s.upsert_local_snapshot(StrategySnapshot {
        strategy_id: 30,
        strategy_ver: 1,
        last_date: 30,
        checked: false,
        kind: 1,
        path: String::new(),
        fields,
    });

    let mut cloned = s.clone();
    assert!(Arc::ptr_eq(
        s.snapshots_by_id.get(&30).unwrap(),
        cloned.snapshots_by_id.get(&30).unwrap()
    ));

    assert!(cloned.set_checked(30, true));
    assert!(!Arc::ptr_eq(
        s.snapshots_by_id.get(&30).unwrap(),
        cloned.snapshots_by_id.get(&30).unwrap()
    ));
    assert!(!s.snapshot(30).unwrap().checked);
    assert!(cloned.snapshot(30).unwrap().checked);
}

#[test]
fn snapshot_applies_new_zero_version_strategy() {
    use crate::commands::strategy_serializer::{FieldValue, StrategySnapshot};

    let mut s = StratsState::new();
    let mut fields = StrategyFields::new();
    fields.insert("StrategyName", FieldValue::String("Zero".to_string()));

    let changed = s.upsert_from_snapshot(&StrategySnapshot {
        strategy_id: 100,
        strategy_ver: 0,
        last_date: 0,
        checked: true,
        kind: 1,
        path: "ZeroPath".to_string(),
        fields,
    });

    assert!(changed);
    let info = s.get(100).unwrap();
    assert_eq!(info.strategy_ver, 0);
    assert_eq!(info.last_date, 0);
    assert_eq!(info.folder_path, "ZeroPath");
    assert!(info.checked);
}
