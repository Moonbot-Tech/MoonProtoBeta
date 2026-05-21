//! Strats sync state — apply StratCommand'ы к локальной модели стратегий.
//!
//! Источник Delphi: `MoonProtoClient.pas:689-800 ProcessStratCommand`.
//!
//! ## Декодинг TStratSnapshot.Data
//!
//! Сервер шлёт сериализованную пачку стратегий в `TStratSnapshot.data: Vec<u8>` через
//! `TStrategySerializer` (RTTI-driven). `apply_snapshot_decoded()` парсит этот blob через
//! `commands::strategy_serializer::parse_strategy_batch` и применяет каждую стратегию в state
//! с Delphi rollback guard по `StrategyLastDate`/`StrategyVer`.
//! State хранит и lightweight `StrategyInfo`, и полный decoded `StrategySnapshot`.
//! Поэтому active library может сама отвечать на `TStratSnapshotRequest`, а
//! приложение может читать последний полный snapshot через public API.

use crate::commands::strat::{StratCheckedItem, StratCommand};
use crate::commands::strategy_serializer::{parse_strategy_batch, StrategyBatch, StrategySnapshot};
use std::collections::HashMap;

/// Информация по одной стратегии — то что хранится клиентом.
#[derive(Debug, Clone)]
pub struct StrategyInfo {
    /// Уникальный идентификатор стратегии (от сервера). 0 = не валидный.
    pub strategy_id: u64,
    /// Версия стратегии из `TStrategySerializer` header.
    pub strategy_ver: i32,
    /// Время последнего апдейта (TDateTime f64 packed как UInt64).
    pub last_date: u64,
    /// Цена продажи (из TStratSellPriceUpdate). 0.0 если не было апдейта.
    pub sell_price: f64,
    /// Checked-state (для UI start/stop).
    pub checked: bool,
    /// Folder path в дереве стратегий (из последнего TStratDelete / Snapshot).
    pub folder_path: String,
}

impl StrategyInfo {
    fn new(strategy_id: u64) -> Self {
        Self {
            strategy_id,
            strategy_ver: 0,
            last_date: 0,
            sell_price: 0.0,
            checked: false,
            folder_path: String::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum StratEvent {
    /// Применён полный snapshot (`Full=true`).
    SnapshotFull {
        server_epoch: u64,
        raw_data: Vec<u8>,
    },
    /// Применён частичный snapshot (`Full=false`).
    SnapshotPartial {
        server_epoch: u64,
        raw_data: Vec<u8>,
    },
    /// Стратегия удалена.
    Deleted { strategy_id: u64 },
    /// Цена продажи обновлена.
    SellPriceUpdated { strategy_id: u64, sell_price: f64 },
    /// Checked-флаги синхронизированы (полная замена или delta).
    CheckedSynced { changed: usize, is_delta: bool },
    /// Эхо checked-state от сервера (после нашего sync).
    CheckedEcho { count: usize },
    /// **Сервер просит у нас snapshot стратегий**.
    /// Это `TStratSnapshotRequest` от сервера. Delphi отвечает fresh rebuild'ом
    /// из живого `Strats`; Rust dispatcher делает то же из `StratsState`.
    /// Если приложение ещё не дало стратегий и серверный snapshot ещё не пришёл,
    /// ответом будет корректный пустой `TStratSnapshot`.
    SnapshotRequested { uid: u64 },
    /// Команда не применима (Unknown).
    Ignored,
}

/// Sync state стратегий клиента — обновляется через `apply(StratCommand)` при получении
/// `MPC_Strat` от сервера.
///
/// **Snapshot применяется через `apply_snapshot_decoded(deflate_data)`** — для полного
/// snapshot'а dispatcher распаковывает raw payload через
/// [`crate::commands::strategy_serializer`] и применяет декодированный batch.
#[derive(Debug, Default)]
pub struct StratsState {
    /// `strategy_id → StrategyInfo`. Удаляется при `TStratDelete`.
    pub by_id: HashMap<u64, StrategyInfo>,
    /// `strategy_id → StrategySnapshot`. Полный decoded snapshot, которым владеет
    /// active library: из него строится ответ на `TStratSnapshotRequest` и его же
    /// читает пользовательский код через API.
    snapshots_by_id: HashMap<u64, StrategySnapshot>,
    /// Серверный epoch последнего применённого snapshot'а — для детекции
    /// out-of-order snapshot'ов после reconnect'а.
    pub last_server_epoch: u64,
}

impl StratsState {
    pub fn new() -> Self {
        Self::default()
    }

    fn get_or_insert(&mut self, strategy_id: u64) -> &mut StrategyInfo {
        self.by_id
            .entry(strategy_id)
            .or_insert_with(|| StrategyInfo::new(strategy_id))
    }

    fn clear_entries(&mut self) {
        self.by_id.clear();
        self.snapshots_by_id.clear();
    }

    /// Применить распарсенную команду.
    pub fn apply(&mut self, cmd: StratCommand) -> StratEvent {
        match cmd {
            StratCommand::Snapshot(snap) => {
                self.last_server_epoch = snap.server_epoch;
                if snap.full {
                    StratEvent::SnapshotFull {
                        server_epoch: snap.server_epoch,
                        raw_data: snap.data,
                    }
                } else {
                    StratEvent::SnapshotPartial {
                        server_epoch: snap.server_epoch,
                        raw_data: snap.data,
                    }
                }
            }
            StratCommand::Delete(d) => {
                self.by_id.remove(&d.strategy_id);
                self.snapshots_by_id.remove(&d.strategy_id);
                StratEvent::Deleted {
                    strategy_id: d.strategy_id,
                }
            }
            StratCommand::SellPriceUpdate(u) => match self.by_id.get_mut(&u.strategy_id) {
                Some(entry) => {
                    entry.sell_price = u.sell_price;
                    StratEvent::SellPriceUpdated {
                        strategy_id: u.strategy_id,
                        sell_price: u.sell_price,
                    }
                }
                None => StratEvent::Ignored,
            },
            StratCommand::CheckedSync(s) => {
                let mut changed = 0;
                if !s.is_delta {
                    // Полная замена — сначала пометить все existing как unchecked.
                    for (_, info) in self.by_id.iter_mut() {
                        info.checked = false;
                    }
                    for snapshot in self.snapshots_by_id.values_mut() {
                        snapshot.checked = false;
                    }
                }
                for it in &s.items {
                    if let Some(entry) = self.by_id.get_mut(&it.strategy_id) {
                        if entry.checked != it.checked {
                            entry.checked = it.checked;
                            changed += 1;
                        }
                    }
                    if let Some(snapshot) = self.snapshots_by_id.get_mut(&it.strategy_id) {
                        snapshot.checked = it.checked;
                    }
                }
                StratEvent::CheckedSynced {
                    changed,
                    is_delta: s.is_delta,
                }
            }
            StratCommand::CheckedEcho(e) => StratEvent::CheckedEcho {
                count: e.items.len(),
            },
            StratCommand::SnapshotRequest { uid } => StratEvent::SnapshotRequested { uid },
            StratCommand::Unknown { .. } => StratEvent::Ignored,
        }
    }

    /// Обновить стратегию из распарсенного TStrategySerializer snapshot'а.
    pub fn upsert(&mut self, strategy_id: u64, last_date: u64, folder_path: String) {
        let entry = self.get_or_insert(strategy_id);
        entry.last_date = last_date;
        entry.folder_path = folder_path;
    }

    /// Заменить owned strategy list списком из приложения.
    ///
    /// Это public API для active library: пользовательский код вызывает его до
    /// init, dispatcher дальше сам поддерживает этот список через протокол.
    pub fn replace_with_snapshots(&mut self, strategies: &[StrategySnapshot]) {
        self.clear_entries();
        for strategy in strategies {
            self.insert_snapshot_unchecked(strategy.clone());
        }
    }

    /// Вставить/обновить одну application-owned стратегию без rollback guard.
    ///
    /// Для локального списка приложение является источником правды, поэтому явно
    /// переданный snapshot должен заменить прежний даже при равных датах/версиях.
    pub fn upsert_local_snapshot(&mut self, strategy: StrategySnapshot) {
        self.insert_snapshot_unchecked(strategy);
    }

    fn insert_snapshot_unchecked(&mut self, s: StrategySnapshot) {
        {
            let entry = self.get_or_insert(s.strategy_id);
            entry.strategy_ver = s.strategy_ver;
            entry.last_date = s.last_date;
            entry.folder_path = s.path.clone();
            entry.checked = s.checked;
        }
        self.snapshots_by_id.insert(s.strategy_id, s);
    }

    /// Применить decoded snapshot одной стратегии (после `parse_strategy_batch`).
    /// Обновляет `last_date`, `folder_path`, `checked` из header'а и сохраняет
    /// полный `StrategySnapshot` для API и ответа на `TStratSnapshotRequest`.
    pub fn upsert_from_snapshot(&mut self, s: &StrategySnapshot) -> bool {
        let existed = self.by_id.contains_key(&s.strategy_id);
        {
            let entry = self.get_or_insert(s.strategy_id);
            if existed && entry.last_date >= s.last_date && entry.strategy_ver >= s.strategy_ver {
                return false;
            }
            entry.strategy_ver = s.strategy_ver;
            entry.last_date = s.last_date;
            entry.folder_path = s.path.clone();
            entry.checked = s.checked;
        }
        self.snapshots_by_id.insert(s.strategy_id, s.clone());
        true
    }

    /// Применить всю batch стратегий из `TStratSnapshot.data` (DEFLATE-compressed payload).
    /// Возвращает декодированный `StrategyBatch` для дальнейшего использования потребителем
    /// (поля стратегий доступны как `HashMap<String, FieldValue>`).
    ///
    /// Возвращает `None` если payload повреждён.
    pub fn apply_snapshot_decoded_with_mode(
        &mut self,
        deflate_data: &[u8],
        full: bool,
    ) -> Option<StrategyBatch> {
        let batch = parse_strategy_batch(deflate_data)?;
        if full {
            self.clear_entries();
        }
        for s in &batch.strategies {
            self.upsert_from_snapshot(s);
        }
        Some(batch)
    }

    pub fn apply_snapshot_decoded(&mut self, deflate_data: &[u8]) -> Option<StrategyBatch> {
        self.apply_snapshot_decoded_with_mode(deflate_data, false)
    }

    pub fn upsert_checked_items(&mut self, items: &[StratCheckedItem]) {
        for it in items {
            let entry = self.get_or_insert(it.strategy_id);
            entry.checked = it.checked;
        }
    }

    pub fn get(&self, strategy_id: u64) -> Option<&StrategyInfo> {
        self.by_id.get(&strategy_id)
    }

    pub fn snapshot(&self, strategy_id: u64) -> Option<&StrategySnapshot> {
        self.snapshots_by_id.get(&strategy_id)
    }

    pub fn snapshots(&self) -> impl Iterator<Item = &StrategySnapshot> {
        self.snapshots_by_id.values()
    }

    pub fn snapshot_vec(&self) -> Vec<StrategySnapshot> {
        let mut out: Vec<_> = self.snapshots_by_id.values().cloned().collect();
        out.sort_by_key(|s| s.strategy_id);
        out
    }

    pub fn iter(&self) -> impl Iterator<Item = (&u64, &StrategyInfo)> {
        self.by_id.iter()
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    pub fn clear(&mut self) {
        self.clear_entries();
        self.last_server_epoch = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::strat::{StratCheckedSync, StratDelete, StratSellPriceUpdate};
    use crate::commands::strategy_serializer::FieldValue;

    #[test]
    fn sell_price_update_ignores_unknown_strategy() {
        let mut s = StratsState::new();
        let ev = s.apply(StratCommand::SellPriceUpdate(StratSellPriceUpdate {
            strategy_id: 100,
            sell_price: 50.5,
        }));
        assert!(matches!(ev, StratEvent::Ignored));
        assert!(s.get(100).is_none());
    }

    #[test]
    fn sell_price_update_existing_strategy() {
        let mut s = StratsState::new();
        s.upsert(100, 0, "F".into());
        let ev = s.apply(StratCommand::SellPriceUpdate(StratSellPriceUpdate {
            strategy_id: 100,
            sell_price: 50.5,
        }));
        match ev {
            StratEvent::SellPriceUpdated {
                strategy_id,
                sell_price,
            } => {
                assert_eq!(strategy_id, 100);
                assert_eq!(sell_price, 50.5);
            }
            other => panic!("wrong event: {other:?}"),
        }
        assert_eq!(s.get(100).unwrap().sell_price, 50.5);
    }

    #[test]
    fn delete_removes_entry() {
        let mut s = StratsState::new();
        let mut fields = HashMap::new();
        fields.insert("Name".to_string(), FieldValue::String("A".to_string()));
        s.upsert_from_snapshot(&StrategySnapshot {
            strategy_id: 100,
            strategy_ver: 1,
            last_date: 1,
            checked: true,
            kind: 1,
            path: "F".into(),
            fields,
        });
        s.apply(StratCommand::Delete(StratDelete {
            strategy_id: 100,
            folder_path: "".into(),
        }));
        assert!(s.get(100).is_none());
        assert!(s.snapshot(100).is_none());
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
        // id=2 не трогался.
        assert!(!s.get(2).unwrap().checked);
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

        let mut b = StrategyBatchBuilder::new();
        let mut fields1 = HashMap::new();
        fields1.insert(
            "Name".to_string(),
            FieldValue::String("Strat-A".to_string()),
        );
        b.write_strategy(&StrategySnapshot {
            strategy_id: 100,
            strategy_ver: 1,
            last_date: 1737000000000,
            checked: true,
            kind: 5,
            path: "F/A".to_string(),
            fields: fields1,
        });
        let mut fields2 = HashMap::new();
        fields2.insert(
            "Name".to_string(),
            FieldValue::String("Strat-B".to_string()),
        );
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
        assert_eq!(
            s.snapshot(100).and_then(|snap| snap.fields.get("Name")),
            Some(&FieldValue::String("Strat-A".to_string()))
        );

        let info200 = s.get(200).unwrap();
        assert_eq!(info200.folder_path, "F/B");
        assert!(!info200.checked);

        // Поля стратегий доступны через возвращённый batch
        assert_eq!(
            batch.strategies[0].fields.get("Name"),
            Some(&FieldValue::String("Strat-A".to_string()))
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
    fn full_snapshot_replaces_missing_strategies() {
        use crate::commands::strategy_serializer::{FieldValue, StrategyBatchBuilder};

        let mut old_fields = HashMap::new();
        old_fields.insert("Name".to_string(), FieldValue::String("Old".to_string()));
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

        let mut new_fields = HashMap::new();
        new_fields.insert("Name".to_string(), FieldValue::String("New".to_string()));
        let mut builder = StrategyBatchBuilder::new();
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

        assert!(s.get(1).is_none());
        assert!(s.snapshot(1).is_none());
        assert!(s.get(2).is_some());
        assert!(s.snapshot(2).is_some());
    }

    #[test]
    fn checked_sync_full_resets_others() {
        let mut s = StratsState::new();
        // Изначально id=1 и id=2 checked.
        s.upsert(1, 0, "".into());
        s.upsert(2, 0, "".into());
        s.by_id.get_mut(&1).unwrap().checked = true;
        s.by_id.get_mut(&2).unwrap().checked = true;
        // Full sync — только id=1 checked. id=2 должен стать unchecked.
        let cmd = StratCommand::CheckedSync(StratCheckedSync {
            items: vec![StratCheckedItem {
                strategy_id: 1,
                checked: true,
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
        assert!(s.get(1).unwrap().checked);
        assert!(!s.get(2).unwrap().checked);
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
        let mut fields = HashMap::new();
        fields.insert("Name".to_string(), FieldValue::String("Old".to_string()));
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
    }

    #[test]
    fn snapshot_applies_new_zero_version_strategy() {
        use crate::commands::strategy_serializer::{FieldValue, StrategySnapshot};

        let mut s = StratsState::new();
        let mut fields = HashMap::new();
        fields.insert("Name".to_string(), FieldValue::String("Zero".to_string()));

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
}
