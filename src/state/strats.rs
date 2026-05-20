//! Strats sync state — apply StratCommand'ы к локальной модели стратегий.
//!
//! Источник Delphi: `MoonProtoClient.pas:689-800 ProcessStratCommand`.
//!
//! ## Декодинг TStratSnapshot.Data
//!
//! Сервер шлёт сериализованную пачку стратегий в `TStratSnapshot.data: Vec<u8>` через
//! `TStrategySerializer` (RTTI-driven). `apply_snapshot_decoded()` парсит этот blob через
//! `commands::strategy_serializer::parse_strategy_batch` и upsert'ит каждую стратегию в state.
//! События `StratEvent::SnapshotFull/Partial { raw_data }` сохраняют исходный
//! `TStrategySerializer` payload для потребителей, которым нужен полный набор
//! полей через `HashMap<String, FieldValue>` для UI-рендеринга.

use std::collections::HashMap;
use crate::commands::strat::{StratCommand, StratCheckedItem, StratSnapshot};
use crate::commands::strategy_serializer::{parse_strategy_batch, StrategyBatch, StrategySnapshot};

/// Информация по одной стратегии — то что хранится клиентом.
#[derive(Debug, Clone)]
pub struct StrategyInfo {
    /// Уникальный идентификатор стратегии (от сервера). 0 = не валидный.
    pub strategy_id: u64,
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
    SnapshotFull { server_epoch: u64, raw_data: Vec<u8> },
    /// Применён частичный snapshot (`Full=false`).
    SnapshotPartial { server_epoch: u64, raw_data: Vec<u8> },
    /// Стратегия удалена.
    Deleted { strategy_id: u64 },
    /// Цена продажи обновлена.
    SellPriceUpdated { strategy_id: u64, sell_price: f64 },
    /// Checked-флаги синхронизированы (полная замена или delta).
    CheckedSynced { changed: usize, is_delta: bool },
    /// Эхо checked-state от сервера (после нашего sync).
    CheckedEcho { count: usize },
    /// **Сервер просит у нас snapshot стратегий** (audit_responsibility B3).
    /// Это `TStratSnapshotRequest` от сервера. Если у диспетчера есть cached full
    /// snapshot, `dispatch_into_active` отвечает автоматически. Иначе приложение
    /// может построить typed snapshot через `client.strat_send_snapshot_batch(...)`.
    SnapshotRequested { uid: u64 },
    /// Команда не применима (Unknown).
    Ignored,
}

/// Sync state стратегий клиента — обновляется через `apply(StratCommand)` при получении
/// `MPC_Strat` от сервера.
///
/// **Snapshot применяется через `apply_snapshot_decoded(deflate_data)`** — для полного
/// snapshot'а потребитель должен распаковать raw payload через
/// [`crate::commands::strategy_serializer`] и применить декодированный batch.
#[derive(Debug, Default)]
pub struct StratsState {
    /// `strategy_id → StrategyInfo`. Удаляется при `TStratDelete`.
    pub by_id: HashMap<u64, StrategyInfo>,
    /// Серверный epoch последнего применённого snapshot'а — для детекции
    /// out-of-order snapshot'ов после reconnect'а.
    pub last_server_epoch: u64,
    /// Последний полный `TStratSnapshot` от сервера (`full=true`). Хранится для
    /// **auto-echo** на серверный `TStratSnapshotRequest`: либа сама шлёт обратно
    /// корректный CmdId=2 пакет через `client.strat_send_snapshot_payload(...)`
    /// в `EventDispatcher::dispatch_into_active`.
    /// Это аналог Delphi `MoonProtoClient.pas:695-699` где клиент auto-respond'ит
    /// `TStratSnapshot.CreateFromStrats(Strats)`.
    ///
    /// **Внимание**: echo'ится **последний полученный** snapshot. Если приложение
    /// модифицировало стратегии локально и эти изменения ещё не дошли до сервера
    /// — они потеряются. Для нормального flow клиент шлёт мутации через
    /// `client.strat_*` API → сервер их применяет → следующий SnapshotRequest
    /// получит уже изменённое. См. responsibility audit F5.
    ///
    /// `None` пока не пришёл ни один full snapshot — в этом случае auto-echo
    /// пропускается, app получит `StratEvent::SnapshotRequested` как раньше.
    pub last_full_snapshot: Option<StratSnapshot>,
}

impl StratsState {
    pub fn new() -> Self {
        Self::default()
    }

    fn get_or_insert(&mut self, strategy_id: u64) -> &mut StrategyInfo {
        self.by_id.entry(strategy_id).or_insert_with(|| StrategyInfo::new(strategy_id))
    }

    /// Применить распарсенную команду.
    pub fn apply(&mut self, cmd: StratCommand) -> StratEvent {
        match cmd {
            StratCommand::Snapshot(snap) => {
                self.last_server_epoch = snap.server_epoch;
                if snap.full {
                    // Сохраняем для auto-echo на следующий SnapshotRequest (audit F5).
                    // Clone — single full snapshot, частота низкая (~раз в сессию).
                    self.last_full_snapshot = Some(snap.clone());
                    StratEvent::SnapshotFull { server_epoch: snap.server_epoch, raw_data: snap.data }
                } else {
                    StratEvent::SnapshotPartial { server_epoch: snap.server_epoch, raw_data: snap.data }
                }
            }
            StratCommand::Delete(d) => {
                self.by_id.remove(&d.strategy_id);
                StratEvent::Deleted { strategy_id: d.strategy_id }
            }
            StratCommand::SellPriceUpdate(u) => {
                let entry = self.get_or_insert(u.strategy_id);
                entry.sell_price = u.sell_price;
                StratEvent::SellPriceUpdated { strategy_id: u.strategy_id, sell_price: u.sell_price }
            }
            StratCommand::CheckedSync(s) => {
                let mut changed = 0;
                if !s.is_delta {
                    // Полная замена — сначала пометить все existing как unchecked.
                    for (_, info) in self.by_id.iter_mut() {
                        info.checked = false;
                    }
                }
                for it in &s.items {
                    let entry = self.get_or_insert(it.strategy_id);
                    if entry.checked != it.checked {
                        entry.checked = it.checked;
                        changed += 1;
                    }
                }
                StratEvent::CheckedSynced { changed, is_delta: s.is_delta }
            }
            StratCommand::CheckedEcho(e) => StratEvent::CheckedEcho { count: e.items.len() },
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

    /// Применить decoded snapshot одной стратегии (после `parse_strategy_batch`).
    /// Обновляет `last_date`, `folder_path`, `checked` из header'а. Поля стратегии (`fields`)
    /// отдаются потребителю наружу — этот state хранит только sync-сводку.
    pub fn upsert_from_snapshot(&mut self, s: &StrategySnapshot) {
        let entry = self.get_or_insert(s.strategy_id);
        entry.last_date = s.last_date;
        entry.folder_path = s.path.clone();
        entry.checked = s.checked;
    }

    /// Применить всю batch стратегий из `TStratSnapshot.data` (DEFLATE-compressed payload).
    /// Возвращает декодированный `StrategyBatch` для дальнейшего использования потребителем
    /// (поля стратегий доступны как `HashMap<String, FieldValue>`).
    ///
    /// Возвращает `None` если payload повреждён.
    pub fn apply_snapshot_decoded(&mut self, deflate_data: &[u8]) -> Option<StrategyBatch> {
        let batch = parse_strategy_batch(deflate_data)?;
        for s in &batch.strategies {
            self.upsert_from_snapshot(s);
        }
        Some(batch)
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
        self.by_id.clear();
        self.last_server_epoch = 0;
        self.last_full_snapshot = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::strat::{StratSellPriceUpdate, StratDelete, StratCheckedSync};

    #[test]
    fn sell_price_creates_entry() {
        let mut s = StratsState::new();
        s.apply(StratCommand::SellPriceUpdate(StratSellPriceUpdate {
            strategy_id: 100,
            sell_price: 50.5,
        }));
        assert_eq!(s.get(100).unwrap().sell_price, 50.5);
    }

    #[test]
    fn delete_removes_entry() {
        let mut s = StratsState::new();
        s.upsert(100, 0, "F".into());
        s.apply(StratCommand::Delete(StratDelete { strategy_id: 100, folder_path: "".into() }));
        assert!(s.get(100).is_none());
    }

    #[test]
    fn checked_sync_delta() {
        let mut s = StratsState::new();
        s.upsert(1, 0, "".into());
        s.upsert(2, 0, "".into());
        // Дельта: только id=1 → checked.
        let cmd = StratCommand::CheckedSync(StratCheckedSync {
            items: vec![StratCheckedItem { strategy_id: 1, checked: true }],
            is_delta: true,
        });
        let ev = s.apply(cmd);
        assert!(matches!(ev, StratEvent::CheckedSynced { changed: 1, is_delta: true }));
        assert!(s.get(1).unwrap().checked);
        // id=2 не трогался.
        assert!(!s.get(2).unwrap().checked);
    }

    #[test]
    fn checked_sync_accepts_more_than_former_rust_cap() {
        let mut s = StratsState::new();
        for strategy_id in 1..=50_001u64 {
            s.upsert_checked_items(&[StratCheckedItem { strategy_id, checked: true }]);
        }

        assert_eq!(s.len(), 50_001);
        assert!(s.get(50_001).unwrap().checked);
    }

    #[test]
    fn apply_snapshot_decoded_upserts_strategies() {
        use crate::commands::strategy_serializer::{StrategyBatchBuilder, FieldValue};

        let mut b = StrategyBatchBuilder::new();
        let mut fields1 = HashMap::new();
        fields1.insert("Name".to_string(), FieldValue::String("Strat-A".to_string()));
        b.write_strategy(&StrategySnapshot {
            strategy_id: 100, strategy_ver: 1, last_date: 1737000000000,
            checked: true, kind: 5, path: "F/A".to_string(), fields: fields1,
        });
        let mut fields2 = HashMap::new();
        fields2.insert("Name".to_string(), FieldValue::String("Strat-B".to_string()));
        b.write_strategy(&StrategySnapshot {
            strategy_id: 200, strategy_ver: 2, last_date: 1737000000001,
            checked: false, kind: 6, path: "F/B".to_string(), fields: fields2,
        });

        let payload = b.finalize();

        let mut s = StratsState::new();
        let batch = s.apply_snapshot_decoded(&payload).unwrap();
        assert_eq!(batch.strategies.len(), 2);

        let info100 = s.get(100).unwrap();
        assert_eq!(info100.last_date, 1737000000000);
        assert_eq!(info100.folder_path, "F/A");
        assert!(info100.checked);

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
    fn checked_sync_full_resets_others() {
        let mut s = StratsState::new();
        // Изначально id=1 и id=2 checked.
        s.upsert(1, 0, "".into());
        s.upsert(2, 0, "".into());
        s.by_id.get_mut(&1).unwrap().checked = true;
        s.by_id.get_mut(&2).unwrap().checked = true;
        // Full sync — только id=1 checked. id=2 должен стать unchecked.
        let cmd = StratCommand::CheckedSync(StratCheckedSync {
            items: vec![StratCheckedItem { strategy_id: 1, checked: true }],
            is_delta: false,
        });
        s.apply(cmd);
        assert!(s.get(1).unwrap().checked);
        assert!(!s.get(2).unwrap().checked);
    }
}
