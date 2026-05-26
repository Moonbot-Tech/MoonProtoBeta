//! Strategy snapshot apply/cache helpers.

use super::{StrategySnapshotPayloadCache, StratsState};
use crate::commands::strat::StratCheckedItem;
use crate::commands::strategy_serializer::{
    parse_strategy_batch_for_each_with_schema_field_types, parse_strategy_batch_with_schema,
    parse_strategy_batch_with_schema_field_types, FieldValue, StrategyBatch, StrategySnapshot,
};
use std::sync::Arc;

impl StratsState {
    pub(super) fn invalidate_snapshot_payload_cache(&mut self) {
        self.snapshot_payload_cache = None;
    }

    fn set_snapshot_payload_cache_from_wire(
        &mut self,
        client_max_last_date: u64,
        deflate_data: &[u8],
    ) {
        self.snapshot_payload_cache = Some(Arc::new(StrategySnapshotPayloadCache {
            client_max_last_date,
            data: deflate_data.to_vec(),
        }));
    }

    fn update_snapshot_payload_cache_after_apply(
        &mut self,
        applied_count: usize,
        client_max_last_date: u64,
        deflate_data: &[u8],
        changed: bool,
    ) {
        if applied_count == self.snapshots_by_id.len() {
            self.set_snapshot_payload_cache_from_wire(client_max_last_date, deflate_data);
        } else if changed {
            self.invalidate_snapshot_payload_cache();
        }
    }

    fn sell_price_from_snapshot(s: &StrategySnapshot) -> f64 {
        match s.fields.get("SellPrice") {
            Some(FieldValue::Double(v)) => *v,
            _ => 0.0,
        }
    }

    /// Обновить стратегию из распарсенного TStrategySerializer snapshot'а.
    pub fn upsert(&mut self, strategy_id: u64, last_date: u64, folder_path: String) {
        let entry = self.get_or_insert(strategy_id);
        entry.last_date = last_date;
        entry.folder_path = folder_path;
        let path = entry.folder_path.clone();
        self.create_folders_for_path(&path);
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
            entry.sell_price = Self::sell_price_from_snapshot(&s);
            entry.checked = s.checked;
            entry.prev_checked = s.checked;
        }
        self.create_folders_for_path(&s.path);
        self.snapshots_by_id.insert(s.strategy_id, Arc::new(s));
        self.invalidate_snapshot_payload_cache();
    }

    /// Применить decoded snapshot одной стратегии (после `parse_strategy_batch`).
    /// Обновляет `last_date`, `folder_path`, `checked` из header'а и сохраняет
    /// полный `StrategySnapshot` для API и ответа на `TStratSnapshotRequest`.
    pub fn upsert_from_snapshot(&mut self, s: &StrategySnapshot) -> bool {
        {
            let (existed, entry) = self.get_or_insert_with_existed(s.strategy_id);
            if existed && entry.last_date >= s.last_date && entry.strategy_ver >= s.strategy_ver {
                return false;
            }
            entry.strategy_ver = s.strategy_ver;
            entry.last_date = s.last_date;
            entry.folder_path = s.path.clone();
            entry.sell_price = Self::sell_price_from_snapshot(s);
            entry.checked = s.checked;
            entry.prev_checked = s.checked;
        }
        self.create_folders_for_path(&s.path);
        self.snapshots_by_id
            .insert(s.strategy_id, Arc::new(s.clone()));
        self.invalidate_snapshot_payload_cache();
        true
    }

    fn upsert_snapshot_owned_without_cache_invalidation(&mut self, s: StrategySnapshot) -> bool {
        {
            let (existed, entry) = self.get_or_insert_with_existed(s.strategy_id);
            if existed && entry.last_date >= s.last_date && entry.strategy_ver >= s.strategy_ver {
                return false;
            }
            entry.strategy_ver = s.strategy_ver;
            entry.last_date = s.last_date;
            entry.folder_path = s.path.clone();
            entry.sell_price = Self::sell_price_from_snapshot(&s);
            entry.checked = s.checked;
            entry.prev_checked = s.checked;
        }
        self.create_folders_for_path(&s.path);
        self.snapshots_by_id.insert(s.strategy_id, Arc::new(s));
        true
    }

    /// Применить всю batch стратегий из `TStratSnapshot.data` (DEFLATE-compressed payload).
    /// Возвращает декодированный `StrategyBatch` для дальнейшего использования потребителем
    /// (поля стратегий доступны как `StrategyFields`).
    ///
    /// Возвращает `None` если payload повреждён.
    pub fn apply_snapshot_decoded_with_mode(
        &mut self,
        deflate_data: &[u8],
        full: bool,
    ) -> Option<StrategyBatch> {
        let batch = match self.schema_field_types.as_deref() {
            Some(field_types) => {
                parse_strategy_batch_with_schema_field_types(deflate_data, Some(field_types))?
            }
            None => parse_strategy_batch_with_schema(deflate_data, None)?,
        };
        let _ = full;
        // Delphi `ApplyStratSnapshot(IsFull=true)` does not clear strategies
        // absent from the incoming payload. They remain local "Own" strategies.
        let count = batch.strategies.len();
        let mut changed = false;
        let mut client_max_last_date = 0u64;
        for s in &batch.strategies {
            client_max_last_date = client_max_last_date.max(s.last_date);
            changed |= self.upsert_from_snapshot(s);
        }
        self.update_snapshot_payload_cache_after_apply(
            count,
            client_max_last_date,
            deflate_data,
            changed,
        );
        Some(batch)
    }

    pub(crate) fn apply_snapshot_decoded_with_mode_in_place(
        &mut self,
        deflate_data: &[u8],
        full: bool,
    ) -> Option<usize> {
        let _ = full;
        let field_types = self.schema_field_types.clone();
        let mut changed = false;
        let mut client_max_last_date = 0u64;
        let count = parse_strategy_batch_for_each_with_schema_field_types(
            deflate_data,
            field_types.as_deref(),
            |s| {
                client_max_last_date = client_max_last_date.max(s.last_date);
                changed |= self.upsert_snapshot_owned_without_cache_invalidation(s);
            },
        )?;
        self.update_snapshot_payload_cache_after_apply(
            count,
            client_max_last_date,
            deflate_data,
            changed,
        );
        Some(count)
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

    pub(crate) fn snapshot_payload_cache(&mut self) -> Option<Arc<StrategySnapshotPayloadCache>> {
        if let Some(cache) = &self.snapshot_payload_cache {
            return Some(Arc::clone(cache));
        }

        if self.snapshots_by_id.is_empty() {
            let cache = Arc::new(StrategySnapshotPayloadCache {
                client_max_last_date: 0,
                data: crate::commands::strategy_serializer::StrategyBatchBuilder::empty_payload(),
            });
            self.snapshot_payload_cache = Some(Arc::clone(&cache));
            return Some(cache);
        }

        let schema = Arc::clone(self.schema.as_ref()?);
        let mut builder = crate::commands::strategy_serializer::StrategyBatchBuilder::new(&schema);
        let mut client_max_last_date = 0u64;
        for strategy in self.snapshots() {
            client_max_last_date = client_max_last_date.max(strategy.last_date);
            builder.write_strategy(strategy);
        }
        let cache = Arc::new(StrategySnapshotPayloadCache {
            client_max_last_date,
            data: builder.finalize(),
        });
        self.snapshot_payload_cache = Some(Arc::clone(&cache));
        Some(cache)
    }
}
