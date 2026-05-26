//! Strats sync state — apply StratCommand'ы к локальной модели стратегий.
//!
//! Источник Delphi: `MoonProtoClient.pas:689-800 ProcessStratCommand`.
//!
//! ## Декодинг TStratSnapshot.Data
//!
//! Сервер шлёт сериализованную пачку стратегий в `TStratSnapshot.data: Vec<u8>` через
//! `TStrategySerializer` (RTTI-driven). `apply_snapshot_decoded()` парсит этот blob через
//! `commands::strategy_serializer::parse_strategy_batch_with_schema` и применяет каждую стратегию в state
//! с Delphi rollback guard по `StrategyLastDate`/`StrategyVer`.
//! State хранит и lightweight `StrategyInfo`, и полный decoded `StrategySnapshot`.
//! Поэтому active library может сама отвечать на `TStratSnapshotRequest`, а
//! приложение может читать последний полный snapshot через public API.

use crate::commands::strat::{StratCheckedItem, StratCommand};
use crate::commands::strategy_schema::StrategySchema;
use crate::commands::strategy_serializer::{
    parse_strategy_batch_for_each_with_schema_field_types, parse_strategy_batch_with_schema,
    parse_strategy_batch_with_schema_field_types, FieldValue, StrategyActiveMode, StrategyBatch,
    StrategyKind, StrategySnapshot,
};
use std::collections::{hash_map::Entry, HashMap};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) struct StrategySnapshotPayloadCache {
    pub client_max_last_date: u64,
    pub data: Vec<u8>,
}

/// Информация по одной стратегии — то что хранится клиентом.
#[derive(Debug, Clone)]
pub struct StrategyInfo {
    /// Уникальный идентификатор стратегии (от сервера). 0 = не валидный.
    pub strategy_id: u64,
    /// Версия стратегии из `TStrategySerializer` header.
    pub strategy_ver: i32,
    /// Время последнего апдейта (TDateTime f64 packed как UInt64).
    pub last_date: u64,
    /// Цена продажи из decoded snapshot field `SellPrice`, если это поле есть.
    pub sell_price: f64,
    /// Checked-state (для UI start/stop).
    pub checked: bool,
    /// Last server-acknowledged checked-state (`TStrategy.PrevChecked`).
    pub prev_checked: bool,
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
            prev_checked: false,
            folder_path: String::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum StratEvent {
    /// Полный snapshot (`Full=true`) успешно применён dispatcher'ом.
    SnapshotFull {
        server_epoch: u64,
        raw_data: Vec<u8>,
    },
    /// Частичный snapshot (`Full=false`) успешно применён dispatcher'ом.
    SnapshotPartial {
        server_epoch: u64,
        raw_data: Vec<u8>,
    },
    /// Результат `TStratDelete`: Delphi сначала пробует удалить StrategyID,
    /// затем FolderPath. Событие приходит только если хотя бы одна операция
    /// реально изменила state.
    Deleted {
        strategy_id: u64,
        folder_path: String,
        strategy_deleted: bool,
        folder_deleted: bool,
    },
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
    /// Получена и распарсена schema стратегий (`TStratSchema`, CmdId=8).
    SchemaApplied {
        raw_len: usize,
        format_version: u8,
        kind_count: usize,
        field_count: usize,
    },
    /// Сервер прислал `TStratSchema`, но raw-deflate/body не распарсились.
    SchemaParseFailed { raw_len: usize },
    /// Диагностический вариант для raw parser/users. Client receive path does
    /// not emit it because Delphi client ignores incoming `TStratSchemaRequest`.
    SchemaRequested { uid: u64 },
    /// Low-level diagnostic for commands that the client state does not apply.
    /// The active dispatcher does not emit this for Delphi-inapplicable
    /// incoming command classes such as unknown/skipped, schema request, or
    /// sell-price update.
    Ignored,
}

/// Sync state стратегий клиента — обновляется через `apply(StratCommand)` при получении
/// `MPC_Strat` от сервера.
///
/// **Snapshot применяется через `apply_snapshot_decoded(deflate_data)`** — для полного
/// snapshot'а dispatcher распаковывает raw payload через
/// [`crate::commands::strategy_serializer`] и применяет декодированный batch.
#[derive(Debug, Clone, Default)]
pub struct StratsState {
    /// `strategy_id → StrategyInfo`. Удаляется при `TStratDelete`.
    pub by_id: HashMap<u64, StrategyInfo>,
    /// Delphi `TStrategies` list order. `by_id` is only the lookup index.
    order: Vec<u64>,
    /// Delphi folder tree analogue, keyed case-insensitively like `SameText`.
    /// Values keep the first observed spelling of the full folder path.
    folders_by_key: HashMap<String, String>,
    /// `strategy_id → StrategySnapshot`. Полный decoded snapshot, которым владеет
    /// active library: из него строится ответ на `TStratSnapshotRequest` и его же
    /// читает пользовательский код через API.
    snapshots_by_id: HashMap<u64, Arc<StrategySnapshot>>,
    /// Серверный epoch последнего применённого snapshot'а — для детекции
    /// out-of-order snapshot'ов после reconnect'а.
    pub last_server_epoch: u64,
    /// Последний raw `TStratSchema.Data` blob.
    schema_raw: Option<Arc<Vec<u8>>>,
    /// Последняя decoded schema стратегий.
    schema: Option<Arc<StrategySchema>>,
    /// `TStratSchema` field name -> TypeID cache for Delphi `BuildReaderProps`.
    /// Stored behind `Arc` so `EventDispatcherSnapshot` clones remain cheap.
    schema_field_types: Option<Arc<HashMap<String, u8>>>,
    /// Cached `TStrategySerializer` payload for `TStratSnapshot.CreateFromStrats`.
    snapshot_payload_cache: Option<Arc<StrategySnapshotPayloadCache>>,
    schema_revision: u64,
    schema_failures: u64,
    schema_last_error: Option<String>,
}

impl StratsState {
    pub fn new() -> Self {
        Self::default()
    }

    fn get_or_insert(&mut self, strategy_id: u64) -> &mut StrategyInfo {
        self.get_or_insert_with_existed(strategy_id).1
    }

    fn get_or_insert_with_existed(&mut self, strategy_id: u64) -> (bool, &mut StrategyInfo) {
        match self.by_id.entry(strategy_id) {
            Entry::Occupied(entry) => (true, entry.into_mut()),
            Entry::Vacant(entry) => {
                self.order.push(strategy_id);
                (false, entry.insert(StrategyInfo::new(strategy_id)))
            }
        }
    }

    fn clear_entries(&mut self) {
        self.by_id.clear();
        self.order.clear();
        self.folders_by_key.clear();
        self.snapshots_by_id.clear();
        self.invalidate_snapshot_payload_cache();
    }

    fn invalidate_snapshot_payload_cache(&mut self) {
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

    fn folder_key(path: &str) -> String {
        path.to_lowercase()
    }

    fn is_same_or_child_folder(candidate_key: &str, folder_key: &str) -> bool {
        candidate_key == folder_key
            || candidate_key
                .strip_prefix(folder_key)
                .is_some_and(|rest| rest.starts_with('/'))
    }

    fn create_folders_for_path(&mut self, path: &str) {
        if path.is_empty() {
            return;
        }

        let full_key = Self::folder_key(path);
        if self.folders_by_key.contains_key(&full_key) {
            return;
        }

        let mut current = String::new();
        for part in path.split('/') {
            if !current.is_empty() {
                current.push('/');
            }
            current.push_str(part);
            let key = Self::folder_key(&current);
            self.folders_by_key.entry(key).or_insert(current.clone());
        }
    }

    fn remove_strategy_by_id(&mut self, strategy_id: u64) -> bool {
        let removed = self.by_id.remove(&strategy_id).is_some();
        if removed {
            self.order.retain(|id| *id != strategy_id);
            self.snapshots_by_id.remove(&strategy_id);
            self.invalidate_snapshot_payload_cache();
        }
        removed
    }

    fn folder_has_strategies(&self, folder_key: &str) -> bool {
        self.by_id.values().any(|entry| {
            let entry_key = Self::folder_key(&entry.folder_path);
            Self::is_same_or_child_folder(&entry_key, folder_key)
        })
    }

    fn delete_folder_by_path(&mut self, path: &str) -> bool {
        if path.is_empty() {
            return false;
        }

        let key = Self::folder_key(path);
        if !self.folders_by_key.contains_key(&key) {
            return false;
        }
        if self.folder_has_strategies(&key) {
            return false;
        }

        let deleted_keys: Vec<String> = self
            .folders_by_key
            .keys()
            .filter(|candidate_key| Self::is_same_or_child_folder(candidate_key, &key))
            .cloned()
            .collect();
        for deleted_key in deleted_keys {
            self.folders_by_key.remove(&deleted_key);
        }
        true
    }

    fn sell_price_from_snapshot(s: &StrategySnapshot) -> f64 {
        match s.fields.get("SellPrice") {
            Some(FieldValue::Double(v)) => *v,
            _ => 0.0,
        }
    }

    /// Применить распарсенную команду.
    ///
    /// For `TStratSnapshot`, this returns the raw snapshot event; the active
    /// dispatcher performs the serializer decode/apply and advances
    /// `last_server_epoch` only after that succeeds, matching Delphi
    /// `ProcessStratCommand`.
    pub fn apply(&mut self, cmd: StratCommand) -> StratEvent {
        match cmd {
            StratCommand::Snapshot(snap) => {
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
                let strategy_deleted = if d.strategy_id != 0 {
                    self.remove_strategy_by_id(d.strategy_id)
                } else {
                    false
                };
                let folder_deleted = if d.folder_path.is_empty() {
                    false
                } else {
                    self.delete_folder_by_path(&d.folder_path)
                };
                if strategy_deleted || folder_deleted {
                    StratEvent::Deleted {
                        strategy_id: d.strategy_id,
                        folder_path: d.folder_path,
                        strategy_deleted,
                        folder_deleted,
                    }
                } else {
                    StratEvent::Ignored
                }
            }
            // Delphi client has no TStratSellPriceUpdate receive branch.
            // This command is client -> server; the server applies sg.SellPrice.
            StratCommand::SellPriceUpdate(_) => StratEvent::Ignored,
            StratCommand::CheckedSync(s) => {
                let mut changed = 0;
                let mut snapshot_payload_changed = false;
                for it in &s.items {
                    if let Some(entry) = self.by_id.get_mut(&it.strategy_id) {
                        if entry.checked != it.checked {
                            changed += 1;
                        }
                        entry.checked = it.checked;
                        entry.prev_checked = it.checked;
                    }
                    if let Some(snapshot) = self.snapshots_by_id.get_mut(&it.strategy_id) {
                        let snapshot = Arc::make_mut(snapshot);
                        if snapshot.checked != it.checked {
                            snapshot.checked = it.checked;
                            snapshot_payload_changed = true;
                        }
                    }
                }
                if snapshot_payload_changed {
                    self.invalidate_snapshot_payload_cache();
                }
                StratEvent::CheckedSynced {
                    changed,
                    is_delta: s.is_delta,
                }
            }
            StratCommand::CheckedEcho(e) => {
                for it in &e.items {
                    if let Some(entry) = self.by_id.get_mut(&it.strategy_id) {
                        if entry.checked == it.checked {
                            entry.prev_checked = it.checked;
                        }
                    }
                }
                StratEvent::CheckedEcho {
                    count: e.items.len(),
                }
            }
            StratCommand::SnapshotRequest { uid } => StratEvent::SnapshotRequested { uid },
            // Delphi client `ProcessStratCommand` has no branch for
            // `TStratSchemaRequest`. It is a client->server request handled by
            // the Delphi server, so a server->client copy is freed silently.
            StratCommand::SchemaRequest { .. } => StratEvent::Ignored,
            StratCommand::Skipped { .. } => StratEvent::Ignored,
            StratCommand::Schema(schema) => self.apply_schema_raw(schema.data),
            StratCommand::Unknown { .. } => StratEvent::Ignored,
        }
    }

    fn apply_schema_raw(&mut self, data: Vec<u8>) -> StratEvent {
        let raw_len = data.len();
        match StrategySchema::parse_compressed(&data) {
            Some(schema) => {
                let format_version = schema.format_version;
                let kind_count = schema.kinds.len();
                let field_count = schema.fields.len();
                let field_types = schema
                    .fields
                    .iter()
                    .map(|field| (field.name.clone(), field.raw_type_id))
                    .collect::<HashMap<_, _>>();
                self.schema_raw = Some(Arc::new(data));
                self.schema = Some(Arc::new(schema));
                self.schema_field_types = Some(Arc::new(field_types));
                self.invalidate_snapshot_payload_cache();
                self.schema_revision = self.schema_revision.saturating_add(1);
                self.schema_last_error = None;
                StratEvent::SchemaApplied {
                    raw_len,
                    format_version,
                    kind_count,
                    field_count,
                }
            }
            None => {
                self.schema_failures = self.schema_failures.saturating_add(1);
                self.schema_last_error = Some(format!(
                    "failed to parse TStratSchema raw blob ({raw_len} bytes)"
                ));
                StratEvent::SchemaParseFailed { raw_len }
            }
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

    /// Delphi `TStrategy.Checked := value`: local UI mutation. It changes
    /// checked state but leaves `PrevChecked` untouched until server sync/echo.
    pub fn set_checked(&mut self, strategy_id: u64, checked: bool) -> bool {
        let Some(entry) = self.by_id.get_mut(&strategy_id) else {
            return false;
        };
        entry.checked = checked;
        if let Some(snapshot) = self.snapshots_by_id.get_mut(&strategy_id) {
            let snapshot = Arc::make_mut(snapshot);
            snapshot.checked = checked;
            self.invalidate_snapshot_payload_cache();
        }
        true
    }

    /// Delphi `TStrategies.GetCheckedDelta`.
    pub fn checked_delta(&self) -> Vec<StratCheckedItem> {
        let mut out = Vec::new();
        for strategy_id in &self.order {
            let Some(entry) = self.by_id.get(strategy_id) else {
                continue;
            };
            if entry.checked != entry.prev_checked {
                out.push(StratCheckedItem {
                    strategy_id: *strategy_id,
                    checked: entry.checked,
                });
            }
        }
        out
    }

    pub fn get(&self, strategy_id: u64) -> Option<&StrategyInfo> {
        self.by_id.get(&strategy_id)
    }

    pub fn snapshot(&self, strategy_id: u64) -> Option<&StrategySnapshot> {
        self.snapshots_by_id.get(&strategy_id).map(Arc::as_ref)
    }

    pub fn has_folder(&self, folder_path: &str) -> bool {
        if folder_path.is_empty() {
            return true;
        }
        self.folders_by_key
            .contains_key(&Self::folder_key(folder_path))
    }

    pub fn snapshots(&self) -> impl Iterator<Item = &StrategySnapshot> {
        self.order
            .iter()
            .filter_map(|strategy_id| self.snapshots_by_id.get(strategy_id).map(Arc::as_ref))
    }

    pub fn snapshot_vec(&self) -> Vec<StrategySnapshot> {
        let mut out = Vec::new();
        for strategy_id in &self.order {
            if let Some(snapshot) = self.snapshots_by_id.get(strategy_id) {
                out.push(snapshot.as_ref().clone());
            }
        }
        out
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

    /// Последняя schema стратегий, полученная через `TStratSchemaRequest` в Init.
    pub fn strategy_schema(&self) -> Option<&StrategySchema> {
        self.schema.as_deref()
    }

    /// Raw-deflate blob последней schema, как пришёл в `TStratSchema.Data`.
    pub fn strategy_schema_raw(&self) -> Option<&[u8]> {
        self.schema_raw.as_deref().map(Vec::as_slice)
    }

    pub fn strategy_schema_revision(&self) -> u64 {
        self.schema_revision
    }

    pub fn strategy_schema_failures(&self) -> u64 {
        self.schema_failures
    }

    pub fn strategy_schema_last_error(&self) -> Option<&str> {
        self.schema_last_error.as_deref()
    }

    /// Delphi `TStrategies.IsThereListingStrat`.
    pub fn is_there_listing_strat_like_delphi(&self, mode: StrategyActiveMode) -> bool {
        self.snapshots().any(|s| {
            s.active_like_delphi(mode) && s.kind_like_delphi() == StrategyKind::NEW_LISTING
        })
    }

    /// Delphi `TStrategies.IsThereListingSell`.
    pub fn is_there_listing_sell_like_delphi(
        &self,
        mode: StrategyActiveMode,
        is_futures: bool,
    ) -> bool {
        let has_listing_sell = self.snapshots().any(|s| {
            s.active_like_delphi(mode)
                && s.kind_like_delphi() == StrategyKind::NEW_LISTING
                && s.sell_from_asset_like_delphi()
        });
        if has_listing_sell {
            return true;
        }
        if is_futures {
            return false;
        }
        self.snapshots().any(|s| {
            s.active_like_delphi(mode)
                && s.short_like_delphi()
                && matches!(
                    s.kind_like_delphi(),
                    StrategyKind::MOON_SHOT | StrategyKind::MOON_HOOK
                )
        })
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
        self.schema_raw = None;
        self.schema = None;
        self.schema_revision = 0;
        self.schema_failures = 0;
        self.schema_last_error = None;
    }
}

#[cfg(test)]
mod tests {
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
        let raw = std::fs::read(&path)
            .unwrap_or_else(|err| panic!("cannot read {}: {err}", path.display()));
        let batch = crate::commands::strategy_serializer::parse_strategy_batch(&raw)
            .unwrap_or_else(|| {
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
}
