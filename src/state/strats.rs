//! Strategy sync state maintained from `MPC_Strat` commands.
//!
//! Delphi source: `MoonProtoClient.pas:689-800 ProcessStratCommand`.
//!
//! `TStratSnapshot.Data` decoding:
//!
//! The server sends an RTTI-driven serialized strategy batch in
//! `TStratSnapshot.data`. `apply_snapshot_decoded()` parses that blob through
//! `commands::strategy_serializer::parse_strategy_batch_with_schema` and applies
//! every strategy with the Delphi rollback guard by `StrategyLastDate` /
//! `StrategyVer`. State keeps both lightweight `StrategyInfo` and the full
//! decoded `StrategySnapshot`, so the Active Lib can answer
//! `TStratSnapshotRequest` itself and applications can read the latest snapshot
//! through public API.

use crate::commands::strat::{StratCheckedItem, StratCommand};
use crate::commands::strategy_schema::StrategySchema;
use crate::commands::strategy_serializer::{StrategyActiveMode, StrategyKind, StrategySnapshot};
use std::collections::{hash_map::Entry, HashMap};
use std::sync::Arc;

mod folders;
mod schema;
mod snapshots;
mod types;

pub(crate) use self::types::StrategySnapshotPayloadCache;
pub use self::types::{StratEvent, StrategyInfo};

/// Client strategy sync state.
///
/// Full snapshots are applied through `apply_snapshot_decoded(deflate_data)`:
/// the dispatcher decompresses the raw payload through
/// [`crate::commands::strategy_serializer`] and applies the decoded batch.
#[derive(Debug, Clone, Default)]
pub struct StratsState {
    /// `strategy_id -> StrategyInfo`; entries are removed by `TStratDelete`.
    pub by_id: HashMap<u64, StrategyInfo>,
    /// Delphi `TStrategies` list order. `by_id` is only the lookup index.
    order: Vec<u64>,
    /// Delphi folder tree analogue, keyed case-insensitively like `SameText`.
    /// Values keep the first observed spelling of the full folder path.
    folders_by_key: HashMap<String, String>,
    /// Full decoded strategy snapshots owned by the Active Lib.
    ///
    /// They are used both for answering `TStratSnapshotRequest` and for
    /// application reads through public API.
    snapshots_by_id: HashMap<u64, Arc<StrategySnapshot>>,
    /// Server epoch of the latest applied snapshot.
    pub last_server_epoch: u64,
    /// Latest raw `TStratSchema.Data` blob.
    schema_raw: Option<Arc<Vec<u8>>>,
    /// Latest decoded strategy schema.
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

    /// Apply one decoded strategy command.
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

    /// Delphi `TStrategies.IsThereListingStrat`.
    pub fn is_there_listing_strat_like_delphi(&self, mode: StrategyActiveMode) -> bool {
        self.snapshots().any(|s| {
            s.active_like_delphi(mode) && s.kind_like_delphi() == StrategyKind::NEW_LISTING
        })
    }

    pub fn has_listing_strategy(&self, mode: StrategyActiveMode) -> bool {
        self.is_there_listing_strat_like_delphi(mode)
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

    pub fn has_listing_sell_strategy(&self, mode: StrategyActiveMode, is_futures: bool) -> bool {
        self.is_there_listing_sell_like_delphi(mode, is_futures)
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
mod tests;
