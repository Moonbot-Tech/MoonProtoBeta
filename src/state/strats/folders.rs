//! Delphi `TStrategies` folder tree helpers.

use super::StratsState;

impl StratsState {
    pub(super) fn folder_key(path: &str) -> String {
        path.to_lowercase()
    }

    fn is_same_or_child_folder(candidate_key: &str, folder_key: &str) -> bool {
        candidate_key == folder_key
            || candidate_key
                .strip_prefix(folder_key)
                .is_some_and(|rest| rest.starts_with('/'))
    }

    pub(super) fn create_folders_for_path(&mut self, path: &str) {
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

    pub(super) fn remove_strategy_by_id(&mut self, strategy_id: u64) -> bool {
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

    pub(super) fn delete_folder_by_path(&mut self, path: &str) -> bool {
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
}
