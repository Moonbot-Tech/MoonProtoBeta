//! Read-only accessors for the orders read-model.

use super::{Order, Orders};

impl Orders {
    /// Get one order by UID.
    pub fn get(&self, uid: u64) -> Option<&Order> {
        self.map.get(&uid).map(AsRef::as_ref)
    }

    /// Iterate all retained orders.
    pub fn iter(&self) -> impl Iterator<Item = &Order> {
        self.map.values().map(AsRef::as_ref)
    }

    /// Number of retained orders.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Current snapshot flag value.
    pub fn current_snapshot_flag(&self) -> u8 {
        self.current_snapshot_flag
    }
}
