//! Read-only accessors for the orders read-model.

use super::{Order, Orders};

impl Orders {
    /// Получить ордер по UID.
    pub fn get(&self, uid: u64) -> Option<&Order> {
        self.map.get(&uid)
    }

    /// Итератор по всем ордерам.
    pub fn iter(&self) -> impl Iterator<Item = &Order> {
        self.map.values()
    }

    /// Количество ордеров.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Текущее значение snapshot flag.
    pub fn current_snapshot_flag(&self) -> u8 {
        self.current_snapshot_flag
    }
}
