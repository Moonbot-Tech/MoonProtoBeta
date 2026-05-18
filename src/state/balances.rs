//! Balance sync state — apply snapshot/incremental updates.
//!
//! Источник Delphi: `MoonProtoEngine.pas:1210-1340 ProcessBalanceCommand + OnBalanceSnapshot
//! + OnBalanceIncrement + ApplyBalanceItem`.
//!
//! ## Wire-format
//! Парсер `commands::balance::parse_balance(cmd_id, payload)` уже распаковывает данные.
//! Этот модуль применяет полученные `BalanceUpdate` к локальной модели.
//!
//! `cmd_id`:
//! - **2** = `TBalanceCommand` (legacy snapshot) — обновить globals + items.
//! - **3** = `TBalanceSnapshotFull` — то же что 2 + маркеты не в Items сбрасываются в default.
//! - **4** = `TBalanceIncrUpdate` — incremental: GlobalChanged-gated globals + merge items.

use std::collections::HashMap;
use crate::commands::balance::{BalanceItem, BalanceUpdate};

#[derive(Debug, Clone, Default)]
pub struct GlobalBalance {
    pub btc_balance_total: f64,
    pub btc_balance_locked: f64,
    pub btc_balance_full: f64,
    pub special_coin_balance: f64,
}

#[derive(Debug, Default)]
pub struct BalancesState {
    pub global: GlobalBalance,
    /// market_name → BalanceItem
    pub by_market: HashMap<String, BalanceItem>,
    pub last_epoch: u16,
    /// Epoch уже выставлялся (после первого apply). До этого epoch=0 принимается как валидный.
    epoch_set: bool,
}

#[derive(Debug, Clone)]
pub enum BalanceEvent {
    /// Применён full snapshot (cmd_id=3): N маркетов получили данные, остальные сброшены в default.
    SnapshotApplied { count: usize, epoch: u16 },
    /// Применён legacy snapshot (cmd_id=2): N маркетов обновлены, остальные не трогаются.
    LegacySnapshotApplied { count: usize, epoch: u16 },
    /// Применён incremental update: N маркетов изменилось, globals обновлены если global_changed=true.
    IncrementalApplied { count: usize, epoch: u16, global_changed: bool },
    /// Epoch не прошёл (старее last_epoch wrap-safe).
    EpochStale { incoming: u16, last: u16 },
}

impl BalancesState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Применить распарсенный `BalanceUpdate`.
    /// Epoch protection: `EpochIsOK` byte-exact с `MoonProtoFunc.pas:188-203`.
    pub fn apply(&mut self, upd: BalanceUpdate) -> BalanceEvent {
        // Epoch check (wrap-safe). До первого apply (epoch_set=false) принимаем любой.
        if self.epoch_set && !epoch_is_ok(self.last_epoch, upd.epoch) {
            return BalanceEvent::EpochStale { incoming: upd.epoch, last: self.last_epoch };
        }

        match upd.cmd_id {
            2 => self.apply_legacy_snapshot(upd),
            3 => self.apply_full_snapshot(upd),
            4 => self.apply_incremental(upd),
            _ => BalanceEvent::EpochStale { incoming: upd.epoch, last: self.last_epoch }, // unknown cmd → no-op
        }
    }

    fn apply_legacy_snapshot(&mut self, upd: BalanceUpdate) -> BalanceEvent {
        self.global = GlobalBalance {
            btc_balance_total: upd.btc_balance_total,
            btc_balance_locked: upd.btc_balance_locked,
            btc_balance_full: upd.btc_balance_full,
            special_coin_balance: upd.special_coin_balance,
        };
        let count = upd.items.len();
        for it in upd.items {
            self.by_market.insert(it.market_name.clone(), it);
        }
        self.last_epoch = upd.epoch;
        self.epoch_set = true;
        BalanceEvent::LegacySnapshotApplied { count, epoch: upd.epoch }
    }

    /// Full snapshot (cmd_id=3): маркеты не в Items получают default (Delphi:1253-1275).
    fn apply_full_snapshot(&mut self, upd: BalanceUpdate) -> BalanceEvent {
        self.global = GlobalBalance {
            btc_balance_total: upd.btc_balance_total,
            btc_balance_locked: upd.btc_balance_locked,
            btc_balance_full: upd.btc_balance_full,
            special_coin_balance: upd.special_coin_balance,
        };

        // Replace state — маркеты НЕ в snapshot сбрасываются в default.
        // Default для leverage_x = 1, остальные 0.
        let mut new_map: HashMap<String, BalanceItem> = HashMap::new();
        let count = upd.items.len();
        for it in upd.items {
            new_map.insert(it.market_name.clone(), it);
        }
        self.by_market = new_map;
        self.last_epoch = upd.epoch;
        self.epoch_set = true;
        BalanceEvent::SnapshotApplied { count, epoch: upd.epoch }
    }

    fn apply_incremental(&mut self, upd: BalanceUpdate) -> BalanceEvent {
        let global_changed = upd.global_changed;
        if global_changed {
            self.global = GlobalBalance {
                btc_balance_total: upd.btc_balance_total,
                btc_balance_locked: upd.btc_balance_locked,
                btc_balance_full: upd.btc_balance_full,
                special_coin_balance: upd.special_coin_balance,
            };
        }
        let count = upd.items.len();
        for it in upd.items {
            self.by_market.insert(it.market_name.clone(), it);
        }
        self.last_epoch = upd.epoch;
        self.epoch_set = true;
        BalanceEvent::IncrementalApplied { count, epoch: upd.epoch, global_changed }
    }

    pub fn get(&self, market_name: &str) -> Option<&BalanceItem> {
        self.by_market.get(market_name)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &BalanceItem)> {
        self.by_market.iter()
    }

    pub fn len(&self) -> usize {
        self.by_market.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_market.is_empty()
    }

    pub fn clear(&mut self) {
        self.by_market.clear();
        self.global = GlobalBalance::default();
        self.last_epoch = 0;
        self.epoch_set = false;
    }
}

/// Wrap-safe epoch comparison: `MoonProtoFunc.pas:188-203 EpochIsOK`.
/// Returns true если new — действительно новое значение (не дубликат и не stale).
fn epoch_is_ok(last: u16, new: u16) -> bool {
    if last == new {
        return false; // duplicate
    }
    last.wrapping_sub(new) > 100
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_item(name: &str, init_bal: f64) -> BalanceItem {
        BalanceItem {
            market_name: name.to_string(),
            balance_hash: 0,
            initial_balance: init_bal,
            leverage_x: 1,
            ..Default::default()
        }
    }

    fn upd(cmd_id: u8, epoch: u16, items: Vec<BalanceItem>) -> BalanceUpdate {
        BalanceUpdate {
            cmd_id,
            epoch,
            global_changed: false,
            btc_balance_total: 1.0,
            btc_balance_locked: 0.5,
            btc_balance_full: 0.5,
            special_coin_balance: 0.0,
            items,
        }
    }

    #[test]
    fn full_snapshot_resets_missing_markets() {
        let mut s = BalancesState::new();
        s.apply(upd(3, 1, vec![make_item("BTCUSDT", 100.0), make_item("ETHUSDT", 50.0)]));
        assert_eq!(s.len(), 2);
        // Новый snapshot — только BTC. ETH должен пропасть.
        s.apply(upd(3, 2, vec![make_item("BTCUSDT", 200.0)]));
        assert_eq!(s.len(), 1);
        assert!(s.get("BTCUSDT").is_some());
        assert!(s.get("ETHUSDT").is_none());
    }

    #[test]
    fn incremental_merges() {
        let mut s = BalancesState::new();
        s.apply(upd(3, 1, vec![make_item("BTCUSDT", 100.0)]));
        // Incremental добавляет ETH без удаления BTC.
        s.apply(upd(4, 2, vec![make_item("ETHUSDT", 50.0)]));
        assert_eq!(s.len(), 2);
        assert_eq!(s.get("BTCUSDT").unwrap().initial_balance, 100.0);
        assert_eq!(s.get("ETHUSDT").unwrap().initial_balance, 50.0);
    }

    #[test]
    fn legacy_snapshot_does_not_reset() {
        let mut s = BalancesState::new();
        s.apply(upd(3, 1, vec![make_item("BTCUSDT", 100.0), make_item("ETHUSDT", 50.0)]));
        // cmd_id=2 = legacy — не сбрасывает отсутствующие.
        s.apply(upd(2, 2, vec![make_item("BTCUSDT", 200.0)]));
        assert_eq!(s.len(), 2);
        assert_eq!(s.get("BTCUSDT").unwrap().initial_balance, 200.0);
        assert_eq!(s.get("ETHUSDT").unwrap().initial_balance, 50.0);
    }

    #[test]
    fn stale_epoch_rejected() {
        let mut s = BalancesState::new();
        s.apply(upd(3, 50, vec![]));
        let ev = s.apply(upd(3, 45, vec![]));
        assert!(matches!(ev, BalanceEvent::EpochStale { .. }));
        assert_eq!(s.last_epoch, 50);
    }

    #[test]
    fn epoch_wrap_accepted() {
        let mut s = BalancesState::new();
        s.apply(upd(3, 65500, vec![]));
        // 65500 → 100: backDist = 65500-100 = 65400 > 100 → accept.
        let ev = s.apply(upd(3, 100, vec![]));
        assert!(matches!(ev, BalanceEvent::SnapshotApplied { .. }));
    }

    #[test]
    fn incremental_global_gated() {
        let mut s = BalancesState::new();
        // First snapshot устанавливает globals.
        s.apply(upd(3, 1, vec![]));
        let initial_btc = s.global.btc_balance_total;

        // Incremental с global_changed=false — globals остаются прежними.
        let mut u = upd(4, 2, vec![]);
        u.btc_balance_total = 999.0; // не применится
        u.global_changed = false;
        s.apply(u);
        assert_eq!(s.global.btc_balance_total, initial_btc);

        // Incremental с global_changed=true — применяется.
        let mut u = upd(4, 3, vec![]);
        u.btc_balance_total = 999.0;
        u.global_changed = true;
        s.apply(u);
        assert_eq!(s.global.btc_balance_total, 999.0);
    }
}
