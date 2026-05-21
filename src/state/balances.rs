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
//!
//! В Delphi epoch для incremental проверяется на уровне отдельного рынка
//! (`m.LastBalanceEpoch`), а full snapshot не проходит через общий epoch-gate.

use std::collections::HashMap;
use crate::commands::balance::{BalanceItem, BalanceUpdate};

const BALANCE_EPS: f64 = 0.00000001;

/// Глобальные суммарные балансы аккаунта (в BTC equivalent).
#[derive(Debug, Clone, Default)]
pub struct GlobalBalance {
    /// Доступный баланс в BTC (свободный + locked, минус долги).
    pub btc_balance_total: f64,
    /// Заблокированная часть баланса в BTC (в открытых ордерах / залогах).
    pub btc_balance_locked: f64,
    /// Полный баланс включая нереализованную прибыль/убыток в BTC equivalent.
    pub btc_balance_full: f64,
    /// Баланс specialCoin (USDT для futures, BUSD/USDC при MA mode и т.д.).
    pub special_coin_balance: f64,
}

/// Sync state балансов клиента. Обновляется через `apply(BalanceUpdate)` при
/// получении `MPC_Balance` пакетов от сервера. Используется в [`crate::events::EventDispatcher`].
///
/// **Семантика snapshot vs incremental**:
/// - `cmd_id=2` (legacy snapshot): обновляются полученные маркеты, остальные не трогаются.
/// - `cmd_id=3` (full snapshot): обновляются полученные, **остальные сбрасываются**.
/// - `cmd_id=4` (incremental): обновление дельты + опциональный обнов globals.
#[derive(Debug, Clone, Default)]
pub struct BalancesState {
    /// Глобальные суммы (BTC, special coin, locked).
    pub global: GlobalBalance,
    /// Per-маркет балансы: ключ = `market_name` (e.g. "BTCUSDT"), значение = строка `BalanceItem`.
    pub by_market: HashMap<String, BalanceItem>,
    /// Последний применённый epoch любого accepted balance-пакета.
    ///
    /// Это поле оставлено для диагностики/back-compat. Для отбрасывания stale
    /// incremental items используется `last_epoch_by_market`, как в Delphi.
    pub last_epoch: u16,
    /// Последний применённый epoch по market_name (Delphi `m.LastBalanceEpoch`).
    last_epoch_by_market: HashMap<String, u16>,
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

/// DoS guard: верхний лимит количества market_name записей в `by_market`. Реальный
/// бот видит сотни-тысячи маркетов; 20_000 — щедрый потолок. Враждебный сервер
/// мог бы accumulate'ить unique market_names через incremental updates (cmd_id=4)
/// → unbounded HashMap. См. `audit_robustness` C-3.
///
/// Cap применяется в `apply_legacy_snapshot` (cmd_id=2) и `apply_incremental`
/// (cmd_id=4). `apply_full_snapshot` (cmd_id=3) не нуждается в cap — full snapshot
/// заменяет map целиком (любой DoS-flood сервера ограничен размером **одного**
/// snapshot'а).
pub const MAX_BALANCE_MARKETS: usize = 20_000;

impl BalancesState {
    pub fn new() -> Self {
        Self::default()
    }

    /// DoS guard insert: вставляет `BalanceItem` если cap не достигнут.
    /// Возвращает true если вставлено, false если cap reached (skip + warn).
    /// Existing key — всегда update (cap не нужен).
    fn try_insert_balance(&mut self, item: BalanceItem) -> bool {
        if !self.by_market.contains_key(&item.market_name)
            && self.by_market.len() >= MAX_BALANCE_MARKETS
        {
            log::warn!(target: "moonproto::balances",
                "BalancesState.by_market at MAX ({MAX_BALANCE_MARKETS}) — rejecting market='{}'",
                item.market_name);
            return false;
        }
        self.by_market.insert(item.market_name.clone(), item);
        true
    }

    fn preserve_max_value(mut item: BalanceItem, previous_max_value: Option<f64>) -> BalanceItem {
        if !(item.max_value > BALANCE_EPS) {
            item.max_value = previous_max_value.unwrap_or(0.0);
        }
        item
    }

    fn prepare_item_for_apply(&self, item: BalanceItem) -> BalanceItem {
        let previous_max_value = self.by_market.get(&item.market_name).map(|prev| prev.max_value);
        Self::preserve_max_value(item, previous_max_value)
    }

    fn insert_balance_mark_epoch(&mut self, item: BalanceItem, epoch: u16) -> bool {
        let item = self.prepare_item_for_apply(item);
        let market_name = item.market_name.clone();
        if self.try_insert_balance(item) {
            self.last_epoch_by_market.insert(market_name, epoch);
            true
        } else {
            false
        }
    }

    fn apply_incremental_item(&mut self, item: BalanceItem, epoch: u16) -> bool {
        if let Some(last) = self.last_epoch_by_market.get(&item.market_name).copied() {
            if !epoch_is_ok(last, epoch) {
                return false;
            }
        }
        self.insert_balance_mark_epoch(item, epoch)
    }

    /// Применить распарсенный `BalanceUpdate`.
    /// Epoch protection для incremental: `EpochIsOK` byte-exact с
    /// `MoonProtoFunc.pas:188-203`, применяется per-market как в Delphi.
    pub fn apply(&mut self, upd: BalanceUpdate) -> BalanceEvent {
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
        let mut count = 0;
        for it in upd.items {
            if self.insert_balance_mark_epoch(it, upd.epoch) { count += 1; }
        }
        self.last_epoch = upd.epoch;
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
            let previous_max_value = new_map.get(&it.market_name)
                .map(|prev| prev.max_value)
                .or_else(|| self.by_market.get(&it.market_name).map(|prev| prev.max_value));
            let it = Self::preserve_max_value(it, previous_max_value);
            self.last_epoch_by_market.insert(it.market_name.clone(), upd.epoch);
            new_map.insert(it.market_name.clone(), it);
        }
        self.by_market = new_map;
        self.last_epoch = upd.epoch;
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
        let mut count = 0;
        for it in upd.items {
            if self.apply_incremental_item(it, upd.epoch) { count += 1; }
        }
        if global_changed || count > 0 {
            self.last_epoch = upd.epoch;
        }
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
        self.last_epoch_by_market.clear();
        self.global = GlobalBalance::default();
        self.last_epoch = 0;
    }
}

// `epoch_is_ok` теперь общий через `state::epoch::epoch_is_ok` (audit_rust_quality #1).
// Окно stale = 100 взято из Delphi `MoonProtoFunc.pas:188-203`.
use super::epoch::epoch_is_ok;

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
    fn full_snapshot_does_not_use_global_epoch_gate() {
        let mut s = BalancesState::new();
        s.apply(upd(3, 50, vec![make_item("BTCUSDT", 100.0)]));
        let ev = s.apply(upd(3, 45, vec![make_item("BTCUSDT", 200.0)]));
        assert!(matches!(ev, BalanceEvent::SnapshotApplied { .. }));
        assert_eq!(s.last_epoch, 45);
        assert_eq!(s.get("BTCUSDT").unwrap().initial_balance, 200.0);
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

    #[test]
    fn incremental_epoch_is_checked_per_market() {
        let mut s = BalancesState::new();
        s.apply(upd(4, 10, vec![make_item("BTCUSDT", 100.0)]));
        s.apply(upd(4, 20, vec![make_item("ETHUSDT", 200.0)]));

        let ev = s.apply(upd(
            4,
            15,
            vec![make_item("BTCUSDT", 150.0), make_item("ETHUSDT", 250.0)],
        ));

        assert!(matches!(ev, BalanceEvent::IncrementalApplied { count: 1, .. }));
        assert_eq!(s.get("BTCUSDT").unwrap().initial_balance, 150.0);
        assert_eq!(s.get("ETHUSDT").unwrap().initial_balance, 200.0);
    }

    #[test]
    fn incremental_for_new_market_not_rejected_by_other_market_epoch() {
        let mut s = BalancesState::new();
        s.apply(upd(4, 100, vec![make_item("BTCUSDT", 100.0)]));

        let ev = s.apply(upd(4, 90, vec![make_item("ETHUSDT", 90.0)]));

        assert!(matches!(ev, BalanceEvent::IncrementalApplied { count: 1, .. }));
        assert_eq!(s.get("ETHUSDT").unwrap().initial_balance, 90.0);
    }

    #[test]
    fn max_value_zero_preserves_previous_like_delphi() {
        let mut s = BalancesState::new();
        let mut first = make_item("BTCUSDT", 100.0);
        first.max_value = 500.0;
        s.apply(upd(3, 1, vec![first]));

        let second = make_item("BTCUSDT", 200.0);
        s.apply(upd(4, 2, vec![second]));

        let item = s.get("BTCUSDT").unwrap();
        assert_eq!(item.initial_balance, 200.0);
        assert_eq!(item.max_value, 500.0);
    }

    #[test]
    fn max_value_positive_updates_previous() {
        let mut s = BalancesState::new();
        let mut first = make_item("BTCUSDT", 100.0);
        first.max_value = 500.0;
        s.apply(upd(3, 1, vec![first]));

        let mut second = make_item("BTCUSDT", 200.0);
        second.max_value = 600.0;
        s.apply(upd(4, 2, vec![second]));

        assert_eq!(s.get("BTCUSDT").unwrap().max_value, 600.0);
    }
}
