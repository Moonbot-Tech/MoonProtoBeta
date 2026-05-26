//! Balance read model maintained by `EventDispatcher`.
//!
//! Source parity: `MoonProtoEngine.pas:1210-1340 ProcessBalanceCommand +
//! OnBalanceSnapshot + OnBalanceIncrement + ApplyBalanceItem`.
//!
//! Public applications normally read this state through
//! `EventDispatcher::balances()` or use `Client::request_balance_snapshot`.
//! Low-level packet parsing happens in `commands::balance`; this module applies
//! already-decoded full snapshots and incremental updates to the local model.
//!
//! Incremental epoch protection is per market, matching Delphi
//! `m.LastBalanceEpoch`. Full snapshots are authoritative for known markets and
//! reset missing rows to default values without a global epoch gate.

use crate::commands::balance::{BalanceItem, BalanceUpdate};
use std::collections::HashMap;

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
/// получении full/incremental balance updates от сервера. Используется в
/// [`crate::events::EventDispatcher`].
///
/// **Семантика snapshot vs incremental**:
/// - `cmd_id=2` (exact `TBalanceCommand`): не применяется к state, как в Delphi.
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
    /// Это поле оставлено для диагностики. Для отбрасывания stale incremental
    /// items используется `last_epoch_by_market`, как в Delphi.
    pub last_epoch: u16,
    /// Последний применённый epoch по market_name (Delphi `m.LastBalanceEpoch`).
    last_epoch_by_market: HashMap<String, u16>,
}

#[derive(Debug, Clone)]
pub enum BalanceEvent {
    /// Применён full snapshot: N маркетов получили данные, остальные сброшены в default.
    SnapshotApplied { count: usize, epoch: u16 },
    /// Применён incremental update: N маркетов изменилось, globals обновлены если global_changed=true.
    IncrementalApplied {
        count: usize,
        epoch: u16,
        global_changed: bool,
    },
    /// Команда распознана, но Delphi-клиент не применяет её к balance state.
    Ignored { cmd_id: u8, epoch: u16 },
    /// Epoch не прошёл (старее last_epoch wrap-safe).
    EpochStale { incoming: u16, last: u16 },
}

impl BalancesState {
    pub fn new() -> Self {
        Self::default()
    }

    fn preserve_max_value(mut item: BalanceItem, previous_max_value: Option<f64>) -> BalanceItem {
        if item.max_value.partial_cmp(&BALANCE_EPS) != Some(std::cmp::Ordering::Greater) {
            item.max_value = previous_max_value.unwrap_or(0.0);
        }
        item
    }

    fn prepare_item_for_apply(&self, item: BalanceItem) -> BalanceItem {
        let previous_max_value = self
            .by_market
            .get(&item.market_name)
            .map(|prev| prev.max_value);
        Self::preserve_max_value(item, previous_max_value)
    }

    fn insert_balance_mark_epoch(&mut self, item: BalanceItem, epoch: u16) -> bool {
        let item = self.prepare_item_for_apply(item);
        let market_name = item.market_name.clone();
        self.by_market.insert(market_name.clone(), item);
        self.last_epoch_by_market.insert(market_name, epoch);
        true
    }

    fn reset_missing_snapshot_item(previous: BalanceItem) -> BalanceItem {
        BalanceItem {
            market_name: previous.market_name,
            balance_hash: previous.balance_hash,
            max_value: previous.max_value,
            leverage_x: 1,
            ..Default::default()
        }
    }

    fn default_missing_snapshot_item(market_name: &str) -> BalanceItem {
        BalanceItem {
            market_name: market_name.to_string(),
            leverage_x: 1,
            ..Default::default()
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
        self.apply_filtered(upd, |_| true)
    }

    /// Применить balance update только для market names, известных текущему списку
    /// Markets. Это active-library путь, соответствующий Delphi
    /// `Markets.MarketByNameFast(item.MarketName)`: unknown market не создаёт
    /// отдельный balance entry.
    pub(crate) fn apply_filtered<F>(
        &mut self,
        upd: BalanceUpdate,
        is_known_market: F,
    ) -> BalanceEvent
    where
        F: Fn(&str) -> bool,
    {
        self.apply_internal(upd, is_known_market, None)
    }

    /// Active-library balance apply with the full known-market universe.
    ///
    /// Delphi full snapshot resets every `TMarket` not present in the snapshot,
    /// including markets that had no previous balance item. A predicate can
    /// filter unknown incoming items, but only the full known list can create
    /// those missing default rows.
    pub(crate) fn apply_with_known_markets(
        &mut self,
        upd: BalanceUpdate,
        known_market_names: &HashMap<String, usize>,
    ) -> BalanceEvent {
        self.apply_internal(
            upd,
            |name| known_market_names.contains_key(name),
            Some(known_market_names),
        )
    }

    fn apply_internal<F>(
        &mut self,
        upd: BalanceUpdate,
        is_known_market: F,
        full_known_markets: Option<&HashMap<String, usize>>,
    ) -> BalanceEvent
    where
        F: Fn(&str) -> bool,
    {
        match upd.cmd_id {
            2 => BalanceEvent::Ignored {
                cmd_id: upd.cmd_id,
                epoch: upd.epoch,
            },
            3 => self.apply_full_snapshot(upd, &is_known_market, full_known_markets),
            4 => self.apply_incremental(upd, &is_known_market),
            _ => BalanceEvent::Ignored {
                cmd_id: upd.cmd_id,
                epoch: upd.epoch,
            },
        }
    }

    /// Full snapshot: маркеты не в Items получают default (Delphi:1253-1275).
    fn apply_full_snapshot<F>(
        &mut self,
        upd: BalanceUpdate,
        is_known_market: &F,
        full_known_markets: Option<&HashMap<String, usize>>,
    ) -> BalanceEvent
    where
        F: Fn(&str) -> bool,
    {
        self.global = GlobalBalance {
            btc_balance_total: upd.btc_balance_total,
            btc_balance_locked: upd.btc_balance_locked,
            btc_balance_full: upd.btc_balance_full,
            special_coin_balance: upd.special_coin_balance,
        };

        // Replace state — маркеты НЕ в snapshot сбрасываются в default, but
        // Delphi does not touch BalanceHash/bnMaxValue/LastBalanceEpoch in that
        // reset branch.
        let previous_map = std::mem::take(&mut self.by_market);
        let mut new_map: HashMap<String, BalanceItem> = HashMap::new();
        let mut count = 0;
        for it in upd.items {
            if !is_known_market(&it.market_name) {
                continue;
            }
            let previous_max_value = new_map
                .get(&it.market_name)
                .map(|prev| prev.max_value)
                .or_else(|| previous_map.get(&it.market_name).map(|prev| prev.max_value));
            let it = Self::preserve_max_value(it, previous_max_value);
            self.last_epoch_by_market
                .insert(it.market_name.clone(), upd.epoch);
            new_map.insert(it.market_name.clone(), it);
            count += 1;
        }

        if let Some(known) = full_known_markets {
            for market_name in known.keys() {
                if new_map.contains_key(market_name) {
                    continue;
                }
                if let Some(previous) = previous_map.get(market_name) {
                    new_map.insert(
                        market_name.clone(),
                        Self::reset_missing_snapshot_item(previous.clone()),
                    );
                } else {
                    new_map.insert(
                        market_name.clone(),
                        Self::default_missing_snapshot_item(market_name),
                    );
                    self.last_epoch_by_market
                        .entry(market_name.clone())
                        .or_insert(0);
                }
            }
        } else {
            for (market_name, previous) in &previous_map {
                if new_map.contains_key(market_name) || !is_known_market(market_name) {
                    continue;
                }
                new_map.insert(
                    market_name.clone(),
                    Self::reset_missing_snapshot_item(previous.clone()),
                );
            }
        }
        self.by_market = new_map;
        self.last_epoch = upd.epoch;
        BalanceEvent::SnapshotApplied {
            count,
            epoch: upd.epoch,
        }
    }

    fn apply_incremental<F>(&mut self, upd: BalanceUpdate, is_known_market: &F) -> BalanceEvent
    where
        F: Fn(&str) -> bool,
    {
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
            if !is_known_market(&it.market_name) {
                continue;
            }
            if self.apply_incremental_item(it, upd.epoch) {
                count += 1;
            }
        }
        if global_changed || count > 0 {
            self.last_epoch = upd.epoch;
        }
        BalanceEvent::IncrementalApplied {
            count,
            epoch: upd.epoch,
            global_changed,
        }
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
mod tests;
