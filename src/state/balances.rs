//! Balance read model maintained by the active `MoonClient` runtime.
//!
//! Source parity: `MoonProtoEngine.pas:1210-1340 ProcessBalanceCommand +
//! OnBalanceSnapshot + OnBalanceIncrement + ApplyBalanceItem`.
//!
//! Public applications normally read this state through `MoonClient` snapshots
//! and request refreshes with the high-level Active Lib methods. Low-level
//! packet parsing happens in `commands::balance`; this module applies already
//! decoded full snapshots and incremental updates to the local model.
//!
//! Incremental epoch protection is per market, matching Delphi
//! `m.LastBalanceEpoch`. Full snapshots are authoritative for known markets and
//! reset missing rows to default values without a global epoch gate.

use crate::commands::balance::{BalanceItem, BalanceUpdate};
use crate::state::eps::EpsProfile;
use std::collections::HashMap;
use std::sync::Arc;

/// Global account balance totals in BTC-equivalent units.
#[derive(Debug, Clone, Default)]
pub struct GlobalBalance {
    /// Available BTC-equivalent balance.
    pub btc_balance_total: f64,
    /// Locked BTC-equivalent balance.
    pub btc_balance_locked: f64,
    /// Full BTC-equivalent balance including unrealized PnL.
    pub btc_balance_full: f64,
    /// Special-coin balance (USDT for futures, BUSD/USDC in MA mode, etc.).
    pub special_coin_balance: f64,
    /// Delphi `TMarkets.FTotalPNL`: sum of per-market `total_profit` for
    /// `TMarket.IsBTCMarket` markets only.
    pub total_pnl: f64,
}

/// Client balance sync state published through active-session snapshots.
///
/// Snapshot vs incremental semantics:
/// - `cmd_id=2` (plain `TBalanceCommand`) is recognized but not applied, like Delphi.
/// - `cmd_id=3` (full snapshot) updates received markets and resets missing rows.
/// - `cmd_id=4` (incremental) updates changed rows and optionally globals.
#[derive(Debug, Clone, Default)]
pub struct BalancesState {
    /// Global totals (BTC, special coin, locked).
    pub global: GlobalBalance,
    /// Per-market balance rows keyed by `market_name`, for example `"BTCUSDT"`.
    by_market: HashMap<String, Arc<BalanceItem>>,
    /// Last applied epoch for any accepted balance packet.
    ///
    /// This field is diagnostic. Stale incremental rows are filtered through
    /// `last_epoch_by_market`, matching Delphi `m.LastBalanceEpoch`.
    pub last_epoch: u16,
    /// Last applied epoch by market name.
    last_epoch_by_market: HashMap<String, u16>,
    eps_profile: EpsProfile,
}

#[derive(Debug, Clone)]
pub enum BalanceEvent {
    /// Full snapshot applied: N markets received rows, missing rows were reset.
    SnapshotApplied { count: usize, epoch: u16 },
    /// Incremental update applied.
    IncrementalApplied {
        count: usize,
        epoch: u16,
        global_changed: bool,
    },
    /// Command was recognized, but Delphi does not apply it to balance state.
    Ignored { cmd_id: u8, epoch: u16 },
    /// Epoch check rejected the packet as stale.
    EpochStale { incoming: u16, last: u16 },
}

impl BalancesState {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn set_eps_profile(&mut self, eps_profile: EpsProfile) {
        self.eps_profile = eps_profile;
    }

    fn preserve_max_value(
        mut item: BalanceItem,
        previous_max_value: Option<f64>,
        eps: f64,
    ) -> BalanceItem {
        if item.max_value.partial_cmp(&eps) != Some(std::cmp::Ordering::Greater) {
            item.max_value = previous_max_value.unwrap_or(0.0);
        }
        item
    }

    fn prepare_item_for_apply(&self, item: BalanceItem) -> BalanceItem {
        let previous_max_value = self
            .by_market
            .get(&item.market_name)
            .map(|prev| prev.max_value);
        Self::preserve_max_value(item, previous_max_value, self.eps_profile.eps)
    }

    fn insert_balance_mark_epoch(&mut self, item: BalanceItem, epoch: u16) -> bool {
        let item = self.prepare_item_for_apply(item);
        let market_name = item.market_name.clone();
        self.by_market.insert(market_name.clone(), Arc::new(item));
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

    /// Apply one decoded `BalanceUpdate`.
    ///
    /// Incremental epoch protection is byte-equivalent to Delphi
    /// `MoonProtoFunc.pas:188-203 EpochIsOK` and is applied per market.
    pub fn apply(&mut self, upd: BalanceUpdate) -> BalanceEvent {
        self.apply_filtered(upd, |_| true)
    }

    /// Apply a balance update only for markets known to the current market list.
    ///
    /// This is the active-library path matching Delphi
    /// `Markets.MarketByNameFast(item.MarketName)`: unknown markets do not
    /// create independent balance rows.
    pub(crate) fn apply_filtered<F>(
        &mut self,
        upd: BalanceUpdate,
        is_known_market: F,
    ) -> BalanceEvent
    where
        F: Fn(&str) -> bool,
    {
        self.apply_internal(upd, is_known_market, None, |_| false)
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
        is_btc_market: impl Fn(&str) -> bool,
    ) -> BalanceEvent {
        self.apply_internal(
            upd,
            |name| known_market_names.contains_key(name),
            Some(known_market_names),
            is_btc_market,
        )
    }

    fn apply_internal<F, B>(
        &mut self,
        upd: BalanceUpdate,
        is_known_market: F,
        full_known_markets: Option<&HashMap<String, usize>>,
        is_btc_market: B,
    ) -> BalanceEvent
    where
        F: Fn(&str) -> bool,
        B: Fn(&str) -> bool,
    {
        let ev = match upd.cmd_id {
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
        };
        if matches!(
            ev,
            BalanceEvent::SnapshotApplied { .. } | BalanceEvent::IncrementalApplied { .. }
        ) {
            self.recalc_total_pnl(is_btc_market);
        }
        ev
    }

    /// Full snapshot: markets missing from `Items` receive default values.
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
            total_pnl: self.global.total_pnl,
        };

        // Replace state — маркеты НЕ в snapshot сбрасываются в default, but
        // Delphi does not touch BalanceHash/bnMaxValue/LastBalanceEpoch in that
        // reset branch.
        let previous_map = std::mem::take(&mut self.by_market);
        let mut new_map: HashMap<String, Arc<BalanceItem>> = HashMap::new();
        let mut count = 0;
        for it in upd.items {
            if !is_known_market(&it.market_name) {
                continue;
            }
            let previous_max_value = new_map
                .get(&it.market_name)
                .map(|prev| prev.max_value)
                .or_else(|| previous_map.get(&it.market_name).map(|prev| prev.max_value));
            let it = Self::preserve_max_value(it, previous_max_value, self.eps_profile.eps);
            self.last_epoch_by_market
                .insert(it.market_name.clone(), upd.epoch);
            new_map.insert(it.market_name.clone(), Arc::new(it));
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
                        Arc::new(Self::reset_missing_snapshot_item(previous.as_ref().clone())),
                    );
                } else {
                    new_map.insert(
                        market_name.clone(),
                        Arc::new(Self::default_missing_snapshot_item(market_name)),
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
                    Arc::new(Self::reset_missing_snapshot_item(previous.as_ref().clone())),
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
                total_pnl: self.global.total_pnl,
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
        self.by_market.get(market_name).map(AsRef::as_ref)
    }

    pub fn global(&self) -> &GlobalBalance {
        &self.global
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &BalanceItem)> {
        self.by_market
            .iter()
            .map(|(market_name, item)| (market_name, item.as_ref()))
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

    fn recalc_total_pnl(&mut self, is_btc_market: impl Fn(&str) -> bool) {
        self.global.total_pnl = self
            .by_market
            .values()
            .map(AsRef::as_ref)
            .filter(|item| is_btc_market(&item.market_name))
            .map(BalanceItem::total_profit)
            .sum();
    }
}

// `epoch_is_ok` теперь общий через `state::epoch::epoch_is_ok` (audit_rust_quality #1).
// Окно stale = 100 взято из Delphi `MoonProtoFunc.pas:188-203`.
use super::epoch::epoch_is_ok;

#[cfg(test)]
mod tests;
