//! Read-only dispatcher snapshot for application callbacks.

use super::*;
use crate::commands::strategy_serializer::StrategySnapshot;

/// Immutable read-model copy delivered to `run_with_dispatcher_state` callbacks.
///
/// The live [`EventDispatcher`] stays owned by the protocol loop. This snapshot
/// is cloned after dispatcher state is updated, then sent through the
/// application callback queue. User code can block or keep the snapshot without
/// blocking protocol ACK/retry/send progress.
#[derive(Debug, Clone)]
pub struct EventDispatcherSnapshot {
    orders: Orders,
    order_books: OrderBooks,
    trades: TradesState,
    balances: BalancesState,
    strats: StratsState,
    settings: SettingsState,
    markets: MarketsState,
    local_strategy_epoch: u64,
}

impl EventDispatcherSnapshot {
    /// Read-only order state, keyed by server order UID.
    pub fn orders(&self) -> &Orders {
        &self.orders
    }

    /// Read-only orderbook state.
    pub fn order_books(&self) -> &OrderBooks {
        &self.order_books
    }

    /// Read-only trades-stream state.
    pub fn trades(&self) -> &TradesState {
        &self.trades
    }

    /// Read-only balance state.
    pub fn balances(&self) -> &BalancesState {
        &self.balances
    }

    /// Read-only strategy state.
    pub fn strats(&self) -> &StratsState {
        &self.strats
    }

    /// Delphi `cfg.ServerStratEpoch` analogue used by local strategy snapshots.
    pub fn local_strategy_epoch(&self) -> u64 {
        self.local_strategy_epoch
    }

    /// Read one full decoded strategy snapshot from the active-library state.
    pub fn strategy_snapshot(&self, strategy_id: u64) -> Option<&StrategySnapshot> {
        self.strats.snapshot(strategy_id)
    }

    /// Iterate full decoded strategy snapshots in Delphi list order.
    pub fn strategy_snapshots(&self) -> impl Iterator<Item = &StrategySnapshot> {
        self.strats.snapshots()
    }

    /// Clone the current strategy snapshot list in Delphi list order.
    pub fn strategy_snapshot_vec(&self) -> Vec<StrategySnapshot> {
        self.strats.snapshot_vec()
    }

    /// Delphi `TStrategies.GetCheckedDelta` over the active-library strategy list.
    pub fn strategy_checked_delta(&self) -> Vec<crate::commands::strat::StratCheckedItem> {
        self.strats.checked_delta()
    }

    /// Read-only UI/settings state.
    pub fn settings(&self) -> &SettingsState {
        &self.settings
    }

    /// Read-only markets state.
    pub fn markets(&self) -> &MarketsState {
        &self.markets
    }
}

impl EventDispatcher {
    /// Copy the current read model for application callback delivery.
    ///
    /// This is a read-only snapshot: it intentionally excludes mutable callback
    /// hooks and the one-shot queued-event buffer from the live dispatcher.
    pub fn snapshot(&self) -> EventDispatcherSnapshot {
        EventDispatcherSnapshot {
            orders: self.orders.clone(),
            order_books: self.order_books.clone(),
            trades: self.trades.clone(),
            balances: self.balances.clone(),
            strats: self.strats.clone(),
            settings: self.settings.clone(),
            markets: self.markets.clone(),
            local_strategy_epoch: self.local_strategy_epoch,
        }
    }
}
