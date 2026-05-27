//! Read-only dispatcher snapshot for application callbacks.

use super::*;
use crate::commands::strategy_serializer::StrategySnapshot;
use crate::state::{OrderBookKind, OrderBookSnapshot, TopOfBook};

/// Immutable read-model copy published by `MoonClient` and by custom
/// low-level active runtimes.
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
    account: AccountState,
    balances: BalancesState,
    transfer_assets: TransferAssetsState,
    coin_card_candles: crate::state::CoinCardCandlesState,
    strats: StratsState,
    settings: SettingsState,
    markets: MarketsState,
    market_history: Option<MarketHistoryHandle>,
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

    /// Current applied orderbook for a market name.
    ///
    /// This is the UI-facing path: it resolves the current server market index
    /// through the maintained markets state and then reads the matching applied
    /// book. It returns `None` while market indexes are stale or the book has
    /// not arrived yet.
    pub fn order_book(&self, market_name: &str, kind: OrderBookKind) -> Option<&OrderBookSnapshot> {
        let market_index = self.markets.market_index_by_name(market_name)?;
        self.order_books.book(market_index, kind)
    }

    /// Best bid/ask from the current applied orderbook for a market name.
    pub fn top_of_book(&self, market_name: &str, kind: OrderBookKind) -> Option<TopOfBook> {
        self.order_book(market_name, kind)
            .map(OrderBookSnapshot::top)
    }

    /// Read-only trades-stream state.
    pub fn trades(&self) -> &TradesState {
        &self.trades
    }

    /// Read-only account-level state.
    pub fn account(&self) -> &AccountState {
        &self.account
    }

    /// Read-only balance state.
    pub fn balances(&self) -> &BalancesState {
        &self.balances
    }

    /// Read-only transferable asset lists by wallet kind.
    pub fn transfer_assets(&self) -> &TransferAssetsState {
        &self.transfer_assets
    }

    /// Demand-driven CoinCard candles by market/history kind.
    pub fn coin_card_candles(&self) -> &crate::state::CoinCardCandlesState {
        &self.coin_card_candles
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

    /// Retained history readers for one market, if trades storage is active.
    pub fn market_history_readers(&self, market_name: &str) -> Option<MarketHistoryReaders> {
        self.market_history.as_ref()?.readers(market_name)
    }

    /// Current rolling volume snapshot for one market, if retained storage is active.
    pub fn market_history_rolling_volumes(
        &self,
        market_name: &str,
        now_time: f64,
    ) -> Option<RollingTradeVolumeSnapshot> {
        self.market_history
            .as_ref()?
            .rolling_volumes(market_name, now_time)
    }

    /// Current rolling volume snapshot at a typed Delphi time.
    pub fn market_history_rolling_volumes_at(
        &self,
        market_name: &str,
        now_time: crate::DelphiTime,
    ) -> Option<RollingTradeVolumeSnapshot> {
        self.market_history_rolling_volumes(market_name, now_time.as_days())
    }

    /// Current rolling volume snapshot using the local system clock.
    pub fn market_history_rolling_volumes_now(
        &self,
        market_name: &str,
    ) -> Option<RollingTradeVolumeSnapshot> {
        self.market_history_rolling_volumes_at(market_name, crate::DelphiTime::now())
    }

    /// Current derived analytics snapshot for one market, if retained storage is active.
    pub fn market_history_derived_snapshot(
        &self,
        market_name: &str,
        now_time: f64,
    ) -> Option<MarketDerivedSnapshot> {
        self.market_history
            .as_ref()?
            .derived_snapshot(market_name, now_time)
    }

    /// Current derived analytics snapshot at a typed Delphi time.
    pub fn market_history_derived_snapshot_at(
        &self,
        market_name: &str,
        now_time: crate::DelphiTime,
    ) -> Option<MarketDerivedSnapshot> {
        self.market_history_derived_snapshot(market_name, now_time.as_days())
    }

    /// Current derived analytics snapshot using the local system clock.
    pub fn market_history_derived_snapshot_now(
        &self,
        market_name: &str,
    ) -> Option<MarketDerivedSnapshot> {
        self.market_history_derived_snapshot_at(market_name, crate::DelphiTime::now())
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
            account: self.account.clone(),
            balances: self.balances.clone(),
            transfer_assets: self.transfer_assets.clone(),
            coin_card_candles: self.coin_card_candles.clone(),
            strats: self.strats.clone(),
            settings: self.settings.clone(),
            markets: self.markets.clone(),
            market_history: self.market_history.clone(),
            local_strategy_epoch: self.local_strategy_epoch,
        }
    }
}
