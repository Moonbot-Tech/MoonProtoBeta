//! Internal high-level runtime command protocol.

use super::*;

pub(super) enum RuntimeCommand {
    Stop,
    SubscribeOrderBook(String),
    SubscribeOrderBooks(Vec<String>),
    UnsubscribeOrderBook(String),
    UnsubscribeOrderBooks(Vec<String>),
    UnsubscribeAllOrderBooks,
    SubscribeAllTrades(bool),
    SubscribeTradesFor {
        want_mm: bool,
        markets: Vec<String>,
    },
    UnsubscribeAllTrades,
    SubscribeCandles {
        markets: Vec<String>,
        kind: crate::commands::candles::DeepHistoryKind,
    },
    UnsubscribeCandles(Vec<String>),
    SetDeltasByTrades(bool),
    BalanceRefresh,
    AccountHedgeModeRefresh,
    AccountApiExpirationRefresh,
    OrderSnapshotRefresh,
    TransferAssetsRefresh,
    TransferAssetsRefreshKind(crate::state::ExchangeKind),
    SetExcludeBlacklistedMarketsFromExchangeDelta(bool),
    EngineAction {
        kind: crate::events::EngineActionKind,
        ticket: super::EngineActionTicket,
        payload: Vec<u8>,
    },
    CoinCardCandles {
        ticket: super::CoinCardCandlesTicket,
        payload: Vec<u8>,
    },
    Ui(UiRuntimeCommand),
    Strat(StratRuntimeCommand),
    StrategySnapshotBatch(Vec<crate::commands::strategy_serializer::StrategySnapshot>),
    StrategySetChecked {
        strategy_id: u64,
        checked: bool,
    },
    StrategySendCheckedDelta,
    StrategyStartStop {
        is_start: bool,
    },
    ReportSchemaRefresh,
    ReportSync {
        ticket: crate::state::ReportSyncTicket,
        request: crate::state::ReportSyncRequest,
    },
    ReportPageApplied(crate::state::ReportSyncPage),
    ReportCheckOpenRows(Arc<[i64]>),
    ReportSetRowsDeleted(Arc<[crate::state::ReportRowsDeleted]>),
    #[cfg(any(test, feature = "diagnostics"))]
    DebugOutgoingBlackhole(bool),
    #[cfg(any(test, feature = "diagnostics"))]
    DebugResetErrEmuDiagnostics,
    #[cfg(any(test, feature = "diagnostics"))]
    DiagFillMarketHistoryToCapacity {
        market_name: String,
        now_time: crate::MoonTime,
        span_ms: i64,
        reply: mpsc::SyncSender<bool>,
    },
    OrderAction(RuntimeCommandKind),
    TradeAction(RuntimeTradeCommandKind),
}

pub(super) enum UiRuntimeCommand {
    SettingsRequest,
    MmSubscribe(bool),
    SendSettings(crate::commands::ui::ClientSettingsCommand),
    UpdateVersion {
        version_name: String,
        is_release: bool,
    },
    SwitchDex(String),
    SwitchSpot(crate::commands::ui::SpotMarketKind),
    LevManage(crate::commands::ui::LevManage),
    EmuTrades {
        market_index: u16,
        base_time: f64,
        points: Vec<crate::commands::ui::EmuTradePoint>,
    },
    TriggerManage {
        action: u8,
        all_markets: bool,
        markets: Vec<u16>,
        keys: Vec<u16>,
    },
    ResetProfit(u8),
    ArbActivateNotify(f64),
    AlertObject(crate::commands::ui::AlertObjectCommand),
    AlertSnapshotRequest,
    ChartTextState(crate::commands::ui::ChartTextStateCommand),
    OrdersHistoryRequest(String),
    RestartNow,
    KernelLicenseStateRequest,
    AutoDetect(bool),
}

pub(super) enum StratRuntimeCommand {
    SellPriceUpdate {
        strategy_id: u64,
        sell_price: f64,
    },
    Delete {
        strategy_id: u64,
        folder_path: String,
    },
}

pub(super) enum RuntimeCommandKind {
    MoveOrder {
        uid: u64,
        new_price: f64,
    },
    CancelOrder {
        uid: u64,
    },
    UpdateStops {
        uid: u64,
        stops: crate::commands::trade::StopSettings,
    },
    UpdateVStop {
        uid: u64,
        params: super::VStopParams,
    },
    SetImmune {
        items: Vec<crate::commands::trade::ImmuneItem>,
    },
    TurnOrderPanicSell {
        uid: u64,
        turn_on: bool,
    },
    RequestOrderStatus {
        uid: u64,
    },
    SwitchPanicSellByMarket {
        market_name: String,
        turn_on: bool,
    },
}

pub(super) enum RuntimeTradeCommandKind {
    NewOrder {
        params: NewOrderParams,
        request_uid: u64,
    },
    JoinOrders {
        market_name: String,
        side: OrderSide,
    },
    SplitOrder(SplitOrderParams),
    MoveAllSells {
        market_name: String,
        params: crate::commands::trade::MoveAllSellsParams,
    },
    MoveAllBuys {
        market_name: String,
        params: crate::commands::trade::MoveAllBuysParams,
    },
    ClosePosition(ClosePositionParams),
    LimitClosePosition {
        market_name: String,
        side: OrderSide,
    },
    SplitPosition {
        market_name: String,
        side: OrderSide,
    },
    SellOrder(SellOrderParams),
    MarketSplitPosition {
        market_name: String,
        side: OrderSide,
    },
    Penalty {
        market_name: String,
    },
    PanicSellAll,
}

#[cfg(any(test, feature = "diagnostics"))]
impl RuntimeCommand {
    pub(super) fn profile_source(&self) -> (u8, usize) {
        match self {
            Self::Stop => (0, 0),
            Self::SubscribeOrderBook(_) => (1, 1),
            Self::SubscribeOrderBooks(names) => (2, names.len()),
            Self::UnsubscribeOrderBook(_) => (3, 1),
            Self::UnsubscribeOrderBooks(names) => (4, names.len()),
            Self::UnsubscribeAllOrderBooks => (5, 0),
            Self::SubscribeAllTrades(_) => (6, 0),
            Self::SubscribeTradesFor { markets, .. } => (7, markets.len()),
            Self::UnsubscribeAllTrades => (8, 0),
            Self::SubscribeCandles { markets, .. } => (9, markets.len()),
            Self::UnsubscribeCandles(markets) => (10, markets.len()),
            Self::SetDeltasByTrades(_) => (11, 0),
            Self::BalanceRefresh => (12, 0),
            Self::AccountHedgeModeRefresh => (13, 0),
            Self::AccountApiExpirationRefresh => (14, 0),
            Self::OrderSnapshotRefresh => (15, 0),
            Self::TransferAssetsRefresh => (16, 0),
            Self::TransferAssetsRefreshKind(_) => (17, 1),
            Self::SetExcludeBlacklistedMarketsFromExchangeDelta(_) => (18, 0),
            Self::EngineAction { payload, .. } => (19, payload.len()),
            Self::CoinCardCandles { payload, .. } => (20, payload.len()),
            Self::Ui(cmd) => cmd.profile_source(),
            Self::Strat(cmd) => cmd.profile_source(),
            Self::StrategySnapshotBatch(strategies) => (50, strategies.len()),
            Self::StrategySetChecked { .. } => (51, 1),
            Self::StrategySendCheckedDelta => (52, 0),
            Self::StrategyStartStop { .. } => (53, 0),
            Self::ReportSchemaRefresh => (54, 0),
            Self::ReportSync { .. } => (55, 1),
            Self::ReportPageApplied(_) => (59, 1),
            Self::ReportCheckOpenRows(rec_ids) => (60, rec_ids.len()),
            Self::ReportSetRowsDeleted(batches) => (61, batches.len()),
            #[cfg(any(test, feature = "diagnostics"))]
            Self::DebugOutgoingBlackhole(_) => (56, 0),
            #[cfg(any(test, feature = "diagnostics"))]
            Self::DebugResetErrEmuDiagnostics => (57, 0),
            #[cfg(any(test, feature = "diagnostics"))]
            Self::DiagFillMarketHistoryToCapacity { .. } => (58, 1),
            Self::OrderAction(kind) => kind.profile_source(),
            Self::TradeAction(kind) => kind.profile_source(),
        }
    }
}

#[cfg(any(test, feature = "diagnostics"))]
impl UiRuntimeCommand {
    fn profile_source(&self) -> (u8, usize) {
        match self {
            Self::SettingsRequest => (20, 0),
            Self::MmSubscribe(_) => (21, 0),
            Self::SendSettings(_) => (22, 1),
            Self::UpdateVersion { .. } => (23, 0),
            Self::SwitchDex(_) => (24, 1),
            Self::SwitchSpot(_) => (25, 1),
            Self::LevManage(_) => (26, 1),
            Self::EmuTrades { points, .. } => (27, points.len()),
            Self::TriggerManage { markets, keys, .. } => (28, markets.len() + keys.len()),
            Self::ResetProfit(_) => (29, 1),
            Self::ArbActivateNotify(_) => (30, 1),
            Self::AlertObject(_) => (31, 1),
            Self::AlertSnapshotRequest => (32, 0),
            Self::ChartTextState(_) => (33, 1),
            Self::OrdersHistoryRequest(_) => (34, 1),
            Self::RestartNow => (35, 0),
            Self::KernelLicenseStateRequest => (36, 0),
            Self::AutoDetect(_) => (37, 0),
        }
    }
}

#[cfg(any(test, feature = "diagnostics"))]
impl StratRuntimeCommand {
    fn profile_source(&self) -> (u8, usize) {
        match self {
            Self::SellPriceUpdate { .. } => (40, 1),
            Self::Delete { .. } => (41, 1),
        }
    }
}

#[cfg(any(test, feature = "diagnostics"))]
impl RuntimeCommandKind {
    fn profile_source(&self) -> (u8, usize) {
        match self {
            Self::MoveOrder { .. } => (60, 1),
            Self::CancelOrder { .. } => (61, 1),
            Self::UpdateStops { .. } => (62, 1),
            Self::UpdateVStop { .. } => (63, 1),
            Self::SetImmune { items } => (64, items.len()),
            Self::TurnOrderPanicSell { .. } => (65, 1),
            Self::RequestOrderStatus { .. } => (66, 1),
            Self::SwitchPanicSellByMarket { .. } => (67, 1),
        }
    }
}

#[cfg(any(test, feature = "diagnostics"))]
impl RuntimeTradeCommandKind {
    fn profile_source(&self) -> (u8, usize) {
        match self {
            Self::NewOrder { .. } => (80, 1),
            Self::JoinOrders { .. } => (81, 1),
            Self::SplitOrder(_) => (82, 1),
            Self::MoveAllSells { .. } => (83, 1),
            Self::MoveAllBuys { .. } => (84, 1),
            Self::ClosePosition(_) => (85, 1),
            Self::LimitClosePosition { .. } => (86, 1),
            Self::SplitPosition { .. } => (87, 1),
            Self::SellOrder(_) => (88, 1),
            Self::MarketSplitPosition { .. } => (89, 1),
            Self::Penalty { .. } => (90, 1),
            Self::PanicSellAll => (91, 0),
        }
    }
}
