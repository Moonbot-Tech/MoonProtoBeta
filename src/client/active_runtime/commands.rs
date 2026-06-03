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
    #[cfg(any(test, feature = "diagnostics"))]
    DebugOutgoingBlackhole(bool),
    #[cfg(any(test, feature = "diagnostics"))]
    DebugResetErrEmuDiagnostics,
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
}
