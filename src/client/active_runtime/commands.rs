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
    TransferAssetsRefresh,
    TransferAssetsRefreshKind(crate::state::ExchangeKind),
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
    StrategySetChecked {
        strategy_id: u64,
        checked: bool,
        reply: mpsc::Sender<bool>,
    },
    StrategySendCheckedDelta,
    StrategyStartStop {
        is_start: bool,
    },
    WithUsizeReply {
        cmd: Box<RuntimeCommand>,
        reply: mpsc::Sender<usize>,
    },
    Request {
        request: RuntimeCommandRequest,
        reply: mpsc::Sender<RuntimeReply>,
    },
    OrderAction {
        kind: RuntimeCommandKind,
        reply: mpsc::Sender<bool>,
    },
    TradeAction {
        kind: RuntimeTradeCommandKind,
        reply: mpsc::Sender<Result<bool, TradeContextError>>,
    },
}

pub(super) enum RuntimeCommandRequest {
    OrderSnapshot {
        timeout: Duration,
    },
    BalanceSnapshot {
        timeout: Duration,
    },
    Balance {
        asset: String,
        timeout: Duration,
    },
    HedgeMode {
        timeout: Duration,
    },
    ApiExpirationTime {
        timeout: Duration,
    },
    TransferAssets {
        kind: crate::state::ExchangeKind,
        timeout: Duration,
    },
    CandlesData {
        timeout: Duration,
    },
    CoinCardCandles {
        market: String,
        ticks: crate::commands::candles::DeepHistoryKind,
        timeout: Duration,
    },
    ClientSettings {
        timeout: Duration,
    },
    EngineRaw {
        payload: Vec<u8>,
        timeout: Duration,
    },
}

pub(super) enum RuntimeReply {
    OrderSnapshot(Result<Vec<crate::state::Order>, mpsc::RecvTimeoutError>),
    BalanceSnapshot(Result<crate::state::BalancesState, mpsc::RecvTimeoutError>),
    Balance(Result<f64, EngineRequestError>),
    HedgeMode(Result<bool, EngineRequestError>),
    ApiExpirationTime(Result<crate::commands::engine_api::ApiExpirationTime, EngineRequestError>),
    TransferAssets(Result<Vec<crate::commands::engine_api::TransferAsset>, EngineRequestError>),
    CandlesData(Result<MergedCandles, mpsc::RecvTimeoutError>),
    CoinCardCandles(Result<Vec<crate::commands::candles::DeepPrice>, EngineRequestError>),
    ClientSettings(Result<crate::commands::ui::ClientSettingsCommand, mpsc::RecvTimeoutError>),
    EngineRaw(Result<EngineResponse, mpsc::RecvTimeoutError>),
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
    SwitchSpot(u8),
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
        on: bool,
        fixed: bool,
        level: f64,
        vol: f64,
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
    NewOrder(NewOrderParams),
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
