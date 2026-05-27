//! Public high-level Active Lib runtime types.

use super::*;

/// Ticket returned after Active Lib has queued a non-blocking Engine API action.
///
/// The server result arrives later as [`crate::events::Event::EngineAction`] and
/// as the underlying [`crate::events::Event::EngineResponse`].
#[derive(Debug, Clone, PartialEq)]
pub struct EngineActionTicket {
    pub kind: crate::events::EngineActionKind,
    pub request_uid: Option<u64>,
    pub method: crate::commands::EngineMethod,
}

/// Ticket returned after a demand-driven CoinCard candles request is queued.
///
/// Completion arrives as [`crate::events::Event::CoinCardCandles`]; the candles
/// are then readable from `snapshot().coin_card_candles()`.
#[derive(Debug, Clone, PartialEq)]
pub struct CoinCardCandlesTicket {
    pub market: String,
    pub kind: crate::commands::candles::DeepHistoryKind,
    pub request_uid: Option<u64>,
}

/// Error returned by the high-level [`MoonClient`](super::MoonClient) runtime API.
#[derive(Debug)]
pub enum MoonClientError {
    /// Connect/init failed before the runtime became usable.
    Connect(ConnectError),
    /// A one-shot runtime request timed out.
    RequestTimeout,
    /// A one-shot runtime request channel was closed.
    RequestDisconnected,
    /// Engine API helper failed.
    EngineRequest(EngineRequestError),
    /// Session route fields required by market-level trade actions are missing.
    TradeContext(TradeContextError),
    /// The runtime thread stopped, panicked, or its command channel is closed.
    RuntimeStopped,
}

impl std::fmt::Display for MoonClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect(err) => write!(f, "{err}"),
            Self::RequestTimeout => write!(f, "MoonProto request timed out"),
            Self::RequestDisconnected => write!(f, "MoonProto request channel disconnected"),
            Self::EngineRequest(err) => write!(f, "{err}"),
            Self::TradeContext(err) => write!(f, "{err}"),
            Self::RuntimeStopped => write!(f, "MoonProto runtime is stopped"),
        }
    }
}

impl std::error::Error for MoonClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connect(err) => Some(err),
            Self::EngineRequest(err) => Some(err),
            Self::TradeContext(err) => Some(err),
            Self::RequestTimeout | Self::RequestDisconnected => None,
            Self::RuntimeStopped => None,
        }
    }
}

impl From<ConnectError> for MoonClientError {
    fn from(err: ConnectError) -> Self {
        Self::Connect(err)
    }
}

impl From<mpsc::RecvTimeoutError> for MoonClientError {
    fn from(err: mpsc::RecvTimeoutError) -> Self {
        match err {
            mpsc::RecvTimeoutError::Timeout => Self::RequestTimeout,
            mpsc::RecvTimeoutError::Disconnected => Self::RequestDisconnected,
        }
    }
}

impl From<EngineRequestError> for MoonClientError {
    fn from(err: EngineRequestError) -> Self {
        Self::EngineRequest(err)
    }
}

impl From<TradeContextError> for MoonClientError {
    fn from(err: TradeContextError) -> Self {
        Self::TradeContext(err)
    }
}

/// User-facing trades stream content selection.
///
/// Low-level wire helpers still use the historical boolean because that is the
/// packet field. `MoonClient` uses this enum so application code does not have
/// to remember what `true` means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradesStreamMode {
    TradesOnly,
    TradesAndMarketMakers,
}

impl TradesStreamMode {
    pub const fn want_market_makers(self) -> bool {
        matches!(self, Self::TradesAndMarketMakers)
    }
}

/// Long/short side for user-facing market trade intents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    Long,
    Short,
}

impl OrderSide {
    pub const fn is_short(self) -> bool {
        matches!(self, Self::Short)
    }
}

/// User-facing parameters for opening a new order.
#[derive(Debug, Clone)]
pub struct NewOrderParams {
    pub market: String,
    pub side: OrderSide,
    pub price: f64,
    pub size: f64,
    /// `None` sends Delphi `StratID=0`.
    pub strategy_id: Option<u64>,
}

impl NewOrderParams {
    pub fn new(market: impl Into<String>, side: OrderSide, price: f64, size: f64) -> Self {
        Self {
            market: market.into(),
            side,
            price,
            size,
            strategy_id: None,
        }
    }

    pub fn with_strategy_id(mut self, strategy_id: u64) -> Self {
        self.strategy_id = Some(strategy_id);
        self
    }
}

/// User-facing parameters for `TSplitOrderCommand`.
#[derive(Debug, Clone)]
pub struct SplitOrderParams {
    pub market: String,
    pub parts: i32,
    pub split_small: bool,
    pub split_small_sell: bool,
}

impl SplitOrderParams {
    pub fn new(market: impl Into<String>, parts: i32) -> Self {
        Self {
            market: market.into(),
            parts,
            split_small: false,
            split_small_sell: false,
        }
    }
}

/// User-facing parameters for market close.
#[derive(Debug, Clone)]
pub struct ClosePositionParams {
    pub market: String,
    pub market_sell: bool,
}

impl ClosePositionParams {
    pub fn new(market: impl Into<String>) -> Self {
        Self {
            market: market.into(),
            market_sell: true,
        }
    }
}

/// User-facing parameters for `TDoSellOrderCommand`.
#[derive(Debug, Clone)]
pub struct SellOrderParams {
    pub market: String,
    pub price: f64,
    pub size: f64,
}

impl SellOrderParams {
    pub fn new(market: impl Into<String>, price: f64, size: f64) -> Self {
        Self {
            market: market.into(),
            price,
            size,
        }
    }
}
