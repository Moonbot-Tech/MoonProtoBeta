//! High-level Active Lib intent handles.

use super::{
    MoonClientError, NewOrderParams, OrderSide, RuntimeCommand, RuntimeCommandKind,
    RuntimeTradeCommandKind, SellOrderParams, SplitOrderParams,
};
use std::sync::mpsc;

/// Order intent handle.
///
/// UI code can keep immutable order snapshots for rendering, but all stateful
/// order actions go through this handle so the runtime applies them to the live
/// `Orders` model before queueing protocol commands.
#[derive(Clone)]
pub struct MoonOrders {
    pub(super) tx: mpsc::Sender<RuntimeCommand>,
}

impl MoonOrders {
    /// Move/replace one tracked order by UID.
    pub fn move_order(&self, uid: u64, new_price: f64) -> Result<bool, MoonClientError> {
        self.send_bool(RuntimeCommandKind::MoveOrder { uid, new_price })
    }

    /// Cancel one tracked order by UID.
    pub fn cancel(&self, uid: u64) -> Result<bool, MoonClientError> {
        self.send_bool(RuntimeCommandKind::CancelOrder { uid })
    }

    /// Update Stops for one tracked order by UID.
    pub fn update_stops(
        &self,
        uid: u64,
        stops: crate::commands::trade::StopSettings,
    ) -> Result<bool, MoonClientError> {
        self.send_bool(RuntimeCommandKind::UpdateStops { uid, stops })
    }

    /// Update VStop for one tracked order by UID.
    pub fn update_vstop(
        &self,
        uid: u64,
        on: bool,
        fixed: bool,
        level: f64,
        vol: f64,
    ) -> Result<bool, MoonClientError> {
        self.send_bool(RuntimeCommandKind::UpdateVStop {
            uid,
            on,
            fixed,
            level,
            vol,
        })
    }

    /// Apply click-immune intent for found active orders.
    pub fn set_immune(
        &self,
        items: Vec<crate::commands::trade::ImmuneItem>,
    ) -> Result<bool, MoonClientError> {
        self.send_bool(RuntimeCommandKind::SetImmune { items })
    }

    /// Toggle panic sell for one tracked order by UID.
    pub fn turn_panic_sell(&self, uid: u64, turn_on: bool) -> Result<bool, MoonClientError> {
        self.send_bool(RuntimeCommandKind::TurnOrderPanicSell { uid, turn_on })
    }

    /// Request a fresh status for one tracked order by UID.
    pub fn request_status(&self, uid: u64) -> Result<bool, MoonClientError> {
        self.send_bool(RuntimeCommandKind::RequestOrderStatus { uid })
    }

    /// Apply market-level panic sell button semantics.
    pub fn switch_panic_sell_by_market(
        &self,
        market_name: impl Into<String>,
        turn_on: bool,
    ) -> Result<bool, MoonClientError> {
        self.send_bool(RuntimeCommandKind::SwitchPanicSellByMarket {
            market_name: market_name.into(),
            turn_on,
        })
    }

    fn send_bool(&self, kind: RuntimeCommandKind) -> Result<bool, MoonClientError> {
        let (tx, rx) = mpsc::channel();
        self.tx
            .send(RuntimeCommand::OrderAction { kind, reply: tx })
            .map_err(|_| MoonClientError::RuntimeStopped)?;
        rx.recv().map_err(|_| MoonClientError::RuntimeStopped)
    }
}

/// Market-level trade intent handle.
///
/// These actions create or manage orders by market name. The caller does not
/// pass `TradeCtx`; the runtime owner derives Delphi route bytes from the
/// active session and queues the same wire commands as the low-level `Client`.
#[derive(Clone)]
pub struct MoonTrade {
    pub(super) tx: mpsc::Sender<RuntimeCommand>,
}

impl MoonTrade {
    /// Send `TNewOrderCommand`.
    pub fn new_order(&self, params: NewOrderParams) -> Result<bool, MoonClientError> {
        self.send_bool(RuntimeTradeCommandKind::NewOrder(params))
    }

    /// Send `TJoinOrdersCommand`.
    pub fn join_orders(
        &self,
        market_name: impl Into<String>,
        side: OrderSide,
    ) -> Result<bool, MoonClientError> {
        self.send_bool(RuntimeTradeCommandKind::JoinOrders {
            market_name: market_name.into(),
            side,
        })
    }

    /// Send `TSplitOrderCommand`.
    pub fn split_order(&self, params: SplitOrderParams) -> Result<bool, MoonClientError> {
        self.send_bool(RuntimeTradeCommandKind::SplitOrder(params))
    }

    /// Send gated `TMoveAllSellsCommand`.
    pub fn move_all_sells(
        &self,
        market_name: impl Into<String>,
        params: crate::commands::trade::MoveAllSellsParams,
    ) -> Result<bool, MoonClientError> {
        self.send_bool(RuntimeTradeCommandKind::MoveAllSells {
            market_name: market_name.into(),
            params,
        })
    }

    /// Send gated `TMoveAllBuysCommand`.
    pub fn move_all_buys(
        &self,
        market_name: impl Into<String>,
        params: crate::commands::trade::MoveAllBuysParams,
    ) -> Result<bool, MoonClientError> {
        self.send_bool(RuntimeTradeCommandKind::MoveAllBuys {
            market_name: market_name.into(),
            params,
        })
    }

    /// Send `TDoClosePositionCommand`.
    pub fn close_position(
        &self,
        params: super::ClosePositionParams,
    ) -> Result<bool, MoonClientError> {
        self.send_bool(RuntimeTradeCommandKind::ClosePosition(params))
    }

    /// Send `TDoLimitClosePositionCommand`.
    pub fn limit_close_position(
        &self,
        market_name: impl Into<String>,
        side: OrderSide,
    ) -> Result<bool, MoonClientError> {
        self.send_bool(RuntimeTradeCommandKind::LimitClosePosition {
            market_name: market_name.into(),
            side,
        })
    }

    /// Send `TDoSplitPositionCommand`.
    pub fn split_position(
        &self,
        market_name: impl Into<String>,
        side: OrderSide,
    ) -> Result<bool, MoonClientError> {
        self.send_bool(RuntimeTradeCommandKind::SplitPosition {
            market_name: market_name.into(),
            side,
        })
    }

    /// Send `TDoSellOrderCommand`.
    pub fn sell_order(&self, params: SellOrderParams) -> Result<bool, MoonClientError> {
        self.send_bool(RuntimeTradeCommandKind::SellOrder(params))
    }

    /// Send `TDoMarketSplitPositionCommand`.
    pub fn market_split_position(
        &self,
        market_name: impl Into<String>,
        side: OrderSide,
    ) -> Result<bool, MoonClientError> {
        self.send_bool(RuntimeTradeCommandKind::MarketSplitPosition {
            market_name: market_name.into(),
            side,
        })
    }

    /// Send `TPenaltyCommand`.
    pub fn penalty(&self, market_name: impl Into<String>) -> Result<bool, MoonClientError> {
        self.send_bool(RuntimeTradeCommandKind::Penalty {
            market_name: market_name.into(),
        })
    }

    fn send_bool(&self, kind: RuntimeTradeCommandKind) -> Result<bool, MoonClientError> {
        let (tx, rx) = mpsc::channel();
        self.tx
            .send(RuntimeCommand::TradeAction { kind, reply: tx })
            .map_err(|_| MoonClientError::RuntimeStopped)?;
        rx.recv()
            .map_err(|_| MoonClientError::RuntimeStopped)?
            .map_err(MoonClientError::from)
    }
}
