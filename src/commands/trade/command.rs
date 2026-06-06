//! Parsed `MPC_Order` command envelope.

use super::*;
use crate::commands::registry::CURRENT_PROTO_CMD_VER;
use std::convert::TryInto;

/// Parsed `TBaseTradeCommand` payloads mapped by Delphi CmdId.
///
/// This is the low-level protocol enum accepted by `state::Orders::apply`.
#[derive(Debug, Clone)]
pub enum TradeCommand {
    /// CmdId=4: `TOrderStatus`, full order snapshot.
    OrderStatus(Box<OrderStatus>),
    /// CmdId=5: `TOrderStatusUpdate`, order delta update.
    OrderStatusUpdate(OrderStatusUpdate),
    /// CmdId=6: `TOrderReplaceCommand`, request to move an order price.
    OrderReplace(OrderReplaceCommand),
    /// CmdId=7: `TOrderReplaceResponse`, move acknowledgement/update.
    OrderReplaceResponse(Box<OrderReplaceResponse>),
    /// CmdId=8: `TAllStatuses`, full order snapshot for cleanup.
    AllStatuses(AllStatuses),
    /// CmdId=9: `TAllStatusesReq`, client request for all orders.
    AllStatusesRequest(BaseCommandHeader),
    /// CmdId=10: `TOrderCancelCommand`, cancel one order.
    OrderCancel(OrderCancelCommand),
    /// CmdId=11: `TJoinOrdersCommand`, join orders into one position.
    JoinOrders(JoinOrdersCommand),
    /// CmdId=12: `TSplitOrderCommand`, split one position/order.
    SplitOrder(SplitOrderCommand),
    /// CmdId=13: `TMoveAllSellsCommand`, move sell orders in bulk.
    MoveAllSells(MoveAllSellsCommand),
    /// CmdId=14: `TDoClosePositionCommand`, close a position.
    DoClosePosition(DoClosePositionCommand),
    /// CmdId=15: `TDoLimitClosePositionCommand`, limit-close a position.
    DoLimitClosePosition(JoinOrdersCommand),
    /// CmdId=16: `TDoSplitPositionCommand`, split a position.
    DoSplitPosition(JoinOrdersCommand),
    /// CmdId=17: `TDoSellOrderCommand`, place sell with price/size.
    DoSellOrder(DoSellOrderCommand),
    /// CmdId=18: `TOrderStatusRequest`, request one order status.
    OrderStatusRequest(TradeEpochHeader),
    /// CmdId=19: `TOrderNotFound`, server says the order is missing.
    OrderNotFound(TradeEpochHeader),
    /// CmdId=20: `TOrderStopsUpdate`, stop settings update.
    OrderStopsUpdate(OrderStopsUpdate),
    /// CmdId=21: `TTurnPanicSellCommand`, toggle panic sell.
    TurnPanicSell(TurnPanicSellCommand),
    /// CmdId=22: `TSetImmuneCommand`, mark orders immune to UI clicks.
    SetImmune(SetImmuneCommand),
    /// CmdId=23: `TPenaltyCommand`, set market penalty/cooldown.
    Penalty(MarketCommandHeader),
    /// CmdId=24: `TTradeVisualCommand`, visual-only command base.
    TradeVisual(MarketCommandHeader),
    /// CmdId=25: `TOrderTracePoint`, trace chart point.
    OrderTracePoint(OrderTracePoint),
    /// CmdId=26: `TCorridorUpdate`, price corridor update.
    CorridorUpdate(CorridorUpdate),
    /// CmdId=27: `TMoveAllBuysCommand`, move buy orders in bulk.
    MoveAllBuys(MoveAllBuysCommand),
    /// CmdId=28: `TBulkReplaceNotify`, bulk replace notification.
    BulkReplaceNotify(BulkReplaceNotify),
    /// CmdId=29: `TVStopUpdate`, volume stop update.
    VStopUpdate(VStopUpdate),
    /// CmdId=30: `TDoMarketSplitPositionCommand`, market-split position.
    DoMarketSplitPosition(JoinOrdersCommand),
    /// CmdId=31: `TClosedSellOrderReportCommand`, exact DB Orders SQL report.
    ClosedSellOrderReport(ClosedSellOrderReport),

    /// CmdId=1: raw `TBaseMarketCommand`, used as an ancestor type.
    BaseMarket(MarketCommandHeader),
    /// CmdId=2: TTradeEpochCommand (raw).
    TradeEpoch(TradeEpochHeader),
    /// CmdId=3: `TNewOrderCommand`, request to create a new order.
    NewOrder(NewOrderCommand),

    /// Unknown CmdId, preserved for forward compatibility.
    Unknown { cmd_id: u8, uid: u64 },
}

impl TradeCommand {
    /// Parse a `TBaseTradeCommand` payload after `MPC_Order` dispatch.
    ///
    /// Wire-format: CmdId(1) + ver(2) + UID(8) + class-specific payload.
    /// Version gate: `ver > 3` returns `Unknown` for forward-compatible skip.
    pub fn parse(payload: &[u8]) -> Option<Self> {
        let mut r = payload;
        let peek_cmd_id = if !r.is_empty() {
            r[0]
        } else {
            return None;
        };
        // Peek version without consuming the buffer.
        if r.len() < 11 {
            return None;
        }
        let ver = u16::from_le_bytes([r[1], r[2]]);
        if ver > CURRENT_PROTO_CMD_VER {
            let uid = u64::from_le_bytes(r[3..11].try_into().unwrap());
            return Some(TradeCommand::Unknown {
                cmd_id: peek_cmd_id,
                uid,
            });
        }

        match peek_cmd_id {
            1 => Some(TradeCommand::BaseMarket(MarketCommandHeader::read(&mut r)?)),
            2 => Some(TradeCommand::TradeEpoch(TradeEpochHeader::read(&mut r)?)),
            3 => Some(TradeCommand::NewOrder(NewOrderCommand::read(&mut r)?)),
            4 => Some(TradeCommand::OrderStatus(Box::new(OrderStatus::read(
                &mut r,
            )?))),
            5 => Some(TradeCommand::OrderStatusUpdate(OrderStatusUpdate::read(
                &mut r,
            )?)),
            6 => Some(TradeCommand::OrderReplace(OrderReplaceCommand::read(
                &mut r,
            )?)),
            7 => Some(TradeCommand::OrderReplaceResponse(Box::new(
                OrderReplaceResponse::read(&mut r)?,
            ))),
            8 => Some(TradeCommand::AllStatuses(AllStatuses::read(&mut r)?)),
            9 => {
                let h = BaseCommandHeader::read(&mut r)?;
                Some(TradeCommand::AllStatusesRequest(h))
            }
            10 => Some(TradeCommand::OrderCancel(OrderCancelCommand::read(&mut r)?)),
            11 => Some(TradeCommand::JoinOrders(JoinOrdersCommand::read(&mut r)?)),
            12 => Some(TradeCommand::SplitOrder(SplitOrderCommand::read(&mut r)?)),
            13 => Some(TradeCommand::MoveAllSells(MoveAllSellsCommand::read(
                &mut r,
            )?)),
            14 => Some(TradeCommand::DoClosePosition(DoClosePositionCommand::read(
                &mut r,
            )?)),
            15 => Some(TradeCommand::DoLimitClosePosition(JoinOrdersCommand::read(
                &mut r,
            )?)),
            16 => Some(TradeCommand::DoSplitPosition(JoinOrdersCommand::read(
                &mut r,
            )?)),
            17 => Some(TradeCommand::DoSellOrder(DoSellOrderCommand::read(&mut r)?)),
            18 => Some(TradeCommand::OrderStatusRequest(TradeEpochHeader::read(
                &mut r,
            )?)),
            19 => Some(TradeCommand::OrderNotFound(TradeEpochHeader::read(&mut r)?)),
            20 => Some(TradeCommand::OrderStopsUpdate(OrderStopsUpdate::read(
                &mut r,
            )?)),
            21 => Some(TradeCommand::TurnPanicSell(TurnPanicSellCommand::read(
                &mut r,
            )?)),
            22 => Some(TradeCommand::SetImmune(SetImmuneCommand::read(&mut r)?)),
            23 => Some(TradeCommand::Penalty(MarketCommandHeader::read(&mut r)?)),
            24 => Some(TradeCommand::TradeVisual(MarketCommandHeader::read(
                &mut r,
            )?)),
            25 => Some(TradeCommand::OrderTracePoint(OrderTracePoint::read(
                &mut r,
            )?)),
            26 => Some(TradeCommand::CorridorUpdate(CorridorUpdate::read(&mut r)?)),
            27 => Some(TradeCommand::MoveAllBuys(MoveAllBuysCommand::read(&mut r)?)),
            28 => Some(TradeCommand::BulkReplaceNotify(BulkReplaceNotify::read(
                &mut r,
            )?)),
            29 => Some(TradeCommand::VStopUpdate(VStopUpdate::read(&mut r)?)),
            30 => Some(TradeCommand::DoMarketSplitPosition(
                JoinOrdersCommand::read(&mut r)?,
            )),
            31 => Some(TradeCommand::ClosedSellOrderReport(
                ClosedSellOrderReport::read(&mut r)?,
            )),
            _ => {
                let uid = u64::from_le_bytes(r[3..11].try_into().unwrap());
                Some(TradeCommand::Unknown {
                    cmd_id: peek_cmd_id,
                    uid,
                })
            }
        }
    }

    /// Command UID used by state matching.
    pub fn uid(&self) -> u64 {
        match self {
            Self::OrderStatus(c) => c.epoch_header.market.base.uid,
            Self::OrderStatusUpdate(c) => c.epoch_header.market.base.uid,
            Self::OrderReplace(c) => c.epoch_header.market.base.uid,
            Self::OrderReplaceResponse(c) => c.epoch_header.market.base.uid,
            Self::AllStatuses(c) => c.header.uid,
            Self::AllStatusesRequest(h) => h.uid,
            Self::OrderCancel(c) => c.epoch_header.market.base.uid,
            Self::JoinOrders(c) => c.market.base.uid,
            Self::SplitOrder(c) => c.market.base.uid,
            Self::MoveAllSells(c) => c.market.base.uid,
            Self::DoClosePosition(c) => c.market.base.uid,
            Self::DoLimitClosePosition(c) => c.market.base.uid,
            Self::DoSplitPosition(c) => c.market.base.uid,
            Self::DoSellOrder(c) => c.market.base.uid,
            Self::OrderStatusRequest(h) => h.market.base.uid,
            Self::OrderNotFound(h) => h.market.base.uid,
            Self::OrderStopsUpdate(c) => c.epoch_header.market.base.uid,
            Self::TurnPanicSell(c) => c.epoch_header.market.base.uid,
            Self::SetImmune(c) => c.header.uid,
            Self::Penalty(h) => h.base.uid,
            Self::TradeVisual(h) => h.base.uid,
            Self::OrderTracePoint(c) => c.market.base.uid,
            Self::CorridorUpdate(c) => c.market.base.uid,
            Self::MoveAllBuys(c) => c.market.base.uid,
            Self::BulkReplaceNotify(c) => c.market.base.uid,
            Self::VStopUpdate(c) => c.epoch_header.market.base.uid,
            Self::DoMarketSplitPosition(c) => c.market.base.uid,
            Self::ClosedSellOrderReport(c) => c.header.uid,
            Self::BaseMarket(h) => h.base.uid,
            Self::TradeEpoch(h) => h.market.base.uid,
            Self::NewOrder(c) => c.market.base.uid,
            Self::Unknown { uid, .. } => *uid,
        }
    }
}
