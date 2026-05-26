//! Parsed `MPC_Order` command envelope.

use super::*;
use crate::commands::registry::CURRENT_PROTO_CMD_VER;
use std::convert::TryInto;

/// Все распарсенные TBaseTradeCommand подкоманды (CmdId маппинг -> variant).
/// Эта enum — public API. State::Orders.apply принимает её и применяет.
#[derive(Debug, Clone)]
pub enum TradeCommand {
    /// CmdId=4: TOrderStatus — полный snapshot ордера.
    OrderStatus(Box<OrderStatus>),
    /// CmdId=5: TOrderStatusUpdate — delta-update полей.
    OrderStatusUpdate(OrderStatusUpdate),
    /// CmdId=6: TOrderReplaceCommand — запрос на перемещение цены.
    OrderReplace(OrderReplaceCommand),
    /// CmdId=7: TOrderReplaceResponse — подтверждение перемещения.
    OrderReplaceResponse(Box<OrderReplaceResponse>),
    /// CmdId=8: TAllStatuses — снапшот всех ордеров (для CleanupMissing).
    AllStatuses(AllStatuses),
    /// CmdId=9: TAllStatusesReq — запрос на получение всех ордеров (client->server).
    AllStatusesRequest(BaseCommandHeader),
    /// CmdId=10: TOrderCancelCommand — отмена ордера.
    OrderCancel(OrderCancelCommand),
    /// CmdId=11: TJoinOrdersCommand — объединить ордера в одну позицию.
    JoinOrders(JoinOrdersCommand),
    /// CmdId=12: TSplitOrderCommand — разделить одну позицию на N частей.
    SplitOrder(SplitOrderCommand),
    /// CmdId=13: TMoveAllSellsCommand — переместить все sell ордера.
    MoveAllSells(MoveAllSellsCommand),
    /// CmdId=14: TDoClosePositionCommand — закрыть позицию.
    DoClosePosition(DoClosePositionCommand),
    /// CmdId=15: TDoLimitClosePositionCommand — limit-закрытие позиции.
    DoLimitClosePosition(JoinOrdersCommand),
    /// CmdId=16: TDoSplitPositionCommand — разделить позицию.
    DoSplitPosition(JoinOrdersCommand),
    /// CmdId=17: TDoSellOrderCommand — выставить sell с конкретной ценой/размером.
    DoSellOrder(DoSellOrderCommand),
    /// CmdId=18: TOrderStatusRequest — запрос конкретного ордера по UID (CleanupMissing).
    OrderStatusRequest(TradeEpochHeader),
    /// CmdId=19: TOrderNotFound — сервер сообщает что ордер не найден.
    OrderNotFound(TradeEpochHeader),
    /// CmdId=20: TOrderStopsUpdate — обновление стопов.
    OrderStopsUpdate(OrderStopsUpdate),
    /// CmdId=21: TTurnPanicSellCommand — включить/выключить panic sell.
    TurnPanicSell(TurnPanicSellCommand),
    /// CmdId=22: TSetImmuneCommand — пометить ордера как immune от UI кликов.
    SetImmune(SetImmuneCommand),
    /// CmdId=23: TPenaltyCommand — пометить маркет penalty (cooldown).
    Penalty(MarketCommandHeader),
    /// CmdId=24: TTradeVisualCommand — base для visual-only команд.
    TradeVisual(MarketCommandHeader),
    /// CmdId=25: TOrderTracePoint — точка трейс-графика.
    OrderTracePoint(OrderTracePoint),
    /// CmdId=26: TCorridorUpdate — корридор цен.
    CorridorUpdate(CorridorUpdate),
    /// CmdId=27: TMoveAllBuysCommand — переместить все buy ордера.
    MoveAllBuys(MoveAllBuysCommand),
    /// CmdId=28: TBulkReplaceNotify — уведомление о массовом replace.
    BulkReplaceNotify(BulkReplaceNotify),
    /// CmdId=29: TVStopUpdate — обновление volume stop.
    VStopUpdate(VStopUpdate),
    /// CmdId=30: TDoMarketSplitPositionCommand — market-split позиции.
    DoMarketSplitPosition(JoinOrdersCommand),

    /// CmdId=1: TBaseMarketCommand (raw, без поверх) — используется как ancestor type.
    BaseMarket(MarketCommandHeader),
    /// CmdId=2: TTradeEpochCommand (raw).
    TradeEpoch(TradeEpochHeader),
    /// CmdId=3: TNewOrderCommand — запрос на создание нового ордера.
    NewOrder(NewOrderCommand),

    /// Команда с неизвестным CmdId — для forward-compatibility.
    Unknown { cmd_id: u8, uid: u64 },
}

impl TradeCommand {
    /// Распарсить TBaseTradeCommand payload (после dispatch'a по MPC_Order).
    ///
    /// Wire-format: CmdId(1) + ver(2) + UID(8) + class-specific payload.
    /// Version gate: если ver > 3 — возвращаем Unknown (forward-compatible skip).
    pub fn parse(payload: &[u8]) -> Option<Self> {
        let mut r = payload;
        let peek_cmd_id = if !r.is_empty() {
            r[0]
        } else {
            return None;
        };
        // Peek ver без consume.
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
            _ => {
                let uid = u64::from_le_bytes(r[3..11].try_into().unwrap());
                Some(TradeCommand::Unknown {
                    cmd_id: peek_cmd_id,
                    uid,
                })
            }
        }
    }

    /// UID команды (для матчинга в state).
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
            Self::BaseMarket(h) => h.base.uid,
            Self::TradeEpoch(h) => h.market.base.uid,
            Self::NewOrder(c) => c.market.base.uid,
            Self::Unknown { uid, .. } => *uid,
        }
    }
}
