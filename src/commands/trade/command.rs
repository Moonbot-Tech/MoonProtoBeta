//! Parsed `MPC_Order` command envelope.

use super::*;
use crate::commands::registry::CURRENT_PROTO_CMD_VER;
use crate::commands::report::{
    CMD_CHECK_ROWS_REQUEST, CMD_ROW_DELETE, CMD_ROW_UPSERT, CMD_SCHEMA, CMD_SCHEMA_REQUEST,
    CMD_SYNC_PAGE, CMD_SYNC_REQUEST,
};
use std::convert::TryInto;

/// Current `MPC_Order` payloads.
///
/// OrdersProto v1 command ids 2..22 and 27..30 are intentionally absent and
/// remain reserved forever. The report and visual subchannels keep their
/// existing ids; canonical order state lives in 41..47.
#[derive(Debug, Clone)]
pub enum TradeCommand {
    Penalty(MarketCommandHeader),
    TradeVisual(MarketCommandHeader),
    OrderTracePoint(OrderTracePoint),
    CorridorUpdate(CorridorUpdate),
    ClosedSellOrderReport(ClosedSellOrderReport),
    ReportRowUpsert(crate::commands::report::RepRowUpsert),
    ReportRowDelete(crate::commands::report::RepRowDelete),
    ReportSyncRequest(crate::commands::report::RepSyncRequest),
    ReportSchemaRequest(BaseCommandHeader),
    ReportSchema(crate::commands::report::RepSchema),
    ReportSyncPage(crate::commands::report::RepSyncPage),
    ReportCheckRowsRequest(crate::commands::report::RepCheckRowsRequest),
    OrderImage(OrderImage),
    OrderPatch(OrderPatch),
    OrdersSnapshot(OrdersSnapshot),
    OrdersCatalog(OrdersCatalog),
    OrderStatusRequest(OrderStatusRequest),
    OrderNotFound(BaseCommandHeader),
    OrderCommand(OrderCommand),
    BaseMarket(MarketCommandHeader),
    Unknown { cmd_id: u8, uid: u64 },
}

impl TradeCommand {
    pub fn parse(payload: &[u8]) -> Option<Self> {
        let mut input = payload;
        Self::read(&mut input)
    }

    pub(super) fn read(input: &mut &[u8]) -> Option<Self> {
        if input.len() < 11 {
            return None;
        }
        let cmd_id = input[0];
        let ver = u16::from_le_bytes([input[1], input[2]]);
        let uid = u64::from_le_bytes(input[3..11].try_into().unwrap());
        if ver > CURRENT_PROTO_CMD_VER {
            *input = &[];
            return Some(Self::Unknown { cmd_id, uid });
        }

        match cmd_id {
            1 => Some(Self::BaseMarket(MarketCommandHeader::read(input)?)),
            23 => Some(Self::Penalty(MarketCommandHeader::read(input)?)),
            24 => Some(Self::TradeVisual(MarketCommandHeader::read(input)?)),
            25 => Some(Self::OrderTracePoint(OrderTracePoint::read(input)?)),
            26 => Some(Self::CorridorUpdate(CorridorUpdate::read(input)?)),
            31 => Some(Self::ClosedSellOrderReport(ClosedSellOrderReport::read(
                input,
            )?)),
            CMD_ROW_UPSERT => Some(Self::ReportRowUpsert(
                crate::commands::report::RepRowUpsert::read(input)?,
            )),
            CMD_ROW_DELETE => Some(Self::ReportRowDelete(
                crate::commands::report::RepRowDelete::read(input)?,
            )),
            CMD_SYNC_REQUEST => Some(Self::ReportSyncRequest(
                crate::commands::report::RepSyncRequest::read(input)?,
            )),
            CMD_SCHEMA_REQUEST => Some(Self::ReportSchemaRequest(BaseCommandHeader::read(input)?)),
            CMD_SCHEMA => Some(Self::ReportSchema(
                crate::commands::report::RepSchema::read(input)?,
            )),
            CMD_SYNC_PAGE => Some(Self::ReportSyncPage(
                crate::commands::report::RepSyncPage::read(input)?,
            )),
            CMD_CHECK_ROWS_REQUEST => Some(Self::ReportCheckRowsRequest(
                crate::commands::report::RepCheckRowsRequest::read(input)?,
            )),
            41 => Some(Self::OrderImage(OrderImage::read(input)?)),
            42 => Some(Self::OrderPatch(OrderPatch::read(input)?)),
            43 => Some(Self::OrdersSnapshot(OrdersSnapshot::read(input)?)),
            44 => Some(Self::OrdersCatalog(OrdersCatalog::read(input)?)),
            45 => Some(Self::OrderStatusRequest(OrderStatusRequest::read(input)?)),
            46 => Some(Self::OrderNotFound(BaseCommandHeader::read(input)?)),
            47 => Some(Self::OrderCommand(OrderCommand::read(input)?)),
            _ => {
                *input = &[];
                Some(Self::Unknown { cmd_id, uid })
            }
        }
    }

    pub fn uid(&self) -> u64 {
        match self {
            Self::Penalty(h) | Self::TradeVisual(h) | Self::BaseMarket(h) => h.base.uid,
            Self::OrderTracePoint(c) => c.market.base.uid,
            Self::CorridorUpdate(c) => c.market.base.uid,
            Self::ClosedSellOrderReport(c) => c.header.uid,
            Self::ReportRowUpsert(c) => c.header.uid,
            Self::ReportRowDelete(c) => c.header.uid,
            Self::ReportSyncRequest(c) => c.header.uid,
            Self::ReportSchemaRequest(h) => h.uid,
            Self::ReportSchema(c) => c.header.uid,
            Self::ReportSyncPage(c) => c.header.uid,
            Self::ReportCheckRowsRequest(c) => c.header.uid,
            Self::OrderImage(c) => c.header.uid,
            Self::OrderPatch(c) => c.header.uid,
            Self::OrdersSnapshot(c) => c.header.uid,
            Self::OrdersCatalog(c) => c.header.uid,
            Self::OrderStatusRequest(c) => c.header.uid,
            Self::OrderNotFound(h) => h.uid,
            Self::OrderCommand(c) => c.header.uid,
            Self::Unknown { uid, .. } => *uid,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retired_order_command_ids_stay_unknown() {
        for cmd_id in (2_u8..=22).chain(27..=30) {
            let mut payload = vec![cmd_id];
            payload.extend_from_slice(&CURRENT_PROTO_CMD_VER.to_le_bytes());
            payload.extend_from_slice(&123_u64.to_le_bytes());
            assert!(matches!(
                TradeCommand::parse(&payload),
                Some(TradeCommand::Unknown {
                    cmd_id: parsed,
                    uid: 123
                }) if parsed == cmd_id
            ));
        }
    }
}
