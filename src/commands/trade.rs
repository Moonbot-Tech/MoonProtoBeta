//! `MPC_Order` payloads.
//!
//! The canonical OrdersProto used by global protocol version 4 permanently
//! retires command ids 2..22 and 27..30. Order state and user intent are
//! carried only by commands 41..47; report and chart-side commands keep their
//! existing ids.

use super::registry::{read_string, CURRENT_PROTO_CMD_VER};

pub(crate) mod builders;
pub use builders::TradeCtx;
pub(crate) use builders::{
    build_do_close_position, build_do_limit_close_position, build_do_market_split_position,
    build_do_sell_order, build_do_split_position, build_move_all_buys, build_move_all_sells,
    build_penalty,
};

mod command;
pub use command::TradeCommand;

mod order_v2;
pub(crate) use order_v2::{
    build_order_command, build_order_status_request, next_order_action_id, state_hash,
    CanonicalOrderState, OrderCatalogRecord, OrderCommand, OrderCommandPayload, OrderDescription,
    OrderImage, OrderPatch, OrderStatusRequest, OrdersCatalog, OrdersSnapshot, OFL_IMMUNE,
    OFL_PANIC_AUTO, OFL_PANIC_ON, ORDER_RECONCILE_MASK, ORDER_SECTION_ALL_MASK,
    ORDER_SECTION_COUNT, OSEC_BUY_EXEC, OSEC_BUY_PLACEMENT, OSEC_BUY_SLOW, OSEC_BUY_TARGET,
    OSEC_FLAGS, OSEC_PHASE, OSEC_PLANNED, OSEC_SELL_EXEC, OSEC_SELL_PLACEMENT, OSEC_SELL_SLOW,
    OSEC_SELL_TARGET, OSEC_STOPS, OSEC_VSTOP,
};

mod enums;
pub use enums::{
    FixedPosition, MoveAllBuysCmdType, MoveAllCmdType, OrderSubType, OrderType, OrderWorkerStatus,
    ReplaceMultiKind,
};

mod headers;
pub(crate) use headers::{BaseCommandHeader, MarketCommandHeader};

mod records;
pub use records::{DelphiBool, ExchangeOrder, ImmuneItem, PriceZone, StopSettings};

mod trace;
#[allow(unused_imports)]
pub(crate) use trace::trace_flags;
pub use trace::{CorridorUpdate, OrderTracePoint};

/// Long/short filter for bulk order actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PositionFilter {
    Both,
    Long,
    Short,
}

impl PositionFilter {
    pub(crate) const fn to_fixed_position(self) -> FixedPosition {
        match self {
            Self::Both => FixedPosition::Both,
            Self::Long => FixedPosition::Long,
            Self::Short => FixedPosition::Short,
        }
    }
}

/// Trader-visible bulk replace mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BulkMoveKind {
    Shift,
    TopVolume,
    LowVolume,
    TopProfit,
    All,
    LastSet,
    LastMoved,
}

impl BulkMoveKind {
    pub(crate) const fn to_replace_multi_kind(self) -> ReplaceMultiKind {
        match self {
            Self::Shift => ReplaceMultiKind::Shift,
            Self::TopVolume => ReplaceMultiKind::TopVol,
            Self::LowVolume => ReplaceMultiKind::LowVol,
            Self::TopProfit => ReplaceMultiKind::TopProfit,
            Self::All => ReplaceMultiKind::All,
            Self::LastSet => ReplaceMultiKind::LastSet,
            Self::LastMoved => ReplaceMultiKind::LastMoved,
        }
    }
}

/// Parameters for a sell-side bulk move.
#[derive(Debug, Clone, Copy)]
pub struct MoveAllSellsParams {
    pub(crate) cmd_type: MoveAllCmdType,
    pub(crate) move_kind: ReplaceMultiKind,
    pub(crate) price: f64,
    pub(crate) price_zone: PriceZone,
    pub(crate) side: FixedPosition,
}

impl MoveAllSellsParams {
    pub fn replace_kind(move_kind: BulkMoveKind, price: f64, side: PositionFilter) -> Self {
        Self {
            cmd_type: MoveAllCmdType::MoveKind,
            move_kind: move_kind.to_replace_multi_kind(),
            price,
            price_zone: PriceZone::default(),
            side: side.to_fixed_position(),
        }
    }

    pub fn price_zone(min_price: f64, max_price: f64, side: PositionFilter) -> Self {
        Self {
            cmd_type: MoveAllCmdType::PriceZone,
            move_kind: ReplaceMultiKind::None,
            price: 0.0,
            price_zone: PriceZone {
                min_p: min_price,
                max_p: max_price,
            },
            side: side.to_fixed_position(),
        }
    }

    pub fn percent(percent: f64) -> Self {
        Self {
            cmd_type: MoveAllCmdType::Pers,
            move_kind: ReplaceMultiKind::None,
            price: percent,
            price_zone: PriceZone::default(),
            side: FixedPosition::Both,
        }
    }
}

/// Parameters for a buy-side bulk move.
#[derive(Debug, Clone, Copy)]
pub struct MoveAllBuysParams {
    pub(crate) cmd_type: MoveAllBuysCmdType,
    pub(crate) move_kind: ReplaceMultiKind,
    pub(crate) price: f64,
    pub(crate) side: FixedPosition,
}

impl MoveAllBuysParams {
    pub fn replace_kind(move_kind: BulkMoveKind, price: f64, side: PositionFilter) -> Self {
        Self {
            cmd_type: MoveAllBuysCmdType::MoveKind,
            move_kind: move_kind.to_replace_multi_kind(),
            price,
            side: side.to_fixed_position(),
        }
    }

    pub fn percent(percent: f64) -> Self {
        Self {
            cmd_type: MoveAllBuysCmdType::Pers,
            move_kind: ReplaceMultiKind::None,
            price: percent,
            side: FixedPosition::Both,
        }
    }
}

/// Exact expanded SQL written to the MoonBot Orders report database.
#[derive(Debug, Clone)]
pub struct ClosedSellOrderReport {
    pub header: BaseCommandHeader,
    pub db_id: i64,
    pub sql: String,
}

impl ClosedSellOrderReport {
    fn read(input: &mut &[u8]) -> Option<Self> {
        let header = BaseCommandHeader::read(input)?;
        if input.len() < 8 {
            return None;
        }
        let db_id = i64::from_le_bytes(input[..8].try_into().ok()?);
        *input = &input[8..];
        let mut pos = 0usize;
        let sql = read_string(input, &mut pos)?;
        *input = &input[pos..];
        Some(Self { header, db_id, sql })
    }
}

#[cfg(test)]
mod tests;
