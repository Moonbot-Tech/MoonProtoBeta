//! MPC_Order channel — все 30 подкоманд TBaseTradeCommand.
//!
//! Источник Delphi: `X:\proj-X\MoonBot\src\MoonProto\MoonProtoTradeStruct.pas` (966 строк).
//!
//! ## Архитектура канала
//!
//! Каждая команда имеет иерархию:
//! - `TBaseCommand` — `cmd_id(1) + ver(2) + UID(8)` = 11 байт header.
//! - `TBaseTradeCommand` extends → CmdClass = MPC_Order (CmdId=0).
//! - `TBaseMarketCommand` extends → + `currency(1) + platform(1) + market_name:UTF8`.
//! - `TTradeEpochCommand` extends `TBaseMarketCommand` → + `epoch:u16 + status:u8`.
//!
//! Wire-format каждой подкоманды строится байт-за-байтом, начиная с inherited.
//!
//! ## Замечание о POrder / TOrderCompact / TStopSettings / TOrderUpdateData
//!
//! Эти Delphi structures — `packed record` без выравнивания. Публичные Rust-типы
//! держат обычные поля API/state, а приватные `Wire*` structs ниже зеркалят
//! fixed wire layout с compile-time проверкой размера.

use super::registry::{write_string, CURRENT_PROTO_CMD_VER};
use std::convert::TryInto;

mod builders;
pub use builders::{
    build_all_statuses_request, build_do_close_position, build_do_limit_close_position,
    build_do_market_split_position, build_do_sell_order, build_do_split_position,
    build_join_orders, build_move_all_buys, build_move_all_sells, build_new_order,
    build_order_cancel, build_order_replace, build_order_status_request, build_order_stops_update,
    build_penalty, build_set_immune, build_split_order, build_turn_panic_sell, build_vstop_update,
    TradeCtx,
};
#[cfg(test)]
use builders::{write_base_command_header, write_market_header};
mod command;
pub use command::TradeCommand;
mod enums;
pub use enums::{
    FixedPosition, MoveAllBuysCmdType, MoveAllCmdType, OrderType, OrderWorkerStatus,
    ReplaceMultiKind,
};
mod headers;
pub use headers::{BaseCommandHeader, MarketCommandHeader, TradeEpochHeader};
mod records;
use records::{
    read_f32_zero_tail, read_f64_zero_tail, read_i32_zero_tail, read_immune_item_zero_tail,
    read_u16_zero_tail, read_u64_zero_tail, read_u8_zero_tail,
};
pub use records::{
    ImmuneItem, OrderCompact, OrderUpdateData, PriceZone, StopSettings, ORDER_COMPACT_SIZE,
    ORDER_UPDATE_DATA_SIZE, STOP_SETTINGS_SIZE,
};
mod trace;
pub use trace::{trace_flags, BulkReplaceNotify, CorridorUpdate, OrderTracePoint};

/// Parameters for `TMoveAllSellsCommand`.
///
/// Keeping the option set in a named struct makes `Client::move_all_sells` and
/// `build_move_all_sells` harder to call with swapped `price` / `zone` / `side`
/// arguments.
#[derive(Debug, Clone, Copy)]
pub struct MoveAllSellsParams {
    pub cmd_type: MoveAllCmdType,
    pub move_kind: ReplaceMultiKind,
    pub price: f64,
    pub price_zone: PriceZone,
    pub side: FixedPosition,
}

/// Parameters for raw `TVStopUpdate` builders.
///
/// High-level client wrappers derive `status` from the local `Orders` state,
/// matching Delphi `BOrderWorker.SendVStopIfChanged`. Low-level builders keep
/// `epoch` and `status` explicit for protocol tests and replay tools.
#[derive(Debug, Clone, Copy)]
pub struct VStopUpdateParams {
    pub status: OrderWorkerStatus,
    pub vstop_on: bool,
    pub vstop_fixed: bool,
    pub vstop_level: f64,
    pub vstop_vol: f64,
}

// ============================================================================
//  CmdId=3: TNewOrderCommand
// ============================================================================

/// `TNewOrderCommand` (TradeStruct.pas:44-53).
/// Запрос клиента на создание нового ордера.
#[derive(Debug, Clone)]
pub struct NewOrderCommand {
    pub market: MarketCommandHeader,
    pub is_short: bool,
    pub price: f64,
    pub strat_id: u64,
    pub order_size: f64,
}

impl NewOrderCommand {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        let market = MarketCommandHeader::read(r)?;
        let is_short = read_u8_zero_tail(r) != 0;
        let price = read_f64_zero_tail(r);
        let strat_id = read_u64_zero_tail(r);
        let order_size = read_f64_zero_tail(r);
        Some(Self {
            market,
            is_short,
            price,
            strat_id,
            order_size,
        })
    }
}

// ============================================================================
//  CmdId=4: TOrderStatus
// ============================================================================

/// `TOrderStatus` (TradeStruct.pas:55-70).
/// Полный snapshot одного ордера. UKey=UK_OrderStatus.
#[derive(Debug, Clone)]
pub struct OrderStatus {
    pub epoch_header: TradeEpochHeader,
    pub buy_order: OrderCompact,
    pub sell_order: OrderCompact,
    pub stops: StopSettings,
    pub strat_id: u64,
    pub is_short: bool,
    pub db_id: i32,
    pub from_cache: bool,
    /// v2+
    pub emulator_mode: bool,
    /// v3+
    pub immune_for_clicks: bool,
}

impl OrderStatus {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        let epoch_header = TradeEpochHeader::read(r)?;
        let buy_order = OrderCompact::read_from_delphi_stream(r);
        let sell_order = OrderCompact::read_from_delphi_stream(r);
        let stops = StopSettings::read_from_delphi_stream(r);
        let strat_id = read_u64_zero_tail(r);
        let is_short = read_u8_zero_tail(r) != 0;
        let db_id = read_i32_zero_tail(r);
        let from_cache = read_u8_zero_tail(r) != 0;

        let ver = epoch_header.market.base.ver;
        let mut emulator_mode = false;
        let mut immune_for_clicks = false;

        if ver >= 2 && !r.is_empty() {
            emulator_mode = read_u8_zero_tail(r) != 0;
        }
        if ver >= 3 && !r.is_empty() {
            immune_for_clicks = read_u8_zero_tail(r) != 0;
        }

        Some(Self {
            epoch_header,
            buy_order,
            sell_order,
            stops,
            strat_id,
            is_short,
            db_id,
            from_cache,
            emulator_mode,
            immune_for_clicks,
        })
    }
}

// ============================================================================
//  CmdId=5: TOrderStatusUpdate
// ============================================================================

/// `TOrderStatusUpdate` (TradeStruct.pas:72-80).
/// Delta-update полей ордера. UKey=UK_OrderStatusShort.
#[derive(Debug, Clone)]
pub struct OrderStatusUpdate {
    pub epoch_header: TradeEpochHeader,
    pub update_data: OrderUpdateData,
    /// Soft-read: появилось в v2+. Если отсутствует — = 0.
    pub sell_reason_code: u8,
}

impl OrderStatusUpdate {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        let epoch_header = TradeEpochHeader::read(r)?;
        let update_data = OrderUpdateData::read_from_delphi_stream(r);
        let sell_reason_code = if !r.is_empty() {
            let v = r[0];
            *r = &r[1..];
            v
        } else {
            0
        };
        Some(Self {
            epoch_header,
            update_data,
            sell_reason_code,
        })
    }
}

// ============================================================================
//  CmdId=6: TOrderReplaceCommand
// ============================================================================

/// `TOrderReplaceCommand` (TradeStruct.pas:83-90). UKey=UK_OrderMove.
/// Запрос на перемещение цены ордера.
#[derive(Debug, Clone)]
pub struct OrderReplaceCommand {
    pub epoch_header: TradeEpochHeader,
    pub order_type: OrderType,
    pub new_price: f64,
}

impl OrderReplaceCommand {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        let epoch_header = TradeEpochHeader::read(r)?;
        let order_type = OrderType::from_byte(read_u8_zero_tail(r));
        let new_price = read_f64_zero_tail(r);
        Some(Self {
            epoch_header,
            order_type,
            new_price,
        })
    }
}

// ============================================================================
//  CmdId=7: TOrderReplaceResponse
// ============================================================================

/// `TOrderReplaceResponse` (TradeStruct.pas:92-102). UKey=UK_OrderMove, MaxRetries=4.
#[derive(Debug, Clone)]
pub struct OrderReplaceResponse {
    pub epoch_header: TradeEpochHeader,
    pub order_type: OrderType,
    pub price: f64,
    pub update_data: OrderUpdateData,
    pub quantity_base: f64,
}

impl OrderReplaceResponse {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        let epoch_header = TradeEpochHeader::read(r)?;
        let order_type = OrderType::from_byte(read_u8_zero_tail(r));
        let price = read_f64_zero_tail(r);
        let update_data = OrderUpdateData::read_from_delphi_stream(r);
        let quantity_base = read_f64_zero_tail(r);
        Some(Self {
            epoch_header,
            order_type,
            price,
            update_data,
            quantity_base,
        })
    }
}

// ============================================================================
//  CmdId=8: TAllStatuses
// ============================================================================

/// `TAllStatuses` (TradeStruct.pas:104-114). Priority=Sliced.
/// Снапшот всех активных ордеров — приходит при reconnect.
#[derive(Debug, Clone)]
pub struct AllStatuses {
    pub header: BaseCommandHeader,
    pub orders: Vec<OrderStatus>,
}

impl AllStatuses {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        let header = BaseCommandHeader::read(r)?;
        if r.len() < 4 {
            return None;
        }
        let count_raw = i32::from_le_bytes(r[0..4].try_into().unwrap());
        *r = &r[4..];
        if count_raw <= 0 {
            return Some(Self {
                header,
                orders: Vec::new(),
            });
        }
        let count = count_raw as usize;
        let mut orders = Vec::with_capacity(count.min(r.len() / 11));
        for _ in 0..count {
            if r.is_empty() {
                break;
            }
            // Каждый order пишется через `o.StoreToStream(Stream)` — то есть **сам** включает
            // свой CmdId(1) + ver(2) + UID(8) + ... header. Delphi читает через
            // `TBaseTradeCommand.FromStream(ms)` и затем cast'ит результат к `TOrderStatus`;
            // значит valid nested item обязан быть CmdId=4.
            if r.first().copied() != Some(4) {
                log::warn!(
                    target: "moonproto::trade",
                    "AllStatuses: nested command is not TOrderStatus (cmd_id={:?})",
                    r.first().copied()
                );
                return None;
            }
            if let Some(order) = OrderStatus::read(r) {
                orders.push(order);
            } else {
                break;
            }
        }
        Some(Self { header, orders })
    }
}

// ============================================================================
//  CmdId=10: TOrderCancelCommand
// ============================================================================

/// `TOrderCancelCommand` (TradeStruct.pas:120-123). UKey=UK_OrderMove.
/// Полностью наследует TTradeEpochCommand без дополнительных полей.
pub type OrderCancelCommand = TradeEpochHeaderTyped;

#[derive(Debug, Clone)]
pub struct TradeEpochHeaderTyped {
    pub epoch_header: TradeEpochHeader,
}

impl TradeEpochHeaderTyped {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        Some(Self {
            epoch_header: TradeEpochHeader::read(r)?,
        })
    }
}

// ============================================================================
//  CmdId=11: TJoinOrdersCommand
// ============================================================================

/// `TJoinOrdersCommand` (TradeStruct.pas:125-132).
/// Используется также как base для CmdId 15/16/30 (Do*).
#[derive(Debug, Clone)]
pub struct JoinOrdersCommand {
    pub market: MarketCommandHeader,
    pub is_short: bool,
}

impl JoinOrdersCommand {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        let market = MarketCommandHeader::read(r)?;
        let is_short = read_u8_zero_tail(r) != 0;
        Some(Self { market, is_short })
    }
}

// ============================================================================
//  CmdId=12: TSplitOrderCommand
// ============================================================================

/// `TSplitOrderCommand` (TradeStruct.pas:134-143).
#[derive(Debug, Clone)]
pub struct SplitOrderCommand {
    pub market: MarketCommandHeader,
    pub split_parts: i32,
    pub split_small: bool,
    pub split_small_sell: bool,
}

impl SplitOrderCommand {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        let market = MarketCommandHeader::read(r)?;
        let split_parts = read_i32_zero_tail(r);
        let split_small = read_u8_zero_tail(r) != 0;
        let split_small_sell = read_u8_zero_tail(r) != 0;
        Some(Self {
            market,
            split_parts,
            split_small,
            split_small_sell,
        })
    }
}

// ============================================================================
//  CmdId=13: TMoveAllSellsCommand
// ============================================================================

/// `TMoveAllSellsCommand` (TradeStruct.pas:145-155).
#[derive(Debug, Clone)]
pub struct MoveAllSellsCommand {
    pub market: MarketCommandHeader,
    pub cmd_type: u8,
    pub move_kind: ReplaceMultiKind,
    pub price: f64,
    pub price_zone: PriceZone,
    pub side: FixedPosition,
}

impl MoveAllSellsCommand {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        let market = MarketCommandHeader::read(r)?;
        let cmd_type = read_u8_zero_tail(r);
        let move_kind = ReplaceMultiKind::from_byte(read_u8_zero_tail(r));
        let price = read_f64_zero_tail(r);
        let price_zone = PriceZone::read_from_delphi_stream(r);
        // Soft-read like Delphi: when older payloads have no Side byte, use Both.
        let side = if !r.is_empty() {
            let v = FixedPosition::from_byte(r[0]);
            *r = &r[1..];
            v
        } else {
            FixedPosition::Both
        };
        Some(Self {
            market,
            cmd_type,
            move_kind,
            price,
            price_zone,
            side,
        })
    }
}

// ============================================================================
//  CmdId=14: TDoClosePositionCommand (MaxRetries=1)
// ============================================================================

#[derive(Debug, Clone)]
pub struct DoClosePositionCommand {
    pub market: MarketCommandHeader,
    pub market_sell: bool,
}

impl DoClosePositionCommand {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        let market = MarketCommandHeader::read(r)?;
        let market_sell = read_u8_zero_tail(r) != 0;
        Some(Self {
            market,
            market_sell,
        })
    }
}

// ============================================================================
//  CmdId=17: TDoSellOrderCommand (MaxRetries=1)
// ============================================================================

#[derive(Debug, Clone)]
pub struct DoSellOrderCommand {
    pub market: MarketCommandHeader,
    pub price: f64,
    pub size: f64,
}

impl DoSellOrderCommand {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        let market = MarketCommandHeader::read(r)?;
        let price = read_f64_zero_tail(r);
        let size = read_f64_zero_tail(r);
        Some(Self {
            market,
            price,
            size,
        })
    }
}

// ============================================================================
//  CmdId=20: TOrderStopsUpdate
// ============================================================================

/// `TOrderStopsUpdate` (TradeStruct.pas:193-200). UKey=UK_OrderMove.
#[derive(Debug, Clone)]
pub struct OrderStopsUpdate {
    pub epoch_header: TradeEpochHeader,
    pub stops: StopSettings,
}

impl OrderStopsUpdate {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        let epoch_header = TradeEpochHeader::read(r)?;
        let stops = StopSettings::read_from_delphi_stream(r);
        Some(Self {
            epoch_header,
            stops,
        })
    }
}

// ============================================================================
//  CmdId=21: TTurnPanicSellCommand
// ============================================================================

#[derive(Debug, Clone)]
pub struct TurnPanicSellCommand {
    pub epoch_header: TradeEpochHeader,
    pub turn_on: bool,
}

impl TurnPanicSellCommand {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        let epoch_header = TradeEpochHeader::read(r)?;
        let turn_on = read_u8_zero_tail(r) != 0;
        Some(Self {
            epoch_header,
            turn_on,
        })
    }
}

// ============================================================================
//  CmdId=22: TSetImmuneCommand
// ============================================================================

/// `TSetImmuneCommand` (TradeStruct.pas:210-223). UKey=UK_ImmuneClicks.
/// UKey.UID вычисляется как sum(Items.UID).
#[derive(Debug, Clone)]
pub struct SetImmuneCommand {
    pub header: BaseCommandHeader,
    pub items: Vec<ImmuneItem>,
}

impl SetImmuneCommand {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        let header = BaseCommandHeader::read(r)?;
        if r.is_empty() {
            return None;
        }
        let n = r[0] as usize;
        *r = &r[1..];
        let mut items = Vec::with_capacity(n);
        for _ in 0..n {
            items.push(read_immune_item_zero_tail(r));
        }
        Some(Self { header, items })
    }
}

// ============================================================================
//  CmdId=27: TMoveAllBuysCommand
// ============================================================================

/// `TMoveAllBuysCommand` (TradeStruct.pas:264-273).
/// **NB**: в отличие от TMoveAllSellsCommand, не имеет PriceZone в wire-format.
#[derive(Debug, Clone)]
pub struct MoveAllBuysCommand {
    pub market: MarketCommandHeader,
    pub cmd_type: u8,
    pub move_kind: ReplaceMultiKind,
    pub price: f64,
    pub side: FixedPosition,
}

impl MoveAllBuysCommand {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        let market = MarketCommandHeader::read(r)?;
        let cmd_type = read_u8_zero_tail(r);
        let move_kind = ReplaceMultiKind::from_byte(read_u8_zero_tail(r));
        let price = read_f64_zero_tail(r);
        let side = if !r.is_empty() {
            let v = FixedPosition::from_byte(r[0]);
            *r = &r[1..];
            v
        } else {
            FixedPosition::Both
        };
        Some(Self {
            market,
            cmd_type,
            move_kind,
            price,
            side,
        })
    }
}

// ============================================================================
//  CmdId=29: TVStopUpdate
// ============================================================================

/// `TVStopUpdate` (TradeStruct.pas:286-296). UKey=UK_OrderMove.
#[derive(Debug, Clone)]
pub struct VStopUpdate {
    pub epoch_header: TradeEpochHeader,
    pub vstop_on: bool,
    pub vstop_fixed: bool,
    pub vstop_level: f64,
    pub vstop_vol: f64,
}

impl VStopUpdate {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        let epoch_header = TradeEpochHeader::read(r)?;
        let vstop_on = read_u8_zero_tail(r) != 0;
        let vstop_fixed = read_u8_zero_tail(r) != 0;
        let vstop_level = read_f64_zero_tail(r);
        let vstop_vol = read_f64_zero_tail(r);
        Some(Self {
            epoch_header,
            vstop_on,
            vstop_fixed,
            vstop_level,
            vstop_vol,
        })
    }
}

#[cfg(test)]
mod tests;
