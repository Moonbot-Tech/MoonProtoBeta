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

use super::registry::{decode_utf8_delphi, write_string, CURRENT_PROTO_CMD_VER};
use std::convert::TryInto;
use zerocopy::byteorder::little_endian::{F32 as LeF32, F64 as LeF64, I64 as LeI64, U64 as LeU64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

mod enums;
pub use enums::{
    FixedPosition, MoveAllBuysCmdType, MoveAllCmdType, OrderType, OrderWorkerStatus,
    ReplaceMultiKind,
};

/// TPriceZone (Vars.pas:73) — packed record: `MinP, MaxP: double`.
#[derive(Debug, Clone, Copy, Default)]
pub struct PriceZone {
    pub min_p: f64,
    pub max_p: f64,
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
struct WirePriceZone {
    min_p: LeF64,
    max_p: LeF64,
}

const PRICE_ZONE_SIZE: usize = std::mem::size_of::<WirePriceZone>();
const _: [(); 16] = [(); PRICE_ZONE_SIZE];

impl PriceZone {
    fn from_wire(wire: WirePriceZone) -> Self {
        Self {
            min_p: wire.min_p.get(),
            max_p: wire.max_p.get(),
        }
    }

    fn to_wire(self) -> WirePriceZone {
        WirePriceZone {
            min_p: LeF64::new(self.min_p),
            max_p: LeF64::new(self.max_p),
        }
    }

    #[cfg(test)]
    fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < PRICE_ZONE_SIZE {
            return None;
        }
        Some(Self::from_wire(
            WirePriceZone::read_from_bytes(&data[..PRICE_ZONE_SIZE]).ok()?,
        ))
    }

    fn read_from_delphi_stream(r: &mut &[u8]) -> Self {
        let bytes = read_zero_tail::<PRICE_ZONE_SIZE>(r);
        let wire = WirePriceZone::read_from_bytes(&bytes).expect("fixed in-memory price zone");
        Self::from_wire(wire)
    }

    fn write_to(self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.to_wire().as_bytes());
    }
}

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

/// TOrderCompact (MarketsU.pas:180, 117 байт packed).
/// Этот тип сериализуется через `ms.Read/Write(BuyOrder, SizeOf(BuyOrder))` —
/// то есть **прямой memcpy** packed struct. Public struct keeps normal Rust
/// fields; private `WireOrderCompact` below owns the Delphi packed layout.
#[derive(Debug, Clone, Copy, Default)]
pub struct OrderCompact {
    pub int_id: i64,             // 8
    pub quantity: f64,           // 8
    pub quantity_remaining: f64, // 8
    pub total_btc: f64,          // 8
    pub spent_btc: f64,          // 8
    pub open_time: f64,          // 8  TDateTime
    pub close_time: f64,         // 8  TDateTime
    pub actual_price: f64,       // 8
    pub mean_price: f64,         // 8
    pub quantity_base: f64,      // 8
    pub actual_q: f64,           // 8
    pub tmp_btc: f64,            // 8
    pub create_time: f64,        // 8  TDateTime
    pub panic_sell_down: f32,    // 4
    pub order_type: u8,          // 1  TOrderType
    pub sub_type: u8,            // 1  TOrderSubType
    pub stop_flag: u8,           // 1
    pub partial_done: u8,        // 1
    pub leverage: u8,            // 1
    pub is_opened: u8,           // 1  boolean
    pub is_closed: u8,           // 1
    pub canceled: u8,            // 1
    pub is_short: u8,            // 1
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
struct WireOrderCompact {
    int_id: LeI64,
    quantity: LeF64,
    quantity_remaining: LeF64,
    total_btc: LeF64,
    spent_btc: LeF64,
    open_time: LeF64,
    close_time: LeF64,
    actual_price: LeF64,
    mean_price: LeF64,
    quantity_base: LeF64,
    actual_q: LeF64,
    tmp_btc: LeF64,
    create_time: LeF64,
    panic_sell_down: LeF32,
    order_type: u8,
    sub_type: u8,
    stop_flag: u8,
    partial_done: u8,
    leverage: u8,
    is_opened: u8,
    is_closed: u8,
    canceled: u8,
    is_short: u8,
}

/// Размер `TOrderCompact` в байтах wire-format'а.
/// 13×8 + 4 + 9×1 = 117 байт (matches Delphi комментарий "~117 байт").
pub const ORDER_COMPACT_SIZE: usize = std::mem::size_of::<WireOrderCompact>();
const _: [(); 117] = [(); ORDER_COMPACT_SIZE];

impl OrderCompact {
    fn from_wire(wire: WireOrderCompact) -> Self {
        Self {
            int_id: wire.int_id.get(),
            quantity: wire.quantity.get(),
            quantity_remaining: wire.quantity_remaining.get(),
            total_btc: wire.total_btc.get(),
            spent_btc: wire.spent_btc.get(),
            open_time: wire.open_time.get(),
            close_time: wire.close_time.get(),
            actual_price: wire.actual_price.get(),
            mean_price: wire.mean_price.get(),
            quantity_base: wire.quantity_base.get(),
            actual_q: wire.actual_q.get(),
            tmp_btc: wire.tmp_btc.get(),
            create_time: wire.create_time.get(),
            panic_sell_down: wire.panic_sell_down.get(),
            order_type: wire.order_type,
            sub_type: wire.sub_type,
            stop_flag: wire.stop_flag,
            partial_done: wire.partial_done,
            leverage: wire.leverage,
            is_opened: wire.is_opened,
            is_closed: wire.is_closed,
            canceled: wire.canceled,
            is_short: wire.is_short,
        }
    }

    fn to_wire(self) -> WireOrderCompact {
        WireOrderCompact {
            int_id: LeI64::new(self.int_id),
            quantity: LeF64::new(self.quantity),
            quantity_remaining: LeF64::new(self.quantity_remaining),
            total_btc: LeF64::new(self.total_btc),
            spent_btc: LeF64::new(self.spent_btc),
            open_time: LeF64::new(self.open_time),
            close_time: LeF64::new(self.close_time),
            actual_price: LeF64::new(self.actual_price),
            mean_price: LeF64::new(self.mean_price),
            quantity_base: LeF64::new(self.quantity_base),
            actual_q: LeF64::new(self.actual_q),
            tmp_btc: LeF64::new(self.tmp_btc),
            create_time: LeF64::new(self.create_time),
            panic_sell_down: LeF32::new(self.panic_sell_down),
            order_type: self.order_type,
            sub_type: self.sub_type,
            stop_flag: self.stop_flag,
            partial_done: self.partial_done,
            leverage: self.leverage,
            is_opened: self.is_opened,
            is_closed: self.is_closed,
            canceled: self.canceled,
            is_short: self.is_short,
        }
    }

    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < ORDER_COMPACT_SIZE {
            return None;
        }
        Some(Self::from_wire(
            WireOrderCompact::read_from_bytes(&data[..ORDER_COMPACT_SIZE]).ok()?,
        ))
    }

    fn read_from_delphi_stream(r: &mut &[u8]) -> Self {
        let bytes = read_zero_tail::<ORDER_COMPACT_SIZE>(r);
        let wire =
            WireOrderCompact::read_from_bytes(&bytes).expect("fixed in-memory order compact");
        Self::from_wire(wire)
    }

    pub fn write_to(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.to_wire().as_bytes());
    }

    /// Применить временное смещение к временным полям. ServerTimeDelta = InitialTime - Now.
    /// Delphi корректирует только валидные `TDateTime` values (`> 1`).
    pub fn adjust_time(&mut self, delta: f64) {
        if self.open_time > 1.0 {
            self.open_time -= delta;
        }
        if self.close_time > 1.0 {
            self.close_time -= delta;
        }
        if self.create_time > 1.0 {
            self.create_time -= delta;
        }
    }
}

/// TStopSettings (MarketsU.pas:215, packed record, 46 байт).
#[derive(Debug, Clone, Copy, Default)]
pub struct StopSettings {
    pub stop_loss_on: u8,        // 1
    pub sl_fixed: u8,            // 1
    pub sl_level: f64,           // 8
    pub sl_spread: f64,          // 8
    pub trailing_on: u8,         // 1
    pub trailing_fixed: u8,      // 1
    pub trailing_level: f64,     // 8
    pub ts_spread: f64,          // 8
    pub use_take_profit: u8,     // 1
    pub take_profit: f64,        // 8
    pub take_profit_changed: u8, // 1
}

/// Wire-size TStopSettings: 6 + 5*8 = 46 байт.
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
struct WireStopSettings {
    stop_loss_on: u8,
    sl_fixed: u8,
    sl_level: LeF64,
    sl_spread: LeF64,
    trailing_on: u8,
    trailing_fixed: u8,
    trailing_level: LeF64,
    ts_spread: LeF64,
    use_take_profit: u8,
    take_profit: LeF64,
    take_profit_changed: u8,
}

pub const STOP_SETTINGS_SIZE: usize = std::mem::size_of::<WireStopSettings>();
const _: [(); 46] = [(); STOP_SETTINGS_SIZE];

impl PartialEq for StopSettings {
    fn eq(&self, other: &Self) -> bool {
        self.stop_loss_on == other.stop_loss_on
            && self.sl_fixed == other.sl_fixed
            && self.sl_level.to_bits() == other.sl_level.to_bits()
            && self.sl_spread.to_bits() == other.sl_spread.to_bits()
            && self.trailing_on == other.trailing_on
            && self.trailing_fixed == other.trailing_fixed
            && self.trailing_level.to_bits() == other.trailing_level.to_bits()
            && self.ts_spread.to_bits() == other.ts_spread.to_bits()
            && self.use_take_profit == other.use_take_profit
            && self.take_profit.to_bits() == other.take_profit.to_bits()
            && self.take_profit_changed == other.take_profit_changed
    }
}

impl StopSettings {
    fn from_wire(wire: WireStopSettings) -> Self {
        Self {
            stop_loss_on: wire.stop_loss_on,
            sl_fixed: wire.sl_fixed,
            sl_level: wire.sl_level.get(),
            sl_spread: wire.sl_spread.get(),
            trailing_on: wire.trailing_on,
            trailing_fixed: wire.trailing_fixed,
            trailing_level: wire.trailing_level.get(),
            ts_spread: wire.ts_spread.get(),
            use_take_profit: wire.use_take_profit,
            take_profit: wire.take_profit.get(),
            take_profit_changed: wire.take_profit_changed,
        }
    }

    fn to_wire(self) -> WireStopSettings {
        WireStopSettings {
            stop_loss_on: self.stop_loss_on,
            sl_fixed: self.sl_fixed,
            sl_level: LeF64::new(self.sl_level),
            sl_spread: LeF64::new(self.sl_spread),
            trailing_on: self.trailing_on,
            trailing_fixed: self.trailing_fixed,
            trailing_level: LeF64::new(self.trailing_level),
            ts_spread: LeF64::new(self.ts_spread),
            use_take_profit: self.use_take_profit,
            take_profit: LeF64::new(self.take_profit),
            take_profit_changed: self.take_profit_changed,
        }
    }

    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < STOP_SETTINGS_SIZE {
            return None;
        }
        Some(Self::from_wire(
            WireStopSettings::read_from_bytes(&data[..STOP_SETTINGS_SIZE]).ok()?,
        ))
    }

    fn read_from_delphi_stream(r: &mut &[u8]) -> Self {
        let bytes = read_zero_tail::<STOP_SETTINGS_SIZE>(r);
        let wire =
            WireStopSettings::read_from_bytes(&bytes).expect("fixed in-memory stop settings");
        Self::from_wire(wire)
    }

    pub fn write_to(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.to_wire().as_bytes());
    }
}

/// TOrderUpdateData (MarketsU.pas:263, packed record, 66 байт).
#[derive(Debug, Clone, Copy, Default)]
pub struct OrderUpdateData {
    pub int_id: i64,             // 8
    pub actual_price: f64,       // 8
    pub open_time: f64,          // 8  TDateTime
    pub quantity: f64,           // 8
    pub quantity_remaining: f64, // 8
    pub actual_q: f64,           // 8
    pub total_btc: f64,          // 8
    pub mean_price: f64,         // 8
    pub partial_done: u8,        // 1
    pub stop_flag: u8,           // 1
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
struct WireOrderUpdateData {
    int_id: LeI64,
    actual_price: LeF64,
    open_time: LeF64,
    quantity: LeF64,
    quantity_remaining: LeF64,
    actual_q: LeF64,
    total_btc: LeF64,
    mean_price: LeF64,
    partial_done: u8,
    stop_flag: u8,
}

pub const ORDER_UPDATE_DATA_SIZE: usize = std::mem::size_of::<WireOrderUpdateData>();
const _: [(); 66] = [(); ORDER_UPDATE_DATA_SIZE];

impl OrderUpdateData {
    fn from_wire(wire: WireOrderUpdateData) -> Self {
        Self {
            int_id: wire.int_id.get(),
            actual_price: wire.actual_price.get(),
            open_time: wire.open_time.get(),
            quantity: wire.quantity.get(),
            quantity_remaining: wire.quantity_remaining.get(),
            actual_q: wire.actual_q.get(),
            total_btc: wire.total_btc.get(),
            mean_price: wire.mean_price.get(),
            partial_done: wire.partial_done,
            stop_flag: wire.stop_flag,
        }
    }

    fn to_wire(self) -> WireOrderUpdateData {
        WireOrderUpdateData {
            int_id: LeI64::new(self.int_id),
            actual_price: LeF64::new(self.actual_price),
            open_time: LeF64::new(self.open_time),
            quantity: LeF64::new(self.quantity),
            quantity_remaining: LeF64::new(self.quantity_remaining),
            actual_q: LeF64::new(self.actual_q),
            total_btc: LeF64::new(self.total_btc),
            mean_price: LeF64::new(self.mean_price),
            partial_done: self.partial_done,
            stop_flag: self.stop_flag,
        }
    }

    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < ORDER_UPDATE_DATA_SIZE {
            return None;
        }
        Some(Self::from_wire(
            WireOrderUpdateData::read_from_bytes(&data[..ORDER_UPDATE_DATA_SIZE]).ok()?,
        ))
    }

    fn read_from_delphi_stream(r: &mut &[u8]) -> Self {
        let bytes = read_zero_tail::<ORDER_UPDATE_DATA_SIZE>(r);
        let wire =
            WireOrderUpdateData::read_from_bytes(&bytes).expect("fixed in-memory order update");
        Self::from_wire(wire)
    }

    pub fn write_to(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.to_wire().as_bytes());
    }

    pub fn adjust_time(&mut self, delta: f64) {
        if self.open_time > 1.0 {
            self.open_time -= delta;
        }
    }
}

/// TImmuneItem (TradeStruct.pas:210, packed) — UID:u64 + Value:bool.
#[derive(Debug, Clone, Copy)]
pub struct ImmuneItem {
    pub uid: u64,
    pub value: bool,
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
struct WireImmuneItem {
    uid: LeU64,
    value: u8,
}

const IMMUNE_ITEM_SIZE: usize = std::mem::size_of::<WireImmuneItem>();
const _: [(); 9] = [(); IMMUNE_ITEM_SIZE];

impl ImmuneItem {
    fn from_wire(wire: WireImmuneItem) -> Self {
        Self {
            uid: wire.uid.get(),
            value: wire.value != 0,
        }
    }

    fn to_wire(self) -> WireImmuneItem {
        WireImmuneItem {
            uid: LeU64::new(self.uid),
            value: self.value as u8,
        }
    }

    fn write_to(self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.to_wire().as_bytes());
    }
}

fn read_zero_tail<const N: usize>(r: &mut &[u8]) -> [u8; N] {
    let mut out = [0u8; N];
    let n = r.len().min(N);
    if n > 0 {
        out[..n].copy_from_slice(&r[..n]);
        *r = &r[n..];
    }
    out
}

fn read_u8_zero_tail(r: &mut &[u8]) -> u8 {
    read_zero_tail::<1>(r)[0]
}

fn read_u16_zero_tail(r: &mut &[u8]) -> u16 {
    u16::from_le_bytes(read_zero_tail::<2>(r))
}

fn read_i32_zero_tail(r: &mut &[u8]) -> i32 {
    i32::from_le_bytes(read_zero_tail::<4>(r))
}

fn read_u64_zero_tail(r: &mut &[u8]) -> u64 {
    u64::from_le_bytes(read_zero_tail::<8>(r))
}

fn read_f32_zero_tail(r: &mut &[u8]) -> f32 {
    f32::from_le_bytes(read_zero_tail::<4>(r))
}

fn read_f64_zero_tail(r: &mut &[u8]) -> f64 {
    f64::from_le_bytes(read_zero_tail::<8>(r))
}

fn read_immune_item_zero_tail(r: &mut &[u8]) -> ImmuneItem {
    let bytes = read_zero_tail::<IMMUNE_ITEM_SIZE>(r);
    let wire = WireImmuneItem::read_from_bytes(&bytes).expect("fixed in-memory immune item");
    ImmuneItem::from_wire(wire)
}

// ============================================================================
//  Базовый header команды (TBaseCommand + TBaseMarketCommand + TTradeEpochCommand)
// ============================================================================

/// Базовый header `TBaseCommand`: cmd_id(1) + ver(2) + UID(8) = 11 байт.
#[derive(Debug, Clone, Copy)]
pub struct BaseCommandHeader {
    pub cmd_id: u8,
    pub ver: u16,
    pub uid: u64,
}

impl BaseCommandHeader {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        if r.len() < 11 {
            return None;
        }
        let cmd_id = r[0];
        let ver = u16::from_le_bytes([r[1], r[2]]);
        let uid = u64::from_le_bytes(r[3..11].try_into().unwrap());
        *r = &r[11..];
        Some(Self { cmd_id, ver, uid })
    }

    pub fn write(&self, out: &mut Vec<u8>) {
        out.push(self.cmd_id);
        out.extend_from_slice(&self.ver.to_le_bytes());
        out.extend_from_slice(&self.uid.to_le_bytes());
    }
}

/// Header `TBaseMarketCommand`: header + currency:u8 + platform:u8 + market_name:UTF8.
/// market_name resolves к market_index при apply в state.
#[derive(Debug, Clone)]
pub struct MarketCommandHeader {
    pub base: BaseCommandHeader,
    pub currency: u8,
    pub platform: u8,
    pub market_name: String,
}

impl MarketCommandHeader {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        let base = BaseCommandHeader::read(r)?;
        if r.len() < 2 {
            return None;
        }
        let currency = r[0];
        let platform = r[1];
        *r = &r[2..];
        let market_name = read_str(r)?;
        Some(Self {
            base,
            currency,
            platform,
            market_name,
        })
    }

    pub fn write(&self, out: &mut Vec<u8>, base_currency: u8, base_platform: u8) {
        self.base.write(out);
        out.push(base_currency);
        out.push(base_platform);
        write_string(out, &self.market_name);
    }
}

/// Header `TTradeEpochCommand`: market_header + epoch:u16 + status:u8.
#[derive(Debug, Clone)]
pub struct TradeEpochHeader {
    pub market: MarketCommandHeader,
    pub epoch: u16,
    pub status: OrderWorkerStatus,
}

impl TradeEpochHeader {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        let market = MarketCommandHeader::read(r)?;
        let epoch = read_u16_zero_tail(r);
        let status = OrderWorkerStatus::from_byte(read_u8_zero_tail(r));
        Some(Self {
            market,
            epoch,
            status,
        })
    }

    pub fn write(&self, out: &mut Vec<u8>, base_currency: u8, base_platform: u8) {
        self.market.write(out, base_currency, base_platform);
        out.extend_from_slice(&self.epoch.to_le_bytes());
        out.push(self.status.to_byte());
    }
}

fn read_str(r: &mut &[u8]) -> Option<String> {
    if r.len() < 2 {
        return None;
    }
    let len = u16::from_le_bytes([r[0], r[1]]) as usize;
    if r.len() < 2 + len {
        return None;
    }
    let s = decode_utf8_delphi(&r[2..2 + len]);
    *r = &r[2 + len..];
    Some(s)
}

// ============================================================================
//  Распарсенная команда (enum TradeCommand)
// ============================================================================

/// Все распарсенные TBaseTradeCommand подкоманды (CmdId маппинг → variant).
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
    /// CmdId=9: TAllStatusesReq — запрос на получение всех ордеров (client→server).
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
//  CmdId=25: TOrderTracePoint
// ============================================================================

/// Trace flags (TradeStruct.pas:234): bit0=IsTemp, bit1=IsFinish, bit2=IsInitial.
pub mod trace_flags {
    pub const IS_TEMP: u8 = 0x01;
    pub const IS_FINISH: u8 = 0x02;
    pub const IS_INITIAL: u8 = 0x04;
}

/// `TOrderTracePoint` (TradeStruct.pas:237-252).
#[derive(Debug, Clone)]
pub struct OrderTracePoint {
    pub market: MarketCommandHeader,
    pub trace_time: f64, // TDateTime
    pub trace_price: f32,
    pub base_price: f32,
    pub stop_price: f32,
    pub ord_type: OrderType,
    pub flags: u8,
}

impl OrderTracePoint {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        let market = MarketCommandHeader::read(r)?;
        let trace_time = read_f64_zero_tail(r);
        let trace_price = read_f32_zero_tail(r);
        let base_price = read_f32_zero_tail(r);
        let stop_price = read_f32_zero_tail(r);
        let ord_type = OrderType::from_byte(read_u8_zero_tail(r));
        let flags = read_u8_zero_tail(r);
        Some(Self {
            market,
            trace_time,
            trace_price,
            base_price,
            stop_price,
            ord_type,
            flags,
        })
    }

    pub fn is_temp(&self) -> bool {
        (self.flags & trace_flags::IS_TEMP) != 0
    }
    pub fn is_finish(&self) -> bool {
        (self.flags & trace_flags::IS_FINISH) != 0
    }
    pub fn is_initial(&self) -> bool {
        (self.flags & trace_flags::IS_INITIAL) != 0
    }

    pub fn adjust_time(&mut self, delta: f64) {
        self.trace_time -= delta;
    }
}

// ============================================================================
//  CmdId=26: TCorridorUpdate
// ============================================================================

/// `TCorridorUpdate` (TradeStruct.pas:255-262). Priority=Low.
#[derive(Debug, Clone)]
pub struct CorridorUpdate {
    pub market: MarketCommandHeader,
    pub price_down: f32,
    pub price_up: f32,
}

impl CorridorUpdate {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        let market = MarketCommandHeader::read(r)?;
        let price_down = read_f32_zero_tail(r);
        let price_up = read_f32_zero_tail(r);
        Some(Self {
            market,
            price_down,
            price_up,
        })
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
//  CmdId=28: TBulkReplaceNotify
// ============================================================================

/// `TBulkReplaceNotify` (TradeStruct.pas:275-284).
/// Уведомление: эти UID'ы массово replace'нуты (UI должна показать как "перемещаются").
#[derive(Debug, Clone)]
pub struct BulkReplaceNotify {
    pub market: MarketCommandHeader,
    pub order_type: OrderType,
    pub uids: Vec<u64>,
}

impl BulkReplaceNotify {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        let market = MarketCommandHeader::read(r)?;
        let order_type = OrderType::from_byte(read_u8_zero_tail(r));
        let count = read_u16_zero_tail(r) as usize;
        let mut uids = Vec::with_capacity(count);
        for _ in 0..count {
            uids.push(read_u64_zero_tail(r));
        }
        Some(Self {
            market,
            order_type,
            uids,
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

// ============================================================================
//  Builders для исходящих команд (client → server)
// ============================================================================

fn write_base_command_header(out: &mut Vec<u8>, cmd_id: u8, uid: u64) {
    out.push(cmd_id);
    out.extend_from_slice(&CURRENT_PROTO_CMD_VER.to_le_bytes());
    out.extend_from_slice(&uid.to_le_bytes());
}

fn write_market_header(
    out: &mut Vec<u8>,
    cmd_id: u8,
    uid: u64,
    market_name: &str,
    currency: u8,
    platform: u8,
) {
    write_base_command_header(out, cmd_id, uid);
    out.push(currency);
    out.push(platform);
    write_string(out, market_name);
}

fn write_trade_epoch_header(
    out: &mut Vec<u8>,
    cmd_id: u8,
    ctx: TradeCtx,
    market_name: &str,
    epoch: u16,
    status: OrderWorkerStatus,
) {
    write_market_header(
        out,
        cmd_id,
        ctx.uid,
        market_name,
        ctx.currency,
        ctx.platform,
    );
    out.extend_from_slice(&epoch.to_le_bytes());
    out.push(status.to_byte());
}

/// Route fields shared by client-originated trade command builders.
///
/// Regular applications should obtain this from [`crate::Client::trade_ctx`],
/// [`crate::Client::random_trade_ctx`], or from tracked order state via
/// [`crate::state::Order::trade_ctx`]. Low-level protocol tools can use
/// [`TradeCtx::with_route`] when they intentionally provide raw Delphi enum
/// ordinals themselves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeCtx {
    pub uid: u64,
    pub currency: u8,
    pub platform: u8,
}

impl TradeCtx {
    /// Build a context with explicit Delphi route ordinals.
    ///
    /// `currency` is `Ord(cfg.BaseCurrency)` and `platform` is
    /// `Ord(cfg.Header.Current)` on the server. Prefer the higher-level helpers
    /// on [`crate::Client`] unless you are writing a protocol tool.
    pub fn with_route(uid: u64, currency: u8, platform: u8) -> Self {
        Self {
            uid,
            currency,
            platform,
        }
    }
}

/// CmdId=6: построить пакет TOrderReplaceCommand.
///
/// Delphi `TOrderReplaceCommand.Create` always sends `Epoch=0` and
/// `Status=OS_None` for client-originated replace commands.
pub fn build_order_replace(
    ctx: TradeCtx,
    market_name: &str,
    order_type: OrderType,
    new_price: f64,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    write_trade_epoch_header(&mut out, 6, ctx, market_name, 0, OrderWorkerStatus::None);
    out.push(order_type.to_byte());
    out.extend_from_slice(&new_price.to_le_bytes());
    out
}

/// CmdId=9: запрос всех ордеров.
pub fn build_all_statuses_request(uid: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(11);
    write_base_command_header(&mut out, 9, uid);
    out
}

/// CmdId=10: TOrderCancelCommand — отмена ордера.
pub fn build_order_cancel(
    ctx: TradeCtx,
    market_name: &str,
    epoch: u16,
    status: OrderWorkerStatus,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_trade_epoch_header(&mut out, 10, ctx, market_name, epoch, status);
    out
}

/// CmdId=11: TJoinOrdersCommand.
pub fn build_join_orders(ctx: TradeCtx, market_name: &str, is_short: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_market_header(
        &mut out,
        11,
        ctx.uid,
        market_name,
        ctx.currency,
        ctx.platform,
    );
    out.push(is_short as u8);
    out
}

/// CmdId=12: TSplitOrderCommand.
pub fn build_split_order(
    ctx: TradeCtx,
    market_name: &str,
    split_parts: i32,
    split_small: bool,
    split_small_sell: bool,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_market_header(
        &mut out,
        12,
        ctx.uid,
        market_name,
        ctx.currency,
        ctx.platform,
    );
    out.extend_from_slice(&split_parts.to_le_bytes());
    out.push(split_small as u8);
    out.push(split_small_sell as u8);
    out
}

/// CmdId=13: TMoveAllSellsCommand.
pub fn build_move_all_sells(
    ctx: TradeCtx,
    market_name: &str,
    params: MoveAllSellsParams,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(48);
    write_market_header(
        &mut out,
        13,
        ctx.uid,
        market_name,
        ctx.currency,
        ctx.platform,
    );
    out.push(params.cmd_type.to_byte());
    out.push(params.move_kind.to_byte());
    out.extend_from_slice(&params.price.to_le_bytes());
    params.price_zone.write_to(&mut out);
    out.push(params.side.to_byte());
    out
}

/// CmdId=14: TDoClosePositionCommand.
pub fn build_do_close_position(ctx: TradeCtx, market_name: &str, market_sell: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_market_header(
        &mut out,
        14,
        ctx.uid,
        market_name,
        ctx.currency,
        ctx.platform,
    );
    out.push(market_sell as u8);
    out
}

/// CmdId=15: TDoLimitClosePositionCommand (= JoinOrdersCommand format).
pub fn build_do_limit_close_position(ctx: TradeCtx, market_name: &str, is_short: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_market_header(
        &mut out,
        15,
        ctx.uid,
        market_name,
        ctx.currency,
        ctx.platform,
    );
    out.push(is_short as u8);
    out
}

/// CmdId=16: TDoSplitPositionCommand (= JoinOrdersCommand format).
pub fn build_do_split_position(ctx: TradeCtx, market_name: &str, is_short: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_market_header(
        &mut out,
        16,
        ctx.uid,
        market_name,
        ctx.currency,
        ctx.platform,
    );
    out.push(is_short as u8);
    out
}

/// CmdId=17: TDoSellOrderCommand.
pub fn build_do_sell_order(ctx: TradeCtx, market_name: &str, price: f64, size: f64) -> Vec<u8> {
    let mut out = Vec::with_capacity(40);
    write_market_header(
        &mut out,
        17,
        ctx.uid,
        market_name,
        ctx.currency,
        ctx.platform,
    );
    out.extend_from_slice(&price.to_le_bytes());
    out.extend_from_slice(&size.to_le_bytes());
    out
}

/// CmdId=18: TOrderStatusRequest — запрос статуса конкретного ордера.
pub fn build_order_status_request(ctx: TradeCtx, market_name: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_trade_epoch_header(&mut out, 18, ctx, market_name, 0, OrderWorkerStatus::None);
    out
}

/// CmdId=20: TOrderStopsUpdate.
pub fn build_order_stops_update(
    ctx: TradeCtx,
    market_name: &str,
    epoch: u16,
    status: OrderWorkerStatus,
    stops: &StopSettings,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    write_trade_epoch_header(&mut out, 20, ctx, market_name, epoch, status);
    stops.write_to(&mut out);
    out
}

/// CmdId=21: TTurnPanicSellCommand.
///
/// Delphi `TTurnPanicSellCommand.Create` does not set inherited
/// `TTradeEpochCommand` fields on the client path, so object zero-init gives
/// `Epoch=0` and `Status=OS_None`.
pub fn build_turn_panic_sell(ctx: TradeCtx, market_name: &str, turn_on: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_trade_epoch_header(&mut out, 21, ctx, market_name, 0, OrderWorkerStatus::None);
    out.push(turn_on as u8);
    out
}

/// CmdId=22: TSetImmuneCommand.
pub fn build_set_immune(uid: u64, items: &[ImmuneItem]) -> Vec<u8> {
    let wire_count = items.len() as u8;
    let wire_items = &items[..wire_count as usize];
    let mut out = Vec::with_capacity(11 + 1 + wire_items.len() * 9);
    write_base_command_header(&mut out, 22, uid);
    out.push(wire_count);
    for it in wire_items {
        it.write_to(&mut out);
    }
    out
}

/// CmdId=27: TMoveAllBuysCommand.
pub fn build_move_all_buys(
    ctx: TradeCtx,
    market_name: &str,
    cmd_type: MoveAllBuysCmdType,
    move_kind: ReplaceMultiKind,
    price: f64,
    side: FixedPosition,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(40);
    write_market_header(
        &mut out,
        27,
        ctx.uid,
        market_name,
        ctx.currency,
        ctx.platform,
    );
    out.push(cmd_type.to_byte());
    out.push(move_kind.to_byte());
    out.extend_from_slice(&price.to_le_bytes());
    out.push(side.to_byte());
    out
}

/// CmdId=29: TVStopUpdate.
pub fn build_vstop_update(
    ctx: TradeCtx,
    market_name: &str,
    epoch: u16,
    params: VStopUpdateParams,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(48);
    write_trade_epoch_header(&mut out, 29, ctx, market_name, epoch, params.status);
    out.push(params.vstop_on as u8);
    out.push(params.vstop_fixed as u8);
    out.extend_from_slice(&params.vstop_level.to_le_bytes());
    out.extend_from_slice(&params.vstop_vol.to_le_bytes());
    out
}

/// CmdId=30: TDoMarketSplitPositionCommand (= JoinOrdersCommand format).
pub fn build_do_market_split_position(ctx: TradeCtx, market_name: &str, is_short: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_market_header(
        &mut out,
        30,
        ctx.uid,
        market_name,
        ctx.currency,
        ctx.platform,
    );
    out.push(is_short as u8);
    out
}

/// CmdId=23: TPenaltyCommand — пометить маркет penalty (cooldown).
/// Аудит docs_api B-04: команда вызывается в TaskWorkers.pas:8361, Unit1.pas:11859/23750.
pub fn build_penalty(ctx: TradeCtx, market_name: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_market_header(
        &mut out,
        23,
        ctx.uid,
        market_name,
        ctx.currency,
        ctx.platform,
    );
    out
}

/// CmdId=3: TNewOrderCommand — запрос на создание нового ордера.
pub fn build_new_order(
    ctx: TradeCtx,
    market_name: &str,
    is_short: bool,
    price: f64,
    strat_id: u64,
    order_size: f64,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(48);
    write_market_header(
        &mut out,
        3,
        ctx.uid,
        market_name,
        ctx.currency,
        ctx.platform,
    );
    out.push(is_short as u8);
    out.extend_from_slice(&price.to_le_bytes());
    out.extend_from_slice(&strat_id.to_le_bytes());
    out.extend_from_slice(&order_size.to_le_bytes());
    out
}

#[cfg(test)]
mod tests;
