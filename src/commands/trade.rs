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
//! Эти Delphi structures — `packed record` без выравнивания. В Rust они представлены
//! как `#[repr(C, packed)]` структуры с тем же layout (см. `types.rs`).

use super::registry::{write_string, CURRENT_PROTO_CMD_VER};
use std::convert::TryInto;

// ============================================================================
//  Базовые типы (соответствуют Vars.pas / MarketsU.pas packed records)
// ============================================================================

/// TOrderType (Vars.pas:57): O_SELL=0, O_BUY=1, O_BuyStop=2, O_BuyLimit=3.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType {
    Sell = 0,
    Buy = 1,
    BuyStop = 2,
    BuyLimit = 3,
}

impl OrderType {
    /// Возвращает `None` если байт неизвестен — caller должен drop packet + log.
    /// Финансовый enum: silent fallback в Default = silent corruption (A-02).
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::Sell),
            1 => Some(Self::Buy),
            2 => Some(Self::BuyStop),
            3 => Some(Self::BuyLimit),
            _ => None,
        }
    }
}

/// TOrderWorkerStatus (MarketsU.pas:39).
/// State machine: None → BuySet → BuyDone → SellSet → SelLAlmostDone → SelLDone (terminal).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderWorkerStatus {
    None = 0,
    BuyFail = 1,
    BuySet = 2,
    BuyCancel = 3,
    BuyDone = 4,
    SellFail = 5,
    SellSet = 6,
    SellCancel = 7,
    SelLDone = 8,
    SelLAlmostDone = 9,
}

impl OrderWorkerStatus {
    /// Возвращает `None` если байт неизвестен. Финансовый enum — silent fallback opasen (A-02).
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::None),
            1 => Some(Self::BuyFail),
            2 => Some(Self::BuySet),
            3 => Some(Self::BuyCancel),
            4 => Some(Self::BuyDone),
            5 => Some(Self::SellFail),
            6 => Some(Self::SellSet),
            7 => Some(Self::SellCancel),
            8 => Some(Self::SelLDone),
            9 => Some(Self::SelLAlmostDone),
            _ => None,
        }
    }

    /// Terminal status — ордер закрыт, воркер удалить.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::SelLDone | Self::BuyCancel | Self::BuyFail | Self::SellFail | Self::SellCancel)
    }
}

/// TFixedPosition (Vars.pas:52): FP_Both=0, FP_Long=1, FP_Short=2.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FixedPosition {
    Both = 0,
    Long = 1,
    Short = 2,
}

impl FixedPosition {
    /// Возвращает `None` если байт неизвестен (A-02).
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::Both),
            1 => Some(Self::Long),
            2 => Some(Self::Short),
            _ => None,
        }
    }
}

/// TMoveAllCmdType (MoonProtoTradeStruct.pas:148 inline comment).
/// Описывает интерпретацию параметра `Price`/`PriceZone` в `TMoveAllSells/BuysCommand`.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoveAllCmdType {
    /// `MoveKind` — двигать всех по правилу из `ReplaceMultiKind`.
    MoveKind = 0,
    /// `PriceZone` — двигать тех чья цена в зоне `[price_zone.min_p, price_zone.max_p]`.
    PriceZone = 1,
    /// `Pers` — персональный режим (см. Delphi server logic).
    Pers = 2,
}

impl MoveAllCmdType {
    /// Возвращает `None` если байт неизвестен (A-02).
    pub fn from_byte(b: u8) -> Option<Self> {
        match b { 0 => Some(Self::MoveKind), 1 => Some(Self::PriceZone), 2 => Some(Self::Pers), _ => None }
    }
}

/// TReplaceMultiKind (Vars.pas:37).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplaceMultiKind {
    None = 0,
    Shift = 1,
    TopVol = 2,
    LowVol = 3,
    TopProfit = 4,
    All = 5,
    LastSet = 6,
    LastMoved = 7,
}

impl ReplaceMultiKind {
    /// Возвращает `None` если байт неизвестен (A-02).
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::None),
            1 => Some(Self::Shift),
            2 => Some(Self::TopVol),
            3 => Some(Self::LowVol),
            4 => Some(Self::TopProfit),
            5 => Some(Self::All),
            6 => Some(Self::LastSet),
            7 => Some(Self::LastMoved),
            _ => None,
        }
    }
}

/// TPriceZone (Vars.pas:73) — packed record: `MinP, MaxP: double`.
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, Default)]
pub struct PriceZone {
    pub min_p: f64,
    pub max_p: f64,
}

/// TOrderCompact (MarketsU.pas:180, 117 байт packed).
/// Этот тип сериализуется через `ms.Read/Write(BuyOrder, SizeOf(BuyOrder))` —
/// то есть **прямой memcpy** packed struct. В Rust используем `#[repr(C, packed)]`
/// с теми же типами и порядком полей.
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, Default)]
pub struct OrderCompact {
    pub int_id: i64,                  // 8
    pub quantity: f64,                // 8
    pub quantity_remaining: f64,      // 8
    pub total_btc: f64,               // 8
    pub spent_btc: f64,               // 8
    pub open_time: f64,               // 8  TDateTime
    pub close_time: f64,              // 8  TDateTime
    pub actual_price: f64,            // 8
    pub mean_price: f64,              // 8
    pub quantity_base: f64,           // 8
    pub actual_q: f64,                // 8
    pub tmp_btc: f64,                 // 8
    pub create_time: f64,             // 8  TDateTime
    pub panic_sell_down: f32,         // 4
    pub order_type: u8,               // 1  TOrderType
    pub sub_type: u8,                 // 1  TOrderSubType
    pub stop_flag: u8,                // 1
    pub partial_done: u8,             // 1
    pub leverage: u8,                 // 1
    pub is_opened: u8,                // 1  boolean
    pub is_closed: u8,                // 1
    pub canceled: u8,                 // 1
    pub is_short: u8,                 // 1
}

/// Размер `TOrderCompact` в байтах wire-format'а.
/// 13×8 + 4 + 9×1 = 117 байт (matches Delphi комментарий "~117 байт").
pub const ORDER_COMPACT_SIZE: usize = 13 * 8 + 4 + 9 * 1;

impl OrderCompact {
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < ORDER_COMPACT_SIZE { return None; }
        // Делаем побайтовую распаковку, потому что packed struct в Rust имеет
        // ограничения на reference-based доступ к полям.
        let mut o = OrderCompact::default();
        let mut p = 0usize;
        o.int_id = i64::from_le_bytes(data[p..p+8].try_into().unwrap()); p += 8;
        o.quantity = f64::from_le_bytes(data[p..p+8].try_into().unwrap()); p += 8;
        o.quantity_remaining = f64::from_le_bytes(data[p..p+8].try_into().unwrap()); p += 8;
        o.total_btc = f64::from_le_bytes(data[p..p+8].try_into().unwrap()); p += 8;
        o.spent_btc = f64::from_le_bytes(data[p..p+8].try_into().unwrap()); p += 8;
        o.open_time = f64::from_le_bytes(data[p..p+8].try_into().unwrap()); p += 8;
        o.close_time = f64::from_le_bytes(data[p..p+8].try_into().unwrap()); p += 8;
        o.actual_price = f64::from_le_bytes(data[p..p+8].try_into().unwrap()); p += 8;
        o.mean_price = f64::from_le_bytes(data[p..p+8].try_into().unwrap()); p += 8;
        o.quantity_base = f64::from_le_bytes(data[p..p+8].try_into().unwrap()); p += 8;
        o.actual_q = f64::from_le_bytes(data[p..p+8].try_into().unwrap()); p += 8;
        o.tmp_btc = f64::from_le_bytes(data[p..p+8].try_into().unwrap()); p += 8;
        o.create_time = f64::from_le_bytes(data[p..p+8].try_into().unwrap()); p += 8;
        o.panic_sell_down = f32::from_le_bytes(data[p..p+4].try_into().unwrap()); p += 4;
        o.order_type = data[p]; p += 1;
        o.sub_type = data[p]; p += 1;
        o.stop_flag = data[p]; p += 1;
        o.partial_done = data[p]; p += 1;
        o.leverage = data[p]; p += 1;
        o.is_opened = data[p]; p += 1;
        o.is_closed = data[p]; p += 1;
        o.canceled = data[p]; p += 1;
        o.is_short = data[p];
        Some(o)
    }

    pub fn write_to(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.int_id.to_le_bytes());
        out.extend_from_slice(&self.quantity.to_le_bytes());
        out.extend_from_slice(&self.quantity_remaining.to_le_bytes());
        out.extend_from_slice(&self.total_btc.to_le_bytes());
        out.extend_from_slice(&self.spent_btc.to_le_bytes());
        out.extend_from_slice(&self.open_time.to_le_bytes());
        out.extend_from_slice(&self.close_time.to_le_bytes());
        out.extend_from_slice(&self.actual_price.to_le_bytes());
        out.extend_from_slice(&self.mean_price.to_le_bytes());
        out.extend_from_slice(&self.quantity_base.to_le_bytes());
        out.extend_from_slice(&self.actual_q.to_le_bytes());
        out.extend_from_slice(&self.tmp_btc.to_le_bytes());
        out.extend_from_slice(&self.create_time.to_le_bytes());
        out.extend_from_slice(&self.panic_sell_down.to_le_bytes());
        out.push(self.order_type);
        out.push(self.sub_type);
        out.push(self.stop_flag);
        out.push(self.partial_done);
        out.push(self.leverage);
        out.push(self.is_opened);
        out.push(self.is_closed);
        out.push(self.canceled);
        out.push(self.is_short);
    }

    /// Применить временное смещение к временным полям. ServerTimeDelta = InitialTime - Now.
    /// Все TDateTime поля корректируются.
    pub fn adjust_time(&mut self, delta: f64) {
        self.open_time -= delta;
        self.close_time -= delta;
        self.create_time -= delta;
    }
}

/// TStopSettings (MarketsU.pas:215, packed record, 46 байт).
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct StopSettings {
    pub stop_loss_on: u8,             // 1
    pub sl_fixed: u8,                 // 1
    pub sl_level: f64,                // 8
    pub sl_spread: f64,               // 8
    pub trailing_on: u8,              // 1
    pub trailing_fixed: u8,           // 1
    pub trailing_level: f64,          // 8
    pub ts_spread: f64,               // 8
    pub use_take_profit: u8,          // 1
    pub take_profit: f64,             // 8
    pub take_profit_changed: u8,      // 1
}

/// Wire-size TStopSettings: 6 + 5*8 = 46 байт.
pub const STOP_SETTINGS_SIZE: usize = 6 + 5 * 8;

impl StopSettings {
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < STOP_SETTINGS_SIZE { return None; }
        let mut s = StopSettings::default();
        let mut p = 0usize;
        s.stop_loss_on = data[p]; p += 1;
        s.sl_fixed = data[p]; p += 1;
        s.sl_level = f64::from_le_bytes(data[p..p+8].try_into().unwrap()); p += 8;
        s.sl_spread = f64::from_le_bytes(data[p..p+8].try_into().unwrap()); p += 8;
        s.trailing_on = data[p]; p += 1;
        s.trailing_fixed = data[p]; p += 1;
        s.trailing_level = f64::from_le_bytes(data[p..p+8].try_into().unwrap()); p += 8;
        s.ts_spread = f64::from_le_bytes(data[p..p+8].try_into().unwrap()); p += 8;
        s.use_take_profit = data[p]; p += 1;
        s.take_profit = f64::from_le_bytes(data[p..p+8].try_into().unwrap()); p += 8;
        s.take_profit_changed = data[p];
        Some(s)
    }

    pub fn write_to(&self, out: &mut Vec<u8>) {
        out.push(self.stop_loss_on);
        out.push(self.sl_fixed);
        out.extend_from_slice(&self.sl_level.to_le_bytes());
        out.extend_from_slice(&self.sl_spread.to_le_bytes());
        out.push(self.trailing_on);
        out.push(self.trailing_fixed);
        out.extend_from_slice(&self.trailing_level.to_le_bytes());
        out.extend_from_slice(&self.ts_spread.to_le_bytes());
        out.push(self.use_take_profit);
        out.extend_from_slice(&self.take_profit.to_le_bytes());
        out.push(self.take_profit_changed);
    }
}

/// TOrderUpdateData (MarketsU.pas:263, packed record, 66 байт).
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, Default)]
pub struct OrderUpdateData {
    pub int_id: i64,                  // 8
    pub actual_price: f64,            // 8
    pub open_time: f64,               // 8  TDateTime
    pub quantity: f64,                // 8
    pub quantity_remaining: f64,      // 8
    pub actual_q: f64,                // 8
    pub total_btc: f64,               // 8
    pub mean_price: f64,              // 8
    pub partial_done: u8,             // 1
    pub stop_flag: u8,                // 1
}

pub const ORDER_UPDATE_DATA_SIZE: usize = 8 * 8 + 2;

impl OrderUpdateData {
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < ORDER_UPDATE_DATA_SIZE { return None; }
        let mut d = OrderUpdateData::default();
        let mut p = 0usize;
        d.int_id = i64::from_le_bytes(data[p..p+8].try_into().unwrap()); p += 8;
        d.actual_price = f64::from_le_bytes(data[p..p+8].try_into().unwrap()); p += 8;
        d.open_time = f64::from_le_bytes(data[p..p+8].try_into().unwrap()); p += 8;
        d.quantity = f64::from_le_bytes(data[p..p+8].try_into().unwrap()); p += 8;
        d.quantity_remaining = f64::from_le_bytes(data[p..p+8].try_into().unwrap()); p += 8;
        d.actual_q = f64::from_le_bytes(data[p..p+8].try_into().unwrap()); p += 8;
        d.total_btc = f64::from_le_bytes(data[p..p+8].try_into().unwrap()); p += 8;
        d.mean_price = f64::from_le_bytes(data[p..p+8].try_into().unwrap()); p += 8;
        d.partial_done = data[p]; p += 1;
        d.stop_flag = data[p];
        Some(d)
    }

    pub fn write_to(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.int_id.to_le_bytes());
        out.extend_from_slice(&self.actual_price.to_le_bytes());
        out.extend_from_slice(&self.open_time.to_le_bytes());
        out.extend_from_slice(&self.quantity.to_le_bytes());
        out.extend_from_slice(&self.quantity_remaining.to_le_bytes());
        out.extend_from_slice(&self.actual_q.to_le_bytes());
        out.extend_from_slice(&self.total_btc.to_le_bytes());
        out.extend_from_slice(&self.mean_price.to_le_bytes());
        out.push(self.partial_done);
        out.push(self.stop_flag);
    }

    pub fn adjust_time(&mut self, delta: f64) {
        self.open_time -= delta;
    }
}

/// TImmuneItem (TradeStruct.pas:210, packed) — UID:u64 + Value:bool.
#[derive(Debug, Clone, Copy)]
pub struct ImmuneItem {
    pub uid: u64,
    pub value: bool,
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
        if r.len() < 11 { return None; }
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
        if r.len() < 2 { return None; }
        let currency = r[0];
        let platform = r[1];
        *r = &r[2..];
        let market_name = read_str(r)?;
        Some(Self { base, currency, platform, market_name })
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
        if r.len() < 3 { return None; }
        let epoch = u16::from_le_bytes([r[0], r[1]]);
        let status = OrderWorkerStatus::from_byte(r[2])?;
        *r = &r[3..];
        Some(Self { market, epoch, status })
    }

    pub fn write(&self, out: &mut Vec<u8>, base_currency: u8, base_platform: u8) {
        self.market.write(out, base_currency, base_platform);
        out.extend_from_slice(&self.epoch.to_le_bytes());
        out.push(self.status as u8);
    }
}

fn read_str(r: &mut &[u8]) -> Option<String> {
    if r.len() < 2 { return None; }
    let len = u16::from_le_bytes([r[0], r[1]]) as usize;
    if r.len() < 2 + len { return None; }
    let s = String::from_utf8_lossy(&r[2..2+len]).to_string();
    *r = &r[2+len..];
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
    OrderStatus(OrderStatus),
    /// CmdId=5: TOrderStatusUpdate — delta-update полей.
    OrderStatusUpdate(OrderStatusUpdate),
    /// CmdId=6: TOrderReplaceCommand — запрос на перемещение цены.
    OrderReplace(OrderReplaceCommand),
    /// CmdId=7: TOrderReplaceResponse — подтверждение перемещения.
    OrderReplaceResponse(OrderReplaceResponse),
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
        let peek_cmd_id = if !r.is_empty() { r[0] } else { return None; };
        // Peek ver без consume.
        if r.len() < 11 { return None; }
        let ver = u16::from_le_bytes([r[1], r[2]]);
        if ver > CURRENT_PROTO_CMD_VER {
            let uid = u64::from_le_bytes(r[3..11].try_into().unwrap());
            return Some(TradeCommand::Unknown { cmd_id: peek_cmd_id, uid });
        }

        match peek_cmd_id {
            1 => Some(TradeCommand::BaseMarket(MarketCommandHeader::read(&mut r)?)),
            2 => Some(TradeCommand::TradeEpoch(TradeEpochHeader::read(&mut r)?)),
            3 => Some(TradeCommand::NewOrder(NewOrderCommand::read(&mut r)?)),
            4 => Some(TradeCommand::OrderStatus(OrderStatus::read(&mut r)?)),
            5 => Some(TradeCommand::OrderStatusUpdate(OrderStatusUpdate::read(&mut r)?)),
            6 => Some(TradeCommand::OrderReplace(OrderReplaceCommand::read(&mut r)?)),
            7 => Some(TradeCommand::OrderReplaceResponse(OrderReplaceResponse::read(&mut r)?)),
            8 => Some(TradeCommand::AllStatuses(AllStatuses::read(&mut r)?)),
            9 => {
                let h = BaseCommandHeader::read(&mut r)?;
                Some(TradeCommand::AllStatusesRequest(h))
            }
            10 => Some(TradeCommand::OrderCancel(OrderCancelCommand::read(&mut r)?)),
            11 => Some(TradeCommand::JoinOrders(JoinOrdersCommand::read(&mut r)?)),
            12 => Some(TradeCommand::SplitOrder(SplitOrderCommand::read(&mut r)?)),
            13 => Some(TradeCommand::MoveAllSells(MoveAllSellsCommand::read(&mut r)?)),
            14 => Some(TradeCommand::DoClosePosition(DoClosePositionCommand::read(&mut r)?)),
            15 => Some(TradeCommand::DoLimitClosePosition(JoinOrdersCommand::read(&mut r)?)),
            16 => Some(TradeCommand::DoSplitPosition(JoinOrdersCommand::read(&mut r)?)),
            17 => Some(TradeCommand::DoSellOrder(DoSellOrderCommand::read(&mut r)?)),
            18 => Some(TradeCommand::OrderStatusRequest(TradeEpochHeader::read(&mut r)?)),
            19 => Some(TradeCommand::OrderNotFound(TradeEpochHeader::read(&mut r)?)),
            20 => Some(TradeCommand::OrderStopsUpdate(OrderStopsUpdate::read(&mut r)?)),
            21 => Some(TradeCommand::TurnPanicSell(TurnPanicSellCommand::read(&mut r)?)),
            22 => Some(TradeCommand::SetImmune(SetImmuneCommand::read(&mut r)?)),
            23 => Some(TradeCommand::Penalty(MarketCommandHeader::read(&mut r)?)),
            24 => Some(TradeCommand::TradeVisual(MarketCommandHeader::read(&mut r)?)),
            25 => Some(TradeCommand::OrderTracePoint(OrderTracePoint::read(&mut r)?)),
            26 => Some(TradeCommand::CorridorUpdate(CorridorUpdate::read(&mut r)?)),
            27 => Some(TradeCommand::MoveAllBuys(MoveAllBuysCommand::read(&mut r)?)),
            28 => Some(TradeCommand::BulkReplaceNotify(BulkReplaceNotify::read(&mut r)?)),
            29 => Some(TradeCommand::VStopUpdate(VStopUpdate::read(&mut r)?)),
            30 => Some(TradeCommand::DoMarketSplitPosition(JoinOrdersCommand::read(&mut r)?)),
            _ => {
                let uid = u64::from_le_bytes(r[3..11].try_into().unwrap());
                Some(TradeCommand::Unknown { cmd_id: peek_cmd_id, uid })
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
        if r.len() < 1 + 8 + 8 + 8 { return None; }
        let is_short = r[0] != 0; *r = &r[1..];
        let price = f64::from_le_bytes(r[0..8].try_into().unwrap()); *r = &r[8..];
        let strat_id = u64::from_le_bytes(r[0..8].try_into().unwrap()); *r = &r[8..];
        let order_size = f64::from_le_bytes(r[0..8].try_into().unwrap()); *r = &r[8..];
        Some(Self { market, is_short, price, strat_id, order_size })
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
        if r.len() < 2 * ORDER_COMPACT_SIZE + STOP_SETTINGS_SIZE + 8 + 1 + 4 + 1 { return None; }
        let buy_order = OrderCompact::from_bytes(&r[..ORDER_COMPACT_SIZE])?;
        *r = &r[ORDER_COMPACT_SIZE..];
        let sell_order = OrderCompact::from_bytes(&r[..ORDER_COMPACT_SIZE])?;
        *r = &r[ORDER_COMPACT_SIZE..];
        let stops = StopSettings::from_bytes(&r[..STOP_SETTINGS_SIZE])?;
        *r = &r[STOP_SETTINGS_SIZE..];
        let strat_id = u64::from_le_bytes(r[0..8].try_into().unwrap()); *r = &r[8..];
        let is_short = r[0] != 0; *r = &r[1..];
        let db_id = i32::from_le_bytes(r[0..4].try_into().unwrap()); *r = &r[4..];
        let from_cache = r[0] != 0; *r = &r[1..];

        let ver = epoch_header.market.base.ver;
        let mut emulator_mode = false;
        let mut immune_for_clicks = false;

        if ver >= 2 {
            if r.is_empty() { return None; }
            emulator_mode = r[0] != 0; *r = &r[1..];
        }
        if ver >= 3 {
            if r.is_empty() { return None; }
            immune_for_clicks = r[0] != 0; *r = &r[1..];
        }

        Some(Self {
            epoch_header, buy_order, sell_order, stops,
            strat_id, is_short, db_id, from_cache, emulator_mode, immune_for_clicks,
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
        if r.len() < ORDER_UPDATE_DATA_SIZE { return None; }
        let update_data = OrderUpdateData::from_bytes(&r[..ORDER_UPDATE_DATA_SIZE])?;
        *r = &r[ORDER_UPDATE_DATA_SIZE..];
        let sell_reason_code = if !r.is_empty() {
            let v = r[0]; *r = &r[1..]; v
        } else { 0 };
        Some(Self { epoch_header, update_data, sell_reason_code })
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
        if r.len() < 1 + 8 { return None; }
        let order_type = OrderType::from_byte(r[0])?; *r = &r[1..];
        let new_price = f64::from_le_bytes(r[0..8].try_into().unwrap()); *r = &r[8..];
        Some(Self { epoch_header, order_type, new_price })
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
        if r.len() < 1 + 8 + ORDER_UPDATE_DATA_SIZE + 8 { return None; }
        let order_type = OrderType::from_byte(r[0])?; *r = &r[1..];
        let price = f64::from_le_bytes(r[0..8].try_into().unwrap()); *r = &r[8..];
        let update_data = OrderUpdateData::from_bytes(&r[..ORDER_UPDATE_DATA_SIZE])?;
        *r = &r[ORDER_UPDATE_DATA_SIZE..];
        let quantity_base = f64::from_le_bytes(r[0..8].try_into().unwrap()); *r = &r[8..];
        Some(Self { epoch_header, order_type, price, update_data, quantity_base })
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
        if r.len() < 4 { return None; }
        let count = i32::from_le_bytes(r[0..4].try_into().unwrap()) as usize;
        *r = &r[4..];
        let mut orders = Vec::with_capacity(count);
        for _ in 0..count {
            // Каждый order пишется через `o.StoreToStream(Stream)` — то есть **сам** включает
            // свой CmdId(1) + ver(2) + UID(8) + ... header. Используем тот же parser.
            // Нужно прочитать TBaseTradeCommand.FromStream(ms) — он сам читает CmdId и dispatch.
            // Здесь каждый order гарантированно TOrderStatus (CmdId=4).
            let order = OrderStatus::read(r)?;
            orders.push(order);
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
        Some(Self { epoch_header: TradeEpochHeader::read(r)? })
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
        if r.is_empty() { return None; }
        let is_short = r[0] != 0; *r = &r[1..];
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
        if r.len() < 4 + 1 + 1 { return None; }
        let split_parts = i32::from_le_bytes(r[0..4].try_into().unwrap()); *r = &r[4..];
        let split_small = r[0] != 0; *r = &r[1..];
        let split_small_sell = r[0] != 0; *r = &r[1..];
        Some(Self { market, split_parts, split_small, split_small_sell })
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
        if r.len() < 1 + 1 + 8 + 16 { return None; }
        let cmd_type = r[0]; *r = &r[1..];
        let move_kind = ReplaceMultiKind::from_byte(r[0])?; *r = &r[1..];
        let price = f64::from_le_bytes(r[0..8].try_into().unwrap()); *r = &r[8..];
        let min_p = f64::from_le_bytes(r[0..8].try_into().unwrap()); *r = &r[8..];
        let max_p = f64::from_le_bytes(r[0..8].try_into().unwrap()); *r = &r[8..];
        let price_zone = PriceZone { min_p, max_p };
        // Soft-read: side появилось позже. На отсутствии — Both (legacy default).
        let side = if !r.is_empty() {
            let v = FixedPosition::from_byte(r[0])?; *r = &r[1..]; v
        } else { FixedPosition::Both };
        Some(Self { market, cmd_type, move_kind, price, price_zone, side })
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
        if r.is_empty() { return None; }
        let market_sell = r[0] != 0; *r = &r[1..];
        Some(Self { market, market_sell })
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
        if r.len() < 16 { return None; }
        let price = f64::from_le_bytes(r[0..8].try_into().unwrap()); *r = &r[8..];
        let size = f64::from_le_bytes(r[0..8].try_into().unwrap()); *r = &r[8..];
        Some(Self { market, price, size })
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
        if r.len() < STOP_SETTINGS_SIZE { return None; }
        let stops = StopSettings::from_bytes(&r[..STOP_SETTINGS_SIZE])?;
        *r = &r[STOP_SETTINGS_SIZE..];
        Some(Self { epoch_header, stops })
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
        if r.is_empty() { return None; }
        let turn_on = r[0] != 0; *r = &r[1..];
        Some(Self { epoch_header, turn_on })
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
        if r.is_empty() { return None; }
        let n = r[0] as usize; *r = &r[1..];
        if r.len() < n * 9 { return None; }
        let mut items = Vec::with_capacity(n);
        for _ in 0..n {
            let uid = u64::from_le_bytes(r[0..8].try_into().unwrap()); *r = &r[8..];
            let value = r[0] != 0; *r = &r[1..];
            items.push(ImmuneItem { uid, value });
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
    pub trace_time: f64,       // TDateTime
    pub trace_price: f32,
    pub base_price: f32,
    pub stop_price: f32,
    pub ord_type: OrderType,
    pub flags: u8,
}

impl OrderTracePoint {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        let market = MarketCommandHeader::read(r)?;
        if r.len() < 8 + 4 * 3 + 1 + 1 { return None; }
        let trace_time = f64::from_le_bytes(r[0..8].try_into().unwrap()); *r = &r[8..];
        let trace_price = f32::from_le_bytes(r[0..4].try_into().unwrap()); *r = &r[4..];
        let base_price = f32::from_le_bytes(r[0..4].try_into().unwrap()); *r = &r[4..];
        let stop_price = f32::from_le_bytes(r[0..4].try_into().unwrap()); *r = &r[4..];
        let ord_type = OrderType::from_byte(r[0])?; *r = &r[1..];
        let flags = r[0]; *r = &r[1..];
        Some(Self { market, trace_time, trace_price, base_price, stop_price, ord_type, flags })
    }

    pub fn is_temp(&self) -> bool { (self.flags & trace_flags::IS_TEMP) != 0 }
    pub fn is_finish(&self) -> bool { (self.flags & trace_flags::IS_FINISH) != 0 }
    pub fn is_initial(&self) -> bool { (self.flags & trace_flags::IS_INITIAL) != 0 }

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
        if r.len() < 8 { return None; }
        let price_down = f32::from_le_bytes(r[0..4].try_into().unwrap()); *r = &r[4..];
        let price_up = f32::from_le_bytes(r[0..4].try_into().unwrap()); *r = &r[4..];
        Some(Self { market, price_down, price_up })
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
        if r.len() < 1 + 1 + 8 { return None; }
        let cmd_type = r[0]; *r = &r[1..];
        let move_kind = ReplaceMultiKind::from_byte(r[0])?; *r = &r[1..];
        let price = f64::from_le_bytes(r[0..8].try_into().unwrap()); *r = &r[8..];
        let side = if !r.is_empty() {
            let v = FixedPosition::from_byte(r[0])?; *r = &r[1..]; v
        } else { FixedPosition::Both };
        Some(Self { market, cmd_type, move_kind, price, side })
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
        if r.len() < 1 + 2 { return None; }
        let order_type = OrderType::from_byte(r[0])?; *r = &r[1..];
        let count = u16::from_le_bytes([r[0], r[1]]) as usize; *r = &r[2..];
        if r.len() < count * 8 { return None; }
        let mut uids = Vec::with_capacity(count);
        for _ in 0..count {
            uids.push(u64::from_le_bytes(r[0..8].try_into().unwrap()));
            *r = &r[8..];
        }
        Some(Self { market, order_type, uids })
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
        if r.len() < 2 + 16 { return None; }
        let vstop_on = r[0] != 0; *r = &r[1..];
        let vstop_fixed = r[0] != 0; *r = &r[1..];
        let vstop_level = f64::from_le_bytes(r[0..8].try_into().unwrap()); *r = &r[8..];
        let vstop_vol = f64::from_le_bytes(r[0..8].try_into().unwrap()); *r = &r[8..];
        Some(Self { epoch_header, vstop_on, vstop_fixed, vstop_level, vstop_vol })
    }
}

// ============================================================================
//  Builders для исходящих команд (client → server)
// ============================================================================

const TRADE_BASE_CURRENCY: u8 = 1; // BC_USDT по умолчанию — клиент должен передавать своё значение
const TRADE_BASE_PLATFORM: u8 = 4; // Platform_FBinance — клиент должен передавать своё значение

fn write_base_command_header(out: &mut Vec<u8>, cmd_id: u8, uid: u64) {
    out.push(cmd_id);
    out.extend_from_slice(&CURRENT_PROTO_CMD_VER.to_le_bytes());
    out.extend_from_slice(&uid.to_le_bytes());
}

fn write_market_header(out: &mut Vec<u8>, cmd_id: u8, uid: u64, market_name: &str, currency: u8, platform: u8) {
    write_base_command_header(out, cmd_id, uid);
    out.push(currency);
    out.push(platform);
    write_string(out, market_name);
}

fn write_trade_epoch_header(out: &mut Vec<u8>, cmd_id: u8, uid: u64, market_name: &str,
                            currency: u8, platform: u8, epoch: u16, status: OrderWorkerStatus) {
    write_market_header(out, cmd_id, uid, market_name, currency, platform);
    out.extend_from_slice(&epoch.to_le_bytes());
    out.push(status as u8);
}

/// Параметры билдера, общие для большинства trade команд.
#[derive(Debug, Clone, Copy)]
pub struct TradeCtx {
    pub uid: u64,
    pub currency: u8,
    pub platform: u8,
}

impl TradeCtx {
    pub fn new(uid: u64) -> Self {
        Self { uid, currency: TRADE_BASE_CURRENCY, platform: TRADE_BASE_PLATFORM }
    }
}

/// CmdId=6: построить пакет TOrderReplaceCommand.
pub fn build_order_replace(ctx: TradeCtx, market_name: &str, epoch: u16, status: OrderWorkerStatus,
                            order_type: OrderType, new_price: f64) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    write_trade_epoch_header(&mut out, 6, ctx.uid, market_name, ctx.currency, ctx.platform, epoch, status);
    out.push(order_type as u8);
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
pub fn build_order_cancel(ctx: TradeCtx, market_name: &str, epoch: u16, status: OrderWorkerStatus) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_trade_epoch_header(&mut out, 10, ctx.uid, market_name, ctx.currency, ctx.platform, epoch, status);
    out
}

/// CmdId=11: TJoinOrdersCommand.
pub fn build_join_orders(ctx: TradeCtx, market_name: &str, is_short: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_market_header(&mut out, 11, ctx.uid, market_name, ctx.currency, ctx.platform);
    out.push(is_short as u8);
    out
}

/// CmdId=12: TSplitOrderCommand.
pub fn build_split_order(ctx: TradeCtx, market_name: &str, split_parts: i32,
                         split_small: bool, split_small_sell: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_market_header(&mut out, 12, ctx.uid, market_name, ctx.currency, ctx.platform);
    out.extend_from_slice(&split_parts.to_le_bytes());
    out.push(split_small as u8);
    out.push(split_small_sell as u8);
    out
}

/// CmdId=13: TMoveAllSellsCommand.
pub fn build_move_all_sells(ctx: TradeCtx, market_name: &str, cmd_type: u8,
                             move_kind: ReplaceMultiKind, price: f64,
                             price_zone: PriceZone, side: FixedPosition) -> Vec<u8> {
    let mut out = Vec::with_capacity(48);
    write_market_header(&mut out, 13, ctx.uid, market_name, ctx.currency, ctx.platform);
    out.push(cmd_type);
    out.push(move_kind as u8);
    out.extend_from_slice(&price.to_le_bytes());
    out.extend_from_slice(&price_zone.min_p.to_le_bytes());
    out.extend_from_slice(&price_zone.max_p.to_le_bytes());
    out.push(side as u8);
    out
}

/// CmdId=14: TDoClosePositionCommand.
pub fn build_do_close_position(ctx: TradeCtx, market_name: &str, market_sell: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_market_header(&mut out, 14, ctx.uid, market_name, ctx.currency, ctx.platform);
    out.push(market_sell as u8);
    out
}

/// CmdId=15: TDoLimitClosePositionCommand (= JoinOrdersCommand format).
pub fn build_do_limit_close_position(ctx: TradeCtx, market_name: &str, is_short: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_market_header(&mut out, 15, ctx.uid, market_name, ctx.currency, ctx.platform);
    out.push(is_short as u8);
    out
}

/// CmdId=16: TDoSplitPositionCommand (= JoinOrdersCommand format).
pub fn build_do_split_position(ctx: TradeCtx, market_name: &str, is_short: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_market_header(&mut out, 16, ctx.uid, market_name, ctx.currency, ctx.platform);
    out.push(is_short as u8);
    out
}

/// CmdId=17: TDoSellOrderCommand.
pub fn build_do_sell_order(ctx: TradeCtx, market_name: &str, price: f64, size: f64) -> Vec<u8> {
    let mut out = Vec::with_capacity(40);
    write_market_header(&mut out, 17, ctx.uid, market_name, ctx.currency, ctx.platform);
    out.extend_from_slice(&price.to_le_bytes());
    out.extend_from_slice(&size.to_le_bytes());
    out
}

/// CmdId=18: TOrderStatusRequest — запрос статуса конкретного ордера.
pub fn build_order_status_request(ctx: TradeCtx, market_name: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_trade_epoch_header(&mut out, 18, ctx.uid, market_name, ctx.currency, ctx.platform,
                              0, OrderWorkerStatus::None);
    out
}

/// CmdId=20: TOrderStopsUpdate.
pub fn build_order_stops_update(ctx: TradeCtx, market_name: &str, epoch: u16,
                                  status: OrderWorkerStatus, stops: &StopSettings) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    write_trade_epoch_header(&mut out, 20, ctx.uid, market_name, ctx.currency, ctx.platform, epoch, status);
    stops.write_to(&mut out);
    out
}

/// CmdId=21: TTurnPanicSellCommand.
pub fn build_turn_panic_sell(ctx: TradeCtx, market_name: &str, epoch: u16,
                              status: OrderWorkerStatus, turn_on: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_trade_epoch_header(&mut out, 21, ctx.uid, market_name, ctx.currency, ctx.platform, epoch, status);
    out.push(turn_on as u8);
    out
}

/// CmdId=22: TSetImmuneCommand.
pub fn build_set_immune(uid: u64, items: &[ImmuneItem]) -> Vec<u8> {
    let mut out = Vec::with_capacity(11 + 1 + items.len() * 9);
    write_base_command_header(&mut out, 22, uid);
    if items.len() > 255 {
        // Delphi: Count записывается как Byte → максимум 255 элементов.
        out.push(255);
        for it in &items[..255] {
            out.extend_from_slice(&it.uid.to_le_bytes());
            out.push(it.value as u8);
        }
    } else {
        out.push(items.len() as u8);
        for it in items {
            out.extend_from_slice(&it.uid.to_le_bytes());
            out.push(it.value as u8);
        }
    }
    out
}

/// CmdId=27: TMoveAllBuysCommand.
pub fn build_move_all_buys(ctx: TradeCtx, market_name: &str, cmd_type: u8,
                            move_kind: ReplaceMultiKind, price: f64, side: FixedPosition) -> Vec<u8> {
    let mut out = Vec::with_capacity(40);
    write_market_header(&mut out, 27, ctx.uid, market_name, ctx.currency, ctx.platform);
    out.push(cmd_type);
    out.push(move_kind as u8);
    out.extend_from_slice(&price.to_le_bytes());
    out.push(side as u8);
    out
}

/// CmdId=29: TVStopUpdate.
pub fn build_vstop_update(ctx: TradeCtx, market_name: &str, epoch: u16, status: OrderWorkerStatus,
                          vstop_on: bool, vstop_fixed: bool, vstop_level: f64, vstop_vol: f64) -> Vec<u8> {
    let mut out = Vec::with_capacity(48);
    write_trade_epoch_header(&mut out, 29, ctx.uid, market_name, ctx.currency, ctx.platform, epoch, status);
    out.push(vstop_on as u8);
    out.push(vstop_fixed as u8);
    out.extend_from_slice(&vstop_level.to_le_bytes());
    out.extend_from_slice(&vstop_vol.to_le_bytes());
    out
}

/// CmdId=30: TDoMarketSplitPositionCommand (= JoinOrdersCommand format).
pub fn build_do_market_split_position(ctx: TradeCtx, market_name: &str, is_short: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_market_header(&mut out, 30, ctx.uid, market_name, ctx.currency, ctx.platform);
    out.push(is_short as u8);
    out
}

/// CmdId=3: TNewOrderCommand — запрос на создание нового ордера.
pub fn build_new_order(ctx: TradeCtx, market_name: &str, is_short: bool,
                       price: f64, strat_id: u64, order_size: f64) -> Vec<u8> {
    let mut out = Vec::with_capacity(48);
    write_market_header(&mut out, 3, ctx.uid, market_name, ctx.currency, ctx.platform);
    out.push(is_short as u8);
    out.extend_from_slice(&price.to_le_bytes());
    out.extend_from_slice(&strat_id.to_le_bytes());
    out.extend_from_slice(&order_size.to_le_bytes());
    out
}
