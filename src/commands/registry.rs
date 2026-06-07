//! Command registry — matches MoonProtoBaseStruct.pas:314-348.
//! Channel dispatch by command class is handled in `protocol::Command`; this
//! module holds the shared wire-string codec and the proto command version gate.
//!
//! Wire format of every command:
//!   CmdId (1 byte) + ver (2 bytes u16 LE) + UID (8 bytes u64 LE) + payload
//!
//! Version gate: if ver > CURRENT_VER (3), skip the command.

use crate::protocol::Command;

pub(crate) const CURRENT_PROTO_CMD_VER: u16 = 3;

/// `UK_None`: no queue deduplication.
pub(crate) const UK_NONE: u8 = 0;
/// `UK_OrderStatus`: low-level order-status request key.
pub(crate) const UK_ORDER_STATUS: u8 = 1;
/// `UK_OrderStatusShort`: low-level short order-status request key.
pub(crate) const UK_ORDER_STATUS_SHORT: u8 = 2;
/// `UK_OrderMove`: replace/cancel/stops/panic/VStop dedup by order task id.
pub(crate) const UK_ORDER_MOVE: u8 = 3;
/// `UK_StopMove`: legacy stop-move dedup ordinal.
pub(crate) const UK_STOP_MOVE: u8 = 4;
/// `UK_StratSnapshot`: singleton strategy snapshot dedup key.
pub(crate) const UK_STRAT_SNAPSHOT: u8 = 5;
/// `UK_BaseUISettings`: singleton client-settings snapshot dedup key.
pub(crate) const UK_BASE_UI_SETTINGS: u8 = 6;
/// `UK_StratSellPriceUpdate`: per-strategy sell-price dedup key.
pub(crate) const UK_STRAT_SELL_PRICE_UPDATE: u8 = 7;
/// `UK_BalanceFull`: singleton full-balance snapshot dedup key.
pub(crate) const UK_BALANCE_FULL: u8 = 8;
/// `UK_TurnMMDetection`: MM-orders subscription command key.
pub(crate) const UK_TURN_MM_DETECTION: u8 = 9;
/// `UK_ImmuneClicks`: batch order-immunity dedup key.
pub(crate) const UK_IMMUNE_CLICKS: u8 = 10;
/// `UK_LevManageSettings`: singleton leverage-management settings key.
pub(crate) const UK_LEV_MANAGE_SETTINGS: u8 = 11;
/// `UK_ArbPrices`: arbitrage price command key.
pub(crate) const UK_ARB_PRICES: u8 = 12;
/// `UK_DexSwitch`: DEX switch command key.
pub(crate) const UK_DEX_SWITCH: u8 = 13;
/// `UK_SpotSwitch`: spot-mode switch command key.
pub(crate) const UK_SPOT_SWITCH: u8 = 14;
/// `UK_ChartTextSnapshot`: per-client chart text snapshot key.
pub(crate) const UK_CHART_TEXT_SNAPSHOT: u8 = 15;
/// `UK_ChartTextState`: singleton chart text state key.
pub(crate) const UK_CHART_TEXT_STATE: u8 = 16;

/// Send priority as protocol metadata, independent from the concrete client
/// queue implementation. Conversion to `SendPriority` happens at the send edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommandPriority {
    Sliced,
    High,
    Low,
}

impl CommandPriority {
    pub(crate) const fn default_retries(self) -> i32 {
        match self {
            Self::Sliced => 6,
            Self::High | Self::Low => 3,
        }
    }
}

/// Wire-base family inherited by a typed command.
///
/// This models command ancestry that matters on the wire: the base header is
/// always `CmdId + ver + UID`, while market/epoch descendants prepend extra
/// fields and also change the inherited UKey rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommandBase {
    Base,
    Market,
    TradeEpoch,
}

/// How a command computes Delphi `TMoonUniqueKey.UID` after `unique_kind` chose
/// the key namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UKeyRule {
    None,
    HeaderUid,
    MarketIndex,
    TradeEpochUid,
    Singleton(u64),
    StrategyId,
    ImmuneItemsSum,
    SendContextClientId,
}

/// Direction is client-side documentation/checking metadata. It does not change
/// wire bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommandDirection {
    Inbound,
    Outbound,
    Both,
}

/// One typed command class as the Rust equivalent of the Delphi RTTI registry
/// row: identity, inherited wire-base, send metadata, and UKey semantics in one
/// place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CommandDescriptor {
    pub(crate) outer: Command,
    pub(crate) id: u8,
    pub(crate) name: &'static str,
    pub(crate) base: CommandBase,
    pub(crate) priority: CommandPriority,
    pub(crate) max_retries: i32,
    pub(crate) default_encrypted: bool,
    pub(crate) unique_kind: u8,
    pub(crate) ukey: UKeyRule,
    pub(crate) direction: CommandDirection,
}

impl CommandDescriptor {
    pub(crate) const fn new(
        outer: Command,
        id: u8,
        name: &'static str,
        base: CommandBase,
        priority: CommandPriority,
        retries: Option<i32>,
        unique_kind: u8,
        ukey: UKeyRule,
        direction: CommandDirection,
    ) -> Self {
        let max_retries = match retries {
            Some(value) => value,
            None => priority.default_retries(),
        };
        Self {
            outer,
            id,
            name,
            base,
            priority,
            max_retries,
            default_encrypted: true,
            unique_kind,
            ukey,
            direction,
        }
    }
}

const fn inherited_ukey(base: CommandBase) -> UKeyRule {
    match base {
        CommandBase::Base => UKeyRule::HeaderUid,
        CommandBase::Market => UKeyRule::MarketIndex,
        CommandBase::TradeEpoch => UKeyRule::TradeEpochUid,
    }
}

macro_rules! cmd_desc {
    (
        $outer:expr, $id:literal, $name:literal,
        base = $base:ident,
        priority = $priority:ident,
        retries = $retries:expr,
        unique = $unique:expr,
        ukey = $ukey:expr,
        direction = $direction:ident
    ) => {
        CommandDescriptor::new(
            $outer,
            $id,
            $name,
            CommandBase::$base,
            CommandPriority::$priority,
            $retries,
            $unique,
            $ukey,
            CommandDirection::$direction,
        )
    };
    (
        $outer:expr, $id:literal, $name:literal,
        base = $base:ident,
        priority = $priority:ident,
        unique = $unique:expr,
        direction = $direction:ident
    ) => {
        cmd_desc!(
            $outer,
            $id,
            $name,
            base = $base,
            priority = $priority,
            retries = None,
            unique = $unique,
            ukey = inherited_ukey(CommandBase::$base),
            direction = $direction
        )
    };
    (
        $outer:expr, $id:literal, $name:literal,
        base = $base:ident,
        priority = $priority:ident,
        direction = $direction:ident
    ) => {
        cmd_desc!(
            $outer,
            $id,
            $name,
            base = $base,
            priority = $priority,
            retries = None,
            unique = UK_NONE,
            ukey = UKeyRule::None,
            direction = $direction
        )
    };
}

pub(crate) const ORDER_COMMANDS: &[CommandDescriptor] = &[
    cmd_desc!(
        Command::Order,
        0,
        "TBaseTradeCommand",
        base = Base,
        priority = High,
        direction = Both
    ),
    cmd_desc!(
        Command::Order,
        1,
        "TBaseMarketCommand",
        base = Market,
        priority = High,
        direction = Both
    ),
    cmd_desc!(
        Command::Order,
        2,
        "TTradeEpochCommand",
        base = TradeEpoch,
        priority = High,
        direction = Both
    ),
    cmd_desc!(
        Command::Order,
        3,
        "TNewOrderCommand",
        base = Market,
        priority = High,
        direction = Outbound
    ),
    cmd_desc!(
        Command::Order,
        4,
        "TOrderStatus",
        base = TradeEpoch,
        priority = High,
        unique = UK_ORDER_STATUS,
        direction = Inbound
    ),
    cmd_desc!(
        Command::Order,
        5,
        "TOrderStatusUpdate",
        base = TradeEpoch,
        priority = High,
        unique = UK_ORDER_STATUS_SHORT,
        direction = Inbound
    ),
    cmd_desc!(
        Command::Order,
        6,
        "TOrderReplaceCommand",
        base = TradeEpoch,
        priority = High,
        unique = UK_ORDER_MOVE,
        direction = Outbound
    ),
    cmd_desc!(
        Command::Order,
        7,
        "TOrderReplaceResponse",
        base = TradeEpoch,
        priority = High,
        retries = Some(4),
        unique = UK_ORDER_MOVE,
        ukey = UKeyRule::TradeEpochUid,
        direction = Inbound
    ),
    cmd_desc!(
        Command::Order,
        8,
        "TAllStatuses",
        base = Base,
        priority = Sliced,
        direction = Inbound
    ),
    cmd_desc!(
        Command::Order,
        9,
        "TAllStatusesReq",
        base = Base,
        priority = High,
        direction = Outbound
    ),
    cmd_desc!(
        Command::Order,
        10,
        "TOrderCancelCommand",
        base = TradeEpoch,
        priority = High,
        unique = UK_ORDER_MOVE,
        direction = Outbound
    ),
    cmd_desc!(
        Command::Order,
        11,
        "TJoinOrdersCommand",
        base = Market,
        priority = High,
        direction = Outbound
    ),
    cmd_desc!(
        Command::Order,
        12,
        "TSplitOrderCommand",
        base = Market,
        priority = High,
        direction = Outbound
    ),
    cmd_desc!(
        Command::Order,
        13,
        "TMoveAllSellsCommand",
        base = Market,
        priority = High,
        direction = Outbound
    ),
    cmd_desc!(
        Command::Order,
        14,
        "TDoClosePositionCommand",
        base = Market,
        priority = High,
        retries = Some(1),
        unique = UK_NONE,
        ukey = UKeyRule::None,
        direction = Outbound
    ),
    cmd_desc!(
        Command::Order,
        15,
        "TDoLimitClosePositionCommand",
        base = Market,
        priority = High,
        retries = Some(1),
        unique = UK_NONE,
        ukey = UKeyRule::None,
        direction = Outbound
    ),
    cmd_desc!(
        Command::Order,
        16,
        "TDoSplitPositionCommand",
        base = Market,
        priority = High,
        retries = Some(1),
        unique = UK_NONE,
        ukey = UKeyRule::None,
        direction = Outbound
    ),
    cmd_desc!(
        Command::Order,
        17,
        "TDoSellOrderCommand",
        base = Market,
        priority = High,
        retries = Some(1),
        unique = UK_NONE,
        ukey = UKeyRule::None,
        direction = Outbound
    ),
    cmd_desc!(
        Command::Order,
        18,
        "TOrderStatusRequest",
        base = TradeEpoch,
        priority = High,
        direction = Outbound
    ),
    cmd_desc!(
        Command::Order,
        19,
        "TOrderNotFound",
        base = TradeEpoch,
        priority = High,
        direction = Inbound
    ),
    cmd_desc!(
        Command::Order,
        20,
        "TOrderStopsUpdate",
        base = TradeEpoch,
        priority = High,
        unique = UK_ORDER_MOVE,
        direction = Both
    ),
    cmd_desc!(
        Command::Order,
        21,
        "TTurnPanicSellCommand",
        base = TradeEpoch,
        priority = High,
        unique = UK_ORDER_MOVE,
        direction = Both
    ),
    cmd_desc!(
        Command::Order,
        22,
        "TSetImmuneCommand",
        base = Base,
        priority = High,
        retries = None,
        unique = UK_IMMUNE_CLICKS,
        ukey = UKeyRule::ImmuneItemsSum,
        direction = Both
    ),
    cmd_desc!(
        Command::Order,
        23,
        "TPenaltyCommand",
        base = Market,
        priority = High,
        direction = Both
    ),
    cmd_desc!(
        Command::Order,
        24,
        "TTradeVisualCommand",
        base = Market,
        priority = High,
        direction = Inbound
    ),
    cmd_desc!(
        Command::Order,
        25,
        "TOrderTracePoint",
        base = Market,
        priority = High,
        direction = Inbound
    ),
    cmd_desc!(
        Command::Order,
        26,
        "TCorridorUpdate",
        base = Market,
        priority = Low,
        direction = Inbound
    ),
    cmd_desc!(
        Command::Order,
        27,
        "TMoveAllBuysCommand",
        base = Market,
        priority = High,
        direction = Outbound
    ),
    cmd_desc!(
        Command::Order,
        28,
        "TBulkReplaceNotify",
        base = Market,
        priority = High,
        direction = Inbound
    ),
    cmd_desc!(
        Command::Order,
        29,
        "TVStopUpdate",
        base = TradeEpoch,
        priority = High,
        unique = UK_ORDER_MOVE,
        direction = Both
    ),
    cmd_desc!(
        Command::Order,
        30,
        "TDoMarketSplitPositionCommand",
        base = Market,
        priority = High,
        retries = Some(1),
        unique = UK_NONE,
        ukey = UKeyRule::None,
        direction = Outbound
    ),
    cmd_desc!(
        Command::Order,
        31,
        "TClosedSellOrderReportCommand",
        base = Base,
        priority = Sliced,
        direction = Inbound
    ),
];

pub(crate) const UI_COMMANDS: &[CommandDescriptor] = &[
    cmd_desc!(
        Command::UI,
        0,
        "TBaseUICommand",
        base = Base,
        priority = High,
        direction = Both
    ),
    cmd_desc!(
        Command::UI,
        1,
        "TClientSettingsCommand",
        base = Base,
        priority = Sliced,
        retries = None,
        unique = UK_BASE_UI_SETTINGS,
        ukey = UKeyRule::Singleton(1),
        direction = Both
    ),
    cmd_desc!(
        Command::UI,
        2,
        "TSettingsRequest",
        base = Base,
        priority = High,
        direction = Outbound
    ),
    cmd_desc!(
        Command::UI,
        3,
        "TStratStartStopCommand",
        base = Base,
        priority = High,
        direction = Both
    ),
    cmd_desc!(
        Command::UI,
        4,
        "TStratStartStopCommandV2",
        base = Base,
        priority = High,
        direction = Both
    ),
    cmd_desc!(
        Command::UI,
        5,
        "TMMOrdersSubscribeCommand",
        base = Base,
        priority = High,
        unique = UK_TURN_MM_DETECTION,
        direction = Outbound
    ),
    cmd_desc!(
        Command::UI,
        6,
        "TUpdateVersionCommand",
        base = Base,
        priority = High,
        direction = Both
    ),
    cmd_desc!(
        Command::UI,
        7,
        "TEmuTradesCommand",
        base = Base,
        priority = Sliced,
        direction = Outbound
    ),
    cmd_desc!(
        Command::UI,
        8,
        "TNewMarketNotifyCommand",
        base = Base,
        priority = High,
        direction = Inbound
    ),
    cmd_desc!(
        Command::UI,
        9,
        "TLevManageCommand",
        base = Base,
        priority = Sliced,
        retries = None,
        unique = UK_LEV_MANAGE_SETTINGS,
        ukey = UKeyRule::Singleton(1),
        direction = Both
    ),
    cmd_desc!(
        Command::UI,
        10,
        "TTriggerManageCommand",
        base = Base,
        priority = Sliced,
        direction = Both
    ),
    cmd_desc!(
        Command::UI,
        11,
        "TResetProfitCommand",
        base = Base,
        priority = High,
        direction = Outbound
    ),
    cmd_desc!(
        Command::UI,
        12,
        "TArbActivateNotify",
        base = Base,
        priority = High,
        direction = Inbound
    ),
    cmd_desc!(
        Command::UI,
        13,
        "TSwitchDexCommand",
        base = Base,
        priority = High,
        unique = UK_DEX_SWITCH,
        direction = Outbound
    ),
    cmd_desc!(
        Command::UI,
        14,
        "TSwitchSpotCommand",
        base = Base,
        priority = High,
        unique = UK_SPOT_SWITCH,
        direction = Outbound
    ),
    cmd_desc!(
        Command::UI,
        15,
        "TAlertObjectCommand",
        base = Base,
        priority = Sliced,
        direction = Both
    ),
    cmd_desc!(
        Command::UI,
        16,
        "TAlertSnapshotRequest",
        base = Base,
        priority = High,
        direction = Outbound
    ),
    cmd_desc!(
        Command::UI,
        17,
        "TChartTextStateCommand",
        base = Base,
        priority = High,
        retries = None,
        unique = UK_CHART_TEXT_STATE,
        ukey = UKeyRule::Singleton(1),
        direction = Outbound
    ),
    cmd_desc!(
        Command::UI,
        18,
        "TChartTextSnapshotCommand",
        base = Base,
        priority = Sliced,
        retries = None,
        unique = UK_CHART_TEXT_SNAPSHOT,
        ukey = UKeyRule::SendContextClientId,
        direction = Inbound
    ),
    cmd_desc!(
        Command::UI,
        19,
        "TOrdersHistoryRequestCommand",
        base = Base,
        priority = High,
        direction = Outbound
    ),
];

pub(crate) const STRAT_COMMANDS: &[CommandDescriptor] = &[
    cmd_desc!(
        Command::Strat,
        0,
        "TBaseStratCommand",
        base = Base,
        priority = High,
        direction = Both
    ),
    cmd_desc!(
        Command::Strat,
        1,
        "TStratSnapshotRequest",
        base = Base,
        priority = High,
        direction = Inbound
    ),
    cmd_desc!(
        Command::Strat,
        2,
        "TStratSnapshot",
        base = Base,
        priority = Sliced,
        retries = None,
        unique = UK_STRAT_SNAPSHOT,
        ukey = UKeyRule::Singleton(1),
        direction = Both
    ),
    cmd_desc!(
        Command::Strat,
        3,
        "TStratDelete",
        base = Base,
        priority = High,
        direction = Outbound
    ),
    cmd_desc!(
        Command::Strat,
        4,
        "TStratSellPriceUpdate",
        base = Base,
        priority = High,
        retries = None,
        unique = UK_STRAT_SELL_PRICE_UPDATE,
        ukey = UKeyRule::StrategyId,
        direction = Outbound
    ),
    cmd_desc!(
        Command::Strat,
        5,
        "TStratCheckedSync",
        base = Base,
        priority = Sliced,
        direction = Outbound
    ),
    cmd_desc!(
        Command::Strat,
        6,
        "TStratCheckedEcho",
        base = Base,
        priority = High,
        direction = Inbound
    ),
    cmd_desc!(
        Command::Strat,
        7,
        "TStratSchemaRequest",
        base = Base,
        priority = High,
        direction = Outbound
    ),
    cmd_desc!(
        Command::Strat,
        8,
        "TStratSchema",
        base = Base,
        priority = Sliced,
        direction = Inbound
    ),
    cmd_desc!(
        Command::Strat,
        9,
        "TDetectSignalCommand",
        base = Base,
        priority = High,
        direction = Inbound
    ),
];

pub(crate) const BALANCE_COMMANDS: &[CommandDescriptor] = &[
    cmd_desc!(
        Command::Balance,
        0,
        "TBaseBalanceCommand",
        base = Base,
        priority = High,
        direction = Both
    ),
    cmd_desc!(
        Command::Balance,
        1,
        "TBalanceCommandBase",
        base = Base,
        priority = High,
        direction = Inbound
    ),
    cmd_desc!(
        Command::Balance,
        2,
        "TBalanceCommand",
        base = Base,
        priority = High,
        direction = Inbound
    ),
    cmd_desc!(
        Command::Balance,
        3,
        "TBalanceSnapshotFull",
        base = Base,
        priority = Sliced,
        retries = None,
        unique = UK_BALANCE_FULL,
        ukey = UKeyRule::Singleton(1),
        direction = Inbound
    ),
    cmd_desc!(
        Command::Balance,
        4,
        "TBalanceIncrUpdate",
        base = Base,
        priority = High,
        direction = Inbound
    ),
    cmd_desc!(
        Command::Balance,
        5,
        "TRequestBalanceRefresh",
        base = Base,
        priority = High,
        direction = Outbound
    ),
    cmd_desc!(
        Command::Balance,
        6,
        "TArbPricesCommand",
        base = Base,
        priority = Low,
        direction = Inbound
    ),
];

pub(crate) const API_COMMANDS: &[CommandDescriptor] = &[
    cmd_desc!(
        Command::API,
        0,
        "TEngineStreamCommand",
        base = Base,
        priority = Sliced,
        direction = Both
    ),
    cmd_desc!(
        Command::API,
        1,
        "TEngineResponse",
        base = Base,
        priority = Sliced,
        direction = Inbound
    ),
    cmd_desc!(
        Command::API,
        2,
        "TEngineRequest",
        base = Base,
        priority = Sliced,
        direction = Outbound
    ),
];

pub(crate) const COMMAND_DESCRIPTOR_DOMAINS: &[&[CommandDescriptor]] = &[
    ORDER_COMMANDS,
    UI_COMMANDS,
    STRAT_COMMANDS,
    BALANCE_COMMANDS,
    API_COMMANDS,
];

pub(crate) fn find_descriptor(outer: Command, id: u8) -> Option<&'static CommandDescriptor> {
    COMMAND_DESCRIPTOR_DOMAINS
        .iter()
        .flat_map(|domain| domain.iter())
        .find(|desc| desc.outer == outer && desc.id == id)
}

/// Read a UTF-8 string with 2-byte LE length prefix.
/// Matches Delphi WriteStringToStreamUtf8/ReadStringFromStreamUtf8.
pub(crate) fn read_string(data: &[u8], pos: &mut usize) -> Option<String> {
    if *pos + 2 > data.len() {
        return None;
    }
    let len = u16::from_le_bytes([data[*pos], data[*pos + 1]]) as usize;
    *pos += 2;
    if *pos + len > data.len() {
        return None;
    }
    let s = decode_utf8_delphi(&data[*pos..*pos + len]);
    *pos += len;
    Some(s)
}

/// Decode UTF-8 with Delphi `TEncoding.UTF8.GetString` replacement semantics.
///
/// Rust `from_utf8_lossy` inserts U+FFFD for invalid input, but Delphi's default
/// replacement fallback inserts ASCII `?`. Protocol parsers use this for every
/// wire string so damaged bytes produce the same machine effect as Delphi.
pub(crate) fn decode_utf8_delphi(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_owned(),
        Err(_) => {
            let mut out = String::with_capacity(bytes.len());
            let mut rest = bytes;
            while !rest.is_empty() {
                match std::str::from_utf8(rest) {
                    Ok(s) => {
                        out.push_str(s);
                        break;
                    }
                    Err(err) => {
                        let valid_up_to = err.valid_up_to();
                        if valid_up_to > 0 {
                            out.push_str(std::str::from_utf8(&rest[..valid_up_to]).unwrap());
                        }
                        out.push('?');
                        let invalid_len = err.error_len().unwrap_or(rest.len() - valid_up_to);
                        rest = &rest[valid_up_to + invalid_len..];
                    }
                }
            }
            out
        }
    }
}

/// Write a UTF-8 string with 2-byte LE length prefix.
pub(crate) fn write_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    let len = bytes.len() as u16;
    let len_usize = usize::from(len);
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(&bytes[..len_usize]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn descriptor_map_has_unique_outer_id_keys() {
        let mut keys = HashSet::new();
        for desc in COMMAND_DESCRIPTOR_DOMAINS
            .iter()
            .flat_map(|domain| domain.iter())
        {
            assert!(
                keys.insert((desc.outer.to_byte(), desc.id)),
                "duplicate command descriptor: {:?} cmd_id={} ({})",
                desc.outer,
                desc.id,
                desc.name
            );
        }
    }

    #[test]
    fn descriptor_map_covers_known_typed_domains() {
        assert_eq!(ORDER_COMMANDS.len(), 32);
        assert_eq!(UI_COMMANDS.len(), 20);
        assert_eq!(STRAT_COMMANDS.len(), 10);
        assert_eq!(BALANCE_COMMANDS.len(), 7);
        assert_eq!(API_COMMANDS.len(), 3);
    }

    #[test]
    fn descriptor_map_keeps_delphi_default_retry_rules() {
        let settings = find_descriptor(Command::UI, 1).unwrap();
        assert_eq!(settings.priority, CommandPriority::Sliced);
        assert_eq!(settings.max_retries, 6);

        let close = find_descriptor(Command::Order, 14).unwrap();
        assert_eq!(close.priority, CommandPriority::High);
        assert_eq!(close.max_retries, 1);

        let balance_refresh = find_descriptor(Command::Balance, 5).unwrap();
        assert_eq!(balance_refresh.priority, CommandPriority::High);
        assert_eq!(balance_refresh.max_retries, 3);
    }

    #[test]
    fn descriptor_map_preserves_inherited_ukey_semantics() {
        let mm = find_descriptor(Command::UI, 5).unwrap();
        assert_eq!(mm.unique_kind, UK_TURN_MM_DETECTION);
        assert_eq!(mm.ukey, UKeyRule::HeaderUid);

        let replace = find_descriptor(Command::Order, 6).unwrap();
        assert_eq!(replace.unique_kind, UK_ORDER_MOVE);
        assert_eq!(replace.ukey, UKeyRule::TradeEpochUid);

        let settings = find_descriptor(Command::UI, 1).unwrap();
        assert_eq!(settings.unique_kind, UK_BASE_UI_SETTINGS);
        assert_eq!(settings.ukey, UKeyRule::Singleton(1));
    }

    #[test]
    fn descriptor_map_keeps_chart_text_send_context_semantics() {
        let state = find_descriptor(Command::UI, 17).unwrap();
        assert_eq!(state.direction, CommandDirection::Outbound);
        assert_eq!(state.ukey, UKeyRule::Singleton(1));

        let snapshot = find_descriptor(Command::UI, 18).unwrap();
        assert_eq!(snapshot.direction, CommandDirection::Inbound);
        assert_eq!(snapshot.ukey, UKeyRule::SendContextClientId);
        assert!(
            snapshot.default_encrypted,
            "class default is encrypted; server-side sender may still override per instance"
        );
    }

    #[test]
    fn descriptor_map_includes_orders_history_request() {
        let desc = find_descriptor(Command::UI, 19).unwrap();
        assert_eq!(desc.name, "TOrdersHistoryRequestCommand");
        assert_eq!(desc.priority, CommandPriority::High);
        assert_eq!(desc.max_retries, 3);
        assert_eq!(desc.unique_kind, UK_NONE);
        assert_eq!(desc.ukey, UKeyRule::None);
    }

    #[test]
    // parity: MoonBot Vars.pas:WriteStringToStreamUtf8
    fn write_string_writes_only_declared_wrapped_len() {
        let s = "a".repeat(65_537);
        let mut buf = Vec::new();
        write_string(&mut buf, &s);

        assert_eq!(&buf[..2], &1u16.to_le_bytes());
        assert_eq!(buf.len(), 2 + 1);

        let mut pos = 0;
        assert_eq!(read_string(&buf, &mut pos).unwrap(), "a");
        assert_eq!(pos, buf.len());
    }

    #[test]
    // parity: MoonBot Vars.pas:ReadStringFromStreamUtf8
    fn read_string_replaces_invalid_utf8_with_question_mark() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&4u16.to_le_bytes());
        buf.extend_from_slice(&[b'a', 0xFF, b'b', 0x80]);

        let mut pos = 0;
        assert_eq!(read_string(&buf, &mut pos).unwrap(), "a?b?");
        assert_eq!(pos, buf.len());
    }

    #[test]
    // parity: MoonBot Vars.pas:ReadStringFromStreamUtf8
    fn read_string_rejects_truncated_declared_body() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&4u16.to_le_bytes());
        buf.extend_from_slice(b"ab");

        let mut pos = 0;
        assert!(read_string(&buf, &mut pos).is_none());
        assert_eq!(
            pos, 2,
            "Delphi ReadBuffer has consumed the length before failing on body bytes"
        );
    }

    #[test]
    fn decode_utf8_delphi_replaces_incomplete_sequence_with_single_question_mark() {
        assert_eq!(decode_utf8_delphi(&[b'a', 0xE2, 0x82]), "a?");
    }
}
