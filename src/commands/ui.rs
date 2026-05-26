//! MPC_UI канал — 14 подкоманд TBaseUICommand.
//!
//! Источник Delphi: `MoonProto/MoonProtoUIStruct.pas` (~790 строк).
//!
//! ## CmdId маппинг
//! - 1  — `TClientSettingsCommand`  (Sliced, UK_BaseUISettings) — большой snapshot настроек UI
//! - 2  — `TSettingsRequest`        (empty)
//! - 3  — `TStratStartStopCommand`  (boolean IsStart)
//! - 4  — `TStratStartStopCommandV2` (IsStart + Items[StratCheckedItem])
//! - 5  — `TMMOrdersSubscribeCommand` (boolean Subscribe, UK_TurnMMDetection)
//! - 6  — `TUpdateVersionCommand`   (VersionName + IsRelease)
//! - 7  — `TEmuTradesCommand`       (Sliced, mIndex + BaseTime + Points[EmuTradePoint])
//! - 8  — `TNewMarketNotifyCommand` (empty, Priority=High)
//! - 9  — `TLevManageCommand`       (Sliced, UK_LevManageSettings)
//! - 10 — `TTriggerManageCommand`   (Sliced, Markets/Keys[])
//! - 11 — `TResetProfitCommand`     (1 byte kind)
//! - 12 — `TArbActivateNotify`      (TDateTime ArbValid)
//! - 13 — `TSwitchDexCommand`       (ShortString\[15\] DexName, UK_DexSwitch, High)
//! - 14 — `TSwitchSpotCommand`      (byte SpotIndex, UK_SpotSwitch, High)
//!
//! ## Замечание про ASCfg / ASCfg2
//! `TAutoStartConfig` (104 байта) и `TAutoStartConfig2` (168 байт) — это packed records
//! из `Config.pas`. На проводе они передаются как `Word size + bytes(size)` blob с soft-read
//! (если sz > SizeOf — лишнее skip'ится; если sz < SizeOf — partial read). В порте они
//! сохраняются как **raw `Vec<u8>`** — потребитель сам решает как распарсить.
//!
//! ## ArbConfig compact format
//! Не raw record! На проводе: `ver:byte + wantedSet:bytes(32) + flags:byte + colorCount:byte
//! + colorCount*5 bytes`. `wantedSet` — Delphi `set of byte` (32 байта = 256 битовая маска).

use super::registry::{decode_utf8_delphi, read_string, write_string, CURRENT_PROTO_CMD_VER};
use super::strat::StratCheckedItem;
use zerocopy::byteorder::little_endian::{F32 as LeF32, U16 as LeU16};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

mod builders;
mod parser;

#[cfg(test)]
pub(crate) use builders::build_new_market_notify;
pub use builders::{
    build_arb_activate_notify, build_client_settings, build_emu_trades, build_lev_manage,
    build_mm_orders_subscribe, build_reset_profit, build_settings_request, build_strat_start_stop,
    build_strat_start_stop_v2, build_switch_dex, build_switch_spot, build_trigger_manage,
    build_update_version,
};

// --- CmdId constants ---
const CMD_CLIENT_SETTINGS: u8 = 1;
const CMD_SETTINGS_REQUEST: u8 = 2;
const CMD_STRAT_START_STOP: u8 = 3;
const CMD_STRAT_START_STOP_V2: u8 = 4;
const CMD_MM_ORDERS_SUBSCRIBE: u8 = 5;
const CMD_UPDATE_VERSION: u8 = 6;
const CMD_EMU_TRADES: u8 = 7;
const CMD_NEW_MARKET_NOTIFY: u8 = 8;
const CMD_LEV_MANAGE: u8 = 9;
const CMD_TRIGGER_MANAGE: u8 = 10;
const CMD_RESET_PROFIT: u8 = 11;
const CMD_ARB_ACTIVATE_NOTIFY: u8 = 12;
const CMD_SWITCH_DEX: u8 = 13;
const CMD_SWITCH_SPOT: u8 = 14;

const LEV_CMD_VER: u8 = 1;

/// `TAutoStartConfig` packed record size in bytes (Config.pas:344).
pub const AS_CFG_SIZE: usize = 104;
/// `TAutoStartConfig2` packed record size in bytes (Config.pas:384).
pub const AS_CFG2_SIZE: usize = 168;

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
struct WireAutoStartConfig {
    bytes: [u8; AS_CFG_SIZE],
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
struct WireAutoStartConfig2 {
    bytes: [u8; AS_CFG2_SIZE],
}

const _: () = assert!(core::mem::size_of::<WireAutoStartConfig>() == AS_CFG_SIZE);
const _: () = assert!(core::mem::size_of::<WireAutoStartConfig2>() == AS_CFG2_SIZE);

impl WireAutoStartConfig {
    fn from_blob(blob: &[u8]) -> Self {
        let mut bytes = [0u8; AS_CFG_SIZE];
        copy_blob_prefix(&mut bytes, blob);
        Self { bytes }
    }
}

impl WireAutoStartConfig2 {
    fn from_blob(blob: &[u8]) -> Self {
        let mut bytes = [0u8; AS_CFG2_SIZE];
        copy_blob_prefix(&mut bytes, blob);
        Self { bytes }
    }
}

/// ArbConfig wire version byte (ArbTypes.pas:25 `ARB_CONFIG_VER = 1`).
pub const ARB_CONFIG_VER: u8 = 1;

// =============================================================================
//  ArbConfig — compact wire form (NOT raw record)
// =============================================================================

/// ArbConfig compact form, как пишется в `TClientSettingsCommand`.
/// Источник: `MoonProtoUIStruct.pas:370-393` (Read) / `450-468` (Write).
#[derive(Debug, Clone)]
pub struct ArbConfigCompact {
    /// 256-битная маска "wanted" платформ (Delphi `set of byte`).
    pub wanted: [bool; 256],
    pub show_absolute: bool,
    pub show_numbers: bool,
    pub show_lines: bool,
    pub show_percent: bool,
    pub show_right: bool,
}

impl Default for ArbConfigCompact {
    /// Defaults из `InitArbConfigDefaults` (ArbTypes.pas:87): ShowLines=true, ShowPercent=true.
    fn default() -> Self {
        Self {
            wanted: [false; 256],
            show_absolute: false,
            show_numbers: false,
            show_lines: true,
            show_percent: true,
            show_right: false,
        }
    }
}

// =============================================================================
//  EmuTradePoint (6 байт packed)
// =============================================================================

/// `TEmuTradePoint = packed record TimeDeltaMS:Word(2) + Price:Single(4)` = 6 байт.
/// Source: MoonProtoUIStruct.pas:115-118.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EmuTradePoint {
    /// Дельта от BaseTime в миллисекундах (0..65535).
    pub time_delta_ms: u16,
    /// Цена. Знак отрицательный = Sell.
    pub price: f32,
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
struct WireEmuTradePoint {
    time_delta_ms: LeU16,
    price: LeF32,
}

const EMU_TRADE_POINT_SIZE: usize = std::mem::size_of::<WireEmuTradePoint>();
const _: [(); 6] = [(); EMU_TRADE_POINT_SIZE];

impl EmuTradePoint {
    #[cfg(test)]
    fn from_wire(wire: WireEmuTradePoint) -> Self {
        Self {
            time_delta_ms: wire.time_delta_ms.get(),
            price: wire.price.get(),
        }
    }

    fn to_wire(self) -> WireEmuTradePoint {
        WireEmuTradePoint {
            time_delta_ms: LeU16::new(self.time_delta_ms),
            price: LeF32::new(self.price),
        }
    }

    #[cfg(test)]
    fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < EMU_TRADE_POINT_SIZE {
            return None;
        }
        Some(Self::from_wire(
            WireEmuTradePoint::read_from_bytes(&data[..EMU_TRADE_POINT_SIZE]).ok()?,
        ))
    }

    fn write_to(self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.to_wire().as_bytes());
    }
}

// =============================================================================
//  Subcommand payloads
// =============================================================================

/// CmdId=1 `TClientSettingsCommand` — большой snapshot настроек MoonBot UI.
///
/// Многие поля append-only soft-read: в зависимости от версии сервера часть полей
/// может отсутствовать. Delphi `CreateFromStream` берёт часть недостающего хвоста
/// из текущего `cfg`; в active path это делает
/// [`UICommand::parse_with_client_settings_fallback`].
///
/// B-01 (docs_api iter-2): `#[derive(Default)]` для удобства потребителя.
/// Из Delphi `TClientSettingsCommand.Create` пользователь получал готовую структуру
/// с дефолтами; в Rust раньше нужно было руками заполнять ~30 полей. Теперь:
/// ```ignore
/// let mut settings = ClientSettingsCommand::default();
/// settings.x_sell = 3;
/// settings.use_g_take_profit = true;
/// settings.g_take_profit = 1.5;
/// client.ui_send_settings(&settings);
/// ```
/// `uid` will be `0` by default. The high-level `Client::ui_send_settings`
/// wrapper generates a fresh wire UID and uses Delphi's fixed `UKey.UID = 1`
/// for queue deduplication. Set this field only when using the low-level
/// builder directly.
#[derive(Debug, Clone, Default)]
pub struct ClientSettingsCommand {
    /// Wire command UID. Leave it as `0` when using `Client::ui_send_settings`;
    /// the wrapper writes a fresh UID and uses the fixed Delphi settings UKey.
    /// Set it manually only when using `build_client_settings` directly.
    pub uid: u64,
    // --- always present (v1+) ---
    pub x_sell: i32,
    pub x_sell_scalp: i32,
    pub x_tmode: bool,
    pub fixed_sell_mode: bool,
    pub fixed_sell_price: f64,
    pub price_drop_level: f32,
    pub trailing_drop: f32,
    pub g_take_profit: f64,
    pub use_g_take_profit: bool,
    pub unused_spread: i32,
    pub panic_if_price_drop: bool,
    pub emu_mode: bool,
    // --- v2+ ---
    pub buy_iceberg: bool,
    pub sell_iceberg: bool,
    /// Из `cfg.OrdersControl.SignOrders` (Config.pas:682). Заливается ВНУТРЬ глобальной конфигурации
    /// в Delphi, потому полей класса под него нет — храним в команде явно.
    pub sign_orders: bool,
    // --- always present (v1+) ---
    pub coins_black_list_text: String,
    pub use_coins_black_list: bool,
    pub temp_bl_symbols: Vec<String>,
    /// `TempBLTimes[i]: TDateTime` — дельта (в днях) оставшегося времени блокировки.
    pub temp_bl_times: Vec<f64>,
    // --- soft-read (опциональны, могут отсутствовать в старых пакетах) ---
    pub use_manual_strategy: bool,
    pub manual_strategy_id: u64,
    pub free_position_check: bool,
    pub vol_drop_level: i32,
    pub use_stop_market: bool,
    /// `TAutoStartConfig` blob (size 104 в текущей версии Delphi). Хранится как raw.
    pub as_cfg: Vec<u8>,
    /// `TAutoStartConfig2` blob (size 168 в текущей версии Delphi).
    pub as_cfg2: Vec<u8>,
    /// HotkeysConfig.SPrice[1..6].
    pub s_price: [f32; 6],
    /// HotkeysConfig.sbNum.
    pub sb_num: u8,
    /// MultiOrders.JoinSellKind (TJoinSellKind: 0=None, 1=FixPrice, 2=FixProfit).
    pub join_sell_kind: u8,
    /// ArbConfig в compact-формате (НЕ raw record).
    pub arb_config: ArbConfigCompact,
}

/// CmdId=3 `TStratStartStopCommand`. Boolean IsStart.
#[derive(Debug, Clone, Copy)]
pub struct StratStartStop {
    pub uid: u64,
    pub is_start: bool,
}

/// CmdId=4 `TStratStartStopCommandV2`. Содержит дельту checked-стратегий.
#[derive(Debug, Clone)]
pub struct StratStartStopV2 {
    pub uid: u64,
    pub is_start: bool,
    pub items: Vec<StratCheckedItem>,
}

/// CmdId=5 `TMMOrdersSubscribeCommand`.
#[derive(Debug, Clone, Copy)]
pub struct MMOrdersSubscribe {
    pub uid: u64,
    pub subscribe: bool,
}

/// CmdId=6 `TUpdateVersionCommand`.
///
/// This is the MoonBot remote-update command, not a passive client-version
/// notification. Delphi writes `VersionName` as UTF-8 string and `IsRelease` as
/// one byte. `VersionName=""` with `is_release=true` is the normal release
/// update button; a non-empty name targets a test/beta build name.
#[derive(Debug, Clone)]
pub struct UpdateVersion {
    pub uid: u64,
    pub version_name: String,
    pub is_release: bool,
}

/// CmdId=7 `TEmuTradesCommand` (Priority=Sliced). Серия эмулированных тиков для одного маркета.
#[derive(Debug, Clone)]
pub struct EmuTrades {
    pub uid: u64,
    pub m_index: u16,
    /// `BaseTime: TDateTime` (Delphi double, дни с 1899-12-30).
    pub base_time: f64,
    pub points: Vec<EmuTradePoint>,
}

/// CmdId=8 `TNewMarketNotifyCommand` (empty body, Priority=High).
#[derive(Debug, Clone, Copy)]
pub struct NewMarketNotify {
    pub uid: u64,
}

/// CmdId=9 `TLevManageCommand` (Sliced, UK_LevManageSettings).
#[derive(Debug, Clone)]
pub struct LevManage {
    pub uid: u64,
    /// Версия внутри принятой команды. Outgoing builder always writes
    /// Delphi's `LevCmdVer = 1`, regardless of this read-model field.
    pub cmd_ver: u8,
    pub auto_max_order: bool,
    pub auto_lev_up: bool,
    pub auto_isolated: bool,
    pub auto_cross: bool,
    pub auto_fix_lev: bool,
    pub fix_lev: i32,
    pub tlg_report: bool,
    pub lev_control: String,
}

/// CmdId=10 `TTriggerManageCommand`.
#[derive(Debug, Clone)]
pub struct TriggerManage {
    pub uid: u64,
    /// 0 = Clear, 1 = Set.
    pub action: u8,
    pub all_markets: bool,
    pub markets: Vec<u16>,
    pub keys: Vec<u16>,
}

/// CmdId=11 `TResetProfitCommand`.
#[derive(Debug, Clone, Copy)]
pub struct ResetProfit {
    pub uid: u64,
    /// 0 = CurProfit, 1 = AllProfit.
    pub reset_kind: u8,
}

/// CmdId=12 `TArbActivateNotify`.
#[derive(Debug, Clone, Copy)]
pub struct ArbActivateNotify {
    pub uid: u64,
    /// `ArbValid: TDateTime`.
    pub arb_valid: f64,
}

/// CmdId=13 `TSwitchDexCommand` (High, UK_DexSwitch).
/// `DexName: ShortString[15]` = 16 байт на проводе (1 байт длины + до 15 байт ASCII).
#[derive(Debug, Clone)]
pub struct SwitchDex {
    pub uid: u64,
    pub dex_name: String,
}

/// CmdId=14 `TSwitchSpotCommand` (High, UK_SpotSwitch).
#[derive(Debug, Clone, Copy)]
pub struct SwitchSpot {
    pub uid: u64,
    /// 0=Crypto, 1=Predict.
    pub spot_index: u8,
}

// =============================================================================
//  UICommand enum
// =============================================================================

#[derive(Debug, Clone)]
pub enum UICommand {
    /// Full client settings snapshot. Boxed to keep the common `UICommand`
    /// envelope small when it is moved through event queues.
    ClientSettings(Box<ClientSettingsCommand>),
    SettingsRequest {
        uid: u64,
    },
    StratStartStop(StratStartStop),
    StratStartStopV2(StratStartStopV2),
    MMOrdersSubscribe(MMOrdersSubscribe),
    UpdateVersion(UpdateVersion),
    EmuTrades(EmuTrades),
    NewMarketNotify(NewMarketNotify),
    LevManage(LevManage),
    TriggerManage(TriggerManage),
    ResetProfit(ResetProfit),
    ArbActivateNotify(ArbActivateNotify),
    SwitchDex(SwitchDex),
    SwitchSpot(SwitchSpot),
    /// Command header is well-formed, but the command version is newer than
    /// this library can parse. Delphi registry marks this as `FSkipped`.
    Skipped {
        cmd_id: u8,
        uid: u64,
        ver: u16,
    },
    Unknown {
        cmd_id: u8,
        uid: u64,
    },
}

// =============================================================================
//  Builders (C → S)
// =============================================================================

fn write_header(out: &mut Vec<u8>, cmd_id: u8, uid: u64) {
    out.push(cmd_id);
    out.extend_from_slice(&CURRENT_PROTO_CMD_VER.to_le_bytes());
    out.extend_from_slice(&uid.to_le_bytes());
}

fn copy_blob_prefix<const N: usize>(dst: &mut [u8; N], blob: &[u8]) {
    let copy = blob.len().min(N);
    dst[..copy].copy_from_slice(&blob[..copy]);
}

// =============================================================================
//  Tests
// =============================================================================

#[cfg(test)]
mod tests;
