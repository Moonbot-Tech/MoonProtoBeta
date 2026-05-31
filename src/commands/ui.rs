//! MoonProto UI/settings command channel.
//!
//! Delphi source: `MoonProto/MoonProtoUIStruct.pas`.
//!
//! ## CmdId mapping
//! - 1  — `TClientSettingsCommand`  (Sliced, UK_BaseUISettings) — full UI/settings snapshot
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
//! ## ASCfg / ASCfg2 blobs
//! `TAutoStartConfig` (104 bytes) and `TAutoStartConfig2` (168 bytes) are
//! Delphi packed records from `Config.pas`. On the wire they are encoded as
//! `Word size + bytes(size)` with soft-read semantics: extra tail bytes are
//! skipped and short payloads are partially copied. MoonProto stores them as
//! raw blobs because there is no stable public Active Lib model for those
//! nested UI-only settings yet.
//!
//! ## ArbConfig compact format
//! This is not a raw Delphi record. The wire form is
//! `ver:byte + wantedSet:bytes(32) + flags:byte + colorCount:byte +
//! colorCount*5 bytes`. `wantedSet` is Delphi `set of byte`
//! (32 bytes = 256-bit mask).

use super::registry::{decode_utf8_delphi, read_string, write_string, CURRENT_PROTO_CMD_VER};
use super::strat::StratCheckedItem;
use zerocopy::byteorder::little_endian::{F32 as LeF32, U16 as LeU16};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

mod builders;
mod parser;

#[cfg(test)]
pub(crate) use builders::build_new_market_notify;
pub(crate) use builders::{
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
pub(crate) const ARB_CONFIG_VER: u8 = 1;

// =============================================================================
//  ArbConfig — compact wire form (NOT raw record)
// =============================================================================

/// Compact `ArbConfig` form as embedded into `TClientSettingsCommand`.
///
/// Delphi source: `MoonProtoUIStruct.pas:370-393` (read) and `450-468`
/// (write).
#[derive(Debug, Clone)]
pub struct ArbConfigCompact {
    /// 256-bit "wanted platform" mask, matching Delphi `set of byte`.
    pub wanted: [bool; 256],
    pub show_absolute: bool,
    pub show_numbers: bool,
    pub show_lines: bool,
    pub show_percent: bool,
    pub show_right: bool,
}

impl Default for ArbConfigCompact {
    /// Defaults from `InitArbConfigDefaults`: show lines and percentages.
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
//  EmuTradePoint (6-byte packed record)
// =============================================================================

/// `TEmuTradePoint = packed record TimeDeltaMS:Word(2) + Price:Single(4)` = 6 bytes.
/// Source: MoonProtoUIStruct.pas:115-118.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EmuTradePoint {
    /// Delta from `BaseTime` in milliseconds.
    pub time_delta_ms: u16,
    /// Trade price. Negative sign means sell side.
    pub price: f32,
}

/// One drawn chart-pencil point for the MoonBot trade emulator.
///
/// This is not a wire record. It is the user-facing input matching
/// `TChartFrame.TryEmulatePrices`: the UI has absolute chart time + price, and
/// Active Lib converts that path into signed [`EmuTradePoint`] rows.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EmuPencilPoint {
    /// Absolute Delphi chart time.
    pub time: crate::DelphiTime,
    /// Drawn chart price.
    pub price: f32,
}

impl EmuPencilPoint {
    pub const fn new(time: crate::DelphiTime, price: f32) -> Self {
        Self { time, price }
    }
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
    /// Emulated buy tick at `time_delta_ms` after the command base time.
    pub fn buy(time_delta_ms: u16, price: f32) -> Self {
        Self {
            time_delta_ms,
            price: price.abs(),
        }
    }

    /// Emulated sell tick at `time_delta_ms` after the command base time.
    ///
    /// Delphi encodes sell side by storing a negative `Price` in the packed
    /// point. Keeping that sign convention in the public constructor lets UI
    /// drawing code build the exact wire shape without exposing raw bytes.
    pub fn sell(time_delta_ms: u16, price: f32) -> Self {
        Self {
            time_delta_ms,
            price: -price.abs(),
        }
    }

    /// Whether this point represents a sell-side tick.
    pub fn is_sell(self) -> bool {
        self.price.is_sign_negative()
    }

    /// Absolute trade price independent of side.
    pub fn abs_price(self) -> f32 {
        self.price.abs()
    }

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

/// User-facing join-sells mode stored in
/// [`ClientSettingsCommand::join_sell_kind`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinSellKind {
    None,
    FixedPrice,
    FixedProfit,
    Unknown(u8),
}

impl JoinSellKind {
    pub fn from_byte(value: u8) -> Self {
        match value {
            0 => Self::None,
            1 => Self::FixedPrice,
            2 => Self::FixedProfit,
            other => Self::Unknown(other),
        }
    }

    pub fn to_byte(self) -> u8 {
        match self {
            Self::None => 0,
            Self::FixedPrice => 1,
            Self::FixedProfit => 2,
            Self::Unknown(value) => value,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::None => "None",
            Self::FixedPrice => "Fixed Price",
            Self::FixedProfit => "Fixed Profit",
            Self::Unknown(_) => "Unknown",
        }
    }
}

/// One temporary coin-blacklist row from [`ClientSettingsCommand`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TempBlacklistEntry<'a> {
    pub symbol: &'a str,
    /// Remaining blacklist duration in Delphi days.
    pub remaining_days: f64,
}

impl TempBlacklistEntry<'_> {
    pub fn remaining_hours(self) -> f64 {
        self.remaining_days * 24.0
    }
}

/// CmdId=1 `TClientSettingsCommand` — full MoonBot UI/settings snapshot.
///
/// Many fields are append-only soft-read fields: depending on server version,
/// part of the tail may be absent. Delphi `CreateFromStream` fills the missing
/// tail from current `cfg`; in the Active Lib path this is handled by
/// `UICommand::parse_with_client_settings_fallback`.
///
/// Normal terminal code edits the retained settings snapshot and sends the
/// whole snapshot back through the high-level active client. `Default` is mainly
/// for tests/tools, not for a live configured terminal session.
/// ```ignore
/// if let Some(current) = &snapshot.settings().client_settings {
///     let mut settings = current.clone();
///     settings.x_sell = 3;
///     settings.use_g_take_profit = true;
///     settings.g_take_profit = 1.5;
///     client.settings().send(settings);
/// }
/// ```
/// `uid` is `0` by default. High-level send helpers write a fresh wire UID and
/// use Delphi's fixed settings UKey slot for queue deduplication. Set this
/// field manually only when using the low-level builder directly.
#[derive(Debug, Clone, Default)]
pub struct ClientSettingsCommand {
    /// Wire command UID. Leave it as `0` when using high-level send helpers.
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
    /// `cfg.OrdersControl.SignOrders` value mirrored explicitly in the Rust
    /// snapshot. Delphi writes it into global config instead of storing a
    /// separate command field.
    pub sign_orders: bool,
    // --- always present (v1+) ---
    pub coins_black_list_text: String,
    pub use_coins_black_list: bool,
    pub temp_bl_symbols: Vec<String>,
    /// `TempBLTimes[i]: TDateTime` delta in days.
    pub temp_bl_times: Vec<f64>,
    // --- soft-read tail (optional in older packets) ---
    pub use_manual_strategy: bool,
    pub manual_strategy_id: u64,
    pub free_position_check: bool,
    pub vol_drop_level: i32,
    pub use_stop_market: bool,
    /// `TAutoStartConfig` blob (104 bytes in the current Delphi version).
    pub as_cfg: Vec<u8>,
    /// `TAutoStartConfig2` blob (168 bytes in the current Delphi version).
    pub as_cfg2: Vec<u8>,
    /// HotkeysConfig.SPrice[1..6].
    pub s_price: [f32; 6],
    /// HotkeysConfig.sbNum.
    pub sb_num: u8,
    /// MultiOrders.JoinSellKind (TJoinSellKind: 0=None, 1=FixPrice, 2=FixProfit).
    pub join_sell_kind: u8,
    /// Compact `ArbConfig` form, not a raw Delphi record.
    pub arb_config: ArbConfigCompact,
}

impl ClientSettingsCommand {
    /// Effective take-profit percentage shown by the main sell control.
    ///
    /// Mirrors terminal state after Delphi `ApplySettingsFromServer` calls
    /// `UpdateFixedButtons`: fixed-sell mode uses the selected `SPrice[sbNum]`
    /// preset, normal mode uses `x_sell`, and `x_sell == 0` falls back to scalp
    /// mode (`x_sell_scalp / 50`).
    pub fn effective_take_profit_percent(&self) -> f64 {
        if self.fixed_sell_mode {
            self.selected_fixed_sell_percent()
        } else if self.x_sell > 0 {
            let mut value = f64::from(self.x_sell);
            if self.x_tmode {
                value *= 10.0;
            }
            value.min(900.0)
        } else {
            f64::from(self.x_sell_scalp) / 50.0
        }
    }

    /// Delphi visible percentage for a 1-based fixed-sell preset button.
    pub fn fixed_sell_preset_percent(&self, slot_1_based: usize) -> Option<f64> {
        if !(1..=6).contains(&slot_1_based) {
            return None;
        }
        let value = f64::from(self.s_price[slot_1_based - 1]);
        Some(if self.x_tmode { value * 10.0 } else { value })
    }

    /// Fixed-sell presets shown as the six sell-price buttons.
    pub fn fixed_sell_presets(&self) -> &[f32; 6] {
        &self.s_price
    }

    /// Delphi-compatible 1-based fixed-sell slot number, clamped to `1..=6`.
    pub fn selected_fixed_sell_slot(&self) -> usize {
        usize::from(self.sb_num.clamp(1, 6))
    }

    /// Raw current fixed-sell preset value selected by [`Self::selected_fixed_sell_slot`].
    pub fn selected_fixed_sell_price(&self) -> f32 {
        self.s_price[self.selected_fixed_sell_slot() - 1]
    }

    /// Delphi visible percentage for the currently selected fixed-sell preset.
    pub fn selected_fixed_sell_percent(&self) -> f64 {
        self.fixed_sell_preset_percent(self.selected_fixed_sell_slot())
            .unwrap_or(0.0)
    }

    /// Typed join-sells mode for the multi-order "M" control.
    pub fn join_sell_mode(&self) -> JoinSellKind {
        JoinSellKind::from_byte(self.join_sell_kind)
    }

    /// Set the join-sells mode while preserving the exact Delphi byte on wire.
    pub fn set_join_sell_mode(&mut self, mode: JoinSellKind) {
        self.join_sell_kind = mode.to_byte();
    }

    /// Temporary blacklist rows as UI entries instead of parallel wire arrays.
    pub fn temp_blacklist_entries(&self) -> impl Iterator<Item = TempBlacklistEntry<'_>> {
        self.temp_bl_symbols
            .iter()
            .zip(self.temp_bl_times.iter())
            .map(|(symbol, remaining_days)| TempBlacklistEntry {
                symbol,
                remaining_days: *remaining_days,
            })
    }
}

/// CmdId=3 `TStratStartStopCommand`. Boolean IsStart.
#[derive(Debug, Clone, Copy)]
pub struct StratStartStop {
    pub uid: u64,
    pub is_start: bool,
}

/// CmdId=4 `TStratStartStopCommandV2`, carrying checked-strategy deltas.
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

/// CmdId=7 `TEmuTradesCommand` (Priority=Sliced), emulated ticks for one market.
#[derive(Debug, Clone)]
pub struct EmuTrades {
    pub uid: u64,
    pub m_index: u16,
    /// `BaseTime: TDateTime` (Delphi double, days since 1899-12-30).
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
    /// Version byte read from the incoming command. Outgoing builder always writes
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

/// Trigger-management action for [`crate::MoonSettings::manage_triggers`].
///
/// Maps the Delphi `TTriggerManageCommand.Action` byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerAction {
    /// Clear the listed triggers (Delphi `Action = 0`).
    Clear,
    /// Set/arm the listed triggers (Delphi `Action = 1`).
    Set,
}

impl TriggerAction {
    /// Delphi wire ordinal: `Clear = 0`, `Set = 1`.
    pub const fn to_byte(self) -> u8 {
        match self {
            Self::Clear => 0,
            Self::Set => 1,
        }
    }
}

/// Which profit counter to reset, for [`crate::MoonSettings::reset_profit`].
///
/// Maps the Delphi `TResetProfitCommand.ResetKind` byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResetProfitKind {
    /// Reset only the current-session profit (Delphi `ResetKind = 0`).
    CurrentProfit,
    /// Reset the all-time accumulated profit (Delphi `ResetKind = 1`).
    AllProfit,
}

impl ResetProfitKind {
    /// Delphi wire ordinal: `CurrentProfit = 0`, `AllProfit = 1`.
    pub const fn to_byte(self) -> u8 {
        match self {
            Self::CurrentProfit => 0,
            Self::AllProfit => 1,
        }
    }
}

/// CmdId=12 `TArbActivateNotify`.
#[derive(Debug, Clone, Copy)]
pub struct ArbActivateNotify {
    pub uid: u64,
    /// `ArbValid: TDateTime`.
    pub arb_valid: f64,
}

/// CmdId=13 `TSwitchDexCommand` (High, UK_DexSwitch).
/// `DexName: ShortString[15]` = 16 wire bytes: one length byte plus up to
/// 15 ASCII bytes.
#[derive(Debug, Clone)]
pub struct SwitchDex {
    pub uid: u64,
    pub dex_name: String,
}

/// Delphi `TSwitchSpotCommand.SpotIndex`.
///
/// Server accepts `0=Crypto` and `1=Predict`; raw future values are preserved
/// for forward compatibility, but normal application code should use the named
/// constants.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct SpotMarketKind(u8);

#[allow(non_upper_case_globals)]
impl SpotMarketKind {
    pub const Crypto: Self = Self(0);
    pub const Predict: Self = Self(1);

    pub const fn from_byte(value: u8) -> Self {
        Self(value)
    }

    pub const fn to_byte(self) -> u8 {
        self.0
    }

    pub const fn is_known(self) -> bool {
        self.0 <= Self::Predict.0
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Crypto => "Crypto",
            Self::Predict => "Predict",
            _ => "Unknown",
        }
    }
}

impl std::fmt::Debug for SpotMarketKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_known() {
            f.write_str(self.name())
        } else {
            write!(f, "Unknown({})", self.0)
        }
    }
}

/// CmdId=14 `TSwitchSpotCommand` (High, UK_SpotSwitch).
#[derive(Debug, Clone, Copy)]
pub struct SwitchSpot {
    pub uid: u64,
    pub spot_index: SpotMarketKind,
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
