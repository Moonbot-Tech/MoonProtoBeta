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
    Unknown {
        cmd_id: u8,
        uid: u64,
    },
}

impl UICommand {
    /// Распарсить TBaseUICommand payload (после dispatch'а по MPC_UI в data_read_int).
    /// Wire-format: `cmd_id:u8 + ver:u16 + UID:u64 + class-specific`.
    /// Version gate: ver > 3 → Unknown.
    pub fn parse(payload: &[u8]) -> Option<Self> {
        Self::parse_with_client_settings_fallback(payload, None)
    }

    /// Parse with Delphi `cfg` fallback for old/truncated `TClientSettingsCommand`
    /// soft-tail fields.
    ///
    /// Delphi `TClientSettingsCommand.CreateFromStream` reads old packets
    /// append-only: if `FreePositionCheck`, `VolDropLevel`, `UseStopMarket`,
    /// `AutoStartConfig`, hotkey prices, or `JoinSellKind` are absent, the command
    /// keeps the current local `cfg` values. The active dispatcher passes its
    /// current settings snapshot here; low-level callers can pass the same value
    /// when decoding historical payloads.
    pub fn parse_with_client_settings_fallback(
        payload: &[u8],
        client_settings_fallback: Option<&ClientSettingsCommand>,
    ) -> Option<Self> {
        if payload.len() < 11 {
            return None;
        }
        let cmd_id = payload[0];
        let ver = u16::from_le_bytes([payload[1], payload[2]]);
        let uid = u64::from_le_bytes(payload[3..11].try_into().unwrap());
        if ver > CURRENT_PROTO_CMD_VER {
            return Some(UICommand::Unknown { cmd_id, uid });
        }
        let mut pos = 11usize;

        match cmd_id {
            CMD_CLIENT_SETTINGS => {
                parse_client_settings(payload, &mut pos, uid, ver, client_settings_fallback)
                    .map(|settings| UICommand::ClientSettings(Box::new(settings)))
            }

            CMD_SETTINGS_REQUEST => Some(UICommand::SettingsRequest { uid }),

            CMD_STRAT_START_STOP => {
                let is_start = read_bool(payload, &mut pos)?;
                Some(UICommand::StratStartStop(StratStartStop { uid, is_start }))
            }

            CMD_STRAT_START_STOP_V2 => {
                let is_start = read_bool(payload, &mut pos)?;
                if pos + 2 > payload.len() {
                    return None;
                }
                let count = u16::from_le_bytes([payload[pos], payload[pos + 1]]) as usize;
                pos += 2;
                let mut items = Vec::with_capacity(count);
                for _ in 0..count {
                    if pos + 9 > payload.len() {
                        return None;
                    }
                    let strategy_id = u64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                    let checked = payload[pos] != 0;
                    pos += 1;
                    items.push(StratCheckedItem {
                        strategy_id,
                        checked,
                    });
                }
                Some(UICommand::StratStartStopV2(StratStartStopV2 {
                    uid,
                    is_start,
                    items,
                }))
            }

            CMD_MM_ORDERS_SUBSCRIBE => {
                let subscribe = read_bool(payload, &mut pos)?;
                Some(UICommand::MMOrdersSubscribe(MMOrdersSubscribe {
                    uid,
                    subscribe,
                }))
            }

            CMD_UPDATE_VERSION => {
                let version_name = read_string(payload, &mut pos)?;
                let is_release = read_bool(payload, &mut pos)?;
                Some(UICommand::UpdateVersion(UpdateVersion {
                    uid,
                    version_name,
                    is_release,
                }))
            }

            CMD_EMU_TRADES => {
                if pos + 2 + 8 + 2 > payload.len() {
                    return None;
                }
                let m_index = u16::from_le_bytes([payload[pos], payload[pos + 1]]);
                pos += 2;
                let base_time = f64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
                pos += 8;
                let count = u16::from_le_bytes([payload[pos], payload[pos + 1]]) as usize;
                pos += 2;
                if pos + count * 6 > payload.len() {
                    return None;
                }
                let mut points = Vec::with_capacity(count);
                for _ in 0..count {
                    let time_delta_ms = u16::from_le_bytes([payload[pos], payload[pos + 1]]);
                    pos += 2;
                    let price = f32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap());
                    pos += 4;
                    points.push(EmuTradePoint {
                        time_delta_ms,
                        price,
                    });
                }
                Some(UICommand::EmuTrades(EmuTrades {
                    uid,
                    m_index,
                    base_time,
                    points,
                }))
            }

            CMD_NEW_MARKET_NOTIFY => Some(UICommand::NewMarketNotify(NewMarketNotify { uid })),

            CMD_LEV_MANAGE => {
                if pos + 1 + 5 + 4 + 1 > payload.len() {
                    return None;
                }
                let cmd_ver = payload[pos];
                pos += 1;
                let auto_max_order = payload[pos] != 0;
                pos += 1;
                let auto_lev_up = payload[pos] != 0;
                pos += 1;
                let auto_isolated = payload[pos] != 0;
                pos += 1;
                let auto_cross = payload[pos] != 0;
                pos += 1;
                let auto_fix_lev = payload[pos] != 0;
                pos += 1;
                let fix_lev = i32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap());
                pos += 4;
                let tlg_report = payload[pos] != 0;
                pos += 1;
                let lev_control = read_string(payload, &mut pos)?;
                Some(UICommand::LevManage(LevManage {
                    uid,
                    cmd_ver,
                    auto_max_order,
                    auto_lev_up,
                    auto_isolated,
                    auto_cross,
                    auto_fix_lev,
                    fix_lev,
                    tlg_report,
                    lev_control,
                }))
            }

            CMD_TRIGGER_MANAGE => {
                if pos + 1 + 1 + 2 > payload.len() {
                    return None;
                }
                let action = payload[pos];
                pos += 1;
                let all_markets = payload[pos] != 0;
                pos += 1;
                let count = u16::from_le_bytes([payload[pos], payload[pos + 1]]) as usize;
                pos += 2;
                if pos + count * 2 > payload.len() {
                    return None;
                }
                let mut markets = Vec::with_capacity(count);
                for _ in 0..count {
                    markets.push(u16::from_le_bytes([payload[pos], payload[pos + 1]]));
                    pos += 2;
                }
                if pos + 2 > payload.len() {
                    return None;
                }
                let count = u16::from_le_bytes([payload[pos], payload[pos + 1]]) as usize;
                pos += 2;
                if pos + count * 2 > payload.len() {
                    return None;
                }
                let mut keys = Vec::with_capacity(count);
                for _ in 0..count {
                    keys.push(u16::from_le_bytes([payload[pos], payload[pos + 1]]));
                    pos += 2;
                }
                Some(UICommand::TriggerManage(TriggerManage {
                    uid,
                    action,
                    all_markets,
                    markets,
                    keys,
                }))
            }

            CMD_RESET_PROFIT => {
                if pos + 1 > payload.len() {
                    return None;
                }
                let reset_kind = payload[pos];
                Some(UICommand::ResetProfit(ResetProfit { uid, reset_kind }))
            }

            CMD_ARB_ACTIVATE_NOTIFY => {
                if pos + 8 > payload.len() {
                    return None;
                }
                let arb_valid = f64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
                Some(UICommand::ArbActivateNotify(ArbActivateNotify {
                    uid,
                    arb_valid,
                }))
            }

            CMD_SWITCH_DEX => {
                // ShortString[15]: byte length + up to 15 bytes content. Total wire = 16 bytes.
                if pos + 16 > payload.len() {
                    return None;
                }
                let len = payload[pos] as usize;
                let len = len.min(15);
                let name_bytes = &payload[pos + 1..pos + 1 + len];
                let dex_name = decode_utf8_delphi(name_bytes);
                Some(UICommand::SwitchDex(SwitchDex { uid, dex_name }))
            }

            CMD_SWITCH_SPOT => {
                if pos + 1 > payload.len() {
                    return None;
                }
                let spot_index = payload[pos];
                Some(UICommand::SwitchSpot(SwitchSpot { uid, spot_index }))
            }

            _ => Some(UICommand::Unknown { cmd_id, uid }),
        }
    }
}

// =============================================================================
//  TClientSettingsCommand parsing helpers
// =============================================================================

fn parse_client_settings(
    data: &[u8],
    pos: &mut usize,
    uid: u64,
    ver: u16,
    fallback: Option<&ClientSettingsCommand>,
) -> Option<ClientSettingsCommand> {
    // Fixed mandatory block: 4+4+1+1+8+4+4+8+1+4+1+1 = 41 bytes
    if *pos + 41 > data.len() {
        return None;
    }
    let x_sell = i32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    let x_sell_scalp = i32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    let x_tmode = data[*pos] != 0;
    *pos += 1;
    let fixed_sell_mode = data[*pos] != 0;
    *pos += 1;
    let fixed_sell_price = f64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    let price_drop_level = f32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    let trailing_drop = f32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    let g_take_profit = f64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    let use_g_take_profit = data[*pos] != 0;
    *pos += 1;
    let unused_spread = i32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    let panic_if_price_drop = data[*pos] != 0;
    *pos += 1;
    let emu_mode = data[*pos] != 0;
    *pos += 1;

    // v2+ fields
    let (buy_iceberg, sell_iceberg, sign_orders) = if ver >= 2 {
        if *pos + 3 > data.len() {
            return None;
        }
        let b = data[*pos] != 0;
        *pos += 1;
        let s = data[*pos] != 0;
        *pos += 1;
        let so = data[*pos] != 0;
        *pos += 1;
        (b, s, so)
    } else {
        (
            false,
            false,
            fallback.map(|f| f.sign_orders).unwrap_or(true),
        )
    };

    let coins_black_list_text = read_string(data, pos)?;
    if *pos + 1 > data.len() {
        return None;
    }
    let use_coins_black_list = data[*pos] != 0;
    *pos += 1;

    if *pos + 4 > data.len() {
        return None;
    }
    let temp_bl_count_raw = i32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    if temp_bl_count_raw < 0 {
        return None;
    }
    let temp_bl_count = temp_bl_count_raw as usize;
    // Каждый TempBL item занимает минимум string length prefix (2) + TDateTime (8).
    // Если count физически не помещается в payload, Delphi stream read would fail;
    // Rust must fail too, not silently truncate and parse tail at a wrong offset.
    if temp_bl_count > (data.len() - *pos) / 10 {
        return None;
    }
    let mut temp_bl_symbols = Vec::with_capacity(temp_bl_count);
    let mut temp_bl_times = Vec::with_capacity(temp_bl_count);
    for _ in 0..temp_bl_count {
        let sym = read_string(data, pos)?;
        if *pos + 8 > data.len() {
            return None;
        }
        let t = f64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
        *pos += 8;
        temp_bl_symbols.push(sym);
        temp_bl_times.push(t);
    }

    // Soft-read tail. Каждая проверка: `pos < len` (поле есть).
    let mut use_manual_strategy = false;
    let mut manual_strategy_id = 0u64;
    if *pos < data.len() {
        if *pos + 9 > data.len() {
            return None;
        }
        use_manual_strategy = data[*pos] != 0;
        *pos += 1;
        manual_strategy_id = u64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
        *pos += 8;
    }

    let free_position_check = if *pos < data.len() {
        if *pos + 1 > data.len() {
            return None;
        }
        let b = data[*pos] != 0;
        *pos += 1;
        b
    } else {
        fallback.map(|f| f.free_position_check).unwrap_or(false)
    };

    let vol_drop_level = if *pos < data.len() {
        if *pos + 4 > data.len() {
            return None;
        }
        let v = i32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
        *pos += 4;
        v
    } else {
        fallback.map(|f| f.vol_drop_level).unwrap_or(0)
    };

    let use_stop_market = if *pos < data.len() {
        if *pos + 1 > data.len() {
            return None;
        }
        let b = data[*pos] != 0;
        *pos += 1;
        b
    } else {
        fallback.map(|f| f.use_stop_market).unwrap_or(false)
    };

    // ASCfg: `if pos + sizeof(Word) < size`  → Delphi `<`, не `<=`, чтобы было что-то ЗА размером.
    let as_cfg = if can_read_sized_blob(data, *pos) {
        read_sized_blob_with_fallback(
            data,
            pos,
            AS_CFG_SIZE,
            fallback.map(|f| f.as_cfg.as_slice()),
        )
    } else {
        fallback.map(|f| f.as_cfg.clone()).unwrap_or_default()
    };
    let as_cfg2 = if can_read_sized_blob(data, *pos) {
        read_sized_blob_with_fallback(
            data,
            pos,
            AS_CFG2_SIZE,
            fallback.map(|f| f.as_cfg2.as_slice()),
        )
    } else {
        fallback.map(|f| f.as_cfg2.clone()).unwrap_or_default()
    };

    // SPrice/sbNum block: `if pos + 25 <= size`
    let (mut s_price, mut sb_num) = fallback
        .map(|f| (f.s_price, f.sb_num))
        .unwrap_or(([0.0f32; 6], 0u8));
    if *pos + 25 <= data.len() {
        for slot in s_price.iter_mut() {
            *slot = f32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
            *pos += 4;
        }
        sb_num = data[*pos];
        *pos += 1;
    }

    let join_sell_kind = if *pos < data.len() {
        let v = data[*pos];
        *pos += 1;
        v
    } else {
        fallback.map(|f| f.join_sell_kind).unwrap_or(0)
    };

    // ArbConfig compact (defaults out of InitArbConfigDefaults if absent or arbVer < 1).
    let mut arb_config = ArbConfigCompact::default();
    if *pos < data.len() {
        let arb_ver = data[*pos];
        *pos += 1;
        // Delphi: `if (arbVer >= 1) and (ms.Position + SizeOf(wantedSet) <= ms.Size)` → `<= size`.
        if arb_ver >= 1 && *pos + 32 <= data.len() {
            // Reset to all-false before reading bits (override default).
            arb_config = ArbConfigCompact {
                wanted: [false; 256],
                show_absolute: false,
                show_numbers: false,
                show_lines: false,
                show_percent: false,
                show_right: false,
            };
            let wanted_bytes = &data[*pos..*pos + 32];
            *pos += 32;
            for i in 0..256 {
                arb_config.wanted[i] = (wanted_bytes[i / 8] >> (i % 8)) & 1 != 0;
            }
            if *pos < data.len() {
                let flags = data[*pos];
                *pos += 1;
                arb_config.show_absolute = (flags & 0b00001) != 0;
                arb_config.show_numbers = (flags & 0b00010) != 0;
                arb_config.show_lines = (flags & 0b00100) != 0;
                arb_config.show_percent = (flags & 0b01000) != 0;
                arb_config.show_right = (flags & 0b10000) != 0;
            }
            if *pos < data.len() {
                let color_count = data[*pos] as usize;
                *pos += 1;
                // skip colorCount * 5 bytes (legacy)
                let skip = color_count * 5;
                *pos = (*pos + skip).min(data.len());
            }
        }
    }

    Some(ClientSettingsCommand {
        uid,
        x_sell,
        x_sell_scalp,
        x_tmode,
        fixed_sell_mode,
        fixed_sell_price,
        price_drop_level,
        trailing_drop,
        g_take_profit,
        use_g_take_profit,
        unused_spread,
        panic_if_price_drop,
        emu_mode,
        buy_iceberg,
        sell_iceberg,
        sign_orders,
        coins_black_list_text,
        use_coins_black_list,
        temp_bl_symbols,
        temp_bl_times,
        use_manual_strategy,
        manual_strategy_id,
        free_position_check,
        vol_drop_level,
        use_stop_market,
        as_cfg,
        as_cfg2,
        s_price,
        sb_num,
        join_sell_kind,
        arb_config,
    })
}

/// Read `sz:Word + bytes(min(sz, len_of_destination))`. Trailing `(sz - destination_size)` bytes
/// are skipped if larger.
///
/// Delphi first assigns `ASCfg := cfg.AutoStartConfig` and then reads only the
/// prefix that exists in the stream. Missing tail bytes therefore keep fallback
/// values; they are not zeroed and not truncated.
/// Delphi гвард: `pos + SizeOf(Word) < size` — оставляем как `<`.
fn read_sized_blob_with_fallback(
    data: &[u8],
    pos: &mut usize,
    destination_size: usize,
    fallback: Option<&[u8]>,
) -> Vec<u8> {
    if !can_read_sized_blob(data, *pos) {
        return fallback.map(Vec::from).unwrap_or_default();
    }
    let sz = u16::from_le_bytes([data[*pos], data[*pos + 1]]) as usize;
    *pos += 2;
    let mut blob = vec![0u8; destination_size];
    if let Some(fallback) = fallback {
        let copy = fallback.len().min(destination_size);
        blob[..copy].copy_from_slice(&fallback[..copy]);
    }

    let available = data.len().saturating_sub(*pos);
    let to_copy = sz.min(destination_size).min(available);
    blob[..to_copy].copy_from_slice(&data[*pos..*pos + to_copy]);
    *pos += sz.min(available);
    blob
}

fn can_read_sized_blob(data: &[u8], pos: usize) -> bool {
    pos + 2 < data.len()
}

fn read_bool(data: &[u8], pos: &mut usize) -> Option<bool> {
    if *pos + 1 > data.len() {
        return None;
    }
    let v = data[*pos] != 0;
    *pos += 1;
    Some(v)
}

// =============================================================================
//  Builders (C → S)
// =============================================================================

fn write_header(out: &mut Vec<u8>, cmd_id: u8, uid: u64) {
    out.push(cmd_id);
    out.extend_from_slice(&CURRENT_PROTO_CMD_VER.to_le_bytes());
    out.extend_from_slice(&uid.to_le_bytes());
}

/// Build CmdId=1 `TClientSettingsCommand`. Версия пишется как `CURRENT_PROTO_CMD_VER` (v3),
/// поэтому BuyIceberg/SellIceberg/SignOrders **всегда** идут на провод.
pub fn build_client_settings(cmd: &ClientSettingsCommand) -> Vec<u8> {
    let mut out = Vec::with_capacity(256);
    write_header(&mut out, CMD_CLIENT_SETTINGS, cmd.uid);

    out.extend_from_slice(&cmd.x_sell.to_le_bytes());
    out.extend_from_slice(&cmd.x_sell_scalp.to_le_bytes());
    out.push(cmd.x_tmode as u8);
    out.push(cmd.fixed_sell_mode as u8);
    out.extend_from_slice(&cmd.fixed_sell_price.to_le_bytes());
    out.extend_from_slice(&cmd.price_drop_level.to_le_bytes());
    out.extend_from_slice(&cmd.trailing_drop.to_le_bytes());
    out.extend_from_slice(&cmd.g_take_profit.to_le_bytes());
    out.push(cmd.use_g_take_profit as u8);
    out.extend_from_slice(&cmd.unused_spread.to_le_bytes());
    out.push(cmd.panic_if_price_drop as u8);
    out.push(cmd.emu_mode as u8);
    // v2+
    out.push(cmd.buy_iceberg as u8);
    out.push(cmd.sell_iceberg as u8);
    out.push(cmd.sign_orders as u8);

    write_string(&mut out, &cmd.coins_black_list_text);
    out.push(cmd.use_coins_black_list as u8);

    let count = cmd.temp_bl_symbols.len().min(cmd.temp_bl_times.len()) as i32;
    out.extend_from_slice(&count.to_le_bytes());
    for i in 0..count as usize {
        write_string(&mut out, &cmd.temp_bl_symbols[i]);
        out.extend_from_slice(&cmd.temp_bl_times[i].to_le_bytes());
    }

    out.push(cmd.use_manual_strategy as u8);
    out.extend_from_slice(&cmd.manual_strategy_id.to_le_bytes());

    out.push(cmd.free_position_check as u8);

    out.extend_from_slice(&cmd.vol_drop_level.to_le_bytes());
    out.push(cmd.use_stop_market as u8);

    // ASCfg / ASCfg2: всегда пишем фиксированный sz = SizeOf(record).
    out.extend_from_slice(&(AS_CFG_SIZE as u16).to_le_bytes());
    write_blob_fixed(&mut out, &cmd.as_cfg, AS_CFG_SIZE);

    out.extend_from_slice(&(AS_CFG2_SIZE as u16).to_le_bytes());
    write_blob_fixed(&mut out, &cmd.as_cfg2, AS_CFG2_SIZE);

    for v in cmd.s_price.iter() {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out.push(cmd.sb_num);

    out.push(cmd.join_sell_kind);

    // ArbConfig compact
    out.push(ARB_CONFIG_VER);
    let mut wanted_bytes = [0u8; 32];
    for i in 0..256 {
        if cmd.arb_config.wanted[i] {
            wanted_bytes[i / 8] |= 1 << (i % 8);
        }
    }
    out.extend_from_slice(&wanted_bytes);
    let mut flags = 0u8;
    if cmd.arb_config.show_absolute {
        flags |= 0b00001;
    }
    if cmd.arb_config.show_numbers {
        flags |= 0b00010;
    }
    if cmd.arb_config.show_lines {
        flags |= 0b00100;
    }
    if cmd.arb_config.show_percent {
        flags |= 0b01000;
    }
    if cmd.arb_config.show_right {
        flags |= 0b10000;
    }
    out.push(flags);
    out.push(0); // colorCount = 0 (legacy slot, не используется)

    out
}

fn write_blob_fixed(out: &mut Vec<u8>, blob: &[u8], target_size: usize) {
    if blob.len() >= target_size {
        out.extend_from_slice(&blob[..target_size]);
    } else {
        out.extend_from_slice(blob);
        out.extend(std::iter::repeat_n(0u8, target_size - blob.len()));
    }
}

/// CmdId=2 `TSettingsRequest` (empty body).
pub fn build_settings_request(uid: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(11);
    write_header(&mut out, CMD_SETTINGS_REQUEST, uid);
    out
}

/// CmdId=3 `TStratStartStopCommand`.
pub fn build_strat_start_stop(uid: u64, is_start: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(12);
    write_header(&mut out, CMD_STRAT_START_STOP, uid);
    out.push(is_start as u8);
    out
}

/// CmdId=4 `TStratStartStopCommandV2`.
pub fn build_strat_start_stop_v2(uid: u64, is_start: bool, items: &[StratCheckedItem]) -> Vec<u8> {
    let count = items.len() as u16;
    let count_usize = usize::from(count);
    let mut out = Vec::with_capacity(11 + 1 + 2 + count_usize * 9);
    write_header(&mut out, CMD_STRAT_START_STOP_V2, uid);
    out.push(is_start as u8);
    out.extend_from_slice(&count.to_le_bytes());
    for it in items.iter().take(count_usize) {
        out.extend_from_slice(&it.strategy_id.to_le_bytes());
        out.push(it.checked as u8);
    }
    out
}

/// CmdId=5 `TMMOrdersSubscribeCommand`.
pub fn build_mm_orders_subscribe(uid: u64, subscribe: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(12);
    write_header(&mut out, CMD_MM_ORDERS_SUBSCRIBE, uid);
    out.push(subscribe as u8);
    out
}

/// CmdId=6 `TUpdateVersionCommand`.
///
/// Low-level wire builder. Prefer [`crate::Client::ui_update_version`] when a
/// running client should mirror Delphi `ServerUpdateSent` behavior.
pub fn build_update_version(uid: u64, version_name: &str, is_release: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_header(&mut out, CMD_UPDATE_VERSION, uid);
    write_string(&mut out, version_name);
    out.push(is_release as u8);
    out
}

/// CmdId=7 `TEmuTradesCommand`.
pub fn build_emu_trades(
    uid: u64,
    m_index: u16,
    base_time: f64,
    points: &[EmuTradePoint],
) -> Vec<u8> {
    let count = points.len() as u16;
    let count_usize = usize::from(count);
    let mut out = Vec::with_capacity(11 + 2 + 8 + 2 + count_usize * 6);
    write_header(&mut out, CMD_EMU_TRADES, uid);
    out.extend_from_slice(&m_index.to_le_bytes());
    out.extend_from_slice(&base_time.to_le_bytes());
    out.extend_from_slice(&count.to_le_bytes());
    for p in points.iter().take(count_usize) {
        out.extend_from_slice(&p.time_delta_ms.to_le_bytes());
        out.extend_from_slice(&p.price.to_le_bytes());
    }
    out
}

/// CmdId=8 `TNewMarketNotifyCommand` (empty).
pub fn build_new_market_notify(uid: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(11);
    write_header(&mut out, CMD_NEW_MARKET_NOTIFY, uid);
    out
}

/// CmdId=9 `TLevManageCommand`. `cmd_ver` пишется как `1` (Delphi `LevCmdVer = 1`).
pub fn build_lev_manage(uid: u64, cmd: &LevManage) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_header(&mut out, CMD_LEV_MANAGE, uid);
    out.push(LEV_CMD_VER);
    out.push(cmd.auto_max_order as u8);
    out.push(cmd.auto_lev_up as u8);
    out.push(cmd.auto_isolated as u8);
    out.push(cmd.auto_cross as u8);
    out.push(cmd.auto_fix_lev as u8);
    out.extend_from_slice(&cmd.fix_lev.to_le_bytes());
    out.push(cmd.tlg_report as u8);
    write_string(&mut out, &cmd.lev_control);
    out
}

/// CmdId=10 `TTriggerManageCommand`.
pub fn build_trigger_manage(
    uid: u64,
    action: u8,
    all_markets: bool,
    markets: &[u16],
    keys: &[u16],
) -> Vec<u8> {
    let market_count = markets.len() as u16;
    let market_count_usize = usize::from(market_count);
    let key_count = keys.len() as u16;
    let key_count_usize = usize::from(key_count);
    let mut out =
        Vec::with_capacity(11 + 1 + 1 + 2 + market_count_usize * 2 + 2 + key_count_usize * 2);
    write_header(&mut out, CMD_TRIGGER_MANAGE, uid);
    out.push(action);
    out.push(all_markets as u8);
    out.extend_from_slice(&market_count.to_le_bytes());
    for m in markets.iter().take(market_count_usize) {
        out.extend_from_slice(&m.to_le_bytes());
    }
    out.extend_from_slice(&key_count.to_le_bytes());
    for k in keys.iter().take(key_count_usize) {
        out.extend_from_slice(&k.to_le_bytes());
    }
    out
}

/// CmdId=11 `TResetProfitCommand`.
pub fn build_reset_profit(uid: u64, reset_kind: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(12);
    write_header(&mut out, CMD_RESET_PROFIT, uid);
    out.push(reset_kind);
    out
}

/// CmdId=12 `TArbActivateNotify`.
pub fn build_arb_activate_notify(uid: u64, arb_valid: f64) -> Vec<u8> {
    let mut out = Vec::with_capacity(19);
    write_header(&mut out, CMD_ARB_ACTIVATE_NOTIFY, uid);
    out.extend_from_slice(&arb_valid.to_le_bytes());
    out
}

/// CmdId=13 `TSwitchDexCommand`. Передаются ровно 16 байт ShortString\[15\].
pub fn build_switch_dex(uid: u64, dex_name: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(27);
    write_header(&mut out, CMD_SWITCH_DEX, uid);
    let bytes = dex_name.as_bytes();
    let len = bytes.len().min(15) as u8;
    out.push(len);
    out.extend_from_slice(&bytes[..len as usize]);
    out.extend(std::iter::repeat_n(0u8, 15 - len as usize));
    out
}

/// CmdId=14 `TSwitchSpotCommand`.
pub fn build_switch_spot(uid: u64, spot_index: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(12);
    write_header(&mut out, CMD_SWITCH_SPOT, uid);
    out.push(spot_index);
    out
}

// =============================================================================
//  Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn header_bytes(cmd_id: u8, uid: u64) -> Vec<u8> {
        let mut v = vec![cmd_id];
        v.extend_from_slice(&CURRENT_PROTO_CMD_VER.to_le_bytes());
        v.extend_from_slice(&uid.to_le_bytes());
        v
    }

    #[test]
    fn parse_settings_request() {
        let payload = header_bytes(CMD_SETTINGS_REQUEST, 99);
        match UICommand::parse(&payload).unwrap() {
            UICommand::SettingsRequest { uid } => assert_eq!(uid, 99),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn strat_start_stop_roundtrip() {
        let raw = build_strat_start_stop(7, true);
        match UICommand::parse(&raw).unwrap() {
            UICommand::StratStartStop(s) => {
                assert_eq!(s.uid, 7);
                assert!(s.is_start);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn strat_start_stop_v2_roundtrip() {
        let items = vec![
            StratCheckedItem {
                strategy_id: 10,
                checked: true,
            },
            StratCheckedItem {
                strategy_id: 20,
                checked: false,
            },
            StratCheckedItem {
                strategy_id: 30,
                checked: true,
            },
        ];
        let raw = build_strat_start_stop_v2(42, false, &items);
        match UICommand::parse(&raw).unwrap() {
            UICommand::StratStartStopV2(s) => {
                assert_eq!(s.uid, 42);
                assert!(!s.is_start);
                assert_eq!(s.items, items);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn mm_orders_subscribe_roundtrip() {
        let raw = build_mm_orders_subscribe(1, true);
        match UICommand::parse(&raw).unwrap() {
            UICommand::MMOrdersSubscribe(m) => assert!(m.subscribe),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn update_version_roundtrip() {
        let raw = build_update_version(2, "MoonBot-7.99", true);
        match UICommand::parse(&raw).unwrap() {
            UICommand::UpdateVersion(u) => {
                assert_eq!(u.version_name, "MoonBot-7.99");
                assert!(u.is_release);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn emu_trades_roundtrip() {
        let points = vec![
            EmuTradePoint {
                time_delta_ms: 0,
                price: 100.5,
            },
            EmuTradePoint {
                time_delta_ms: 1500,
                price: -101.2,
            }, // sell
            EmuTradePoint {
                time_delta_ms: 3000,
                price: 99.8,
            },
        ];
        let raw = build_emu_trades(3, 42, 45123.5, &points);
        match UICommand::parse(&raw).unwrap() {
            UICommand::EmuTrades(e) => {
                assert_eq!(e.m_index, 42);
                assert_eq!(e.base_time, 45123.5);
                assert_eq!(e.points, points);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn lev_manage_roundtrip() {
        let cmd = LevManage {
            uid: 5,
            cmd_ver: 77,
            auto_max_order: true,
            auto_lev_up: false,
            auto_isolated: true,
            auto_cross: false,
            auto_fix_lev: true,
            fix_lev: 25,
            tlg_report: true,
            lev_control: "BTC,ETH".to_string(),
        };
        let raw = build_lev_manage(5, &cmd);
        match UICommand::parse(&raw).unwrap() {
            UICommand::LevManage(l) => {
                assert_eq!(l.uid, 5);
                assert_eq!(l.cmd_ver, 1);
                assert!(l.auto_max_order);
                assert!(!l.auto_lev_up);
                assert!(l.auto_isolated);
                assert_eq!(l.fix_lev, 25);
                assert_eq!(l.lev_control, "BTC,ETH");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn trigger_manage_roundtrip() {
        let markets = vec![1u16, 2, 3, 4, 5];
        let keys = vec![10u16, 20, 30];
        let raw = build_trigger_manage(11, 1, false, &markets, &keys);
        match UICommand::parse(&raw).unwrap() {
            UICommand::TriggerManage(t) => {
                assert_eq!(t.action, 1);
                assert!(!t.all_markets);
                assert_eq!(t.markets, markets);
                assert_eq!(t.keys, keys);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn word_count_builders_write_only_declared_wrapped_count_like_delphi() {
        let items: Vec<_> = (0..65_537u64)
            .map(|i| StratCheckedItem {
                strategy_id: i + 100,
                checked: i % 2 == 0,
            })
            .collect();
        let raw = build_strat_start_stop_v2(42, true, &items);
        assert_eq!(raw.len(), 11 + 1 + 2 + 9);
        match UICommand::parse(&raw).unwrap() {
            UICommand::StratStartStopV2(s) => {
                assert!(s.is_start);
                assert_eq!(s.items, vec![items[0]]);
            }
            _ => panic!("wrong variant"),
        }

        let points = vec![
            EmuTradePoint {
                time_delta_ms: 123,
                price: -77.5,
            };
            65_537
        ];
        let raw = build_emu_trades(3, 42, 45123.5, &points);
        assert_eq!(raw.len(), 11 + 2 + 8 + 2 + 6);
        match UICommand::parse(&raw).unwrap() {
            UICommand::EmuTrades(e) => {
                assert_eq!(e.points, vec![points[0]]);
            }
            _ => panic!("wrong variant"),
        }

        let markets: Vec<_> = (0..65_537usize).map(|i| i as u16).collect();
        let keys: Vec<_> = (0..65_537usize)
            .map(|i| i.wrapping_add(900) as u16)
            .collect();
        let raw = build_trigger_manage(11, 1, false, &markets, &keys);
        assert_eq!(raw.len(), 11 + 1 + 1 + 2 + 2 + 2 + 2);
        match UICommand::parse(&raw).unwrap() {
            UICommand::TriggerManage(t) => {
                assert_eq!(t.markets, vec![markets[0]]);
                assert_eq!(t.keys, vec![keys[0]]);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn reset_profit_roundtrip() {
        let raw = build_reset_profit(8, 1);
        match UICommand::parse(&raw).unwrap() {
            UICommand::ResetProfit(r) => assert_eq!(r.reset_kind, 1),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn arb_activate_notify_roundtrip() {
        let raw = build_arb_activate_notify(9, 45678.25);
        match UICommand::parse(&raw).unwrap() {
            UICommand::ArbActivateNotify(a) => assert_eq!(a.arb_valid, 45678.25),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn switch_dex_truncates_to_15() {
        let raw = build_switch_dex(13, "VeryLongDexName_OverflowExtra");
        match UICommand::parse(&raw).unwrap() {
            UICommand::SwitchDex(s) => {
                assert_eq!(s.uid, 13);
                assert_eq!(s.dex_name, "VeryLongDexName"); // 15 chars
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn switch_dex_short_name() {
        let raw = build_switch_dex(14, "Uni");
        match UICommand::parse(&raw).unwrap() {
            UICommand::SwitchDex(s) => assert_eq!(s.dex_name, "Uni"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn switch_dex_invalid_utf8_uses_delphi_question_mark_fallback() {
        let mut raw = Vec::new();
        write_header(&mut raw, CMD_SWITCH_DEX, 16);
        raw.push(4);
        raw.extend_from_slice(&[b'D', 0xFF, b'X', 0x80]);
        raw.extend_from_slice(&[0; 11]);

        match UICommand::parse(&raw).unwrap() {
            UICommand::SwitchDex(s) => assert_eq!(s.dex_name, "D?X?"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn switch_spot_roundtrip() {
        let raw = build_switch_spot(15, 1);
        match UICommand::parse(&raw).unwrap() {
            UICommand::SwitchSpot(s) => assert_eq!(s.spot_index, 1),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn new_market_notify_empty() {
        let raw = build_new_market_notify(20);
        match UICommand::parse(&raw).unwrap() {
            UICommand::NewMarketNotify(n) => assert_eq!(n.uid, 20),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn client_settings_roundtrip_full() {
        let mut wanted = [false; 256];
        wanted[0] = true;
        wanted[1] = true;
        wanted[100] = true;
        wanted[255] = true;

        let cmd = ClientSettingsCommand {
            uid: 1,
            x_sell: 50,
            x_sell_scalp: 10,
            x_tmode: true,
            fixed_sell_mode: false,
            fixed_sell_price: 0.05,
            price_drop_level: 1.5,
            trailing_drop: 0.5,
            g_take_profit: 100.0,
            use_g_take_profit: true,
            unused_spread: 0,
            panic_if_price_drop: true,
            emu_mode: false,
            buy_iceberg: true,
            sell_iceberg: false,
            sign_orders: true,
            coins_black_list_text: "BTC,ETH".to_string(),
            use_coins_black_list: true,
            temp_bl_symbols: vec!["DOGE".to_string(), "SHIB".to_string()],
            temp_bl_times: vec![0.001, 0.002],
            use_manual_strategy: true,
            manual_strategy_id: 9999,
            free_position_check: true,
            vol_drop_level: 50,
            use_stop_market: true,
            as_cfg: vec![0xAAu8; AS_CFG_SIZE],
            as_cfg2: vec![0xBBu8; AS_CFG2_SIZE],
            s_price: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            sb_num: 7,
            join_sell_kind: 2,
            arb_config: ArbConfigCompact {
                wanted,
                show_absolute: true,
                show_numbers: false,
                show_lines: true,
                show_percent: false,
                show_right: true,
            },
        };
        let raw = build_client_settings(&cmd);
        match UICommand::parse(&raw).unwrap() {
            UICommand::ClientSettings(p) => {
                assert_eq!(p.uid, 1);
                assert_eq!(p.x_sell, 50);
                assert_eq!(p.fixed_sell_price, 0.05);
                assert!(p.buy_iceberg);
                assert!(!p.sell_iceberg);
                assert!(p.sign_orders);
                assert_eq!(p.coins_black_list_text, "BTC,ETH");
                assert_eq!(
                    p.temp_bl_symbols,
                    vec!["DOGE".to_string(), "SHIB".to_string()]
                );
                assert_eq!(p.temp_bl_times, vec![0.001, 0.002]);
                assert_eq!(p.manual_strategy_id, 9999);
                assert_eq!(p.s_price, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
                assert_eq!(p.sb_num, 7);
                assert_eq!(p.join_sell_kind, 2);
                assert_eq!(p.as_cfg.len(), AS_CFG_SIZE);
                assert_eq!(p.as_cfg2.len(), AS_CFG2_SIZE);
                assert!(p.arb_config.wanted[0]);
                assert!(p.arb_config.wanted[1]);
                assert!(p.arb_config.wanted[100]);
                assert!(p.arb_config.wanted[255]);
                assert!(!p.arb_config.wanted[2]);
                assert!(p.arb_config.show_absolute);
                assert!(!p.arb_config.show_numbers);
                assert!(p.arb_config.show_lines);
                assert!(!p.arb_config.show_percent);
                assert!(p.arb_config.show_right);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn client_settings_soft_tail_uses_delphi_cfg_fallback() {
        let mut raw = Vec::new();
        raw.push(CMD_CLIENT_SETTINGS);
        raw.extend_from_slice(&1u16.to_le_bytes());
        raw.extend_from_slice(&7u64.to_le_bytes());
        raw.extend_from_slice(&[0u8; 41]);
        write_string(&mut raw, "");
        raw.push(0);
        raw.extend_from_slice(&0i32.to_le_bytes());

        let fallback = ClientSettingsCommand {
            sign_orders: false,
            free_position_check: true,
            vol_drop_level: 77,
            use_stop_market: true,
            as_cfg: vec![0xAA, 0xAB],
            as_cfg2: vec![0xBA, 0xBB],
            s_price: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            sb_num: 9,
            join_sell_kind: 2,
            ..ClientSettingsCommand::default()
        };

        match UICommand::parse_with_client_settings_fallback(&raw, Some(&fallback)).unwrap() {
            UICommand::ClientSettings(p) => {
                assert_eq!(p.uid, 7);
                assert!(!p.sign_orders, "ver<2 keeps Delphi cfg SignOrders");
                assert!(!p.use_manual_strategy);
                assert_eq!(p.manual_strategy_id, 0);
                assert!(p.free_position_check);
                assert_eq!(p.vol_drop_level, 77);
                assert!(p.use_stop_market);
                assert_eq!(p.as_cfg, vec![0xAA, 0xAB]);
                assert_eq!(p.as_cfg2, vec![0xBA, 0xBB]);
                assert_eq!(p.s_price, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
                assert_eq!(p.sb_num, 9);
                assert_eq!(p.join_sell_kind, 2);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn client_settings_short_ascfg_overlays_delphi_cfg_fallback() {
        let mut raw = Vec::new();
        raw.push(CMD_CLIENT_SETTINGS);
        raw.extend_from_slice(&3u16.to_le_bytes());
        raw.extend_from_slice(&7u64.to_le_bytes());
        raw.extend_from_slice(&[0u8; 41]);
        raw.extend_from_slice(&[0u8; 3]); // BuyIceberg, SellIceberg, SignOrders
        write_string(&mut raw, "");
        raw.push(0);
        raw.extend_from_slice(&0i32.to_le_bytes());
        raw.push(0); // UseManualStrategy
        raw.extend_from_slice(&0u64.to_le_bytes());
        raw.push(0); // FreePositionCheck
        raw.extend_from_slice(&0i32.to_le_bytes());
        raw.push(0); // UseStopMarket
        raw.extend_from_slice(&2u16.to_le_bytes());
        raw.extend_from_slice(&[0x11, 0x22]);

        let fallback_as_cfg: Vec<u8> = (0..AS_CFG_SIZE).map(|i| i as u8).collect();
        let fallback_as_cfg2: Vec<u8> = (0..AS_CFG2_SIZE).map(|i| 255u8 - i as u8).collect();
        let fallback = ClientSettingsCommand {
            as_cfg: fallback_as_cfg.clone(),
            as_cfg2: fallback_as_cfg2.clone(),
            ..ClientSettingsCommand::default()
        };

        match UICommand::parse_with_client_settings_fallback(&raw, Some(&fallback)).unwrap() {
            UICommand::ClientSettings(p) => {
                assert_eq!(p.as_cfg.len(), AS_CFG_SIZE);
                assert_eq!(&p.as_cfg[..2], &[0x11, 0x22]);
                assert_eq!(&p.as_cfg[2..], &fallback_as_cfg[2..]);
                assert_eq!(p.as_cfg2, fallback_as_cfg2);
            }
            _ => panic!("wrong variant"),
        }
    }

    fn client_settings_v1_prefix_with_temp_bl_count(count: i32) -> Vec<u8> {
        let mut raw = Vec::new();
        raw.push(CMD_CLIENT_SETTINGS);
        raw.extend_from_slice(&1u16.to_le_bytes());
        raw.extend_from_slice(&7u64.to_le_bytes());
        raw.extend_from_slice(&[0u8; 41]);
        write_string(&mut raw, "");
        raw.push(0);
        raw.extend_from_slice(&count.to_le_bytes());
        raw
    }

    #[test]
    fn client_settings_rejects_impossible_temp_bl_count_without_silent_truncate() {
        let mut raw = client_settings_v1_prefix_with_temp_bl_count(2);
        write_string(&mut raw, "A");
        raw.extend_from_slice(&1.0f64.to_le_bytes());

        assert!(
            UICommand::parse(&raw).is_none(),
            "Delphi reads exactly TempBLCount items; Rust must not truncate count and parse tail at a wrong offset"
        );
    }

    #[test]
    fn client_settings_rejects_negative_temp_bl_count_like_corrupt_stream() {
        let raw = client_settings_v1_prefix_with_temp_bl_count(-1);

        assert!(UICommand::parse(&raw).is_none());
    }

    #[test]
    fn version_gate_returns_unknown() {
        let mut payload = vec![CMD_CLIENT_SETTINGS, 99, 0];
        payload.extend_from_slice(&77u64.to_le_bytes());
        match UICommand::parse(&payload).unwrap() {
            UICommand::Unknown { cmd_id, uid } => {
                assert_eq!(cmd_id, CMD_CLIENT_SETTINGS);
                assert_eq!(uid, 77);
            }
            _ => panic!("wrong variant"),
        }
    }
}
