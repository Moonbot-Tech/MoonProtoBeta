//! Inbound `MPC_UI` command parser.

#![cfg_attr(feature = "diagnostics", allow(dead_code))]

use super::*;
impl UICommand {
    /// Parse a TBaseUICommand payload (after MPC_UI dispatch in data_read_int).
    /// Wire-format: `cmd_id:u8 + ver:u16 + UID:u64 + class-specific`.
    /// Version gate: ver > 3 -> [`UICommand::Skipped`], matching Delphi
    /// registry `FSkipped`.
    #[doc(hidden)]
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
    #[doc(hidden)]
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
            return Some(UICommand::Skipped { cmd_id, uid, ver });
        }
        let mut pos = 11usize;

        match cmd_id {
            CMD_CLIENT_SETTINGS => {
                parse_client_settings(payload, &mut pos, uid, ver, client_settings_fallback)
                    .map(|settings| UICommand::ClientSettings(Box::new(settings)))
            }

            CMD_SETTINGS_REQUEST => Some(UICommand::SettingsRequest { uid }),

            CMD_STRAT_START_STOP => {
                let is_start = read_bool_zero_tail(payload, &mut pos);
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
                    items.push(StratCheckedItem::read_from_delphi_stream(payload, &mut pos));
                }
                Some(UICommand::StratStartStopV2(StratStartStopV2 {
                    uid,
                    is_start,
                    items,
                }))
            }

            CMD_MM_ORDERS_SUBSCRIBE => {
                let subscribe = read_bool_zero_tail(payload, &mut pos);
                Some(UICommand::MMOrdersSubscribe(MMOrdersSubscribe {
                    uid,
                    subscribe,
                }))
            }

            CMD_UPDATE_VERSION => {
                let version_name = read_string(payload, &mut pos)?;
                let is_release = read_bool_zero_tail(payload, &mut pos);
                Some(UICommand::UpdateVersion(UpdateVersion {
                    uid,
                    version_name,
                    is_release,
                }))
            }

            CMD_EMU_TRADES => {
                let m_index = read_u16_zero_tail(payload, &mut pos);
                let base_time = f64::from_bits(read_u64_zero_tail(payload, &mut pos));
                let count = read_u16_zero_tail(payload, &mut pos) as usize;
                let mut points = Vec::with_capacity(count);
                for _ in 0..count {
                    points.push(EmuTradePoint::read_from_delphi_stream(payload, &mut pos));
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
                let markets = read_word_array_zero_tail(payload, &mut pos, count);
                let keys_count = read_u16_preserve_tail(payload, &mut pos, count as u16) as usize;
                let keys = read_word_array_zero_tail(payload, &mut pos, keys_count);
                Some(UICommand::TriggerManage(TriggerManage {
                    uid,
                    action,
                    all_markets,
                    markets,
                    keys,
                }))
            }

            CMD_RESET_PROFIT => {
                let kind = ResetProfitKind::from_byte(read_u8_zero_tail(payload, &mut pos));
                Some(UICommand::ResetProfit(ResetProfit {
                    #[cfg(any(test, feature = "diagnostics"))]
                    uid,
                    kind,
                }))
            }

            CMD_ARB_ACTIVATE_NOTIFY => {
                let arb_valid = f64::from_bits(read_u64_zero_tail(payload, &mut pos));
                Some(UICommand::ArbActivateNotify(ArbActivateNotify {
                    uid,
                    arb_valid,
                }))
            }

            CMD_SWITCH_DEX => {
                // ShortString[15]: byte length + up to 15 bytes content. Total wire = 16 bytes.
                let dex_name = read_short_string15_zero_tail(payload, &mut pos);
                Some(UICommand::SwitchDex(SwitchDex { uid, dex_name }))
            }

            CMD_SWITCH_SPOT => {
                let spot_index = SpotMarketKind::from_byte(read_u8_zero_tail(payload, &mut pos));
                Some(UICommand::SwitchSpot(SwitchSpot { uid, spot_index }))
            }

            CMD_ALERT_OBJECT => {
                let obj_uid = read_u64_zero_tail(payload, &mut pos);
                let upsert = read_bool_zero_tail(payload, &mut pos);
                let market_name = read_string(payload, &mut pos)?;
                if pos + 4 > payload.len() {
                    return None;
                }
                let len = i32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap());
                pos += 4;
                let skipped = len < 0 || (len as usize) > payload.len().saturating_sub(pos);
                let blob = if skipped {
                    Vec::new()
                } else if len > 0 {
                    payload[pos..pos + len as usize].to_vec()
                } else {
                    Vec::new()
                };
                Some(UICommand::AlertObject(AlertObjectCommand {
                    uid,
                    market_name,
                    obj_uid,
                    upsert,
                    blob,
                    skipped,
                }))
            }

            CMD_ALERT_SNAPSHOT_REQUEST => Some(UICommand::AlertSnapshotRequest { uid }),

            CMD_CHART_TEXT_STATE => {
                let market_name = read_string(payload, &mut pos)?;
                let need_filters = read_bool_zero_tail(payload, &mut pos);
                let need_debug_lines = read_bool_zero_tail(payload, &mut pos);
                Some(UICommand::ChartTextState(ChartTextStateCommand {
                    uid,
                    market_name,
                    need_filters,
                    need_debug_lines,
                }))
            }

            CMD_CHART_TEXT_SNAPSHOT => {
                let market_name = read_string(payload, &mut pos)?;

                if pos + 2 > payload.len() {
                    return None;
                }
                let filter_count = u16::from_le_bytes([payload[pos], payload[pos + 1]]) as usize;
                pos += 2;
                let mut filter_lines = Vec::with_capacity(filter_count);
                for _ in 0..filter_count {
                    filter_lines.push(read_string(payload, &mut pos)?);
                }

                if pos + 2 > payload.len() {
                    return None;
                }
                let debug_count = u16::from_le_bytes([payload[pos], payload[pos + 1]]) as usize;
                pos += 2;
                let mut debug_lines = Vec::with_capacity(debug_count);
                for _ in 0..debug_count {
                    debug_lines.push(read_string(payload, &mut pos)?);
                }

                Some(UICommand::ChartTextSnapshot(ChartTextSnapshotCommand {
                    uid,
                    market_name,
                    filter_lines,
                    debug_lines,
                }))
            }

            _ => Some(UICommand::Unknown { cmd_id, uid }),
        }
    }
}

impl EmuTradePoint {
    fn read_from_delphi_stream(data: &[u8], pos: &mut usize) -> Self {
        Self {
            time_delta_ms: read_u16_zero_tail(data, pos),
            price: f32::from_bits(read_u32_zero_tail(data, pos)),
        }
    }
}

fn read_short_string15_zero_tail(data: &[u8], pos: &mut usize) -> String {
    let mut bytes = [0u8; 16];
    read_into_prefix(data, pos, &mut bytes);
    let len = bytes[0] as usize;
    let len = len.min(15);
    decode_utf8_delphi(&bytes[1..1 + len])
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
    let use_coins_black_list = read_bool_zero_tail(data, pos);

    let temp_bl_count_raw = read_i32_zero_tail(data, pos);
    if temp_bl_count_raw < 0 {
        return None;
    }
    let temp_bl_count = temp_bl_count_raw as usize;
    let temp_bl_capacity =
        bounded_collection_capacity(data, *pos, temp_bl_count, TEMP_BL_MIN_WIRE_ITEM_SIZE);
    let mut temp_bl_symbols = Vec::new();
    temp_bl_symbols.try_reserve(temp_bl_capacity).ok()?;
    let mut temp_bl_times = Vec::new();
    temp_bl_times.try_reserve(temp_bl_capacity).ok()?;
    for _ in 0..temp_bl_count {
        let sym = read_string(data, pos)?;
        let t = f64::from_bits(read_u64_zero_tail(data, pos));
        temp_bl_symbols.push(sym);
        temp_bl_times.push(t);
    }

    // Soft-read tail. Each check: `pos < len` (the field is present).
    let mut use_manual_strategy = false;
    let mut manual_strategy_id = 0u64;
    if *pos < data.len() {
        use_manual_strategy = read_bool_zero_tail(data, pos);
        manual_strategy_id = read_u64_zero_tail(data, pos);
    }

    let free_position_check = if *pos < data.len() {
        read_bool_zero_tail(data, pos)
    } else {
        fallback.map(|f| f.free_position_check).unwrap_or(false)
    };

    let vol_drop_level = if *pos < data.len() {
        read_i32_preserve_tail(data, pos, fallback.map(|f| f.vol_drop_level).unwrap_or(0))
    } else {
        fallback.map(|f| f.vol_drop_level).unwrap_or(0)
    };

    let use_stop_market = if *pos < data.len() {
        read_bool_zero_tail(data, pos)
    } else {
        fallback.map(|f| f.use_stop_market).unwrap_or(false)
    };

    // ASCfg: `if pos + sizeof(Word) < size` -> Delphi uses `<`, not `<=`, so there is something PAST the size field.
    let as_cfg = if can_read_sized_blob(data, *pos) {
        read_sized_autostart_config_with_fallback(data, pos, fallback.map(|f| f.as_cfg.as_slice()))
    } else {
        fallback.map(|f| f.as_cfg.clone()).unwrap_or_default()
    };
    let as_cfg2 = if can_read_sized_blob(data, *pos) {
        read_sized_autostart_config2_with_fallback(
            data,
            pos,
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
        // Delphi: `if (arbVer >= 1) and (ms.Position + SizeOf(wantedSet) <= ms.Size)` -> `<= size`.
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
/// Delphi guard: `pos + SizeOf(Word) < size` — kept as `<`.
fn read_sized_autostart_config_with_fallback(
    data: &[u8],
    pos: &mut usize,
    fallback: Option<&[u8]>,
) -> Vec<u8> {
    let bytes = read_sized_fixed_blob_with_fallback::<AS_CFG_SIZE>(data, pos, fallback);
    WireAutoStartConfig::from_blob(&bytes).as_blob()
}

fn read_sized_autostart_config2_with_fallback(
    data: &[u8],
    pos: &mut usize,
    fallback: Option<&[u8]>,
) -> Vec<u8> {
    let bytes = read_sized_fixed_blob_with_fallback::<AS_CFG2_SIZE>(data, pos, fallback);
    WireAutoStartConfig2::from_blob(&bytes).as_blob()
}

fn read_sized_fixed_blob_with_fallback<const N: usize>(
    data: &[u8],
    pos: &mut usize,
    fallback: Option<&[u8]>,
) -> [u8; N] {
    if !can_read_sized_blob(data, *pos) {
        let mut blob = [0u8; N];
        if let Some(fallback) = fallback {
            copy_blob_prefix(&mut blob, fallback);
        }
        return blob;
    }
    let sz = u16::from_le_bytes([data[*pos], data[*pos + 1]]) as usize;
    *pos += 2;
    let mut blob = [0u8; N];
    if let Some(fallback) = fallback {
        copy_blob_prefix(&mut blob, fallback);
    }

    let available = data.len().saturating_sub(*pos);
    let to_copy = sz.min(N).min(available);
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

fn read_bool_zero_tail(data: &[u8], pos: &mut usize) -> bool {
    read_u8_zero_tail(data, pos) != 0
}

fn read_u8_zero_tail(data: &[u8], pos: &mut usize) -> u8 {
    let mut bytes = [0u8; 1];
    read_into_prefix(data, pos, &mut bytes);
    bytes[0]
}

fn read_u16_zero_tail(data: &[u8], pos: &mut usize) -> u16 {
    let mut bytes = [0u8; 2];
    read_into_prefix(data, pos, &mut bytes);
    u16::from_le_bytes(bytes)
}

fn read_u32_zero_tail(data: &[u8], pos: &mut usize) -> u32 {
    let mut bytes = [0u8; 4];
    read_into_prefix(data, pos, &mut bytes);
    u32::from_le_bytes(bytes)
}

fn read_u64_zero_tail(data: &[u8], pos: &mut usize) -> u64 {
    let mut bytes = [0u8; 8];
    read_into_prefix(data, pos, &mut bytes);
    u64::from_le_bytes(bytes)
}

fn read_i32_zero_tail(data: &[u8], pos: &mut usize) -> i32 {
    let mut bytes = [0u8; 4];
    read_into_prefix(data, pos, &mut bytes);
    i32::from_le_bytes(bytes)
}

fn read_i32_preserve_tail(data: &[u8], pos: &mut usize, current: i32) -> i32 {
    let mut bytes = current.to_le_bytes();
    read_into_prefix(data, pos, &mut bytes);
    i32::from_le_bytes(bytes)
}

const TEMP_BL_MIN_WIRE_ITEM_SIZE: usize = 10; // string length prefix + f64 time

fn bounded_collection_capacity(
    data: &[u8],
    pos: usize,
    count: usize,
    min_item_size: usize,
) -> usize {
    if min_item_size == 0 {
        return count;
    }
    data.len()
        .saturating_sub(pos)
        .checked_div(min_item_size)
        .map_or(count, |max| count.min(max))
}

fn read_u16_preserve_tail(data: &[u8], pos: &mut usize, current: u16) -> u16 {
    let mut bytes = current.to_le_bytes();
    read_into_prefix(data, pos, &mut bytes);
    u16::from_le_bytes(bytes)
}

fn read_word_array_zero_tail(data: &[u8], pos: &mut usize, count: usize) -> Vec<u16> {
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(read_u16_zero_tail(data, pos));
    }
    values
}

fn read_into_prefix(data: &[u8], pos: &mut usize, dst: &mut [u8]) {
    let n = data.len().saturating_sub(*pos).min(dst.len());
    if n > 0 {
        dst[..n].copy_from_slice(&data[*pos..*pos + n]);
        *pos += n;
    }
}
