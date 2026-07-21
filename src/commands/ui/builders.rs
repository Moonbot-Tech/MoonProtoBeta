//! Outbound `MPC_UI` command builders.

use super::*;

/// Build CmdId=1 `TClientSettingsCommand`. The version is written as `CURRENT_PROTO_CMD_VER` (v4),
/// so BuyIceberg/SellIceberg/SignOrders **always** go on the wire.
#[doc(hidden)]
pub(crate) fn build_client_settings(cmd: &ClientSettingsCommand) -> Vec<u8> {
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

    // ASCfg / ASCfg2: always write a fixed sz = SizeOf(record).
    out.extend_from_slice(&(AS_CFG_SIZE as u16).to_le_bytes());
    write_autostart_config(&mut out, &cmd.as_cfg);

    out.extend_from_slice(&(AS_CFG2_SIZE as u16).to_le_bytes());
    write_autostart_config2(&mut out, &cmd.as_cfg2);

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
    out.push(0); // colorCount = 0 (legacy slot, unused)
    out.push(cmd.trailing_stop as u8);

    out
}

fn write_autostart_config(out: &mut Vec<u8>, blob: &[u8]) {
    out.extend_from_slice(WireAutoStartConfig::from_blob(blob).as_bytes());
}

fn write_autostart_config2(out: &mut Vec<u8>, blob: &[u8]) {
    out.extend_from_slice(WireAutoStartConfig2::from_blob(blob).as_bytes());
}

/// CmdId=2 `TSettingsRequest` (empty body).
#[doc(hidden)]
pub(crate) fn build_settings_request(uid: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(11);
    write_header(&mut out, CMD_SETTINGS_REQUEST, uid);
    out
}

/// CmdId=3 `TStratStartStopCommand`.
#[doc(hidden)]
pub(crate) fn build_strat_start_stop(uid: u64, is_start: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(12);
    write_header(&mut out, CMD_STRAT_START_STOP, uid);
    out.push(is_start as u8);
    out
}

/// CmdId=4 `TStratStartStopCommandV2`.
#[doc(hidden)]
pub(crate) fn build_strat_start_stop_v2(
    uid: u64,
    is_start: bool,
    items: &[StratCheckedItem],
) -> Vec<u8> {
    let count = items.len() as u16;
    let count_usize = usize::from(count);
    let mut out = Vec::with_capacity(11 + 1 + 2 + count_usize * 9);
    write_header(&mut out, CMD_STRAT_START_STOP_V2, uid);
    out.push(is_start as u8);
    out.extend_from_slice(&count.to_le_bytes());
    for it in items.iter().take(count_usize) {
        it.write_to(&mut out);
    }
    out
}

/// CmdId=5 `TMMOrdersSubscribeCommand`.
#[doc(hidden)]
pub(crate) fn build_mm_orders_subscribe(uid: u64, subscribe: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(12);
    write_header(&mut out, CMD_MM_ORDERS_SUBSCRIBE, uid);
    out.push(subscribe as u8);
    out
}

/// CmdId=6 `TUpdateVersionCommand`.
///
/// Low-level wire builder. Prefer [`crate::Client::ui_update_version`] when a
/// running client should mirror Delphi `ServerUpdateSent` behavior.
#[doc(hidden)]
pub(crate) fn build_update_version(uid: u64, version_name: &str, is_release: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_header(&mut out, CMD_UPDATE_VERSION, uid);
    write_string(&mut out, version_name);
    out.push(is_release as u8);
    out
}

/// CmdId=7 `TEmuTradesCommand`.
#[doc(hidden)]
pub(crate) fn build_emu_trades(
    uid: u64,
    m_index: u16,
    base_time: f64,
    points: &[EmuTradePoint],
) -> Vec<u8> {
    let count = points.len().min(usize::from(u16::MAX)) as u16;
    let count_usize = usize::from(count);
    let mut out = Vec::with_capacity(11 + 2 + 8 + 2 + count_usize * 6);
    write_header(&mut out, CMD_EMU_TRADES, uid);
    out.extend_from_slice(&m_index.to_le_bytes());
    out.extend_from_slice(&base_time.to_le_bytes());
    out.extend_from_slice(&count.to_le_bytes());
    for p in points.iter().take(count_usize) {
        p.write_to(&mut out);
    }
    out
}

/// CmdId=8 `TNewMarketNotifyCommand` (empty, server → client).
///
/// Crate-internal test helper: Active Lib treats this command as an inbound
/// listing-refresh wake-up, not as client-send API.
#[cfg(test)]
pub(crate) fn build_new_market_notify(uid: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(11);
    write_header(&mut out, CMD_NEW_MARKET_NOTIFY, uid);
    out
}

/// CmdId=9 `TLevManageCommand`. `cmd_ver` is written as `1` (Delphi `LevCmdVer = 1`).
#[doc(hidden)]
pub(crate) fn build_lev_manage(uid: u64, cmd: &LevManage) -> Vec<u8> {
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
#[doc(hidden)]
pub(crate) fn build_trigger_manage(
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
#[doc(hidden)]
pub(crate) fn build_reset_profit(uid: u64, reset_kind: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(12);
    write_header(&mut out, CMD_RESET_PROFIT, uid);
    out.push(reset_kind);
    out
}

/// CmdId=12 `TArbActivateNotify`.
#[doc(hidden)]
pub(crate) fn build_arb_activate_notify(uid: u64, arb_valid: f64) -> Vec<u8> {
    let mut out = Vec::with_capacity(19);
    write_header(&mut out, CMD_ARB_ACTIVATE_NOTIFY, uid);
    out.extend_from_slice(&arb_valid.to_le_bytes());
    out
}

/// CmdId=13 `TSwitchDexCommand`. Exactly 16 bytes of ShortString\[15\] are sent.
#[doc(hidden)]
pub(crate) fn build_switch_dex(uid: u64, dex_name: &str) -> Vec<u8> {
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
#[doc(hidden)]
pub(crate) fn build_switch_spot(uid: u64, spot_index: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(12);
    write_header(&mut out, CMD_SWITCH_SPOT, uid);
    out.push(spot_index);
    out
}

/// CmdId=15 `TAlertObjectCommand`.
#[doc(hidden)]
pub(crate) fn build_alert_object(cmd: &AlertObjectCommand) -> Vec<u8> {
    let len = if cmd.upsert {
        cmd.blob.len().min(i32::MAX as usize)
    } else {
        0
    };
    let mut out = Vec::with_capacity(11 + 8 + 1 + 2 + cmd.market_name.len() + 4 + len);
    write_header(&mut out, CMD_ALERT_OBJECT, cmd.uid);
    out.extend_from_slice(&cmd.obj_uid.to_le_bytes());
    out.push(cmd.upsert as u8);
    write_string(&mut out, &cmd.market_name);
    out.extend_from_slice(&(len as i32).to_le_bytes());
    if len > 0 {
        out.extend_from_slice(&cmd.blob[..len]);
    }
    out
}

/// CmdId=16 `TAlertSnapshotRequest` (empty body).
#[doc(hidden)]
pub(crate) fn build_alert_snapshot_request(uid: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(11);
    write_header(&mut out, CMD_ALERT_SNAPSHOT_REQUEST, uid);
    out
}

/// CmdId=17 `TChartTextStateCommand`.
#[doc(hidden)]
pub(crate) fn build_chart_text_state(cmd: &ChartTextStateCommand) -> Vec<u8> {
    let mut out = Vec::with_capacity(11 + 2 + cmd.market_name.len() + 2);
    write_header(&mut out, CMD_CHART_TEXT_STATE, cmd.uid);
    write_string(&mut out, &cmd.market_name);
    out.push(cmd.need_filters as u8);
    out.push(cmd.need_debug_lines as u8);
    out
}

#[cfg(test)]
#[doc(hidden)]
pub fn build_chart_text_snapshot_for_test(cmd: &ChartTextSnapshotCommand) -> Vec<u8> {
    let filter_count = cmd.filter_lines.len().min(usize::from(u16::MAX));
    let debug_count = cmd.debug_lines.len().min(usize::from(u16::MAX));
    let mut out = Vec::new();
    write_header(&mut out, CMD_CHART_TEXT_SNAPSHOT, cmd.uid);
    write_string(&mut out, &cmd.market_name);
    out.extend_from_slice(&(filter_count as u16).to_le_bytes());
    for line in cmd.filter_lines.iter().take(filter_count) {
        write_string(&mut out, line);
    }
    out.extend_from_slice(&(debug_count as u16).to_le_bytes());
    for line in cmd.debug_lines.iter().take(debug_count) {
        write_string(&mut out, line);
    }
    out
}

/// CmdId=19 `TOrdersHistoryRequestCommand`.
#[doc(hidden)]
pub(crate) fn build_orders_history_request(uid: u64, market_name: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(11 + 2 + market_name.len());
    write_header(&mut out, CMD_ORDERS_HISTORY_REQUEST, uid);
    write_string(&mut out, market_name);
    out
}

/// CmdId=21 `TRestartNowCommand` (empty body).
#[doc(hidden)]
pub(crate) fn build_restart_now(uid: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(11);
    write_header(&mut out, CMD_RESTART_NOW, uid);
    out
}

/// CmdId=23 `TKernelLicenseStateRequest`.
#[doc(hidden)]
pub(crate) fn build_kernel_license_state_request(uid: u64, activate_feature: i32) -> Vec<u8> {
    let mut out = Vec::with_capacity(15);
    write_header(&mut out, CMD_KERNEL_LICENSE_STATE_REQUEST, uid);
    out.extend_from_slice(&activate_feature.to_le_bytes());
    out
}

/// CmdId=25 `TAutoDetectCommand`.
#[doc(hidden)]
pub(crate) fn build_auto_detect(uid: u64, active: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(12);
    write_header(&mut out, CMD_AUTO_DETECT, uid);
    out.push(active as u8);
    out
}
