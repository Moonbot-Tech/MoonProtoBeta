//! Inbound `MPC_UI` command parser.

use super::*;

impl UICommand {
    /// Распарсить TBaseUICommand payload (после dispatch'а по MPC_UI в data_read_int).
    /// Wire-format: `cmd_id:u8 + ver:u16 + UID:u64 + class-specific`.
    /// Version gate: ver > 3 -> [`UICommand::Skipped`], matching Delphi
    /// registry `FSkipped`.
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
                let reset_kind = read_u8_zero_tail(payload, &mut pos);
                Some(UICommand::ResetProfit(ResetProfit { uid, reset_kind }))
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
                let spot_index = read_u8_zero_tail(payload, &mut pos);
                Some(UICommand::SwitchSpot(SwitchSpot { uid, spot_index }))
            }

            _ => Some(UICommand::Unknown { cmd_id, uid }),
        }
    }
}
