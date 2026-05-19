/// MPC_Balance unpacker — byte-exact port of MoonProtoBalanceStruct.pas.
/// Source: MoonProtoBalanceStruct.pas:438-486 (TBalanceItem.ReadFromStream)
///
/// Uses bitmask optimization: each field has a bit in Flags (u32).
/// Only fields with bit set are present in wire data.

use super::registry::read_string;

/// One market's balance data
#[derive(Debug, Clone, Default)]
pub struct BalanceItem {
    pub market_name: String,
    pub balance_hash: u64,

    pub initial_balance: f64,
    pub locked_balance: f64,

    pub pos_size: f64,
    pub pos_price: f64,
    pub liq_price: f64,
    pub pos_dir: u8,

    pub long_pos_size: f64,
    pub long_pos_price: f64,
    pub long_liq_price: f64,
    pub long_position_type: u8,

    pub short_pos_size: f64,
    pub short_pos_price: f64,
    pub short_liq_price: f64,
    pub short_position_type: u8,

    pub asset_balance: f64,
    pub asset_balance_full: f64,

    pub total_profit_b: f64,
    pub total_profit_l: f64,
    pub total_profit_s: f64,

    pub max_value: f64,

    pub leverage_x: i32,
    pub position_type: u8,
}

/// Full balance update (snapshot or incremental)
#[derive(Debug, Clone)]
pub struct BalanceUpdate {
    pub cmd_id: u8,    // 002=legacy, 003=full, 004=incremental
    pub epoch: u16,
    pub global_changed: bool,  // only for incremental (004)
    pub btc_balance_total: f64,
    pub btc_balance_locked: f64,
    pub btc_balance_full: f64,
    pub special_coin_balance: f64,
    pub items: Vec<BalanceItem>,
}

// =============================================================================
//  Builders (C → S)
// =============================================================================

/// CmdId=5 `TRequestBalanceRefresh` (MoonProtoBalanceStruct.pas:191).
/// Запрашивает у сервера повторную отправку текущего snapshot балансов.
/// Empty body — только wrapping заголовок команды (CmdId + ver + uid).
///
/// Priority = MPS_High, encrypted = true, max_retries = 3 (default для High).
/// Docs_api audit B-03 — отсутствие этого builder'а блокировало возможность
/// запросить refresh из Rust клиента.
pub fn build_request_balance_refresh(uid: u64) -> Vec<u8> {
    const CMD_REQUEST_BALANCE_REFRESH: u8 = 5;
    const CURRENT_PROTO_CMD_VER: u16 = 3;
    let mut out = Vec::with_capacity(11);
    out.push(CMD_REQUEST_BALANCE_REFRESH);
    out.extend_from_slice(&CURRENT_PROTO_CMD_VER.to_le_bytes());
    out.extend_from_slice(&uid.to_le_bytes());
    out
}

// =============================================================================
//  Parsers (S → C)
// =============================================================================

/// Parse balance command payload (after command header CmdId+ver+UID stripped).
/// cmd_id determines the format (002/003 vs 004).
pub fn parse_balance(cmd_id: u8, data: &[u8]) -> Option<BalanceUpdate> {
    let mut pos = 0usize;

    // Epoch (2 bytes)
    if pos + 2 > data.len() { return None; }
    let epoch = u16::from_le_bytes([data[pos], data[pos + 1]]);
    pos += 2;

    let mut result = BalanceUpdate {
        cmd_id,
        epoch,
        global_changed: false,
        btc_balance_total: 0.0,
        btc_balance_locked: 0.0,
        btc_balance_full: 0.0,
        special_coin_balance: 0.0,
        items: Vec::new(),
    };

    if cmd_id == 4 {
        // Incremental: GlobalChanged flag gates global fields
        if pos >= data.len() { return Some(result); }
        result.global_changed = data[pos] != 0;
        pos += 1;

        if result.global_changed {
            if pos + 32 > data.len() { return Some(result); }
            result.btc_balance_total = f64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()); pos += 8;
            result.btc_balance_locked = f64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()); pos += 8;
            result.btc_balance_full = f64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()); pos += 8;
            result.special_coin_balance = f64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()); pos += 8;
        }
    } else {
        // Full: always has global fields
        if pos + 32 > data.len() { return Some(result); }
        result.btc_balance_total = f64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()); pos += 8;
        result.btc_balance_locked = f64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()); pos += 8;
        result.btc_balance_full = f64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()); pos += 8;
        result.special_coin_balance = f64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()); pos += 8;
    }

    // Count
    if pos + 4 > data.len() { return Some(result); }
    let count_raw = i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
    pos += 4;
    // DoS guard: BalanceItem минимум ~14 байт (string-prefix u16 + hash 8 + flags 4).
    // count * 14 > remaining → malformed/adversarial.
    if count_raw < 0 || (count_raw as usize).saturating_mul(14) > data.len().saturating_sub(pos) {
        log::warn!(target: "moonproto::balance",
            "BalanceUpdate: invalid count={} (remaining={})", count_raw, data.len() - pos);
        return Some(result);
    }
    let count = count_raw as usize;

    // Items
    for _ in 0..count {
        if let Some(item) = read_balance_item(data, &mut pos) {
            result.items.push(item);
        } else {
            break;
        }
    }

    Some(result)
}

/// Read one TBalanceItem from data at position.
fn read_balance_item(data: &[u8], pos: &mut usize) -> Option<BalanceItem> {
    let market_name = read_string(data, pos)?;

    if *pos + 12 > data.len() { return None; } // hash(8) + flags(4)
    let balance_hash = u64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    let flags = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;

    let mut item = BalanceItem {
        market_name,
        balance_hash,
        ..Default::default()
    };
    item.leverage_x = 1; // default for leverage

    // Read fields by bitmask — order MUST match Delphi exactly (22 fields)
    let mut bit = 0u32;

    item.initial_balance = read_flagged_f64(data, pos, flags, &mut bit);
    item.locked_balance = read_flagged_f64(data, pos, flags, &mut bit);
    item.pos_size = read_flagged_f64(data, pos, flags, &mut bit);
    item.pos_price = read_flagged_f64(data, pos, flags, &mut bit);
    item.liq_price = read_flagged_f64(data, pos, flags, &mut bit);
    item.pos_dir = read_flagged_u8(data, pos, flags, &mut bit, 0);
    item.long_pos_size = read_flagged_f64(data, pos, flags, &mut bit);
    item.long_pos_price = read_flagged_f64(data, pos, flags, &mut bit);
    item.long_liq_price = read_flagged_f64(data, pos, flags, &mut bit);
    item.long_position_type = read_flagged_u8(data, pos, flags, &mut bit, 0);
    item.short_pos_size = read_flagged_f64(data, pos, flags, &mut bit);
    item.short_pos_price = read_flagged_f64(data, pos, flags, &mut bit);
    item.short_liq_price = read_flagged_f64(data, pos, flags, &mut bit);
    item.short_position_type = read_flagged_u8(data, pos, flags, &mut bit, 0);
    item.asset_balance = read_flagged_f64(data, pos, flags, &mut bit);
    item.asset_balance_full = read_flagged_f64(data, pos, flags, &mut bit);
    item.total_profit_b = read_flagged_f64(data, pos, flags, &mut bit);
    item.total_profit_l = read_flagged_f64(data, pos, flags, &mut bit);
    item.total_profit_s = read_flagged_f64(data, pos, flags, &mut bit);
    item.max_value = read_flagged_f64(data, pos, flags, &mut bit);
    item.leverage_x = read_flagged_i32(data, pos, flags, &mut bit, 1);
    item.position_type = read_flagged_u8(data, pos, flags, &mut bit, 0);

    Some(item)
}

fn read_flagged_f64(data: &[u8], pos: &mut usize, flags: u32, bit: &mut u32) -> f64 {
    let present = (flags & (1 << *bit)) != 0;
    *bit += 1;
    if present && *pos + 8 <= data.len() {
        let v = f64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
        *pos += 8;
        v
    } else {
        0.0
    }
}

fn read_flagged_i32(data: &[u8], pos: &mut usize, flags: u32, bit: &mut u32, default: i32) -> i32 {
    let present = (flags & (1 << *bit)) != 0;
    *bit += 1;
    if present && *pos + 4 <= data.len() {
        let v = i32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
        *pos += 4;
        v
    } else {
        default
    }
}

fn read_flagged_u8(data: &[u8], pos: &mut usize, flags: u32, bit: &mut u32, default: u8) -> u8 {
    let present = (flags & (1 << *bit)) != 0;
    *bit += 1;
    if present && *pos < data.len() {
        let v = data[*pos];
        *pos += 1;
        v
    } else {
        default
    }
}
