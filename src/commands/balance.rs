//! Balance rows and low-level balance command helpers.
//!
//! Regular applications read live position, liquidation, leverage, and wallet
//! values from retained Active Lib state. The parser/builder functions in this
//! module are byte-exact protocol tools kept inside the crate for diagnostics
//! and tests, not terminal-facing data models.
use super::registry::read_string;
use crate::commands::market::PositionType;
use crate::commands::trade::OrderType;

const MAX_BALANCE_ITEMS: usize = u16::MAX as usize + 1;
const BALANCE_ITEM_MIN_WIRE_SIZE: usize = 2;

/// One market's decoded balance row.
///
/// Normal chart/order UI should read the active values from `Market`; this row
/// remains useful for account tables, diagnostics, and full balance snapshots.
#[derive(Debug, Clone, Default)]
pub(crate) struct BalanceItem {
    pub market_name: String,
    pub balance_hash: u64,

    pub initial_balance: f64,
    pub locked_balance: f64,

    pub pos_size: f64,
    pub pos_price: f64,
    pub liq_price: f64,
    pub pos_dir: OrderType,

    pub long_pos_size: f64,
    pub long_pos_price: f64,
    pub long_liq_price: f64,
    pub long_position_type: PositionType,

    pub short_pos_size: f64,
    pub short_pos_price: f64,
    pub short_liq_price: f64,
    pub short_position_type: PositionType,

    pub asset_balance: f64,
    pub asset_balance_full: f64,

    pub total_profit_b: f64,
    pub total_profit_l: f64,
    pub total_profit_s: f64,

    pub max_value: f64,

    pub leverage_x: i32,
    pub position_type: PositionType,
}

/// Balance update payload.
///
/// `cmd_id=2` has the same item layout as a full snapshot, but the Delphi
/// client parses the object and then ignores it in `ProcessBalanceCommand`.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub(crate) struct BalanceUpdate {
    pub cmd_id: u8, // 002=base ignored by client, 003=full, 004=incremental
    pub epoch: u16,
    pub global_changed: bool, // only for incremental (004)
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
///
/// Requests the server to resend the current balance snapshot. The body is
/// empty; only the command envelope is sent (CmdId + ver + uid).
///
/// Priority = `MPS_High`, encrypted = true, max_retries = 3.
#[doc(hidden)]
pub(crate) fn build_request_balance_refresh(uid: u64) -> Vec<u8> {
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
/// cmd_id determines the wire format (002/003 vs 004); state application is
/// only defined by Delphi for 003/004.
#[doc(hidden)]
pub(crate) fn parse_balance(cmd_id: u8, data: &[u8]) -> Option<BalanceUpdate> {
    let mut pos = 0usize;

    // Delphi reads fixed fields with TStream.Read into zero-initialized object
    // fields. Short reads keep the unread tail zero and do not raise.
    let epoch = read_u16_zero_tail(data, &mut pos);

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
        result.global_changed = read_bool_zero_tail(data, &mut pos);

        if result.global_changed {
            result.btc_balance_total = read_f64_zero_tail(data, &mut pos);
            result.btc_balance_locked = read_f64_zero_tail(data, &mut pos);
            result.btc_balance_full = read_f64_zero_tail(data, &mut pos);
            result.special_coin_balance = read_f64_zero_tail(data, &mut pos);
        }
    } else {
        // Full: always has global fields
        result.btc_balance_total = read_f64_zero_tail(data, &mut pos);
        result.btc_balance_locked = read_f64_zero_tail(data, &mut pos);
        result.btc_balance_full = read_f64_zero_tail(data, &mut pos);
        result.special_coin_balance = read_f64_zero_tail(data, &mut pos);
    }

    let count_raw = read_i32_zero_tail(data, &mut pos);

    if count_raw <= 0 {
        return Some(result);
    }
    let count = count_raw as usize;
    if count > MAX_BALANCE_ITEMS {
        log::warn!(
            target: "moonproto::balance",
            "Balance row count {count} exceeds cap {MAX_BALANCE_ITEMS}"
        );
        return None;
    }
    let min_wire = count.checked_mul(BALANCE_ITEM_MIN_WIRE_SIZE)?;
    if data.len().saturating_sub(pos) < min_wire {
        log::warn!(
            target: "moonproto::balance",
            "Balance row count {count} exceeds payload envelope"
        );
        return None;
    }
    result.items.try_reserve_exact(count).ok()?;

    for _ in 0..count {
        if let Some(item) = read_balance_item(data, &mut pos) {
            result.items.push(item);
        } else {
            return None;
        }
    }

    Some(result)
}

/// Read one TBalanceItem from data at position.
fn read_balance_item(data: &[u8], pos: &mut usize) -> Option<BalanceItem> {
    let market_name = read_string(data, pos)?;

    let balance_hash = read_u64_zero_tail(data, pos);
    let flags = read_u32_zero_tail(data, pos);

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
    item.pos_dir = OrderType::from_byte(read_flagged_u8(data, pos, flags, &mut bit, 0));
    item.long_pos_size = read_flagged_f64(data, pos, flags, &mut bit);
    item.long_pos_price = read_flagged_f64(data, pos, flags, &mut bit);
    item.long_liq_price = read_flagged_f64(data, pos, flags, &mut bit);
    item.long_position_type =
        PositionType::from_byte(read_flagged_u8(data, pos, flags, &mut bit, 0));
    item.short_pos_size = read_flagged_f64(data, pos, flags, &mut bit);
    item.short_pos_price = read_flagged_f64(data, pos, flags, &mut bit);
    item.short_liq_price = read_flagged_f64(data, pos, flags, &mut bit);
    item.short_position_type =
        PositionType::from_byte(read_flagged_u8(data, pos, flags, &mut bit, 0));
    item.asset_balance = read_flagged_f64(data, pos, flags, &mut bit);
    item.asset_balance_full = read_flagged_f64(data, pos, flags, &mut bit);
    item.total_profit_b = read_flagged_f64(data, pos, flags, &mut bit);
    item.total_profit_l = read_flagged_f64(data, pos, flags, &mut bit);
    item.total_profit_s = read_flagged_f64(data, pos, flags, &mut bit);
    item.max_value = read_flagged_f64(data, pos, flags, &mut bit);
    item.leverage_x = read_flagged_i32(data, pos, flags, &mut bit, 1);
    item.position_type = PositionType::from_byte(read_flagged_u8(data, pos, flags, &mut bit, 0));

    Some(item)
}

fn read_flagged_f64(data: &[u8], pos: &mut usize, flags: u32, bit: &mut u32) -> f64 {
    let present = (flags & (1 << *bit)) != 0;
    *bit += 1;
    if present {
        read_f64_zero_tail(data, pos)
    } else {
        0.0
    }
}

fn read_flagged_i32(data: &[u8], pos: &mut usize, flags: u32, bit: &mut u32, default: i32) -> i32 {
    let present = (flags & (1 << *bit)) != 0;
    *bit += 1;
    if present {
        read_i32_zero_tail(data, pos)
    } else {
        default
    }
}

fn read_zero_tail<const N: usize>(data: &[u8], pos: &mut usize) -> [u8; N] {
    let mut out = [0u8; N];
    let available = data.len().saturating_sub(*pos).min(N);
    if available > 0 {
        out[..available].copy_from_slice(&data[*pos..*pos + available]);
        *pos += available;
    }
    out
}

fn read_bool_zero_tail(data: &[u8], pos: &mut usize) -> bool {
    read_zero_tail::<1>(data, pos)[0] != 0
}

fn read_u16_zero_tail(data: &[u8], pos: &mut usize) -> u16 {
    u16::from_le_bytes(read_zero_tail::<2>(data, pos))
}

fn read_u32_zero_tail(data: &[u8], pos: &mut usize) -> u32 {
    u32::from_le_bytes(read_zero_tail::<4>(data, pos))
}

fn read_u64_zero_tail(data: &[u8], pos: &mut usize) -> u64 {
    u64::from_le_bytes(read_zero_tail::<8>(data, pos))
}

fn read_i32_zero_tail(data: &[u8], pos: &mut usize) -> i32 {
    i32::from_le_bytes(read_zero_tail::<4>(data, pos))
}

fn read_f64_zero_tail(data: &[u8], pos: &mut usize) -> f64 {
    f64::from_le_bytes(read_zero_tail::<8>(data, pos))
}

fn read_flagged_u8(data: &[u8], pos: &mut usize, flags: u32, bit: &mut u32, default: u8) -> u8 {
    let present = (flags & (1 << *bit)) != 0;
    *bit += 1;
    if present {
        read_zero_tail::<1>(data, pos)[0]
    } else {
        default
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn full_balance_payload_with_count(count: i32, item_bytes: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&7u16.to_le_bytes());
        for v in [1.0_f64, 2.0, 3.0, 4.0] {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out.extend_from_slice(&count.to_le_bytes());
        out.extend_from_slice(item_bytes);
        out
    }

    fn zero_flags_item(name: &str, hash: u64) -> Vec<u8> {
        let mut out = Vec::new();
        super::super::registry::write_string(&mut out, name);
        out.extend_from_slice(&hash.to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes());
        out
    }

    #[test]
    // parity: MoonBot MoonProtoBalanceStruct.pas:TBalanceItem.ReadFromStream
    fn balance_parser_rejects_truncated_next_item() {
        let item = zero_flags_item("BTCUSDT", 99);
        let payload = full_balance_payload_with_count(2, &item);

        assert!(parse_balance(3, &payload).is_none());
    }

    #[test]
    // parity: MoonBot MoonProtoBalanceStruct.pas:TBalanceItem.ReadFromStream
    fn balance_parser_zero_tails_short_fixed_fields() {
        let mut item = Vec::new();
        super::super::registry::write_string(&mut item, "BTCUSDT");
        // Short BalanceHash keeps present low bytes; Flags are absent and stay zero.
        item.extend_from_slice(&[0x34, 0x12]);

        let payload = full_balance_payload_with_count(1, &item);
        let parsed = parse_balance(3, &payload).unwrap();

        assert_eq!(parsed.items.len(), 1);
        assert_eq!(parsed.items[0].market_name, "BTCUSDT");
        assert_eq!(parsed.items[0].balance_hash, 0x1234);
        assert_eq!(parsed.items[0].leverage_x, 1);
    }

    #[test]
    fn balance_parser_zero_tails_present_flagged_field_and_consumes_tail() {
        let mut item = Vec::new();
        super::super::registry::write_string(&mut item, "BTCUSDT");
        item.extend_from_slice(&99u64.to_le_bytes());
        item.extend_from_slice(&1u32.to_le_bytes()); // InitialBalance present
        item.extend_from_slice(&[0x01, 0x02]); // short f64 payload

        let payload = full_balance_payload_with_count(1, &item);
        let parsed = parse_balance(3, &payload).unwrap();

        assert_eq!(parsed.items.len(), 1);
        assert_eq!(parsed.items[0].initial_balance.to_bits(), 0x0201);
    }

    #[test]
    // parity: MoonBot MoonProtoBalanceStruct.pas:TBalanceCommand.CreateFromStream (Count guard)
    fn balance_parser_negative_count_has_no_items() {
        let payload = full_balance_payload_with_count(-1, &[]);

        let parsed = parse_balance(3, &payload).unwrap();

        assert!(parsed.items.is_empty());
    }

    #[test]
    fn balance_parser_rejects_absurd_count_before_loop() {
        let payload = full_balance_payload_with_count((MAX_BALANCE_ITEMS as i32) + 1, &[]);

        assert!(parse_balance(3, &payload).is_none());
    }

    #[test]
    fn balance_parser_rejects_count_outside_payload_envelope() {
        let mut item = Vec::new();
        super::super::registry::write_string(&mut item, "");
        let payload = full_balance_payload_with_count(2, &item);

        assert!(parse_balance(3, &payload).is_none());
    }
}
