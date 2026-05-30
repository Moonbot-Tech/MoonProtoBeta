//! MPC_Balance subcommand CmdId=6 — `TArbPricesCommand`.
//!
//! Delphi source: `MoonProtoBalanceStruct.pas:199-205, 607-633`.
//!
//! Wire-format:
//!   BaseCommand header (CmdId=6 + ver:u16 + UID:u64) + len:i32 LE + payload:bytes(len).
//!
//! `payload` is raw kernel data. The compact format is decoded by
//! [`parse_arb_payload_compact`], the Rust port of
//! `ArbClientU.pas:ParseArbPayloadCompact`.

use super::registry::CURRENT_PROTO_CMD_VER;

const ARB_PRICES_CMD_ID: u8 = 6;

#[derive(Debug, Clone)]
#[doc(hidden)]
pub(crate) struct ArbPricesCommand {
    pub uid: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
#[doc(hidden)]
pub(crate) enum ArbPayload {
    Price {
        version: u8,
        blocks: Vec<ArbPriceBlock>,
    },
    Isolation {
        version: u8,
        entries: Vec<ArbIsolationEntry>,
    },
}

#[derive(Debug, Clone, PartialEq)]
#[doc(hidden)]
pub(crate) struct ArbPriceBlock {
    pub market_index: u16,
    pub prices: Vec<ArbPriceItem>,
}

#[derive(Debug, Clone, PartialEq)]
#[doc(hidden)]
pub(crate) struct ArbPriceItem {
    pub platform_code: u8,
    pub price: f32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub(crate) struct ArbIsolationEntry {
    pub market_index: u16,
    pub platform_code: u8,
    pub flags: u8,
}

const ARB_VER_MIN: u8 = 1;
const CMD_PRICE: u8 = 1;
const CMD_ISOL: u8 = 2;

/// Parse `TArbPricesCommand`.
///
/// `payload` must already be routed from the MPC_Balance channel. Returns
/// `None` when `cmd_id != 6` or the command envelope is too short.
#[doc(hidden)]
pub(crate) fn parse_arb_prices(payload: &[u8]) -> Option<ArbPricesCommand> {
    if payload.len() < 11 {
        return None;
    }
    let cmd_id = payload[0];
    if cmd_id != ARB_PRICES_CMD_ID {
        return None;
    }
    let ver = u16::from_le_bytes([payload[1], payload[2]]);
    if ver > CURRENT_PROTO_CMD_VER {
        return None;
    }
    let uid = u64::from_le_bytes(payload[3..11].try_into().unwrap());

    let mut pos = 11;
    if pos + 4 > payload.len() {
        return Some(ArbPricesCommand {
            uid,
            payload: Vec::new(),
        });
    }
    let len = i32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap());
    pos += 4;
    let blob = if len > 0 {
        let len = len as usize;
        if pos + len <= payload.len() {
            payload[pos..pos + len].to_vec()
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };
    Some(ArbPricesCommand { uid, payload: blob })
}

/// Decode compact kernel→client arb payload.
///
/// Delphi source:
/// - `ArbClientU.pas:299-327` — dispatcher;
/// - `ArbClientU.pas:205-226` — compact price items;
/// - `ArbClientU.pas:232-259` — compact isolation snapshot.
#[doc(hidden)]
pub(crate) fn parse_arb_payload_compact(payload: &[u8]) -> Option<ArbPayload> {
    if payload.len() < 2 {
        return None;
    }

    let version = payload[0];
    if version < ARB_VER_MIN {
        return None;
    }

    let mut pos = 1usize;
    if version <= 2 {
        return Some(ArbPayload::Price {
            version,
            blocks: parse_price_items_compact(payload, &mut pos),
        });
    }

    if pos >= payload.len() {
        return None;
    }
    let cmd = payload[pos];
    pos += 1;

    match cmd {
        CMD_PRICE => Some(ArbPayload::Price {
            version,
            blocks: parse_price_items_compact(payload, &mut pos),
        }),
        CMD_ISOL => Some(ArbPayload::Isolation {
            version,
            entries: parse_isolation_compact(payload, &mut pos),
        }),
        _ => None,
    }
}

fn parse_price_items_compact(data: &[u8], pos: &mut usize) -> Vec<ArbPriceBlock> {
    let mut blocks = Vec::new();

    while *pos + 3 <= data.len() {
        let market_index = u16::from_le_bytes([data[*pos], data[*pos + 1]]);
        *pos += 2;
        let price_count = data[*pos] as usize;
        *pos += 1;

        let block_size = price_count.saturating_mul(5);
        if *pos + block_size > data.len() {
            break;
        }

        let mut prices = Vec::with_capacity(price_count);
        for _ in 0..price_count {
            let platform_code = data[*pos];
            *pos += 1;
            let price = f32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
            *pos += 4;
            prices.push(ArbPriceItem {
                platform_code,
                price,
            });
        }

        blocks.push(ArbPriceBlock {
            market_index,
            prices,
        });
    }

    blocks
}

fn parse_isolation_compact(data: &[u8], pos: &mut usize) -> Vec<ArbIsolationEntry> {
    if *pos + 2 > data.len() {
        return Vec::new();
    }

    let count = u16::from_le_bytes([data[*pos], data[*pos + 1]]) as usize;
    *pos += 2;
    let mut entries = Vec::with_capacity(count);

    for _ in 0..count {
        if *pos + 4 > data.len() {
            break;
        }

        let market_index = u16::from_le_bytes([data[*pos], data[*pos + 1]]);
        *pos += 2;
        let platform_code = data[*pos];
        *pos += 1;
        let flags = data[*pos];
        *pos += 1;

        entries.push(ArbIsolationEntry {
            market_index,
            platform_code,
            flags,
        });
    }

    entries
}

/// Build `TArbPricesCommand` for low-level protocol tools.
#[doc(hidden)]
#[allow(dead_code)]
pub(crate) fn build_arb_prices(uid: u64, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(11 + 4 + payload.len());
    out.push(ARB_PRICES_CMD_ID);
    out.extend_from_slice(&CURRENT_PROTO_CMD_VER.to_le_bytes());
    out.extend_from_slice(&uid.to_le_bytes());
    out.extend_from_slice(&(payload.len() as i32).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

#[cfg(test)]
mod tests;
