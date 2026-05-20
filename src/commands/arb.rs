//! MPC_Balance подкоманда CmdId=6 — `TArbPricesCommand`.
//!
//! Источник Delphi: `MoonProtoBalanceStruct.pas:199-205, 607-633`.
//!
//! Wire-format:
//!   BaseCommand header (CmdId=6 + ver:u16 + UID:u64) + len:i32 LE + payload:bytes(len).
//!
//! `payload` — raw bytes от kernel'а. Компактный формат декодируется через
//! [`parse_arb_payload_compact`] (порт `ArbClientU.pas:ParseArbPayloadCompact`).

use super::registry::CURRENT_PROTO_CMD_VER;

const ARB_PRICES_CMD_ID: u8 = 6;

#[derive(Debug, Clone)]
pub struct ArbPricesCommand {
    pub uid: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ArbPayload {
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
pub struct ArbPriceBlock {
    pub market_index: u16,
    pub prices: Vec<ArbPriceItem>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ArbPriceItem {
    pub platform_code: u8,
    pub price: f32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArbIsolationEntry {
    pub market_index: u16,
    pub platform_code: u8,
    pub flags: u8,
}

const ARB_VER_MIN: u8 = 1;
const CMD_PRICE: u8 = 1;
const CMD_ISOL: u8 = 2;

/// Парсер `TArbPricesCommand`. Принимает payload **уже после** dispatch'а по MPC_Balance.
/// Возвращает `None` если cmd_id ≠ 6 или payload слишком короткий.
pub fn parse_arb_prices(payload: &[u8]) -> Option<ArbPricesCommand> {
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
pub fn parse_arb_payload_compact(payload: &[u8]) -> Option<ArbPayload> {
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

/// Билдер `TArbPricesCommand` (если клиенту нужно слать обратно — rare case).
pub fn build_arb_prices(uid: u64, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(11 + 4 + payload.len());
    out.push(ARB_PRICES_CMD_ID);
    out.extend_from_slice(&CURRENT_PROTO_CMD_VER.to_le_bytes());
    out.extend_from_slice(&uid.to_le_bytes());
    out.extend_from_slice(&(payload.len() as i32).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let original = b"hello arb data";
        let raw = build_arb_prices(42, original);
        let parsed = parse_arb_prices(&raw).unwrap();
        assert_eq!(parsed.uid, 42);
        assert_eq!(parsed.payload, original);
    }

    #[test]
    fn wrong_cmd_id_returns_none() {
        // CmdId=99 ≠ 6
        let mut payload = vec![99u8, 3, 0];
        payload.extend_from_slice(&1u64.to_le_bytes());
        payload.extend_from_slice(&0i32.to_le_bytes());
        assert!(parse_arb_prices(&payload).is_none());
    }

    #[test]
    fn empty_payload() {
        let raw = build_arb_prices(7, &[]);
        let parsed = parse_arb_prices(&raw).unwrap();
        assert!(parsed.payload.is_empty());
    }

    #[test]
    fn negative_len_is_empty_like_delphi() {
        let mut raw = Vec::new();
        raw.push(ARB_PRICES_CMD_ID);
        raw.extend_from_slice(&CURRENT_PROTO_CMD_VER.to_le_bytes());
        raw.extend_from_slice(&7u64.to_le_bytes());
        raw.extend_from_slice(&(-1i32).to_le_bytes());

        let parsed = parse_arb_prices(&raw).unwrap();
        assert!(parsed.payload.is_empty());
    }

    #[test]
    fn compact_v2_price_payload() {
        let mut payload = vec![2u8];
        payload.extend_from_slice(&42u16.to_le_bytes());
        payload.push(2);
        payload.push(7);
        payload.extend_from_slice(&123.25f32.to_le_bytes());
        payload.push(8);
        payload.extend_from_slice(&99.5f32.to_le_bytes());
        payload.extend_from_slice(&9u16.to_le_bytes());
        payload.push(1);
        payload.push(100);
        payload.extend_from_slice(&1.5f32.to_le_bytes());

        let parsed = parse_arb_payload_compact(&payload).unwrap();
        assert_eq!(
            parsed,
            ArbPayload::Price {
                version: 2,
                blocks: vec![
                    ArbPriceBlock {
                        market_index: 42,
                        prices: vec![
                            ArbPriceItem {
                                platform_code: 7,
                                price: 123.25,
                            },
                            ArbPriceItem {
                                platform_code: 8,
                                price: 99.5,
                            },
                        ],
                    },
                    ArbPriceBlock {
                        market_index: 9,
                        prices: vec![ArbPriceItem {
                            platform_code: 100,
                            price: 1.5,
                        }],
                    },
                ],
            }
        );
    }

    #[test]
    fn compact_v3_price_payload() {
        let mut payload = vec![3u8, CMD_PRICE];
        payload.extend_from_slice(&11u16.to_le_bytes());
        payload.push(1);
        payload.push(102);
        payload.extend_from_slice(&77.0f32.to_le_bytes());

        let parsed = parse_arb_payload_compact(&payload).unwrap();
        assert_eq!(
            parsed,
            ArbPayload::Price {
                version: 3,
                blocks: vec![ArbPriceBlock {
                    market_index: 11,
                    prices: vec![ArbPriceItem {
                        platform_code: 102,
                        price: 77.0,
                    }],
                }],
            }
        );
    }

    #[test]
    fn compact_v3_isolation_payload() {
        let mut payload = vec![3u8, CMD_ISOL];
        payload.extend_from_slice(&2u16.to_le_bytes());
        payload.extend_from_slice(&42u16.to_le_bytes());
        payload.push(7);
        payload.push(0b01);
        payload.extend_from_slice(&43u16.to_le_bytes());
        payload.push(8);
        payload.push(0b10);

        let parsed = parse_arb_payload_compact(&payload).unwrap();
        assert_eq!(
            parsed,
            ArbPayload::Isolation {
                version: 3,
                entries: vec![
                    ArbIsolationEntry {
                        market_index: 42,
                        platform_code: 7,
                        flags: 0b01,
                    },
                    ArbIsolationEntry {
                        market_index: 43,
                        platform_code: 8,
                        flags: 0b10,
                    },
                ],
            }
        );
    }

    #[test]
    fn compact_truncated_price_block_stops_before_partial_block() {
        let mut payload = vec![2u8];
        payload.extend_from_slice(&42u16.to_le_bytes());
        payload.push(1);
        payload.push(7);
        payload.extend_from_slice(&123.25f32.to_le_bytes());
        payload.extend_from_slice(&43u16.to_le_bytes());
        payload.push(2);
        payload.push(8);
        payload.extend_from_slice(&99.5f32.to_le_bytes());

        let parsed = parse_arb_payload_compact(&payload).unwrap();
        assert_eq!(
            parsed,
            ArbPayload::Price {
                version: 2,
                blocks: vec![ArbPriceBlock {
                    market_index: 42,
                    prices: vec![ArbPriceItem {
                        platform_code: 7,
                        price: 123.25,
                    }],
                }],
            }
        );
    }

    #[test]
    fn compact_truncated_isolation_keeps_complete_entries() {
        let mut payload = vec![3u8, CMD_ISOL];
        payload.extend_from_slice(&2u16.to_le_bytes());
        payload.extend_from_slice(&42u16.to_le_bytes());
        payload.push(7);
        payload.push(0b01);
        payload.extend_from_slice(&43u16.to_le_bytes());
        payload.push(8);

        let parsed = parse_arb_payload_compact(&payload).unwrap();
        assert_eq!(
            parsed,
            ArbPayload::Isolation {
                version: 3,
                entries: vec![ArbIsolationEntry {
                    market_index: 42,
                    platform_code: 7,
                    flags: 0b01,
                }],
            }
        );
    }

    #[test]
    fn compact_rejects_invalid_header() {
        assert!(parse_arb_payload_compact(&[]).is_none());
        assert!(parse_arb_payload_compact(&[1]).is_none());
        assert!(parse_arb_payload_compact(&[0, 1]).is_none());
        assert!(parse_arb_payload_compact(&[3]).is_none());
        assert!(parse_arb_payload_compact(&[3, 99]).is_none());
    }
}
