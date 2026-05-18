//! MPC_Balance подкоманда CmdId=6 — `TArbPricesCommand`.
//!
//! Источник Delphi: `MoonProtoBalanceStruct.pas:199-205, 607-633`.
//!
//! Wire-format:
//!   BaseCommand header (CmdId=6 + ver:u16 + UID:u64) + len:i32 LE + payload:bytes(len).
//!
//! `payload` — raw bytes от kernel'а (компактный формат `ParseArbPayloadCompact` —
//! TODO для Stage 3 если нужен structured decoder).

use super::registry::CURRENT_PROTO_CMD_VER;

const ARB_PRICES_CMD_ID: u8 = 6;

#[derive(Debug, Clone)]
pub struct ArbPricesCommand {
    pub uid: u64,
    pub payload: Vec<u8>,
}

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
        return Some(ArbPricesCommand { uid, payload: Vec::new() });
    }
    let len = i32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    let blob = if len > 0 && pos + len <= payload.len() {
        payload[pos..pos + len].to_vec()
    } else {
        Vec::new()
    };
    Some(ArbPricesCommand { uid, payload: blob })
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
}
