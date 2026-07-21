//! Delphi-shaped command headers for `MPC_Order`.

use crate::commands::registry::{decode_utf8_delphi, write_string};
use std::convert::TryInto;

/// Base `TBaseCommand` header: cmd_id(1) + ver(2) + UID(8) = 11 bytes.
#[derive(Debug, Clone, Copy)]
pub struct BaseCommandHeader {
    pub cmd_id: u8,
    pub ver: u16,
    pub uid: u64,
}

impl BaseCommandHeader {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        if r.len() < 11 {
            return None;
        }
        let cmd_id = r[0];
        let ver = u16::from_le_bytes([r[1], r[2]]);
        let uid = u64::from_le_bytes(r[3..11].try_into().unwrap());
        *r = &r[11..];
        Some(Self { cmd_id, ver, uid })
    }

    pub fn write(&self, out: &mut Vec<u8>) {
        out.push(self.cmd_id);
        out.extend_from_slice(&self.ver.to_le_bytes());
        out.extend_from_slice(&self.uid.to_le_bytes());
    }
}

/// Header `TBaseMarketCommand`: header + currency:u8 + platform:u8 + market_name:UTF8.
/// `market_name` resolves to `market_index` when applied to state.
#[derive(Debug, Clone)]
pub struct MarketCommandHeader {
    pub base: BaseCommandHeader,
    pub currency: u8,
    pub platform: u8,
    pub market_name: String,
}

impl MarketCommandHeader {
    pub fn read(r: &mut &[u8]) -> Option<Self> {
        let base = BaseCommandHeader::read(r)?;
        if r.len() < 2 {
            return None;
        }
        let currency = r[0];
        let platform = r[1];
        *r = &r[2..];
        let market_name = read_str(r)?;
        Some(Self {
            base,
            currency,
            platform,
            market_name,
        })
    }

    pub fn write(&self, out: &mut Vec<u8>, base_currency: u8, base_platform: u8) {
        self.base.write(out);
        out.push(base_currency);
        out.push(base_platform);
        write_string(out, &self.market_name);
    }
}

fn read_str(r: &mut &[u8]) -> Option<String> {
    if r.len() < 2 {
        return None;
    }
    let len = u16::from_le_bytes([r[0], r[1]]) as usize;
    if r.len() < 2 + len {
        return None;
    }
    let s = decode_utf8_delphi(&r[2..2 + len]);
    *r = &r[2 + len..];
    Some(s)
}
