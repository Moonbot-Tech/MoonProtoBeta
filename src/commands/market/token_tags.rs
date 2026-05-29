use super::{write_str, EngineStreamReader};

/// `TTokenTag` flag set (Vars.pas:64). On the wire it is an i32 bitmask.
///
/// Bits correspond to the ordinals of the Delphi `TTokenTag` enum:
/// `(tag_none, tag_Monitoring, tag_Fan, tag_seed, tag_launch, tag_gaming,
///   tag_New, tag_OLD, tag_BNB, tag_Alpha, tag_OICapped, tag_TradFi)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TokenTags(u32);

impl TokenTags {
    pub const NONE: Self = Self(1 << 0);
    pub const MONITORING: Self = Self(1 << 1);
    pub const FAN: Self = Self(1 << 2);
    pub const SEED: Self = Self(1 << 3);
    pub const LAUNCH: Self = Self(1 << 4);
    pub const GAMING: Self = Self(1 << 5);
    pub const NEW: Self = Self(1 << 6);
    pub const OLD: Self = Self(1 << 7);
    pub const BNB: Self = Self(1 << 8);
    pub const ALPHA: Self = Self(1 << 9);
    pub const OI_CAPPED: Self = Self(1 << 10);
    pub const TRAD_FI: Self = Self(1 << 11);

    pub const fn empty() -> Self {
        Self(0)
    }
    pub const fn bits(self) -> u32 {
        self.0
    }
    pub const fn from_bits(b: u32) -> Self {
        Self(b)
    }
    pub fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }
    pub fn is_empty(self) -> bool {
        self.0 == 0
    }
}

impl core::ops::BitOr for TokenTags {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl core::ops::BitAnd for TokenTags {
    type Output = Self;
    fn bitand(self, rhs: Self) -> Self {
        Self(self.0 & rhs.0)
    }
}

#[derive(Debug, Clone, PartialEq)]
#[doc(hidden)]
pub struct MarketTokenTags {
    pub market_name: String,
    pub tags: TokenTags,
}

/// `emk_CheckBinanceTags` response: list of (market_name, tags).
/// Wire-form (MoonProtoEngineServer.pas:324-333):
///   `count:i32 + (market_name:string + tags:i32)[count]`.
#[doc(hidden)]
pub fn parse_token_tags_response(data: &[u8]) -> Option<Vec<MarketTokenTags>> {
    let mut r = EngineStreamReader::new(data);
    // MarketTokenTags: market_name (string u16+chars) + tags (i32) = at least 6 bytes.
    let count = r.read_count()?;
    let mut out = Vec::with_capacity(r.bounded_count_capacity(count, 6));
    for _ in 0..count {
        let market_name = r.read_str()?;
        let tags_int = r.read_int()? as u32;
        out.push(MarketTokenTags {
            market_name,
            tags: TokenTags::from_bits(tags_int),
        });
    }
    Some(out)
}

#[doc(hidden)]
pub fn build_token_tags_response(items: &[MarketTokenTags]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + items.len() * 16);
    out.extend_from_slice(&(items.len() as i32).to_le_bytes());
    for it in items {
        write_str(&mut out, &it.market_name);
        out.extend_from_slice(&(it.tags.bits() as i32).to_le_bytes());
    }
    out
}
