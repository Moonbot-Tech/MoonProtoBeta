#[cfg(test)]
use super::write_str;
use super::{EngineStreamReader, MAX_MARKETS_LIST_ROWS};

const MARKET_INDEX_NAME_MIN_WIRE_SIZE: usize = 2;

/// `emk_GetMarketsIndexes` response: list of market names in the same order as in `Markets.FList`.
/// `index` = position in the array (corresponds to `mIndex` in Delphi).
/// Wire-form (MoonProtoEngineServer.pas:278-284):
///   `count:i32 + names[count] (UTF-8 strings)`.
#[doc(hidden)]
pub(crate) fn parse_markets_indexes_response(data: &[u8]) -> Option<Vec<String>> {
    let mut r = EngineStreamReader::new(data);
    let count = r.read_count_bounded(
        MARKET_INDEX_NAME_MIN_WIRE_SIZE,
        MAX_MARKETS_LIST_ROWS,
        "GetMarketsIndexes.names",
    )?;
    let mut names = Vec::new();
    names.try_reserve_exact(count).ok()?;
    for _ in 0..count {
        names.push(r.read_str()?);
    }
    Some(names)
}

#[cfg(test)]
pub(crate) fn build_markets_indexes_response(names: &[String]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + names.iter().map(|s| 2 + s.len()).sum::<usize>());
    out.extend_from_slice(&(names.len() as i32).to_le_bytes());
    for n in names {
        write_str(&mut out, n);
    }
    out
}
