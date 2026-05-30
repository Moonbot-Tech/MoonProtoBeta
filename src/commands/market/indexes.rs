#[cfg(test)]
use super::write_str;
use super::EngineStreamReader;

/// `emk_GetMarketsIndexes` response: list of market names in the same order as in `Markets.FList`.
/// `index` = position in the array (corresponds to `mIndex` in Delphi).
/// Wire-form (MoonProtoEngineServer.pas:278-284):
///   `count:i32 + names[count] (UTF-8 strings)`.
#[doc(hidden)]
pub(crate) fn parse_markets_indexes_response(data: &[u8]) -> Option<Vec<String>> {
    let mut r = EngineStreamReader::new(data);
    // Each name is a UTF-8 string with a u16 prefix. At least 2 bytes (empty string).
    let count = r.read_count()?;
    let mut names = Vec::with_capacity(r.bounded_count_capacity(count, 2));
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
