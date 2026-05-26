use super::{write_str, EngineStreamReader};

/// Ответ `emk_GetMarketsIndexes`: список имён маркетов в том же порядке что в `Markets.FList`.
/// `index` = позиция в массиве (соответствует `mIndex` в Delphi).
/// Wire-form (MoonProtoEngineServer.pas:278-284):
///   `count:i32 + names[count] (UTF-8 strings)`.
pub fn parse_markets_indexes_response(data: &[u8]) -> Option<Vec<String>> {
    let mut r = EngineStreamReader::new(data);
    // Каждое имя — UTF-8 string с u16-prefix. Минимум 2 байта (пустая строка).
    let count = r.read_count()?;
    let mut names = Vec::with_capacity(r.bounded_count_capacity(count, 2));
    for _ in 0..count {
        names.push(r.read_str()?);
    }
    Some(names)
}

pub fn build_markets_indexes_response(names: &[String]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + names.iter().map(|s| 2 + s.len()).sum::<usize>());
    out.extend_from_slice(&(names.len() as i32).to_le_bytes());
    for n in names {
        write_str(&mut out, n);
    }
    out
}
