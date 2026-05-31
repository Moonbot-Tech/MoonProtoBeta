//! `TStrategySerializer` snapshot reader.

use super::{
    FieldValue, StrategyBatch, StrategyFields, StrategySnapshot, TID_BOOL, TID_BYTE, TID_DOUBLE,
    TID_INT32, TID_INT64, TID_SINGLE, TID_STRING, TID_UINT32, TID_UINT64, TID_WORD, TID_ZERO_FLAG,
};
use crate::commands::inflate::read_inflate_to_vec;
use crate::commands::registry::decode_utf8_delphi;
use crate::commands::strategy_schema::StrategySchema;
use crate::commands::strict_read::{read_i32, read_u16, read_u64, read_u8};
use flate2::read::DeflateDecoder;
use std::collections::HashMap;
use std::sync::Arc;

/// Parse from a DEFLATE-compressed payload (as it arrives in `TStratSnapshot.data`).
pub fn parse_strategy_batch(deflate_bytes: &[u8]) -> Option<StrategyBatch> {
    parse_strategy_batch_with_schema(deflate_bytes, None)
}

/// Parse a payload with field validation against the live `TStratSchema`.
///
/// If a schema is provided, the reader replicates Delphi `BuildReaderProps`/`ReadField`:
/// the name must exist in the public `TStrategy` schema, and the wire TypeID must
/// match the RTTI TypeID. Otherwise the field is skipped via `SkipFieldByTypeID`.
/// Without a schema the function stays a generic wire-format reader for diagnostics.
pub(crate) fn parse_strategy_batch_with_schema(
    deflate_bytes: &[u8],
    schema: Option<&StrategySchema>,
) -> Option<StrategyBatch> {
    let schema_field_types = schema.map(build_schema_field_type_map);
    parse_strategy_batch_with_schema_field_types(deflate_bytes, schema_field_types.as_ref())
}

pub(crate) fn parse_strategy_batch_with_schema_field_types(
    deflate_bytes: &[u8],
    schema_field_types: Option<&HashMap<String, u8>>,
) -> Option<StrategyBatch> {
    let mut decoder = DeflateDecoder::new(deflate_bytes);
    let decompressed = read_inflate_to_vec(
        &mut decoder,
        strategy_plain_capacity_hint(deflate_bytes.len()),
    )
    .ok()?;
    parse_strategy_batch_plain_with_schema_field_types(&decompressed, schema_field_types)
}

pub(crate) fn parse_strategy_batch_for_each_with_schema_field_types<F>(
    deflate_bytes: &[u8],
    schema_field_types: Option<&HashMap<String, u8>>,
    mut on_strategy: F,
) -> Option<usize>
where
    F: FnMut(StrategySnapshot),
{
    let mut decoder = DeflateDecoder::new(deflate_bytes);
    let decompressed = read_inflate_to_vec(
        &mut decoder,
        strategy_plain_capacity_hint(deflate_bytes.len()),
    )
    .ok()?;
    parse_strategy_batch_plain_for_each_with_schema_field_types(
        &decompressed,
        schema_field_types,
        &mut on_strategy,
    )
}

fn strategy_plain_capacity_hint(deflate_len: usize) -> usize {
    // Live strategy snapshots are raw-deflate RTTI streams with many repeated
    // field names/zero values; prod 44KB payloads currently inflate to ~1.5MB.
    // Delphi reads a contiguous memory stream, so pre-sizing keeps Rust from
    // repeatedly reallocating/copying the decompressed stream before parsing it.
    deflate_len
        .saturating_mul(40)
        .clamp(4 * 1024, 8 * 1024 * 1024)
}

/// Parse an already-decompressed flat payload (for the case where decompression was done externally).
#[cfg(test)]
pub(crate) fn parse_strategy_batch_plain(data: &[u8]) -> Option<StrategyBatch> {
    parse_strategy_batch_plain_with_schema(data, None)
}

#[cfg(test)]
pub(crate) fn parse_strategy_batch_plain_with_schema(
    data: &[u8],
    schema: Option<&StrategySchema>,
) -> Option<StrategyBatch> {
    let schema_field_types = schema.map(build_schema_field_type_map);
    parse_strategy_batch_plain_with_schema_field_types(data, schema_field_types.as_ref())
}

fn parse_strategy_batch_plain_with_schema_field_types(
    data: &[u8],
    schema_field_types: Option<&HashMap<String, u8>>,
) -> Option<StrategyBatch> {
    let mut pos = 0usize;
    let names = read_dict(data, &mut pos)?;
    let paths = read_dict_arc(data, &mut pos)?;
    let field_names = names
        .iter()
        .map(|name| Arc::<str>::from(name.as_str()))
        .collect::<Vec<_>>();
    let reader_fields =
        schema_field_types.map(|field_types| build_reader_fields(&names, field_types));
    let strat_count = read_u16(data, &mut pos)? as usize;
    let mut strategies = Vec::with_capacity(strat_count);
    for _ in 0..strat_count {
        strategies.push(read_strategy(
            data,
            &mut pos,
            &field_names,
            &paths,
            reader_fields.as_deref(),
        )?);
    }
    Some(StrategyBatch {
        names,
        paths,
        strategies,
    })
}

fn parse_strategy_batch_plain_for_each_with_schema_field_types<F>(
    data: &[u8],
    schema_field_types: Option<&HashMap<String, u8>>,
    on_strategy: &mut F,
) -> Option<usize>
where
    F: FnMut(StrategySnapshot),
{
    let mut pos = 0usize;
    let field_names = read_dict_arc(data, &mut pos)?;
    let paths = read_dict_arc(data, &mut pos)?;
    let reader_fields =
        schema_field_types.map(|field_types| build_reader_fields_arc(&field_names, field_types));
    let strat_count = read_u16(data, &mut pos)? as usize;
    for _ in 0..strat_count {
        let strategy = read_strategy(
            data,
            &mut pos,
            &field_names,
            &paths,
            reader_fields.as_deref(),
        )?;
        on_strategy(strategy);
    }
    Some(strat_count)
}

fn read_dict(data: &[u8], pos: &mut usize) -> Option<Vec<String>> {
    let count = read_u16(data, pos)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let len = read_u8(data, pos)? as usize;
        if *pos + len > data.len() {
            return None;
        }
        let s = decode_utf8_delphi(&data[*pos..*pos + len]);
        *pos += len;
        out.push(s);
    }
    Some(out)
}

fn read_dict_arc(data: &[u8], pos: &mut usize) -> Option<Vec<Arc<str>>> {
    let count = read_u16(data, pos)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let len = read_u8(data, pos)? as usize;
        if *pos + len > data.len() {
            return None;
        }
        let s = decode_utf8_delphi(&data[*pos..*pos + len]);
        *pos += len;
        out.push(Arc::<str>::from(s));
    }
    Some(out)
}

fn build_reader_fields(names: &[String], schema_by_name: &HashMap<String, u8>) -> Vec<Option<u8>> {
    // Delphi `TStrategySerializer.BuildReaderProps`: NameDict -> RTTI field
    // mapping is built once for the whole snapshot, then ReadField only indexes
    // it by FieldIdx. The active library caches the schema name map at
    // `TStratSchema` apply time, so live snapshots do not rebuild it.
    names
        .iter()
        .map(|name| schema_by_name.get(name.as_str()).copied())
        .collect()
}

fn build_reader_fields_arc(
    names: &[Arc<str>],
    schema_by_name: &HashMap<String, u8>,
) -> Vec<Option<u8>> {
    // Same reader-prop table as `build_reader_fields`, but the active apply
    // path does not need the public `Vec<String>` returned by generic parser.
    names
        .iter()
        .map(|name| schema_by_name.get(name.as_ref()).copied())
        .collect()
}

fn build_schema_field_type_map(schema: &StrategySchema) -> HashMap<String, u8> {
    schema
        .fields
        .iter()
        .map(|field| (field.name.clone(), field.raw_type_id))
        .collect()
}

fn read_strategy(
    data: &[u8],
    pos: &mut usize,
    field_names: &[Arc<str>],
    paths: &[Arc<str>],
    reader_fields: Option<&[Option<u8>]>,
) -> Option<StrategySnapshot> {
    let strategy_id = read_u64(data, pos)?;
    let strategy_ver = read_i32(data, pos)?;
    let last_date = read_u64(data, pos)?;
    let checked = read_u8(data, pos)? != 0;
    let kind = read_u8(data, pos)?;
    let path_id = read_u16(data, pos)? as usize;
    let path = paths
        .get(path_id)
        .cloned()
        .unwrap_or_else(|| Arc::<str>::from(""));

    let field_count = read_u16(data, pos)? as usize;
    let mut fields = StrategyFields::with_capacity(field_count);

    for _ in 0..field_count {
        let field_idx = read_u16(data, pos)? as usize;
        let type_id = read_u8(data, pos)?;
        let is_zero = (type_id & TID_ZERO_FLAG) != 0;
        let real_type = type_id & 0x7F;

        if let Some(reader_fields) = reader_fields {
            let Some(expected_type_id) = reader_fields.get(field_idx).and_then(|field| *field)
            else {
                skip_field_by_type_id(data, pos, type_id)?;
                continue;
            };
            if real_type != expected_type_id {
                skip_field_by_type_id(data, pos, type_id)?;
                continue;
            }
        }

        let value: Option<FieldValue> = if is_zero {
            // Value bytes are absent (Delphi: `If (TypeID and TID_ZERO_FLAG) <> 0 then exit`).
            FieldValue::zero(real_type)
        } else {
            try_read_field_value(data, pos, real_type)
        };

        if let Some(v) = value {
            if let Some(name) = field_names.get(field_idx) {
                fields.push_deserialized_field(Arc::clone(name), v);
            }
            // Otherwise the field is of a known type, but the name is not in the dictionary.
            // Delphi behavior: ReaderProps[idx] = nil → SkipField; at this point we have
            // ALREADY read the value, so we just ignore it (the position is correct).
        }
        // If value=None and !is_zero, this is the unknown-TypeID case: `try_read_field_value`
        // did a fallback skip of 8 bytes (like the Delphi `SkipFieldByTypeID` else branch pas:373).
    }

    Some(StrategySnapshot {
        strategy_id,
        strategy_ver,
        last_date,
        checked,
        kind,
        path,
        fields,
    })
}

/// Reads a value by `type_id`. If type_id is unknown, fall back to skipping 8 bytes
/// (like `SkipFieldByTypeID` pas:373: `Stream.Position := Stream.Position + 8`).
pub(crate) fn try_read_field_value(
    data: &[u8],
    pos: &mut usize,
    type_id: u8,
) -> Option<FieldValue> {
    match type_id {
        TID_BOOL => Some(FieldValue::Bool(read_zero_tail::<1>(data, pos)[0] != 0)),
        TID_BYTE => Some(FieldValue::Byte(read_zero_tail::<1>(data, pos)[0])),
        TID_WORD => Some(FieldValue::Word(u16::from_le_bytes(read_zero_tail::<2>(
            data, pos,
        )))),
        TID_INT32 => Some(FieldValue::Int32(i32::from_le_bytes(read_zero_tail::<4>(
            data, pos,
        )))),
        TID_UINT32 => Some(FieldValue::UInt32(u32::from_le_bytes(read_zero_tail::<4>(
            data, pos,
        )))),
        TID_INT64 => Some(FieldValue::Int64(i64::from_le_bytes(read_zero_tail::<8>(
            data, pos,
        )))),
        TID_UINT64 => Some(FieldValue::UInt64(u64::from_le_bytes(read_zero_tail::<8>(
            data, pos,
        )))),
        TID_SINGLE => Some(FieldValue::Single(f32::from_le_bytes(read_zero_tail::<4>(
            data, pos,
        )))),
        TID_DOUBLE => Some(FieldValue::Double(f64::from_le_bytes(read_zero_tail::<8>(
            data, pos,
        )))),
        TID_STRING => {
            let len = read_u16(data, pos)? as usize;
            let available = data.len().saturating_sub(*pos).min(len);
            let s = if available == len {
                // Normal path: full string present. Decode straight from the wire
                // slice — no intermediate zero-filled Vec + copy per field.
                let s = decode_utf8_delphi(&data[*pos..*pos + len]);
                *pos += len;
                s
            } else {
                // Short tail (malformed/truncated): zero-pad to len like Delphi
                // `Stream.Read`. Rare; the temp buffer only allocates here.
                let mut bytes = vec![0u8; len];
                if available > 0 {
                    bytes[..available].copy_from_slice(&data[*pos..*pos + available]);
                    *pos += available;
                }
                decode_utf8_delphi(&bytes)
            };
            Some(FieldValue::String(s))
        }
        _ => {
            // Unknown — fallback skip of 8 bytes. The position advances, but no value is returned.
            *pos = (*pos + 8).min(data.len());
            None
        }
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

fn skip_field_by_type_id(data: &[u8], pos: &mut usize, type_id: u8) -> Option<()> {
    if (type_id & TID_ZERO_FLAG) != 0 {
        return Some(());
    }

    let size = match type_id & 0x7F {
        TID_BOOL | TID_BYTE => Some(1),
        TID_WORD => Some(2),
        TID_INT32 | TID_UINT32 | TID_SINGLE => Some(4),
        TID_INT64 | TID_UINT64 | TID_DOUBLE => Some(8),
        TID_STRING => {
            let len = read_u16(data, pos)? as usize;
            *pos = (*pos + len).min(data.len());
            return Some(());
        }
        _ => Some(8),
    }?;

    *pos = (*pos + size).min(data.len());
    Some(())
}
