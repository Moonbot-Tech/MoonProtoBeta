//! `TStrategySerializer` snapshot writer.

use super::{FieldValue, StrategySnapshot, TID_ZERO_FLAG};
use crate::commands::strategy_schema::{StrategySchema, StrategySchemaField};
use flate2::write::DeflateEncoder;
use flate2::Compression;
use std::collections::HashMap;
use std::io::Write;

/// Builder for producing a DEFLATE-compressed snapshot. Wire-format mirror of
/// `BeginWrite/WriteStrategy/FinalizeWrite`: dicts, headers, type IDs, zero flag,
/// raw-deflate, and length truncation match Delphi.
///
/// The Delphi writer walks RTTI `TStrategy` + `GetStrategyPropMask`. Rust does
/// not keep a static copy of these tables: for a non-empty snapshot the writer
/// requires the live `TStratSchema` received from the server in Init. The schema
/// gives the same public field order, TypeID, PropMask visibility, and non-zero
/// defaults.
#[derive(Debug)]
pub(crate) struct StrategyBatchBuilder<'a> {
    schema: &'a StrategySchema,
    name_dict: Vec<String>,
    name_idx: HashMap<String, u16>,
    path_dict: Vec<String>,
    path_idx: HashMap<String, u16>,
    body: Vec<u8>,
    count: u16,
}

impl<'a> StrategyBatchBuilder<'a> {
    pub(crate) fn new(schema: &'a StrategySchema) -> Self {
        Self {
            schema,
            name_dict: Vec::new(),
            name_idx: HashMap::new(),
            path_dict: Vec::new(),
            path_idx: HashMap::new(),
            body: Vec::new(),
            count: 0,
        }
    }

    /// Valid serializer payload with zero strategies. No schema is needed,
    /// because Delphi `FinalizeWrite` writes empty dicts/body for an empty batch.
    pub(crate) fn empty_payload() -> Vec<u8> {
        finalize_strategy_batch(Vec::new(), Vec::new(), Vec::new(), 0)
    }

    fn name_index(&mut self, name: &str) -> u16 {
        if let Some(&i) = self.name_idx.get(name) {
            return i;
        }
        let i = self.name_dict.len() as u16;
        self.name_dict.push(name.to_string());
        self.name_idx.insert(name.to_string(), i);
        i
    }

    fn path_index(&mut self, path: &str) -> u16 {
        if let Some(&i) = self.path_idx.get(path) {
            return i;
        }
        let i = self.path_dict.len() as u16;
        self.path_dict.push(path.to_string());
        self.path_idx.insert(path.to_string(), i);
        i
    }

    /// Add a single strategy.
    pub(crate) fn write_strategy(&mut self, s: &StrategySnapshot) {
        let path_id = self.path_index(&s.path);
        // Header
        self.body.extend_from_slice(&s.strategy_id.to_le_bytes());
        self.body.extend_from_slice(&s.strategy_ver.to_le_bytes());
        self.body.extend_from_slice(&s.last_date.to_le_bytes());
        self.body.push(s.checked as u8);
        self.body.push(s.kind);
        self.body.extend_from_slice(&path_id.to_le_bytes());

        // Serialize the fields. Write the count (placeholder), update it later.
        let count_offset = self.body.len();
        self.body.extend_from_slice(&[0u8, 0u8]);
        let mut field_count = 0u16;

        // Schema fields are written in Delphi RTTI declaration order. The
        // visibility bitset is exactly `GetStrategyPropMask(kind)`.
        for field in &self.schema.fields {
            if !field.visible_for_kind(s.kind) {
                continue;
            }
            let Some(value) = s.fields.get(field.name.as_str()) else {
                continue;
            };
            if !strategy_schema_field_should_write(field, value) {
                continue;
            }
            let idx = self.name_index(&field.name);
            self.body.extend_from_slice(&idx.to_le_bytes());
            write_field(&mut self.body, value);
            field_count = field_count.wrapping_add(1);
        }
        // Backfill count
        self.body[count_offset..count_offset + 2].copy_from_slice(&field_count.to_le_bytes());
        self.count = self.count.wrapping_add(1);
    }

    /// Finalize into a DEFLATE-compressed payload (TStratSnapshot.data format).
    pub(crate) fn finalize(self) -> Vec<u8> {
        finalize_strategy_batch(self.name_dict, self.path_dict, self.body, self.count)
    }
}

fn strategy_schema_field_should_write(field: &StrategySchemaField, value: &FieldValue) -> bool {
    if !value.matches_type_id_inner(field.raw_type_id) {
        return false;
    }
    if let Some(default_value) = &field.default_value {
        return !value.equals_delphi_value_for_type_id_inner(default_value, field.raw_type_id);
    }
    !value.is_zero_for_type_id_inner(field.raw_type_id)
}

fn finalize_strategy_batch(
    name_dict: Vec<String>,
    path_dict: Vec<String>,
    body: Vec<u8>,
    count: u16,
) -> Vec<u8> {
    let mut plain = Vec::with_capacity(body.len() + 64);

    // NameDict
    plain.extend_from_slice(&(name_dict.len() as u16).to_le_bytes());
    for n in &name_dict {
        let b = n.as_bytes();
        // PathLen/NameLen are a byte (max 255). For strategies, field names are < 255 bytes.
        write_u8_len_bytes(&mut plain, b);
    }
    // PathDict
    plain.extend_from_slice(&(path_dict.len() as u16).to_le_bytes());
    for p in &path_dict {
        let b = p.as_bytes();
        write_u8_len_bytes(&mut plain, b);
    }
    // StratCount + body
    plain.extend_from_slice(&count.to_le_bytes());
    plain.extend_from_slice(&body);

    // DEFLATE compress (raw, no zlib header — Delphi -15)
    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&plain).unwrap();
    encoder.finish().unwrap()
}

pub(crate) fn write_field(out: &mut Vec<u8>, v: &FieldValue) {
    let type_id = v.type_id_inner();
    if v.is_zero() {
        // Write only the TypeID with the ZERO flag; value bytes are absent.
        out.push(type_id | TID_ZERO_FLAG);
        return;
    }
    out.push(type_id);
    match v {
        FieldValue::Bool(b) => out.push(*b as u8),
        FieldValue::Byte(b) => out.push(*b),
        FieldValue::Word(w) => out.extend_from_slice(&w.to_le_bytes()),
        FieldValue::Int32(i) => out.extend_from_slice(&i.to_le_bytes()),
        FieldValue::UInt32(u) => out.extend_from_slice(&u.to_le_bytes()),
        FieldValue::Int64(i) => out.extend_from_slice(&i.to_le_bytes()),
        FieldValue::UInt64(u) => out.extend_from_slice(&u.to_le_bytes()),
        FieldValue::Single(f) => out.extend_from_slice(&f.to_le_bytes()),
        FieldValue::Double(d) => out.extend_from_slice(&d.to_le_bytes()),
        FieldValue::String(s) => {
            let b = s.as_bytes();
            write_u16_len_bytes(out, b);
        }
    }
}

pub(crate) fn write_u8_len_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = bytes.len() as u8;
    let len_usize = usize::from(len);
    out.push(len);
    out.extend_from_slice(&bytes[..len_usize]);
}

fn write_u16_len_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = bytes.len() as u16;
    let len_usize = usize::from(len);
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&bytes[..len_usize]);
}
