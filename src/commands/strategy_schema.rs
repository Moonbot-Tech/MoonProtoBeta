//! `StrategySchemaBuilder.pas` schema blob reader.
//!
//! Delphi builds this blob from live `TStrategy` RTTI plus `GetFieldPickInfo`
//! and sends it as `TStratSchema.Data`: raw DEFLATE (-15), little-endian body.
//! The Rust active library stores the decoded schema so consumers do not carry
//! hardcoded strategy field UI metadata.

use std::io::Read;

use flate2::read::DeflateDecoder;

use super::strategy_serializer::{
    FieldValue, StrategyKind, TID_BOOL, TID_BYTE, TID_DOUBLE, TID_INT32, TID_INT64, TID_SINGLE,
    TID_STRING, TID_UINT32, TID_UINT64, TID_WORD,
};
use super::strict_read::{
    read_f32, read_f64, read_i32, read_i64, read_str16, read_str8, read_u16, read_u32, read_u64,
    read_u8,
};

pub(crate) const SCHEMA_FORMAT_VERSION: u8 = 1;

const UI_EDIT: u8 = 0;
const UI_CHECKBOX: u8 = 1;
const UI_COMBO: u8 = 2;
const UI_COLOR: u8 = 3;

const LA_NONE: u8 = 0;
const LA_COMMENT: u8 = 1;
const LA_FILTER_CLASS: u8 = 2;
const LA_CHAPTER_CLASS: u8 = 3;

const FLAG_HAS_STATIC: u8 = 0x10;
const FLAG_HAS_DYNAMIC: u8 = 0x20;
const FLAG_DEFAULT_NZ: u8 = 0x40;

/// Complete decoded `TStratSchema.Data` body.
#[derive(Debug, Clone, PartialEq)]
pub struct StrategySchema {
    pub format_version: u8,
    pub kinds: Vec<StrategySchemaKind>,
    pub fields: Vec<StrategySchemaField>,
}

/// One `TStrategyKind` entry from the schema kind table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategySchemaKind {
    pub ordinal: u8,
    pub name: String,
}

/// UI/wire metadata for one public `TStrategy` field.
#[derive(Debug, Clone, PartialEq)]
pub struct StrategySchemaField {
    pub name: String,
    pub(crate) raw_type_id: u8,
    pub type_id: StrategyFieldType,
    pub(crate) raw_flags: u8,
    pub ui_kind: StrategyFieldUiKind,
    pub layout: StrategyFieldLayout,
    pub default_value: Option<FieldValue>,
    /// `TStrategyKind` ordinals for which this field is visible.
    pub visible_kind_ordinals: Vec<u8>,
    /// Same visibility as a hot-path bitset by raw `TStrategyKind` ordinal.
    ///
    /// Internal serializer fast path. Public code should use
    /// `visible_kind_ordinals`, `visible_for_kind`, or
    /// `visible_for_strategy_kind`.
    pub(crate) visible_kind_mask: u32,
    /// Raw pipe string from Delphi `WriteStr16`, when `FLAG_HAS_STATIC` is set.
    pub(crate) static_picklist_raw: Option<String>,
    /// Split view of `static_picklist_raw`.
    pub static_picklist: Vec<String>,
    /// Dynamic picklist source, when `FLAG_HAS_DYNAMIC` is set.
    pub dynamic_picklist: Option<StrategyDynamicPicklist>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrategyFieldType {
    Bool,
    Int32,
    Int64,
    Double,
    String,
    Byte,
    Word,
    UInt32,
    UInt64,
    Single,
    Unknown(u8),
}

impl StrategyFieldType {
    pub fn from_type_id(type_id: u8) -> Self {
        match type_id {
            TID_BOOL => Self::Bool,
            TID_INT32 => Self::Int32,
            TID_INT64 => Self::Int64,
            TID_DOUBLE => Self::Double,
            TID_STRING => Self::String,
            TID_BYTE => Self::Byte,
            TID_WORD => Self::Word,
            TID_UINT32 => Self::UInt32,
            TID_UINT64 => Self::UInt64,
            TID_SINGLE => Self::Single,
            v => Self::Unknown(v),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Bool => "Bool",
            Self::Int32 => "Int32",
            Self::Int64 => "Int64",
            Self::Double => "Double",
            Self::String => "String",
            Self::Byte => "Byte",
            Self::Word => "Word",
            Self::UInt32 => "UInt32",
            Self::UInt64 => "UInt64",
            Self::Single => "Single",
            Self::Unknown(_) => "Unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrategyFieldUiKind {
    Edit,
    Checkbox,
    Combo,
    Color,
    Unknown(u8),
}

impl StrategyFieldUiKind {
    pub fn from_flags(flags: u8) -> Self {
        match flags & 0x03 {
            UI_EDIT => Self::Edit,
            UI_CHECKBOX => Self::Checkbox,
            UI_COMBO => Self::Combo,
            UI_COLOR => Self::Color,
            v => Self::Unknown(v),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StrategyFieldLayout {
    None,
    Comment(String),
    FilterClass(String),
    ChapterClass { value: String, chapter: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StrategyDynamicPicklist {
    /// Field `UseHookStrategy`: local strategies where kind is `sk_MoonHook`,
    /// with an empty item first.
    HookStrategies,
    /// Fields `ComboStart` / `ComboEnd`: all local strategies.
    AllStrategies,
    /// Future Delphi field name with dynamic bit set.
    FieldName(String),
}

impl StrategySchema {
    /// Parse raw-deflate `TStratSchema.Data`.
    pub fn parse_compressed(deflate_bytes: &[u8]) -> Option<Self> {
        let mut decoder = DeflateDecoder::new(deflate_bytes);
        let mut plain = Vec::new();
        decoder.read_to_end(&mut plain).ok()?;
        Self::parse_plain(&plain)
    }

    /// Parse already decompressed schema body.
    pub fn parse_plain(data: &[u8]) -> Option<Self> {
        let mut pos = 0usize;
        let format_version = read_u8(data, &mut pos)?;
        if format_version != SCHEMA_FORMAT_VERSION {
            return None;
        }
        let kind_count = read_u8(data, &mut pos)? as usize;
        let mut kinds = Vec::with_capacity(kind_count);
        for _ in 0..kind_count {
            let ordinal = read_u8(data, &mut pos)?;
            let name = read_str8(data, &mut pos)?;
            kinds.push(StrategySchemaKind { ordinal, name });
        }

        let field_count = read_u16(data, &mut pos)? as usize;
        let mut fields = Vec::with_capacity(field_count);
        let vis_bytes = (kind_count + 7) / 8;
        for _ in 0..field_count {
            let name = read_str8(data, &mut pos)?;
            let raw_type_id = read_u8(data, &mut pos)?;
            let raw_flags = read_u8(data, &mut pos)?;
            let layout = read_layout(data, &mut pos, raw_flags)?;
            let default_value = if raw_flags & FLAG_DEFAULT_NZ != 0 {
                Some(read_raw_value_by_type_id(data, &mut pos, raw_type_id)?)
            } else {
                None
            };

            if pos + vis_bytes > data.len() {
                return None;
            }
            let visibility = &data[pos..pos + vis_bytes];
            pos += vis_bytes;
            let mut visible_kind_ordinals = Vec::new();
            for (idx, kind) in kinds.iter().enumerate() {
                let byte_idx = idx >> 3;
                let bit = idx & 7;
                if visibility
                    .get(byte_idx)
                    .is_some_and(|b| (b & (1u8 << bit)) != 0)
                {
                    visible_kind_ordinals.push(kind.ordinal);
                }
            }
            let visible_kind_mask = visible_kind_mask(&visible_kind_ordinals);

            let static_picklist_raw = if raw_flags & FLAG_HAS_STATIC != 0 {
                Some(read_str16(data, &mut pos)?)
            } else {
                None
            };
            let static_picklist = static_picklist_raw
                .as_deref()
                .map(split_picklist)
                .unwrap_or_default();
            let dynamic_picklist = if raw_flags & FLAG_HAS_DYNAMIC != 0 {
                Some(dynamic_picklist_for_field(&name))
            } else {
                None
            };

            fields.push(StrategySchemaField {
                name,
                raw_type_id,
                type_id: StrategyFieldType::from_type_id(raw_type_id),
                raw_flags,
                ui_kind: StrategyFieldUiKind::from_flags(raw_flags),
                layout,
                default_value,
                visible_kind_ordinals,
                visible_kind_mask,
                static_picklist_raw,
                static_picklist,
                dynamic_picklist,
            });
        }

        if pos != data.len() {
            return None;
        }

        Some(Self {
            format_version,
            kinds,
            fields,
        })
    }

    pub fn kind_name(&self, ordinal: u8) -> Option<&str> {
        self.kinds
            .iter()
            .find(|k| k.ordinal == ordinal)
            .map(|k| k.name.as_str())
    }

    pub fn field(&self, name: &str) -> Option<&StrategySchemaField> {
        self.fields.iter().find(|f| f.name == name)
    }
}

impl StrategySchemaField {
    #[doc(hidden)]
    pub fn raw_type_id(&self) -> u8 {
        self.raw_type_id
    }

    #[doc(hidden)]
    pub fn raw_flags(&self) -> u8 {
        self.raw_flags
    }

    #[doc(hidden)]
    pub fn static_picklist_raw(&self) -> Option<&str> {
        self.static_picklist_raw.as_deref()
    }

    pub fn visible_for_kind(&self, kind: u8) -> bool {
        if kind < 32 {
            self.visible_kind_mask & (1u32 << kind) != 0
        } else {
            self.visible_kind_ordinals.contains(&kind)
        }
    }

    pub fn visible_for_strategy_kind(&self, kind: StrategyKind) -> bool {
        self.visible_for_kind(kind.to_byte())
    }
}

pub(crate) fn visible_kind_mask(ordinals: &[u8]) -> u32 {
    ordinals.iter().fold(0u32, |mask, &kind| {
        if kind < 32 {
            mask | (1u32 << kind)
        } else {
            mask
        }
    })
}

fn read_layout(data: &[u8], pos: &mut usize, flags: u8) -> Option<StrategyFieldLayout> {
    match (flags >> 2) & 0x03 {
        LA_NONE => Some(StrategyFieldLayout::None),
        LA_COMMENT => Some(StrategyFieldLayout::Comment(read_str8(data, pos)?)),
        LA_FILTER_CLASS => Some(StrategyFieldLayout::FilterClass(read_str8(data, pos)?)),
        LA_CHAPTER_CLASS => Some(StrategyFieldLayout::ChapterClass {
            value: read_str8(data, pos)?,
            chapter: read_str8(data, pos)?,
        }),
        _ => None,
    }
}

fn read_raw_value_by_type_id(data: &[u8], pos: &mut usize, type_id: u8) -> Option<FieldValue> {
    match type_id {
        TID_BOOL => Some(FieldValue::Bool(read_u8(data, pos)? != 0)),
        TID_BYTE => Some(FieldValue::Byte(read_u8(data, pos)?)),
        TID_WORD => Some(FieldValue::Word(read_u16(data, pos)?)),
        TID_INT32 => Some(FieldValue::Int32(read_i32(data, pos)?)),
        TID_UINT32 => Some(FieldValue::UInt32(read_u32(data, pos)?)),
        TID_INT64 => Some(FieldValue::Int64(read_i64(data, pos)?)),
        TID_UINT64 => Some(FieldValue::UInt64(read_u64(data, pos)?)),
        TID_SINGLE => Some(FieldValue::Single(read_f32(data, pos)?)),
        TID_DOUBLE => Some(FieldValue::Double(read_f64(data, pos)?)),
        TID_STRING => Some(FieldValue::String(read_str16(data, pos)?)),
        _ => None,
    }
}

fn dynamic_picklist_for_field(field_name: &str) -> StrategyDynamicPicklist {
    match field_name {
        "UseHookStrategy" => StrategyDynamicPicklist::HookStrategies,
        "ComboStart" | "ComboEnd" => StrategyDynamicPicklist::AllStrategies,
        other => StrategyDynamicPicklist::FieldName(other.to_string()),
    }
}

fn split_picklist(raw: &str) -> Vec<String> {
    if raw.is_empty() {
        Vec::new()
    } else {
        raw.split('|').map(str::to_string).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::DeflateEncoder;
    use flate2::Compression;
    use std::io::Write;

    fn str8(out: &mut Vec<u8>, value: &str) {
        out.push(value.len() as u8);
        out.extend_from_slice(value.as_bytes());
    }

    fn str16(out: &mut Vec<u8>, value: &str) {
        out.extend_from_slice(&(value.len() as u16).to_le_bytes());
        out.extend_from_slice(value.as_bytes());
    }

    fn deflate(raw: &[u8]) -> Vec<u8> {
        let mut enc = DeflateEncoder::new(Vec::new(), Compression::default());
        enc.write_all(raw).unwrap();
        enc.finish().unwrap()
    }

    #[test]
    // parity: MoonBot StrategySchemaBuilder.pas:BuildStrategySchemaBlob
    fn parses_strategy_schema_body() {
        let mut raw = Vec::new();
        raw.push(SCHEMA_FORMAT_VERSION);
        raw.push(2); // kind_count
        raw.push(1);
        str8(&mut raw, "Telegram");
        raw.push(20);
        str8(&mut raw, "MoonHook");
        raw.extend_from_slice(&4u16.to_le_bytes());

        str8(&mut raw, "StrategyName");
        raw.push(TID_STRING);
        raw.push((LA_CHAPTER_CLASS << 2) | UI_EDIT);
        str8(&mut raw, "General");
        str8(&mut raw, "Main");
        raw.push(0b0000_0011);

        str8(&mut raw, "AcceptCommands");
        raw.push(TID_BOOL);
        raw.push(UI_CHECKBOX | FLAG_DEFAULT_NZ);
        raw.push(1);
        raw.push(0b0000_0001);

        str8(&mut raw, "SignalType");
        raw.push(TID_STRING);
        raw.push(UI_COMBO | FLAG_HAS_STATIC);
        raw.push(0b0000_0011);
        str16(&mut raw, "Telegram|MoonHook");

        str8(&mut raw, "UseHookStrategy");
        raw.push(TID_STRING);
        raw.push(UI_COMBO | FLAG_HAS_DYNAMIC);
        raw.push(0b0000_0010);

        let schema = StrategySchema::parse_compressed(&deflate(&raw)).unwrap();
        assert_eq!(schema.format_version, SCHEMA_FORMAT_VERSION);
        assert_eq!(schema.kinds.len(), 2);
        assert_eq!(schema.kind_name(20), Some("MoonHook"));
        assert_eq!(schema.fields.len(), 4);

        let first = &schema.fields[0];
        assert_eq!(first.name, "StrategyName");
        assert_eq!(first.type_id, StrategyFieldType::String);
        assert_eq!(first.ui_kind, StrategyFieldUiKind::Edit);
        assert_eq!(
            first.layout,
            StrategyFieldLayout::ChapterClass {
                value: "General".to_string(),
                chapter: "Main".to_string()
            }
        );
        assert_eq!(first.visible_kind_ordinals, vec![1, 20]);

        let checkbox = &schema.fields[1];
        assert_eq!(checkbox.default_value, Some(FieldValue::Bool(true)));
        assert_eq!(checkbox.visible_kind_ordinals, vec![1]);

        let signal = &schema.fields[2];
        assert_eq!(
            signal.static_picklist_raw.as_deref(),
            Some("Telegram|MoonHook")
        );
        assert_eq!(
            signal.static_picklist,
            vec!["Telegram".to_string(), "MoonHook".to_string()]
        );

        let dynamic = &schema.fields[3];
        assert_eq!(
            dynamic.dynamic_picklist,
            Some(StrategyDynamicPicklist::HookStrategies)
        );
        assert_eq!(dynamic.visible_kind_ordinals, vec![20]);
    }

    #[test]
    fn rejects_trailing_or_truncated_schema_body() {
        let mut raw = vec![SCHEMA_FORMAT_VERSION, 0];
        raw.extend_from_slice(&0u16.to_le_bytes());
        let mut with_tail = raw.clone();
        with_tail.push(99);
        assert!(StrategySchema::parse_plain(&raw).is_some());
        assert!(StrategySchema::parse_plain(&with_tail).is_none());
        assert!(StrategySchema::parse_plain(&[SCHEMA_FORMAT_VERSION, 3]).is_none());
    }
}
