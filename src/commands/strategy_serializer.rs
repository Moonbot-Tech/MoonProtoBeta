//! `TStrategySerializer` reader/writer — Delphi wire-format port.
//!
//! Источник Delphi: `MoonProto/StrategySerializer.pas` (~1118 строк).
//!
//! ## Назначение
//! Парсит RTTI-driven binary snapshot стратегий из payload'а `TStratSnapshot.data`.
//! Сервер (Delphi MoonBot) использует RTTI для итерации по public-полям `TStrategy`;
//! Rust-клиент не имеет RTTI, поэтому хранит поля как `StrategyFields`:
//! плотный список `(FieldName, FieldValue)` с lookup по имени.
//! Для typed writer и Delphi `ReadField` TypeID-проверок Rust использует live
//! `TStratSchema`, полученную от сервера, а не статическую копию `TStrategy`
//! field order/defaults.
//!
//! ## Wire format (после DEFLATE decompression, raw -15)
//!
//! ```text
//! NameDict:    Count:u16 + (NameLen:u8 + Name:bytes[NameLen]) * Count    // UTF-8
//! PathDict:    Count:u16 + (PathLen:u8 + Path:bytes[PathLen]) * Count    // UTF-8
//! StratCount:  u16
//! Strategies[StratCount]:
//!     StrategyID:        u64
//!     StrategyVer:       i32
//!     StrategyLastDate:  u64    // unix epoch ms
//!     Checked:           u8     // boolean
//!     Kind:              u8     // TStrategyKind ordinal
//!     PathID:            u16    // index в PathDict
//!     FieldCount:        u16
//!     Fields[FieldCount]:
//!         FieldIdx:      u16    // index в NameDict
//!         TypeID:        u8     // (с возможным флагом TID_ZERO_FLAG = 0x80)
//!         [value]               // отсутствует если ZERO_FLAG установлен; иначе зависит от типа
//! ```
//!
//! ## TypeID constants
//! - `TID_BOOL=1`:    1 byte
//! - `TID_INT32=2`:   4 bytes (signed)
//! - `TID_INT64=3`:   8 bytes (signed)
//! - `TID_DOUBLE=4`:  8 bytes (f64)
//! - `TID_STRING=5`:  u16 LE prefix + UTF-8 bytes
//! - `TID_BYTE=6`:    1 byte (unsigned)
//! - `TID_WORD=7`:    2 bytes (unsigned)
//! - `TID_UINT32=8`:  4 bytes (unsigned)
//! - `TID_UINT64=9`:  8 bytes (unsigned)
//! - `TID_SINGLE=10`: 4 bytes (f32)
//! - `TID_ZERO_FLAG = 0x80` (high bit): значение = zero для соответствующего типа, value bytes отсутствуют.
//!
//! ## Unknown TypeID
//! Reader делает fallback skip 8 байт (как Delphi `SkipFieldByTypeID`).

use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::Arc;

use super::registry::decode_utf8_delphi;
use super::strategy_schema::{StrategySchema, StrategySchemaField};
use super::strict_read::{read_i32, read_u16, read_u64, read_u8};
use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use flate2::Compression;

// =============================================================================
//  TypeID constants
// =============================================================================

pub const TID_BOOL: u8 = 1;
pub const TID_INT32: u8 = 2;
pub const TID_INT64: u8 = 3;
pub const TID_DOUBLE: u8 = 4;
pub const TID_STRING: u8 = 5;
pub const TID_BYTE: u8 = 6;
pub const TID_WORD: u8 = 7;
pub const TID_UINT32: u8 = 8;
pub const TID_UINT64: u8 = 9;
pub const TID_SINGLE: u8 = 10;
pub const TID_ZERO_FLAG: u8 = 0x80;

// =============================================================================
//  FieldValue
// =============================================================================

/// Decoded поле стратегии. Соответствует Delphi `TValue` после RTTI-десериализации.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Double(f64),
    String(String),
    Byte(u8),
    Word(u16),
    UInt32(u32),
    UInt64(u64),
    Single(f32),
}

impl FieldValue {
    /// Zero значение для указанного TypeID. Используется когда установлен `TID_ZERO_FLAG`.
    pub fn zero(type_id: u8) -> Option<Self> {
        Some(match type_id & 0x7F {
            TID_BOOL => FieldValue::Bool(false),
            TID_INT32 => FieldValue::Int32(0),
            TID_INT64 => FieldValue::Int64(0),
            TID_DOUBLE => FieldValue::Double(0.0),
            TID_STRING => FieldValue::String(String::new()),
            TID_BYTE => FieldValue::Byte(0),
            TID_WORD => FieldValue::Word(0),
            TID_UINT32 => FieldValue::UInt32(0),
            TID_UINT64 => FieldValue::UInt64(0),
            TID_SINGLE => FieldValue::Single(0.0),
            _ => return None,
        })
    }

    pub fn type_id(&self) -> u8 {
        match self {
            FieldValue::Bool(_) => TID_BOOL,
            FieldValue::Int32(_) => TID_INT32,
            FieldValue::Int64(_) => TID_INT64,
            FieldValue::Double(_) => TID_DOUBLE,
            FieldValue::String(_) => TID_STRING,
            FieldValue::Byte(_) => TID_BYTE,
            FieldValue::Word(_) => TID_WORD,
            FieldValue::UInt32(_) => TID_UINT32,
            FieldValue::UInt64(_) => TID_UINT64,
            FieldValue::Single(_) => TID_SINGLE,
        }
    }

    pub fn matches_type_id(&self, type_id: u8) -> bool {
        self.type_id() == (type_id & 0x7F)
    }

    /// True если значение эквивалентно zero для своего типа.
    /// Соответствует `IsZeroValue` (StrategySerializer.pas:337-355).
    pub fn is_zero(&self) -> bool {
        match self {
            FieldValue::Bool(b) => !*b,
            FieldValue::Int32(v) => *v == 0,
            FieldValue::Int64(v) => *v == 0,
            FieldValue::Double(v) => v.abs() < 1e-10,
            FieldValue::String(s) => s.is_empty(),
            FieldValue::Byte(v) => *v == 0,
            FieldValue::Word(v) => *v == 0,
            FieldValue::UInt32(v) => *v == 0,
            FieldValue::UInt64(v) => *v == 0,
            FieldValue::Single(v) => v.abs() < 1e-10,
        }
    }

    pub fn is_zero_for_type_id(&self, type_id: u8) -> bool {
        self.matches_type_id(type_id) && self.is_zero()
    }

    /// Сравнение как Delphi `IsDefaultValue`: float/single через `1e-10`,
    /// остальные типы точно, и только при совпавшем TypeID.
    pub fn equals_delphi_value_for_type_id(&self, other: &Self, type_id: u8) -> bool {
        if !self.matches_type_id(type_id) || !other.matches_type_id(type_id) {
            return false;
        }
        match (type_id & 0x7F, self, other) {
            (TID_BOOL, FieldValue::Bool(a), FieldValue::Bool(b)) => a == b,
            (TID_BYTE, FieldValue::Byte(a), FieldValue::Byte(b)) => a == b,
            (TID_WORD, FieldValue::Word(a), FieldValue::Word(b)) => a == b,
            (TID_INT32, FieldValue::Int32(a), FieldValue::Int32(b)) => a == b,
            (TID_UINT32, FieldValue::UInt32(a), FieldValue::UInt32(b)) => a == b,
            (TID_INT64, FieldValue::Int64(a), FieldValue::Int64(b)) => a == b,
            (TID_UINT64, FieldValue::UInt64(a), FieldValue::UInt64(b)) => a == b,
            (TID_SINGLE, FieldValue::Single(a), FieldValue::Single(b)) => (*a - *b).abs() < 1e-10,
            (TID_DOUBLE, FieldValue::Double(a), FieldValue::Double(b)) => (*a - *b).abs() < 1e-10,
            (TID_STRING, FieldValue::String(a), FieldValue::String(b)) => a == b,
            _ => false,
        }
    }
}

// =============================================================================
//  StrategySnapshot
// =============================================================================

/// Распакованный snapshot одной стратегии. Поля хранятся в `StrategyFields` по
/// имени; потребитель использует `FieldValue::*` extractors для строгой
/// типизации.
#[derive(Debug, Clone)]
pub struct StrategySnapshot {
    pub strategy_id: u64,
    pub strategy_ver: i32,
    /// Unix epoch ms (TDateTime → UnixTimeToDelphi на стороне сервера, см. pas:671).
    pub last_date: u64,
    pub checked: bool,
    pub kind: u8,
    /// Folder path (из PathDict по PathID; пустая строка если PathID out-of-range).
    pub path: String,
    pub fields: StrategyFields,
}

/// Decoded strategy fields keyed by Delphi `NameDict` field name.
///
/// This is intentionally a dense vector, not a Rust `HashMap`: Delphi reads a
/// compact RTTI field stream in order, and each strategy usually has only a
/// small visible field set. A dense list avoids per-field hashing while keeping
/// the public ergonomic operations (`get`, `insert`, `iter`).
#[derive(Debug, Clone, Default)]
pub struct StrategyFields {
    entries: Vec<(Arc<str>, FieldValue)>,
}

impl StrategyFields {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
        }
    }

    pub fn insert<K>(&mut self, key: K, value: FieldValue) -> Option<FieldValue>
    where
        K: Into<Arc<str>>,
    {
        let key = key.into();
        if let Some((_, existing)) = self
            .entries
            .iter_mut()
            .find(|(name, _)| name.as_ref() == key.as_ref())
        {
            return Some(std::mem::replace(existing, value));
        }
        self.entries.push((key, value));
        None
    }

    #[inline]
    fn push_deserialized_field(&mut self, key: Arc<str>, value: FieldValue) {
        // Delphi `TStrategySerializer` writes each RTTI field at most once per
        // strategy. The hot reader path can append directly; public `insert`
        // keeps replacement semantics for user-built snapshots.
        self.entries.push((key, value));
    }

    pub fn get(&self, key: &str) -> Option<&FieldValue> {
        self.entries
            .iter()
            .find(|(name, _)| name.as_ref() == key)
            .map(|(_, value)| value)
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Arc<str>, &FieldValue)> {
        self.entries.iter().map(|(name, value)| (name, value))
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl<K> FromIterator<(K, FieldValue)> for StrategyFields
where
    K: Into<Arc<str>>,
{
    fn from_iter<T: IntoIterator<Item = (K, FieldValue)>>(iter: T) -> Self {
        let mut fields = Self::new();
        for (key, value) in iter {
            fields.insert(key, value);
        }
        fields
    }
}

/// Raw Delphi `TStrategyKind` ordinal (`Strategies.pas`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StrategyKind(pub u8);

impl StrategyKind {
    pub const UNKNOWN: Self = Self(0);
    pub const TELEGRAM: Self = Self(1);
    pub const DROPS: Self = Self(2);
    pub const WALLS: Self = Self(3);
    pub const VOLUMES: Self = Self(4);
    pub const PUMP_DETECTION: Self = Self(5);
    pub const MOON_SHOT: Self = Self(6);
    pub const V_LITE: Self = Self(7);
    pub const DELTA: Self = Self(8);
    pub const WAVES: Self = Self(9);
    pub const COMBO: Self = Self(10);
    pub const UDP: Self = Self(11);
    pub const MANUAL: Self = Self(12);
    pub const MOON_STRIKE: Self = Self(13);
    pub const NEW_LISTING: Self = Self(14);
    pub const LIQUIDATIONS: Self = Self(15);
    pub const TOP_MARKET: Self = Self(16);
    pub const EMA: Self = Self(17);
    pub const SPREAD: Self = Self(18);
    pub const CHART_WALL: Self = Self(19);
    pub const MOON_HOOK: Self = Self(20);
    pub const ACTIVITY: Self = Self(21);
    pub const ALERTS: Self = Self(22);
    pub const WATCHER: Self = Self(23);
}

/// Delphi strategy active-state mode from `TStratForm.CheckActive`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StrategyActiveMode {
    /// `cfg.MoonProtoConfig.ActiveClient = true`.
    ActiveClient,
    /// `UsingMoonProto = true` and not `ActiveClient`.
    UsingMoonProto,
    /// Standalone MoonBot path, without MoonProto split.
    Standalone,
}

impl StrategySnapshot {
    pub fn kind_like_delphi(&self) -> StrategyKind {
        StrategyKind(self.kind)
    }

    pub fn field_bool_or_false(&self, name: &str) -> bool {
        matches!(self.fields.get(name), Some(FieldValue::Bool(true)))
    }

    pub fn auto_buy_like_delphi(&self) -> bool {
        self.field_bool_or_false("AutoBuy")
    }

    pub fn run_detect_on_kernel_like_delphi(&self) -> bool {
        self.field_bool_or_false("RunDetectOnKernel")
    }

    pub fn short_like_delphi(&self) -> bool {
        self.field_bool_or_false("Short")
    }

    pub fn sell_from_asset_like_delphi(&self) -> bool {
        self.field_bool_or_false("SellFromAsset")
    }

    /// Delphi `TStrategy.CanAutoBuy`.
    pub fn can_auto_buy_like_delphi(&self) -> bool {
        (self.auto_buy_like_delphi() || self.kind_like_delphi() == StrategyKind::MOON_SHOT)
            && self.kind_like_delphi() != StrategyKind::MANUAL
    }

    /// Delphi `TStratForm.CheckActive` / `bStartCheckedClick` active assignment.
    pub fn active_like_delphi(&self, mode: StrategyActiveMode) -> bool {
        match mode {
            StrategyActiveMode::ActiveClient => {
                self.checked
                    && !self.can_auto_buy_like_delphi()
                    && !self.run_detect_on_kernel_like_delphi()
            }
            StrategyActiveMode::UsingMoonProto => {
                self.checked
                    && (self.can_auto_buy_like_delphi() || self.run_detect_on_kernel_like_delphi())
            }
            StrategyActiveMode::Standalone => self.checked,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct StrategyBatch {
    pub names: Vec<String>,
    pub paths: Vec<String>,
    pub strategies: Vec<StrategySnapshot>,
}

// =============================================================================
//  Парсер
// =============================================================================

/// Парсинг с DEFLATE-сжатого payload'а (как приходит в `TStratSnapshot.data`).
pub fn parse_strategy_batch(deflate_bytes: &[u8]) -> Option<StrategyBatch> {
    parse_strategy_batch_with_schema(deflate_bytes, None)
}

/// Парсинг payload'а с проверкой полей по live `TStratSchema`.
///
/// Если schema передана, reader повторяет Delphi `BuildReaderProps`/`ReadField`:
/// имя должно существовать в public `TStrategy` schema, а wire TypeID должен
/// совпасть с RTTI TypeID. Иначе поле пропускается через `SkipFieldByTypeID`.
/// Без schema функция остаётся generic reader'ом wire-format для диагностики.
pub fn parse_strategy_batch_with_schema(
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
    let mut decompressed = Vec::with_capacity(strategy_plain_capacity_hint(deflate_bytes.len()));
    decoder.read_to_end(&mut decompressed).ok()?;
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
    let mut decompressed = Vec::with_capacity(strategy_plain_capacity_hint(deflate_bytes.len()));
    decoder.read_to_end(&mut decompressed).ok()?;
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

/// Парсинг уже распакованного плоского payload'а (для случая если decompression сделан снаружи).
pub fn parse_strategy_batch_plain(data: &[u8]) -> Option<StrategyBatch> {
    parse_strategy_batch_plain_with_schema(data, None)
}

pub fn parse_strategy_batch_plain_with_schema(
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
    let paths = read_dict(data, &mut pos)?;
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
    let paths = read_dict(data, &mut pos)?;
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
    paths: &[String],
    reader_fields: Option<&[Option<u8>]>,
) -> Option<StrategySnapshot> {
    let strategy_id = read_u64(data, pos)?;
    let strategy_ver = read_i32(data, pos)?;
    let last_date = read_u64(data, pos)?;
    let checked = read_u8(data, pos)? != 0;
    let kind = read_u8(data, pos)?;
    let path_id = read_u16(data, pos)? as usize;
    let path = paths.get(path_id).cloned().unwrap_or_default();

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
            // Value bytes отсутствуют (Delphi: `If (TypeID and TID_ZERO_FLAG) <> 0 then exit`).
            FieldValue::zero(real_type)
        } else {
            try_read_field_value(data, pos, real_type)
        };

        if let Some(v) = value {
            if let Some(name) = field_names.get(field_idx) {
                fields.push_deserialized_field(Arc::clone(name), v);
            }
            // Иначе — поле известного типа, но имя не в словаре. Поведение Delphi:
            // ReaderProps[idx] = nil → SkipField; в данной точке мы УЖЕ прочитали значение,
            // так что просто игнорируем (позиция корректна).
        }
        // Если value=None и !is_zero — это случай unknown TypeID: `try_read_field_value`
        // выполнил fallback skip 8 байт (как Delphi `SkipFieldByTypeID` else branch pas:373).
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

/// Читает значение по `type_id`. Если type_id неизвестный — fallback skip 8 байт
/// (как `SkipFieldByTypeID` pas:373: `Stream.Position := Stream.Position + 8`).
fn try_read_field_value(data: &[u8], pos: &mut usize, type_id: u8) -> Option<FieldValue> {
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
            let mut bytes = vec![0u8; len];
            let available = data.len().saturating_sub(*pos).min(len);
            if available > 0 {
                bytes[..available].copy_from_slice(&data[*pos..*pos + available]);
                *pos += available;
            }
            let s = decode_utf8_delphi(&bytes);
            Some(FieldValue::String(s))
        }
        _ => {
            // Unknown — fallback skip 8 байт. Позиция сдвигается, но значение не возвращается.
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

// =============================================================================
//  Writer (для тестов и опционального клиентского `WriteStrategy`)
// =============================================================================

/// Builder для создания DEFLATE-compressed snapshot'а. Wire-format зеркало
/// `BeginWrite/WriteStrategy/FinalizeWrite`: dicts, headers, type IDs, zero flag,
/// raw-deflate и length truncation совпадают с Delphi.
///
/// Delphi writer идёт по RTTI `TStrategy` + `GetStrategyPropMask`. Rust не
/// хранит статическую копию этих таблиц: для non-empty snapshot writer требует
/// live `TStratSchema`, полученную от сервера в Init. Schema даёт тот же порядок
/// public fields, TypeID, PropMask visibility и non-zero defaults.
#[derive(Debug)]
pub struct StrategyBatchBuilder<'a> {
    schema: &'a StrategySchema,
    name_dict: Vec<String>,
    name_idx: HashMap<String, u16>,
    path_dict: Vec<String>,
    path_idx: HashMap<String, u16>,
    body: Vec<u8>,
    count: u16,
}

impl<'a> StrategyBatchBuilder<'a> {
    pub fn new(schema: &'a StrategySchema) -> Self {
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

    /// Валидный serializer payload с нулём стратегий. Schema не нужна, потому
    /// что Delphi `FinalizeWrite` для empty batch пишет пустые dicts/body.
    pub fn empty_payload() -> Vec<u8> {
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

    /// Добавить одну стратегию.
    pub fn write_strategy(&mut self, s: &StrategySnapshot) {
        let path_id = self.path_index(&s.path);
        // Header
        self.body.extend_from_slice(&s.strategy_id.to_le_bytes());
        self.body.extend_from_slice(&s.strategy_ver.to_le_bytes());
        self.body.extend_from_slice(&s.last_date.to_le_bytes());
        self.body.push(s.checked as u8);
        self.body.push(s.kind);
        self.body.extend_from_slice(&path_id.to_le_bytes());

        // Сериализуем поля. Записываем количество (placeholder), потом обновим.
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

    /// Финализировать в DEFLATE-compressed payload (формат TStratSnapshot.data).
    pub fn finalize(self) -> Vec<u8> {
        finalize_strategy_batch(self.name_dict, self.path_dict, self.body, self.count)
    }
}

fn strategy_schema_field_should_write(field: &StrategySchemaField, value: &FieldValue) -> bool {
    if !value.matches_type_id(field.raw_type_id) {
        return false;
    }
    if let Some(default_value) = &field.default_value {
        return !value.equals_delphi_value_for_type_id(default_value, field.raw_type_id);
    }
    !value.is_zero_for_type_id(field.raw_type_id)
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
        // PathLen/NameLen — byte (max 255). Для стратегий имена полей < 255 байт.
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

    // DEFLATE compress (raw, без zlib header — Delphi -15)
    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&plain).unwrap();
    encoder.finish().unwrap()
}

fn write_field(out: &mut Vec<u8>, v: &FieldValue) {
    let type_id = v.type_id();
    if v.is_zero() {
        // Записываем только TypeID с флагом ZERO; value bytes отсутствуют.
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

fn write_u8_len_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
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

// =============================================================================
//  Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::strategy_schema::{
        visible_kind_mask, StrategyFieldLayout, StrategyFieldType, StrategyFieldUiKind,
        StrategySchemaKind,
    };

    fn schema_field(
        name: &str,
        type_id: u8,
        default_value: Option<FieldValue>,
        visible_kind_ordinals: &[u8],
    ) -> StrategySchemaField {
        StrategySchemaField {
            name: name.to_string(),
            raw_type_id: type_id,
            type_id: StrategyFieldType::from_type_id(type_id),
            raw_flags: 0,
            ui_kind: StrategyFieldUiKind::Edit,
            layout: StrategyFieldLayout::None,
            default_value,
            visible_kind_ordinals: visible_kind_ordinals.to_vec(),
            visible_kind_mask: visible_kind_mask(visible_kind_ordinals),
            static_picklist_raw: None,
            static_picklist: Vec::new(),
            dynamic_picklist: None,
        }
    }

    fn schema_for_fields(fields: Vec<StrategySchemaField>) -> StrategySchema {
        StrategySchema {
            format_version: 1,
            kinds: vec![
                StrategySchemaKind {
                    ordinal: 1,
                    name: "Kind1".to_string(),
                },
                StrategySchemaKind {
                    ordinal: 5,
                    name: "Kind5".to_string(),
                },
            ],
            fields,
        }
    }

    fn sample_schema() -> StrategySchema {
        schema_for_fields(vec![
            schema_field("StrategyName", TID_STRING, None, &[1, 5]),
            schema_field("Comment", TID_STRING, None, &[1, 5]),
            schema_field("AcceptCommands", TID_BOOL, None, &[1, 5]),
            schema_field("KeepAlert", TID_INT32, Some(FieldValue::Int32(60)), &[1, 5]),
            schema_field("OrderSize", TID_DOUBLE, None, &[1, 5]),
            schema_field(
                "UseStopLoss",
                TID_BOOL,
                Some(FieldValue::Bool(true)),
                &[1, 5],
            ),
            schema_field(
                "StopLoss",
                TID_DOUBLE,
                Some(FieldValue::Double(-5.0)),
                &[1, 5],
            ),
            schema_field(
                "PendingOrderSpread",
                TID_DOUBLE,
                Some(FieldValue::Double(0.1)),
                &[1, 5],
            ),
            schema_field("DebugLog", TID_BOOL, None, &[1, 5]),
            schema_field(
                "SellOrderColor",
                TID_STRING,
                Some(FieldValue::String("00FF00".to_string())),
                &[1, 5],
            ),
            schema_field(
                "SignalType",
                TID_STRING,
                Some(FieldValue::String("DropsDetection".to_string())),
                &[1, 5],
            ),
        ])
    }

    fn sample_strategy(id: u64, name: &str, path: &str) -> StrategySnapshot {
        let mut fields = StrategyFields::new();
        fields.insert("StrategyName", FieldValue::String(name.to_string()));
        fields.insert("OrderSize", FieldValue::Double(123.45));
        fields.insert("KeepAlert", FieldValue::Int32(61));
        fields.insert("AcceptCommands", FieldValue::Bool(true));
        fields.insert("Comment", FieldValue::String("Test strategy".to_string()));
        StrategySnapshot {
            strategy_id: id,
            strategy_ver: 1,
            last_date: 1737000000000, // 2026-01-16 UTC ms
            checked: true,
            kind: 5,
            path: path.to_string(),
            fields,
        }
    }

    fn strategy_with_fields(
        kind: StrategyKind,
        checked: bool,
        fields: &[(&str, FieldValue)],
    ) -> StrategySnapshot {
        StrategySnapshot {
            strategy_id: 1,
            strategy_ver: 1,
            last_date: 1,
            checked,
            kind: kind.0,
            path: String::new(),
            fields: fields
                .iter()
                .map(|(name, value)| (Arc::<str>::from(*name), value.clone()))
                .collect(),
        }
    }

    #[test]
    fn strategy_active_helpers_match_delphi_check_active_modes() {
        let listing = strategy_with_fields(StrategyKind::NEW_LISTING, true, &[]);
        assert!(listing.active_like_delphi(StrategyActiveMode::ActiveClient));
        assert!(!listing.active_like_delphi(StrategyActiveMode::UsingMoonProto));
        assert!(listing.active_like_delphi(StrategyActiveMode::Standalone));

        let moonshot = strategy_with_fields(StrategyKind::MOON_SHOT, true, &[]);
        assert!(
            moonshot.can_auto_buy_like_delphi(),
            "Delphi CanAutoBuy is true for MoonShot even when AutoBuy=false"
        );
        assert!(!moonshot.active_like_delphi(StrategyActiveMode::ActiveClient));
        assert!(moonshot.active_like_delphi(StrategyActiveMode::UsingMoonProto));

        let remote_kernel = strategy_with_fields(
            StrategyKind::NEW_LISTING,
            true,
            &[("RunDetectOnKernel", FieldValue::Bool(true))],
        );
        assert!(!remote_kernel.active_like_delphi(StrategyActiveMode::ActiveClient));
        assert!(remote_kernel.active_like_delphi(StrategyActiveMode::UsingMoonProto));
    }

    #[test]
    fn empty_batch_roundtrip() {
        let compressed = StrategyBatchBuilder::empty_payload();
        let parsed = parse_strategy_batch(&compressed).unwrap();
        assert!(parsed.names.is_empty());
        assert!(parsed.paths.is_empty());
        assert!(parsed.strategies.is_empty());
    }

    #[test]
    fn single_strategy_roundtrip() {
        let schema = sample_schema();
        let mut b = StrategyBatchBuilder::new(&schema);
        let s = sample_strategy(100, "Strat-1", "Folder/A");
        b.write_strategy(&s);
        let compressed = b.finalize();

        let parsed = parse_strategy_batch(&compressed).unwrap();
        assert_eq!(parsed.strategies.len(), 1);
        let ps = &parsed.strategies[0];
        assert_eq!(ps.strategy_id, 100);
        assert_eq!(ps.strategy_ver, 1);
        assert!(ps.checked);
        assert_eq!(ps.kind, 5);
        assert_eq!(ps.path, "Folder/A");
        assert_eq!(
            ps.fields.get("StrategyName"),
            Some(&FieldValue::String("Strat-1".to_string()))
        );
        assert_eq!(
            ps.fields.get("OrderSize"),
            Some(&FieldValue::Double(123.45))
        );
        assert_eq!(ps.fields.get("KeepAlert"), Some(&FieldValue::Int32(61)));
        assert_eq!(
            ps.fields.get("AcceptCommands"),
            Some(&FieldValue::Bool(true))
        );
    }

    #[test]
    fn writer_uses_schema_field_order_for_name_dict() {
        let mut fields = StrategyFields::new();
        fields.insert("OrderSize", FieldValue::Double(1.0));
        fields.insert("StrategyName", FieldValue::String("A".to_string()));
        fields.insert("UnknownZ", FieldValue::Byte(1));
        fields.insert("AcceptCommands", FieldValue::Bool(true));
        fields.insert("UnknownA", FieldValue::Byte(2));
        fields.insert("Comment", FieldValue::String("C".to_string()));

        let schema = sample_schema();
        let mut b = StrategyBatchBuilder::new(&schema);
        b.write_strategy(&StrategySnapshot {
            strategy_id: 1,
            strategy_ver: 1,
            last_date: 0,
            checked: true,
            kind: 1,
            path: String::new(),
            fields,
        });

        let parsed = parse_strategy_batch(&b.finalize()).unwrap();
        assert_eq!(
            parsed.names,
            vec![
                "StrategyName".to_string(),
                "Comment".to_string(),
                "AcceptCommands".to_string(),
                "OrderSize".to_string(),
            ]
        );
    }

    #[test]
    fn writer_skips_schema_defaults_unknown_fields_and_type_mismatches() {
        let mut fields = StrategyFields::new();
        fields.insert("StrategyName", FieldValue::String("Local".to_string()));
        fields.insert("KeepAlert", FieldValue::Int32(60));
        fields.insert("UseStopLoss", FieldValue::Bool(true));
        fields.insert("StopLoss", FieldValue::Double(-5.0));
        fields.insert("PendingOrderSpread", FieldValue::Double(0.1));
        fields.insert("DebugLog", FieldValue::Bool(false));
        fields.insert("UnknownA", FieldValue::Byte(7));
        fields.insert("OrderSize", FieldValue::String("wrong type".to_string()));
        fields.insert("SellOrderColor", FieldValue::String(String::new()));

        let schema = sample_schema();
        let mut b = StrategyBatchBuilder::new(&schema);
        b.write_strategy(&StrategySnapshot {
            strategy_id: 1,
            strategy_ver: 1,
            last_date: 0,
            checked: true,
            kind: 1,
            path: String::new(),
            fields,
        });

        let parsed = parse_strategy_batch(&b.finalize()).unwrap();
        assert_eq!(
            parsed.names,
            vec!["StrategyName".to_string(), "SellOrderColor".to_string()]
        );
        let ps = &parsed.strategies[0];
        assert_eq!(
            ps.fields.get("StrategyName"),
            Some(&FieldValue::String("Local".to_string()))
        );
        assert_eq!(
            ps.fields.get("SellOrderColor"),
            Some(&FieldValue::String(String::new()))
        );
        assert!(!ps.fields.contains_key("KeepAlert"));
        assert!(!ps.fields.contains_key("UseStopLoss"));
        assert!(!ps.fields.contains_key("StopLoss"));
        assert!(!ps.fields.contains_key("PendingOrderSpread"));
        assert!(!ps.fields.contains_key("DebugLog"));
        assert!(!ps.fields.contains_key("UnknownA"));
        assert!(!ps.fields.contains_key("OrderSize"));
    }

    #[test]
    fn multiple_strategies_share_name_dict() {
        let schema = sample_schema();
        let mut b = StrategyBatchBuilder::new(&schema);
        b.write_strategy(&sample_strategy(1, "A", "Folder/X"));
        b.write_strategy(&sample_strategy(2, "B", "Folder/X")); // same path
        b.write_strategy(&sample_strategy(3, "C", "Folder/Y")); // new path
        let compressed = b.finalize();

        let parsed = parse_strategy_batch(&compressed).unwrap();
        assert_eq!(parsed.strategies.len(), 3);
        // Имена уникальны: StrategyName, OrderSize, KeepAlert, AcceptCommands, Comment — 5 имён.
        assert_eq!(parsed.names.len(), 5);
        // Пути уникальны: 2 штуки.
        assert_eq!(parsed.paths.len(), 2);
    }

    #[test]
    fn zero_flag_encoded_for_zero_values() {
        let mut fields = StrategyFields::new();
        fields.insert("KeepAlert", FieldValue::Int32(0));
        fields.insert("UseStopLoss", FieldValue::Bool(false));
        fields.insert("SignalType", FieldValue::String(String::new()));
        fields.insert("DebugLog", FieldValue::Bool(false));

        let s = StrategySnapshot {
            strategy_id: 1,
            strategy_ver: 1,
            last_date: 0,
            checked: false,
            kind: 1,
            path: String::new(),
            fields,
        };

        let schema = sample_schema();
        let mut b = StrategyBatchBuilder::new(&schema);
        b.write_strategy(&s);
        let compressed = b.finalize();

        let parsed = parse_strategy_batch(&compressed).unwrap();
        let ps = &parsed.strategies[0];
        assert_eq!(ps.fields.get("KeepAlert"), Some(&FieldValue::Int32(0)));
        assert_eq!(ps.fields.get("UseStopLoss"), Some(&FieldValue::Bool(false)));
        assert_eq!(
            ps.fields.get("SignalType"),
            Some(&FieldValue::String(String::new()))
        );
        assert!(!ps.fields.contains_key("DebugLog"));
    }

    #[test]
    fn all_primitive_types_roundtrip() {
        let values = [
            FieldValue::Bool(true),
            FieldValue::Byte(200),
            FieldValue::Word(60000),
            FieldValue::Int32(-12345),
            FieldValue::UInt32(3_000_000_000),
            FieldValue::Int64(-9_876_543_210),
            FieldValue::UInt64(12_345_678_901_234),
            FieldValue::Single(3.125),
            FieldValue::Double(2.75),
            FieldValue::String("Hello 世界 🚀".to_string()),
        ];

        for value in values {
            let mut bytes = Vec::new();
            write_field(&mut bytes, &value);
            let mut pos = 0usize;
            let type_id = read_u8(&bytes, &mut pos).unwrap();
            assert_eq!(type_id & 0x7F, value.type_id());
            let parsed = if (type_id & TID_ZERO_FLAG) != 0 {
                FieldValue::zero(type_id).unwrap()
            } else {
                try_read_field_value(&bytes, &mut pos, type_id).unwrap()
            };
            assert_eq!(parsed, value);
            assert_eq!(pos, bytes.len());
        }
    }

    #[test]
    fn writer_wraps_name_path_and_string_lengths_like_delphi() {
        let long_name = "N".repeat(257);
        let long_path = "P".repeat(257);
        let long_value = "V".repeat(65_537);

        let mut name_bytes = Vec::new();
        write_u8_len_bytes(&mut name_bytes, long_name.as_bytes());
        assert_eq!(name_bytes, vec![1, b'N']);

        let mut fields = StrategyFields::new();
        fields.insert("Comment", FieldValue::String(long_value));

        let s = StrategySnapshot {
            strategy_id: 1000,
            strategy_ver: 1,
            last_date: 1737000000000,
            checked: true,
            kind: 1,
            path: long_path,
            fields,
        };

        let schema = sample_schema();
        let mut b = StrategyBatchBuilder::new(&schema);
        b.write_strategy(&s);
        let compressed = b.finalize();
        let parsed = parse_strategy_batch(&compressed).unwrap();
        let ps = &parsed.strategies[0];

        assert_eq!(ps.path, "P");
        assert_eq!(
            ps.fields.get("Comment"),
            Some(&FieldValue::String("V".to_string()))
        );
    }

    #[test]
    fn missing_path_id_yields_empty() {
        // Конструируем raw plain payload где PathID=99 при пустом PathDict.
        let mut plain = Vec::new();
        // NameDict: 1 name "X"
        plain.extend_from_slice(&1u16.to_le_bytes());
        plain.push(1);
        plain.push(b'X');
        // PathDict: empty
        plain.extend_from_slice(&0u16.to_le_bytes());
        // StratCount: 1
        plain.extend_from_slice(&1u16.to_le_bytes());
        // Strategy
        plain.extend_from_slice(&42u64.to_le_bytes()); // id
        plain.extend_from_slice(&1i32.to_le_bytes()); // ver
        plain.extend_from_slice(&0u64.to_le_bytes()); // last_date
        plain.push(0); // checked
        plain.push(0); // kind
        plain.extend_from_slice(&99u16.to_le_bytes()); // path_id (OOR)
        plain.extend_from_slice(&0u16.to_le_bytes()); // field count

        let parsed = parse_strategy_batch_plain(&plain).unwrap();
        assert_eq!(parsed.strategies.len(), 1);
        assert_eq!(parsed.strategies[0].path, ""); // PathID out of range → empty
    }

    #[test]
    fn unknown_type_id_skipped_8_bytes() {
        // FieldIdx=0, TypeID=99 (неизвестный) → reader должен пропустить 8 байт.
        // После этого должен корректно прочитать следующее поле.
        let mut plain = Vec::new();
        // NameDict: 2 names
        plain.extend_from_slice(&2u16.to_le_bytes());
        plain.push(1);
        plain.push(b'A');
        plain.push(1);
        plain.push(b'B');
        // PathDict
        plain.extend_from_slice(&0u16.to_le_bytes());
        // StratCount
        plain.extend_from_slice(&1u16.to_le_bytes());
        // Strategy header
        plain.extend_from_slice(&1u64.to_le_bytes());
        plain.extend_from_slice(&1i32.to_le_bytes());
        plain.extend_from_slice(&0u64.to_le_bytes());
        plain.push(0);
        plain.push(0);
        plain.extend_from_slice(&0u16.to_le_bytes());
        // FieldCount=2
        plain.extend_from_slice(&2u16.to_le_bytes());
        // Field 0: idx=0, typeID=99 (unknown), 8 bytes value (всё нули)
        plain.extend_from_slice(&0u16.to_le_bytes());
        plain.push(99);
        plain.extend_from_slice(&[0u8; 8]);
        // Field 1: idx=1, typeID=TID_INT32, value=42
        plain.extend_from_slice(&1u16.to_le_bytes());
        plain.push(TID_INT32);
        plain.extend_from_slice(&42i32.to_le_bytes());

        let parsed = parse_strategy_batch_plain(&plain).unwrap();
        let ps = &parsed.strategies[0];
        // Field A не разобран (unknown TypeID).
        assert_eq!(ps.fields.get("A"), None);
        // Field B разобран как Int32=42.
        assert_eq!(ps.fields.get("B"), Some(&FieldValue::Int32(42)));
    }

    #[test]
    fn known_field_type_mismatch_is_skipped_like_delphi_read_field() {
        let mut plain = Vec::new();
        // NameDict: OrderSize expects TID_DOUBLE, Comment expects TID_STRING.
        plain.extend_from_slice(&2u16.to_le_bytes());
        plain.push(9);
        plain.extend_from_slice(b"OrderSize");
        plain.push(7);
        plain.extend_from_slice(b"Comment");
        // PathDict
        plain.extend_from_slice(&0u16.to_le_bytes());
        // StratCount
        plain.extend_from_slice(&1u16.to_le_bytes());
        // Strategy header
        plain.extend_from_slice(&1u64.to_le_bytes());
        plain.extend_from_slice(&1i32.to_le_bytes());
        plain.extend_from_slice(&0u64.to_le_bytes());
        plain.push(0);
        plain.push(0);
        plain.extend_from_slice(&0u16.to_le_bytes());
        // FieldCount=2
        plain.extend_from_slice(&2u16.to_le_bytes());
        // Field 0: OrderSize but wire type is String; Delphi skips it.
        plain.extend_from_slice(&0u16.to_le_bytes());
        plain.push(TID_STRING);
        plain.extend_from_slice(&3u16.to_le_bytes());
        plain.extend_from_slice(b"bad");
        // Field 1: Comment, correct string, proves skip consumed exact bytes.
        plain.extend_from_slice(&1u16.to_le_bytes());
        plain.push(TID_STRING);
        plain.extend_from_slice(&2u16.to_le_bytes());
        plain.extend_from_slice(b"ok");

        let schema = schema_for_fields(vec![
            schema_field("OrderSize", TID_DOUBLE, None, &[0]),
            schema_field("Comment", TID_STRING, None, &[0]),
        ]);
        let parsed = parse_strategy_batch_plain_with_schema(&plain, Some(&schema)).unwrap();
        let ps = &parsed.strategies[0];
        assert!(!ps.fields.contains_key("OrderSize"));
        assert_eq!(
            ps.fields.get("Comment"),
            Some(&FieldValue::String("ok".to_string()))
        );
    }

    #[test]
    fn string_field_value_zero_fills_short_body_like_delphi_read_field() {
        let mut plain = Vec::new();
        // NameDict: one string field.
        plain.extend_from_slice(&1u16.to_le_bytes());
        plain.push(7);
        plain.extend_from_slice(b"Comment");
        // PathDict
        plain.extend_from_slice(&0u16.to_le_bytes());
        // StratCount
        plain.extend_from_slice(&1u16.to_le_bytes());
        // Strategy header
        plain.extend_from_slice(&1u64.to_le_bytes());
        plain.extend_from_slice(&1i32.to_le_bytes());
        plain.extend_from_slice(&0u64.to_le_bytes());
        plain.push(0);
        plain.push(0);
        plain.extend_from_slice(&0u16.to_le_bytes());
        // FieldCount=1
        plain.extend_from_slice(&1u16.to_le_bytes());
        // Field 0: declared string Len=3, but only one body byte is present.
        plain.extend_from_slice(&0u16.to_le_bytes());
        plain.push(TID_STRING);
        plain.extend_from_slice(&3u16.to_le_bytes());
        plain.push(b'a');

        let parsed = parse_strategy_batch_plain(&plain).unwrap();
        let ps = &parsed.strategies[0];
        assert_eq!(
            ps.fields.get("Comment"),
            Some(&FieldValue::String("a\0\0".to_string()))
        );
    }

    #[test]
    fn scalar_field_value_zero_fills_short_body_like_delphi_stream_read() {
        let mut plain = Vec::new();
        // NameDict: one Int32 field.
        plain.extend_from_slice(&1u16.to_le_bytes());
        plain.push(9);
        plain.extend_from_slice(b"KeepAlert");
        // PathDict
        plain.extend_from_slice(&0u16.to_le_bytes());
        // StratCount
        plain.extend_from_slice(&1u16.to_le_bytes());
        // Strategy header
        plain.extend_from_slice(&1u64.to_le_bytes());
        plain.extend_from_slice(&1i32.to_le_bytes());
        plain.extend_from_slice(&0u64.to_le_bytes());
        plain.push(0);
        plain.push(0);
        plain.extend_from_slice(&0u16.to_le_bytes());
        // FieldCount=1
        plain.extend_from_slice(&1u16.to_le_bytes());
        // Field 0: declared Int32, but only low two bytes are present.
        plain.extend_from_slice(&0u16.to_le_bytes());
        plain.push(TID_INT32);
        plain.extend_from_slice(&0x1234u16.to_le_bytes());

        let parsed = parse_strategy_batch_plain(&plain).unwrap();
        let ps = &parsed.strategies[0];
        assert_eq!(ps.fields.get("KeepAlert"), Some(&FieldValue::Int32(0x1234)));
    }

    #[test]
    fn known_field_type_mismatch_fixed_skip_consumes_short_tail_like_delphi() {
        let mut plain = Vec::new();
        // NameDict: OrderSize expects TID_DOUBLE.
        plain.extend_from_slice(&1u16.to_le_bytes());
        plain.push(9);
        plain.extend_from_slice(b"OrderSize");
        // PathDict
        plain.extend_from_slice(&0u16.to_le_bytes());
        // StratCount
        plain.extend_from_slice(&1u16.to_le_bytes());
        // Strategy header
        plain.extend_from_slice(&1u64.to_le_bytes());
        plain.extend_from_slice(&1i32.to_le_bytes());
        plain.extend_from_slice(&0u64.to_le_bytes());
        plain.push(0);
        plain.push(0);
        plain.extend_from_slice(&0u16.to_le_bytes());
        // FieldCount=1
        plain.extend_from_slice(&1u16.to_le_bytes());
        // Field 0: OrderSize but wire type is Int64; only one byte of the
        // skipped fixed-size value is present.
        plain.extend_from_slice(&0u16.to_le_bytes());
        plain.push(TID_INT64);
        plain.push(0xAA);

        let schema = schema_for_fields(vec![schema_field("OrderSize", TID_DOUBLE, None, &[0])]);
        let parsed = parse_strategy_batch_plain_with_schema(&plain, Some(&schema)).unwrap();
        assert!(parsed.strategies[0].fields.is_empty());
    }

    #[test]
    fn known_field_type_mismatch_string_skip_consumes_short_body_like_delphi() {
        let mut plain = Vec::new();
        // NameDict: OrderSize expects TID_DOUBLE.
        plain.extend_from_slice(&1u16.to_le_bytes());
        plain.push(9);
        plain.extend_from_slice(b"OrderSize");
        // PathDict
        plain.extend_from_slice(&0u16.to_le_bytes());
        // StratCount
        plain.extend_from_slice(&1u16.to_le_bytes());
        // Strategy header
        plain.extend_from_slice(&1u64.to_le_bytes());
        plain.extend_from_slice(&1i32.to_le_bytes());
        plain.extend_from_slice(&0u64.to_le_bytes());
        plain.push(0);
        plain.push(0);
        plain.extend_from_slice(&0u16.to_le_bytes());
        // FieldCount=1
        plain.extend_from_slice(&1u16.to_le_bytes());
        // Field 0: OrderSize but wire type is String; Len is present, body is short.
        plain.extend_from_slice(&0u16.to_le_bytes());
        plain.push(TID_STRING);
        plain.extend_from_slice(&5u16.to_le_bytes());
        plain.push(b'x');

        let schema = schema_for_fields(vec![schema_field("OrderSize", TID_DOUBLE, None, &[0])]);
        let parsed = parse_strategy_batch_plain_with_schema(&plain, Some(&schema)).unwrap();
        assert!(parsed.strategies[0].fields.is_empty());
    }

    #[test]
    fn invalid_utf8_dicts_and_string_fields_use_delphi_question_mark_fallback() {
        let mut plain = Vec::new();
        // NameDict: one invalid UTF-8 field name "N?me".
        plain.extend_from_slice(&1u16.to_le_bytes());
        plain.push(4);
        plain.extend_from_slice(&[b'N', 0xFF, b'm', b'e']);
        // PathDict: one invalid UTF-8 path "P?".
        plain.extend_from_slice(&1u16.to_le_bytes());
        plain.push(2);
        plain.extend_from_slice(&[b'P', 0x80]);
        // StratCount
        plain.extend_from_slice(&1u16.to_le_bytes());
        // Strategy header
        plain.extend_from_slice(&1u64.to_le_bytes());
        plain.extend_from_slice(&1i32.to_le_bytes());
        plain.extend_from_slice(&0u64.to_le_bytes());
        plain.push(0);
        plain.push(0);
        plain.extend_from_slice(&0u16.to_le_bytes());
        // FieldCount=1, field value "V?"
        plain.extend_from_slice(&1u16.to_le_bytes());
        plain.extend_from_slice(&0u16.to_le_bytes());
        plain.push(TID_STRING);
        plain.extend_from_slice(&2u16.to_le_bytes());
        plain.extend_from_slice(&[b'V', 0xFF]);

        let parsed = parse_strategy_batch_plain(&plain).unwrap();
        assert_eq!(parsed.names, vec!["N?me".to_string()]);
        assert_eq!(parsed.paths, vec!["P?".to_string()]);
        let ps = &parsed.strategies[0];
        assert_eq!(ps.path, "P?");
        assert_eq!(
            ps.fields.get("N?me"),
            Some(&FieldValue::String("V?".to_string()))
        );
    }

    #[test]
    fn truncated_payload_returns_none() {
        let mut plain = Vec::new();
        // Только частичный NameDict header (нет данных)
        plain.extend_from_slice(&100u16.to_le_bytes()); // обещано 100 имён
                                                        // Но больше нет данных → должен вернуть None
        let parsed = parse_strategy_batch_plain(&plain);
        assert!(parsed.is_none());
    }

    #[test]
    fn field_value_type_id_match() {
        assert_eq!(FieldValue::Bool(true).type_id(), TID_BOOL);
        assert_eq!(FieldValue::Byte(0).type_id(), TID_BYTE);
        assert_eq!(FieldValue::Word(0).type_id(), TID_WORD);
        assert_eq!(FieldValue::Int32(0).type_id(), TID_INT32);
        assert_eq!(FieldValue::UInt32(0).type_id(), TID_UINT32);
        assert_eq!(FieldValue::Int64(0).type_id(), TID_INT64);
        assert_eq!(FieldValue::UInt64(0).type_id(), TID_UINT64);
        assert_eq!(FieldValue::Single(0.0).type_id(), TID_SINGLE);
        assert_eq!(FieldValue::Double(0.0).type_id(), TID_DOUBLE);
        assert_eq!(FieldValue::String(String::new()).type_id(), TID_STRING);
    }

    #[test]
    fn field_value_zero_for_each_type() {
        assert_eq!(FieldValue::zero(TID_BOOL), Some(FieldValue::Bool(false)));
        assert_eq!(FieldValue::zero(TID_INT32), Some(FieldValue::Int32(0)));
        assert_eq!(
            FieldValue::zero(TID_STRING),
            Some(FieldValue::String(String::new()))
        );
        assert_eq!(FieldValue::zero(TID_DOUBLE), Some(FieldValue::Double(0.0)));
        assert_eq!(FieldValue::zero(99), None);
    }

    #[test]
    fn is_zero_for_each_type() {
        assert!(FieldValue::Bool(false).is_zero());
        assert!(!FieldValue::Bool(true).is_zero());
        assert!(FieldValue::Int32(0).is_zero());
        assert!(!FieldValue::Int32(1).is_zero());
        assert!(FieldValue::String(String::new()).is_zero());
        assert!(!FieldValue::String("x".to_string()).is_zero());
        assert!(FieldValue::Double(0.0).is_zero());
        assert!(FieldValue::Double(1e-15).is_zero()); // < 1e-10
        assert!(!FieldValue::Double(1e-5).is_zero());
    }
}
