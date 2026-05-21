//! `TStrategySerializer` reader/writer — byte-exact port.
//!
//! Источник Delphi: `MoonProto/StrategySerializer.pas` (~1118 строк).
//!
//! ## Назначение
//! Парсит RTTI-driven binary snapshot стратегий из payload'а `TStratSnapshot.data`.
//! Сервер (Delphi MoonBot) использует RTTI для итерации по public-полям `TStrategy`;
//! Rust-клиент не имеет RTTI, поэтому хранит поля как `HashMap<FieldName, FieldValue>`.
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
}

// =============================================================================
//  StrategySnapshot
// =============================================================================

/// Распакованный snapshot одной стратегии. Поля хранятся в HashMap по имени —
/// потребитель использует `FieldValue::*` extractors для строгой типизации.
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
    pub fields: HashMap<String, FieldValue>,
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
    let mut decoder = DeflateDecoder::new(deflate_bytes);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed).ok()?;
    parse_strategy_batch_plain(&decompressed)
}

/// Парсинг уже распакованного плоского payload'а (для случая если decompression сделан снаружи).
pub fn parse_strategy_batch_plain(data: &[u8]) -> Option<StrategyBatch> {
    let mut pos = 0usize;
    let names = read_dict(data, &mut pos)?;
    let paths = read_dict(data, &mut pos)?;
    let strat_count = read_u16(data, &mut pos)? as usize;
    let mut strategies = Vec::with_capacity(strat_count);
    for _ in 0..strat_count {
        strategies.push(read_strategy(data, &mut pos, &names, &paths)?);
    }
    Some(StrategyBatch {
        names,
        paths,
        strategies,
    })
}

fn read_dict(data: &[u8], pos: &mut usize) -> Option<Vec<String>> {
    let count = read_u16(data, pos)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let len = read_u8(data, pos)? as usize;
        if *pos + len > data.len() {
            return None;
        }
        let s = String::from_utf8_lossy(&data[*pos..*pos + len]).to_string();
        *pos += len;
        out.push(s);
    }
    Some(out)
}

fn read_strategy(
    data: &[u8],
    pos: &mut usize,
    names: &[String],
    paths: &[String],
) -> Option<StrategySnapshot> {
    let strategy_id = read_u64(data, pos)?;
    let strategy_ver = read_i32(data, pos)?;
    let last_date = read_u64(data, pos)?;
    let checked = read_u8(data, pos)? != 0;
    let kind = read_u8(data, pos)?;
    let path_id = read_u16(data, pos)? as usize;
    let path = paths.get(path_id).cloned().unwrap_or_default();

    let field_count = read_u16(data, pos)? as usize;
    let mut fields = HashMap::with_capacity(field_count);

    for _ in 0..field_count {
        let field_idx = read_u16(data, pos)? as usize;
        let type_id = read_u8(data, pos)?;
        let is_zero = (type_id & TID_ZERO_FLAG) != 0;
        let real_type = type_id & 0x7F;

        let value: Option<FieldValue> = if is_zero {
            // Value bytes отсутствуют (Delphi: `If (TypeID and TID_ZERO_FLAG) <> 0 then exit`).
            FieldValue::zero(real_type)
        } else {
            try_read_field_value(data, pos, real_type)
        };

        if let Some(v) = value {
            if let Some(name) = names.get(field_idx) {
                fields.insert(name.clone(), v);
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
        TID_BOOL => Some(FieldValue::Bool(read_u8(data, pos)? != 0)),
        TID_BYTE => Some(FieldValue::Byte(read_u8(data, pos)?)),
        TID_WORD => Some(FieldValue::Word(read_u16(data, pos)?)),
        TID_INT32 => Some(FieldValue::Int32(read_i32(data, pos)?)),
        TID_UINT32 => Some(FieldValue::UInt32(read_u32(data, pos)?)),
        TID_INT64 => Some(FieldValue::Int64(read_i64(data, pos)?)),
        TID_UINT64 => Some(FieldValue::UInt64(read_u64(data, pos)?)),
        TID_SINGLE => Some(FieldValue::Single(read_f32(data, pos)?)),
        TID_DOUBLE => Some(FieldValue::Double(read_f64(data, pos)?)),
        TID_STRING => {
            let len = read_u16(data, pos)? as usize;
            if *pos + len > data.len() {
                return None;
            }
            let s = String::from_utf8_lossy(&data[*pos..*pos + len]).to_string();
            *pos += len;
            Some(FieldValue::String(s))
        }
        _ => {
            // Unknown — fallback skip 8 байт. Позиция сдвигается, но значение не возвращается.
            *pos = (*pos + 8).min(data.len());
            None
        }
    }
}

// --- Primitive readers ---
fn read_u8(d: &[u8], p: &mut usize) -> Option<u8> {
    if *p + 1 > d.len() {
        return None;
    }
    let v = d[*p];
    *p += 1;
    Some(v)
}
fn read_u16(d: &[u8], p: &mut usize) -> Option<u16> {
    if *p + 2 > d.len() {
        return None;
    }
    let v = u16::from_le_bytes(d[*p..*p + 2].try_into().unwrap());
    *p += 2;
    Some(v)
}
fn read_i32(d: &[u8], p: &mut usize) -> Option<i32> {
    if *p + 4 > d.len() {
        return None;
    }
    let v = i32::from_le_bytes(d[*p..*p + 4].try_into().unwrap());
    *p += 4;
    Some(v)
}
fn read_u32(d: &[u8], p: &mut usize) -> Option<u32> {
    if *p + 4 > d.len() {
        return None;
    }
    let v = u32::from_le_bytes(d[*p..*p + 4].try_into().unwrap());
    *p += 4;
    Some(v)
}
fn read_i64(d: &[u8], p: &mut usize) -> Option<i64> {
    if *p + 8 > d.len() {
        return None;
    }
    let v = i64::from_le_bytes(d[*p..*p + 8].try_into().unwrap());
    *p += 8;
    Some(v)
}
fn read_u64(d: &[u8], p: &mut usize) -> Option<u64> {
    if *p + 8 > d.len() {
        return None;
    }
    let v = u64::from_le_bytes(d[*p..*p + 8].try_into().unwrap());
    *p += 8;
    Some(v)
}
fn read_f32(d: &[u8], p: &mut usize) -> Option<f32> {
    if *p + 4 > d.len() {
        return None;
    }
    let v = f32::from_le_bytes(d[*p..*p + 4].try_into().unwrap());
    *p += 4;
    Some(v)
}
fn read_f64(d: &[u8], p: &mut usize) -> Option<f64> {
    if *p + 8 > d.len() {
        return None;
    }
    let v = f64::from_le_bytes(d[*p..*p + 8].try_into().unwrap());
    *p += 8;
    Some(v)
}

// =============================================================================
//  Writer (для тестов и опционального клиентского `WriteStrategy`)
// =============================================================================

/// Builder для создания DEFLATE-compressed snapshot'а. Зеркало `BeginWrite/WriteStrategy/FinalizeWrite`.
/// Не используется в продакшене клиента (клиент только парсит); существует для round-trip тестов.
#[derive(Debug, Default)]
pub struct StrategyBatchBuilder {
    name_dict: Vec<String>,
    name_idx: HashMap<String, u16>,
    path_dict: Vec<String>,
    path_idx: HashMap<String, u16>,
    body: Vec<u8>,
    count: u16,
}

impl StrategyBatchBuilder {
    pub fn new() -> Self {
        Self::default()
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

        // Стабильный порядок сериализации полей (детерминированный для тестов).
        let mut entries: Vec<_> = s.fields.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));

        for (name, value) in entries {
            let idx = self.name_index(name);
            self.body.extend_from_slice(&idx.to_le_bytes());
            write_field(&mut self.body, value);
            field_count += 1;
        }
        // Backfill count
        self.body[count_offset..count_offset + 2].copy_from_slice(&field_count.to_le_bytes());
        self.count += 1;
    }

    /// Финализировать в DEFLATE-compressed payload (формат TStratSnapshot.data).
    pub fn finalize(self) -> Vec<u8> {
        let mut plain = Vec::with_capacity(self.body.len() + 64);

        // NameDict
        plain.extend_from_slice(&(self.name_dict.len() as u16).to_le_bytes());
        for n in &self.name_dict {
            let b = n.as_bytes();
            // PathLen/NameLen — byte (max 255). Для стратегий имена полей < 255 байт.
            plain.push(b.len() as u8);
            plain.extend_from_slice(b);
        }
        // PathDict
        plain.extend_from_slice(&(self.path_dict.len() as u16).to_le_bytes());
        for p in &self.path_dict {
            let b = p.as_bytes();
            plain.push(b.len() as u8);
            plain.extend_from_slice(b);
        }
        // StratCount + body
        plain.extend_from_slice(&self.count.to_le_bytes());
        plain.extend_from_slice(&self.body);

        // DEFLATE compress (raw, без zlib header — Delphi -15)
        let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&plain).unwrap();
        encoder.finish().unwrap()
    }
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
            out.extend_from_slice(&(b.len() as u16).to_le_bytes());
            out.extend_from_slice(b);
        }
    }
}

// =============================================================================
//  Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_strategy(id: u64, name: &str, path: &str) -> StrategySnapshot {
        let mut fields = HashMap::new();
        fields.insert(
            "StrategyName".to_string(),
            FieldValue::String(name.to_string()),
        );
        fields.insert("OrderSize".to_string(), FieldValue::Double(123.45));
        fields.insert("KeepAlert".to_string(), FieldValue::Int32(60));
        fields.insert("AcceptCommands".to_string(), FieldValue::Bool(true));
        fields.insert(
            "Comment".to_string(),
            FieldValue::String("Test strategy".to_string()),
        );
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

    #[test]
    fn empty_batch_roundtrip() {
        let builder = StrategyBatchBuilder::new();
        let compressed = builder.finalize();
        let parsed = parse_strategy_batch(&compressed).unwrap();
        assert!(parsed.names.is_empty());
        assert!(parsed.paths.is_empty());
        assert!(parsed.strategies.is_empty());
    }

    #[test]
    fn single_strategy_roundtrip() {
        let mut b = StrategyBatchBuilder::new();
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
        assert_eq!(ps.fields.get("KeepAlert"), Some(&FieldValue::Int32(60)));
        assert_eq!(
            ps.fields.get("AcceptCommands"),
            Some(&FieldValue::Bool(true))
        );
    }

    #[test]
    fn multiple_strategies_share_name_dict() {
        let mut b = StrategyBatchBuilder::new();
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
        let mut fields = HashMap::new();
        fields.insert("ZeroInt".to_string(), FieldValue::Int32(0));
        fields.insert("ZeroBool".to_string(), FieldValue::Bool(false));
        fields.insert("ZeroStr".to_string(), FieldValue::String(String::new()));
        fields.insert("NonZeroInt".to_string(), FieldValue::Int32(42));

        let s = StrategySnapshot {
            strategy_id: 1,
            strategy_ver: 1,
            last_date: 0,
            checked: false,
            kind: 0,
            path: String::new(),
            fields,
        };

        let mut b = StrategyBatchBuilder::new();
        b.write_strategy(&s);
        let compressed = b.finalize();

        let parsed = parse_strategy_batch(&compressed).unwrap();
        let ps = &parsed.strategies[0];
        assert_eq!(ps.fields.get("ZeroInt"), Some(&FieldValue::Int32(0)));
        assert_eq!(ps.fields.get("ZeroBool"), Some(&FieldValue::Bool(false)));
        assert_eq!(
            ps.fields.get("ZeroStr"),
            Some(&FieldValue::String(String::new()))
        );
        assert_eq!(ps.fields.get("NonZeroInt"), Some(&FieldValue::Int32(42)));
    }

    #[test]
    fn all_primitive_types_roundtrip() {
        let mut fields = HashMap::new();
        fields.insert("F_Bool".to_string(), FieldValue::Bool(true));
        fields.insert("F_Byte".to_string(), FieldValue::Byte(200));
        fields.insert("F_Word".to_string(), FieldValue::Word(60000));
        fields.insert("F_Int32".to_string(), FieldValue::Int32(-12345));
        fields.insert("F_UInt32".to_string(), FieldValue::UInt32(3_000_000_000));
        fields.insert("F_Int64".to_string(), FieldValue::Int64(-9_876_543_210));
        fields.insert(
            "F_UInt64".to_string(),
            FieldValue::UInt64(12_345_678_901_234),
        );
        fields.insert("F_Single".to_string(), FieldValue::Single(3.125));
        fields.insert("F_Double".to_string(), FieldValue::Double(2.75));
        fields.insert(
            "F_String".to_string(),
            FieldValue::String("Hello 世界 🚀".to_string()),
        );

        let s = StrategySnapshot {
            strategy_id: 999,
            strategy_ver: 7,
            last_date: 1737000000000,
            checked: true,
            kind: 1,
            path: "P".to_string(),
            fields: fields.clone(),
        };

        let mut b = StrategyBatchBuilder::new();
        b.write_strategy(&s);
        let compressed = b.finalize();
        let parsed = parse_strategy_batch(&compressed).unwrap();
        let ps = &parsed.strategies[0];

        for (k, v) in &fields {
            assert_eq!(ps.fields.get(k), Some(v), "mismatch on {}", k);
        }
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
