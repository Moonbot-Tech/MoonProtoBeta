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

#[cfg(test)]
use super::strategy_schema::{StrategySchema, StrategySchemaField};
#[cfg(test)]
use std::sync::Arc;

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

mod reader;
mod types;
mod writer;

#[cfg(test)]
pub(crate) use self::reader::try_read_field_value;
pub use self::reader::{
    parse_strategy_batch, parse_strategy_batch_plain, parse_strategy_batch_plain_with_schema,
    parse_strategy_batch_with_schema,
};
pub(crate) use self::reader::{
    parse_strategy_batch_for_each_with_schema_field_types,
    parse_strategy_batch_with_schema_field_types,
};
pub use self::types::{
    FieldValue, StrategyActiveMode, StrategyFields, StrategyKind, StrategySnapshot,
};
pub use self::writer::StrategyBatchBuilder;
#[cfg(test)]
pub(crate) use self::writer::{write_field, write_u8_len_bytes};
#[cfg(test)]
pub(crate) use super::strict_read::read_u8;

#[derive(Debug, Clone, Default)]
pub struct StrategyBatch {
    pub names: Vec<String>,
    pub paths: Vec<String>,
    pub strategies: Vec<StrategySnapshot>,
}

// =============================================================================
//  Tests
// =============================================================================

#[cfg(test)]
mod tests;
