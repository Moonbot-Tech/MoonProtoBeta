//! Schema-aware strategy editor API.
//!
//! `StrategySnapshot` is the retained/wire shape: it preserves the exact field
//! list used by `TStrategySerializer`. UI and tests should not hand-write that
//! list for common edits. `StrategyEditor` validates every changed field against
//! the live `TStratSchema`, and typed wrappers such as `MoonShotStrategy` expose
//! the small Delphi-like property surface user code actually wants.

use std::error::Error;
use std::fmt;

use super::{field_names, FieldValue, StrategyFields, StrategyKind, StrategySnapshot};
use crate::commands::strategy_schema::{StrategyFieldType, StrategySchema, StrategySchemaField};
use crate::MoonTime;

pub const FIELD_SIGNAL_TYPE: &str = "SignalType";
pub const FIELD_EMULATOR_MODE: &str = "EmulatorMode";
pub const FIELD_IGNORE_FILTERS: &str = "IgnoreFilters";
pub const FIELD_COINS_WHITE_LIST: &str = "CoinsWhiteList";
pub const FIELD_COINS_BLACK_LIST: &str = "CoinsBlackList";
pub const FIELD_ORDER_SIZE: &str = "OrderSize";
pub const FIELD_MSHOT_PRICE_MIN: &str = "MShotPriceMin";
pub const FIELD_MSHOT_PRICE: &str = "MShotPrice";

#[derive(Debug, Clone, PartialEq)]
pub enum StrategyEditError {
    UnknownStrategyKind {
        kind: StrategyKind,
    },
    WrongStrategyKind {
        expected: StrategyKind,
        actual: StrategyKind,
    },
    MissingField {
        field: String,
    },
    HiddenField {
        field: String,
        kind: StrategyKind,
    },
    TypeMismatch {
        field: String,
        expected: StrategyFieldType,
        value: &'static str,
    },
    NumberOutOfRange {
        field: String,
        expected: StrategyFieldType,
        value: f64,
    },
}

impl fmt::Display for StrategyEditError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownStrategyKind { kind } => {
                write!(
                    f,
                    "strategy kind {} is absent from live schema",
                    kind.ordinal()
                )
            }
            Self::WrongStrategyKind { expected, actual } => write!(
                f,
                "wrong strategy kind: expected {}, got {}",
                expected.ordinal(),
                actual.ordinal()
            ),
            Self::MissingField { field } => write!(f, "strategy field `{field}` is absent"),
            Self::HiddenField { field, kind } => write!(
                f,
                "strategy field `{field}` is not visible for kind {}",
                kind.ordinal()
            ),
            Self::TypeMismatch {
                field,
                expected,
                value,
            } => write!(
                f,
                "strategy field `{field}` expects {}, got {value}",
                expected.name()
            ),
            Self::NumberOutOfRange {
                field,
                expected,
                value,
            } => write!(
                f,
                "strategy field `{field}` value {value} cannot be represented as {}",
                expected.name()
            ),
        }
    }
}

impl Error for StrategyEditError {}

/// Generic schema-aware editor for any strategy kind.
///
/// It keeps an owned `StrategySnapshot`, validates every edited field against
/// the live schema, and preserves untouched/future fields from snapshots loaded
/// from the server.
#[derive(Debug, Clone)]
pub struct StrategyEditor<'a> {
    schema: &'a StrategySchema,
    snapshot: StrategySnapshot,
}

impl<'a> StrategyEditor<'a> {
    pub fn new<P>(
        schema: &'a StrategySchema,
        strategy_id: u64,
        kind: StrategyKind,
        path: P,
    ) -> Result<Self, StrategyEditError>
    where
        P: Into<std::sync::Arc<str>>,
    {
        ensure_kind(schema, kind)?;
        Ok(Self {
            schema,
            snapshot: StrategySnapshot::new_at(
                strategy_id,
                1,
                MoonTime::now(),
                false,
                kind,
                path,
                StrategyFields::new(),
            ),
        })
    }

    pub fn from_snapshot(
        schema: &'a StrategySchema,
        snapshot: &StrategySnapshot,
    ) -> Result<Self, StrategyEditError> {
        ensure_kind(schema, snapshot.kind())?;
        Ok(Self {
            schema,
            snapshot: snapshot.clone(),
        })
    }

    pub fn snapshot(&self) -> &StrategySnapshot {
        &self.snapshot
    }

    pub fn snapshot_mut(&mut self) -> &mut StrategySnapshot {
        &mut self.snapshot
    }

    pub fn into_snapshot(self) -> StrategySnapshot {
        self.snapshot
    }

    pub fn strategy_id(&self) -> u64 {
        self.snapshot.strategy_id
    }

    pub fn strategy_ver(&self) -> i32 {
        self.snapshot.strategy_ver
    }

    pub fn kind(&self) -> StrategyKind {
        self.snapshot.kind()
    }

    pub fn checked(&self) -> bool {
        self.snapshot.checked
    }

    pub fn set_checked(&mut self, checked: bool) {
        self.snapshot.checked = checked;
    }

    pub fn path(&self) -> &str {
        self.snapshot.path.as_ref()
    }

    pub fn set_path<P>(&mut self, path: P)
    where
        P: Into<std::sync::Arc<str>>,
    {
        self.snapshot.path = path.into();
    }

    pub fn touch(&mut self, time: MoonTime) {
        self.snapshot.strategy_ver = self.snapshot.strategy_ver.saturating_add(1);
        self.snapshot.last_date = super::types::moon_time_to_strategy_last_date(time);
    }

    pub fn touch_now(&mut self) {
        self.touch(MoonTime::now());
    }

    pub fn bool(&self, field: &str) -> Result<bool, StrategyEditError> {
        self.value(field).map(|value| match value {
            FieldValue::Bool(v) => v,
            _ => false,
        })
    }

    pub fn number(&self, field: &str) -> Result<f64, StrategyEditError> {
        self.value(field).and_then(|value| match value {
            FieldValue::Double(v) => Ok(v),
            FieldValue::Single(v) => Ok(f64::from(v)),
            FieldValue::Int32(v) => Ok(f64::from(v)),
            FieldValue::Int64(v) => Ok(v as f64),
            FieldValue::Byte(v) => Ok(f64::from(v)),
            FieldValue::Word(v) => Ok(f64::from(v)),
            FieldValue::UInt32(v) => Ok(f64::from(v)),
            FieldValue::UInt64(v) => Ok(v as f64),
            _ => Err(StrategyEditError::TypeMismatch {
                field: field.to_string(),
                expected: self.field(field)?.type_id,
                value: value.kind_name(),
            }),
        })
    }

    pub fn string(&self, field: &str) -> Result<String, StrategyEditError> {
        self.value(field).and_then(|value| match value {
            FieldValue::String(v) => Ok(v),
            _ => Err(StrategyEditError::TypeMismatch {
                field: field.to_string(),
                expected: self.field(field)?.type_id,
                value: value.kind_name(),
            }),
        })
    }

    pub fn set_bool(&mut self, field: &str, value: bool) -> Result<(), StrategyEditError> {
        self.set_value(field, FieldValue::Bool(value))
    }

    pub fn set_number(&mut self, field: &str, value: f64) -> Result<(), StrategyEditError> {
        let schema_field = self.field(field)?;
        let value = number_value(field, schema_field.type_id, value)?;
        self.set_value(field, value)
    }

    pub fn set_string(
        &mut self,
        field: &str,
        value: impl Into<String>,
    ) -> Result<(), StrategyEditError> {
        self.set_value(field, FieldValue::String(value.into()))
    }

    pub fn set_value(&mut self, field: &str, value: FieldValue) -> Result<(), StrategyEditError> {
        let schema_field = self.visible_field(field)?;
        if !value.matches_type_id_inner(schema_field.raw_type_id) {
            return Err(StrategyEditError::TypeMismatch {
                field: field.to_string(),
                expected: schema_field.type_id,
                value: value.kind_name(),
            });
        }
        self.snapshot
            .fields
            .insert(schema_field.name.as_str(), value);
        Ok(())
    }

    pub fn set_value_if_visible(
        &mut self,
        field: &str,
        value: FieldValue,
    ) -> Result<bool, StrategyEditError> {
        let Some(schema_field) = self.schema.field(field) else {
            return Ok(false);
        };
        if !schema_field.visible_for_strategy_kind(self.kind()) {
            return Ok(false);
        }
        if !value.matches_type_id_inner(schema_field.raw_type_id) {
            return Err(StrategyEditError::TypeMismatch {
                field: field.to_string(),
                expected: schema_field.type_id,
                value: value.kind_name(),
            });
        }
        self.snapshot
            .fields
            .insert(schema_field.name.as_str(), value);
        Ok(true)
    }

    fn value(&self, field: &str) -> Result<FieldValue, StrategyEditError> {
        let schema_field = self.visible_field(field)?;
        Ok(self
            .snapshot
            .fields
            .get(field)
            .cloned()
            .or_else(|| schema_field.default_value.clone())
            .or_else(|| FieldValue::zero_for_type_id(schema_field.raw_type_id))
            .unwrap_or_else(|| FieldValue::String(String::new())))
    }

    fn field(&self, field: &str) -> Result<&'a StrategySchemaField, StrategyEditError> {
        self.schema
            .field(field)
            .ok_or_else(|| StrategyEditError::MissingField {
                field: field.to_string(),
            })
    }

    fn visible_field(&self, field: &str) -> Result<&'a StrategySchemaField, StrategyEditError> {
        let schema_field = self.field(field)?;
        if !schema_field.visible_for_strategy_kind(self.kind()) {
            return Err(StrategyEditError::HiddenField {
                field: field.to_string(),
                kind: self.kind(),
            });
        }
        Ok(schema_field)
    }
}

/// Typed editable view for MoonShot strategies.
///
/// Public fields intentionally mirror the way terminal code thinks about one
/// strategy. `into_snapshot` writes them back through `StrategyEditor`, so the
/// wire payload still follows live schema order and type checks.
#[derive(Debug, Clone)]
pub struct MoonShotStrategy {
    base: StrategySnapshot,
    pub name: String,
    pub path: String,
    pub checked: bool,
    pub auto_buy: bool,
    pub emulator_mode: bool,
    pub ignore_filters: bool,
    pub mshot_price_min: f64,
    pub mshot_price: f64,
    pub order_size: f64,
    pub coins_white_list: String,
    pub coins_black_list: String,
}

impl MoonShotStrategy {
    pub fn new(strategy_id: u64) -> Self {
        Self {
            base: StrategySnapshot::new_at(
                strategy_id,
                1,
                MoonTime::now(),
                false,
                StrategyKind::MOON_SHOT,
                "",
                StrategyFields::new(),
            ),
            name: String::new(),
            path: String::new(),
            checked: false,
            auto_buy: true,
            emulator_mode: false,
            ignore_filters: false,
            mshot_price_min: 2.0,
            mshot_price: 7.0,
            order_size: 0.0,
            coins_white_list: String::new(),
            coins_black_list: String::new(),
        }
    }

    pub fn from_snapshot(
        schema: &StrategySchema,
        snapshot: &StrategySnapshot,
    ) -> Result<Self, StrategyEditError> {
        if snapshot.kind() != StrategyKind::MOON_SHOT {
            return Err(StrategyEditError::WrongStrategyKind {
                expected: StrategyKind::MOON_SHOT,
                actual: snapshot.kind(),
            });
        }
        let editor = StrategyEditor::from_snapshot(schema, snapshot)?;
        Ok(Self {
            base: snapshot.clone(),
            name: editor.string(field_names::STRATEGY_NAME)?,
            path: editor.path().to_string(),
            checked: editor.checked(),
            auto_buy: editor.bool(field_names::AUTO_BUY).unwrap_or(true),
            emulator_mode: editor.bool(FIELD_EMULATOR_MODE)?,
            ignore_filters: editor.bool(FIELD_IGNORE_FILTERS)?,
            mshot_price_min: editor.number(FIELD_MSHOT_PRICE_MIN)?,
            mshot_price: editor.number(FIELD_MSHOT_PRICE)?,
            order_size: editor.number(FIELD_ORDER_SIZE)?,
            coins_white_list: editor.string(FIELD_COINS_WHITE_LIST)?,
            coins_black_list: editor.string(FIELD_COINS_BLACK_LIST)?,
        })
    }

    pub fn strategy_id(&self) -> u64 {
        self.base.strategy_id
    }

    pub fn strategy_ver(&self) -> i32 {
        self.base.strategy_ver
    }

    pub fn last_edit_time(&self) -> MoonTime {
        self.base.last_edit_time()
    }

    pub fn into_snapshot(
        self,
        schema: &StrategySchema,
    ) -> Result<StrategySnapshot, StrategyEditError> {
        let mut editor = StrategyEditor::from_snapshot(schema, &self.base)?;
        editor.snapshot_mut().kind = StrategyKind::MOON_SHOT.to_byte();
        editor.set_path(self.path);
        editor.set_checked(self.checked);
        editor.set_string(field_names::STRATEGY_NAME, self.name)?;
        editor.set_string(FIELD_SIGNAL_TYPE, "MoonShot")?;
        editor.set_value_if_visible(field_names::AUTO_BUY, FieldValue::Bool(self.auto_buy))?;
        editor.set_bool(FIELD_EMULATOR_MODE, self.emulator_mode)?;
        editor.set_bool(FIELD_IGNORE_FILTERS, self.ignore_filters)?;
        editor.set_number(FIELD_MSHOT_PRICE_MIN, self.mshot_price_min)?;
        editor.set_number(FIELD_MSHOT_PRICE, self.mshot_price)?;
        editor.set_number(FIELD_ORDER_SIZE, self.order_size)?;
        editor.set_string(FIELD_COINS_WHITE_LIST, self.coins_white_list)?;
        editor.set_string(FIELD_COINS_BLACK_LIST, self.coins_black_list)?;
        editor.touch_now();
        Ok(editor.into_snapshot())
    }
}

fn ensure_kind(schema: &StrategySchema, kind: StrategyKind) -> Result<(), StrategyEditError> {
    schema
        .kind_name_for_strategy_kind(kind)
        .map(|_| ())
        .ok_or(StrategyEditError::UnknownStrategyKind { kind })
}

fn number_value(
    field: &str,
    expected: StrategyFieldType,
    value: f64,
) -> Result<FieldValue, StrategyEditError> {
    if !value.is_finite() {
        return Err(StrategyEditError::NumberOutOfRange {
            field: field.to_string(),
            expected,
            value,
        });
    }
    Ok(match expected {
        StrategyFieldType::Double => FieldValue::Double(value),
        StrategyFieldType::Single => {
            if value < f32::MIN as f64 || value > f32::MAX as f64 {
                return Err(StrategyEditError::NumberOutOfRange {
                    field: field.to_string(),
                    expected,
                    value,
                });
            }
            FieldValue::Single(value as f32)
        }
        StrategyFieldType::Int32 => FieldValue::Int32(integer_value(field, expected, value)?),
        StrategyFieldType::Int64 => FieldValue::Int64(integer_value(field, expected, value)?),
        StrategyFieldType::Byte => FieldValue::Byte(integer_value(field, expected, value)?),
        StrategyFieldType::Word => FieldValue::Word(integer_value(field, expected, value)?),
        StrategyFieldType::UInt32 => FieldValue::UInt32(integer_value(field, expected, value)?),
        StrategyFieldType::UInt64 => FieldValue::UInt64(integer_value(field, expected, value)?),
        _ => {
            return Err(StrategyEditError::TypeMismatch {
                field: field.to_string(),
                expected,
                value: "number",
            })
        }
    })
}

fn integer_value<T>(
    field: &str,
    expected: StrategyFieldType,
    value: f64,
) -> Result<T, StrategyEditError>
where
    T: TryFrom<i128>,
{
    let rounded = value.round();
    if (value - rounded).abs() > f64::EPSILON {
        return Err(StrategyEditError::NumberOutOfRange {
            field: field.to_string(),
            expected,
            value,
        });
    }
    let int_value = rounded as i128;
    T::try_from(int_value).map_err(|_| StrategyEditError::NumberOutOfRange {
        field: field.to_string(),
        expected,
        value,
    })
}

trait FieldValueKindName {
    fn kind_name(&self) -> &'static str;
}

impl FieldValueKindName for FieldValue {
    fn kind_name(&self) -> &'static str {
        match self {
            FieldValue::Bool(_) => "Bool",
            FieldValue::Int32(_) => "Int32",
            FieldValue::Int64(_) => "Int64",
            FieldValue::Double(_) => "Double",
            FieldValue::String(_) => "String",
            FieldValue::Byte(_) => "Byte",
            FieldValue::Word(_) => "Word",
            FieldValue::UInt32(_) => "UInt32",
            FieldValue::UInt64(_) => "UInt64",
            FieldValue::Single(_) => "Single",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::strategy_schema::{
        StrategyFieldLayout, StrategyFieldType, StrategyFieldUiKind, StrategySchemaKind,
    };

    fn schema_field(name: &str, type_id: u8, visible: &[StrategyKind]) -> StrategySchemaField {
        let visible_kind_ordinals: Vec<_> = visible.iter().map(|kind| kind.ordinal()).collect();
        StrategySchemaField {
            name: name.to_string(),
            raw_type_id: type_id,
            type_id: StrategyFieldType::from_type_id(type_id),
            raw_flags: 0,
            ui_kind: StrategyFieldUiKind::Edit,
            layout: StrategyFieldLayout::None,
            default_value: None,
            visible_kind_mask: crate::commands::strategy_schema::visible_kind_mask(
                &visible_kind_ordinals,
            ),
            visible_kind_ordinals,
            static_picklist_raw: None,
            static_picklist: Vec::new(),
            dynamic_picklist: None,
        }
    }

    fn moonshot_schema() -> StrategySchema {
        let moon = StrategyKind::MOON_SHOT;
        let telegram = StrategyKind::TELEGRAM;
        StrategySchema {
            format_version: 1,
            kinds: vec![
                StrategySchemaKind {
                    ordinal: moon.ordinal(),
                    name: "MoonShot".to_string(),
                },
                StrategySchemaKind {
                    ordinal: telegram.ordinal(),
                    name: "Telegram".to_string(),
                },
            ],
            fields: vec![
                schema_field(
                    field_names::STRATEGY_NAME,
                    super::super::TID_STRING,
                    &[moon],
                ),
                schema_field(FIELD_SIGNAL_TYPE, super::super::TID_STRING, &[moon]),
                schema_field(field_names::AUTO_BUY, super::super::TID_BOOL, &[moon]),
                schema_field(FIELD_EMULATOR_MODE, super::super::TID_BOOL, &[moon]),
                schema_field(FIELD_IGNORE_FILTERS, super::super::TID_BOOL, &[moon]),
                schema_field(FIELD_COINS_WHITE_LIST, super::super::TID_STRING, &[moon]),
                schema_field(FIELD_COINS_BLACK_LIST, super::super::TID_STRING, &[moon]),
                schema_field(FIELD_ORDER_SIZE, super::super::TID_DOUBLE, &[moon]),
                schema_field(FIELD_MSHOT_PRICE_MIN, super::super::TID_DOUBLE, &[moon]),
                schema_field(FIELD_MSHOT_PRICE, super::super::TID_DOUBLE, &[moon]),
                schema_field("TelegramOnly", super::super::TID_BOOL, &[telegram]),
            ],
        }
    }

    #[test]
    fn moonshot_strategy_roundtrip_keeps_unknown_fields() {
        let schema = moonshot_schema();
        let mut fields = StrategyFields::new();
        fields.insert(
            "FutureServerField",
            FieldValue::String("keep-me".to_string()),
        );
        fields.insert(FIELD_ORDER_SIZE, FieldValue::Double(100.0));
        fields.insert(FIELD_IGNORE_FILTERS, FieldValue::Bool(true));
        fields.insert(
            field_names::STRATEGY_NAME,
            FieldValue::String("Old".to_string()),
        );

        let original = StrategySnapshot::new(
            77,
            4,
            1_700_000_000_000,
            false,
            StrategyKind::MOON_SHOT,
            "OldFolder",
            fields,
        );
        let mut shot = MoonShotStrategy::from_snapshot(&schema, &original).unwrap();
        assert!(shot.ignore_filters);
        shot.name = "New".to_string();
        shot.path = "FireTest".to_string();
        shot.checked = true;
        shot.order_size = 250.0;
        shot.coins_white_list = "ETH".to_string();
        shot.coins_black_list.clear();

        let updated = shot.into_snapshot(&schema).unwrap();
        assert_eq!(updated.strategy_id, 77);
        assert!(updated.checked);
        assert_eq!(updated.path.as_ref(), "FireTest");
        assert_eq!(
            updated.fields.get("FutureServerField"),
            Some(&FieldValue::String("keep-me".to_string()))
        );
        assert_eq!(
            updated.fields.get(field_names::STRATEGY_NAME),
            Some(&FieldValue::String("New".to_string()))
        );
        assert_eq!(
            updated.fields.get(FIELD_ORDER_SIZE),
            Some(&FieldValue::Double(250.0))
        );
        assert!(updated.strategy_ver > original.strategy_ver);
        assert!(updated.last_date >= original.last_date);
    }

    #[test]
    fn generic_editor_rejects_hidden_field() {
        let schema = moonshot_schema();
        let mut editor =
            StrategyEditor::new(&schema, 1, StrategyKind::MOON_SHOT, "FireTest").unwrap();

        let err = editor.set_bool("TelegramOnly", true).unwrap_err();
        assert!(matches!(err, StrategyEditError::HiddenField { .. }));
    }

    #[test]
    fn generic_editor_rejects_wrong_type() {
        let schema = moonshot_schema();
        let mut editor =
            StrategyEditor::new(&schema, 1, StrategyKind::MOON_SHOT, "FireTest").unwrap();

        let err = editor
            .set_value(FIELD_ORDER_SIZE, FieldValue::String("bad".to_string()))
            .unwrap_err();
        assert!(matches!(err, StrategyEditError::TypeMismatch { .. }));
    }
}
