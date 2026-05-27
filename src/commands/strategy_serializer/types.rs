use std::sync::Arc;

use super::{
    TID_BOOL, TID_BYTE, TID_DOUBLE, TID_INT32, TID_INT64, TID_SINGLE, TID_STRING, TID_UINT32,
    TID_UINT64, TID_WORD,
};

/// Common Delphi `TStrategy` field names.
///
/// The snapshot remains schema-driven, so this list is intentionally small:
/// it covers fields the Active Lib itself reads and fields most UI code needs.
pub mod field_names {
    pub const STRATEGY_NAME: &str = "StrategyName";
    pub const SELL_PRICE: &str = "SellPrice";
    pub const AUTO_BUY: &str = "AutoBuy";
    pub const RUN_DETECT_ON_KERNEL: &str = "RunDetectOnKernel";
    pub const SHORT: &str = "Short";
    pub const SELL_FROM_ASSET: &str = "SellFromAsset";
}

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

/// Распакованный snapshot одной стратегии. Поля хранятся в `StrategyFields` по
/// имени; потребитель использует `FieldValue::*` extractors для строгой
/// типизации.
#[derive(Debug, Clone)]
pub struct StrategySnapshot {
    pub strategy_id: u64,
    pub strategy_ver: i32,
    /// Unix epoch ms (TDateTime -> UnixTimeToDelphi на стороне сервера, см. pas:671).
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
    pub(super) fn push_deserialized_field(&mut self, key: Arc<str>, value: FieldValue) {
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

    pub fn get_bool(&self, key: &str) -> Option<bool> {
        match self.get(key) {
            Some(FieldValue::Bool(value)) => Some(*value),
            _ => None,
        }
    }

    pub fn get_double(&self, key: &str) -> Option<f64> {
        match self.get(key) {
            Some(FieldValue::Double(value)) => Some(*value),
            _ => None,
        }
    }

    pub fn get_string(&self, key: &str) -> Option<&str> {
        match self.get(key) {
            Some(FieldValue::String(value)) => Some(value),
            _ => None,
        }
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
    pub fn kind(&self) -> StrategyKind {
        self.kind_like_delphi()
    }

    pub fn kind_like_delphi(&self) -> StrategyKind {
        StrategyKind(self.kind)
    }

    pub fn field_bool_or_false(&self, name: &str) -> bool {
        self.fields.get_bool(name).unwrap_or(false)
    }

    pub fn strategy_name(&self) -> Option<&str> {
        self.fields.get_string(field_names::STRATEGY_NAME)
    }

    pub fn sell_price_field(&self) -> Option<f64> {
        self.fields.get_double(field_names::SELL_PRICE)
    }

    pub fn auto_buy(&self) -> bool {
        self.auto_buy_like_delphi()
    }

    pub fn auto_buy_like_delphi(&self) -> bool {
        self.field_bool_or_false(field_names::AUTO_BUY)
    }

    pub fn run_detect_on_kernel(&self) -> bool {
        self.run_detect_on_kernel_like_delphi()
    }

    pub fn run_detect_on_kernel_like_delphi(&self) -> bool {
        self.field_bool_or_false(field_names::RUN_DETECT_ON_KERNEL)
    }

    pub fn is_short(&self) -> bool {
        self.short_like_delphi()
    }

    pub fn short_like_delphi(&self) -> bool {
        self.field_bool_or_false(field_names::SHORT)
    }

    pub fn sell_from_asset(&self) -> bool {
        self.sell_from_asset_like_delphi()
    }

    pub fn sell_from_asset_like_delphi(&self) -> bool {
        self.field_bool_or_false(field_names::SELL_FROM_ASSET)
    }

    pub fn can_auto_buy(&self) -> bool {
        self.can_auto_buy_like_delphi()
    }

    /// Delphi `TStrategy.CanAutoBuy`.
    pub fn can_auto_buy_like_delphi(&self) -> bool {
        (self.auto_buy_like_delphi() || self.kind_like_delphi() == StrategyKind::MOON_SHOT)
            && self.kind_like_delphi() != StrategyKind::MANUAL
    }

    pub fn is_active(&self, mode: StrategyActiveMode) -> bool {
        self.active_like_delphi(mode)
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
