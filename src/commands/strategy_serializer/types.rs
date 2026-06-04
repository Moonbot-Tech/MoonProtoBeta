use std::sync::Arc;

use super::{
    TID_BOOL, TID_BYTE, TID_DOUBLE, TID_INT32, TID_INT64, TID_SINGLE, TID_STRING, TID_UINT32,
    TID_UINT64, TID_WORD,
};
use crate::MoonTime;

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

/// Decoded strategy field value, equivalent to Delphi `TValue` after RTTI deserialization.
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
    /// Zero value for one TypeID, used when `TID_ZERO_FLAG` is set.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn zero(type_id: u8) -> Option<Self> {
        Self::zero_for_type_id(type_id)
    }

    pub(crate) fn zero_for_type_id(type_id: u8) -> Option<Self> {
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

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn type_id(&self) -> u8 {
        self.type_id_inner()
    }

    pub(crate) fn type_id_inner(&self) -> u8 {
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

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn matches_type_id(&self, type_id: u8) -> bool {
        self.matches_type_id_inner(type_id)
    }

    pub(crate) fn matches_type_id_inner(&self, type_id: u8) -> bool {
        self.type_id_inner() == (type_id & 0x7F)
    }

    /// True when this value is equivalent to zero for its own TypeID.
    /// Matches `IsZeroValue` in `StrategySerializer.pas`.
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

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn is_zero_for_type_id(&self, type_id: u8) -> bool {
        self.is_zero_for_type_id_inner(type_id)
    }

    pub(crate) fn is_zero_for_type_id_inner(&self, type_id: u8) -> bool {
        self.matches_type_id_inner(type_id) && self.is_zero()
    }

    /// Compare like Delphi `IsDefaultValue`: floats use `1e-10`, other types
    /// compare exactly, and both sides must match the given TypeID.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn equals_delphi_value_for_type_id(&self, other: &Self, type_id: u8) -> bool {
        self.equals_delphi_value_for_type_id_inner(other, type_id)
    }

    pub(crate) fn equals_delphi_value_for_type_id_inner(&self, other: &Self, type_id: u8) -> bool {
        if !self.matches_type_id_inner(type_id) || !other.matches_type_id_inner(type_id) {
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

/// Decoded snapshot of one strategy.
///
/// Fields are stored by Delphi field name. Consumers can use `FieldValue::*`
/// extractors, typed getters on `StrategyFields`, or higher-level convenience
/// methods on `StrategySnapshot`.
#[derive(Debug, Clone)]
pub struct StrategySnapshot {
    pub strategy_id: u64,
    pub strategy_ver: i32,
    /// Unix epoch milliseconds used by Delphi `FLastEditDate`/rollback guards.
    ///
    /// UI code should use [`Self::last_edit_time`] for display and
    /// [`Self::new_at`] when creating snapshots from a typed timestamp. The raw
    /// integer stays public because local strategy sync must preserve the exact
    /// monotonic value sent to the server.
    pub last_date: u64,
    pub checked: bool,
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub kind: u8,
    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) kind: u8,
    /// Folder path from `PathDict` by `PathID`; empty when `PathID` is out of range.
    ///
    /// `Arc<str>`: many strategies share the same folder path, so the reader
    /// hands out a refcount bump per strategy instead of a fresh heap copy —
    /// matching Delphi's copy-on-write string assignment.
    pub path: Arc<str>,
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
pub struct StrategyKind(u8);

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

    /// Build from the Delphi `TStrategyKind` ordinal exposed by
    /// [`StrategySchemaKind`](crate::StrategySchemaKind).
    pub const fn from_ordinal(value: u8) -> Self {
        Self(value)
    }

    /// Delphi `TStrategyKind` ordinal.
    pub const fn ordinal(self) -> u8 {
        self.0
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub const fn from_byte(value: u8) -> Self {
        Self(value)
    }

    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) const fn from_byte(value: u8) -> Self {
        Self(value)
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub const fn to_byte(self) -> u8 {
        self.0
    }

    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) const fn to_byte(self) -> u8 {
        self.0
    }
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
    /// Build one local strategy snapshot for [`InitialStrategies`](crate::InitialStrategies)
    /// or [`MoonStrategies::sync_local_strategies`](crate::MoonStrategies::sync_local_strategies).
    ///
    /// `kind` is typed so terminal code does not pass raw Delphi ordinals.
    pub fn new<P>(
        strategy_id: u64,
        strategy_ver: i32,
        last_date: u64,
        checked: bool,
        kind: StrategyKind,
        path: P,
        fields: StrategyFields,
    ) -> Self
    where
        P: Into<Arc<str>>,
    {
        Self {
            strategy_id,
            strategy_ver,
            last_date,
            checked,
            kind: kind.to_byte(),
            path: path.into(),
            fields,
        }
    }

    /// Build one local strategy snapshot from a typed UI timestamp.
    ///
    /// The serializer still stores `last_date` as Unix milliseconds because
    /// Delphi uses that integer for stale-snapshot guards; this constructor
    /// keeps application code on the normal MoonProto time type.
    pub fn new_at<P>(
        strategy_id: u64,
        strategy_ver: i32,
        last_edit_time: MoonTime,
        checked: bool,
        kind: StrategyKind,
        path: P,
        fields: StrategyFields,
    ) -> Self
    where
        P: Into<Arc<str>>,
    {
        Self::new(
            strategy_id,
            strategy_ver,
            moon_time_to_strategy_last_date(last_edit_time),
            checked,
            kind,
            path,
            fields,
        )
    }

    /// Last edit timestamp as the normal public MoonProto time type.
    pub fn last_edit_time(&self) -> MoonTime {
        strategy_last_date_to_moon_time(self.last_date)
    }

    // parity: MoonBot Strategies.pas:TStrategy.StrategyKind
    pub fn kind(&self) -> StrategyKind {
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

    // parity: MoonBot Strategies.pas:TStrategy.AutoBuy
    pub fn auto_buy(&self) -> bool {
        self.field_bool_or_false(field_names::AUTO_BUY)
    }

    // parity: MoonBot Strategies.pas:TStrategy.RunDetectOnKernel
    pub fn run_detect_on_kernel(&self) -> bool {
        self.field_bool_or_false(field_names::RUN_DETECT_ON_KERNEL)
    }

    // parity: MoonBot Strategies.pas:TStrategy.Short
    pub fn is_short(&self) -> bool {
        self.field_bool_or_false(field_names::SHORT)
    }

    // parity: MoonBot Strategies.pas:TStrategy.SellFromAsset
    pub fn sell_from_asset(&self) -> bool {
        self.field_bool_or_false(field_names::SELL_FROM_ASSET)
    }

    // parity: MoonBot Strategies.pas:TStrategy.CanAutoBuy
    pub fn can_auto_buy(&self) -> bool {
        (self.auto_buy() || self.kind() == StrategyKind::MOON_SHOT)
            && self.kind() != StrategyKind::MANUAL
    }

    // parity: MoonBot Strategies.pas:TStratForm.CheckActive (active assignment)
    pub fn is_active(&self, mode: StrategyActiveMode) -> bool {
        match mode {
            StrategyActiveMode::ActiveClient => {
                self.checked && !self.can_auto_buy() && !self.run_detect_on_kernel()
            }
            StrategyActiveMode::UsingMoonProto => {
                self.checked && (self.can_auto_buy() || self.run_detect_on_kernel())
            }
            StrategyActiveMode::Standalone => self.checked,
        }
    }
}

pub(crate) fn strategy_last_date_to_moon_time(last_date: u64) -> MoonTime {
    MoonTime::from_unix_millis(last_date.min(i64::MAX as u64) as i64)
}

pub(crate) fn moon_time_to_strategy_last_date(time: MoonTime) -> u64 {
    time.unix_millis().max(0) as u64
}
