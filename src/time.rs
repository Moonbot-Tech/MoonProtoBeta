//! Time helpers used by MoonProto public API and wire adapters.
//!
//! Public API uses [`MoonTime`], a compact Unix-milliseconds timestamp. The
//! MoonBot wire-day value is a wire-format detail: packets are converted at the
//! boundary so retained histories and UI-facing rows do not carry
//! protocol-native floating-point time.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(any(test, feature = "diagnostics"))]
pub(crate) const SECONDS_PER_DAY: f64 = 86_400.0;
pub(crate) const MILLISECONDS_PER_DAY: f64 = 86_400_000.0;
pub(crate) const MILLIS_PER_SECOND: i64 = 1_000;
pub(crate) const MILLIS_PER_MINUTE: i64 = 60 * MILLIS_PER_SECOND;
pub(crate) const MILLIS_PER_HOUR: i64 = 60 * MILLIS_PER_MINUTE;
pub(crate) const UNIX_EPOCH_AS_DELPHI_DAYS: f64 = 25_569.0;

/// Public MoonProto timestamp.
///
/// Stored as Unix milliseconds so hot retained-history rows stay 8 bytes wide
/// and range scans use integer comparisons. Conversion to [`SystemTime`] is
/// available for UI/framework integration, but the hot path does not call
/// `SystemTime::now()` per row.
#[repr(transparent)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MoonTime(i64);

impl MoonTime {
    pub const MIN: Self = Self(i64::MIN);
    pub const MAX: Self = Self(i64::MAX);
    pub const ZERO: Self = Self(0);

    #[inline]
    pub const fn from_unix_millis(millis: i64) -> Self {
        Self(millis)
    }

    #[inline]
    pub fn from_unix_seconds(seconds: f64) -> Option<Self> {
        let millis = (seconds * 1000.0).round();
        finite_f64_to_i64(millis).map(Self)
    }

    #[inline]
    pub fn from_system_time(time: SystemTime) -> Option<Self> {
        match time.duration_since(UNIX_EPOCH) {
            Ok(delta) => {
                let millis = delta.as_millis();
                (millis <= i64::MAX as u128).then_some(Self(millis as i64))
            }
            Err(err) => {
                let millis = err.duration().as_millis();
                (millis <= i64::MAX as u128).then_some(Self(-(millis as i64)))
            }
        }
    }

    #[inline]
    pub fn now() -> Self {
        Self::from_system_time(SystemTime::now()).unwrap_or(Self::ZERO)
    }

    #[inline]
    pub const fn unix_millis(self) -> i64 {
        self.0
    }

    #[inline]
    pub fn unix_seconds(self) -> f64 {
        self.0 as f64 / 1000.0
    }

    pub fn system_time(self) -> Option<SystemTime> {
        let duration = Duration::from_millis(self.0.unsigned_abs());
        if self.0 >= 0 {
            UNIX_EPOCH.checked_add(duration)
        } else {
            UNIX_EPOCH.checked_sub(duration)
        }
    }

    #[inline]
    pub(crate) fn from_delphi_days(days: f64) -> Option<Self> {
        let millis = ((days - UNIX_EPOCH_AS_DELPHI_DAYS) * MILLISECONDS_PER_DAY).round();
        finite_f64_to_i64(millis).map(Self)
    }

    #[inline]
    pub(crate) fn to_delphi_days(self) -> f64 {
        self.0 as f64 / MILLISECONDS_PER_DAY + UNIX_EPOCH_AS_DELPHI_DAYS
    }
}

impl From<i64> for MoonTime {
    #[inline]
    fn from(value: i64) -> Self {
        Self::from_unix_millis(value)
    }
}

impl From<MoonTime> for i64 {
    #[inline]
    fn from(value: MoonTime) -> Self {
        value.unix_millis()
    }
}

fn finite_f64_to_i64(value: f64) -> Option<i64> {
    (value.is_finite() && value >= i64::MIN as f64 && value <= i64::MAX as f64)
        .then_some(value as i64)
}

/// MoonBot wire time value: days since `1899-12-30`.
///
/// This type exists only for protocol diagnostics and tests. Normal builds keep
/// raw wire-day values as a wire detail and expose [`MoonTime`] to applications.
#[cfg(any(test, feature = "diagnostics"))]
#[repr(transparent)]
#[derive(Debug, Default, Clone, Copy, PartialEq, PartialOrd)]
pub struct DelphiTime(f64);

#[cfg(any(test, feature = "diagnostics"))]
impl DelphiTime {
    pub const ZERO: Self = Self(0.0);

    #[inline]
    pub const fn from_days(days: f64) -> Self {
        Self(days)
    }

    #[inline]
    pub fn from_unix_seconds(seconds: f64) -> Self {
        Self(seconds / SECONDS_PER_DAY + UNIX_EPOCH_AS_DELPHI_DAYS)
    }

    #[inline]
    pub fn from_unix_millis(millis: i64) -> Self {
        Self(millis as f64 / MILLISECONDS_PER_DAY + UNIX_EPOCH_AS_DELPHI_DAYS)
    }

    #[inline]
    pub fn from_system_time(time: SystemTime) -> Self {
        match time.duration_since(UNIX_EPOCH) {
            Ok(delta) => Self::from_unix_seconds(delta.as_secs_f64()),
            Err(err) => Self::from_unix_seconds(-err.duration().as_secs_f64()),
        }
    }

    #[inline]
    pub fn now() -> Self {
        Self::from_system_time(SystemTime::now())
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn to_moon_time(self) -> Option<MoonTime> {
        MoonTime::from_delphi_days(self.0)
    }

    #[inline]
    pub const fn as_days(self) -> f64 {
        self.0
    }

    #[inline]
    pub fn unix_seconds(self) -> Option<f64> {
        let seconds = (self.0 - UNIX_EPOCH_AS_DELPHI_DAYS) * SECONDS_PER_DAY;
        seconds.is_finite().then_some(seconds)
    }

    #[inline]
    pub fn unix_millis(self) -> Option<i64> {
        let millis = (self.unix_seconds()? * 1000.0).round();
        (millis.is_finite() && millis >= i64::MIN as f64 && millis <= i64::MAX as f64)
            .then_some(millis as i64)
    }

    pub fn system_time(self) -> Option<SystemTime> {
        let seconds = self.unix_seconds()?;
        if !seconds.is_finite() || seconds.abs() > u64::MAX as f64 {
            return None;
        }
        let duration = Duration::from_secs_f64(seconds.abs());
        if seconds >= 0.0 {
            UNIX_EPOCH.checked_add(duration)
        } else {
            UNIX_EPOCH.checked_sub(duration)
        }
    }
}

#[cfg(any(test, feature = "diagnostics"))]
impl From<f64> for DelphiTime {
    #[inline]
    fn from(value: f64) -> Self {
        Self::from_days(value)
    }
}

#[cfg(any(test, feature = "diagnostics"))]
impl From<DelphiTime> for f64 {
    #[inline]
    fn from(value: DelphiTime) -> Self {
        value.as_days()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unix_epoch_roundtrip() {
        let moon = MoonTime::from_unix_millis(0);
        assert_eq!(moon.to_delphi_days(), UNIX_EPOCH_AS_DELPHI_DAYS);
        assert_eq!(moon.unix_seconds(), 0.0);
        assert_eq!(moon.unix_millis(), 0);

        let dt = DelphiTime::from_unix_seconds(0.0);
        assert_eq!(dt.to_moon_time(), Some(moon));
    }

    #[test]
    fn system_time_roundtrip_handles_pre_epoch() {
        let before = UNIX_EPOCH - Duration::from_secs(86_400);
        let moon = MoonTime::from_system_time(before).unwrap();
        assert_eq!(moon.unix_millis(), -86_400_000);
        assert_eq!(moon.to_delphi_days(), UNIX_EPOCH_AS_DELPHI_DAYS - 1.0);
    }

    #[test]
    fn known_day_converts_to_unix_millis() {
        let dt = MoonTime::from_delphi_days(45_000.25).unwrap();
        let expected = ((45_000.25 - UNIX_EPOCH_AS_DELPHI_DAYS) * MILLISECONDS_PER_DAY) as i64;
        assert_eq!(dt.unix_millis(), expected);
    }

    #[test]
    fn huge_or_nan_values_return_none_instead_of_panicking() {
        for value in [
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MAX,
            -f64::MAX,
        ] {
            assert_eq!(MoonTime::from_delphi_days(value), None);
            assert_eq!(MoonTime::from_unix_seconds(value), None);
        }
    }
}
