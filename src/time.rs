//! Time helpers for Delphi `TDateTime` values used by MoonProto.
//!
//! MoonProto inherits MoonBot's Delphi representation: days since
//! `1899-12-30`, stored as `f64`. This is not Unix time. Public structs keep
//! some raw fields for byte-level compatibility and dense history storage, but
//! application code should convert through [`DelphiTime`] instead of casting the
//! raw day value to a Unix timestamp.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const SECONDS_PER_DAY: f64 = 86_400.0;
pub const MILLISECONDS_PER_DAY: f64 = 86_400_000.0;
pub const UNIX_EPOCH_AS_DELPHI_DAYS: f64 = 25_569.0;

/// Delphi `TDateTime` value: days since `1899-12-30`.
#[repr(transparent)]
#[derive(Debug, Default, Clone, Copy, PartialEq, PartialOrd)]
pub struct DelphiTime(f64);

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

    #[inline]
    pub const fn as_days(self) -> f64 {
        self.0
    }

    #[inline]
    pub fn unix_seconds(self) -> Option<f64> {
        self.0
            .is_finite()
            .then_some((self.0 - UNIX_EPOCH_AS_DELPHI_DAYS) * SECONDS_PER_DAY)
    }

    #[inline]
    pub fn unix_millis(self) -> Option<i64> {
        self.unix_seconds()
            .map(|seconds| (seconds * 1000.0).round() as i64)
    }

    pub fn system_time(self) -> Option<SystemTime> {
        let seconds = self.unix_seconds()?;
        if seconds >= 0.0 {
            Some(UNIX_EPOCH + Duration::from_secs_f64(seconds))
        } else {
            Some(UNIX_EPOCH - Duration::from_secs_f64(-seconds))
        }
    }
}

impl From<f64> for DelphiTime {
    #[inline]
    fn from(value: f64) -> Self {
        Self::from_days(value)
    }
}

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
        let dt = DelphiTime::from_unix_seconds(0.0);
        assert_eq!(dt.as_days(), UNIX_EPOCH_AS_DELPHI_DAYS);
        assert_eq!(dt.unix_seconds(), Some(0.0));
        assert_eq!(dt.unix_millis(), Some(0));
    }

    #[test]
    fn system_time_roundtrip_handles_pre_epoch() {
        let before = UNIX_EPOCH - Duration::from_secs(86_400);
        let dt = DelphiTime::from_system_time(before);
        assert_eq!(dt.as_days(), UNIX_EPOCH_AS_DELPHI_DAYS - 1.0);
    }

    #[test]
    fn known_day_converts_to_unix_millis() {
        let dt = DelphiTime::from_days(45_000.25);
        let expected = ((45_000.25 - UNIX_EPOCH_AS_DELPHI_DAYS) * MILLISECONDS_PER_DAY) as i64;
        assert_eq!(dt.unix_millis(), Some(expected));
    }
}
