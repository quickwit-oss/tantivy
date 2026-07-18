use std::fmt;
use std::io::{Read, Write};

use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;
use time::{OffsetDateTime, PrimitiveDateTime, UtcOffset};

use crate::BinarySerializable;

/// Precision with which datetimes are truncated when stored in fast fields. This setting is only
/// relevant for fast fields. In the docstore, datetimes are always saved with nanosecond precision.
#[derive(
    Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "lowercase")]
pub enum DateTimePrecision {
    /// Second precision.
    #[default]
    Seconds,
    /// Millisecond precision.
    Milliseconds,
    /// Microsecond precision.
    Microseconds,
    /// Nanosecond precision.
    Nanoseconds,
}

/// A date/time value with nanoseconds precision.
///
/// This timestamp does not carry any explicit time zone information.
/// Users are responsible for applying the provided conversion
/// functions consistently. Internally the time zone is assumed
/// to be UTC, which is also used implicitly for JSON serialization.
///
/// All constructors and conversions are provided as explicit
/// functions and not by implementing any `From`/`Into` traits
/// to prevent unintended usage.
#[derive(Clone, Default, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DateTime {
    // Timestamp in nanoseconds.
    pub(crate) timestamp_nanos: i64,
}

impl DateTime {
    /// Minimum possible `DateTime` value.
    pub const MIN: DateTime = DateTime {
        timestamp_nanos: i64::MIN,
    };

    /// Maximum possible `DateTime` value.
    pub const MAX: DateTime = DateTime {
        timestamp_nanos: i64::MAX,
    };

    /// Create new from UNIX timestamp in seconds
    pub const fn from_timestamp_secs(seconds: i64) -> Self {
        Self {
            timestamp_nanos: seconds * 1_000_000_000,
        }
    }

    /// Create new from UNIX timestamp in milliseconds
    pub const fn from_timestamp_millis(milliseconds: i64) -> Self {
        Self {
            timestamp_nanos: milliseconds * 1_000_000,
        }
    }

    /// Create new from UNIX timestamp in microseconds.
    pub const fn from_timestamp_micros(microseconds: i64) -> Self {
        Self {
            timestamp_nanos: microseconds * 1_000,
        }
    }

    /// Create new from UNIX timestamp in nanoseconds.
    pub const fn from_timestamp_nanos(nanoseconds: i64) -> Self {
        Self {
            timestamp_nanos: nanoseconds,
        }
    }

    /// Create new from `OffsetDateTime`
    ///
    /// The given date/time is converted to UTC and the actual
    /// time zone is discarded.
    pub fn from_utc(dt: OffsetDateTime) -> Self {
        let timestamp_nanos = dt.unix_timestamp_nanos() as i64;
        Self { timestamp_nanos }
    }

    /// Create new from `PrimitiveDateTime`
    ///
    /// Implicitly assumes that the given date/time is in UTC!
    /// Otherwise the original value must only be reobtained with
    /// [`Self::into_primitive()`].
    pub fn from_primitive(dt: PrimitiveDateTime) -> Self {
        Self::from_utc(dt.assume_utc())
    }

    /// Convert to UNIX timestamp in seconds.
    pub const fn into_timestamp_secs(self) -> i64 {
        // Euclidean division floors towards negative infinity so that timestamps
        // before the epoch are rounded down to their containing second instead of
        // towards zero (which would move them forward in time).
        self.timestamp_nanos.div_euclid(1_000_000_000)
    }

    /// Convert to UNIX timestamp in milliseconds.
    pub const fn into_timestamp_millis(self) -> i64 {
        self.timestamp_nanos.div_euclid(1_000_000)
    }

    /// Convert to UNIX timestamp in microseconds.
    pub const fn into_timestamp_micros(self) -> i64 {
        self.timestamp_nanos.div_euclid(1_000)
    }

    /// Convert to UNIX timestamp in nanoseconds.
    pub const fn into_timestamp_nanos(self) -> i64 {
        self.timestamp_nanos
    }

    /// Convert to UTC `OffsetDateTime`
    pub fn into_utc(self) -> OffsetDateTime {
        let utc_datetime = OffsetDateTime::from_unix_timestamp_nanos(self.timestamp_nanos as i128)
            .expect("valid UNIX timestamp");
        debug_assert_eq!(UtcOffset::UTC, utc_datetime.offset());
        utc_datetime
    }

    /// Convert to `OffsetDateTime` with the given time zone
    pub fn into_offset(self, offset: UtcOffset) -> OffsetDateTime {
        self.into_utc().to_offset(offset)
    }

    /// Convert to `PrimitiveDateTime` without any time zone
    ///
    /// The value should have been constructed with [`Self::from_primitive()`].
    /// Otherwise the time zone is implicitly assumed to be UTC.
    pub fn into_primitive(self) -> PrimitiveDateTime {
        let utc_datetime = self.into_utc();
        // Discard the UTC time zone offset
        debug_assert_eq!(UtcOffset::UTC, utc_datetime.offset());
        PrimitiveDateTime::new(utc_datetime.date(), utc_datetime.time())
    }

    /// Truncates the timestamp to the corresponding precision.
    ///
    /// Truncation always floors towards the earlier instant, so a timestamp is
    /// mapped to the start of the bucket that contains it. This relies on
    /// Euclidean division, otherwise timestamps before the epoch would be
    /// rounded towards zero and land in the following bucket.
    pub fn truncate(self, precision: DateTimePrecision) -> Self {
        let truncated_timestamp_nanos = match precision {
            DateTimePrecision::Seconds => {
                self.timestamp_nanos.div_euclid(1_000_000_000) * 1_000_000_000
            }
            DateTimePrecision::Milliseconds => {
                self.timestamp_nanos.div_euclid(1_000_000) * 1_000_000
            }
            DateTimePrecision::Microseconds => self.timestamp_nanos.div_euclid(1_000) * 1_000,
            DateTimePrecision::Nanoseconds => self.timestamp_nanos,
        };
        Self {
            timestamp_nanos: truncated_timestamp_nanos,
        }
    }
}

impl fmt::Debug for DateTime {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let utc_rfc3339 = self.into_utc().format(&Rfc3339).map_err(|_| fmt::Error)?;
        f.write_str(&utc_rfc3339)
    }
}

impl BinarySerializable for DateTime {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> std::io::Result<()> {
        let timestamp_micros = self.into_timestamp_micros();
        <i64 as BinarySerializable>::serialize(&timestamp_micros, writer)
    }

    fn deserialize<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let timestamp_micros = <i64 as BinarySerializable>::deserialize(reader)?;
        Ok(Self::from_timestamp_micros(timestamp_micros))
    }
}

#[cfg(test)]
mod tests {
    use super::{DateTime, DateTimePrecision};

    #[test]
    fn test_into_timestamp_floors_for_negative_values() {
        // A timestamp of -0.5s is inside the wall-clock second `[-1s, 0s)`, so
        // converting it to a coarser unit must floor to -1, not round toward zero.
        assert_eq!(
            DateTime::from_timestamp_nanos(-500_000_000).into_timestamp_secs(),
            -1
        );
        assert_eq!(
            DateTime::from_timestamp_nanos(-1_500_000_000).into_timestamp_secs(),
            -2
        );

        assert_eq!(
            DateTime::from_timestamp_nanos(-500_000).into_timestamp_millis(),
            -1
        );
        assert_eq!(
            DateTime::from_timestamp_nanos(-1_500_000).into_timestamp_millis(),
            -2
        );

        assert_eq!(
            DateTime::from_timestamp_nanos(-500).into_timestamp_micros(),
            -1
        );
        assert_eq!(
            DateTime::from_timestamp_nanos(-1_500).into_timestamp_micros(),
            -2
        );
    }

    #[test]
    fn test_into_timestamp_unchanged_for_positive_and_exact_values() {
        assert_eq!(
            DateTime::from_timestamp_nanos(1_500_000_000).into_timestamp_secs(),
            1
        );
        assert_eq!(
            DateTime::from_timestamp_nanos(-1_000_000_000).into_timestamp_secs(),
            -1
        );
        assert_eq!(DateTime::from_timestamp_nanos(0).into_timestamp_secs(), 0);

        assert_eq!(
            DateTime::from_timestamp_nanos(1_500_000).into_timestamp_millis(),
            1
        );
        assert_eq!(
            DateTime::from_timestamp_nanos(1_500).into_timestamp_micros(),
            1
        );
    }

    #[test]
    fn test_truncate_never_moves_forward_in_time() {
        // Truncating to a coarser precision must never produce a value that is
        // later than the input, otherwise the timestamp lands in the wrong bucket.
        let negative = DateTime::from_timestamp_nanos(-500_000_000);
        assert_eq!(
            negative.truncate(DateTimePrecision::Seconds),
            DateTime::from_timestamp_secs(-1),
        );
        assert!(negative.truncate(DateTimePrecision::Seconds) <= negative);

        assert_eq!(
            DateTime::from_timestamp_nanos(-1_500_000_000).truncate(DateTimePrecision::Seconds),
            DateTime::from_timestamp_secs(-2),
        );

        assert_eq!(
            DateTime::from_timestamp_nanos(-1_500_000).truncate(DateTimePrecision::Milliseconds),
            DateTime::from_timestamp_millis(-2),
        );
        assert_eq!(
            DateTime::from_timestamp_nanos(-1_500).truncate(DateTimePrecision::Microseconds),
            DateTime::from_timestamp_micros(-2),
        );
    }

    #[test]
    fn test_truncate_buckets_same_second_together() {
        // Every instant inside the wall-clock second `[-1s, 0s)` must truncate to
        // the same bucket (-1s) at second precision.
        let second_start = DateTime::from_timestamp_secs(-1);
        for nanos in [-1_000_000_000, -999_999_999, -500_000_000, -1] {
            assert_eq!(
                DateTime::from_timestamp_nanos(nanos).truncate(DateTimePrecision::Seconds),
                second_start,
                "nanos {nanos} should truncate to -1s",
            );
        }
        // ...and the next second up must be a different bucket.
        assert_eq!(
            DateTime::from_timestamp_nanos(0).truncate(DateTimePrecision::Seconds),
            DateTime::from_timestamp_secs(0),
        );
        assert_eq!(
            DateTime::from_timestamp_nanos(500_000_000).truncate(DateTimePrecision::Seconds),
            DateTime::from_timestamp_secs(0),
        );
    }

    #[test]
    fn test_truncate_positive_values_unchanged() {
        assert_eq!(
            DateTime::from_timestamp_nanos(1_500_000_000).truncate(DateTimePrecision::Seconds),
            DateTime::from_timestamp_secs(1),
        );
        assert_eq!(
            DateTime::from_timestamp_nanos(1_999_999_999).truncate(DateTimePrecision::Seconds),
            DateTime::from_timestamp_secs(1),
        );
        assert_eq!(
            DateTime::from_timestamp_nanos(42).truncate(DateTimePrecision::Nanoseconds),
            DateTime::from_timestamp_nanos(42),
        );
    }

    #[test]
    fn test_truncate_bucket_invariant_over_range() {
        // For any timestamp and precision, the truncated value must be <= the
        // original and within one unit of it (i.e. a proper floor to the bucket).
        let units = [
            (DateTimePrecision::Seconds, 1_000_000_000i64),
            (DateTimePrecision::Milliseconds, 1_000_000),
            (DateTimePrecision::Microseconds, 1_000),
        ];
        for (precision, unit) in units {
            for nanos in [-3_333_333_333i64, -1_000_000_001, -7, 0, 7, 1_000_000_001] {
                let dt = DateTime::from_timestamp_nanos(nanos);
                let truncated = dt.truncate(precision).into_timestamp_nanos();
                assert!(
                    truncated <= nanos,
                    "{truncated} !<= {nanos} for {precision:?}"
                );
                assert!(nanos - truncated < unit, "gap too large for {precision:?}");
                assert_eq!(truncated % unit, 0, "not aligned for {precision:?}");
            }
        }
    }
}
