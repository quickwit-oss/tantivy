use serde::{Deserialize, Serialize};

/// DateHistogramAggregation is similar to `HistogramAggregation`, but it can only be used with date
/// type.
///
/// Currently only **fixed time** intervals are supported. Calendar-aware time intervals are not
/// supported.
///
/// Like the histogram, values are rounded down into the closest bucket.
///
/// For this calculation all fastfield values are converted to f64.
///
/// # Limitations/Compatibility
/// Only fixed time intervals are supported.
///
/// # JSON Format
/// ```json
/// {
///     "prices": {
///         "date_histogram": {
///             "field": "price",
///             "fixed_interval": "30d"
///         }
///     }
/// }
/// ```
///
/// Response
/// See [`BucketEntry`](crate::aggregation::agg_result::BucketEntry)
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DateHistogramAggregationReq {
    /// The field to aggregate on.
    pub field: String,
    /// The interval to chunk your data range. Each bucket spans a value range of
    /// [0..fixed_interval). Accepted values
    ///
    /// Fixed intervals are configured with the `fixed_interval` parameter.
    /// In contrast to calendar-aware intervals, fixed intervals are a fixed number of SI units and
    /// never deviate, regardless of where they fall on the calendar. One second is always
    /// composed of 1000ms. This allows fixed intervals to be specified in any multiple of the
    /// supported units. However, it means fixed intervals cannot express other units such as
    /// months, since the duration of a month is not a fixed quantity. Attempting to specify a
    /// calendar interval like month or quarter will return an Error.
    ///
    /// The accepted units for fixed intervals are:
    /// * `ms`: milliseconds
    /// * `s`: seconds. Defined as 1000 milliseconds each.
    /// * `m`: minutes. Defined as 60 seconds each (60_000 milliseconds).
    /// * `h`: hours. Defined as 60 minutes each (3_600_000 milliseconds).
    /// * `d`: days. Defined as 24 hours (86_400_000 milliseconds).
    ///
    /// Fractional time values are not supported, but you can address this by shifting to another
    /// time unit (e.g., `1.5h` could instead be specified as `90m`).
    pub fixed_interval: String,
    /// Intervals implicitly defines an absolute grid of buckets `[interval * k, interval * (k +
    /// 1))`.
    pub offset: Option<String>,
    /// Whether to return the buckets as a hash map
    #[serde(default)]
    pub keyed: bool,
}

impl DateHistogramAggregationReq {
    fn validate(&self) -> crate::Result<()> {
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
/// Errors when parsing the fixed interval for `DateHistogramAggregationReq`.
pub enum DateHistogramParseError {
    /// Unit not recognized in passed String
    UnitNotRecognized(String),
    /// Number not found in passed String
    NumberMissing(String),
    /// Unit not found in passed String
    UnitMissing(String),
}

fn parse_into_milliseconds(input: &str) -> Result<u64, DateHistogramParseError> {
    let split_boundary = input
        .as_bytes()
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    let (number, unit) = input.split_at(split_boundary);
    if number.is_empty() {
        return Err(DateHistogramParseError::NumberMissing(input.to_string()));
    }
    if unit.is_empty() {
        return Err(DateHistogramParseError::UnitMissing(input.to_string()));
    }
    let number: u64 = number
        .parse()
        // Technically this should never happen, but there was a bug
        // here and being defensive does not hurt.
        .map_err(|_err| DateHistogramParseError::NumberMissing(input.to_string()))?;

    let multiplier_from_unit = match unit {
        "ms" => 1,
        "s" => 1000,
        "m" => 60 * 1000,
        "h" => 60 * 60 * 1000,
        "d" => 24 * 60 * 60 * 1000,
        _ => return Err(DateHistogramParseError::UnitNotRecognized(unit.to_string())),
    };

    Ok(number * multiplier_from_unit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_into_milliseconds() {
        assert_eq!(parse_into_milliseconds("1m").unwrap(), 60_000);
        assert_eq!(parse_into_milliseconds("2m").unwrap(), 120_000);
        assert_eq!(
            parse_into_milliseconds("2y").unwrap_err(),
            DateHistogramParseError::UnitNotRecognized("y".to_string())
        );
        assert_eq!(
            parse_into_milliseconds("2000").unwrap_err(),
            DateHistogramParseError::UnitMissing("2000".to_string())
        );
        assert_eq!(
            parse_into_milliseconds("ms").unwrap_err(),
            DateHistogramParseError::NumberMissing("ms".to_string())
        );
    }

    #[test]
    fn test_parse_into_milliseconds_do_not_accept_non_ascii() {
        assert!(parse_into_milliseconds("１m").is_err());
    }
}
