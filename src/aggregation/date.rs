use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::TantivyError;

pub(crate) fn format_date(val: i64) -> crate::Result<String> {
    let datetime = OffsetDateTime::from_unix_timestamp_nanos(val as i128).map_err(|err| {
        TantivyError::InvalidArgument(format!(
            "Could not convert {val:?} to OffsetDateTime, err {err:?}"
        ))
    })?;
    let key_as_string = datetime
        .format(&Rfc3339)
        .map_err(|_err| TantivyError::InvalidArgument("Could not serialize date".to_string()))?;
    Ok(key_as_string)
}

pub(crate) fn parse_date(date_string: &str) -> crate::Result<i64> {
    OffsetDateTime::parse(date_string, &Rfc3339)
        .map_err(|err| {
            TantivyError::InvalidArgument(format!(
                "Could not parse '{date_string}' as RFC3339 date, err: {err:?}"
            ))
        })
        .map(|datetime| datetime.unix_timestamp_nanos() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_date_roundtrip() -> crate::Result<()> {
        let timestamp = 1697548800_001_001_001i64; // 2023-10-17T13:20:00Z
        let date_string = format_date(timestamp)?;
        let parsed_timestamp = parse_date(&date_string)?;
        assert_eq!(timestamp, parsed_timestamp, "Roundtrip conversion failed");

        Ok(())
    }

    #[test]
    fn test_invalid_date_parsing() {
        // Test with invalid date format
        let result = parse_date("invalid date");
        assert!(result.is_err(), "Should error on invalid date format");

        let result = parse_date("2023/10/17 12:00:00");
        assert!(result.is_err(), "Should error on non-RFC3339 format");
    }
}
