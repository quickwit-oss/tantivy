use serde_json::Value;
use tantivy::aggregation::agg_result::AggregationResults;

/// Convert AggregationResults to JSON for comparison
pub fn aggregation_results_to_json(results: &AggregationResults) -> Value {
    serde_json::to_value(results).expect("Failed to serialize aggregation results")
}

/// Compare two JSON values with tolerance for floating point numbers
pub fn json_values_match(actual: &Value, expected: &Value, tolerance: f64) -> bool {
    match (actual, expected) {
        (Value::Number(a), Value::Number(e)) => {
            let a_f64 = a.as_f64().unwrap_or(0.0);
            let e_f64 = e.as_f64().unwrap_or(0.0);
            (a_f64 - e_f64).abs() < tolerance
        }
        (Value::Object(a_map), Value::Object(e_map)) => {
            if a_map.len() != e_map.len() {
                return false;
            }
            for (key, expected_val) in e_map {
                match a_map.get(key) {
                    Some(actual_val) => {
                        if !json_values_match(actual_val, expected_val, tolerance) {
                            return false;
                        }
                    }
                    None => return false,
                }
            }
            true
        }
        (Value::Array(a_arr), Value::Array(e_arr)) => {
            if a_arr.len() != e_arr.len() {
                return false;
            }
            for (actual_item, expected_item) in a_arr.iter().zip(e_arr.iter()) {
                if !json_values_match(actual_item, expected_item, tolerance) {
                    return false;
                }
            }
            true
        }
        _ => actual == expected,
    }
}

/// Assert that aggregation results match expected JSON structure
///
/// This utility provides a way to validate entire aggregation results by comparing
/// them against an expected JSON structure. It handles floating point comparisons with
/// tolerance and provides error messages when assertions fail.
///
/// # Arguments
/// * `actual_results` - The AggregationResults returned from aggregation search
/// * `expected_json` - A serde_json::Value representing the expected structure
/// * `tolerance` - Floating point tolerance for numeric comparisons
///
/// # Example
/// ```rust
/// let expected = json!({
///     "electronics": {
///         "doc_count": 2,
///         "avg_price": {
///             "value": 899.0
///         }
///     }
/// });
/// assert_aggregation_results_match(&result, expected, 0.1);
/// ```
pub fn assert_aggregation_results_match(
    actual_results: &AggregationResults,
    expected_json: Value,
    tolerance: f64,
) {
    let actual_json = aggregation_results_to_json(actual_results);

    if !json_values_match(&actual_json, &expected_json, tolerance) {
        panic!(
            "Aggregation results do not match expected JSON.\nActual:\n{}\nExpected:\n{}",
            serde_json::to_string_pretty(&actual_json).unwrap(),
            serde_json::to_string_pretty(&expected_json).unwrap()
        );
    }
}

/// Macro for asserting aggregation results with default tolerance
#[macro_export]
macro_rules! assert_agg_results {
    ($actual:expr, $expected:expr) => {
        $crate::common::test_helpers::assert_aggregation_results_match($actual, $expected, 0.1)
    };
    ($actual:expr, $expected:expr, $tolerance:expr) => {
        $crate::common::test_helpers::assert_aggregation_results_match(
            $actual, $expected, $tolerance,
        )
    };
}
