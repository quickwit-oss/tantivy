use tantivy::aggregation::agg_result::{AggregationResult, BucketResult, MetricResult};
use tantivy::aggregation::metric::Stats;

/// Helper function to extract filter bucket result
pub fn get_filter_bucket(result: &AggregationResult) -> Option<&tantivy::aggregation::agg_result::FilterBucketResult> {
    if let AggregationResult::BucketResult(BucketResult::Filter(filter_result)) = result {
        Some(filter_result)
    } else {
        None
    }
}

/// Helper function to extract metric value from various metric types
pub fn get_metric_value(result: &AggregationResult) -> Option<f64> {
    match result {
        AggregationResult::MetricResult(MetricResult::Average(avg)) => avg.value,
        AggregationResult::MetricResult(MetricResult::Count(count)) => count.value,
        AggregationResult::MetricResult(MetricResult::Sum(sum)) => sum.value,
        AggregationResult::MetricResult(MetricResult::Min(min)) => min.value,
        AggregationResult::MetricResult(MetricResult::Max(max)) => max.value,
        AggregationResult::MetricResult(MetricResult::Cardinality(card)) => card.value,
        _ => None,
    }
}

/// Helper function to extract stats from a stats aggregation
pub fn get_stats(result: &AggregationResult) -> Option<&Stats> {
    if let AggregationResult::MetricResult(MetricResult::Stats(stats)) = result {
        Some(stats)
    } else {
        None
    }
}

/// Helper function to validate a filter bucket with expected doc count
pub fn validate_filter_bucket(result: &AggregationResult, expected_doc_count: u64) -> bool {
    if let Some(filter_bucket) = get_filter_bucket(result) {
        filter_bucket.doc_count == expected_doc_count
    } else {
        false
    }
}

/// Helper function to validate a metric value within tolerance
pub fn validate_metric_value(result: &AggregationResult, expected_value: f64, tolerance: f64) -> bool {
    if let Some(actual_value) = get_metric_value(result) {
        (actual_value - expected_value).abs() < tolerance
    } else {
        false
    }
}
