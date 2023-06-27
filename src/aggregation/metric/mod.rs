//! Module for all metric aggregations.
//!
//! The aggregations in this family compute metrics based on values extracted
//! from the documents that are being aggregated. Values are extracted from the fast field of
//! the document.
//! Some aggregations output a single numeric metric (e.g. Average) and are called
//! single-value numeric metrics aggregation, others generate multiple metrics (e.g. Stats) and are
//! called multi-value numeric metrics aggregation.
//!
//! ## Supported Metric Aggregations
//! - [Average](AverageAggregation)
//! - [Stats](StatsAggregation)
//! - [Min](MinAggregation)
//! - [Max](MaxAggregation)
//! - [Sum](SumAggregation)
//! - [Count](CountAggregation)
//! - [Percentiles](PercentilesAggregationReq)

mod average;
mod count;
mod max;
mod min;
mod percentiles;
mod stats;
mod sum;
pub use average::*;
pub use count::*;
pub use max::*;
pub use min::*;
pub use percentiles::*;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
pub use stats::*;
pub use sum::*;

/// Single-metric aggregations use this common result structure.
///
/// Main reason to wrap it in value is to match elasticsearch output structure.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SingleMetricResult {
    /// The value of the single value metric.
    pub value: Option<f64>,
}

impl From<f64> for SingleMetricResult {
    fn from(value: f64) -> Self {
        Self { value: Some(value) }
    }
}

impl From<Option<f64>> for SingleMetricResult {
    fn from(value: Option<f64>) -> Self {
        Self { value }
    }
}

/// This is the wrapper of percentile entries, which can be vector or hashmap
/// depending on if it's keyed or not.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PercentileValues {
    /// Vector format percentile entries
    Vec(Vec<PercentileValuesVecEntry>),
    /// HashMap format percentile entries. Key is the serialized percentile
    HashMap(FxHashMap<String, f64>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// The entry when requesting percentiles with keyed: false
pub struct PercentileValuesVecEntry {
    key: f64,
    value: f64,
}

/// Single-metric aggregations use this common result structure.
///
/// Main reason to wrap it in value is to match elasticsearch output structure.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PercentilesMetricResult {
    /// The result of the percentile metric.
    pub values: PercentileValues,
}

#[cfg(test)]
mod tests {
    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::agg_result::AggregationResults;
    use crate::aggregation::AggregationCollector;
    use crate::query::AllQuery;
    use crate::schema::{NumericOptions, Schema};
    use crate::Index;

    #[test]
    fn test_metric_aggregations() {
        let mut schema_builder = Schema::builder();
        let field_options = NumericOptions::default().set_fast();
        let field = schema_builder.add_f64_field("price", field_options);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index.writer_for_tests().unwrap();

        for i in 0..3 {
            index_writer
                .add_document(doc!(
                    field => i as f64,
                ))
                .unwrap();
        }
        index_writer.commit().unwrap();

        for i in 3..6 {
            index_writer
                .add_document(doc!(
                    field => i as f64,
                ))
                .unwrap();
        }
        index_writer.commit().unwrap();

        let aggregations_json = r#"{
            "price_avg": { "avg": { "field": "price" } },
            "price_count": { "value_count": { "field": "price" } },
            "price_max": { "max": { "field": "price" } },
            "price_min": { "min": { "field": "price" } },
            "price_stats": { "stats": { "field": "price" } },
            "price_sum": { "sum": { "field": "price" } }
        }"#;
        let aggregations: Aggregations = serde_json::from_str(aggregations_json).unwrap();
        let collector = AggregationCollector::from_aggs(aggregations, Default::default());
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let aggregations_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();
        let aggregations_res_json = serde_json::to_value(aggregations_res).unwrap();

        assert_eq!(aggregations_res_json["price_avg"]["value"], 2.5);
        assert_eq!(aggregations_res_json["price_count"]["value"], 6.0);
        assert_eq!(aggregations_res_json["price_max"]["value"], 5.0);
        assert_eq!(aggregations_res_json["price_min"]["value"], 0.0);
        assert_eq!(aggregations_res_json["price_sum"]["value"], 15.0);
    }
}
