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
mod cardinality;
mod count;
mod extended_stats;
mod max;
mod min;
mod percentiles;
mod stats;
mod sum;
mod top_hits;

use std::collections::HashMap;

pub use average::*;
pub use cardinality::*;
use columnar::{Column, ColumnType};
pub use count::*;
pub use extended_stats::*;
pub use max::*;
pub use min::*;
pub use percentiles::*;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
pub use stats::*;
pub use sum::*;
pub use top_hits::*;

use crate::schema::OwnedValue;

/// Contains all information required by metric aggregations like avg, min, max, sum, stats,
/// extended_stats, count, percentiles.
#[repr(C)]
pub struct MetricAggReqData {
    /// True if the field is of number or date type.
    pub is_number_or_date_type: bool,
    /// The type of the field.
    pub field_type: ColumnType,
    /// The missing value normalized to the internal u64 representation of the field type.
    pub missing_u64: Option<u64>,
    /// The column accessor to access the fast field values.
    pub accessor: Column<u64>,
    /// Used when converting to intermediate result
    pub collecting_for: StatsType,
    /// The missing value
    pub missing: Option<f64>,
    /// The name of the aggregation.
    pub name: String,
}

impl MetricAggReqData {
    /// Estimate the memory consumption of this struct in bytes.
    pub fn get_memory_consumption(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

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

/// Average metric result with intermediate data for merging.
///
/// Unlike [`SingleMetricResult`], this struct includes the raw `sum` and `count`
/// values that can be used for multi-step query merging.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AverageMetricResult {
    /// The computed average value. None if no documents matched.
    pub value: Option<f64>,
    /// The sum of all values (for multi-step merging).
    pub sum: f64,
    /// The count of all values (for multi-step merging).
    pub count: u64,
}

/// Cardinality metric result with computed value and raw HLL sketch for multi-step merging.
///
/// The `value` field contains the computed cardinality estimate.
/// The `sketch` field contains the serialized HyperLogLog++ sketch that can be used
/// for merging results across multiple query steps.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CardinalityMetricResult {
    /// The computed cardinality estimate.
    pub value: Option<f64>,
    /// The serialized HyperLogLog++ sketch for multi-step merging.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sketch: Option<CardinalityCollector>,
}

impl PartialEq for CardinalityMetricResult {
    fn eq(&self, other: &Self) -> bool {
        // Only compare values, not sketch (sketch comparison is complex)
        self.value == other.value
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

/// Percentiles metric result with computed values and raw sketch for multi-step merging.
///
/// The `values` field contains the computed percentile values.
/// The `sketch` field contains the serialized DDSketch that can be used for merging
/// results across multiple query steps.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PercentilesMetricResult {
    /// The computed percentile values.
    pub values: PercentileValues,
    /// The serialized DDSketch for multi-step merging.
    /// This is the raw sketch data that can be deserialized and merged with other sketches.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sketch: Option<PercentilesCollector>,
}

impl PartialEq for PercentilesMetricResult {
    fn eq(&self, other: &Self) -> bool {
        // Only compare values, not sketch (sketch comparison is complex)
        self.values == other.values
    }
}

/// The top_hits metric results entry
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TopHitsVecEntry {
    /// The sort values of the document, depending on the sort criteria in the request.
    pub sort: Vec<Option<u64>>,

    /// Search results, for queries that include field retrieval requests
    /// (`docvalue_fields`).
    #[serde(rename = "docvalue_fields")]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub doc_value_fields: HashMap<String, OwnedValue>,
}

/// The top_hits metric aggregation results a list of top hits by sort criteria.
///
/// The main reason for wrapping it in `hits` is to match elasticsearch output structure.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TopHitsMetricResult {
    /// The result of the top_hits metric.
    pub hits: Vec<TopHitsVecEntry>,
}

#[cfg(test)]
mod tests {
    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::agg_result::AggregationResults;
    use crate::aggregation::AggregationCollector;
    use crate::query::AllQuery;
    use crate::schema::{NumericOptions, Schema};
    use crate::{Index, IndexWriter};

    #[test]
    fn test_metric_aggregations() {
        let mut schema_builder = Schema::builder();
        let field_options = NumericOptions::default().set_fast();
        let field = schema_builder.add_f64_field("price", field_options);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();

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

    #[test]
    fn test_average_returns_sum_and_count() {
        let mut schema_builder = Schema::builder();
        let field_options = NumericOptions::default().set_fast();
        let field = schema_builder.add_f64_field("price", field_options);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();

        // Add documents with values 0, 1, 2, 3, 4, 5
        // sum = 15, count = 6, avg = 2.5
        for i in 0..6 {
            index_writer
                .add_document(doc!(
                    field => i as f64,
                ))
                .unwrap();
        }
        index_writer.commit().unwrap();

        let aggregations_json = r#"{ "price_avg": { "avg": { "field": "price" } } }"#;
        let aggregations: Aggregations = serde_json::from_str(aggregations_json).unwrap();
        let collector = AggregationCollector::from_aggs(aggregations, Default::default());
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let aggregations_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();
        let aggregations_res_json = serde_json::to_value(aggregations_res).unwrap();

        // Verify all three fields are present and correct
        assert_eq!(aggregations_res_json["price_avg"]["value"], 2.5);
        assert_eq!(aggregations_res_json["price_avg"]["sum"], 15.0);
        assert_eq!(aggregations_res_json["price_avg"]["count"], 6);
    }

    #[test]
    fn test_percentiles_returns_sketch() {
        let mut schema_builder = Schema::builder();
        let field_options = NumericOptions::default().set_fast();
        let field = schema_builder.add_f64_field("latency", field_options);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();

        // Add documents with latency values
        for i in 0..100 {
            index_writer
                .add_document(doc!(
                    field => i as f64,
                ))
                .unwrap();
        }
        index_writer.commit().unwrap();

        let aggregations_json =
            r#"{ "latency_percentiles": { "percentiles": { "field": "latency" } } }"#;
        let aggregations: Aggregations = serde_json::from_str(aggregations_json).unwrap();
        let collector = AggregationCollector::from_aggs(aggregations, Default::default());
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let aggregations_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();
        let aggregations_res_json = serde_json::to_value(aggregations_res).unwrap();

        // Verify percentile values are present
        assert!(aggregations_res_json["latency_percentiles"]["values"].is_object());
        // Verify sketch is present (serialized DDSketch)
        assert!(aggregations_res_json["latency_percentiles"]["sketch"].is_object());
    }

    #[test]
    fn test_cardinality_returns_sketch() {
        let mut schema_builder = Schema::builder();
        let field_options = NumericOptions::default().set_fast();
        let field = schema_builder.add_u64_field("user_id", field_options);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();

        // Add documents with some duplicate user_ids
        for i in 0..50 {
            index_writer
                .add_document(doc!(
                    field => (i % 10) as u64,  // 10 unique values
                ))
                .unwrap();
        }
        index_writer.commit().unwrap();

        let aggregations_json =
            r#"{ "unique_users": { "cardinality": { "field": "user_id" } } }"#;
        let aggregations: Aggregations = serde_json::from_str(aggregations_json).unwrap();
        let collector = AggregationCollector::from_aggs(aggregations, Default::default());
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let aggregations_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();
        let aggregations_res_json = serde_json::to_value(aggregations_res).unwrap();

        // Verify cardinality value is present and approximately correct
        let cardinality = aggregations_res_json["unique_users"]["value"].as_f64().unwrap();
        assert!(cardinality >= 9.0 && cardinality <= 11.0); // HLL is approximate
        // Verify sketch is present (serialized HyperLogLog++)
        assert!(aggregations_res_json["unique_users"]["sketch"].is_object());
    }
}
