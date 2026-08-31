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
use columnar::ColumnType;
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

use crate::aggregation::SegmentValueSourcePlan;
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
    /// Immutable recipe used to instantiate one runtime source per segment collector.
    pub(crate) source: SegmentValueSourcePlan,
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
    /// The percentile key (e.g. 1.0, 5.0, 25.0).
    pub key: f64,
    /// The percentile value. `NaN` when there are no values.
    pub value: f64,
}

/// Single-metric aggregations use this common result structure.
///
/// Main reason to wrap it in value is to match elasticsearch output structure.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PercentilesMetricResult {
    /// The result of the percentile metric.
    pub values: PercentileValues,
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
    use std::net::Ipv6Addr;

    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::agg_result::AggregationResults;
    use crate::aggregation::AggregationCollector;
    use crate::query::AllQuery;
    use crate::schema::{NumericOptions, Schema, FAST};
    use crate::{DateTime, Index, IndexWriter};

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
    fn test_value_count_all_supported_physical_types_and_missing() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let i64_field = schema_builder.add_i64_field("i64", FAST);
        let u64_field = schema_builder.add_u64_field("u64", FAST);
        let f64_field = schema_builder.add_f64_field("f64", FAST);
        let str_field = schema_builder.add_text_field("str", FAST);
        let date_field = schema_builder.add_date_field("date", FAST);
        let bool_field = schema_builder.add_bool_field("bool", FAST);
        let ip_field = schema_builder.add_ip_addr_field("ip", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests()?;
        writer.add_document(doc!(
            i64_field => -1i64,
            u64_field => 2u64,
            f64_field => 3.5f64,
            str_field => "value",
            date_field => DateTime::from_timestamp_secs(1_700_000_000),
            bool_field => true,
            ip_field => Ipv6Addr::LOCALHOST,
        ))?;
        writer.add_document(doc!())?;
        writer.commit()?;

        let aggregations: Aggregations = serde_json::from_value(json!({
            "i64": { "value_count": { "field": "i64" } },
            "u64": { "value_count": { "field": "u64" } },
            "f64": { "value_count": { "field": "f64" } },
            "str": { "value_count": { "field": "str" } },
            "date": { "value_count": { "field": "date" } },
            "bool": { "value_count": { "field": "bool" } },
            "ip": { "value_count": { "field": "ip" } },
            "f64_missing": { "value_count": { "field": "f64", "missing": 0.0 } }
        }))?;
        let collector = AggregationCollector::from_aggs(aggregations, Default::default());
        let result: AggregationResults =
            index.reader()?.searcher().search(&AllQuery, &collector)?;
        let result = serde_json::to_value(result)?;

        for name in ["i64", "u64", "f64", "str", "date", "bool", "ip"] {
            assert_eq!(result[name]["value"], 1.0, "field {name}");
        }
        assert_eq!(result["f64_missing"]["value"], 2.0);
        Ok(())
    }
}
