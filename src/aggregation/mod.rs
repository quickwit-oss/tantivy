//! # Aggregations
//!
//! An aggregation summarizes your data as statistics on buckets or metrics.
//!
//! Aggregations can provide answer to questions like:
//! - What is the average price of all sold articles?
//! - How many errors with status code 500 do we have per day?
//! - What is the average listing price of cars grouped by color?
//!
//! There are two categories: [Metrics](metric) and [Buckets](bucket).
//!
//! ## Prerequisite
//! Currently aggregations work only on [fast fields](`crate::fastfield`). Fast fields
//! of type `u64`, `f64`, `i64`, `date` and fast fields on text fields.
//!
//! ## Usage
//! To use aggregations, build an aggregation request by constructing
//! [`Aggregations`](agg_req::Aggregations).
//! Create an [`AggregationCollector`] from this request. `AggregationCollector` implements the
//! [`Collector`](crate::collector::Collector) trait and can be passed as collector into
//! [`Searcher::search()`](crate::Searcher::search).
//!
//!
//! ## JSON Format
//! Aggregations request and result structures de/serialize into elasticsearch compatible JSON.
//!
//! Notice: Intermediate aggregation results should not be de/serialized via JSON format.
//! Postcard is a good choice.
//!
//! ```verbatim
//! let agg_req: Aggregations = serde_json::from_str(json_request_string).unwrap();
//! let collector = AggregationCollector::from_aggs(agg_req, None);
//! let searcher = reader.searcher();
//! let agg_res = searcher.search(&term_query, &collector).unwrap_err();
//! let json_response_string: String = &serde_json::to_string(&agg_res)?;
//! ```
//!
//! ## Supported Aggregations
//! - [Bucket](bucket)
//!     - [Histogram](bucket::HistogramAggregation)
//!     - [DateHistogram](bucket::DateHistogramAggregationReq)
//!     - [Range](bucket::RangeAggregation)
//!     - [Terms](bucket::TermsAggregation)
//! - [Metric](metric)
//!     - [Average](metric::AverageAggregation)
//!     - [Stats](metric::StatsAggregation)
//!     - [Min](metric::MinAggregation)
//!     - [Max](metric::MaxAggregation)
//!     - [Sum](metric::SumAggregation)
//!     - [Count](metric::CountAggregation)
//!     - [Percentiles](metric::PercentilesAggregationReq)
//!
//! # Example
//! Compute the average metric, by building [`agg_req::Aggregations`], which is built from an
//! `(String, agg_req::Aggregation)` iterator.
//!
//! Requests are compatible with the elasticsearch JSON request format.
//!
//! ```
//! use tantivy::aggregation::agg_req::Aggregations;
//!
//! let elasticsearch_compatible_json_req = r#"
//! {
//!   "average": {
//!     "avg": { "field": "score" }
//!   },
//!   "range": {
//!     "range": {
//!       "field": "score",
//!       "ranges": [
//!         { "to": 3.0 },
//!         { "from": 3.0, "to": 7.0 },
//!         { "from": 7.0, "to": 20.0 },
//!         { "from": 20.0 }
//!       ]
//!     },
//!     "aggs": {
//!       "average_in_range": { "avg": { "field": "score" } }
//!     }
//!   }
//! }
//! "#;
//! let agg_req: Aggregations =
//!     serde_json::from_str(elasticsearch_compatible_json_req).unwrap();
//! ```
//! # Code Organization
//!
//! Check the [README](https://github.com/quickwit-oss/tantivy/tree/main/src/aggregation#readme) on github to see how the code is organized.
//!
//! # Nested Aggregation
//!
//! Buckets can contain sub-aggregations. In this example we create buckets with the range
//! aggregation and then calculate the average on each bucket.
//! ```
//! use tantivy::aggregation::agg_req::*;
//! use serde_json::json;
//!
//! let agg_req_1: Aggregations = serde_json::from_value(json!({
//!     "rangef64": {
//!         "range": {
//!             "field": "score",
//!             "ranges": [
//!                 { "from": 3, "to": 7000 },
//!                 { "from": 7000, "to": 20000 },
//!                 { "from": 50000, "to": 60000 }
//!             ]
//!         },
//!         "aggs": {
//!             "average_in_range": { "avg": { "field": "score" } }
//!         }
//!     },
//! }))
//! .unwrap();
//! ```
//!
//! # Distributed Aggregation
//! When the data is distributed on different [`Index`](crate::Index) instances, the
//! [`DistributedAggregationCollector`] provides functionality to merge data between independent
//! search calls by returning
//! [`IntermediateAggregationResults`](intermediate_agg_result::IntermediateAggregationResults).
//! `IntermediateAggregationResults` provides the
//! [`merge_fruits`](intermediate_agg_result::IntermediateAggregationResults::merge_fruits) method
//! to merge multiple results. The merged result can then be converted into
//! [`AggregationResults`](agg_result::AggregationResults) via the
//! [`into_final_result`](intermediate_agg_result::IntermediateAggregationResults::into_final_result) method.

mod agg_limits;
pub mod agg_req;
mod agg_req_with_accessor;
pub mod agg_result;
pub mod bucket;
mod buf_collector;
mod collector;
mod date;
mod error;
pub mod intermediate_agg_result;
pub mod metric;

mod segment_agg_result;
use std::collections::HashMap;
use std::fmt::Display;

#[cfg(test)]
mod agg_tests;

mod agg_bench;

pub use agg_limits::AggregationLimits;
pub use collector::{
    AggregationCollector, AggregationSegmentCollector, DistributedAggregationCollector,
    DEFAULT_BUCKET_LIMIT,
};
use columnar::{ColumnType, MonotonicallyMappableToU64};
pub(crate) use date::format_date;
pub use error::AggregationError;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

/// Represents an associative array `(key => values)` in a very efficient manner.
#[derive(PartialEq, Serialize, Deserialize)]
pub(crate) struct VecWithNames<T> {
    pub(crate) values: Vec<T>,
    keys: Vec<String>,
}

impl<T: Clone> Clone for VecWithNames<T> {
    fn clone(&self) -> Self {
        Self {
            values: self.values.clone(),
            keys: self.keys.clone(),
        }
    }
}

impl<T> Default for VecWithNames<T> {
    fn default() -> Self {
        Self {
            values: Default::default(),
            keys: Default::default(),
        }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for VecWithNames<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<T> From<HashMap<String, T>> for VecWithNames<T> {
    fn from(map: HashMap<String, T>) -> Self {
        VecWithNames::from_entries(map.into_iter().collect_vec())
    }
}

impl<T> VecWithNames<T> {
    fn from_entries(mut entries: Vec<(String, T)>) -> Self {
        // Sort to ensure order of elements match across multiple instances
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        let mut data = Vec::with_capacity(entries.len());
        let mut data_names = Vec::with_capacity(entries.len());
        for entry in entries {
            data_names.push(entry.0);
            data.push(entry.1);
        }
        VecWithNames {
            values: data,
            keys: data_names,
        }
    }
    fn iter(&self) -> impl Iterator<Item = (&str, &T)> + '_ {
        self.keys().zip(self.values.iter())
    }
    fn keys(&self) -> impl Iterator<Item = &str> + '_ {
        self.keys.iter().map(|key| key.as_str())
    }
    fn values_mut(&mut self) -> impl Iterator<Item = &mut T> + '_ {
        self.values.iter_mut()
    }
    fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
    fn len(&self) -> usize {
        self.keys.len()
    }
    fn get(&self, name: &str) -> Option<&T> {
        self.keys()
            .position(|key| key == name)
            .map(|pos| &self.values[pos])
    }
}

/// The serialized key is used in a `HashMap`.
pub type SerializedKey = String;

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd)]
/// The key to identify a bucket.
#[serde(untagged)]
pub enum Key {
    /// String key
    Str(String),
    /// `f64` key
    F64(f64),
}
impl Eq for Key {}
impl std::hash::Hash for Key {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        match self {
            Key::Str(text) => text.hash(state),
            Key::F64(val) => val.to_bits().hash(state),
        }
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Str(l), Self::Str(r)) => l == r,
            (Self::F64(l), Self::F64(r)) => l == r,
            _ => false,
        }
    }
}

impl Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Key::Str(val) => f.write_str(val),
            Key::F64(val) => f.write_str(&val.to_string()),
        }
    }
}

/// Inverse of `to_fastfield_u64`. Used to convert to `f64` for metrics.
///
/// # Panics
/// Only `u64`, `f64`, `date`, and `i64` are supported.
pub(crate) fn f64_from_fastfield_u64(val: u64, field_type: &ColumnType) -> f64 {
    match field_type {
        ColumnType::U64 => val as f64,
        ColumnType::I64 | ColumnType::DateTime => i64::from_u64(val) as f64,
        ColumnType::F64 => f64::from_u64(val),
        _ => {
            panic!("unexpected type {field_type:?}. This should not happen")
        }
    }
}

/// Converts the `f64` value to fast field value space, which is always u64.
///
/// If the fast field has `u64`, values are stored unchanged as `u64` in the fast field.
///
/// If the fast field has `f64` values are converted and stored to `u64` using a
/// monotonic mapping.
/// A `f64` value of e.g. `2.0` needs to be converted using the same monotonic
/// conversion function, so that the value matches the `u64` value stored in the fast
/// field.
pub(crate) fn f64_to_fastfield_u64(val: f64, field_type: &ColumnType) -> Option<u64> {
    match field_type {
        ColumnType::U64 => Some(val as u64),
        ColumnType::I64 | ColumnType::DateTime => Some((val as i64).to_u64()),
        ColumnType::F64 => Some(val.to_u64()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv6Addr;

    use columnar::DateTime;
    use serde_json::Value;
    use time::OffsetDateTime;

    use super::agg_req::Aggregations;
    use super::segment_agg_result::AggregationLimits;
    use super::*;
    use crate::indexer::NoMergePolicy;
    use crate::query::{AllQuery, TermQuery};
    use crate::schema::{IndexRecordOption, Schema, TextFieldIndexing, FAST, STRING};
    use crate::{Index, Term};

    pub fn get_test_index_with_num_docs(
        merge_segments: bool,
        num_docs: usize,
    ) -> crate::Result<Index> {
        get_test_index_from_values(
            merge_segments,
            &(0..num_docs).map(|el| el as f64).collect::<Vec<f64>>(),
        )
    }

    pub fn exec_request(agg_req: Aggregations, index: &Index) -> crate::Result<Value> {
        exec_request_with_query(agg_req, index, None)
    }
    pub fn exec_request_with_query(
        agg_req: Aggregations,
        index: &Index,
        query: Option<(&str, &str)>,
    ) -> crate::Result<Value> {
        exec_request_with_query_and_memory_limit(agg_req, index, query, Default::default())
    }

    pub fn exec_request_with_query_and_memory_limit(
        agg_req: Aggregations,
        index: &Index,
        query: Option<(&str, &str)>,
        limits: AggregationLimits,
    ) -> crate::Result<Value> {
        let collector = AggregationCollector::from_aggs(agg_req, limits);

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let agg_res = if let Some((field, term)) = query {
            let text_field = reader.searcher().schema().get_field(field).unwrap();

            let term_query = TermQuery::new(
                Term::from_field_text(text_field, term),
                IndexRecordOption::Basic,
            );

            searcher.search(&term_query, &collector)?
        } else {
            searcher.search(&AllQuery, &collector)?
        };

        // Test serialization/deserialization roundtrip
        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;
        Ok(res)
    }

    pub fn get_test_index_from_values(
        merge_segments: bool,
        values: &[f64],
    ) -> crate::Result<Index> {
        // Every value gets its own segment
        let mut segment_and_values = vec![];
        for value in values {
            segment_and_values.push(vec![(*value, value.to_string())]);
        }
        get_test_index_from_values_and_terms(merge_segments, &segment_and_values)
    }

    pub fn get_test_index_from_terms(
        merge_segments: bool,
        values: &[Vec<&str>],
    ) -> crate::Result<Index> {
        // Every value gets its own segment
        let segment_and_values = values
            .iter()
            .map(|terms| {
                terms
                    .iter()
                    .enumerate()
                    .map(|(i, term)| (i as f64, term.to_string()))
                    .collect()
            })
            .collect::<Vec<_>>();
        get_test_index_from_values_and_terms(merge_segments, &segment_and_values)
    }

    pub fn get_test_index_from_values_and_terms(
        merge_segments: bool,
        segment_and_values: &[Vec<(f64, String)>],
    ) -> crate::Result<Index> {
        let mut schema_builder = Schema::builder();
        let text_fieldtype = crate::schema::TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_index_option(IndexRecordOption::Basic)
                    .set_fieldnorms(false),
            )
            .set_fast(None)
            .set_stored();
        let text_field = schema_builder.add_text_field("text", text_fieldtype.clone());
        let text_field_id = schema_builder.add_text_field("text_id", text_fieldtype);
        let string_field_id = schema_builder.add_text_field("string_id", STRING | FAST);
        let score_fieldtype = crate::schema::NumericOptions::default().set_fast();
        let score_field = schema_builder.add_u64_field("score", score_fieldtype.clone());
        let score_field_f64 = schema_builder.add_f64_field("score_f64", score_fieldtype.clone());
        let score_field_i64 = schema_builder.add_i64_field("score_i64", score_fieldtype);
        let fraction_field = schema_builder.add_f64_field(
            "fraction_f64",
            crate::schema::NumericOptions::default().set_fast(),
        );
        let index = Index::create_in_ram(schema_builder.build());
        {
            // let mut index_writer = index.writer_for_tests()?;
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.set_merge_policy(Box::new(NoMergePolicy));
            for values in segment_and_values {
                for (i, term) in values {
                    let i = *i;
                    // writing the segment
                    index_writer.add_document(doc!(
                        text_field => "cool",
                        text_field_id => term.to_string(),
                        string_field_id => term.to_string(),
                        score_field => i as u64,
                        score_field_f64 => i,
                        score_field_i64 => i as i64,
                        fraction_field => i/100.0,
                    ))?;
                }
                index_writer.commit()?;
            }
        }
        if merge_segments {
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            if segment_ids.len() > 1 {
                let mut index_writer = index.writer_for_tests()?;
                index_writer.merge(&segment_ids).wait()?;
                index_writer.wait_merging_threads()?;
            }
        }

        Ok(index)
    }

    pub fn get_test_index_2_segments(merge_segments: bool) -> crate::Result<Index> {
        let mut schema_builder = Schema::builder();
        let text_fieldtype = crate::schema::TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default().set_index_option(IndexRecordOption::WithFreqs),
            )
            .set_fast(None)
            .set_stored();
        let text_field = schema_builder.add_text_field("text", text_fieldtype);
        let date_field = schema_builder.add_date_field("date", FAST);
        schema_builder.add_text_field("dummy_text", STRING);
        let score_fieldtype = crate::schema::NumericOptions::default().set_fast();
        let score_field = schema_builder.add_u64_field("score", score_fieldtype.clone());
        let score_field_f64 = schema_builder.add_f64_field("score_f64", score_fieldtype.clone());
        let ip_addr_field = schema_builder.add_ip_addr_field("ip_addr", FAST);

        let multivalue = crate::schema::NumericOptions::default().set_fast();
        let scores_field_i64 = schema_builder.add_i64_field("scores_i64", multivalue);

        let score_field_i64 = schema_builder.add_i64_field("score_i64", score_fieldtype);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_for_tests()?;
            // writing the segment
            index_writer.add_document(doc!(
                text_field => "cool",
                date_field => DateTime::from_utc(OffsetDateTime::from_unix_timestamp(1_546_300_800).unwrap()),
                score_field => 1u64,
                ip_addr_field => Ipv6Addr::from(1u128),
                score_field_f64 => 1f64,
                score_field_i64 => 1i64,
                scores_field_i64 => 1i64,
                scores_field_i64 => 2i64,
            ))?;
            index_writer.add_document(doc!(
                text_field => "cool",
                date_field => DateTime::from_utc(OffsetDateTime::from_unix_timestamp(1_546_300_800 + 86400).unwrap()),
                score_field => 3u64,
                score_field_f64 => 3f64,
                score_field_i64 => 3i64,
                scores_field_i64 => 5i64,
                scores_field_i64 => 5i64,
            ))?;
            index_writer.add_document(doc!(
                text_field => "cool",
                date_field => DateTime::from_utc(OffsetDateTime::from_unix_timestamp(1_546_300_800 + 86400).unwrap()),
                score_field => 5u64,
                score_field_f64 => 5f64,
                score_field_i64 => 5i64,
            ))?;
            index_writer.add_document(doc!(
                text_field => "nohit",
                date_field => DateTime::from_utc(OffsetDateTime::from_unix_timestamp(1_546_300_800 + 86400).unwrap()),
                score_field => 6u64,
                score_field_f64 => 6f64,
                score_field_i64 => 6i64,
            ))?;
            index_writer.add_document(doc!(
                text_field => "cool",
                date_field => DateTime::from_utc(OffsetDateTime::from_unix_timestamp(1_546_300_800 + 86400).unwrap()),
                score_field => 7u64,
                score_field_f64 => 7f64,
                score_field_i64 => 7i64,
            ))?;
            index_writer.commit()?;
            index_writer.add_document(doc!(
                text_field => "cool",
                date_field => DateTime::from_utc(OffsetDateTime::from_unix_timestamp(1_546_300_800 + 86400).unwrap()),
                score_field => 11u64,
                score_field_f64 => 11f64,
                score_field_i64 => 11i64,
            ))?;
            index_writer.add_document(doc!(
                text_field => "cool",
                date_field => DateTime::from_utc(OffsetDateTime::from_unix_timestamp(1_546_300_800 + 86400 + 86400).unwrap()),
                score_field => 14u64,
                score_field_f64 => 14f64,
                score_field_i64 => 14i64,
            ))?;

            index_writer.add_document(doc!(
                text_field => "cool",
                date_field => DateTime::from_utc(OffsetDateTime::from_unix_timestamp(1_546_300_800 + 86400 + 86400).unwrap()),
                score_field => 44u64,
                score_field_f64 => 44.5f64,
                score_field_i64 => 44i64,
            ))?;

            index_writer.commit()?;

            // no hits segment
            index_writer.add_document(doc!(
                text_field => "nohit",
                date_field => DateTime::from_utc(OffsetDateTime::from_unix_timestamp(1_546_300_800 + 86400 + 86400).unwrap()),
                score_field => 44u64,
                score_field_f64 => 44.5f64,
                score_field_i64 => 44i64,
            ))?;

            index_writer.commit()?;
        }
        if merge_segments {
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            let mut index_writer = index.writer_for_tests()?;
            index_writer.merge(&segment_ids).wait()?;
            index_writer.wait_merging_threads()?;
        }

        Ok(index)
    }
}
