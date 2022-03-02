//! # Aggregations
//!
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
//! # Usage
//!
//!
//! To use aggregations, build an aggregation request by constructing
//! [Aggregations](agg_req::Aggregations).
//! Create an [AggregationCollector] from this request. AggregationCollector implements the
//! `Collector` trait and can be passed as collector into `searcher.search()`.
//!
//! # JSON Format
//! Aggregations request and result structures de/serialize into elasticsearch compatible JSON.
//!
//! ```verbatim
//! let agg_req: Aggregations = serde_json::from_str(json_request_string).unwrap();
//! let collector = AggregationCollector::from_aggs(agg_req);
//! let searcher = reader.searcher();
//! let agg_res = searcher.search(&term_query, &collector).unwrap_err();
//! let json_response_string: String = &serde_json::to_string(&agg_res)?;
//! ```
//! # Limitations
//!
//! Currently aggregations work only on single value fast fields of type u64, f64 and i64.
//!
//! # Example
//! Compute the average metric, by building [agg_req::Aggregations], which is built from an (String,
//! [agg_req::Aggregation]) iterator.
//!
//! ```
//! use tantivy::aggregation::agg_req::{Aggregations, Aggregation, MetricAggregation};
//! use tantivy::aggregation::AggregationCollector;
//! use tantivy::aggregation::metric::AverageAggregation;
//! use tantivy::query::AllQuery;
//! use tantivy::aggregation::agg_result::AggregationResults;
//! use tantivy::IndexReader;
//!
//! # #[allow(dead_code)]
//! fn aggregate_on_index(reader: &IndexReader) {
//!     let agg_req: Aggregations = vec![
//!     (
//!             "average".to_string(),
//!             Aggregation::Metric(MetricAggregation::Average(
//!                 AverageAggregation::from_field_name("score".to_string()),
//!             )),
//!         ),
//!     ]
//!     .into_iter()
//!     .collect();
//!
//!     let collector = AggregationCollector::from_aggs(agg_req);
//!
//!     let searcher = reader.searcher();
//!     let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();
//! }
//! ```
//! # Example JSON
//! Requests are compatible with the elasticsearch json request format.
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
//! let agg_req: Aggregations = serde_json::from_str(elasticsearch_compatible_json_req).unwrap();
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
//! use tantivy::aggregation::agg_req::{Aggregations, Aggregation, BucketAggregation,
//! MetricAggregation, BucketAggregationType};
//! use tantivy::aggregation::metric::AverageAggregation;
//! use tantivy::aggregation::bucket::RangeAggregation;
//! let sub_agg_req_1: Aggregations = vec![(
//!    "average_in_range".to_string(),
//!         Aggregation::Metric(MetricAggregation::Average(
//!             AverageAggregation::from_field_name("score".to_string()),
//!         )),
//! )]
//! .into_iter()
//! .collect();
//!
//! let agg_req_1: Aggregations = vec![
//!     (
//!         "range".to_string(),
//!         Aggregation::Bucket(BucketAggregation {
//!             bucket_agg: BucketAggregationType::Range(RangeAggregation{
//!                 field: "score".to_string(),
//!                 ranges: vec![(3f64..7f64).into(), (7f64..20f64).into()],
//!             }),
//!             sub_aggregation: sub_agg_req_1.clone(),
//!         }),
//!     ),
//! ]
//! .into_iter()
//! .collect();
//! ```
//!
//! # Distributed Aggregation
//! When the data is distributed on different [crate::Index] instances, the
//! [DistributedAggregationCollector] provides functionality to merge data between independent
//! search calls by returning
//! [IntermediateAggregationResults](intermediate_agg_result::IntermediateAggregationResults).
//! IntermediateAggregationResults provides the
//! [merge_fruits](intermediate_agg_result::IntermediateAggregationResults::merge_fruits) method to
//! merge multiple results. The merged result can then be converted into
//! [agg_result::AggregationResults] via the [Into] trait.

pub mod agg_req;
mod agg_req_with_accessor;
pub mod agg_result;
pub mod bucket;
mod collector;
pub mod intermediate_agg_result;
pub mod metric;
mod segment_agg_result;

use std::collections::HashMap;
use std::fmt::Display;

pub use collector::{
    AggregationCollector, AggregationSegmentCollector, DistributedAggregationCollector,
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::fastfield::FastValue;
use crate::schema::Type;

/// Represents an associative array `(key => values)` in a very efficient manner.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct VecWithNames<T: Clone> {
    values: Vec<T>,
    keys: Vec<String>,
}
impl<T: Clone> Default for VecWithNames<T> {
    fn default() -> Self {
        Self {
            values: Default::default(),
            keys: Default::default(),
        }
    }
}

impl<T: Clone + std::fmt::Debug> std::fmt::Debug for VecWithNames<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<T: Clone> From<HashMap<String, T>> for VecWithNames<T> {
    fn from(map: HashMap<String, T>) -> Self {
        VecWithNames::from_entries(map.into_iter().collect_vec())
    }
}

impl<T: Clone> VecWithNames<T> {
    fn from_entries(mut entries: Vec<(String, T)>) -> Self {
        // Sort to ensure order of elements match across multiple instances
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        let mut data = vec![];
        let mut data_names = vec![];
        for entry in entries {
            data_names.push(entry.0);
            data.push(entry.1);
        }
        VecWithNames {
            values: data,
            keys: data_names,
        }
    }
    fn into_iter(self) -> impl Iterator<Item = (String, T)> {
        self.keys.into_iter().zip(self.values.into_iter())
    }
    fn iter(&self) -> impl Iterator<Item = (&str, &T)> + '_ {
        self.keys().zip(self.values.iter())
    }
    fn keys(&self) -> impl Iterator<Item = &str> + '_ {
        self.keys.iter().map(|key| key.as_str())
    }
    fn values(&self) -> impl Iterator<Item = &T> + '_ {
        self.values.iter()
    }
    fn values_mut(&mut self) -> impl Iterator<Item = &mut T> + '_ {
        self.values.iter_mut()
    }
    fn entries(&self) -> impl Iterator<Item = (&str, &T)> + '_ {
        self.keys().zip(self.values.iter())
    }
    fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

/// The serialized key is used in a HashMap.
pub type SerializedKey = String;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, PartialOrd)]
/// The key to identify a bucket.
#[serde(untagged)]
pub enum Key {
    /// String key
    Str(String),
    /// f64 key
    F64(f64),
}

impl Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Key::Str(val) => f.write_str(val),
            Key::F64(val) => f.write_str(&val.to_string()),
        }
    }
}

/// Invert of to_fastfield_u64. Used to convert to f64 for metrics.
///
/// # Panics
/// Only u64, f64, i64 is supported
pub(crate) fn f64_from_fastfield_u64(val: u64, field_type: &Type) -> f64 {
    match field_type {
        Type::U64 => val as f64,
        Type::I64 => i64::from_u64(val) as f64,
        Type::F64 => f64::from_u64(val),
        _ => {
            panic!("unexpected type {:?}. This should not happen", field_type)
        }
    }
}

/// Converts the f64 value to fast field value space.
///
/// If the fast field has u64, values are stored as u64 in the fast field.
/// A f64 value of e.g. 2.0 therefore needs to be converted to 1u64
///
/// If the fast field has f64 values are converted and stored to u64 using a
/// monotonic mapping.
/// A f64 value of e.g. 2.0 needs to be converted using the same monotonic
/// conversion function, so that the value matches the u64 value stored in the fast
/// field.
pub(crate) fn f64_to_fastfield_u64(val: f64, field_type: &Type) -> Option<u64> {
    match field_type {
        Type::U64 => Some(val as u64),
        Type::I64 => Some((val as i64).to_u64()),
        Type::F64 => Some(val.to_u64()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {

    use futures::executor::block_on;
    use serde_json::Value;

    use super::agg_req::{Aggregation, Aggregations, BucketAggregation};
    use super::bucket::RangeAggregation;
    use super::collector::AggregationCollector;
    use super::metric::AverageAggregation;
    use crate::aggregation::agg_req::{BucketAggregationType, MetricAggregation};
    use crate::aggregation::agg_result::AggregationResults;
    use crate::aggregation::intermediate_agg_result::IntermediateAggregationResults;
    use crate::aggregation::segment_agg_result::DOC_BLOCK_SIZE;
    use crate::aggregation::DistributedAggregationCollector;
    use crate::query::{AllQuery, TermQuery};
    use crate::schema::{Cardinality, IndexRecordOption, Schema, TextFieldIndexing};
    use crate::{Index, Term};

    fn get_avg_req(field_name: &str) -> Aggregation {
        Aggregation::Metric(MetricAggregation::Average(
            AverageAggregation::from_field_name(field_name.to_string()),
        ))
    }

    pub fn get_test_index_with_num_docs(
        merge_segments: bool,
        num_docs: usize,
    ) -> crate::Result<Index> {
        let mut schema_builder = Schema::builder();
        let text_fieldtype = crate::schema::TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("default")
                    .set_index_option(IndexRecordOption::WithFreqs),
            )
            .set_stored();
        let text_field = schema_builder.add_text_field("text", text_fieldtype);
        let score_fieldtype =
            crate::schema::NumericOptions::default().set_fast(Cardinality::SingleValue);
        let score_field = schema_builder.add_u64_field("score", score_fieldtype.clone());
        let score_field_f64 = schema_builder.add_f64_field("score_f64", score_fieldtype.clone());
        let score_field_i64 = schema_builder.add_i64_field("score_i64", score_fieldtype);
        let fraction_field = schema_builder.add_f64_field(
            "fraction_f64",
            crate::schema::NumericOptions::default().set_fast(Cardinality::SingleValue),
        );
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_for_tests()?;
            for i in 0..num_docs {
                // writing the segment
                index_writer.add_document(doc!(
                    text_field => "cool",
                    score_field => i as u64,
                    score_field_f64 => i as f64,
                    score_field_i64 => i as i64,
                    fraction_field => i as f64/100.0,
                ))?;
            }

            index_writer.commit()?;
        }
        if merge_segments {
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            let mut index_writer = index.writer_for_tests()?;
            block_on(index_writer.merge(&segment_ids))?;
            index_writer.wait_merging_threads()?;
        }

        Ok(index)
    }

    // *** EVERY BUCKET-TYPE SHOULD BE TESTED HERE ***
    fn test_aggregation_flushing(
        merge_segments: bool,
        use_distributed_collector: bool,
    ) -> crate::Result<()> {
        let index = get_test_index_with_num_docs(merge_segments, 300)?;

        let reader = index.reader()?;
        let text_field = reader.searcher().schema().get_field("text").unwrap();

        let term_query = TermQuery::new(
            Term::from_field_text(text_field, "cool"),
            IndexRecordOption::Basic,
        );

        assert_eq!(DOC_BLOCK_SIZE, 256);
        // In the tree we cache Documents of DOC_BLOCK_SIZE, before passing them down as one block.
        //
        // Build a request so that on the first level we have one full cache, which is then flushed.
        // The same cache should have some residue docs at the end, which are flushed (Range 0-266)
        // -> 266 docs
        //
        // The second level should also have some residue docs in the cache that are flushed at the
        // end.
        //
        // A second bucket on the first level should have the cache unfilled

        // let elasticsearch_compatible_json_req = r#"
        let elasticsearch_compatible_json_req = r#"
        {
        "bucketsL1": {
            "range": {
                "field": "score",
                "ranges": [ { "to": 3.0 }, { "from": 3.0, "to": 266.0 }, { "from": 266.0 } ]
            },
            "aggs": {
                "bucketsL2": {
                    "range": {
                        "field": "score",
                        "ranges": [ { "to": 100.0 }, { "from": 100.0, "to": 266.0 }, { "from": 266.0 } ]
                    }
                }
            }
        }
        }
        "#;

        let agg_req: Aggregations =
            serde_json::from_str(elasticsearch_compatible_json_req).unwrap();

        let agg_res: AggregationResults = if use_distributed_collector {
            let collector = DistributedAggregationCollector::from_aggs(agg_req);

            let searcher = reader.searcher();
            searcher.search(&term_query, &collector).unwrap().into()
        } else {
            let collector = AggregationCollector::from_aggs(agg_req);

            let searcher = reader.searcher();
            searcher.search(&term_query, &collector).unwrap()
        };

        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;

        assert_eq!(res["bucketsL1"]["buckets"][0]["doc_count"], 3);
        assert_eq!(
            res["bucketsL1"]["buckets"][0]["bucketsL2"]["buckets"][0]["doc_count"],
            3
        );
        assert_eq!(res["bucketsL1"]["buckets"][1]["key"], "3-266");
        assert_eq!(res["bucketsL1"]["buckets"][1]["doc_count"], 266 - 3);
        assert_eq!(
            res["bucketsL1"]["buckets"][1]["bucketsL2"]["buckets"][0]["doc_count"],
            97
        );
        assert_eq!(
            res["bucketsL1"]["buckets"][1]["bucketsL2"]["buckets"][1]["doc_count"],
            166
        );
        assert_eq!(
            res["bucketsL1"]["buckets"][1]["bucketsL2"]["buckets"][2]["doc_count"],
            0
        );
        assert_eq!(
            res["bucketsL1"]["buckets"][2]["bucketsL2"]["buckets"][2]["doc_count"],
            300 - 266
        );
        assert_eq!(res["bucketsL1"]["buckets"][2]["doc_count"], 300 - 266);

        Ok(())
    }

    #[test]
    fn test_aggregation_flushing_variants() {
        test_aggregation_flushing(false, false).unwrap();
        test_aggregation_flushing(false, true).unwrap();
        test_aggregation_flushing(true, false).unwrap();
        test_aggregation_flushing(true, true).unwrap();
    }

    pub fn get_test_index_2_segments(merge_segments: bool) -> crate::Result<Index> {
        let mut schema_builder = Schema::builder();
        let text_fieldtype = crate::schema::TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("default")
                    .set_index_option(IndexRecordOption::WithFreqs),
            )
            .set_stored();
        let text_field = schema_builder.add_text_field("text", text_fieldtype);
        let score_fieldtype =
            crate::schema::NumericOptions::default().set_fast(Cardinality::SingleValue);
        let score_field = schema_builder.add_u64_field("score", score_fieldtype.clone());
        let score_field_f64 = schema_builder.add_f64_field("score_f64", score_fieldtype.clone());

        let multivalue =
            crate::schema::NumericOptions::default().set_fast(Cardinality::MultiValues);
        let scores_field_i64 = schema_builder.add_i64_field("scores_i64", multivalue);

        let score_field_i64 = schema_builder.add_i64_field("score_i64", score_fieldtype);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_for_tests()?;
            // writing the segment
            index_writer.add_document(doc!(
                text_field => "cool",
                score_field => 1u64,
                score_field_f64 => 1f64,
                score_field_i64 => 1i64,
                scores_field_i64 => 1i64,
                scores_field_i64 => 2i64,
            ))?;
            index_writer.add_document(doc!(
                text_field => "cool",
                score_field => 3u64,
                score_field_f64 => 3f64,
                score_field_i64 => 3i64,
                scores_field_i64 => 5i64,
                scores_field_i64 => 5i64,
            ))?;
            index_writer.add_document(doc!(
                text_field => "cool",
                score_field => 5u64,
                score_field_f64 => 5f64,
                score_field_i64 => 5i64,
            ))?;
            index_writer.add_document(doc!(
                text_field => "nohit",
                score_field => 6u64,
                score_field_f64 => 6f64,
                score_field_i64 => 6i64,
            ))?;
            index_writer.add_document(doc!(
                text_field => "cool",
                score_field => 7u64,
                score_field_f64 => 7f64,
                score_field_i64 => 7i64,
            ))?;
            index_writer.commit()?;
            index_writer.add_document(doc!(
                text_field => "cool",
                score_field => 11u64,
                score_field_f64 => 11f64,
                score_field_i64 => 11i64,
            ))?;
            index_writer.add_document(doc!(
                text_field => "cool",
                score_field => 14u64,
                score_field_f64 => 14f64,
                score_field_i64 => 14i64,
            ))?;

            index_writer.add_document(doc!(
                text_field => "cool",
                score_field => 44u64,
                score_field_f64 => 44.5f64,
                score_field_i64 => 44i64,
            ))?;

            index_writer.commit()?;

            // no hits segment
            index_writer.add_document(doc!(
                text_field => "nohit",
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
            block_on(index_writer.merge(&segment_ids))?;
            index_writer.wait_merging_threads()?;
        }

        Ok(index)
    }

    #[test]
    fn test_aggregation_level1() -> crate::Result<()> {
        let index = get_test_index_2_segments(true)?;

        let reader = index.reader()?;
        let text_field = reader.searcher().schema().get_field("text").unwrap();

        let term_query = TermQuery::new(
            Term::from_field_text(text_field, "cool"),
            IndexRecordOption::Basic,
        );

        let agg_req_1: Aggregations = vec![
            ("average_i64".to_string(), get_avg_req("score_i64")),
            ("average_f64".to_string(), get_avg_req("score_f64")),
            ("average".to_string(), get_avg_req("score")),
            (
                "range".to_string(),
                Aggregation::Bucket(BucketAggregation {
                    bucket_agg: BucketAggregationType::Range(RangeAggregation {
                        field: "score".to_string(),
                        ranges: vec![(3f64..7f64).into(), (7f64..20f64).into()],
                    }),
                    sub_aggregation: Default::default(),
                }),
            ),
            (
                "rangef64".to_string(),
                Aggregation::Bucket(BucketAggregation {
                    bucket_agg: BucketAggregationType::Range(RangeAggregation {
                        field: "score_f64".to_string(),
                        ranges: vec![(3f64..7f64).into(), (7f64..20f64).into()],
                    }),
                    sub_aggregation: Default::default(),
                }),
            ),
            (
                "rangei64".to_string(),
                Aggregation::Bucket(BucketAggregation {
                    bucket_agg: BucketAggregationType::Range(RangeAggregation {
                        field: "score_i64".to_string(),
                        ranges: vec![(3f64..7f64).into(), (7f64..20f64).into()],
                    }),
                    sub_aggregation: Default::default(),
                }),
            ),
        ]
        .into_iter()
        .collect();

        let collector = AggregationCollector::from_aggs(agg_req_1);

        let searcher = reader.searcher();
        let agg_res: AggregationResults = searcher.search(&term_query, &collector).unwrap();

        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;
        assert_eq!(res["average"]["value"], 12.142857142857142);
        assert_eq!(res["average_f64"]["value"], 12.214285714285714);
        assert_eq!(res["average_i64"]["value"], 12.142857142857142);
        assert_eq!(
            res["range"]["buckets"],
            json!(
            [
            {
              "key": "*-3",
              "doc_count": 1,
              "to": 3.0
            },
            {
              "key": "3-7",
              "doc_count": 2,
              "from": 3.0,
              "to": 7.0
            },
            {
              "key": "7-20",
              "doc_count": 3,
              "from": 7.0,
              "to": 20.0
            },
            {
              "key": "20-*",
              "doc_count": 1,
              "from": 20.0
            }
            ])
        );

        Ok(())
    }

    fn test_aggregation_level2(
        merge_segments: bool,
        use_distributed_collector: bool,
        use_elastic_json_req: bool,
    ) -> crate::Result<()> {
        let index = get_test_index_2_segments(merge_segments)?;

        let reader = index.reader()?;
        let text_field = reader.searcher().schema().get_field("text").unwrap();

        let term_query = TermQuery::new(
            Term::from_field_text(text_field, "cool"),
            IndexRecordOption::Basic,
        );

        let query_with_no_hits = TermQuery::new(
            Term::from_field_text(text_field, "thistermdoesnotexist"),
            IndexRecordOption::Basic,
        );

        let sub_agg_req: Aggregations =
            vec![("average_in_range".to_string(), get_avg_req("score"))]
                .into_iter()
                .collect();
        let agg_req: Aggregations = if use_elastic_json_req {
            let elasticsearch_compatible_json_req = r#"
{
  "rangef64": {
    "range": {
      "field": "score_f64",
      "ranges": [
        { "to": 3.0 },
        { "from": 3.0, "to": 7.0 },
        { "from": 7.0, "to": 19.0 },
        { "from": 19.0, "to": 20.0 },
        { "from": 20.0 }
      ]
    },
    "aggs": {
      "average_in_range": { "avg": { "field": "score" } }
    }
  },
  "rangei64": {
    "range": {
      "field": "score_i64",
      "ranges": [
        { "to": 3.0 },
        { "from": 3.0, "to": 7.0 },
        { "from": 7.0, "to": 19.0 },
        { "from": 19.0, "to": 20.0 },
        { "from": 20.0 }
      ]
    },
    "aggs": {
      "average_in_range": { "avg": { "field": "score" } }
    }
  },
  "average": {
    "avg": { "field": "score" }
  },
  "range": {
    "range": {
      "field": "score",
      "ranges": [
        { "to": 3.0 },
        { "from": 3.0, "to": 7.0 },
        { "from": 7.0, "to": 19.0 },
        { "from": 19.0, "to": 20.0 },
        { "from": 20.0 }
      ]
    },
    "aggs": {
      "average_in_range": { "avg": { "field": "score" } }
    }
  }
}
"#;
            let value: Aggregations =
                serde_json::from_str(elasticsearch_compatible_json_req).unwrap();
            value
        } else {
            let agg_req: Aggregations = vec![
                ("average".to_string(), get_avg_req("score")),
                (
                    "range".to_string(),
                    Aggregation::Bucket(BucketAggregation {
                        bucket_agg: BucketAggregationType::Range(RangeAggregation {
                            field: "score".to_string(),
                            ranges: vec![
                                (3f64..7f64).into(),
                                (7f64..19f64).into(),
                                (19f64..20f64).into(),
                            ],
                        }),
                        sub_aggregation: sub_agg_req.clone(),
                    }),
                ),
                (
                    "rangef64".to_string(),
                    Aggregation::Bucket(BucketAggregation {
                        bucket_agg: BucketAggregationType::Range(RangeAggregation {
                            field: "score_f64".to_string(),
                            ranges: vec![
                                (3f64..7f64).into(),
                                (7f64..19f64).into(),
                                (19f64..20f64).into(),
                            ],
                        }),
                        sub_aggregation: sub_agg_req.clone(),
                    }),
                ),
                (
                    "rangei64".to_string(),
                    Aggregation::Bucket(BucketAggregation {
                        bucket_agg: BucketAggregationType::Range(RangeAggregation {
                            field: "score_i64".to_string(),
                            ranges: vec![
                                (3f64..7f64).into(),
                                (7f64..19f64).into(),
                                (19f64..20f64).into(),
                            ],
                        }),
                        sub_aggregation: sub_agg_req,
                    }),
                ),
            ]
            .into_iter()
            .collect();
            agg_req
        };

        let agg_res: AggregationResults = if use_distributed_collector {
            let collector = DistributedAggregationCollector::from_aggs(agg_req.clone());

            let searcher = reader.searcher();
            let res = searcher.search(&term_query, &collector).unwrap();
            // Test de/serialization roundtrip on intermediate_agg_result
            let res: IntermediateAggregationResults =
                serde_json::from_str(&serde_json::to_string(&res).unwrap()).unwrap();
            res.into()
        } else {
            let collector = AggregationCollector::from_aggs(agg_req.clone());

            let searcher = reader.searcher();
            searcher.search(&term_query, &collector).unwrap()
        };

        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;

        assert_eq!(res["range"]["buckets"][1]["key"], "3-7");
        assert_eq!(res["range"]["buckets"][1]["doc_count"], 2u64);
        assert_eq!(res["rangef64"]["buckets"][1]["doc_count"], 2u64);
        assert_eq!(res["rangei64"]["buckets"][1]["doc_count"], 2u64);

        assert_eq!(res["average"]["value"], 12.142857142857142f64);
        assert_eq!(res["range"]["buckets"][2]["key"], "7-19");
        assert_eq!(res["range"]["buckets"][2]["doc_count"], 3u64);
        assert_eq!(res["rangef64"]["buckets"][2]["doc_count"], 3u64);
        assert_eq!(res["rangei64"]["buckets"][2]["doc_count"], 3u64);
        assert_eq!(res["rangei64"]["buckets"][5], serde_json::Value::Null);

        assert_eq!(res["range"]["buckets"][4]["key"], "20-*");
        assert_eq!(res["range"]["buckets"][4]["doc_count"], 1u64);
        assert_eq!(res["rangef64"]["buckets"][4]["doc_count"], 1u64);
        assert_eq!(res["rangei64"]["buckets"][4]["doc_count"], 1u64);

        assert_eq!(res["range"]["buckets"][3]["key"], "19-20");
        assert_eq!(res["range"]["buckets"][3]["doc_count"], 0u64);
        assert_eq!(res["rangef64"]["buckets"][3]["doc_count"], 0u64);
        assert_eq!(res["rangei64"]["buckets"][3]["doc_count"], 0u64);

        assert_eq!(
            res["range"]["buckets"][3]["average_in_range"]["value"],
            serde_json::Value::Null
        );

        assert_eq!(
            res["range"]["buckets"][4]["average_in_range"]["value"],
            44.0f64
        );
        assert_eq!(
            res["rangef64"]["buckets"][4]["average_in_range"]["value"],
            44.0f64
        );
        assert_eq!(
            res["rangei64"]["buckets"][4]["average_in_range"]["value"],
            44.0f64
        );

        assert_eq!(
            res["range"]["7-19"]["average_in_range"]["value"],
            res["rangef64"]["7-19"]["average_in_range"]["value"]
        );
        assert_eq!(
            res["range"]["7-19"]["average_in_range"]["value"],
            res["rangei64"]["7-19"]["average_in_range"]["value"]
        );

        // Test empty result set
        let collector = AggregationCollector::from_aggs(agg_req);
        let searcher = reader.searcher();
        searcher.search(&query_with_no_hits, &collector).unwrap();

        Ok(())
    }

    #[test]
    fn test_aggregation_level2_multi_segments() -> crate::Result<()> {
        test_aggregation_level2(false, false, false)
    }

    #[test]
    fn test_aggregation_level2_single_segment() -> crate::Result<()> {
        test_aggregation_level2(true, false, false)
    }

    #[test]
    fn test_aggregation_level2_multi_segments_distributed_collector() -> crate::Result<()> {
        test_aggregation_level2(false, true, false)
    }

    #[test]
    fn test_aggregation_level2_single_segment_distributed_collector() -> crate::Result<()> {
        test_aggregation_level2(true, true, false)
    }

    #[test]
    fn test_aggregation_level2_multi_segments_use_json() -> crate::Result<()> {
        test_aggregation_level2(false, false, true)
    }

    #[test]
    fn test_aggregation_level2_single_segment_use_json() -> crate::Result<()> {
        test_aggregation_level2(true, false, true)
    }

    #[test]
    fn test_aggregation_level2_multi_segments_distributed_collector_use_json() -> crate::Result<()>
    {
        test_aggregation_level2(false, true, true)
    }

    #[test]
    fn test_aggregation_level2_single_segment_distributed_collector_use_json() -> crate::Result<()>
    {
        test_aggregation_level2(true, true, true)
    }

    #[test]
    fn test_aggregation_invalid_requests() -> crate::Result<()> {
        let index = get_test_index_2_segments(false)?;

        let reader = index.reader()?;

        let avg_on_field = |field_name: &str| {
            let agg_req_1: Aggregations = vec![(
                "average".to_string(),
                Aggregation::Metric(MetricAggregation::Average(
                    AverageAggregation::from_field_name(field_name.to_string()),
                )),
            )]
            .into_iter()
            .collect();

            let collector = AggregationCollector::from_aggs(agg_req_1);

            let searcher = reader.searcher();

            searcher.search(&AllQuery, &collector).unwrap_err()
        };

        let agg_res = avg_on_field("text");
        assert_eq!(
            format!("{:?}", agg_res),
            r#"InvalidArgument("Only single value fast fields of type f64, u64, i64 are supported, but got Str ")"#
        );

        let agg_res = avg_on_field("not_exist_field");
        assert_eq!(
            format!("{:?}", agg_res),
            r#"FieldNotFound("not_exist_field")"#
        );

        let agg_res = avg_on_field("scores_i64");
        assert_eq!(
            format!("{:?}", agg_res),
            r#"InvalidArgument("Invalid field type in aggregation I64, only Cardinality::SingleValue supported")"#
        );

        Ok(())
    }

    #[cfg(all(test, feature = "unstable"))]
    mod bench {

        use rand::{thread_rng, Rng};
        use test::{self, Bencher};

        use super::*;
        use crate::aggregation::metric::StatsAggregation;
        use crate::query::AllQuery;

        fn get_test_index_bench(merge_segments: bool) -> crate::Result<Index> {
            let mut schema_builder = Schema::builder();
            let text_fieldtype = crate::schema::TextOptions::default()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer("default")
                        .set_index_option(IndexRecordOption::WithFreqs),
                )
                .set_stored();
            let text_field = schema_builder.add_text_field("text", text_fieldtype);
            let score_fieldtype =
                crate::schema::NumericOptions::default().set_fast(Cardinality::SingleValue);
            let score_field = schema_builder.add_u64_field("score", score_fieldtype.clone());
            let score_field_f64 =
                schema_builder.add_f64_field("score_f64", score_fieldtype.clone());
            let score_field_i64 = schema_builder.add_i64_field("score_i64", score_fieldtype);
            let index = Index::create_from_tempdir(schema_builder.build())?;
            {
                let mut rng = thread_rng();
                let mut index_writer = index.writer_for_tests()?;
                // writing the segment
                for _ in 0..1_000_000 {
                    let val: f64 = rng.gen_range(0.0..1_000_000.0);
                    index_writer.add_document(doc!(
                        text_field => "cool",
                        score_field => val as u64,
                        score_field_f64 => val as f64,
                        score_field_i64 => val as i64,
                    ))?;
                }
                index_writer.commit()?;
            }
            if merge_segments {
                let segment_ids = index
                    .searchable_segment_ids()
                    .expect("Searchable segments failed.");
                let mut index_writer = index.writer_for_tests()?;
                block_on(index_writer.merge(&segment_ids))?;
                index_writer.wait_merging_threads()?;
            }

            Ok(index)
        }

        #[bench]
        fn bench_aggregation_average_u64(b: &mut Bencher) {
            let index = get_test_index_bench(false).unwrap();
            let reader = index.reader().unwrap();
            let text_field = reader.searcher().schema().get_field("text").unwrap();

            b.iter(|| {
                let term_query = TermQuery::new(
                    Term::from_field_text(text_field, "cool"),
                    IndexRecordOption::Basic,
                );

                let agg_req_1: Aggregations = vec![(
                    "average".to_string(),
                    Aggregation::Metric(MetricAggregation::Average(
                        AverageAggregation::from_field_name("score".to_string()),
                    )),
                )]
                .into_iter()
                .collect();

                let collector = AggregationCollector::from_aggs(agg_req_1);

                let searcher = reader.searcher();
                let agg_res: AggregationResults =
                    searcher.search(&term_query, &collector).unwrap().into();

                agg_res
            });
        }

        #[bench]
        fn bench_aggregation_stats_f64(b: &mut Bencher) {
            let index = get_test_index_bench(false).unwrap();
            let reader = index.reader().unwrap();
            let text_field = reader.searcher().schema().get_field("text").unwrap();

            b.iter(|| {
                let term_query = TermQuery::new(
                    Term::from_field_text(text_field, "cool"),
                    IndexRecordOption::Basic,
                );

                let agg_req_1: Aggregations = vec![(
                    "average_f64".to_string(),
                    Aggregation::Metric(MetricAggregation::Stats(
                        StatsAggregation::from_field_name("score_f64".to_string()),
                    )),
                )]
                .into_iter()
                .collect();

                let collector = AggregationCollector::from_aggs(agg_req_1);

                let searcher = reader.searcher();
                let agg_res: AggregationResults =
                    searcher.search(&term_query, &collector).unwrap().into();

                agg_res
            });
        }

        #[bench]
        fn bench_aggregation_average_f64(b: &mut Bencher) {
            let index = get_test_index_bench(false).unwrap();
            let reader = index.reader().unwrap();
            let text_field = reader.searcher().schema().get_field("text").unwrap();

            b.iter(|| {
                let term_query = TermQuery::new(
                    Term::from_field_text(text_field, "cool"),
                    IndexRecordOption::Basic,
                );

                let agg_req_1: Aggregations = vec![(
                    "average_f64".to_string(),
                    Aggregation::Metric(MetricAggregation::Average(
                        AverageAggregation::from_field_name("score_f64".to_string()),
                    )),
                )]
                .into_iter()
                .collect();

                let collector = AggregationCollector::from_aggs(agg_req_1);

                let searcher = reader.searcher();
                let agg_res: AggregationResults =
                    searcher.search(&term_query, &collector).unwrap().into();

                agg_res
            });
        }

        #[bench]
        fn bench_aggregation_average_u64_and_f64(b: &mut Bencher) {
            let index = get_test_index_bench(false).unwrap();
            let reader = index.reader().unwrap();
            let text_field = reader.searcher().schema().get_field("text").unwrap();

            b.iter(|| {
                let term_query = TermQuery::new(
                    Term::from_field_text(text_field, "cool"),
                    IndexRecordOption::Basic,
                );

                let agg_req_1: Aggregations = vec![
                    (
                        "average_f64".to_string(),
                        Aggregation::Metric(MetricAggregation::Average(
                            AverageAggregation::from_field_name("score_f64".to_string()),
                        )),
                    ),
                    (
                        "average".to_string(),
                        Aggregation::Metric(MetricAggregation::Average(
                            AverageAggregation::from_field_name("score".to_string()),
                        )),
                    ),
                ]
                .into_iter()
                .collect();

                let collector = AggregationCollector::from_aggs(agg_req_1);

                let searcher = reader.searcher();
                let agg_res: AggregationResults =
                    searcher.search(&term_query, &collector).unwrap().into();

                agg_res
            });
        }

        #[bench]
        fn bench_aggregation_range_only(b: &mut Bencher) {
            let index = get_test_index_bench(false).unwrap();
            let reader = index.reader().unwrap();

            b.iter(|| {
                let agg_req_1: Aggregations = vec![(
                    "rangef64".to_string(),
                    Aggregation::Bucket(BucketAggregation {
                        bucket_agg: BucketAggregationType::Range(RangeAggregation {
                            field: "score_f64".to_string(),
                            ranges: vec![
                                (3f64..7000f64).into(),
                                (7000f64..20000f64).into(),
                                (20000f64..30000f64).into(),
                                (30000f64..40000f64).into(),
                                (40000f64..50000f64).into(),
                                (50000f64..60000f64).into(),
                            ],
                        }),
                        sub_aggregation: Default::default(),
                    }),
                )]
                .into_iter()
                .collect();

                let collector = AggregationCollector::from_aggs(agg_req_1);

                let searcher = reader.searcher();
                let agg_res: AggregationResults =
                    searcher.search(&AllQuery, &collector).unwrap().into();

                agg_res
            });
        }

        #[bench]
        fn bench_aggregation_sub_tree(b: &mut Bencher) {
            let index = get_test_index_bench(false).unwrap();
            let reader = index.reader().unwrap();
            let text_field = reader.searcher().schema().get_field("text").unwrap();

            b.iter(|| {
                let term_query = TermQuery::new(
                    Term::from_field_text(text_field, "cool"),
                    IndexRecordOption::Basic,
                );

                let sub_agg_req_1: Aggregations = vec![(
                    "average_in_range".to_string(),
                    Aggregation::Metric(MetricAggregation::Average(
                        AverageAggregation::from_field_name("score".to_string()),
                    )),
                )]
                .into_iter()
                .collect();

                let agg_req_1: Aggregations = vec![
                    (
                        "average".to_string(),
                        Aggregation::Metric(MetricAggregation::Average(
                            AverageAggregation::from_field_name("score".to_string()),
                        )),
                    ),
                    (
                        "rangef64".to_string(),
                        Aggregation::Bucket(BucketAggregation {
                            bucket_agg: BucketAggregationType::Range(RangeAggregation {
                                field: "score_f64".to_string(),
                                ranges: vec![
                                    (3f64..7000f64).into(),
                                    (7000f64..20000f64).into(),
                                    (20000f64..60000f64).into(),
                                ],
                            }),
                            sub_aggregation: sub_agg_req_1.clone(),
                        }),
                    ),
                ]
                .into_iter()
                .collect();

                let collector = AggregationCollector::from_aggs(agg_req_1);

                let searcher = reader.searcher();
                let agg_res: AggregationResults =
                    searcher.search(&term_query, &collector).unwrap().into();

                agg_res
            });
        }
    }
}
