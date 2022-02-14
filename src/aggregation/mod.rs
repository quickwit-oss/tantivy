//! # Aggregations
//!
//!
//! Aggregation summarizes your data as statistics on buckets or metrics.
//!
//! Aggregations can provide answer to questions like:
//! - What is the average price of all sold articles?
//! - How many errors with status code 500 do we have per day?
//! - What is the average listing price of cars grouped by color?
//!
//! # Usage
//!
//! To use aggregations, build an aggregation request by constructing [agg_req::Aggregations].
//! Create an [AggregationCollector] from this request. AggregationCollector implements the
//! `Collector` trait and can be passed as collector into `searcher.search()`.
//!
//! # Example
//! Compute the average metric, by building [agg_req::Aggregations], which is built from an (String,
//! [agg_req::Aggregation]) iterator.
//!
//! ```
//! use tantivy::aggregation::agg_req::{Aggregations, Aggregation, BucketAggregation};
//! use tantivy::query::AllQuery;
//! let agg_req: Aggregations = vec![
//! (
//!         "average".to_string(),
//!         Aggregation::Metric(MetricAggregation::Average(
//!             AverageAggregation::from_field_name("score".to_string()),
//!         )),
//!     ),
//! ]
//! .into_iter()
//! .collect();
//!
//! let collector = AggregationCollector::from_aggs(agg_req);
//!
//! let searcher = reader.searcher();
//! let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();
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
//! BucketAggregationType};
//! let sub_agg_req_1: Aggregations = vec![(
//!    "average_in_range".to_string(),
//!    Aggregation::Metric(MetricAggregation::Average {
//!        field_name: "score".to_string(),
//!     }),
//! )]
//! .into_iter()
//! .collect();
//!
//! let agg_req_1: Aggregations = vec![
//!     (
//!         "range".to_string(),
//!         Aggregation::Bucket(BucketAggregation {
//!             bucket_agg: BucketAggregationType::Range(RangeAggregation{
//!                 field_name: "score".to_string(),
//!                 buckets: vec![(3f64..7f64), (7f64..20f64)],
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

pub use collector::{AggregationCollector, DistributedAggregationCollector};
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
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Deserialize, Ord, PartialOrd)]
/// The key to identify a bucket.
pub enum Key {
    /// String key
    Str(String),
    /// u64 key
    U64(u64),
    /// i64 key
    I64(i64),
}

impl Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Key::Str(val) => f.write_str(val),
            Key::U64(val) => f.write_str(&val.to_string()),
            Key::I64(val) => f.write_str(&val.to_string()),
        }
    }
}

impl Serialize for Key {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        serializer.serialize_str(&self.to_string())
    }
}

/// Invert of to_fastfield_u64
pub(crate) fn f64_from_fastfield_u64(val: u64, field_type: &Type) -> f64 {
    match field_type {
        Type::U64 => val as f64,
        Type::I64 => i64::from_u64(val) as f64,
        Type::F64 => f64::from_u64(val),
        Type::Date | Type::Str | Type::Facet | Type::Bytes => unimplemented!(),
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
pub(crate) fn f64_to_fastfield_u64(val: f64, field_type: &Type) -> u64 {
    match field_type {
        Type::U64 => val as u64,
        Type::I64 => (val as i64).to_u64(),
        Type::F64 => val.to_u64(),
        Type::Date | Type::Str | Type::Facet | Type::Bytes => unimplemented!(),
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
    use crate::aggregation::DistributedAggregationCollector;
    use crate::query::TermQuery;
    use crate::schema::{Cardinality, IndexRecordOption, Schema, TextFieldIndexing};
    use crate::{Index, Term};

    fn get_avg_req(field_name: &str) -> Aggregation {
        Aggregation::Metric(MetricAggregation::Average(
            AverageAggregation::from_field_name(field_name.to_string()),
        ))
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
            crate::schema::IntOptions::default().set_fast(Cardinality::SingleValue);
        let score_field = schema_builder.add_u64_field("score", score_fieldtype.clone());
        let score_field_f64 = schema_builder.add_f64_field("score_f64", score_fieldtype.clone());
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
            ))?;
            index_writer.add_document(doc!(
                text_field => "cool",
                score_field => 3u64,
                score_field_f64 => 3f64,
                score_field_i64 => 3i64,
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
        let index = get_test_index_2_segments(false)?;

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
                        field_name: "score".to_string(),
                        buckets: vec![(3f64..7f64), (7f64..20f64)],
                    }),
                    sub_aggregation: Default::default(),
                }),
            ),
            (
                "rangef64".to_string(),
                Aggregation::Bucket(BucketAggregation {
                    bucket_agg: BucketAggregationType::Range(RangeAggregation {
                        field_name: "score_f64".to_string(),
                        buckets: vec![(3f64..7f64), (7f64..20f64)],
                    }),
                    sub_aggregation: Default::default(),
                }),
            ),
            (
                "rangei64".to_string(),
                Aggregation::Bucket(BucketAggregation {
                    bucket_agg: BucketAggregationType::Range(RangeAggregation {
                        field_name: "score_i64".to_string(),
                        buckets: vec![(3f64..7f64), (7f64..20f64)],
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
            json!([
                {
                  "key": "7-20",
                  "doc_count": 3
                },
                {
                  "key": "3-7",
                  "doc_count": 2
                },
                {
                  "key": "*-3",
                  "doc_count": 1
                },
                {
                  "key": "20-*",
                  "doc_count": 1
                }
            ])
        );

        Ok(())
    }

    fn test_aggregation_level2(
        merge_segments: bool,
        use_distributed_collector: bool,
    ) -> crate::Result<()> {
        let index = get_test_index_2_segments(merge_segments)?;

        let reader = index.reader()?;
        let text_field = reader.searcher().schema().get_field("text").unwrap();

        let term_query = TermQuery::new(
            Term::from_field_text(text_field, "cool"),
            IndexRecordOption::Basic,
        );

        let sub_agg_req_1: Aggregations =
            vec![("average_in_range".to_string(), get_avg_req("score"))]
                .into_iter()
                .collect();

        let agg_req_1: Aggregations = vec![
            ("average".to_string(), get_avg_req("score")),
            (
                "range".to_string(),
                Aggregation::Bucket(BucketAggregation {
                    bucket_agg: BucketAggregationType::Range(RangeAggregation {
                        field_name: "score".to_string(),
                        buckets: vec![(3f64..7f64), (7f64..20f64)],
                    }),
                    sub_aggregation: sub_agg_req_1.clone(),
                }),
            ),
            (
                "rangef64".to_string(),
                Aggregation::Bucket(BucketAggregation {
                    bucket_agg: BucketAggregationType::Range(RangeAggregation {
                        field_name: "score_f64".to_string(),
                        buckets: vec![(3f64..7f64), (7f64..20f64)],
                    }),
                    sub_aggregation: sub_agg_req_1.clone(),
                }),
            ),
            (
                "rangei64".to_string(),
                Aggregation::Bucket(BucketAggregation {
                    bucket_agg: BucketAggregationType::Range(RangeAggregation {
                        field_name: "score_i64".to_string(),
                        buckets: vec![(3f64..7f64), (7f64..20f64)],
                    }),
                    sub_aggregation: sub_agg_req_1,
                }),
            ),
        ]
        .into_iter()
        .collect();

        let agg_res: AggregationResults = if use_distributed_collector {
            let collector = DistributedAggregationCollector::from_aggs(agg_req_1);

            let searcher = reader.searcher();
            searcher.search(&term_query, &collector).unwrap().into()
        } else {
            let collector = AggregationCollector::from_aggs(agg_req_1);

            let searcher = reader.searcher();
            searcher.search(&term_query, &collector).unwrap().into()
        };

        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;

        assert_eq!(res["average"]["value"], 12.142857142857142f64);
        assert_eq!(res["range"]["buckets"][0]["key"], "7-20");
        assert_eq!(res["range"]["buckets"][0]["doc_count"], 3u64);
        assert_eq!(res["rangef64"]["buckets"][0]["doc_count"], 3u64);
        assert_eq!(res["rangei64"]["buckets"][0]["doc_count"], 3u64);
        assert_eq!(res["rangei64"]["buckets"][4], serde_json::Value::Null);

        assert_eq!(res["range"]["buckets"][1]["key"], "3-7");
        assert_eq!(res["range"]["buckets"][1]["doc_count"], 2u64);
        assert_eq!(res["rangef64"]["buckets"][1]["doc_count"], 2u64);
        assert_eq!(res["rangei64"]["buckets"][1]["doc_count"], 2u64);

        assert_eq!(res["range"]["buckets"][3]["key"], "20-*");
        assert_eq!(res["range"]["buckets"][3]["doc_count"], 1u64);
        assert_eq!(res["rangef64"]["buckets"][3]["doc_count"], 1u64);
        assert_eq!(res["rangei64"]["buckets"][3]["doc_count"], 1u64);

        assert_eq!(
            res["range"]["buckets"][3]["average_in_range"]["value"],
            44.0f64
        );
        assert_eq!(
            res["rangef64"]["buckets"][3]["average_in_range"]["value"],
            44.0f64
        );
        assert_eq!(
            res["rangei64"]["buckets"][3]["average_in_range"]["value"],
            44.0f64
        );

        assert_eq!(
            res["range"]["7-20"]["average_in_range"]["value"],
            res["rangef64"]["7-20"]["average_in_range"]["value"]
        );
        assert_eq!(
            res["range"]["7-20"]["average_in_range"]["value"],
            res["rangei64"]["7-20"]["average_in_range"]["value"]
        );

        Ok(())
    }

    #[test]
    fn test_aggregation_level2_multi_segments() -> crate::Result<()> {
        test_aggregation_level2(false, false)
    }

    #[test]
    fn test_aggregation_level2_single_segment() -> crate::Result<()> {
        test_aggregation_level2(true, false)
    }

    #[test]
    fn test_aggregation_level2_multi_segments_distributed_collector() -> crate::Result<()> {
        test_aggregation_level2(false, true)
    }

    #[test]
    fn test_aggregation_level2_single_segment_distributed_collector() -> crate::Result<()> {
        test_aggregation_level2(true, true)
    }

    #[test]
    fn test_aggregation_invalid_requests() -> crate::Result<()> {
        let index = get_test_index_2_segments(false)?;

        let reader = index.reader()?;
        let text_field = reader.searcher().schema().get_field("text").unwrap();

        let term_query = TermQuery::new(
            Term::from_field_text(text_field, "cool"),
            IndexRecordOption::Basic,
        );

        let agg_req_1: Aggregations = vec![(
            "average".to_string(),
            Aggregation::Metric(MetricAggregation::Average(
                AverageAggregation::from_field_name("text".to_string()),
            )),
        )]
        .into_iter()
        .collect();

        let collector = AggregationCollector::from_aggs(agg_req_1);

        let searcher = reader.searcher();
        let agg_res = searcher.search(&term_query, &collector).unwrap_err();

        assert_eq!(
            format!("{:?}", agg_res),
            r#"InvalidArgument("Invalid field type in aggregation Str, only f64, u64, i64 is supported")"#
        );
        Ok(())
    }

    #[cfg(all(test, feature = "unstable"))]
    mod bench {

        use rand::{thread_rng, Rng};
        use test::{self, Bencher};

        use super::*;

        fn get_test_index_2_segments(merge_segments: bool) -> crate::Result<Index> {
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
                crate::schema::IntOptions::default().set_fast(Cardinality::SingleValue);
            let score_field = schema_builder.add_u64_field("score", score_fieldtype.clone());
            let score_field_f64 =
                schema_builder.add_f64_field("score_f64", score_fieldtype.clone());
            let score_field_i64 = schema_builder.add_i64_field("score_i64", score_fieldtype);
            let index = Index::create_in_ram(schema_builder.build());
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
            let index = get_test_index_2_segments(false).unwrap();
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
        fn bench_aggregation_average_f64(b: &mut Bencher) {
            let index = get_test_index_2_segments(false).unwrap();
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
            let index = get_test_index_2_segments(false).unwrap();
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
        fn bench_aggregation_sub_tree(b: &mut Bencher) {
            let index = get_test_index_2_segments(false).unwrap();
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
                            bucket_agg: BucketAggregationType::RangeAggregation(RangeAggregation {
                                field_name: "score_f64".to_string(),
                                buckets: vec![
                                    (3f64..7000f64),
                                    (7000f64..20000f64),
                                    (20000f64..60000f64),
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
