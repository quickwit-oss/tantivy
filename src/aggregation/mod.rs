//! This module contains code for aggregation.
//!
//! agg_req contains the aggregation request.
//! agg_req_with_accessor contains the aggregation request plus fast field accessors etc, which are
//! used during collection.
//!
//! segment_agg_result is the aggregation result tree during collection.
//! intermediate_agg_result is the aggregation tree for merging with other trees.
//! agg_result is the final aggregation tree.

mod agg_req;
mod agg_req_with_accessor;
mod agg_result;
mod bucket;
mod executor;
mod intermediate_agg_result;
mod metric;
mod segment_agg_result;

use std::collections::HashMap;

pub use agg_req::Aggregation;
pub use agg_req::BucketAggregationType;
pub use agg_req::MetricAggregation;
use itertools::Itertools;

use self::intermediate_agg_result::IntermediateAggregationResults;

/// VecWithNames will be used for th
#[derive(Clone, PartialEq)]
pub struct VecWithNames<T: Clone> {
    data: Vec<T>,
    data_names: Vec<String>,
}
impl<T: Clone> Default for VecWithNames<T> {
    fn default() -> Self {
        Self {
            data: Default::default(),
            data_names: Default::default(),
        }
    }
}

impl<T: Clone + std::fmt::Debug> std::fmt::Debug for VecWithNames<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VecWithNames")
            .field("data", &self.data)
            .field("data_names", &self.data_names)
            .finish()
    }
}

impl<T: Clone> From<HashMap<String, T>> for VecWithNames<T> {
    fn from(map: HashMap<String, T>) -> Self {
        VecWithNames::from_iter(map.into_iter())
    }
}

impl<T: Clone> VecWithNames<T> {
    fn from_iter(iter: impl Iterator<Item = (String, T)>) -> Self {
        let mut entries = iter.collect_vec();
        // Sort to ensure order of elements match across multiple instances
        entries.sort_by_cached_key(|entry| entry.0.to_string());
        let mut data = vec![];
        let mut data_names = vec![];
        for entry in entries {
            data_names.push(entry.0);
            data.push(entry.1);
        }
        VecWithNames { data, data_names }
    }

    fn iter_mut(&mut self) -> impl Iterator<Item = (&String, &mut T)> + '_ {
        self.data_names.iter().zip(self.data.iter_mut())
    }
    fn into_iter(self) -> impl Iterator<Item = (String, T)> {
        self.data_names.into_iter().zip(self.data.into_iter())
    }
    fn iter(&self) -> impl Iterator<Item = (&String, &T)> + '_ {
        self.data_names.iter().zip(self.data.iter())
    }
    fn keys(&self) -> impl Iterator<Item = &String> + '_ {
        self.data_names.iter()
    }
    fn values(&self) -> impl Iterator<Item = &T> + '_ {
        self.data.iter()
    }
    fn values_mut(&mut self) -> impl Iterator<Item = &mut T> + '_ {
        self.data.iter_mut()
    }
    fn entries(&self) -> impl Iterator<Item = (&String, &T)> + '_ {
        self.data_names.iter().zip(self.data.iter())
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
/// The key to identify a bucket.
pub enum Key {
    Str(String),
    U64(u64),
    I64(i64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BucketDataEntry {
    KeyCount(BucketDataEntryKeyCount),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BucketDataEntryKeyCount {
    key: Key,
    doc_count: u64,
    values: Option<Vec<u64>>,
    sub_aggregation: Option<Box<IntermediateAggregationResults>>,
}

#[cfg(test)]
mod tests {

    use crate::{
        query::TermQuery,
        schema::{Cardinality, IndexRecordOption, Schema, TextFieldIndexing, INDEXED},
        Index, Term,
    };

    use super::{
        agg_req::Aggregation,
        agg_req::{Aggregations, BucketAggregation},
        bucket::RangeAggregationReq,
        executor::AggregationCollector,
        BucketAggregationType, MetricAggregation,
    };

    fn get_test_index_2_segments() -> crate::Result<Index> {
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
        let score_field = schema_builder.add_u64_field("score", score_fieldtype);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_for_tests()?;
            // writing the segment
            index_writer.add_document(doc!(
                text_field => "cool",
                score_field => 3u64,
            ))?;
            index_writer.add_document(doc!(
                text_field => "cool",
                score_field => 5u64,
            ))?;
            index_writer.add_document(doc!(
                text_field => "cool",
                score_field => 7u64,
            ))?;
            index_writer.commit()?;
            index_writer.add_document(doc!(
                text_field => "cool",
                score_field => 11u64,
            ))?;
            index_writer.add_document(doc!(
                text_field => "cool",
                score_field => 13u64,
            ))?;
            index_writer.commit()?;
        }
        //{
        //let segment_ids = index
        //.searchable_segment_ids()
        //.expect("Searchable segments failed.");
        //let mut index_writer = index.writer_for_tests()?;
        //block_on(index_writer.merge(&segment_ids))?;
        //index_writer.wait_merging_threads()?;
        //}
        //
        Ok(index)
    }

    #[test]
    fn test_aggregation_level1() -> crate::Result<()> {
        let index = get_test_index_2_segments()?;

        let reader = index.reader()?;
        let text_field = reader.searcher().schema().get_field("text").unwrap();

        let term_query = TermQuery::new(
            Term::from_field_text(text_field, "cool"),
            IndexRecordOption::Basic,
        );

        let agg_req_1: Aggregations = vec![
            (
                "average".to_string(),
                Aggregation::Metric(MetricAggregation::Average {
                    field_name: "score".to_string(),
                }),
            ),
            (
                "range".to_string(),
                Aggregation::Bucket(BucketAggregation {
                    bucket_agg: BucketAggregationType::RangeAggregation(RangeAggregationReq {
                        field_name: "score".to_string(),
                        buckets: vec![(3..7), (7..20)],
                    }),
                    sub_aggregation: Default::default(),
                }),
            ),
        ]
        .into_iter()
        .collect();

        let collector = AggregationCollector::from_aggs(agg_req_1);

        let searcher = reader.searcher();
        let agg_res = searcher.search(&term_query, &collector).unwrap();
        dbg!(&agg_res);

        Ok(())
    }

    #[test]
    fn test_aggregation_level2() -> crate::Result<()> {
        let index = get_test_index_2_segments()?;

        let reader = index.reader()?;
        let text_field = reader.searcher().schema().get_field("text").unwrap();

        let term_query = TermQuery::new(
            Term::from_field_text(text_field, "cool"),
            IndexRecordOption::Basic,
        );

        let sub_agg_req_1: Aggregations = vec![(
            "average_in_range".to_string(),
            Aggregation::Metric(MetricAggregation::Average {
                field_name: "score".to_string(),
            }),
        )]
        .into_iter()
        .collect();

        let agg_req_1: Aggregations = vec![
            (
                "average".to_string(),
                Aggregation::Metric(MetricAggregation::Average {
                    field_name: "score".to_string(),
                }),
            ),
            (
                "range".to_string(),
                Aggregation::Bucket(BucketAggregation {
                    bucket_agg: BucketAggregationType::RangeAggregation(RangeAggregationReq {
                        field_name: "score".to_string(),
                        buckets: vec![(3..7), (7..20)],
                    }),
                    sub_aggregation: sub_agg_req_1,
                }),
            ),
        ]
        .into_iter()
        .collect();

        let collector = AggregationCollector::from_aggs(agg_req_1);

        let searcher = reader.searcher();
        let agg_res = searcher.search(&term_query, &collector).unwrap();
        dbg!(&agg_res);
        //

        Ok(())
    }
}
