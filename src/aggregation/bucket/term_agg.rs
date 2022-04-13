use std::fmt::Debug;

use fnv::FnvHashMap;
use serde::{Deserialize, Serialize};

use crate::aggregation::agg_req_with_accessor::{
    AggregationsWithAccessor, BucketAggregationWithAccessor,
};
use crate::aggregation::intermediate_agg_result::{
    IntermediateBucketResult, IntermediateTermBucketEntry, IntermediateTermBucketResult,
};
use crate::aggregation::segment_agg_result::SegmentAggregationResultsCollector;
use crate::error::DataCorruption;
use crate::fastfield::MultiValuedFastFieldReader;
use crate::schema::Type;
use crate::DocId;

/// Creates a bucket for every unique term
///
/// ### Terminology
/// Shard parameters are supposed to be equivalent to elasticsearch shard parameter.
/// Since they are
///
/// ## Document count error
/// To improve performance, results from one segment are cut off at `segment_size`. On a index with
/// a single segment this is fine. When combining results of multiple segments, terms that
/// don't make it in the top n of a shard increase the theoretical upper bound error by lowest
/// term-count.
///
/// Even with a larger `segment_size` value, doc_count values for a terms aggregation may be
/// approximate. As a result, any sub-aggregations on the terms aggregation may also be approximate.
/// `sum_other_doc_count` is the number of documents that didn’t make it into the the top size
/// terms. If this is greater than 0, you can be sure that the terms agg had to throw away some
/// buckets, either because they didn’t fit into size on the root node or they didn’t fit into
/// `segment_size` on the segment node.
///
/// ## Per bucket document count error
/// If you set the `show_term_doc_count_error` parameter to true, the terms aggregation will include
/// doc_count_error_upper_bound, which is an upper bound to the error on the doc_count returned by
/// each segment. It’s the sum of the size of the largest bucket on each shard that didn’t fit into
/// shard_size.
///
/// Result type is [BucketResult](crate::aggregation::agg_result::BucketResult) with
/// [TermBucketEntry](crate::aggregation::agg_result::BucketEntry) on the
/// AggregationCollector.
///
/// Result type is
/// [crate::aggregation::intermediate_agg_result::IntermediateBucketResult] with
/// [crate::aggregation::intermediate_agg_result::IntermediateTermBucketEntry] on the
/// DistributedAggregationCollector.
///
/// # Limitations/Compatibility
///
/// # Request JSON Format
/// ```json
/// {
///     "genres": {
///         "terms":{ "field": "genre" }
///     }
/// }
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TermsAggregation {
    /// The field to aggregate on.
    pub field: String,
    /// By default, the top 10 terms with the most documents are returned.
    /// Larger values for size are more expensive.
    pub size: Option<u32>,

    /// Unused by tantivy.
    ///
    /// Since tantivy doesn't know shards, this parameter is merely there to be used by consumers
    /// of tantivy. shard_size is the number of terms returned by each shard.
    /// The default value in elasticsearch is size * 1.5 + 10.
    ///
    /// Should never be smaller than size.
    pub shard_size: Option<u32>,

    /// The get more accurate results, we fetch more than `size` from each segment.
    ///
    /// Increasing this value is will increase the cost for more accuracy.
    ///
    /// Defaults to 10 * size.
    pub segment_size: Option<u32>,

    /// If you set the `show_term_doc_count_error` parameter to true, the terms aggregation will
    /// include doc_count_error_upper_bound, which is an upper bound to the error on the
    /// doc_count returned by each shard. It’s the sum of the size of the largest bucket on
    /// each segment that didn’t fit into `shard_size`.
    #[serde(default = "default_show_term_doc_count_error")]
    pub show_term_doc_count_error: bool,

    /// Filter all terms than are lower `min_doc_count`. Defaults to 1.
    ///
    /// **Expensive**: When set to 0, this will return all terms in the field.
    pub min_doc_count: Option<u64>,
}
impl Default for TermsAggregation {
    fn default() -> Self {
        Self {
            field: Default::default(),
            size: Default::default(),
            shard_size: Default::default(),
            show_term_doc_count_error: true,
            min_doc_count: Default::default(),
            segment_size: Default::default(),
        }
    }
}

fn default_show_term_doc_count_error() -> bool {
    true
}

/// Same as TermsAggregation, but with populated defaults.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TermsAggregationInternal {
    /// The field to aggregate on.
    pub field: String,
    /// By default, the top 10 terms with the most documents are returned.
    /// Larger values for size are more expensive.
    ///
    /// Defaults to 10.
    pub size: u32,

    /// If you set the `show_term_doc_count_error` parameter to true, the terms aggregation will
    /// include doc_count_error_upper_bound, which is an upper bound to the error on the
    /// doc_count returned by each shard. It’s the sum of the size of the largest bucket on
    /// each segment that didn’t fit into `shard_size`.
    pub show_term_doc_count_error: bool,

    /// The get more accurate results, we fetch more than `size` from each segment.
    ///
    /// Increasing this value is will increase the cost for more accuracy.
    pub segment_size: u32,

    /// Filter all terms than are lower `min_doc_count`. Defaults to 1.
    ///
    /// *Expensive*: When set to 0, this will return all terms in the field.
    pub min_doc_count: u64,
}

impl TermsAggregationInternal {
    pub(crate) fn from_req(req: &TermsAggregation) -> Self {
        let size = req.size.unwrap_or(10);

        let mut segment_size = req.segment_size.unwrap_or(size * 10);

        segment_size = segment_size.max(size);
        TermsAggregationInternal {
            field: req.field.to_string(),
            size,
            segment_size,
            show_term_doc_count_error: req.show_term_doc_count_error,
            min_doc_count: req.min_doc_count.unwrap_or(1),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
/// Container to store term_ids and their buckets.
struct TermBuckets {
    pub(crate) entries: FnvHashMap<u32, TermBucketEntry>,
    blueprint: Option<SegmentAggregationResultsCollector>,
}

#[derive(Clone, PartialEq, Default)]
struct TermBucketEntry {
    doc_count: u64,
    sub_aggregations: Option<SegmentAggregationResultsCollector>,
}

impl Debug for TermBucketEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TermBucketEntry")
            .field("doc_count", &self.doc_count)
            .finish()
    }
}

impl TermBucketEntry {
    fn from_blueprint(blueprint: &Option<SegmentAggregationResultsCollector>) -> Self {
        Self {
            doc_count: 0,
            sub_aggregations: blueprint.clone(),
        }
    }

    pub(crate) fn into_intermediate_bucket_entry(
        self,
        agg_with_accessor: &AggregationsWithAccessor,
    ) -> crate::Result<IntermediateTermBucketEntry> {
        let sub_aggregation = if let Some(sub_aggregation) = self.sub_aggregations {
            sub_aggregation.into_intermediate_aggregations_result(agg_with_accessor)?
        } else {
            Default::default()
        };

        Ok(IntermediateTermBucketEntry {
            doc_count: self.doc_count,
            sub_aggregation,
        })
    }
}

impl TermBuckets {
    pub(crate) fn from_req_and_validate(
        sub_aggregation: &AggregationsWithAccessor,
        _max_term_id: usize,
    ) -> crate::Result<Self> {
        let has_sub_aggregations = sub_aggregation.is_empty();

        let blueprint = if has_sub_aggregations {
            let sub_aggregation =
                SegmentAggregationResultsCollector::from_req_and_validate(sub_aggregation)?;
            Some(sub_aggregation)
        } else {
            None
        };

        Ok(TermBuckets {
            blueprint,
            entries: Default::default(),
        })
    }

    fn increment_bucket(
        &mut self,
        term_ids: &[u64],
        doc: DocId,
        bucket_with_accessor: &AggregationsWithAccessor,
        blueprint: &Option<SegmentAggregationResultsCollector>,
    ) {
        // self.ensure_vec_exists(term_ids);
        for &term_id in term_ids {
            let entry = self
                .entries
                .entry(term_id as u32)
                .or_insert_with(|| TermBucketEntry::from_blueprint(blueprint));
            entry.doc_count += 1;
            if let Some(sub_aggregations) = entry.sub_aggregations.as_mut() {
                sub_aggregations.collect(doc, bucket_with_accessor);
            }
        }
    }

    fn force_flush(&mut self, agg_with_accessor: &AggregationsWithAccessor) {
        for entry in &mut self.entries.values_mut() {
            if let Some(sub_aggregations) = entry.sub_aggregations.as_mut() {
                sub_aggregations.flush_staged_docs(agg_with_accessor, false);
            }
        }
    }
}

/// The collector puts values from the fast field into the correct buckets and does a conversion to
/// the correct datatype.
#[derive(Clone, Debug, PartialEq)]
pub struct SegmentTermCollector {
    /// The buckets containing the aggregation data.
    term_buckets: TermBuckets,
    req: TermsAggregationInternal,
    field_type: Type,
    blueprint: Option<SegmentAggregationResultsCollector>,
}

impl SegmentTermCollector {
    pub(crate) fn from_req_and_validate(
        req: &TermsAggregation,
        sub_aggregations: &AggregationsWithAccessor,
        field_type: Type,
        accessor: &MultiValuedFastFieldReader<u64>,
    ) -> crate::Result<Self> {
        let max_term_id = accessor.max_value();
        let term_buckets =
            TermBuckets::from_req_and_validate(sub_aggregations, max_term_id as usize)?;

        let has_sub_aggregations = !sub_aggregations.is_empty();
        let blueprint = if has_sub_aggregations {
            let sub_aggregation =
                SegmentAggregationResultsCollector::from_req_and_validate(sub_aggregations)?;
            Some(sub_aggregation)
        } else {
            None
        };

        Ok(SegmentTermCollector {
            req: TermsAggregationInternal::from_req(req),
            term_buckets,
            field_type,
            blueprint,
        })
    }

    pub(crate) fn into_intermediate_bucket_result(
        self,
        agg_with_accessor: &BucketAggregationWithAccessor,
    ) -> crate::Result<IntermediateBucketResult> {
        let mut entries: Vec<_> = self.term_buckets.entries.into_iter().collect();

        let (term_doc_count_before_cutoff, sum_other_doc_count) =
            cut_off_buckets(&mut entries, self.req.segment_size as usize);

        let inverted_index = agg_with_accessor
            .inverted_index
            .as_ref()
            .expect("internal error: inverted index not loaded for term aggregation");
        let term_dict = inverted_index.terms();

        let mut dict: FnvHashMap<String, IntermediateTermBucketEntry> = Default::default();
        let mut buffer = vec![];
        for (term_id, entry) in entries {
            term_dict
                .ord_to_term(term_id as u64, &mut buffer)
                .expect("could not find term");
            dict.insert(
                String::from_utf8(buffer.to_vec())
                    .map_err(|utf8_err| DataCorruption::comment_only(utf8_err.to_string()))?,
                entry.into_intermediate_bucket_entry(&agg_with_accessor.sub_aggregation)?,
            );
        }
        if self.req.min_doc_count == 0 {
            let mut stream = term_dict.stream()?;
            while let Some((key, _ord)) = stream.next() {
                let key = std::str::from_utf8(key)
                    .map_err(|utf8_err| DataCorruption::comment_only(utf8_err.to_string()))?;
                if !dict.contains_key(key) {
                    dict.insert(key.to_owned(), Default::default());
                }
            }
        }

        Ok(IntermediateBucketResult::Terms(
            IntermediateTermBucketResult {
                entries: dict,
                sum_other_doc_count,
                doc_count_error_upper_bound: term_doc_count_before_cutoff,
            },
        ))
    }

    #[inline]
    pub(crate) fn collect_block(
        &mut self,
        doc: &[DocId],
        bucket_with_accessor: &BucketAggregationWithAccessor,
        force_flush: bool,
    ) {
        let accessor = bucket_with_accessor
            .accessor
            .as_multi()
            .expect("unexpected fast field cardinatility");
        let mut iter = doc.chunks_exact(4);
        let mut vals1 = vec![];
        let mut vals2 = vec![];
        let mut vals3 = vec![];
        let mut vals4 = vec![];
        for docs in iter.by_ref() {
            accessor.get_vals(docs[0], &mut vals1);
            accessor.get_vals(docs[1], &mut vals2);
            accessor.get_vals(docs[2], &mut vals3);
            accessor.get_vals(docs[3], &mut vals4);

            self.term_buckets.increment_bucket(
                &vals1,
                docs[0],
                &bucket_with_accessor.sub_aggregation,
                &self.blueprint,
            );
            self.term_buckets.increment_bucket(
                &vals2,
                docs[1],
                &bucket_with_accessor.sub_aggregation,
                &self.blueprint,
            );
            self.term_buckets.increment_bucket(
                &vals3,
                docs[2],
                &bucket_with_accessor.sub_aggregation,
                &self.blueprint,
            );
            self.term_buckets.increment_bucket(
                &vals4,
                docs[3],
                &bucket_with_accessor.sub_aggregation,
                &self.blueprint,
            );
        }
        for &doc in iter.remainder() {
            accessor.get_vals(doc, &mut vals1);

            self.term_buckets.increment_bucket(
                &vals1,
                doc,
                &bucket_with_accessor.sub_aggregation,
                &self.blueprint,
            );
        }
        if force_flush {
            self.term_buckets
                .force_flush(&bucket_with_accessor.sub_aggregation);
        }
    }
}

pub(crate) trait GetDocCount {
    fn doc_count(&self) -> u64;
}
impl GetDocCount for (u32, TermBucketEntry) {
    fn doc_count(&self) -> u64 {
        self.1.doc_count
    }
}

pub(crate) fn cut_off_buckets<T: GetDocCount + Debug>(
    entries: &mut Vec<T>,
    num_elem: usize,
) -> (u64, u64) {
    entries.sort_unstable_by_key(|entry| std::cmp::Reverse(entry.doc_count()));

    let term_doc_count_before_cutoff = entries
        .get(num_elem)
        .map(|entry| entry.doc_count())
        .unwrap_or(0);

    let sum_other_doc_count = entries
        .get(num_elem..)
        .map(|cut_off_range| cut_off_range.iter().map(|entry| entry.doc_count()).sum())
        .unwrap_or(0);

    entries.truncate(num_elem);
    (term_doc_count_before_cutoff, sum_other_doc_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregation::agg_req::{
        get_term_dict_field_names, Aggregation, Aggregations, BucketAggregation,
        BucketAggregationType,
    };
    use crate::aggregation::tests::{exec_request, get_test_index_from_terms};

    #[test]
    fn terms_aggregation_test_single_segment() -> crate::Result<()> {
        terms_aggregation_test_merge_segment(true)
    }
    #[test]
    fn terms_aggregation_test() -> crate::Result<()> {
        terms_aggregation_test_merge_segment(false)
    }
    fn terms_aggregation_test_merge_segment(merge_segments: bool) -> crate::Result<()> {
        let segment_and_terms = vec![
            vec!["terma"],
            vec!["termb"],
            vec!["termc"],
            vec!["terma"],
            vec!["terma"],
            vec!["terma"],
            vec!["termb"],
            vec!["terma"],
        ];
        let index = get_test_index_from_terms(merge_segments, &segment_and_terms)?;

        let agg_req: Aggregations = vec![(
            "my_texts".to_string(),
            Aggregation::Bucket(BucketAggregation {
                bucket_agg: BucketAggregationType::Terms(TermsAggregation {
                    field: "string_id".to_string(),
                    ..Default::default()
                }),
                sub_aggregation: Default::default(),
            }),
        )]
        .into_iter()
        .collect();

        let res = exec_request(agg_req, &index)?;
        assert_eq!(res["my_texts"]["buckets"][0]["key"], "terma");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 5);
        assert_eq!(res["my_texts"]["buckets"][1]["key"], "termb");
        assert_eq!(res["my_texts"]["buckets"][1]["doc_count"], 2);
        assert_eq!(res["my_texts"]["buckets"][2]["doc_count"], 1);
        assert_eq!(res["my_texts"]["buckets"][2]["key"], "termc");
        assert_eq!(res["my_texts"]["sum_other_doc_count"], 0);

        let agg_req: Aggregations = vec![(
            "my_texts".to_string(),
            Aggregation::Bucket(BucketAggregation {
                bucket_agg: BucketAggregationType::Terms(TermsAggregation {
                    field: "string_id".to_string(),
                    size: Some(2),
                    shard_size: Some(2),
                    ..Default::default()
                }),
                sub_aggregation: Default::default(),
            }),
        )]
        .into_iter()
        .collect();

        let res = exec_request(agg_req, &index)?;
        assert_eq!(res["my_texts"]["buckets"][0]["key"], "terma");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 5);
        assert_eq!(res["my_texts"]["buckets"][1]["key"], "termb");
        assert_eq!(res["my_texts"]["buckets"][1]["doc_count"], 2);
        assert_eq!(
            res["my_texts"]["buckets"][2]["key"],
            serde_json::Value::Null
        );
        assert_eq!(res["my_texts"]["sum_other_doc_count"], 1);

        // test min_doc_count
        let agg_req: Aggregations = vec![(
            "my_texts".to_string(),
            Aggregation::Bucket(BucketAggregation {
                bucket_agg: BucketAggregationType::Terms(TermsAggregation {
                    field: "string_id".to_string(),
                    size: Some(2),
                    shard_size: Some(2),
                    min_doc_count: Some(3),
                    ..Default::default()
                }),
                sub_aggregation: Default::default(),
            }),
        )]
        .into_iter()
        .collect();

        let res = exec_request(agg_req.clone(), &index)?;
        assert_eq!(res["my_texts"]["buckets"][0]["key"], "terma");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 5);
        assert_eq!(
            res["my_texts"]["buckets"][1]["key"],
            serde_json::Value::Null
        );
        assert_eq!(res["my_texts"]["sum_other_doc_count"], 0); // TODO sum_other_doc_count with min_doc_count

        assert_eq!(
            get_term_dict_field_names(&agg_req),
            vec!["string_id".to_string(),].into_iter().collect()
        );

        Ok(())
    }

    #[test]
    fn terms_aggregation_min_doc_count_special_case() -> crate::Result<()> {
        let terms_per_segment = vec![
            vec!["terma", "terma", "termb", "termb", "termb", "termc"], /* termc doesn't make it
                                                                         * from this segment */
            vec!["terma", "terma", "termb", "termc", "termc"], /* termb doesn't make it from
                                                                * this segment */
        ];

        let index = get_test_index_from_terms(false, &terms_per_segment)?;

        let agg_req: Aggregations = vec![(
            "my_texts".to_string(),
            Aggregation::Bucket(BucketAggregation {
                bucket_agg: BucketAggregationType::Terms(TermsAggregation {
                    field: "string_id".to_string(),
                    size: Some(2),
                    segment_size: Some(2),
                    ..Default::default()
                }),
                sub_aggregation: Default::default(),
            }),
        )]
        .into_iter()
        .collect();

        let res = exec_request(agg_req, &index)?;
        println!("{}", &serde_json::to_string_pretty(&res).unwrap());

        assert_eq!(res["my_texts"]["buckets"][0]["key"], "terma");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 4);
        assert_eq!(res["my_texts"]["buckets"][1]["key"], "termb");
        assert_eq!(res["my_texts"]["buckets"][1]["doc_count"], 3);
        assert_eq!(
            res["my_texts"]["buckets"][2]["doc_count"],
            serde_json::Value::Null
        );
        assert_eq!(res["my_texts"]["sum_other_doc_count"], 4);
        assert_eq!(res["my_texts"]["doc_count_error_upper_bound"], 2);

        Ok(())
    }

    #[test]
    fn terms_aggregation_error_count_test() -> crate::Result<()> {
        let terms_per_segment = vec![
            vec!["terma", "terma", "termb", "termb", "termb", "termc"], /* termc doesn't make it
                                                                         * from this segment */
            vec!["terma", "terma", "termb", "termc", "termc"], /* termb doesn't make it from
                                                                * this segment */
        ];

        let index = get_test_index_from_terms(false, &terms_per_segment)?;

        let agg_req: Aggregations = vec![(
            "my_texts".to_string(),
            Aggregation::Bucket(BucketAggregation {
                bucket_agg: BucketAggregationType::Terms(TermsAggregation {
                    field: "string_id".to_string(),
                    size: Some(2),
                    segment_size: Some(2),
                    ..Default::default()
                }),
                sub_aggregation: Default::default(),
            }),
        )]
        .into_iter()
        .collect();

        let res = exec_request(agg_req, &index)?;
        println!("{}", &serde_json::to_string_pretty(&res).unwrap());

        assert_eq!(res["my_texts"]["buckets"][0]["key"], "terma");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 4);
        assert_eq!(res["my_texts"]["buckets"][1]["key"], "termb");
        assert_eq!(res["my_texts"]["buckets"][1]["doc_count"], 3);
        assert_eq!(
            res["my_texts"]["buckets"][2]["doc_count"],
            serde_json::Value::Null
        );
        assert_eq!(res["my_texts"]["sum_other_doc_count"], 4);
        assert_eq!(res["my_texts"]["doc_count_error_upper_bound"], 2);

        Ok(())
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {

    use itertools::Itertools;
    use rand::seq::SliceRandom;
    use rand::thread_rng;

    use super::*;

    fn get_collector_with_buckets(num_docs: u64) -> TermBuckets {
        TermBuckets::from_req_and_validate(&Default::default(), num_docs as usize).unwrap()
    }

    fn get_rand_terms(total_terms: u64, num_terms_returned: u64) -> Vec<u64> {
        let mut rng = thread_rng();

        let all_terms = (0..total_terms - 1).collect_vec();

        let mut vals = vec![];
        for _ in 0..num_terms_returned {
            let val = all_terms.as_slice().choose(&mut rng).unwrap();
            vals.push(*val);
        }

        vals
    }

    fn bench_term_buckets(b: &mut test::Bencher, num_terms: u64, total_terms: u64) {
        let mut collector = get_collector_with_buckets(total_terms);
        let vals = get_rand_terms(total_terms, num_terms);
        let aggregations_with_accessor: AggregationsWithAccessor = Default::default();
        b.iter(|| {
            for &val in &vals {
                collector.increment_bucket(&[val], 0, &aggregations_with_accessor, &None);
            }
        })
    }

    #[bench]
    fn bench_term_buckets_500_of_1_000_000(b: &mut test::Bencher) {
        bench_term_buckets(b, 500u64, 1_000_000u64)
    }

    #[bench]
    fn bench_term_buckets_1_000_000_of_50_000(b: &mut test::Bencher) {
        bench_term_buckets(b, 1_000_000u64, 50_000u64)
    }

    #[bench]
    fn bench_term_buckets_1_000_000_of_50(b: &mut test::Bencher) {
        bench_term_buckets(b, 1_000_000u64, 50u64)
    }

    #[bench]
    fn bench_term_buckets_1_000_000_of_1_000_000(b: &mut test::Bencher) {
        bench_term_buckets(b, 1_000_000u64, 1_000_000u64)
    }
}
