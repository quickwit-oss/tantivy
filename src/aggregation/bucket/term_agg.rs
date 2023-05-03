use std::fmt::Debug;

use columnar::ColumnType;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use super::{CustomOrder, Order, OrderTarget};
use crate::aggregation::agg_limits::MemoryConsumption;
use crate::aggregation::agg_req_with_accessor::{
    AggregationWithAccessor, AggregationsWithAccessor,
};
use crate::aggregation::f64_from_fastfield_u64;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateBucketResult,
    IntermediateKey, IntermediateTermBucketEntry, IntermediateTermBucketResult,
};
use crate::aggregation::segment_agg_result::{
    build_segment_agg_collector, SegmentAggregationCollector,
};
use crate::error::DataCorruption;
use crate::TantivyError;

/// Creates a bucket for every unique term and counts the number of occurences.
/// Note that doc_count in the response buckets equals term count here.
///
/// If the text is untokenized and single value, that means one term per document and therefore it
/// is in fact doc count.
///
/// ## Prerequisite
/// Term aggregations work only on [fast fields](`crate::fastfield`) of type `u64`, `f64`, `i64` and
/// text.
///
/// ## Document count error
/// To improve performance, results from one segment are cut off at `segment_size`. On a index with
/// a single segment this is fine. When combining results of multiple segments, terms that
/// don't make it in the top n of a shard increase the theoretical upper bound error by lowest
/// term-count.
///
/// Even with a larger `segment_size` value, doc_count values for a terms aggregation may be
/// approximate. As a result, any sub-aggregations on the terms aggregation may also be approximate.
/// `sum_other_doc_count` is the number of documents that didn’t make it into the top size
/// terms. If this is greater than 0, you can be sure that the terms agg had to throw away some
/// buckets, either because they didn’t fit into size on the root node or they didn’t fit into
/// `segment_size` on the segment node.
///
/// ## Per bucket document count error
/// If you set the `show_term_doc_count_error` parameter to true, the terms aggregation will include
/// doc_count_error_upper_bound, which is an upper bound to the error on the doc_count returned by
/// each segment. It’s the sum of the size of the largest bucket on each segment that didn’t fit
/// into segment_size.
///
/// Result type is [`BucketResult`](crate::aggregation::agg_result::BucketResult) with
/// [`BucketEntry`](crate::aggregation::agg_result::BucketEntry) on the
/// `AggregationCollector`.
///
/// Result type is
/// [`IntermediateBucketResult`](crate::aggregation::intermediate_agg_result::IntermediateBucketResult) with
/// [`IntermediateTermBucketEntry`](crate::aggregation::intermediate_agg_result::IntermediateTermBucketEntry) on the
/// `DistributedAggregationCollector`.
///
/// # Limitations/Compatibility
///
/// Each segment returns up to [segment_size](TermsAggregation::segment_size) results. This
/// differences to elasticsearch behaviour.
///
/// # Request JSON Format
/// ```json
/// {
///     "genres": {
///         "terms":{ "field": "genre" }
///     }
/// }
/// ```
///
/// /// # Response JSON Format
/// ```json
/// {
///     ...
///     "aggregations": {
///         "genres": {
///             "doc_count_error_upper_bound": 0,
///             "sum_other_doc_count": 0,
///             "buckets": [
///                 { "key": "drumnbass", "doc_count": 6 },
///                 { "key": "raggae", "doc_count": 4 },
///                 { "key": "jazz", "doc_count": 2 }
///             ]
///         }
///     }
/// }
/// ```

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TermsAggregation {
    /// The field to aggregate on.
    pub field: String,
    /// By default, the top 10 terms with the most documents are returned.
    /// Larger values for size are more expensive.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub size: Option<u32>,

    /// Unused by tantivy.
    ///
    /// Since tantivy doesn't know shards, this parameter is merely there to be used by consumers
    /// of tantivy. shard_size is the number of terms returned by each shard.
    /// The default value in elasticsearch is size * 1.5 + 10.
    ///
    /// Should never be smaller than size.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    #[serde(alias = "shard_size")]
    pub split_size: Option<u32>,

    /// The get more accurate results, we fetch more than `size` from each segment.
    ///
    /// Increasing this value is will increase the cost for more accuracy.
    ///
    /// Defaults to 10 * size.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub segment_size: Option<u32>,

    /// If you set the `show_term_doc_count_error` parameter to true, the terms aggregation will
    /// include doc_count_error_upper_bound, which is an upper bound to the error on the
    /// doc_count returned by each shard. It’s the sum of the size of the largest bucket on
    /// each segment that didn’t fit into `shard_size`.
    ///
    /// Defaults to true when ordering by count desc.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub show_term_doc_count_error: Option<bool>,

    /// Filter all terms that are lower than `min_doc_count`. Defaults to 1.
    ///
    /// **Expensive**: When set to 0, this will return all terms in the field.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub min_doc_count: Option<u64>,

    /// Set the order. `String` is here a target, which is either "_count", "_key", or the name of
    /// a metric sub_aggregation.
    ///
    /// Single value metrics like average can be addressed by its name.
    /// Multi value metrics like stats are required to address their field by name e.g.
    /// "stats.avg"
    ///
    /// Examples in JSON format:
    /// { "_count": "asc" }
    /// { "_key": "asc" }
    /// { "average_price": "asc" }
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub order: Option<CustomOrder>,
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
    /// each segment that didn’t fit into `segment_size`.
    pub show_term_doc_count_error: bool,

    /// The get more accurate results, we fetch more than `size` from each segment.
    ///
    /// Increasing this value is will increase the cost for more accuracy.
    pub segment_size: u32,

    /// Filter all terms that are lower than `min_doc_count`. Defaults to 1.
    ///
    /// *Expensive*: When set to 0, this will return all terms in the field.
    pub min_doc_count: u64,

    pub order: CustomOrder,
}

impl TermsAggregationInternal {
    pub(crate) fn from_req(req: &TermsAggregation) -> Self {
        let size = req.size.unwrap_or(10);

        let mut segment_size = req.segment_size.unwrap_or(size * 10);

        let order = req.order.clone().unwrap_or_default();
        segment_size = segment_size.max(size);
        TermsAggregationInternal {
            field: req.field.to_string(),
            size,
            segment_size,
            show_term_doc_count_error: req
                .show_term_doc_count_error
                .unwrap_or_else(|| order == CustomOrder::default()),
            min_doc_count: req.min_doc_count.unwrap_or(1),
            order,
        }
    }
}

#[derive(Clone, Debug, Default)]
/// Container to store term_ids/or u64 values and their buckets.
struct TermBuckets {
    pub(crate) entries: FxHashMap<u64, u32>,
    pub(crate) sub_aggs: FxHashMap<u64, Box<dyn SegmentAggregationCollector>>,
}

impl TermBuckets {
    fn get_memory_consumption(&self) -> usize {
        let sub_aggs_mem = self.sub_aggs.memory_consumption();
        let buckets_mem = self.entries.memory_consumption();
        sub_aggs_mem + buckets_mem
    }

    fn force_flush(
        &mut self,
        agg_with_accessor: &mut AggregationsWithAccessor,
    ) -> crate::Result<()> {
        for sub_aggregations in &mut self.sub_aggs.values_mut() {
            sub_aggregations.as_mut().flush(agg_with_accessor)?;
        }
        Ok(())
    }
}

/// The collector puts values from the fast field into the correct buckets and does a conversion to
/// the correct datatype.
#[derive(Clone, Debug)]
pub struct SegmentTermCollector {
    /// The buckets containing the aggregation data.
    term_buckets: TermBuckets,
    req: TermsAggregationInternal,
    blueprint: Option<Box<dyn SegmentAggregationCollector>>,
    field_type: ColumnType,
    accessor_idx: usize,
}

pub(crate) fn get_agg_name_and_property(name: &str) -> (&str, &str) {
    let (agg_name, agg_property) = name.split_once('.').unwrap_or((name, ""));
    (agg_name, agg_property)
}

impl SegmentAggregationCollector for SegmentTermCollector {
    fn add_intermediate_aggregation_result(
        self: Box<Self>,
        agg_with_accessor: &AggregationsWithAccessor,
        results: &mut IntermediateAggregationResults,
    ) -> crate::Result<()> {
        let name = agg_with_accessor.aggs.keys[self.accessor_idx].to_string();
        let agg_with_accessor = &agg_with_accessor.aggs.values[self.accessor_idx];

        let bucket = self.into_intermediate_bucket_result(agg_with_accessor)?;
        results.push(name, IntermediateAggregationResult::Bucket(bucket));

        Ok(())
    }

    #[inline]
    fn collect(
        &mut self,
        doc: crate::DocId,
        agg_with_accessor: &mut AggregationsWithAccessor,
    ) -> crate::Result<()> {
        self.collect_block(&[doc], agg_with_accessor)
    }

    #[inline]
    fn collect_block(
        &mut self,
        docs: &[crate::DocId],
        agg_with_accessor: &mut AggregationsWithAccessor,
    ) -> crate::Result<()> {
        let bucket_agg_accessor = &mut agg_with_accessor.aggs.values[self.accessor_idx];

        let mem_pre = self.get_memory_consumption();

        bucket_agg_accessor
            .column_block_accessor
            .fetch_block(docs, &bucket_agg_accessor.accessor);
        for term_id in bucket_agg_accessor.column_block_accessor.iter_vals() {
            let entry = self.term_buckets.entries.entry(term_id).or_default();
            *entry += 1;
        }
        // has subagg
        if let Some(blueprint) = self.blueprint.as_ref() {
            for (doc, term_id) in bucket_agg_accessor.column_block_accessor.iter_docid_vals() {
                let sub_aggregations = self
                    .term_buckets
                    .sub_aggs
                    .entry(term_id)
                    .or_insert_with(|| blueprint.clone());
                sub_aggregations.collect(doc, &mut bucket_agg_accessor.sub_aggregation)?;
            }
        }

        let mem_delta = self.get_memory_consumption() - mem_pre;
        bucket_agg_accessor
            .limits
            .add_memory_consumed(mem_delta as u64);
        bucket_agg_accessor.limits.validate_memory_consumption()?;

        Ok(())
    }

    fn flush(&mut self, agg_with_accessor: &mut AggregationsWithAccessor) -> crate::Result<()> {
        let sub_aggregation_accessor =
            &mut agg_with_accessor.aggs.values[self.accessor_idx].sub_aggregation;

        self.term_buckets.force_flush(sub_aggregation_accessor)?;
        Ok(())
    }
}

impl SegmentTermCollector {
    fn get_memory_consumption(&self) -> usize {
        let self_mem = std::mem::size_of::<Self>();
        let term_buckets_mem = self.term_buckets.get_memory_consumption();
        self_mem + term_buckets_mem
    }

    pub(crate) fn from_req_and_validate(
        req: &TermsAggregation,
        sub_aggregations: &mut AggregationsWithAccessor,
        field_type: ColumnType,
        accessor_idx: usize,
    ) -> crate::Result<Self> {
        let term_buckets = TermBuckets::default();

        if let Some(custom_order) = req.order.as_ref() {
            // Validate sub aggregtion exists
            if let OrderTarget::SubAggregation(sub_agg_name) = &custom_order.target {
                let (agg_name, _agg_property) = get_agg_name_and_property(sub_agg_name);

                sub_aggregations.aggs.get(agg_name).ok_or_else(|| {
                    TantivyError::InvalidArgument(format!(
                        "could not find aggregation with name {} in metric sub_aggregations",
                        agg_name
                    ))
                })?;
            }
        }

        let has_sub_aggregations = !sub_aggregations.is_empty();
        let blueprint = if has_sub_aggregations {
            let sub_aggregation = build_segment_agg_collector(sub_aggregations)?;
            Some(sub_aggregation)
        } else {
            None
        };

        Ok(SegmentTermCollector {
            req: TermsAggregationInternal::from_req(req),
            term_buckets,
            blueprint,
            field_type,
            accessor_idx,
        })
    }

    #[inline]
    pub(crate) fn into_intermediate_bucket_result(
        mut self,
        agg_with_accessor: &AggregationWithAccessor,
    ) -> crate::Result<IntermediateBucketResult> {
        let mut entries: Vec<(u64, u32)> = self.term_buckets.entries.into_iter().collect();

        let order_by_sub_aggregation =
            matches!(self.req.order.target, OrderTarget::SubAggregation(_));

        match self.req.order.target {
            OrderTarget::Key => {
                // We rely on the fact, that term ordinals match the order of the strings
                // TODO: We could have a special collector, that keeps only TOP n results at any
                // time.
                if self.req.order.order == Order::Desc {
                    entries.sort_unstable_by_key(|bucket| std::cmp::Reverse(bucket.0));
                } else {
                    entries.sort_unstable_by_key(|bucket| bucket.0);
                }
            }
            OrderTarget::SubAggregation(_name) => {
                // don't sort and cut off since it's hard to make assumptions on the quality of the
                // results when cutting off du to unknown nature of the sub_aggregation (possible
                // to check).
            }
            OrderTarget::Count => {
                if self.req.order.order == Order::Desc {
                    entries.sort_unstable_by_key(|bucket| std::cmp::Reverse(bucket.1));
                } else {
                    entries.sort_unstable_by_key(|bucket| bucket.1);
                }
            }
        }

        let (term_doc_count_before_cutoff, sum_other_doc_count) = if order_by_sub_aggregation {
            (0, 0)
        } else {
            cut_off_buckets(&mut entries, self.req.segment_size as usize)
        };

        let mut dict: FxHashMap<IntermediateKey, IntermediateTermBucketEntry> = Default::default();
        dict.reserve(entries.len());

        let mut into_intermediate_bucket_entry =
            |id, doc_count| -> crate::Result<IntermediateTermBucketEntry> {
                let intermediate_entry = if self.blueprint.as_ref().is_some() {
                    let mut sub_aggregation_res = IntermediateAggregationResults::default();
                    self.term_buckets
                        .sub_aggs
                        .remove(&id)
                        .unwrap_or_else(|| {
                            panic!(
                                "Internal Error: could not find subaggregation for id {}",
                                id
                            )
                        })
                        .add_intermediate_aggregation_result(
                            &agg_with_accessor.sub_aggregation,
                            &mut sub_aggregation_res,
                        )?;

                    IntermediateTermBucketEntry {
                        doc_count,
                        sub_aggregation: sub_aggregation_res,
                    }
                } else {
                    IntermediateTermBucketEntry {
                        doc_count,
                        sub_aggregation: Default::default(),
                    }
                };
                Ok(intermediate_entry)
            };

        if self.field_type == ColumnType::Str {
            let term_dict = agg_with_accessor
                .str_dict_column
                .as_ref()
                .expect("internal error: term dictionary not found for term aggregation");

            let mut buffer = String::new();
            for (term_id, doc_count) in entries {
                if !term_dict.ord_to_str(term_id, &mut buffer)? {
                    return Err(TantivyError::InternalError(format!(
                        "Couldn't find term_id {} in dict",
                        term_id
                    )));
                }

                let intermediate_entry = into_intermediate_bucket_entry(term_id, doc_count)?;

                dict.insert(IntermediateKey::Str(buffer.to_string()), intermediate_entry);
            }
            if self.req.min_doc_count == 0 {
                // TODO: Handle rev streaming for descending sorting by keys
                let mut stream = term_dict.dictionary().stream()?;
                while let Some((key, _ord)) = stream.next() {
                    if dict.len() >= self.req.segment_size as usize {
                        break;
                    }

                    let key = IntermediateKey::Str(
                        std::str::from_utf8(key)
                            .map_err(|utf8_err| DataCorruption::comment_only(utf8_err.to_string()))?
                            .to_string(),
                    );
                    dict.entry(key).or_default();
                }
            }
        } else {
            for (val, doc_count) in entries {
                let intermediate_entry = into_intermediate_bucket_entry(val, doc_count)?;
                let val = f64_from_fastfield_u64(val, &self.field_type);
                dict.insert(IntermediateKey::F64(val), intermediate_entry);
            }
        };

        Ok(IntermediateBucketResult::Terms(
            IntermediateTermBucketResult {
                entries: dict,
                sum_other_doc_count,
                doc_count_error_upper_bound: term_doc_count_before_cutoff,
            },
        ))
    }
}

pub(crate) trait GetDocCount {
    fn doc_count(&self) -> u64;
}
impl GetDocCount for (u64, u32) {
    fn doc_count(&self) -> u64 {
        self.1 as u64
    }
}
impl GetDocCount for (String, IntermediateTermBucketEntry) {
    fn doc_count(&self) -> u64 {
        self.1.doc_count as u64
    }
}

pub(crate) fn cut_off_buckets<T: GetDocCount + Debug>(
    entries: &mut Vec<T>,
    num_elem: usize,
) -> (u64, u64) {
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
    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::tests::{
        exec_request, exec_request_with_query, exec_request_with_query_and_memory_limit,
        get_test_index_from_terms, get_test_index_from_values_and_terms,
    };
    use crate::aggregation::AggregationLimits;

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

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "string_id",
                },
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        assert_eq!(res["my_texts"]["buckets"][0]["key"], "terma");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 5);
        assert_eq!(res["my_texts"]["buckets"][1]["key"], "termb");
        assert_eq!(res["my_texts"]["buckets"][1]["doc_count"], 2);
        assert_eq!(res["my_texts"]["buckets"][2]["key"], "termc");
        assert_eq!(res["my_texts"]["buckets"][2]["doc_count"], 1);
        assert_eq!(res["my_texts"]["sum_other_doc_count"], 0);

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "string_id",
                    "size": 2,
                    "segment_size": 2
                },
            }
        }))
        .unwrap();

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
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "string_id",
                    "size": 2,
                    "min_doc_count": 3,
                },
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        assert_eq!(res["my_texts"]["buckets"][0]["key"], "terma");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 5);
        assert_eq!(
            res["my_texts"]["buckets"][1]["key"],
            serde_json::Value::Null
        );
        assert_eq!(res["my_texts"]["sum_other_doc_count"], 0); // TODO sum_other_doc_count with min_doc_count
        Ok(())
    }

    #[test]
    fn terms_aggregation_test_order_count_single_segment() -> crate::Result<()> {
        terms_aggregation_test_order_count_merge_segment(true)
    }
    #[test]
    fn terms_aggregation_test_count_order() -> crate::Result<()> {
        terms_aggregation_test_order_count_merge_segment(false)
    }
    fn terms_aggregation_test_order_count_merge_segment(merge_segments: bool) -> crate::Result<()> {
        let segment_and_terms = vec![
            vec![(5.0, "terma".to_string())],
            vec![(2.0, "termb".to_string())],
            vec![(2.0, "terma".to_string())],
            vec![(1.0, "termc".to_string())],
            vec![(1.0, "termc".to_string())],
            vec![(1.0, "termc".to_string())],
            vec![(5.0, "terma".to_string())],
            vec![(5.0, "terma".to_string())],
            vec![(5.0, "terma".to_string())],
            vec![(8.0, "termb".to_string())],
            vec![(5.0, "terma".to_string())],
        ];
        let index = get_test_index_from_values_and_terms(merge_segments, &segment_and_terms)?;

        let sub_agg: Aggregations = serde_json::from_value(json!({
            "avg_score": {
                "avg": {
                    "field": "score",
                }
            },
            "stats_score": {
                "stats": {
                    "field": "score",
                }
            }
        }))
        .unwrap();

        // sub agg desc
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "string_id",
                    "order": {
                        "_count": "asc",
                    },
                },
                "aggs": sub_agg,
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        assert_eq!(res["my_texts"]["buckets"][0]["key"], "termb");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 2);
        assert_eq!(res["my_texts"]["buckets"][0]["avg_score"]["value"], 5.0);

        assert_eq!(res["my_texts"]["buckets"][1]["key"], "termc");
        assert_eq!(res["my_texts"]["buckets"][1]["doc_count"], 3);
        assert_eq!(res["my_texts"]["buckets"][1]["avg_score"]["value"], 1.0);

        assert_eq!(res["my_texts"]["buckets"][2]["key"], "terma");
        assert_eq!(res["my_texts"]["buckets"][2]["doc_count"], 6);
        assert_eq!(res["my_texts"]["buckets"][2]["avg_score"]["value"], 4.5);

        assert_eq!(res["my_texts"]["sum_other_doc_count"], 0);

        // Agg on non string
        //
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_scores1": {
                "terms": {
                    "field": "score",
                    "order": {
                        "_count": "asc",
                    },
                },
                "aggs": sub_agg,
            },
            "my_scores2": {
                "terms": {
                    "field": "score_f64",
                    "order": {
                        "_count": "asc",
                    },
                },
                "aggs": sub_agg,
            },
            "my_scores3": {
                "terms": {
                    "field": "score_i64",
                    "order": {
                        "_count": "asc",
                    },
                },
                "aggs": sub_agg,
            }

        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        assert_eq!(res["my_scores1"]["buckets"][0]["key"], 8.0);
        assert_eq!(res["my_scores1"]["buckets"][0]["doc_count"], 1);
        assert_eq!(res["my_scores1"]["buckets"][0]["avg_score"]["value"], 8.0);

        assert_eq!(res["my_scores1"]["buckets"][1]["key"], 2.0);
        assert_eq!(res["my_scores1"]["buckets"][1]["doc_count"], 2);
        assert_eq!(res["my_scores1"]["buckets"][1]["avg_score"]["value"], 2.0);

        assert_eq!(res["my_scores1"]["buckets"][2]["key"], 1.0);
        assert_eq!(res["my_scores1"]["buckets"][2]["doc_count"], 3);
        assert_eq!(res["my_scores1"]["buckets"][2]["avg_score"]["value"], 1.0);

        assert_eq!(res["my_scores1"]["buckets"][3]["key"], 5.0);
        assert_eq!(res["my_scores1"]["buckets"][3]["doc_count"], 5);
        assert_eq!(res["my_scores1"]["buckets"][3]["avg_score"]["value"], 5.0);

        assert_eq!(res["my_scores1"]["sum_other_doc_count"], 0);

        assert_eq!(res["my_scores2"]["buckets"][0]["key"], 8.0);
        assert_eq!(res["my_scores2"]["buckets"][0]["doc_count"], 1);
        assert_eq!(res["my_scores2"]["buckets"][0]["avg_score"]["value"], 8.0);

        assert_eq!(res["my_scores2"]["buckets"][1]["key"], 2.0);
        assert_eq!(res["my_scores2"]["buckets"][1]["doc_count"], 2);
        assert_eq!(res["my_scores2"]["buckets"][1]["avg_score"]["value"], 2.0);

        assert_eq!(res["my_scores2"]["buckets"][2]["key"], 1.0);
        assert_eq!(res["my_scores2"]["buckets"][2]["doc_count"], 3);
        assert_eq!(res["my_scores2"]["buckets"][2]["avg_score"]["value"], 1.0);

        assert_eq!(res["my_scores2"]["sum_other_doc_count"], 0);

        assert_eq!(res["my_scores3"]["buckets"][0]["key"], 8.0);
        assert_eq!(res["my_scores3"]["buckets"][0]["doc_count"], 1);
        assert_eq!(res["my_scores3"]["buckets"][0]["avg_score"]["value"], 8.0);

        assert_eq!(res["my_scores3"]["buckets"][1]["key"], 2.0);
        assert_eq!(res["my_scores3"]["buckets"][1]["doc_count"], 2);
        assert_eq!(res["my_scores3"]["buckets"][1]["avg_score"]["value"], 2.0);

        assert_eq!(res["my_scores3"]["buckets"][2]["key"], 1.0);
        assert_eq!(res["my_scores3"]["buckets"][2]["doc_count"], 3);
        assert_eq!(res["my_scores3"]["buckets"][2]["avg_score"]["value"], 1.0);

        assert_eq!(res["my_scores3"]["sum_other_doc_count"], 0);

        Ok(())
    }

    #[test]
    fn terms_aggregation_test_order_sub_agg_single_segment() -> crate::Result<()> {
        terms_aggregation_test_order_sub_agg_merge_segment(true)
    }
    #[test]
    fn terms_aggregation_test_sub_agg_order() -> crate::Result<()> {
        terms_aggregation_test_order_sub_agg_merge_segment(false)
    }
    fn terms_aggregation_test_order_sub_agg_merge_segment(
        merge_segments: bool,
    ) -> crate::Result<()> {
        let segment_and_terms = vec![
            vec![(5.0, "terma".to_string())],
            vec![(4.0, "termb".to_string())],
            vec![(1.0, "termc".to_string())],
            vec![(1.0, "termc".to_string())],
            vec![(1.0, "termc".to_string())],
            vec![(5.0, "terma".to_string())],
            vec![(5.0, "terma".to_string())],
            vec![(5.0, "terma".to_string())],
            vec![(8.0, "termb".to_string())],
            vec![(5.0, "terma".to_string())],
        ];
        let index = get_test_index_from_values_and_terms(merge_segments, &segment_and_terms)?;

        let sub_agg: Aggregations = serde_json::from_value(json!({
            "avg_score": {
                "avg": {
                    "field": "score",
                }
            },
            "stats_score": {
                "stats": {
                    "field": "score",
                }
            }
        }))
        .unwrap();

        // sub agg desc
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "string_id",
                    "order": {
                        "avg_score": "desc"
                    }
                },
                "aggs": sub_agg,
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        assert_eq!(res["my_texts"]["buckets"][0]["key"], "termb");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 2);
        assert_eq!(res["my_texts"]["buckets"][0]["avg_score"]["value"], 6.0);

        assert_eq!(res["my_texts"]["buckets"][1]["key"], "terma");
        assert_eq!(res["my_texts"]["buckets"][1]["doc_count"], 5);
        assert_eq!(res["my_texts"]["buckets"][1]["avg_score"]["value"], 5.0);

        assert_eq!(res["my_texts"]["buckets"][2]["key"], "termc");
        assert_eq!(res["my_texts"]["buckets"][2]["doc_count"], 3);
        assert_eq!(res["my_texts"]["buckets"][2]["avg_score"]["value"], 1.0);

        assert_eq!(res["my_texts"]["sum_other_doc_count"], 0);

        // sub agg asc
        //
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "string_id",
                    "order": {
                        "avg_score": "asc"
                    }
                },
                "aggs": sub_agg,
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;

        assert_eq!(res["my_texts"]["buckets"][0]["key"], "termc");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 3);
        assert_eq!(res["my_texts"]["buckets"][0]["avg_score"]["value"], 1.0);

        assert_eq!(res["my_texts"]["buckets"][1]["key"], "terma");
        assert_eq!(res["my_texts"]["buckets"][1]["doc_count"], 5);
        assert_eq!(res["my_texts"]["buckets"][1]["avg_score"]["value"], 5.0);

        assert_eq!(res["my_texts"]["buckets"][2]["key"], "termb");
        assert_eq!(res["my_texts"]["buckets"][2]["doc_count"], 2);
        assert_eq!(res["my_texts"]["buckets"][2]["avg_score"]["value"], 6.0);

        assert_eq!(res["my_texts"]["sum_other_doc_count"], 0);

        // sub agg multi value asc
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "string_id",
                    "order": {
                        "stats_score.avg": "asc"
                    }
                },
                "aggs": sub_agg,
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;

        assert_eq!(res["my_texts"]["buckets"][0]["key"], "termc");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 3);
        assert_eq!(res["my_texts"]["buckets"][0]["avg_score"]["value"], 1.0);

        assert_eq!(res["my_texts"]["buckets"][1]["key"], "terma");
        assert_eq!(res["my_texts"]["buckets"][1]["doc_count"], 5);
        assert_eq!(res["my_texts"]["buckets"][1]["avg_score"]["value"], 5.0);

        assert_eq!(res["my_texts"]["buckets"][2]["key"], "termb");
        assert_eq!(res["my_texts"]["buckets"][2]["doc_count"], 2);
        assert_eq!(res["my_texts"]["buckets"][2]["avg_score"]["value"], 6.0);

        assert_eq!(res["my_texts"]["sum_other_doc_count"], 0);

        // sub agg invalid request
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "string_id",
                    "order": {
                        "doesnotexist": "asc"
                    }
                },
                "aggs": sub_agg,
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index);
        assert!(res.is_err());

        Ok(())
    }

    #[test]
    fn terms_aggregation_test_order_key_single_segment() -> crate::Result<()> {
        terms_aggregation_test_order_key_merge_segment(true)
    }
    #[test]
    fn terms_aggregation_test_key_order() -> crate::Result<()> {
        terms_aggregation_test_order_key_merge_segment(false)
    }
    fn terms_aggregation_test_order_key_merge_segment(merge_segments: bool) -> crate::Result<()> {
        let segment_and_terms = vec![
            vec![(5.0, "terma".to_string())],
            vec![(4.0, "termb".to_string())],
            vec![(1.0, "termc".to_string())],
            vec![(1.0, "termc".to_string())],
            vec![(1.0, "termc".to_string())],
            vec![(5.0, "terma".to_string())],
            vec![(5.0, "terma".to_string())],
            vec![(5.0, "terma".to_string())],
            vec![(8.0, "termb".to_string())],
            vec![(5.0, "terma".to_string())],
        ];
        let index = get_test_index_from_values_and_terms(merge_segments, &segment_and_terms)?;

        // key asc
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "string_id",
                    "order": {
                        "_key": "asc"
                    }
                },
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        assert_eq!(res["my_texts"]["buckets"][0]["key"], "terma");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 5);
        assert_eq!(res["my_texts"]["buckets"][1]["key"], "termb");
        assert_eq!(res["my_texts"]["buckets"][1]["doc_count"], 2);
        assert_eq!(res["my_texts"]["buckets"][2]["key"], "termc");
        assert_eq!(res["my_texts"]["buckets"][2]["doc_count"], 3);
        assert_eq!(res["my_texts"]["sum_other_doc_count"], 0);

        // key desc and size cut_off
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "string_id",
                    "order": {
                        "_key": "asc"
                    },
                    "size": 2
                },
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        assert_eq!(res["my_texts"]["buckets"][0]["key"], "terma");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 5);
        assert_eq!(res["my_texts"]["buckets"][1]["key"], "termb");
        assert_eq!(res["my_texts"]["buckets"][1]["doc_count"], 2);
        assert_eq!(
            res["my_texts"]["buckets"][2]["doc_count"],
            serde_json::Value::Null
        );

        assert_eq!(res["my_texts"]["sum_other_doc_count"], 3);

        // key asc and segment_size cut_off
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "string_id",
                    "order": {
                        "_key": "asc"
                    },
                    "size": 2,
                    "segment_size": 2
                },
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        assert_eq!(res["my_texts"]["buckets"][0]["key"], "terma");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 5);
        assert_eq!(res["my_texts"]["buckets"][1]["key"], "termb");
        assert_eq!(res["my_texts"]["buckets"][1]["doc_count"], 2);
        assert_eq!(
            res["my_texts"]["buckets"][2]["doc_count"],
            serde_json::Value::Null
        );

        // key desc
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "string_id",
                    "order": {
                        "_key": "desc"
                    },
                },
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        assert_eq!(res["my_texts"]["buckets"][0]["key"], "termc");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 3);
        assert_eq!(res["my_texts"]["buckets"][1]["key"], "termb");
        assert_eq!(res["my_texts"]["buckets"][1]["doc_count"], 2);
        assert_eq!(res["my_texts"]["buckets"][2]["key"], "terma");
        assert_eq!(res["my_texts"]["buckets"][2]["doc_count"], 5);
        assert_eq!(res["my_texts"]["sum_other_doc_count"], 0);

        // key desc, size cut_off
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "string_id",
                    "order": {
                        "_key": "desc"
                    },
                    "size": 2
                },
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        assert_eq!(res["my_texts"]["buckets"][0]["key"], "termc");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 3);
        assert_eq!(res["my_texts"]["buckets"][1]["key"], "termb");
        assert_eq!(res["my_texts"]["buckets"][1]["doc_count"], 2);
        assert_eq!(
            res["my_texts"]["buckets"][2]["doc_count"],
            serde_json::Value::Null
        );
        assert_eq!(res["my_texts"]["sum_other_doc_count"], 5);

        // key desc, segment_size cut_off
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "string_id",
                    "order": {
                        "_key": "desc"
                    },
                    "size": 2,
                    "segment_size": 2
                },
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        assert_eq!(res["my_texts"]["buckets"][0]["key"], "termc");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 3);
        assert_eq!(res["my_texts"]["buckets"][1]["key"], "termb");
        assert_eq!(res["my_texts"]["buckets"][1]["doc_count"], 2);
        assert_eq!(
            res["my_texts"]["buckets"][2]["doc_count"],
            serde_json::Value::Null
        );

        Ok(())
    }

    #[test]
    fn terms_aggregation_min_doc_count_special_case() -> crate::Result<()> {
        let terms_per_segment = vec![
            vec!["terma", "terma", "termb", "termb", "termb", "termc"],
            vec!["terma", "terma", "termb", "termc", "termc"],
        ];

        let index = get_test_index_from_terms(false, &terms_per_segment)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "string_id",
                    "min_doc_count": 0,
                },
            }
        }))
        .unwrap();

        // searching for terma, but min_doc_count will return all terms
        let res = exec_request_with_query(agg_req, &index, Some(("string_id", "terma")))?;

        assert_eq!(res["my_texts"]["buckets"][0]["key"], "terma");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 4);
        assert_eq!(res["my_texts"]["buckets"][1]["key"], "termc");
        assert_eq!(res["my_texts"]["buckets"][1]["doc_count"], 0);
        assert_eq!(res["my_texts"]["buckets"][2]["key"], "termb");
        assert_eq!(res["my_texts"]["buckets"][2]["doc_count"], 0);
        assert_eq!(res["my_texts"]["sum_other_doc_count"], 0);
        assert_eq!(res["my_texts"]["doc_count_error_upper_bound"], 0);

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

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "string_id",
                    "size": 2,
                    "segment_size": 2
                },
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;

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

        // disable doc_count_error_upper_bound

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "string_id",
                    "size": 2,
                    "segment_size": 2,
                    "show_term_doc_count_error": false
                },
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;

        assert_eq!(res["my_texts"]["sum_other_doc_count"], 4);
        assert_eq!(
            res["my_texts"]["doc_count_error_upper_bound"],
            serde_json::Value::Null
        );

        Ok(())
    }

    #[test]
    fn terms_aggregation_term_bucket_limit() -> crate::Result<()> {
        let terms: Vec<String> = (0..20_000).map(|el| el.to_string()).collect();
        let terms_per_segment = vec![terms.iter().map(|el| el.as_str()).collect()];

        let index = get_test_index_from_terms(true, &terms_per_segment)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "string_id",
                    "min_doc_count": 0,
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query_and_memory_limit(
            agg_req,
            &index,
            None,
            AggregationLimits::new(Some(50_000), None),
        )
        .unwrap_err();
        assert!(res
            .to_string()
            .contains("Aborting aggregation because memory limit was exceeded. Limit: 50.00 KB"));

        Ok(())
    }

    #[test]
    fn terms_aggregation_different_tokenizer_on_ff_test() -> crate::Result<()> {
        let terms = vec!["Hello Hello", "Hallo Hallo"];

        let index = get_test_index_from_terms(true, &[terms])?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "text_id",
                    "min_doc_count": 0,
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None).unwrap();
        println!("{}", serde_json::to_string_pretty(&res).unwrap());

        assert_eq!(res["my_texts"]["buckets"][0]["key"], "Hallo Hallo");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 1);

        assert_eq!(res["my_texts"]["buckets"][1]["key"], "Hello Hello");
        assert_eq!(res["my_texts"]["buckets"][1]["doc_count"], 1);

        Ok(())
    }

    #[test]
    fn test_json_format() -> crate::Result<()> {
        let agg_req: Aggregations = serde_json::from_value(json!({
            "term_agg_test": {
                "terms": {
                    "field": "string_id",
                    "size": 2,
                    "segment_size": 2,
                    "order": {
                        "_key": "desc"
                    }
                },
            }
        }))
        .unwrap();

        let elasticsearch_compatible_json = json!(
        {
        "term_agg_test":{
            "terms": {
                "field": "string_id",
                "size": 2u64,
                "segment_size": 2u64,
                "order": {"_key": "desc"}
            }
        }
        });

        let agg_req_deser: Aggregations =
            serde_json::from_str(&serde_json::to_string(&elasticsearch_compatible_json).unwrap())
                .unwrap();
        assert_eq!(agg_req, agg_req_deser);

        let elasticsearch_compatible_json = json!(
        {
        "term_agg_test":{
            "terms": {
                "field": "string_id",
                "split_size": 2u64,
            }
        }
        });

        // test alias shard_size, split_size
        let agg_req: Aggregations = serde_json::from_value(json!({
            "term_agg_test": {
                "terms": {
                    "field": "string_id",
                    "split_size": 2,
                },
            }
        }))
        .unwrap();

        let agg_req_deser: Aggregations =
            serde_json::from_str(&serde_json::to_string(&elasticsearch_compatible_json).unwrap())
                .unwrap();
        assert_eq!(agg_req, agg_req_deser);

        let elasticsearch_compatible_json = json!(
        {
        "term_agg_test":{
            "terms": {
                "field": "string_id",
                "shard_size": 2u64,
            }
        }
        });

        let agg_req_deser: Aggregations =
            serde_json::from_str(&serde_json::to_string(&elasticsearch_compatible_json).unwrap())
                .unwrap();
        assert_eq!(agg_req, agg_req_deser);

        Ok(())
    }
}
