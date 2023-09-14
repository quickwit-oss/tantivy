use std::fmt::Debug;

use columnar::{BytesColumn, ColumnType, MonotonicallyMappableToU64, StrColumn};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use super::{CustomOrder, Order, OrderTarget};
use crate::aggregation::agg_limits::MemoryConsumption;
use crate::aggregation::agg_req_with_accessor::{
    AggregationWithAccessor, AggregationsWithAccessor,
};
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateBucketResult,
    IntermediateKey, IntermediateTermBucketEntry, IntermediateTermBucketResult,
};
use crate::aggregation::segment_agg_result::{
    build_segment_agg_collector, SegmentAggregationCollector,
};
use crate::aggregation::{f64_from_fastfield_u64, format_date, Key};
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

    /// The missing parameter defines how documents that are missing a value should be treated.
    /// By default they will be ignored but it is also possible to treat them as if they had a
    /// value. Examples in JSON format:
    /// { "missing": "NO_DATA" }
    ///
    /// # Internal
    ///
    /// Internally, `missing` requires some specialized handling in some scenarios.
    ///
    /// Simple Case:
    /// In the simplest case, we can just put the missing value in the termmap use that. In case of
    /// text we put a special u64::MAX and replace it at the end with the actual missing value,
    /// when loading the text.
    /// Special Case 1:
    /// If we have multiple columns on one field, we need to have a union on the indices on both
    /// columns, to find docids without a value. That requires a special missing aggreggation.
    /// Special Case 2: if the key is of type text and the column is numerical, we also need to use
    /// the special missing aggregation, since there is no mechanism in the numerical column to
    /// add text.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub missing: Option<Key>,
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
    pub missing: Option<Key>,
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
            missing: req.missing.clone(),
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
        results.push(name, IntermediateAggregationResult::Bucket(bucket))?;

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

        if let Some(missing) = bucket_agg_accessor.missing_value_for_accessor {
            bucket_agg_accessor
                .column_block_accessor
                .fetch_block_with_missing(docs, &bucket_agg_accessor.accessor, missing);
        } else {
            bucket_agg_accessor
                .column_block_accessor
                .fetch_block(docs, &bucket_agg_accessor.accessor);
        }

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
            .add_memory_consumed(mem_delta as u64)?;

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
        if field_type == ColumnType::Bytes || field_type == ColumnType::Bool {
            return Err(TantivyError::InvalidArgument(format!(
                "terms aggregation is not supported for column type {:?}",
                field_type
            )));
        }
        let term_buckets = TermBuckets::default();

        if let Some(custom_order) = req.order.as_ref() {
            // Validate sub aggregtion exists
            if let OrderTarget::SubAggregation(sub_agg_name) = &custom_order.target {
                let (agg_name, _agg_property) = get_agg_name_and_property(sub_agg_name);

                sub_aggregations.aggs.get(agg_name).ok_or_else(|| {
                    TantivyError::InvalidArgument(format!(
                        "could not find aggregation with name {agg_name} in metric \
                         sub_aggregations"
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
                            panic!("Internal Error: could not find subaggregation for id {id}")
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
                .cloned()
                .unwrap_or_else(|| {
                    StrColumn::wrap(BytesColumn::empty(agg_with_accessor.accessor.num_docs()))
                });
            let mut buffer = String::new();
            for (term_id, doc_count) in entries {
                let intermediate_entry = into_intermediate_bucket_entry(term_id, doc_count)?;
                // Special case for missing key
                if term_id == u64::MAX {
                    let missing_key = self
                        .req
                        .missing
                        .as_ref()
                        .expect("Found placeholder term_id but `missing` is None");
                    match missing_key {
                        Key::Str(missing) => {
                            buffer.clear();
                            buffer.push_str(missing);
                            dict.insert(
                                IntermediateKey::Str(buffer.to_string()),
                                intermediate_entry,
                            );
                        }
                        Key::F64(val) => {
                            buffer.push_str(&val.to_string());
                            dict.insert(IntermediateKey::F64(*val), intermediate_entry);
                        }
                    }
                } else {
                    if !term_dict.ord_to_str(term_id, &mut buffer)? {
                        return Err(TantivyError::InternalError(format!(
                            "Couldn't find term_id {term_id} in dict"
                        )));
                    }
                    dict.insert(IntermediateKey::Str(buffer.to_string()), intermediate_entry);
                }
            }
            if self.req.min_doc_count == 0 {
                // TODO: Handle rev streaming for descending sorting by keys
                let mut stream = term_dict.dictionary().stream()?;
                let empty_sub_aggregation = IntermediateAggregationResults::empty_from_req(
                    agg_with_accessor.agg.sub_aggregation(),
                );
                while let Some((key, _ord)) = stream.next() {
                    if dict.len() >= self.req.segment_size as usize {
                        break;
                    }

                    let key = IntermediateKey::Str(
                        std::str::from_utf8(key)
                            .map_err(|utf8_err| DataCorruption::comment_only(utf8_err.to_string()))?
                            .to_string(),
                    );

                    dict.entry(key.clone())
                        .or_insert_with(|| IntermediateTermBucketEntry {
                            doc_count: 0,
                            sub_aggregation: empty_sub_aggregation.clone(),
                        });
                }
            }
        } else if self.field_type == ColumnType::DateTime {
            for (val, doc_count) in entries {
                let intermediate_entry = into_intermediate_bucket_entry(val, doc_count)?;
                let val = i64::from_u64(val);
                let date = format_date(val)?;
                dict.insert(IntermediateKey::Str(date), intermediate_entry);
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
    use common::DateTime;
    use time::{Date, Month};

    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::tests::{
        exec_request, exec_request_with_query, exec_request_with_query_and_memory_limit,
        get_test_index_from_terms, get_test_index_from_values_and_terms,
    };
    use crate::aggregation::AggregationLimits;
    use crate::indexer::NoMergePolicy;
    use crate::schema::{Schema, FAST, STRING};
    use crate::Index;

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
    fn terms_aggregation_min_doc_count_special_case_with_sub_agg_empty_merge() -> crate::Result<()>
    {
        let mut schema_builder = Schema::builder();
        let string_field_1 = schema_builder.add_text_field("string1", STRING | FAST);
        let string_field_2 = schema_builder.add_text_field("string2", STRING | FAST);
        let score_fieldtype = crate::schema::NumericOptions::default().set_fast();
        let score_field = schema_builder.add_u64_field("score", score_fieldtype);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.set_merge_policy(Box::new(NoMergePolicy));
            // writing the segment
            index_writer.add_document(doc!(
                string_field_1 => "A".to_string(),
                string_field_2 => "hit".to_string(),
                score_field => 1u64,
            ))?;
            index_writer.add_document(doc!(
                string_field_1 => "B".to_string(),
                string_field_2 => "nohit".to_string(), // this doc gets filtered in this segment,
                                                       // but the term will still be loaded because
                                                       // min_doc_count == 0
                score_field => 2u64,
            ))?;
            index_writer.commit()?;

            index_writer.add_document(doc!(
                string_field_1 => "A".to_string(),
                string_field_2 => "hit".to_string(),
                score_field => 2u64,
            ))?;
            index_writer.add_document(doc!(
                string_field_1 => "B".to_string(),
                string_field_2 => "hit".to_string(),
                score_field => 4u64,
            ))?;
            index_writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "string1",
                    "min_doc_count": 0,
                },
                "aggs":{
                    "elhistogram": {
                        "histogram": {
                            "field": "score",
                            "interval": 1
                        }
                    }
                }
            }
        }))
        .unwrap();

        // searching for terma, but min_doc_count will return all terms
        let res = exec_request_with_query(agg_req, &index, Some(("string2", "hit")))?;

        assert_eq!(res["my_texts"]["buckets"][0]["key"], "A");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 2);
        assert_eq!(
            res["my_texts"]["buckets"][0]["elhistogram"]["buckets"],
            json!([{ "doc_count": 1, "key": 1.0 }, { "doc_count": 1, "key": 2.0 } ])
        );
        assert_eq!(res["my_texts"]["buckets"][1]["key"], "B");
        assert_eq!(res["my_texts"]["buckets"][1]["doc_count"], 1);
        assert_eq!(
            res["my_texts"]["buckets"][1]["elhistogram"]["buckets"],
            json!([ { "doc_count": 1, "key": 4.0 } ])
        );
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
        assert_eq!(index.searchable_segments().unwrap().len(), 2);

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
    #[test]
    fn terms_empty_json() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let json = schema_builder.add_json_field("json", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests().unwrap();
        // => Segment with empty json
        index_writer.add_document(doc!()).unwrap();
        index_writer.commit().unwrap();
        // => Segment with json, but no field partially_empty
        index_writer
            .add_document(doc!(json => json!({"different_field": "blue"})))
            .unwrap();
        index_writer.commit().unwrap();
        //// => Segment with field partially_empty
        index_writer
            .add_document(doc!(json => json!({"partially_empty": "blue"})))
            .unwrap();
        index_writer.add_document(doc!())?;
        index_writer.commit().unwrap();

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "json.partially_empty"
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        assert_eq!(res["my_texts"]["buckets"][0]["key"], "blue");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 1);
        assert_eq!(res["my_texts"]["buckets"][1], serde_json::Value::Null);
        assert_eq!(res["my_texts"]["sum_other_doc_count"], 0);
        assert_eq!(res["my_texts"]["doc_count_error_upper_bound"], 0);

        Ok(())
    }

    #[test]
    fn terms_aggregation_bytes() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let bytes_field = schema_builder.add_bytes_field("bytes", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.set_merge_policy(Box::new(NoMergePolicy));
            index_writer.add_document(doc!(
                bytes_field => vec![1,2,3],
            ))?;
            index_writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "bytes"
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        // TODO: Returning an error would be better instead of an empty result, since this is not a
        // JSON field
        assert_eq!(
            res["my_texts"]["buckets"][0]["key"],
            serde_json::Value::Null
        );
        assert_eq!(res["my_texts"]["sum_other_doc_count"], 0);
        assert_eq!(res["my_texts"]["doc_count_error_upper_bound"], 0);

        Ok(())
    }

    #[test]
    fn terms_aggregation_missing_multi_value() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", FAST);
        let id_field = schema_builder.add_u64_field("id", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.set_merge_policy(Box::new(NoMergePolicy));
            index_writer.add_document(doc!(
                text_field => "Hello Hello",
                text_field => "Hello Hello",
                id_field => 1u64,
                id_field => 1u64,
            ))?;
            // Missing
            index_writer.add_document(doc!())?;
            index_writer.add_document(doc!(
                text_field => "Hello Hello",
            ))?;
            index_writer.add_document(doc!(
                text_field => "Hello Hello",
            ))?;
            index_writer.commit()?;
            // Empty segment special case
            index_writer.add_document(doc!())?;
            index_writer.commit()?;
            // Full segment special case
            index_writer.add_document(doc!(
                text_field => "Hello Hello",
                id_field => 1u64,
            ))?;
            index_writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "text",
                    "missing": "Empty"
                },
            },
            "my_texts2": {
                "terms": {
                    "field": "text",
                    "missing": 1337
                },
            },
            "my_ids": {
                "terms": {
                    "field": "id",
                    "missing": 1337
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        // text field
        assert_eq!(res["my_texts"]["buckets"][0]["key"], "Hello Hello");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 5);
        assert_eq!(res["my_texts"]["buckets"][1]["key"], "Empty");
        assert_eq!(res["my_texts"]["buckets"][1]["doc_count"], 2);
        assert_eq!(
            res["my_texts"]["buckets"][2]["key"],
            serde_json::Value::Null
        );
        // text field with numner as missing fallback
        assert_eq!(res["my_texts2"]["buckets"][0]["key"], "Hello Hello");
        assert_eq!(res["my_texts2"]["buckets"][0]["doc_count"], 5);
        assert_eq!(res["my_texts2"]["buckets"][1]["key"], 1337.0);
        assert_eq!(res["my_texts2"]["buckets"][1]["doc_count"], 2);
        assert_eq!(
            res["my_texts2"]["buckets"][2]["key"],
            serde_json::Value::Null
        );
        assert_eq!(res["my_texts"]["sum_other_doc_count"], 0);
        assert_eq!(res["my_texts"]["doc_count_error_upper_bound"], 0);

        // id field
        assert_eq!(res["my_ids"]["buckets"][0]["key"], 1337.0);
        assert_eq!(res["my_ids"]["buckets"][0]["doc_count"], 4);
        assert_eq!(res["my_ids"]["buckets"][1]["key"], 1.0);
        assert_eq!(res["my_ids"]["buckets"][1]["doc_count"], 3);
        assert_eq!(res["my_ids"]["buckets"][2]["key"], serde_json::Value::Null);

        Ok(())
    }
    #[test]
    fn terms_aggregation_missing_simple_id() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_u64_field("id", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.set_merge_policy(Box::new(NoMergePolicy));
            index_writer.add_document(doc!(
                id_field => 1u64,
            ))?;
            // Missing
            index_writer.add_document(doc!())?;
            index_writer.add_document(doc!())?;
            index_writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_ids": {
                "terms": {
                    "field": "id",
                    "missing": 1337
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        // id field
        assert_eq!(res["my_ids"]["buckets"][0]["key"], 1337.0);
        assert_eq!(res["my_ids"]["buckets"][0]["doc_count"], 2);
        assert_eq!(res["my_ids"]["buckets"][1]["key"], 1.0);
        assert_eq!(res["my_ids"]["buckets"][1]["doc_count"], 1);
        assert_eq!(res["my_ids"]["buckets"][2]["key"], serde_json::Value::Null);

        Ok(())
    }

    #[test]
    fn terms_aggregation_missing1() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", FAST);
        let id_field = schema_builder.add_u64_field("id", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.set_merge_policy(Box::new(NoMergePolicy));
            index_writer.add_document(doc!(
                text_field => "Hello Hello",
                id_field => 1u64,
            ))?;
            // Missing
            index_writer.add_document(doc!())?;
            index_writer.add_document(doc!(
                text_field => "Hello Hello",
            ))?;
            index_writer.add_document(doc!(
                text_field => "Hello Hello",
            ))?;
            index_writer.commit()?;
            // Empty segment special case
            index_writer.add_document(doc!())?;
            index_writer.commit()?;
            // Full segment special case
            index_writer.add_document(doc!(
                text_field => "Hello Hello",
                id_field => 1u64,
            ))?;
            index_writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "text",
                    "missing": "Empty"
                },
            },
            "my_texts2": {
                "terms": {
                    "field": "text",
                    "missing": 1337
                },
            },
            "my_ids": {
                "terms": {
                    "field": "id",
                    "missing": 1337
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        // text field
        assert_eq!(res["my_texts"]["buckets"][0]["key"], "Hello Hello");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 4);
        assert_eq!(res["my_texts"]["buckets"][1]["key"], "Empty");
        assert_eq!(res["my_texts"]["buckets"][1]["doc_count"], 2);
        assert_eq!(
            res["my_texts"]["buckets"][2]["key"],
            serde_json::Value::Null
        );
        // text field with numner as missing fallback
        assert_eq!(res["my_texts2"]["buckets"][0]["key"], "Hello Hello");
        assert_eq!(res["my_texts2"]["buckets"][0]["doc_count"], 4);
        assert_eq!(res["my_texts2"]["buckets"][1]["key"], 1337.0);
        assert_eq!(res["my_texts2"]["buckets"][1]["doc_count"], 2);
        assert_eq!(
            res["my_texts2"]["buckets"][2]["key"],
            serde_json::Value::Null
        );
        assert_eq!(res["my_texts"]["sum_other_doc_count"], 0);
        assert_eq!(res["my_texts"]["doc_count_error_upper_bound"], 0);

        // id field
        assert_eq!(res["my_ids"]["buckets"][0]["key"], 1337.0);
        assert_eq!(res["my_ids"]["buckets"][0]["doc_count"], 4);
        assert_eq!(res["my_ids"]["buckets"][1]["key"], 1.0);
        assert_eq!(res["my_ids"]["buckets"][1]["doc_count"], 2);
        assert_eq!(res["my_ids"]["buckets"][2]["key"], serde_json::Value::Null);

        Ok(())
    }
    #[test]
    fn terms_aggregation_missing_empty() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("text", FAST);
        schema_builder.add_u64_field("id", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.set_merge_policy(Box::new(NoMergePolicy));
            // Empty segment special case
            index_writer.add_document(doc!())?;
            index_writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "text",
                    "missing": "Empty"
                },
            },
            "my_texts2": {
                "terms": {
                    "field": "text",
                    "missing": 1337
                },
            },
            "my_ids": {
                "terms": {
                    "field": "id",
                    "missing": 1337
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        // text field
        assert_eq!(res["my_texts"]["buckets"][0]["key"], "Empty");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 1);
        assert_eq!(
            res["my_texts"]["buckets"][1]["key"],
            serde_json::Value::Null
        );
        // text field with number as missing fallback
        assert_eq!(res["my_texts2"]["buckets"][0]["key"], 1337.0);
        assert_eq!(res["my_texts2"]["buckets"][0]["doc_count"], 1);
        assert_eq!(
            res["my_texts2"]["buckets"][1]["key"],
            serde_json::Value::Null
        );
        assert_eq!(res["my_texts"]["sum_other_doc_count"], 0);
        assert_eq!(res["my_texts"]["doc_count_error_upper_bound"], 0);

        // id field
        assert_eq!(res["my_ids"]["buckets"][0]["key"], 1337.0);
        assert_eq!(res["my_ids"]["buckets"][0]["doc_count"], 1);
        assert_eq!(res["my_ids"]["buckets"][1]["key"], serde_json::Value::Null);

        Ok(())
    }

    #[test]
    fn terms_aggregation_date() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let date_field = schema_builder.add_date_field("date_field", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut writer = index.writer_with_num_threads(1, 15_000_000)?;
            writer.add_document(doc!(date_field=>DateTime::from_primitive(Date::from_calendar_date(1982, Month::September, 17)?.with_hms(0, 0, 0)?)))?;
            writer.add_document(doc!(date_field=>DateTime::from_primitive(Date::from_calendar_date(1982, Month::September, 17)?.with_hms(0, 0, 0)?)))?;
            writer.add_document(doc!(date_field=>DateTime::from_primitive(Date::from_calendar_date(1983, Month::September, 27)?.with_hms(0, 0, 0)?)))?;
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_date": {
                "terms": {
                    "field": "date_field"
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        // date_field field
        assert_eq!(res["my_date"]["buckets"][0]["key"], "1982-09-17T00:00:00Z");
        assert_eq!(res["my_date"]["buckets"][0]["doc_count"], 2);
        assert_eq!(res["my_date"]["buckets"][1]["key"], "1983-09-27T00:00:00Z");
        assert_eq!(res["my_date"]["buckets"][1]["doc_count"], 1);
        assert_eq!(res["my_date"]["buckets"][2]["key"], serde_json::Value::Null);

        Ok(())
    }
    #[test]
    fn terms_aggregation_date_missing() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let date_field = schema_builder.add_date_field("date_field", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut writer = index.writer_with_num_threads(1, 15_000_000)?;
            writer.add_document(doc!(date_field=>DateTime::from_primitive(Date::from_calendar_date(1982, Month::September, 17)?.with_hms(0, 0, 0)?)))?;
            writer.add_document(doc!(date_field=>DateTime::from_primitive(Date::from_calendar_date(1982, Month::September, 17)?.with_hms(0, 0, 0)?)))?;
            writer.add_document(doc!(date_field=>DateTime::from_primitive(Date::from_calendar_date(1983, Month::September, 27)?.with_hms(0, 0, 0)?)))?;
            writer.add_document(doc!())?;
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_date": {
                "terms": {
                    "field": "date_field",
                    "missing": "1982-09-17T00:00:00Z"
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        // date_field field
        assert_eq!(res["my_date"]["buckets"][0]["key"], "1982-09-17T00:00:00Z");
        assert_eq!(res["my_date"]["buckets"][0]["doc_count"], 3);
        assert_eq!(res["my_date"]["buckets"][1]["key"], "1983-09-27T00:00:00Z");
        assert_eq!(res["my_date"]["buckets"][1]["doc_count"], 1);
        assert_eq!(res["my_date"]["buckets"][2]["key"], serde_json::Value::Null);

        Ok(())
    }
}
