use std::fmt::Debug;
use std::io;
use std::net::Ipv6Addr;

use columnar::column_values::CompactSpaceU64Accessor;
use columnar::{
    Column, ColumnBlockAccessor, ColumnType, Dictionary, MonotonicallyMappableToU128,
    MonotonicallyMappableToU64, NumericalValue, StrColumn,
};
use common::BitSet;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use super::{CustomOrder, Order, OrderTarget};
use crate::aggregation::agg_data::{
    build_segment_agg_collectors, AggRefNode, AggregationsSegmentCtx,
};
use crate::aggregation::agg_limits::MemoryConsumption;
use crate::aggregation::agg_req::Aggregations;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateBucketResult,
    IntermediateKey, IntermediateTermBucketEntry, IntermediateTermBucketResult,
};
use crate::aggregation::segment_agg_result::SegmentAggregationCollector;
use crate::aggregation::{format_date, Key};
use crate::error::DataCorruption;
use crate::TantivyError;

/// Contains all information required by the SegmentTermCollector to perform the
/// terms aggregation on a segment.
pub struct TermsAggReqData {
    /// The column accessor to access the fast field values.
    pub accessor: Column<u64>,
    /// The type of the column.
    pub column_type: ColumnType,
    /// The string dictionary column if the field is of type text.
    pub str_dict_column: Option<StrColumn>,
    /// The missing value as u64 value.
    pub missing_value_for_accessor: Option<u64>,
    /// The column block accessor to access the fast field values.
    pub column_block_accessor: ColumnBlockAccessor<u64>,
    /// Note: sub_aggregation_blueprint is filled later when building collectors
    pub sub_aggregation_blueprint: Option<Box<dyn SegmentAggregationCollector>>,
    /// Used to build the correct nested result when we have an empty result.
    pub sug_aggregations: Aggregations,
    /// The name of the aggregation.
    pub name: String,
    /// The normalized term aggregation request.
    pub req: TermsAggregationInternal,
    /// Preloaded allowed term ords (string columns only). If set, only ords present are collected.
    pub allowed_term_ids: Option<BitSet>,
}

impl TermsAggReqData {
    /// Estimate the memory consumption of this struct in bytes.
    pub fn get_memory_consumption(&self) -> usize {
        std::mem::size_of::<Self>()
            + std::mem::size_of::<TermsAggregationInternal>()
            + self
                .allowed_term_ids
                .as_ref()
                .map(|bs| bs.len() / 8)
                .unwrap_or(0)
    }
}

/// Creates a bucket for every unique term and counts the number of occurrences.
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

#[derive(Clone, Debug, PartialEq)]
pub enum IncludeExcludeParam {
    /// A single string pattern is treated as regex.
    Regex(String),
    /// An array of strings is treated as exact values.
    Values(Vec<String>),
}

impl Serialize for IncludeExcludeParam {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        match self {
            IncludeExcludeParam::Regex(s) => serializer.serialize_str(s),
            IncludeExcludeParam::Values(v) => v.serialize(serializer),
        }
    }
}

// Custom deserializer to accept either a single string (regex) or an array of strings (values).
impl<'de> Deserialize<'de> for IncludeExcludeParam {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        use serde::de::{self, SeqAccess, Visitor};
        struct IncludeExcludeVisitor;

        impl<'de> Visitor<'de> for IncludeExcludeVisitor {
            type Value = IncludeExcludeParam;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string (regex) or an array of strings")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where E: de::Error {
                Ok(IncludeExcludeParam::Regex(v.to_string()))
            }

            fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
            where E: de::Error {
                Ok(IncludeExcludeParam::Regex(v.to_string()))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where E: de::Error {
                Ok(IncludeExcludeParam::Regex(v))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where A: SeqAccess<'de> {
                let mut values: Vec<String> = Vec::new();
                while let Some(elem) = seq.next_element::<String>()? {
                    values.push(elem);
                }
                Ok(IncludeExcludeParam::Values(values))
            }
        }

        deserializer.deserialize_any(IncludeExcludeVisitor)
    }
}

/// The terms aggregation allows you to group documents by unique values of a field.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TermsAggregation {
    /// The field to aggregate on.
    pub field: String,
    /// By default, the top 10 terms with the most documents are returned.
    /// Larger values for size are more expensive.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub size: Option<u32>,

    /// To get more accurate results, we fetch more than `size` from each segment.
    ///
    /// Increasing this value is will increase the cost for more accuracy.
    ///
    /// Defaults to 10 * size.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    #[serde(alias = "shard_size")]
    #[serde(alias = "split_size")]
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
    /// columns, to find docids without a value. That requires a special missing aggregation.
    /// Special Case 2: if the key is of type text and the column is numerical, we also need to use
    /// the special missing aggregation, since there is no mechanism in the numerical column to
    /// add text.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub missing: Option<Key>,

    /// Include terms by either regex (single string) or exact values (array).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub include: Option<IncludeExcludeParam>,
    /// Exclude terms by either regex (single string) or exact values (array).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub exclude: Option<IncludeExcludeParam>,
}

/// Same as TermsAggregation, but with populated defaults.
#[derive(Clone, Debug, PartialEq)]
pub struct TermsAggregationInternal {
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

    /// Set the order. `String` is here a target, which is either "_count", "_key", or the name of
    /// a metric sub_aggregation.
    pub order: CustomOrder,

    /// The missing parameter defines how documents that are missing a value should be treated.
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

    fn force_flush(&mut self, agg_data: &mut AggregationsSegmentCtx) -> crate::Result<()> {
        for sub_aggregations in &mut self.sub_aggs.values_mut() {
            sub_aggregations.as_mut().flush(agg_data)?;
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
    accessor_idx: usize,
}

pub(crate) fn get_agg_name_and_property(name: &str) -> (&str, &str) {
    let (agg_name, agg_property) = name.split_once('.').unwrap_or((name, ""));
    (agg_name, agg_property)
}

impl SegmentAggregationCollector for SegmentTermCollector {
    fn add_intermediate_aggregation_result(
        self: Box<Self>,
        agg_data: &AggregationsSegmentCtx,
        results: &mut IntermediateAggregationResults,
    ) -> crate::Result<()> {
        let name = agg_data.get_term_req_data(self.accessor_idx).name.clone();

        let bucket = self.into_intermediate_bucket_result(agg_data)?;
        results.push(name, IntermediateAggregationResult::Bucket(bucket))?;

        Ok(())
    }

    #[inline]
    fn collect(
        &mut self,
        doc: crate::DocId,
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        self.collect_block(&[doc], agg_data)
    }

    #[inline]
    fn collect_block(
        &mut self,
        docs: &[crate::DocId],
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        let mut req_data = agg_data.take_term_req_data(self.accessor_idx);

        let mem_pre = self.get_memory_consumption();

        if let Some(missing) = req_data.missing_value_for_accessor {
            req_data.column_block_accessor.fetch_block_with_missing(
                docs,
                &req_data.accessor,
                missing,
            );
        } else {
            req_data
                .column_block_accessor
                .fetch_block(docs, &req_data.accessor);
        }

        for term_id in req_data.column_block_accessor.iter_vals() {
            if let Some(allowed_bs) = req_data.allowed_term_ids.as_ref() {
                if !allowed_bs.contains(term_id as u32) {
                    continue;
                }
            }
            let entry = self.term_buckets.entries.entry(term_id).or_default();
            *entry += 1;
        }
        // has subagg
        if let Some(blueprint) = req_data.sub_aggregation_blueprint.as_ref() {
            for (doc, term_id) in req_data
                .column_block_accessor
                .iter_docid_vals(docs, &req_data.accessor)
            {
                if let Some(allowed_bs) = req_data.allowed_term_ids.as_ref() {
                    if !allowed_bs.contains(term_id as u32) {
                        continue;
                    }
                }
                let sub_aggregations = self
                    .term_buckets
                    .sub_aggs
                    .entry(term_id)
                    .or_insert_with(|| blueprint.clone());
                sub_aggregations.collect(doc, agg_data)?;
            }
        }

        let mem_delta = self.get_memory_consumption() - mem_pre;
        if mem_delta > 0 {
            agg_data.limits.add_memory_consumed(mem_delta as u64)?;
        }
        agg_data.put_back_term_req_data(self.accessor_idx, req_data);

        Ok(())
    }

    fn flush(&mut self, agg_data: &mut AggregationsSegmentCtx) -> crate::Result<()> {
        self.term_buckets.force_flush(agg_data)?;
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
        req_data: &mut AggregationsSegmentCtx,
        node: &AggRefNode,
    ) -> crate::Result<Self> {
        let terms_req_data = req_data.get_term_req_data(node.idx_in_req_data);
        let column_type = terms_req_data.column_type;
        let accessor_idx = node.idx_in_req_data;
        if column_type == ColumnType::Bytes {
            return Err(TantivyError::InvalidArgument(format!(
                "terms aggregation is not supported for column type {column_type:?}"
            )));
        }
        let term_buckets = TermBuckets::default();

        // Validate sub aggregation exists
        if let OrderTarget::SubAggregation(sub_agg_name) = &terms_req_data.req.order.target {
            let (agg_name, _agg_property) = get_agg_name_and_property(sub_agg_name);

            node.get_sub_agg(agg_name, &req_data.per_request)
                .ok_or_else(|| {
                    TantivyError::InvalidArgument(format!(
                        "could not find aggregation with name {agg_name} in metric \
                         sub_aggregations"
                    ))
                })?;
        }

        let has_sub_aggregations = !node.children.is_empty();
        let blueprint = if has_sub_aggregations {
            let sub_aggregation = build_segment_agg_collectors(req_data, &node.children)?;
            Some(sub_aggregation)
        } else {
            None
        };
        let terms_req_data = req_data.get_term_req_data_mut(node.idx_in_req_data);
        terms_req_data.sub_aggregation_blueprint = blueprint;

        Ok(SegmentTermCollector {
            term_buckets,
            accessor_idx,
        })
    }

    #[inline]
    pub(crate) fn into_intermediate_bucket_result(
        mut self,
        agg_data: &AggregationsSegmentCtx,
    ) -> crate::Result<IntermediateBucketResult> {
        let term_req = agg_data.get_term_req_data(self.accessor_idx);
        let mut entries: Vec<(u64, u32)> = self.term_buckets.entries.into_iter().collect();

        let order_by_sub_aggregation =
            matches!(term_req.req.order.target, OrderTarget::SubAggregation(_));

        match &term_req.req.order.target {
            OrderTarget::Key => {
                // We rely on the fact, that term ordinals match the order of the strings
                // TODO: We could have a special collector, that keeps only TOP n results at any
                // time.
                if term_req.req.order.order == Order::Desc {
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
                if term_req.req.order.order == Order::Desc {
                    entries.sort_unstable_by_key(|bucket| std::cmp::Reverse(bucket.1));
                } else {
                    entries.sort_unstable_by_key(|bucket| bucket.1);
                }
            }
        }

        let (term_doc_count_before_cutoff, sum_other_doc_count) = if order_by_sub_aggregation {
            (0, 0)
        } else {
            cut_off_buckets(&mut entries, term_req.req.segment_size as usize)
        };

        let mut dict: FxHashMap<IntermediateKey, IntermediateTermBucketEntry> = Default::default();
        dict.reserve(entries.len());

        let mut into_intermediate_bucket_entry =
            |id, doc_count| -> crate::Result<IntermediateTermBucketEntry> {
                let intermediate_entry = if term_req.sub_aggregation_blueprint.as_ref().is_some() {
                    let mut sub_aggregation_res = IntermediateAggregationResults::default();
                    self.term_buckets
                        .sub_aggs
                        .remove(&id)
                        .unwrap_or_else(|| {
                            panic!("Internal Error: could not find subaggregation for id {id}")
                        })
                        .add_intermediate_aggregation_result(agg_data, &mut sub_aggregation_res)?;

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

        if term_req.column_type == ColumnType::Str {
            let fallback_dict = Dictionary::empty();
            let term_dict = term_req
                .str_dict_column
                .as_ref()
                .map(|el| el.dictionary())
                .unwrap_or_else(|| &fallback_dict);
            let mut buffer = Vec::new();

            // special case for missing key
            if let Some(index) = entries.iter().position(|value| value.0 == u64::MAX) {
                let entry = entries[index];
                let intermediate_entry = into_intermediate_bucket_entry(entry.0, entry.1)?;
                let missing_key = term_req
                    .req
                    .missing
                    .as_ref()
                    .expect("Found placeholder term_id but `missing` is None");
                match missing_key {
                    Key::Str(missing) => {
                        buffer.clear();
                        buffer.extend_from_slice(missing.as_bytes());
                        dict.insert(
                            IntermediateKey::Str(
                                String::from_utf8(buffer.to_vec())
                                    .expect("could not convert to String"),
                            ),
                            intermediate_entry,
                        );
                    }
                    Key::F64(val) => {
                        dict.insert(IntermediateKey::F64(*val), intermediate_entry);
                    }
                    Key::U64(val) => {
                        dict.insert(IntermediateKey::U64(*val), intermediate_entry);
                    }
                    Key::I64(val) => {
                        dict.insert(IntermediateKey::I64(*val), intermediate_entry);
                    }
                }

                entries.swap_remove(index);
            }

            // Sort by term ord
            entries.sort_unstable_by_key(|bucket| bucket.0);
            let mut idx = 0;
            term_dict.sorted_ords_to_term_cb(
                entries.iter().map(|(term_id, _)| *term_id),
                |term| {
                    let entry = entries[idx];
                    let intermediate_entry = into_intermediate_bucket_entry(entry.0, entry.1)
                        .map_err(io::Error::other)?;
                    dict.insert(
                        IntermediateKey::Str(
                            String::from_utf8(term.to_vec()).expect("could not convert to String"),
                        ),
                        intermediate_entry,
                    );
                    idx += 1;
                    Ok(())
                },
            )?;

            if term_req.req.min_doc_count == 0 {
                // TODO: Handle rev streaming for descending sorting by keys
                let mut stream = term_dict.stream()?;
                let empty_sub_aggregation =
                    IntermediateAggregationResults::empty_from_req(&term_req.sug_aggregations);
                while stream.advance() {
                    if dict.len() >= term_req.req.segment_size as usize {
                        break;
                    }

                    // Respect allowed filters if present
                    if let Some(allowed_bs) = term_req.allowed_term_ids.as_ref() {
                        if !allowed_bs.contains(stream.term_ord() as u32) {
                            continue;
                        }
                    }

                    let key = IntermediateKey::Str(
                        std::str::from_utf8(stream.key())
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
        } else if term_req.column_type == ColumnType::DateTime {
            for (val, doc_count) in entries {
                let intermediate_entry = into_intermediate_bucket_entry(val, doc_count)?;
                let val = i64::from_u64(val);
                let date = format_date(val)?;
                dict.insert(IntermediateKey::Str(date), intermediate_entry);
            }
        } else if term_req.column_type == ColumnType::Bool {
            for (val, doc_count) in entries {
                let intermediate_entry = into_intermediate_bucket_entry(val, doc_count)?;
                let val = bool::from_u64(val);
                dict.insert(IntermediateKey::Bool(val), intermediate_entry);
            }
        } else if term_req.column_type == ColumnType::IpAddr {
            let compact_space_accessor = term_req
                .accessor
                .values
                .clone()
                .downcast_arc::<CompactSpaceU64Accessor>()
                .map_err(|_| {
                    TantivyError::AggregationError(
                        crate::aggregation::AggregationError::InternalError(
                            "Type mismatch: Could not downcast to CompactSpaceU64Accessor"
                                .to_string(),
                        ),
                    )
                })?;

            for (val, doc_count) in entries {
                let intermediate_entry = into_intermediate_bucket_entry(val, doc_count)?;
                let val: u128 = compact_space_accessor.compact_to_u128(val as u32);
                let val = Ipv6Addr::from_u128(val);
                dict.insert(IntermediateKey::IpAddr(val), intermediate_entry);
            }
        } else {
            for (val, doc_count) in entries {
                let intermediate_entry = into_intermediate_bucket_entry(val, doc_count)?;
                if term_req.column_type == ColumnType::U64 {
                    dict.insert(IntermediateKey::U64(val), intermediate_entry);
                } else if term_req.column_type == ColumnType::I64 {
                    dict.insert(IntermediateKey::I64(i64::from_u64(val)), intermediate_entry);
                } else {
                    let val = f64::from_u64(val);
                    let val: NumericalValue = val.into();

                    match val.normalize() {
                        NumericalValue::U64(val) => {
                            dict.insert(IntermediateKey::U64(val), intermediate_entry);
                        }
                        NumericalValue::I64(val) => {
                            dict.insert(IntermediateKey::I64(val), intermediate_entry);
                        }
                        NumericalValue::F64(val) => {
                            dict.insert(IntermediateKey::F64(val), intermediate_entry);
                        }
                    }
                };
            }
        };

        Ok(IntermediateBucketResult::Terms {
            buckets: IntermediateTermBucketResult {
                entries: dict,
                sum_other_doc_count,
                doc_count_error_upper_bound: term_doc_count_before_cutoff,
            },
        })
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
    use std::net::IpAddr;
    use std::str::FromStr;

    use common::DateTime;
    use time::{Date, Month};

    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::intermediate_agg_result::{
        IntermediateAggregationResult, IntermediateAggregationResults,
    };
    use crate::aggregation::tests::{
        exec_request, exec_request_with_query, exec_request_with_query_and_memory_limit,
        get_test_index_from_terms, get_test_index_from_values_and_terms,
    };
    use crate::aggregation::{AggregationLimitsGuard, DistributedAggregationCollector};
    use crate::indexer::NoMergePolicy;
    use crate::query::AllQuery;
    use crate::schema::{
        IndexRecordOption, IntoIpv6Addr, Schema, TextFieldIndexing, TextOptions, FAST, STORED,
        STRING, TEXT,
    };
    use crate::{Index, IndexWriter};

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

        // include filter: only terma and termc
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "string_id",
                    "include": ["terma", "termc"],
                },
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        assert_eq!(res["my_texts"]["buckets"][0]["key"], "terma");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 5);
        assert_eq!(res["my_texts"]["buckets"][1]["key"], "termc");
        assert_eq!(res["my_texts"]["buckets"][1]["doc_count"], 1);
        assert_eq!(res["my_texts"]["sum_other_doc_count"], 0);

        // exclude filter: remove termc
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "string_id",
                    "exclude": ["termc"],
                },
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        assert_eq!(res["my_texts"]["buckets"][0]["key"], "terma");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 5);
        assert_eq!(res["my_texts"]["buckets"][1]["key"], "termb");
        assert_eq!(res["my_texts"]["buckets"][1]["doc_count"], 2);
        assert_eq!(res["my_texts"]["sum_other_doc_count"], 0);

        // include regex (single string): only termb
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "string_id",
                    "include": "termb",
                },
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        assert_eq!(res["my_texts"]["buckets"][0]["key"], "termb");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 2);
        assert_eq!(res["my_texts"]["sum_other_doc_count"], 0);

        // include regex (term.*) with exclude regex (termc): expect terma and termb
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_texts": {
                "terms": {
                    "field": "string_id",
                    "include": "term.*",
                    "exclude": "termc",
                },
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        assert_eq!(res["my_texts"]["buckets"][0]["key"], "terma");
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 5);
        assert_eq!(res["my_texts"]["buckets"][1]["key"], "termb");
        assert_eq!(res["my_texts"]["buckets"][1]["doc_count"], 2);
        assert_eq!(res["my_texts"]["sum_other_doc_count"], 0);

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
            vec!["terma", "terma", "termb", "termb", "termb"],
            vec!["terma", "terma", "termb"],
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
        assert_eq!(res["my_texts"]["buckets"][1]["key"], "termb");
        assert_eq!(res["my_texts"]["buckets"][1]["doc_count"], 0);
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
            AggregationLimitsGuard::new(Some(50_000), None),
        )
        .unwrap_err();
        assert!(res
            .to_string()
            .contains("Aborting aggregation because memory limit was exceeded. Limit: 50.00 KB"));

        Ok(())
    }

    #[test]
    fn terms_aggregation_different_tokenizer_on_ff_test() -> crate::Result<()> {
        let terms = vec!["Hello Hello", "Hallo Hallo", "Hallo Hallo"];

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
        assert_eq!(res["my_texts"]["buckets"][0]["doc_count"], 2);

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
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
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
        // text field with number as missing fallback
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
    fn terms_aggregation_u64_value() -> crate::Result<()> {
        // Make sure that large u64 are not truncated
        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_u64_field("id", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.set_merge_policy(Box::new(NoMergePolicy));
            index_writer.add_document(doc!(
                id_field => 9_223_372_036_854_775_807u64,
            ))?;
            index_writer.add_document(doc!(
                id_field => 1_769_070_189_829_214_202u64,
            ))?;
            index_writer.add_document(doc!(
                id_field => 1_769_070_189_829_214_202u64,
            ))?;
            index_writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_ids": {
                "terms": {
                    "field": "id"
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        // id field
        assert_eq!(
            res["my_ids"]["buckets"][0]["key"],
            1_769_070_189_829_214_202u64
        );
        assert_eq!(res["my_ids"]["buckets"][0]["doc_count"], 2);
        assert_eq!(
            res["my_ids"]["buckets"][1]["key"],
            9_223_372_036_854_775_807u64
        );
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
        // text field with number as missing fallback
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

    #[test]
    fn terms_aggregation_bool() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_bool_field("bool_field", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut writer = index.writer_with_num_threads(1, 15_000_000)?;
            writer.add_document(doc!(field=>true))?;
            writer.add_document(doc!(field=>false))?;
            writer.add_document(doc!(field=>true))?;
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_bool": {
                "terms": {
                    "field": "bool_field"
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        assert_eq!(res["my_bool"]["buckets"][0]["key"], 1.0);
        assert_eq!(res["my_bool"]["buckets"][0]["key_as_string"], "true");
        assert_eq!(res["my_bool"]["buckets"][0]["doc_count"], 2);
        assert_eq!(res["my_bool"]["buckets"][1]["key"], 0.0);
        assert_eq!(res["my_bool"]["buckets"][1]["key_as_string"], "false");
        assert_eq!(res["my_bool"]["buckets"][1]["doc_count"], 1);
        assert_eq!(res["my_bool"]["buckets"][2]["key"], serde_json::Value::Null);

        Ok(())
    }

    #[test]
    fn terms_aggregation_ip_addr() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_ip_addr_field("ip_field", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut writer = index.writer_with_num_threads(1, 15_000_000)?;
            // IpV6 loopback
            writer.add_document(doc!(field=>IpAddr::from_str("::1").unwrap().into_ipv6_addr()))?;
            writer.add_document(doc!(field=>IpAddr::from_str("::1").unwrap().into_ipv6_addr()))?;
            // IpV4
            writer.add_document(
                doc!(field=>IpAddr::from_str("127.0.0.1").unwrap().into_ipv6_addr()),
            )?;
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_bool": {
                "terms": {
                    "field": "ip_field"
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;
        // print as json
        // println!("{}", serde_json::to_string_pretty(&res).unwrap());

        assert_eq!(res["my_bool"]["buckets"][0]["key"], "::1");
        assert_eq!(res["my_bool"]["buckets"][0]["doc_count"], 2);
        assert_eq!(res["my_bool"]["buckets"][1]["key"], "127.0.0.1");
        assert_eq!(res["my_bool"]["buckets"][1]["doc_count"], 1);
        assert_eq!(res["my_bool"]["buckets"][2]["key"], serde_json::Value::Null);

        Ok(())
    }

    #[test]
    fn terms_aggs_hosts_and_tags_merge_on_mixed_order_request() -> crate::Result<()> {
        // This test ensures that merging of aggregation results works correctly
        // by key name rather than relying on iteration order.
        // This is a regression test for a bug where merge used .zip() which relied
        // on iteration order, causing incorrect results when aggregations were built
        // in different orders on different segments.
        let build_index = || -> crate::Result<Index> {
            let mut schema_builder = Schema::builder();
            let host = schema_builder.add_text_field("host", FAST);
            let tags = schema_builder.add_text_field("tags", FAST);
            let schema = schema_builder.build();

            let index = Index::create_in_ram(schema.clone());
            let mut writer = index.writer(50_000_000).unwrap();

            // --- Ingest documents (batch #1) ---
            writer.add_document(doc!(
                host => "192.168.0.10",
                tags => "nice",
            ))?;
            writer.add_document(doc!(
                host => "192.168.0.1",
                tags => "nice",
            ))?;
            writer.add_document(doc!(
                host => "192.168.0.11",
                tags => "nice",
            ))?;
            writer.add_document(doc!(
                host => "192.168.0.10",
                tags => "nice",
                tags => "cool",
            ))?;
            writer.add_document(doc!(
                host => "192.168.0.1",
                tags => "nice",
                tags => "cool",
            ))?;

            writer.commit()?;

            // --- Ingest documents (batch #2) ---
            writer.add_document(doc!())?;
            writer.add_document(doc!())?;
            writer.add_document(doc!(
                host => "192.168.0.10",
            ))?;
            writer.add_document(doc!(
                host => "192.168.0.10",
            ))?;
            writer.add_document(doc!())?;

            writer.commit()?;
            Ok(index)
        };
        let index = build_index()?;
        let index2 = build_index()?;

        let search = |idx: &Index,
                      agg_req: &Aggregations|
         -> crate::Result<IntermediateAggregationResults> {
            let collector = DistributedAggregationCollector::from_aggs(
                agg_req.clone(),
                AggregationLimitsGuard::default(),
            );
            let reader = idx.reader()?;
            let searcher = reader.searcher();
            let agg_res = searcher.search(&AllQuery, &collector)?;
            Ok(agg_res)
        };

        // --- Aggregations: terms on host and tags ---
        let agg_req: Aggregations = serde_json::from_value(json!({
            "hosts": { "terms": { "field": "host" } },
            "tags":  { "terms": { "field": "tags" } }
        }))
        .unwrap();

        let mut agg_res = search(&index, &agg_req)?;

        // --- Aggregations: terms on host and tags ---
        let mut agg_req2: Aggregations = Aggregations::with_capacity(20);
        agg_req2.insert(
            "tags".to_string(),
            serde_json::from_value(json!({ "terms": { "field": "tags" } }))?,
        );
        agg_req2.insert(
            "hosts".to_string(),
            serde_json::from_value(json!({ "terms": { "field": "host" } }))?,
        );
        // Note: We create agg_req2 with potentially different insertion order than agg_req,
        // but since Aggregations is a HashMap, iteration order is not guaranteed.
        // The test verifies that merge works correctly regardless of the actual order.

        let agg_res2 = search(&index2, &agg_req2)?;

        agg_res.merge_fruits(agg_res2).unwrap();
        let agg_json = serde_json::to_value(
            &agg_res.into_final_result(agg_req2, AggregationLimitsGuard::default())?,
        )?;

        // hosts:
        let hosts = &agg_json["hosts"]["buckets"];
        assert_eq!(hosts[0]["key"], "192.168.0.10");
        assert_eq!(hosts[0]["doc_count"], 8);
        assert_eq!(hosts[1]["key"], "192.168.0.1");
        assert_eq!(hosts[1]["doc_count"], 4);
        assert_eq!(hosts[2]["key"], "192.168.0.11");
        assert_eq!(hosts[2]["doc_count"], 2);
        // Implementation currently reports error bounds/other count; ensure zero.
        assert_eq!(agg_json["hosts"]["doc_count_error_upper_bound"], 0);
        assert_eq!(agg_json["hosts"]["sum_other_doc_count"], 0);

        // tags:
        let tags_buckets = &agg_json["tags"]["buckets"];
        assert_eq!(tags_buckets[0]["key"], "nice");
        assert_eq!(tags_buckets[0]["doc_count"], 10);
        assert_eq!(tags_buckets[1]["key"], "cool");
        assert_eq!(tags_buckets[1]["doc_count"], 4);
        assert_eq!(agg_json["tags"]["doc_count_error_upper_bound"], 0);
        assert_eq!(agg_json["tags"]["sum_other_doc_count"], 0);

        Ok(())
    }
}
