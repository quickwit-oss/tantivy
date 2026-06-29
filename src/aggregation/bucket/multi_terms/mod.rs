use std::fmt::Debug;
use std::net::Ipv6Addr;

use columnar::column_values::CompactSpaceU64Accessor;
use columnar::{
    Column, ColumnType, Dictionary, MonotonicallyMappableToU128, MonotonicallyMappableToU64,
    NumericalValue, StrColumn,
};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use super::{CustomOrder, Order, OrderTarget};
use crate::aggregation::agg_data::{
    build_segment_agg_collectors, AggRefNode, AggregationsSegmentCtx,
};
use crate::aggregation::agg_req::Aggregations;
use crate::aggregation::bucket::term_agg::{
    cut_off_buckets, get_agg_name_and_property, GetDocCount,
};
use crate::aggregation::buffered_sub_aggs::{BufferedSubAggs, HighCardSubAggBuffer};
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateBucketResult,
    IntermediateKey, IntermediateTermBucketEntry,
};
use crate::aggregation::segment_agg_result::{BucketIdProvider, SegmentAggregationCollector};
use crate::aggregation::{format_date, BucketId, Key};
use crate::TantivyError;

/// Multi-terms aggregation: one bucket per unique combination of values across N term fields.
///
/// Behaves like the Elasticsearch `multi_terms` aggregation. Uses the same
/// `size`/`shard_size`/`min_doc_count`/`order` semantics as [`TermsAggregation`], but the
/// composite key is a vector of values -- one per declared field.
///
/// ## Response key format
/// Each bucket has a `key` array `["rock", "Product A"]` and a `key_as_string` `"rock|Product A"`.
///
/// ## Limitations
/// - `missing` on a multi-column/type-mismatched field: only string fallback is supported on
///   multi-column JSON fields; non-string fallback requires a single compatible column.
///
/// [`TermsAggregation`]: super::term_agg::TermsAggregation
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct MultiTermsAggregation {
    /// The fields to aggregate on, in order. The composite key will have one element per entry.
    pub terms: Vec<MultiTermsField>,

    /// By default, the top 10 term combinations with the most documents are returned.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub size: Option<u32>,

    /// Per-segment candidate count. Defaults to `ceil(size * 1.5) + 10`.
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        alias = "shard_size",
        alias = "split_size"
    )]
    pub segment_size: Option<u32>,

    /// Include `doc_count_error_upper_bound` in the response.
    /// Defaults to `true` when ordering by count desc (the default order).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub show_term_doc_count_error: Option<bool>,

    /// Minimum document count for a bucket to be returned. Defaults to 1.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub min_doc_count: Option<u64>,

    /// Sort order. Defaults to descending `_count`, tie-broken by ascending key.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub order: Option<CustomOrder>,
}

/// One term field in a [`MultiTermsAggregation`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MultiTermsField {
    /// The fast field to aggregate on.
    pub field: String,
    /// Documents missing a value for this field get this key element.
    /// When absent, documents without a value are excluded from all combos.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub missing: Option<Key>,
}

/// Same as [`MultiTermsAggregation`] but with all defaults filled in.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MultiTermsAggregationInternal {
    pub terms: Vec<MultiTermsField>,
    pub size: u32,
    pub segment_size: u32,
    pub show_term_doc_count_error: bool,
    pub min_doc_count: u64,
    pub order: CustomOrder,
}

impl MultiTermsAggregationInternal {
    pub(crate) fn from_req(req: &MultiTermsAggregation) -> Self {
        let size = req.size.unwrap_or(10);
        let default_shard = (size as u64 * 3).div_ceil(2) as u32 + 10;
        let mut segment_size = req.segment_size.unwrap_or(default_shard);
        segment_size = segment_size.max(size);
        let order = req.order.clone().unwrap_or_default();
        MultiTermsAggregationInternal {
            terms: req.terms.clone(),
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

/// Accessors for one field in the multi_terms request.
#[derive(Debug, Clone)]
pub struct MultiTermsFieldAccessors {
    /// All columns for this field (a JSON multi-type field can have several).
    pub columns: Vec<(Column<u64>, ColumnType)>,
    /// String dictionary for resolving term ordinals back to strings.
    pub str_dict_column: Option<StrColumn>,
    /// The user-configured fallback key (kept for synthetic missing resolution).
    pub missing: Option<Key>,
    /// Precomputed `KeyElem` to push when a doc has no value for this field.
    /// `None` means drop the whole combo.
    pub missing_key_elem: Option<KeyElem>,
    /// Field name.
    pub field: String,
}

impl MultiTermsFieldAccessors {
    pub(crate) fn get_memory_consumption(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.columns.len() * std::mem::size_of::<(Column<u64>, ColumnType)>()
    }
}

/// Per-request data bundle passed to the segment collector.
#[derive(Debug, Clone)]
pub struct MultiTermsAggReqData {
    /// Aggregation name used to look up this entry in the result tree.
    pub name: String,
    /// Original request (needed for final-result conversion).
    pub req: MultiTermsAggregation,
    /// One accessor per field listed in `req.terms`.
    pub fields: Vec<MultiTermsFieldAccessors>,
    /// Sub-aggregation descriptor (empty when no sub-aggs).
    pub sub_aggregations: Aggregations,
}

impl MultiTermsAggReqData {
    /// Returns the approximate heap bytes consumed by this entry.
    pub fn get_memory_consumption(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .fields
                .iter()
                .map(MultiTermsFieldAccessors::get_memory_consumption)
                .sum::<usize>()
    }
}

/// One element of a composite key.
///
/// `accessor_idx` identifies which column of the field produced the value,
/// enabling correct resolution back to a typed [`IntermediateKey`].
/// The sentinel `u32::MAX` marks a synthetic missing value (resolved via the
/// `missing` field of [`MultiTermsFieldAccessors`] rather than a column lookup).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct KeyElem {
    /// Index into `MultiTermsFieldAccessors::columns`, or `u32::MAX` for synthetic missing.
    pub accessor_idx: u32,
    /// Raw fast-field u64 value (term ord for Str, `to_u64` encoding for numerics).
    pub raw: u64,
}

impl KeyElem {
    /// Synthetic missing sentinel, resolved directly via `MultiTermsFieldAccessors::missing`.
    pub const SYNTHETIC_MISSING: u32 = u32::MAX;

    /// Creates a `KeyElem` for a real column value.
    pub fn new(accessor_idx: u32, raw: u64) -> Self {
        Self { accessor_idx, raw }
    }

    /// Creates a `KeyElem` that encodes a synthetic missing value.
    pub fn synthetic_missing() -> Self {
        Self {
            accessor_idx: Self::SYNTHETIC_MISSING,
            raw: 0,
        }
    }

    /// Returns `true` if this element encodes a synthetic missing value.
    pub fn is_synthetic_missing(self) -> bool {
        self.accessor_idx == Self::SYNTHETIC_MISSING
    }
}

/// The composite key for one combination of field values, inline-allocated for up to 4 fields.
pub type MultiTermsKey = SmallVec<[KeyElem; 4]>;

#[derive(Clone, Copy, Debug, Default)]
struct MultiTermsBucket {
    pub count: u32,
    pub bucket_id: BucketId,
}

impl MultiTermsBucket {
    fn new(bucket_id: BucketId) -> Self {
        Self {
            count: 0,
            bucket_id,
        }
    }
}

// GetDocCount for cut_off_buckets compat
#[derive(Debug)]
struct MultiTermsBucketEntry {
    key: MultiTermsKey,
    bucket: MultiTermsBucket,
}

impl GetDocCount for MultiTermsBucketEntry {
    fn doc_count(&self) -> u64 {
        self.bucket.count as u64
    }
}

// ---------------------------------------------------------------------------
// Segment collector
// ---------------------------------------------------------------------------

/// Segment-level collector for [`MultiTermsAggregation`].
#[derive(Debug)]
pub struct SegmentMultiTermsCollector {
    /// One bucket map per parent bucket (for nested aggs).
    parent_buckets: Vec<FxHashMap<MultiTermsKey, MultiTermsBucket>>,
    sub_agg: Option<BufferedSubAggs<HighCardSubAggBuffer>>,
    bucket_id_provider: BucketIdProvider,
    req_data_idx: usize,
}

impl SegmentMultiTermsCollector {
    /// Constructs the collector from the per-request context, validating the field
    /// configuration and sub-aggregation ordering target.
    pub fn from_req_and_validate(
        req: &mut AggregationsSegmentCtx,
        node: &AggRefNode,
    ) -> crate::Result<Self> {
        let req_data = &req.per_request.multi_terms_req_data[node.idx_in_req_data];

        if req_data.fields.is_empty() {
            return Err(TantivyError::InvalidArgument(
                "multi_terms aggregation requires at least one field".to_string(),
            ));
        }

        for field_acc in &req_data.fields {
            for (_, col_type) in &field_acc.columns {
                if *col_type == ColumnType::Bytes {
                    return Err(TantivyError::InvalidArgument(format!(
                        "multi_terms aggregation is not supported for column type {:?} in field {}",
                        col_type, field_acc.field
                    )));
                }
            }
        }

        // Validate sub-agg ordering target.
        let req_agg = MultiTermsAggregationInternal::from_req(&req_data.req);
        if let OrderTarget::SubAggregation(sub_agg_name) = &req_agg.order.target {
            let (agg_name, _) = get_agg_name_and_property(sub_agg_name);
            node.get_sub_agg(agg_name, &req.per_request)
                .ok_or_else(|| {
                    TantivyError::InvalidArgument(format!(
                        "could not find aggregation with name {agg_name} in metric \
                         sub_aggregations"
                    ))
                })?;
        }

        let sub_agg = if !node.children.is_empty() {
            let sub_collector = build_segment_agg_collectors(req, &node.children)?;
            Some(BufferedSubAggs::new(sub_collector))
        } else {
            None
        };

        Ok(Self {
            parent_buckets: vec![FxHashMap::default()],
            sub_agg,
            bucket_id_provider: BucketIdProvider::default(),
            req_data_idx: node.idx_in_req_data,
        })
    }

    fn get_memory_consumption(&self, parent_bucket_id: BucketId) -> usize {
        let map = &self.parent_buckets[parent_bucket_id as usize];
        map.capacity()
            * (std::mem::size_of::<MultiTermsKey>() + std::mem::size_of::<MultiTermsBucket>())
    }

    /// Depth-first cartesian product walk over all fields.
    /// Pushes one `KeyElem` per field onto `prefix`, then at depth == num_fields emits the key.
    fn visit(
        doc_id: crate::DocId,
        field_idx: usize,
        prefix: &mut MultiTermsKey,
        req_data: &MultiTermsAggReqData,
        buckets: &mut FxHashMap<MultiTermsKey, MultiTermsBucket>,
        sub_agg: &mut Option<BufferedSubAggs<HighCardSubAggBuffer>>,
        bucket_id_provider: &mut BucketIdProvider,
    ) {
        let field_acc = &req_data.fields[field_idx];
        let is_last = field_idx + 1 == req_data.fields.len();
        let mut any_value = false;

        for (accessor_idx, (col, _col_type)) in field_acc.columns.iter().enumerate() {
            for raw in col.values_for_doc(doc_id) {
                any_value = true;
                prefix.push(KeyElem::new(accessor_idx as u32, raw));
                if is_last {
                    Self::emit(doc_id, prefix, buckets, sub_agg, bucket_id_provider);
                } else {
                    Self::visit(
                        doc_id,
                        field_idx + 1,
                        prefix,
                        req_data,
                        buckets,
                        sub_agg,
                        bucket_id_provider,
                    );
                }
                prefix.pop();
            }
        }

        if !any_value {
            if let Some(missing_elem) = field_acc.missing_key_elem {
                prefix.push(missing_elem);
                if is_last {
                    Self::emit(doc_id, prefix, buckets, sub_agg, bucket_id_provider);
                } else {
                    Self::visit(
                        doc_id,
                        field_idx + 1,
                        prefix,
                        req_data,
                        buckets,
                        sub_agg,
                        bucket_id_provider,
                    );
                }
                prefix.pop();
            } // else drop the combo for this doc
        }
    }

    #[inline]
    fn emit(
        doc_id: crate::DocId,
        key: &MultiTermsKey,
        buckets: &mut FxHashMap<MultiTermsKey, MultiTermsBucket>,
        sub_agg: &mut Option<BufferedSubAggs<HighCardSubAggBuffer>>,
        bucket_id_provider: &mut BucketIdProvider,
    ) {
        let bucket = buckets
            .entry(key.clone())
            .or_insert_with(|| MultiTermsBucket::new(bucket_id_provider.next_bucket_id()));
        bucket.count += 1;
        if let Some(sub_agg) = sub_agg {
            sub_agg.push(bucket.bucket_id, doc_id);
        }
    }

    /// Convert the bucket map to the intermediate result.
    fn into_intermediate_bucket_result_inner(
        req_data: &MultiTermsAggReqData,
        mut sub_agg_collector: Option<&mut dyn SegmentAggregationCollector>,
        bucket_map: FxHashMap<MultiTermsKey, MultiTermsBucket>,
        agg_data: &AggregationsSegmentCtx,
    ) -> crate::Result<IntermediateBucketResult> {
        let req = MultiTermsAggregationInternal::from_req(&req_data.req);

        let mut entries: Vec<MultiTermsBucketEntry> = bucket_map
            .into_iter()
            .map(|(key, bucket)| MultiTermsBucketEntry { key, bucket })
            .collect();

        // Sort by the requested order target.
        match &req.order.target {
            OrderTarget::Count => {
                if req.order.order == Order::Desc {
                    entries.sort_unstable_by_key(|e| std::cmp::Reverse(e.bucket.count));
                } else {
                    entries.sort_unstable_by_key(|e| e.bucket.count);
                }
            }
            OrderTarget::Key => {
                // Resolve keys then sort lexicographically.
                // We compare in column-ordinal space (u64 raw) which preserves
                // term-dictionary ordering for Str columns.
                // Fallback for different types: compare by accessor_idx first.
                let ord = req.order.order;
                entries.sort_unstable_by(|a, b| {
                    let cmp = a
                        .key
                        .iter()
                        .zip(b.key.iter())
                        .find_map(|(ea, eb)| {
                            let c = ea
                                .accessor_idx
                                .cmp(&eb.accessor_idx)
                                .then(ea.raw.cmp(&eb.raw));
                            if c != std::cmp::Ordering::Equal {
                                Some(c)
                            } else {
                                None
                            }
                        })
                        .unwrap_or(std::cmp::Ordering::Equal);
                    if ord == Order::Desc {
                        cmp.reverse()
                    } else {
                        cmp
                    }
                });
            }
            OrderTarget::SubAggregation(sub_agg_path) => {
                let coll = sub_agg_collector.as_deref().ok_or_else(|| {
                    TantivyError::InvalidArgument(format!(
                        "Could not find sub-aggregation collector for path {sub_agg_path}"
                    ))
                })?;
                let (agg_name, agg_prop) = get_agg_name_and_property(sub_agg_path);
                let mut keyed: Vec<(f64, MultiTermsBucketEntry)> = entries
                    .into_iter()
                    .map(|entry| {
                        let metric = coll
                            .compute_metric_value(
                                entry.bucket.bucket_id,
                                agg_name,
                                agg_prop,
                                agg_data,
                            )
                            .unwrap_or(0.0);
                        (metric, entry)
                    })
                    .collect();
                if req.order.order == Order::Desc {
                    keyed.sort_unstable_by(|a, b| b.0.total_cmp(&a.0));
                } else {
                    keyed.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
                }
                entries = keyed.into_iter().map(|(_, e)| e).collect();
            }
        }

        let (term_doc_count_before_cutoff, sum_other_doc_count) =
            cut_off_buckets(&mut entries, req.segment_size as usize);

        let mut result_entries: FxHashMap<Vec<IntermediateKey>, IntermediateTermBucketEntry> =
            FxHashMap::with_capacity_and_hasher(entries.len(), Default::default());

        for entry in entries {
            let intermediate_key = resolve_multi_terms_key(&entry.key, req_data)?;
            let mut sub_aggregation_res = IntermediateAggregationResults::default();
            if let Some(sub_agg_collector) = sub_agg_collector.as_deref_mut() {
                sub_agg_collector.add_intermediate_aggregation_result(
                    agg_data,
                    &mut sub_aggregation_res,
                    entry.bucket.bucket_id,
                )?;
            }
            result_entries.insert(
                intermediate_key,
                IntermediateTermBucketEntry {
                    doc_count: entry.bucket.count as u64,
                    sub_aggregation: sub_aggregation_res,
                },
            );
        }

        Ok(IntermediateBucketResult::MultiTerms {
            buckets: IntermediateMultiTermsBucketResult {
                entries: result_entries,
                sum_other_doc_count,
                doc_count_error_upper_bound: term_doc_count_before_cutoff,
            },
        })
    }
}

impl SegmentAggregationCollector for SegmentMultiTermsCollector {
    fn add_intermediate_aggregation_result(
        &mut self,
        agg_data: &AggregationsSegmentCtx,
        results: &mut IntermediateAggregationResults,
        bucket: BucketId,
    ) -> crate::Result<()> {
        self.prepare_max_bucket(bucket, agg_data)?;
        let bucket_map = std::mem::take(&mut self.parent_buckets[bucket as usize]);
        let req_data = &agg_data.per_request.multi_terms_req_data[self.req_data_idx];
        let name = req_data.name.clone();
        let result = Self::into_intermediate_bucket_result_inner(
            req_data,
            self.sub_agg
                .as_mut()
                .map(BufferedSubAggs::get_sub_agg_collector),
            bucket_map,
            agg_data,
        )?;
        results.push(name, IntermediateAggregationResult::Bucket(result))?;
        Ok(())
    }

    #[inline]
    fn collect(
        &mut self,
        parent_bucket_id: BucketId,
        docs: &[crate::DocId],
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        let mem_pre = self.get_memory_consumption(parent_bucket_id);

        let req_data = &agg_data.per_request.multi_terms_req_data[self.req_data_idx];
        let buckets = &mut self.parent_buckets[parent_bucket_id as usize];
        let sub_agg = &mut self.sub_agg;
        let bucket_id_provider = &mut self.bucket_id_provider;

        let mut prefix: MultiTermsKey = SmallVec::with_capacity(req_data.fields.len());
        for &doc_id in docs {
            Self::visit(
                doc_id,
                0,
                &mut prefix,
                req_data,
                buckets,
                sub_agg,
                bucket_id_provider,
            );
        }

        let mem_delta = self
            .get_memory_consumption(parent_bucket_id)
            .saturating_sub(mem_pre);
        if mem_delta > 0 {
            agg_data
                .context
                .limits
                .add_memory_consumed(mem_delta as u64)?;
        }

        if let Some(sub_agg) = &mut self.sub_agg {
            sub_agg.check_flush_local(agg_data)?;
        }
        Ok(())
    }

    #[inline]
    fn flush(&mut self, agg_data: &mut AggregationsSegmentCtx) -> crate::Result<()> {
        if let Some(sub_agg) = &mut self.sub_agg {
            sub_agg.flush(agg_data)?;
        }
        Ok(())
    }

    fn prepare_max_bucket(
        &mut self,
        max_bucket: BucketId,
        _agg_data: &AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        let needed = max_bucket as usize + 1;
        if self.parent_buckets.len() < needed {
            self.parent_buckets.resize_with(needed, FxHashMap::default);
        }
        Ok(())
    }

    fn compute_metric_value(
        &self,
        _bucket_id: BucketId,
        _sub_agg_name: &str,
        _sub_agg_property: &str,
        _agg_data: &AggregationsSegmentCtx,
    ) -> Option<f64> {
        None
    }
}

/// Resolve a composite key (one `KeyElem` per field) to a `Vec<IntermediateKey>`.
fn resolve_multi_terms_key(
    key: &MultiTermsKey,
    req_data: &MultiTermsAggReqData,
) -> crate::Result<Vec<IntermediateKey>> {
    key.iter()
        .zip(req_data.fields.iter())
        .map(|(elem, field_acc)| resolve_key_elem(*elem, field_acc))
        .collect()
}

/// Resolve one [`KeyElem`] for one field to an [`IntermediateKey`].
fn resolve_key_elem(
    elem: KeyElem,
    field_acc: &MultiTermsFieldAccessors,
) -> crate::Result<IntermediateKey> {
    if elem.is_synthetic_missing() {
        // Synthetic missing -- resolve directly from the configured fallback.
        return Ok(field_acc
            .missing
            .as_ref()
            .map(|k| IntermediateKey::from(k.clone()))
            .unwrap_or(IntermediateKey::Str(String::new())));
    }

    let (col, col_type) = &field_acc.columns[elem.accessor_idx as usize];
    resolve_column_value(elem.raw, col_type, &field_acc.str_dict_column, col)
}

/// Convert a raw u64 from a specific column type to an [`IntermediateKey`].
///
/// Mirrors the logic in `composite/collector.rs:resolve_term` but emits
/// [`IntermediateKey`] instead of [`CompositeIntermediateKey`].
fn resolve_column_value(
    val: u64,
    col_type: &ColumnType,
    str_dict_column: &Option<StrColumn>,
    col: &Column<u64>,
) -> crate::Result<IntermediateKey> {
    match col_type {
        ColumnType::Str => {
            let fallback_dict = Dictionary::empty();
            let term_dict = str_dict_column
                .as_ref()
                .map(|c| c.dictionary())
                .unwrap_or_else(|| &fallback_dict);
            let mut buffer = Vec::new();
            term_dict.ord_to_term(val, &mut buffer)?;
            Ok(IntermediateKey::Str(
                String::from_utf8(buffer).expect("term dict returned non-UTF-8"),
            ))
        }
        ColumnType::DateTime => {
            let val = i64::from_u64(val);
            let date = format_date(val)?;
            Ok(IntermediateKey::Str(date))
        }
        ColumnType::Bool => Ok(IntermediateKey::Bool(bool::from_u64(val))),
        ColumnType::IpAddr => {
            let compact_space_accessor = col
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
            let val128 = compact_space_accessor.compact_to_u128(val as u32);
            Ok(IntermediateKey::IpAddr(Ipv6Addr::from_u128(val128)))
        }
        ColumnType::U64 => Ok(IntermediateKey::U64(val)),
        ColumnType::I64 => Ok(IntermediateKey::I64(i64::from_u64(val))),
        _ => {
            // F64 and other numeric types
            let f = f64::from_u64(val);
            let normalized: NumericalValue = f.into();
            Ok(match normalized.normalize() {
                NumericalValue::U64(v) => IntermediateKey::U64(v),
                NumericalValue::I64(v) => IntermediateKey::I64(v),
                NumericalValue::F64(v) => IntermediateKey::F64(v),
            })
        }
    }
}

/// Intermediate (segment-merged) result for multi_terms aggregation.
#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateMultiTermsBucketResult {
    /// Bucket entries keyed by composite `Vec<IntermediateKey>`.
    pub(crate) entries: FxHashMap<Vec<IntermediateKey>, IntermediateTermBucketEntry>,
    pub(crate) sum_other_doc_count: u64,
    /// Sum of the first-cut-off bucket's doc count per segment (used for error bound).
    pub(crate) doc_count_error_upper_bound: u64,
}

impl IntermediateMultiTermsBucketResult {
    /// Returns a reference to the map of bucket entries keyed by composite `Vec<IntermediateKey>`.
    pub fn entries(&self) -> &FxHashMap<Vec<IntermediateKey>, IntermediateTermBucketEntry> {
        &self.entries
    }

    /// Returns the count of documents not included in the returned buckets.
    pub fn sum_other_doc_count(&self) -> u64 {
        self.sum_other_doc_count
    }

    /// Returns the upper bound of the error on document counts in the returned buckets.
    pub fn doc_count_error_upper_bound(&self) -> u64 {
        self.doc_count_error_upper_bound
    }

    pub(crate) fn into_final_result(
        self,
        req: &MultiTermsAggregation,
        sub_aggregation_req: &Aggregations,
        limits: &mut crate::aggregation::AggregationLimitsGuard,
    ) -> crate::Result<crate::aggregation::agg_result::BucketResult> {
        use crate::aggregation::agg_result::{BucketResult, MultiTermsBucketEntry};
        use crate::aggregation::bucket::term_agg::cut_off_buckets;

        let req = MultiTermsAggregationInternal::from_req(req);

        let mut buckets: Vec<MultiTermsBucketEntry> = self
            .entries
            .into_iter()
            .filter(|(_, e)| e.doc_count >= req.min_doc_count)
            .map(|(key_vec, entry)| {
                let keys: Vec<Key> = key_vec.iter().cloned().map(Key::from).collect();
                let key_as_string = keys
                    .iter()
                    .map(|k| k.to_string())
                    .collect::<Vec<_>>()
                    .join("|");
                Ok(MultiTermsBucketEntry {
                    key_as_string,
                    key: keys,
                    doc_count: entry.doc_count,
                    sub_aggregation: entry
                        .sub_aggregation
                        .into_final_result_internal(sub_aggregation_req, limits)?,
                })
            })
            .collect::<crate::Result<_>>()?;

        // Sort by order.
        match &req.order.target {
            OrderTarget::Count => {
                if req.order.order == Order::Desc {
                    buckets.sort_unstable_by_key(|b| std::cmp::Reverse(b.doc_count));
                } else {
                    buckets.sort_unstable_by_key(|b| b.doc_count);
                }
            }
            OrderTarget::Key => {
                buckets.sort_by(|left, right| {
                    let cmp = left
                        .key
                        .iter()
                        .zip(right.key.iter())
                        .find_map(|(l, r)| {
                            let c = l.partial_cmp(r)?;
                            if c != std::cmp::Ordering::Equal {
                                Some(c)
                            } else {
                                None
                            }
                        })
                        .unwrap_or(std::cmp::Ordering::Equal);
                    if req.order.order == Order::Asc {
                        cmp
                    } else {
                        cmp.reverse()
                    }
                });
            }
            OrderTarget::SubAggregation(name) => {
                let (agg_name, agg_property) = get_agg_name_and_property(name);
                let mut buckets_with_val = buckets
                    .into_iter()
                    .map(|bucket| {
                        let val = bucket
                            .sub_aggregation
                            .get_value_from_aggregation(agg_name, agg_property)?
                            .unwrap_or(f64::MIN);
                        Ok((bucket, val))
                    })
                    .collect::<crate::Result<Vec<_>>>()?;
                buckets_with_val.sort_by(|(_, v1), (_, v2)| match req.order.order {
                    Order::Desc => v2.total_cmp(v1),
                    Order::Asc => v1.total_cmp(v2),
                });
                buckets = buckets_with_val.into_iter().map(|(b, _)| b).collect();
            }
        }

        let (_before_cutoff, sum_other_from_final) =
            cut_off_buckets(&mut buckets, req.size as usize);

        let doc_count_error_upper_bound = if req.show_term_doc_count_error {
            Some(self.doc_count_error_upper_bound)
        } else {
            None
        };

        Ok(BucketResult::MultiTerms {
            buckets,
            sum_other_doc_count: self.sum_other_doc_count + sum_other_from_final,
            doc_count_error_upper_bound,
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::tests::exec_request;
    use crate::aggregation::DistributedAggregationCollector;
    use crate::query::AllQuery;
    use crate::schema::{Schema, FAST, STRING};
    use crate::{Index, IndexWriter};

    /// Build a small index with `genre` (STRING|FAST) and `product` (STRING|FAST) fields,
    /// plus an optional `score` (u64 FAST) field.
    ///
    /// `docs` is a list of `(genre, product_opt)` pairs; `product_opt = None` means the
    /// document has no `product` value (triggers missing-handling logic).
    /// `scores` maps 1-to-1 with docs (use `&[]` when not needed).
    /// When `one_doc_per_segment` is true, each doc gets its own segment.
    fn build_two_field_index(
        docs: &[(&str, Option<&str>)],
        scores: &[u64],
        one_doc_per_segment: bool,
    ) -> crate::Result<Index> {
        let mut schema_builder = Schema::builder();
        let genre_field = schema_builder.add_text_field("genre", STRING | FAST);
        let product_field = schema_builder.add_text_field("product", STRING | FAST);
        let score_field = schema_builder
            .add_u64_field("score", crate::schema::NumericOptions::default().set_fast());
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer: IndexWriter = index.writer_with_num_threads(1, 20_000_000)?;
            for (idx, &(genre, product_opt)) in docs.iter().enumerate() {
                let score = scores.get(idx).copied().unwrap_or(0);
                let mut d = doc!(genre_field => genre, score_field => score);
                if let Some(product) = product_opt {
                    d.add_text(product_field, product);
                }
                index_writer.add_document(d)?;
                if one_doc_per_segment {
                    index_writer.commit()?;
                }
            }
            if !one_doc_per_segment {
                index_writer.commit()?;
            }
        }
        Ok(index)
    }

    #[test]
    fn test_multi_terms_basic_two_fields() -> crate::Result<()> {
        let index = build_two_field_index(
            &[
                ("rock", Some("A")),
                ("rock", Some("A")),
                ("rock", Some("B")),
                ("pop", Some("A")),
            ],
            &[],
            false,
        )?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "genres_and_products": {
                "multi_terms": {
                    "terms": [{"field": "genre"}, {"field": "product"}],
                    "size": 10
                }
            }
        }))?;

        let res = exec_request(agg_req, &index)?;
        let buckets = res["genres_and_products"]["buckets"].as_array().unwrap();

        // Expect: rock|A (2), rock|B (1), pop|A (1)
        assert_eq!(buckets.len(), 3);
        assert_eq!(buckets[0]["key_as_string"], "rock|A");
        assert_eq!(buckets[0]["doc_count"], 2);
        assert_eq!(buckets[1]["doc_count"], 1);
        assert_eq!(buckets[2]["doc_count"], 1);

        // Verify key is an array
        assert!(buckets[0]["key"].is_array());
        assert_eq!(buckets[0]["key"][0], "rock");
        assert_eq!(buckets[0]["key"][1], "A");
        Ok(())
    }

    #[test]
    fn test_multi_terms_order_by_key_asc() -> crate::Result<()> {
        let index = build_two_field_index(
            &[
                ("rock", Some("A")),
                ("pop", Some("B")),
                ("metal", Some("A")),
            ],
            &[],
            false,
        )?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [{"field": "genre"}, {"field": "product"}],
                    "order": {"_key": "asc"}
                }
            }
        }))?;
        let res = exec_request(agg_req, &index)?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        // Alphabetical ascending: metal|A, pop|B, rock|A
        assert_eq!(buckets[0]["key_as_string"], "metal|A");
        assert_eq!(buckets[1]["key_as_string"], "pop|B");
        assert_eq!(buckets[2]["key_as_string"], "rock|A");
        Ok(())
    }

    #[test]
    fn test_multi_terms_min_doc_count() -> crate::Result<()> {
        let index = build_two_field_index(
            &[("rock", Some("A")), ("rock", Some("A")), ("pop", Some("A"))],
            &[],
            false,
        )?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [{"field": "genre"}, {"field": "product"}],
                    "min_doc_count": 2
                }
            }
        }))?;
        let res = exec_request(agg_req, &index)?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0]["key_as_string"], "rock|A");
        Ok(())
    }

    #[test]
    fn test_multi_terms_size_and_sum_other() -> crate::Result<()> {
        let index = build_two_field_index(
            &[
                ("rock", Some("A")),
                ("rock", Some("A")),
                ("rock", Some("A")),
                ("pop", Some("B")),
                ("pop", Some("B")),
                ("metal", Some("C")),
            ],
            &[],
            false,
        )?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [{"field": "genre"}, {"field": "product"}],
                    "size": 2
                }
            }
        }))?;
        let res = exec_request(agg_req, &index)?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0]["key_as_string"], "rock|A");
        // sum_other_doc_count should account for the dropped bucket
        assert!(res["mt"]["sum_other_doc_count"].as_u64().unwrap() > 0);
        Ok(())
    }

    #[test]
    fn test_multi_terms_combo_dropped_when_field_missing_no_fallback() -> crate::Result<()> {
        let index = build_two_field_index(
            &[
                ("rock", Some("A")),
                ("rock", None), // no product -> combo dropped
            ],
            &[],
            false,
        )?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [{"field": "genre"}, {"field": "product"}]
                }
            }
        }))?;
        let res = exec_request(agg_req, &index)?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        // Only rock|A (1 doc); the doc without product is dropped.
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0]["doc_count"], 1);
        Ok(())
    }

    #[test]
    fn test_multi_terms_missing_fallback() -> crate::Result<()> {
        let index = build_two_field_index(
            &[
                ("rock", Some("A")),
                ("rock", None), // product is missing -> gets "UNKNOWN"
            ],
            &[],
            false,
        )?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [
                        {"field": "genre"},
                        {"field": "product", "missing": "UNKNOWN"}
                    ]
                }
            }
        }))?;
        let res = exec_request(agg_req, &index)?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        // Expect rock|A (1) and rock|UNKNOWN (1)
        assert_eq!(buckets.len(), 2);
        let keys: Vec<&str> = buckets
            .iter()
            .map(|b| b["key_as_string"].as_str().unwrap())
            .collect();
        assert!(keys.contains(&"rock|A"), "expected rock|A, got {keys:?}");
        assert!(
            keys.contains(&"rock|UNKNOWN"),
            "expected rock|UNKNOWN, got {keys:?}"
        );
        Ok(())
    }

    #[test]
    fn test_multi_terms_sub_aggregation() -> crate::Result<()> {
        let index = build_two_field_index(
            &[
                ("rock", Some("A")),
                ("rock", Some("A")),
                ("pop", Some("B")),
                ("pop", Some("B")),
            ],
            &[1, 2, 3, 4],
            false,
        )?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [{"field": "genre"}, {"field": "product"}]
                },
                "aggs": {
                    "avg_score": {"avg": {"field": "score"}}
                }
            }
        }))?;
        let res = exec_request(agg_req, &index)?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 2);
        // Each bucket should have avg_score sub-agg
        for bucket in buckets {
            assert!(
                bucket.get("avg_score").is_some(),
                "missing avg_score in {bucket}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_multi_terms_key_as_string_pipe_join() -> crate::Result<()> {
        let index = build_two_field_index(&[("hello world", Some("foo|bar"))], &[], false)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [{"field": "genre"}, {"field": "product"}]
                }
            }
        }))?;
        let res = exec_request(agg_req, &index)?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 1);
        // key_as_string must pipe-join the values
        assert_eq!(
            buckets[0]["key_as_string"].as_str().unwrap(),
            "hello world|foo|bar"
        );
        Ok(())
    }

    #[test]
    fn test_multi_terms_serde_round_trip() {
        use crate::aggregation::agg_result::BucketResult;

        let result = BucketResult::MultiTerms {
            buckets: vec![crate::aggregation::agg_result::MultiTermsBucketEntry {
                key_as_string: "rock|A".to_string(),
                key: vec![
                    crate::aggregation::Key::Str("rock".to_string()),
                    crate::aggregation::Key::Str("A".to_string()),
                ],
                doc_count: 5,
                sub_aggregation: Default::default(),
            }],
            sum_other_doc_count: 2,
            doc_count_error_upper_bound: Some(1),
        };

        let json_str = serde_json::to_string(&result).unwrap();
        let deserialized: BucketResult = serde_json::from_str(&json_str).unwrap();
        assert_eq!(result, deserialized);
    }

    #[test]
    fn test_multi_terms_multi_segment_merge() -> crate::Result<()> {
        // Two docs in separate segments contributing to the same key -> merge must sum counts.
        let index = build_two_field_index(
            &[("rock", Some("A")), ("rock", Some("A"))],
            &[],
            true, // one doc per segment
        )?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [{"field": "genre"}, {"field": "product"}]
                }
            }
        }))?;
        let res = exec_request(agg_req, &index)?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0]["doc_count"], 2);
        Ok(())
    }

    #[test]
    fn test_multi_terms_distributed_collector() -> crate::Result<()> {
        let index = build_two_field_index(
            &[("rock", Some("A")), ("rock", Some("A")), ("pop", Some("B"))],
            &[],
            true, // one doc per segment -> exercises merge
        )?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [{"field": "genre"}, {"field": "product"}]
                }
            }
        }))?;

        let collector =
            DistributedAggregationCollector::from_aggs(agg_req.clone(), Default::default());
        let searcher = index.reader()?.searcher();
        let intermediate = searcher.search(&AllQuery, &collector)?;
        let res = intermediate.into_final_result(agg_req, Default::default())?;

        // Serialize to JSON for convenient indexing
        let res_val: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&res).unwrap()).unwrap();
        let buckets = res_val["mt"]["buckets"].as_array().unwrap();
        assert!(buckets.len() >= 2);
        Ok(())
    }

    #[test]
    fn test_multi_terms_error_empty_terms() -> crate::Result<()> {
        // Need at least one document so a segment exists and validation runs.
        let index = build_two_field_index(&[("rock", Some("A"))], &[], false)?;
        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": []
                }
            }
        }))?;
        // Empty terms list should fail at collector construction (per-segment validation).
        let res = exec_request(agg_req, &index);
        assert!(res.is_err(), "expected error for empty terms");
        Ok(())
    }
}
