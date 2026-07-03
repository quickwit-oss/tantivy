use std::fmt::Debug;
use std::net::Ipv6Addr;

use columnar::column_values::CompactSpaceU64Accessor;
use columnar::{
    Column, ColumnBlockAccessor, ColumnType, Dictionary, MonotonicallyMappableToU128,
    MonotonicallyMappableToU64, NumericalValue, StrColumn,
};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use super::{CustomOrder, Order, OrderTarget};
use crate::aggregation::agg_data::{
    build_segment_agg_collectors, AggRefNode, AggregationsSegmentCtx, PerRequestAggSegCtx,
};
use crate::aggregation::agg_req::Aggregations;
use crate::aggregation::bucket::term_agg::{
    cut_off_buckets, get_agg_name_and_property, Bucket, GetDocCount, HashMapTermBuckets,
    PagedTermMap, TermAggregationMap, VecTermBuckets, VecTermBucketsNoAgg,
    MAX_NUM_TERMS_FOR_PAGED_MAP, MAX_NUM_TERMS_FOR_VEC,
};
use crate::aggregation::buffered_sub_aggs::{
    BufferedSubAggs, HighCardSubAggBuffer, LowCardBufferedSubAggs, LowCardSubAggBuffer,
    SubAggBuffer,
};
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
    /// True if this multi_terms aggregation is at the top level of the aggregation tree
    /// (not nested). Used to gate the Vec/Paged fast-path storage tiers, which assume a
    /// bounded number of parent buckets (mirrors [`TermsAggReqData::is_top_level`]).
    pub is_top_level: bool,
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

/// Inline capacity of [`MultiTermsKey`]
const MULTI_TERMS_KEY_INLINE_CAPACITY: usize = 3;

/// The composite key for one combination of field values, inline-allocated for up to
/// [`MULTI_TERMS_KEY_INLINE_CAPACITY`] fields.
pub type MultiTermsKey = SmallVec<[KeyElem; MULTI_TERMS_KEY_INLINE_CAPACITY]>;

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

/// Shared validation for both the fast and recursive-fallback collectors: checks the
/// field configuration and (when ordering by a sub-aggregation) that the target exists.
fn validate_multi_terms(
    req_data: &MultiTermsAggReqData,
    node: &AggRefNode,
    per_request: &PerRequestAggSegCtx,
) -> crate::Result<()> {
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
        node.get_sub_agg(agg_name, per_request).ok_or_else(|| {
            TantivyError::InvalidArgument(format!(
                "could not find aggregation with name {agg_name} in metric sub_aggregations"
            ))
        })?;
    }
    Ok(())
}

/// Builds the segment collector for a multi_terms aggregation, validating the request.
///
/// Picks the packed-`u64`-key fast path (see [`compute_fast_path_layout`]) whenever every
/// field is a single, single-valued ("full") u64-backed column whose combined bit width
/// fits in 64 bits. Falls back to the recursive [`SegmentMultiTermsCollector`] for
/// multivalue fields, multi-column (JSON) fields, or too many/too wide fields.
pub(crate) fn build_segment_multi_terms_collector(
    req: &mut AggregationsSegmentCtx,
    node: &AggRefNode,
) -> crate::Result<Box<dyn SegmentAggregationCollector>> {
    let req_data = req.per_request.multi_terms_req_data[node.idx_in_req_data].clone();
    validate_multi_terms(&req_data, node, &req.per_request)?;

    if let Some(packs) = compute_fast_path_layout(&req_data.fields) {
        return build_fast_multi_terms_collector(req, node, req_data, packs);
    }

    Ok(Box::new(SegmentMultiTermsCollector::from_validated(
        req, node,
    )?))
}

impl SegmentMultiTermsCollector {
    /// Constructs the recursive fallback collector. Assumes `validate_multi_terms` has
    /// already been run by [`build_segment_multi_terms_collector`].
    fn from_validated(req: &mut AggregationsSegmentCtx, node: &AggRefNode) -> crate::Result<Self> {
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

    fn get_memory_consumption(&self, parent_bucket_id: BucketId, num_fields: usize) -> usize {
        let map = &self.parent_buckets[parent_bucket_id as usize];
        let spilled_bytes = if num_fields > MULTI_TERMS_KEY_INLINE_CAPACITY {
            num_fields * std::mem::size_of::<KeyElem>()
        } else {
            0
        };
        let per_entry = std::mem::size_of::<MultiTermsKey>()
            + std::mem::size_of::<MultiTermsBucket>()
            + spilled_bytes;
        map.capacity() * per_entry
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
            // Multi-valued columns can repeat the same value for one doc (e.g. `tag: ["a",
            // "a"]`); without deduping, the repeated value would be emitted as two separate
            // combos, inflating doc_count and double-pushing the doc into sub-aggregations.
            let mut raw_values: SmallVec<[u64; 2]> = col.values_for_doc(doc_id).collect();
            if col.get_cardinality().is_multivalue() && raw_values.len() > 1 {
                raw_values.sort_unstable();
                raw_values.dedup();
            }
            for raw in raw_values {
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
                            .unwrap_or(f64::MIN);
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
            cut_off_buckets(&mut entries, req.segment_size as usize, None);

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
            let doc_count = entry.bucket.count as u64;
            // Distinct raw keys can resolve to the same `IntermediateKey` (e.g. a synthetic
            // missing key that renders to the same string as a real value in another entry) --
            // merge into the existing bucket instead of overwriting it.
            match result_entries.entry(intermediate_key) {
                std::collections::hash_map::Entry::Occupied(mut occupied) => {
                    let existing = occupied.get_mut();
                    existing.doc_count += doc_count;
                    existing.sub_aggregation.merge_fruits(sub_aggregation_res)?;
                }
                std::collections::hash_map::Entry::Vacant(vacant) => {
                    vacant.insert(IntermediateTermBucketEntry {
                        doc_count,
                        sub_aggregation: sub_aggregation_res,
                    });
                }
            }
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
        let req_data = &agg_data.per_request.multi_terms_req_data[self.req_data_idx];
        let num_fields = req_data.fields.len();
        let mem_pre = self.get_memory_consumption(parent_bucket_id, num_fields);

        let buckets = &mut self.parent_buckets[parent_bucket_id as usize];
        let sub_agg = &mut self.sub_agg;
        let bucket_id_provider = &mut self.bucket_id_provider;

        let mut prefix: MultiTermsKey = SmallVec::with_capacity(num_fields);
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
            .get_memory_consumption(parent_bucket_id, num_fields)
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

// ---------------------------------------------------------------------------
// Fast path: packed-u64-key segment collector
// ---------------------------------------------------------------------------

/// Per-field bit-packing layout for the fast path: how many bits this field occupies in
/// the packed `u64` key, and at what shift.
///
/// Field 0 occupies the highest bits, the last field the lowest (`shift == 0`). This
/// makes numeric ordering of the packed `u64` equivalent to the lexicographic
/// `(field_idx, raw)` comparison the recursive path uses for `OrderTarget::Key`.
///
/// Values are packed relative to `min_value` (i.e. `raw - min_value`) rather than
/// absolute, so the bit width only has to cover the column's observed *range*. This
/// matters for fields like `DateTime`/`I64` whose `to_u64` mapping flips the sign bit,
/// which otherwise forces a 64-bit width regardless of how narrow the actual range is.
#[derive(Clone, Copy, Debug)]
struct FieldPack {
    shift: u32,
    mask: u64,
    min_value: u64,
}

/// Computes the packed-key layout for the fast path, or `None` if any field is
/// ineligible: a JSON multi-column field, a field that is not "full" (i.e. not exactly
/// one value per doc -- this rules out both optional and multivalued columns), or a
/// combination of fields whose bit widths don't fit in a single `u64`.
fn compute_fast_path_layout(fields: &[MultiTermsFieldAccessors]) -> Option<Vec<FieldPack>> {
    let mut widths = Vec::with_capacity(fields.len());
    let mut mins = Vec::with_capacity(fields.len());
    for field_acc in fields {
        if field_acc.columns.len() != 1 {
            return None;
        }
        let (col, _col_type) = &field_acc.columns[0];
        if !col.get_cardinality().is_full() {
            return None;
        }
        let min_value = col.min_value();
        let range = col.max_value() - min_value;
        widths.push(64 - range.leading_zeros());
        mins.push(min_value);
    }

    let total_width: u32 = widths.iter().sum();
    if total_width > 64 {
        return None;
    }

    // Walk fields last-to-first so the last field gets `shift == 0`, then reverse so the
    // result lines back up with `fields`' original order.
    let mut packs = Vec::with_capacity(fields.len());
    let mut shift = 0u32;
    for (&width, &min_value) in widths.iter().zip(mins.iter()).rev() {
        let mask = if width >= 64 {
            u64::MAX
        } else {
            (1u64 << width) - 1
        };
        packs.push(FieldPack {
            shift,
            mask,
            min_value,
        });
        shift += width;
    }
    packs.reverse();
    Some(packs)
}

/// Upper bound on the packed key space, used to pick a bucket-storage tier exactly like
/// [`term_agg::build_segment_term_collector`] does with a single column's `max_value`.
///
/// [`term_agg::build_segment_term_collector`]: super::term_agg::build_segment_term_collector
fn compute_max_packed(fields: &[MultiTermsFieldAccessors], packs: &[FieldPack]) -> u64 {
    fields
        .iter()
        .zip(packs.iter())
        .map(|(field_acc, pack)| {
            let (col, _col_type) = &field_acc.columns[0];
            (col.max_value() - pack.min_value)
                .checked_shl(pack.shift)
                .unwrap_or(0)
        })
        .sum()
}

/// Unpacks one field's raw ordinal out of a packed key and resolves it through the
/// same [`resolve_column_value`] used by the recursive fallback path, so both paths
/// produce byte-identical `IntermediateKey`s.
fn resolve_packed_key(
    packed: u64,
    packs: &[FieldPack],
    req_data: &MultiTermsAggReqData,
) -> crate::Result<Vec<IntermediateKey>> {
    packs
        .iter()
        .zip(req_data.fields.iter())
        .map(|(pack, field_acc)| {
            let raw = (packed.checked_shr(pack.shift).unwrap_or(0) & pack.mask) + pack.min_value;
            let (col, col_type) = &field_acc.columns[0];
            resolve_column_value(raw, col_type, &field_acc.str_dict_column, col)
        })
        .collect()
}

/// Builds the fast-path segment collector, selecting a bucket-storage tier the same way
/// [`term_agg::build_segment_term_collector`] does, keyed on the packed key space
/// (`max_packed`) instead of a single column's `max_value`.
///
/// [`term_agg::build_segment_term_collector`]: super::term_agg::build_segment_term_collector
fn build_fast_multi_terms_collector(
    req: &mut AggregationsSegmentCtx,
    node: &AggRefNode,
    req_data: MultiTermsAggReqData,
    packs: Vec<FieldPack>,
) -> crate::Result<Box<dyn SegmentAggregationCollector>> {
    let has_sub_aggregations = !node.children.is_empty();
    let is_top_level = req_data.is_top_level;
    let max_packed = compute_max_packed(&req_data.fields, &packs);
    let num_fields = req_data.fields.len();

    let sub_agg_collector = if has_sub_aggregations {
        Some(build_segment_agg_collectors(req, &node.children)?)
    } else {
        None
    };

    let mut bucket_id_provider = BucketIdProvider::default();
    if is_top_level && max_packed < MAX_NUM_TERMS_FOR_VEC && !has_sub_aggregations {
        let term_buckets = VecTermBucketsNoAgg::new(max_packed + 1, &mut bucket_id_provider);
        let collector: SegmentMultiTermsFastCollector<_, HighCardSubAggBuffer> =
            SegmentMultiTermsFastCollector {
                parent_buckets: vec![term_buckets],
                sub_agg: None,
                bucket_id_provider,
                req_data_idx: node.idx_in_req_data,
                packs,
                max_packed,
                field_block_accessors: vec![ColumnBlockAccessor::default(); num_fields],
                packed_keys_buf: Vec::new(),
            };
        Ok(Box::new(collector))
    } else if is_top_level && max_packed < MAX_NUM_TERMS_FOR_VEC {
        let term_buckets = VecTermBuckets::new(max_packed + 1, &mut bucket_id_provider);
        let sub_agg = sub_agg_collector.map(LowCardBufferedSubAggs::new);
        let collector: SegmentMultiTermsFastCollector<_, LowCardSubAggBuffer> =
            SegmentMultiTermsFastCollector {
                parent_buckets: vec![term_buckets],
                sub_agg,
                bucket_id_provider,
                req_data_idx: node.idx_in_req_data,
                packs,
                max_packed,
                field_block_accessors: vec![ColumnBlockAccessor::default(); num_fields],
                packed_keys_buf: Vec::new(),
            };
        Ok(Box::new(collector))
    } else if max_packed < MAX_NUM_TERMS_FOR_PAGED_MAP && is_top_level {
        let term_buckets = PagedTermMap::new(max_packed + 1, &mut bucket_id_provider);
        let sub_agg = sub_agg_collector.map(BufferedSubAggs::new);
        let collector: SegmentMultiTermsFastCollector<PagedTermMap, HighCardSubAggBuffer> =
            SegmentMultiTermsFastCollector {
                parent_buckets: vec![term_buckets],
                sub_agg,
                bucket_id_provider,
                req_data_idx: node.idx_in_req_data,
                packs,
                max_packed,
                field_block_accessors: vec![ColumnBlockAccessor::default(); num_fields],
                packed_keys_buf: Vec::new(),
            };
        Ok(Box::new(collector))
    } else {
        let term_buckets = HashMapTermBuckets::default();
        let sub_agg = sub_agg_collector.map(BufferedSubAggs::new);
        let collector: SegmentMultiTermsFastCollector<HashMapTermBuckets, HighCardSubAggBuffer> =
            SegmentMultiTermsFastCollector {
                parent_buckets: vec![term_buckets],
                sub_agg,
                bucket_id_provider,
                req_data_idx: node.idx_in_req_data,
                packs,
                max_packed,
                field_block_accessors: vec![ColumnBlockAccessor::default(); num_fields],
                packed_keys_buf: Vec::new(),
            };
        Ok(Box::new(collector))
    }
}

/// Fast-path segment collector for [`MultiTermsAggregation`]: packs each field's raw
/// column ordinal into a single `u64` key and reuses the `term_agg` cardinality-
/// specialized bucket storage ([`TermAggregationMap`]) exactly as if it were a single
/// synthetic term field.
///
/// Only constructed when [`compute_fast_path_layout`] confirms every field is a single,
/// full (single-valued) u64-backed column whose combined bit width fits in 64 bits; see
/// [`build_segment_multi_terms_collector`].
#[derive(Debug)]
struct SegmentMultiTermsFastCollector<TermMap: TermAggregationMap, B: SubAggBuffer> {
    /// One bucket map per parent bucket (for nested aggs).
    parent_buckets: Vec<TermMap>,
    sub_agg: Option<BufferedSubAggs<B>>,
    bucket_id_provider: BucketIdProvider,
    req_data_idx: usize,
    packs: Vec<FieldPack>,
    /// Upper bound on the packed key space; used to initialize each parent bucket's
    /// `TermMap`.
    max_packed: u64,
    /// One block accessor per field -- unlike the single-field `terms` agg, all `N`
    /// field blocks must be decoded and kept live simultaneously to build the packed
    /// key, so the shared `AggregationsSegmentCtx::column_block_accessor` cannot be
    /// reused here.
    field_block_accessors: Vec<ColumnBlockAccessor<u64>>,
    /// Scratch buffer for the packed keys of the current doc block. Reused across
    /// `collect` calls to avoid reallocating.
    packed_keys_buf: Vec<u64>,
}

impl<TermMap: TermAggregationMap, B: SubAggBuffer> SegmentMultiTermsFastCollector<TermMap, B> {
    #[inline]
    fn get_memory_consumption(&self, parent_bucket_id: BucketId) -> usize {
        self.parent_buckets[parent_bucket_id as usize].get_memory_consumption()
    }
}

impl<TermMap: TermAggregationMap, B: SubAggBuffer> SegmentAggregationCollector
    for SegmentMultiTermsFastCollector<TermMap, B>
{
    fn add_intermediate_aggregation_result(
        &mut self,
        agg_data: &AggregationsSegmentCtx,
        results: &mut IntermediateAggregationResults,
        bucket: BucketId,
    ) -> crate::Result<()> {
        self.prepare_max_bucket(bucket, agg_data)?;
        let term_buckets = std::mem::replace(
            &mut self.parent_buckets[bucket as usize],
            TermMap::new(0, &mut self.bucket_id_provider),
        );
        let req_data = &agg_data.per_request.multi_terms_req_data[self.req_data_idx];
        let name = req_data.name.clone();
        let result = into_intermediate_bucket_result_fast(
            req_data,
            self.sub_agg
                .as_mut()
                .map(BufferedSubAggs::get_sub_agg_collector),
            term_buckets,
            &self.packs,
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

        self.packed_keys_buf.clear();
        self.packed_keys_buf.resize(docs.len(), 0u64);

        for (field_idx, field_acc) in req_data.fields.iter().enumerate() {
            let (col, _col_type) = &field_acc.columns[0];
            let pack = self.packs[field_idx];
            let block_acc = &mut self.field_block_accessors[field_idx];
            block_acc.fetch_block_with_is_full(docs, col, true);
            for (dst, v) in self.packed_keys_buf.iter_mut().zip(block_acc.iter_vals()) {
                *dst |= (v - pack.min_value).checked_shl(pack.shift).unwrap_or(0);
            }
        }

        let term_buckets = &mut self.parent_buckets[parent_bucket_id as usize];
        if let Some(sub_agg) = &mut self.sub_agg {
            for (&doc_id, &key) in docs.iter().zip(self.packed_keys_buf.iter()) {
                let bucket_id = term_buckets.term_entry(key, &mut self.bucket_id_provider);
                sub_agg.push(bucket_id, doc_id);
            }
        } else {
            for &key in self.packed_keys_buf.iter() {
                term_buckets.term_entry(key, &mut self.bucket_id_provider);
            }
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
        while self.parent_buckets.len() <= max_bucket as usize {
            let term_buckets: TermMap = TermMap::new(self.max_packed, &mut self.bucket_id_provider);
            self.parent_buckets.push(term_buckets);
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

/// Convert a fast-path bucket map (packed `u64` keys) to the intermediate result.
/// Mirrors [`SegmentMultiTermsCollector::into_intermediate_bucket_result_inner`], only
/// diverging on key resolution (`resolve_packed_key` instead of `resolve_multi_terms_key`).
fn into_intermediate_bucket_result_fast<TermMap: TermAggregationMap>(
    req_data: &MultiTermsAggReqData,
    mut sub_agg_collector: Option<&mut dyn SegmentAggregationCollector>,
    term_buckets: TermMap,
    packs: &[FieldPack],
    agg_data: &AggregationsSegmentCtx,
) -> crate::Result<IntermediateBucketResult> {
    let req = MultiTermsAggregationInternal::from_req(&req_data.req);
    let mut entries: Vec<(u64, Bucket)> = term_buckets.into_vec();

    match &req.order.target {
        OrderTarget::Key => {
            // Numeric order of the packed key matches the field-0-high-bits layout's
            // lexicographic (field_idx, raw) comparison -- see `FieldPack`.
            if req.order.order == Order::Desc {
                entries.sort_unstable_by_key(|bucket| std::cmp::Reverse(bucket.0));
            } else {
                entries.sort_unstable_by_key(|bucket| bucket.0);
            }
        }
        OrderTarget::SubAggregation(sub_agg_path) => {
            let coll = sub_agg_collector.as_deref().ok_or_else(|| {
                TantivyError::InvalidArgument(format!(
                    "Could not find sub-aggregation collector for path {sub_agg_path}"
                ))
            })?;
            let (agg_name, agg_prop) = get_agg_name_and_property(sub_agg_path);
            let mut keyed: Vec<(f64, (u64, Bucket))> = entries
                .into_iter()
                .map(|bucket| {
                    let metric_value = coll
                        .compute_metric_value(bucket.1.bucket_id, agg_name, agg_prop, agg_data)
                        .unwrap_or(f64::MIN);
                    (metric_value, bucket)
                })
                .collect();
            if req.order.order == Order::Desc {
                keyed.sort_unstable_by(|a, b| b.0.total_cmp(&a.0));
            } else {
                keyed.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
            }
            entries = keyed.into_iter().map(|(_, e)| e).collect();
        }
        OrderTarget::Count => {
            if req.order.order == Order::Desc {
                entries.sort_unstable_by_key(|bucket| std::cmp::Reverse(bucket.1.count));
            } else {
                entries.sort_unstable_by_key(|bucket| bucket.1.count);
            }
        }
    }

    let (term_doc_count_before_cutoff, sum_other_doc_count) =
        cut_off_buckets(&mut entries, req.segment_size as usize, None);

    let mut result_entries: FxHashMap<Vec<IntermediateKey>, IntermediateTermBucketEntry> =
        FxHashMap::with_capacity_and_hasher(entries.len(), Default::default());

    for (packed, bucket) in entries {
        let intermediate_key = resolve_packed_key(packed, packs, req_data)?;
        let mut sub_aggregation_res = IntermediateAggregationResults::default();
        if let Some(sub_agg_collector) = sub_agg_collector.as_deref_mut() {
            sub_agg_collector.add_intermediate_aggregation_result(
                agg_data,
                &mut sub_aggregation_res,
                bucket.bucket_id,
            )?;
        }
        result_entries.insert(
            intermediate_key,
            IntermediateTermBucketEntry {
                doc_count: bucket.count as u64,
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
                let key_as_string = key_vec
                    .iter()
                    .map(|k| match k {
                        // Bool keys need special-casing: `Key` form is numeric (1/0),  but
                        // `key_as_string` must still carry the "true"/"false" string form.
                        IntermediateKey::Bool(b) => b.to_string(),
                        other => Key::from(other.clone()).to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join("|");
                let keys: Vec<Key> = key_vec.into_iter().map(Key::from).collect();
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
            cut_off_buckets(&mut buckets, req.size as usize, None);

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
    use common::DateTime;
    use serde_json::json;
    use time::{Date, Month};

    use super::*;
    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::tests::{exec_request, exec_request_with_query};
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

        assert_eq!(buckets.len(), 3);
        assert_eq!(buckets[0]["key_as_string"], "rock|A");
        assert_eq!(buckets[0]["doc_count"], 2);
        // rock|B and pop|A tie on doc_count; their relative order is unspecified.
        let tied: std::collections::HashSet<&str> = [
            buckets[1]["key_as_string"].as_str().unwrap(),
            buckets[2]["key_as_string"].as_str().unwrap(),
        ]
        .into_iter()
        .collect();
        assert_eq!(tied, ["rock|B", "pop|A"].into_iter().collect());
        assert_eq!(buckets[1]["doc_count"], 1);
        assert_eq!(buckets[2]["doc_count"], 1);

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
        assert!(res["mt"]["sum_other_doc_count"].as_u64().unwrap() > 0);
        Ok(())
    }

    #[test]
    fn test_multi_terms_combo_dropped_when_field_missing_no_fallback() -> crate::Result<()> {
        let index = build_two_field_index(&[("rock", Some("A")), ("rock", None)], &[], false)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [{"field": "genre"}, {"field": "product"}]
                }
            }
        }))?;
        let res = exec_request(agg_req, &index)?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        // 2nd doc dropped
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0]["doc_count"], 1);
        Ok(())
    }

    #[test]
    fn test_multi_terms_missing_fallback() -> crate::Result<()> {
        let index = build_two_field_index(&[("rock", Some("A")), ("rock", None)], &[], false)?;

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
    fn test_multi_terms_missing_fallback_on_json_path_absent_from_segment() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let genre_field = schema_builder.add_text_field("genre", STRING | FAST);
        let json_field = schema_builder.add_json_field("attrs", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer: IndexWriter = index.writer_with_num_threads(1, 20_000_000)?;
            writer.add_document(doc!(genre_field => "rock", json_field => json!({"other": 5})))?;
            writer.add_document(doc!(genre_field => "pop", json_field => json!({"other": 6})))?;
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [
                        {"field": "genre"},
                        {"field": "attrs.val", "missing": "UNKNOWN"}
                    ]
                }
            }
        }))?;
        let res = exec_request(agg_req, &index)?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 2);
        let keys: Vec<&str> = buckets
            .iter()
            .map(|b| b["key_as_string"].as_str().unwrap())
            .collect();
        assert!(
            keys.contains(&"rock|UNKNOWN"),
            "expected rock|UNKNOWN, got {keys:?}"
        );
        assert!(
            keys.contains(&"pop|UNKNOWN"),
            "expected pop|UNKNOWN, got {keys:?}"
        );
        Ok(())
    }

    #[test]
    fn test_multi_terms_memory_consumption_accounts_for_spilled_keys() {
        let mut collector = SegmentMultiTermsCollector {
            parent_buckets: vec![FxHashMap::default()],
            sub_agg: None,
            bucket_id_provider: BucketIdProvider::default(),
            req_data_idx: 0,
        };
        for i in 0..64u64 {
            let mut key: MultiTermsKey = SmallVec::new();
            key.push(KeyElem::new(0, i));
            collector.parent_buckets[0].insert(
                key,
                MultiTermsBucket {
                    count: 1,
                    bucket_id: 0,
                },
            );
        }

        let num_spilled_fields = MULTI_TERMS_KEY_INLINE_CAPACITY + 2;
        let mem_within_inline_capacity =
            collector.get_memory_consumption(0, MULTI_TERMS_KEY_INLINE_CAPACITY);
        let mem_with_spilled_fields = collector.get_memory_consumption(0, num_spilled_fields);

        let capacity = collector.parent_buckets[0].capacity();
        let expected_delta = capacity * num_spilled_fields * std::mem::size_of::<KeyElem>();
        assert_eq!(
            mem_with_spilled_fields - mem_within_inline_capacity,
            expected_delta
        );
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
        assert_eq!(buckets[0]["key_as_string"], "rock|A");
        Ok(())
    }

    #[test]
    fn test_multi_terms_distributed_collector() -> crate::Result<()> {
        let index = build_two_field_index(
            &[("rock", Some("A")), ("rock", Some("A")), ("pop", Some("B"))],
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

        let collector =
            DistributedAggregationCollector::from_aggs(agg_req.clone(), Default::default());
        let searcher = index.reader()?.searcher();
        let intermediate = searcher.search(&AllQuery, &collector)?;
        let res = intermediate.into_final_result(agg_req, Default::default())?;

        let res_val: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&res).unwrap()).unwrap();
        let buckets = res_val["mt"]["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0]["key_as_string"], "rock|A");
        assert_eq!(buckets[0]["doc_count"], 2);
        assert_eq!(buckets[1]["key_as_string"], "pop|B");
        assert_eq!(buckets[1]["doc_count"], 1);
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
        let res = exec_request(agg_req, &index);
        assert!(res.is_err(), "expected error for empty terms");
        Ok(())
    }

    #[test]
    fn test_multi_terms_rejects_bytes_field() -> crate::Result<()> {
        // `Bytes` columns are explicitly unsupported
        let mut schema_builder = Schema::builder();
        let genre_field = schema_builder.add_text_field("genre", STRING | FAST);
        let bytes_field = schema_builder.add_bytes_field("raw", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer: IndexWriter = index.writer_with_num_threads(1, 20_000_000)?;
            writer.add_document(doc!(genre_field => "rock", bytes_field => vec![1u8, 2, 3]))?;
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [{"field": "genre"}, {"field": "raw"}]
                }
            }
        }))?;
        let res = exec_request(agg_req, &index);
        assert!(res.is_err(), "expected error for Bytes field, got {res:?}");
        Ok(())
    }

    #[test]
    fn test_multi_terms_rejects_numeric_missing_on_bool_field() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let genre_field = schema_builder.add_text_field("genre", STRING | FAST);
        let bool_field = schema_builder.add_bool_field("flag", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer: IndexWriter = index.writer_with_num_threads(1, 20_000_000)?;
            writer.add_document(doc!(genre_field => "rock", bool_field => true))?;
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [
                        {"field": "genre"},
                        {"field": "flag", "missing": 1}
                    ]
                }
            }
        }))?;
        let res = exec_request(agg_req, &index);
        assert!(
            res.is_err(),
            "expected error for numeric missing on bool field, got {res:?}"
        );
        Ok(())
    }

    #[test]
    fn test_multi_terms_accepts_numeric_missing_on_str_field() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let genre_field = schema_builder.add_text_field("genre", STRING | FAST);
        let tag_field = schema_builder.add_text_field("tag", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer: IndexWriter = index.writer_with_num_threads(1, 20_000_000)?;
            writer.add_document(doc!(genre_field => "rock", tag_field => "a"))?;
            writer.add_document(doc!(genre_field => "rock"))?;
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [
                        {"field": "genre"},
                        {"field": "tag", "missing": 1}
                    ]
                }
            }
        }))?;
        let res = exec_request(agg_req, &index)?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 2, "expected {buckets:?}");
        Ok(())
    }

    // -----------------------------------------------------------------
    // Fast-path (packed-u64-key) specific tests
    // -----------------------------------------------------------------

    #[test]
    fn test_multi_terms_fast_path_multi_type_fields() -> crate::Result<()> {
        // bool + date + u64, all FAST/full columns -> should hit the packed-key fast path
        // and exercise `resolve_column_value` for each non-Str type through the unpack path.
        let mut schema_builder = Schema::builder();
        let bool_field = schema_builder.add_bool_field("flag", FAST);
        let date_field = schema_builder.add_date_field("when", FAST);
        let score_field = schema_builder
            .add_u64_field("score", crate::schema::NumericOptions::default().set_fast());
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        let d1 = DateTime::from_primitive(
            Date::from_calendar_date(1982, Month::September, 17)?.with_hms(0, 0, 0)?,
        );
        let d2 = DateTime::from_primitive(
            Date::from_calendar_date(1983, Month::September, 27)?.with_hms(0, 0, 0)?,
        );
        {
            let mut writer: IndexWriter = index.writer_with_num_threads(1, 20_000_000)?;
            writer
                .add_document(doc!(bool_field => true, date_field => d1, score_field => 10u64))?;
            writer
                .add_document(doc!(bool_field => true, date_field => d1, score_field => 10u64))?;
            writer
                .add_document(doc!(bool_field => false, date_field => d2, score_field => 20u64))?;
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [{"field": "flag"}, {"field": "when"}, {"field": "score"}]
                }
            }
        }))?;
        let res = exec_request(agg_req, &index)?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 2);

        // a bool field's `key` slot is numeric (1/0) while `key_as_string` carries the
        // "true"/"false" string form. See
        // https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/boolean
        let true_bucket = buckets
            .iter()
            .find(|b| b["key"][0] == 1.0)
            .expect("expected a bucket with flag=true");
        assert_eq!(true_bucket["doc_count"], 2);
        assert_eq!(true_bucket["key"][1], "1982-09-17T00:00:00Z");
        assert_eq!(true_bucket["key"][2], 10);
        assert_eq!(true_bucket["key_as_string"], "true|1982-09-17T00:00:00Z|10");

        let false_bucket = buckets
            .iter()
            .find(|b| b["key"][0] == 0.0)
            .expect("expected a bucket with flag=false");
        assert_eq!(false_bucket["doc_count"], 1);
        assert_eq!(false_bucket["key"][1], "1983-09-27T00:00:00Z");
        assert_eq!(false_bucket["key"][2], 20);
        assert_eq!(
            false_bucket["key_as_string"],
            "false|1983-09-27T00:00:00Z|20"
        );

        Ok(())
    }

    #[test]
    fn test_multi_terms_fast_path_constant_field() -> crate::Result<()> {
        // `score` is 0 for every doc -> max_value == 0 -> width-0 field in the packed key.
        // Grouping is effectively driven by `genre` alone, but the constant value must
        // still resolve correctly for every bucket.
        let index = build_two_field_index(
            &[("rock", Some("A")), ("rock", Some("A")), ("pop", Some("A"))],
            &[0, 0, 0],
            false,
        )?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [{"field": "genre"}, {"field": "score"}]
                }
            }
        }))?;
        let res = exec_request(agg_req, &index)?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 2);
        for bucket in buckets {
            assert_eq!(bucket["key"][1], 0);
        }
        let rock_bucket = buckets
            .iter()
            .find(|b| b["key"][0] == "rock")
            .expect("expected a rock bucket");
        assert_eq!(rock_bucket["doc_count"], 2);
        Ok(())
    }

    #[test]
    fn test_multi_terms_fast_path_noncontiguous_docs() -> crate::Result<()> {
        let index = build_two_field_index(
            &[
                ("even", Some("A")),
                ("odd", Some("B")),
                ("even", Some("A")),
                ("odd", Some("B")),
                ("even", Some("A")),
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
        let res = exec_request_with_query(agg_req, &index, Some(("genre", "even")))?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0]["key_as_string"], "even|A");
        assert_eq!(buckets[0]["doc_count"], 3);
        Ok(())
    }

    #[test]
    fn test_multi_terms_wide_fields_fallback() -> crate::Result<()> {
        // `score` alone needs the full 64 bits (max_value close to u64::MAX), so combined
        // with any other field the packed layout would exceed 64 bits -> must fall back
        // to the recursive collector. Verify results are still correct.
        let index = build_two_field_index(
            &[("rock", Some("A")), ("rock", Some("A")), ("pop", Some("A"))],
            &[u64::MAX - 1, u64::MAX - 1, 5],
            false,
        )?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [{"field": "genre"}, {"field": "score"}]
                }
            }
        }))?;
        let res = exec_request(agg_req, &index)?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 2);
        let rock_bucket = buckets
            .iter()
            .find(|b| b["key"][0] == "rock")
            .expect("expected a rock bucket");
        assert_eq!(rock_bucket["doc_count"], 2);
        assert_eq!(rock_bucket["key"][1], u64::MAX - 1);
        let pop_bucket = buckets
            .iter()
            .find(|b| b["key"][0] == "pop")
            .expect("expected a pop bucket");
        assert_eq!(pop_bucket["doc_count"], 1);
        assert_eq!(pop_bucket["key"][1], 5);
        Ok(())
    }

    #[test]
    fn test_multi_terms_json_multi_column_fallback() -> crate::Result<()> {
        // A JSON field with mixed value types within one segment produces more than one
        // column for that field -> not eligible for the packed-key fast path (rules out
        // JSON multi-type fields), so this must go through the recursive collector.
        let mut schema_builder = Schema::builder();
        let genre_field = schema_builder.add_text_field("genre", STRING | FAST);
        let json_field = schema_builder.add_json_field("attrs", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut writer: IndexWriter = index.writer_with_num_threads(1, 20_000_000)?;
            writer
                .add_document(doc!(genre_field => "rock", json_field => json!({"val": "blue"})))?;
            writer.add_document(doc!(genre_field => "rock", json_field => json!({"val": 5.0})))?;
            writer
                .add_document(doc!(genre_field => "pop", json_field => json!({"val": "blue"})))?;
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [{"field": "genre"}, {"field": "attrs.val"}]
                }
            }
        }))?;
        let res = exec_request(agg_req, &index)?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 3);
        let doc_counts: u64 = buckets
            .iter()
            .map(|b| b["doc_count"].as_u64().unwrap())
            .sum();
        assert_eq!(doc_counts, 3);
        Ok(())
    }

    #[test]
    fn test_multi_terms_merges_buckets_on_intermediate_key_collision() -> crate::Result<()> {
        // A synthetic missing key and a real value can resolve to the same `IntermediateKey`.
        //  The two distinct raw keys must merge into one bucket instead of the second silently
        // overwriting the first.
        let mut schema_builder = Schema::builder();
        let genre_field = schema_builder.add_text_field("genre", STRING | FAST);
        let json_field = schema_builder.add_json_field("attrs", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut writer: IndexWriter = index.writer_with_num_threads(1, 20_000_000)?;
            writer.add_document(
                doc!(genre_field => "rock", json_field => json!({"val": "2023-01-01T00:00:00Z"})),
            )?;
            // No `attrs.val` at all -> falls back to the synthetic missing key, which resolves
            // via the `missing` string below to the same RFC3339 string as the real date above.
            writer.add_document(doc!(genre_field => "rock"))?;
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [
                        {"field": "genre"},
                        {"field": "attrs.val", "missing": "2023-01-01T00:00:00Z"}
                    ]
                }
            }
        }))?;
        let res = exec_request(agg_req, &index)?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        assert_eq!(
            buckets.len(),
            1,
            "expected the two colliding keys to merge into one bucket, got {buckets:?}"
        );
        assert_eq!(buckets[0]["doc_count"], 2);
        assert_eq!(buckets[0]["key_as_string"], "rock|2023-01-01T00:00:00Z");
        Ok(())
    }

    #[test]
    fn test_multi_terms_dedups_repeated_value_in_multivalued_field() -> crate::Result<()> {
        // A multivalued column can repeat the same value twice for one doc (e.g.
        // `tag: ["a", "a"]`), this shouldn't generate 2 hits for one bucket.
        let mut schema_builder = Schema::builder();
        let genre_field = schema_builder.add_text_field("genre", STRING | FAST);
        let tag_field = schema_builder.add_text_field("tag", STRING | FAST);
        let score_field = schema_builder
            .add_u64_field("score", crate::schema::NumericOptions::default().set_fast());
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer: IndexWriter = index.writer_with_num_threads(1, 20_000_000)?;
            let mut d = doc!(genre_field => "rock", score_field => 10u64);
            d.add_text(tag_field, "a");
            d.add_text(tag_field, "a"); // repeated value for the same doc
            writer.add_document(d)?;
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [{"field": "genre"}, {"field": "tag"}]
                },
                "aggs": {
                    "avg_score": {"avg": {"field": "score"}}
                }
            }
        }))?;
        let res = exec_request(agg_req, &index)?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        assert_eq!(
            buckets.len(),
            1,
            "expected a single deduped bucket, got {buckets:?}"
        );
        assert_eq!(buckets[0]["key_as_string"], "rock|a");
        assert_eq!(buckets[0]["doc_count"], 1);
        assert_eq!(buckets[0]["avg_score"]["value"], 10.0);
        Ok(())
    }

    #[test]
    fn test_multi_terms_missing_subagg_value_sorts_last_at_segment_cutoff() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let genre_field = schema_builder.add_text_field("genre", STRING | FAST);
        let product_field = schema_builder.add_text_field("product", STRING | FAST);
        let delta_field = schema_builder
            .add_i64_field("delta", crate::schema::NumericOptions::default().set_fast());
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer: IndexWriter = index.writer_with_num_threads(1, 20_000_000)?;
            writer.add_document(
                doc!(genre_field => "rock", product_field => "A", delta_field => -5i64),
            )?;
            // No `delta` value at all -> avg_delta is missing for this bucket.
            writer.add_document(doc!(genre_field => "rock", product_field => "B"))?;
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [{"field": "genre"}, {"field": "product"}],
                    "size": 1,
                    "segment_size": 1,
                    "order": {"avg_delta": "desc"}
                },
                "aggs": {
                    "avg_delta": {"avg": {"field": "delta"}}
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
    fn test_multi_terms_missing_subagg_value_sorts_last_at_segment_cutoff_recursive_path(
    ) -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let genre_field = schema_builder.add_text_field("genre", STRING | FAST);
        let product_field = schema_builder.add_text_field("product", STRING | FAST);
        let delta_field = schema_builder
            .add_i64_field("delta", crate::schema::NumericOptions::default().set_fast());
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer: IndexWriter = index.writer_with_num_threads(1, 20_000_000)?;
            writer.add_document(
                doc!(genre_field => "rock", product_field => "A", delta_field => -5i64),
            )?;
            // No `delta` value at all -> avg_delta is missing for this bucket.
            writer.add_document(doc!(genre_field => "rock", product_field => "B"))?;
            // Multivalued `product` for this doc forces the whole column to be
            // non-full/multivalued, disqualifying the fast path for the entire request.
            let mut multivalued_doc = doc!(genre_field => "other");
            multivalued_doc.add_text(product_field, "C");
            multivalued_doc.add_text(product_field, "D");
            writer.add_document(multivalued_doc)?;
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [{"field": "genre"}, {"field": "product"}],
                    "size": 1,
                    "segment_size": 1,
                    "order": {"avg_delta": "desc"}
                },
                "aggs": {
                    "avg_delta": {"avg": {"field": "delta"}}
                }
            }
        }))?;
        let res = exec_request(agg_req, &index)?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 1, "expected {buckets:?}");
        assert_eq!(buckets[0]["key_as_string"], "rock|A");
        Ok(())
    }
}
