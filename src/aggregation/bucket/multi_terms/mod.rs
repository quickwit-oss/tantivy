use std::fmt::Debug;
use std::net::Ipv6Addr;
use std::sync::Arc;

use columnar::column_values::CompactSpaceU64Accessor;
use columnar::{
    Column, ColumnBlockAccessor, ColumnType, Dictionary, MonotonicallyMappableToU128,
    MonotonicallyMappableToU64, NumericalValue, StrColumn,
};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use super::{BucketIdSlot, CustomOrder, Order, OrderTarget};
use crate::aggregation::agg_data::{
    build_segment_agg_collectors, AggRefNode, AggregationsSegmentCtx, PerRequestAggSegCtx,
};
use crate::aggregation::agg_req::Aggregations;
use crate::aggregation::bucket::term_agg::{
    cut_off_buckets, get_agg_name_and_property, AggregationMapKey, Bucket, GetDocCount,
    HashMapTermBuckets, PagedTermMap, TermAggregationMap, VecTermBuckets,
    LAZY_BUCKET_ID_GENERATION_THRESHOLD, MAX_NUM_TERMS_FOR_LOWCARD_SUBAGG,
    MAX_NUM_TERMS_FOR_PAGED_MAP, MAX_NUM_TERMS_FOR_VEC,
};
use crate::aggregation::buffered_sub_aggs::{
    BufferedSubAggs, HighCardSubAggBuffer, LowCardSubAggBuffer, SubAggBuffer,
};
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateBucketResult,
    IntermediateKey, IntermediateTermBucketEntry, PruneMode,
};
use crate::aggregation::segment_agg_result::{BucketIdProvider, SegmentAggregationCollector};
use crate::aggregation::{f64_to_fastfield_u64, format_date, BucketId, Key};
use crate::{DocId, TantivyError};

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
/// - Bytes columns are not supported.
/// - A non-string `missing` value must be representable by at least one physical column using the
///   same lenient coercions as a terms aggregation.
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

/// Missing-value handling for one requested field.
///
/// The shared `all_columns` list is used solely to check whether the field is absent across every
/// physical type, mirroring the union-of-column-indices semantics of `TermMissingAgg`.
#[derive(Debug, Clone)]
pub struct MultiTermsMissingAccessor {
    /// All physical columns for this requested field, used for existence checks only.
    pub all_columns: Arc<[Column<u64>]>,
    /// The user-configured fallback key, used to resolve synthetic missing values.
    pub key: Key,
    /// Raw value injected for missing documents. This reuses an existing string term ordinal when
    /// available and otherwise uses a collision-free sentinel.
    pub missing_value: u64,
}

/// One typed accessor for one field in a multi_terms collector.
#[derive(Debug, Clone)]
pub struct MultiTermsFieldAccessor {
    /// The single physical column collected by this accessor.
    pub column: Column<u64>,
    /// Physical type of `column`.
    pub column_type: ColumnType,
    /// String dictionary corresponding to `column`, present only for `ColumnType::Str`.
    pub str_dict_column: Option<StrColumn>,
    /// Field name.
    pub field: String,
}

impl MultiTermsFieldAccessor {
    pub(crate) fn get_memory_consumption(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

/// Per-request data bundle passed to the segment collector.
#[derive(Debug, Clone)]
pub struct MultiTermsAggReqData {
    /// Aggregation name used to look up this entry in the result tree.
    pub name: String,
    /// Original request (needed for final-result conversion).
    pub req: MultiTermsAggregation,
    /// One typed accessor per field listed in `req.terms`.
    pub fields: Vec<MultiTermsFieldAccessor>,
    /// Missing-value handling corresponding to `fields`. Only the designated physical accessor
    /// choice for each requested field carries `Some`, preventing duplicate missing buckets when
    /// type-specific collectors are merged.
    pub missing_accessors: Vec<Option<MultiTermsMissingAccessor>>,
    /// Sub-aggregation descriptor (empty when no sub-aggs).
    pub sub_aggregations: Aggregations,
    /// True if this multi_terms aggregation is at the top level of the aggregation tree
    /// (not nested). Used to gate the Vec/Paged packed-key storage tiers, which assume a
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
                .map(MultiTermsFieldAccessor::get_memory_consumption)
                .sum::<usize>()
            + self.missing_accessors.len()
                * std::mem::size_of::<Option<MultiTermsMissingAccessor>>()
    }
}

/// Inline capacity of [`MultiTermsKey`].
const MULTI_TERMS_KEY_INLINE_CAPACITY: usize = 3;

/// The composite key for one combination of raw fast-field values, inline-allocated for up to
/// [`MULTI_TERMS_KEY_INLINE_CAPACITY`] fields.
pub type MultiTermsKey = SmallVec<[u64; MULTI_TERMS_KEY_INLINE_CAPACITY]>;

impl AggregationMapKey for MultiTermsKey {
    fn heap_memory_usage(&self) -> usize {
        if self.spilled() {
            self.capacity() * std::mem::size_of::<u64>()
        } else {
            0
        }
    }
}

/// Returns the raw value to inject for missing documents while decoding this physical column.
/// Mixed-type fields require a later union-of-columns existence check instead.
#[inline]
fn block_missing_value(missing: Option<&MultiTermsMissingAccessor>) -> Option<u64> {
    missing
        .filter(|missing| missing.all_columns.len() == 1)
        .map(|missing| missing.missing_value)
}

/// Fetches one field into the shared block accessor and forwards safe missing handling. Ordering
/// is requested only by multi-terms, which needs values from different fields aligned by document.
/// Returns whether decoding produced exactly one aligned value per document.
#[inline]
fn fetch_field_block(
    docs: &[crate::DocId],
    field: &MultiTermsFieldAccessor,
    missing: Option<&MultiTermsMissingAccessor>,
    block_accessor: &mut ColumnBlockAccessor<u64>,
) -> bool {
    let missing_value = block_missing_value(missing);
    block_accessor.fetch_block_with_missing_unique_per_doc(
        docs,
        &field.column,
        missing_value,
        true,
    );
    if block_accessor.values().len() != docs.len() {
        return false;
    }
    !field.column.get_cardinality().is_multivalue() || block_accessor.docids() == docs
}

/// Packing operations used by the unified collector.
///
/// Compact layouts use a packed `u64`; layouts wider than 64 bits use [`MultiTermsKey`].
/// Collection, bucket accounting, sub-aggregation buffering, and result conversion are shared.
trait MultiTermsPacking: Clone + Debug + 'static {
    type PackingType: AggregationMapKey + Ord;

    fn new_packing(&self) -> Self::PackingType;
    fn clear_packing(&self, key: &mut Self::PackingType);
    fn push(&self, key: &mut Self::PackingType, field_idx: usize, value: u64);

    /// Pushes one field's aligned values into every active key buffer.
    fn push_full_values<I>(&self, keys: &mut [Self::PackingType], field_idx: usize, values: I)
    where I: IntoIterator<Item = u64>;

    fn unpack(
        &self,
        key: &Self::PackingType,
        req_data: &MultiTermsAggReqData,
    ) -> crate::Result<Vec<IntermediateKey>>;
}

#[derive(Clone, Debug)]
struct U64ArrayKeyPacking {
    num_fields: usize,
}

impl MultiTermsPacking for U64ArrayKeyPacking {
    type PackingType = MultiTermsKey;

    fn new_packing(&self) -> Self::PackingType {
        SmallVec::with_capacity(self.num_fields)
    }

    fn clear_packing(&self, key: &mut Self::PackingType) {
        key.clear();
    }

    fn push(&self, key: &mut Self::PackingType, field_idx: usize, value: u64) {
        debug_assert_eq!(key.len(), field_idx);
        key.push(value);
    }

    fn push_full_values<I>(&self, keys: &mut [Self::PackingType], field_idx: usize, values: I)
    where I: IntoIterator<Item = u64> {
        for (key, value) in keys.iter_mut().zip(values) {
            debug_assert!(key.len() <= field_idx);
            key.push(value);
        }
    }

    fn unpack(
        &self,
        key: &Self::PackingType,
        req_data: &MultiTermsAggReqData,
    ) -> crate::Result<Vec<IntermediateKey>> {
        key.iter()
            .zip(req_data.fields.iter())
            .zip(req_data.missing_accessors.iter())
            .map(|((value, field_acc), missing)| {
                resolve_key_value(*value, field_acc, missing.as_ref())
            })
            .collect()
    }
}

impl MultiTermsPacking for PackedU64KeyPacking {
    type PackingType = u64;

    fn new_packing(&self) -> Self::PackingType {
        0
    }

    fn clear_packing(&self, key: &mut Self::PackingType) {
        *key = 0;
    }

    fn push(&self, key: &mut Self::PackingType, field_idx: usize, value: u64) {
        let pack = self.packs[field_idx];
        let offset = value - pack.min_value;
        debug_assert!(offset <= pack.max_offset);
        *key |= shift_packed_bits(offset, pack.shift);
    }

    #[inline]
    fn push_full_values<I>(&self, keys: &mut [Self::PackingType], field_idx: usize, values: I)
    where I: IntoIterator<Item = u64> {
        let pack = self.packs[field_idx];
        for (key, value) in keys.iter_mut().zip(values) {
            let offset = value - pack.min_value;
            debug_assert!(offset <= pack.max_offset);
            *key |= shift_packed_bits(offset, pack.shift);
        }
    }

    fn unpack(
        &self,
        key: &Self::PackingType,
        req_data: &MultiTermsAggReqData,
    ) -> crate::Result<Vec<IntermediateKey>> {
        self.packs
            .iter()
            .zip(req_data.fields.iter())
            .zip(req_data.missing_accessors.iter())
            .map(|((pack, field), missing)| {
                let offset = key.checked_shr(pack.shift).unwrap_or(0) & pack.mask;
                resolve_key_value(offset + pack.min_value, field, missing.as_ref())
            })
            .collect()
    }
}

#[derive(Debug)]
struct MultiTermsBucketEntry<K, B> {
    key: K,
    bucket: Bucket<B>,
}

impl<K, B: BucketIdSlot> GetDocCount for MultiTermsBucketEntry<K, B> {
    fn doc_count(&self) -> u64 {
        self.bucket.count as u64
    }
}

// ---------------------------------------------------------------------------
// Segment collector
// ---------------------------------------------------------------------------

/// Segment-level collector shared by packed-u64 and u64-array key representations.
#[derive(Debug)]
struct SegmentMultiTermsCollector<Packing, BucketMap, Buffer>
where
    Packing: MultiTermsPacking,
    BucketMap: TermAggregationMap<Packing::PackingType>,
    Buffer: SubAggBuffer,
{
    /// One bucket map per parent bucket (for nested aggs).
    parent_buckets: Vec<BucketMap>,
    sub_agg: Option<BufferedSubAggs<Buffer>>,
    bucket_id_provider: BucketIdProvider,
    req_data: MultiTermsAggReqData,
    packing: Packing,
    /// Argument used when creating another map for a nested parent bucket. For a dense Vec map
    /// this is the number of keys; for paged/hash maps it is the maximum packed key (or
    /// ignored).
    map_init_value: u64,
    /// Computed once because column cardinality is immutable within a segment.
    all_fields_full: bool,
    /// Partial keys for the current block, grouped by document.
    keys_buf: Vec<Packing::PackingType>,
    /// Documents still able to form a key from the field accessors processed so far. A document
    /// is removed when a field accessor has no value and no `missing` fallback. IDs remain unique
    /// even when `keys_buf` contains multiple combinations for the same document.
    alive_docs: Vec<DocId>,
    /// One document ID per partial Cartesian-product combination in `keys_buf`. Empty while
    /// combinations and unique documents are one-to-one.
    doc_ids_per_partial_combination: Vec<DocId>,
}

/// Validates the field configuration and, when ordering by a sub-aggregation, that the target
/// exists.
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
    if req_data.fields.len() != req_data.missing_accessors.len() {
        return Err(TantivyError::AggregationError(
            crate::aggregation::AggregationError::InternalError(
                "multi_terms fields and missing accessors have different lengths".to_string(),
            ),
        ));
    }

    for field_acc in &req_data.fields {
        if field_acc.column_type == ColumnType::Bytes {
            return Err(TantivyError::InvalidArgument(format!(
                "multi_terms aggregation is not supported for column type {:?} in field {}",
                field_acc.column_type, field_acc.field
            )));
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

/// Returns the configured missing encoding if `doc_id` is absent from every physical column for
/// this requested field. `None` means this typed collector branch must drop the document.
#[inline]
fn missing_value_for_doc(
    missing: Option<&MultiTermsMissingAccessor>,
    doc_id: crate::DocId,
) -> Option<u64> {
    let missing = missing?;
    let field_has_value = missing
        .all_columns
        .iter()
        .any(|column| column.index.has_value(doc_id));
    (!field_has_value).then_some(missing.missing_value)
}

/// Expands the existing partial combinations with every value of one field.
#[allow(clippy::too_many_arguments)]
fn expand_partial_combinations_for_field<Packing: MultiTermsPacking>(
    packing: &Packing,
    field_idx: usize,
    field: &MultiTermsFieldAccessor,
    missing: Option<&MultiTermsMissingAccessor>,
    block_accessor: &ColumnBlockAccessor<u64>,
    keys_buf: &mut Vec<Packing::PackingType>,
    alive_docs: &mut Vec<DocId>,
    doc_ids_per_partial_combination: &mut Vec<DocId>,
) {
    // Keep the current partial combinations as a prefix while appending their expansion.
    let partial_combination_count = keys_buf.len();
    // In one-to-one mode the input document IDs are implicit in `alive_docs`; otherwise they
    // occupy the prefix of `doc_ids_per_partial_combination`.
    let doc_ids_have_input_prefix = !doc_ids_per_partial_combination.is_empty();
    debug_assert_eq!(
        partial_combination_count,
        if doc_ids_have_input_prefix {
            doc_ids_per_partial_combination.len()
        } else {
            alive_docs.len()
        }
    );

    {
        let mut field_values = block_accessor
            .iter_docid_vals(alive_docs, &field.column)
            .peekable();
        let mut doc_values = SmallVec::<[u64; 2]>::new();
        let mut combination_start = 0;

        for &doc_id in alive_docs.iter() {
            let num_combinations_for_doc = if doc_ids_have_input_prefix {
                doc_ids_per_partial_combination[combination_start..partial_combination_count]
                    .iter()
                    .take_while(|&&combination_doc_id| combination_doc_id == doc_id)
                    .count()
            } else {
                1
            };
            debug_assert_ne!(num_combinations_for_doc, 0);
            let combination_end = combination_start + num_combinations_for_doc;

            debug_assert!(field_values
                .peek()
                .is_none_or(|(value_doc, _)| *value_doc >= doc_id));
            doc_values.clear();
            while let Some((_, value)) = field_values.next_if(|(value_doc, _)| *value_doc == doc_id)
            {
                doc_values.push(value);
            }
            if doc_values.is_empty() {
                if let Some(value) = missing_value_for_doc(missing, doc_id) {
                    doc_values.push(value);
                }
            }

            for combination_idx in combination_start..combination_end {
                for &value in &doc_values {
                    let mut key = keys_buf[combination_idx].clone();
                    packing.push(&mut key, field_idx, value);
                    keys_buf.push(key);
                    doc_ids_per_partial_combination.push(doc_id);
                }
            }
            combination_start = combination_end;
        }
        debug_assert_eq!(combination_start, partial_combination_count);
        debug_assert!(field_values.next().is_none());
    }

    keys_buf.drain(..partial_combination_count);
    if doc_ids_have_input_prefix {
        doc_ids_per_partial_combination.drain(..partial_combination_count);
    }

    alive_docs.clear();
    alive_docs.extend_from_slice(doc_ids_per_partial_combination);
    alive_docs.dedup();
    if doc_ids_per_partial_combination.len() == alive_docs.len() {
        doc_ids_per_partial_combination.clear();
    }
}

/// Builds the segment collector for a multi_terms aggregation, validating the request.
///
/// Every value, including missing, is represented as a plain `u64` and collected through block
/// decoding. [`compute_packed_u64_layout`] chooses a packed `u64` key when possible and otherwise
/// uses [`MultiTermsKey`].
pub(crate) fn build_segment_multi_terms_collector(
    req: &mut AggregationsSegmentCtx,
    node: &AggRefNode,
) -> crate::Result<Box<dyn SegmentAggregationCollector>> {
    let req_data = req.per_request.multi_terms_req_data[node.idx_in_req_data].clone();
    validate_multi_terms(&req_data, node, &req.per_request)?;

    let packed_layout = compute_packed_u64_layout(&req_data.fields, &req_data.missing_accessors);
    if node.children.is_empty() {
        build_multi_terms_collector::<()>(req, node, req_data, packed_layout)
    } else {
        build_multi_terms_collector::<BucketId>(req, node, req_data, packed_layout)
    }
}

impl<Packing, BucketMap, Buffer> SegmentMultiTermsCollector<Packing, BucketMap, Buffer>
where
    Packing: MultiTermsPacking,
    BucketMap: TermAggregationMap<Packing::PackingType>,
    Buffer: SubAggBuffer,
{
    fn get_memory_consumption(&self, parent_bucket_id: BucketId) -> usize {
        self.parent_buckets[parent_bucket_id as usize].get_memory_consumption()
    }

    fn build_keys_from_full_fields(
        &mut self,
        docs: &[DocId],
        block_accessor: &mut ColumnBlockAccessor<u64>,
    ) {
        for (field_idx, field) in self.req_data.fields.iter().enumerate() {
            fetch_field_block(
                docs,
                field,
                self.req_data.missing_accessors[field_idx].as_ref(),
                block_accessor,
            );
            self.packing.push_full_values(
                &mut self.keys_buf,
                field_idx,
                block_accessor.iter_vals(),
            );
        }
    }

    fn build_keys_from_non_full_fields(
        &mut self,
        docs: &[DocId],
        block_accessor: &mut ColumnBlockAccessor<u64>,
    ) {
        self.alive_docs.clear();
        self.alive_docs.extend_from_slice(docs);
        self.doc_ids_per_partial_combination.clear();

        for (field_idx, field) in self.req_data.fields.iter().enumerate() {
            let missing = self.req_data.missing_accessors[field_idx].as_ref();
            let has_one_value_per_doc =
                fetch_field_block(&self.alive_docs, field, missing, block_accessor);
            if has_one_value_per_doc {
                if self.doc_ids_per_partial_combination.is_empty() {
                    self.packing.push_full_values(
                        &mut self.keys_buf,
                        field_idx,
                        block_accessor.iter_vals(),
                    );
                } else {
                    let mut values = block_accessor.iter_vals();
                    let mut keys = self.keys_buf.iter_mut();
                    for key_doc_group in self
                        .doc_ids_per_partial_combination
                        .chunk_by(|left, right| left == right)
                    {
                        let value = values.next().unwrap();
                        for key in keys.by_ref().take(key_doc_group.len()) {
                            self.packing.push(key, field_idx, value);
                        }
                    }
                    debug_assert!(values.next().is_none());
                    debug_assert!(keys.next().is_none());
                }
                continue;
            }

            // Until expansion, sparse single-value fields can filter keys in place.
            if self.doc_ids_per_partial_combination.is_empty()
                && !field.column.get_cardinality().is_multivalue()
                && missing.is_none()
            {
                let mut source_idx = 0usize;
                let mut target_idx = 0usize;
                for (&doc_id, &value) in block_accessor.docids().iter().zip(block_accessor.values())
                {
                    while self.alive_docs[source_idx] < doc_id {
                        source_idx += 1;
                    }
                    debug_assert_eq!(self.alive_docs[source_idx], doc_id);
                    if target_idx != source_idx {
                        self.alive_docs[target_idx] = doc_id;
                        self.keys_buf.swap(target_idx, source_idx);
                    }
                    self.packing
                        .push(&mut self.keys_buf[target_idx], field_idx, value);
                    source_idx += 1;
                    target_idx += 1;
                }
                self.alive_docs.truncate(target_idx);
                self.keys_buf.truncate(target_idx);
                continue;
            }

            expand_partial_combinations_for_field(
                &self.packing,
                field_idx,
                field,
                missing,
                block_accessor,
                &mut self.keys_buf,
                &mut self.alive_docs,
                &mut self.doc_ids_per_partial_combination,
            );
        }
    }

    /// Convert any bucket-map/packing pair to the shared intermediate result.
    fn into_intermediate_bucket_result(
        packing: &Packing,
        req_data: &MultiTermsAggReqData,
        mut sub_agg_collector: Option<&mut dyn SegmentAggregationCollector>,
        bucket_map: BucketMap,
        agg_data: &AggregationsSegmentCtx,
    ) -> crate::Result<IntermediateBucketResult> {
        let req = MultiTermsAggregationInternal::from_req(&req_data.req);

        let mut entries: Vec<MultiTermsBucketEntry<Packing::PackingType, BucketMap::Slot>> =
            bucket_map
                .into_vec()
                .into_iter()
                .map(|(key, bucket)| MultiTermsBucketEntry { key, bucket })
                .collect();

        match &req.order.target {
            OrderTarget::Count => {
                if req.order.order == Order::Desc {
                    entries.sort_unstable_by_key(|entry| std::cmp::Reverse(entry.bucket.count));
                } else {
                    entries.sort_unstable_by_key(|entry| entry.bucket.count);
                }
            }
            OrderTarget::Key => {
                if BucketMap::SORTED_BY_KEY {
                    if req.order.order == Order::Desc {
                        entries.reverse();
                    }
                } else if req.order.order == Order::Desc {
                    entries.sort_unstable_by(|left, right| right.key.cmp(&left.key));
                } else {
                    entries.sort_unstable_by(|left, right| left.key.cmp(&right.key));
                }
            }
            OrderTarget::SubAggregation(sub_agg_path) => {
                let collector = sub_agg_collector.as_deref().ok_or_else(|| {
                    TantivyError::InvalidArgument(format!(
                        "Could not find sub-aggregation collector for path {sub_agg_path}"
                    ))
                })?;
                let (agg_name, agg_prop) = get_agg_name_and_property(sub_agg_path);
                let mut keyed: Vec<(f64, MultiTermsBucketEntry<_, _>)> = entries
                    .into_iter()
                    .map(|entry| {
                        let metric = collector
                            .compute_metric_value(
                                entry.bucket.bucket_id.to_bucket_id(),
                                agg_name,
                                agg_prop,
                                agg_data,
                            )
                            .unwrap_or(f64::MIN);
                        (metric, entry)
                    })
                    .collect();
                if req.order.order == Order::Desc {
                    keyed.sort_unstable_by(|left, right| right.0.total_cmp(&left.0));
                } else {
                    keyed.sort_unstable_by(|left, right| left.0.total_cmp(&right.0));
                }
                entries = keyed.into_iter().map(|(_, entry)| entry).collect();
            }
        }

        let (term_doc_count_before_cutoff, sum_other_doc_count) =
            cut_off_buckets(&mut entries, req.segment_size as usize, None);

        let mut result_entries: FxHashMap<Vec<IntermediateKey>, IntermediateTermBucketEntry> =
            FxHashMap::with_capacity_and_hasher(entries.len(), Default::default());

        for entry in entries {
            let intermediate_key = packing.unpack(&entry.key, req_data)?;
            let mut sub_aggregation_res = IntermediateAggregationResults::default();
            if let Some(sub_agg_collector) = sub_agg_collector.as_deref_mut() {
                sub_agg_collector.add_intermediate_aggregation_result(
                    agg_data,
                    &mut sub_aggregation_res,
                    entry.bucket.bucket_id.to_bucket_id(),
                )?;
            }
            let doc_count = entry.bucket.count as u64;
            // Distinct encoded keys can resolve to the same public key (for example a synthetic
            // missing string and a real date). Merge rather than overwriting either contribution.
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

impl<Packing, BucketMap, Buffer> SegmentAggregationCollector
    for SegmentMultiTermsCollector<Packing, BucketMap, Buffer>
where
    Packing: MultiTermsPacking,
    BucketMap: TermAggregationMap<Packing::PackingType>,
    Buffer: SubAggBuffer,
{
    fn add_intermediate_aggregation_result(
        &mut self,
        agg_data: &AggregationsSegmentCtx,
        results: &mut IntermediateAggregationResults,
        bucket: BucketId,
    ) -> crate::Result<()> {
        self.prepare_max_bucket(bucket, agg_data)?;
        let bucket_map = std::mem::replace(
            &mut self.parent_buckets[bucket as usize],
            // This replacement is only a move-out placeholder; collection for this parent bucket
            // is complete, so avoid allocating another dense map (and another full set of ids).
            BucketMap::new(0, &mut self.bucket_id_provider),
        );
        let name = self.req_data.name.clone();
        let result = Self::into_intermediate_bucket_result(
            &self.packing,
            &self.req_data,
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

        let packing = &self.packing;
        self.keys_buf
            .resize_with(docs.len(), || packing.new_packing());
        for key in &mut self.keys_buf {
            packing.clear_packing(key);
        }
        let block_accessor = &mut agg_data.column_block_accessor;
        if self.all_fields_full {
            self.build_keys_from_full_fields(docs, block_accessor);
        } else {
            self.build_keys_from_non_full_fields(docs, block_accessor);
        }

        let buckets = &mut self.parent_buckets[parent_bucket_id as usize];
        let sub_agg = &mut self.sub_agg;
        let bucket_id_provider = &mut self.bucket_id_provider;
        if let Some(sub_agg) = sub_agg {
            let doc_ids = if self.all_fields_full {
                docs
            } else if self.doc_ids_per_partial_combination.is_empty() {
                self.alive_docs.as_slice()
            } else {
                self.doc_ids_per_partial_combination.as_slice()
            };
            for (&doc_id, key) in doc_ids.iter().zip(&self.keys_buf) {
                let bucket_id = buckets.term_entry(key.clone(), bucket_id_provider);
                sub_agg.push(bucket_id.to_bucket_id(), doc_id);
            }
        } else {
            for key in &self.keys_buf {
                buckets.term_entry(key.clone(), bucket_id_provider);
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
            self.parent_buckets.push(BucketMap::new(
                self.map_init_value,
                &mut self.bucket_id_provider,
            ));
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
// Packed-u64 key representation
// ---------------------------------------------------------------------------

/// Per-field bit layout within a packed `u64` key.
///
/// Field 0 occupies the highest bits, so numeric packed-key order is the same as lexicographic raw
/// value order. Values are offset by `min_value`; an injectable missing value is part of that
/// ordinary value range.
#[derive(Clone, Copy, Debug)]
struct FieldPack {
    shift: u32,
    mask: u64,
    min_value: u64,
    max_offset: u64,
}

/// Computes a packed-`u64` layout regardless of column cardinality.
///
/// `None` means that the lossless encoded key needs more than 64 bits. Optional and multivalued
/// columns remain eligible, and a reachable missing encoding is included in its field's range.
fn compute_packed_u64_layout(
    fields: &[MultiTermsFieldAccessor],
    missing_accessors: &[Option<MultiTermsMissingAccessor>],
) -> Option<Vec<FieldPack>> {
    if fields.len() != missing_accessors.len() {
        return None;
    }

    let mut field_layouts = Vec::with_capacity(fields.len());
    let mut total_width = 0u32;
    for (field, missing) in fields.iter().zip(missing_accessors.iter()) {
        let mut min_value = field.column.min_value();
        let mut max_value = field.column.max_value();

        // Full columns cannot emit missing for any document in this segment, so do not widen their
        // packed domain for an unreachable fallback.
        if !field.column.get_cardinality().is_full() {
            if let Some(missing) = missing {
                min_value = min_value.min(missing.missing_value);
                max_value = max_value.max(missing.missing_value);
            }
        }

        let max_offset = max_value - min_value;
        let width = 64 - max_offset.leading_zeros();
        total_width = total_width.checked_add(width)?;
        if total_width > 64 {
            return None;
        }
        field_layouts.push((width, min_value, max_offset));
    }

    let mut packs = Vec::with_capacity(fields.len());
    let mut shift = 0u32;
    for &(width, min_value, max_offset) in field_layouts.iter().rev() {
        let mask = if width == 64 {
            u64::MAX
        } else if width == 0 {
            0
        } else {
            (1u64 << width) - 1
        };
        packs.push(FieldPack {
            shift,
            mask,
            min_value,
            max_offset,
        });
        shift += width;
    }
    packs.reverse();
    Some(packs)
}

#[inline]
fn shift_packed_bits(bits: u64, shift: u32) -> u64 {
    if bits == 0 {
        0
    } else {
        bits.checked_shl(shift)
            .expect("non-zero packed bits fit in u64")
    }
}

fn compute_max_packed(packs: &[FieldPack]) -> u64 {
    packs.iter().fold(0u64, |packed, field| {
        packed | shift_packed_bits(field.max_offset, field.shift)
    })
}

#[derive(Clone, Debug)]
struct PackedU64KeyPacking {
    packs: Vec<FieldPack>,
}

/// Selects the key packing and bucket storage, then boxes the concrete collector.
fn build_multi_terms_collector<BucketSlot: BucketIdSlot>(
    req: &mut AggregationsSegmentCtx,
    node: &AggRefNode,
    req_data: MultiTermsAggReqData,
    packed_layout: Option<Vec<FieldPack>>,
) -> crate::Result<Box<dyn SegmentAggregationCollector>> {
    if let Some(packs) = packed_layout {
        let is_top_level = req_data.is_top_level;
        let max_packed = compute_max_packed(&packs);
        let num_terms = max_packed.saturating_add(1);
        let packing = PackedU64KeyPacking { packs };

        if is_top_level
            && !node.children.is_empty()
            && max_packed < MAX_NUM_TERMS_FOR_LOWCARD_SUBAGG
        {
            return box_multi_terms_collector::<LowCardSubAggBuffer, _, VecTermBuckets<BucketSlot>>(
                req, node, req_data, packing, num_terms, true,
            );
        }
        if is_top_level && max_packed < MAX_NUM_TERMS_FOR_VEC {
            if num_terms > LAZY_BUCKET_ID_GENERATION_THRESHOLD {
                return box_multi_terms_collector::<
                    HighCardSubAggBuffer,
                    _,
                    VecTermBuckets<BucketSlot, true>,
                >(req, node, req_data, packing, num_terms, true);
            }
            return box_multi_terms_collector::<HighCardSubAggBuffer, _, VecTermBuckets<BucketSlot>>(
                req, node, req_data, packing, num_terms, true,
            );
        }
        if is_top_level && max_packed < MAX_NUM_TERMS_FOR_PAGED_MAP {
            return box_multi_terms_collector::<HighCardSubAggBuffer, _, PagedTermMap<BucketSlot>>(
                req, node, req_data, packing, max_packed, false,
            );
        }
        return box_multi_terms_collector::<HighCardSubAggBuffer, _, HashMapTermBuckets<BucketSlot>>(
            req, node, req_data, packing, 0, false,
        );
    }

    let packing = U64ArrayKeyPacking {
        num_fields: req_data.fields.len(),
    };
    box_multi_terms_collector::<
        HighCardSubAggBuffer,
        _,
        HashMapTermBuckets<BucketSlot, MultiTermsKey>,
    >(req, node, req_data, packing, 0, false)
}

fn box_multi_terms_collector<Buffer, Packing, BucketMap>(
    req: &mut AggregationsSegmentCtx,
    node: &AggRefNode,
    req_data: MultiTermsAggReqData,
    packing: Packing,
    map_init_value: u64,
    charge_initial_map_memory: bool,
) -> crate::Result<Box<dyn SegmentAggregationCollector>>
where
    Buffer: SubAggBuffer + 'static,
    Packing: MultiTermsPacking,
    BucketMap: TermAggregationMap<Packing::PackingType>,
{
    let mut bucket_id_provider = BucketIdProvider::default();
    let buckets = BucketMap::new(map_init_value, &mut bucket_id_provider);
    if charge_initial_map_memory {
        req.context
            .limits
            .add_memory_consumed(buckets.get_memory_consumption() as u64)?;
    }
    let sub_agg = if node.children.is_empty() {
        None
    } else {
        Some(BufferedSubAggs::<Buffer>::new(
            build_segment_agg_collectors(req, &node.children)?,
        ))
    };
    let all_fields_full = req_data
        .fields
        .iter()
        .all(|field| field.column.get_cardinality().is_full());
    Ok(Box::new(SegmentMultiTermsCollector::<
        Packing,
        BucketMap,
        Buffer,
    > {
        parent_buckets: vec![buckets],
        sub_agg,
        bucket_id_provider,
        req_data,
        packing,
        map_init_value,
        all_fields_full,
        keys_buf: Vec::new(),
        alive_docs: Vec::new(),
        doc_ids_per_partial_combination: Vec::new(),
    }))
}

/// Resolve one raw fast-field value, recognizing the configured missing encoding first.
///
/// When the encoding reuses a string term ordinal, both paths resolve identically by construction.
fn resolve_key_value(
    value: u64,
    field_acc: &MultiTermsFieldAccessor,
    missing: Option<&MultiTermsMissingAccessor>,
) -> crate::Result<IntermediateKey> {
    if let Some(missing) = missing.filter(|missing| missing.missing_value == value) {
        return resolve_missing_key(missing, field_acc);
    }

    resolve_column_value(
        value,
        &field_acc.column_type,
        &field_acc.str_dict_column,
        &field_acc.column,
    )
}

/// Resolve the request's missing key using the selected column's lenient numeric coercion. String
/// fallbacks (and numeric fallbacks attached to string columns) keep their request representation.
fn resolve_missing_key(
    missing: &MultiTermsMissingAccessor,
    field_acc: &MultiTermsFieldAccessor,
) -> crate::Result<IntermediateKey> {
    let numeric_value = match missing.key {
        Key::F64(value) => Some(value),
        Key::I64(value) => Some(value as f64),
        Key::U64(value) => Some(value as f64),
        Key::Str(_) => None,
    };
    if field_acc.column_type.numerical_type().is_some() {
        if let Some(value) = numeric_value {
            let encoded = f64_to_fastfield_u64(value, &field_acc.column_type).ok_or_else(|| {
                TantivyError::AggregationError(crate::aggregation::AggregationError::InternalError(
                    format!(
                        "could not encode multi_terms missing value for column type {:?}",
                        field_acc.column_type
                    ),
                ))
            })?;
            return resolve_column_value(
                encoded,
                &field_acc.column_type,
                &field_acc.str_dict_column,
                &field_acc.column,
            );
        }
    }
    Ok(IntermediateKey::from(missing.key.clone()))
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

    pub(crate) fn prune_intermediate_results(
        &mut self,
        req: &MultiTermsAggregation,
        sub_aggregation_req: &Aggregations,
        mode: PruneMode,
    ) -> crate::Result<()> {
        let req = MultiTermsAggregationInternal::from_req(req);
        let size = if mode == PruneMode::Final {
            self.entries
                .retain(|_, entry| entry.doc_count >= req.min_doc_count);
            req.size as usize
        } else {
            req.segment_size as usize
        };

        if self.entries.len() > size {
            let mut entries: Vec<_> = self.entries.drain().collect();
            match &req.order.target {
                OrderTarget::Count => {
                    if req.order.order == Order::Desc {
                        entries.select_nth_unstable_by_key(size, |(_, entry)| {
                            std::cmp::Reverse(entry.doc_count)
                        });
                    } else {
                        entries.select_nth_unstable_by_key(size, |(_, entry)| entry.doc_count);
                    }
                }
                OrderTarget::Key => {
                    entries.select_nth_unstable_by(size, |(left, _), (right, _)| {
                        let cmp = left
                            .iter()
                            .zip(right.iter())
                            .find_map(|(left, right)| {
                                let cmp = left.partial_cmp(right)?;
                                (cmp != std::cmp::Ordering::Equal).then_some(cmp)
                            })
                            .unwrap_or(std::cmp::Ordering::Equal);
                        if req.order.order == Order::Desc {
                            cmp.reverse()
                        } else {
                            cmp
                        }
                    });
                }
                OrderTarget::SubAggregation(sub_agg_path) => {
                    let (agg_name, agg_prop) = get_agg_name_and_property(sub_agg_path);
                    let mut keyed: Vec<_> = entries
                        .into_iter()
                        .map(|entry| {
                            let value = entry
                                .1
                                .sub_aggregation
                                .get_value_from_aggregation(agg_name, agg_prop)
                                .ok()
                                .flatten()
                                .unwrap_or(f64::MIN);
                            (value, entry)
                        })
                        .collect();
                    if req.order.order == Order::Desc {
                        keyed
                            .select_nth_unstable_by(size, |left, right| right.0.total_cmp(&left.0));
                    } else {
                        keyed
                            .select_nth_unstable_by(size, |left, right| left.0.total_cmp(&right.0));
                    }
                    entries = keyed.into_iter().map(|(_, entry)| entry).collect();
                }
            }

            let cutoff_doc_count = entries[size].1.doc_count;
            self.sum_other_doc_count += entries[size..]
                .iter()
                .map(|(_, entry)| entry.doc_count)
                .sum::<u64>();
            if mode == PruneMode::Intermediate {
                self.doc_count_error_upper_bound += cutoff_doc_count;
            }
            entries.truncate(size);
            self.entries = entries.into_iter().collect();
        }

        for entry in self.entries.values_mut() {
            entry
                .sub_aggregation
                .prune_intermediate_results(sub_aggregation_req, mode)?;
        }
        Ok(())
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
    fn test_multi_terms_missing_reuses_existing_term_before_segment_cutoff() -> crate::Result<()> {
        let index =
            build_two_field_index(&[("rock", Some("UNKNOWN")), ("rock", None)], &[2, 3], false)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [
                        {"field": "genre"},
                        {"field": "product", "missing": "UNKNOWN"}
                    ],
                    "size": 1,
                    "segment_size": 1
                },
                "aggs": {
                    "sum_score": {"sum": {"field": "score"}}
                }
            }
        }))?;
        let res = exec_request(agg_req, &index)?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 1, "unexpected buckets: {buckets:?}");
        assert_eq!(buckets[0]["key_as_string"], "rock|UNKNOWN");
        assert_eq!(buckets[0]["doc_count"], 2);
        assert_eq!(buckets[0]["sum_score"]["value"], 5.0);
        assert_eq!(res["mt"]["sum_other_doc_count"], 0);
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
        let num_spilled_fields = MULTI_TERMS_KEY_INLINE_CAPACITY + 2;
        let mut bucket_id_provider = BucketIdProvider::default();
        let mut inline_map = HashMapTermBuckets::<(), MultiTermsKey>::default();
        let mut spilled_map = HashMapTermBuckets::<(), MultiTermsKey>::default();
        let mut expected_spilled_bytes = 0;

        for i in 0..64u64 {
            let inline_key: MultiTermsKey = (0..MULTI_TERMS_KEY_INLINE_CAPACITY)
                .map(|field_idx| i + field_idx as u64)
                .collect();
            let spilled_key: MultiTermsKey = (0..num_spilled_fields)
                .map(|field_idx| i + field_idx as u64)
                .collect();
            assert!(!inline_key.spilled());
            assert!(spilled_key.spilled());
            expected_spilled_bytes += spilled_key.capacity() * std::mem::size_of::<u64>();

            inline_map.term_entry(inline_key, &mut bucket_id_provider);
            spilled_map.term_entry(spilled_key, &mut bucket_id_provider);
        }

        assert_eq!(
            spilled_map.get_memory_consumption() - inline_map.get_memory_consumption(),
            expected_spilled_bytes
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
    // Packed-u64/u64-array key collector tests
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
    fn test_multi_terms_missing_fast_path_noncontiguous_docs() -> crate::Result<()> {
        let index = build_two_field_index(
            &[
                ("even", Some("A")),
                ("odd", Some("B")),
                ("even", None),
                ("odd", Some("B")),
                ("even", Some("A")),
            ],
            &[],
            false,
        )?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [
                        {"field": "genre"},
                        {"field": "product", "missing": "MISSING"}
                    ]
                }
            }
        }))?;
        let res = exec_request_with_query(agg_req, &index, Some(("genre", "even")))?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 2, "unexpected {buckets:?}");
        assert!(buckets
            .iter()
            .any(|bucket| bucket["key_as_string"] == "even|A" && bucket["doc_count"] == 2));
        assert!(buckets
            .iter()
            .any(|bucket| bucket["key_as_string"] == "even|MISSING" && bucket["doc_count"] == 1));
        Ok(())
    }

    #[test]
    fn test_multi_terms_wide_fields_use_u64_array_key() -> crate::Result<()> {
        // `score` alone needs the full 64 bits (max_value close to u64::MAX), so combined
        // with any other field the packed layout exceeds 64 bits. The same collector switches to
        // `MultiTermsKey` while preserving the result.
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
    fn test_multi_terms_filters_single_values_between_fields() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let first_field = schema_builder.add_text_field("first", STRING | FAST);
        let second_field = schema_builder.add_text_field("second", STRING | FAST);
        let third_field = schema_builder.add_text_field("third", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer: IndexWriter = index.writer_with_num_threads(1, 20_000_000)?;
            writer
                .add_document(doc!(first_field => "a", second_field => "x", third_field => "p"))?;
            writer.add_document(doc!(second_field => "x", third_field => "q"))?;
            writer.add_document(doc!(first_field => "b", third_field => "r"))?;
            writer
                .add_document(doc!(first_field => "c", second_field => "z", third_field => "s"))?;
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [
                        {"field": "first"},
                        {"field": "second"},
                        {"field": "third"}
                    ],
                    "order": {"_key": "asc"}
                }
            }
        }))?;
        let res = exec_request(agg_req, &index)?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 2, "unexpected {buckets:?}");
        assert_eq!(buckets[0]["key_as_string"], "a|x|p");
        assert_eq!(buckets[1]["key_as_string"], "c|z|s");
        Ok(())
    }

    #[test]
    fn test_multi_terms_u64_array_packs_optional_field_directly() -> crate::Result<()> {
        // `big` requires 64 bits and `tag` requires another bit, selecting `MultiTermsKey`.
        // `tag` is optional but not multivalued, so collection should still build one key per
        // surviving document directly. Putting it first also exercises validity filtering before
        // decoding the later full column.
        let mut schema_builder = Schema::builder();
        let tag_field = schema_builder.add_text_field("tag", STRING | FAST);
        let big_field = schema_builder
            .add_u64_field("big", crate::schema::NumericOptions::default().set_fast());
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer: IndexWriter = index.writer_with_num_threads(1, 20_000_000)?;
            writer.add_document(doc!(tag_field => "a", big_field => 0u64))?;
            writer.add_document(doc!(big_field => u64::MAX))?;
            writer.add_document(doc!(tag_field => "b", big_field => 5u64))?;
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [{"field": "tag"}, {"field": "big"}],
                    "order": {"_key": "asc"}
                }
            }
        }))?;
        let res = exec_request(agg_req, &index)?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 2, "unexpected {buckets:?}");
        assert_eq!(buckets[0]["key_as_string"], "a|0");
        assert_eq!(buckets[1]["key_as_string"], "b|5");

        let missing_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [
                        {"field": "tag", "missing": "MISSING"},
                        {"field": "big"}
                    ],
                    "order": {"_key": "asc"}
                }
            }
        }))?;
        let res = exec_request(missing_req, &index)?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 3, "unexpected {buckets:?}");
        assert!(buckets
            .iter()
            .any(|bucket| bucket["key_as_string"] == "MISSING|18446744073709551615"));
        Ok(())
    }

    #[test]
    fn test_multi_terms_u64_max_handles_multivalue_missing_and_sub_aggregation() -> crate::Result<()>
    {
        let mut schema_builder = Schema::builder();
        let tag_field = schema_builder.add_text_field("tag", STRING | FAST);
        let big_field = schema_builder
            .add_u64_field("big", crate::schema::NumericOptions::default().set_fast());
        let score_field = schema_builder
            .add_u64_field("score", crate::schema::NumericOptions::default().set_fast());
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer: IndexWriter = index.writer_with_num_threads(1, 20_000_000)?;
            let mut first = doc!(big_field => 0u64, score_field => 1u64);
            first.add_text(tag_field, "a");
            first.add_text(tag_field, "b");
            writer.add_document(first)?;
            writer.add_document(doc!(
                tag_field => "a",
                big_field => u64::MAX,
                score_field => 2u64
            ))?;
            writer.add_document(doc!(tag_field => "b", score_field => 3u64))?;
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [
                        {"field": "tag"},
                        {"field": "big", "missing": 7}
                    ],
                    "size": 10,
                    "order": {"_key": "asc"}
                },
                "aggs": {
                    "sum_score": {"sum": {"field": "score"}}
                }
            }
        }))?;
        let res = exec_request(agg_req, &index)?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        let expected = [
            ("a|0", 1.0),
            ("a|18446744073709551615", 2.0),
            ("b|0", 1.0),
            ("b|7", 3.0),
        ];
        assert_eq!(buckets.len(), expected.len(), "unexpected {buckets:?}");
        for ((expected_key, expected_sum), bucket) in expected.iter().zip(buckets) {
            assert_eq!(bucket["key_as_string"], *expected_key);
            assert_eq!(bucket["doc_count"], 1);
            assert_eq!(bucket["sum_score"]["value"], *expected_sum);
        }
        Ok(())
    }

    #[test]
    fn test_multi_terms_json_type_fanout_uses_optional_columns() -> crate::Result<()> {
        // A JSON field with mixed value types fans out into type-specific collectors. Each
        // selected column is optional in this segment and now remains eligible for packed keys.
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
    fn test_multi_terms_json_type_fanout_missing_and_sub_aggregations() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let attrs_field = schema_builder.add_json_field("attrs", FAST);
        let score_field = schema_builder
            .add_u64_field("score", crate::schema::NumericOptions::default().set_fast());
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer: IndexWriter = index.writer_with_num_threads(1, 20_000_000)?;
            writer.add_document(
                doc!(attrs_field => json!({"left": "x", "right": "y"}), score_field => 1u64),
            )?;
            writer.add_document(
                doc!(attrs_field => json!({"left": 10.0, "right": "y"}), score_field => 2u64),
            )?;
            writer.commit()?;

            writer.add_document(
                doc!(attrs_field => json!({"left": "x", "right": true}), score_field => 3u64),
            )?;
            writer.add_document(doc!(attrs_field => json!({"right": "y"}), score_field => 4u64))?;
            writer.add_document(doc!(attrs_field => json!({"left": "x"}), score_field => 5u64))?;
            writer.commit()?;

            writer.add_document(doc!(score_field => 6u64))?;
            // One document with both physical types in both fields must fan out to all four
            // type combinations while its sub-aggregation contribution follows each bucket.
            writer.add_document(doc!(
                attrs_field => json!({"left": ["x", 10.0], "right": ["y", true]}),
                score_field => 7u64
            ))?;
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [
                        {"field": "attrs.left", "missing": "MISSING_LEFT"},
                        {"field": "attrs.right", "missing": "MISSING_RIGHT"}
                    ],
                    "size": 20,
                    "order": {"sum_score": "desc"}
                },
                "aggs": {
                    "sum_score": {"sum": {"field": "score"}}
                }
            }
        }))?;
        let res = exec_request(agg_req, &index)?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        let total_doc_count: u64 = buckets
            .iter()
            .map(|bucket| bucket["doc_count"].as_u64().unwrap())
            .sum();
        assert_eq!(total_doc_count, 10, "unexpected buckets: {buckets:?}");

        let expected = [
            ("x|true", 2, 10.0),
            ("10|y", 2, 9.0),
            ("x|y", 2, 8.0),
            ("10|true", 1, 7.0),
            ("MISSING_LEFT|MISSING_RIGHT", 1, 6.0),
            ("x|MISSING_RIGHT", 1, 5.0),
            ("MISSING_LEFT|y", 1, 4.0),
        ];
        assert_eq!(
            buckets.len(),
            expected.len(),
            "unexpected buckets: {buckets:?}"
        );
        let ordered_keys: Vec<&str> = buckets
            .iter()
            .map(|bucket| bucket["key_as_string"].as_str().unwrap())
            .collect();
        assert_eq!(
            ordered_keys,
            expected.iter().map(|(key, _, _)| *key).collect::<Vec<_>>()
        );
        for (key, doc_count, score_sum) in expected {
            let bucket = buckets
                .iter()
                .find(|bucket| bucket["key_as_string"] == key)
                .unwrap_or_else(|| panic!("missing bucket {key:?} in {buckets:?}"));
            assert_eq!(bucket["doc_count"], doc_count, "bucket {key}");
            assert_eq!(bucket["sum_score"]["value"], score_sum, "bucket {key}");
        }

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
    fn test_multi_terms_expands_multiple_multivalued_fields_before_filtering() -> crate::Result<()>
    {
        let mut schema_builder = Schema::builder();
        let first_field = schema_builder.add_text_field("first", STRING | FAST);
        let second_field = schema_builder.add_text_field("second", STRING | FAST);
        let last_field = schema_builder.add_text_field("last", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer: IndexWriter = index.writer_with_num_threads(1, 20_000_000)?;

            let mut first_doc = doc!(last_field => "p");
            for value in ["a", "b"] {
                first_doc.add_text(first_field, value);
            }
            for value in ["x", "y", "z"] {
                first_doc.add_text(second_field, value);
            }
            writer.add_document(first_doc)?;

            // This document builds a partial key, then gets removed by the final optional field.
            writer.add_document(doc!(first_field => "c", second_field => "q"))?;

            let mut third_doc = doc!(last_field => "u");
            for value in ["d", "e"] {
                third_doc.add_text(first_field, value);
            }
            for value in ["r", "s"] {
                third_doc.add_text(second_field, value);
            }
            writer.add_document(third_doc)?;
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [
                        {"field": "first"},
                        {"field": "second"},
                        {"field": "last"}
                    ],
                    "size": 20,
                    "order": {"_key": "asc"}
                }
            }
        }))?;
        let res = exec_request(agg_req, &index)?;
        let keys: Vec<&str> = res["mt"]["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|bucket| bucket["key_as_string"].as_str().unwrap())
            .collect();
        assert_eq!(
            keys,
            [
                "a|x|p", "a|y|p", "a|z|p", "b|x|p", "b|y|p", "b|z|p", "d|r|u", "d|s|u", "e|r|u",
                "e|s|u",
            ]
        );
        Ok(())
    }

    #[test]
    fn test_multi_terms_expands_three_multivalued_fields() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let first_field = schema_builder.add_text_field("first", STRING | FAST);
        let second_field = schema_builder.add_text_field("second", STRING | FAST);
        let third_field = schema_builder.add_text_field("third", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer: IndexWriter = index.writer_with_num_threads(1, 20_000_000)?;
            let mut document = doc!();
            for value in ["a", "b"] {
                document.add_text(first_field, value);
            }
            for value in ["x", "y"] {
                document.add_text(second_field, value);
            }
            for value in ["p", "q"] {
                document.add_text(third_field, value);
            }
            writer.add_document(document)?;
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [
                        {"field": "first"},
                        {"field": "second"},
                        {"field": "third"}
                    ],
                    "order": {"_key": "asc"}
                }
            }
        }))?;
        let res = exec_request(agg_req, &index)?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        let keys: Vec<&str> = buckets
            .iter()
            .map(|bucket| bucket["key_as_string"].as_str().unwrap())
            .collect();
        assert_eq!(
            keys,
            ["a|x|p", "a|x|q", "a|y|p", "a|y|q", "b|x|p", "b|x|q", "b|y|p", "b|y|q",]
        );
        assert!(buckets.iter().all(|bucket| bucket["doc_count"] == 1));
        Ok(())
    }

    #[test]
    fn test_multi_terms_dedups_mixed_repeats_in_multivalued_field() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let genre_field = schema_builder.add_text_field("genre", STRING | FAST);
        let tag_field = schema_builder.add_text_field("tag", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer: IndexWriter = index.writer_with_num_threads(1, 20_000_000)?;
            let mut d = doc!(genre_field => "rock");
            d.add_text(tag_field, "a");
            d.add_text(tag_field, "a");
            d.add_text(tag_field, "b");
            d.add_text(tag_field, "b");
            d.add_text(tag_field, "c");
            writer.add_document(d)?;
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "mt": {
                "multi_terms": {
                    "terms": [{"field": "genre"}, {"field": "tag"}]
                }
            }
        }))?;
        let res = exec_request(agg_req, &index)?;
        let buckets = res["mt"]["buckets"].as_array().unwrap();
        let mut keys: Vec<&str> = buckets
            .iter()
            .map(|b| b["key_as_string"].as_str().unwrap())
            .collect();
        keys.sort_unstable();
        assert_eq!(keys, vec!["rock|a", "rock|b", "rock|c"]);
        for bucket in buckets {
            assert_eq!(bucket["doc_count"], 1);
        }
        Ok(())
    }

    #[test]
    fn test_multi_terms_dedups_repeated_value_in_multivalued_field() -> crate::Result<()> {
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
    fn test_multi_terms_missing_subagg_value_sorts_last_with_multivalued_field() -> crate::Result<()>
    {
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
            // Multivalued `product` forces Cartesian key generation while retaining the same
            // block-decoding collector and, when its ranges fit, the packed key representation.
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
