use std::fmt::Debug;
use std::hash::Hash;
use std::io;

use columnar::column_values::CompactSpaceU64Accessor;
use columnar::{Column, ColumnType, Dictionary, StrColumn};
use common::{BitSet, TinySet};
use datasketches::hll::{Coupon, HllSketch, HllType, HllUnion};
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::aggregation::agg_data::AggregationsSegmentCtx;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateMetricResult,
};
use crate::aggregation::segment_agg_result::SegmentAggregationCollector;
use crate::aggregation::*;
use crate::TantivyError;

/// Log2 of the number of registers for the HLL sketch.
/// 2^11 = 2048 registers, giving ~2.3% relative error and ~1KB per sketch (Hll4).
const LG_K: u8 = 11;

/// Promote FxHashSet<u64> -> PagedBitset at ~3% density (`len * 32 >
/// dict_num_terms`). Past this point the bitset (~`dict_num_terms / 7.5`
/// bytes) is smaller than the hashset (~10 B/entry minimum) and avoids
/// the per-insert hash.
const PROMOTION_RATIO: u64 = 32;

/// # Cardinality
///
/// The cardinality aggregation allows for computing an estimate
/// of the number of different values in a data set based on the
/// Apache DataSketches HyperLogLog algorithm. This is particularly useful for
/// understanding the uniqueness of values in a large dataset where counting
/// each unique value individually would be computationally expensive.
///
/// For example, you might use a cardinality aggregation to estimate the number
/// of unique visitors to a website by aggregating on a field that contains
/// user IDs or session IDs.
///
/// To use the cardinality aggregation, you'll need to provide a field to
/// aggregate on. The following example demonstrates a request for the cardinality
/// of the "user_id" field:
///
/// ```JSON
/// {
///     "cardinality": {
///         "field": "user_id"
///     }
/// }
/// ```
///
/// This request will return an estimate of the number of unique values in the
/// "user_id" field.
///
/// ## Missing Values
///
/// The `missing` parameter defines how documents that are missing a value should be treated.
/// By default, documents without a value for the specified field are ignored. However, you can
/// specify a default value for these documents using the `missing` parameter. This can be useful
/// when you want to include documents with missing values in the aggregation.
///
/// For example, the following request treats documents with missing values in the "user_id"
/// field as if they had a value of "unknown":
///
/// ```JSON
/// {
///     "cardinality": {
///         "field": "user_id",
///         "missing": "unknown"
///     }
/// }
/// ```
///
/// # Estimation Accuracy
///
/// The cardinality aggregation provides an approximate count, which is usually
/// accurate within a small error range. This trade-off allows for efficient
/// computation even on very large datasets.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CardinalityAggregationReq {
    /// The field name to compute the percentiles on.
    pub field: String,
    /// The missing parameter defines how documents that are missing a value should be treated.
    /// By default they will be ignored but it is also possible to treat them as if they had a
    /// value. Examples in JSON format:
    /// { "field": "my_numbers", "missing": "10.0" }
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub missing: Option<Key>,
}

/// Contains all information required by the SegmentCardinalityCollector to perform the
/// cardinality aggregation on a segment.
pub struct CardinalityAggReqData {
    /// The column accessor to access the fast field values.
    pub accessor: Column<u64>,
    /// The column_type of the field.
    pub column_type: ColumnType,
    /// The string dictionary column if the field is of type string.
    pub str_dict_column: Option<StrColumn>,
    /// The missing value normalized to the internal u64 representation of the field type.
    pub missing_value_for_accessor: Option<u64>,
    /// The name of the aggregation.
    pub name: String,
    /// The aggregation request.
    pub req: CardinalityAggregationReq,
}

impl CardinalityAggReqData {
    /// Estimate the memory consumption of this struct in bytes.
    pub fn get_memory_consumption(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl CardinalityAggregationReq {
    /// Creates a new [`CardinalityAggregationReq`] instance from a field name.
    pub fn from_field_name(field_name: String) -> Self {
        Self {
            field: field_name,
            missing: None,
        }
    }
    /// Returns the field name the aggregation is computed on.
    pub fn field_name(&self) -> &str {
        &self.field
    }
}

/// A CouponCache is here to cache the mapping term ordinal -> coupon (see above).
/// The idea is that we do not want to fetch terms associated to several term ordinals,
/// several times due to the fact that we have several buckets.
enum CouponCache {
    Dense {
        coupon_map: Vec<Coupon>,
        missing_coupon_opt: Option<Coupon>,
    },
    Sparse {
        coupon_map: FxHashMap<u64, Coupon>,
        missing_coupon_opt: Option<Coupon>,
    },
}

impl CouponCache {
    fn new(
        term_ords: Vec<u64>,
        coupons: Vec<Coupon>,
        missing_coupon_opt: Option<Coupon>,
    ) -> CouponCache {
        let num_terms = term_ords.len();
        assert_eq!(num_terms, coupons.len());
        if term_ords.is_empty() {
            return CouponCache::Dense {
                coupon_map: Vec::new(),
                missing_coupon_opt,
            };
        }
        let highest_term_ord = term_ords.last().copied().unwrap_or(0u64);
        // We prefer the dense implementation, if it is not too wasteful.
        // There are two cases for which we can use it.
        // 1- if the data is small.
        // 2- if the data is not necessarily small, but due to a high occupancy ratio, the RAM usage
        // is not that much bigger than if we had used a HashSet. (occupancy ratio + extra
        // metadata ~ x2.25)
        let should_use_dense =
            highest_term_ord < 1_000_000u64 || highest_term_ord < num_terms as u64 * 3u64;
        if should_use_dense {
            let mut coupon_map: Vec<Coupon> = vec![Coupon::EMPTY; highest_term_ord as usize + 1];
            for (term_ord, coupon) in term_ords.into_iter().zip(coupons.into_iter()) {
                coupon_map[term_ord as usize] = coupon;
            }
            CouponCache::Dense {
                coupon_map,
                missing_coupon_opt,
            }
        } else {
            let coupon_map: FxHashMap<u64, Coupon> = term_ords.into_iter().zip(coupons).collect();
            CouponCache::Sparse {
                coupon_map,
                missing_coupon_opt,
            }
        }
    }
}

// =================================================================
// PagedBitset: a sparse bitset indexed by term_ord.
//
// Used as the dense alternative to FxHashSet<u64> once a string
// cardinality bucket has accumulated enough unique term ordinals.
// Memory is bounded to (touched pages) * (page bytes), not
// (max_term_ord / 8).
//
// Page geometry mirrors `PagedTermMap` in `term_agg.rs`: 1024 ords
// per page, lazy `Vec<Option<Box<Page>>>` directory.
// =================================================================
const BITSET_PAGE_SHIFT: u32 = 10;
const BITSET_PAGE_BITS: u64 = 1u64 << BITSET_PAGE_SHIFT; // 1024
const BITSET_PAGE_MASK: u64 = BITSET_PAGE_BITS - 1;
const BITSET_WORDS_PER_PAGE: usize = (BITSET_PAGE_BITS / 64) as usize; // 16

#[derive(Clone)]
struct PagedBitsetPage {
    words: [TinySet; BITSET_WORDS_PER_PAGE],
}

impl PagedBitsetPage {
    fn new() -> Self {
        Self {
            words: [TinySet::empty(); BITSET_WORDS_PER_PAGE],
        }
    }
}

pub(crate) struct PagedBitset {
    pages: Vec<Option<Box<PagedBitsetPage>>>,
    /// Cached number of set bits, maintained on insert.
    count: u64,
}

impl PagedBitset {
    /// Allocates a directory big enough to hold ords up to and including
    /// `max_term_ord`. Pages are allocated lazily on first set.
    fn with_max_term_ord(max_term_ord: u64) -> Self {
        let max_page_idx = (max_term_ord >> BITSET_PAGE_SHIFT) as usize;
        let num_pages = max_page_idx + 1;
        Self {
            pages: vec![None; num_pages],
            count: 0,
        }
    }

    #[inline]
    fn insert(&mut self, term_ord: u64) {
        let page_idx = (term_ord >> BITSET_PAGE_SHIFT) as usize;
        let intra = term_ord & BITSET_PAGE_MASK;
        let word_idx = (intra >> 6) as usize;
        let bit_idx = (intra & 63) as u32;

        let page = match &mut self.pages[page_idx] {
            Some(p) => p,
            None => {
                self.pages[page_idx] = Some(Box::new(PagedBitsetPage::new()));
                self.pages[page_idx].as_mut().unwrap()
            }
        };
        if page.words[word_idx].insert_mut(bit_idx) {
            self.count += 1;
        }
    }

    /// Number of set bits. O(1).
    #[inline]
    fn len(&self) -> u64 {
        self.count
    }

    /// Iterate set ords in ascending order.
    fn iter_sorted(&self) -> impl Iterator<Item = u64> + '_ {
        self.pages
            .iter()
            .enumerate()
            .filter_map(|(page_idx, page_opt)| page_opt.as_ref().map(|p| (page_idx, p)))
            .flat_map(|(page_idx, page)| {
                let page_base_ord = (page_idx as u64) << BITSET_PAGE_SHIFT;
                page.words
                    .iter()
                    .enumerate()
                    .flat_map(move |(word_idx, &word)| {
                        let word_base_ord = page_base_ord + (word_idx as u64) * 64;
                        word.into_iter()
                            .map(move |bit| word_base_ord + u64::from(bit))
                    })
            })
    }
}

/// Threshold below which we use `BitSet` instead of `TermOrdSet`.
///
/// Both `BitSet` and `FxHashSet<u64>` have the same 32-byte struct, so the comparison is heap only:
///   * `BitSet` at T=256: 5 `TinySet` words covering 258 bits (with the missing-value sentinel) =
///     40 bytes.
///   * `FxHashSet<u64>` after one insert: 4-bucket hashbrown table ≈ 56 bytes
pub(crate) const BITSET_MAX_TERM_ORD: u64 = 256;

// =================================================================
// TermOrdAccumulator: per-bucket abstraction over the entries set.
//
// Implementations:
//   - `BitSet` (from `common`): used when `column.max_value()` is small (< BITSET_MAX_TERM_ORD).
//     Pre-allocated, no promotion.
//   - `TermOrdSet`: adaptive, starts as FxHashSet and promotes to a paged bitset when occupancy
//     crosses the density threshold (only if promotion is enabled — typically gated on top-level
//     aggregation).
//
// The trait lets `SegmentCardinalityCollector` be generic over the choice
// so the hot collect() loop monomorphizes to a direct call (no enum
// dispatch per insert).
// =================================================================
pub(crate) trait TermOrdAccumulator: Sized {
    /// Construct an empty accumulator.
    /// `max_term_ord_inclusive` is the largest term_ord that may be
    /// inserted (used to size pre-allocated bitsets and the dense bitset
    /// on promotion).
    fn new(max_term_ord_inclusive: u64) -> Self;
    fn insert(&mut self, term_ord: u64);
    /// Bulk insert. Implementations may override to hoist any inner
    /// dispatch outside the loop. Default loops `insert`.
    #[inline]
    fn extend_from_iter<I: IntoIterator<Item = u64>>(&mut self, ords: I) {
        for ord in ords {
            self.insert(ord);
        }
    }
    /// Hook called once per ingested block. Adaptive impls use this to
    /// decide on sparse->dense promotion.
    fn maybe_compact(&mut self) {}
    fn len(&self) -> usize;
    fn iter_ords(&self) -> impl Iterator<Item = u64> + '_;
}

impl TermOrdAccumulator for BitSet {
    #[inline]
    fn new(max_term_ord_inclusive: u64) -> Self {
        // `BitSet::with_max_value(M)` accepts ords in [0, M).
        // We need ords up to and including `max_term_ord_inclusive`, plus
        // the missing-value sentinel `column.max_value() + 1`.
        BitSet::with_max_value((max_term_ord_inclusive + 2) as u32)
    }
    #[inline]
    fn insert(&mut self, term_ord: u64) {
        BitSet::insert(self, term_ord as u32);
    }
    #[inline]
    fn len(&self) -> usize {
        BitSet::len(self)
    }
    fn iter_ords(&self) -> impl Iterator<Item = u64> + '_ {
        // `BitSet` itself doesn't expose iteration, but
        // `BitSet::tinyset(bucket)` does. Walk per-bucket and yield each
        // set bit. The capacity is `max_value()`; iterating to
        // `div_ceil(64)` covers every possible ord exactly once.
        let num_buckets = self.max_value().div_ceil(64);
        (0..num_buckets).flat_map(move |bucket| {
            let chunk_base = u64::from(bucket) * 64;
            self.tinyset(bucket)
                .into_iter()
                .map(move |bit| chunk_base + u64::from(bit))
        })
    }
}

// =================================================================
// TermOrdSet: adaptive sparse->dense accumulator.
//
// Starts as an FxHashSet (cheap when few ords are seen). When occupancy
// crosses `len * PROMOTION_RATIO > max_term_ord_inclusive`, drains into
// a `PagedBitset` and continues dense. Promotion is one-way.
// =================================================================
pub(crate) struct TermOrdSet {
    inner: TermOrdSetInner,
    /// Largest term_ord that may be inserted. Used for both sizing the
    /// dense bitset on promotion and as the promotion-threshold reference.
    max_term_ord_inclusive: u64,
}

enum TermOrdSetInner {
    Sparse(FxHashSet<u64>),
    Dense(PagedBitset),
}

impl TermOrdAccumulator for TermOrdSet {
    fn new(max_term_ord_inclusive: u64) -> Self {
        Self {
            inner: TermOrdSetInner::Sparse(FxHashSet::default()),
            max_term_ord_inclusive,
        }
    }

    #[inline]
    fn insert(&mut self, term_ord: u64) {
        match &mut self.inner {
            TermOrdSetInner::Sparse(set) => {
                set.insert(term_ord);
            }
            TermOrdSetInner::Dense(bitset) => bitset.insert(term_ord),
        }
    }

    /// Hoist the Sparse/Dense match outside the per-ord loop so that a
    /// block of inserts dispatches once.
    fn extend_from_iter<I: IntoIterator<Item = u64>>(&mut self, ords: I) {
        match &mut self.inner {
            TermOrdSetInner::Sparse(set) => {
                for ord in ords {
                    set.insert(ord);
                }
            }
            TermOrdSetInner::Dense(bitset) => {
                for ord in ords {
                    bitset.insert(ord);
                }
            }
        }
    }

    fn maybe_compact(&mut self) {
        let TermOrdSetInner::Sparse(set) = &mut self.inner else {
            return;
        };
        if set.len() > 1 && set.len() as u64 * PROMOTION_RATIO <= self.max_term_ord_inclusive {
            return;
        }
        // Size for ord <= max_term_ord_inclusive plus the missing sentinel
        // (column.max_value() + 1, which may equal max_term_ord_inclusive
        // when the column references every dictionary term).
        let mut bitset = PagedBitset::with_max_term_ord(self.max_term_ord_inclusive + 1);
        let set = std::mem::take(set);
        for ord in set {
            bitset.insert(ord);
        }
        self.inner = TermOrdSetInner::Dense(bitset);
    }

    fn len(&self) -> usize {
        match &self.inner {
            TermOrdSetInner::Sparse(set) => set.len(),
            TermOrdSetInner::Dense(bitset) => bitset.len() as usize,
        }
    }

    fn iter_ords(&self) -> impl Iterator<Item = u64> + '_ {
        match &self.inner {
            TermOrdSetInner::Sparse(set) => itertools::Either::Left(set.iter().copied()),
            TermOrdSetInner::Dense(bitset) => itertools::Either::Right(bitset.iter_sorted()),
        }
    }
}

pub(crate) struct SegmentCardinalityCollector<S: TermOrdAccumulator> {
    /// Buckets are Some(_) until they get consumed by into_intermediate_results().
    buckets: Vec<Option<SegmentCardinalityCollectorBucket<S>>>,
    accessor_idx: usize,
    /// The column accessor to access the fast field values.
    accessor: Column<u64>,
    /// The column_type of the field.
    column_type: ColumnType,
    /// The missing value normalized to the internal u64 representation of the field type.
    missing_value_for_accessor: Option<u64>,
    coupon_cache: Option<CouponCache>,
    /// Largest term_ord that may be inserted into a bucket. For str columns
    /// this is `accessor.max_value()`; for non-str columns this is unused
    /// (no inserts go into `entries`) and set to 0.
    max_term_ord_inclusive: u64,
}

impl<S: TermOrdAccumulator> Debug for SegmentCardinalityCollector<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("SegmentCardinalityCollector")
            .field("column_type", &self.column_type)
            .field(
                "missing_value_for_accessor",
                &self.missing_value_for_accessor,
            )
            .finish()
    }
}

pub(crate) struct SegmentCardinalityCollectorBucket<S: TermOrdAccumulator> {
    cardinality: CardinalityCollector,
    entries: S,
}
impl<S: TermOrdAccumulator> SegmentCardinalityCollectorBucket<S> {
    #[inline(always)]
    pub fn new(column_type: ColumnType, max_term_ord_inclusive: u64) -> Self {
        Self {
            cardinality: CardinalityCollector::new(column_type as u8),
            entries: S::new(max_term_ord_inclusive),
        }
    }

    // Returns a intermediate metric result.
    //
    // If the column is not str, the values have been added to the
    // sketch during collection.
    //
    // If the column is str, then the values are dictionary encoded
    // and have not been added to the sketch yet.
    // We need to resolves the term ords accumulated in self.entries
    // with the coupon cache, and append the results to the sketch.
    fn into_intermediate_metric_result(
        mut self,
        coupon_cache_opt: Option<&CouponCache>,
    ) -> crate::Result<IntermediateMetricResult> {
        if let Some(coupon_cache) = coupon_cache_opt {
            // Sketch must be empty for str columns: coupons are appended here
            // from the term_ord set (and not directly during collection).
            assert!(self.cardinality.sketch.is_empty());
            append_to_sketch(&self.entries, coupon_cache, &mut self.cardinality);
        }
        Ok(IntermediateMetricResult::Cardinality(self.cardinality))
    }
}

/// Builds a coupon cache from the given buckets, dictionary, and optional missing value.
/// Returns a mapping from term_ord to the hash (coupon) of the associated term.
fn build_coupon_cache<S: TermOrdAccumulator>(
    buckets: &[Option<SegmentCardinalityCollectorBucket<S>>],
    dictionary: &Dictionary,
    missing_value_opt: Option<&Key>,
) -> io::Result<CouponCache> {
    let term_ords_capacity: usize = buckets
        .iter()
        .flatten()
        .map(|bucket| bucket.entries.len())
        .max()
        .unwrap_or(0)
        * 2;
    let mut term_ords_set = FxHashSet::with_capacity_and_hasher(term_ords_capacity, FxBuildHasher);
    for bucket in buckets.iter().flatten() {
        term_ords_set.extend(bucket.entries.iter_ords());
    }
    let mut term_ords: Vec<u64> = term_ords_set.into_iter().collect();
    term_ords.sort_unstable();

    term_ords.pop_if(|highest_term_ord| *highest_term_ord >= dictionary.num_terms() as u64);

    let mut coupons: Vec<Coupon> = Vec::with_capacity(term_ords.len());
    let all_term_ords_found: bool =
        dictionary.sorted_ords_to_term_cb(&term_ords, |term_bytes| {
            let coupon: Coupon = Coupon::from_hash(term_bytes);
            coupons.push(coupon);
        })?;
    assert!(all_term_ords_found);

    // Regardless of whether or not there is effectively a missing value in one of the buckets,
    // we populate the cache with the missing key too (if any).
    let missing_coupon_opt: Option<Coupon> = missing_value_opt.map(|missing_key| {
        if let Key::Str(missing_value_str) = missing_key {
            Coupon::from_hash(missing_value_str.as_bytes())
        } else {
            // See https://github.com/quickwit-oss/tantivy/issues/2891
            // A missing key with a type different from Str will not work as intended
            // for the moment.
            //
            // Right now this is just a partial workaround.
            Coupon::from_hash("__tantivy_missing_non_str__".as_bytes())
        }
    });
    Ok(CouponCache::new(term_ords, coupons, missing_coupon_opt))
}

fn append_to_sketch<S: TermOrdAccumulator>(
    term_ords: &S,
    coupon_cache: &CouponCache,
    sketch: &mut CardinalityCollector,
) {
    match coupon_cache {
        CouponCache::Dense {
            coupon_map,
            missing_coupon_opt,
        } => {
            for term_ord in term_ords.iter_ords() {
                if let Some(coupon) = coupon_map
                    .get(term_ord as usize)
                    .copied()
                    .or(*missing_coupon_opt)
                {
                    sketch.insert_coupon(coupon);
                }
            }
        }
        CouponCache::Sparse {
            coupon_map,
            missing_coupon_opt,
        } => {
            for term_ord in term_ords.iter_ords() {
                if let Some(coupon) = coupon_map.get(&term_ord).copied().or(*missing_coupon_opt) {
                    sketch.insert_coupon(coupon);
                }
            }
        }
    }
}

impl<S: TermOrdAccumulator> SegmentCardinalityCollector<S> {
    pub fn from_req(
        column_type: ColumnType,
        accessor_idx: usize,
        accessor: Column<u64>,
        missing_value_for_accessor: Option<u64>,
        max_term_ord_inclusive: u64,
    ) -> Self {
        Self {
            buckets: Vec::new(),
            column_type,
            accessor_idx,
            accessor,
            missing_value_for_accessor,
            coupon_cache: None,
            max_term_ord_inclusive,
        }
    }

    fn fetch_block_with_field(
        &mut self,
        docs: &[crate::DocId],
        agg_data: &mut AggregationsSegmentCtx,
    ) {
        agg_data.column_block_accessor.fetch_block_with_missing(
            docs,
            &self.accessor,
            self.missing_value_for_accessor,
        );
    }
}

impl<S: TermOrdAccumulator + 'static> SegmentAggregationCollector
    for SegmentCardinalityCollector<S>
{
    fn add_intermediate_aggregation_result(
        &mut self,
        agg_data: &AggregationsSegmentCtx,
        results: &mut IntermediateAggregationResults,
        bucket_id: BucketId,
    ) -> crate::Result<()> {
        self.prepare_max_bucket(bucket_id, agg_data)?;
        let req_data = &agg_data.get_cardinality_req_data(self.accessor_idx);
        // Strings are dictionary encoded. Fetching the terms associated to strings
        // is expensive. For this reason, we do that once for all buckets and cache the results
        // here.
        if let Some(str_dict_column) = &req_data.str_dict_column {
            // Ensure the coupon cache is populated.
            // A mapping from term_ord to the hash of the associated term.
            // The missing value sentinel will be associated to the hash of the missing value if
            // any.
            if self.coupon_cache.is_none() {
                self.coupon_cache = Some(build_coupon_cache(
                    &self.buckets,
                    str_dict_column.dictionary(),
                    req_data.req.missing.as_ref(),
                )?);
            }
        }
        let name = req_data.name.to_string();
        // take the bucket in buckets and replace it with a new empty one
        let Some(bucket) = self.buckets[bucket_id as usize].take() else {
            return Err(crate::TantivyError::InternalError(
                "the same bucket should not be finalized twice.".to_string(),
            ));
        };
        let intermediate_result =
            bucket.into_intermediate_metric_result(self.coupon_cache.as_ref())?;
        results.push(
            name,
            IntermediateAggregationResult::Metric(intermediate_result),
        )?;

        Ok(())
    }

    fn collect(
        &mut self,
        parent_bucket_id: BucketId,
        docs: &[crate::DocId],
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        self.fetch_block_with_field(docs, agg_data);
        let Some(bucket) = &mut self.buckets[parent_bucket_id as usize].as_mut() else {
            return Err(crate::TantivyError::InternalError(
                "collection should not happen after finalization".to_string(),
            ));
        };
        let col_block_accessor = &agg_data.column_block_accessor;
        if self.column_type == ColumnType::Str {
            // The trait dispatches once per block (via `extend_from_iter`)
            // for adaptive variants and inlines to a tight loop for the
            // BitSet path. `maybe_compact` is the per-block hook for
            // promotion.
            bucket
                .entries
                .extend_from_iter(col_block_accessor.iter_vals());
            bucket.entries.maybe_compact();
        } else if self.column_type == ColumnType::IpAddr {
            let compact_space_accessor = self
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
            for val in col_block_accessor.iter_vals() {
                let val: u128 = compact_space_accessor.compact_to_u128(val as u32);
                bucket.cardinality.insert(val);
            }
        } else {
            for val in col_block_accessor.iter_vals() {
                bucket.cardinality.insert(val);
            }
        }

        Ok(())
    }

    fn prepare_max_bucket(
        &mut self,
        max_bucket: BucketId,
        _agg_data: &AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        if max_bucket as usize >= self.buckets.len() {
            let column_type = self.column_type;
            let max_term_ord_inclusive = self.max_term_ord_inclusive;
            self.buckets.resize_with(max_bucket as usize + 1, || {
                Some(SegmentCardinalityCollectorBucket::<S>::new(
                    column_type,
                    max_term_ord_inclusive,
                ))
            });
        }
        Ok(())
    }

    fn compute_metric_value(
        &self,
        bucket_id: BucketId,
        sub_agg_name: &str,
        sub_agg_property: &str,
        agg_data: &AggregationsSegmentCtx,
    ) -> Option<f64> {
        let req_data = &agg_data.get_cardinality_req_data(self.accessor_idx);
        if req_data.name != sub_agg_name || !sub_agg_property.is_empty() {
            return None;
        }
        let bucket = self.buckets.get(bucket_id as usize)?.as_ref()?;
        // For string columns the HLL sketch is empty until materialization; entries holds
        // the deduplicated term ordinals seen, which is the exact distinct count.
        // For numeric columns the sketch is populated during collect.
        if self.column_type == ColumnType::Str {
            Some(bucket.entries.len() as f64)
        } else {
            Some(bucket.cardinality.sketch.estimate().trunc())
        }
    }
}

#[derive(Clone, Debug)]
/// The cardinality collector used during segment collection and for merging results.
/// Uses Apache DataSketches HLL (lg_k=11, Hll4) for compact binary serialization
/// and cross-language compatibility (e.g. Java `datasketches` library).
pub struct CardinalityCollector {
    sketch: HllSketch,
    /// Salt derived from `ColumnType`, used to differentiate values of different column types
    /// that map to the same u64 (e.g. bool `false` = 0 vs i64 `0`).
    /// Not serialized — only needed during insertion, not after sketch registers are populated.
    salt: u8,
}

impl Default for CardinalityCollector {
    fn default() -> Self {
        Self::new(0)
    }
}

impl PartialEq for CardinalityCollector {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

impl Serialize for CardinalityCollector {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let bytes = self.sketch.serialize();
        serializer.serialize_bytes(&bytes)
    }
}

impl<'de> Deserialize<'de> for CardinalityCollector {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
        let sketch = HllSketch::deserialize(&bytes).map_err(serde::de::Error::custom)?;
        Ok(Self { sketch, salt: 0 })
    }
}

impl CardinalityCollector {
    fn new(salt: u8) -> Self {
        Self {
            sketch: HllSketch::new(LG_K, HllType::Hll4),
            salt,
        }
    }

    /// Insert a value into the HLL sketch, salted by the column type.
    /// The salt ensures that identical u64 values from different column types
    /// (e.g. bool `false` vs i64 `0`) are counted as distinct.
    fn insert<T: Hash>(&mut self, value: T) {
        self.sketch.update((self.salt, value));
    }

    fn insert_coupon(&mut self, coupon: Coupon) {
        self.sketch.update_with_coupon(coupon);
    }

    /// Compute the final cardinality estimate.
    pub fn finalize(self) -> Option<f64> {
        Some(self.sketch.estimate().trunc())
    }

    /// Serialize the HLL sketch to its compact binary representation.
    /// The format is cross-language compatible with Apache DataSketches (Java, C++, Python).
    pub fn to_sketch_bytes(&self) -> Vec<u8> {
        self.sketch.serialize()
    }

    pub(crate) fn merge_fruits(&mut self, right: CardinalityCollector) -> crate::Result<()> {
        let mut union = HllUnion::new(LG_K);
        union.update(&self.sketch);
        union.update(&right.sketch);
        self.sketch = union.to_sketch(HllType::Hll4);
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::net::IpAddr;
    use std::str::FromStr;

    use columnar::MonotonicallyMappableToU64;

    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::tests::{exec_request, get_test_index_from_terms};
    use crate::schema::{IntoIpv6Addr, Schema, FAST, STRING};
    use crate::Index;

    #[test]
    fn cardinality_aggregation_test_empty_index() -> crate::Result<()> {
        let values = vec![];
        let index = get_test_index_from_terms(false, &values)?;
        let agg_req: Aggregations = serde_json::from_value(json!({
            "cardinality": {
                "cardinality": {
                    "field": "string_id",
                }
            },
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        assert_eq!(res["cardinality"]["value"], 0.0);

        Ok(())
    }

    #[test]
    fn cardinality_aggregation_test_single_segment() -> crate::Result<()> {
        cardinality_aggregation_test_merge_segment(true)
    }
    #[test]
    fn cardinality_aggregation_test() -> crate::Result<()> {
        cardinality_aggregation_test_merge_segment(false)
    }
    fn cardinality_aggregation_test_merge_segment(merge_segments: bool) -> crate::Result<()> {
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
            "cardinality": {
                "cardinality": {
                    "field": "string_id",
                }
            },
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        assert_eq!(res["cardinality"]["value"], 3.0);

        Ok(())
    }

    /// Build a single-segment string-cardinality index with 32 unique terms.
    /// `column.max_value() = 31` is well below `BITSET_MAX_TERM_ORD`,
    /// so the bucket exercises the `BitSet` path end to end.
    #[test]
    fn cardinality_aggregation_test_str_bitset() -> crate::Result<()> {
        let terms: Vec<String> = (0..32).map(|i| format!("term_{i}")).collect();
        let term_refs: Vec<Vec<&str>> = terms.iter().map(|t| vec![t.as_str()]).collect::<Vec<_>>();
        // single segment so we have a single dictionary of 32 terms.
        let index = get_test_index_from_terms(true, &term_refs)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "cardinality": {
                "cardinality": { "field": "string_id" }
            },
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        assert_eq!(res["cardinality"]["value"], 32.0);
        Ok(())
    }

    /// `BitSet` path with a `missing` parameter: the column-level missing
    /// sentinel (`column.max_value() + 1`) flows into the bitset, the
    /// dict lookup filter at finalization drops it, and the missing
    /// coupon is applied separately.
    #[test]
    fn cardinality_aggregation_test_str_bitset_with_missing() {
        let mut schema_builder = Schema::builder();
        let name_field = schema_builder.add_text_field("name", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests().unwrap();
        for i in 0..16 {
            let term = format!("t{i:02}");
            writer.add_document(doc!(name_field => term)).unwrap();
        }
        // One empty doc, exercising the missing sentinel.
        writer.add_document(doc!()).unwrap();
        writer.commit().unwrap();

        let agg_req: Aggregations = serde_json::from_value(json!({
            "cardinality": {
                "cardinality": {
                    "field": "name",
                    "missing": "MISSING_SENTINEL_KEY",
                }
            },
        }))
        .unwrap();

        let res = exec_request(agg_req, &index).unwrap();
        // 16 distinct real terms + 1 distinct "missing" value = 17.
        assert_eq!(res["cardinality"]["value"], 17.0);
    }

    /// Unit-test the PagedBitset itself: cross-page inserts produce sorted
    /// iteration, len() matches the inserted set, and duplicates are
    /// idempotent.
    #[test]
    fn paged_bitset_basic() {
        use super::PagedBitset;
        // Span several pages: BITSET_PAGE_BITS = 1024, so ords > 1024 land
        // on the second page, > 2048 on the third, etc.
        let ords = [0u64, 1, 63, 64, 1023, 1024, 1025, 4096, 4097, 9999, 10_000];
        let max_ord = *ords.iter().max().unwrap();
        let mut bitset = PagedBitset::with_max_term_ord(max_ord);
        for &ord in &ords {
            bitset.insert(ord);
            // Idempotent: inserting again must not increase count.
            bitset.insert(ord);
        }
        assert_eq!(bitset.len(), ords.len() as u64);
        let collected: Vec<u64> = bitset.iter_sorted().collect();
        let mut expected: Vec<u64> = ords.to_vec();
        expected.sort_unstable();
        assert_eq!(collected, expected);
    }

    /// Unit-test `TermOrdSet`: starts Sparse, promotes to Dense on
    /// `maybe_compact` once the density threshold is crossed, and
    /// `iter_ords()` yields the same set in either state. Ords spanning
    /// multiple paged-bitset pages exercise the Dense iter ordering.
    #[test]
    fn term_ord_set_promotes_on_maybe_compact() {
        use super::{TermOrdAccumulator, TermOrdSet, PROMOTION_RATIO};
        // Pick max so promotion needs few inserts: len * RATIO > max with
        // RATIO=32 and max=64 trips at len=3 (3*32=96 > 64).
        let max_term_ord = 64u64;
        let mut set = <TermOrdSet as TermOrdAccumulator>::new(max_term_ord);
        // Two inserts: should stay Sparse after maybe_compact (2 * RATIO = 64, not > 64).
        set.insert(0);
        set.insert(7);
        set.maybe_compact();
        assert_eq!(set.len(), 2);

        // Third insert promotes on next maybe_compact.
        set.insert(20);
        assert_eq!(set.len(), 3);
        // Sanity check: at len=3, 3 * PROMOTION_RATIO = 96 > 64.
        assert!(3u64 * PROMOTION_RATIO > max_term_ord);
        set.maybe_compact();

        // Post-promotion: extending continues to work.
        set.insert(15);
        set.insert(15); // dup
        assert_eq!(set.len(), 4);

        let mut collected: Vec<u64> = set.iter_ords().collect();
        collected.sort_unstable();
        assert_eq!(collected, vec![0, 7, 15, 20]);
    }

    /// Unit-test the `BitSet` impl of `TermOrdAccumulator`: insert,
    /// dedup, and iter_ords order.
    #[test]
    fn bitset_accumulator_basic() {
        use common::BitSet;

        use super::TermOrdAccumulator;
        let mut set = <BitSet as TermOrdAccumulator>::new(255);
        for ord in [0u64, 1, 63, 64, 65, 128, 200, 200, 0] {
            <BitSet as TermOrdAccumulator>::insert(&mut set, ord);
        }
        assert_eq!(<BitSet as TermOrdAccumulator>::len(&set), 7);
        let collected: Vec<u64> = set.iter_ords().collect();
        assert_eq!(collected, vec![0, 1, 63, 64, 65, 128, 200]);
    }

    #[test]
    fn cardinality_aggregation_u64() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_u64_field("id", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer = index.writer_for_tests()?;
            writer.add_document(doc!(id_field => 1u64))?;
            writer.add_document(doc!(id_field => 2u64))?;
            writer.add_document(doc!(id_field => 3u64))?;
            writer.add_document(doc!())?;
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "cardinality": {
                "cardinality": {
                    "field": "id",
                    "missing": 0u64
                },
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        assert_eq!(res["cardinality"]["value"], 4.0);

        Ok(())
    }

    #[test]
    fn cardinality_aggregation_ip_addr() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_ip_addr_field("ip_field", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer = index.writer_for_tests()?;
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
            "cardinality": {
                "cardinality": {
                    "field": "ip_field"
                },
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        assert_eq!(res["cardinality"]["value"], 2.0);

        Ok(())
    }

    #[test]
    fn cardinality_aggregation_json() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_json_field("json", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer = index.writer_for_tests()?;
            writer.add_document(doc!(field => json!({"value": false})))?;
            writer.add_document(doc!(field => json!({"value": true})))?;
            writer.add_document(doc!(field => json!({"value": i64::from_u64(0u64)})))?;
            writer.add_document(doc!(field => json!({"value": i64::from_u64(1u64)})))?;
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "cardinality": {
                "cardinality": {
                    "field": "json.value"
                },
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        assert_eq!(res["cardinality"]["value"], 4.0);

        Ok(())
    }

    #[test]
    fn cardinality_collector_serde_roundtrip() {
        use super::CardinalityCollector;

        let mut collector = CardinalityCollector::default();
        collector.insert("hello");
        collector.insert("world");
        collector.insert("hello"); // duplicate

        let serialized = serde_json::to_vec(&collector).unwrap();
        let deserialized: CardinalityCollector = serde_json::from_slice(&serialized).unwrap();

        let original_estimate = collector.finalize().unwrap();
        let roundtrip_estimate = deserialized.finalize().unwrap();
        assert_eq!(original_estimate, roundtrip_estimate);
        assert_eq!(original_estimate, 2.0);
    }

    #[test]
    fn cardinality_collector_merge() {
        use super::CardinalityCollector;

        let mut left = CardinalityCollector::default();
        left.insert("a");
        left.insert("b");

        let mut right = CardinalityCollector::default();
        right.insert("b");
        right.insert("c");

        left.merge_fruits(right).unwrap();
        let estimate = left.finalize().unwrap();
        assert_eq!(estimate, 3.0);
    }

    /// Verifies that merging two small sketches (both in List/Set coupon mode)
    /// produces an exact result — i.e. the HllUnion does not unnecessarily
    /// promote to the full HLL array when the combined cardinality is small.
    #[test]
    fn cardinality_collector_merge_stays_exact_for_small_sets() {
        use super::CardinalityCollector;

        let mut left = CardinalityCollector::default();
        for i in 0u64..50 {
            left.insert(i);
        }

        let mut right = CardinalityCollector::default();
        for i in 30u64..100 {
            right.insert(i);
        }

        left.merge_fruits(right).unwrap();
        let estimate = left.finalize().unwrap();
        // 100 distinct values (0..100). Both sketches are in Set mode (< 192 coupons),
        // so the union should stay in coupon mode and give an exact count.
        assert_eq!(estimate, 100.0);
    }

    #[test]
    fn cardinality_collector_serialize_deserialize_binary() {
        use datasketches::hll::HllSketch;

        use super::CardinalityCollector;

        let mut collector = CardinalityCollector::default();
        collector.insert("apple");
        collector.insert("banana");
        collector.insert("cherry");

        let bytes = collector.to_sketch_bytes();
        let deserialized = HllSketch::deserialize(&bytes).unwrap();
        assert!((deserialized.estimate() - 3.0).abs() < 0.01);
    }

    /// Tests that the `missing` parameter correctly counts a single empty document
    /// for both u64 and str columns.
    #[test]
    fn cardinality_aggregation_missing_value_single_empty_doc() {
        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_u64_field("id", FAST);
        let name_field = schema_builder.add_text_field("name", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests().unwrap();
        writer
            .add_document(doc!(id_field=>1u64,name_field=>"some_name"))
            .unwrap();
        writer.add_document(doc!()).unwrap();
        writer.commit().unwrap();

        {
            // int colum with missing value non redundant
            let agg_req: Aggregations = serde_json::from_value(json!({
                "cardinality": {
                    "cardinality": {
                        "field": "id",
                        "missing": 42u64
                    },
                }
            }))
            .unwrap();
            let res = exec_request(agg_req, &index).unwrap();
            assert_eq!(res["cardinality"]["value"], 2.0);
        }

        {
            // int colum with missing value redundant
            let agg_req: Aggregations = serde_json::from_value(json!({
                "cardinality": {
                    "cardinality": {
                        "field": "id",
                        "missing": 1u64
                    },
                }
            }))
            .unwrap();
            let res = exec_request(agg_req, &index).unwrap();
            assert_eq!(res["cardinality"]["value"], 1.0);
        }

        {
            // str colum with missing value non redundant
            // With more than one segment, this is not well handled.
            let agg_req: Aggregations = serde_json::from_value(json!({
                "cardinality": {
                    "cardinality": {
                        "field": "name",
                        "missing": "other_name"
                    },
                }
            }))
            .unwrap();
            let res = exec_request(agg_req, &index).unwrap();
            assert_eq!(res["cardinality"]["value"], 2.0);
        }

        {
            // str colum with missing value redundant
            let agg_req: Aggregations = serde_json::from_value(json!({
                "cardinality": {
                    "cardinality": {
                        "field": "name",
                        "missing": "some_name"
                    },
                }
            }))
            .unwrap();
            let res = exec_request(agg_req, &index).unwrap();
            assert_eq!(res["cardinality"]["value"], 1.0);
        }

        {
            // str column with missing value with a number type.
            let agg_req: Aggregations = serde_json::from_value(json!({
                "cardinality": {
                    "cardinality": {
                        "field": "name",
                        "missing": 3,
                    },
                }
            }))
            .unwrap();
            let res = exec_request(agg_req, &index).unwrap();
            assert_eq!(res["cardinality"]["value"], 2.0);
        }
    }

    #[test]
    fn cardinality_collector_salt_differentiates_types() {
        use super::CardinalityCollector;

        // Without salt, same u64 value from different column types would collide
        let mut collector_bool = CardinalityCollector::new(5); // e.g. ColumnType::Bool
        collector_bool.insert(0u64); // false
        collector_bool.insert(1u64); // true

        let mut collector_i64 = CardinalityCollector::new(2); // e.g. ColumnType::I64
        collector_i64.insert(0u64);
        collector_i64.insert(1u64);

        // Merge them
        collector_bool.merge_fruits(collector_i64).unwrap();
        let estimate = collector_bool.finalize().unwrap();
        // Should be 4 because salt makes (5, 0) != (2, 0) and (5, 1) != (2, 1)
        assert_eq!(estimate, 4.0);
    }
}
