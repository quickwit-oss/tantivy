//! Fused collector for the very common shape `terms` (low cardinality) × a single
//! `histogram`/`date_histogram` sub-aggregation with nothing nested below it.
//!
//! See [`SegmentTermHistogramCollector`] for the approach and [`maybe_build_collector`] for the
//! conditions under which it is used.

use std::fmt::Debug;

use columnar::{Column, ColumnBlockAccessor, ColumnType};

use super::{
    Bucket, SegmentTermCollector, TermsAggReqData, VecTermBuckets, MAX_NUM_BUCKETS_FOR_COUNT_LANES,
    NUM_LOW_CARD_COUNT_LANES,
};
use crate::aggregation::agg_data::{AggKind, AggRefNode, AggregationsSegmentCtx};
use crate::aggregation::bucket::{
    get_bucket_pos_f64, prepare_histogram_dense_range, HistogramAggReqData,
    SegmentHistogramCollector,
};
use crate::aggregation::buffered_sub_aggs::LowCardSubAggBuffer;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults,
};
use crate::aggregation::segment_agg_result::{BucketIdProvider, SegmentAggregationCollector};
use crate::aggregation::{f64_from_fastfield_u64, BucketId};

/// Maximum number of physical counters in the fused flat grid. Above this the grid would be too
/// large/cache-unfriendly, so we fall back to the general buffered path. Count lanes are included
/// in this limit: `num_terms × num_time_buckets × LANES` may not exceed it.
///
/// Since we are only at the top-level, this won't be multiplied by any parent buckets.
const MAX_FUSED_GRID_COUNTERS: usize = 16_384;

/// Scalar storage for grids whose term cardinality is too high for count lanes to pay off.
const SINGLE_COUNT_LANE: usize = 1;

/// Fixed linear-scan sizes over encoded `u64` bucket starts. The collector picks the smallest
/// specialization that fits; larger histograms use floating-point bucket computation.
const NUM_SMALL_LINEAR_BUCKETS: usize = 4;
const NUM_LARGE_LINEAR_BUCKETS: usize = 8;

trait BucketResolver: Debug + 'static {
    /// Fetches the histogram values needed for this block. Resolvers that do not inspect the
    /// histogram column (notably [`SingleBucketResolver`]) leave this as a no-op.
    fn prepare_block(&mut self, docs: &[crate::DocId]);

    /// Number of logical histogram buckets produced by this resolver.
    fn num_buckets(&self) -> usize;

    /// Resolves and counts one bucket per term when every histogram value is in bounds. Keeping the
    /// loop inside the resolver lets each implementation compile to its simplest block traversal.
    fn collect_block<const LANES: usize>(
        &mut self,
        term_ids: impl Iterator<Item = u64>,
        counts: &mut [[u32; LANES]],
    );

    /// Resolves values with hard bounds while preserving each term's total document count.
    fn collect_block_with_bounds<const LANES: usize>(
        &mut self,
        term_ids: impl Iterator<Item = u64>,
        counts: &mut [[u32; LANES]],
        term_counts: &mut [[u32; LANES]],
    );
}

#[inline]
fn increment_grid_count<const LANES: usize>(
    counts: &mut [[u32; LANES]],
    term_id: u64,
    bucket: usize,
    num_time_buckets: usize,
    count_lane: usize,
) {
    debug_assert!(
        bucket < num_time_buckets,
        "histogram bucket outside dense range"
    );
    counts[term_id as usize * num_time_buckets + bucket][count_lane] += 1;
}

/// Resolver for a histogram whose entire value range maps to one bucket. It deliberately owns no
/// block accessor: collecting this shape does not read or decode the histogram column at all.
#[derive(Debug, Default)]
struct SingleBucketResolver {
    next_count_lane: usize,
}

impl BucketResolver for SingleBucketResolver {
    #[inline]
    fn prepare_block(&mut self, _docs: &[crate::DocId]) {}

    #[inline]
    fn num_buckets(&self) -> usize {
        1
    }

    #[inline]
    fn collect_block<const LANES: usize>(
        &mut self,
        term_ids: impl Iterator<Item = u64>,
        counts: &mut [[u32; LANES]],
    ) {
        for term_id in term_ids {
            self.next_count_lane = (self.next_count_lane + 1) % LANES;
            increment_grid_count(counts, term_id, 0, 1, self.next_count_lane);
        }
    }

    fn collect_block_with_bounds<const LANES: usize>(
        &mut self,
        _term_ids: impl Iterator<Item = u64>,
        _counts: &mut [[u32; LANES]],
        _term_counts: &mut [[u32; LANES]],
    ) {
        unreachable!("SingleBucketResolver is only constructed without hard bounds");
    }
}

/// The general resolver. It preserves the existing field conversion and floating-point bucket
/// calculation for histograms that do not use a specialized resolver.
#[derive(Debug)]
struct ComputedBucketResolver {
    hist_block: ColumnBlockAccessor<u64>,
    next_count_lane: usize,
    accessor: Column<u64>,
    is_full: bool,
    field_type: ColumnType,
    interval: f64,
    offset: f64,
    base_pos: i64,
    num_buckets: usize,
    bounds: crate::aggregation::bucket::HistogramBounds,
}

impl ComputedBucketResolver {
    fn new(hist_req_data: &HistogramAggReqData, base_pos: i64, num_buckets: usize) -> Self {
        Self {
            hist_block: ColumnBlockAccessor::default(),
            next_count_lane: 0,
            accessor: hist_req_data.accessor.clone(),
            is_full: hist_req_data.accessor.get_cardinality().is_full(),
            field_type: hist_req_data.field_type,
            interval: hist_req_data.req.interval,
            offset: hist_req_data.offset,
            base_pos,
            num_buckets,
            bounds: hist_req_data.bounds,
        }
    }
}

impl BucketResolver for ComputedBucketResolver {
    #[inline]
    fn prepare_block(&mut self, docs: &[crate::DocId]) {
        self.hist_block
            .fetch_block_with_is_full(docs, &self.accessor, self.is_full);
    }

    #[inline]
    fn num_buckets(&self) -> usize {
        self.num_buckets
    }

    #[inline]
    fn collect_block<const LANES: usize>(
        &mut self,
        term_ids: impl Iterator<Item = u64>,
        counts: &mut [[u32; LANES]],
    ) {
        let field_type = self.field_type;
        let interval = self.interval;
        let offset = self.offset;
        let base_pos = self.base_pos;
        let num_buckets = self.num_buckets;
        for (term_id, hist_raw) in term_ids.zip(self.hist_block.iter_vals()) {
            let val = f64_from_fastfield_u64(hist_raw, field_type);
            let bucket = (get_bucket_pos_f64(val, interval, offset) as i64 - base_pos) as usize;
            self.next_count_lane = (self.next_count_lane + 1) % LANES;
            increment_grid_count(counts, term_id, bucket, num_buckets, self.next_count_lane);
        }
    }

    #[inline]
    fn collect_block_with_bounds<const LANES: usize>(
        &mut self,
        term_ids: impl Iterator<Item = u64>,
        counts: &mut [[u32; LANES]],
        term_counts: &mut [[u32; LANES]],
    ) {
        let field_type = self.field_type;
        let interval = self.interval;
        let offset = self.offset;
        let base_pos = self.base_pos;
        let num_buckets = self.num_buckets;
        let bounds = self.bounds;
        let mut next_count_lane = self.next_count_lane;
        for (term_id, hist_raw) in term_ids.zip(self.hist_block.iter_vals()) {
            let val = f64_from_fastfield_u64(hist_raw, field_type);
            if bounds.contains(val) {
                let bucket = (get_bucket_pos_f64(val, interval, offset) as i64 - base_pos) as usize;
                increment_grid_count(counts, term_id, bucket, num_buckets, next_count_lane);
            } else {
                term_counts[term_id as usize][next_count_lane] += 1;
            }
            next_count_lane = (next_count_lane + 1) % LANES;
        }
        self.next_count_lane = next_count_lane;
    }
}

/// Resolver for a small histogram grid. Bucket starts are precomputed in monotonic fast-field
/// `u64` space, then scanned linearly. `NUM_BUCKETS` is fixed so the optimizer can unroll the scan.
#[derive(Debug)]
struct LinearBucketResolver<const NUM_BUCKETS: usize> {
    hist_block: ColumnBlockAccessor<u64>,
    next_count_lane: usize,
    accessor: Column<u64>,
    boundaries: [u64; NUM_BUCKETS],
    num_buckets: usize,
}

impl<const NUM_BUCKETS: usize> LinearBucketResolver<NUM_BUCKETS> {
    fn new(
        hist_req_data: &HistogramAggReqData,
        base_pos: i64,
        num_time_buckets: usize,
    ) -> Option<Self> {
        assert!(num_time_buckets > 1 && num_time_buckets <= NUM_BUCKETS);
        let max_encoded_value = hist_req_data.accessor.max_value();
        // Padding must compare false for every column value. There is no such `u64` sentinel when
        // the column contains `u64::MAX`, so that edge case uses the computed resolver instead.
        let padding = max_encoded_value.checked_add(1)?;
        let mut boundaries = [padding; NUM_BUCKETS];
        let mut bucket_start = hist_req_data.accessor.min_value();
        for bucket in 1..num_time_buckets {
            bucket_start = first_encoded_value_for_bucket(
                bucket_start,
                max_encoded_value,
                bucket,
                hist_req_data,
                base_pos,
            );
            boundaries[bucket - 1] = bucket_start;
        }
        Some(Self {
            hist_block: ColumnBlockAccessor::default(),
            next_count_lane: 0,
            accessor: hist_req_data.accessor.clone(),
            boundaries,
            num_buckets: num_time_buckets,
        })
    }

    #[inline]
    fn resolve(boundaries: &[u64; NUM_BUCKETS], hist_raw: u64) -> usize {
        let mut bucket = 0;
        for &boundary in boundaries {
            bucket += (hist_raw >= boundary) as usize;
        }
        bucket
    }
}

impl<const NUM_BUCKETS: usize> BucketResolver for LinearBucketResolver<NUM_BUCKETS> {
    #[inline]
    fn prepare_block(&mut self, docs: &[crate::DocId]) {
        self.hist_block
            .fetch_block_with_is_full(docs, &self.accessor, true);
    }

    #[inline]
    fn num_buckets(&self) -> usize {
        self.num_buckets
    }

    #[inline]
    fn collect_block<const LANES: usize>(
        &mut self,
        term_ids: impl Iterator<Item = u64>,
        counts: &mut [[u32; LANES]],
    ) {
        let num_buckets = self.num_buckets;
        let boundaries = &self.boundaries;
        for (term_id, hist_raw) in term_ids.zip(self.hist_block.iter_vals()) {
            let bucket = Self::resolve(boundaries, hist_raw);
            self.next_count_lane = (self.next_count_lane + 1) % LANES;
            increment_grid_count(counts, term_id, bucket, num_buckets, self.next_count_lane);
        }
    }

    fn collect_block_with_bounds<const LANES: usize>(
        &mut self,
        _term_ids: impl Iterator<Item = u64>,
        _counts: &mut [[u32; LANES]],
        _term_counts: &mut [[u32; LANES]],
    ) {
        panic!(
            "LinearBucketResolver does not support hard bounds and should not be constructed with \
             them"
        );
    }
}

/// Finds the first monotonic fast-field value assigned to `target_bucket` or a later bucket. Doing
/// this binary search once during construction preserves the existing floating-point edge behavior
/// without paying for conversion/division during collection.
fn first_encoded_value_for_bucket(
    mut encoded_lower_bound: u64,
    mut encoded_upper_bound: u64,
    target_bucket: usize,
    hist_req_data: &HistogramAggReqData,
    base_pos: i64,
) -> u64 {
    while encoded_lower_bound < encoded_upper_bound {
        let encoded_midpoint =
            encoded_lower_bound + (encoded_upper_bound - encoded_lower_bound) / 2;
        let val = f64_from_fastfield_u64(encoded_midpoint, hist_req_data.field_type);
        let bucket = (get_bucket_pos_f64(val, hist_req_data.req.interval, hist_req_data.offset)
            as i64
            - base_pos) as usize;
        if bucket < target_bucket {
            encoded_lower_bound = encoded_midpoint + 1;
        } else {
            encoded_upper_bound = encoded_midpoint;
        }
    }
    encoded_lower_bound
}

/// Fused collector for `terms` (low cardinality) × a single `histogram`/`date_histogram` leaf with
/// nothing nested below it, when the resulting counter grid is small (see
/// [`MAX_FUSED_GRID_COUNTERS`]).
///
/// It keeps a flat, fully dense 2D counter grid
/// (`counts[term * num_time_buckets + bucket][lane]`) and a per-term total. Cycling writes through
/// independent lanes avoids a serial read/modify/write dependency when consecutive documents hit
/// the same cell. A single pass reads both the term and histogram columns in document order and
/// bumps the counters directly — no doc-id buffering, no per-term scattered re-fetch, no dynamic
/// dispatch on flush, no per-bucket key/id storage during collection (keys are derived from the
/// index at the end).
///
/// At result time the flat grid is expanded back into the regular term map + histogram storage and
/// handed to the shared intermediate-result builders, so cross-segment merging is identical to the
/// general path.
#[derive(Debug)]
struct SegmentTermHistogramCollector<R: BucketResolver, const LANES: usize> {
    /// Per-term count of docs *outside* `hard_bounds` (still in `doc_count`, but in no bucket).
    /// Per-term total = this + the term's `counts` row-sum; left empty when there are no hard
    /// bounds (every doc is in-bounds, so there's no remainder to track).
    term_counts: Vec<[u32; LANES]>,
    /// Flattened `[num_terms * num_time_buckets]` histogram counters, each split into independent
    /// count lanes.
    ///
    /// Each term id gets its own contiguous slice of `num_time_buckets` histogram counters. When
    /// we count all docs (#nofilter), we can derive the per-term total as the sum over that term's
    /// slice.
    counts: Vec<[u32; LANES]>,
    /// `bucket_pos` mapped to time-bucket index 0.
    base_pos: i64,
    terms_req_data: TermsAggReqData,
    /// The (cloned, normalized) histogram request: its column + interval/offset/bounds.
    hist_req_data: HistogramAggReqData,
    /// Private term block accessor. The bucket resolver owns a histogram block accessor when it
    /// needs one; the single-bucket resolver deliberately does not.
    term_block: ColumnBlockAccessor<u64>,
    bucket_resolver: R,
    /// No hard bounds, so every doc is in-bounds.
    all_docs_in_bounds: bool,
    /// The term column is full (a fused-path precondition); cached so `collect` skips the
    /// per-block cardinality lookup in `fetch_block`.
    term_is_full: bool,
}

impl<R: BucketResolver, const LANES: usize> SegmentAggregationCollector
    for SegmentTermHistogramCollector<R, LANES>
{
    fn add_intermediate_aggregation_result(
        &mut self,
        agg_data: &AggregationsSegmentCtx,
        results: &mut IntermediateAggregationResults,
        parent_bucket_id: BucketId,
    ) -> crate::Result<()> {
        debug_assert_eq!(
            parent_bucket_id, 0,
            "fused term-histogram collector is top-level only"
        );
        // Expand the flat grid back into the regular structures and reuse the shared builders, so
        // ordering/cut-off/dict handling and cross-segment merging match the general path exactly.
        let mut bucket_id_provider = BucketIdProvider::default();
        // Per-term total = histogram row-sum (in-bounds) + `term_counts` (out-of-bounds remainder,
        // empty when there are no hard bounds).
        let num_time_buckets = self.bucket_resolver.num_buckets();
        let term_buckets = VecTermBuckets {
            buckets: self
                .counts
                .chunks_exact(num_time_buckets)
                .enumerate()
                .map(|(term_id, row)| {
                    let in_bounds: u32 = row.iter().flat_map(|lanes| lanes.iter()).sum();
                    let out_of_bounds = self
                        .term_counts
                        .get(term_id)
                        .map(|lanes| lanes.iter().sum())
                        .unwrap_or(0);
                    Bucket {
                        count: in_bounds + out_of_bounds,
                        bucket_id: bucket_id_provider.next_bucket_id(),
                    }
                })
                .collect(),
        };
        let mut histogram = SegmentHistogramCollector::<()>::from_dense_rows(
            self.hist_req_data.clone(),
            self.base_pos,
            num_time_buckets,
            &self.counts,
        );
        let name = self.terms_req_data.name.clone();
        let bucket = SegmentTermCollector::<VecTermBuckets<BucketId>, LowCardSubAggBuffer>::into_intermediate_bucket_result(
            &self.terms_req_data,
            Some(&mut histogram as &mut dyn SegmentAggregationCollector),
            term_buckets,
            agg_data,
        )?;
        results.push(name, IntermediateAggregationResult::Bucket(bucket))?;
        Ok(())
    }

    #[inline]
    fn collect(
        &mut self,
        parent_bucket_id: BucketId,
        docs: &[crate::DocId],
        _agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        debug_assert_eq!(
            parent_bucket_id, 0,
            "fused term-histogram collector is top-level only"
        );

        // The term column is always needed. The resolver fetches the histogram column only when
        // bucket selection depends on its values; `SingleBucketResolver` makes this a no-op.
        self.term_block.fetch_block_with_is_full(
            docs,
            &self.terms_req_data.accessor,
            self.term_is_full,
        );
        self.bucket_resolver.prepare_block(docs);

        // Keep separate bounded and unbounded entry points so the common path has no bounds branch,
        // while the computed resolver can retain a direct hard-bounds loop without materializing an
        // iterator of `Option<usize>`.
        if self.all_docs_in_bounds {
            self.bucket_resolver
                .collect_block::<LANES>(self.term_block.iter_vals(), &mut self.counts);
        } else {
            self.bucket_resolver.collect_block_with_bounds::<LANES>(
                self.term_block.iter_vals(),
                &mut self.counts,
                &mut self.term_counts,
            );
        }
        Ok(())
    }

    fn flush(&mut self, _agg_data: &mut AggregationsSegmentCtx) -> crate::Result<()> {
        // Nothing is buffered: `collect` writes the flat grid directly.
        Ok(())
    }

    fn prepare_max_bucket(
        &mut self,
        _max_bucket: BucketId,
        _agg_data: &AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        // Top-level: the flat grid is allocated up front.
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

/// Builds the fused terms×histogram collector for a single top-level parent, when the shape is
/// eligible. Returns `Ok(None)` to fall back to the general buffered terms path.
///
/// Eligibility: top-level, low-cardinality terms over a full column with no missing/include-exclude
/// handling; a single `histogram`/`date_histogram` leaf (no nesting below it) over a full column;
/// and a physical counter grid no larger than [`MAX_FUSED_GRID_COUNTERS`].
pub(super) fn maybe_build_collector(
    agg_data: &mut AggregationsSegmentCtx,
    node: &AggRefNode,
    terms_req_data: &TermsAggReqData,
    col_max_val: u64,
    is_top_level: bool,
) -> crate::Result<Option<Box<dyn SegmentAggregationCollector>>> {
    // Both columns must be full (one value per doc) so their values align positionally with `docs`
    // and we can zip them. Requiring full columns also makes the terms agg's `missing` config a
    // no-op (`fetch_block_with_missing` early-returns on full columns), so we needn't check for it.
    //
    // We don't cap the term cardinality here: the flat grid is bounded by the total physical
    // counter count (`num_terms * num_time_buckets * LANES <= MAX_FUSED_GRID_COUNTERS`) checked
    // below, which subsumes it.
    //
    // We only allow this at the top-level, since we don't know how many buckets are created. We
    // are less likely to get enough docs for the preallocation to be worth and there's a risk of
    // using too much memory. We could check the maximum theoretical buckets up-front and pass
    // them down.
    let fuseable = is_top_level
        // TODO: We can easily support this
        && terms_req_data.allowed_term_ids.is_none()
        && terms_req_data.accessor.get_cardinality().is_full()
        // The flat counters are `u32`, bumped once per value, so no count can exceed the column's
        // value count. (Essentially always true here: the column is full, so its value count
        // equals the doc count, and `DocId` is `u32`.)
        && terms_req_data.accessor.values.num_vals() < u32::MAX
        && node.children.len() == 1
        && matches!(
            node.children[0].kind,
            AggKind::Histogram | AggKind::DateHistogram
        )
        && node.children[0].children.is_empty()
        && agg_data.per_request.histogram_req_data[node.children[0].idx_in_req_data]
            .accessor
            .get_cardinality()
            .is_full();
    if !fuseable {
        return Ok(None);
    }

    // Clone + normalize the histogram request and get its dense bucket range; only take the fused
    // path when the physical counter grid is small enough. Very small logical grids use multiple
    // counters per cell; larger grids retain scalar cells to avoid paying for lanes when writes are
    // already spread across many locations.
    let Some((hist_req_data, range)) = prepare_histogram_dense_range(agg_data, &node.children[0])?
    else {
        return Ok(None);
    };
    let num_terms = col_max_val.saturating_add(1) as usize;
    let num_grid_cells = num_terms.saturating_mul(range.len);
    let use_count_lanes = num_grid_cells <= MAX_NUM_BUCKETS_FOR_COUNT_LANES;
    let num_count_lanes = if use_count_lanes {
        NUM_LOW_CARD_COUNT_LANES
    } else {
        SINGLE_COUNT_LANE
    };
    if num_grid_cells.saturating_mul(num_count_lanes) > MAX_FUSED_GRID_COUNTERS {
        return Ok(None);
    }

    let collector = if use_count_lanes {
        build_collector::<NUM_LOW_CARD_COUNT_LANES>(
            agg_data,
            terms_req_data,
            hist_req_data,
            num_terms,
            range.len,
            range.base_pos,
        )?
    } else {
        build_collector::<SINGLE_COUNT_LANE>(
            agg_data,
            terms_req_data,
            hist_req_data,
            num_terms,
            range.len,
            range.base_pos,
        )?
    };
    Ok(Some(collector))
}

fn build_collector<const LANES: usize>(
    agg_data: &mut AggregationsSegmentCtx,
    terms_req_data: &TermsAggReqData,
    hist_req_data: HistogramAggReqData,
    num_terms: usize,
    num_time_buckets: usize,
    base_pos: i64,
) -> crate::Result<Box<dyn SegmentAggregationCollector>> {
    const { assert!(LANES > 0, "a fused grid needs at least one count lane") };

    let all_docs_in_bounds =
        hist_req_data.bounds.min == f64::MIN && hist_req_data.bounds.max == f64::MAX;
    if all_docs_in_bounds && num_time_buckets == 1 {
        return build_collector_with_resolver::<SingleBucketResolver, LANES>(
            agg_data,
            terms_req_data,
            hist_req_data,
            num_terms,
            base_pos,
            SingleBucketResolver::default(),
        );
    }
    if all_docs_in_bounds && num_time_buckets <= NUM_SMALL_LINEAR_BUCKETS {
        if let Some(resolver) = LinearBucketResolver::<NUM_SMALL_LINEAR_BUCKETS>::new(
            &hist_req_data,
            base_pos,
            num_time_buckets,
        ) {
            return build_collector_with_resolver::<_, LANES>(
                agg_data,
                terms_req_data,
                hist_req_data,
                num_terms,
                base_pos,
                resolver,
            );
        }
    } else if all_docs_in_bounds && num_time_buckets <= NUM_LARGE_LINEAR_BUCKETS {
        if let Some(resolver) = LinearBucketResolver::<NUM_LARGE_LINEAR_BUCKETS>::new(
            &hist_req_data,
            base_pos,
            num_time_buckets,
        ) {
            return build_collector_with_resolver::<_, LANES>(
                agg_data,
                terms_req_data,
                hist_req_data,
                num_terms,
                base_pos,
                resolver,
            );
        }
    }

    let resolver = ComputedBucketResolver::new(&hist_req_data, base_pos, num_time_buckets);
    build_collector_with_resolver::<_, LANES>(
        agg_data,
        terms_req_data,
        hist_req_data,
        num_terms,
        base_pos,
        resolver,
    )
}

fn build_collector_with_resolver<R: BucketResolver, const LANES: usize>(
    agg_data: &mut AggregationsSegmentCtx,
    terms_req_data: &TermsAggReqData,
    hist_req_data: HistogramAggReqData,
    num_terms: usize,
    base_pos: i64,
    bucket_resolver: R,
) -> crate::Result<Box<dyn SegmentAggregationCollector>> {
    let num_time_buckets = bucket_resolver.num_buckets();
    // No hard bounds means every doc is in-bounds, letting `collect` use its branch-free loop and
    // leaving `term_counts` (the out-of-bounds remainder) unused.
    let all_docs_in_bounds =
        hist_req_data.bounds.min == f64::MIN && hist_req_data.bounds.max == f64::MAX;
    let counts = vec![[0u32; LANES]; num_terms * num_time_buckets];
    let term_counts = if all_docs_in_bounds {
        Vec::new()
    } else {
        vec![[0u32; LANES]; num_terms]
    };
    // Charge both grids, including replicated lanes, to the aggregation memory limit.
    let memory_consumption =
        std::mem::size_of_val(counts.as_slice()) + std::mem::size_of_val(term_counts.as_slice());
    agg_data
        .context
        .limits
        .add_memory_consumed(memory_consumption as u64)?;

    Ok(Box::new(SegmentTermHistogramCollector::<R, LANES> {
        term_counts,
        counts,
        base_pos,
        terms_req_data: terms_req_data.clone(),
        hist_req_data,
        term_block: ColumnBlockAccessor::default(),
        bucket_resolver,
        all_docs_in_bounds,
        term_is_full: terms_req_data.accessor.get_cardinality().is_full(),
    }))
}

#[cfg(test)]
mod tests {
    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::tests::{
        exec_request, exec_request_with_query_and_memory_limit,
        get_test_index_from_values_and_terms,
    };
    use crate::aggregation::AggregationLimitsGuard;

    /// Hand-computed correctness check for the fused terms×histogram fast path
    /// ([`super::SegmentTermHistogramCollector`]): low-cardinality terms × a histogram leaf over
    /// full columns, exercised single- and multi-segment.
    #[test]
    fn fused_term_histogram_test() -> crate::Result<()> {
        fused_term_histogram_with_opt(false)?;
        fused_term_histogram_with_opt(true)?;
        Ok(())
    }

    fn fused_term_histogram_with_opt(merge_segments: bool) -> crate::Result<()> {
        // 300 docs: term = {a, b, c} by i % 3, histogram value = i % 20 (interval 1 => buckets
        // 0..19). gcd(3, 20) = 1, so every (term, bucket) pair occurs exactly 300 / 60 = 5 times.
        let docs: Vec<(f64, String)> = (0..300u64)
            .map(|i| {
                (
                    (i % 20) as f64,
                    ["a", "b", "c"][(i % 3) as usize].to_string(),
                )
            })
            .collect();
        // Two segments, to also exercise cross-segment merging of the fused per-term histograms.
        let segments = vec![docs[..150].to_vec(), docs[150..].to_vec()];
        let index = get_test_index_from_values_and_terms(merge_segments, &segments)?;

        let agg_req: Aggregations = serde_json::from_value(serde_json::json!({
            "by_term": {
                "terms": { "field": "string_id", "order": { "_key": "asc" } },
                "aggs": {
                    "histo": { "histogram": { "field": "score_f64", "interval": 1.0 } }
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;

        for (term_idx, term) in ["a", "b", "c"].iter().enumerate() {
            assert_eq!(res["by_term"]["buckets"][term_idx]["key"], *term);
            assert_eq!(res["by_term"]["buckets"][term_idx]["doc_count"], 100);
            let histo = &res["by_term"]["buckets"][term_idx]["histo"]["buckets"];
            for b in 0..20usize {
                assert_eq!(histo[b]["key"], b as f64, "term {term} bucket {b}");
                assert_eq!(histo[b]["doc_count"], 5, "term {term} bucket {b}");
            }
            assert_eq!(histo[20], serde_json::Value::Null);
        }
        assert_eq!(res["by_term"]["buckets"][3], serde_json::Value::Null);

        Ok(())
    }

    /// A histogram whose values all map to one bucket uses the resolver that counts terms without
    /// reading the histogram column.
    #[test]
    fn fused_term_histogram_single_bucket_resolver() -> crate::Result<()> {
        // The ten distinct values all map to bucket 0 at interval 10. With three terms coprime to
        // the value count, each term occurs 30 times across the 90 documents.
        let docs: Vec<(f64, String)> = (0..90usize)
            .map(|i| ((i % 10) as f64, ["a", "b", "c"][i % 3].to_string()))
            .collect();
        let index = get_test_index_from_values_and_terms(true, &[docs])?;
        let agg_req: Aggregations = serde_json::from_value(serde_json::json!({
            "by_term": {
                "terms": { "field": "string_id", "order": { "_key": "asc" } },
                "aggs": {
                    "histo": { "histogram": { "field": "score_f64", "interval": 10.0 } }
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        for (term_idx, term) in ["a", "b", "c"].iter().enumerate() {
            let term_bucket = &res["by_term"]["buckets"][term_idx];
            assert_eq!(term_bucket["key"], *term);
            assert_eq!(term_bucket["doc_count"], 30);
            let histo = &term_bucket["histo"]["buckets"];
            assert_eq!(histo[0]["key"], 0.0);
            assert_eq!(histo[0]["doc_count"], 30);
            assert_eq!(histo[1], serde_json::Value::Null);
        }

        Ok(())
    }

    /// Four and eight histogram buckets take their corresponding fixed-size linear resolvers.
    /// Negative values also exercise monotonic `f64` fast-field boundaries on both sides of zero.
    #[test]
    fn fused_term_histogram_linear_bucket_resolver() -> crate::Result<()> {
        for num_buckets in [4usize, 8] {
            // Three terms are coprime with both bucket counts, so every pair occurs 10 times.
            let docs: Vec<(f64, String)> = (0..3 * num_buckets * 10)
                .map(|i| {
                    (
                        (i % num_buckets) as f64 - (num_buckets / 2) as f64,
                        ["a", "b", "c"][i % 3].to_string(),
                    )
                })
                .collect();
            let index = get_test_index_from_values_and_terms(true, &[docs])?;
            let agg_req: Aggregations = serde_json::from_value(serde_json::json!({
                "by_term": {
                    "terms": { "field": "string_id", "order": { "_key": "asc" } },
                    "aggs": {
                        "histo": { "histogram": { "field": "score_f64", "interval": 1.0 } }
                    }
                }
            }))
            .unwrap();

            let res = exec_request(agg_req, &index)?;
            for (term_idx, term) in ["a", "b", "c"].iter().enumerate() {
                assert_eq!(res["by_term"]["buckets"][term_idx]["key"], *term);
                assert_eq!(
                    res["by_term"]["buckets"][term_idx]["doc_count"],
                    num_buckets * 10
                );
                let histo = &res["by_term"]["buckets"][term_idx]["histo"]["buckets"];
                for bucket in 0..num_buckets {
                    assert_eq!(
                        histo[bucket]["key"],
                        bucket as f64 - (num_buckets / 2) as f64
                    );
                    assert_eq!(histo[bucket]["doc_count"], 10);
                }
            }
        }
        Ok(())
    }

    /// A `missing` config on a *full* term column still takes the fused path (the string sentinel
    /// is just `col_max + 1`, so the column stays low-cardinality). Since no doc is missing, the
    /// real term buckets must be exactly as without `missing`.
    #[test]
    fn fused_term_histogram_with_missing_on_full_column() -> crate::Result<()> {
        let docs: Vec<(f64, String)> = (0..300u64)
            .map(|i| {
                (
                    (i % 20) as f64,
                    ["a", "b", "c"][(i % 3) as usize].to_string(),
                )
            })
            .collect();
        let index = get_test_index_from_values_and_terms(true, &[docs])?;

        let agg_req: Aggregations = serde_json::from_value(serde_json::json!({
            "by_term": {
                "terms": { "field": "string_id", "missing": "MISSING", "order": { "_key": "asc" } },
                "aggs": {
                    "histo": { "histogram": { "field": "score_f64", "interval": 1.0 } }
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;

        // Column is full, so "MISSING" never applies: a, b, c are unchanged (100 docs, 5 per
        // bucket).
        for (term_idx, term) in ["a", "b", "c"].iter().enumerate() {
            assert_eq!(res["by_term"]["buckets"][term_idx]["key"], *term);
            assert_eq!(res["by_term"]["buckets"][term_idx]["doc_count"], 100);
            let histo = &res["by_term"]["buckets"][term_idx]["histo"]["buckets"];
            for b in 0..20usize {
                assert_eq!(histo[b]["doc_count"], 5, "term {term} bucket {b}");
            }
        }

        Ok(())
    }

    /// Term cardinality is not what gates fusing: the flat grid is bounded by the total cell count
    /// (`num_terms * num_time_buckets`), not the term count, so many terms still fuse.
    #[test]
    fn fused_term_histogram_many_terms() -> crate::Result<()> {
        let num_terms = 150usize;
        let docs_per_term = 2usize;
        // All docs share histogram value 0 (a single bucket), so the grid is 150 x 1 = 150 cells.
        let docs: Vec<(f64, String)> = (0..num_terms * docs_per_term)
            .map(|i| (0.0, format!("t{:03}", i % num_terms)))
            .collect();
        let index = get_test_index_from_values_and_terms(true, &[docs])?;

        let agg_req: Aggregations = serde_json::from_value(serde_json::json!({
            "by_term": {
                "terms": { "field": "string_id", "size": 1000, "order": { "_key": "asc" } },
                "aggs": {
                    "histo": { "histogram": { "field": "score_f64", "interval": 1.0 } }
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;

        let buckets = res["by_term"]["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), num_terms);
        for (i, bucket) in buckets.iter().enumerate() {
            assert_eq!(bucket["key"], format!("t{i:03}"));
            assert_eq!(bucket["doc_count"], docs_per_term as u64);
            assert_eq!(bucket["histo"]["buckets"][0]["key"], 0.0);
            assert_eq!(
                bucket["histo"]["buckets"][0]["doc_count"],
                docs_per_term as u64
            );
        }

        Ok(())
    }

    /// `hard_bounds` exercises the non-derived `term_counts` branch: a term's `doc_count` must
    /// count *every* doc with that term, including docs whose histogram value is outside the
    /// bounds (those are excluded from the histogram buckets but still counted for the term). This
    /// is the case where the per-doc `term_counts` increment cannot be replaced by the grid
    /// row-sum.
    #[test]
    fn fused_term_histogram_with_hard_bounds() -> crate::Result<()> {
        // 300 docs: term = {a, b, c} by i % 3, value = i % 20. Per term: 100 docs, each value in
        // 0..=19 occurring 5 times.
        let docs: Vec<(f64, String)> = (0..300u64)
            .map(|i| {
                (
                    (i % 20) as f64,
                    ["a", "b", "c"][(i % 3) as usize].to_string(),
                )
            })
            .collect();
        let index = get_test_index_from_values_and_terms(true, &[docs])?;

        // hard_bounds [5, 14] (inclusive) keeps only values 5..=14 in the histogram (10 buckets);
        // values 0..=4 and 15..=19 are out of bounds.
        let agg_req: Aggregations = serde_json::from_value(serde_json::json!({
            "by_term": {
                "terms": { "field": "string_id", "order": { "_key": "asc" } },
                "aggs": {
                    "histo": {
                        "histogram": {
                            "field": "score_f64",
                            "interval": 1.0,
                            "hard_bounds": { "min": 5.0, "max": 14.0 }
                        }
                    }
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;

        for (term_idx, term) in ["a", "b", "c"].iter().enumerate() {
            assert_eq!(res["by_term"]["buckets"][term_idx]["key"], *term);
            // doc_count includes the 50 per-term docs whose value is outside [5, 14].
            assert_eq!(res["by_term"]["buckets"][term_idx]["doc_count"], 100);
            let histo = &res["by_term"]["buckets"][term_idx]["histo"]["buckets"];
            for b in 0..10usize {
                let key = 5 + b;
                assert_eq!(histo[b]["key"], key as f64, "term {term} bucket key {key}");
                assert_eq!(histo[b]["doc_count"], 5, "term {term} bucket {key}");
            }
            // Only the 10 in-bounds buckets exist.
            assert_eq!(histo[10], serde_json::Value::Null);
        }

        Ok(())
    }

    /// Non-binding `hard_bounds` (wider than the data, with mid-interval edges) must still produce
    /// exact results via the derive-from-grid path: since no doc is out of bounds, normalization
    /// drops the bound, every doc lands in the dense range, and each term's total equals its
    /// histogram row-sum. This is the case that previously fell back to the per-doc counter only
    /// because `bounds != [MIN, MAX]`.
    #[test]
    fn fused_term_histogram_with_non_binding_hard_bounds() -> crate::Result<()> {
        // 300 docs: term = {a, b, c} by i % 3, value = i % 20. Data values span [0, 19].
        let docs: Vec<(f64, String)> = (0..300u64)
            .map(|i| {
                (
                    (i % 20) as f64,
                    ["a", "b", "c"][(i % 3) as usize].to_string(),
                )
            })
            .collect();
        let index = get_test_index_from_values_and_terms(true, &[docs])?;

        // Bounds wider than [0, 19], with mid-interval edges -> they exclude nothing.
        let agg_req: Aggregations = serde_json::from_value(serde_json::json!({
            "by_term": {
                "terms": { "field": "string_id", "order": { "_key": "asc" } },
                "aggs": {
                    "histo": {
                        "histogram": {
                            "field": "score_f64",
                            "interval": 1.0,
                            "hard_bounds": { "min": -0.5, "max": 19.5 }
                        }
                    }
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;

        for (term_idx, term) in ["a", "b", "c"].iter().enumerate() {
            assert_eq!(res["by_term"]["buckets"][term_idx]["key"], *term);
            // Every doc is in-bounds, so the per-term total is the full 100 (as without bounds).
            assert_eq!(res["by_term"]["buckets"][term_idx]["doc_count"], 100);
            let histo = &res["by_term"]["buckets"][term_idx]["histo"]["buckets"];
            for b in 0..20usize {
                assert_eq!(histo[b]["key"], b as f64, "term {term} bucket {b}");
                assert_eq!(histo[b]["doc_count"], 5, "term {term} bucket {b}");
            }
            assert_eq!(histo[20], serde_json::Value::Null);
        }

        Ok(())
    }

    /// Regression: with hard bounds the fused path allocates `term_counts` (one `u32`/term) on top
    /// of the grid, and that allocation must be charged to the memory limit. With many terms and a
    /// single time bucket the two are equal in size, so a limit admitting the grid alone but not
    /// grid + `term_counts` must fail.
    #[test]
    fn fused_term_histogram_hard_bounds_charges_term_counts() -> crate::Result<()> {
        // 16k distinct terms, one doc each; values alternate in/out of the single-bucket bounds
        // [5, 5] so the bounds bind and `term_counts` is allocated. num_terms=16000,
        // num_time_buckets=1 => `counts` and `term_counts` are ~64 KB each.
        let docs: Vec<(f64, String)> = (0..16_000u64)
            .map(|i| (if i % 2 == 0 { 5.0 } else { 10.0 }, format!("t{i:05}")))
            .collect();
        let index = get_test_index_from_values_and_terms(true, &[docs])?;

        let agg_req: Aggregations = serde_json::from_value(serde_json::json!({
            "by_term": {
                "terms": { "field": "string_id" },
                "aggs": {
                    "histo": {
                        "histogram": {
                            "field": "score_f64",
                            "interval": 1.0,
                            "hard_bounds": { "min": 5.0, "max": 5.0 }
                        }
                    }
                }
            }
        }))
        .unwrap();

        // ~96 KB admits the grid (~64 KB) but not grid + `term_counts` (~128 KB).
        let err = exec_request_with_query_and_memory_limit(
            agg_req,
            &index,
            None,
            AggregationLimitsGuard::new(Some(96_000), None),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("memory limit was exceeded"),
            "expected a memory-limit error, got: {err}"
        );

        Ok(())
    }
}
