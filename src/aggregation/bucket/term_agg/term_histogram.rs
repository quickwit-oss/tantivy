//! Fused collector for the very common shape `terms` (low cardinality) × a single
//! `histogram`/`date_histogram` sub-aggregation with nothing nested below it.
//!
//! See [`SegmentTermHistogramCollector`] for the approach and [`maybe_build_collector`] for the
//! conditions under which it is used.

use columnar::ColumnBlockAccessor;

use super::{Bucket, SegmentTermCollector, TermsAggReqData, VecTermBuckets};
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

/// Maximum number of cells (`num_terms × num_time_buckets`) in the fused flat 2D grid. Above this
/// the grid would be too large/cache-unfriendly, so we fall back to the general buffered path.
/// `1 << 14` cells = 128 KB of `u64` counters, comfortably L2-resident.
///
/// Since we are only at the top-level, this won't be multiplied by any parent buckets.
const MAX_FUSED_GRID_BUCKETS: usize = 16384;

/// Fused collector for `terms` (low cardinality) × a single `histogram`/`date_histogram` leaf with
/// nothing nested below it, when the resulting `num_terms × num_time_buckets` grid is small (see
/// [`MAX_FUSED_GRID_BUCKETS`]).
///
/// It keeps a flat, fully dense 2D counter grid (`counts[term * num_time_buckets + bucket]`) and a
/// per-term total. A single pass reads both the term and histogram columns in document order and
/// bumps the counters directly — no doc-id buffering, no per-term scattered re-fetch, no dynamic
/// dispatch on flush, no per-bucket key/id storage during collection (keys are derived from the
/// index at the end).
///
/// At result time the flat grid is expanded back into the regular term map + histogram storage and
/// handed to the shared intermediate-result builders, so cross-segment merging is identical to the
/// general path.
#[derive(Debug)]
pub(crate) struct SegmentTermHistogramCollector {
    /// Per-term count of docs *outside* `hard_bounds` (still in `doc_count`, but in no bucket).
    /// Per-term total = this + the term's `counts` row-sum; left empty when there are no hard
    /// bounds (every doc is in-bounds, so there's no remainder to track).
    term_counts: Vec<u32>,
    /// Flattened `[num_terms * num_time_buckets]` histogram counters (`u32`, see
    /// `term_counts`).
    ///
    /// Each term id get its own contiguous slice of `num_time_buckets` histogram counter.
    /// When we count all docs (#nofilter), we can derive the per-term total as the sum over that
    /// term's slice.
    counts: Vec<u32>,
    /// Histogram buckets per term (the dense time-range length).
    num_time_buckets: usize,
    /// `bucket_pos` mapped to time-bucket index 0.
    base_pos: i64,
    terms_req_data: TermsAggReqData,
    /// The (cloned, normalized) histogram request: its column + interval/offset/bounds.
    hist_req_data: HistogramAggReqData,
    /// Private block accessors for both columns. We read them together, so each needs its own
    /// (the shared `agg_data` scratch accessor only holds one block at a time). Owning them keeps
    /// `collect` independent of `agg_data`.
    term_block: ColumnBlockAccessor<u64>,
    hist_block: ColumnBlockAccessor<u64>,
    /// No hard bounds, so every doc is in-bounds.
    all_docs_in_bounds: bool,
    /// Both columns are full (fused-path precondition); cached so `collect` skips the per-block
    /// cardinality lookup in `fetch_block`.
    is_full: bool,
}

impl SegmentAggregationCollector for SegmentTermHistogramCollector {
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
        let term_buckets = VecTermBuckets {
            buckets: self
                .counts
                .chunks_exact(self.num_time_buckets)
                .enumerate()
                .map(|(term_id, row)| {
                    let in_bounds: u32 = row.iter().sum();
                    let out_of_bounds = self.term_counts.get(term_id).copied().unwrap_or(0);
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
            self.num_time_buckets,
            &self.counts,
        );
        let name = self.terms_req_data.name.clone();
        let bucket = SegmentTermCollector::<VecTermBuckets, LowCardSubAggBuffer>::into_intermediate_bucket_result(
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

        // Fetch both columns into our own accessors (we read them together, so they can't share the
        // single `agg_data` scratch accessor). The collector owns all its inputs, so `collect`
        // doesn't touch `agg_data`.
        self.term_block
            .fetch_block_with_is_full(docs, &self.terms_req_data.accessor, self.is_full);
        self.hist_block
            .fetch_block_with_is_full(docs, &self.hist_req_data.accessor, self.is_full);

        // Hoist the loop-invariant fields into locals: the optimizer can't prove the
        // `self.counts`/`self.term_counts` writes don't alias these `self` fields, so it can't keep
        // them in registers and re-reads them from memory every iteration — ~15% slower on
        // `terms_status_with_date_histogram` when read straight from `self`.
        // Note: check which are actually relevant.
        let field_type = self.hist_req_data.field_type;
        let bounds = self.hist_req_data.bounds;
        let interval = self.hist_req_data.req.interval;
        let offset = self.hist_req_data.offset;
        let base_pos = self.base_pos;
        let num_time_buckets = self.num_time_buckets;
        let all_docs_in_bounds = self.all_docs_in_bounds;
        let term_counts = &mut self.term_counts;
        let counts = &mut self.counts;

        // Both columns are full (checked at construction), so values align with `docs` positionally
        // and are read together in one pass.
        // In-bounds docs bump the `counts` grid, out-of-bounds bump `term_counts`; deriving the
        // total at flush avoids a per-doc `term_counts` RMW that serializes on
        // store-to-load forwarding.
        for (term_id, hist_raw) in self.term_block.iter_vals().zip(self.hist_block.iter_vals()) {
            let term_id = term_id as usize;
            let val = f64_from_fastfield_u64(hist_raw, field_type);
            if all_docs_in_bounds || bounds.contains(val) {
                let bucket = (get_bucket_pos_f64(val, interval, offset) as i64 - base_pos) as usize;
                debug_assert!(
                    bucket < num_time_buckets,
                    "histogram bucket outside dense range"
                );
                counts[term_id * num_time_buckets + bucket] += 1;
            } else {
                term_counts[term_id] += 1;
            }
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
/// and a `num_terms × num_time_buckets` grid no larger than [`MAX_FUSED_GRID_BUCKETS`].
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
    // We don't cap the term cardinality here: the flat grid is bounded by the total cell count
    // (`num_terms * num_time_buckets <= MAX_FUSED_GRID_BUCKETS`) checked below, which subsumes it.
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
    // path when the flat `num_terms × num_time_buckets` grid is small enough.
    let Some((hist_req_data, range)) = prepare_histogram_dense_range(agg_data, &node.children[0])?
    else {
        return Ok(None);
    };
    let num_terms = col_max_val.saturating_add(1) as usize;
    if num_terms.saturating_mul(range.len) > MAX_FUSED_GRID_BUCKETS {
        return Ok(None);
    }

    // No hard bounds means every doc is in-bounds, letting `collect` short-circuit the bounds
    // check — and leaving `term_counts` (the out-of-bounds remainder) unused, so we skip allocating
    // it.
    let all_docs_in_bounds =
        hist_req_data.bounds.min == f64::MIN && hist_req_data.bounds.max == f64::MAX;
    let counts = vec![0u32; num_terms * range.len];
    let term_counts = if all_docs_in_bounds {
        Vec::new()
    } else {
        vec![0u32; num_terms]
    };
    // Charge both grids to the aggregation memory limit.
    agg_data.context.limits.add_memory_consumed(
        ((counts.len() + term_counts.len()) * std::mem::size_of::<u32>()) as u64,
    )?;
    Ok(Some(Box::new(SegmentTermHistogramCollector {
        term_counts,
        counts,
        num_time_buckets: range.len,
        base_pos: range.base_pos,
        terms_req_data: terms_req_data.clone(),
        hist_req_data,
        term_block: ColumnBlockAccessor::default(),
        hist_block: ColumnBlockAccessor::default(),
        all_docs_in_bounds,
        is_full: terms_req_data.accessor.get_cardinality().is_full(),
    })))
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

    /// Term cardinality above the general path's `MAX_NUM_TERMS_FOR_VEC` (100) still fuses: the
    /// flat grid is bounded by the total cell count (`num_terms * num_time_buckets`), not the
    /// term count.
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
