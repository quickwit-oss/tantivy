//! Segment collector for `cardinality` over a `ColumnType::Str` column.
//!
//! Strings are dictionary encoded, and resolving a term ordinal to its bytes
//! is the expensive part. So instead of hashing values during collection, the
//! collector accumulates *term ordinals* per bucket and, at finalization,
//! builds one shared term_ord -> coupon cache for every bucket at once. The
//! coupons are then appended into a [`CardinalityCollector`], which is the
//! same type the numeric collector produces — merging across segments and
//! across column kinds stays in the parent module.

use std::fmt::Debug;
use std::io;

use columnar::{Column, ColumnType, Dictionary};
use datasketches::hll::Coupon;
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};

use super::term_ord_accumulator::TermOrdAccumulator;
use super::CardinalityCollector;
use crate::aggregation::agg_data::AggregationsSegmentCtx;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateMetricResult,
};
use crate::aggregation::segment_agg_result::SegmentAggregationCollector;
use crate::aggregation::*;

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
            // We don't really care about the value here. We will populate all the values we will
            // read anyway.
            let uninitialized_coupon = Coupon::from_hash(0);
            let mut coupon_map: Vec<Coupon> =
                vec![uninitialized_coupon; highest_term_ord as usize + 1];

            for (term_ord, coupon) in term_ords.into_iter().zip(coupons) {
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

/// Segment collector for `cardinality` over a `ColumnType::Str` column.
///
/// Hidden contract: the column passed at construction must be a str column
/// whose values are term ordinals of the associated dictionary, and
/// `max_term_ord_inclusive` must be `accessor.max_value()`. The missing
/// sentinel `accessor.max_value() + 1` may additionally be inserted when
/// `missing_value_for_accessor` is set, hence accumulators size for
/// `max_term_ord_inclusive + 1`.
pub(crate) struct SegmentStrCardinalityCollector<S: TermOrdAccumulator> {
    /// Buckets are Some(_) until they get consumed by
    /// `add_intermediate_aggregation_result`.
    buckets: Vec<Option<S>>,
    accessor_idx: usize,
    /// The column accessor to access the fast field values (term ordinals).
    accessor: Column<u64>,
    /// The missing value normalized to the internal u64 representation of the field type.
    missing_value_for_accessor: Option<u64>,
    /// Lazily built at finalization time, shared by every bucket.
    coupon_cache: Option<CouponCache>,
    /// Largest term_ord that may be inserted into a bucket, i.e.
    /// `accessor.max_value()`.
    max_term_ord_inclusive: u64,
}

impl<S: TermOrdAccumulator> Debug for SegmentStrCardinalityCollector<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("SegmentStrCardinalityCollector")
            .field("num_buckets", &self.buckets.len())
            .field(
                "missing_value_for_accessor",
                &self.missing_value_for_accessor,
            )
            .finish()
    }
}

/// Builds a coupon cache from the given buckets, dictionary, and optional missing value.
/// Returns a mapping from term_ord to the hash (coupon) of the associated term.
fn build_coupon_cache<S: TermOrdAccumulator>(
    buckets: &[Option<S>],
    dictionary: &Dictionary,
    missing_value_opt: Option<&Key>,
) -> io::Result<CouponCache> {
    // Pass 1 computes the capacity hint, pass 2 inserts.
    let mut max_bucket_len = 0usize;
    for bucket in buckets.iter().flatten() {
        max_bucket_len = max_bucket_len.max(bucket.len());
    }
    let mut term_ords_set = FxHashSet::with_capacity_and_hasher(max_bucket_len * 2, FxBuildHasher);
    for bucket in buckets.iter().flatten() {
        term_ords_set.extend(bucket.iter_ords());
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

fn append_to_sketch(
    term_ords: &impl TermOrdAccumulator,
    coupon_cache: &CouponCache,
    sketch: &mut CardinalityCollector,
) {
    match coupon_cache {
        CouponCache::Dense {
            coupon_map,
            missing_coupon_opt,
        } => {
            if let Some(missing_coupon) = missing_coupon_opt {
                for term_ord in term_ords.iter_ords() {
                    let coupon: Coupon = coupon_map
                        .get(term_ord as usize)
                        .copied()
                        .unwrap_or(*missing_coupon);
                    sketch.insert_coupon(coupon);
                }
            } else {
                for term_ord in term_ords.iter_ords() {
                    if let Some(coupon) = coupon_map.get(term_ord as usize).copied() {
                        sketch.insert_coupon(coupon);
                    }
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

impl<S: TermOrdAccumulator> SegmentStrCardinalityCollector<S> {
    pub fn from_req(
        accessor_idx: usize,
        accessor: Column<u64>,
        missing_value_for_accessor: Option<u64>,
        max_term_ord_inclusive: u64,
    ) -> Self {
        Self {
            buckets: Vec::new(),
            accessor_idx,
            accessor,
            missing_value_for_accessor,
            coupon_cache: None,
            max_term_ord_inclusive,
        }
    }
}

impl<S: TermOrdAccumulator + 'static> SegmentAggregationCollector
    for SegmentStrCardinalityCollector<S>
{
    fn add_intermediate_aggregation_result(
        &mut self,
        agg_data: &AggregationsSegmentCtx,
        results: &mut IntermediateAggregationResults,
        bucket_id: BucketId,
    ) -> crate::Result<()> {
        self.prepare_max_bucket(bucket_id, agg_data)?;
        let req_data = &agg_data.get_cardinality_req_data(self.accessor_idx);
        let Some(str_dict_column) = &req_data.str_dict_column else {
            return Err(crate::TantivyError::InternalError(
                "a str cardinality collector requires a str dictionary column".to_string(),
            ));
        };
        // Strings are dictionary encoded. Fetching the terms associated to strings
        // is expensive. For this reason, we do that once for all buckets and cache the results
        // here.
        //
        // The cache maps a term_ord to the hash of the associated term. The missing value
        // sentinel will be associated to the hash of the missing value if any.
        if self.coupon_cache.is_none() {
            self.coupon_cache = Some(build_coupon_cache(
                &self.buckets,
                str_dict_column.dictionary(),
                req_data.req.missing.as_ref(),
            )?);
        }
        let name = req_data.name.to_string();
        // take the bucket in buckets and replace it with a new empty one
        let Some(term_ords) = self.buckets[bucket_id as usize].take() else {
            return Err(crate::TantivyError::InternalError(
                "the same bucket should not be finalized twice.".to_string(),
            ));
        };
        let mut cardinality = CardinalityCollector::new(ColumnType::Str as u8);
        if let Some(coupon_cache) = self.coupon_cache.as_ref() {
            append_to_sketch(&term_ords, coupon_cache, &mut cardinality);
        }
        results.push(
            name,
            IntermediateAggregationResult::Metric(IntermediateMetricResult::Cardinality(
                cardinality,
            )),
        )?;

        Ok(())
    }

    fn collect(
        &mut self,
        parent_bucket_id: BucketId,
        docs: &[crate::DocId],
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        agg_data.column_block_accessor.fetch_block_with_missing(
            docs,
            &self.accessor,
            self.missing_value_for_accessor,
        );
        let Some(term_ords) = self.buckets[parent_bucket_id as usize].as_mut() else {
            return Err(crate::TantivyError::InternalError(
                "collection should not happen after finalization".to_string(),
            ));
        };
        // Promotion check runs on the pre-block state: the first call
        // sees an empty set (no-op), and the last block of inserts
        // doesn't trigger a promotion of a set we won't grow further.
        // The trait dispatches once per block (via `extend_from_iter`)
        // for adaptive variants and inlines to a tight loop for the
        // BitSet path.
        term_ords.maybe_compact();
        term_ords.extend_from_iter(agg_data.column_block_accessor.iter_vals());
        Ok(())
    }

    fn prepare_max_bucket(
        &mut self,
        max_bucket: BucketId,
        _agg_data: &AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        if max_bucket as usize >= self.buckets.len() {
            let max_term_ord_inclusive = self.max_term_ord_inclusive;
            self.buckets.resize_with(max_bucket as usize + 1, || {
                Some(S::new(max_term_ord_inclusive))
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
        // The sketch isn't built until finalization; the term_ord set's len is
        // the exact distinct count.
        let term_ords = self.buckets.get(bucket_id as usize)?.as_ref()?;
        Some(term_ords.len() as f64)
    }
}

#[cfg(test)]
mod tests {
    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::tests::{exec_request, get_test_index_from_terms};
    use crate::schema::{Schema, FAST, STRING};
    use crate::Index;

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
}
