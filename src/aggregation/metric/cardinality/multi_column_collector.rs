//! Segment collector for `cardinality` when a segment contains multiple physical columns for the
//! same logical field (e.g. a dynamic JSON path with both `Str` and numeric columns).
//!
//! Combining all physical columns and optional cross-column `missing` detection into one
//! collector ensures that:
//! - non-`Str` columns (such as `I64`, `U64`, `F64`) share a single per-bucket HLL sketch,
//!   deduplicating mathematically equal values (`1i64 == 1.0f64`) immediately during collection;
//! - a `missing` key present in the `Str` dictionary is inserted as that existing term ordinal
//!   into the `Str` accumulator (deduplicating against real strings), while any other `missing`
//!   key is inserted as its canonical coupon into the shared non-`Str` sketch (deduplicating
//!   against numeric columns while staying disjoint from `Str` values);
//! - `compute_metric_value` returns the combined segment-level cardinality estimate (including
//!   all columns and `missing` contributions) in O(1) without cloning or mutating sketches.

use std::fmt::Debug;

use columnar::Column;
use datasketches::hll::Coupon;

use super::numeric_collector::NumericCardinalitySource;
use super::str_collector::StrCardinalityCollector;
use super::{CardinalityAggReqData, CardinalityCollector};
use crate::aggregation::agg_data::AggregationsSegmentCtx;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateMetricResult,
};
use crate::aggregation::segment_agg_result::SegmentAggregationCollector;
use crate::aggregation::value_source::ValueSource;
use crate::aggregation::BucketId;

/// Destination for a `missing` value observed across all physical columns of a multi-column
/// cardinality field.
#[derive(Clone, Copy, Debug)]
pub(crate) enum MultiColumnMissingTarget {
    /// `missing` is a `Key::Str` already present in this segment's `Str` dictionary.
    /// Inserting its ordinal into the `Str` accumulator deduplicates against real strings.
    StrTermOrd(u64),
    /// `missing` is either non-string (may overlap numeric columns, so goes into the shared
    /// non-`Str` sketch) or a `Key::Str` absent from the segment's `Str` dictionary (disjoint
    /// from all real values in the segment, so safe in the shared non-`Str` sketch).
    Sketch(Coupon),
}

/// Cross-column `missing` configuration for [`MultiColumnCardinalityAggReqData`].
pub(crate) struct MultiColumnMissingConfig {
    /// All physical columns for the logical field in this segment (none of which is `Full`).
    pub(crate) accessors: Vec<Column<u64>>,
    /// Where to record the missing value when a bucket sees its first all-column missing doc.
    pub(crate) target: MultiColumnMissingTarget,
}

/// Request data for [`SegmentMultiColumnCardinalityCollector`].
pub(crate) struct MultiColumnCardinalityAggReqData {
    /// The name of the aggregation.
    pub(crate) name: String,
    /// Optional physical `Str` column for the logical field (at most one per segment).
    pub(crate) str_accessor: Option<Box<dyn ValueSource>>,
    /// All non-`Str` physical columns (`I64`, `U64`, `F64`, `Bool`, `DateTime`, `IpAddr`).
    pub(crate) numeric_accessors: Vec<Box<dyn ValueSource>>,
    /// Present iff `missing` is configured and no physical column for the field is `Full`.
    pub(crate) missing: Option<MultiColumnMissingConfig>,
}

impl MultiColumnCardinalityAggReqData {
    /// Estimate the memory consumption of this struct in bytes.
    pub fn get_memory_consumption(&self) -> usize {
        let missing_cols_size = self.missing.as_ref().map_or(0, |m| {
            m.accessors.len() * std::mem::size_of::<Column<u64>>()
        });
        std::mem::size_of::<Self>()
            + self.numeric_accessors.len() * std::mem::size_of::<Box<dyn ValueSource>>()
            + missing_cols_size
    }
}

struct MissingState {
    accessors: Vec<Column<u64>>,
    target: MultiColumnMissingTarget,
    /// Per-bucket flag indicating whether at least one document had no value in any column.
    bucket_has_missing: Vec<bool>,
}

impl Debug for MissingState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MissingState")
            .field("num_accessors", &self.accessors.len())
            .field("target", &self.target)
            .finish()
    }
}

/// Unified segment cardinality collector for fields spanning multiple physical columns in one
/// segment.
#[derive(Debug)]
pub(crate) struct SegmentMultiColumnCardinalityCollector {
    name: String,
    str_collector: Option<StrCardinalityCollector>,
    numeric_sources: Vec<NumericCardinalitySource>,
    /// Shared per-bucket HLL sketch for all non-`Str` columns (and non-dictionary `missing`
    /// coupons). Buckets are `Some(_)` until consumed by `add_intermediate_aggregation_result`.
    numeric_buckets: Vec<Option<CardinalityCollector>>,
    missing: Option<MissingState>,
}

impl SegmentMultiColumnCardinalityCollector {
    pub(crate) fn from_req(req_data: MultiColumnCardinalityAggReqData) -> crate::Result<Self> {
        let MultiColumnCardinalityAggReqData {
            name,
            str_accessor,
            numeric_accessors,
            missing,
        } = req_data;
        let str_collector = str_accessor.map(|accessor| {
            StrCardinalityCollector::from_req(CardinalityAggReqData {
                accessor,
                missing_value_for_accessor: None,
                missing_coupon: None,
                name: name.clone(),
            })
        });
        if matches!(
            missing.as_ref().map(|m| m.target),
            Some(MultiColumnMissingTarget::StrTermOrd(_))
        ) && str_collector.is_none()
        {
            return Err(crate::TantivyError::InternalError(
                "StrTermOrd missing target requires a Str column".to_string(),
            ));
        }
        let numeric_sources = numeric_accessors
            .into_iter()
            .map(NumericCardinalitySource::new)
            .collect::<crate::Result<Vec<_>>>()?;
        let missing = missing.map(|m| MissingState {
            accessors: m.accessors,
            target: m.target,
            bucket_has_missing: Vec::new(),
        });
        Ok(Self {
            name,
            str_collector,
            numeric_sources,
            numeric_buckets: Vec::new(),
            missing,
        })
    }
}

impl SegmentAggregationCollector for SegmentMultiColumnCardinalityCollector {
    fn add_intermediate_aggregation_result(
        &mut self,
        agg_data: &AggregationsSegmentCtx,
        results: &mut IntermediateAggregationResults,
        bucket_id: BucketId,
    ) -> crate::Result<()> {
        self.prepare_max_bucket(bucket_id, agg_data)?;
        let Some(mut cardinality) = self.numeric_buckets[bucket_id as usize].take() else {
            return Err(crate::TantivyError::InternalError(
                "the same bucket should not be finalized twice.".to_string(),
            ));
        };
        if let Some(str_collector) = &mut self.str_collector {
            str_collector.append_bucket_to_sketch(agg_data, bucket_id, &mut cardinality)?;
        }
        results.push(
            self.name.clone(),
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
        if let Some(str_collector) = &mut self.str_collector {
            str_collector.collect(parent_bucket_id, docs, agg_data)?;
        }
        let cardinality = self.numeric_buckets[parent_bucket_id as usize]
            .as_mut()
            .ok_or_else(|| {
                crate::TantivyError::InternalError(
                    "collection should not happen after finalization".to_string(),
                )
            })?;
        for source in &mut self.numeric_sources {
            source.collect_into(docs, cardinality, agg_data)?;
        }
        if let Some(missing) = &mut self.missing {
            let has_missing = &mut missing.bucket_has_missing[parent_bucket_id as usize];
            if !*has_missing {
                for &doc in docs {
                    if !missing.accessors.iter().any(|acc| acc.index.has_value(doc)) {
                        *has_missing = true;
                        match missing.target {
                            MultiColumnMissingTarget::StrTermOrd(term_ord) => {
                                let str_collector =
                                    self.str_collector.as_mut().ok_or_else(|| {
                                        crate::TantivyError::InternalError(
                                            "StrTermOrd missing target requires a Str collector"
                                                .to_string(),
                                        )
                                    })?;
                                str_collector.insert_term_ord(parent_bucket_id, term_ord)?;
                            }
                            MultiColumnMissingTarget::Sketch(coupon) => {
                                cardinality.insert_coupon(coupon);
                            }
                        }
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    fn prepare_max_bucket(
        &mut self,
        max_bucket: BucketId,
        agg_data: &AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        let target_len = max_bucket as usize + 1;
        if target_len > self.numeric_buckets.len() {
            self.numeric_buckets
                .resize_with(target_len, || Some(CardinalityCollector::default()));
        }
        if let Some(str_collector) = &mut self.str_collector {
            str_collector.prepare_max_bucket(max_bucket, agg_data)?;
        }
        if let Some(missing) = &mut self.missing {
            if target_len > missing.bucket_has_missing.len() {
                missing.bucket_has_missing.resize(target_len, false);
            }
        }
        Ok(())
    }

    fn compute_metric_value(
        &self,
        bucket_id: BucketId,
        sub_agg_name: &str,
        sub_agg_property: &str,
        _agg_data: &AggregationsSegmentCtx,
    ) -> Option<f64> {
        if self.name != sub_agg_name || !sub_agg_property.is_empty() {
            return None;
        }
        let numeric_estimate = self
            .numeric_buckets
            .get(bucket_id as usize)?
            .as_ref()?
            .sketch
            .estimate();
        let str_count = match &self.str_collector {
            Some(str_collector) => str_collector.bucket_len(bucket_id)? as f64,
            None => 0.0,
        };
        // `str_collector` and `numeric_buckets` partition disjoint value domains (an in-dictionary
        // `Key::Str` missing value is routed to `str_collector`, while numeric and absent-string
        // missing values go into `numeric_buckets`), so their distinct counts add cleanly without
        // double-counting.
        Some((str_count + numeric_estimate).trunc())
    }
}
