//! Segment collector that emits the configured `missing` coupon for a `cardinality` aggregation
//! when a segment contains multiple typed columns for the same logical field (e.g. a JSON path
//! with both `Str` and numeric columns).
//!
//! In that case, no individual column collector can decide on its own whether a document is
//! missing the field. This sibling collector checks all columns of the field for each document
//! and, as soon as a bucket sees at least one document missing across all columns, records that
//! bucket as containing the canonical `missing` coupon.

use columnar::Column;
use datasketches::hll::Coupon;

use super::CardinalityCollector;
use crate::aggregation::agg_data::AggregationsSegmentCtx;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateMetricResult,
};
use crate::aggregation::segment_agg_result::SegmentAggregationCollector;
use crate::aggregation::BucketId;

/// Request data for [`SegmentMissingCardinalityCollector`].
pub(crate) struct MissingCardinalityAggReqData {
    /// All physical columns for the logical field in this segment.
    pub(crate) accessors: Vec<Column<u64>>,
    /// The name of the aggregation.
    pub(crate) name: String,
    /// Pre-computed canonical coupon for the configured `missing` key.
    pub(crate) missing_coupon: Coupon,
}

impl MissingCardinalityAggReqData {
    /// Estimate the memory consumption of this struct in bytes.
    pub fn get_memory_consumption(&self) -> usize {
        std::mem::size_of::<Self>() + self.accessors.len() * std::mem::size_of::<Column<u64>>()
    }
}

/// Companion cardinality collector for multi-column segments with a `missing` key.
#[derive(Debug)]
pub(crate) struct SegmentMissingCardinalityCollector {
    accessors: Vec<Column<u64>>,
    name: String,
    missing_coupon: Coupon,
    /// Per-bucket flag indicating whether at least one document had no value in any column.
    bucket_has_missing: Vec<bool>,
}

impl SegmentMissingCardinalityCollector {
    pub(crate) fn from_req(req_data: MissingCardinalityAggReqData) -> Self {
        Self {
            accessors: req_data.accessors,
            name: req_data.name,
            missing_coupon: req_data.missing_coupon,
            bucket_has_missing: Vec::new(),
        }
    }
}

impl SegmentAggregationCollector for SegmentMissingCardinalityCollector {
    fn add_intermediate_aggregation_result(
        &mut self,
        agg_data: &AggregationsSegmentCtx,
        results: &mut IntermediateAggregationResults,
        bucket_id: BucketId,
    ) -> crate::Result<()> {
        self.prepare_max_bucket(bucket_id, agg_data)?;
        let mut cardinality = CardinalityCollector::default();
        if self.bucket_has_missing[bucket_id as usize] {
            cardinality.insert_coupon(self.missing_coupon);
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
        _agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        let has_missing = &mut self.bucket_has_missing[parent_bucket_id as usize];
        if *has_missing {
            return Ok(());
        }
        for &doc in docs {
            let has_value = self.accessors.iter().any(|acc| acc.index.has_value(doc));
            if !has_value {
                *has_missing = true;
                break;
            }
        }
        Ok(())
    }

    fn prepare_max_bucket(
        &mut self,
        max_bucket: BucketId,
        _agg_data: &AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        if max_bucket as usize >= self.bucket_has_missing.len() {
            self.bucket_has_missing
                .resize(max_bucket as usize + 1, false);
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
