//! Compact strings are hashed as bytes directly, without retaining distinct terms.

use columnar::{ColumnType, StrColumn};
use datasketches::hll::Coupon;

use super::{CardinalityAggReqData, CardinalityCollector};
use crate::aggregation::agg_data::AggregationsSegmentCtx;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateMetricResult,
};
use crate::aggregation::segment_agg_result::SegmentAggregationCollector;
use crate::aggregation::{BucketId, Key};

#[derive(Debug)]
pub(crate) struct CompactCardinalityCollector {
    column: StrColumn,
    name: String,
    missing: Option<Coupon>,
    buffer: Vec<u8>,
    buckets: Vec<Option<CardinalityCollector>>,
}

impl CompactCardinalityCollector {
    pub(crate) fn new(request: CardinalityAggReqData, column: StrColumn) -> Self {
        // Match the existing dictionary collector's missing-value hashing.
        let missing = request.req.missing.as_ref().map(|key| match key {
            Key::Str(value) => Coupon::from_value(value.as_bytes()),
            _ => Coupon::from_value(b"__tantivy_missing_non_str__".as_slice()),
        });
        Self {
            column,
            name: request.name,
            missing,
            buffer: Vec::new(),
            buckets: Vec::new(),
        }
    }
}

impl SegmentAggregationCollector for CompactCardinalityCollector {
    fn collect(
        &mut self,
        parent_bucket: BucketId,
        docs: &[crate::DocId],
        context: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        let previous_capacity = self.buffer.capacity();
        let bucket = self.buckets[parent_bucket as usize]
            .as_mut()
            .ok_or_else(|| {
                crate::TantivyError::InternalError("cardinality bucket already finalized".into())
            })?;
        for &doc in docs {
            let values = self.column.value_range(doc)?;
            if values.is_empty() {
                if let Some(coupon) = self.missing {
                    bucket.insert_coupon(coupon);
                }
            }
            for value in values {
                self.column.read_value(value, &mut self.buffer)?;
                bucket.insert_coupon(Coupon::from_value(self.buffer.as_slice()));
            }
        }
        context
            .context
            .limits
            .add_memory_consumed((self.buffer.capacity() - previous_capacity) as u64)?;
        Ok(())
    }

    fn prepare_max_bucket(
        &mut self,
        max_bucket: BucketId,
        _context: &AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        while self.buckets.len() <= max_bucket as usize {
            self.buckets
                .push(Some(CardinalityCollector::new(ColumnType::Str as u8)));
        }
        Ok(())
    }

    fn add_intermediate_aggregation_result(
        &mut self,
        context: &AggregationsSegmentCtx,
        results: &mut IntermediateAggregationResults,
        bucket: BucketId,
    ) -> crate::Result<()> {
        self.prepare_max_bucket(bucket, context)?;
        let result = self.buckets[bucket as usize].take().ok_or_else(|| {
            crate::TantivyError::InternalError("cardinality bucket already finalized".into())
        })?;
        results.push(
            self.name.clone(),
            IntermediateAggregationResult::Metric(IntermediateMetricResult::Cardinality(result)),
        )?;
        Ok(())
    }

    fn compute_metric_value(
        &self,
        bucket: BucketId,
        name: &str,
        property: &str,
        _context: &AggregationsSegmentCtx,
    ) -> Option<f64> {
        if name != self.name || !property.is_empty() {
            return None;
        }
        Some(
            self.buckets
                .get(bucket as usize)?
                .as_ref()?
                .sketch
                .estimate()
                .trunc(),
        )
    }
}
