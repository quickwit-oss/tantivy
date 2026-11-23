//! Segment collector for `cardinality` over any non-str column
//! (numeric, bool, date, IpAddr).
//!
//! Unlike the str case there is no dictionary to resolve, so values go
//! straight into a per-bucket HLL sketch during collection. The produced
//! [`CardinalityCollector`] is the same type the str collector produces, so
//! intermediate merging stays in the parent module.

use std::fmt::Debug;
use std::sync::Arc;

use columnar::column_values::CompactSpaceU64Accessor;
use columnar::{ColumnType, DateTime, MonotonicallyMappableToU64};
use datasketches::hll::Coupon;

use super::{CardinalityAggReqData, CardinalityCollector};
use crate::aggregation::agg_data::AggregationsSegmentCtx;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateMetricResult,
};
use crate::aggregation::segment_agg_result::SegmentAggregationCollector;
use crate::aggregation::value_source::ValueSource;
use crate::aggregation::*;
use crate::TantivyError;

/// Per-column value reader for non-`Str` cardinality collectors.
///
/// Shared between [`SegmentNumericCardinalityCollector`] (single-column segments) and
/// [`super::SegmentMultiColumnCardinalityCollector`] (multi-column segments) so both hash
/// non-`Str` values through the exact same canonical coupon dispatch loop.
pub(super) struct NumericCardinalitySource {
    /// The column accessor to access the fast field values.
    accessor: Box<dyn ValueSource>,
    /// The column_type of the field.
    column_type: ColumnType,
    /// Set iff `column_type == ColumnType::IpAddr`. Resolved once at
    /// construction: the raw column values are compact-space codes that must
    /// be expanded to their u128 ip representation before hashing.
    compact_space_accessor: Option<Arc<CompactSpaceU64Accessor>>,
}

impl Debug for NumericCardinalitySource {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("NumericCardinalitySource")
            .field("column_type", &self.column_type)
            .finish()
    }
}

impl NumericCardinalitySource {
    pub(super) fn new(accessor: Box<dyn ValueSource>) -> crate::Result<Self> {
        let column_type = accessor.column_type();
        if matches!(column_type, ColumnType::Str | ColumnType::Bytes) {
            return Err(TantivyError::InternalError(format!(
                "unexpected column type {column_type:?} in NumericCardinalitySource"
            )));
        }
        let compact_space_accessor = if column_type == ColumnType::IpAddr {
            let compact_space_accessor = accessor
                .as_physical_column()
                .ok_or_else(|| {
                    TantivyError::AggregationError(
                        crate::aggregation::AggregationError::InternalError(
                            "IpAddr cardinality requires a physical column".to_string(),
                        ),
                    )
                })?
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
            Some(compact_space_accessor)
        } else {
            None
        };
        Ok(Self {
            accessor,
            column_type,
            compact_space_accessor,
        })
    }

    pub(super) fn collect_into(
        &mut self,
        docs: &[crate::DocId],
        cardinality: &mut CardinalityCollector,
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        agg_data
            .column_block_accessor
            .fetch_block(docs, &mut *self.accessor);
        let col_block_accessor = &agg_data.column_block_accessor;
        // Dispatch on column_type once per block so the hot per-value loop stays branch-light
        // with zero allocations.
        match self.column_type {
            ColumnType::U64 => {
                for val in col_block_accessor.iter_vals() {
                    cardinality.insert_u64(val);
                }
            }
            ColumnType::I64 => {
                for val in col_block_accessor.iter_vals() {
                    cardinality.insert_i64(i64::from_u64(val));
                }
            }
            ColumnType::F64 => {
                for val in col_block_accessor.iter_vals() {
                    cardinality.insert_f64(f64::from_u64(val));
                }
            }
            ColumnType::Bool => {
                for val in col_block_accessor.iter_vals() {
                    cardinality.insert_bool(bool::from_u64(val));
                }
            }
            ColumnType::DateTime => {
                for val in col_block_accessor.iter_vals() {
                    cardinality
                        .insert_datetime_nanos(DateTime::from_u64(val).into_timestamp_nanos());
                }
            }
            ColumnType::IpAddr => {
                let compact_space_accessor =
                    self.compact_space_accessor.as_ref().ok_or_else(|| {
                        crate::TantivyError::InternalError(
                            "IpAddr cardinality requires compact_space_accessor".to_string(),
                        )
                    })?;
                for val in col_block_accessor.iter_vals() {
                    let val: u128 = compact_space_accessor.compact_to_u128(val as u32);
                    cardinality.insert_ip_addr(val);
                }
            }
            ColumnType::Str | ColumnType::Bytes => {
                return Err(crate::TantivyError::InternalError(format!(
                    "unexpected column type {:?} in NumericCardinalitySource",
                    self.column_type
                )));
            }
        }
        Ok(())
    }
}

/// Segment collector for `cardinality` over any non-str column
/// (numeric, bool, date, IpAddr).
///
/// Hidden contract: `column_type` must not be `ColumnType::Str`. Values are
/// inserted into the HLL sketch during collection, so the sketch of a bucket
/// is already complete when the bucket is finalized.
pub(crate) struct SegmentNumericCardinalityCollector {
    /// Buckets are Some(_) until they get consumed by
    /// `add_intermediate_aggregation_result`.
    buckets: Vec<Option<CardinalityCollector>>,
    /// The name of the aggregation.
    name: String,
    /// Per-column reader and canonical coupon dispatcher.
    source: NumericCardinalitySource,
    /// Pre-computed canonical HLL coupon for the configured `missing` key, if handled by this
    /// collector.
    missing_coupon: Option<Coupon>,
}

impl Debug for SegmentNumericCardinalityCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("SegmentNumericCardinalityCollector")
            .field("column_type", &self.source.column_type)
            .field("missing_coupon", &self.missing_coupon)
            .finish()
    }
}

impl SegmentNumericCardinalityCollector {
    pub(crate) fn from_req(req_data: CardinalityAggReqData) -> crate::Result<Self> {
        let CardinalityAggReqData {
            accessor,
            missing_coupon,
            name,
            ..
        } = req_data;
        let source = NumericCardinalitySource::new(accessor)?;
        Ok(Self {
            buckets: Vec::new(),
            name,
            source,
            missing_coupon,
        })
    }
}

impl SegmentAggregationCollector for SegmentNumericCardinalityCollector {
    fn add_intermediate_aggregation_result(
        &mut self,
        agg_data: &AggregationsSegmentCtx,
        results: &mut IntermediateAggregationResults,
        bucket_id: BucketId,
    ) -> crate::Result<()> {
        self.prepare_max_bucket(bucket_id, agg_data)?;
        let name = self.name.clone();
        // take the bucket in buckets and replace it with a new empty one
        let Some(cardinality) = self.buckets[bucket_id as usize].take() else {
            return Err(crate::TantivyError::InternalError(
                "the same bucket should not be finalized twice.".to_string(),
            ));
        };
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
        let cardinality = self.buckets[parent_bucket_id as usize]
            .as_mut()
            .ok_or_else(|| {
                crate::TantivyError::InternalError(
                    "collection should not happen after finalization".to_string(),
                )
            })?;
        self.source.collect_into(docs, cardinality, agg_data)?;
        if let Some(missing_coupon) = self.missing_coupon {
            if agg_data.column_block_accessor.has_missing_docs(docs) {
                cardinality.insert_coupon(missing_coupon);
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
            self.buckets.resize_with(max_bucket as usize + 1, || {
                Some(CardinalityCollector::default())
            });
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
        let cardinality = self.buckets.get(bucket_id as usize)?.as_ref()?;
        Some(cardinality.sketch.estimate().trunc())
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;
    use std::str::FromStr;

    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::tests::exec_request;
    use crate::schema::{IntoIpv6Addr, Schema, FAST};
    use crate::Index;

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
}
