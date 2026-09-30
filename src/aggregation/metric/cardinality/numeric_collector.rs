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
use columnar::ColumnType;

use super::CardinalityCollector;
use crate::aggregation::agg_data::AggregationsSegmentCtx;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateMetricResult,
};
use crate::aggregation::segment_agg_result::SegmentAggregationCollector;
use crate::aggregation::value_source::ValueSource;
use crate::aggregation::*;
use crate::TantivyError;

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
    accessor_idx: usize,
    /// The column accessor to access the fast field values.
    accessor: Arc<dyn ValueSource>,
    /// The column_type of the field.
    column_type: ColumnType,
    /// Set iff `column_type == ColumnType::IpAddr`. Resolved once at
    /// construction: the raw column values are compact-space codes that must
    /// be expanded to their u128 ip representation before hashing.
    compact_space_accessor: Option<Arc<CompactSpaceU64Accessor>>,
    /// The missing value normalized to the internal u64 representation of the field type.
    missing_value_for_accessor: Option<u64>,
}

impl Debug for SegmentNumericCardinalityCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("SegmentNumericCardinalityCollector")
            .field("column_type", &self.column_type)
            .field(
                "missing_value_for_accessor",
                &self.missing_value_for_accessor,
            )
            .finish()
    }
}

impl SegmentNumericCardinalityCollector {
    pub fn from_req(
        accessor_idx: usize,
        accessor: Arc<dyn ValueSource>,
        missing_value_for_accessor: Option<u64>,
    ) -> crate::Result<Self> {
        let column_type = accessor.column_type();
        assert_ne!(column_type, ColumnType::Str);
        let compact_space_accessor = if column_type == ColumnType::IpAddr {
            let compact_space_accessor = accessor
                .as_column()
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
            buckets: Vec::new(),
            accessor_idx,
            accessor,
            column_type,
            compact_space_accessor,
            missing_value_for_accessor,
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
        let name = agg_data
            .get_cardinality_req_data(self.accessor_idx)
            .name
            .to_string();
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
        agg_data.column_block_accessor.fetch_block_with_missing(
            docs,
            &*self.accessor,
            self.missing_value_for_accessor,
        );
        let cardinality = self.buckets[parent_bucket_id as usize]
            .as_mut()
            .ok_or_else(|| {
                crate::TantivyError::InternalError(
                    "collection should not happen after finalization".to_string(),
                )
            })?;
        let col_block_accessor = &agg_data.column_block_accessor;
        if let Some(compact_space_accessor) = self.compact_space_accessor.as_ref() {
            for val in col_block_accessor.iter_vals() {
                let val: u128 = compact_space_accessor.compact_to_u128(val as u32);
                cardinality.insert(val);
            }
        } else {
            for val in col_block_accessor.iter_vals() {
                cardinality.insert(val);
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
            self.buckets.resize_with(max_bucket as usize + 1, || {
                Some(CardinalityCollector::new(column_type as u8))
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
