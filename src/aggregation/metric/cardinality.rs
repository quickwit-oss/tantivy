use std::collections::hash_map::DefaultHasher;
use std::hash::BuildHasher;

use columnar::column_values::CompactSpaceU64Accessor;
use hyperloglogplus::{HyperLogLog, HyperLogLogPlus};
use rustc_hash::FxHashSet;
use serde::{Deserialize, Serialize};

use crate::aggregation::agg_req_with_accessor::AggregationsWithAccessor;
use crate::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use crate::aggregation::segment_agg_result::SegmentAggregationCollector;
use crate::aggregation::*;

use crate::TantivyError;

use self::agg_req_with_accessor::AggregationWithAccessor;
use self::intermediate_agg_result::IntermediateAggregationResult;
use self::intermediate_agg_result::IntermediateMetricResult;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DefaultBuildHasher;

impl BuildHasher for DefaultBuildHasher {
    type Hasher = DefaultHasher;

    fn build_hasher(&self) -> Self::Hasher {
        DefaultHasher::new()
    }
}

/// # Cardinality
///
/// The cardinality aggregation allows for computing an estimate
/// of the number of different values in a data set based on the
/// HyperLogLog++ alogrithm.
///
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

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SegmentCardinalityCollector {
    entries: FxHashSet<u64>,
    column_type: ColumnType,
    accessor_idx: usize,
}

impl SegmentCardinalityCollector {
    pub fn from_req(column_type: ColumnType, accessor_idx: usize) -> Self {
        Self {
            entries: Default::default(),
            column_type,
            accessor_idx,
        }
    }

    fn into_intermediate_metric_result(
        self,
        agg_with_accessor: &AggregationWithAccessor,
    ) -> crate::Result<IntermediateMetricResult> {
        let mut collector = CardinalityCollector::new();
        let entries: Vec<u64> = self.entries.into_iter().collect();
        if self.column_type == ColumnType::Str {
            // TODO: better error handling
            let term_dict = agg_with_accessor.str_dict_column.as_ref().cloned().unwrap();
            for term_id in entries {
                let mut buffer = String::new();
                if !term_dict.ord_to_str(term_id, &mut buffer)? {
                    return Err(TantivyError::InternalError(format!(
                        "Couldn't find term_id {term_id} in dict"
                    )));
                }
                collector.sketch.insert_any(&buffer);
            }
        } else if self.column_type == ColumnType::IpAddr {
            let compact_space_accessor = agg_with_accessor
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
            for val in entries {
                let val: u128 = compact_space_accessor.compact_to_u128(val as u32);
                collector.sketch.insert_any(&val);
            }
        } else {
            for val in entries {
                collector.sketch.insert_any(&val);
            }
        }

        Ok(IntermediateMetricResult::Cardinality(collector))
    }
}

impl SegmentAggregationCollector for SegmentCardinalityCollector {
    fn add_intermediate_aggregation_result(
        self: Box<Self>,
        agg_with_accessor: &AggregationsWithAccessor,
        results: &mut IntermediateAggregationResults,
    ) -> crate::Result<()> {
        let name = agg_with_accessor.aggs.keys[self.accessor_idx].to_string();
        let agg_with_accessor = &agg_with_accessor.aggs.values[self.accessor_idx];

        let intermediate_result = self.into_intermediate_metric_result(agg_with_accessor)?;
        results.push(
            name,
            IntermediateAggregationResult::Metric(intermediate_result),
        )?;

        Ok(())
    }

    fn collect(
        &mut self,
        doc: crate::DocId,
        agg_with_accessor: &mut AggregationsWithAccessor,
    ) -> crate::Result<()> {
        self.collect_block(&[doc], agg_with_accessor)
    }

    fn collect_block(
        &mut self,
        docs: &[crate::DocId],
        agg_with_accessor: &mut AggregationsWithAccessor,
    ) -> crate::Result<()> {
        let bucket_agg_accessor = &mut agg_with_accessor.aggs.values[self.accessor_idx];
        bucket_agg_accessor
            .column_block_accessor
            .fetch_block(docs, &bucket_agg_accessor.accessor);

        for term_id in bucket_agg_accessor.column_block_accessor.iter_vals() {
            self.entries.insert(term_id);
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// The percentiles collector used during segment collection and for merging results.
pub struct CardinalityCollector {
    // TODO
    sketch: HyperLogLogPlus<u64, DefaultBuildHasher>,
}
impl Default for CardinalityCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for CardinalityCollector {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

impl CardinalityCollector {
    /// Compute the final cardinality estimate.
    pub fn finalize(self) -> Option<f64> {
        Some(self.sketch.clone().count().trunc())
    }

    fn new() -> Self {
        Self {
            sketch: HyperLogLogPlus::new(16, DefaultBuildHasher {}).unwrap(),
        }
    }

    pub(crate) fn merge_fruits(&mut self, right: CardinalityCollector) -> crate::Result<()> {
        self.sketch.merge(&right.sketch).map_err(|err| {
            TantivyError::AggregationError(AggregationError::InternalError(format!(
                "Error while merging cardinality {err:?}"
            )))
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::net::IpAddr;
    use std::str::FromStr;

    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::tests::{
        exec_request, exec_request_with_query, get_test_index_from_terms,
    };

    use crate::indexer::NoMergePolicy;
    use crate::schema::{IntoIpv6Addr, Schema, FAST};
    use crate::Index;

    #[test]
    fn cardinality_aggregation_test_empty_index() -> crate::Result<()> {
        // test index without segments
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

    #[test]
    fn cardinality_aggregation_u64() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_u64_field("id", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.set_merge_policy(Box::new(NoMergePolicy));
            index_writer.add_document(doc!(
                id_field => 1u64,
            ))?;
            index_writer.add_document(doc!(
                id_field => 2u64,
            ))?;
            index_writer.add_document(doc!(
                id_field => 3u64,
            ))?;
            index_writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "cardinality": {
                "cardinality": {
                    "field": "id"
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;
        assert_eq!(res["cardinality"]["value"], 3.0);

        Ok(())
    }

    #[test]
    fn cardinality_aggregation_ip_addr() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_ip_addr_field("ip_field", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer = index.writer_with_num_threads(1, 20_000_000)?;
            writer.set_merge_policy(Box::new(NoMergePolicy));
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

        let res = exec_request_with_query(agg_req, &index, None)?;
        assert_eq!(res["cardinality"]["value"], 2.0);

        Ok(())
    }
}
