use std::collections::hash_map::DefaultHasher;
use std::hash::{BuildHasher, Hasher};

use columnar::column_values::CompactSpaceU64Accessor;
use columnar::Dictionary;
use common::f64_to_u64;
use hyperloglogplus::{HyperLogLog, HyperLogLogPlus};
use rustc_hash::FxHashSet;
use serde::{Deserialize, Serialize};

use crate::aggregation::agg_req_with_accessor::{
    AggregationWithAccessor, AggregationsWithAccessor,
};
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateMetricResult,
};
use crate::aggregation::segment_agg_result::SegmentAggregationCollector;
use crate::aggregation::*;
use crate::TantivyError;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BuildSaltedHasher {
    salt: u8,
}

impl BuildHasher for BuildSaltedHasher {
    type Hasher = DefaultHasher;

    fn build_hasher(&self) -> Self::Hasher {
        let mut hasher = DefaultHasher::new();
        hasher.write_u8(self.salt);

        hasher
    }
}

/// # Cardinality
///
/// The cardinality aggregation allows for computing an estimate
/// of the number of different values in a data set based on the
/// HyperLogLog++ algorithm. This is particularly useful for understanding the
/// uniqueness of values in a large dataset where counting each unique value
/// individually would be computationally expensive.
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
    cardinality: CardinalityCollector,
    entries: FxHashSet<u64>,
    column_type: ColumnType,
    accessor_idx: usize,
    missing: Option<Key>,
}

impl SegmentCardinalityCollector {
    pub fn from_req(column_type: ColumnType, accessor_idx: usize, missing: &Option<Key>) -> Self {
        Self {
            cardinality: CardinalityCollector::new(column_type as u8),
            entries: Default::default(),
            column_type,
            accessor_idx,
            missing: missing.clone(),
        }
    }

    fn fetch_block_with_field(
        &mut self,
        docs: &[crate::DocId],
        agg_accessor: &mut AggregationWithAccessor,
    ) {
        if let Some(missing) = agg_accessor.missing_value_for_accessor {
            agg_accessor.column_block_accessor.fetch_block_with_missing(
                docs,
                &agg_accessor.accessor,
                missing,
            );
        } else {
            agg_accessor
                .column_block_accessor
                .fetch_block(docs, &agg_accessor.accessor);
        }
    }

    fn into_intermediate_metric_result(
        mut self,
        agg_with_accessor: &AggregationWithAccessor,
    ) -> crate::Result<IntermediateMetricResult> {
        if self.column_type == ColumnType::Str {
            let fallback_dict = Dictionary::empty();
            let dict = agg_with_accessor
                .str_dict_column
                .as_ref()
                .map(|el| el.dictionary())
                .unwrap_or_else(|| &fallback_dict);
            let mut has_missing = false;

            // TODO: replace FxHashSet with something that allows iterating in order
            // (e.g. sparse bitvec)
            let mut term_ids = vec![];
            for term_ord in self.entries.into_iter() {
                if term_ord == u64::MAX {
                    has_missing = true;
                } else {
                    // we can reasonably exclude values above u32::MAX
                    term_ids.push(term_ord as u32);
                }
            }
            term_ids.sort_unstable();
            dict.sorted_ords_to_term_cb(term_ids.iter().map(|term| *term as u64), |term| {
                self.cardinality.sketch.insert_any(&term);
                Ok(())
            })?;
            if has_missing {
                // Replace missing with the actual value provided
                let missing_key = self
                    .missing
                    .as_ref()
                    .expect("Found sentinel value u64::MAX for term_ord but `missing` is not set");
                match missing_key {
                    Key::Str(missing) => {
                        self.cardinality.sketch.insert_any(&missing);
                    }
                    Key::F64(val) => {
                        let val = f64_to_u64(*val);
                        self.cardinality.sketch.insert_any(&val);
                    }
                    Key::U64(val) => {
                        self.cardinality.sketch.insert_any(&val);
                    }
                    Key::I64(val) => {
                        self.cardinality.sketch.insert_any(&val);
                    }
                }
            }
        }

        Ok(IntermediateMetricResult::Cardinality(self.cardinality))
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
        self.fetch_block_with_field(docs, bucket_agg_accessor);

        let col_block_accessor = &bucket_agg_accessor.column_block_accessor;
        if self.column_type == ColumnType::Str {
            for term_ord in col_block_accessor.iter_vals() {
                self.entries.insert(term_ord);
            }
        } else if self.column_type == ColumnType::IpAddr {
            let compact_space_accessor = bucket_agg_accessor
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
                self.cardinality.sketch.insert_any(&val);
            }
        } else {
            for val in col_block_accessor.iter_vals() {
                self.cardinality.sketch.insert_any(&val);
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// The percentiles collector used during segment collection and for merging results.
pub struct CardinalityCollector {
    sketch: HyperLogLogPlus<u64, BuildSaltedHasher>,
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

impl CardinalityCollector {
    /// Compute the final cardinality estimate.
    pub fn finalize(self) -> Option<f64> {
        Some(self.sketch.clone().count().trunc())
    }

    fn new(salt: u8) -> Self {
        Self {
            sketch: HyperLogLogPlus::new(16, BuildSaltedHasher { salt }).unwrap(),
        }
    }

    pub(crate) fn merge_fruits(&mut self, right: Self) -> crate::Result<()> {
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

    use columnar::MonotonicallyMappableToU64;

    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::tests::{exec_request, get_test_index_from_terms};
    use crate::schema::{IntoIpv6Addr, Schema, FAST};
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
}
