use std::fmt::Debug;
use std::hash::Hash;
use std::io;

use columnar::column_values::CompactSpaceU64Accessor;
use columnar::{Column, ColumnType, Dictionary, StrColumn};
use datasketches::hll::{Coupon, HllSketch, HllType, HllUnion};
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::aggregation::agg_data::AggregationsSegmentCtx;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateMetricResult,
};
use crate::aggregation::segment_agg_result::SegmentAggregationCollector;
use crate::aggregation::*;
use crate::TantivyError;

/// Log2 of the number of registers for the HLL sketch.
/// 2^11 = 2048 registers, giving ~2.3% relative error and ~1KB per sketch (Hll4).
const LG_K: u8 = 11;

/// # Cardinality
///
/// The cardinality aggregation allows for computing an estimate
/// of the number of different values in a data set based on the
/// Apache DataSketches HyperLogLog algorithm. This is particularly useful for
/// understanding the uniqueness of values in a large dataset where counting
/// each unique value individually would be computationally expensive.
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

/// Contains all information required by the SegmentCardinalityCollector to perform the
/// cardinality aggregation on a segment.
pub struct CardinalityAggReqData {
    /// The column accessor to access the fast field values.
    pub accessor: Column<u64>,
    /// The column_type of the field.
    pub column_type: ColumnType,
    /// The string dictionary column if the field is of type string.
    pub str_dict_column: Option<StrColumn>,
    /// The missing value normalized to the internal u64 representation of the field type.
    pub missing_value_for_accessor: Option<u64>,
    /// The name of the aggregation.
    pub name: String,
    /// The aggregation request.
    pub req: CardinalityAggregationReq,
}

impl CardinalityAggReqData {
    /// Estimate the memory consumption of this struct in bytes.
    pub fn get_memory_consumption(&self) -> usize {
        std::mem::size_of::<Self>()
    }
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
            let mut coupon_map: Vec<Coupon> = vec![Coupon::EMPTY; highest_term_ord as usize + 1];
            for (term_ord, coupon) in term_ords.into_iter().zip(coupons.into_iter()) {
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

pub(crate) struct SegmentCardinalityCollector {
    /// Buckets are Some(_) until they get consumed by into_intermediate_results().
    buckets: Vec<Option<SegmentCardinalityCollectorBucket>>,
    accessor_idx: usize,
    /// The column accessor to access the fast field values.
    accessor: Column<u64>,
    /// The column_type of the field.
    column_type: ColumnType,
    /// The missing value normalized to the internal u64 representation of the field type.
    missing_value_for_accessor: Option<u64>,
    coupon_cache: Option<CouponCache>,
}

impl Debug for SegmentCardinalityCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("SegmentCardinalityCollector")
            .field("column_type", &self.column_type)
            .field(
                "missing_value_for_accessor",
                &self.missing_value_for_accessor,
            )
            .finish()
    }
}

pub(crate) struct SegmentCardinalityCollectorBucket {
    cardinality: CardinalityCollector,
    entries: FxHashSet<u64>,
}
impl SegmentCardinalityCollectorBucket {
    #[inline(always)]
    pub fn new(column_type: ColumnType) -> Self {
        Self {
            cardinality: CardinalityCollector::new(column_type as u8),
            entries: FxHashSet::default(),
        }
    }

    // Returns a intermediate metric result.
    //
    // If the column is not str, the values have been added to the
    // sketch during collection.
    //
    // If the column is str, then the values are dictionary encoded
    // and have not been added to the sketch yet.
    // We need to resolves the term ords accumulated in self.entries
    // with the coupon cache, and append the results to the sketch.
    fn into_intermediate_metric_result(
        mut self,
        coupon_cache_opt: Option<&CouponCache>,
    ) -> crate::Result<IntermediateMetricResult> {
        if let Some(coupon_cache) = coupon_cache_opt {
            assert!(self.cardinality.sketch.is_empty());
            append_to_sketch(&self.entries, coupon_cache, &mut self.cardinality);
        }
        Ok(IntermediateMetricResult::Cardinality(self.cardinality))
    }
}

/// Builds a coupon cache from the given buckets, dictionary, and optional missing value.
/// Returns a mapping from term_ord to the hash (coupon) of the associated term.
fn build_coupon_cache(
    buckets: &[Option<SegmentCardinalityCollectorBucket>],
    dictionary: &Dictionary,
    missing_value_opt: Option<&Key>,
) -> io::Result<CouponCache> {
    let term_ords_capacity: usize = buckets
        .iter()
        .flatten()
        .map(|bucket| bucket.entries.len())
        .max()
        .unwrap_or(0)
        * 2;
    let mut term_ords_set = FxHashSet::with_capacity_and_hasher(term_ords_capacity, FxBuildHasher);
    for bucket in buckets.iter().flatten() {
        term_ords_set.extend(bucket.entries.iter().copied());
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
    term_ords: &FxHashSet<u64>,
    coupon_cache: &CouponCache,
    sketch: &mut CardinalityCollector,
) {
    match coupon_cache {
        CouponCache::Dense {
            coupon_map,
            missing_coupon_opt,
        } => {
            for &term_ord in term_ords {
                if let Some(coupon) = coupon_map
                    .get(term_ord as usize)
                    .copied()
                    .or(*missing_coupon_opt)
                {
                    sketch.insert_coupon(coupon);
                }
            }
        }
        CouponCache::Sparse {
            coupon_map,
            missing_coupon_opt,
        } => {
            for term_ord in term_ords {
                if let Some(coupon) = coupon_map.get(term_ord).copied().or(*missing_coupon_opt) {
                    sketch.insert_coupon(coupon);
                }
            }
        }
    }
}

impl SegmentCardinalityCollector {
    pub fn from_req(
        column_type: ColumnType,
        accessor_idx: usize,
        accessor: Column<u64>,
        missing_value_for_accessor: Option<u64>,
    ) -> Self {
        Self {
            buckets: Vec::new(),
            column_type,
            accessor_idx,
            accessor,
            missing_value_for_accessor,
            coupon_cache: None,
        }
    }

    fn fetch_block_with_field(
        &mut self,
        docs: &[crate::DocId],
        agg_data: &mut AggregationsSegmentCtx,
    ) {
        agg_data.column_block_accessor.fetch_block_with_missing(
            docs,
            &self.accessor,
            self.missing_value_for_accessor,
        );
    }
}

impl SegmentAggregationCollector for SegmentCardinalityCollector {
    fn add_intermediate_aggregation_result(
        &mut self,
        agg_data: &AggregationsSegmentCtx,
        results: &mut IntermediateAggregationResults,
        bucket_id: BucketId,
    ) -> crate::Result<()> {
        self.prepare_max_bucket(bucket_id, agg_data)?;
        let req_data = &agg_data.get_cardinality_req_data(self.accessor_idx);
        // Strings are dictionary encoded. Fetching the terms associated to strings
        // is expensive. For this reason, we do that once for all buckets and cache the results
        // here.
        if let Some(str_dict_column) = &req_data.str_dict_column {
            // Ensure the coupon cache is populated.
            // A mapping from term_ord to the hash of the associated term.
            // The missing value sentinel will be associated to the hash of the missing value if
            // any.
            if self.coupon_cache.is_none() {
                self.coupon_cache = Some(build_coupon_cache(
                    &self.buckets,
                    str_dict_column.dictionary(),
                    req_data.req.missing.as_ref(),
                )?);
            }
        }
        let name = req_data.name.to_string();
        // take the bucket in buckets and replace it with a new empty one
        let Some(bucket) = self.buckets[bucket_id as usize].take() else {
            return Err(crate::TantivyError::InternalError(
                "the same bucket should not be finalized twice.".to_string(),
            ));
        };
        let intermediate_result =
            bucket.into_intermediate_metric_result(self.coupon_cache.as_ref())?;
        results.push(
            name,
            IntermediateAggregationResult::Metric(intermediate_result),
        )?;

        Ok(())
    }

    fn collect(
        &mut self,
        parent_bucket_id: BucketId,
        docs: &[crate::DocId],
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        self.fetch_block_with_field(docs, agg_data);
        let Some(bucket) = &mut self.buckets[parent_bucket_id as usize].as_mut() else {
            return Err(crate::TantivyError::InternalError(
                "collection should not happen after finalization".to_string(),
            ));
        };
        let col_block_accessor = &agg_data.column_block_accessor;
        if self.column_type == ColumnType::Str {
            for term_ord in col_block_accessor.iter_vals() {
                bucket.entries.insert(term_ord);
            }
        } else if self.column_type == ColumnType::IpAddr {
            let compact_space_accessor = self
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
                bucket.cardinality.insert(val);
            }
        } else {
            for val in col_block_accessor.iter_vals() {
                bucket.cardinality.insert(val);
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
                Some(SegmentCardinalityCollectorBucket::new(self.column_type))
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
/// The cardinality collector used during segment collection and for merging results.
/// Uses Apache DataSketches HLL (lg_k=11, Hll4) for compact binary serialization
/// and cross-language compatibility (e.g. Java `datasketches` library).
pub struct CardinalityCollector {
    sketch: HllSketch,
    /// Salt derived from `ColumnType`, used to differentiate values of different column types
    /// that map to the same u64 (e.g. bool `false` = 0 vs i64 `0`).
    /// Not serialized — only needed during insertion, not after sketch registers are populated.
    salt: u8,
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

impl Serialize for CardinalityCollector {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let bytes = self.sketch.serialize();
        serializer.serialize_bytes(&bytes)
    }
}

impl<'de> Deserialize<'de> for CardinalityCollector {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
        let sketch = HllSketch::deserialize(&bytes).map_err(serde::de::Error::custom)?;
        Ok(Self { sketch, salt: 0 })
    }
}

impl CardinalityCollector {
    fn new(salt: u8) -> Self {
        Self {
            sketch: HllSketch::new(LG_K, HllType::Hll4),
            salt,
        }
    }

    /// Insert a value into the HLL sketch, salted by the column type.
    /// The salt ensures that identical u64 values from different column types
    /// (e.g. bool `false` vs i64 `0`) are counted as distinct.
    fn insert<T: Hash>(&mut self, value: T) {
        self.sketch.update((self.salt, value));
    }

    fn insert_coupon(&mut self, coupon: Coupon) {
        self.sketch.update_with_coupon(coupon);
    }

    /// Compute the final cardinality estimate.
    pub fn finalize(self) -> Option<f64> {
        Some(self.sketch.estimate().trunc())
    }

    /// Serialize the HLL sketch to its compact binary representation.
    /// The format is cross-language compatible with Apache DataSketches (Java, C++, Python).
    pub fn to_sketch_bytes(&self) -> Vec<u8> {
        self.sketch.serialize()
    }

    pub(crate) fn merge_fruits(&mut self, right: CardinalityCollector) -> crate::Result<()> {
        let mut union = HllUnion::new(LG_K);
        union.update(&self.sketch);
        union.update(&right.sketch);
        self.sketch = union.to_sketch(HllType::Hll4);
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
    use crate::schema::{IntoIpv6Addr, Schema, FAST, STRING};
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

    #[test]
    fn cardinality_collector_serde_roundtrip() {
        use super::CardinalityCollector;

        let mut collector = CardinalityCollector::default();
        collector.insert("hello");
        collector.insert("world");
        collector.insert("hello"); // duplicate

        let serialized = serde_json::to_vec(&collector).unwrap();
        let deserialized: CardinalityCollector = serde_json::from_slice(&serialized).unwrap();

        let original_estimate = collector.finalize().unwrap();
        let roundtrip_estimate = deserialized.finalize().unwrap();
        assert_eq!(original_estimate, roundtrip_estimate);
        assert_eq!(original_estimate, 2.0);
    }

    #[test]
    fn cardinality_collector_merge() {
        use super::CardinalityCollector;

        let mut left = CardinalityCollector::default();
        left.insert("a");
        left.insert("b");

        let mut right = CardinalityCollector::default();
        right.insert("b");
        right.insert("c");

        left.merge_fruits(right).unwrap();
        let estimate = left.finalize().unwrap();
        assert_eq!(estimate, 3.0);
    }

    /// Verifies that merging two small sketches (both in List/Set coupon mode)
    /// produces an exact result — i.e. the HllUnion does not unnecessarily
    /// promote to the full HLL array when the combined cardinality is small.
    #[test]
    fn cardinality_collector_merge_stays_exact_for_small_sets() {
        use super::CardinalityCollector;

        let mut left = CardinalityCollector::default();
        for i in 0u64..50 {
            left.insert(i);
        }

        let mut right = CardinalityCollector::default();
        for i in 30u64..100 {
            right.insert(i);
        }

        left.merge_fruits(right).unwrap();
        let estimate = left.finalize().unwrap();
        // 100 distinct values (0..100). Both sketches are in Set mode (< 192 coupons),
        // so the union should stay in coupon mode and give an exact count.
        assert_eq!(estimate, 100.0);
    }

    #[test]
    fn cardinality_collector_serialize_deserialize_binary() {
        use datasketches::hll::HllSketch;

        use super::CardinalityCollector;

        let mut collector = CardinalityCollector::default();
        collector.insert("apple");
        collector.insert("banana");
        collector.insert("cherry");

        let bytes = collector.to_sketch_bytes();
        let deserialized = HllSketch::deserialize(&bytes).unwrap();
        assert!((deserialized.estimate() - 3.0).abs() < 0.01);
    }

    /// Tests that the `missing` parameter correctly counts a single empty document
    /// for both u64 and str columns.
    #[test]
    fn cardinality_aggregation_missing_value_single_empty_doc() {
        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_u64_field("id", FAST);
        let name_field = schema_builder.add_text_field("name", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests().unwrap();
        writer
            .add_document(doc!(id_field=>1u64,name_field=>"some_name"))
            .unwrap();
        writer.add_document(doc!()).unwrap();
        writer.commit().unwrap();

        {
            // int colum with missing value non redundant
            let agg_req: Aggregations = serde_json::from_value(json!({
                "cardinality": {
                    "cardinality": {
                        "field": "id",
                        "missing": 42u64
                    },
                }
            }))
            .unwrap();
            let res = exec_request(agg_req, &index).unwrap();
            assert_eq!(res["cardinality"]["value"], 2.0);
        }

        {
            // int colum with missing value redundant
            let agg_req: Aggregations = serde_json::from_value(json!({
                "cardinality": {
                    "cardinality": {
                        "field": "id",
                        "missing": 1u64
                    },
                }
            }))
            .unwrap();
            let res = exec_request(agg_req, &index).unwrap();
            assert_eq!(res["cardinality"]["value"], 1.0);
        }

        {
            // str colum with missing value non redundant
            // With more than one segment, this is not well handled.
            let agg_req: Aggregations = serde_json::from_value(json!({
                "cardinality": {
                    "cardinality": {
                        "field": "name",
                        "missing": "other_name"
                    },
                }
            }))
            .unwrap();
            let res = exec_request(agg_req, &index).unwrap();
            assert_eq!(res["cardinality"]["value"], 2.0);
        }

        {
            // str colum with missing value redundant
            let agg_req: Aggregations = serde_json::from_value(json!({
                "cardinality": {
                    "cardinality": {
                        "field": "name",
                        "missing": "some_name"
                    },
                }
            }))
            .unwrap();
            let res = exec_request(agg_req, &index).unwrap();
            assert_eq!(res["cardinality"]["value"], 1.0);
        }

        {
            // str column with missing value with a number type.
            let agg_req: Aggregations = serde_json::from_value(json!({
                "cardinality": {
                    "cardinality": {
                        "field": "name",
                        "missing": 3,
                    },
                }
            }))
            .unwrap();
            let res = exec_request(agg_req, &index).unwrap();
            assert_eq!(res["cardinality"]["value"], 2.0);
        }
    }

    #[test]
    fn cardinality_collector_salt_differentiates_types() {
        use super::CardinalityCollector;

        // Without salt, same u64 value from different column types would collide
        let mut collector_bool = CardinalityCollector::new(5); // e.g. ColumnType::Bool
        collector_bool.insert(0u64); // false
        collector_bool.insert(1u64); // true

        let mut collector_i64 = CardinalityCollector::new(2); // e.g. ColumnType::I64
        collector_i64.insert(0u64);
        collector_i64.insert(1u64);

        // Merge them
        collector_bool.merge_fruits(collector_i64).unwrap();
        let estimate = collector_bool.finalize().unwrap();
        // Should be 4 because salt makes (5, 0) != (2, 0) and (5, 1) != (2, 1)
        assert_eq!(estimate, 4.0);
    }
}
