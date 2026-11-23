//! Cardinality aggregation.
//!
//!   * [`str_collector`] holds the `ColumnType::Str` segment collector, which accumulates term
//!     ordinals and resolves them to HLL coupons at finalization.
//!   * [`numeric_collector`] holds the segment collector for every other column type, which feeds
//!     the HLL sketch directly during collection.
//!   * [`missing_collector`] handles the `missing` key when a segment has multiple typed columns
//!     for the same logical field (e.g. a dynamic JSON path).
//!
//! All segment collectors hash values through the canonical coupon functions in this module and
//! converge on the same [`IntermediateMetricResult::Cardinality`] payload, so results merge
//! consistently across segments, column types, and `missing` keys through
//! [`CardinalityCollector::merge_fruits`].

mod missing_collector;
mod numeric_collector;
mod str_collector;
mod term_ord_accumulator;

use columnar::{ColumnType, StrColumn};
use common::BitSet;
use datasketches::hll::{Coupon, HllSketch, HllType, HllUnion};
pub(crate) use missing_collector::{
    MissingCardinalityAggReqData, SegmentMissingCardinalityCollector,
};
pub(crate) use numeric_collector::SegmentNumericCardinalityCollector;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
pub(crate) use str_collector::SegmentStrCardinalityCollector;
pub(crate) use term_ord_accumulator::{TermOrdSet, BITSET_MAX_TERM_ORD};

use crate::aggregation::segment_agg_result::SegmentAggregationCollector;
use crate::aggregation::value_source::ValueSource;
use crate::aggregation::*;

/// Log2 of the number of registers for the HLL sketch.
/// 2^11 = 2048 registers, giving ~2.3% relative error and ~1KB per sketch (Hll4).
const LG_K: u8 = 11;

// Domain tags written as byte 0 of every hashed payload so distinct value domains can never
// produce the same byte stream in MurmurHash3.
//
// Numeric values share one unified mathematical domain partitioned into three disjoint
// representations:
//   - `SALT_NUM_NONNEG`: exact integers in `[0, 2^64 - 1]` (lossless `u64`, covering `u64`,
//     non-negative `i64`, `0.0`/`-0.0`, and exact integral `f64` in `[0, 2^64)`).
//   - `SALT_NUM_NEG`: exact negative integers in `[-2^63, -1]` (lossless `i64`, covering
//     negative `i64` and exact integral `f64` in `[-2^63, -1]`).
//   - `SALT_NUM_FLOAT`: non-integral finite `f64`, finite `f64` outside `[-2^63, 2^64)`, `±Inf`,
//     and canonicalized `NaN`.
const SALT_NUM_NONNEG: u8 = 0;
const SALT_NUM_NEG: u8 = 1;
const SALT_NUM_FLOAT: u8 = 2;
const SALT_STR: u8 = 3;
const SALT_BOOL: u8 = 4;
const SALT_IP_ADDR: u8 = 5;
const SALT_DATETIME: u8 = 6;

/// Exact IEEE-754 value `-2^63 == i64::MIN as f64`.
const NEG_TWO_POW_63: f64 = -9_223_372_036_854_775_808.0;
/// Exact IEEE-754 value `2^64`. Every finite `f64` strictly below this bound that is also
/// non-negative and integral fits losslessly in `u64` (its maximum is `2^64 - 2048 < u64::MAX`).
/// Using `< TWO_POW_64` avoids the `u64::MAX as f64 == 2^64` rounding hazard.
const TWO_POW_64: f64 = 18_446_744_073_709_551_616.0;
/// Canonical quiet-NaN bit pattern so all NaN payloads hash identically.
const CANONICAL_NAN_BITS: u64 = f64::NAN.to_bits();

#[inline(always)]
fn coupon_from_tagged_u64(tag: u8, val: u64) -> Coupon {
    let mut bytes = [0u8; 9];
    bytes[0] = tag;
    bytes[1..9].copy_from_slice(&val.to_le_bytes());
    Coupon::from_value(bytes)
}

#[inline(always)]
fn coupon_from_tagged_u128(tag: u8, val: u128) -> Coupon {
    let mut bytes = [0u8; 17];
    bytes[0] = tag;
    bytes[1..17].copy_from_slice(&val.to_le_bytes());
    Coupon::from_value(bytes)
}

/// Computes the canonical HLL coupon for an unsigned 64-bit integer.
#[inline(always)]
pub(crate) fn coupon_from_u64(val: u64) -> Coupon {
    coupon_from_tagged_u64(SALT_NUM_NONNEG, val)
}

/// Computes the canonical HLL coupon for a signed 64-bit integer.
///
/// Non-negative values share the exact representation of `u64` so cross-segment `I64` and `U64`
/// columns count equal values once without losing 64-bit precision.
#[inline(always)]
pub(crate) fn coupon_from_i64(val: i64) -> Coupon {
    if val >= 0 {
        coupon_from_tagged_u64(SALT_NUM_NONNEG, val as u64)
    } else {
        coupon_from_tagged_u64(SALT_NUM_NEG, val as u64)
    }
}

/// Computes the canonical HLL coupon for an `f64`.
///
/// Exact integer floats in `[-2^63, 2^64)` (including `-0.0` -> `0`) map to the same canonical
/// integer coupon as `i64`/`u64`. Fractional, out-of-range, `±Inf`, and canonicalized `NaN`
/// values use the separate float tag so they never collide with integers.
#[inline(always)]
pub(crate) fn coupon_from_f64(val: f64) -> Coupon {
    if val >= 0.0 {
        if val < TWO_POW_64 && val.trunc() == val {
            return coupon_from_tagged_u64(SALT_NUM_NONNEG, val as u64);
        }
    } else if val >= NEG_TWO_POW_63 && val.trunc() == val {
        return coupon_from_tagged_u64(SALT_NUM_NEG, (val as i64) as u64);
    }
    let bits = if val.is_nan() {
        CANONICAL_NAN_BITS
    } else {
        val.to_bits()
    };
    coupon_from_tagged_u64(SALT_NUM_FLOAT, bits)
}

/// Computes the canonical HLL coupon for a UTF-8 string byte slice.
#[inline(always)]
pub(crate) fn coupon_from_str_bytes(bytes: &[u8]) -> Coupon {
    Coupon::from_value((SALT_STR, bytes))
}

/// Computes the canonical HLL coupon for a boolean value.
#[inline(always)]
pub(crate) fn coupon_from_bool(val: bool) -> Coupon {
    Coupon::from_value([SALT_BOOL, u8::from(val)])
}

/// Computes the canonical HLL coupon for a `DateTime` nanosecond timestamp.
#[inline(always)]
pub(crate) fn coupon_from_datetime_nanos(timestamp_nanos: i64) -> Coupon {
    coupon_from_tagged_u64(SALT_DATETIME, timestamp_nanos as u64)
}

/// Computes the canonical HLL coupon for an IPv6 / IPv4-mapped `u128` address.
#[inline(always)]
pub(crate) fn coupon_from_ip_addr(ip_u128: u128) -> Coupon {
    coupon_from_tagged_u128(SALT_IP_ADDR, ip_u128)
}

/// Computes the canonical HLL coupon for a `missing` [`Key`], independent of the segment's
/// column type.
#[inline]
pub(crate) fn coupon_from_key(key: &Key) -> Coupon {
    match key {
        Key::Str(val) => coupon_from_str_bytes(val.as_bytes()),
        Key::I64(val) => coupon_from_i64(*val),
        Key::U64(val) => coupon_from_u64(*val),
        Key::F64(val) => coupon_from_f64(*val),
    }
}

/// Upper bound (inclusive) on valid term ordinals in a physical `StrColumn`.
///
/// Using `max(ords.max_value(), dict.num_terms().saturating_sub(1))` guarantees that the
/// missing-value sentinel `max_term_ord_inclusive + 1` is always `>= dict.num_terms()`.
pub(crate) fn str_max_term_ord_inclusive(str_col: &StrColumn) -> u64 {
    str_col
        .ords()
        .max_value()
        .max((str_col.dictionary().num_terms() as u64).saturating_sub(1))
}

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

/// Contains all information required by the segment cardinality collectors to perform the
/// cardinality aggregation on a segment.
pub(crate) struct CardinalityAggReqData {
    /// The column accessor to access the fast field values.
    pub(crate) accessor: Box<dyn ValueSource>,
    /// For `Str` columns, either an existing dictionary ordinal (when `missing` is a `Key::Str`
    /// already present in the dictionary) or the sentinel `max_term_ord_inclusive + 1`.
    /// Always `None` for non-`Str` columns, which insert `missing_coupon` directly.
    pub(crate) missing_value_for_accessor: Option<u64>,
    /// Pre-computed canonical HLL coupon for the configured `missing` key when handled directly
    /// by this collector.
    pub(crate) missing_coupon: Option<Coupon>,
    /// The name of the aggregation.
    pub(crate) name: String,
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

#[derive(Clone, Debug)]
/// The cardinality collector used during segment collection and for merging results.
/// Uses Apache DataSketches HLL (lg_k=11, Hll8) for compact binary serialization
/// and cross-language compatibility (e.g. Java `datasketches` library).
pub struct CardinalityCollector {
    sketch: HllSketch,
}

impl Default for CardinalityCollector {
    fn default() -> Self {
        Self {
            sketch: HllSketch::new(LG_K, HllType::Hll8)
                .expect("LG_K is within the supported range"),
        }
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
        Ok(Self { sketch })
    }
}

impl CardinalityCollector {
    #[inline(always)]
    fn insert_coupon(&mut self, coupon: Coupon) {
        self.sketch.update_with_coupon(coupon);
    }

    #[inline(always)]
    fn insert_u64(&mut self, value: u64) {
        self.insert_coupon(coupon_from_u64(value));
    }

    #[inline(always)]
    fn insert_i64(&mut self, value: i64) {
        self.insert_coupon(coupon_from_i64(value));
    }

    #[inline(always)]
    fn insert_f64(&mut self, value: f64) {
        self.insert_coupon(coupon_from_f64(value));
    }

    #[inline(always)]
    fn insert_bool(&mut self, value: bool) {
        self.insert_coupon(coupon_from_bool(value));
    }

    #[inline(always)]
    fn insert_datetime_nanos(&mut self, timestamp_nanos: i64) {
        self.insert_coupon(coupon_from_datetime_nanos(timestamp_nanos));
    }

    #[inline(always)]
    fn insert_ip_addr(&mut self, ip_u128: u128) {
        self.insert_coupon(coupon_from_ip_addr(ip_u128));
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
        if right.sketch.is_empty() {
            return Ok(());
        }
        if self.sketch.is_empty() {
            self.sketch = right.sketch;
            return Ok(());
        }
        let mut union = HllUnion::new(LG_K).expect("LG_K is within the supported range");
        union.update(&self.sketch);
        union.update(&right.sketch);
        self.sketch = union.to_sketch(HllType::Hll8);
        Ok(())
    }
}

/// Builds the segment collector for a cardinality aggregation.
///
/// str and non-str columns use two entirely different collectors: str
/// accumulates term ordinals and resolves them into HLL coupons at
/// finalization, non-str feeds the HLL sketch directly. Both produce the same
/// [`IntermediateMetricResult::Cardinality`], so they merge uniformly.
pub(crate) fn build_segment_cardinality_collector(
    req_data: CardinalityAggReqData,
) -> crate::Result<Box<dyn SegmentAggregationCollector>> {
    if req_data.accessor.column_type() != ColumnType::Str {
        return Ok(Box::new(SegmentNumericCardinalityCollector::from_req(
            req_data,
        )?));
    }
    // For str columns, we need to collect the set of term ordinals encountered.
    //
    // In the case of physical columns, the dictionary is known so we have an upper bound
    // on the term ord that will be encountered.
    //
    // We choose a different representation depending on the maximum number of terms:
    //   * small (< BITSET_MAX_TERM_ORD): `BitSet`, pre-allocated.
    //   * large: `TermOrdSet` (sparse HashSet that promotes to a paged bitset).
    Ok(
        if let Some(str_col) = req_data.accessor.as_physical_str_column() {
            let max_term_ord_inclusive = str_max_term_ord_inclusive(str_col);
            if max_term_ord_inclusive < BITSET_MAX_TERM_ORD {
                Box::new(SegmentStrCardinalityCollector::<BitSet>::from_req(
                    req_data,
                    max_term_ord_inclusive,
                ))
            } else {
                Box::new(SegmentStrCardinalityCollector::<TermOrdSet>::from_req(
                    req_data,
                    max_term_ord_inclusive,
                ))
            }
        } else {
            // For the moment, for non physical str column, we rely on the regular
            // term ord set.
            // TODO fix me! Although we do not have control over this, these values are likely
            // created using a inc counter, with their unicity given by a hashmap already.
            // We probably do not need another HashSet.
            Box::new(SegmentStrCardinalityCollector::<TermOrdSet>::from_req(
                req_data,
                u64::MAX,
            ))
        },
    )
}

#[cfg(test)]
mod tests {
    use columnar::MonotonicallyMappableToU64;

    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::tests::{exec_request, get_test_index_from_terms};
    use crate::schema::{Schema, FAST, STRING};
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
    fn cardinality_aggregation_bytes_excluded_from_accessors() -> crate::Result<()> {
        // `Bytes` columns are opened as raw per-segment dictionary ordinals (like `Str`), but
        // unlike `Str`, cardinality has no dictionary-resolution path for them: it would hash
        // the raw ordinal directly, which are segment dependant. Ignore bytes values instead of
        // counting them wrong.
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_bytes_field("raw", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer = index.writer_for_tests()?;
            writer.add_document(doc!(field => vec![1u8]))?;
            writer.add_document(doc!(field => vec![2u8]))?;
            writer.commit()?;
            writer.add_document(doc!(field => vec![3u8]))?;
            writer.add_document(doc!(field => vec![4u8]))?;
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "cardinality": {
                "cardinality": {
                    "field": "raw"
                },
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        assert_eq!(res["cardinality"]["value"], 0.0);

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

    /// A JSON path that resolves to both a Str column and a numeric column
    /// produces two collector instances per segment — one with `Str` buckets
    /// and one with `Numeric` buckets. Their `IntermediateMetricResult`s must
    /// merge into the union cardinality.
    #[test]
    fn cardinality_aggregation_json_str_and_numeric() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_json_field("json", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer = index.writer_for_tests()?;
            writer.add_document(doc!(field => json!({"value": "hello"})))?;
            writer.add_document(doc!(field => json!({"value": "world"})))?;
            writer.add_document(doc!(field => json!({"value": "hello"})))?; // dup str
            writer.add_document(doc!(field => json!({"value": i64::from_u64(7u64)})))?;
            writer.add_document(doc!(field => json!({"value": i64::from_u64(42u64)})))?;
            writer.add_document(doc!(field => json!({"value": i64::from_u64(7u64)})))?; // dup num
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
        // 4 distinct values: "hello", "world", 7, 42.
        assert_eq!(res["cardinality"]["value"], 4.0);

        Ok(())
    }

    #[test]
    fn cardinality_collector_serde_roundtrip() {
        use super::{coupon_from_str_bytes, CardinalityCollector};

        let mut collector = CardinalityCollector::default();
        collector.insert_coupon(coupon_from_str_bytes(b"hello"));
        collector.insert_coupon(coupon_from_str_bytes(b"world"));
        collector.insert_coupon(coupon_from_str_bytes(b"hello")); // duplicate

        let serialized = serde_json::to_vec(&collector).unwrap();
        let deserialized: CardinalityCollector = serde_json::from_slice(&serialized).unwrap();

        let original_estimate = collector.finalize().unwrap();
        let roundtrip_estimate = deserialized.finalize().unwrap();
        assert_eq!(original_estimate, roundtrip_estimate);
        assert_eq!(original_estimate, 2.0);
    }

    #[test]
    fn cardinality_collector_merge() {
        use super::{coupon_from_str_bytes, CardinalityCollector};

        let mut left = CardinalityCollector::default();
        left.insert_coupon(coupon_from_str_bytes(b"a"));
        left.insert_coupon(coupon_from_str_bytes(b"b"));

        let mut right = CardinalityCollector::default();
        right.insert_coupon(coupon_from_str_bytes(b"b"));
        right.insert_coupon(coupon_from_str_bytes(b"c"));

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
            left.insert_u64(i);
        }

        let mut right = CardinalityCollector::default();
        for i in 30u64..100 {
            right.insert_u64(i);
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

        use super::{coupon_from_str_bytes, CardinalityCollector};

        let mut collector = CardinalityCollector::default();
        collector.insert_coupon(coupon_from_str_bytes(b"apple"));
        collector.insert_coupon(coupon_from_str_bytes(b"banana"));
        collector.insert_coupon(coupon_from_str_bytes(b"cherry"));

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

        // Same numeric/boolean values from different domains must not collide.
        let mut collector_bool = CardinalityCollector::default();
        collector_bool.insert_bool(false);
        collector_bool.insert_bool(true);

        let mut collector_i64 = CardinalityCollector::default();
        collector_i64.insert_i64(0);
        collector_i64.insert_i64(1);

        // Merge them
        collector_bool.merge_fruits(collector_i64).unwrap();
        let estimate = collector_bool.finalize().unwrap();
        // Should be 4 because domain tags keep bool false/true distinct from integer 0/1.
        assert_eq!(estimate, 4.0);
    }

    #[test]
    fn canonical_coupons_numeric_equivalence_and_precision() {
        use super::{
            coupon_from_bool, coupon_from_datetime_nanos, coupon_from_f64, coupon_from_i64,
            coupon_from_ip_addr, coupon_from_key, coupon_from_str_bytes, coupon_from_u64,
        };
        use crate::aggregation::Key;

        // 0 across all numeric representations including -0.0
        let c0 = coupon_from_u64(0);
        assert_eq!(c0, coupon_from_i64(0));
        assert_eq!(c0, coupon_from_f64(0.0));
        assert_eq!(c0, coupon_from_f64(-0.0));
        assert_eq!(c0, coupon_from_key(&Key::U64(0)));
        assert_eq!(c0, coupon_from_key(&Key::I64(0)));
        assert_eq!(c0, coupon_from_key(&Key::F64(0.0)));
        assert_eq!(c0, coupon_from_key(&Key::F64(-0.0)));

        // Positive and negative integers
        assert_eq!(coupon_from_i64(1), coupon_from_u64(1));
        assert_eq!(coupon_from_i64(1), coupon_from_f64(1.0));
        assert_eq!(coupon_from_i64(-42), coupon_from_f64(-42.0));
        assert_eq!(coupon_from_i64(i64::MAX), coupon_from_u64(i64::MAX as u64));

        // Exact integer floats at 64-bit boundaries
        assert_eq!(coupon_from_i64(i64::MIN), coupon_from_f64(i64::MIN as f64));
        assert_eq!(
            coupon_from_u64(1u64 << 63),
            coupon_from_f64(9_223_372_036_854_775_808.0)
        );
        // Largest f64 strictly below 2^64 is 2^64 - 2048.
        let max_exact_u64_float = f64::from_bits(0x43ef_ffff_ffff_ffff);
        assert_eq!(
            coupon_from_u64(18_446_744_073_709_549_568),
            coupon_from_f64(max_exact_u64_float)
        );
        // 2^64 (f64) must NOT collide with u64::MAX (2^64 - 1).
        assert_ne!(
            coupon_from_u64(u64::MAX),
            coupon_from_f64(18_446_744_073_709_551_616.0)
        );
        // -2^63 - 2048 (f64) must NOT collide with i64::MIN (-2^63).
        assert_ne!(
            coupon_from_i64(i64::MIN),
            coupon_from_f64(-9_223_372_036_854_777_856.0)
        );
        // 2^53 vs 2^53 + 1 preserve 64-bit precision.
        assert_eq!(
            coupon_from_i64(9_007_199_254_740_992),
            coupon_from_f64(9_007_199_254_740_992.0)
        );
        assert_ne!(
            coupon_from_i64(9_007_199_254_740_992),
            coupon_from_i64(9_007_199_254_740_993)
        );
        assert_ne!(coupon_from_u64(u64::MAX), coupon_from_u64(u64::MAX - 1));
        assert_ne!(coupon_from_i64(i64::MIN), coupon_from_i64(i64::MIN + 1));

        // Canonical NaN (quiet, negative, payload, signaling) and distinct ±Inf
        assert_eq!(coupon_from_f64(f64::NAN), coupon_from_f64(-f64::NAN));
        assert_eq!(
            coupon_from_f64(f64::NAN),
            coupon_from_f64(f64::from_bits(0x7ff8_0000_0000_0001))
        );
        assert_eq!(
            coupon_from_f64(f64::NAN),
            coupon_from_f64(f64::from_bits(0x7ff0_0000_0000_0001))
        );
        assert_ne!(
            coupon_from_f64(f64::INFINITY),
            coupon_from_f64(f64::NEG_INFINITY)
        );
        assert_ne!(coupon_from_f64(f64::NAN), coupon_from_f64(f64::INFINITY));
        // Subnormals and extreme finite floats stay distinct from zero and each other.
        let pos_subnormal = f64::from_bits(1);
        let neg_subnormal = f64::from_bits(0x8000_0000_0000_0001);
        assert_ne!(coupon_from_f64(pos_subnormal), coupon_from_u64(0));
        assert_ne!(coupon_from_f64(neg_subnormal), coupon_from_u64(0));
        assert_ne!(
            coupon_from_f64(pos_subnormal),
            coupon_from_f64(neg_subnormal)
        );
        assert_ne!(coupon_from_f64(f64::MAX), coupon_from_f64(f64::MIN));
        assert_ne!(coupon_from_f64(f64::MAX), coupon_from_f64(f64::INFINITY));

        // Cross-domain isolation (including former unsalted 1-byte string "\0" vs u64 0).
        let domains = [
            coupon_from_u64(0),
            coupon_from_str_bytes(b"\0"),
            coupon_from_str_bytes(b"0"),
            coupon_from_bool(false),
            coupon_from_datetime_nanos(0),
            coupon_from_ip_addr(0),
        ];
        for i in 0..domains.len() {
            for j in (i + 1)..domains.len() {
                assert_ne!(domains[i], domains[j], "collision between {i} and {j}");
            }
        }
        let ones = [
            coupon_from_u64(1),
            coupon_from_str_bytes(b"1"),
            coupon_from_bool(true),
            coupon_from_datetime_nanos(1),
            coupon_from_ip_addr(1),
        ];
        for i in 0..ones.len() {
            for j in (i + 1)..ones.len() {
                assert_ne!(ones[i], ones[j], "collision between {i} and {j}");
            }
        }
    }

    #[test]
    fn cardinality_missing_numeric_on_str_segment_matches_numeric_segment() -> crate::Result<()> {
        use crate::aggregation::tests::exec_request_with_query;
        use crate::indexer::NoMergePolicy;

        let mut schema_builder = Schema::builder();
        let tag = schema_builder.add_text_field("tag", STRING);
        let json_field = schema_builder.add_json_field("json", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer = index.writer_with_num_threads(1, 20_000_000)?;
            writer.set_merge_policy(Box::new(NoMergePolicy));

            // Segment 1: only a Str column exists for `json.v`.
            // Doc with `"hello"` is excluded by query so only the missing doc contributes.
            writer.add_document(doc!(tag => "skip", json_field => json!({"v": "hello"})))?;
            writer.add_document(doc!(tag => "keep"))?;
            writer.commit()?;

            // Segment 2: I64 column with value `1`.
            writer.add_document(doc!(tag => "keep", json_field => json!({"v": 1i64})))?;
            writer.commit()?;
        }

        // Exact issue #2891 case: Str segment missing=1 + I64 segment value=1 -> cardinality 1.
        let agg_req: Aggregations = serde_json::from_value(json!({
            "cardinality": {
                "cardinality": {
                    "field": "json.v",
                    "missing": 1
                }
            }
        }))
        .unwrap();
        let res = exec_request_with_query(agg_req, &index, Some(("tag", "keep")))?;
        assert_eq!(res["cardinality"]["value"], 1.0);

        // Including the `"hello"` doc yields 2 distinct values: `"hello"` and `1`.
        let agg_req_all: Aggregations = serde_json::from_value(json!({
            "cardinality": {
                "cardinality": {
                    "field": "json.v",
                    "missing": 1.0
                }
            }
        }))
        .unwrap();
        let res_all = exec_request(agg_req_all, &index)?;
        assert_eq!(res_all["cardinality"]["value"], 2.0);

        Ok(())
    }

    #[test]
    fn cardinality_cross_segment_heterogeneous_numeric_types() -> crate::Result<()> {
        use crate::indexer::NoMergePolicy;

        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer = index.writer_with_num_threads(1, 20_000_000)?;
            writer.set_merge_policy(Box::new(NoMergePolicy));

            // Segment 1: detected as I64.
            for v in [-5i64, 0, 1, 42] {
                writer.add_document(doc!(json_field => json!({"num": v})))?;
            }
            writer.commit()?;

            // Segment 2: detected as U64 (u64::MAX forces unsigned).
            for v in [0u64, 1, 42, u64::MAX] {
                writer.add_document(doc!(json_field => json!({"num": v})))?;
            }
            writer.commit()?;

            // Segment 3: detected as F64 (fractional and huge values force float).
            for v in [-5.0f64, -0.0, 0.0, 1.0, 42.0, 1.5, 1e30] {
                writer.add_document(doc!(json_field => json!({"num": v})))?;
            }
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "cardinality": {
                "cardinality": {
                    "field": "json.num"
                }
            }
        }))
        .unwrap();
        let res = exec_request(agg_req, &index)?;
        // Distinct mathematical values: {-5, 0, 1, 42, u64::MAX, 1.5, 1e30} = 7.
        assert_eq!(res["cardinality"]["value"], 7.0);

        Ok(())
    }

    #[test]
    fn cardinality_f64_special_values_zero_inf_nan() -> crate::Result<()> {
        use crate::indexer::NoMergePolicy;

        let mut schema_builder = Schema::builder();
        let f_field = schema_builder.add_f64_field("val", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer = index.writer_with_num_threads(1, 20_000_000)?;
            writer.set_merge_policy(Box::new(NoMergePolicy));

            writer.add_document(doc!(f_field => 0.0f64))?;
            writer.add_document(doc!(f_field => -0.0f64))?;
            writer.add_document(doc!(f_field => f64::INFINITY))?;
            writer.add_document(doc!(f_field => f64::NEG_INFINITY))?;
            writer.add_document(doc!(f_field => f64::NAN))?;
            writer.commit()?;

            writer.add_document(doc!(f_field => -0.0f64))?;
            writer.add_document(doc!(f_field => f64::from_bits(0x7ff8_0000_0000_0001)))?;
            writer.add_document(doc!(f_field => -f64::NAN))?;
            writer.add_document(doc!(f_field => f64::INFINITY))?;
            writer.add_document(doc!(f_field => 1.5f64))?;
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "cardinality": {
                "cardinality": {
                    "field": "val"
                }
            }
        }))
        .unwrap();
        let res = exec_request(agg_req, &index)?;
        // Distinct: {0, +Inf, -Inf, canonical NaN, 1.5} = 5.
        assert_eq!(res["cardinality"]["value"], 5.0);

        Ok(())
    }

    #[test]
    fn cardinality_64bit_integer_precision_preserved() -> crate::Result<()> {
        use crate::indexer::NoMergePolicy;

        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer = index.writer_with_num_threads(1, 20_000_000)?;
            writer.set_merge_policy(Box::new(NoMergePolicy));

            // Segment 1 (I64): 2^53, 2^53 + 1, i64::MIN, i64::MIN + 1, i64::MAX
            writer.add_document(doc!(json_field => json!({"n": 9_007_199_254_740_992i64})))?;
            writer.add_document(doc!(json_field => json!({"n": 9_007_199_254_740_993i64})))?;
            writer.add_document(doc!(json_field => json!({"n": i64::MIN})))?;
            writer.add_document(doc!(json_field => json!({"n": i64::MIN + 1})))?;
            writer.add_document(doc!(json_field => json!({"n": i64::MAX})))?;
            writer.commit()?;

            // Segment 2 (U64): i64::MAX (dup), 2^63, u64::MAX - 1, u64::MAX
            writer.add_document(doc!(json_field => json!({"n": i64::MAX as u64})))?;
            writer.add_document(doc!(json_field => json!({"n": 1u64 << 63})))?;
            writer.add_document(doc!(json_field => json!({"n": u64::MAX - 1})))?;
            writer.add_document(doc!(json_field => json!({"n": u64::MAX})))?;
            writer.commit()?;

            // Segment 3 (F64):
            // - 2^53 (exact match with seg 1)
            // - -2^63 == i64::MIN (exact match with seg 1)
            // - 2^63 (exact match with seg 2)
            // - 2^64 == 18446744073709551616.0 (strictly > u64::MAX, distinct!)
            // - -2^63 - 2048 == -9223372036854777856.0 (strictly < i64::MIN, distinct!)
            // - 1.5 (fractional, distinct!)
            writer.add_document(doc!(json_field => json!({"n": 9_007_199_254_740_992.0f64})))?;
            writer
                .add_document(doc!(json_field => json!({"n": -9_223_372_036_854_775_808.0f64})))?;
            writer
                .add_document(doc!(json_field => json!({"n": 9_223_372_036_854_775_808.0f64})))?;
            writer
                .add_document(doc!(json_field => json!({"n": 18_446_744_073_709_551_616.0f64})))?;
            writer
                .add_document(doc!(json_field => json!({"n": -9_223_372_036_854_777_856.0f64})))?;
            writer.add_document(doc!(json_field => json!({"n": 1.5f64})))?;
            writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "cardinality": {
                "cardinality": {
                    "field": "json.n"
                }
            }
        }))
        .unwrap();
        let res = exec_request(agg_req, &index)?;
        // Distinct:
        // { 2^53, 2^53+1, i64::MIN, i64::MIN+1, i64::MAX, 2^63, u64::MAX-1, u64::MAX,
        //   2^64, -2^63-2048, 1.5 } = 11.
        assert_eq!(res["cardinality"]["value"], 11.0);

        Ok(())
    }

    #[test]
    fn cardinality_missing_numeric_no_precision_loss_or_truncation() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let i64_field = schema_builder.add_i64_field("i", FAST);
        let u64_field = schema_builder.add_u64_field("u", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer = index.writer_for_tests()?;
            writer.add_document(doc!(i64_field => 9_007_199_254_740_992i64, u64_field => 0u64))?;
            writer.add_document(doc!(i64_field => 1i64, u64_field => u64::MAX))?;
            writer.add_document(doc!())?;
            writer.commit()?;
        }

        // Large 64-bit integer missing must not round through f64 to 2^53.
        let req_prec: Aggregations = serde_json::from_value(json!({
            "c": { "cardinality": { "field": "i", "missing": 9_007_199_254_740_993i64 } }
        }))
        .unwrap();
        assert_eq!(exec_request(req_prec, &index)?["c"]["value"], 3.0);

        // Fractional missing 1.5 on I64 column containing 1 must not truncate to 1.
        let req_frac: Aggregations = serde_json::from_value(json!({
            "c": { "cardinality": { "field": "i", "missing": 1.5 } }
        }))
        .unwrap();
        assert_eq!(exec_request(req_frac, &index)?["c"]["value"], 3.0);

        // Negative missing -5 on U64 column containing 0 must not saturate to 0.
        let req_neg: Aggregations = serde_json::from_value(json!({
            "c": { "cardinality": { "field": "u", "missing": -5 } }
        }))
        .unwrap();
        assert_eq!(exec_request(req_neg, &index)?["c"]["value"], 3.0);

        // u64::MAX - 1 missing on U64 column containing u64::MAX must stay distinct.
        let req_u64: Aggregations = serde_json::from_value(json!({
            "c": { "cardinality": { "field": "u", "missing": u64::MAX - 1 } }
        }))
        .unwrap();
        assert_eq!(exec_request(req_u64, &index)?["c"]["value"], 3.0);

        Ok(())
    }

    #[test]
    fn cardinality_cross_type_non_collision() -> crate::Result<()> {
        use std::net::IpAddr;
        use std::str::FromStr;

        use crate::indexer::NoMergePolicy;
        use crate::schema::IntoIpv6Addr;

        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", FAST);
        let bool_field = schema_builder.add_bool_field("b", FAST);
        let date_field = schema_builder.add_date_field("d", FAST);
        let ip_field = schema_builder.add_ip_addr_field("ip", FAST);
        let i64_field = schema_builder.add_i64_field("num", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer = index.writer_with_num_threads(1, 20_000_000)?;
            writer.set_merge_policy(Box::new(NoMergePolicy));

            // JSON field across types: 1 (i64), "1" (str), true (bool)
            writer.add_document(doc!(
                json_field => json!({"v": 1i64}),
                bool_field => true,
                date_field => common::DateTime::from_timestamp_nanos(1),
                ip_field => IpAddr::from_str("::1").unwrap().into_ipv6_addr(),
                i64_field => 1i64,
            ))?;
            writer.add_document(doc!(json_field => json!({"v": "1"})))?;
            writer.commit()?;

            writer.add_document(doc!(json_field => json!({"v": true})))?;
            writer.add_document(doc!(json_field => json!({"v": 1.0f64})))?;
            writer.commit()?;
        }

        // 1i64 and 1.0f64 collapse to one numeric value, while "1" and true stay distinct -> 3.
        let req_json: Aggregations = serde_json::from_value(json!({
            "c": { "cardinality": { "field": "json.v" } }
        }))
        .unwrap();
        assert_eq!(exec_request(req_json, &index)?["c"]["value"], 3.0);

        // String missing "1" on numeric column containing 1 -> 2 ("1" != 1).
        let req_str_on_num: Aggregations = serde_json::from_value(json!({
            "c": { "cardinality": { "field": "num", "missing": "1" } }
        }))
        .unwrap();
        assert_eq!(exec_request(req_str_on_num, &index)?["c"]["value"], 2.0);

        // Numeric missing 1 on Bool (true), DateTime (1ns), IpAddr (::1) -> each has 2 distinct.
        for field in ["b", "d", "ip"] {
            let req: Aggregations = serde_json::from_value(json!({
                "c": { "cardinality": { "field": field, "missing": 1 } }
            }))
            .unwrap();
            assert_eq!(
                exec_request(req, &index)?["c"]["value"],
                2.0,
                "field={field}"
            );
        }

        Ok(())
    }

    #[test]
    fn cardinality_missing_across_multi_column_and_absent_segments() -> crate::Result<()> {
        use crate::indexer::NoMergePolicy;

        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer = index.writer_with_num_threads(1, 20_000_000)?;
            writer.set_merge_policy(Box::new(NoMergePolicy));

            // Seg 1: Str column only + missing doc
            writer.add_document(doc!(json_field => json!({"v": "a"})))?;
            writer.add_document(doc!())?;
            writer.commit()?;

            // Seg 2: I64 column only + missing doc
            writer.add_document(doc!(json_field => json!({"v": 1i64})))?;
            writer.add_document(doc!())?;
            writer.commit()?;

            // Seg 3: BOTH Str and I64 columns in same segment + missing doc
            writer.add_document(doc!(json_field => json!({"v": "b"})))?;
            writer.add_document(doc!(json_field => json!({"v": 2i64})))?;
            writer.add_document(doc!())?;
            writer.commit()?;

            // Seg 4: Absent field entirely (missing-only segment)
            writer.add_document(doc!())?;
            writer.commit()?;
        }

        for (missing_json, expected) in [
            (json!(1), 4.0),         // {"a", "b", 1, 2}
            (json!(1.0), 4.0),       // {"a", "b", 1, 2}
            (json!("a"), 4.0),       // {"a", "b", 1, 2}
            (json!("unknown"), 5.0), // {"a", "b", 1, 2, "unknown"}
            (json!(99.5), 5.0),      // {"a", "b", 1, 2, 99.5}
        ] {
            let req: Aggregations = serde_json::from_value(json!({
                "c": {
                    "cardinality": {
                        "field": "json.v",
                        "missing": missing_json
                    }
                }
            }))
            .unwrap();
            assert_eq!(
                exec_request(req, &index)?["c"]["value"],
                expected,
                "missing={missing_json}"
            );
        }

        // Multi-column segment with NO missing docs must NOT emit missing key.
        let mut schema_builder2 = Schema::builder();
        let json_field2 = schema_builder2.add_json_field("json", FAST);
        let index2 = Index::create_in_ram(schema_builder2.build());
        {
            let mut writer = index2.writer_for_tests()?;
            writer.add_document(doc!(json_field2 => json!({"v": "a"})))?;
            writer.add_document(doc!(json_field2 => json!({"v": 1i64})))?;
            writer.commit()?;
        }
        let req_no_missing: Aggregations = serde_json::from_value(json!({
            "c": {
                "cardinality": {
                    "field": "json.v",
                    "missing": "SHOULD_NOT_APPEAR"
                }
            }
        }))
        .unwrap();
        assert_eq!(exec_request(req_no_missing, &index2)?["c"]["value"], 2.0);

        Ok(())
    }

    #[test]
    fn cardinality_str_term_ord_set_with_numeric_missing() -> crate::Result<()> {
        use crate::aggregation::tests::exec_request_with_query;
        use crate::indexer::NoMergePolicy;

        let mut schema_builder = Schema::builder();
        let tag = schema_builder.add_text_field("tag", STRING);
        let json_field = schema_builder.add_json_field("json", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer = index.writer_with_num_threads(1, 20_000_000)?;
            writer.set_merge_policy(Box::new(NoMergePolicy));

            // Segment 1: > BITSET_MAX_TERM_ORD (256) distinct strings forces the `TermOrdSet` path.
            for i in 0..300 {
                writer.add_document(
                    doc!(tag => "skip", json_field => json!({"v": format!("term_{i:03}")})),
                )?;
            }
            writer.add_document(doc!(tag => "keep"))?;
            writer.commit()?;

            // Segment 2: numeric column containing 42.
            writer.add_document(doc!(tag => "keep", json_field => json!({"v": 42i64})))?;
            writer.commit()?;
        }

        let req: Aggregations = serde_json::from_value(json!({
            "c": { "cardinality": { "field": "json.v", "missing": 42 } }
        }))
        .unwrap();
        // Both segments contribute canonical `42` -> exact cardinality 1.
        assert_eq!(
            exec_request_with_query(req, &index, Some(("tag", "keep")))?["c"]["value"],
            1.0
        );

        Ok(())
    }

    #[test]
    fn cardinality_nested_buckets_mixed_and_multi_column_missing() -> crate::Result<()> {
        use crate::indexer::NoMergePolicy;

        let mut schema_builder = Schema::builder();
        let group = schema_builder.add_text_field("group", STRING | FAST);
        let json_field = schema_builder.add_json_field("json", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer = index.writer_with_num_threads(1, 20_000_000)?;
            writer.set_merge_policy(Box::new(NoMergePolicy));

            // Segment 1: multi-column (Str + I64) in the same segment.
            writer.add_document(doc!(group => "g1", json_field => json!({"v": "a"})))?;
            writer.add_document(doc!(group => "g1"))?; // missing -> 1
            writer.add_document(doc!(group => "g2", json_field => json!({"v": "b"})))?;
            writer.add_document(doc!(group => "g3", json_field => json!({"v": "c"})))?;
            writer.add_document(doc!(group => "g3", json_field => json!({"v": 2i64})))?;
            writer.add_document(doc!(group => "g3"))?; // missing -> 1
            writer.commit()?;

            // Segment 2: U64 + F64 split across segments.
            writer.add_document(doc!(group => "g1", json_field => json!({"v": 1u64})))?;
            writer.add_document(doc!(group => "g3", json_field => json!({"v": 1u64})))?;
            writer.add_document(doc!(group => "g3", json_field => json!({"v": u64::MAX})))?;
            writer.commit()?;

            // Segment 3: F64 only.
            writer.add_document(doc!(group => "g2", json_field => json!({"v": 1.5f64})))?;
            writer.commit()?;
        }

        let req: Aggregations = serde_json::from_value(json!({
            "by_group": {
                "terms": { "field": "group" },
                "aggs": {
                    "card": {
                        "cardinality": {
                            "field": "json.v",
                            "missing": 1
                        }
                    }
                }
            }
        }))
        .unwrap();
        let res = exec_request(req, &index)?;
        let buckets = res["by_group"]["buckets"].as_array().unwrap();
        let by_key: std::collections::HashMap<&str, f64> = buckets
            .iter()
            .map(|b| {
                (
                    b["key"].as_str().unwrap(),
                    b["card"]["value"].as_f64().unwrap(),
                )
            })
            .collect();
        // g1: {"a", 1} = 2 (missing 1 merges with 1u64 from seg 2)
        assert_eq!(by_key["g1"], 2.0);
        // g2: {"b", 1.5} = 2 (no missing docs in g2)
        assert_eq!(by_key["g2"], 2.0);
        // g3: {"c", 2, 1, u64::MAX} = 4 (missing 1 merges with 1u64 from seg 2)
        assert_eq!(by_key["g3"], 4.0);

        Ok(())
    }

    #[test]
    fn cardinality_multivalued_field_with_missing() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let scores = schema_builder.add_i64_field("scores", FAST);
        let index_with_missing = Index::create_in_ram(schema_builder.build());
        {
            let mut writer = index_with_missing.writer_for_tests()?;
            writer.add_document(doc!(scores => 10i64, scores => 20i64))?;
            writer.add_document(doc!())?; // missing doc even though total vals >= num_docs
            writer.add_document(doc!(scores => 20i64, scores => 30i64))?;
            writer.commit()?;
        }

        let req: Aggregations = serde_json::from_value(json!({
            "c": { "cardinality": { "field": "scores", "missing": 99 } }
        }))
        .unwrap();
        assert_eq!(
            exec_request(req.clone(), &index_with_missing)?["c"]["value"],
            4.0
        );

        // When every multivalued doc has at least one value, missing must not be inserted.
        let mut schema_builder2 = Schema::builder();
        let scores2 = schema_builder2.add_i64_field("scores", FAST);
        let index_no_missing = Index::create_in_ram(schema_builder2.build());
        {
            let mut writer = index_no_missing.writer_for_tests()?;
            writer.add_document(doc!(scores2 => 10i64, scores2 => 20i64))?;
            writer.add_document(doc!(scores2 => 20i64, scores2 => 30i64))?;
            writer.commit()?;
        }
        assert_eq!(exec_request(req, &index_no_missing)?["c"]["value"], 3.0);

        Ok(())
    }

    #[test]
    fn cardinality_missing_consistent_with_missing_only_segment_for_all_types() -> crate::Result<()>
    {
        use std::net::IpAddr;
        use std::str::FromStr;

        use crate::indexer::NoMergePolicy;
        use crate::schema::IntoIpv6Addr;

        let mut schema_builder = Schema::builder();
        let s_field = schema_builder.add_text_field("s", STRING | FAST);
        let i_field = schema_builder.add_i64_field("i", FAST);
        let u_field = schema_builder.add_u64_field("u", FAST);
        let f_field = schema_builder.add_f64_field("f", FAST);
        let b_field = schema_builder.add_bool_field("b", FAST);
        let d_field = schema_builder.add_date_field("d", FAST);
        let ip_field = schema_builder.add_ip_addr_field("ip", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut writer = index.writer_with_num_threads(1, 20_000_000)?;
            writer.set_merge_policy(Box::new(NoMergePolicy));

            // Seg 1: populated values + one empty doc.
            writer.add_document(doc!(
                s_field => "hello",
                i_field => 1i64,
                u_field => 1u64,
                f_field => 1.0f64,
                b_field => true,
                d_field => common::DateTime::from_timestamp_nanos(100),
                ip_field => IpAddr::from_str("::1").unwrap().into_ipv6_addr(),
            ))?;
            writer.add_document(doc!())?;
            writer.commit()?;

            // Seg 2: missing-only segment (empty doc only).
            writer.add_document(doc!())?;
            writer.commit()?;
        }

        // Missing value in seg 1 and seg 2 must merge into 1 distinct missing value -> total 2.
        for (field, missing) in [
            ("s", json!("missing_str")),
            ("s", json!(7)),
            ("i", json!(7)),
            ("i", json!("missing_str")),
            ("u", json!(7)),
            ("f", json!(7.5)),
            ("b", json!("missing_bool")),
            ("b", json!(1)),
            ("d", json!("missing_date")),
            ("d", json!(100)),
            ("ip", json!("missing_ip")),
            ("ip", json!(1)),
        ] {
            let req: Aggregations = serde_json::from_value(json!({
                "c": { "cardinality": { "field": field, "missing": missing } }
            }))
            .unwrap();
            assert_eq!(
                exec_request(req, &index)?["c"]["value"],
                2.0,
                "field={field}, missing={missing}"
            );
        }

        // Redundant missing across segments must stay 1.
        for (field, missing) in [
            ("s", json!("hello")),
            ("i", json!(1)),
            ("i", json!(1.0)),
            ("u", json!(1)),
            ("f", json!(1)),
        ] {
            let req: Aggregations = serde_json::from_value(json!({
                "c": { "cardinality": { "field": field, "missing": missing } }
            }))
            .unwrap();
            assert_eq!(
                exec_request(req, &index)?["c"]["value"],
                1.0,
                "redundant field={field}, missing={missing}"
            );
        }

        Ok(())
    }

    #[test]
    fn cardinality_distributed_serde_roundtrip_cross_type() -> crate::Result<()> {
        use crate::aggregation::intermediate_agg_result::IntermediateAggregationResults;
        use crate::aggregation::DistributedAggregationCollector;
        use crate::indexer::NoMergePolicy;
        use crate::query::AllQuery;

        let build_single_seg = |doc_val: serde_json::Value| -> crate::Result<Index> {
            let mut sb = Schema::builder();
            let jf = sb.add_json_field("json", FAST);
            let idx = Index::create_in_ram(sb.build());
            let mut w = idx.writer_with_num_threads(1, 20_000_000)?;
            w.set_merge_policy(Box::new(NoMergePolicy));
            if !doc_val.is_null() {
                w.add_document(doc!(jf => doc_val))?;
            } else {
                w.add_document(doc!())?;
            }
            w.commit()?;
            Ok(idx)
        };

        let idx_str = {
            let mut sb = Schema::builder();
            let jf = sb.add_json_field("json", FAST);
            let idx = Index::create_in_ram(sb.build());
            let mut w = idx.writer_with_num_threads(1, 20_000_000)?;
            w.add_document(doc!(jf => json!({"v": "hello"})))?;
            w.add_document(doc!())?;
            w.commit()?;
            idx
        };
        let idx_i64 = build_single_seg(json!({"v": 1i64}))?;
        let idx_u64 = {
            let mut sb = Schema::builder();
            let jf = sb.add_json_field("json", FAST);
            let idx = Index::create_in_ram(sb.build());
            let mut w = idx.writer_with_num_threads(1, 20_000_000)?;
            w.add_document(doc!(jf => json!({"v": 1u64})))?;
            w.add_document(doc!(jf => json!({"v": u64::MAX})))?;
            w.commit()?;
            idx
        };
        let idx_f64 = build_single_seg(json!({"v": 1.0f64}))?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "c": { "cardinality": { "field": "json.v", "missing": 1 } }
        }))
        .unwrap();

        let mut merged = IntermediateAggregationResults::default();
        for idx in [&idx_str, &idx_i64, &idx_u64, &idx_f64] {
            let collector =
                DistributedAggregationCollector::from_aggs(agg_req.clone(), Default::default());
            let intermediate = idx.reader()?.searcher().search(&AllQuery, &collector)?;
            // Round-trip through postcard to mirror distributed Quickwit merges.
            let bytes = postcard::to_stdvec(&intermediate).unwrap();
            let decoded: IntermediateAggregationResults = postcard::from_bytes(&bytes).unwrap();
            merged.merge_fruits(decoded)?;
        }

        let final_res = merged.into_final_result(agg_req, Default::default())?;
        let val = serde_json::to_value(&final_res)?;
        // Distinct values: {"hello", 1, u64::MAX} = 3.
        assert_eq!(val["c"]["value"], 3.0);

        Ok(())
    }
}
