use std::cmp::Ordering;
use std::fmt::Debug;
use std::net::{AddrParseError, IpAddr};
use std::str::FromStr;

use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use crate::aggregation::bucket::multi_terms::numeric_types::num_cmp::{
    cmp_i64_f64, cmp_i64_u64, cmp_u64_f64,
};
use crate::aggregation::bucket::multi_terms::sources::{BucketSource, KeyPage};
use crate::aggregation::bucket::{MultiTermsAggReqData, MultiTermsAggregation, Order};
use crate::aggregation::deserialize_f64;
use crate::aggregation::intermediate_agg_result::CompositeIntermediateKey;
use crate::schema::IntoIpv6Addr;
use crate::TantivyError;

/// Position of missing keys in the ordering.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum MissingOrder {
    /// Missing keys appear first in ascending order, last in descending order.
    #[default]
    Default,
    /// Missing keys should appear first.
    First,
    /// Missing keys should appear last.
    Last,
}

fn agg_source_default_order() -> Order {
    Order::Asc
}

/// Term source for a composite aggregation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TermCompositeAggregationSource {
    /// The name used to refer to this source in the composite key.
    #[serde(skip)]
    pub name: String,
    /// The field to aggregate on.
    pub field: String,
    /// The order for this source.
    #[serde(default = "agg_source_default_order")]
    pub order: Order,
    /// Whether to create a `null` bucket for documents without value for this
    /// field. By default documents without a value are ignored.
    #[serde(default)]
    pub missing_bucket: bool,
    /// Whether missing keys should appear first or last.
    #[serde(default)]
    pub missing_order: MissingOrder,
}

/// Histogram source for a composite aggregation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HistogramCompositeAggregationSource {
    /// The name used to refer to this source in the composite key.
    #[serde(skip)]
    pub name: String,
    /// The field to aggregate on.
    pub field: String,
    /// The interval for the histogram. For datetime fields, this is expressed.
    /// in milliseconds.
    #[serde(deserialize_with = "deserialize_f64")]
    pub interval: f64,
    /// The order for this source.
    #[serde(default = "agg_source_default_order")]
    pub order: Order,
    /// Whether to create a `null` bucket for documents without value for this
    /// field. By default documents without a value are ignored.
    #[serde(default)]
    pub missing_bucket: bool,
    /// Whether missing keys should appear first or last.
    #[serde(default)]
    pub missing_order: MissingOrder,
}

/// Calendar intervals supported for date histogram sources
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CalendarInterval {
    /// A year between Jan 1st and Dec 31st, taking into account leap years.
    Year,
    /// A month between the 1st and the last day of the month.
    Month,
    /// A week between Monday and Sunday.
    Week,
}

/// Date histogram source for a composite aggregation.
///
/// Time zone not supported yet. Every interval is aligned on UTC.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DateHistogramCompositeAggregationSource {
    /// The name used to refer to this source in the composite key.
    #[serde(skip)]
    pub name: String,
    /// The field to aggregate on.
    pub field: String,
    /// The fixed interval for the histogram. Either this or `calendar_interval`.
    /// must be set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fixed_interval: Option<String>,
    /// The calendar adjusted interval for the histogram. Either this or
    /// `fixed_interval` must be set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub calendar_interval: Option<CalendarInterval>,
    /// The order for this source.
    #[serde(default = "agg_source_default_order")]
    pub order: Order,
    /// Whether to create a `null` bucket for documents without value for this
    /// field. By default documents without a value are ignored. Not supported
    /// in Elasticsearch.
    #[serde(default)]
    pub missing_bucket: bool,
    /// Whether missing keys should appear first or last.
    #[serde(default)]
    pub missing_order: MissingOrder,
}

/// Source for the composite aggregation. A composite aggregation can have
/// multiple sources.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompositeAggregationSource {
    /// Terms source.
    Terms(TermCompositeAggregationSource),
    /// Histogram source.
    Histogram(HistogramCompositeAggregationSource),
    /// Date histogram source.
    DateHistogram(DateHistogramCompositeAggregationSource),
}

impl CompositeAggregationSource {
    pub(crate) fn field(&self) -> &str {
        match self {
            CompositeAggregationSource::Terms(source) => &source.field,
            CompositeAggregationSource::Histogram(source) => &source.field,
            CompositeAggregationSource::DateHistogram(source) => &source.field,
        }
    }

    pub(crate) fn name(&self) -> &str {
        match self {
            CompositeAggregationSource::Terms(source) => &source.name,
            CompositeAggregationSource::Histogram(source) => &source.name,
            CompositeAggregationSource::DateHistogram(source) => &source.name,
        }
    }

    pub(crate) fn order(&self) -> Order {
        match self {
            CompositeAggregationSource::Terms(source) => source.order,
            CompositeAggregationSource::Histogram(source) => source.order,
            CompositeAggregationSource::DateHistogram(source) => source.order,
        }
    }

    pub(crate) fn missing_order(&self) -> MissingOrder {
        match self {
            CompositeAggregationSource::Terms(source) => source.missing_order,
            CompositeAggregationSource::Histogram(source) => source.missing_order,
            CompositeAggregationSource::DateHistogram(source) => source.missing_order,
        }
    }

    pub(crate) fn missing_bucket(&self) -> bool {
        match self {
            CompositeAggregationSource::Terms(source) => source.missing_bucket,
            CompositeAggregationSource::Histogram(source) => source.missing_bucket,
            CompositeAggregationSource::DateHistogram(source) => source.missing_bucket,
        }
    }
}

/// Returns a page of buckets keyed by named term, histogram, or date-histogram sources.
///
/// Keys follow each source's configured order. The `after` key excludes that key and
/// all keys preceding it from the next page.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(
    try_from = "CompositeAggregationSerde",
    into = "CompositeAggregationSerde"
)]
pub struct CompositeAggregation {
    /// The fields and bucketting strategies.
    pub sources: Vec<CompositeAggregationSource>,
    /// Number of buckets to return (page size).
    pub size: u32,
    /// The key of the previous page's last bucket.
    pub after: FxHashMap<String, AfterKey>,
}

impl CompositeAggregation {
    /// Builds this segment's multi-terms request with mapped sources and key pagination.
    pub(crate) fn build_multi_terms_segment_request(
        &self,
        name: &str,
        reader: &crate::SegmentReader,
    ) -> crate::Result<MultiTermsAggReqData> {
        let sources = self
            .sources
            .iter()
            .map(|source| {
                BucketSource::build(
                    reader,
                    source,
                    self.after.get(source.name()).map(|key| &key.0),
                )
            })
            .collect::<crate::Result<Vec<_>>>()?;
        Ok(MultiTermsAggReqData {
            name: name.to_string(),
            req: MultiTermsAggregation::default(),
            fields: Vec::new(),
            missing_accessors: Vec::new(),
            is_top_level: false,
            key_page: Some(KeyPage {
                sources,
                size: self.size as usize,
            }),
        })
    }
}

#[derive(Serialize, Deserialize)]
struct CompositeAggregationSerde {
    sources: Vec<FxHashMap<String, CompositeAggregationSource>>,
    size: u32,
    #[serde(default, skip_serializing_if = "FxHashMap::is_empty")]
    after: FxHashMap<String, AfterKey>,
}

impl TryFrom<CompositeAggregationSerde> for CompositeAggregation {
    type Error = TantivyError;

    fn try_from(value: CompositeAggregationSerde) -> Result<Self, Self::Error> {
        let mut sources = Vec::with_capacity(value.sources.len());
        for map in value.sources {
            if map.len() != 1 {
                return Err(TantivyError::InvalidArgument(
                    "each composite source must have exactly one named entry".to_string(),
                ));
            }
            let (name, mut source) = map.into_iter().next().unwrap();
            match &mut source {
                CompositeAggregationSource::Terms(source) => {
                    source.name = name;
                }
                CompositeAggregationSource::Histogram(source) => {
                    source.name = name;
                }
                CompositeAggregationSource::DateHistogram(source) => {
                    source.name = name;
                }
            }
            sources.push(source);
        }
        Ok(CompositeAggregation {
            sources,
            size: value.size,
            after: value.after,
        })
    }
}

impl From<CompositeAggregation> for CompositeAggregationSerde {
    fn from(value: CompositeAggregation) -> Self {
        let mut serde_sources = Vec::with_capacity(value.sources.len());
        for source in value.sources {
            let (name, stored_source) = match source {
                CompositeAggregationSource::Terms(source) => {
                    let name = source.name.clone();
                    // name field is #[serde(skip)] so it won't be serialized inside the value
                    (name, CompositeAggregationSource::Terms(source))
                }
                CompositeAggregationSource::Histogram(source) => {
                    let name = source.name.clone();
                    (name, CompositeAggregationSource::Histogram(source))
                }
                CompositeAggregationSource::DateHistogram(source) => {
                    let name = source.name.clone();
                    (name, CompositeAggregationSource::DateHistogram(source))
                }
            };
            let mut map = FxHashMap::default();
            map.insert(name, stored_source);
            serde_sources.push(map);
        }
        CompositeAggregationSerde {
            sources: serde_sources,
            size: value.size,
            after: value.after,
        }
    }
}

/// After key is a string that encodes the intermediate composite key as "<type>:<value>"
/// A wrapper type for CompositeIntermediateKey that serializes/deserializes
/// to/from the "<type>:<value>" format.
#[derive(Clone, Debug, PartialEq)]
pub struct AfterKey(pub CompositeIntermediateKey);

impl Serialize for AfterKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let s = match &self.0 {
            CompositeIntermediateKey::Bool(b) => format!("bool:{}", b),
            CompositeIntermediateKey::Str(s) => format!("str:{}", s),
            CompositeIntermediateKey::I64(i) => format!("i64:{}", i),
            CompositeIntermediateKey::U64(u) => format!("u64:{}", u),
            CompositeIntermediateKey::F64(f) => format!("f64:{}", f),
            CompositeIntermediateKey::IpAddr(ip) => format!("ip:{}", ip),
            CompositeIntermediateKey::DateTime(dt) => format!("dt:{}", dt),
            CompositeIntermediateKey::Null => "null:".to_string(),
        };
        serializer.serialize_str(&s)
    }
}

impl<'de> Deserialize<'de> for AfterKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        let s = String::deserialize(deserializer)?;
        let parts: Vec<&str> = s.splitn(2, ':').collect();

        if parts.len() != 2 {
            return Err(serde::de::Error::custom("invalid after key format"));
        }

        let key = match parts[0] {
            "bool" => {
                let b = parts[1].parse::<bool>().map_err(|e| {
                    serde::de::Error::custom(format!("failed to parse bool: {}", e))
                })?;
                CompositeIntermediateKey::Bool(b)
            }
            "str" => CompositeIntermediateKey::Str(parts[1].to_string()),
            "i64" => {
                let i = parts[1]
                    .parse::<i64>()
                    .map_err(|e| serde::de::Error::custom(format!("failed to parse i64: {}", e)))?;
                CompositeIntermediateKey::I64(i)
            }
            "u64" => {
                let u = parts[1]
                    .parse::<u64>()
                    .map_err(|e| serde::de::Error::custom(format!("failed to parse u64: {}", e)))?;
                CompositeIntermediateKey::U64(u)
            }
            "f64" => {
                let f = parts[1]
                    .parse::<f64>()
                    .map_err(|e| serde::de::Error::custom(format!("failed to parse f64: {}", e)))?;
                if f.is_nan() {
                    return Err(serde::de::Error::custom(
                        "NaN is not supported in after key",
                    ));
                }
                CompositeIntermediateKey::F64(f)
            }
            "ip" => {
                let ip = IpAddr::from_str(parts[1]).map_err(|e: AddrParseError| {
                    serde::de::Error::custom(format!("failed to parse ip: {}", e))
                })?;
                CompositeIntermediateKey::IpAddr(ip.into_ipv6_addr())
            }
            "dt" => {
                let dt = parts[1].parse::<i64>().map_err(|e| {
                    serde::de::Error::custom(format!("failed to parse datetime: {}", e))
                })?;
                CompositeIntermediateKey::DateTime(dt)
            }
            "null" => CompositeIntermediateKey::Null,
            _ => {
                return Err(serde::de::Error::custom("invalid after key type"));
            }
        };

        Ok(AfterKey(key))
    }
}

impl From<CompositeIntermediateKey> for AfterKey {
    fn from(key: CompositeIntermediateKey) -> Self {
        AfterKey(key)
    }
}

impl From<AfterKey> for CompositeIntermediateKey {
    fn from(value: AfterKey) -> Self {
        value.0
    }
}

/// Calculates the ordering between intermediate keys.
pub fn composite_intermediate_key_ordering(
    left_opt: &CompositeIntermediateKey,
    right_opt: &CompositeIntermediateKey,
    order: Order,
    missing_order: MissingOrder,
) -> crate::Result<Ordering> {
    use CompositeIntermediateKey as CIKey;
    let mut forced_ordering = false;
    let asc_ordering = match (left_opt, right_opt) {
        // null comparisons
        (CIKey::Null, CIKey::Null) => Ordering::Equal,
        (CIKey::Null, _) => {
            forced_ordering = missing_order != MissingOrder::Default;
            match missing_order {
                MissingOrder::First => Ordering::Less,
                MissingOrder::Last => Ordering::Greater,
                MissingOrder::Default => Ordering::Less,
            }
        }
        (_, CIKey::Null) => {
            forced_ordering = missing_order != MissingOrder::Default;
            match missing_order {
                MissingOrder::First => Ordering::Greater,
                MissingOrder::Last => Ordering::Less,
                MissingOrder::Default => Ordering::Greater,
            }
        }
        // same type comparisons
        (CIKey::Bool(left), CIKey::Bool(right)) => left.cmp(right),
        (CIKey::I64(left), CIKey::I64(right)) => left.cmp(right),
        (CIKey::Str(left), CIKey::Str(right)) => left.cmp(right),
        (CIKey::IpAddr(left), CIKey::IpAddr(right)) => left.cmp(right),
        (CIKey::DateTime(left), CIKey::DateTime(right)) => left.cmp(right),
        (CIKey::U64(left), CIKey::U64(right)) => left.cmp(right),
        (CIKey::F64(f), CIKey::F64(_)) | (CIKey::F64(_), CIKey::F64(f)) if f.is_nan() => {
            return Err(TantivyError::InvalidArgument(
                "NaN comparison is not supported".to_string(),
            ))
        }
        (CIKey::F64(left), CIKey::F64(right)) => left.partial_cmp(right).unwrap_or(Ordering::Equal),
        // numeric cross-type comparisons
        (CIKey::F64(left), CIKey::I64(right)) => cmp_i64_f64(*right, *left)?.reverse(),
        (CIKey::F64(left), CIKey::U64(right)) => cmp_u64_f64(*right, *left)?.reverse(),
        (CIKey::I64(left), CIKey::F64(right)) => cmp_i64_f64(*left, *right)?,
        (CIKey::I64(left), CIKey::U64(right)) => cmp_i64_u64(*left, *right),
        (CIKey::U64(left), CIKey::I64(right)) => cmp_i64_u64(*right, *left).reverse(),
        (CIKey::U64(left), CIKey::F64(right)) => cmp_u64_f64(*left, *right)?,
        // other cross-type comparisons
        (left, right) => left.key_type().cmp(&right.key_type()),
    };
    if !forced_ordering && order == Order::Desc {
        Ok(asc_ordering.reverse())
    } else {
        Ok(asc_ordering)
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use serde_json::json;
    use time::format_description::well_known::Rfc3339;
    use time::OffsetDateTime;

    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::tests::exec_request;
    use crate::schema::{Schema, FAST, STRING};
    use crate::Index;

    fn datetime_from_iso_str(date_str: &str) -> common::DateTime {
        let dt = OffsetDateTime::parse(date_str, &Rfc3339)
            .unwrap_or_else(|_| panic!("Failed to parse date: {}", date_str));
        let timestamp_secs = dt.unix_timestamp_nanos();
        common::DateTime::from_timestamp_nanos(timestamp_secs as i64)
    }

    fn ms_timestamp_from_iso_str(date_str: &str) -> i64 {
        let dt = OffsetDateTime::parse(date_str, &Rfc3339)
            .unwrap_or_else(|_| panic!("Failed to parse date: {}", date_str));
        (dt.unix_timestamp_nanos() / 1_000_000) as i64
    }

    /// Runs the query and compares the result buckets to the expected buckets,
    /// then run the same query with a all possible `after` keys and different
    /// page sizes.
    fn exec_and_assert_all_paginations(
        index: &Index,
        composite_agg_sources: serde_json::Value,
        expected_buckets: serde_json::Value,
    ) {
        let expected_buckets_vec = expected_buckets.as_array().unwrap();

        for page_size in 1..=expected_buckets_vec.len() {
            let page_count = expected_buckets_vec.len().div_ceil(page_size);
            let mut after_key = None;
            for page_idx in 0..page_count {
                let mut agg_req_json = json!({
                    "my_composite": {
                        "composite": {
                            "sources": composite_agg_sources,
                            "size": page_size,
                        }
                    }
                });
                if page_idx > 0 {
                    agg_req_json["my_composite"]["composite"]["after"] = after_key.take().unwrap();
                }
                let agg_req: Aggregations = serde_json::from_value(agg_req_json).unwrap();
                let res = exec_request(agg_req.clone(), index).unwrap();
                let expected_page_buckets = &expected_buckets_vec[page_idx * page_size
                    ..std::cmp::min((page_idx + 1) * page_size, expected_buckets_vec.len())];
                assert_eq!(
                    &res["my_composite"]["buckets"],
                    &json!(expected_page_buckets),
                    "pagination failed at page {}, size {}, query: {:?}",
                    page_idx,
                    page_size,
                    agg_req,
                );
                assert!(
                    res["my_composite"].get("after_key").is_some(),
                    "expected after_key on every non-empty page"
                );
                after_key = Some(res["my_composite"]["after_key"].clone());
            }
            // Using the after_key from the last page must yield an empty page.
            let agg_req_json = json!({
                "my_composite": {
                    "composite": {
                        "sources": composite_agg_sources,
                        "size": page_size,
                        "after": after_key,
                    }
                }
            });
            let agg_req: Aggregations = serde_json::from_value(agg_req_json).unwrap();
            let res = exec_request(agg_req.clone(), index).unwrap();
            assert_eq!(
                res["my_composite"]["buckets"],
                json!([]),
                "expected no buckets when using after_key from last page, query: {:?}",
                agg_req
            );
        }
    }

    fn composite_aggregation_test(merge_segments: bool) -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let string_field = schema_builder.add_text_field("string_id", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.add_document(doc!(string_field => "terma"))?;
            index_writer.add_document(doc!(string_field => "termb"))?;
            index_writer.add_document(doc!(string_field => "termc"))?;
            index_writer.add_document(doc!(string_field => "terma"))?;
            index_writer.commit()?;
            index_writer.add_document(doc!(string_field => "terma"))?;
            index_writer.add_document(doc!(string_field => "terma"))?;
            index_writer.add_document(doc!(string_field => "termb"))?;
            index_writer.add_document(doc!(string_field => "terma"))?;
            index_writer.commit()?;
            if merge_segments {
                index_writer.wait_merging_threads()?;
            }
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_composite": {
                "composite": {
                    "sources": [
                        {"term1": {"terms": {"field": "string_id"}}}
                    ],
                    "size": 10
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        let buckets = &res["my_composite"]["buckets"];

        assert_eq!(
            buckets,
            &json!([
                {"key": {"term1": "terma"}, "doc_count": 5},
                {"key": {"term1": "termb"}, "doc_count": 2},
                {"key": {"term1": "termc"}, "doc_count": 1}
            ])
        );

        Ok(())
    }

    #[test]
    fn composite_aggregation_term_single_segment() -> crate::Result<()> {
        composite_aggregation_test(true)
    }

    #[test]
    fn composite_aggregation_term_multi_segment() -> crate::Result<()> {
        composite_aggregation_test(false)
    }

    fn composite_aggregation_term_size_limit(merge_segments: bool) -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let string_field = schema_builder.add_text_field("string_id", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.add_document(doc!(string_field => "terma"))?;
            index_writer.add_document(doc!(string_field => "termb"))?;
            index_writer.commit()?;
            index_writer.add_document(doc!(string_field => "termc"))?;
            index_writer.add_document(doc!(string_field => "termd"))?;
            index_writer.add_document(doc!(string_field => "terme"))?;
            index_writer.commit()?;
            if merge_segments {
                index_writer.wait_merging_threads()?;
            }
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_composite": {
                "composite": {
                    "sources": [
                        {"myterm": {"terms": {"field": "string_id"}}}
                    ],
                    "size": 3
                }
            }
        }))
        .unwrap();
        let res = exec_request(agg_req, &index)?;
        let buckets = &res["my_composite"]["buckets"];
        // Should only return 3 buckets due to size limit
        assert_eq!(
            buckets,
            &json!([
                {"key": {"myterm": "terma"}, "doc_count": 1},
                {"key": {"myterm": "termb"}, "doc_count": 1},
                {"key": {"myterm": "termc"}, "doc_count": 1}
            ])
        );

        // next page
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_composite": {
                "composite": {
                    "sources": [
                        {"myterm": {"terms": {"field": "string_id"}}}
                    ],
                    "size": 3,
                    "after":  &res["my_composite"]["after_key"]
                }
            }
        }))
        .unwrap();
        let res = exec_request(agg_req, &index)?;
        let buckets = &res["my_composite"]["buckets"];
        assert_eq!(
            buckets,
            &json!([
                {"key": {"myterm": "termd"}, "doc_count": 1},
                {"key": {"myterm": "terme"}, "doc_count": 1}
            ])
        );

        // paginating past last page should be empty
        let agg_req_json = json!({
            "my_composite": {
                "composite": {
                    "sources": [
                        {"myterm": {"terms": {"field": "string_id"}}}
                    ],
                    "size": 3,
                    "after":  &res["my_composite"]["after_key"]
                }
            }
        });
        let agg_req: Aggregations = serde_json::from_value(agg_req_json).unwrap();
        let res = exec_request(agg_req.clone(), &index).unwrap();
        assert!(res["my_composite"].get("after_key").is_none());
        assert_eq!(
            res["my_composite"]["buckets"],
            json!([]),
            "expected no buckets when using after_key from last page, query: {:?}",
            agg_req
        );
        Ok(())
    }

    #[test]
    fn composite_aggregation_term_size_limit_single_segment() -> crate::Result<()> {
        composite_aggregation_term_size_limit(true)
    }

    #[test]
    fn composite_aggregation_term_size_limit_multi_segment() -> crate::Result<()> {
        composite_aggregation_term_size_limit(false)
    }

    #[test]
    fn composite_aggregation_term_ordering() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let string_field = schema_builder.add_text_field("string_id", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.add_document(doc!(string_field => "zebra"))?;
            index_writer.add_document(doc!(string_field => "apple"))?;
            index_writer.add_document(doc!(string_field => "banana"))?;
            index_writer.add_document(doc!(string_field => "cherry"))?;
            index_writer.add_document(doc!(string_field => "dog"))?;
            index_writer.add_document(doc!(string_field => "elephant"))?;
            index_writer.add_document(doc!(string_field => "fox"))?;
            index_writer.add_document(doc!(string_field => "grape"))?;
            index_writer.commit()?;
        }

        // Test ascending order (default)
        let agg_req: Aggregations = serde_json::from_value(json!({
            "fruity_aggreg": {
                "composite": {
                    "sources": [
                        {"myterm": {"terms": {"field": "string_id", "order": "asc"}}}
                    ],
                    "size": 5
                }
            }
        }))
        .unwrap();
        let res = exec_request(agg_req, &index)?;
        let buckets = &res["fruity_aggreg"]["buckets"];
        // Should return only 5 buckets due to size limit, in ascending order
        assert_eq!(
            buckets,
            &json!([
                {"key": {"myterm": "apple"}, "doc_count": 1},
                {"key": {"myterm": "banana"}, "doc_count": 1},
                {"key": {"myterm": "cherry"}, "doc_count": 1},
                {"key": {"myterm": "dog"}, "doc_count": 1},
                {"key": {"myterm": "elephant"}, "doc_count": 1}
            ])
        );

        // Test descending order
        let agg_req: Aggregations = serde_json::from_value(json!({
            "fruity_aggreg": {
                "composite": {
                    "sources": [
                        {"myterm": {"terms": {"field": "string_id", "order": "desc"}}}
                    ],
                    "size": 5
                }
            }
        }))
        .unwrap();
        let res = exec_request(agg_req, &index)?;
        let buckets = &res["fruity_aggreg"]["buckets"];
        // Should return only 5 buckets due to size limit, in descending order
        assert_eq!(
            buckets,
            &json!([
                {"key": {"myterm": "zebra"}, "doc_count": 1},
                {"key": {"myterm": "grape"}, "doc_count": 1},
                {"key": {"myterm": "fox"}, "doc_count": 1},
                {"key": {"myterm": "elephant"}, "doc_count": 1},
                {"key": {"myterm": "dog"}, "doc_count": 1}
            ])
        );

        // next page in descending order
        let agg_req: Aggregations = serde_json::from_value(json!({
            "fruity_aggreg": {
                "composite": {
                    "sources": [
                        {"myterm": {"terms": {"field": "string_id", "order": "desc"}}}
                    ],
                    "size": 5,
                    "after":  &res["fruity_aggreg"]["after_key"]
                }
            }
        }))
        .unwrap();
        let res = exec_request(agg_req, &index)?;
        let buckets = &res["fruity_aggreg"]["buckets"];
        // Should return only 5 buckets due to size limit, in descending order
        assert_eq!(
            buckets,
            &json!([
                {"key": {"myterm": "cherry"}, "doc_count": 1},
                {"key": {"myterm": "banana"}, "doc_count": 1},
                {"key": {"myterm": "apple"}, "doc_count": 1}
            ])
        );
        assert_eq!(
            res["fruity_aggreg"]["after_key"],
            json!({"myterm": "str:apple"})
        );

        Ok(())
    }

    #[test]
    fn composite_aggregation_term_missing_values() -> crate::Result<()> {
        // Create index with some documents having missing values
        let mut schema_builder = Schema::builder();
        let string_field = schema_builder.add_text_field("string_id", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.add_document(doc!(string_field => "terma"))?;
            index_writer.add_document(doc!(string_field => "termb"))?;
            index_writer.add_document(doc!())?;
            index_writer.add_document(doc!(string_field => "terma"))?;
            index_writer.commit()?;
        }

        // Test without missing bucket (should ignore missing values)
        exec_and_assert_all_paginations(
            &index,
            json!([
                {"myterm": {"terms": {"field": "string_id", "missing_bucket": false}}}
            ]),
            json!([
                {"key": {"myterm": "terma"}, "doc_count": 2},
                {"key": {"myterm": "termb"}, "doc_count": 1}
            ]),
        );

        // Test with missing bucket enabled
        exec_and_assert_all_paginations(
            &index,
            json!([
                {"myterm": {"terms": {"field": "string_id", "missing_bucket": true}}}
            ]),
            // Should have 3 buckets including the missing bucket
            // Missing bucket should come first in ascending order by default
            json!([
                {"key": {"myterm": null}, "doc_count": 1},
                {"key": {"myterm": "terma"}, "doc_count": 2},
                {"key": {"myterm": "termb"}, "doc_count": 1}
            ]),
        );

        Ok(())
    }

    #[test]
    fn composite_pagination_respects_missing_order_for_every_source() -> crate::Result<()> {
        let mut schema = Schema::builder();
        let string = schema.add_text_field("string", STRING | FAST);
        let number = schema.add_u64_field("number", FAST);
        let date = schema.add_date_field("date", FAST);
        let index = Index::create_in_ram(schema.build());
        let mut writer = index.writer_for_tests()?;
        writer.add_document(doc!(string => "b", number => 23u64,
            date => common::DateTime::from_timestamp_secs(86_400)))?;
        writer.add_document(doc!())?;
        writer.add_document(doc!(string => "a", number => 12u64,
            date => common::DateTime::from_timestamp_secs(0)))?;
        writer.commit()?;

        for (kind, mut source, values) in [
            ("terms", json!({"field": "string"}), json!(["a", "b"])),
            (
                "histogram",
                json!({"field": "number", "interval": 10}),
                json!([10.0, 20.0]),
            ),
            (
                "date_histogram",
                json!({"field": "date", "fixed_interval": "1d"}),
                json!([0, 86_400_000]),
            ),
        ] {
            source["missing_bucket"] = json!(true);
            for order in ["asc", "desc"] {
                for missing_order in ["first", "last", "default"] {
                    source["order"] = json!(order);
                    source["missing_order"] = json!(missing_order);
                    let mut keys = values.as_array().unwrap().clone();
                    if order == "desc" {
                        keys.reverse();
                    }
                    let missing_first =
                        missing_order == "first" || (missing_order == "default" && order == "asc");
                    keys.insert(if missing_first { 0 } else { keys.len() }, json!(null));
                    let buckets: Vec<_> = keys
                        .into_iter()
                        .map(|key| json!({"key": {"value": key}, "doc_count": 1}))
                        .collect();
                    exec_and_assert_all_paginations(
                        &index,
                        json!([{"value": {(kind): source}}]),
                        json!(buckets),
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn composite_aggregation_term_multi_source() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let cat = schema_builder.add_text_field("category", STRING | FAST);
        let status = schema_builder.add_text_field("status", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.add_document(doc!(cat => "electronics", status => "active"))?;
            index_writer.add_document(doc!(cat => "electronics", status => "inactive"))?;
            index_writer.add_document(doc!(cat => "electronics", status => "active"))?;
            index_writer.add_document(doc!(cat => "books", status => "active"))?;
            index_writer.add_document(doc!(cat => "books", status => "inactive"))?;
            index_writer.add_document(doc!(cat => "clothing", status => "active"))?;
            index_writer.commit()?;
        }

        exec_and_assert_all_paginations(
            &index,
            json!([
                {"category": {"terms": {"field": "category"}}},
                {"status": {"terms": {"field": "status"}}}
            ]),
            // Should have composite keys with both dimensions in sorted order
            json!([
                {"key": {"category": "books", "status": "active"}, "doc_count": 1},
                {"key": {"category": "books", "status": "inactive"}, "doc_count": 1},
                {"key": {"category": "clothing", "status": "active"}, "doc_count": 1},
                {"key": {"category": "electronics", "status": "active"}, "doc_count": 2},
                {"key": {"category": "electronics", "status": "inactive"}, "doc_count": 1}
            ]),
        );

        Ok(())
    }

    #[test]
    fn composite_aggregation_term_multi_source_ordering() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let cat = schema_builder.add_text_field("category", STRING | FAST);
        let priority = schema_builder.add_text_field("priority", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.add_document(doc!(cat => "zebra", priority => "high"))?;
            index_writer.add_document(doc!(cat => "apple", priority => "low"))?;
            index_writer.add_document(doc!(cat => "zebra", priority => "low"))?;
            index_writer.add_document(doc!(cat => "apple", priority => "high"))?;
            index_writer.commit()?;
        }

        // Test with different ordering on different sources
        exec_and_assert_all_paginations(
            &index,
            json!([
                {"category": {"terms": {"field": "category", "order": "asc"}}},
                {"priority": {"terms": {"field": "priority", "order": "desc"}}}
            ]),
            json!([
                {"key": {"category": "apple", "priority": "low"}, "doc_count": 1},
                {"key": {"category": "apple", "priority": "high"}, "doc_count": 1},
                {"key": {"category": "zebra", "priority": "low"}, "doc_count": 1},
                {"key": {"category": "zebra", "priority": "high"}, "doc_count": 1}
            ]),
        );

        Ok(())
    }

    #[test]
    fn composite_aggregation_term_with_sub_aggregations() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let score_field = schema_builder.add_f64_field("score_f64", FAST);
        let string_field = schema_builder.add_text_field("string_id", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.add_document(doc!(score_field => 5.0f64, string_field => "terma"))?;
            index_writer.add_document(doc!(score_field => 2.0f64, string_field => "termb"))?;
            index_writer.add_document(doc!(score_field => 3.0f64, string_field => "terma"))?;
            index_writer.add_document(doc!(score_field => 7.0f64, string_field => "termb"))?;
            index_writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_composite": {
                "composite": {
                    "sources": [
                        {"myterm": {"terms": {"field": "string_id"}}}
                    ],
                    "size": 10
                },
                "aggs": {
                    "avg_score": {
                        "avg": {
                            "field": "score_f64"
                        }
                    },
                    "max_score": {
                        "max": {
                            "field": "score_f64"
                        }
                    }
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        let buckets = &res["my_composite"]["buckets"];

        // Check that sub-aggregations are computed for each bucket with specific values
        assert_eq!(
            buckets,
            &json!([
                {
                    "key": {"myterm": "terma"},
                    "doc_count": 2,
                    "avg_score": {"value": 4.0}, // (5+3)/2
                    "max_score": {"value": 5.0}
                },
                {
                    "key": {"myterm": "termb"},
                    "doc_count": 2,
                    "avg_score": {"value": 4.5}, // (2+7)/2
                    "max_score": {"value": 7.0}
                }
            ])
        );

        Ok(())
    }

    #[test]
    fn composite_aggregation_term_validation_errors() -> crate::Result<()> {
        // Create index with explicit document creation
        let mut schema_builder = Schema::builder();
        let string_field = schema_builder.add_text_field("string_id", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());

        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.add_document(doc!(string_field => "term"))?;
            index_writer.commit()?;
        }

        // Test empty sources
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_composite": {
                "composite": {
                    "sources": [],
                    "size": 10
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index);
        assert!(res.is_err());

        // Test size = 0
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_composite": {
                "composite": {
                    "sources": [
                        {"myterm": {"terms": {"field": "string_id"}}}
                    ],
                    "size": 0
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index);
        assert!(res.is_err());

        Ok(())
    }

    #[test]
    fn composite_aggregation_term_numeric_fields() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let score_field = schema_builder.add_f64_field("score", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.add_document(doc!(score_field => 1.0f64))?;
            index_writer.add_document(doc!(score_field => 2.0f64))?;
            index_writer.add_document(doc!(score_field => 1.0f64))?;
            index_writer.add_document(doc!(score_field => 3.33f64))?;
            index_writer.commit()?;
            index_writer.add_document(doc!(score_field => 1.0f64))?;
            index_writer.commit()?;
        }

        // Test composite aggregation on numeric field
        exec_and_assert_all_paginations(
            &index,
            json!([
                {"score": {"terms": {"field": "score"}}}
            ]),
            json!([
                {"key": {"score": 1}, "doc_count": 3},
                {"key": {"score": 2}, "doc_count": 1},
                {"key": {"score": 3.33}, "doc_count": 1}
            ]),
        );

        Ok(())
    }

    #[test]
    fn composite_aggregation_term_date_fields() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let date_field = schema_builder.add_date_field("timestamp", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            // Add documents with different dates
            index_writer
                .add_document(doc!(date_field => datetime_from_iso_str("2021-01-01T00:00:00Z")))?;
            index_writer
                .add_document(doc!(date_field => datetime_from_iso_str("2022-01-01T00:00:00Z")))?;
            index_writer
                .add_document(doc!(date_field => datetime_from_iso_str("2021-01-01T00:00:00Z")))?; // duplicate
            index_writer
                .add_document(doc!(date_field => datetime_from_iso_str("2023-01-01T00:00:00Z")))?;
            index_writer.commit()?;
        }

        // Test composite aggregation on date field
        exec_and_assert_all_paginations(
            &index,
            json!([
                {"timestamp": {"terms": {"field": "timestamp"}}}
            ]),
            json!([
                {"key": {"timestamp": 1609459200000i64}, "doc_count": 2},
                {"key": {"timestamp": 1640995200000i64}, "doc_count": 1},
                {"key": {"timestamp": 1672531200000i64}, "doc_count": 1}
            ]),
        );

        Ok(())
    }

    #[test]
    fn composite_aggregation_term_ip_fields() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let ip_field = schema_builder.add_ip_addr_field("ip_addr", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let ipv4 = |ip: &str| ip.parse::<Ipv4Addr>().unwrap().to_ipv6_mapped();
            let ipv6 = |ip: &str| ip.parse::<Ipv6Addr>().unwrap();
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.add_document(doc!(ip_field => ipv4("192.168.1.1")))?;
            index_writer.add_document(doc!(ip_field => ipv4("10.0.0.1")))?;
            index_writer.add_document(doc!(ip_field => ipv4("192.168.1.1")))?; // duplicate
            index_writer.add_document(doc!(ip_field => ipv4("172.16.0.1")))?;
            index_writer.add_document(doc!(ip_field => ipv6("2001:db8::1")))?;
            index_writer.add_document(doc!(ip_field => ipv6("::1")))?; // localhost
            index_writer.add_document(doc!())?;
            index_writer.add_document(doc!(ip_field => ipv6("2001:db8::1")))?; // duplicate
            index_writer.commit()?;
        }

        // Test composite aggregation on IP field
        exec_and_assert_all_paginations(
            &index,
            json!([
                {"ip_addr": {"terms": {"field": "ip_addr"}}}
            ]),
            json!([
                {"key": {"ip_addr": "::1"}, "doc_count": 1},
                {"key": {"ip_addr": "10.0.0.1"}, "doc_count": 1},
                {"key": {"ip_addr": "172.16.0.1"}, "doc_count": 1},
                {"key": {"ip_addr": "192.168.1.1"}, "doc_count": 2},
                {"key": {"ip_addr": "2001:db8::1"}, "doc_count": 2}
            ]),
        );

        Ok(())
    }

    #[test]
    fn composite_aggregation_term_multiple_column_types() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let score_field = schema_builder.add_f64_field("score", FAST);
        let string_field = schema_builder.add_text_field("string_id", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.add_document(doc!(score_field => 1.0f64, string_field => "apple"))?;
            index_writer.add_document(doc!(score_field => 2.0f64, string_field => "banana"))?;
            index_writer.add_document(doc!(score_field => 1.0f64, string_field => "apple"))?;
            index_writer.add_document(doc!(score_field => 2.0f64, string_field => "banana"))?;
            index_writer.add_document(doc!(score_field => 3.0f64, string_field => "cherry"))?;
            index_writer.add_document(doc!(score_field => 1.0f64, string_field => "banana"))?;
            index_writer.commit()?;
        }

        // Test composite aggregation mixing numeric and text fields
        exec_and_assert_all_paginations(
            &index,
            json!([
                {"category": {"terms": {"field": "string_id", "order": "asc"}}},
                {"score": {"terms": {"field": "score", "order": "desc"}}}
            ]),
            json!([
                {"key": {"category": "apple", "score": 1}, "doc_count": 2},
                {"key": {"category": "banana", "score": 2}, "doc_count": 2},
                {"key": {"category": "banana", "score": 1}, "doc_count": 1},
                {"key": {"category": "cherry", "score": 3}, "doc_count": 1}
            ]),
        );

        Ok(())
    }

    #[test]
    fn composite_aggregation_term_json_various_types() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json_data", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.add_document(
                doc!(json_field => json!({"cat": "elec", "price": 999, "avail": true})),
            )?;
            index_writer.add_document(
                doc!(json_field => json!({"cat": "books", "price": 15, "avail": false})),
            )?;
            index_writer.add_document(
                doc!(json_field => json!({"cat": "elec", "price": 200, "avail": true})),
            )?;
            index_writer.add_document(
                doc!(json_field => json!({"cat": "books", "price": 25, "avail": true})),
            )?;
            index_writer.commit()?;
        }

        exec_and_assert_all_paginations(
            &index,
            json!([
                {"cat": {"terms": {"field": "json_data.cat"}}},
                {"avail": {"terms": {"field": "json_data.avail"}}},
                {"price": {"terms": {"field": "json_data.price", "order": "desc"}}}
            ]),
            json!([
                {"key": {"cat": "books", "avail": false, "price": 15}, "doc_count": 1},
                {"key": {"cat": "books", "avail": true, "price": 25}, "doc_count": 1},
                {"key": {"cat": "elec", "avail": true, "price": 999}, "doc_count": 1},
                {"key": {"cat": "elec", "avail": true, "price": 200}, "doc_count": 1}
            ]),
        );

        Ok(())
    }

    #[test]
    fn composite_aggregation_term_json_missing_fields() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json_data", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer
                .add_document(doc!(json_field => json!({"cat": "elec", "brand": "apple"})))?;
            index_writer
                .add_document(doc!(json_field => json!({"cat": "books", "brand": "gut"})))?;
            index_writer.add_document(doc!(json_field => json!({"cat": "books"})))?; // missing brand
            index_writer.add_document(doc!(json_field => json!({"brand": "samsung"})))?; // missing category
            index_writer
                .add_document(doc!(json_field => json!({"cat": "elec", "brand": "samsung"})))?;
            index_writer.commit()?;
        }

        // Test with missing bucket enabled
        exec_and_assert_all_paginations(
            &index,
            json!([
                {"cat": {"terms": {"field": "json_data.cat", "missing_bucket": true}}},
                {"brand": {"terms": {"field": "json_data.brand", "missing_bucket": true, "missing_order": "last"}}}
            ]),
            json!([
                {"key": {"cat": null, "brand": "samsung"}, "doc_count": 1},
                {"key": {"cat": "books", "brand": "gut"}, "doc_count": 1},
                {"key": {"cat": "books", "brand": null}, "doc_count": 1},
                {"key": {"cat": "elec", "brand": "apple"}, "doc_count": 1},
                {"key": {"cat": "elec", "brand": "samsung"}, "doc_count": 1}
            ]),
        );

        // Small twist on the missing order of the second source
        exec_and_assert_all_paginations(
            &index,
            json!([
                {"cat": {"terms": {"field": "json_data.cat", "missing_bucket": true}}},
                {"brand": {"terms": {"field": "json_data.brand", "missing_bucket": true, "missing_order": "first"}}}
            ]),
            json!([
                {"key": {"cat": null, "brand": "samsung"}, "doc_count": 1},
                {"key": {"cat": "books", "brand": null}, "doc_count": 1},
                {"key": {"cat": "books", "brand": "gut"}, "doc_count": 1},
                {"key": {"cat": "elec", "brand": "apple"}, "doc_count": 1},
                {"key": {"cat": "elec", "brand": "samsung"}, "doc_count": 1}
            ]),
        );

        Ok(())
    }

    #[test]
    fn composite_aggregation_term_json_nested_fields() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json_data", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.add_document(
                doc!(json_field => json!({"prod": {"name": "laptop", "cpu": "intel"}})),
            )?;
            index_writer.add_document(
                doc!(json_field => json!({"prod": {"name": "phone", "cpu": "snap"}})),
            )?;
            index_writer.add_document(
                doc!(json_field => json!({"prod": {"name": "laptop", "cpu": "amd"}})),
            )?;
            index_writer.add_document(
                doc!(json_field => json!({"prod": {"name": "tablet", "cpu": "intel"}})),
            )?;
            index_writer.commit()?;
        }

        exec_and_assert_all_paginations(
            &index,
            json!([
                {"name": {"terms": {"field": "json_data.prod.name"}}},
                {"cpu": {"terms": {"field": "json_data.prod.cpu"}}}
            ]),
            json!([
                {"key": {"name": "laptop", "cpu": "amd"}, "doc_count": 1},
                {"key": {"name": "laptop", "cpu": "intel"}, "doc_count": 1},
                {"key": {"name": "phone", "cpu": "snap"}, "doc_count": 1},
                {"key": {"name": "tablet", "cpu": "intel"}, "doc_count": 1}
            ]),
        );

        Ok(())
    }

    #[test]
    fn composite_aggregation_term_json_mixed_types() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json_data", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.add_document(doc!(json_field => json!({"id": "doc1"})))?;
            // this segment's numeric is i64
            index_writer.add_document(doc!(json_field => json!({"id": 100})))?;
            index_writer.add_document(doc!(json_field => json!({"id": true})))?;
            index_writer.add_document(doc!(json_field => json!({"id": "doc2"})))?;
            index_writer.add_document(doc!(json_field => json!({"id": 50})))?;
            index_writer.add_document(doc!(json_field => json!({"id": false})))?;
            index_writer.add_document(doc!(json_field => json!({"id": "doc3"})))?;
            index_writer.commit()?;
            // this segment's numeric is f64
            index_writer.add_document(doc!(json_field => json!({"id": 33.3})))?;
            index_writer.add_document(doc!(json_field => json!({"id": 50})))?;
            index_writer.commit()?;
            // this segment contains dates
            index_writer.add_document(doc!(json_field => json!({"id": "doc4"})))?;
            index_writer.add_document(doc!(json_field => json!({"id": "2023-01-01T00:00:00Z"})))?;
            index_writer.add_document(doc!(json_field => json!({"id": "2023-01-02T00:00:00Z"})))?;
            index_writer.commit()?;
        }

        exec_and_assert_all_paginations(
            &index,
            json!([
                {"id": {"terms": {"field": "json_data.id", "order": "asc"}}}
            ]),
            json!([
                {"key": {"id": false}, "doc_count": 1},
                {"key": {"id": true}, "doc_count": 1},
                {"key": {"id": "doc1"}, "doc_count": 1},
                {"key": {"id": "doc2"}, "doc_count": 1},
                {"key": {"id": "doc3"}, "doc_count": 1},
                {"key": {"id": "doc4"}, "doc_count": 1},
                {"key": {"id": 33.3}, "doc_count": 1},
                {"key": {"id": 50}, "doc_count": 2},
                {"key": {"id": 100}, "doc_count": 1},
                {"key": {"id": ms_timestamp_from_iso_str("2023-01-01T00:00:00Z")}, "doc_count": 1},
                {"key": {"id": ms_timestamp_from_iso_str("2023-01-02T00:00:00Z")}, "doc_count": 1},
            ]),
        );

        // Test descending order
        exec_and_assert_all_paginations(
            &index,
            json!([
                {"id": {"terms": {"field": "json_data.id", "order": "desc"}}}
            ]),
            json!([
                {"key": {"id": ms_timestamp_from_iso_str("2023-01-02T00:00:00Z")}, "doc_count": 1},
                {"key": {"id": ms_timestamp_from_iso_str("2023-01-01T00:00:00Z")}, "doc_count": 1},
                {"key": {"id": 100}, "doc_count": 1},
                {"key": {"id": 50}, "doc_count": 2},
                {"key": {"id": 33.3}, "doc_count": 1},
                {"key": {"id": "doc4"}, "doc_count": 1},
                {"key": {"id": "doc3"}, "doc_count": 1},
                {"key": {"id": "doc2"}, "doc_count": 1},
                {"key": {"id": "doc1"}, "doc_count": 1},
                {"key": {"id": true}, "doc_count": 1},
                {"key": {"id": false}, "doc_count": 1}
            ]),
        );

        Ok(())
    }

    #[test]
    fn composite_aggregation_term_multi_value_fields() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", FAST | STRING);
        let num_field = schema_builder.add_u64_field("num", FAST);
        let index = Index::create_in_ram(schema_builder.build());

        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            // Document with multiple values for text and num fields
            index_writer.add_document(doc!(
                text_field => "apple",
                text_field => "banana",
                num_field => 10u64,
                num_field => 20u64,
                num_field => 10u64,
            ))?;
            index_writer.add_document(doc!(
                text_field => "cherry",
                num_field => 30u64,
            ))?;
            // Multi valued document with duplicate values
            index_writer.add_document(doc!(
                text_field => "elderberry",
                text_field => "date",
                text_field => "elderberry",
                num_field => 40u64,
            ))?;

            index_writer.commit()?;
        }

        exec_and_assert_all_paginations(
            &index,
            json!([
                {"text_terms": {"terms": {"field": "text"}}}
            ]),
            json!([
                {"key": {"text_terms": "apple"}, "doc_count": 1},
                {"key": {"text_terms": "banana"}, "doc_count": 1},
                {"key": {"text_terms": "cherry"}, "doc_count": 1},
                {"key": {"text_terms": "date"}, "doc_count": 1},
                {"key": {"text_terms": "elderberry"}, "doc_count": 1}
            ]),
        );

        exec_and_assert_all_paginations(
            &index,
            json!([
                {"num_terms": {"terms": {"field": "num"}}}
            ]),
            json!([
                {"key": {"num_terms": 10}, "doc_count": 1},
                {"key": {"num_terms": 20}, "doc_count": 1},
                {"key": {"num_terms": 30}, "doc_count": 1},
                {"key": {"num_terms": 40}, "doc_count": 1}
            ]),
        );

        exec_and_assert_all_paginations(
            &index,
            json!([
                {"num_terms": {"terms": {"field": "num"}}},
                {"text_terms": {"terms": {"field": "text"}}}
            ]),
            json!([
                {"key": {"num_terms": 10, "text_terms": "apple"}, "doc_count": 1},
                {"key": {"num_terms": 10, "text_terms": "banana"}, "doc_count": 1},
                {"key": {"num_terms": 20, "text_terms": "apple"}, "doc_count": 1},
                {"key": {"num_terms": 20, "text_terms": "banana"}, "doc_count": 1},
                {"key": {"num_terms": 30, "text_terms": "cherry"}, "doc_count": 1},
                {"key": {"num_terms": 40, "text_terms": "date"}, "doc_count": 1},
                {"key": {"num_terms": 40, "text_terms": "elderberry"}, "doc_count": 1}
            ]),
        );

        Ok(())
    }

    #[test]
    fn composite_aggregation_histogram_basic() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let num_field = schema_builder.add_f64_field("value", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.add_document(doc!(num_field => -0.5f64))?;
            index_writer.add_document(doc!(num_field => 1.0f64))?;
            index_writer.add_document(doc!(num_field => 2.0f64))?;
            index_writer.add_document(doc!(num_field => 5.0f64))?;
            index_writer.add_document(doc!(num_field => 7.0f64))?;
            index_writer.add_document(doc!(num_field => 11.0f64))?;
            index_writer.commit()?;
        }

        // Histogram with interval 5
        exec_and_assert_all_paginations(
            &index,
            json!([
                {"val_hist": {"histogram": {"field": "value", "interval": 5.0}}}
            ]),
            json!([
                {"key": {"val_hist": -5.0}, "doc_count": 1},
                {"key": {"val_hist": 0.0}, "doc_count": 2},
                {"key": {"val_hist": 5.0}, "doc_count": 2},
                {"key": {"val_hist": 10.0}, "doc_count": 1}
            ]),
        );
        Ok(())
    }

    #[test]
    fn composite_aggregation_deduplicates_mapped_source_keys() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json_data", FAST);
        let metric_field = schema_builder.add_u64_field("metric", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            // Numeric and date columns collapse into the same histogram buckets.
            index_writer.add_document(doc!(
                json_field => json!({
                    "value": [12, 22, 15, 12.5, "1970-01-01T00:00:00.012Z"],
                    "tag": ["x", "y", "x"]
                }),
                metric_field => 5u64,
                metric_field => 5u64,
            ))?;
            index_writer.add_document(doc!(
                json_field => json!({"value": 19, "tag": "x"}),
                metric_field => 7u64,
            ))?;
            index_writer.add_document(doc!(
                json_field => json!({"tag": "x"}),
                metric_field => 3u64,
            ))?;
            index_writer.commit()?;
        }
        for order in ["asc", "desc"] {
            for histogram_first in [true, false] {
                let mut sources = vec![
                    json!({"value": {"histogram": {
                        "field": "json_data.value", "interval": 10,
                        "order": order, "missing_bucket": true
                    }}}),
                    json!({"tag": {"terms": {"field": "json_data.tag", "order": order}}}),
                ];
                let mut expected_buckets = [
                    json!({"key": {"value": null, "tag": "x"}, "doc_count": 1, "total": {"value": 3.0}}),
                    json!({"key": {"value": 10.0, "tag": "x"}, "doc_count": 2, "total": {"value": 17.0}}),
                    json!({"key": {"value": 10.0, "tag": "y"}, "doc_count": 1, "total": {"value": 10.0}}),
                    json!({"key": {"value": 20.0, "tag": "x"}, "doc_count": 1, "total": {"value": 10.0}}),
                    json!({"key": {"value": 20.0, "tag": "y"}, "doc_count": 1, "total": {"value": 10.0}}),
                ];
                if !histogram_first {
                    sources.reverse();
                    expected_buckets.swap(2, 3);
                }
                if order == "desc" {
                    expected_buckets.reverse();
                }
                // Child sums retain duplicate metric values, not duplicate bucket membership.
                for page_size in 1..=expected_buckets.len() {
                    let mut after = None;
                    for expected_page in expected_buckets.chunks(page_size) {
                        let mut request = json!({"my_composite": {
                            "composite": {"sources": sources, "size": page_size},
                            "aggs": {"total": {"sum": {"field": "metric"}}}
                        }});
                        if let Some(after_key) = after.take() {
                            request["my_composite"]["composite"]["after"] = after_key;
                        }
                        let res = exec_request(serde_json::from_value(request)?, &index)?;
                        assert_eq!(res["my_composite"]["buckets"], json!(expected_page));
                        after = Some(res["my_composite"]["after_key"].clone());
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn composite_pagination_skips_prefixes_without_a_qualifying_suffix() -> crate::Result<()> {
        let mut schema = Schema::builder();
        let values = schema.add_u64_field("values", FAST);
        let metric = schema.add_u64_field("metric", FAST);
        let index = Index::create_in_ram(schema.build());
        let mut writer = index.writer_with_num_threads(1, 20_000_000)?;
        // More than one collection block. Every document has 32^3 distinct tuples,
        // but only two qualifying tuples per document are needed for this page.
        for _ in 0..130 {
            let mut doc = doc!(metric => 3u64, metric => 3u64);
            for value in (0..32u64).rev() {
                doc.add_u64(values, value);
            }
            writer.add_document(doc)?;
        }
        writer.commit()?;
        for (order, last_suffix, absent_suffix, next_prefix, suffixes) in [
            ("asc", "u64:31", "u64:99", 21, [0, 1]),
            ("desc", "u64:0", "i64:-1", 19, [31, 30]),
        ] {
            for after in [
                json!({"a": "u64:20", "b": "u64:31", "c": last_suffix}),
                // Nonexistent suffixes: the prefix equal to `after` produces no result.
                json!({"a": "u64:20", "b": "u64:31", "c": absent_suffix}),
                json!({"a": "u64:20", "b": "u64:99", "c": "u64:0"}),
            ] {
                let req = json!({"page": {
                    "composite": {
                        "sources": [
                            {"a": {"terms": {"field": "values", "order": order}}},
                            {"b": {"terms": {"field": "values"}}},
                            {"c": {"terms": {"field": "values", "order": order}}}
                        ],
                        "size": 2,
                        "after": after
                    },
                    "aggs": {"total": {"sum": {"field": "metric"}}}
                }});
                let res = exec_request(serde_json::from_value(req)?, &index)?;
                assert_eq!(
                    res["page"]["buckets"],
                    json!([
                        {"key": {"a": next_prefix, "b": 0, "c": suffixes[0]}, "doc_count": 130, "total": {"value": 780.0}},
                        {"key": {"a": next_prefix, "b": 0, "c": suffixes[1]}, "doc_count": 130, "total": {"value": 780.0}}
                    ])
                );
            }
        }
        Ok(())
    }

    #[test]
    fn composite_buckets_preserve_counts_and_child_metrics_under_terms() -> crate::Result<()> {
        let mut schema = Schema::builder();
        let parent = schema.add_text_field("parent", STRING | FAST);
        let data = schema.add_json_field("data", FAST);
        let metric = schema.add_u64_field("metric", FAST);
        let index = Index::create_in_ram(schema.build());
        let mut writer = index.writer_with_num_threads(1, 20_000_000)?;
        for _ in 0..130 {
            writer.add_document(doc!(
                parent => "a", parent => "b",
                data => json!({
                    "price": [12, 15, 22, "1970-01-01T00:00:00.012Z"],
                    "tag": ["x", "z", "x"]
                }),
                metric => 3u64, metric => 3u64,
            ))?;
        }
        writer.commit()?;
        let request = json!({"parents": {
            "terms": {"field": "parent", "order": {"_key": "asc"}},
            "aggs": {"page": {
                "composite": {
                    "sources": [
                        {"tag": {"terms": {"field": "data.tag", "order": "desc"}}},
                        {"price": {"histogram": {"field": "data.price", "interval": 10}}}
                    ],
                    "size": 2,
                    "after": {"tag": "str:z", "price": "f64:15"}
                },
                "aggs": {
                    "total": {"sum": {"field": "metric"}},
                    "tuples": {
                        "multi_terms": {"terms": [{"field": "metric"}]},
                        "aggs": {"total": {"sum": {"field": "metric"}}}
                    }
                }
            }}
        }});
        let result = exec_request(serde_json::from_value(request)?, &index)?;
        let parents = result["parents"]["buckets"].as_array().unwrap();
        assert_eq!(parents.len(), 2);
        for parent in parents {
            let buckets = parent["page"]["buckets"].as_array().unwrap();
            assert_eq!(buckets.len(), 2);
            assert_eq!(buckets[0]["key"], json!({"tag": "z", "price": 20.0}));
            assert_eq!(buckets[1]["key"], json!({"tag": "x", "price": 10.0}));
            for bucket in buckets {
                assert_eq!(bucket["doc_count"], 130);
                assert_eq!(bucket["total"]["value"], 780.0);
                assert_eq!(
                    bucket["tuples"]["buckets"],
                    json!([{"key": [3], "key_as_string": "3", "doc_count": 130, "total": {"value": 780.0}}])
                );
            }
        }
        Ok(())
    }

    #[test]
    fn composite_aggregation_histogram_json_mixed_types() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json_data", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            // this segment's numeric is i64
            index_writer.add_document(doc!(json_field => json!({"id": "doc1"})))?;
            index_writer.add_document(doc!(json_field => json!({"id": 100})))?;
            index_writer.add_document(doc!(json_field => json!({"id": true})))?;
            index_writer.add_document(doc!(json_field => json!({"id": "doc2"})))?;
            index_writer.add_document(doc!(json_field => json!({"id": 50})))?;
            index_writer.add_document(doc!(json_field => json!({"id": false})))?;
            index_writer.add_document(doc!(json_field => json!({"id": "doc3"})))?;
            index_writer.commit()?;
            // this segment's numeric is f64 and it also contains a date column
            index_writer.add_document(doc!(json_field => json!({"id": 33.3})))?;
            index_writer.add_document(doc!(json_field => json!({"id": 50})))?;
            index_writer.add_document(doc!(json_field => json!({"id": -0.01})))?;
            index_writer.add_document(doc!(json_field => json!({"id": "2023-01-01T00:00:00Z"})))?;
            index_writer.commit()?;
        }

        exec_and_assert_all_paginations(
            &index,
            json!([
                {"id": {"histogram": {"field": "json_data.id", "interval": 50, "order": "asc"}}}
            ]),
            json!([
                {"key": {"id": -50.0}, "doc_count": 1},
                {"key": {"id": 0.0}, "doc_count": 1},
                {"key": {"id": 50.0}, "doc_count": 2},
                {"key": {"id": 100.0}, "doc_count": 1},
                {"key": {"id": ms_timestamp_from_iso_str("2023-01-01T00:00:00Z") as f64}, "doc_count": 1},
            ]),
        );

        // Test descending order
        exec_and_assert_all_paginations(
            &index,
            json!([
                {"id": {"histogram": {"field": "json_data.id", "interval": 50, "order": "desc"}}}
            ]),
            json!([
                {"key": {"id": ms_timestamp_from_iso_str("2023-01-01T00:00:00Z") as f64},"doc_count": 1},
                {"key": {"id": 100.0}, "doc_count": 1},
                {"key": {"id": 50.0}, "doc_count": 2},
                {"key": {"id": 0.0}, "doc_count": 1},
                {"key": {"id": -50.0}, "doc_count": 1},
            ]),
        );

        Ok(())
    }

    #[test]
    fn composite_aggregation_date_histogram_calendar_interval() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let date_field = schema_builder.add_date_field("dt", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.add_document(doc!(
                date_field => datetime_from_iso_str("2021-01-01T00:00:00Z"),
                date_field => datetime_from_iso_str("2021-02-01T00:00:00Z"),
                date_field => datetime_from_iso_str("2021-01-01T00:00:00Z"),
            ))?;
            index_writer
                .add_document(doc!(date_field => datetime_from_iso_str("2021-02-01T00:00:00Z")))?;
            index_writer
                .add_document(doc!(date_field => datetime_from_iso_str("2022-01-01T00:00:00Z")))?;
            index_writer
                .add_document(doc!(date_field => datetime_from_iso_str("2023-01-01T00:00:00Z")))?;
            index_writer.commit()?;
        }

        // Date histogram with calendar_interval = "year"
        exec_and_assert_all_paginations(
            &index,
            json!([
                {"dt_hist": {"date_histogram": {"field": "dt", "calendar_interval": "year"}}}
            ]),
            json!([
                {"key": {"dt_hist": ms_timestamp_from_iso_str("2021-01-01T00:00:00Z")}, "doc_count": 2},
                {"key": {"dt_hist": ms_timestamp_from_iso_str("2022-01-01T00:00:00Z")}, "doc_count": 1},
                {"key": {"dt_hist": ms_timestamp_from_iso_str("2023-01-01T00:00:00Z")}, "doc_count": 1}
            ]),
        );
        Ok(())
    }

    #[test]
    fn composite_aggregation_date_histogram_fixed_interval() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let date_field = schema_builder.add_date_field("dt", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.add_document(doc!(
                date_field => datetime_from_iso_str("2021-01-01T00:00:00Z"),
                date_field => datetime_from_iso_str("2021-01-01T05:00:00Z"),
                date_field => datetime_from_iso_str("2021-01-01T00:00:00Z"),
            ))?;
            index_writer
                .add_document(doc!(date_field => datetime_from_iso_str("2021-01-01T05:30:00Z")))?;
            index_writer
                .add_document(doc!(date_field => datetime_from_iso_str("2021-01-01T06:00:00Z")))?;
            index_writer
                .add_document(doc!(date_field => datetime_from_iso_str("2021-01-01T12:00:00Z")))?;
            index_writer
                .add_document(doc!(date_field => datetime_from_iso_str("2021-01-01T18:00:00Z")))?;
            index_writer.add_document(doc!(
                date_field => datetime_from_iso_str("1969-12-31T19:00:00Z"),
                date_field => datetime_from_iso_str("1969-12-31T23:00:00Z"),
            ))?;
            index_writer.commit()?;
        }

        exec_and_assert_all_paginations(
            &index,
            json!([
                {"dt_hist": {"date_histogram": {"field": "dt", "fixed_interval": "6h"}}}
            ]),
            json!([
                {"key": {"dt_hist": ms_timestamp_from_iso_str("1969-12-31T18:00:00Z")}, "doc_count": 1},
                {"key": {"dt_hist": ms_timestamp_from_iso_str("2021-01-01T00:00:00Z")}, "doc_count": 2},
                {"key": {"dt_hist": ms_timestamp_from_iso_str("2021-01-01T06:00:00Z")}, "doc_count": 1},
                {"key": {"dt_hist": ms_timestamp_from_iso_str("2021-01-01T12:00:00Z")}, "doc_count": 1},
                {"key": {"dt_hist": ms_timestamp_from_iso_str("2021-01-01T18:00:00Z")}, "doc_count": 1}
            ]),
        );
        Ok(())
    }

    #[test]
    fn composite_aggregation_mixed_term_and_date_histogram() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let date_field = schema_builder.add_date_field("timestamp", FAST);
        let category_field = schema_builder.add_text_field("category", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());

        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.add_document(doc!(
                date_field => datetime_from_iso_str("2021-01-01T05:00:00Z"),
                category_field => "electronics"
            ))?;
            index_writer.add_document(doc!(
                date_field => datetime_from_iso_str("2021-01-15T10:30:00Z"),
                category_field => "electronics"
            ))?;
            index_writer.add_document(doc!(
                date_field => datetime_from_iso_str("2021-01-05T12:00:00Z"),
                category_field => "books"
            ))?;
            index_writer.add_document(doc!(
                date_field => datetime_from_iso_str("2021-02-10T08:45:00Z"),
                category_field => "books"
            ))?;
            index_writer.add_document(doc!(
                date_field => datetime_from_iso_str("2021-02-05T14:20:00Z"),
                category_field => "clothing"
            ))?;
            index_writer.add_document(doc!(
                date_field => datetime_from_iso_str("2021-02-20T09:15:00Z"),
                category_field => "clothing"
            ))?;

            index_writer.commit()?;
        }

        exec_and_assert_all_paginations(
            &index,
            json!([
                {"category": {"terms": {"field": "category"}}},
                {"month": {"date_histogram": {"field": "timestamp", "calendar_interval": "month"}}}
            ]),
            json!([
                {"key": {"category": "books", "month": ms_timestamp_from_iso_str("2021-01-01T00:00:00Z")}, "doc_count": 1},
                {"key": {"category": "books", "month": ms_timestamp_from_iso_str("2021-02-01T00:00:00Z")}, "doc_count": 1},
                {"key": {"category": "clothing", "month": ms_timestamp_from_iso_str("2021-02-01T00:00:00Z")}, "doc_count": 2},
                {"key": {"category": "electronics", "month": ms_timestamp_from_iso_str("2021-01-01T00:00:00Z")}, "doc_count": 2}
            ]),
        );

        // Test with different ordering for sources with a size limit
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_composite": {
                "composite": {
                    "sources": [
                        {"month": {"date_histogram": {"field": "timestamp", "calendar_interval": "month"}}},
                        {"category": {"terms": {"field": "category", "order": "desc"}}}
                    ],
                    "size": 3
                }
            }
        }))
        .unwrap();
        let res = exec_request(agg_req, &index)?;
        let buckets = &res["my_composite"]["buckets"];
        assert_eq!(
            buckets,
            &json!([
                {"key": {"month": ms_timestamp_from_iso_str("2021-01-01T00:00:00Z"), "category": "electronics"}, "doc_count": 2},
                {"key": {"month": ms_timestamp_from_iso_str("2021-01-01T00:00:00Z"), "category": "books"}, "doc_count": 1},
                {"key": {"month": ms_timestamp_from_iso_str("2021-02-01T00:00:00Z"), "category": "clothing"}, "doc_count": 2},
            ]),
        );

        // next page
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_composite": {
                "composite": {
                    "sources": [
                        {"month": {"date_histogram": {"field": "timestamp", "calendar_interval": "month"}}},
                        {"category": {"terms": {"field": "category", "order": "desc"}}}
                    ],
                    "size": 3,
                    "after": res["my_composite"]["after_key"]
                }
            }
        }))
        .unwrap();
        let res = exec_request(agg_req, &index)?;
        let buckets = &res["my_composite"]["buckets"];
        assert_eq!(
            buckets,
            &json!([
                {"key": {"month": ms_timestamp_from_iso_str("2021-02-01T00:00:00Z"), "category": "books"}, "doc_count": 1},
            ]),
        );
        let feb_2021_ns = ms_timestamp_from_iso_str("2021-02-01T00:00:00Z") * 1_000_000;
        assert_eq!(
            res["my_composite"]["after_key"],
            json!({
                "month": format!("dt:{}", feb_2021_ns),
                "category": "str:books"
            })
        );

        Ok(())
    }

    #[test]
    fn composite_aggregation_no_matching_columns() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let date_field = schema_builder.add_f64_field("dt", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.add_document(doc!(date_field => 1.0))?;
            index_writer.add_document(doc!(date_field => 2.0))?;
            index_writer.commit()?;
        }

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_composite": {
                "composite": {
                    "sources": [
                        {"dt_hist": {"date_histogram": {"field": "dt", "fixed_interval": "6h"}}}
                    ],
                    "size": 10
                }
            }
        }))
        .unwrap();
        let res = exec_request(agg_req, &index)?;
        let buckets = &res["my_composite"]["buckets"];
        assert_eq!(buckets, &json!([]));

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_composite": {
                "composite": {
                    "sources": [
                        {"dt_hist": {"date_histogram": {"field": "dt", "fixed_interval": "6h", "missing_bucket": true}}}
                    ],
                    "size": 10,
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        let buckets = &res["my_composite"]["buckets"];

        assert_eq!(
            buckets,
            &json!([{"key": {"dt_hist": null}, "doc_count": 2}])
        );
        Ok(())
    }
}
