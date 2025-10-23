mod accessors;
mod calendar_interval;
mod collector;
mod map;
mod numeric_types;

use core::panic;
use std::cmp::Ordering;
use std::fmt::Debug;

use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use crate::aggregation::agg_result::CompositeKey;
pub use crate::aggregation::bucket::composite::accessors::{
    CompositeAccessor, CompositeAggReqData, CompositeSourceAccessors, PrecomputedDateInterval,
};
pub use crate::aggregation::bucket::composite::collector::SegmentCompositeCollector;
use crate::aggregation::bucket::composite::numeric_types::num_cmp::{
    cmp_i64_f64, cmp_i64_u64, cmp_u64_f64,
};
use crate::aggregation::bucket::Order;
use crate::aggregation::deserialize_f64;
use crate::aggregation::intermediate_agg_result::CompositeIntermediateKey;
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

/// Term source for a composite aggregation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TermCompositeAggregationSource {
    /// The name used to refer to this source in the composite key.
    #[serde(skip)]
    pub name: String,
    /// The field to aggregate on.
    pub field: String,
    /// The order for this source.
    #[serde(default = "Order::asc")]
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
    #[serde(default = "Order::asc")]
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
    #[serde(default = "Order::asc")]
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

/// A paginable aggregation that performs on multiple dimensions (sources),
/// potentially mixing term and range queries.
///
/// Pagination is made possible because the buckets are ordered by the composite
/// key, so the next page can be fetched "efficiently" by filtering using range
/// queries on the key dimensions.
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
    pub after: FxHashMap<String, CompositeKey>,
}

#[derive(Serialize, Deserialize)]
struct CompositeAggregationSerde {
    sources: Vec<FxHashMap<String, CompositeAggregationSource>>,
    size: u32,
    #[serde(default, skip_serializing_if = "FxHashMap::is_empty")]
    after: FxHashMap<String, CompositeKey>,
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

/// Ordering key for intermediate composite keys when comparing different types.
fn type_order_key(key: &CompositeIntermediateKey, order: Order) -> i32 {
    let apply_order = match order {
        Order::Asc => 1,
        Order::Desc => -1,
    };
    match key {
        CompositeIntermediateKey::Null => panic!("unexpected"),
        CompositeIntermediateKey::Bool(_) => 1 * apply_order,
        CompositeIntermediateKey::Str(_) => 2 * apply_order,
        CompositeIntermediateKey::I64(_) => 3 * apply_order,
        CompositeIntermediateKey::F64(_) => 3 * apply_order,
        CompositeIntermediateKey::U64(_) => 3 * apply_order,
        CompositeIntermediateKey::IpAddr(_) => 4,
        CompositeIntermediateKey::DateTime(_) => 4,
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
        (type_a, type_b) => {
            forced_ordering = true;
            type_order_key(type_a, order).cmp(&type_order_key(type_b, order))
        }
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
            .expect(&format!("Failed to parse date: {}", date_str));
        let timestamp_secs = dt.unix_timestamp_nanos();
        common::DateTime::from_timestamp_nanos(timestamp_secs as i64)
    }

    fn ms_timestamp_from_iso_str(date_str: &str) -> i64 {
        let dt = OffsetDateTime::parse(date_str, &Rfc3339)
            .expect(&format!("Failed to parse date: {}", date_str));
        (dt.unix_timestamp_nanos() / 1_000_000) as i64
    }

    /// Runs the query and compares the result buckets to the expected buckets,
    /// then run the same query with a all possible `after` keys and different
    /// page sizes.
    fn exec_and_assert_all_paginations(
        index: &Index,
        composite_agg_req: serde_json::Value,
        expected_buckets: serde_json::Value,
    ) {
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_composite": {
                "composite": composite_agg_req
            }
        }))
        .unwrap();
        let res = exec_request(agg_req, &index).unwrap();
        let buckets = &res["my_composite"]["buckets"];
        assert_eq!(buckets, &expected_buckets);

        // Check that all returned buckets can be used as after key
        // Note: this is not a requirement of the API, only the key explicitly
        // returned as after_key is guaranteed to work, but this is a nice
        // property of the implementation.
        for (i, expected_bucket) in expected_buckets.as_array().unwrap().iter().enumerate() {
            let new_composite_agg_req = json!({
                "sources": composite_agg_req["sources"].clone(),
                "size": composite_agg_req["size"].clone(),
                "after": expected_bucket["key"].clone()
            });
            let agg_req: Aggregations = serde_json::from_value(json!({
                "my_composite": {
                    "composite": new_composite_agg_req
                }
            }))
            .unwrap();
            let paginated_res = exec_request(agg_req, &index).unwrap();
            assert_eq!(
                &paginated_res["my_composite"]["buckets"],
                &json!(&expected_buckets.as_array().unwrap()[i + 1..]),
                "query with after key from bucket failed: {}",
                new_composite_agg_req.to_string()
            );
        }

        // paginate 1 by 1
        let one_by_one_composite_agg_req = json!({
            "sources": composite_agg_req["sources"].clone(),
            "size": 1,
        });
        let mut after_key = None;
        for i in 0..expected_buckets.as_array().unwrap().len() {
            let mut paged_req = one_by_one_composite_agg_req.clone();
            if let Some(after_key) = after_key {
                paged_req["after"] = after_key;
            }
            let agg_req: Aggregations = serde_json::from_value(json!({
                "my_composite": {
                    "composite": paged_req
                }
            }))
            .unwrap();
            let paged_res = exec_request(agg_req, &index).unwrap();
            assert_eq!(
                &paged_res["my_composite"]["buckets"],
                &json!(&[&expected_buckets[i]]),
                "1-by-1 pagination failed at index {}, query: {}",
                i,
                paged_req.to_string()
            );
            after_key = paged_res["my_composite"].get("after_key").cloned();
        }
        // Ideally, we should not require the user to issue an extra request
        // because we could know that this is the last page.
        if let Some(last_after_key) = after_key {
            let mut last_page_req = one_by_one_composite_agg_req.clone();
            last_page_req["after"] = last_after_key;
            let agg_req: Aggregations = serde_json::from_value(json!({
                "my_composite": {
                    "composite": last_page_req
                }
            }))
            .unwrap();
            let paged_res = exec_request(agg_req, &index).unwrap();
            assert_eq!(
                &paged_res["my_composite"]["buckets"],
                &json!([]),
                "last page request failed, query: {}",
                last_page_req.to_string()
            );
            after_key = paged_res["my_composite"].get("after_key").cloned();
        }
        assert_eq!(after_key, None);
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
        assert!(res["my_composite"].get("after_key").is_none());

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
        assert!(res["my_composite"].get("after_key").is_none());

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
            json!({
                "sources": [
                    {"myterm": {"terms": {"field": "string_id", "missing_bucket": false}}}
                ],
                "size": 10
            }),
            json!([
                {"key": {"myterm": "terma"}, "doc_count": 2},
                {"key": {"myterm": "termb"}, "doc_count": 1}
            ]),
        );

        // Test with missing bucket enabled
        exec_and_assert_all_paginations(
            &index,
            json!({
                "sources": [
                    {"myterm": {"terms": {"field": "string_id", "missing_bucket": true}}}
                ],
                "size": 10
            }),
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
    fn composite_aggregation_term_missing_order() -> crate::Result<()> {
        // Create index with missing values
        let mut schema_builder = Schema::builder();
        let string_field = schema_builder.add_text_field("string_id", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());

        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.add_document(doc!(string_field => "termb"))?;
            index_writer.add_document(doc!())?;
            index_writer.add_document(doc!(string_field => "terma"))?;
            index_writer.commit()?;
        }

        // Test missing_order: "first"
        exec_and_assert_all_paginations(
            &index,
            json!({
                "sources": [
                    {
                        "myterm": {
                            "terms": {
                                "field": "string_id",
                                "missing_bucket": true,
                                "missing_order": "first",
                                "order": "asc"
                            }
                        }
                    }
                ],
                "size": 10
            }),
            json!([
                {"key": {"myterm": null}, "doc_count": 1},
                {"key": {"myterm": "terma"}, "doc_count": 1},
                {"key": {"myterm": "termb"}, "doc_count": 1}
            ]),
        );

        // Test missing_order: "last"
        exec_and_assert_all_paginations(
            &index,
            json!({
                "sources": [
                    {
                        "myterm": {
                            "terms": {
                                "field": "string_id",
                                "missing_bucket": true,
                                "missing_order": "last",
                                "order": "asc"
                            }
                        }
                    }
                ],
                "size": 10
            }),
            json!([
                {"key": {"myterm": "terma"}, "doc_count": 1},
                {"key": {"myterm": "termb"}, "doc_count": 1},
                {"key": {"myterm": null}, "doc_count": 1}
            ]),
        );

        // Test missing_order: "default" with desc order
        exec_and_assert_all_paginations(
            &index,
            json!({
                "sources": [
                    {
                        "myterm": {
                            "terms": {
                                "field": "string_id",
                                "missing_bucket": true,
                                "missing_order": "default",
                                "order": "desc"
                            }
                        }
                    }
                ],
                "size": 10
            }),
            json!([
                {"key": {"myterm": "termb"}, "doc_count": 1},
                {"key": {"myterm": "terma"}, "doc_count": 1},
                {"key": {"myterm": null}, "doc_count": 1}
            ]),
        );

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
            json!({
                "sources": [
                    {"category": {"terms": {"field": "category"}}},
                    {"status": {"terms": {"field": "status"}}}
                ],
                "size": 10
            }),
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
            json!({
                "sources": [
                    {"category": {"terms": {"field": "category", "order": "asc"}}},
                    {"priority": {"terms": {"field": "priority", "order": "desc"}}}
                ],
                "size": 10
            }),
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
            json!({
                "sources": [
                    {"score": {"terms": {"field": "score"}}}
                ],
                "size": 10
            }),
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
            json!({
                "sources": [
                    {"timestamp": {"terms": {"field": "timestamp"}}}
                ],
                "size": 10
            }),
            json!([
                {"key": {"timestamp": "2021-01-01T00:00:00Z"}, "doc_count": 2},
                {"key": {"timestamp": "2022-01-01T00:00:00Z"}, "doc_count": 1},
                {"key": {"timestamp": "2023-01-01T00:00:00Z"}, "doc_count": 1}
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
            json!({
                "sources": [
                    {"ip_addr": {"terms": {"field": "ip_addr"}}}
                ],
                "size": 10
            }),
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
            json!({
                "sources": [
                    {"category": {"terms": {"field": "string_id", "order": "asc"}}},
                    {"score": {"terms": {"field": "score", "order": "desc"}}}
                ],
                "size": 10
            }),
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
            json!({
                "sources": [
                    {"cat": {"terms": {"field": "json_data.cat"}}},
                    {"avail": {"terms": {"field": "json_data.avail"}}},
                    {"price": {"terms": {"field": "json_data.price", "order": "desc"}}}
                ],
                "size": 10
            }),
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
            json!({
                "sources": [
                    {"cat": {"terms": {"field": "json_data.cat", "missing_bucket": true}}},
                    {"brand": {"terms": {"field": "json_data.brand", "missing_bucket": true, "missing_order": "last"}}}
                ],
                "size": 10
            }),
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
            json!({
                "sources": [
                    {"cat": {"terms": {"field": "json_data.cat", "missing_bucket": true}}},
                    {"brand": {"terms": {"field": "json_data.brand", "missing_bucket": true, "missing_order": "first"}}}
                ],
                "size": 10
            }),
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
            json!({
                "sources": [
                    {"name": {"terms": {"field": "json_data.prod.name"}}},
                    {"cpu": {"terms": {"field": "json_data.prod.cpu"}}}
                ],
                "size": 10
            }),
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
        }

        exec_and_assert_all_paginations(
            &index,
            json!({
                "sources": [
                    {"id": {"terms": {"field": "json_data.id", "order": "asc"}}}
                ],
                "size": 10
            }),
            json!([
                {"key": {"id": false}, "doc_count": 1},
                {"key": {"id": true}, "doc_count": 1},
                {"key": {"id": "doc1"}, "doc_count": 1},
                {"key": {"id": "doc2"}, "doc_count": 1},
                {"key": {"id": "doc3"}, "doc_count": 1},
                {"key": {"id": 33.3}, "doc_count": 1},
                {"key": {"id": 50}, "doc_count": 2},
                {"key": {"id": 100}, "doc_count": 1}
            ]),
        );

        // Test descending order
        exec_and_assert_all_paginations(
            &index,
            json!({
                "sources": [
                    {"id": {"terms": {"field": "json_data.id", "order": "desc"}}}
                ],
                "size": 10
            }),
            json!([
                {"key": {"id": 100}, "doc_count": 1},
                {"key": {"id": 50}, "doc_count": 2},
                {"key": {"id": 33.3}, "doc_count": 1},
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
            json!({
                "sources": [
                    {"text_terms": {"terms": {"field": "text"}}}
                ],
                "size": 10
            }),
            json!([
                {"key": {"text_terms": "apple"}, "doc_count": 1},
                {"key": {"text_terms": "banana"}, "doc_count": 1},
                {"key": {"text_terms": "cherry"}, "doc_count": 1},
                {"key": {"text_terms": "date"}, "doc_count": 1},
                // this is not the doc count but the term occurrence count
                // https://github.com/quickwit-oss/tantivy/issues/2721
                {"key": {"text_terms": "elderberry"}, "doc_count": 2}
            ]),
        );

        exec_and_assert_all_paginations(
            &index,
            json!({
                "sources": [
                    {"num_terms": {"terms": {"field": "num"}}}
                ],
                "size": 10
            }),
            json!([
                {"key": {"num_terms": 10}, "doc_count": 1},
                {"key": {"num_terms": 20}, "doc_count": 1},
                {"key": {"num_terms": 30}, "doc_count": 1},
                {"key": {"num_terms": 40}, "doc_count": 1}
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
            json!({
                "sources": [
                    {"val_hist": {"histogram": {"field": "value", "interval": 5.0}}}
                ],
                "size": 10
            }),
            json!([
                {"key": {"val_hist": 0.0}, "doc_count": 2},
                {"key": {"val_hist": 5.0}, "doc_count": 2},
                {"key": {"val_hist": 10.0}, "doc_count": 1}
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
            index_writer
                .add_document(doc!(date_field => datetime_from_iso_str("2021-01-01T00:00:00Z")))?;
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
            json!({
                "sources": [
                    {"dt_hist": {"date_histogram": {"field": "dt", "calendar_interval": "year"}}}
                ],
                "size": 10
            }),
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
            index_writer
                .add_document(doc!(date_field => datetime_from_iso_str("2021-01-01T00:00:00Z")))?;
            index_writer
                .add_document(doc!(date_field => datetime_from_iso_str("2021-01-01T05:30:00Z")))?;
            index_writer
                .add_document(doc!(date_field => datetime_from_iso_str("2021-01-01T06:00:00Z")))?;
            index_writer
                .add_document(doc!(date_field => datetime_from_iso_str("2021-01-01T12:00:00Z")))?;
            index_writer
                .add_document(doc!(date_field => datetime_from_iso_str("2021-01-01T18:00:00Z")))?;
            index_writer.commit()?;
        }

        exec_and_assert_all_paginations(
            &index,
            json!({
                "sources": [
                    {"dt_hist": {"date_histogram": {"field": "dt", "fixed_interval": "6h"}}}
                ],
                "size": 10
            }),
            json!([
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
            json!({
                "sources": [
                    {"category": {"terms": {"field": "category"}}},
                    {"month": {"date_histogram": {"field": "timestamp", "calendar_interval": "month"}}}
                ],
                "size": 10
            }),
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
        assert!(res["my_composite"].get("after_key").is_none());

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
