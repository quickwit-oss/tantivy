use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::net::Ipv6Addr;

use columnar::column_values::CompactSpaceU64Accessor;
use columnar::{
    ColumnType, Dictionary, MonotonicallyMappableToU128, MonotonicallyMappableToU64, NumericalValue,
};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use crate::aggregation::agg_req_with_accessor::{
    AggregationWithAccessor, AggregationsWithAccessor, CompositeAccessor,
};
use crate::aggregation::bucket::Order;
use crate::aggregation::format_date;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateBucketResult,
    IntermediateCompositeBucketEntry, IntermediateCompositeBucketResult, IntermediateKey,
};
use crate::aggregation::segment_agg_result::{
    build_segment_agg_collector, SegmentAggregationCollector,
};
use crate::TantivyError;

/// The position of missing keys in the ordering
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum MissingOrder {
    /// Missing keys appear first in ascending order, last in descending order
    #[default]
    Default,
    /// Missing keys should appear first
    First,
    /// Missing keys should appear last
    Last,
}

/// Determine the ordering between potentially missing intermediate keys
pub(crate) fn composite_ordering(
    left_opt: &Option<IntermediateKey>,
    right_opt: &Option<IntermediateKey>,
    order: Order,
    missing_order: MissingOrder,
) -> Ordering {
    match (left_opt, right_opt) {
        (Some(left), Some(right)) => {
            // only floats are not totally ordered, let's not care about NaN/Inf here
            let ord = left.partial_cmp(&right).unwrap_or(Ordering::Equal);
            match order {
                Order::Asc => ord,
                Order::Desc => ord.reverse(),
            }
        }
        (None, Some(_)) => match (missing_order, order) {
            (MissingOrder::First, _) => Ordering::Less,
            (MissingOrder::Last, _) => Ordering::Greater,
            (MissingOrder::Default, Order::Asc) => Ordering::Less,
            (MissingOrder::Default, Order::Desc) => Ordering::Greater,
        },
        (Some(_), None) => match (missing_order, order) {
            (MissingOrder::First, _) => Ordering::Greater,
            (MissingOrder::Last, _) => Ordering::Less,
            (MissingOrder::Default, Order::Asc) => Ordering::Greater,
            (MissingOrder::Default, Order::Desc) => Ordering::Less,
        },
        (None, None) => Ordering::Equal,
    }
}

/// Term source for a composite aggregation
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TermCompositeAggregationSource {
    /// The name used to refer to this source in the composite key
    #[serde(skip)]
    pub name: String,
    /// The field to aggregate on
    pub field: String,
    /// The order for this source
    #[serde(default = "Order::asc")]
    pub order: Order,
    /// Whether to create a `null` bucket for documents without value for this
    /// field. By default documents without a value are ignored.
    #[serde(default)]
    pub missing_bucket: bool,
    /// Whether missing keys should appear first or last
    #[serde(default)]
    pub missing_order: MissingOrder,
}

/// Source for the composite aggregation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompositeAggregationSource {
    /// Terms source
    Terms(TermCompositeAggregationSource),
    // /// Histogram source
    // Histogram { field: String, interval: f64 },
    // /// Date histogram source
    // DateHistogram { field: String, fixed_interval: Option<String>, },
}

impl CompositeAggregationSource {
    pub(crate) fn field(&self) -> &str {
        match self {
            CompositeAggregationSource::Terms(term_source) => &term_source.field,
        }
    }

    pub(crate) fn name(&self) -> &str {
        match self {
            CompositeAggregationSource::Terms(term_source) => &term_source.name,
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
    /// The fields and bucketting strategies
    pub sources: Vec<CompositeAggregationSource>,
    /// Number of buckets to return (page size)
    pub size: u32,
}

#[derive(Serialize, Deserialize)]
struct CompositeAggregationSerde {
    sources: Vec<FxHashMap<String, CompositeAggregationSource>>,
    size: u32,
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
                CompositeAggregationSource::Terms(term_source) => {
                    term_source.name = name;
                }
            }
            sources.push(source);
        }
        Ok(CompositeAggregation {
            sources,
            size: value.size,
        })
    }
}

impl From<CompositeAggregation> for CompositeAggregationSerde {
    fn from(value: CompositeAggregation) -> Self {
        let mut serde_sources = Vec::with_capacity(value.sources.len());
        for source in value.sources {
            let (name, stored_source) = match source {
                CompositeAggregationSource::Terms(term_source) => {
                    let name = term_source.name.clone();
                    // name field is #[serde(skip)] so it won't be serialized inside the value
                    (name, CompositeAggregationSource::Terms(term_source))
                }
            };
            let mut map = FxHashMap::default();
            map.insert(name, stored_source);
            serde_sources.push(map);
        }
        CompositeAggregationSerde {
            sources: serde_sources,
            size: value.size,
        }
    }
}

#[derive(Clone, Debug)]
struct CompositeBucketCollector {
    count: u32,
    sub_aggs: Option<Box<dyn SegmentAggregationCollector>>,
}

impl CompositeBucketCollector {
    fn new(sub_aggs: Option<Box<dyn SegmentAggregationCollector>>) -> Self {
        CompositeBucketCollector { count: 0, sub_aggs }
    }
    #[inline]
    fn collect(
        &mut self,
        doc: crate::DocId,
        agg_with_accessor: &mut AggregationsWithAccessor,
    ) -> crate::Result<()> {
        self.count += 1;
        if let Some(sub_aggs) = &mut self.sub_aggs {
            sub_aggs.collect(doc, agg_with_accessor)?;
        }
        Ok(())
    }
}

/// The value is represented as a tuple of:
/// - the column index or missing value sentinel
///   - if the value is present, store the accessor index + 1
///   - if the value is missing, store 0 (for missing first) or u8::MAX (for missing last)
/// - the fast field value u64 representation
///   - 0 if the field is missing
///   - regular u64 repr if the ordering is ascending
///   - bitwise NOT of the u64 repr if the ordering is descending
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
struct InternalValueRepr(u8, u64);

impl InternalValueRepr {
    #[inline]
    fn new(raw: u64, accessor_idx: u8, order: Order) -> Self {
        match order {
            Order::Asc => InternalValueRepr(accessor_idx + 1, raw),
            Order::Desc => InternalValueRepr(accessor_idx + 1, !raw),
        }
    }
    #[inline]
    fn new_missing(order: Order, missing_order: MissingOrder) -> Self {
        let column_idx = match (missing_order, order) {
            (MissingOrder::First, _) => 0,
            (MissingOrder::Last, _) => u8::MAX,
            (MissingOrder::Default, Order::Asc) => 0,
            (MissingOrder::Default, Order::Desc) => u8::MAX,
        };
        InternalValueRepr(column_idx, 0)
    }
    #[inline]
    fn decode(self, order: Order) -> Option<(u8, u64)> {
        if self.0 == u8::MAX || self.0 == 0 {
            return None;
        }
        match order {
            Order::Asc => Some((self.0 - 1, self.1)),
            Order::Desc => Some((self.0 - 1, !self.1)),
        }
    }
}

/// The value is represented as a tuple of:
/// - the column index the value was extracted from
/// - the fast field value u64 representation
#[derive(Clone, Debug, Default)]
struct CompositeBuckets {
    pub(crate) buckets: BTreeMap<Vec<InternalValueRepr>, CompositeBucketCollector>,
}

/// The collector puts values from the fast field into the correct buckets and
/// does a conversion to the correct datatype.
#[derive(Clone, Debug)]
pub struct SegmentCompositeCollector {
    buckets: CompositeBuckets,
    req: CompositeAggregation,
    blueprint: Option<Box<dyn SegmentAggregationCollector>>,
    accessor_idx: usize,
}

impl SegmentAggregationCollector for SegmentCompositeCollector {
    fn add_intermediate_aggregation_result(
        self: Box<Self>,
        agg_with_accessor: &AggregationsWithAccessor,
        results: &mut IntermediateAggregationResults,
    ) -> crate::Result<()> {
        let name = agg_with_accessor.aggs.keys[self.accessor_idx].to_string();
        let agg_with_accessor = &agg_with_accessor.aggs.values[self.accessor_idx];

        let buckets = self.into_intermediate_bucket_result(agg_with_accessor)?;

        results.push(
            name,
            IntermediateAggregationResult::Bucket(IntermediateBucketResult::Composite { buckets }),
        )?;

        Ok(())
    }

    #[inline]
    fn collect(
        &mut self,
        doc: crate::DocId,
        agg_with_accessor: &mut AggregationsWithAccessor,
    ) -> crate::Result<()> {
        self.collect_block(&[doc], agg_with_accessor)
    }

    #[inline]
    fn collect_block(
        &mut self,
        docs: &[crate::DocId],
        agg_with_accessor: &mut AggregationsWithAccessor,
    ) -> crate::Result<()> {
        let bucket_agg_accessor = &mut agg_with_accessor.aggs.values[self.accessor_idx];
        let accessors = &bucket_agg_accessor.composite_accessors;

        let mem_pre = self.get_memory_consumption();

        for doc in docs {
            let mut sub_level_values = Vec::with_capacity(accessors.len());
            recursive_key_visitor(
                *doc,
                &self.blueprint,
                self.req.size,
                &mut bucket_agg_accessor.sub_aggregation,
                accessors,
                &self.req.sources,
                &mut sub_level_values,
                &mut self.buckets,
            )?;
        }

        let mem_delta = self.get_memory_consumption() - mem_pre;
        if mem_delta > 0 {
            bucket_agg_accessor.limits.add_memory_consumed(mem_delta)?;
        }

        Ok(())
    }

    fn flush(&mut self, agg_with_accessor: &mut AggregationsWithAccessor) -> crate::Result<()> {
        let sub_aggregation_accessor =
            &mut agg_with_accessor.aggs.values[self.accessor_idx].sub_aggregation;

        for sub_agg_collector in self.buckets.buckets.values_mut() {
            if let Some(sub_aggs_collector) = &mut sub_agg_collector.sub_aggs {
                sub_aggs_collector.flush(sub_aggregation_accessor)?;
            }
        }
        Ok(())
    }
}

impl SegmentCompositeCollector {
    fn get_memory_consumption(&self) -> u64 {
        // TODO get correct estimate (these are just the keys)
        (self.buckets.buckets.len() * (std::mem::size_of::<InternalValueRepr>())) as u64
    }

    pub(crate) fn from_req_and_validate(
        req: &CompositeAggregation,
        sub_aggregations: &mut AggregationsWithAccessor,
        col_types: &[Vec<ColumnType>], // for validation
        accessor_idx: usize,
    ) -> crate::Result<Self> {
        if req.sources.is_empty() {
            return Err(TantivyError::InvalidArgument(
                "composite aggregation must have at least one source".to_string(),
            ));
        }
        if req.size == 0 {
            return Err(TantivyError::InvalidArgument(
                "composite aggregation 'size' must be > 0".to_string(),
            ));
        }

        for source_columns in col_types {
            if source_columns.is_empty() {
                return Err(TantivyError::InvalidArgument(
                    "composite aggregation source must have at least one accessor".to_string(),
                ));
            }
            if source_columns.contains(&ColumnType::Bytes) {
                return Err(TantivyError::InvalidArgument(
                    "composite aggregation does not support 'bytes' field type".to_string(),
                ));
            }
            if source_columns.contains(&ColumnType::DateTime) && source_columns.len() > 1 {
                return Err(TantivyError::InvalidArgument(
                    "composite aggregation expects 'date' fields to have a single column"
                        .to_string(),
                ));
            }
            if source_columns.contains(&ColumnType::IpAddr) && source_columns.len() > 1 {
                return Err(TantivyError::InvalidArgument(
                    "composite aggregation expects 'ip' fields to have a single column".to_string(),
                ));
            }
        }

        let blueprint = if !sub_aggregations.is_empty() {
            let sub_aggregation = build_segment_agg_collector(sub_aggregations)?;
            Some(sub_aggregation)
        } else {
            None
        };

        Ok(SegmentCompositeCollector {
            buckets: CompositeBuckets::default(),
            req: req.clone(),
            blueprint,
            accessor_idx,
        })
    }

    #[inline]
    pub(crate) fn into_intermediate_bucket_result(
        self,
        agg_with_accessor: &AggregationWithAccessor,
    ) -> crate::Result<IntermediateCompositeBucketResult> {
        let mut dict: FxHashMap<Vec<Option<IntermediateKey>>, IntermediateCompositeBucketEntry> =
            Default::default();
        dict.reserve(self.buckets.buckets.len());

        for (key_internal_repr, agg) in self.buckets.buckets {
            let key = resolve_key(key_internal_repr, &self.req.sources, agg_with_accessor)?;
            let mut sub_aggregation_res = IntermediateAggregationResults::default();
            if let Some(sub_aggs_collector) = agg.sub_aggs {
                sub_aggs_collector.add_intermediate_aggregation_result(
                    &agg_with_accessor.sub_aggregation,
                    &mut sub_aggregation_res,
                )?;
            }

            dict.insert(
                key,
                IntermediateCompositeBucketEntry {
                    doc_count: agg.count,
                    sub_aggregation: sub_aggregation_res,
                },
            );
        }

        Ok(IntermediateCompositeBucketResult {
            entries: dict,
            target_size: self.req.size,
            orders: self
                .req
                .sources
                .iter()
                .map(|s| match s {
                    CompositeAggregationSource::Terms(t) => (t.order, t.missing_order),
                })
                .collect(),
        })
    }
}

fn collect_bucket_with_limit(
    doc_id: crate::DocId,
    sub_aggregation: &mut AggregationsWithAccessor,
    buckets: &mut CompositeBuckets,
    key: &Vec<InternalValueRepr>,
    blueprint: &Option<Box<dyn SegmentAggregationCollector>>,
    limit: u32,
) -> crate::Result<()> {
    let buckets = &mut buckets.buckets;
    if (buckets.len() as u32) < limit {
        buckets
            .entry(key.clone())
            .or_insert_with(|| CompositeBucketCollector::new(blueprint.clone()))
            .collect(doc_id, sub_aggregation)?;
        return Ok(());
    }
    if let Some(entry) = buckets.get_mut(key) {
        entry.collect(doc_id, sub_aggregation)?;
        return Ok(());
    }
    let last_entry = buckets.last_entry().unwrap();
    if key < last_entry.key() {
        // we have a better bucket, evict the worst one
        last_entry.remove();
        buckets
            .entry(key.clone())
            .or_insert_with(|| CompositeBucketCollector::new(blueprint.clone()))
            .collect(doc_id, sub_aggregation)?;
    }
    Ok(())
}

fn resolve_key(
    internal_key: Vec<InternalValueRepr>,
    sources: &[CompositeAggregationSource],
    agg_with_accessor: &AggregationWithAccessor,
) -> crate::Result<Vec<Option<IntermediateKey>>> {
    internal_key
        .into_iter()
        .enumerate()
        .map(|(idx, val)| {
            resolve_internal_value_repr(
                val,
                &sources[idx],
                &agg_with_accessor.composite_accessors[idx],
            )
        })
        .collect()
}

fn resolve_internal_value_repr(
    internal_value_repr: InternalValueRepr,
    source: &CompositeAggregationSource,
    composite_accessors: &[CompositeAccessor],
) -> crate::Result<Option<IntermediateKey>> {
    let decoded_value_opt = match source {
        CompositeAggregationSource::Terms(term_source) => {
            internal_value_repr.decode(term_source.order)
        }
    };
    let Some((decoded_accessor_idx, val)) = decoded_value_opt else {
        return Ok(None);
    };
    let CompositeAccessor {
        column_type,
        str_dict_column,
        column,
    } = &composite_accessors[decoded_accessor_idx as usize];

    let key = if *column_type == ColumnType::Str {
        let fallback_dict = Dictionary::empty();
        let term_dict = str_dict_column
            .as_ref()
            .map(|el| el.dictionary())
            .unwrap_or_else(|| &fallback_dict);

        // TODO try use sorted_ords_to_term_cb to batch
        let mut buffer = Vec::new();
        term_dict.ord_to_term(val, &mut buffer)?;
        IntermediateKey::Str(
            String::from_utf8(buffer.to_vec()).expect("could not convert to String"),
        )
    } else if *column_type == ColumnType::DateTime {
        let val = i64::from_u64(val);
        let date = format_date(val)?;
        IntermediateKey::Str(date)
    } else if *column_type == ColumnType::Bool {
        let val = bool::from_u64(val);
        IntermediateKey::Bool(val)
    } else if *column_type == ColumnType::IpAddr {
        let compact_space_accessor = column
            .values
            .clone()
            .downcast_arc::<CompactSpaceU64Accessor>()
            .map_err(|_| {
                TantivyError::AggregationError(crate::aggregation::AggregationError::InternalError(
                    "Type mismatch: Could not downcast to CompactSpaceU64Accessor".to_string(),
                ))
            })?;
        let val: u128 = compact_space_accessor.compact_to_u128(val as u32);
        let val = Ipv6Addr::from_u128(val);
        IntermediateKey::IpAddr(val)
    } else {
        if *column_type == ColumnType::U64 {
            IntermediateKey::U64(val)
        } else if *column_type == ColumnType::I64 {
            IntermediateKey::I64(i64::from_u64(val))
        } else {
            let val = f64::from_u64(val);
            let val: NumericalValue = val.into();

            match val.normalize() {
                NumericalValue::U64(val) => IntermediateKey::U64(val),
                NumericalValue::I64(val) => IntermediateKey::I64(val),
                NumericalValue::F64(val) => IntermediateKey::F64(val),
            }
        }
    };
    Ok(Some(key))
}

/// Depth-first walk of the accessors to build the composite key combinations
/// and update the buckets.
fn recursive_key_visitor(
    doc_id: crate::DocId,
    blueprint: &Option<Box<dyn SegmentAggregationCollector>>,
    limit: u32,
    sub_aggregation: &mut AggregationsWithAccessor,
    accessors: &[Vec<CompositeAccessor>],
    sources: &[CompositeAggregationSource],
    sub_level_values: &mut Vec<InternalValueRepr>,
    buckets: &mut CompositeBuckets,
) -> crate::Result<()> {
    if accessors.is_empty() {
        collect_bucket_with_limit(
            doc_id,
            sub_aggregation,
            buckets,
            sub_level_values,
            blueprint,
            limit,
        )?;
        return Ok(());
    }
    let current_level_accessor = &accessors[0];
    let current_level_source = &sources[0];
    let sub_level_accessors = &accessors[1..];
    let sub_level_sources = &sources[1..];
    let mut missing = true;
    for (i, accessor) in current_level_accessor.iter().enumerate() {
        let values = accessor.column.values_for_doc(doc_id);
        match current_level_source {
            CompositeAggregationSource::Terms(term_source) => {
                for value in values {
                    missing = false;
                    sub_level_values.push(InternalValueRepr::new(
                        value,
                        i as u8,
                        term_source.order,
                    ));
                    recursive_key_visitor(
                        doc_id,
                        blueprint,
                        limit,
                        sub_aggregation,
                        sub_level_accessors,
                        sub_level_sources,
                        sub_level_values,
                        buckets,
                    )?;
                    sub_level_values.pop();
                }
            }
        }
    }
    if missing {
        match current_level_source {
            CompositeAggregationSource::Terms(term_source) => {
                if term_source.missing_bucket == false {
                    // missing bucket not requested, skip this branch
                    return Ok(());
                }
                sub_level_values.push(InternalValueRepr::new_missing(
                    term_source.order,
                    term_source.missing_order,
                ));
            }
        }

        recursive_key_visitor(
            doc_id,
            blueprint,
            limit,
            sub_aggregation,
            sub_level_accessors,
            sub_level_sources,
            sub_level_values,
            buckets,
        )?;
        sub_level_values.pop();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use common::DateTime;
    use serde_json::json;

    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::tests::exec_request;
    use crate::schema::{Schema, FAST, STRING};
    use crate::Index;

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
    fn composite_aggregation_test_single_segment() -> crate::Result<()> {
        composite_aggregation_test(true)
    }

    #[test]
    fn composite_aggregation_test_multi_segment() -> crate::Result<()> {
        composite_aggregation_test(false)
    }

    fn composite_aggregation_test_size_limit(merge_segments: bool) -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let string_field = schema_builder.add_text_field("string_id", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.add_document(doc!(string_field => "terma"))?;
            index_writer.add_document(doc!(string_field => "termb"))?;
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

        Ok(())
    }

    #[test]
    fn composite_aggregation_test_size_limit_single_segment() -> crate::Result<()> {
        composite_aggregation_test_size_limit(true)
    }

    #[test]
    fn composite_aggregation_test_size_limit_multi_segment() -> crate::Result<()> {
        composite_aggregation_test_size_limit(false)
    }

    #[test]
    fn composite_aggregation_test_ordering() -> crate::Result<()> {
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
            "my_composite": {
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
        let buckets = &res["my_composite"]["buckets"];

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
            "my_composite": {
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
        let buckets = &res["my_composite"]["buckets"];

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

        Ok(())
    }

    #[test]
    fn composite_aggregation_test_missing_values() -> crate::Result<()> {
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
        {
            let agg_req: Aggregations = serde_json::from_value(json!({
                "my_composite": {
                    "composite": {
                        "sources": [
                            {"myterm": {"terms": {"field": "string_id", "missing_bucket": false}}}
                        ],
                        "size": 10
                    }
                }
            }))
            .unwrap();

            let res = exec_request(agg_req, &index)?;
            let buckets = &res["my_composite"]["buckets"];

            // Should only have 2 buckets (terma, termb), missing values ignored
            assert_eq!(
                buckets,
                &json!([
                    {"key": {"myterm": "terma"}, "doc_count": 2},
                    {"key": {"myterm": "termb"}, "doc_count": 1}
                ])
            );
        }

        // Test with missing bucket enabled
        {
            let agg_req: Aggregations = serde_json::from_value(json!({
                "my_composite": {
                    "composite": {
                        "sources": [
                            {"myterm": {"terms": {"field": "string_id", "missing_bucket": true}}}
                        ],
                        "size": 10
                    }
                }
            }))
            .unwrap();

            let res = exec_request(agg_req, &index)?;
            let buckets = &res["my_composite"]["buckets"];

            // Should have 3 buckets including the missing bucket
            // Missing bucket should come first in ascending order by default
            assert_eq!(
                buckets,
                &json!([
                    {"key": {"myterm": null}, "doc_count": 1},
                    {"key": {"myterm": "terma"}, "doc_count": 2},
                    {"key": {"myterm": "termb"}, "doc_count": 1}
                ])
            );
        }

        Ok(())
    }

    #[test]
    fn composite_aggregation_test_missing_order() -> crate::Result<()> {
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
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_composite": {
                "composite": {
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
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        let buckets = &res["my_composite"]["buckets"];

        // Missing should be first
        assert_eq!(
            buckets,
            &json!([
                {"key": {"myterm": null}, "doc_count": 1},
                {"key": {"myterm": "terma"}, "doc_count": 1},
                {"key": {"myterm": "termb"}, "doc_count": 1}
            ])
        );

        // Test missing_order: "last"
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_composite": {
                "composite": {
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
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        let buckets = &res["my_composite"]["buckets"];

        // Missing should be last
        assert_eq!(
            buckets,
            &json!([
                {"key": {"myterm": "terma"}, "doc_count": 1},
                {"key": {"myterm": "termb"}, "doc_count": 1},
                {"key": {"myterm": null}, "doc_count": 1}
            ])
        );

        // Test missing_order: "default" with desc order
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_composite": {
                "composite": {
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
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        let buckets = &res["my_composite"]["buckets"];

        // In desc order with default missing_order, missing should appear last
        assert_eq!(
            buckets,
            &json!([
                {"key": {"myterm": "termb"}, "doc_count": 1},
                {"key": {"myterm": "terma"}, "doc_count": 1},
                {"key": {"myterm": null}, "doc_count": 1}
            ])
        );

        Ok(())
    }

    #[test]
    fn composite_aggregation_test_multi_source() -> crate::Result<()> {
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

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_composite": {
                "composite": {
                    "sources": [
                        {"category": {"terms": {"field": "category"}}},
                        {"status": {"terms": {"field": "status"}}}
                    ],
                    "size": 10
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        let buckets = &res["my_composite"]["buckets"];

        // Should have composite keys with both dimensions in sorted order
        assert_eq!(
            buckets,
            &json!([
                {"key": {"category": "books", "status": "active"}, "doc_count": 1},
                {"key": {"category": "books", "status": "inactive"}, "doc_count": 1},
                {"key": {"category": "clothing", "status": "active"}, "doc_count": 1},
                {"key": {"category": "electronics", "status": "active"}, "doc_count": 2},
                {"key": {"category": "electronics", "status": "inactive"}, "doc_count": 1}
            ])
        );

        Ok(())
    }

    #[test]
    fn composite_aggregation_test_multi_source_ordering() -> crate::Result<()> {
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
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_composite": {
                "composite": {
                    "sources": [
                        {"category": {"terms": {"field": "category", "order": "asc"}}},
                        {"priority": {"terms": {"field": "priority", "order": "desc"}}}
                    ],
                    "size": 10
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        let buckets = &res["my_composite"]["buckets"];

        // Should be sorted by category asc, then priority desc
        assert_eq!(
            buckets,
            &json!([
                {"key": {"category": "apple", "priority": "low"}, "doc_count": 1},
                {"key": {"category": "apple", "priority": "high"}, "doc_count": 1},
                {"key": {"category": "zebra", "priority": "low"}, "doc_count": 1},
                {"key": {"category": "zebra", "priority": "high"}, "doc_count": 1}
            ])
        );

        Ok(())
    }

    #[test]
    fn composite_aggregation_test_with_sub_aggregations() -> crate::Result<()> {
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
    fn composite_aggregation_test_validation_errors() -> crate::Result<()> {
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
    fn composite_aggregation_test_numeric_fields() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let score_field = schema_builder.add_f64_field("score", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.add_document(doc!(score_field => 1.0f64))?;
            index_writer.add_document(doc!(score_field => 2.0f64))?;
            index_writer.add_document(doc!(score_field => 1.0f64))?;
            index_writer.add_document(doc!(score_field => 3.0f64))?;
            index_writer.commit()?;
        }

        // Test composite aggregation on numeric field
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_composite": {
                "composite": {
                    "sources": [
                        {"score": {"terms": {"field": "score"}}}
                    ],
                    "size": 10
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        let buckets = &res["my_composite"]["buckets"];

        // Should be ordered by numeric value
        assert_eq!(
            buckets,
            &json!([
                {"key": {"score": 1}, "doc_count": 2}, // Two docs with score 1.0
                {"key": {"score": 2}, "doc_count": 1},
                {"key": {"score": 3}, "doc_count": 1}
            ])
        );

        Ok(())
    }

    #[test]
    fn composite_aggregation_test_date_fields() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let date_field = schema_builder.add_date_field("timestamp", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            // Add documents with different dates (string timestamps)
            index_writer
                .add_document(doc!(date_field => DateTime::from_timestamp_secs(1609459200)))?; // 2021-01-01
            index_writer
                .add_document(doc!(date_field => DateTime::from_timestamp_secs(1640995200)))?; // 2022-01-01
            index_writer
                .add_document(doc!(date_field => DateTime::from_timestamp_secs(1609459200)))?; // 2021 duplicate
            index_writer
                .add_document(doc!(date_field => DateTime::from_timestamp_secs(1672531200)))?; // 2023-01-01
            index_writer.commit()?;
        }

        // Test composite aggregation on date field
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_composite": {
                "composite": {
                    "sources": [
                        {"timestamp": {"terms": {"field": "timestamp"}}}
                    ],
                    "size": 10
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        let buckets = &res["my_composite"]["buckets"];

        // Should be ordered by date value (as formatted strings)
        assert_eq!(
            buckets,
            &json!([
                {"key": {"timestamp": "2021-01-01T00:00:00Z"}, "doc_count": 2},
                {"key": {"timestamp": "2022-01-01T00:00:00Z"}, "doc_count": 1},
                {"key": {"timestamp": "2023-01-01T00:00:00Z"}, "doc_count": 1}
            ])
        );

        Ok(())
    }

    #[test]
    fn composite_aggregation_test_ip_fields() -> crate::Result<()> {
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
            index_writer.add_document(doc!(ip_field => ipv6("2001:db8::1")))?; // duplicate
            index_writer.commit()?;
        }

        // Test composite aggregation on IP field
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_composite": {
                "composite": {
                    "sources": [
                        {"ip_addr": {"terms": {"field": "ip_addr"}}}
                    ],
                    "size": 10
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        let buckets = &res["my_composite"]["buckets"];

        // Should be ordered by IP address
        assert_eq!(
            buckets,
            &json!([
                {"key": {"ip_addr": "::1"}, "doc_count": 1},
                {"key": {"ip_addr": "10.0.0.1"}, "doc_count": 1},
                {"key": {"ip_addr": "172.16.0.1"}, "doc_count": 1},
                {"key": {"ip_addr": "192.168.1.1"}, "doc_count": 2},
                {"key": {"ip_addr": "2001:db8::1"}, "doc_count": 2}
            ])
        );

        Ok(())
    }

    #[test]
    fn composite_aggregation_test_multiple_column_types() -> crate::Result<()> {
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
            index_writer.commit()?;
        }

        // Test composite aggregation mixing numeric and text fields
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_composite": {
                "composite": {
                    "sources": [
                        {"category": {"terms": {"field": "string_id", "order": "asc"}}},
                        {"score": {"terms": {"field": "score", "order": "desc"}}}
                    ],
                    "size": 10
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        let buckets = &res["my_composite"]["buckets"];

        // Should handle mixed types correctly
        assert!(buckets.as_array().unwrap().len() > 0);

        // Check that keys contain both string and numeric values
        for bucket in buckets.as_array().unwrap() {
            assert!(bucket["key"]["category"].is_string());
            assert!(bucket["key"]["score"].is_number());
        }

        Ok(())
    }

    #[test]
    fn composite_aggregation_test_json_various_types() -> crate::Result<()> {
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

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_composite": {
                "composite": {
                    "sources": [
                        {"cat": {"terms": {"field": "json_data.cat"}}},
                        {"avail": {"terms": {"field": "json_data.avail"}}},
                        {"price": {"terms": {"field": "json_data.price", "order": "desc"}}}
                    ],
                    "size": 10
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        let buckets = &res["my_composite"]["buckets"];

        // Verify buckets with mixed types (booleans stored as numbers 0/1)
        assert_eq!(
            buckets,
            &json!([
                {"key": {"cat": "books", "avail": false, "price": 15}, "doc_count": 1},
                {"key": {"cat": "books", "avail": true, "price": 25}, "doc_count": 1},
                {"key": {"cat": "elec", "avail": true, "price": 999}, "doc_count": 1},
                {"key": {"cat": "elec", "avail": true, "price": 200}, "doc_count": 1}
            ])
        );

        Ok(())
    }

    #[test]
    fn composite_aggregation_test_json_missing_fields() -> crate::Result<()> {
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
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_composite": {
                "composite": {
                    "sources": [
                        {"cat": {"terms": {"field": "json_data.cat", "missing_bucket": true}}},
                        {"brand": {"terms": {"field": "json_data.brand", "missing_bucket": true, "missing_order": "last"}}}
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
                {"key": {"cat": null, "brand": "samsung"}, "doc_count": 1},
                {"key": {"cat": "books", "brand": "gut"}, "doc_count": 1},
                {"key": {"cat": "books", "brand": null}, "doc_count": 1},
                {"key": {"cat": "elec", "brand": "apple"}, "doc_count": 1},
                {"key": {"cat": "elec", "brand": "samsung"}, "doc_count": 1}
            ])
        );

        Ok(())
    }

    #[test]
    fn composite_aggregation_test_json_nested_fields() -> crate::Result<()> {
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

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_composite": {
                "composite": {
                    "sources": [
                        {"name": {"terms": {"field": "json_data.prod.name"}}},
                        {"cpu": {"terms": {"field": "json_data.prod.cpu"}}}
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
                {"key": {"name": "laptop", "cpu": "amd"}, "doc_count": 1},
                {"key": {"name": "laptop", "cpu": "intel"}, "doc_count": 1},
                {"key": {"name": "phone", "cpu": "snap"}, "doc_count": 1},
                {"key": {"name": "tablet", "cpu": "intel"}, "doc_count": 1}
            ])
        );

        Ok(())
    }

    #[test]
    fn composite_aggregation_test_json_mixed_types() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json_data", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
            index_writer.add_document(doc!(json_field => json!({"id": "doc1"})))?;
            index_writer.add_document(doc!(json_field => json!({"id": 100})))?;
            index_writer.add_document(doc!(json_field => json!({"id": true})))?;
            index_writer.add_document(doc!(json_field => json!({"id": "doc2"})))?;
            index_writer.add_document(doc!(json_field => json!({"id": 50})))?;
            index_writer.add_document(doc!(json_field => json!({"id": false})))?;
            index_writer.add_document(doc!(json_field => json!({"id": "doc3"})))?;
            index_writer.commit()?;
        }

        // Test ascending order - let's first see what we get
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_composite": {
                "composite": {
                    "sources": [
                        {"id": {"terms": {"field": "json_data.id", "order": "asc"}}}
                    ],
                    "size": 10
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        let buckets = &res["my_composite"]["buckets"];

        // In ascending order: booleans, numbers, then strings
        assert_eq!(
            buckets,
            &json!([
                {"key": {"id": false}, "doc_count": 1},
                {"key": {"id": true}, "doc_count": 1},
                {"key": {"id": "doc1"}, "doc_count": 1},
                {"key": {"id": "doc2"}, "doc_count": 1},
                {"key": {"id": "doc3"}, "doc_count": 1},
                {"key": {"id": 50}, "doc_count": 1},
                {"key": {"id": 100}, "doc_count": 1}
            ])
        );

        // Test descending order
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_composite": {
                "composite": {
                    "sources": [
                        {"id": {"terms": {"field": "json_data.id", "order": "desc"}}}
                    ],
                    "size": 10
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;
        let buckets = &res["my_composite"]["buckets"];

        // In descending order
        assert_eq!(
            buckets,
            &json!([
                {"key": {"id": 100}, "doc_count": 1},
                {"key": {"id": 50}, "doc_count": 1},
                {"key": {"id": "doc3"}, "doc_count": 1},
                {"key": {"id": "doc2"}, "doc_count": 1},
                {"key": {"id": "doc1"}, "doc_count": 1},
                {"key": {"id": true}, "doc_count": 1},
                {"key": {"id": false}, "doc_count": 1}
            ])
        );

        Ok(())
    }
}
