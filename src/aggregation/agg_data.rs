use std::sync::Arc;

use columnar::{Column, ColumnType, StrColumn};
use common::BitSet;
use rustc_hash::FxHashSet;
use serde::Serialize;
use tantivy_fst::Regex;

use crate::aggregation::accessor_helpers::{
    get_all_value_sources, get_dynamic_columns, get_missing_val_as_u64_lenient,
    get_numeric_or_date_column_types, get_value_source,
};
use crate::aggregation::agg_req::{Aggregation, AggregationVariants, Aggregations};
use crate::aggregation::bucket::{
    build_segment_filter_collector, build_segment_histogram_collector,
    build_segment_multi_terms_collector, build_segment_range_collector, CompositeAggReqData,
    CompositeAggregation, CompositeSourceAccessors, FilterAggReqData, HistogramAggReqData,
    IncludeExcludeParam, MissingTermAggReqData, MultiTermsAggReqData, MultiTermsAggregation,
    MultiTermsFieldAccessor, MultiTermsMissingAccessor, RangeAggReqData, TermMissingAgg,
    TermsAggReqData, TermsAggregation, TermsAggregationInternal,
};
use crate::aggregation::metric::{
    build_segment_stats_collector, AverageAggregation, CardinalityAggReqData,
    CardinalityAggregationReq, CountAggregation, ExtendedStatsAggregation, MaxAggregation,
    MetricAggReqData, MinAggregation, SegmentExtendedStatsCollector, SegmentPercentilesCollector,
    StatsAggregation, StatsType, SumAggregation, TopHitsAggReqData, TopHitsSegmentCollector,
};
use crate::aggregation::segment_agg_result::{
    GenericSegmentAggregationResultsCollector, SegmentAggregationCollector,
};
use crate::aggregation::{
    f64_to_fastfield_u64, AggContextParams, ColumnBlockAccessor, Key, ValueSource,
    ValueSourceRegistry,
};
use crate::{SegmentOrdinal, SegmentReader};

/// Shared state passed to the collectors during collection on a segment.
pub struct AggregationsSegmentCtx {
    pub(crate) context: AggContextParams,
    /// Scratch buffers shared by all of the collectors of the tree, to load a block of values.
    pub(crate) column_block_accessor: ColumnBlockAccessor,
}

impl AggregationsSegmentCtx {
    pub(crate) fn new(context: AggContextParams) -> Self {
        AggregationsSegmentCtx {
            context,
            column_block_accessor: ColumnBlockAccessor::default(),
        }
    }
}

/// A node of the per-segment aggregation request tree.
///
/// A node owns the request data of its aggregation, including its value source. Building the
/// collectors consumes the tree: each node is turned into exactly one collector, which takes
/// ownership of the node's request data.
///
/// A single aggregation of the request can expand into several sibling nodes (e.g. a terms
/// aggregation over a JSON field with several column types).
pub(crate) struct AggNode {
    pub(crate) data: AggNodeData,
    pub(crate) children: Vec<AggNode>,
}

/// The request data of an [`AggNode`]. Each type of aggregation has its own request data
/// struct.
pub(crate) enum AggNodeData {
    Terms(TermsAggReqData),
    MissingTerm(MissingTermAggReqData),
    Cardinality(CardinalityAggReqData),
    /// Shared by avg, min, max, sum, stats, extended_stats, count and percentiles.
    Metric(MetricAggReqData),
    TopHits(Box<TopHitsAggReqData>),
    /// Shared by histogram and date_histogram.
    Histogram(HistogramAggReqData),
    Range(RangeAggReqData),
    Filter(Box<FilterAggReqData>),
    Composite(CompositeAggReqData),
    MultiTerms(MultiTermsAggReqData),
}

impl AggNode {
    fn new(data: AggNodeData, children: Vec<AggNode>) -> Self {
        AggNode { data, children }
    }

    /// Name of the aggregation, as given in the request.
    pub(crate) fn name(&self) -> &str {
        match &self.data {
            AggNodeData::Terms(req_data) => &req_data.name,
            AggNodeData::MissingTerm(req_data) => &req_data.name,
            AggNodeData::Cardinality(req_data) => &req_data.name,
            AggNodeData::Metric(req_data) => &req_data.name,
            AggNodeData::TopHits(req_data) => &req_data.name,
            AggNodeData::Histogram(req_data) => &req_data.name,
            AggNodeData::Range(req_data) => &req_data.name,
            AggNodeData::Filter(req_data) => &req_data.name,
            AggNodeData::Composite(req_data) => &req_data.name,
            AggNodeData::MultiTerms(req_data) => &req_data.name,
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn kind_name(&self) -> &'static str {
        match &self.data {
            AggNodeData::Terms(_) => "Terms",
            AggNodeData::MissingTerm(_) => "MissingTerm",
            AggNodeData::Cardinality(_) => "Cardinality",
            AggNodeData::Metric(_) => "Metric",
            AggNodeData::TopHits(_) => "TopHits",
            AggNodeData::Histogram(req_data) if req_data.is_date_histogram => "DateHistogram",
            AggNodeData::Histogram(_) => "Histogram",
            AggNodeData::Range(_) => "Range",
            AggNodeData::Filter(_) => "Filter",
            AggNodeData::Composite(_) => "Composite",
            AggNodeData::MultiTerms(_) => "MultiTerms",
        }
    }

    /// Estimate the memory consumption of this node's request data in bytes, excluding its
    /// children.
    ///
    /// The node itself is not counted: it is consumed when building its collector, which only
    /// keeps the request data.
    pub(crate) fn get_memory_consumption(&self) -> usize {
        match &self.data {
            AggNodeData::Terms(req_data) => req_data.get_memory_consumption(),
            AggNodeData::MissingTerm(req_data) => req_data.get_memory_consumption(),
            AggNodeData::Cardinality(req_data) => req_data.get_memory_consumption(),
            AggNodeData::Metric(req_data) => req_data.get_memory_consumption(),
            AggNodeData::TopHits(req_data) => req_data.get_memory_consumption(),
            AggNodeData::Histogram(req_data) => req_data.get_memory_consumption(),
            AggNodeData::Range(req_data) => req_data.get_memory_consumption(),
            AggNodeData::Filter(req_data) => req_data.get_memory_consumption(),
            AggNodeData::Composite(req_data) => req_data.get_memory_consumption(),
            AggNodeData::MultiTerms(req_data) => req_data.get_memory_consumption(),
        }
    }
}

/// Returns the child aggregation named `name`, if any.
pub(crate) fn find_sub_agg<'a>(children: &'a [AggNode], name: &str) -> Option<&'a AggNode> {
    children.iter().find(|child| child.name() == name)
}

/// Convert the aggregation tree into a serializable struct representation.
/// Each node contains: { name, kind, children }.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn get_view_tree(nodes: &[AggNode]) -> Vec<AggTreeViewNode> {
    let mut views: Vec<AggTreeViewNode> = nodes
        .iter()
        .map(|node| AggTreeViewNode {
            name: node.name().to_string(),
            kind: node.kind_name().to_string(),
            children: get_view_tree(&node.children),
        })
        .collect();
    views.sort_by_key(|view| serde_json::to_string(view).unwrap());
    views
}

/// Builds the collectors for `nodes`, consuming them.
///
/// This is where the request data of `nodes` is charged to the memory limits. Hidden contract:
/// collector builders must not charge their request data again, and a builder that consumes a
/// child node without calling this function (e.g. the flattened terms×histogram collector) has to
/// charge that child itself.
pub(crate) fn build_segment_agg_collectors(
    ctx: &mut AggregationsSegmentCtx,
    nodes: Vec<AggNode>,
) -> crate::Result<Box<dyn SegmentAggregationCollector>> {
    // The request data is moved into the collectors, so we need to measure it beforehand.
    let memory_consumption: usize = nodes.iter().map(AggNode::get_memory_consumption).sum();
    let mut collectors = Vec::with_capacity(nodes.len());
    for node in nodes {
        collectors.push(build_segment_agg_collector(ctx, node)?);
    }

    ctx.context
        .limits
        .add_memory_consumed(memory_consumption as u64)?;
    // Single collector special case
    if collectors.len() == 1 {
        return Ok(collectors.pop().unwrap());
    }
    let agg = GenericSegmentAggregationResultsCollector { aggs: collectors };
    Ok(Box::new(agg))
}

/// Builds the sub-aggregation collectors of a bucket aggregation, consuming `children`.
///
/// Returns `None` when there are no children: collectors rely on `None` to skip doc buffering.
pub(crate) fn build_sub_agg_collectors(
    ctx: &mut AggregationsSegmentCtx,
    children: Vec<AggNode>,
) -> crate::Result<Option<Box<dyn SegmentAggregationCollector>>> {
    if children.is_empty() {
        return Ok(None);
    }
    Ok(Some(build_segment_agg_collectors(ctx, children)?))
}

/// Builds the collector for `node`, consuming it.
pub(crate) fn build_segment_agg_collector(
    ctx: &mut AggregationsSegmentCtx,
    node: AggNode,
) -> crate::Result<Box<dyn SegmentAggregationCollector>> {
    let AggNode { data, children } = node;
    match data {
        AggNodeData::Terms(req_data) => {
            crate::aggregation::bucket::build_segment_term_collector(ctx, req_data, children)
        }
        AggNodeData::MissingTerm(req_data) => {
            if req_data.accessors.is_empty() {
                return Err(crate::TantivyError::InternalError(
                    "MissingTerm aggregation requires at least one field accessor.".to_string(),
                ));
            }
            let sub_agg = build_sub_agg_collectors(ctx, children)?;
            Ok(Box::new(TermMissingAgg::new(req_data, sub_agg)))
        }
        AggNodeData::Cardinality(req_data) => {
            crate::aggregation::metric::build_segment_cardinality_collector(req_data)
        }
        AggNodeData::Metric(req_data) => match req_data.collecting_for {
            StatsType::Sum
            | StatsType::Average
            | StatsType::Count
            | StatsType::Max
            | StatsType::Min
            | StatsType::Stats => build_segment_stats_collector(req_data),
            StatsType::ExtendedStats(sigma) => Ok(Box::new(
                SegmentExtendedStatsCollector::from_req(req_data, sigma),
            )),
            StatsType::Percentiles => Ok(Box::new(
                SegmentPercentilesCollector::from_req_and_validate(req_data),
            )),
        },
        AggNodeData::TopHits(req_data) => {
            Ok(Box::new(TopHitsSegmentCollector::from_req(*req_data)))
        }
        AggNodeData::Histogram(req_data) => {
            build_segment_histogram_collector(ctx, req_data, children)
        }
        AggNodeData::Range(req_data) => build_segment_range_collector(ctx, req_data, children),
        AggNodeData::Filter(req_data) => build_segment_filter_collector(ctx, *req_data, children),
        AggNodeData::Composite(req_data) => {
            let sub_agg = build_sub_agg_collectors(ctx, children)?;
            Ok(Box::new(
                crate::aggregation::bucket::SegmentCompositeCollector::from_req_and_validate(
                    req_data, sub_agg,
                )?,
            ))
        }
        AggNodeData::MultiTerms(req_data) => {
            build_segment_multi_terms_collector(ctx, req_data, children)
        }
    }
}

/// Builds the aggregation request tree for a segment.
///
/// This resolves the value sources of every aggregation and validates field types.
pub(crate) fn build_aggregations_data_from_req(
    aggs: &Aggregations,
    reader: &SegmentReader,
    segment_ordinal: SegmentOrdinal,
    context: &AggContextParams,
) -> crate::Result<Vec<AggNode>> {
    let mut agg_tree = Vec::with_capacity(aggs.len());
    for (name, agg) in aggs.iter() {
        let nodes = build_nodes(name, agg, reader, segment_ordinal, context, true)?;
        agg_tree.extend(nodes);
    }
    Ok(agg_tree)
}

/// Resolves the substitute value used for documents that have none.
///
/// Only the `Str` arms of [`get_missing_val_as_u64_lenient`] read the column's max value — they
/// place the sentinel one past the last real term ordinal — and that bound exists only for a
/// materialized column. A computed text source therefore cannot support `missing`; for numeric
/// types the argument is ignored, so any value will do.
fn missing_value_for_source(
    accessor: &dyn ValueSource,
    missing: &Key,
    field_name: &str,
) -> crate::Result<Option<u64>> {
    let column_type = accessor.column_type();
    let column_max_value = match accessor.as_column() {
        Some(column) => column.max_value(),
        None if column_type == ColumnType::Str => {
            return Err(crate::TantivyError::InvalidArgument(format!(
                "`missing` is not supported for the computed text value source `{field_name}`"
            )));
        }
        None => 0,
    };
    get_missing_val_as_u64_lenient(column_type, column_max_value, missing, field_name)
}

/// Extracts the materialized column, rejecting a computed source.
///
/// For the aggregations that read values through per-document random access or
/// `ColumnIndex::has_value`, neither of which `ValueSource` can express.
fn require_physical_column(
    source: &dyn ValueSource,
    field_name: &str,
    agg_kind: &str,
) -> crate::Result<Column<u64>> {
    source.as_column().cloned().ok_or_else(|| {
        crate::TantivyError::InvalidArgument(format!(
            "{agg_kind} does not support the computed value source `{field_name}`"
        ))
    })
}

fn build_nodes(
    agg_name: &str,
    req: &Aggregation,
    reader: &SegmentReader,
    segment_ordinal: SegmentOrdinal,
    context: &AggContextParams,
    is_top_level: bool,
) -> crate::Result<Vec<AggNode>> {
    use AggregationVariants::*;
    let value_sources = &context.value_sources;
    match &req.agg {
        Range(range_req) => {
            let accessor = get_value_source(
                reader,
                value_sources,
                &range_req.field,
                Some(get_numeric_or_date_column_types()),
            )?;
            let node_data = AggNodeData::Range(RangeAggReqData {
                accessor,
                name: agg_name.to_string(),
                req: range_req.clone(),
                is_top_level,
            });
            let children = build_children(&req.sub_aggregation, reader, segment_ordinal, context)?;
            Ok(vec![AggNode::new(node_data, children)])
        }
        Histogram(histo_req) => {
            let accessor = get_value_source(
                reader,
                value_sources,
                &histo_req.field,
                Some(get_numeric_or_date_column_types()),
            )?;
            let req_data =
                HistogramAggReqData::new(accessor, agg_name.to_string(), histo_req.clone(), false)?;
            let node_data = AggNodeData::Histogram(req_data);
            let children = build_children(&req.sub_aggregation, reader, segment_ordinal, context)?;
            Ok(vec![AggNode::new(node_data, children)])
        }
        DateHistogram(date_req) => {
            let accessor = get_value_source(
                reader,
                value_sources,
                &date_req.field,
                Some(&[ColumnType::DateTime]),
            )?;
            // Convert to histogram request, normalize to ns precision
            let mut histo_req = date_req.to_histogram_req()?;
            histo_req.normalize_date_time();
            let req_data =
                HistogramAggReqData::new(accessor, agg_name.to_string(), histo_req, true)?;
            let node_data = AggNodeData::Histogram(req_data);
            let children = build_children(&req.sub_aggregation, reader, segment_ordinal, context)?;
            Ok(vec![AggNode::new(node_data, children)])
        }
        Terms(terms_req) => build_terms_or_cardinality_nodes(
            agg_name,
            &terms_req.field,
            &terms_req.missing,
            reader,
            segment_ordinal,
            context,
            &req.sub_aggregation,
            TermsOrCardinalityRequest::Terms(terms_req.clone()),
            is_top_level,
        ),
        Cardinality(card_req) => build_terms_or_cardinality_nodes(
            agg_name,
            &card_req.field,
            &card_req.missing,
            reader,
            segment_ordinal,
            context,
            &req.sub_aggregation,
            TermsOrCardinalityRequest::Cardinality(card_req.clone()),
            is_top_level,
        ),
        Average(AverageAggregation { field, missing, .. })
        | Max(MaxAggregation { field, missing, .. })
        | Min(MinAggregation { field, missing, .. })
        | Stats(StatsAggregation { field, missing, .. })
        | ExtendedStats(ExtendedStatsAggregation { field, missing, .. })
        | Sum(SumAggregation { field, missing, .. })
        | Count(CountAggregation { field, missing, .. }) => {
            let allowed_column_types = if matches!(&req.agg, Count(_)) {
                Some(
                    &[
                        ColumnType::I64,
                        ColumnType::U64,
                        ColumnType::F64,
                        ColumnType::Str,
                        ColumnType::DateTime,
                        ColumnType::Bool,
                        ColumnType::IpAddr,
                    ][..],
                )
            } else {
                Some(get_numeric_or_date_column_types())
            };
            let collecting_for = match &req.agg {
                Average(_) => StatsType::Average,
                Max(_) => StatsType::Max,
                Min(_) => StatsType::Min,
                Stats(_) => StatsType::Stats,
                ExtendedStats(req) => StatsType::ExtendedStats(req.sigma),
                Sum(_) => StatsType::Sum,
                Count(_) => StatsType::Count,
                _ => {
                    return Err(crate::TantivyError::InvalidArgument(
                        "Internal error: unexpected aggregation type in metric aggregation \
                         handling."
                            .to_string(),
                    ))
                }
            };
            let accessor = get_value_source(reader, value_sources, field, allowed_column_types)?;
            let field_type = accessor.column_type();
            let node_data = AggNodeData::Metric(MetricAggReqData {
                accessor,
                name: agg_name.to_string(),
                collecting_for,
                missing: *missing,
                missing_u64: (*missing).and_then(|m| f64_to_fastfield_u64(m, &field_type)),
                is_number_or_date_type: matches!(
                    field_type,
                    ColumnType::I64 | ColumnType::U64 | ColumnType::F64 | ColumnType::DateTime
                ),
            });
            let children = build_children(&req.sub_aggregation, reader, segment_ordinal, context)?;
            Ok(vec![AggNode::new(node_data, children)])
        }
        // Percentiles handled as Metric as well
        AggregationVariants::Percentiles(percentiles_req) => {
            percentiles_req.validate()?;
            let accessor = get_value_source(
                reader,
                value_sources,
                percentiles_req.field_name(),
                Some(get_numeric_or_date_column_types()),
            )?;
            let field_type = accessor.column_type();
            let node_data = AggNodeData::Metric(MetricAggReqData {
                accessor,
                name: agg_name.to_string(),
                collecting_for: StatsType::Percentiles,
                missing: percentiles_req.missing,
                missing_u64: percentiles_req
                    .missing
                    .and_then(|m| f64_to_fastfield_u64(m, &field_type)),
                is_number_or_date_type: matches!(
                    field_type,
                    ColumnType::I64 | ColumnType::U64 | ColumnType::F64 | ColumnType::DateTime
                ),
            });
            let children = build_children(&req.sub_aggregation, reader, segment_ordinal, context)?;
            Ok(vec![AggNode::new(node_data, children)])
        }
        AggregationVariants::TopHits(top_hits_req) => {
            let mut top_hits = top_hits_req.clone();
            top_hits.validate_and_resolve_field_names(reader.fast_fields().columnar())?;
            let accessors: Vec<(Column<u64>, ColumnType)> = top_hits
                .field_names()
                .iter()
                .map(|field| {
                    let source = get_value_source(
                        reader,
                        value_sources,
                        field,
                        Some(get_numeric_or_date_column_types()),
                    )?;
                    // Sort fields are read one document at a time via `values_for_doc`, which has
                    // no block equivalent.
                    let column = require_physical_column(&*source, field, "top_hits")?;
                    Ok((column, source.column_type()))
                })
                .collect::<crate::Result<_>>()?;

            let value_accessors = top_hits
                .value_field_names()
                .iter()
                .map(|field_name| {
                    Ok((
                        field_name.to_string(),
                        get_dynamic_columns(reader, field_name)?,
                    ))
                })
                .collect::<crate::Result<_>>()?;

            let node_data = AggNodeData::TopHits(Box::new(TopHitsAggReqData {
                accessors,
                value_accessors,
                segment_ordinal,
                name: agg_name.to_string(),
                req: top_hits.clone(),
            }));
            let children = build_children(&req.sub_aggregation, reader, segment_ordinal, context)?;
            Ok(vec![AggNode::new(node_data, children)])
        }
        AggregationVariants::Composite(composite_req) => Ok(vec![build_composite_node(
            agg_name,
            reader,
            segment_ordinal,
            context,
            &req.sub_aggregation,
            composite_req,
        )?]),
        AggregationVariants::MultiTerms(multi_terms_req) => build_multi_terms_nodes(
            agg_name,
            reader,
            segment_ordinal,
            context,
            &req.sub_aggregation,
            multi_terms_req,
            is_top_level,
        ),
        AggregationVariants::Filter(filter_req) => {
            // Build the query and evaluator upfront
            let schema = reader.schema();
            let tokenizers = &context.tokenizers;
            let query = filter_req.parse_query(schema, tokenizers)?;
            let evaluator = crate::aggregation::bucket::DocumentQueryEvaluator::new(
                query,
                schema.clone(),
                reader,
            )?;

            let node_data = AggNodeData::Filter(Box::new(FilterAggReqData {
                name: agg_name.to_string(),
                segment_reader: reader.clone(),
                evaluator,
                is_top_level,
            }));
            let children = build_children(&req.sub_aggregation, reader, segment_ordinal, context)?;
            Ok(vec![AggNode::new(node_data, children)])
        }
    }
}

fn build_composite_node(
    agg_name: &str,
    reader: &SegmentReader,
    _segment_ordinal: SegmentOrdinal,
    context: &AggContextParams,
    sub_aggs: &Aggregations,
    req: &CompositeAggregation,
) -> crate::Result<AggNode> {
    let mut composite_accessors = Vec::with_capacity(req.sources.len());
    for source in &req.sources {
        let source_after_key_opt = req.after.get(source.name()).map(|k| &k.0);
        let source_accessor =
            CompositeSourceAccessors::build_for_source(reader, source, source_after_key_opt)?;
        composite_accessors.push(source_accessor);
    }
    let agg = CompositeAggReqData {
        name: agg_name.to_string(),
        req: req.clone(),
        composite_accessors,
    };
    let children = build_children(sub_aggs, reader, _segment_ordinal, context)?;
    Ok(AggNode::new(AggNodeData::Composite(agg), children))
}

fn build_multi_terms_nodes(
    agg_name: &str,
    reader: &SegmentReader,
    segment_ordinal: SegmentOrdinal,
    context: &AggContextParams,
    sub_aggs: &Aggregations,
    req: &MultiTermsAggregation,
    is_top_level: bool,
) -> crate::Result<Vec<AggNode>> {
    if req.terms.is_empty() {
        return Err(crate::TantivyError::InvalidArgument(
            "multi_terms aggregation requires at least one field".to_string(),
        ));
    }

    let value_sources = &context.value_sources;
    let mut accessors_by_field = Vec::with_capacity(req.terms.len());
    for field_def in &req.terms {
        let field_name = &field_def.field;
        let str_dict_column = reader.fast_fields().str(field_name)?;
        // multi_terms resolves missing values through `ColumnIndex::has_value` per document, and
        // exposes its columns on a public struct, so it stays physical-only.
        let columns =
            get_term_agg_accessors(reader, value_sources, field_name, &field_def.missing, true)?
                .into_iter()
                .map(|source| {
                    let column = require_physical_column(&*source, field_name, "multi_terms")?;
                    Ok((column, source.column_type()))
                })
                .collect::<crate::Result<Vec<_>>>()?;

        if let Some((_, column_type)) = columns
            .iter()
            .find(|(_, column_type)| *column_type == ColumnType::Bytes)
        {
            return Err(crate::TantivyError::InvalidArgument(format!(
                "multi_terms aggregation is not supported for column type {:?} in field {}",
                column_type, field_name
            )));
        }

        // Exactly one typed accessor choice carries missing handling. It checks all physical
        // columns before injecting the fallback, so a value in another type-specific collector is
        // not mistaken for a missing field and a genuinely missing document is not counted once
        // per type.
        let missing_accessor = prepare_multi_terms_missing(
            &columns,
            str_dict_column.as_ref(),
            field_def.missing.as_ref(),
            field_name,
        )?;

        let mut typed_accessors = Vec::with_capacity(columns.len());
        for (column_idx, (column, column_type)) in columns.into_iter().enumerate() {
            let missing = match &missing_accessor {
                Some((missing_idx, missing)) if *missing_idx == column_idx => Some(missing.clone()),
                _ => None,
            };
            typed_accessors.push((
                MultiTermsFieldAccessor {
                    column,
                    column_type,
                    str_dict_column: if column_type == ColumnType::Str {
                        str_dict_column.clone()
                    } else {
                        None
                    },
                    field: field_name.clone(),
                },
                missing,
            ));
        }
        accessors_by_field.push(typed_accessors);
    }

    // Fan out one collector for every Cartesian product of physical column choices. Collectors
    // share the aggregation name, so their intermediate buckets are merged by
    // `IntermediateAggregationResults::push`. As with terms aggregation type fan-out,
    // `segment_size` is applied per physical combination and the merged error/count metadata is
    // therefore the sum of those independently cut-off results.
    let mut field_combinations: Vec<
        Vec<(MultiTermsFieldAccessor, Option<MultiTermsMissingAccessor>)>,
    > = vec![Vec::with_capacity(req.terms.len())];
    for typed_accessors in accessors_by_field {
        let mut next = Vec::new();
        for field_choices in field_combinations {
            for typed_accessor in &typed_accessors {
                let mut combination = field_choices.clone();
                combination.push(typed_accessor.clone());
                next.push(combination);
            }
        }
        field_combinations = next;
    }

    let mut nodes = Vec::with_capacity(field_combinations.len());
    for field_choices in field_combinations {
        let (fields, missing_accessors) = field_choices.into_iter().unzip();
        let node_data = AggNodeData::MultiTerms(MultiTermsAggReqData {
            name: agg_name.to_string(),
            req: req.clone(),
            fields,
            missing_accessors,
            is_top_level,
        });
        let children = build_children(sub_aggs, reader, segment_ordinal, context)?;
        nodes.push(AggNode::new(node_data, children));
    }
    Ok(nodes)
}

fn prepare_multi_terms_missing(
    columns: &[(Column<u64>, ColumnType)],
    str_dict_column: Option<&StrColumn>,
    missing: Option<&Key>,
    field_name: &str,
) -> crate::Result<Option<(usize, MultiTermsMissingAccessor)>> {
    let Some(missing) = missing else {
        return Ok(None);
    };

    // Attach string fallbacks to the string column when one exists. A string fallback on any
    // other physical type is handled synthetically, just like the special terms missing
    // collector.
    let column_idx = if matches!(missing, Key::Str(_)) {
        columns
            .iter()
            .position(|(_, column_type)| *column_type == ColumnType::Str)
            .unwrap_or(0)
    } else {
        // Prefer an exact physical type for numeric missing values, then any numerical type, then
        // a string column (which accepts numeric fallbacks through a synthetic value).
        let preferred_type = match missing {
            Key::F64(_) => ColumnType::F64,
            Key::I64(_) => ColumnType::I64,
            Key::U64(_) => ColumnType::U64,
            Key::Str(_) => unreachable!("handled above"),
        };
        columns
            .iter()
            .position(|(_, column_type)| *column_type == preferred_type)
            .or_else(|| {
                columns
                    .iter()
                    .position(|(_, column_type)| column_type.numerical_type().is_some())
            })
            .or_else(|| {
                columns
                    .iter()
                    .position(|(_, column_type)| *column_type == ColumnType::Str)
            })
            .unwrap_or(0)
    };

    let (column, column_type) = &columns[column_idx];
    if !matches!(missing, Key::Str(_)) && *column_type != ColumnType::Str {
        // Validate the same lenient numeric coercions as a terms aggregation.
        get_missing_val_as_u64_lenient(*column_type, column.max_value(), missing, field_name)?;
    }

    // Reuse an existing term ordinal so real and missing values enter the same bucket before the
    // segment-level cutoff. Non-string columns and missing terms absent from the dictionary keep
    // using a collision-free sentinel.
    let existing_term_ord = match (missing, *column_type, str_dict_column) {
        (Key::Str(missing_str), ColumnType::Str, Some(str_dict_column)) => str_dict_column
            .dictionary()
            .term_ord(missing_str.as_bytes())?,
        _ => None,
    };

    // A full physical column means every document has a value for this logical field, so no typed
    // collector branch can ever emit the configured missing value.
    if columns
        .iter()
        .any(|(column, _)| column.get_cardinality().is_full())
    {
        return Ok(None);
    }

    let all_columns = Arc::from(
        columns
            .iter()
            .map(|(column, _)| column.clone())
            .collect::<Vec<_>>(),
    );
    Ok(Some((
        column_idx,
        MultiTermsMissingAccessor {
            all_columns,
            key: missing.clone(),
            missing_value: existing_term_ord.unwrap_or_else(|| find_missing_sentinel(column)),
        },
    )))
}

/// Returns a value that cannot collide with a value in `column`.
///
/// Usually one of the column bounds leaves a free value. Only a column whose bounds span the
/// entire `u64` domain requires the slower scan.
fn find_missing_sentinel(column: &Column<u64>) -> u64 {
    if let Some(sentinel) = column.max_value().checked_add(1) {
        return sentinel;
    }
    if let Some(sentinel) = column.min_value().checked_sub(1) {
        return sentinel;
    }

    // TODO: This is an extreme edge case that would be better handled by a collector that does
    // not use sentinel missing values. For now, we just scan the column.
    let values: FxHashSet<u64> = column.values.iter().collect();
    let mut sentinel = 1u64;
    while values.contains(&sentinel) {
        sentinel += 1;
    }
    sentinel
}

fn build_children(
    aggs: &Aggregations,
    reader: &SegmentReader,
    segment_ordinal: SegmentOrdinal,
    context: &AggContextParams,
) -> crate::Result<Vec<AggNode>> {
    let mut children = Vec::new();
    for (name, agg) in aggs.iter() {
        children.extend(build_nodes(
            name,
            agg,
            reader,
            segment_ordinal,
            context,
            false,
        )?);
    }
    Ok(children)
}

fn get_term_agg_accessors(
    reader: &SegmentReader,
    value_sources: &ValueSourceRegistry,
    field_name: &str,
    missing: &Option<Key>,
    include_bytes: bool,
) -> crate::Result<Vec<Box<dyn ValueSource>>> {
    // `terms` and `multi_terms` both explicitly reject `Bytes` columns downstream, which needs
    // to actually see them as a real column (rather than the empty shim below) to do so.
    // `cardinality` has no such rejection: it would hash raw `Bytes` term ordinals as if they
    // were comparable numeric values, but those ordinals are segment-local, so distinct byte
    // values in different segments could collide and be undercounted. Keep `Bytes` out of its
    // accessors entirely instead.
    let mut allowed_column_types = vec![
        ColumnType::I64,
        ColumnType::U64,
        ColumnType::F64,
        ColumnType::Str,
        ColumnType::DateTime,
        ColumnType::Bool,
        ColumnType::IpAddr,
    ];
    if include_bytes {
        allowed_column_types.push(ColumnType::Bytes);
    }

    // In case the column is empty we want the shim column to match the missing type
    let fallback_type = missing
        .as_ref()
        .map(|missing| match missing {
            Key::Str(_) => ColumnType::Str,
            Key::F64(_) => ColumnType::F64,
            Key::I64(_) => ColumnType::I64,
            Key::U64(_) => ColumnType::U64,
        })
        .unwrap_or(ColumnType::U64);

    let sources = get_all_value_sources(
        reader,
        value_sources,
        field_name,
        Some(&allowed_column_types),
        fallback_type,
    )?;

    Ok(sources)
}

enum TermsOrCardinalityRequest {
    Terms(TermsAggregation),
    Cardinality(CardinalityAggregationReq),
}
impl TermsOrCardinalityRequest {
    fn as_terms(&self) -> Option<&TermsAggregation> {
        match self {
            TermsOrCardinalityRequest::Terms(t) => Some(t),
            _ => None,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn build_terms_or_cardinality_nodes(
    agg_name: &str,
    field_name: &str,
    missing: &Option<Key>,
    reader: &SegmentReader,
    segment_ordinal: SegmentOrdinal,
    context: &AggContextParams,
    sub_aggs: &Aggregations,
    req: TermsOrCardinalityRequest,
    is_top_level: bool,
) -> crate::Result<Vec<AggNode>> {
    let mut nodes = Vec::new();

    let str_dict_column = reader.fast_fields().str(field_name)?;
    let value_sources = &context.value_sources;

    let include_bytes = matches!(req, TermsOrCardinalityRequest::Terms(_));
    let sources =
        get_term_agg_accessors(reader, value_sources, field_name, missing, include_bytes)?;

    // Special handling when missing + multi column or incompatible type on text/date.
    let missing_and_more_than_one_col = sources.len() > 1 && missing.is_some();
    let text_on_non_text_col = sources.len() == 1
        && sources[0].column_type() != ColumnType::Str
        && matches!(missing, Some(Key::Str(_)));

    let use_special_missing_agg = missing_and_more_than_one_col || text_on_non_text_col;

    // If special missing handling is required, build a MissingTerm node that carries all
    // accessors (across any column types) for existence checks.
    if use_special_missing_agg {
        let fallback_type = missing
            .as_ref()
            .map(|missing| match missing {
                Key::Str(_) => ColumnType::Str,
                Key::F64(_) => ColumnType::F64,
                Key::I64(_) => ColumnType::I64,
                Key::U64(_) => ColumnType::U64,
            })
            .unwrap_or(ColumnType::U64);
        // This path inspects `ColumnIndex::has_value` per document per accessor to decide which
        // documents are missing across several typed columns. There is no way to ask that through
        // `ValueSource`, so it stays physical-only.
        let all_accessors =
            get_all_value_sources(reader, value_sources, field_name, None, fallback_type)?
                .into_iter()
                .map(|source| {
                    let column = require_physical_column(
                        &*source,
                        field_name,
                        "terms with `missing` across multiple column types",
                    )?;
                    Ok((column, source.column_type()))
                })
                .collect::<crate::Result<Vec<_>>>()?;
        // This case only happens when we have term aggregation, or we fail
        let req = req.as_terms().cloned().ok_or_else(|| {
            crate::TantivyError::InvalidArgument(
                "Cardinality aggregation with missing on non-text/number field is not supported."
                    .to_string(),
            )
        })?;

        let children = build_children(sub_aggs, reader, segment_ordinal, context)?;
        let node_data = AggNodeData::MissingTerm(MissingTermAggReqData {
            accessors: all_accessors,
            name: agg_name.to_string(),
            req,
        });
        nodes.push(AggNode::new(node_data, children));
    }

    // Add one node per accessor
    for accessor in sources {
        let column_type = accessor.column_type();
        let missing_value_for_accessor = if use_special_missing_agg {
            None
        } else if let Some(m) = missing.as_ref() {
            missing_value_for_source(&*accessor, m, field_name)?
        } else {
            None
        };

        let children = build_children(sub_aggs, reader, segment_ordinal, context)?;
        let node_data = match req {
            TermsOrCardinalityRequest::Terms(ref req) => {
                let mut allowed_term_ids = None;
                if req.include.is_some() || req.exclude.is_some() {
                    if column_type != ColumnType::Str {
                        // Skip non-string columns entirely when filtering is requested.
                        // When excluding, the behavior could be to include non-string values
                        continue;
                    }
                    let str_col = str_dict_column
                        .as_ref()
                        .expect("str_dict_column must exist for string column");
                    allowed_term_ids = build_allowed_term_ids_for_str(
                        str_col,
                        &req.include,
                        &req.exclude,
                        missing.is_some(),
                    )?;
                };
                AggNodeData::Terms(TermsAggReqData {
                    accessor,
                    str_dict_column: str_dict_column.clone(),
                    missing_value_for_accessor,
                    name: agg_name.to_string(),
                    req: TermsAggregationInternal::from_req(req),
                    sub_aggregations: sub_aggs.clone(),
                    allowed_term_ids,
                    is_top_level,
                })
            }
            TermsOrCardinalityRequest::Cardinality(ref req) => {
                // `str_dict_column` is computed once per field; for JSON paths
                // with mixed types it's `Some` even on the numeric req_data.
                // Cardinality only consults it for the str column path, so
                // gate by column_type to avoid driving non-str collectors
                // through the coupon-cache path.
                let str_dict_column_for_req = if column_type == ColumnType::Str {
                    str_dict_column.clone()
                } else {
                    None
                };
                AggNodeData::Cardinality(CardinalityAggReqData {
                    accessor,
                    str_dict_column: str_dict_column_for_req,
                    missing_value_for_accessor,
                    name: agg_name.to_string(),
                    req: req.clone(),
                })
            }
        };
        nodes.push(AggNode::new(node_data, children));
    }

    Ok(nodes)
}

/// Builds a single BitSet of allowed term ordinals for a string dictionary column according to
/// include/exclude parameters.
///
/// When `reserve_missing_sentinel` is true, the bitset will have 1 additional slot for the missing
/// term ordinal
fn build_allowed_term_ids_for_str(
    str_col: &StrColumn,
    include: &Option<IncludeExcludeParam>,
    exclude: &Option<IncludeExcludeParam>,
    reserve_missing_sentinel: bool,
) -> crate::Result<Option<BitSet>> {
    let mut allowed: Option<BitSet> = None;
    let missing_sentinel_adjustment = if reserve_missing_sentinel { 1 } else { 0 };
    let allowed_capacity = str_col.dictionary().num_terms() as u32 + missing_sentinel_adjustment;
    if let Some(include) = include {
        // add matches
        allowed = Some(BitSet::with_max_value(allowed_capacity));
        let allowed = allowed.as_mut().unwrap();
        for_each_matching_term_ord(str_col, include, |ord| {
            let _ = allowed.insert(ord);
        })?;
    };

    if let Some(exclude) = exclude {
        if allowed.is_none() {
            // Start with all terms allowed
            allowed = Some(BitSet::with_max_value_and_full(allowed_capacity));
        }
        let allowed = allowed.as_mut().unwrap();
        for_each_matching_term_ord(str_col, exclude, |ord| allowed.remove(ord))?;
    }

    Ok(allowed)
}

/// Apply a callback to each matching term ordinal for the given include/exclude parameter.
fn for_each_matching_term_ord(
    str_col: &StrColumn,
    param: &IncludeExcludeParam,
    mut cb: impl FnMut(u32),
) -> crate::Result<()> {
    match param {
        IncludeExcludeParam::Regex(pattern) => {
            let re = Regex::new(pattern).map_err(|e| {
                crate::TantivyError::InvalidArgument(format!("Invalid regex `{}`: {}", pattern, e))
            })?;
            // TODO: we can handle patterns like `^prefix.*` more efficiently
            let mut stream = str_col
                .dictionary()
                .search(re)
                .without_keys()
                .into_stream()?;
            while stream.advance() {
                cb(stream.term_ord() as u32);
            }
        }
        IncludeExcludeParam::Values(values) => {
            let set: FxHashSet<&str> = values.iter().map(|s| s.as_str()).collect();
            let mut stream = str_col.dictionary().stream()?;
            while stream.advance() {
                if let Ok(key_str) = std::str::from_utf8(stream.key()) {
                    if set.contains(key_str) {
                        cb(stream.term_ord() as u32);
                    }
                }
            }
        }
    }
    Ok(())
}

/// Convert the aggregation tree to something serializable and easy to read.
#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
pub struct AggTreeViewNode {
    pub name: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub children: Vec<AggTreeViewNode>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::tests::get_test_index_2_segments;

    fn agg_from_json(val: serde_json::Value) -> crate::aggregation::agg_req::Aggregation {
        serde_json::from_value(val).unwrap()
    }

    fn multi_terms_req_data(agg_tree: &[AggNode]) -> Vec<&MultiTermsAggReqData> {
        agg_tree
            .iter()
            .filter_map(|node| match &node.data {
                AggNodeData::MultiTerms(req_data) => Some(req_data),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn test_multi_terms_expands_physical_column_cartesian_product() -> crate::Result<()> {
        let mut schema_builder = crate::schema::Schema::builder();
        let attrs = schema_builder.add_json_field("attrs", crate::schema::FAST);
        let score = schema_builder
            .add_u64_field("score", crate::schema::NumericOptions::default().set_fast());
        let index = crate::Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests()?;
        writer.add_document(
            crate::doc!(attrs => json!({"left": "x", "right": "y"}), score => 1u64),
        )?;
        writer.add_document(
            crate::doc!(attrs => json!({"left": 10.5, "right": true}), score => 2u64),
        )?;
        writer.commit()?;

        let agg = agg_from_json(json!({
            "multi_terms": {
                "terms": [
                    {"field": "attrs.left"},
                    {"field": "attrs.right"}
                ]
            },
            "aggs": {
                "sum_score": {"sum": {"field": "score"}}
            }
        }));
        let aggs: Aggregations = vec![("mt".to_string(), agg)].into_iter().collect();
        let searcher = index.reader()?.searcher();
        let agg_tree = build_aggregations_data_from_req(
            &aggs,
            searcher.segment_reader(0),
            0,
            &Default::default(),
        )?;
        let multi_terms_req_data = multi_terms_req_data(&agg_tree);

        assert_eq!(agg_tree.len(), 4);
        assert_eq!(multi_terms_req_data.len(), 4);
        assert!(agg_tree.iter().all(|node| node.children.len() == 1));

        let actual_types: FxHashSet<Vec<ColumnType>> = multi_terms_req_data
            .iter()
            .map(|req_data| {
                req_data
                    .fields
                    .iter()
                    .map(|field| field.column_type)
                    .collect()
            })
            .collect();
        let expected_types: FxHashSet<Vec<ColumnType>> = [
            vec![ColumnType::F64, ColumnType::Bool],
            vec![ColumnType::F64, ColumnType::Str],
            vec![ColumnType::Str, ColumnType::Bool],
            vec![ColumnType::Str, ColumnType::Str],
        ]
        .into_iter()
        .collect();
        assert_eq!(actual_types, expected_types);
        assert!(multi_terms_req_data
            .iter()
            .flat_map(|req_data| &req_data.fields)
            .all(|field| {
                field.str_dict_column.is_some() == (field.column_type == ColumnType::Str)
            }));

        Ok(())
    }

    #[test]
    fn test_multi_terms_skips_missing_when_any_physical_column_is_full() -> crate::Result<()> {
        let mut schema_builder = crate::schema::Schema::builder();
        let attrs = schema_builder.add_json_field("attrs", crate::schema::FAST);
        let index = crate::Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests()?;
        writer.add_document(crate::doc!(attrs => json!({"value": ["a", 10.5]})))?;
        writer.add_document(crate::doc!(attrs => json!({"value": "b"})))?;
        writer.commit()?;

        let agg = agg_from_json(json!({
            "multi_terms": {
                "terms": [{"field": "attrs.value", "missing": "MISSING"}]
            }
        }));
        let aggs: Aggregations = vec![("mt".to_string(), agg)].into_iter().collect();
        let searcher = index.reader()?.searcher();
        let agg_tree = build_aggregations_data_from_req(
            &aggs,
            searcher.segment_reader(0),
            0,
            &Default::default(),
        )?;
        let multi_terms_req_data = multi_terms_req_data(&agg_tree);

        assert_eq!(multi_terms_req_data.len(), 2);
        assert!(multi_terms_req_data
            .iter()
            .any(|req_data| req_data.fields[0].column.get_cardinality().is_full()));
        assert!(multi_terms_req_data
            .iter()
            .all(|req_data| req_data.missing_accessors.iter().all(Option::is_none)));

        Ok(())
    }

    #[test]
    fn test_tree_roots_and_expansion_terms_missing_on_numeric() -> crate::Result<()> {
        let index = get_test_index_2_segments(true)?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let seg_reader = searcher.segment_reader(0u32);

        // Build request with:
        // 1) Terms on numeric field with missing as string => expands to MissingTerm + Terms
        // 2) Avg metric
        // 3) Terms on string with child histogram
        let terms_score_missing = agg_from_json(json!({
            "terms": {"field": "score", "missing": "NA"}
        }));
        let avg_score = agg_from_json(json!({
            "avg": {"field": "score"}
        }));
        let terms_string_with_child = agg_from_json(json!({
            "terms": {"field": "string_id"},
            "aggs": {
                "histo": {"histogram": {"field": "score", "interval": 10.0}}
            }
        }));

        let aggs: Aggregations = vec![
            ("t_score_missing_str".to_string(), terms_score_missing),
            ("avg_score".to_string(), avg_score),
            ("terms_string".to_string(), terms_string_with_child),
        ]
        .into_iter()
        .collect();

        let agg_tree =
            build_aggregations_data_from_req(&aggs, seg_reader, 0u32, &Default::default())?;
        let printed_nodes = get_view_tree(&agg_tree);
        let printed = serde_json::to_value(&printed_nodes).unwrap();

        let expected = json!([
            {"name": "avg_score", "kind": "Metric"},
            {"name": "t_score_missing_str", "kind": "MissingTerm"},
            {"name": "t_score_missing_str", "kind": "Terms"},
            {"name": "terms_string", "kind": "Terms", "children": [
                {"name": "histo", "kind": "Histogram"}
            ]}
        ]);
        assert_eq!(
            printed,
            expected,
            "tree json:\n{}",
            serde_json::to_string_pretty(&printed).unwrap()
        );

        Ok(())
    }
}
