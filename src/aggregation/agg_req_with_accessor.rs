//! This will enhance the request tree with access to the fastfield and metadata.

use std::collections::HashMap;
use std::io;

use columnar::{Column, ColumnBlockAccessor, ColumnType, DynamicColumn, StrColumn};

use super::agg_limits::ResourceLimitGuard;
use super::agg_req::{Aggregation, AggregationVariants, Aggregations};
use super::bucket::{
    DateHistogramAggregationReq, HistogramAggregation, RangeAggregation, TermsAggregation,
};
use super::metric::{
    AverageAggregation, CountAggregation, ExtendedStatsAggregation, MaxAggregation, MinAggregation,
    StatsAggregation, SumAggregation,
};
use super::segment_agg_result::AggregationLimits;
use super::VecWithNames;
use crate::aggregation::{f64_to_fastfield_u64, Key};
use crate::index::SegmentReader;
use crate::SegmentOrdinal;

#[derive(Default)]
pub(crate) struct AggregationsWithAccessor {
    pub aggs: VecWithNames<AggregationWithAccessor>,
}

impl AggregationsWithAccessor {
    fn from_data(aggs: VecWithNames<AggregationWithAccessor>) -> Self {
        Self { aggs }
    }

    pub fn is_empty(&self) -> bool {
        self.aggs.is_empty()
    }
}

pub struct AggregationWithAccessor {
    pub(crate) segment_ordinal: SegmentOrdinal,
    /// In general there can be buckets without fast field access, e.g. buckets that are created
    /// based on search terms. That is not that case currently, but eventually this needs to be
    /// Option or moved.
    pub(crate) accessor: Column<u64>,
    /// Load insert u64 for missing use case
    pub(crate) missing_value_for_accessor: Option<u64>,
    pub(crate) str_dict_column: Option<StrColumn>,
    pub(crate) field_type: ColumnType,
    pub(crate) sub_aggregation: AggregationsWithAccessor,
    pub(crate) limits: ResourceLimitGuard,
    pub(crate) column_block_accessor: ColumnBlockAccessor<u64>,
    /// Used for missing term aggregation, which checks all columns for existence.
    /// And also for `top_hits` aggregation, which may sort on multiple fields.
    /// By convention the missing aggregation is chosen, when this property is set
    /// (instead bein set in `agg`).
    /// If this needs to used by other aggregations, we need to refactor this.
    // NOTE: we can make all other aggregations use this instead of the `accessor` and `field_type`
    // (making them obsolete) But will it have a performance impact?
    pub(crate) accessors: Vec<(Column<u64>, ColumnType)>,
    /// Map field names to all associated column accessors.
    /// This field is used for `docvalue_fields`, which is currently only supported for `top_hits`.
    pub(crate) value_accessors: HashMap<String, Vec<DynamicColumn>>,
    pub(crate) agg: Aggregation,
}

impl AggregationWithAccessor {
    /// May return multiple accessors if the aggregation is e.g. on mixed field types.
    fn try_from_agg(
        agg: &Aggregation,
        sub_aggregation: &Aggregations,
        reader: &SegmentReader,
        segment_ordinal: SegmentOrdinal,
        limits: AggregationLimits,
    ) -> crate::Result<Vec<AggregationWithAccessor>> {
        let mut agg = agg.clone();

        let add_agg_with_accessor = |agg: &Aggregation,
                                     accessor: Column<u64>,
                                     column_type: ColumnType,
                                     aggs: &mut Vec<AggregationWithAccessor>|
         -> crate::Result<()> {
            let res = AggregationWithAccessor {
                segment_ordinal,
                accessor,
                accessors: Default::default(),
                value_accessors: Default::default(),
                field_type: column_type,
                sub_aggregation: get_aggs_with_segment_accessor_and_validate(
                    sub_aggregation,
                    reader,
                    segment_ordinal,
                    &limits,
                )?,
                agg: agg.clone(),
                limits: limits.new_guard(),
                missing_value_for_accessor: None,
                str_dict_column: None,
                column_block_accessor: Default::default(),
            };
            aggs.push(res);
            Ok(())
        };

        let add_agg_with_accessors = |agg: &Aggregation,
                                      accessors: Vec<(Column<u64>, ColumnType)>,
                                      aggs: &mut Vec<AggregationWithAccessor>,
                                      value_accessors: HashMap<String, Vec<DynamicColumn>>|
         -> crate::Result<()> {
            let (accessor, field_type) = accessors.first().expect("at least one accessor");
            let res = AggregationWithAccessor {
                segment_ordinal,
                // TODO: We should do away with the `accessor` field altogether
                accessor: accessor.clone(),
                value_accessors,
                field_type: *field_type,
                accessors,
                sub_aggregation: get_aggs_with_segment_accessor_and_validate(
                    sub_aggregation,
                    reader,
                    segment_ordinal,
                    &limits,
                )?,
                agg: agg.clone(),
                limits: limits.new_guard(),
                missing_value_for_accessor: None,
                str_dict_column: None,
                column_block_accessor: Default::default(),
            };
            aggs.push(res);
            Ok(())
        };

        let mut res: Vec<AggregationWithAccessor> = Vec::new();
        use AggregationVariants::*;

        match agg.agg {
            Range(RangeAggregation {
                field: ref field_name,
                ..
            }) => {
                let (accessor, column_type) =
                    get_ff_reader(reader, field_name, Some(get_numeric_or_date_column_types()))?;
                add_agg_with_accessor(&agg, accessor, column_type, &mut res)?;
            }
            Histogram(HistogramAggregation {
                field: ref field_name,
                ..
            }) => {
                let (accessor, column_type) =
                    get_ff_reader(reader, field_name, Some(get_numeric_or_date_column_types()))?;
                add_agg_with_accessor(&agg, accessor, column_type, &mut res)?;
            }
            DateHistogram(DateHistogramAggregationReq {
                field: ref field_name,
                ..
            }) => {
                let (accessor, column_type) =
                    // Only DateTime is supported for DateHistogram
                    get_ff_reader(reader, field_name, Some(&[ColumnType::DateTime]))?;
                add_agg_with_accessor(&agg, accessor, column_type, &mut res)?;
            }
            Terms(TermsAggregation {
                field: ref field_name,
                ref missing,
                ..
            }) => {
                let str_dict_column = reader.fast_fields().str(field_name)?;
                let allowed_column_types = [
                    ColumnType::I64,
                    ColumnType::U64,
                    ColumnType::F64,
                    ColumnType::Str,
                    ColumnType::DateTime,
                    ColumnType::Bool,
                    ColumnType::IpAddr,
                    // ColumnType::Bytes Unsupported
                ];

                // In case the column is empty we want the shim column to match the missing type
                let fallback_type = missing
                    .as_ref()
                    .map(|missing| match missing {
                        Key::Str(_) => ColumnType::Str,
                        Key::F64(_) => ColumnType::F64,
                    })
                    .unwrap_or(ColumnType::U64);
                let column_and_types = get_all_ff_reader_or_empty(
                    reader,
                    field_name,
                    Some(&allowed_column_types),
                    fallback_type,
                )?;
                let missing_and_more_than_one_col = column_and_types.len() > 1 && missing.is_some();
                let text_on_non_text_col = column_and_types.len() == 1
                    && column_and_types[0].1.numerical_type().is_some()
                    && missing
                        .as_ref()
                        .map(|m| matches!(m, Key::Str(_)))
                        .unwrap_or(false);

                // Actually we could convert the text to a number and have the fast path, if it is
                // provided in Rfc3339 format. But this use case is probably common
                // enough to justify the effort.
                let text_on_date_col = column_and_types.len() == 1
                    && column_and_types[0].1 == ColumnType::DateTime
                    && missing
                        .as_ref()
                        .map(|m| matches!(m, Key::Str(_)))
                        .unwrap_or(false);

                let use_special_missing_agg =
                    missing_and_more_than_one_col || text_on_non_text_col || text_on_date_col;
                if use_special_missing_agg {
                    let column_and_types =
                        get_all_ff_reader_or_empty(reader, field_name, None, fallback_type)?;

                    let accessors = column_and_types
                        .iter()
                        .map(|c_t| (c_t.0.clone(), c_t.1))
                        .collect();
                    add_agg_with_accessors(&agg, accessors, &mut res, Default::default())?;
                }

                for (accessor, column_type) in column_and_types {
                    let missing_value_term_agg = if use_special_missing_agg {
                        None
                    } else {
                        missing.clone()
                    };

                    let missing_value_for_accessor = if let Some(missing) =
                        missing_value_term_agg.as_ref()
                    {
                        get_missing_val(column_type, missing, agg.agg.get_fast_field_names()[0])?
                    } else {
                        None
                    };

                    let agg = AggregationWithAccessor {
                        segment_ordinal,
                        missing_value_for_accessor,
                        accessor,
                        accessors: Default::default(),
                        value_accessors: Default::default(),
                        field_type: column_type,
                        sub_aggregation: get_aggs_with_segment_accessor_and_validate(
                            sub_aggregation,
                            reader,
                            segment_ordinal,
                            &limits,
                        )?,
                        agg: agg.clone(),
                        str_dict_column: str_dict_column.clone(),
                        limits: limits.new_guard(),
                        column_block_accessor: Default::default(),
                    };
                    res.push(agg);
                }
            }
            Average(AverageAggregation {
                field: ref field_name,
                ..
            })
            | Count(CountAggregation {
                field: ref field_name,
                ..
            })
            | Max(MaxAggregation {
                field: ref field_name,
                ..
            })
            | Min(MinAggregation {
                field: ref field_name,
                ..
            })
            | Stats(StatsAggregation {
                field: ref field_name,
                ..
            })
            | ExtendedStats(ExtendedStatsAggregation {
                field: ref field_name,
                ..
            })
            | Sum(SumAggregation {
                field: ref field_name,
                ..
            }) => {
                let (accessor, column_type) =
                    get_ff_reader(reader, field_name, Some(get_numeric_or_date_column_types()))?;
                add_agg_with_accessor(&agg, accessor, column_type, &mut res)?;
            }
            Percentiles(ref percentiles) => {
                let (accessor, column_type) = get_ff_reader(
                    reader,
                    percentiles.field_name(),
                    Some(get_numeric_or_date_column_types()),
                )?;
                add_agg_with_accessor(&agg, accessor, column_type, &mut res)?;
            }
            TopHits(ref mut top_hits) => {
                top_hits.validate_and_resolve_field_names(reader.fast_fields().columnar())?;
                let accessors: Vec<(Column<u64>, ColumnType)> = top_hits
                    .field_names()
                    .iter()
                    .map(|field| {
                        get_ff_reader(reader, field, Some(get_numeric_or_date_column_types()))
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

                add_agg_with_accessors(&agg, accessors, &mut res, value_accessors)?;
            }
        };

        Ok(res)
    }
}

fn get_missing_val(
    column_type: ColumnType,
    missing: &Key,
    field_name: &str,
) -> crate::Result<Option<u64>> {
    let missing_val = match missing {
        Key::Str(_) if column_type == ColumnType::Str => Some(u64::MAX),
        // Allow fallback to number on text fields
        Key::F64(_) if column_type == ColumnType::Str => Some(u64::MAX),
        Key::F64(val) if column_type.numerical_type().is_some() => {
            f64_to_fastfield_u64(*val, &column_type)
        }
        _ => {
            return Err(crate::TantivyError::InvalidArgument(format!(
                "Missing value {:?} for field {} is not supported for column type {:?}",
                missing, field_name, column_type
            )));
        }
    };
    Ok(missing_val)
}

fn get_numeric_or_date_column_types() -> &'static [ColumnType] {
    &[
        ColumnType::F64,
        ColumnType::U64,
        ColumnType::I64,
        ColumnType::DateTime,
    ]
}

pub(crate) fn get_aggs_with_segment_accessor_and_validate(
    aggs: &Aggregations,
    reader: &SegmentReader,
    segment_ordinal: SegmentOrdinal,
    limits: &AggregationLimits,
) -> crate::Result<AggregationsWithAccessor> {
    let mut aggss = Vec::new();
    for (key, agg) in aggs.iter() {
        let aggs = AggregationWithAccessor::try_from_agg(
            agg,
            agg.sub_aggregation(),
            reader,
            segment_ordinal,
            limits.clone(),
        )?;
        for agg in aggs {
            aggss.push((key.to_string(), agg));
        }
    }
    Ok(AggregationsWithAccessor::from_data(
        VecWithNames::from_entries(aggss),
    ))
}

/// Get fast field reader or empty as default.
fn get_ff_reader(
    reader: &SegmentReader,
    field_name: &str,
    allowed_column_types: Option<&[ColumnType]>,
) -> crate::Result<(columnar::Column<u64>, ColumnType)> {
    let ff_fields = reader.fast_fields();
    let ff_field_with_type = ff_fields
        .u64_lenient_for_type(allowed_column_types, field_name)?
        .unwrap_or_else(|| {
            (
                Column::build_empty_column(reader.num_docs()),
                ColumnType::U64,
            )
        });
    Ok(ff_field_with_type)
}

fn get_dynamic_columns(
    reader: &SegmentReader,
    field_name: &str,
) -> crate::Result<Vec<columnar::DynamicColumn>> {
    let ff_fields = reader.fast_fields().dynamic_column_handles(field_name)?;
    let cols = ff_fields
        .iter()
        .map(|h| h.open())
        .collect::<io::Result<_>>()?;
    assert!(!ff_fields.is_empty(), "field {} not found", field_name);
    Ok(cols)
}

/// Get all fast field reader or empty as default.
///
/// Is guaranteed to return at least one column.
fn get_all_ff_reader_or_empty(
    reader: &SegmentReader,
    field_name: &str,
    allowed_column_types: Option<&[ColumnType]>,
    fallback_type: ColumnType,
) -> crate::Result<Vec<(columnar::Column<u64>, ColumnType)>> {
    let ff_fields = reader.fast_fields();
    let mut ff_field_with_type =
        ff_fields.u64_lenient_for_type_all(allowed_column_types, field_name)?;
    if ff_field_with_type.is_empty() {
        ff_field_with_type.push((Column::build_empty_column(reader.num_docs()), fallback_type));
    }
    Ok(ff_field_with_type)
}
