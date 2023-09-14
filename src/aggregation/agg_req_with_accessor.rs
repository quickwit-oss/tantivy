//! This will enhance the request tree with access to the fastfield and metadata.

use columnar::{Column, ColumnBlockAccessor, ColumnType, StrColumn};

use super::agg_limits::ResourceLimitGuard;
use super::agg_req::{Aggregation, AggregationVariants, Aggregations};
use super::bucket::{
    DateHistogramAggregationReq, HistogramAggregation, RangeAggregation, TermsAggregation,
};
use super::metric::{
    AverageAggregation, CountAggregation, MaxAggregation, MinAggregation, StatsAggregation,
    SumAggregation,
};
use super::segment_agg_result::AggregationLimits;
use super::VecWithNames;
use crate::aggregation::{f64_to_fastfield_u64, Key};
use crate::SegmentReader;

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
    /// By convention the missing aggregation is chosen, when this property is set
    /// (instead bein set in `agg`).
    /// If this needs to used by other aggregations, we need to refactor this.
    pub(crate) accessors: Vec<Column<u64>>,
    pub(crate) agg: Aggregation,
}

impl AggregationWithAccessor {
    /// May return multiple accessors if the aggregation is e.g. on mixed field types.
    fn try_from_agg(
        agg: &Aggregation,
        sub_aggregation: &Aggregations,
        reader: &SegmentReader,
        limits: AggregationLimits,
    ) -> crate::Result<Vec<AggregationWithAccessor>> {
        let add_agg_with_accessor = |accessor: Column<u64>,
                                     column_type: ColumnType,
                                     aggs: &mut Vec<AggregationWithAccessor>|
         -> crate::Result<()> {
            let res = AggregationWithAccessor {
                accessor,
                accessors: Vec::new(),
                field_type: column_type,
                sub_aggregation: get_aggs_with_segment_accessor_and_validate(
                    sub_aggregation,
                    reader,
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
        match &agg.agg {
            Range(RangeAggregation {
                field: field_name, ..
            }) => {
                let (accessor, column_type) =
                    get_ff_reader(reader, field_name, Some(get_numeric_or_date_column_types()))?;
                add_agg_with_accessor(accessor, column_type, &mut res)?;
            }
            Histogram(HistogramAggregation {
                field: field_name, ..
            }) => {
                let (accessor, column_type) =
                    get_ff_reader(reader, field_name, Some(get_numeric_or_date_column_types()))?;
                add_agg_with_accessor(accessor, column_type, &mut res)?;
            }
            DateHistogram(DateHistogramAggregationReq {
                field: field_name, ..
            }) => {
                let (accessor, column_type) =
                    get_ff_reader(reader, field_name, Some(get_numeric_or_date_column_types()))?;
                add_agg_with_accessor(accessor, column_type, &mut res)?;
            }
            Terms(TermsAggregation {
                field: field_name,
                missing,
                ..
            }) => {
                let str_dict_column = reader.fast_fields().str(field_name)?;
                let allowed_column_types = [
                    ColumnType::I64,
                    ColumnType::U64,
                    ColumnType::F64,
                    ColumnType::Str,
                    ColumnType::DateTime,
                    // ColumnType::Bytes Unsupported
                    // ColumnType::Bool Unsupported
                    // ColumnType::IpAddr Unsupported
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

                    let accessors: Vec<Column> =
                        column_and_types.iter().map(|(a, _)| a.clone()).collect();
                    let agg_wit_acc = AggregationWithAccessor {
                        missing_value_for_accessor: None,
                        accessor: accessors[0].clone(),
                        accessors,
                        field_type: ColumnType::U64,
                        sub_aggregation: get_aggs_with_segment_accessor_and_validate(
                            sub_aggregation,
                            reader,
                            &limits,
                        )?,
                        agg: agg.clone(),
                        str_dict_column: str_dict_column.clone(),
                        limits: limits.new_guard(),
                        column_block_accessor: Default::default(),
                    };
                    res.push(agg_wit_acc);
                }

                for (accessor, column_type) in column_and_types {
                    let missing_value_term_agg = if use_special_missing_agg {
                        None
                    } else {
                        missing.clone()
                    };

                    let missing_value_for_accessor =
                        if let Some(missing) = missing_value_term_agg.as_ref() {
                            get_missing_val(column_type, missing, agg.agg.get_fast_field_name())?
                        } else {
                            None
                        };

                    let agg = AggregationWithAccessor {
                        missing_value_for_accessor,
                        accessor,
                        accessors: Vec::new(),
                        field_type: column_type,
                        sub_aggregation: get_aggs_with_segment_accessor_and_validate(
                            sub_aggregation,
                            reader,
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
                field: field_name, ..
            })
            | Count(CountAggregation {
                field: field_name, ..
            })
            | Max(MaxAggregation {
                field: field_name, ..
            })
            | Min(MinAggregation {
                field: field_name, ..
            })
            | Stats(StatsAggregation {
                field: field_name, ..
            })
            | Sum(SumAggregation {
                field: field_name, ..
            }) => {
                let (accessor, column_type) =
                    get_ff_reader(reader, field_name, Some(get_numeric_or_date_column_types()))?;
                add_agg_with_accessor(accessor, column_type, &mut res)?;
            }
            Percentiles(percentiles) => {
                let (accessor, column_type) = get_ff_reader(
                    reader,
                    percentiles.field_name(),
                    Some(get_numeric_or_date_column_types()),
                )?;
                add_agg_with_accessor(accessor, column_type, &mut res)?;
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
    limits: &AggregationLimits,
) -> crate::Result<AggregationsWithAccessor> {
    let mut aggss = Vec::new();
    for (key, agg) in aggs.iter() {
        let aggs = AggregationWithAccessor::try_from_agg(
            agg,
            agg.sub_aggregation(),
            reader,
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
