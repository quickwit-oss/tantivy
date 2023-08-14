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
        let mut missing_value_term_agg = None;
        let mut str_dict_column = None;
        use AggregationVariants::*;
        let acc_field_types: Vec<(Column, ColumnType)> = match &agg.agg {
            Range(RangeAggregation {
                field: field_name, ..
            }) => vec![get_ff_reader(
                reader,
                field_name,
                Some(get_numeric_or_date_column_types()),
            )?],
            Histogram(HistogramAggregation {
                field: field_name, ..
            }) => vec![get_ff_reader(
                reader,
                field_name,
                Some(get_numeric_or_date_column_types()),
            )?],
            DateHistogram(DateHistogramAggregationReq {
                field: field_name, ..
            }) => vec![get_ff_reader(
                reader,
                field_name,
                Some(get_numeric_or_date_column_types()),
            )?],
            Terms(TermsAggregation {
                field: field_name,
                missing,
                ..
            }) => {
                missing_value_term_agg = missing.clone();
                str_dict_column = reader.fast_fields().str(field_name)?;
                let allowed_column_types = [
                    ColumnType::I64,
                    ColumnType::U64,
                    ColumnType::F64,
                    ColumnType::Str,
                    // ColumnType::Bytes Unsupported
                    // ColumnType::Bool Unsupported
                    // ColumnType::IpAddr Unsupported
                    // ColumnType::DateTime Unsupported
                ];

                // In case the column is empty we want the shim column to match the missing type
                let fallback_type = missing
                    .as_ref()
                    .map(|missing| match missing {
                        Key::Str(_) => ColumnType::Str,
                        Key::F64(_) => ColumnType::F64,
                    })
                    .unwrap_or(ColumnType::U64);
                get_all_ff_reader_or_empty(
                    reader,
                    field_name,
                    Some(&allowed_column_types),
                    fallback_type,
                )?
            }
            Average(AverageAggregation { field: field_name })
            | Count(CountAggregation { field: field_name })
            | Max(MaxAggregation { field: field_name })
            | Min(MinAggregation { field: field_name })
            | Stats(StatsAggregation { field: field_name })
            | Sum(SumAggregation { field: field_name }) => {
                let (accessor, field_type) =
                    get_ff_reader(reader, field_name, Some(get_numeric_or_date_column_types()))?;

                vec![(accessor, field_type)]
            }
            Percentiles(percentiles) => {
                let (accessor, field_type) = get_ff_reader(
                    reader,
                    percentiles.field_name(),
                    Some(get_numeric_or_date_column_types()),
                )?;
                vec![(accessor, field_type)]
            }
        };

        let aggs: Vec<AggregationWithAccessor> = acc_field_types
            .into_iter()
            .map(|(accessor, column_type)| {
                let missing_value_for_accessor =
                    if let Some(missing) = missing_value_term_agg.as_ref() {
                        get_missing_val(column_type, missing, agg.agg.get_fast_field_name())?
                    } else {
                        None
                    };

                Ok(AggregationWithAccessor {
                    missing_value_for_accessor,
                    accessor,
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
                })
            })
            .collect::<crate::Result<_>>()?;
        Ok(aggs)
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
