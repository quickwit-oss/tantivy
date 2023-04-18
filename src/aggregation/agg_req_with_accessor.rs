//! This will enhance the request tree with access to the fastfield and metadata.

use columnar::{Column, ColumnBlockAccessor, ColumnType, StrColumn};

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
use crate::SegmentReader;

#[derive(Clone, Default)]
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

#[derive(Clone)]
pub struct AggregationWithAccessor {
    /// In general there can be buckets without fast field access, e.g. buckets that are created
    /// based on search terms. So eventually this needs to be Option or moved.
    pub(crate) accessor: Column<u64>,
    pub(crate) str_dict_column: Option<StrColumn>,
    pub(crate) field_type: ColumnType,
    pub(crate) sub_aggregation: AggregationsWithAccessor,
    pub(crate) limits: AggregationLimits,
    pub(crate) column_block_accessor: ColumnBlockAccessor<u64>,
    pub(crate) agg: Aggregation,
}

impl AggregationWithAccessor {
    fn try_from_agg(
        agg: &Aggregation,
        sub_aggregation: &Aggregations,
        reader: &SegmentReader,
        limits: AggregationLimits,
    ) -> crate::Result<AggregationWithAccessor> {
        let mut str_dict_column = None;
        use AggregationVariants::*;
        let (accessor, field_type) = match &agg.agg {
            Range(RangeAggregation {
                field: field_name, ..
            }) => get_ff_reader_and_validate(
                reader,
                field_name,
                Some(get_numeric_or_date_column_types()),
            )?,
            Histogram(HistogramAggregation {
                field: field_name, ..
            }) => get_ff_reader_and_validate(
                reader,
                field_name,
                Some(get_numeric_or_date_column_types()),
            )?,
            DateHistogram(DateHistogramAggregationReq {
                field: field_name, ..
            }) => get_ff_reader_and_validate(
                reader,
                field_name,
                Some(get_numeric_or_date_column_types()),
            )?,
            Terms(TermsAggregation {
                field: field_name, ..
            }) => {
                str_dict_column = reader.fast_fields().str(field_name)?;
                get_ff_reader_and_validate(reader, field_name, None)?
            }
            Average(AverageAggregation { field: field_name })
            | Count(CountAggregation { field: field_name })
            | Max(MaxAggregation { field: field_name })
            | Min(MinAggregation { field: field_name })
            | Stats(StatsAggregation { field: field_name })
            | Sum(SumAggregation { field: field_name }) => {
                let (accessor, field_type) = get_ff_reader_and_validate(
                    reader,
                    field_name,
                    Some(get_numeric_or_date_column_types()),
                )?;

                (accessor, field_type)
            }
            Percentiles(percentiles) => {
                let (accessor, field_type) = get_ff_reader_and_validate(
                    reader,
                    percentiles.field_name(),
                    Some(get_numeric_or_date_column_types()),
                )?;
                (accessor, field_type)
            }
        };
        let sub_aggregation = sub_aggregation.clone();
        Ok(AggregationWithAccessor {
            accessor,
            field_type,
            sub_aggregation: get_aggs_with_accessor_and_validate(
                &sub_aggregation,
                reader,
                &limits.clone(),
            )?,
            agg: agg.clone(),
            str_dict_column,
            limits,
            column_block_accessor: Default::default(),
        })
    }
}

fn get_numeric_or_date_column_types() -> &'static [ColumnType] {
    &[
        ColumnType::F64,
        ColumnType::U64,
        ColumnType::I64,
        ColumnType::DateTime,
    ]
}

pub(crate) fn get_aggs_with_accessor_and_validate(
    aggs: &Aggregations,
    reader: &SegmentReader,
    limits: &AggregationLimits,
) -> crate::Result<AggregationsWithAccessor> {
    let mut aggss = Vec::new();
    for (key, agg) in aggs.iter() {
        aggss.push((
            key.to_string(),
            AggregationWithAccessor::try_from_agg(
                agg,
                agg.sub_aggregation(),
                reader,
                limits.clone(),
            )?,
        ));
    }
    Ok(AggregationsWithAccessor::from_data(
        VecWithNames::from_entries(aggss),
    ))
}

/// Get fast field reader with given cardinatility.
fn get_ff_reader_and_validate(
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
