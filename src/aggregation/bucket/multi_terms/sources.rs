use std::cmp::Ordering;

use columnar::column_values::{CompactHit, CompactSpaceU64Accessor};
use columnar::{Column, ColumnType, MonotonicallyMappableToU64, StrColumn, TermOrdHit};

use super::numeric_types::num_proj;
use super::numeric_types::num_proj::ProjectedNumber;
use crate::aggregation::accessor_helpers::get_numeric_or_date_column_types;
use crate::aggregation::bucket::composite::CompositeAggregationSource;
use crate::aggregation::bucket::{parse_into_milliseconds, CalendarInterval, MissingOrder, Order};
use crate::aggregation::intermediate_agg_result::CompositeIntermediateKey;
use crate::{SegmentReader, TantivyError};

/// Bucket key types in ascending sort order; numeric types share one position.
#[derive(Ord, PartialOrd, PartialEq, Eq)]
pub(crate) enum KeyType {
    Bool,
    Str,
    Numeric,
    IpAddr,
    DateTime,
}

impl From<ColumnType> for KeyType {
    fn from(column_type: ColumnType) -> Self {
        match column_type {
            ColumnType::Bool => Self::Bool,
            ColumnType::Str => Self::Str,
            ColumnType::F64 | ColumnType::I64 | ColumnType::U64 => Self::Numeric,
            ColumnType::IpAddr => Self::IpAddr,
            ColumnType::DateTime => Self::DateTime,
            ColumnType::Bytes => panic!("unsupported"),
        }
    }
}

impl CompositeIntermediateKey {
    /// Returns the key's type for sorting. Missing keys must be compared separately.
    pub(crate) fn key_type(&self) -> KeyType {
        match self {
            Self::Bool(_) => KeyType::Bool,
            Self::Str(_) => KeyType::Str,
            Self::F64(_) | Self::I64(_) | Self::U64(_) => KeyType::Numeric,
            Self::IpAddr(_) => KeyType::IpAddr,
            Self::DateTime(_) => KeyType::DateTime,
            Self::Null => panic!("null must be handled separately"),
        }
    }
}

/// Sources, size, and optional pagination for a page of bucket keys.
#[derive(Debug)]
pub(crate) struct KeyPage {
    pub(crate) sources: Vec<BucketSource>,
    pub(crate) size: usize,
}

/// Mapping from column values to bucket keys.
#[derive(Debug, Clone, Copy)]
pub(crate) enum BucketKeyMapping {
    /// Use the column value as the key.
    Terms,
    /// Map numerical values to interval buckets.
    Histogram {
        /// Width of each bucket.
        interval: f64,
    },
    /// Map dates to fixed or calendar interval buckets.
    DateHistogram {
        /// Parsed date interval.
        interval: DateInterval,
    },
}

/// One source column and its string dictionary.
#[derive(Debug, Clone)]
pub(crate) struct SourceColumn {
    pub column: Column<u64>,
    pub column_type: ColumnType,
    /// String dictionary for terms sources.
    pub str_dict_column: Option<StrColumn>,
}

/// A field's columns, mapping to bucket keys, ordering, and pagination key.
#[derive(Debug, Clone)]
pub(crate) struct BucketSource {
    pub mapping: BucketKeyMapping,
    pub order: Order,
    pub missing_order: MissingOrder,
    pub missing_bucket: bool,
    pub columns: Vec<SourceColumn>,
    pub after: SourceAfterKey,
}

/// Pagination position for one source, projected into column or bucket values.
#[derive(Debug, Clone)]
pub(crate) enum SourceAfterKey {
    None,
    Missing,
    Value {
        /// First column whose type matches or follows the pagination key's type.
        column: usize,
        key: ColumnAfterKey,
    },
}

impl BucketSource {
    /// Whether missing values precede column values in the requested order.
    pub fn missing_first(&self) -> bool {
        matches!(
            (self.missing_order, self.order),
            (MissingOrder::First, _) | (MissingOrder::Default, Order::Asc)
        )
    }

    /// Opens the field's columns and projects the after key into column space.
    ///
    /// `after` is absent when pagination is not requested, and is `Null` when
    /// the previous page ended on a missing value for this source.
    pub fn build(
        reader: &SegmentReader,
        source: &CompositeAggregationSource,
        after: Option<&CompositeIntermediateKey>,
    ) -> crate::Result<Self> {
        let field = source.field();
        let order = source.order();
        let mapping = match source {
            CompositeAggregationSource::Terms(_) => BucketKeyMapping::Terms,
            CompositeAggregationSource::Histogram(source) => BucketKeyMapping::Histogram {
                interval: source.interval,
            },
            CompositeAggregationSource::DateHistogram(source) => BucketKeyMapping::DateHistogram {
                interval: DateInterval::parse(&source.fixed_interval, source.calendar_interval)?,
            },
        };
        if matches!(after, Some(CompositeIntermediateKey::Null)) && !source.missing_bucket() {
            return Err(TantivyError::InvalidArgument(
                "the 'after' key for a source cannot be null when 'missing_bucket' is false"
                    .to_string(),
            ));
        }

        let allowed_column_types: &[ColumnType] = match mapping {
            BucketKeyMapping::Terms => &[
                ColumnType::I64,
                ColumnType::U64,
                ColumnType::F64,
                ColumnType::Str,
                ColumnType::DateTime,
                ColumnType::Bool,
                ColumnType::IpAddr,
            ],
            BucketKeyMapping::Histogram { .. } => get_numeric_or_date_column_types(),
            BucketKeyMapping::DateHistogram { .. } => &[ColumnType::DateTime],
        };
        let mut columns_and_types = reader
            .fast_fields()
            .u64_lenient_for_type_all(Some(allowed_column_types), field)?;
        let after_column = if matches!(mapping, BucketKeyMapping::Terms) {
            columns_and_types.sort_by_key(|(_, column_type)| KeyType::from(*column_type));
            if order == Order::Desc {
                columns_and_types.reverse();
            }
            pagination_start_column(&columns_and_types, after, order)
        } else {
            0
        };
        let columns: Vec<SourceColumn> = columns_and_types
            .into_iter()
            .map(|(column, column_type)| {
                Ok(SourceColumn {
                    column,
                    column_type,
                    str_dict_column: if column_type == ColumnType::Str {
                        reader.fast_fields().str(field)?
                    } else {
                        None
                    },
                })
            })
            .collect::<crate::Result<_>>()?;

        let after = match after {
            None => SourceAfterKey::None,
            Some(CompositeIntermediateKey::Null) => SourceAfterKey::Missing,
            Some(key) => SourceAfterKey::Value {
                column: after_column,
                key: match (mapping, key) {
                    (BucketKeyMapping::Terms, key) => match columns.get(after_column) {
                        Some(column) => ColumnAfterKey::project(column, key, field, order)?,
                        None => ColumnAfterKey::before_values(order),
                    },
                    (
                        BucketKeyMapping::Histogram { interval },
                        CompositeIntermediateKey::F64(key),
                    ) => num_proj::f64_to_i64(*key / interval).map(i64::to_u64),
                    (
                        BucketKeyMapping::DateHistogram { .. },
                        CompositeIntermediateKey::DateTime(key),
                    ) => ColumnAfterKey::Exact(key.to_u64()),
                    (BucketKeyMapping::Histogram { .. }, _) => {
                        return Err(TantivyError::InvalidArgument(
                            "After key type invalid for histogram source".to_string(),
                        ));
                    }
                    (BucketKeyMapping::DateHistogram { .. }, _) => {
                        return Err(TantivyError::InvalidArgument(
                            "After key type invalid for date histogram source".to_string(),
                        ));
                    }
                },
            },
        };
        Ok(BucketSource {
            mapping,
            order,
            missing_order: source.missing_order(),
            missing_bucket: source.missing_bucket(),
            columns,
            after,
        })
    }
}

/// Index of the first column whose type matches or follows the after key's type.
fn pagination_start_column(
    sorted_columns: &[(Column<u64>, ColumnType)],
    after: Option<&CompositeIntermediateKey>,
    order: Order,
) -> usize {
    let after_key = match after {
        None | Some(CompositeIntermediateKey::Null) => return 0,
        Some(key) => key,
    };
    let after_type = after_key.key_type();
    sorted_columns
        .iter()
        .position(|(_, column_type)| match order {
            Order::Asc => KeyType::from(*column_type) >= after_type,
            Order::Desc => KeyType::from(*column_type) <= after_type,
        })
        .unwrap_or(sorted_columns.len())
}

/// Date interval in nanoseconds or calendar units.
#[derive(Clone, Copy, Debug)]
pub(crate) enum DateInterval {
    FixedNanoseconds(i64),
    Calendar(CalendarInterval),
}

impl DateInterval {
    /// Parses a fixed or calendar interval; exactly one must be provided.
    pub fn parse(
        fixed_interval: &Option<String>,
        calendar_interval: Option<CalendarInterval>,
    ) -> crate::Result<Self> {
        match (fixed_interval, calendar_interval) {
            (Some(_), Some(_)) | (None, None) => Err(TantivyError::InvalidArgument(
                "date histogram source must set exactly one of fixed_interval or calendar_interval"
                    .to_string(),
            )),
            (Some(fixed_interval), None) => {
                let fixed_interval_ms = parse_into_milliseconds(fixed_interval)?;
                Ok(Self::FixedNanoseconds(fixed_interval_ms * 1_000_000))
            }
            (None, Some(calendar_interval)) => Ok(Self::Calendar(calendar_interval)),
        }
    }
}

/// Pagination key projected into column values or mapped bucket values.
/// Inexact numeric conversions and absent dictionary keys use the next representable value.
pub(crate) type ColumnAfterKey = ProjectedNumber<u64>;

impl ColumnAfterKey {
    /// Compares a column value with the pagination key projected into the same column.
    pub fn compare_column_value(&self, value: u64) -> Ordering {
        match self {
            Self::Exact(after) => value.cmp(after),
            Self::Next(after) if value >= *after => Ordering::Greater,
            Self::Next(_) | Self::AfterLast => Ordering::Less,
        }
    }

    /// Projects the after key into column values. The caller compares column types first.
    pub fn project(
        column: &SourceColumn,
        after_key: &CompositeIntermediateKey,
        field: &str,
        order: Order,
    ) -> crate::Result<Self> {
        use CompositeIntermediateKey as Key;
        Ok(match (column.column_type, after_key) {
            (ColumnType::Bytes, _) => panic!("unsupported"),
            (ColumnType::I64, Key::I64(key)) | (ColumnType::DateTime, Key::DateTime(key)) => {
                Self::Exact(key.to_u64())
            }
            (ColumnType::I64, Key::U64(key)) => num_proj::u64_to_i64(*key).map(i64::to_u64),
            (ColumnType::I64, Key::F64(key)) => num_proj::f64_to_i64(*key).map(i64::to_u64),
            (ColumnType::U64, Key::I64(key)) => num_proj::i64_to_u64(*key),
            (ColumnType::U64, Key::U64(key)) => Self::Exact(*key),
            (ColumnType::U64, Key::F64(key)) => num_proj::f64_to_u64(*key),
            (ColumnType::F64, Key::I64(key)) => num_proj::i64_to_f64(*key).map(f64::to_u64),
            (ColumnType::F64, Key::U64(key)) => num_proj::u64_to_f64(*key).map(f64::to_u64),
            (ColumnType::F64, Key::F64(key)) => Self::Exact(key.to_u64()),
            (ColumnType::Bool, Key::Bool(key)) => Self::Exact(key.to_u64()),
            (ColumnType::Str, Key::Str(key)) => {
                let dictionary = column
                    .str_dict_column
                    .as_ref()
                    .expect("dictionary missing for string column")
                    .dictionary();
                match dictionary.term_ord_or_next(key).map_err(|_| {
                    TantivyError::InvalidArgument(format!(
                        "failed to lookup after_key '{key}' for field '{field}'"
                    ))
                })? {
                    TermOrdHit::Exact(ord) => Self::Exact(ord),
                    TermOrdHit::Next(ord) => Self::Next(ord),
                }
            }
            (ColumnType::IpAddr, Key::IpAddr(key)) => {
                let column = column
                    .column
                    .values
                    .clone()
                    .downcast_arc::<CompactSpaceU64Accessor>()
                    .map_err(|_| {
                        TantivyError::AggregationError(
                            crate::aggregation::AggregationError::InternalError(
                                "type mismatch: could not downcast to CompactSpaceU64Accessor"
                                    .to_string(),
                            ),
                        )
                    })?;
                match column.u128_to_next_compact(key.to_bits()) {
                    CompactHit::Exact(value) => Self::Exact(value as u64),
                    CompactHit::Next(value) => Self::Next(value as u64),
                    CompactHit::AfterLast => Self::AfterLast,
                }
            }
            // Missing keys are handled separately; later types need no value comparison.
            _ => Self::before_values(order),
        })
    }

    /// Places the pagination key before all values in the requested order.
    fn before_values(order: Order) -> Self {
        match order {
            Order::Asc => Self::Next(0),
            Order::Desc => Self::AfterLast,
        }
    }
}
