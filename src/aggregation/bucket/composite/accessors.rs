use std::fmt::Debug;
use std::net::IpAddr;
use std::str::FromStr;

use columnar::column_values::{CompactHit, CompactSpaceU64Accessor};
use columnar::{Column, ColumnType, MonotonicallyMappableToU64, StrColumn, TermOrdHit};

use crate::aggregation::accessor_helpers::{get_all_ff_readers, get_numeric_or_date_column_types};
use crate::aggregation::agg_result::CompositeKey;
use crate::aggregation::bucket::composite::numeric_types::num_proj;
use crate::aggregation::bucket::composite::numeric_types::num_proj::ProjectedNumber;
use crate::aggregation::bucket::{
    parse_into_milliseconds, CalendarInterval, CompositeAggregation, CompositeAggregationSource,
    MissingOrder, Order,
};
use crate::aggregation::date::parse_date;
use crate::aggregation::segment_agg_result::SegmentAggregationCollector;
use crate::schema::IntoIpv6Addr;
use crate::{SegmentReader, TantivyError};

/// Contains all information required by the SegmentCompositeCollector to perform the
/// composite aggregation on a segment.
pub struct CompositeAggReqData {
    /// Note: sub_aggregation_blueprint is filled later when building collectors
    pub sub_aggregation_blueprint: Option<Box<dyn SegmentAggregationCollector>>,
    /// The name of the aggregation.
    pub name: String,
    /// The normalized term aggregation request.
    pub req: CompositeAggregation,
    /// Accessors for each source, each source can have multiple accessors (columns).
    pub composite_accessors: Vec<CompositeSourceAccessors>,
}

/// Accessors for a single column in a composite source.
pub struct CompositeAccessor {
    /// The fast field column
    pub column: Column<u64>,
    /// The column type
    pub column_type: ColumnType,
    /// Term dictionary if the column type is Str
    ///
    /// Only used by term sources
    pub str_dict_column: Option<StrColumn>,
    /// Parsed date interval for date histogram sources
    pub date_histogram_interval: PrecomputedDateInterval,
}

/// Accessors to all the columns that belong to the field of a composite source.
pub struct CompositeSourceAccessors {
    /// The accessors for this source
    pub accessors: Vec<CompositeAccessor>,
    /// The key after which to start collecting results. Applies to the first
    /// column of the source.
    pub after_key: PrecomputedAfterKey,

    /// The column index the after_key applies to. The after_key only applies to
    /// one column. Columns before should be skipped. Columns after should be
    /// kept without comparison to the after_key.
    pub after_key_accessor_idx: usize,

    /// Whether to skip missing values because of the after_key. Skipping only
    /// applies if the value for previous columns were exactly equal to the
    /// corresponding after keys (is_on_after_key).
    pub skip_missing: bool,

    /// The after key was set to null to indicate that the last collected key
    /// was a missing value.
    pub is_after_key_explicit_missing: bool,
}

impl CompositeSourceAccessors {
    /// Creates a new set of accessors for the composite source.
    ///
    /// Precomputes some values to make collection faster.
    pub fn build_for_source(
        reader: &SegmentReader,
        source: &CompositeAggregationSource,
        // First option is None when no after key was set in the query, the
        // second option is None when the after key was set but its value for
        // this source was set to `null`
        source_after_key_opt: Option<&CompositeKey>,
    ) -> crate::Result<Self> {
        let is_after_key_explicit_missing = source_after_key_opt
            .map(|after_key| matches!(after_key, CompositeKey::Null))
            .unwrap_or(false);
        let mut skip_missing = false;
        if let Some(CompositeKey::Null) = source_after_key_opt {
            if !source.missing_bucket() {
                return Err(TantivyError::InvalidArgument(
                    "the 'after' key for a source cannot be null when 'missing_bucket' is false"
                        .to_string(),
                ));
            }
        } else if source_after_key_opt.is_some() {
            // if missing buckets come first and we have a non null after key, we skip missing
            if MissingOrder::First == source.missing_order() {
                skip_missing = true;
            }
            if MissingOrder::Default == source.missing_order() && Order::Asc == source.order() {
                skip_missing = true;
            }
        };

        match source {
            CompositeAggregationSource::Terms(source) => {
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
                let mut columns_and_types =
                    get_all_ff_readers(reader, &source.field, Some(&allowed_column_types))?;

                columns_and_types
                    .sort_by_key(|(_, col_type)| col_type_order_key(col_type, source.order));
                let mut after_key_accessor_idx = 0;
                if let Some(source_after_key_explicit_opt) = source_after_key_opt {
                    after_key_accessor_idx = skip_for_key(
                        &columns_and_types,
                        &source_after_key_explicit_opt,
                        source.missing_order,
                        source.order,
                    )?;
                }

                let source_collectors: Vec<CompositeAccessor> = columns_and_types
                    .into_iter()
                    .map(|(column, column_type)| {
                        Ok(CompositeAccessor {
                            column,
                            column_type,
                            str_dict_column: reader.fast_fields().str(&source.field)?,
                            date_histogram_interval: PrecomputedDateInterval::NotApplicable,
                        })
                    })
                    .collect::<crate::Result<_>>()?;

                let after_key = if let Some(first_col) =
                    source_collectors.get(after_key_accessor_idx)
                {
                    match source_after_key_opt {
                        Some(after_key) => PrecomputedAfterKey::precompute(
                            &first_col,
                            after_key,
                            &source.field,
                            source.missing_order,
                            source.order,
                        )?,
                        None => {
                            precompute_missing_after_key(false, source.missing_order, source.order)
                        }
                    }
                } else {
                    // if no columns, we don't care about the after_key
                    PrecomputedAfterKey::Next(0)
                };

                Ok(CompositeSourceAccessors {
                    accessors: source_collectors,
                    is_after_key_explicit_missing,
                    skip_missing,
                    after_key,
                    after_key_accessor_idx,
                })
            }
            CompositeAggregationSource::Histogram(source) => {
                let column_and_types: Vec<(Column, ColumnType)> = get_all_ff_readers(
                    reader,
                    &source.field,
                    Some(get_numeric_or_date_column_types()),
                )?;
                let source_collectors: Vec<CompositeAccessor> = column_and_types
                    .into_iter()
                    .map(|(column, column_type)| {
                        Ok(CompositeAccessor {
                            column,
                            column_type,
                            str_dict_column: None,
                            date_histogram_interval: PrecomputedDateInterval::NotApplicable,
                        })
                    })
                    .collect::<crate::Result<_>>()?;
                let after_key = match source_after_key_opt {
                    Some(CompositeKey::I64(key)) => {
                        PrecomputedAfterKey::Exact((*key as f64).to_u64())
                    }
                    Some(CompositeKey::U64(key)) => {
                        PrecomputedAfterKey::Exact((*key as f64).to_u64())
                    }
                    Some(CompositeKey::F64(key)) => {
                        PrecomputedAfterKey::Exact((*key as f64).to_u64())
                    }
                    Some(CompositeKey::Null) => {
                        precompute_missing_after_key(true, source.missing_order, source.order)
                    }
                    None => precompute_missing_after_key(true, source.missing_order, source.order),
                    _ => {
                        return Err(crate::TantivyError::InvalidArgument(
                            "After key type invalid for interval composite source".to_string(),
                        ));
                    }
                };
                Ok(CompositeSourceAccessors {
                    accessors: source_collectors,
                    is_after_key_explicit_missing,
                    skip_missing,
                    after_key,
                    after_key_accessor_idx: 0,
                })
            }
            CompositeAggregationSource::DateHistogram(source) => {
                let column_and_types =
                    get_all_ff_readers(reader, &source.field, Some(&[ColumnType::DateTime]))?;
                let date_histogram_interval =
                    PrecomputedDateInterval::from_date_histogram_source_intervals(
                        &source.fixed_interval,
                        source.calendar_interval,
                    )?;
                let source_collectors: Vec<CompositeAccessor> = column_and_types
                    .into_iter()
                    .map(|(column, column_type)| {
                        Ok(CompositeAccessor {
                            column,
                            column_type,
                            str_dict_column: None,
                            date_histogram_interval,
                        })
                    })
                    .collect::<crate::Result<_>>()?;
                let after_key = match source_after_key_opt {
                    Some(CompositeKey::I64(key)) => PrecomputedAfterKey::Exact(key.to_u64()),
                    Some(CompositeKey::Null) => {
                        precompute_missing_after_key(true, source.missing_order, source.order)
                    }
                    None => precompute_missing_after_key(true, source.missing_order, source.order),
                    _ => {
                        return Err(crate::TantivyError::InvalidArgument(
                            "After key type invalid for interval composite source".to_string(),
                        ));
                    }
                };
                Ok(CompositeSourceAccessors {
                    accessors: source_collectors,
                    is_after_key_explicit_missing,
                    skip_missing,
                    after_key,
                    after_key_accessor_idx: 0,
                })
            }
        }
    }
}

/// Sort orders:
/// - Asc: Bool->Str->F64/I64/U64->DateTime/IpAddr
/// - Desc: U64/I64/F64->Str->Bool->DateTime/IpAddr
fn col_type_order_key(col_type: &ColumnType, composite_order: Order) -> i32 {
    let apply_order = match composite_order {
        Order::Asc => 1,
        Order::Desc => -1,
    };
    match col_type {
        ColumnType::Bool => 1 * apply_order,
        ColumnType::Str => 2 * apply_order,
        // numeric types are coerced so it will be either U64, I64 or F64
        ColumnType::F64 => 3 * apply_order,
        ColumnType::I64 => 3 * apply_order,
        ColumnType::U64 => 3 * apply_order,
        // DateTime/IpAddr cannot be automatically deduced from
        // json, so if present we are guaranteed to have exactly
        // one column
        ColumnType::DateTime => 4,
        ColumnType::IpAddr => 4,
        ColumnType::Bytes => panic!("unsupported"),
    }
}

fn find_skip_idx<T>(
    columns_and_types: &Vec<(T, ColumnType)>,
    order: Order,
    skip_until_col_type_order_key: i32,
) -> crate::Result<usize> {
    for (idx, (_, col_type)) in columns_and_types.iter().enumerate() {
        let col_type_order = col_type_order_key(col_type, order);
        if col_type_order >= skip_until_col_type_order_key {
            return Ok(idx);
        }
    }
    Ok(columns_and_types.len())
}

fn skip_for_key<T>(
    columns_and_types: &Vec<(T, ColumnType)>,
    after_key: &CompositeKey,
    missing_order: MissingOrder,
    order: Order,
) -> crate::Result<usize> {
    match (after_key, order) {
        // Asc: Bool->Str->F64/I64/U64->DateTime/IpAddr
        (CompositeKey::Bool(_), Order::Asc) => find_skip_idx(columns_and_types, order, 1),
        (CompositeKey::Str(_), Order::Asc) => find_skip_idx(columns_and_types, order, 2),
        (CompositeKey::F64(_) | CompositeKey::I64(_) | CompositeKey::U64(_), Order::Asc) => {
            find_skip_idx(columns_and_types, order, 3)
        }
        // Desc: U64/I64/F64->Str->Bool->DateTime/IpAddr
        (CompositeKey::F64(_) | CompositeKey::I64(_) | CompositeKey::U64(_), Order::Desc) => {
            find_skip_idx(columns_and_types, order, -3)
        }
        (CompositeKey::Str(_), Order::Desc) => find_skip_idx(columns_and_types, order, -2),
        (CompositeKey::Bool(_), Order::Desc) => find_skip_idx(columns_and_types, order, -1),
        (CompositeKey::Null, _) => {
            match (missing_order, order) {
                (MissingOrder::First, _) | (MissingOrder::Default, Order::Asc) => {
                    Ok(0) // don't skip any columns
                }
                (MissingOrder::Last, _) | (MissingOrder::Default, Order::Desc) => {
                    // all columns are skipped
                    Ok(columns_and_types.len())
                }
            }
        }
    }
}

fn precompute_missing_after_key(
    is_after_key_explicit_missing: bool,
    missing_order: MissingOrder,
    order: Order,
) -> PrecomputedAfterKey {
    let after_last = PrecomputedAfterKey::AfterLast;
    let before_first = PrecomputedAfterKey::Next(0);
    match (is_after_key_explicit_missing, missing_order, order) {
        (true, MissingOrder::First, Order::Asc) => before_first,
        (true, MissingOrder::First, Order::Desc) => after_last,
        (true, MissingOrder::Last, Order::Asc) => after_last,
        (true, MissingOrder::Last, Order::Desc) => before_first,
        (true, MissingOrder::Default, Order::Asc) => before_first,
        (true, MissingOrder::Default, Order::Desc) => after_last,
        (false, _, Order::Asc) => before_first,
        (false, _, Order::Desc) => after_last,
    }
}

/// A parsed representation of the date interval for date histogram sources
#[derive(Clone, Copy, Debug)]
pub enum PrecomputedDateInterval {
    /// This is not a date histogram source
    NotApplicable,
    /// Source was configured with a fixed interval
    FixedMilliseconds(i64),
    /// Source was configured with a calendar interval
    Calendar(CalendarInterval),
}

impl PrecomputedDateInterval {
    /// Validates the date histogram source interval fields and parses a date interval from them.
    pub fn from_date_histogram_source_intervals(
        fixed_interval: &Option<String>,
        calendar_interval: Option<CalendarInterval>,
    ) -> crate::Result<Self> {
        match (fixed_interval, calendar_interval) {
            (Some(_), Some(_)) | (None, None) => Err(TantivyError::InvalidArgument(
                "date histogram source must one and only one of fixed_interval or \
                 calendar_interval set"
                    .to_string(),
            )),
            (Some(fixed_interval), None) => {
                let fixed_interval_ms = parse_into_milliseconds(&fixed_interval)?;
                Ok(PrecomputedDateInterval::FixedMilliseconds(
                    fixed_interval_ms,
                ))
            }
            (None, Some(calendar_interval)) => {
                Ok(PrecomputedDateInterval::Calendar(calendar_interval))
            }
        }
    }
}

/// The after key projected to the u64 column space
///
/// Some column types (term, IP) might not have an exact representation of the
/// specified after key
#[derive(Debug)]
pub enum PrecomputedAfterKey {
    /// The after key could be exactly represented in the column space.
    Exact(u64),
    /// The after key could not be exactly represented exactly represented, so
    /// this is the next closest one.
    Next(u64),
    /// The after key could not be represented in the column space, it is
    /// greater than all value
    AfterLast,
}

impl From<TermOrdHit> for PrecomputedAfterKey {
    fn from(hit: TermOrdHit) -> Self {
        match hit {
            TermOrdHit::Exact(ord) => PrecomputedAfterKey::Exact(ord),
            // TermOrdHit represents AfterLast as Next(u64::MAX), we keep it as is
            TermOrdHit::Next(ord) => PrecomputedAfterKey::Next(ord),
        }
    }
}

impl From<CompactHit> for PrecomputedAfterKey {
    fn from(hit: CompactHit) -> Self {
        match hit {
            CompactHit::Exact(ord) => PrecomputedAfterKey::Exact(ord as u64),
            CompactHit::Next(ord) => PrecomputedAfterKey::Next(ord as u64),
            CompactHit::AfterLast => PrecomputedAfterKey::AfterLast,
        }
    }
}

impl<T: MonotonicallyMappableToU64> From<ProjectedNumber<T>> for PrecomputedAfterKey {
    fn from(num: ProjectedNumber<T>) -> Self {
        match num {
            ProjectedNumber::Exact(number) => PrecomputedAfterKey::Exact(number.to_u64()),
            ProjectedNumber::Next(number) => PrecomputedAfterKey::Next(number.to_u64()),
            ProjectedNumber::AfterLast => PrecomputedAfterKey::AfterLast,
        }
    }
}

// /!\ These operators only makes sense if both values are in the same column space
impl PrecomputedAfterKey {
    pub fn equals(&self, column_value: u64) -> bool {
        match self {
            PrecomputedAfterKey::Exact(v) => *v == column_value,
            PrecomputedAfterKey::Next(_) => false,
            PrecomputedAfterKey::AfterLast => false,
        }
    }

    pub fn gt(&self, column_value: u64) -> bool {
        match self {
            PrecomputedAfterKey::Exact(v) => *v > column_value,
            PrecomputedAfterKey::Next(v) => *v > column_value,
            PrecomputedAfterKey::AfterLast => true,
        }
    }

    pub fn lt(&self, column_value: u64) -> bool {
        match self {
            PrecomputedAfterKey::Exact(v) => *v < column_value,
            // a value equal to the next is greater than the after key
            PrecomputedAfterKey::Next(v) => *v <= column_value,
            PrecomputedAfterKey::AfterLast => false,
        }
    }

    fn precompute_i64(key: &CompositeKey, missing_order: MissingOrder, order: Order) -> Self {
        // avoid rough casting
        match key {
            CompositeKey::I64(k) => PrecomputedAfterKey::Exact(k.to_u64()),
            CompositeKey::U64(k) => num_proj::u64_to_i64(*k).into(),
            CompositeKey::F64(k) => num_proj::f64_to_i64(*k).into(),
            CompositeKey::Bool(_) => Self::keep_all(order),
            CompositeKey::Str(_) => Self::keep_all(order),
            CompositeKey::Null => precompute_missing_after_key(false, missing_order, order),
        }
    }

    fn precompute_u64(key: &CompositeKey, missing_order: MissingOrder, order: Order) -> Self {
        match key {
            CompositeKey::I64(k) => num_proj::i64_to_u64(*k).into(),
            CompositeKey::U64(k) => PrecomputedAfterKey::Exact(*k),
            CompositeKey::F64(k) => num_proj::f64_to_u64(*k).into(),
            CompositeKey::Bool(_) => Self::keep_all(order),
            CompositeKey::Str(_) => Self::keep_all(order),
            CompositeKey::Null => precompute_missing_after_key(false, missing_order, order),
        }
    }

    fn precompute_f64(key: &CompositeKey, missing_order: MissingOrder, order: Order) -> Self {
        match key {
            CompositeKey::F64(k) => PrecomputedAfterKey::Exact(k.to_u64()),
            CompositeKey::I64(k) => num_proj::i64_to_f64(*k).into(),
            CompositeKey::U64(k) => num_proj::u64_to_f64(*k).into(),
            CompositeKey::Bool(_) => Self::keep_all(order),
            CompositeKey::Str(_) => Self::keep_all(order),
            CompositeKey::Null => precompute_missing_after_key(false, missing_order, order),
        }
    }

    fn precompute_ip_addr(column: &Column<u64>, key: &str, field: &str) -> crate::Result<Self> {
        let compact_space_accessor = column
            .values
            .clone()
            .downcast_arc::<CompactSpaceU64Accessor>()
            .map_err(|_| {
                TantivyError::AggregationError(crate::aggregation::AggregationError::InternalError(
                    "type mismatch: could not downcast to CompactSpaceU64Accessor".to_string(),
                ))
            })?;
        let ip_u128 = IpAddr::from_str(key)
            .map_err(|_| {
                TantivyError::InvalidArgument(format!(
                    "failed to parse after_key '{}' as IpAddr for field '{}'",
                    key, field
                ))
            })?
            .into_ipv6_addr()
            .to_bits();
        let ip_next_compact = compact_space_accessor.u128_to_next_compact(ip_u128);
        Ok(ip_next_compact.into())
    }

    fn precompute_term_ord(
        str_dict_column: &Option<StrColumn>,
        key: &str,
        field: &str,
    ) -> crate::Result<Self> {
        let dict = str_dict_column
            .as_ref()
            .expect("dictionary missing for str accessor")
            .dictionary();
        let next_ord = dict.term_ord_or_next(key).map_err(|_| {
            TantivyError::InvalidArgument(format!(
                "failed to lookup after_key '{}' for field '{}'",
                key, field
            ))
        })?;
        Ok(next_ord.into())
    }

    /// Assumes that the relevant columns were already skipped
    pub fn precompute(
        composite_accessor: &CompositeAccessor,
        source_after_key: &CompositeKey,
        field: &str,
        missing_order: MissingOrder,
        order: Order,
    ) -> crate::Result<Self> {
        let precomputed_key = match (composite_accessor.column_type, source_after_key) {
            (_, CompositeKey::F64(f)) if f.is_nan() => {
                return Err(crate::TantivyError::InvalidArgument(format!(
                    "unexptected NaN in after key {:?}",
                    source_after_key
                )));
            }
            (ColumnType::I64, key) => {
                PrecomputedAfterKey::precompute_i64(key, missing_order, order)
            }
            (ColumnType::U64, key) => {
                PrecomputedAfterKey::precompute_u64(key, missing_order, order)
            }
            (ColumnType::F64, key) => {
                PrecomputedAfterKey::precompute_f64(key, missing_order, order)
            }
            (ColumnType::Bool, CompositeKey::Bool(key)) => PrecomputedAfterKey::Exact(key.to_u64()),
            (ColumnType::Bool, CompositeKey::Null) => {
                precompute_missing_after_key(false, missing_order, order)
            }
            (ColumnType::Bool, _) => PrecomputedAfterKey::keep_all(order),
            (ColumnType::Str, CompositeKey::Str(key)) => PrecomputedAfterKey::precompute_term_ord(
                &composite_accessor.str_dict_column,
                key,
                field,
            )?,
            (ColumnType::Str, CompositeKey::Null) => {
                precompute_missing_after_key(false, missing_order, order)
            }
            (ColumnType::Str, _) => PrecomputedAfterKey::keep_all(order),
            (ColumnType::DateTime, CompositeKey::Str(key)) => {
                PrecomputedAfterKey::Exact(parse_date(key)?.to_u64())
            }
            (ColumnType::IpAddr, CompositeKey::Str(key)) => {
                PrecomputedAfterKey::precompute_ip_addr(&composite_accessor.column, key, field)?
            }
            (ColumnType::Bytes, _) => panic!("unsupported"),
            (ColumnType::DateTime | ColumnType::IpAddr, CompositeKey::Null) => {
                precompute_missing_after_key(false, missing_order, order)
            }
            (ColumnType::DateTime | ColumnType::IpAddr, _) => {
                // we don't support fields for which the schema changes
                return Err(crate::TantivyError::InvalidArgument(format!(
                    "after key {:?} does not match column type {:?} for field '{}'",
                    source_after_key, composite_accessor.column_type, field
                )));
            }
        };
        Ok(precomputed_key)
    }

    fn keep_all(order: Order) -> Self {
        match order {
            Order::Asc => PrecomputedAfterKey::Next(0),
            Order::Desc => PrecomputedAfterKey::Next(u64::MAX),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv6Addr;

    use super::super::type_order_key;
    use super::*;
    use crate::aggregation::intermediate_agg_result::CompositeIntermediateKey;

    #[test]
    fn test_sort_order_keys_aligned() {
        // it is important that the order keys used to order column types and
        // intermediate key types are the same
        for order in [Order::Asc, Order::Desc] {
            assert_eq!(
                col_type_order_key(&ColumnType::Bool, order),
                type_order_key(&CompositeIntermediateKey::Bool(true), order)
            );
            assert_eq!(
                col_type_order_key(&ColumnType::Str, order),
                type_order_key(&CompositeIntermediateKey::Str("".to_string()), order)
            );
            assert_eq!(
                col_type_order_key(&ColumnType::I64, order),
                type_order_key(&CompositeIntermediateKey::I64(0), order)
            );
            assert_eq!(
                col_type_order_key(&ColumnType::U64, order),
                type_order_key(&CompositeIntermediateKey::U64(0), order)
            );
            assert_eq!(
                col_type_order_key(&ColumnType::F64, order),
                type_order_key(&CompositeIntermediateKey::F64(0.0), order)
            );
            assert_eq!(
                col_type_order_key(&ColumnType::DateTime, order),
                type_order_key(&CompositeIntermediateKey::DateTime(0), order)
            );
            assert_eq!(
                col_type_order_key(&ColumnType::IpAddr, order),
                type_order_key(
                    &CompositeIntermediateKey::IpAddr(Ipv6Addr::LOCALHOST),
                    order
                )
            );
        }
    }
}
