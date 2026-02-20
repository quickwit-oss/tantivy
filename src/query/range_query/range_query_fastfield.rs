//! Fastfields support efficient scanning for range queries.
//! We use this variant only if the fastfield exists, otherwise the default in `range_query` is
//! used, which uses the term dictionary + postings.

use std::net::Ipv6Addr;
use std::ops::{Bound, RangeInclusive};

use columnar::{
    BytesColumn, Cardinality, Column, ColumnType, MonotonicallyMappableToU128,
    MonotonicallyMappableToU64, NumericalType, StrColumn,
};
use common::bounds::{BoundsRange, TransformBound};

use super::contiguous_doc_set::ContiguousDocSet;
use super::fast_field_range_doc_set::RangeDocSet;
use crate::index::SegmentReader;
use crate::query::{
    AllScorer, ConstScorer, EmptyScorer, EnableScoring, Explanation, Query, Scorer, Weight,
};
use crate::schema::{Type, ValueBytes};
use crate::{DocId, DocSet, Order, Score, TantivyError, Term};

#[derive(Clone, Debug)]
/// `FastFieldRangeQuery` is the same as [RangeQuery] but only uses the fast field
pub struct FastFieldRangeQuery {
    bounds: BoundsRange<Term>,
}
impl FastFieldRangeQuery {
    /// Create new `FastFieldRangeQuery`
    pub fn new(lower_bound: Bound<Term>, upper_bound: Bound<Term>) -> FastFieldRangeQuery {
        Self {
            bounds: BoundsRange::new(lower_bound, upper_bound),
        }
    }
}

impl Query for FastFieldRangeQuery {
    fn weight(&self, _enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        Ok(Box::new(FastFieldRangeWeight::new(self.bounds.clone())))
    }
}

/// `FastFieldRangeWeight` uses the fast field to execute range queries.
#[derive(Clone, Debug)]
pub struct FastFieldRangeWeight {
    bounds: BoundsRange<Term>,
}

impl FastFieldRangeWeight {
    /// Create a new FastFieldRangeWeight
    pub fn new(bounds: BoundsRange<Term>) -> Self {
        Self { bounds }
    }
}

impl Weight for FastFieldRangeWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        // Check if both bounds are Bound::Unbounded
        if self.bounds.is_unbounded() {
            return Ok(Box::new(AllScorer::new(reader.max_doc())));
        }

        let term = self
            .bounds
            .get_inner()
            .expect("At least one bound must be set");
        let schema = reader.schema();
        let field_type = schema.get_field_entry(term.field()).field_type();
        assert_eq!(
            term.typ(),
            field_type.value_type(),
            "Field is of type {:?}, but got term of type {:?}",
            field_type,
            term.typ()
        );
        let field_name = term.get_full_path(reader.schema());

        // Extract sort order if this field is the index sort field (enables sorted optimization
        // below).
        let sort_order = reader
            .sort_by_field()
            .filter(|sbf| sbf.field == field_name)
            .map(|sbf| sbf.order);

        let get_value_bytes = |term: &Term| term.value().value_bytes_payload();

        let term_value = term.value();
        if field_type.is_json() {
            let bounds = self
                .bounds
                .map_bound(|term| term.value().as_json_value_bytes().unwrap().to_owned());
            // Unlike with other field types JSON may have multiple columns of different types
            // under the same name
            //
            // In the JSON case the provided type in term may not exactly match the column type,
            // especially with the numeric type interpolation
            let json_value_bytes = term_value
                .as_json_value_bytes()
                .expect("expected json type in term");
            let typ = json_value_bytes.typ();

            match typ {
                Type::Str => {
                    let Some(str_dict_column): Option<StrColumn> =
                        reader.fast_fields().str(&field_name)?
                    else {
                        return Ok(Box::new(EmptyScorer));
                    };
                    let dict = str_dict_column.dictionary();

                    let bounds = self.bounds.map_bound(get_value_bytes);
                    let (lower_bound, upper_bound) =
                        dict.term_bounds_to_ord(bounds.lower_bound, bounds.upper_bound)?;
                    let fast_field_reader = reader.fast_fields();
                    let Some((column, _col_type)) = fast_field_reader
                        .u64_lenient_for_type(Some(&[ColumnType::Str]), &field_name)?
                    else {
                        return Ok(Box::new(EmptyScorer));
                    };
                    // JSON subfields can't be sort_by_field targets, so no sorted optimization.
                    search_on_u64_ff(
                        column,
                        boost,
                        BoundsRange::new(lower_bound, upper_bound),
                        None,
                    )
                }
                Type::U64 | Type::I64 | Type::F64 => {
                    search_on_json_numerical_field(reader, &field_name, typ, bounds, boost)
                }
                Type::Date => {
                    let fast_field_reader = reader.fast_fields();
                    let Some((column, _col_type)) = fast_field_reader
                        .u64_lenient_for_type(Some(&[ColumnType::DateTime]), &field_name)?
                    else {
                        return Ok(Box::new(EmptyScorer));
                    };
                    let bounds = bounds.map_bound(|term| term.as_date().unwrap().to_u64());
                    // JSON subfields can't be sort_by_field targets, so no sorted optimization.
                    search_on_u64_ff(
                        column,
                        boost,
                        BoundsRange::new(bounds.lower_bound, bounds.upper_bound),
                        None,
                    )
                }
                Type::Bool | Type::Facet | Type::Bytes | Type::Json | Type::IpAddr => {
                    Err(crate::TantivyError::InvalidArgument(format!(
                        "unsupported value bytes type in json term value_bytes {:?}",
                        term_value.typ()
                    )))
                }
            }
        } else if field_type.is_ip_addr() {
            let parse_ip_from_bytes = |term: &Term| {
                term.value().as_ip_addr().ok_or_else(|| {
                    crate::TantivyError::InvalidArgument("Expected ip address".to_string())
                })
            };
            let bounds: BoundsRange<Ipv6Addr> = self.bounds.map_bound_res(parse_ip_from_bytes)?;

            let Some(ip_addr_column): Option<Column<Ipv6Addr>> =
                reader.fast_fields().column_opt(&field_name)?
            else {
                return Ok(Box::new(EmptyScorer));
            };
            let value_range = bound_range_inclusive_ip(
                &bounds.lower_bound,
                &bounds.upper_bound,
                ip_addr_column.min_value(),
                ip_addr_column.max_value(),
            );
            let docset = RangeDocSet::new(value_range, ip_addr_column);
            Ok(Box::new(ConstScorer::new(docset, boost)))
        } else if field_type.is_str() {
            let Some(str_dict_column): Option<StrColumn> = reader.fast_fields().str(&field_name)?
            else {
                return Ok(Box::new(EmptyScorer));
            };
            let dict = str_dict_column.dictionary();

            let bounds = self.bounds.map_bound(get_value_bytes);
            let (lower_bound, upper_bound) =
                dict.term_bounds_to_ord(bounds.lower_bound, bounds.upper_bound)?;
            let fast_field_reader = reader.fast_fields();
            let Some((column, _col_type)) =
                fast_field_reader.u64_lenient_for_type(None, &field_name)?
            else {
                return Ok(Box::new(EmptyScorer));
            };
            search_on_u64_ff(
                column,
                boost,
                BoundsRange::new(lower_bound, upper_bound),
                sort_order,
            )
        } else if field_type.is_bytes() {
            let Some(bytes_column): Option<BytesColumn> =
                reader.fast_fields().bytes(&field_name)?
            else {
                return Ok(Box::new(EmptyScorer));
            };
            let dict = bytes_column.dictionary();

            let bounds = self.bounds.map_bound(get_value_bytes);
            let (lower_bound, upper_bound) =
                dict.term_bounds_to_ord(bounds.lower_bound, bounds.upper_bound)?;
            let fast_field_reader = reader.fast_fields();
            let Some((column, _col_type)) =
                fast_field_reader.u64_lenient_for_type(None, &field_name)?
            else {
                return Ok(Box::new(EmptyScorer));
            };
            search_on_u64_ff(
                column,
                boost,
                BoundsRange::new(lower_bound, upper_bound),
                sort_order,
            )
        } else {
            assert!(
                maps_to_u64_fastfield(field_type.value_type()),
                "{field_type:?}"
            );

            let bounds = self.bounds.map_bound_res(|term| {
                let value = term.value();
                let val = if let Some(val) = value.as_u64() {
                    val
                } else if let Some(val) = value.as_i64() {
                    val.to_u64()
                } else if let Some(val) = value.as_f64() {
                    val.to_u64()
                } else if let Some(val) = value.as_date() {
                    val.to_u64()
                } else {
                    return Err(TantivyError::InvalidArgument(format!(
                        "Expected term with u64, i64, f64 or date, but got {term:?}"
                    )));
                };
                Ok(val)
            })?;

            let fast_field_reader = reader.fast_fields();
            let Some((column, _col_type)) = fast_field_reader.u64_lenient_for_type(
                Some(&[
                    ColumnType::U64,
                    ColumnType::I64,
                    ColumnType::F64,
                    ColumnType::DateTime,
                ]),
                &field_name,
            )?
            else {
                return Ok(Box::new(EmptyScorer));
            };
            search_on_u64_ff(
                column,
                boost,
                BoundsRange::new(bounds.lower_bound, bounds.upper_bound),
                sort_order,
            )
        }
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(TantivyError::InvalidArgument(format!(
                "Document #({doc}) does not match"
            )));
        }
        let explanation = Explanation::new("Const", scorer.score());

        Ok(explanation)
    }
}

/// On numerical fields the column type may not match the user provided one.
///
/// Convert into fast field value space and search.
fn search_on_json_numerical_field(
    reader: &SegmentReader,
    field_name: &str,
    typ: Type,
    bounds: BoundsRange<ValueBytes<Vec<u8>>>,
    boost: Score,
) -> crate::Result<Box<dyn Scorer>> {
    // Since we don't know which type was interpolated for the internal column we
    // have to check for all numeric types (only one exists)
    let allowed_column_types: Option<&[ColumnType]> =
        Some(&[ColumnType::F64, ColumnType::I64, ColumnType::U64]);
    let fast_field_reader = reader.fast_fields();
    let Some((column, col_type)) =
        fast_field_reader.u64_lenient_for_type(allowed_column_types, field_name)?
    else {
        return Ok(Box::new(EmptyScorer));
    };
    let actual_column_type: NumericalType = col_type
        .numerical_type()
        .unwrap_or_else(|| panic!("internal error: couldn't cast to numerical_type: {col_type:?}"));

    let bounds = match typ.numerical_type().unwrap() {
        NumericalType::I64 => {
            let bounds = bounds.map_bound(|term| term.as_i64().unwrap());
            match actual_column_type {
                NumericalType::I64 => bounds.map_bound(|&term| term.to_u64()),
                NumericalType::U64 => {
                    bounds.transform_inner(
                        |&val| {
                            if val < 0 {
                                return TransformBound::NewBound(Bound::Unbounded);
                            }
                            TransformBound::Existing(val as u64)
                        },
                        |&val| {
                            if val < 0 {
                                // no hits case
                                return TransformBound::NewBound(Bound::Excluded(0));
                            }
                            TransformBound::Existing(val as u64)
                        },
                    )
                }
                NumericalType::F64 => bounds.map_bound(|&term| (term as f64).to_u64()),
            }
        }
        NumericalType::U64 => {
            let bounds = bounds.map_bound(|term| term.as_u64().unwrap());
            match actual_column_type {
                NumericalType::U64 => bounds.map_bound(|&term| term.to_u64()),
                NumericalType::I64 => {
                    bounds.transform_inner(
                        |&val| {
                            if val > i64::MAX as u64 {
                                // Actual no hits case
                                return TransformBound::NewBound(Bound::Excluded(i64::MAX as u64));
                            }
                            TransformBound::Existing((val as i64).to_u64())
                        },
                        |&val| {
                            if val > i64::MAX as u64 {
                                return TransformBound::NewBound(Bound::Unbounded);
                            }
                            TransformBound::Existing((val as i64).to_u64())
                        },
                    )
                }
                NumericalType::F64 => bounds.map_bound(|&term| (term as f64).to_u64()),
            }
        }
        NumericalType::F64 => {
            let bounds = bounds.map_bound(|term| term.as_f64().unwrap());
            match actual_column_type {
                NumericalType::U64 => transform_from_f64_bounds::<u64>(&bounds),
                NumericalType::I64 => transform_from_f64_bounds::<i64>(&bounds),
                NumericalType::F64 => bounds.map_bound(|&term| term.to_u64()),
            }
        }
    };
    // JSON subfields can't be sort_by_field targets, so no sorted optimization.
    search_on_u64_ff(
        column,
        boost,
        BoundsRange::new(bounds.lower_bound, bounds.upper_bound),
        None,
    )
}

trait IntType {
    fn min() -> Self;
    fn max() -> Self;
    fn to_f64(self) -> f64;
    fn from_f64(val: f64) -> Self;
}
impl IntType for i64 {
    fn min() -> Self {
        Self::MIN
    }
    fn max() -> Self {
        Self::MAX
    }
    fn to_f64(self) -> f64 {
        self as f64
    }
    fn from_f64(val: f64) -> Self {
        val as Self
    }
}
impl IntType for u64 {
    fn min() -> Self {
        Self::MIN
    }
    fn max() -> Self {
        Self::MAX
    }
    fn to_f64(self) -> f64 {
        self as f64
    }
    fn from_f64(val: f64) -> Self {
        val as Self
    }
}

fn transform_from_f64_bounds<T: IntType + MonotonicallyMappableToU64>(
    bounds: &BoundsRange<f64>,
) -> BoundsRange<u64> {
    bounds.transform_inner(
        |&lower_bound| {
            if lower_bound < T::min().to_f64() {
                return TransformBound::NewBound(Bound::Unbounded);
            }
            if lower_bound > T::max().to_f64() {
                // no hits case
                return TransformBound::NewBound(Bound::Excluded(u64::MAX));
            }

            if lower_bound.fract() == 0.0 {
                TransformBound::Existing(T::from_f64(lower_bound).to_u64())
            } else {
                TransformBound::NewBound(Bound::Included(T::from_f64(lower_bound.trunc()).to_u64()))
            }
        },
        |&upper_bound| {
            if upper_bound < T::min().to_f64() {
                return TransformBound::NewBound(Bound::Unbounded);
            }
            if upper_bound > T::max().to_f64() {
                // no hits case
                return TransformBound::NewBound(Bound::Included(u64::MAX));
            }
            if upper_bound.fract() == 0.0 {
                TransformBound::Existing(T::from_f64(upper_bound).to_u64())
            } else {
                TransformBound::NewBound(Bound::Included(T::from_f64(upper_bound.trunc()).to_u64()))
            }
        },
    )
}

fn search_on_u64_ff(
    column: Column<u64>,
    boost: Score,
    bounds: BoundsRange<u64>,
    sort_order: Option<Order>,
) -> crate::Result<Box<dyn Scorer>> {
    let col_min_value = column.min_value();
    let col_max_value = column.max_value();
    #[expect(clippy::reversed_empty_ranges)]
    let value_range = bound_to_value_range(
        &bounds.lower_bound,
        &bounds.upper_bound,
        column.min_value(),
        column.max_value(),
    )
    .unwrap_or(1..=0); // empty range
    if value_range.is_empty() {
        return Ok(Box::new(EmptyScorer));
    }
    if col_min_value >= *value_range.start() && col_max_value <= *value_range.end() {
        // all values in the column are within the range.
        if column.index.get_cardinality() == Cardinality::Full {
            if boost != 1.0f32 {
                return Ok(Box::new(ConstScorer::new(
                    AllScorer::new(column.num_docs()),
                    boost,
                )));
            } else {
                return Ok(Box::new(AllScorer::new(column.num_docs())));
            }
        } else {
            // TODO Make it a field presence request for that specific column
        }
    }

    // Sorted index optimization: when the index is sorted by this field,
    // binary search for the matching DocId range instead of scanning.
    if let Some(order) = sort_order {
        let cardinality = column.index.get_cardinality();
        if matches!(cardinality, Cardinality::Full | Cardinality::Optional) {
            return Ok(sorted_range_scorer(
                &column,
                boost,
                &value_range,
                order,
                cardinality,
            ));
        }
    }

    let docset = RangeDocSet::new(value_range, column);
    Ok(Box::new(ConstScorer::new(docset, boost)))
}

/// Builds a scorer by binary-searching the matching DocId range in a sorted column.
///
/// # Design
///
/// When an index is sorted by a field, documents with adjacent values have adjacent DocIds.
/// This means the set of matching DocIds for a range query is always contiguous — if DocId 5
/// matches and DocId 9 matches, then 6, 7, 8 must also match. This lets us replace O(n)
/// column scanning with O(log n) binary search to find the boundaries.
///
/// The algorithm has three phases:
///
/// 1. **NULL boundary**: NULLs cluster at one end of the segment (start for ASC, end for DESC). We
///    binary search using `column.first(doc).is_some()` to find where real values begin, because
///    NULL docs have no value to compare against — only presence can be tested.
///
/// 2. **Value boundary**: Within the non-NULL range, two binary searches find the first and last
///    matching DocIds. The result is a half-open range `[start, end)` where `start` is the first
///    match and `end` is one past the last match.
///
/// 3. **ContiguousDocSet**: Wraps the `[start, end)` range as a `DocSet`.
///
/// Only `Full` and `Optional` cardinalities are supported. `Multivalued` columns
/// have multiple values per doc and cannot be binary-searched.
fn sorted_range_scorer(
    column: &Column<u64>,
    boost: Score,
    value_range: &RangeInclusive<u64>,
    order: Order,
    cardinality: Cardinality,
) -> Box<dyn Scorer> {
    let num_docs = column.num_docs();
    if num_docs == 0 {
        return Box::new(EmptyScorer);
    }

    let lower = *value_range.start();
    let upper = *value_range.end();

    // Determine the non-NULL range within the segment.
    // - ASC: NULLs at start (low DocIds), values ascending after.
    // - DESC: values descending first, NULLs at end (high DocIds).
    let (non_null_start, non_null_end) = match cardinality {
        Cardinality::Full => (0u32, num_docs),
        Cardinality::Optional => match order {
            Order::Asc => {
                // Binary search for first DocId with a value (NULLs at start).
                let start = binary_search_null_boundary(column, 0, num_docs, Order::Asc);
                (start, num_docs)
            }
            Order::Desc => {
                // Binary search for first NULL DocId (NULLs at end).
                let end = binary_search_null_boundary(column, 0, num_docs, Order::Desc);
                (0, end)
            }
        },
        _ => unreachable!("caller filters out unsupported cardinality"),
    };

    if non_null_start >= non_null_end {
        return Box::new(EmptyScorer);
    }

    let (start_target, end_target) = match order {
        Order::Asc => (lower, upper),
        Order::Desc => (upper, lower),
    };
    let start_doc = binary_search_sorted(
        column,
        non_null_start,
        non_null_end,
        start_target,
        order,
        false, // inclusive: find first match
    );
    let end_doc = binary_search_sorted(
        column,
        non_null_start,
        non_null_end,
        end_target,
        order,
        true, // exclusive: find one past last match
    );

    let docset = ContiguousDocSet::new(start_doc, end_doc);
    Box::new(ConstScorer::new(docset, boost))
}

/// Binary search for the boundary between NULLs and non-NULLs.
///
/// This is separated from value search because NULL docs have no stored value —
/// `column.first(doc)` returns `None`. We can only test presence (`is_some()`),
/// not compare against a target value. Once the NULL boundary is known, the
/// non-NULL range is passed to `binary_search_sorted` which can safely `.expect()`
/// on every lookup.
///
/// - `Order::Asc`: NULLs are at the start. Returns the first DocId with a value.
/// - `Order::Desc`: NULLs are at the end. Returns the first DocId without a value (i.e., past all
///   valued docs).
fn binary_search_null_boundary(column: &Column<u64>, lo: u32, hi: u32, order: Order) -> u32 {
    let mut lo = lo;
    let mut hi = hi;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let has_value = column.first(mid).is_some();
        match order {
            Order::Asc => {
                // NULLs at start. Looking for first doc WITH a value.
                if has_value {
                    hi = mid;
                } else {
                    lo = mid + 1;
                }
            }
            Order::Desc => {
                // NULLs at end. Looking for first doc WITHOUT a value.
                if has_value {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
        }
    }
    lo
}

/// Binary search on a sorted column for the boundary of a value range.
///
/// Returns a DocId forming one side of the half-open range `[start, end)`:
/// - `strict=false` (inclusive): first doc whose value is at or past `target` — used for `start`.
/// - `strict=true` (exclusive): first doc whose value is strictly past `target` — used for `end`.
///
/// The caller guarantees that `[lo, hi)` contains only non-NULL docs
/// (the NULL boundary was already computed by `binary_search_null_boundary`).
fn binary_search_sorted(
    column: &Column<u64>,
    lo: u32,
    hi: u32,
    target: u64,
    order: Order,
    strict: bool,
) -> u32 {
    let mut lo = lo;
    let mut hi = hi;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        // Safe: caller guarantees [lo, hi) is non-NULL (see binary_search_null_boundary).
        let val = column
            .first(mid)
            .expect("doc in non-NULL range has no value");
        let go_right = match (order, strict) {
            (Order::Asc, false) => val < target,
            (Order::Asc, true) => val <= target,
            (Order::Desc, false) => val > target,
            (Order::Desc, true) => val >= target,
        };
        if go_right {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// Returns true if the type maps to a u64 fast field
pub(crate) fn maps_to_u64_fastfield(typ: Type) -> bool {
    match typ {
        Type::U64 | Type::I64 | Type::F64 | Type::Bool | Type::Date => true,
        Type::IpAddr => false,
        Type::Str | Type::Facet | Type::Bytes | Type::Json => false,
    }
}

fn bound_range_inclusive_ip(
    lower_bound: &Bound<Ipv6Addr>,
    upper_bound: &Bound<Ipv6Addr>,
    min_value: Ipv6Addr,
    max_value: Ipv6Addr,
) -> RangeInclusive<Ipv6Addr> {
    let start_value = match lower_bound {
        Bound::Included(ip_addr) => *ip_addr,
        Bound::Excluded(ip_addr) => Ipv6Addr::from(ip_addr.to_u128() + 1),
        Bound::Unbounded => min_value,
    };

    let end_value = match upper_bound {
        Bound::Included(ip_addr) => *ip_addr,
        Bound::Excluded(ip_addr) => Ipv6Addr::from(ip_addr.to_u128() - 1),
        Bound::Unbounded => max_value,
    };
    start_value..=end_value
}

// Returns None, if the range cannot be converted to a inclusive range (which equals to a empty
// range).
fn bound_to_value_range<T: MonotonicallyMappableToU64>(
    lower_bound: &Bound<T>,
    upper_bound: &Bound<T>,
    min_value: T,
    max_value: T,
) -> Option<RangeInclusive<T>> {
    let mut start_value = match lower_bound {
        Bound::Included(val) => *val,
        Bound::Excluded(val) => T::from_u64(val.to_u64().checked_add(1)?),
        Bound::Unbounded => min_value,
    };
    if start_value.partial_cmp(&min_value) == Some(std::cmp::Ordering::Less) {
        start_value = min_value;
    }
    let end_value = match upper_bound {
        Bound::Included(val) => *val,
        Bound::Excluded(val) => T::from_u64(val.to_u64().checked_sub(1)?),
        Bound::Unbounded => max_value,
    };
    Some(start_value..=end_value)
}

#[cfg(test)]
mod tests {
    use std::ops::{Bound, RangeInclusive};

    use common::bounds::BoundsRange;
    use common::DateTime;
    use proptest::prelude::*;
    use rand::rngs::StdRng;
    use rand::seq::IndexedRandom;
    use rand::SeedableRng;
    use time::format_description::well_known::Rfc3339;
    use time::OffsetDateTime;

    use crate::collector::{Count, TopDocs};
    use crate::fastfield::FastValue;
    use crate::query::range_query::range_query_fastfield::FastFieldRangeWeight;
    use crate::query::{QueryParser, RangeQuery, Weight};
    use crate::schema::{
        DateOptions, Field, NumericOptions, Schema, SchemaBuilder, FAST, INDEXED, STORED, STRING,
        TEXT,
    };
    use crate::{Index, IndexWriter, TantivyDocument, Term, TERMINATED};

    #[test]
    fn test_text_field_ff_range_query() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("title", TEXT | FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());
        let mut index_writer = index.writer_for_tests()?;
        let title = schema.get_field("title").unwrap();
        index_writer.add_document(doc!(
          title => "bbb"
        ))?;
        index_writer.add_document(doc!(
          title => "ddd"
        ))?;
        index_writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let query_parser = QueryParser::for_index(&index, vec![title]);

        let test_query = |query, num_hits| {
            let query = query_parser.parse_query(query).unwrap();
            let top_docs = searcher
                .search(&query, &TopDocs::with_limit(10).order_by_score())
                .unwrap();
            assert_eq!(top_docs.len(), num_hits);
        };

        test_query("title:[aaa TO ccc]", 1);
        test_query("title:[aaa TO bbb]", 1);
        test_query("title:[bbb TO bbb]", 1);
        test_query("title:[bbb TO ddd]", 2);
        test_query("title:[bbb TO eee]", 2);
        test_query("title:[bb TO eee]", 2);
        test_query("title:[ccc TO ccc]", 0);
        test_query("title:[ccc TO ddd]", 1);
        test_query("title:[ccc TO eee]", 1);

        test_query("title:[aaa TO *}", 2);
        test_query("title:[bbb TO *]", 2);
        test_query("title:[bb TO *]", 2);
        test_query("title:[ccc TO *]", 1);
        test_query("title:[ddd TO *]", 1);
        test_query("title:[dddd TO *]", 0);

        test_query("title:{aaa TO *}", 2);
        test_query("title:{bbb TO *]", 1);
        test_query("title:{bb TO *]", 2);
        test_query("title:{ccc TO *]", 1);
        test_query("title:{ddd TO *]", 0);
        test_query("title:{dddd TO *]", 0);

        test_query("title:[* TO bb]", 0);
        test_query("title:[* TO bbb]", 1);
        test_query("title:[* TO ccc]", 1);
        test_query("title:[* TO ddd]", 2);
        test_query("title:[* TO ddd}", 1);
        test_query("title:[* TO eee]", 2);

        Ok(())
    }

    #[test]
    fn test_date_range_query() {
        let mut schema_builder = Schema::builder();
        let options = DateOptions::default()
            .set_precision(common::DateTimePrecision::Microseconds)
            .set_fast();
        let date_field = schema_builder.add_date_field("date", options);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema.clone());
        {
            let mut index_writer = index.writer_with_num_threads(1, 50_000_000).unwrap();
            // This is added a string and creates a string column!
            index_writer
                .add_document(doc!(date_field => DateTime::from_utc(
                    OffsetDateTime::parse("2022-12-01T00:00:01Z", &Rfc3339).unwrap(),
                )))
                .unwrap();
            index_writer
                .add_document(doc!(date_field => DateTime::from_utc(
                    OffsetDateTime::parse("2023-12-01T00:00:01Z", &Rfc3339).unwrap(),
                )))
                .unwrap();
            index_writer
                .add_document(doc!(date_field => DateTime::from_utc(
                    OffsetDateTime::parse("2015-02-01T00:00:00.001Z", &Rfc3339).unwrap(),
                )))
                .unwrap();
            index_writer.commit().unwrap();
        }

        // Date field
        let dt1 =
            DateTime::from_utc(OffsetDateTime::parse("2022-12-01T00:00:01Z", &Rfc3339).unwrap());
        let dt2 =
            DateTime::from_utc(OffsetDateTime::parse("2023-12-01T00:00:01Z", &Rfc3339).unwrap());
        let dt3 = DateTime::from_utc(
            OffsetDateTime::parse("2015-02-01T00:00:00.001Z", &Rfc3339).unwrap(),
        );
        let dt4 = DateTime::from_utc(
            OffsetDateTime::parse("2015-02-01T00:00:00.002Z", &Rfc3339).unwrap(),
        );

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let query_parser = QueryParser::for_index(&index, vec![date_field]);
        let test_query = |query, num_hits| {
            let query = query_parser.parse_query(query).unwrap();
            let top_docs = searcher
                .search(&query, &TopDocs::with_limit(10).order_by_score())
                .unwrap();
            assert_eq!(top_docs.len(), num_hits);
        };

        test_query(
            "date:[2015-02-01T00:00:00.001Z TO 2015-02-01T00:00:00.001Z]",
            1,
        );
        test_query(
            "date:[2015-02-01T00:00:00.001Z TO 2015-02-01T00:00:00.002Z}",
            1,
        );
        test_query(
            "date:[2015-02-01T00:00:00.001Z TO 2015-02-01T00:00:00.002Z]",
            1,
        );
        test_query(
            "date:{2015-02-01T00:00:00.001Z TO 2015-02-01T00:00:00.002Z]",
            0,
        );

        let count = |range_query: RangeQuery| searcher.search(&range_query, &Count).unwrap();
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(Term::from_field_date(date_field, dt3)),
                Bound::Excluded(Term::from_field_date(date_field, dt4)),
            )),
            1
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(Term::from_field_date(date_field, dt3)),
                Bound::Included(Term::from_field_date(date_field, dt4)),
            )),
            1
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(Term::from_field_date(date_field, dt1)),
                Bound::Included(Term::from_field_date(date_field, dt2)),
            )),
            2
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(Term::from_field_date(date_field, dt1)),
                Bound::Excluded(Term::from_field_date(date_field, dt2)),
            )),
            1
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Excluded(Term::from_field_date(date_field, dt1)),
                Bound::Excluded(Term::from_field_date(date_field, dt2)),
            )),
            0
        );
    }

    fn get_json_term<T: FastValue>(field: Field, path: &str, value: T) -> Term {
        let mut term = Term::from_field_json_path(field, path, true);
        term.append_type_and_fast_value(value);
        term
    }

    #[test]
    fn mixed_numerical_test() {
        let mut schema_builder = Schema::builder();
        schema_builder.add_i64_field("id_i64", STORED | FAST);
        schema_builder.add_u64_field("id_u64", STORED | FAST);
        schema_builder.add_f64_field("id_f64", STORED | FAST);
        let schema = schema_builder.build();

        fn get_json_term<T: FastValue>(schema: &Schema, path: &str, value: T) -> Term {
            let field = schema.get_field(path).unwrap();
            Term::from_fast_value(field, &value)
            // term.append_type_and_fast_value(value);
            // term
        }
        let index = Index::create_in_ram(schema.clone());
        {
            let mut index_writer = index.writer_with_num_threads(1, 50_000_000).unwrap();

            let doc = json!({
                "id_u64": 0,
                "id_i64": 50,
            });
            let doc = TantivyDocument::parse_json(&schema, &serde_json::to_string(&doc).unwrap())
                .unwrap();
            index_writer.add_document(doc).unwrap();
            let doc = json!({
                "id_u64": 10,
                "id_i64": 1000,
            });
            let doc = TantivyDocument::parse_json(&schema, &serde_json::to_string(&doc).unwrap())
                .unwrap();
            index_writer.add_document(doc).unwrap();

            index_writer.commit().unwrap();
        }

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let count = |range_query: RangeQuery| searcher.search(&range_query, &Count).unwrap();

        // u64 on u64
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(&schema, "id_u64", 10u64)),
                Bound::Included(get_json_term(&schema, "id_u64", 10u64)),
            )),
            1
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(&schema, "id_u64", 9u64)),
                Bound::Excluded(get_json_term(&schema, "id_u64", 10u64)),
            )),
            0
        );

        // i64 on i64
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(&schema, "id_i64", 50i64)),
                Bound::Included(get_json_term(&schema, "id_i64", 1000i64)),
            )),
            2
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(&schema, "id_i64", 50i64)),
                Bound::Excluded(get_json_term(&schema, "id_i64", 1000i64)),
            )),
            1
        );
    }

    #[test]
    fn json_range_mixed_val() {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", TEXT | STORED | FAST);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_with_num_threads(1, 50_000_000).unwrap();
            let doc = json!({
                "mixed_val": 10000,
            });
            index_writer.add_document(doc!(json_field => doc)).unwrap();
            let doc = json!({
                "mixed_val": 20000,
            });
            index_writer.add_document(doc!(json_field => doc)).unwrap();
            let doc = json!({
                "mixed_val": "1000a",
            });
            index_writer.add_document(doc!(json_field => doc)).unwrap();
            let doc = json!({
                "mixed_val": "2000a",
            });
            index_writer.add_document(doc!(json_field => doc)).unwrap();
            index_writer.commit().unwrap();
        }
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let count = |range_query: RangeQuery| searcher.search(&range_query, &Count).unwrap();

        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "mixed_val", 10000u64)),
                Bound::Included(get_json_term(json_field, "mixed_val", 20000u64)),
            )),
            2
        );
        fn get_json_term_str(field: Field, path: &str, value: &str) -> Term {
            let mut term = Term::from_field_json_path(field, path, true);
            term.append_type_and_str(value);
            term
        }
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term_str(json_field, "mixed_val", "1000a")),
                Bound::Included(get_json_term_str(json_field, "mixed_val", "2000b")),
            )),
            2
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term_str(json_field, "mixed_val", "1000")),
                Bound::Included(get_json_term_str(json_field, "mixed_val", "2000a")),
            )),
            2
        );
    }

    #[test]
    fn json_range_test() {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", TEXT | STORED | FAST);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        let u64_val = u64::MAX - 1;
        {
            let mut index_writer = index.writer_with_num_threads(1, 50_000_000).unwrap();
            let doc = json!({
                "id_u64": 0,
                "id_f64": 10.5,
                "id_i64": -100,
                "date": "2022-12-01T00:00:01Z"
            });
            index_writer.add_document(doc!(json_field => doc)).unwrap();
            let doc = json!({
                "id_u64": u64_val,
                "id_f64": 1000.5,
                "id_i64": 1000,
                "date": "2023-12-01T00:00:01Z"
            });
            index_writer.add_document(doc!(json_field => doc)).unwrap();
            let doc = json!({
                "date": "2015-02-01T00:00:00.001Z"
            });
            index_writer.add_document(doc!(json_field => doc)).unwrap();

            index_writer.commit().unwrap();
        }

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let count = |range_query: RangeQuery| searcher.search(&range_query, &Count).unwrap();

        // u64 on u64
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_u64", u64_val)),
                Bound::Included(get_json_term(json_field, "id_u64", u64_val)),
            )),
            1
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_u64", u64_val)),
                Bound::Excluded(get_json_term(json_field, "id_u64", u64_val)),
            )),
            0
        );
        // f64 on u64 field
        assert_eq!(
            count(RangeQuery::new(
                // We need to subtract since there is some inaccuracy
                Bound::Included(get_json_term(
                    json_field,
                    "id_u64",
                    (u64_val - 10000) as f64
                )),
                Bound::Included(get_json_term(json_field, "id_u64", (u64_val) as f64)),
            )),
            1
        );
        // i64 on u64
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_u64", 0_i64)),
                Bound::Included(get_json_term(json_field, "id_u64", 0_i64)),
            )),
            1
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_u64", 1_i64)),
                Bound::Included(get_json_term(json_field, "id_u64", 1_i64)),
            )),
            0
        );
        // u64 on f64
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_f64", 10_u64)),
                Bound::Included(get_json_term(json_field, "id_f64", 11_u64)),
            )),
            1
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_f64", 10_u64)),
                Bound::Included(get_json_term(json_field, "id_f64", 2000_u64)),
            )),
            2
        );
        // i64 on f64
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_f64", 10_i64)),
                Bound::Included(get_json_term(json_field, "id_f64", 2000_i64)),
            )),
            2
        );

        // i64 on i64
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_i64", -1000i64)),
                Bound::Included(get_json_term(json_field, "id_i64", 1000i64)),
            )),
            2
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_i64", 1000i64)),
                Bound::Excluded(get_json_term(json_field, "id_i64", 1001i64)),
            )),
            1
        );

        // u64 on i64
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_i64", 0_u64)),
                Bound::Included(get_json_term(json_field, "id_i64", 1000u64)),
            )),
            1
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_i64", 0_u64)),
                Bound::Included(get_json_term(json_field, "id_i64", 999u64)),
            )),
            0
        );
        // f64 on i64 field
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_i64", -1000.0)),
                Bound::Included(get_json_term(json_field, "id_i64", 1000.0)),
            )),
            2
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_i64", -1000.0f64)),
                Bound::Excluded(get_json_term(json_field, "id_i64", 1000.0f64)),
            )),
            1
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_i64", -1000.0f64)),
                Bound::Included(get_json_term(json_field, "id_i64", 1000.0f64)),
            )),
            2
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_i64", -1000.0f64)),
                Bound::Excluded(get_json_term(json_field, "id_i64", 1000.01f64)),
            )),
            2
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_i64", -1000.0f64)),
                Bound::Included(get_json_term(json_field, "id_i64", 999.99f64)),
            )),
            1
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Excluded(get_json_term(json_field, "id_i64", 999.9)),
                Bound::Excluded(get_json_term(json_field, "id_i64", 1000.1)),
            )),
            1
        );

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let query_parser = QueryParser::for_index(&index, vec![json_field]);
        let test_query = |query, num_hits| {
            let query = query_parser.parse_query(query).unwrap();
            let top_docs = searcher
                .search(&query, &TopDocs::with_limit(10).order_by_score())
                .unwrap();
            assert_eq!(top_docs.len(), num_hits);
        };

        test_query(
            "json.date:[2015-02-01T00:00:00.001Z TO 2015-02-01T00:00:00.001Z]",
            1,
        );
        test_query(
            "json.date:[2015-02-01T00:00:00.001Z TO 2015-02-01T00:00:00.002Z}",
            1,
        );
        test_query(
            "json.date:[2015-02-01T00:00:00.001Z TO 2015-02-01T00:00:00.002Z]",
            1,
        );
        test_query(
            "json.date:{2015-02-01T00:00:00.001Z TO 2015-02-01T00:00:00.002Z]",
            0,
        );

        // Date field
        let dt1 =
            DateTime::from_utc(OffsetDateTime::parse("2022-12-01T00:00:01Z", &Rfc3339).unwrap());
        let dt2 =
            DateTime::from_utc(OffsetDateTime::parse("2023-12-01T00:00:01Z", &Rfc3339).unwrap());

        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "date", dt1)),
                Bound::Included(get_json_term(json_field, "date", dt2)),
            )),
            2
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "date", dt1)),
                Bound::Excluded(get_json_term(json_field, "date", dt2)),
            )),
            1
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Excluded(get_json_term(json_field, "date", dt1)),
                Bound::Excluded(get_json_term(json_field, "date", dt2)),
            )),
            0
        );
        // Date precision test. We don't want to truncate the precision
        let dt3 = DateTime::from_utc(
            OffsetDateTime::parse("2015-02-01T00:00:00.001Z", &Rfc3339).unwrap(),
        );
        let dt4 = DateTime::from_utc(
            OffsetDateTime::parse("2015-02-01T00:00:00.002Z", &Rfc3339).unwrap(),
        );
        let query = RangeQuery::new(
            Bound::Included(get_json_term(json_field, "date", dt3)),
            Bound::Excluded(get_json_term(json_field, "date", dt4)),
        );
        assert_eq!(count(query), 1);
    }

    #[derive(Clone, Debug)]
    pub struct Doc {
        pub id_name: String,
        pub id: u64,
    }

    fn operation_strategy() -> impl Strategy<Value = Doc> {
        prop_oneof![
            (0u64..10_000u64).prop_map(doc_from_id_1),
            (1u64..10_000u64).prop_map(doc_from_id_2),
        ]
    }

    fn doc_from_id_1(id: u64) -> Doc {
        let id = id * 1000;
        Doc {
            id_name: format!("id_name{id:010}"),
            id,
        }
    }
    fn doc_from_id_2(id: u64) -> Doc {
        let id = id * 1000;
        Doc {
            id_name: format!("id_name{:010}", id - 1),
            id,
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10))]
        #[test]
        fn test_range_for_docs_prop(ops in proptest::collection::vec(operation_strategy(), 1..1000)) {
            assert!(test_id_range_for_docs(ops).is_ok());
        }
    }

    #[test]
    fn range_regression1_test() {
        let ops = vec![doc_from_id_1(0)];
        assert!(test_id_range_for_docs(ops).is_ok());
    }

    #[test]
    fn range_regression1_test_json() {
        let ops = vec![doc_from_id_1(0)];
        assert!(test_id_range_for_docs_json(ops).is_ok());
    }

    #[test]
    fn test_range_regression2() {
        let ops = vec![
            doc_from_id_1(52),
            doc_from_id_1(63),
            doc_from_id_1(12),
            doc_from_id_2(91),
            doc_from_id_2(33),
        ];
        assert!(test_id_range_for_docs(ops).is_ok());
    }

    #[test]
    fn test_range_regression3() {
        let ops = vec![doc_from_id_1(9), doc_from_id_1(0), doc_from_id_1(13)];
        assert!(test_id_range_for_docs(ops).is_ok());
    }

    #[test]
    fn test_range_regression_simplified() {
        let mut schema_builder = SchemaBuilder::new();
        let field = schema_builder.add_u64_field("test_field", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer: IndexWriter = index.writer_for_tests().unwrap();
        writer.add_document(doc!(field=>52_000u64)).unwrap();
        writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let range_query = FastFieldRangeWeight::new(BoundsRange::new(
            Bound::Included(Term::from_field_u64(field, 50_000)),
            Bound::Included(Term::from_field_u64(field, 50_002)),
        ));
        let scorer = range_query
            .scorer(searcher.segment_reader(0), 1.0f32)
            .unwrap();
        assert_eq!(scorer.doc(), TERMINATED);
    }

    #[test]
    fn range_regression3_test() {
        let ops = vec![doc_from_id_1(1), doc_from_id_1(2), doc_from_id_1(3)];
        assert!(test_id_range_for_docs(ops).is_ok());
    }

    #[test]
    fn range_regression4_test() {
        let ops = vec![doc_from_id_2(100)];
        assert!(test_id_range_for_docs(ops).is_ok());
    }

    pub fn create_index_from_docs(docs: &[Doc], json_field: bool) -> Index {
        let mut schema_builder = Schema::builder();
        if json_field {
            let json_field = schema_builder.add_json_field("json", TEXT | STORED | FAST);
            let schema = schema_builder.build();

            let index = Index::create_in_ram(schema);

            {
                let mut index_writer = index.writer_with_num_threads(1, 50_000_000).unwrap();
                for doc in docs.iter() {
                    let doc = json!({
                        "ids_i64": doc.id as i64,
                        "ids_i64": doc.id as i64,
                        "ids_f64": doc.id as f64,
                        "ids_f64": doc.id as f64,
                        "ids": doc.id,
                        "ids": doc.id,
                        "id": doc.id,
                        "id_f64": doc.id as f64,
                        "id_i64": doc.id as i64,
                        "id_name": doc.id_name.to_string(),
                        "id_name_fast": doc.id_name.to_string(),
                    });
                    index_writer.add_document(doc!(json_field => doc)).unwrap();
                }

                index_writer.commit().unwrap();
            }
            index
        } else {
            let id_u64_field = schema_builder.add_u64_field("id", INDEXED | STORED | FAST);
            let ids_u64_field = schema_builder
                .add_u64_field("ids", NumericOptions::default().set_fast().set_indexed());

            let id_f64_field = schema_builder.add_f64_field("id_f64", INDEXED | STORED | FAST);
            let ids_f64_field = schema_builder.add_f64_field(
                "ids_f64",
                NumericOptions::default().set_fast().set_indexed(),
            );

            let id_i64_field = schema_builder.add_i64_field("id_i64", INDEXED | STORED | FAST);
            let ids_i64_field = schema_builder.add_i64_field(
                "ids_i64",
                NumericOptions::default().set_fast().set_indexed(),
            );

            let text_field = schema_builder.add_text_field("id_name", STRING | STORED);
            let text_field2 = schema_builder.add_text_field("id_name_fast", STRING | STORED | FAST);
            let schema = schema_builder.build();

            let index = Index::create_in_ram(schema);

            {
                let mut index_writer = index.writer_with_num_threads(1, 50_000_000).unwrap();
                for doc in docs.iter() {
                    index_writer
                        .add_document(doc!(
                            ids_i64_field => doc.id as i64,
                            ids_i64_field => doc.id as i64,
                            ids_f64_field => doc.id as f64,
                            ids_f64_field => doc.id as f64,
                            ids_u64_field => doc.id,
                            ids_u64_field => doc.id,
                            id_u64_field => doc.id,
                            id_f64_field => doc.id as f64,
                            id_i64_field => doc.id as i64,
                            text_field => doc.id_name.to_string(),
                            text_field2 => doc.id_name.to_string(),
                        ))
                        .unwrap();
                }

                index_writer.commit().unwrap();
            }
            index
        }
    }

    fn test_id_range_for_docs(docs: Vec<Doc>) -> crate::Result<()> {
        test_id_range_for_docs_with_opt(docs, false)
    }
    fn test_id_range_for_docs_json(docs: Vec<Doc>) -> crate::Result<()> {
        test_id_range_for_docs_with_opt(docs, true)
    }

    fn test_id_range_for_docs_with_opt(docs: Vec<Doc>, json: bool) -> crate::Result<()> {
        let index = create_index_from_docs(&docs, json);
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        let mut rng: StdRng = StdRng::from_seed([1u8; 32]);

        let get_num_hits = |query| searcher.search(&query, &Count).unwrap();
        let query_from_text = |text: &str| {
            QueryParser::for_index(&index, vec![])
                .parse_query(text)
                .unwrap()
        };

        let field_path = |field: &str| {
            if json {
                format!("json.{field}")
            } else {
                field.to_string()
            }
        };

        let gen_query_inclusive = |field: &str, range: RangeInclusive<u64>| {
            format!(
                "{}:[{} TO {}]",
                field_path(field),
                range.start(),
                range.end()
            )
        };
        let gen_query_exclusive = |field: &str, range: RangeInclusive<u64>| {
            format!(
                "{}:{{{} TO {}}}",
                field_path(field),
                range.start(),
                range.end()
            )
        };

        let test_sample = |sample_docs: Vec<Doc>| {
            let mut ids: Vec<u64> = sample_docs.iter().map(|doc| doc.id).collect();
            ids.sort();
            let expected_num_hits = docs
                .iter()
                .filter(|doc| (ids[0]..=ids[1]).contains(&doc.id))
                .count();

            let query = gen_query_inclusive("id", ids[0]..=ids[1]);
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);

            let query = gen_query_inclusive("ids", ids[0]..=ids[1]);
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);

            // Text query
            {
                let test_text_query = |field_name: &str| {
                    let mut id_names: Vec<&str> =
                        sample_docs.iter().map(|doc| doc.id_name.as_str()).collect();
                    id_names.sort();
                    let expected_num_hits = docs
                        .iter()
                        .filter(|doc| (id_names[0]..=id_names[1]).contains(&doc.id_name.as_str()))
                        .count();
                    let query = format!(
                        "{}:[{} TO {}]",
                        field_path(field_name),
                        id_names[0],
                        id_names[1]
                    );
                    assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);
                };

                test_text_query("id_name");
                test_text_query("id_name_fast");
            }

            // Exclusive range
            let expected_num_hits = docs
                .iter()
                .filter(|doc| {
                    (ids[0].saturating_add(1)..=ids[1].saturating_sub(1)).contains(&doc.id)
                })
                .count();

            let query = gen_query_exclusive("id", ids[0]..=ids[1]);
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);

            let query = gen_query_exclusive("ids", ids[0]..=ids[1]);
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);

            // Intersection search
            let id_filter = sample_docs[0].id_name.to_string();
            let expected_num_hits = docs
                .iter()
                .filter(|doc| (ids[0]..=ids[1]).contains(&doc.id) && doc.id_name == id_filter)
                .count();
            let query = format!(
                "{} AND {}:{}",
                gen_query_inclusive("id", ids[0]..=ids[1]),
                field_path("id_name"),
                &id_filter
            );
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);
            let query = format!(
                "{} AND {}:{}",
                gen_query_inclusive("id_f64", ids[0]..=ids[1]),
                field_path("id_name"),
                &id_filter
            );
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);
            let query = format!(
                "{} AND {}:{}",
                gen_query_inclusive("id_i64", ids[0]..=ids[1]),
                field_path("id_name"),
                &id_filter
            );
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);

            // Intersection search on multivalue id field
            let id_filter = sample_docs[0].id_name.to_string();
            let query = format!(
                "{} AND {}:{}",
                gen_query_inclusive("ids", ids[0]..=ids[1]),
                field_path("id_name"),
                &id_filter
            );
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);
            let query = format!(
                "{} AND {}:{}",
                gen_query_inclusive("ids_f64", ids[0]..=ids[1]),
                field_path("id_name"),
                &id_filter
            );
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);
            let query = format!(
                "{} AND {}:{}",
                gen_query_inclusive("ids_i64", ids[0]..=ids[1]),
                field_path("id_name"),
                &id_filter
            );
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);
        };

        test_sample(vec![docs[0].clone(), docs[0].clone()]);

        let samples: Vec<_> = docs.choose_multiple(&mut rng, 3).collect();

        if samples.len() > 1 {
            test_sample(vec![samples[0].clone(), samples[1].clone()]);
            test_sample(vec![samples[1].clone(), samples[1].clone()]);
        }
        if samples.len() > 2 {
            test_sample(vec![samples[1].clone(), samples[2].clone()]);
        }

        Ok(())
    }

    #[test]
    fn test_bytes_field_ff_range_query() -> crate::Result<()> {
        use crate::schema::BytesOptions;

        let mut schema_builder = Schema::builder();
        let bytes_field = schema_builder
            .add_bytes_field("data", BytesOptions::default().set_fast().set_indexed());
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());
        let mut index_writer: IndexWriter = index.writer_for_tests()?;

        // Insert documents with lexicographically sortable byte values
        // Using simple byte sequences that have clear ordering
        let values: Vec<Vec<u8>> = vec![
            vec![0x00, 0x10],
            vec![0x00, 0x20],
            vec![0x00, 0x30],
            vec![0x01, 0x00],
            vec![0x01, 0x10],
            vec![0x02, 0x00],
        ];

        for value in &values {
            let mut doc = TantivyDocument::new();
            doc.add_bytes(bytes_field, value);
            index_writer.add_document(doc)?;
        }
        index_writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();

        // Test: Range query [0x00, 0x20] to [0x01, 0x00] (inclusive)
        // Should match: [0x00, 0x20], [0x00, 0x30], [0x01, 0x00]
        let lower = Term::from_field_bytes(bytes_field, &[0x00, 0x20]);
        let upper = Term::from_field_bytes(bytes_field, &[0x01, 0x00]);
        let range_query = RangeQuery::new(Bound::Included(lower), Bound::Included(upper));
        let count = searcher.search(&range_query, &Count)?;
        assert_eq!(
            count, 3,
            "Expected 3 documents in range [0x00,0x20] to [0x01,0x00]"
        );

        // Test: Range query > [0x01, 0x00] (exclusive lower bound)
        // Should match: [0x01, 0x10], [0x02, 0x00]
        let lower = Term::from_field_bytes(bytes_field, &[0x01, 0x00]);
        let range_query = RangeQuery::new(Bound::Excluded(lower), Bound::Unbounded);
        let count = searcher.search(&range_query, &Count)?;
        assert_eq!(count, 2, "Expected 2 documents > [0x01,0x00]");

        // Test: Range query < [0x00, 0x30] (exclusive upper bound)
        // Should match: [0x00, 0x10], [0x00, 0x20]
        let upper = Term::from_field_bytes(bytes_field, &[0x00, 0x30]);
        let range_query = RangeQuery::new(Bound::Unbounded, Bound::Excluded(upper));
        let count = searcher.search(&range_query, &Count)?;
        assert_eq!(count, 2, "Expected 2 documents < [0x00,0x30]");

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod ip_range_tests {
    use proptest::prelude::ProptestConfig;
    use proptest::strategy::Strategy;
    use proptest::{prop_oneof, proptest};

    use super::*;
    use crate::collector::Count;
    use crate::query::QueryParser;
    use crate::schema::{Schema, FAST, INDEXED, STORED, STRING};
    use crate::{Index, IndexWriter};

    #[derive(Clone, Debug)]
    pub struct Doc {
        pub id: String,
        pub ip: Ipv6Addr,
    }

    fn operation_strategy() -> impl Strategy<Value = Doc> {
        prop_oneof![
            (0u64..10_000u64).prop_map(doc_from_id_1),
            (1u64..10_000u64).prop_map(doc_from_id_2),
        ]
    }

    pub fn doc_from_id_1(id: u64) -> Doc {
        let id = id * 1000;
        Doc {
            // ip != id
            id: id.to_string(),
            ip: Ipv6Addr::from_u128(id as u128),
        }
    }
    fn doc_from_id_2(id: u64) -> Doc {
        let id = id * 1000;
        Doc {
            // ip != id
            id: (id - 1).to_string(),
            ip: Ipv6Addr::from_u128(id as u128),
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10))]
        #[test]
        fn test_ip_range_for_docs_prop(ops in proptest::collection::vec(operation_strategy(), 1..1000)) {
            assert!(test_ip_range_for_docs(&ops).is_ok());
        }
    }

    #[test]
    fn test_ip_range_regression1() {
        let ops = &[doc_from_id_1(0)];
        assert!(test_ip_range_for_docs(ops).is_ok());
    }

    #[test]
    fn test_ip_range_regression2() {
        let ops = &[
            doc_from_id_1(52),
            doc_from_id_1(63),
            doc_from_id_1(12),
            doc_from_id_2(91),
            doc_from_id_2(33),
        ];
        assert!(test_ip_range_for_docs(ops).is_ok());
    }

    #[test]
    fn test_ip_range_regression3() {
        let ops = &[doc_from_id_1(1), doc_from_id_1(2), doc_from_id_1(3)];
        assert!(test_ip_range_for_docs(ops).is_ok());
    }

    #[test]
    fn test_ip_range_regression3_simple() {
        let mut schema_builder = Schema::builder();
        let ips_field = schema_builder.add_ip_addr_field("ips", FAST | INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer: IndexWriter = index.writer_for_tests().unwrap();
        let ip_addrs: Vec<Ipv6Addr> = [1000, 2000, 3000]
            .into_iter()
            .map(Ipv6Addr::from_u128)
            .collect();
        for &ip_addr in &ip_addrs {
            writer
                .add_document(doc!(ips_field=>ip_addr, ips_field=>ip_addr))
                .unwrap();
        }
        writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let range_weight = FastFieldRangeWeight::new(BoundsRange::new(
            Bound::Included(Term::from_field_ip_addr(ips_field, ip_addrs[1])),
            Bound::Included(Term::from_field_ip_addr(ips_field, ip_addrs[2])),
        ));

        let count =
            crate::query::weight::Weight::count(&range_weight, searcher.segment_reader(0)).unwrap();
        assert_eq!(count, 2);
    }

    pub fn create_index_from_ip_docs(docs: &[Doc]) -> Index {
        let mut schema_builder = Schema::builder();
        let ip_field = schema_builder.add_ip_addr_field("ip", STORED | FAST);
        let ips_field = schema_builder.add_ip_addr_field("ips", FAST | INDEXED);
        let text_field = schema_builder.add_text_field("id", STRING | STORED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        {
            let mut index_writer = index.writer_with_num_threads(2, 60_000_000).unwrap();
            for doc in docs.iter() {
                index_writer
                    .add_document(doc!(
                        ips_field => doc.ip,
                        ips_field => doc.ip,
                        ip_field => doc.ip,
                        text_field => doc.id.to_string(),
                    ))
                    .unwrap();
            }

            index_writer.commit().unwrap();
        }
        index
    }

    fn test_ip_range_for_docs(docs: &[Doc]) -> crate::Result<()> {
        let index = create_index_from_ip_docs(docs);
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        let get_num_hits = |query| searcher.search(&query, &Count).unwrap();
        let query_from_text = |text: &str| {
            QueryParser::for_index(&index, vec![])
                .parse_query(text)
                .unwrap()
        };

        let gen_query_inclusive = |field: &str, ip_range: &RangeInclusive<Ipv6Addr>| {
            format!("{field}:[{} TO {}]", ip_range.start(), ip_range.end())
        };

        let test_sample = |sample_docs: &[Doc]| {
            let mut ips: Vec<Ipv6Addr> = sample_docs.iter().map(|doc| doc.ip).collect();
            ips.sort();
            let ip_range = ips[0]..=ips[1];
            let expected_num_hits = docs
                .iter()
                .filter(|doc| (ips[0]..=ips[1]).contains(&doc.ip))
                .count();

            let query = gen_query_inclusive("ip", &ip_range);
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);

            let query = gen_query_inclusive("ips", &ip_range);
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);

            // Intersection search
            let id_filter = sample_docs[0].id.to_string();
            let expected_num_hits = docs
                .iter()
                .filter(|doc| ip_range.contains(&doc.ip) && doc.id == id_filter)
                .count();
            let query = format!(
                "{} AND id:{}",
                gen_query_inclusive("ip", &ip_range),
                &id_filter
            );
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);

            // Intersection search on multivalue ip field
            let id_filter = sample_docs[0].id.to_string();
            let query = format!(
                "{} AND id:{}",
                gen_query_inclusive("ips", &ip_range),
                &id_filter
            );
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);
        };

        test_sample(&[docs[0].clone(), docs[0].clone()]);
        if docs.len() > 1 {
            test_sample(&[docs[0].clone(), docs[1].clone()]);
            test_sample(&[docs[1].clone(), docs[1].clone()]);
        }
        if docs.len() > 2 {
            test_sample(&[docs[1].clone(), docs[2].clone()]);
        }

        Ok(())
    }
}

#[cfg(test)]
mod sorted_range_tests {
    use std::ops::Bound;

    use common::bounds::BoundsRange;

    use crate::collector::Count;
    use crate::index::{Index, IndexSettings, IndexSortByField};
    use crate::query::range_query::range_query_fastfield::FastFieldRangeWeight;
    use crate::query::{RangeQuery, Scorer, Weight};
    use crate::schema::{BytesOptions, NumericOptions, SchemaBuilder, FAST};
    use crate::{IndexWriter, Order, TantivyDocument, Term};

    fn create_sorted_index_u64(order: Order, values: &[u64]) -> (Index, crate::schema::Field) {
        let mut schema_builder = SchemaBuilder::new();
        let field = schema_builder.add_u64_field(
            "intval",
            NumericOptions::default()
                .set_fast()
                .set_indexed()
                .set_stored(),
        );
        let schema = schema_builder.build();

        let index = Index::builder()
            .schema(schema)
            .settings(IndexSettings {
                sort_by_field: Some(IndexSortByField {
                    field: "intval".to_string(),
                    order,
                }),
                ..Default::default()
            })
            .create_in_ram()
            .unwrap();

        let mut writer = index.writer_for_tests().unwrap();
        for &val in values {
            writer.add_document(doc!(field => val)).unwrap();
        }
        writer.commit().unwrap();
        (index, field)
    }

    fn create_sorted_index_optional_u64(
        order: Order,
        values: &[Option<u64>],
    ) -> (Index, crate::schema::Field, crate::schema::Field) {
        let mut schema_builder = SchemaBuilder::new();
        let label_field = schema_builder.add_text_field("label", crate::schema::STRING);
        let field = schema_builder
            .add_u64_field("intval", NumericOptions::default().set_fast().set_indexed());
        let schema = schema_builder.build();

        let index = Index::builder()
            .schema(schema)
            .settings(IndexSettings {
                sort_by_field: Some(IndexSortByField {
                    field: "intval".to_string(),
                    order,
                }),
                ..Default::default()
            })
            .create_in_ram()
            .unwrap();

        let mut writer = index.writer_for_tests().unwrap();
        for (i, val) in values.iter().enumerate() {
            match val {
                Some(v) => {
                    writer
                        .add_document(doc!(label_field => format!("doc{i}"), field => *v))
                        .unwrap();
                }
                None => {
                    writer
                        .add_document(doc!(label_field => format!("doc{i}")))
                        .unwrap();
                }
            }
        }
        writer.commit().unwrap();
        (index, field, label_field)
    }

    fn count_range(
        index: &Index,
        field: crate::schema::Field,
        lower: Bound<u64>,
        upper: Bound<u64>,
    ) -> usize {
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let query = RangeQuery::new(
            lower.map(|v| Term::from_field_u64(field, v)),
            upper.map(|v| Term::from_field_u64(field, v)),
        );
        searcher.search(&query, &Count).unwrap()
    }

    fn delete_by_u64_value(index: &Index, field: crate::schema::Field, value: u64) {
        let mut writer: IndexWriter = index.writer_for_tests().unwrap();
        writer.delete_term(Term::from_field_u64(field, value));
        writer.commit().unwrap();
    }

    fn create_unsorted_index_u64(values: &[u64]) -> (Index, crate::schema::Field) {
        let mut schema_builder = SchemaBuilder::new();
        let field = schema_builder.add_u64_field(
            "intval",
            NumericOptions::default()
                .set_fast()
                .set_indexed()
                .set_stored(),
        );
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests().unwrap();
        for &val in values {
            writer.add_document(doc!(field => val)).unwrap();
        }
        writer.commit().unwrap();
        (index, field)
    }

    /// Build a scorer for the given field on the given reader.
    fn make_scorer(
        reader: &crate::SegmentReader,
        field: crate::schema::Field,
        lower: u64,
        upper: u64,
    ) -> Box<dyn Scorer> {
        let weight = FastFieldRangeWeight::new(BoundsRange::new(
            Bound::Included(Term::from_field_u64(field, lower)),
            Bound::Included(Term::from_field_u64(field, upper)),
        ));
        weight.scorer(reader, 1.0).unwrap()
    }

    #[test]
    fn sorted_range_activates_when_sort_field_matches() {
        for order in [Order::Asc, Order::Desc] {
            let (index, field) = create_sorted_index_u64(order, &[10, 20, 30, 40, 50]);
            let reader = index.reader().unwrap();
            let searcher = reader.searcher();
            let seg = searcher.segment_reader(0);
            let sorted_cost = make_scorer(seg, field, 20, 40).cost();

            let (unsorted_index, unsorted_field) = create_unsorted_index_u64(&[10, 20, 30, 40, 50]);
            let unsorted_reader = unsorted_index.reader().unwrap();
            let unsorted_searcher = unsorted_reader.searcher();
            let unsorted_seg = unsorted_searcher.segment_reader(0);
            let unsorted_cost = make_scorer(unsorted_seg, unsorted_field, 20, 40).cost();
            assert!(
                sorted_cost < unsorted_cost,
                "order={order:?}: sorted path cost ({sorted_cost}) should be less than fallback \
                 ({unsorted_cost})"
            );
        }
    }

    fn assert_sorted_range_cases(order: Order, cases: &[(&str, Bound<u64>, Bound<u64>, usize)]) {
        let (index, field) =
            create_sorted_index_u64(order, &[10, 20, 30, 40, 50, 60, 70, 80, 90, 100]);
        for (name, lower, upper, expected) in cases {
            let actual = count_range(&index, field, lower.clone(), upper.clone());
            assert_eq!(actual, *expected, "order={order:?}, case={name}");
        }
    }

    #[test]
    fn sorted_range_falls_back_when_index_not_sorted() {
        let (index, field) = create_unsorted_index_u64(&[10, 20, 30, 40, 50]);
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let seg = searcher.segment_reader(0);
        let no_sort_cost = make_scorer(seg, field, 20, 40).cost();
        assert!(no_sort_cost > 0, "Expected nonzero cost from RangeDocSet");
        // And the result is correct.
        assert_eq!(
            count_range(&index, field, Bound::Included(20), Bound::Included(40)),
            3
        );
    }

    #[test]
    fn sorted_range_falls_back_when_sort_field_mismatches_query_field() {
        let mut schema_builder = SchemaBuilder::new();
        let intval_field = schema_builder
            .add_u64_field("intval", NumericOptions::default().set_fast().set_indexed());
        let other_field = schema_builder
            .add_u64_field("other", NumericOptions::default().set_fast().set_indexed());
        let index = Index::builder()
            .schema(schema_builder.build())
            .settings(IndexSettings {
                sort_by_field: Some(IndexSortByField {
                    field: "intval".to_string(),
                    order: Order::Asc,
                }),
                ..Default::default()
            })
            .create_in_ram()
            .unwrap();
        let mut writer = index.writer_for_tests().unwrap();
        for &v in &[10u64, 20, 30, 40, 50] {
            writer
                .add_document(doc!(intval_field => v, other_field => v * 2))
                .unwrap();
        }
        writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let seg = searcher.segment_reader(0);

        // Query is on "other" but index is sorted by "intval" — should not use sorted path.
        let mismatched_cost = make_scorer(seg, other_field, 40, 80).cost();

        let mut unsorted_schema_builder = SchemaBuilder::new();
        let unsorted_intval = unsorted_schema_builder
            .add_u64_field("intval", NumericOptions::default().set_fast().set_indexed());
        let unsorted_other = unsorted_schema_builder
            .add_u64_field("other", NumericOptions::default().set_fast().set_indexed());
        let unsorted_index = Index::create_in_ram(unsorted_schema_builder.build());
        let mut unsorted_writer = unsorted_index.writer_for_tests().unwrap();
        for &v in &[10u64, 20, 30, 40, 50] {
            unsorted_writer
                .add_document(doc!(unsorted_intval => v, unsorted_other => v * 2))
                .unwrap();
        }
        unsorted_writer.commit().unwrap();
        let unsorted_reader = unsorted_index.reader().unwrap();
        let unsorted_searcher = unsorted_reader.searcher();
        let unsorted_seg = unsorted_searcher.segment_reader(0);
        let no_sort_cost = make_scorer(unsorted_seg, unsorted_other, 40, 80).cost();

        // Both should produce the same fallback cost since sort field != query field.
        assert_eq!(
            mismatched_cost, no_sort_cost,
            "Mismatched sort field should produce same cost as no-sort fallback"
        );
    }

    #[test]
    fn sorted_range_asc_bounds_matrix() {
        let cases = vec![
            ("inclusive", Bound::Included(20), Bound::Included(80), 7),
            ("exclusive", Bound::Excluded(20), Bound::Excluded(80), 5),
            (
                "mixed_incl_excl",
                Bound::Included(20),
                Bound::Excluded(80),
                6,
            ),
            (
                "mixed_excl_incl",
                Bound::Excluded(20),
                Bound::Included(80),
                6,
            ),
            ("unbounded_lower", Bound::Unbounded, Bound::Included(50), 5),
            ("unbounded_upper", Bound::Included(50), Bound::Unbounded, 6),
            (
                "wide_range",
                Bound::Included(0),
                Bound::Included(u64::MAX),
                10,
            ),
            ("exact_hit", Bound::Included(50), Bound::Included(50), 1),
            ("exact_miss", Bound::Included(55), Bound::Included(55), 0),
        ];
        assert_sorted_range_cases(Order::Asc, &cases);
    }

    #[test]
    fn sorted_range_desc_bounds_matrix() {
        let cases = vec![
            ("inclusive", Bound::Included(40), Bound::Included(60), 3),
            ("exclusive", Bound::Excluded(20), Bound::Excluded(80), 5),
            (
                "mixed_incl_excl",
                Bound::Included(20),
                Bound::Excluded(80),
                6,
            ),
            (
                "mixed_excl_incl",
                Bound::Excluded(20),
                Bound::Included(80),
                6,
            ),
            ("unbounded_lower", Bound::Unbounded, Bound::Included(50), 5),
            ("unbounded_upper", Bound::Included(50), Bound::Unbounded, 6),
            ("exact_hit", Bound::Included(50), Bound::Included(50), 1),
            ("exact_miss", Bound::Included(55), Bound::Included(55), 0),
        ];
        assert_sorted_range_cases(Order::Desc, &cases);
    }

    #[test]
    fn sorted_range_optional_nulls_excluded() {
        // ASC and DESC both exclude NULLs from range results.
        for order in [Order::Asc, Order::Desc] {
            let (index, field, _) = create_sorted_index_optional_u64(
                order,
                &[None, None, Some(10), Some(20), Some(30), Some(40)],
            );
            assert_eq!(
                count_range(&index, field, Bound::Included(10), Bound::Included(30)),
                3,
                "order={order:?}: NULLs should be excluded from range"
            );
            // Wide range still excludes NULLs.
            assert_eq!(
                count_range(&index, field, Bound::Included(0), Bound::Included(u64::MAX)),
                4,
                "order={order:?}: wide range should still exclude NULLs"
            );
        }

        // All-NULL segment returns empty.
        let (index, field, _) =
            create_sorted_index_optional_u64(Order::Asc, &[None, None, None, None, None]);
        assert_eq!(
            count_range(&index, field, Bound::Included(0), Bound::Included(100)),
            0,
            "all-NULL segment should return 0"
        );

        // Single non-NULL among NULLs.
        let (index, field, _) =
            create_sorted_index_optional_u64(Order::Asc, &[None, None, None, None, Some(50)]);
        assert_eq!(
            count_range(&index, field, Bound::Included(50), Bound::Included(50)),
            1,
            "single non-NULL exact match"
        );

        // Interleaved NULLs and zeros (regression: NULL encoded as 0 must not collide).
        let (index, field, _) =
            create_sorted_index_optional_u64(Order::Asc, &[None, Some(0), None, Some(1)]);
        assert_eq!(
            count_range(&index, field, Bound::Included(0), Bound::Included(0)),
            1,
            "NULL-vs-zero interleave regression"
        );
    }

    /// Helper: create a sorted index with Optional cardinality across multiple
    /// segments (one commit per batch), then force-merge into a single segment.
    fn create_merged_sorted_index_optional_u64(
        order: Order,
        batches: &[&[Option<u64>]],
    ) -> (Index, crate::schema::Field) {
        let mut schema_builder = SchemaBuilder::new();
        let label_field = schema_builder.add_text_field("label", crate::schema::STRING);
        let field = schema_builder
            .add_u64_field("intval", NumericOptions::default().set_fast().set_indexed());
        let schema = schema_builder.build();

        let index = Index::builder()
            .schema(schema)
            .settings(IndexSettings {
                sort_by_field: Some(IndexSortByField {
                    field: "intval".to_string(),
                    order,
                }),
                ..Default::default()
            })
            .create_in_ram()
            .unwrap();

        let mut doc_counter = 0usize;
        {
            let mut writer = index.writer_for_tests().unwrap();
            for batch in batches {
                for val in *batch {
                    match val {
                        Some(v) => {
                            writer
                                .add_document(
                                    doc!(label_field => format!("doc{doc_counter}"), field => *v),
                                )
                                .unwrap();
                        }
                        None => {
                            writer
                                .add_document(doc!(label_field => format!("doc{doc_counter}")))
                                .unwrap();
                        }
                    }
                    doc_counter += 1;
                }
                writer.commit().unwrap();
            }
        }

        // Force merge all segments into one.
        {
            let segment_ids = index
                .searchable_segment_ids()
                .expect("searchable_segment_ids failed");
            let mut writer: IndexWriter = index.writer_for_tests().unwrap();
            writer.merge(&segment_ids).wait().expect("merge failed");
            writer
                .wait_merging_threads()
                .expect("wait_merging_threads failed");
        }

        (index, field)
    }

    /// Regression test: after merging multiple segments with NULLs and zeros,
    /// the sorted range path must still correctly exclude NULLs.
    /// Depends on the merger NULL-ordering fix (tantivy PR #106).
    #[test]
    fn sorted_range_optional_correctness_after_merge() {
        for order in [Order::Asc, Order::Desc] {
            let (index, field) = create_merged_sorted_index_optional_u64(
                order,
                &[
                    &[None, Some(0), Some(10)],
                    &[None, Some(0), Some(20)],
                    &[None, Some(5), Some(30)],
                ],
            );

            let reader = index.reader().unwrap();
            let searcher = reader.searcher();
            assert_eq!(
                searcher.segment_readers().len(),
                1,
                "order={order:?}: expected 1 segment after merge"
            );

            // Range [0, 20]: should match 0, 0, 5, 10, 20 = 5 docs (NULLs excluded).
            assert_eq!(
                count_range(&index, field, Bound::Included(0), Bound::Included(20)),
                5,
                "order={order:?}: range [0,20]"
            );
            // Range [0, 0]: should match exactly the two zeros.
            assert_eq!(
                count_range(&index, field, Bound::Included(0), Bound::Included(0)),
                2,
                "order={order:?}: range [0,0]"
            );
            // Wide range covering all non-NULL values.
            assert_eq!(
                count_range(&index, field, Bound::Included(0), Bound::Included(u64::MAX)),
                6,
                "order={order:?}: wide range"
            );
        }
    }

    /// Regression test: the sort key previously used `f64::coerce(nv) as f32`,
    /// which has only 24 bits of mantissa.  Values above 2^24 (16,777,216)
    /// could collide — e.g. 16,777,216 and 16,777,217 both round to the same
    /// f32 — causing wrong sort order and incorrect binary-search results.
    /// The fix compares each NumericalValue variant in its native type.
    #[test]
    fn sorted_range_f32_precision_no_collision_regression() {
        // 16_777_216 = 2^24, 16_777_217 = 2^24+1: identical as f32
        let (index, field) = create_sorted_index_u64(Order::Asc, &[16_777_217, 16_777_216]);
        // Exact match for 16_777_216 must return exactly 1 doc, not 2.
        assert_eq!(
            count_range(
                &index,
                field,
                Bound::Included(16_777_216),
                Bound::Included(16_777_216)
            ),
            1
        );
        // Exact match for 16_777_217 must also return exactly 1 doc.
        assert_eq!(
            count_range(
                &index,
                field,
                Bound::Included(16_777_217),
                Bound::Included(16_777_217)
            ),
            1
        );
        // Range covering both must return 2.
        assert_eq!(
            count_range(
                &index,
                field,
                Bound::Included(16_777_216),
                Bound::Included(16_777_217)
            ),
            2
        );
    }

    /// Regression test: even after widening the sort key from f32 to f64,
    /// u64 values above 2^53 could still collide — e.g.
    /// 9,007,199,254,740,992 (2^53) and 9,007,199,254,740,993 (2^53+1) round to
    /// the same f64 — causing wrong sort order and incorrect binary-search
    /// results.  The fix compares each NumericalValue variant in its native
    /// type, so u64 values are compared as u64 with zero precision loss.
    #[test]
    fn sorted_range_u64_precision_above_2_53_regression() {
        let (index, field) =
            create_sorted_index_u64(Order::Asc, &[9_007_199_254_740_993, 9_007_199_254_740_992]);
        // Exact match for 2^53 must return exactly 1, not 2.
        assert_eq!(
            count_range(
                &index,
                field,
                Bound::Included(9_007_199_254_740_992),
                Bound::Included(9_007_199_254_740_992)
            ),
            1
        );
        // Exact match for 2^53+1 must also return exactly 1.
        assert_eq!(
            count_range(
                &index,
                field,
                Bound::Included(9_007_199_254_740_993),
                Bound::Included(9_007_199_254_740_993)
            ),
            1
        );
    }

    #[test]
    fn sorted_range_with_deletes() {
        // Delete middle values (40, 50), query spanning them.
        let (index, field) = create_sorted_index_u64(Order::Asc, &[10, 20, 30, 40, 50, 60, 70, 80]);
        delete_by_u64_value(&index, field, 40);
        delete_by_u64_value(&index, field, 50);
        assert_eq!(
            count_range(&index, field, Bound::Included(30), Bound::Included(60)),
            2,
            "middle deletes: only 30, 60 survive"
        );
        assert_eq!(
            count_range(&index, field, Bound::Included(40), Bound::Included(50)),
            0,
            "all matching docs deleted"
        );

        // Delete boundary values (first, last).
        let (index, field) = create_sorted_index_u64(Order::Asc, &[10, 20, 30, 40, 50, 60, 70, 80]);
        delete_by_u64_value(&index, field, 10);
        delete_by_u64_value(&index, field, 80);
        assert_eq!(
            count_range(&index, field, Bound::Included(10), Bound::Included(80)),
            6,
            "boundary deletes: 6 of 8 survive"
        );
    }

    #[test]
    fn sorted_range_str_dictionary_bounds_correct() {
        let mut schema_builder = SchemaBuilder::new();
        let field = schema_builder.add_text_field("strval", crate::schema::TEXT | FAST);
        let schema = schema_builder.build();
        let index = Index::builder()
            .schema(schema)
            .settings(IndexSettings {
                sort_by_field: Some(IndexSortByField {
                    field: "strval".to_string(),
                    order: Order::Asc,
                }),
                ..Default::default()
            })
            .create_in_ram()
            .unwrap();
        let mut writer = index.writer_for_tests().unwrap();
        for val in &["apple", "banana", "cherry", "date", "elderberry"] {
            writer.add_document(doc!(field => *val)).unwrap();
        }
        writer.commit().unwrap();

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let lower = Term::from_field_text(field, "banana");
        let upper = Term::from_field_text(field, "date");
        let query = RangeQuery::new(Bound::Included(lower), Bound::Included(upper));
        let count = searcher.search(&query, &Count).unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn sorted_range_bytes_dictionary_bounds_correct() {
        let mut schema_builder = SchemaBuilder::new();
        let field = schema_builder
            .add_bytes_field("bytesval", BytesOptions::default().set_fast().set_indexed());
        let schema = schema_builder.build();
        let index = Index::builder()
            .schema(schema)
            .settings(IndexSettings {
                sort_by_field: Some(IndexSortByField {
                    field: "bytesval".to_string(),
                    order: Order::Asc,
                }),
                ..Default::default()
            })
            .create_in_ram()
            .unwrap();
        let mut writer = index.writer_for_tests().unwrap();
        let values: Vec<Vec<u8>> = vec![
            vec![0x00, 0x10],
            vec![0x00, 0x20],
            vec![0x00, 0x30],
            vec![0x01, 0x00],
        ];
        for val in &values {
            let mut doc = TantivyDocument::new();
            doc.add_bytes(field, val);
            writer.add_document(doc).unwrap();
        }
        writer.commit().unwrap();

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let lower = Term::from_field_bytes(field, &[0x00, 0x20]);
        let upper = Term::from_field_bytes(field, &[0x00, 0x30]);
        let query = RangeQuery::new(Bound::Included(lower), Bound::Included(upper));
        let count = searcher.search(&query, &Count).unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn sorted_range_edge_cases() {
        // Empty segment.
        let (index, field) = create_sorted_index_u64(Order::Asc, &[]);
        assert_eq!(
            count_range(&index, field, Bound::Included(0), Bound::Included(100)),
            0,
            "empty segment"
        );

        // Single doc: matching and not matching.
        let (index, field) = create_sorted_index_u64(Order::Asc, &[50]);
        assert_eq!(
            count_range(&index, field, Bound::Included(50), Bound::Included(50)),
            1,
            "single doc match"
        );
        assert_eq!(
            count_range(&index, field, Bound::Included(100), Bound::Included(200)),
            0,
            "single doc miss"
        );

        // u64::MAX boundary.
        let (index, field) =
            create_sorted_index_u64(Order::Asc, &[u64::MAX - 2, u64::MAX - 1, u64::MAX]);
        assert_eq!(
            count_range(
                &index,
                field,
                Bound::Included(u64::MAX - 1),
                Bound::Included(u64::MAX)
            ),
            2,
            "max u64 boundary"
        );

        // u64 min (zero) boundary.
        let (index, field) = create_sorted_index_u64(Order::Asc, &[0, 1, 2]);
        assert_eq!(
            count_range(&index, field, Bound::Included(0), Bound::Included(0)),
            1,
            "min u64 boundary"
        );

        // All same value.
        let vals: Vec<u64> = vec![42; 100];
        let (index, field) = create_sorted_index_u64(Order::Asc, &vals);
        assert_eq!(
            count_range(&index, field, Bound::Included(42), Bound::Included(42)),
            100,
            "all same: exact match"
        );
        assert_eq!(
            count_range(&index, field, Bound::Included(43), Bound::Included(43)),
            0,
            "all same: miss"
        );
        assert_eq!(
            count_range(&index, field, Bound::Included(41), Bound::Included(43)),
            100,
            "all same: covering range"
        );
    }

    fn create_sorted_intersection_test_index() -> (Index, crate::schema::Field, crate::schema::Field)
    {
        let mut schema_builder = SchemaBuilder::new();
        let intval = schema_builder
            .add_u64_field("intval", NumericOptions::default().set_fast().set_indexed());
        let label = schema_builder.add_text_field("label", crate::schema::STRING);
        let index = Index::builder()
            .schema(schema_builder.build())
            .settings(IndexSettings {
                sort_by_field: Some(IndexSortByField {
                    field: "intval".to_string(),
                    order: Order::Asc,
                }),
                ..Default::default()
            })
            .create_in_ram()
            .unwrap();
        let mut writer = index.writer_for_tests().unwrap();
        for (v, l) in &[
            (10u64, "alpha"),
            (20, "beta"),
            (30, "alpha"),
            (40, "beta"),
            (50, "alpha"),
            (60, "beta"),
        ] {
            writer
                .add_document(doc!(intval => *v, label => *l))
                .unwrap();
        }
        writer.commit().unwrap();
        (index, intval, label)
    }

    #[test]
    fn sorted_range_term_intersection_matrix() {
        let (index, intval, label) = create_sorted_intersection_test_index();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let cases = vec![
            ("medium_range_alpha", 20, 50, "alpha", 2usize),
            ("narrow_range_alpha", 30, 30, "alpha", 1usize),
            ("broad_range_beta", 10, 60, "beta", 3usize),
        ];
        for (name, lower, upper, term, expected_count) in cases {
            let range_q = RangeQuery::new(
                Bound::Included(Term::from_field_u64(intval, lower)),
                Bound::Included(Term::from_field_u64(intval, upper)),
            );
            let term_q = crate::query::TermQuery::new(
                Term::from_field_text(label, term),
                crate::schema::IndexRecordOption::Basic,
            );
            let bool_q =
                crate::query::BooleanQuery::intersection(vec![Box::new(range_q), Box::new(term_q)]);
            let count = searcher.search(&bool_q, &Count).unwrap();
            assert_eq!(count, expected_count, "{name}");
        }
    }

    use proptest::prelude::*;

    /// Reference implementation: filter values in Rust and count matches.
    fn reference_count(values: &[u64], lower: Bound<u64>, upper: Bound<u64>) -> usize {
        values
            .iter()
            .filter(|&&v| {
                let lo_ok = match lower {
                    Bound::Included(lo) => v >= lo,
                    Bound::Excluded(lo) => v > lo,
                    Bound::Unbounded => true,
                };
                let hi_ok = match upper {
                    Bound::Included(hi) => v <= hi,
                    Bound::Excluded(hi) => v < hi,
                    Bound::Unbounded => true,
                };
                lo_ok && hi_ok
            })
            .count()
    }

    /// Strategy that produces a valid (lower, upper) bound pair where
    /// at least one bound is set (as required by RangeQuery).
    fn bound_pair_strategy() -> impl Strategy<Value = (Bound<u64>, Bound<u64>)> {
        // Generate two values, then pick bound types
        (0u64..10000, 0u64..10000, 0u8..4).prop_map(|(a, b, variant)| {
            let (lo_val, hi_val) = if a <= b { (a, b) } else { (b, a) };
            match variant {
                0 => (Bound::Included(lo_val), Bound::Included(hi_val)),
                1 => (Bound::Excluded(lo_val), Bound::Included(hi_val)),
                2 => (Bound::Included(lo_val), Bound::Excluded(hi_val)),
                _ => (Bound::Excluded(lo_val), Bound::Excluded(hi_val)),
            }
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn proptest_sorted_asc_range_equivalence(
            values in proptest::collection::vec(0u64..10000, 1..200),
            bounds in bound_pair_strategy(),
        ) {
            let (index, field) = create_sorted_index_u64(Order::Asc, &values);
            let actual = count_range(&index, field, bounds.0, bounds.1);
            let expected = reference_count(&values, bounds.0, bounds.1);
            prop_assert_eq!(actual, expected);
        }

        #[test]
        fn proptest_sorted_desc_range_equivalence(
            values in proptest::collection::vec(0u64..10000, 1..200),
            bounds in bound_pair_strategy(),
        ) {
            let (index, field) = create_sorted_index_u64(Order::Desc, &values);
            let actual = count_range(&index, field, bounds.0, bounds.1);
            let expected = reference_count(&values, bounds.0, bounds.1);
            prop_assert_eq!(actual, expected);
        }

        #[test]
        fn proptest_sorted_with_nulls_equivalence(
            non_null_values in proptest::collection::vec(0u64..10000, 0..100),
            null_count in 0usize..50,
            bounds in bound_pair_strategy(),
        ) {
            let mut values: Vec<Option<u64>> = non_null_values.iter().map(|&v| Some(v)).collect();
            for _ in 0..null_count {
                values.push(None);
            }
            let (index, field, _) = create_sorted_index_optional_u64(Order::Asc, &values);
            let actual = count_range(&index, field, bounds.0, bounds.1);
            // Nulls should never match
            let expected = reference_count(&non_null_values, bounds.0, bounds.1);
            prop_assert_eq!(actual, expected);
        }

        #[test]
        fn proptest_sorted_with_deletes_equivalence(
            values in proptest::collection::vec(0u64..100, 5..100),
            bounds in bound_pair_strategy(),
            delete_indices in proptest::collection::vec(0usize..100, 0..20),
        ) {
            let (index, field) = create_sorted_index_u64(Order::Asc, &values);
            // Deduplicate delete values — we delete by value, so multiple
            // docs with the same value all get deleted.
            let mut deleted_values = std::collections::HashSet::new();
            for &idx in &delete_indices {
                if idx < values.len() {
                    let v = values[idx];
                    if !deleted_values.contains(&v) {
                        deleted_values.insert(v);
                        delete_by_u64_value(&index, field, v);
                    }
                }
            }
            let surviving: Vec<u64> = values
                .iter()
                .filter(|v| !deleted_values.contains(v))
                .copied()
                .collect();
            let actual = count_range(&index, field, bounds.0, bounds.1);
            let expected = reference_count(&surviving, bounds.0, bounds.1);
            prop_assert_eq!(actual, expected);
        }

        #[test]
        fn proptest_sorted_vs_unsorted_equivalence(
            values in proptest::collection::vec(0u64..10000, 1..200),
            bounds in bound_pair_strategy(),
        ) {
            // Sorted index
            let (sorted_index, sorted_field) =
                create_sorted_index_u64(Order::Asc, &values);
            let sorted_count =
                count_range(&sorted_index, sorted_field, bounds.0, bounds.1);

            // Unsorted index (same data, no sort setting)
            let mut schema_builder = SchemaBuilder::new();
            let unsorted_field = schema_builder.add_u64_field(
                "intval",
                NumericOptions::default().set_fast().set_indexed(),
            );
            let unsorted_index = Index::create_in_ram(schema_builder.build());
            let mut writer = unsorted_index.writer_for_tests().unwrap();
            for &val in &values {
                writer.add_document(doc!(unsorted_field => val)).unwrap();
            }
            writer.commit().unwrap();
            let unsorted_count =
                count_range(&unsorted_index, unsorted_field, bounds.0, bounds.1);

            prop_assert_eq!(sorted_count, unsorted_count);
        }
    }
}
