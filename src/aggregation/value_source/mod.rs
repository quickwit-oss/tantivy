mod block_accessor;
#[cfg(feature = "jitexpr")]
pub(crate) mod jitexpr_value_source;
mod value_source_registry;

#[cfg(test)]
pub(crate) mod tests;

use std::borrow::Borrow;
use std::io;

pub(crate) use block_accessor::ColumnBlockAccessor;
use columnar::{
    Cardinality, Column, ColumnType, ColumnValues, Dictionary, MonotonicallyMappableToU64, RowId,
    StrColumn,
};
#[cfg(feature = "jitexpr")]
pub use jitexpr_value_source::JitExprValueSourceProvider;
pub use value_source_registry::{ValueSourceProvider, ValueSourceRegistry};

use crate::aggregation::f64_from_fastfield_u64;
use crate::DocId;

/// A source of values for a block of documents.
pub trait ValueSource: std::fmt::Debug {
    /// Logical type of the encoded values returned by this source.
    ///
    /// Numeric values use the corresponding monotonic `u64` mapping; string/bytes
    /// values are dictionary ordinals and IP addresses use the compact space ord.
    fn column_type(&self) -> ColumnType;

    /// Loads the values for `docs` into `values`.
    ///
    /// Precondition: `docs` has to be strictly increasing.
    ///
    /// The output buffers are reused across blocks: on entry, `values` and `docids`
    /// hold stale data from a previous call. Implementations must clear their
    /// content (not append to it).
    ///
    /// On return, depending on the returned `Cardinality`:
    /// - `Full`: `values.len() == docs.len()` and `values[i]` is the value of `docs[i]`. `docids`
    ///   is left unspecified and must not be read by the caller.
    /// - `Optional` / `Multivalued`: `docids.len() == values.len()` and `values[i]` is a value of
    ///   `docids[i]`. `docids` only contains docs from `docs`. A doc is repeated once per value.
    ///
    /// `row_ids` is scratch the implementation may use freely.
    ///
    /// Takes `&mut self` so that implementations can keep per-segment state (caches, scratch
    /// buffers) across blocks. Each source is owned by a single collector.
    fn load_block(
        &mut self,
        docs: &[DocId],
        values: &mut Vec<u64>,
        docids: &mut Vec<DocId>,
        row_ids: &mut Vec<RowId>,
    ) -> Cardinality;

    /// Returns a source with the same docs, and the values converted to `f64`.
    ///
    /// Only changes `U64` and `I64` sources: the other types are returned unchanged. `u64` and
    /// `i64` values above 2^53 are rounded.
    ///
    /// Used by the metric aggregations, which collect `f64` values anyway, so that a `missing`
    /// value the column type cannot represent (e.g. `-1` for a `u64` column) is not corrupted.
    fn to_f64(self: Box<Self>) -> Box<dyn ValueSource>;

    /// Returns the physical column, if this source is backed by one.
    fn as_physical_column(&self) -> Option<&Column<u64>> {
        None
    }

    /// Global value bounds, for fast paths that need to size or clamp something up front.
    fn bounds(&self) -> Option<(u64, u64)> {
        let column = self.as_physical_column()?;
        Some((column.min_value(), column.max_value()))
    }

    /// Returns the physical text column, if this source is backed by one.
    ///
    /// For the paths that need the full sstable dictionary (regex search, streaming all of the
    /// terms) rather than resolving ords. `Some` implies that `term_dictionary()` is `Some` and
    /// that its ords are sorted with the terms.
    fn as_physical_str_column(&self) -> Option<&StrColumn> {
        None
    }

    /// Dictionary resolving the term ords returned by `load_block`, for `Str` sources.
    ///
    /// Contract: every `Str` source must return `Some`, even if it has no values (it then returns
    /// an empty dictionary). Non-`Str` sources return `None`.
    ///
    /// Contract: the ords returned by earlier `load_block` calls remain valid. The dictionary
    /// may grow as more blocks are loaded.
    fn term_dictionary(&self) -> Option<&dyn ValueSourceDictionary> {
        None
    }
}

/// Maps term ords of a `Str` [`ValueSource`] back into terms.
///
/// For regular dictionary encoded columns, these term ords is the position
/// of the term in the list of lexicographically sorted terms, and the dictionary itself
/// is implemented using a sstable.
///
/// For more dynamic ValueSource, the mapping from term is typically built dynamically.
pub trait ValueSourceDictionary {
    /// Number of terms in the dictionary. Valid ords are `0..num_terms()`.
    /// For dynamic columns, this number can vary with the number of encounterred
    /// terms.
    fn num_terms(&self) -> u64;

    /// Calls `callback` with the term associated with each ord, in the order of `sorted_ords`.
    ///
    /// Precondition: `sorted_ords` is sorted in ascending order.
    ///
    /// Returns false if an ord was not found in the dictionary.
    fn sorted_ords_to_term_cb(
        &self,
        sorted_ords: &[u64],
        callback: &mut dyn FnMut(&[u8]),
    ) -> io::Result<bool>;
}

impl ValueSourceDictionary for Dictionary {
    fn num_terms(&self) -> u64 {
        Dictionary::num_terms(self) as u64
    }

    fn sorted_ords_to_term_cb(
        &self,
        sorted_ords: &[u64],
        callback: &mut dyn FnMut(&[u8]),
    ) -> io::Result<bool> {
        Dictionary::sorted_ords_to_term_cb(self, sorted_ords, callback)
    }
}

// Lenient columns have erased their logical type; the tuple retains it alongside the values.
//
// WARNING! This does not work as expected for ColumnType Str (because we lack the dictionary)
impl<ColumnRef: Borrow<Column<u64>> + std::fmt::Debug> ValueSource for (ColumnRef, ColumnType) {
    #[inline]
    fn column_type(&self) -> ColumnType {
        self.1
    }

    #[inline]
    fn load_block(
        &mut self,
        docs: &[DocId],
        values: &mut Vec<u64>,
        docids: &mut Vec<DocId>,
        row_ids: &mut Vec<RowId>,
    ) -> Cardinality {
        let column = self.0.borrow();
        let cardinality = column.index.get_cardinality();
        if cardinality.is_full() {
            load_full_column_values(docs, &*column.values, values);
        } else {
            docids.clear();
            row_ids.clear();
            column.row_ids_for_docs(docs, docids, row_ids);
            values.resize(row_ids.len(), 0u64);
            column.values.get_vals(row_ids, values);
        }
        cardinality
    }

    fn to_f64(self: Box<Self>) -> Box<dyn ValueSource> {
        // We clone the column (cheap: `Arc`s), as `ColumnRef` may be a reference.
        let (column_ref, column_type) = *self;
        let column: Column<u64> = column_ref.borrow().clone();
        coerce_to_f64(Box::new((column, column_type)))
    }

    #[inline]
    fn as_physical_column(&self) -> Option<&Column<u64>> {
        Some(self.0.borrow())
    }
}

// A physical text column: the values are the term ords of its dictionary.
impl ValueSource for StrColumn {
    #[inline]
    fn column_type(&self) -> ColumnType {
        ColumnType::Str
    }

    #[inline]
    fn load_block(
        &mut self,
        docs: &[DocId],
        values: &mut Vec<u64>,
        docids: &mut Vec<DocId>,
        row_ids: &mut Vec<RowId>,
    ) -> Cardinality {
        (self.ords(), ColumnType::Str).load_block(docs, values, docids, row_ids)
    }

    fn to_f64(self: Box<Self>) -> Box<dyn ValueSource> {
        self
    }

    #[inline]
    fn as_physical_column(&self) -> Option<&Column<u64>> {
        Some(self.ords())
    }

    fn as_physical_str_column(&self) -> Option<&StrColumn> {
        Some(self)
    }

    fn term_dictionary(&self) -> Option<&dyn ValueSourceDictionary> {
        Some(self.dictionary())
    }
}

/// Wraps a `U64` or `I64` source in an `F64CoercedValueSource`. Returns the other sources
/// unchanged.
///
/// For the `ValueSource::to_f64` implementations that have no cheaper way to produce `f64`.
pub(crate) fn coerce_to_f64(source: Box<dyn ValueSource>) -> Box<dyn ValueSource> {
    let inner_type = source.column_type();
    if !matches!(inner_type, ColumnType::U64 | ColumnType::I64) {
        return source;
    }
    Box::new(F64CoercedValueSource {
        inner: source,
        inner_type,
    })
}

/// A `U64` or `I64` source, with its values converted to `f64`. See `ValueSource::to_f64`.
///
/// The values are converted in place after each `load_block` of the inner source. This keeps the
/// batched path of the inner source, for one more virtual call per block. (Mapping the column
/// values with `monotonic_map_column` would add a virtual call per value instead.)
#[derive(Debug)]
pub(crate) struct F64CoercedValueSource {
    inner: Box<dyn ValueSource>,
    /// Invariant: `U64` or `I64`, the column type of `inner`.
    inner_type: ColumnType,
}

impl ValueSource for F64CoercedValueSource {
    fn column_type(&self) -> ColumnType {
        ColumnType::F64
    }

    fn load_block(
        &mut self,
        docs: &[DocId],
        values: &mut Vec<u64>,
        docids: &mut Vec<DocId>,
        row_ids: &mut Vec<RowId>,
    ) -> Cardinality {
        let cardinality = self.inner.load_block(docs, values, docids, row_ids);
        let inner_type = self.inner_type;
        for value in values.iter_mut() {
            *value = f64_from_fastfield_u64(*value, inner_type).to_u64();
        }
        cardinality
    }

    fn to_f64(self: Box<Self>) -> Box<dyn ValueSource> {
        self
    }
}

/// `docs` has to be sorted ascending and free of duplicates.
#[inline]
fn load_full_column_values(
    docs: &[DocId],
    column_values: &dyn ColumnValues<u64>,
    values: &mut Vec<u64>,
) {
    // Skip the resize when already the right length (common case: fixed-size blocks).
    if values.len() != docs.len() {
        values.resize(docs.len(), 0u64);
    }
    // When the docs form a contiguous ascending run we can fetch the values as a single range.
    // This lets codecs (e.g. bitpacked) bulk-decode the slice instead of gathering value-by-value.
    if is_contiguous(docs) {
        column_values.get_range(docs[0] as u64, values);
    } else {
        column_values.get_vals(docs, values);
    }
}

/// Returns true if `docs` is a contiguous ascending run `[d, d + 1, ..., d + n - 1]`.
///
/// `docs` has to be sorted ascending and free of duplicates.
#[inline]
fn is_contiguous(docs: &[u32]) -> bool {
    let (Some(&first), Some(&last)) = (docs.first(), docs.last()) else {
        return false;
    };
    debug_assert!(
        docs.windows(2).all(|w| w[0] < w[1]),
        "fetch_block requires docs sorted ascending without duplicates"
    );
    (last - first) as usize + 1 == docs.len()
}
