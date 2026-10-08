mod block_accessor;
#[cfg(feature = "jitexpr")]
mod jitexpr_value_source;
mod value_source_registry;

#[cfg(test)]
pub(crate) mod tests;

use std::borrow::Borrow;
use std::io;

pub(crate) use block_accessor::ColumnBlockAccessor;
use columnar::{Cardinality, Column, ColumnType, ColumnValues, Dictionary, RowId, StrColumn};
#[cfg(feature = "jitexpr")]
pub use jitexpr_value_source::JitExprValueSourceProvider;
pub use value_source_registry::{ValueSourceProvider, ValueSourceRegistry};

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

    /// Returns the physical column, if this source is backed by one.
    fn as_column(&self) -> Option<&Column<u64>> {
        None
    }

    /// Global value bounds, for fast paths that need to size or clamp something up front.
    fn bounds(&self) -> Option<(u64, u64)> {
        let column = self.as_column()?;
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
    /// Returns true if the order of the ords matches the lexicographic order of the terms.
    ///
    /// Aggregations relying on that property (e.g. a terms aggregation ordered by `_key`) must
    /// check it.
    fn ords_sorted_with_terms(&self) -> bool;

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
    fn ords_sorted_with_terms(&self) -> bool {
        true
    }

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

    #[inline]
    fn as_column(&self) -> Option<&Column<u64>> {
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

    #[inline]
    fn as_column(&self) -> Option<&Column<u64>> {
        Some(self.ords())
    }

    fn as_physical_str_column(&self) -> Option<&StrColumn> {
        Some(self)
    }

    fn term_dictionary(&self) -> Option<&dyn ValueSourceDictionary> {
        Some(self.dictionary())
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
