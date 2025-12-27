mod dictionary_encoded;
mod serialize;

use std::fmt::{self, Debug};
use std::io::Write;
use std::ops::{Range, RangeInclusive};
use std::sync::Arc;

use common::BinarySerializable;
pub use dictionary_encoded::{BytesColumn, StrColumn};
pub use serialize::{
    open_column_bytes, open_column_str, open_column_u64, open_column_u128,
    open_column_u128_as_compact_u64, serialize_column_mappable_to_u64,
    serialize_column_mappable_to_u128,
};

use crate::column_index::{ColumnIndex, Set};
use crate::column_values::monotonic_mapping::StrictlyMonotonicMappingToInternal;
use crate::column_values::{ColumnValues, monotonic_map_column};
use crate::{Cardinality, DocId, EmptyColumnValues, MonotonicallyMappableToU64, RowId};

#[derive(Clone)]
pub struct Column<T = u64> {
    pub index: ColumnIndex,
    pub values: Arc<dyn ColumnValues<T>>,
}

impl<T: Debug + PartialOrd + Send + Sync + Copy + 'static> Debug for Column<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let num_docs = self.num_docs();
        let entries = (0..num_docs)
            .map(|i| (i, self.values_for_doc(i).collect::<Vec<_>>()))
            .filter(|(_, vals)| !vals.is_empty());
        f.debug_map().entries(entries).finish()
    }
}

impl<T: PartialOrd + Default> Column<T> {
    pub fn build_empty_column(num_docs: u32) -> Column<T> {
        Column {
            index: ColumnIndex::Empty { num_docs },
            values: Arc::new(EmptyColumnValues),
        }
    }
}

impl<T: MonotonicallyMappableToU64> Column<T> {
    pub fn to_u64_monotonic(self) -> Column<u64> {
        let values = Arc::new(monotonic_map_column(
            self.values,
            StrictlyMonotonicMappingToInternal::<T>::new(),
        ));
        Column {
            index: self.index,
            values,
        }
    }
}

impl<T: PartialOrd + Copy + Debug + Send + Sync + 'static> Column<T> {
    #[inline]
    pub fn get_cardinality(&self) -> Cardinality {
        self.index.get_cardinality()
    }

    pub fn num_docs(&self) -> RowId {
        match &self.index {
            ColumnIndex::Empty { num_docs } => *num_docs,
            ColumnIndex::Full => self.values.num_vals(),
            ColumnIndex::Optional(optional_index) => optional_index.num_docs(),
            ColumnIndex::Multivalued(col_index) => {
                // The multivalued index contains all value start row_id,
                // and one extra value at the end with the overall number of rows.
                col_index.num_docs()
            }
        }
    }

    pub fn min_value(&self) -> T {
        self.values.min_value()
    }

    pub fn max_value(&self) -> T {
        self.values.max_value()
    }

    #[inline]
    pub fn first(&self, row_id: RowId) -> Option<T> {
        self.values_for_doc(row_id).next()
    }

    /// Translates a block of docids to row_ids.
    ///
    /// returns the row_ids and the matching docids on the same index
    /// e.g.
    /// DocId In:  [0, 5, 6]
    /// DocId Out: [0, 0, 6, 6]
    /// RowId Out: [0, 1, 2, 3]
    #[inline]
    pub fn row_ids_for_docs(
        &self,
        doc_ids: &[DocId],
        doc_ids_out: &mut Vec<DocId>,
        row_ids: &mut Vec<RowId>,
    ) {
        self.index.docids_to_rowids(doc_ids, doc_ids_out, row_ids)
    }

    /// Get an iterator over the values for the provided docid.
    #[inline]
    pub fn values_for_doc(&self, doc_id: DocId) -> impl Iterator<Item = T> + '_ {
        self.index
            .value_row_ids(doc_id)
            .map(|value_row_id: RowId| self.values.get_val(value_row_id))
    }

    /// Get the docids of values which are in the provided value and docid range.
    #[inline]
    pub fn get_docids_for_value_range(
        &self,
        value_range: ValueRange<T>,
        selected_docid_range: Range<u32>,
        doc_ids: &mut Vec<u32>,
    ) {
        // convert passed docid range to row id range
        let rowid_range = self
            .index
            .docid_range_to_rowids(selected_docid_range.clone());

        // Load rows
        self.values
            .get_row_ids_for_value_range(value_range, rowid_range, doc_ids);
        // Convert rows to docids
        self.index
            .select_batch_in_place(selected_docid_range.start, doc_ids);
    }

    pub fn first_or_default_col(self, default_value: T) -> Arc<dyn ColumnValues<T>> {
        Arc::new(FirstValueWithDefault {
            column: self,
            default_value,
        })
    }
}

// Separate impl block for methods requiring `Default` for `T`.
impl<T: PartialOrd + Copy + Debug + Send + Sync + 'static + Default> Column<T> {
    /// Load the first value for each docid in the provided slice.
    ///
    /// The `docids` vector is mutated: documents that do not match the `value_range` are removed.
    /// The `values` vector is populated with the values of the remaining documents.
    #[inline]
    pub fn first_vals_in_value_range(
        &self,
        docids: &mut Vec<DocId>,
        values: &mut Vec<Option<T>>,
        value_range: ValueRange<T>,
    ) {
        // TODO: Move `COLLECT_BLOCK_BUFFER_LEN` to allow for use here, or use a different constant
        // in this context.
        const BLOCK_LEN: usize = 64; // Corresponds to COLLECT_BLOCK_BUFFER_LEN in tantivy's docset
        match (&self.index, value_range) {
            (ColumnIndex::Empty { .. }, value_range) => {
                let nulls_match = match &value_range {
                    ValueRange::All => true,
                    ValueRange::Inclusive(_) => false,
                    ValueRange::GreaterThan(_, nulls_match) => *nulls_match,
                    ValueRange::LessThan(_, nulls_match) => *nulls_match,
                };
                if nulls_match {
                    for _ in 0..docids.len() {
                        values.push(None);
                    }
                } else {
                    docids.clear();
                }
            }
            (ColumnIndex::Full, value_range) => {
                self.values
                    .get_vals_in_value_range(docids, values, value_range);
            }
            (ColumnIndex::Optional(optional_index), value_range) => {
                let len = docids.len();
                // Ensure the input docids length does not exceed BLOCK_LEN for stack allocation
                // safety. If it does, we might need to handle this with multiple
                // chunks or fallback to heap. For now, an assert is used to confirm
                // expected usage within batch processing limits.
                assert!(
                    len <= BLOCK_LEN,
                    "Input docids length ({}) exceeds BLOCK_LEN ({})",
                    len,
                    BLOCK_LEN
                );

                let mut input_docs_buffer = [0u32; BLOCK_LEN];
                input_docs_buffer[..len].copy_from_slice(docids);

                let mut dense_row_ids_buffer = [0u32; BLOCK_LEN];
                let mut dense_values_buffer = [T::default(); BLOCK_LEN];
                let mut presence_mask: u64 = 0; // Bitmask to track which input_docs have a value
                let mut num_present = 0;

                // Phase 1: Identify existing RowIds and build dense_row_ids_buffer
                for (i, &doc_id) in input_docs_buffer[..len].iter().enumerate() {
                    if let Some(row_id) = optional_index.rank_if_exists(doc_id) {
                        dense_row_ids_buffer[num_present] = row_id;
                        presence_mask |= 1u64 << i; // Set bit for present docid
                        num_present += 1;
                    }
                }

                // Phase 2: Batch fetch values for present docs
                if num_present > 0 {
                    self.values.get_vals(
                        &dense_row_ids_buffer[..num_present],
                        &mut dense_values_buffer[..num_present],
                    );
                }

                // Determine if nulls match the value range
                let nulls_match = match &value_range {
                    ValueRange::All => true,
                    ValueRange::Inclusive(_) => false,
                    ValueRange::GreaterThan(_, nulls_match) => *nulls_match,
                    ValueRange::LessThan(_, nulls_match) => *nulls_match,
                };

                // Phase 3: Filter and merge results, reconstructing docids and values
                docids.clear();
                values.clear();

                let mut dense_values_cursor = 0;
                for i in 0..len {
                    let original_doc_id = input_docs_buffer[i];
                    if (presence_mask & (1u64 << i)) != 0 {
                        // This doc_id was present in the optional index and has a value
                        let val = dense_values_buffer[dense_values_cursor];
                        dense_values_cursor += 1;

                        // Check if the value matches the value range
                        let value_matches = match &value_range {
                            ValueRange::All => true,
                            ValueRange::Inclusive(r) => r.contains(&val),
                            ValueRange::GreaterThan(t, _) => val > *t,
                            ValueRange::LessThan(t, _) => val < *t,
                        };

                        if value_matches {
                            docids.push(original_doc_id);
                            values.push(Some(val));
                        }
                    } else if nulls_match {
                        // This doc_id was not present in the optional index (null) and nulls match
                        docids.push(original_doc_id);
                        values.push(None);
                    }
                }
            }
            (ColumnIndex::Multivalued(multivalued_index), value_range) => {
                let nulls_match = match &value_range {
                    ValueRange::All => true,
                    ValueRange::Inclusive(_) => false,
                    ValueRange::GreaterThan(_, nulls_match) => *nulls_match,
                    ValueRange::LessThan(_, nulls_match) => *nulls_match,
                };
                let mut write_head = 0;
                for i in 0..docids.len() {
                    let docid = docids[i];
                    let row_range = multivalued_index.range(docid);
                    let is_empty = row_range.start == row_range.end;
                    if !is_empty {
                        let val = self.values.get_val(row_range.start);
                        let matches = match &value_range {
                            ValueRange::All => true,
                            ValueRange::Inclusive(r) => r.contains(&val),
                            ValueRange::GreaterThan(t, _) => val > *t,
                            ValueRange::LessThan(t, _) => val < *t,
                        };
                        if matches {
                            docids[write_head] = docid;
                            values.push(Some(val));
                            write_head += 1;
                        }
                    } else if nulls_match {
                        docids[write_head] = docid;
                        values.push(None);
                        write_head += 1;
                    }
                }
                docids.truncate(write_head);
            }
        }
    }
}

/// A range of values.
///
/// This type is intended to be used in batch APIs, where the cost of unpacking the enum
/// is outweighed by the time spent processing a batch.
///
/// Implementers should pattern match on the variants to use optimized loops for each case.
#[derive(Clone, Debug)]
pub enum ValueRange<T> {
    /// A range that includes both start and end.
    Inclusive(RangeInclusive<T>),
    /// A range that matches all values.
    All,
    /// A range that matches all values greater than the threshold.
    /// The boolean flag indicates if null values should be included.
    GreaterThan(T, bool),
    /// A range that matches all values less than the threshold.
    /// The boolean flag indicates if null values should be included.
    LessThan(T, bool),
}

impl BinarySerializable for Cardinality {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> std::io::Result<()> {
        self.to_code().serialize(writer)
    }

    fn deserialize<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        let cardinality_code = u8::deserialize(reader)?;
        let cardinality = Cardinality::try_from_code(cardinality_code)?;
        Ok(cardinality)
    }
}

// TODO simplify or optimize
struct FirstValueWithDefault<T: Copy> {
    column: Column<T>,
    default_value: T,
}

impl<T: PartialOrd + Debug + Send + Sync + Copy + 'static> ColumnValues<T>
    for FirstValueWithDefault<T>
{
    #[inline(always)]
    fn get_val(&self, idx: u32) -> T {
        self.column.first(idx).unwrap_or(self.default_value)
    }

    fn min_value(&self) -> T {
        self.column.values.min_value()
    }

    fn max_value(&self) -> T {
        self.column.values.max_value()
    }

    fn num_vals(&self) -> u32 {
        match &self.column.index {
            ColumnIndex::Empty { .. } => 0u32,
            ColumnIndex::Full => self.column.values.num_vals(),
            ColumnIndex::Optional(optional_idx) => optional_idx.num_docs(),
            ColumnIndex::Multivalued(multivalue_idx) => multivalue_idx.num_docs(),
        }
    }
}
