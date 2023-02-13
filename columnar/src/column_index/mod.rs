mod merge;
mod multivalued_index;
mod optional_index;
mod serialize;

use std::ops::Range;

pub use merge::merge_column_index;
pub use optional_index::{OptionalIndex, Set};
pub use serialize::{open_column_index, serialize_column_index, SerializableColumnIndex};

use crate::column_index::multivalued_index::MultiValueIndex;
use crate::{Cardinality, DocId, RowId};

#[derive(Clone)]
pub enum ColumnIndex {
    Full,
    Optional(OptionalIndex),
    /// In addition, at index num_rows, an extra value is added
    /// containing the overal number of values.
    Multivalued(MultiValueIndex),
}

impl From<OptionalIndex> for ColumnIndex {
    fn from(optional_index: OptionalIndex) -> ColumnIndex {
        ColumnIndex::Optional(optional_index)
    }
}

impl From<MultiValueIndex> for ColumnIndex {
    fn from(multi_value_index: MultiValueIndex) -> ColumnIndex {
        ColumnIndex::Multivalued(multi_value_index)
    }
}

impl ColumnIndex {
    pub fn get_cardinality(&self) -> Cardinality {
        match self {
            ColumnIndex::Full => Cardinality::Full,
            ColumnIndex::Optional(_) => Cardinality::Optional,
            ColumnIndex::Multivalued(_) => Cardinality::Multivalued,
        }
    }

    /// Returns true if and only if there are at least one value associated to the row.
    pub fn has_value(&self, doc_id: DocId) -> bool {
        match self {
            ColumnIndex::Full => true,
            ColumnIndex::Optional(optional_index) => optional_index.contains(doc_id),
            ColumnIndex::Multivalued(multivalued_index) => {
                multivalued_index.range(doc_id).len() > 0
            }
        }
    }

    pub fn value_row_ids(&self, doc_id: DocId) -> Range<RowId> {
        match self {
            ColumnIndex::Full => doc_id..doc_id + 1,
            ColumnIndex::Optional(optional_index) => {
                if let Some(val) = optional_index.rank_if_exists(doc_id) {
                    val..val + 1
                } else {
                    0..0
                }
            }
            ColumnIndex::Multivalued(multivalued_index) => multivalued_index.range(doc_id),
        }
    }

    pub fn docid_range_to_rowids(&self, doc_id: Range<DocId>) -> Range<RowId> {
        match self {
            ColumnIndex::Full => doc_id,
            ColumnIndex::Optional(optional_index) => {
                let row_start = optional_index.rank(doc_id.start);
                let row_end = optional_index.rank(doc_id.end);
                row_start..row_end
            }
            ColumnIndex::Multivalued(multivalued_index) => {
                let end_docid = doc_id.end.min(multivalued_index.num_docs() - 1) + 1;
                let start_docid = doc_id.start.min(end_docid);

                let row_start = multivalued_index.start_index_column.get_val(start_docid);
                let row_end = multivalued_index.start_index_column.get_val(end_docid);

                row_start..row_end
            }
        }
    }

    pub fn select_batch_in_place(&self, rank_ids: &mut Vec<RowId>, doc_id_start: DocId) {
        match self {
            ColumnIndex::Full => {
                // No need to do anything:
                // value_idx and row_idx are the same.
            }
            ColumnIndex::Optional(optional_index) => {
                optional_index.select_batch(&mut rank_ids[..]);
            }
            ColumnIndex::Multivalued(multivalued_index) => {
                multivalued_index.select_batch_in_place(doc_id_start, rank_ids)
            }
        }
    }
}
