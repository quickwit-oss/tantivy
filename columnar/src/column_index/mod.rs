mod multivalued_index;
mod optional_index;
mod serialize;

use std::ops::Range;

pub use optional_index::{OptionalIndex, SerializableOptionalIndex, Set};
pub use serialize::{open_column_index, serialize_column_index, SerializableColumnIndex};

use crate::column_index::multivalued_index::MultiValueIndex;
use crate::{Cardinality, RowId};

#[derive(Clone)]
pub enum ColumnIndex {
    Full,
    Optional(OptionalIndex),
    // TODO Remove the static by fixing the codec if possible.
    /// The column values enclosed contains for all row_id,
    /// the value start_index.
    ///
    /// In addition, at index num_rows, an extra value is added
    /// containing the overal number of values.
    Multivalued(MultiValueIndex),
}

impl ColumnIndex {
    pub fn get_cardinality(&self) -> Cardinality {
        match self {
            ColumnIndex::Full => Cardinality::Full,
            ColumnIndex::Optional(_) => Cardinality::Optional,
            ColumnIndex::Multivalued(_) => Cardinality::Multivalued,
        }
    }

    pub fn value_row_ids(&self, row_id: RowId) -> Range<RowId> {
        match self {
            ColumnIndex::Full => row_id..row_id + 1,
            ColumnIndex::Optional(optional_index) => {
                if let Some(val) = optional_index.rank_if_exists(row_id) {
                    val..val + 1
                } else {
                    0..0
                }
            }
            ColumnIndex::Multivalued(multivalued_index) => multivalued_index.range(row_id),
        }
    }

    pub fn select_batch_in_place(&self, rank_ids: &mut Vec<RowId>) {
        match self {
            ColumnIndex::Full => {
                // No need to do anything:
                // value_idx and row_idx are the same.
            }
            ColumnIndex::Optional(optional_index) => {
                optional_index.select_batch(&mut rank_ids[..]);
            }
            ColumnIndex::Multivalued(multivalued_index) => {
                // TODO important: avoid using 0u32, and restart from the beginning all of the time.
                multivalued_index.select_batch_in_place(0u32, rank_ids)
            }
        }
    }
}
