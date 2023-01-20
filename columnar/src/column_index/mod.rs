mod multivalued_index;
mod optional_index;
mod serialize;

use std::ops::Range;
use std::sync::Arc;

pub use optional_index::{OptionalIndex, SerializableOptionalIndex, Set};
pub use serialize::{open_column_index, serialize_column_index, SerializableColumnIndex};

use crate::column_values::ColumnValues;
use crate::{Cardinality, RowId};

#[derive(Clone)]
pub enum ColumnIndex<'a> {
    Full,
    Optional(OptionalIndex),
    // TODO Remove the static by fixing the codec if possible.
    /// The column values enclosed contains for all row_id,
    /// the value start_index.
    ///
    /// In addition, at index num_rows, an extra value is added
    /// containing the overal number of values.
    Multivalued(Arc<dyn ColumnValues<RowId> + 'a>),
}

impl<'a> ColumnIndex<'a> {
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
            ColumnIndex::Multivalued(multivalued_index) => {
                let multivalued_index_ref = &**multivalued_index;
                let start: u32 = multivalued_index_ref.get_val(row_id);
                let end: u32 = multivalued_index_ref.get_val(row_id + 1);
                start..end
            }
        }
    }
}
