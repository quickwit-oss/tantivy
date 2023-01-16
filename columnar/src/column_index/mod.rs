mod multivalued_index;
mod optional_index;
mod serialize;

use std::sync::Arc;

pub use optional_index::{OptionalIndex, SerializableOptionalIndex, Set};
pub use serialize::{open_column_index, serialize_column_index, SerializableColumnIndex};

use crate::column_values::ColumnValues;
use crate::{Cardinality, RowId};

#[derive(Clone)]
pub enum ColumnIndex<'a> {
    Full,
    Optional(OptionalIndex),
    // TODO remove the Arc<dyn> apart from serialization this is not
    // dynamic at all.
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

    pub fn num_rows(&self) -> RowId {
        match self {
            ColumnIndex::Full => {
                todo!()
            }
            ColumnIndex::Optional(optional_index) => optional_index.num_rows(),
            ColumnIndex::Multivalued(multivalued_index) => multivalued_index.num_vals() - 1,
        }
    }
}
