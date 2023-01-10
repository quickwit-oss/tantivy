mod dictionary_encoded;
mod serialize;

use std::ops::Deref;
use std::sync::Arc;

use common::BinarySerializable;
pub use dictionary_encoded::BytesColumn;
pub use serialize::{open_column_bytes, open_column_u64, serialize_column_u64};

use crate::column_index::ColumnIndex;
use crate::column_values::ColumnValues;
use crate::{Cardinality, RowId};

#[derive(Clone)]
pub struct Column<T> {
    pub idx: ColumnIndex<'static>,
    pub values: Arc<dyn ColumnValues<T>>,
}

use crate::column_index::Set;

impl<T: PartialOrd> Column<T> {
    pub fn first(&self, row_id: RowId) -> Option<T> {
        match &self.idx {
            ColumnIndex::Full => Some(self.values.get_val(row_id)),
            ColumnIndex::Optional(opt_idx) => {
                let value_row_idx = opt_idx.rank_if_exists(row_id)?;
                Some(self.values.get_val(value_row_idx))
            }
            ColumnIndex::Multivalued(_multivalued_index) => {
                todo!();
            }
        }
    }
}

impl<T> Deref for Column<T> {
    type Target = ColumnIndex<'static>;

    fn deref(&self) -> &Self::Target {
        &self.idx
    }
}

impl BinarySerializable for Cardinality {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.to_code().serialize(writer)
    }

    fn deserialize<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        let cardinality_code = u8::deserialize(reader)?;
        let cardinality = Cardinality::try_from_code(cardinality_code)?;
        Ok(cardinality)
    }
}
