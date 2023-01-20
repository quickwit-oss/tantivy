mod dictionary_encoded;
mod serialize;

use std::ops::Deref;
use std::sync::Arc;

use common::BinarySerializable;
pub use dictionary_encoded::{BytesColumn, StrColumn};
pub use serialize::{
    open_column_bytes, open_column_u128, open_column_u64, serialize_column_mappable_to_u128,
    serialize_column_mappable_to_u64,
};

use crate::column_index::ColumnIndex;
use crate::column_values::ColumnValues;
use crate::{Cardinality, RowId};

#[derive(Clone)]
pub struct Column<T> {
    pub idx: ColumnIndex<'static>,
    pub values: Arc<dyn ColumnValues<T>>,
}

impl<T: PartialOrd> Column<T> {
    pub fn num_rows(&self) -> RowId {
        match &self.idx {
            ColumnIndex::Full => self.values.num_vals() as u32,
            ColumnIndex::Optional(optional_index) => optional_index.num_rows(),
            ColumnIndex::Multivalued(col_index) => {
                // The multivalued index contains all value start row_id,
                // and one extra value at the end with the overall number of rows.
                col_index.num_vals() - 1
            }
        }
    }

    pub fn min_value(&self) -> T {
        self.values.min_value()
    }
    pub fn max_value(&self) -> T {
        self.values.max_value()
    }
}

impl<T: PartialOrd + Copy + Send + Sync + 'static> Column<T> {
    pub fn first(&self, row_id: RowId) -> Option<T> {
        self.values(row_id).next()
    }

    pub fn values(&self, row_id: RowId) -> impl Iterator<Item = T> + '_ {
        self.value_row_ids(row_id)
            .map(|value_row_id: RowId| self.values.get_val(value_row_id))
    }

    pub fn first_or_default_col(self, default_value: T) -> Arc<dyn ColumnValues<T>> {
        Arc::new(FirstValueWithDefault {
            column: self,
            default_value,
        })
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

// TODO simplify or optimize
struct FirstValueWithDefault<T: Copy> {
    column: Column<T>,
    default_value: T,
}

impl<T: PartialOrd + Send + Sync + Copy + 'static> ColumnValues<T> for FirstValueWithDefault<T> {
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
        match &self.column.idx {
            ColumnIndex::Full => self.column.values.num_vals(),
            ColumnIndex::Optional(optional_idx) => optional_idx.num_rows(),
            ColumnIndex::Multivalued(_) => todo!(),
        }
    }
}
