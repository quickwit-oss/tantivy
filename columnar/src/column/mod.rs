mod dictionary_encoded;
mod serialize;

use std::fmt::Debug;
use std::io::Write;
use std::ops::Deref;
use std::sync::Arc;

use common::BinarySerializable;
pub use dictionary_encoded::{BytesColumn, StrColumn};
pub use serialize::{
    open_column_bytes, open_column_str, open_column_u128, open_column_u64,
    serialize_column_mappable_to_u128, serialize_column_mappable_to_u64,
};

use crate::column_index::ColumnIndex;
use crate::column_values::monotonic_mapping::StrictlyMonotonicMappingToInternal;
use crate::column_values::{monotonic_map_column, ColumnValues};
use crate::{Cardinality, MonotonicallyMappableToU64, RowId};

#[derive(Clone)]
pub struct Column<T = u64> {
    pub idx: ColumnIndex,
    pub values: Arc<dyn ColumnValues<T>>,
}

impl<T: MonotonicallyMappableToU64> Column<T> {
    pub fn to_u64_monotonic(self) -> Column<u64> {
        let values = Arc::new(monotonic_map_column(
            self.values,
            StrictlyMonotonicMappingToInternal::<T>::new(),
        ));
        Column {
            idx: self.idx,
            values,
        }
    }
}

impl<T: PartialOrd + Copy + Debug + Send + Sync + 'static> Column<T> {
    pub fn get_cardinality(&self) -> Cardinality {
        self.idx.get_cardinality()
    }

    pub fn num_rows(&self) -> RowId {
        match &self.idx {
            ColumnIndex::Full => self.values.num_vals() as u32,
            ColumnIndex::Optional(optional_index) => optional_index.num_rows(),
            ColumnIndex::Multivalued(col_index) => {
                // The multivalued index contains all value start row_id,
                // and one extra value at the end with the overall number of rows.
                col_index.num_rows()
            }
        }
    }

    pub fn min_value(&self) -> T {
        self.values.min_value()
    }

    pub fn max_value(&self) -> T {
        self.values.max_value()
    }

    pub fn first(&self, row_id: RowId) -> Option<T> {
        self.values(row_id).next()
    }

    pub fn values(&self, row_id: RowId) -> impl Iterator<Item = T> + '_ {
        self.value_row_ids(row_id)
            .map(|value_row_id: RowId| self.values.get_val(value_row_id))
    }

    /// Fils the output vector with the (possibly multiple values that are associated_with
    /// `row_id`.
    ///
    /// This method clears the `output` vector.
    pub fn fill_vals(&self, row_id: RowId, output: &mut Vec<T>) {
        output.clear();
        output.extend(self.values(row_id));
    }

    pub fn first_or_default_col(self, default_value: T) -> Arc<dyn ColumnValues<T>> {
        Arc::new(FirstValueWithDefault {
            column: self,
            default_value,
        })
    }
}

impl<T> Deref for Column<T> {
    type Target = ColumnIndex;

    fn deref(&self) -> &Self::Target {
        &self.idx
    }
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
            ColumnIndex::Multivalued(multivalue_idx) => multivalue_idx.num_rows(),
        }
    }
}
