use common::HasLen;
use common::file_slice::FileSlice;

use crate::Cardinality;
use crate::column_type_header::ColumnType;


pub struct ColumnHandle {
    column_name: String, //< Mostly for debug and display.
    data: FileSlice,
    column_type: ColumnType,
    cardinality: Cardinality,
}

impl ColumnHandle {
    pub fn new(column_name: String, data: FileSlice, column_type: ColumnType, cardinality: Cardinality) -> Self {
        ColumnHandle {
            column_name,
            data,
            column_type,
            cardinality,
        }
    }

    pub fn column_name(&self) -> &str {
        self.column_name.as_str()
    }

    pub fn num_bytes(&self) -> usize {
        self.data.len()
    }

    pub fn column_type(&self) -> ColumnType {
        self.column_type
    }

    pub fn cardinality(&self) -> Cardinality {
        self.cardinality
    }
}


