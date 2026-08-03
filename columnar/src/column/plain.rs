use crate::{Cardinality, ColumnIndex, RowId};

/// A byte column whose values are stored directly rather than as dictionary ordinals.
///
/// The encoded payload and value-access implementation are added with the standalone plain-column
/// reader. This type establishes the public reader hierarchy and its encoding-neutral metadata.
#[derive(Clone, Debug)]
pub struct PlainBytesColumn {
    pub(crate) column_index: ColumnIndex,
    pub(crate) num_values: u32,
    // TODO: Add the OnPair16 model, compressed payload, and value offsets when implementing the
    // standalone plain-column reader.
}

impl PlainBytesColumn {
    /// Returns the number of rows in the column.
    pub fn num_rows(&self) -> RowId {
        match &self.column_index {
            ColumnIndex::Empty { num_docs } => *num_docs,
            ColumnIndex::Full => self.num_values,
            ColumnIndex::Optional(optional_index) => optional_index.num_docs(),
            ColumnIndex::Multivalued(multivalued_index) => multivalued_index.num_docs(),
        }
    }

    /// Returns the number of values in the column.
    pub fn num_values(&self) -> u32 {
        self.num_values
    }

    /// Returns the column index mapping rows to physical values.
    pub fn column_index(&self) -> &ColumnIndex {
        &self.column_index
    }

    /// Returns the cardinality of the column.
    pub fn get_cardinality(&self) -> Cardinality {
        self.column_index.get_cardinality()
    }
}

/// UTF-8 view over a [`PlainBytesColumn`].
#[derive(Clone, Debug)]
pub struct PlainStrColumn(PlainBytesColumn);

impl PlainStrColumn {
    /// Wraps a plain byte column as a string column.
    pub fn wrap(bytes_column: PlainBytesColumn) -> Self {
        PlainStrColumn(bytes_column)
    }

    /// Returns the underlying byte column.
    pub fn as_bytes(&self) -> &PlainBytesColumn {
        &self.0
    }
}

impl From<PlainStrColumn> for PlainBytesColumn {
    fn from(str_column: PlainStrColumn) -> Self {
        str_column.0
    }
}
