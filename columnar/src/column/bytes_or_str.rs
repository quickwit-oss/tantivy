use std::fmt;

use super::{
    DictionaryEncodedBytesColumn, DictionaryEncodedStrColumn, PlainBytesColumn, PlainStrColumn,
};
use crate::{Cardinality, ColumnIndex, PayloadEncoding, RowId};

/// A byte column, independently of its payload encoding.
#[derive(Clone)]
pub enum BytesColumn {
    /// Values are represented by ordinals into a sorted dictionary.
    DictionaryEncoded(DictionaryEncodedBytesColumn),
    /// Values are stored directly.
    Plain(PlainBytesColumn),
}

impl fmt::Debug for BytesColumn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BytesColumn::DictionaryEncoded(column) => {
                f.debug_tuple("DictionaryEncoded").field(column).finish()
            }
            BytesColumn::Plain(column) => f.debug_tuple("Plain").field(column).finish(),
        }
    }
}

impl BytesColumn {
    /// Returns the number of rows in the column.
    pub fn num_rows(&self) -> RowId {
        match self {
            BytesColumn::DictionaryEncoded(column) => column.num_rows(),
            BytesColumn::Plain(column) => column.num_rows(),
        }
    }

    /// Returns the number of values in the column.
    pub fn num_values(&self) -> u32 {
        match self {
            BytesColumn::DictionaryEncoded(column) => column.num_values(),
            BytesColumn::Plain(column) => column.num_values(),
        }
    }

    /// Returns the cardinality of the column.
    pub fn get_cardinality(&self) -> Cardinality {
        self.column_index().get_cardinality()
    }

    /// Returns the column index mapping rows to physical values.
    pub fn column_index(&self) -> &ColumnIndex {
        match self {
            BytesColumn::DictionaryEncoded(column) => column.column_index(),
            BytesColumn::Plain(column) => column.column_index(),
        }
    }

    /// Returns the payload encoding used by this column.
    pub fn payload_encoding(&self) -> PayloadEncoding {
        match self {
            BytesColumn::DictionaryEncoded(_) => PayloadEncoding::Dictionary,
            BytesColumn::Plain(_) => PayloadEncoding::Plain,
        }
    }

    /// Returns the dictionary-encoded column, if this column uses dictionary encoding.
    pub fn as_dictionary_encoded(&self) -> Option<&DictionaryEncodedBytesColumn> {
        match self {
            BytesColumn::DictionaryEncoded(column) => Some(column),
            BytesColumn::Plain(_) => None,
        }
    }

    /// Returns the plain column, if this column uses plain encoding.
    pub fn as_plain(&self) -> Option<&PlainBytesColumn> {
        match self {
            BytesColumn::DictionaryEncoded(_) => None,
            BytesColumn::Plain(column) => Some(column),
        }
    }
}

impl From<DictionaryEncodedBytesColumn> for BytesColumn {
    fn from(column: DictionaryEncodedBytesColumn) -> Self {
        BytesColumn::DictionaryEncoded(column)
    }
}

impl From<PlainBytesColumn> for BytesColumn {
    fn from(column: PlainBytesColumn) -> Self {
        BytesColumn::Plain(column)
    }
}

/// A string column, independently of its payload encoding.
#[derive(Clone)]
pub enum StrColumn {
    /// Values are represented by ordinals into a sorted dictionary.
    DictionaryEncoded(DictionaryEncodedStrColumn),
    /// Values are stored directly.
    Plain(PlainStrColumn),
}

impl fmt::Debug for StrColumn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StrColumn::DictionaryEncoded(column) => {
                f.debug_tuple("DictionaryEncoded").field(column).finish()
            }
            StrColumn::Plain(column) => f.debug_tuple("Plain").field(column).finish(),
        }
    }
}

impl StrColumn {
    /// Returns the number of rows in the column.
    pub fn num_rows(&self) -> RowId {
        match self {
            StrColumn::DictionaryEncoded(column) => column.num_rows(),
            StrColumn::Plain(column) => column.as_bytes().num_rows(),
        }
    }

    /// Returns the number of values in the column.
    pub fn num_values(&self) -> u32 {
        match self {
            StrColumn::DictionaryEncoded(column) => column.num_values(),
            StrColumn::Plain(column) => column.as_bytes().num_values(),
        }
    }

    /// Returns the cardinality of the column.
    pub fn get_cardinality(&self) -> Cardinality {
        self.column_index().get_cardinality()
    }

    /// Returns the column index mapping rows to physical values.
    pub fn column_index(&self) -> &ColumnIndex {
        match self {
            StrColumn::DictionaryEncoded(column) => column.column_index(),
            StrColumn::Plain(column) => column.as_bytes().column_index(),
        }
    }

    /// Returns the payload encoding used by this column.
    pub fn payload_encoding(&self) -> PayloadEncoding {
        match self {
            StrColumn::DictionaryEncoded(_) => PayloadEncoding::Dictionary,
            StrColumn::Plain(_) => PayloadEncoding::Plain,
        }
    }

    /// Returns the dictionary-encoded column, if this column uses dictionary encoding.
    pub fn as_dictionary_encoded(&self) -> Option<&DictionaryEncodedStrColumn> {
        match self {
            StrColumn::DictionaryEncoded(column) => Some(column),
            StrColumn::Plain(_) => None,
        }
    }

    /// Returns the plain column, if this column uses plain encoding.
    pub fn as_plain(&self) -> Option<&PlainStrColumn> {
        match self {
            StrColumn::DictionaryEncoded(_) => None,
            StrColumn::Plain(column) => Some(column),
        }
    }
}

impl From<DictionaryEncodedStrColumn> for StrColumn {
    fn from(column: DictionaryEncodedStrColumn) -> Self {
        StrColumn::DictionaryEncoded(column)
    }
}

impl From<PlainStrColumn> for StrColumn {
    fn from(column: PlainStrColumn) -> Self {
        StrColumn::Plain(column)
    }
}

impl From<StrColumn> for BytesColumn {
    fn from(column: StrColumn) -> Self {
        match column {
            StrColumn::DictionaryEncoded(column) => BytesColumn::DictionaryEncoded(column.into()),
            StrColumn::Plain(column) => BytesColumn::Plain(column.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dictionary_encoded_column_metadata_and_downcasts() {
        let bytes_column: BytesColumn = DictionaryEncodedBytesColumn::empty(3).into();
        assert_eq!(bytes_column.payload_encoding(), PayloadEncoding::Dictionary);
        assert_eq!(bytes_column.num_rows(), 3);
        assert_eq!(bytes_column.num_values(), 0);
        assert_eq!(bytes_column.get_cardinality(), Cardinality::Optional);
        assert!(bytes_column.as_dictionary_encoded().is_some());
        assert!(bytes_column.as_plain().is_none());

        let str_column: StrColumn =
            DictionaryEncodedStrColumn::wrap(DictionaryEncodedBytesColumn::empty(3)).into();
        assert_eq!(str_column.payload_encoding(), PayloadEncoding::Dictionary);
        assert_eq!(str_column.num_rows(), 3);
        assert_eq!(str_column.num_values(), 0);
        assert!(str_column.as_dictionary_encoded().is_some());
        assert!(str_column.as_plain().is_none());
    }

    #[test]
    fn test_plain_column_metadata_and_downcasts() {
        let plain_bytes = PlainBytesColumn::for_test(ColumnIndex::Full, &[b"first", b"second"]);
        let bytes_column = BytesColumn::Plain(plain_bytes.clone());
        assert_eq!(bytes_column.payload_encoding(), PayloadEncoding::Plain);
        assert_eq!(bytes_column.num_rows(), 2);
        assert_eq!(bytes_column.num_values(), 2);
        assert_eq!(bytes_column.get_cardinality(), Cardinality::Full);
        assert!(bytes_column.as_dictionary_encoded().is_none());
        assert!(bytes_column.as_plain().is_some());

        let str_column = StrColumn::Plain(PlainStrColumn::wrap(plain_bytes));
        assert_eq!(str_column.payload_encoding(), PayloadEncoding::Plain);
        assert_eq!(str_column.num_rows(), 2);
        assert_eq!(str_column.num_values(), 2);
        assert!(str_column.as_dictionary_encoded().is_none());
        assert!(str_column.as_plain().is_some());

        let bytes_from_str: BytesColumn = str_column.into();
        assert_eq!(bytes_from_str.payload_encoding(), PayloadEncoding::Plain);
    }
}
