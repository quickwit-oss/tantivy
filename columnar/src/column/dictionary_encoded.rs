use std::ops::Deref;
use std::sync::Arc;
use std::{fmt, io};

use sstable::{Dictionary, VoidSSTable};

use crate::column::Column;
use crate::{ColumnIndex, RowId};

/// Dictionary encoded column.
///
/// The column simply gives access to a regular u64-column that, in
/// which the values are term-ordinals.
///
/// These ordinals are ids uniquely identify the bytes that are stored in
/// the column. These ordinals are small, and sorted in the same order
/// as the term_ord_column.
#[derive(Clone)]
pub struct BytesColumn {
    pub(crate) dictionary: Arc<Dictionary<VoidSSTable>>,
    pub(crate) term_ord_column: Column<u64>,
}

impl fmt::Debug for BytesColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BytesColumn")
            .field("term_ord_column", &self.term_ord_column)
            .finish()
    }
}

impl BytesColumn {
    pub fn empty(num_docs: u32) -> BytesColumn {
        BytesColumn {
            dictionary: Arc::new(Dictionary::empty()),
            term_ord_column: Column::build_empty_column(num_docs),
        }
    }

    /// Fills the given `output` buffer with the term associated to the ordinal `ord`.
    ///
    /// Returns `false` if the term does not exist (e.g. `term_ord` is greater or equal to the
    /// overll number of terms).
    pub fn ord_to_bytes(&self, ord: u64, output: &mut Vec<u8>) -> io::Result<bool> {
        self.dictionary.ord_to_term(ord, output)
    }

    /// Returns the number of rows in the column.
    pub fn num_rows(&self) -> RowId {
        self.term_ord_column.num_docs()
    }

    pub fn term_ords(&self, row_id: RowId) -> impl Iterator<Item = u64> + '_ {
        self.term_ord_column.values_for_doc(row_id)
    }

    /// Returns the column of ordinals
    pub fn ords(&self) -> &Column<u64> {
        &self.term_ord_column
    }

    pub(crate) fn into_column_index(self) -> ColumnIndex {
        self.term_ord_column.index
    }

    pub fn num_terms(&self) -> usize {
        self.dictionary.num_terms()
    }

    pub fn dictionary(&self) -> &Dictionary<VoidSSTable> {
        self.dictionary.as_ref()
    }
}

#[derive(Clone)]
pub struct StrColumn(BytesColumn);

impl fmt::Debug for StrColumn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.term_ord_column)
    }
}

impl From<StrColumn> for BytesColumn {
    fn from(str_column: StrColumn) -> BytesColumn {
        str_column.0
    }
}

impl StrColumn {
    pub(crate) fn into_column_index(self) -> ColumnIndex {
        self.0.into_column_index()
    }

    pub fn wrap(bytes_column: BytesColumn) -> StrColumn {
        StrColumn(bytes_column)
    }

    pub fn dictionary(&self) -> &Dictionary<VoidSSTable> {
        self.0.dictionary.as_ref()
    }

    /// Splits the column into its dictionary and its column of term ords.
    pub fn into_parts(self) -> (Arc<Dictionary<VoidSSTable>>, Column<u64>) {
        (self.0.dictionary, self.0.term_ord_column)
    }

    /// Fills the buffer with the term associated with `term_ord`.
    ///
    /// Returns `Ok(false)` if the term ordinal is out of bounds.
    /// If the term is not found or on error, `output` is left empty.
    pub fn ord_to_str(&self, term_ord: u64, output: &mut String) -> io::Result<bool> {
        // We take the buffer out of `output` to reuse its allocation.
        // `ord_to_term` may leave arbitrary bytes in it when it does not find the term
        // or returns an error, so it only goes back into `output` after UTF-8 validation.
        let mut buffer: Vec<u8> = std::mem::take(output).into_bytes();
        match self.0.dictionary.ord_to_term(term_ord, &mut buffer) {
            Ok(true) => {}
            Ok(false) => {
                *output = into_empty_string(buffer);
                return Ok(false);
            }
            Err(io_error) => {
                *output = into_empty_string(buffer);
                return Err(io_error);
            }
        }
        match String::from_utf8(buffer) {
            Ok(term_str) => {
                *output = term_str;
                Ok(true)
            }
            Err(utf8_error) => {
                *output = into_empty_string(utf8_error.into_bytes());
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Not valid utf-8",
                ))
            }
        }
    }
}

/// Turns a buffer back into an empty `String`, keeping its allocation.
#[inline(always)]
fn into_empty_string(mut buffer: Vec<u8>) -> String {
    buffer.clear();
    // An empty buffer is always valid UTF-8.
    String::from_utf8(buffer).unwrap_or_default()
}

impl Deref for StrColumn {
    type Target = BytesColumn;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnarReader, ColumnarWriter, DynamicColumn};

    fn invalid_utf8_str_column() -> StrColumn {
        let mut buffer = Vec::new();
        let mut columnar_writer = ColumnarWriter::default();
        columnar_writer.record_bytes(0, "bytes", b"\xff");
        columnar_writer.serialize(1, None, &mut buffer).unwrap();
        let columnar_reader = ColumnarReader::open(buffer).unwrap();
        let column_handles = columnar_reader.read_columns("bytes").unwrap();
        let DynamicColumn::Bytes(bytes_column) = column_handles[0].open().unwrap() else {
            panic!("expected a bytes column");
        };
        StrColumn::wrap(bytes_column)
    }

    #[test]
    fn test_ord_to_str_invalid_utf8() {
        let str_column = invalid_utf8_str_column();
        let mut output = String::from("previous");
        let error = str_column.ord_to_str(0, &mut output).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(output.is_empty());
    }

    #[test]
    fn test_ord_to_str_out_of_bounds_leaves_valid_utf8() {
        let str_column = invalid_utf8_str_column();
        let mut output = String::from("previous");
        // Out of bounds: `ord_to_term` decodes the invalid term before returning `Ok(false)`.
        assert!(!str_column.ord_to_str(1, &mut output).unwrap());
        assert!(output.is_empty());
        assert!(std::str::from_utf8(output.as_bytes()).is_ok());
    }
}
