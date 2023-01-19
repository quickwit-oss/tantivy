use std::io;
use std::ops::Deref;
use std::sync::Arc;

use sstable::{Dictionary, VoidSSTable};

use crate::column::Column;
use crate::RowId;

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

impl BytesColumn {
    /// Fills the given `output` buffer with the term associated to the ordinal `ord`.
    ///
    /// Returns `false` if the term does not exist (e.g. `term_ord` is greater or equal to the
    /// overll number of terms).
    pub fn ord_to_bytes(&self, ord: u64, output: &mut Vec<u8>) -> io::Result<bool> {
        self.dictionary.ord_to_term(ord, output)
    }

    /// Returns the number of rows in the column.
    pub fn num_rows(&self) -> RowId {
        self.term_ord_column.num_rows()
    }

    /// Returns the column of ordinals
    pub fn ords(&self) -> &Column<u64> {
        &self.term_ord_column
    }
}

#[derive(Clone)]
pub struct StrColumn(BytesColumn);

impl From<BytesColumn> for StrColumn {
    fn from(bytes_col: BytesColumn) -> Self {
        StrColumn(bytes_col)
    }
}

impl StrColumn {
    /// Fills the buffer
    pub fn ord_to_str(&self, term_ord: u64, output: &mut String) -> io::Result<bool> {
        unsafe {
            let buf = output.as_mut_vec();
            self.0.dictionary.ord_to_term(term_ord, buf)?;
            // TODO consider remove checks if it hurts performance.
            if std::str::from_utf8(buf.as_slice()).is_err() {
                buf.clear();
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Not valid utf-8",
                ));
            }
        }
        Ok(true)
    }
}

impl Deref for StrColumn {
    type Target = BytesColumn;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
