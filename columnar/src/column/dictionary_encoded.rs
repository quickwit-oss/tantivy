use std::io;
use std::ops::Deref;
use std::sync::Arc;

use sstable::{Dictionary, VoidSSTable};

use crate::column::Column;
use crate::column_index::ColumnIndex;

/// Dictionary encoded column.
#[derive(Clone)]
pub struct BytesColumn {
    pub(crate) dictionary: Arc<Dictionary<VoidSSTable>>,
    pub(crate) term_ord_column: Column<u64>,
}

impl BytesColumn {
    /// Returns `false` if the term does not exist (e.g. `term_ord` is greater or equal to the
    /// overll number of terms).
    pub fn term_ord_to_str(&self, term_ord: u64, output: &mut Vec<u8>) -> io::Result<bool> {
        self.dictionary.ord_to_term(term_ord, output)
    }

    pub fn term_ords(&self) -> &Column<u64> {
        &self.term_ord_column
    }
}

impl Deref for BytesColumn {
    type Target = ColumnIndex<'static>;

    fn deref(&self) -> &Self::Target {
        &**self.term_ords()
    }
}

#[cfg(test)]
mod tests {
    use crate::{ColumnarReader, ColumnarWriter};
}
