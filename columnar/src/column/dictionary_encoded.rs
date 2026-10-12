use std::ops::Deref;
use std::sync::Arc;
use std::{fmt, io};

use sstable::{Dictionary, VoidSSTable};

use crate::column::Column;
use crate::{ColumnIndex, RowId};

/// String/byte values stored in a dictionary or in independent compact groups.
///
/// Use `value_range`, `read_value`, and `first` for codec-neutral access.
/// Dictionary-only ordinal methods panic on compact columns; use `ords_opt`
/// and `dictionary_opt` when testing for that capability.
#[derive(Clone)]
pub struct BytesColumn {
    storage: BytesStorage,
}

#[derive(Clone)]
enum BytesStorage {
    Dictionary {
        dictionary: Arc<Dictionary<VoidSSTable>>,
        ords: Column<u64>,
    },
    Compact {
        column: crate::CompactColumn,
        index: ColumnIndex,
    },
}

impl fmt::Debug for BytesColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BytesColumn")
            .field("compact", &self.is_compact())
            .field("num_docs", &self.num_rows())
            .finish()
    }
}

impl BytesColumn {
    /// Builds a dictionary view for algorithms requiring globally sorted term ordinals.
    ///
    /// Dictionary columns are cloned cheaply. Compact columns are fully decoded and
    /// re-encoded in memory. Do not use this adapter for document-at-a-time reads.
    pub fn to_dictionary_encoded(&self) -> io::Result<Self> {
        if !self.is_compact() {
            return Ok(self.clone());
        }
        let mut writer = crate::ColumnarWriter::default();
        writer.record_column_type("value", crate::ColumnType::Bytes, false);
        let mut buffer = Vec::new();
        for doc in 0..self.num_rows() {
            for value in self.value_range(doc)? {
                self.read_value(value, &mut buffer)?;
                writer.record_bytes(doc, "value", &buffer);
            }
        }
        let mut serialized = Vec::new();
        writer.serialize(self.num_rows(), None, &mut serialized)?;
        let reader = crate::ColumnarReader::open(serialized)?;
        let crate::DynamicColumn::Bytes(column) = reader.read_columns("value")?[0].open()? else {
            unreachable!("the adapter writes a bytes column");
        };
        Ok(column)
    }

    pub fn empty(num_docs: u32) -> BytesColumn {
        Self::from_dictionary(
            Arc::new(Dictionary::empty()),
            Column::build_empty_column(num_docs),
        )
    }

    pub(crate) fn from_dictionary(
        dictionary: Arc<Dictionary<VoidSSTable>>,
        ords: Column<u64>,
    ) -> Self {
        Self {
            storage: BytesStorage::Dictionary { dictionary, ords },
        }
    }

    pub(crate) fn from_compact(column: crate::CompactColumn) -> Self {
        let index = ColumnIndex::Grouped(column.clone());
        Self {
            storage: BytesStorage::Compact { column, index },
        }
    }

    pub fn is_compact(&self) -> bool {
        self.compact().is_some()
    }

    pub fn compact(&self) -> Option<&crate::CompactColumn> {
        match &self.storage {
            BytesStorage::Compact { column, .. } => Some(column),
            _ => None,
        }
    }

    /// A dictionary capability. Compact values have no term ordinals.
    pub fn dictionary_opt(&self) -> Option<&Dictionary<VoidSSTable>> {
        match &self.storage {
            BytesStorage::Dictionary { dictionary, .. } => Some(dictionary),
            _ => None,
        }
    }

    pub fn ords_opt(&self) -> Option<&Column<u64>> {
        match &self.storage {
            BytesStorage::Dictionary { ords, .. } => Some(ords),
            _ => None,
        }
    }

    pub fn index(&self) -> &ColumnIndex {
        match &self.storage {
            BytesStorage::Dictionary { ords, .. } => &ords.index,
            BytesStorage::Compact { index, .. } => index,
        }
    }

    pub fn num_values(&self) -> u32 {
        match &self.storage {
            BytesStorage::Dictionary { ords, .. } => ords.values.num_vals(),
            BytesStorage::Compact { column, .. } => column.num_values(),
        }
    }

    /// Reads a value by its position in the column, not by a term ordinal.
    pub fn read_value(&self, value: RowId, output: &mut Vec<u8>) -> io::Result<()> {
        match &self.storage {
            BytesStorage::Compact { column, .. } => column.read_value(value, output),
            BytesStorage::Dictionary { dictionary, ords } => {
                output.clear();
                if value >= ords.values.num_vals() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "value out of range",
                    ));
                }
                if !dictionary.ord_to_term(ords.values.get_val(value), output)? {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "missing term ordinal",
                    ));
                }
                Ok(())
            }
        }
    }

    pub fn value_range(&self, doc: RowId) -> io::Result<std::ops::Range<RowId>> {
        if doc >= self.num_rows() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "document out of range",
            ));
        }
        if let Some(column) = self.compact() {
            return column.value_range(doc);
        }
        Ok(self.index().value_row_ids(doc))
    }

    /// Replaces `output` with the first value. False means missing; an empty
    /// buffer with true means a present empty value.
    pub fn first(&self, doc: RowId, output: &mut Vec<u8>) -> io::Result<bool> {
        output.clear();
        let range = self.value_range(doc)?;
        if range.is_empty() {
            return Ok(false);
        }
        self.read_value(range.start, output)?;
        Ok(true)
    }

    /// Fills the given `output` buffer with the term associated to the ordinal `ord`.
    ///
    /// Returns `false` if the term does not exist (e.g. `term_ord` is greater or equal to the
    /// overll number of terms).
    pub fn ord_to_bytes(&self, ord: u64, output: &mut Vec<u8>) -> io::Result<bool> {
        let dictionary = self.dictionary_opt().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "compact columns have no term ordinals",
            )
        })?;
        dictionary.ord_to_term(ord, output)
    }

    /// Returns the number of rows in the column.
    pub fn num_rows(&self) -> RowId {
        match &self.storage {
            BytesStorage::Dictionary { ords, .. } => ords.num_docs(),
            BytesStorage::Compact { column, .. } => column.num_docs(),
        }
    }

    pub fn term_ords(&self, row_id: RowId) -> impl Iterator<Item = u64> + '_ {
        self.ords().values_for_doc(row_id)
    }

    /// Returns the column of ordinals
    pub fn ords(&self) -> &Column<u64> {
        self.ords_opt()
            .expect("compact columns have no term ordinals; use value_range/read_value")
    }

    pub(crate) fn into_column_index(self) -> ColumnIndex {
        match self.storage {
            BytesStorage::Dictionary { ords, .. } => ords.index,
            BytesStorage::Compact { index, .. } => index,
        }
    }

    pub fn num_terms(&self) -> usize {
        self.dictionary().num_terms()
    }

    pub fn dictionary(&self) -> &Dictionary<VoidSSTable> {
        self.dictionary_opt()
            .expect("compact columns have no term dictionary; use read_value")
    }
}

#[derive(Clone)]
pub struct StrColumn(BytesColumn);

impl fmt::Debug for StrColumn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.0)
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
        self.0.dictionary()
    }

    /// Fills the buffer
    pub fn ord_to_str(&self, term_ord: u64, output: &mut String) -> io::Result<bool> {
        unsafe {
            let buf = output.as_mut_vec();
            if !self.0.ord_to_bytes(term_ord, buf)? {
                return Ok(false);
            }
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

    /// Reads the first string through either storage codec.
    pub fn first_str(&self, doc: RowId, output: &mut String) -> io::Result<bool> {
        // Never leave an invalid UTF-8 String behind, including on read errors.
        let mut bytes = std::mem::take(output).into_bytes();
        let result = self.0.first(doc, &mut bytes);
        if std::str::from_utf8(&bytes).is_err() {
            bytes.clear();
            *output = String::from_utf8(bytes).unwrap();
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid compact UTF-8",
            ));
        }
        *output = String::from_utf8(bytes).unwrap();
        result
    }
}

impl Deref for StrColumn {
    type Target = BytesColumn;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
