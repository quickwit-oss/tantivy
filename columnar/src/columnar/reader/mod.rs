use std::{io, mem};

use common::file_slice::FileSlice;
use common::BinarySerializable;
use sstable::{Dictionary, RangeSSTable};

use crate::columnar::{format_version, ColumnType};
use crate::dynamic_column::DynamicColumnHandle;

fn io_invalid_data(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

/// The ColumnarReader makes it possible to access a set of columns
/// associated to field names.
pub struct ColumnarReader {
    column_dictionary: Dictionary<RangeSSTable>,
    column_data: FileSlice,
}

impl ColumnarReader {
    /// Opens a new Columnar file.
    pub fn open<F>(file_slice: F) -> io::Result<ColumnarReader>
    where FileSlice: From<F> {
        Self::open_inner(file_slice.into())
    }

    fn open_inner(file_slice: FileSlice) -> io::Result<ColumnarReader> {
        let (file_slice_without_sstable_len, footer_slice) = file_slice
            .split_from_end(mem::size_of::<u64>() + format_version::VERSION_FOOTER_NUM_BYTES);
        let footer_bytes = footer_slice.read_bytes()?;
        let (mut sstable_len_bytes, version_footer_bytes) =
            footer_bytes.rsplit(format_version::VERSION_FOOTER_NUM_BYTES);
        let version_footer_bytes: [u8; format_version::VERSION_FOOTER_NUM_BYTES] =
            version_footer_bytes.as_slice().try_into().unwrap();
        let _version = format_version::parse_footer(version_footer_bytes)?;
        let sstable_len = u64::deserialize(&mut sstable_len_bytes)?;
        let (column_data, sstable) =
            file_slice_without_sstable_len.split_from_end(sstable_len as usize);
        let column_dictionary = Dictionary::open(sstable)?;
        Ok(ColumnarReader {
            column_dictionary,
            column_data,
        })
    }

    // TODO Add unit tests
    pub fn list_columns(&self) -> io::Result<Vec<(String, DynamicColumnHandle)>> {
        let mut stream = self.column_dictionary.stream()?;
        let mut results = Vec::new();
        while stream.advance() {
            let key_bytes: &[u8] = stream.key();
            let column_code: u8 = key_bytes.last().cloned().unwrap();
            let column_type: ColumnType = ColumnType::try_from_code(column_code)
                .map_err(|_| io_invalid_data(format!("Unknown column code `{column_code}`")))?;
            let range = stream.value().clone();
            let column_name =
                // The last two bytes are respectively the 0u8 separator and the column_type.
                String::from_utf8_lossy(&key_bytes[..key_bytes.len() - 2]).to_string();
            let file_slice = self
                .column_data
                .slice(range.start as usize..range.end as usize);
            let column_handle = DynamicColumnHandle {
                file_slice,
                column_type,
            };
            results.push((column_name, column_handle));
        }
        Ok(results)
    }

    /// Get all columns for the given column name.
    ///
    /// There can be more than one column associated to a given column name, provided they have
    /// different types.
    // TODO fix ugly API
    pub fn read_columns(&self, column_name: &str) -> io::Result<Vec<DynamicColumnHandle>> {
        // Each column is a associated to a given `column_key`,
        // that starts by `column_name\0column_header`.
        //
        // Listing the columns associated to the given column name is therefore equivalent to
        // listing `column_key` with the prefix `column_name\0`.
        //
        // This is in turn equivalent to searching for the range
        // `[column_name,\0`..column_name\1)`.

        // TODO can we get some more generic `prefix(..)` logic in the dictioanry.
        let mut start_key = column_name.to_string();
        start_key.push('\0');
        let mut end_key = column_name.to_string();
        end_key.push(1u8 as char);
        let mut stream = self
            .column_dictionary
            .range()
            .ge(start_key.as_bytes())
            .lt(end_key.as_bytes())
            .into_stream()?;
        let mut results = Vec::new();
        while stream.advance() {
            let key_bytes: &[u8] = stream.key();
            assert!(key_bytes.starts_with(start_key.as_bytes()));
            let column_code: u8 = key_bytes.last().cloned().unwrap();
            let column_type = ColumnType::try_from_code(column_code)
                .map_err(|_| io_invalid_data(format!("Unknown column code `{column_code}`")))?;
            let range = stream.value().clone();
            let file_slice = self
                .column_data
                .slice(range.start as usize..range.end as usize);
            let dynamic_column_handle = DynamicColumnHandle {
                file_slice,
                column_type,
            };
            results.push(dynamic_column_handle);
        }
        Ok(results)
    }

    /// Return the number of columns in the columnar.
    pub fn num_columns(&self) -> usize {
        self.column_dictionary.num_terms()
    }
}
