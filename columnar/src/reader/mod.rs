use std::ops::Range;
use std::{io, mem};

use common::file_slice::FileSlice;
use common::BinarySerializable;
use sstable::{Dictionary, SSTableRange};

use crate::column_type_header::ColumnTypeAndCardinality;

fn io_invalid_data(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg) // format!("Invalid key found.
                                                    // {key_bytes:?}")));
}
pub struct ColumnarReader {
    column_dictionary: Dictionary<SSTableRange>,
    column_data: FileSlice,
}

impl ColumnarReader {
    pub fn num_columns(&self) -> usize {
        self.column_dictionary.num_terms()
    }

    pub fn open(file_slice: FileSlice) -> io::Result<ColumnarReader> {
        let (file_slice_without_sstable_len, sstable_len_bytes) =
            file_slice.split_from_end(mem::size_of::<u64>());
        let mut sstable_len_bytes = sstable_len_bytes.read_bytes()?;
        let sstable_len = u64::deserialize(&mut sstable_len_bytes)?;
        let (column_data, sstable) =
            file_slice_without_sstable_len.split_from_end(sstable_len as usize);
        let column_dictionary = Dictionary::open(sstable)?;
        Ok(ColumnarReader {
            column_dictionary,
            column_data,
        })
    }

    pub fn read_columns(
        &self,
        field_name: &str,
    ) -> io::Result<Vec<(ColumnTypeAndCardinality, Range<u64>)>> {
        let mut start_key = field_name.to_string();
        start_key.push('\0');
        let mut end_key = field_name.to_string();
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
            if !key_bytes.starts_with(start_key.as_bytes()) {
                return Err(io_invalid_data(format!("Invalid key found. {key_bytes:?}")));
            }
            let column_code: u8 = key_bytes.last().cloned().unwrap();
            let column_type_and_cardinality = ColumnTypeAndCardinality::try_from_code(column_code)
                .ok_or_else(|| io_invalid_data(format!("Unknown column code `{column_code}`")))?;
            let range = stream.value().clone();
            results.push((column_type_and_cardinality, range));
        }
        Ok(results)
    }
}
