use std::{fmt, io, mem};

use common::file_slice::FileSlice;
use common::BinarySerializable;
use sstable::{Dictionary, RangeSSTable};

use crate::columnar::{format_version, ColumnType};
use crate::dynamic_column::DynamicColumnHandle;
use crate::RowId;

fn io_invalid_data(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

/// The ColumnarReader makes it possible to access a set of columns
/// associated to field names.
#[derive(Clone)]
pub struct ColumnarReader {
    column_dictionary: Dictionary<RangeSSTable>,
    column_data: FileSlice,
    num_rows: RowId,
}

impl fmt::Debug for ColumnarReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let num_rows = self.num_rows();
        let columns = self.list_columns().unwrap();
        let num_cols = columns.len();
        let mut debug_struct = f.debug_struct("Columnar");
        debug_struct
            .field("num_rows", &num_rows)
            .field("num_cols", &num_cols);
        for (col_name, dynamic_column_handle) in columns.into_iter().take(5) {
            let col = dynamic_column_handle.open().unwrap();
            if col.num_values() > 10 {
                debug_struct.field(&col_name, &"..");
            } else {
                debug_struct.field(&col_name, &col);
            }
        }
        if num_cols > 5 {
            debug_struct.finish_non_exhaustive()?;
        } else {
            debug_struct.finish()?;
        }
        Ok(())
    }
}

/// Functions by both the async/sync code listing columns.
/// It takes a stream from the column sstable and return the list of
/// `DynamicColumn` available in it.
fn read_all_columns_in_stream(
    mut stream: sstable::Streamer<'_, RangeSSTable>,
    column_data: &FileSlice,
) -> io::Result<Vec<DynamicColumnHandle>> {
    let mut results = Vec::new();
    while stream.advance() {
        let key_bytes: &[u8] = stream.key();
        let Some(column_code) = key_bytes.last().copied() else {
            return Err(io_invalid_data("Empty column name.".to_string()));
        };
        let column_type = ColumnType::try_from_code(column_code)
            .map_err(|_| io_invalid_data(format!("Unknown column code `{column_code}`")))?;
        let range = stream.value();
        let file_slice = column_data.slice(range.start as usize..range.end as usize);
        let dynamic_column_handle = DynamicColumnHandle {
            file_slice,
            column_type,
        };
        results.push(dynamic_column_handle);
    }
    Ok(results)
}

impl ColumnarReader {
    /// Opens a new Columnar file.
    pub fn open<F>(file_slice: F) -> io::Result<ColumnarReader>
    where FileSlice: From<F> {
        Self::open_inner(file_slice.into())
    }

    fn open_inner(file_slice: FileSlice) -> io::Result<ColumnarReader> {
        let (file_slice_without_sstable_len, footer_slice) = file_slice
            .split_from_end(mem::size_of::<u64>() + 4 + format_version::VERSION_FOOTER_NUM_BYTES);
        let footer_bytes = footer_slice.read_bytes()?;
        let sstable_len = u64::deserialize(&mut &footer_bytes[0..8])?;
        let num_rows = u32::deserialize(&mut &footer_bytes[8..12])?;
        let version_footer_bytes: [u8; format_version::VERSION_FOOTER_NUM_BYTES] =
            footer_bytes[12..].try_into().unwrap();
        let _version = format_version::parse_footer(version_footer_bytes)?;
        let (column_data, sstable) =
            file_slice_without_sstable_len.split_from_end(sstable_len as usize);
        let column_dictionary = Dictionary::open(sstable)?;
        Ok(ColumnarReader {
            column_dictionary,
            column_data,
            num_rows,
        })
    }

    pub fn num_rows(&self) -> RowId {
        self.num_rows
    }
    // Iterate over the columns in a sorted way
    pub fn iter_columns(
        &self,
    ) -> io::Result<impl Iterator<Item = (String, DynamicColumnHandle)> + '_> {
        let mut stream = self.column_dictionary.stream()?;
        Ok(std::iter::from_fn(move || {
            if stream.advance() {
                let key_bytes: &[u8] = stream.key();
                let column_code: u8 = key_bytes.last().cloned().unwrap();
                // TODO Error Handling. The API gets quite ugly when returning the error here, so
                // instead we could just check the first N columns upfront.
                let column_type: ColumnType = ColumnType::try_from_code(column_code)
                    .map_err(|_| io_invalid_data(format!("Unknown column code `{column_code}`")))
                    .unwrap();
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
                Some((column_name, column_handle))
            } else {
                None
            }
        }))
    }

    // TODO Add unit tests
    pub fn list_columns(&self) -> io::Result<Vec<(String, DynamicColumnHandle)>> {
        Ok(self.iter_columns()?.collect())
    }

    fn stream_for_column_range(&self, column_name: &str) -> sstable::StreamerBuilder<RangeSSTable> {
        // Each column is a associated to a given `column_key`,
        // that starts by `column_name\0column_header`.
        //
        // Listing the columns associated to the given column name is therefore equivalent to
        // listing `column_key` with the prefix `column_name\0`.
        //
        // This is in turn equivalent to searching for the range
        // `[column_name,\0`..column_name\1)`.
        // TODO can we get some more generic `prefix(..)` logic in the dictionary.
        let mut start_key = column_name.to_string();
        start_key.push('\0');
        let mut end_key = column_name.to_string();
        end_key.push(1u8 as char);
        self.column_dictionary
            .range()
            .ge(start_key.as_bytes())
            .lt(end_key.as_bytes())
    }

    pub async fn read_columns_async(
        &self,
        column_name: &str,
    ) -> io::Result<Vec<DynamicColumnHandle>> {
        let stream = self
            .stream_for_column_range(column_name)
            .into_stream_async()
            .await?;
        read_all_columns_in_stream(stream, &self.column_data)
    }

    /// Get all columns for the given column name.
    ///
    /// There can be more than one column associated to a given column name, provided they have
    /// different types.
    pub fn read_columns(&self, column_name: &str) -> io::Result<Vec<DynamicColumnHandle>> {
        let stream = self.stream_for_column_range(column_name).into_stream()?;
        read_all_columns_in_stream(stream, &self.column_data)
    }

    /// Return the number of columns in the columnar.
    pub fn num_columns(&self) -> usize {
        self.column_dictionary.num_terms()
    }
}

#[cfg(test)]
mod tests {
    use crate::{ColumnType, ColumnarReader, ColumnarWriter};

    #[test]
    fn test_list_columns() {
        let mut columnar_writer = ColumnarWriter::default();
        columnar_writer.record_column_type("col1", ColumnType::Str, false);
        columnar_writer.record_column_type("col2", ColumnType::U64, false);
        let mut buffer = Vec::new();
        columnar_writer.serialize(1, None, &mut buffer).unwrap();
        let columnar = ColumnarReader::open(buffer).unwrap();
        let columns = columnar.list_columns().unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(&columns[0].0, "col1");
        assert_eq!(columns[0].1.column_type(), ColumnType::Str);
        assert_eq!(&columns[1].0, "col2");
        assert_eq!(columns[1].1.column_type(), ColumnType::U64);
    }

    #[test]
    fn test_list_columns_strict_typing_prevents_coercion() {
        let mut columnar_writer = ColumnarWriter::default();
        columnar_writer.record_column_type("count", ColumnType::U64, false);
        columnar_writer.record_numerical(1, "count", 1u64);
        let mut buffer = Vec::new();
        columnar_writer.serialize(2, None, &mut buffer).unwrap();
        let columnar = ColumnarReader::open(buffer).unwrap();
        let columns = columnar.list_columns().unwrap();
        assert_eq!(columns.len(), 1);
        assert_eq!(&columns[0].0, "count");
        assert_eq!(columns[0].1.column_type(), ColumnType::U64);
    }

    #[test]
    #[should_panic(expected = "Input type forbidden")]
    fn test_list_columns_strict_typing_panics_on_wrong_types() {
        let mut columnar_writer = ColumnarWriter::default();
        columnar_writer.record_column_type("count", ColumnType::U64, false);
        columnar_writer.record_numerical(1, "count", 1i64);
    }
}
