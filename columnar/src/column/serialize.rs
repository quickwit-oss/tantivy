use std::io;
use std::io::Write;
use std::sync::Arc;

use common::{CountingWriter, OwnedBytes};
use sstable::Dictionary;

use crate::column::{BytesColumn, Column};
use crate::column_index::{serialize_column_index, SerializableColumnIndex};
use crate::column_values::{
    serialize_column_values, ColumnValues, MonotonicallyMappableToU64, ALL_CODEC_TYPES,
};
pub fn serialize_column_u64<T: MonotonicallyMappableToU64>(
    column_index: SerializableColumnIndex<'_>,
    column_values: &impl ColumnValues<T>,
    output: &mut impl Write,
) -> io::Result<()> {
    let mut counting_writer = CountingWriter::wrap(output);
    serialize_column_index(column_index, &mut counting_writer)?;
    let column_index_num_bytes = counting_writer.written_bytes() as u32;
    let output = counting_writer.finish();
    serialize_column_values(column_values, &ALL_CODEC_TYPES[..], output)?;
    output.write_all(&column_index_num_bytes.to_le_bytes())?;
    Ok(())
}

pub fn open_column_u64<T: MonotonicallyMappableToU64>(bytes: OwnedBytes) -> io::Result<Column<T>> {
    let (body, column_index_num_bytes_payload) = bytes.rsplit(4);
    let column_index_num_bytes = u32::from_le_bytes(
        column_index_num_bytes_payload
            .as_slice()
            .try_into()
            .unwrap(),
    );
    let (column_index_data, column_values_data) = body.split(column_index_num_bytes as usize);
    let column_index = crate::column_index::open_column_index(column_index_data)?;
    let column_values = crate::column_values::open_u64_mapped(column_values_data)?;
    Ok(Column {
        idx: column_index,
        values: column_values,
    })
}

pub fn open_column_bytes(data: OwnedBytes) -> io::Result<BytesColumn> {
    let (body, dictionary_len_bytes) = data.rsplit(4);
    let dictionary_len = u32::from_le_bytes(dictionary_len_bytes.as_slice().try_into().unwrap());
    let (dictionary_bytes, column_bytes) = body.split(dictionary_len as usize);
    let dictionary = Arc::new(Dictionary::from_bytes(dictionary_bytes)?);
    let term_ord_column = crate::column::open_column_u64::<u64>(column_bytes)?;
    Ok(BytesColumn {
        dictionary,
        term_ord_column,
    })
}
