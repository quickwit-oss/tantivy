use std::fmt::Debug;
use std::io;
use std::io::Write;
use std::sync::Arc;

use common::OwnedBytes;
use sstable::Dictionary;

use crate::column::{BytesColumn, Column};
use crate::column_index::{serialize_column_index, SerializableColumnIndex};
use crate::column_values::serialize::serialize_column_values_u128;
use crate::column_values::{
    serialize_column_values, ColumnValues, FastFieldCodecType, MonotonicallyMappableToU128,
    MonotonicallyMappableToU64,
};

pub fn serialize_column_mappable_to_u128<
    F: Fn() -> I,
    I: Iterator<Item = T>,
    T: MonotonicallyMappableToU128,
>(
    column_index: SerializableColumnIndex<'_>,
    column_values: F,
    num_vals: u32,
    output: &mut impl Write,
) -> io::Result<()> {
    let column_index_num_bytes = serialize_column_index(column_index, output)?;
    serialize_column_values_u128(
        || column_values().map(|val| val.to_u128()),
        num_vals,
        output,
    )?;
    output.write_all(&column_index_num_bytes.to_le_bytes())?;
    Ok(())
}

pub fn serialize_column_mappable_to_u64<T: MonotonicallyMappableToU64 + Debug>(
    column_index: SerializableColumnIndex<'_>,
    column_values: &impl ColumnValues<T>,
    output: &mut impl Write,
) -> io::Result<()> {
    let column_index_num_bytes = serialize_column_index(column_index, output)?;
    serialize_column_values(
        column_values,
        &[
            FastFieldCodecType::Bitpacked,
            FastFieldCodecType::BlockwiseLinear,
        ],
        output,
    )?;
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

pub fn open_column_u128<T: MonotonicallyMappableToU128>(
    bytes: OwnedBytes,
) -> io::Result<Column<T>> {
    let (body, column_index_num_bytes_payload) = bytes.rsplit(4);
    let column_index_num_bytes = u32::from_le_bytes(
        column_index_num_bytes_payload
            .as_slice()
            .try_into()
            .unwrap(),
    );
    let (column_index_data, column_values_data) = body.split(column_index_num_bytes as usize);
    let column_index = crate::column_index::open_column_index(column_index_data)?;
    let column_values = crate::column_values::open_u128_mapped(column_values_data)?;
    Ok(Column {
        idx: column_index,
        values: column_values,
    })
}

pub fn open_column_bytes<T: From<BytesColumn>>(data: OwnedBytes) -> io::Result<T> {
    let (body, dictionary_len_bytes) = data.rsplit(4);
    let dictionary_len = u32::from_le_bytes(dictionary_len_bytes.as_slice().try_into().unwrap());
    let (dictionary_bytes, column_bytes) = body.split(dictionary_len as usize);
    let dictionary = Arc::new(Dictionary::from_bytes(dictionary_bytes)?);
    let term_ord_column = crate::column::open_column_u64::<u64>(column_bytes)?;
    let bytes_column = BytesColumn {
        dictionary,
        term_ord_column,
    };
    Ok(bytes_column.into())
}
